// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package interlock

import (
	"context"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"go.uber.org/zap"
)

type result struct {
	chk *chunk.Chunk
	err error
}

type outerEvent struct {
	event      *chunk.Event
	selected bool // if this event is selected by the outer side
}

// ParallelNestedLoopApplyInterDirc is the interlock for apply.
type ParallelNestedLoopApplyInterDirc struct {
	baseInterlockingDirectorate

	// outer-side fields
	cursor        int
	outerInterDirc     InterlockingDirectorate
	outerFilter   memex.CNFExprs
	outerList     *chunk.List
	outerEventMutex sync.Mutex
	outer         bool

	// inner-side fields
	// use slices since the inner side is paralleled
	corDefCauss       [][]*memex.CorrelatedDeferredCauset
	innerFilter   []memex.CNFExprs
	innerInterDircs    []InterlockingDirectorate
	innerList     []*chunk.List
	innerChunk    []*chunk.Chunk
	innerSelected [][]bool
	innerIter     []chunk.Iterator
	outerEvent      []*chunk.Event
	hasMatch      []bool
	hasNull       []bool
	joiners       []joiner

	// fields about concurrency control
	concurrency int
	started     uint32
	freeChkCh   chan *chunk.Chunk
	resultChkCh chan result
	outerEventCh  chan outerEvent
	exit        chan struct{}
	workerWg    sync.WaitGroup
	notifyWg    sync.WaitGroup

	// fields about cache
	cache              *applyCache
	useCache           bool
	cacheHitCounter    int64
	cacheAccessCounter int64
	cacheLock          sync.RWMutex

	memTracker *memory.Tracker // track memory usage.
}

// Open implements the InterlockingDirectorate interface.
func (e *ParallelNestedLoopApplyInterDirc) Open(ctx context.Context) error {
	err := e.outerInterDirc.Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	e.outerList = chunk.NewList(retTypes(e.outerInterDirc), e.initCap, e.maxChunkSize)
	e.outerList.GetMemTracker().SetLabel(memory.LabelForOuterList)
	e.outerList.GetMemTracker().AttachTo(e.memTracker)

	e.innerList = make([]*chunk.List, e.concurrency)
	e.innerChunk = make([]*chunk.Chunk, e.concurrency)
	e.innerSelected = make([][]bool, e.concurrency)
	e.innerIter = make([]chunk.Iterator, e.concurrency)
	e.outerEvent = make([]*chunk.Event, e.concurrency)
	e.hasMatch = make([]bool, e.concurrency)
	e.hasNull = make([]bool, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.innerChunk[i] = newFirstChunk(e.innerInterDircs[i])
		e.innerList[i] = chunk.NewList(retTypes(e.innerInterDircs[i]), e.initCap, e.maxChunkSize)
		e.innerList[i].GetMemTracker().SetLabel(memory.LabelForInnerList)
		e.innerList[i].GetMemTracker().AttachTo(e.memTracker)
	}

	e.freeChkCh = make(chan *chunk.Chunk, e.concurrency)
	e.resultChkCh = make(chan result, e.concurrency+1) // innerWorkers + outerWorker
	e.outerEventCh = make(chan outerEvent)
	e.exit = make(chan struct{})
	for i := 0; i < e.concurrency; i++ {
		e.freeChkCh <- newFirstChunk(e)
	}

	if e.useCache {
		if e.cache, err = newApplyCache(e.ctx); err != nil {
			return err
		}
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

// Next implements the InterlockingDirectorate interface.
func (e *ParallelNestedLoopApplyInterDirc) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if atomic.CompareAndSwapUint32(&e.started, 0, 1) {
		e.workerWg.Add(1)
		go e.outerWorker(ctx)
		for i := 0; i < e.concurrency; i++ {
			e.workerWg.Add(1)
			workID := i
			go e.innerWorker(ctx, workID)
		}
		e.notifyWg.Add(1)
		go e.notifyWorker(ctx)
	}
	result := <-e.resultChkCh
	if result.err != nil {
		return result.err
	}
	if result.chk == nil { // no more data
		req.Reset()
		return nil
	}
	req.SwapDeferredCausets(result.chk)
	e.freeChkCh <- result.chk
	return nil
}

// Close implements the InterlockingDirectorate interface.
func (e *ParallelNestedLoopApplyInterDirc) Close() error {
	e.memTracker = nil
	err := e.outerInterDirc.Close()
	if atomic.LoadUint32(&e.started) == 1 {
		close(e.exit)
		e.notifyWg.Wait()
		e.started = 0
	}

	if e.runtimeStats != nil {
		runtimeStats := newJoinRuntimeStats()
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, runtimeStats)
		if e.useCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			runtimeStats.setCacheInfo(true, hitRatio)
		} else {
			runtimeStats.setCacheInfo(false, 0)
		}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", e.concurrency))
	}
	return err
}

// notifyWorker waits for all inner/outer-workers finishing and then put an empty
// chunk into the resultCh to notify the upper interlock there is no more data.
func (e *ParallelNestedLoopApplyInterDirc) notifyWorker(ctx context.Context) {
	defer e.handleWorkerPanic(ctx, &e.notifyWg)
	e.workerWg.Wait()
	e.putResult(nil, nil)
}

func (e *ParallelNestedLoopApplyInterDirc) outerWorker(ctx context.Context) {
	defer trace.StartRegion(ctx, "ParallelApplyOuterWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	var selected []bool
	var err error
	for {
		chk := newFirstChunk(e.outerInterDirc)
		if err := Next(ctx, e.outerInterDirc, chk); err != nil {
			e.putResult(nil, err)
			return
		}
		if chk.NumEvents() == 0 {
			close(e.outerEventCh)
			return
		}
		e.outerList.Add(chk)
		outerIter := chunk.NewIterator4Chunk(chk)
		selected, err = memex.VectorizedFilter(e.ctx, e.outerFilter, outerIter, selected)
		if err != nil {
			e.putResult(nil, err)
			return
		}
		for i := 0; i < chk.NumEvents(); i++ {
			event := chk.GetEvent(i)
			select {
			case e.outerEventCh <- outerEvent{&event, selected[i]}:
			case <-e.exit:
				return
			}
		}
	}
}

func (e *ParallelNestedLoopApplyInterDirc) innerWorker(ctx context.Context, id int) {
	defer trace.StartRegion(ctx, "ParallelApplyInnerWorker").End()
	defer e.handleWorkerPanic(ctx, &e.workerWg)
	for {
		var chk *chunk.Chunk
		select {
		case chk = <-e.freeChkCh:
		case <-e.exit:
			return
		}
		err := e.fillInnerChunk(ctx, id, chk)
		if err == nil && chk.NumEvents() == 0 { // no more data, this goroutine can exit
			return
		}
		if e.putResult(chk, err) {
			return
		}
	}
}

func (e *ParallelNestedLoopApplyInterDirc) putResult(chk *chunk.Chunk, err error) (exit bool) {
	select {
	case e.resultChkCh <- result{chk, err}:
		return false
	case <-e.exit:
		return true
	}
}

func (e *ParallelNestedLoopApplyInterDirc) handleWorkerPanic(ctx context.Context, wg *sync.WaitGroup) {
	if r := recover(); r != nil {
		err := errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("parallel nested loop join worker panicked", zap.Error(err), zap.Stack("stack"))
		e.resultChkCh <- result{nil, err}
	}
	if wg != nil {
		wg.Done()
	}
}

// fetchAllInners reads all data from the inner causet and stores them in a List.
func (e *ParallelNestedLoopApplyInterDirc) fetchAllInners(ctx context.Context, id int) (err error) {
	var key []byte
	for _, defCaus := range e.corDefCauss[id] {
		*defCaus.Data = e.outerEvent[id].GetCauset(defCaus.Index, defCaus.RetType)
		if e.useCache {
			if key, err = codec.EncodeKey(e.ctx.GetStochastikVars().StmtCtx, key, *defCaus.Data); err != nil {
				return err
			}
		}
	}
	if e.useCache { // look up the cache
		atomic.AddInt64(&e.cacheAccessCounter, 1)
		e.cacheLock.RLock()
		value, err := e.cache.Get(key)
		e.cacheLock.RUnlock()
		if err != nil {
			return err
		}
		if value != nil {
			e.innerList[id] = value
			atomic.AddInt64(&e.cacheHitCounter, 1)
			return nil
		}
	}

	err = e.innerInterDircs[id].Open(ctx)
	defer terror.Call(e.innerInterDircs[id].Close)
	if err != nil {
		return err
	}

	if e.useCache {
		// create a new one in this case since it may be in the cache
		e.innerList[id] = chunk.NewList(retTypes(e.innerInterDircs[id]), e.initCap, e.maxChunkSize)
	} else {
		e.innerList[id].Reset()
	}

	innerIter := chunk.NewIterator4Chunk(e.innerChunk[id])
	for {
		err := Next(ctx, e.innerInterDircs[id], e.innerChunk[id])
		if err != nil {
			return err
		}
		if e.innerChunk[id].NumEvents() == 0 {
			break
		}

		e.innerSelected[id], err = memex.VectorizedFilter(e.ctx, e.innerFilter[id], innerIter, e.innerSelected[id])
		if err != nil {
			return err
		}
		for event := innerIter.Begin(); event != innerIter.End(); event = innerIter.Next() {
			if e.innerSelected[id][event.Idx()] {
				e.innerList[id].AppendEvent(event)
			}
		}
	}

	if e.useCache { // uFIDelate the cache
		e.cacheLock.Lock()
		defer e.cacheLock.Unlock()
		if _, err := e.cache.Set(key, e.innerList[id]); err != nil {
			return err
		}
	}
	return nil
}

func (e *ParallelNestedLoopApplyInterDirc) fetchNextOuterEvent(id int, req *chunk.Chunk) (event *chunk.Event, exit bool) {
	for {
		select {
		case outerEvent, ok := <-e.outerEventCh:
			if !ok { // no more data
				return nil, false
			}
			if !outerEvent.selected {
				if e.outer {
					e.joiners[id].onMissMatch(false, *outerEvent.event, req)
					if req.IsFull() {
						return nil, false
					}
				}
				continue // try the next outer event
			}
			return outerEvent.event, false
		case <-e.exit:
			return nil, true
		}
	}
}

func (e *ParallelNestedLoopApplyInterDirc) fillInnerChunk(ctx context.Context, id int, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter[id] == nil || e.innerIter[id].Current() == e.innerIter[id].End() {
			if e.outerEvent[id] != nil && !e.hasMatch[id] {
				e.joiners[id].onMissMatch(e.hasNull[id], *e.outerEvent[id], req)
			}
			var exit bool
			e.outerEvent[id], exit = e.fetchNextOuterEvent(id, req)
			if exit || req.IsFull() || e.outerEvent[id] == nil {
				return nil
			}

			e.hasMatch[id] = false
			e.hasNull[id] = false

			err = e.fetchAllInners(ctx, id)
			if err != nil {
				return err
			}
			e.innerIter[id] = chunk.NewIterator4List(e.innerList[id])
			e.innerIter[id].Begin()
		}

		matched, isNull, err := e.joiners[id].tryToMatchInners(*e.outerEvent[id], e.innerIter[id], req)
		e.hasMatch[id] = e.hasMatch[id] || matched
		e.hasNull[id] = e.hasNull[id] || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
