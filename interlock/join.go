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
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/bitmap"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var (
	_ InterlockingDirectorate = &HashJoinInterDirc{}
	_ InterlockingDirectorate = &NestedLoopApplyInterDirc{}
)

// HashJoinInterDirc implements the hash join algorithm.
type HashJoinInterDirc struct {
	baseInterlockingDirectorate

	probeSideInterDirc InterlockingDirectorate
	buildSideInterDirc InterlockingDirectorate
	buildSideEstCount  float64
	outerFilter        memex.CNFExprs
	probeKeys          []*memex.DeferredCauset
	buildKeys          []*memex.DeferredCauset
	isNullEQ           []bool
	probeTypes         []*types.FieldType
	buildTypes         []*types.FieldType

	// concurrency is the number of partition, build and join workers.
	concurrency   uint
	rowContainer  *hashEventContainer
	buildFinished chan error

	// closeCh add a dagger for closing interlock.
	closeCh        chan struct{}
	joinType       causetembedded.JoinType
	requiredEvents int64

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	probeChkResourceCh chan *probeChkResource
	probeResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinWorkerResult

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker   // track disk usage.

	outerMatchedStatus []*bitmap.ConcurrentBitmap
	useOuterToBuild    bool

	prepared    bool
	isOuterJoin bool

	// joinWorkerWaitGroup is for sync multiple join workers.
	joinWorkerWaitGroup sync.WaitGroup
	finished            atomic.Value

	stats *hashJoinRuntimeStats
}

// probeChkResource stores the result of the join probe side fetch worker,
// `dest` is for Chunk reuse: after join workers process the probe side chunk which is read from `dest`,
// they'll causetstore the used chunk as `chk`, and then the probe side fetch worker will put new data into `chk` and write `chk` into dest.
type probeChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

// Close implements the InterlockingDirectorate Close interface.
func (e *HashJoinInterDirc) Close() error {
	close(e.closeCh)
	e.finished.CausetStore(true)
	if e.prepared {
		if e.buildFinished != nil {
			for range e.buildFinished {
			}
		}
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.probeChkResourceCh != nil {
			close(e.probeChkResourceCh)
			for range e.probeChkResourceCh {
			}
		}
		for i := range e.probeResultChs {
			for range e.probeResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.probeChkResourceCh = nil
		e.joinChkResourceCh = nil
		terror.Call(e.rowContainer.Close)
	}
	e.outerMatchedStatus = e.outerMatchedStatus[:0]

	if e.stats != nil && e.rowContainer != nil {
		e.stats.hashStat = e.rowContainer.stat
	}
	err := e.baseInterlockingDirectorate.Close()
	return err
}

// Open implements the InterlockingDirectorate Open interface.
func (e *HashJoinInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	e.diskTracker = disk.NewTracker(e.id, -1)
	e.diskTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.DiskTracker)

	e.closeCh = make(chan struct{})
	e.finished.CausetStore(false)
	e.joinWorkerWaitGroup = sync.WaitGroup{}

	if e.probeTypes == nil {
		e.probeTypes = retTypes(e.probeSideInterDirc)
	}
	if e.buildTypes == nil {
		e.buildTypes = retTypes(e.buildSideInterDirc)
	}
	if e.runtimeStats != nil {
		e.stats = &hashJoinRuntimeStats{
			concurrent: cap(e.joiners),
		}
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, e.stats)
	}
	return nil
}

// fetchProbeSideChunks get chunks from fetches chunks from the big causet in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinInterDirc) fetchProbeSideChunks(ctx context.Context) {
	hasWaitedForBuild := false
	for {
		if e.finished.Load().(bool) {
			return
		}

		var probeSideResource *probeChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case probeSideResource, ok = <-e.probeChkResourceCh:
			if !ok {
				return
			}
		}
		probeSideResult := probeSideResource.chk
		if e.isOuterJoin {
			required := int(atomic.LoadInt64(&e.requiredEvents))
			probeSideResult.SetRequiredEvents(required, e.maxChunkSize)
		}
		err := Next(ctx, e.probeSideInterDirc, probeSideResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
				err: err,
			}
			return
		}
		if !hasWaitedForBuild {
			if probeSideResult.NumEvents() == 0 && !e.useOuterToBuild {
				e.finished.CausetStore(true)
				return
			}
			emptyBuild, buildErr := e.wait4BuildSide()
			if buildErr != nil {
				e.joinResultCh <- &hashjoinWorkerResult{
					err: buildErr,
				}
				return
			} else if emptyBuild {
				return
			}
			hasWaitedForBuild = true
		}

		if probeSideResult.NumEvents() == 0 {
			return
		}

		probeSideResource.dest <- probeSideResult
	}
}

func (e *HashJoinInterDirc) wait4BuildSide() (emptyBuild bool, err error) {
	select {
	case <-e.closeCh:
		return true, nil
	case err := <-e.buildFinished:
		if err != nil {
			return false, err
		}
	}
	if e.rowContainer.Len() == uint64(0) && (e.joinType == causetembedded.InnerJoin || e.joinType == causetembedded.SemiJoin) {
		return true, nil
	}
	return false, nil
}

// fetchBuildSideEvents fetches all rows from build side interlock, and append them
// to e.buildSideResult.
func (e *HashJoinInterDirc) fetchBuildSideEvents(ctx context.Context, chkCh chan<- *chunk.Chunk, doneCh <-chan struct{}) {
	defer close(chkCh)
	var err error
	for {
		if e.finished.Load().(bool) {
			return
		}
		chk := chunk.NewChunkWithCapacity(e.buildSideInterDirc.base().retFieldTypes, e.ctx.GetStochastikVars().MaxChunkSize)
		err = Next(ctx, e.buildSideInterDirc, chk)
		if err != nil {
			e.buildFinished <- errors.Trace(err)
			return
		}
		failpoint.Inject("errorFetchBuildSideEventsMockOOMPanic", nil)
		if chk.NumEvents() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-e.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *HashJoinInterDirc) initializeForProbe() {
	// e.probeResultChs is for transmitting the chunks which causetstore the data of
	// probeSideInterDirc, it'll be written by probe side worker goroutine, and read by join
	// workers.
	e.probeResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.probeChkResourceCh is for transmitting the used probeSideInterDirc chunks from
	// join workers to probeSideInterDirc worker.
	e.probeChkResourceCh = make(chan *probeChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeChkResourceCh <- &probeChkResource{
			chk:  newFirstChunk(e.probeSideInterDirc),
			dest: e.probeResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)
}

func (e *HashJoinInterDirc) fetchAndProbeHashBlock(ctx context.Context) {
	e.initializeForProbe()
	e.joinWorkerWaitGroup.Add(1)
	go soliton.WithRecovery(func() {
		defer trace.StartRegion(ctx, "HashJoinProbeSideFetcher").End()
		e.fetchProbeSideChunks(ctx)
	}, e.handleProbeSideFetcherPanic)

	probeKeyDefCausIdx := make([]int, len(e.probeKeys))
	for i := range e.probeKeys {
		probeKeyDefCausIdx[i] = e.probeKeys[i].Index
	}

	// Start e.concurrency join workers to probe hash causet and join build side and
	// probe side rows.
	for i := uint(0); i < e.concurrency; i++ {
		e.joinWorkerWaitGroup.Add(1)
		workID := i
		go soliton.WithRecovery(func() {
			defer trace.StartRegion(ctx, "HashJoinWorker").End()
			e.runJoinWorker(workID, probeKeyDefCausIdx)
		}, e.handleJoinWorkerPanic)
	}
	go soliton.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (e *HashJoinInterDirc) handleProbeSideFetcherPanic(r interface{}) {
	for i := range e.probeResultChs {
		close(e.probeResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinInterDirc) handleJoinWorkerPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

// Concurrently handling unmatched rows from the hash causet
func (e *HashJoinInterDirc) handleUnmatchedEventsFromHashBlock(workerID uint) {
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}
	numChks := e.rowContainer.NumChunks()
	for i := int(workerID); i < numChks; i += int(e.concurrency) {
		chk, err := e.rowContainer.GetChunk(i)
		if err != nil {
			// Catching the error and send it
			joinResult.err = err
			e.joinResultCh <- joinResult
			return
		}
		for j := 0; j < chk.NumEvents(); j++ {
			if !e.outerMatchedStatus[i].UnsafeIsSet(j) { // process unmatched outer rows
				e.joiners[workerID].onMissMatch(false, chk.GetEvent(j), joinResult.chk)
			}
			if joinResult.chk.IsFull() {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(workerID)
				if !ok {
					return
				}
			}
		}
	}

	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumEvents() > 0) {
		e.joinResultCh <- joinResult
	}
}

func (e *HashJoinInterDirc) waitJoinWorkersAndCloseResultChan() {
	e.joinWorkerWaitGroup.Wait()
	if e.useOuterToBuild {
		// Concurrently handling unmatched rows from the hash causet at the tail
		for i := uint(0); i < e.concurrency; i++ {
			var workerID = i
			e.joinWorkerWaitGroup.Add(1)
			go soliton.WithRecovery(func() { e.handleUnmatchedEventsFromHashBlock(workerID) }, e.handleJoinWorkerPanic)
		}
		e.joinWorkerWaitGroup.Wait()
	}
	close(e.joinResultCh)
}

func (e *HashJoinInterDirc) runJoinWorker(workerID uint, probeKeyDefCausIdx []int) {
	probeTime := int64(0)
	if e.stats != nil {
		start := time.Now()
		defer func() {
			t := time.Since(start)
			atomic.AddInt64(&e.stats.probe, probeTime)
			atomic.AddInt64(&e.stats.fetchAndProbe, int64(t))
			e.stats.setMaxFetchAndProbeTime(int64(t))
		}()
	}

	var (
		probeSideResult *chunk.Chunk
		selected        = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: e.probeResultChs[workerID],
	}
	hCtx := &hashContext{
		allTypes:      e.probeTypes,
		keyDefCausIdx: probeKeyDefCausIdx,
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case probeSideResult, ok = <-e.probeResultChs[workerID]:
		}
		if !ok {
			break
		}
		start := time.Now()
		if e.useOuterToBuild {
			ok, joinResult = e.join2ChunkForOuterHashJoin(workerID, probeSideResult, hCtx, joinResult)
		} else {
			ok, joinResult = e.join2Chunk(workerID, probeSideResult, hCtx, joinResult, selected)
		}
		probeTime += int64(time.Since(start))
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		e.probeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumEvents() > 0) {
		e.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumEvents() == 0 {
		e.joinChkResourceCh[workerID] <- joinResult.chk
	}
}

func (e *HashJoinInterDirc) joinMatchedProbeSideEvent2ChunkForOuterHashJoin(workerID uint, probeKey uint64, probeSideEvent chunk.Event, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	buildSideEvents, rowsPtrs, err := e.rowContainer.GetMatchedEventsAndPtrs(probeKey, probeSideEvent, hCtx)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideEvents) == 0 {
		return true, joinResult
	}

	iter := chunk.NewIterator4Slice(buildSideEvents)
	var outerMatchStatus []outerEventStatusFlag
	rowIdx := 0
	for iter.Begin(); iter.Current() != iter.End(); {
		outerMatchStatus, err = e.joiners[workerID].tryToMatchOuters(iter, probeSideEvent, joinResult.chk, outerMatchStatus)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		for i := range outerMatchStatus {
			if outerMatchStatus[i] == outerEventMatched {
				e.outerMatchedStatus[rowsPtrs[rowIdx+i].ChkIdx].Set(int(rowsPtrs[rowIdx+i].EventIdx))
			}
		}
		rowIdx += len(outerMatchStatus)
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}
func (e *HashJoinInterDirc) joinMatchedProbeSideEvent2Chunk(workerID uint, probeKey uint64, probeSideEvent chunk.Event, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	buildSideEvents, _, err := e.rowContainer.GetMatchedEventsAndPtrs(probeKey, probeSideEvent, hCtx)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(buildSideEvents) == 0 {
		e.joiners[workerID].onMissMatch(false, probeSideEvent, joinResult.chk)
		return true, joinResult
	}
	iter := chunk.NewIterator4Slice(buildSideEvents)
	hasMatch, hasNull := false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, isNull, err := e.joiners[workerID].tryToMatchInners(probeSideEvent, iter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !hasMatch {
		e.joiners[workerID].onMissMatch(hasNull, probeSideEvent, joinResult.chk)
	}
	return true, joinResult
}

func (e *HashJoinInterDirc) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
	}
	return ok, joinResult
}

func (e *HashJoinInterDirc) join2Chunk(workerID uint, probeSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	selected, err = memex.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(probeSideChk), selected)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	hCtx.initHash(probeSideChk.NumEvents())
	for keyIdx, i := range hCtx.keyDefCausIdx {
		ignoreNull := len(e.isNullEQ) > keyIdx && e.isNullEQ[keyIdx]
		err = codec.HashChunkSelected(e.rowContainer.sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[i], i, hCtx.buf, hCtx.hasNull, selected, ignoreNull)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}

	for i := range selected {
		if !selected[i] || hCtx.hasNull[i] { // process unmatched probe side rows
			e.joiners[workerID].onMissMatch(false, probeSideChk.GetEvent(i), joinResult.chk)
		} else { // process matched probe side rows
			probeKey, probeEvent := hCtx.hashVals[i].Sum64(), probeSideChk.GetEvent(i)
			ok, joinResult = e.joinMatchedProbeSideEvent2Chunk(workerID, probeKey, probeEvent, hCtx, joinResult)
			if !ok {
				return false, joinResult
			}
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// join2ChunkForOuterHashJoin joins chunks when using the outer to build a hash causet (refer to outer hash join)
func (e *HashJoinInterDirc) join2ChunkForOuterHashJoin(workerID uint, probeSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinWorkerResult) (ok bool, _ *hashjoinWorkerResult) {
	hCtx.initHash(probeSideChk.NumEvents())
	for _, i := range hCtx.keyDefCausIdx {
		err := codec.HashChunkDeferredCausets(e.rowContainer.sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[i], i, hCtx.buf, hCtx.hasNull)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}
	for i := 0; i < probeSideChk.NumEvents(); i++ {
		probeKey, probeEvent := hCtx.hashVals[i].Sum64(), probeSideChk.GetEvent(i)
		ok, joinResult = e.joinMatchedProbeSideEvent2ChunkForOuterHashJoin(workerID, probeKey, probeEvent, hCtx, joinResult)
		if !ok {
			return false, joinResult
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// Next implements the InterlockingDirectorate Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash causet;
// step 2. fetch data from probe child in a background goroutine and probe the hash causet in multiple join workers.
func (e *HashJoinInterDirc) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.buildFinished = make(chan error, 1)
		go soliton.WithRecovery(func() {
			defer trace.StartRegion(ctx, "HashJoinHashBlockBuilder").End()
			e.fetchAndBuildHashBlock(ctx)
		}, e.handleFetchAndBuildHashBlockPanic)
		e.fetchAndProbeHashBlock(ctx)
		e.prepared = true
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredEvents, int64(req.RequiredEvents()))
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.CausetStore(true)
		return result.err
	}
	req.SwapDeferredCausets(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinInterDirc) handleFetchAndBuildHashBlockPanic(r interface{}) {
	if r != nil {
		e.buildFinished <- errors.Errorf("%v", r)
	}
	close(e.buildFinished)
}

func (e *HashJoinInterDirc) fetchAndBuildHashBlock(ctx context.Context) {
	if e.stats != nil {
		start := time.Now()
		defer func() {
			e.stats.fetchAndBuildHashBlock = time.Since(start)
		}()
	}
	// buildSideResultCh transfers build side chunk from build side fetch to build hash causet.
	buildSideResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	fetchBuildSideEventsOk := make(chan error, 1)
	go soliton.WithRecovery(
		func() {
			defer trace.StartRegion(ctx, "HashJoinBuildSideFetcher").End()
			e.fetchBuildSideEvents(ctx, buildSideResultCh, doneCh)
		},
		func(r interface{}) {
			if r != nil {
				fetchBuildSideEventsOk <- errors.Errorf("%v", r)
			}
			close(fetchBuildSideEventsOk)
		},
	)

	// TODO: Parallel build hash causet. Currently not support because `unsafeHashBlock` is not thread-safe.
	err := e.buildHashBlockForList(buildSideResultCh)
	if err != nil {
		e.buildFinished <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchBuildSideEvents be finished.
	// 1. if buildHashBlockForList fails
	// 2. if probeSideResult.NumEvents() == 0, fetchProbeSideChunks will not wait for the build side.
	for range buildSideResultCh {
	}
	// Check whether err is nil to avoid sending redundant error into buildFinished.
	if err == nil {
		if err = <-fetchBuildSideEventsOk; err != nil {
			e.buildFinished <- err
		}
	}
}

// buildHashBlockForList builds hash causet from `list`.
func (e *HashJoinInterDirc) buildHashBlockForList(buildSideResultCh <-chan *chunk.Chunk) error {
	buildKeyDefCausIdx := make([]int, len(e.buildKeys))
	for i := range e.buildKeys {
		buildKeyDefCausIdx[i] = e.buildKeys[i].Index
	}
	hCtx := &hashContext{
		allTypes:      e.buildTypes,
		keyDefCausIdx: buildKeyDefCausIdx,
	}
	var err error
	var selected []bool
	e.rowContainer = newHashEventContainer(e.ctx, int(e.buildSideEstCount), hCtx)
	e.rowContainer.GetMemTracker().AttachTo(e.memTracker)
	e.rowContainer.GetMemTracker().SetLabel(memory.LabelForBuildSideResult)
	e.rowContainer.GetDiskTracker().AttachTo(e.diskTracker)
	e.rowContainer.GetDiskTracker().SetLabel(memory.LabelForBuildSideResult)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill := e.rowContainer.CausetActionSpill()
		failpoint.Inject("testEventContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				actionSpill = e.rowContainer.rowContainer.CausetActionSpillForTest()
				defer actionSpill.(*chunk.SpillDiskCausetAction).WaitForTest()
			}
		})
		e.ctx.GetStochastikVars().StmtCtx.MemTracker.FallbackOldAndSetNewCausetAction(actionSpill)
	}
	for chk := range buildSideResultCh {
		if e.finished.Load().(bool) {
			return nil
		}
		if !e.useOuterToBuild {
			err = e.rowContainer.PutChunk(chk, e.isNullEQ)
		} else {
			var bitMap = bitmap.NewConcurrentBitmap(chk.NumEvents())
			e.outerMatchedStatus = append(e.outerMatchedStatus, bitMap)
			e.memTracker.Consume(bitMap.BytesConsumed())
			if len(e.outerFilter) == 0 {
				err = e.rowContainer.PutChunk(chk, e.isNullEQ)
			} else {
				selected, err = memex.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(chk), selected)
				if err != nil {
					return err
				}
				err = e.rowContainer.PutChunkSelected(chk, selected, e.isNullEQ)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// NestedLoopApplyInterDirc is the interlock for apply.
type NestedLoopApplyInterDirc struct {
	baseInterlockingDirectorate

	ctx            stochastikctx.Context
	innerEvents    []chunk.Event
	cursor         int
	innerInterDirc InterlockingDirectorate
	outerInterDirc InterlockingDirectorate
	innerFilter    memex.CNFExprs
	outerFilter    memex.CNFExprs

	joiner joiner

	cache              *applyCache
	canUseCache        bool
	cacheHitCounter    int
	cacheAccessCounter int

	outerSchema []*memex.CorrelatedDeferredCauset

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerEvent       *chunk.Event
	hasMatch         bool
	hasNull          bool

	outer bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the InterlockingDirectorate interface.
func (e *NestedLoopApplyInterDirc) Close() error {
	e.innerEvents = nil
	e.memTracker = nil
	if e.runtimeStats != nil {
		runtimeStats := newJoinRuntimeStats()
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, runtimeStats)
		if e.canUseCache {
			var hitRatio float64
			if e.cacheAccessCounter > 0 {
				hitRatio = float64(e.cacheHitCounter) / float64(e.cacheAccessCounter)
			}
			runtimeStats.setCacheInfo(true, hitRatio)
		} else {
			runtimeStats.setCacheInfo(false, 0)
		}
	}
	return e.outerInterDirc.Close()
}

// Open implements the InterlockingDirectorate interface.
func (e *NestedLoopApplyInterDirc) Open(ctx context.Context) error {
	err := e.outerInterDirc.Open(ctx)
	if err != nil {
		return err
	}
	e.cursor = 0
	e.innerEvents = e.innerEvents[:0]
	e.outerChunk = newFirstChunk(e.outerInterDirc)
	e.innerChunk = newFirstChunk(e.innerInterDirc)
	e.innerList = chunk.NewList(retTypes(e.innerInterDirc), e.initCap, e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel(memory.LabelForInnerList)
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	if e.canUseCache {
		e.cache, err = newApplyCache(e.ctx)
		if err != nil {
			return err
		}
		e.cacheHitCounter = 0
		e.cacheAccessCounter = 0
		e.cache.GetMemTracker().AttachTo(e.memTracker)
	}
	return nil
}

func (e *NestedLoopApplyInterDirc) fetchSelectedOuterEvent(ctx context.Context, chk *chunk.Chunk) (*chunk.Event, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumEvents() {
			err := Next(ctx, e.outerInterDirc, e.outerChunk)
			if err != nil {
				return nil, err
			}
			if e.outerChunk.NumEvents() == 0 {
				return nil, nil
			}
			e.outerSelected, err = memex.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
			if err != nil {
				return nil, err
			}
			e.outerChunkCursor = 0
		}
		outerEvent := e.outerChunk.GetEvent(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			return &outerEvent, nil
		} else if e.outer {
			e.joiner.onMissMatch(false, outerEvent, chk)
			if chk.IsFull() {
				return nil, nil
			}
		}
	}
}

// fetchAllInners reads all data from the inner causet and stores them in a List.
func (e *NestedLoopApplyInterDirc) fetchAllInners(ctx context.Context) error {
	err := e.innerInterDirc.Open(ctx)
	defer terror.Call(e.innerInterDirc.Close)
	if err != nil {
		return err
	}

	if e.canUseCache {
		// create a new one since it may be in the cache
		e.innerList = chunk.NewList(retTypes(e.innerInterDirc), e.initCap, e.maxChunkSize)
	} else {
		e.innerList.Reset()
	}
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := Next(ctx, e.innerInterDirc, e.innerChunk)
		if err != nil {
			return err
		}
		if e.innerChunk.NumEvents() == 0 {
			return nil
		}

		e.innerSelected, err = memex.VectorizedFilter(e.ctx, e.innerFilter, innerIter, e.innerSelected)
		if err != nil {
			return err
		}
		for event := innerIter.Begin(); event != innerIter.End(); event = innerIter.Next() {
			if e.innerSelected[event.Idx()] {
				e.innerList.AppendEvent(event)
			}
		}
	}
}

// Next implements the InterlockingDirectorate interface.
func (e *NestedLoopApplyInterDirc) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			if e.outerEvent != nil && !e.hasMatch {
				e.joiner.onMissMatch(e.hasNull, *e.outerEvent, req)
			}
			e.outerEvent, err = e.fetchSelectedOuterEvent(ctx, req)
			if e.outerEvent == nil || err != nil {
				return err
			}
			e.hasMatch = false
			e.hasNull = false

			if e.canUseCache {
				var key []byte
				for _, defCaus := range e.outerSchema {
					*defCaus.Data = e.outerEvent.GetCauset(defCaus.Index, defCaus.RetType)
					key, err = codec.EncodeKey(e.ctx.GetStochastikVars().StmtCtx, key, *defCaus.Data)
					if err != nil {
						return err
					}
				}
				e.cacheAccessCounter++
				value, err := e.cache.Get(key)
				if err != nil {
					return err
				}
				if value != nil {
					e.innerList = value
					e.cacheHitCounter++
				} else {
					err = e.fetchAllInners(ctx)
					if err != nil {
						return err
					}
					if _, err := e.cache.Set(key, e.innerList); err != nil {
						return err
					}
				}
			} else {
				for _, defCaus := range e.outerSchema {
					*defCaus.Data = e.outerEvent.GetCauset(defCaus.Index, defCaus.RetType)
				}
				err = e.fetchAllInners(ctx)
				if err != nil {
					return err
				}
			}
			e.innerIter = chunk.NewIterator4List(e.innerList)
			e.innerIter.Begin()
		}

		matched, isNull, err := e.joiner.tryToMatchInners(*e.outerEvent, e.innerIter, req)
		e.hasMatch = e.hasMatch || matched
		e.hasNull = e.hasNull || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}

// cacheInfo is used to save the concurrency information of the interlock operator
type cacheInfo struct {
	hitRatio float64
	useCache bool
}

type joinRuntimeStats struct {
	*execdetails.RuntimeStatsWithConcurrencyInfo

	applyCache  bool
	cache       cacheInfo
	hasHashStat bool
	hashStat    hashStatistic
}

func newJoinRuntimeStats() *joinRuntimeStats {
	stats := &joinRuntimeStats{
		RuntimeStatsWithConcurrencyInfo: &execdetails.RuntimeStatsWithConcurrencyInfo{},
	}
	return stats
}

// setCacheInfo sets the cache information. Only used for apply interlock.
func (e *joinRuntimeStats) setCacheInfo(useCache bool, hitRatio float64) {
	e.Lock()
	e.applyCache = true
	e.cache.useCache = useCache
	e.cache.hitRatio = hitRatio
	e.Unlock()
}

func (e *joinRuntimeStats) setHashStat(hashStat hashStatistic) {
	e.Lock()
	e.hasHashStat = true
	e.hashStat = hashStat
	e.Unlock()
}

func (e *joinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString(e.RuntimeStatsWithConcurrencyInfo.String())
	if e.applyCache {
		if e.cache.useCache {
			buf.WriteString(fmt.Sprintf(", cache:ON, cacheHitRatio:%.3f%%", e.cache.hitRatio*100))
		} else {
			buf.WriteString(fmt.Sprintf(", cache:OFF"))
		}
	}
	if e.hasHashStat {
		buf.WriteString(", " + e.hashStat.String())
	}
	return buf.String()
}

// Tp implements the RuntimeStats interface.
func (e *joinRuntimeStats) Tp() int {
	return execdetails.TpJoinRuntimeStats
}

type hashJoinRuntimeStats struct {
	fetchAndBuildHashBlock time.Duration
	hashStat               hashStatistic
	fetchAndProbe          int64
	probe                  int64
	concurrent             int
	maxFetchAndProbe       int64
}

func (e *hashJoinRuntimeStats) setMaxFetchAndProbeTime(t int64) {
	for {
		value := atomic.LoadInt64(&e.maxFetchAndProbe)
		if t <= value {
			return
		}
		if atomic.CompareAndSwapInt64(&e.maxFetchAndProbe, value, t) {
			return
		}
	}
}

// Tp implements the RuntimeStats interface.
func (e *hashJoinRuntimeStats) Tp() int {
	return execdetails.TpHashJoinRuntimeStats
}

func (e *hashJoinRuntimeStats) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if e.fetchAndBuildHashBlock > 0 {
		buf.WriteString("build_hash_block:{total:")
		buf.WriteString(e.fetchAndBuildHashBlock.String())
		buf.WriteString(", fetch:")
		buf.WriteString((e.fetchAndBuildHashBlock - e.hashStat.buildBlockElapse).String())
		buf.WriteString(", build:")
		buf.WriteString(e.hashStat.buildBlockElapse.String())
		buf.WriteString("}")
	}
	if e.probe > 0 {
		buf.WriteString(", probe:{concurrency:")
		buf.WriteString(strconv.Itoa(e.concurrent))
		buf.WriteString(", total:")
		buf.WriteString(time.Duration(e.fetchAndProbe).String())
		buf.WriteString(", max:")
		buf.WriteString(time.Duration(atomic.LoadInt64(&e.maxFetchAndProbe)).String())
		buf.WriteString(", probe:")
		buf.WriteString(time.Duration(e.probe).String())
		buf.WriteString(", fetch:")
		buf.WriteString(time.Duration(e.fetchAndProbe - e.probe).String())
		if e.hashStat.probeDefCauslision > 0 {
			buf.WriteString(", probe_defCauslision:")
			buf.WriteString(strconv.Itoa(e.hashStat.probeDefCauslision))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

func (e *hashJoinRuntimeStats) Clone() execdetails.RuntimeStats {
	return &hashJoinRuntimeStats{
		fetchAndBuildHashBlock: e.fetchAndBuildHashBlock,
		hashStat:               e.hashStat,
		fetchAndProbe:          e.fetchAndProbe,
		probe:                  e.probe,
		concurrent:             e.concurrent,
		maxFetchAndProbe:       e.maxFetchAndProbe,
	}
}

func (e *hashJoinRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*hashJoinRuntimeStats)
	if !ok {
		return
	}
	e.fetchAndBuildHashBlock += tmp.fetchAndBuildHashBlock
	e.hashStat.buildBlockElapse += tmp.hashStat.buildBlockElapse
	e.hashStat.probeDefCauslision += tmp.hashStat.probeDefCauslision
	e.fetchAndProbe += tmp.fetchAndProbe
	e.probe += tmp.probe
	if e.maxFetchAndProbe < tmp.maxFetchAndProbe {
		e.maxFetchAndProbe = tmp.maxFetchAndProbe
	}
}
