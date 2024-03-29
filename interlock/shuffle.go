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
	"sync"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

// ShuffleInterDirc is the interlock to run other interlocks in a parallel manner.
//  1. It fetches chunks from `DataSource`.
//  2. It splits tuples from `DataSource` into N partitions (Only "split by hash" is implemented so far).
//  3. It invokes N workers in parallel, assign each partition as input to each worker and execute child interlocks.
//  4. It defCauslects outputs from each worker, then sends outputs to its parent.
//
//                                +-------------+
//                        +-------| Main Thread |
//                        |       +------+------+
//                        |              ^
//                        |              |
//                        |              +
//                        v             +++
//                 outputHolderCh       | | outputCh (1 x Concurrency)
//                        v             +++
//                        |              ^
//                        |              |
//                        |      +-------+-------+
//                        v      |               |
//                 +--------------+             +--------------+
//          +----- |    worker    |   .......   |    worker    |  worker (N Concurrency): child interlock, eg. WindowInterDirc (+SortInterDirc)
//          |      +------------+-+             +-+------------+
//          |                 ^                 ^
//          |                 |                 |
//          |                +-+  +-+  ......  +-+
//          |                | |  | |          | |
//          |                ...  ...          ...  inputCh (Concurrency x 1)
//          v                | |  | |          | |
//    inputHolderCh          +++  +++          +++
//          v                 ^    ^            ^
//          |                 |    |            |
//          |          +------o----+            |
//          |          |      +-----------------+-----+
//          |          |                              |
//          |      +---+------------+------------+----+-----------+
//          |      |              Partition Splitter              |
//          |      +--------------+-+------------+-+--------------+
//          |                             ^
//          |                             |
//          |             +---------------v-----------------+
//          +---------->  |    fetch data from DataSource   |
//                        +---------------------------------+
//
////////////////////////////////////////////////////////////////////////////////////////
type ShuffleInterDirc struct {
	baseInterlockingDirectorate
	concurrency int
	workers     []*shuffleWorker

	prepared bool
	executed bool

	splitter   partitionSplitter
	dataSource InterlockingDirectorate

	finishCh chan struct{}
	outputCh chan *shuffleOutput
}

type shuffleOutput struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// Open implements the InterlockingDirectorate Open interface.
func (e *ShuffleInterDirc) Open(ctx context.Context) error {
	if err := e.dataSource.Open(ctx); err != nil {
		return err
	}
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.finishCh = make(chan struct{}, 1)
	e.outputCh = make(chan *shuffleOutput, e.concurrency)

	for _, w := range e.workers {
		w.finishCh = e.finishCh

		w.inputCh = make(chan *chunk.Chunk, 1)
		w.inputHolderCh = make(chan *chunk.Chunk, 1)
		w.outputCh = e.outputCh
		w.outputHolderCh = make(chan *chunk.Chunk, 1)

		if err := w.childInterDirc.Open(ctx); err != nil {
			return err
		}

		w.inputHolderCh <- newFirstChunk(e.dataSource)
		w.outputHolderCh <- newFirstChunk(e)
	}

	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *ShuffleInterDirc) Close() error {
	if !e.prepared {
		for _, w := range e.workers {
			close(w.inputHolderCh)
			close(w.inputCh)
			close(w.outputHolderCh)
		}
		close(e.outputCh)
	}
	close(e.finishCh)
	for _, w := range e.workers {
		for range w.inputCh {
		}
	}
	for range e.outputCh { // workers exit before `e.outputCh` is closed.
	}
	e.executed = false

	if e.runtimeStats != nil {
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("ShuffleConcurrency", e.concurrency))
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, runtimeStats)
	}

	err := e.dataSource.Close()
	err1 := e.baseInterlockingDirectorate.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(err1)
}

func (e *ShuffleInterDirc) prepare4ParallelInterDirc(ctx context.Context) {
	go e.fetchDataAndSplit(ctx)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(e.workers))
	for _, w := range e.workers {
		go w.run(ctx, waitGroup)
	}

	go e.waitWorkerAndCloseOutput(waitGroup)
}

func (e *ShuffleInterDirc) waitWorkerAndCloseOutput(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.outputCh)
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ShuffleInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		e.prepare4ParallelInterDirc(ctx)
		e.prepared = true
	}

	failpoint.Inject("shuffleError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("ShuffleInterDirc.Next error"))
		}
	})

	if e.executed {
		return nil
	}

	result, ok := <-e.outputCh
	if !ok {
		e.executed = true
		return nil
	}
	if result.err != nil {
		return result.err
	}
	req.SwapDeferredCausets(result.chk) // `shuffleWorker` will not send an empty `result.chk` to `e.outputCh`.
	result.giveBackCh <- result.chk

	return nil
}

func recoveryShuffleInterDirc(output chan *shuffleOutput, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &shuffleOutput{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("shuffle panicked", zap.Error(err), zap.Stack("stack"))
}

func (e *ShuffleInterDirc) fetchDataAndSplit(ctx context.Context) {
	var (
		err           error
		workerIndices []int
	)
	results := make([]*chunk.Chunk, len(e.workers))
	chk := newFirstChunk(e.dataSource)

	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleInterDirc(e.outputCh, r)
		}
		for _, w := range e.workers {
			close(w.inputCh)
		}
	}()

	for {
		err = Next(ctx, e.dataSource, chk)
		if err != nil {
			e.outputCh <- &shuffleOutput{err: err}
			return
		}
		if chk.NumEvents() == 0 {
			break
		}

		workerIndices, err = e.splitter.split(e.ctx, chk, workerIndices)
		if err != nil {
			e.outputCh <- &shuffleOutput{err: err}
			return
		}
		numEvents := chk.NumEvents()
		for i := 0; i < numEvents; i++ {
			workerIdx := workerIndices[i]
			w := e.workers[workerIdx]

			if results[workerIdx] == nil {
				select {
				case <-e.finishCh:
					return
				case results[workerIdx] = <-w.inputHolderCh:
					break
				}
			}
			results[workerIdx].AppendEvent(chk.GetEvent(i))
			if results[workerIdx].IsFull() {
				w.inputCh <- results[workerIdx]
				results[workerIdx] = nil
			}
		}
	}
	for i, w := range e.workers {
		if results[i] != nil {
			w.inputCh <- results[i]
			results[i] = nil
		}
	}
}

var _ InterlockingDirectorate = &shuffleWorker{}

// shuffleWorker is the multi-thread worker executing child interlocks within "partition".
type shuffleWorker struct {
	baseInterlockingDirectorate
	childInterDirc InterlockingDirectorate

	finishCh <-chan struct{}
	executed bool

	// Workers get inputs from dataFetcherThread by `inputCh`,
	//   and output results to main thread by `outputCh`.
	// `inputHolderCh` and `outputHolderCh` are "Chunk Holder" channels of `inputCh` and `outputCh` respectively,
	//   which give the `*Chunk` back, to implement the data transport in a streaming manner.
	inputCh        chan *chunk.Chunk
	inputHolderCh  chan *chunk.Chunk
	outputCh       chan *shuffleOutput
	outputHolderCh chan *chunk.Chunk
}

// Open implements the InterlockingDirectorate Open interface.
func (e *shuffleWorker) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	e.executed = false
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *shuffleWorker) Close() error {
	return errors.Trace(e.baseInterlockingDirectorate.Close())
}

// Next implements the InterlockingDirectorate Next interface.
// It is called by `Tail` interlock within "shuffle", to fetch data from `DataSource` by `inputCh`.
func (e *shuffleWorker) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.executed {
		return nil
	}
	select {
	case <-e.finishCh:
		e.executed = true
		return nil
	case result, ok := <-e.inputCh:
		if !ok || result.NumEvents() == 0 {
			e.executed = true
			return nil
		}
		req.SwapDeferredCausets(result)
		e.inputHolderCh <- result
		return nil
	}
}

func (e *shuffleWorker) run(ctx context.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			recoveryShuffleInterDirc(e.outputCh, r)
		}
		waitGroup.Done()
	}()

	for {
		select {
		case <-e.finishCh:
			return
		case chk := <-e.outputHolderCh:
			if err := Next(ctx, e.childInterDirc, chk); err != nil {
				e.outputCh <- &shuffleOutput{err: err}
				return
			}

			// Should not send an empty `chk` to `e.outputCh`.
			if chk.NumEvents() == 0 {
				return
			}
			e.outputCh <- &shuffleOutput{chk: chk, giveBackCh: e.outputHolderCh}
		}
	}
}

var _ partitionSplitter = &partitionHashSplitter{}

type partitionSplitter interface {
	split(ctx stochastikctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error)
}

type partitionHashSplitter struct {
	byItems    []memex.Expression
	numWorkers int
	hashKeys   [][]byte
}

func (s *partitionHashSplitter) split(ctx stochastikctx.Context, input *chunk.Chunk, workerIndices []int) ([]int, error) {
	var err error
	s.hashKeys, err = getGroupKey(ctx, input, s.hashKeys, s.byItems)
	if err != nil {
		return workerIndices, err
	}
	workerIndices = workerIndices[:0]
	numEvents := input.NumEvents()
	for i := 0; i < numEvents; i++ {
		workerIndices = append(workerIndices, int(murmur3.Sum32(s.hashKeys[i]))%s.numWorkers)
	}
	return workerIndices, nil
}
