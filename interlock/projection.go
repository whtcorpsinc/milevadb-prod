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
	"fmt"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"go.uber.org/zap"
)

// This file contains the implementation of the physical Projection Operator:
// https://en.wikipedia.org/wiki/Projection_(relational_algebra)
//
// NOTE:
// 1. The number of "projectionWorker" is controlled by the global stochastik
//    variable "milevadb_projection_concurrency".
// 2. Unparallel version is used when one of the following situations occurs:
//    a. "milevadb_projection_concurrency" is set to 0.
//    b. The estimated input size is smaller than "milevadb_max_chunk_size".
//    c. This projection can not be executed vectorially.

type projectionInput struct {
	chk          *chunk.Chunk
	targetWorker *projectionWorker
}

type projectionOutput struct {
	chk  *chunk.Chunk
	done chan error
}

// ProjectionInterDirc implements the physical Projection Operator:
// https://en.wikipedia.org/wiki/Projection_(relational_algebra)
type ProjectionInterDirc struct {
	baseInterlockingDirectorate

	evaluatorSuit *memex.EvaluatorSuite

	finishCh    chan struct{}
	outputCh    chan *projectionOutput
	fetcher     projectionInputFetcher
	numWorkers  int64
	workers     []*projectionWorker
	childResult *chunk.Chunk

	// parentReqEvents indicates how many rows the parent interlock is
	// requiring. It is set when parallelInterDircute() is called and used by the
	// concurrent projectionInputFetcher.
	//
	// NOTE: It should be protected by atomic operations.
	parentReqEvents int64

	memTracker *memory.Tracker
	wg         sync.WaitGroup

	calculateNoDelay bool
	prepared         bool
}

// Open implements the InterlockingDirectorate Open interface.
func (e *ProjectionInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	return e.open(ctx)
}

func (e *ProjectionInterDirc) open(ctx context.Context) error {
	e.prepared = false
	e.parentReqEvents = int64(e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	// For now a Projection can not be executed vectorially only because it
	// contains "SetVar" or "GetVar" functions, in this scenario this
	// Projection can not be executed parallelly.
	if e.numWorkers > 0 && !e.evaluatorSuit.Vectorizable() {
		e.numWorkers = 0
	}

	if e.isUnparallelInterDirc() {
		e.childResult = newFirstChunk(e.children[0])
		e.memTracker.Consume(e.childResult.MemoryUsage())
	}

	return nil
}

// Next implements the InterlockingDirectorate Next interface.
//
// Here we explain the execution flow of the parallel projection implementation.
// There are 3 main components:
//   1. "projectionInputFetcher": Fetch input "Chunk" from child.
//   2. "projectionWorker":       Do the projection work.
//   3. "ProjectionInterDirc.Next":    Return result to parent.
//
// 1. "projectionInputFetcher" gets its input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it fetches child's result into "input.chk" and:
//   a. Dispatches this input to the worker specified in "input.targetWorker"
//   b. Dispatches this output to the main thread: "ProjectionInterDirc.Next"
//   c. Dispatches this output to the worker specified in "input.targetWorker"
// It is finished and exited once:
//   a. There is no more input from child.
//   b. "ProjectionInterDirc" close the "globalFinishCh"
//
// 2. "projectionWorker" gets its input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it calculates the projection result use "input.chk" as the input
// and "output.chk" as the output, once the calculation is done, it:
//   a. Sends "nil" or error to "output.done" to mark this input is finished.
//   b. Returns the "input" resource to "projectionInputFetcher.inputCh"
// They are finished and exited once:
//   a. "ProjectionInterDirc" closes the "globalFinishCh"
//
// 3. "ProjectionInterDirc.Next" gets its output resources from its "outputCh" channel.
// After receiving an output from "outputCh", it should wait to receive a "nil"
// or error from "output.done" channel. Once a "nil" or error is received:
//   a. Returns this output to its parent
//   b. Returns the "output" resource to "projectionInputFetcher.outputCh"
//
//  +-----------+----------------------+--------------------------+
//  |           |                      |                          |
//  |  +--------+---------+   +--------+---------+       +--------+---------+
//  |  | projectionWorker |   + projectionWorker |  ...  + projectionWorker |
//  |  +------------------+   +------------------+       +------------------+
//  |       ^       ^              ^       ^                  ^       ^
//  |       |       |              |       |                  |       |
//  |    inputCh outputCh       inputCh outputCh           inputCh outputCh
//  |       ^       ^              ^       ^                  ^       ^
//  |       |       |              |       |                  |       |
//  |                              |       |
//  |                              |       +----------------->outputCh
//  |                              |       |                      |
//  |                              |       |                      v
//  |                      +-------+-------+--------+   +---------------------+
//  |                      | projectionInputFetcher |   | ProjectionInterDirc.Next |
//  |                      +------------------------+   +---------+-----------+
//  |                              ^       ^                      |
//  |                              |       |                      |
//  |                           inputCh outputCh                  |
//  |                              ^       ^                      |
//  |                              |       |                      |
//  +------------------------------+       +----------------------+
//
func (e *ProjectionInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.isUnparallelInterDirc() {
		return e.unParallelInterDircute(ctx, req)
	}
	return e.parallelInterDircute(ctx, req)

}

func (e *ProjectionInterDirc) isUnparallelInterDirc() bool {
	return e.numWorkers <= 0
}

func (e *ProjectionInterDirc) unParallelInterDircute(ctx context.Context, chk *chunk.Chunk) error {
	// transmit the requiredEvents
	e.childResult.SetRequiredEvents(chk.RequiredEvents(), e.maxChunkSize)
	mSize := e.childResult.MemoryUsage()
	err := Next(ctx, e.children[0], e.childResult)
	e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
	if err != nil {
		return err
	}
	if e.childResult.NumEvents() == 0 {
		return nil
	}
	err = e.evaluatorSuit.Run(e.ctx, e.childResult, chk)
	return err
}

func (e *ProjectionInterDirc) parallelInterDircute(ctx context.Context, chk *chunk.Chunk) error {
	atomic.StoreInt64(&e.parentReqEvents, int64(chk.RequiredEvents()))
	if !e.prepared {
		e.prepare(ctx)
		e.prepared = true
	}

	output, ok := <-e.outputCh
	if !ok {
		return nil
	}

	err := <-output.done
	if err != nil {
		return err
	}
	mSize := output.chk.MemoryUsage()
	chk.SwapDeferredCausets(output.chk)
	e.memTracker.Consume(output.chk.MemoryUsage() - mSize)
	e.fetcher.outputCh <- output
	return nil
}

func (e *ProjectionInterDirc) prepare(ctx context.Context) {
	e.finishCh = make(chan struct{})
	e.outputCh = make(chan *projectionOutput, e.numWorkers)

	// Initialize projectionInputFetcher.
	e.fetcher = projectionInputFetcher{
		proj:           e,
		child:          e.children[0],
		globalFinishCh: e.finishCh,
		globalOutputCh: e.outputCh,
		inputCh:        make(chan *projectionInput, e.numWorkers),
		outputCh:       make(chan *projectionOutput, e.numWorkers),
	}

	// Initialize projectionWorker.
	e.workers = make([]*projectionWorker, 0, e.numWorkers)
	for i := int64(0); i < e.numWorkers; i++ {
		e.workers = append(e.workers, &projectionWorker{
			proj:            e,
			sctx:            e.ctx,
			evaluatorSuit:   e.evaluatorSuit,
			globalFinishCh:  e.finishCh,
			inputGiveBackCh: e.fetcher.inputCh,
			inputCh:         make(chan *projectionInput, 1),
			outputCh:        make(chan *projectionOutput, 1),
		})

		inputChk := newFirstChunk(e.children[0])
		e.memTracker.Consume(inputChk.MemoryUsage())
		e.fetcher.inputCh <- &projectionInput{
			chk:          inputChk,
			targetWorker: e.workers[i],
		}

		outputChk := newFirstChunk(e)
		e.memTracker.Consume(outputChk.MemoryUsage())
		e.fetcher.outputCh <- &projectionOutput{
			chk:  outputChk,
			done: make(chan error, 1),
		}
	}

	e.wg.Add(1)
	go e.fetcher.run(ctx)

	for i := range e.workers {
		e.wg.Add(1)
		go e.workers[i].run(ctx)
	}
}

func (e *ProjectionInterDirc) drainInputCh(ch chan *projectionInput) {
	close(ch)
	for item := range ch {
		if item.chk != nil {
			e.memTracker.Consume(-item.chk.MemoryUsage())
		}
	}
}

func (e *ProjectionInterDirc) drainOutputCh(ch chan *projectionOutput) {
	close(ch)
	for item := range ch {
		if item.chk != nil {
			e.memTracker.Consume(-item.chk.MemoryUsage())
		}
	}
}

// Close implements the InterlockingDirectorate Close interface.
func (e *ProjectionInterDirc) Close() error {
	if e.isUnparallelInterDirc() {
		e.memTracker.Consume(-e.childResult.MemoryUsage())
		e.childResult = nil
	}
	if e.prepared {
		close(e.finishCh)
		e.wg.Wait() // Wait for fetcher and workers to finish and exit.

		// clear fetcher
		e.drainInputCh(e.fetcher.inputCh)
		e.drainOutputCh(e.fetcher.outputCh)

		// clear workers
		for _, w := range e.workers {
			e.drainInputCh(w.inputCh)
			e.drainOutputCh(w.outputCh)
		}
	}
	if e.baseInterlockingDirectorate.runtimeStats != nil {
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{}
		if e.isUnparallelInterDirc() {
			runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", 0))
		} else {
			runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", int(e.numWorkers)))
		}
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, runtimeStats)
	}
	return e.baseInterlockingDirectorate.Close()
}

type projectionInputFetcher struct {
	proj           *ProjectionInterDirc
	child          InterlockingDirectorate
	globalFinishCh <-chan struct{}
	globalOutputCh chan<- *projectionOutput
	wg             sync.WaitGroup

	inputCh  chan *projectionInput
	outputCh chan *projectionOutput
}

// run gets projectionInputFetcher's input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it fetches child's result into "input.chk" and:
//   a. Dispatches this input to the worker specified in "input.targetWorker"
//   b. Dispatches this output to the main thread: "ProjectionInterDirc.Next"
//   c. Dispatches this output to the worker specified in "input.targetWorker"
//
// It is finished and exited once:
//   a. There is no more input from child.
//   b. "ProjectionInterDirc" close the "globalFinishCh"
func (f *projectionInputFetcher) run(ctx context.Context) {
	defer trace.StartRegion(ctx, "ProjectionFetcher").End()
	var output *projectionOutput
	defer func() {
		if r := recover(); r != nil {
			recoveryProjection(output, r)
		}
		close(f.globalOutputCh)
		f.proj.wg.Done()
	}()

	for {
		input := readProjectionInput(f.inputCh, f.globalFinishCh)
		if input == nil {
			return
		}
		targetWorker := input.targetWorker

		output = readProjectionOutput(f.outputCh, f.globalFinishCh)
		if output == nil {
			f.proj.memTracker.Consume(-input.chk.MemoryUsage())
			return
		}

		f.globalOutputCh <- output

		requiredEvents := atomic.LoadInt64(&f.proj.parentReqEvents)
		input.chk.SetRequiredEvents(int(requiredEvents), f.proj.maxChunkSize)
		mSize := input.chk.MemoryUsage()
		err := Next(ctx, f.child, input.chk)
		f.proj.memTracker.Consume(input.chk.MemoryUsage() - mSize)
		if err != nil || input.chk.NumEvents() == 0 {
			output.done <- err
			f.proj.memTracker.Consume(-input.chk.MemoryUsage())
			return
		}

		targetWorker.inputCh <- input
		targetWorker.outputCh <- output
	}
}

type projectionWorker struct {
	proj            *ProjectionInterDirc
	sctx            stochastikctx.Context
	evaluatorSuit   *memex.EvaluatorSuite
	globalFinishCh  <-chan struct{}
	inputGiveBackCh chan<- *projectionInput

	// channel "input" and "output" is :
	// a. initialized by "ProjectionInterDirc.prepare"
	// b. written	  by "projectionInputFetcher.run"
	// c. read    	  by "projectionWorker.run"
	inputCh  chan *projectionInput
	outputCh chan *projectionOutput
}

// run gets projectionWorker's input and output resources from its
// "inputCh" and "outputCh" channel, once the input and output resources are
// abtained, it calculate the projection result use "input.chk" as the input
// and "output.chk" as the output, once the calculation is done, it:
//   a. Sends "nil" or error to "output.done" to mark this input is finished.
//   b. Returns the "input" resource to "projectionInputFetcher.inputCh".
//
// It is finished and exited once:
//   a. "ProjectionInterDirc" closes the "globalFinishCh".
func (w *projectionWorker) run(ctx context.Context) {
	defer trace.StartRegion(ctx, "ProjectionWorker").End()
	var output *projectionOutput
	defer func() {
		if r := recover(); r != nil {
			recoveryProjection(output, r)
		}
		w.proj.wg.Done()
	}()
	for {
		input := readProjectionInput(w.inputCh, w.globalFinishCh)
		if input == nil {
			return
		}

		output = readProjectionOutput(w.outputCh, w.globalFinishCh)
		if output == nil {
			return
		}

		mSize := output.chk.MemoryUsage() + input.chk.MemoryUsage()
		err := w.evaluatorSuit.Run(w.sctx, input.chk, output.chk)
		w.proj.memTracker.Consume(output.chk.MemoryUsage() + input.chk.MemoryUsage() - mSize)
		output.done <- err

		if err != nil {
			return
		}

		w.inputGiveBackCh <- input
	}
}

func recoveryProjection(output *projectionOutput, r interface{}) {
	if output != nil {
		output.done <- errors.Errorf("%v", r)
	}
	buf := soliton.GetStack()
	logutil.BgLogger().Error("projection interlock panicked", zap.String("error", fmt.Sprintf("%v", r)), zap.String("stack", string(buf)))
}

func readProjectionInput(inputCh <-chan *projectionInput, finishCh <-chan struct{}) *projectionInput {
	select {
	case <-finishCh:
		return nil
	case input, ok := <-inputCh:
		if !ok {
			return nil
		}
		return input
	}
}

func readProjectionOutput(outputCh <-chan *projectionOutput, finishCh <-chan struct{}) *projectionOutput {
	select {
	case <-finishCh:
		return nil
	case output, ok := <-outputCh:
		if !ok {
			return nil
		}
		return output
	}
}
