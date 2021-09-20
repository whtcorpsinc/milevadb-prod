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
	"unsafe"

	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/allegrosql"
	"github.com/whtcorpsinc/milevadb/causet"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"go.uber.org/zap"
)

var (
	_ InterlockingDirectorate = &IndexMergeReaderInterlockingDirectorate{}
)

// IndexMergeReaderInterlockingDirectorate accesses a causet with multiple index/causet scan.
// There are three types of workers:
// 1. partialBlockWorker/partialIndexWorker, which are used to fetch the handles
// 2. indexMergeProcessWorker, which is used to do the `Union` operation.
// 3. indexMergeBlockScanWorker, which is used to get the causet tuples with the given handles.
//
// The execution flow is really like IndexLookUpReader. However, it uses multiple index scans
// or causet scans to get the handles:
// 1. use the partialBlockWorkers and partialIndexWorkers to fetch the handles (a batch per time)
//    and send them to the indexMergeProcessWorker.
// 2. indexMergeProcessWorker do the `Union` operation for a batch of handles it have got.
//    For every handle in the batch:
//    1. check whether it has been accessed.
//    2. if not, record it and send it to the indexMergeBlockScanWorker.
//    3. if accessed, just ignore it.
type IndexMergeReaderInterlockingDirectorate struct {
	baseInterlockingDirectorate

	causet           causet.Block
	indexes          []*perceptron.IndexInfo
	descs            []bool
	ranges           [][]*ranger.Range
	posetPosetDagPBs []*fidelpb.PosetDagRequest
	startTS          uint64
	blockRequest     *fidelpb.PosetDagRequest
	// defCausumns are only required by union scan.
	defCausumns       []*perceptron.DeferredCausetInfo
	partialStreamings []bool
	blockStreaming    bool
	*dataReaderBuilder
	// All fields above are immublock.

	tblWorkerWg    sync.WaitGroup
	processWokerWg sync.WaitGroup
	finished       chan struct{}

	workerStarted bool
	keyRanges     [][]ekv.KeyRange

	resultCh   chan *lookupBlockTask
	resultCurr *lookupBlockTask
	feedbacks  []*statistics.QueryFeedback

	// memTracker is used to track the memory usage of this interlock.
	memTracker *memory.Tracker

	// checHoTTexValue is used to check the consistency of the index data.
	*checHoTTexValue

	corDefCausInIdxSide bool
	partialCausets      [][]causetembedded.PhysicalCauset
	corDefCausInTblSide bool
	tblCausets          []causetembedded.PhysicalCauset
	corDefCausInAccess  bool
	idxDefCauss         [][]*memex.DeferredCauset
	defCausLens         [][]int

	handleDefCauss causetembedded.HandleDefCauss
}

// Open implements the InterlockingDirectorate Open interface
func (e *IndexMergeReaderInterlockingDirectorate) Open(ctx context.Context) error {
	e.keyRanges = make([][]ekv.KeyRange, 0, len(e.partialCausets))
	for i, plan := range e.partialCausets {
		_, ok := plan[0].(*causetembedded.PhysicalIndexScan)
		if !ok {
			if e.causet.Meta().IsCommonHandle {
				keyRanges, err := allegrosql.CommonHandleRangesToKVRanges(e.ctx.GetStochastikVars().StmtCtx, getPhysicalBlockID(e.causet), e.ranges[i])
				if err != nil {
					return err
				}
				e.keyRanges = append(e.keyRanges, keyRanges)
			} else {
				e.keyRanges = append(e.keyRanges, nil)
			}
			continue
		}
		keyRange, err := allegrosql.IndexRangesToKVRanges(e.ctx.GetStochastikVars().StmtCtx, getPhysicalBlockID(e.causet), e.indexes[i].ID, e.ranges[i], e.feedbacks[i])
		if err != nil {
			return err
		}
		e.keyRanges = append(e.keyRanges, keyRange)
	}
	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupBlockTask, atomic.LoadInt32(&LookupBlockTaskChannelSize))
	return nil
}

func (e *IndexMergeReaderInterlockingDirectorate) startWorkers(ctx context.Context) error {
	exitCh := make(chan struct{})
	workCh := make(chan *lookupBlockTask, 1)
	fetchCh := make(chan *lookupBlockTask, len(e.keyRanges))

	e.startIndexMergeProcessWorker(ctx, workCh, fetchCh)

	var err error
	var partialWorkerWg sync.WaitGroup
	for i := 0; i < len(e.keyRanges); i++ {
		partialWorkerWg.Add(1)
		if e.indexes[i] != nil {
			err = e.startPartialIndexWorker(ctx, exitCh, fetchCh, i, &partialWorkerWg, e.keyRanges[i])
		} else {
			err = e.startPartialBlockWorker(ctx, exitCh, fetchCh, i, &partialWorkerWg)
		}
		if err != nil {
			partialWorkerWg.Done()
			break
		}
	}
	go e.waitPartialWorkersAndCloseFetchChan(&partialWorkerWg, fetchCh)
	if err != nil {
		close(exitCh)
		return err
	}
	e.startIndexMergeBlockScanWorker(ctx, workCh)
	e.workerStarted = true
	return nil
}

func (e *IndexMergeReaderInterlockingDirectorate) waitPartialWorkersAndCloseFetchChan(partialWorkerWg *sync.WaitGroup, fetchCh chan *lookupBlockTask) {
	partialWorkerWg.Wait()
	close(fetchCh)
}

func (e *IndexMergeReaderInterlockingDirectorate) startIndexMergeProcessWorker(ctx context.Context, workCh chan<- *lookupBlockTask, fetch <-chan *lookupBlockTask) {
	idxMergeProcessWorker := &indexMergeProcessWorker{}
	e.processWokerWg.Add(1)
	go func() {
		defer trace.StartRegion(ctx, "IndexMergeProcessWorker").End()
		soliton.WithRecovery(
			func() {
				idxMergeProcessWorker.fetchLoop(ctx, fetch, workCh, e.resultCh, e.finished)
			},
			idxMergeProcessWorker.handleLoopFetcherPanic(ctx, e.resultCh),
		)
		e.processWokerWg.Done()
	}()
}

func (e *IndexMergeReaderInterlockingDirectorate) startPartialIndexWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *lookupBlockTask, workID int, partialWorkerWg *sync.WaitGroup, keyRange []ekv.KeyRange) error {
	if e.runtimeStats != nil {
		defCauslInterDirc := true
		e.posetPosetDagPBs[workID].DefCauslectInterDircutionSummaries = &defCauslInterDirc
	}

	var builder allegrosql.RequestBuilder
	ekvReq, err := builder.SetKeyRanges(keyRange).
		SetPosetDagRequest(e.posetPosetDagPBs[workID]).
		SetStartTS(e.startTS).
		SetDesc(e.descs[workID]).
		SetKeepOrder(false).
		SetStreaming(e.partialStreamings[workID]).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		SetMemTracker(e.memTracker).
		Build()
	if err != nil {
		return err
	}

	result, err := allegrosql.SelectWithRuntimeStats(ctx, e.ctx, ekvReq, e.handleDefCauss.GetFieldsTypes(), e.feedbacks[workID], getPhysicalCausetIDs(e.partialCausets[workID]), e.id)
	if err != nil {
		return err
	}

	result.Fetch(ctx)
	worker := &partialIndexWorker{
		sc:           e.ctx,
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetStochastikVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
	}

	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}

	failpoint.Inject("startPartialIndexWorkerErr", func() error {
		return errors.New("inject an error before start partialIndexWorker")
	})

	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialIndexWorker").End()
		defer partialWorkerWg.Done()
		ctx1, cancel := context.WithCancel(ctx)
		var err error
		soliton.WithRecovery(
			func() {
				_, err = worker.fetchHandles(ctx1, result, exitCh, fetchCh, e.resultCh, e.finished, e.handleDefCauss)
			},
			e.handleHandlesFetcherPanic(ctx, e.resultCh, "partialIndexWorker"),
		)
		if err != nil {
			e.feedbacks[workID].Invalidate()
		}
		cancel()
		if err := result.Close(); err != nil {
			logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
		}
		e.ctx.StoreQueryFeedback(e.feedbacks[workID])
	}()

	return nil
}

func (e *IndexMergeReaderInterlockingDirectorate) buildPartialBlockReader(ctx context.Context, workID int) InterlockingDirectorate {
	blockReaderInterDirc := &BlockReaderInterlockingDirectorate{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(e.ctx, e.schemaReplicant, 0),
		causet:                      e.causet,
		posetPosetDagPB:             e.posetPosetDagPBs[workID],
		startTS:                     e.startTS,
		streaming:                   e.partialStreamings[workID],
		feedback:                    statistics.NewQueryFeedback(0, nil, 0, false),
		plans:                       e.partialCausets[workID],
		ranges:                      e.ranges[workID],
	}
	return blockReaderInterDirc
}

func (e *IndexMergeReaderInterlockingDirectorate) startPartialBlockWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *lookupBlockTask, workID int,
	partialWorkerWg *sync.WaitGroup) error {
	partialBlockReader := e.buildPartialBlockReader(ctx, workID)
	err := partialBlockReader.Open(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("open Select result failed:", zap.Error(err))
		return err
	}
	blockInfo := e.partialCausets[workID][0].(*causetembedded.PhysicalBlockScan).Block
	worker := &partialBlockWorker{
		sc:           e.ctx,
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetStochastikVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
		blockReader:  partialBlockReader,
		blockInfo:    blockInfo,
	}

	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}
	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialBlockWorker").End()
		defer partialWorkerWg.Done()
		ctx1, cancel := context.WithCancel(ctx)
		var err error
		soliton.WithRecovery(
			func() {
				_, err = worker.fetchHandles(ctx1, exitCh, fetchCh, e.resultCh, e.finished, e.handleDefCauss)
			},
			e.handleHandlesFetcherPanic(ctx, e.resultCh, "partialBlockWorker"),
		)
		if err != nil {
			e.feedbacks[workID].Invalidate()
		}
		cancel()
		if err := worker.blockReader.Close(); err != nil {
			logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
		}
		e.ctx.StoreQueryFeedback(e.feedbacks[workID])
	}()
	return nil
}

type partialBlockWorker struct {
	sc           stochastikctx.Context
	batchSize    int
	maxBatchSize int
	maxChunkSize int
	blockReader  InterlockingDirectorate
	blockInfo    *perceptron.BlockInfo
}

func (w *partialBlockWorker) fetchHandles(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *lookupBlockTask, resultCh chan<- *lookupBlockTask,
	finished <-chan struct{}, handleDefCauss causetembedded.HandleDefCauss) (count int64, err error) {
	chk := chunk.NewChunkWithCapacity(retTypes(w.blockReader), w.maxChunkSize)
	for {
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, handleDefCauss)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- err
			resultCh <- &lookupBlockTask{
				doneCh: doneCh,
			}
			return count, err
		}
		if len(handles) == 0 {
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildBlockTask(handles, retChunk)
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
	}
}

func (w *partialBlockWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, handleDefCauss causetembedded.HandleDefCauss) (
	handles []ekv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]ekv.Handle, 0, w.batchSize)
	for len(handles) < w.batchSize {
		chk.SetRequiredEvents(w.batchSize-len(handles), w.maxChunkSize)
		err = errors.Trace(w.blockReader.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if chk.NumEvents() == 0 {
			return handles, retChk, nil
		}
		for i := 0; i < chk.NumEvents(); i++ {
			handle, err := handleDefCauss.BuildHandle(chk.GetEvent(i))
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialBlockWorker) buildBlockTask(handles []ekv.Handle, retChk *chunk.Chunk) *lookupBlockTask {
	task := &lookupBlockTask{
		handles:   handles,
		idxEvents: retChk,
	}

	task.doneCh = make(chan error, 1)
	return task
}

func (e *IndexMergeReaderInterlockingDirectorate) startIndexMergeBlockScanWorker(ctx context.Context, workCh <-chan *lookupBlockTask) {
	lookupConcurrencyLimit := e.ctx.GetStochastikVars().IndexLookupConcurrency()
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &indexMergeBlockScanWorker{
			workCh:         workCh,
			finished:       e.finished,
			buildTblReader: e.buildFinalBlockReader,
			tblCausets:     e.tblCausets,
			memTracker:     memory.NewTracker(memory.LabelForSimpleTask, -1),
		}
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			defer trace.StartRegion(ctx, "IndexMergeBlockScanWorker").End()
			var task *lookupBlockTask
			soliton.WithRecovery(
				func() { task = worker.pickAndInterDircTask(ctx1) },
				worker.handlePickAndInterDircTaskPanic(ctx1, task),
			)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexMergeReaderInterlockingDirectorate) buildFinalBlockReader(ctx context.Context, handles []ekv.Handle) (InterlockingDirectorate, error) {
	blockReaderInterDirc := &BlockReaderInterlockingDirectorate{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(e.ctx, e.schemaReplicant, 0),
		causet:                      e.causet,
		posetPosetDagPB:             e.blockRequest,
		startTS:                     e.startTS,
		streaming:                   e.blockStreaming,
		defCausumns:                 e.defCausumns,
		feedback:                    statistics.NewQueryFeedback(0, nil, 0, false),
		plans:                       e.tblCausets,
	}
	blockReaderInterDirc.buildVirtualDeferredCausetInfo()
	blockReader, err := e.dataReaderBuilder.buildBlockReaderFromHandles(ctx, blockReaderInterDirc, handles)
	if err != nil {
		logutil.Logger(ctx).Error("build causet reader from handles failed", zap.Error(err))
		return nil, err
	}
	return blockReader, nil
}

// Next implements InterlockingDirectorate Next interface.
func (e *IndexMergeReaderInterlockingDirectorate) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.workerStarted {
		if err := e.startWorkers(ctx); err != nil {
			return err
		}
	}

	req.Reset()
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return errors.Trace(err)
		}
		if resultTask == nil {
			return nil
		}
		for resultTask.cursor < len(resultTask.rows) {
			req.AppendEvent(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if req.NumEvents() >= e.maxChunkSize {
				return nil
			}
		}
	}
}

func (e *IndexMergeReaderInterlockingDirectorate) getResultTask() (*lookupBlockTask, error) {
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}
	if err := <-task.doneCh; err != nil {
		return nil, errors.Trace(err)
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

func (e *IndexMergeReaderInterlockingDirectorate) handleHandlesFetcherPanic(ctx context.Context, resultCh chan<- *lookupBlockTask, worker string) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}

		err4Panic := errors.Errorf("panic in IndexMergeReaderInterlockingDirectorate %s: %v", worker, r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		doneCh := make(chan error, 1)
		doneCh <- err4Panic
		resultCh <- &lookupBlockTask{
			doneCh: doneCh,
		}
	}
}

// Close implements InterDirc Close interface.
func (e *IndexMergeReaderInterlockingDirectorate) Close() error {
	if e.finished == nil {
		return nil
	}
	close(e.finished)
	e.processWokerWg.Wait()
	e.tblWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
	// TODO: how to causetstore e.feedbacks
	return nil
}

type indexMergeProcessWorker struct {
}

func (w *indexMergeProcessWorker) fetchLoop(ctx context.Context, fetchCh <-chan *lookupBlockTask,
	workCh chan<- *lookupBlockTask, resultCh chan<- *lookupBlockTask, finished <-chan struct{}) {
	defer func() {
		close(workCh)
		close(resultCh)
	}()

	distinctHandles := ekv.NewHandleMap()

	for task := range fetchCh {
		handles := task.handles
		fhs := make([]ekv.Handle, 0, 8)
		for _, h := range handles {
			if _, ok := distinctHandles.Get(h); !ok {
				fhs = append(fhs, h)
				distinctHandles.Set(h, true)
			}
		}
		if len(fhs) == 0 {
			continue
		}
		task := &lookupBlockTask{
			handles: fhs,
			doneCh:  make(chan error, 1),
		}
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case workCh <- task:
			resultCh <- task
		}
	}
}

func (w *indexMergeProcessWorker) handleLoopFetcherPanic(ctx context.Context, resultCh chan<- *lookupBlockTask) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}

		err4Panic := errors.Errorf("panic in IndexMergeReaderInterlockingDirectorate indexMergeBlockWorker: %v", r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		doneCh := make(chan error, 1)
		doneCh <- err4Panic
		resultCh <- &lookupBlockTask{
			doneCh: doneCh,
		}
	}
}

type partialIndexWorker struct {
	sc           stochastikctx.Context
	batchSize    int
	maxBatchSize int
	maxChunkSize int
}

func (w *partialIndexWorker) fetchHandles(
	ctx context.Context,
	result allegrosql.SelectResult,
	exitCh <-chan struct{},
	fetchCh chan<- *lookupBlockTask,
	resultCh chan<- *lookupBlockTask,
	finished <-chan struct{},
	handleDefCauss causetembedded.HandleDefCauss) (count int64, err error) {
	chk := chunk.NewChunkWithCapacity(handleDefCauss.GetFieldsTypes(), w.maxChunkSize)
	for {
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, result, handleDefCauss)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- err
			resultCh <- &lookupBlockTask{
				doneCh: doneCh,
			}
			return count, err
		}
		if len(handles) == 0 {
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildBlockTask(handles, retChunk)
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
	}
}

func (w *partialIndexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult allegrosql.SelectResult, handleDefCauss causetembedded.HandleDefCauss) (
	handles []ekv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]ekv.Handle, 0, w.batchSize)
	for len(handles) < w.batchSize {
		chk.SetRequiredEvents(w.batchSize-len(handles), w.maxChunkSize)
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if chk.NumEvents() == 0 {
			return handles, retChk, nil
		}
		for i := 0; i < chk.NumEvents(); i++ {
			handle, err := handleDefCauss.BuildHandleFromIndexEvent(chk.GetEvent(i))
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialIndexWorker) buildBlockTask(handles []ekv.Handle, retChk *chunk.Chunk) *lookupBlockTask {
	task := &lookupBlockTask{
		handles:   handles,
		idxEvents: retChk,
	}

	task.doneCh = make(chan error, 1)
	return task
}

type indexMergeBlockScanWorker struct {
	workCh         <-chan *lookupBlockTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []ekv.Handle) (InterlockingDirectorate, error)
	tblCausets     []causetembedded.PhysicalCauset

	// memTracker is used to track the memory usage of this interlock.
	memTracker *memory.Tracker
}

func (w *indexMergeBlockScanWorker) pickAndInterDircTask(ctx context.Context) (task *lookupBlockTask) {
	var ok bool
	for {
		select {
		case task, ok = <-w.workCh:
			if !ok {
				return
			}
		case <-w.finished:
			return
		}
		err := w.executeTask(ctx, task)
		task.doneCh <- err
	}
}

func (w *indexMergeBlockScanWorker) handlePickAndInterDircTaskPanic(ctx context.Context, task *lookupBlockTask) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}

		err4Panic := errors.Errorf("panic in IndexMergeReaderInterlockingDirectorate indexMergeBlockWorker: %v", r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		task.doneCh <- err4Panic
	}
}

func (w *indexMergeBlockScanWorker) executeTask(ctx context.Context, task *lookupBlockTask) error {
	blockReader, err := w.buildTblReader(ctx, task.handles)
	if err != nil {
		logutil.Logger(ctx).Error("build causet reader failed", zap.Error(err))
		return err
	}
	defer terror.Call(blockReader.Close)
	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Event, 0, handleCnt)
	for {
		chk := newFirstChunk(blockReader)
		err = Next(ctx, blockReader, chk)
		if err != nil {
			logutil.Logger(ctx).Error("causet reader fetch next chunk failed", zap.Error(err))
			return err
		}
		if chk.NumEvents() == 0 {
			break
		}
		memUsage = chk.MemoryUsage()
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			task.rows = append(task.rows, event)
		}
	}

	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Event{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if handleCnt != len(task.rows) && len(w.tblCausets) == 1 {
		return errors.Errorf("handle count %d isn't equal to value count %d", handleCnt, len(task.rows))
	}
	return nil
}
