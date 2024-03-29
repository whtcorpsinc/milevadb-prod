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
	"sync"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/interlock/aggfuncs"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

type aggPartialResultMapper map[string][]aggfuncs.PartialResult

// baseHashAggWorker stores the common attributes of HashAggFinalWorker and HashAggPartialWorker.
type baseHashAggWorker struct {
	ctx          stochastikctx.Context
	finishCh     <-chan struct{}
	aggFuncs     []aggfuncs.AggFunc
	maxChunkSize int
}

func newBaseHashAggWorker(ctx stochastikctx.Context, finishCh <-chan struct{}, aggFuncs []aggfuncs.AggFunc, maxChunkSize int) baseHashAggWorker {
	return baseHashAggWorker{
		ctx:          ctx,
		finishCh:     finishCh,
		aggFuncs:     aggFuncs,
		maxChunkSize: maxChunkSize,
	}
}

// HashAggPartialWorker indicates the partial workers of parallel hash agg execution,
// the number of the worker can be set by `milevadb_hashagg_partial_concurrency`.
type HashAggPartialWorker struct {
	baseHashAggWorker

	inputCh           chan *chunk.Chunk
	outputChs         []chan *HashAggIntermData
	globalOutputCh    chan *AfFinalResult
	giveBackCh        chan<- *HashAggInput
	partialResultsMap aggPartialResultMapper
	groupByItems      []memex.Expression
	groupKey          [][]byte
	// chk stores the input data from child,
	// and is reused by childInterDirc and partial worker.
	chk        *chunk.Chunk
	memTracker *memory.Tracker
}

// HashAggFinalWorker indicates the final workers of parallel hash agg execution,
// the number of the worker can be set by `milevadb_hashagg_final_concurrency`.
type HashAggFinalWorker struct {
	baseHashAggWorker

	rowBuffer           []types.Causet
	mublockEvent          chunk.MutEvent
	partialResultMap    aggPartialResultMapper
	groupSet            set.StringSet
	inputCh             chan *HashAggIntermData
	outputCh            chan *AfFinalResult
	finalResultHolderCh chan *chunk.Chunk
	groupKeys           [][]byte
}

// AfFinalResult indicates aggregation functions final result.
type AfFinalResult struct {
	chk        *chunk.Chunk
	err        error
	giveBackCh chan *chunk.Chunk
}

// HashAggInterDirc deals with all the aggregate functions.
// It is built from the Aggregate Causet. When Next() is called, it reads all the data from Src
// and uFIDelates all the items in PartialAggFuncs.
// The parallel execution flow is as the following graph shows:
//
//                            +-------------+
//                            | Main Thread |
//                            +------+------+
//                                   ^
//                                   |
//                                   +
//                              +-+-            +-+
//                              | |    ......   | |  finalOutputCh
//                              +++-            +-+
//                               ^
//                               |
//                               +---------------+
//                               |               |
//                 +--------------+             +--------------+
//                 | final worker |     ......  | final worker |
//                 +------------+-+             +-+------------+
//                              ^                 ^
//                              |                 |
//                             +-+  +-+  ......  +-+
//                             | |  | |          | |
//                             ...  ...          ...    partialOutputChs
//                             | |  | |          | |
//                             +++  +++          +++
//                              ^    ^            ^
//          +-+                 |    |            |
//          | |        +--------o----+            |
// inputCh  +-+        |        +-----------------+---+
//          | |        |                              |
//          ...    +---+------------+            +----+-----------+
//          | |    | partial worker |   ......   | partial worker |
//          +++    +--------------+-+            +-+--------------+
//           |                     ^                ^
//           |                     |                |
//      +----v---------+          +++ +-+          +++
//      | data fetcher | +------> | | | |  ......  | |   partialInputChs
//      +--------------+          +-+ +-+          +-+
type HashAggInterDirc struct {
	baseInterlockingDirectorate

	sc               *stmtctx.StatementContext
	PartialAggFuncs  []aggfuncs.AggFunc
	FinalAggFuncs    []aggfuncs.AggFunc
	partialResultMap aggPartialResultMapper
	groupSet         set.StringSet
	groupKeys        []string
	cursor4GroupKey  int
	GroupByItems     []memex.Expression
	groupKeyBuffer   [][]byte

	finishCh         chan struct{}
	finalOutputCh    chan *AfFinalResult
	partialOutputChs []chan *HashAggIntermData
	inputCh          chan *HashAggInput
	partialInputChs  []chan *chunk.Chunk
	partialWorkers   []HashAggPartialWorker
	finalWorkers     []HashAggFinalWorker
	defaultVal       *chunk.Chunk
	childResult      *chunk.Chunk

	// isChildReturnEmpty indicates whether the child interlock only returns an empty input.
	isChildReturnEmpty bool
	// After we support parallel execution for aggregation functions with distinct,
	// we can remove this attribute.
	isUnparallelInterDirc bool
	prepared         bool
	executed         bool

	memTracker *memory.Tracker // track memory usage.
}

// HashAggInput indicates the input of hash agg exec.
type HashAggInput struct {
	chk *chunk.Chunk
	// giveBackCh is bound with specific partial worker,
	// it's used to reuse the `chk`,
	// and tell the data-fetcher which partial worker it should send data to.
	giveBackCh chan<- *chunk.Chunk
}

// HashAggIntermData indicates the intermediate data of aggregation execution.
type HashAggIntermData struct {
	groupKeys        []string
	cursor           int
	partialResultMap aggPartialResultMapper
}

// getPartialResultBatch fetches a batch of partial results from HashAggIntermData.
func (d *HashAggIntermData) getPartialResultBatch(sc *stmtctx.StatementContext, prs [][]aggfuncs.PartialResult, aggFuncs []aggfuncs.AggFunc, maxChunkSize int) (_ [][]aggfuncs.PartialResult, groupKeys []string, reachEnd bool) {
	keyStart := d.cursor
	for ; d.cursor < len(d.groupKeys) && len(prs) < maxChunkSize; d.cursor++ {
		prs = append(prs, d.partialResultMap[d.groupKeys[d.cursor]])
	}
	if d.cursor == len(d.groupKeys) {
		reachEnd = true
	}
	return prs, d.groupKeys[keyStart:d.cursor], reachEnd
}

// Close implements the InterlockingDirectorate Close interface.
func (e *HashAggInterDirc) Close() error {
	if e.isUnparallelInterDirc {
		e.memTracker.Consume(-e.childResult.MemoryUsage())
		e.childResult = nil
		e.groupSet = nil
		e.partialResultMap = nil
		return e.baseInterlockingDirectorate.Close()
	}
	// `Close` may be called after `Open` without calling `Next` in test.
	if !e.prepared {
		close(e.inputCh)
		for _, ch := range e.partialOutputChs {
			close(ch)
		}
		for _, ch := range e.partialInputChs {
			close(ch)
		}
		close(e.finalOutputCh)
	}
	close(e.finishCh)
	for _, ch := range e.partialOutputChs {
		for range ch {
		}
	}
	for _, ch := range e.partialInputChs {
		for chk := range ch {
			e.memTracker.Consume(-chk.MemoryUsage())
		}
	}
	for range e.finalOutputCh {
	}
	e.executed = false

	if e.runtimeStats != nil {
		var partialConcurrency, finalConcurrency int
		if e.isUnparallelInterDirc {
			partialConcurrency = 0
			finalConcurrency = 0
		} else {
			partialConcurrency = cap(e.partialWorkers)
			finalConcurrency = cap(e.finalWorkers)
		}
		partialConcurrencyInfo := execdetails.NewConcurrencyInfo("PartialConcurrency", partialConcurrency)
		finalConcurrencyInfo := execdetails.NewConcurrencyInfo("FinalConcurrency", finalConcurrency)
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{}
		runtimeStats.SetConcurrencyInfo(partialConcurrencyInfo, finalConcurrencyInfo)
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(e.id, runtimeStats)
	}
	return e.baseInterlockingDirectorate.Close()
}

// Open implements the InterlockingDirectorate Open interface.
func (e *HashAggInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	e.prepared = false

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	if e.isUnparallelInterDirc {
		e.initForUnparallelInterDirc()
		return nil
	}
	e.initForParallelInterDirc(e.ctx)
	return nil
}

func (e *HashAggInterDirc) initForUnparallelInterDirc() {
	e.groupSet = set.NewStringSet()
	e.partialResultMap = make(aggPartialResultMapper)
	e.groupKeyBuffer = make([][]byte, 0, 8)
	e.childResult = newFirstChunk(e.children[0])
	e.memTracker.Consume(e.childResult.MemoryUsage())
}

func (e *HashAggInterDirc) initForParallelInterDirc(ctx stochastikctx.Context) {
	stochastikVars := e.ctx.GetStochastikVars()
	finalConcurrency := stochastikVars.HashAggFinalConcurrency()
	partialConcurrency := stochastikVars.HashAggPartialConcurrency()
	e.isChildReturnEmpty = true
	e.finalOutputCh = make(chan *AfFinalResult, finalConcurrency)
	e.inputCh = make(chan *HashAggInput, partialConcurrency)
	e.finishCh = make(chan struct{}, 1)

	e.partialInputChs = make([]chan *chunk.Chunk, partialConcurrency)
	for i := range e.partialInputChs {
		e.partialInputChs[i] = make(chan *chunk.Chunk, 1)
	}
	e.partialOutputChs = make([]chan *HashAggIntermData, finalConcurrency)
	for i := range e.partialOutputChs {
		e.partialOutputChs[i] = make(chan *HashAggIntermData, partialConcurrency)
	}

	e.partialWorkers = make([]HashAggPartialWorker, partialConcurrency)
	e.finalWorkers = make([]HashAggFinalWorker, finalConcurrency)

	// Init partial workers.
	for i := 0; i < partialConcurrency; i++ {
		w := HashAggPartialWorker{
			baseHashAggWorker: newBaseHashAggWorker(e.ctx, e.finishCh, e.PartialAggFuncs, e.maxChunkSize),
			inputCh:           e.partialInputChs[i],
			outputChs:         e.partialOutputChs,
			giveBackCh:        e.inputCh,
			globalOutputCh:    e.finalOutputCh,
			partialResultsMap: make(aggPartialResultMapper),
			groupByItems:      e.GroupByItems,
			chk:               newFirstChunk(e.children[0]),
			groupKey:          make([][]byte, 0, 8),
			memTracker:        e.memTracker,
		}
		e.memTracker.Consume(w.chk.MemoryUsage())
		e.partialWorkers[i] = w

		input := &HashAggInput{
			chk:        newFirstChunk(e.children[0]),
			giveBackCh: w.inputCh,
		}
		e.memTracker.Consume(input.chk.MemoryUsage())
		e.inputCh <- input
	}

	// Init final workers.
	for i := 0; i < finalConcurrency; i++ {
		e.finalWorkers[i] = HashAggFinalWorker{
			baseHashAggWorker:   newBaseHashAggWorker(e.ctx, e.finishCh, e.FinalAggFuncs, e.maxChunkSize),
			partialResultMap:    make(aggPartialResultMapper),
			groupSet:            set.NewStringSet(),
			inputCh:             e.partialOutputChs[i],
			outputCh:            e.finalOutputCh,
			finalResultHolderCh: make(chan *chunk.Chunk, 1),
			rowBuffer:           make([]types.Causet, 0, e.Schema().Len()),
			mublockEvent:          chunk.MutEventFromTypes(retTypes(e)),
			groupKeys:           make([][]byte, 0, 8),
		}
		e.finalWorkers[i].finalResultHolderCh <- newFirstChunk(e)
	}
}

func (w *HashAggPartialWorker) getChildInput() bool {
	select {
	case <-w.finishCh:
		return false
	case chk, ok := <-w.inputCh:
		if !ok {
			return false
		}
		w.chk.SwapDeferredCausets(chk)
		w.giveBackCh <- &HashAggInput{
			chk:        chk,
			giveBackCh: w.inputCh,
		}
	}
	return true
}

func recoveryHashAgg(output chan *AfFinalResult, r interface{}) {
	err := errors.Errorf("%v", r)
	output <- &AfFinalResult{err: errors.Errorf("%v", r)}
	logutil.BgLogger().Error("parallel hash aggregation panicked", zap.Error(err), zap.Stack("stack"))
}

func (w *HashAggPartialWorker) run(ctx stochastikctx.Context, waitGroup *sync.WaitGroup, finalConcurrency int) {
	needShuffle, sc := false, ctx.GetStochastikVars().StmtCtx
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.globalOutputCh, r)
		}
		if needShuffle {
			w.shuffleIntermData(sc, finalConcurrency)
		}
		w.memTracker.Consume(-w.chk.MemoryUsage())
		waitGroup.Done()
	}()
	for {
		if !w.getChildInput() {
			return
		}
		if err := w.uFIDelatePartialResult(ctx, sc, w.chk, len(w.partialResultsMap)); err != nil {
			w.globalOutputCh <- &AfFinalResult{err: err}
			return
		}
		// The intermData can be promised to be not empty if reaching here,
		// so we set needShuffle to be true.
		needShuffle = true
	}
}

func (w *HashAggPartialWorker) uFIDelatePartialResult(ctx stochastikctx.Context, sc *stmtctx.StatementContext, chk *chunk.Chunk, finalConcurrency int) (err error) {
	w.groupKey, err = getGroupKey(w.ctx, chk, w.groupKey, w.groupByItems)
	if err != nil {
		return err
	}

	partialResults := w.getPartialResult(sc, w.groupKey, w.partialResultsMap)
	numEvents := chk.NumEvents()
	rows := make([]chunk.Event, 1)
	for i := 0; i < numEvents; i++ {
		for j, af := range w.aggFuncs {
			rows[0] = chk.GetEvent(i)
			if _, err := af.UFIDelatePartialResult(ctx, rows, partialResults[i][j]); err != nil {
				return err
			}
		}
	}
	return nil
}

// shuffleIntermData shuffles the intermediate data of partial workers to corresponded final workers.
// We only support parallel execution for single-machine, so process of encode and decode can be skipped.
func (w *HashAggPartialWorker) shuffleIntermData(sc *stmtctx.StatementContext, finalConcurrency int) {
	groupKeysSlice := make([][]string, finalConcurrency)
	for groupKey := range w.partialResultsMap {
		finalWorkerIdx := int(murmur3.Sum32([]byte(groupKey))) % finalConcurrency
		if groupKeysSlice[finalWorkerIdx] == nil {
			groupKeysSlice[finalWorkerIdx] = make([]string, 0, len(w.partialResultsMap)/finalConcurrency)
		}
		groupKeysSlice[finalWorkerIdx] = append(groupKeysSlice[finalWorkerIdx], groupKey)
	}

	for i := range groupKeysSlice {
		if groupKeysSlice[i] == nil {
			continue
		}
		w.outputChs[i] <- &HashAggIntermData{
			groupKeys:        groupKeysSlice[i],
			partialResultMap: w.partialResultsMap,
		}
	}
}

// getGroupKey evaluates the group items and args of aggregate functions.
func getGroupKey(ctx stochastikctx.Context, input *chunk.Chunk, groupKey [][]byte, groupByItems []memex.Expression) ([][]byte, error) {
	numEvents := input.NumEvents()
	avlGroupKeyLen := mathutil.Min(len(groupKey), numEvents)
	for i := 0; i < avlGroupKeyLen; i++ {
		groupKey[i] = groupKey[i][:0]
	}
	for i := avlGroupKeyLen; i < numEvents; i++ {
		groupKey = append(groupKey, make([]byte, 0, 10*len(groupByItems)))
	}

	for _, item := range groupByItems {
		tp := item.GetType()
		buf, err := memex.GetDeferredCauset(tp.EvalType(), numEvents)
		if err != nil {
			return nil, err
		}

		if err := memex.EvalExpr(ctx, item, input, buf); err != nil {
			memex.PutDeferredCauset(buf)
			return nil, err
		}
		// This check is used to avoid error during the execution of `EncodeDecimal`.
		if item.GetType().Tp == allegrosql.TypeNewDecimal {
			newTp := *tp
			newTp.Flen = 0
			tp = &newTp
		}
		groupKey, err = codec.HashGroupKey(ctx.GetStochastikVars().StmtCtx, input.NumEvents(), buf, groupKey, tp)
		if err != nil {
			memex.PutDeferredCauset(buf)
			return nil, err
		}
		memex.PutDeferredCauset(buf)
	}
	return groupKey, nil
}

func (w baseHashAggWorker) getPartialResult(sc *stmtctx.StatementContext, groupKey [][]byte, mapper aggPartialResultMapper) [][]aggfuncs.PartialResult {
	n := len(groupKey)
	partialResults := make([][]aggfuncs.PartialResult, n)
	for i := 0; i < n; i++ {
		var ok bool
		if partialResults[i], ok = mapper[string(groupKey[i])]; ok {
			continue
		}
		for _, af := range w.aggFuncs {
			partialResult, _ := af.AllocPartialResult()
			partialResults[i] = append(partialResults[i], partialResult)
		}
		mapper[string(groupKey[i])] = partialResults[i]
	}
	return partialResults
}

func (w *HashAggFinalWorker) getPartialInput() (input *HashAggIntermData, ok bool) {
	select {
	case <-w.finishCh:
		return nil, false
	case input, ok = <-w.inputCh:
		if !ok {
			return nil, false
		}
	}
	return
}

func (w *HashAggFinalWorker) consumeIntermData(sctx stochastikctx.Context) (err error) {
	var (
		input            *HashAggIntermData
		ok               bool
		intermDataBuffer [][]aggfuncs.PartialResult
		groupKeys        []string
		sc               = sctx.GetStochastikVars().StmtCtx
	)
	for {
		if input, ok = w.getPartialInput(); !ok {
			return nil
		}
		if intermDataBuffer == nil {
			intermDataBuffer = make([][]aggfuncs.PartialResult, 0, w.maxChunkSize)
		}
		// Consume input in batches, size of every batch is less than w.maxChunkSize.
		for reachEnd := false; !reachEnd; {
			intermDataBuffer, groupKeys, reachEnd = input.getPartialResultBatch(sc, intermDataBuffer[:0], w.aggFuncs, w.maxChunkSize)
			groupKeysLen := len(groupKeys)
			w.groupKeys = w.groupKeys[:0]
			for i := 0; i < groupKeysLen; i++ {
				w.groupKeys = append(w.groupKeys, []byte(groupKeys[i]))
			}
			finalPartialResults := w.getPartialResult(sc, w.groupKeys, w.partialResultMap)
			for i, groupKey := range groupKeys {
				if !w.groupSet.Exist(groupKey) {
					w.groupSet.Insert(groupKey)
				}
				prs := intermDataBuffer[i]
				for j, af := range w.aggFuncs {
					if _, err = af.MergePartialResult(sctx, prs[j], finalPartialResults[i][j]); err != nil {
						return err
					}
				}
			}
		}
	}
}

func (w *HashAggFinalWorker) getFinalResult(sctx stochastikctx.Context) {
	result, finished := w.receiveFinalResultHolder()
	if finished {
		return
	}
	w.groupKeys = w.groupKeys[:0]
	for groupKey := range w.groupSet {
		w.groupKeys = append(w.groupKeys, []byte(groupKey))
	}
	partialResults := w.getPartialResult(sctx.GetStochastikVars().StmtCtx, w.groupKeys, w.partialResultMap)
	for i := 0; i < len(w.groupSet); i++ {
		for j, af := range w.aggFuncs {
			if err := af.AppendFinalResult2Chunk(sctx, partialResults[i][j], result); err != nil {
				logutil.BgLogger().Error("HashAggFinalWorker failed to append final result to Chunk", zap.Error(err))
			}
		}
		if len(w.aggFuncs) == 0 {
			result.SetNumVirtualEvents(result.NumEvents() + 1)
		}
		if result.IsFull() {
			w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
			result, finished = w.receiveFinalResultHolder()
			if finished {
				return
			}
		}
	}
	w.outputCh <- &AfFinalResult{chk: result, giveBackCh: w.finalResultHolderCh}
}

func (w *HashAggFinalWorker) receiveFinalResultHolder() (*chunk.Chunk, bool) {
	select {
	case <-w.finishCh:
		return nil, true
	case result, ok := <-w.finalResultHolderCh:
		return result, !ok
	}
}

func (w *HashAggFinalWorker) run(ctx stochastikctx.Context, waitGroup *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(w.outputCh, r)
		}
		waitGroup.Done()
	}()
	if err := w.consumeIntermData(ctx); err != nil {
		w.outputCh <- &AfFinalResult{err: err}
	}
	w.getFinalResult(ctx)
}

// Next implements the InterlockingDirectorate Next interface.
func (e *HashAggInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.isUnparallelInterDirc {
		return e.unparallelInterDirc(ctx, req)
	}
	return e.parallelInterDirc(ctx, req)
}

func (e *HashAggInterDirc) fetchChildData(ctx context.Context) {
	var (
		input *HashAggInput
		chk   *chunk.Chunk
		ok    bool
		err   error
	)
	defer func() {
		if r := recover(); r != nil {
			recoveryHashAgg(e.finalOutputCh, r)
		}
		for i := range e.partialInputChs {
			close(e.partialInputChs[i])
		}
	}()
	for {
		select {
		case <-e.finishCh:
			return
		case input, ok = <-e.inputCh:
			if !ok {
				return
			}
			chk = input.chk
		}
		mSize := chk.MemoryUsage()
		err = Next(ctx, e.children[0], chk)
		if err != nil {
			e.finalOutputCh <- &AfFinalResult{err: err}
			e.memTracker.Consume(-mSize)
			return
		}
		if chk.NumEvents() == 0 {
			e.memTracker.Consume(-mSize)
			return
		}
		e.memTracker.Consume(chk.MemoryUsage() - mSize)
		input.giveBackCh <- chk
	}
}

func (e *HashAggInterDirc) waitPartialWorkerAndCloseOutputChs(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.inputCh)
	for input := range e.inputCh {
		e.memTracker.Consume(-input.chk.MemoryUsage())
	}
	for _, ch := range e.partialOutputChs {
		close(ch)
	}
}

func (e *HashAggInterDirc) waitFinalWorkerAndCloseFinalOutput(waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	close(e.finalOutputCh)
}

func (e *HashAggInterDirc) prepare4ParallelInterDirc(ctx context.Context) {
	go e.fetchChildData(ctx)

	partialWorkerWaitGroup := &sync.WaitGroup{}
	partialWorkerWaitGroup.Add(len(e.partialWorkers))
	for i := range e.partialWorkers {
		go e.partialWorkers[i].run(e.ctx, partialWorkerWaitGroup, len(e.finalWorkers))
	}
	go e.waitPartialWorkerAndCloseOutputChs(partialWorkerWaitGroup)

	finalWorkerWaitGroup := &sync.WaitGroup{}
	finalWorkerWaitGroup.Add(len(e.finalWorkers))
	for i := range e.finalWorkers {
		go e.finalWorkers[i].run(e.ctx, finalWorkerWaitGroup)
	}
	go e.waitFinalWorkerAndCloseFinalOutput(finalWorkerWaitGroup)
}

// HashAggInterDirc employs one input reader, M partial workers and N final workers to execute parallelly.
// The parallel execution flow is:
// 1. input reader reads data from child interlock and send them to partial workers.
// 2. partial worker receives the input data, uFIDelates the partial results, and shuffle the partial results to the final workers.
// 3. final worker receives partial results from all the partial workers, evaluates the final results and sends the final results to the main thread.
func (e *HashAggInterDirc) parallelInterDirc(ctx context.Context, chk *chunk.Chunk) error {
	if !e.prepared {
		e.prepare4ParallelInterDirc(ctx)
		e.prepared = true
	}

	failpoint.Inject("parallelHashAggError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("HashAggInterDirc.parallelInterDirc error"))
		}
	})

	if e.executed {
		return nil
	}
	for {
		result, ok := <-e.finalOutputCh
		if !ok {
			e.executed = true
			if e.isChildReturnEmpty && e.defaultVal != nil {
				chk.Append(e.defaultVal, 0, 1)
			}
			return nil
		}
		if result.err != nil {
			return result.err
		}
		chk.SwapDeferredCausets(result.chk)
		result.chk.Reset()
		result.giveBackCh <- result.chk
		if chk.NumEvents() > 0 {
			e.isChildReturnEmpty = false
			return nil
		}
	}
}

// unparallelInterDirc executes hash aggregation algorithm in single thread.
func (e *HashAggInterDirc) unparallelInterDirc(ctx context.Context, chk *chunk.Chunk) error {
	// In this stage we consider all data from src as a single group.
	if !e.prepared {
		err := e.execute(ctx)
		if err != nil {
			return err
		}
		if (len(e.groupSet) == 0) && len(e.GroupByItems) == 0 {
			// If no groupby and no data, we should add an empty group.
			// For example:
			// "select count(c) from t;" should return one event [0]
			// "select count(c) from t group by c1;" should return empty result set.
			e.groupSet.Insert("")
			e.groupKeys = append(e.groupKeys, "")
		}
		e.prepared = true
	}
	chk.Reset()

	// Since we return e.maxChunkSize rows every time, so we should not traverse
	// `groupSet` because of its randomness.
	for ; e.cursor4GroupKey < len(e.groupKeys); e.cursor4GroupKey++ {
		partialResults := e.getPartialResults(e.groupKeys[e.cursor4GroupKey])
		if len(e.PartialAggFuncs) == 0 {
			chk.SetNumVirtualEvents(chk.NumEvents() + 1)
		}
		for i, af := range e.PartialAggFuncs {
			if err := af.AppendFinalResult2Chunk(e.ctx, partialResults[i], chk); err != nil {
				return err
			}
		}
		if chk.IsFull() {
			e.cursor4GroupKey++
			return nil
		}
	}
	return nil
}

// execute fetches Chunks from src and uFIDelate each aggregate function for each event in Chunk.
func (e *HashAggInterDirc) execute(ctx context.Context) (err error) {
	for {
		mSize := e.childResult.MemoryUsage()
		err := Next(ctx, e.children[0], e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}

		failpoint.Inject("unparallelHashAggError", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("HashAggInterDirc.unparallelInterDirc error"))
			}
		})

		// no more data.
		if e.childResult.NumEvents() == 0 {
			return nil
		}

		e.groupKeyBuffer, err = getGroupKey(e.ctx, e.childResult, e.groupKeyBuffer, e.GroupByItems)
		if err != nil {
			return err
		}

		for j := 0; j < e.childResult.NumEvents(); j++ {
			groupKey := string(e.groupKeyBuffer[j]) // do memory copy here, because e.groupKeyBuffer may be reused.
			if !e.groupSet.Exist(groupKey) {
				e.groupSet.Insert(groupKey)
				e.groupKeys = append(e.groupKeys, groupKey)
			}
			partialResults := e.getPartialResults(groupKey)
			for i, af := range e.PartialAggFuncs {
				memDelta, err := af.UFIDelatePartialResult(e.ctx, []chunk.Event{e.childResult.GetEvent(j)}, partialResults[i])
				if err != nil {
					return err
				}
				e.memTracker.Consume(memDelta)
			}
		}
	}
}

func (e *HashAggInterDirc) getPartialResults(groupKey string) []aggfuncs.PartialResult {
	partialResults, ok := e.partialResultMap[groupKey]
	if !ok {
		partialResults = make([]aggfuncs.PartialResult, 0, len(e.PartialAggFuncs))
		for _, af := range e.PartialAggFuncs {
			partialResult, memDelta := af.AllocPartialResult()
			partialResults = append(partialResults, partialResult)
			e.memTracker.Consume(memDelta)
		}
		e.partialResultMap[groupKey] = partialResults
	}
	return partialResults
}

// StreamAggInterDirc deals with all the aggregate functions.
// It assumes all the input data is sorted by group by key.
// When Next() is called, it will return a result for the same group.
type StreamAggInterDirc struct {
	baseInterlockingDirectorate

	executed bool
	// isChildReturnEmpty indicates whether the child interlock only returns an empty input.
	isChildReturnEmpty bool
	defaultVal         *chunk.Chunk
	groupChecker       *vecGroupChecker
	inputIter          *chunk.Iterator4Chunk
	inputEvent           chunk.Event
	aggFuncs           []aggfuncs.AggFunc
	partialResults     []aggfuncs.PartialResult
	groupEvents          []chunk.Event
	childResult        *chunk.Chunk

	memTracker *memory.Tracker // track memory usage.
}

// Open implements the InterlockingDirectorate Open interface.
func (e *StreamAggInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	e.childResult = newFirstChunk(e.children[0])
	e.executed = false
	e.isChildReturnEmpty = true
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputEvent = e.inputIter.End()

	e.partialResults = make([]aggfuncs.PartialResult, 0, len(e.aggFuncs))
	for _, aggFunc := range e.aggFuncs {
		partialResult, memDelta := aggFunc.AllocPartialResult()
		e.partialResults = append(e.partialResults, partialResult)
		e.memTracker.Consume(memDelta)
	}

	// bytesLimit <= 0 means no limit, for now we just track the memory footprint
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)
	e.memTracker.Consume(e.childResult.MemoryUsage())
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *StreamAggInterDirc) Close() error {
	e.memTracker.Consume(-e.childResult.MemoryUsage())
	e.childResult = nil
	e.groupChecker.reset()
	return e.baseInterlockingDirectorate.Close()
}

// Next implements the InterlockingDirectorate Next interface.
func (e *StreamAggInterDirc) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for !e.executed && !req.IsFull() {
		err = e.consumeOneGroup(ctx, req)
		if err != nil {
			e.executed = true
			return err
		}
	}
	return nil
}

func (e *StreamAggInterDirc) consumeOneGroup(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.groupChecker.isExhausted() {
		if err = e.consumeCurGroupEventsAndFetchChild(ctx, chk); err != nil {
			return err
		}
		if !e.executed {
			_, err := e.groupChecker.splitIntoGroups(e.childResult)
			if err != nil {
				return err
			}
		} else {
			return nil
		}
	}
	begin, end := e.groupChecker.getNextGroup()
	for i := begin; i < end; i++ {
		e.groupEvents = append(e.groupEvents, e.childResult.GetEvent(i))
	}

	for meetLastGroup := end == e.childResult.NumEvents(); meetLastGroup; {
		meetLastGroup = false
		if err = e.consumeCurGroupEventsAndFetchChild(ctx, chk); err != nil || e.executed {
			return err
		}

		isFirstGroupSameAsPrev, err := e.groupChecker.splitIntoGroups(e.childResult)
		if err != nil {
			return err
		}

		if isFirstGroupSameAsPrev {
			begin, end = e.groupChecker.getNextGroup()
			for i := begin; i < end; i++ {
				e.groupEvents = append(e.groupEvents, e.childResult.GetEvent(i))
			}
			meetLastGroup = end == e.childResult.NumEvents()
		}
	}

	err = e.consumeGroupEvents()
	if err != nil {
		return err
	}

	return e.appendResult2Chunk(chk)
}

func (e *StreamAggInterDirc) consumeGroupEvents() error {
	if len(e.groupEvents) == 0 {
		return nil
	}

	for i, aggFunc := range e.aggFuncs {
		memDelta, err := aggFunc.UFIDelatePartialResult(e.ctx, e.groupEvents, e.partialResults[i])
		if err != nil {
			return err
		}
		e.memTracker.Consume(memDelta)
	}
	e.groupEvents = e.groupEvents[:0]
	return nil
}

func (e *StreamAggInterDirc) consumeCurGroupEventsAndFetchChild(ctx context.Context, chk *chunk.Chunk) (err error) {
	// Before fetching a new batch of input, we should consume the last group.
	err = e.consumeGroupEvents()
	if err != nil {
		return err
	}

	mSize := e.childResult.MemoryUsage()
	err = Next(ctx, e.children[0], e.childResult)
	e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
	if err != nil {
		return err
	}

	// No more data.
	if e.childResult.NumEvents() == 0 {
		if !e.isChildReturnEmpty {
			err = e.appendResult2Chunk(chk)
		} else if e.defaultVal != nil {
			chk.Append(e.defaultVal, 0, 1)
		}
		e.executed = true
		return err
	}
	// Reach here, "e.childrenResults[0].NumEvents() > 0" is guaranteed.
	e.isChildReturnEmpty = false
	e.inputEvent = e.inputIter.Begin()
	return nil
}

// appendResult2Chunk appends result of all the aggregation functions to the
// result chunk, and reset the evaluation context for each aggregation.
func (e *StreamAggInterDirc) appendResult2Chunk(chk *chunk.Chunk) error {
	for i, aggFunc := range e.aggFuncs {
		err := aggFunc.AppendFinalResult2Chunk(e.ctx, e.partialResults[i], chk)
		if err != nil {
			return err
		}
		aggFunc.ResetPartialResult(e.partialResults[i])
	}
	if len(e.aggFuncs) == 0 {
		chk.SetNumVirtualEvents(chk.NumEvents() + 1)
	}
	return nil
}

// vecGroupChecker is used to split a given chunk according to the `group by` memex in a vectorized manner
// It is usually used for streamAgg
type vecGroupChecker struct {
	ctx          stochastikctx.Context
	GroupByItems []memex.Expression

	// groupOffset holds the offset of the last event in each group of the current chunk
	groupOffset []int
	// groupCount is the count of groups in the current chunk
	groupCount int
	// nextGroupID records the group id of the next group to be consumed
	nextGroupID int

	// lastGroupKeyOfPrevChk is the groupKey of the last group of the previous chunk
	lastGroupKeyOfPrevChk []byte
	// firstGroupKey and lastGroupKey are used to causetstore the groupKey of the first and last group of the current chunk
	firstGroupKey []byte
	lastGroupKey  []byte

	// firstEventCausets and lastEventCausets causetstore the results of the memex evaluation for the first and last rows of the current chunk in causet
	// They are used to encode to get firstGroupKey and lastGroupKey
	firstEventCausets []types.Causet
	lastEventCausets  []types.Causet

	// sameGroup is used to check whether the current event belongs to the same group as the previous event
	sameGroup []bool

	// set these functions for testing
	allocateBuffer func(evalType types.EvalType, capacity int) (*chunk.DeferredCauset, error)
	releaseBuffer  func(buf *chunk.DeferredCauset)
}

func newVecGroupChecker(ctx stochastikctx.Context, items []memex.Expression) *vecGroupChecker {
	return &vecGroupChecker{
		ctx:          ctx,
		GroupByItems: items,
		groupCount:   0,
		nextGroupID:  0,
		sameGroup:    make([]bool, 1024),
	}
}

// splitIntoGroups splits a chunk into multiple groups which the event in the same group have the same groupKey
// `isFirstGroupSameAsPrev` indicates whether the groupKey of the first group of the newly passed chunk is equal to the groupKey of the last group left before
// TODO: Since all the group by items are only a defCausumn reference, guaranteed by building projection below aggregation, we can directly compare data in a chunk.
func (e *vecGroupChecker) splitIntoGroups(chk *chunk.Chunk) (isFirstGroupSameAsPrev bool, err error) {
	// The numEvents can not be zero. `fetchChild` is called before `splitIntoGroups` is called.
	// if numEvents == 0, it will be returned in `fetchChild`. See `fetchChild` for more details.
	numEvents := chk.NumEvents()

	e.reset()
	e.nextGroupID = 0
	if len(e.GroupByItems) == 0 {
		e.groupOffset = append(e.groupOffset, numEvents)
		e.groupCount = 1
		return true, nil
	}

	for _, item := range e.GroupByItems {
		err = e.getFirstAndLastEventCauset(item, chk, numEvents)
		if err != nil {
			return false, err
		}
	}
	e.firstGroupKey, err = codec.EncodeValue(e.ctx.GetStochastikVars().StmtCtx, e.firstGroupKey, e.firstEventCausets...)
	if err != nil {
		return false, err
	}

	e.lastGroupKey, err = codec.EncodeValue(e.ctx.GetStochastikVars().StmtCtx, e.lastGroupKey, e.lastEventCausets...)
	if err != nil {
		return false, err
	}

	if len(e.lastGroupKeyOfPrevChk) == 0 {
		isFirstGroupSameAsPrev = false
	} else {
		if bytes.Equal(e.lastGroupKeyOfPrevChk, e.firstGroupKey) {
			isFirstGroupSameAsPrev = true
		} else {
			isFirstGroupSameAsPrev = false
		}
	}

	if length := len(e.lastGroupKey); len(e.lastGroupKeyOfPrevChk) >= length {
		e.lastGroupKeyOfPrevChk = e.lastGroupKeyOfPrevChk[:length]
	} else {
		e.lastGroupKeyOfPrevChk = make([]byte, length)
	}
	copy(e.lastGroupKeyOfPrevChk, e.lastGroupKey)

	if bytes.Equal(e.firstGroupKey, e.lastGroupKey) {
		e.groupOffset = append(e.groupOffset, numEvents)
		e.groupCount = 1
		return isFirstGroupSameAsPrev, nil
	}

	if cap(e.sameGroup) < numEvents {
		e.sameGroup = make([]bool, 0, numEvents)
	}
	e.sameGroup = append(e.sameGroup, false)
	for i := 1; i < numEvents; i++ {
		e.sameGroup = append(e.sameGroup, true)
	}

	for _, item := range e.GroupByItems {
		err = e.evalGroupItemsAndResolveGroups(item, chk, numEvents)
		if err != nil {
			return false, err
		}
	}

	for i := 1; i < numEvents; i++ {
		if !e.sameGroup[i] {
			e.groupOffset = append(e.groupOffset, i)
		}
	}
	e.groupOffset = append(e.groupOffset, numEvents)
	e.groupCount = len(e.groupOffset)
	return isFirstGroupSameAsPrev, nil
}

func (e *vecGroupChecker) getFirstAndLastEventCauset(item memex.Expression, chk *chunk.Chunk, numEvents int) (err error) {
	var firstEventCauset, lastEventCauset types.Causet
	tp := item.GetType()
	eType := tp.EvalType()
	switch eType {
	case types.ETInt:
		firstEventVal, firstEventIsNull, err := item.EvalInt(e.ctx, chk.GetEvent(0))
		if err != nil {
			return err
		}
		lastEventVal, lastEventIsNull, err := item.EvalInt(e.ctx, chk.GetEvent(numEvents-1))
		if err != nil {
			return err
		}
		if !firstEventIsNull {
			firstEventCauset.SetInt64(firstEventVal)
		} else {
			firstEventCauset.SetNull()
		}
		if !lastEventIsNull {
			lastEventCauset.SetInt64(lastEventVal)
		} else {
			lastEventCauset.SetNull()
		}
	case types.ETReal:
		firstEventVal, firstEventIsNull, err := item.EvalReal(e.ctx, chk.GetEvent(0))
		if err != nil {
			return err
		}
		lastEventVal, lastEventIsNull, err := item.EvalReal(e.ctx, chk.GetEvent(numEvents-1))
		if err != nil {
			return err
		}
		if !firstEventIsNull {
			firstEventCauset.SetFloat64(firstEventVal)
		} else {
			firstEventCauset.SetNull()
		}
		if !lastEventIsNull {
			lastEventCauset.SetFloat64(lastEventVal)
		} else {
			lastEventCauset.SetNull()
		}
	case types.ETDecimal:
		firstEventVal, firstEventIsNull, err := item.EvalDecimal(e.ctx, chk.GetEvent(0))
		if err != nil {
			return err
		}
		lastEventVal, lastEventIsNull, err := item.EvalDecimal(e.ctx, chk.GetEvent(numEvents-1))
		if err != nil {
			return err
		}
		if !firstEventIsNull {
			// make a copy to avoid DATA RACE
			firstCauset := types.MyDecimal{}
			err := firstCauset.FromString(firstEventVal.ToString())
			if err != nil {
				return err
			}
			firstEventCauset.SetMysqlDecimal(&firstCauset)
		} else {
			firstEventCauset.SetNull()
		}
		if !lastEventIsNull {
			// make a copy to avoid DATA RACE
			lastCauset := types.MyDecimal{}
			err := lastCauset.FromString(lastEventVal.ToString())
			if err != nil {
				return err
			}
			lastEventCauset.SetMysqlDecimal(&lastCauset)
		} else {
			lastEventCauset.SetNull()
		}
	case types.ETDatetime, types.ETTimestamp:
		firstEventVal, firstEventIsNull, err := item.EvalTime(e.ctx, chk.GetEvent(0))
		if err != nil {
			return err
		}
		lastEventVal, lastEventIsNull, err := item.EvalTime(e.ctx, chk.GetEvent(numEvents-1))
		if err != nil {
			return err
		}
		if !firstEventIsNull {
			firstEventCauset.SetMysqlTime(firstEventVal)
		} else {
			firstEventCauset.SetNull()
		}
		if !lastEventIsNull {
			lastEventCauset.SetMysqlTime(lastEventVal)
		} else {
			lastEventCauset.SetNull()
		}
	case types.ETDuration:
		firstEventVal, firstEventIsNull, err := item.EvalDuration(e.ctx, chk.GetEvent(0))
		if err != nil {
			return err
		}
		lastEventVal, lastEventIsNull, err := item.EvalDuration(e.ctx, chk.GetEvent(numEvents-1))
		if err != nil {
			return err
		}
		if !firstEventIsNull {
			firstEventCauset.SetMysqlDuration(firstEventVal)
		} else {
			firstEventCauset.SetNull()
		}
		if !lastEventIsNull {
			lastEventCauset.SetMysqlDuration(lastEventVal)
		} else {
			lastEventCauset.SetNull()
		}
	case types.ETJson:
		firstEventVal, firstEventIsNull, err := item.EvalJSON(e.ctx, chk.GetEvent(0))
		if err != nil {
			return err
		}
		lastEventVal, lastEventIsNull, err := item.EvalJSON(e.ctx, chk.GetEvent(numEvents-1))
		if err != nil {
			return err
		}
		if !firstEventIsNull {
			// make a copy to avoid DATA RACE
			firstEventCauset.SetMysqlJSON(firstEventVal.Copy())
		} else {
			firstEventCauset.SetNull()
		}
		if !lastEventIsNull {
			// make a copy to avoid DATA RACE
			lastEventCauset.SetMysqlJSON(lastEventVal.Copy())
		} else {
			lastEventCauset.SetNull()
		}
	case types.ETString:
		firstEventVal, firstEventIsNull, err := item.EvalString(e.ctx, chk.GetEvent(0))
		if err != nil {
			return err
		}
		lastEventVal, lastEventIsNull, err := item.EvalString(e.ctx, chk.GetEvent(numEvents-1))
		if err != nil {
			return err
		}
		if !firstEventIsNull {
			// make a copy to avoid DATA RACE
			firstCauset := string([]byte(firstEventVal))
			firstEventCauset.SetString(firstCauset, tp.DefCauslate)
		} else {
			firstEventCauset.SetNull()
		}
		if !lastEventIsNull {
			// make a copy to avoid DATA RACE
			lastCauset := string([]byte(lastEventVal))
			lastEventCauset.SetString(lastCauset, tp.DefCauslate)
		} else {
			lastEventCauset.SetNull()
		}
	default:
		err = errors.New(fmt.Sprintf("invalid eval type %v", eType))
		return err
	}

	e.firstEventCausets = append(e.firstEventCausets, firstEventCauset)
	e.lastEventCausets = append(e.lastEventCausets, lastEventCauset)
	return err
}

// evalGroupItemsAndResolveGroups evaluates the chunk according to the memex item.
// And resolve the rows into groups according to the evaluation results
func (e *vecGroupChecker) evalGroupItemsAndResolveGroups(item memex.Expression, chk *chunk.Chunk, numEvents int) (err error) {
	tp := item.GetType()
	eType := tp.EvalType()
	if e.allocateBuffer == nil {
		e.allocateBuffer = memex.GetDeferredCauset
	}
	if e.releaseBuffer == nil {
		e.releaseBuffer = memex.PutDeferredCauset
	}
	defCaus, err := e.allocateBuffer(eType, numEvents)
	if err != nil {
		return err
	}
	defer e.releaseBuffer(defCaus)
	err = memex.EvalExpr(e.ctx, item, chk, defCaus)
	if err != nil {
		return err
	}

	previousIsNull := defCaus.IsNull(0)
	switch eType {
	case types.ETInt:
		vals := defCaus.Int64s()
		for i := 1; i < numEvents; i++ {
			isNull := defCaus.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETReal:
		vals := defCaus.Float64s()
		for i := 1; i < numEvents; i++ {
			isNull := defCaus.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDecimal:
		vals := defCaus.Decimals()
		for i := 1; i < numEvents; i++ {
			isNull := defCaus.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i].Compare(&vals[i-1]) != 0 {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDatetime, types.ETTimestamp:
		vals := defCaus.Times()
		for i := 1; i < numEvents; i++ {
			isNull := defCaus.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i].Compare(vals[i-1]) != 0 {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETDuration:
		vals := defCaus.GoDurations()
		for i := 1; i < numEvents; i++ {
			isNull := defCaus.IsNull(i)
			if e.sameGroup[i] {
				switch {
				case !previousIsNull && !isNull:
					if vals[i] != vals[i-1] {
						e.sameGroup[i] = false
					}
				case isNull != previousIsNull:
					e.sameGroup[i] = false
				}
			}
			previousIsNull = isNull
		}
	case types.ETJson:
		var previousKey, key json.BinaryJSON
		if !previousIsNull {
			previousKey = defCaus.GetJSON(0)
		}
		for i := 1; i < numEvents; i++ {
			isNull := defCaus.IsNull(i)
			if !isNull {
				key = defCaus.GetJSON(i)
			}
			if e.sameGroup[i] {
				if isNull == previousIsNull {
					if !isNull && json.CompareBinary(previousKey, key) != 0 {
						e.sameGroup[i] = false
					}
				} else {
					e.sameGroup[i] = false
				}
			}
			if !isNull {
				previousKey = key
			}
			previousIsNull = isNull
		}
	case types.ETString:
		previousKey := codec.ConvertByDefCauslationStr(defCaus.GetString(0), tp)
		for i := 1; i < numEvents; i++ {
			key := codec.ConvertByDefCauslationStr(defCaus.GetString(i), tp)
			isNull := defCaus.IsNull(i)
			if e.sameGroup[i] {
				if isNull != previousIsNull || previousKey != key {
					e.sameGroup[i] = false
				}
			}
			previousKey = key
			previousIsNull = isNull
		}
	default:
		err = errors.New(fmt.Sprintf("invalid eval type %v", eType))
	}
	if err != nil {
		return err
	}

	return err
}

func (e *vecGroupChecker) getNextGroup() (begin, end int) {
	if e.nextGroupID == 0 {
		begin = 0
	} else {
		begin = e.groupOffset[e.nextGroupID-1]
	}
	end = e.groupOffset[e.nextGroupID]
	e.nextGroupID++
	return begin, end
}

func (e *vecGroupChecker) isExhausted() bool {
	return e.nextGroupID >= e.groupCount
}

func (e *vecGroupChecker) reset() {
	if e.groupOffset != nil {
		e.groupOffset = e.groupOffset[:0]
	}
	if e.sameGroup != nil {
		e.sameGroup = e.sameGroup[:0]
	}
	if e.firstGroupKey != nil {
		e.firstGroupKey = e.firstGroupKey[:0]
	}
	if e.lastGroupKey != nil {
		e.lastGroupKey = e.lastGroupKey[:0]
	}
	if e.firstEventCausets != nil {
		e.firstEventCausets = e.firstEventCausets[:0]
	}
	if e.lastEventCausets != nil {
		e.lastEventCausets = e.lastEventCausets[:0]
	}
}
