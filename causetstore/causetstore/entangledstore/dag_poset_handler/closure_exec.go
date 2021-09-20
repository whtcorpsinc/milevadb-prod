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

package cophandler

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/juju/errors"
	"github.com/ngaut/entangledstore/einsteindb/dbreader"
	"github.com/ngaut/entangledstore/einsteindb/mvcc"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	mockpkg "github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

const chunkMaxRows = 1024

const (
	pkDefCausNotExists = iota
	pkDefCausIsSigned
	pkDefCausIsUnsigned
	pkDefCausIsCommon
)

func mapPkStatusToHandleStatus(pkStatus int) blockcodec.HandleStatus {
	switch pkStatus {
	case pkDefCausNotExists:
		return blockcodec.HandleNotNeeded
	case pkDefCausIsCommon | pkDefCausIsSigned:
		return blockcodec.HandleDefault
	case pkDefCausIsUnsigned:
		return blockcodec.HandleIsUnsigned
	}
	return blockcodec.HandleDefault
}

// buildClosureInterlockingDirectorate build a closureInterlockingDirectorate for the PosetDagRequest.
// Currently the composition of interlocks are:
// 	blockScan|indexScan [selection] [topN | limit | agg]
func buildClosureInterlockingDirectorate(posetPosetDagCtx *posetPosetDagContext, posetPosetDagReq *fidelpb.PosetDagRequest) (*closureInterlockingDirectorate, error) {
	ce, err := newClosureInterlockingDirectorate(posetPosetDagCtx, posetPosetDagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	interlocks := posetPosetDagReq.InterlockingDirectorates
	scanInterDirc := interlocks[0]
	if scanInterDirc.Tp == fidelpb.InterDircType_TypeTableScan {
		ce.processor = &blockScanProcessor{closureInterlockingDirectorate: ce}
	} else {
		ce.processor = &indexScanProcessor{closureInterlockingDirectorate: ce}
	}
	if len(interlocks) == 1 {
		return ce, nil
	}
	if secondInterDirc := interlocks[1]; secondInterDirc.Tp == fidelpb.InterDircType_TypeSelection {
		ce.selectionCtx.conditions, err = convertToExprs(ce.sc, ce.fieldTps, secondInterDirc.Selection.Conditions)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ce.processor = &selectionProcessor{closureInterlockingDirectorate: ce}
	}
	lastInterlockingDirectorate := interlocks[len(interlocks)-1]
	switch lastInterlockingDirectorate.Tp {
	case fidelpb.InterDircType_TypeLimit:
		ce.limit = int(lastInterlockingDirectorate.Limit.Limit)
	case fidelpb.InterDircType_TypeTopN:
		err = buildTopNProcessor(ce, lastInterlockingDirectorate.TopN)
	case fidelpb.InterDircType_TypeAggregation:
		err = buildHashAggProcessor(ce, posetPosetDagCtx, lastInterlockingDirectorate.Aggregation)
	case fidelpb.InterDircType_TypeStreamAgg:
		err = buildStreamAggProcessor(ce, posetPosetDagCtx, interlocks)
	case fidelpb.InterDircType_TypeSelection:
		ce.processor = &selectionProcessor{closureInterlockingDirectorate: ce}
	default:
		panic("unknown interlock type " + lastInterlockingDirectorate.Tp.String())
	}
	if err != nil {
		return nil, err
	}
	return ce, nil
}

func convertToExprs(sc *stmtctx.StatementContext, fieldTps []*types.FieldType, pbExprs []*fidelpb.Expr) ([]memex.Expression, error) {
	exprs := make([]memex.Expression, 0, len(pbExprs))
	for _, expr := range pbExprs {
		e, err := memex.PBToExpr(expr, fieldTps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

func newClosureInterlockingDirectorate(posetPosetDagCtx *posetPosetDagContext, posetPosetDagReq *fidelpb.PosetDagRequest) (*closureInterlockingDirectorate, error) {
	e := &closureInterlockingDirectorate{
		posetPosetDagContext: posetPosetDagCtx,
		outputOff:            posetPosetDagReq.OutputOffsets,
		startTS:              posetPosetDagCtx.startTS,
		limit:                math.MaxInt64,
	}
	seCtx := mockpkg.NewContext()
	seCtx.GetStochastikVars().StmtCtx = e.sc
	e.seCtx = seCtx
	interlocks := posetPosetDagReq.InterlockingDirectorates
	scanInterDirc := interlocks[0]
	switch scanInterDirc.Tp {
	case fidelpb.InterDircType_TypeTableScan:
		tblScan := interlocks[0].TblScan
		e.unique = true
		e.scanCtx.desc = tblScan.Desc
	case fidelpb.InterDircType_TypeIndexScan:
		idxScan := interlocks[0].IdxScan
		e.unique = idxScan.GetUnique()
		e.scanCtx.desc = idxScan.Desc
		e.initIdxScanCtx(idxScan)
	default:
		panic(fmt.Sprintf("unknown first interlock type %s", interlocks[0].Tp))
	}
	ranges, err := extractKVRanges(posetPosetDagCtx.dbReader.StartKey, posetPosetDagCtx.dbReader.EndKey, posetPosetDagCtx.keyRanges, e.scanCtx.desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if posetPosetDagReq.GetDefCauslectRangeCounts() {
		e.counts = make([]int64, len(ranges))
	}
	e.ekvRanges = ranges
	e.scanCtx.chk = chunk.NewChunkWithCapacity(e.fieldTps, 32)
	if e.idxScanCtx == nil {
		e.scanCtx.causetDecoder, err = e.evalContext.newRowCausetDecoder()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return e, nil
}

func (e *closureInterlockingDirectorate) initIdxScanCtx(idxScan *fidelpb.IndexScan) {
	e.idxScanCtx = new(idxScanCtx)
	e.idxScanCtx.columnLen = len(e.columnInfos)
	e.idxScanCtx.pkStatus = pkDefCausNotExists

	e.idxScanCtx.primaryDeferredCausetIds = idxScan.PrimaryDeferredCausetIds

	lastDeferredCauset := e.columnInfos[len(e.columnInfos)-1]

	if len(e.idxScanCtx.primaryDeferredCausetIds) == 0 {
		if lastDeferredCauset.GetPkHandle() {
			if allegrosql.HasUnsignedFlag(uint(lastDeferredCauset.GetFlag())) {
				e.idxScanCtx.pkStatus = pkDefCausIsUnsigned
			} else {
				e.idxScanCtx.pkStatus = pkDefCausIsSigned
			}
			e.idxScanCtx.columnLen--
		} else if lastDeferredCauset.DeferredCausetId == perceptron.ExtraHandleID {
			e.idxScanCtx.pkStatus = pkDefCausIsSigned
			e.idxScanCtx.columnLen--
		}
	} else {
		e.idxScanCtx.pkStatus = pkDefCausIsCommon
		e.idxScanCtx.columnLen -= len(e.idxScanCtx.primaryDeferredCausetIds)
	}

	colInfos := make([]rowcodec.DefCausInfo, len(e.columnInfos))
	for i := range colInfos {
		col := e.columnInfos[i]
		colInfos[i] = rowcodec.DefCausInfo{
			ID:         col.DeferredCausetId,
			Ft:         e.fieldTps[i],
			IsPKHandle: col.GetPkHandle(),
		}
	}
	e.idxScanCtx.colInfos = colInfos

	colIDs := make(map[int64]int, len(colInfos))
	for i, col := range colInfos[:e.idxScanCtx.columnLen] {
		colIDs[col.ID] = i
	}
	e.scanCtx.newDefCauslationIds = colIDs

	// We don't need to decode handle here, and colIDs >= 0 always.
	e.scanCtx.newDefCauslationRd = rowcodec.NewByteCausetDecoder(colInfos[:e.idxScanCtx.columnLen], []int64{-1}, nil, nil)
}

func isCountAgg(pbAgg *fidelpb.Aggregation) bool {
	if len(pbAgg.AggFunc) == 1 && len(pbAgg.GroupBy) == 0 {
		aggFunc := pbAgg.AggFunc[0]
		if aggFunc.Tp == fidelpb.ExprType_Count && len(aggFunc.Children) == 1 {
			return true
		}
	}
	return false
}

func tryBuildCountProcessor(e *closureInterlockingDirectorate, interlocks []*fidelpb.InterlockingDirectorate) (bool, error) {
	if len(interlocks) > 2 {
		return false, nil
	}
	agg := interlocks[1].Aggregation
	if !isCountAgg(agg) {
		return false, nil
	}
	child := agg.AggFunc[0].Children[0]
	switch child.Tp {
	case fidelpb.ExprType_DeferredCausetRef:
		_, idx, err := codec.DecodeInt(child.Val)
		if err != nil {
			return false, errors.Trace(err)
		}
		e.aggCtx.col = e.columnInfos[idx]
		if e.aggCtx.col.PkHandle {
			e.processor = &countStarProcessor{skipVal: skipVal(true), closureInterlockingDirectorate: e}
		} else {
			e.processor = &countDeferredCausetProcessor{closureInterlockingDirectorate: e}
		}
	case fidelpb.ExprType_Null, fidelpb.ExprType_ScalarFunc:
		return false, nil
	default:
		e.processor = &countStarProcessor{skipVal: skipVal(true), closureInterlockingDirectorate: e}
	}
	return true, nil
}

func buildTopNProcessor(e *closureInterlockingDirectorate, topN *fidelpb.TopN) error {
	heap, conds, err := getTopNInfo(e.evalContext, topN)
	if err != nil {
		return errors.Trace(err)
	}

	ctx := &topNCtx{
		heap:         heap,
		orderByExprs: conds,
		sortRow:      e.newTopNSortRow(),
	}

	e.topNCtx = ctx
	e.processor = &topNProcessor{closureInterlockingDirectorate: e}
	return nil
}

func buildHashAggProcessor(e *closureInterlockingDirectorate, ctx *posetPosetDagContext, agg *fidelpb.Aggregation) error {
	aggs, groupBys, err := getAggInfo(ctx, agg)
	if err != nil {
		return err
	}
	e.processor = &hashAggProcessor{
		closureInterlockingDirectorate: e,
		aggExprs:                       aggs,
		groupByExprs:                   groupBys,
		groups:                         map[string]struct{}{},
		groupKeys:                      nil,
		aggCtxsMap:                     map[string][]*aggregation.AggEvaluateContext{},
	}
	return nil
}

func buildStreamAggProcessor(e *closureInterlockingDirectorate, ctx *posetPosetDagContext, interlocks []*fidelpb.InterlockingDirectorate) error {
	ok, err := tryBuildCountProcessor(e, interlocks)
	if err != nil || ok {
		return err
	}
	return buildHashAggProcessor(e, ctx, interlocks[len(interlocks)-1].Aggregation)
}

// closureInterlockingDirectorate is an execution engine that flatten the PosetDagRequest.InterlockingDirectorates to a single closure `processor` that
// process key/value pairs. We can define many closures for different HoTTs of requests, try to use the specially
// optimized one for some frequently used query.
type closureInterlockingDirectorate struct {
	*posetPosetDagContext
	outputOff    []uint32
	seCtx        stochastikctx.Context
	ekvRanges    []ekv.KeyRange
	startTS      uint64
	ignoreLock   bool
	lockChecked  bool
	scanCtx      scanCtx
	idxScanCtx   *idxScanCtx
	selectionCtx selectionCtx
	aggCtx       aggCtx
	topNCtx      *topNCtx

	rowCount int
	unique   bool
	limit    int

	oldChunks []fidelpb.Chunk
	oldRowBuf []byte
	processor closureProcessor

	counts []int64
}

type closureProcessor interface {
	dbreader.ScanProcessor
	Finish() error
}

type scanCtx struct {
	count                    int
	limit                    int
	chk                      *chunk.Chunk
	desc                     bool
	causetDecoder            *rowcodec.ChunkCausetDecoder
	primaryDeferredCausetIds []int64

	newDefCauslationRd  *rowcodec.BytesCausetDecoder
	newDefCauslationIds map[int64]int
}

type idxScanCtx struct {
	pkStatus                 int
	columnLen                int
	colInfos                 []rowcodec.DefCausInfo
	primaryDeferredCausetIds []int64
}

type aggCtx struct {
	col *fidelpb.DeferredCausetInfo
}

type selectionCtx struct {
	conditions []memex.Expression
}

type topNCtx struct {
	heap         *topNHeap
	orderByExprs []memex.Expression
	sortRow      *sortRow
}

func (e *closureInterlockingDirectorate) execute() ([]fidelpb.Chunk, error) {
	err := e.checkRangeLock()
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbReader := e.dbReader
	for i, ran := range e.ekvRanges {
		if e.isPointGetRange(ran) {
			val, err := dbReader.Get(ran.StartKey, e.startTS)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(val) == 0 {
				continue
			}
			if e.counts != nil {
				e.counts[i]++
			}
			err = e.processor.Process(ran.StartKey, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			oldCnt := e.rowCount
			if e.scanCtx.desc {
				err = dbReader.ReverseScan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processor)
			} else {
				err = dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processor)
			}
			delta := int64(e.rowCount - oldCnt)
			if e.counts != nil {
				e.counts[i] += delta
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if e.rowCount == e.limit {
			break
		}
	}
	err = e.processor.Finish()
	return e.oldChunks, err
}

func (e *closureInterlockingDirectorate) isPointGetRange(ran ekv.KeyRange) bool {
	if len(e.primaryDefCauss) > 0 {
		return false
	}
	return e.unique && ran.IsPoint()
}

func (e *closureInterlockingDirectorate) checkRangeLock() error {
	if !e.ignoreLock && !e.lockChecked {
		for _, ran := range e.ekvRanges {
			err := e.checkRangeLockForRange(ran)
			if err != nil {
				return err
			}
		}
		e.lockChecked = true
	}
	return nil
}

func (e *closureInterlockingDirectorate) checkRangeLockForRange(ran ekv.KeyRange) error {
	it := e.lockStore.NewIterator()
	for it.Seek(ran.StartKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), ran.EndKey) {
			break
		}
		dagger := mvcc.DecodeLock(it.Value())
		err := checkLock(dagger, it.Key(), e.startTS, e.resolvedLocks)
		if err != nil {
			return err
		}
	}
	return nil
}

type countStarProcessor struct {
	skipVal
	*closureInterlockingDirectorate
}

// countStarProcess is used for `count(*)`.
func (e *countStarProcessor) Process(key, value []byte) error {
	e.rowCount++
	return nil
}

func (e *countStarProcessor) Finish() error {
	return e.countFinish()
}

// countFinish is used for `count(*)`.
func (e *closureInterlockingDirectorate) countFinish() error {
	d := types.NewIntCauset(int64(e.rowCount))
	rowData, err := codec.EncodeValue(e.sc, nil, d)
	if err != nil {
		return errors.Trace(err)
	}
	e.oldChunks = appendRow(e.oldChunks, rowData, 0)
	return nil
}

type countDeferredCausetProcessor struct {
	skipVal
	*closureInterlockingDirectorate
}

func (e *countDeferredCausetProcessor) Process(key, value []byte) error {
	if e.idxScanCtx != nil {
		values, _, err := blockcodec.CutIndexKeyNew(key, e.idxScanCtx.columnLen)
		if err != nil {
			return errors.Trace(err)
		}
		if values[0][0] != codec.NilFlag {
			e.rowCount++
		}
	} else {
		// Since the handle value doesn't affect the count result, we don't need to decode the handle.
		isNull, err := e.scanCtx.causetDecoder.DeferredCausetIsNull(value, e.aggCtx.col.DeferredCausetId, e.aggCtx.col.DefaultVal)
		if err != nil {
			return errors.Trace(err)
		}
		if !isNull {
			e.rowCount++
		}
	}
	return nil
}

func (e *countDeferredCausetProcessor) Finish() error {
	return e.countFinish()
}

type skipVal bool

func (s skipVal) SkipValue() bool {
	return bool(s)
}

type blockScanProcessor struct {
	skipVal
	*closureInterlockingDirectorate
}

func (e *blockScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ScanBreak
	}
	e.rowCount++
	err := e.blockScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *blockScanProcessor) Finish() error {
	return e.scanFinish()
}

func (e *closureInterlockingDirectorate) processCore(key, value []byte) error {
	if e.idxScanCtx != nil {
		return e.indexScanProcessCore(key, value)
	}
	return e.blockScanProcessCore(key, value)
}

func (e *closureInterlockingDirectorate) hasSelection() bool {
	return len(e.selectionCtx.conditions) > 0
}

func (e *closureInterlockingDirectorate) processSelection() (gotRow bool, err error) {
	chk := e.scanCtx.chk
	event := chk.GetRow(chk.NumRows() - 1)
	gotRow = true
	for _, expr := range e.selectionCtx.conditions {
		wc := e.sc.WarningCount()
		d, err := expr.Eval(event)
		if err != nil {
			return false, errors.Trace(err)
		}

		if d.IsNull() {
			gotRow = false
		} else {
			isBool, err := d.ToBool(e.sc)
			isBool, err = memex.HandleOverflowOnSelection(e.sc, isBool, err)
			if err != nil {
				return false, errors.Trace(err)
			}
			gotRow = isBool != 0
		}
		if !gotRow {
			if e.sc.WarningCount() > wc {
				// Deep-copy error object here, because the data it referenced is going to be truncated.
				warns := e.sc.TruncateWarnings(int(wc))
				for i, warn := range warns {
					warns[i].Err = e.copyError(warn.Err)
				}
				e.sc.AppendWarnings(warns)
			}
			chk.TruncateTo(chk.NumRows() - 1)
			break
		}
	}
	return
}

func (e *closureInterlockingDirectorate) copyError(err error) error {
	if err == nil {
		return nil
	}
	var ret error
	x := errors.Cause(err)
	switch y := x.(type) {
	case *terror.Error:
		ret = terror.ToALLEGROSQLError(y)
	default:
		ret = errors.New(err.Error())
	}
	return ret
}

func (e *closureInterlockingDirectorate) blockScanProcessCore(key, value []byte) error {
	handle, err := blockcodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.scanCtx.causetDecoder.DecodeToChunk(value, handle, e.scanCtx.chk)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *closureInterlockingDirectorate) scanFinish() error {
	return e.chunkToOldChunk(e.scanCtx.chk)
}

type indexScanProcessor struct {
	skipVal
	*closureInterlockingDirectorate
}

func (e *indexScanProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ScanBreak
	}
	e.rowCount++
	err := e.indexScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *indexScanProcessor) Finish() error {
	return e.scanFinish()
}

func (e *closureInterlockingDirectorate) indexScanProcessCore(key, value []byte) error {
	handleStatus := mapPkStatusToHandleStatus(e.idxScanCtx.pkStatus)
	restoredDefCauss := make([]rowcodec.DefCausInfo, 0, len(e.idxScanCtx.colInfos))
	for _, c := range e.idxScanCtx.colInfos {
		if c.ID != -1 {
			restoredDefCauss = append(restoredDefCauss, c)
		}
	}
	values, err := blockcodec.DecodeIndexKV(key, value, e.idxScanCtx.columnLen, handleStatus, restoredDefCauss)
	if err != nil {
		return err
	}
	chk := e.scanCtx.chk
	causetDecoder := codec.NewCausetDecoder(chk, e.sc.TimeZone)
	for i, colVal := range values {
		if i < len(e.fieldTps) {
			_, err = causetDecoder.DecodeOne(colVal, i, e.fieldTps[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *closureInterlockingDirectorate) chunkToOldChunk(chk *chunk.Chunk) error {
	var oldRow []types.Causet
	for i := 0; i < chk.NumRows(); i++ {
		oldRow = oldRow[:0]
		for _, outputOff := range e.outputOff {
			d := chk.GetRow(i).GetCauset(int(outputOff), e.fieldTps[outputOff])
			oldRow = append(oldRow, d)
		}
		var err error
		e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf[:0], oldRow...)
		if err != nil {
			return errors.Trace(err)
		}
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	chk.Reset()
	return nil
}

type selectionProcessor struct {
	skipVal
	*closureInterlockingDirectorate
}

func (e *selectionProcessor) Process(key, value []byte) error {
	if e.rowCount == e.limit {
		return dbreader.ScanBreak
	}
	err := e.processCore(key, value)
	if err != nil {
		return errors.Trace(err)
	}
	gotRow, err := e.processSelection()
	if err != nil {
		return err
	}
	if gotRow {
		e.rowCount++
		if e.scanCtx.chk.NumRows() == chunkMaxRows {
			err = e.chunkToOldChunk(e.scanCtx.chk)
		}
	}
	return err
}

func (e *selectionProcessor) Finish() error {
	return e.scanFinish()
}

type topNProcessor struct {
	skipVal
	*closureInterlockingDirectorate
}

func (e *topNProcessor) Process(key, value []byte) (err error) {
	if err = e.processCore(key, value); err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection()
		if err1 != nil || !gotRow {
			return err1
		}
	}

	ctx := e.topNCtx
	event := e.scanCtx.chk.GetRow(0)
	for i, expr := range ctx.orderByExprs {
		d, err := expr.Eval(event)
		if err != nil {
			return errors.Trace(err)
		}
		d.Copy(&ctx.sortRow.key[i])
	}
	e.scanCtx.chk.Reset()

	if ctx.heap.tryToAddRow(ctx.sortRow) {
		ctx.sortRow.data[0] = safeCopy(key)
		ctx.sortRow.data[1] = safeCopy(value)
		ctx.sortRow = e.newTopNSortRow()
	}
	return errors.Trace(ctx.heap.err)
}

func (e *closureInterlockingDirectorate) newTopNSortRow() *sortRow {
	return &sortRow{
		key:  make([]types.Causet, len(e.evalContext.columnInfos)),
		data: make([][]byte, 2),
	}
}

func (e *topNProcessor) Finish() error {
	ctx := e.topNCtx
	sort.Sort(&ctx.heap.topNSorter)
	chk := e.scanCtx.chk
	for _, event := range ctx.heap.rows {
		err := e.processCore(event.data[0], event.data[1])
		if err != nil {
			return err
		}
		if chk.NumRows() == chunkMaxRows {
			if err = e.chunkToOldChunk(chk); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return e.chunkToOldChunk(chk)
}

type hashAggProcessor struct {
	skipVal
	*closureInterlockingDirectorate

	aggExprs     []aggregation.Aggregation
	groupByExprs []memex.Expression
	groups       map[string]struct{}
	groupKeys    [][]byte
	aggCtxsMap   map[string][]*aggregation.AggEvaluateContext
}

func (e *hashAggProcessor) Process(key, value []byte) (err error) {
	err = e.processCore(key, value)
	if err != nil {
		return err
	}
	if e.hasSelection() {
		gotRow, err1 := e.processSelection()
		if err1 != nil || !gotRow {
			return err1
		}
	}
	event := e.scanCtx.chk.GetRow(e.scanCtx.chk.NumRows() - 1)
	gk, err := e.getGroupKey(event)
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
	}
	// UFIDelate aggregate memexs.
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		err = agg.UFIDelate(aggCtxs[i], e.sc, event)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.scanCtx.chk.Reset()
	return nil
}

func (e *hashAggProcessor) getGroupKey(event chunk.Row) ([]byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil
	}
	key := make([]byte, 0, 32)
	for _, item := range e.groupByExprs {
		v, err := item.Eval(event)
		if err != nil {
			return nil, errors.Trace(err)
		}
		b, err := codec.EncodeValue(e.sc, nil, v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key = append(key, b...)
	}
	return key, nil
}

func (e *hashAggProcessor) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	aggCtxs, ok := e.aggCtxsMap[string(groupKey)]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.sc))
		}
		e.aggCtxsMap[string(groupKey)] = aggCtxs
	}
	return aggCtxs
}

func (e *hashAggProcessor) Finish() error {
	for i, gk := range e.groupKeys {
		aggCtxs := e.getContexts(gk)
		e.oldRowBuf = e.oldRowBuf[:0]
		for i, agg := range e.aggExprs {
			partialResults := agg.GetPartialResult(aggCtxs[i])
			var err error
			e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf, partialResults...)
			if err != nil {
				return err
			}
		}
		e.oldRowBuf = append(e.oldRowBuf, gk...)
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	return nil
}

func safeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

func checkLock(dagger mvcc.MvccLock, key []byte, startTS uint64, resolved []uint64) error {
	if isResolved(startTS, resolved) {
		return nil
	}
	lockVisible := dagger.StartTS < startTS
	isWriteLock := dagger.Op == uint8(ekvrpcpb.Op_Put) || dagger.Op == uint8(ekvrpcpb.Op_Del)
	isPrimaryGet := startTS == math.MaxUint64 && bytes.Equal(dagger.Primary, key)
	if lockVisible && isWriteLock && !isPrimaryGet {
		return BuildLockErr(key, dagger.Primary, dagger.StartTS, uint64(dagger.TTL), dagger.Op)
	}
	return nil
}

func isResolved(startTS uint64, resolved []uint64) bool {
	for _, v := range resolved {
		if startTS == v {
			return true
		}
	}
	return false
}

func exceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
