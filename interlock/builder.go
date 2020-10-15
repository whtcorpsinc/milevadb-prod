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
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/ekvproto/pkg/diagnosticspb"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/allegrosql"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/interlock/aggfuncs"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	causetutil "github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"go.uber.org/zap"
)

var (
	interlockCounterMergeJoinInterDirc            = metrics.InterlockingDirectorateCounter.WithLabelValues("MergeJoinInterDirc")
	interlockCountHashJoinInterDirc               = metrics.InterlockingDirectorateCounter.WithLabelValues("HashJoinInterDirc")
	interlockCounterHashAggInterDirc              = metrics.InterlockingDirectorateCounter.WithLabelValues("HashAggInterDirc")
	interlockStreamAggInterDirc                   = metrics.InterlockingDirectorateCounter.WithLabelValues("StreamAggInterDirc")
	interlockCounterSortInterDirc                 = metrics.InterlockingDirectorateCounter.WithLabelValues("SortInterDirc")
	interlockCounterTopNInterDirc                 = metrics.InterlockingDirectorateCounter.WithLabelValues("TopNInterDirc")
	interlockCounterNestedLoopApplyInterDirc      = metrics.InterlockingDirectorateCounter.WithLabelValues("NestedLoopApplyInterDirc")
	interlockCounterIndexLookUpJoin          = metrics.InterlockingDirectorateCounter.WithLabelValues("IndexLookUpJoin")
	interlockCounterIndexLookUpInterlockingDirectorate      = metrics.InterlockingDirectorateCounter.WithLabelValues("IndexLookUpInterlockingDirectorate")
	interlockCounterIndexMergeReaderInterlockingDirectorate = metrics.InterlockingDirectorateCounter.WithLabelValues("IndexMergeReaderInterlockingDirectorate")
)

// interlockBuilder builds an InterlockingDirectorate from a Causet.
// The SchemaReplicant must not change during execution.
type interlockBuilder struct {
	ctx        stochastikctx.Context
	is         schemareplicant.SchemaReplicant
	snapshotTS uint64 // The consistent snapshot timestamp for the interlock to read data.
	err        error  // err is set when there is error happened during InterlockingDirectorate building process.
	hasLock    bool
}

func newInterlockingDirectorateBuilder(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) *interlockBuilder {
	return &interlockBuilder{
		ctx: ctx,
		is:  is,
	}
}

// MockPhysicalCauset is used to return a specified interlock in when build.
// It is mainly used for testing.
type MockPhysicalCauset interface {
	causetcore.PhysicalCauset
	GetInterlockingDirectorate() InterlockingDirectorate
}

func (b *interlockBuilder) build(p causetcore.Causet) InterlockingDirectorate {
	switch v := p.(type) {
	case nil:
		return nil
	case *causetcore.Change:
		return b.buildChange(v)
	case *causetcore.CheckBlock:
		return b.buildCheckBlock(v)
	case *causetcore.RecoverIndex:
		return b.buildRecoverIndex(v)
	case *causetcore.CleanupIndex:
		return b.buildCleanupIndex(v)
	case *causetcore.ChecHoTTexRange:
		return b.buildChecHoTTexRange(v)
	case *causetcore.ChecksumBlock:
		return b.buildChecksumBlock(v)
	case *causetcore.ReloadExprPushdownBlacklist:
		return b.buildReloadExprPushdownBlacklist(v)
	case *causetcore.ReloadOptMemruleBlacklist:
		return b.buildReloadOptMemruleBlacklist(v)
	case *causetcore.AdminPlugins:
		return b.buildAdminPlugins(v)
	case *causetcore.DBS:
		return b.buildDBS(v)
	case *causetcore.Deallocate:
		return b.buildDeallocate(v)
	case *causetcore.Delete:
		return b.buildDelete(v)
	case *causetcore.InterDircute:
		return b.buildInterDircute(v)
	case *causetcore.Trace:
		return b.buildTrace(v)
	case *causetcore.Explain:
		return b.buildExplain(v)
	case *causetcore.PointGetCauset:
		return b.buildPointGet(v)
	case *causetcore.BatchPointGetCauset:
		return b.buildBatchPointGet(v)
	case *causetcore.Insert:
		return b.buildInsert(v)
	case *causetcore.LoadData:
		return b.buildLoadData(v)
	case *causetcore.LoadStats:
		return b.buildLoadStats(v)
	case *causetcore.IndexAdvise:
		return b.buildIndexAdvise(v)
	case *causetcore.PhysicalLimit:
		return b.buildLimit(v)
	case *causetcore.Prepare:
		return b.buildPrepare(v)
	case *causetcore.PhysicalLock:
		return b.buildSelectLock(v)
	case *causetcore.CancelDBSJobs:
		return b.buildCancelDBSJobs(v)
	case *causetcore.ShowNextEventID:
		return b.buildShowNextEventID(v)
	case *causetcore.ShowDBS:
		return b.buildShowDBS(v)
	case *causetcore.PhysicalShowDBSJobs:
		return b.buildShowDBSJobs(v)
	case *causetcore.ShowDBSJobQueries:
		return b.buildShowDBSJobQueries(v)
	case *causetcore.ShowSlow:
		return b.buildShowSlow(v)
	case *causetcore.PhysicalShow:
		return b.buildShow(v)
	case *causetcore.Simple:
		return b.buildSimple(v)
	case *causetcore.Set:
		return b.buildSet(v)
	case *causetcore.SetConfig:
		return b.buildSetConfig(v)
	case *causetcore.PhysicalSort:
		return b.buildSort(v)
	case *causetcore.PhysicalTopN:
		return b.buildTopN(v)
	case *causetcore.PhysicalUnionAll:
		return b.buildUnionAll(v)
	case *causetcore.UFIDelate:
		return b.buildUFIDelate(v)
	case *causetcore.PhysicalUnionScan:
		return b.buildUnionScanInterDirc(v)
	case *causetcore.PhysicalHashJoin:
		return b.buildHashJoin(v)
	case *causetcore.PhysicalMergeJoin:
		return b.buildMergeJoin(v)
	case *causetcore.PhysicalIndexJoin:
		return b.buildIndexLookUpJoin(v)
	case *causetcore.PhysicalIndexMergeJoin:
		return b.buildIndexLookUpMergeJoin(v)
	case *causetcore.PhysicalIndexHashJoin:
		return b.buildIndexNestedLoopHashJoin(v)
	case *causetcore.PhysicalSelection:
		return b.buildSelection(v)
	case *causetcore.PhysicalHashAgg:
		return b.buildHashAgg(v)
	case *causetcore.PhysicalStreamAgg:
		return b.buildStreamAgg(v)
	case *causetcore.PhysicalProjection:
		return b.buildProjection(v)
	case *causetcore.PhysicalMemBlock:
		return b.buildMemBlock(v)
	case *causetcore.PhysicalBlockDual:
		return b.buildBlockDual(v)
	case *causetcore.PhysicalApply:
		return b.buildApply(v)
	case *causetcore.PhysicalMaxOneEvent:
		return b.buildMaxOneEvent(v)
	case *causetcore.Analyze:
		return b.buildAnalyze(v)
	case *causetcore.PhysicalBlockReader:
		return b.buildBlockReader(v)
	case *causetcore.PhysicalIndexReader:
		return b.buildIndexReader(v)
	case *causetcore.PhysicalIndexLookUpReader:
		return b.buildIndexLookUpReader(v)
	case *causetcore.PhysicalWindow:
		return b.buildWindow(v)
	case *causetcore.PhysicalShuffle:
		return b.buildShuffle(v)
	case *causetcore.PhysicalShuffleDataSourceStub:
		return b.buildShuffleDataSourceStub(v)
	case *causetcore.ALLEGROSQLBindCauset:
		return b.buildALLEGROSQLBindInterDirc(v)
	case *causetcore.SplitRegion:
		return b.buildSplitRegion(v)
	case *causetcore.PhysicalIndexMergeReader:
		return b.buildIndexMergeReader(v)
	case *causetcore.SelectInto:
		return b.buildSelectInto(v)
	case *causetcore.AdminShowTelemetry:
		return b.buildAdminShowTelemetry(v)
	case *causetcore.AdminResetTelemetryID:
		return b.buildAdminResetTelemetryID(v)
	default:
		if mp, ok := p.(MockPhysicalCauset); ok {
			return mp.GetInterlockingDirectorate()
		}

		b.err = ErrUnknownCauset.GenWithStack("Unknown Causet %T", p)
		return nil
	}
}

func (b *interlockBuilder) buildCancelDBSJobs(v *causetcore.CancelDBSJobs) InterlockingDirectorate {
	e := &CancelDBSJobsInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		jobIDs:       v.JobIDs,
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = err
		return nil
	}

	e.errs, b.err = admin.CancelJobs(txn, e.jobIDs)
	if b.err != nil {
		return nil
	}
	return e
}

func (b *interlockBuilder) buildChange(v *causetcore.Change) InterlockingDirectorate {
	return &ChangeInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		ChangeStmt:   v.ChangeStmt,
	}
}

func (b *interlockBuilder) buildShowNextEventID(v *causetcore.ShowNextEventID) InterlockingDirectorate {
	e := &ShowNextEventIDInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		tblName:      v.BlockName,
	}
	return e
}

func (b *interlockBuilder) buildShowDBS(v *causetcore.ShowDBS) InterlockingDirectorate {
	// We get DBSInfo here because for InterlockingDirectorates that returns result set,
	// next will be called after transaction has been committed.
	// We need the transaction to get DBSInfo.
	e := &ShowDBSInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
	}

	var err error
	tenantManager := petri.GetPetri(e.ctx).DBS().TenantManager()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	e.dbsTenantID, err = tenantManager.GetTenantID(ctx)
	cancel()
	if err != nil {
		b.err = err
		return nil
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		b.err = err
		return nil
	}

	dbsInfo, err := admin.GetDBSInfo(txn)
	if err != nil {
		b.err = err
		return nil
	}
	e.dbsInfo = dbsInfo
	e.selfID = tenantManager.ID()
	return e
}

func (b *interlockBuilder) buildShowDBSJobs(v *causetcore.PhysicalShowDBSJobs) InterlockingDirectorate {
	e := &ShowDBSJobsInterDirc{
		jobNumber:    int(v.JobNumber),
		is:           b.is,
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
	}
	return e
}

func (b *interlockBuilder) buildShowDBSJobQueries(v *causetcore.ShowDBSJobQueries) InterlockingDirectorate {
	e := &ShowDBSJobQueriesInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		jobIDs:       v.JobIDs,
	}
	return e
}

func (b *interlockBuilder) buildShowSlow(v *causetcore.ShowSlow) InterlockingDirectorate {
	e := &ShowSlowInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		ShowSlow:     v.ShowSlow,
	}
	return e
}

// buildIndexLookUpChecker builds check information to IndexLookUpReader.
func buildIndexLookUpChecker(b *interlockBuilder, p *causetcore.PhysicalIndexLookUpReader,
	e *IndexLookUpInterlockingDirectorate) {
	is := p.IndexCausets[0].(*causetcore.PhysicalIndexScan)
	fullDefCausLen := len(is.Index.DeferredCausets) + len(p.CommonHandleDefCauss)
	if !e.isCommonHandle() {
		fullDefCausLen += 1
	}
	e.posetPosetDagPB.OutputOffsets = make([]uint32, fullDefCausLen)
	for i := 0; i < fullDefCausLen; i++ {
		e.posetPosetDagPB.OutputOffsets[i] = uint32(i)
	}

	ts := p.BlockCausets[0].(*causetcore.PhysicalBlockScan)
	e.handleIdx = ts.HandleIdx

	e.ranges = ranger.FullRange()

	tps := make([]*types.FieldType, 0, fullDefCausLen)
	for _, defCaus := range is.DeferredCausets {
		tps = append(tps, &defCaus.FieldType)
	}

	if !e.isCommonHandle() {
		tps = append(tps, types.NewFieldType(allegrosql.TypeLonglong))
	}

	e.checHoTTexValue = &checHoTTexValue{idxDefCausTps: tps}

	defCausNames := make([]string, 0, len(is.IdxDefCauss))
	for i := range is.IdxDefCauss {
		defCausNames = append(defCausNames, is.DeferredCausets[i].Name.O)
	}
	if defcaus, missingDefCausName := causet.FindDefCauss(e.causet.DefCauss(), defCausNames, true); missingDefCausName != "" {
		b.err = causetcore.ErrUnknownDeferredCauset.GenWithStack("Unknown defCausumn %s", missingDefCausName)
	} else {
		e.idxTblDefCauss = defcaus
	}
}

func (b *interlockBuilder) buildCheckBlock(v *causetcore.CheckBlock) InterlockingDirectorate {
	readerInterDircs := make([]*IndexLookUpInterlockingDirectorate, 0, len(v.IndexLookUpReaders))
	for _, readerCauset := range v.IndexLookUpReaders {
		readerInterDirc, err := buildNoRangeIndexLookUpReader(b, readerCauset)
		if err != nil {
			b.err = errors.Trace(err)
			return nil
		}
		buildIndexLookUpChecker(b, readerCauset, readerInterDirc)

		readerInterDircs = append(readerInterDircs, readerInterDirc)
	}

	e := &CheckBlockInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		dbName:       v.DBName,
		causet:        v.Block,
		indexInfos:   v.IndexInfos,
		is:           b.is,
		srcs:         readerInterDircs,
		exitCh:       make(chan struct{}),
		retCh:        make(chan error, len(readerInterDircs)),
		checHoTTex:   v.ChecHoTTex,
	}
	return e
}

func buildIdxDefCaussConcatHandleDefCauss(tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo) []*perceptron.DeferredCausetInfo {
	handleLen := 1
	var pkDefCauss []*perceptron.IndexDeferredCauset
	if tblInfo.IsCommonHandle {
		pkIdx := blocks.FindPrimaryIndex(tblInfo)
		pkDefCauss = pkIdx.DeferredCausets
		handleLen = len(pkIdx.DeferredCausets)
	}
	defCausumns := make([]*perceptron.DeferredCausetInfo, 0, len(indexInfo.DeferredCausets)+handleLen)
	for _, idxDefCaus := range indexInfo.DeferredCausets {
		defCausumns = append(defCausumns, tblInfo.DeferredCausets[idxDefCaus.Offset])
	}
	if tblInfo.IsCommonHandle {
		for _, c := range pkDefCauss {
			defCausumns = append(defCausumns, tblInfo.DeferredCausets[c.Offset])
		}
		return defCausumns
	}
	handleOffset := len(defCausumns)
	handleDefCaussInfo := &perceptron.DeferredCausetInfo{
		ID:     perceptron.ExtraHandleID,
		Name:   perceptron.ExtraHandleName,
		Offset: handleOffset,
	}
	handleDefCaussInfo.FieldType = *types.NewFieldType(allegrosql.TypeLonglong)
	defCausumns = append(defCausumns, handleDefCaussInfo)
	return defCausumns
}

func (b *interlockBuilder) buildRecoverIndex(v *causetcore.RecoverIndex) InterlockingDirectorate {
	tblInfo := v.Block.BlockInfo
	t, err := b.is.BlockByName(v.Block.Schema, tblInfo.Name)
	if err != nil {
		b.err = err
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	index := blocks.GetWriblockIndexByName(idxName, t)
	if index == nil {
		b.err = errors.Errorf("index `%v` is not found in causet `%v`.", v.IndexName, v.Block.Name.O)
		return nil
	}
	e := &RecoverIndexInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		defCausumns:      buildIdxDefCaussConcatHandleDefCauss(tblInfo, index.Meta()),
		index:        index,
		causet:        t,
		physicalID:   t.Meta().ID,
	}
	sessCtx := e.ctx.GetStochastikVars().StmtCtx
	e.handleDefCauss = buildHandleDefCaussForInterDirc(sessCtx, tblInfo, index.Meta(), e.defCausumns)
	return e
}

func buildHandleDefCaussForInterDirc(sctx *stmtctx.StatementContext, tblInfo *perceptron.BlockInfo,
	idxInfo *perceptron.IndexInfo, allDefCausInfo []*perceptron.DeferredCausetInfo) causetcore.HandleDefCauss {
	if !tblInfo.IsCommonHandle {
		extraDefCausPos := len(allDefCausInfo) - 1
		intDefCaus := &memex.DeferredCauset{
			Index:   extraDefCausPos,
			RetType: types.NewFieldType(allegrosql.TypeLonglong),
		}
		return causetcore.NewIntHandleDefCauss(intDefCaus)
	}
	tblDefCauss := make([]*memex.DeferredCauset, len(tblInfo.DeferredCausets))
	for i := 0; i < len(tblInfo.DeferredCausets); i++ {
		c := tblInfo.DeferredCausets[i]
		tblDefCauss[i] = &memex.DeferredCauset{
			RetType: &c.FieldType,
			ID:      c.ID,
		}
	}
	pkIdx := blocks.FindPrimaryIndex(tblInfo)
	for i, c := range pkIdx.DeferredCausets {
		tblDefCauss[c.Offset].Index = len(idxInfo.DeferredCausets) + i
	}
	return causetcore.NewCommonHandleDefCauss(sctx, tblInfo, pkIdx, tblDefCauss)
}

func (b *interlockBuilder) buildCleanupIndex(v *causetcore.CleanupIndex) InterlockingDirectorate {
	tblInfo := v.Block.BlockInfo
	t, err := b.is.BlockByName(v.Block.Schema, tblInfo.Name)
	if err != nil {
		b.err = err
		return nil
	}
	idxName := strings.ToLower(v.IndexName)
	var index causet.Index
	for _, idx := range t.Indices() {
		if idx.Meta().State != perceptron.StatePublic {
			continue
		}
		if idxName == idx.Meta().Name.L {
			index = idx
			break
		}
	}

	if index == nil {
		b.err = errors.Errorf("index `%v` is not found in causet `%v`.", v.IndexName, v.Block.Name.O)
		return nil
	}
	e := &CleanupIndexInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		defCausumns:      buildIdxDefCaussConcatHandleDefCauss(tblInfo, index.Meta()),
		index:        index,
		causet:        t,
		physicalID:   t.Meta().ID,
		batchSize:    20000,
	}
	sessCtx := e.ctx.GetStochastikVars().StmtCtx
	e.handleDefCauss = buildHandleDefCaussForInterDirc(sessCtx, tblInfo, index.Meta(), e.defCausumns)
	return e
}

func (b *interlockBuilder) buildChecHoTTexRange(v *causetcore.ChecHoTTexRange) InterlockingDirectorate {
	tb, err := b.is.BlockByName(v.Block.Schema, v.Block.Name)
	if err != nil {
		b.err = err
		return nil
	}
	e := &ChecHoTTexRangeInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		handleRanges: v.HandleRanges,
		causet:        tb.Meta(),
		is:           b.is,
	}
	idxName := strings.ToLower(v.IndexName)
	for _, idx := range tb.Indices() {
		if idx.Meta().Name.L == idxName {
			e.index = idx.Meta()
			e.startKey = make([]types.Causet, len(e.index.DeferredCausets))
			break
		}
	}
	return e
}

func (b *interlockBuilder) buildChecksumBlock(v *causetcore.ChecksumBlock) InterlockingDirectorate {
	e := &ChecksumBlockInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		blocks:       make(map[int64]*checksumContext),
		done:         false,
	}
	startTs, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	for _, t := range v.Blocks {
		e.blocks[t.BlockInfo.ID] = newChecksumContext(t.DBInfo, t.BlockInfo, startTs)
	}
	return e
}

func (b *interlockBuilder) buildReloadExprPushdownBlacklist(v *causetcore.ReloadExprPushdownBlacklist) InterlockingDirectorate {
	return &ReloadExprPushdownBlacklistInterDirc{baseInterlockingDirectorate{ctx: b.ctx}}
}

func (b *interlockBuilder) buildReloadOptMemruleBlacklist(v *causetcore.ReloadOptMemruleBlacklist) InterlockingDirectorate {
	return &ReloadOptMemruleBlacklistInterDirc{baseInterlockingDirectorate{ctx: b.ctx}}
}

func (b *interlockBuilder) buildAdminPlugins(v *causetcore.AdminPlugins) InterlockingDirectorate {
	return &AdminPluginsInterDirc{baseInterlockingDirectorate: baseInterlockingDirectorate{ctx: b.ctx}, CausetAction: v.CausetAction, Plugins: v.Plugins}
}

func (b *interlockBuilder) buildDeallocate(v *causetcore.Deallocate) InterlockingDirectorate {
	base := newBaseInterlockingDirectorate(b.ctx, nil, v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &DeallocateInterDirc{
		baseInterlockingDirectorate: base,
		Name:         v.Name,
	}
	return e
}

func (b *interlockBuilder) buildSelectLock(v *causetcore.PhysicalLock) InterlockingDirectorate {
	b.hasLock = true
	if b.err = b.uFIDelateForUFIDelateTSIfNeeded(v.Children()[0]); b.err != nil {
		return nil
	}
	// Build 'select for uFIDelate' using the 'for uFIDelate' ts.
	b.snapshotTS = b.ctx.GetStochastikVars().TxnCtx.GetForUFIDelateTS()

	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	if !b.ctx.GetStochastikVars().InTxn() {
		// Locking of rows for uFIDelate using SELECT FOR UFIDelATE only applies when autocommit
		// is disabled (either by beginning transaction with START TRANSACTION or by setting
		// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
		// See https://dev.allegrosql.com/doc/refman/5.7/en/innodb-locking-reads.html
		return src
	}
	e := &SelectLockInterDirc{
		baseInterlockingDirectorate:     newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), src),
		Lock:             v.Lock,
		tblID2Handle:     v.TblID2Handle,
		partitionedBlock: v.PartitionedBlock,
	}
	return e
}

func (b *interlockBuilder) buildLimit(v *causetcore.PhysicalLimit) InterlockingDirectorate {
	childInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	n := int(mathutil.MinUint64(v.Count, uint64(b.ctx.GetStochastikVars().MaxChunkSize)))
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDirc)
	base.initCap = n
	e := &LimitInterDirc{
		baseInterlockingDirectorate: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	return e
}

func (b *interlockBuilder) buildPrepare(v *causetcore.Prepare) InterlockingDirectorate {
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	return &PrepareInterDirc{
		baseInterlockingDirectorate: base,
		is:           b.is,
		name:         v.Name,
		sqlText:      v.ALLEGROSQLText,
	}
}

func (b *interlockBuilder) buildInterDircute(v *causetcore.InterDircute) InterlockingDirectorate {
	e := &InterDircuteInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		is:           b.is,
		name:         v.Name,
		usingVars:    v.UsingVars,
		id:           v.InterDircID,
		stmt:         v.Stmt,
		plan:         v.Causet,
		outputNames:  v.OutputNames(),
	}
	return e
}

func (b *interlockBuilder) buildShow(v *causetcore.PhysicalShow) InterlockingDirectorate {
	e := &ShowInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		Tp:           v.Tp,
		DBName:       perceptron.NewCIStr(v.DBName),
		Block:        v.Block,
		DeferredCauset:       v.DeferredCauset,
		IndexName:    v.IndexName,
		Flag:         v.Flag,
		Roles:        v.Roles,
		User:         v.User,
		is:           b.is,
		Full:         v.Full,
		IfNotExists:  v.IfNotExists,
		GlobalScope:  v.GlobalScope,
		Extended:     v.Extended,
	}
	if e.Tp == ast.ShowGrants && e.User == nil {
		// The input is a "show grants" memex, fulfill the user and roles field.
		// Note: "show grants" result are different from "show grants for current_user",
		// The former determine privileges with roles, while the later doesn't.
		vars := e.ctx.GetStochastikVars()
		e.User = &auth.UserIdentity{Username: vars.User.AuthUsername, Hostname: vars.User.AuthHostname}
		e.Roles = vars.ActiveRoles
	}
	if e.Tp == ast.ShowMasterStatus {
		// show master status need start ts.
		if _, err := e.ctx.Txn(true); err != nil {
			b.err = err
		}
	}
	return e
}

func (b *interlockBuilder) buildSimple(v *causetcore.Simple) InterlockingDirectorate {
	switch s := v.Statement.(type) {
	case *ast.GrantStmt:
		return b.buildGrant(s)
	case *ast.RevokeStmt:
		return b.buildRevoke(s)
	case *ast.BRIEStmt:
		return b.buildBRIE(s, v.Schema())
	}
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &SimpleInterDirc{
		baseInterlockingDirectorate: base,
		Statement:    v.Statement,
		is:           b.is,
	}
	return e
}

func (b *interlockBuilder) buildSet(v *causetcore.Set) InterlockingDirectorate {
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity
	e := &SetInterlockingDirectorate{
		baseInterlockingDirectorate: base,
		vars:         v.VarAssigns,
	}
	return e
}

func (b *interlockBuilder) buildSetConfig(v *causetcore.SetConfig) InterlockingDirectorate {
	return &SetConfigInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		p:            v,
	}
}

func (b *interlockBuilder) buildInsert(v *causetcore.Insert) InterlockingDirectorate {
	if v.SelectCauset != nil {
		// Try to uFIDelate the forUFIDelateTS for insert/replace into select memexs.
		// Set the selectCauset parameter to nil to make it always uFIDelate the forUFIDelateTS.
		if b.err = b.uFIDelateForUFIDelateTSIfNeeded(nil); b.err != nil {
			return nil
		}
	}
	b.snapshotTS = b.ctx.GetStochastikVars().TxnCtx.GetForUFIDelateTS()
	selectInterDirc := b.build(v.SelectCauset)
	if b.err != nil {
		return nil
	}
	var baseInterDirc baseInterlockingDirectorate
	if selectInterDirc != nil {
		baseInterDirc = newBaseInterlockingDirectorate(b.ctx, nil, v.ID(), selectInterDirc)
	} else {
		baseInterDirc = newBaseInterlockingDirectorate(b.ctx, nil, v.ID())
	}
	baseInterDirc.initCap = chunk.ZeroCapacity

	ivs := &InsertValues{
		baseInterlockingDirectorate:              baseInterDirc,
		Block:                     v.Block,
		DeferredCausets:                   v.DeferredCausets,
		Lists:                     v.Lists,
		SetList:                   v.SetList,
		GenExprs:                  v.GenDefCauss.Exprs,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		hasRefDefCauss:                v.NeedFillDefaultValue,
		SelectInterDirc:                selectInterDirc,
	}
	err := ivs.initInsertDeferredCausets()
	if err != nil {
		b.err = err
		return nil
	}

	if v.IsReplace {
		return b.buildReplace(ivs)
	}
	insert := &InsertInterDirc{
		InsertValues: ivs,
		OnDuplicate:  append(v.OnDuplicate, v.GenDefCauss.OnDuplicates...),
	}
	return insert
}

func (b *interlockBuilder) buildLoadData(v *causetcore.LoadData) InterlockingDirectorate {
	tbl, ok := b.is.BlockByID(v.Block.BlockInfo.ID)
	if !ok {
		b.err = errors.Errorf("Can not get causet %d", v.Block.BlockInfo.ID)
		return nil
	}
	insertVal := &InsertValues{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, nil, v.ID()),
		Block:        tbl,
		DeferredCausets:      v.DeferredCausets,
		GenExprs:     v.GenDefCauss.Exprs,
	}
	loadDataInfo := &LoadDataInfo{
		event:                make([]types.Causet, 0, len(insertVal.insertDeferredCausets)),
		InsertValues:       insertVal,
		Path:               v.Path,
		Block:              tbl,
		FieldsInfo:         v.FieldsInfo,
		LinesInfo:          v.LinesInfo,
		IgnoreLines:        v.IgnoreLines,
		DeferredCausetAssignments:  v.DeferredCausetAssignments,
		DeferredCausetsAndUserVars: v.DeferredCausetsAndUserVars,
		Ctx:                b.ctx,
	}
	defCausumnNames := loadDataInfo.initFieldMappings()
	err := loadDataInfo.initLoadDeferredCausets(defCausumnNames)
	if err != nil {
		b.err = err
		return nil
	}
	loadDataInterDirc := &LoadDataInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, nil, v.ID()),
		IsLocal:      v.IsLocal,
		OnDuplicate:  v.OnDuplicate,
		loadDataInfo: loadDataInfo,
	}
	var defaultLoadDataBatchCnt uint64 = 20000 // TODO this will be changed to variable in another pr
	loadDataInterDirc.loadDataInfo.InitQueues()
	loadDataInterDirc.loadDataInfo.SetMaxEventsInBatch(defaultLoadDataBatchCnt)

	return loadDataInterDirc
}

func (b *interlockBuilder) buildLoadStats(v *causetcore.LoadStats) InterlockingDirectorate {
	e := &LoadStatsInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, nil, v.ID()),
		info:         &LoadStatsInfo{v.Path, b.ctx},
	}
	return e
}

func (b *interlockBuilder) buildIndexAdvise(v *causetcore.IndexAdvise) InterlockingDirectorate {
	e := &IndexAdviseInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, nil, v.ID()),
		IsLocal:      v.IsLocal,
		indexAdviseInfo: &IndexAdviseInfo{
			Path:        v.Path,
			MaxMinutes:  v.MaxMinutes,
			MaxIndexNum: v.MaxIndexNum,
			LinesInfo:   v.LinesInfo,
			Ctx:         b.ctx,
		},
	}
	return e
}

func (b *interlockBuilder) buildReplace(vals *InsertValues) InterlockingDirectorate {
	replaceInterDirc := &ReplaceInterDirc{
		InsertValues: vals,
	}
	return replaceInterDirc
}

func (b *interlockBuilder) buildGrant(grant *ast.GrantStmt) InterlockingDirectorate {
	e := &GrantInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, nil, 0),
		Privs:        grant.Privs,
		ObjectType:   grant.ObjectType,
		Level:        grant.Level,
		Users:        grant.Users,
		WithGrant:    grant.WithGrant,
		TLSOptions:   grant.TLSOptions,
		is:           b.is,
	}
	return e
}

func (b *interlockBuilder) buildRevoke(revoke *ast.RevokeStmt) InterlockingDirectorate {
	e := &RevokeInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, nil, 0),
		ctx:          b.ctx,
		Privs:        revoke.Privs,
		ObjectType:   revoke.ObjectType,
		Level:        revoke.Level,
		Users:        revoke.Users,
		is:           b.is,
	}
	return e
}

func (b *interlockBuilder) buildDBS(v *causetcore.DBS) InterlockingDirectorate {
	e := &DBSInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		stmt:         v.Statement,
		is:           b.is,
	}
	return e
}

// buildTrace builds a TraceInterDirc for future executing. This method will be called
// at build().
func (b *interlockBuilder) buildTrace(v *causetcore.Trace) InterlockingDirectorate {
	t := &TraceInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		stmtNode:     v.StmtNode,
		builder:      b,
		format:       v.Format,
	}
	if t.format == causetcore.TraceFormatLog {
		return &SortInterDirc{
			baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), t),
			ByItems: []*causetutil.ByItems{
				{Expr: &memex.DeferredCauset{
					Index:   0,
					RetType: types.NewFieldType(allegrosql.TypeTimestamp),
				}},
			},
			schemaReplicant: v.Schema(),
		}
	}
	return t
}

// buildExplain builds a explain interlock. `e.rows` defCauslects final result to `ExplainInterDirc`.
func (b *interlockBuilder) buildExplain(v *causetcore.Explain) InterlockingDirectorate {
	explainInterDirc := &ExplainInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		explain:      v,
	}
	if v.Analyze {
		if b.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl == nil {
			b.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl = execdetails.NewRuntimeStatsDefCausl()
		}
		explainInterDirc.analyzeInterDirc = b.build(v.TargetCauset)
	}
	return explainInterDirc
}

func (b *interlockBuilder) buildSelectInto(v *causetcore.SelectInto) InterlockingDirectorate {
	child := b.build(v.TargetCauset)
	if b.err != nil {
		return nil
	}
	return &SelectIntoInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), child),
		intoOpt:      v.IntoOpt,
	}
}

func (b *interlockBuilder) buildUnionScanInterDirc(v *causetcore.PhysicalUnionScan) InterlockingDirectorate {
	reader := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	return b.buildUnionScanFromReader(reader, v)
}

// buildUnionScanFromReader builds union scan interlock from child interlock.
// Note that this function may be called by inner workers of index lookup join concurrently.
// Be careful to avoid data race.
func (b *interlockBuilder) buildUnionScanFromReader(reader InterlockingDirectorate, v *causetcore.PhysicalUnionScan) InterlockingDirectorate {
	// Adjust UnionScan->PartitionBlock->Reader
	// to PartitionBlock->UnionScan->Reader
	// The build of UnionScan interlock is delay to the nextPartition() function
	// because the Reader interlock is available there.
	if x, ok := reader.(*PartitionBlockInterlockingDirectorate); ok {
		nextPartitionForReader := x.nextPartition
		x.nextPartition = nextPartitionForUnionScan{
			b:     b,
			us:    v,
			child: nextPartitionForReader,
		}
		return x
	}
	// If reader is union, it means a partitiont causet and we should transfer as above.
	if x, ok := reader.(*UnionInterDirc); ok {
		for i, child := range x.children {
			x.children[i] = b.buildUnionScanFromReader(child, v)
			if b.err != nil {
				return nil
			}
		}
		return x
	}
	us := &UnionScanInterDirc{baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), reader)}
	// Get the handle defCausumn index of the below Causet.
	us.belowHandleDefCauss = v.HandleDefCauss
	us.mublockEvent = chunk.MutEventFromTypes(retTypes(us))

	// If the push-downed condition contains virtual defCausumn, we may build a selection upon reader
	originReader := reader
	if sel, ok := reader.(*SelectionInterDirc); ok {
		reader = sel.children[0]
	}

	switch x := reader.(type) {
	case *BlockReaderInterlockingDirectorate:
		us.desc = x.desc
		// Union scan can only be in a write transaction, so DirtyDB should has non-nil value now, thus
		// GetDirtyDB() is safe here. If this causet has been modified in the transaction, non-nil DirtyBlock
		// can be found in DirtyDB now, so GetDirtyBlock is safe; if this causet has not been modified in the
		// transaction, empty DirtyBlock would be inserted into DirtyDB, it does not matter when multiple
		// goroutines write empty DirtyBlock to DirtyDB for this causet concurrently. Although the DirtyDB looks
		// safe for data race in all the cases, the map of golang will throw panic when it's accessed in parallel.
		// So we dagger it when getting dirty causet.
		us.conditions, us.conditionsWithVirDefCaus = causetcore.SplitSelCondsWithVirtualDeferredCauset(v.Conditions)
		us.defCausumns = x.defCausumns
		us.causet = x.causet
		us.virtualDeferredCausetIndex = x.virtualDeferredCausetIndex
	case *IndexReaderInterlockingDirectorate:
		us.desc = x.desc
		for _, ic := range x.index.DeferredCausets {
			for i, defCaus := range x.defCausumns {
				if defCaus.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.conditions, us.conditionsWithVirDefCaus = causetcore.SplitSelCondsWithVirtualDeferredCauset(v.Conditions)
		us.defCausumns = x.defCausumns
		us.causet = x.causet
	case *IndexLookUpInterlockingDirectorate:
		us.desc = x.desc
		for _, ic := range x.index.DeferredCausets {
			for i, defCaus := range x.defCausumns {
				if defCaus.Name.L == ic.Name.L {
					us.usedIndex = append(us.usedIndex, i)
					break
				}
			}
		}
		us.conditions, us.conditionsWithVirDefCaus = causetcore.SplitSelCondsWithVirtualDeferredCauset(v.Conditions)
		us.defCausumns = x.defCausumns
		us.causet = x.causet
		us.virtualDeferredCausetIndex = buildVirtualDeferredCausetIndex(us.Schema(), us.defCausumns)
	default:
		// The mem causet will not be written by allegrosql directly, so we can omit the union scan to avoid err reporting.
		return originReader
	}
	return us
}

// buildMergeJoin builds MergeJoinInterDirc interlock.
func (b *interlockBuilder) buildMergeJoin(v *causetcore.PhysicalMergeJoin) InterlockingDirectorate {
	leftInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightInterDirc := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	defaultValues := v.DefaultValues
	if defaultValues == nil {
		if v.JoinType == causetcore.RightOuterJoin {
			defaultValues = make([]types.Causet, leftInterDirc.Schema().Len())
		} else {
			defaultValues = make([]types.Causet, rightInterDirc.Schema().Len())
		}
	}

	e := &MergeJoinInterDirc{
		stmtCtx:      b.ctx.GetStochastikVars().StmtCtx,
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), leftInterDirc, rightInterDirc),
		compareFuncs: v.CompareFuncs,
		joiner: newJoiner(
			b.ctx,
			v.JoinType,
			v.JoinType == causetcore.RightOuterJoin,
			defaultValues,
			v.OtherConditions,
			retTypes(leftInterDirc),
			retTypes(rightInterDirc),
			markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema()),
		),
		isOuterJoin: v.JoinType.IsOuterJoin(),
		desc:        v.Desc,
	}

	leftBlock := &mergeJoinBlock{
		childIndex: 0,
		joinKeys:   v.LeftJoinKeys,
		filters:    v.LeftConditions,
	}
	rightBlock := &mergeJoinBlock{
		childIndex: 1,
		joinKeys:   v.RightJoinKeys,
		filters:    v.RightConditions,
	}

	if v.JoinType == causetcore.RightOuterJoin {
		e.innerBlock = leftBlock
		e.outerBlock = rightBlock
	} else {
		e.innerBlock = rightBlock
		e.outerBlock = leftBlock
	}
	e.innerBlock.isInner = true

	// optimizer should guarantee that filters on inner causet are pushed down
	// to einsteindb or extracted to a Selection.
	if len(e.innerBlock.filters) != 0 {
		b.err = errors.Annotate(ErrBuildInterlockingDirectorate, "merge join's inner filter should be empty.")
		return nil
	}

	interlockCounterMergeJoinInterDirc.Inc()
	return e
}

func (b *interlockBuilder) buildSideEstCount(v *causetcore.PhysicalHashJoin) float64 {
	buildSide := v.Children()[v.InnerChildIdx]
	if v.UseOuterToBuild {
		buildSide = v.Children()[1-v.InnerChildIdx]
	}
	if buildSide.Stats().HistDefCausl == nil || buildSide.Stats().HistDefCausl.Pseudo {
		return 0.0
	}
	return buildSide.StatsCount()
}

func (b *interlockBuilder) buildHashJoin(v *causetcore.PhysicalHashJoin) InterlockingDirectorate {
	leftInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}

	rightInterDirc := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}

	e := &HashJoinInterDirc{
		baseInterlockingDirectorate:    newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), leftInterDirc, rightInterDirc),
		concurrency:     v.Concurrency,
		joinType:        v.JoinType,
		isOuterJoin:     v.JoinType.IsOuterJoin(),
		useOuterToBuild: v.UseOuterToBuild,
	}
	defaultValues := v.DefaultValues
	lhsTypes, rhsTypes := retTypes(leftInterDirc), retTypes(rightInterDirc)
	if v.InnerChildIdx == 1 {
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildInterlockingDirectorate, "join's inner condition should be empty")
			return nil
		}
	} else {
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildInterlockingDirectorate, "join's inner condition should be empty")
			return nil
		}
	}

	// consider defCauslations
	leftTypes := make([]*types.FieldType, 0, len(retTypes(leftInterDirc)))
	for _, tp := range retTypes(leftInterDirc) {
		leftTypes = append(leftTypes, tp.Clone())
	}
	rightTypes := make([]*types.FieldType, 0, len(retTypes(rightInterDirc)))
	for _, tp := range retTypes(rightInterDirc) {
		rightTypes = append(rightTypes, tp.Clone())
	}
	leftIsBuildSide := true

	e.isNullEQ = v.IsNullEQ
	if v.UseOuterToBuild {
		// uFIDelate the buildSideEstCount due to changing the build side
		if v.InnerChildIdx == 1 {
			e.buildSideInterDirc, e.buildKeys = leftInterDirc, v.LeftJoinKeys
			e.probeSideInterDirc, e.probeKeys = rightInterDirc, v.RightJoinKeys
			e.outerFilter = v.LeftConditions
		} else {
			e.buildSideInterDirc, e.buildKeys = rightInterDirc, v.RightJoinKeys
			e.probeSideInterDirc, e.probeKeys = leftInterDirc, v.LeftJoinKeys
			e.outerFilter = v.RightConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Causet, e.probeSideInterDirc.Schema().Len())
		}
	} else {
		if v.InnerChildIdx == 0 {
			e.buildSideInterDirc, e.buildKeys = leftInterDirc, v.LeftJoinKeys
			e.probeSideInterDirc, e.probeKeys = rightInterDirc, v.RightJoinKeys
			e.outerFilter = v.RightConditions
		} else {
			e.buildSideInterDirc, e.buildKeys = rightInterDirc, v.RightJoinKeys
			e.probeSideInterDirc, e.probeKeys = leftInterDirc, v.LeftJoinKeys
			e.outerFilter = v.LeftConditions
			leftIsBuildSide = false
		}
		if defaultValues == nil {
			defaultValues = make([]types.Causet, e.buildSideInterDirc.Schema().Len())
		}
	}
	e.buildSideEstCount = b.buildSideEstCount(v)
	childrenUsedSchema := markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues,
			v.OtherConditions, lhsTypes, rhsTypes, childrenUsedSchema)
	}
	interlockCountHashJoinInterDirc.Inc()

	for i := range v.EqualConditions {
		chs, defCausl := v.EqualConditions[i].CharsetAndDefCauslation(e.ctx)
		bt := leftTypes[v.LeftJoinKeys[i].Index]
		bt.Charset, bt.DefCauslate = chs, defCausl
		pt := rightTypes[v.RightJoinKeys[i].Index]
		pt.Charset, pt.DefCauslate = chs, defCausl
	}
	if leftIsBuildSide {
		e.buildTypes, e.probeTypes = leftTypes, rightTypes
	} else {
		e.buildTypes, e.probeTypes = rightTypes, leftTypes
	}
	return e
}

func (b *interlockBuilder) buildHashAgg(v *causetcore.PhysicalHashAgg) InterlockingDirectorate {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	stochastikVars := b.ctx.GetStochastikVars()
	e := &HashAggInterDirc{
		baseInterlockingDirectorate:    newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), src),
		sc:              stochastikVars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
		GroupByItems:    v.GroupByItems,
	}
	// We take `create causet t(a int, b int);` as example.
	//
	// 1. If all the aggregation functions are FIRST_ROW, we do not need to set the defaultVal for them:
	// e.g.
	// allegrosql> select distinct a, b from t;
	// 0 rows in set (0.00 sec)
	//
	// 2. If there exists group by items, we do not need to set the defaultVal for them either:
	// e.g.
	// allegrosql> select avg(a) from t group by b;
	// Empty set (0.00 sec)
	//
	// allegrosql> select avg(a) from t group by a;
	// +--------+
	// | avg(a) |
	// +--------+
	// |  NULL  |
	// +--------+
	// 1 event in set (0.00 sec)
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstEvent(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
	}
	for _, aggDesc := range v.AggFuncs {
		if aggDesc.HasDistinct || len(aggDesc.OrderByItems) > 0 {
			e.isUnparallelInterDirc = true
		}
	}
	// When we set both milevadb_hashagg_final_concurrency and milevadb_hashagg_partial_concurrency to 1,
	// we do not need to parallelly execute hash agg,
	// and this action can be a workaround when meeting some unexpected situation using parallelInterDirc.
	if finalCon, partialCon := stochastikVars.HashAggFinalConcurrency(), stochastikVars.HashAggPartialConcurrency(); finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		e.isUnparallelInterDirc = true
	}
	partialOrdinal := 0
	for i, aggDesc := range v.AggFuncs {
		if e.isUnparallelInterDirc {
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(b.ctx, aggDesc, i))
		} else {
			ordinal := []int{partialOrdinal}
			partialOrdinal++
			if aggDesc.Name == ast.AggFuncAvg {
				ordinal = append(ordinal, partialOrdinal+1)
				partialOrdinal++
			}
			partialAggDesc, finalDesc := aggDesc.Split(ordinal)
			partialAggFunc := aggfuncs.Build(b.ctx, partialAggDesc, i)
			finalAggFunc := aggfuncs.Build(b.ctx, finalDesc, i)
			e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
			e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
			if partialAggDesc.Name == ast.AggFuncGroupConcat {
				// For group_concat, finalAggFunc and partialAggFunc need shared `truncate` flag to do duplicate.
				finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
					partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
				)
			}
		}
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendCauset(i, &value)
		}
	}

	interlockCounterHashAggInterDirc.Inc()
	return e
}

func (b *interlockBuilder) buildStreamAgg(v *causetcore.PhysicalStreamAgg) InterlockingDirectorate {
	src := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &StreamAggInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), src),
		groupChecker: newVecGroupChecker(b.ctx, v.GroupByItems),
		aggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.AggFuncs)),
	}
	if len(v.GroupByItems) != 0 || aggregation.IsAllFirstEvent(v.AggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(retTypes(e), 1)
	}
	for i, aggDesc := range v.AggFuncs {
		aggFunc := aggfuncs.Build(b.ctx, aggDesc, i)
		e.aggFuncs = append(e.aggFuncs, aggFunc)
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendCauset(i, &value)
		}
	}

	interlockStreamAggInterDirc.Inc()
	return e
}

func (b *interlockBuilder) buildSelection(v *causetcore.PhysicalSelection) InterlockingDirectorate {
	childInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &SelectionInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDirc),
		filters:      v.Conditions,
	}
	return e
}

func (b *interlockBuilder) buildProjection(v *causetcore.PhysicalProjection) InterlockingDirectorate {
	childInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	e := &ProjectionInterDirc{
		baseInterlockingDirectorate:     newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDirc),
		numWorkers:       int64(b.ctx.GetStochastikVars().ProjectionConcurrency()),
		evaluatorSuit:    memex.NewEvaluatorSuite(v.Exprs, v.AvoidDeferredCausetEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation event count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(b.ctx.GetStochastikVars().MaxChunkSize) {
		e.numWorkers = 0
	}
	return e
}

func (b *interlockBuilder) buildBlockDual(v *causetcore.PhysicalBlockDual) InterlockingDirectorate {
	if v.EventCount != 0 && v.EventCount != 1 {
		b.err = errors.Errorf("buildBlockDual failed, invalid event count for dual causet: %v", v.EventCount)
		return nil
	}
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())
	base.initCap = v.EventCount
	e := &BlockDualInterDirc{
		baseInterlockingDirectorate: base,
		numDualEvents:  v.EventCount,
	}
	return e
}

func (b *interlockBuilder) getSnapshotTS() (uint64, error) {
	if b.snapshotTS != 0 {
		// Return the cached value.
		return b.snapshotTS, nil
	}

	snapshotTS := b.ctx.GetStochastikVars().SnapshotTS
	txn, err := b.ctx.Txn(true)
	if err != nil {
		return 0, err
	}
	if snapshotTS == 0 {
		snapshotTS = txn.StartTS()
	}
	b.snapshotTS = snapshotTS
	if b.snapshotTS == 0 {
		return 0, errors.Trace(ErrGetStartTS)
	}
	return snapshotTS, nil
}

func (b *interlockBuilder) buildMemBlock(v *causetcore.PhysicalMemBlock) InterlockingDirectorate {
	switch v.DBName.L {
	case soliton.MetricSchemaName.L:
		return &MemBlockReaderInterDirc{
			baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
			causet:        v.Block,
			retriever: &MetricRetriever{
				causet:     v.Block,
				extractor: v.Extractor.(*causetcore.MetricBlockExtractor),
			},
		}
	case soliton.InformationSchemaName.L:
		switch v.Block.Name.L {
		case strings.ToLower(schemareplicant.BlockClusterConfig):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &clusterConfigRetriever{
					extractor: v.Extractor.(*causetcore.ClusterBlockExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterLoad):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*causetcore.ClusterBlockExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_LoadInfo,
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterHardware):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*causetcore.ClusterBlockExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_HardwareInfo,
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterSystemInfo):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &clusterServerInfoRetriever{
					extractor:      v.Extractor.(*causetcore.ClusterBlockExtractor),
					serverInfoType: diagnosticspb.ServerInfoType_SystemInfo,
				},
			}
		case strings.ToLower(schemareplicant.BlockClusterLog):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &clusterLogRetriever{
					extractor: v.Extractor.(*causetcore.ClusterLogBlockExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockInspectionResult):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &inspectionResultRetriever{
					extractor: v.Extractor.(*causetcore.InspectionResultBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockInspectionSummary):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &inspectionSummaryRetriever{
					causet:     v.Block,
					extractor: v.Extractor.(*causetcore.InspectionSummaryBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockInspectionMemrules):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &inspectionMemruleRetriever{
					extractor: v.Extractor.(*causetcore.InspectionMemruleBlockExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockMetricSummary):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &MetricsSummaryRetriever{
					causet:     v.Block,
					extractor: v.Extractor.(*causetcore.MetricSummaryBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockMetricSummaryByLabel):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &MetricsSummaryByLabelRetriever{
					causet:     v.Block,
					extractor: v.Extractor.(*causetcore.MetricSummaryBlockExtractor),
					timeRange: v.QueryTimeRange,
				},
			}
		case strings.ToLower(schemareplicant.BlockSchemata),
			strings.ToLower(schemareplicant.BlockStatistics),
			strings.ToLower(schemareplicant.BlockMilevaDBIndexes),
			strings.ToLower(schemareplicant.BlockViews),
			strings.ToLower(schemareplicant.BlockBlocks),
			strings.ToLower(schemareplicant.BlockSequences),
			strings.ToLower(schemareplicant.BlockPartitions),
			strings.ToLower(schemareplicant.BlockEngines),
			strings.ToLower(schemareplicant.BlockDefCauslations),
			strings.ToLower(schemareplicant.BlockAnalyzeStatus),
			strings.ToLower(schemareplicant.BlockClusterInfo),
			strings.ToLower(schemareplicant.BlockProfiling),
			strings.ToLower(schemareplicant.BlockCharacterSets),
			strings.ToLower(schemareplicant.BlockKeyDeferredCauset),
			strings.ToLower(schemareplicant.BlockUserPrivileges),
			strings.ToLower(schemareplicant.BlockMetricBlocks),
			strings.ToLower(schemareplicant.BlockDefCauslationCharacterSetApplicability),
			strings.ToLower(schemareplicant.BlockProcesslist),
			strings.ToLower(schemareplicant.ClusterBlockProcesslist),
			strings.ToLower(schemareplicant.BlockEinsteinDBRegionStatus),
			strings.ToLower(schemareplicant.BlockEinsteinDBRegionPeers),
			strings.ToLower(schemareplicant.BlockMilevaDBHotRegions),
			strings.ToLower(schemareplicant.BlockStochastikVar),
			strings.ToLower(schemareplicant.BlockConstraints),
			strings.ToLower(schemareplicant.BlockTiFlashReplica),
			strings.ToLower(schemareplicant.BlockMilevaDBServersInfo),
			strings.ToLower(schemareplicant.BlockEinsteinDBStoreStatus),
			strings.ToLower(schemareplicant.BlockStatementsSummary),
			strings.ToLower(schemareplicant.BlockStatementsSummaryHistory),
			strings.ToLower(schemareplicant.ClusterBlockStatementsSummary),
			strings.ToLower(schemareplicant.ClusterBlockStatementsSummaryHistory):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &memblockRetriever{
					causet:   v.Block,
					defCausumns: v.DeferredCausets,
				},
			}
		case strings.ToLower(schemareplicant.BlockDeferredCausets):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &hugeMemBlockRetriever{
					causet:   v.Block,
					defCausumns: v.DeferredCausets,
				},
			}

		case strings.ToLower(schemareplicant.BlockSlowQuery), strings.ToLower(schemareplicant.ClusterBlockSlowLog):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &slowQueryRetriever{
					causet:      v.Block,
					outputDefCauss: v.DeferredCausets,
					extractor:  v.Extractor.(*causetcore.SlowQueryExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockStorageStats):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &blockStorageStatsRetriever{
					causet:      v.Block,
					outputDefCauss: v.DeferredCausets,
					extractor:  v.Extractor.(*causetcore.BlockStorageStatsExtractor),
				},
			}
		case strings.ToLower(schemareplicant.BlockDBSJobs):
			return &DBSJobsReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				is:           b.is,
			}
		case strings.ToLower(schemareplicant.BlockTiFlashBlocks),
			strings.ToLower(schemareplicant.BlockTiFlashSegments):
			return &MemBlockReaderInterDirc{
				baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
				causet:        v.Block,
				retriever: &TiFlashSystemBlockRetriever{
					causet:      v.Block,
					outputDefCauss: v.DeferredCausets,
					extractor:  v.Extractor.(*causetcore.TiFlashSystemBlockExtractor),
				},
			}
		}
	}
	tb, _ := b.is.BlockByID(v.Block.ID)
	return &BlockScanInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		t:            tb,
		defCausumns:      v.DeferredCausets,
	}
}

func (b *interlockBuilder) buildSort(v *causetcore.PhysicalSort) InterlockingDirectorate {
	childInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortInterDirc := SortInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDirc),
		ByItems:      v.ByItems,
		schemaReplicant:       v.Schema(),
	}
	interlockCounterSortInterDirc.Inc()
	return &sortInterDirc
}

func (b *interlockBuilder) buildTopN(v *causetcore.PhysicalTopN) InterlockingDirectorate {
	childInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	sortInterDirc := SortInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDirc),
		ByItems:      v.ByItems,
		schemaReplicant:       v.Schema(),
	}
	interlockCounterTopNInterDirc.Inc()
	return &TopNInterDirc{
		SortInterDirc: sortInterDirc,
		limit:    &causetcore.PhysicalLimit{Count: v.Count, Offset: v.Offset},
	}
}

func (b *interlockBuilder) buildApply(v *causetcore.PhysicalApply) InterlockingDirectorate {
	var (
		innerCauset causetcore.PhysicalCauset
		outerCauset causetcore.PhysicalCauset
	)
	if v.InnerChildIdx == 0 {
		innerCauset = v.Children()[0]
		outerCauset = v.Children()[1]
	} else {
		innerCauset = v.Children()[1]
		outerCauset = v.Children()[0]
	}
	v.OuterSchema = causetcore.ExtractCorDeferredCausetsBySchema4PhysicalCauset(innerCauset, outerCauset.Schema())
	leftChild := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	rightChild := b.build(v.Children()[1])
	if b.err != nil {
		return nil
	}
	otherConditions := append(memex.ScalarFuncs2Exprs(v.EqualConditions), v.OtherConditions...)
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Causet, v.Children()[v.InnerChildIdx].Schema().Len())
	}
	outerInterDirc, innerInterDirc := leftChild, rightChild
	outerFilter, innerFilter := v.LeftConditions, v.RightConditions
	if v.InnerChildIdx == 0 {
		outerInterDirc, innerInterDirc = rightChild, leftChild
		outerFilter, innerFilter = v.RightConditions, v.LeftConditions
	}
	tupleJoiner := newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
		defaultValues, otherConditions, retTypes(leftChild), retTypes(rightChild), nil)
	serialInterDirc := &NestedLoopApplyInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), outerInterDirc, innerInterDirc),
		innerInterDirc:    innerInterDirc,
		outerInterDirc:    outerInterDirc,
		outerFilter:  outerFilter,
		innerFilter:  innerFilter,
		outer:        v.JoinType != causetcore.InnerJoin,
		joiner:       tupleJoiner,
		outerSchema:  v.OuterSchema,
		ctx:          b.ctx,
		canUseCache:  v.CanUseCache,
	}
	interlockCounterNestedLoopApplyInterDirc.Inc()

	// try parallel mode
	if v.Concurrency > 1 {
		innerInterDircs := make([]InterlockingDirectorate, 0, v.Concurrency)
		innerFilters := make([]memex.CNFExprs, 0, v.Concurrency)
		corDefCauss := make([][]*memex.CorrelatedDeferredCauset, 0, v.Concurrency)
		joiners := make([]joiner, 0, v.Concurrency)
		for i := 0; i < v.Concurrency; i++ {
			clonedInnerCauset, err := causetcore.SafeClone(innerCauset)
			if err != nil {
				b.err = nil
				return serialInterDirc
			}
			corDefCaus := causetcore.ExtractCorDeferredCausetsBySchema4PhysicalCauset(clonedInnerCauset, outerCauset.Schema())
			clonedInnerInterDirc := b.build(clonedInnerCauset)
			if b.err != nil {
				b.err = nil
				return serialInterDirc
			}
			innerInterDircs = append(innerInterDircs, clonedInnerInterDirc)
			corDefCauss = append(corDefCauss, corDefCaus)
			innerFilters = append(innerFilters, innerFilter.Clone())
			joiners = append(joiners, newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0,
				defaultValues, otherConditions, retTypes(leftChild), retTypes(rightChild), nil))
		}

		allInterDircs := append([]InterlockingDirectorate{outerInterDirc}, innerInterDircs...)

		return &ParallelNestedLoopApplyInterDirc{
			baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), allInterDircs...),
			innerInterDircs:   innerInterDircs,
			outerInterDirc:    outerInterDirc,
			outerFilter:  outerFilter,
			innerFilter:  innerFilters,
			outer:        v.JoinType != causetcore.InnerJoin,
			joiners:      joiners,
			corDefCauss:      corDefCauss,
			concurrency:  v.Concurrency,
			useCache:     true,
		}
	}
	return serialInterDirc
}

func (b *interlockBuilder) buildMaxOneEvent(v *causetcore.PhysicalMaxOneEvent) InterlockingDirectorate {
	childInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDirc)
	base.initCap = 2
	base.maxChunkSize = 2
	e := &MaxOneEventInterDirc{baseInterlockingDirectorate: base}
	return e
}

func (b *interlockBuilder) buildUnionAll(v *causetcore.PhysicalUnionAll) InterlockingDirectorate {
	childInterDircs := make([]InterlockingDirectorate, len(v.Children()))
	for i, child := range v.Children() {
		childInterDircs[i] = b.build(child)
		if b.err != nil {
			return nil
		}
	}
	e := &UnionInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDircs...),
		concurrency:  b.ctx.GetStochastikVars().UnionConcurrency(),
	}
	return e
}

func buildHandleDefCaussForSplit(sc *stmtctx.StatementContext, tbInfo *perceptron.BlockInfo) causetcore.HandleDefCauss {
	if tbInfo.IsCommonHandle {
		primaryIdx := blocks.FindPrimaryIndex(tbInfo)
		blockDefCauss := make([]*memex.DeferredCauset, len(tbInfo.DeferredCausets))
		for i, defCaus := range tbInfo.DeferredCausets {
			blockDefCauss[i] = &memex.DeferredCauset{
				ID:      defCaus.ID,
				RetType: &defCaus.FieldType,
			}
		}
		for i, pkDefCaus := range primaryIdx.DeferredCausets {
			blockDefCauss[pkDefCaus.Offset].Index = i
		}
		return causetcore.NewCommonHandleDefCauss(sc, tbInfo, primaryIdx, blockDefCauss)
	}
	intDefCaus := &memex.DeferredCauset{
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	return causetcore.NewIntHandleDefCauss(intDefCaus)
}

func (b *interlockBuilder) buildSplitRegion(v *causetcore.SplitRegion) InterlockingDirectorate {
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())
	base.initCap = 1
	base.maxChunkSize = 1
	if v.IndexInfo != nil {
		return &SplitIndexRegionInterDirc{
			baseInterlockingDirectorate:   base,
			blockInfo:      v.BlockInfo,
			partitionNames: v.PartitionNames,
			indexInfo:      v.IndexInfo,
			lower:          v.Lower,
			upper:          v.Upper,
			num:            v.Num,
			valueLists:     v.ValueLists,
		}
	}
	handleDefCauss := buildHandleDefCaussForSplit(b.ctx.GetStochastikVars().StmtCtx, v.BlockInfo)
	if len(v.ValueLists) > 0 {
		return &SplitBlockRegionInterDirc{
			baseInterlockingDirectorate:   base,
			blockInfo:      v.BlockInfo,
			partitionNames: v.PartitionNames,
			handleDefCauss:     handleDefCauss,
			valueLists:     v.ValueLists,
		}
	}
	return &SplitBlockRegionInterDirc{
		baseInterlockingDirectorate:   base,
		blockInfo:      v.BlockInfo,
		partitionNames: v.PartitionNames,
		handleDefCauss:     handleDefCauss,
		lower:          v.Lower,
		upper:          v.Upper,
		num:            v.Num,
	}
}

func (b *interlockBuilder) buildUFIDelate(v *causetcore.UFIDelate) InterlockingDirectorate {
	tblID2block := make(map[int64]causet.Block, len(v.TblDefCausPosInfos))
	for _, info := range v.TblDefCausPosInfos {
		tbl, _ := b.is.BlockByID(info.TblID)
		tblID2block[info.TblID] = tbl
		if len(v.PartitionedBlock) > 0 {
			// The v.PartitionedBlock defCauslects the partitioned causet.
			// Replace the original causet with the partitioned causet to support partition selection.
			// e.g. uFIDelate t partition (p0, p1), the new values are not belong to the given set p0, p1
			// Using the causet in v.PartitionedBlock returns a proper error, while using the original causet can't.
			for _, p := range v.PartitionedBlock {
				if info.TblID == p.Meta().ID {
					tblID2block[info.TblID] = p
				}
			}
		}
	}
	if b.err = b.uFIDelateForUFIDelateTSIfNeeded(v.SelectCauset); b.err != nil {
		return nil
	}
	b.snapshotTS = b.ctx.GetStochastikVars().TxnCtx.GetForUFIDelateTS()
	selInterDirc := b.build(v.SelectCauset)
	if b.err != nil {
		return nil
	}
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), selInterDirc)
	base.initCap = chunk.ZeroCapacity
	uFIDelateInterDirc := &UFIDelateInterDirc{
		baseInterlockingDirectorate:              base,
		OrderedList:               v.OrderedList,
		allAssignmentsAreConstant: v.AllAssignmentsAreConstant,
		tblID2block:               tblID2block,
		tblDefCausPosInfos:            v.TblDefCausPosInfos,
	}
	return uFIDelateInterDirc
}

func (b *interlockBuilder) buildDelete(v *causetcore.Delete) InterlockingDirectorate {
	tblID2block := make(map[int64]causet.Block, len(v.TblDefCausPosInfos))
	for _, info := range v.TblDefCausPosInfos {
		tblID2block[info.TblID], _ = b.is.BlockByID(info.TblID)
	}
	if b.err = b.uFIDelateForUFIDelateTSIfNeeded(v.SelectCauset); b.err != nil {
		return nil
	}
	b.snapshotTS = b.ctx.GetStochastikVars().TxnCtx.GetForUFIDelateTS()
	selInterDirc := b.build(v.SelectCauset)
	if b.err != nil {
		return nil
	}
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), selInterDirc)
	base.initCap = chunk.ZeroCapacity
	deleteInterDirc := &DeleteInterDirc{
		baseInterlockingDirectorate:   base,
		tblID2Block:    tblID2block,
		IsMultiBlock:   v.IsMultiBlock,
		tblDefCausPosInfos: v.TblDefCausPosInfos,
	}
	return deleteInterDirc
}

// uFIDelateForUFIDelateTSIfNeeded uFIDelates the ForUFIDelateTS for a pessimistic transaction if needed.
// PointGet interlock will get conflict error if the ForUFIDelateTS is older than the latest commitTS,
// so we don't need to uFIDelate now for better latency.
func (b *interlockBuilder) uFIDelateForUFIDelateTSIfNeeded(selectCauset causetcore.PhysicalCauset) error {
	txnCtx := b.ctx.GetStochastikVars().TxnCtx
	if !txnCtx.IsPessimistic {
		return nil
	}
	if _, ok := selectCauset.(*causetcore.PointGetCauset); ok {
		return nil
	}
	// Activate the invalid txn, use the txn startTS as newForUFIDelateTS
	txn, err := b.ctx.Txn(false)
	if err != nil {
		return err
	}
	if !txn.Valid() {
		_, err := b.ctx.Txn(true)
		if err != nil {
			return err
		}
		return nil
	}
	// The Repeablock Read transaction use Read Committed level to read data for writing (insert, uFIDelate, delete, select for uFIDelate),
	// We should always uFIDelate/refresh the for-uFIDelate-ts no matter the isolation level is RR or RC.
	if b.ctx.GetStochastikVars().IsPessimisticReadConsistency() {
		return b.refreshForUFIDelateTSForRC()
	}
	return UFIDelateForUFIDelateTS(b.ctx, 0)
}

// refreshForUFIDelateTSForRC is used to refresh the for-uFIDelate-ts for reading data at read consistency level in pessimistic transaction.
// It could use the cached tso from the memex future to avoid get tso many times.
func (b *interlockBuilder) refreshForUFIDelateTSForRC() error {
	defer func() {
		b.snapshotTS = b.ctx.GetStochastikVars().TxnCtx.GetForUFIDelateTS()
	}()
	future := b.ctx.GetStochastikVars().TxnCtx.GetStmtFutureForRC()
	if future == nil {
		return nil
	}
	newForUFIDelateTS, waitErr := future.Wait()
	if waitErr != nil {
		logutil.BgLogger().Warn("wait tso failed",
			zap.Uint64("startTS", b.ctx.GetStochastikVars().TxnCtx.StartTS),
			zap.Error(waitErr))
	}
	b.ctx.GetStochastikVars().TxnCtx.SetStmtFutureForRC(nil)
	// If newForUFIDelateTS is 0, it will force to get a new for-uFIDelate-ts from FIDel.
	return UFIDelateForUFIDelateTS(b.ctx, newForUFIDelateTS)
}

func (b *interlockBuilder) buildAnalyzeIndexPushdown(task causetcore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64, autoAnalyze string) *analyzeTask {
	_, offset := timeutil.Zone(b.ctx.GetStochastikVars().Location())
	sc := b.ctx.GetStochastikVars().StmtCtx
	e := &AnalyzeIndexInterDirc{
		ctx:            b.ctx,
		blockID:        task.BlockID,
		isCommonHandle: task.TblInfo.IsCommonHandle,
		idxInfo:        task.IndexInfo,
		concurrency:    b.ctx.GetStochastikVars().IndexSerialScanConcurrency(),
		analyzePB: &fidelpb.AnalyzeReq{
			Tp:             fidelpb.AnalyzeType_TypeIndex,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts: opts,
	}
	e.analyzePB.IdxReq = &fidelpb.AnalyzeIndexReq{
		BucketSize: int64(opts[ast.AnalyzeOptNumBuckets]),
		NumDeferredCausets: int32(len(task.IndexInfo.DeferredCausets)),
	}
	if e.isCommonHandle && e.idxInfo.Primary {
		e.analyzePB.Tp = fidelpb.AnalyzeType_TypeCommonHandle
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.IdxReq.CmsketchDepth = &depth
	e.analyzePB.IdxReq.CmsketchWidth = &width
	job := &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze index " + task.IndexInfo.Name.O}
	return &analyzeTask{taskType: idxTask, idxInterDirc: e, job: job}
}

func (b *interlockBuilder) buildAnalyzeIndexIncremental(task causetcore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64) *analyzeTask {
	h := petri.GetPetri(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&perceptron.BlockInfo{}, task.BlockID.PersistID)
	analyzeTask := b.buildAnalyzeIndexPushdown(task, opts, "")
	if statsTbl.Pseudo {
		return analyzeTask
	}
	idx, ok := statsTbl.Indices[task.IndexInfo.ID]
	if !ok || idx.Len() == 0 || idx.LastAnalyzePos.IsNull() {
		return analyzeTask
	}
	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(idx.Flag) {
		exec := analyzeTask.idxInterDirc
		if idx.CMSketch != nil {
			width, depth := idx.CMSketch.GetWidthAndDepth()
			exec.analyzePB.IdxReq.CmsketchWidth = &width
			exec.analyzePB.IdxReq.CmsketchDepth = &depth
		}
		oldHist = idx.Histogram.Copy()
	} else {
		_, bktID := idx.LessEventCountWithBktIdx(idx.LastAnalyzePos)
		if bktID == 0 {
			return analyzeTask
		}
		oldHist = idx.TruncateHistogram(bktID)
	}
	oldHist = oldHist.RemoveUpperBound()
	analyzeTask.taskType = idxIncrementalTask
	analyzeTask.idxIncrementalInterDirc = &analyzeIndexIncrementalInterDirc{AnalyzeIndexInterDirc: *analyzeTask.idxInterDirc, oldHist: oldHist, oldCMS: idx.CMSketch}
	analyzeTask.job = &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: "analyze incremental index " + task.IndexInfo.Name.O}
	return analyzeTask
}

func (b *interlockBuilder) buildAnalyzeDeferredCausetsPushdown(task causetcore.AnalyzeDeferredCausetsTask, opts map[ast.AnalyzeOptionType]uint64, autoAnalyze string) *analyzeTask {
	defcaus := task.DefCaussInfo
	if hasPkHist(task.HandleDefCauss) {
		defCausInfo := task.TblInfo.DeferredCausets[task.HandleDefCauss.GetDefCaus(0).Index]
		defcaus = append([]*perceptron.DeferredCausetInfo{defCausInfo}, defcaus...)
	} else if task.HandleDefCauss != nil && !task.HandleDefCauss.IsInt() {
		defcaus = make([]*perceptron.DeferredCausetInfo, 0, len(task.DefCaussInfo)+task.HandleDefCauss.NumDefCauss())
		for i := 0; i < task.HandleDefCauss.NumDefCauss(); i++ {
			defcaus = append(defcaus, task.TblInfo.DeferredCausets[task.HandleDefCauss.GetDefCaus(i).Index])
		}
		defcaus = append(defcaus, task.DefCaussInfo...)
		task.DefCaussInfo = defcaus
	}

	_, offset := timeutil.Zone(b.ctx.GetStochastikVars().Location())
	sc := b.ctx.GetStochastikVars().StmtCtx
	e := &AnalyzeDeferredCausetsInterDirc{
		ctx:         b.ctx,
		blockID:     task.BlockID,
		defcausInfo:    task.DefCaussInfo,
		handleDefCauss:  task.HandleDefCauss,
		concurrency: b.ctx.GetStochastikVars().DistALLEGROSQLScanConcurrency(),
		analyzePB: &fidelpb.AnalyzeReq{
			Tp:             fidelpb.AnalyzeType_TypeDeferredCauset,
			Flags:          sc.PushDownFlags(),
			TimeZoneOffset: offset,
		},
		opts: opts,
	}
	depth := int32(opts[ast.AnalyzeOptCMSketchDepth])
	width := int32(opts[ast.AnalyzeOptCMSketchWidth])
	e.analyzePB.DefCausReq = &fidelpb.AnalyzeDeferredCausetsReq{
		BucketSize:    int64(opts[ast.AnalyzeOptNumBuckets]),
		SampleSize:    maxRegionSampleSize,
		SketchSize:    maxSketchSize,
		DeferredCausetsInfo:   soliton.DeferredCausetsToProto(defcaus, task.HandleDefCauss != nil && task.HandleDefCauss.IsInt()),
		CmsketchDepth: &depth,
		CmsketchWidth: &width,
	}
	if task.TblInfo != nil {
		e.analyzePB.DefCausReq.PrimaryDeferredCausetIds = blocks.TryGetCommonPkDeferredCausetIds(task.TblInfo)
	}
	b.err = causetcore.SetPBDeferredCausetsDefaultValue(b.ctx, e.analyzePB.DefCausReq.DeferredCausetsInfo, defcaus)
	job := &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: autoAnalyze + "analyze defCausumns"}
	return &analyzeTask{taskType: defCausTask, defCausInterDirc: e, job: job}
}

func (b *interlockBuilder) buildAnalyzePKIncremental(task causetcore.AnalyzeDeferredCausetsTask, opts map[ast.AnalyzeOptionType]uint64) *analyzeTask {
	h := petri.GetPetri(b.ctx).StatsHandle()
	statsTbl := h.GetPartitionStats(&perceptron.BlockInfo{}, task.BlockID.PersistID)
	analyzeTask := b.buildAnalyzeDeferredCausetsPushdown(task, opts, "")
	if statsTbl.Pseudo {
		return analyzeTask
	}
	if task.HandleDefCauss == nil || !task.HandleDefCauss.IsInt() {
		return analyzeTask
	}
	defCaus, ok := statsTbl.DeferredCausets[task.HandleDefCauss.GetDefCaus(0).ID]
	if !ok || defCaus.Len() == 0 || defCaus.LastAnalyzePos.IsNull() {
		return analyzeTask
	}
	var oldHist *statistics.Histogram
	if statistics.IsAnalyzed(defCaus.Flag) {
		oldHist = defCaus.Histogram.Copy()
	} else {
		d, err := defCaus.LastAnalyzePos.ConvertTo(b.ctx.GetStochastikVars().StmtCtx, defCaus.Tp)
		if err != nil {
			b.err = err
			return nil
		}
		_, bktID := defCaus.LessEventCountWithBktIdx(d)
		if bktID == 0 {
			return analyzeTask
		}
		oldHist = defCaus.TruncateHistogram(bktID)
		oldHist.NDV = int64(oldHist.TotalEventCount())
	}
	exec := analyzeTask.defCausInterDirc
	analyzeTask.taskType = pkIncrementalTask
	analyzeTask.defCausIncrementalInterDirc = &analyzePKIncrementalInterDirc{AnalyzeDeferredCausetsInterDirc: *exec, oldHist: oldHist}
	analyzeTask.job = &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: "analyze incremental primary key"}
	return analyzeTask
}

func (b *interlockBuilder) buildAnalyzeFastDeferredCauset(e *AnalyzeInterDirc, task causetcore.AnalyzeDeferredCausetsTask, opts map[ast.AnalyzeOptionType]uint64) {
	findTask := false
	for _, eTask := range e.tasks {
		if eTask.fastInterDirc != nil && eTask.fastInterDirc.blockID.Equals(&task.BlockID) {
			eTask.fastInterDirc.defcausInfo = append(eTask.fastInterDirc.defcausInfo, task.DefCaussInfo...)
			findTask = true
			break
		}
	}
	if !findTask {
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			return
		}
		fastInterDirc := &AnalyzeFastInterDirc{
			ctx:         b.ctx,
			blockID:     task.BlockID,
			defcausInfo:    task.DefCaussInfo,
			handleDefCauss:  task.HandleDefCauss,
			opts:        opts,
			tblInfo:     task.TblInfo,
			concurrency: concurrency,
			wg:          &sync.WaitGroup{},
		}
		b.err = fastInterDirc.calculateEstimateSampleStep()
		if b.err != nil {
			return
		}
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastInterDirc: fastInterDirc,
			job:      &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: task.PartitionName, JobInfo: "fast analyze defCausumns"},
		})
	}
}

func (b *interlockBuilder) buildAnalyzeFastIndex(e *AnalyzeInterDirc, task causetcore.AnalyzeIndexTask, opts map[ast.AnalyzeOptionType]uint64) {
	findTask := false
	for _, eTask := range e.tasks {
		if eTask.fastInterDirc != nil && eTask.fastInterDirc.blockID.Equals(&task.BlockID) {
			eTask.fastInterDirc.idxsInfo = append(eTask.fastInterDirc.idxsInfo, task.IndexInfo)
			findTask = true
			break
		}
	}
	if !findTask {
		var concurrency int
		concurrency, b.err = getBuildStatsConcurrency(e.ctx)
		if b.err != nil {
			return
		}
		fastInterDirc := &AnalyzeFastInterDirc{
			ctx:         b.ctx,
			blockID:     task.BlockID,
			idxsInfo:    []*perceptron.IndexInfo{task.IndexInfo},
			opts:        opts,
			tblInfo:     task.TblInfo,
			concurrency: concurrency,
			wg:          &sync.WaitGroup{},
		}
		b.err = fastInterDirc.calculateEstimateSampleStep()
		if b.err != nil {
			return
		}
		e.tasks = append(e.tasks, &analyzeTask{
			taskType: fastTask,
			fastInterDirc: fastInterDirc,
			job:      &statistics.AnalyzeJob{DBName: task.DBName, BlockName: task.BlockName, PartitionName: "fast analyze index " + task.IndexInfo.Name.O},
		})
	}
}

func (b *interlockBuilder) buildAnalyze(v *causetcore.Analyze) InterlockingDirectorate {
	e := &AnalyzeInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		tasks:        make([]*analyzeTask, 0, len(v.DefCausTasks)+len(v.IdxTasks)),
		wg:           &sync.WaitGroup{},
	}
	enableFastAnalyze := b.ctx.GetStochastikVars().EnableFastAnalyze
	autoAnalyze := ""
	if b.ctx.GetStochastikVars().InRestrictedALLEGROSQL {
		autoAnalyze = "auto "
	}
	for _, task := range v.DefCausTasks {
		if task.Incremental {
			e.tasks = append(e.tasks, b.buildAnalyzePKIncremental(task, v.Opts))
		} else {
			if enableFastAnalyze {
				b.buildAnalyzeFastDeferredCauset(e, task, v.Opts)
			} else {
				e.tasks = append(e.tasks, b.buildAnalyzeDeferredCausetsPushdown(task, v.Opts, autoAnalyze))
			}
		}
		if b.err != nil {
			return nil
		}
	}
	for _, task := range v.IdxTasks {
		if task.Incremental {
			e.tasks = append(e.tasks, b.buildAnalyzeIndexIncremental(task, v.Opts))
		} else {
			if enableFastAnalyze {
				b.buildAnalyzeFastIndex(e, task, v.Opts)
			} else {
				e.tasks = append(e.tasks, b.buildAnalyzeIndexPushdown(task, v.Opts, autoAnalyze))
			}
		}
		if b.err != nil {
			return nil
		}
	}
	return e
}

func constructDistInterDirc(sctx stochastikctx.Context, plans []causetcore.PhysicalCauset) ([]*fidelpb.InterlockingDirectorate, bool, error) {
	streaming := true
	interlocks := make([]*fidelpb.InterlockingDirectorate, 0, len(plans))
	for _, p := range plans {
		execPB, err := p.ToPB(sctx, ekv.EinsteinDB)
		if err != nil {
			return nil, false, err
		}
		if !causetcore.SupportStreaming(p) {
			streaming = false
		}
		interlocks = append(interlocks, execPB)
	}
	return interlocks, streaming, nil
}

// markChildrenUsedDefCauss compares each child with the output schemaReplicant, and mark
// each defCausumn of the child is used by output or not.
func markChildrenUsedDefCauss(outputSchema *memex.Schema, childSchema ...*memex.Schema) (childrenUsed [][]bool) {
	for _, child := range childSchema {
		used := memex.GetUsedList(outputSchema.DeferredCausets, child)
		childrenUsed = append(childrenUsed, used)
	}
	return
}

func constructDistInterDircForTiFlash(sctx stochastikctx.Context, p causetcore.PhysicalCauset) ([]*fidelpb.InterlockingDirectorate, bool, error) {
	execPB, err := p.ToPB(sctx, ekv.TiFlash)
	return []*fidelpb.InterlockingDirectorate{execPB}, false, err

}

func (b *interlockBuilder) constructPosetDagReq(plans []causetcore.PhysicalCauset, storeType ekv.StoreType) (posetPosetDagReq *fidelpb.PosetDagRequest, streaming bool, err error) {
	posetPosetDagReq = &fidelpb.PosetDagRequest{}
	posetPosetDagReq.TimeZoneName, posetPosetDagReq.TimeZoneOffset = timeutil.Zone(b.ctx.GetStochastikVars().Location())
	sc := b.ctx.GetStochastikVars().StmtCtx
	if sc.RuntimeStatsDefCausl != nil {
		defCauslInterDirc := true
		posetPosetDagReq.DefCauslectInterDircutionSummaries = &defCauslInterDirc
	}
	posetPosetDagReq.Flags = sc.PushDownFlags()
	if storeType == ekv.TiFlash {
		var interlocks []*fidelpb.InterlockingDirectorate
		interlocks, streaming, err = constructDistInterDircForTiFlash(b.ctx, plans[0])
		posetPosetDagReq.RootInterlockingDirectorate = interlocks[0]
	} else {
		posetPosetDagReq.InterlockingDirectorates, streaming, err = constructDistInterDirc(b.ctx, plans)
	}

	allegrosql.SetEncodeType(b.ctx, posetPosetDagReq)
	return posetPosetDagReq, streaming, err
}

func (b *interlockBuilder) corDefCausInDistCauset(plans []causetcore.PhysicalCauset) bool {
	for _, p := range plans {
		x, ok := p.(*causetcore.PhysicalSelection)
		if !ok {
			continue
		}
		for _, cond := range x.Conditions {
			if len(memex.ExtractCorDeferredCausets(cond)) > 0 {
				return true
			}
		}
	}
	return false
}

// corDefCausInAccess checks whether there's correlated defCausumn in access conditions.
func (b *interlockBuilder) corDefCausInAccess(p causetcore.PhysicalCauset) bool {
	var access []memex.Expression
	switch x := p.(type) {
	case *causetcore.PhysicalBlockScan:
		access = x.AccessCondition
	case *causetcore.PhysicalIndexScan:
		access = x.AccessCondition
	}
	for _, cond := range access {
		if len(memex.ExtractCorDeferredCausets(cond)) > 0 {
			return true
		}
	}
	return false
}

func (b *interlockBuilder) buildIndexLookUpJoin(v *causetcore.PhysicalIndexJoin) InterlockingDirectorate {
	outerInterDirc := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := retTypes(outerInterDirc)
	innerCauset := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerCauset.Schema().Len())
	for i, defCaus := range innerCauset.Schema().DeferredCausets {
		innerTypes[i] = defCaus.RetType
	}

	var (
		outerFilter           []memex.Expression
		leftTypes, rightTypes []*types.FieldType
	)

	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildInterlockingDirectorate, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildInterlockingDirectorate, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Causet, len(innerTypes))
	}
	hasPrefixDefCaus := false
	for _, l := range v.IdxDefCausLens {
		if l != types.UnspecifiedLength {
			hasPrefixDefCaus = true
			break
		}
	}
	e := &IndexLookUpJoin{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), outerInterDirc),
		outerCtx: outerCtx{
			rowTypes: outerTypes,
			filter:   outerFilter,
		},
		innerCtx: innerCtx{
			readerBuilder: &dataReaderBuilder{Causet: innerCauset, interlockBuilder: b},
			rowTypes:      innerTypes,
			defCausLens:       v.IdxDefCausLens,
			hasPrefixDefCaus:  hasPrefixDefCaus,
		},
		workerWg:      new(sync.WaitGroup),
		isOuterJoin:   v.JoinType.IsOuterJoin(),
		indexRanges:   v.Ranges,
		keyOff2IdxOff: v.KeyOff2IdxOff,
		lastDefCausHelper: v.CompareFilters,
	}
	childrenUsedSchema := markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	e.joiner = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema)
	outerKeyDefCauss := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		outerKeyDefCauss[i] = v.OuterJoinKeys[i].Index
	}
	e.outerCtx.keyDefCauss = outerKeyDefCauss
	innerKeyDefCauss := make([]int, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		innerKeyDefCauss[i] = v.InnerJoinKeys[i].Index
	}
	e.innerCtx.keyDefCauss = innerKeyDefCauss
	e.joinResult = newFirstChunk(e)
	interlockCounterIndexLookUpJoin.Inc()
	return e
}

func (b *interlockBuilder) buildIndexLookUpMergeJoin(v *causetcore.PhysicalIndexMergeJoin) InterlockingDirectorate {
	outerInterDirc := b.build(v.Children()[1-v.InnerChildIdx])
	if b.err != nil {
		return nil
	}
	outerTypes := retTypes(outerInterDirc)
	innerCauset := v.Children()[v.InnerChildIdx]
	innerTypes := make([]*types.FieldType, innerCauset.Schema().Len())
	for i, defCaus := range innerCauset.Schema().DeferredCausets {
		innerTypes[i] = defCaus.RetType
	}
	var (
		outerFilter           []memex.Expression
		leftTypes, rightTypes []*types.FieldType
	)
	if v.InnerChildIdx == 0 {
		leftTypes, rightTypes = innerTypes, outerTypes
		outerFilter = v.RightConditions
		if len(v.LeftConditions) > 0 {
			b.err = errors.Annotate(ErrBuildInterlockingDirectorate, "join's inner condition should be empty")
			return nil
		}
	} else {
		leftTypes, rightTypes = outerTypes, innerTypes
		outerFilter = v.LeftConditions
		if len(v.RightConditions) > 0 {
			b.err = errors.Annotate(ErrBuildInterlockingDirectorate, "join's inner condition should be empty")
			return nil
		}
	}
	defaultValues := v.DefaultValues
	if defaultValues == nil {
		defaultValues = make([]types.Causet, len(innerTypes))
	}
	outerKeyDefCauss := make([]int, len(v.OuterJoinKeys))
	for i := 0; i < len(v.OuterJoinKeys); i++ {
		outerKeyDefCauss[i] = v.OuterJoinKeys[i].Index
	}
	innerKeyDefCauss := make([]int, len(v.InnerJoinKeys))
	for i := 0; i < len(v.InnerJoinKeys); i++ {
		innerKeyDefCauss[i] = v.InnerJoinKeys[i].Index
	}
	interlockCounterIndexLookUpJoin.Inc()

	e := &IndexLookUpMergeJoin{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), outerInterDirc),
		outerMergeCtx: outerMergeCtx{
			rowTypes:      outerTypes,
			filter:        outerFilter,
			joinKeys:      v.OuterJoinKeys,
			keyDefCauss:       outerKeyDefCauss,
			needOuterSort: v.NeedOuterSort,
			compareFuncs:  v.OuterCompareFuncs,
		},
		innerMergeCtx: innerMergeCtx{
			readerBuilder:           &dataReaderBuilder{Causet: innerCauset, interlockBuilder: b},
			rowTypes:                innerTypes,
			joinKeys:                v.InnerJoinKeys,
			keyDefCauss:                 innerKeyDefCauss,
			compareFuncs:            v.CompareFuncs,
			defCausLens:                 v.IdxDefCausLens,
			desc:                    v.Desc,
			keyOff2KeyOffOrderByIdx: v.KeyOff2KeyOffOrderByIdx,
		},
		workerWg:      new(sync.WaitGroup),
		isOuterJoin:   v.JoinType.IsOuterJoin(),
		indexRanges:   v.Ranges,
		keyOff2IdxOff: v.KeyOff2IdxOff,
		lastDefCausHelper: v.CompareFilters,
	}
	childrenUsedSchema := markChildrenUsedDefCauss(v.Schema(), v.Children()[0].Schema(), v.Children()[1].Schema())
	joiners := make([]joiner, e.ctx.GetStochastikVars().IndexLookupJoinConcurrency())
	for i := 0; i < len(joiners); i++ {
		joiners[i] = newJoiner(b.ctx, v.JoinType, v.InnerChildIdx == 0, defaultValues, v.OtherConditions, leftTypes, rightTypes, childrenUsedSchema)
	}
	e.joiners = joiners
	return e
}

func (b *interlockBuilder) buildIndexNestedLoopHashJoin(v *causetcore.PhysicalIndexHashJoin) InterlockingDirectorate {
	e := b.buildIndexLookUpJoin(&(v.PhysicalIndexJoin)).(*IndexLookUpJoin)
	idxHash := &IndexNestedLoopHashJoin{
		IndexLookUpJoin: *e,
		keepOuterOrder:  v.KeepOuterOrder,
	}
	concurrency := e.ctx.GetStochastikVars().IndexLookupJoinConcurrency()
	idxHash.joiners = make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.joiners[i] = e.joiner.Clone()
	}
	return idxHash
}

// containsLimit tests if the execs contains Limit because we do not know whether `Limit` has consumed all of its' source,
// so the feedback may not be accurate.
func containsLimit(execs []*fidelpb.InterlockingDirectorate) bool {
	for _, exec := range execs {
		if exec.Limit != nil {
			return true
		}
	}
	return false
}

// When allow batch cop is 1, only agg / topN uses batch cop.
// When allow batch cop is 2, every query uses batch cop.
func (e *BlockReaderInterlockingDirectorate) setBatchCop(v *causetcore.PhysicalBlockReader) {
	if e.storeType != ekv.TiFlash || e.keepOrder {
		return
	}
	switch e.ctx.GetStochastikVars().AllowBatchCop {
	case 1:
		for _, p := range v.BlockCausets {
			switch p.(type) {
			case *causetcore.PhysicalHashAgg, *causetcore.PhysicalStreamAgg, *causetcore.PhysicalTopN, *causetcore.PhysicalBroadCastJoin:
				e.batchCop = true
			}
		}
	case 2:
		e.batchCop = true
	}
	return
}

func buildNoRangeBlockReader(b *interlockBuilder, v *causetcore.PhysicalBlockReader) (*BlockReaderInterlockingDirectorate, error) {
	blockCausets := v.BlockCausets
	if v.StoreType == ekv.TiFlash {
		blockCausets = []causetcore.PhysicalCauset{v.GetBlockCauset()}
	}
	posetPosetDagReq, streaming, err := b.constructPosetDagReq(blockCausets, v.StoreType)
	if err != nil {
		return nil, err
	}
	ts := v.GetBlockScan()
	tbl, _ := b.is.BlockByID(ts.Block.ID)
	isPartition, physicalBlockID := ts.IsPartition()
	if isPartition {
		pt := tbl.(causet.PartitionedBlock)
		tbl = pt.GetPartition(physicalBlockID)
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &BlockReaderInterlockingDirectorate{
		baseInterlockingDirectorate:   newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPB:          posetPosetDagReq,
		startTS:        startTS,
		causet:          tbl,
		keepOrder:      ts.KeepOrder,
		desc:           ts.Desc,
		defCausumns:        ts.DeferredCausets,
		streaming:      streaming,
		corDefCausInFilter: b.corDefCausInDistCauset(v.BlockCausets),
		corDefCausInAccess: b.corDefCausInAccess(v.BlockCausets[0]),
		plans:          v.BlockCausets,
		blockCauset:      v.GetBlockCauset(),
		storeType:      v.StoreType,
	}
	e.setBatchCop(v)
	e.buildVirtualDeferredCausetInfo()
	if containsLimit(posetPosetDagReq.InterlockingDirectorates) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(getPhysicalBlockID(tbl), ts.Hist, int64(ts.StatsCount()), ts.Desc)
	}
	defCauslect := statistics.DefCauslectFeedback(b.ctx.GetStochastikVars().StmtCtx, e.feedback, len(ts.Ranges))
	if !defCauslect {
		e.feedback.Invalidate()
	}
	e.posetPosetDagPB.DefCauslectRangeCounts = &defCauslect
	if v.StoreType == ekv.MilevaDB && b.ctx.GetStochastikVars().User != nil {
		// User info is used to do privilege check. It is only used in MilevaDB cluster memory causet.
		e.posetPosetDagPB.User = &fidelpb.UserIdentity{
			UserName: b.ctx.GetStochastikVars().User.Username,
			UserHost: b.ctx.GetStochastikVars().User.Hostname,
		}
	}

	for i := range v.Schema().DeferredCausets {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(i))
	}

	return e, nil
}

// buildBlockReader builds a causet reader interlock. It first build a no range causet reader,
// and then uFIDelate it ranges from causet scan plan.
func (b *interlockBuilder) buildBlockReader(v *causetcore.PhysicalBlockReader) InterlockingDirectorate {
	if b.ctx.GetStochastikVars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	ret, err := buildNoRangeBlockReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	ts := v.GetBlockScan()
	ret.ranges = ts.Ranges
	sctx := b.ctx.GetStochastikVars().StmtCtx
	sctx.BlockIDs = append(sctx.BlockIDs, ts.Block.ID)

	if !b.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	if v.StoreType == ekv.TiFlash {
		tmp, _ := b.is.BlockByID(ts.Block.ID)
		tbl := tmp.(causet.PartitionedBlock)
		partitions, err := partitionPruning(b.ctx, tbl, v.PartitionInfo.PruningConds, v.PartitionInfo.PartitionNames, v.PartitionInfo.DeferredCausets, v.PartitionInfo.DeferredCausetNames)
		if err != nil {
			b.err = err
			return nil
		}
		partsInterlockingDirectorate := make([]InterlockingDirectorate, 0, len(partitions))
		for _, part := range partitions {
			exec, err := buildNoRangeBlockReader(b, v)
			if err != nil {
				b.err = err
				return nil
			}
			exec.ranges = ts.Ranges
			nexec, err := nextPartitionForBlockReader{exec: exec}.nextPartition(context.Background(), part)
			if err != nil {
				b.err = err
				return nil
			}
			partsInterlockingDirectorate = append(partsInterlockingDirectorate, nexec)
		}
		if len(partsInterlockingDirectorate) == 0 {
			return &BlockDualInterDirc{baseInterlockingDirectorate: *ret.base()}
		}
		if len(partsInterlockingDirectorate) == 1 {
			return partsInterlockingDirectorate[0]
		}
		return &UnionInterDirc{
			baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), partsInterlockingDirectorate...),
			concurrency:  b.ctx.GetStochastikVars().UnionConcurrency(),
		}
	}

	nextPartition := nextPartitionForBlockReader{ret}
	exec, err := buildPartitionBlock(b, ts.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
		return nil
	}
	return exec
}

func buildPartitionBlock(b *interlockBuilder, tblInfo *perceptron.BlockInfo, partitionInfo *causetcore.PartitionInfo, e InterlockingDirectorate, n nextPartition) (InterlockingDirectorate, error) {
	tmp, _ := b.is.BlockByID(tblInfo.ID)
	tbl := tmp.(causet.PartitionedBlock)
	partitions, err := partitionPruning(b.ctx, tbl, partitionInfo.PruningConds, partitionInfo.PartitionNames, partitionInfo.DeferredCausets, partitionInfo.DeferredCausetNames)
	if err != nil {
		return nil, err
	}

	if len(partitions) == 0 {
		return &BlockDualInterDirc{baseInterlockingDirectorate: *e.base()}, nil
	}
	return &PartitionBlockInterlockingDirectorate{
		baseInterlockingDirectorate:  *e.base(),
		partitions:    partitions,
		nextPartition: n,
	}, nil
}

func buildNoRangeIndexReader(b *interlockBuilder, v *causetcore.PhysicalIndexReader) (*IndexReaderInterlockingDirectorate, error) {
	posetPosetDagReq, streaming, err := b.constructPosetDagReq(v.IndexCausets, ekv.EinsteinDB)
	if err != nil {
		return nil, err
	}
	is := v.IndexCausets[0].(*causetcore.PhysicalIndexScan)
	tbl, _ := b.is.BlockByID(is.Block.ID)
	isPartition, physicalBlockID := is.IsPartition()
	if isPartition {
		pt := tbl.(causet.PartitionedBlock)
		tbl = pt.GetPartition(physicalBlockID)
	} else {
		physicalBlockID = is.Block.ID
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &IndexReaderInterlockingDirectorate{
		baseInterlockingDirectorate:    newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPB:           posetPosetDagReq,
		startTS:         startTS,
		physicalBlockID: physicalBlockID,
		causet:           tbl,
		index:           is.Index,
		keepOrder:       is.KeepOrder,
		desc:            is.Desc,
		defCausumns:         is.DeferredCausets,
		streaming:       streaming,
		corDefCausInFilter:  b.corDefCausInDistCauset(v.IndexCausets),
		corDefCausInAccess:  b.corDefCausInAccess(v.IndexCausets[0]),
		idxDefCauss:         is.IdxDefCauss,
		defCausLens:         is.IdxDefCausLens,
		plans:           v.IndexCausets,
		outputDeferredCausets:   v.OutputDeferredCausets,
	}
	if containsLimit(posetPosetDagReq.InterlockingDirectorates) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(e.physicalBlockID, is.Hist, int64(is.StatsCount()), is.Desc)
	}
	defCauslect := statistics.DefCauslectFeedback(b.ctx.GetStochastikVars().StmtCtx, e.feedback, len(is.Ranges))
	if !defCauslect {
		e.feedback.Invalidate()
	}
	e.posetPosetDagPB.DefCauslectRangeCounts = &defCauslect

	for _, defCaus := range v.OutputDeferredCausets {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(defCaus.Index))
	}

	return e, nil
}

func (b *interlockBuilder) buildIndexReader(v *causetcore.PhysicalIndexReader) InterlockingDirectorate {
	if b.ctx.GetStochastikVars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	ret, err := buildNoRangeIndexReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	is := v.IndexCausets[0].(*causetcore.PhysicalIndexScan)
	ret.ranges = is.Ranges
	sctx := b.ctx.GetStochastikVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Block.Name.O+":"+is.Index.Name.O)

	if !b.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := is.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	nextPartition := nextPartitionForIndexReader{exec: ret}
	exec, err := buildPartitionBlock(b, is.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
	}
	return exec
}

func buildBlockReq(b *interlockBuilder, schemaLen int, plans []causetcore.PhysicalCauset) (posetPosetDagReq *fidelpb.PosetDagRequest, streaming bool, val causet.Block, err error) {
	blockReq, blockStreaming, err := b.constructPosetDagReq(plans, ekv.EinsteinDB)
	if err != nil {
		return nil, false, nil, err
	}
	for i := 0; i < schemaLen; i++ {
		blockReq.OutputOffsets = append(blockReq.OutputOffsets, uint32(i))
	}
	ts := plans[0].(*causetcore.PhysicalBlockScan)
	tbl, _ := b.is.BlockByID(ts.Block.ID)
	isPartition, physicalBlockID := ts.IsPartition()
	if isPartition {
		pt := tbl.(causet.PartitionedBlock)
		tbl = pt.GetPartition(physicalBlockID)
	}
	return blockReq, blockStreaming, tbl, err
}

func buildIndexReq(b *interlockBuilder, schemaLen, handleLen int, plans []causetcore.PhysicalCauset) (posetPosetDagReq *fidelpb.PosetDagRequest, streaming bool, err error) {
	indexReq, indexStreaming, err := b.constructPosetDagReq(plans, ekv.EinsteinDB)
	if err != nil {
		return nil, false, err
	}
	indexReq.OutputOffsets = []uint32{}
	for i := 0; i < handleLen; i++ {
		indexReq.OutputOffsets = append(indexReq.OutputOffsets, uint32(schemaLen+i))
	}
	if len(indexReq.OutputOffsets) == 0 {
		indexReq.OutputOffsets = []uint32{uint32(schemaLen)}
	}
	return indexReq, indexStreaming, err
}

func buildNoRangeIndexLookUpReader(b *interlockBuilder, v *causetcore.PhysicalIndexLookUpReader) (*IndexLookUpInterlockingDirectorate, error) {
	is := v.IndexCausets[0].(*causetcore.PhysicalIndexScan)
	indexReq, indexStreaming, err := buildIndexReq(b, len(is.Index.DeferredCausets), len(v.CommonHandleDefCauss), v.IndexCausets)
	if err != nil {
		return nil, err
	}
	blockReq, blockStreaming, tbl, err := buildBlockReq(b, v.Schema().Len(), v.BlockCausets)
	if err != nil {
		return nil, err
	}
	ts := v.BlockCausets[0].(*causetcore.PhysicalBlockScan)
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &IndexLookUpInterlockingDirectorate{
		baseInterlockingDirectorate:      newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPB:             indexReq,
		startTS:           startTS,
		causet:             tbl,
		index:             is.Index,
		keepOrder:         is.KeepOrder,
		desc:              is.Desc,
		blockRequest:      blockReq,
		defCausumns:           ts.DeferredCausets,
		indexStreaming:    indexStreaming,
		blockStreaming:    blockStreaming,
		dataReaderBuilder: &dataReaderBuilder{interlockBuilder: b},
		corDefCausInIdxSide:   b.corDefCausInDistCauset(v.IndexCausets),
		corDefCausInTblSide:   b.corDefCausInDistCauset(v.BlockCausets),
		corDefCausInAccess:    b.corDefCausInAccess(v.IndexCausets[0]),
		idxDefCauss:           is.IdxDefCauss,
		defCausLens:           is.IdxDefCausLens,
		idxCausets:          v.IndexCausets,
		tblCausets:          v.BlockCausets,
		PushedLimit:       v.PushedLimit,
	}

	if containsLimit(indexReq.InterlockingDirectorates) {
		e.feedback = statistics.NewQueryFeedback(0, nil, 0, is.Desc)
	} else {
		e.feedback = statistics.NewQueryFeedback(getPhysicalBlockID(tbl), is.Hist, int64(is.StatsCount()), is.Desc)
	}
	// Do not defCauslect the feedback for causet request.
	defCauslectBlock := false
	e.blockRequest.DefCauslectRangeCounts = &defCauslectBlock
	defCauslectIndex := statistics.DefCauslectFeedback(b.ctx.GetStochastikVars().StmtCtx, e.feedback, len(is.Ranges))
	if !defCauslectIndex {
		e.feedback.Invalidate()
	}
	e.posetPosetDagPB.DefCauslectRangeCounts = &defCauslectIndex
	if v.ExtraHandleDefCaus != nil {
		e.handleIdx = append(e.handleIdx, v.ExtraHandleDefCaus.Index)
		e.handleDefCauss = []*memex.DeferredCauset{v.ExtraHandleDefCaus}
	} else {
		for _, handleDefCaus := range v.CommonHandleDefCauss {
			e.handleIdx = append(e.handleIdx, handleDefCaus.Index)
		}
		e.handleDefCauss = v.CommonHandleDefCauss
		e.primaryKeyIndex = blocks.FindPrimaryIndex(tbl.Meta())
	}
	return e, nil
}

func (b *interlockBuilder) buildIndexLookUpReader(v *causetcore.PhysicalIndexLookUpReader) InterlockingDirectorate {
	if b.ctx.GetStochastikVars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	ret, err := buildNoRangeIndexLookUpReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}

	is := v.IndexCausets[0].(*causetcore.PhysicalIndexScan)
	ts := v.BlockCausets[0].(*causetcore.PhysicalBlockScan)

	ret.ranges = is.Ranges
	interlockCounterIndexLookUpInterlockingDirectorate.Inc()

	sctx := b.ctx.GetStochastikVars().StmtCtx
	sctx.IndexNames = append(sctx.IndexNames, is.Block.Name.O+":"+is.Index.Name.O)
	sctx.BlockIDs = append(sctx.BlockIDs, ts.Block.ID)

	if !b.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := is.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	nextPartition := nextPartitionForIndexLookUp{exec: ret}
	exec, err := buildPartitionBlock(b, ts.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
		return nil
	}
	return exec
}

func buildNoRangeIndexMergeReader(b *interlockBuilder, v *causetcore.PhysicalIndexMergeReader) (*IndexMergeReaderInterlockingDirectorate, error) {
	partialCausetCount := len(v.PartialCausets)
	partialReqs := make([]*fidelpb.PosetDagRequest, 0, partialCausetCount)
	partialStreamings := make([]bool, 0, partialCausetCount)
	indexes := make([]*perceptron.IndexInfo, 0, partialCausetCount)
	keepOrders := make([]bool, 0, partialCausetCount)
	descs := make([]bool, 0, partialCausetCount)
	feedbacks := make([]*statistics.QueryFeedback, 0, partialCausetCount)
	ts := v.BlockCausets[0].(*causetcore.PhysicalBlockScan)
	for i := 0; i < partialCausetCount; i++ {
		var tempReq *fidelpb.PosetDagRequest
		var tempStreaming bool
		var err error

		feedback := statistics.NewQueryFeedback(0, nil, 0, ts.Desc)
		feedback.Invalidate()
		feedbacks = append(feedbacks, feedback)

		if is, ok := v.PartialCausets[i][0].(*causetcore.PhysicalIndexScan); ok {
			tempReq, tempStreaming, err = buildIndexReq(b, len(is.Index.DeferredCausets), ts.HandleDefCauss.NumDefCauss(), v.PartialCausets[i])
			keepOrders = append(keepOrders, is.KeepOrder)
			descs = append(descs, is.Desc)
			indexes = append(indexes, is.Index)
		} else {
			ts := v.PartialCausets[i][0].(*causetcore.PhysicalBlockScan)
			tempReq, tempStreaming, _, err = buildBlockReq(b, len(ts.DeferredCausets), v.PartialCausets[i])
			keepOrders = append(keepOrders, ts.KeepOrder)
			descs = append(descs, ts.Desc)
			indexes = append(indexes, nil)
		}
		if err != nil {
			return nil, err
		}
		defCauslect := false
		tempReq.DefCauslectRangeCounts = &defCauslect
		partialReqs = append(partialReqs, tempReq)
		partialStreamings = append(partialStreamings, tempStreaming)
	}
	blockReq, blockStreaming, tblInfo, err := buildBlockReq(b, v.Schema().Len(), v.BlockCausets)
	if err != nil {
		return nil, err
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	e := &IndexMergeReaderInterlockingDirectorate{
		baseInterlockingDirectorate:      newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID()),
		posetPosetDagPBs:            partialReqs,
		startTS:           startTS,
		causet:             tblInfo,
		indexes:           indexes,
		descs:             descs,
		blockRequest:      blockReq,
		defCausumns:           ts.DeferredCausets,
		partialStreamings: partialStreamings,
		blockStreaming:    blockStreaming,
		partialCausets:      v.PartialCausets,
		tblCausets:          v.BlockCausets,
		dataReaderBuilder: &dataReaderBuilder{interlockBuilder: b},
		feedbacks:         feedbacks,
		handleDefCauss:        ts.HandleDefCauss,
	}
	defCauslectBlock := false
	e.blockRequest.DefCauslectRangeCounts = &defCauslectBlock
	return e, nil
}

func (b *interlockBuilder) buildIndexMergeReader(v *causetcore.PhysicalIndexMergeReader) InterlockingDirectorate {
	ret, err := buildNoRangeIndexMergeReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}
	ret.ranges = make([][]*ranger.Range, 0, len(v.PartialCausets))
	sctx := b.ctx.GetStochastikVars().StmtCtx
	for i := 0; i < len(v.PartialCausets); i++ {
		if is, ok := v.PartialCausets[i][0].(*causetcore.PhysicalIndexScan); ok {
			ret.ranges = append(ret.ranges, is.Ranges)
			sctx.IndexNames = append(sctx.IndexNames, is.Block.Name.O+":"+is.Index.Name.O)
		} else {
			ret.ranges = append(ret.ranges, v.PartialCausets[i][0].(*causetcore.PhysicalBlockScan).Ranges)
			if ret.causet.Meta().IsCommonHandle {
				tblInfo := ret.causet.Meta()
				sctx.IndexNames = append(sctx.IndexNames, tblInfo.Name.O+":"+blocks.FindPrimaryIndex(tblInfo).Name.O)
			}
		}
	}
	ts := v.BlockCausets[0].(*causetcore.PhysicalBlockScan)
	sctx.BlockIDs = append(sctx.BlockIDs, ts.Block.ID)
	interlockCounterIndexMergeReaderInterlockingDirectorate.Inc()

	if !b.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Block.GetPartitionInfo(); pi == nil {
		return ret
	}

	nextPartition := nextPartitionForIndexMerge{ret}
	exec, err := buildPartitionBlock(b, ts.Block, &v.PartitionInfo, ret, nextPartition)
	if err != nil {
		b.err = err
		return nil
	}
	return exec
}

// dataReaderBuilder build an interlock.
// The interlock can be used to read data in the ranges which are constructed by datums.
// Differences from interlockBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result interlock is already opened.
type dataReaderBuilder struct {
	causetcore.Causet
	*interlockBuilder

	selectResultHook // for testing
}

type mockPhysicalIndexReader struct {
	causetcore.PhysicalCauset

	e InterlockingDirectorate
}

func (builder *dataReaderBuilder) buildInterlockingDirectorateForIndexJoin(ctx context.Context, lookUpContents []*indexJoinLookUpContent,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (InterlockingDirectorate, error) {
	return builder.buildInterlockingDirectorateForIndexJoinInternal(ctx, builder.Causet, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
}

func (builder *dataReaderBuilder) buildInterlockingDirectorateForIndexJoinInternal(ctx context.Context, plan causetcore.Causet, lookUpContents []*indexJoinLookUpContent,
	IndexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (InterlockingDirectorate, error) {
	switch v := plan.(type) {
	case *causetcore.PhysicalBlockReader:
		return builder.buildBlockReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *causetcore.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *causetcore.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	case *causetcore.PhysicalUnionScan:
		return builder.buildUnionScanForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	// The inner child of IndexJoin might be Projection when a combination of the following conditions is true:
	// 	1. The inner child fetch data using indexLookupReader
	// 	2. PK is not handle
	// 	3. The inner child needs to keep order
	// In this case, an extra defCausumn milevadb_rowid will be appended in the output result of IndexLookupReader(see copTask.doubleReadNeedProj).
	// Then we need a Projection upon IndexLookupReader to prune the redundant defCausumn.
	case *causetcore.PhysicalProjection:
		return builder.buildProjectionForIndexJoin(ctx, v, lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
	// Need to support physical selection because after PR 16389, MilevaDB will push down all the expr supported by EinsteinDB or TiFlash
	// in predicate push down stage, so if there is an expr which only supported by TiFlash, a physical selection will be added after index read
	case *causetcore.PhysicalSelection:
		childInterDirc, err := builder.buildInterlockingDirectorateForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, IndexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		exec := &SelectionInterDirc{
			baseInterlockingDirectorate: newBaseInterlockingDirectorate(builder.ctx, v.Schema(), v.ID(), childInterDirc),
			filters:      v.Conditions,
		}
		err = exec.open(ctx)
		return exec, err
	case *mockPhysicalIndexReader:
		return v.e, nil
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildUnionScanForIndexJoin(ctx context.Context, v *causetcore.PhysicalUnionScan,
	values []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (InterlockingDirectorate, error) {
	childBuilder := &dataReaderBuilder{Causet: v.Children()[0], interlockBuilder: builder.interlockBuilder}
	reader, err := childBuilder.buildInterlockingDirectorateForIndexJoin(ctx, values, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}

	ret := builder.buildUnionScanFromReader(reader, v)
	if us, ok := ret.(*UnionScanInterDirc); ok {
		err = us.open(ctx)
	}
	return ret, err
}

func (builder *dataReaderBuilder) buildBlockReaderForIndexJoin(ctx context.Context, v *causetcore.PhysicalBlockReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (InterlockingDirectorate, error) {
	e, err := buildNoRangeBlockReader(builder.interlockBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.causet.Meta()
	if v.IsCommonHandle {
		kvRanges, err := buildKvRangesForIndexJoin(e.ctx, getPhysicalBlockID(e.causet), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		if tbInfo.GetPartitionInfo() == nil {
			return builder.buildBlockReaderFromKvRanges(ctx, e, kvRanges)
		}
		e.kvRangeBuilder = kvRangeBuilderFromFunc(func(pid int64) ([]ekv.KeyRange, error) {
			return buildKvRangesForIndexJoin(e.ctx, pid, -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		})
		nextPartition := nextPartitionForBlockReader{e}
		return buildPartitionBlock(builder.interlockBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
	}
	handles := make([]ekv.Handle, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		isValidHandle := true
		handle := ekv.IntHandle(content.keys[0].GetInt64())
		for _, key := range content.keys {
			if handle.IntValue() != key.GetInt64() {
				isValidHandle = false
				break
			}
		}
		if isValidHandle {
			handles = append(handles, handle)
		}
	}

	if tbInfo.GetPartitionInfo() == nil {
		return builder.buildBlockReaderFromHandles(ctx, e, handles)
	}
	if !builder.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
		return builder.buildBlockReaderFromHandles(ctx, e, handles)
	}

	e.kvRangeBuilder = kvRangeBuilderFromHandles(handles)
	nextPartition := nextPartitionForBlockReader{e}
	return buildPartitionBlock(builder.interlockBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
}

type kvRangeBuilderFromFunc func(pid int64) ([]ekv.KeyRange, error)

func (h kvRangeBuilderFromFunc) buildKeyRange(pid int64) ([]ekv.KeyRange, error) {
	return h(pid)
}

type kvRangeBuilderFromHandles []ekv.Handle

func (h kvRangeBuilderFromHandles) buildKeyRange(pid int64) ([]ekv.KeyRange, error) {
	handles := []ekv.Handle(h)
	sort.Slice(handles, func(i, j int) bool {
		return handles[i].Compare(handles[j]) < 0
	})
	return allegrosql.BlockHandlesToKVRanges(pid, handles), nil
}

func (builder *dataReaderBuilder) buildBlockReaderBase(ctx context.Context, e *BlockReaderInterlockingDirectorate, reqBuilderWithRange allegrosql.RequestBuilder) (*BlockReaderInterlockingDirectorate, error) {
	startTS, err := builder.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	kvReq, err := reqBuilderWithRange.
		SetPosetDagRequest(e.posetPosetDagPB).
		SetStartTS(startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)
	e.resultHandler = &blockResultHandler{}
	result, err := builder.SelectResult(ctx, builder.ctx, kvReq, retTypes(e), e.feedback, getPhysicalCausetIDs(e.plans), e.id)
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildBlockReaderFromHandles(ctx context.Context, e *BlockReaderInterlockingDirectorate, handles []ekv.Handle) (*BlockReaderInterlockingDirectorate, error) {
	sort.Slice(handles, func(i, j int) bool {
		return handles[i].Compare(handles[j]) < 0
	})
	var b allegrosql.RequestBuilder
	b.SetBlockHandles(getPhysicalBlockID(e.causet), handles)
	return builder.buildBlockReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildBlockReaderFromKvRanges(ctx context.Context, e *BlockReaderInterlockingDirectorate, ranges []ekv.KeyRange) (InterlockingDirectorate, error) {
	var b allegrosql.RequestBuilder
	b.SetKeyRanges(ranges)
	return builder.buildBlockReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *causetcore.PhysicalIndexReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (InterlockingDirectorate, error) {
	e, err := buildNoRangeIndexReader(builder.interlockBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.causet.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
		kvRanges, err := buildKvRangesForIndexJoin(e.ctx, e.physicalBlockID, e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx, kvRanges)
		return e, err
	}

	e.ranges, err = buildRangesForIndexJoin(e.ctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}
	nextPartition := nextPartitionForIndexReader{exec: e}
	ret, err := buildPartitionBlock(builder.interlockBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
	if err != nil {
		return nil, err
	}
	err = ret.Open(ctx)
	return ret, err
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *causetcore.PhysicalIndexLookUpReader,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (InterlockingDirectorate, error) {
	e, err := buildNoRangeIndexLookUpReader(builder.interlockBuilder, v)
	if err != nil {
		return nil, err
	}

	tbInfo := e.causet.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetStochastikVars().UseDynamicPartitionPrune() {
		e.kvRanges, err = buildKvRangesForIndexJoin(e.ctx, getPhysicalBlockID(e.causet), e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx)
		return e, err
	}

	e.ranges, err = buildRangesForIndexJoin(e.ctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}
	nextPartition := nextPartitionForIndexLookUp{exec: e}
	ret, err := buildPartitionBlock(builder.interlockBuilder, tbInfo, &v.PartitionInfo, e, nextPartition)
	if err != nil {
		return nil, err
	}
	err = ret.Open(ctx)
	return ret, err
}

func (builder *dataReaderBuilder) buildProjectionForIndexJoin(ctx context.Context, v *causetcore.PhysicalProjection,
	lookUpContents []*indexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (InterlockingDirectorate, error) {
	physicalIndexLookUp, isDoubleRead := v.Children()[0].(*causetcore.PhysicalIndexLookUpReader)
	if !isDoubleRead {
		return nil, errors.Errorf("inner child of Projection should be IndexLookupReader, but got %T", v)
	}
	childInterDirc, err := builder.buildIndexLookUpReaderForIndexJoin(ctx, physicalIndexLookUp, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}

	e := &ProjectionInterDirc{
		baseInterlockingDirectorate:     newBaseInterlockingDirectorate(builder.ctx, v.Schema(), v.ID(), childInterDirc),
		numWorkers:       int64(builder.ctx.GetStochastikVars().ProjectionConcurrency()),
		evaluatorSuit:    memex.NewEvaluatorSuite(v.Exprs, v.AvoidDeferredCausetEvaluator),
		calculateNoDelay: v.CalculateNoDelay,
	}

	// If the calculation event count for this Projection operator is smaller
	// than a Chunk size, we turn back to the un-parallel Projection
	// implementation to reduce the goroutine overhead.
	if int64(v.StatsCount()) < int64(builder.ctx.GetStochastikVars().MaxChunkSize) {
		e.numWorkers = 0
	}
	err = e.open(ctx)

	return e, err
}

// buildRangesForIndexJoin builds ekv ranges for index join when the inner plan is index scan plan.
func buildRangesForIndexJoin(ctx stochastikctx.Context, lookUpContents []*indexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) ([]*ranger.Range, error) {
	retRanges := make([]*ranger.Range, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	tmFIDelatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.keys[keyOff]
				ran.HighVal[idxOff] = content.keys[keyOff]
			}
		}
		if cwc == nil {
			// A deep copy is need here because the old []*range.Range is overwriten
			for _, ran := range ranges {
				retRanges = append(retRanges, ran.Clone())
			}
			continue
		}
		nextDefCausRanges, err := cwc.BuildRangesByEvent(ctx, content.event)
		if err != nil {
			return nil, err
		}
		for _, nextDefCausRan := range nextDefCausRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextDefCausRan.LowVal[0]
				ran.HighVal[lastPos] = nextDefCausRan.HighVal[0]
				ran.LowExclude = nextDefCausRan.LowExclude
				ran.HighExclude = nextDefCausRan.HighExclude
				tmFIDelatumRanges = append(tmFIDelatumRanges, ran.Clone())
			}
		}
	}

	if cwc == nil {
		return retRanges, nil
	}

	return ranger.UnionRanges(ctx.GetStochastikVars().StmtCtx, tmFIDelatumRanges, true)
}

// buildKvRangesForIndexJoin builds ekv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(ctx stochastikctx.Context, blockID, indexID int64, lookUpContents []*indexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *causetcore.DefCausWithCmpFuncManager) (_ []ekv.KeyRange, err error) {
	kvRanges := make([]ekv.KeyRange, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	sc := ctx.GetStochastikVars().StmtCtx
	tmFIDelatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.keys[keyOff]
				ran.HighVal[idxOff] = content.keys[keyOff]
			}
		}
		if cwc == nil {
			// Index id is -1 means it's a common handle.
			var tmpKvRanges []ekv.KeyRange
			var err error
			if indexID == -1 {
				tmpKvRanges, err = allegrosql.CommonHandleRangesToKVRanges(sc, blockID, ranges)
			} else {
				tmpKvRanges, err = allegrosql.IndexRangesToKVRanges(sc, blockID, indexID, ranges, nil)
			}
			if err != nil {
				return nil, err
			}
			kvRanges = append(kvRanges, tmpKvRanges...)
			continue
		}
		nextDefCausRanges, err := cwc.BuildRangesByEvent(ctx, content.event)
		if err != nil {
			return nil, err
		}
		for _, nextDefCausRan := range nextDefCausRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextDefCausRan.LowVal[0]
				ran.HighVal[lastPos] = nextDefCausRan.HighVal[0]
				ran.LowExclude = nextDefCausRan.LowExclude
				ran.HighExclude = nextDefCausRan.HighExclude
				tmFIDelatumRanges = append(tmFIDelatumRanges, ran.Clone())
			}
		}
	}

	if cwc == nil {
		sort.Slice(kvRanges, func(i, j int) bool {
			return bytes.Compare(kvRanges[i].StartKey, kvRanges[j].StartKey) < 0
		})
		return kvRanges, nil
	}

	tmFIDelatumRanges, err = ranger.UnionRanges(ctx.GetStochastikVars().StmtCtx, tmFIDelatumRanges, true)
	if err != nil {
		return nil, err
	}
	// Index id is -1 means it's a common handle.
	if indexID == -1 {
		return allegrosql.CommonHandleRangesToKVRanges(ctx.GetStochastikVars().StmtCtx, blockID, tmFIDelatumRanges)
	}
	return allegrosql.IndexRangesToKVRanges(ctx.GetStochastikVars().StmtCtx, blockID, indexID, tmFIDelatumRanges, nil)
}

func (b *interlockBuilder) buildWindow(v *causetcore.PhysicalWindow) *WindowInterDirc {
	childInterDirc := b.build(v.Children()[0])
	if b.err != nil {
		return nil
	}
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID(), childInterDirc)
	groupByItems := make([]memex.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		groupByItems = append(groupByItems, item.DefCaus)
	}
	orderByDefCauss := make([]*memex.DeferredCauset, 0, len(v.OrderBy))
	for _, item := range v.OrderBy {
		orderByDefCauss = append(orderByDefCauss, item.DefCaus)
	}
	windowFuncs := make([]aggfuncs.AggFunc, 0, len(v.WindowFuncDescs))
	partialResults := make([]aggfuncs.PartialResult, 0, len(v.WindowFuncDescs))
	resultDefCausIdx := v.Schema().Len() - len(v.WindowFuncDescs)
	for _, desc := range v.WindowFuncDescs {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, desc.Name, desc.Args, false)
		if err != nil {
			b.err = err
			return nil
		}
		agg := aggfuncs.BuildWindowFunctions(b.ctx, aggDesc, resultDefCausIdx, orderByDefCauss)
		windowFuncs = append(windowFuncs, agg)
		partialResult, _ := agg.AllocPartialResult()
		partialResults = append(partialResults, partialResult)
		resultDefCausIdx++
	}
	var processor windowProcessor
	if v.Frame == nil {
		processor = &aggWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
	} else if v.Frame.Type == ast.Events {
		processor = &rowFrameWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
			start:          v.Frame.Start,
			end:            v.Frame.End,
		}
	} else {
		cmpResult := int64(-1)
		if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
			cmpResult = 1
		}
		processor = &rangeFrameWindowProcessor{
			windowFuncs:       windowFuncs,
			partialResults:    partialResults,
			start:             v.Frame.Start,
			end:               v.Frame.End,
			orderByDefCauss:       orderByDefCauss,
			expectedCmpResult: cmpResult,
		}
	}
	return &WindowInterDirc{baseInterlockingDirectorate: base,
		processor:      processor,
		groupChecker:   newVecGroupChecker(b.ctx, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}
}

func (b *interlockBuilder) buildShuffle(v *causetcore.PhysicalShuffle) *ShuffleInterDirc {
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())
	shuffle := &ShuffleInterDirc{baseInterlockingDirectorate: base,
		concurrency: v.Concurrency,
	}

	switch v.SplitterType {
	case causetcore.PartitionHashSplitterType:
		shuffle.splitter = &partitionHashSplitter{
			byItems:    v.HashByItems,
			numWorkers: shuffle.concurrency,
		}
	default:
		panic("Not implemented. Should not reach here.")
	}

	shuffle.dataSource = b.build(v.DataSource)
	if b.err != nil {
		return nil
	}

	// head & tail of physical plans' chain within "partition".
	var head, tail = v.Children()[0], v.Tail

	shuffle.workers = make([]*shuffleWorker, shuffle.concurrency)
	for i := range shuffle.workers {
		w := &shuffleWorker{
			baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.DataSource.Schema(), v.DataSource.ID()),
		}

		stub := causetcore.PhysicalShuffleDataSourceStub{
			Worker: (unsafe.Pointer)(w),
		}.Init(b.ctx, v.DataSource.Stats(), v.DataSource.SelectBlockOffset(), nil)
		stub.SetSchema(v.DataSource.Schema())

		tail.SetChildren(stub)
		w.childInterDirc = b.build(head)
		if b.err != nil {
			return nil
		}

		shuffle.workers[i] = w
	}

	return shuffle
}

func (b *interlockBuilder) buildShuffleDataSourceStub(v *causetcore.PhysicalShuffleDataSourceStub) *shuffleWorker {
	return (*shuffleWorker)(v.Worker)
}

func (b *interlockBuilder) buildALLEGROSQLBindInterDirc(v *causetcore.ALLEGROSQLBindCauset) InterlockingDirectorate {
	base := newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())
	base.initCap = chunk.ZeroCapacity

	e := &ALLEGROSQLBindInterDirc{
		baseInterlockingDirectorate: base,
		sqlBindOp:    v.ALLEGROSQLBindOp,
		normdOrigALLEGROSQL: v.NormdOrigALLEGROSQL,
		bindALLEGROSQL:      v.BindALLEGROSQL,
		charset:      v.Charset,
		defCauslation:    v.DefCauslation,
		EDB:           v.EDB,
		isGlobal:     v.IsGlobal,
		bindAst:      v.BindStmt,
	}
	return e
}

// NewEventCausetDecoder creates a chunk causetDecoder for new event format event value decode.
func NewEventCausetDecoder(ctx stochastikctx.Context, schemaReplicant *memex.Schema, tbl *perceptron.BlockInfo) *rowcodec.ChunkCausetDecoder {
	getDefCausInfoByID := func(tbl *perceptron.BlockInfo, defCausID int64) *perceptron.DeferredCausetInfo {
		for _, defCaus := range tbl.DeferredCausets {
			if defCaus.ID == defCausID {
				return defCaus
			}
		}
		return nil
	}
	var pkDefCauss []int64
	reqDefCauss := make([]rowcodec.DefCausInfo, len(schemaReplicant.DeferredCausets))
	for i := range schemaReplicant.DeferredCausets {
		idx, defCaus := i, schemaReplicant.DeferredCausets[i]
		isPK := (tbl.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.RetType.Flag)) || defCaus.ID == perceptron.ExtraHandleID
		if isPK {
			pkDefCauss = append(pkDefCauss, defCaus.ID)
		}
		isGeneratedDefCaus := false
		if defCaus.VirtualExpr != nil {
			isGeneratedDefCaus = true
		}
		reqDefCauss[idx] = rowcodec.DefCausInfo{
			ID:            defCaus.ID,
			VirtualGenDefCaus: isGeneratedDefCaus,
			Ft:            defCaus.RetType,
		}
	}
	if len(pkDefCauss) == 0 {
		pkDefCauss = blocks.TryGetCommonPkDeferredCausetIds(tbl)
		if len(pkDefCauss) == 0 {
			pkDefCauss = []int64{0}
		}
	}
	defVal := func(i int, chk *chunk.Chunk) error {
		ci := getDefCausInfoByID(tbl, reqDefCauss[i].ID)
		d, err := causet.GetDefCausOriginDefaultValue(ctx, ci)
		if err != nil {
			return err
		}
		chk.AppendCauset(i, &d)
		return nil
	}
	return rowcodec.NewChunkCausetDecoder(reqDefCauss, pkDefCauss, defVal, ctx.GetStochastikVars().TimeZone)
}

func (b *interlockBuilder) buildBatchPointGet(plan *causetcore.BatchPointGetCauset) InterlockingDirectorate {
	if b.ctx.GetStochastikVars().IsPessimisticReadConsistency() {
		if err := b.refreshForUFIDelateTSForRC(); err != nil {
			b.err = err
			return nil
		}
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		b.err = err
		return nil
	}
	causetDecoder := NewEventCausetDecoder(b.ctx, plan.Schema(), plan.TblInfo)
	e := &BatchPointGetInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, plan.Schema(), plan.ID()),
		tblInfo:      plan.TblInfo,
		idxInfo:      plan.IndexInfo,
		rowCausetDecoder:   causetDecoder,
		startTS:      startTS,
		keepOrder:    plan.KeepOrder,
		desc:         plan.Desc,
		dagger:         plan.Lock,
		waitTime:     plan.LockWaitTime,
		partPos:      plan.PartitionDefCausPos,
		defCausumns:      plan.DeferredCausets,
	}
	if e.dagger {
		b.hasLock = true
	}
	var capacity int
	if plan.IndexInfo != nil && !isCommonHandleRead(plan.TblInfo, plan.IndexInfo) {
		e.idxVals = plan.IndexValues
		capacity = len(e.idxVals)
	} else {
		// `SELECT a FROM t WHERE a IN (1, 1, 2, 1, 2)` should not return duplicated rows
		handles := make([]ekv.Handle, 0, len(plan.Handles))
		dedup := ekv.NewHandleMap()
		if plan.IndexInfo == nil {
			for _, handle := range plan.Handles {
				if _, found := dedup.Get(handle); found {
					continue
				}
				dedup.Set(handle, true)
				handles = append(handles, handle)
			}
		} else {
			for _, value := range plan.IndexValues {
				handleBytes, err := EncodeUniqueIndexValuesForKey(e.ctx, e.tblInfo, plan.IndexInfo, value)
				if err != nil {
					b.err = err
					return nil
				}
				handle, err := ekv.NewCommonHandle(handleBytes)
				if err != nil {
					b.err = err
					return nil
				}
				if _, found := dedup.Get(handle); found {
					continue
				}
				dedup.Set(handle, true)
				handles = append(handles, handle)
			}
		}
		e.handles = handles
		capacity = len(e.handles)
	}
	e.base().initCap = capacity
	e.base().maxChunkSize = capacity
	e.buildVirtualDeferredCausetInfo()
	return e
}

func isCommonHandleRead(tbl *perceptron.BlockInfo, idx *perceptron.IndexInfo) bool {
	return tbl.IsCommonHandle && idx.Primary
}

func getPhysicalBlockID(t causet.Block) int64 {
	if p, ok := t.(causet.PhysicalBlock); ok {
		return p.GetPhysicalID()
	}
	return t.Meta().ID
}

func (b *interlockBuilder) buildAdminShowTelemetry(v *causetcore.AdminShowTelemetry) InterlockingDirectorate {
	return &AdminShowTelemetryInterDirc{baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())}
}

func (b *interlockBuilder) buildAdminResetTelemetryID(v *causetcore.AdminResetTelemetryID) InterlockingDirectorate {
	return &AdminResetTelemetryIDInterDirc{baseInterlockingDirectorate: newBaseInterlockingDirectorate(b.ctx, v.Schema(), v.ID())}
}

func partitionPruning(ctx stochastikctx.Context, tbl causet.PartitionedBlock, conds []memex.Expression, partitionNames []perceptron.CIStr,
	defCausumns []*memex.DeferredCauset, defCausumnNames types.NameSlice) ([]causet.PhysicalBlock, error) {
	idxArr, err := causetcore.PartitionPruning(ctx, tbl, conds, partitionNames, defCausumns, defCausumnNames)
	if err != nil {
		return nil, err
	}

	pi := tbl.Meta().GetPartitionInfo()
	var ret []causet.PhysicalBlock
	if fullRangePartition(idxArr) {
		ret = make([]causet.PhysicalBlock, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			p := tbl.GetPartition(def.ID)
			ret = append(ret, p)
		}
	} else {
		ret = make([]causet.PhysicalBlock, 0, len(idxArr))
		for _, idx := range idxArr {
			pid := pi.Definitions[idx].ID
			p := tbl.GetPartition(pid)
			ret = append(ret, p)
		}
	}
	return ret, nil
}

func fullRangePartition(idxArr []int) bool {
	return len(idxArr) == 1 && idxArr[0] == causetcore.FullRange
}
