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
	"sort"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/allegrosql"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/ekv"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// make sure `BlockReaderInterlockingDirectorate` implements `InterlockingDirectorate`.
var _ InterlockingDirectorate = &BlockReaderInterlockingDirectorate{}

// selectResultHook is used to replog allegrosql.SelectWithRuntimeStats safely for testing.
type selectResultHook struct {
	selectResultFunc func(ctx context.Context, sctx stochastikctx.Context, kvReq *ekv.Request,
		fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copCausetIDs []int) (allegrosql.SelectResult, error)
}

func (sr selectResultHook) SelectResult(ctx context.Context, sctx stochastikctx.Context, kvReq *ekv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copCausetIDs []int, rootCausetID int) (allegrosql.SelectResult, error) {
	if sr.selectResultFunc == nil {
		return allegrosql.SelectWithRuntimeStats(ctx, sctx, kvReq, fieldTypes, fb, copCausetIDs, rootCausetID)
	}
	return sr.selectResultFunc(ctx, sctx, kvReq, fieldTypes, fb, copCausetIDs)
}

type kvRangeBuilder interface {
	buildKeyRange(pid int64) ([]ekv.KeyRange, error)
}

// BlockReaderInterlockingDirectorate sends PosetDag request and reads causet data from ekv layer.
type BlockReaderInterlockingDirectorate struct {
	baseInterlockingDirectorate

	causet causet.Block

	// The source of key ranges varies from case to case.
	// It may be calculated from PyhsicalCauset by interlockBuilder, or calculated from argument by dataBuilder;
	// It may be calculated from ranger.Ranger, or calculated from handles.
	// The causet ID may also change because of the partition causet, and causes the key range to change.
	// So instead of keeping a `range` struct field, it's better to define a interface.
	kvRangeBuilder
	// TODO: remove this field, use the kvRangeBuilder interface.
	ranges []*ranger.Range

	// kvRanges are only use for union scan.
	kvRanges []ekv.KeyRange
	posetPosetDagPB    *fidelpb.PosetDagRequest
	startTS  uint64
	// defCausumns are only required by union scan and virtual defCausumn.
	defCausumns []*perceptron.DeferredCausetInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *blockResultHandler
	feedback      *statistics.QueryFeedback
	plans         []causetcore.PhysicalCauset
	blockCauset     causetcore.PhysicalCauset

	memTracker       *memory.Tracker
	selectResultHook // for testing

	keepOrder bool
	desc      bool
	streaming bool
	storeType ekv.StoreType
	// corDefCausInFilter tells whether there's correlated defCausumn in filter.
	corDefCausInFilter bool
	// corDefCausInAccess tells whether there's correlated defCausumn in access conditions.
	corDefCausInAccess bool
	// virtualDeferredCausetIndex records all the indices of virtual defCausumns and sort them in definition
	// to make sure we can compute the virtual defCausumn in right order.
	virtualDeferredCausetIndex []int
	// virtualDeferredCausetRetFieldTypes records the RetFieldTypes of virtual defCausumns.
	virtualDeferredCausetRetFieldTypes []*types.FieldType
	// batchCop indicates whether use super batch interlock request, only works for TiFlash engine.
	batchCop bool
}

// Open initialzes necessary variables for using this interlock.
func (e *BlockReaderInterlockingDirectorate) Open(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("BlockReaderInterlockingDirectorate.Open", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	var err error
	if e.corDefCausInFilter {
		if e.storeType == ekv.TiFlash {
			execs, _, err := constructDistInterDircForTiFlash(e.ctx, e.blockCauset)
			if err != nil {
				return err
			}
			e.posetPosetDagPB.RootInterlockingDirectorate = execs[0]
		} else {
			e.posetPosetDagPB.InterlockingDirectorates, _, err = constructDistInterDirc(e.ctx, e.plans)
			if err != nil {
				return err
			}
		}
	}
	if e.runtimeStats != nil {
		defCauslInterDirc := true
		e.posetPosetDagPB.DefCauslectInterDircutionSummaries = &defCauslInterDirc
	}
	if e.corDefCausInAccess {
		ts := e.plans[0].(*causetcore.PhysicalBlockScan)
		access := ts.AccessCondition
		pkTP := ts.Block.GetPkDefCausInfo().FieldType
		e.ranges, err = ranger.BuildBlockRange(access, e.ctx.GetStochastikVars().StmtCtx, &pkTP)
		if err != nil {
			return err
		}
	}

	e.resultHandler = &blockResultHandler{}
	if e.feedback != nil && e.feedback.Hist != nil {
		// EncodeInt don't need *memex.Context.
		var ok bool
		e.ranges, ok = e.feedback.Hist.SplitRange(nil, e.ranges, false)
		if !ok {
			e.feedback.Invalidate()
		}
	}
	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder, e.desc)
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult allegrosql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by blockReaderHandler.
func (e *BlockReaderInterlockingDirectorate) Next(ctx context.Context, req *chunk.Chunk) error {
	logutil.Eventf(ctx, "causet scan causet: %s, range: %v", stringutil.MemoizeStr(func() string {
		var blockName string
		if spacetime := e.causet.Meta(); spacetime != nil {
			blockName = spacetime.Name.L
		}
		return blockName
	}), e.ranges)
	if err := e.resultHandler.nextChunk(ctx, req); err != nil {
		e.feedback.Invalidate()
		return err
	}

	err := FillVirtualDeferredCausetValue(e.virtualDeferredCausetRetFieldTypes, e.virtualDeferredCausetIndex, e.schemaReplicant, e.defCausumns, e.ctx, req)
	if err != nil {
		return err
	}

	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *BlockReaderInterlockingDirectorate) Close() error {
	var err error
	if e.resultHandler != nil {
		err = e.resultHandler.Close()
	}
	e.kvRanges = e.kvRanges[:0]
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// buildResp first builds request and sends it to einsteindb using allegrosql.Select. It uses SelectResut returned by the callee
// to fetch all results.
func (e *BlockReaderInterlockingDirectorate) buildResp(ctx context.Context, ranges []*ranger.Range) (allegrosql.SelectResult, error) {
	var builder allegrosql.RequestBuilder
	var reqBuilder *allegrosql.RequestBuilder
	if e.kvRangeBuilder != nil {
		kvRange, err := e.kvRangeBuilder.buildKeyRange(getPhysicalBlockID(e.causet))
		if err != nil {
			return nil, err
		}
		reqBuilder = builder.SetKeyRanges(kvRange)
	} else if e.causet.Meta() != nil && e.causet.Meta().IsCommonHandle {
		reqBuilder = builder.SetCommonHandleRanges(e.ctx.GetStochastikVars().StmtCtx, getPhysicalBlockID(e.causet), ranges)
	} else {
		reqBuilder = builder.SetBlockRanges(getPhysicalBlockID(e.causet), ranges, e.feedback)
	}
	kvReq, err := reqBuilder.
		SetPosetDagRequest(e.posetPosetDagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetAllowBatchCop(e.batchCop).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)

	result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalCausetIDs(e.plans), e.id)
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

func buildVirtualDeferredCausetIndex(schemaReplicant *memex.Schema, defCausumns []*perceptron.DeferredCausetInfo) []int {
	virtualDeferredCausetIndex := make([]int, 0, len(defCausumns))
	for i, defCaus := range schemaReplicant.DeferredCausets {
		if defCaus.VirtualExpr != nil {
			virtualDeferredCausetIndex = append(virtualDeferredCausetIndex, i)
		}
	}
	sort.Slice(virtualDeferredCausetIndex, func(i, j int) bool {
		return causetcore.FindDeferredCausetInfoByID(defCausumns, schemaReplicant.DeferredCausets[virtualDeferredCausetIndex[i]].ID).Offset <
			causetcore.FindDeferredCausetInfoByID(defCausumns, schemaReplicant.DeferredCausets[virtualDeferredCausetIndex[j]].ID).Offset
	})
	return virtualDeferredCausetIndex
}

// buildVirtualDeferredCausetInfo saves virtual defCausumn indices and sort them in definition order
func (e *BlockReaderInterlockingDirectorate) buildVirtualDeferredCausetInfo() {
	e.virtualDeferredCausetIndex = buildVirtualDeferredCausetIndex(e.Schema(), e.defCausumns)
	if len(e.virtualDeferredCausetIndex) > 0 {
		e.virtualDeferredCausetRetFieldTypes = make([]*types.FieldType, len(e.virtualDeferredCausetIndex))
		for i, idx := range e.virtualDeferredCausetIndex {
			e.virtualDeferredCausetRetFieldTypes[i] = e.schemaReplicant.DeferredCausets[idx].RetType
		}
	}
}

type blockResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true and want ascending order,
	// `optionalResult` will handles the request whose range is in signed int range, and
	// `result` will handle the request whose range is exceed signed int range.
	// If we want descending order, `optionalResult` will handles the request whose range is exceed signed, and
	// the `result` will handle the request whose range is in signed.
	// Otherwise, we just set `optionalFinished` true and the `result` handles the whole ranges.
	optionalResult allegrosql.SelectResult
	result         allegrosql.SelectResult

	optionalFinished bool
}

func (tr *blockResultHandler) open(optionalResult, result allegrosql.SelectResult) {
	if optionalResult == nil {
		tr.optionalFinished = true
		tr.result = result
		return
	}
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *blockResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			return err
		}
		if chk.NumEvents() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.Next(ctx, chk)
}

func (tr *blockResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (tr *blockResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return err
}
