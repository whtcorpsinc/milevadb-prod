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

package core

import (
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/allegrosql"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// ToPB implements PhysicalCauset ToPB interface.
func (p *basePhysicalCauset) ToPB(_ stochastikctx.Context, _ ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.baseCauset.ExplainID())
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	sc := ctx.GetStochastikVars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := memex.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggInterDirc := &fidelpb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		aggInterDirc.AggFunc = append(aggInterDirc.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	interlockID := ""
	if storeType == ekv.TiFlash {
		var err error
		aggInterDirc.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		interlockID = p.ExplainID().String()
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeAggregation, Aggregation: aggInterDirc, InterlockingDirectorateId: &interlockID}, nil
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	sc := ctx.GetStochastikVars().StmtCtx
	client := ctx.GetClient()
	groupByExprs, err := memex.ExpressionsToPBList(sc, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggInterDirc := &fidelpb.Aggregation{
		GroupBy: groupByExprs,
	}
	for _, aggFunc := range p.AggFuncs {
		aggInterDirc.AggFunc = append(aggInterDirc.AggFunc, aggregation.AggFuncToPBExpr(sc, client, aggFunc))
	}
	interlockID := ""
	if storeType == ekv.TiFlash {
		var err error
		aggInterDirc.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		interlockID = p.ExplainID().String()
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeStreamAgg, Aggregation: aggInterDirc, InterlockingDirectorateId: &interlockID}, nil
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalSelection) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	sc := ctx.GetStochastikVars().StmtCtx
	client := ctx.GetClient()
	conditions, err := memex.ExpressionsToPBList(sc, p.Conditions, client)
	if err != nil {
		return nil, err
	}
	selInterDirc := &fidelpb.Selection{
		Conditions: conditions,
	}
	interlockID := ""
	if storeType == ekv.TiFlash {
		var err error
		selInterDirc.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		interlockID = p.ExplainID().String()
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeSelection, Selection: selInterDirc, InterlockingDirectorateId: &interlockID}, nil
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalTopN) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	sc := ctx.GetStochastikVars().StmtCtx
	client := ctx.GetClient()
	topNInterDirc := &fidelpb.TopN{
		Limit: p.Count,
	}
	for _, item := range p.ByItems {
		topNInterDirc.OrderBy = append(topNInterDirc.OrderBy, memex.SortByItemToPB(sc, client, item.Expr, item.Desc))
	}
	interlockID := ""
	if storeType == ekv.TiFlash {
		var err error
		topNInterDirc.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		interlockID = p.ExplainID().String()
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeTopN, TopN: topNInterDirc, InterlockingDirectorateId: &interlockID}, nil
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalLimit) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	limitInterDirc := &fidelpb.Limit{
		Limit: p.Count,
	}
	interlockID := ""
	if storeType == ekv.TiFlash {
		var err error
		limitInterDirc.Child, err = p.children[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		interlockID = p.ExplainID().String()
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeLimit, Limit: limitInterDirc, InterlockingDirectorateId: &interlockID}, nil
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalBlockScan) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	tsInterDirc := blocks.BuildBlockScanFromInfos(p.Block, p.DeferredCausets)
	tsInterDirc.Desc = p.Desc
	if p.isPartition {
		tsInterDirc.BlockId = p.physicalBlockID
	}
	interlockID := ""
	if storeType == ekv.TiFlash && p.IsGlobalRead {
		tsInterDirc.NextReadEngine = fidelpb.EngineType_TiFlash
		ranges := allegrosql.BlockRangesToKVRanges(tsInterDirc.BlockId, p.Ranges, nil)
		for _, keyRange := range ranges {
			tsInterDirc.Ranges = append(tsInterDirc.Ranges, fidelpb.KeyRange{Low: keyRange.StartKey, High: keyRange.EndKey})
		}
	}
	if storeType == ekv.TiFlash {
		interlockID = p.ExplainID().String()
	}
	err := SetPBDeferredCausetsDefaultValue(ctx, tsInterDirc.DeferredCausets, p.DeferredCausets)
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeBlockScan, TblScan: tsInterDirc, InterlockingDirectorateId: &interlockID}, err
}

// checkCoverIndex checks whether we can pass unique info to EinsteinDB. We should push it if and only if the length of
// range and index are equal.
func checkCoverIndex(idx *perceptron.IndexInfo, ranges []*ranger.Range) bool {
	// If the index is (c1, c2) but the query range only contains c1, it is not a unique get.
	if !idx.Unique {
		return false
	}
	for _, rg := range ranges {
		if len(rg.LowVal) != len(idx.DeferredCausets) {
			return false
		}
	}
	return true
}

func findDeferredCausetInfoByID(infos []*perceptron.DeferredCausetInfo, id int64) *perceptron.DeferredCausetInfo {
	for _, info := range infos {
		if info.ID == id {
			return info
		}
	}
	return nil
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalIndexScan) ToPB(ctx stochastikctx.Context, _ ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	columns := make([]*perceptron.DeferredCausetInfo, 0, p.schemaReplicant.Len())
	blockDeferredCausets := p.Block.DefCauss()
	for _, col := range p.schemaReplicant.DeferredCausets {
		if col.ID == perceptron.ExtraHandleID {
			columns = append(columns, perceptron.NewExtraHandleDefCausInfo())
		} else {
			columns = append(columns, findDeferredCausetInfoByID(blockDeferredCausets, col.ID))
		}
	}
	var pkDefCausIds []int64
	if p.NeedCommonHandle {
		pkDefCausIds = blocks.TryGetCommonPkDeferredCausetIds(p.Block)
	}
	idxInterDirc := &fidelpb.IndexScan{
		BlockId:          p.Block.ID,
		IndexId:          p.Index.ID,
		DeferredCausets:          soliton.DeferredCausetsToProto(columns, p.Block.PKIsHandle),
		Desc:             p.Desc,
		PrimaryDeferredCausetIds: pkDefCausIds,
	}
	if p.isPartition {
		idxInterDirc.BlockId = p.physicalBlockID
	}
	unique := checkCoverIndex(p.Index, p.Ranges)
	idxInterDirc.Unique = &unique
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeIndexScan, IdxScan: idxInterDirc}, nil
}

// ToPB implements PhysicalCauset ToPB interface.
func (p *PhysicalBroadCastJoin) ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	sc := ctx.GetStochastikVars().StmtCtx
	client := ctx.GetClient()
	leftJoinKeys := make([]memex.Expression, 0, len(p.LeftJoinKeys))
	rightJoinKeys := make([]memex.Expression, 0, len(p.RightJoinKeys))
	for _, leftKey := range p.LeftJoinKeys {
		leftJoinKeys = append(leftJoinKeys, leftKey)
	}
	for _, rightKey := range p.RightJoinKeys {
		rightJoinKeys = append(rightJoinKeys, rightKey)
	}
	lChildren, err := p.children[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rChildren, err := p.children[1].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	left, err := memex.ExpressionsToPBList(sc, leftJoinKeys, client)
	if err != nil {
		return nil, err
	}
	right, err := memex.ExpressionsToPBList(sc, rightJoinKeys, client)
	if err != nil {
		return nil, err
	}
	pbJoinType := fidelpb.JoinType_TypeInnerJoin
	switch p.JoinType {
	case LeftOuterJoin:
		pbJoinType = fidelpb.JoinType_TypeLeftOuterJoin
	case RightOuterJoin:
		pbJoinType = fidelpb.JoinType_TypeRightOuterJoin
	}
	join := &fidelpb.Join{
		JoinType:      pbJoinType,
		JoinInterDircType:  fidelpb.JoinInterDircType_TypeHashJoin,
		InnerIdx:      int64(p.InnerChildIdx),
		LeftJoinKeys:  left,
		RightJoinKeys: right,
		Children:      []*fidelpb.InterlockingDirectorate{lChildren, rChildren},
	}

	interlockID := p.ExplainID().String()
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeJoin, Join: join, InterlockingDirectorateId: &interlockID}, nil
}

// SetPBDeferredCausetsDefaultValue sets the default values of fidelpb.DeferredCausetInfos.
func SetPBDeferredCausetsDefaultValue(ctx stochastikctx.Context, pbDeferredCausets []*fidelpb.DeferredCausetInfo, columns []*perceptron.DeferredCausetInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that EinsteinDB will return NULL properly,
		// They real values will be compute later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbDeferredCausets[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.OriginDefaultValue == nil {
			continue
		}

		sessVars := ctx.GetStochastikVars()
		originStrict := sessVars.StrictALLEGROSQLMode
		sessVars.StrictALLEGROSQLMode = false
		d, err := causet.GetDefCausOriginDefaultValue(ctx, c)
		sessVars.StrictALLEGROSQLMode = originStrict
		if err != nil {
			return err
		}

		pbDeferredCausets[i].DefaultVal, err = blockcodec.EncodeValue(sessVars.StmtCtx, nil, d)
		if err != nil {
			return err
		}
	}
	return nil
}

// SupportStreaming returns true if a pushed down operation supports using interlock streaming API.
// Note that this function handle pushed down physical plan only! It's called in constructPosetDagReq.
// Some plans are difficult (if possible) to implement streaming, and some are pointless to do so.
// TODO: Support more HoTTs of physical plan.
func SupportStreaming(p PhysicalCauset) bool {
	switch p.(type) {
	case *PhysicalIndexScan, *PhysicalSelection, *PhysicalBlockScan:
		return true
	}
	return false
}
