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

package cascades

import (
	"math"

	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	impl "github.com/whtcorpsinc/milevadb/causet/implementation"
	"github.com/whtcorpsinc/milevadb/causet/memo"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/memex"
)

// ImplementationMemrule defines the interface for implementation rules.
type ImplementationMemrule interface {
	// Match checks if current GroupExpr matches this rule under required physical property.
	Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool)
	// OnImplement generates physical plan using this rule for current GroupExpr. Note that
	// childrenReqProps of generated physical plan should be set correspondingly in this function.
	OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error)
}

var defaultImplementationMap = map[memo.Operand][]ImplementationMemrule{
	memo.OperandBlockDual: {
		&ImplBlockDual{},
	},
	memo.OperandMemBlockScan: {
		&ImplMemBlockScan{},
	},
	memo.OperandProjection: {
		&ImplProjection{},
	},
	memo.OperandBlockScan: {
		&ImplBlockScan{},
	},
	memo.OperandIndexScan: {
		&ImplIndexScan{},
	},
	memo.OperandEinsteinDBSingleGather: {
		&ImplEinsteinDBSingleReadGather{},
	},
	memo.OperandShow: {
		&ImplShow{},
	},
	memo.OperandSelection: {
		&ImplSelection{},
	},
	memo.OperandSort: {
		&ImplSort{},
	},
	memo.OperanPosetDaggregation: {
		&ImplHashAgg{},
	},
	memo.OperandLimit: {
		&ImplLimit{},
	},
	memo.OperandTopN: {
		&ImplTopN{},
		&ImplTopNAsLimit{},
	},
	memo.OperandJoin: {
		&ImplHashJoinBuildLeft{},
		&ImplHashJoinBuildRight{},
		&ImplMergeJoin{},
	},
	memo.OperandUnionAll: {
		&ImplUnionAll{},
	},
	memo.OperandApply: {
		&ImplApply{},
	},
	memo.OperandMaxOneRow: {
		&ImplMaxOneRow{},
	},
	memo.OperandWindow: {
		&ImplWindow{},
	},
}

// ImplBlockDual implements LogicalBlockDual as PhysicalBlockDual.
type ImplBlockDual struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplBlockDual) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	if !prop.IsEmpty() {
		return false
	}
	return true
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplBlockDual) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicDual := expr.ExprNode.(*causetembedded.LogicalBlockDual)
	dual := causetembedded.PhysicalBlockDual{RowCount: logicDual.RowCount}.Init(logicDual.SCtx(), logicProp.Stats, logicDual.SelectBlockOffset())
	dual.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewBlockDualImpl(dual)}, nil
}

// ImplMemBlockScan implements LogicalMemBlock as PhysicalMemBlock.
type ImplMemBlockScan struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplMemBlockScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	if !prop.IsEmpty() {
		return false
	}
	return true
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplMemBlockScan) OnImplement(
	expr *memo.GroupExpr,
	reqProp *property.PhysicalProperty,
) ([]memo.Implementation, error) {
	logic := expr.ExprNode.(*causetembedded.LogicalMemBlock)
	logicProp := expr.Group.Prop
	physical := causetembedded.PhysicalMemBlock{
		DBName:          logic.DBName,
		Block:           logic.BlockInfo,
		DeferredCausets: logic.BlockInfo.DeferredCausets,
		Extractor:       logic.Extractor,
	}.Init(logic.SCtx(), logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logic.SelectBlockOffset())
	physical.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewMemBlockScanImpl(physical)}, nil
}

// ImplProjection implements LogicalProjection as PhysicalProjection.
type ImplProjection struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplProjection) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplProjection) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicProj := expr.ExprNode.(*causetembedded.LogicalProjection)
	childProp, ok := logicProj.TryToGetChildProp(reqProp)
	if !ok {
		return nil, nil
	}
	proj := causetembedded.PhysicalProjection{
		Exprs:                        logicProj.Exprs,
		CalculateNoDelay:             logicProj.CalculateNoDelay,
		AvoidDeferredCausetEvaluator: logicProj.AvoidDeferredCausetEvaluator,
	}.Init(logicProj.SCtx(), logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logicProj.SelectBlockOffset(), childProp)
	proj.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewProjectionImpl(proj)}, nil
}

// ImplEinsteinDBSingleReadGather implements EinsteinDBSingleGather
// as PhysicalBlockReader or PhysicalIndexReader.
type ImplEinsteinDBSingleReadGather struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplEinsteinDBSingleReadGather) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplEinsteinDBSingleReadGather) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	sg := expr.ExprNode.(*causetembedded.EinsteinDBSingleGather)
	if sg.IsIndexGather {
		reader := sg.GetPhysicalIndexReader(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), reqProp)
		return []memo.Implementation{impl.NewIndexReaderImpl(reader, sg.Source.TblDefCausHists)}, nil
	}
	reader := sg.GetPhysicalBlockReader(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), reqProp)
	return []memo.Implementation{impl.NewBlockReaderImpl(reader, sg.Source.TblDefCausHists)}, nil
}

// ImplBlockScan implements BlockScan as PhysicalBlockScan.
type ImplBlockScan struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplBlockScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	ts := expr.ExprNode.(*causetembedded.LogicalBlockScan)
	return prop.IsEmpty() || (len(prop.Items) == 1 && ts.HandleDefCauss != nil && prop.Items[0].DefCaus.Equal(nil, ts.HandleDefCauss.GetDefCaus(0)))
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplBlockScan) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicalScan := expr.ExprNode.(*causetembedded.LogicalBlockScan)
	ts := logicalScan.GetPhysicalScan(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		ts.KeepOrder = true
		ts.Desc = reqProp.Items[0].Desc
	}
	tblDefCauss, tblDefCausHists := logicalScan.Source.TblDefCauss, logicalScan.Source.TblDefCausHists
	return []memo.Implementation{impl.NewBlockScanImpl(ts, tblDefCauss, tblDefCausHists)}, nil
}

// ImplIndexScan implements IndexScan as PhysicalIndexScan.
type ImplIndexScan struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplIndexScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	is := expr.ExprNode.(*causetembedded.LogicalIndexScan)
	return is.MatchIndexProp(prop)
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplIndexScan) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalScan := expr.ExprNode.(*causetembedded.LogicalIndexScan)
	is := logicalScan.GetPhysicalIndexScan(expr.Group.Prop.Schema, expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		is.KeepOrder = true
		if reqProp.Items[0].Desc {
			is.Desc = true
		}
	}
	return []memo.Implementation{impl.NewIndexScanImpl(is, logicalScan.Source.TblDefCausHists)}, nil
}

// ImplShow is the implementation rule which implements LogicalShow to
// PhysicalShow.
type ImplShow struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplShow) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplShow) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	show := expr.ExprNode.(*causetembedded.LogicalShow)

	// TODO(zz-jason): unifying LogicalShow and PhysicalShow to a single
	// struct. So that we don't need to create a new PhysicalShow object, which
	// can help us to reduce the gc pressure of golang runtime and improve the
	// overall performance.
	showPhys := causetembedded.PhysicalShow{ShowContents: show.ShowContents}.Init(show.SCtx())
	showPhys.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewShowImpl(showPhys)}, nil
}

// ImplSelection is the implementation rule which implements LogicalSelection
// to PhysicalSelection.
type ImplSelection struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplSelection) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplSelection) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalSel := expr.ExprNode.(*causetembedded.LogicalSelection)
	physicalSel := causetembedded.PhysicalSelection{
		Conditions: logicalSel.Conditions,
	}.Init(logicalSel.SCtx(), expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logicalSel.SelectBlockOffset(), reqProp.Clone())
	switch expr.Group.EngineType {
	case memo.EngineMilevaDB:
		return []memo.Implementation{impl.NewMilevaDBSelectionImpl(physicalSel)}, nil
	case memo.EngineEinsteinDB:
		return []memo.Implementation{impl.NewEinsteinDBSelectionImpl(physicalSel)}, nil
	default:
		return nil, causetembedded.ErrInternal.GenWithStack("Unsupported EngineType '%s' for Selection.", expr.Group.EngineType.String())
	}
}

// ImplSort is the implementation rule which implements LogicalSort
// to PhysicalSort or NominalSort.
type ImplSort struct {
}

// Match implements ImplementationMemrule match interface.
func (r *ImplSort) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	ls := expr.ExprNode.(*causetembedded.LogicalSort)
	return causetembedded.MatchItems(prop, ls.ByItems)
}

// OnImplement implements ImplementationMemrule OnImplement interface.
// If all of the sort items are defCausumns, generate a NominalSort, otherwise
// generate a PhysicalSort.
func (r *ImplSort) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	ls := expr.ExprNode.(*causetembedded.LogicalSort)
	if newProp, canUseNominal := causetembedded.GetPropByOrderByItems(ls.ByItems); canUseNominal {
		newProp.ExpectedCnt = reqProp.ExpectedCnt
		ns := causetembedded.NominalSort{}.Init(
			ls.SCtx(), expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), ls.SelectBlockOffset(), newProp)
		return []memo.Implementation{impl.NewNominalSortImpl(ns)}, nil
	}
	ps := causetembedded.PhysicalSort{ByItems: ls.ByItems}.Init(
		ls.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		ls.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	return []memo.Implementation{impl.NewSortImpl(ps)}, nil
}

// ImplHashAgg is the implementation rule which implements LogicalAggregation
// to PhysicalHashAgg.
type ImplHashAgg struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplHashAgg) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	// TODO: deal with the hints when we have implemented StreamAgg.
	return prop.IsEmpty()
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplHashAgg) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	la := expr.ExprNode.(*causetembedded.LogicalAggregation)
	hashAgg := causetembedded.NewPhysicalHashAgg(
		la,
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	hashAgg.SetSchema(expr.Group.Prop.Schema.Clone())
	switch expr.Group.EngineType {
	case memo.EngineMilevaDB:
		return []memo.Implementation{impl.NewMilevaDBHashAggImpl(hashAgg)}, nil
	case memo.EngineEinsteinDB:
		return []memo.Implementation{impl.NewEinsteinDBHashAggImpl(hashAgg)}, nil
	default:
		return nil, causetembedded.ErrInternal.GenWithStack("Unsupported EngineType '%s' for HashAggregation.", expr.Group.EngineType.String())
	}
}

// ImplLimit is the implementation rule which implements LogicalLimit
// to PhysicalLimit.
type ImplLimit struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplLimit) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplLimit) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalLimit := expr.ExprNode.(*causetembedded.LogicalLimit)
	newProp := &property.PhysicalProperty{ExpectedCnt: float64(logicalLimit.Count + logicalLimit.Offset)}
	physicalLimit := causetembedded.PhysicalLimit{
		Offset: logicalLimit.Offset,
		Count:  logicalLimit.Count,
	}.Init(logicalLimit.SCtx(), expr.Group.Prop.Stats, logicalLimit.SelectBlockOffset(), newProp)
	return []memo.Implementation{impl.NewLimitImpl(physicalLimit)}, nil
}

// ImplTopN is the implementation rule which implements LogicalTopN
// to PhysicalTopN.
type ImplTopN struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplTopN) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	topN := expr.ExprNode.(*causetembedded.LogicalTopN)
	if expr.Group.EngineType != memo.EngineMilevaDB {
		return prop.IsEmpty()
	}
	return causetembedded.MatchItems(prop, topN.ByItems)
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplTopN) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	lt := expr.ExprNode.(*causetembedded.LogicalTopN)
	resultProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	topN := causetembedded.PhysicalTopN{
		ByItems: lt.ByItems,
		Count:   lt.Count,
		Offset:  lt.Offset,
	}.Init(lt.SCtx(), expr.Group.Prop.Stats, lt.SelectBlockOffset(), resultProp)
	switch expr.Group.EngineType {
	case memo.EngineMilevaDB:
		return []memo.Implementation{impl.NewMilevaDBTopNImpl(topN)}, nil
	case memo.EngineEinsteinDB:
		return []memo.Implementation{impl.NewEinsteinDBTopNImpl(topN)}, nil
	default:
		return nil, causetembedded.ErrInternal.GenWithStack("Unsupported EngineType '%s' for TopN.", expr.Group.EngineType.String())
	}
}

// ImplTopNAsLimit is the implementation rule which implements LogicalTopN
// as PhysicalLimit with required order property.
type ImplTopNAsLimit struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplTopNAsLimit) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	topN := expr.ExprNode.(*causetembedded.LogicalTopN)
	_, canUseLimit := causetembedded.GetPropByOrderByItems(topN.ByItems)
	return canUseLimit && causetembedded.MatchItems(prop, topN.ByItems)
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplTopNAsLimit) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	lt := expr.ExprNode.(*causetembedded.LogicalTopN)
	newProp := &property.PhysicalProperty{ExpectedCnt: float64(lt.Count + lt.Offset)}
	newProp.Items = make([]property.Item, len(lt.ByItems))
	for i, item := range lt.ByItems {
		newProp.Items[i].DefCaus = item.Expr.(*memex.DeferredCauset)
		newProp.Items[i].Desc = item.Desc
	}
	physicalLimit := causetembedded.PhysicalLimit{
		Offset: lt.Offset,
		Count:  lt.Count,
	}.Init(lt.SCtx(), expr.Group.Prop.Stats, lt.SelectBlockOffset(), newProp)
	return []memo.Implementation{impl.NewLimitImpl(physicalLimit)}, nil
}

func getImplForHashJoin(expr *memo.GroupExpr, prop *property.PhysicalProperty, innerIdx int, useOuterToBuild bool) memo.Implementation {
	join := expr.ExprNode.(*causetembedded.LogicalJoin)
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[0] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	chReqProps[1] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	stats := expr.Group.Prop.Stats
	if prop.ExpectedCnt < stats.RowCount {
		expCntScale := prop.ExpectedCnt / stats.RowCount
		chReqProps[1-innerIdx].ExpectedCnt = expr.Children[1-innerIdx].Prop.Stats.RowCount * expCntScale
	}
	hashJoin := causetembedded.NewPhysicalHashJoin(join, innerIdx, useOuterToBuild, stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(expr.Group.Prop.Schema)
	return impl.NewHashJoinImpl(hashJoin)
}

// ImplHashJoinBuildLeft implements LogicalJoin to PhysicalHashJoin which uses the left child to build hash causet.
type ImplHashJoinBuildLeft struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplHashJoinBuildLeft) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	switch expr.ExprNode.(*causetembedded.LogicalJoin).JoinType {
	case causetembedded.InnerJoin, causetembedded.LeftOuterJoin, causetembedded.RightOuterJoin:
		return prop.IsEmpty()
	default:
		return false
	}
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplHashJoinBuildLeft) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	join := expr.ExprNode.(*causetembedded.LogicalJoin)
	switch join.JoinType {
	case causetembedded.InnerJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 0, false)}, nil
	case causetembedded.LeftOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, true)}, nil
	case causetembedded.RightOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 0, false)}, nil
	default:
		return nil, nil
	}
}

// ImplHashJoinBuildRight implements LogicalJoin to PhysicalHashJoin which uses the right child to build hash causet.
type ImplHashJoinBuildRight struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplHashJoinBuildRight) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplHashJoinBuildRight) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	join := expr.ExprNode.(*causetembedded.LogicalJoin)
	switch join.JoinType {
	case causetembedded.SemiJoin, causetembedded.AntiSemiJoin,
		causetembedded.LeftOuterSemiJoin, causetembedded.AntiLeftOuterSemiJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, false)}, nil
	case causetembedded.InnerJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, false)}, nil
	case causetembedded.LeftOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, false)}, nil
	case causetembedded.RightOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 0, true)}, nil
	}
	return nil, nil
}

// ImplMergeJoin implements LogicalMergeJoin to PhysicalMergeJoin.
type ImplMergeJoin struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplMergeJoin) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplMergeJoin) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	join := expr.ExprNode.(*causetembedded.LogicalJoin)
	physicalMergeJoins := join.GetMergeJoin(reqProp, expr.Schema(), expr.Group.Prop.Stats, expr.Children[0].Prop.Stats, expr.Children[1].Prop.Stats)
	mergeJoinImpls := make([]memo.Implementation, 0, len(physicalMergeJoins))
	for _, physicalCauset := range physicalMergeJoins {
		physicalMergeJoin := physicalCauset.(*causetembedded.PhysicalMergeJoin)
		mergeJoinImpls = append(mergeJoinImpls, impl.NewMergeJoinImpl(physicalMergeJoin))
	}
	return mergeJoinImpls, nil
}

// ImplUnionAll implements LogicalUnionAll to PhysicalUnionAll.
type ImplUnionAll struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplUnionAll) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (r *ImplUnionAll) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalUnion := expr.ExprNode.(*causetembedded.LogicalUnionAll)
	chReqProps := make([]*property.PhysicalProperty, len(expr.Children))
	for i := range expr.Children {
		chReqProps[i] = &property.PhysicalProperty{ExpectedCnt: reqProp.ExpectedCnt}
	}
	physicalUnion := causetembedded.PhysicalUnionAll{}.Init(
		logicalUnion.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		logicalUnion.SelectBlockOffset(),
		chReqProps...,
	)
	physicalUnion.SetSchema(expr.Group.Prop.Schema)
	return []memo.Implementation{impl.NewUnionAllImpl(physicalUnion)}, nil
}

// ImplApply implements LogicalApply to PhysicalApply
type ImplApply struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplApply) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.AllDefCaussFromSchema(expr.Children[0].Prop.Schema)
}

// OnImplement implements ImplementationMemrule OnImplement interface
func (r *ImplApply) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	la := expr.ExprNode.(*causetembedded.LogicalApply)
	join := la.GetHashJoin(reqProp)
	physicalApply := causetembedded.PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorDefCauss,
	}.Init(
		la.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		la.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: reqProp.Items},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	physicalApply.SetSchema(expr.Group.Prop.Schema)
	return []memo.Implementation{impl.NewApplyImpl(physicalApply)}, nil
}

// ImplMaxOneRow implements LogicalMaxOneRow to PhysicalMaxOneRow.
type ImplMaxOneRow struct {
}

// Match implements ImplementationMemrule Match interface.
func (r *ImplMaxOneRow) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationMemrule OnImplement interface
func (r *ImplMaxOneRow) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	mor := expr.ExprNode.(*causetembedded.LogicalMaxOneRow)
	physicalMaxOneRow := causetembedded.PhysicalMaxOneRow{}.Init(
		mor.SCtx(),
		expr.Group.Prop.Stats,
		mor.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: 2})
	return []memo.Implementation{impl.NewMaxOneRowImpl(physicalMaxOneRow)}, nil
}

// ImplWindow implements LogicalWindow to PhysicalWindow.
type ImplWindow struct {
}

// Match implements ImplementationMemrule Match interface.
func (w *ImplWindow) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	lw := expr.ExprNode.(*causetembedded.LogicalWindow)
	var byItems []property.Item
	byItems = append(byItems, lw.PartitionBy...)
	byItems = append(byItems, lw.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems}
	return prop.IsPrefix(childProperty)
}

// OnImplement implements ImplementationMemrule OnImplement interface.
func (w *ImplWindow) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	lw := expr.ExprNode.(*causetembedded.LogicalWindow)
	var byItems []property.Item
	byItems = append(byItems, lw.PartitionBy...)
	byItems = append(byItems, lw.OrderBy...)
	physicalWindow := causetembedded.PhysicalWindow{
		WindowFuncDescs: lw.WindowFuncDescs,
		PartitionBy:     lw.PartitionBy,
		OrderBy:         lw.OrderBy,
		Frame:           lw.Frame,
	}.Init(
		lw.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		lw.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems},
	)
	physicalWindow.SetSchema(expr.Group.Prop.Schema)
	return []memo.Implementation{impl.NewWindowImpl(physicalWindow)}, nil
}
