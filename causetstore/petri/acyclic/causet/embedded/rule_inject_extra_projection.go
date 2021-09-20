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

package embedded

import (
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// injectExtraProjection is used to extract the memexs of specific
// operators into a physical Projection operator and inject the Projection below
// the operators. Thus we can accelerate the memex evaluation by eager
// evaluation.
func injectExtraProjection(plan PhysicalCauset) PhysicalCauset {
	return NewProjInjector().inject(plan)
}

type projInjector struct {
}

// NewProjInjector builds a projInjector.
func NewProjInjector() *projInjector {
	return &projInjector{}
}

func (pe *projInjector) inject(plan PhysicalCauset) PhysicalCauset {
	for i, child := range plan.Children() {
		plan.Children()[i] = pe.inject(child)
	}

	switch p := plan.(type) {
	case *PhysicalHashAgg:
		plan = InjectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalStreamAgg:
		plan = InjectProjBelowAgg(plan, p.AggFuncs, p.GroupByItems)
	case *PhysicalSort:
		plan = InjectProjBelowSort(p, p.ByItems)
	case *PhysicalTopN:
		plan = InjectProjBelowSort(p, p.ByItems)
	case *NominalSort:
		plan = TurnNominalSortIntoProj(p, p.OnlyDeferredCauset, p.ByItems)
	}
	return plan
}

// wrapCastForAggFunc wraps the args of an aggregate function with a cast function.
// If the mode is FinalMode or Partial2Mode, we do not need to wrap cast upon the args,
// since the types of the args are already the expected.
func wrapCastForAggFuncs(sctx stochastikctx.Context, aggFuncs []*aggregation.AggFuncDesc) {
	for i := range aggFuncs {
		if aggFuncs[i].Mode != aggregation.FinalMode && aggFuncs[i].Mode != aggregation.Partial2Mode {
			aggFuncs[i].WrapCastForAggArgs(sctx)
		}
	}
}

// InjectProjBelowAgg injects a ProjOperator below AggOperator. So that All
// scalar functions in aggregation may speed up by vectorized evaluation in
// the `proj`. If all the args of `aggFuncs`, and all the item of `groupByItems`
// are columns or constants, we do not need to build the `proj`.
func InjectProjBelowAgg(aggCauset PhysicalCauset, aggFuncs []*aggregation.AggFuncDesc, groupByItems []memex.Expression) PhysicalCauset {
	hasScalarFunc := false

	wrapCastForAggFuncs(aggCauset.SCtx(), aggFuncs)
	for i := 0; !hasScalarFunc && i < len(aggFuncs); i++ {
		for _, arg := range aggFuncs[i].Args {
			_, isScalarFunc := arg.(*memex.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
		for _, byItem := range aggFuncs[i].OrderByItems {
			_, isScalarFunc := byItem.Expr.(*memex.ScalarFunction)
			hasScalarFunc = hasScalarFunc || isScalarFunc
		}
	}
	for i := 0; !hasScalarFunc && i < len(groupByItems); i++ {
		_, isScalarFunc := groupByItems[i].(*memex.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return aggCauset
	}

	projSchemaDefCauss := make([]*memex.DeferredCauset, 0, len(aggFuncs)+len(groupByItems))
	projExprs := make([]memex.Expression, 0, cap(projSchemaDefCauss))
	cursor := 0

	for _, f := range aggFuncs {
		for i, arg := range f.Args {
			if _, isCnst := arg.(*memex.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, arg)
			newArg := &memex.DeferredCauset{
				UniqueID: aggCauset.SCtx().GetStochastikVars().AllocCausetDeferredCausetID(),
				RetType:  arg.GetType(),
				Index:    cursor,
			}
			projSchemaDefCauss = append(projSchemaDefCauss, newArg)
			f.Args[i] = newArg
			cursor++
		}
		for _, byItem := range f.OrderByItems {
			if _, isCnst := byItem.Expr.(*memex.Constant); isCnst {
				continue
			}
			projExprs = append(projExprs, byItem.Expr)
			newArg := &memex.DeferredCauset{
				UniqueID: aggCauset.SCtx().GetStochastikVars().AllocCausetDeferredCausetID(),
				RetType:  byItem.Expr.GetType(),
				Index:    cursor,
			}
			projSchemaDefCauss = append(projSchemaDefCauss, newArg)
			byItem.Expr = newArg
			cursor++
		}
	}

	for i, item := range groupByItems {
		if _, isCnst := item.(*memex.Constant); isCnst {
			continue
		}
		projExprs = append(projExprs, item)
		newArg := &memex.DeferredCauset{
			UniqueID: aggCauset.SCtx().GetStochastikVars().AllocCausetDeferredCausetID(),
			RetType:  item.GetType(),
			Index:    cursor,
		}
		projSchemaDefCauss = append(projSchemaDefCauss, newArg)
		groupByItems[i] = newArg
		cursor++
	}

	child := aggCauset.Children()[0]
	prop := aggCauset.GetChildReqProps(0).Clone()
	proj := PhysicalProjection{
		Exprs:                        projExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(aggCauset.SCtx(), child.statsInfo().ScaleByExpectCnt(prop.ExpectedCnt), aggCauset.SelectBlockOffset(), prop)
	proj.SetSchema(memex.NewSchema(projSchemaDefCauss...))
	proj.SetChildren(child)

	aggCauset.SetChildren(proj)
	return aggCauset
}

// InjectProjBelowSort extracts the ScalarFunctions of `orderByItems` into a
// PhysicalProjection and injects it below PhysicalTopN/PhysicalSort. The schemaReplicant
// of PhysicalSort and PhysicalTopN are the same as the schemaReplicant of their
// children. When a projection is injected as the child of PhysicalSort and
// PhysicalTopN, some extra columns will be added into the schemaReplicant of the
// Projection, thus we need to add another Projection upon them to prune the
// redundant columns.
func InjectProjBelowSort(p PhysicalCauset, orderByItems []*soliton.ByItems) PhysicalCauset {
	hasScalarFunc, numOrderByItems := false, len(orderByItems)
	for i := 0; !hasScalarFunc && i < numOrderByItems; i++ {
		_, isScalarFunc := orderByItems[i].Expr.(*memex.ScalarFunction)
		hasScalarFunc = hasScalarFunc || isScalarFunc
	}
	if !hasScalarFunc {
		return p
	}

	topProjExprs := make([]memex.Expression, 0, p.Schema().Len())
	for i := range p.Schema().DeferredCausets {
		col := p.Schema().DeferredCausets[i].Clone().(*memex.DeferredCauset)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                        topProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), p.statsInfo(), p.SelectBlockOffset(), nil)
	topProj.SetSchema(p.Schema().Clone())
	topProj.SetChildren(p)

	childCauset := p.Children()[0]
	bottomProjSchemaDefCauss := make([]*memex.DeferredCauset, 0, len(childCauset.Schema().DeferredCausets)+numOrderByItems)
	bottomProjExprs := make([]memex.Expression, 0, len(childCauset.Schema().DeferredCausets)+numOrderByItems)
	for _, col := range childCauset.Schema().DeferredCausets {
		newDefCaus := col.Clone().(*memex.DeferredCauset)
		newDefCaus.Index = childCauset.Schema().DeferredCausetIndex(newDefCaus)
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newDefCaus)
		bottomProjExprs = append(bottomProjExprs, newDefCaus)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*memex.ScalarFunction); !isScalarFunc {
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &memex.DeferredCauset{
			UniqueID: p.SCtx().GetStochastikVars().AllocCausetDeferredCausetID(),
			RetType:  itemExpr.GetType(),
			Index:    len(bottomProjSchemaDefCauss),
		}
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newArg)
		item.Expr = newArg
	}

	childProp := p.GetChildReqProps(0).Clone()
	bottomProj := PhysicalProjection{
		Exprs:                        bottomProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), childCauset.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	bottomProj.SetSchema(memex.NewSchema(bottomProjSchemaDefCauss...))
	bottomProj.SetChildren(childCauset)
	p.SetChildren(bottomProj)

	if origChildProj, isChildProj := childCauset.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}

	return topProj
}

// TurnNominalSortIntoProj will turn nominal sort into two projections. This is to check if the scalar functions will
// overflow.
func TurnNominalSortIntoProj(p PhysicalCauset, onlyDeferredCauset bool, orderByItems []*soliton.ByItems) PhysicalCauset {
	if onlyDeferredCauset {
		return p.Children()[0]
	}

	numOrderByItems := len(orderByItems)
	childCauset := p.Children()[0]

	bottomProjSchemaDefCauss := make([]*memex.DeferredCauset, 0, len(childCauset.Schema().DeferredCausets)+numOrderByItems)
	bottomProjExprs := make([]memex.Expression, 0, len(childCauset.Schema().DeferredCausets)+numOrderByItems)
	for _, col := range childCauset.Schema().DeferredCausets {
		newDefCaus := col.Clone().(*memex.DeferredCauset)
		newDefCaus.Index = childCauset.Schema().DeferredCausetIndex(newDefCaus)
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newDefCaus)
		bottomProjExprs = append(bottomProjExprs, newDefCaus)
	}

	for _, item := range orderByItems {
		itemExpr := item.Expr
		if _, isScalarFunc := itemExpr.(*memex.ScalarFunction); !isScalarFunc {
			continue
		}
		bottomProjExprs = append(bottomProjExprs, itemExpr)
		newArg := &memex.DeferredCauset{
			UniqueID: p.SCtx().GetStochastikVars().AllocCausetDeferredCausetID(),
			RetType:  itemExpr.GetType(),
			Index:    len(bottomProjSchemaDefCauss),
		}
		bottomProjSchemaDefCauss = append(bottomProjSchemaDefCauss, newArg)
	}

	childProp := p.GetChildReqProps(0).Clone()
	bottomProj := PhysicalProjection{
		Exprs:                        bottomProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), childCauset.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	bottomProj.SetSchema(memex.NewSchema(bottomProjSchemaDefCauss...))
	bottomProj.SetChildren(childCauset)

	topProjExprs := make([]memex.Expression, 0, childCauset.Schema().Len())
	for i := range childCauset.Schema().DeferredCausets {
		col := childCauset.Schema().DeferredCausets[i].Clone().(*memex.DeferredCauset)
		col.Index = i
		topProjExprs = append(topProjExprs, col)
	}
	topProj := PhysicalProjection{
		Exprs:                        topProjExprs,
		AvoidDeferredCausetEvaluator: false,
	}.Init(p.SCtx(), childCauset.statsInfo().ScaleByExpectCnt(childProp.ExpectedCnt), p.SelectBlockOffset(), childProp)
	topProj.SetSchema(childCauset.Schema().Clone())
	topProj.SetChildren(bottomProj)

	if origChildProj, isChildProj := childCauset.(*PhysicalProjection); isChildProj {
		refine4NeighbourProj(bottomProj, origChildProj)
	}

	return topProj
}
