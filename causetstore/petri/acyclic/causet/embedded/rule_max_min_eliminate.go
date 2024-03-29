// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"context"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/types"
)

// maxMinEliminator tries to eliminate max/min aggregate function.
// For ALLEGROALLEGROSQL like `select max(id) from t;`, we could optimize it to `select max(id) from (select id from t order by id desc limit 1 where id is not null) t;`.
// For ALLEGROALLEGROSQL like `select min(id) from t;`, we could optimize it to `select max(id) from (select id from t order by id limit 1 where id is not null) t;`.
// For ALLEGROALLEGROSQL like `select max(id), min(id) from t;`, we could optimize it to the cartesianJoin result of the two queries above if `id` has an index.
type maxMinEliminator struct {
}

func (a *maxMinEliminator) optimize(ctx context.Context, p LogicalCauset) (LogicalCauset, error) {
	return a.eliminateMaxMin(p), nil
}

// composeAggsByInnerJoin composes the scalar aggregations by cartesianJoin.
func (a *maxMinEliminator) composeAggsByInnerJoin(aggs []*LogicalAggregation) (plan LogicalCauset) {
	plan = aggs[0]
	sctx := plan.SCtx()
	for i := 1; i < len(aggs); i++ {
		join := LogicalJoin{JoinType: InnerJoin}.Init(sctx, plan.SelectBlockOffset())
		join.SetChildren(plan, aggs[i])
		join.schemaReplicant = buildLogicalJoinSchema(InnerJoin, join)
		join.cartesianJoin = true
		plan = join
	}
	return
}

// checkDefCausCanUseIndex checks whether there is an AccessPath satisfy the conditions:
// 1. all of the selection's condition can be pushed down as AccessConds of the path.
// 2. the path can keep order for `col` after pushing down the conditions.
func (a *maxMinEliminator) checkDefCausCanUseIndex(plan LogicalCauset, col *memex.DeferredCauset, conditions []memex.Expression) bool {
	switch p := plan.(type) {
	case *LogicalSelection:
		conditions = append(conditions, p.Conditions...)
		return a.checkDefCausCanUseIndex(p.children[0], col, conditions)
	case *DataSource:
		// Check whether there is an AccessPath can use index for col.
		for _, path := range p.possibleAccessPaths {
			if path.IsIntHandlePath {
				// Since causet path can contain accessConds of at most one column,
				// we only need to check if all of the conditions can be pushed down as accessConds
				// and `col` is the handle column.
				if p.handleDefCauss != nil && col.Equal(nil, p.handleDefCauss.GetDefCaus(0)) {
					if _, filterConds := ranger.DetachCondsForDeferredCauset(p.ctx, conditions, col); len(filterConds) != 0 {
						return false
					}
					return true
				}
			} else {
				indexDefCauss, indexDefCausLen := path.FullIdxDefCauss, path.FullIdxDefCausLens
				if path.IsCommonHandlePath {
					indexDefCauss, indexDefCausLen = p.commonHandleDefCauss, p.commonHandleLens
				}
				// 1. whether all of the conditions can be pushed down as accessConds.
				// 2. whether the AccessPath can satisfy the order property of `col` with these accessConds.
				result, err := ranger.DetachCondAndBuildRangeForIndex(p.ctx, conditions, indexDefCauss, indexDefCausLen)
				if err != nil || len(result.RemainedConds) != 0 {
					continue
				}
				for i := 0; i <= result.EqCondCount; i++ {
					if i < len(indexDefCauss) && col.Equal(nil, indexDefCauss[i]) {
						return true
					}
				}
			}
		}
		return false
	default:
		return false
	}
}

// cloneSubCausets shallow clones the subCauset. We only consider `Selection` and `DataSource` here,
// because we have restricted the subCauset in `checkDefCausCanUseIndex`.
func (a *maxMinEliminator) cloneSubCausets(plan LogicalCauset) LogicalCauset {
	switch p := plan.(type) {
	case *LogicalSelection:
		newConditions := make([]memex.Expression, len(p.Conditions))
		copy(newConditions, p.Conditions)
		sel := LogicalSelection{Conditions: newConditions}.Init(p.ctx, p.blockOffset)
		sel.SetChildren(a.cloneSubCausets(p.children[0]))
		return sel
	case *DataSource:
		// Quick clone a DataSource.
		// ReadOnly fields uses a shallow copy, while the fields which will be overwritten must use a deep copy.
		newDs := *p
		newDs.baseLogicalCauset = newBaseLogicalCauset(p.ctx, p.tp, &newDs, p.blockOffset)
		newDs.schemaReplicant = p.schemaReplicant.Clone()
		newDs.DeferredCausets = make([]*perceptron.DeferredCausetInfo, len(p.DeferredCausets))
		copy(newDs.DeferredCausets, p.DeferredCausets)
		newAccessPaths := make([]*soliton.AccessPath, 0, len(p.possibleAccessPaths))
		for _, path := range p.possibleAccessPaths {
			newPath := *path
			newAccessPaths = append(newAccessPaths, &newPath)
		}
		newDs.possibleAccessPaths = newAccessPaths
		return &newDs
	}
	// This won't happen, because we have checked the subtree.
	return nil
}

// splitAggFuncAndChecHoTTices splits the agg to multiple aggs and check whether each agg needs a sort
// after the transformation. For example, we firstly split the allegrosql: `select max(a), min(a), max(b) from t` ->
// `select max(a) from t` + `select min(a) from t` + `select max(b) from t`.
// Then we check whether `a` and `b` have indices. If any of the used column has no index, we cannot eliminate
// this aggregation.
func (a *maxMinEliminator) splitAggFuncAndChecHoTTices(agg *LogicalAggregation) (aggs []*LogicalAggregation, canEliminate bool) {
	for _, f := range agg.AggFuncs {
		// We must make sure the args of max/min is a simple single column.
		col, ok := f.Args[0].(*memex.DeferredCauset)
		if !ok {
			return nil, false
		}
		if !a.checkDefCausCanUseIndex(agg.children[0], col, make([]memex.Expression, 0)) {
			return nil, false
		}
	}
	aggs = make([]*LogicalAggregation, 0, len(agg.AggFuncs))
	// we can split the aggregation only if all of the aggFuncs pass the check.
	for i, f := range agg.AggFuncs {
		newAgg := LogicalAggregation{AggFuncs: []*aggregation.AggFuncDesc{f}}.Init(agg.ctx, agg.blockOffset)
		newAgg.SetChildren(a.cloneSubCausets(agg.children[0]))
		newAgg.schemaReplicant = memex.NewSchema(agg.schemaReplicant.DeferredCausets[i])
		if err := newAgg.PruneDeferredCausets([]*memex.DeferredCauset{newAgg.schemaReplicant.DeferredCausets[0]}); err != nil {
			return nil, false
		}
		aggs = append(aggs, newAgg)
	}
	return aggs, true
}

// eliminateSingleMaxMin tries to convert a single max/min to Limit+Sort operators.
func (a *maxMinEliminator) eliminateSingleMaxMin(agg *LogicalAggregation) *LogicalAggregation {
	f := agg.AggFuncs[0]
	child := agg.Children()[0]
	ctx := agg.SCtx()

	// If there's no column in f.GetArgs()[0], we still need limit and read data from real causet because the result should be NULL if the input is empty.
	if len(memex.ExtractDeferredCausets(f.Args[0])) > 0 {
		// If it can be NULL, we need to filter NULL out first.
		if !allegrosql.HasNotNullFlag(f.Args[0].GetType().Flag) {
			sel := LogicalSelection{}.Init(ctx, agg.blockOffset)
			isNullFunc := memex.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), f.Args[0])
			notNullFunc := memex.NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(allegrosql.TypeTiny), isNullFunc)
			sel.Conditions = []memex.Expression{notNullFunc}
			sel.SetChildren(agg.Children()[0])
			child = sel
		}

		// Add Sort and Limit operators.
		// For max function, the sort order should be desc.
		desc := f.Name == ast.AggFuncMax
		// Compose Sort operator.
		sort := LogicalSort{}.Init(ctx, agg.blockOffset)
		sort.ByItems = append(sort.ByItems, &soliton.ByItems{Expr: f.Args[0], Desc: desc})
		sort.SetChildren(child)
		child = sort
	}

	// Compose Limit operator.
	li := LogicalLimit{Count: 1}.Init(ctx, agg.blockOffset)
	li.SetChildren(child)

	// If no data in the child, we need to return NULL instead of empty. This cannot be done by sort and limit themselves.
	// Since now there would be at most one event returned, the remained agg operator is not expensive anymore.
	agg.SetChildren(li)
	return agg
}

// eliminateMaxMin tries to convert max/min to Limit+Sort operators.
func (a *maxMinEliminator) eliminateMaxMin(p LogicalCauset) LogicalCauset {
	newChildren := make([]LogicalCauset, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChildren = append(newChildren, a.eliminateMaxMin(child))
	}
	p.SetChildren(newChildren...)
	if agg, ok := p.(*LogicalAggregation); ok {
		if len(agg.GroupByItems) != 0 {
			return agg
		}
		// Make sure that all of the aggFuncs are Max or Min.
		for _, aggFunc := range agg.AggFuncs {
			if aggFunc.Name != ast.AggFuncMax && aggFunc.Name != ast.AggFuncMin {
				return agg
			}
		}
		if len(agg.AggFuncs) == 1 {
			// If there is only one aggFunc, we don't need to guarantee that the child of it is a data
			// source, or whether the sort can be eliminated. This transformation won't be worse than previous.
			return a.eliminateSingleMaxMin(agg)
		}
		// If we have more than one aggFunc, we can eliminate this agg only if all of the aggFuncs can benefit from
		// their column's index.
		aggs, canEliminate := a.splitAggFuncAndChecHoTTices(agg)
		if !canEliminate {
			return agg
		}
		for i := range aggs {
			aggs[i] = a.eliminateSingleMaxMin(aggs[i])
		}
		return a.composeAggsByInnerJoin(aggs)
	}
	return p
}

func (*maxMinEliminator) name() string {
	return "max_min_eliminate"
}
