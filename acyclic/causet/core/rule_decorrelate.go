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
	"context"
	"math"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/types"
)

// canPullUpAgg checks if an apply can pull an aggregation up.
func (la *LogicalApply) canPullUpAgg() bool {
	if la.JoinType != InnerJoin && la.JoinType != LeftOuterJoin {
		return false
	}
	if len(la.EqualConditions)+len(la.LeftConditions)+len(la.RightConditions)+len(la.OtherConditions) > 0 {
		return false
	}
	return len(la.children[0].Schema().Keys) > 0
}

// canPullUp checks if an aggregation can be pulled up. An aggregate function like count(*) cannot be pulled up.
func (la *LogicalAggregation) canPullUp() bool {
	if len(la.GroupByItems) > 0 {
		return false
	}
	for _, f := range la.AggFuncs {
		for _, arg := range f.Args {
			expr := memex.EvaluateExprWithNull(la.ctx, la.children[0].Schema(), arg)
			if con, ok := expr.(*memex.Constant); !ok || !con.Value.IsNull() {
				return false
			}
		}
	}
	return true
}

// deCorDefCausFromEqExpr checks whether it's an equal condition of form `col = correlated col`. If so we will change the decorrelated
// column to normal column to make a new equal condition.
func (la *LogicalApply) deCorDefCausFromEqExpr(expr memex.Expression) memex.Expression {
	sf, ok := expr.(*memex.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return nil
	}
	if col, lOk := sf.GetArgs()[0].(*memex.DeferredCauset); lOk {
		if corDefCaus, rOk := sf.GetArgs()[1].(*memex.CorrelatedDeferredCauset); rOk {
			ret := corDefCaus.Decorrelate(la.Schema())
			if _, ok := ret.(*memex.CorrelatedDeferredCauset); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return memex.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), ret, col)
		}
	}
	if corDefCaus, lOk := sf.GetArgs()[0].(*memex.CorrelatedDeferredCauset); lOk {
		if col, rOk := sf.GetArgs()[1].(*memex.DeferredCauset); rOk {
			ret := corDefCaus.Decorrelate(la.Schema())
			if _, ok := ret.(*memex.CorrelatedDeferredCauset); ok {
				return nil
			}
			// We should make sure that the equal condition's left side is the join's left join key, right is the right key.
			return memex.NewFunctionInternal(la.ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), ret, col)
		}
	}
	return nil
}

// ExtractCorrelatedDefCauss4LogicalCauset recursively extracts all of the correlated columns
// from a plan tree by calling LogicalCauset.ExtractCorrelatedDefCauss.
func ExtractCorrelatedDefCauss4LogicalCauset(p LogicalCauset) []*memex.CorrelatedDeferredCauset {
	corDefCauss := p.ExtractCorrelatedDefCauss()
	for _, child := range p.Children() {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4LogicalCauset(child)...)
	}
	return corDefCauss
}

// ExtractCorrelatedDefCauss4PhysicalCauset recursively extracts all of the correlated columns
// from a plan tree by calling PhysicalCauset.ExtractCorrelatedDefCauss.
func ExtractCorrelatedDefCauss4PhysicalCauset(p PhysicalCauset) []*memex.CorrelatedDeferredCauset {
	corDefCauss := p.ExtractCorrelatedDefCauss()
	for _, child := range p.Children() {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
	}
	return corDefCauss
}

// decorrelateSolver tries to convert apply plan to join plan.
type decorrelateSolver struct{}

func (s *decorrelateSolver) aggDefaultValueMap(agg *LogicalAggregation) map[int]*memex.Constant {
	defaultValueMap := make(map[int]*memex.Constant, len(agg.AggFuncs))
	for i, f := range agg.AggFuncs {
		switch f.Name {
		case ast.AggFuncBitOr, ast.AggFuncBitXor, ast.AggFuncCount:
			defaultValueMap[i] = memex.NewZero()
		case ast.AggFuncBitAnd:
			defaultValueMap[i] = &memex.Constant{Value: types.NewUintCauset(math.MaxUint64), RetType: types.NewFieldType(allegrosql.TypeLonglong)}
		}
	}
	return defaultValueMap
}

// optimize implements logicalOptMemrule interface.
func (s *decorrelateSolver) optimize(ctx context.Context, p LogicalCauset) (LogicalCauset, error) {
	if apply, ok := p.(*LogicalApply); ok {
		outerCauset := apply.children[0]
		innerCauset := apply.children[1]
		apply.CorDefCauss = extractCorDeferredCausetsBySchema4LogicalCauset(apply.children[1], apply.children[0].Schema())
		if len(apply.CorDefCauss) == 0 {
			// If the inner plan is non-correlated, the apply will be simplified to join.
			join := &apply.LogicalJoin
			join.self = join
			p = join
		} else if sel, ok := innerCauset.(*LogicalSelection); ok {
			// If the inner plan is a selection, we add this condition to join predicates.
			// Notice that no matter what HoTT of join is, it's always right.
			newConds := make([]memex.Expression, 0, len(sel.Conditions))
			for _, cond := range sel.Conditions {
				newConds = append(newConds, cond.Decorrelate(outerCauset.Schema()))
			}
			apply.AttachOnConds(newConds)
			innerCauset = sel.children[0]
			apply.SetChildren(outerCauset, innerCauset)
			return s.optimize(ctx, p)
		} else if m, ok := innerCauset.(*LogicalMaxOneRow); ok {
			if m.children[0].MaxOneRow() {
				innerCauset = m.children[0]
				apply.SetChildren(outerCauset, innerCauset)
				return s.optimize(ctx, p)
			}
		} else if proj, ok := innerCauset.(*LogicalProjection); ok {
			for i, expr := range proj.Exprs {
				proj.Exprs[i] = expr.Decorrelate(outerCauset.Schema())
			}
			apply.columnSubstitute(proj.Schema(), proj.Exprs)
			innerCauset = proj.children[0]
			apply.SetChildren(outerCauset, innerCauset)
			if apply.JoinType != SemiJoin && apply.JoinType != LeftOuterSemiJoin && apply.JoinType != AntiSemiJoin && apply.JoinType != AntiLeftOuterSemiJoin {
				proj.SetSchema(apply.Schema())
				proj.Exprs = append(memex.DeferredCauset2Exprs(outerCauset.Schema().Clone().DeferredCausets), proj.Exprs...)
				apply.SetSchema(memex.MergeSchema(outerCauset.Schema(), innerCauset.Schema()))
				np, err := s.optimize(ctx, p)
				if err != nil {
					return nil, err
				}
				proj.SetChildren(np)
				return proj, nil
			}
			return s.optimize(ctx, p)
		} else if agg, ok := innerCauset.(*LogicalAggregation); ok {
			if apply.canPullUpAgg() && agg.canPullUp() {
				innerCauset = agg.children[0]
				apply.JoinType = LeftOuterJoin
				apply.SetChildren(outerCauset, innerCauset)
				agg.SetSchema(apply.Schema())
				agg.GroupByItems = memex.DeferredCauset2Exprs(outerCauset.Schema().Keys[0])
				newAggFuncs := make([]*aggregation.AggFuncDesc, 0, apply.Schema().Len())

				outerDefCaussInSchema := make([]*memex.DeferredCauset, 0, outerCauset.Schema().Len())
				for i, col := range outerCauset.Schema().DeferredCausets {
					first, err := aggregation.NewAggFuncDesc(agg.ctx, ast.AggFuncFirstRow, []memex.Expression{col}, false)
					if err != nil {
						return nil, err
					}
					newAggFuncs = append(newAggFuncs, first)

					outerDefCaus, _ := outerCauset.Schema().DeferredCausets[i].Clone().(*memex.DeferredCauset)
					outerDefCaus.RetType = first.RetTp
					outerDefCaussInSchema = append(outerDefCaussInSchema, outerDefCaus)
				}
				apply.SetSchema(memex.MergeSchema(memex.NewSchema(outerDefCaussInSchema...), innerCauset.Schema()))
				resetNotNullFlag(apply.schemaReplicant, outerCauset.Schema().Len(), apply.schemaReplicant.Len())

				for i, aggFunc := range agg.AggFuncs {
					if idx := apply.schemaReplicant.DeferredCausetIndex(aggFunc.Args[0].(*memex.DeferredCauset)); idx != -1 {
						desc, err := aggregation.NewAggFuncDesc(agg.ctx, agg.AggFuncs[i].Name, []memex.Expression{apply.schemaReplicant.DeferredCausets[idx]}, false)
						if err != nil {
							return nil, err
						}
						newAggFuncs = append(newAggFuncs, desc)
					}
				}
				agg.AggFuncs = newAggFuncs
				np, err := s.optimize(ctx, p)
				if err != nil {
					return nil, err
				}
				agg.SetChildren(np)
				// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
				// agg.buildProjectionIfNecessary()
				agg.collectGroupByDeferredCausets()
				return agg, nil
			}
			// We can pull up the equal conditions below the aggregation as the join key of the apply, if only
			// the equal conditions contain the correlated column of this apply.
			if sel, ok := agg.children[0].(*LogicalSelection); ok && apply.JoinType == LeftOuterJoin {
				var (
					eqCondWithCorDefCaus []*memex.ScalarFunction
					remainedExpr     []memex.Expression
				)
				// Extract the equal condition.
				for _, cond := range sel.Conditions {
					if expr := apply.deCorDefCausFromEqExpr(cond); expr != nil {
						eqCondWithCorDefCaus = append(eqCondWithCorDefCaus, expr.(*memex.ScalarFunction))
					} else {
						remainedExpr = append(remainedExpr, cond)
					}
				}
				if len(eqCondWithCorDefCaus) > 0 {
					originalExpr := sel.Conditions
					sel.Conditions = remainedExpr
					apply.CorDefCauss = extractCorDeferredCausetsBySchema4LogicalCauset(apply.children[1], apply.children[0].Schema())
					// There's no other correlated column.
					groupByDefCauss := memex.NewSchema(agg.groupByDefCauss...)
					if len(apply.CorDefCauss) == 0 {
						join := &apply.LogicalJoin
						join.EqualConditions = append(join.EqualConditions, eqCondWithCorDefCaus...)
						for _, eqCond := range eqCondWithCorDefCaus {
							clonedDefCaus := eqCond.GetArgs()[1].(*memex.DeferredCauset)
							// If the join key is not in the aggregation's schemaReplicant, add first event function.
							if agg.schemaReplicant.DeferredCausetIndex(eqCond.GetArgs()[1].(*memex.DeferredCauset)) == -1 {
								newFunc, err := aggregation.NewAggFuncDesc(apply.ctx, ast.AggFuncFirstRow, []memex.Expression{clonedDefCaus}, false)
								if err != nil {
									return nil, err
								}
								agg.AggFuncs = append(agg.AggFuncs, newFunc)
								agg.schemaReplicant.Append(clonedDefCaus)
								agg.schemaReplicant.DeferredCausets[agg.schemaReplicant.Len()-1].RetType = newFunc.RetTp
							}
							// If group by defcaus don't contain the join key, add it into this.
							if !groupByDefCauss.Contains(clonedDefCaus) {
								agg.GroupByItems = append(agg.GroupByItems, clonedDefCaus)
								groupByDefCauss.Append(clonedDefCaus)
							}
						}
						agg.collectGroupByDeferredCausets()
						// The selection may be useless, check and remove it.
						if len(sel.Conditions) == 0 {
							agg.SetChildren(sel.children[0])
						}
						defaultValueMap := s.aggDefaultValueMap(agg)
						// We should use it directly, rather than building a projection.
						if len(defaultValueMap) > 0 {
							proj := LogicalProjection{}.Init(agg.ctx, agg.blockOffset)
							proj.SetSchema(apply.schemaReplicant)
							proj.Exprs = memex.DeferredCauset2Exprs(apply.schemaReplicant.DeferredCausets)
							for i, val := range defaultValueMap {
								pos := proj.schemaReplicant.DeferredCausetIndex(agg.schemaReplicant.DeferredCausets[i])
								ifNullFunc := memex.NewFunctionInternal(agg.ctx, ast.Ifnull, types.NewFieldType(allegrosql.TypeLonglong), agg.schemaReplicant.DeferredCausets[i], val)
								proj.Exprs[pos] = ifNullFunc
							}
							proj.SetChildren(apply)
							p = proj
						}
						return s.optimize(ctx, p)
					}
					sel.Conditions = originalExpr
					apply.CorDefCauss = extractCorDeferredCausetsBySchema4LogicalCauset(apply.children[1], apply.children[0].Schema())
				}
			}
		} else if sort, ok := innerCauset.(*LogicalSort); ok {
			// Since we only pull up Selection, Projection, Aggregation, MaxOneRow,
			// the top level Sort has no effect on the subquery's result.
			innerCauset = sort.children[0]
			apply.SetChildren(outerCauset, innerCauset)
			return s.optimize(ctx, p)
		}
	}
	newChildren := make([]LogicalCauset, 0, len(p.Children()))
	for _, child := range p.Children() {
		np, err := s.optimize(ctx, child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, np)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func (*decorrelateSolver) name() string {
	return "decorrelate"
}
