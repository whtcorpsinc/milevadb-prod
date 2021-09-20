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
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type pFIDelSolver struct{}

func (s *pFIDelSolver) optimize(ctx context.Context, lp LogicalCauset) (LogicalCauset, error) {
	_, p := lp.PredicatePushDown(nil)
	return p, nil
}

func addSelection(p LogicalCauset, child LogicalCauset, conditions []memex.Expression, chIdx int) {
	if len(conditions) == 0 {
		p.Children()[chIdx] = child
		return
	}
	conditions = memex.PropagateConstant(p.SCtx(), conditions)
	// Return causet dual when filter is constant false or null.
	dual := Conds2BlockDual(child, conditions)
	if dual != nil {
		p.Children()[chIdx] = dual
		return
	}
	selection := LogicalSelection{Conditions: conditions}.Init(p.SCtx(), p.SelectBlockOffset())
	selection.SetChildren(child)
	p.Children()[chIdx] = selection
}

// PredicatePushDown implements LogicalCauset interface.
func (p *baseLogicalCauset) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	if len(p.children) == 0 {
		return predicates, p.self
	}
	child := p.children[0]
	rest, newChild := child.PredicatePushDown(predicates)
	addSelection(p.self, newChild, rest, 0)
	return nil, p.self
}

func splitSetGetVarFunc(filters []memex.Expression) ([]memex.Expression, []memex.Expression) {
	canBePushDown := make([]memex.Expression, 0, len(filters))
	canNotBePushDown := make([]memex.Expression, 0, len(filters))
	for _, expr := range filters {
		if memex.HasGetSetVarFunc(expr) {
			canNotBePushDown = append(canNotBePushDown, expr)
		} else {
			canBePushDown = append(canBePushDown, expr)
		}
	}
	return canBePushDown, canNotBePushDown
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalSelection) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	canBePushDown, canNotBePushDown := splitSetGetVarFunc(p.Conditions)
	retConditions, child := p.children[0].PredicatePushDown(append(canBePushDown, predicates...))
	retConditions = append(retConditions, canNotBePushDown...)
	if len(retConditions) > 0 {
		p.Conditions = memex.PropagateConstant(p.ctx, retConditions)
		// Return causet dual when filter is constant false or null.
		dual := Conds2BlockDual(p, p.Conditions)
		if dual != nil {
			return nil, dual
		}
		return nil, p
	}
	return nil, child
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalUnionScan) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	retainedPredicates, _ := p.children[0].PredicatePushDown(predicates)
	p.conditions = make([]memex.Expression, 0, len(predicates))
	p.conditions = append(p.conditions, predicates...)
	// The conditions in UnionScan is only used for added rows, so parent Selection should not be removed.
	return retainedPredicates, p
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (ds *DataSource) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	ds.allConds = predicates
	ds.pushedDownConds, predicates = memex.PushDownExprs(ds.ctx.GetStochastikVars().StmtCtx, predicates, ds.ctx.GetClient(), ekv.UnSpecified)
	return predicates, ds
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalBlockDual) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	return predicates, p
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalJoin) PredicatePushDown(predicates []memex.Expression) (ret []memex.Expression, retCauset LogicalCauset) {
	simplifyOuterJoin(p, predicates)
	var equalCond []*memex.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []memex.Expression
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2BlockDual(p, predicates)
		if dual != nil {
			return ret, dual
		}
		// Handle where conditions
		predicates = memex.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := DeriveOtherConditions(p, false, true)
		rightCond = append(p.RightConditions, derivedRightJoinCond...)
		p.RightConditions = nil
		ret = append(memex.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case RightOuterJoin:
		predicates = p.outerJoinPropConst(predicates)
		dual := Conds2BlockDual(p, predicates)
		if dual != nil {
			return ret, dual
		}
		// Handle where conditions
		predicates = memex.ExtractFiltersFromDNFs(p.ctx, predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := DeriveOtherConditions(p, true, false)
		leftCond = append(p.LeftConditions, derivedLeftJoinCond...)
		p.LeftConditions = nil
		ret = append(memex.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case SemiJoin, InnerJoin:
		tempCond := make([]memex.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, memex.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		tempCond = memex.ExtractFiltersFromDNFs(p.ctx, tempCond)
		tempCond = memex.PropagateConstant(p.ctx, tempCond)
		// Return causet dual when filter is constant false or null.
		dual := Conds2BlockDual(p, tempCond)
		if dual != nil {
			return ret, dual
		}
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(tempCond, true, true)
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	case AntiSemiJoin:
		predicates = memex.PropagateConstant(p.ctx, predicates)
		// Return causet dual when filter is constant false or null.
		dual := Conds2BlockDual(p, predicates)
		if dual != nil {
			return ret, dual
		}
		// `predicates` should only contain left conditions or constant filters.
		_, leftPushCond, rightPushCond, _ = p.extractOnCondition(predicates, true, true)
		// Do not derive `is not null` for anti join, since it may cause wrong results.
		// For example:
		// `select * from t t1 where t1.a not in (select b from t t2)` does not imply `t2.b is not null`,
		// `select * from t t1 where t1.a not in (select a from t t2 where t1.b = t2.b` does not imply `t1.b is not null`,
		// `select * from t t1 where not exists (select * from t t2 where t2.a = t1.a)` does not imply `t1.a is not null`,
		leftCond = leftPushCond
		rightCond = append(p.RightConditions, rightPushCond...)
		p.RightConditions = nil

	}
	leftCond = memex.RemoveDupExprs(p.ctx, leftCond)
	rightCond = memex.RemoveDupExprs(p.ctx, rightCond)
	leftRet, lCh := p.children[0].PredicatePushDown(leftCond)
	rightRet, rCh := p.children[1].PredicatePushDown(rightCond)
	addSelection(p, lCh, leftRet, 0)
	addSelection(p, rCh, rightRet, 1)
	p.uFIDelateEQCond()
	p.mergeSchema()
	buildKeyInfo(p)
	return ret, p.self
}

// uFIDelateEQCond will extract the arguments of a equal condition that connect two memexs.
func (p *LogicalJoin) uFIDelateEQCond() {
	lChild, rChild := p.children[0], p.children[1]
	var lKeys, rKeys []memex.Expression
	for i := len(p.OtherConditions) - 1; i >= 0; i-- {
		need2Remove := false
		if eqCond, ok := p.OtherConditions[i].(*memex.ScalarFunction); ok && eqCond.FuncName.L == ast.EQ {
			// If it is a column equal condition converted from `[not] in (subq)`, do not move it
			// to EqualConditions, and keep it in OtherConditions. Reference comments in `extractOnCondition`
			// for detailed reasons.
			if memex.IsEQCondFromIn(eqCond) {
				continue
			}
			lExpr, rExpr := eqCond.GetArgs()[0], eqCond.GetArgs()[1]
			if memex.ExprFromSchema(lExpr, lChild.Schema()) && memex.ExprFromSchema(rExpr, rChild.Schema()) {
				lKeys = append(lKeys, lExpr)
				rKeys = append(rKeys, rExpr)
				need2Remove = true
			} else if memex.ExprFromSchema(lExpr, rChild.Schema()) && memex.ExprFromSchema(rExpr, lChild.Schema()) {
				lKeys = append(lKeys, rExpr)
				rKeys = append(rKeys, lExpr)
				need2Remove = true
			}
		}
		if need2Remove {
			p.OtherConditions = append(p.OtherConditions[:i], p.OtherConditions[i+1:]...)
		}
	}
	if len(lKeys) > 0 {
		needLProj, needRProj := false, false
		for i := range lKeys {
			_, lOk := lKeys[i].(*memex.DeferredCauset)
			_, rOk := rKeys[i].(*memex.DeferredCauset)
			needLProj = needLProj || !lOk
			needRProj = needRProj || !rOk
		}

		var lProj, rProj *LogicalProjection
		if needLProj {
			lProj = p.getProj(0)
		}
		if needRProj {
			rProj = p.getProj(1)
		}
		for i := range lKeys {
			lKey, rKey := lKeys[i], rKeys[i]
			if lProj != nil {
				lKey = lProj.appendExpr(lKey)
			}
			if rProj != nil {
				rKey = rProj.appendExpr(rKey)
			}
			eqCond := memex.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), lKey, rKey)
			p.EqualConditions = append(p.EqualConditions, eqCond.(*memex.ScalarFunction))
		}
	}
}

func (p *LogicalProjection) appendExpr(expr memex.Expression) *memex.DeferredCauset {
	if col, ok := expr.(*memex.DeferredCauset); ok {
		return col
	}
	expr = memex.DeferredCausetSubstitute(expr, p.schemaReplicant, p.Exprs)
	p.Exprs = append(p.Exprs, expr)

	col := &memex.DeferredCauset{
		UniqueID: p.ctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  expr.GetType(),
	}
	col.SetCoercibility(expr.Coercibility())
	p.schemaReplicant.Append(col)
	return col
}

func (p *LogicalJoin) getProj(idx int) *LogicalProjection {
	child := p.children[idx]
	proj, ok := child.(*LogicalProjection)
	if ok {
		return proj
	}
	proj = LogicalProjection{Exprs: make([]memex.Expression, 0, child.Schema().Len())}.Init(p.ctx, child.SelectBlockOffset())
	for _, col := range child.Schema().DeferredCausets {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(child.Schema().Clone())
	proj.SetChildren(child)
	p.children[idx] = proj
	return proj
}

// simplifyOuterJoin transforms "LeftOuterJoin/RightOuterJoin" to "InnerJoin" if possible.
func simplifyOuterJoin(p *LogicalJoin, predicates []memex.Expression) {
	if p.JoinType != LeftOuterJoin && p.JoinType != RightOuterJoin && p.JoinType != InnerJoin {
		return
	}

	innerBlock := p.children[0]
	outerBlock := p.children[1]
	if p.JoinType == LeftOuterJoin {
		innerBlock, outerBlock = outerBlock, innerBlock
	}

	// first simplify embedded outer join.
	if innerCauset, ok := innerBlock.(*LogicalJoin); ok {
		simplifyOuterJoin(innerCauset, predicates)
	}
	if outerCauset, ok := outerBlock.(*LogicalJoin); ok {
		simplifyOuterJoin(outerCauset, predicates)
	}

	if p.JoinType == InnerJoin {
		return
	}
	// then simplify embedding outer join.
	canBeSimplified := false
	for _, expr := range predicates {
		// avoid the case where the expr only refers to the schemaReplicant of outerBlock
		if memex.ExprFromSchema(expr, outerBlock.Schema()) {
			continue
		}
		isOk := isNullRejected(p.ctx, innerBlock.Schema(), expr)
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = InnerJoin
	}
}

// isNullRejected check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner causet that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullRejected(ctx stochastikctx.Context, schemaReplicant *memex.Schema, expr memex.Expression) bool {
	expr = memex.PushDownNot(ctx, expr)
	sc := ctx.GetStochastikVars().StmtCtx
	sc.InNullRejectCheck = true
	result := memex.EvaluateExprWithNull(ctx, schemaReplicant, expr)
	sc.InNullRejectCheck = false
	x, ok := result.(*memex.Constant)
	if !ok {
		return false
	}
	if x.Value.IsNull() {
		return true
	} else if isTrue, err := x.Value.ToBool(sc); err == nil && isTrue == 0 {
		return true
	}
	return false
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalProjection) PredicatePushDown(predicates []memex.Expression) (ret []memex.Expression, retCauset LogicalCauset) {
	canBePushed := make([]memex.Expression, 0, len(predicates))
	canNotBePushed := make([]memex.Expression, 0, len(predicates))
	for _, expr := range p.Exprs {
		if memex.HasAssignSetVarFunc(expr) {
			_, child := p.baseLogicalCauset.PredicatePushDown(nil)
			return predicates, child
		}
	}
	for _, cond := range predicates {
		newFilter := memex.DeferredCausetSubstitute(cond, p.Schema(), p.Exprs)
		if !memex.HasGetSetVarFunc(newFilter) {
			canBePushed = append(canBePushed, memex.DeferredCausetSubstitute(cond, p.Schema(), p.Exprs))
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	remained, child := p.baseLogicalCauset.PredicatePushDown(canBePushed)
	return append(remained, canNotBePushed...), child
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalUnionAll) PredicatePushDown(predicates []memex.Expression) (ret []memex.Expression, retCauset LogicalCauset) {
	for i, proj := range p.children {
		newExprs := make([]memex.Expression, 0, len(predicates))
		newExprs = append(newExprs, predicates...)
		retCond, newChild := proj.PredicatePushDown(newExprs)
		addSelection(p, newChild, retCond, i)
	}
	return nil, p
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (la *LogicalAggregation) PredicatePushDown(predicates []memex.Expression) (ret []memex.Expression, retCauset LogicalCauset) {
	var condsToPush []memex.Expression
	exprsOriginal := make([]memex.Expression, 0, len(la.AggFuncs))
	for _, fun := range la.AggFuncs {
		exprsOriginal = append(exprsOriginal, fun.Args[0])
	}
	groupByDeferredCausets := memex.NewSchema(la.groupByDefCauss...)
	for _, cond := range predicates {
		switch cond.(type) {
		case *memex.Constant:
			condsToPush = append(condsToPush, cond)
			// Consider ALLEGROALLEGROSQL list "select sum(b) from t group by a having 1=0". "1=0" is a constant predicate which should be
			// retained and pushed down at the same time. Because we will get a wrong query result that contains one column
			// with value 0 rather than an empty query result.
			ret = append(ret, cond)
		case *memex.ScalarFunction:
			extractedDefCauss := memex.ExtractDeferredCausets(cond)
			ok := true
			for _, col := range extractedDefCauss {
				if !groupByDeferredCausets.Contains(col) {
					ok = false
					break
				}
			}
			if ok {
				newFunc := memex.DeferredCausetSubstitute(cond, la.Schema(), exprsOriginal)
				condsToPush = append(condsToPush, newFunc)
			} else {
				ret = append(ret, cond)
			}
		default:
			ret = append(ret, cond)
		}
	}
	la.baseLogicalCauset.PredicatePushDown(condsToPush)
	return ret, la
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalLimit) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	// Limit forbids any condition to push down.
	p.baseLogicalCauset.PredicatePushDown(nil)
	return predicates, p
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalMaxOneRow) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	// MaxOneRow forbids any condition to push down.
	p.baseLogicalCauset.PredicatePushDown(nil)
	return predicates, p
}

// DeriveOtherConditions given a LogicalJoin, check the OtherConditions to see if we can derive more
// conditions for left/right child pushdown.
func DeriveOtherConditions(p *LogicalJoin, deriveLeft bool, deriveRight bool) (leftCond []memex.Expression,
	rightCond []memex.Expression) {
	leftCauset, rightCauset := p.children[0], p.children[1]
	isOuterSemi := (p.JoinType == LeftOuterSemiJoin) || (p.JoinType == AntiLeftOuterSemiJoin)
	for _, expr := range p.OtherConditions {
		if deriveLeft {
			leftRelaxedCond := memex.DeriveRelaxedFiltersFromDNF(expr, leftCauset.Schema())
			if leftRelaxedCond != nil {
				leftCond = append(leftCond, leftRelaxedCond)
			}
			notNullExpr := deriveNotNullExpr(expr, leftCauset.Schema())
			if notNullExpr != nil {
				leftCond = append(leftCond, notNullExpr)
			}
		}
		if deriveRight {
			rightRelaxedCond := memex.DeriveRelaxedFiltersFromDNF(expr, rightCauset.Schema())
			if rightRelaxedCond != nil {
				rightCond = append(rightCond, rightRelaxedCond)
			}
			// For LeftOuterSemiJoin and AntiLeftOuterSemiJoin, we can actually generate
			// `col is not null` according to memexs in `OtherConditions` now, but we
			// are putting column equal condition converted from `in (subq)` into
			// `OtherConditions`(@sa https://github.com/whtcorpsinc/milevadb/pull/9051), then it would
			// cause wrong results, so we disable this optimization for outer semi joins now.
			// TODO enable this optimization for outer semi joins later by checking whether
			// condition in `OtherConditions` is converted from `in (subq)`.
			if isOuterSemi {
				continue
			}
			notNullExpr := deriveNotNullExpr(expr, rightCauset.Schema())
			if notNullExpr != nil {
				rightCond = append(rightCond, notNullExpr)
			}
		}
	}
	return
}

// deriveNotNullExpr generates a new memex `not(isnull(col))` given `col1 op col2`,
// in which `col` is in specified schemaReplicant. Caller guarantees that only one of `col1` or
// `col2` is in schemaReplicant.
func deriveNotNullExpr(expr memex.Expression, schemaReplicant *memex.Schema) memex.Expression {
	binop, ok := expr.(*memex.ScalarFunction)
	if !ok || len(binop.GetArgs()) != 2 {
		return nil
	}
	ctx := binop.GetCtx()
	arg0, lOK := binop.GetArgs()[0].(*memex.DeferredCauset)
	arg1, rOK := binop.GetArgs()[1].(*memex.DeferredCauset)
	if !lOK || !rOK {
		return nil
	}
	childDefCaus := schemaReplicant.RetrieveDeferredCauset(arg0)
	if childDefCaus == nil {
		childDefCaus = schemaReplicant.RetrieveDeferredCauset(arg1)
	}
	if isNullRejected(ctx, schemaReplicant, expr) && !allegrosql.HasNotNullFlag(childDefCaus.RetType.Flag) {
		return memex.BuildNotNullExpr(ctx, childDefCaus)
	}
	return nil
}

// Conds2BlockDual builds a LogicalBlockDual if cond is constant false or null.
func Conds2BlockDual(p LogicalCauset, conds []memex.Expression) LogicalCauset {
	if len(conds) != 1 {
		return nil
	}
	con, ok := conds[0].(*memex.Constant)
	if !ok {
		return nil
	}
	sc := p.SCtx().GetStochastikVars().StmtCtx
	if memex.ContainMublockConst(p.SCtx(), []memex.Expression{con}) {
		return nil
	}
	if isTrue, err := con.Value.ToBool(sc); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := LogicalBlockDual{}.Init(p.SCtx(), p.SelectBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
}

// outerJoinPropConst propagates constant equal and column equal conditions over outer join.
func (p *LogicalJoin) outerJoinPropConst(predicates []memex.Expression) []memex.Expression {
	outerBlock := p.children[0]
	innerBlock := p.children[1]
	if p.JoinType == RightOuterJoin {
		innerBlock, outerBlock = outerBlock, innerBlock
	}
	lenJoinConds := len(p.EqualConditions) + len(p.LeftConditions) + len(p.RightConditions) + len(p.OtherConditions)
	joinConds := make([]memex.Expression, 0, lenJoinConds)
	for _, equalCond := range p.EqualConditions {
		joinConds = append(joinConds, equalCond)
	}
	joinConds = append(joinConds, p.LeftConditions...)
	joinConds = append(joinConds, p.RightConditions...)
	joinConds = append(joinConds, p.OtherConditions...)
	p.EqualConditions = nil
	p.LeftConditions = nil
	p.RightConditions = nil
	p.OtherConditions = nil
	nullSensitive := p.JoinType == AntiLeftOuterSemiJoin || p.JoinType == LeftOuterSemiJoin
	joinConds, predicates = memex.PropConstOverOuterJoin(p.ctx, joinConds, predicates, outerBlock.Schema(), innerBlock.Schema(), nullSensitive)
	p.AttachOnConds(joinConds)
	return predicates
}

// GetPartitionByDefCauss extracts 'partition by' columns from the Window.
func (p *LogicalWindow) GetPartitionByDefCauss() []*memex.DeferredCauset {
	partitionDefCauss := make([]*memex.DeferredCauset, 0, len(p.PartitionBy))
	for _, partitionItem := range p.PartitionBy {
		partitionDefCauss = append(partitionDefCauss, partitionItem.DefCaus)
	}
	return partitionDefCauss
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalWindow) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	canBePushed := make([]memex.Expression, 0, len(predicates))
	canNotBePushed := make([]memex.Expression, 0, len(predicates))
	partitionDefCauss := memex.NewSchema(p.GetPartitionByDefCauss()...)
	for _, cond := range predicates {
		// We can push predicate beneath Window, only if all of the
		// extractedDefCauss are part of partitionBy columns.
		if memex.ExprFromSchema(cond, partitionDefCauss) {
			canBePushed = append(canBePushed, cond)
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	p.baseLogicalCauset.PredicatePushDown(canBePushed)
	return canNotBePushed, p
}

// PredicatePushDown implements LogicalCauset PredicatePushDown interface.
func (p *LogicalMemBlock) PredicatePushDown(predicates []memex.Expression) ([]memex.Expression, LogicalCauset) {
	if p.Extractor != nil {
		predicates = p.Extractor.Extract(p.ctx, p.schemaReplicant, p.names, predicates)
	}
	return predicates, p.self
}

func (*pFIDelSolver) name() string {
	return "predicate_push_down"
}
