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

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/types"
)

type columnPruner struct {
}

func (s *columnPruner) optimize(ctx context.Context, lp LogicalCauset) (LogicalCauset, error) {
	err := lp.PruneDeferredCausets(lp.Schema().DeferredCausets)
	return lp, err
}

// ExprsHasSideEffects checks if any of the memexs has side effects.
func ExprsHasSideEffects(exprs []memex.Expression) bool {
	for _, expr := range exprs {
		if exprHasSetVarOrSleep(expr) {
			return true
		}
	}
	return false
}

// exprHasSetVarOrSleep checks if the memex has SetVar function or Sleep function.
func exprHasSetVarOrSleep(expr memex.Expression) bool {
	scalaFunc, isScalaFunc := expr.(*memex.ScalarFunction)
	if !isScalaFunc {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar || scalaFunc.FuncName.L == ast.Sleep {
		return true
	}
	for _, arg := range scalaFunc.GetArgs() {
		if exprHasSetVarOrSleep(arg) {
			return true
		}
	}
	return false
}

// PruneDeferredCausets implements LogicalCauset interface.
// If any memex has SetVar function or Sleep function, we do not prune it.
func (p *LogicalProjection) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	child := p.children[0]
	used := memex.GetUsedList(parentUsedDefCauss, p.schemaReplicant)

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !exprHasSetVarOrSleep(p.Exprs[i]) {
			p.schemaReplicant.DeferredCausets = append(p.schemaReplicant.DeferredCausets[:i], p.schemaReplicant.DeferredCausets[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	selfUsedDefCauss := make([]*memex.DeferredCauset, 0, len(p.Exprs))
	selfUsedDefCauss = memex.ExtractDeferredCausetsFromExpressions(selfUsedDefCauss, p.Exprs, nil)
	return child.PruneDeferredCausets(selfUsedDefCauss)
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *LogicalSelection) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	child := p.children[0]
	parentUsedDefCauss = memex.ExtractDeferredCausetsFromExpressions(parentUsedDefCauss, p.Conditions, nil)
	return child.PruneDeferredCausets(parentUsedDefCauss)
}

// PruneDeferredCausets implements LogicalCauset interface.
func (la *LogicalAggregation) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	child := la.children[0]
	used := memex.GetUsedList(parentUsedDefCauss, la.Schema())

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			la.schemaReplicant.DeferredCausets = append(la.schemaReplicant.DeferredCausets[:i], la.schemaReplicant.DeferredCausets[i+1:]...)
			la.AggFuncs = append(la.AggFuncs[:i], la.AggFuncs[i+1:]...)
		}
	}
	var selfUsedDefCauss []*memex.DeferredCauset
	for _, aggrFunc := range la.AggFuncs {
		selfUsedDefCauss = memex.ExtractDeferredCausetsFromExpressions(selfUsedDefCauss, aggrFunc.Args, nil)

		var defcaus []*memex.DeferredCauset
		aggrFunc.OrderByItems, defcaus = pruneByItems(aggrFunc.OrderByItems)
		selfUsedDefCauss = append(selfUsedDefCauss, defcaus...)
	}
	if len(la.AggFuncs) == 0 {
		// If all the aggregate functions are pruned, we should add an aggregate function to keep the correctness.
		one, err := aggregation.NewAggFuncDesc(la.ctx, ast.AggFuncFirstRow, []memex.Expression{memex.NewOne()}, false)
		if err != nil {
			return err
		}
		la.AggFuncs = []*aggregation.AggFuncDesc{one}
		col := &memex.DeferredCauset{
			UniqueID: la.ctx.GetStochastikVars().AllocCausetDeferredCausetID(),
			RetType:  types.NewFieldType(allegrosql.TypeLonglong),
		}
		la.schemaReplicant.DeferredCausets = []*memex.DeferredCauset{col}
	}

	if len(la.GroupByItems) > 0 {
		for i := len(la.GroupByItems) - 1; i >= 0; i-- {
			defcaus := memex.ExtractDeferredCausets(la.GroupByItems[i])
			if len(defcaus) == 0 {
				la.GroupByItems = append(la.GroupByItems[:i], la.GroupByItems[i+1:]...)
			} else {
				selfUsedDefCauss = append(selfUsedDefCauss, defcaus...)
			}
		}
		// If all the group by items are pruned, we should add a constant 1 to keep the correctness.
		// Because `select count(*) from t` is different from `select count(*) from t group by 1`.
		if len(la.GroupByItems) == 0 {
			la.GroupByItems = []memex.Expression{memex.NewOne()}
		}
	}
	return child.PruneDeferredCausets(selfUsedDefCauss)
}

func pruneByItems(old []*soliton.ByItems) (new []*soliton.ByItems, parentUsedDefCauss []*memex.DeferredCauset) {
	new = make([]*soliton.ByItems, 0, len(old))
	for _, byItem := range old {
		defcaus := memex.ExtractDeferredCausets(byItem.Expr)
		if len(defcaus) == 0 {
			if !memex.IsRuntimeConstExpr(byItem.Expr) {
				new = append(new, byItem)
			}
		} else if byItem.Expr.GetType().Tp == allegrosql.TypeNull {
			// do nothing, should be filtered
		} else {
			parentUsedDefCauss = append(parentUsedDefCauss, defcaus...)
			new = append(new, byItem)
		}
	}
	return
}

// PruneDeferredCausets implements LogicalCauset interface.
// If any memex can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the memexs contain non-deterministic functions, such as rand().
func (ls *LogicalSort) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	child := ls.children[0]
	var defcaus []*memex.DeferredCauset
	ls.ByItems, defcaus = pruneByItems(ls.ByItems)
	parentUsedDefCauss = append(parentUsedDefCauss, defcaus...)
	return child.PruneDeferredCausets(parentUsedDefCauss)
}

// PruneDeferredCausets implements LogicalCauset interface.
// If any memex can view as a constant in execution stage, such as correlated column, constant,
// we do prune them. Note that we can't prune the memexs contain non-deterministic functions, such as rand().
func (lt *LogicalTopN) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	child := lt.children[0]
	var defcaus []*memex.DeferredCauset
	lt.ByItems, defcaus = pruneByItems(lt.ByItems)
	parentUsedDefCauss = append(parentUsedDefCauss, defcaus...)
	return child.PruneDeferredCausets(parentUsedDefCauss)
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *LogicalUnionAll) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	used := memex.GetUsedList(parentUsedDefCauss, p.schemaReplicant)
	hasBeenUsed := false
	for i := range used {
		hasBeenUsed = hasBeenUsed || used[i]
		if hasBeenUsed {
			break
		}
	}
	if !hasBeenUsed {
		parentUsedDefCauss = make([]*memex.DeferredCauset, len(p.schemaReplicant.DeferredCausets))
		copy(parentUsedDefCauss, p.schemaReplicant.DeferredCausets)
	}
	for _, child := range p.Children() {
		err := child.PruneDeferredCausets(parentUsedDefCauss)
		if err != nil {
			return err
		}
	}

	if hasBeenUsed {
		// keep the schemaReplicant of LogicalUnionAll same as its children's
		used := memex.GetUsedList(p.children[0].Schema().DeferredCausets, p.schemaReplicant)
		for i := len(used) - 1; i >= 0; i-- {
			if !used[i] {
				p.schemaReplicant.DeferredCausets = append(p.schemaReplicant.DeferredCausets[:i], p.schemaReplicant.DeferredCausets[i+1:]...)
			}
		}
	}
	return nil
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *LogicalUnionScan) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	for i := 0; i < p.handleDefCauss.NumDefCauss(); i++ {
		parentUsedDefCauss = append(parentUsedDefCauss, p.handleDefCauss.GetDefCaus(i))
	}
	return p.children[0].PruneDeferredCausets(parentUsedDefCauss)
}

// PruneDeferredCausets implements LogicalCauset interface.
func (ds *DataSource) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	used := memex.GetUsedList(parentUsedDefCauss, ds.schemaReplicant)

	exprDefCauss := memex.ExtractDeferredCausetsFromExpressions(nil, ds.allConds, nil)
	exprUsed := memex.GetUsedList(exprDefCauss, ds.schemaReplicant)

	originSchemaDeferredCausets := ds.schemaReplicant.DeferredCausets
	originDeferredCausets := ds.DeferredCausets
	for i := len(used) - 1; i >= 0; i-- {
		if ds.blockInfo.IsCommonHandle && allegrosql.HasPriKeyFlag(ds.schemaReplicant.DeferredCausets[i].RetType.Flag) {
			// Do not prune common handle column.
			continue
		}
		if !used[i] && !exprUsed[i] {
			ds.schemaReplicant.DeferredCausets = append(ds.schemaReplicant.DeferredCausets[:i], ds.schemaReplicant.DeferredCausets[i+1:]...)
			ds.DeferredCausets = append(ds.DeferredCausets[:i], ds.DeferredCausets[i+1:]...)
		}
	}
	// For ALLEGROALLEGROSQL like `select 1 from t`, einsteindb's response will be empty if no column is in schemaReplicant.
	// So we'll force to push one if schemaReplicant doesn't have any column.
	if ds.schemaReplicant.Len() == 0 {
		var handleDefCaus *memex.DeferredCauset
		var handleDefCausInfo *perceptron.DeferredCausetInfo
		if ds.causet.Type().IsClusterBlock() && len(originDeferredCausets) > 0 {
			// use the first line.
			handleDefCaus = originSchemaDeferredCausets[0]
			handleDefCausInfo = originDeferredCausets[0]
		} else {
			if ds.handleDefCauss != nil {
				handleDefCaus = ds.handleDefCauss.GetDefCaus(0)
			} else {
				handleDefCaus = ds.newExtraHandleSchemaDefCaus()
			}
			handleDefCausInfo = perceptron.NewExtraHandleDefCausInfo()
		}
		ds.DeferredCausets = append(ds.DeferredCausets, handleDefCausInfo)
		ds.schemaReplicant.Append(handleDefCaus)
	}
	if ds.handleDefCauss != nil && ds.handleDefCauss.IsInt() && ds.schemaReplicant.DeferredCausetIndex(ds.handleDefCauss.GetDefCaus(0)) == -1 {
		ds.handleDefCauss = nil
	}
	return nil
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *LogicalBlockDual) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	used := memex.GetUsedList(parentUsedDefCauss, p.Schema())

	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			p.schemaReplicant.DeferredCausets = append(p.schemaReplicant.DeferredCausets[:i], p.schemaReplicant.DeferredCausets[i+1:]...)
		}
	}
	return nil
}

func (p *LogicalJoin) extractUsedDefCauss(parentUsedDefCauss []*memex.DeferredCauset) (leftDefCauss []*memex.DeferredCauset, rightDefCauss []*memex.DeferredCauset) {
	for _, eqCond := range p.EqualConditions {
		parentUsedDefCauss = append(parentUsedDefCauss, memex.ExtractDeferredCausets(eqCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedDefCauss = append(parentUsedDefCauss, memex.ExtractDeferredCausets(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedDefCauss = append(parentUsedDefCauss, memex.ExtractDeferredCausets(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedDefCauss = append(parentUsedDefCauss, memex.ExtractDeferredCausets(otherCond)...)
	}
	lChild := p.children[0]
	rChild := p.children[1]
	for _, col := range parentUsedDefCauss {
		if lChild.Schema().Contains(col) {
			leftDefCauss = append(leftDefCauss, col)
		} else if rChild.Schema().Contains(col) {
			rightDefCauss = append(rightDefCauss, col)
		}
	}
	return leftDefCauss, rightDefCauss
}

func (p *LogicalJoin) mergeSchema() {
	lChild := p.children[0]
	rChild := p.children[1]
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin {
		p.schemaReplicant = lChild.Schema().Clone()
	} else if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		joinDefCaus := p.schemaReplicant.DeferredCausets[len(p.schemaReplicant.DeferredCausets)-1]
		p.schemaReplicant = lChild.Schema().Clone()
		p.schemaReplicant.Append(joinDefCaus)
	} else {
		p.schemaReplicant = memex.MergeSchema(lChild.Schema(), rChild.Schema())
	}
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *LogicalJoin) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	leftDefCauss, rightDefCauss := p.extractUsedDefCauss(parentUsedDefCauss)

	err := p.children[0].PruneDeferredCausets(leftDefCauss)
	if err != nil {
		return err
	}

	err = p.children[1].PruneDeferredCausets(rightDefCauss)
	if err != nil {
		return err
	}

	p.mergeSchema()
	if p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		joinDefCaus := p.schemaReplicant.DeferredCausets[len(p.schemaReplicant.DeferredCausets)-1]
		parentUsedDefCauss = append(parentUsedDefCauss, joinDefCaus)
	}
	p.inlineProjection(parentUsedDefCauss)
	return nil
}

// PruneDeferredCausets implements LogicalCauset interface.
func (la *LogicalApply) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	leftDefCauss, rightDefCauss := la.extractUsedDefCauss(parentUsedDefCauss)

	err := la.children[1].PruneDeferredCausets(rightDefCauss)
	if err != nil {
		return err
	}

	la.CorDefCauss = extractCorDeferredCausetsBySchema4LogicalCauset(la.children[1], la.children[0].Schema())
	for _, col := range la.CorDefCauss {
		leftDefCauss = append(leftDefCauss, &col.DeferredCauset)
	}

	err = la.children[0].PruneDeferredCausets(leftDefCauss)
	if err != nil {
		return err
	}

	la.mergeSchema()
	return nil
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *LogicalLock) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	if !IsSelectForUFIDelateLockType(p.Lock.LockType) {
		return p.baseLogicalCauset.PruneDeferredCausets(parentUsedDefCauss)
	}

	if len(p.partitionedBlock) > 0 {
		// If the children include partitioned blocks, do not prune columns.
		// Because the interlock needs the partitioned columns to calculate the dagger key.
		return p.children[0].PruneDeferredCausets(p.Schema().DeferredCausets)
	}

	for _, defcaus := range p.tblID2Handle {
		for _, col := range defcaus {
			for i := 0; i < col.NumDefCauss(); i++ {
				parentUsedDefCauss = append(parentUsedDefCauss, col.GetDefCaus(i))
			}
		}
	}
	return p.children[0].PruneDeferredCausets(parentUsedDefCauss)
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *LogicalWindow) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	windowDeferredCausets := p.GetWindowResultDeferredCausets()
	len := 0
	for _, col := range parentUsedDefCauss {
		used := false
		for _, windowDeferredCauset := range windowDeferredCausets {
			if windowDeferredCauset.Equal(nil, col) {
				used = true
				break
			}
		}
		if !used {
			parentUsedDefCauss[len] = col
			len++
		}
	}
	parentUsedDefCauss = parentUsedDefCauss[:len]
	parentUsedDefCauss = p.extractUsedDefCauss(parentUsedDefCauss)
	err := p.children[0].PruneDeferredCausets(parentUsedDefCauss)
	if err != nil {
		return err
	}

	p.SetSchema(p.children[0].Schema().Clone())
	p.Schema().Append(windowDeferredCausets...)
	return nil
}

func (p *LogicalWindow) extractUsedDefCauss(parentUsedDefCauss []*memex.DeferredCauset) []*memex.DeferredCauset {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			parentUsedDefCauss = append(parentUsedDefCauss, memex.ExtractDeferredCausets(arg)...)
		}
	}
	for _, by := range p.PartitionBy {
		parentUsedDefCauss = append(parentUsedDefCauss, by.DefCaus)
	}
	for _, by := range p.OrderBy {
		parentUsedDefCauss = append(parentUsedDefCauss, by.DefCaus)
	}
	return parentUsedDefCauss
}

func (*columnPruner) name() string {
	return "column_prune"
}
