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
	"context"

	"github.com/whtcorpsinc/milevadb/memex"
)

// canProjectionBeEliminatedLoose checks whether a projection can be eliminated,
// returns true if every memex is a single column.
func canProjectionBeEliminatedLoose(p *LogicalProjection) bool {
	for _, expr := range p.Exprs {
		_, ok := expr.(*memex.DeferredCauset)
		if !ok {
			return false
		}
	}
	return true
}

// canProjectionBeEliminatedStrict checks whether a projection can be
// eliminated, returns true if the projection just copy its child's output.
func canProjectionBeEliminatedStrict(p *PhysicalProjection) bool {
	// If this projection is specially added for `DO`, we keep it.
	if p.CalculateNoDelay {
		return false
	}
	if p.Schema().Len() == 0 {
		return true
	}
	child := p.Children()[0]
	if p.Schema().Len() != child.Schema().Len() {
		return false
	}
	for i, expr := range p.Exprs {
		col, ok := expr.(*memex.DeferredCauset)
		if !ok || !col.Equal(nil, child.Schema().DeferredCausets[i]) {
			return false
		}
	}
	return true
}

func resolveDeferredCausetAndReplace(origin *memex.DeferredCauset, replace map[string]*memex.DeferredCauset) {
	dst := replace[string(origin.HashCode(nil))]
	if dst != nil {
		retType, inOperand := origin.RetType, origin.InOperand
		*origin = *dst
		origin.RetType, origin.InOperand = retType, inOperand
	}
}

// ResolveExprAndReplace replaces columns fields of memexs by children logical plans.
func ResolveExprAndReplace(origin memex.Expression, replace map[string]*memex.DeferredCauset) {
	switch expr := origin.(type) {
	case *memex.DeferredCauset:
		resolveDeferredCausetAndReplace(expr, replace)
	case *memex.CorrelatedDeferredCauset:
		resolveDeferredCausetAndReplace(&expr.DeferredCauset, replace)
	case *memex.ScalarFunction:
		for _, arg := range expr.GetArgs() {
			ResolveExprAndReplace(arg, replace)
		}
	}
}

func doPhysicalProjectionElimination(p PhysicalCauset) PhysicalCauset {
	for i, child := range p.Children() {
		p.Children()[i] = doPhysicalProjectionElimination(child)
	}

	proj, isProj := p.(*PhysicalProjection)
	if !isProj || !canProjectionBeEliminatedStrict(proj) {
		return p
	}
	child := p.Children()[0]
	return child
}

// eliminatePhysicalProjection should be called after physical optimization to
// eliminate the redundant projection left after logical projection elimination.
func eliminatePhysicalProjection(p PhysicalCauset) PhysicalCauset {
	oldSchema := p.Schema()
	newRoot := doPhysicalProjectionElimination(p)
	newDefCauss := newRoot.Schema().DeferredCausets
	for i, oldDefCaus := range oldSchema.DeferredCausets {
		oldDefCaus.Index = newDefCauss[i].Index
		oldDefCaus.ID = newDefCauss[i].ID
		oldDefCaus.UniqueID = newDefCauss[i].UniqueID
		oldDefCaus.VirtualExpr = newDefCauss[i].VirtualExpr
		newRoot.Schema().DeferredCausets[i] = oldDefCaus
	}
	return newRoot
}

type projectionEliminator struct {
}

// optimize implements the logicalOptMemrule interface.
func (pe *projectionEliminator) optimize(ctx context.Context, lp LogicalCauset) (LogicalCauset, error) {
	root := pe.eliminate(lp, make(map[string]*memex.DeferredCauset), false)
	return root, nil
}

// eliminate eliminates the redundant projection in a logical plan.
func (pe *projectionEliminator) eliminate(p LogicalCauset, replace map[string]*memex.DeferredCauset, canEliminate bool) LogicalCauset {
	proj, isProj := p.(*LogicalProjection)
	childFlag := canEliminate
	if _, isUnion := p.(*LogicalUnionAll); isUnion {
		childFlag = false
	} else if _, isAgg := p.(*LogicalAggregation); isAgg || isProj {
		childFlag = true
	} else if _, isWindow := p.(*LogicalWindow); isWindow {
		childFlag = true
	}
	for i, child := range p.Children() {
		p.Children()[i] = pe.eliminate(child, replace, childFlag)
	}

	switch x := p.(type) {
	case *LogicalJoin:
		x.schemaReplicant = buildLogicalJoinSchema(x.JoinType, x)
	case *LogicalApply:
		x.schemaReplicant = buildLogicalJoinSchema(x.JoinType, x)
	default:
		for _, dst := range p.Schema().DeferredCausets {
			resolveDeferredCausetAndReplace(dst, replace)
		}
	}
	p.replaceExprDeferredCausets(replace)
	if isProj {
		if child, ok := p.Children()[0].(*LogicalProjection); ok && !ExprsHasSideEffects(child.Exprs) {
			for i := range proj.Exprs {
				proj.Exprs[i] = memex.FoldConstant(ReplaceDeferredCausetOfExpr(proj.Exprs[i], child, child.Schema()))
			}
			p.Children()[0] = child.Children()[0]
		}
	}

	if !(isProj && canEliminate && canProjectionBeEliminatedLoose(proj)) {
		return p
	}
	exprs := proj.Exprs
	for i, col := range proj.Schema().DeferredCausets {
		replace[string(col.HashCode(nil))] = exprs[i].(*memex.DeferredCauset)
	}
	return p.Children()[0]
}

// ReplaceDeferredCausetOfExpr replaces column of memex by another LogicalProjection.
func ReplaceDeferredCausetOfExpr(expr memex.Expression, proj *LogicalProjection, schemaReplicant *memex.Schema) memex.Expression {
	switch v := expr.(type) {
	case *memex.DeferredCauset:
		idx := schemaReplicant.DeferredCausetIndex(v)
		if idx != -1 && idx < len(proj.Exprs) {
			return proj.Exprs[idx]
		}
	case *memex.ScalarFunction:
		for i := range v.GetArgs() {
			v.GetArgs()[i] = ReplaceDeferredCausetOfExpr(v.GetArgs()[i], proj, schemaReplicant)
		}
	}
	return expr
}

func (p *LogicalJoin) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	for _, equalExpr := range p.EqualConditions {
		ResolveExprAndReplace(equalExpr, replace)
	}
	for _, leftExpr := range p.LeftConditions {
		ResolveExprAndReplace(leftExpr, replace)
	}
	for _, rightExpr := range p.RightConditions {
		ResolveExprAndReplace(rightExpr, replace)
	}
	for _, otherExpr := range p.OtherConditions {
		ResolveExprAndReplace(otherExpr, replace)
	}
}

func (p *LogicalProjection) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	for _, expr := range p.Exprs {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalAggregation) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	for _, agg := range la.AggFuncs {
		for _, aggExpr := range agg.Args {
			ResolveExprAndReplace(aggExpr, replace)
		}
	}
	for _, gbyItem := range la.GroupByItems {
		ResolveExprAndReplace(gbyItem, replace)
	}
	la.collectGroupByDeferredCausets()
}

func (p *LogicalSelection) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	for _, expr := range p.Conditions {
		ResolveExprAndReplace(expr, replace)
	}
}

func (la *LogicalApply) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	la.LogicalJoin.replaceExprDeferredCausets(replace)
	for _, coDefCaus := range la.CorDefCauss {
		dst := replace[string(coDefCaus.DeferredCauset.HashCode(nil))]
		if dst != nil {
			coDefCaus.DeferredCauset = *dst
		}
	}
}

func (ls *LogicalSort) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	for _, byItem := range ls.ByItems {
		ResolveExprAndReplace(byItem.Expr, replace)
	}
}

func (lt *LogicalTopN) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	for _, byItem := range lt.ByItems {
		ResolveExprAndReplace(byItem.Expr, replace)
	}
}

func (p *LogicalWindow) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
	for _, desc := range p.WindowFuncDescs {
		for _, arg := range desc.Args {
			ResolveExprAndReplace(arg, replace)
		}
	}
	for _, item := range p.PartitionBy {
		resolveDeferredCausetAndReplace(item.DefCaus, replace)
	}
	for _, item := range p.OrderBy {
		resolveDeferredCausetAndReplace(item.DefCaus, replace)
	}
}

func (*projectionEliminator) name() string {
	return "projection_eliminate"
}
