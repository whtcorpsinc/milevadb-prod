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
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/disjointset"
)

// ResolveIndices implements Causet interface.
func (p *PhysicalProjection) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Exprs {
		p.Exprs[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	childProj, isProj := p.children[0].(*PhysicalProjection)
	if !isProj {
		return
	}
	refine4NeighbourProj(p, childProj)
	return
}

// refine4NeighbourProj refines the index for p.Exprs whose type is *DeferredCauset when
// there is two neighbouring Projections.
// This function is introduced because that different childProj.Expr may refer
// to the same index of childProj.Schema, so we need to keep this relation
// between the specified memexs in the parent Projection.
func refine4NeighbourProj(p, childProj *PhysicalProjection) {
	inputIdx2OutputIdxes := make(map[int][]int)
	for i, expr := range childProj.Exprs {
		col, isDefCaus := expr.(*memex.DeferredCauset)
		if !isDefCaus {
			continue
		}
		inputIdx2OutputIdxes[col.Index] = append(inputIdx2OutputIdxes[col.Index], i)
	}
	childSchemaUnionSet := disjointset.NewIntSet(childProj.schemaReplicant.Len())
	for _, outputIdxes := range inputIdx2OutputIdxes {
		if len(outputIdxes) <= 1 {
			continue
		}
		for i := 1; i < len(outputIdxes); i++ {
			childSchemaUnionSet.Union(outputIdxes[0], outputIdxes[i])
		}
	}

	for _, expr := range p.Exprs {
		col, isDefCaus := expr.(*memex.DeferredCauset)
		if !isDefCaus {
			continue
		}
		col.Index = childSchemaUnionSet.FindRoot(col.Index)
	}
}

// ResolveIndices implements Causet interface.
func (p *PhysicalHashJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, fun := range p.EqualConditions {
		lArg, err := fun.GetArgs()[0].ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = lArg.(*memex.DeferredCauset)
		rArg, err := fun.GetArgs()[1].ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = rArg.(*memex.DeferredCauset)
		p.EqualConditions[i] = memex.NewFunctionInternal(fun.GetCtx(), fun.FuncName.L, fun.GetType(), lArg, rArg).(*memex.ScalarFunction)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(memex.MergeSchema(lSchema, rSchema))
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalBroadCastJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftJoinKeys {
		newKey, err := col.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = newKey.(*memex.DeferredCauset)
	}
	for i, col := range p.RightJoinKeys {
		newKey, err := col.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = newKey.(*memex.DeferredCauset)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(memex.MergeSchema(lSchema, rSchema))
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalMergeJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i, col := range p.LeftJoinKeys {
		newKey, err := col.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
		p.LeftJoinKeys[i] = newKey.(*memex.DeferredCauset)
	}
	for i, col := range p.RightJoinKeys {
		newKey, err := col.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
		p.RightJoinKeys[i] = newKey.(*memex.DeferredCauset)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(memex.MergeSchema(lSchema, rSchema))
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalIndexJoin) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	lSchema := p.children[0].Schema()
	rSchema := p.children[1].Schema()
	for i := range p.InnerJoinKeys {
		newOuterKey, err := p.OuterJoinKeys[i].ResolveIndices(p.children[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.OuterJoinKeys[i] = newOuterKey.(*memex.DeferredCauset)
		newInnerKey, err := p.InnerJoinKeys[i].ResolveIndices(p.children[p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		p.InnerJoinKeys[i] = newInnerKey.(*memex.DeferredCauset)
	}
	for i, expr := range p.LeftConditions {
		p.LeftConditions[i], err = expr.ResolveIndices(lSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.RightConditions {
		p.RightConditions[i], err = expr.ResolveIndices(rSchema)
		if err != nil {
			return err
		}
	}
	mergedSchema := memex.MergeSchema(lSchema, rSchema)
	for i, expr := range p.OtherConditions {
		p.OtherConditions[i], err = expr.ResolveIndices(mergedSchema)
		if err != nil {
			return err
		}
	}
	if p.CompareFilters != nil {
		err = p.CompareFilters.resolveIndices(p.children[1-p.InnerChildIdx].Schema())
		if err != nil {
			return err
		}
		for i := range p.CompareFilters.affectedDefCausSchema.DeferredCausets {
			resolvedDefCaus, err1 := p.CompareFilters.affectedDefCausSchema.DeferredCausets[i].ResolveIndices(p.children[1-p.InnerChildIdx].Schema())
			if err1 != nil {
				return err1
			}
			p.CompareFilters.affectedDefCausSchema.DeferredCausets[i] = resolvedDefCaus.(*memex.DeferredCauset)
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalUnionScan) ResolveIndices() (err error) {
	err = p.basePhysicalCauset.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	resolvedHandleDefCaus, err := p.HandleDefCauss.ResolveIndices(p.children[0].Schema())
	if err != nil {
		return err
	}
	p.HandleDefCauss = resolvedHandleDefCaus
	return
}

// resolveIndicesForVirtualDeferredCauset resolves dependent columns's indices for virtual columns.
func resolveIndicesForVirtualDeferredCauset(result []*memex.DeferredCauset, schemaReplicant *memex.Schema) error {
	for _, col := range result {
		if col.VirtualExpr != nil {
			newExpr, err := col.VirtualExpr.ResolveIndices(schemaReplicant)
			if err != nil {
				return err
			}
			col.VirtualExpr = newExpr
		}
	}
	return nil
}

// ResolveIndices implements Causet interface.
func (p *PhysicalBlockReader) ResolveIndices() error {
	err := resolveIndicesForVirtualDeferredCauset(p.schemaReplicant.DeferredCausets, p.schemaReplicant)
	if err != nil {
		return err
	}
	return p.blockCauset.ResolveIndices()
}

// ResolveIndices implements Causet interface.
func (p *PhysicalIndexReader) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	err = p.indexCauset.ResolveIndices()
	if err != nil {
		return err
	}
	for i, col := range p.OutputDeferredCausets {
		newDefCaus, err := col.ResolveIndices(p.indexCauset.Schema())
		if err != nil {
			return err
		}
		p.OutputDeferredCausets[i] = newDefCaus.(*memex.DeferredCauset)
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() (err error) {
	err = resolveIndicesForVirtualDeferredCauset(p.blockCauset.Schema().DeferredCausets, p.schemaReplicant)
	if err != nil {
		return err
	}
	err = p.blockCauset.ResolveIndices()
	if err != nil {
		return err
	}
	err = p.indexCauset.ResolveIndices()
	if err != nil {
		return err
	}
	if p.ExtraHandleDefCaus != nil {
		newDefCaus, err := p.ExtraHandleDefCaus.ResolveIndices(p.blockCauset.Schema())
		if err != nil {
			return err
		}
		p.ExtraHandleDefCaus = newDefCaus.(*memex.DeferredCauset)
	}
	for i, commonHandleDefCaus := range p.CommonHandleDefCauss {
		newDefCaus, err := commonHandleDefCaus.ResolveIndices(p.BlockCausets[0].Schema())
		if err != nil {
			return err
		}
		p.CommonHandleDefCauss[i] = newDefCaus.(*memex.DeferredCauset)
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalIndexMergeReader) ResolveIndices() (err error) {
	err = resolveIndicesForVirtualDeferredCauset(p.blockCauset.Schema().DeferredCausets, p.schemaReplicant)
	if err != nil {
		return err
	}
	if p.blockCauset != nil {
		err = p.blockCauset.ResolveIndices()
		if err != nil {
			return err
		}
	}
	for i := 0; i < len(p.partialCausets); i++ {
		err = p.partialCausets[i].ResolveIndices()
		if err != nil {
			return err
		}
	}
	return nil
}

// ResolveIndices implements Causet interface.
func (p *PhysicalSelection) ResolveIndices() (err error) {
	err = p.basePhysicalCauset.ResolveIndices()
	if err != nil {
		return err
	}
	for i, expr := range p.Conditions {
		p.Conditions[i], err = expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *basePhysicalAgg) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, aggFun := range p.AggFuncs {
		for i, arg := range aggFun.Args {
			aggFun.Args[i], err = arg.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
		for _, byItem := range aggFun.OrderByItems {
			byItem.Expr, err = byItem.Expr.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	for i, item := range p.GroupByItems {
		p.GroupByItems[i], err = item.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalSort) ResolveIndices() (err error) {
	err = p.basePhysicalCauset.ResolveIndices()
	if err != nil {
		return err
	}
	for _, item := range p.ByItems {
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return err
}

// ResolveIndices implements Causet interface.
func (p *PhysicalWindow) ResolveIndices() (err error) {
	err = p.physicalSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for i := 0; i < len(p.Schema().DeferredCausets)-len(p.WindowFuncDescs); i++ {
		col := p.Schema().DeferredCausets[i]
		newDefCaus, err := col.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.Schema().DeferredCausets[i] = newDefCaus.(*memex.DeferredCauset)
	}
	for i, item := range p.PartitionBy {
		newDefCaus, err := item.DefCaus.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.PartitionBy[i].DefCaus = newDefCaus.(*memex.DeferredCauset)
	}
	for i, item := range p.OrderBy {
		newDefCaus, err := item.DefCaus.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		p.OrderBy[i].DefCaus = newDefCaus.(*memex.DeferredCauset)
	}
	for _, desc := range p.WindowFuncDescs {
		for i, arg := range desc.Args {
			desc.Args[i], err = arg.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	if p.Frame != nil {
		for i := range p.Frame.Start.CalcFuncs {
			p.Frame.Start.CalcFuncs[i], err = p.Frame.Start.CalcFuncs[i].ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
		for i := range p.Frame.End.CalcFuncs {
			p.Frame.End.CalcFuncs[i], err = p.Frame.End.CalcFuncs[i].ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ResolveIndices implements Causet interface.
func (p *PhysicalShuffle) ResolveIndices() (err error) {
	err = p.basePhysicalCauset.ResolveIndices()
	if err != nil {
		return err
	}
	for i := range p.HashByItems {
		// "Shuffle" get value of items from `DataSource`, other than children[0].
		p.HashByItems[i], err = p.HashByItems[i].ResolveIndices(p.DataSource.Schema())
		if err != nil {
			return err
		}
	}
	return err
}

// ResolveIndices implements Causet interface.
func (p *PhysicalTopN) ResolveIndices() (err error) {
	err = p.basePhysicalCauset.ResolveIndices()
	if err != nil {
		return err
	}
	for _, item := range p.ByItems {
		item.Expr, err = item.Expr.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalApply) ResolveIndices() (err error) {
	err = p.PhysicalHashJoin.ResolveIndices()
	if err != nil {
		return err
	}
	// p.OuterSchema may have duplicated CorrelatedDeferredCausets,
	// we deduplicate it here.
	dedupDefCauss := make(map[int64]*memex.CorrelatedDeferredCauset, len(p.OuterSchema))
	for _, col := range p.OuterSchema {
		dedupDefCauss[col.UniqueID] = col
	}
	p.OuterSchema = make([]*memex.CorrelatedDeferredCauset, 0, len(dedupDefCauss))
	for _, col := range dedupDefCauss {
		newDefCaus, err := col.DeferredCauset.ResolveIndices(p.children[0].Schema())
		if err != nil {
			return err
		}
		col.DeferredCauset = *newDefCaus.(*memex.DeferredCauset)
		p.OuterSchema = append(p.OuterSchema, col)
	}
	// Resolve index for equal conditions again, because apply is different from
	// hash join on the fact that equal conditions are evaluated against the join result,
	// so columns from equal conditions come from merged schemaReplicant of children, instead of
	// single child's schemaReplicant.
	joinedSchema := memex.MergeSchema(p.children[0].Schema(), p.children[1].Schema())
	for i, cond := range p.PhysicalHashJoin.EqualConditions {
		newSf, err := cond.ResolveIndices(joinedSchema)
		if err != nil {
			return err
		}
		p.PhysicalHashJoin.EqualConditions[i] = newSf.(*memex.ScalarFunction)
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *UFIDelate) ResolveIndices() (err error) {
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	schemaReplicant := p.SelectCauset.Schema()
	for _, assign := range p.OrderedList {
		newDefCaus, err := assign.DefCaus.ResolveIndices(schemaReplicant)
		if err != nil {
			return err
		}
		assign.DefCaus = newDefCaus.(*memex.DeferredCauset)
		assign.Expr, err = assign.Expr.ResolveIndices(schemaReplicant)
		if err != nil {
			return err
		}
	}
	return
}

// ResolveIndices implements Causet interface.
func (p *PhysicalLock) ResolveIndices() (err error) {
	err = p.basePhysicalCauset.ResolveIndices()
	if err != nil {
		return err
	}
	for i, defcaus := range p.TblID2Handle {
		for j, col := range defcaus {
			resolvedDefCaus, err := col.ResolveIndices(p.children[0].Schema())
			if err != nil {
				return err
			}
			p.TblID2Handle[i][j] = resolvedDefCaus
		}
	}
	return nil
}

// ResolveIndices implements Causet interface.
func (p *Insert) ResolveIndices() (err error) {
	err = p.baseSchemaProducer.ResolveIndices()
	if err != nil {
		return err
	}
	for _, asgn := range p.OnDuplicate {
		newDefCaus, err := asgn.DefCaus.ResolveIndices(p.blockSchema)
		if err != nil {
			return err
		}
		asgn.DefCaus = newDefCaus.(*memex.DeferredCauset)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
		if err != nil {
			return err
		}
	}
	for _, set := range p.SetList {
		newDefCaus, err := set.DefCaus.ResolveIndices(p.blockSchema)
		if err != nil {
			return err
		}
		set.DefCaus = newDefCaus.(*memex.DeferredCauset)
		set.Expr, err = set.Expr.ResolveIndices(p.blockSchema)
		if err != nil {
			return err
		}
	}
	for i, expr := range p.GenDefCauss.Exprs {
		p.GenDefCauss.Exprs[i], err = expr.ResolveIndices(p.blockSchema)
		if err != nil {
			return err
		}
	}
	for _, asgn := range p.GenDefCauss.OnDuplicates {
		newDefCaus, err := asgn.DefCaus.ResolveIndices(p.blockSchema)
		if err != nil {
			return err
		}
		asgn.DefCaus = newDefCaus.(*memex.DeferredCauset)
		asgn.Expr, err = asgn.Expr.ResolveIndices(p.Schema4OnDuplicate)
		if err != nil {
			return err
		}
	}
	return
}

func (p *physicalSchemaProducer) ResolveIndices() (err error) {
	err = p.basePhysicalCauset.ResolveIndices()
	return err
}

func (p *baseSchemaProducer) ResolveIndices() (err error) {
	return
}

// ResolveIndices implements Causet interface.
func (p *basePhysicalCauset) ResolveIndices() (err error) {
	for _, child := range p.children {
		err = child.ResolveIndices()
		if err != nil {
			return err
		}
	}
	return
}
