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
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// Init initializes LogicalAggregation.
func (la LogicalAggregation) Init(ctx stochastikctx.Context, offset int) *LogicalAggregation {
	la.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeAgg, &la, offset)
	return &la
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx stochastikctx.Context, offset int) *LogicalJoin {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeJoin, &p, offset)
	return &p
}

// Init initializes DataSource.
func (ds DataSource) Init(ctx stochastikctx.Context, offset int) *DataSource {
	ds.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeDataSource, &ds, offset)
	return &ds
}

// Init initializes EinsteinDBSingleGather.
func (sg EinsteinDBSingleGather) Init(ctx stochastikctx.Context, offset int) *EinsteinDBSingleGather {
	sg.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeEinsteinDBSingleGather, &sg, offset)
	return &sg
}

// Init initializes LogicalBlockScan.
func (ts LogicalBlockScan) Init(ctx stochastikctx.Context, offset int) *LogicalBlockScan {
	ts.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeBlockScan, &ts, offset)
	return &ts
}

// Init initializes LogicalIndexScan.
func (is LogicalIndexScan) Init(ctx stochastikctx.Context, offset int) *LogicalIndexScan {
	is.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeIdxScan, &is, offset)
	return &is
}

// Init initializes LogicalApply.
func (la LogicalApply) Init(ctx stochastikctx.Context, offset int) *LogicalApply {
	la.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeApply, &la, offset)
	return &la
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx stochastikctx.Context, offset int) *LogicalSelection {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeSel, &p, offset)
	return &p
}

// Init initializes PhysicalSelection.
func (p PhysicalSelection) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalSelection {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeSel, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionScan.
func (p LogicalUnionScan) Init(ctx stochastikctx.Context, offset int) *LogicalUnionScan {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeUnionScan, &p, offset)
	return &p
}

// Init initializes LogicalProjection.
func (p LogicalProjection) Init(ctx stochastikctx.Context, offset int) *LogicalProjection {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeProj, &p, offset)
	return &p
}

// Init initializes PhysicalProjection.
func (p PhysicalProjection) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalProjection {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeProj, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalUnionAll.
func (p LogicalUnionAll) Init(ctx stochastikctx.Context, offset int) *LogicalUnionAll {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeUnion, &p, offset)
	return &p
}

// Init initializes LogicalPartitionUnionAll.
func (p LogicalPartitionUnionAll) Init(ctx stochastikctx.Context, offset int) *LogicalPartitionUnionAll {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypePartitionUnion, &p, offset)
	return &p
}

// Init initializes PhysicalUnionAll.
func (p PhysicalUnionAll) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalUnionAll {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeUnion, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalSort.
func (ls LogicalSort) Init(ctx stochastikctx.Context, offset int) *LogicalSort {
	ls.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeSort, &ls, offset)
	return &ls
}

// Init initializes PhysicalSort.
func (p PhysicalSort) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalSort {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeSort, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes NominalSort.
func (p NominalSort) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *NominalSort {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeSort, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalTopN.
func (lt LogicalTopN) Init(ctx stochastikctx.Context, offset int) *LogicalTopN {
	lt.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeTopN, &lt, offset)
	return &lt
}

// Init initializes PhysicalTopN.
func (p PhysicalTopN) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalTopN {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeTopN, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx stochastikctx.Context, offset int) *LogicalLimit {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeLimit, &p, offset)
	return &p
}

// Init initializes PhysicalLimit.
func (p PhysicalLimit) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalLimit {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeLimit, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalBlockDual.
func (p LogicalBlockDual) Init(ctx stochastikctx.Context, offset int) *LogicalBlockDual {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeDual, &p, offset)
	return &p
}

// Init initializes PhysicalBlockDual.
func (p PhysicalBlockDual) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *PhysicalBlockDual {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeDual, &p, offset)
	p.stats = stats
	return &p
}

// Init initializes LogicalMaxOneRow.
func (p LogicalMaxOneRow) Init(ctx stochastikctx.Context, offset int) *LogicalMaxOneRow {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeMaxOneRow, &p, offset)
	return &p
}

// Init initializes PhysicalMaxOneRow.
func (p PhysicalMaxOneRow) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalMaxOneRow {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeMaxOneRow, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes LogicalWindow.
func (p LogicalWindow) Init(ctx stochastikctx.Context, offset int) *LogicalWindow {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeWindow, &p, offset)
	return &p
}

// Init initializes PhysicalWindow.
func (p PhysicalWindow) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalWindow {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeWindow, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalShuffle.
func (p PhysicalShuffle) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalShuffle {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeShuffle, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalShuffleDataSourceStub.
func (p PhysicalShuffleDataSourceStub) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalShuffleDataSourceStub {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeShuffleDataSourceStub, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes UFIDelate.
func (p UFIDelate) Init(ctx stochastikctx.Context) *UFIDelate {
	p.baseCauset = newBaseCauset(ctx, plancodec.TypeUFIDelate, 0)
	return &p
}

// Init initializes Delete.
func (p Delete) Init(ctx stochastikctx.Context) *Delete {
	p.baseCauset = newBaseCauset(ctx, plancodec.TypeDelete, 0)
	return &p
}

// Init initializes Insert.
func (p Insert) Init(ctx stochastikctx.Context) *Insert {
	p.baseCauset = newBaseCauset(ctx, plancodec.TypeInsert, 0)
	return &p
}

// Init initializes LogicalShow.
func (p LogicalShow) Init(ctx stochastikctx.Context) *LogicalShow {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeShow, &p, 0)
	return &p
}

// Init initializes LogicalShowDBSJobs.
func (p LogicalShowDBSJobs) Init(ctx stochastikctx.Context) *LogicalShowDBSJobs {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeShowDBSJobs, &p, 0)
	return &p
}

// Init initializes PhysicalShow.
func (p PhysicalShow) Init(ctx stochastikctx.Context) *PhysicalShow {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeShow, &p, 0)
	// Just use pseudo stats to avoid panic.
	p.stats = &property.StatsInfo{RowCount: 1}
	return &p
}

// Init initializes PhysicalShowDBSJobs.
func (p PhysicalShowDBSJobs) Init(ctx stochastikctx.Context) *PhysicalShowDBSJobs {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeShowDBSJobs, &p, 0)
	// Just use pseudo stats to avoid panic.
	p.stats = &property.StatsInfo{RowCount: 1}
	return &p
}

// Init initializes LogicalLock.
func (p LogicalLock) Init(ctx stochastikctx.Context) *LogicalLock {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeLock, &p, 0)
	return &p
}

// Init initializes PhysicalLock.
func (p PhysicalLock) Init(ctx stochastikctx.Context, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalLock {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeLock, &p, 0)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalBlockScan.
func (p PhysicalBlockScan) Init(ctx stochastikctx.Context, offset int) *PhysicalBlockScan {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeBlockScan, &p, offset)
	return &p
}

// Init initializes PhysicalIndexScan.
func (p PhysicalIndexScan) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexScan {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeIdxScan, &p, offset)
	return &p
}

// Init initializes LogicalMemBlock.
func (p LogicalMemBlock) Init(ctx stochastikctx.Context, offset int) *LogicalMemBlock {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, plancodec.TypeMemBlockScan, &p, offset)
	return &p
}

// Init initializes PhysicalMemBlock.
func (p PhysicalMemBlock) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *PhysicalMemBlock {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeMemBlockScan, &p, offset)
	p.stats = stats
	return &p
}

// Init initializes PhysicalHashJoin.
func (p PhysicalHashJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalHashJoin {
	tp := plancodec.TypeHashJoin
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, tp, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes BatchPointGetCauset.
func (p PhysicalBroadCastJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalBroadCastJoin {
	tp := plancodec.TypeBroadcastJoin
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, tp, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p

}

// Init initializes PhysicalMergeJoin.
func (p PhysicalMergeJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *PhysicalMergeJoin {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeMergeJoin, &p, offset)
	p.stats = stats
	return &p
}

// Init initializes basePhysicalAgg.
func (base basePhysicalAgg) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int) *basePhysicalAgg {
	base.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeHashAgg, &base, offset)
	base.stats = stats
	return &base
}

func (base basePhysicalAgg) initForHash(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalHashAgg {
	p := &PhysicalHashAgg{base}
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeHashAgg, p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

func (base basePhysicalAgg) initForStream(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalStreamAgg {
	p := &PhysicalStreamAgg{base}
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeStreamAgg, p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return p
}

// Init initializes PhysicalApply.
func (p PhysicalApply) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalApply {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeApply, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalUnionScan.
func (p PhysicalUnionScan) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalUnionScan {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeUnionScan, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalIndexLookUpReader.
func (p PhysicalIndexLookUpReader) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexLookUpReader {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeIndexLookUp, &p, offset)
	p.BlockCausets = flattenPushDownCauset(p.blockCauset)
	p.IndexCausets = flattenPushDownCauset(p.indexCauset)
	p.schemaReplicant = p.blockCauset.Schema()
	return &p
}

// Init initializes PhysicalIndexMergeReader.
func (p PhysicalIndexMergeReader) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexMergeReader {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeIndexMerge, &p, offset)
	if p.blockCauset != nil {
		p.stats = p.blockCauset.statsInfo()
	} else {
		var totalRowCount float64
		for _, partCauset := range p.partialCausets {
			totalRowCount += partCauset.StatsCount()
		}
		p.stats = p.partialCausets[0].statsInfo().ScaleByExpectCnt(totalRowCount)
		p.stats.StatsVersion = p.partialCausets[0].statsInfo().StatsVersion
	}
	p.PartialCausets = make([][]PhysicalCauset, 0, len(p.partialCausets))
	for _, partialCauset := range p.partialCausets {
		tempCausets := flattenPushDownCauset(partialCauset)
		p.PartialCausets = append(p.PartialCausets, tempCausets)
	}
	if p.blockCauset != nil {
		p.BlockCausets = flattenPushDownCauset(p.blockCauset)
		p.schemaReplicant = p.blockCauset.Schema()
	} else {
		switch p.PartialCausets[0][0].(type) {
		case *PhysicalBlockScan:
			p.schemaReplicant = p.PartialCausets[0][0].Schema()
		default:
			is := p.PartialCausets[0][0].(*PhysicalIndexScan)
			p.schemaReplicant = is.dataSourceSchema
		}
	}
	return &p
}

// Init initializes PhysicalBlockReader.
func (p PhysicalBlockReader) Init(ctx stochastikctx.Context, offset int) *PhysicalBlockReader {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeBlockReader, &p, offset)
	if p.blockCauset != nil {
		p.BlockCausets = flattenPushDownCauset(p.blockCauset)
		p.schemaReplicant = p.blockCauset.Schema()
	}
	return &p
}

// Init initializes PhysicalIndexReader.
func (p PhysicalIndexReader) Init(ctx stochastikctx.Context, offset int) *PhysicalIndexReader {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeIndexReader, &p, offset)
	p.SetSchema(nil)
	return &p
}

// Init initializes PhysicalIndexJoin.
func (p PhysicalIndexJoin) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalIndexJoin {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, plancodec.TypeIndexJoin, &p, offset)
	p.childrenReqProps = props
	p.stats = stats
	return &p
}

// Init initializes PhysicalIndexMergeJoin.
func (p PhysicalIndexMergeJoin) Init(ctx stochastikctx.Context) *PhysicalIndexMergeJoin {
	ctx.GetStochastikVars().CausetID++
	p.tp = plancodec.TypeIndexMergeJoin
	p.id = ctx.GetStochastikVars().CausetID
	p.ctx = ctx
	return &p
}

// Init initializes PhysicalIndexHashJoin.
func (p PhysicalIndexHashJoin) Init(ctx stochastikctx.Context) *PhysicalIndexHashJoin {
	ctx.GetStochastikVars().CausetID++
	p.tp = plancodec.TypeIndexHashJoin
	p.id = ctx.GetStochastikVars().CausetID
	p.ctx = ctx
	return &p
}

// Init initializes BatchPointGetCauset.
func (p BatchPointGetCauset) Init(ctx stochastikctx.Context, stats *property.StatsInfo, schemaReplicant *memex.Schema, names []*types.FieldName, offset int) *BatchPointGetCauset {
	p.baseCauset = newBaseCauset(ctx, plancodec.TypeBatchPointGet, offset)
	p.schemaReplicant = schemaReplicant
	p.names = names
	p.stats = stats
	p.DeferredCausets = ExpandVirtualDeferredCauset(p.DeferredCausets, p.schemaReplicant, p.TblInfo.DeferredCausets)
	return &p
}

// Init initializes PointGetCauset.
func (p PointGetCauset) Init(ctx stochastikctx.Context, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PointGetCauset {
	p.baseCauset = newBaseCauset(ctx, plancodec.TypePointGet, offset)
	p.stats = stats
	p.DeferredCausets = ExpandVirtualDeferredCauset(p.DeferredCausets, p.schemaReplicant, p.TblInfo.DeferredCausets)
	return &p
}

func flattenTreeCauset(plan PhysicalCauset, plans []PhysicalCauset) []PhysicalCauset {
	plans = append(plans, plan)
	for _, child := range plan.Children() {
		plans = flattenTreeCauset(child, plans)
	}
	return plans
}

// flattenPushDownCauset converts a plan tree to a list, whose head is the leaf node like causet scan.
func flattenPushDownCauset(p PhysicalCauset) []PhysicalCauset {
	plans := make([]PhysicalCauset, 0, 5)
	plans = flattenTreeCauset(p, plans)
	for i := 0; i < len(plans)/2; i++ {
		j := len(plans) - i - 1
		plans[i], plans[j] = plans[j], plans[i]
	}
	return plans
}
