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
	"math"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

var (
	_ LogicalCauset = &LogicalJoin{}
	_ LogicalCauset = &LogicalAggregation{}
	_ LogicalCauset = &LogicalProjection{}
	_ LogicalCauset = &LogicalSelection{}
	_ LogicalCauset = &LogicalApply{}
	_ LogicalCauset = &LogicalMaxOneRow{}
	_ LogicalCauset = &LogicalBlockDual{}
	_ LogicalCauset = &DataSource{}
	_ LogicalCauset = &EinsteinDBSingleGather{}
	_ LogicalCauset = &LogicalBlockScan{}
	_ LogicalCauset = &LogicalIndexScan{}
	_ LogicalCauset = &LogicalUnionAll{}
	_ LogicalCauset = &LogicalSort{}
	_ LogicalCauset = &LogicalLock{}
	_ LogicalCauset = &LogicalLimit{}
	_ LogicalCauset = &LogicalWindow{}
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin.
type JoinType int

const (
	// InnerJoin means inner join.
	InnerJoin JoinType = iota
	// LeftOuterJoin means left join.
	LeftOuterJoin
	// RightOuterJoin means right join.
	RightOuterJoin
	// SemiJoin means if event a in causet A matches some rows in B, just output a.
	SemiJoin
	// AntiSemiJoin means if event a in causet A does not match any event in B, then output a.
	AntiSemiJoin
	// LeftOuterSemiJoin means if event a in causet A matches some rows in B, output (a, true), otherwise, output (a, false).
	LeftOuterSemiJoin
	// AntiLeftOuterSemiJoin means if event a in causet A matches some rows in B, output (a, false), otherwise, output (a, true).
	AntiLeftOuterSemiJoin
)

// IsOuterJoin returns if this joiner is a outer joiner
func (tp JoinType) IsOuterJoin() bool {
	return tp == LeftOuterJoin || tp == RightOuterJoin ||
		tp == LeftOuterSemiJoin || tp == AntiLeftOuterSemiJoin
}

func (tp JoinType) String() string {
	switch tp {
	case InnerJoin:
		return "inner join"
	case LeftOuterJoin:
		return "left outer join"
	case RightOuterJoin:
		return "right outer join"
	case SemiJoin:
		return "semi join"
	case AntiSemiJoin:
		return "anti semi join"
	case LeftOuterSemiJoin:
		return "left outer semi join"
	case AntiLeftOuterSemiJoin:
		return "anti left outer semi join"
	}
	return "unsupported join type"
}

const (
	preferLeftAsINLJInner uint = 1 << iota
	preferRightAsINLJInner
	preferLeftAsINLHJInner
	preferRightAsINLHJInner
	preferLeftAsINLMJInner
	preferRightAsINLMJInner
	preferHashJoin
	preferMergeJoin
	preferBCJoin
	preferHashAgg
	preferStreamAgg
)

const (
	preferEinsteinDB = 1 << iota
	preferTiFlash
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	logicalSchemaProducer

	JoinType      JoinType
	reordered     bool
	cartesianJoin bool
	StraightJoin  bool

	// hintInfo stores the join algorithm hint information specified by client.
	hintInfo       *blockHintInfo
	preferJoinType uint

	EqualConditions []*memex.ScalarFunction
	LeftConditions  memex.CNFExprs
	RightConditions memex.CNFExprs
	OtherConditions memex.CNFExprs

	leftProperties  [][]*memex.DeferredCauset
	rightProperties [][]*memex.DeferredCauset

	// DefaultValues is only used for left/right outer join, which is values the inner event's should be when the outer causet
	// doesn't match any inner causet's event.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Causet

	// redundantSchema contains columns which are eliminated in join.
	// For select * from a join b using (c); a.c will in output schemaReplicant, and b.c will in redundantSchema.
	redundantSchema *memex.Schema
	redundantNames  types.NameSlice

	// equalCondOutCnt indicates the estimated count of joined rows after evaluating `EqualConditions`.
	equalCondOutCnt float64
}

// Shallow shallow copies a LogicalJoin struct.
func (p *LogicalJoin) Shallow() *LogicalJoin {
	join := *p
	return join.Init(p.ctx, p.blockOffset)
}

// GetJoinKeys extracts join keys(columns) from EqualConditions. It returns left join keys, right
// join keys and an `isNullEQ` array which means the `joinKey[i]` is a `NullEQ` function. The `hasNullEQ`
// means whether there is a `NullEQ` of a join key.
func (p *LogicalJoin) GetJoinKeys() (leftKeys, rightKeys []*memex.DeferredCauset, isNullEQ []bool, hasNullEQ bool) {
	for _, expr := range p.EqualConditions {
		leftKeys = append(leftKeys, expr.GetArgs()[0].(*memex.DeferredCauset))
		rightKeys = append(rightKeys, expr.GetArgs()[1].(*memex.DeferredCauset))
		isNullEQ = append(isNullEQ, expr.FuncName.L == ast.NullEQ)
		hasNullEQ = hasNullEQ || expr.FuncName.L == ast.NullEQ
	}
	return
}

func (p *LogicalJoin) columnSubstitute(schemaReplicant *memex.Schema, exprs []memex.Expression) {
	for i, cond := range p.LeftConditions {
		p.LeftConditions[i] = memex.DeferredCausetSubstitute(cond, schemaReplicant, exprs)
	}

	for i, cond := range p.RightConditions {
		p.RightConditions[i] = memex.DeferredCausetSubstitute(cond, schemaReplicant, exprs)
	}

	for i, cond := range p.OtherConditions {
		p.OtherConditions[i] = memex.DeferredCausetSubstitute(cond, schemaReplicant, exprs)
	}

	for i := len(p.EqualConditions) - 1; i >= 0; i-- {
		newCond := memex.DeferredCausetSubstitute(p.EqualConditions[i], schemaReplicant, exprs).(*memex.ScalarFunction)

		// If the columns used in the new filter all come from the left child,
		// we can push this filter to it.
		if memex.ExprFromSchema(newCond, p.children[0].Schema()) {
			p.LeftConditions = append(p.LeftConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		if memex.ExprFromSchema(newCond, p.children[1].Schema()) {
			p.RightConditions = append(p.RightConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		_, lhsIsDefCaus := newCond.GetArgs()[0].(*memex.DeferredCauset)
		_, rhsIsDefCaus := newCond.GetArgs()[1].(*memex.DeferredCauset)

		// If the columns used in the new filter are not all memex.DeferredCauset,
		// we can not use it as join's equal condition.
		if !(lhsIsDefCaus && rhsIsDefCaus) {
			p.OtherConditions = append(p.OtherConditions, newCond)
			p.EqualConditions = append(p.EqualConditions[:i], p.EqualConditions[i+1:]...)
			continue
		}

		p.EqualConditions[i] = newCond
	}
}

// AttachOnConds extracts on conditions for join and set the `EqualConditions`, `LeftConditions`, `RightConditions` and
// `OtherConditions` by the result of extract.
func (p *LogicalJoin) AttachOnConds(onConds []memex.Expression) {
	eq, left, right, other := p.extractOnCondition(onConds, false, false)
	p.AppendJoinConds(eq, left, right, other)
}

// AppendJoinConds appends new join conditions.
func (p *LogicalJoin) AppendJoinConds(eq []*memex.ScalarFunction, left, right, other []memex.Expression) {
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (p *LogicalJoin) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.RightConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	return corDefCauss
}

// ExtractJoinKeys extract join keys as a schemaReplicant for child with childIdx.
func (p *LogicalJoin) ExtractJoinKeys(childIdx int) *memex.Schema {
	joinKeys := make([]*memex.DeferredCauset, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[childIdx].(*memex.DeferredCauset))
	}
	return memex.NewSchema(joinKeys...)
}

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	logicalSchemaProducer

	Exprs []memex.Expression

	// calculateGenDefCauss indicates the projection is for calculating generated columns.
	// In *UFIDelATE*, we should know this to tell different projections.
	calculateGenDefCauss bool

	// CalculateNoDelay indicates this Projection is the root Causet and should be
	// calculated without delay and will not return any result to client.
	// Currently it is "true" only when the current allegrosql query is a "DO" memex.
	// See "https://dev.allegrosql.com/doc/refman/5.7/en/do.html" for more detail.
	CalculateNoDelay bool

	// AvoidDeferredCausetEvaluator is a temporary variable which is ONLY used to avoid
	// building columnEvaluator for the memexs of Projection which is
	// built by buildProjection4Union.
	// This can be removed after column pool being supported.
	// Related issue: MilevaDB#8141(https://github.com/whtcorpsinc/milevadb/issues/8141)
	AvoidDeferredCausetEvaluator bool
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (p *LogicalProjection) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// GetUsedDefCauss extracts all of the DeferredCausets used by proj.
func (p *LogicalProjection) GetUsedDefCauss() (usedDefCauss []*memex.DeferredCauset) {
	for _, expr := range p.Exprs {
		usedDefCauss = append(usedDefCauss, memex.ExtractDeferredCausets(expr)...)
	}
	return usedDefCauss
}

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	logicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []memex.Expression
	// groupByDefCauss stores the columns that are group-by items.
	groupByDefCauss []*memex.DeferredCauset

	// aggHints stores aggregation hint information.
	aggHints aggHintInfo

	possibleProperties [][]*memex.DeferredCauset
	inputCount         float64 // inputCount is the input count of this plan.
}

// HasDistinct shows whether LogicalAggregation has functions with distinct.
func (la *LogicalAggregation) HasDistinct() bool {
	for _, aggFunc := range la.AggFuncs {
		if aggFunc.HasDistinct {
			return true
		}
	}
	return false
}

// CopyAggHints copies the aggHints from another LogicalAggregation.
func (la *LogicalAggregation) CopyAggHints(agg *LogicalAggregation) {
	// TODO: Copy the hint may make the un-applicable hint throw the
	// same warning message more than once. We'd better add a flag for
	// `HaveThrownWarningMessage` to avoid this. Besides, finalAgg and
	// partialAgg (in cascades causet) should share the same hint, instead
	// of a copy.
	la.aggHints = agg.aggHints
}

// IsPartialModeAgg returns if all of the AggFuncs are partialMode.
func (la *LogicalAggregation) IsPartialModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.Partial1Mode
}

// IsCompleteModeAgg returns if all of the AggFuncs are CompleteMode.
func (la *LogicalAggregation) IsCompleteModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.CompleteMode
}

// GetGroupByDefCauss returns the groupByDefCauss. If the groupByDefCauss haven't be collected,
// this method would collect them at first. If the GroupByItems have been changed,
// we should explicitly collect GroupByDeferredCausets before this method.
func (la *LogicalAggregation) GetGroupByDefCauss() []*memex.DeferredCauset {
	if la.groupByDefCauss == nil {
		la.collectGroupByDeferredCausets()
	}
	return la.groupByDefCauss
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (la *LogicalAggregation) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(la.GroupByItems)+len(la.AggFuncs))
	for _, expr := range la.GroupByItems {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	for _, fun := range la.AggFuncs {
		for _, arg := range fun.Args {
			corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(arg)...)
		}
	}
	return corDefCauss
}

// GetUsedDefCauss extracts all of the DeferredCausets used by agg including GroupByItems and AggFuncs.
func (la *LogicalAggregation) GetUsedDefCauss() (usedDefCauss []*memex.DeferredCauset) {
	for _, groupByItem := range la.GroupByItems {
		usedDefCauss = append(usedDefCauss, memex.ExtractDeferredCausets(groupByItem)...)
	}
	for _, aggDesc := range la.AggFuncs {
		for _, expr := range aggDesc.Args {
			usedDefCauss = append(usedDefCauss, memex.ExtractDeferredCausets(expr)...)
		}
	}
	return usedDefCauss
}

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	baseLogicalCauset

	// Originally the WHERE or ON condition is parsed into a single memex,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []memex.Expression
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (p *LogicalSelection) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.Conditions))
	for _, cond := range p.Conditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(cond)...)
	}
	return corDefCauss
}

// LogicalApply gets one event from outer interlock and gets one event from inner interlock according to outer event.
type LogicalApply struct {
	LogicalJoin

	CorDefCauss []*memex.CorrelatedDeferredCauset
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (la *LogicalApply) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := la.LogicalJoin.ExtractCorrelatedDefCauss()
	for i := len(corDefCauss) - 1; i >= 0; i-- {
		if la.children[0].Schema().Contains(&corDefCauss[i].DeferredCauset) {
			corDefCauss = append(corDefCauss[:i], corDefCauss[i+1:]...)
		}
	}
	return corDefCauss
}

// LogicalMaxOneRow checks if a query returns no more than one event.
type LogicalMaxOneRow struct {
	baseLogicalCauset
}

// LogicalBlockDual represents a dual causet plan.
type LogicalBlockDual struct {
	logicalSchemaProducer

	RowCount int
}

// LogicalMemBlock represents a memory causet or virtual causet
// Some memory blocks wants to take the tenantship of some predications
// e.g
// SELECT * FROM cluster_log WHERE type='einsteindb' AND address='192.16.5.32'
// Assume that the causet `cluster_log` is a memory causet, which is used
// to retrieve logs from remote components. In the above situation we should
// send log search request to the target EinsteinDB (192.16.5.32) directly instead of
// requesting all cluster components log search gRPC interface to retrieve
// log message and filtering them in MilevaDB node.
type LogicalMemBlock struct {
	logicalSchemaProducer

	Extractor MemBlockPredicateExtractor
	DBName    perceptron.CIStr
	BlockInfo *perceptron.BlockInfo
	// QueryTimeRange is used to specify the time range for metrics summary blocks and inspection blocks
	// e.g: select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from metrics_summary_by_label;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_summary;
	//      select /*+ time_range('2020-02-02 12:10:00', '2020-02-02 13:00:00') */ from inspection_result;
	QueryTimeRange QueryTimeRange
}

// LogicalUnionScan is only used in non read-only txn.
type LogicalUnionScan struct {
	baseLogicalCauset

	conditions []memex.Expression

	handleDefCauss HandleDefCauss
}

// DataSource represents a blockScan without condition push down.
type DataSource struct {
	logicalSchemaProducer

	astIndexHints   []*ast.IndexHint
	IndexHints      []indexHintInfo
	causet          causet.Block
	blockInfo       *perceptron.BlockInfo
	DeferredCausets []*perceptron.DeferredCausetInfo
	DBName          perceptron.CIStr

	BlockAsName *perceptron.CIStr
	// indexMergeHints are the hint for indexmerge.
	indexMergeHints []indexHintInfo
	// pushedDownConds are the conditions that will be pushed down to interlock.
	pushedDownConds []memex.Expression
	// allConds contains all the filters on this causet. For now it's maintained
	// in predicate push down and used only in partition pruning.
	allConds []memex.Expression

	statisticBlock *statistics.Block
	blockStats     *property.StatsInfo

	// possibleAccessPaths stores all the possible access path for physical plan, including causet scan.
	possibleAccessPaths []*soliton.AccessPath

	// The data source may be a partition, rather than a real causet.
	isPartition     bool
	physicalBlockID int64
	partitionNames  []perceptron.CIStr

	// handleDefCaus represents the handle column for the datasource, either the
	// int primary key column or extra handle column.
	//handleDefCaus *memex.DeferredCauset
	handleDefCauss HandleDefCauss
	// TblDefCauss contains the original columns of causet before being pruned, and it
	// is used for estimating causet scan cost.
	TblDefCauss []*memex.DeferredCauset
	// commonHandleDefCauss and commonHandleLens save the info of primary key which is the clustered index.
	commonHandleDefCauss []*memex.DeferredCauset
	commonHandleLens     []int
	// TblDefCausHists contains the Histogram of all original causet columns,
	// it is converted from statisticBlock, and used for IO/network cost estimating.
	TblDefCausHists *statistics.HistDefCausl
	// preferStoreType means the DataSource is enforced to which storage.
	preferStoreType int
	// preferPartitions causetstore the map, the key represents causetstore type, the value represents the partition name list.
	preferPartitions map[int][]perceptron.CIStr
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (ds *DataSource) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(ds.pushedDownConds))
	for _, expr := range ds.pushedDownConds {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// EinsteinDBSingleGather is a leaf logical operator of MilevaDB layer to gather
// tuples from EinsteinDB regions.
type EinsteinDBSingleGather struct {
	logicalSchemaProducer
	Source *DataSource
	// IsIndexGather marks if this EinsteinDBSingleGather gathers tuples from an IndexScan.
	// in implementation phase, we need this flag to determine whether to generate
	// PhysicalBlockReader or PhysicalIndexReader.
	IsIndexGather bool
	Index         *perceptron.IndexInfo
}

// LogicalBlockScan is the logical causet scan operator for EinsteinDB.
type LogicalBlockScan struct {
	logicalSchemaProducer
	Source         *DataSource
	HandleDefCauss HandleDefCauss
	AccessConds    memex.CNFExprs
	Ranges         []*ranger.Range
}

// LogicalIndexScan is the logical index scan operator for EinsteinDB.
type LogicalIndexScan struct {
	logicalSchemaProducer
	// DataSource should be read-only here.
	Source       *DataSource
	IsDoubleRead bool

	EqCondCount int
	AccessConds memex.CNFExprs
	Ranges      []*ranger.Range

	Index              *perceptron.IndexInfo
	DeferredCausets    []*perceptron.DeferredCausetInfo
	FullIdxDefCauss    []*memex.DeferredCauset
	FullIdxDefCausLens []int
	IdxDefCauss        []*memex.DeferredCauset
	IdxDefCausLens     []int
}

// MatchIndexProp checks if the indexScan can match the required property.
func (p *LogicalIndexScan) MatchIndexProp(prop *property.PhysicalProperty) (match bool) {
	if prop.IsEmpty() {
		return true
	}
	if all, _ := prop.AllSameOrder(); !all {
		return false
	}
	for i, col := range p.IdxDefCauss {
		if col.Equal(nil, prop.Items[0].DefCaus) {
			return matchIndicesProp(p.IdxDefCauss[i:], p.IdxDefCausLens[i:], prop.Items)
		} else if i >= p.EqCondCount {
			break
		}
	}
	return false
}

// getBlockPath finds the BlockPath from a group of accessPaths.
func getBlockPath(paths []*soliton.AccessPath) *soliton.AccessPath {
	for _, path := range paths {
		if path.IsBlockPath() {
			return path
		}
	}
	return nil
}

func (ds *DataSource) buildBlockGather() LogicalCauset {
	ts := LogicalBlockScan{Source: ds, HandleDefCauss: ds.handleDefCauss}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.Schema())
	sg := EinsteinDBSingleGather{Source: ds, IsIndexGather: false}.Init(ds.ctx, ds.blockOffset)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(ts)
	return sg
}

func (ds *DataSource) buildIndexGather(path *soliton.AccessPath) LogicalCauset {
	is := LogicalIndexScan{
		Source:             ds,
		IsDoubleRead:       false,
		Index:              path.Index,
		FullIdxDefCauss:    path.FullIdxDefCauss,
		FullIdxDefCausLens: path.FullIdxDefCausLens,
		IdxDefCauss:        path.IdxDefCauss,
		IdxDefCausLens:     path.IdxDefCausLens,
	}.Init(ds.ctx, ds.blockOffset)

	is.DeferredCausets = make([]*perceptron.DeferredCausetInfo, len(ds.DeferredCausets))
	copy(is.DeferredCausets, ds.DeferredCausets)
	is.SetSchema(ds.Schema())
	is.IdxDefCauss, is.IdxDefCausLens = memex.IndexInfo2PrefixDefCauss(is.DeferredCausets, is.schemaReplicant.DeferredCausets, is.Index)

	sg := EinsteinDBSingleGather{
		Source:        ds,
		IsIndexGather: true,
		Index:         path.Index,
	}.Init(ds.ctx, ds.blockOffset)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(is)
	return sg
}

// Convert2Gathers builds logical EinsteinDBSingleGathers from DataSource.
func (ds *DataSource) Convert2Gathers() (gathers []LogicalCauset) {
	tg := ds.buildBlockGather()
	gathers = append(gathers, tg)
	for _, path := range ds.possibleAccessPaths {
		if !path.IsIntHandlePath {
			path.FullIdxDefCauss, path.FullIdxDefCausLens = memex.IndexInfo2DefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, path.Index)
			path.IdxDefCauss, path.IdxDefCausLens = memex.IndexInfo2PrefixDefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, path.Index)
			// If index columns can cover all of the needed columns, we can use a IndexGather + IndexScan.
			if ds.isCoveringIndex(ds.schemaReplicant.DeferredCausets, path.FullIdxDefCauss, path.FullIdxDefCausLens, ds.blockInfo) {
				gathers = append(gathers, ds.buildIndexGather(path))
			}
			// TODO: If index columns can not cover the schemaReplicant, use IndexLookUpGather.
		}
	}
	return gathers
}

func (ds *DataSource) deriveCommonHandleBlockPathStats(path *soliton.AccessPath, conds []memex.Expression, isIm bool) (bool, error) {
	path.CountAfterAccess = float64(ds.statisticBlock.Count)
	path.Ranges = ranger.FullNotNullRange()
	path.IdxDefCauss, path.IdxDefCausLens = memex.IndexInfo2PrefixDefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, path.Index)
	path.FullIdxDefCauss, path.FullIdxDefCausLens = memex.IndexInfo2DefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, path.Index)
	if len(conds) == 0 {
		return false, nil
	}
	sc := ds.ctx.GetStochastikVars().StmtCtx
	if len(path.IdxDefCauss) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxDefCauss, path.IdxDefCausLens)
		if err != nil {
			return false, err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.BlockFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.CountAfterAccess, err = ds.blockStats.HistDefCausl.GetRowCountByIndexRanges(sc, path.Index.ID, path.Ranges)
		if err != nil {
			return false, err
		}
	} else {
		path.BlockFilters = conds
	}
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorDefCausAccessCondFromFilters(ds.ctx, path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.BlockFilters = remained
		if len(accesses) > 0 && ds.statisticBlock.Pseudo {
			path.CountAfterAccess = ds.statisticBlock.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticBlock.Count)
			for i := range accesses {
				col := path.IdxDefCauss[path.EqOrInCondCount+i]
				ndv := ds.getDeferredCausetNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticBlock.Count))
	}
	// Check whether there's only point query.
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.Index.DeferredCausets) {
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.Index.DeferredCausets); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRanges && !haveNullVal, nil
}

// deriveBlockPathStats will fulfill the information that the AccessPath need.
// And it will check whether the primary key is covered only by point query.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveBlockPathStats(path *soliton.AccessPath, conds []memex.Expression, isIm bool) (bool, error) {
	if path.IsCommonHandlePath {
		return ds.deriveCommonHandleBlockPathStats(path, conds, isIm)
	}
	var err error
	sc := ds.ctx.GetStochastikVars().StmtCtx
	path.CountAfterAccess = float64(ds.statisticBlock.Count)
	path.BlockFilters = conds
	var pkDefCaus *memex.DeferredCauset
	columnLen := len(ds.schemaReplicant.DeferredCausets)
	isUnsigned := false
	if ds.blockInfo.PKIsHandle {
		if pkDefCausInfo := ds.blockInfo.GetPkDefCausInfo(); pkDefCausInfo != nil {
			isUnsigned = allegrosql.HasUnsignedFlag(pkDefCausInfo.Flag)
			pkDefCaus = memex.DefCausInfo2DefCaus(ds.schemaReplicant.DeferredCausets, pkDefCausInfo)
		}
	} else if columnLen > 0 && ds.schemaReplicant.DeferredCausets[columnLen-1].ID == perceptron.ExtraHandleID {
		pkDefCaus = ds.schemaReplicant.DeferredCausets[columnLen-1]
	}
	if pkDefCaus == nil {
		path.Ranges = ranger.FullIntRange(isUnsigned)
		return false, nil
	}

	path.Ranges = ranger.FullIntRange(isUnsigned)
	if len(conds) == 0 {
		return false, nil
	}
	path.AccessConds, path.BlockFilters = ranger.DetachCondsForDeferredCauset(ds.ctx, conds, pkDefCaus)
	// If there's no access cond, we try to find that whether there's memex containing correlated column that
	// can be used to access data.
	corDefCausInAccessConds := false
	if len(path.AccessConds) == 0 {
		for i, filter := range path.BlockFilters {
			eqFunc, ok := filter.(*memex.ScalarFunction)
			if !ok || eqFunc.FuncName.L != ast.EQ {
				continue
			}
			lDefCaus, lOk := eqFunc.GetArgs()[0].(*memex.DeferredCauset)
			if lOk && lDefCaus.Equal(ds.ctx, pkDefCaus) {
				_, rOk := eqFunc.GetArgs()[1].(*memex.CorrelatedDeferredCauset)
				if rOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.BlockFilters = append(path.BlockFilters[:i], path.BlockFilters[i+1:]...)
					corDefCausInAccessConds = true
					break
				}
			}
			rDefCaus, rOk := eqFunc.GetArgs()[1].(*memex.DeferredCauset)
			if rOk && rDefCaus.Equal(ds.ctx, pkDefCaus) {
				_, lOk := eqFunc.GetArgs()[0].(*memex.CorrelatedDeferredCauset)
				if lOk {
					path.AccessConds = append(path.AccessConds, filter)
					path.BlockFilters = append(path.BlockFilters[:i], path.BlockFilters[i+1:]...)
					corDefCausInAccessConds = true
					break
				}
			}
		}
	}
	if corDefCausInAccessConds {
		path.CountAfterAccess = 1
		return true, nil
	}
	path.Ranges, err = ranger.BuildBlockRange(path.AccessConds, sc, pkDefCaus.RetType)
	if err != nil {
		return false, err
	}
	path.CountAfterAccess, err = ds.statisticBlock.GetRowCountByIntDeferredCausetRanges(sc, pkDefCaus.ID, path.Ranges)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticBlock.Count))
	}
	// Check whether the primary key is covered by point query.
	noIntervalRange := true
	for _, ran := range path.Ranges {
		if !ran.IsPoint(sc) {
			noIntervalRange = false
			break
		}
	}
	return noIntervalRange, err
}

func (ds *DataSource) fillIndexPath(path *soliton.AccessPath, conds []memex.Expression) error {
	sc := ds.ctx.GetStochastikVars().StmtCtx
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.statisticBlock.Count)
	path.IdxDefCauss, path.IdxDefCausLens = memex.IndexInfo2PrefixDefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, path.Index)
	path.FullIdxDefCauss, path.FullIdxDefCausLens = memex.IndexInfo2DefCauss(ds.DeferredCausets, ds.schemaReplicant.DeferredCausets, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.DeferredCausets) == len(path.IdxDefCauss) {
		handleDefCaus := ds.getPKIsHandleDefCaus()
		if handleDefCaus != nil && !allegrosql.HasUnsignedFlag(handleDefCaus.RetType.Flag) {
			alreadyHandle := false
			for _, col := range path.IdxDefCauss {
				if col.ID == perceptron.ExtraHandleID || col.Equal(nil, handleDefCaus) {
					alreadyHandle = true
				}
			}
			// Don't add one column twice to the index. May cause unexpected errors.
			if !alreadyHandle {
				path.IdxDefCauss = append(path.IdxDefCauss, handleDefCaus)
				path.IdxDefCausLens = append(path.IdxDefCausLens, types.UnspecifiedLength)
			}
		}
	}
	if len(path.IdxDefCauss) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxDefCauss, path.IdxDefCausLens)
		if err != nil {
			return err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.BlockFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.CountAfterAccess, err = ds.blockStats.HistDefCausl.GetRowCountByIndexRanges(sc, path.Index.ID, path.Ranges)
		if err != nil {
			return err
		}
	} else {
		path.BlockFilters = conds
	}
	return nil
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// And it will check whether this index is full matched by point query. We will use this check to
// determine whether we remove other paths or not.
// conds is the conditions used to generate the DetachRangeResult for path.
// isIm indicates whether this function is called to generate the partial path for IndexMerge.
func (ds *DataSource) deriveIndexPathStats(path *soliton.AccessPath, conds []memex.Expression, isIm bool) bool {
	sc := ds.ctx.GetStochastikVars().StmtCtx
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitCorDefCausAccessCondFromFilters(ds.ctx, path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.BlockFilters = remained
		if len(accesses) > 0 && ds.statisticBlock.Pseudo {
			path.CountAfterAccess = ds.statisticBlock.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticBlock.Count)
			for i := range accesses {
				col := path.IdxDefCauss[path.EqOrInCondCount+i]
				ndv := ds.getDeferredCausetNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	var indexFilters []memex.Expression
	indexFilters, path.BlockFilters = ds.splitIndexFilterConditions(path.BlockFilters, path.FullIdxDefCauss, path.FullIdxDefCausLens, ds.blockInfo)
	path.IndexFilters = append(path.IndexFilters, indexFilters...)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount && !isIm {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/SelectionFactor, float64(ds.statisticBlock.Count))
	}
	if path.IndexFilters != nil {
		selectivity, _, err := ds.blockStats.HistDefCausl.Selectivity(ds.ctx, path.IndexFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		if isIm {
			path.CountAfterIndex = path.CountAfterAccess * selectivity
		} else {
			path.CountAfterIndex = math.Max(path.CountAfterAccess*selectivity, ds.stats.RowCount)
		}
	}
	// Check whether there's only point query.
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.Index.DeferredCausets) {
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.Index.DeferredCausets); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRanges && !haveNullVal
}

func getPKIsHandleDefCausFromSchema(defcaus []*perceptron.DeferredCausetInfo, schemaReplicant *memex.Schema, pkIsHandle bool) *memex.DeferredCauset {
	if !pkIsHandle {
		// If the PKIsHandle is false, return the ExtraHandleDeferredCauset.
		for i, col := range defcaus {
			if col.ID == perceptron.ExtraHandleID {
				return schemaReplicant.DeferredCausets[i]
			}
		}
		return nil
	}
	for i, col := range defcaus {
		if allegrosql.HasPriKeyFlag(col.Flag) {
			return schemaReplicant.DeferredCausets[i]
		}
	}
	return nil
}

func (ds *DataSource) getPKIsHandleDefCaus() *memex.DeferredCauset {
	return getPKIsHandleDefCausFromSchema(ds.DeferredCausets, ds.schemaReplicant, ds.blockInfo.PKIsHandle)
}

func (p *LogicalIndexScan) getPKIsHandleDefCaus(schemaReplicant *memex.Schema) *memex.DeferredCauset {
	// We cannot use p.Source.getPKIsHandleDefCaus() here,
	// Because we may re-prune p.DeferredCausets and p.schemaReplicant during the transformation.
	// That will make p.DeferredCausets different from p.Source.DeferredCausets.
	return getPKIsHandleDefCausFromSchema(p.DeferredCausets, schemaReplicant, p.Source.blockInfo.PKIsHandle)
}

// BlockInfo returns the *BlockInfo of data source.
func (ds *DataSource) BlockInfo() *perceptron.BlockInfo {
	return ds.blockInfo
}

// LogicalUnionAll represents LogicalUnionAll plan.
type LogicalUnionAll struct {
	logicalSchemaProducer
}

// LogicalPartitionUnionAll represents the LogicalUnionAll plan is for partition causet.
type LogicalPartitionUnionAll struct {
	LogicalUnionAll
}

// LogicalSort stands for the order by plan.
type LogicalSort struct {
	baseLogicalCauset

	ByItems []*soliton.ByItems
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (ls *LogicalSort) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(ls.ByItems))
	for _, item := range ls.ByItems {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(item.Expr)...)
	}
	return corDefCauss
}

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	baseLogicalCauset

	ByItems    []*soliton.ByItems
	Offset     uint64
	Count      uint64
	limitHints limitHintInfo
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (lt *LogicalTopN) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(lt.ByItems))
	for _, item := range lt.ByItems {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(item.Expr)...)
	}
	return corDefCauss
}

// isLimit checks if TopN is a limit plan.
func (lt *LogicalTopN) isLimit() bool {
	return len(lt.ByItems) == 0
}

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	baseLogicalCauset

	Offset     uint64
	Count      uint64
	limitHints limitHintInfo
}

// LogicalLock represents a select dagger plan.
type LogicalLock struct {
	baseLogicalCauset

	Lock             *ast.SelectLockInfo
	tblID2Handle     map[int64][]HandleDefCauss
	partitionedBlock []causet.PartitionedBlock
}

// WindowFrame represents a window function frame.
type WindowFrame struct {
	Type  ast.FrameType
	Start *FrameBound
	End   *FrameBound
}

// FrameBound is the boundary of a frame.
type FrameBound struct {
	Type      ast.BoundType
	UnBounded bool
	Num       uint64
	// CalcFuncs is used for range framed windows.
	// We will build the date_add or date_sub functions for frames like `INTERVAL '2:30' MINUTE_SECOND FOLLOWING`,
	// and plus or minus for frames like `1 preceding`.
	CalcFuncs []memex.Expression
	// CmpFuncs is used to decide whether one event is included in the current frame.
	CmpFuncs []memex.CompareFunc
}

// LogicalWindow represents a logical window function plan.
type LogicalWindow struct {
	logicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.Item
	OrderBy         []property.Item
	Frame           *WindowFrame
}

// ExtractCorrelatedDefCauss implements LogicalCauset interface.
func (p *LogicalWindow) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
			}
		}
	}
	return corDefCauss
}

// GetWindowResultDeferredCausets returns the columns storing the result of the window function.
func (p *LogicalWindow) GetWindowResultDeferredCausets() []*memex.DeferredCauset {
	return p.schemaReplicant.DeferredCausets[p.schemaReplicant.Len()-len(p.WindowFuncDescs):]
}

// ExtractCorDeferredCausetsBySchema only extracts the correlated columns that match the specified schemaReplicant.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schemaReplicant is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func ExtractCorDeferredCausetsBySchema(corDefCauss []*memex.CorrelatedDeferredCauset, schemaReplicant *memex.Schema, resolveIndex bool) []*memex.CorrelatedDeferredCauset {
	resultCorDefCauss := make([]*memex.CorrelatedDeferredCauset, schemaReplicant.Len())
	for _, corDefCaus := range corDefCauss {
		idx := schemaReplicant.DeferredCausetIndex(&corDefCaus.DeferredCauset)
		if idx != -1 {
			if resultCorDefCauss[idx] == nil {
				resultCorDefCauss[idx] = &memex.CorrelatedDeferredCauset{
					DeferredCauset: *schemaReplicant.DeferredCausets[idx],
					Data:           new(types.Causet),
				}
			}
			corDefCaus.Data = resultCorDefCauss[idx].Data
		}
	}
	// Shrink slice. e.g. [col1, nil, col2, nil] will be changed to [col1, col2].
	length := 0
	for _, col := range resultCorDefCauss {
		if col != nil {
			resultCorDefCauss[length] = col
			length++
		}
	}
	resultCorDefCauss = resultCorDefCauss[:length]

	if resolveIndex {
		for _, corDefCaus := range resultCorDefCauss {
			corDefCaus.Index = schemaReplicant.DeferredCausetIndex(&corDefCaus.DeferredCauset)
		}
	}

	return resultCorDefCauss
}

// extractCorDeferredCausetsBySchema4LogicalCauset only extracts the correlated columns that match the specified schemaReplicant.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schemaReplicant is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func extractCorDeferredCausetsBySchema4LogicalCauset(p LogicalCauset, schemaReplicant *memex.Schema) []*memex.CorrelatedDeferredCauset {
	corDefCauss := ExtractCorrelatedDefCauss4LogicalCauset(p)
	return ExtractCorDeferredCausetsBySchema(corDefCauss, schemaReplicant, false)
}

// ExtractCorDeferredCausetsBySchema4PhysicalCauset only extracts the correlated columns that match the specified schemaReplicant.
// e.g. If the correlated columns from plan are [t1.a, t2.a, t3.a] and specified schemaReplicant is [t2.a, t2.b, t2.c],
// only [t2.a] is returned.
func ExtractCorDeferredCausetsBySchema4PhysicalCauset(p PhysicalCauset, schemaReplicant *memex.Schema) []*memex.CorrelatedDeferredCauset {
	corDefCauss := ExtractCorrelatedDefCauss4PhysicalCauset(p)
	return ExtractCorDeferredCausetsBySchema(corDefCauss, schemaReplicant, true)
}

// ShowContents stores the contents for the `SHOW` memex.
type ShowContents struct {
	Tp             ast.ShowStmtType // Databases/Blocks/DeferredCausets/....
	DBName         string
	Block          *ast.BlockName          // Used for showing columns.
	DeferredCauset *ast.DeferredCausetName // Used for `desc causet column`.
	IndexName      perceptron.CIStr
	Flag           int                  // Some flag parsed from allegrosql, such as FULL.
	User           *auth.UserIdentity   // Used for show grants.
	Roles          []*auth.RoleIdentity // Used for show grants.

	Full        bool
	IfNotExists bool // Used for `show create database if not exists`.
	GlobalScope bool // Used by show variables.
	Extended    bool // Used for `show extended columns from ...`
}

// LogicalShow represents a show plan.
type LogicalShow struct {
	logicalSchemaProducer
	ShowContents
}

// LogicalShowDBSJobs is for showing DBS job list.
type LogicalShowDBSJobs struct {
	logicalSchemaProducer

	JobNumber int64
}
