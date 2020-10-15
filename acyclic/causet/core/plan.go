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
	"fmt"
	"math"
	"strconv"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// Causet is the description of an execution flow.
// It is created from ast.Node first, then optimized by the optimizer,
// finally used by the interlock to create a Cursor which executes the memex.
type Causet interface {
	// Get the schemaReplicant.
	Schema() *memex.Schema

	// Get the ID.
	ID() int

	// TP get the plan type.
	TP() string

	// Get the ID in explain memex
	ExplainID() fmt.Stringer

	// ExplainInfo returns operator information to be explained.
	ExplainInfo() string

	// replaceExprDeferredCausets replace all the column reference in the plan's memex node.
	replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset)

	SCtx() stochastikctx.Context

	// property.StatsInfo will return the property.StatsInfo for this plan.
	statsInfo() *property.StatsInfo

	// OutputNames returns the outputting names of each column.
	OutputNames() types.NameSlice

	// SetOutputNames sets the outputting name by the given slice.
	SetOutputNames(names types.NameSlice)

	SelectBlockOffset() int
}

func enforceProperty(p *property.PhysicalProperty, tsk task, ctx stochastikctx.Context) task {
	if p.IsEmpty() || tsk.plan() == nil {
		return tsk
	}
	tsk = finishCopTask(ctx, tsk)
	sortReqProp := &property.PhysicalProperty{TaskTp: property.RootTaskType, Items: p.Items, ExpectedCnt: math.MaxFloat64}
	sort := PhysicalSort{ByItems: make([]*soliton.ByItems, 0, len(p.Items))}.Init(ctx, tsk.plan().statsInfo(), tsk.plan().SelectBlockOffset(), sortReqProp)
	for _, col := range p.Items {
		sort.ByItems = append(sort.ByItems, &soliton.ByItems{Expr: col.DefCaus, Desc: col.Desc})
	}
	return sort.attach2Task(tsk)
}

// optimizeByShuffle insert `PhysicalShuffle` to optimize performance by running in a parallel manner.
func optimizeByShuffle(pp PhysicalCauset, tsk task, ctx stochastikctx.Context) task {
	if tsk.plan() == nil {
		return tsk
	}

	// Don't use `tsk.plan()` here, which will probably be different from `pp`.
	// Eg., when `pp` is `NominalSort`, `tsk.plan()` would be its child.
	switch p := pp.(type) {
	case *PhysicalWindow:
		if shuffle := optimizeByShuffle4Window(p, ctx); shuffle != nil {
			return shuffle.attach2Task(tsk)
		}
	}
	return tsk
}

func optimizeByShuffle4Window(pp *PhysicalWindow, ctx stochastikctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetStochastikVars().WindowConcurrency()
	if concurrency <= 1 {
		return nil
	}

	sort, ok := pp.Children()[0].(*PhysicalSort)
	if !ok {
		// Multi-thread executing on SORTED data source is not effective enough by current implementation.
		// TODO: Implement a better one.
		return nil
	}
	tail, dataSource := sort, sort.Children()[0]

	partitionBy := make([]*memex.DeferredCauset, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		partitionBy = append(partitionBy, item.DefCaus)
	}
	NDV := int(getCardinality(partitionBy, dataSource.Schema(), dataSource.statsInfo()))
	if NDV <= 1 {
		return nil
	}
	concurrency = mathutil.Min(concurrency, NDV)

	byItems := make([]memex.Expression, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		byItems = append(byItems, item.DefCaus)
	}
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := PhysicalShuffle{
		Concurrency:  concurrency,
		Tail:         tail,
		DataSource:   dataSource,
		SplitterType: PartitionHashSplitterType,
		HashByItems:  byItems,
	}.Init(ctx, pp.statsInfo(), pp.SelectBlockOffset(), reqProp)
	return shuffle
}

// LogicalCauset is a tree of logical operators.
// We can do a lot of logical optimizations to it, like predicate pushdown and column pruning.
type LogicalCauset interface {
	Causet

	// HashCode encodes a LogicalCauset to fast compare whether a LogicalCauset equals to another.
	// We use a strict encode method here which ensures there is no conflict.
	HashCode() []byte

	// PredicatePushDown pushes down the predicates in the where/on/having clauses as deeply as possible.
	// It will accept a predicate that is an memex slice, and return the memexs that can't be pushed.
	// Because it might change the root if the having clause exists, we need to return a plan that represents a new root.
	PredicatePushDown([]memex.Expression) ([]memex.Expression, LogicalCauset)

	// PruneDeferredCausets prunes the unused columns.
	PruneDeferredCausets([]*memex.DeferredCauset) error

	// findBestTask converts the logical plan to the physical plan. It's a new interface.
	// It is called recursively from the parent to the children to create the result physical plan.
	// Some logical plans will convert the children to the physical plans in different ways, and return the one
	// With the lowest cost and how many plans are found in this function.
	// planCounter is a counter for causet to force a plan.
	// If planCounter > 0, the clock_th plan generated in this function will be returned.
	// If planCounter = 0, the plan generated in this function will not be considered.
	// If planCounter = -1, then we will not force plan.
	findBestTask(prop *property.PhysicalProperty, planCounter *CausetCounterTp) (task, int64, error)

	// BuildKeyInfo will collect the information of unique keys into schemaReplicant.
	// Because this method is also used in cascades causet, we cannot use
	// things like `p.schemaReplicant` or `p.children` inside it. We should use the `selfSchema`
	// and `childSchema` instead.
	BuildKeyInfo(selfSchema *memex.Schema, childSchema []*memex.Schema)

	// pushDownTopN will push down the topN or limit operator during logical optimization.
	pushDownTopN(topN *LogicalTopN) LogicalCauset

	// recursiveDeriveStats derives statistic info between plans.
	recursiveDeriveStats(colGroups [][]*memex.DeferredCauset) (*property.StatsInfo, error)

	// DeriveStats derives statistic info for current plan node given child stats.
	// We need selfSchema, childSchema here because it makes this method can be used in
	// cascades causet, where LogicalCauset might not record its children or schemaReplicant.
	DeriveStats(childStats []*property.StatsInfo, selfSchema *memex.Schema, childSchema []*memex.Schema, colGroups [][]*memex.DeferredCauset) (*property.StatsInfo, error)

	// ExtractDefCausGroups extracts column groups from child operator whose DNVs are required by the current operator.
	// For example, if current operator is LogicalAggregation of `Group By a, b`, we indicate the child operators to maintain
	// and propagate the NDV info of column group (a, b), to improve the event count estimation of current LogicalAggregation.
	// The parameter colGroups are column groups required by upper operators, besides from the column groups derived from
	// current operator, we should pass down parent colGroups to child operator as many as possible.
	ExtractDefCausGroups(colGroups [][]*memex.DeferredCauset) [][]*memex.DeferredCauset

	// PreparePossibleProperties is only used for join and aggregation. Like group by a,b,c, all permutation of (a,b,c) is
	// valid, but the ordered indices in leaf plan is limited. So we can get all possible order properties by a pre-walking.
	PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset

	// exhaustPhysicalCausets generates all possible plans that can match the required property.
	// It will return:
	// 1. All possible plans that can match the required property.
	// 2. Whether the ALLEGROALLEGROSQL hint can work. Return true if there is no hint.
	exhaustPhysicalCausets(*property.PhysicalProperty) (physicalCausets []PhysicalCauset, hintCanWork bool)

	// ExtractCorrelatedDefCauss extracts correlated columns inside the LogicalCauset.
	ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset

	// MaxOneRow means whether this operator only returns max one event.
	MaxOneRow() bool

	// Get all the children.
	Children() []LogicalCauset

	// SetChildren sets the children for the plan.
	SetChildren(...LogicalCauset)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalCauset)

	// rollBackTaskMap roll back all taskMap's logs after TimeStamp TS.
	rollBackTaskMap(TS uint64)
}

// PhysicalCauset is a tree of the physical operators.
type PhysicalCauset interface {
	Causet

	// attach2Task makes the current physical plan as the father of task's physicalCauset and uFIDelates the cost of
	// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
	attach2Task(...task) task

	// ToPB converts physical plan to fidelpb interlock.
	ToPB(ctx stochastikctx.Context, storeType ekv.StoreType) (*fidelpb.InterlockingDirectorate, error)

	// getChildReqProps gets the required property by child index.
	GetChildReqProps(idx int) *property.PhysicalProperty

	// StatsCount returns the count of property.StatsInfo for this plan.
	StatsCount() float64

	// ExtractCorrelatedDefCauss extracts correlated columns inside the PhysicalCauset.
	ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset

	// Get all the children.
	Children() []PhysicalCauset

	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalCauset)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child PhysicalCauset)

	// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
	ResolveIndices() error

	// Stats returns the StatsInfo of the plan.
	Stats() *property.StatsInfo

	// ExplainNormalizedInfo returns operator normalized information for generating digest.
	ExplainNormalizedInfo() string

	// Clone clones this physical plan.
	Clone() (PhysicalCauset, error)
}

type baseLogicalCauset struct {
	baseCauset

	taskMap map[string]task
	// taskMapBak forms a backlog stack of taskMap, used to roll back the taskMap.
	taskMapBak []string
	// taskMapBakTS stores the timestamps of logs.
	taskMapBakTS []uint64
	self         LogicalCauset
	maxOneRow    bool
	children     []LogicalCauset
}

func (p *baseLogicalCauset) MaxOneRow() bool {
	return p.maxOneRow
}

// ExplainInfo implements Causet interface.
func (p *baseLogicalCauset) ExplainInfo() string {
	return ""
}

type basePhysicalCauset struct {
	baseCauset

	childrenReqProps []*property.PhysicalProperty
	self             PhysicalCauset
	children         []PhysicalCauset
}

func (p *basePhysicalCauset) cloneWithSelf(newSelf PhysicalCauset) (*basePhysicalCauset, error) {
	base := &basePhysicalCauset{
		baseCauset: p.baseCauset,
		self:     newSelf,
	}
	for _, child := range p.children {
		cloned, err := child.Clone()
		if err != nil {
			return nil, err
		}
		base.children = append(base.children, cloned)
	}
	for _, prop := range p.childrenReqProps {
		base.childrenReqProps = append(base.childrenReqProps, prop.Clone())
	}
	return base, nil
}

// Clone implements PhysicalCauset interface.
func (p *basePhysicalCauset) Clone() (PhysicalCauset, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p.self)
}

// ExplainInfo implements Causet interface.
func (p *basePhysicalCauset) ExplainInfo() string {
	return ""
}

// ExplainInfo implements Causet interface.
func (p *basePhysicalCauset) ExplainNormalizedInfo() string {
	return ""
}

func (p *basePhysicalCauset) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *basePhysicalCauset) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	return nil
}

// GetlogicalTS4TaskMap get the logical TimeStamp now to help rollback the TaskMap changes after that.
func (p *baseLogicalCauset) GetlogicalTS4TaskMap() uint64 {
	p.ctx.GetStochastikVars().StmtCtx.TaskMapBakTS += 1
	return p.ctx.GetStochastikVars().StmtCtx.TaskMapBakTS
}

func (p *baseLogicalCauset) rollBackTaskMap(TS uint64) {
	if !p.ctx.GetStochastikVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		return
	}
	if len(p.taskMapBak) > 0 {
		// Rollback all the logs with TimeStamp TS.
		N := len(p.taskMapBak)
		for i := 0; i < N; i++ {
			cur := p.taskMapBak[i]
			if p.taskMapBakTS[i] < TS {
				continue
			}

			// Remove the i_th log.
			p.taskMapBak = append(p.taskMapBak[:i], p.taskMapBak[i+1:]...)
			p.taskMapBakTS = append(p.taskMapBakTS[:i], p.taskMapBakTS[i+1:]...)
			i--
			N--

			// Roll back taskMap.
			p.taskMap[cur] = nil
		}
	}
	for _, child := range p.children {
		child.rollBackTaskMap(TS)
	}
}

func (p *baseLogicalCauset) getTask(prop *property.PhysicalProperty) task {
	key := prop.HashCode()
	return p.taskMap[string(key)]
}

func (p *baseLogicalCauset) storeTask(prop *property.PhysicalProperty, task task) {
	key := prop.HashCode()
	if p.ctx.GetStochastikVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		// Empty string for useless change.
		TS := p.GetlogicalTS4TaskMap()
		p.taskMapBakTS = append(p.taskMapBakTS, TS)
		p.taskMapBak = append(p.taskMapBak, string(key))
	}
	p.taskMap[string(key)] = task
}

// HasMaxOneRow returns if the LogicalCauset will output at most one event.
func HasMaxOneRow(p LogicalCauset, childMaxOneRow []bool) bool {
	if len(childMaxOneRow) == 0 {
		// The reason why we use this check is that, this function
		// is used both in causet/core and causet/cascades.
		// In cascades causet, LogicalCauset may have no `children`.
		return false
	}
	switch x := p.(type) {
	case *LogicalLock, *LogicalLimit, *LogicalSort, *LogicalSelection,
		*LogicalApply, *LogicalProjection, *LogicalWindow, *LogicalAggregation:
		return childMaxOneRow[0]
	case *LogicalMaxOneRow:
		return true
	case *LogicalJoin:
		switch x.JoinType {
		case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
			return childMaxOneRow[0]
		default:
			return childMaxOneRow[0] && childMaxOneRow[1]
		}
	}
	return false
}

// BuildKeyInfo implements LogicalCauset BuildKeyInfo interface.
func (p *baseLogicalCauset) BuildKeyInfo(selfSchema *memex.Schema, childSchema []*memex.Schema) {
	childMaxOneRow := make([]bool, len(p.children))
	for i := range p.children {
		childMaxOneRow[i] = p.children[i].MaxOneRow()
	}
	p.maxOneRow = HasMaxOneRow(p.self, childMaxOneRow)
}

// BuildKeyInfo implements LogicalCauset BuildKeyInfo interface.
func (p *logicalSchemaProducer) BuildKeyInfo(selfSchema *memex.Schema, childSchema []*memex.Schema) {
	selfSchema.Keys = nil
	p.baseLogicalCauset.BuildKeyInfo(selfSchema, childSchema)
}

func newBaseCauset(ctx stochastikctx.Context, tp string, offset int) baseCauset {
	ctx.GetStochastikVars().CausetID++
	id := ctx.GetStochastikVars().CausetID
	return baseCauset{
		tp:          tp,
		id:          id,
		ctx:         ctx,
		blockOffset: offset,
	}
}

func newBaseLogicalCauset(ctx stochastikctx.Context, tp string, self LogicalCauset, offset int) baseLogicalCauset {
	return baseLogicalCauset{
		taskMap:      make(map[string]task),
		taskMapBak:   make([]string, 0, 10),
		taskMapBakTS: make([]uint64, 0, 10),
		baseCauset:     newBaseCauset(ctx, tp, offset),
		self:         self,
	}
}

func newBasePhysicalCauset(ctx stochastikctx.Context, tp string, self PhysicalCauset, offset int) basePhysicalCauset {
	return basePhysicalCauset{
		baseCauset: newBaseCauset(ctx, tp, offset),
		self:     self,
	}
}

func (p *baseLogicalCauset) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	return nil
}

// PruneDeferredCausets implements LogicalCauset interface.
func (p *baseLogicalCauset) PruneDeferredCausets(parentUsedDefCauss []*memex.DeferredCauset) error {
	if len(p.children) == 0 {
		return nil
	}
	return p.children[0].PruneDeferredCausets(parentUsedDefCauss)
}

// baseCauset implements base Causet interface.
// Should be used as embedded struct in Causet implementations.
type baseCauset struct {
	tp          string
	id          int
	ctx         stochastikctx.Context
	stats       *property.StatsInfo
	blockOffset int
}

// OutputNames returns the outputting names of each column.
func (p *baseCauset) OutputNames() types.NameSlice {
	return nil
}

func (p *baseCauset) SetOutputNames(names types.NameSlice) {
}

func (p *baseCauset) replaceExprDeferredCausets(replace map[string]*memex.DeferredCauset) {
}

// ID implements Causet ID interface.
func (p *baseCauset) ID() int {
	return p.id
}

// property.StatsInfo implements the Causet interface.
func (p *baseCauset) statsInfo() *property.StatsInfo {
	return p.stats
}

// ExplainInfo implements Causet interface.
func (p *baseCauset) ExplainInfo() string {
	return "N/A"
}

func (p *baseCauset) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return p.tp + "_" + strconv.Itoa(p.id)
	})
}

// TP implements Causet interface.
func (p *baseCauset) TP() string {
	return p.tp
}

func (p *baseCauset) SelectBlockOffset() int {
	return p.blockOffset
}

// Stats implements Causet Stats interface.
func (p *baseCauset) Stats() *property.StatsInfo {
	return p.stats
}

// Schema implements Causet Schema interface.
func (p *baseLogicalCauset) Schema() *memex.Schema {
	return p.children[0].Schema()
}

func (p *baseLogicalCauset) OutputNames() types.NameSlice {
	return p.children[0].OutputNames()
}

func (p *baseLogicalCauset) SetOutputNames(names types.NameSlice) {
	p.children[0].SetOutputNames(names)
}

// Schema implements Causet Schema interface.
func (p *basePhysicalCauset) Schema() *memex.Schema {
	return p.children[0].Schema()
}

// Children implements LogicalCauset Children interface.
func (p *baseLogicalCauset) Children() []LogicalCauset {
	return p.children
}

// Children implements PhysicalCauset Children interface.
func (p *basePhysicalCauset) Children() []PhysicalCauset {
	return p.children
}

// SetChildren implements LogicalCauset SetChildren interface.
func (p *baseLogicalCauset) SetChildren(children ...LogicalCauset) {
	p.children = children
}

// SetChildren implements PhysicalCauset SetChildren interface.
func (p *basePhysicalCauset) SetChildren(children ...PhysicalCauset) {
	p.children = children
}

// SetChild implements LogicalCauset SetChild interface.
func (p *baseLogicalCauset) SetChild(i int, child LogicalCauset) {
	p.children[i] = child
}

// SetChild implements PhysicalCauset SetChild interface.
func (p *basePhysicalCauset) SetChild(i int, child PhysicalCauset) {
	p.children[i] = child
}

// Context implements Causet Context interface.
func (p *baseCauset) SCtx() stochastikctx.Context {
	return p.ctx
}
