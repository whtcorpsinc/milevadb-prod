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
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// task is a new version of `PhysicalCausetInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTask or a ParallelTask.
type task interface {
	count() float64
	addCost(cost float64)
	cost() float64
	copy() task
	plan() PhysicalCauset
	invalid() bool
}

// copTask is a task that runs in a distributed ekv causetstore.
// TODO: In future, we should split copTask to indexTask and blockTask.
type copTask struct {
	indexCauset PhysicalCauset
	blockCauset PhysicalCauset
	cst         float64
	// indexCausetFinished means we have finished index plan.
	indexCausetFinished bool
	// keepOrder indicates if the plan scans data by order.
	keepOrder bool
	// doubleReadNeedProj means an extra prune is needed because
	// in double read case, it may output one more column for handle(event id).
	doubleReadNeedProj bool

	extraHandleDefCaus   *memex.DeferredCauset
	commonHandleDefCauss []*memex.DeferredCauset
	// tblDefCausHists stores the original stats of DataSource, it is used to get
	// average event width when computing network cost.
	tblDefCausHists *statistics.HistDefCausl
	// tblDefCauss stores the original columns of DataSource before being pruned, it
	// is used to compute average event width when computing scan cost.
	tblDefCauss         []*memex.DeferredCauset
	idxMergePartCausets []PhysicalCauset
	// rootTaskConds stores select conditions containing virtual columns.
	// These conditions can't push to EinsteinDB, so we have to add a selection for rootTask
	rootTaskConds []memex.Expression

	// For causet partition.
	partitionInfo PartitionInfo
}

func (t *copTask) invalid() bool {
	return t.blockCauset == nil && t.indexCauset == nil
}

func (t *rootTask) invalid() bool {
	return t.p == nil
}

func (t *copTask) count() float64 {
	if t.indexCausetFinished {
		return t.blockCauset.statsInfo().RowCount
	}
	return t.indexCauset.statsInfo().RowCount
}

func (t *copTask) addCost(cst float64) {
	t.cst += cst
}

func (t *copTask) cost() float64 {
	return t.cst
}

func (t *copTask) copy() task {
	nt := *t
	return &nt
}

func (t *copTask) plan() PhysicalCauset {
	if t.indexCausetFinished {
		return t.blockCauset
	}
	return t.indexCauset
}

func attachCauset2Task(p PhysicalCauset, t task) task {
	switch v := t.(type) {
	case *copTask:
		if v.indexCausetFinished {
			p.SetChildren(v.blockCauset)
			v.blockCauset = p
		} else {
			p.SetChildren(v.indexCauset)
			v.indexCauset = p
		}
	case *rootTask:
		p.SetChildren(v.p)
		v.p = p
	}
	return t
}

// finishIndexCauset means we no longer add plan to index plan, and compute the network cost for it.
func (t *copTask) finishIndexCauset() {
	if t.indexCausetFinished {
		return
	}
	cnt := t.count()
	t.indexCausetFinished = true
	sessVars := t.indexCauset.SCtx().GetStochastikVars()
	// Network cost of transferring rows of index scan to MilevaDB.
	t.cst += cnt * sessVars.NetworkFactor * t.tblDefCausHists.GetAvgRowSize(t.indexCauset.SCtx(), t.indexCauset.Schema().DeferredCausets, true, false)

	if t.blockCauset == nil {
		return
	}
	// Calculate the IO cost of causet scan here because we cannot know its stats until we finish index plan.
	t.blockCauset.(*PhysicalBlockScan).stats = t.indexCauset.statsInfo()
	var p PhysicalCauset
	for p = t.indexCauset; len(p.Children()) > 0; p = p.Children()[0] {
	}
	rowSize := t.tblDefCausHists.GetIndexAvgRowSize(t.indexCauset.SCtx(), t.tblDefCauss, p.(*PhysicalIndexScan).Index.Unique)
	t.cst += cnt * rowSize * sessVars.ScanFactor
}

func (t *copTask) getStoreType() ekv.StoreType {
	if t.blockCauset == nil {
		return ekv.EinsteinDB
	}
	tp := t.blockCauset
	for len(tp.Children()) > 0 {
		if len(tp.Children()) > 1 {
			return ekv.TiFlash
		}
		tp = tp.Children()[0]
	}
	if ts, ok := tp.(*PhysicalBlockScan); ok {
		return ts.StoreType
	}
	return ekv.EinsteinDB
}

func (p *basePhysicalCauset) attach2Task(tasks ...task) task {
	t := finishCopTask(p.ctx, tasks[0].copy())
	return attachCauset2Task(p.self, t)
}

func (p *PhysicalUnionScan) attach2Task(tasks ...task) task {
	p.stats = tasks[0].plan().statsInfo()
	return p.basePhysicalCauset.attach2Task(tasks...)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schemaReplicant = BuildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: p.GetCost(lTask.count(), rTask.count(), lTask.cost(), rTask.cost()),
	}
}

// GetCost computes the cost of apply operator.
func (p *PhysicalApply) GetCost(lCount, rCount, lCost, rCost float64) float64 {
	var cpuCost float64
	sessVars := p.ctx.GetStochastikVars()
	if len(p.LeftConditions) > 0 {
		cpuCost += lCount * sessVars.CPUFactor
		lCount *= SelectionFactor
	}
	if len(p.RightConditions) > 0 {
		cpuCost += lCount * rCount * sessVars.CPUFactor
		rCount *= SelectionFactor
	}
	if len(p.EqualConditions)+len(p.OtherConditions) > 0 {
		if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
			p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
			cpuCost += lCount * rCount * sessVars.CPUFactor * 0.5
		} else {
			cpuCost += lCount * rCount * sessVars.CPUFactor
		}
	}
	// Apply uses a NestedLoop method for execution.
	// For every event from the left(outer) side, it executes
	// the whole right(inner) plan tree. So the cost of apply
	// should be : apply cost + left cost + left count * right cost
	return cpuCost + lCost + lCount*rCost
}

func (p *PhysicalIndexMergeJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := finishCopTask(p.ctx, tasks[1-p.InnerChildIdx].copy())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	return &rootTask{
		p:   p,
		cst: p.GetCost(outerTask, innerTask),
	}
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexMergeJoin) GetCost(outerTask, innerTask task) float64 {
	var cpuCost float64
	outerCnt, innerCnt := outerTask.count(), innerTask.count()
	sessVars := p.ctx.GetStochastikVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.CPUFactor * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.CPUFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (sortFactor + 1.0) * batchSize * cpuFactor
	// If `p.NeedOuterSort` is true, the sortFactor is batchSize * Log2(batchSize).
	// Otherwise, it's 0.
	batchSize := math.Min(float64(p.ctx.GetStochastikVars().IndexJoinBatchSize), outerCnt)
	sortFactor := 0.0
	if p.NeedOuterSort {
		sortFactor = math.Log2(float64(batchSize))
	}
	if batchSize > 2 {
		innerCPUCost += outerCnt * (sortFactor + 1.0) * sessVars.CPUFactor
	}
	// Add cost of building inner interlocks. CPU cost of building CausetTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * cpuFactor
	// Since we don't know the number of CausetTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.CPUFactor
	innerConcurrency := float64(p.ctx.GetStochastikVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of merge join in inner worker.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	avgProbeCnt := numPairs / outerCnt
	var probeCost float64
	// Inner workers do merge join in parallel, but they can only save ONE outer batch
	// results. So as the number of outer batch exceeds inner concurrency, it would fall back to
	// linear execution. In a word, the merge join only run in parallel for the first
	// `innerConcurrency` number of inner tasks.
	if outerCnt/batchSize >= innerConcurrency {
		probeCost = (numPairs - batchSize*avgProbeCnt*(innerConcurrency-1)) * sessVars.CPUFactor
	} else {
		probeCost = batchSize * avgProbeCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.ConcurrencyFactor

	// Index merge join save the join results in inner worker.
	// So the memory cost consider the results size for each batch.
	memoryCost := innerConcurrency * (batchSize * avgProbeCnt) * sessVars.MemoryFactor

	innerCausetCost := outerCnt * innerTask.cost()
	return outerTask.cost() + innerCausetCost + cpuCost + memoryCost
}

func (p *PhysicalIndexHashJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := finishCopTask(p.ctx, tasks[1-p.InnerChildIdx].copy())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	return &rootTask{
		p:   p,
		cst: p.GetCost(outerTask, innerTask),
	}
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexHashJoin) GetCost(outerTask, innerTask task) float64 {
	var cpuCost float64
	outerCnt, innerCnt := outerTask.count(), innerTask.count()
	sessVars := p.ctx.GetStochastikVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.CPUFactor * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.CPUFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(sessVars.IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.CPUFactor
	}
	// Add cost of building inner interlocks. CPU cost of building CausetTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * CPUFactor
	// Since we don't know the number of CausetTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.CPUFactor
	concurrency := float64(sessVars.IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / concurrency
	// CPU cost of building hash causet for outer results concurrently.
	// (outerCnt / batchSize) * (batchSize * CPUFactor)
	outerCPUCost := outerCnt * sessVars.CPUFactor
	cpuCost += outerCPUCost / concurrency
	// Cost of probing hash causet concurrently.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	// Inner workers do hash join in parallel, but they can only save ONE outer
	// batch results. So as the number of outer batch exceeds inner concurrency,
	// it would fall back to linear execution. In a word, the hash join only runs
	// in parallel for the first `innerConcurrency` number of inner tasks.
	var probeCost float64
	if outerCnt/batchSize >= concurrency {
		probeCost = (numPairs - batchSize*innerCnt*(concurrency-1)) * sessVars.CPUFactor
	} else {
		probeCost = batchSize * innerCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost
	// Cost of additional concurrent goroutines.
	cpuCost += (concurrency + 1.0) * sessVars.ConcurrencyFactor
	// Memory cost of hash blocks for outer rows. The computed result is the upper bound,
	// since the interlock is pipelined and not all workers are always in full load.
	memoryCost := concurrency * (batchSize * distinctFactor) * innerCnt * sessVars.MemoryFactor
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerCausetCost := outerCnt * innerTask.cost()
	return outerTask.cost() + innerCausetCost + cpuCost + memoryCost
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := finishCopTask(p.ctx, tasks[1-p.InnerChildIdx].copy())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	return &rootTask{
		p:   p,
		cst: p.GetCost(outerTask, innerTask),
	}
}

// GetCost computes the cost of index join operator and its children.
func (p *PhysicalIndexJoin) GetCost(outerTask, innerTask task) float64 {
	var cpuCost float64
	outerCnt, innerCnt := outerTask.count(), innerTask.count()
	sessVars := p.ctx.GetStochastikVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.CPUFactor * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.CPUFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(p.ctx.GetStochastikVars().IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.CPUFactor
	}
	// Add cost of building inner interlocks. CPU cost of building CausetTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * CPUFactor
	// Since we don't know the number of CausetTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.CPUFactor
	// CPU cost of building hash causet for inner results:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * innerCnt * CPUFactor
	innerCPUCost += outerCnt * distinctFactor * innerCnt * sessVars.CPUFactor
	innerConcurrency := float64(p.ctx.GetStochastikVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of probing hash causet in main thread.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	probeCost := numPairs * sessVars.CPUFactor
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.ConcurrencyFactor
	// Memory cost of hash blocks for inner rows. The computed result is the upper bound,
	// since the interlock is pipelined and not all workers are always in full load.
	memoryCost := innerConcurrency * (batchSize * distinctFactor) * innerCnt * sessVars.MemoryFactor
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerCausetCost := outerCnt * innerTask.cost()
	return outerTask.cost() + innerCausetCost + cpuCost + memoryCost
}

func getAvgRowSize(stats *property.StatsInfo, schemaReplicant *memex.Schema) (size float64) {
	if stats.HistDefCausl != nil {
		size = stats.HistDefCausl.GetAvgRowSizeListInDisk(schemaReplicant.DeferredCausets)
	} else {
		// Estimate using just the type info.
		defcaus := schemaReplicant.DeferredCausets
		for _, col := range defcaus {
			size += float64(chunk.EstimateTypeWidth(col.GetType()))
		}
	}
	return
}

// GetCost computes cost of hash join operator itself.
func (p *PhysicalHashJoin) GetCost(lCnt, rCnt float64) float64 {
	buildCnt, probeCnt := lCnt, rCnt
	build := p.children[0]
	// Taking the right as the inner for right join or using the outer to build a hash causet.
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		buildCnt, probeCnt = rCnt, lCnt
		build = p.children[1]
	}
	sessVars := p.ctx.GetStochastikVars()
	oomUseTmpStorage := config.GetGlobalConfig().OOMUseTmpStorage
	memQuota := sessVars.StmtCtx.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(build.statsInfo(), build.Schema())
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*buildCnt > float64(memQuota)
	// Cost of building hash causet.
	cpuCost := buildCnt * sessVars.CPUFactor
	memoryCost := buildCnt * sessVars.MemoryFactor
	diskCost := buildCnt * sessVars.DiskFactor * rowSize
	// Number of matched event pairs regarding the equal join conditions.
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	// For semi-join class, if `OtherConditions` is empty, we already know
	// the join results after querying hash causet, otherwise, we have to
	// evaluate those resulted event pairs after querying hash causet; if we
	// find one pair satisfying the `OtherConditions`, we then know the
	// join result for this given outer event, otherwise we have to iterate
	// to the end of those pairs; since we have no idea about when we can
	// terminate the iteration, we assume that we need to iterate half of
	// those pairs in average.
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	// Cost of querying hash causet is cheap actually, so we just compute the cost of
	// evaluating `OtherConditions` and joining event pairs.
	probeCost := numPairs * sessVars.CPUFactor
	probeDiskCost := numPairs * sessVars.DiskFactor * rowSize
	// Cost of evaluating outer filter.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		// Input outer count for the above compution should be adjusted by SelectionFactor.
		probeCost *= SelectionFactor
		probeDiskCost *= SelectionFactor
		probeCost += probeCnt * sessVars.CPUFactor
	}
	diskCost += probeDiskCost
	probeCost /= float64(p.Concurrency)
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + float64(p.Concurrency+1)*sessVars.ConcurrencyFactor
	// Cost of traveling the hash causet to resolve missing matched cases when building the hash causet from the outer causet
	if p.UseOuterToBuild {
		if spill {
			// It runs in sequence when build data is on disk. See handleUnmatchedRowsFromHashBlockInDisk
			cpuCost += buildCnt * sessVars.CPUFactor
		} else {
			cpuCost += buildCnt * sessVars.CPUFactor / float64(p.Concurrency)
		}
		diskCost += buildCnt * sessVars.DiskFactor * rowSize
	}

	if spill {
		memoryCost *= float64(memQuota) / (rowSize * buildCnt)
	} else {
		diskCost = 0
	}
	return cpuCost + memoryCost + diskCost
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	task := &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
	return task
}

// GetCost computes cost of broadcast join operator itself.
func (p *PhysicalBroadCastJoin) GetCost(lCnt, rCnt float64) float64 {
	buildCnt := lCnt
	if p.InnerChildIdx == 1 {
		buildCnt = rCnt
	}
	sessVars := p.ctx.GetStochastikVars()
	// Cost of building hash causet.
	cpuCost := buildCnt * sessVars.CopCPUFactor
	memoryCost := buildCnt * sessVars.MemoryFactor
	// Number of matched event pairs regarding the equal join conditions.
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	probeCost := numPairs * sessVars.CopCPUFactor
	// should divided by the concurrency in tiflash, which should be the number of embedded in tiflash nodes.
	probeCost /= float64(sessVars.CopTiFlashConcurrencyFactor)
	cpuCost += probeCost

	// todo since TiFlash join is significant faster than MilevaDB join, maybe
	//  need to add a variable like 'tiflash_accelerate_factor', and divide
	//  the final cost by that factor
	return cpuCost + memoryCost
}

func (p *PhysicalBroadCastJoin) attach2Task(tasks ...task) task {
	lTask, lok := tasks[0].(*copTask)
	rTask, rok := tasks[1].(*copTask)
	if !lok || !rok || (lTask.getStoreType() != ekv.TiFlash && rTask.getStoreType() != ekv.TiFlash) {
		return invalidTask
	}
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schemaReplicant = BuildPhysicalJoinSchema(p.JoinType, p)
	if !lTask.indexCausetFinished {
		lTask.finishIndexCauset()
	}
	if !rTask.indexCausetFinished {
		rTask.finishIndexCauset()
	}

	lCost := lTask.cost()
	rCost := rTask.cost()

	task := &copTask{
		tblDefCausHists:     rTask.tblDefCausHists,
		indexCausetFinished: true,
		blockCauset:         p,
		cst:                 lCost + rCost + p.GetCost(lTask.count(), rTask.count()),
	}
	return task
}

// GetCost computes cost of merge join operator itself.
func (p *PhysicalMergeJoin) GetCost(lCnt, rCnt float64) float64 {
	outerCnt := lCnt
	innerKeys := p.RightJoinKeys
	innerSchema := p.children[1].Schema()
	innerStats := p.children[1].statsInfo()
	if p.JoinType == RightOuterJoin {
		outerCnt = rCnt
		innerKeys = p.LeftJoinKeys
		innerSchema = p.children[0].Schema()
		innerStats = p.children[0].statsInfo()
	}
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	sessVars := p.ctx.GetStochastikVars()
	probeCost := numPairs * sessVars.CPUFactor
	// Cost of evaluating outer filters.
	var cpuCost float64
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		probeCost *= SelectionFactor
		cpuCost += outerCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost
	// For merge join, only one group of rows with same join key(not null) are cached,
	// we compute average memory cost using estimated group size.
	NDV := getCardinality(innerKeys, innerSchema, innerStats)
	memoryCost := (innerStats.RowCount / NDV) * sessVars.MemoryFactor
	return cpuCost + memoryCost
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
}

func buildIndexLookUpTask(ctx stochastikctx.Context, t *copTask) *rootTask {
	newTask := &rootTask{cst: t.cst}
	sessVars := ctx.GetStochastikVars()
	p := PhysicalIndexLookUpReader{
		blockCauset:          t.blockCauset,
		indexCauset:          t.indexCauset,
		ExtraHandleDefCaus:   t.extraHandleDefCaus,
		CommonHandleDefCauss: t.commonHandleDefCauss,
	}.Init(ctx, t.blockCauset.SelectBlockOffset())
	p.PartitionInfo = t.partitionInfo
	setBlockScanToBlockRowIDScan(p.blockCauset)
	p.stats = t.blockCauset.statsInfo()
	// Add cost of building causet reader interlocks. Handles are extracted in batch style,
	// each handle is a range, the CPU cost of building CausetTasks should be:
	// (indexRows / batchSize) * batchSize * CPUFactor
	// Since we don't know the number of CausetTasks built, ignore these network cost now.
	indexRows := t.indexCauset.statsInfo().RowCount
	newTask.cst += indexRows * sessVars.CPUFactor
	// Add cost of worker goroutines in index lookup.
	numTblWorkers := float64(sessVars.IndexLookupConcurrency())
	newTask.cst += (numTblWorkers + 1) * sessVars.ConcurrencyFactor
	// When building causet reader interlock for each batch, we would sort the handles. CPU
	// cost of sort is:
	// CPUFactor * batchSize * Log2(batchSize) * (indexRows / batchSize)
	indexLookupSize := float64(sessVars.IndexLookupSize)
	batchSize := math.Min(indexLookupSize, indexRows)
	if batchSize > 2 {
		sortCPUCost := (indexRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
		newTask.cst += sortCPUCost
	}
	// Also, we need to sort the retrieved rows if index lookup reader is expected to return
	// ordered results. Note that event count of these two sorts can be different, if there are
	// operators above causet scan.
	blockRows := t.blockCauset.statsInfo().RowCount
	selectivity := blockRows / indexRows
	batchSize = math.Min(indexLookupSize*selectivity, blockRows)
	if t.keepOrder && batchSize > 2 {
		sortCPUCost := (blockRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
		newTask.cst += sortCPUCost
	}
	if t.doubleReadNeedProj {
		schemaReplicant := p.IndexCausets[0].(*PhysicalIndexScan).dataSourceSchema
		proj := PhysicalProjection{Exprs: memex.DeferredCauset2Exprs(schemaReplicant.DeferredCausets)}.Init(ctx, p.stats, t.blockCauset.SelectBlockOffset(), nil)
		proj.SetSchema(schemaReplicant)
		proj.SetChildren(p)
		newTask.p = proj
	} else {
		newTask.p = p
	}
	return newTask
}

// finishCopTask means we close the interlock task and create a root task.
func finishCopTask(ctx stochastikctx.Context, task task) task {
	t, ok := task.(*copTask)
	if !ok {
		return task
	}
	sessVars := ctx.GetStochastikVars()
	// CausetTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistALLEGROSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistALLEGROSQLScanConcurrency.
	copIterWorkers := float64(t.plan().SCtx().GetStochastikVars().DistALLEGROSQLScanConcurrency())
	t.finishIndexCauset()
	// Network cost of transferring rows of causet scan to MilevaDB.
	if t.blockCauset != nil {
		t.cst += t.count() * sessVars.NetworkFactor * t.tblDefCausHists.GetAvgRowSize(ctx, t.blockCauset.Schema().DeferredCausets, false, false)

		tp := t.blockCauset
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalBroadCastJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalBlockScan)
		ts.DeferredCausets = ExpandVirtualDeferredCauset(ts.DeferredCausets, ts.schemaReplicant, ts.Block.DeferredCausets)
	}
	t.cst /= copIterWorkers
	newTask := &rootTask{
		cst: t.cst,
	}
	if t.idxMergePartCausets != nil {
		p := PhysicalIndexMergeReader{
			partialCausets: t.idxMergePartCausets,
			blockCauset:    t.blockCauset,
		}.Init(ctx, t.idxMergePartCausets[0].SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		setBlockScanToBlockRowIDScan(p.blockCauset)
		newTask.p = p
		return newTask
	}
	if t.indexCauset != nil && t.blockCauset != nil {
		newTask = buildIndexLookUpTask(ctx, t)
	} else if t.indexCauset != nil {
		p := PhysicalIndexReader{indexCauset: t.indexCauset}.Init(ctx, t.indexCauset.SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		p.stats = t.indexCauset.statsInfo()
		newTask.p = p
	} else {
		tp := t.blockCauset
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalBroadCastJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalBlockScan)
		p := PhysicalBlockReader{
			blockCauset:    t.blockCauset,
			StoreType:      ts.StoreType,
			IsCommonHandle: ts.Block.IsCommonHandle,
		}.Init(ctx, t.blockCauset.SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		p.stats = t.blockCauset.statsInfo()
		newTask.p = p
	}

	if len(t.rootTaskConds) > 0 {
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, newTask.p.statsInfo(), newTask.p.SelectBlockOffset())
		sel.SetChildren(newTask.p)
		newTask.p = sel
	}

	return newTask
}

// setBlockScanToBlockRowIDScan is to uFIDelate the isChildOfIndexLookUp attribute of PhysicalBlockScan child
func setBlockScanToBlockRowIDScan(p PhysicalCauset) {
	if ts, ok := p.(*PhysicalBlockScan); ok {
		ts.SetIsChildOfIndexLookUp(true)
	} else {
		for _, child := range p.Children() {
			setBlockScanToBlockRowIDScan(child)
		}
	}
}

// rootTask is the final sink node of a plan graph. It should be a single goroutine on milevadb.
type rootTask struct {
	p   PhysicalCauset
	cst float64
}

func (t *rootTask) copy() task {
	return &rootTask{
		p:   t.p,
		cst: t.cst,
	}
}

func (t *rootTask) count() float64 {
	return t.p.statsInfo().RowCount
}

func (t *rootTask) addCost(cst float64) {
	t.cst += cst
}

func (t *rootTask) cost() float64 {
	return t.cst
}

func (t *rootTask) plan() PhysicalCauset {
	return t.p
}

func (p *PhysicalLimit) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	sunk := false
	if cop, ok := t.(*copTask); ok {
		// For double read which requires order being kept, the limit cannot be pushed down to the causet side,
		// because handles would be reordered before being sent to causet scan.
		if (!cop.keepOrder || !cop.indexCausetFinished || cop.indexCauset == nil) && len(cop.rootTaskConds) == 0 {
			// When limit is pushed down, we should remove its offset.
			newCount := p.Offset + p.Count
			childProfile := cop.plan().statsInfo()
			// Strictly speaking, for the event count of stats, we should multiply newCount with "regionNum",
			// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
			stats := deriveLimitStats(childProfile, float64(newCount))
			pushedDownLimit := PhysicalLimit{Count: newCount}.Init(p.ctx, stats, p.blockOffset)
			cop = attachCauset2Task(pushedDownLimit, cop).(*copTask)
		}
		t = finishCopTask(p.ctx, cop)
		sunk = p.sinkIntoIndexLookUp(t)
	}
	if sunk {
		return t
	}
	return attachCauset2Task(p, t)
}

func (p *PhysicalLimit) sinkIntoIndexLookUp(t task) bool {
	root := t.(*rootTask)
	reader, isDoubleRead := root.p.(*PhysicalIndexLookUpReader)
	proj, isProj := root.p.(*PhysicalProjection)
	if !isDoubleRead && !isProj {
		return false
	}
	if isProj {
		reader, isDoubleRead = proj.Children()[0].(*PhysicalIndexLookUpReader)
		if !isDoubleRead {
			return false
		}
	}
	// We can sink Limit into IndexLookUpReader only if blockCauset contains no Selection.
	ts, isBlockScan := reader.blockCauset.(*PhysicalBlockScan)
	if !isBlockScan {
		return false
	}
	reader.PushedLimit = &PushedDownLimit{
		Offset: p.Offset,
		Count:  p.Count,
	}
	ts.stats = p.stats
	reader.stats = p.stats
	if isProj {
		proj.stats = p.stats
	}
	return true
}

// GetCost computes cost of TopN operator itself.
func (p *PhysicalTopN) GetCost(count float64, isRoot bool) float64 {
	heapSize := float64(p.Offset + p.Count)
	if heapSize < 2.0 {
		heapSize = 2.0
	}
	sessVars := p.ctx.GetStochastikVars()
	// Ignore the cost of `doCompaction` in current implementation of `TopNInterDirc`, since it is the
	// special side-effect of our Chunk format in MilevaDB layer, which may not exist in interlock's
	// implementation, or may be removed in the future if we change data format.
	// Note that we are using worst complexity to compute CPU cost, because it is simpler compared with
	// considering probabilities of average complexity, i.e, we may not need adjust heap for each input
	// event.
	var cpuCost float64
	if isRoot {
		cpuCost = count * math.Log2(heapSize) * sessVars.CPUFactor
	} else {
		cpuCost = count * math.Log2(heapSize) * sessVars.CopCPUFactor
	}
	memoryCost := heapSize * sessVars.MemoryFactor
	return cpuCost + memoryCost
}

// canPushDown checks if this topN can be pushed down. If each of the memex can be converted to pb, it can be pushed.
func (p *PhysicalTopN) canPushDown(cop *copTask) bool {
	exprs := make([]memex.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	return memex.CanExprsPushDown(p.ctx.GetStochastikVars().StmtCtx, exprs, p.ctx.GetClient(), cop.getStoreType())
}

func (p *PhysicalTopN) allDefCaussFromSchema(schemaReplicant *memex.Schema) bool {
	defcaus := make([]*memex.DeferredCauset, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		defcaus = append(defcaus, memex.ExtractDeferredCausets(item.Expr)...)
	}
	return len(schemaReplicant.DeferredCausetsIndices(defcaus)) > 0
}

// GetCost computes the cost of in memory sort.
func (p *PhysicalSort) GetCost(count float64, schemaReplicant *memex.Schema) float64 {
	if count < 2.0 {
		count = 2.0
	}
	sessVars := p.ctx.GetStochastikVars()
	cpuCost := count * math.Log2(count) * sessVars.CPUFactor
	memoryCost := count * sessVars.MemoryFactor

	oomUseTmpStorage := config.GetGlobalConfig().OOMUseTmpStorage
	memQuota := sessVars.StmtCtx.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(p.statsInfo(), schemaReplicant)
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*count > float64(memQuota)
	diskCost := count * sessVars.DiskFactor * rowSize
	if !spill {
		diskCost = 0
	} else {
		memoryCost *= float64(memQuota) / (rowSize * count)
	}
	return cpuCost + memoryCost + diskCost
}

func (p *PhysicalSort) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	t = attachCauset2Task(p, t)
	t.addCost(p.GetCost(t.count(), p.Schema()))
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	if p.OnlyDeferredCauset {
		return tasks[0]
	}
	t := tasks[0].copy()
	t = attachCauset2Task(p, t)
	return t
}

func (p *PhysicalTopN) getPushedDownTopN(childCauset PhysicalCauset) *PhysicalTopN {
	newByItems := make([]*soliton.ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	newCount := p.Offset + p.Count
	childProfile := childCauset.statsInfo()
	// Strictly speaking, for the event count of pushed down TopN, we should multiply newCount with "regionNum",
	// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
	stats := deriveLimitStats(childProfile, float64(newCount))
	topN := PhysicalTopN{
		ByItems: newByItems,
		Count:   newCount,
	}.Init(p.ctx, stats, p.blockOffset)
	topN.SetChildren(childCauset)
	return topN
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputCount := t.count()
	if copTask, ok := t.(*copTask); ok && p.canPushDown(copTask) && len(copTask.rootTaskConds) == 0 {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to causet plan.
		var pushedDownTopN *PhysicalTopN
		if !copTask.indexCausetFinished && p.allDefCaussFromSchema(copTask.indexCauset.Schema()) {
			pushedDownTopN = p.getPushedDownTopN(copTask.indexCauset)
			copTask.indexCauset = pushedDownTopN
		} else {
			copTask.finishIndexCauset()
			pushedDownTopN = p.getPushedDownTopN(copTask.blockCauset)
			copTask.blockCauset = pushedDownTopN
		}
		copTask.addCost(pushedDownTopN.GetCost(inputCount, false))
	}
	rootTask := finishCopTask(p.ctx, t)
	rootTask.addCost(p.GetCost(rootTask.count(), true))
	rootTask = attachCauset2Task(p, rootTask)
	return rootTask
}

// GetCost computes the cost of projection operator itself.
func (p *PhysicalProjection) GetCost(count float64) float64 {
	sessVars := p.ctx.GetStochastikVars()
	cpuCost := count * sessVars.CPUFactor
	concurrency := float64(sessVars.ProjectionConcurrency())
	if concurrency <= 0 {
		return cpuCost
	}
	cpuCost /= concurrency
	concurrencyCost := (1 + concurrency) * sessVars.ConcurrencyFactor
	return cpuCost + concurrencyCost
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if copTask, ok := t.(*copTask); ok {
		// TODO: support projection push down.
		t = finishCopTask(p.ctx, copTask)
	}
	t = attachCauset2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	return t
}

func (p *PhysicalUnionAll) attach2Task(tasks ...task) task {
	t := &rootTask{p: p}
	childCausets := make([]PhysicalCauset, 0, len(tasks))
	var childMaxCost float64
	for _, task := range tasks {
		task = finishCopTask(p.ctx, task)
		childCost := task.cost()
		if childCost > childMaxCost {
			childMaxCost = childCost
		}
		childCausets = append(childCausets, task.plan())
	}
	p.SetChildren(childCausets...)
	sessVars := p.ctx.GetStochastikVars()
	// Children of UnionInterDirc are executed in parallel.
	t.cst = childMaxCost + float64(1+len(tasks))*sessVars.ConcurrencyFactor
	return t
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	sessVars := sel.ctx.GetStochastikVars()
	t := finishCopTask(sel.ctx, tasks[0].copy())
	t.addCost(t.count() * sessVars.CPUFactor)
	t = attachCauset2Task(sel, t)
	return t
}

// CheckAggCanPushCop checks whether the aggFuncs and groupByItems can
// be pushed down to interlock.
func CheckAggCanPushCop(sctx stochastikctx.Context, aggFuncs []*aggregation.AggFuncDesc, groupByItems []memex.Expression, storeType ekv.StoreType) bool {
	sc := sctx.GetStochastikVars().StmtCtx
	client := sctx.GetClient()
	for _, aggFunc := range aggFuncs {
		if memex.ContainVirtualDeferredCauset(aggFunc.Args) {
			return false
		}
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			return false
		}
		if !aggregation.CheckAggPushDown(aggFunc, storeType) {
			return false
		}
		if !memex.CanExprsPushDown(sc, aggFunc.Args, client, storeType) {
			return false
		}
	}
	if memex.ContainVirtualDeferredCauset(groupByItems) {
		return false
	}
	return memex.CanExprsPushDown(sc, groupByItems, client, storeType)
}

// AggInfo stores the information of an Aggregation.
type AggInfo struct {
	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []memex.Expression
	Schema       *memex.Schema
}

// BuildFinalModeAggregation splits either LogicalAggregation or PhysicalAggregation to finalAgg and partial1Agg,
// returns the information of partial and final agg.
// partialIsCop means whether partial agg is a cop task.
func BuildFinalModeAggregation(
	sctx stochastikctx.Context, original *AggInfo, partialIsCop bool) (partial, final *AggInfo, funcMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) {

	funcMap = make(map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc, len(original.AggFuncs))
	partial = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(original.AggFuncs)),
		GroupByItems: original.GroupByItems,
		Schema:       memex.NewSchema(),
	}
	partialCursor := 0
	final = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, len(original.AggFuncs)),
		GroupByItems: make([]memex.Expression, 0, len(original.GroupByItems)),
		Schema:       original.Schema,
	}

	partialGbySchema := memex.NewSchema()
	// add group by columns
	for _, gbyExpr := range partial.GroupByItems {
		var gbyDefCaus *memex.DeferredCauset
		if col, ok := gbyExpr.(*memex.DeferredCauset); ok {
			gbyDefCaus = col
		} else {
			gbyDefCaus = &memex.DeferredCauset{
				UniqueID: sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
				RetType:  gbyExpr.GetType(),
			}
		}
		partialGbySchema.Append(gbyDefCaus)
		final.GroupByItems = append(final.GroupByItems, gbyDefCaus)
	}

	// TODO: Refactor the way of constructing aggregation functions.
	// This fop loop is ugly, but I do not find a proper way to reconstruct
	// it right away.
	for i, aggFunc := range original.AggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFunc.Name
		args := make([]memex.Expression, 0, len(aggFunc.Args))
		if aggFunc.HasDistinct {
			/*
				eg: SELECT COUNT(DISTINCT a), SUM(b) FROM t GROUP BY c

				change from
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
				to
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
						[cop]: group by: c, a
			*/
			for _, distinctArg := range aggFunc.Args {
				// 1. add all args to partial.GroupByItems
				foundInGroupBy := false
				for j, gbyExpr := range partial.GroupByItems {
					if gbyExpr.Equal(sctx, distinctArg) {
						foundInGroupBy = true
						args = append(args, partialGbySchema.DeferredCausets[j])
						break
					}
				}
				if !foundInGroupBy {
					partial.GroupByItems = append(partial.GroupByItems, distinctArg)
					var gbyDefCaus *memex.DeferredCauset
					if col, ok := distinctArg.(*memex.DeferredCauset); ok {
						gbyDefCaus = col
					} else {
						gbyDefCaus = &memex.DeferredCauset{
							UniqueID: sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
							RetType:  distinctArg.GetType(),
						}
					}
					partialGbySchema.Append(gbyDefCaus)
					if !partialIsCop {
						// if partial is a cop task, firstrow function is redundant since group by items are outputted
						// by group by schemaReplicant, and final functions use group by schemaReplicant as their arguments.
						// if partial agg is not cop, we must append firstrow function & schemaReplicant, to output the group by
						// items.
						// maybe we can unify them sometime.
						firstRow, err := aggregation.NewAggFuncDesc(sctx, ast.AggFuncFirstRow, []memex.Expression{gbyDefCaus}, false)
						if err != nil {
							panic("NewAggFuncDesc FirstRow meets error: " + err.Error())
						}
						partial.AggFuncs = append(partial.AggFuncs, firstRow)
						newDefCaus, _ := gbyDefCaus.Clone().(*memex.DeferredCauset)
						newDefCaus.RetType = firstRow.RetTp
						partial.Schema.Append(newDefCaus)
						partialCursor++
					}
					args = append(args, gbyDefCaus)
				}
			}

			finalAggFunc.HasDistinct = true
			finalAggFunc.Mode = aggregation.CompleteMode
		} else {
			if aggregation.NeedCount(finalAggFunc.Name) {
				ft := types.NewFieldType(allegrosql.TypeLonglong)
				ft.Flen, ft.Charset, ft.DefCauslate = 21, charset.CharsetBin, charset.DefCauslationBin
				partial.Schema.Append(&memex.DeferredCauset{
					UniqueID: sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
					RetType:  ft,
				})
				args = append(args, partial.Schema.DeferredCausets[partialCursor])
				partialCursor++
			}
			if finalAggFunc.Name == ast.AggFuncApproxCountDistinct {
				ft := types.NewFieldType(allegrosql.TypeString)
				ft.Charset, ft.DefCauslate = charset.CharsetBin, charset.DefCauslationBin
				ft.Flag |= allegrosql.NotNullFlag
				partial.Schema.Append(&memex.DeferredCauset{
					UniqueID: sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
					RetType:  ft,
				})
				args = append(args, partial.Schema.DeferredCausets[partialCursor])
				partialCursor++
			}
			if aggregation.NeedValue(finalAggFunc.Name) {
				partial.Schema.Append(&memex.DeferredCauset{
					UniqueID: sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
					RetType:  original.Schema.DeferredCausets[i].GetType(),
				})
				args = append(args, partial.Schema.DeferredCausets[partialCursor])
				partialCursor++
			}
			if aggFunc.Name == ast.AggFuncAvg {
				cntAgg := *aggFunc
				cntAgg.Name = ast.AggFuncCount
				cntAgg.RetTp = partial.Schema.DeferredCausets[partialCursor-2].GetType()
				cntAgg.RetTp.Flag = aggFunc.RetTp.Flag
				sumAgg := *aggFunc
				sumAgg.Name = ast.AggFuncSum
				sumAgg.RetTp = partial.Schema.DeferredCausets[partialCursor-1].GetType()
				partial.AggFuncs = append(partial.AggFuncs, &cntAgg, &sumAgg)
			} else if aggFunc.Name == ast.AggFuncApproxCountDistinct {
				approxCountDistinctAgg := *aggFunc
				approxCountDistinctAgg.Name = ast.AggFuncApproxCountDistinct
				approxCountDistinctAgg.RetTp = partial.Schema.DeferredCausets[partialCursor-1].GetType()
				partial.AggFuncs = append(partial.AggFuncs, &approxCountDistinctAgg)
			} else {
				partial.AggFuncs = append(partial.AggFuncs, aggFunc)
			}

			finalAggFunc.Mode = aggregation.FinalMode
			funcMap[aggFunc] = finalAggFunc
		}

		finalAggFunc.Args = args
		finalAggFunc.RetTp = aggFunc.RetTp
		final.AggFuncs[i] = finalAggFunc
	}
	partial.Schema.Append(partialGbySchema.DeferredCausets...)
	return
}

func (p *basePhysicalAgg) newPartialAggregate(copTaskType ekv.StoreType) (partial, final PhysicalCauset) {
	// Check if this aggregation can push down.
	if !CheckAggCanPushCop(p.ctx, p.AggFuncs, p.GroupByItems, copTaskType) {
		return nil, p.self
	}
	partialPref, finalPref, funcMap := BuildFinalModeAggregation(p.ctx, &AggInfo{
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
		Schema:       p.Schema().Clone(),
	}, true)
	if p.tp == plancodec.TypeStreamAgg && len(partialPref.GroupByItems) != len(finalPref.GroupByItems) {
		return nil, p.self
	}
	// Remove unnecessary FirstRow.
	partialPref.AggFuncs = RemoveUnnecessaryFirstRow(p.ctx,
		finalPref.AggFuncs, finalPref.GroupByItems,
		partialPref.AggFuncs, partialPref.GroupByItems, partialPref.Schema, funcMap)
	if copTaskType == ekv.MilevaDB {
		// For partial agg of MilevaDB cop task, since MilevaDB interlock reuse the MilevaDB interlock,
		// and MilevaDB aggregation interlock won't output the group by value,
		// so we need add `firstrow` aggregation function to output the group by value.
		aggFuncs, err := genFirstRowAggForGroupBy(p.ctx, partialPref.GroupByItems)
		if err != nil {
			return nil, p.self
		}
		partialPref.AggFuncs = append(partialPref.AggFuncs, aggFuncs...)
	}
	p.AggFuncs = partialPref.AggFuncs
	p.GroupByItems = partialPref.GroupByItems
	p.schemaReplicant = partialPref.Schema
	partialAgg := p.self
	// Create physical "final" aggregation.
	prop := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if p.tp == plancodec.TypeStreamAgg {
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalPref.AggFuncs,
			GroupByItems: finalPref.GroupByItems,
		}.initForStream(p.ctx, p.stats, p.blockOffset, prop)
		finalAgg.schemaReplicant = finalPref.Schema
		return partialAgg, finalAgg
	}

	finalAgg := basePhysicalAgg{
		AggFuncs:     finalPref.AggFuncs,
		GroupByItems: finalPref.GroupByItems,
	}.initForHash(p.ctx, p.stats, p.blockOffset, prop)
	finalAgg.schemaReplicant = finalPref.Schema
	return partialAgg, finalAgg
}

func genFirstRowAggForGroupBy(ctx stochastikctx.Context, groupByItems []memex.Expression) ([]*aggregation.AggFuncDesc, error) {
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(groupByItems))
	for _, groupBy := range groupByItems {
		agg, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncFirstRow, []memex.Expression{groupBy}, false)
		if err != nil {
			return nil, err
		}
		aggFuncs = append(aggFuncs, agg)
	}
	return aggFuncs, nil
}

// RemoveUnnecessaryFirstRow removes unnecessary FirstRow of the aggregation. This function can be
// used for both LogicalAggregation and PhysicalAggregation.
// When the select column is same with the group by key, the column can be removed and gets value from the group by key.
// e.g
// select a, count(b) from t group by a;
// The schemaReplicant is [firstrow(a), count(b), a]. The column firstrow(a) is unnecessary.
// Can optimize the schemaReplicant to [count(b), a] , and change the index to get value.
func RemoveUnnecessaryFirstRow(
	sctx stochastikctx.Context,
	finalAggFuncs []*aggregation.AggFuncDesc,
	finalGbyItems []memex.Expression,
	partialAggFuncs []*aggregation.AggFuncDesc,
	partialGbyItems []memex.Expression,
	partialSchema *memex.Schema,
	funcMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) []*aggregation.AggFuncDesc {

	partialCursor := 0
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(partialAggFuncs))
	for _, aggFunc := range partialAggFuncs {
		if aggFunc.Name == ast.AggFuncFirstRow {
			canOptimize := false
			for j, gbyExpr := range partialGbyItems {
				if j >= len(finalGbyItems) {
					// after distinct push, len(partialGbyItems) may larger than len(finalGbyItems)
					// for example,
					// select /*+ HASH_AGG() */ a, count(distinct a) from t;
					// will generate to,
					//   HashAgg root  funcs:count(distinct a), funcs:firstrow(a)"
					//     HashAgg cop  group by:a, funcs:firstrow(a)->DeferredCauset#6"
					// the firstrow in root task can not be removed.
					break
				}
				if gbyExpr.Equal(sctx, aggFunc.Args[0]) {
					canOptimize = true
					funcMap[aggFunc].Args[0] = finalGbyItems[j]
					break
				}
			}
			if canOptimize {
				partialSchema.DeferredCausets = append(partialSchema.DeferredCausets[:partialCursor], partialSchema.DeferredCausets[partialCursor+1:]...)
				continue
			}
		}
		partialCursor += computePartialCursorOffset(aggFunc.Name)
		newAggFuncs = append(newAggFuncs, aggFunc)
	}
	return newAggFuncs
}

func computePartialCursorOffset(name string) int {
	offset := 0
	if aggregation.NeedCount(name) {
		offset++
	}
	if aggregation.NeedValue(name) {
		offset++
	}
	if name == ast.AggFuncApproxCountDistinct {
		offset++
	}
	return offset
}

func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		// We should not push agg down across double read, since the data of second read is ordered by handle instead of index.
		// The `extraHandleDefCaus` is added if the double read needs to keep order. So we just use it to decided
		// whether the following plan is double read with order reserved.
		if cop.extraHandleDefCaus != nil || len(cop.rootTaskConds) > 0 {
			t = finishCopTask(p.ctx, cop)
			inputRows = t.count()
			attachCauset2Task(p, t)
		} else {
			copTaskType := cop.getStoreType()
			partialAgg, finalAgg := p.newPartialAggregate(copTaskType)
			if partialAgg != nil {
				if cop.blockCauset != nil {
					cop.finishIndexCauset()
					partialAgg.SetChildren(cop.blockCauset)
					cop.blockCauset = partialAgg
				} else {
					partialAgg.SetChildren(cop.indexCauset)
					cop.indexCauset = partialAgg
				}
				cop.addCost(p.GetCost(inputRows, false))
			}
			t = finishCopTask(p.ctx, cop)
			inputRows = t.count()
			attachCauset2Task(finalAgg, t)
		}
	} else {
		attachCauset2Task(p, t)
	}
	t.addCost(p.GetCost(inputRows, true))
	return t
}

// GetCost computes cost of stream aggregation considering CPU/memory.
func (p *PhysicalStreamAgg) GetCost(inputRows float64, isRoot bool) float64 {
	aggFuncFactor := p.getAggFuncCostFactor()
	var cpuCost float64
	sessVars := p.ctx.GetStochastikVars()
	if isRoot {
		cpuCost = inputRows * sessVars.CPUFactor * aggFuncFactor
	} else {
		cpuCost = inputRows * sessVars.CopCPUFactor * aggFuncFactor
	}
	rowsPerGroup := inputRows / p.statsInfo().RowCount
	memoryCost := rowsPerGroup * distinctFactor * sessVars.MemoryFactor * float64(p.numDistinctFunc())
	return cpuCost + memoryCost
}

// cpuCostDivisor computes the concurrency to which we would amortize CPU cost
// for hash aggregation.
func (p *PhysicalHashAgg) cpuCostDivisor(hasDistinct bool) (float64, float64) {
	if hasDistinct {
		return 0, 0
	}
	stochastikVars := p.ctx.GetStochastikVars()
	finalCon, partialCon := stochastikVars.HashAggFinalConcurrency(), stochastikVars.HashAggPartialConcurrency()
	// According to `ValidateSetSystemVar`, `finalCon` and `partialCon` cannot be less than or equal to 0.
	if finalCon == 1 && partialCon == 1 {
		return 0, 0
	}
	// It is tricky to decide which concurrency we should use to amortize CPU cost. Since cost of hash
	// aggregation is tend to be under-estimated as explained in `attach2Task`, we choose the smaller
	// concurrecy to make some compensation.
	return math.Min(float64(finalCon), float64(partialCon)), float64(finalCon + partialCon)
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		if len(cop.rootTaskConds) == 0 {
			copTaskType := cop.getStoreType()
			partialAgg, finalAgg := p.newPartialAggregate(copTaskType)
			if partialAgg != nil {
				if cop.blockCauset != nil {
					cop.finishIndexCauset()
					partialAgg.SetChildren(cop.blockCauset)
					cop.blockCauset = partialAgg
				} else {
					partialAgg.SetChildren(cop.indexCauset)
					cop.indexCauset = partialAgg
				}
				cop.addCost(p.GetCost(inputRows, false))
			}
			// In `newPartialAggregate`, we are using stats of final aggregation as stats
			// of `partialAgg`, so the network cost of transferring result rows of `partialAgg`
			// to MilevaDB is normally under-estimated for hash aggregation, since the group-by
			// column may be independent of the column used for region distribution, so a closer
			// estimation of network cost for hash aggregation may multiply the number of
			// regions involved in the `partialAgg`, which is unknown however.
			t = finishCopTask(p.ctx, cop)
			inputRows = t.count()
			attachCauset2Task(finalAgg, t)
		} else {
			t = finishCopTask(p.ctx, cop)
			inputRows = t.count()
			attachCauset2Task(p, t)
		}
	} else {
		attachCauset2Task(p, t)
	}
	// We may have 3-phase hash aggregation actually, strictly speaking, we'd better
	// calculate cost of each phase and sum the results up, but in fact we don't have
	// region level causet stats, and the concurrency of the `partialAgg`,
	// i.e, max(number_of_regions, DistALLEGROSQLScanConcurrency) is unknown either, so it is hard
	// to compute costs separately. We ignore region level parallelism for both hash
	// aggregation and stream aggregation when calculating cost, though this would lead to inaccuracy,
	// hopefully this inaccuracy would be imposed on both aggregation implementations,
	// so they are still comparable horizontally.
	// Also, we use the stats of `partialAgg` as the input of cost computing for MilevaDB layer
	// hash aggregation, it would cause under-estimation as the reason mentioned in comment above.
	// To make it simple, we also treat 2-phase parallel hash aggregation in MilevaDB layer as
	// 1-phase when computing cost.
	t.addCost(p.GetCost(inputRows, true))
	return t
}

// GetCost computes the cost of hash aggregation considering CPU/memory.
func (p *PhysicalHashAgg) GetCost(inputRows float64, isRoot bool) float64 {
	cardinality := p.statsInfo().RowCount
	numDistinctFunc := p.numDistinctFunc()
	aggFuncFactor := p.getAggFuncCostFactor()
	var cpuCost float64
	sessVars := p.ctx.GetStochastikVars()
	if isRoot {
		cpuCost = inputRows * sessVars.CPUFactor * aggFuncFactor
		divisor, con := p.cpuCostDivisor(numDistinctFunc > 0)
		if divisor > 0 {
			cpuCost /= divisor
			// Cost of additional goroutines.
			cpuCost += (con + 1) * sessVars.ConcurrencyFactor
		}
	} else {
		cpuCost = inputRows * sessVars.CopCPUFactor * aggFuncFactor
	}
	memoryCost := cardinality * sessVars.MemoryFactor * float64(len(p.AggFuncs))
	// When aggregation has distinct flag, we would allocate a map for each group to
	// check duplication.
	memoryCost += inputRows * distinctFactor * sessVars.MemoryFactor * float64(numDistinctFunc)
	return cpuCost + memoryCost
}
