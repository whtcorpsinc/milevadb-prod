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

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/dagger"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	utilhint "github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"go.uber.org/atomic"
)

// OptimizeAstNode optimizes the query to a physical plan directly.
var OptimizeAstNode func(ctx context.Context, sctx stochastikctx.Context, node ast.Node, is schemareplicant.SchemaReplicant) (Causet, types.NameSlice, error)

// AllowCartesianProduct means whether milevadb allows cartesian join without equal conditions.
var AllowCartesianProduct = atomic.NewBool(true)

const (
	flagGcSubstitute uint64 = 1 << iota
	flagPrunDeferredCausets
	flagBuildKeyInfo
	flagDecorrelate
	flagEliminateAgg
	flagEliminateProjection
	flagMaxMinEliminate
	flagPredicatePushDown
	flagEliminateOuterJoin
	flagPartitionProcessor
	flagPushDownAgg
	flagPushDownTopN
	flagJoinReOrder
	flagPrunDeferredCausetsAgain
)

var optMemruleList = []logicalOptMemrule{
	&gcSubstituter{},
	&columnPruner{},
	&buildKeySolver{},
	&decorrelateSolver{},
	&aggregationEliminator{},
	&projectionEliminator{},
	&maxMinEliminator{},
	&pFIDelSolver{},
	&outerJoinEliminator{},
	&partitionProcessor{},
	&aggregationPushDownSolver{},
	&pushDownTopNOptimizer{},
	&joinReOrderSolver{},
	&columnPruner{}, // column pruning again at last, note it will mess up the results of buildKeySolver
}

// logicalOptMemrule means a logical optimizing rule, which contains decorrelate, pFIDel, column pruning, etc.
type logicalOptMemrule interface {
	optimize(context.Context, LogicalCauset) (LogicalCauset, error)
	name() string
}

// BuildLogicalCauset used to build logical plan from ast.Node.
func BuildLogicalCauset(ctx context.Context, sctx stochastikctx.Context, node ast.Node, is schemareplicant.SchemaReplicant) (Causet, types.NameSlice, error) {
	sctx.GetStochastikVars().CausetID = 0
	sctx.GetStochastikVars().CausetDeferredCausetID = 0
	builder := NewCausetBuilder(sctx, is, &utilhint.BlockHintProcessor{})
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, err
	}
	return p, p.OutputNames(), err
}

// CheckPrivilege checks the privilege for a user.
func CheckPrivilege(activeRoles []*auth.RoleIdentity, pm privilege.Manager, vs []visitInfo) error {
	for _, v := range vs {
		if !pm.RequestVerification(activeRoles, v.EDB, v.causet, v.column, v.privilege) {
			if v.err == nil {
				return ErrPrivilegeCheckFail
			}
			return v.err
		}
	}
	return nil
}

// CheckBlockLock checks the causet dagger.
func CheckBlockLock(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant, vs []visitInfo) error {
	if !config.BlockLockEnabled() {
		return nil
	}
	checker := dagger.NewChecker(ctx, is)
	for i := range vs {
		err := checker.CheckBlockLock(vs[i].EDB, vs[i].causet, vs[i].privilege)
		if err != nil {
			return err
		}
	}
	return nil
}

// DoOptimize optimizes a logical plan to a physical plan.
func DoOptimize(ctx context.Context, sctx stochastikctx.Context, flag uint64, logic LogicalCauset) (PhysicalCauset, float64, error) {
	// if there is something after flagPrunDeferredCausets, do flagPrunDeferredCausetsAgain
	if flag&flagPrunDeferredCausets > 0 && flag-flagPrunDeferredCausets > flagPrunDeferredCausets {
		flag |= flagPrunDeferredCausetsAgain
	}
	logic, err := logicalOptimize(ctx, flag, logic)
	if err != nil {
		return nil, 0, err
	}
	if !AllowCartesianProduct.Load() && existsCartesianProduct(logic) {
		return nil, 0, errors.Trace(ErrCartesianProductUnsupported)
	}
	planCounter := CausetCounterTp(sctx.GetStochastikVars().StmtCtx.StmtHints.ForceNthCauset)
	if planCounter == 0 {
		planCounter = -1
	}
	physical, cost, err := physicalOptimize(logic, &planCounter)
	if err != nil {
		return nil, 0, err
	}
	finalCauset := postOptimize(sctx, physical)
	return finalCauset, cost, nil
}

func postOptimize(sctx stochastikctx.Context, plan PhysicalCauset) PhysicalCauset {
	plan = eliminatePhysicalProjection(plan)
	plan = injectExtraProjection(plan)
	plan = eliminateUnionScanAndLock(sctx, plan)
	plan = enableParallelApply(sctx, plan)
	return plan
}

func enableParallelApply(sctx stochastikctx.Context, plan PhysicalCauset) PhysicalCauset {
	if !sctx.GetStochastikVars().EnableParallelApply {
		return plan
	}
	// the parallel apply has three limitation:
	// 1. the parallel implementation now cannot keep order;
	// 2. the inner child has to support clone;
	// 3. if one Apply is in the inner side of another Apply, it cannot be parallel, for example:
	//		The topology of 3 Apply operators are A1(A2, A3), which means A2 is the outer child of A1
	//		while A3 is the inner child. Then A1 and A2 can be parallel and A3 cannot.
	if apply, ok := plan.(*PhysicalApply); ok {
		outerIdx := 1 - apply.InnerChildIdx
		noOrder := len(apply.GetChildReqProps(outerIdx).Items) == 0 // limitation 1
		_, err := SafeClone(apply.Children()[apply.InnerChildIdx])
		supportClone := err == nil // limitation 2
		if noOrder && supportClone {
			apply.Concurrency = sctx.GetStochastikVars().InterlockingDirectorateConcurrency
		}

		// because of the limitation 3, we cannot parallelize Apply operators in this Apply's inner size,
		// so we only invoke recursively for its outer child.
		apply.SetChild(outerIdx, enableParallelApply(sctx, apply.Children()[outerIdx]))
		return apply
	}
	for i, child := range plan.Children() {
		plan.SetChild(i, enableParallelApply(sctx, child))
	}
	return plan
}

func logicalOptimize(ctx context.Context, flag uint64, logic LogicalCauset) (LogicalCauset, error) {
	var err error
	for i, rule := range optMemruleList {
		// The order of flags is same as the order of optMemrule in the list.
		// We use a bitmask to record which opt rules should be used. If the i-th bit is 1, it means we should
		// apply i-th optimizing rule.
		if flag&(1<<uint(i)) == 0 || isLogicalMemruleDisabled(rule) {
			continue
		}
		logic, err = rule.optimize(ctx, logic)
		if err != nil {
			return nil, err
		}
	}
	return logic, err
}

func isLogicalMemruleDisabled(r logicalOptMemrule) bool {
	disabled := DefaultDisabledLogicalMemrulesList.Load().(set.StringSet).Exist(r.name())
	return disabled
}

func physicalOptimize(logic LogicalCauset, planCounter *CausetCounterTp) (PhysicalCauset, float64, error) {
	if _, err := logic.recursiveDeriveStats(nil); err != nil {
		return nil, 0, err
	}

	preparePossibleProperties(logic)

	prop := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	logic.SCtx().GetStochastikVars().StmtCtx.TaskMapBakTS = 0
	t, _, err := logic.findBestTask(prop, planCounter)
	if err != nil {
		return nil, 0, err
	}
	if *planCounter > 0 {
		logic.SCtx().GetStochastikVars().StmtCtx.AppendWarning(errors.Errorf("The parameter of nth_plan() is out of range."))
	}
	if t.invalid() {
		return nil, 0, ErrInternal.GenWithStackByArgs("Can't find a proper physical plan for this query")
	}

	err = t.plan().ResolveIndices()
	return t.plan(), t.cost(), err
}

// eliminateUnionScanAndLock set dagger property for PointGet and BatchPointGet and eliminates UnionScan and Lock.
func eliminateUnionScanAndLock(sctx stochastikctx.Context, p PhysicalCauset) PhysicalCauset {
	var pointGet *PointGetCauset
	var batchPointGet *BatchPointGetCauset
	var physLock *PhysicalLock
	var unionScan *PhysicalUnionScan
	iteratePhysicalCauset(p, func(p PhysicalCauset) bool {
		if len(p.Children()) > 1 {
			return false
		}
		switch x := p.(type) {
		case *PointGetCauset:
			pointGet = x
		case *BatchPointGetCauset:
			batchPointGet = x
		case *PhysicalLock:
			physLock = x
		case *PhysicalUnionScan:
			unionScan = x
		}
		return true
	})
	if pointGet == nil && batchPointGet == nil {
		return p
	}
	if physLock == nil && unionScan == nil {
		return p
	}
	if physLock != nil {
		dagger, waitTime := getLockWaitTime(sctx, physLock.Lock)
		if !dagger {
			return p
		}
		if pointGet != nil {
			pointGet.Lock = dagger
			pointGet.LockWaitTime = waitTime
		} else {
			batchPointGet.Lock = dagger
			batchPointGet.LockWaitTime = waitTime
		}
	}
	return transformPhysicalCauset(p, func(p PhysicalCauset) PhysicalCauset {
		if p == physLock {
			return p.Children()[0]
		}
		if p == unionScan {
			return p.Children()[0]
		}
		return p
	})
}

func iteratePhysicalCauset(p PhysicalCauset, f func(p PhysicalCauset) bool) {
	if !f(p) {
		return
	}
	for _, child := range p.Children() {
		iteratePhysicalCauset(child, f)
	}
}

func transformPhysicalCauset(p PhysicalCauset, f func(p PhysicalCauset) PhysicalCauset) PhysicalCauset {
	for i, child := range p.Children() {
		p.Children()[i] = transformPhysicalCauset(child, f)
	}
	return f(p)
}

func existsCartesianProduct(p LogicalCauset) bool {
	if join, ok := p.(*LogicalJoin); ok && len(join.EqualConditions) == 0 {
		return join.JoinType == InnerJoin || join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin
	}
	for _, child := range p.Children() {
		if existsCartesianProduct(child) {
			return true
		}
	}
	return false
}

// DefaultDisabledLogicalMemrulesList indicates the logical rules which should be banned.
var DefaultDisabledLogicalMemrulesList *atomic.Value

func init() {
	memex.EvalAstExpr = evalAstExpr
	memex.RewriteAstExpr = rewriteAstExpr
	DefaultDisabledLogicalMemrulesList = new(atomic.Value)
	DefaultDisabledLogicalMemrulesList.CausetStore(set.NewStringSet())
}
