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

package causet

import (
	"context"
	"math"
	"runtime/trace"
	"strings"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/bindinfo"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/causet/cascades"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

// GetPreparedStmt extract the prepared memex from the execute memex.
func GetPreparedStmt(stmt *ast.InterDircuteStmt, vars *variable.StochastikVars) (ast.StmtNode, error) {
	var ok bool
	execID := stmt.InterDircID
	if stmt.Name != "" {
		if execID, ok = vars.PreparedStmtNameToID[stmt.Name]; !ok {
			return nil, causetcore.ErrStmtNotFound
		}
	}
	if preparedPointer, ok := vars.PreparedStmts[execID]; ok {
		preparedObj, ok := preparedPointer.(*causetcore.CachedPrepareStmt)
		if !ok {
			return nil, errors.Errorf("invalid CachedPrepareStmt type")
		}
		return preparedObj.PreparedAst.Stmt, nil
	}
	return nil, causetcore.ErrStmtNotFound
}

// IsReadOnly check whether the ast.Node is a read only memex.
func IsReadOnly(node ast.Node, vars *variable.StochastikVars) bool {
	if execStmt, isInterDircStmt := node.(*ast.InterDircuteStmt); isInterDircStmt {
		s, err := GetPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Warn("GetPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(s)
	}
	return ast.IsReadOnly(node)
}

// Optimize does optimization and creates a Causet.
// The node must be prepared first.
func Optimize(ctx context.Context, sctx stochastikctx.Context, node ast.Node, is schemareplicant.SchemaReplicant) (causetcore.Causet, types.NameSlice, error) {
	sessVars := sctx.GetStochastikVars()

	// Because for write stmt, TiFlash has a different results when dagger the data in point get plan. We ban the TiFlash
	// engine in not read only stmt.
	if _, isolationReadContainTiFlash := sessVars.IsolationReadEngines[ekv.TiFlash]; isolationReadContainTiFlash && !IsReadOnly(node, sessVars) {
		delete(sessVars.IsolationReadEngines, ekv.TiFlash)
		defer func() {
			sessVars.IsolationReadEngines[ekv.TiFlash] = struct{}{}
		}()
	}

	if _, isolationReadContainEinsteinDB := sessVars.IsolationReadEngines[ekv.EinsteinDB]; isolationReadContainEinsteinDB {
		var fp causetcore.Causet
		if fpv, ok := sctx.Value(causetcore.PointCausetKey).(causetcore.PointCausetVal); ok {
			// point plan is already tried in a multi-memex query.
			fp = fpv.Causet
		} else {
			fp = causetcore.TryFastCauset(sctx, node)
		}
		if fp != nil {
			if !useMaxTS(sctx, fp) {
				sctx.PrepareTSFuture(ctx)
			}
			return fp, fp.OutputNames(), nil
		}
	}

	sctx.PrepareTSFuture(ctx)

	blockHints := hint.ExtractBlockHintsFromStmtNode(node, sctx)
	stmtHints, warns := handleStmtHints(blockHints)
	sessVars.StmtCtx.StmtHints = stmtHints
	for _, warn := range warns {
		sctx.GetStochastikVars().StmtCtx.AppendWarning(warn)
	}
	warns = warns[:0]
	bestCauset, names, _, err := optimize(ctx, sctx, node, is)
	if err != nil {
		return nil, nil, err
	}
	if !(sessVars.UseCausetBaselines || sessVars.EvolveCausetBaselines) {
		return bestCauset, names, nil
	}
	stmtNode, ok := node.(ast.StmtNode)
	if !ok {
		return bestCauset, names, nil
	}
	bindRecord, scope := getBindRecord(sctx, stmtNode)
	if bindRecord == nil {
		return bestCauset, names, nil
	}
	if sctx.GetStochastikVars().SelectLimit != math.MaxUint64 {
		sctx.GetStochastikVars().StmtCtx.AppendWarning(errors.New("sql_select_limit is set, so plan binding is not activated"))
		return bestCauset, names, nil
	}
	bestCausetHint := causetcore.GenHintsFromPhysicalCauset(bestCauset)
	if len(bindRecord.Bindings) > 0 {
		orgBinding := bindRecord.Bindings[0] // the first is the original binding
		for _, tbHint := range blockHints {  // consider causet hints which contained by the original binding
			if orgBinding.Hint.ContainBlockHint(tbHint.HintName.String()) {
				bestCausetHint = append(bestCausetHint, tbHint)
			}
		}
	}
	bestCausetHintStr := hint.RestoreOptimizerHints(bestCausetHint)

	defer func() {
		sessVars.StmtCtx.StmtHints = stmtHints
		for _, warn := range warns {
			sctx.GetStochastikVars().StmtCtx.AppendWarning(warn)
		}
	}()
	binding := bindRecord.FindBinding(bestCausetHintStr)
	// If the best bestCauset is in baselines, just use it.
	if binding != nil && binding.Status == bindinfo.Using {
		if sctx.GetStochastikVars().UseCausetBaselines {
			stmtHints, warns = handleStmtHints(binding.Hint.GetFirstBlockHints())
		}
		return bestCauset, names, nil
	}
	bestCostAmongHints := math.MaxFloat64
	var bestCausetAmongHints causetcore.Causet
	originHints := hint.DefCauslectHint(stmtNode)
	// Try to find the best binding.
	for _, binding := range bindRecord.Bindings {
		if binding.Status != bindinfo.Using {
			continue
		}
		metrics.BindUsageCounter.WithLabelValues(scope).Inc()
		hint.BindHint(stmtNode, binding.Hint)
		curStmtHints, curWarns := handleStmtHints(binding.Hint.GetFirstBlockHints())
		sctx.GetStochastikVars().StmtCtx.StmtHints = curStmtHints
		plan, _, cost, err := optimize(ctx, sctx, node, is)
		if err != nil {
			binding.Status = bindinfo.Invalid
			handleInvalidBindRecord(ctx, sctx, scope, bindinfo.BindRecord{
				OriginalALLEGROSQL: bindRecord.OriginalALLEGROSQL,
				EDB:          bindRecord.EDB,
				Bindings:    []bindinfo.Binding{binding},
			})
			continue
		}
		if cost < bestCostAmongHints {
			if sctx.GetStochastikVars().UseCausetBaselines {
				stmtHints, warns = curStmtHints, curWarns
			}
			bestCostAmongHints = cost
			bestCausetAmongHints = plan
		}
	}
	// 1. If there is already a evolution task, we do not need to handle it again.
	// 2. If the origin binding contain `read_from_storage` hint, we should ignore the evolve task.
	// 3. If the best plan contain TiFlash hint, we should ignore the evolve task.
	if sctx.GetStochastikVars().EvolveCausetBaselines && binding == nil &&
		!originHints.ContainBlockHint(causetcore.HintReadFromStorage) &&
		!bindRecord.Bindings[0].Hint.ContainBlockHint(causetcore.HintReadFromStorage) {
		handleEvolveTasks(ctx, sctx, bindRecord, stmtNode, bestCausetHintStr)
	}
	// Restore the hint to avoid changing the stmt node.
	hint.BindHint(stmtNode, originHints)
	if sctx.GetStochastikVars().UseCausetBaselines && bestCausetAmongHints != nil {
		return bestCausetAmongHints, names, nil
	}
	return bestCauset, names, nil
}

func optimize(ctx context.Context, sctx stochastikctx.Context, node ast.Node, is schemareplicant.SchemaReplicant) (causetcore.Causet, types.NameSlice, float64, error) {
	// build logical plan
	sctx.GetStochastikVars().CausetID = 0
	sctx.GetStochastikVars().CausetDeferredCausetID = 0
	hintProcessor := &hint.BlockHintProcessor{Ctx: sctx}
	node.Accept(hintProcessor)
	builder := causetcore.NewCausetBuilder(sctx, is, hintProcessor)

	// reset fields about rewrite
	sctx.GetStochastikVars().RewritePhaseInfo.Reset()
	beginRewrite := time.Now()
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, 0, err
	}
	sctx.GetStochastikVars().RewritePhaseInfo.DurationRewrite = time.Since(beginRewrite)

	sctx.GetStochastikVars().StmtCtx.Blocks = builder.GetDBBlockInfo()
	activeRoles := sctx.GetStochastikVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the causet information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := causetcore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, nil, 0, err
		}
	}

	if err := causetcore.CheckBlockLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, 0, err
	}

	// Handle the execute memex.
	if execCauset, ok := p.(*causetcore.InterDircute); ok {
		err := execCauset.OptimizePreparedCauset(ctx, sctx, is)
		return p, p.OutputNames(), 0, err
	}

	names := p.OutputNames()

	// Handle the non-logical plan memex.
	logic, isLogicalCauset := p.(causetcore.LogicalCauset)
	if !isLogicalCauset {
		return p, names, 0, nil
	}

	// Handle the logical plan memex, use cascades causet if enabled.
	if sctx.GetStochastikVars().GetEnableCascadesCausetAppend() {
		finalCauset, cost, err := cascades.DefaultOptimizer.FindBestCauset(sctx, logic)
		return finalCauset, names, cost, err
	}

	beginOpt := time.Now()
	finalCauset, cost, err := causetcore.DoOptimize(ctx, sctx, builder.GetOptFlag(), logic)
	sctx.GetStochastikVars().DurationOptimization = time.Since(beginOpt)
	return finalCauset, names, cost, err
}

func extractSelectAndNormalizeDigest(stmtNode ast.StmtNode) (*ast.SelectStmt, string, string) {
	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		switch x.Stmt.(type) {
		case *ast.SelectStmt:
			causetcore.EraseLastSemicolon(x)
			normalizeExplainALLEGROSQL := BerolinaSQL.Normalize(x.Text())
			idx := strings.Index(normalizeExplainALLEGROSQL, "select")
			normalizeALLEGROSQL := normalizeExplainALLEGROSQL[idx:]
			hash := BerolinaSQL.DigestNormalized(normalizeALLEGROSQL)
			return x.Stmt.(*ast.SelectStmt), normalizeALLEGROSQL, hash
		}
	case *ast.SelectStmt:
		causetcore.EraseLastSemicolon(x)
		normalizedALLEGROSQL, hash := BerolinaSQL.NormalizeDigest(x.Text())
		return x, normalizedALLEGROSQL, hash
	}
	return nil, "", ""
}

func getBindRecord(ctx stochastikctx.Context, stmt ast.StmtNode) (*bindinfo.BindRecord, string) {
	// When the petri is initializing, the bind will be nil.
	if ctx.Value(bindinfo.StochastikBindInfoKeyType) == nil {
		return nil, ""
	}
	selectStmt, normalizedALLEGROSQL, hash := extractSelectAndNormalizeDigest(stmt)
	if selectStmt == nil {
		return nil, ""
	}
	stochastikHandle := ctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
	bindRecord := stochastikHandle.GetBindRecord(normalizedALLEGROSQL, ctx.GetStochastikVars().CurrentDB)
	if bindRecord == nil {
		bindRecord = stochastikHandle.GetBindRecord(normalizedALLEGROSQL, "")
	}
	if bindRecord != nil {
		if bindRecord.HasUsingBinding() {
			return bindRecord, metrics.ScopeStochastik
		}
		return nil, ""
	}
	globalHandle := petri.GetPetri(ctx).BindHandle()
	if globalHandle == nil {
		return nil, ""
	}
	bindRecord = globalHandle.GetBindRecord(hash, normalizedALLEGROSQL, ctx.GetStochastikVars().CurrentDB)
	if bindRecord == nil {
		bindRecord = globalHandle.GetBindRecord(hash, normalizedALLEGROSQL, "")
	}
	return bindRecord, metrics.ScopeGlobal
}

func handleInvalidBindRecord(ctx context.Context, sctx stochastikctx.Context, level string, bindRecord bindinfo.BindRecord) {
	stochastikHandle := sctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
	err := stochastikHandle.DropBindRecord(bindRecord.OriginalALLEGROSQL, bindRecord.EDB, &bindRecord.Bindings[0])
	if err != nil {
		logutil.Logger(ctx).Info("drop stochastik bindings failed")
	}
	if level == metrics.ScopeStochastik {
		return
	}

	globalHandle := petri.GetPetri(sctx).BindHandle()
	globalHandle.AddDropInvalidBindTask(&bindRecord)
}

func handleEvolveTasks(ctx context.Context, sctx stochastikctx.Context, br *bindinfo.BindRecord, stmtNode ast.StmtNode, planHint string) {
	bindALLEGROSQL := bindinfo.GenerateBindALLEGROSQL(ctx, stmtNode, planHint)
	if bindALLEGROSQL == "" {
		return
	}
	charset, collation := sctx.GetStochastikVars().GetCharsetInfo()
	binding := bindinfo.Binding{
		BindALLEGROSQL:   bindALLEGROSQL,
		Status:    bindinfo.PendingVerify,
		Charset:   charset,
		DefCauslation: collation,
		Source:    bindinfo.Evolve,
	}
	globalHandle := petri.GetPetri(sctx).BindHandle()
	globalHandle.AddEvolveCausetTask(br.OriginalALLEGROSQL, br.EDB, binding)
}

// useMaxTS returns true when meets following conditions:
//  1. ctx is auto commit tagged.
//  2. plan is point get by pk.
func useMaxTS(ctx stochastikctx.Context, p causetcore.Causet) bool {
	if !causetcore.IsAutoCommitTxn(ctx) {
		return false
	}

	v, ok := p.(*causetcore.PointGetCauset)
	return ok && (v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle))
}

// OptimizeInterDircStmt to optimize prepare memex protocol "execute" memex
// this is a short path ONLY does things filling prepare related params
// for point select like plan which does not need extra things
func OptimizeInterDircStmt(ctx context.Context, sctx stochastikctx.Context,
	execAst *ast.InterDircuteStmt, is schemareplicant.SchemaReplicant) (causetcore.Causet, error) {
	defer trace.StartRegion(ctx, "Optimize").End()
	var err error
	builder := causetcore.NewCausetBuilder(sctx, is, nil)
	p, err := builder.Build(ctx, execAst)
	if err != nil {
		return nil, err
	}
	if execCauset, ok := p.(*causetcore.InterDircute); ok {
		err = execCauset.OptimizePreparedCauset(ctx, sctx, is)
		return execCauset.Causet, err
	}
	err = errors.Errorf("invalid result plan type, should be InterDircute")
	return nil, err
}

func handleStmtHints(hints []*ast.BlockOptimizerHint) (stmtHints stmtctx.StmtHints, warns []error) {
	if len(hints) == 0 {
		return
	}
	var memoryQuotaHint, useToJAHint, useCascadesHint, maxInterDircutionTime, forceNthCauset *ast.BlockOptimizerHint
	var memoryQuotaHintCnt, useToJAHintCnt, useCascadesHintCnt, noIndexMergeHintCnt, readReplicaHintCnt, maxInterDircutionTimeCnt, forceNthCausetCnt int
	for _, hint := range hints {
		switch hint.HintName.L {
		case "memory_quota":
			memoryQuotaHint = hint
			memoryQuotaHintCnt++
		case "use_toja":
			useToJAHint = hint
			useToJAHintCnt++
		case "use_cascades":
			useCascadesHint = hint
			useCascadesHintCnt++
		case "no_index_merge":
			noIndexMergeHintCnt++
		case "read_consistent_replica":
			readReplicaHintCnt++
		case "max_execution_time":
			maxInterDircutionTimeCnt++
			maxInterDircutionTime = hint
		case "nth_plan":
			forceNthCausetCnt++
			forceNthCauset = hint
		}
	}
	// Handle MEMORY_QUOTA
	if memoryQuotaHintCnt != 0 {
		if memoryQuotaHintCnt > 1 {
			warn := errors.Errorf("MEMORY_QUOTA() s defined more than once, only the last definition takes effect: MEMORY_QUOTA(%v)", memoryQuotaHint.HintData.(int64))
			warns = append(warns, warn)
		}
		// InterlockingDirectorate use MemoryQuota <= 0 to indicate no memory limit, here use < 0 to handle hint syntax error.
		if memoryQuota := memoryQuotaHint.HintData.(int64); memoryQuota < 0 {
			warn := errors.New("The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)")
			warns = append(warns, warn)
		} else {
			stmtHints.HasMemQuotaHint = true
			stmtHints.MemQuotaQuery = memoryQuota
			if memoryQuota == 0 {
				warn := errors.New("Setting the MEMORY_QUOTA to 0 means no memory limit")
				warns = append(warns, warn)
			}
		}
	}
	// Handle USE_TOJA
	if useToJAHintCnt != 0 {
		if useToJAHintCnt > 1 {
			warn := errors.Errorf("USE_TOJA() is defined more than once, only the last definition takes effect: USE_TOJA(%v)", useToJAHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasAllowInSubqToJoinAnPosetDaggHint = true
		stmtHints.AllowInSubqToJoinAnPosetDagg = useToJAHint.HintData.(bool)
	}
	// Handle USE_CASCADES
	if useCascadesHintCnt != 0 {
		if useCascadesHintCnt > 1 {
			warn := errors.Errorf("USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(%v)", useCascadesHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasEnableCascadesCausetAppendHint = true
		stmtHints.EnableCascadesCausetAppend = useCascadesHint.HintData.(bool)
	}
	// Handle NO_INDEX_MERGE
	if noIndexMergeHintCnt != 0 {
		if noIndexMergeHintCnt > 1 {
			warn := errors.New("NO_INDEX_MERGE() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.NoIndexMergeHint = true
	}
	// Handle READ_CONSISTENT_REPLICA
	if readReplicaHintCnt != 0 {
		if readReplicaHintCnt > 1 {
			warn := errors.New("READ_CONSISTENT_REPLICA() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.HasReplicaReadHint = true
		stmtHints.ReplicaRead = byte(ekv.ReplicaReadFollower)
	}
	// Handle MAX_EXECUTION_TIME
	if maxInterDircutionTimeCnt != 0 {
		if maxInterDircutionTimeCnt > 1 {
			warn := errors.Errorf("MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(%v)", maxInterDircutionTime.HintData.(uint64))
			warns = append(warns, warn)
		}
		stmtHints.HasMaxInterDircutionTime = true
		stmtHints.MaxInterDircutionTime = maxInterDircutionTime.HintData.(uint64)
	}
	// Handle NTH_PLAN
	if forceNthCausetCnt != 0 {
		if forceNthCausetCnt > 1 {
			warn := errors.Errorf("NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN(%v)", forceNthCauset.HintData.(int64))
			warns = append(warns, warn)
		}
		stmtHints.ForceNthCauset = forceNthCauset.HintData.(int64)
		if stmtHints.ForceNthCauset < 1 {
			stmtHints.ForceNthCauset = -1
			warn := errors.Errorf("the hintdata for NTH_PLAN() is too small, hint ignored.")
			warns = append(warns, warn)
		}
	} else {
		stmtHints.ForceNthCauset = -1
	}
	return
}

func init() {
	causetcore.OptimizeAstNode = Optimize
}
