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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/ekvcache"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/texttree"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/BerolinaSQL_driver"
	"go.uber.org/zap"
)

var planCacheCounter = metrics.CausetCacheCounter.WithLabelValues("prepare")

// ShowDBS is for showing DBS information.
type ShowDBS struct {
	baseSchemaProducer
}

// ShowSlow is for showing slow queries.
type ShowSlow struct {
	baseSchemaProducer

	*ast.ShowSlow
}

// ShowDBSJobQueries is for showing DBS job queries allegrosql.
type ShowDBSJobQueries struct {
	baseSchemaProducer

	JobIDs []int64
}

// ShowNextRowID is for showing the next global event ID.
type ShowNextRowID struct {
	baseSchemaProducer
	BlockName *ast.BlockName
}

// CheckBlock is used for checking causet data, built from the 'admin check causet' memex.
type CheckBlock struct {
	baseSchemaProducer

	DBName             string
	Block              causet.Block
	IndexInfos         []*perceptron.IndexInfo
	IndexLookUpReaders []*PhysicalIndexLookUpReader
	ChecHoTTex         bool
}

// RecoverIndex is used for backfilling corrupted index data.
type RecoverIndex struct {
	baseSchemaProducer

	Block     *ast.BlockName
	IndexName string
}

// CleanupIndex is used to delete dangling index data.
type CleanupIndex struct {
	baseSchemaProducer

	Block     *ast.BlockName
	IndexName string
}

// ChecHoTTexRange is used for checking index data, output the index values that handle within begin and end.
type ChecHoTTexRange struct {
	baseSchemaProducer

	Block     *ast.BlockName
	IndexName string

	HandleRanges []ast.HandleRange
}

// ChecksumBlock is used for calculating causet checksum, built from the `admin checksum causet` memex.
type ChecksumBlock struct {
	baseSchemaProducer

	Blocks []*ast.BlockName
}

// CancelDBSJobs represents a cancel DBS jobs plan.
type CancelDBSJobs struct {
	baseSchemaProducer

	JobIDs []int64
}

// ReloadExprPushdownBlacklist reloads the data from expr_pushdown_blacklist causet.
type ReloadExprPushdownBlacklist struct {
	baseSchemaProducer
}

// ReloadOptMemruleBlacklist reloads the data from opt_rule_blacklist causet.
type ReloadOptMemruleBlacklist struct {
	baseSchemaProducer
}

// AdminPluginsCausetAction indicate action will be taken on plugins.
type AdminPluginsCausetAction int

const (
	// Enable indicates enable plugins.
	Enable AdminPluginsCausetAction = iota + 1
	// Disable indicates disable plugins.
	Disable
)

// AdminPlugins administrates milevadb plugins.
type AdminPlugins struct {
	baseSchemaProducer
	CausetAction AdminPluginsCausetAction
	Plugins      []string
}

// AdminShowTelemetry displays telemetry status including tracking ID, status and so on.
type AdminShowTelemetry struct {
	baseSchemaProducer
}

// AdminResetTelemetryID regenerates a new telemetry tracking ID.
type AdminResetTelemetryID struct {
	baseSchemaProducer
}

// Change represents a change plan.
type Change struct {
	baseSchemaProducer
	*ast.ChangeStmt
}

// Prepare represents prepare plan.
type Prepare struct {
	baseSchemaProducer

	Name           string
	ALLEGROSQLText string
}

// InterDircute represents prepare plan.
type InterDircute struct {
	baseSchemaProducer

	Name          string
	UsingVars     []memex.Expression
	PrepareParams []types.Causet
	InterDircID   uint32
	Stmt          ast.StmtNode
	StmtType      string
	Causet        Causet
}

// OptimizePreparedCauset optimizes the prepared memex.
func (e *InterDircute) OptimizePreparedCauset(ctx context.Context, sctx stochastikctx.Context, is schemareplicant.SchemaReplicant) error {
	vars := sctx.GetStochastikVars()
	if e.Name != "" {
		e.InterDircID = vars.PreparedStmtNameToID[e.Name]
	}
	preparedPointer, ok := vars.PreparedStmts[e.InterDircID]
	if !ok {
		return errors.Trace(ErrStmtNotFound)
	}
	preparedObj, ok := preparedPointer.(*CachedPrepareStmt)
	if !ok {
		return errors.Errorf("invalid CachedPrepareStmt type")
	}
	prepared := preparedObj.PreparedAst
	vars.StmtCtx.StmtType = prepared.StmtType

	paramLen := len(e.PrepareParams)
	if paramLen > 0 {
		// for binary protocol execute, argument is placed in vars.PrepareParams
		if len(prepared.Params) != paramLen {
			return errors.Trace(ErrWrongParamCount)
		}
		vars.PreparedParams = e.PrepareParams
		for i, val := range vars.PreparedParams {
			param := prepared.Params[i].(*driver.ParamMarkerExpr)
			param.Causet = val
			param.InInterDircute = true
		}
	} else {
		// for `execute stmt using @a, @b, @c`, using value in e.UsingVars
		if len(prepared.Params) != len(e.UsingVars) {
			return errors.Trace(ErrWrongParamCount)
		}

		for i, usingVar := range e.UsingVars {
			val, err := usingVar.Eval(chunk.Row{})
			if err != nil {
				return err
			}
			param := prepared.Params[i].(*driver.ParamMarkerExpr)
			param.Causet = val
			param.InInterDircute = true
			vars.PreparedParams = append(vars.PreparedParams, val)
		}
	}

	if prepared.SchemaVersion != is.SchemaMetaVersion() {
		// In order to avoid some correctness issues, we have to clear the
		// cached plan once the schemaReplicant version is changed.
		// Cached plan in prepared struct does NOT have a "cache key" with
		// schemaReplicant version like prepared plan cache key
		prepared.CachedCauset = nil
		preparedObj.InterlockingDirectorate = nil
		// If the schemaReplicant version has changed we need to preprocess it again,
		// if this time it failed, the real reason for the error is schemaReplicant changed.
		err := Preprocess(sctx, prepared.Stmt, is, InPrepare)
		if err != nil {
			return ErrSchemaChanged.GenWithStack("Schema change caused error: %s", err.Error())
		}
		prepared.SchemaVersion = is.SchemaMetaVersion()
	}
	err := e.getPhysicalCauset(ctx, sctx, is, preparedObj)
	if err != nil {
		return err
	}
	e.Stmt = prepared.Stmt
	return nil
}

func (e *InterDircute) checkPreparedPriv(ctx context.Context, sctx stochastikctx.Context,
	preparedObj *CachedPrepareStmt, is schemareplicant.SchemaReplicant) error {
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := CheckPrivilege(sctx.GetStochastikVars().ActiveRoles, pm, preparedObj.VisitInfos); err != nil {
			return err
		}
	}
	err := CheckBlockLock(sctx, is, preparedObj.VisitInfos)
	return err
}

func (e *InterDircute) setFoundInCausetCache(sctx stochastikctx.Context, opt bool) error {
	vars := sctx.GetStochastikVars()
	err := vars.SetSystemVar(variable.MilevaDBFoundInCausetCache, variable.BoolToIntStr(opt))
	return err
}

func (e *InterDircute) getPhysicalCauset(ctx context.Context, sctx stochastikctx.Context, is schemareplicant.SchemaReplicant, preparedStmt *CachedPrepareStmt) error {
	stmtCtx := sctx.GetStochastikVars().StmtCtx
	prepared := preparedStmt.PreparedAst
	stmtCtx.UseCache = prepared.UseCache
	var cacheKey ekvcache.Key
	if prepared.UseCache {
		cacheKey = NewPSTMTCausetCacheKey(sctx.GetStochastikVars(), e.InterDircID, prepared.SchemaVersion)
	}
	if prepared.CachedCauset != nil {
		// Rewriting the memex in the select.where condition  will convert its
		// type from "paramMarker" to "Constant".When Point Select queries are executed,
		// the memex in the where condition will not be evaluated,
		// so you don't need to consider whether prepared.useCache is enabled.
		plan := prepared.CachedCauset.(Causet)
		names := prepared.CachedNames.(types.NameSlice)
		err := e.rebuildRange(plan)
		if err != nil {
			logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
			goto REBUILD
		}
		if metrics.ResetblockCausetCacheCounterFortTest {
			metrics.CausetCacheCounter.WithLabelValues("prepare").Inc()
		} else {
			planCacheCounter.Inc()
		}
		err = e.setFoundInCausetCache(sctx, true)
		if err != nil {
			return err
		}
		e.names = names
		e.Causet = plan
		stmtCtx.PointInterDirc = true
		return nil
	}
	if prepared.UseCache {
		if cacheValue, exists := sctx.PreparedCausetCache().Get(cacheKey); exists {
			if err := e.checkPreparedPriv(ctx, sctx, preparedStmt, is); err != nil {
				return err
			}
			cachedVal := cacheValue.(*PSTMTCausetCacheValue)
			planValid := true
			for tblInfo, unionScan := range cachedVal.TblInfo2UnionScan {
				if !unionScan && blockHasDirtyContent(sctx, tblInfo) {
					planValid = false
					// TODO we can inject UnionScan into cached plan to avoid invalidating it, though
					// rebuilding the filters in UnionScan is pretty trivial.
					sctx.PreparedCausetCache().Delete(cacheKey)
					break
				}
			}
			if planValid {
				err := e.rebuildRange(cachedVal.Causet)
				if err != nil {
					logutil.BgLogger().Debug("rebuild range failed", zap.Error(err))
					goto REBUILD
				}
				err = e.setFoundInCausetCache(sctx, true)
				if err != nil {
					return err
				}
				if metrics.ResetblockCausetCacheCounterFortTest {
					metrics.CausetCacheCounter.WithLabelValues("prepare").Inc()
				} else {
					planCacheCounter.Inc()
				}
				e.names = cachedVal.OutPutNames
				e.Causet = cachedVal.Causet
				stmtCtx.SetCausetDigest(preparedStmt.NormalizedCauset, preparedStmt.CausetDigest)
				return nil
			}
		}
	}

REBUILD:
	stmt := TryAddExtraLimit(sctx, prepared.Stmt)
	p, names, err := OptimizeAstNode(ctx, sctx, stmt, is)
	if err != nil {
		return err
	}
	err = e.tryCachePointCauset(ctx, sctx, preparedStmt, is, p)
	if err != nil {
		return err
	}
	e.names = names
	e.Causet = p
	_, isBlockDual := p.(*PhysicalBlockDual)
	if !isBlockDual && prepared.UseCache {
		cached := NewPSTMTCausetCacheValue(p, names, stmtCtx.TblInfo2UnionScan)
		preparedStmt.NormalizedCauset, preparedStmt.CausetDigest = NormalizeCauset(p)
		stmtCtx.SetCausetDigest(preparedStmt.NormalizedCauset, preparedStmt.CausetDigest)
		sctx.PreparedCausetCache().Put(cacheKey, cached)
	}
	err = e.setFoundInCausetCache(sctx, false)
	return err
}

// tryCachePointCauset will try to cache point execution plan, there may be some
// short paths for these executions, currently "point select" and "point uFIDelate"
func (e *InterDircute) tryCachePointCauset(ctx context.Context, sctx stochastikctx.Context,
	preparedStmt *CachedPrepareStmt, is schemareplicant.SchemaReplicant, p Causet) error {
	var (
		prepared = preparedStmt.PreparedAst
		ok       bool
		err      error
		names    types.NameSlice
	)
	switch p.(type) {
	case *PointGetCauset:
		ok, err = IsPointGetWithPKOrUniqueKeyByAutoCommit(sctx, p)
		names = p.OutputNames()
		if err != nil {
			return err
		}
	case *UFIDelate:
		ok, err = IsPointUFIDelateByAutoCommit(sctx, p)
		if err != nil {
			return err
		}
		if ok {
			// make constant memex causetstore paramMarker
			sctx.GetStochastikVars().StmtCtx.PointInterDirc = true
			p, names, err = OptimizeAstNode(ctx, sctx, prepared.Stmt, is)
		}
	}
	if ok {
		// just cache point plan now
		prepared.CachedCauset = p
		prepared.CachedNames = names
		preparedStmt.NormalizedCauset, preparedStmt.CausetDigest = NormalizeCauset(p)
		sctx.GetStochastikVars().StmtCtx.SetCausetDigest(preparedStmt.NormalizedCauset, preparedStmt.CausetDigest)
	}
	return err
}

func (e *InterDircute) rebuildRange(p Causet) error {
	sctx := p.SCtx()
	sc := p.SCtx().GetStochastikVars().StmtCtx
	var err error
	switch x := p.(type) {
	case *PhysicalBlockReader:
		ts := x.BlockCausets[0].(*PhysicalBlockScan)
		var pkDefCaus *memex.DeferredCauset
		if ts.Block.IsCommonHandle {
			pk := blocks.FindPrimaryIndex(ts.Block)
			pkDefCauss := make([]*memex.DeferredCauset, 0, len(pk.DeferredCausets))
			pkDefCaussLen := make([]int, 0, len(pk.DeferredCausets))
			for _, colInfo := range pk.DeferredCausets {
				pkDefCauss = append(pkDefCauss, memex.DefCausInfo2DefCaus(ts.schemaReplicant.DeferredCausets, ts.Block.DeferredCausets[colInfo.Offset]))
				pkDefCaussLen = append(pkDefCaussLen, colInfo.Length)
			}
			res, err := ranger.DetachCondAndBuildRangeForIndex(p.SCtx(), ts.AccessCondition, pkDefCauss, pkDefCaussLen)
			if err != nil {
				return err
			}
			ts.Ranges = res.Ranges
		} else {
			if ts.Block.PKIsHandle {
				if pkDefCausInfo := ts.Block.GetPkDefCausInfo(); pkDefCausInfo != nil {
					pkDefCaus = memex.DefCausInfo2DefCaus(ts.schemaReplicant.DeferredCausets, pkDefCausInfo)
				}
			}
			if pkDefCaus != nil {
				ts.Ranges, err = ranger.BuildBlockRange(ts.AccessCondition, sc, pkDefCaus.RetType)
				if err != nil {
					return err
				}
			} else {
				ts.Ranges = ranger.FullIntRange(false)
			}
		}
	case *PhysicalIndexReader:
		is := x.IndexCausets[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
	case *PhysicalIndexLookUpReader:
		is := x.IndexCausets[0].(*PhysicalIndexScan)
		is.Ranges, err = e.buildRangeForIndexScan(sctx, is)
		if err != nil {
			return err
		}
	case *PointGetCauset:
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxDefCauss, x.IdxDefCausLens)
				if err != nil {
					return err
				}
				for i := range x.IndexValues {
					x.IndexValues[i] = ranges.Ranges[0].LowVal[i]
				}
			} else {
				var pkDefCaus *memex.DeferredCauset
				if x.TblInfo.PKIsHandle {
					if pkDefCausInfo := x.TblInfo.GetPkDefCausInfo(); pkDefCausInfo != nil {
						pkDefCaus = memex.DefCausInfo2DefCaus(x.schemaReplicant.DeferredCausets, pkDefCausInfo)
					}
				}
				if pkDefCaus != nil {
					ranges, err := ranger.BuildBlockRange(x.AccessConditions, x.ctx.GetStochastikVars().StmtCtx, pkDefCaus.RetType)
					if err != nil {
						return err
					}
					x.Handle = ekv.IntHandle(ranges[0].LowVal[0].GetInt64())
				}
			}
		}
		// The code should never run here as long as we're not using point get for partition causet.
		// And if we change the logic one day, here work as defensive programming to cache the error.
		if x.PartitionInfo != nil {
			return errors.New("point get for partition causet can not use plan cache")
		}
		if x.HandleParam != nil {
			var iv int64
			iv, err = x.HandleParam.Causet.ToInt64(sc)
			if err != nil {
				return err
			}
			x.Handle = ekv.IntHandle(iv)
			return nil
		}
		for i, param := range x.IndexValueParams {
			if param != nil {
				x.IndexValues[i] = param.Causet
			}
		}
		return nil
	case *BatchPointGetCauset:
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx, x.AccessConditions, x.IdxDefCauss, x.IdxDefCausLens)
				if err != nil {
					return err
				}
				for i := range x.IndexValues {
					for j := range ranges.Ranges[i].LowVal {
						x.IndexValues[i][j] = ranges.Ranges[i].LowVal[j]
					}
				}
			} else {
				var pkDefCaus *memex.DeferredCauset
				if x.TblInfo.PKIsHandle {
					if pkDefCausInfo := x.TblInfo.GetPkDefCausInfo(); pkDefCausInfo != nil {
						pkDefCaus = memex.DefCausInfo2DefCaus(x.schemaReplicant.DeferredCausets, pkDefCausInfo)
					}
				}
				if pkDefCaus != nil {
					ranges, err := ranger.BuildBlockRange(x.AccessConditions, x.ctx.GetStochastikVars().StmtCtx, pkDefCaus.RetType)
					if err != nil {
						return err
					}
					for i := range ranges {
						x.Handles[i] = ekv.IntHandle(ranges[i].LowVal[0].GetInt64())
					}
				}
			}
		}
		for i, param := range x.HandleParams {
			if param != nil {
				var iv int64
				iv, err = param.Causet.ToInt64(sc)
				if err != nil {
					return err
				}
				x.Handles[i] = ekv.IntHandle(iv)
			}
		}
		for i, params := range x.IndexValueParams {
			if len(params) < 1 {
				continue
			}
			for j, param := range params {
				if param != nil {
					x.IndexValues[i][j] = param.Causet
				}
			}
		}
	case PhysicalCauset:
		for _, child := range x.Children() {
			err = e.rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *Insert:
		if x.SelectCauset != nil {
			return e.rebuildRange(x.SelectCauset)
		}
	case *UFIDelate:
		if x.SelectCauset != nil {
			return e.rebuildRange(x.SelectCauset)
		}
	case *Delete:
		if x.SelectCauset != nil {
			return e.rebuildRange(x.SelectCauset)
		}
	}
	return nil
}

func (e *InterDircute) buildRangeForIndexScan(sctx stochastikctx.Context, is *PhysicalIndexScan) ([]*ranger.Range, error) {
	if len(is.IdxDefCauss) == 0 {
		return ranger.FullRange(), nil
	}
	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, is.AccessCondition, is.IdxDefCauss, is.IdxDefCausLens)
	if err != nil {
		return nil, err
	}
	return res.Ranges, nil
}

// Deallocate represents deallocate plan.
type Deallocate struct {
	baseSchemaProducer

	Name string
}

// Set represents a plan for set stmt.
type Set struct {
	baseSchemaProducer

	VarAssigns []*memex.VarAssignment
}

// SetConfig represents a plan for set config stmt.
type SetConfig struct {
	baseSchemaProducer

	Type     string
	Instance string
	Name     string
	Value    memex.Expression
}

// ALLEGROSQLBindOpType repreents the ALLEGROALLEGROSQL bind type
type ALLEGROSQLBindOpType int

const (
	// OpALLEGROSQLBindCreate represents the operation to create a ALLEGROALLEGROSQL bind.
	OpALLEGROSQLBindCreate ALLEGROSQLBindOpType = iota
	// OpALLEGROSQLBindDrop represents the operation to drop a ALLEGROALLEGROSQL bind.
	OpALLEGROSQLBindDrop
	// OpFlushBindings is used to flush plan bindings.
	OpFlushBindings
	// OpCaptureBindings is used to capture plan bindings.
	OpCaptureBindings
	// OpEvolveBindings is used to evolve plan binding.
	OpEvolveBindings
	// OpReloadBindings is used to reload plan binding.
	OpReloadBindings
)

// ALLEGROSQLBindCauset represents a plan for ALLEGROALLEGROSQL bind.
type ALLEGROSQLBindCauset struct {
	baseSchemaProducer

	ALLEGROSQLBindOp    ALLEGROSQLBindOpType
	NormdOrigALLEGROSQL string
	BindALLEGROSQL      string
	IsGlobal            bool
	BindStmt            ast.StmtNode
	EDB                 string
	Charset             string
	DefCauslation       string
}

// Simple represents a simple memex plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode
}

// InsertGeneratedDeferredCausets is for completing generated columns in Insert.
// We resolve generation memexs in plan, and eval those in interlock.
type InsertGeneratedDeferredCausets struct {
	DeferredCausets []*ast.DeferredCausetName
	Exprs           []memex.Expression
	OnDuplicates    []*memex.Assignment
}

// Insert represents an insert plan.
type Insert struct {
	baseSchemaProducer

	Block             causet.Block
	blockSchema       *memex.Schema
	blockDefCausNames types.NameSlice
	DeferredCausets   []*ast.DeferredCausetName
	Lists             [][]memex.Expression
	SetList           []*memex.Assignment

	OnDuplicate        []*memex.Assignment
	Schema4OnDuplicate *memex.Schema
	names4OnDuplicate  types.NameSlice

	GenDefCauss InsertGeneratedDeferredCausets

	SelectCauset PhysicalCauset

	IsReplace bool

	// NeedFillDefaultValue is true when expr in value list reference other column.
	NeedFillDefaultValue bool

	AllAssignmentsAreConstant bool
}

// UFIDelate represents UFIDelate plan.
type UFIDelate struct {
	baseSchemaProducer

	OrderedList []*memex.Assignment

	AllAssignmentsAreConstant bool

	SelectCauset PhysicalCauset

	TblDefCausPosInfos TblDefCausPosInfoSlice

	// Used when partition sets are given.
	// e.g. uFIDelate t partition(p0) set a = 1;
	PartitionedBlock []causet.PartitionedBlock
}

// Delete represents a delete plan.
type Delete struct {
	baseSchemaProducer

	IsMultiBlock bool

	SelectCauset PhysicalCauset

	TblDefCausPosInfos TblDefCausPosInfoSlice
}

// AnalyzeBlockID is hybrid causet id used to analyze causet.
type AnalyzeBlockID struct {
	PersistID      int64
	DefCauslectIDs []int64
}

// StoreAsDefCauslectID indicates whether collect causet id is same as persist causet id.
// for new partition implementation is TRUE but FALSE for old partition implementation
func (h *AnalyzeBlockID) StoreAsDefCauslectID() bool {
	return h.PersistID == h.DefCauslectIDs[0]
}

func (h *AnalyzeBlockID) String() string {
	return fmt.Sprintf("%d => %v", h.DefCauslectIDs, h.PersistID)
}

// Equals indicates whether two causet id is equal.
func (h *AnalyzeBlockID) Equals(t *AnalyzeBlockID) bool {
	if h == t {
		return true
	}
	if h == nil || t == nil {
		return false
	}
	if h.PersistID != t.PersistID {
		return false
	}
	if len(h.DefCauslectIDs) != len(t.DefCauslectIDs) {
		return false
	}
	if len(h.DefCauslectIDs) == 1 {
		return h.DefCauslectIDs[0] == t.DefCauslectIDs[0]
	}
	for _, hp := range h.DefCauslectIDs {
		var matchOne bool
		for _, tp := range t.DefCauslectIDs {
			if tp == hp {
				matchOne = true
				break
			}
		}
		if !matchOne {
			return false
		}
	}
	return true
}

// analyzeInfo is used to causetstore the database name, causet name and partition name of analyze task.
type analyzeInfo struct {
	DBName        string
	BlockName     string
	PartitionName string
	BlockID       AnalyzeBlockID
	Incremental   bool
}

// AnalyzeDeferredCausetsTask is used for analyze columns.
type AnalyzeDeferredCausetsTask struct {
	HandleDefCauss HandleDefCauss
	DefCaussInfo   []*perceptron.DeferredCausetInfo
	TblInfo        *perceptron.BlockInfo
	analyzeInfo
}

// AnalyzeIndexTask is used for analyze index.
type AnalyzeIndexTask struct {
	IndexInfo *perceptron.IndexInfo
	TblInfo   *perceptron.BlockInfo
	analyzeInfo
}

// Analyze represents an analyze plan
type Analyze struct {
	baseSchemaProducer

	DefCausTasks []AnalyzeDeferredCausetsTask
	IdxTasks     []AnalyzeIndexTask
	Opts         map[ast.AnalyzeOptionType]uint64
}

// LoadData represents a loaddata plan.
type LoadData struct {
	baseSchemaProducer

	IsLocal         bool
	OnDuplicate     ast.OnDuplicateKeyHandlingType
	Path            string
	Block           *ast.BlockName
	DeferredCausets []*ast.DeferredCausetName
	FieldsInfo      *ast.FieldsClause
	LinesInfo       *ast.LinesClause
	IgnoreLines     uint64

	DeferredCausetAssignments  []*ast.Assignment
	DeferredCausetsAndUserVars []*ast.DeferredCausetNameOrUserVar

	GenDefCauss InsertGeneratedDeferredCausets
}

// LoadStats represents a load stats plan.
type LoadStats struct {
	baseSchemaProducer

	Path string
}

// IndexAdvise represents a index advise plan.
type IndexAdvise struct {
	baseSchemaProducer

	IsLocal     bool
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LinesInfo   *ast.LinesClause
}

// SplitRegion represents a split regions plan.
type SplitRegion struct {
	baseSchemaProducer

	BlockInfo      *perceptron.BlockInfo
	PartitionNames []perceptron.CIStr
	IndexInfo      *perceptron.IndexInfo
	Lower          []types.Causet
	Upper          []types.Causet
	Num            int
	ValueLists     [][]types.Causet
}

// SplitRegionStatus represents a split regions status plan.
type SplitRegionStatus struct {
	baseSchemaProducer

	Block     causet.Block
	IndexInfo *perceptron.IndexInfo
}

// DBS represents a DBS memex plan.
type DBS struct {
	baseSchemaProducer

	Statement ast.DBSNode
}

// SelectInto represents a select-into plan.
type SelectInto struct {
	baseSchemaProducer

	TargetCauset Causet
	IntoOpt      *ast.SelectIntoOption
}

// Explain represents a explain plan.
type Explain struct {
	baseSchemaProducer

	TargetCauset  Causet
	Format        string
	Analyze       bool
	InterDircStmt ast.StmtNode

	Rows             [][]string
	explainedCausets map[int]bool
}

// GetExplainRowsForCauset get explain rows for plan.
func GetExplainRowsForCauset(plan Causet) (rows [][]string) {
	explain := &Explain{
		TargetCauset: plan,
		Format:       ast.ExplainFormatROW,
		Analyze:      false,
	}
	if err := explain.RenderResult(); err != nil {
		return rows
	}
	return explain.Rows
}

// prepareSchema prepares explain's result schemaReplicant.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)

	switch {
	case format == ast.ExplainFormatROW && !e.Analyze:
		fieldNames = []string{"id", "estRows", "task", "access object", "operator info"}
	case format == ast.ExplainFormatROW && e.Analyze:
		fieldNames = []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == ast.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == ast.ExplainFormatHint:
		fieldNames = []string{"hint"}
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		defcaus: make([]*memex.DeferredCauset, 0, len(fieldNames)),
		names:   make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildDeferredCausetWithName("", fieldName, allegrosql.TypeString, allegrosql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.names = cwn.names
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.TargetCauset == nil {
		return nil
	}
	switch strings.ToLower(e.Format) {
	case ast.ExplainFormatROW:
		if e.Rows == nil || e.Analyze {
			e.explainedCausets = map[int]bool{}
			err := e.explainCausetInRowFormat(e.TargetCauset, "root", "", "", true)
			if err != nil {
				return err
			}
		}
	case ast.ExplainFormatDOT:
		if physicalCauset, ok := e.TargetCauset.(PhysicalCauset); ok {
			e.prepareDotInfo(physicalCauset)
		}
	case ast.ExplainFormatHint:
		hints := GenHintsFromPhysicalCauset(e.TargetCauset)
		hints = append(hints, hint.ExtractBlockHintsFromStmtNode(e.InterDircStmt, nil)...)
		e.Rows = append(e.Rows, []string{hint.RestoreOptimizerHints(hints)})
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// explainCausetInRowFormat generates explain information for root-tasks.
func (e *Explain) explainCausetInRowFormat(p Causet, taskType, driverSide, indent string, isLastChild bool) (err error) {
	e.prepareOperatorInfo(p, taskType, driverSide, indent, isLastChild)
	e.explainedCausets[p.ID()] = true

	// For every child we create a new sub-tree rooted by it.
	childIndent := texttree.Indent4Child(indent, isLastChild)

	if physCauset, ok := p.(PhysicalCauset); ok {
		// indicate driven side and driving side of 'join' and 'apply'
		// See issue https://github.com/whtcorpsinc/milevadb/issues/14602.
		driverSideInfo := make([]string, len(physCauset.Children()))
		buildSide := -1

		switch plan := physCauset.(type) {
		case *PhysicalApply:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalHashJoin:
			if plan.UseOuterToBuild {
				buildSide = plan.InnerChildIdx ^ 1
			} else {
				buildSide = plan.InnerChildIdx
			}
		case *PhysicalMergeJoin:
			if plan.JoinType == RightOuterJoin {
				buildSide = 0
			} else {
				buildSide = 1
			}
		case *PhysicalIndexJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexMergeJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalIndexHashJoin:
			buildSide = plan.InnerChildIdx ^ 1
		case *PhysicalBroadCastJoin:
			buildSide = plan.InnerChildIdx
		}

		if buildSide != -1 {
			driverSideInfo[0], driverSideInfo[1] = "(Build)", "(Probe)"
		} else {
			buildSide = 0
		}

		// Always put the Build above the Probe.
		for i := range physCauset.Children() {
			pchild := &physCauset.Children()[i^buildSide]
			if e.explainedCausets[(*pchild).ID()] {
				continue
			}
			err = e.explainCausetInRowFormat(*pchild, taskType, driverSideInfo[i], childIndent, i == len(physCauset.Children())-1)
			if err != nil {
				return
			}
		}
	}

	switch x := p.(type) {
	case *PhysicalBlockReader:
		var storeType string
		switch x.StoreType {
		case ekv.EinsteinDB, ekv.TiFlash, ekv.MilevaDB:
			// expected do nothing
		default:
			return errors.Errorf("the causetstore type %v is unknown", x.StoreType)
		}
		storeType = x.StoreType.Name()
		err = e.explainCausetInRowFormat(x.blockCauset, "cop["+storeType+"]", "", childIndent, true)
	case *PhysicalIndexReader:
		err = e.explainCausetInRowFormat(x.indexCauset, "cop[einsteindb]", "", childIndent, true)
	case *PhysicalIndexLookUpReader:
		err = e.explainCausetInRowFormat(x.indexCauset, "cop[einsteindb]", "(Build)", childIndent, false)
		err = e.explainCausetInRowFormat(x.blockCauset, "cop[einsteindb]", "(Probe)", childIndent, true)
	case *PhysicalIndexMergeReader:
		for _, pchild := range x.partialCausets {
			err = e.explainCausetInRowFormat(pchild, "cop[einsteindb]", "(Build)", childIndent, false)
		}
		err = e.explainCausetInRowFormat(x.blockCauset, "cop[einsteindb]", "(Probe)", childIndent, true)
	case *Insert:
		if x.SelectCauset != nil {
			err = e.explainCausetInRowFormat(x.SelectCauset, "root", "", childIndent, true)
		}
	case *UFIDelate:
		if x.SelectCauset != nil {
			err = e.explainCausetInRowFormat(x.SelectCauset, "root", "", childIndent, true)
		}
	case *Delete:
		if x.SelectCauset != nil {
			err = e.explainCausetInRowFormat(x.SelectCauset, "root", "", childIndent, true)
		}
	case *InterDircute:
		if x.Causet != nil {
			err = e.explainCausetInRowFormat(x.Causet, "root", "", indent, true)
		}
	}
	return
}

func getRuntimeInfo(ctx stochastikctx.Context, p Causet) (actRows, analyzeInfo, memoryInfo, diskInfo string) {
	runtimeStatsDefCausl := ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl
	if runtimeStatsDefCausl == nil {
		return
	}
	explainID := p.ID()

	// There maybe some mock information for cop task to let runtimeStatsDefCausl.Exists(p.ExplainID()) is true.
	// So check copTaskEkxecDetail first and print the real cop task information if it's not empty.
	if runtimeStatsDefCausl.ExistsCopStats(explainID) {
		copstats := runtimeStatsDefCausl.GetCopStats(explainID)
		analyzeInfo = copstats.String()
		actRows = fmt.Sprint(copstats.GetActRows())
	} else if runtimeStatsDefCausl.ExistsRootStats(explainID) {
		rootstats := runtimeStatsDefCausl.GetRootStats(explainID)
		analyzeInfo = rootstats.String()
		actRows = fmt.Sprint(rootstats.GetActRows())
	} else {
		analyzeInfo = "time:0ns, loops:0"
		actRows = "0"
	}

	memoryInfo = "N/A"
	memTracker := ctx.GetStochastikVars().StmtCtx.MemTracker.SearchTrackerWithoutLock(p.ID())
	if memTracker != nil {
		memoryInfo = memTracker.BytesToString(memTracker.MaxConsumed())
	}

	diskInfo = "N/A"
	diskTracker := ctx.GetStochastikVars().StmtCtx.DiskTracker.SearchTrackerWithoutLock(p.ID())
	if diskTracker != nil {
		diskInfo = diskTracker.BytesToString(diskTracker.MaxConsumed())
	}
	return
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, estimated rows, task type, access object and other operator info.
func (e *Explain) prepareOperatorInfo(p Causet, taskType, driverSide, indent string, isLastChild bool) {
	if p.ExplainID().String() == "_0" {
		return
	}

	id := texttree.PrettyIdentifier(p.ExplainID().String()+driverSide, indent, isLastChild)

	estRows := "N/A"
	if si := p.statsInfo(); si != nil {
		estRows = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}

	var accessObject, operatorInfo string
	if plan, ok := p.(dataAccesser); ok {
		accessObject = plan.AccessObject()
		operatorInfo = plan.OperatorInfo(false)
	} else {
		if pa, ok := p.(partitionAccesser); ok && e.ctx != nil {
			accessObject = pa.accessObject(e.ctx)
		}
		operatorInfo = p.ExplainInfo()
	}

	var event []string
	if e.Analyze {
		actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfo(e.ctx, p)
		event = []string{id, estRows, actRows, taskType, accessObject, analyzeInfo, operatorInfo, memoryInfo, diskInfo}
	} else {
		event = []string{id, estRows, taskType, accessObject, operatorInfo}
	}
	e.Rows = append(e.Rows, event)
}

func (e *Explain) prepareDotInfo(p PhysicalCauset) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(p, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

func (e *Explain) prepareTaskDot(p PhysicalCauset, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", p.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(p.Children()) == 0 {
		if taskTp == "cop" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", p.ExplainID())
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", p.ExplainID())
	}

	var CausetTasks []PhysicalCauset
	var pipelines []string

	for planQueue := []PhysicalCauset{p}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curCauset := planQueue[0]
		switch copCauset := curCauset.(type) {
		case *PhysicalBlockReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copCauset.ExplainID(), copCauset.blockCauset.ExplainID()))
			CausetTasks = append(CausetTasks, copCauset.blockCauset)
		case *PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copCauset.ExplainID(), copCauset.indexCauset.ExplainID()))
			CausetTasks = append(CausetTasks, copCauset.indexCauset)
		case *PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copCauset.ExplainID(), copCauset.blockCauset.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copCauset.ExplainID(), copCauset.indexCauset.ExplainID()))
			CausetTasks = append(CausetTasks, copCauset.blockCauset)
			CausetTasks = append(CausetTasks, copCauset.indexCauset)
		case *PhysicalIndexMergeReader:
			for i := 0; i < len(copCauset.partialCausets); i++ {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copCauset.ExplainID(), copCauset.partialCausets[i].ExplainID()))
				CausetTasks = append(CausetTasks, copCauset.partialCausets[i])
			}
			if copCauset.blockCauset != nil {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copCauset.ExplainID(), copCauset.blockCauset.ExplainID()))
				CausetTasks = append(CausetTasks, copCauset.blockCauset)
			}
		}
		for _, child := range curCauset.Children() {
			fmt.Fprintf(buffer, "\"%s\" -> \"%s\"\n", curCauset.ExplainID(), child.ExplainID())
			planQueue = append(planQueue, child)
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range CausetTasks {
		e.prepareTaskDot(cop.(PhysicalCauset), "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. stochastik is not InTxn
//  3. plan is point get by pk, or point get by unique index (no double read)
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx stochastikctx.Context, p Causet) (bool, error) {
	if !IsAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	if proj, ok := p.(*PhysicalProjection); ok {
		p = proj.Children()[0]
	}

	switch v := p.(type) {
	case *PhysicalIndexReader:
		indexScan := v.IndexCausets[0].(*PhysicalIndexScan)
		return indexScan.IsPointGetByUniqueKey(ctx.GetStochastikVars().StmtCtx), nil
	case *PhysicalBlockReader:
		blockScan := v.BlockCausets[0].(*PhysicalBlockScan)
		isPointRange := len(blockScan.Ranges) == 1 && blockScan.Ranges[0].IsPoint(ctx.GetStochastikVars().StmtCtx)
		if !isPointRange {
			return false, nil
		}
		pkLength := 1
		if blockScan.Block.IsCommonHandle {
			pkIdx := blocks.FindPrimaryIndex(blockScan.Block)
			pkLength = len(pkIdx.DeferredCausets)
		}
		return len(blockScan.Ranges[0].LowVal) == pkLength, nil
	case *PointGetCauset:
		// If the PointGetCauset needs to read data using unique index (double read), we
		// can't use max uint64, because using math.MaxUint64 can't guarantee repeablock-read
		// and the data and index would be inconsistent!
		isPointGet := v.IndexInfo == nil || (v.IndexInfo.Primary && v.TblInfo.IsCommonHandle)
		return isPointGet, nil
	default:
		return false, nil
	}
}

// IsAutoCommitTxn checks if stochastik is in autocommit mode and not InTxn
// used for fast plan like point get
func IsAutoCommitTxn(ctx stochastikctx.Context) bool {
	return ctx.GetStochastikVars().IsAutocommit() && !ctx.GetStochastikVars().InTxn()
}

// IsPointUFIDelateByAutoCommit checks if plan p is point uFIDelate and is in autocommit context
func IsPointUFIDelateByAutoCommit(ctx stochastikctx.Context, p Causet) (bool, error) {
	if !IsAutoCommitTxn(ctx) {
		return false, nil
	}

	// check plan
	uFIDelCauset, ok := p.(*UFIDelate)
	if !ok {
		return false, nil
	}
	if _, isFastSel := uFIDelCauset.SelectCauset.(*PointGetCauset); isFastSel {
		return true, nil
	}
	return false, nil
}

func buildSchemaAndNameFromIndex(defcaus []*memex.DeferredCauset, dbName perceptron.CIStr, tblInfo *perceptron.BlockInfo, idxInfo *perceptron.IndexInfo) (*memex.Schema, types.NameSlice) {
	schemaReplicant := memex.NewSchema(defcaus...)
	idxDefCauss := idxInfo.DeferredCausets
	names := make([]*types.FieldName, 0, len(idxDefCauss))
	tblName := tblInfo.Name
	for _, col := range idxDefCauss {
		names = append(names, &types.FieldName{
			OrigTblName:     tblName,
			OrigDefCausName: col.Name,
			DBName:          dbName,
			TblName:         tblName,
			DefCausName:     col.Name,
		})
	}
	return schemaReplicant, names
}

func buildSchemaAndNameFromPKDefCaus(pkDefCaus *memex.DeferredCauset, dbName perceptron.CIStr, tblInfo *perceptron.BlockInfo) (*memex.Schema, types.NameSlice) {
	schemaReplicant := memex.NewSchema([]*memex.DeferredCauset{pkDefCaus}...)
	names := make([]*types.FieldName, 0, 1)
	tblName := tblInfo.Name
	col := tblInfo.GetPkDefCausInfo()
	names = append(names, &types.FieldName{
		OrigTblName:     tblName,
		OrigDefCausName: col.Name,
		DBName:          dbName,
		TblName:         tblName,
		DefCausName:     col.Name,
	})
	return schemaReplicant, names
}

func locateHashPartition(ctx stochastikctx.Context, expr memex.Expression, pi *perceptron.PartitionInfo, r []types.Causet) (int, error) {
	ret, isNull, err := expr.EvalInt(ctx, chunk.MutRowFromCausets(r).ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	if ret < 0 {
		ret = 0 - ret
	}
	return int(ret % int64(pi.Num)), nil
}
