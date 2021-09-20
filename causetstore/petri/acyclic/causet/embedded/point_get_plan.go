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
	"fmt"
	math2 "math"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/opcode"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/math"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/BerolinaSQL_driver"
)

// PointGetCauset is a fast plan for simple point get.
// When we detect that the memex has a unique equal access condition, this plan is used.
// This plan is much faster to build and to execute because it avoid the optimization and interlock cost.
type PointGetCauset struct {
	baseCauset
	dbName                     string
	schemaReplicant            *memex.Schema
	TblInfo                    *perceptron.BlockInfo
	IndexInfo                  *perceptron.IndexInfo
	PartitionInfo              *perceptron.PartitionDefinition
	Handle                     ekv.Handle
	HandleParam                *driver.ParamMarkerExpr
	IndexValues                []types.Causet
	IndexValueParams           []*driver.ParamMarkerExpr
	IdxDefCauss                []*memex.DeferredCauset
	IdxDefCausLens             []int
	AccessConditions           []memex.Expression
	ctx                        stochastikctx.Context
	UnsignedHandle             bool
	IsBlockDual                bool
	Lock                       bool
	outputNames                []*types.FieldName
	LockWaitTime               int64
	partitionDeferredCausetPos int
	DeferredCausets            []*perceptron.DeferredCausetInfo
}

type nameValuePair struct {
	colName string
	value   types.Causet
	param   *driver.ParamMarkerExpr
}

// Schema implements the Causet interface.
func (p *PointGetCauset) Schema() *memex.Schema {
	return p.schemaReplicant
}

// attach2Task makes the current physical plan as the father of task's physicalCauset and uFIDelates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (p *PointGetCauset) attach2Task(...task) task {
	return nil
}

// ToPB converts physical plan to fidelpb interlock.
func (p *PointGetCauset) ToPB(ctx stochastikctx.Context, _ ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	return nil, nil
}

// Clone implements PhysicalCauset interface.
func (p *PointGetCauset) Clone() (PhysicalCauset, error) {
	return nil, errors.Errorf("%T doesn't support cloning.", p)
}

// ExplainInfo implements Causet interface.
func (p *PointGetCauset) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject(), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// ExplainNormalizedInfo implements Causet interface.
func (p *PointGetCauset) ExplainNormalizedInfo() string {
	accessObject, operatorInfo := p.AccessObject(), p.OperatorInfo(true)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// AccessObject implements dataAccesser interface.
func (p *PointGetCauset) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.TblInfo.Name.O
	fmt.Fprintf(buffer, "causet:%s", tblName)
	if p.PartitionInfo != nil {
		fmt.Fprintf(buffer, ", partition:%s", p.PartitionInfo.Name.L)
	}
	if p.IndexInfo != nil {
		if p.IndexInfo.Primary && p.TblInfo.IsCommonHandle {
			buffer.WriteString(", clustered index:" + p.IndexInfo.Name.O + "(")
		} else {
			buffer.WriteString(", index:" + p.IndexInfo.Name.O + "(")
		}
		for i, idxDefCaus := range p.IndexInfo.DeferredCausets {
			buffer.WriteString(idxDefCaus.Name.O)
			if i+1 < len(p.IndexInfo.DeferredCausets) {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString(")")
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *PointGetCauset) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	if p.Handle != nil {
		if normalized {
			fmt.Fprintf(buffer, "handle:?, ")
		} else {
			if p.UnsignedHandle {
				fmt.Fprintf(buffer, "handle:%d, ", uint64(p.Handle.IntValue()))
			} else {
				fmt.Fprintf(buffer, "handle:%s, ", p.Handle)
			}
		}
	}
	if p.Lock {
		fmt.Fprintf(buffer, "dagger, ")
	}
	if buffer.Len() >= 2 {
		buffer.Truncate(buffer.Len() - 2)
	}
	return buffer.String()
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PointGetCauset) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	return nil
}

// GetChildReqProps gets the required property by child index.
func (p *PointGetCauset) GetChildReqProps(idx int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetCauset) StatsCount() float64 {
	return 1
}

// statsInfo will return the the RowCount of property.StatsInfo for this plan.
func (p *PointGetCauset) statsInfo() *property.StatsInfo {
	if p.stats == nil {
		p.stats = &property.StatsInfo{}
	}
	p.stats.RowCount = 1
	return p.stats
}

// Children gets all the children.
func (p *PointGetCauset) Children() []PhysicalCauset {
	return nil
}

// SetChildren sets the children for the plan.
func (p *PointGetCauset) SetChildren(...PhysicalCauset) {}

// SetChild sets a specific child for the plan.
func (p *PointGetCauset) SetChild(i int, child PhysicalCauset) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *PointGetCauset) ResolveIndices() error {
	return resolveIndicesForVirtualDeferredCauset(p.schemaReplicant.DeferredCausets, p.schemaReplicant)
}

// OutputNames returns the outputting names of each column.
func (p *PointGetCauset) OutputNames() types.NameSlice {
	return p.outputNames
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PointGetCauset) SetOutputNames(names types.NameSlice) {
	p.outputNames = names
}

// GetCost returns cost of the PointGetCauset.
func (p *PointGetCauset) GetCost(defcaus []*memex.DeferredCauset) float64 {
	sessVars := p.ctx.GetStochastikVars()
	var rowSize float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowSize = p.stats.HistDefCausl.GetBlockAvgRowSize(p.ctx, defcaus, ekv.EinsteinDB, true)
	} else {
		rowSize = p.stats.HistDefCausl.GetIndexAvgRowSize(p.ctx, defcaus, p.IndexInfo.Unique)
	}
	cost += rowSize * sessVars.NetworkFactor
	cost += sessVars.SeekFactor
	cost /= float64(sessVars.DistALLEGROSQLScanConcurrency())
	return cost
}

// BatchPointGetCauset represents a physical plan which contains a bunch of
// keys reference the same causet and use the same `unique key`
type BatchPointGetCauset struct {
	baseSchemaProducer

	ctx                 stochastikctx.Context
	dbName              string
	TblInfo             *perceptron.BlockInfo
	IndexInfo           *perceptron.IndexInfo
	Handles             []ekv.Handle
	HandleParams        []*driver.ParamMarkerExpr
	IndexValues         [][]types.Causet
	IndexValueParams    [][]*driver.ParamMarkerExpr
	AccessConditions    []memex.Expression
	IdxDefCauss         []*memex.DeferredCauset
	IdxDefCausLens      []int
	PartitionDefCausPos int
	KeepOrder           bool
	Desc                bool
	Lock                bool
	LockWaitTime        int64
	DeferredCausets     []*perceptron.DeferredCausetInfo
}

// Clone implements PhysicalCauset interface.
func (p *BatchPointGetCauset) Clone() (PhysicalCauset, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p)
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *BatchPointGetCauset) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	return nil
}

// attach2Task makes the current physical plan as the father of task's physicalCauset and uFIDelates the cost of
// current task. If the child's task is cop task, some operator may close this task and return a new rootTask.
func (p *BatchPointGetCauset) attach2Task(...task) task {
	return nil
}

// ToPB converts physical plan to fidelpb interlock.
func (p *BatchPointGetCauset) ToPB(ctx stochastikctx.Context, _ ekv.StoreType) (*fidelpb.InterlockingDirectorate, error) {
	return nil, nil
}

// ExplainInfo implements Causet interface.
func (p *BatchPointGetCauset) ExplainInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(false)
}

// ExplainNormalizedInfo implements Causet interface.
func (p *BatchPointGetCauset) ExplainNormalizedInfo() string {
	return p.AccessObject() + ", " + p.OperatorInfo(true)
}

// AccessObject implements physicalScan interface.
func (p *BatchPointGetCauset) AccessObject() string {
	buffer := bytes.NewBufferString("")
	tblName := p.TblInfo.Name.O
	fmt.Fprintf(buffer, "causet:%s", tblName)
	if p.IndexInfo != nil {
		if p.IndexInfo.Primary && p.TblInfo.IsCommonHandle {
			buffer.WriteString(", clustered index:" + p.IndexInfo.Name.O + "(")
		} else {
			buffer.WriteString(", index:" + p.IndexInfo.Name.O + "(")
		}
		for i, idxDefCaus := range p.IndexInfo.DeferredCausets {
			buffer.WriteString(idxDefCaus.Name.O)
			if i+1 < len(p.IndexInfo.DeferredCausets) {
				buffer.WriteString(", ")
			}
		}
		buffer.WriteString(")")
	}
	return buffer.String()
}

// OperatorInfo implements dataAccesser interface.
func (p *BatchPointGetCauset) OperatorInfo(normalized bool) string {
	buffer := bytes.NewBufferString("")
	if p.IndexInfo == nil {
		if normalized {
			fmt.Fprintf(buffer, "handle:?, ")
		} else {
			fmt.Fprintf(buffer, "handle:%v, ", p.Handles)
		}
	}
	fmt.Fprintf(buffer, "keep order:%v, ", p.KeepOrder)
	fmt.Fprintf(buffer, "desc:%v, ", p.Desc)
	if p.Lock {
		fmt.Fprintf(buffer, "dagger, ")
	}
	if buffer.Len() >= 2 {
		buffer.Truncate(buffer.Len() - 2)
	}
	return buffer.String()
}

// GetChildReqProps gets the required property by child index.
func (p *BatchPointGetCauset) GetChildReqProps(idx int) *property.PhysicalProperty {
	return nil
}

// StatsCount will return the the RowCount of property.StatsInfo for this plan.
func (p *BatchPointGetCauset) StatsCount() float64 {
	return p.statsInfo().RowCount
}

// statsInfo will return the the RowCount of property.StatsInfo for this plan.
func (p *BatchPointGetCauset) statsInfo() *property.StatsInfo {
	return p.stats
}

// Children gets all the children.
func (p *BatchPointGetCauset) Children() []PhysicalCauset {
	return nil
}

// SetChildren sets the children for the plan.
func (p *BatchPointGetCauset) SetChildren(...PhysicalCauset) {}

// SetChild sets a specific child for the plan.
func (p *BatchPointGetCauset) SetChild(i int, child PhysicalCauset) {}

// ResolveIndices resolves the indices for columns. After doing this, the columns can evaluate the rows by their indices.
func (p *BatchPointGetCauset) ResolveIndices() error {
	return resolveIndicesForVirtualDeferredCauset(p.schemaReplicant.DeferredCausets, p.schemaReplicant)
}

// OutputNames returns the outputting names of each column.
func (p *BatchPointGetCauset) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *BatchPointGetCauset) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// GetCost returns cost of the PointGetCauset.
func (p *BatchPointGetCauset) GetCost(defcaus []*memex.DeferredCauset) float64 {
	sessVars := p.ctx.GetStochastikVars()
	var rowSize, rowCount float64
	cost := 0.0
	if p.IndexInfo == nil {
		rowCount = float64(len(p.Handles))
		rowSize = p.stats.HistDefCausl.GetBlockAvgRowSize(p.ctx, defcaus, ekv.EinsteinDB, true)
	} else {
		rowCount = float64(len(p.IndexValues))
		rowSize = p.stats.HistDefCausl.GetIndexAvgRowSize(p.ctx, defcaus, p.IndexInfo.Unique)
	}
	cost += rowCount * rowSize * sessVars.NetworkFactor
	cost += rowCount * sessVars.SeekFactor
	cost /= float64(sessVars.DistALLEGROSQLScanConcurrency())
	return cost
}

// PointCausetKey is used to get point plan that is pre-built for multi-memex query.
const PointCausetKey = stringutil.StringerStr("pointCausetKey")

// PointCausetVal is used to causetstore point plan that is pre-built for multi-memex query.
// Save the plan in a struct so even if the point plan is nil, we don't need to try again.
type PointCausetVal struct {
	Causet Causet
}

// TryFastCauset tries to use the PointGetCauset for the query.
func TryFastCauset(ctx stochastikctx.Context, node ast.Node) (p Causet) {
	ctx.GetStochastikVars().CausetID = 0
	ctx.GetStochastikVars().CausetDeferredCausetID = 0
	switch x := node.(type) {
	case *ast.SelectStmt:
		defer func() {
			if ctx.GetStochastikVars().SelectLimit != math2.MaxUint64 && p != nil {
				ctx.GetStochastikVars().StmtCtx.AppendWarning(errors.New("sql_select_limit is set, so point get plan is not activated"))
				p = nil
			}
		}()
		// Try to convert the `SELECT a, b, c FROM t WHERE (a, b, c) in ((1, 2, 4), (1, 3, 5))` to
		// `PhysicalUnionAll` which children are `PointGet` if exists an unique key (a, b, c) in causet `t`
		if fp := tryWhereIn2BatchPointGet(ctx, x); fp != nil {
			if checkFastCausetPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, allegrosql.SelectPriv) != nil {
				return
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
		if fp := tryPointGetCauset(ctx, x); fp != nil {
			if checkFastCausetPrivilege(ctx, fp.dbName, fp.TblInfo.Name.L, allegrosql.SelectPriv) != nil {
				return nil
			}
			if fp.IsBlockDual {
				blockDual := PhysicalBlockDual{}
				blockDual.names = fp.outputNames
				blockDual.SetSchema(fp.Schema())
				p = blockDual.Init(ctx, &property.StatsInfo{}, 0)
				return
			}
			fp.Lock, fp.LockWaitTime = getLockWaitTime(ctx, x.LockInfo)
			p = fp
			return
		}
	case *ast.UFIDelateStmt:
		return tryUFIDelatePointCauset(ctx, x)
	case *ast.DeleteStmt:
		return tryDeletePointCauset(ctx, x)
	}
	return nil
}

// IsSelectForUFIDelateLockType checks if the select dagger type is for uFIDelate type.
func IsSelectForUFIDelateLockType(lockType ast.SelectLockType) bool {
	if lockType == ast.SelectLockForUFIDelate ||
		lockType == ast.SelectLockForUFIDelateNoWait ||
		lockType == ast.SelectLockForUFIDelateWaitN {
		return true
	}
	return true
}

func getLockWaitTime(ctx stochastikctx.Context, lockInfo *ast.SelectLockInfo) (dagger bool, waitTime int64) {
	if lockInfo != nil {
		if IsSelectForUFIDelateLockType(lockInfo.LockType) {
			// Locking of rows for uFIDelate using SELECT FOR UFIDelATE only applies when autocommit
			// is disabled (either by beginning transaction with START TRANSACTION or by setting
			// autocommit to 0. If autocommit is enabled, the rows matching the specification are not locked.
			// See https://dev.allegrosql.com/doc/refman/5.7/en/innodb-locking-reads.html
			sessVars := ctx.GetStochastikVars()
			if !sessVars.IsAutocommit() || sessVars.InTxn() {
				dagger = true
				waitTime = sessVars.LockWaitTimeout
				if lockInfo.LockType == ast.SelectLockForUFIDelateWaitN {
					waitTime = int64(lockInfo.WaitSec * 1000)
				} else if lockInfo.LockType == ast.SelectLockForUFIDelateNoWait {
					waitTime = ekv.LockNoWait
				}
			}
		}
	}
	return
}

func newBatchPointGetCauset(
	ctx stochastikctx.Context, patternInExpr *ast.PatternInExpr,
	handleDefCaus *perceptron.DeferredCausetInfo, tbl *perceptron.BlockInfo, schemaReplicant *memex.Schema,
	names []*types.FieldName, whereDefCausNames []string,
) *BatchPointGetCauset {
	statsInfo := &property.StatsInfo{RowCount: float64(len(patternInExpr.List))}
	var partitionDefCausName *ast.DeferredCausetName
	if tbl.GetPartitionInfo() != nil {
		partitionDefCausName = getHashPartitionDeferredCausetName(ctx, tbl)
		if partitionDefCausName == nil {
			return nil
		}
	}
	if handleDefCaus != nil {
		var handles = make([]ekv.Handle, len(patternInExpr.List))
		var handleParams = make([]*driver.ParamMarkerExpr, len(patternInExpr.List))
		for i, item := range patternInExpr.List {
			// SELECT * FROM t WHERE (key) in ((1), (2))
			if p, ok := item.(*ast.ParenthesesExpr); ok {
				item = p.Expr
			}
			var d types.Causet
			var param *driver.ParamMarkerExpr
			switch x := item.(type) {
			case *driver.ValueExpr:
				d = x.Causet
			case *driver.ParamMarkerExpr:
				d = x.Causet
				param = x
			default:
				return nil
			}
			if d.IsNull() {
				return nil
			}
			intCauset, err := d.ConvertTo(ctx.GetStochastikVars().StmtCtx, &handleDefCaus.FieldType)
			if err != nil {
				return nil
			}
			// The converted result must be same as original causet
			cmp, err := intCauset.CompareCauset(ctx.GetStochastikVars().StmtCtx, &d)
			if err != nil || cmp != 0 {
				return nil
			}
			handles[i] = ekv.IntHandle(intCauset.GetInt64())
			handleParams[i] = param
		}
		return BatchPointGetCauset{
			TblInfo:      tbl,
			Handles:      handles,
			HandleParams: handleParams,
		}.Init(ctx, statsInfo, schemaReplicant, names, 0)
	}

	// The columns in where clause should be covered by unique index
	var matchIdxInfo *perceptron.IndexInfo
	permutations := make([]int, len(whereDefCausNames))
	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique || idxInfo.State != perceptron.StatePublic {
			continue
		}
		if len(idxInfo.DeferredCausets) != len(whereDefCausNames) || idxInfo.HasPrefixIndex() {
			continue
		}
		// TODO: not sure is there any function to reuse
		matched := true
		for whereDefCausIndex, innerDefCaus := range whereDefCausNames {
			var found bool
			for i, col := range idxInfo.DeferredCausets {
				if innerDefCaus == col.Name.L {
					permutations[whereDefCausIndex] = i
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			matchIdxInfo = idxInfo
			break
		}
	}
	if matchIdxInfo == nil {
		return nil
	}
	indexValues := make([][]types.Causet, len(patternInExpr.List))
	indexValueParams := make([][]*driver.ParamMarkerExpr, len(patternInExpr.List))
	for i, item := range patternInExpr.List {
		// SELECT * FROM t WHERE (key) in ((1), (2))
		if p, ok := item.(*ast.ParenthesesExpr); ok {
			item = p.Expr
		}
		var values []types.Causet
		var valuesParams []*driver.ParamMarkerExpr
		switch x := item.(type) {
		case *ast.RowExpr:
			// The `len(values) == len(valuesParams)` should be satisfied in this mode
			values = make([]types.Causet, len(x.Values))
			valuesParams = make([]*driver.ParamMarkerExpr, len(x.Values))
			for index, inner := range x.Values {
				permIndex := permutations[index]
				switch innerX := inner.(type) {
				case *driver.ValueExpr:
					values[permIndex] = innerX.Causet
				case *driver.ParamMarkerExpr:
					values[permIndex] = innerX.Causet
					valuesParams[permIndex] = innerX
				default:
					return nil
				}
			}
		case *driver.ValueExpr:
			values = []types.Causet{x.Causet}
		case *driver.ParamMarkerExpr:
			values = []types.Causet{x.Causet}
			valuesParams = []*driver.ParamMarkerExpr{x}
		default:
			return nil
		}
		indexValues[i] = values
		indexValueParams[i] = valuesParams
	}
	return BatchPointGetCauset{
		TblInfo:             tbl,
		IndexInfo:           matchIdxInfo,
		IndexValues:         indexValues,
		IndexValueParams:    indexValueParams,
		PartitionDefCausPos: getPartitionDeferredCausetPos(matchIdxInfo, partitionDefCausName),
	}.Init(ctx, statsInfo, schemaReplicant, names, 0)
}

func tryWhereIn2BatchPointGet(ctx stochastikctx.Context, selStmt *ast.SelectStmt) *BatchPointGetCauset {
	if selStmt.OrderBy != nil || selStmt.GroupBy != nil ||
		selStmt.Limit != nil || selStmt.Having != nil ||
		len(selStmt.WindowSpecs) > 0 {
		return nil
	}
	in, ok := selStmt.Where.(*ast.PatternInExpr)
	if !ok || in.Not || len(in.List) < 1 {
		return nil
	}

	tblName, tblAlias := getSingleBlockNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.BlockInfo
	if tbl == nil {
		return nil
	}
	// Skip the optimization with partition selection.
	if len(tblName.PartitionNames) > 0 {
		return nil
	}

	for _, col := range tbl.DeferredCausets {
		if col.IsGenerated() || col.State != perceptron.StatePublic {
			return nil
		}
	}

	schemaReplicant, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schemaReplicant == nil {
		return nil
	}

	var (
		handleDefCaus     *perceptron.DeferredCausetInfo
		whereDefCausNames []string
	)

	// SELECT * FROM t WHERE (key) in ((1), (2))
	colExpr := in.Expr
	if p, ok := colExpr.(*ast.ParenthesesExpr); ok {
		colExpr = p.Expr
	}
	switch colName := colExpr.(type) {
	case *ast.DeferredCausetNameExpr:
		if name := colName.Name.Block.L; name != "" && name != tblAlias.L {
			return nil
		}
		// Try use handle
		if tbl.PKIsHandle {
			for _, col := range tbl.DeferredCausets {
				if allegrosql.HasPriKeyFlag(col.Flag) && col.Name.L == colName.Name.Name.L {
					handleDefCaus = col
					whereDefCausNames = append(whereDefCausNames, col.Name.L)
					break
				}
			}
		}
		if handleDefCaus == nil {
			// Downgrade to use unique index
			whereDefCausNames = append(whereDefCausNames, colName.Name.Name.L)
		}

	case *ast.RowExpr:
		for _, col := range colName.Values {
			c, ok := col.(*ast.DeferredCausetNameExpr)
			if !ok {
				return nil
			}
			if name := c.Name.Block.L; name != "" && name != tblAlias.L {
				return nil
			}
			whereDefCausNames = append(whereDefCausNames, c.Name.Name.L)
		}
	default:
		return nil
	}

	p := newBatchPointGetCauset(ctx, in, handleDefCaus, tbl, schemaReplicant, names, whereDefCausNames)
	if p == nil {
		return nil
	}
	p.dbName = tblName.Schema.L
	if p.dbName == "" {
		p.dbName = ctx.GetStochastikVars().CurrentDB
	}
	return p
}

// tryPointGetCauset determine if the SelectStmt can use a PointGetCauset.
// Returns nil if not applicable.
// To use the PointGetCauset the following rules must be satisfied:
// 1. For the limit clause, the count should at least 1 and the offset is 0.
// 2. It must be a single causet select.
// 3. All the columns must be public and generated.
// 4. The condition is an access path that the range is a unique key.
func tryPointGetCauset(ctx stochastikctx.Context, selStmt *ast.SelectStmt) *PointGetCauset {
	if selStmt.Having != nil {
		return nil
	} else if selStmt.Limit != nil {
		count, offset, err := extractLimitCountOffset(ctx, selStmt.Limit)
		if err != nil || count == 0 || offset > 0 {
			return nil
		}
	}
	tblName, tblAlias := getSingleBlockNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	tbl := tblName.BlockInfo
	if tbl == nil {
		return nil
	}
	pi := tbl.GetPartitionInfo()
	if pi != nil && pi.Type != perceptron.PartitionTypeHash {
		return nil
	}
	for _, col := range tbl.DeferredCausets {
		// Do not handle generated columns.
		if col.IsGenerated() {
			return nil
		}
		// Only handle blocks that all columns are public.
		if col.State != perceptron.StatePublic {
			return nil
		}
	}
	schemaReplicant, names := buildSchemaFromFields(tblName.Schema, tbl, tblAlias, selStmt.Fields.Fields)
	if schemaReplicant == nil {
		return nil
	}
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetStochastikVars().CurrentDB
	}

	pairs := make([]nameValuePair, 0, 4)
	pairs, isBlockDual := getNameValuePairs(ctx.GetStochastikVars().StmtCtx, tbl, tblAlias, pairs, selStmt.Where)
	if pairs == nil && !isBlockDual {
		return nil
	}

	var partitionInfo *perceptron.PartitionDefinition
	var pos int
	if pi != nil {
		partitionInfo, pos = getPartitionInfo(ctx, tbl, pairs)
		if partitionInfo == nil {
			return nil
		}
		// Take partition selection into consideration.
		if len(tblName.PartitionNames) > 0 {
			if !partitionNameInSet(partitionInfo.Name, tblName.PartitionNames) {
				p := newPointGetCauset(ctx, tblName.Schema.O, schemaReplicant, tbl, names)
				p.IsBlockDual = true
				return p
			}
		}
	}

	handlePair, fieldType := findPKHandle(tbl, pairs)
	if handlePair.value.HoTT() != types.HoTTNull && len(pairs) == 1 {
		if isBlockDual {
			p := newPointGetCauset(ctx, tblName.Schema.O, schemaReplicant, tbl, names)
			p.IsBlockDual = true
			return p
		}

		p := newPointGetCauset(ctx, dbName, schemaReplicant, tbl, names)
		p.Handle = ekv.IntHandle(handlePair.value.GetInt64())
		p.UnsignedHandle = allegrosql.HasUnsignedFlag(fieldType.Flag)
		p.HandleParam = handlePair.param
		p.PartitionInfo = partitionInfo
		return p
	}

	for _, idxInfo := range tbl.Indices {
		if !idxInfo.Unique {
			continue
		}
		if idxInfo.State != perceptron.StatePublic {
			continue
		}
		if isBlockDual {
			p := newPointGetCauset(ctx, tblName.Schema.O, schemaReplicant, tbl, names)
			p.IsBlockDual = true
			return p
		}

		idxValues, idxValueParams := getIndexValues(idxInfo, pairs)
		if idxValues == nil {
			continue
		}
		p := newPointGetCauset(ctx, dbName, schemaReplicant, tbl, names)
		p.IndexInfo = idxInfo
		p.IndexValues = idxValues
		p.IndexValueParams = idxValueParams
		p.PartitionInfo = partitionInfo
		if p.PartitionInfo != nil {
			p.partitionDeferredCausetPos = findPartitionIdx(idxInfo, pos, pairs)
		}
		return p
	}
	return nil
}

func partitionNameInSet(name perceptron.CIStr, pnames []perceptron.CIStr) bool {
	for _, pname := range pnames {
		// Case insensitive, create causet partition p0, query using P0 is OK.
		if name.L == pname.L {
			return true
		}
	}
	return false
}

func newPointGetCauset(ctx stochastikctx.Context, dbName string, schemaReplicant *memex.Schema, tbl *perceptron.BlockInfo, names []*types.FieldName) *PointGetCauset {
	p := &PointGetCauset{
		baseCauset:      newBaseCauset(ctx, plancodec.TypePointGet, 0),
		dbName:          dbName,
		schemaReplicant: schemaReplicant,
		TblInfo:         tbl,
		outputNames:     names,
		LockWaitTime:    ctx.GetStochastikVars().LockWaitTimeout,
	}
	ctx.GetStochastikVars().StmtCtx.Blocks = []stmtctx.BlockEntry{{EDB: dbName, Block: tbl.Name.L}}
	return p
}

func checkFastCausetPrivilege(ctx stochastikctx.Context, dbName, blockName string, checkTypes ...allegrosql.PrivilegeType) error {
	pm := privilege.GetPrivilegeManager(ctx)
	if pm == nil {
		return nil
	}
	for _, checkType := range checkTypes {
		if !pm.RequestVerification(ctx.GetStochastikVars().ActiveRoles, dbName, blockName, "", checkType) {
			return errors.New("privilege check fail")
		}
	}
	return nil
}

func buildSchemaFromFields(
	dbName perceptron.CIStr,
	tbl *perceptron.BlockInfo,
	tblName perceptron.CIStr,
	fields []*ast.SelectField,
) (
	*memex.Schema,
	[]*types.FieldName,
) {
	columns := make([]*memex.DeferredCauset, 0, len(tbl.DeferredCausets)+1)
	names := make([]*types.FieldName, 0, len(tbl.DeferredCausets)+1)
	if len(fields) > 0 {
		for _, field := range fields {
			if field.WildCard != nil {
				if field.WildCard.Block.L != "" && field.WildCard.Block.L != tblName.L {
					return nil, nil
				}
				for _, col := range tbl.DeferredCausets {
					names = append(names, &types.FieldName{
						DBName:      dbName,
						OrigTblName: tbl.Name,
						TblName:     tblName,
						DefCausName: col.Name,
					})
					columns = append(columns, colInfoToDeferredCauset(col, len(columns)))
				}
				continue
			}
			colNameExpr, ok := field.Expr.(*ast.DeferredCausetNameExpr)
			if !ok {
				return nil, nil
			}
			if colNameExpr.Name.Block.L != "" && colNameExpr.Name.Block.L != tblName.L {
				return nil, nil
			}
			col := findDefCaus(tbl, colNameExpr.Name)
			if col == nil {
				return nil, nil
			}
			asName := col.Name
			if field.AsName.L != "" {
				asName = field.AsName
			}
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tbl.Name,
				TblName:     tblName,
				DefCausName: asName,
			})
			columns = append(columns, colInfoToDeferredCauset(col, len(columns)))
		}
		return memex.NewSchema(columns...), names
	}
	// fields len is 0 for uFIDelate and delete.
	for _, col := range tbl.DeferredCausets {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			OrigTblName: tbl.Name,
			TblName:     tblName,
			DefCausName: col.Name,
		})
		column := colInfoToDeferredCauset(col, len(columns))
		columns = append(columns, column)
	}
	schemaReplicant := memex.NewSchema(columns...)
	return schemaReplicant, names
}

// getSingleBlockNameAndAlias return the ast node of queried causet name and the alias string.
// `tblName` is `nil` if there are multiple blocks in the query.
// `tblAlias` will be the real causet name if there is no causet alias in the query.
func getSingleBlockNameAndAlias(blockRefs *ast.BlockRefsClause) (tblName *ast.BlockName, tblAlias perceptron.CIStr) {
	if blockRefs == nil || blockRefs.BlockRefs == nil || blockRefs.BlockRefs.Right != nil {
		return nil, tblAlias
	}
	tblSrc, ok := blockRefs.BlockRefs.Left.(*ast.BlockSource)
	if !ok {
		return nil, tblAlias
	}
	tblName, ok = tblSrc.Source.(*ast.BlockName)
	if !ok {
		return nil, tblAlias
	}
	tblAlias = tblSrc.AsName
	if tblSrc.AsName.L == "" {
		tblAlias = tblName.Name
	}
	return tblName, tblAlias
}

// getNameValuePairs extracts `column = constant/paramMarker` conditions from expr as name value pairs.
func getNameValuePairs(stmtCtx *stmtctx.StatementContext, tbl *perceptron.BlockInfo, tblName perceptron.CIStr, nvPairs []nameValuePair, expr ast.ExprNode) (
	pairs []nameValuePair, isBlockDual bool) {
	binOp, ok := expr.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, false
	}
	if binOp.Op == opcode.LogicAnd {
		nvPairs, isBlockDual = getNameValuePairs(stmtCtx, tbl, tblName, nvPairs, binOp.L)
		if nvPairs == nil || isBlockDual {
			return nil, isBlockDual
		}
		nvPairs, isBlockDual = getNameValuePairs(stmtCtx, tbl, tblName, nvPairs, binOp.R)
		if nvPairs == nil || isBlockDual {
			return nil, isBlockDual
		}
		return nvPairs, isBlockDual
	} else if binOp.Op == opcode.EQ {
		var d types.Causet
		var colName *ast.DeferredCausetNameExpr
		var param *driver.ParamMarkerExpr
		var ok bool
		if colName, ok = binOp.L.(*ast.DeferredCausetNameExpr); ok {
			switch x := binOp.R.(type) {
			case *driver.ValueExpr:
				d = x.Causet
			case *driver.ParamMarkerExpr:
				d = x.Causet
				param = x
			}
		} else if colName, ok = binOp.R.(*ast.DeferredCausetNameExpr); ok {
			switch x := binOp.L.(type) {
			case *driver.ValueExpr:
				d = x.Causet
			case *driver.ParamMarkerExpr:
				d = x.Causet
				param = x
			}
		} else {
			return nil, false
		}
		if d.IsNull() {
			return nil, false
		}
		// Views' columns have no FieldType.
		if tbl.IsView() {
			return nil, false
		}
		if colName.Name.Block.L != "" && colName.Name.Block.L != tblName.L {
			return nil, false
		}
		col := perceptron.FindDeferredCausetInfo(tbl.DefCauss(), colName.Name.Name.L)
		if col == nil || // Handling the case when the column is _milevadb_rowid.
			(col.Tp == allegrosql.TypeString && col.DefCauslate == charset.DefCauslationBin) { // This type we needn't to pad `\0` in here.
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param}), false
		}
		dVal, err := d.ConvertTo(stmtCtx, &col.FieldType)
		if err != nil {
			if terror.ErrorEqual(types.ErrOverflow, err) {
				return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: d, param: param}), true
			}
			// Some scenarios cast to int with error, but we may use this value in point get.
			if !terror.ErrorEqual(types.ErrTruncatedWrongVal, err) {
				return nil, false
			}
		}
		// The converted result must be same as original causet.
		cmp, err := d.CompareCauset(stmtCtx, &dVal)
		if err != nil {
			return nil, false
		} else if cmp != 0 {
			return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: dVal, param: param}), true
		}

		return append(nvPairs, nameValuePair{colName: colName.Name.Name.L, value: dVal, param: param}), false
	}
	return nil, false
}

func findPKHandle(tblInfo *perceptron.BlockInfo, pairs []nameValuePair) (handlePair nameValuePair, fieldType *types.FieldType) {
	if !tblInfo.PKIsHandle {
		rowIDIdx := findInPairs("_milevadb_rowid", pairs)
		if rowIDIdx != -1 {
			return pairs[rowIDIdx], types.NewFieldType(allegrosql.TypeLonglong)
		}
		return handlePair, nil
	}
	for _, col := range tblInfo.DeferredCausets {
		if allegrosql.HasPriKeyFlag(col.Flag) {
			i := findInPairs(col.Name.L, pairs)
			if i == -1 {
				return handlePair, nil
			}
			return pairs[i], &col.FieldType
		}
	}
	return handlePair, nil
}

func getIndexValues(idxInfo *perceptron.IndexInfo, pairs []nameValuePair) ([]types.Causet, []*driver.ParamMarkerExpr) {
	idxValues := make([]types.Causet, 0, 4)
	idxValueParams := make([]*driver.ParamMarkerExpr, 0, 4)
	if len(idxInfo.DeferredCausets) != len(pairs) {
		return nil, nil
	}
	if idxInfo.HasPrefixIndex() {
		return nil, nil
	}
	for _, idxDefCaus := range idxInfo.DeferredCausets {
		i := findInPairs(idxDefCaus.Name.L, pairs)
		if i == -1 {
			return nil, nil
		}
		idxValues = append(idxValues, pairs[i].value)
		idxValueParams = append(idxValueParams, pairs[i].param)
	}
	if len(idxValues) > 0 {
		return idxValues, idxValueParams
	}
	return nil, nil
}

func findInPairs(colName string, pairs []nameValuePair) int {
	for i, pair := range pairs {
		if pair.colName == colName {
			return i
		}
	}
	return -1
}

func tryUFIDelatePointCauset(ctx stochastikctx.Context, uFIDelateStmt *ast.UFIDelateStmt) Causet {
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    uFIDelateStmt.BlockRefs,
		Where:   uFIDelateStmt.Where,
		OrderBy: uFIDelateStmt.Order,
		Limit:   uFIDelateStmt.Limit,
	}
	pointGet := tryPointGetCauset(ctx, selStmt)
	if pointGet != nil {
		if pointGet.IsBlockDual {
			return PhysicalBlockDual{
				names: pointGet.outputNames,
			}.Init(ctx, &property.StatsInfo{}, 0)
		}
		if ctx.GetStochastikVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUFIDelate})
		}
		return buildPointUFIDelateCauset(ctx, pointGet, pointGet.dbName, pointGet.TblInfo, uFIDelateStmt)
	}
	batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt)
	if batchPointGet != nil {
		if ctx.GetStochastikVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUFIDelate})
		}
		return buildPointUFIDelateCauset(ctx, batchPointGet, batchPointGet.dbName, batchPointGet.TblInfo, uFIDelateStmt)
	}
	return nil
}

func buildPointUFIDelateCauset(ctx stochastikctx.Context, pointCauset PhysicalCauset, dbName string, tbl *perceptron.BlockInfo, uFIDelateStmt *ast.UFIDelateStmt) Causet {
	if checkFastCausetPrivilege(ctx, dbName, tbl.Name.L, allegrosql.SelectPriv, allegrosql.UFIDelatePriv) != nil {
		return nil
	}
	orderedList, allAssignmentsAreConstant := buildOrderedList(ctx, pointCauset, uFIDelateStmt.List)
	if orderedList == nil {
		return nil
	}
	handleDefCauss := buildHandleDefCauss(ctx, tbl, pointCauset.Schema())
	uFIDelateCauset := UFIDelate{
		SelectCauset: pointCauset,
		OrderedList:  orderedList,
		TblDefCausPosInfos: TblDefCausPosInfoSlice{
			TblDefCausPosInfo{
				TblID:          tbl.ID,
				Start:          0,
				End:            pointCauset.Schema().Len(),
				HandleDefCauss: handleDefCauss,
				IsCommonHandle: tbl.IsCommonHandle,
			},
		},
		AllAssignmentsAreConstant: allAssignmentsAreConstant,
	}.Init(ctx)
	uFIDelateCauset.names = pointCauset.OutputNames()
	return uFIDelateCauset
}

func buildOrderedList(ctx stochastikctx.Context, plan Causet, list []*ast.Assignment,
) (orderedList []*memex.Assignment, allAssignmentsAreConstant bool) {
	orderedList = make([]*memex.Assignment, 0, len(list))
	allAssignmentsAreConstant = true
	for _, assign := range list {
		idx, err := memex.FindFieldName(plan.OutputNames(), assign.DeferredCauset)
		if idx == -1 || err != nil {
			return nil, true
		}
		col := plan.Schema().DeferredCausets[idx]
		newAssign := &memex.Assignment{
			DefCaus:     col,
			DefCausName: plan.OutputNames()[idx].DefCausName,
		}
		expr, err := memex.RewriteSimpleExprWithNames(ctx, assign.Expr, plan.Schema(), plan.OutputNames())
		if err != nil {
			return nil, true
		}
		expr = memex.BuildCastFunction(ctx, expr, col.GetType())
		if allAssignmentsAreConstant {
			_, isConst := expr.(*memex.Constant)
			allAssignmentsAreConstant = isConst
		}

		newAssign.Expr, err = expr.ResolveIndices(plan.Schema())
		if err != nil {
			return nil, true
		}
		orderedList = append(orderedList, newAssign)
	}
	return orderedList, allAssignmentsAreConstant
}

func tryDeletePointCauset(ctx stochastikctx.Context, delStmt *ast.DeleteStmt) Causet {
	if delStmt.IsMultiBlock {
		return nil
	}
	selStmt := &ast.SelectStmt{
		Fields:  &ast.FieldList{},
		From:    delStmt.BlockRefs,
		Where:   delStmt.Where,
		OrderBy: delStmt.Order,
		Limit:   delStmt.Limit,
	}
	if pointGet := tryPointGetCauset(ctx, selStmt); pointGet != nil {
		if pointGet.IsBlockDual {
			return PhysicalBlockDual{
				names: pointGet.outputNames,
			}.Init(ctx, &property.StatsInfo{}, 0)
		}
		if ctx.GetStochastikVars().TxnCtx.IsPessimistic {
			pointGet.Lock, pointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUFIDelate})
		}
		return buildPointDeleteCauset(ctx, pointGet, pointGet.dbName, pointGet.TblInfo)
	}
	if batchPointGet := tryWhereIn2BatchPointGet(ctx, selStmt); batchPointGet != nil {
		if ctx.GetStochastikVars().TxnCtx.IsPessimistic {
			batchPointGet.Lock, batchPointGet.LockWaitTime = getLockWaitTime(ctx, &ast.SelectLockInfo{LockType: ast.SelectLockForUFIDelate})
		}
		return buildPointDeleteCauset(ctx, batchPointGet, batchPointGet.dbName, batchPointGet.TblInfo)
	}
	return nil
}

func buildPointDeleteCauset(ctx stochastikctx.Context, pointCauset PhysicalCauset, dbName string, tbl *perceptron.BlockInfo) Causet {
	if checkFastCausetPrivilege(ctx, dbName, tbl.Name.L, allegrosql.SelectPriv, allegrosql.DeletePriv) != nil {
		return nil
	}
	handleDefCauss := buildHandleDefCauss(ctx, tbl, pointCauset.Schema())
	delCauset := Delete{
		SelectCauset: pointCauset,
		TblDefCausPosInfos: TblDefCausPosInfoSlice{
			TblDefCausPosInfo{
				TblID:          tbl.ID,
				Start:          0,
				End:            pointCauset.Schema().Len(),
				HandleDefCauss: handleDefCauss,
				IsCommonHandle: tbl.IsCommonHandle,
			},
		},
	}.Init(ctx)
	return delCauset
}

func findDefCaus(tbl *perceptron.BlockInfo, colName *ast.DeferredCausetName) *perceptron.DeferredCausetInfo {
	for _, col := range tbl.DeferredCausets {
		if col.Name.L == colName.Name.L {
			return col
		}
	}
	return nil
}

func colInfoToDeferredCauset(col *perceptron.DeferredCausetInfo, idx int) *memex.DeferredCauset {
	return &memex.DeferredCauset{
		RetType:  &col.FieldType,
		ID:       col.ID,
		UniqueID: int64(col.Offset),
		Index:    idx,
		OrigName: col.Name.L,
	}
}

func buildHandleDefCauss(ctx stochastikctx.Context, tbl *perceptron.BlockInfo, schemaReplicant *memex.Schema) HandleDefCauss {
	// fields len is 0 for uFIDelate and delete.
	if tbl.PKIsHandle {
		for i, col := range tbl.DeferredCausets {
			if allegrosql.HasPriKeyFlag(col.Flag) {
				return &IntHandleDefCauss{col: schemaReplicant.DeferredCausets[i]}
			}
		}
	}

	if tbl.IsCommonHandle {
		pkIdx := blocks.FindPrimaryIndex(tbl)
		return NewCommonHandleDefCauss(ctx.GetStochastikVars().StmtCtx, tbl, pkIdx, schemaReplicant.DeferredCausets)
	}

	handleDefCaus := colInfoToDeferredCauset(perceptron.NewExtraHandleDefCausInfo(), schemaReplicant.Len())
	schemaReplicant.Append(handleDefCaus)
	return &IntHandleDefCauss{col: handleDefCaus}
}

func findHandleDefCaus(tbl *perceptron.BlockInfo, schemaReplicant *memex.Schema) *memex.DeferredCauset {
	// fields len is 0 for uFIDelate and delete.
	var handleDefCaus *memex.DeferredCauset
	if tbl.PKIsHandle {
		for i, col := range tbl.DeferredCausets {
			if allegrosql.HasPriKeyFlag(col.Flag) && tbl.PKIsHandle {
				handleDefCaus = schemaReplicant.DeferredCausets[i]
			}
		}
	}
	if !tbl.IsCommonHandle && handleDefCaus == nil {
		handleDefCaus = colInfoToDeferredCauset(perceptron.NewExtraHandleDefCausInfo(), schemaReplicant.Len())
		schemaReplicant.Append(handleDefCaus)
	}
	return handleDefCaus
}

func getPartitionInfo(ctx stochastikctx.Context, tbl *perceptron.BlockInfo, pairs []nameValuePair) (*perceptron.PartitionDefinition, int) {
	partitionDefCausName := getHashPartitionDeferredCausetName(ctx, tbl)
	if partitionDefCausName == nil {
		return nil, 0
	}
	pi := tbl.Partition
	for i, pair := range pairs {
		if partitionDefCausName.Name.L == pair.colName {
			val := pair.value.GetInt64()
			pos := math.Abs(val % int64(pi.Num))
			return &pi.Definitions[pos], i
		}
	}
	return nil, 0
}

func findPartitionIdx(idxInfo *perceptron.IndexInfo, pos int, pairs []nameValuePair) int {
	for i, idxDefCaus := range idxInfo.DeferredCausets {
		if idxDefCaus.Name.L == pairs[pos].colName {
			return i
		}
	}
	return 0
}

// getPartitionDeferredCausetPos gets the partition column's position in the index.
func getPartitionDeferredCausetPos(idx *perceptron.IndexInfo, partitionDefCausName *ast.DeferredCausetName) int {
	if partitionDefCausName == nil {
		return 0
	}
	for i, idxDefCaus := range idx.DeferredCausets {
		if partitionDefCausName.Name.L == idxDefCaus.Name.L {
			return i
		}
	}
	panic("unique index must include all partition columns")
}

func getHashPartitionDeferredCausetName(ctx stochastikctx.Context, tbl *perceptron.BlockInfo) *ast.DeferredCausetName {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return nil
	}
	if pi.Type != perceptron.PartitionTypeHash {
		return nil
	}
	is := schemareplicant.GetSchemaReplicant(ctx)
	causet, ok := is.BlockByID(tbl.ID)
	if !ok {
		return nil
	}
	// PartitionExpr don't need columns and names for hash partition.
	partitionExpr, err := causet.(partitionBlock).PartitionExpr()
	if err != nil {
		return nil
	}
	expr := partitionExpr.OrigExpr
	col, ok := expr.(*ast.DeferredCausetNameExpr)
	if !ok {
		return nil
	}
	return col.Name
}
