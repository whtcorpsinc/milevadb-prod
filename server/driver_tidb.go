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

package server

import (
	"context"
	"crypto/tls"
	"sync/atomic"

	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

// MilevaDBDriver implements IDriver.
type MilevaDBDriver struct {
	causetstore ekv.CausetStorage
}

// NewMilevaDBDriver creates a new MilevaDBDriver.
func NewMilevaDBDriver(causetstore ekv.CausetStorage) *MilevaDBDriver {
	driver := &MilevaDBDriver{
		causetstore: causetstore,
	}
	return driver
}

// MilevaDBContext implements QueryCtx.
type MilevaDBContext struct {
	stochastik.Stochastik
	currentDB string
	stmts     map[int]*MilevaDBStatement
}

// MilevaDBStatement implements PreparedStatement.
type MilevaDBStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	paramsType  []byte
	ctx         *MilevaDBContext
	rs          ResultSet
	allegrosql         string
}

// ID implements PreparedStatement ID method.
func (ts *MilevaDBStatement) ID() int {
	return int(ts.id)
}

// Execute implements PreparedStatement Execute method.
func (ts *MilevaDBStatement) Execute(ctx context.Context, args []types.Causet) (rs ResultSet, err error) {
	milevadbRecordset, err := ts.ctx.ExecutePreparedStmt(ctx, ts.id, args)
	if err != nil {
		return nil, err
	}
	if milevadbRecordset == nil {
		return
	}
	rs = &milevadbResultSet{
		recordSet:    milevadbRecordset,
		preparedStmt: ts.ctx.GetStochastikVars().PreparedStmts[ts.id].(*core.CachedPrepareStmt),
	}
	return
}

// AppendParam implements PreparedStatement AppendParam method.
func (ts *MilevaDBStatement) AppendParam(paramID int, data []byte) error {
	if paramID >= len(ts.boundParams) {
		return allegrosql.NewErr(allegrosql.ErrWrongArguments, "stmt_send_longdata")
	}
	// If len(data) is 0, append an empty byte slice to the end to distinguish no data and no parameter.
	if len(data) == 0 {
		ts.boundParams[paramID] = []byte{}
	} else {
		ts.boundParams[paramID] = append(ts.boundParams[paramID], data...)
	}
	return nil
}

// NumParams implements PreparedStatement NumParams method.
func (ts *MilevaDBStatement) NumParams() int {
	return ts.numParams
}

// BoundParams implements PreparedStatement BoundParams method.
func (ts *MilevaDBStatement) BoundParams() [][]byte {
	return ts.boundParams
}

// SetParamsType implements PreparedStatement SetParamsType method.
func (ts *MilevaDBStatement) SetParamsType(paramsType []byte) {
	ts.paramsType = paramsType
}

// GetParamsType implements PreparedStatement GetParamsType method.
func (ts *MilevaDBStatement) GetParamsType() []byte {
	return ts.paramsType
}

// StoreResultSet stores ResultSet for stmt fetching
func (ts *MilevaDBStatement) StoreResultSet(rs ResultSet) {
	// refer to https://dev.allegrosql.com/doc/refman/5.7/en/cursor-restrictions.html
	// You can have open only a single cursor per prepared statement.
	// closing previous ResultSet before associating a new ResultSet with this statement
	// if it exists
	if ts.rs != nil {
		terror.Call(ts.rs.Close)
	}
	ts.rs = rs
}

// GetResultSet gets ResultSet associated this statement
func (ts *MilevaDBStatement) GetResultSet() ResultSet {
	return ts.rs
}

// Reset implements PreparedStatement Reset method.
func (ts *MilevaDBStatement) Reset() {
	for i := range ts.boundParams {
		ts.boundParams[i] = nil
	}

	// closing previous ResultSet if it exists
	if ts.rs != nil {
		terror.Call(ts.rs.Close)
		ts.rs = nil
	}
}

// Close implements PreparedStatement Close method.
func (ts *MilevaDBStatement) Close() error {
	//TODO close at milevadb level
	err := ts.ctx.DropPreparedStmt(ts.id)
	if err != nil {
		return err
	}
	delete(ts.ctx.stmts, int(ts.id))

	// close ResultSet associated with this statement
	if ts.rs != nil {
		terror.Call(ts.rs.Close)
	}
	return nil
}

// OpenCtx implements IDriver.
func (qd *MilevaDBDriver) OpenCtx(connID uint64, capability uint32, defCauslation uint8, dbname string, tlsState *tls.ConnectionState) (*MilevaDBContext, error) {
	se, err := stochastik.CreateStochastik(qd.causetstore)
	if err != nil {
		return nil, err
	}
	se.SetTLSState(tlsState)
	err = se.SetDefCauslation(int(defCauslation))
	if err != nil {
		return nil, err
	}
	se.SetClientCapability(capability)
	se.SetConnectionID(connID)
	tc := &MilevaDBContext{
		Stochastik:   se,
		currentDB: dbname,
		stmts:     make(map[int]*MilevaDBStatement),
	}
	return tc, nil
}

// CurrentDB implements QueryCtx CurrentDB method.
func (tc *MilevaDBContext) CurrentDB() string {
	return tc.currentDB
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *MilevaDBContext) WarningCount() uint16 {
	return tc.GetStochastikVars().StmtCtx.WarningCount()
}

// ExecuteStmt implements QueryCtx interface.
func (tc *MilevaDBContext) ExecuteStmt(ctx context.Context, stmt ast.StmtNode) (ResultSet, error) {
	rs, err := tc.Stochastik.ExecuteStmt(ctx, stmt)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	return &milevadbResultSet{
		recordSet: rs,
	}, nil
}

// Close implements QueryCtx Close method.
func (tc *MilevaDBContext) Close() error {
	// close PreparedStatement associated with this connection
	for _, v := range tc.stmts {
		terror.Call(v.Close)
	}

	tc.Stochastik.Close()
	return nil
}

// FieldList implements QueryCtx FieldList method.
func (tc *MilevaDBContext) FieldList(block string) (defCausumns []*DeferredCausetInfo, err error) {
	fields, err := tc.Stochastik.FieldList(block)
	if err != nil {
		return nil, err
	}
	defCausumns = make([]*DeferredCausetInfo, 0, len(fields))
	for _, f := range fields {
		defCausumns = append(defCausumns, convertDeferredCausetInfo(f))
	}
	return defCausumns, nil
}

// GetStatement implements QueryCtx GetStatement method.
func (tc *MilevaDBContext) GetStatement(stmtID int) PreparedStatement {
	tcStmt := tc.stmts[stmtID]
	if tcStmt != nil {
		return tcStmt
	}
	return nil
}

// Prepare implements QueryCtx Prepare method.
func (tc *MilevaDBContext) Prepare(allegrosql string) (statement PreparedStatement, defCausumns, params []*DeferredCausetInfo, err error) {
	stmtID, paramCount, fields, err := tc.Stochastik.PrepareStmt(allegrosql)
	if err != nil {
		return
	}
	stmt := &MilevaDBStatement{
		allegrosql:         allegrosql,
		id:          stmtID,
		numParams:   paramCount,
		boundParams: make([][]byte, paramCount),
		ctx:         tc,
	}
	statement = stmt
	defCausumns = make([]*DeferredCausetInfo, len(fields))
	for i := range fields {
		defCausumns[i] = convertDeferredCausetInfo(fields[i])
	}
	params = make([]*DeferredCausetInfo, paramCount)
	for i := range params {
		params[i] = &DeferredCausetInfo{
			Type: allegrosql.TypeBlob,
		}
	}
	tc.stmts[int(stmtID)] = stmt
	return
}

type milevadbResultSet struct {
	recordSet    sqlexec.RecordSet
	defCausumns      []*DeferredCausetInfo
	rows         []chunk.Row
	closed       int32
	preparedStmt *core.CachedPrepareStmt
}

func (trs *milevadbResultSet) NewChunk() *chunk.Chunk {
	return trs.recordSet.NewChunk()
}

func (trs *milevadbResultSet) Next(ctx context.Context, req *chunk.Chunk) error {
	return trs.recordSet.Next(ctx, req)
}

func (trs *milevadbResultSet) StoreFetchedRows(rows []chunk.Row) {
	trs.rows = rows
}

func (trs *milevadbResultSet) GetFetchedRows() []chunk.Row {
	if trs.rows == nil {
		trs.rows = make([]chunk.Row, 0, 1024)
	}
	return trs.rows
}

func (trs *milevadbResultSet) Close() error {
	if !atomic.CompareAndSwapInt32(&trs.closed, 0, 1) {
		return nil
	}
	err := trs.recordSet.Close()
	trs.recordSet = nil
	return err
}

// OnFetchReturned implements fetchNotifier#OnFetchReturned
func (trs *milevadbResultSet) OnFetchReturned() {
	if cl, ok := trs.recordSet.(fetchNotifier); ok {
		cl.OnFetchReturned()
	}
}

func (trs *milevadbResultSet) DeferredCausets() []*DeferredCausetInfo {
	if trs.defCausumns != nil {
		return trs.defCausumns
	}
	// for prepare statement, try to get cached defCausumnInfo array
	if trs.preparedStmt != nil {
		ps := trs.preparedStmt
		if defCausInfos, ok := ps.DeferredCausetInfos.([]*DeferredCausetInfo); ok {
			trs.defCausumns = defCausInfos
		}
	}
	if trs.defCausumns == nil {
		fields := trs.recordSet.Fields()
		for _, v := range fields {
			trs.defCausumns = append(trs.defCausumns, convertDeferredCausetInfo(v))
		}
		if trs.preparedStmt != nil {
			// if DeferredCausetInfo struct has allocated object,
			// here maybe we need deep copy DeferredCausetInfo to do caching
			trs.preparedStmt.DeferredCausetInfos = trs.defCausumns
		}
	}
	return trs.defCausumns
}

func convertDeferredCausetInfo(fld *ast.ResultField) (ci *DeferredCausetInfo) {
	ci = &DeferredCausetInfo{
		Name:    fld.DeferredCausetAsName.O,
		OrgName: fld.DeferredCauset.Name.O,
		Block:   fld.BlockAsName.O,
		Schema:  fld.DBName.O,
		Flag:    uint16(fld.DeferredCauset.Flag),
		Charset: uint16(allegrosql.CharsetNameToID(fld.DeferredCauset.Charset)),
		Type:    fld.DeferredCauset.Tp,
	}

	if fld.Block != nil {
		ci.OrgBlock = fld.Block.Name.O
	}
	if fld.DeferredCauset.Flen == types.UnspecifiedLength {
		ci.DeferredCausetLength = 0
	} else {
		ci.DeferredCausetLength = uint32(fld.DeferredCauset.Flen)
	}
	if fld.DeferredCauset.Tp == allegrosql.TypeNewDecimal {
		// Consider the negative sign.
		ci.DeferredCausetLength++
		if fld.DeferredCauset.Decimal > int(types.DefaultFsp) {
			// Consider the decimal point.
			ci.DeferredCausetLength++
		}
	} else if types.IsString(fld.DeferredCauset.Tp) ||
		fld.DeferredCauset.Tp == allegrosql.TypeEnum || fld.DeferredCauset.Tp == allegrosql.TypeSet { // issue #18870
		// Fix issue #4540.
		// The flen is a hint, not a precise value, so most client will not use the value.
		// But we found in rare MyALLEGROSQL client, like Navicat for MyALLEGROSQL(version before 12) will truncate
		// the `show create block` result. To fix this case, we must use a large enough flen to prevent
		// the truncation, in MyALLEGROSQL, it will multiply bytes length by a multiple based on character set.
		// For examples:
		// * latin, the multiple is 1
		// * gb2312, the multiple is 2
		// * Utf-8, the multiple is 3
		// * utf8mb4, the multiple is 4
		// We used to check non-string types to avoid the truncation problem in some MyALLEGROSQL
		// client such as Navicat. Now we only allow string type enter this branch.
		charsetDesc, err := charset.GetCharsetDesc(fld.DeferredCauset.Charset)
		if err != nil {
			ci.DeferredCausetLength = ci.DeferredCausetLength * 4
		} else {
			ci.DeferredCausetLength = ci.DeferredCausetLength * uint32(charsetDesc.Maxlen)
		}
	}

	if fld.DeferredCauset.Decimal == types.UnspecifiedLength {
		if fld.DeferredCauset.Tp == allegrosql.TypeDuration {
			ci.Decimal = uint8(types.DefaultFsp)
		} else {
			ci.Decimal = allegrosql.NotFixedDec
		}
	} else {
		ci.Decimal = uint8(fld.DeferredCauset.Decimal)
	}

	// Keep things compatible for old clients.
	// Refer to allegrosql-server/allegrosql/protodefCaus.cc send_result_set_metadata()
	if ci.Type == allegrosql.TypeVarchar {
		ci.Type = allegrosql.TypeVarString
	}
	return
}
