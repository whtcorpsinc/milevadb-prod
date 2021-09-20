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

package interlock

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/causet"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/stmtsummary"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapembedded"
)

// processinfoSetter is the interface use to set current running process info.
type processinfoSetter interface {
	SetProcessInfo(string, time.Time, byte, uint64)
}

// recordSet wraps an interlock, implements sqlexec.RecordSet interface
type recordSet struct {
	fields     []*ast.ResultField
	interlock  InterlockingDirectorate
	stmt       *InterDircStmt
	lastErr    error
	txnStartTS uint64
}

func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		a.fields = defCausNames2ResultFields(a.interlock.Schema(), a.stmt.OutputNames, a.stmt.Ctx.GetStochastikVars().CurrentDB)
	}
	return a.fields
}

func defCausNames2ResultFields(schemaReplicant *memex.Schema, names []*types.FieldName, defaultDB string) []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, schemaReplicant.Len())
	defaultDBCIStr := perceptron.NewCIStr(defaultDB)
	for i := 0; i < schemaReplicant.Len(); i++ {
		dbName := names[i].DBName
		if dbName.L == "" && names[i].TblName.L != "" {
			dbName = defaultDBCIStr
		}
		origDefCausName := names[i].OrigDefCausName
		if origDefCausName.L == "" {
			origDefCausName = names[i].DefCausName
		}
		rf := &ast.ResultField{
			DeferredCauset:       &perceptron.DeferredCausetInfo{Name: origDefCausName, FieldType: *schemaReplicant.DeferredCausets[i].RetType},
			DeferredCausetAsName: names[i].DefCausName,
			Block:                &perceptron.BlockInfo{Name: names[i].OrigTblName},
			BlockAsName:          names[i].TblName,
			DBName:               dbName,
		}
		// This is for compatibility.
		// See issue https://github.com/whtcorpsinc/milevadb/issues/10513 .
		if len(rf.DeferredCausetAsName.O) > allegrosql.MaxAliasIdentifierLen {
			rf.DeferredCausetAsName.O = rf.DeferredCausetAsName.O[:allegrosql.MaxAliasIdentifierLen]
		}
		// Usually the length of O equals the length of L.
		// Add this len judgement to avoid panic.
		if len(rf.DeferredCausetAsName.L) > allegrosql.MaxAliasIdentifierLen {
			rf.DeferredCausetAsName.L = rf.DeferredCausetAsName.L[:allegrosql.MaxAliasIdentifierLen]
		}
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's interlock to get next available chunk for later usage.
// If chunk does not contain any rows, then we uFIDelate last query found rows in stochastik variable as current found rows.
// The reason we need uFIDelate is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply uFIDelate last query found rows by the number of event in chunk.
func (a *recordSet) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute allegrosql panic", zap.String("allegrosql", a.stmt.GetTextToLog()), zap.Stack("stack"))
	}()

	err = Next(ctx, a.interlock, req)
	if err != nil {
		a.lastErr = err
		return err
	}
	numEvents := req.NumEvents()
	if numEvents == 0 {
		if a.stmt != nil {
			a.stmt.Ctx.GetStochastikVars().LastFoundEvents = a.stmt.Ctx.GetStochastikVars().StmtCtx.FoundEvents()
		}
		return nil
	}
	if a.stmt != nil {
		a.stmt.Ctx.GetStochastikVars().StmtCtx.AddFoundEvents(uint64(numEvents))
	}
	return nil
}

// NewChunk create a chunk base on top-level interlock's newFirstChunk().
func (a *recordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(a.interlock)
}

func (a *recordSet) Close() error {
	err := a.interlock.Close()
	a.stmt.CloseRecordSet(a.txnStartTS, a.lastErr)
	return err
}

// OnFetchReturned implements commandLifeCycle#OnFetchReturned
func (a *recordSet) OnFetchReturned() {
	a.stmt.LogSlowQuery(a.txnStartTS, a.lastErr == nil, true)
}

// InterDircStmt implements the sqlexec.Statement interface, it builds a causet.Causet to an sqlexec.Statement.
type InterDircStmt struct {
	// GoCtx stores parent go context.Context for a stmt.
	GoCtx context.Context
	// SchemaReplicant stores a reference to the schemaReplicant information.
	SchemaReplicant schemareplicant.SchemaReplicant
	// Causet stores a reference to the final physical plan.
	Causet causetembedded.Causet
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx stochastikctx.Context

	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority        bool
	isPreparedStmt       bool
	isSelectForUFIDelate bool
	retryCount           uint
	retryStartTime       time.Time

	// OutputNames will be set if using cached plan
	OutputNames []*types.FieldName
	PsStmt      *causetembedded.CachedPrepareStmt
}

// PointGet short path for point exec directly from plan, keep only necessary steps
func (a *InterDircStmt) PointGet(ctx context.Context, is schemareplicant.SchemaReplicant) (*recordSet, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("InterDircStmt.PointGet", opentracing.ChildOf(span.Context()))
		span1.LogKV("allegrosql", a.OriginText())
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	startTs := uint64(math.MaxUint64)
	err := a.Ctx.InitTxnWithStartTS(startTs)
	if err != nil {
		return nil, err
	}
	a.Ctx.GetStochastikVars().StmtCtx.Priority = ekv.PriorityHigh

	// try to reuse point get interlock
	if a.PsStmt.InterlockingDirectorate != nil {
		exec, ok := a.PsStmt.InterlockingDirectorate.(*PointGetInterlockingDirectorate)
		if !ok {
			logutil.Logger(ctx).Error("invalid interlock type, not PointGetInterlockingDirectorate for point get path")
			a.PsStmt.InterlockingDirectorate = nil
		} else {
			// CachedCauset type is already checked in last step
			pointGetCauset := a.PsStmt.PreparedAst.CachedCauset.(*causetembedded.PointGetCauset)
			exec.Init(pointGetCauset, startTs)
			a.PsStmt.InterlockingDirectorate = exec
		}
	}
	if a.PsStmt.InterlockingDirectorate == nil {
		b := newInterlockingDirectorateBuilder(a.Ctx, is)
		newInterlockingDirectorate := b.build(a.Causet)
		if b.err != nil {
			return nil, b.err
		}
		a.PsStmt.InterlockingDirectorate = newInterlockingDirectorate
	}
	pointInterlockingDirectorate := a.PsStmt.InterlockingDirectorate.(*PointGetInterlockingDirectorate)
	if err = pointInterlockingDirectorate.Open(ctx); err != nil {
		terror.Call(pointInterlockingDirectorate.Close)
		return nil, err
	}
	return &recordSet{
		interlock:  pointInterlockingDirectorate,
		stmt:       a,
		txnStartTS: startTs,
	}, nil
}

// OriginText returns original memex as a string.
func (a *InterDircStmt) OriginText() string {
	return a.Text
}

// IsPrepared returns true if stmt is a prepare memex.
func (a *InterDircStmt) IsPrepared() bool {
	return a.isPreparedStmt
}

// IsReadOnly returns true if a memex is read only.
// If current StmtNode is an InterDircuteStmt, we can get its prepared stmt,
// then using ast.IsReadOnly function to determine a memex is read only or not.
func (a *InterDircStmt) IsReadOnly(vars *variable.StochastikVars) bool {
	return causet.IsReadOnly(a.StmtNode, vars)
}

// RebuildCauset rebuilds current execute memex plan.
// It returns the current information schemaReplicant version that 'a' is using.
func (a *InterDircStmt) RebuildCauset(ctx context.Context) (int64, error) {
	is := schemareplicant.GetSchemaReplicant(a.Ctx)
	a.SchemaReplicant = is
	if err := causetembedded.Preprocess(a.Ctx, a.StmtNode, is, causetembedded.InTxnRetry); err != nil {
		return 0, err
	}
	p, names, err := causet.Optimize(ctx, a.Ctx, a.StmtNode, is)
	if err != nil {
		return 0, err
	}
	a.OutputNames = names
	a.Causet = p
	return is.SchemaMetaVersion(), nil
}

// InterDirc builds an InterlockingDirectorate from a plan. If the InterlockingDirectorate doesn't return result,
// like the INSERT, UFIDelATE memexs, it executes in this function, if the InterlockingDirectorate returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
func (a *InterDircStmt) InterDirc(ctx context.Context) (_ sqlexec.RecordSet, err error) {
	defer func() {
		r := recover()
		if r == nil {
			if a.retryCount > 0 {
				metrics.StatementPessimisticRetryCount.Observe(float64(a.retryCount))
			}
			lockKeysCnt := a.Ctx.GetStochastikVars().StmtCtx.LockKeysCount
			if lockKeysCnt > 0 {
				metrics.StatementLockKeysCount.Observe(float64(lockKeysCnt))
			}
			return
		}
		if str, ok := r.(string); !ok || !strings.HasPrefix(str, memory.PanicMemoryExceed) {
			panic(r)
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute allegrosql panic", zap.String("allegrosql", a.GetTextToLog()), zap.Stack("stack"))
	}()

	sctx := a.Ctx
	ctx = stochastikctx.SetCommitCtx(ctx, sctx)
	if _, ok := a.Causet.(*causetembedded.Analyze); ok && sctx.GetStochastikVars().InRestrictedALLEGROSQL {
		oriStats, _ := sctx.GetStochastikVars().GetSystemVar(variable.MilevaDBBuildStatsConcurrency)
		oriScan := sctx.GetStochastikVars().DistALLEGROSQLScanConcurrency()
		oriIndex := sctx.GetStochastikVars().IndexSerialScanConcurrency()
		oriIso, _ := sctx.GetStochastikVars().GetSystemVar(variable.TxnIsolation)
		terror.Log(sctx.GetStochastikVars().SetSystemVar(variable.MilevaDBBuildStatsConcurrency, "1"))
		sctx.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(1)
		sctx.GetStochastikVars().SetIndexSerialScanConcurrency(1)
		terror.Log(sctx.GetStochastikVars().SetSystemVar(variable.TxnIsolation, ast.ReadCommitted))
		defer func() {
			terror.Log(sctx.GetStochastikVars().SetSystemVar(variable.MilevaDBBuildStatsConcurrency, oriStats))
			sctx.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(oriScan)
			sctx.GetStochastikVars().SetIndexSerialScanConcurrency(oriIndex)
			terror.Log(sctx.GetStochastikVars().SetSystemVar(variable.TxnIsolation, oriIso))
		}()
	}

	if sctx.GetStochastikVars().StmtCtx.HasMemQuotaHint {
		sctx.GetStochastikVars().StmtCtx.MemTracker.SetBytesLimit(sctx.GetStochastikVars().StmtCtx.MemQuotaQuery)
	}

	e, err := a.buildInterlockingDirectorate()
	if err != nil {
		return nil, err
	}

	if err = e.Open(ctx); err != nil {
		terror.Call(e.Close)
		return nil, err
	}

	cmd32 := atomic.LoadUint32(&sctx.GetStochastikVars().CommandValue)
	cmd := byte(cmd32)
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		pi = raw
		allegrosql := a.OriginText()
		if simple, ok := a.Causet.(*causetembedded.Simple); ok && simple.Statement != nil {
			if ss, ok := simple.Statement.(ast.SensitiveStmtNode); ok {
				// Use SecureText to avoid leak password information.
				allegrosql = ss.SecureText()
			}
		}
		maxInterDircutionTime := getMaxInterDircutionTime(sctx)
		// UFIDelate processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(allegrosql, time.Now(), cmd, maxInterDircutionTime)
		if a.Ctx.GetStochastikVars().StmtCtx.StmtType == "" {
			a.Ctx.GetStochastikVars().StmtCtx.StmtType = GetStmtLabel(a.StmtNode)
		}
	}

	isPessimistic := sctx.GetStochastikVars().TxnCtx.IsPessimistic

	// Special handle for "select for uFIDelate memex" in pessimistic transaction.
	if isPessimistic && a.isSelectForUFIDelate {
		return a.handlePessimisticSelectForUFIDelate(ctx, e)
	}

	if handled, result, err := a.handleNoDelay(ctx, e, isPessimistic); handled {
		return result, err
	}

	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() {
		txnStartTS = txn.StartTS()
	}
	return &recordSet{
		interlock:  e,
		stmt:       a,
		txnStartTS: txnStartTS,
	}, nil
}

func (a *InterDircStmt) handleNoDelay(ctx context.Context, e InterlockingDirectorate, isPessimistic bool) (handled bool, rs sqlexec.RecordSet, err error) {
	sc := a.Ctx.GetStochastikVars().StmtCtx
	defer func() {
		// If the stmt have no rs like `insert`, The stochastik tracker detachment will be directly
		// done in the `defer` function. If the rs is not nil, the detachment will be done in
		// `rs.Close` in `handleStmt`
		if sc != nil && rs == nil {
			if sc.MemTracker != nil {
				sc.MemTracker.DetachFromGlobalTracker()
			}
			if sc.DiskTracker != nil {
				sc.DiskTracker.DetachFromGlobalTracker()
			}
		}
	}()

	toCheck := e
	if explain, ok := e.(*ExplainInterDirc); ok {
		if explain.analyzeInterDirc != nil {
			toCheck = explain.analyzeInterDirc
		}
	}

	// If the interlock doesn't return any result to the client, we execute it without delay.
	if toCheck.Schema().Len() == 0 {
		if isPessimistic {
			return true, nil, a.handlePessimisticDML(ctx, e)
		}
		r, err := a.handleNoDelayInterlockingDirectorate(ctx, e)
		return true, r, err
	} else if proj, ok := toCheck.(*ProjectionInterDirc); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" memex. Take "DO 1, @a=2;" as an example:
		// the Projection has two memexs and two defCausumns in the schemaReplicant, but we should
		// not return the result of the two memexs.
		r, err := a.handleNoDelayInterlockingDirectorate(ctx, e)
		return true, r, err
	}

	return false, nil, nil
}

// getMaxInterDircutionTime get the max execution timeout value.
func getMaxInterDircutionTime(sctx stochastikctx.Context) uint64 {
	if sctx.GetStochastikVars().StmtCtx.HasMaxInterDircutionTime {
		return sctx.GetStochastikVars().StmtCtx.MaxInterDircutionTime
	}
	return sctx.GetStochastikVars().MaxInterDircutionTime
}

type chunkEventRecordSet struct {
	rows     []chunk.Event
	idx      int
	fields   []*ast.ResultField
	e        InterlockingDirectorate
	execStmt *InterDircStmt
}

func (c *chunkEventRecordSet) Fields() []*ast.ResultField {
	return c.fields
}

func (c *chunkEventRecordSet) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for !chk.IsFull() && c.idx < len(c.rows) {
		chk.AppendEvent(c.rows[c.idx])
		c.idx++
	}
	return nil
}

func (c *chunkEventRecordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(c.e)
}

func (c *chunkEventRecordSet) Close() error {
	c.execStmt.CloseRecordSet(c.execStmt.Ctx.GetStochastikVars().TxnCtx.StartTS, nil)
	return nil
}

func (a *InterDircStmt) handlePessimisticSelectForUFIDelate(ctx context.Context, e InterlockingDirectorate) (sqlexec.RecordSet, error) {
	for {
		rs, err := a.runPessimisticSelectForUFIDelate(ctx, e)
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			return nil, err
		}
		if e == nil {
			return rs, nil
		}
	}
}

func (a *InterDircStmt) runPessimisticSelectForUFIDelate(ctx context.Context, e InterlockingDirectorate) (sqlexec.RecordSet, error) {
	defer func() {
		terror.Log(e.Close())
	}()
	var rows []chunk.Event
	var err error
	req := newFirstChunk(e)
	for {
		err = Next(ctx, e, req)
		if err != nil {
			// Handle 'write conflict' error.
			break
		}
		if req.NumEvents() == 0 {
			fields := defCausNames2ResultFields(e.Schema(), a.OutputNames, a.Ctx.GetStochastikVars().CurrentDB)
			return &chunkEventRecordSet{rows: rows, fields: fields, e: e, execStmt: a}, nil
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, a.Ctx.GetStochastikVars().MaxChunkSize)
	}
	return nil, err
}

func (a *InterDircStmt) handleNoDelayInterlockingDirectorate(ctx context.Context, e InterlockingDirectorate) (sqlexec.RecordSet, error) {
	sctx := a.Ctx
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("interlock.handleNoDelayInterlockingDirectorate", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Check if "milevadb_snapshot" is set for the write interlocks.
	// In history read mode, we can not do write operations.
	switch e.(type) {
	case *DeleteInterDirc, *InsertInterDirc, *UFIDelateInterDirc, *ReplaceInterDirc, *LoadDataInterDirc, *DBSInterDirc:
		snapshotTS := sctx.GetStochastikVars().SnapshotTS
		if snapshotTS != 0 {
			return nil, errors.New("can not execute write memex when 'milevadb_snapshot' is set")
		}
		lowResolutionTSO := sctx.GetStochastikVars().LowResolutionTSO
		if lowResolutionTSO {
			return nil, errors.New("can not execute write memex when 'milevadb_low_resolution_tso' is set")
		}
	}

	var err error
	defer func() {
		terror.Log(e.Close())
		a.logAudit()
	}()

	err = Next(ctx, e, newFirstChunk(e))
	if err != nil {
		return nil, err
	}
	return nil, err
}

func (a *InterDircStmt) handlePessimisticDML(ctx context.Context, e InterlockingDirectorate) error {
	sctx := a.Ctx
	// Do not active the transaction here.
	// When autocommit = 0 and transaction in pessimistic mode,
	// memexs like set xxx = xxx; should not active the transaction.
	txn, err := sctx.Txn(false)
	if err != nil {
		return err
	}
	txnCtx := sctx.GetStochastikVars().TxnCtx
	for {
		startPointGetLocking := time.Now()
		_, err = a.handleNoDelayInterlockingDirectorate(ctx, e)
		if !txn.Valid() {
			return err
		}
		if err != nil {
			// It is possible the DML has point get plan that locks the key.
			e, err = a.handlePessimisticLockError(ctx, err)
			if err != nil {
				if ErrDeadlock.Equal(err) {
					metrics.StatementDeadlockDetectDuration.Observe(time.Since(startPointGetLocking).Seconds())
				}
				return err
			}
			continue
		}
		keys, err1 := txn.(pessimisticTxn).KeysNeedToLock()
		if err1 != nil {
			return err1
		}
		keys = txnCtx.DefCauslectUnchangedEventKeys(keys)
		if len(keys) == 0 {
			return nil
		}
		seVars := sctx.GetStochastikVars()
		lockCtx := newLockCtx(seVars, seVars.LockWaitTimeout)
		var lockKeyStats *execdetails.LockKeysDetails
		ctx = context.WithValue(ctx, execdetails.LockKeysDetailCtxKey, &lockKeyStats)
		startLocking := time.Now()
		err = txn.LockKeys(ctx, lockCtx, keys...)
		if lockKeyStats != nil {
			seVars.StmtCtx.MergeLockKeysInterDircDetails(lockKeyStats)
		}
		if err == nil {
			return nil
		}
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			if ErrDeadlock.Equal(err) {
				metrics.StatementDeadlockDetectDuration.Observe(time.Since(startLocking).Seconds())
			}
			return err
		}
	}
}

// UFIDelateForUFIDelateTS uFIDelates the ForUFIDelateTS, if newForUFIDelateTS is 0, it obtain a new TS from FIDel.
func UFIDelateForUFIDelateTS(seCtx stochastikctx.Context, newForUFIDelateTS uint64) error {
	txn, err := seCtx.Txn(false)
	if err != nil {
		return err
	}
	if !txn.Valid() {
		return errors.Trace(ekv.ErrInvalidTxn)
	}
	if newForUFIDelateTS == 0 {
		version, err := seCtx.GetStore().CurrentVersion()
		if err != nil {
			return err
		}
		newForUFIDelateTS = version.Ver
	}
	seCtx.GetStochastikVars().TxnCtx.SetForUFIDelateTS(newForUFIDelateTS)
	txn.SetOption(ekv.SnapshotTS, seCtx.GetStochastikVars().TxnCtx.GetForUFIDelateTS())
	return nil
}

// handlePessimisticLockError uFIDelates TS and rebuild interlock if the err is write conflict.
func (a *InterDircStmt) handlePessimisticLockError(ctx context.Context, err error) (InterlockingDirectorate, error) {
	txnCtx := a.Ctx.GetStochastikVars().TxnCtx
	var newForUFIDelateTS uint64
	if deadlock, ok := errors.Cause(err).(*einsteindb.ErrDeadlock); ok {
		if !deadlock.IsRetryable {
			return nil, ErrDeadlock
		}
		logutil.Logger(ctx).Info("single memex deadlock, retry memex",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("lockTS", deadlock.LockTs),
			zap.Stringer("lockKey", ekv.Key(deadlock.LockKey)),
			zap.Uint64("deadlockKeyHash", deadlock.DeadlockKeyHash))
	} else if terror.ErrorEqual(ekv.ErrWriteConflict, err) {
		errStr := err.Error()
		forUFIDelateTS := txnCtx.GetForUFIDelateTS()
		logutil.Logger(ctx).Debug("pessimistic write conflict, retry memex",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("forUFIDelateTS", forUFIDelateTS),
			zap.String("err", errStr))
		// Always uFIDelate forUFIDelateTS by getting a new timestamp from FIDel.
		// If we use the conflict commitTS as the new forUFIDelateTS and async commit
		// is used, the commitTS of this transaction may exceed the max timestamp
		// that FIDel allocates. Then, the change may be invisible to a new transaction,
		// which means linearizability is broken.
	} else {
		// this branch if err not nil, always uFIDelate forUFIDelateTS to avoid problem described below
		// for nowait, when ErrLock happened, ErrLockAcquireFailAndNoWaitSet will be returned, and in the same txn
		// the select for uFIDelateTs must be uFIDelated, otherwise there maybe rollback problem.
		// begin;  select for uFIDelate key1(here ErrLocked or other errors(or max_execution_time like soliton),
		//         key1 dagger not get and async rollback key1 is raised)
		//         select for uFIDelate key1 again(this time dagger succ(maybe dagger released by others))
		//         the async rollback operation rollbacked the dagger just acquired
		if err != nil {
			tsErr := UFIDelateForUFIDelateTS(a.Ctx, 0)
			if tsErr != nil {
				logutil.Logger(ctx).Warn("UFIDelateForUFIDelateTS failed", zap.Error(tsErr))
			}
		}
		return nil, err
	}
	if a.retryCount >= config.GetGlobalConfig().PessimisticTxn.MaxRetryCount {
		return nil, errors.New("pessimistic dagger retry limit reached")
	}
	a.retryCount++
	a.retryStartTime = time.Now()
	err = UFIDelateForUFIDelateTS(a.Ctx, newForUFIDelateTS)
	if err != nil {
		return nil, err
	}
	e, err := a.buildInterlockingDirectorate()
	if err != nil {
		return nil, err
	}
	// Rollback the memex change before retry it.
	a.Ctx.StmtRollback()
	a.Ctx.GetStochastikVars().StmtCtx.ResetForRetry()

	if err = e.Open(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

func extractConflictCommitTS(errStr string) uint64 {
	strs := strings.Split(errStr, "conflictCommitTS=")
	if len(strs) != 2 {
		return 0
	}
	tsPart := strs[1]
	length := strings.IndexByte(tsPart, ',')
	if length < 0 {
		return 0
	}
	tsStr := tsPart[:length]
	ts, err := strconv.ParseUint(tsStr, 10, 64)
	if err != nil {
		return 0
	}
	return ts
}

type pessimisticTxn interface {
	ekv.Transaction
	// KeysNeedToLock returns the keys need to be locked.
	KeysNeedToLock() ([]ekv.Key, error)
}

// buildInterlockingDirectorate build a interlock from plan, prepared memex may need additional procedure.
func (a *InterDircStmt) buildInterlockingDirectorate() (InterlockingDirectorate, error) {
	ctx := a.Ctx
	stmtCtx := ctx.GetStochastikVars().StmtCtx
	if _, ok := a.Causet.(*causetembedded.InterDircute); !ok {
		// Do not sync transaction for InterDircute memex, because the real optimization work is done in
		// "InterDircuteInterDirc.Build".
		useMaxTS, err := causetembedded.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.Causet)
		if err != nil {
			return nil, err
		}
		if useMaxTS {
			logutil.BgLogger().Debug("init txnStartTS with MaxUint64", zap.Uint64("conn", ctx.GetStochastikVars().ConnectionID), zap.String("text", a.Text))
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else if ctx.GetStochastikVars().SnapshotTS != 0 {
			if _, ok := a.Causet.(*causetembedded.CheckBlock); ok {
				err = ctx.InitTxnWithStartTS(ctx.GetStochastikVars().SnapshotTS)
			}
		}
		if err != nil {
			return nil, err
		}

		if stmtPri := stmtCtx.Priority; stmtPri == allegrosql.NoPriority {
			switch {
			case useMaxTS:
				stmtCtx.Priority = ekv.PriorityHigh
			case a.LowerPriority:
				stmtCtx.Priority = ekv.PriorityLow
			}
		}
	}
	if _, ok := a.Causet.(*causetembedded.Analyze); ok && ctx.GetStochastikVars().InRestrictedALLEGROSQL {
		ctx.GetStochastikVars().StmtCtx.Priority = ekv.PriorityLow
	}

	b := newInterlockingDirectorateBuilder(ctx, a.SchemaReplicant)
	e := b.build(a.Causet)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	// InterDircuteInterDirc is not a real InterlockingDirectorate, we only use it to build another InterlockingDirectorate from a prepared memex.
	if interlockInterDirc, ok := e.(*InterDircuteInterDirc); ok {
		err := interlockInterDirc.Build(b)
		if err != nil {
			return nil, err
		}
		a.Ctx.SetValue(stochastikctx.QueryString, interlockInterDirc.stmt.Text())
		a.OutputNames = interlockInterDirc.outputNames
		a.isPreparedStmt = true
		a.Causet = interlockInterDirc.plan
		if interlockInterDirc.lowerPriority {
			ctx.GetStochastikVars().StmtCtx.Priority = ekv.PriorityLow
		}
		e = interlockInterDirc.stmtInterDirc
	}
	a.isSelectForUFIDelate = b.hasLock && (!stmtCtx.InDeleteStmt && !stmtCtx.InUFIDelateStmt)
	return e, nil
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

func (a *InterDircStmt) logAudit() {
	sessVars := a.Ctx.GetStochastikVars()
	if sessVars.InRestrictedALLEGROSQL {
		return
	}
	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			cmd := allegrosql.Command2Str[byte(atomic.LoadUint32(&a.Ctx.GetStochastikVars().CommandValue))]
			ctx := context.WithValue(context.Background(), plugin.InterDircStartTimeCtxKey, a.Ctx.GetStochastikVars().StartTime)
			audit.OnGeneralEvent(ctx, sessVars, plugin.Log, cmd)
		}
		return nil
	})
	if err != nil {
		log.Error("log audit log failure", zap.Error(err))
	}
}

// FormatALLEGROSQL is used to format the original ALLEGROALLEGROSQL, e.g. truncating long ALLEGROALLEGROSQL, appending prepared arguments.
func FormatALLEGROSQL(allegrosql string, pps variable.PreparedParams) stringutil.StringerFunc {
	return func() string {
		cfg := config.GetGlobalConfig()
		length := len(allegrosql)
		if maxQueryLen := atomic.LoadUint64(&cfg.Log.QueryLogMaxLen); uint64(length) > maxQueryLen {
			allegrosql = fmt.Sprintf("%.*q(len:%d)", maxQueryLen, allegrosql, length)
		}
		return QueryReplacer.Replace(allegrosql) + pps.String()
	}
}

var (
	stochastikInterDircuteRunDurationInternal = metrics.StochastikInterDircuteRunDuration.WithLabelValues(metrics.LblInternal)
	stochastikInterDircuteRunDurationGeneral  = metrics.StochastikInterDircuteRunDuration.WithLabelValues(metrics.LblGeneral)
)

// FinishInterDircuteStmt is used to record some information after `InterDircStmt` execution finished:
// 1. record slow log if needed.
// 2. record summary memex.
// 3. record execute duration metric.
// 4. uFIDelate the `PrevStmt` in stochastik variable.
func (a *InterDircStmt) FinishInterDircuteStmt(txnTS uint64, succ bool, hasMoreResults bool) {
	sessVars := a.Ctx.GetStochastikVars()
	execDetail := sessVars.StmtCtx.GetInterDircDetails()
	// Attach commit/lockKeys runtime stats to interlock runtime stats.
	if (execDetail.CommitDetail != nil || execDetail.LockKeysDetail != nil) && sessVars.StmtCtx.RuntimeStatsDefCausl != nil {
		statsWithCommit := &execdetails.RuntimeStatsWithCommit{
			Commit:   execDetail.CommitDetail,
			LockKeys: execDetail.LockKeysDetail,
		}
		sessVars.StmtCtx.RuntimeStatsDefCausl.RegisterStats(a.Causet.ID(), statsWithCommit)
	}
	// `LowSlowQuery` and `SummaryStmt` must be called before recording `PrevStmt`.
	a.LogSlowQuery(txnTS, succ, hasMoreResults)
	a.SummaryStmt(succ)
	prevStmt := a.GetTextToLog()
	if config.RedactLogEnabled() {
		sessVars.PrevStmt = FormatALLEGROSQL(prevStmt, nil)
	} else {
		pps := types.CloneEvent(sessVars.PreparedParams)
		sessVars.PrevStmt = FormatALLEGROSQL(prevStmt, pps)
	}

	executeDuration := time.Since(sessVars.StartTime) - sessVars.DurationCompile
	if sessVars.InRestrictedALLEGROSQL {
		stochastikInterDircuteRunDurationInternal.Observe(executeDuration.Seconds())
	} else {
		stochastikInterDircuteRunDurationGeneral.Observe(executeDuration.Seconds())
	}
}

// CloseRecordSet will finish the execution of current memex and do some record work
func (a *InterDircStmt) CloseRecordSet(txnStartTS uint64, lastErr error) {
	a.FinishInterDircuteStmt(txnStartTS, lastErr == nil, false)
	a.logAudit()
	// Detach the Memory and disk tracker for the previous stmtCtx from GlobalMemoryUsageTracker and GlobalDiskUsageTracker
	if stmtCtx := a.Ctx.GetStochastikVars().StmtCtx; stmtCtx != nil {
		if stmtCtx.DiskTracker != nil {
			stmtCtx.DiskTracker.DetachFromGlobalTracker()
		}
		if stmtCtx.MemTracker != nil {
			stmtCtx.MemTracker.DetachFromGlobalTracker()
		}
	}
}

// LogSlowQuery is used to print the slow query in the log files.
func (a *InterDircStmt) LogSlowQuery(txnTS uint64, succ bool, hasMoreResults bool) {
	sessVars := a.Ctx.GetStochastikVars()
	level := log.GetLevel()
	cfg := config.GetGlobalConfig()
	costTime := time.Since(sessVars.StartTime) + sessVars.DurationParse
	threshold := time.Duration(atomic.LoadUint64(&cfg.Log.SlowThreshold)) * time.Millisecond
	enable := cfg.Log.EnableSlowLog
	// if the level is Debug, print slow logs anyway
	if (!enable || costTime < threshold) && level > zapembedded.DebugLevel {
		return
	}
	var allegrosql stringutil.StringerFunc
	normalizedALLEGROSQL, digest := sessVars.StmtCtx.ALLEGROSQLDigest()
	if config.RedactLogEnabled() {
		allegrosql = FormatALLEGROSQL(normalizedALLEGROSQL, nil)
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		allegrosql = FormatALLEGROSQL(sensitiveStmt.SecureText(), nil)
	} else {
		allegrosql = FormatALLEGROSQL(a.Text, sessVars.PreparedParams)
	}

	var blockIDs, indexNames string
	if len(sessVars.StmtCtx.BlockIDs) > 0 {
		blockIDs = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.BlockIDs), " ", ",", -1)
	}
	if len(sessVars.StmtCtx.IndexNames) > 0 {
		indexNames = strings.Replace(fmt.Sprintf("%v", sessVars.StmtCtx.IndexNames), " ", ",", -1)
	}
	var stmtDetail execdetails.StmtInterDircDetails
	stmtDetailRaw := a.GoCtx.Value(execdetails.StmtInterDircDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtInterDircDetails))
	}
	execDetail := sessVars.StmtCtx.GetInterDircDetails()
	copTaskInfo := sessVars.StmtCtx.CausetTasksDetails()
	statsInfos := causetembedded.GetStatsInfo(a.Causet)
	memMax := sessVars.StmtCtx.MemTracker.MaxConsumed()
	diskMax := sessVars.StmtCtx.DiskTracker.MaxConsumed()
	_, planDigest := getCausetDigest(a.Ctx, a.Causet)
	slowItems := &variable.SlowQueryLogItems{
		TxnTS:                    txnTS,
		ALLEGROALLEGROSQL:        allegrosql.String(),
		Digest:                   digest,
		TimeTotal:                costTime,
		TimeParse:                sessVars.DurationParse,
		TimeCompile:              sessVars.DurationCompile,
		TimeOptimize:             sessVars.DurationOptimization,
		TimeWaitTS:               sessVars.DurationWaitTS,
		IndexNames:               indexNames,
		StatsInfos:               statsInfos,
		CausetTasks:              copTaskInfo,
		InterDircDetail:          execDetail,
		MemMax:                   memMax,
		DiskMax:                  diskMax,
		Succ:                     succ,
		Causet:                   getCausetTree(a.Causet),
		CausetDigest:             planDigest,
		Prepared:                 a.isPreparedStmt,
		HasMoreResults:           hasMoreResults,
		CausetFromCache:          sessVars.FoundInCausetCache,
		RewriteInfo:              sessVars.RewritePhaseInfo,
		KVTotal:                  time.Duration(atomic.LoadInt64(&stmtDetail.WaitKVResFIDeluration)),
		FIDelTotal:               time.Duration(atomic.LoadInt64(&stmtDetail.WaitFIDelResFIDeluration)),
		BackoffTotal:             time.Duration(atomic.LoadInt64(&stmtDetail.BackoffDuration)),
		WriteALLEGROSQLRespTotal: stmtDetail.WriteALLEGROSQLResFIDeluration,
		InterDircRetryCount:      a.retryCount,
	}
	if a.retryCount > 0 {
		slowItems.InterDircRetryTime = costTime - sessVars.DurationParse - sessVars.DurationCompile - time.Since(a.retryStartTime)
	}
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		slowItems.PrevStmt = sessVars.PrevStmt.String()
	}
	if costTime < threshold {
		logutil.SlowQueryLogger.Debug(sessVars.SlowLogFormat(slowItems))
	} else {
		logutil.SlowQueryLogger.Warn(sessVars.SlowLogFormat(slowItems))
		metrics.TotalQueryProcHistogram.Observe(costTime.Seconds())
		metrics.TotalCopProcHistogram.Observe(execDetail.ProcessTime.Seconds())
		metrics.TotalCopWaitHistogram.Observe(execDetail.WaitTime.Seconds())
		var userString string
		if sessVars.User != nil {
			userString = sessVars.User.String()
		}
		petri.GetPetri(a.Ctx).LogSlowQuery(&petri.SlowQueryInfo{
			ALLEGROALLEGROSQL: allegrosql.String(),
			Digest:            digest,
			Start:             sessVars.StartTime,
			Duration:          costTime,
			Detail:            sessVars.StmtCtx.GetInterDircDetails(),
			Succ:              succ,
			ConnID:            sessVars.ConnectionID,
			TxnTS:             txnTS,
			User:              userString,
			EDB:               sessVars.CurrentDB,
			BlockIDs:          blockIDs,
			IndexNames:        indexNames,
			Internal:          sessVars.InRestrictedALLEGROSQL,
		})
	}
}

// getCausetTree will try to get the select plan tree if the plan is select or the select plan of delete/uFIDelate/insert memex.
func getCausetTree(p causetembedded.Causet) string {
	cfg := config.GetGlobalConfig()
	if atomic.LoadUint32(&cfg.Log.RecordCausetInSlowLog) == 0 {
		return ""
	}
	planTree := causetembedded.EncodeCauset(p)
	if len(planTree) == 0 {
		return planTree
	}
	return variable.SlowLogCausetPrefix + planTree + variable.SlowLogCausetSuffix
}

// getCausetDigest will try to get the select plan tree if the plan is select or the select plan of delete/uFIDelate/insert memex.
func getCausetDigest(sctx stochastikctx.Context, p causetembedded.Causet) (normalized, planDigest string) {
	normalized, planDigest = sctx.GetStochastikVars().StmtCtx.GetCausetDigest()
	if len(normalized) > 0 {
		return
	}
	normalized, planDigest = causetembedded.NormalizeCauset(p)
	sctx.GetStochastikVars().StmtCtx.SetCausetDigest(normalized, planDigest)
	return
}

// SummaryStmt defCauslects memexs for information_schema.memexs_summary
func (a *InterDircStmt) SummaryStmt(succ bool) {
	sessVars := a.Ctx.GetStochastikVars()
	var userString string
	if sessVars.User != nil {
		userString = sessVars.User.Username
	}

	// Internal ALLEGROSQLs must also be recorded to keep the consistency of `PrevStmt` and `PrevStmtDigest`.
	if !stmtsummary.StmtSummaryByDigestMap.Enabled() || ((sessVars.InRestrictedALLEGROSQL || len(userString) == 0) && !stmtsummary.StmtSummaryByDigestMap.EnabledInternal()) {
		sessVars.SetPrevStmtDigest("")
		return
	}
	// Ignore `PREPARE` memexs, but record `EXECUTE` memexs.
	if _, ok := a.StmtNode.(*ast.PrepareStmt); ok {
		return
	}
	stmtCtx := sessVars.StmtCtx
	normalizedALLEGROSQL, digest := stmtCtx.ALLEGROSQLDigest()
	costTime := time.Since(sessVars.StartTime) + sessVars.DurationParse

	var prevALLEGROSQL, prevALLEGROSQLDigest string
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		// If prevALLEGROSQLDigest is not recorded, it means this `commit` is the first ALLEGROALLEGROSQL once stmt summary is enabled,
		// so it's OK just to ignore it.
		if prevALLEGROSQLDigest = sessVars.GetPrevStmtDigest(); len(prevALLEGROSQLDigest) == 0 {
			return
		}
		prevALLEGROSQL = sessVars.PrevStmt.String()
	}
	sessVars.SetPrevStmtDigest(digest)

	// No need to encode every time, so encode lazily.
	planGenerator := func() string {
		return causetembedded.EncodeCauset(a.Causet)
	}
	// Generating plan digest is slow, only generate it once if it's 'Point_Get'.
	// If it's a point get, different ALLEGROSQLs leads to different plans, so ALLEGROALLEGROSQL digest
	// is enough to distinguish different plans in this case.
	var planDigest string
	var planDigestGen func() string
	if a.Causet.TP() == plancodec.TypePointGet {
		planDigestGen = func() string {
			_, planDigest := getCausetDigest(a.Ctx, a.Causet)
			return planDigest
		}
	} else {
		_, planDigest = getCausetDigest(a.Ctx, a.Causet)
	}

	execDetail := stmtCtx.GetInterDircDetails()
	copTaskInfo := stmtCtx.CausetTasksDetails()
	memMax := stmtCtx.MemTracker.MaxConsumed()
	diskMax := stmtCtx.DiskTracker.MaxConsumed()
	allegrosql := a.GetTextToLog()
	stmtInterDircInfo := &stmtsummary.StmtInterDircInfo{
		SchemaName:           strings.ToLower(sessVars.CurrentDB),
		OriginalALLEGROSQL:   allegrosql,
		NormalizedALLEGROSQL: normalizedALLEGROSQL,
		Digest:               digest,
		PrevALLEGROSQL:       prevALLEGROSQL,
		PrevALLEGROSQLDigest: prevALLEGROSQLDigest,
		CausetGenerator:      planGenerator,
		CausetDigest:         planDigest,
		CausetDigestGen:      planDigestGen,
		User:                 userString,
		TotalLatency:         costTime,
		ParseLatency:         sessVars.DurationParse,
		CompileLatency:       sessVars.DurationCompile,
		StmtCtx:              stmtCtx,
		CausetTasks:          copTaskInfo,
		InterDircDetail:      &execDetail,
		MemMax:               memMax,
		DiskMax:              diskMax,
		StartTime:            sessVars.StartTime,
		IsInternal:           sessVars.InRestrictedALLEGROSQL,
		Succeed:              succ,
		CausetInCache:        sessVars.FoundInCausetCache,
		InterDircRetryCount:  a.retryCount,
	}
	if a.retryCount > 0 {
		stmtInterDircInfo.InterDircRetryTime = costTime - sessVars.DurationParse - sessVars.DurationCompile - time.Since(a.retryStartTime)
	}
	stmtsummary.StmtSummaryByDigestMap.AddStatement(stmtInterDircInfo)
}

// GetTextToLog return the query text to log.
func (a *InterDircStmt) GetTextToLog() string {
	var allegrosql string
	if config.RedactLogEnabled() {
		allegrosql, _ = a.Ctx.GetStochastikVars().StmtCtx.ALLEGROSQLDigest()
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		allegrosql = sensitiveStmt.SecureText()
	} else {
		allegrosql = a.Text
	}
	return allegrosql
}
