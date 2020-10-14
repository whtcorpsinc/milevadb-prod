// Copyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package stochastik

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"go.uber.org/zap"
)

type petriMap struct {
	petris map[string]*petri.Petri
	mu      sync.Mutex
}

func (dm *petriMap) Get(causetstore ekv.CausetStorage) (d *petri.Petri, err error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// If this is the only petri instance, and the caller doesn't provide causetstore.
	if len(dm.petris) == 1 && causetstore == nil {
		for _, r := range dm.petris {
			return r, nil
		}
	}

	key := causetstore.UUID()
	d = dm.petris[key]
	if d != nil {
		return
	}

	dbsLease := time.Duration(atomic.LoadInt64(&schemaLease))
	statisticLease := time.Duration(atomic.LoadInt64(&statsLease))
	idxUsageSyncLease := time.Duration(atomic.LoadInt64(&indexUsageSyncLease))
	err = soliton.RunWithRetry(soliton.DefaultMaxRetries, soliton.RetryInterval, func() (retry bool, err1 error) {
		logutil.BgLogger().Info("new petri",
			zap.String("causetstore", causetstore.UUID()),
			zap.Stringer("dbs lease", dbsLease),
			zap.Stringer("stats lease", statisticLease),
			zap.Stringer("index usage sync lease", idxUsageSyncLease))
		factory := createStochastikFunc(causetstore)
		sysFactory := createStochastikWithPetriFunc(causetstore)
		d = petri.NewPetri(causetstore, dbsLease, statisticLease, idxUsageSyncLease, factory)
		err1 = d.Init(dbsLease, sysFactory)
		if err1 != nil {
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			logutil.BgLogger().Error("[dbs] init petri failed",
				zap.Error(err1))
		}
		return true, err1
	})
	if err != nil {
		return nil, err
	}
	dm.petris[key] = d

	return
}

func (dm *petriMap) Delete(causetstore ekv.CausetStorage) {
	dm.mu.Lock()
	delete(dm.petris, causetstore.UUID())
	dm.mu.Unlock()
}

var (
	domap = &petriMap{
		petris: map[string]*petri.Petri{},
	}
	// causetstore.UUID()-> IfBootstrapped
	storeBootstrapped     = make(map[string]bool)
	storeBootstrappedLock sync.Mutex

	// schemaLease is the time for re-uFIDelating remote schemaReplicant.
	// In online DBS, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schemaReplicant.
	// Default schemaReplicant lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schemaReplicant lease, like 300s+.
	schemaLease = int64(1 * time.Second)

	// statsLease is the time for reload stats block.
	statsLease = int64(3 * time.Second)

	// indexUsageSyncLease is the time for index usage synchronization.
	indexUsageSyncLease = int64(60 * time.Second)
)

// ResetStoreForWithEinsteinDBTest is only used in the test code.
// TODO: Remove domap and storeBootstrapped. Use causetstore.SetOption() to do it.
func ResetStoreForWithEinsteinDBTest(causetstore ekv.CausetStorage) {
	domap.Delete(causetstore)
	unsetStoreBootstrapped(causetstore.UUID())
}

func setStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	storeBootstrapped[storeUUID] = true
}

// unsetStoreBootstrapped delete causetstore uuid from stored bootstrapped map.
// currently this function only used for test.
func unsetStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	delete(storeBootstrapped, storeUUID)
}

// SetSchemaLease changes the default schemaReplicant lease time for DBS.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	atomic.StoreInt64(&schemaLease, int64(lease))
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	atomic.StoreInt64(&statsLease, int64(lease))
}

// SetIndexUsageSyncLease changes the default index usage sync lease time for loading info.
func SetIndexUsageSyncLease(lease time.Duration) {
	atomic.StoreInt64(&indexUsageSyncLease, int64(lease))
}

// DisableStats4Test disables the stats for tests.
func DisableStats4Test() {
	SetStatsLease(-1)
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx stochastikctx.Context, src string) ([]ast.StmtNode, error) {
	logutil.BgLogger().Debug("compiling", zap.String("source", src))
	charset, defCauslation := ctx.GetStochastikVars().GetCharsetInfo()
	p := BerolinaSQL.New()
	p.EnableWindowFunc(ctx.GetStochastikVars().EnableWindowFunction)
	p.SetALLEGROSQLMode(ctx.GetStochastikVars().ALLEGROSQLMode)
	stmts, warns, err := p.Parse(src, charset, defCauslation)
	for _, warn := range warns {
		ctx.GetStochastikVars().StmtCtx.AppendWarning(warn)
	}
	if err != nil {
		logutil.BgLogger().Warn("compiling",
			zap.String("source", src),
			zap.Error(err))
		return nil, err
	}
	return stmts, nil
}

func recordAbortTxnDuration(sessVars *variable.StochastikVars) {
	duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
	if sessVars.TxnCtx.IsPessimistic {
		transactionDurationPessimisticAbort.Observe(duration)
	} else {
		transactionDurationOptimisticAbort.Observe(duration)
	}
}

func finishStmt(ctx context.Context, se *stochastik, meetsErr error, allegrosql sqlexec.Statement) error {
	err := autoCommitAfterStmt(ctx, se, meetsErr, allegrosql)
	if se.txn.pending() {
		// After run statement finish, txn state is still pending means the
		// statement never need a Txn(), such as:
		//
		// set @@milevadb_general_log = 1
		// set @@autocommit = 0
		// select 1
		//
		// Reset txn state to invalid to dispose the pending start ts.
		se.txn.changeToInvalid()
	}
	if err != nil {
		return err
	}
	return checkStmtLimit(ctx, se)
}

func autoCommitAfterStmt(ctx context.Context, se *stochastik, meetsErr error, allegrosql sqlexec.Statement) error {
	sessVars := se.stochastikVars
	if meetsErr != nil {
		if !sessVars.InTxn() {
			logutil.BgLogger().Info("rollbackTxn for dbs/autocommit failed")
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars)
		} else if se.txn.Valid() && se.txn.IsPessimistic() && executor.ErrDeadlock.Equal(meetsErr) {
			logutil.BgLogger().Info("rollbackTxn for deadlock", zap.Uint64("txn", se.txn.StartTS()))
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars)
		}
		return meetsErr
	}

	if !sessVars.InTxn() {
		if err := se.CommitTxn(ctx); err != nil {
			if _, ok := allegrosql.(*executor.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
				err = errors.Annotatef(err, "previous statement: %s", se.GetStochastikVars().PrevStmt)
			}
			return err
		}
		return nil
	}
	return nil
}

func checkStmtLimit(ctx context.Context, se *stochastik) error {
	// If the user insert, insert, insert ... but never commit, MilevaDB would OOM.
	// So we limit the statement count in a transaction here.
	var err error
	sessVars := se.GetStochastikVars()
	history := GetHistory(se)
	if history.Count() > int(config.GetGlobalConfig().Performance.StmtCountLimit) {
		if !sessVars.BatchCommit {
			se.RollbackTxn(ctx)
			return errors.Errorf("statement count %d exceeds the transaction limitation, autocommit = %t",
				history.Count(), sessVars.IsAutocommit())
		}
		err = se.NewTxn(ctx)
		// The transaction does not committed yet, we need to keep it in transaction.
		// The last history could not be "commit"/"rollback" statement.
		// It means it is impossible to start a new transaction at the end of the transaction.
		// Because after the server executed "commit"/"rollback" statement, the stochastik is out of the transaction.
		sessVars.SetStatusFlag(allegrosql.ServerStatusInTrans, true)
	}
	return err
}

// GetHistory get all stmtHistory in current txn. Exported only for test.
func GetHistory(ctx stochastikctx.Context) *StmtHistory {
	hist, ok := ctx.GetStochastikVars().TxnCtx.History.(*StmtHistory)
	if ok {
		return hist
	}
	hist = new(StmtHistory)
	ctx.GetStochastikVars().TxnCtx.History = hist
	return hist
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(ctx context.Context, sctx stochastikctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk()
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req.CopyConstruct())
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			rows = append(rows, event)
		}
	}
	return rows, nil
}

// ResultSetToStringSlice changes the RecordSet to [][]string.
func ResultSetToStringSlice(ctx context.Context, s Stochastik, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := GetRows4Test(ctx, s, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i := range rows {
		event := rows[i]
		iRow := make([]string, event.Len())
		for j := 0; j < event.Len(); j++ {
			if event.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := event.GetCauset(j, &rs.Fields()[j].DeferredCauset.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

// Stochastik errors.
var (
	ErrForUFIDelateCantRetry = terror.ClassStochastik.New(errno.ErrForUFIDelateCantRetry, errno.MyALLEGROSQLErrName[errno.ErrForUFIDelateCantRetry])
)
