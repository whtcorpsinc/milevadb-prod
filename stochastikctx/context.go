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

package stochastikctx

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/owner"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/kvcache"
	"github.com/whtcorpsinc/fidelpb/go-binlog"
)

// Context is an interface for transaction and executive args environment.
type Context interface {
	// NewTxn creates a new transaction for further execution.
	// If old transaction is valid, it is committed first.
	// It's used in BEGIN statement and DBS statements to commit old transaction.
	NewTxn(context.Context) error

	// Txn returns the current transaction which is created before executing a statement.
	// The returned ekv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (ekv.Transaction, error)

	// GetClient gets a ekv.Client.
	GetClient() ekv.Client

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	GetStochastikVars() *variable.StochastikVars

	GetStochastikManager() soliton.StochastikManager

	// RefreshTxnCtx commits old transaction without retry,
	// and creates a new transaction.
	// now just for load data and batch insert.
	RefreshTxnCtx(context.Context) error

	// InitTxnWithStartTS initializes a transaction with startTS.
	// It should be called right before we builds an executor.
	InitTxnWithStartTS(startTS uint64) error

	// GetStore returns the causetstore of stochastik.
	GetStore() ekv.CausetStorage

	// PreparedPlanCache returns the cache of the physical plan
	PreparedPlanCache() *kvcache.SimpleLRUCache

	// StoreQueryFeedback stores the query feedback.
	StoreQueryFeedback(feedback interface{})

	// HasDirtyContent checks whether there's dirty uFIDelate on the given block.
	HasDirtyContent(tid int64) bool

	// StmtCommit flush all changes by the statement to the underlying transaction.
	StmtCommit()
	// StmtRollback provides statement level rollback.
	StmtRollback()
	// StmtGetMutation gets the binlog mutation for current statement.
	StmtGetMutation(int64) *binlog.BlockMutation
	// DBSOwnerChecker returns owner.DBSOwnerChecker.
	DBSOwnerChecker() owner.DBSOwnerChecker
	// AddBlockLock adds block lock to the stochastik lock map.
	AddBlockLock([]perceptron.BlockLockTpInfo)
	// ReleaseBlockLocks releases block locks in the stochastik lock map.
	ReleaseBlockLocks(locks []perceptron.BlockLockTpInfo)
	// ReleaseBlockLockByBlockID releases block locks in the stochastik lock map by block ID.
	ReleaseBlockLockByBlockIDs(blockIDs []int64)
	// CheckBlockLocked checks the block lock.
	CheckBlockLocked(tblID int64) (bool, perceptron.BlockLockType)
	// GetAllBlockLocks gets all block locks block id and EDB id hold by the stochastik.
	GetAllBlockLocks() []perceptron.BlockLockTpInfo
	// ReleaseAllBlockLocks releases all block locks hold by the stochastik.
	ReleaseAllBlockLocks()
	// HasLockedBlocks uses to check whether this stochastik locked any blocks.
	HasLockedBlocks() bool
	// PrepareTSFuture uses to prepare timestamp by future.
	PrepareTSFuture(ctx context.Context)
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	case LastExecuteDBS:
		return "last_execute_dbs"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrade job.
	Initing basicCtxType = 2
	// LastExecuteDBS is the key for whether the stochastik execute a dbs command last time.
	LastExecuteDBS basicCtxType = 3
)

type connIDCtxKeyType struct{}

// ConnID is the key in context.
var ConnID = connIDCtxKeyType{}

// SetCommitCtx sets connection id into context
func SetCommitCtx(ctx context.Context, sessCtx Context) context.Context {
	return context.WithValue(ctx, ConnID, sessCtx.GetStochastikVars().ConnectionID)
}
