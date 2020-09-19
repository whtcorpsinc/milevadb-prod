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

// Package mock is just for test only.
package mock

import (
	"context"
	"fmt"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/owner"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/kvcache"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/fidelpb/go-binlog"
)

var _ stochastikctx.Context = (*Context)(nil)
var _ sqlexec.ALLEGROSQLExecutor = (*Context)(nil)

// Context represents mocked stochastikctx.Context.
type Context struct {
	values      map[fmt.Stringer]interface{}
	txn         wrapTxn    // mock global variable
	CausetStore       ekv.CausetStorage // mock global variable
	stochastikVars *variable.StochastikVars
	ctx         context.Context
	cancel      context.CancelFunc
	sm          soliton.StochastikManager
	pcache      *kvcache.SimpleLRUCache
}

type wrapTxn struct {
	ekv.Transaction
}

func (txn *wrapTxn) Valid() bool {
	return txn.Transaction != nil && txn.Transaction.Valid()
}

// Execute implements sqlexec.ALLEGROSQLExecutor Execute interface.
func (c *Context) Execute(ctx context.Context, allegrosql string) ([]sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Support.")
}

// ExecuteInternal implements sqlexec.ALLEGROSQLExecutor ExecuteInternal interface.
func (c *Context) ExecuteInternal(ctx context.Context, allegrosql string) ([]sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Support.")
}

type mockDBSOwnerChecker struct{}

func (c *mockDBSOwnerChecker) IsOwner() bool { return true }

// DBSOwnerChecker returns owner.DBSOwnerChecker.
func (c *Context) DBSOwnerChecker() owner.DBSOwnerChecker {
	return &mockDBSOwnerChecker{}
}

// SetValue implements stochastikctx.Context SetValue interface.
func (c *Context) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

// Value implements stochastikctx.Context Value interface.
func (c *Context) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

// ClearValue implements stochastikctx.Context ClearValue interface.
func (c *Context) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

// HasDirtyContent implements stochastikctx.Context ClearValue interface.
func (c *Context) HasDirtyContent(tid int64) bool {
	return false
}

// GetStochastikVars implements the stochastikctx.Context GetStochastikVars interface.
func (c *Context) GetStochastikVars() *variable.StochastikVars {
	return c.stochastikVars
}

// Txn implements stochastikctx.Context Txn interface.
func (c *Context) Txn(bool) (ekv.Transaction, error) {
	return &c.txn, nil
}

// GetClient implements stochastikctx.Context GetClient interface.
func (c *Context) GetClient() ekv.Client {
	if c.CausetStore == nil {
		return nil
	}
	return c.CausetStore.GetClient()
}

// GetGlobalSysVar implements GlobalVarAccessor GetGlobalSysVar interface.
func (c *Context) GetGlobalSysVar(ctx stochastikctx.Context, name string) (string, error) {
	v := variable.GetSysVar(name)
	if v == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return v.Value, nil
}

// SetGlobalSysVar implements GlobalVarAccessor SetGlobalSysVar interface.
func (c *Context) SetGlobalSysVar(ctx stochastikctx.Context, name string, value string) error {
	v := variable.GetSysVar(name)
	if v == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	v.Value = value
	return nil
}

// PreparedPlanCache implements the stochastikctx.Context interface.
func (c *Context) PreparedPlanCache() *kvcache.SimpleLRUCache {
	return c.pcache
}

// NewTxn implements the stochastikctx.Context interface.
func (c *Context) NewTxn(context.Context) error {
	if c.CausetStore == nil {
		return errors.New("causetstore is not set")
	}
	if c.txn.Valid() {
		err := c.txn.Commit(c.ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	txn, err := c.CausetStore.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	c.txn.Transaction = txn
	return nil
}

// RefreshTxnCtx implements the stochastikctx.Context interface.
func (c *Context) RefreshTxnCtx(ctx context.Context) error {
	return errors.Trace(c.NewTxn(ctx))
}

// InitTxnWithStartTS implements the stochastikctx.Context interface with startTS.
func (c *Context) InitTxnWithStartTS(startTS uint64) error {
	if c.txn.Valid() {
		return nil
	}
	if c.CausetStore != nil {
		txn, err := c.CausetStore.BeginWithStartTS(startTS)
		if err != nil {
			return errors.Trace(err)
		}
		c.txn.Transaction = txn
	}
	return nil
}

// GetStore gets the causetstore of stochastik.
func (c *Context) GetStore() ekv.CausetStorage {
	return c.CausetStore
}

// GetStochastikManager implements the stochastikctx.Context interface.
func (c *Context) GetStochastikManager() soliton.StochastikManager {
	return c.sm
}

// SetStochastikManager set the stochastik manager.
func (c *Context) SetStochastikManager(sm soliton.StochastikManager) {
	c.sm = sm
}

// Cancel implements the Stochastik interface.
func (c *Context) Cancel() {
	c.cancel()
}

// GoCtx returns standard stochastikctx.Context that bind with current transaction.
func (c *Context) GoCtx() context.Context {
	return c.ctx
}

// StoreQueryFeedback stores the query feedback.
func (c *Context) StoreQueryFeedback(_ interface{}) {}

// StmtCommit implements the stochastikctx.Context interface.
func (c *Context) StmtCommit() {}

// StmtRollback implements the stochastikctx.Context interface.
func (c *Context) StmtRollback() {
}

// StmtGetMutation implements the stochastikctx.Context interface.
func (c *Context) StmtGetMutation(blockID int64) *binlog.BlockMutation {
	return nil
}

// StmtAddDirtyBlockOP implements the stochastikctx.Context interface.
func (c *Context) StmtAddDirtyBlockOP(op int, tid int64, handle ekv.Handle) {
}

// AddBlockLock implements the stochastikctx.Context interface.
func (c *Context) AddBlockLock(_ []perceptron.BlockLockTpInfo) {
}

// ReleaseBlockLocks implements the stochastikctx.Context interface.
func (c *Context) ReleaseBlockLocks(locks []perceptron.BlockLockTpInfo) {
}

// ReleaseBlockLockByBlockIDs implements the stochastikctx.Context interface.
func (c *Context) ReleaseBlockLockByBlockIDs(blockIDs []int64) {
}

// CheckBlockLocked implements the stochastikctx.Context interface.
func (c *Context) CheckBlockLocked(_ int64) (bool, perceptron.BlockLockType) {
	return false, perceptron.BlockLockNone
}

// GetAllBlockLocks implements the stochastikctx.Context interface.
func (c *Context) GetAllBlockLocks() []perceptron.BlockLockTpInfo {
	return nil
}

// ReleaseAllBlockLocks implements the stochastikctx.Context interface.
func (c *Context) ReleaseAllBlockLocks() {
}

// HasLockedBlocks implements the stochastikctx.Context interface.
func (c *Context) HasLockedBlocks() bool {
	return false
}

// PrepareTSFuture implements the stochastikctx.Context interface.
func (c *Context) PrepareTSFuture(ctx context.Context) {
}

// Close implements the stochastikctx.Context interface.
func (c *Context) Close() {
}

// NewContext creates a new mocked stochastikctx.Context.
func NewContext() *Context {
	ctx, cancel := context.WithCancel(context.Background())
	sctx := &Context{
		values:      make(map[fmt.Stringer]interface{}),
		stochastikVars: variable.NewStochastikVars(),
		ctx:         ctx,
		cancel:      cancel,
	}
	sctx.stochastikVars.InitChunkSize = 2
	sctx.stochastikVars.MaxChunkSize = 32
	sctx.stochastikVars.StmtCtx.TimeZone = time.UTC
	sctx.stochastikVars.StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	sctx.stochastikVars.StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	sctx.stochastikVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	if err := sctx.GetStochastikVars().SetSystemVar(variable.MaxAllowedPacket, "67108864"); err != nil {
		panic(err)
	}
	return sctx
}

// HookKeyForTest is as alias, used by context.WithValue.
// golint forbits using string type as key in context.WithValue.
type HookKeyForTest string
