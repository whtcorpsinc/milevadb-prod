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

package bindinfo

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/format"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/BerolinaSQL_driver"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	utilBerolinaSQL "github.com/whtcorpsinc/milevadb/soliton/BerolinaSQL"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/stmtsummary"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"go.uber.org/zap"
)

// BindHandle is used to handle all global allegrosql bind operations.
type BindHandle struct {
	sctx struct {
		sync.Mutex
		stochastikctx.Context
	}

	// bindInfo caches the allegrosql bind info from storage.
	//
	// The Mutex protects that there is only one goroutine changes the content
	// of atomic.Value.
	//
	// NOTE: Concurrent Value Write:
	//
	//    bindInfo.Lock()
	//    newCache := bindInfo.Value.Load()
	//    do the write operation on the newCache
	//    bindInfo.Value.CausetStore(newCache)
	//    bindInfo.Unlock()
	//
	// NOTE: Concurrent Value Read:
	//
	//    cache := bindInfo.Load().
	//    read the content
	//
	bindInfo struct {
		sync.Mutex
		atomic.Value
		BerolinaSQL         *BerolinaSQL.BerolinaSQL
		lastUFIDelateTime types.Time
	}

	// invalidBindRecordMap indicates the invalid bind records found during querying.
	// A record will be deleted from this map, after 2 bind-lease, after it is dropped from the ekv.
	invalidBindRecordMap tmpBindRecordMap

	// pendingVerifyBindRecordMap indicates the pending verify bind records that found during query.
	pendingVerifyBindRecordMap tmpBindRecordMap
}

// Lease influences the duration of loading bind info and handling invalid bind.
var Lease = 3 * time.Second

const (
	// TenantKey is the bindinfo tenant path that is saved to etcd.
	TenantKey = "/milevadb/bindinfo/tenant"
	// Prompt is the prompt for bindinfo tenant manager.
	Prompt = "bindinfo"
)

type bindRecordUFIDelate struct {
	bindRecord *BindRecord
	uFIDelateTime time.Time
}

// NewBindHandle creates a new BindHandle.
func NewBindHandle(ctx stochastikctx.Context) *BindHandle {
	handle := &BindHandle{}
	handle.sctx.Context = ctx
	handle.bindInfo.Value.CausetStore(make(cache, 32))
	handle.bindInfo.BerolinaSQL = BerolinaSQL.New()
	handle.invalidBindRecordMap.Value.CausetStore(make(map[string]*bindRecordUFIDelate))
	handle.invalidBindRecordMap.flushFunc = func(record *BindRecord) error {
		return handle.DropBindRecord(record.OriginalALLEGROSQL, record.EDB, &record.Bindings[0])
	}
	handle.pendingVerifyBindRecordMap.Value.CausetStore(make(map[string]*bindRecordUFIDelate))
	handle.pendingVerifyBindRecordMap.flushFunc = func(record *BindRecord) error {
		// BindALLEGROSQL has already been validated when coming here, so we use nil sctx parameter.
		return handle.AddBindRecord(nil, record)
	}
	return handle
}

// UFIDelate uFIDelates the global allegrosql bind cache.
func (h *BindHandle) UFIDelate(fullLoad bool) (err error) {
	h.bindInfo.Lock()
	lastUFIDelateTime := h.bindInfo.lastUFIDelateTime
	h.bindInfo.Unlock()

	allegrosql := "select original_sql, bind_sql, default_db, status, create_time, uFIDelate_time, charset, collation, source from allegrosql.bind_info"
	if !fullLoad {
		allegrosql += " where uFIDelate_time > \"" + lastUFIDelateTime.String() + "\""
	}
	// We need to apply the uFIDelates by order, wrong apply order of same original allegrosql may cause inconsistent state.
	allegrosql += " order by uFIDelate_time"

	// No need to acquire the stochastik context dagger for InterDircRestrictedALLEGROSQL, it
	// uses another background stochastik.
	rows, _, err := h.sctx.Context.(sqlexec.RestrictedALLEGROSQLInterlockingDirectorate).InterDircRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return err
	}

	// Make sure there is only one goroutine writes the cache.
	h.bindInfo.Lock()
	newCache := h.bindInfo.Value.Load().(cache).copy()
	defer func() {
		h.bindInfo.lastUFIDelateTime = lastUFIDelateTime
		h.bindInfo.Value.CausetStore(newCache)
		h.bindInfo.Unlock()
	}()

	for _, event := range rows {
		hash, spacetime, err := h.newBindRecord(event)
		// UFIDelate lastUFIDelateTime to the newest one.
		if spacetime.Bindings[0].UFIDelateTime.Compare(lastUFIDelateTime) > 0 {
			lastUFIDelateTime = spacetime.Bindings[0].UFIDelateTime
		}
		if err != nil {
			logutil.BgLogger().Info("uFIDelate bindinfo failed", zap.Error(err))
			continue
		}

		oldRecord := newCache.getBindRecord(hash, spacetime.OriginalALLEGROSQL, spacetime.EDB)
		newRecord := merge(oldRecord, spacetime).removeDeletedBindings()
		if len(newRecord.Bindings) > 0 {
			newCache.setBindRecord(hash, newRecord)
		} else {
			newCache.removeDeletedBindRecord(hash, newRecord)
		}
		uFIDelateMetrics(metrics.ScopeGlobal, oldRecord, newCache.getBindRecord(hash, spacetime.OriginalALLEGROSQL, spacetime.EDB), true)
	}
	return nil
}

// CreateBindRecord creates a BindRecord to the storage and the cache.
// It replaces all the exists bindings for the same normalized ALLEGROALLEGROSQL.
func (h *BindHandle) CreateBindRecord(sctx stochastikctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}

	exec, _ := h.sctx.Context.(sqlexec.ALLEGROSQLInterlockingDirectorate)
	h.sctx.Lock()
	_, err = exec.InterDircuteInternal(context.TODO(), "BEGIN")
	if err != nil {
		h.sctx.Unlock()
		return
	}

	normalizedALLEGROSQL := BerolinaSQL.DigestNormalized(record.OriginalALLEGROSQL)
	oldRecord := h.GetBindRecord(normalizedALLEGROSQL, record.OriginalALLEGROSQL, record.EDB)

	defer func() {
		if err != nil {
			_, err1 := exec.InterDircuteInternal(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		_, err = exec.InterDircuteInternal(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			return
		}

		// Make sure there is only one goroutine writes the cache and uses BerolinaSQL.
		h.bindInfo.Lock()
		if oldRecord != nil {
			h.removeBindRecord(normalizedALLEGROSQL, oldRecord)
		}
		h.appendBindRecord(normalizedALLEGROSQL, record)
		h.bindInfo.Unlock()
	}()

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}
	now := types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())), allegrosql.TypeTimestamp, 3)

	if oldRecord != nil {
		for _, binding := range oldRecord.Bindings {
			_, err1 = exec.InterDircuteInternal(context.TODO(), h.logicalDeleteBindInfoALLEGROSQL(record.OriginalALLEGROSQL, record.EDB, now, binding.BindALLEGROSQL))
			if err != nil {
				return err1
			}
		}
	}

	for i := range record.Bindings {
		record.Bindings[i].CreateTime = now
		record.Bindings[i].UFIDelateTime = now

		// insert the BindRecord to the storage.
		_, err = exec.InterDircuteInternal(context.TODO(), h.insertBindInfoALLEGROSQL(record.OriginalALLEGROSQL, record.EDB, record.Bindings[i]))
		if err != nil {
			return err
		}
	}
	return nil
}

// AddBindRecord adds a BindRecord to the storage and BindRecord to the cache.
func (h *BindHandle) AddBindRecord(sctx stochastikctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}

	oldRecord := h.GetBindRecord(BerolinaSQL.DigestNormalized(record.OriginalALLEGROSQL), record.OriginalALLEGROSQL, record.EDB)
	var duplicateBinding *Binding
	if oldRecord != nil {
		binding := oldRecord.FindBinding(record.Bindings[0].ID)
		if binding != nil {
			// There is already a binding with status `Using`, `PendingVerify` or `Rejected`, we could directly cancel the job.
			if record.Bindings[0].Status == PendingVerify {
				return nil
			}
			// Otherwise, we need to remove it before insert.
			duplicateBinding = binding
		}
	}

	exec, _ := h.sctx.Context.(sqlexec.ALLEGROSQLInterlockingDirectorate)
	h.sctx.Lock()
	_, err = exec.InterDircuteInternal(context.TODO(), "BEGIN")
	if err != nil {
		h.sctx.Unlock()
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.InterDircuteInternal(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		_, err = exec.InterDircuteInternal(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			return
		}

		// Make sure there is only one goroutine writes the cache and uses BerolinaSQL.
		h.bindInfo.Lock()
		h.appendBindRecord(BerolinaSQL.DigestNormalized(record.OriginalALLEGROSQL), record)
		h.bindInfo.Unlock()
	}()

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}

	if duplicateBinding != nil {
		_, err = exec.InterDircuteInternal(context.TODO(), h.deleteBindInfoALLEGROSQL(record.OriginalALLEGROSQL, record.EDB, duplicateBinding.BindALLEGROSQL))
		if err != nil {
			return err
		}
	}

	now := types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())), allegrosql.TypeTimestamp, 3)
	for i := range record.Bindings {
		if duplicateBinding != nil {
			record.Bindings[i].CreateTime = duplicateBinding.CreateTime
		} else {
			record.Bindings[i].CreateTime = now
		}
		record.Bindings[i].UFIDelateTime = now

		// insert the BindRecord to the storage.
		_, err = exec.InterDircuteInternal(context.TODO(), h.insertBindInfoALLEGROSQL(record.OriginalALLEGROSQL, record.EDB, record.Bindings[i]))
		if err != nil {
			return err
		}
	}
	return nil
}

// DropBindRecord drops a BindRecord to the storage and BindRecord int the cache.
func (h *BindHandle) DropBindRecord(originalALLEGROSQL, EDB string, binding *Binding) (err error) {
	exec, _ := h.sctx.Context.(sqlexec.ALLEGROSQLInterlockingDirectorate)
	h.sctx.Lock()
	_, err = exec.InterDircuteInternal(context.TODO(), "BEGIN")
	if err != nil {
		h.sctx.Unlock()
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.InterDircuteInternal(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		_, err = exec.InterDircuteInternal(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			return
		}

		record := &BindRecord{OriginalALLEGROSQL: originalALLEGROSQL, EDB: EDB}
		if binding != nil {
			record.Bindings = append(record.Bindings, *binding)
		}
		// Make sure there is only one goroutine writes the cache and uses BerolinaSQL.
		h.bindInfo.Lock()
		h.removeBindRecord(BerolinaSQL.DigestNormalized(originalALLEGROSQL), record)
		h.bindInfo.Unlock()
	}()

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}

	uFIDelateTs := types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())), allegrosql.TypeTimestamp, 3)

	bindALLEGROSQL := ""
	if binding != nil {
		bindALLEGROSQL = binding.BindALLEGROSQL
	}

	_, err = exec.InterDircuteInternal(context.TODO(), h.logicalDeleteBindInfoALLEGROSQL(originalALLEGROSQL, EDB, uFIDelateTs, bindALLEGROSQL))
	return err
}

// tmpBindRecordMap is used to temporarily save bind record changes.
// Those changes will be flushed into causetstore periodically.
type tmpBindRecordMap struct {
	sync.Mutex
	atomic.Value
	flushFunc func(record *BindRecord) error
}

// flushToStore calls flushFunc for items in tmpBindRecordMap and removes them with a delay.
func (tmpMap *tmpBindRecordMap) flushToStore() {
	tmpMap.Lock()
	defer tmpMap.Unlock()
	newMap := copyBindRecordUFIDelateMap(tmpMap.Load().(map[string]*bindRecordUFIDelate))
	for key, bindRecord := range newMap {
		if bindRecord.uFIDelateTime.IsZero() {
			err := tmpMap.flushFunc(bindRecord.bindRecord)
			if err != nil {
				logutil.BgLogger().Error("flush bind record failed", zap.Error(err))
			}
			bindRecord.uFIDelateTime = time.Now()
			continue
		}

		if time.Since(bindRecord.uFIDelateTime) > 6*time.Second {
			delete(newMap, key)
			uFIDelateMetrics(metrics.ScopeGlobal, bindRecord.bindRecord, nil, false)
		}
	}
	tmpMap.CausetStore(newMap)
}

// Add puts a BindRecord into tmpBindRecordMap.
func (tmpMap *tmpBindRecordMap) Add(bindRecord *BindRecord) {
	key := bindRecord.OriginalALLEGROSQL + ":" + bindRecord.EDB + ":" + bindRecord.Bindings[0].ID
	if _, ok := tmpMap.Load().(map[string]*bindRecordUFIDelate)[key]; ok {
		return
	}
	tmpMap.Lock()
	defer tmpMap.Unlock()
	if _, ok := tmpMap.Load().(map[string]*bindRecordUFIDelate)[key]; ok {
		return
	}
	newMap := copyBindRecordUFIDelateMap(tmpMap.Load().(map[string]*bindRecordUFIDelate))
	newMap[key] = &bindRecordUFIDelate{
		bindRecord: bindRecord,
	}
	tmpMap.CausetStore(newMap)
	uFIDelateMetrics(metrics.ScopeGlobal, nil, bindRecord, false)
}

// DropInvalidBindRecord executes the drop BindRecord tasks.
func (h *BindHandle) DropInvalidBindRecord() {
	h.invalidBindRecordMap.flushToStore()
}

// AddDropInvalidBindTask adds BindRecord which needs to be deleted into invalidBindRecordMap.
func (h *BindHandle) AddDropInvalidBindTask(invalidBindRecord *BindRecord) {
	h.invalidBindRecordMap.Add(invalidBindRecord)
}

// Size returns the size of bind info cache.
func (h *BindHandle) Size() int {
	size := 0
	for _, bindRecords := range h.bindInfo.Load().(cache) {
		size += len(bindRecords)
	}
	return size
}

// GetBindRecord returns the BindRecord of the (normdOrigALLEGROSQL,EDB) if BindRecord exist.
func (h *BindHandle) GetBindRecord(hash, normdOrigALLEGROSQL, EDB string) *BindRecord {
	return h.bindInfo.Load().(cache).getBindRecord(hash, normdOrigALLEGROSQL, EDB)
}

// GetAllBindRecord returns all bind records in cache.
func (h *BindHandle) GetAllBindRecord() (bindRecords []*BindRecord) {
	bindRecordMap := h.bindInfo.Load().(cache)
	for _, bindRecord := range bindRecordMap {
		bindRecords = append(bindRecords, bindRecord...)
	}
	return bindRecords
}

// newBindRecord builds BindRecord from a tuple in storage.
func (h *BindHandle) newBindRecord(event chunk.Row) (string, *BindRecord, error) {
	hint := Binding{
		BindALLEGROSQL:    event.GetString(1),
		Status:     event.GetString(3),
		CreateTime: event.GetTime(4),
		UFIDelateTime: event.GetTime(5),
		Charset:    event.GetString(6),
		DefCauslation:  event.GetString(7),
		Source:     event.GetString(8),
	}
	bindRecord := &BindRecord{
		OriginalALLEGROSQL: event.GetString(0),
		EDB:          event.GetString(2),
		Bindings:    []Binding{hint},
	}
	hash := BerolinaSQL.DigestNormalized(bindRecord.OriginalALLEGROSQL)
	h.sctx.Lock()
	defer h.sctx.Unlock()
	h.sctx.GetStochastikVars().CurrentDB = bindRecord.EDB
	err := bindRecord.prepareHints(h.sctx.Context)
	return hash, bindRecord, err
}

// appendBindRecord addes the BindRecord to the cache, all the stale BindRecords are
// removed from the cache after this operation.
func (h *BindHandle) appendBindRecord(hash string, spacetime *BindRecord) {
	newCache := h.bindInfo.Value.Load().(cache).copy()
	oldRecord := newCache.getBindRecord(hash, spacetime.OriginalALLEGROSQL, spacetime.EDB)
	newRecord := merge(oldRecord, spacetime)
	newCache.setBindRecord(hash, newRecord)
	h.bindInfo.Value.CausetStore(newCache)
	uFIDelateMetrics(metrics.ScopeGlobal, oldRecord, newRecord, false)
}

// removeBindRecord removes the BindRecord from the cache.
func (h *BindHandle) removeBindRecord(hash string, spacetime *BindRecord) {
	newCache := h.bindInfo.Value.Load().(cache).copy()
	oldRecord := newCache.getBindRecord(hash, spacetime.OriginalALLEGROSQL, spacetime.EDB)
	newCache.removeDeletedBindRecord(hash, spacetime)
	h.bindInfo.Value.CausetStore(newCache)
	uFIDelateMetrics(metrics.ScopeGlobal, oldRecord, newCache.getBindRecord(hash, spacetime.OriginalALLEGROSQL, spacetime.EDB), false)
}

// removeDeletedBindRecord removes the BindRecord which has same originALLEGROSQL and EDB with specified BindRecord.
func (c cache) removeDeletedBindRecord(hash string, spacetime *BindRecord) {
	spacetimes, ok := c[hash]
	if !ok {
		return
	}

	for i := len(spacetimes) - 1; i >= 0; i-- {
		if spacetimes[i].isSame(spacetime) {
			spacetimes[i] = spacetimes[i].remove(spacetime)
			if len(spacetimes[i].Bindings) == 0 {
				spacetimes = append(spacetimes[:i], spacetimes[i+1:]...)
			}
			if len(spacetimes) == 0 {
				delete(c, hash)
				return
			}
		}
	}
	c[hash] = spacetimes
}

func (c cache) setBindRecord(hash string, spacetime *BindRecord) {
	spacetimes := c[hash]
	for i := range spacetimes {
		if spacetimes[i].EDB == spacetime.EDB && spacetimes[i].OriginalALLEGROSQL == spacetime.OriginalALLEGROSQL {
			spacetimes[i] = spacetime
			return
		}
	}
	c[hash] = append(c[hash], spacetime)
}

func (c cache) copy() cache {
	newCache := make(cache, len(c))
	for k, v := range c {
		bindRecords := make([]*BindRecord, len(v))
		copy(bindRecords, v)
		newCache[k] = bindRecords
	}
	return newCache
}

func copyBindRecordUFIDelateMap(oldMap map[string]*bindRecordUFIDelate) map[string]*bindRecordUFIDelate {
	newMap := make(map[string]*bindRecordUFIDelate, len(oldMap))
	for k, v := range oldMap {
		newMap[k] = v
	}
	return newMap
}

func (c cache) getBindRecord(hash, normdOrigALLEGROSQL, EDB string) *BindRecord {
	bindRecords := c[hash]
	for _, bindRecord := range bindRecords {
		if bindRecord.OriginalALLEGROSQL == normdOrigALLEGROSQL && bindRecord.EDB == EDB {
			return bindRecord
		}
	}
	return nil
}

func (h *BindHandle) deleteBindInfoALLEGROSQL(normdOrigALLEGROSQL, EDB, bindALLEGROSQL string) string {
	return fmt.Sprintf(
		`DELETE FROM allegrosql.bind_info WHERE original_sql=%s AND default_db=%s AND bind_sql=%s`,
		memex.Quote(normdOrigALLEGROSQL),
		memex.Quote(EDB),
		memex.Quote(bindALLEGROSQL),
	)
}

func (h *BindHandle) insertBindInfoALLEGROSQL(orignalALLEGROSQL string, EDB string, info Binding) string {
	return fmt.Sprintf(`INSERT INTO allegrosql.bind_info VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		memex.Quote(orignalALLEGROSQL),
		memex.Quote(info.BindALLEGROSQL),
		memex.Quote(EDB),
		memex.Quote(info.Status),
		memex.Quote(info.CreateTime.String()),
		memex.Quote(info.UFIDelateTime.String()),
		memex.Quote(info.Charset),
		memex.Quote(info.DefCauslation),
		memex.Quote(info.Source),
	)
}

func (h *BindHandle) logicalDeleteBindInfoALLEGROSQL(originalALLEGROSQL, EDB string, uFIDelateTs types.Time, bindingALLEGROSQL string) string {
	allegrosql := fmt.Sprintf(`UFIDelATE allegrosql.bind_info SET status=%s,uFIDelate_time=%s WHERE original_sql=%s and default_db=%s`,
		memex.Quote(deleted),
		memex.Quote(uFIDelateTs.String()),
		memex.Quote(originalALLEGROSQL),
		memex.Quote(EDB))
	if bindingALLEGROSQL == "" {
		return allegrosql
	}
	return allegrosql + fmt.Sprintf(` and bind_sql = %s`, memex.Quote(bindingALLEGROSQL))
}

// CaptureBaselines is used to automatically capture plan baselines.
func (h *BindHandle) CaptureBaselines() {
	BerolinaSQL4Capture := BerolinaSQL.New()
	schemas, sqls := stmtsummary.StmtSummaryByDigestMap.GetMoreThanOnceSelect()
	for i := range sqls {
		stmt, err := BerolinaSQL4Capture.ParseOneStmt(sqls[i], "", "")
		if err != nil {
			logutil.BgLogger().Debug("parse ALLEGROALLEGROSQL failed", zap.String("ALLEGROALLEGROSQL", sqls[i]), zap.Error(err))
			continue
		}
		normalizedALLEGROSQL, digiest := BerolinaSQL.NormalizeDigest(sqls[i])
		dbName := utilBerolinaSQL.GetDefaultDB(stmt, schemas[i])
		if r := h.GetBindRecord(digiest, normalizedALLEGROSQL, dbName); r != nil && r.HasUsingBinding() {
			continue
		}
		h.sctx.Lock()
		h.sctx.GetStochastikVars().CurrentDB = schemas[i]
		oriIsolationRead := h.sctx.GetStochastikVars().IsolationReadEngines
		// TODO: support all engines plan hint in capture baselines.
		h.sctx.GetStochastikVars().IsolationReadEngines = map[ekv.StoreType]struct{}{ekv.EinsteinDB: {}}
		hints, err := getHintsForALLEGROSQL(h.sctx.Context, sqls[i])
		h.sctx.GetStochastikVars().IsolationReadEngines = oriIsolationRead
		h.sctx.Unlock()
		if err != nil {
			logutil.BgLogger().Debug("generate hints failed", zap.String("ALLEGROALLEGROSQL", sqls[i]), zap.Error(err))
			continue
		}
		bindALLEGROSQL := GenerateBindALLEGROSQL(context.TODO(), stmt, hints)
		if bindALLEGROSQL == "" {
			continue
		}
		charset, collation := h.sctx.GetStochastikVars().GetCharsetInfo()
		binding := Binding{
			BindALLEGROSQL:   bindALLEGROSQL,
			Status:    Using,
			Charset:   charset,
			DefCauslation: collation,
			Source:    Capture,
		}
		// We don't need to pass the `sctx` because the BindALLEGROSQL has been validated already.
		err = h.AddBindRecord(nil, &BindRecord{OriginalALLEGROSQL: normalizedALLEGROSQL, EDB: dbName, Bindings: []Binding{binding}})
		if err != nil {
			logutil.BgLogger().Info("capture baseline failed", zap.String("ALLEGROALLEGROSQL", sqls[i]), zap.Error(err))
		}
	}
}

func getHintsForALLEGROSQL(sctx stochastikctx.Context, allegrosql string) (string, error) {
	origVals := sctx.GetStochastikVars().UseCausetBaselines
	sctx.GetStochastikVars().UseCausetBaselines = false
	recordSets, err := sctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircuteInternal(context.TODO(), fmt.Sprintf("explain format='hint' %s", allegrosql))
	sctx.GetStochastikVars().UseCausetBaselines = origVals
	if len(recordSets) > 0 {
		defer terror.Log(recordSets[0].Close())
	}
	if err != nil {
		return "", err
	}
	chk := recordSets[0].NewChunk()
	err = recordSets[0].Next(context.TODO(), chk)
	if err != nil {
		return "", err
	}
	return chk.GetRow(0).GetString(0), nil
}

// GenerateBindALLEGROSQL generates binding sqls from stmt node and plan hints.
func GenerateBindALLEGROSQL(ctx context.Context, stmtNode ast.StmtNode, planHint string) string {
	// If would be nil for very simple cases such as point get, we do not need to evolve for them.
	if planHint == "" {
		return ""
	}
	paramChecker := &paramMarkerChecker{}
	stmtNode.Accept(paramChecker)
	// We need to evolve on current allegrosql, but we cannot restore values for paramMarkers yet,
	// so just ignore them now.
	if paramChecker.hasParamMarker {
		return ""
	}
	// We need to evolve plan based on the current allegrosql, not the original allegrosql which may have different parameters.
	// So here we would remove the hint and inject the current best plan hint.
	hint.BindHint(stmtNode, &hint.HintsSet{})
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := stmtNode.Restore(restoreCtx)
	if err != nil {
		logutil.Logger(ctx).Warn("Restore ALLEGROALLEGROSQL failed", zap.Error(err))
	}
	bindALLEGROSQL := sb.String()
	selectIdx := strings.Index(bindALLEGROSQL, "SELECT")
	// Remove possible `explain` prefix.
	bindALLEGROSQL = bindALLEGROSQL[selectIdx:]
	return strings.Replace(bindALLEGROSQL, "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
}

type paramMarkerChecker struct {
	hasParamMarker bool
}

func (e *paramMarkerChecker) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*driver.ParamMarkerExpr); ok {
		e.hasParamMarker = true
		return in, true
	}
	return in, false
}

func (e *paramMarkerChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// AddEvolveCausetTask adds the evolve plan task into memory cache. It would be flushed to causetstore periodically.
func (h *BindHandle) AddEvolveCausetTask(originalALLEGROSQL, EDB string, binding Binding) {
	br := &BindRecord{
		OriginalALLEGROSQL: originalALLEGROSQL,
		EDB:          EDB,
		Bindings:    []Binding{binding},
	}
	h.pendingVerifyBindRecordMap.Add(br)
}

// SaveEvolveTasksToStore saves the evolve task into causetstore.
func (h *BindHandle) SaveEvolveTasksToStore() {
	h.pendingVerifyBindRecordMap.flushToStore()
}

func getEvolveParameters(ctx stochastikctx.Context) (time.Duration, time.Time, time.Time, error) {
	allegrosql := fmt.Sprintf("select variable_name, variable_value from allegrosql.global_variables where variable_name in ('%s', '%s', '%s')",
		variable.MilevaDBEvolveCausetTaskMaxTime, variable.MilevaDBEvolveCausetTaskStartTime, variable.MilevaDBEvolveCausetTaskEndTime)
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLInterlockingDirectorate).InterDircRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	maxTime, startTimeStr, endTimeStr := int64(variable.DefMilevaDBEvolveCausetTaskMaxTime), variable.DefMilevaDBEvolveCausetTaskStartTime, variable.DefAutoAnalyzeEndTime
	for _, event := range rows {
		switch event.GetString(0) {
		case variable.MilevaDBEvolveCausetTaskMaxTime:
			maxTime, err = strconv.ParseInt(event.GetString(1), 10, 64)
			if err != nil {
				return 0, time.Time{}, time.Time{}, err
			}
		case variable.MilevaDBEvolveCausetTaskStartTime:
			startTimeStr = event.GetString(1)
		case variable.MilevaDBEvolveCausetTaskEndTime:
			endTimeStr = event.GetString(1)
		}
	}
	startTime, err := time.ParseInLocation(variable.FullDayTimeFormat, startTimeStr, time.UTC)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err

	}
	endTime, err := time.ParseInLocation(variable.FullDayTimeFormat, endTimeStr, time.UTC)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	return time.Duration(maxTime) * time.Second, startTime, endTime, nil
}

const (
	// acceptFactor is the factor to decide should we accept the pending verified plan.
	// A pending verified plan will be accepted if it performs at least `acceptFactor` times better than the accepted plans.
	acceptFactor = 1.5
	// nextVerifyDuration is the duration that we will retry the rejected plans.
	nextVerifyDuration = 7 * 24 * time.Hour
)

func (h *BindHandle) getOnePendingVerifyJob() (string, string, Binding) {
	cache := h.bindInfo.Value.Load().(cache)
	for _, bindRecords := range cache {
		for _, bindRecord := range bindRecords {
			for _, bind := range bindRecord.Bindings {
				if bind.Status == PendingVerify {
					return bindRecord.OriginalALLEGROSQL, bindRecord.EDB, bind
				}
				if bind.Status != Rejected {
					continue
				}
				dur, err := bind.SinceUFIDelateTime()
				// Should not happen.
				if err != nil {
					continue
				}
				// Rejected and retry it now.
				if dur > nextVerifyDuration {
					return bindRecord.OriginalALLEGROSQL, bindRecord.EDB, bind
				}
			}
		}
	}
	return "", "", Binding{}
}

func (h *BindHandle) getRunningDuration(sctx stochastikctx.Context, EDB, allegrosql string, maxTime time.Duration) (time.Duration, error) {
	ctx := context.TODO()
	if EDB != "" {
		_, err := sctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircuteInternal(ctx, fmt.Sprintf("use `%s`", EDB))
		if err != nil {
			return 0, err
		}
	}
	ctx, cancelFunc := context.WithCancel(ctx)
	timer := time.NewTimer(maxTime)
	resultChan := make(chan error)
	startTime := time.Now()
	go runALLEGROSQL(ctx, sctx, allegrosql, resultChan)
	select {
	case err := <-resultChan:
		cancelFunc()
		if err != nil {
			return 0, err
		}
		return time.Since(startTime), nil
	case <-timer.C:
		cancelFunc()
	}
	<-resultChan
	return -1, nil
}

func runALLEGROSQL(ctx context.Context, sctx stochastikctx.Context, allegrosql string, resultChan chan<- error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			resultChan <- fmt.Errorf("run allegrosql panicked: %v", string(buf))
		}
	}()
	recordSets, err := sctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircuteInternal(ctx, allegrosql)
	if err != nil {
		if len(recordSets) > 0 {
			terror.Call(recordSets[0].Close)
		}
		resultChan <- err
		return
	}
	recordSet := recordSets[0]
	chk := recordSets[0].NewChunk()
	for {
		err = recordSet.Next(ctx, chk)
		if err != nil || chk.NumRows() == 0 {
			break
		}
	}
	terror.Call(recordSets[0].Close)
	resultChan <- err
}

// HandleEvolveCausetTask tries to evolve one plan task.
// It only handle one tasks once because we want each task could use the latest parameters.
func (h *BindHandle) HandleEvolveCausetTask(sctx stochastikctx.Context, adminEvolve bool) error {
	originalALLEGROSQL, EDB, binding := h.getOnePendingVerifyJob()
	if originalALLEGROSQL == "" {
		return nil
	}
	maxTime, startTime, endTime, err := getEvolveParameters(sctx)
	if err != nil {
		return err
	}
	if maxTime == 0 || (!timeutil.WithinDayTimePeriod(startTime, endTime, time.Now()) && !adminEvolve) {
		return nil
	}
	sctx.GetStochastikVars().UseCausetBaselines = true
	acceptedCausetTime, err := h.getRunningDuration(sctx, EDB, binding.BindALLEGROSQL, maxTime)
	// If we just return the error to the caller, this job will be retried again and again and cause endless logs,
	// since it is still in the bind record. Now we just drop it and if it is actually retryable,
	// we will hope for that we can capture this evolve task again.
	if err != nil {
		return h.DropBindRecord(originalALLEGROSQL, EDB, &binding)
	}
	// If the accepted plan timeouts, it is hard to decide the timeout for verify plan.
	// Currently we simply mark the verify plan as `using` if it could run successfully within maxTime.
	if acceptedCausetTime > 0 {
		maxTime = time.Duration(float64(acceptedCausetTime) / acceptFactor)
	}
	sctx.GetStochastikVars().UseCausetBaselines = false
	verifyCausetTime, err := h.getRunningDuration(sctx, EDB, binding.BindALLEGROSQL, maxTime)
	if err != nil {
		return h.DropBindRecord(originalALLEGROSQL, EDB, &binding)
	}
	if verifyCausetTime < 0 {
		binding.Status = Rejected
	} else {
		binding.Status = Using
	}
	// We don't need to pass the `sctx` because the BindALLEGROSQL has been validated already.
	return h.AddBindRecord(nil, &BindRecord{OriginalALLEGROSQL: originalALLEGROSQL, EDB: EDB, Bindings: []Binding{binding}})
}

// Clear resets the bind handle. It is only used for test.
func (h *BindHandle) Clear() {
	h.bindInfo.Lock()
	h.bindInfo.CausetStore(make(cache))
	h.bindInfo.lastUFIDelateTime = types.ZeroTimestamp
	h.bindInfo.Unlock()
	h.invalidBindRecordMap.CausetStore(make(map[string]*bindRecordUFIDelate))
	h.pendingVerifyBindRecordMap.CausetStore(make(map[string]*bindRecordUFIDelate))
}

// FlushBindings flushes the BindRecord in temp maps to storage and loads them into cache.
func (h *BindHandle) FlushBindings() error {
	h.DropInvalidBindRecord()
	h.SaveEvolveTasksToStore()
	return h.UFIDelate(false)
}

// ReloadBindings clears existing binding cache and do a full load from allegrosql.bind_info.
// It is used to maintain consistency between cache and allegrosql.bind_info if the causet is deleted or truncated.
func (h *BindHandle) ReloadBindings() error {
	h.bindInfo.Lock()
	h.bindInfo.CausetStore(make(cache))
	h.bindInfo.lastUFIDelateTime = types.ZeroTimestamp
	h.bindInfo.Unlock()
	return h.UFIDelate(true)
}
