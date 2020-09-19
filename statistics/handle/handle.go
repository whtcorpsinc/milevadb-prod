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

package handle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// statsCache caches the blocks in memory for Handle.
type statsCache struct {
	blocks map[int64]*statistics.Block
	// version is the latest version of cache.
	version  uint64
	memUsage int64
}

// Handle can uFIDelate stats info periodically.
type Handle struct {
	mu struct {
		sync.Mutex
		ctx stochastikctx.Context
		// rateMap contains the error rate delta from feedback.
		rateMap errorRateDeltaMap
		// pid2tid is the map from partition ID to block ID.
		pid2tid map[int64]int64
		// schemaVersion is the version of information schemaReplicant when `pid2tid` is built.
		schemaVersion int64
	}

	// It can be read by multiple readers at the same time without acquiring lock, but it can be
	// written only after acquiring the lock.
	statsCache struct {
		sync.Mutex
		atomic.Value
		memTracker *memory.Tracker
	}

	restrictedExec sqlexec.RestrictedALLEGROSQLExecutor

	// dbsEventCh is a channel to notify a dbs operation has happened.
	// It is sent only by owner or the drop stats executor, and read by stats handle.
	dbsEventCh chan *soliton.Event
	// listHead contains all the stats collector required by stochastik.
	listHead *StochastikStatsDefCauslector
	// globalMap contains all the delta map from collectors when we dump them to KV.
	globalMap blockDeltaMap
	// feedback is used to causetstore query feedback info.
	feedback *statistics.QueryFeedbackMap

	lease atomic2.Duration
}

// Clear the statsCache, only for test.
func (h *Handle) Clear() {
	h.mu.Lock()
	h.statsCache.Lock()
	h.statsCache.CausetStore(statsCache{blocks: make(map[int64]*statistics.Block)})
	h.statsCache.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
	h.statsCache.Unlock()
	for len(h.dbsEventCh) > 0 {
		<-h.dbsEventCh
	}
	h.feedback = statistics.NewQueryFeedbackMap()
	h.mu.ctx.GetStochastikVars().InitChunkSize = 1
	h.mu.ctx.GetStochastikVars().MaxChunkSize = 1
	h.mu.ctx.GetStochastikVars().EnableChunkRPC = false
	h.mu.ctx.GetStochastikVars().SetProjectionConcurrency(0)
	h.listHead = &StochastikStatsDefCauslector{mapper: make(blockDeltaMap), rateMap: make(errorRateDeltaMap)}
	h.globalMap = make(blockDeltaMap)
	h.mu.rateMap = make(errorRateDeltaMap)
	h.mu.Unlock()
}

// NewHandle creates a Handle for uFIDelate stats.
func NewHandle(ctx stochastikctx.Context, lease time.Duration) *Handle {
	handle := &Handle{
		dbsEventCh: make(chan *soliton.Event, 100),
		listHead:   &StochastikStatsDefCauslector{mapper: make(blockDeltaMap), rateMap: make(errorRateDeltaMap)},
		globalMap:  make(blockDeltaMap),
		feedback:   statistics.NewQueryFeedbackMap(),
	}
	handle.lease.CausetStore(lease)
	// It is safe to use it concurrently because the exec won't touch the ctx.
	if exec, ok := ctx.(sqlexec.RestrictedALLEGROSQLExecutor); ok {
		handle.restrictedExec = exec
	}
	handle.statsCache.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
	handle.mu.ctx = ctx
	handle.mu.rateMap = make(errorRateDeltaMap)
	handle.statsCache.CausetStore(statsCache{blocks: make(map[int64]*statistics.Block)})
	return handle
}

// Lease returns the stats lease.
func (h *Handle) Lease() time.Duration {
	return h.lease.Load()
}

// SetLease sets the stats lease.
func (h *Handle) SetLease(lease time.Duration) {
	h.lease.CausetStore(lease)
}

// GetQueryFeedback gets the query feedback. It is only used in test.
func (h *Handle) GetQueryFeedback() *statistics.QueryFeedbackMap {
	defer func() {
		h.feedback = statistics.NewQueryFeedbackMap()
	}()
	return h.feedback
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// UFIDelate reads stats meta from causetstore and uFIDelates the stats map.
func (h *Handle) UFIDelate(is schemareplicant.SchemaReplicant) error {
	oldCache := h.statsCache.Load().(statsCache)
	lastVersion := oldCache.version
	// We need this because for two blocks, the smaller version may write later than the one with larger version.
	// Consider the case that there are two blocks A and B, their version and commit time is (A0, A1) and (B0, B1),
	// and A0 < B0 < B1 < A1. We will first read the stats of B, and uFIDelate the lastVersion to B0, but we cannot read
	// the block stats of A0 if we read stats that greater than lastVersion which is B0.
	// We can read the stats if the diff between commit time and version is less than three lease.
	offset := DurationToTS(3 * h.Lease())
	if oldCache.version >= offset {
		lastVersion = lastVersion - offset
	} else {
		lastVersion = 0
	}
	allegrosql := fmt.Sprintf("SELECT version, block_id, modify_count, count from allegrosql.stats_meta where version > %d order by version", lastVersion)
	rows, _, err := h.restrictedExec.ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}

	blocks := make([]*statistics.Block, 0, len(rows))
	deletedTableIDs := make([]int64, 0, len(rows))
	for _, event := range rows {
		version := event.GetUint64(0)
		physicalID := event.GetInt64(1)
		modifyCount := event.GetInt64(2)
		count := event.GetInt64(3)
		lastVersion = version
		h.mu.Lock()
		block, ok := h.getTableByPhysicalID(is, physicalID)
		h.mu.Unlock()
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats meta block, maybe it has been dropped", zap.Int64("ID", physicalID))
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		blockInfo := block.Meta()
		tbl, err := h.blockStatsFromStorage(blockInfo, physicalID, false, nil)
		// Error is not nil may mean that there are some dbs changes on this block, we will not uFIDelate it.
		if err != nil {
			logutil.BgLogger().Debug("error occurred when read block stats", zap.String("block", blockInfo.Name.O), zap.Error(err))
			continue
		}
		if tbl == nil {
			deletedTableIDs = append(deletedTableIDs, physicalID)
			continue
		}
		tbl.Version = version
		tbl.Count = count
		tbl.ModifyCount = modifyCount
		tbl.Name = getFullTableName(is, blockInfo)
		blocks = append(blocks, tbl)
	}
	h.uFIDelateStatsCache(oldCache.uFIDelate(blocks, deletedTableIDs, lastVersion))
	return nil
}

func (h *Handle) getTableByPhysicalID(is schemareplicant.SchemaReplicant, physicalID int64) (block.Block, bool) {
	if is.SchemaMetaVersion() != h.mu.schemaVersion {
		h.mu.schemaVersion = is.SchemaMetaVersion()
		h.mu.pid2tid = buildPartitionID2TableID(is)
	}
	if id, ok := h.mu.pid2tid[physicalID]; ok {
		return is.TableByID(id)
	}
	return is.TableByID(physicalID)
}

func buildPartitionID2TableID(is schemareplicant.SchemaReplicant) map[int64]int64 {
	mapper := make(map[int64]int64)
	for _, EDB := range is.AllSchemas() {
		tbls := EDB.Tables
		for _, tbl := range tbls {
			pi := tbl.GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, def := range pi.Definitions {
				mapper[def.ID] = tbl.ID
			}
		}
	}
	return mapper
}

// GetMemConsumed returns the mem size of statscache consumed
func (h *Handle) GetMemConsumed() (size int64) {
	size = h.statsCache.memTracker.BytesConsumed()
	return
}

// GetAllTableStatsMemUsage get all the mem usage with true block.
// only used by test.
func (h *Handle) GetAllTableStatsMemUsage() int64 {
	data := h.statsCache.Value.Load().(statsCache)
	cache := data.copy()
	allUsage := int64(0)
	for _, t := range cache.blocks {
		allUsage += t.MemoryUsage()
	}
	return allUsage
}

// GetTableStats retrieves the statistics block from cache, and the cache will be uFIDelated by a goroutine.
func (h *Handle) GetTableStats(tblInfo *perceptron.TableInfo) *statistics.Block {
	return h.GetPartitionStats(tblInfo, tblInfo.ID)
}

// GetPartitionStats retrieves the partition stats from cache.
func (h *Handle) GetPartitionStats(tblInfo *perceptron.TableInfo, pid int64) *statistics.Block {
	statsCache := h.statsCache.Load().(statsCache)
	tbl, ok := statsCache.blocks[pid]
	if !ok {
		tbl = statistics.PseudoTable(tblInfo)
		tbl.PhysicalID = pid
		h.uFIDelateStatsCache(statsCache.uFIDelate([]*statistics.Block{tbl}, nil, statsCache.version))
		return tbl
	}
	return tbl
}

// CanRuntimePrune indicates whether tbl support runtime prune for block and first partition id.
func (h *Handle) CanRuntimePrune(tid, p0Id int64) bool {
	if h == nil {
		return false
	}
	if tid == p0Id {
		return false
	}
	statsCache := h.statsCache.Load().(statsCache)
	_, tblExists := statsCache.blocks[tid]
	if tblExists {
		return true
	}
	_, partExists := statsCache.blocks[p0Id]
	if !partExists {
		return true
	}
	return false
}

func (h *Handle) uFIDelateStatsCache(newCache statsCache) {
	h.statsCache.Lock()
	oldCache := h.statsCache.Load().(statsCache)
	if oldCache.version <= newCache.version {
		h.statsCache.memTracker.Consume(newCache.memUsage - oldCache.memUsage)
		h.statsCache.CausetStore(newCache)
	}
	h.statsCache.Unlock()
}

func (sc statsCache) copy() statsCache {
	newCache := statsCache{blocks: make(map[int64]*statistics.Block, len(sc.blocks)),
		version:  sc.version,
		memUsage: sc.memUsage}
	for k, v := range sc.blocks {
		newCache.blocks[k] = v
	}
	return newCache
}

//initMemoryUsage calc total memory usage of statsCache and set statsCache.memUsage
//should be called after the blocks and their stats are initilazed
func (sc statsCache) initMemoryUsage() {
	sum := int64(0)
	for _, tb := range sc.blocks {
		sum += tb.MemoryUsage()
	}
	sc.memUsage = sum
	return
}

// uFIDelate uFIDelates the statistics block cache using copy on write.
func (sc statsCache) uFIDelate(blocks []*statistics.Block, deletedIDs []int64, newVersion uint64) statsCache {
	newCache := sc.copy()
	newCache.version = newVersion
	for _, tbl := range blocks {
		id := tbl.PhysicalID
		if ptbl, ok := newCache.blocks[id]; ok {
			newCache.memUsage -= ptbl.MemoryUsage()
		}
		newCache.blocks[id] = tbl
		newCache.memUsage += tbl.MemoryUsage()
	}
	for _, id := range deletedIDs {
		if ptbl, ok := newCache.blocks[id]; ok {
			newCache.memUsage -= ptbl.MemoryUsage()
		}
		delete(newCache.blocks, id)
	}
	return newCache
}

// LoadNeededHistograms will load histograms for those needed columns.
func (h *Handle) LoadNeededHistograms() (err error) {
	defcaus := statistics.HistogramNeededDeferredCausets.AllDefCauss()
	reader, err := h.getStatsReader(nil)
	if err != nil {
		return err
	}

	defer func() {
		err1 := h.releaseStatsReader(reader)
		if err1 != nil && err == nil {
			err = err1
		}
	}()

	for _, col := range defcaus {
		statsCache := h.statsCache.Load().(statsCache)
		tbl, ok := statsCache.blocks[col.TableID]
		if !ok {
			continue
		}
		tbl = tbl.Copy()
		c, ok := tbl.DeferredCausets[col.DeferredCausetID]
		if !ok || c.Len() > 0 {
			statistics.HistogramNeededDeferredCausets.Delete(col)
			continue
		}
		hg, err := h.histogramFromStorage(reader, col.TableID, c.ID, &c.Info.FieldType, c.NDV, 0, c.LastUFIDelateVersion, c.NullCount, c.TotDefCausSize, c.Correlation)
		if err != nil {
			return errors.Trace(err)
		}
		cms, err := h.cmSketchFromStorage(reader, col.TableID, 0, col.DeferredCausetID)
		if err != nil {
			return errors.Trace(err)
		}
		tbl.DeferredCausets[c.ID] = &statistics.DeferredCauset{
			PhysicalID: col.TableID,
			Histogram:  *hg,
			Info:       c.Info,
			CMSketch:   cms,
			Count:      int64(hg.TotalRowCount()),
			IsHandle:   c.IsHandle,
		}
		h.uFIDelateStatsCache(statsCache.uFIDelate([]*statistics.Block{tbl}, nil, statsCache.version))
		statistics.HistogramNeededDeferredCausets.Delete(col)
	}
	return nil
}

// LastUFIDelateVersion gets the last uFIDelate version.
func (h *Handle) LastUFIDelateVersion() uint64 {
	return h.statsCache.Load().(statsCache).version
}

// SetLastUFIDelateVersion sets the last uFIDelate version.
func (h *Handle) SetLastUFIDelateVersion(version uint64) {
	statsCache := h.statsCache.Load().(statsCache)
	h.uFIDelateStatsCache(statsCache.uFIDelate(nil, nil, version))
}

// FlushStats flushes the cached stats uFIDelate into causetstore.
func (h *Handle) FlushStats() {
	for len(h.dbsEventCh) > 0 {
		e := <-h.dbsEventCh
		if err := h.HandleDBSEvent(e); err != nil {
			logutil.BgLogger().Debug("[stats] handle dbs event fail", zap.Error(err))
		}
	}
	if err := h.DumpStatsDeltaToKV(DumpAll); err != nil {
		logutil.BgLogger().Debug("[stats] dump stats delta fail", zap.Error(err))
	}
	if err := h.DumpStatsFeedbackToKV(); err != nil {
		logutil.BgLogger().Debug("[stats] dump stats feedback fail", zap.Error(err))
	}
}

func (h *Handle) cmSketchFromStorage(reader *statsReader, tblID int64, isIndex, histID int64) (_ *statistics.CMSketch, err error) {
	selALLEGROSQL := fmt.Sprintf("select cm_sketch from allegrosql.stats_histograms where block_id = %d and is_index = %d and hist_id = %d", tblID, isIndex, histID)
	rows, _, err := reader.read(selALLEGROSQL)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	selALLEGROSQL = fmt.Sprintf("select HIGH_PRIORITY value, count from allegrosql.stats_top_n where block_id = %d and is_index = %d and hist_id = %d", tblID, isIndex, histID)
	topNRows, _, err := reader.read(selALLEGROSQL)
	if err != nil {
		return nil, err
	}
	return statistics.DecodeCMSketch(rows[0].GetBytes(0), topNRows)
}

func (h *Handle) indexStatsFromStorage(reader *statsReader, event chunk.Row, block *statistics.Block, blockInfo *perceptron.TableInfo) error {
	histID := event.GetInt64(2)
	distinct := event.GetInt64(3)
	histVer := event.GetUint64(4)
	nullCount := event.GetInt64(5)
	idx := block.Indices[histID]
	errorRate := statistics.ErrorRate{}
	flag := event.GetInt64(8)
	lastAnalyzePos := event.GetCauset(10, types.NewFieldType(allegrosql.TypeBlob))
	if statistics.IsAnalyzed(flag) && !reader.isHistory() {
		h.mu.rateMap.clear(block.PhysicalID, histID, true)
	} else if idx != nil {
		errorRate = idx.ErrorRate
	}
	for _, idxInfo := range blockInfo.Indices {
		if histID != idxInfo.ID {
			continue
		}
		if idx == nil || idx.LastUFIDelateVersion < histVer {
			hg, err := h.histogramFromStorage(reader, block.PhysicalID, histID, types.NewFieldType(allegrosql.TypeBlob), distinct, 1, histVer, nullCount, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(reader, block.PhysicalID, 1, idxInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			idx = &statistics.Index{Histogram: *hg, CMSketch: cms, Info: idxInfo, ErrorRate: errorRate, StatsVer: event.GetInt64(7), Flag: flag}
			lastAnalyzePos.Copy(&idx.LastAnalyzePos)
		}
		break
	}
	if idx != nil {
		block.Indices[histID] = idx
	} else {
		logutil.BgLogger().Debug("we cannot find index id in block info. It may be deleted.", zap.Int64("indexID", histID), zap.String("block", blockInfo.Name.O))
	}
	return nil
}

func (h *Handle) columnStatsFromStorage(reader *statsReader, event chunk.Row, block *statistics.Block, blockInfo *perceptron.TableInfo, loadAll bool) error {
	histID := event.GetInt64(2)
	distinct := event.GetInt64(3)
	histVer := event.GetUint64(4)
	nullCount := event.GetInt64(5)
	totDefCausSize := event.GetInt64(6)
	correlation := event.GetFloat64(9)
	lastAnalyzePos := event.GetCauset(10, types.NewFieldType(allegrosql.TypeBlob))
	col := block.DeferredCausets[histID]
	errorRate := statistics.ErrorRate{}
	flag := event.GetInt64(8)
	if statistics.IsAnalyzed(flag) && !reader.isHistory() {
		h.mu.rateMap.clear(block.PhysicalID, histID, false)
	} else if col != nil {
		errorRate = col.ErrorRate
	}
	for _, colInfo := range blockInfo.DeferredCausets {
		if histID != colInfo.ID {
			continue
		}
		isHandle := blockInfo.PKIsHandle && allegrosql.HasPriKeyFlag(colInfo.Flag)
		// We will not load buckets if:
		// 1. Lease > 0, and:
		// 2. this column is not handle, and:
		// 3. the column doesn't has buckets before, and:
		// 4. loadAll is false.
		notNeedLoad := h.Lease() > 0 &&
			!isHandle &&
			(col == nil || col.Len() == 0 && col.LastUFIDelateVersion < histVer) &&
			!loadAll
		if notNeedLoad {
			count, err := h.columnCountFromStorage(reader, block.PhysicalID, histID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.DeferredCauset{
				PhysicalID: block.PhysicalID,
				Histogram:  *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totDefCausSize),
				Info:       colInfo,
				Count:      count + nullCount,
				ErrorRate:  errorRate,
				IsHandle:   blockInfo.PKIsHandle && allegrosql.HasPriKeyFlag(colInfo.Flag),
				Flag:       flag,
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			col.Histogram.Correlation = correlation
			break
		}
		if col == nil || col.LastUFIDelateVersion < histVer || loadAll {
			hg, err := h.histogramFromStorage(reader, block.PhysicalID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totDefCausSize, correlation)
			if err != nil {
				return errors.Trace(err)
			}
			cms, err := h.cmSketchFromStorage(reader, block.PhysicalID, 0, colInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			col = &statistics.DeferredCauset{
				PhysicalID: block.PhysicalID,
				Histogram:  *hg,
				Info:       colInfo,
				CMSketch:   cms,
				Count:      int64(hg.TotalRowCount()),
				ErrorRate:  errorRate,
				IsHandle:   blockInfo.PKIsHandle && allegrosql.HasPriKeyFlag(colInfo.Flag),
				Flag:       flag,
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			break
		}
		if col.TotDefCausSize != totDefCausSize {
			newDefCaus := *col
			newDefCaus.TotDefCausSize = totDefCausSize
			col = &newDefCaus
		}
		break
	}
	if col != nil {
		block.DeferredCausets[col.ID] = col
	} else {
		// If we didn't find a DeferredCauset or Index in blockInfo, we won't load the histogram for it.
		// But don't worry, next lease the dbs will be uFIDelated, and we will load a same block for two times to
		// avoid error.
		logutil.BgLogger().Debug("we cannot find column in block info now. It may be deleted", zap.Int64("colID", histID), zap.String("block", blockInfo.Name.O))
	}
	return nil
}

// blockStatsFromStorage loads block stats info from storage.
func (h *Handle) blockStatsFromStorage(blockInfo *perceptron.TableInfo, physicalID int64, loadAll bool, historyStatsExec sqlexec.RestrictedALLEGROSQLExecutor) (_ *statistics.Block, err error) {
	reader, err := h.getStatsReader(historyStatsExec)
	if err != nil {
		return nil, err
	}
	defer func() {
		err1 := h.releaseStatsReader(reader)
		if err == nil && err1 != nil {
			err = err1
		}
	}()
	block, ok := h.statsCache.Load().(statsCache).blocks[physicalID]
	// If block stats is pseudo, we also need to copy it, since we will use the column stats when
	// the average error rate of it is small.
	if !ok || historyStatsExec != nil {
		histDefCausl := statistics.HistDefCausl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			DeferredCausets:        make(map[int64]*statistics.DeferredCauset, len(blockInfo.DeferredCausets)),
			Indices:        make(map[int64]*statistics.Index, len(blockInfo.Indices)),
		}
		block = &statistics.Block{
			HistDefCausl: histDefCausl,
		}
	} else {
		// We copy it before writing to avoid race.
		block = block.Copy()
	}
	block.Pseudo = false
	selALLEGROSQL := fmt.Sprintf("select block_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from allegrosql.stats_histograms where block_id = %d", physicalID)
	rows, _, err := reader.read(selALLEGROSQL)
	// Check deleted block.
	if err != nil || len(rows) == 0 {
		return nil, nil
	}
	for _, event := range rows {
		if event.GetInt64(1) > 0 {
			err = h.indexStatsFromStorage(reader, event, block, blockInfo)
		} else {
			err = h.columnStatsFromStorage(reader, event, block, blockInfo, loadAll)
		}
		if err != nil {
			return nil, err
		}
	}
	return h.extendedStatsFromStorage(reader, block, physicalID, loadAll)
}

func (h *Handle) extendedStatsFromStorage(reader *statsReader, block *statistics.Block, physicalID int64, loadAll bool) (*statistics.Block, error) {
	lastVersion := uint64(0)
	if block.ExtendedStats != nil && !loadAll {
		lastVersion = block.ExtendedStats.LastUFIDelateVersion
	} else {
		block.ExtendedStats = statistics.NewExtendedStatsDefCausl()
	}
	allegrosql := fmt.Sprintf("select stats_name, EDB, status, type, column_ids, scalar_stats, blob_stats, version from allegrosql.stats_extended where block_id = %d and status in (%d, %d) and version > %d", physicalID, StatsStatusAnalyzed, StatsStatusDeleted, lastVersion)
	rows, _, err := reader.read(allegrosql)
	if err != nil || len(rows) == 0 {
		return block, nil
	}
	for _, event := range rows {
		lastVersion = mathutil.MaxUint64(lastVersion, event.GetUint64(7))
		key := statistics.ExtendedStatsKey{
			StatsName: event.GetString(0),
			EDB:        event.GetString(1),
		}
		status := uint8(event.GetInt64(2))
		if status == StatsStatusDeleted {
			delete(block.ExtendedStats.Stats, key)
		} else {
			item := &statistics.ExtendedStatsItem{
				Tp:         uint8(event.GetInt64(3)),
				ScalarVals: event.GetFloat64(5),
				StringVals: event.GetString(6),
			}
			colIDs := event.GetString(4)
			err := json.Unmarshal([]byte(colIDs), &item.DefCausIDs)
			if err != nil {
				logutil.BgLogger().Debug("decode column IDs failed", zap.String("column_ids", colIDs), zap.Error(err))
				return nil, err
			}
			block.ExtendedStats.Stats[key] = item
		}
	}
	block.ExtendedStats.LastUFIDelateVersion = lastVersion
	return block, nil
}

// SaveStatsToStorage saves the stats to storage.
func (h *Handle) SaveStatsToStorage(blockID int64, count int64, isIndex int, hg *statistics.Histogram, cms *statistics.CMSketch, isAnalyzed int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor)
	_, err = exec.Execute(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(context.Background(), exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}

	version := txn.StartTS()
	sqls := make([]string, 0, 4)
	// If the count is less than 0, then we do not want to uFIDelate the modify count and count.
	if count >= 0 {
		sqls = append(sqls, fmt.Sprintf("replace into allegrosql.stats_meta (version, block_id, count) values (%d, %d, %d)", version, blockID, count))
	} else {
		sqls = append(sqls, fmt.Sprintf("uFIDelate allegrosql.stats_meta set version = %d where block_id = %d", version, blockID))
	}
	data, err := statistics.EncodeCMSketchWithoutTopN(cms)
	if err != nil {
		return
	}
	// Delete outdated data
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_top_n where block_id = %d and is_index = %d and hist_id = %d", blockID, isIndex, hg.ID))
	for _, meta := range cms.TopN() {
		sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_top_n (block_id, is_index, hist_id, value, count) values (%d, %d, %d, X'%X', %d)", blockID, isIndex, hg.ID, meta.Data, meta.Count))
	}
	flag := 0
	if isAnalyzed == 1 {
		flag = statistics.AnalyzeFlag
	}
	sqls = append(sqls, fmt.Sprintf("replace into allegrosql.stats_histograms (block_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, flag, correlation) values (%d, %d, %d, %d, %d, %d, X'%X', %d, %d, %d, %f)",
		blockID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, data, hg.TotDefCausSize, statistics.CurStatsVersion, flag, hg.Correlation))
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_buckets where block_id = %d and is_index = %d and hist_id = %d", blockID, isIndex, hg.ID))
	sc := h.mu.ctx.GetStochastikVars().StmtCtx
	var lastAnalyzePos []byte
	for i := range hg.Buckets {
		count := hg.Buckets[i].Count
		if i > 0 {
			count -= hg.Buckets[i-1].Count
		}
		var upperBound types.Causet
		upperBound, err = hg.GetUpper(i).ConvertTo(sc, types.NewFieldType(allegrosql.TypeBlob))
		if err != nil {
			return
		}
		if i == len(hg.Buckets)-1 {
			lastAnalyzePos = upperBound.GetBytes()
		}
		var lowerBound types.Causet
		lowerBound, err = hg.GetLower(i).ConvertTo(sc, types.NewFieldType(allegrosql.TypeBlob))
		if err != nil {
			return
		}
		sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_buckets(block_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound) values(%d, %d, %d, %d, %d, %d, X'%X', X'%X')", blockID, isIndex, hg.ID, i, count, hg.Buckets[i].Repeat, lowerBound.GetBytes(), upperBound.GetBytes()))
	}
	if isAnalyzed == 1 && len(lastAnalyzePos) > 0 {
		sqls = append(sqls, fmt.Sprintf("uFIDelate allegrosql.stats_histograms set last_analyze_pos = X'%X' where block_id = %d and is_index = %d and hist_id = %d", lastAnalyzePos, blockID, isIndex, hg.ID))
	}
	return execALLEGROSQLs(context.Background(), exec, sqls)
}

// SaveMetaToStorage will save stats_meta to storage.
func (h *Handle) SaveMetaToStorage(blockID, count, modifyCount int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor)
	_, err = exec.Execute(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	var allegrosql string
	version := txn.StartTS()
	allegrosql = fmt.Sprintf("replace into allegrosql.stats_meta (version, block_id, count, modify_count) values (%d, %d, %d, %d)", version, blockID, count, modifyCount)
	_, err = exec.Execute(ctx, allegrosql)
	return
}

func (h *Handle) histogramFromStorage(reader *statsReader, blockID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64, totDefCausSize int64, corr float64) (_ *statistics.Histogram, err error) {
	selALLEGROSQL := fmt.Sprintf("select count, repeats, lower_bound, upper_bound from allegrosql.stats_buckets where block_id = %d and is_index = %d and hist_id = %d order by bucket_id", blockID, isIndex, colID)
	rows, fields, err := reader.read(selALLEGROSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := statistics.NewHistogram(colID, distinct, nullCount, ver, tp, bucketSize, totDefCausSize)
	hg.Correlation = corr
	totalCount := int64(0)
	for i := 0; i < bucketSize; i++ {
		count := rows[i].GetInt64(0)
		repeats := rows[i].GetInt64(1)
		var upperBound, lowerBound types.Causet
		if isIndex == 1 {
			lowerBound = rows[i].GetCauset(2, &fields[2].DeferredCauset.FieldType)
			upperBound = rows[i].GetCauset(3, &fields[3].DeferredCauset.FieldType)
		} else {
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			d := rows[i].GetCauset(2, &fields[2].DeferredCauset.FieldType)
			lowerBound, err = d.ConvertTo(sc, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetCauset(3, &fields[3].DeferredCauset.FieldType)
			upperBound, err = d.ConvertTo(sc, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		totalCount += count
		hg.AppendBucket(&lowerBound, &upperBound, totalCount, repeats)
	}
	hg.PreCalculateScalar()
	return hg, nil
}

func (h *Handle) columnCountFromStorage(reader *statsReader, blockID, colID int64) (int64, error) {
	selALLEGROSQL := fmt.Sprintf("select sum(count) from allegrosql.stats_buckets where block_id = %d and is_index = %d and hist_id = %d", blockID, 0, colID)
	rows, _, err := reader.read(selALLEGROSQL)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if rows[0].IsNull(0) {
		return 0, nil
	}
	return rows[0].GetMyDecimal(0).ToInt()
}

func (h *Handle) statsMetaByTableIDFromStorage(blockID int64, historyStatsExec sqlexec.RestrictedALLEGROSQLExecutor) (version uint64, modifyCount, count int64, err error) {
	selALLEGROSQL := fmt.Sprintf("SELECT version, modify_count, count from allegrosql.stats_meta where block_id = %d order by version", blockID)
	var rows []chunk.Row
	if historyStatsExec == nil {
		rows, _, err = h.restrictedExec.ExecRestrictedALLEGROSQL(selALLEGROSQL)
	} else {
		rows, _, err = historyStatsExec.ExecRestrictedALLEGROSQLWithSnapshot(selALLEGROSQL)
	}
	if err != nil || len(rows) == 0 {
		return
	}
	version = rows[0].GetUint64(0)
	modifyCount = rows[0].GetInt64(1)
	count = rows[0].GetInt64(2)
	return
}

// statsReader is used for simplify code that needs to read system blocks in different sqls
// but requires the same transactions.
type statsReader struct {
	ctx     stochastikctx.Context
	history sqlexec.RestrictedALLEGROSQLExecutor
}

func (sr *statsReader) read(allegrosql string) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	if sr.history != nil {
		return sr.history.ExecRestrictedALLEGROSQLWithSnapshot(allegrosql)
	}
	rc, err := sr.ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), allegrosql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return nil, nil, err
	}
	for {
		req := rc[0].NewChunk()
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return nil, nil, err
		}
		if req.NumRows() == 0 {
			break
		}
		for i := 0; i < req.NumRows(); i++ {
			rows = append(rows, req.GetRow(i))
		}
	}
	return rows, rc[0].Fields(), nil
}

func (sr *statsReader) isHistory() bool {
	return sr.history != nil
}

func (h *Handle) getStatsReader(history sqlexec.RestrictedALLEGROSQLExecutor) (*statsReader, error) {
	failpoint.Inject("mockGetStatsReaderFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail genStatsReader error"))
		}
	})
	if history != nil {
		return &statsReader{history: history}, nil
	}
	h.mu.Lock()
	_, err := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), "begin")
	if err != nil {
		return nil, err
	}
	return &statsReader{ctx: h.mu.ctx}, nil
}

func (h *Handle) releaseStatsReader(reader *statsReader) error {
	if reader.history != nil {
		return nil
	}
	_, err := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), "commit")
	h.mu.Unlock()
	return err
}

const (
	// StatsStatusInited is the status for extended stats which are just registered but have not been analyzed yet.
	StatsStatusInited uint8 = iota
	// StatsStatusAnalyzed is the status for extended stats which have been collected in analyze.
	StatsStatusAnalyzed
	// StatsStatusDeleted is the status for extended stats which were dropped. These "deleted" records would be removed from storage by GCStats().
	StatsStatusDeleted
)

// InsertExtendedStats inserts a record into allegrosql.stats_extended and uFIDelate version in allegrosql.stats_meta.
func (h *Handle) InsertExtendedStats(statsName, EDB string, colIDs []int64, tp int, blockID int64, ifNotExists bool) (err error) {
	bytes, err := json.Marshal(colIDs)
	if err != nil {
		return errors.Trace(err)
	}
	strDefCausIDs := string(bytes)
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor)
	_, err = exec.Execute(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	allegrosql := fmt.Sprintf("INSERT INTO allegrosql.stats_extended(stats_name, EDB, type, block_id, column_ids, version, status) VALUES ('%s', '%s', %d, %d, '%s', %d, %d)", statsName, EDB, tp, blockID, strDefCausIDs, version, StatsStatusInited)
	_, err = exec.Execute(ctx, allegrosql)
	// Key exists, but `if not exists` is specified, so we ignore this error.
	if ekv.ErrKeyExists.Equal(err) && ifNotExists {
		err = nil
	}
	return
}

// MarkExtendedStatsDeleted uFIDelate the status of allegrosql.stats_extended to be `deleted` and the version of allegrosql.stats_meta.
func (h *Handle) MarkExtendedStatsDeleted(statsName, EDB string, blockID int64) (err error) {
	if blockID < 0 {
		allegrosql := fmt.Sprintf("SELECT block_id FROM allegrosql.stats_extended WHERE stats_name = '%s' and EDB = '%s'", statsName, EDB)
		rows, _, err := h.restrictedExec.ExecRestrictedALLEGROSQL(allegrosql)
		if err != nil {
			return errors.Trace(err)
		}
		if len(rows) == 0 {
			return nil
		}
		blockID = rows[0].GetInt64(0)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor)
	_, err = exec.Execute(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	sqls := make([]string, 2)
	sqls[0] = fmt.Sprintf("UFIDelATE allegrosql.stats_extended SET version = %d, status = %d WHERE stats_name = '%s' and EDB = '%s'", version, StatsStatusDeleted, statsName, EDB)
	sqls[1] = fmt.Sprintf("UFIDelATE allegrosql.stats_meta SET version = %d WHERE block_id = %d", version, blockID)
	return execALLEGROSQLs(ctx, exec, sqls)
}

// ReloadExtendedStatistics drops the cache for extended statistics and reload data from allegrosql.stats_extended.
func (h *Handle) ReloadExtendedStatistics() error {
	reader, err := h.getStatsReader(nil)
	if err != nil {
		return err
	}
	oldCache := h.statsCache.Load().(statsCache)
	blocks := make([]*statistics.Block, 0, len(oldCache.blocks))
	for physicalID, tbl := range oldCache.blocks {
		t, err := h.extendedStatsFromStorage(reader, tbl.Copy(), physicalID, true)
		if err != nil {
			return err
		}
		blocks = append(blocks, t)
	}
	err = h.releaseStatsReader(reader)
	if err != nil {
		return err
	}
	// Note that this uFIDelate may fail when the statsCache.version has been modified by others.
	h.uFIDelateStatsCache(oldCache.uFIDelate(blocks, nil, oldCache.version))
	return nil
}

// BuildExtendedStats build extended stats for column groups if needed based on the column samples.
func (h *Handle) BuildExtendedStats(blockID int64, defcaus []*perceptron.DeferredCausetInfo, collectors []*statistics.SampleDefCauslector) (*statistics.ExtendedStatsDefCausl, error) {
	allegrosql := fmt.Sprintf("SELECT stats_name, EDB, type, column_ids FROM allegrosql.stats_extended WHERE block_id = %d and status in (%d, %d)", blockID, StatsStatusAnalyzed, StatsStatusInited)
	rows, _, err := h.restrictedExec.ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	statsDefCausl := statistics.NewExtendedStatsDefCausl()
	for _, event := range rows {
		key := statistics.ExtendedStatsKey{
			StatsName: event.GetString(0),
			EDB:        event.GetString(1),
		}
		item := &statistics.ExtendedStatsItem{Tp: uint8(event.GetInt64(2))}
		colIDs := event.GetString(3)
		err := json.Unmarshal([]byte(colIDs), &item.DefCausIDs)
		if err != nil {
			logutil.BgLogger().Error("invalid column_ids in allegrosql.stats_extended, skip collecting extended stats for this event", zap.String("column_ids", colIDs), zap.Error(err))
			continue
		}
		item = h.fillExtendedStatsItemVals(item, defcaus, collectors)
		if item != nil {
			statsDefCausl.Stats[key] = item
		}
	}
	if len(statsDefCausl.Stats) == 0 {
		return nil, nil
	}
	return statsDefCausl, nil
}

func (h *Handle) fillExtendedStatsItemVals(item *statistics.ExtendedStatsItem, defcaus []*perceptron.DeferredCausetInfo, collectors []*statistics.SampleDefCauslector) *statistics.ExtendedStatsItem {
	switch item.Tp {
	case ast.StatsTypeCardinality, ast.StatsTypeDependency:
		return nil
	case ast.StatsTypeCorrelation:
		return h.fillExtStatsCorrVals(item, defcaus, collectors)
	}
	return nil
}

func (h *Handle) fillExtStatsCorrVals(item *statistics.ExtendedStatsItem, defcaus []*perceptron.DeferredCausetInfo, collectors []*statistics.SampleDefCauslector) *statistics.ExtendedStatsItem {
	colOffsets := make([]int, 0, 2)
	for _, id := range item.DefCausIDs {
		for i, col := range defcaus {
			if col.ID == id {
				colOffsets = append(colOffsets, i)
				break
			}
		}
	}
	if len(colOffsets) != 2 {
		return nil
	}
	// samplesX and samplesY are in order of handle, i.e, their SampleItem.Ordinals are in order.
	samplesX := collectors[colOffsets[0]].Samples
	// We would modify Ordinal of samplesY, so we make a deep copy.
	samplesY := statistics.CopySampleItems(collectors[colOffsets[1]].Samples)
	sampleNum := len(samplesX)
	if sampleNum == 1 {
		item.ScalarVals = float64(1)
		return item
	}
	h.mu.Lock()
	sc := h.mu.ctx.GetStochastikVars().StmtCtx
	h.mu.Unlock()
	var err error
	samplesX, err = statistics.SortSampleItems(sc, samplesX)
	if err != nil {
		return nil
	}
	samplesYInXOrder := make([]*statistics.SampleItem, sampleNum)
	for i, itemX := range samplesX {
		itemY := samplesY[itemX.Ordinal]
		itemY.Ordinal = i
		samplesYInXOrder[i] = itemY
	}
	samplesYInYOrder, err := statistics.SortSampleItems(sc, samplesYInXOrder)
	if err != nil {
		return nil
	}
	var corrXYSum float64
	for i := 1; i < sampleNum; i++ {
		corrXYSum += float64(i) * float64(samplesYInYOrder[i].Ordinal)
	}
	// X means the ordinal of the item in original sequence, Y means the oridnal of the item in the
	// sorted sequence, we know that X and Y value sets are both:
	// 0, 1, ..., sampleNum-1
	// we can simply compute sum(X) = sum(Y) =
	//    (sampleNum-1)*sampleNum / 2
	// and sum(X^2) = sum(Y^2) =
	//    (sampleNum-1)*sampleNum*(2*sampleNum-1) / 6
	// We use "Pearson correlation coefficient" to compute the order correlation of columns,
	// the formula is based on https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
	// Note that (itemsCount*corrX2Sum - corrXSum*corrXSum) would never be zero when sampleNum is larger than 1.
	itemsCount := float64(sampleNum)
	corrXSum := (itemsCount - 1) * itemsCount / 2.0
	corrX2Sum := (itemsCount - 1) * itemsCount * (2*itemsCount - 1) / 6.0
	item.ScalarVals = (itemsCount*corrXYSum - corrXSum*corrXSum) / (itemsCount*corrX2Sum - corrXSum*corrXSum)
	return item
}

// SaveExtendedStatsToStorage writes extended stats of a block into allegrosql.stats_extended.
func (h *Handle) SaveExtendedStatsToStorage(blockID int64, extStats *statistics.ExtendedStatsDefCausl, isLoad bool) (err error) {
	if extStats == nil || len(extStats.Stats) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor)
	_, err = exec.Execute(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	version := txn.StartTS()
	sqls := make([]string, 0, 1+len(extStats.Stats))
	for key, item := range extStats.Stats {
		bytes, err := json.Marshal(item.DefCausIDs)
		if err != nil {
			return errors.Trace(err)
		}
		strDefCausIDs := string(bytes)
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			// If isLoad is true, it's INSERT; otherwise, it's UFIDelATE.
			sqls = append(sqls, fmt.Sprintf("replace into allegrosql.stats_extended values ('%s', '%s', %d, %d, '%s', %f, null, %d, %d)", key.StatsName, key.EDB, item.Tp, blockID, strDefCausIDs, item.ScalarVals, version, StatsStatusAnalyzed))
		case ast.StatsTypeDependency:
			sqls = append(sqls, fmt.Sprintf("replace into allegrosql.stats_extended values ('%s', '%s', %d, %d, '%s', null, '%s', %d, %d)", key.StatsName, key.EDB, item.Tp, blockID, strDefCausIDs, item.StringVals, version, StatsStatusAnalyzed))
		}
	}
	if !isLoad {
		sqls = append(sqls, fmt.Sprintf("UFIDelATE allegrosql.stats_meta SET version = %d WHERE block_id = %d", version, blockID))
	}
	return execALLEGROSQLs(ctx, exec, sqls)
}
