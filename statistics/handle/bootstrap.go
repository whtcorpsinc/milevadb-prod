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
	"fmt"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"go.uber.org/zap"
)

func (h *Handle) initStatsMeta4Chunk(is schemareplicant.SchemaReplicant, cache *statsCache, iter *chunk.Iterator4Chunk) {
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		physicalID := event.GetInt64(1)
		causet, ok := h.getTableByPhysicalID(is, physicalID)
		if !ok {
			logutil.BgLogger().Debug("unknown physical ID in stats spacetime causet, maybe it has been dropped", zap.Int64("ID", physicalID))
			continue
		}
		blockInfo := causet.Meta()
		newHistDefCausl := statistics.HistDefCausl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			Count:          event.GetInt64(3),
			ModifyCount:    event.GetInt64(2),
			DeferredCausets:        make(map[int64]*statistics.DeferredCauset, len(blockInfo.DeferredCausets)),
			Indices:        make(map[int64]*statistics.Index, len(blockInfo.Indices)),
		}
		tbl := &statistics.Block{
			HistDefCausl: newHistDefCausl,
			Version:  event.GetUint64(0),
			Name:     getFullTableName(is, blockInfo),
		}
		cache.blocks[physicalID] = tbl
	}
}

func (h *Handle) initStatsMeta(is schemareplicant.SchemaReplicant) (statsCache, error) {
	allegrosql := "select HIGH_PRIORITY version, block_id, modify_count, count from allegrosql.stats_spacetime"
	rc, err := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), allegrosql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return statsCache{}, errors.Trace(err)
	}
	blocks := statsCache{blocks: make(map[int64]*statistics.Block)}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return statsCache{}, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsMeta4Chunk(is, &blocks, iter)
	}
	return blocks, nil
}

func (h *Handle) initStatsHistograms4Chunk(is schemareplicant.SchemaReplicant, cache *statsCache, iter *chunk.Iterator4Chunk) {
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		causet, ok := cache.blocks[event.GetInt64(0)]
		if !ok {
			continue
		}
		id, ndv, nullCount, version, totDefCausSize := event.GetInt64(2), event.GetInt64(3), event.GetInt64(5), event.GetUint64(4), event.GetInt64(7)
		lastAnalyzePos := event.GetCauset(11, types.NewFieldType(allegrosql.TypeBlob))
		tbl, _ := h.getTableByPhysicalID(is, causet.PhysicalID)
		if event.GetInt64(1) > 0 {
			var idxInfo *perceptron.IndexInfo
			for _, idx := range tbl.Meta().Indices {
				if idx.ID == id {
					idxInfo = idx
					break
				}
			}
			if idxInfo == nil {
				continue
			}
			cms, err := statistics.DecodeCMSketch(event.GetBytes(6), nil)
			if err != nil {
				cms = nil
				terror.Log(errors.Trace(err))
			}
			hist := statistics.NewHistogram(id, ndv, nullCount, version, types.NewFieldType(allegrosql.TypeBlob), chunk.InitialCapacity, 0)
			index := &statistics.Index{
				Histogram: *hist,
				CMSketch:  cms,
				Info:      idxInfo,
				StatsVer:  event.GetInt64(8),
				Flag:      event.GetInt64(10),
			}
			lastAnalyzePos.Copy(&index.LastAnalyzePos)
			causet.Indices[hist.ID] = index
		} else {
			var colInfo *perceptron.DeferredCausetInfo
			for _, col := range tbl.Meta().DeferredCausets {
				if col.ID == id {
					colInfo = col
					break
				}
			}
			if colInfo == nil {
				continue
			}
			hist := statistics.NewHistogram(id, ndv, nullCount, version, &colInfo.FieldType, 0, totDefCausSize)
			hist.Correlation = event.GetFloat64(9)
			col := &statistics.DeferredCauset{
				Histogram:  *hist,
				PhysicalID: causet.PhysicalID,
				Info:       colInfo,
				Count:      nullCount,
				IsHandle:   tbl.Meta().PKIsHandle && allegrosql.HasPriKeyFlag(colInfo.Flag),
				Flag:       event.GetInt64(10),
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			causet.DeferredCausets[hist.ID] = col
		}
	}
}

func (h *Handle) initStatsHistograms(is schemareplicant.SchemaReplicant, cache *statsCache) error {
	allegrosql := "select HIGH_PRIORITY block_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from allegrosql.stats_histograms"
	rc, err := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), allegrosql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsHistograms4Chunk(is, cache, iter)
	}
	return nil
}

func (h *Handle) initStatsTopN4Chunk(cache *statsCache, iter *chunk.Iterator4Chunk) {
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		causet, ok := cache.blocks[event.GetInt64(0)]
		if !ok {
			continue
		}
		idx, ok := causet.Indices[event.GetInt64(1)]
		if !ok || idx.CMSketch == nil {
			continue
		}
		data := make([]byte, len(event.GetBytes(2)))
		copy(data, event.GetBytes(2))
		idx.CMSketch.AppendTopN(data, event.GetUint64(3))
	}
}

func (h *Handle) initStatsTopN(cache *statsCache) error {
	allegrosql := "select HIGH_PRIORITY block_id, hist_id, value, count from allegrosql.stats_top_n where is_index = 1"
	rc, err := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), allegrosql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		h.initStatsTopN4Chunk(cache, iter)
	}
	return nil
}

func initStatsBuckets4Chunk(ctx stochastikctx.Context, cache *statsCache, iter *chunk.Iterator4Chunk) {
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		blockID, isIndex, histID := event.GetInt64(0), event.GetInt64(1), event.GetInt64(2)
		causet, ok := cache.blocks[blockID]
		if !ok {
			continue
		}
		var lower, upper types.Causet
		var hist *statistics.Histogram
		if isIndex > 0 {
			index, ok := causet.Indices[histID]
			if !ok {
				continue
			}
			hist = &index.Histogram
			lower, upper = types.NewBytesCauset(event.GetBytes(5)), types.NewBytesCauset(event.GetBytes(6))
		} else {
			column, ok := causet.DeferredCausets[histID]
			if !ok {
				continue
			}
			column.Count += event.GetInt64(3)
			if !allegrosql.HasPriKeyFlag(column.Info.Flag) {
				continue
			}
			hist = &column.Histogram
			d := types.NewBytesCauset(event.GetBytes(5))
			var err error
			lower, err = d.ConvertTo(ctx.GetStochastikVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				logutil.BgLogger().Debug("decode bucket lower bound failed", zap.Error(err))
				delete(causet.DeferredCausets, histID)
				continue
			}
			d = types.NewBytesCauset(event.GetBytes(6))
			upper, err = d.ConvertTo(ctx.GetStochastikVars().StmtCtx, &column.Info.FieldType)
			if err != nil {
				logutil.BgLogger().Debug("decode bucket upper bound failed", zap.Error(err))
				delete(causet.DeferredCausets, histID)
				continue
			}
		}
		hist.AppendBucket(&lower, &upper, event.GetInt64(3), event.GetInt64(4))
	}
}

func (h *Handle) initStatsBuckets(cache *statsCache) error {
	allegrosql := "select HIGH_PRIORITY block_id, is_index, hist_id, count, repeats, lower_bound, upper_bound from allegrosql.stats_buckets order by block_id, is_index, hist_id, bucket_id"
	rc, err := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), allegrosql)
	if len(rc) > 0 {
		defer terror.Call(rc[0].Close)
	}
	if err != nil {
		return errors.Trace(err)
	}
	req := rc[0].NewChunk()
	iter := chunk.NewIterator4Chunk(req)
	for {
		err := rc[0].Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}
		initStatsBuckets4Chunk(h.mu.ctx, cache, iter)
	}
	lastVersion := uint64(0)
	for _, causet := range cache.blocks {
		lastVersion = mathutil.MaxUint64(lastVersion, causet.Version)
		for _, idx := range causet.Indices {
			for i := 1; i < idx.Len(); i++ {
				idx.Buckets[i].Count += idx.Buckets[i-1].Count
			}
			idx.PreCalculateScalar()
		}
		for _, col := range causet.DeferredCausets {
			for i := 1; i < col.Len(); i++ {
				col.Buckets[i].Count += col.Buckets[i-1].Count
			}
			col.PreCalculateScalar()
		}
	}
	cache.version = lastVersion
	return nil
}

// InitStats will init the stats cache using full load strategy.
func (h *Handle) InitStats(is schemareplicant.SchemaReplicant) (err error) {
	h.mu.Lock()
	defer func() {
		_, err1 := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), "commit")
		if err == nil && err1 != nil {
			err = err1
		}
		h.mu.Unlock()
	}()
	_, err = h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), "begin")
	if err != nil {
		return err
	}
	cache, err := h.initStatsMeta(is)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsHistograms(is, &cache)
	if err != nil {
		return errors.Trace(err)
	}
	err = h.initStatsTopN(&cache)
	if err != nil {
		return err
	}
	err = h.initStatsBuckets(&cache)
	if err != nil {
		return errors.Trace(err)
	}
	cache.initMemoryUsage()
	h.uFIDelateStatsCache(cache)
	return nil
}

func getFullTableName(is schemareplicant.SchemaReplicant, tblInfo *perceptron.TableInfo) string {
	for _, schemaReplicant := range is.AllSchemas() {
		if t, err := is.TableByName(schemaReplicant.Name, tblInfo.Name); err == nil {
			if t.Meta().ID == tblInfo.ID {
				return schemaReplicant.Name.O + "." + tblInfo.Name.O
			}
		}
	}
	return fmt.Sprintf("%d", tblInfo.ID)
}
