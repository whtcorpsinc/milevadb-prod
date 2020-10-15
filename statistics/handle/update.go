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
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"go.uber.org/zap"
)

type blockDeltaMap map[int64]variable.TableDelta

func (m blockDeltaMap) uFIDelate(id int64, delta int64, count int64, colSize *map[int64]int64) {
	item := m[id]
	item.Delta += delta
	item.Count += count
	if item.DefCausSize == nil {
		item.DefCausSize = make(map[int64]int64)
	}
	if colSize != nil {
		for key, val := range *colSize {
			item.DefCausSize[key] += val
		}
	}
	m[id] = item
}

type errorRateDelta struct {
	PkID         int64
	PkErrorRate  *statistics.ErrorRate
	IdxErrorRate map[int64]*statistics.ErrorRate
}

type errorRateDeltaMap map[int64]errorRateDelta

func (m errorRateDeltaMap) uFIDelate(blockID int64, histID int64, rate float64, isIndex bool) {
	item := m[blockID]
	if isIndex {
		if item.IdxErrorRate == nil {
			item.IdxErrorRate = make(map[int64]*statistics.ErrorRate)
		}
		if item.IdxErrorRate[histID] == nil {
			item.IdxErrorRate[histID] = &statistics.ErrorRate{}
		}
		item.IdxErrorRate[histID].UFIDelate(rate)
	} else {
		if item.PkErrorRate == nil {
			item.PkID = histID
			item.PkErrorRate = &statistics.ErrorRate{}
		}
		item.PkErrorRate.UFIDelate(rate)
	}
	m[blockID] = item
}

func (m errorRateDeltaMap) merge(deltaMap errorRateDeltaMap) {
	for blockID, item := range deltaMap {
		tbl := m[blockID]
		for histID, errorRate := range item.IdxErrorRate {
			if tbl.IdxErrorRate == nil {
				tbl.IdxErrorRate = make(map[int64]*statistics.ErrorRate)
			}
			if tbl.IdxErrorRate[histID] == nil {
				tbl.IdxErrorRate[histID] = &statistics.ErrorRate{}
			}
			tbl.IdxErrorRate[histID].Merge(errorRate)
		}
		if item.PkErrorRate != nil {
			if tbl.PkErrorRate == nil {
				tbl.PkID = item.PkID
				tbl.PkErrorRate = &statistics.ErrorRate{}
			}
			tbl.PkErrorRate.Merge(item.PkErrorRate)
		}
		m[blockID] = tbl
	}
}

func (m errorRateDeltaMap) clear(blockID int64, histID int64, isIndex bool) {
	item := m[blockID]
	if isIndex {
		delete(item.IdxErrorRate, histID)
	} else {
		item.PkErrorRate = nil
	}
	m[blockID] = item
}

func (h *Handle) merge(s *StochastikStatsDefCauslector, rateMap errorRateDeltaMap) {
	for id, item := range s.mapper {
		h.globalMap.uFIDelate(id, item.Delta, item.Count, &item.DefCausSize)
	}
	s.mapper = make(blockDeltaMap)
	rateMap.merge(s.rateMap)
	s.rateMap = make(errorRateDeltaMap)
	h.feedback.Merge(s.feedback)
	s.feedback = statistics.NewQueryFeedbackMap()
}

// StochastikStatsDefCauslector is a list item that holds the delta mapper. If you want to write or read mapper, you must dagger it.
type StochastikStatsDefCauslector struct {
	sync.Mutex

	mapper   blockDeltaMap
	feedback *statistics.QueryFeedbackMap
	rateMap  errorRateDeltaMap
	next     *StochastikStatsDefCauslector
	// deleted is set to true when a stochastik is closed. Every time we sweep the list, we will remove the useless collector.
	deleted bool
}

// Delete only sets the deleted flag true, it will be deleted from list when DumpStatsDeltaToKV is called.
func (s *StochastikStatsDefCauslector) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// UFIDelate will uFIDelates the delta and count for one causet id.
func (s *StochastikStatsDefCauslector) UFIDelate(id int64, delta int64, count int64, colSize *map[int64]int64) {
	s.Lock()
	defer s.Unlock()
	s.mapper.uFIDelate(id, delta, count, colSize)
}

var (
	// MinLogScanCount is the minimum scan count for a feedback to be logged.
	MinLogScanCount = int64(1000)
	// MinLogErrorRate is the minimum error rate for a feedback to be logged.
	MinLogErrorRate = 0.5
)

// StoreQueryFeedback merges the feedback into stats collector.
func (s *StochastikStatsDefCauslector) StoreQueryFeedback(feedback interface{}, h *Handle) error {
	q := feedback.(*statistics.QueryFeedback)
	if !q.Valid || q.Hist == nil {
		return nil
	}
	err := h.RecalculateExpectCount(q)
	if err != nil {
		return errors.Trace(err)
	}
	rate := q.CalcErrorRate()
	if !(rate >= MinLogErrorRate && (q.Actual() >= MinLogScanCount || q.Expected >= MinLogScanCount)) {
		return nil
	}
	metrics.SignificantFeedbackCounter.Inc()
	metrics.StatsInaccuracyRate.Observe(rate)
	if log.GetLevel() == zap.DebugLevel {
		h.logDetailedInfo(q)
	}
	s.Lock()
	defer s.Unlock()
	isIndex := q.Tp == statistics.IndexType
	s.rateMap.uFIDelate(q.PhysicalID, q.Hist.ID, rate, isIndex)
	s.feedback.Append(q)
	return nil
}

// NewStochastikStatsDefCauslector allocates a stats collector for a stochastik.
func (h *Handle) NewStochastikStatsDefCauslector() *StochastikStatsDefCauslector {
	h.listHead.Lock()
	defer h.listHead.Unlock()
	newDefCauslector := &StochastikStatsDefCauslector{
		mapper:   make(blockDeltaMap),
		rateMap:  make(errorRateDeltaMap),
		next:     h.listHead.next,
		feedback: statistics.NewQueryFeedbackMap(),
	}
	h.listHead.next = newDefCauslector
	return newDefCauslector
}

var (
	// DumpStatsDeltaRatio is the lower bound of `Modify Count / Block Count` for stats delta to be dumped.
	DumpStatsDeltaRatio = 1 / 10000.0
	// dumpStatsMaxDuration is the max duration since last uFIDelate.
	dumpStatsMaxDuration = time.Hour
)

// needDumpStatsDelta returns true when only uFIDelates a small portion of the causet and the time since last uFIDelate
// do not exceed one hour.
func needDumpStatsDelta(h *Handle, id int64, item variable.TableDelta, currentTime time.Time) bool {
	if item.InitTime.IsZero() {
		item.InitTime = currentTime
	}
	tbl, ok := h.statsCache.Load().(statsCache).blocks[id]
	if !ok {
		// No need to dump if the stats is invalid.
		return false
	}
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		// Dump the stats to ekv at least once an hour.
		return true
	}
	if tbl.Count == 0 || float64(item.Count)/float64(tbl.Count) > DumpStatsDeltaRatio {
		// Dump the stats when there are many modifications.
		return true
	}
	return false
}

type dumpMode bool

const (
	// DumpAll indicates dump all the delta info in to ekv.
	DumpAll dumpMode = true
	// DumFIDelelta indicates dump part of the delta info in to ekv.
	DumFIDelelta dumpMode = false
)

// sweepList will loop over the list, merge each stochastik's local stats into handle
// and remove closed stochastik's collector.
func (h *Handle) sweepList() {
	prev := h.listHead
	prev.Lock()
	errorRateMap := make(errorRateDeltaMap)
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		// Merge the stochastik stats into handle and error rate map.
		h.merge(curr, errorRateMap)
		if curr.deleted {
			prev.next = curr.next
			// Since the stochastik is already closed, we can safely unlock it here.
			curr.Unlock()
		} else {
			// Unlock the previous dagger, so we only holds at most two stochastik's dagger at the same time.
			prev.Unlock()
			prev = curr
		}
	}
	prev.Unlock()
	h.mu.Lock()
	h.mu.rateMap.merge(errorRateMap)
	h.mu.Unlock()
	h.siftFeedbacks()
}

// siftFeedbacks eliminates feedbacks which are overlapped with others. It is a tradeoff between
// feedback accuracy and its overhead.
func (h *Handle) siftFeedbacks() {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	for k, qs := range h.feedback.Feedbacks {
		fbs := make([]statistics.Feedback, 0, len(qs)*2)
		for _, q := range qs {
			fbs = append(fbs, q.Feedback...)
		}
		if len(fbs) == 0 {
			delete(h.feedback.Feedbacks, k)
			continue
		}
		h.feedback.Feedbacks[k] = h.feedback.Feedbacks[k][:1]
		h.feedback.Feedbacks[k][0].Feedback, _ = statistics.NonOverlappedFeedbacks(sc, fbs)
	}
	h.feedback.Size = len(h.feedback.Feedbacks)
}

// DumpStatsDeltaToKV sweeps the whole list and uFIDelates the global map, then we dumps every causet that held in map to KV.
// If the mode is `DumFIDelelta`, it will only dump that delta info that `Modify Count / Block Count` greater than a ratio.
func (h *Handle) DumpStatsDeltaToKV(mode dumpMode) error {
	h.sweepList()
	currentTime := time.Now()
	for id, item := range h.globalMap {
		if mode == DumFIDelelta && !needDumpStatsDelta(h, id, item, currentTime) {
			continue
		}
		uFIDelated, err := h.dumpTableStatCountToKV(id, item)
		if err != nil {
			return errors.Trace(err)
		}
		if uFIDelated {
			h.globalMap.uFIDelate(id, -item.Delta, -item.Count, nil)
		}
		if err = h.dumpTableStatDefCausSizeToKV(id, item); err != nil {
			return errors.Trace(err)
		}
		if uFIDelated {
			delete(h.globalMap, id)
		} else {
			m := h.globalMap[id]
			m.DefCausSize = nil
			h.globalMap[id] = m
		}
	}
	return nil
}

// dumpTableStatDeltaToKV dumps a single delta with some causet to KV and uFIDelates the version.
func (h *Handle) dumpTableStatCountToKV(id int64, delta variable.TableDelta) (uFIDelated bool, err error) {
	if delta.Count == 0 {
		return true, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate)
	_, err = exec.InterDircute(ctx, "begin")
	if err != nil {
		return false, errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(context.Background(), exec, err)
	}()

	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return false, errors.Trace(err)
	}
	startTS := txn.StartTS()
	var allegrosql string
	if delta.Delta < 0 {
		allegrosql = fmt.Sprintf("uFIDelate allegrosql.stats_spacetime set version = %d, count = count - %d, modify_count = modify_count + %d where block_id = %d and count >= %d", startTS, -delta.Delta, delta.Count, id, -delta.Delta)
	} else {
		allegrosql = fmt.Sprintf("uFIDelate allegrosql.stats_spacetime set version = %d, count = count + %d, modify_count = modify_count + %d where block_id = %d", startTS, delta.Delta, delta.Count, id)
	}
	err = execALLEGROSQLs(context.Background(), exec, []string{allegrosql})
	uFIDelated = h.mu.ctx.GetStochastikVars().StmtCtx.AffectedRows() > 0
	return
}

func (h *Handle) dumpTableStatDefCausSizeToKV(id int64, delta variable.TableDelta) error {
	if len(delta.DefCausSize) == 0 {
		return nil
	}
	values := make([]string, 0, len(delta.DefCausSize))
	for histID, deltaDefCausSize := range delta.DefCausSize {
		if deltaDefCausSize == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d)", id, histID, deltaDefCausSize))
	}
	if len(values) == 0 {
		return nil
	}
	allegrosql := fmt.Sprintf("insert into allegrosql.stats_histograms (block_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key uFIDelate tot_col_size = tot_col_size + values(tot_col_size)", strings.Join(values, ","))
	_, _, err := h.restrictedInterDirc.InterDircRestrictedALLEGROSQL(allegrosql)
	return errors.Trace(err)
}

// DumpStatsFeedbackToKV dumps the stats feedback to KV.
func (h *Handle) DumpStatsFeedbackToKV() error {
	var err error
	for _, fbs := range h.feedback.Feedbacks {
		for _, fb := range fbs {
			if fb.Tp == statistics.PkType {
				err = h.DumpFeedbackToKV(fb)
			} else {
				t, ok := h.statsCache.Load().(statsCache).blocks[fb.PhysicalID]
				if ok {
					err = h.DumpFeedbackForIndex(fb, t)
				}
			}
			if err != nil {
				// For simplicity, we just drop other feedbacks in case of error.
				break
			}
		}
	}
	h.feedback = statistics.NewQueryFeedbackMap()
	return errors.Trace(err)
}

// DumpFeedbackToKV dumps the given feedback to physical ekv layer.
func (h *Handle) DumpFeedbackToKV(fb *statistics.QueryFeedback) error {
	vals, err := statistics.EncodeFeedback(fb)
	if err != nil {
		logutil.BgLogger().Debug("error occurred when encoding feedback", zap.Error(err))
		return nil
	}
	var isIndex int64
	if fb.Tp == statistics.IndexType {
		isIndex = 1
	}
	allegrosql := fmt.Sprintf("insert into allegrosql.stats_feedback (block_id, hist_id, is_index, feedback) values "+
		"(%d, %d, %d, X'%X')", fb.PhysicalID, fb.Hist.ID, isIndex, vals)
	h.mu.Lock()
	_, err = h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), allegrosql)
	h.mu.Unlock()
	if err != nil {
		metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblError).Inc()
	} else {
		metrics.DumpFeedbackCounter.WithLabelValues(metrics.LblOK).Inc()
	}
	return errors.Trace(err)
}

// UFIDelateStatsByLocalFeedback will uFIDelate statistics by the local feedback.
// Currently, we dump the feedback with the period of 10 minutes, which means
// it takes 10 minutes for a feedback to take effect. However, we can use the
// feedback locally on this milevadb-server, so it could be used more timely.
func (h *Handle) UFIDelateStatsByLocalFeedback(is schemareplicant.SchemaReplicant) {
	h.sweepList()
	for _, fbs := range h.feedback.Feedbacks {
		for _, fb := range fbs {
			h.mu.Lock()
			causet, ok := h.getTableByPhysicalID(is, fb.PhysicalID)
			h.mu.Unlock()
			if !ok {
				continue
			}
			tblStats := h.GetPartitionStats(causet.Meta(), fb.PhysicalID)
			newTblStats := tblStats.Copy()
			if fb.Tp == statistics.IndexType {
				idx, ok := tblStats.Indices[fb.Hist.ID]
				if !ok || idx.Histogram.Len() == 0 {
					continue
				}
				newIdx := *idx
				eqFB, ranFB := statistics.SplitFeedbackByQueryType(fb.Feedback)
				newIdx.CMSketch = statistics.UFIDelateCMSketch(idx.CMSketch, eqFB)
				newIdx.Histogram = *statistics.UFIDelateHistogram(&idx.Histogram, &statistics.QueryFeedback{Feedback: ranFB})
				newIdx.Histogram.PreCalculateScalar()
				newIdx.Flag = statistics.ResetAnalyzeFlag(newIdx.Flag)
				newTblStats.Indices[fb.Hist.ID] = &newIdx
			} else {
				col, ok := tblStats.DeferredCausets[fb.Hist.ID]
				if !ok || col.Histogram.Len() == 0 {
					continue
				}
				newDefCaus := *col
				// only use the range query to uFIDelate primary key
				_, ranFB := statistics.SplitFeedbackByQueryType(fb.Feedback)
				newFB := &statistics.QueryFeedback{Feedback: ranFB}
				newFB = newFB.DecodeIntValues()
				newDefCaus.Histogram = *statistics.UFIDelateHistogram(&col.Histogram, newFB)
				newDefCaus.Flag = statistics.ResetAnalyzeFlag(newDefCaus.Flag)
				newTblStats.DeferredCausets[fb.Hist.ID] = &newDefCaus
			}
			oldCache := h.statsCache.Load().(statsCache)
			h.uFIDelateStatsCache(oldCache.uFIDelate([]*statistics.Block{newTblStats}, nil, oldCache.version))
		}
	}
}

// UFIDelateErrorRate uFIDelates the error rate of columns from h.rateMap to cache.
func (h *Handle) UFIDelateErrorRate(is schemareplicant.SchemaReplicant) {
	h.mu.Lock()
	tbls := make([]*statistics.Block, 0, len(h.mu.rateMap))
	for id, item := range h.mu.rateMap {
		causet, ok := h.getTableByPhysicalID(is, id)
		if !ok {
			continue
		}
		tbl := h.GetPartitionStats(causet.Meta(), id).Copy()
		if item.PkErrorRate != nil && tbl.DeferredCausets[item.PkID] != nil {
			col := *tbl.DeferredCausets[item.PkID]
			col.ErrorRate.Merge(item.PkErrorRate)
			tbl.DeferredCausets[item.PkID] = &col
		}
		for key, val := range item.IdxErrorRate {
			if tbl.Indices[key] == nil {
				continue
			}
			idx := *tbl.Indices[key]
			idx.ErrorRate.Merge(val)
			tbl.Indices[key] = &idx
		}
		tbls = append(tbls, tbl)
		delete(h.mu.rateMap, id)
	}
	h.mu.Unlock()
	oldCache := h.statsCache.Load().(statsCache)
	h.uFIDelateStatsCache(oldCache.uFIDelate(tbls, nil, oldCache.version))
}

// HandleUFIDelateStats uFIDelate the stats using feedback.
func (h *Handle) HandleUFIDelateStats(is schemareplicant.SchemaReplicant) error {
	allegrosql := "SELECT distinct block_id from allegrosql.stats_feedback"
	blocks, _, err := h.restrictedInterDirc.InterDircRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	if len(blocks) == 0 {
		return nil
	}

	for _, ptbl := range blocks {
		// this func lets `defer` works normally, where `Close()` should be called before any return
		err = func() error {
			tbl := ptbl.GetInt64(0)
			allegrosql = fmt.Sprintf("select block_id, hist_id, is_index, feedback from allegrosql.stats_feedback where block_id=%d order by hist_id, is_index", tbl)
			rc, err := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), allegrosql)
			if len(rc) > 0 {
				defer terror.Call(rc[0].Close)
			}
			if err != nil {
				return errors.Trace(err)
			}
			blockID, histID, isIndex := int64(-1), int64(-1), int64(-1)
			var rows []chunk.Row
			for {
				req := rc[0].NewChunk()
				iter := chunk.NewIterator4Chunk(req)
				err := rc[0].Next(context.TODO(), req)
				if err != nil {
					return errors.Trace(err)
				}
				if req.NumRows() == 0 {
					if len(rows) > 0 {
						if err := h.handleSingleHistogramUFIDelate(is, rows); err != nil {
							return errors.Trace(err)
						}
					}
					break
				}
				for event := iter.Begin(); event != iter.End(); event = iter.Next() {
					// len(rows) > 100000 limits the rows to avoid OOM
					if event.GetInt64(0) != blockID || event.GetInt64(1) != histID || event.GetInt64(2) != isIndex || len(rows) > 100000 {
						if len(rows) > 0 {
							if err := h.handleSingleHistogramUFIDelate(is, rows); err != nil {
								return errors.Trace(err)
							}
						}
						blockID, histID, isIndex = event.GetInt64(0), event.GetInt64(1), event.GetInt64(2)
						rows = rows[:0]
					}
					rows = append(rows, event)
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

// handleSingleHistogramUFIDelate uFIDelates the Histogram and CM Sketch using these feedbacks. All the feedbacks for
// the same index or column are gathered in `rows`.
func (h *Handle) handleSingleHistogramUFIDelate(is schemareplicant.SchemaReplicant, rows []chunk.Row) (err error) {
	physicalTableID, histID, isIndex := rows[0].GetInt64(0), rows[0].GetInt64(1), rows[0].GetInt64(2)
	defer func() {
		if err == nil {
			err = errors.Trace(h.deleteOutdatedFeedback(physicalTableID, histID, isIndex))
		}
	}()
	h.mu.Lock()
	causet, ok := h.getTableByPhysicalID(is, physicalTableID)
	h.mu.Unlock()
	// The causet has been deleted.
	if !ok {
		return nil
	}
	var tbl *statistics.Block
	if causet.Meta().GetPartitionInfo() != nil {
		tbl = h.GetPartitionStats(causet.Meta(), physicalTableID)
	} else {
		tbl = h.GetTableStats(causet.Meta())
	}
	var cms *statistics.CMSketch
	var hist *statistics.Histogram
	if isIndex == 1 {
		idx, ok := tbl.Indices[histID]
		if ok && idx.Histogram.Len() > 0 {
			idxHist := idx.Histogram
			hist = &idxHist
			cms = idx.CMSketch.Copy()
		}
	} else {
		col, ok := tbl.DeferredCausets[histID]
		if ok && col.Histogram.Len() > 0 {
			colHist := col.Histogram
			hist = &colHist
		}
	}
	// The column or index has been deleted.
	if hist == nil {
		return nil
	}
	q := &statistics.QueryFeedback{}
	for _, event := range rows {
		err1 := statistics.DecodeFeedback(event.GetBytes(3), q, cms, hist.Tp)
		if err1 != nil {
			logutil.BgLogger().Debug("decode feedback failed", zap.Error(err))
		}
	}
	err = h.dumpStatsUFIDelateToKV(physicalTableID, isIndex, q, hist, cms)
	return errors.Trace(err)
}

func (h *Handle) deleteOutdatedFeedback(blockID, histID, isIndex int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	hasData := true
	for hasData {
		allegrosql := fmt.Sprintf("delete from allegrosql.stats_feedback where block_id = %d and hist_id = %d and is_index = %d limit 10000", blockID, histID, isIndex)
		_, err := h.mu.ctx.(sqlexec.ALLEGROSQLInterlockingDirectorate).InterDircute(context.TODO(), allegrosql)
		if err != nil {
			return errors.Trace(err)
		}
		hasData = h.mu.ctx.GetStochastikVars().StmtCtx.AffectedRows() > 0
	}
	return nil
}

func (h *Handle) dumpStatsUFIDelateToKV(blockID, isIndex int64, q *statistics.QueryFeedback, hist *statistics.Histogram, cms *statistics.CMSketch) error {
	hist = statistics.UFIDelateHistogram(hist, q)
	err := h.SaveStatsToStorage(blockID, -1, int(isIndex), hist, cms, 0)
	metrics.UFIDelateStatsCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
	return errors.Trace(err)
}

const (
	// StatsTenantKey is the stats tenant path that is saved to etcd.
	StatsTenantKey = "/milevadb/stats/tenant"
	// StatsPrompt is the prompt for stats tenant manager.
	StatsPrompt = "stats"
)

// AutoAnalyzeMinCnt means if the count of causet is less than this value, we needn't do auto analyze.
var AutoAnalyzeMinCnt int64 = 1000

// TableAnalyzed checks if the causet is analyzed.
func TableAnalyzed(tbl *statistics.Block) bool {
	for _, col := range tbl.DeferredCausets {
		if col.Count > 0 {
			return true
		}
	}
	for _, idx := range tbl.Indices {
		if idx.Histogram.Len() > 0 {
			return true
		}
	}
	return false
}

// NeedAnalyzeTable checks if we need to analyze the causet:
// 1. If the causet has never been analyzed, we need to analyze it when it has
//    not been modified for a while.
// 2. If the causet had been analyzed before, we need to analyze it when
//    "tbl.ModifyCount/tbl.Count > autoAnalyzeRatio" and the current time is
//    between `start` and `end`.
func NeedAnalyzeTable(tbl *statistics.Block, limit time.Duration, autoAnalyzeRatio float64, start, end, now time.Time) (bool, string) {
	analyzed := TableAnalyzed(tbl)
	if !analyzed {
		t := time.Unix(0, oracle.ExtractPhysical(tbl.Version)*int64(time.Millisecond))
		dur := time.Since(t)
		return dur >= limit, fmt.Sprintf("causet unanalyzed, time since last uFIDelated %vs", dur)
	}
	// Auto analyze is disabled.
	if autoAnalyzeRatio == 0 {
		return false, ""
	}
	// No need to analyze it.
	if float64(tbl.ModifyCount)/float64(tbl.Count) <= autoAnalyzeRatio {
		return false, ""
	}
	// Tests if current time is within the time period.
	return timeutil.WithinDayTimePeriod(start, end, now), fmt.Sprintf("too many modifications(%v/%v>%v)", tbl.ModifyCount, tbl.Count, autoAnalyzeRatio)
}

func (h *Handle) getAutoAnalyzeParameters() map[string]string {
	allegrosql := fmt.Sprintf("select variable_name, variable_value from allegrosql.global_variables where variable_name in ('%s', '%s', '%s')",
		variable.MilevaDBAutoAnalyzeRatio, variable.MilevaDBAutoAnalyzeStartTime, variable.MilevaDBAutoAnalyzeEndTime)
	rows, _, err := h.restrictedInterDirc.InterDircRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return map[string]string{}
	}
	parameters := make(map[string]string, len(rows))
	for _, event := range rows {
		parameters[event.GetString(0)] = event.GetString(1)
	}
	return parameters
}

func parseAutoAnalyzeRatio(ratio string) float64 {
	autoAnalyzeRatio, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		return variable.DefAutoAnalyzeRatio
	}
	return math.Max(autoAnalyzeRatio, 0)
}

func parseAnalyzePeriod(start, end string) (time.Time, time.Time, error) {
	if start == "" {
		start = variable.DefAutoAnalyzeStartTime
	}
	if end == "" {
		end = variable.DefAutoAnalyzeEndTime
	}
	s, err := time.ParseInLocation(variable.FullDayTimeFormat, start, time.UTC)
	if err != nil {
		return s, s, errors.Trace(err)
	}
	e, err := time.ParseInLocation(variable.FullDayTimeFormat, end, time.UTC)
	return s, e, err
}

// HandleAutoAnalyze analyzes the newly created causet or index.
func (h *Handle) HandleAutoAnalyze(is schemareplicant.SchemaReplicant) {
	dbs := is.AllSchemaNames()
	parameters := h.getAutoAnalyzeParameters()
	autoAnalyzeRatio := parseAutoAnalyzeRatio(parameters[variable.MilevaDBAutoAnalyzeRatio])
	start, end, err := parseAnalyzePeriod(parameters[variable.MilevaDBAutoAnalyzeStartTime], parameters[variable.MilevaDBAutoAnalyzeEndTime])
	if err != nil {
		logutil.BgLogger().Error("[stats] parse auto analyze period failed", zap.Error(err))
		return
	}
	for _, EDB := range dbs {
		tbls := is.SchemaTables(perceptron.NewCIStr(EDB))
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			pi := tblInfo.GetPartitionInfo()
			if pi == nil {
				statsTbl := h.GetTableStats(tblInfo)
				allegrosql := "analyze causet `" + EDB + "`.`" + tblInfo.Name.O + "`"
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, allegrosql)
				if analyzed {
					return
				}
				continue
			}
			for _, def := range pi.Definitions {
				allegrosql := "analyze causet `" + EDB + "`.`" + tblInfo.Name.O + "`" + " partition `" + def.Name.O + "`"
				statsTbl := h.GetPartitionStats(tblInfo, def.ID)
				analyzed := h.autoAnalyzeTable(tblInfo, statsTbl, start, end, autoAnalyzeRatio, allegrosql)
				if analyzed {
					return
				}
				continue
			}
		}
	}
}

func (h *Handle) autoAnalyzeTable(tblInfo *perceptron.TableInfo, statsTbl *statistics.Block, start, end time.Time, ratio float64, allegrosql string) bool {
	if statsTbl.Pseudo || statsTbl.Count < AutoAnalyzeMinCnt {
		return false
	}
	if needAnalyze, reason := NeedAnalyzeTable(statsTbl, 20*h.Lease(), ratio, start, end, time.Now()); needAnalyze {
		logutil.BgLogger().Info("[stats] auto analyze triggered", zap.String("allegrosql", allegrosql), zap.String("reason", reason))
		h.execAutoAnalyze(allegrosql)
		return true
	}
	for _, idx := range tblInfo.Indices {
		if _, ok := statsTbl.Indices[idx.ID]; !ok && idx.State == perceptron.StatePublic {
			allegrosql = fmt.Sprintf("%s index `%s`", allegrosql, idx.Name.O)
			logutil.BgLogger().Info("[stats] auto analyze for unanalyzed", zap.String("allegrosql", allegrosql))
			h.execAutoAnalyze(allegrosql)
			return true
		}
	}
	return false
}

func (h *Handle) execAutoAnalyze(allegrosql string) {
	startTime := time.Now()
	_, _, err := h.restrictedInterDirc.InterDircRestrictedALLEGROSQL(allegrosql)
	dur := time.Since(startTime)
	metrics.AutoAnalyzeHistogram.Observe(dur.Seconds())
	if err != nil {
		logutil.BgLogger().Error("[stats] auto analyze failed", zap.String("allegrosql", allegrosql), zap.Duration("cost_time", dur), zap.Error(err))
		metrics.AutoAnalyzeCounter.WithLabelValues("failed").Inc()
	} else {
		metrics.AutoAnalyzeCounter.WithLabelValues("succ").Inc()
	}
}

// formatBuckets formats bucket from lowBkt to highBkt.
func formatBuckets(hg *statistics.Histogram, lowBkt, highBkt, idxDefCauss int) string {
	if lowBkt == highBkt {
		return hg.BucketToString(lowBkt, idxDefCauss)
	}
	if lowBkt+1 == highBkt {
		return fmt.Sprintf("%s, %s", hg.BucketToString(lowBkt, idxDefCauss), hg.BucketToString(highBkt, idxDefCauss))
	}
	// do not care the midbse buckets
	return fmt.Sprintf("%s, (%d buckets, total count %d), %s", hg.BucketToString(lowBkt, idxDefCauss),
		highBkt-lowBkt-1, hg.Buckets[highBkt-1].Count-hg.Buckets[lowBkt].Count, hg.BucketToString(highBkt, idxDefCauss))
}

func colRangeToStr(c *statistics.DeferredCauset, ran *ranger.Range, actual int64, factor float64) string {
	lowCount, lowBkt := c.LessRowCountWithBktIdx(ran.LowVal[0])
	highCount, highBkt := c.LessRowCountWithBktIdx(ran.HighVal[0])
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, buckets: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&c.Histogram, lowBkt, highBkt, 0))
}

func logForIndexRange(idx *statistics.Index, ran *ranger.Range, actual int64, factor float64) string {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	lb, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		return ""
	}
	rb, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		return ""
	}
	if idx.CMSketch != nil && bytes.Compare(ekv.Key(lb).PrefixNext(), rb) >= 0 {
		str, err := types.CausetsToString(ran.LowVal, true)
		if err != nil {
			return ""
		}
		return fmt.Sprintf("value: %s, actual: %d, expected: %d", str, actual, int64(float64(idx.QueryBytes(lb))*factor))
	}
	l, r := types.NewBytesCauset(lb), types.NewBytesCauset(rb)
	lowCount, lowBkt := idx.LessRowCountWithBktIdx(l)
	highCount, highBkt := idx.LessRowCountWithBktIdx(r)
	return fmt.Sprintf("range: %s, actual: %d, expected: %d, histogram: {%s}", ran.String(), actual,
		int64((highCount-lowCount)*factor), formatBuckets(&idx.Histogram, lowBkt, highBkt, len(idx.Info.DeferredCausets)))
}

func logForIndex(prefix string, t *statistics.Block, idx *statistics.Index, ranges []*ranger.Range, actual []int64, factor float64) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.StatsVer != statistics.Version1 {
		for i, ran := range ranges {
			logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.String("rangeStr", logForIndexRange(idx, ran, actual[i], factor)))
		}
		return
	}
	for i, ran := range ranges {
		rangePosition := statistics.GetOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.String("rangeStr", logForIndexRange(idx, ran, actual[i], factor)))
			continue
		}
		equalityString, err := types.CausetsToString(ran.LowVal[:rangePosition], true)
		if err != nil {
			continue
		}
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			continue
		}
		equalityCount := idx.CMSketch.QueryBytes(bytes)
		rang := ranger.Range{
			LowVal:  []types.Causet{ran.LowVal[rangePosition]},
			HighVal: []types.Causet{ran.HighVal[rangePosition]},
		}
		colName := idx.Info.DeferredCausets[rangePosition].Name.L
		// prefer index stats over column stats
		if idxHist := t.IndexStartWithDeferredCauset(colName); idxHist != nil && idxHist.Histogram.Len() > 0 {
			rangeString := logForIndexRange(idxHist, &rang, -1, factor)
			logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
				zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
				zap.String("range", rangeString))
		} else if colHist := t.DeferredCausetByName(colName); colHist != nil && colHist.Histogram.Len() > 0 {
			err = convertRangeType(&rang, colHist.Tp, time.UTC)
			if err == nil {
				rangeString := colRangeToStr(colHist, &rang, -1, factor)
				logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
					zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
					zap.String("range", rangeString))
			}
		} else {
			count, err := statistics.GetPseudoRowCountByDeferredCausetRanges(sc, float64(t.Count), []*ranger.Range{&rang}, 0)
			if err == nil {
				logutil.BgLogger().Debug(prefix, zap.String("index", idx.Info.Name.O), zap.Int64("actual", actual[i]),
					zap.String("equality", equalityString), zap.Uint64("expected equality", equalityCount),
					zap.Stringer("range", &rang), zap.Float64("pseudo count", math.Round(count)))
			}
		}
	}
}

func (h *Handle) logDetailedInfo(q *statistics.QueryFeedback) {
	t, ok := h.statsCache.Load().(statsCache).blocks[q.PhysicalID]
	if !ok {
		return
	}
	isIndex := q.Hist.IsIndexHist()
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		logutil.BgLogger().Debug("decode to ranges failed", zap.Error(err))
		return
	}
	actual := make([]int64, 0, len(q.Feedback))
	for _, fb := range q.Feedback {
		actual = append(actual, fb.Count)
	}
	logPrefix := fmt.Sprintf("[stats-feedback] %s", t.Name)
	if isIndex {
		idx := t.Indices[q.Hist.ID]
		if idx == nil || idx.Histogram.Len() == 0 {
			return
		}
		logForIndex(logPrefix, t, idx, ranges, actual, idx.GetIncreaseFactor(t.Count))
	} else {
		c := t.DeferredCausets[q.Hist.ID]
		if c == nil || c.Histogram.Len() == 0 {
			return
		}
		logForPK(logPrefix, c, ranges, actual, c.GetIncreaseFactor(t.Count))
	}
}

func logForPK(prefix string, c *statistics.DeferredCauset, ranges []*ranger.Range, actual []int64, factor float64) {
	for i, ran := range ranges {
		if ran.LowVal[0].GetInt64()+1 >= ran.HighVal[0].GetInt64() {
			continue
		}
		logutil.BgLogger().Debug(prefix, zap.String("column", c.Info.Name.O), zap.String("rangeStr", colRangeToStr(c, ran, actual[i], factor)))
	}
}

// RecalculateExpectCount recalculates the expect event count if the origin event count is estimated by pseudo.
func (h *Handle) RecalculateExpectCount(q *statistics.QueryFeedback) error {
	t, ok := h.statsCache.Load().(statsCache).blocks[q.PhysicalID]
	if !ok {
		return nil
	}
	blockPseudo := t.Pseudo || t.IsOutdated()
	if !blockPseudo {
		return nil
	}
	isIndex := q.Hist.Tp.Tp == allegrosql.TypeBlob
	id := q.Hist.ID
	if isIndex && (t.Indices[id] == nil || !t.Indices[id].NotAccurate()) {
		return nil
	}
	if !isIndex && (t.DeferredCausets[id] == nil || !t.DeferredCausets[id].NotAccurate()) {
		return nil
	}

	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	ranges, err := q.DecodeToRanges(isIndex)
	if err != nil {
		return errors.Trace(err)
	}
	expected := 0.0
	if isIndex {
		idx := t.Indices[id]
		expected, err = idx.GetRowCount(sc, ranges, t.ModifyCount)
		expected *= idx.GetIncreaseFactor(t.Count)
	} else {
		c := t.DeferredCausets[id]
		expected, err = c.GetDeferredCausetRowCount(sc, ranges, t.ModifyCount, true)
		expected *= c.GetIncreaseFactor(t.Count)
	}
	q.Expected = int64(expected)
	return err
}

func (h *Handle) dumpRangeFeedback(sc *stmtctx.StatementContext, ran *ranger.Range, rangeCount float64, q *statistics.QueryFeedback) error {
	lowIsNull := ran.LowVal[0].IsNull()
	if q.Tp == statistics.IndexType {
		lower, err := codec.EncodeKey(sc, nil, ran.LowVal[0])
		if err != nil {
			return errors.Trace(err)
		}
		upper, err := codec.EncodeKey(sc, nil, ran.HighVal[0])
		if err != nil {
			return errors.Trace(err)
		}
		ran.LowVal[0].SetBytes(lower)
		ran.HighVal[0].SetBytes(upper)
	} else {
		if !statistics.SupportDeferredCausetType(q.Hist.Tp) {
			return nil
		}
		if ran.LowVal[0].HoTT() == types.HoTTMinNotNull {
			ran.LowVal[0] = types.GetMinValue(q.Hist.Tp)
		}
		if ran.HighVal[0].HoTT() == types.HoTTMaxValue {
			ran.HighVal[0] = types.GetMaxValue(q.Hist.Tp)
		}
	}
	ranges, ok := q.Hist.SplitRange(sc, []*ranger.Range{ran}, q.Tp == statistics.IndexType)
	if !ok {
		logutil.BgLogger().Debug("type of histogram and ranges mismatch")
		return nil
	}
	counts := make([]float64, 0, len(ranges))
	sum := 0.0
	for i, r := range ranges {
		// Though after `SplitRange`, we may have ranges like `[l, r]`, we still use
		// `betweenRowCount` to compute the estimation since the ranges of feedback are all in `[l, r)`
		// form, that is to say, we ignore the exclusiveness of ranges from `SplitRange` and just use
		// its result of boundary values.
		count := q.Hist.BetweenRowCount(r.LowVal[0], r.HighVal[0])
		// We have to include `NullCount` of histogram for [l, r) cases where l is null because `betweenRowCount`
		// does not include null values of lower bound.
		if i == 0 && lowIsNull {
			count += float64(q.Hist.NullCount)
		}
		sum += count
		counts = append(counts, count)
	}
	if sum <= 1 {
		return nil
	}
	// We assume that each part contributes the same error rate.
	adjustFactor := rangeCount / sum
	for i, r := range ranges {
		q.Feedback = append(q.Feedback, statistics.Feedback{Lower: &r.LowVal[0], Upper: &r.HighVal[0], Count: int64(counts[i] * adjustFactor)})
	}
	return errors.Trace(h.DumpFeedbackToKV(q))
}

func convertRangeType(ran *ranger.Range, ft *types.FieldType, loc *time.Location) error {
	err := statistics.ConvertCausetsType(ran.LowVal, ft, loc)
	if err != nil {
		return err
	}
	return statistics.ConvertCausetsType(ran.HighVal, ft, loc)
}

// DumpFeedbackForIndex dumps the feedback for index.
// For queries that contains both equality and range query, we will split them and UFIDelate accordingly.
func (h *Handle) DumpFeedbackForIndex(q *statistics.QueryFeedback, t *statistics.Block) error {
	idx, ok := t.Indices[q.Hist.ID]
	if !ok {
		return nil
	}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	if idx.CMSketch == nil || idx.StatsVer != statistics.Version1 {
		return h.DumpFeedbackToKV(q)
	}
	ranges, err := q.DecodeToRanges(true)
	if err != nil {
		logutil.BgLogger().Debug("decode feedback ranges fail", zap.Error(err))
		return nil
	}
	for i, ran := range ranges {
		rangePosition := statistics.GetOrdinalOfRangeCond(sc, ran)
		// only contains range or equality query
		if rangePosition == 0 || rangePosition == len(ran.LowVal) {
			continue
		}

		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			logutil.BgLogger().Debug("encode keys fail", zap.Error(err))
			continue
		}
		equalityCount := float64(idx.CMSketch.QueryBytes(bytes)) * idx.GetIncreaseFactor(t.Count)
		rang := &ranger.Range{
			LowVal:  []types.Causet{ran.LowVal[rangePosition]},
			HighVal: []types.Causet{ran.HighVal[rangePosition]},
		}
		colName := idx.Info.DeferredCausets[rangePosition].Name.L
		var rangeCount float64
		rangeFB := &statistics.QueryFeedback{PhysicalID: q.PhysicalID}
		// prefer index stats over column stats
		if idx := t.IndexStartWithDeferredCauset(colName); idx != nil && idx.Histogram.Len() != 0 {
			rangeCount, err = t.GetRowCountByIndexRanges(sc, idx.ID, []*ranger.Range{rang})
			rangeFB.Tp, rangeFB.Hist = statistics.IndexType, &idx.Histogram
		} else if col := t.DeferredCausetByName(colName); col != nil && col.Histogram.Len() != 0 {
			err = convertRangeType(rang, col.Tp, time.UTC)
			if err == nil {
				rangeCount, err = t.GetRowCountByDeferredCausetRanges(sc, col.ID, []*ranger.Range{rang})
				rangeFB.Tp, rangeFB.Hist = statistics.DefCausType, &col.Histogram
			}
		} else {
			continue
		}
		if err != nil {
			logutil.BgLogger().Debug("get event count by ranges fail", zap.Error(err))
			continue
		}

		equalityCount, rangeCount = getNewCountForIndex(equalityCount, rangeCount, float64(t.Count), float64(q.Feedback[i].Count))
		value := types.NewBytesCauset(bytes)
		q.Feedback[i] = statistics.Feedback{Lower: &value, Upper: &value, Count: int64(equalityCount)}
		err = h.dumpRangeFeedback(sc, rang, rangeCount, rangeFB)
		if err != nil {
			logutil.BgLogger().Debug("dump range feedback fail", zap.Error(err))
			continue
		}
	}
	return errors.Trace(h.DumpFeedbackToKV(q))
}

// minAdjustFactor is the minimum adjust factor of each index feedback.
// We use it to avoid adjusting too much when the assumption of independence failed.
const minAdjustFactor = 0.7

// getNewCountForIndex adjust the estimated `eqCount` and `rangeCount` according to the real count.
// We assumes that `eqCount` and `rangeCount` contribute the same error rate.
func getNewCountForIndex(eqCount, rangeCount, totalCount, realCount float64) (float64, float64) {
	estimate := (eqCount / totalCount) * (rangeCount / totalCount) * totalCount
	if estimate <= 1 {
		return eqCount, rangeCount
	}
	adjustFactor := math.Sqrt(realCount / estimate)
	adjustFactor = math.Max(adjustFactor, minAdjustFactor)
	return eqCount * adjustFactor, rangeCount * adjustFactor
}
