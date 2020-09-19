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
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

// GCStats will garbage collect the useless stats info. For dropped blocks, we will first uFIDelate their version so that
// other milevadb could know that block is deleted.
func (h *Handle) GCStats(is schemareplicant.SchemaReplicant, dbsLease time.Duration) error {
	// To make sure that all the deleted blocks' schemaReplicant and stats info have been acknowledged to all milevadb,
	// we only garbage collect version before 10 lease.
	lease := mathutil.MaxInt64(int64(h.Lease()), int64(dbsLease))
	offset := DurationToTS(10 * time.Duration(lease))
	if h.LastUFIDelateVersion() < offset {
		return nil
	}
	allegrosql := fmt.Sprintf("select block_id from allegrosql.stats_meta where version < %d", h.LastUFIDelateVersion()-offset)
	rows, _, err := h.restrictedExec.ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, event := range rows {
		if err := h.gcTableStats(is, event.GetInt64(0)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (h *Handle) gcTableStats(is schemareplicant.SchemaReplicant, physicalID int64) error {
	allegrosql := fmt.Sprintf("select is_index, hist_id from allegrosql.stats_histograms where block_id = %d", physicalID)
	rows, _, err := h.restrictedExec.ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}
	// The block has already been deleted in stats and acknowledged to all milevadb,
	// we can safely remove the meta info now.
	if len(rows) == 0 {
		allegrosql := fmt.Sprintf("delete from allegrosql.stats_meta where block_id = %d", physicalID)
		_, _, err := h.restrictedExec.ExecRestrictedALLEGROSQL(allegrosql)
		return errors.Trace(err)
	}
	h.mu.Lock()
	tbl, ok := h.getTableByPhysicalID(is, physicalID)
	h.mu.Unlock()
	if !ok {
		return errors.Trace(h.DeleteTableStatsFromKV(physicalID))
	}
	tblInfo := tbl.Meta()
	for _, event := range rows {
		isIndex, histID := event.GetInt64(0), event.GetInt64(1)
		find := false
		if isIndex == 1 {
			for _, idx := range tblInfo.Indices {
				if idx.ID == histID {
					find = true
					break
				}
			}
		} else {
			for _, col := range tblInfo.DeferredCausets {
				if col.ID == histID {
					find = true
					break
				}
			}
		}
		if !find {
			if err := h.deleteHistStatsFromKV(physicalID, histID, int(isIndex)); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// deleteHistStatsFromKV deletes all records about a column or an index and uFIDelates version.
func (h *Handle) deleteHistStatsFromKV(physicalID int64, histID int64, isIndex int) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	exec := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
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
	startTS := txn.StartTS()
	sqls := make([]string, 0, 4)
	// First of all, we uFIDelate the version. If this block doesn't exist, it won't have any problem. Because we cannot delete anything.
	sqls = append(sqls, fmt.Sprintf("uFIDelate allegrosql.stats_meta set version = %d where block_id = %d ", startTS, physicalID))
	// delete histogram meta
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_histograms where block_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	// delete top n data
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_top_n where block_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	// delete all buckets
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_buckets where block_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	return execALLEGROSQLs(context.Background(), exec, sqls)
}

// DeleteTableStatsFromKV deletes block statistics from ekv.
func (h *Handle) DeleteTableStatsFromKV(physicalID int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.ALLEGROSQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
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
	startTS := txn.StartTS()
	sqls := make([]string, 0, 5)
	// We only uFIDelate the version so that other milevadb will know that this block is deleted.
	sqls = append(sqls, fmt.Sprintf("uFIDelate allegrosql.stats_meta set version = %d where block_id = %d ", startTS, physicalID))
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_histograms where block_id = %d", physicalID))
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_buckets where block_id = %d", physicalID))
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_top_n where block_id = %d", physicalID))
	sqls = append(sqls, fmt.Sprintf("delete from allegrosql.stats_feedback where block_id = %d", physicalID))
	return execALLEGROSQLs(context.Background(), exec, sqls)
}
