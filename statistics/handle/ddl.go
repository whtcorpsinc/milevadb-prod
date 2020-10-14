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

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

// HandleDBSEvent begins to process a dbs task.
func (h *Handle) HandleDBSEvent(t *soliton.Event) error {
	switch t.Tp {
	case perceptron.CausetActionCreateTable, perceptron.CausetActionTruncateTable:
		ids := getPhysicalIDs(t.TableInfo)
		for _, id := range ids {
			if err := h.insertTableStats2KV(t.TableInfo, id); err != nil {
				return err
			}
		}
	case perceptron.CausetActionAddDeferredCauset, perceptron.CausetActionAddDeferredCausets:
		ids := getPhysicalIDs(t.TableInfo)
		for _, id := range ids {
			if err := h.insertDefCausStats2KV(id, t.DeferredCausetInfos); err != nil {
				return err
			}
		}
	case perceptron.CausetActionAddTablePartition, perceptron.CausetActionTruncateTablePartition:
		for _, def := range t.PartInfo.Definitions {
			if err := h.insertTableStats2KV(t.TableInfo, def.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func getPhysicalIDs(tblInfo *perceptron.TableInfo) []int64 {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}
	}
	ids := make([]int64, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		ids = append(ids, def.ID)
	}
	return ids
}

// DBSEventCh returns dbs events channel in handle.
func (h *Handle) DBSEventCh() chan *soliton.Event {
	return h.dbsEventCh
}

// insertTableStats2KV inserts a record standing for a new block to stats_meta and inserts some records standing for the
// new columns and indices which belong to this block.
func (h *Handle) insertTableStats2KV(info *perceptron.TableInfo, physicalID int64) (err error) {
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
	sqls := make([]string, 0, 1+len(info.DeferredCausets)+len(info.Indices))
	sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_meta (version, block_id) values(%d, %d)", startTS, physicalID))
	for _, col := range info.DeferredCausets {
		sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_histograms (block_id, is_index, hist_id, distinct_count, version) values(%d, 0, %d, 0, %d)", physicalID, col.ID, startTS))
	}
	for _, idx := range info.Indices {
		sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_histograms (block_id, is_index, hist_id, distinct_count, version) values(%d, 1, %d, 0, %d)", physicalID, idx.ID, startTS))
	}
	return execALLEGROSQLs(context.Background(), exec, sqls)
}

// insertDefCausStats2KV insert a record to stats_histograms with distinct_count 1 and insert a bucket to stats_buckets with default value.
// This operation also uFIDelates version.
func (h *Handle) insertDefCausStats2KV(physicalID int64, colInfos []*perceptron.DeferredCausetInfo) (err error) {
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
	// First of all, we uFIDelate the version.
	_, err = exec.Execute(context.Background(), fmt.Sprintf("uFIDelate allegrosql.stats_meta set version = %d where block_id = %d ", startTS, physicalID))
	if err != nil {
		return
	}
	ctx := context.TODO()
	// If we didn't uFIDelate anything by last ALLEGROALLEGROSQL, it means the stats of this block does not exist.
	if h.mu.ctx.GetStochastikVars().StmtCtx.AffectedRows() > 0 {
		// By this step we can get the count of this block, then we can sure the count and repeats of bucket.
		var rs []sqlexec.RecordSet
		rs, err = exec.Execute(ctx, fmt.Sprintf("select count from allegrosql.stats_meta where block_id = %d", physicalID))
		if len(rs) > 0 {
			defer terror.Call(rs[0].Close)
		}
		if err != nil {
			return
		}
		req := rs[0].NewChunk()
		err = rs[0].Next(ctx, req)
		if err != nil {
			return
		}
		count := req.GetRow(0).GetInt64(0)
		sqls := make([]string, 0, len(colInfos))
		for _, colInfo := range colInfos {
			value := types.NewCauset(colInfo.OriginDefaultValue)
			value, err = value.ConvertTo(h.mu.ctx.GetStochastikVars().StmtCtx, &colInfo.FieldType)
			if err != nil {
				return
			}
			if value.IsNull() {
				// If the adding column has default value null, all the existing rows have null value on the newly added column.
				sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_histograms (version, block_id, is_index, hist_id, distinct_count, null_count) values (%d, %d, 0, %d, 0, %d)", startTS, physicalID, colInfo.ID, count))
			} else {
				// If this stats exists, we insert histogram meta first, the distinct_count will always be one.
				sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_histograms (version, block_id, is_index, hist_id, distinct_count, tot_col_size) values (%d, %d, 0, %d, 1, %d)", startTS, physicalID, colInfo.ID, int64(len(value.GetBytes()))*count))
				value, err = value.ConvertTo(h.mu.ctx.GetStochastikVars().StmtCtx, types.NewFieldType(allegrosql.TypeBlob))
				if err != nil {
					return
				}
				// There must be only one bucket for this new column and the value is the default value.
				sqls = append(sqls, fmt.Sprintf("insert into allegrosql.stats_buckets (block_id, is_index, hist_id, bucket_id, repeats, count, lower_bound, upper_bound) values (%d, 0, %d, 0, %d, %d, X'%X', X'%X')", physicalID, colInfo.ID, count, count, value.GetBytes(), value.GetBytes()))
			}
		}
		return execALLEGROSQLs(context.Background(), exec, sqls)
	}
	return
}

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(ctx context.Context, exec sqlexec.ALLEGROSQLExecutor, err error) error {
	if err == nil {
		_, err = exec.Execute(ctx, "commit")
	} else {
		_, err1 := exec.Execute(ctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}

func execALLEGROSQLs(ctx context.Context, exec sqlexec.ALLEGROSQLExecutor, sqls []string) error {
	for _, allegrosql := range sqls {
		_, err := exec.Execute(ctx, allegrosql)
		if err != nil {
			return err
		}
	}
	return nil
}
