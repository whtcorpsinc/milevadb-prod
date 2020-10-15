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

package expensivequery

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Handle is the handler for expensive query.
type Handle struct {
	exitCh chan struct{}
	sm     atomic.Value
}

// NewExpensiveQueryHandle builds a new expensive query handler.
func NewExpensiveQueryHandle(exitCh chan struct{}) *Handle {
	return &Handle{exitCh: exitCh}
}

// SetStochastikManager sets the StochastikManager which is used to fetching the info
// of all active stochastik.
func (eqh *Handle) SetStochastikManager(sm soliton.StochastikManager) *Handle {
	eqh.sm.CausetStore(sm)
	return eqh
}

// Run starts a expensive query checker goroutine at the start time of the server.
func (eqh *Handle) Run() {
	threshold := atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
	// use 100ms as tickInterval temply, may use given interval or use defined variable later
	tickInterval := time.Millisecond * time.Duration(100)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	sm := eqh.sm.Load().(soliton.StochastikManager)
	for {
		select {
		case <-ticker.C:
			processInfo := sm.ShowProcessList()
			for _, info := range processInfo {
				if len(info.Info) == 0 {
					continue
				}
				costTime := time.Since(info.Time)
				if !info.ExceedExpensiveTimeThresh && costTime >= time.Second*time.Duration(threshold) && log.GetLevel() <= zapcore.WarnLevel {
					logExpensiveQuery(costTime, info)
					info.ExceedExpensiveTimeThresh = true
				}

				if info.MaxInterDircutionTime > 0 && costTime > time.Duration(info.MaxInterDircutionTime)*time.Millisecond {
					sm.Kill(info.ID, true)
				}
			}
			threshold = atomic.LoadUint64(&variable.ExpensiveQueryTimeThreshold)
		case <-eqh.exitCh:
			return
		}
	}
}

// LogOnQueryExceedMemQuota prints a log when memory usage of connID is out of memory quota.
func (eqh *Handle) LogOnQueryExceedMemQuota(connID uint64) {
	if log.GetLevel() > zapcore.WarnLevel {
		return
	}
	// The out-of-memory ALLEGROALLEGROSQL may be the internal ALLEGROALLEGROSQL which is executed during
	// the bootstrap phase, and the `sm` is not set at this phase. This is
	// unlikely to happen except for testing. Thus we do not need to log
	// detailed message for it.
	v := eqh.sm.Load()
	if v == nil {
		logutil.BgLogger().Info("expensive_query during bootstrap phase", zap.Uint64("conn_id", connID))
		return
	}
	sm := v.(soliton.StochastikManager)
	info, ok := sm.GetProcessInfo(connID)
	if !ok {
		return
	}
	logExpensiveQuery(time.Since(info.Time), info)
}

func genLogFields(costTime time.Duration, info *soliton.ProcessInfo) []zap.Field {
	logFields := make([]zap.Field, 0, 20)
	logFields = append(logFields, zap.String("cost_time", strconv.FormatFloat(costTime.Seconds(), 'f', -1, 64)+"s"))
	execDetail := info.StmtCtx.GetInterDircDetails()
	logFields = append(logFields, execDetail.ToZapFields()...)
	if copTaskInfo := info.StmtCtx.CausetTasksDetails(); copTaskInfo != nil {
		logFields = append(logFields, copTaskInfo.ToZapFields()...)
	}
	if statsInfo := info.StatsInfo(info.Causet); len(statsInfo) > 0 {
		var buf strings.Builder
		firstComma := false
		vStr := ""
		for k, v := range statsInfo {
			if v == 0 {
				vStr = "pseudo"
			} else {
				vStr = strconv.FormatUint(v, 10)
			}
			if firstComma {
				buf.WriteString("," + k + ":" + vStr)
			} else {
				buf.WriteString(k + ":" + vStr)
				firstComma = true
			}
		}
		logFields = append(logFields, zap.String("stats", buf.String()))
	}
	if info.ID != 0 {
		logFields = append(logFields, zap.Uint64("conn_id", info.ID))
	}
	if len(info.User) > 0 {
		logFields = append(logFields, zap.String("user", info.User))
	}
	if len(info.EDB) > 0 {
		logFields = append(logFields, zap.String("database", info.EDB))
	}
	var blockIDs, indexNames string
	if len(info.StmtCtx.BlockIDs) > 0 {
		blockIDs = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.BlockIDs), " ", ",", -1)
		logFields = append(logFields, zap.String("block_ids", blockIDs))
	}
	if len(info.StmtCtx.IndexNames) > 0 {
		indexNames = strings.Replace(fmt.Sprintf("%v", info.StmtCtx.IndexNames), " ", ",", -1)
		logFields = append(logFields, zap.String("index_names", indexNames))
	}
	logFields = append(logFields, zap.Uint64("txn_start_ts", info.CurTxnStartTS))
	if memTracker := info.StmtCtx.MemTracker; memTracker != nil {
		logFields = append(logFields, zap.String("mem_max", fmt.Sprintf("%d Bytes (%v)", memTracker.MaxConsumed(), memTracker.BytesToString(memTracker.MaxConsumed()))))
	}

	const logALLEGROSQLLen = 1024 * 8
	var allegrosql string
	if len(info.Info) > 0 {
		allegrosql = info.Info
	}
	if len(allegrosql) > logALLEGROSQLLen {
		allegrosql = fmt.Sprintf("%s len(%d)", allegrosql[:logALLEGROSQLLen], len(allegrosql))
	}
	logFields = append(logFields, zap.String("allegrosql", allegrosql))
	return logFields
}

// logExpensiveQuery logs the queries which exceed the time threshold or memory threshold.
func logExpensiveQuery(costTime time.Duration, info *soliton.ProcessInfo) {
	logutil.BgLogger().Warn("expensive_query", genLogFields(costTime, info)...)
}
