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

package variable_test

import (
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
)

var _ = SerialSuites(&testStochastikSuite{})

type testStochastikSuite struct {
}

func (*testStochastikSuite) TestSetSystemVariable(c *C) {
	v := variable.NewStochastikVars()
	v.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	v.TimeZone = time.UTC
	tests := []struct {
		key   string
		value interface{}
		err   bool
	}{
		{variable.TxnIsolation, "SERIALIZABLE", true},
		{variable.TimeZone, "xyz", true},
		{variable.MilevaDBOptAggPushDown, "1", false},
		{variable.MilevaDBOptDistinctAggPushDown, "1", false},
		{variable.MilevaDBMemQuotaQuery, "1024", false},
		{variable.MilevaDBMemQuotaHashJoin, "1024", false},
		{variable.MilevaDBMemQuotaMergeJoin, "1024", false},
		{variable.MilevaDBMemQuotaSort, "1024", false},
		{variable.MilevaDBMemQuotaTopn, "1024", false},
		{variable.MilevaDBMemQuotaIndexLookupReader, "1024", false},
		{variable.MilevaDBMemQuotaIndexLookupJoin, "1024", false},
		{variable.MilevaDBMemQuotaNestedLoopApply, "1024", false},
		{variable.MilevaDBEnableStmtSummary, "1", false},
	}
	for _, t := range tests {
		err := variable.SetStochastikSystemVar(v, t.key, types.NewCauset(t.value))
		if t.err {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (*testStochastikSuite) TestStochastik(c *C) {
	ctx := mock.NewContext()

	ss := ctx.GetStochastikVars().StmtCtx
	c.Assert(ss, NotNil)

	// For AffectedRows
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(1))
	ss.AddAffectedRows(1)
	c.Assert(ss.AffectedRows(), Equals, uint64(2))

	// For RecordRows
	ss.AddRecordRows(1)
	c.Assert(ss.RecordRows(), Equals, uint64(1))
	ss.AddRecordRows(1)
	c.Assert(ss.RecordRows(), Equals, uint64(2))

	// For FoundRows
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(1))
	ss.AddFoundRows(1)
	c.Assert(ss.FoundRows(), Equals, uint64(2))

	// For UFIDelatedRows
	ss.AddUFIDelatedRows(1)
	c.Assert(ss.UFIDelatedRows(), Equals, uint64(1))
	ss.AddUFIDelatedRows(1)
	c.Assert(ss.UFIDelatedRows(), Equals, uint64(2))

	// For TouchedRows
	ss.AddTouchedRows(1)
	c.Assert(ss.TouchedRows(), Equals, uint64(1))
	ss.AddTouchedRows(1)
	c.Assert(ss.TouchedRows(), Equals, uint64(2))

	// For CopiedRows
	ss.AddCopiedRows(1)
	c.Assert(ss.CopiedRows(), Equals, uint64(1))
	ss.AddCopiedRows(1)
	c.Assert(ss.CopiedRows(), Equals, uint64(2))

	// For last insert id
	ctx.GetStochastikVars().SetLastInsertID(1)
	c.Assert(ctx.GetStochastikVars().StmtCtx.LastInsertID, Equals, uint64(1))

	ss.ResetForRetry()
	c.Assert(ss.AffectedRows(), Equals, uint64(0))
	c.Assert(ss.FoundRows(), Equals, uint64(0))
	c.Assert(ss.UFIDelatedRows(), Equals, uint64(0))
	c.Assert(ss.RecordRows(), Equals, uint64(0))
	c.Assert(ss.TouchedRows(), Equals, uint64(0))
	c.Assert(ss.CopiedRows(), Equals, uint64(0))
	c.Assert(ss.WarningCount(), Equals, uint16(0))
}

func (*testStochastikSuite) TestSlowLogFormat(c *C) {
	ctx := mock.NewContext()

	seVar := ctx.GetStochastikVars()
	c.Assert(seVar, NotNil)

	seVar.User = &auth.UserIdentity{Username: "root", Hostname: "192.168.0.1"}
	seVar.ConnectionInfo = &variable.ConnectionInfo{ClientIP: "192.168.0.1"}
	seVar.ConnectionID = 1
	seVar.CurrentDB = "test"
	seVar.InRestrictedALLEGROSQL = true
	txnTS := uint64(406649736972468225)
	costTime := time.Second
	execDetail := execdetails.InterDircDetails{
		ProcessTime:   time.Second * time.Duration(2),
		WaitTime:      time.Minute,
		BackoffTime:   time.Millisecond,
		RequestCount:  2,
		TotalKeys:     10000,
		ProcessedKeys: 20001,
	}
	statsInfos := make(map[string]uint64)
	statsInfos["t1"] = 0
	CausetTasks := &stmtctx.CausetTasksDetails{
		NumCausetTasks:       10,
		AvgProcessTime:    time.Second,
		P90ProcessTime:    time.Second * 2,
		MaxProcessAddress: "10.6.131.78",
		MaxProcessTime:    time.Second * 3,
		AvgWaitTime:       time.Millisecond * 10,
		P90WaitTime:       time.Millisecond * 20,
		MaxWaitTime:       time.Millisecond * 30,
		MaxWaitAddress:    "10.6.131.79",
		MaxBackoffTime:    make(map[string]time.Duration),
		AvgBackoffTime:    make(map[string]time.Duration),
		P90BackoffTime:    make(map[string]time.Duration),
		TotBackoffTime:    make(map[string]time.Duration),
		TotBackoffTimes:   make(map[string]int),
		MaxBackoffAddress: make(map[string]string),
	}

	backoffs := []string{"rpcEinsteinDB", "rpcFIDel", "regionMiss"}
	for _, backoff := range backoffs {
		CausetTasks.MaxBackoffTime[backoff] = time.Millisecond * 200
		CausetTasks.MaxBackoffAddress[backoff] = "127.0.0.1"
		CausetTasks.AvgBackoffTime[backoff] = time.Millisecond * 200
		CausetTasks.P90BackoffTime[backoff] = time.Millisecond * 200
		CausetTasks.TotBackoffTime[backoff] = time.Millisecond * 200
		CausetTasks.TotBackoffTimes[backoff] = 200
	}

	var memMax int64 = 2333
	var diskMax int64 = 6666
	resultFields := `# Txn_start_ts: 406649736972468225
# User@Host: root[root] @ 192.168.0.1 [192.168.0.1]
# Conn_ID: 1
# InterDirc_retry_time: 5.1 InterDirc_retry_count: 3
# Query_time: 1
# Parse_time: 0.00000001
# Compile_time: 0.00000001
# Rewrite_time: 0.000000003 Preproc_subqueries: 2 Preproc_subqueries_time: 0.000000002
# Optimize_time: 0.00000001
# Wait_TS: 0.000000003
# Process_time: 2 Wait_time: 60 Backoff_time: 0.001 Request_count: 2 Total_keys: 10000 Process_keys: 20001
# EDB: test
# Index_names: [t1:a,t2:b]
# Is_internal: true
# Digest: f94c76d7fa8f60e438118752bfbfb71fe9e1934888ac415ddd8625b121af124c
# Stats: t1:pseudo
# Num_cop_tasks: 10
# Cop_proc_avg: 1 Cop_proc_p90: 2 Cop_proc_max: 3 Cop_proc_addr: 10.6.131.78
# Cop_wait_avg: 0.01 Cop_wait_p90: 0.02 Cop_wait_max: 0.03 Cop_wait_addr: 10.6.131.79
# Cop_backoff_regionMiss_total_times: 200 Cop_backoff_regionMiss_total_time: 0.2 Cop_backoff_regionMiss_max_time: 0.2 Cop_backoff_regionMiss_max_addr: 127.0.0.1 Cop_backoff_regionMiss_avg_time: 0.2 Cop_backoff_regionMiss_p90_time: 0.2
# Cop_backoff_rpcFIDel_total_times: 200 Cop_backoff_rpcFIDel_total_time: 0.2 Cop_backoff_rpcFIDel_max_time: 0.2 Cop_backoff_rpcFIDel_max_addr: 127.0.0.1 Cop_backoff_rpcFIDel_avg_time: 0.2 Cop_backoff_rpcFIDel_p90_time: 0.2
# Cop_backoff_rpcEinsteinDB_total_times: 200 Cop_backoff_rpcEinsteinDB_total_time: 0.2 Cop_backoff_rpcEinsteinDB_max_time: 0.2 Cop_backoff_rpcEinsteinDB_max_addr: 127.0.0.1 Cop_backoff_rpcEinsteinDB_avg_time: 0.2 Cop_backoff_rpcEinsteinDB_p90_time: 0.2
# Mem_max: 2333
# Disk_max: 6666
# Prepared: true
# Causet_from_cache: true
# Has_more_results: true
# KV_total: 10
# FIDel_total: 11
# Backoff_total: 12
# Write_sql_response_total: 1
# Succ: true`
	allegrosql := "select * from t;"
	_, digest := BerolinaSQL.NormalizeDigest(allegrosql)
	logItems := &variable.SlowQueryLogItems{
		TxnTS:             txnTS,
		ALLEGROALLEGROSQL:               allegrosql,
		Digest:            digest,
		TimeTotal:         costTime,
		TimeParse:         time.Duration(10),
		TimeCompile:       time.Duration(10),
		TimeOptimize:      time.Duration(10),
		TimeWaitTS:        time.Duration(3),
		IndexNames:        "[t1:a,t2:b]",
		StatsInfos:        statsInfos,
		CausetTasks:          CausetTasks,
		InterDircDetail:        execDetail,
		MemMax:            memMax,
		DiskMax:           diskMax,
		Prepared:          true,
		CausetFromCache:     true,
		HasMoreResults:    true,
		KVTotal:           10 * time.Second,
		FIDelTotal:           11 * time.Second,
		BackoffTotal:      12 * time.Second,
		WriteALLEGROSQLRespTotal: 1 * time.Second,
		Succ:              true,
		RewriteInfo: variable.RewritePhaseInfo{
			DurationRewrite:            3,
			DurationPreprocessSubQuery: 2,
			PreprocessSubQueries:       2,
		},
		InterDircRetryCount: 3,
		InterDircRetryTime:  5*time.Second + time.Millisecond*100,
	}
	logString := seVar.SlowLogFormat(logItems)
	c.Assert(logString, Equals, resultFields+"\n"+allegrosql)

	seVar.CurrentDBChanged = true
	logString = seVar.SlowLogFormat(logItems)
	c.Assert(logString, Equals, resultFields+"\n"+"use test;\n"+allegrosql)
	c.Assert(seVar.CurrentDBChanged, IsFalse)
}

func (*testStochastikSuite) TestIsolationRead(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tiflash", "milevadb"}
	})
	sessVars := variable.NewStochastikVars()
	_, ok := sessVars.IsolationReadEngines[ekv.MilevaDB]
	c.Assert(ok, Equals, true)
	_, ok = sessVars.IsolationReadEngines[ekv.EinsteinDB]
	c.Assert(ok, Equals, false)
	_, ok = sessVars.IsolationReadEngines[ekv.TiFlash]
	c.Assert(ok, Equals, true)
}
