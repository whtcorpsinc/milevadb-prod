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

package variable

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testVarsutilSuite{})

type testVarsutilSuite struct {
}

func (s *testVarsutilSuite) TestMilevaDBOptOn(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		val string
		on  bool
	}{
		{"ON", true},
		{"on", true},
		{"On", true},
		{"1", true},
		{"off", false},
		{"No", false},
		{"0", false},
		{"1.1", false},
		{"", false},
	}
	for _, t := range tbl {
		on := MilevaDBOptOn(t.val)
		c.Assert(on, Equals, t.on)
	}
}

func (s *testVarsutilSuite) TestNewStochastikVars(c *C) {
	defer testleak.AfterTest(c)()
	vars := NewStochastikVars()

	c.Assert(vars.IndexJoinBatchSize, Equals, DefIndexJoinBatchSize)
	c.Assert(vars.IndexLookupSize, Equals, DefIndexLookupSize)
	c.Assert(vars.indexLookupConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.indexSerialScanConcurrency, Equals, DefIndexSerialScanConcurrency)
	c.Assert(vars.indexLookupJoinConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.hashJoinConcurrency, Equals, DefMilevaDBHashJoinConcurrency)
	c.Assert(vars.IndexLookupConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.IndexSerialScanConcurrency(), Equals, DefIndexSerialScanConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.HashJoinConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.AllowBatchCop, Equals, DefMilevaDBAllowBatchCop)
	c.Assert(vars.AllowBCJ, Equals, DefOptBCJ)
	c.Assert(vars.projectionConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.hashAggPartialConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.hashAggFinalConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.windowConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.distALLEGROSQLScanConcurrency, Equals, DefDistALLEGROSQLScanConcurrency)
	c.Assert(vars.ProjectionConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.HashAggPartialConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.HashAggFinalConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.DistALLEGROSQLScanConcurrency(), Equals, DefDistALLEGROSQLScanConcurrency)
	c.Assert(vars.InterlockingDirectorateConcurrency, Equals, DefInterlockingDirectorateConcurrency)
	c.Assert(vars.MaxChunkSize, Equals, DefMaxChunkSize)
	c.Assert(vars.DMLBatchSize, Equals, DefDMLBatchSize)
	c.Assert(vars.MemQuotaQuery, Equals, config.GetGlobalConfig().MemQuotaQuery)
	c.Assert(vars.MemQuotaHashJoin, Equals, int64(DefMilevaDBMemQuotaHashJoin))
	c.Assert(vars.MemQuotaMergeJoin, Equals, int64(DefMilevaDBMemQuotaMergeJoin))
	c.Assert(vars.MemQuotaSort, Equals, int64(DefMilevaDBMemQuotaSort))
	c.Assert(vars.MemQuotaTopn, Equals, int64(DefMilevaDBMemQuotaTopn))
	c.Assert(vars.MemQuotaIndexLookupReader, Equals, int64(DefMilevaDBMemQuotaIndexLookupReader))
	c.Assert(vars.MemQuotaIndexLookupJoin, Equals, int64(DefMilevaDBMemQuotaIndexLookupJoin))
	c.Assert(vars.MemQuotaNestedLoopApply, Equals, int64(DefMilevaDBMemQuotaNestedLoopApply))
	c.Assert(vars.EnableRadixJoin, Equals, DefMilevaDBUseRadixJoin)
	c.Assert(vars.AllowWriteRowID, Equals, DefOptWriteRowID)
	c.Assert(vars.MilevaDBOptJoinReorderThreshold, Equals, DefMilevaDBOptJoinReorderThreshold)
	c.Assert(vars.EnableFastAnalyze, Equals, DefMilevaDBUseFastAnalyze)
	c.Assert(vars.FoundInCausetCache, Equals, DefMilevaDBFoundInCausetCache)
	c.Assert(vars.AllowAutoRandExplicitInsert, Equals, DefMilevaDBAllowAutoRandExplicitInsert)
	c.Assert(vars.ShardAllocateStep, Equals, int64(DefMilevaDBShardAllocateStep))
	c.Assert(vars.EnableChangeDeferredCausetType, Equals, DefMilevaDBChangeDeferredCausetType)

	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.MemQuota))
	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.BatchSize))
}

func assertFieldsGreaterThanZero(c *C, val reflect.Value) {
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		c.Assert(fieldVal.Int(), Greater, int64(0))
	}
}

func (s *testVarsutilSuite) TestVarsutil(c *C) {
	defer testleak.AfterTest(c)()
	v := NewStochastikVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()

	err := SetStochastikSystemVar(v, "autocommit", types.NewStringCauset("1"))
	c.Assert(err, IsNil)
	val, err := GetStochastikSystemVar(v, "autocommit")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(SetStochastikSystemVar(v, "autocommit", types.Causet{}), NotNil)

	// 0 converts to OFF
	err = SetStochastikSystemVar(v, "foreign_key_checks", types.NewStringCauset("0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, "foreign_key_checks")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "OFF")

	// 1/ON is not supported (generates a warning and sets to OFF)
	err = SetStochastikSystemVar(v, "foreign_key_checks", types.NewStringCauset("1"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, "foreign_key_checks")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "OFF")

	err = SetStochastikSystemVar(v, "sql_mode", types.NewStringCauset("strict_trans_blocks"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, "sql_mode")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "STRICT_TRANS_TABLES")
	c.Assert(v.StrictALLEGROSQLMode, IsTrue)
	SetStochastikSystemVar(v, "sql_mode", types.NewStringCauset(""))
	c.Assert(v.StrictALLEGROSQLMode, IsFalse)

	err = SetStochastikSystemVar(v, "character_set_connection", types.NewStringCauset("utf8"))
	c.Assert(err, IsNil)
	err = SetStochastikSystemVar(v, "defCauslation_connection", types.NewStringCauset("utf8_general_ci"))
	c.Assert(err, IsNil)
	charset, defCauslation := v.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(defCauslation, Equals, "utf8_general_ci")

	c.Assert(SetStochastikSystemVar(v, "character_set_results", types.Causet{}), IsNil)

	// Test case for time_zone stochastik variable.
	tests := []struct {
		input        string
		expect       string
		compareValue bool
		diff         time.Duration
		err          error
	}{
		{"Europe/Helsinki", "Europe/Helsinki", true, -2 * time.Hour, nil},
		{"US/Eastern", "US/Eastern", true, 5 * time.Hour, nil},
		//TODO: Check it out and reopen this case.
		//{"SYSTEM", "Local", false, 0},
		{"+10:00", "", true, -10 * time.Hour, nil},
		{"-6:00", "", true, 6 * time.Hour, nil},
		{"+14:00", "", true, -14 * time.Hour, nil},
		{"-12:59", "", true, 12*time.Hour + 59*time.Minute, nil},
		{"+14:01", "", false, -14 * time.Hour, ErrUnknownTimeZone.GenWithStackByArgs("+14:01")},
		{"-13:00", "", false, 13 * time.Hour, ErrUnknownTimeZone.GenWithStackByArgs("-13:00")},
	}
	for _, tt := range tests {
		err = SetStochastikSystemVar(v, TimeZone, types.NewStringCauset(tt.input))
		if tt.err != nil {
			c.Assert(err, NotNil)
			continue
		}

		c.Assert(err, IsNil)
		c.Assert(v.TimeZone.String(), Equals, tt.expect)
		if tt.compareValue {
			SetStochastikSystemVar(v, TimeZone, types.NewStringCauset(tt.input))
			t1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			t2 := time.Date(2000, 1, 1, 0, 0, 0, 0, v.TimeZone)
			c.Assert(t2.Sub(t1), Equals, tt.diff)
		}
	}
	err = SetStochastikSystemVar(v, TimeZone, types.NewStringCauset("6:00"))
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ErrUnknownTimeZone), IsTrue)

	// Test case for allegrosql mode.
	for str, mode := range allegrosql.Str2ALLEGROSQLMode {
		SetStochastikSystemVar(v, "sql_mode", types.NewStringCauset(str))
		if modeParts, exists := allegrosql.CombinationALLEGROSQLMode[str]; exists {
			for _, part := range modeParts {
				mode |= allegrosql.Str2ALLEGROSQLMode[part]
			}
		}
		c.Assert(v.ALLEGROSQLMode, Equals, mode)
	}

	err = SetStochastikSystemVar(v, "milevadb_opt_broadcast_join", types.NewStringCauset("1"))
	c.Assert(err, IsNil)
	err = SetStochastikSystemVar(v, "milevadb_allow_batch_cop", types.NewStringCauset("0"))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)
	err = SetStochastikSystemVar(v, "milevadb_opt_broadcast_join", types.NewStringCauset("0"))
	c.Assert(err, IsNil)
	err = SetStochastikSystemVar(v, "milevadb_allow_batch_cop", types.NewStringCauset("0"))
	c.Assert(err, IsNil)
	err = SetStochastikSystemVar(v, "milevadb_opt_broadcast_join", types.NewStringCauset("1"))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	// Combined sql_mode
	SetStochastikSystemVar(v, "sql_mode", types.NewStringCauset("REAL_AS_FLOAT,ANSI_QUOTES"))
	c.Assert(v.ALLEGROSQLMode, Equals, allegrosql.ModeRealAsFloat|allegrosql.ModeANSIQuotes)

	// Test case for milevadb_index_serial_scan_concurrency.
	c.Assert(v.IndexSerialScanConcurrency(), Equals, DefIndexSerialScanConcurrency)
	SetStochastikSystemVar(v, MilevaDBIndexSerialScanConcurrency, types.NewStringCauset("4"))
	c.Assert(v.IndexSerialScanConcurrency(), Equals, 4)

	// Test case for milevadb_batch_insert.
	c.Assert(v.BatchInsert, IsFalse)
	SetStochastikSystemVar(v, MilevaDBBatchInsert, types.NewStringCauset("1"))
	c.Assert(v.BatchInsert, IsTrue)

	c.Assert(v.InitChunkSize, Equals, 32)
	c.Assert(v.MaxChunkSize, Equals, 1024)
	err = SetStochastikSystemVar(v, MilevaDBMaxChunkSize, types.NewStringCauset("2"))
	c.Assert(err, NotNil)
	err = SetStochastikSystemVar(v, MilevaDBInitChunkSize, types.NewStringCauset("1024"))
	c.Assert(err, NotNil)

	// Test case for MilevaDBConfig stochastik variable.
	err = SetStochastikSystemVar(v, MilevaDBConfig, types.NewStringCauset("abc"))
	c.Assert(terror.ErrorEqual(err, ErrReadOnly), IsTrue)
	val, err = GetStochastikSystemVar(v, MilevaDBConfig)
	c.Assert(err, IsNil)
	bVal, err := json.MarshalIndent(config.GetGlobalConfig(), "", "\t")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, string(bVal))

	SetStochastikSystemVar(v, MilevaDBEnableStreaming, types.NewStringCauset("1"))
	val, err = GetStochastikSystemVar(v, MilevaDBEnableStreaming)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(v.EnableStreaming, Equals, true)
	SetStochastikSystemVar(v, MilevaDBEnableStreaming, types.NewStringCauset("0"))
	val, err = GetStochastikSystemVar(v, MilevaDBEnableStreaming)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.EnableStreaming, Equals, false)

	c.Assert(v.OptimizerSelectivityLevel, Equals, DefMilevaDBOptimizerSelectivityLevel)
	SetStochastikSystemVar(v, MilevaDBOptimizerSelectivityLevel, types.NewIntCauset(1))
	c.Assert(v.OptimizerSelectivityLevel, Equals, 1)

	err = SetStochastikSystemVar(v, MilevaDBDBSReorgWorkerCount, types.NewIntCauset(-1))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	SetStochastikSystemVar(v, MilevaDBDBSReorgWorkerCount, types.NewIntCauset(int64(maxDBSReorgWorkerCount)+1))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	err = SetStochastikSystemVar(v, MilevaDBRetryLimit, types.NewStringCauset("3"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBRetryLimit)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "3")
	c.Assert(v.RetryLimit, Equals, int64(3))

	c.Assert(v.EnableBlockPartition, Equals, "")
	err = SetStochastikSystemVar(v, MilevaDBEnableBlockPartition, types.NewStringCauset("on"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBEnableBlockPartition)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "on")
	c.Assert(v.EnableBlockPartition, Equals, "on")

	c.Assert(v.MilevaDBOptJoinReorderThreshold, Equals, DefMilevaDBOptJoinReorderThreshold)
	err = SetStochastikSystemVar(v, MilevaDBOptJoinReorderThreshold, types.NewIntCauset(5))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptJoinReorderThreshold)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5")
	c.Assert(v.MilevaDBOptJoinReorderThreshold, Equals, 5)

	err = SetStochastikSystemVar(v, MilevaDBCheckMb4ValueInUTF8, types.NewStringCauset("1"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBCheckMb4ValueInUTF8)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, true)
	err = SetStochastikSystemVar(v, MilevaDBCheckMb4ValueInUTF8, types.NewStringCauset("0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBCheckMb4ValueInUTF8)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, false)

	SetStochastikSystemVar(v, MilevaDBLowResolutionTSO, types.NewStringCauset("1"))
	val, err = GetStochastikSystemVar(v, MilevaDBLowResolutionTSO)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(v.LowResolutionTSO, Equals, true)
	SetStochastikSystemVar(v, MilevaDBLowResolutionTSO, types.NewStringCauset("0"))
	val, err = GetStochastikSystemVar(v, MilevaDBLowResolutionTSO)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.LowResolutionTSO, Equals, false)

	c.Assert(v.CorrelationThreshold, Equals, 0.9)
	err = SetStochastikSystemVar(v, MilevaDBOptCorrelationThreshold, types.NewStringCauset("0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptCorrelationThreshold)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.CorrelationThreshold, Equals, float64(0))

	c.Assert(v.CPUFactor, Equals, 3.0)
	err = SetStochastikSystemVar(v, MilevaDBOptCPUFactor, types.NewStringCauset("5.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptCPUFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.CPUFactor, Equals, 5.0)

	c.Assert(v.CopCPUFactor, Equals, 3.0)
	err = SetStochastikSystemVar(v, MilevaDBOptCopCPUFactor, types.NewStringCauset("5.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptCopCPUFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.CopCPUFactor, Equals, 5.0)

	c.Assert(v.CopTiFlashConcurrencyFactor, Equals, 24.0)
	err = SetStochastikSystemVar(v, MilevaDBOptTiFlashConcurrencyFactor, types.NewStringCauset("5.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptTiFlashConcurrencyFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.CopCPUFactor, Equals, 5.0)

	c.Assert(v.NetworkFactor, Equals, 1.0)
	err = SetStochastikSystemVar(v, MilevaDBOptNetworkFactor, types.NewStringCauset("3.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptNetworkFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "3.0")
	c.Assert(v.NetworkFactor, Equals, 3.0)

	c.Assert(v.ScanFactor, Equals, 1.5)
	err = SetStochastikSystemVar(v, MilevaDBOptScanFactor, types.NewStringCauset("3.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptScanFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "3.0")
	c.Assert(v.ScanFactor, Equals, 3.0)

	c.Assert(v.DescScanFactor, Equals, 3.0)
	err = SetStochastikSystemVar(v, MilevaDBOptDescScanFactor, types.NewStringCauset("5.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptDescScanFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.DescScanFactor, Equals, 5.0)

	c.Assert(v.SeekFactor, Equals, 20.0)
	err = SetStochastikSystemVar(v, MilevaDBOptSeekFactor, types.NewStringCauset("50.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptSeekFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "50.0")
	c.Assert(v.SeekFactor, Equals, 50.0)

	c.Assert(v.MemoryFactor, Equals, 0.001)
	err = SetStochastikSystemVar(v, MilevaDBOptMemoryFactor, types.NewStringCauset("1.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptMemoryFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1.0")
	c.Assert(v.MemoryFactor, Equals, 1.0)

	c.Assert(v.DiskFactor, Equals, 1.5)
	err = SetStochastikSystemVar(v, MilevaDBOptDiskFactor, types.NewStringCauset("1.1"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptDiskFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1.1")
	c.Assert(v.DiskFactor, Equals, 1.1)

	c.Assert(v.ConcurrencyFactor, Equals, 3.0)
	err = SetStochastikSystemVar(v, MilevaDBOptConcurrencyFactor, types.NewStringCauset("5.0"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBOptConcurrencyFactor)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "5.0")
	c.Assert(v.ConcurrencyFactor, Equals, 5.0)

	SetStochastikSystemVar(v, MilevaDBReplicaRead, types.NewStringCauset("follower"))
	val, err = GetStochastikSystemVar(v, MilevaDBReplicaRead)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "follower")
	c.Assert(v.GetReplicaRead(), Equals, ekv.ReplicaReadFollower)
	SetStochastikSystemVar(v, MilevaDBReplicaRead, types.NewStringCauset("leader"))
	val, err = GetStochastikSystemVar(v, MilevaDBReplicaRead)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "leader")
	c.Assert(v.GetReplicaRead(), Equals, ekv.ReplicaReadLeader)
	SetStochastikSystemVar(v, MilevaDBReplicaRead, types.NewStringCauset("leader-and-follower"))
	val, err = GetStochastikSystemVar(v, MilevaDBReplicaRead)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "leader-and-follower")
	c.Assert(v.GetReplicaRead(), Equals, ekv.ReplicaReadMixed)

	err = SetStochastikSystemVar(v, MilevaDBEnableStmtSummary, types.NewStringCauset("on"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBEnableStmtSummary)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")

	err = SetStochastikSystemVar(v, MilevaDBStmtSummaryRefreshInterval, types.NewStringCauset("10"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBStmtSummaryRefreshInterval)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")

	err = SetStochastikSystemVar(v, MilevaDBStmtSummaryHistorySize, types.NewStringCauset("10"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBStmtSummaryHistorySize)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")

	err = SetStochastikSystemVar(v, MilevaDBStmtSummaryMaxStmtCount, types.NewStringCauset("10"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBStmtSummaryMaxStmtCount)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")
	err = SetStochastikSystemVar(v, MilevaDBStmtSummaryMaxStmtCount, types.NewStringCauset("a"))
	c.Assert(err, ErrorMatches, ".*Incorrect argument type to variable 'milevadb_stmt_summary_max_stmt_count'")

	err = SetStochastikSystemVar(v, MilevaDBStmtSummaryMaxALLEGROSQLLength, types.NewStringCauset("10"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBStmtSummaryMaxALLEGROSQLLength)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "10")
	err = SetStochastikSystemVar(v, MilevaDBStmtSummaryMaxALLEGROSQLLength, types.NewStringCauset("a"))
	c.Assert(err, ErrorMatches, ".*Incorrect argument type to variable 'milevadb_stmt_summary_max_sql_length'")

	err = SetStochastikSystemVar(v, MilevaDBFoundInCausetCache, types.NewStringCauset("1"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBFoundInCausetCache)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.systems[MilevaDBFoundInCausetCache], Equals, "1")

	err = SetStochastikSystemVar(v, MilevaDBEnableChangeDeferredCausetType, types.NewStringCauset("on"))
	c.Assert(err, IsNil)
	val, err = GetStochastikSystemVar(v, MilevaDBEnableChangeDeferredCausetType)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(v.systems[MilevaDBEnableChangeDeferredCausetType], Equals, "1")
}

func (s *testVarsutilSuite) TestSetOverflowBehave(c *C) {
	ddRegWorker := maxDBSReorgWorkerCount + 1
	SetDBSReorgWorkerCounter(ddRegWorker)
	c.Assert(maxDBSReorgWorkerCount, Equals, GetDBSReorgWorkerCounter())

	dbsReorgBatchSize := MaxDBSReorgBatchSize + 1
	SetDBSReorgBatchSize(dbsReorgBatchSize)
	c.Assert(MaxDBSReorgBatchSize, Equals, GetDBSReorgBatchSize())
	dbsReorgBatchSize = MinDBSReorgBatchSize - 1
	SetDBSReorgBatchSize(dbsReorgBatchSize)
	c.Assert(MinDBSReorgBatchSize, Equals, GetDBSReorgBatchSize())

	val := milevadbOptInt64("a", 1)
	c.Assert(val, Equals, int64(1))
	val2 := milevadbOptFloat64("b", 1.2)
	c.Assert(val2, Equals, 1.2)
}

func (s *testVarsutilSuite) TestValidate(c *C) {
	v := NewStochastikVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()
	v.TimeZone = time.UTC

	tests := []struct {
		key   string
		value string
		error bool
	}{
		{MilevaDBAutoAnalyzeStartTime, "15:04", false},
		{MilevaDBAutoAnalyzeStartTime, "15:04 -0700", false},
		{DelayKeyWrite, "ON", false},
		{DelayKeyWrite, "OFF", false},
		{DelayKeyWrite, "ALL", false},
		{DelayKeyWrite, "3", true},
		{ForeignKeyChecks, "3", true},
		{MaxSpRecursionDepth, "256", false},
		{StochastikTrackGtids, "OFF", false},
		{StochastikTrackGtids, "OWN_GTID", false},
		{StochastikTrackGtids, "ALL_GTIDS", false},
		{StochastikTrackGtids, "ON", true},
		{EnforceGtidConsistency, "OFF", false},
		{EnforceGtidConsistency, "ON", false},
		{EnforceGtidConsistency, "WARN", false},
		{QueryCacheType, "OFF", false},
		{QueryCacheType, "ON", false},
		{QueryCacheType, "DEMAND", false},
		{QueryCacheType, "3", true},
		{SecureAuth, "1", false},
		{SecureAuth, "3", true},
		{MyISAMUseMmap, "ON", false},
		{MyISAMUseMmap, "OFF", false},
		{MilevaDBEnableBlockPartition, "ON", false},
		{MilevaDBEnableBlockPartition, "OFF", false},
		{MilevaDBEnableBlockPartition, "AUTO", false},
		{MilevaDBEnableBlockPartition, "UN", true},
		{MilevaDBOptCorrelationExpFactor, "a", true},
		{MilevaDBOptCorrelationExpFactor, "-10", true},
		{MilevaDBOptCorrelationThreshold, "a", true},
		{MilevaDBOptCorrelationThreshold, "-2", true},
		{MilevaDBOptCPUFactor, "a", true},
		{MilevaDBOptCPUFactor, "-2", true},
		{MilevaDBOptTiFlashConcurrencyFactor, "-2", true},
		{MilevaDBOptCopCPUFactor, "a", true},
		{MilevaDBOptCopCPUFactor, "-2", true},
		{MilevaDBOptNetworkFactor, "a", true},
		{MilevaDBOptNetworkFactor, "-2", true},
		{MilevaDBOptScanFactor, "a", true},
		{MilevaDBOptScanFactor, "-2", true},
		{MilevaDBOptDescScanFactor, "a", true},
		{MilevaDBOptDescScanFactor, "-2", true},
		{MilevaDBOptSeekFactor, "a", true},
		{MilevaDBOptSeekFactor, "-2", true},
		{MilevaDBOptMemoryFactor, "a", true},
		{MilevaDBOptMemoryFactor, "-2", true},
		{MilevaDBOptDiskFactor, "a", true},
		{MilevaDBOptDiskFactor, "-2", true},
		{MilevaDBOptConcurrencyFactor, "a", true},
		{MilevaDBOptConcurrencyFactor, "-2", true},
		{TxnIsolation, "READ-UNCOMMITTED", true},
		{MilevaDBInitChunkSize, "a", true},
		{MilevaDBInitChunkSize, "-1", true},
		{MilevaDBMaxChunkSize, "a", true},
		{MilevaDBMaxChunkSize, "-1", true},
		{MilevaDBOptJoinReorderThreshold, "a", true},
		{MilevaDBOptJoinReorderThreshold, "-1", true},
		{MilevaDBReplicaRead, "invalid", true},
		{MilevaDBTxnMode, "invalid", true},
		{MilevaDBTxnMode, "pessimistic", false},
		{MilevaDBTxnMode, "optimistic", false},
		{MilevaDBTxnMode, "", false},
		{MilevaDBIsolationReadEngines, "", true},
		{MilevaDBIsolationReadEngines, "einsteindb", false},
		{MilevaDBIsolationReadEngines, "EinsteinDB,tiflash", false},
		{MilevaDBIsolationReadEngines, "   einsteindb,   tiflash  ", false},
		{MilevaDBShardAllocateStep, "ad", true},
		{MilevaDBShardAllocateStep, "-123", false},
		{MilevaDBShardAllocateStep, "128", false},
		{MilevaDBEnableAmendPessimisticTxn, "0", false},
		{MilevaDBEnableAmendPessimisticTxn, "1", false},
		{MilevaDBEnableAmendPessimisticTxn, "256", true},
	}

	for _, t := range tests {
		_, err := ValidateSetSystemVar(v, t.key, t.value, ScopeGlobal)
		if t.error {
			c.Assert(err, NotNil, Commentf("%v got err=%v", t, err))
		} else {
			c.Assert(err, IsNil, Commentf("%v got err=%v", t, err))
		}
	}

}

func (s *testVarsutilSuite) TestValidateStmtSummary(c *C) {
	v := NewStochastikVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()
	v.TimeZone = time.UTC

	tests := []struct {
		key   string
		value string
		error bool
		scope ScopeFlag
	}{
		{MilevaDBEnableStmtSummary, "a", true, ScopeStochastik},
		{MilevaDBEnableStmtSummary, "-1", true, ScopeStochastik},
		{MilevaDBEnableStmtSummary, "", false, ScopeStochastik},
		{MilevaDBEnableStmtSummary, "", true, ScopeGlobal},
		{MilevaDBStmtSummaryInternalQuery, "a", true, ScopeStochastik},
		{MilevaDBStmtSummaryInternalQuery, "-1", true, ScopeStochastik},
		{MilevaDBStmtSummaryInternalQuery, "", false, ScopeStochastik},
		{MilevaDBStmtSummaryInternalQuery, "", true, ScopeGlobal},
		{MilevaDBStmtSummaryRefreshInterval, "a", true, ScopeStochastik},
		{MilevaDBStmtSummaryRefreshInterval, "", false, ScopeStochastik},
		{MilevaDBStmtSummaryRefreshInterval, "", true, ScopeGlobal},
		{MilevaDBStmtSummaryRefreshInterval, "0", true, ScopeGlobal},
		{MilevaDBStmtSummaryRefreshInterval, "99999999999", true, ScopeGlobal},
		{MilevaDBStmtSummaryHistorySize, "a", true, ScopeStochastik},
		{MilevaDBStmtSummaryHistorySize, "", false, ScopeStochastik},
		{MilevaDBStmtSummaryHistorySize, "", true, ScopeGlobal},
		{MilevaDBStmtSummaryHistorySize, "0", false, ScopeGlobal},
		{MilevaDBStmtSummaryHistorySize, "-1", true, ScopeGlobal},
		{MilevaDBStmtSummaryHistorySize, "99999999", true, ScopeGlobal},
		{MilevaDBStmtSummaryMaxStmtCount, "a", true, ScopeStochastik},
		{MilevaDBStmtSummaryMaxStmtCount, "", false, ScopeStochastik},
		{MilevaDBStmtSummaryMaxStmtCount, "", true, ScopeGlobal},
		{MilevaDBStmtSummaryMaxStmtCount, "0", true, ScopeGlobal},
		{MilevaDBStmtSummaryMaxStmtCount, "99999999", true, ScopeGlobal},
		{MilevaDBStmtSummaryMaxALLEGROSQLLength, "a", true, ScopeStochastik},
		{MilevaDBStmtSummaryMaxALLEGROSQLLength, "", false, ScopeStochastik},
		{MilevaDBStmtSummaryMaxALLEGROSQLLength, "", true, ScopeGlobal},
		{MilevaDBStmtSummaryMaxALLEGROSQLLength, "0", false, ScopeGlobal},
		{MilevaDBStmtSummaryMaxALLEGROSQLLength, "-1", true, ScopeGlobal},
		{MilevaDBStmtSummaryMaxALLEGROSQLLength, "99999999999", true, ScopeGlobal},
	}

	for _, t := range tests {
		_, err := ValidateSetSystemVar(v, t.key, t.value, t.scope)
		if t.error {
			c.Assert(err, NotNil, Commentf("%v got err=%v", t, err))
		} else {
			c.Assert(err, IsNil, Commentf("%v got err=%v", t, err))
		}
	}
}

func (s *testVarsutilSuite) TestConcurrencyVariables(c *C) {
	defer testleak.AfterTest(c)()
	vars := NewStochastikVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor()

	wdConcurrency := 2
	c.Assert(vars.windowConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.WindowConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	err := SetStochastikSystemVar(vars, MilevaDBWindowConcurrency, types.NewIntCauset(int64(wdConcurrency)))
	c.Assert(err, IsNil)
	c.Assert(vars.windowConcurrency, Equals, wdConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, wdConcurrency)

	c.Assert(vars.indexLookupConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.IndexLookupConcurrency(), Equals, DefInterlockingDirectorateConcurrency)
	exeConcurrency := DefInterlockingDirectorateConcurrency + 1
	err = SetStochastikSystemVar(vars, MilevaDBInterlockingDirectorateConcurrency, types.NewIntCauset(int64(exeConcurrency)))
	c.Assert(err, IsNil)
	c.Assert(vars.indexLookupConcurrency, Equals, ConcurrencyUnset)
	c.Assert(vars.IndexLookupConcurrency(), Equals, exeConcurrency)
	c.Assert(vars.WindowConcurrency(), Equals, wdConcurrency)
}
