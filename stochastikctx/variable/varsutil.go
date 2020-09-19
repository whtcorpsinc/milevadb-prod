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
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
)

// secondsPerYear represents seconds in a normal year. Leap year is not considered here.
const secondsPerYear = 60 * 60 * 24 * 365

// SetDBSReorgWorkerCounter sets dbsReorgWorkerCounter count.
// Max worker count is maxDBSReorgWorkerCount.
func SetDBSReorgWorkerCounter(cnt int32) {
	if cnt > maxDBSReorgWorkerCount {
		cnt = maxDBSReorgWorkerCount
	}
	atomic.StoreInt32(&dbsReorgWorkerCounter, cnt)
}

// GetDBSReorgWorkerCounter gets dbsReorgWorkerCounter.
func GetDBSReorgWorkerCounter() int32 {
	return atomic.LoadInt32(&dbsReorgWorkerCounter)
}

// SetDBSReorgBatchSize sets dbsReorgBatchSize size.
// Max batch size is MaxDBSReorgBatchSize.
func SetDBSReorgBatchSize(cnt int32) {
	if cnt > MaxDBSReorgBatchSize {
		cnt = MaxDBSReorgBatchSize
	}
	if cnt < MinDBSReorgBatchSize {
		cnt = MinDBSReorgBatchSize
	}
	atomic.StoreInt32(&dbsReorgBatchSize, cnt)
}

// GetDBSReorgBatchSize gets dbsReorgBatchSize.
func GetDBSReorgBatchSize() int32 {
	return atomic.LoadInt32(&dbsReorgBatchSize)
}

// SetDBSErrorCountLimit sets dbsErrorCountlimit size.
func SetDBSErrorCountLimit(cnt int64) {
	atomic.StoreInt64(&dbsErrorCountlimit, cnt)
}

// GetDBSErrorCountLimit gets dbsErrorCountlimit size.
func GetDBSErrorCountLimit() int64 {
	return atomic.LoadInt64(&dbsErrorCountlimit)
}

// SetMaxDeltaSchemaCount sets maxDeltaSchemaCount size.
func SetMaxDeltaSchemaCount(cnt int64) {
	atomic.StoreInt64(&maxDeltaSchemaCount, cnt)
}

// GetMaxDeltaSchemaCount gets maxDeltaSchemaCount size.
func GetMaxDeltaSchemaCount() int64 {
	return atomic.LoadInt64(&maxDeltaSchemaCount)
}

// GetStochastikSystemVar gets a system variable.
// If it is a stochastik only variable, use the default value defined in code.
// Returns error if there is no such variable.
func GetStochastikSystemVar(s *StochastikVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetStochastikOnlySysVars(s, key)
	if err != nil || ok {
		return gVal, err
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", err
	}
	s.systems[key] = gVal
	return gVal, nil
}

// GetStochastikOnlySysVars get the default value defined in code for stochastik only variable.
// The return bool value indicates whether it's a stochastik only variable.
func GetStochastikOnlySysVars(s *StochastikVars, key string) (string, bool, error) {
	sysVar := SysVars[key]
	if sysVar == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
	}
	// For virtual system variables:
	switch sysVar.Name {
	case MilevaDBCurrentTS:
		return fmt.Sprintf("%d", s.TxnCtx.StartTS), true, nil
	case MilevaDBLastTxnInfo:
		info, err := json.Marshal(s.LastTxnInfo)
		if err != nil {
			return "", true, err
		}
		return string(info), true, nil
	case MilevaDBGeneralLog:
		return fmt.Sprintf("%d", atomic.LoadUint32(&ProcessGeneralLog)), true, nil
	case MilevaDBPProfALLEGROSQLCPU:
		val := "0"
		if EnablePProfALLEGROSQLCPU.Load() {
			val = "1"
		}
		return val, true, nil
	case MilevaDBExpensiveQueryTimeThreshold:
		return fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)), true, nil
	case MilevaDBConfig:
		conf := config.GetGlobalConfig()
		j, err := json.MarshalIndent(conf, "", "\t")
		if err != nil {
			return "", false, err
		}
		return string(j), true, nil
	case MilevaDBForcePriority:
		return allegrosql.Priority2Str[allegrosql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], true, nil
	case MilevaDBDBSSlowOprThreshold:
		return strconv.FormatUint(uint64(atomic.LoadUint32(&DBSSlowOprThreshold)), 10), true, nil
	case PluginDir:
		return config.GetGlobalConfig().Plugin.Dir, true, nil
	case PluginLoad:
		return config.GetGlobalConfig().Plugin.Load, true, nil
	case MilevaDBSlowLogThreshold:
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.SlowThreshold), 10), true, nil
	case MilevaDBRecordPlanInSlowLog:
		return strconv.FormatUint(uint64(atomic.LoadUint32(&config.GetGlobalConfig().Log.RecordPlanInSlowLog)), 10), true, nil
	case MilevaDBEnableSlowLog:
		return BoolToIntStr(config.GetGlobalConfig().Log.EnableSlowLog), true, nil
	case MilevaDBQueryLogMaxLen:
		return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen), 10), true, nil
	case MilevaDBCheckMb4ValueInUTF8:
		return BoolToIntStr(config.GetGlobalConfig().CheckMb4ValueInUTF8), true, nil
	case MilevaDBCapturePlanBaseline:
		return CapturePlanBaseline.GetVal(), true, nil
	case MilevaDBFoundInPlanCache:
		return BoolToIntStr(s.PrevFoundInPlanCache), true, nil
	case MilevaDBEnableDefCauslectExecutionInfo:
		return BoolToIntStr(config.GetGlobalConfig().EnableDefCauslectExecutionInfo), true, nil
	}
	sVal, ok := s.GetSystemVar(key)
	if ok {
		return sVal, true, nil
	}
	if sysVar.Scope&ScopeGlobal == 0 {
		// None-Global variable can use pre-defined default value.
		return sysVar.Value, true, nil
	}
	return "", false, nil
}

// GetGlobalSystemVar gets a global system variable.
func GetGlobalSystemVar(s *StochastikVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetScopeNoneSystemVar(key)
	if err != nil || ok {
		return gVal, err
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", err
	}
	return gVal, nil
}

// GetScopeNoneSystemVar checks the validation of `key`,
// and return the default value if its scope is `ScopeNone`.
func GetScopeNoneSystemVar(key string) (string, bool, error) {
	sysVar := SysVars[key]
	if sysVar == nil {
		return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
	}
	if sysVar.Scope == ScopeNone {
		return sysVar.Value, true, nil
	}
	return "", false, nil
}

// epochShiftBits is used to reserve logical part of the timestamp.
const epochShiftBits = 18

// SetStochastikSystemVar sets system variable and uFIDelates StochastikVars states.
func SetStochastikSystemVar(vars *StochastikVars, name string, value types.Causet) error {
	name = strings.ToLower(name)
	sysVar := SysVars[name]
	if sysVar == nil {
		return ErrUnknownSystemVar
	}
	sVal := ""
	var err error
	if !value.IsNull() {
		sVal, err = value.ToString()
	}
	if err != nil {
		return err
	}
	sVal, err = ValidateSetSystemVar(vars, name, sVal, ScopeStochastik)
	if err != nil {
		return err
	}
	CheckDeprecationSetSystemVar(vars, name)
	return vars.SetSystemVar(name, sVal)
}

// ValidateGetSystemVar checks if system variable exists and validates its scope when get system variable.
func ValidateGetSystemVar(name string, isGlobal bool) error {
	sysVar, exists := SysVars[name]
	if !exists {
		return ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	switch sysVar.Scope {
	case ScopeGlobal:
		if !isGlobal {
			return ErrIncorrectScope.GenWithStackByArgs(name, "GLOBAL")
		}
	case ScopeStochastik:
		if isGlobal {
			return ErrIncorrectScope.GenWithStackByArgs(name, "SESSION")
		}
	}
	return nil
}

func checkUInt64SystemVar(name, value string, min, max uint64, vars *StochastikVars) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if value[0] == '-' {
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	if val > max {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	return value, nil
}

func checkInt64SystemVar(name, value string, min, max int64, vars *StochastikVars) (string, error) {
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	if val > max {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	return value, nil
}

func checkInt64SystemVarWithError(name, value string, min, max int64) (string, error) {
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min || val > max {
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	}
	return value, nil
}

const (
	// initChunkSizeUpperBound indicates upper bound value of milevadb_init_chunk_size.
	initChunkSizeUpperBound = 32
	// maxChunkSizeLowerBound indicates lower bound value of milevadb_max_chunk_size.
	maxChunkSizeLowerBound = 32
)

// CheckDeprecationSetSystemVar checks if the system variable is deprecated.
func CheckDeprecationSetSystemVar(s *StochastikVars, name string) {
	switch name {
	case MilevaDBIndexLookupConcurrency, MilevaDBIndexLookupJoinConcurrency,
		MilevaDBHashJoinConcurrency, MilevaDBHashAggPartialConcurrency, MilevaDBHashAggFinalConcurrency,
		MilevaDBProjectionConcurrency, MilevaDBWindowConcurrency:
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, MilevaDBExecutorConcurrency))
	case MilevaDBMemQuotaHashJoin, MilevaDBMemQuotaMergeJoin,
		MilevaDBMemQuotaSort, MilevaDBMemQuotaTopn,
		MilevaDBMemQuotaIndexLookupReader, MilevaDBMemQuotaIndexLookupJoin,
		MilevaDBMemQuotaNestedLoopApply:
		s.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(name, MilevaDBMemQuotaQuery))
	}
}

// ValidateSetSystemVar checks if system variable satisfies specific restriction.
func ValidateSetSystemVar(vars *StochastikVars, name string, value string, scope ScopeFlag) (string, error) {
	if strings.EqualFold(value, "DEFAULT") {
		if val := GetSysVar(name); val != nil {
			return val.Value, nil
		}
		return value, ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	switch name {
	case ConnectTimeout:
		return checkUInt64SystemVar(name, value, 2, secondsPerYear, vars)
	case DefaultWeekFormat:
		return checkUInt64SystemVar(name, value, 0, 7, vars)
	case DelayKeyWrite:
		if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		} else if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ALL") || value == "2" {
			return "ALL", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case FlushTime:
		return checkUInt64SystemVar(name, value, 0, secondsPerYear, vars)
	case ForeignKeyChecks:
		if strings.EqualFold(value, "ON") || value == "1" {
			// MilevaDB does not yet support foreign keys.
			// For now, resist the change and show a warning.
			vars.StmtCtx.AppendWarning(ErrUnsupportedValueForVar.GenWithStackByArgs(name, value))
			return "OFF", nil
		} else if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case GroupConcatMaxLen:
		// https://dev.allegrosql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
		// Minimum Value 4
		// Maximum Value (64-bit platforms) 18446744073709551615
		// Maximum Value (32-bit platforms) 4294967295
		maxLen := uint64(math.MaxUint64)
		if mathutil.IntBits == 32 {
			maxLen = uint64(math.MaxUint32)
		}
		return checkUInt64SystemVar(name, value, 4, maxLen, vars)
	case InteractiveTimeout:
		return checkUInt64SystemVar(name, value, 1, secondsPerYear, vars)
	case InnodbCommitConcurrency:
		return checkUInt64SystemVar(name, value, 0, 1000, vars)
	case InnodbFastShutdown:
		return checkUInt64SystemVar(name, value, 0, 2, vars)
	case InnodbLockWaitTimeout:
		return checkUInt64SystemVar(name, value, 1, 1073741824, vars)
	// See "https://dev.allegrosql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_allowed_packet"
	case MaxAllowedPacket:
		return checkUInt64SystemVar(name, value, 1024, MaxOfMaxAllowedPacket, vars)
	case MaxConnections:
		return checkUInt64SystemVar(name, value, 1, 100000, vars)
	case MaxConnectErrors:
		return checkUInt64SystemVar(name, value, 1, math.MaxUint64, vars)
	case MaxSortLength:
		return checkUInt64SystemVar(name, value, 4, 8388608, vars)
	case MaxSpRecursionDepth:
		return checkUInt64SystemVar(name, value, 0, 255, vars)
	case MaxUserConnections:
		return checkUInt64SystemVar(name, value, 0, 4294967295, vars)
	case OldPasswords:
		return checkUInt64SystemVar(name, value, 0, 2, vars)
	case MilevaDBMaxDeltaSchemaCount:
		return checkInt64SystemVar(name, value, 100, 16384, vars)
	case StochastikTrackGtids:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "OWN_GTID") || value == "1" {
			return "OWN_GTID", nil
		} else if strings.EqualFold(value, "ALL_GTIDS") || value == "2" {
			return "ALL_GTIDS", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case ALLEGROSQLSelectLimit:
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case MilevaDBStoreLimit:
		return checkInt64SystemVar(name, value, 0, math.MaxInt64, vars)
	case SyncBinlog:
		return checkUInt64SystemVar(name, value, 0, 4294967295, vars)
	case BlockDefinitionCache:
		return checkUInt64SystemVar(name, value, 400, 524288, vars)
	case TmpBlockSize:
		return checkUInt64SystemVar(name, value, 1024, math.MaxUint64, vars)
	case WaitTimeout:
		return checkUInt64SystemVar(name, value, 0, 31536000, vars)
	case MaxPreparedStmtCount:
		return checkInt64SystemVar(name, value, -1, 1048576, vars)
	case AutoIncrementIncrement, AutoIncrementOffset:
		return checkUInt64SystemVar(name, value, 1, math.MaxUint16, vars)
	case TimeZone:
		if strings.EqualFold(value, "SYSTEM") {
			return "SYSTEM", nil
		}
		_, err := parseTimeZone(value)
		return value, err
	case ValidatePasswordLength, ValidatePasswordNumberCount:
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case WarningCount, ErrorCount:
		return value, ErrReadOnly.GenWithStackByArgs(name)
	case EnforceGtidConsistency:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		} else if strings.EqualFold(value, "WARN") || value == "2" {
			return "WARN", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case QueryCacheType:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		} else if strings.EqualFold(value, "DEMAND") || value == "2" {
			return "DEMAND", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case SecureAuth:
		if strings.EqualFold(value, "ON") || value == "1" {
			return "1", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case WindowingUseHighPrecision:
		if strings.EqualFold(value, "OFF") || value == "0" {
			return "OFF", nil
		} else if strings.EqualFold(value, "ON") || value == "1" {
			return "ON", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MilevaDBOptBCJ:
		if (strings.EqualFold(value, "ON") || value == "1") && vars.AllowBatchCop == 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs("Can't set Broadcast Join to 1 but milevadb_allow_batch_cop is 0, please active batch cop at first.")
		}
		return value, nil
	case MilevaDBSkipUTF8Check, MilevaDBSkipASCIICheck, MilevaDBOptAggPushDown,
		MilevaDBOptDistinctAggPushDown, MilevaDBOptInSubqToJoinAnPosetDagg, MilevaDBEnableFastAnalyze,
		MilevaDBBatchInsert, MilevaDBDisableTxnAutoRetry, MilevaDBEnableStreaming, MilevaDBEnableChunkRPC,
		MilevaDBBatchDelete, MilevaDBBatchCommit, MilevaDBEnableCascadesPlanner, MilevaDBEnableWindowFunction, MilevaDBPProfALLEGROSQLCPU,
		MilevaDBLowResolutionTSO, MilevaDBEnableIndexMerge, MilevaDBEnableNoopFuncs,
		MilevaDBCheckMb4ValueInUTF8, MilevaDBEnableSlowLog, MilevaDBRecordPlanInSlowLog,
		MilevaDBScatterRegion, MilevaDBGeneralLog, MilevaDBConstraintCheckInPlace, MilevaDBEnableVectorizedExpression,
		MilevaDBFoundInPlanCache, MilevaDBEnableDefCauslectExecutionInfo, MilevaDBAllowAutoRandExplicitInsert,
		MilevaDBEnableClusteredIndex, MilevaDBEnableTelemetry, MilevaDBEnableChangeDeferredCausetType, MilevaDBEnableAmendPessimisticTxn:
		fallthrough
	case GeneralLog, AvoidTemporalUpgrade, BigBlocks, CheckProxyUsers, LogBin,
		CoreFile, EndMakersInJSON, ALLEGROSQLLogBin, OfflineMode, PseudoSlaveMode, LowPriorityUFIDelates,
		SkipNameResolve, ALLEGROSQLSafeUFIDelates, serverReadOnly, SlaveAllowBatching,
		Flush, PerformanceSchema, LocalInFile, ShowOldTemporals, KeepFilesOnCreate, AutoCommit,
		ALLEGROSQLWarnings, UniqueChecks, OldAlterBlock, LogBinTrustFunctionCreators, ALLEGROSQLBigSelects,
		BinlogDirectNonTransactionalUFIDelates, ALLEGROSQLQuoteShowCreate, AutomaticSpPrivileges,
		RelayLogPurge, ALLEGROSQLAutoIsNull, QueryCacheWlockInvalidate, ValidatePasswordCheckUserName,
		SuperReadOnly, BinlogOrderCommits, MasterVerifyChecksum, BinlogRowQueryLogEvents, LogSlowSlaveStatements,
		LogSlowAdminStatements, LogQueriesNotUsingIndexes, Profiling:
		if strings.EqualFold(value, "ON") {
			return "1", nil
		} else if strings.EqualFold(value, "OFF") {
			return "0", nil
		}
		val, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			if val == 0 {
				return "0", nil
			} else if val == 1 {
				return "1", nil
			}
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MyISAMUseMmap, InnodbBlockLocks, InnodbStatusOutput, InnodbAdaptiveFlushing, InnodbRandomReadAhead,
		InnodbStatsPersistent, InnodbBufferPoolLoadAbort, InnodbBufferPoolLoadNow, InnodbBufferPoolDumpNow,
		InnodbCmpPerIndexEnabled, InnodbFilePerBlock, InnodbPrintAllDeadlocks,
		InnodbStrictMode, InnodbAdaptiveHashIndex, InnodbFtEnableStopword, InnodbStatusOutputLocks:
		if strings.EqualFold(value, "ON") {
			return "1", nil
		} else if strings.EqualFold(value, "OFF") {
			return "0", nil
		}
		val, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			if val == 1 || val < 0 {
				return "1", nil
			} else if val == 0 {
				return "0", nil
			}
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MaxExecutionTime:
		return checkUInt64SystemVar(name, value, 0, math.MaxUint64, vars)
	case ThreadPoolSize:
		return checkUInt64SystemVar(name, value, 1, 64, vars)
	case MilevaDBEnableBlockPartition:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "on", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "off", nil
		case strings.EqualFold(value, "AUTO"):
			return "auto", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MilevaDBDBSReorgBatchSize:
		return checkUInt64SystemVar(name, value, uint64(MinDBSReorgBatchSize), uint64(MaxDBSReorgBatchSize), vars)
	case MilevaDBDBSErrorCountLimit:
		return checkUInt64SystemVar(name, value, uint64(0), math.MaxInt64, vars)
	case MilevaDBExpensiveQueryTimeThreshold:
		return checkUInt64SystemVar(name, value, MinExpensiveQueryTimeThreshold, math.MaxInt64, vars)
	case MilevaDBIndexLookupConcurrency,
		MilevaDBIndexLookupJoinConcurrency,
		MilevaDBHashJoinConcurrency,
		MilevaDBHashAggPartialConcurrency,
		MilevaDBHashAggFinalConcurrency,
		MilevaDBWindowConcurrency:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 && v != ConcurrencyUnset {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case MilevaDBExecutorConcurrency,
		MilevaDBDistALLEGROSQLScanConcurrency,
		MilevaDBIndexSerialScanConcurrency,
		MilevaDBIndexJoinBatchSize,
		MilevaDBIndexLookupSize,
		MilevaDBDBSReorgWorkerCount,
		MilevaDBBackoffLockFast, MilevaDBBackOffWeight,
		MilevaDBOptimizerSelectivityLevel:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case MilevaDBOptCorrelationExpFactor, MilevaDBDMLBatchSize:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case MilevaDBOptCorrelationThreshold:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 || v > 1 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case MilevaDBAllowBatchCop:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v == 0 && vars.AllowBCJ {
			return value, ErrWrongValueForVar.GenWithStackByArgs("Can't set batch cop 0 but milevadb_opt_broadcast_join is 1, please set milevadb_opt_broadcast_join 0 at first")
		}
		if v < 0 || v > 2 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case MilevaDBOptCPUFactor,
		MilevaDBOptTiFlashConcurrencyFactor,
		MilevaDBOptCopCPUFactor,
		MilevaDBOptNetworkFactor,
		MilevaDBOptScanFactor,
		MilevaDBOptDescScanFactor,
		MilevaDBOptSeekFactor,
		MilevaDBOptMemoryFactor,
		MilevaDBOptDiskFactor,
		MilevaDBOptConcurrencyFactor:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case MilevaDBProjectionConcurrency,
		MilevaDBMemQuotaQuery,
		MilevaDBMemQuotaHashJoin,
		MilevaDBMemQuotaMergeJoin,
		MilevaDBMemQuotaSort,
		MilevaDBMemQuotaTopn,
		MilevaDBMemQuotaIndexLookupReader,
		MilevaDBMemQuotaIndexLookupJoin,
		MilevaDBMemQuotaNestedLoopApply,
		MilevaDBRetryLimit,
		MilevaDBSlowLogThreshold,
		MilevaDBQueryLogMaxLen,
		MilevaDBEvolvePlanTaskMaxTime:
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name)
		}
		return value, nil
	case MilevaDBAutoAnalyzeStartTime, MilevaDBAutoAnalyzeEndTime, MilevaDBEvolvePlanTaskStartTime, MilevaDBEvolvePlanTaskEndTime:
		v, err := setDayTime(vars, value)
		if err != nil {
			return "", err
		}
		return v, nil
	case MilevaDBAutoAnalyzeRatio:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil || v < 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		return value, nil
	case TxnIsolation, TransactionIsolation:
		upVal := strings.ToUpper(value)
		_, exists := TxIsolationNames[upVal]
		if !exists {
			return "", ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		switch upVal {
		case "SERIALIZABLE", "READ-UNCOMMITTED":
			skipIsolationLevelCheck, err := GetStochastikSystemVar(vars, MilevaDBSkipIsolationLevelCheck)
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(value)
			if err != nil {
				returnErr = err
			}
			if !MilevaDBOptOn(skipIsolationLevelCheck) || err != nil {
				return "", returnErr
			}
			//SET TRANSACTION ISOLATION LEVEL will affect two internal variables:
			// 1. tx_isolation
			// 2. transaction_isolation
			// The following if condition is used to deduplicate two same warnings.
			if name == "transaction_isolation" {
				vars.StmtCtx.AppendWarning(returnErr)
			}
		}
		return upVal, nil
	case MilevaDBInitChunkSize:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		if v > initChunkSizeUpperBound {
			return value, errors.Errorf("milevadb_init_chunk_size(%d) cannot be bigger than %d", v, initChunkSizeUpperBound)
		}
		return value, nil
	case MilevaDBMaxChunkSize:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < maxChunkSizeLowerBound {
			return value, errors.Errorf("milevadb_max_chunk_size(%d) cannot be smaller than %d", v, maxChunkSizeLowerBound)
		}
		return value, nil
	case MilevaDBOptJoinReorderThreshold:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v < 0 || v >= 64 {
			return value, errors.Errorf("milevadb_join_order_algo_threshold(%d) cannot be smaller than 0 or larger than 63", v)
		}
	case MilevaDBWaitSplitRegionTimeout:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v <= 0 {
			return value, errors.Errorf("milevadb_wait_split_region_timeout(%d) cannot be smaller than 1", v)
		}
	case MilevaDBReplicaRead:
		if strings.EqualFold(value, "follower") {
			return "follower", nil
		} else if strings.EqualFold(value, "leader-and-follower") {
			return "leader-and-follower", nil
		} else if strings.EqualFold(value, "leader") || len(value) == 0 {
			return "leader", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MilevaDBTxnMode:
		switch strings.ToUpper(value) {
		case ast.Pessimistic, ast.Optimistic, "":
		default:
			return value, ErrWrongValueForVar.GenWithStackByArgs(MilevaDBTxnMode, value)
		}
	case MilevaDBRowFormatVersion:
		v, err := strconv.Atoi(value)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		if v != DefMilevaDBRowFormatV1 && v != DefMilevaDBRowFormatV2 {
			return value, errors.Errorf("Unsupported event format version %d", v)
		}
	case MilevaDBPartitionPruneMode:
		if !PartitionPruneMode(value).Valid() {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
	case MilevaDBAllowRemoveAutoInc, MilevaDBUsePlanBaselines, MilevaDBEvolvePlanBaselines, MilevaDBEnableParallelApply:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "on", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "off", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MilevaDBCapturePlanBaseline:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "on", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "off", nil
		case value == "":
			return "", nil
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MilevaDBEnableStmtSummary, MilevaDBStmtSummaryInternalQuery:
		switch {
		case strings.EqualFold(value, "ON") || value == "1":
			return "1", nil
		case strings.EqualFold(value, "OFF") || value == "0":
			return "0", nil
		case value == "":
			if scope == ScopeStochastik {
				return "", nil
			}
		}
		return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
	case MilevaDBStmtSummaryRefreshInterval:
		if value == "" && scope == ScopeStochastik {
			return "", nil
		}
		return checkInt64SystemVarWithError(name, value, 1, math.MaxInt32)
	case MilevaDBStmtSummaryHistorySize:
		if value == "" && scope == ScopeStochastik {
			return "", nil
		}
		return checkInt64SystemVarWithError(name, value, 0, math.MaxUint8)
	case MilevaDBStmtSummaryMaxStmtCount:
		if value == "" && scope == ScopeStochastik {
			return "", nil
		}
		return checkInt64SystemVarWithError(name, value, 1, math.MaxInt16)
	case MilevaDBStmtSummaryMaxALLEGROSQLLength:
		if value == "" && scope == ScopeStochastik {
			return "", nil
		}
		return checkInt64SystemVarWithError(name, value, 0, math.MaxInt32)
	case MilevaDBIsolationReadEngines:
		engines := strings.Split(value, ",")
		var formatVal string
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			if i != 0 {
				formatVal += ","
			}
			switch {
			case strings.EqualFold(engine, ekv.EinsteinDB.Name()):
				formatVal += ekv.EinsteinDB.Name()
			case strings.EqualFold(engine, ekv.TiFlash.Name()):
				formatVal += ekv.TiFlash.Name()
			case strings.EqualFold(engine, ekv.MilevaDB.Name()):
				formatVal += ekv.MilevaDB.Name()
			default:
				return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
			}
		}
		return formatVal, nil
	case MilevaDBMetricSchemaStep, MilevaDBMetricSchemaRangeDuration:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongValueForVar.GenWithStackByArgs(name, value)
		}
		if v < 10 || v > 60*60*60 {
			return value, errors.Errorf("%v(%d) cannot be smaller than %v or larger than %v", name, v, 10, 60*60*60)
		}
		return value, nil
	case DefCauslationConnection, DefCauslationDatabase, DefCauslationServer:
		if _, err := defCauslate.GetDefCauslationByName(value); err != nil {
			return value, errors.Trace(err)
		}
	case MilevaDBShardAllocateStep:
		return checkInt64SystemVar(name, value, 1, math.MaxInt64, vars)
	}
	return value, nil
}

// MilevaDBOptOn could be used for all milevadb stochastik variable options, we use "ON"/1 to turn on those options.
func MilevaDBOptOn(opt string) bool {
	return strings.EqualFold(opt, "ON") || opt == "1"
}

func milevadbOptPositiveInt32(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil || val <= 0 {
		return defaultVal
	}
	return val
}

func milevadbOptInt64(opt string, defaultVal int64) int64 {
	val, err := strconv.ParseInt(opt, 10, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

func milevadbOptFloat64(opt string, defaultVal float64) float64 {
	val, err := strconv.ParseFloat(opt, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

func parseTimeZone(s string) (*time.Location, error) {
	if strings.EqualFold(s, "SYSTEM") {
		return timeutil.SystemLocation(), nil
	}

	loc, err := time.LoadLocation(s)
	if err == nil {
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	// The time zone's value should in [-12:59,+14:00].
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, err := types.ParseDuration(nil, s[1:], 0)
		if err == nil {
			if s[0] == '-' {
				if d.Duration > 12*time.Hour+59*time.Minute {
					return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
				}
			} else {
				if d.Duration > 14*time.Hour {
					return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
				}
			}

			ofst := int(d.Duration / time.Second)
			if s[0] == '-' {
				ofst = -ofst
			}
			return time.FixedZone("", ofst), nil
		}
	}

	return nil, ErrUnknownTimeZone.GenWithStackByArgs(s)
}

func setSnapshotTS(s *StochastikVars, sVal string) error {
	if sVal == "" {
		s.SnapshotTS = 0
		return nil
	}

	if tso, err := strconv.ParseUint(sVal, 10, 64); err == nil {
		s.SnapshotTS = tso
		return nil
	}

	t, err := types.ParseTime(s.StmtCtx, sVal, allegrosql.TypeTimestamp, types.MaxFsp)
	if err != nil {
		return err
	}

	t1, err := t.GoTime(s.TimeZone)
	s.SnapshotTS = GoTimeToTS(t1)
	return err
}

// GoTimeToTS converts a Go time to uint64 timestamp.
func GoTimeToTS(t time.Time) uint64 {
	ts := (t.UnixNano() / int64(time.Millisecond)) << epochShiftBits
	return uint64(ts)
}

const (
	localDayTimeFormat = "15:04"
	// FullDayTimeFormat is the full format of analyze start time and end time.
	FullDayTimeFormat = "15:04 -0700"
)

func setDayTime(s *StochastikVars, val string) (string, error) {
	var t time.Time
	var err error
	if len(val) <= len(localDayTimeFormat) {
		t, err = time.ParseInLocation(localDayTimeFormat, val, s.TimeZone)
	} else {
		t, err = time.ParseInLocation(FullDayTimeFormat, val, s.TimeZone)
	}
	if err != nil {
		return "", err
	}
	return t.Format(FullDayTimeFormat), nil
}

// serverGlobalVariable is used to handle variables that acts in server and global scope.
type serverGlobalVariable struct {
	sync.Mutex
	serverVal string
	globalVal string
}

// Set sets the value according to variable scope.
func (v *serverGlobalVariable) Set(val string, isServer bool) {
	v.Lock()
	if isServer {
		v.serverVal = val
	} else {
		v.globalVal = val
	}
	v.Unlock()
}

// GetVal gets the value.
func (v *serverGlobalVariable) GetVal() string {
	v.Lock()
	defer v.Unlock()
	if v.serverVal != "" {
		return v.serverVal
	}
	return v.globalVal
}
