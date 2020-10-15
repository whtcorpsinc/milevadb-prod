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
	"math"
	"os"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/uber-go/atomic"
)

/*
	Steps to add a new MilevaDB specific system variable:

	1. Add a new variable name with comment in this file.
	2. Add the default value of the new variable in this file.
	3. Add SysVar instance in 'defaultSysVars' slice with the default value.
	4. Add a field in `StochastikVars`.
	5. UFIDelate the `NewStochastikVars` function to set the field to its default value.
	6. UFIDelate the `variable.SetStochastikSystemVar` function to use the new value when SET memex is executed.
	7. If it is a global variable, add it in `stochastik.loadCommonGlobalVarsALLEGROSQL`.
	8. UFIDelate ValidateSetSystemVar if the variable's value need to be validated.
	9. Use this variable to control the behavior in code.
*/

// MilevaDB system variable names that only in stochastik scope.
const (
	MilevaDBDBSSlowOprThreshold = "dbs_slow_threshold"

	// milevadb_snapshot is used for reading history data, the default value is empty string.
	// The value can be a datetime string like '2020-11-11 20:20:20' or a tso string. When this variable is set, the stochastik reads history data of that time.
	MilevaDBSnapshot = "milevadb_snapshot"

	// milevadb_opt_agg_push_down is used to enable/disable the optimizer rule of aggregation push down.
	MilevaDBOptAggPushDown = "milevadb_opt_agg_push_down"

	MilevaDBOptBCJ = "milevadb_opt_broadcast_join"
	// milevadb_opt_distinct_agg_push_down is used to decide whether agg with distinct should be pushed to einsteindb/tiflash.
	MilevaDBOptDistinctAggPushDown = "milevadb_opt_distinct_agg_push_down"

	// milevadb_opt_write_row_id is used to enable/disable the operations of insert、replace and uFIDelate to _milevadb_rowid.
	MilevaDBOptWriteRowID = "milevadb_opt_write_row_id"

	// Auto analyze will run if (causet modify count)/(causet event count) is greater than this value.
	MilevaDBAutoAnalyzeRatio = "milevadb_auto_analyze_ratio"

	// Auto analyze will run if current time is within start time and end time.
	MilevaDBAutoAnalyzeStartTime = "milevadb_auto_analyze_start_time"
	MilevaDBAutoAnalyzeEndTime   = "milevadb_auto_analyze_end_time"

	// milevadb_checksum_block_concurrency is used to speed up the ADMIN CHECKSUM TABLE
	// memex, when a causet has multiple indices, those indices can be
	// scanned concurrently, with the cost of higher system performance impact.
	MilevaDBChecksumBlockConcurrency = "milevadb_checksum_block_concurrency"

	// MilevaDBCurrentTS is used to get the current transaction timestamp.
	// It is read-only.
	MilevaDBCurrentTS = "milevadb_current_ts"

	// MilevaDBLastTxnInfo is used to get the last transaction info within the current stochastik.
	MilevaDBLastTxnInfo = "milevadb_last_txn_info"

	// milevadb_config is a read-only variable that shows the config of the current server.
	MilevaDBConfig = "milevadb_config"

	// milevadb_batch_insert is used to enable/disable auto-split insert data. If set this option on, insert interlock will automatically
	// insert data into multiple batches and use a single txn for each batch. This will be helpful when inserting large data.
	MilevaDBBatchInsert = "milevadb_batch_insert"

	// milevadb_batch_delete is used to enable/disable auto-split delete data. If set this option on, delete interlock will automatically
	// split data into multiple batches and use a single txn for each batch. This will be helpful when deleting large data.
	MilevaDBBatchDelete = "milevadb_batch_delete"

	// milevadb_batch_commit is used to enable/disable auto-split the transaction.
	// If set this option on, the transaction will be committed when it reaches stmt-count-limit and starts a new transaction.
	MilevaDBBatchCommit = "milevadb_batch_commit"

	// milevadb_dml_batch_size is used to split the insert/delete data into small batches.
	// It only takes effort when milevadb_batch_insert/milevadb_batch_delete is on.
	// Its default value is 20000. When the event size is large, 20k rows could be larger than 100MB.
	// User could change it to a smaller one to avoid breaking the transaction size limitation.
	MilevaDBDMLBatchSize = "milevadb_dml_batch_size"

	// The following stochastik variables controls the memory quota during query execution.
	// "milevadb_mem_quota_query":				control the memory quota of a query.
	MilevaDBMemQuotaQuery               = "milevadb_mem_quota_query" // Bytes.
	MilevaDBNestedLoopJoinCacheCapacity = "milevadb_nested_loop_join_cache_capacity"
	// TODO: remove them below sometime, it should have only one Quota(MilevaDBMemQuotaQuery).
	MilevaDBMemQuotaHashJoin          = "milevadb_mem_quota_hashjoin"          // Bytes.
	MilevaDBMemQuotaMergeJoin         = "milevadb_mem_quota_mergejoin"         // Bytes.
	MilevaDBMemQuotaSort              = "milevadb_mem_quota_sort"              // Bytes.
	MilevaDBMemQuotaTopn              = "milevadb_mem_quota_topn"              // Bytes.
	MilevaDBMemQuotaIndexLookupReader = "milevadb_mem_quota_indexlookupreader" // Bytes.
	MilevaDBMemQuotaIndexLookupJoin   = "milevadb_mem_quota_indexlookupjoin"   // Bytes.
	MilevaDBMemQuotaNestedLoopApply   = "milevadb_mem_quota_nestedloopapply"   // Bytes.

	// milevadb_general_log is used to log every query in the server in info level.
	MilevaDBGeneralLog = "milevadb_general_log"

	// milevadb_pprof_sql_cpu is used to add label allegrosql label to pprof result.
	MilevaDBPProfALLEGROSQLCPU = "milevadb_pprof_sql_cpu"

	// milevadb_retry_limit is the maximum number of retries when committing a transaction.
	MilevaDBRetryLimit = "milevadb_retry_limit"

	// milevadb_disable_txn_auto_retry disables transaction auto retry.
	MilevaDBDisableTxnAutoRetry = "milevadb_disable_txn_auto_retry"

	// milevadb_enable_streaming enables MilevaDB to use streaming API for interlock requests.
	MilevaDBEnableStreaming = "milevadb_enable_streaming"

	// milevadb_enable_chunk_rpc enables MilevaDB to use Chunk format for interlock requests.
	MilevaDBEnableChunkRPC = "milevadb_enable_chunk_rpc"

	// milevadb_optimizer_selectivity_level is used to control the selectivity estimation level.
	MilevaDBOptimizerSelectivityLevel = "milevadb_optimizer_selectivity_level"

	// milevadb_txn_mode is used to control the transaction behavior.
	MilevaDBTxnMode = "milevadb_txn_mode"

	// milevadb_row_format_version is used to control milevadb event format version current.
	MilevaDBRowFormatVersion = "milevadb_row_format_version"

	// milevadb_enable_block_partition is used to control causet partition feature.
	// The valid value include auto/on/off:
	// on or auto: enable causet partition if the partition type is implemented.
	// off: always disable causet partition.
	MilevaDBEnableBlockPartition = "milevadb_enable_block_partition"

	// milevadb_skip_isolation_level_check is used to control whether to return error when set unsupported transaction
	// isolation level.
	MilevaDBSkipIsolationLevelCheck = "milevadb_skip_isolation_level_check"

	// MilevaDBLowResolutionTSO is used for reading data with low resolution TSO which is uFIDelated once every two seconds
	MilevaDBLowResolutionTSO = "milevadb_low_resolution_tso"

	// MilevaDBReplicaRead is used for reading data from replicas, followers for example.
	MilevaDBReplicaRead = "milevadb_replica_read"

	// MilevaDBAllowRemoveAutoInc indicates whether a user can drop the auto_increment defCausumn attribute or not.
	MilevaDBAllowRemoveAutoInc = "milevadb_allow_remove_auto_inc"

	// MilevaDBEvolveCausetTaskMaxTime controls the max time of a single evolution task.
	MilevaDBEvolveCausetTaskMaxTime = "milevadb_evolve_plan_task_max_time"

	// MilevaDBEvolveCausetTaskStartTime is the start time of evolution task.
	MilevaDBEvolveCausetTaskStartTime = "milevadb_evolve_plan_task_start_time"
	// MilevaDBEvolveCausetTaskEndTime is the end time of evolution task.
	MilevaDBEvolveCausetTaskEndTime = "milevadb_evolve_plan_task_end_time"

	// milevadb_slow_log_threshold is used to set the slow log threshold in the server.
	MilevaDBSlowLogThreshold = "milevadb_slow_log_threshold"

	// milevadb_record_plan_in_slow_log is used to log the plan of the slow query.
	MilevaDBRecordCausetInSlowLog = "milevadb_record_plan_in_slow_log"

	// milevadb_enable_slow_log enables MilevaDB to log slow queries.
	MilevaDBEnableSlowLog = "milevadb_enable_slow_log"

	// milevadb_query_log_max_len is used to set the max length of the query in the log.
	MilevaDBQueryLogMaxLen = "milevadb_query_log_max_len"

	// MilevaDBCheckMb4ValueInUTF8 is used to control whether to enable the check wrong utf8 value.
	MilevaDBCheckMb4ValueInUTF8 = "milevadb_check_mb4_value_in_utf8"

	// MilevaDBFoundInCausetCache indicates whether the last memex was found in plan cache
	MilevaDBFoundInCausetCache = "last_plan_from_cache"

	// MilevaDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random defCausumn is allowed.
	MilevaDBAllowAutoRandExplicitInsert = "allow_auto_random_explicit_insert"
)

// MilevaDB system variable names that both in stochastik and global scope.
const (
	// milevadb_build_stats_concurrency is used to speed up the ANALYZE memex, when a causet has multiple indices,
	// those indices can be scanned concurrently, with the cost of higher system performance impact.
	MilevaDBBuildStatsConcurrency = "milevadb_build_stats_concurrency"

	// milevadb_allegrosql_scan_concurrency is used to set the concurrency of a allegrosql scan task.
	// A allegrosql scan task can be a causet scan or a index scan, which may be distributed to many EinsteinDB nodes.
	// Higher concurrency may reduce latency, but with the cost of higher memory usage and system performance impact.
	// If the query has a LIMIT clause, high concurrency makes the system do much more work than needed.
	// milevadb_allegrosql_scan_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBDistALLEGROSQLScanConcurrency = "milevadb_allegrosql_scan_concurrency"

	// milevadb_opt_insubquery_to_join_and_agg is used to enable/disable the optimizer rule of rewriting IN subquery.
	MilevaDBOptInSubqToJoinAnPosetDagg = "milevadb_opt_insubq_to_join_and_agg"

	// milevadb_opt_correlation_threshold is a guard to enable event count estimation using defCausumn order correlation.
	MilevaDBOptCorrelationThreshold = "milevadb_opt_correlation_threshold"

	// milevadb_opt_correlation_exp_factor is an exponential factor to control heuristic approach when milevadb_opt_correlation_threshold is not satisfied.
	MilevaDBOptCorrelationExpFactor = "milevadb_opt_correlation_exp_factor"

	// milevadb_opt_cpu_factor is the CPU cost of processing one memex for one event.
	MilevaDBOptCPUFactor = "milevadb_opt_cpu_factor"
	// milevadb_opt_copcpu_factor is the CPU cost of processing one memex for one event in interlock.
	MilevaDBOptCopCPUFactor = "milevadb_opt_copcpu_factor"
	// milevadb_opt_tiflash_concurrency_factor is concurrency number of tiflash computation.
	MilevaDBOptTiFlashConcurrencyFactor = "milevadb_opt_tiflash_concurrency_factor"
	// milevadb_opt_network_factor is the network cost of transferring 1 byte data.
	MilevaDBOptNetworkFactor = "milevadb_opt_network_factor"
	// milevadb_opt_scan_factor is the IO cost of scanning 1 byte data on EinsteinDB.
	MilevaDBOptScanFactor = "milevadb_opt_scan_factor"
	// milevadb_opt_desc_factor is the IO cost of scanning 1 byte data on EinsteinDB in desc order.
	MilevaDBOptDescScanFactor = "milevadb_opt_desc_factor"
	// milevadb_opt_seek_factor is the IO cost of seeking the start value in a range on EinsteinDB or TiFlash.
	MilevaDBOptSeekFactor = "milevadb_opt_seek_factor"
	// milevadb_opt_memory_factor is the memory cost of storing one tuple.
	MilevaDBOptMemoryFactor = "milevadb_opt_memory_factor"
	// milevadb_opt_disk_factor is the IO cost of reading/writing one byte to temporary disk.
	MilevaDBOptDiskFactor = "milevadb_opt_disk_factor"
	// milevadb_opt_concurrency_factor is the CPU cost of additional one goroutine.
	MilevaDBOptConcurrencyFactor = "milevadb_opt_concurrency_factor"

	// milevadb_index_join_batch_size is used to set the batch size of a index lookup join.
	// The index lookup join fetches batches of data from outer interlock and constructs ranges for inner interlock.
	// This value controls how much of data in a batch to do the index join.
	// Large value may reduce the latency but consumes more system resource.
	MilevaDBIndexJoinBatchSize = "milevadb_index_join_batch_size"

	// milevadb_index_lookup_size is used for index lookup interlock.
	// The index lookup interlock first scan a batch of handles from a index, then use those handles to lookup the causet
	// rows, this value controls how much of handles in a batch to do a lookup task.
	// Small value sends more RPCs to EinsteinDB, consume more system resource.
	// Large value may do more work than needed if the query has a limit.
	MilevaDBIndexLookupSize = "milevadb_index_lookup_size"

	// milevadb_index_lookup_concurrency is used for index lookup interlock.
	// A lookup task may have 'milevadb_index_lookup_size' of handles at maximun, the handles may be distributed
	// in many EinsteinDB nodes, we executes multiple concurrent index lookup tasks concurrently to reduce the time
	// waiting for a task to finish.
	// Set this value higher may reduce the latency but consumes more system resource.
	// milevadb_index_lookup_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBIndexLookupConcurrency = "milevadb_index_lookup_concurrency"

	// milevadb_index_lookup_join_concurrency is used for index lookup join interlock.
	// IndexLookUpJoin starts "milevadb_index_lookup_join_concurrency" inner workers
	// to fetch inner rows and join the matched (outer, inner) event pairs.
	// milevadb_index_lookup_join_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBIndexLookupJoinConcurrency = "milevadb_index_lookup_join_concurrency"

	// milevadb_index_serial_scan_concurrency is used for controlling the concurrency of index scan operation
	// when we need to keep the data output order the same as the order of index data.
	MilevaDBIndexSerialScanConcurrency = "milevadb_index_serial_scan_concurrency"

	// MilevaDBMaxChunkSize is used to control the max chunk size during query execution.
	MilevaDBMaxChunkSize = "milevadb_max_chunk_size"

	// MilevaDBAllowBatchCop means if we should send batch interlock to TiFlash. It can be set to 0, 1 and 2.
	// 0 means never use batch cop, 1 means use batch cop in case of aggregation and join, 2, means to force to send batch cop for any query.
	// The default value is 0
	MilevaDBAllowBatchCop = "milevadb_allow_batch_cop"

	// MilevaDBInitChunkSize is used to control the init chunk size during query execution.
	MilevaDBInitChunkSize = "milevadb_init_chunk_size"

	// milevadb_enable_cascades_causet is used to control whether to enable the cascades causet.
	MilevaDBEnableCascadesCausetAppend = "milevadb_enable_cascades_causet"

	// milevadb_skip_utf8_check skips the UTF8 validate process, validate UTF8 has performance cost, if we can make sure
	// the input string values are valid, we can skip the check.
	MilevaDBSkipUTF8Check = "milevadb_skip_utf8_check"

	// milevadb_skip_ascii_check skips the ASCII validate process
	// old milevadb may already have fields with invalid ASCII bytes
	// disable ASCII validate can guarantee a safe replication
	MilevaDBSkipASCIICheck = "milevadb_skip_ascii_check"

	// milevadb_hash_join_concurrency is used for hash join interlock.
	// The hash join outer interlock starts multiple concurrent join workers to probe the hash causet.
	// milevadb_hash_join_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBHashJoinConcurrency = "milevadb_hash_join_concurrency"

	// milevadb_projection_concurrency is used for projection operator.
	// This variable controls the worker number of projection operator.
	// milevadb_projection_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBProjectionConcurrency = "milevadb_projection_concurrency"

	// milevadb_hashagg_partial_concurrency is used for hash agg interlock.
	// The hash agg interlock starts multiple concurrent partial workers to do partial aggregate works.
	// milevadb_hashagg_partial_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBHashAggPartialConcurrency = "milevadb_hashagg_partial_concurrency"

	// milevadb_hashagg_final_concurrency is used for hash agg interlock.
	// The hash agg interlock starts multiple concurrent final workers to do final aggregate works.
	// milevadb_hashagg_final_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBHashAggFinalConcurrency = "milevadb_hashagg_final_concurrency"

	// milevadb_window_concurrency is used for window parallel interlock.
	// milevadb_window_concurrency is deprecated, use milevadb_interlock_concurrency instead.
	MilevaDBWindowConcurrency = "milevadb_window_concurrency"

	// milevadb_enable_parallel_apply is used for parallel apply.
	MilevaDBEnableParallelApply = "milevadb_enable_parallel_apply"

	// milevadb_backoff_lock_fast is used for einsteindb backoff base time in milliseconds.
	MilevaDBBackoffLockFast = "milevadb_backoff_lock_fast"

	// milevadb_backoff_weight is used to control the max back off time in MilevaDB.
	// The default maximum back off time is a small value.
	// BackOffWeight could multiply it to let the user adjust the maximum time for retrying.
	// Only positive integers can be accepted, which means that the maximum back off time can only grow.
	MilevaDBBackOffWeight = "milevadb_backoff_weight"

	// milevadb_dbs_reorg_worker_cnt defines the count of dbs reorg workers.
	MilevaDBDBSReorgWorkerCount = "milevadb_dbs_reorg_worker_cnt"

	// milevadb_dbs_reorg_batch_size defines the transaction batch size of dbs reorg workers.
	MilevaDBDBSReorgBatchSize = "milevadb_dbs_reorg_batch_size"

	// milevadb_dbs_error_count_limit defines the count of dbs error limit.
	MilevaDBDBSErrorCountLimit = "milevadb_dbs_error_count_limit"

	// milevadb_dbs_reorg_priority defines the operations priority of adding indices.
	// It can be: PRIORITY_LOW, PRIORITY_NORMAL, PRIORITY_HIGH
	MilevaDBDBSReorgPriority = "milevadb_dbs_reorg_priority"

	// MilevaDBEnableChangeDeferredCausetType is used to control whether to enable the change defCausumn type.
	MilevaDBEnableChangeDeferredCausetType = "milevadb_enable_change_defCausumn_type"

	// milevadb_max_delta_schema_count defines the max length of deltaSchemaInfos.
	// deltaSchemaInfos is a queue that maintains the history of schemaReplicant changes.
	MilevaDBMaxDeltaSchemaCount = "milevadb_max_delta_schema_count"

	// milevadb_scatter_region will scatter the regions for DBSs when it is ON.
	MilevaDBScatterRegion = "milevadb_scatter_region"

	// MilevaDBWaitSplitRegionFinish defines the split region behaviour is sync or async.
	MilevaDBWaitSplitRegionFinish = "milevadb_wait_split_region_finish"

	// MilevaDBWaitSplitRegionTimeout uses to set the split and scatter region back off time.
	MilevaDBWaitSplitRegionTimeout = "milevadb_wait_split_region_timeout"

	// milevadb_force_priority defines the operations priority of all memexs.
	// It can be "NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"
	MilevaDBForcePriority = "milevadb_force_priority"

	// milevadb_enable_radix_join indicates to use radix hash join algorithm to execute
	// HashJoin.
	MilevaDBEnableRadixJoin = "milevadb_enable_radix_join"

	// milevadb_constraint_check_in_place indicates to check the constraint when the ALLEGROALLEGROSQL executing.
	// It could hurt the performance of bulking insert when it is ON.
	MilevaDBConstraintCheckInPlace = "milevadb_constraint_check_in_place"

	// milevadb_enable_window_function is used to control whether to enable the window function.
	MilevaDBEnableWindowFunction = "milevadb_enable_window_function"

	// milevadb_enable_vectorized_memex is used to control whether to enable the vectorized memex evaluation.
	MilevaDBEnableVectorizedExpression = "milevadb_enable_vectorized_memex"

	// MilevaDBOptJoinReorderThreshold defines the threshold less than which
	// we'll choose a rather time consuming algorithm to calculate the join order.
	MilevaDBOptJoinReorderThreshold = "milevadb_opt_join_reorder_threshold"

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY causet to parse.
	MilevaDBSlowQueryFile = "milevadb_slow_query_file"

	// MilevaDBEnableFastAnalyze indicates to use fast analyze.
	MilevaDBEnableFastAnalyze = "milevadb_enable_fast_analyze"

	// MilevaDBExpensiveQueryTimeThreshold indicates the time threshold of expensive query.
	MilevaDBExpensiveQueryTimeThreshold = "milevadb_expensive_query_time_threshold"

	// MilevaDBEnableIndexMerge indicates to generate IndexMergePath.
	MilevaDBEnableIndexMerge = "milevadb_enable_index_merge"

	// MilevaDBEnableNoopFuncs set true will enable using fake funcs(like get_lock release_lock)
	MilevaDBEnableNoopFuncs = "milevadb_enable_noop_functions"

	// MilevaDBEnableStmtSummary indicates whether the memex summary is enabled.
	MilevaDBEnableStmtSummary = "milevadb_enable_stmt_summary"

	// MilevaDBStmtSummaryInternalQuery indicates whether the memex summary contain internal query.
	MilevaDBStmtSummaryInternalQuery = "milevadb_stmt_summary_internal_query"

	// MilevaDBStmtSummaryRefreshInterval indicates the refresh interval in seconds for each memex summary.
	MilevaDBStmtSummaryRefreshInterval = "milevadb_stmt_summary_refresh_interval"

	// MilevaDBStmtSummaryHistorySize indicates the history size of each memex summary.
	MilevaDBStmtSummaryHistorySize = "milevadb_stmt_summary_history_size"

	// MilevaDBStmtSummaryMaxStmtCount indicates the max number of memexs kept in memory.
	MilevaDBStmtSummaryMaxStmtCount = "milevadb_stmt_summary_max_stmt_count"

	// MilevaDBStmtSummaryMaxALLEGROSQLLength indicates the max length of displayed normalized allegrosql and sample allegrosql.
	MilevaDBStmtSummaryMaxALLEGROSQLLength = "milevadb_stmt_summary_max_sql_length"

	// MilevaDBCaptureCausetBaseline indicates whether the capture of plan baselines is enabled.
	MilevaDBCaptureCausetBaseline = "milevadb_capture_plan_baselines"

	// MilevaDBUseCausetBaselines indicates whether the use of plan baselines is enabled.
	MilevaDBUseCausetBaselines = "milevadb_use_plan_baselines"

	// MilevaDBEvolveCausetBaselines indicates whether the evolution of plan baselines is enabled.
	MilevaDBEvolveCausetBaselines = "milevadb_evolve_plan_baselines"

	// MilevaDBIsolationReadEngines indicates the milevadb only read from the stores whose engine type is involved in IsolationReadEngines.
	// Now, only support EinsteinDB and TiFlash.
	MilevaDBIsolationReadEngines = "milevadb_isolation_read_engines"

	// MilevaDBStoreLimit indicates the limit of sending request to a causetstore, 0 means without limit.
	MilevaDBStoreLimit = "milevadb_store_limit"

	// MilevaDBMetricSchemaStep indicates the step when query metric schemaReplicant.
	MilevaDBMetricSchemaStep = "milevadb_metric_query_step"

	// MilevaDBMetricSchemaRangeDuration indicates the range duration when query metric schemaReplicant.
	MilevaDBMetricSchemaRangeDuration = "milevadb_metric_query_range_duration"

	// MilevaDBEnableDefCauslectInterDircutionInfo indicates that whether execution info is defCauslected.
	MilevaDBEnableDefCauslectInterDircutionInfo = "milevadb_enable_defCauslect_execution_info"

	// DefInterlockingDirectorateConcurrency is used for controlling the concurrency of all types of interlocks.
	MilevaDBInterlockingDirectorateConcurrency = "milevadb_interlock_concurrency"

	// MilevaDBEnableClusteredIndex indicates if clustered index feature is enabled.
	MilevaDBEnableClusteredIndex = "milevadb_enable_clustered_index"

	// MilevaDBPartitionPruneMode indicates the partition prune mode used.
	MilevaDBPartitionPruneMode = "milevadb_partition_prune_mode"

	// MilevaDBSlowLogMasking indicates that whether masking the query data when log slow query.
	// Deprecated: use MilevaDBRedactLog instead.
	MilevaDBSlowLogMasking = "milevadb_slow_log_masking"

	// MilevaDBRedactLog indicates that whether redact log.
	MilevaDBRedactLog = "milevadb_redact_log"

	// MilevaDBShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
	MilevaDBShardAllocateStep = "milevadb_shard_allocate_step"
	// MilevaDBEnableTelemetry indicates that whether usage data report to WHTCORPS INC is enabled.
	MilevaDBEnableTelemetry = "milevadb_enable_telemetry"

	// MilevaDBEnableAmendPessimisticTxn indicates if amend pessimistic transactions is enabled.
	MilevaDBEnableAmendPessimisticTxn = "milevadb_enable_amend_pessimistic_txn"
)

// Default MilevaDB system variable values.
const (
	DefHostname                        = "localhost"
	DefIndexLookupConcurrency          = ConcurrencyUnset
	DefIndexLookupJoinConcurrency      = ConcurrencyUnset
	DefIndexSerialScanConcurrency      = 1
	DefIndexJoinBatchSize              = 25000
	DefIndexLookupSize                 = 20000
	DefDistALLEGROSQLScanConcurrency          = 15
	DefBuildStatsConcurrency           = 4
	DefAutoAnalyzeRatio                = 0.5
	DefAutoAnalyzeStartTime            = "00:00 +0000"
	DefAutoAnalyzeEndTime              = "23:59 +0000"
	DefAutoIncrementIncrement          = 1
	DefAutoIncrementOffset             = 1
	DefChecksumBlockConcurrency        = 4
	DefSkipUTF8Check                   = false
	DefSkipASCIICheck                  = false
	DefOptAggPushDown                  = false
	DefOptBCJ                          = false
	DefOptWriteRowID                   = false
	DefOptCorrelationThreshold         = 0.9
	DefOptCorrelationExpFactor         = 1
	DefOptCPUFactor                    = 3.0
	DefOptCopCPUFactor                 = 3.0
	DefOptTiFlashConcurrencyFactor     = 24.0
	DefOptNetworkFactor                = 1.0
	DefOptScanFactor                   = 1.5
	DefOptDescScanFactor               = 3.0
	DefOptSeekFactor                   = 20.0
	DefOptMemoryFactor                 = 0.001
	DefOptDiskFactor                   = 1.5
	DefOptConcurrencyFactor            = 3.0
	DefOptInSubqToJoinAnPosetDagg           = true
	DefBatchInsert                     = false
	DefBatchDelete                     = false
	DefBatchCommit                     = false
	DefCurretTS                        = 0
	DefInitChunkSize                   = 32
	DefMaxChunkSize                    = 1024
	DefDMLBatchSize                    = 0
	DefMaxPreparedStmtCount            = -1
	DefWaitTimeout                     = 0
	DefMilevaDBMemQuotaHashJoin            = 32 << 30 // 32GB.
	DefMilevaDBMemQuotaMergeJoin           = 32 << 30 // 32GB.
	DefMilevaDBMemQuotaSort                = 32 << 30 // 32GB.
	DefMilevaDBMemQuotaTopn                = 32 << 30 // 32GB.
	DefMilevaDBMemQuotaIndexLookupReader   = 32 << 30 // 32GB.
	DefMilevaDBMemQuotaIndexLookupJoin     = 32 << 30 // 32GB.
	DefMilevaDBMemQuotaNestedLoopApply     = 32 << 30 // 32GB.
	DefMilevaDBMemQuotaDistALLEGROSQL             = 32 << 30 // 32GB.
	DefMilevaDBGeneralLog                  = 0
	DefMilevaDBPProfALLEGROSQLCPU                 = 0
	DefMilevaDBRetryLimit                  = 10
	DefMilevaDBDisableTxnAutoRetry         = true
	DefMilevaDBConstraintCheckInPlace      = false
	DefMilevaDBHashJoinConcurrency         = ConcurrencyUnset
	DefMilevaDBProjectionConcurrency       = ConcurrencyUnset
	DefMilevaDBOptimizerSelectivityLevel   = 0
	DefMilevaDBAllowBatchCop               = 1
	DefMilevaDBTxnMode                     = ""
	DefMilevaDBRowFormatV1                 = 1
	DefMilevaDBRowFormatV2                 = 2
	DefMilevaDBDBSReorgWorkerCount         = 4
	DefMilevaDBDBSReorgBatchSize           = 256
	DefMilevaDBDBSErrorCountLimit          = 512
	DefMilevaDBMaxDeltaSchemaCount         = 1024
	DefMilevaDBChangeDeferredCausetType            = false
	DefMilevaDBHashAggPartialConcurrency   = ConcurrencyUnset
	DefMilevaDBHashAggFinalConcurrency     = ConcurrencyUnset
	DefMilevaDBWindowConcurrency           = ConcurrencyUnset
	DefMilevaDBForcePriority               = allegrosql.NoPriority
	DefMilevaDBUseRadixJoin                = false
	DefEnableWindowFunction            = true
	DefEnableVectorizedExpression      = true
	DefMilevaDBOptJoinReorderThreshold     = 0
	DefMilevaDBDBSSlowOprThreshold         = 300
	DefMilevaDBUseFastAnalyze              = false
	DefMilevaDBSkipIsolationLevelCheck     = false
	DefMilevaDBExpensiveQueryTimeThreshold = 60 // 60s
	DefMilevaDBScatterRegion               = false
	DefMilevaDBWaitSplitRegionFinish       = true
	DefWaitSplitRegionTimeout          = 300 // 300s
	DefMilevaDBEnableNoopFuncs             = false
	DefMilevaDBAllowRemoveAutoInc          = false
	DefMilevaDBUseCausetBaselines            = true
	DefMilevaDBEvolveCausetBaselines         = false
	DefMilevaDBEvolveCausetTaskMaxTime       = 600 // 600s
	DefMilevaDBEvolveCausetTaskStartTime     = "00:00 +0000"
	DefMilevaDBEvolveCausetTaskEndTime       = "23:59 +0000"
	DefInnodbLockWaitTimeout           = 50 // 50s
	DefMilevaDBStoreLimit                  = 0
	DefMilevaDBMetricSchemaStep            = 60 // 60s
	DefMilevaDBMetricSchemaRangeDuration   = 60 // 60s
	DefMilevaDBFoundInCausetCache            = false
	DefMilevaDBEnableDefCauslectInterDircutionInfo  = true
	DefMilevaDBAllowAutoRandExplicitInsert = false
	DefMilevaDBEnableClusteredIndex        = false
	DefMilevaDBSlowLogMasking              = false
	DefMilevaDBShardAllocateStep           = math.MaxInt64
	DefMilevaDBEnableTelemetry             = true
	DefMilevaDBEnableParallelApply         = false
	DefMilevaDBEnableAmendPessimisticTxn   = true
)

// Process global variables.
var (
	ProcessGeneralLog      uint32
	EnablePProfALLEGROSQLCPU            = atomic.NewBool(false)
	dbsReorgWorkerCounter  int32 = DefMilevaDBDBSReorgWorkerCount
	maxDBSReorgWorkerCount int32 = 128
	dbsReorgBatchSize      int32 = DefMilevaDBDBSReorgBatchSize
	dbsErrorCountlimit     int64 = DefMilevaDBDBSErrorCountLimit
	maxDeltaSchemaCount    int64 = DefMilevaDBMaxDeltaSchemaCount
	// Export for testing.
	MaxDBSReorgBatchSize int32 = 10240
	MinDBSReorgBatchSize int32 = 32
	// DBSSlowOprThreshold is the threshold for dbs slow operations, uint is millisecond.
	DBSSlowOprThreshold            uint32 = DefMilevaDBDBSSlowOprThreshold
	ForcePriority                         = int32(DefMilevaDBForcePriority)
	ServerHostname, _                     = os.Hostname()
	MaxOfMaxAllowedPacket          uint64 = 1073741824
	ExpensiveQueryTimeThreshold    uint64 = DefMilevaDBExpensiveQueryTimeThreshold
	MinExpensiveQueryTimeThreshold uint64 = 10 //10s
	CaptureCausetBaseline                   = serverGlobalVariable{globalVal: "0"}
	DefInterlockingDirectorateConcurrency                = 5
)
