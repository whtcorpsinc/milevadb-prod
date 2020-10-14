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
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/versioninfo"
)

// ScopeFlag is for system variable whether can be changed in global/stochastik dynamically or not.
type ScopeFlag uint8

const (
	// ScopeNone means the system variable can not be changed dynamically.
	ScopeNone ScopeFlag = 0
	// ScopeGlobal means the system variable can be changed globally.
	ScopeGlobal ScopeFlag = 1 << 0
	// ScopeStochastik means the system variable can only be changed in current stochastik.
	ScopeStochastik ScopeFlag = 1 << 1
)

// SysVar is for system variable.
type SysVar struct {
	// Scope is for whether can be changed or not
	Scope ScopeFlag

	// Name is the variable name.
	Name string

	// Value is the variable value.
	Value string
}

// SysVars is global sys vars map.
var SysVars map[string]*SysVar

// GetSysVar returns sys var info for name as key.
func GetSysVar(name string) *SysVar {
	name = strings.ToLower(name)
	return SysVars[name]
}

// PluginVarNames is global plugin var names set.
var PluginVarNames []string

func init() {
	SysVars = make(map[string]*SysVar)
	for _, v := range defaultSysVars {
		SysVars[v.Name] = v
	}
	initSynonymsSysVariables()
}

// BoolToIntStr converts bool to int string, for example "0" or "1".
func BoolToIntStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func boolToOnOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// BoolToInt32 converts bool to int32
func BoolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// we only support MyALLEGROSQL now
var defaultSysVars = []*SysVar{
	{ScopeGlobal, "gtid_mode", "OFF"},
	{ScopeGlobal, FlushTime, "0"},
	{ScopeNone, "performance_schema_max_mutex_classes", "200"},
	{ScopeGlobal | ScopeStochastik, LowPriorityUFIDelates, "0"},
	{ScopeGlobal | ScopeStochastik, StochastikTrackGtids, "OFF"},
	{ScopeGlobal | ScopeStochastik, "ndbinfo_max_rows", ""},
	{ScopeGlobal | ScopeStochastik, "ndb_index_stat_option", ""},
	{ScopeGlobal | ScopeStochastik, OldPasswords, "0"},
	{ScopeNone, "innodb_version", "5.6.25"},
	{ScopeGlobal, MaxConnections, "151"},
	{ScopeGlobal | ScopeStochastik, BigBlocks, "0"},
	{ScopeNone, "skip_external_locking", "1"},
	{ScopeNone, "innodb_sync_array_size", "1"},
	{ScopeStochastik, "rand_seed2", ""},
	{ScopeGlobal, ValidatePasswordCheckUserName, "0"},
	{ScopeGlobal, "validate_password_number_count", "1"},
	{ScopeStochastik, "gtid_next", ""},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLSelectLimit, "18446744073709551615"},
	{ScopeGlobal, "ndb_show_foreign_key_mock_blocks", ""},
	{ScopeNone, "multi_range_count", "256"},
	{ScopeGlobal | ScopeStochastik, DefaultWeekFormat, "0"},
	{ScopeGlobal | ScopeStochastik, "binlog_error_action", "IGNORE_ERROR"},
	{ScopeGlobal | ScopeStochastik, "default_storage_engine", "InnoDB"},
	{ScopeNone, "ft_query_expansion_limit", "20"},
	{ScopeGlobal, MaxConnectErrors, "100"},
	{ScopeGlobal, SyncBinlog, "0"},
	{ScopeNone, "max_digest_length", "1024"},
	{ScopeNone, "innodb_force_load_corrupted", "0"},
	{ScopeNone, "performance_schema_max_block_handles", "4000"},
	{ScopeGlobal, InnodbFastShutdown, "1"},
	{ScopeNone, "ft_max_word_len", "84"},
	{ScopeGlobal, "log_backward_compatible_user_definitions", ""},
	{ScopeNone, "lc_messages_dir", "/usr/local/allegrosql-5.6.25-osx10.8-x86_64/share/"},
	{ScopeGlobal, "ft_boolean_syntax", "+ -><()~*:\"\"&|"},
	{ScopeGlobal, BlockDefinitionCache, "-1"},
	{ScopeNone, SkipNameResolve, "0"},
	{ScopeNone, "performance_schema_max_file_handles", "32768"},
	{ScopeStochastik, "transaction_allow_batching", ""},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLModeVar, allegrosql.DefaultALLEGROSQLMode},
	{ScopeNone, "performance_schema_max_statement_classes", "168"},
	{ScopeGlobal, "server_id", "0"},
	{ScopeGlobal, "innodb_flushing_avg_loops", "30"},
	{ScopeGlobal | ScopeStochastik, TmpBlockSize, "16777216"},
	{ScopeGlobal, "innodb_max_purge_lag", "0"},
	{ScopeGlobal | ScopeStochastik, "preload_buffer_size", "32768"},
	{ScopeGlobal, CheckProxyUsers, "0"},
	{ScopeNone, "have_query_cache", "YES"},
	{ScopeGlobal, "innodb_flush_log_at_timeout", "1"},
	{ScopeGlobal, "innodb_max_undo_log_size", ""},
	{ScopeGlobal | ScopeStochastik, "range_alloc_block_size", "4096"},
	{ScopeGlobal, ConnectTimeout, "10"},
	{ScopeGlobal | ScopeStochastik, MaxExecutionTime, "0"},
	{ScopeGlobal | ScopeStochastik, DefCauslationServer, allegrosql.DefaultDefCauslationName},
	{ScopeNone, "have_rtree_keys", "YES"},
	{ScopeGlobal, "innodb_old_blocks_pct", "37"},
	{ScopeGlobal, "innodb_file_format", "Antelope"},
	{ScopeGlobal, "innodb_compression_failure_threshold_pct", "5"},
	{ScopeNone, "performance_schema_events_waits_history_long_size", "10000"},
	{ScopeGlobal, "innodb_checksum_algorithm", "innodb"},
	{ScopeNone, "innodb_ft_sort_pll_degree", "2"},
	{ScopeNone, "thread_stack", "262144"},
	{ScopeGlobal, "relay_log_info_repository", "FILE"},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLLogBin, "1"},
	{ScopeGlobal, SuperReadOnly, "0"},
	{ScopeGlobal | ScopeStochastik, "max_delayed_threads", "20"},
	{ScopeNone, "protodefCaus_version", "10"},
	{ScopeGlobal | ScopeStochastik, "new", "OFF"},
	{ScopeGlobal | ScopeStochastik, "myisam_sort_buffer_size", "8388608"},
	{ScopeGlobal | ScopeStochastik, "optimizer_trace_offset", "-1"},
	{ScopeGlobal, InnodbBufferPoolDumpAtShutdown, "0"},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLNotes, "1"},
	{ScopeGlobal, InnodbCmpPerIndexEnabled, "0"},
	{ScopeGlobal, "innodb_ft_server_stopword_block", ""},
	{ScopeNone, "performance_schema_max_file_instances", "7693"},
	{ScopeNone, "log_output", "FILE"},
	{ScopeGlobal, "binlog_group_commit_sync_delay", ""},
	{ScopeGlobal, "binlog_group_commit_sync_no_delay_count", ""},
	{ScopeNone, "have_crypt", "YES"},
	{ScopeGlobal, "innodb_log_write_ahead_size", ""},
	{ScopeNone, "innodb_log_group_home_dir", "./"},
	{ScopeNone, "performance_schema_events_statements_history_size", "10"},
	{ScopeGlobal, GeneralLog, "0"},
	{ScopeGlobal, "validate_password_dictionary_file", ""},
	{ScopeGlobal, BinlogOrderCommits, "1"},
	{ScopeGlobal, "key_cache_division_limit", "100"},
	{ScopeGlobal | ScopeStochastik, "max_insert_delayed_threads", "20"},
	{ScopeNone, "performance_schema_stochastik_connect_attrs_size", "512"},
	{ScopeGlobal | ScopeStochastik, "time_zone", "SYSTEM"},
	{ScopeGlobal, "innodb_max_dirty_pages_pct", "75"},
	{ScopeGlobal, InnodbFilePerBlock, "1"},
	{ScopeGlobal, InnodbLogCompressedPages, "1"},
	{ScopeNone, "skip_networking", "0"},
	{ScopeGlobal, "innodb_monitor_reset", ""},
	{ScopeNone, "have_ssl", "DISABLED"},
	{ScopeNone, "have_openssl", "DISABLED"},
	{ScopeNone, "ssl_ca", ""},
	{ScopeNone, "ssl_cert", ""},
	{ScopeNone, "ssl_key", ""},
	{ScopeNone, "ssl_cipher", ""},
	{ScopeNone, "tls_version", "TLSv1,TLSv1.1,TLSv1.2"},
	{ScopeNone, "system_time_zone", "CST"},
	{ScopeGlobal, InnodbPrintAllDeadlocks, "0"},
	{ScopeNone, "innodb_autoinc_lock_mode", "1"},
	{ScopeGlobal, "key_buffer_size", "8388608"},
	{ScopeGlobal | ScopeStochastik, ForeignKeyChecks, "OFF"},
	{ScopeGlobal, "host_cache_size", "279"},
	{ScopeGlobal, DelayKeyWrite, "ON"},
	{ScopeNone, "metadata_locks_cache_size", "1024"},
	{ScopeNone, "innodb_force_recovery", "0"},
	{ScopeGlobal, "innodb_file_format_max", "Antelope"},
	{ScopeGlobal | ScopeStochastik, "debug", ""},
	{ScopeGlobal, "log_warnings", "1"},
	{ScopeGlobal, OfflineMode, "0"},
	{ScopeGlobal | ScopeStochastik, InnodbStrictMode, "1"},
	{ScopeGlobal, "innodb_rollback_segments", "128"},
	{ScopeGlobal | ScopeStochastik, "join_buffer_size", "262144"},
	{ScopeNone, "innodb_mirrored_log_groups", "1"},
	{ScopeGlobal, "max_binlog_size", "1073741824"},
	{ScopeGlobal, "concurrent_insert", "AUTO"},
	{ScopeGlobal, InnodbAdaptiveHashIndex, "1"},
	{ScopeGlobal, InnodbFtEnableStopword, "1"},
	{ScopeGlobal, "general_log_file", "/usr/local/allegrosql/data/localhost.log"},
	{ScopeGlobal | ScopeStochastik, InnodbSupportXA, "1"},
	{ScopeGlobal, "innodb_compression_level", "6"},
	{ScopeNone, "innodb_file_format_check", "1"},
	{ScopeNone, "myisam_mmap_size", "18446744073709551615"},
	{ScopeNone, "innodb_buffer_pool_instances", "8"},
	{ScopeGlobal | ScopeStochastik, BlockEncryptionMode, "aes-128-ecb"},
	{ScopeGlobal | ScopeStochastik, "max_length_for_sort_data", "1024"},
	{ScopeNone, "character_set_system", "utf8"},
	{ScopeGlobal | ScopeStochastik, InteractiveTimeout, "28800"},
	{ScopeGlobal, InnodbOptimizeFullTextOnly, "0"},
	{ScopeNone, "character_sets_dir", "/usr/local/allegrosql-5.6.25-osx10.8-x86_64/share/charsets/"},
	{ScopeGlobal | ScopeStochastik, QueryCacheType, "OFF"},
	{ScopeNone, "innodb_rollback_on_timeout", "0"},
	{ScopeGlobal | ScopeStochastik, "query_alloc_block_size", "8192"},
	{ScopeGlobal | ScopeStochastik, InitConnect, ""},
	{ScopeNone, "have_compress", "YES"},
	{ScopeNone, "thread_concurrency", "10"},
	{ScopeGlobal | ScopeStochastik, "query_prealloc_size", "8192"},
	{ScopeNone, "relay_log_space_limit", "0"},
	{ScopeGlobal | ScopeStochastik, MaxUserConnections, "0"},
	{ScopeNone, "performance_schema_max_thread_classes", "50"},
	{ScopeGlobal, "innodb_api_trx_level", "0"},
	{ScopeNone, "disconnect_on_expired_password", "1"},
	{ScopeNone, "performance_schema_max_file_classes", "50"},
	{ScopeGlobal, "expire_logs_days", "0"},
	{ScopeGlobal | ScopeStochastik, BinlogRowQueryLogEvents, "0"},
	{ScopeGlobal, "default_password_lifetime", ""},
	{ScopeNone, "pid_file", "/usr/local/allegrosql/data/localhost.pid"},
	{ScopeNone, "innodb_undo_blockspaces", "0"},
	{ScopeGlobal, InnodbStatusOutputLocks, "0"},
	{ScopeNone, "performance_schema_accounts_size", "100"},
	{ScopeGlobal | ScopeStochastik, "max_error_count", "64"},
	{ScopeGlobal, "max_write_lock_count", "18446744073709551615"},
	{ScopeNone, "performance_schema_max_socket_instances", "322"},
	{ScopeNone, "performance_schema_max_block_instances", "12500"},
	{ScopeGlobal, "innodb_stats_persistent_sample_pages", "20"},
	{ScopeGlobal, "show_compatibility_56", ""},
	{ScopeNone, "innodb_open_files", "2000"},
	{ScopeGlobal, "innodb_spin_wait_delay", "6"},
	{ScopeGlobal, "thread_cache_size", "9"},
	{ScopeGlobal, LogSlowAdminStatements, "0"},
	{ScopeNone, "innodb_checksums", "ON"},
	{ScopeNone, "hostname", ServerHostname},
	{ScopeGlobal | ScopeStochastik, "auto_increment_offset", "1"},
	{ScopeNone, "ft_stopword_file", "(built-in)"},
	{ScopeGlobal, "innodb_max_dirty_pages_pct_lwm", "0"},
	{ScopeGlobal, LogQueriesNotUsingIndexes, "0"},
	{ScopeStochastik, "timestamp", ""},
	{ScopeGlobal | ScopeStochastik, QueryCacheWlockInvalidate, "0"},
	{ScopeGlobal | ScopeStochastik, "sql_buffer_result", "OFF"},
	{ScopeGlobal | ScopeStochastik, "character_set_filesystem", "binary"},
	{ScopeGlobal | ScopeStochastik, "defCauslation_database", allegrosql.DefaultDefCauslationName},
	{ScopeGlobal | ScopeStochastik, AutoIncrementIncrement, strconv.FormatInt(DefAutoIncrementIncrement, 10)},
	{ScopeGlobal | ScopeStochastik, AutoIncrementOffset, strconv.FormatInt(DefAutoIncrementOffset, 10)},
	{ScopeGlobal | ScopeStochastik, "max_heap_block_size", "16777216"},
	{ScopeGlobal | ScopeStochastik, "div_precision_increment", "4"},
	{ScopeGlobal, "innodb_lru_scan_depth", "1024"},
	{ScopeGlobal, "innodb_purge_rseg_truncate_frequency", ""},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLAutoIsNull, "0"},
	{ScopeNone, "innodb_api_enable_binlog", "0"},
	{ScopeGlobal | ScopeStochastik, "innodb_ft_user_stopword_block", ""},
	{ScopeNone, "server_id_bits", "32"},
	{ScopeGlobal, "innodb_log_checksum_algorithm", ""},
	{ScopeNone, "innodb_buffer_pool_load_at_startup", "1"},
	{ScopeGlobal | ScopeStochastik, "sort_buffer_size", "262144"},
	{ScopeGlobal, "innodb_flush_neighbors", "1"},
	{ScopeNone, "innodb_use_sys_malloc", "1"},
	{ScopeStochastik, PluginLoad, ""},
	{ScopeStochastik, PluginDir, "/data/deploy/plugin"},
	{ScopeNone, "performance_schema_max_socket_classes", "10"},
	{ScopeNone, "performance_schema_max_stage_classes", "150"},
	{ScopeGlobal, "innodb_purge_batch_size", "300"},
	{ScopeNone, "have_profiling", "NO"},
	{ScopeGlobal | ScopeStochastik, "character_set_client", allegrosql.DefaultCharset},
	{ScopeGlobal, InnodbBufferPoolDumpNow, "0"},
	{ScopeGlobal, RelayLogPurge, "1"},
	{ScopeGlobal, "ndb_distribution", ""},
	{ScopeGlobal, "myisam_data_pointer_size", "6"},
	{ScopeGlobal, "ndb_optimization_delay", ""},
	{ScopeGlobal, "innodb_ft_num_word_optimize", "2000"},
	{ScopeGlobal | ScopeStochastik, "max_join_size", "18446744073709551615"},
	{ScopeNone, CoreFile, "0"},
	{ScopeGlobal | ScopeStochastik, "max_seeks_for_key", "18446744073709551615"},
	{ScopeNone, "innodb_log_buffer_size", "8388608"},
	{ScopeGlobal, "delayed_insert_timeout", "300"},
	{ScopeGlobal, "max_relay_log_size", "0"},
	{ScopeGlobal | ScopeStochastik, MaxSortLength, "1024"},
	{ScopeNone, "metadata_locks_hash_instances", "8"},
	{ScopeGlobal, "ndb_eventbuffer_free_percent", ""},
	{ScopeNone, "large_files_support", "1"},
	{ScopeGlobal, "binlog_max_flush_queue_time", "0"},
	{ScopeGlobal, "innodb_fill_factor", ""},
	{ScopeGlobal, "log_syslog_facility", ""},
	{ScopeNone, "innodb_ft_min_token_size", "3"},
	{ScopeGlobal | ScopeStochastik, "transaction_write_set_extraction", ""},
	{ScopeGlobal | ScopeStochastik, "ndb_blob_write_batch_bytes", ""},
	{ScopeGlobal, "automatic_sp_privileges", "1"},
	{ScopeGlobal, "innodb_flush_sync", ""},
	{ScopeNone, "performance_schema_events_statements_history_long_size", "10000"},
	{ScopeGlobal, "innodb_monitor_disable", ""},
	{ScopeNone, "innodb_doublewrite", "1"},
	{ScopeNone, "log_bin_use_v1_row_events", "0"},
	{ScopeStochastik, "innodb_optimize_point_storage", ""},
	{ScopeNone, "innodb_api_disable_rowlock", "0"},
	{ScopeGlobal, "innodb_adaptive_flushing_lwm", "10"},
	{ScopeNone, "innodb_log_files_in_group", "2"},
	{ScopeGlobal, InnodbBufferPoolLoadNow, "0"},
	{ScopeNone, "performance_schema_max_rwlock_classes", "40"},
	{ScopeNone, "binlog_gtid_simple_recovery", "1"},
	{ScopeNone, Port, "4000"},
	{ScopeNone, "performance_schema_digests_size", "10000"},
	{ScopeGlobal | ScopeStochastik, Profiling, "0"},
	{ScopeNone, "lower_case_block_names", "2"},
	{ScopeStochastik, "rand_seed1", ""},
	{ScopeGlobal, "sha256_password_proxy_users", ""},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLQuoteShowCreate, "1"},
	{ScopeGlobal | ScopeStochastik, "binlogging_impossible_mode", "IGNORE_ERROR"},
	{ScopeGlobal | ScopeStochastik, QueryCacheSize, "1048576"},
	{ScopeGlobal, "innodb_stats_transient_sample_pages", "8"},
	{ScopeGlobal, InnodbStatsOnMetadata, "0"},
	{ScopeNone, "server_uuid", "00000000-0000-0000-0000-000000000000"},
	{ScopeNone, "open_files_limit", "5000"},
	{ScopeGlobal | ScopeStochastik, "ndb_force_send", ""},
	{ScopeNone, "skip_show_database", "0"},
	{ScopeGlobal, "log_timestamps", ""},
	{ScopeNone, "version_compile_machine", "x86_64"},
	{ScopeGlobal, "event_scheduler", "OFF"},
	{ScopeGlobal | ScopeStochastik, "ndb_deferred_constraints", ""},
	{ScopeGlobal, "log_syslog_include_pid", ""},
	{ScopeStochastik, "last_insert_id", ""},
	{ScopeNone, "innodb_ft_cache_size", "8000000"},
	{ScopeNone, LogBin, "0"},
	{ScopeGlobal, InnodbDisableSortFileCache, "0"},
	{ScopeGlobal, "log_error_verbosity", ""},
	{ScopeNone, "performance_schema_hosts_size", "100"},
	{ScopeGlobal, "innodb_replication_delay", "0"},
	{ScopeGlobal, SlowQueryLog, "0"},
	{ScopeStochastik, "debug_sync", ""},
	{ScopeGlobal, InnodbStatsAutoRecalc, "1"},
	{ScopeGlobal | ScopeStochastik, "lc_messages", "en_US"},
	{ScopeGlobal | ScopeStochastik, "bulk_insert_buffer_size", "8388608"},
	{ScopeGlobal | ScopeStochastik, BinlogDirectNonTransactionalUFIDelates, "0"},
	{ScopeGlobal, "innodb_change_buffering", "all"},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLBigSelects, "1"},
	{ScopeGlobal | ScopeStochastik, CharacterSetResults, allegrosql.DefaultCharset},
	{ScopeGlobal, "innodb_max_purge_lag_delay", "0"},
	{ScopeGlobal | ScopeStochastik, "stochastik_track_schema", ""},
	{ScopeGlobal, "innodb_io_capacity_max", "2000"},
	{ScopeGlobal, "innodb_autoextend_increment", "64"},
	{ScopeGlobal | ScopeStochastik, "binlog_format", "STATEMENT"},
	{ScopeGlobal | ScopeStochastik, "optimizer_trace", "enabled=off,one_line=off"},
	{ScopeGlobal | ScopeStochastik, "read_rnd_buffer_size", "262144"},
	{ScopeNone, "version_comment", "MilevaDB Server (Apache License 2.0) " + versioninfo.MilevaDBEdition + " Edition, MyALLEGROSQL 5.7 compatible"},
	{ScopeGlobal | ScopeStochastik, NetWriteTimeout, "60"},
	{ScopeGlobal, InnodbBufferPoolLoadAbort, "0"},
	{ScopeGlobal | ScopeStochastik, TxnIsolation, "REPEATABLE-READ"},
	{ScopeGlobal | ScopeStochastik, TransactionIsolation, "REPEATABLE-READ"},
	{ScopeGlobal | ScopeStochastik, "defCauslation_connection", allegrosql.DefaultDefCauslationName},
	{ScopeGlobal | ScopeStochastik, "transaction_prealloc_size", "4096"},
	{ScopeNone, "performance_schema_setup_objects_size", "100"},
	{ScopeGlobal, "sync_relay_log", "10000"},
	{ScopeGlobal, "innodb_ft_result_cache_limit", "2000000000"},
	{ScopeNone, "innodb_sort_buffer_size", "1048576"},
	{ScopeGlobal, "innodb_ft_enable_diag_print", "OFF"},
	{ScopeNone, "thread_handling", "one-thread-per-connection"},
	{ScopeGlobal, "stored_program_cache", "256"},
	{ScopeNone, "performance_schema_max_mutex_instances", "15906"},
	{ScopeGlobal, "innodb_adaptive_max_sleep_delay", "150000"},
	{ScopeNone, "large_pages", "OFF"},
	{ScopeGlobal | ScopeStochastik, "stochastik_track_system_variables", ""},
	{ScopeGlobal, "innodb_change_buffer_max_size", "25"},
	{ScopeGlobal, LogBinTrustFunctionCreators, "0"},
	{ScopeNone, "innodb_write_io_threads", "4"},
	{ScopeGlobal, "mysql_native_password_proxy_users", ""},
	{ScopeGlobal, serverReadOnly, "0"},
	{ScopeNone, "large_page_size", "0"},
	{ScopeNone, "block_open_cache_instances", "1"},
	{ScopeGlobal, InnodbStatsPersistent, "1"},
	{ScopeGlobal | ScopeStochastik, "stochastik_track_state_change", ""},
	{ScopeNone, "optimizer_switch", "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,subquery_materialization_cost_based=on,use_index_extensions=on"},
	{ScopeGlobal, "delayed_queue_size", "1000"},
	{ScopeNone, "innodb_read_only", "0"},
	{ScopeNone, "datetime_format", "%Y-%m-%d %H:%i:%s"},
	{ScopeGlobal, "log_syslog", ""},
	{ScopeNone, "version", allegrosql.ServerVersion},
	{ScopeGlobal | ScopeStochastik, "transaction_alloc_block_size", "8192"},
	{ScopeGlobal, "innodb_large_prefix", "OFF"},
	{ScopeNone, "performance_schema_max_cond_classes", "80"},
	{ScopeGlobal, "innodb_io_capacity", "200"},
	{ScopeGlobal, "max_binlog_cache_size", "18446744073709547520"},
	{ScopeGlobal | ScopeStochastik, "ndb_index_stat_enable", ""},
	{ScopeGlobal, "executed_gtids_compression_period", ""},
	{ScopeNone, "time_format", "%H:%i:%s"},
	{ScopeGlobal | ScopeStochastik, OldAlterBlock, "0"},
	{ScopeGlobal | ScopeStochastik, "long_query_time", "10.000000"},
	{ScopeNone, "innodb_use_native_aio", "0"},
	{ScopeGlobal, "log_throttle_queries_not_using_indexes", "0"},
	{ScopeNone, "locked_in_memory", "0"},
	{ScopeNone, "innodb_api_enable_mdl", "0"},
	{ScopeGlobal, "binlog_cache_size", "32768"},
	{ScopeGlobal, "innodb_compression_pad_pct_max", "50"},
	{ScopeGlobal, InnodbCommitConcurrency, "0"},
	{ScopeNone, "ft_min_word_len", "4"},
	{ScopeGlobal, EnforceGtidConsistency, "OFF"},
	{ScopeGlobal, SecureAuth, "1"},
	{ScopeNone, "max_tmp_blocks", "32"},
	{ScopeGlobal, InnodbRandomReadAhead, "0"},
	{ScopeGlobal | ScopeStochastik, UniqueChecks, "1"},
	{ScopeGlobal, "internal_tmp_disk_storage_engine", ""},
	{ScopeGlobal | ScopeStochastik, "myisam_repair_threads", "1"},
	{ScopeGlobal, "ndb_eventbuffer_max_alloc", ""},
	{ScopeGlobal, "innodb_read_ahead_threshold", "56"},
	{ScopeGlobal, "key_cache_block_size", "1024"},
	{ScopeNone, "ndb_recv_thread_cpu_mask", ""},
	{ScopeGlobal, "gtid_purged", ""},
	{ScopeGlobal, "max_binlog_stmt_cache_size", "18446744073709547520"},
	{ScopeGlobal | ScopeStochastik, "lock_wait_timeout", "31536000"},
	{ScopeGlobal | ScopeStochastik, "read_buffer_size", "131072"},
	{ScopeNone, "innodb_read_io_threads", "4"},
	{ScopeGlobal | ScopeStochastik, MaxSpRecursionDepth, "0"},
	{ScopeNone, "ignore_builtin_innodb", "0"},
	{ScopeGlobal, "slow_query_log_file", "/usr/local/allegrosql/data/localhost-slow.log"},
	{ScopeGlobal, "innodb_thread_sleep_delay", "10000"},
	{ScopeNone, "license", "Apache License 2.0"},
	{ScopeGlobal, "innodb_ft_aux_block", ""},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLWarnings, "0"},
	{ScopeGlobal | ScopeStochastik, KeepFilesOnCreate, "0"},
	{ScopeNone, "innodb_data_file_path", "ibdata1:12M:autoextend"},
	{ScopeNone, "performance_schema_setup_actors_size", "100"},
	{ScopeNone, "innodb_additional_mem_pool_size", "8388608"},
	{ScopeNone, "log_error", "/usr/local/allegrosql/data/localhost.err"},
	{ScopeGlobal, "binlog_stmt_cache_size", "32768"},
	{ScopeNone, "relay_log_info_file", "relay-log.info"},
	{ScopeNone, "innodb_ft_total_cache_size", "640000000"},
	{ScopeNone, "performance_schema_max_rwlock_instances", "9102"},
	{ScopeGlobal, "block_open_cache", "2000"},
	{ScopeNone, "performance_schema_events_stages_history_long_size", "10000"},
	{ScopeGlobal | ScopeStochastik, AutoCommit, "1"},
	{ScopeStochastik, "insert_id", ""},
	{ScopeGlobal | ScopeStochastik, "default_tmp_storage_engine", "InnoDB"},
	{ScopeGlobal | ScopeStochastik, "optimizer_search_depth", "62"},
	{ScopeGlobal, "max_points_in_geometry", ""},
	{ScopeGlobal, "innodb_stats_sample_pages", "8"},
	{ScopeGlobal | ScopeStochastik, "profiling_history_size", "15"},
	{ScopeGlobal | ScopeStochastik, "character_set_database", allegrosql.DefaultCharset},
	{ScopeNone, "have_symlink", "YES"},
	{ScopeGlobal | ScopeStochastik, "storage_engine", "InnoDB"},
	{ScopeGlobal | ScopeStochastik, "sql_log_off", "0"},
	// In MyALLEGROSQL, the default value of `explicit_defaults_for_timestamp` is `0`.
	// But In MilevaDB, it's set to `1` to be consistent with MilevaDB timestamp behavior.
	// See: https://github.com/whtcorpsinc/milevadb/pull/6068 for details
	{ScopeNone, "explicit_defaults_for_timestamp", "1"},
	{ScopeNone, "performance_schema_events_waits_history_size", "10"},
	{ScopeGlobal, "log_syslog_tag", ""},
	{ScopeGlobal | ScopeStochastik, TxReadOnly, "0"},
	{ScopeGlobal | ScopeStochastik, TransactionReadOnly, "0"},
	{ScopeGlobal, "innodb_undo_log_truncate", ""},
	{ScopeStochastik, "innodb_create_intrinsic", ""},
	{ScopeGlobal, "gtid_executed_compression_period", ""},
	{ScopeGlobal, "ndb_log_empty_epochs", ""},
	{ScopeGlobal, MaxPreparedStmtCount, strconv.FormatInt(DefMaxPreparedStmtCount, 10)},
	{ScopeNone, "have_geometry", "YES"},
	{ScopeGlobal | ScopeStochastik, "optimizer_trace_max_mem_size", "16384"},
	{ScopeGlobal | ScopeStochastik, "net_retry_count", "10"},
	{ScopeStochastik, "ndb_block_no_logging", ""},
	{ScopeGlobal | ScopeStochastik, "optimizer_trace_features", "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on"},
	{ScopeGlobal, "innodb_flush_log_at_trx_commit", "1"},
	{ScopeGlobal, "rewriter_enabled", ""},
	{ScopeGlobal, "query_cache_min_res_unit", "4096"},
	{ScopeGlobal | ScopeStochastik, "uFIDelablock_views_with_limit", "YES"},
	{ScopeGlobal | ScopeStochastik, "optimizer_prune_level", "1"},
	{ScopeGlobal | ScopeStochastik, "completion_type", "NO_CHAIN"},
	{ScopeGlobal, "binlog_checksum", "CRC32"},
	{ScopeNone, "report_port", "3306"},
	{ScopeGlobal | ScopeStochastik, ShowOldTemporals, "0"},
	{ScopeGlobal, "query_cache_limit", "1048576"},
	{ScopeGlobal, "innodb_buffer_pool_size", "134217728"},
	{ScopeGlobal, InnodbAdaptiveFlushing, "1"},
	{ScopeNone, "datadir", "/usr/local/allegrosql/data/"},
	{ScopeGlobal | ScopeStochastik, WaitTimeout, strconv.FormatInt(DefWaitTimeout, 10)},
	{ScopeGlobal, "innodb_monitor_enable", ""},
	{ScopeNone, "date_format", "%Y-%m-%d"},
	{ScopeGlobal, "innodb_buffer_pool_filename", "ib_buffer_pool"},
	{ScopeGlobal, "slow_launch_time", "2"},
	{ScopeGlobal | ScopeStochastik, "ndb_use_transactions", ""},
	{ScopeNone, "innodb_purge_threads", "1"},
	{ScopeGlobal, "innodb_concurrency_tickets", "5000"},
	{ScopeGlobal, "innodb_monitor_reset_all", ""},
	{ScopeNone, "performance_schema_users_size", "100"},
	{ScopeGlobal, "ndb_log_uFIDelated_only", ""},
	{ScopeNone, "basedir", "/usr/local/allegrosql"},
	{ScopeGlobal, "innodb_old_blocks_time", "1000"},
	{ScopeGlobal, "innodb_stats_method", "nulls_equal"},
	{ScopeGlobal | ScopeStochastik, InnodbLockWaitTimeout, strconv.FormatInt(DefInnodbLockWaitTimeout, 10)},
	{ScopeGlobal, LocalInFile, "1"},
	{ScopeGlobal | ScopeStochastik, "myisam_stats_method", "nulls_unequal"},
	{ScopeNone, "version_compile_os", "osx10.8"},
	{ScopeNone, "relay_log_recovery", "0"},
	{ScopeNone, "old", "0"},
	{ScopeGlobal | ScopeStochastik, InnodbBlockLocks, "1"},
	{ScopeNone, PerformanceSchema, "0"},
	{ScopeNone, "myisam_recover_options", "OFF"},
	{ScopeGlobal | ScopeStochastik, NetBufferLength, "16384"},
	{ScopeGlobal | ScopeStochastik, "binlog_row_image", "FULL"},
	{ScopeNone, "innodb_locks_unsafe_for_binlog", "0"},
	{ScopeStochastik, "rbr_exec_mode", ""},
	{ScopeGlobal, "myisam_max_sort_file_size", "9223372036853727232"},
	{ScopeNone, "back_log", "80"},
	{ScopeNone, "lower_case_file_system", "1"},
	{ScopeGlobal | ScopeStochastik, GroupConcatMaxLen, "1024"},
	{ScopeStochastik, "pseudo_thread_id", ""},
	{ScopeNone, "socket", "/tmp/myssock"},
	{ScopeNone, "have_dynamic_loading", "YES"},
	{ScopeGlobal, "rewriter_verbose", ""},
	{ScopeGlobal, "innodb_undo_logs", "128"},
	{ScopeNone, "performance_schema_max_cond_instances", "3504"},
	{ScopeGlobal, "delayed_insert_limit", "100"},
	{ScopeGlobal, Flush, "0"},
	{ScopeGlobal | ScopeStochastik, "eq_range_index_dive_limit", "10"},
	{ScopeNone, "performance_schema_events_stages_history_size", "10"},
	{ScopeGlobal | ScopeStochastik, "character_set_connection", allegrosql.DefaultCharset},
	{ScopeGlobal, MyISAMUseMmap, "0"},
	{ScopeGlobal | ScopeStochastik, "ndb_join_pushdown", ""},
	{ScopeGlobal | ScopeStochastik, CharacterSetServer, allegrosql.DefaultCharset},
	{ScopeGlobal, "validate_password_special_char_count", "1"},
	{ScopeNone, "performance_schema_max_thread_instances", "402"},
	{ScopeGlobal | ScopeStochastik, "ndbinfo_show_hidden", ""},
	{ScopeGlobal | ScopeStochastik, "net_read_timeout", "30"},
	{ScopeNone, "innodb_page_size", "16384"},
	{ScopeGlobal | ScopeStochastik, MaxAllowedPacket, "67108864"},
	{ScopeNone, "innodb_log_file_size", "50331648"},
	{ScopeGlobal, "sync_relay_log_info", "10000"},
	{ScopeGlobal | ScopeStochastik, "optimizer_trace_limit", "1"},
	{ScopeNone, "innodb_ft_max_token_size", "84"},
	{ScopeGlobal, "validate_password_length", "8"},
	{ScopeGlobal, "ndb_log_binlog_index", ""},
	{ScopeGlobal, "innodb_api_bk_commit_interval", "5"},
	{ScopeNone, "innodb_undo_directory", "."},
	{ScopeNone, "bind_address", "*"},
	{ScopeGlobal, "innodb_sync_spin_loops", "30"},
	{ScopeGlobal | ScopeStochastik, ALLEGROSQLSafeUFIDelates, "0"},
	{ScopeNone, "tmFIDelir", "/var/tmp/"},
	{ScopeGlobal, "innodb_thread_concurrency", "0"},
	{ScopeGlobal, "innodb_buffer_pool_dump_pct", ""},
	{ScopeGlobal | ScopeStochastik, "lc_time_names", "en_US"},
	{ScopeGlobal | ScopeStochastik, "max_statement_time", ""},
	{ScopeGlobal | ScopeStochastik, EndMakersInJSON, "0"},
	{ScopeGlobal, AvoidTemporalUpgrade, "0"},
	{ScopeGlobal, "key_cache_age_threshold", "300"},
	{ScopeGlobal, InnodbStatusOutput, "0"},
	{ScopeStochastik, "identity", ""},
	{ScopeGlobal | ScopeStochastik, "min_examined_row_limit", "0"},
	{ScopeGlobal, "sync_frm", "ON"},
	{ScopeGlobal, "innodb_online_alter_log_max_size", "134217728"},
	{ScopeStochastik, WarningCount, "0"},
	{ScopeStochastik, ErrorCount, "0"},
	{ScopeGlobal | ScopeStochastik, "information_schema_stats_expiry", "86400"},
	{ScopeGlobal, "thread_pool_size", "16"},
	{ScopeGlobal | ScopeStochastik, WindowingUseHighPrecision, "ON"},
	/* MilevaDB specific variables */
	{ScopeStochastik, MilevaDBSnapshot, ""},
	{ScopeStochastik, MilevaDBOptAggPushDown, BoolToIntStr(DefOptAggPushDown)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptBCJ, BoolToIntStr(DefOptBCJ)},
	{ScopeStochastik, MilevaDBOptDistinctAggPushDown, BoolToIntStr(config.GetGlobalConfig().Performance.DistinctAggPushDown)},
	{ScopeStochastik, MilevaDBOptWriteRowID, BoolToIntStr(DefOptWriteRowID)},
	{ScopeGlobal | ScopeStochastik, MilevaDBBuildStatsConcurrency, strconv.Itoa(DefBuildStatsConcurrency)},
	{ScopeGlobal, MilevaDBAutoAnalyzeRatio, strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64)},
	{ScopeGlobal, MilevaDBAutoAnalyzeStartTime, DefAutoAnalyzeStartTime},
	{ScopeGlobal, MilevaDBAutoAnalyzeEndTime, DefAutoAnalyzeEndTime},
	{ScopeStochastik, MilevaDBChecksumBlockConcurrency, strconv.Itoa(DefChecksumBlockConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBExecutorConcurrency, strconv.Itoa(DefExecutorConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBDistALLEGROSQLScanConcurrency, strconv.Itoa(DefDistALLEGROSQLScanConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptInSubqToJoinAnPosetDagg, BoolToIntStr(DefOptInSubqToJoinAnPosetDagg)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptCorrelationThreshold, strconv.FormatFloat(DefOptCorrelationThreshold, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptCorrelationExpFactor, strconv.Itoa(DefOptCorrelationExpFactor)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptCPUFactor, strconv.FormatFloat(DefOptCPUFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptTiFlashConcurrencyFactor, strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptCopCPUFactor, strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptNetworkFactor, strconv.FormatFloat(DefOptNetworkFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptScanFactor, strconv.FormatFloat(DefOptScanFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptDescScanFactor, strconv.FormatFloat(DefOptDescScanFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptSeekFactor, strconv.FormatFloat(DefOptSeekFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptMemoryFactor, strconv.FormatFloat(DefOptMemoryFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptDiskFactor, strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptConcurrencyFactor, strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64)},
	{ScopeGlobal | ScopeStochastik, MilevaDBIndexJoinBatchSize, strconv.Itoa(DefIndexJoinBatchSize)},
	{ScopeGlobal | ScopeStochastik, MilevaDBIndexLookupSize, strconv.Itoa(DefIndexLookupSize)},
	{ScopeGlobal | ScopeStochastik, MilevaDBIndexLookupConcurrency, strconv.Itoa(DefIndexLookupConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBIndexLookupJoinConcurrency, strconv.Itoa(DefIndexLookupJoinConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBIndexSerialScanConcurrency, strconv.Itoa(DefIndexSerialScanConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBSkipUTF8Check, BoolToIntStr(DefSkipUTF8Check)},
	{ScopeGlobal | ScopeStochastik, MilevaDBSkipASCIICheck, BoolToIntStr(DefSkipASCIICheck)},
	{ScopeStochastik, MilevaDBBatchInsert, BoolToIntStr(DefBatchInsert)},
	{ScopeStochastik, MilevaDBBatchDelete, BoolToIntStr(DefBatchDelete)},
	{ScopeStochastik, MilevaDBBatchCommit, BoolToIntStr(DefBatchCommit)},
	{ScopeGlobal | ScopeStochastik, MilevaDBDMLBatchSize, strconv.Itoa(DefDMLBatchSize)},
	{ScopeStochastik, MilevaDBCurrentTS, strconv.Itoa(DefCurretTS)},
	{ScopeStochastik, MilevaDBLastTxnInfo, strconv.Itoa(DefCurretTS)},
	{ScopeGlobal | ScopeStochastik, MilevaDBMaxChunkSize, strconv.Itoa(DefMaxChunkSize)},
	{ScopeGlobal | ScopeStochastik, MilevaDBAllowBatchCop, strconv.Itoa(DefMilevaDBAllowBatchCop)},
	{ScopeGlobal | ScopeStochastik, MilevaDBInitChunkSize, strconv.Itoa(DefInitChunkSize)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableCascadesPlanner, "0"},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableIndexMerge, "0"},
	{ScopeStochastik, MilevaDBMemQuotaQuery, strconv.FormatInt(config.GetGlobalConfig().MemQuotaQuery, 10)},
	{ScopeStochastik, MilevaDBMemQuotaHashJoin, strconv.FormatInt(DefMilevaDBMemQuotaHashJoin, 10)},
	{ScopeStochastik, MilevaDBMemQuotaMergeJoin, strconv.FormatInt(DefMilevaDBMemQuotaMergeJoin, 10)},
	{ScopeStochastik, MilevaDBMemQuotaSort, strconv.FormatInt(DefMilevaDBMemQuotaSort, 10)},
	{ScopeStochastik, MilevaDBMemQuotaTopn, strconv.FormatInt(DefMilevaDBMemQuotaTopn, 10)},
	{ScopeStochastik, MilevaDBMemQuotaIndexLookupReader, strconv.FormatInt(DefMilevaDBMemQuotaIndexLookupReader, 10)},
	{ScopeStochastik, MilevaDBMemQuotaIndexLookupJoin, strconv.FormatInt(DefMilevaDBMemQuotaIndexLookupJoin, 10)},
	{ScopeStochastik, MilevaDBMemQuotaNestedLoopApply, strconv.FormatInt(DefMilevaDBMemQuotaNestedLoopApply, 10)},
	{ScopeStochastik, MilevaDBEnableStreaming, "0"},
	{ScopeStochastik, MilevaDBEnableChunkRPC, "1"},
	{ScopeStochastik, TxnIsolationOneShot, ""},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableBlockPartition, "on"},
	{ScopeGlobal | ScopeStochastik, MilevaDBHashJoinConcurrency, strconv.Itoa(DefMilevaDBHashJoinConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBProjectionConcurrency, strconv.Itoa(DefMilevaDBProjectionConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBHashAggPartialConcurrency, strconv.Itoa(DefMilevaDBHashAggPartialConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBHashAggFinalConcurrency, strconv.Itoa(DefMilevaDBHashAggFinalConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBWindowConcurrency, strconv.Itoa(DefMilevaDBWindowConcurrency)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableParallelApply, BoolToIntStr(DefMilevaDBEnableParallelApply)},
	{ScopeGlobal | ScopeStochastik, MilevaDBBackoffLockFast, strconv.Itoa(ekv.DefBackoffLockFast)},
	{ScopeGlobal | ScopeStochastik, MilevaDBBackOffWeight, strconv.Itoa(ekv.DefBackOffWeight)},
	{ScopeGlobal | ScopeStochastik, MilevaDBRetryLimit, strconv.Itoa(DefMilevaDBRetryLimit)},
	{ScopeGlobal | ScopeStochastik, MilevaDBDisableTxnAutoRetry, BoolToIntStr(DefMilevaDBDisableTxnAutoRetry)},
	{ScopeGlobal | ScopeStochastik, MilevaDBConstraintCheckInPlace, BoolToIntStr(DefMilevaDBConstraintCheckInPlace)},
	{ScopeGlobal | ScopeStochastik, MilevaDBTxnMode, DefMilevaDBTxnMode},
	{ScopeGlobal, MilevaDBRowFormatVersion, strconv.Itoa(DefMilevaDBRowFormatV1)},
	{ScopeStochastik, MilevaDBOptimizerSelectivityLevel, strconv.Itoa(DefMilevaDBOptimizerSelectivityLevel)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableWindowFunction, BoolToIntStr(DefEnableWindowFunction)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableVectorizedExpression, BoolToIntStr(DefEnableVectorizedExpression)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableFastAnalyze, BoolToIntStr(DefMilevaDBUseFastAnalyze)},
	{ScopeGlobal | ScopeStochastik, MilevaDBSkipIsolationLevelCheck, BoolToIntStr(DefMilevaDBSkipIsolationLevelCheck)},
	/* The following variable is defined as stochastik scope but is actually server scope. */
	{ScopeStochastik, MilevaDBGeneralLog, strconv.Itoa(DefMilevaDBGeneralLog)},
	{ScopeStochastik, MilevaDBPProfALLEGROSQLCPU, strconv.Itoa(DefMilevaDBPProfALLEGROSQLCPU)},
	{ScopeStochastik, MilevaDBDBSSlowOprThreshold, strconv.Itoa(DefMilevaDBDBSSlowOprThreshold)},
	{ScopeStochastik, MilevaDBConfig, ""},
	{ScopeGlobal, MilevaDBDBSReorgWorkerCount, strconv.Itoa(DefMilevaDBDBSReorgWorkerCount)},
	{ScopeGlobal, MilevaDBDBSReorgBatchSize, strconv.Itoa(DefMilevaDBDBSReorgBatchSize)},
	{ScopeGlobal, MilevaDBDBSErrorCountLimit, strconv.Itoa(DefMilevaDBDBSErrorCountLimit)},
	{ScopeStochastik, MilevaDBDBSReorgPriority, "PRIORITY_LOW"},
	{ScopeGlobal, MilevaDBMaxDeltaSchemaCount, strconv.Itoa(DefMilevaDBMaxDeltaSchemaCount)},
	{ScopeGlobal, MilevaDBEnableChangeDeferredCausetType, BoolToIntStr(DefMilevaDBChangeDeferredCausetType)},
	{ScopeStochastik, MilevaDBForcePriority, allegrosql.Priority2Str[DefMilevaDBForcePriority]},
	{ScopeStochastik, MilevaDBEnableRadixJoin, BoolToIntStr(DefMilevaDBUseRadixJoin)},
	{ScopeGlobal | ScopeStochastik, MilevaDBOptJoinReorderThreshold, strconv.Itoa(DefMilevaDBOptJoinReorderThreshold)},
	{ScopeStochastik, MilevaDBSlowQueryFile, ""},
	{ScopeGlobal, MilevaDBScatterRegion, BoolToIntStr(DefMilevaDBScatterRegion)},
	{ScopeStochastik, MilevaDBWaitSplitRegionFinish, BoolToIntStr(DefMilevaDBWaitSplitRegionFinish)},
	{ScopeStochastik, MilevaDBWaitSplitRegionTimeout, strconv.Itoa(DefWaitSplitRegionTimeout)},
	{ScopeStochastik, MilevaDBLowResolutionTSO, "0"},
	{ScopeStochastik, MilevaDBExpensiveQueryTimeThreshold, strconv.Itoa(DefMilevaDBExpensiveQueryTimeThreshold)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableNoopFuncs, BoolToIntStr(DefMilevaDBEnableNoopFuncs)},
	{ScopeStochastik, MilevaDBReplicaRead, "leader"},
	{ScopeStochastik, MilevaDBAllowRemoveAutoInc, BoolToIntStr(DefMilevaDBAllowRemoveAutoInc)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableStmtSummary, BoolToIntStr(config.GetGlobalConfig().StmtSummary.Enable)},
	{ScopeGlobal | ScopeStochastik, MilevaDBStmtSummaryInternalQuery, BoolToIntStr(config.GetGlobalConfig().StmtSummary.EnableInternalQuery)},
	{ScopeGlobal | ScopeStochastik, MilevaDBStmtSummaryRefreshInterval, strconv.Itoa(config.GetGlobalConfig().StmtSummary.RefreshInterval)},
	{ScopeGlobal | ScopeStochastik, MilevaDBStmtSummaryHistorySize, strconv.Itoa(config.GetGlobalConfig().StmtSummary.HistorySize)},
	{ScopeGlobal | ScopeStochastik, MilevaDBStmtSummaryMaxStmtCount, strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), 10)},
	{ScopeGlobal | ScopeStochastik, MilevaDBStmtSummaryMaxALLEGROSQLLength, strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxALLEGROSQLLength), 10)},
	{ScopeGlobal | ScopeStochastik, MilevaDBCapturePlanBaseline, "off"},
	{ScopeGlobal | ScopeStochastik, MilevaDBUsePlanBaselines, boolToOnOff(DefMilevaDBUsePlanBaselines)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEvolvePlanBaselines, boolToOnOff(DefMilevaDBEvolvePlanBaselines)},
	{ScopeGlobal, MilevaDBEvolvePlanTaskMaxTime, strconv.Itoa(DefMilevaDBEvolvePlanTaskMaxTime)},
	{ScopeGlobal, MilevaDBEvolvePlanTaskStartTime, DefMilevaDBEvolvePlanTaskStartTime},
	{ScopeGlobal, MilevaDBEvolvePlanTaskEndTime, DefMilevaDBEvolvePlanTaskEndTime},
	{ScopeStochastik, MilevaDBIsolationReadEngines, strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ", ")},
	{ScopeGlobal | ScopeStochastik, MilevaDBStoreLimit, strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().EinsteinDBClient.StoreLimit), 10)},
	{ScopeStochastik, MilevaDBMetricSchemaStep, strconv.Itoa(DefMilevaDBMetricSchemaStep)},
	{ScopeStochastik, MilevaDBMetricSchemaRangeDuration, strconv.Itoa(DefMilevaDBMetricSchemaRangeDuration)},
	{ScopeStochastik, MilevaDBSlowLogThreshold, strconv.Itoa(logutil.DefaultSlowThreshold)},
	{ScopeStochastik, MilevaDBRecordPlanInSlowLog, strconv.Itoa(logutil.DefaultRecordPlanInSlowLog)},
	{ScopeStochastik, MilevaDBEnableSlowLog, BoolToIntStr(logutil.DefaultMilevaDBEnableSlowLog)},
	{ScopeStochastik, MilevaDBQueryLogMaxLen, strconv.Itoa(logutil.DefaultQueryLogMaxLen)},
	{ScopeStochastik, MilevaDBCheckMb4ValueInUTF8, BoolToIntStr(config.GetGlobalConfig().CheckMb4ValueInUTF8)},
	{ScopeStochastik, MilevaDBFoundInPlanCache, BoolToIntStr(DefMilevaDBFoundInPlanCache)},
	{ScopeStochastik, MilevaDBEnableDefCauslectExecutionInfo, BoolToIntStr(DefMilevaDBEnableDefCauslectExecutionInfo)},
	{ScopeGlobal | ScopeStochastik, MilevaDBAllowAutoRandExplicitInsert, boolToOnOff(DefMilevaDBAllowAutoRandExplicitInsert)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableClusteredIndex, BoolToIntStr(DefMilevaDBEnableClusteredIndex)},
	{ScopeGlobal | ScopeStochastik, MilevaDBPartitionPruneMode, string(StaticOnly)},
	{ScopeGlobal, MilevaDBSlowLogMasking, BoolToIntStr(DefMilevaDBSlowLogMasking)},
	{ScopeGlobal, MilevaDBRedactLog, strconv.Itoa(config.DefMilevaDBRedactLog)},
	{ScopeGlobal | ScopeStochastik, MilevaDBShardAllocateStep, strconv.Itoa(DefMilevaDBShardAllocateStep)},
	{ScopeGlobal, MilevaDBEnableTelemetry, BoolToIntStr(DefMilevaDBEnableTelemetry)},
	{ScopeGlobal | ScopeStochastik, MilevaDBEnableAmendPessimisticTxn, boolToOnOff(DefMilevaDBEnableAmendPessimisticTxn)},

	// for compatibility purpose, we should leave them alone.
	// TODO: Follow the Terminology UFIDelates of MyALLEGROSQL after their changes arrived.
	// https://mysqlhighavailability.com/allegrosql-terminology-uFIDelates/
	{ScopeStochastik, PseudoSlaveMode, ""},
	{ScopeGlobal, "slave_pending_jobs_size_max", "16777216"},
	{ScopeGlobal, "slave_transaction_retries", "10"},
	{ScopeGlobal, "slave_checkpoint_period", "300"},
	{ScopeGlobal, MasterVerifyChecksum, "0"},
	{ScopeGlobal, "rpl_semi_sync_master_trace_level", ""},
	{ScopeGlobal, "master_info_repository", "FILE"},
	{ScopeGlobal, "rpl_stop_slave_timeout", "31536000"},
	{ScopeGlobal, "slave_net_timeout", "3600"},
	{ScopeGlobal, "sync_master_info", "10000"},
	{ScopeGlobal, "init_slave", ""},
	{ScopeGlobal, SlaveCompressedProtodefCaus, "0"},
	{ScopeGlobal, "rpl_semi_sync_slave_trace_level", ""},
	{ScopeGlobal, LogSlowSlaveStatements, "0"},
	{ScopeGlobal, "slave_checkpoint_group", "512"},
	{ScopeNone, "slave_load_tmFIDelir", "/var/tmp/"},
	{ScopeGlobal, "slave_parallel_type", ""},
	{ScopeGlobal, "slave_parallel_workers", "0"},
	{ScopeGlobal, "rpl_semi_sync_master_timeout", ""},
	{ScopeNone, "slave_skip_errors", "OFF"},
	{ScopeGlobal, "sql_slave_skip_counter", "0"},
	{ScopeGlobal, "rpl_semi_sync_slave_enabled", ""},
	{ScopeGlobal, "rpl_semi_sync_master_enabled", ""},
	{ScopeGlobal, "slave_preserve_commit_order", ""},
	{ScopeGlobal, "slave_exec_mode", "STRICT"},
	{ScopeNone, "log_slave_uFIDelates", "0"},
	{ScopeGlobal, "rpl_semi_sync_master_wait_point", ""},
	{ScopeGlobal, "slave_sql_verify_checksum", "1"},
	{ScopeGlobal, "slave_max_allowed_packet", "1073741824"},
	{ScopeGlobal, "rpl_semi_sync_master_wait_for_slave_count", ""},
	{ScopeGlobal, "rpl_semi_sync_master_wait_no_slave", ""},
	{ScopeGlobal, "slave_rows_search_algorithms", "TABLE_SCAN,INDEX_SCAN"},
	{ScopeGlobal, SlaveAllowBatching, "0"},
}

// SynonymsSysVariables is synonyms of system variables.
var SynonymsSysVariables = map[string][]string{}

func addSynonymsSysVariables(synonyms ...string) {
	for _, s := range synonyms {
		SynonymsSysVariables[s] = synonyms
	}
}

func initSynonymsSysVariables() {
	addSynonymsSysVariables(TxnIsolation, TransactionIsolation)
	addSynonymsSysVariables(TxReadOnly, TransactionReadOnly)
}

// SetNamesVariables is the system variable names related to set names statements.
var SetNamesVariables = []string{
	"character_set_client",
	"character_set_connection",
	"character_set_results",
}

// SetCharsetVariables is the system variable names related to set charset statements.
var SetCharsetVariables = []string{
	"character_set_client",
	"character_set_results",
}

const (
	// CharacterSetConnection is the name for character_set_connection system variable.
	CharacterSetConnection = "character_set_connection"
	// DefCauslationConnection is the name for defCauslation_connection system variable.
	DefCauslationConnection = "defCauslation_connection"
	// CharsetDatabase is the name for character_set_database system variable.
	CharsetDatabase = "character_set_database"
	// DefCauslationDatabase is the name for defCauslation_database system variable.
	DefCauslationDatabase = "defCauslation_database"
	// GeneralLog is the name for 'general_log' system variable.
	GeneralLog = "general_log"
	// AvoidTemporalUpgrade is the name for 'avoid_temporal_upgrade' system variable.
	AvoidTemporalUpgrade = "avoid_temporal_upgrade"
	// MaxPreparedStmtCount is the name for 'max_prepared_stmt_count' system variable.
	MaxPreparedStmtCount = "max_prepared_stmt_count"
	// BigBlocks is the name for 'big_blocks' system variable.
	BigBlocks = "big_blocks"
	// CheckProxyUsers is the name for 'check_proxy_users' system variable.
	CheckProxyUsers = "check_proxy_users"
	// CoreFile is the name for 'core_file' system variable.
	CoreFile = "core_file"
	// DefaultWeekFormat is the name for 'default_week_format' system variable.
	DefaultWeekFormat = "default_week_format"
	// GroupConcatMaxLen is the name for 'group_concat_max_len' system variable.
	GroupConcatMaxLen = "group_concat_max_len"
	// DelayKeyWrite is the name for 'delay_key_write' system variable.
	DelayKeyWrite = "delay_key_write"
	// EndMakersInJSON is the name for 'end_markers_in_json' system variable.
	EndMakersInJSON = "end_markers_in_json"
	// InnodbCommitConcurrency is the name for 'innodb_commit_concurrency' system variable.
	InnodbCommitConcurrency = "innodb_commit_concurrency"
	// InnodbFastShutdown is the name for 'innodb_fast_shutdown' system variable.
	InnodbFastShutdown = "innodb_fast_shutdown"
	// InnodbLockWaitTimeout is the name for 'innodb_lock_wait_timeout' system variable.
	InnodbLockWaitTimeout = "innodb_lock_wait_timeout"
	// ALLEGROSQLLogBin is the name for 'sql_log_bin' system variable.
	ALLEGROSQLLogBin = "sql_log_bin"
	// LogBin is the name for 'log_bin' system variable.
	LogBin = "log_bin"
	// MaxSortLength is the name for 'max_sort_length' system variable.
	MaxSortLength = "max_sort_length"
	// MaxSpRecursionDepth is the name for 'max_sp_recursion_depth' system variable.
	MaxSpRecursionDepth = "max_sp_recursion_depth"
	// MaxUserConnections is the name for 'max_user_connections' system variable.
	MaxUserConnections = "max_user_connections"
	// OfflineMode is the name for 'offline_mode' system variable.
	OfflineMode = "offline_mode"
	// InteractiveTimeout is the name for 'interactive_timeout' system variable.
	InteractiveTimeout = "interactive_timeout"
	// FlushTime is the name for 'flush_time' system variable.
	FlushTime = "flush_time"
	// PseudoSlaveMode is the name for 'pseudo_slave_mode' system variable.
	PseudoSlaveMode = "pseudo_slave_mode"
	// LowPriorityUFIDelates is the name for 'low_priority_uFIDelates' system variable.
	LowPriorityUFIDelates = "low_priority_uFIDelates"
	// StochastikTrackGtids is the name for 'stochastik_track_gtids' system variable.
	StochastikTrackGtids = "stochastik_track_gtids"
	// OldPasswords is the name for 'old_passwords' system variable.
	OldPasswords = "old_passwords"
	// MaxConnections is the name for 'max_connections' system variable.
	MaxConnections = "max_connections"
	// SkipNameResolve is the name for 'skip_name_resolve' system variable.
	SkipNameResolve = "skip_name_resolve"
	// ForeignKeyChecks is the name for 'foreign_key_checks' system variable.
	ForeignKeyChecks = "foreign_key_checks"
	// ALLEGROSQLSafeUFIDelates is the name for 'sql_safe_uFIDelates' system variable.
	ALLEGROSQLSafeUFIDelates = "sql_safe_uFIDelates"
	// WarningCount is the name for 'warning_count' system variable.
	WarningCount = "warning_count"
	// ErrorCount is the name for 'error_count' system variable.
	ErrorCount = "error_count"
	// ALLEGROSQLSelectLimit is the name for 'sql_select_limit' system variable.
	ALLEGROSQLSelectLimit = "sql_select_limit"
	// MaxConnectErrors is the name for 'max_connect_errors' system variable.
	MaxConnectErrors = "max_connect_errors"
	// BlockDefinitionCache is the name for 'block_definition_cache' system variable.
	BlockDefinitionCache = "block_definition_cache"
	// TmpBlockSize is the name for 'tmp_block_size' system variable.
	TmpBlockSize = "tmp_block_size"
	// ConnectTimeout is the name for 'connect_timeout' system variable.
	ConnectTimeout = "connect_timeout"
	// SyncBinlog is the name for 'sync_binlog' system variable.
	SyncBinlog = "sync_binlog"
	// BlockEncryptionMode is the name for 'block_encryption_mode' system variable.
	BlockEncryptionMode = "block_encryption_mode"
	// WaitTimeout is the name for 'wait_timeout' system variable.
	WaitTimeout = "wait_timeout"
	// ValidatePasswordNumberCount is the name of 'validate_password_number_count' system variable.
	ValidatePasswordNumberCount = "validate_password_number_count"
	// ValidatePasswordLength is the name of 'validate_password_length' system variable.
	ValidatePasswordLength = "validate_password_length"
	// PluginDir is the name of 'plugin_dir' system variable.
	PluginDir = "plugin_dir"
	// PluginLoad is the name of 'plugin_load' system variable.
	PluginLoad = "plugin_load"
	// Port is the name for 'port' system variable.
	Port = "port"
	// DataDir is the name for 'datadir' system variable.
	DataDir = "datadir"
	// Profiling is the name for 'Profiling' system variable.
	Profiling = "profiling"
	// Socket is the name for 'socket' system variable.
	Socket = "socket"
	// BinlogOrderCommits is the name for 'binlog_order_commits' system variable.
	BinlogOrderCommits = "binlog_order_commits"
	// MasterVerifyChecksum is the name for 'master_verify_checksum' system variable.
	MasterVerifyChecksum = "master_verify_checksum"
	// ValidatePasswordCheckUserName is the name for 'validate_password_check_user_name' system variable.
	ValidatePasswordCheckUserName = "validate_password_check_user_name"
	// SuperReadOnly is the name for 'super_read_only' system variable.
	SuperReadOnly = "super_read_only"
	// ALLEGROSQLNotes is the name for 'sql_notes' system variable.
	ALLEGROSQLNotes = "sql_notes"
	// QueryCacheType is the name for 'query_cache_type' system variable.
	QueryCacheType = "query_cache_type"
	// SlaveCompressedProtodefCaus is the name for 'slave_compressed_protodefCaus' system variable.
	SlaveCompressedProtodefCaus = "slave_compressed_protodefCaus"
	// BinlogRowQueryLogEvents is the name for 'binlog_rows_query_log_events' system variable.
	BinlogRowQueryLogEvents = "binlog_rows_query_log_events"
	// LogSlowSlaveStatements is the name for 'log_slow_slave_statements' system variable.
	LogSlowSlaveStatements = "log_slow_slave_statements"
	// LogSlowAdminStatements is the name for 'log_slow_admin_statements' system variable.
	LogSlowAdminStatements = "log_slow_admin_statements"
	// LogQueriesNotUsingIndexes is the name for 'log_queries_not_using_indexes' system variable.
	LogQueriesNotUsingIndexes = "log_queries_not_using_indexes"
	// QueryCacheWlockInvalidate is the name for 'query_cache_wlock_invalidate' system variable.
	QueryCacheWlockInvalidate = "query_cache_wlock_invalidate"
	// ALLEGROSQLAutoIsNull is the name for 'sql_auto_is_null' system variable.
	ALLEGROSQLAutoIsNull = "sql_auto_is_null"
	// RelayLogPurge is the name for 'relay_log_purge' system variable.
	RelayLogPurge = "relay_log_purge"
	// AutomaticSpPrivileges is the name for 'automatic_sp_privileges' system variable.
	AutomaticSpPrivileges = "automatic_sp_privileges"
	// ALLEGROSQLQuoteShowCreate is the name for 'sql_quote_show_create' system variable.
	ALLEGROSQLQuoteShowCreate = "sql_quote_show_create"
	// SlowQueryLog is the name for 'slow_query_log' system variable.
	SlowQueryLog = "slow_query_log"
	// BinlogDirectNonTransactionalUFIDelates is the name for 'binlog_direct_non_transactional_uFIDelates' system variable.
	BinlogDirectNonTransactionalUFIDelates = "binlog_direct_non_transactional_uFIDelates"
	// ALLEGROSQLBigSelects is the name for 'sql_big_selects' system variable.
	ALLEGROSQLBigSelects = "sql_big_selects"
	// LogBinTrustFunctionCreators is the name for 'log_bin_trust_function_creators' system variable.
	LogBinTrustFunctionCreators = "log_bin_trust_function_creators"
	// OldAlterBlock is the name for 'old_alter_block' system variable.
	OldAlterBlock = "old_alter_block"
	// EnforceGtidConsistency is the name for 'enforce_gtid_consistency' system variable.
	EnforceGtidConsistency = "enforce_gtid_consistency"
	// SecureAuth is the name for 'secure_auth' system variable.
	SecureAuth = "secure_auth"
	// UniqueChecks is the name for 'unique_checks' system variable.
	UniqueChecks = "unique_checks"
	// ALLEGROSQLWarnings is the name for 'sql_warnings' system variable.
	ALLEGROSQLWarnings = "sql_warnings"
	// AutoCommit is the name for 'autocommit' system variable.
	AutoCommit = "autocommit"
	// KeepFilesOnCreate is the name for 'keep_files_on_create' system variable.
	KeepFilesOnCreate = "keep_files_on_create"
	// ShowOldTemporals is the name for 'show_old_temporals' system variable.
	ShowOldTemporals = "show_old_temporals"
	// LocalInFile is the name for 'local_infile' system variable.
	LocalInFile = "local_infile"
	// PerformanceSchema is the name for 'performance_schema' system variable.
	PerformanceSchema = "performance_schema"
	// Flush is the name for 'flush' system variable.
	Flush = "flush"
	// SlaveAllowBatching is the name for 'slave_allow_batching' system variable.
	SlaveAllowBatching = "slave_allow_batching"
	// MyISAMUseMmap is the name for 'myisam_use_mmap' system variable.
	MyISAMUseMmap = "myisam_use_mmap"
	// InnodbFilePerBlock is the name for 'innodb_file_per_block' system variable.
	InnodbFilePerBlock = "innodb_file_per_block"
	// InnodbLogCompressedPages is the name for 'innodb_log_compressed_pages' system variable.
	InnodbLogCompressedPages = "innodb_log_compressed_pages"
	// InnodbPrintAllDeadlocks is the name for 'innodb_print_all_deadlocks' system variable.
	InnodbPrintAllDeadlocks = "innodb_print_all_deadlocks"
	// InnodbStrictMode is the name for 'innodb_strict_mode' system variable.
	InnodbStrictMode = "innodb_strict_mode"
	// InnodbCmpPerIndexEnabled is the name for 'innodb_cmp_per_index_enabled' system variable.
	InnodbCmpPerIndexEnabled = "innodb_cmp_per_index_enabled"
	// InnodbBufferPoolDumpAtShutdown is the name for 'innodb_buffer_pool_dump_at_shutdown' system variable.
	InnodbBufferPoolDumpAtShutdown = "innodb_buffer_pool_dump_at_shutdown"
	// InnodbAdaptiveHashIndex is the name for 'innodb_adaptive_hash_index' system variable.
	InnodbAdaptiveHashIndex = "innodb_adaptive_hash_index"
	// InnodbFtEnableStopword is the name for 'innodb_ft_enable_stopword' system variable.
	InnodbFtEnableStopword = "innodb_ft_enable_stopword"
	// InnodbSupportXA is the name for 'innodb_support_xa' system variable.
	InnodbSupportXA = "innodb_support_xa"
	// InnodbOptimizeFullTextOnly is the name for 'innodb_optimize_fulltext_only' system variable.
	InnodbOptimizeFullTextOnly = "innodb_optimize_fulltext_only"
	// InnodbStatusOutputLocks is the name for 'innodb_status_output_locks' system variable.
	InnodbStatusOutputLocks = "innodb_status_output_locks"
	// InnodbBufferPoolDumpNow is the name for 'innodb_buffer_pool_dump_now' system variable.
	InnodbBufferPoolDumpNow = "innodb_buffer_pool_dump_now"
	// InnodbBufferPoolLoadNow is the name for 'innodb_buffer_pool_load_now' system variable.
	InnodbBufferPoolLoadNow = "innodb_buffer_pool_load_now"
	// InnodbStatsOnMetadata is the name for 'innodb_stats_on_metadata' system variable.
	InnodbStatsOnMetadata = "innodb_stats_on_metadata"
	// InnodbDisableSortFileCache is the name for 'innodb_disable_sort_file_cache' system variable.
	InnodbDisableSortFileCache = "innodb_disable_sort_file_cache"
	// InnodbStatsAutoRecalc is the name for 'innodb_stats_auto_recalc' system variable.
	InnodbStatsAutoRecalc = "innodb_stats_auto_recalc"
	// InnodbBufferPoolLoadAbort is the name for 'innodb_buffer_pool_load_abort' system variable.
	InnodbBufferPoolLoadAbort = "innodb_buffer_pool_load_abort"
	// InnodbStatsPersistent is the name for 'innodb_stats_persistent' system variable.
	InnodbStatsPersistent = "innodb_stats_persistent"
	// InnodbRandomReadAhead is the name for 'innodb_random_read_ahead' system variable.
	InnodbRandomReadAhead = "innodb_random_read_ahead"
	// InnodbAdaptiveFlushing is the name for 'innodb_adaptive_flushing' system variable.
	InnodbAdaptiveFlushing = "innodb_adaptive_flushing"
	// InnodbBlockLocks is the name for 'innodb_block_locks' system variable.
	InnodbBlockLocks = "innodb_block_locks"
	// InnodbStatusOutput is the name for 'innodb_status_output' system variable.
	InnodbStatusOutput = "innodb_status_output"

	// NetBufferLength is the name for 'net_buffer_length' system variable.
	NetBufferLength = "net_buffer_length"
	// QueryCacheSize is the name of 'query_cache_size' system variable.
	QueryCacheSize = "query_cache_size"
	// TxReadOnly is the name of 'tx_read_only' system variable.
	TxReadOnly = "tx_read_only"
	// TransactionReadOnly is the name of 'transaction_read_only' system variable.
	TransactionReadOnly = "transaction_read_only"
	// CharacterSetServer is the name of 'character_set_server' system variable.
	CharacterSetServer = "character_set_server"
	// AutoIncrementIncrement is the name of 'auto_increment_increment' system variable.
	AutoIncrementIncrement = "auto_increment_increment"
	// AutoIncrementOffset is the name of 'auto_increment_offset' system variable.
	AutoIncrementOffset = "auto_increment_offset"
	// InitConnect is the name of 'init_connect' system variable.
	InitConnect = "init_connect"
	// DefCauslationServer is the name of 'defCauslation_server' variable.
	DefCauslationServer = "defCauslation_server"
	// NetWriteTimeout is the name of 'net_write_timeout' variable.
	NetWriteTimeout = "net_write_timeout"
	// ThreadPoolSize is the name of 'thread_pool_size' variable.
	ThreadPoolSize = "thread_pool_size"
	// WindowingUseHighPrecision is the name of 'windowing_use_high_precision' system variable.
	WindowingUseHighPrecision = "windowing_use_high_precision"
)

// GlobalVarAccessor is the interface for accessing global scope system and status variables.
type GlobalVarAccessor interface {
	// GetAllSysVars gets all the global system variable values.
	GetAllSysVars() (map[string]string, error)
	// GetGlobalSysVar gets the global system variable value for name.
	GetGlobalSysVar(name string) (string, error)
	// SetGlobalSysVar sets the global system variable name to value.
	SetGlobalSysVar(name string, value string) error
}
