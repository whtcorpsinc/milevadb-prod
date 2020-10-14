// Copyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package stochastik

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"go.uber.org/zap"
)

const (
	// CreateUserTable is the ALLEGROALLEGROSQL statement creates User block in system EDB.
	CreateUserTable = `CREATE TABLE if not exists allegrosql.user (
		Host				CHAR(64),
		User				CHAR(32),
		authentication_string	TEXT,
		Select_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Insert_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		UFIDelate_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Delete_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Process_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Grant_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		References_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_db_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Super_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_tmp_block_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_blocks_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Index_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_user_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Event_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_role_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Drop_role_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Account_locked			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Shutdown_priv			ENUM('N','Y') NOT NULL DEFAULT 'N',
		Reload_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		FILE_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Config_priv				ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_Tablespace_Priv    ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, User));`
	// CreateGlobalPrivTable is the ALLEGROALLEGROSQL statement creates Global scope privilege block in system EDB.
	CreateGlobalPrivTable = "CREATE TABLE if not exists allegrosql.global_priv (" +
		"Host char(60) NOT NULL DEFAULT ''," +
		"User char(80) NOT NULL DEFAULT ''," +
		"Priv longtext NOT NULL DEFAULT ''," +
		"PRIMARY KEY (Host, User)" +
		")"
	// CreateDBPrivTable is the ALLEGROALLEGROSQL statement creates EDB scope privilege block in system EDB.
	CreateDBPrivTable = `CREATE TABLE if not exists allegrosql.EDB (
		Host			CHAR(60),
		EDB			CHAR(64),
		User			CHAR(32),
		Select_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Insert_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		UFIDelate_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Delete_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Drop_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Grant_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		References_priv 	ENUM('N','Y') Not Null DEFAULT 'N',
		Index_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Alter_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Create_tmp_block_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Lock_blocks_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_view_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Show_view_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Create_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Alter_routine_priv	ENUM('N','Y') NOT NULL DEFAULT 'N',
		Execute_priv		ENUM('N','Y') Not Null DEFAULT 'N',
		Event_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		Trigger_priv		ENUM('N','Y') NOT NULL DEFAULT 'N',
		PRIMARY KEY (Host, EDB, User));`
	// CreateTablePrivTable is the ALLEGROALLEGROSQL statement creates block scope privilege block in system EDB.
	CreateTablePrivTable = `CREATE TABLE if not exists allegrosql.blocks_priv (
		Host		CHAR(60),
		EDB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		Grantor		CHAR(77),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		Table_priv	SET('Select','Insert','UFIDelate','Delete','Create','Drop','Grant','Index','Alter','Create View','Show View','Trigger','References'),
		DeferredCauset_priv	SET('Select','Insert','UFIDelate'),
		PRIMARY KEY (Host, EDB, User, Table_name));`
	// CreateDeferredCausetPrivTable is the ALLEGROALLEGROSQL statement creates column scope privilege block in system EDB.
	CreateDeferredCausetPrivTable = `CREATE TABLE if not exists allegrosql.columns_priv(
		Host		CHAR(60),
		EDB		CHAR(64),
		User		CHAR(32),
		Table_name	CHAR(64),
		DeferredCauset_name	CHAR(64),
		Timestamp	Timestamp DEFAULT CURRENT_TIMESTAMP,
		DeferredCauset_priv	SET('Select','Insert','UFIDelate'),
		PRIMARY KEY (Host, EDB, User, Table_name, DeferredCauset_name));`
	// CreateGlobalVariablesTable is the ALLEGROALLEGROSQL statement creates global variable block in system EDB.
	// TODO: MyALLEGROSQL puts GLOBAL_VARIABLES block in INFORMATION_SCHEMA EDB.
	// INFORMATION_SCHEMA is a virtual EDB in MilevaDB. So we put this block in system EDB.
	// Maybe we will put it back to INFORMATION_SCHEMA.
	CreateGlobalVariablesTable = `CREATE TABLE if not exists allegrosql.GLOBAL_VARIABLES(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null);`
	// CreateMilevaDBTable is the ALLEGROALLEGROSQL statement creates a block in system EDB.
	// This block is a key-value struct contains some information used by MilevaDB.
	// Currently we only put bootstrapped in it which indicates if the system is already bootstrapped.
	CreateMilevaDBTable = `CREATE TABLE if not exists allegrosql.milevadb(
		VARIABLE_NAME  VARCHAR(64) Not Null PRIMARY KEY,
		VARIABLE_VALUE VARCHAR(1024) DEFAULT Null,
		COMMENT VARCHAR(1024));`

	// CreateHelpTopic is the ALLEGROALLEGROSQL statement creates help_topic block in system EDB.
	// See: https://dev.allegrosql.com/doc/refman/5.5/en/system-database.html#system-database-help-blocks
	CreateHelpTopic = `CREATE TABLE if not exists allegrosql.help_topic (
  		help_topic_id int(10) unsigned NOT NULL,
  		name char(64) NOT NULL,
  		help_category_id smallint(5) unsigned NOT NULL,
  		description text NOT NULL,
  		example text NOT NULL,
  		url text NOT NULL,
  		PRIMARY KEY (help_topic_id),
  		UNIQUE KEY name (name)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='help topics';`

	// CreateStatsMetaTable stores the meta of block statistics.
	CreateStatsMetaTable = `CREATE TABLE if not exists allegrosql.stats_meta (
		version bigint(64) unsigned NOT NULL,
		block_id bigint(64) NOT NULL,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		count bigint(64) unsigned NOT NULL DEFAULT 0,
		index idx_ver(version),
		unique index tbl(block_id)
	);`

	// CreateStatsDefCaussTable stores the statistics of block columns.
	CreateStatsDefCaussTable = `CREATE TABLE if not exists allegrosql.stats_histograms (
		block_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		distinct_count bigint(64) NOT NULL,
		null_count bigint(64) NOT NULL DEFAULT 0,
		tot_col_size bigint(64) NOT NULL DEFAULT 0,
		modify_count bigint(64) NOT NULL DEFAULT 0,
		version bigint(64) unsigned NOT NULL DEFAULT 0,
		cm_sketch blob,
		stats_ver bigint(64) NOT NULL DEFAULT 0,
		flag bigint(64) NOT NULL DEFAULT 0,
		correlation double NOT NULL DEFAULT 0,
		last_analyze_pos blob DEFAULT NULL,
		unique index tbl(block_id, is_index, hist_id)
	);`

	// CreateStatsBucketsTable stores the histogram info for every block columns.
	CreateStatsBucketsTable = `CREATE TABLE if not exists allegrosql.stats_buckets (
		block_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		bucket_id bigint(64) NOT NULL,
		count bigint(64) NOT NULL,
		repeats bigint(64) NOT NULL,
		upper_bound blob NOT NULL,
		lower_bound blob ,
		unique index tbl(block_id, is_index, hist_id, bucket_id)
	);`

	// CreateGCDeleteRangeTable stores schemas which can be deleted by DeleteRange.
	CreateGCDeleteRangeTable = `CREATE TABLE IF NOT EXISTS allegrosql.gc_delete_range (
		job_id BIGINT NOT NULL COMMENT "the DBS job ID",
		element_id BIGINT NOT NULL COMMENT "the schemaReplicant element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_index (job_id, element_id)
	);`

	// CreateGCDeleteRangeDoneTable stores schemas which are already deleted by DeleteRange.
	CreateGCDeleteRangeDoneTable = `CREATE TABLE IF NOT EXISTS allegrosql.gc_delete_range_done (
		job_id BIGINT NOT NULL COMMENT "the DBS job ID",
		element_id BIGINT NOT NULL COMMENT "the schemaReplicant element ID",
		start_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		end_key VARCHAR(255) NOT NULL COMMENT "encoded in hex",
		ts BIGINT NOT NULL COMMENT "timestamp in uint64",
		UNIQUE KEY delete_range_done_index (job_id, element_id)
	);`

	// CreateStatsFeedbackTable stores the feedback info which is used to uFIDelate stats.
	CreateStatsFeedbackTable = `CREATE TABLE IF NOT EXISTS allegrosql.stats_feedback (
		block_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		feedback blob NOT NULL,
		index hist(block_id, is_index, hist_id)
	);`

	// CreateBindInfoTable stores the allegrosql bind info which is used to uFIDelate globalBindCache.
	CreateBindInfoTable = `CREATE TABLE IF NOT EXISTS allegrosql.bind_info (
		original_sql text NOT NULL  ,
      	bind_sql text NOT NULL ,
      	default_db text  NOT NULL,
		status text NOT NULL,
		create_time timestamp(3) NOT NULL,
		uFIDelate_time timestamp(3) NOT NULL,
		charset text NOT NULL,
		collation text NOT NULL,
		source varchar(10) NOT NULL default 'unknown',
		INDEX sql_index(original_sql(1024),default_db(1024)) COMMENT "accelerate the speed when add global binding query",
		INDEX time_index(uFIDelate_time) COMMENT "accelerate the speed when querying with last uFIDelate time"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`

	// CreateRoleEdgesTable stores the role and user relationship information.
	CreateRoleEdgesTable = `CREATE TABLE IF NOT EXISTS allegrosql.role_edges (
		FROM_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		FROM_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		TO_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		WITH_ADMIN_OPTION enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
		PRIMARY KEY (FROM_HOST,FROM_USER,TO_HOST,TO_USER)
	);`

	// CreateDefaultRolesTable stores the active roles for a user.
	CreateDefaultRolesTable = `CREATE TABLE IF NOT EXISTS allegrosql.default_roles (
		HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
		USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		DEFAULT_ROLE_HOST char(60) COLLATE utf8_bin NOT NULL DEFAULT '%',
		DEFAULT_ROLE_USER char(32) COLLATE utf8_bin NOT NULL DEFAULT '',
		PRIMARY KEY (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER)
	)`

	// CreateStatsTopNTable stores topn data of a cmsketch with top n.
	CreateStatsTopNTable = `CREATE TABLE if not exists allegrosql.stats_top_n (
		block_id bigint(64) NOT NULL,
		is_index tinyint(2) NOT NULL,
		hist_id bigint(64) NOT NULL,
		value longblob,
		count bigint(64) UNSIGNED NOT NULL,
		index tbl(block_id, is_index, hist_id)
	);`

	// CreateExprPushdownBlacklist stores the expressions which are not allowed to be pushed down.
	CreateExprPushdownBlacklist = `CREATE TABLE IF NOT EXISTS allegrosql.expr_pushdown_blacklist (
		name char(100) NOT NULL,
		store_type char(100) NOT NULL DEFAULT 'einsteindb,tiflash,milevadb',
		reason varchar(200)
	);`

	// CreateOptMemruleBlacklist stores the list of disabled optimizing operations.
	CreateOptMemruleBlacklist = `CREATE TABLE IF NOT EXISTS allegrosql.opt_rule_blacklist (
		name char(100) NOT NULL
	);`

	// CreateStatsExtended stores the registered extended statistics.
	CreateStatsExtended = `CREATE TABLE IF NOT EXISTS allegrosql.stats_extended (
		stats_name varchar(32) NOT NULL,
		EDB varchar(32) NOT NULL,
		type tinyint(4) NOT NULL,
		block_id bigint(64) NOT NULL,
		column_ids varchar(32) NOT NULL,
		scalar_stats double DEFAULT NULL,
		blob_stats blob DEFAULT NULL,
		version bigint(64) unsigned NOT NULL,
		status tinyint(4) NOT NULL,
		PRIMARY KEY(stats_name, EDB),
		KEY idx_1 (block_id, status, version),
		KEY idx_2 (status, version)
	);`

	// CreateSchemaIndexUsageTable stores the index usage information.
	CreateSchemaIndexUsageTable = `CREATE TABLE IF NOT EXISTS allegrosql.schema_index_usage (
		TABLE_SCHEMA varchar(64),
		TABLE_NAME varchar(64),
		INDEX_NAME varchar(64),
		QUERY_COUNT bigint(64),
		ROWS_SELECTED bigint(64),
		LAST_USED_AT timestamp,
		LAST_UFIDelATED_AT timestamp,
		PRIMARY KEY(TABLE_SCHEMA, TABLE_NAME, INDEX_NAME)
	);`
)

// bootstrap initiates system EDB for a causetstore.
func bootstrap(s Stochastik) {
	startTime := time.Now()
	dom := petri.GetPetri(s)
	for {
		b, err := checkBootstrapped(s)
		if err != nil {
			logutil.BgLogger().Fatal("check bootstrap error",
				zap.Error(err))
		}
		// For rolling upgrade, we can't do upgrade only in the owner.
		if b {
			upgrade(s)
			logutil.BgLogger().Info("upgrade successful in bootstrap",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		// To reduce conflict when multiple MilevaDB-server start at the same time.
		// Actually only one server need to do the bootstrap. So we chose DBS owner to do this.
		if dom.DBS().OwnerManager().IsOwner() {
			doDBSWorks(s)
			doDMLWorks(s)
			logutil.BgLogger().Info("bootstrap successful",
				zap.Duration("take time", time.Since(startTime)))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

const (
	// varTrue is the true value in allegrosql.MilevaDB block for boolean columns.
	varTrue = "True"
	// varFalse is the false value in allegrosql.MilevaDB block for boolean columns.
	varFalse = "False"
	// The variable name in allegrosql.MilevaDB block.
	// It is used for checking if the causetstore is bootstrapped by any MilevaDB server.
	// If the value is `True`, the causetstore is already bootstrapped by a MilevaDB server.
	bootstrappedVar = "bootstrapped"
	// The variable name in allegrosql.MilevaDB block.
	// It is used for getting the version of the MilevaDB server which bootstrapped the causetstore.
	milevadbServerVersionVar = "milevadb_server_version"
	// The variable name in allegrosql.milevadb block and it will be used when we want to know
	// system timezone.
	milevadbSystemTZ = "system_tz"
	// The variable name in allegrosql.milevadb block and it will indicate if the new collations are enabled in the MilevaDB cluster.
	milevadbNewDefCauslationEnabled = "new_collation_enabled"
	// Const for MilevaDB server version 2.
	version2  = 2
	version3  = 3
	version4  = 4
	version5  = 5
	version6  = 6
	version7  = 7
	version8  = 8
	version9  = 9
	version10 = 10
	version11 = 11
	version12 = 12
	version13 = 13
	version14 = 14
	version15 = 15
	version16 = 16
	version17 = 17
	version18 = 18
	version19 = 19
	version20 = 20
	version21 = 21
	version22 = 22
	version23 = 23
	version24 = 24
	version25 = 25
	version26 = 26
	version27 = 27
	version28 = 28
	// version29 is not needed.
	version30 = 30
	version31 = 31
	version32 = 32
	version33 = 33
	version34 = 34
	version35 = 35
	version36 = 36
	version37 = 37
	version38 = 38
	version39 = 39
	// version40 is the version that introduce new collation in MilevaDB,
	// see https://github.com/whtcorpsinc/milevadb/pull/14574 for more details.
	version40 = 40
	version41 = 41
	// version42 add storeType and reason column in expr_pushdown_blacklist
	version42 = 42
	// version43 uFIDelates global variables related to statement summary.
	version43 = 43
	// version44 delete milevadb_isolation_read_engines from allegrosql.global_variables to avoid unexpected behavior after upgrade.
	version44 = 44
	// version45 introduces CONFIG_PRIV for SET CONFIG statements.
	version45 = 45
	// version46 fix a bug in v3.1.1.
	version46 = 46
	// version47 add Source to bindings to indicate the way binding created.
	version47 = 47
	// version48 reset all deprecated concurrency related system-variables if they were all default value.
	version48 = 48
	// version49 introduces allegrosql.stats_extended block.
	version49 = 49
	// version50 add allegrosql.schema_index_usage block.
	version50 = 50
	// version51 introduces CreateTablespacePriv to allegrosql.user.
	version51 = 51
)

var (
	bootstrapVersion = []func(Stochastik, int64){
		upgradeToVer2,
		upgradeToVer3,
		upgradeToVer4,
		upgradeToVer5,
		upgradeToVer6,
		upgradeToVer7,
		upgradeToVer8,
		upgradeToVer9,
		upgradeToVer10,
		upgradeToVer11,
		upgradeToVer12,
		upgradeToVer13,
		upgradeToVer14,
		upgradeToVer15,
		upgradeToVer16,
		upgradeToVer17,
		upgradeToVer18,
		upgradeToVer19,
		upgradeToVer20,
		upgradeToVer21,
		upgradeToVer22,
		upgradeToVer23,
		upgradeToVer24,
		upgradeToVer25,
		upgradeToVer26,
		upgradeToVer27,
		upgradeToVer28,
		upgradeToVer29,
		upgradeToVer30,
		upgradeToVer31,
		upgradeToVer32,
		upgradeToVer33,
		upgradeToVer34,
		upgradeToVer35,
		upgradeToVer36,
		upgradeToVer37,
		upgradeToVer38,
		upgradeToVer39,
		upgradeToVer40,
		upgradeToVer41,
		upgradeToVer42,
		upgradeToVer43,
		upgradeToVer44,
		upgradeToVer45,
		upgradeToVer46,
		upgradeToVer47,
		upgradeToVer48,
		upgradeToVer49,
		upgradeToVer50,
		upgradeToVer51,
	}
)

func checkBootstrapped(s Stochastik) (bool, error) {
	//  Check if system EDB exists.
	_, err := s.Execute(context.Background(), fmt.Sprintf("USE %s;", allegrosql.SystemDB))
	if err != nil && schemareplicant.ErrDatabaseNotExists.NotEqual(err) {
		logutil.BgLogger().Fatal("check bootstrap error",
			zap.Error(err))
	}
	// Check bootstrapped variable value in MilevaDB block.
	sVal, _, err := getMilevaDBVar(s, bootstrappedVar)
	if err != nil {
		if schemareplicant.ErrTableNotExists.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	isBootstrapped := sVal == varTrue
	if isBootstrapped {
		// Make sure that doesn't affect the following operations.
		if err = s.CommitTxn(context.Background()); err != nil {
			return false, errors.Trace(err)
		}
	}
	return isBootstrapped, nil
}

// getMilevaDBVar gets variable value from allegrosql.milevadb block.
// Those variables are used by MilevaDB server.
func getMilevaDBVar(s Stochastik, name string) (sVal string, isNull bool, e error) {
	allegrosql := fmt.Sprintf(`SELECT HIGH_PRIORITY VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s"`,
		allegrosql.SystemDB, allegrosql.MilevaDBTable, name)
	ctx := context.Background()
	rs, err := s.Execute(ctx, allegrosql)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if len(rs) != 1 {
		return "", true, errors.New("Wrong number of Recordset")
	}
	r := rs[0]
	defer terror.Call(r.Close)
	req := r.NewChunk()
	err = r.Next(ctx, req)
	if err != nil || req.NumRows() == 0 {
		return "", true, errors.Trace(err)
	}
	event := req.GetRow(0)
	if event.IsNull(0) {
		return "", true, nil
	}
	return event.GetString(0), false, nil
}

// upgrade function  will do some upgrade works, when the system is bootstrapped by low version MilevaDB server
// For example, add new system variables into allegrosql.global_variables block.
func upgrade(s Stochastik) {
	ver, err := getBootstrapVersion(s)
	terror.MustNil(err)
	if ver >= currentBootstrapVersion {
		// It is already bootstrapped/upgraded by a higher version MilevaDB server.
		return
	}
	// Do upgrade works then uFIDelate bootstrap version.
	for _, upgrade := range bootstrapVersion {
		upgrade(s, ver)
	}

	uFIDelateBootstrapVer(s)
	_, err = s.Execute(context.Background(), "COMMIT")

	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("uFIDelate bootstrap ver failed",
			zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if MilevaDB is already upgraded.
		v, err1 := getBootstrapVersion(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("upgrade failed", zap.Error(err1))
		}
		if v >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by a higher version MilevaDB server.
			return
		}
		logutil.BgLogger().Fatal("[Upgrade] upgrade failed",
			zap.Int64("from", ver),
			zap.Int("to", currentBootstrapVersion),
			zap.Error(err))
	}
}

// upgradeToVer2 uFIDelates to version 2.
func upgradeToVer2(s Stochastik, ver int64) {
	if ver >= version2 {
		return
	}
	// Version 2 add two system variable for DistALLEGROSQL concurrency controlling.
	// Insert allegrosql related system variable.
	distALLEGROSQLVars := []string{variable.MilevaDBDistALLEGROSQLScanConcurrency}
	values := make([]string, 0, len(distALLEGROSQLVars))
	for _, v := range distALLEGROSQLVars {
		value := fmt.Sprintf(`("%s", "%s")`, v, variable.SysVars[v].Value)
		values = append(values, value)
	}
	allegrosql := fmt.Sprintf("INSERT HIGH_PRIORITY IGNORE INTO %s.%s VALUES %s;", allegrosql.SystemDB, allegrosql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, allegrosql)
}

// upgradeToVer3 uFIDelates to version 3.
func upgradeToVer3(s Stochastik, ver int64) {
	if ver >= version3 {
		return
	}
	// Version 3 fix tx_read_only variable value.
	allegrosql := fmt.Sprintf("UFIDelATE HIGH_PRIORITY %s.%s set variable_value = '0' where variable_name = 'tx_read_only';",
		allegrosql.SystemDB, allegrosql.GlobalVariablesTable)
	mustExecute(s, allegrosql)
}

// upgradeToVer4 uFIDelates to version 4.
func upgradeToVer4(s Stochastik, ver int64) {
	if ver >= version4 {
		return
	}
	allegrosql := CreateStatsMetaTable
	mustExecute(s, allegrosql)
}

func upgradeToVer5(s Stochastik, ver int64) {
	if ver >= version5 {
		return
	}
	mustExecute(s, CreateStatsDefCaussTable)
	mustExecute(s, CreateStatsBucketsTable)
}

func upgradeToVer6(s Stochastik, ver int64) {
	if ver >= version6 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Super_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", schemareplicant.ErrDeferredCausetExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as MilevaDB doesn't check them in older versions.
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Super_priv='Y'")
}

func upgradeToVer7(s Stochastik, ver int64) {
	if ver >= version7 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Process_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", schemareplicant.ErrDeferredCausetExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as MilevaDB doesn't check them in older versions.
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Process_priv='Y'")
}

func upgradeToVer8(s Stochastik, ver int64) {
	if ver >= version8 {
		return
	}
	// This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
	if _, err := s.Execute(context.Background(), "SELECT HIGH_PRIORITY `Process_priv` from allegrosql.user limit 0"); err == nil {
		return
	}
	upgradeToVer7(s, ver)
}

func upgradeToVer9(s Stochastik, ver int64) {
	if ver >= version9 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", schemareplicant.ErrDeferredCausetExists)
	// For reasons of compatibility, set the non-exists privilege column value to 'Y', as MilevaDB doesn't check them in older versions.
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Trigger_priv='Y'")
}

func doReentrantDBS(s Stochastik, allegrosql string, ignorableErrs ...error) {
	_, err := s.Execute(context.Background(), allegrosql)
	for _, ignorableErr := range ignorableErrs {
		if terror.ErrorEqual(err, ignorableErr) {
			return
		}
	}
	if err != nil {
		logutil.BgLogger().Fatal("doReentrantDBS error", zap.Error(err))
	}
}

func upgradeToVer10(s Stochastik, ver int64) {
	if ver >= version10 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", schemareplicant.ErrDeferredCausetNotExists, schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_buckets ADD COLUMN `lower_bound` BLOB", schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms ADD COLUMN `null_count` bigint(64) NOT NULL DEFAULT 0", schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms DROP COLUMN distinct_ratio", dbs.ErrCantDropFieldOrKey)
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms DROP COLUMN use_count_to_estimate", dbs.ErrCantDropFieldOrKey)
}

func upgradeToVer11(s Stochastik, ver int64) {
	if ver >= version11 {
		return
	}
	_, err := s.Execute(context.Background(), "ALTER TABLE allegrosql.user ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`")
	if err != nil {
		if terror.ErrorEqual(err, schemareplicant.ErrDeferredCausetExists) {
			return
		}
		logutil.BgLogger().Fatal("upgradeToVer11 error", zap.Error(err))
	}
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET References_priv='Y'")
}

func upgradeToVer12(s Stochastik, ver int64) {
	if ver >= version12 {
		return
	}
	ctx := context.Background()
	_, err := s.Execute(ctx, "BEGIN")
	terror.MustNil(err)
	allegrosql := "SELECT HIGH_PRIORITY user, host, password FROM allegrosql.user WHERE password != ''"
	rs, err := s.Execute(ctx, allegrosql)
	if terror.ErrorEqual(err, core.ErrUnknownDeferredCauset) {
		allegrosql := "SELECT HIGH_PRIORITY user, host, authentication_string FROM allegrosql.user WHERE authentication_string != ''"
		rs, err = s.Execute(ctx, allegrosql)
	}
	terror.MustNil(err)
	r := rs[0]
	sqls := make([]string, 0, 1)
	defer terror.Call(r.Close)
	req := r.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	err = r.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		for event := it.Begin(); event != it.End(); event = it.Next() {
			user := event.GetString(0)
			host := event.GetString(1)
			pass := event.GetString(2)
			var newPass string
			newPass, err = oldPasswordUpgrade(pass)
			terror.MustNil(err)
			uFIDelateALLEGROSQL := fmt.Sprintf(`UFIDelATE HIGH_PRIORITY allegrosql.user set password = "%s" where user="%s" and host="%s"`, newPass, user, host)
			sqls = append(sqls, uFIDelateALLEGROSQL)
		}
		err = r.Next(ctx, req)
	}
	terror.MustNil(err)

	for _, allegrosql := range sqls {
		mustExecute(s, allegrosql)
	}

	allegrosql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "MilevaDB bootstrap version.") ON DUPLICATE KEY UFIDelATE VARIABLE_VALUE="%d"`,
		allegrosql.SystemDB, allegrosql.MilevaDBTable, milevadbServerVersionVar, version12, version12)
	mustExecute(s, allegrosql)

	mustExecute(s, "COMMIT")
}

func upgradeToVer13(s Stochastik, ver int64) {
	if ver >= version13 {
		return
	}
	sqls := []string{
		"ALTER TABLE allegrosql.user ADD COLUMN `Create_tmp_block_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
		"ALTER TABLE allegrosql.user ADD COLUMN `Lock_blocks_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_block_priv`",
		"ALTER TABLE allegrosql.user ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE allegrosql.user ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE allegrosql.user ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE allegrosql.user ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE allegrosql.user ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
	}
	ctx := context.Background()
	for _, allegrosql := range sqls {
		_, err := s.Execute(ctx, allegrosql)
		if err != nil {
			if terror.ErrorEqual(err, schemareplicant.ErrDeferredCausetExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer13 error", zap.Error(err))
		}
	}
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Create_tmp_block_priv='Y',Lock_blocks_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer14(s Stochastik, ver int64) {
	if ver >= version14 {
		return
	}
	sqls := []string{
		"ALTER TABLE allegrosql.EDB ADD COLUMN `References_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Create_tmp_block_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Alter_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Lock_blocks_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_block_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Create_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Lock_blocks_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Show_view_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Event_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
		"ALTER TABLE allegrosql.EDB ADD COLUMN `Trigger_priv` enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Event_priv`",
	}
	ctx := context.Background()
	for _, allegrosql := range sqls {
		_, err := s.Execute(ctx, allegrosql)
		if err != nil {
			if terror.ErrorEqual(err, schemareplicant.ErrDeferredCausetExists) {
				continue
			}
			logutil.BgLogger().Fatal("upgradeToVer14 error", zap.Error(err))
		}
	}
}

func upgradeToVer15(s Stochastik, ver int64) {
	if ver >= version15 {
		return
	}
	var err error
	_, err = s.Execute(context.Background(), CreateGCDeleteRangeTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer15 error", zap.Error(err))
	}
}

func upgradeToVer16(s Stochastik, ver int64) {
	if ver >= version16 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms ADD COLUMN `cm_sketch` blob", schemareplicant.ErrDeferredCausetExists)
}

func upgradeToVer17(s Stochastik, ver int64) {
	if ver >= version17 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user MODIFY User CHAR(32)")
}

func upgradeToVer18(s Stochastik, ver int64) {
	if ver >= version18 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms ADD COLUMN `tot_col_size` bigint(64) NOT NULL DEFAULT 0", schemareplicant.ErrDeferredCausetExists)
}

func upgradeToVer19(s Stochastik, ver int64) {
	if ver >= version19 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.EDB MODIFY User CHAR(32)")
	doReentrantDBS(s, "ALTER TABLE allegrosql.blocks_priv MODIFY User CHAR(32)")
	doReentrantDBS(s, "ALTER TABLE allegrosql.columns_priv MODIFY User CHAR(32)")
}

func upgradeToVer20(s Stochastik, ver int64) {
	if ver >= version20 {
		return
	}
	doReentrantDBS(s, CreateStatsFeedbackTable)
}

func upgradeToVer21(s Stochastik, ver int64) {
	if ver >= version21 {
		return
	}
	mustExecute(s, CreateGCDeleteRangeDoneTable)

	doReentrantDBS(s, "ALTER TABLE allegrosql.gc_delete_range DROP INDEX job_id", dbs.ErrCantDropFieldOrKey)
	doReentrantDBS(s, "ALTER TABLE allegrosql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", dbs.ErrDupKeyName)
	doReentrantDBS(s, "ALTER TABLE allegrosql.gc_delete_range DROP INDEX element_id", dbs.ErrCantDropFieldOrKey)
}

func upgradeToVer22(s Stochastik, ver int64) {
	if ver >= version22 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms ADD COLUMN `stats_ver` bigint(64) NOT NULL DEFAULT 0", schemareplicant.ErrDeferredCausetExists)
}

func upgradeToVer23(s Stochastik, ver int64) {
	if ver >= version23 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms ADD COLUMN `flag` bigint(64) NOT NULL DEFAULT 0", schemareplicant.ErrDeferredCausetExists)
}

// writeSystemTZ writes system timezone info into allegrosql.milevadb
func writeSystemTZ(s Stochastik) {
	allegrosql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%s", "MilevaDB Global System Timezone.") ON DUPLICATE KEY UFIDelATE VARIABLE_VALUE="%s"`,
		allegrosql.SystemDB, allegrosql.MilevaDBTable, milevadbSystemTZ, timeutil.InferSystemTZ(), timeutil.InferSystemTZ())
	mustExecute(s, allegrosql)
}

// upgradeToVer24 initializes `System` timezone according to docs/design/2020-09-10-adding-tz-env.md
func upgradeToVer24(s Stochastik, ver int64) {
	if ver >= version24 {
		return
	}
	writeSystemTZ(s)
}

// upgradeToVer25 uFIDelates milevadb_max_chunk_size to new low bound value 32 if previous value is small than 32.
func upgradeToVer25(s Stochastik, ver int64) {
	if ver >= version25 {
		return
	}
	allegrosql := fmt.Sprintf("UFIDelATE HIGH_PRIORITY %[1]s.%[2]s SET VARIABLE_VALUE = '%[4]d' WHERE VARIABLE_NAME = '%[3]s' AND VARIABLE_VALUE < %[4]d",
		allegrosql.SystemDB, allegrosql.GlobalVariablesTable, variable.MilevaDBMaxChunkSize, variable.DefInitChunkSize)
	mustExecute(s, allegrosql)
}

func upgradeToVer26(s Stochastik, ver int64) {
	if ver >= version26 {
		return
	}
	mustExecute(s, CreateRoleEdgesTable)
	mustExecute(s, CreateDefaultRolesTable)
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Create_role_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Drop_role_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Account_locked` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	// user with Create_user_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Create_role_priv='Y',Drop_role_priv='Y' WHERE Create_user_priv='Y'")
	// user with Create_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'")
}

func upgradeToVer27(s Stochastik, ver int64) {
	if ver >= version27 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms ADD COLUMN `correlation` double NOT NULL DEFAULT 0", schemareplicant.ErrDeferredCausetExists)
}

func upgradeToVer28(s Stochastik, ver int64) {
	if ver >= version28 {
		return
	}
	doReentrantDBS(s, CreateBindInfoTable)
}

func upgradeToVer29(s Stochastik, ver int64) {
	// upgradeToVer29 only need to be run when the current version is 28.
	if ver != version28 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.bind_info change create_time create_time timestamp(3)")
	doReentrantDBS(s, "ALTER TABLE allegrosql.bind_info change uFIDelate_time uFIDelate_time timestamp(3)")
	doReentrantDBS(s, "ALTER TABLE allegrosql.bind_info add index sql_index (original_sql(1024),default_db(1024))", dbs.ErrDupKeyName)
}

func upgradeToVer30(s Stochastik, ver int64) {
	if ver >= version30 {
		return
	}
	mustExecute(s, CreateStatsTopNTable)
}

func upgradeToVer31(s Stochastik, ver int64) {
	if ver >= version31 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.stats_histograms ADD COLUMN `last_analyze_pos` blob default null", schemareplicant.ErrDeferredCausetExists)
}

func upgradeToVer32(s Stochastik, ver int64) {
	if ver >= version32 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.blocks_priv MODIFY block_priv SET('Select','Insert','UFIDelate','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')")
}

func upgradeToVer33(s Stochastik, ver int64) {
	if ver >= version33 {
		return
	}
	doReentrantDBS(s, CreateExprPushdownBlacklist)
}

func upgradeToVer34(s Stochastik, ver int64) {
	if ver >= version34 {
		return
	}
	doReentrantDBS(s, CreateOptMemruleBlacklist)
}

func upgradeToVer35(s Stochastik, ver int64) {
	if ver >= version35 {
		return
	}
	allegrosql := fmt.Sprintf("UFIDelATE HIGH_PRIORITY %s.%s SET VARIABLE_NAME = '%s' WHERE VARIABLE_NAME = 'milevadb_back_off_weight'",
		allegrosql.SystemDB, allegrosql.GlobalVariablesTable, variable.MilevaDBBackOffWeight)
	mustExecute(s, allegrosql)
}

func upgradeToVer36(s Stochastik, ver int64) {
	if ver >= version36 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Shutdown_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	// A root user will have those privileges after upgrading.
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Shutdown_priv='Y' where Super_priv='Y'")
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Create_tmp_block_priv='Y',Lock_blocks_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'")
}

func upgradeToVer37(s Stochastik, ver int64) {
	if ver >= version37 {
		return
	}
	// when upgrade from old milevadb and no 'milevadb_enable_window_function' in GLOBAL_VARIABLES, init it with 0.
	allegrosql := fmt.Sprintf("INSERT IGNORE INTO  %s.%s (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('%s', '%d')",
		allegrosql.SystemDB, allegrosql.GlobalVariablesTable, variable.MilevaDBEnableWindowFunction, 0)
	mustExecute(s, allegrosql)
}

func upgradeToVer38(s Stochastik, ver int64) {
	if ver >= version38 {
		return
	}
	var err error
	_, err = s.Execute(context.Background(), CreateGlobalPrivTable)
	if err != nil {
		logutil.BgLogger().Fatal("upgradeToVer38 error", zap.Error(err))
	}
}

func upgradeToVer39(s Stochastik, ver int64) {
	if ver >= version39 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Reload_priv='Y' where Super_priv='Y'")
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET File_priv='Y' where Super_priv='Y'")
}

func writeNewDefCauslationParameter(s Stochastik, flag bool) {
	comment := "If the new collations are enabled. Do not edit it."
	b := varFalse
	if flag {
		b = varTrue
	}
	allegrosql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", '%s', '%s') ON DUPLICATE KEY UFIDelATE VARIABLE_VALUE='%s'`,
		allegrosql.SystemDB, allegrosql.MilevaDBTable, milevadbNewDefCauslationEnabled, b, comment, b)
	mustExecute(s, allegrosql)
}

func upgradeToVer40(s Stochastik, ver int64) {
	if ver >= version40 {
		return
	}
	// There is no way to enable new collation for an existing MilevaDB cluster.
	writeNewDefCauslationParameter(s, false)
}

func upgradeToVer41(s Stochastik, ver int64) {
	if ver >= version41 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user CHANGE `password` `authentication_string` TEXT", schemareplicant.ErrDeferredCausetExists, schemareplicant.ErrDeferredCausetNotExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `password` TEXT as (`authentication_string`)", schemareplicant.ErrDeferredCausetExists)
}

// writeDefaultExprPushDownBlacklist writes default expr pushdown blacklist into allegrosql.expr_pushdown_blacklist
func writeDefaultExprPushDownBlacklist(s Stochastik) {
	mustExecute(s, "INSERT HIGH_PRIORITY INTO allegrosql.expr_pushdown_blacklist VALUES"+
		"('date_add','tiflash', 'DST(daylight saving time) does not take effect in TiFlash date_add'),"+
		"('cast','tiflash', 'Behavior of some corner cases(overflow, truncate etc) is different in TiFlash and MilevaDB')")
}

func upgradeToVer42(s Stochastik, ver int64) {
	if ver >= version42 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.expr_pushdown_blacklist ADD COLUMN `store_type` char(100) NOT NULL DEFAULT 'einsteindb,tiflash,milevadb'", schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.expr_pushdown_blacklist ADD COLUMN `reason` varchar(200)", schemareplicant.ErrDeferredCausetExists)
	writeDefaultExprPushDownBlacklist(s)
}

// Convert statement summary global variables to non-empty values.
func writeStmtSummaryVars(s Stochastik) {
	allegrosql := fmt.Sprintf("UFIDelATE %s.%s SET variable_value='%%s' WHERE variable_name='%%s' AND variable_value=''", allegrosql.SystemDB, allegrosql.GlobalVariablesTable)
	stmtSummaryConfig := config.GetGlobalConfig().StmtSummary
	mustExecute(s, fmt.Sprintf(allegrosql, variable.BoolToIntStr(stmtSummaryConfig.Enable), variable.MilevaDBEnableStmtSummary))
	mustExecute(s, fmt.Sprintf(allegrosql, variable.BoolToIntStr(stmtSummaryConfig.EnableInternalQuery), variable.MilevaDBStmtSummaryInternalQuery))
	mustExecute(s, fmt.Sprintf(allegrosql, strconv.Itoa(stmtSummaryConfig.RefreshInterval), variable.MilevaDBStmtSummaryRefreshInterval))
	mustExecute(s, fmt.Sprintf(allegrosql, strconv.Itoa(stmtSummaryConfig.HistorySize), variable.MilevaDBStmtSummaryHistorySize))
	mustExecute(s, fmt.Sprintf(allegrosql, strconv.FormatUint(uint64(stmtSummaryConfig.MaxStmtCount), 10), variable.MilevaDBStmtSummaryMaxStmtCount))
	mustExecute(s, fmt.Sprintf(allegrosql, strconv.FormatUint(uint64(stmtSummaryConfig.MaxALLEGROSQLLength), 10), variable.MilevaDBStmtSummaryMaxALLEGROSQLLength))
}

func upgradeToVer43(s Stochastik, ver int64) {
	if ver >= version43 {
		return
	}
	writeStmtSummaryVars(s)
}

func upgradeToVer44(s Stochastik, ver int64) {
	if ver >= version44 {
		return
	}
	mustExecute(s, "DELETE FROM allegrosql.global_variables where variable_name = \"milevadb_isolation_read_engines\"")
}

func upgradeToVer45(s Stochastik, ver int64) {
	if ver >= version45 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Config_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Config_priv='Y' where Super_priv='Y'")
}

// In v3.1.1, we wrongly replace the context of upgradeToVer39 with upgradeToVer44. If we upgrade from v3.1.1 to a newer version,
// upgradeToVer39 will be missed. So we redo upgradeToVer39 here to make sure the upgrading from v3.1.1 succeed.
func upgradeToVer46(s Stochastik, ver int64) {
	if ver >= version46 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Reload_priv='Y' where Super_priv='Y'")
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET File_priv='Y' where Super_priv='Y'")
}

func upgradeToVer47(s Stochastik, ver int64) {
	if ver >= version47 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.bind_info ADD COLUMN `source` varchar(10) NOT NULL default 'unknown'", schemareplicant.ErrDeferredCausetExists)
}

func upgradeToVer48(s Stochastik, ver int64) {
	if ver >= version48 {
		return
	}
	defValues := map[string]string{
		variable.MilevaDBIndexLookupConcurrency:     "4",
		variable.MilevaDBIndexLookupJoinConcurrency: "4",
		variable.MilevaDBHashAggFinalConcurrency:    "4",
		variable.MilevaDBHashAggPartialConcurrency:  "4",
		variable.MilevaDBWindowConcurrency:          "4",
		variable.MilevaDBProjectionConcurrency:      "4",
		variable.MilevaDBHashJoinConcurrency:        "5",
	}
	names := make([]string, 0, len(defValues))
	for n := range defValues {
		names = append(names, n)
	}

	selectALLEGROSQL := "select HIGH_PRIORITY * from allegrosql.global_variables where variable_name in ('" + strings.Join(names, quoteCommaQuote) + "')"
	ctx := context.Background()
	rs, err := s.Execute(ctx, selectALLEGROSQL)
	terror.MustNil(err)
	r := rs[0]
	defer terror.Call(r.Close)
	req := r.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	err = r.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		for event := it.Begin(); event != it.End(); event = it.Next() {
			n := strings.ToLower(event.GetString(0))
			v := event.GetString(1)
			if defValue, ok := defValues[n]; !ok || defValue != v {
				return
			}
		}
		err = r.Next(ctx, req)
	}
	terror.MustNil(err)

	mustExecute(s, "BEGIN")
	v := strconv.Itoa(variable.ConcurrencyUnset)
	allegrosql := fmt.Sprintf("UFIDelATE %s.%s SET variable_value='%%s' WHERE variable_name='%%s'", allegrosql.SystemDB, allegrosql.GlobalVariablesTable)
	for _, name := range names {
		mustExecute(s, fmt.Sprintf(allegrosql, v, name))
	}
	mustExecute(s, "COMMIT")
}

func upgradeToVer49(s Stochastik, ver int64) {
	if ver >= version49 {
		return
	}
	doReentrantDBS(s, CreateStatsExtended)
}

func upgradeToVer50(s Stochastik, ver int64) {
	if ver >= version50 {
		return
	}
	doReentrantDBS(s, CreateSchemaIndexUsageTable)
}

func upgradeToVer51(s Stochastik, ver int64) {
	if ver >= version51 {
		return
	}
	doReentrantDBS(s, "ALTER TABLE allegrosql.user ADD COLUMN `Create_blockspace_priv` ENUM('N','Y') DEFAULT 'N'", schemareplicant.ErrDeferredCausetExists)
	mustExecute(s, "UFIDelATE HIGH_PRIORITY allegrosql.user SET Create_blockspace_priv='Y' where Super_priv='Y'")
}

// uFIDelateBootstrapVer uFIDelates bootstrap version variable in allegrosql.MilevaDB block.
func uFIDelateBootstrapVer(s Stochastik) {
	// UFIDelate bootstrap version.
	allegrosql := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES ("%s", "%d", "MilevaDB bootstrap version.") ON DUPLICATE KEY UFIDelATE VARIABLE_VALUE="%d"`,
		allegrosql.SystemDB, allegrosql.MilevaDBTable, milevadbServerVersionVar, currentBootstrapVersion, currentBootstrapVersion)
	mustExecute(s, allegrosql)
}

// getBootstrapVersion gets bootstrap version from allegrosql.milevadb block;
func getBootstrapVersion(s Stochastik) (int64, error) {
	sVal, isNull, err := getMilevaDBVar(s, milevadbServerVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	return strconv.ParseInt(sVal, 10, 64)
}

// doDBSWorks executes DBS statements in bootstrap stage.
func doDBSWorks(s Stochastik) {
	// Create a test database.
	mustExecute(s, "CREATE DATABASE IF NOT EXISTS test")
	// Create system EDB.
	mustExecute(s, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", allegrosql.SystemDB))
	// Create user block.
	mustExecute(s, CreateUserTable)
	// Create privilege blocks.
	mustExecute(s, CreateGlobalPrivTable)
	mustExecute(s, CreateDBPrivTable)
	mustExecute(s, CreateTablePrivTable)
	mustExecute(s, CreateDeferredCausetPrivTable)
	// Create global system variable block.
	mustExecute(s, CreateGlobalVariablesTable)
	// Create MilevaDB block.
	mustExecute(s, CreateMilevaDBTable)
	// Create help block.
	mustExecute(s, CreateHelpTopic)
	// Create stats_meta block.
	mustExecute(s, CreateStatsMetaTable)
	// Create stats_columns block.
	mustExecute(s, CreateStatsDefCaussTable)
	// Create stats_buckets block.
	mustExecute(s, CreateStatsBucketsTable)
	// Create gc_delete_range block.
	mustExecute(s, CreateGCDeleteRangeTable)
	// Create gc_delete_range_done block.
	mustExecute(s, CreateGCDeleteRangeDoneTable)
	// Create stats_feedback block.
	mustExecute(s, CreateStatsFeedbackTable)
	// Create role_edges block.
	mustExecute(s, CreateRoleEdgesTable)
	// Create default_roles block.
	mustExecute(s, CreateDefaultRolesTable)
	// Create bind_info block.
	mustExecute(s, CreateBindInfoTable)
	// Create stats_topn_store block.
	mustExecute(s, CreateStatsTopNTable)
	// Create expr_pushdown_blacklist block.
	mustExecute(s, CreateExprPushdownBlacklist)
	// Create opt_rule_blacklist block.
	mustExecute(s, CreateOptMemruleBlacklist)
	// Create stats_extended block.
	mustExecute(s, CreateStatsExtended)
	// Create schema_index_usage.
	mustExecute(s, CreateSchemaIndexUsageTable)
}

// doDMLWorks executes DML statements in bootstrap stage.
// All the statements run in a single transaction.
func doDMLWorks(s Stochastik) {
	mustExecute(s, "BEGIN")

	// Insert a default user with empty password.
	mustExecute(s, `INSERT HIGH_PRIORITY INTO allegrosql.user VALUES
		("%", "root", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y")`)

	// Init global system variables block.
	values := make([]string, 0, len(variable.SysVars))
	for k, v := range variable.SysVars {
		// Stochastik only variable should not be inserted.
		if v.Scope != variable.ScopeStochastik {
			vVal := v.Value
			if v.Name == variable.MilevaDBTxnMode && config.GetGlobalConfig().CausetStore == "einsteindb" {
				vVal = "pessimistic"
			}
			if v.Name == variable.MilevaDBRowFormatVersion {
				vVal = strconv.Itoa(variable.DefMilevaDBRowFormatV2)
			}
			if v.Name == variable.MilevaDBEnableClusteredIndex {
				vVal = "1"
			}
			if v.Name == variable.MilevaDBPartitionPruneMode {
				vVal = string(variable.StaticOnly)
				if flag.Lookup("test.v") != nil || flag.Lookup("check.v") != nil || config.CheckTableBeforeDrop {
					// enable Dynamic Prune by default in test case.
					vVal = string(variable.DynamicOnly)
				}
			}
			value := fmt.Sprintf(`("%s", "%s")`, strings.ToLower(k), vVal)
			values = append(values, value)
		}
	}
	allegrosql := fmt.Sprintf("INSERT HIGH_PRIORITY INTO %s.%s VALUES %s;", allegrosql.SystemDB, allegrosql.GlobalVariablesTable,
		strings.Join(values, ", "))
	mustExecute(s, allegrosql)

	allegrosql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%s", "Bootstrap flag. Do not delete.")
		ON DUPLICATE KEY UFIDelATE VARIABLE_VALUE="%s"`,
		allegrosql.SystemDB, allegrosql.MilevaDBTable, bootstrappedVar, varTrue, varTrue)
	mustExecute(s, allegrosql)

	allegrosql = fmt.Sprintf(`INSERT HIGH_PRIORITY INTO %s.%s VALUES("%s", "%d", "Bootstrap version. Do not delete.")`,
		allegrosql.SystemDB, allegrosql.MilevaDBTable, milevadbServerVersionVar, currentBootstrapVersion)
	mustExecute(s, allegrosql)

	writeSystemTZ(s)

	writeNewDefCauslationParameter(s, config.GetGlobalConfig().NewDefCauslationsEnabledOnFirstBootstrap)

	writeDefaultExprPushDownBlacklist(s)

	writeStmtSummaryVars(s)

	_, err := s.Execute(context.Background(), "COMMIT")
	if err != nil {
		sleepTime := 1 * time.Second
		logutil.BgLogger().Info("doDMLWorks failed", zap.Error(err), zap.Duration("sleeping time", sleepTime))
		time.Sleep(sleepTime)
		// Check if MilevaDB is already bootstrapped.
		b, err1 := checkBootstrapped(s)
		if err1 != nil {
			logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err1))
		}
		if b {
			return
		}
		logutil.BgLogger().Fatal("doDMLWorks failed", zap.Error(err))
	}
}

func mustExecute(s Stochastik, allegrosql string) {
	_, err := s.Execute(context.Background(), allegrosql)
	if err != nil {
		debug.PrintStack()
		logutil.BgLogger().Fatal("mustExecute error", zap.Error(err))
	}
}

// oldPasswordUpgrade upgrade password to MyALLEGROSQL compatible format
func oldPasswordUpgrade(pass string) (string, error) {
	hash1, err := hex.DecodeString(pass)
	if err != nil {
		return "", errors.Trace(err)
	}

	hash2 := auth.Sha1Hash(hash1)
	newpass := fmt.Sprintf("*%X", hash2)
	return newpass, nil
}
