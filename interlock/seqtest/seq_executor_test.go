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

// Note: All the tests in this file will be executed sequentially.

package interlock_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	pb "github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	dbssolitonutil "github.com/whtcorpsinc/milevadb/dbs/solitonutil"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/gcutil"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	TestingT(t)
}

var _ = SerialSuites(&seqTestSuite{})
var _ = SerialSuites(&seqTestSuite1{})

type seqTestSuite struct {
	cluster     cluster.Cluster
	causetstore ekv.CausetStorage
	petri       *petri.Petri
	*BerolinaSQL.BerolinaSQL
	ctx *mock.Context
}

var mockEinsteinDB = flag.Bool("mockEinsteinDB", true, "use mock einsteindb causetstore in interlock test")

func (s *seqTestSuite) SetUpSuite(c *C) {
	s.BerolinaSQL = BerolinaSQL.New()
	flag.Lookup("mockEinsteinDB")
	useMockEinsteinDB := *mockEinsteinDB
	if useMockEinsteinDB {
		var err error
		s.causetstore, err = mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		stochastik.SetSchemaLease(0)
		stochastik.DisableStats4Test()
	}
	d, err := stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	d.SetStatsUFIDelating(true)
	s.petri = d
}

func (s *seqTestSuite) TearDownSuite(c *C) {
	s.petri.Close()
	s.causetstore.Close()
}

func (s *seqTestSuite) TestEarlyClose(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet earlyclose (id int primary key)")

	N := 100
	// Insert N rows.
	var values []string
	for i := 0; i < N; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustInterDirc("insert earlyclose values " + strings.Join(values, ","))

	// Get causet ID for split.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("earlyclose"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the causet.
	s.cluster.SplitBlock(tblID, N/2)

	ctx := context.Background()
	for i := 0; i < N/2; i++ {
		rss, err1 := tk.Se.InterDircute(ctx, "select * from earlyclose order by id")
		c.Assert(err1, IsNil)
		rs := rss[0]
		req := rs.NewChunk()
		err = rs.Next(ctx, req)
		c.Assert(err, IsNil)
		rs.Close()
	}

	// Goroutine should not leak when error happen.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/handleTaskOnceError", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/handleTaskOnceError"), IsNil)
	}()
	rss, err := tk.Se.InterDircute(ctx, "select * from earlyclose")
	c.Assert(err, IsNil)
	rs := rss[0]
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, NotNil)
	rs.Close()
}

type stats struct {
}

func (s stats) GetScope(status string) variable.ScopeFlag { return variable.DefaultStatusVarScopeFlag }

func (s stats) Stats(vars *variable.StochastikVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	var a, b interface{}
	b = "123"
	m["test_interface_nil"] = a
	m["test_interface"] = b
	m["test_interface_slice"] = []interface{}{"a", "b", "c"}
	return m, nil
}

func (s *seqTestSuite) TestShow(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	testALLEGROSQL := `drop causet if exists show_test`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = `create causet SHOW_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int comment "c1_comment", c2 int, c3 int default 1, c4 text, c5 boolean, key idx_wide_c4(c3, c4(10))) ENGINE=InnoDB AUTO_INCREMENT=28934 DEFAULT CHARSET=utf8 COMMENT "block_comment";`
	tk.MustInterDirc(testALLEGROSQL)

	testALLEGROSQL = "show defCausumns from show_test;"
	result := tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 6)

	testALLEGROSQL = "show create causet show_test;"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event := result.Events()[0]
	// For issue https://github.com/whtcorpsinc/milevadb/issues/1061
	expectedEvent := []interface{}{
		"SHOW_test", "CREATE TABLE `SHOW_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `c1` int(11) DEFAULT NULL COMMENT 'c1_comment',\n  `c2` int(11) DEFAULT NULL,\n  `c3` int(11) DEFAULT 1,\n  `c4` text DEFAULT NULL,\n  `c5` tinyint(1) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_wide_c4` (`c3`,`c4`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=28934 COMMENT='block_comment'"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// For issue https://github.com/whtcorpsinc/milevadb/issues/1918
	testALLEGROSQL = `create causet ptest(
		a int primary key,
		b double NOT NULL DEFAULT 2.0,
		c varchar(10) NOT NULL,
		d time unique,
		e timestamp NULL,
		f timestamp
	);`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create causet ptest;"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"ptest", "CREATE TABLE `ptest` (\n  `a` int(11) NOT NULL,\n  `b` double NOT NULL DEFAULT 2.0,\n  `c` varchar(10) NOT NULL,\n  `d` time DEFAULT NULL,\n  `e` timestamp NULL DEFAULT NULL,\n  `f` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `d` (`d`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// Issue #4684.
	tk.MustInterDirc("drop causet if exists `t1`")
	testALLEGROSQL = "create causet `t1` (" +
		"`c1` tinyint unsigned default null," +
		"`c2` smallint unsigned default null," +
		"`c3` mediumint unsigned default null," +
		"`c4` int unsigned default null," +
		"`c5` bigint unsigned default null);"

	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create causet t1"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"t1", "CREATE TABLE `t1` (\n" +
			"  `c1` tinyint(3) unsigned DEFAULT NULL,\n" +
			"  `c2` smallint(5) unsigned DEFAULT NULL,\n" +
			"  `c3` mediumint(8) unsigned DEFAULT NULL,\n" +
			"  `c4` int(10) unsigned DEFAULT NULL,\n" +
			"  `c5` bigint(20) unsigned DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// Issue #7665
	tk.MustInterDirc("drop causet if exists `decimalschema`")
	testALLEGROSQL = "create causet `decimalschema` (`c1` decimal);"
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create causet decimalschema"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"decimalschema", "CREATE TABLE `decimalschema` (\n" +
			"  `c1` decimal(11,0) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	tk.MustInterDirc("drop causet if exists `decimalschema`")
	testALLEGROSQL = "create causet `decimalschema` (`c1` decimal(15));"
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create causet decimalschema"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"decimalschema", "CREATE TABLE `decimalschema` (\n" +
			"  `c1` decimal(15,0) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// test SHOW CREATE TABLE with invisible index
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc(`create causet t (
		a int,
		b int,
		c int UNIQUE KEY,
		d int UNIQUE KEY,
		index invisible_idx_b (b) invisible,
		index (d) invisible)`)
	expected :=
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) DEFAULT NULL,\n" +
			"  `b` int(11) DEFAULT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `d` int(11) DEFAULT NULL,\n" +
			"  KEY `invisible_idx_b` (`b`) /*!80000 INVISIBLE */,\n" +
			"  KEY `d` (`d`) /*!80000 INVISIBLE */,\n" +
			"  UNIQUE KEY `c` (`c`),\n" +
			"  UNIQUE KEY `d_2` (`d`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	tk.MustQuery("show create causet t").Check(testkit.Events(expected))
	tk.MustInterDirc("drop causet t")

	testALLEGROSQL = "SHOW VARIABLES LIKE 'character_set_results';"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)

	// Test case for index type and comment
	tk.MustInterDirc(`create causet show_index (id int, c int, primary key (id), index cIdx using hash (c) comment "index_comment_for_cIdx");`)
	tk.MustInterDirc(`create index idx1 on show_index (id) using hash;`)
	tk.MustInterDirc(`create index idx2 on show_index (id) comment 'idx';`)
	tk.MustInterDirc(`create index idx3 on show_index (id) using hash comment 'idx';`)
	tk.MustInterDirc(`alter causet show_index add index idx4 (id) using btree comment 'idx';`)
	tk.MustInterDirc(`create index idx5 using hash on show_index (id) using btree comment 'idx';`)
	tk.MustInterDirc(`create index idx6 using hash on show_index (id);`)
	tk.MustInterDirc(`create index idx7 on show_index (id);`)
	tk.MustInterDirc(`create index idx8 on show_index (id) visible;`)
	tk.MustInterDirc(`create index idx9 on show_index (id) invisible;`)
	tk.MustInterDirc(`create index expr_idx on show_index ((id*2+1))`)
	testALLEGROSQL = "SHOW index from show_index;"
	tk.MustQuery(testALLEGROSQL).Check(solitonutil.EventsWithSep("|",
		"show_index|0|PRIMARY|1|id|A|0|<nil>|<nil>||BTREE| |YES|NULL",
		"show_index|1|cIdx|1|c|A|0|<nil>|<nil>|YES|HASH||index_comment_for_cIdx|YES|NULL",
		"show_index|1|idx1|1|id|A|0|<nil>|<nil>|YES|HASH| |YES|NULL",
		"show_index|1|idx2|1|id|A|0|<nil>|<nil>|YES|BTREE||idx|YES|NULL",
		"show_index|1|idx3|1|id|A|0|<nil>|<nil>|YES|HASH||idx|YES|NULL",
		"show_index|1|idx4|1|id|A|0|<nil>|<nil>|YES|BTREE||idx|YES|NULL",
		"show_index|1|idx5|1|id|A|0|<nil>|<nil>|YES|BTREE||idx|YES|NULL",
		"show_index|1|idx6|1|id|A|0|<nil>|<nil>|YES|HASH| |YES|NULL",
		"show_index|1|idx7|1|id|A|0|<nil>|<nil>|YES|BTREE| |YES|NULL",
		"show_index|1|idx8|1|id|A|0|<nil>|<nil>|YES|BTREE| |YES|NULL",
		"show_index|1|idx9|1|id|A|0|<nil>|<nil>|YES|BTREE| |NO|NULL",
		"show_index|1|expr_idx|1|NULL|A|0|<nil>|<nil>|YES|BTREE| |YES|(`id` * 2 + 1)",
	))

	// For show like with escape
	testALLEGROSQL = `show blocks like 'SHOW\_test'`
	result = tk.MustQuery(testALLEGROSQL)
	rows := result.Events()
	c.Check(rows, HasLen, 1)
	c.Check(rows[0], DeepEquals, []interface{}{"SHOW_test"})

	var ss stats
	variable.RegisterStatistics(ss)
	testALLEGROSQL = "show status like 'character_set_results';"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), NotNil)

	tk.MustQuery("SHOW PROCEDURE STATUS WHERE EDB='test'").Check(testkit.Events())
	tk.MustQuery("SHOW TRIGGERS WHERE `Trigger` ='test'").Check(testkit.Events())
	tk.MustQuery("SHOW PROCESSLIST;").Check(testkit.Events())
	tk.MustQuery("SHOW FULL PROCESSLIST;").Check(testkit.Events())
	tk.MustQuery("SHOW EVENTS WHERE EDB = 'test'").Check(testkit.Events())
	tk.MustQuery("SHOW PLUGINS").Check(testkit.Events())
	tk.MustQuery("SHOW PROFILES").Check(testkit.Events())

	// +-------------+--------------------+--------------+------------------+-------------------+
	// | File        | Position           | Binlog_Do_DB | Binlog_Ignore_DB | InterDircuted_Gtid_Set |
	// +-------------+--------------------+--------------+------------------+-------------------+
	// | milevadb-binlog | 400668057259474944 |              |                  |                   |
	// +-------------+--------------------+--------------+------------------+-------------------+
	result = tk.MustQuery("SHOW MASTER STATUS")
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	c.Check(event, HasLen, 5)
	c.Assert(event[1].(string) != "0", IsTrue)

	tk.MustQuery("SHOW PRIVILEGES")

	// Test show create database
	testALLEGROSQL = `create database show_test_DB`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create database show_test_DB;"
	tk.MustQuery(testALLEGROSQL).Check(solitonutil.EventsWithSep("|",
		"show_test_DB|CREATE DATABASE `show_test_DB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))
	testALLEGROSQL = "show create database if not exists show_test_DB;"
	tk.MustQuery(testALLEGROSQL).Check(solitonutil.EventsWithSep("|",
		"show_test_DB|CREATE DATABASE /*!32312 IF NOT EXISTS*/ `show_test_DB` /*!40100 DEFAULT CHARACTER SET utf8mb4 */",
	))

	tk.MustInterDirc("use show_test_DB")
	result = tk.MustQuery("SHOW index from show_index from test where DeferredCauset_name = 'c'")
	c.Check(result.Events(), HasLen, 1)

	// Test show full defCausumns
	// for issue https://github.com/whtcorpsinc/milevadb/issues/4224
	tk.MustInterDirc(`drop causet if exists show_test_comment`)
	tk.MustInterDirc(`create causet show_test_comment (id int not null default 0 comment "show_test_comment_id")`)
	tk.MustQuery(`show full defCausumns from show_test_comment`).Check(solitonutil.EventsWithSep("|",
		"id|int(11)|<nil>|NO||0||select,insert,uFIDelate,references|show_test_comment_id",
	))

	// Test show create causet with AUTO_INCREMENT option
	// for issue https://github.com/whtcorpsinc/milevadb/issues/3747
	tk.MustInterDirc(`drop causet if exists show_auto_increment`)
	tk.MustInterDirc(`create causet show_auto_increment (id int key auto_increment) auto_increment=4`)
	tk.MustQuery(`show create causet show_auto_increment`).Check(solitonutil.EventsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=4",
	))
	// for issue https://github.com/whtcorpsinc/milevadb/issues/4678
	autoIDStep := autoid.GetStep()
	tk.MustInterDirc("insert into show_auto_increment values(20)")
	autoID := autoIDStep + 21
	tk.MustQuery(`show create causet show_auto_increment`).Check(solitonutil.EventsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT="+strconv.Itoa(int(autoID)),
	))
	tk.MustInterDirc(`drop causet show_auto_increment`)
	tk.MustInterDirc(`create causet show_auto_increment (id int primary key auto_increment)`)
	tk.MustQuery(`show create causet show_auto_increment`).Check(solitonutil.EventsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustInterDirc("insert into show_auto_increment values(10)")
	autoID = autoIDStep + 11
	tk.MustQuery(`show create causet show_auto_increment`).Check(solitonutil.EventsWithSep("|",
		""+
			"show_auto_increment CREATE TABLE `show_auto_increment` (\n"+
			"  `id` int(11) NOT NULL AUTO_INCREMENT,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT="+strconv.Itoa(int(autoID)),
	))

	// Test show causet with defCausumn's comment contain escape character
	// for issue https://github.com/whtcorpsinc/milevadb/issues/4411
	tk.MustInterDirc(`drop causet if exists show_escape_character`)
	tk.MustInterDirc(`create causet show_escape_character(id int comment 'a\rb\nc\td\0ef')`)
	tk.MustQuery(`show create causet show_escape_character`).Check(solitonutil.EventsWithSep("|",
		""+
			"show_escape_character CREATE TABLE `show_escape_character` (\n"+
			"  `id` int(11) DEFAULT NULL COMMENT 'a\\rb\\nc	d\\0ef'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// for issue https://github.com/whtcorpsinc/milevadb/issues/4424
	tk.MustInterDirc("drop causet if exists show_test")
	testALLEGROSQL = `create causet show_test(
		a varchar(10) COMMENT 'a\nb\rc\td\0e'
	) COMMENT='a\nb\rc\td\0e';`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create causet show_test;"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `a` varchar(10) DEFAULT NULL COMMENT 'a\\nb\\rc	d\\0e'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='a\\nb\\rc	d\\0e'"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// for issue https://github.com/whtcorpsinc/milevadb/issues/4425
	tk.MustInterDirc("drop causet if exists show_test")
	testALLEGROSQL = `create causet show_test(
		a varchar(10) DEFAULT 'a\nb\rc\td\0e'
	);`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create causet show_test;"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `a` varchar(10) DEFAULT 'a\\nb\\rc	d\\0e'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// for issue https://github.com/whtcorpsinc/milevadb/issues/4426
	tk.MustInterDirc("drop causet if exists show_test")
	testALLEGROSQL = `create causet show_test(
		a bit(1),
		b bit(32) DEFAULT 0b0,
		c bit(1) DEFAULT 0b1,
		d bit(10) DEFAULT 0b1010
	);`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = "show create causet show_test;"
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `a` bit(1) DEFAULT NULL,\n  `b` bit(32) DEFAULT b'0',\n  `c` bit(1) DEFAULT b'1',\n  `d` bit(10) DEFAULT b'1010'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// for issue #4255
	result = tk.MustQuery(`show function status like '%'`)
	result.Check(result.Events())
	result = tk.MustQuery(`show plugins like '%'`)
	result.Check(result.Events())

	// for issue #4740
	testALLEGROSQL = `drop causet if exists t`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = `create causet t (a int1, b int2, c int3, d int4, e int8)`
	tk.MustInterDirc(testALLEGROSQL)
	testALLEGROSQL = `show create causet t;`
	result = tk.MustQuery(testALLEGROSQL)
	c.Check(result.Events(), HasLen, 1)
	event = result.Events()[0]
	expectedEvent = []interface{}{
		"t",
		"CREATE TABLE `t` (\n" +
			"  `a` tinyint(4) DEFAULT NULL,\n" +
			"  `b` smallint(6) DEFAULT NULL,\n" +
			"  `c` mediumint(9) DEFAULT NULL,\n" +
			"  `d` int(11) DEFAULT NULL,\n" +
			"  `e` bigint(20) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	}
	for i, r := range event {
		c.Check(r, Equals, expectedEvent[i])
	}

	// Test get default defCauslate for a specified charset.
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`create causet t (a int) default charset=utf8mb4`)
	tk.MustQuery(`show create causet t`).Check(solitonutil.EventsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Test range partition
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`CREATE TABLE t (a int) PARTITION BY RANGE(a) (
 	PARTITION p0 VALUES LESS THAN (10),
 	PARTITION p1 VALUES LESS THAN (20),
 	PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	tk.MustQuery("show create causet t").Check(solitonutil.EventsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"+"\nPARTITION BY RANGE ( `a` ) (\n  PARTITION `p0` VALUES LESS THAN (10),\n  PARTITION `p1` VALUES LESS THAN (20),\n  PARTITION `p2` VALUES LESS THAN (MAXVALUE)\n)",
	))

	tk.MustInterDirc(`drop causet if exists t`)
	_, err := tk.InterDirc(`CREATE TABLE t (x int, y char) PARTITION BY RANGE(y) (
 	PARTITION p0 VALUES LESS THAN (10),
 	PARTITION p1 VALUES LESS THAN (20),
 	PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	c.Assert(err, NotNil)

	// Test range defCausumns partition
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`CREATE TABLE t (a int, b int, c char, d int) PARTITION BY RANGE COLUMNS(a,d,c) (
 	PARTITION p0 VALUES LESS THAN (5,10,'ggg'),
 	PARTITION p1 VALUES LESS THAN (10,20,'mmm'),
 	PARTITION p2 VALUES LESS THAN (15,30,'sss'),
        PARTITION p3 VALUES LESS THAN (50,MAXVALUE,MAXVALUE))`)
	tk.MustQuery("show create causet t").Check(solitonutil.EventsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL,\n"+
			"  `b` int(11) DEFAULT NULL,\n"+
			"  `c` char(1) DEFAULT NULL,\n"+
			"  `d` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Test hash partition
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`CREATE TABLE t (a int) PARTITION BY HASH(a) PARTITIONS 4`)
	tk.MustQuery("show create causet t").Check(solitonutil.EventsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"+"\nPARTITION BY HASH( `a` )\nPARTITIONS 4",
	))

	// Test show create causet compression type.
	tk.MustInterDirc(`drop causet if exists t1`)
	tk.MustInterDirc(`CREATE TABLE t1 (c1 INT) COMPRESSION="zlib";`)
	tk.MustQuery("show create causet t1").Check(solitonutil.EventsWithSep("|",
		"t1 CREATE TABLE `t1` (\n"+
			"  `c1` int(11) DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMPRESSION='zlib'",
	))

	// Test show create causet year type
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`create causet t(y year unsigned signed zerofill zerofill, x int, primary key(y));`)
	tk.MustQuery(`show create causet t`).Check(solitonutil.EventsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `y` year(4) NOT NULL,\n"+
			"  `x` int(11) DEFAULT NULL,\n"+
			"  PRIMARY KEY (`y`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test show create causet with zerofill flag
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`create causet t(id int primary key, val tinyint(10) zerofill);`)
	tk.MustQuery(`show create causet t`).Check(solitonutil.EventsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `id` int(11) NOT NULL,\n"+
			"  `val` tinyint(10) unsigned zerofill DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test show defCausumns with different types of default value
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`create causet t(
		c0 int default 1,
		c1 int default b'010',
		c2 bigint default x'A7',
		c3 bit(8) default b'00110001',
		c4 varchar(6) default b'00110001',
		c5 varchar(6) default '\'C6\'',
		c6 enum('s', 'm', 'l', 'xl') default 'xl',
		c7 set('a', 'b', 'c', 'd') default 'a,c,c',
		c8 datetime default current_timestamp on uFIDelate current_timestamp,
		c9 year default '2020'
	);`)
	tk.MustQuery(`show defCausumns from t`).Check(solitonutil.EventsWithSep("|",
		"c0|int(11)|YES||1|",
		"c1|int(11)|YES||2|",
		"c2|bigint(20)|YES||167|",
		"c3|bit(8)|YES||b'110001'|",
		"c4|varchar(6)|YES||1|",
		"c5|varchar(6)|YES||'C6'|",
		"c6|enum('s','m','l','xl')|YES||xl|",
		"c7|set('a','b','c','d')|YES||a,c|",
		"c8|datetime|YES||CURRENT_TIMESTAMP|DEFAULT_GENERATED on uFIDelate CURRENT_TIMESTAMP",
		"c9|year(4)|YES||2020|",
	))

	// Test if 'show [status|variables]' is sorted by Variable_name (#14542)
	sqls := []string{
		"show global status;",
		"show stochastik status;",
		"show global variables",
		"show stochastik variables"}

	for _, allegrosql := range sqls {
		res := tk.MustQuery(allegrosql)
		c.Assert(res, NotNil)
		sorted := tk.MustQuery(allegrosql).Sort()
		c.Assert(sorted, NotNil)
		c.Check(res, DeepEquals, sorted)
	}
}

func (s *seqTestSuite) TestShowStatsHealthy(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int)")
	tk.MustInterDirc("create index idx on t(a)")
	tk.MustInterDirc("analyze causet t")
	tk.MustQuery("show stats_healthy").Check(testkit.Events("test t  100"))
	tk.MustInterDirc("insert into t values (1), (2)")
	do, _ := stochastik.GetPetri(s.causetstore)
	do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll)
	tk.MustInterDirc("analyze causet t")
	tk.MustQuery("show stats_healthy").Check(testkit.Events("test t  100"))
	tk.MustInterDirc("insert into t values (3), (4), (5), (6), (7), (8), (9), (10)")
	do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll)
	do.StatsHandle().UFIDelate(do.SchemaReplicant())
	tk.MustQuery("show stats_healthy").Check(testkit.Events("test t  19"))
	tk.MustInterDirc("analyze causet t")
	tk.MustQuery("show stats_healthy").Check(testkit.Events("test t  100"))
	tk.MustInterDirc("delete from t")
	do.StatsHandle().DumpStatsDeltaToKV(handle.DumpAll)
	do.StatsHandle().UFIDelate(do.SchemaReplicant())
	tk.MustQuery("show stats_healthy").Check(testkit.Events("test t  0"))
}

// TestIndexDoubleReadClose checks that when a index double read returns before reading all the rows, the goroutine doesn't
// leak. For testing allegrosql with multiple regions, we need to manually split a mock EinsteinDB.
func (s *seqTestSuite) TestIndexDoubleReadClose(c *C) {
	if _, ok := s.causetstore.GetClient().(*einsteindb.CopClient); !ok {
		// Make sure the causetstore is einsteindb causetstore.
		return
	}
	originSize := atomic.LoadInt32(&interlock.LookupBlockTaskChannelSize)
	atomic.StoreInt32(&interlock.LookupBlockTaskChannelSize, 1)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("set @@milevadb_index_lookup_size = '10'")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet dist (id int primary key, c_idx int, c_defCaus int, index (c_idx))")

	// Insert 100 rows.
	var values []string
	for i := 0; i < 100; i++ {
		values = append(values, fmt.Sprintf("(%d, %d, %d)", i, i, i))
	}
	tk.MustInterDirc("insert dist values " + strings.Join(values, ","))

	rs, err := tk.InterDirc("select * from dist where c_idx between 0 and 100")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)
	keyword := "pickAndInterDircTask"
	rs.Close()
	time.Sleep(time.Millisecond * 10)
	c.Check(checkGoroutineExists(keyword), IsFalse)
	atomic.StoreInt32(&interlock.LookupBlockTaskChannelSize, originSize)
}

// TestIndexMergeReaderClose checks that when a partial index worker failed to start, the goroutine doesn't
// leak.
func (s *seqTestSuite) TestIndexMergeReaderClose(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int)")
	tk.MustInterDirc("create index idx1 on t(a)")
	tk.MustInterDirc("create index idx2 on t(b)")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/startPartialIndexWorkerErr", "return"), IsNil)
	err := tk.QueryToErr("select /*+ USE_INDEX_MERGE(t, idx1, idx2) */ * from t where a > 10 or b < 100")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/startPartialIndexWorkerErr"), IsNil)
	c.Assert(err, NotNil)
	c.Check(checkGoroutineExists("fetchLoop"), IsFalse)
	c.Check(checkGoroutineExists("fetchHandles"), IsFalse)
	c.Check(checkGoroutineExists("waitPartialWorkersAndCloseFetchChan"), IsFalse)
}

func (s *seqTestSuite) TestParallelHashAggClose(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("insert into t values(1,1),(2,2)")
	// desc select sum(a) from (select cast(t.a as signed) as a, b from t) t group by b
	// HashAgg_8                | 2.40  | root       | group by:t.b, funcs:sum(t.a)
	// └─Projection_9           | 3.00  | root       | cast(test.t.a), test.t.b
	//   └─BlockReader_11       | 3.00  | root       | data:BlockFullScan_10
	//     └─BlockFullScan_10   | 3.00  | cop[einsteindb]  | causet:t, keep order:fa$se, stats:pseudo |

	// Goroutine should not leak when error happen.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/parallelHashAggError", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/parallelHashAggError"), IsNil)
	}()
	ctx := context.Background()
	rss, err := tk.Se.InterDircute(ctx, "select sum(a) from (select cast(t.a as signed) as a, b from t) t group by b;")
	c.Assert(err, IsNil)
	rs := rss[0]
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err.Error(), Equals, "HashAggInterDirc.parallelInterDirc error")
}

func (s *seqTestSuite) TestUnparallelHashAggClose(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("insert into t values(1,1),(2,2)")

	// Goroutine should not leak when error happen.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/unparallelHashAggError", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/unparallelHashAggError"), IsNil)
	}()
	ctx := context.Background()
	rss, err := tk.Se.InterDircute(ctx, "select sum(distinct a) from (select cast(t.a as signed) as a, b from t) t group by b;")
	c.Assert(err, IsNil)
	rs := rss[0]
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err.Error(), Equals, "HashAggInterDirc.unparallelInterDirc error")
}

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	profile.WriteTo(buf, 1)
	str := buf.String()
	return strings.Contains(str, keyword)
}

func (s *seqTestSuite) TestAdminShowNextID(c *C) {
	HelperTestAdminShowNextID(c, s, `admin show `)
	HelperTestAdminShowNextID(c, s, `show causet `)
}

func HelperTestAdminShowNextID(c *C, s *seqTestSuite, str string) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange"), IsNil)
	}()
	step := int64(10)
	autoIDStep := autoid.GetStep()
	autoid.SetStep(step)
	defer autoid.SetStep(autoIDStep)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t,tt")
	tk.MustInterDirc("create causet t(id int, c int)")
	// Start handle is 1.
	r := tk.MustQuery(str + " t next_row_id")
	r.Check(testkit.Events("test t _milevadb_rowid 1 AUTO_INCREMENT"))
	// Event ID is step + 1.
	tk.MustInterDirc("insert into t values(1, 1)")
	r = tk.MustQuery(str + " t next_row_id")
	r.Check(testkit.Events("test t _milevadb_rowid 11 AUTO_INCREMENT"))
	// Event ID is original + step.
	for i := 0; i < int(step); i++ {
		tk.MustInterDirc("insert into t values(10000, 1)")
	}
	r = tk.MustQuery(str + " t next_row_id")
	r.Check(testkit.Events("test t _milevadb_rowid 21 AUTO_INCREMENT"))
	tk.MustInterDirc("drop causet t")

	// test for a causet with the primary key
	tk.MustInterDirc("create causet tt(id int primary key auto_increment, c int)")
	// Start handle is 1.
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Events("test tt id 1 AUTO_INCREMENT"))
	// After rebasing auto ID, event ID is 20 + step + 1.
	tk.MustInterDirc("insert into tt values(20, 1)")
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Events("test tt id 31 AUTO_INCREMENT"))
	// test for renaming the causet
	tk.MustInterDirc("drop database if exists test1")
	tk.MustInterDirc("create database test1")
	tk.MustInterDirc("rename causet test.tt to test1.tt")
	tk.MustInterDirc("use test1")
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Events("test1 tt id 31 AUTO_INCREMENT"))
	tk.MustInterDirc("insert test1.tt values ()")
	r = tk.MustQuery(str + " tt next_row_id")
	r.Check(testkit.Events("test1 tt id 41 AUTO_INCREMENT"))
	tk.MustInterDirc("drop causet tt")

	solitonutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer solitonutil.ConfigTestUtils.RestoreAutoRandomTestConfig()
	tk.MustInterDirc("set @@allow_auto_random_explicit_insert = true")

	// Test for a causet with auto_random primary key.
	tk.MustInterDirc("create causet t3(id bigint primary key auto_random(5), c int)")
	// Start handle is 1.
	r = tk.MustQuery(str + " t3 next_row_id")
	r.Check(testkit.Events("test1 t3 id 1 AUTO_RANDOM"))
	// Insert some rows.
	tk.MustInterDirc("insert into t3 (c) values (1), (2);")
	r = tk.MustQuery(str + " t3 next_row_id")
	r.Check(testkit.Events("test1 t3 id 11 AUTO_RANDOM"))
	// Rebase.
	tk.MustInterDirc("insert into t3 (id, c) values (103, 3);")
	r = tk.MustQuery(str + " t3 next_row_id")
	r.Check(testkit.Events("test1 t3 id 114 AUTO_RANDOM"))

	// Test for a sequence.
	tk.MustInterDirc("create sequence seq1 start 15 cache 57")
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Events("test1 seq1 _milevadb_rowid 1 AUTO_INCREMENT", "test1 seq1  15 SEQUENCE"))
	r = tk.MustQuery("select nextval(seq1)")
	r.Check(testkit.Events("15"))
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Events("test1 seq1 _milevadb_rowid 1 AUTO_INCREMENT", "test1 seq1  72 SEQUENCE"))
	r = tk.MustQuery("select nextval(seq1)")
	r.Check(testkit.Events("16"))
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Events("test1 seq1 _milevadb_rowid 1 AUTO_INCREMENT", "test1 seq1  72 SEQUENCE"))
	r = tk.MustQuery("select setval(seq1, 96)")
	r.Check(testkit.Events("96"))
	r = tk.MustQuery(str + " seq1 next_row_id")
	r.Check(testkit.Events("test1 seq1 _milevadb_rowid 1 AUTO_INCREMENT", "test1 seq1  97 SEQUENCE"))
}

func (s *seqTestSuite) TestNoHistoryWhenDisableRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists history")
	tk.MustInterDirc("create causet history (a int)")
	tk.MustInterDirc("set @@autocommit = 0")

	// retry_limit = 0 will not add history.
	tk.MustInterDirc("set @@milevadb_retry_limit = 0")
	tk.MustInterDirc("insert history values (1)")
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 0)

	// Disable auto_retry will add history for auto committed only
	tk.MustInterDirc("set @@autocommit = 1")
	tk.MustInterDirc("set @@milevadb_retry_limit = 10")
	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 1")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/stochastik/keepHistory", `return(true)`), IsNil)
	tk.MustInterDirc("insert history values (1)")
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 1)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/stochastik/keepHistory"), IsNil)
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert history values (1)")
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 0)
	tk.MustInterDirc("commit")

	// Enable auto_retry will add history for both.
	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/stochastik/keepHistory", `return(true)`), IsNil)
	tk.MustInterDirc("insert history values (1)")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/stochastik/keepHistory"), IsNil)
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 1)
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert history values (1)")
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 2)
	tk.MustInterDirc("commit")
}

func (s *seqTestSuite) TestPrepareMaxParamCountCheck(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (v int)")
	normalALLEGROSQL, normalParams := generateBatchALLEGROSQL(math.MaxUint16)
	_, err := tk.InterDirc(normalALLEGROSQL, normalParams...)
	c.Assert(err, IsNil)

	bigALLEGROSQL, bigParams := generateBatchALLEGROSQL(math.MaxUint16 + 2)
	_, err = tk.InterDirc(bigALLEGROSQL, bigParams...)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[interlock:1390]Prepared memex contains too many placeholders")
}

func generateBatchALLEGROSQL(paramCount int) (allegrosql string, paramSlice []interface{}) {
	params := make([]interface{}, 0, paramCount)
	placeholders := make([]string, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		params = append(params, i)
		placeholders = append(placeholders, "(?)")
	}
	return "insert into t values " + strings.Join(placeholders, ","), params
}

func (s *seqTestSuite) TestCartesianProduct(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(c1 int)")
	causetembedded.AllowCartesianProduct.CausetStore(false)
	err := tk.InterDircToErr("select * from t t1, t t2")
	c.Check(causetembedded.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	err = tk.InterDircToErr("select * from t t1 left join t t2 on 1")
	c.Check(causetembedded.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	err = tk.InterDircToErr("select * from t t1 right join t t2 on 1")
	c.Check(causetembedded.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	causetembedded.AllowCartesianProduct.CausetStore(true)
}

func (s *seqTestSuite) TestBatchInsertDelete(c *C) {
	originLimit := atomic.LoadUint64(&ekv.TxnTotalSizeLimit)
	defer func() {
		atomic.StoreUint64(&ekv.TxnTotalSizeLimit, originLimit)
	}()
	// Set the limitation to a small value, make it easier to reach the limitation.
	atomic.StoreUint64(&ekv.TxnTotalSizeLimit, 5000)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists batch_insert")
	tk.MustInterDirc("create causet batch_insert (c int)")
	tk.MustInterDirc("drop causet if exists batch_insert_on_duplicate")
	tk.MustInterDirc("create causet batch_insert_on_duplicate (id int primary key, c int)")
	// Insert 10 rows.
	tk.MustInterDirc("insert into batch_insert values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)")
	r := tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("10"))
	// Insert 10 rows.
	tk.MustInterDirc("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("20"))
	// Insert 20 rows.
	tk.MustInterDirc("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("40"))
	// Insert 40 rows.
	tk.MustInterDirc("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("80"))
	// Insert 80 rows.
	tk.MustInterDirc("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("160"))
	tk.MustInterDirc("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("320"))
	// for on duplicate key
	for i := 0; i < 320; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into batch_insert_on_duplicate values(%d, %d);", i, i))
	}
	r = tk.MustQuery("select count(*) from batch_insert_on_duplicate;")
	r.Check(testkit.Events("320"))

	// This will meet txn too large error.
	_, err := tk.InterDirc("insert into batch_insert (c) select * from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrTxnTooLarge.Equal(err), IsTrue)
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("320"))

	// Test milevadb_batch_insert could not work if enable-batch-dml is disabled.
	tk.MustInterDirc("set @@stochastik.milevadb_batch_insert=1;")
	_, err = tk.InterDirc("insert into batch_insert (c) select * from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrTxnTooLarge.Equal(err), IsTrue)
	tk.MustInterDirc("set @@stochastik.milevadb_batch_insert=0;")

	// for on duplicate key
	_, err = tk.InterDirc(`insert into batch_insert_on_duplicate select * from batch_insert_on_duplicate as tt
		on duplicate key uFIDelate batch_insert_on_duplicate.id=batch_insert_on_duplicate.id+1000;`)
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrTxnTooLarge.Equal(err), IsTrue, Commentf("%v", err))
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("320"))

	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EnableBatchDML = true
	})

	// Change to batch inset mode and batch size to 50.
	tk.MustInterDirc("set @@stochastik.milevadb_batch_insert=1;")
	tk.MustInterDirc("set @@stochastik.milevadb_dml_batch_size=50;")
	tk.MustInterDirc("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("640"))

	// Enlarge the batch size to 150 which is larger than the txn limitation (100).
	// So the insert will meet error.
	tk.MustInterDirc("set @@stochastik.milevadb_dml_batch_size=600;")
	_, err = tk.InterDirc("insert into batch_insert (c) select * from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrTxnTooLarge.Equal(err), IsTrue)
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("640"))
	// Set it back to 50.
	tk.MustInterDirc("set @@stochastik.milevadb_dml_batch_size=50;")

	// for on duplicate key
	_, err = tk.InterDirc(`insert into batch_insert_on_duplicate select * from batch_insert_on_duplicate as tt
		on duplicate key uFIDelate batch_insert_on_duplicate.id=batch_insert_on_duplicate.id+1000;`)
	c.Assert(err, IsNil)
	r = tk.MustQuery("select count(*) from batch_insert_on_duplicate;")
	r.Check(testkit.Events("320"))

	// Disable BachInsert mode in transition.
	tk.MustInterDirc("begin;")
	_, err = tk.InterDirc("insert into batch_insert (c) select * from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrTxnTooLarge.Equal(err), IsTrue)
	tk.MustInterDirc("rollback;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("640"))

	tk.MustInterDirc("drop causet if exists com_batch_insert")
	tk.MustInterDirc("create causet com_batch_insert (c int)")
	allegrosql := "insert into com_batch_insert values "
	values := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		values = append(values, "(1)")
	}
	allegrosql = allegrosql + strings.Join(values, ",")
	tk.MustInterDirc(allegrosql)
	tk.MustQuery("select count(*) from com_batch_insert;").Check(testkit.Events("200"))

	// Test case for batch delete.
	// This will meet txn too large error.
	_, err = tk.InterDirc("delete from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrTxnTooLarge.Equal(err), IsTrue)
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("640"))
	// Enable batch delete and set batch size to 50.
	tk.MustInterDirc("set @@stochastik.milevadb_batch_delete=on;")
	tk.MustInterDirc("set @@stochastik.milevadb_dml_batch_size=50;")
	tk.MustInterDirc("delete from batch_insert;")
	// Make sure that all rows are gone.
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Events("0"))
}

type checkPrioClient struct {
	einsteindb.Client
	priority pb.CommandPri
	mu       struct {
		sync.RWMutex
		checkPrio bool
	}
}

func (c *checkPrioClient) setCheckPriority(priority pb.CommandPri) {
	atomic.StoreInt32((*int32)(&c.priority), int32(priority))
}

func (c *checkPrioClient) getCheckPriority() pb.CommandPri {
	return (pb.CommandPri)(atomic.LoadInt32((*int32)(&c.priority)))
}

func (c *checkPrioClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	c.mu.RLock()
	defer func() {
		c.mu.RUnlock()
	}()
	if c.mu.checkPrio {
		switch req.Type {
		case einsteindbrpc.CmdCop:
			if c.getCheckPriority() != req.Priority {
				return nil, errors.New("fail to set priority")
			}
		}
	}
	return resp, err
}

type seqTestSuite1 struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	cli         *checkPrioClient
}

func (s *seqTestSuite1) SetUpSuite(c *C) {
	cli := &checkPrioClient{}
	hijackClient := func(c einsteindb.Client) einsteindb.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *seqTestSuite1) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *seqTestSuite1) TestCoprocessorPriority(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t (id int primary key)")
	tk.MustInterDirc("create causet t1 (id int, v int, unique index i_id (id))")
	defer tk.MustInterDirc("drop causet t")
	defer tk.MustInterDirc("drop causet t1")
	tk.MustInterDirc("insert into t values (1)")

	// Insert some data to make sure plan build IndexLookup for t1.
	for i := 0; i < 10; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
	}

	cli := s.cli
	cli.mu.Lock()
	cli.mu.checkPrio = true
	cli.mu.Unlock()

	cli.setCheckPriority(pb.CommandPri_High)
	tk.MustQuery("select id from t where id = 1")
	tk.MustQuery("select * from t1 where id = 1")

	cli.setCheckPriority(pb.CommandPri_Normal)
	tk.MustQuery("select count(*) from t")
	tk.MustInterDirc("uFIDelate t set id = 3")
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t select * from t limit 2")
	tk.MustInterDirc("delete from t")

	// Insert some data to make sure plan build IndexLookup for t.
	tk.MustInterDirc("insert into t values (1), (2)")

	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.Log.ExpensiveThreshold = 0
	})

	cli.setCheckPriority(pb.CommandPri_High)
	tk.MustQuery("select id from t where id = 1")
	tk.MustQuery("select * from t1 where id = 1")
	tk.MustInterDirc("delete from t where id = 2")
	tk.MustInterDirc("uFIDelate t set id = 2 where id = 1")

	cli.setCheckPriority(pb.CommandPri_Low)
	tk.MustQuery("select count(*) from t")
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t values (3)")

	// Test priority specified by ALLEGROALLEGROSQL memex.
	cli.setCheckPriority(pb.CommandPri_High)
	tk.MustQuery("select HIGH_PRIORITY * from t")

	cli.setCheckPriority(pb.CommandPri_Low)
	tk.MustQuery("select LOW_PRIORITY id from t where id = 1")

	cli.setCheckPriority(pb.CommandPri_High)
	tk.MustInterDirc("set milevadb_force_priority = 'HIGH_PRIORITY'")
	tk.MustQuery("select * from t").Check(testkit.Events("3"))
	tk.MustInterDirc("uFIDelate t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Events("0", "1"))

	cli.setCheckPriority(pb.CommandPri_Low)
	tk.MustInterDirc("set milevadb_force_priority = 'LOW_PRIORITY'")
	tk.MustQuery("select * from t").Check(testkit.Events("4"))
	tk.MustInterDirc("uFIDelate t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Events("0", "1"))

	cli.setCheckPriority(pb.CommandPri_Normal)
	tk.MustInterDirc("set milevadb_force_priority = 'DELAYED'")
	tk.MustQuery("select * from t").Check(testkit.Events("5"))
	tk.MustInterDirc("uFIDelate t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Events("0", "1"))

	cli.setCheckPriority(pb.CommandPri_Low)
	tk.MustInterDirc("set milevadb_force_priority = 'NO_PRIORITY'")
	tk.MustQuery("select * from t").Check(testkit.Events("6"))
	tk.MustInterDirc("uFIDelate t set id = id + 1")
	tk.MustQuery("select v from t1 where id = 0 or id = 1").Check(testkit.Events("0", "1"))

	cli.mu.Lock()
	cli.mu.checkPrio = false
	cli.mu.Unlock()
}

func (s *seqTestSuite) TestShowForNewDefCauslations(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	expectEvents := testkit.Events(
		"ascii_bin ascii 65 Yes Yes 1",
		"binary binary 63 Yes Yes 1",
		"latin1_bin latin1 47 Yes Yes 1",
		"utf8_bin utf8 83 Yes Yes 1",
		"utf8_general_ci utf8 33  Yes 1",
		"utf8_unicode_ci utf8 192  Yes 1",
		"utf8mb4_bin utf8mb4 46 Yes Yes 1",
		"utf8mb4_general_ci utf8mb4 45  Yes 1",
		"utf8mb4_unicode_ci utf8mb4 224  Yes 1",
	)
	tk.MustQuery("show defCauslation").Check(expectEvents)
	tk.MustQuery("select * from information_schema.COLLATIONS").Check(expectEvents)
}

func (s *seqTestSuite) TestForbidUnsupportedDefCauslations(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.causetstore)
	mustGetUnsupportedDefCauslation := func(allegrosql string, defCausl string) {
		tk.MustGetErrMsg(allegrosql, fmt.Sprintf("[dbs:1273]Unsupported defCauslation when new defCauslation is enabled: '%s'", defCausl))
	}

	mustGetUnsupportedDefCauslation("select 'a' defCauslate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("select cast('a' as char) defCauslate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("set names utf8 defCauslate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("set stochastik defCauslation_server = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("set stochastik defCauslation_database = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("set stochastik defCauslation_connection = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("set global defCauslation_server = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("set global defCauslation_database = 'utf8_roman_ci'", "utf8_roman_ci")
	mustGetUnsupportedDefCauslation("set global defCauslation_connection = 'utf8_roman_ci'", "utf8_roman_ci")
}

func (s *seqTestSuite) TestAutoIncIDInRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (id int not null auto_increment primary key)")

	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values ()")
	tk.MustInterDirc("insert into t values (),()")
	tk.MustInterDirc("insert into t values ()")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/stochastik/mockCommitRetryForAutoIncID", `return(true)`), IsNil)
	tk.MustInterDirc("commit")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/stochastik/mockCommitRetryForAutoIncID"), IsNil)

	tk.MustInterDirc("insert into t values ()")
	tk.MustQuery(`select * from t`).Check(testkit.Events("1", "2", "3", "4", "5"))
}

func (s *seqTestSuite) TestAutoRandIDRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	solitonutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer solitonutil.ConfigTestUtils.RestoreAutoRandomTestConfig()
	tk.MustInterDirc("create database if not exists auto_random_retry")
	tk.MustInterDirc("use auto_random_retry")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id bigint auto_random(3) primary key)")

	extractMaskedOrderedHandles := func() []int64 {
		handles, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "auto_random_retry", "t")
		c.Assert(err, IsNil)
		return solitonutil.ConfigTestUtils.MaskSortHandles(handles, 3, allegrosql.TypeLong)
	}

	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("set @@milevadb_retry_limit = 10")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values ()")
	tk.MustInterDirc("insert into t values (),()")
	tk.MustInterDirc("insert into t values ()")

	stochastik.ResetMockAutoRandIDRetryCount(5)
	fpName := "github.com/whtcorpsinc/milevadb/stochastik/mockCommitRetryForAutoRandID"
	c.Assert(failpoint.Enable(fpName, `return(true)`), IsNil)
	tk.MustInterDirc("commit")
	c.Assert(failpoint.Disable(fpName), IsNil)
	tk.MustInterDirc("insert into t values ()")
	maskedHandles := extractMaskedOrderedHandles()
	c.Assert(maskedHandles, DeepEquals, []int64{1, 2, 3, 4, 5})

	stochastik.ResetMockAutoRandIDRetryCount(11)
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values ()")
	c.Assert(failpoint.Enable(fpName, `return(true)`), IsNil)
	// Insertion failure will skip the 6 in retryInfo.
	tk.MustGetErrCode("commit", errno.ErrTxnRetryable)
	c.Assert(failpoint.Disable(fpName), IsNil)

	tk.MustInterDirc("insert into t values ()")
	maskedHandles = extractMaskedOrderedHandles()
	c.Assert(maskedHandles, DeepEquals, []int64{1, 2, 3, 4, 5, 7})
}

func (s *seqTestSuite) TestAutoRandRecoverBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	solitonutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer solitonutil.ConfigTestUtils.RestoreAutoRandomTestConfig()
	tk.MustInterDirc("create database if not exists test_recover")
	tk.MustInterDirc("use test_recover")
	tk.MustInterDirc("drop causet if exists t_recover_auto_rand")
	defer func(originGC bool) {
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// Disable emulator GC.
	// Otherwise emulator GC will delete causet record as soon as possible after execute drop causet dbs.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`

	// Set GC safe point.
	tk.MustInterDirc(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))
	err := gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange"), IsNil)
	}()
	const autoRandIDStep = 5000
	stp := autoid.GetStep()
	autoid.SetStep(autoRandIDStep)
	defer autoid.SetStep(stp)

	// Check rebase auto_random id.
	tk.MustInterDirc("create causet t_recover_auto_rand (a bigint auto_random(5) primary key);")
	tk.MustInterDirc("insert into t_recover_auto_rand values (),(),()")
	tk.MustInterDirc("drop causet t_recover_auto_rand")
	tk.MustInterDirc("recover causet t_recover_auto_rand")
	tk.MustInterDirc("insert into t_recover_auto_rand values (),(),()")
	hs, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test_recover", "t_recover_auto_rand")
	c.Assert(err, IsNil)
	ordered := solitonutil.ConfigTestUtils.MaskSortHandles(hs, 5, allegrosql.TypeLong)

	c.Assert(ordered, DeepEquals, []int64{1, 2, 3, autoRandIDStep + 1, autoRandIDStep + 2, autoRandIDStep + 3})
}

func (s *seqTestSuite) TestMaxDeltaSchemaCount(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(variable.DefMilevaDBMaxDeltaSchemaCount))
	gvc := petri.GetPetri(tk.Se).GetGlobalVarsCache()
	gvc.Disable()

	tk.MustInterDirc("set @@global.milevadb_max_delta_schema_count= -1")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect milevadb_max_delta_schema_count value: '-1'"))
	// Make sure a new stochastik will load global variables.
	tk.Se = nil
	tk.MustInterDirc("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(100))
	tk.MustInterDirc(fmt.Sprintf("set @@global.milevadb_max_delta_schema_count= %v", uint64(math.MaxInt64)))
	tk.MustQuery("show warnings;").Check(testkit.Events(fmt.Sprintf("Warning 1292 Truncated incorrect milevadb_max_delta_schema_count value: '%d'", uint64(math.MaxInt64))))
	tk.Se = nil
	tk.MustInterDirc("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(16384))
	_, err := tk.InterDirc("set @@global.milevadb_max_delta_schema_count= invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	tk.MustInterDirc("set @@global.milevadb_max_delta_schema_count= 2048")
	tk.Se = nil
	tk.MustInterDirc("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(2048))
	tk.MustQuery("select @@global.milevadb_max_delta_schema_count").Check(testkit.Events("2048"))
}

func (s *seqTestSuite) TestOOMPanicInHashJoinWhenFetchBuildEvents(c *C) {
	fpName := "github.com/whtcorpsinc/milevadb/interlock/errorFetchBuildSideEventsMockOOMPanic"
	c.Assert(failpoint.Enable(fpName, `panic("ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable(fpName), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(c1 int, c2 int)")
	tk.MustInterDirc("insert into t values(1,1),(2,2)")
	err := tk.QueryToErr("select * from t as t2  join t as t1 where t1.c1=t2.c1")
	c.Assert(err.Error(), Equals, "failpoint panic: ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")
}

func (s *seqTestSuite) TestIssue18744(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t, t1;`)
	tk.MustInterDirc(`CREATE TABLE t (
  id int(11) NOT NULL,
  a bigint(20) DEFAULT NULL,
  b char(20) DEFAULT NULL,
  c datetime DEFAULT NULL,
  d double DEFAULT NULL,
  e json DEFAULT NULL,
  f decimal(40,6) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY a (a),
  KEY b (b),
  KEY c (c),
  KEY d (d),
  KEY f (f)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustInterDirc(`CREATE TABLE t1 (
  id int(11) NOT NULL,
  a bigint(20) DEFAULT NULL,
  b char(20) DEFAULT NULL,
  c datetime DEFAULT NULL,
  d double DEFAULT NULL,
  e json DEFAULT NULL,
  f decimal(40,6) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY a (a),
  KEY b (b),
  KEY c (c),
  KEY d (d),
  KEY f (f)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustInterDirc(`insert into t1(id) values(0),(1),(2);`)
	tk.MustInterDirc(`insert into t values(0, 2010,  "2010-01-01 01:01:00" , "2010-01-01 01:01:00" , 2010 , 2010 , 2010.000000);`)
	tk.MustInterDirc(`insert into t values(1 , NULL , NULL                , NULL                , NULL , NULL ,        NULL);`)
	tk.MustInterDirc(`insert into t values(2 , 2012 , "2012-01-01 01:01:00" , "2012-01-01 01:01:00" , 2012 , 2012 , 2012.000000);`)
	tk.MustInterDirc(`set milevadb_index_lookup_join_concurrency=1`)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinOuterWorkerErr", "return"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinOuterWorkerErr"), IsNil)
	}()
	err := tk.QueryToErr(`select /*+ inl_hash_join(t2) */ t1.id, t2.id from t1 join t t2 on t1.a = t2.a order by t1.a ASC limit 1;`)
	c.Assert(err.Error(), Equals, "mocHoTTexHashJoinOuterWorkerErr")
}

func (s *seqTestSuite) TestIssue19410(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, t1, t2, t3;")
	tk.MustInterDirc("create causet t(a int, b enum('A', 'B'));")
	tk.MustInterDirc("create causet t1(a1 int, b1 enum('B', 'A') NOT NULL, UNIQUE KEY (b1));")
	tk.MustInterDirc("insert into t values (1, 'A');")
	tk.MustInterDirc("insert into t1 values (1, 'A');")
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Events("1 A 1 A"))
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t join t1 on t.b = t1.b1;").Check(testkit.Events("1 A 1 A"))

	tk.MustInterDirc("create causet t2(a1 int, b1 enum('C', 'D') NOT NULL, UNIQUE KEY (b1));")
	tk.MustInterDirc("insert into t2 values (1, 'C');")
	tk.MustQuery("select /*+ INL_HASH_JOIN(t2) */ * from t join t2 on t.b = t2.b1;").Check(testkit.Events())
	tk.MustQuery("select /*+ INL_JOIN(t2) */ * from t join t2 on t.b = t2.b1;").Check(testkit.Events())

	tk.MustInterDirc("create causet t3(a1 int, b1 enum('A', 'B') NOT NULL, UNIQUE KEY (b1));")
	tk.MustInterDirc("insert into t3 values (1, 'A');")
	tk.MustQuery("select /*+ INL_HASH_JOIN(t3) */ * from t join t3 on t.b = t3.b1;").Check(testkit.Events("1 A 1 A"))
	tk.MustQuery("select /*+ INL_JOIN(t3) */ * from t join t3 on t.b = t3.b1;").Check(testkit.Events("1 A 1 A"))
}
