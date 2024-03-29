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

package dbs_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/israce"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testIntegrationSuite1{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite2{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite3{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite4{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite5{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite6{&testIntegrationSuite{}})
var _ = SerialSuites(&testIntegrationSuite7{&testIntegrationSuite{}})

type testIntegrationSuite struct {
	lease       time.Duration
	cluster     cluster.Cluster
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	ctx         stochastikctx.Context
}

func setupIntegrationSuite(s *testIntegrationSuite, c *C) {
	var err error
	s.lease = 50 * time.Millisecond
	dbs.SetWaitTimeWhenErrorOccurred(0)

	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	stochastik.SetSchemaLease(s.lease)
	stochastik.DisableStats4Test()
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)

	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	s.ctx = se.(stochastikctx.Context)
	_, err = se.InterDircute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
}

func tearDownIntegrationSuiteTest(s *testIntegrationSuite, c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		tk.MustInterDirc(fmt.Sprintf("drop causet %v", blockName))
	}
}

func tearDownIntegrationSuite(s *testIntegrationSuite, c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	setupIntegrationSuite(s, c)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	tearDownIntegrationSuite(s, c)
}

type testIntegrationSuite1 struct{ *testIntegrationSuite }
type testIntegrationSuite2 struct{ *testIntegrationSuite }

func (s *testIntegrationSuite2) TearDownTest(c *C) {
	tearDownIntegrationSuiteTest(s.testIntegrationSuite, c)
}

type testIntegrationSuite3 struct{ *testIntegrationSuite }
type testIntegrationSuite4 struct{ *testIntegrationSuite }
type testIntegrationSuite5 struct{ *testIntegrationSuite }
type testIntegrationSuite6 struct{ *testIntegrationSuite }
type testIntegrationSuite7 struct{ *testIntegrationSuite }

func (s *testIntegrationSuite5) TestNoZeroDateMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	defer tk.MustInterDirc("set stochastik sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';")

	tk.MustInterDirc("use test;")
	tk.MustInterDirc("set stochastik sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION';")
	tk.MustGetErrCode("create causet test_zero_date(agent_start_time date NOT NULL DEFAULT '0000-00-00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create causet test_zero_date(agent_start_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create causet test_zero_date(agent_start_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create causet test_zero_date(a timestamp default '0000-00-00 00');", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create causet test_zero_date(a timestamp default 0);", errno.ErrInvalidDefault)
}

func (s *testIntegrationSuite2) TestInvalidDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("USE test;")

	_, err := tk.InterDirc("create causet t(c1 decimal default 1.7976931348623157E308)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))

	_, err = tk.InterDirc("create causet t( c1 varchar(2) default 'MilevaDB');")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))
}

// TestKeyWithoutLength for issue #13452
func (s testIntegrationSuite3) TestKeyWithoutLengthCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("USE test")

	_, err := tk.InterDirc("create causet t_without_length (a text primary key)")
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*BLOB/TEXT defCausumn 'a' used in key specification without a key length")
}

// TestInvalidNameWhenCreateTable for issue #3848
func (s *testIntegrationSuite3) TestInvalidNameWhenCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("USE test;")

	_, err := tk.InterDirc("create causet t(xxx.t.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, dbs.ErrWrongDBName), IsTrue, Commentf("err %v", err))

	_, err = tk.InterDirc("create causet t(test.tttt.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, dbs.ErrWrongTableName), IsTrue, Commentf("err %v", err))

	_, err = tk.InterDirc("create causet t(t.tttt.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, dbs.ErrWrongDBName), IsTrue, Commentf("err %v", err))
}

// TestCreateTableIfNotExists for issue #6879
func (s *testIntegrationSuite3) TestCreateTableIfNotExists(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("USE test;")

	tk.MustInterDirc("create causet ct1(a bigint)")
	tk.MustInterDirc("create causet ct(a bigint)")

	// Test duplicate create-causet with `LIKE` clause
	tk.MustInterDirc("create causet if not exists ct like ct1;")
	warnings := tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(schemareplicant.ErrTableExists, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
	c.Assert(lastWarn.Level, Equals, stmtctx.WarnLevelNote)

	// Test duplicate create-causet without `LIKE` clause
	tk.MustInterDirc("create causet if not exists ct(b bigint, c varchar(60));")
	warnings = tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(schemareplicant.ErrTableExists, lastWarn.Err), IsTrue)
}

// for issue #9910
func (s *testIntegrationSuite2) TestCreateTableWithKeyWord(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("USE test;")

	_, err := tk.InterDirc("create causet t1(pump varchar(20), drainer varchar(20), node_id varchar(20), node_state varchar(20));")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite1) TestUniqueKeyNullValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("USE test")
	tk.MustInterDirc("create causet t(a int primary key, b varchar(255))")

	tk.MustInterDirc("insert into t values(1, NULL)")
	tk.MustInterDirc("insert into t values(2, NULL)")
	tk.MustInterDirc("alter causet t add unique index b(b);")
	res := tk.MustQuery("select count(*) from t use index(b);")
	res.Check(testkit.Rows("2"))
	tk.MustInterDirc("admin check causet t")
	tk.MustInterDirc("admin check index t b")
}

func (s *testIntegrationSuite2) TestUniqueKeyNullValueClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("drop database if exists unique_null_val;")
	tk.MustInterDirc("create database unique_null_val;")
	tk.MustInterDirc("use unique_null_val;")
	tk.MustInterDirc("create causet t (a varchar(10), b float, c varchar(255), primary key (a, b));")
	tk.MustInterDirc("insert into t values ('1', 1, NULL);")
	tk.MustInterDirc("insert into t values ('2', 2, NULL);")
	tk.MustInterDirc("alter causet t add unique index c(c);")
	tk.MustQuery("select count(*) from t use index(c);").Check(testkit.Rows("2"))
	tk.MustInterDirc("admin check causet t;")
	tk.MustInterDirc("admin check index t c;")
}

// TestModifyDeferredCausetAfterAddIndex Issue 5134
func (s *testIntegrationSuite3) TestModifyDeferredCausetAfterAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet city (city VARCHAR(2) KEY);")
	tk.MustInterDirc("alter causet city change defCausumn city city varchar(50);")
	tk.MustInterDirc(`insert into city values ("abc"), ("abd");`)
}

func (s *testIntegrationSuite3) TestIssue2293(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t_issue_2293 (a int)")
	tk.MustGetErrCode("alter causet t_issue_2293 add b int not null default 'a'", errno.ErrInvalidDefault)
	tk.MustInterDirc("insert into t_issue_2293 value(1)")
	tk.MustQuery("select * from t_issue_2293").Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite2) TestIssue6101(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t1 (quantity decimal(2) unsigned);")
	_, err := tk.InterDirc("insert into t1 values (500), (-500), (~0), (-1);")
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(errno.ErrWarnDataOutOfRange))
	tk.MustInterDirc("drop causet t1")

	tk.MustInterDirc("set sql_mode=''")
	tk.MustInterDirc("create causet t1 (quantity decimal(2) unsigned);")
	tk.MustInterDirc("insert into t1 values (500), (-500), (~0), (-1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("99", "0", "99", "0"))
	tk.MustInterDirc("drop causet t1")
}

func (s *testIntegrationSuite2) TestIssue19229(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("CREATE TABLE enumt (type enum('a', 'b') );")
	_, err := tk.InterDirc("insert into enumt values('xxx');")
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(errno.WarnDataTruncated))
	_, err = tk.InterDirc("insert into enumt values(-1);")
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(errno.WarnDataTruncated))
	tk.MustInterDirc("drop causet enumt")

	tk.MustInterDirc("CREATE TABLE sett (type set('a', 'b') );")
	_, err = tk.InterDirc("insert into sett values('xxx');")
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(errno.WarnDataTruncated))
	_, err = tk.InterDirc("insert into sett values(-1);")
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, errors.ErrCode(errno.WarnDataTruncated))
	tk.MustInterDirc("drop causet sett")
}

func (s *testIntegrationSuite1) TestIndexLength(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet idx_len(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0))")
	tk.MustInterDirc("create index idx on idx_len(a)")
	tk.MustInterDirc("alter causet idx_len add index idxa(a)")
	tk.MustInterDirc("create index idx1 on idx_len(b)")
	tk.MustInterDirc("alter causet idx_len add index idxb(b)")
	tk.MustInterDirc("create index idx2 on idx_len(c)")
	tk.MustInterDirc("alter causet idx_len add index idxc(c)")
	tk.MustInterDirc("create index idx3 on idx_len(d)")
	tk.MustInterDirc("alter causet idx_len add index idxd(d)")
	tk.MustInterDirc("create index idx4 on idx_len(f)")
	tk.MustInterDirc("alter causet idx_len add index idxf(f)")
	tk.MustInterDirc("create index idx5 on idx_len(g)")
	tk.MustInterDirc("alter causet idx_len add index idxg(g)")
	tk.MustInterDirc("create causet idx_len1(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0), index(a), index(b), index(c), index(d), index(f), index(g))")
}

func (s *testIntegrationSuite3) TestIssue3833(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet issue3833 (b char(0), c binary(0), d  varchar(0))")
	tk.MustGetErrCode("create index idx on issue3833 (b)", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("alter causet issue3833 add index idx (b)", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("create causet issue3833_2 (b char(0), c binary(0), d varchar(0), index(b))", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("create index idx on issue3833 (c)", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("alter causet issue3833 add index idx (c)", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("create causet issue3833_2 (b char(0), c binary(0), d varchar(0), index(c))", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("create index idx on issue3833 (d)", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("alter causet issue3833 add index idx (d)", errno.ErrWrongKeyDeferredCauset)
	tk.MustGetErrCode("create causet issue3833_2 (b char(0), c binary(0), d varchar(0), index(d))", errno.ErrWrongKeyDeferredCauset)
}

func (s *testIntegrationSuite1) TestIssue2858And2717(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("create causet t_issue_2858_bit (a bit(64) default b'0')")
	tk.MustInterDirc("insert into t_issue_2858_bit value ()")
	tk.MustInterDirc(`insert into t_issue_2858_bit values (100), ('10'), ('\0')`)
	tk.MustQuery("select a+0 from t_issue_2858_bit").Check(testkit.Rows("0", "100", "12592", "0"))
	tk.MustInterDirc(`alter causet t_issue_2858_bit alter defCausumn a set default '\0'`)

	tk.MustInterDirc("create causet t_issue_2858_hex (a int default 0x123)")
	tk.MustInterDirc("insert into t_issue_2858_hex value ()")
	tk.MustInterDirc("insert into t_issue_2858_hex values (123), (0x321)")
	tk.MustQuery("select a from t_issue_2858_hex").Check(testkit.Rows("291", "123", "801"))
	tk.MustInterDirc(`alter causet t_issue_2858_hex alter defCausumn a set default 0x321`)
}

func (s *testIntegrationSuite1) TestIssue4432(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("create causet tx (defCaus bit(10) default 'a')")
	tk.MustInterDirc("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustInterDirc("drop causet tx")

	tk.MustInterDirc("create causet tx (defCaus bit(10) default 0x61)")
	tk.MustInterDirc("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustInterDirc("drop causet tx")

	tk.MustInterDirc("create causet tx (defCaus bit(10) default 97)")
	tk.MustInterDirc("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustInterDirc("drop causet tx")

	tk.MustInterDirc("create causet tx (defCaus bit(10) default 0b1100001)")
	tk.MustInterDirc("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustInterDirc("drop causet tx")
}

func (s *testIntegrationSuite1) TestIssue5092(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("create causet t_issue_5092 (a int)")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn (b int, c int)")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn if not exists (b int, c int)")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn b1 int after b, add defCausumn c1 int after c")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn d int after b, add defCausumn e int first, add defCausumn f int after c1, add defCausumn g int, add defCausumn h int first")
	tk.MustQuery("show create causet t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `h` int(11) DEFAULT NULL,\n" +
		"  `e` int(11) DEFAULT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `d` int(11) DEFAULT NULL,\n" +
		"  `b1` int(11) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `c1` int(11) DEFAULT NULL,\n" +
		"  `f` int(11) DEFAULT NULL,\n" +
		"  `g` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// The following two memexs are consistent with MariaDB.
	tk.MustGetErrCode("alter causet t_issue_5092 add defCausumn if not exists d int, add defCausumn d int", errno.ErrDupFieldName)
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn dd int, add defCausumn if not exists dd int")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn if not exists (d int, e int), add defCausumn ff text")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn b2 int after b1, add defCausumn c2 int first")
	tk.MustQuery("show create causet t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `c2` int(11) DEFAULT NULL,\n" +
		"  `h` int(11) DEFAULT NULL,\n" +
		"  `e` int(11) DEFAULT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `d` int(11) DEFAULT NULL,\n" +
		"  `b1` int(11) DEFAULT NULL,\n" +
		"  `b2` int(11) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `c1` int(11) DEFAULT NULL,\n" +
		"  `f` int(11) DEFAULT NULL,\n" +
		"  `g` int(11) DEFAULT NULL,\n" +
		"  `dd` int(11) DEFAULT NULL,\n" +
		"  `ff` text DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("drop causet t_issue_5092")

	tk.MustInterDirc("create causet t_issue_5092 (a int default 1)")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn (b int default 2, c int default 3)")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn b1 int default 22 after b, add defCausumn c1 int default 33 after c")
	tk.MustInterDirc("insert into t_issue_5092 value ()")
	tk.MustQuery("select * from t_issue_5092").Check(testkit.Rows("1 2 22 3 33"))
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn d int default 4 after c1, add defCausumn aa int default 0 first")
	tk.MustQuery("select * from t_issue_5092").Check(testkit.Rows("0 1 2 22 3 33 4"))
	tk.MustQuery("show create causet t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `aa` int(11) DEFAULT 0,\n" +
		"  `a` int(11) DEFAULT 1,\n" +
		"  `b` int(11) DEFAULT 2,\n" +
		"  `b1` int(11) DEFAULT 22,\n" +
		"  `c` int(11) DEFAULT 3,\n" +
		"  `c1` int(11) DEFAULT 33,\n" +
		"  `d` int(11) DEFAULT 4\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("drop causet t_issue_5092")

	tk.MustInterDirc("create causet t_issue_5092 (a int)")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn (b int, c int)")
	tk.MustInterDirc("alter causet t_issue_5092 drop defCausumn b,drop defCausumn c")
	tk.MustGetErrCode("alter causet t_issue_5092 drop defCausumn c, drop defCausumn c", errno.ErrCantDropFieldOrKey)
	tk.MustInterDirc("alter causet t_issue_5092 drop defCausumn if exists b,drop defCausumn if exists c")
	tk.MustGetErrCode("alter causet t_issue_5092 drop defCausumn g, drop defCausumn d", errno.ErrCantDropFieldOrKey)
	tk.MustInterDirc("drop causet t_issue_5092")

	tk.MustInterDirc("create causet t_issue_5092 (a int)")
	tk.MustInterDirc("alter causet t_issue_5092 add defCausumn (b int, c int)")
	tk.MustGetErrCode("alter causet t_issue_5092 drop defCausumn if exists a, drop defCausumn b, drop defCausumn c", errno.ErrCantRemoveAllFields)
	tk.MustGetErrCode("alter causet t_issue_5092 drop defCausumn if exists c, drop defCausumn c", errno.ErrCantDropFieldOrKey)
	tk.MustInterDirc("alter causet t_issue_5092 drop defCausumn c, drop defCausumn if exists c")
	tk.MustInterDirc("drop causet t_issue_5092")
}

func (s *testIntegrationSuite5) TestErrnoErrorCode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")

	// create database
	allegrosql := "create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tk.MustGetErrCode(allegrosql, errno.ErrTooLongIdent)
	allegrosql = "create database test"
	tk.MustGetErrCode(allegrosql, errno.ErrDBCreateExists)
	allegrosql = "create database test1 character set uft8;"
	tk.MustGetErrCode(allegrosql, errno.ErrUnknownCharacterSet)
	allegrosql = "create database test2 character set gkb;"
	tk.MustGetErrCode(allegrosql, errno.ErrUnknownCharacterSet)
	allegrosql = "create database test3 character set laitn1;"
	tk.MustGetErrCode(allegrosql, errno.ErrUnknownCharacterSet)
	// drop database
	allegrosql = "drop database db_not_exist"
	tk.MustGetErrCode(allegrosql, errno.ErrDBDropExists)
	// create causet
	tk.MustInterDirc("create causet test_error_code_succ (c1 int, c2 int, c3 int, primary key(c3))")
	allegrosql = "create causet test_error_code_succ (c1 int, c2 int, c3 int)"
	tk.MustGetErrCode(allegrosql, errno.ErrTableExists)
	allegrosql = "create causet test_error_code1 (c1 int, c2 int, c2 int)"
	tk.MustGetErrCode(allegrosql, errno.ErrDupFieldName)
	allegrosql = "create causet test_error_code1 (c1 int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	tk.MustGetErrCode(allegrosql, errno.ErrTooLongIdent)
	allegrosql = "create causet test_error_code1 (c1 int, `_milevadb_rowid` int)"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDeferredCausetName)
	allegrosql = "create causet aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa(a int)"
	tk.MustGetErrCode(allegrosql, errno.ErrTooLongIdent)
	allegrosql = "create causet test_error_code1 (c1 int, c2 int, key aa (c1, c2), key aa (c1))"
	tk.MustGetErrCode(allegrosql, errno.ErrDupKeyName)
	allegrosql = "create causet test_error_code1 (c1 int, c2 int, c3 int, key(c_not_exist))"
	tk.MustGetErrCode(allegrosql, errno.ErrKeyDeferredCausetDoesNotExits)
	allegrosql = "create causet test_error_code1 (c1 int, c2 int, c3 int, primary key(c_not_exist))"
	tk.MustGetErrCode(allegrosql, errno.ErrKeyDeferredCausetDoesNotExits)
	allegrosql = "create causet test_error_code1 (c1 int not null default '')"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidDefault)
	allegrosql = "CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT 2.0 DEFAULT now());"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidDefault)
	allegrosql = "CREATE TABLE `t` (`a` double DEFAULT now());"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidDefault)
	allegrosql = "create causet t1(a int) character set uft8;"
	tk.MustGetErrCode(allegrosql, errno.ErrUnknownCharacterSet)
	allegrosql = "create causet t1(a int) character set gkb;"
	tk.MustGetErrCode(allegrosql, errno.ErrUnknownCharacterSet)
	allegrosql = "create causet t1(a int) character set laitn1;"
	tk.MustGetErrCode(allegrosql, errno.ErrUnknownCharacterSet)
	allegrosql = "create causet test_error_code (a int not null ,b int not null,c int not null, d int not null, foreign key (b, c) references product(id));"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongFkDef)
	allegrosql = "create causet test_error_code_2;"
	tk.MustGetErrCode(allegrosql, errno.ErrTableMustHaveDeferredCausets)
	allegrosql = "create causet test_error_code_2 (unique(c1));"
	tk.MustGetErrCode(allegrosql, errno.ErrTableMustHaveDeferredCausets)
	allegrosql = "create causet test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), primary key(c2));"
	tk.MustGetErrCode(allegrosql, errno.ErrMultiplePriKey)
	allegrosql = "create causet test_error_code_3(pt blob ,primary key (pt));"
	tk.MustGetErrCode(allegrosql, errno.ErrBlobKeyWithoutLength)
	allegrosql = "create causet test_error_code_3(a text, unique (a(3073)));"
	tk.MustGetErrCode(allegrosql, errno.ErrTooLongKey)
	allegrosql = "create causet test_error_code_3(`id` int, key `primary`(`id`));"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongNameForIndex)
	allegrosql = "create causet t2(c1.c2 blob default null);"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongTableName)
	allegrosql = "create causet t2 (id int default null primary key , age int);"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidDefault)
	allegrosql = "create causet t2 (id int null primary key , age int);"
	tk.MustGetErrCode(allegrosql, errno.ErrPrimaryCantHaveNull)
	allegrosql = "create causet t2 (id int default null, age int, primary key(id));"
	tk.MustGetErrCode(allegrosql, errno.ErrPrimaryCantHaveNull)
	allegrosql = "create causet t2 (id int null, age int, primary key(id));"
	tk.MustGetErrCode(allegrosql, errno.ErrPrimaryCantHaveNull)
	allegrosql = "create causet t2 (id int auto_increment);"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongAutoKey)
	allegrosql = "create causet t2 (id int auto_increment, a int key);"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongAutoKey)
	allegrosql = "create causet t2 (a datetime(2) default current_timestamp(3));"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidDefault)
	allegrosql = "create causet t2 (a datetime(2) default current_timestamp(2) on uFIDelate current_timestamp);"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidOnUFIDelate)
	allegrosql = "create causet t2 (a datetime default current_timestamp on uFIDelate current_timestamp(2));"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidOnUFIDelate)
	allegrosql = "create causet t2 (a datetime(2) default current_timestamp(2) on uFIDelate current_timestamp(3));"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidOnUFIDelate)
	allegrosql = "create causet t(a blob(10), index(a(0)));"
	tk.MustGetErrCode(allegrosql, errno.ErrKeyPart0)
	allegrosql = "create causet t(a char(10), index(a(0)));"
	tk.MustGetErrCode(allegrosql, errno.ErrKeyPart0)

	allegrosql = "create causet t2 (id int primary key , age int);"
	tk.MustInterDirc(allegrosql)

	// add defCausumn
	allegrosql = "alter causet test_error_code_succ add defCausumn c1 int"
	tk.MustGetErrCode(allegrosql, errno.ErrDupFieldName)
	allegrosql = "alter causet test_error_code_succ add defCausumn aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int"
	tk.MustGetErrCode(allegrosql, errno.ErrTooLongIdent)
	allegrosql = "alter causet test_comment comment 'test comment'"
	tk.MustGetErrCode(allegrosql, errno.ErrNoSuchTable)
	allegrosql = "alter causet test_error_code_succ add defCausumn `a ` int ;"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDeferredCausetName)
	allegrosql = "alter causet test_error_code_succ add defCausumn `_milevadb_rowid` int ;"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDeferredCausetName)
	tk.MustInterDirc("create causet test_on_uFIDelate (c1 int, c2 int);")
	allegrosql = "alter causet test_on_uFIDelate add defCausumn c3 int on uFIDelate current_timestamp;"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidOnUFIDelate)
	allegrosql = "create causet test_on_uFIDelate_2(c int on uFIDelate current_timestamp);"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidOnUFIDelate)

	// add defCausumns
	allegrosql = "alter causet test_error_code_succ add defCausumn c1 int, add defCausumn c1 int"
	tk.MustGetErrCode(allegrosql, errno.ErrDupFieldName)
	allegrosql = "alter causet test_error_code_succ add defCausumn (aa int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	tk.MustGetErrCode(allegrosql, errno.ErrTooLongIdent)
	allegrosql = "alter causet test_error_code_succ add defCausumn `a ` int, add defCausumn `b ` int;"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDeferredCausetName)
	tk.MustInterDirc("create causet test_add_defCausumns_on_uFIDelate (c1 int, c2 int);")
	allegrosql = "alter causet test_add_defCausumns_on_uFIDelate add defCausumn cc int, add defCausumn c3 int on uFIDelate current_timestamp;"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidOnUFIDelate)

	// drop defCausumn
	allegrosql = "alter causet test_error_code_succ drop c_not_exist"
	tk.MustGetErrCode(allegrosql, errno.ErrCantDropFieldOrKey)
	tk.MustInterDirc("create causet test_drop_defCausumn (c1 int );")
	allegrosql = "alter causet test_drop_defCausumn drop defCausumn c1;"
	tk.MustGetErrCode(allegrosql, errno.ErrCantRemoveAllFields)
	// drop defCausumns
	allegrosql = "alter causet test_error_code_succ drop c_not_exist, drop cc_not_exist"
	tk.MustGetErrCode(allegrosql, errno.ErrCantDropFieldOrKey)
	tk.MustInterDirc("create causet test_drop_defCausumns (c1 int);")
	tk.MustInterDirc("alter causet test_drop_defCausumns add defCausumn c2 int first, add defCausumn c3 int after c1")
	allegrosql = "alter causet test_drop_defCausumns drop defCausumn c1, drop defCausumn c2, drop defCausumn c3;"
	tk.MustGetErrCode(allegrosql, errno.ErrCantRemoveAllFields)
	allegrosql = "alter causet test_drop_defCausumns drop defCausumn c1, add defCausumn c2 int;"
	tk.MustGetErrCode(allegrosql, errno.ErrUnsupportedDBSOperation)
	allegrosql = "alter causet test_drop_defCausumns drop defCausumn c1, drop defCausumn c1;"
	tk.MustGetErrCode(allegrosql, errno.ErrCantDropFieldOrKey)
	// add index
	allegrosql = "alter causet test_error_code_succ add index idx (c_not_exist)"
	tk.MustGetErrCode(allegrosql, errno.ErrKeyDeferredCausetDoesNotExits)
	tk.MustInterDirc("alter causet test_error_code_succ add index idx (c1)")
	allegrosql = "alter causet test_error_code_succ add index idx (c1)"
	tk.MustGetErrCode(allegrosql, errno.ErrDupKeyName)
	// drop index
	allegrosql = "alter causet test_error_code_succ drop index idx_not_exist"
	tk.MustGetErrCode(allegrosql, errno.ErrCantDropFieldOrKey)
	allegrosql = "alter causet test_error_code_succ drop defCausumn c3"
	tk.MustGetErrCode(allegrosql, errno.ErrUnsupportedDBSOperation)
	// modify defCausumn
	allegrosql = "alter causet test_error_code_succ modify testx.test_error_code_succ.c1 bigint"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDBName)
	allegrosql = "alter causet test_error_code_succ modify t.c1 bigint"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongTableName)
	allegrosql = "alter causet test_error_code_succ change c1 _milevadb_rowid bigint"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDeferredCausetName)
	allegrosql = "alter causet test_error_code_succ rename defCausumn c1 to _milevadb_rowid"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDeferredCausetName)
	// insert value
	tk.MustInterDirc("create causet test_error_code_null(c1 char(100) not null);")
	allegrosql = "insert into test_error_code_null (c1) values(null);"
	tk.MustGetErrCode(allegrosql, errno.ErrBadNull)
}

func (s *testIntegrationSuite3) TestTableDBSWithFloatType(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustGetErrCode("create causet t (a decimal(1, 2))", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("create causet t (a float(1, 2))", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("create causet t (a double(1, 2))", errno.ErrMBiggerThanD)
	tk.MustInterDirc("create causet t (a double(1, 1))")
	tk.MustGetErrCode("alter causet t add defCausumn b decimal(1, 2)", errno.ErrMBiggerThanD)
	// add multi defCausumns now not support, so no case.
	tk.MustGetErrCode("alter causet t modify defCausumn a float(1, 4)", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("alter causet t change defCausumn a aa float(1, 4)", errno.ErrMBiggerThanD)
	tk.MustInterDirc("drop causet t")
}

func (s *testIntegrationSuite1) TestTableDBSWithTimeType(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustGetErrCode("create causet t (a time(7))", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("create causet t (a datetime(7))", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("create causet t (a timestamp(7))", errno.ErrTooBigPrecision)
	_, err := tk.InterDirc("create causet t (a time(-1))")
	c.Assert(err, NotNil)
	tk.MustInterDirc("create causet t (a datetime)")
	tk.MustGetErrCode("alter causet t add defCausumn b time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t add defCausumn b datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t add defCausumn b timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t modify defCausumn a time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t modify defCausumn a datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t modify defCausumn a timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t change defCausumn a aa time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t change defCausumn a aa datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter causet t change defCausumn a aa timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustInterDirc("alter causet t change defCausumn a aa datetime(0)")
	tk.MustInterDirc("drop causet t")
}

func (s *testIntegrationSuite2) TestUFIDelateMultipleTable(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database umt_db")
	tk.MustInterDirc("use umt_db")
	tk.MustInterDirc("create causet t1 (c1 int, c2 int)")
	tk.MustInterDirc("insert t1 values (1, 1), (2, 2)")
	tk.MustInterDirc("create causet t2 (c1 int, c2 int)")
	tk.MustInterDirc("insert t2 values (1, 3), (2, 5)")
	ctx := tk.Se.(stochastikctx.Context)
	dom := petri.GetPetri(ctx)
	is := dom.SchemaReplicant()
	EDB, ok := is.SchemaByName(perceptron.NewCIStr("umt_db"))
	c.Assert(ok, IsTrue)
	t1Tbl, err := is.TableByName(perceptron.NewCIStr("umt_db"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t1Info := t1Tbl.Meta()

	// Add a new defCausumn in write only state.
	newDeferredCauset := &perceptron.DeferredCausetInfo{
		ID:                 100,
		Name:               perceptron.NewCIStr("c3"),
		Offset:             2,
		DefaultValue:       9,
		OriginDefaultValue: 9,
		FieldType:          *types.NewFieldType(allegrosql.TypeLonglong),
		State:              perceptron.StateWriteOnly,
	}
	t1Info.DeferredCausets = append(t1Info.DeferredCausets, newDeferredCauset)

	ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
		m := spacetime.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UFIDelateTable(EDB.ID, t1Info), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustInterDirc("uFIDelate t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1", "8 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 10", "2 10"))

	newDeferredCauset.State = perceptron.StatePublic

	ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
		m := spacetime.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UFIDelateTable(EDB.ID, t1Info), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1 9", "8 2 9"))
	tk.MustInterDirc("drop database umt_db")
}

func (s *testIntegrationSuite2) TestNullGeneratedDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("CREATE TABLE `t` (" +
		"`a` int(11) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) GENERATED ALWAYS AS (`a` + `b`) VIRTUAL," +
		"`h` varchar(10) DEFAULT NULL," +
		"`m` int(11) DEFAULT NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	tk.MustInterDirc("insert into t values()")
	tk.MustInterDirc("alter causet t add index idx_c(c)")
	tk.MustInterDirc("drop causet t")
}

func (s *testIntegrationSuite2) TestDependedGeneratedDeferredCausetPrior2GeneratedDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("CREATE TABLE `t` (" +
		"`a` int(11) DEFAULT NULL," +
		"`b` int(11) GENERATED ALWAYS AS (`a` + 1) VIRTUAL," +
		"`c` int(11) GENERATED ALWAYS AS (`b` + 1) VIRTUAL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	// should check unknown defCausumn first, then the prior ones.
	allegrosql := "alter causet t add defCausumn d int as (c + f + 1) first"
	tk.MustGetErrCode(allegrosql, errno.ErrBadField)

	// depended generated defCausumn should be prior to generated defCausumn self
	allegrosql = "alter causet t add defCausumn d int as (c+1) first"
	tk.MustGetErrCode(allegrosql, errno.ErrGeneratedDeferredCausetNonPrior)

	// correct case
	tk.MustInterDirc("alter causet t add defCausumn d int as (c+1) after c")

	// check position nil case
	tk.MustInterDirc("alter causet t add defCausumn(e int as (c+1))")
}

func (s *testIntegrationSuite3) TestChangingCharsetToUtf8(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t1(a varchar(20) charset utf8)")
	tk.MustInterDirc("insert into t1 values (?)", "t1_value")
	tk.MustInterDirc("alter causet t1 defCauslate uTf8mB4_uNiCoDe_Ci charset Utf8mB4 charset uTF8Mb4 defCauslate UTF8MB4_BiN")
	tk.MustInterDirc("alter causet t1 modify defCausumn a varchar(20) charset utf8mb4")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("t1_value"))

	tk.MustInterDirc("create causet t(a varchar(20) charset latin1)")
	tk.MustInterDirc("insert into t values (?)", "t_value")

	tk.MustInterDirc("alter causet t modify defCausumn a varchar(20) charset latin1")
	tk.MustQuery("select * from t;").Check(testkit.Rows("t_value"))

	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(20) charset utf8", errno.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(20) charset utf8mb4", errno.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(20) charset utf8 defCauslate utf8_bin", errno.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(20) charset utf8mb4 defCauslate utf8mb4_general_ci", errno.ErrUnsupportedDBSOperation)

	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(20) charset utf8mb4 defCauslate utf8bin", errno.ErrUnknownDefCauslation)
	tk.MustGetErrCode("alter causet t defCauslate LATIN1_GENERAL_CI charset utf8 defCauslate utf8_bin", errno.ErrConflictingDeclarations)
	tk.MustGetErrCode("alter causet t defCauslate LATIN1_GENERAL_CI defCauslate UTF8MB4_UNICODE_ci defCauslate utf8_bin", errno.ErrDefCauslationCharsetMismatch)
}

func (s *testIntegrationSuite4) TestChangingTableCharset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("USE test")
	tk.MustInterDirc("create causet t(a char(10)) charset latin1 defCauslate latin1_bin")

	tk.MustGetErrCode("alter causet t charset gbk", errno.ErrUnknownCharacterSet)
	tk.MustGetErrCode("alter causet t charset ''", errno.ErrUnknownCharacterSet)

	tk.MustGetErrCode("alter causet t charset utf8mb4 defCauslate '' defCauslate utf8mb4_bin;", errno.ErrUnknownDefCauslation)

	tk.MustGetErrCode("alter causet t charset utf8 defCauslate latin1_bin", errno.ErrDefCauslationCharsetMismatch)
	tk.MustGetErrCode("alter causet t charset utf8 defCauslate utf8mb4_bin;", errno.ErrDefCauslationCharsetMismatch)
	tk.MustGetErrCode("alter causet t charset utf8 defCauslate utf8_bin defCauslate utf8mb4_bin defCauslate utf8_bin;", errno.ErrDefCauslationCharsetMismatch)

	tk.MustGetErrCode("alter causet t charset utf8", errno.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t charset utf8mb4", errno.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t charset utf8mb4 defCauslate utf8mb4_bin", errno.ErrUnsupportedDBSOperation)

	tk.MustGetErrCode("alter causet t charset latin1 charset utf8 charset utf8mb4 defCauslate utf8_bin;", errno.ErrConflictingDeclarations)

	// Test change defCausumn charset when changing causet charset.
	tk.MustInterDirc("drop causet t;")
	tk.MustInterDirc("create causet t(a varchar(10)) charset utf8")
	tk.MustInterDirc("alter causet t convert to charset utf8mb4;")
	checkCharset := func(chs, defCausl string) {
		tbl := testGetTableByName(c, s.ctx, "test", "t")
		c.Assert(tbl, NotNil)
		c.Assert(tbl.Meta().Charset, Equals, chs)
		c.Assert(tbl.Meta().DefCauslate, Equals, defCausl)
		for _, defCaus := range tbl.Meta().DeferredCausets {
			c.Assert(defCaus.Charset, Equals, chs)
			c.Assert(defCaus.DefCauslate, Equals, defCausl)
		}
	}
	checkCharset(charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4)

	// Test when defCausumn charset can not convert to the target charset.
	tk.MustInterDirc("drop causet t;")
	tk.MustInterDirc("create causet t(a varchar(10) character set ascii) charset utf8mb4")
	tk.MustGetErrCode("alter causet t convert to charset utf8mb4;", errno.ErrUnsupportedDBSOperation)

	tk.MustInterDirc("drop causet t;")
	tk.MustInterDirc("create causet t(a varchar(10) character set utf8) charset utf8")
	tk.MustInterDirc("alter causet t convert to charset utf8 defCauslate utf8_general_ci;")
	checkCharset(charset.CharsetUTF8, "utf8_general_ci")

	// Test when causet charset is equal to target charset but defCausumn charset is not equal.
	tk.MustInterDirc("drop causet t;")
	tk.MustInterDirc("create causet t(a varchar(10) character set utf8) charset utf8mb4")
	tk.MustInterDirc("alter causet t convert to charset utf8mb4 defCauslate utf8mb4_general_ci;")
	checkCharset(charset.CharsetUTF8MB4, "utf8mb4_general_ci")

	// Mock causet info with charset is "". Old MilevaDB maybe create causet with charset is "".
	EDB, ok := petri.GetPetri(s.ctx).SchemaReplicant().SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	tbl := testGetTableByName(c, s.ctx, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Charset = ""
	tblInfo.DefCauslate = ""
	uFIDelateTableInfo := func(tblInfo *perceptron.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.CausetStore = s.causetstore
		err := mockCtx.NewTxn(context.Background())
		c.Assert(err, IsNil)
		txn, err := mockCtx.Txn(true)
		c.Assert(err, IsNil)
		mt := spacetime.NewMeta(txn)

		err = mt.UFIDelateTable(EDB.ID, tblInfo)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
	uFIDelateTableInfo(tblInfo)

	// check causet charset is ""
	tk.MustInterDirc("alter causet t add defCausumn b varchar(10);") //  load latest schemaReplicant.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Charset, Equals, "")
	c.Assert(tbl.Meta().DefCauslate, Equals, "")
	// Test when causet charset is "", this for compatibility.
	tk.MustInterDirc("alter causet t convert to charset utf8mb4;")
	checkCharset(charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4)

	// Test when defCausumn charset is "".
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.DeferredCausets[0].Charset = ""
	tblInfo.DeferredCausets[0].DefCauslate = ""
	uFIDelateTableInfo(tblInfo)
	// check causet charset is ""
	tk.MustInterDirc("alter causet t drop defCausumn b;") //  load latest schemaReplicant.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().DeferredCausets[0].Charset, Equals, "")
	c.Assert(tbl.Meta().DeferredCausets[0].DefCauslate, Equals, "")
	tk.MustInterDirc("alter causet t convert to charset utf8mb4;")
	checkCharset(charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4)

	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("create causet t (a blob) character set utf8;")
	tk.MustInterDirc("alter causet t charset=utf8mb4 defCauslate=utf8mb4_bin;")
	tk.MustQuery("show create causet t").Check(solitonutil.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` blob DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("create causet t(a varchar(5) charset utf8) charset utf8")
	tk.MustInterDirc("alter causet t charset utf8mb4")
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Charset, Equals, "utf8mb4")
	c.Assert(tbl.Meta().DefCauslate, Equals, "utf8mb4_bin")
	for _, defCaus := range tbl.Meta().DeferredCausets {
		// DeferredCauset charset and defCauslate should remain unchanged.
		c.Assert(defCaus.Charset, Equals, "utf8")
		c.Assert(defCaus.DefCauslate, Equals, "utf8_bin")
	}

	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("create causet t(a varchar(5) charset utf8 defCauslate utf8_unicode_ci) charset utf8 defCauslate utf8_unicode_ci")
	tk.MustInterDirc("alter causet t defCauslate utf8_danish_ci")
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Charset, Equals, "utf8")
	c.Assert(tbl.Meta().DefCauslate, Equals, "utf8_danish_ci")
	for _, defCaus := range tbl.Meta().DeferredCausets {
		c.Assert(defCaus.Charset, Equals, "utf8")
		// DeferredCauset defCauslate should remain unchanged.
		c.Assert(defCaus.DefCauslate, Equals, "utf8_unicode_ci")
	}
}

func (s *testIntegrationSuite5) TestModifyDeferredCausetOption(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test")
	tk.MustInterDirc("use test")

	errMsg := "[dbs:8200]" // unsupported modify defCausumn with references
	assertErrCode := func(allegrosql string, errCodeStr string) {
		_, err := tk.InterDirc(allegrosql)
		c.Assert(err, NotNil)
		c.Assert(err.Error()[:len(errCodeStr)], Equals, errCodeStr)
	}

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (b char(1) default null) engine=InnoDB default charset=utf8mb4 defCauslate=utf8mb4_general_ci")
	tk.MustInterDirc("alter causet t1 modify defCausumn b char(1) character set utf8mb4 defCauslate utf8mb4_general_ci")

	tk.MustInterDirc("drop causet t1")
	tk.MustInterDirc("create causet t1 (b char(1) defCauslate utf8mb4_general_ci)")
	tk.MustInterDirc("alter causet t1 modify b char(1) character set utf8mb4 defCauslate utf8mb4_general_ci")

	tk.MustInterDirc("drop causet t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1 (a int(11) default null)")
	tk.MustInterDirc("create causet t2 (b char, c int)")
	assertErrCode("alter causet t2 modify defCausumn c int references t1(a)", errMsg)
	_, err := tk.InterDirc("alter causet t1 change a a varchar(16)")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: type varchar(16) not match origin int(11)")
	_, err = tk.InterDirc("alter causet t1 change a a varchar(10)")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: type varchar(10) not match origin int(11)")
	_, err = tk.InterDirc("alter causet t1 change a a datetime")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: type datetime not match origin int(11)")
	_, err = tk.InterDirc("alter causet t1 change a a int(11) unsigned")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: can't change unsigned integer to signed or vice versa, and milevadb_enable_change_defCausumn_type is false")
	_, err = tk.InterDirc("alter causet t2 change b b int(11) unsigned")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: type int(11) not match origin char(1)")
}

func (s *testIntegrationSuite4) TestIndexOnMultipleGeneratedDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("create database if not exists test")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int as (a + 1), c int as (b + 1))")
	tk.MustInterDirc("insert into t (a) values (1)")
	tk.MustInterDirc("create index idx on t (c)")
	tk.MustQuery("select * from t where c > 1").Check(testkit.Rows("1 2 3"))
	res := tk.MustQuery("select * from t use index(idx) where c > 1")
	tk.MustQuery("select * from t ignore index(idx) where c > 1").Check(res.Rows())
	tk.MustInterDirc("admin check causet t")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int as (a + 1), c int as (b + 1), d int as (c + 1))")
	tk.MustInterDirc("insert into t (a) values (1)")
	tk.MustInterDirc("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 3 4"))
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustInterDirc("admin check causet t")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a bigint, b decimal as (a+1), c varchar(20) as (b*2), d float as (a*23+b-1+length(c)))")
	tk.MustInterDirc("insert into t (a) values (1)")
	tk.MustInterDirc("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 4 25"))
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustInterDirc("admin check causet t")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a varchar(10), b float as (length(a)+123), c varchar(20) as (right(a, 2)), d float as (b+b-7+1-3+3*ASCII(c)))")
	tk.MustInterDirc("insert into t (a) values ('adorable')")
	tk.MustInterDirc("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("adorable 131 le 577")) // 131+131-7+1-3+3*108
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustInterDirc("admin check causet t")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a bigint, b decimal as (a), c int(10) as (a+b), d float as (a+b+c), e decimal as (a+b+c+d))")
	tk.MustInterDirc("insert into t (a) values (1)")
	tk.MustInterDirc("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 1 2 4 8"))
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustInterDirc("admin check causet t")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a bigint, b bigint as (a+1) virtual, c bigint as (b+1) virtual)")
	tk.MustInterDirc("alter causet t add index idx_b(b)")
	tk.MustInterDirc("alter causet t add index idx_c(c)")
	tk.MustInterDirc("insert into t(a) values(1)")
	tk.MustInterDirc("alter causet t add defCausumn(d bigint as (c+1) virtual)")
	tk.MustInterDirc("alter causet t add index idx_d(d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 3 4"))
	res = tk.MustQuery("select * from t use index(idx_d) where d > 2")
	tk.MustQuery("select * from t ignore index(idx_d) where d > 2").Check(res.Rows())
	tk.MustInterDirc("admin check causet t")
}

func (s *testIntegrationSuite2) TestCaseInsensitiveCharsetAndDefCauslate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("create database if not exists test_charset_defCauslate")
	defer tk.MustInterDirc("drop database test_charset_defCauslate")
	tk.MustInterDirc("use test_charset_defCauslate")
	tk.MustInterDirc("create causet t(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=UTF8_BIN;")
	tk.MustInterDirc("create causet t1(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=uTF8_BIN;")
	tk.MustInterDirc("create causet t2(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8 COLLATE=utf8_BIN;")
	tk.MustInterDirc("create causet t3(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_BIN;")
	tk.MustInterDirc("create causet t4(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_general_ci;")

	tk.MustInterDirc("create causet t5(a varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI;")
	tk.MustInterDirc("insert into t5 values ('特克斯和凯科斯群岛')")

	EDB, ok := petri.GetPetri(s.ctx).SchemaReplicant().SchemaByName(perceptron.NewCIStr("test_charset_defCauslate"))
	c.Assert(ok, IsTrue)
	tbl := testGetTableByName(c, s.ctx, "test_charset_defCauslate", "t5")
	tblInfo := tbl.Meta().Clone()
	c.Assert(tblInfo.Charset, Equals, "utf8mb4")
	c.Assert(tblInfo.DeferredCausets[0].Charset, Equals, "utf8mb4")

	tblInfo.Version = perceptron.TableInfoVersion2
	tblInfo.Charset = "UTF8MB4"

	uFIDelateTableInfo := func(tblInfo *perceptron.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.CausetStore = s.causetstore
		err := mockCtx.NewTxn(context.Background())
		c.Assert(err, IsNil)
		txn, err := mockCtx.Txn(true)
		c.Assert(err, IsNil)
		mt := spacetime.NewMeta(txn)
		c.Assert(ok, IsTrue)
		err = mt.UFIDelateTable(EDB.ID, tblInfo)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
	uFIDelateTableInfo(tblInfo)
	tk.MustInterDirc("alter causet t5 add defCausumn b varchar(10);") //  load latest schemaReplicant.

	tblInfo = testGetTableByName(c, s.ctx, "test_charset_defCauslate", "t5").Meta()
	c.Assert(tblInfo.Charset, Equals, "utf8mb4")
	c.Assert(tblInfo.DeferredCausets[0].Charset, Equals, "utf8mb4")

	// For perceptron.TableInfoVersion3, it is believed that all charsets / defCauslations are lower-cased, do not do case-convert
	tblInfo = tblInfo.Clone()
	tblInfo.Version = perceptron.TableInfoVersion3
	tblInfo.Charset = "UTF8MB4"
	uFIDelateTableInfo(tblInfo)
	tk.MustInterDirc("alter causet t5 add defCausumn c varchar(10);") //  load latest schemaReplicant.

	tblInfo = testGetTableByName(c, s.ctx, "test_charset_defCauslate", "t5").Meta()
	c.Assert(tblInfo.Charset, Equals, "UTF8MB4")
	c.Assert(tblInfo.DeferredCausets[0].Charset, Equals, "utf8mb4")
}

func (s *testIntegrationSuite3) TestZeroFillCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists abc;")
	tk.MustInterDirc("create causet abc(y year, z tinyint(10) zerofill, primary key(y));")
	is := s.dom.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("abc"))
	c.Assert(err, IsNil)
	var yearDefCaus, zDefCaus *perceptron.DeferredCausetInfo
	for _, defCaus := range tbl.Meta().DeferredCausets {
		if defCaus.Name.String() == "y" {
			yearDefCaus = defCaus
		}
		if defCaus.Name.String() == "z" {
			zDefCaus = defCaus
		}
	}
	c.Assert(yearDefCaus, NotNil)
	c.Assert(yearDefCaus.Tp, Equals, allegrosql.TypeYear)
	c.Assert(allegrosql.HasUnsignedFlag(yearDefCaus.Flag), IsTrue)

	c.Assert(zDefCaus, NotNil)
	c.Assert(allegrosql.HasUnsignedFlag(zDefCaus.Flag), IsTrue)
}

func (s *testIntegrationSuite5) TestBitDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t_bit (c1 bit(10) default 250, c2 int);")
	tk.MustInterDirc("insert into t_bit set c2=1;")
	tk.MustQuery("select bin(c1),c2 from t_bit").Check(testkit.Rows("11111010 1"))
	tk.MustInterDirc("drop causet t_bit")

	tk.MustInterDirc("create causet t_bit (a int)")
	tk.MustInterDirc("insert into t_bit value (1)")
	tk.MustInterDirc("alter causet t_bit add defCausumn c bit(16) null default b'1100110111001'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x19\xb9"))
	tk.MustInterDirc("uFIDelate t_bit set c = b'11100000000111'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x38\x07"))

	tk.MustInterDirc(`create causet testalltypes1 (
    field_1 bit default 1,
    field_2 tinyint null default null
	);`)
	tk.MustInterDirc(`create causet testalltypes2 (
    field_1 bit null default null,
    field_2 tinyint null default null,
    field_3 tinyint unsigned null default null,
    field_4 bigint null default null,
    field_5 bigint unsigned null default null,
    field_6 mediumblob null default null,
    field_7 longblob null default null,
    field_8 blob null default null,
    field_9 tinyblob null default null,
    field_10 varbinary(255) null default null,
    field_11 binary(255) null default null,
    field_12 mediumtext null default null,
    field_13 longtext null default null,
    field_14 text null default null,
    field_15 tinytext null default null,
    field_16 char(255) null default null,
    field_17 numeric null default null,
    field_18 decimal null default null,
    field_19 integer null default null,
    field_20 integer unsigned null default null,
    field_21 int null default null,
    field_22 int unsigned null default null,
    field_23 mediumint null default null,
    field_24 mediumint unsigned null default null,
    field_25 smallint null default null,
    field_26 smallint unsigned null default null,
    field_27 float null default null,
    field_28 double null default null,
    field_29 double precision null default null,
    field_30 real null default null,
    field_31 varchar(255) null default null,
    field_32 date null default null,
    field_33 time null default null,
    field_34 datetime null default null,
    field_35 timestamp null default null
	);`)
}

func (s *testIntegrationSuite5) TestBackwardCompatibility(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test_backward_compatibility")
	defer tk.MustInterDirc("drop database test_backward_compatibility")
	tk.MustInterDirc("use test_backward_compatibility")
	tk.MustInterDirc("create causet t(a int primary key, b int)")
	for i := 0; i < 200; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// alter causet t add index idx_b(b);
	is := s.dom.SchemaReplicant()
	schemaName := perceptron.NewCIStr("test_backward_compatibility")
	blockName := perceptron.NewCIStr("t")
	schemaReplicant, ok := is.SchemaByName(schemaName)
	c.Assert(ok, IsTrue)
	tbl, err := is.TableByName(schemaName, blockName)
	c.Assert(err, IsNil)

	// Split the causet.
	s.cluster.SplitTable(tbl.Meta().ID, 100)

	unique := false
	indexName := perceptron.NewCIStr("idx_b")
	indexPartSpecification := &ast.IndexPartSpecification{
		DeferredCauset: &ast.DeferredCausetName{
			Schema: schemaName,
			Block:  blockName,
			Name:   perceptron.NewCIStr("b"),
		},
		Length: types.UnspecifiedLength,
	}
	indexPartSpecifications := []*ast.IndexPartSpecification{indexPartSpecification}
	var indexOption *ast.IndexOption
	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		TableID:    tbl.Meta().ID,
		Type:       perceptron.CausetActionAddIndex,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{unique, indexName, indexPartSpecifications, indexOption},
	}
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	t := spacetime.NewMeta(txn)
	job.ID, err = t.GenGlobalID()
	c.Assert(err, IsNil)
	job.Version = 1
	job.StartTS = txn.StartTS()

	// Simulate old MilevaDB init the add index job, old MilevaDB will not init the perceptron.Job.ReorgMeta field,
	// if we set job.SnapshotVer here, can simulate the behavior.
	job.SnapshotVer = txn.StartTS()
	err = t.EnQueueDBSJob(job)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	ticker := time.NewTicker(s.lease)
	defer ticker.Stop()
	for range ticker.C {
		historyJob, err := getHistoryDBSJob(s.causetstore, job.ID)
		c.Assert(err, IsNil)
		if historyJob == nil {

			continue
		}
		c.Assert(historyJob.Error, IsNil)

		if historyJob.IsSynced() {
			break
		}
	}

	// finished add index
	tk.MustInterDirc("admin check index t idx_b")
}

func (s *testIntegrationSuite3) TestMultiRegionGetTableEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_get_endhandle")
	tk.MustInterDirc("create database test_get_endhandle")
	tk.MustInterDirc("use test_get_endhandle")

	tk.MustInterDirc("create causet t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get causet ID for split.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test_get_endhandle"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DBS()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the causet.
	s.cluster.SplitTable(tblID, 100)

	maxHandle, emptyTable := getMaxTableHandle(testCtx, s.causetstore)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, Equals, ekv.IntHandle(1000))

	tk.MustInterDirc("insert into t values(10000, 1000)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.causetstore)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, Equals, ekv.IntHandle(1001))
}

type testMaxTableRowIDContext struct {
	c   *C
	d   dbs.DBS
	tbl causet.Block
}

func newTestMaxTableRowIDContext(c *C, d dbs.DBS, tbl causet.Block) *testMaxTableRowIDContext {
	return &testMaxTableRowIDContext{
		c:   c,
		d:   d,
		tbl: tbl,
	}
}

func getMaxTableHandle(ctx *testMaxTableRowIDContext, causetstore ekv.CausetStorage) (ekv.Handle, bool) {
	c := ctx.c
	d := ctx.d
	tbl := ctx.tbl
	curVer, err := causetstore.CurrentVersion()
	c.Assert(err, IsNil)
	maxHandle, emptyTable, err := d.GetTableMaxHandle(curVer.Ver, tbl.(causet.PhysicalTable))
	c.Assert(err, IsNil)
	return maxHandle, emptyTable
}

func checkGetMaxTableRowID(ctx *testMaxTableRowIDContext, causetstore ekv.CausetStorage, expectEmpty bool, expectMaxHandle ekv.Handle) {
	c := ctx.c
	maxHandle, emptyTable := getMaxTableHandle(ctx, causetstore)
	c.Assert(emptyTable, Equals, expectEmpty)
	c.Assert(maxHandle, solitonutil.HandleEquals, expectMaxHandle)
}

func getHistoryDBSJob(causetstore ekv.CausetStorage, id int64) (*perceptron.Job, error) {
	var job *perceptron.Job

	err := ekv.RunInNewTxn(causetstore, false, func(txn ekv.Transaction) error {
		t := spacetime.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDBSJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

func (s *testIntegrationSuite6) TestCreateTableTooLarge(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	allegrosql := "create causet t_too_large ("
	cnt := 3000
	for i := 1; i <= cnt; i++ {
		allegrosql += fmt.Sprintf("a%d double, b%d double, c%d double, d%d double", i, i, i, i)
		if i != cnt {
			allegrosql += ","
		}
	}
	allegrosql += ");"
	tk.MustGetErrCode(allegrosql, errno.ErrTooManyFields)

	originLimit := atomic.LoadUint32(&dbs.TableDeferredCausetCountLimit)
	atomic.StoreUint32(&dbs.TableDeferredCausetCountLimit, uint32(cnt*4))
	_, err := tk.InterDirc(allegrosql)
	c.Assert(ekv.ErrEntryTooLarge.Equal(err), IsTrue, Commentf("err:%v", err))
	atomic.StoreUint32(&dbs.TableDeferredCausetCountLimit, originLimit)
}

func (s *testIntegrationSuite3) TestChangeDeferredCausetPosition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("create causet position (a int default 1, b int default 2)")
	tk.MustInterDirc("insert into position value ()")
	tk.MustInterDirc("insert into position values (3,4)")
	tk.MustQuery("select * from position").Check(testkit.Rows("1 2", "3 4"))
	tk.MustInterDirc("alter causet position modify defCausumn b int first")
	tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3"))
	tk.MustInterDirc("insert into position value ()")
	tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3", "<nil> 1"))

	tk.MustInterDirc("create causet position1 (a int, b int, c double, d varchar(5))")
	tk.MustInterDirc(`insert into position1 value (1, 2, 3.14, 'MilevaDB')`)
	tk.MustInterDirc("alter causet position1 modify defCausumn d varchar(5) after a")
	tk.MustQuery("select * from position1").Check(testkit.Rows("1 MilevaDB 2 3.14"))
	tk.MustInterDirc("alter causet position1 modify defCausumn a int after c")
	tk.MustQuery("select * from position1").Check(testkit.Rows("MilevaDB 2 3.14 1"))
	tk.MustInterDirc("alter causet position1 modify defCausumn c double first")
	tk.MustQuery("select * from position1").Check(testkit.Rows("3.14 MilevaDB 2 1"))
	tk.MustGetErrCode("alter causet position1 modify defCausumn b int after b", errno.ErrBadField)

	tk.MustInterDirc("create causet position2 (a int, b int)")
	tk.MustInterDirc("alter causet position2 add index t(a, b)")
	tk.MustInterDirc("alter causet position2 modify defCausumn b int first")
	tk.MustInterDirc("insert into position2 value (3, 5)")
	tk.MustQuery("select a from position2 where a = 3").Check(testkit.Rows())
	tk.MustInterDirc("alter causet position2 change defCausumn b c int first")
	tk.MustQuery("select * from position2 where c = 3").Check(testkit.Rows("3 5"))
	tk.MustGetErrCode("alter causet position2 change defCausumn c b int after c", errno.ErrBadField)

	tk.MustInterDirc("create causet position3 (a int default 2)")
	tk.MustInterDirc("alter causet position3 modify defCausumn a int default 5 first")
	tk.MustInterDirc("insert into position3 value ()")
	tk.MustQuery("select * from position3").Check(testkit.Rows("5"))

	tk.MustInterDirc("create causet position4 (a int, b int)")
	tk.MustInterDirc("alter causet position4 add index t(b)")
	tk.MustInterDirc("alter causet position4 change defCausumn b c int first")
	createALLEGROSQL := tk.MustQuery("show create causet position4").Rows()[0][1]
	expectedALLEGROSQL := []string{
		"CREATE TABLE `position4` (",
		"  `c` int(11) DEFAULT NULL,",
		"  `a` int(11) DEFAULT NULL,",
		"  KEY `t` (`c`)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	}
	c.Assert(createALLEGROSQL, Equals, strings.Join(expectedALLEGROSQL, "\n"))
}

func (s *testIntegrationSuite2) TestAddIndexAfterAddDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("create causet test_add_index_after_add_defCaus(a int, b int not null default '0')")
	tk.MustInterDirc("insert into test_add_index_after_add_defCaus values(1, 2),(2,2)")
	tk.MustInterDirc("alter causet test_add_index_after_add_defCaus add defCausumn c int not null default '0'")
	allegrosql := "alter causet test_add_index_after_add_defCaus add unique index cc(c) "
	tk.MustGetErrCode(allegrosql, errno.ErrDupEntry)
	allegrosql = "alter causet test_add_index_after_add_defCaus add index idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);"
	tk.MustGetErrCode(allegrosql, errno.ErrTooManyKeyParts)
}

func (s *testIntegrationSuite3) TestResolveCharset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists resolve_charset")
	tk.MustInterDirc(`CREATE TABLE resolve_charset (a varchar(255) DEFAULT NULL) DEFAULT CHARSET=latin1`)
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("resolve_charset"))
	c.Assert(err, IsNil)
	c.Assert(tbl.DefCauss()[0].Charset, Equals, "latin1")
	tk.MustInterDirc("INSERT INTO resolve_charset VALUES('鰈')")

	tk.MustInterDirc("create database resolve_charset charset binary")
	tk.MustInterDirc("use resolve_charset")
	tk.MustInterDirc(`CREATE TABLE resolve_charset (a varchar(255) DEFAULT NULL) DEFAULT CHARSET=latin1`)

	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("resolve_charset"), perceptron.NewCIStr("resolve_charset"))
	c.Assert(err, IsNil)
	c.Assert(tbl.DefCauss()[0].Charset, Equals, "latin1")
	c.Assert(tbl.Meta().Charset, Equals, "latin1")

	tk.MustInterDirc(`CREATE TABLE resolve_charset1 (a varchar(255) DEFAULT NULL)`)
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("resolve_charset"), perceptron.NewCIStr("resolve_charset1"))
	c.Assert(err, IsNil)
	c.Assert(tbl.DefCauss()[0].Charset, Equals, "binary")
	c.Assert(tbl.Meta().Charset, Equals, "binary")
}

func (s *testIntegrationSuite6) TestAddDeferredCausetTooMany(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	count := int(atomic.LoadUint32(&dbs.TableDeferredCausetCountLimit) - 1)
	var defcaus []string
	for i := 0; i < count; i++ {
		defcaus = append(defcaus, fmt.Sprintf("a%d int", i))
	}
	createALLEGROSQL := fmt.Sprintf("create causet t_defCausumn_too_many (%s)", strings.Join(defcaus, ","))
	tk.MustInterDirc(createALLEGROSQL)
	tk.MustInterDirc("alter causet t_defCausumn_too_many add defCausumn a_512 int")
	alterALLEGROSQL := "alter causet t_defCausumn_too_many add defCausumn a_513 int"
	tk.MustGetErrCode(alterALLEGROSQL, errno.ErrTooManyFields)
}

func (s *testIntegrationSuite3) TestAlterDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")

	tk.MustInterDirc("create causet test_alter_defCausumn (a int default 111, b varchar(8), c varchar(8) not null, d timestamp on uFIDelate current_timestamp)")
	tk.MustInterDirc("insert into test_alter_defCausumn set b = 'a', c = 'aa'")
	tk.MustQuery("select a from test_alter_defCausumn").Check(testkit.Rows("111"))
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("test_alter_defCausumn"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	defCausA := tblInfo.DeferredCausets[0]
	hasNoDefault := allegrosql.HasNoDefaultValueFlag(defCausA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	tk.MustInterDirc("alter causet test_alter_defCausumn alter defCausumn a set default 222")
	tk.MustInterDirc("insert into test_alter_defCausumn set b = 'b', c = 'bb'")
	tk.MustQuery("select a from test_alter_defCausumn").Check(testkit.Rows("111", "222"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("test_alter_defCausumn"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	defCausA = tblInfo.DeferredCausets[0]
	hasNoDefault = allegrosql.HasNoDefaultValueFlag(defCausA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	tk.MustInterDirc("alter causet test_alter_defCausumn alter defCausumn b set default null")
	tk.MustInterDirc("insert into test_alter_defCausumn set c = 'cc'")
	tk.MustQuery("select b from test_alter_defCausumn").Check(testkit.Rows("a", "b", "<nil>"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("test_alter_defCausumn"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	defCausC := tblInfo.DeferredCausets[2]
	hasNoDefault = allegrosql.HasNoDefaultValueFlag(defCausC.Flag)
	c.Assert(hasNoDefault, IsTrue)
	tk.MustInterDirc("alter causet test_alter_defCausumn alter defCausumn c set default 'xx'")
	tk.MustInterDirc("insert into test_alter_defCausumn set a = 123")
	tk.MustQuery("select c from test_alter_defCausumn").Check(testkit.Rows("aa", "bb", "cc", "xx"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("test_alter_defCausumn"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	defCausC = tblInfo.DeferredCausets[2]
	hasNoDefault = allegrosql.HasNoDefaultValueFlag(defCausC.Flag)
	c.Assert(hasNoDefault, IsFalse)
	// TODO: After fix issue 2606.
	// tk.MustInterDirc( "alter causet test_alter_defCausumn alter defCausumn d set default null")
	tk.MustInterDirc("alter causet test_alter_defCausumn alter defCausumn a drop default")
	tk.MustInterDirc("insert into test_alter_defCausumn set b = 'd', c = 'dd'")
	tk.MustQuery("select a from test_alter_defCausumn").Check(testkit.Rows("111", "222", "222", "123", "<nil>"))

	// for failing tests
	allegrosql := "alter causet db_not_exist.test_alter_defCausumn alter defCausumn b set default 'c'"
	tk.MustGetErrCode(allegrosql, errno.ErrNoSuchTable)
	allegrosql = "alter causet test_not_exist alter defCausumn b set default 'c'"
	tk.MustGetErrCode(allegrosql, errno.ErrNoSuchTable)
	allegrosql = "alter causet test_alter_defCausumn alter defCausumn defCaus_not_exist set default 'c'"
	tk.MustGetErrCode(allegrosql, errno.ErrBadField)
	allegrosql = "alter causet test_alter_defCausumn alter defCausumn c set default null"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidDefault)

	// The followings tests whether adding constraints via change / modify defCausumn
	// is forbidden as expected.
	tk.MustInterDirc("drop causet if exists mc")
	tk.MustInterDirc("create causet mc(a int key, b int, c int)")
	_, err = tk.InterDirc("alter causet mc modify defCausumn a int key") // Adds a new primary key
	c.Assert(err, NotNil)
	_, err = tk.InterDirc("alter causet mc modify defCausumn c int unique") // Adds a new unique key
	c.Assert(err, NotNil)
	result := tk.MustQuery("show create causet mc")
	createALLEGROSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createALLEGROSQL, Equals, expected)

	// Change / modify defCausumn should preserve index options.
	tk.MustInterDirc("drop causet if exists mc")
	tk.MustInterDirc("create causet mc(a int key, b int, c int unique)")
	tk.MustInterDirc("alter causet mc modify defCausumn a bigint") // NOT NULL & PRIMARY KEY should be preserved
	tk.MustInterDirc("alter causet mc modify defCausumn b bigint")
	tk.MustInterDirc("alter causet mc modify defCausumn c bigint") // Unique should be preserved
	result = tk.MustQuery("show create causet mc")
	createALLEGROSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` bigint(20) DEFAULT NULL,\n  `c` bigint(20) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `c` (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createALLEGROSQL, Equals, expected)

	// Dropping or keeping auto_increment is allowed, however adding is not allowed.
	tk.MustInterDirc("drop causet if exists mc")
	tk.MustInterDirc("create causet mc(a int key auto_increment, b int)")
	tk.MustInterDirc("alter causet mc modify defCausumn a bigint auto_increment") // Keeps auto_increment
	result = tk.MustQuery("show create causet mc")
	createALLEGROSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL AUTO_INCREMENT,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createALLEGROSQL, Equals, expected)
	_, err = tk.InterDirc("alter causet mc modify defCausumn a bigint") // Droppping auto_increment is not allow when @@milevadb_allow_remove_auto_inc == 'off'
	c.Assert(err, NotNil)
	tk.MustInterDirc("set @@milevadb_allow_remove_auto_inc = on")
	tk.MustInterDirc("alter causet mc modify defCausumn a bigint") // Dropping auto_increment is ok when @@milevadb_allow_remove_auto_inc == 'on'
	result = tk.MustQuery("show create causet mc")
	createALLEGROSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createALLEGROSQL, Equals, expected)

	_, err = tk.InterDirc("alter causet mc modify defCausumn a bigint auto_increment") // Adds auto_increment should throw error
	c.Assert(err, NotNil)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t1 (a varchar(10),b varchar(100),c tinyint,d varchar(3071),index(a),index(a,b),index (c,d)) charset = ascii;")
	tk.MustGetErrCode("alter causet t1 modify defCausumn a varchar(3000);", errno.ErrTooLongKey)
	// check modify defCausumn with rename defCausumn.
	tk.MustGetErrCode("alter causet t1 change defCausumn a x varchar(3000);", errno.ErrTooLongKey)
	tk.MustGetErrCode("alter causet t1 modify defCausumn c bigint;", errno.ErrTooLongKey)

	tk.MustInterDirc("drop causet if exists multi_unique")
	tk.MustInterDirc("create causet multi_unique (a int unique unique)")
	tk.MustInterDirc("drop causet multi_unique")
	tk.MustInterDirc("create causet multi_unique (a int key primary key unique unique)")
	tk.MustInterDirc("drop causet multi_unique")
	tk.MustInterDirc("create causet multi_unique (a int key unique unique key unique)")
	tk.MustInterDirc("drop causet multi_unique")
	tk.MustInterDirc("create causet multi_unique (a serial serial default value)")
	tk.MustInterDirc("drop causet multi_unique")
	tk.MustInterDirc("create causet multi_unique (a serial serial default value serial default value)")
	tk.MustInterDirc("drop causet multi_unique")
}

func (s *testIntegrationSuite) assertWarningInterDirc(tk *testkit.TestKit, c *C, allegrosql string, expectedWarn *terror.Error) {
	_, err := tk.InterDirc(allegrosql)
	c.Assert(err, IsNil)
	st := tk.Se.GetStochastikVars().StmtCtx
	c.Assert(st.WarningCount(), Equals, uint16(1))
	c.Assert(expectedWarn.Equal(st.GetWarnings()[0].Err), IsTrue, Commentf("error:%v", err))
}

func (s *testIntegrationSuite) assertAlterWarnInterDirc(tk *testkit.TestKit, c *C, allegrosql string) {
	s.assertWarningInterDirc(tk, c, allegrosql, dbs.ErrAlterOperationNotSupported)
}

func (s *testIntegrationSuite) assertAlterErrorInterDirc(tk *testkit.TestKit, c *C, allegrosql string) {
	tk.MustGetErrCode(allegrosql, errno.ErrAlterOperationNotSupportedReason)
}

func (s *testIntegrationSuite3) TestAlterAlgorithm(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, t1")
	defer tk.MustInterDirc("drop causet if exists t")

	tk.MustInterDirc(`create causet t(
	a int,
	b varchar(100),
	c int,
	INDEX idx_c(c)) PARTITION BY RANGE ( a ) (
	PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	)`)
	s.assertAlterWarnInterDirc(tk, c, "alter causet t modify defCausumn a bigint, ALGORITHM=INPLACE;")
	tk.MustInterDirc("alter causet t modify defCausumn a bigint, ALGORITHM=INPLACE, ALGORITHM=INSTANT;")
	tk.MustInterDirc("alter causet t modify defCausumn a bigint, ALGORITHM=DEFAULT;")

	// Test add/drop index
	s.assertAlterErrorInterDirc(tk, c, "alter causet t add index idx_b(b), ALGORITHM=INSTANT")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t add index idx_b1(b), ALGORITHM=COPY")
	tk.MustInterDirc("alter causet t add index idx_b2(b), ALGORITHM=INPLACE")
	tk.MustInterDirc("alter causet t add index idx_b3(b), ALGORITHM=DEFAULT")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t drop index idx_b3, ALGORITHM=INPLACE")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t drop index idx_b1, ALGORITHM=COPY")
	tk.MustInterDirc("alter causet t drop index idx_b2, ALGORITHM=INSTANT")

	// Test rename
	s.assertAlterWarnInterDirc(tk, c, "alter causet t rename to t1, ALGORITHM=COPY")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t1 rename to t2, ALGORITHM=INPLACE")
	tk.MustInterDirc("alter causet t2 rename to t, ALGORITHM=INSTANT")
	tk.MustInterDirc("alter causet t rename to t1, ALGORITHM=DEFAULT")
	tk.MustInterDirc("alter causet t1 rename to t")

	// Test rename index
	s.assertAlterWarnInterDirc(tk, c, "alter causet t rename index idx_c to idx_c1, ALGORITHM=COPY")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t rename index idx_c1 to idx_c2, ALGORITHM=INPLACE")
	tk.MustInterDirc("alter causet t rename index idx_c2 to idx_c, ALGORITHM=INSTANT")
	tk.MustInterDirc("alter causet t rename index idx_c to idx_c1, ALGORITHM=DEFAULT")

	// partition.
	s.assertAlterWarnInterDirc(tk, c, "alter causet t ALGORITHM=COPY, truncate partition p1")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t ALGORITHM=INPLACE, truncate partition p2")
	tk.MustInterDirc("alter causet t ALGORITHM=INSTANT, truncate partition p3")

	s.assertAlterWarnInterDirc(tk, c, "alter causet t add partition (partition p4 values less than (2002)), ALGORITHM=COPY")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t add partition (partition p5 values less than (3002)), ALGORITHM=INPLACE")
	tk.MustInterDirc("alter causet t add partition (partition p6 values less than (4002)), ALGORITHM=INSTANT")

	s.assertAlterWarnInterDirc(tk, c, "alter causet t ALGORITHM=COPY, drop partition p4")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t ALGORITHM=INPLACE, drop partition p5")
	tk.MustInterDirc("alter causet t ALGORITHM=INSTANT, drop partition p6")

	// Block options
	s.assertAlterWarnInterDirc(tk, c, "alter causet t comment = 'test', ALGORITHM=COPY")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t comment = 'test', ALGORITHM=INPLACE")
	tk.MustInterDirc("alter causet t comment = 'test', ALGORITHM=INSTANT")

	s.assertAlterWarnInterDirc(tk, c, "alter causet t default charset = utf8mb4, ALGORITHM=COPY")
	s.assertAlterWarnInterDirc(tk, c, "alter causet t default charset = utf8mb4, ALGORITHM=INPLACE")
	tk.MustInterDirc("alter causet t default charset = utf8mb4, ALGORITHM=INSTANT")
}

func (s *testIntegrationSuite3) TestAlterTableAddUniqueOnPartionRangeDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	defer tk.MustInterDirc("drop causet if exists t")

	tk.MustInterDirc(`create causet t(
	a int,
	b varchar(100),
	c int,
	INDEX idx_c(c))
	PARTITION BY RANGE COLUMNS( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	)`)
	tk.MustInterDirc("insert into t values (4, 'xxx', 4)")
	tk.MustInterDirc("insert into t values (4, 'xxx', 9)") // Note the repeated 4
	tk.MustInterDirc("insert into t values (17, 'xxx', 12)")
	tk.MustGetErrCode("alter causet t add unique index idx_a(a)", errno.ErrDupEntry)

	tk.MustInterDirc("delete from t where a = 4")
	tk.MustInterDirc("alter causet t add unique index idx_a(a)")
	tk.MustInterDirc("alter causet t add unique index idx_ac(a, c)")
	tk.MustGetErrCode("alter causet t add unique index idx_b(b)", errno.ErrUniqueKeyNeedAllFieldsInPf)
}

func (s *testIntegrationSuite5) TestFulltextIndexIgnore(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t_ft")
	defer tk.MustInterDirc("drop causet if exists t_ft")
	// Make sure that creating and altering to add a fulltext key gives the correct warning
	s.assertWarningInterDirc(tk, c, "create causet t_ft (a text, fulltext key (a))", dbs.ErrTableCantHandleFt)
	s.assertWarningInterDirc(tk, c, "alter causet t_ft add fulltext key (a)", dbs.ErrTableCantHandleFt)

	// Make sure causet t_ft still has no indexes even after it was created and altered
	r := tk.MustQuery("show index from t_ft")
	c.Assert(r.Rows(), HasLen, 0)
	r = tk.MustQuery("select * from information_schema.statistics where block_schema='test' and block_name='t_ft'")
	c.Assert(r.Rows(), HasLen, 0)
}

func (s *testIntegrationSuite1) TestTreatOldVersionUTF8AsUTF8MB4(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	defer tk.MustInterDirc("drop causet if exists t")

	tk.MustInterDirc("create causet t (a varchar(10) character set utf8, b varchar(10) character set ascii) charset=utf8mb4;")
	tk.MustGetErrCode("insert into t set a= x'f09f8c80';", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version causet info with defCausumn charset is utf8.
	EDB, ok := petri.GetPetri(s.ctx).SchemaReplicant().SchemaByName(perceptron.NewCIStr("test"))
	tbl := testGetTableByName(c, s.ctx, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Version = perceptron.TableInfoVersion0
	tblInfo.DeferredCausets[0].Version = perceptron.DeferredCausetInfoVersion0
	uFIDelateTableInfo := func(tblInfo *perceptron.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.CausetStore = s.causetstore
		err := mockCtx.NewTxn(context.Background())
		c.Assert(err, IsNil)
		txn, err := mockCtx.Txn(true)
		c.Assert(err, IsNil)
		mt := spacetime.NewMeta(txn)
		c.Assert(ok, IsTrue)
		err = mt.UFIDelateTable(EDB.ID, tblInfo)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
	uFIDelateTableInfo(tblInfo)
	tk.MustInterDirc("alter causet t add defCausumn c varchar(10) character set utf8;") // load latest schemaReplicant.
	c.Assert(config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4, IsTrue)
	tk.MustInterDirc("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustInterDirc("alter causet t drop defCausumn c;") //  reload schemaReplicant.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version causet info with causet and defCausumn charset is utf8.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Charset = charset.CharsetUTF8
	tblInfo.DefCauslate = charset.DefCauslationUTF8
	tblInfo.Version = perceptron.TableInfoVersion0
	tblInfo.DeferredCausets[0].Version = perceptron.DeferredCausetInfoVersion0
	uFIDelateTableInfo(tblInfo)

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustInterDirc("alter causet t add defCausumn c varchar(10);") //  load latest schemaReplicant.
	tk.MustInterDirc("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` varchar(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustInterDirc("alter causet t drop defCausumn c;") //  reload schemaReplicant.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test modify defCausumn charset.
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustInterDirc("alter causet t modify defCausumn a varchar(10) character set utf8mb4") //  change defCausumn charset.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl.Meta().DeferredCausets[0].Charset, Equals, charset.CharsetUTF8MB4)
	c.Assert(tbl.Meta().DeferredCausets[0].DefCauslate, Equals, charset.DefCauslationUTF8MB4)
	c.Assert(tbl.Meta().DeferredCausets[0].Version, Equals, perceptron.DeferredCausetInfoVersion0)
	tk.MustInterDirc("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Test for change defCausumn should not modify the defCausumn version.
	tk.MustInterDirc("alter causet t change defCausumn a a varchar(20)") //  change defCausumn.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl.Meta().DeferredCausets[0].Charset, Equals, charset.CharsetUTF8MB4)
	c.Assert(tbl.Meta().DeferredCausets[0].DefCauslate, Equals, charset.DefCauslationUTF8MB4)
	c.Assert(tbl.Meta().DeferredCausets[0].Version, Equals, perceptron.DeferredCausetInfoVersion0)

	// Test for v2.1.5 and v2.1.6 that causet version is 1 but defCausumn version is 0.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Charset = charset.CharsetUTF8
	tblInfo.DefCauslate = charset.DefCauslationUTF8
	tblInfo.Version = perceptron.TableInfoVersion1
	tblInfo.DeferredCausets[0].Version = perceptron.DeferredCausetInfoVersion0
	tblInfo.DeferredCausets[0].Charset = charset.CharsetUTF8
	tblInfo.DeferredCausets[0].DefCauslate = charset.DefCauslationUTF8
	uFIDelateTableInfo(tblInfo)
	c.Assert(config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4, IsTrue)
	tk.MustInterDirc("alter causet t change defCausumn b b varchar(20) character set ascii") // reload schemaReplicant.
	tk.MustInterDirc("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(20) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustInterDirc("alter causet t change defCausumn b b varchar(30) character set ascii") // reload schemaReplicant.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(30) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test for alter causet convert charset
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustInterDirc("alter causet t drop defCausumn b") // reload schemaReplicant.
	tk.MustInterDirc("alter causet t convert to charset utf8mb4;")

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustInterDirc("alter causet t add defCausumn b varchar(50);") // reload schemaReplicant.
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(50) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func (s *testIntegrationSuite3) TestDefaultValueIsString(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	defer tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int default b'1');")
	tbl := testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl.Meta().DeferredCausets[0].DefaultValue, Equals, "1")
}

func (s *testIntegrationSuite5) TestChangingDBCharset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("DROP DATABASE IF EXISTS alterdb1")
	tk.MustInterDirc("CREATE DATABASE alterdb1 CHARSET=utf8 COLLATE=utf8_unicode_ci")

	// No default EDB errors.
	noDBFailedCases := []struct {
		stmt   string
		errMsg string
	}{
		{
			"ALTER DATABASE CHARACTER SET = 'utf8'",
			"[causet:1046]No database selected",
		},
		{
			"ALTER SCHEMA `` CHARACTER SET = 'utf8'",
			"[dbs:1102]Incorrect database name ''",
		},
	}
	for _, fc := range noDBFailedCases {
		c.Assert(tk.InterDircToErr(fc.stmt).Error(), Equals, fc.errMsg, Commentf("%v", fc.stmt))
	}

	verifyDBCharsetAndDefCauslate := func(dbName, chs string, defCausl string) {
		// check `SHOW CREATE SCHEMA`.
		r := tk.MustQuery("SHOW CREATE SCHEMA " + dbName).Rows()[0][1].(string)
		c.Assert(strings.Contains(r, "CHARACTER SET "+chs), IsTrue)

		template := `SELECT
					DEFAULT_CHARACTER_SET_NAME,
					DEFAULT_COLLATION_NAME
				FROM INFORMATION_SCHEMA.SCHEMATA
				WHERE SCHEMA_NAME = '%s'`
		allegrosql := fmt.Sprintf(template, dbName)
		tk.MustQuery(allegrosql).Check(testkit.Rows(fmt.Sprintf("%s %s", chs, defCausl)))

		dom := petri.GetPetri(s.ctx)
		// Make sure the causet schemaReplicant is the new schemaReplicant.
		err := dom.Reload()
		c.Assert(err, IsNil)
		dbInfo, ok := dom.SchemaReplicant().SchemaByName(perceptron.NewCIStr(dbName))
		c.Assert(ok, Equals, true)
		c.Assert(dbInfo.Charset, Equals, chs)
		c.Assert(dbInfo.DefCauslate, Equals, defCausl)
	}

	tk.MustInterDirc("ALTER SCHEMA alterdb1 COLLATE = utf8mb4_general_ci")
	verifyDBCharsetAndDefCauslate("alterdb1", "utf8mb4", "utf8mb4_general_ci")

	tk.MustInterDirc("DROP DATABASE IF EXISTS alterdb2")
	tk.MustInterDirc("CREATE DATABASE alterdb2 CHARSET=utf8 COLLATE=utf8_unicode_ci")
	tk.MustInterDirc("USE alterdb2")

	failedCases := []struct {
		stmt   string
		errMsg string
	}{
		{
			"ALTER SCHEMA `` CHARACTER SET = 'utf8'",
			"[dbs:1102]Incorrect database name ''",
		},
		{
			"ALTER DATABASE CHARACTER SET = ''",
			"[BerolinaSQL:1115]Unknown character set: ''",
		},
		{
			"ALTER DATABASE CHARACTER SET = 'INVALID_CHARSET'",
			"[BerolinaSQL:1115]Unknown character set: 'INVALID_CHARSET'",
		},
		{
			"ALTER SCHEMA COLLATE = ''",
			"[dbs:1273]Unknown defCauslation: ''",
		},
		{
			"ALTER DATABASE COLLATE = 'INVALID_COLLATION'",
			"[dbs:1273]Unknown defCauslation: 'INVALID_COLLATION'",
		},
		{
			"ALTER DATABASE CHARACTER SET = 'utf8' DEFAULT CHARSET = 'utf8mb4'",
			"[dbs:1302]Conflicting declarations: 'CHARACTER SET utf8' and 'CHARACTER SET utf8mb4'",
		},
		{
			"ALTER SCHEMA CHARACTER SET = 'utf8' COLLATE = 'utf8mb4_bin'",
			"[dbs:1302]Conflicting declarations: 'CHARACTER SET utf8' and 'CHARACTER SET utf8mb4'",
		},
		{
			"ALTER DATABASE COLLATE = 'utf8mb4_bin' COLLATE = 'utf8_bin'",
			"[dbs:1302]Conflicting declarations: 'CHARACTER SET utf8mb4' and 'CHARACTER SET utf8'",
		},
	}

	for _, fc := range failedCases {
		c.Assert(tk.InterDircToErr(fc.stmt).Error(), Equals, fc.errMsg, Commentf("%v", fc.stmt))
	}
	tk.MustInterDirc("ALTER SCHEMA CHARACTER SET = 'utf8' COLLATE = 'utf8_unicode_ci'")
	verifyDBCharsetAndDefCauslate("alterdb2", "utf8", "utf8_unicode_ci")

	tk.MustInterDirc("ALTER SCHEMA CHARACTER SET = 'utf8mb4'")
	verifyDBCharsetAndDefCauslate("alterdb2", "utf8mb4", "utf8mb4_bin")

	tk.MustInterDirc("ALTER SCHEMA CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_general_ci'")
	verifyDBCharsetAndDefCauslate("alterdb2", "utf8mb4", "utf8mb4_general_ci")

	// Test changing charset of schemaReplicant with uppercase name. See https://github.com/whtcorpsinc/milevadb/issues/19273.
	tk.MustInterDirc("drop database if exists TEST_UPPERCASE_DB_CHARSET;")
	tk.MustInterDirc("create database TEST_UPPERCASE_DB_CHARSET;")
	tk.MustInterDirc("use TEST_UPPERCASE_DB_CHARSET;")
	tk.MustInterDirc("alter database TEST_UPPERCASE_DB_CHARSET default character set utf8;")
	tk.MustInterDirc("alter database TEST_UPPERCASE_DB_CHARSET default character set utf8mb4;")
}

func (s *testIntegrationSuite4) TestDropAutoIncrementIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test")
	tk.MustInterDirc("use test")

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int auto_increment, unique key (a))")
	dropIndexALLEGROSQL := "alter causet t1 drop index a"
	tk.MustGetErrCode(dropIndexALLEGROSQL, errno.ErrWrongAutoKey)

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int(11) not null auto_increment, b int(11), c bigint, unique key (a, b, c))")
	dropIndexALLEGROSQL = "alter causet t1 drop index a"
	tk.MustGetErrCode(dropIndexALLEGROSQL, errno.ErrWrongAutoKey)
}

func (s *testIntegrationSuite4) TestInsertIntoGeneratedDeferredCausetWithDefaultExpr(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test")
	tk.MustInterDirc("use test")

	// insert into virtual / stored defCausumns
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int, b int as (-a) virtual, c int as (-a) stored)")
	tk.MustInterDirc("insert into t1 values (1, default, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 -1 -1"))
	tk.MustInterDirc("delete from t1")

	// insert multiple rows
	tk.MustInterDirc("insert into t1(a,b) values (1, default), (2, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 -1 -1", "2 -2 -2"))
	tk.MustInterDirc("delete from t1")

	// insert into generated defCausumns only
	tk.MustInterDirc("insert into t1(b) values (default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustInterDirc("delete from t1")
	tk.MustInterDirc("insert into t1(c) values (default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustInterDirc("delete from t1")

	// generated defCausumns with index
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t2 like t1")
	tk.MustInterDirc("alter causet t2 add index idx1(a)")
	tk.MustInterDirc("alter causet t2 add index idx2(b)")
	tk.MustInterDirc("insert into t2 values (1, default, default)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 -1 -1"))
	tk.MustInterDirc("delete from t2")
	tk.MustInterDirc("alter causet t2 drop index idx1")
	tk.MustInterDirc("alter causet t2 drop index idx2")
	tk.MustInterDirc("insert into t2 values (1, default, default)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 -1 -1"))

	// generated defCausumns in different position
	tk.MustInterDirc("drop causet if exists t3")
	tk.MustInterDirc("create causet t3 (gc1 int as (r+1), gc2 int as (r+1) stored, gc3 int as (gc2+1), gc4 int as (gc1+1) stored, r int)")
	tk.MustInterDirc("insert into t3 values (default, default, default, default, 1)")
	tk.MustQuery("select * from t3").Check(testkit.Rows("2 2 3 3 1"))

	// generated defCausumns in replace memex
	tk.MustInterDirc("create causet t4 (a int key, b int, c int as (a+1), d int as (b+1) stored)")
	tk.MustInterDirc("insert into t4 values (1, 10, default, default)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 10 2 11"))
	tk.MustInterDirc("replace into t4 values (1, 20, default, default)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 20 2 21"))

	// generated defCausumns with default function is not allowed
	tk.MustInterDirc("create causet t5 (a int default 10, b int as (a+1))")
	tk.MustGetErrCode("insert into t5 values (20, default(a))", errno.ErrBadGeneratedDeferredCauset)

	tk.MustInterDirc("drop causet t1, t2, t3, t4, t5")
}

func (s *testIntegrationSuite3) TestSqlFunctionsInGeneratedDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, t1")

	// In generated defCausumns memex, these items are not allowed:
	// 1. Blocked function (for full function list, please visit https://github.com/allegrosql/allegrosql-server/blob/5.7/errno-test/suite/gdefCaus/inc/gdefCaus_blocked_sql_funcs_main.inc)
	// Note: This list is not complete, if you need a complete list, please refer to MyALLEGROSQL 5.7 source code.
	tk.MustGetErrCode("create causet t (a int, b int as (sysdate()))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	// 2. Non-builtin function
	tk.MustGetErrCode("create causet t (a int, b int as (non_exist_funcA()))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	// 3. values(x) function
	tk.MustGetErrCode("create causet t (a int, b int as (values(a)))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	// 4. Subquery
	tk.MustGetErrCode("create causet t (a int, b int as ((SELECT 1 FROM t1 UNION SELECT 1 FROM t1)))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	// 5. Variable & functions related to variable
	tk.MustInterDirc("set @x = 1")
	tk.MustGetErrCode("create causet t (a int, b int as (@x))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	tk.MustGetErrCode("create causet t (a int, b int as (@@max_connections))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	tk.MustGetErrCode("create causet t (a int, b int as (@y:=1))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	tk.MustGetErrCode(`create causet t (a int, b int as (getvalue("x")))`, errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	tk.MustGetErrCode(`create causet t (a int, b int as (setvalue("y", 1)))`, errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	// 6. Aggregate function
	tk.MustGetErrCode("create causet t1 (a int, b int as (avg(a)));", errno.ErrInvalidGroupFuncUse)

	// Determinate functions are allowed:
	tk.MustInterDirc("create causet t1 (a int, b int generated always as (abs(a)) virtual)")
	tk.MustInterDirc("insert into t1 values (-1, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("-1 1"))

	// Functions added in MyALLEGROSQL 8.0, but now not supported in MilevaDB
	// They will be deal with non-exists function, and throw error.git
	tk.MustGetErrCode("create causet t (a int, b int as (uFIDelatexml(1, 1, 1)))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	tk.MustGetErrCode("create causet t (a int, b int as (memex_digest(1)))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	tk.MustGetErrCode("create causet t (a int, b int as (memex_digest_text(1)))", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
}

func (s *testIntegrationSuite3) TestBerolinaSQLIssue284(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet test.t_BerolinaSQL_issue_284(c1 int not null primary key)")
	_, err := tk.InterDirc("create causet test.t_BerolinaSQL_issue_284_2(id int not null primary key, c1 int not null, constraint foreign key (c1) references t_BerolinaSQL_issue_284(c1))")
	c.Assert(err, IsNil)

	tk.MustInterDirc("drop causet test.t_BerolinaSQL_issue_284")
	tk.MustInterDirc("drop causet test.t_BerolinaSQL_issue_284_2")
}

func (s *testIntegrationSuite7) TestAddExpressionIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")

	tk.MustInterDirc("create causet t (a int, b real);")
	tk.MustInterDirc("insert into t values (1, 2.1);")
	tk.MustInterDirc("alter causet t add index idx((a+b));")

	tblInfo, err := s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defCausumns := tblInfo.Meta().DeferredCausets
	c.Assert(len(defCausumns), Equals, 3)
	c.Assert(defCausumns[2].Hidden, IsTrue)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustInterDirc("alter causet t add index idx_multi((a+b),(a+1), b);")
	tblInfo, err = s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defCausumns = tblInfo.Meta().DeferredCausets
	c.Assert(len(defCausumns), Equals, 5)
	c.Assert(defCausumns[3].Hidden, IsTrue)
	c.Assert(defCausumns[4].Hidden, IsTrue)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustInterDirc("alter causet t drop index idx;")
	tblInfo, err = s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defCausumns = tblInfo.Meta().DeferredCausets
	c.Assert(len(defCausumns), Equals, 4)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustInterDirc("alter causet t drop index idx_multi;")
	tblInfo, err = s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defCausumns = tblInfo.Meta().DeferredCausets
	c.Assert(len(defCausumns), Equals, 2)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	// Issue #17111
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a varchar(10), b varchar(10));")
	tk.MustInterDirc("alter causet t1 add unique index ei_ab ((concat(a, b)));")
	tk.MustInterDirc("alter causet t1 alter index ei_ab invisible;")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, key((a+1)), key((a+2)), key idx((a+3)), key((a+4)));")
}

func (s *testIntegrationSuite7) TestCreateExpressionIndexError(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a int, b real);")
	tk.MustGetErrCode("alter causet t add primary key ((a+b));", errno.ErrFunctionalIndexPrimaryKey)

	// Test for error
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a int, b real);")
	tk.MustGetErrCode("alter causet t add primary key ((a+b));", errno.ErrFunctionalIndexPrimaryKey)
	tk.MustGetErrCode("alter causet t add index ((rand()));", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)
	tk.MustGetErrCode("alter causet t add index ((now()+1));", errno.ErrGeneratedDeferredCausetFunctionIsNotAllowed)

	tk.MustInterDirc("alter causet t add defCausumn (_V$_idx_0 int);")
	tk.MustGetErrCode("alter causet t add index idx((a+1));", errno.ErrDupFieldName)
	tk.MustInterDirc("alter causet t drop defCausumn _V$_idx_0;")
	tk.MustInterDirc("alter causet t add index idx((a+1));")
	tk.MustGetErrCode("alter causet t add defCausumn (_V$_idx_0 int);", errno.ErrDupFieldName)
	tk.MustInterDirc("alter causet t drop index idx;")
	tk.MustInterDirc("alter causet t add defCausumn (_V$_idx_0 int);")

	tk.MustInterDirc("alter causet t add defCausumn (_V$_memex_index_0 int);")
	tk.MustGetErrCode("alter causet t add index ((a+1));", errno.ErrDupFieldName)
	tk.MustInterDirc("alter causet t drop defCausumn _V$_memex_index_0;")
	tk.MustInterDirc("alter causet t add index ((a+1));")
	tk.MustGetErrCode("alter causet t drop defCausumn _V$_memex_index_0;", errno.ErrCantDropFieldOrKey)
	tk.MustGetErrCode("alter causet t add defCausumn e int as (_V$_memex_index_0 + 1);", errno.ErrBadField)
}

func (s *testIntegrationSuite7) TestAddExpressionIndexOnPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc(`create causet t(
	a int,
	b varchar(100),
	c int)
	PARTITION BY RANGE ( a ) (
	PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	);`)
	tk.MustInterDirc("insert into t values (1, 'test', 2), (12, 'test', 3), (15, 'test', 10), (20, 'test', 20);")
	tk.MustInterDirc("alter causet t add index idx((a+c));")

	tblInfo, err := s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defCausumns := tblInfo.Meta().DeferredCausets
	c.Assert(len(defCausumns), Equals, 4)
	c.Assert(defCausumns[3].Hidden, IsTrue)

	tk.MustQuery("select * from t order by a;").Check(testkit.Rows("1 test 2", "12 test 3", "15 test 10", "20 test 20"))
}

// TestCreateTableWithAutoIdCache test the auto_id_cache causet option.
// `auto_id_cache` take effects on handle too when `PKIshandle` is false,
// or even there is no auto_increment defCausumn at all.
func (s *testIntegrationSuite3) TestCreateTableWithAutoIdCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("USE test;")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")

	// Test primary key is handle.
	tk.MustInterDirc("create causet t(a int auto_increment key) auto_id_cache 100")
	tblInfo, err := s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().AutoIdCache, Equals, int64(100))
	tk.MustInterDirc("insert into t values()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustInterDirc("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache
	tk.MustInterDirc("rename causet t to t1;")
	tk.MustInterDirc("insert into t1 values()")
	tk.MustQuery("select * from t1").Check(testkit.Rows("101"))

	// Test primary key is not handle.
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t(a int) auto_id_cache 100")
	_, err = s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)

	tk.MustInterDirc("insert into t values()")
	tk.MustQuery("select _milevadb_rowid from t").Check(testkit.Rows("1"))
	tk.MustInterDirc("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache
	tk.MustInterDirc("rename causet t to t1;")
	tk.MustInterDirc("insert into t1 values()")
	tk.MustQuery("select _milevadb_rowid from t1").Check(testkit.Rows("101"))

	// Test both auto_increment and rowid exist.
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t(a int null, b int auto_increment unique) auto_id_cache 100")
	_, err = s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)

	tk.MustInterDirc("insert into t(b) values(NULL)")
	tk.MustQuery("select b, _milevadb_rowid from t").Check(testkit.Rows("1 2"))
	tk.MustInterDirc("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustInterDirc("rename causet t to t1;")
	tk.MustInterDirc("insert into t1(b) values(NULL)")
	tk.MustQuery("select b, _milevadb_rowid from t1").Check(testkit.Rows("101 102"))
	tk.MustInterDirc("delete from t1")

	// Test alter auto_id_cache.
	tk.MustInterDirc("alter causet t1 auto_id_cache 200")
	tblInfo, err = s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().AutoIdCache, Equals, int64(200))

	tk.MustInterDirc("insert into t1(b) values(NULL)")
	tk.MustQuery("select b, _milevadb_rowid from t1").Check(testkit.Rows("201 202"))
	tk.MustInterDirc("delete from t1")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustInterDirc("rename causet t1 to t;")
	tk.MustInterDirc("insert into t(b) values(NULL)")
	tk.MustQuery("select b, _milevadb_rowid from t").Check(testkit.Rows("401 402"))
	tk.MustInterDirc("delete from t")

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t(a int auto_increment key) auto_id_cache 3")
	tblInfo, err = s.dom.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().AutoIdCache, Equals, int64(3))

	// Test insert batch size(4 here) greater than the customized autoid step(3 here).
	tk.MustInterDirc("insert into t(a) values(NULL),(NULL),(NULL),(NULL)")
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustInterDirc("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustInterDirc("rename causet t to t1;")
	tk.MustInterDirc("insert into t1(a) values(NULL)")
	next := tk.MustQuery("select a from t1").Rows()[0][0].(string)
	nextInt, err := strconv.Atoi(next)
	c.Assert(err, IsNil)
	c.Assert(nextInt, Greater, 5)

	// Test auto_id_cache overflows int64.
	tk.MustInterDirc("drop causet if exists t;")
	_, err = tk.InterDirc("create causet t(a int) auto_id_cache = 9223372036854775808")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "causet option auto_id_cache overflows int64")

	tk.MustInterDirc("create causet t(a int) auto_id_cache = 9223372036854775807")
	_, err = tk.InterDirc("alter causet t auto_id_cache = 9223372036854775808")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "causet option auto_id_cache overflows int64")
}

func (s *testIntegrationSuite4) TestAlterIndexVisibility(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists alter_index_test")
	tk.MustInterDirc("USE alter_index_test;")
	tk.MustInterDirc("drop causet if exists t, t1, t2, t3;")

	tk.MustInterDirc("create causet t(a int NOT NULL, b int, key(a), unique(b) invisible)")
	query := queryIndexOnTable("alter_index_test", "t")
	tk.MustQuery(query).Check(testkit.Rows("a YES", "b NO"))

	tk.MustInterDirc("alter causet t alter index a invisible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b NO"))

	tk.MustInterDirc("alter causet t alter index b visible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b YES"))

	tk.MustInterDirc("alter causet t alter index b invisible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b NO"))

	tk.MustGetErrMsg("alter causet t alter index non_exists_idx visible", "[schemaReplicant:1176]Key 'non_exists_idx' doesn't exist in causet 't'")

	// Alter implicit primary key to invisible index should throw error
	tk.MustInterDirc("create causet t1(a int NOT NULL, unique(a))")
	tk.MustGetErrMsg("alter causet t1 alter index a invisible", "[dbs:3522]A primary key index cannot be invisible")

	// Alter explicit primary key to invisible index should throw error
	tk.MustInterDirc("create causet t2(a int, primary key(a))")
	tk.MustGetErrMsg("alter causet t2 alter index PRIMARY invisible", `[BerolinaSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use line 1 defCausumn 34 near "PRIMARY invisible" `)

	// Alter memex index
	tk.MustInterDirc("create causet t3(a int NOT NULL, b int)")
	tk.MustInterDirc("alter causet t3 add index idx((a+b));")
	query = queryIndexOnTable("alter_index_test", "t3")
	tk.MustQuery(query).Check(testkit.Rows("idx YES"))

	tk.MustInterDirc("alter causet t3 alter index idx invisible")
	tk.MustQuery(query).Check(testkit.Rows("idx NO"))
}

func queryIndexOnTable(dbName, blockName string) string {
	return fmt.Sprintf("select distinct index_name, is_visible from information_schema.statistics where block_schema = '%s' and block_name = '%s' order by index_name", dbName, blockName)
}

func (s *testIntegrationSuite5) TestDropDeferredCausetWithCompositeIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	query := queryIndexOnTable("drop_composite_index_test", "t_drop_defCausumn_with_comp_idx")
	tk.MustInterDirc("create database if not exists drop_composite_index_test")
	tk.MustInterDirc("use drop_composite_index_test")
	tk.MustInterDirc("create causet t_drop_defCausumn_with_comp_idx(a int, b int, c int)")
	defer tk.MustInterDirc("drop causet if exists t_drop_defCausumn_with_comp_idx")
	tk.MustInterDirc("create index idx_bc on t_drop_defCausumn_with_comp_idx(b, c)")
	tk.MustInterDirc("create index idx_b on t_drop_defCausumn_with_comp_idx(b)")
	tk.MustGetErrMsg("alter causet t_drop_defCausumn_with_comp_idx drop defCausumn b", "[dbs:8200]can't drop defCausumn b with composite index covered or Primary Key covered now")
	tk.MustQuery(query).Check(testkit.Rows("idx_b YES", "idx_bc YES"))
	tk.MustInterDirc("alter causet t_drop_defCausumn_with_comp_idx alter index idx_bc invisible")
	tk.MustInterDirc("alter causet t_drop_defCausumn_with_comp_idx alter index idx_b invisible")
	tk.MustGetErrMsg("alter causet t_drop_defCausumn_with_comp_idx drop defCausumn b", "[dbs:8200]can't drop defCausumn b with composite index covered or Primary Key covered now")
	tk.MustQuery(query).Check(testkit.Rows("idx_b NO", "idx_bc NO"))
}

func (s *testIntegrationSuite5) TestDropDeferredCausetWithIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet t_drop_defCausumn_with_idx(a int, b int, c int)")
	defer tk.MustInterDirc("drop causet if exists t_drop_defCausumn_with_idx")
	tk.MustInterDirc("create index idx on t_drop_defCausumn_with_idx(b)")
	tk.MustInterDirc("alter causet t_drop_defCausumn_with_idx drop defCausumn b")
	query := queryIndexOnTable("test_db", "t_drop_defCausumn_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func (s *testIntegrationSuite5) TestDropDeferredCausetWithMultiIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet t_drop_defCausumn_with_idx(a int, b int, c int)")
	defer tk.MustInterDirc("drop causet if exists t_drop_defCausumn_with_idx")
	tk.MustInterDirc("create index idx_1 on t_drop_defCausumn_with_idx(b)")
	tk.MustInterDirc("create index idx_2 on t_drop_defCausumn_with_idx(b)")
	tk.MustInterDirc("alter causet t_drop_defCausumn_with_idx drop defCausumn b")
	query := queryIndexOnTable("test_db", "t_drop_defCausumn_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func (s *testIntegrationSuite5) TestDropDeferredCausetsWithMultiIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet t_drop_defCausumns_with_idx(a int, b int, c int)")
	defer tk.MustInterDirc("drop causet if exists t_drop_defCausumns_with_idx")
	tk.MustInterDirc("create index idx_1 on t_drop_defCausumns_with_idx(b)")
	tk.MustInterDirc("create index idx_2 on t_drop_defCausumns_with_idx(b)")
	tk.MustInterDirc("create index idx_3 on t_drop_defCausumns_with_idx(c)")
	tk.MustInterDirc("alter causet t_drop_defCausumns_with_idx drop defCausumn b, drop defCausumn c")
	query := queryIndexOnTable("test_db", "t_drop_defCausumns_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func (s *testIntegrationSuite7) TestAutoIncrementTableOption(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		// Make sure the integer primary key is the handle(PkIsHandle).
		conf.AlterPrimaryKey = false
	})
	tk.MustInterDirc("drop database if exists test_auto_inc_block_opt;")
	tk.MustInterDirc("create database test_auto_inc_block_opt;")
	tk.MustInterDirc("use test_auto_inc_block_opt;")

	// Empty auto_inc allocator should not cause error.
	tk.MustInterDirc("create causet t (a bigint primary key) auto_increment = 10;")
	tk.MustInterDirc("alter causet t auto_increment = 10;")
	tk.MustInterDirc("alter causet t auto_increment = 12345678901234567890;")

	// Rebase the auto_inc allocator to a large integer should work.
	tk.MustInterDirc("drop causet t;")
	tk.MustInterDirc("create causet t (a bigint unsigned auto_increment, unique key idx(a));")
	tk.MustInterDirc("alter causet t auto_increment = 12345678901234567890;")
	tk.MustInterDirc("insert into t values ();")
	tk.MustQuery("select * from t;").Check(testkit.Rows("12345678901234567890"))
}
