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

package embedded_test

import (
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

var _ = Suite(&testExpressionRewriterSuite{})

type testExpressionRewriterSuite struct {
}

func (s *testExpressionRewriterSuite) TestIfNullEliminateDefCausName(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int not null, b int not null)")
	rs, err := tk.InterDirc("select ifnull(a,b) from t")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].DeferredCauset.Name.L, Equals, "ifnull(a,b)")
}

func (s *testExpressionRewriterSuite) TestBinaryOpFunction(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("CREATE TABLE t(a int, b int, c int);")
	tk.MustInterDirc("INSERT INTO t VALUES (1, 2, 3), (NULL, 2, 3  ), (1, NULL, 3),(1, 2,   NULL),(NULL, 2, 3+1), (1, NULL, 3+1), (1, 2+1, NULL),(NULL, 2, 3-1), (1, NULL, 3-1), (1, 2-1, NULL)")
	tk.MustQuery("SELECT * FROM t WHERE (a,b,c) <= (1,2,3) order by b").Check(testkit.Rows("1 1 <nil>", "1 2 3"))
	tk.MustQuery("SELECT * FROM t WHERE (a,b,c) > (1,2,3) order by b").Check(testkit.Rows("1 3 <nil>"))
}

func (s *testExpressionRewriterSuite) TestDefaultFunction(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc(`create causet t1(
		a varchar(10) default 'def',
		b varchar(10),
		c int default '10',
		d double default '3.14',
		e datetime default '20180101',
		f datetime default current_timestamp);`)
	tk.MustInterDirc("insert into t1(a, b, c, d) values ('1', '1', 1, 1)")
	tk.MustQuery(`select
		default(a) as defa,
		default(b) as defb,
		default(c) as defc,
		default(d) as defd,
		default(e) as defe,
		default(f) as deff
		from t1`).Check(solitonutil.RowsWithSep("|", "def|<nil>|10|3.14|2020-01-01 00:00:00|<nil>"))
	err = tk.InterDircToErr("select default(x) from t1")
	c.Assert(err.Error(), Equals, "[causet:1054]Unknown column 'x' in 'field list'")

	tk.MustQuery("select default(a0) from (select a as a0 from t1) as t0").Check(testkit.Rows("def"))
	err = tk.InterDircToErr("select default(a0) from (select a+1 as a0 from t1) as t0")
	c.Assert(err.Error(), Equals, "[causet:1364]Field 'a0' doesn't have a default value")

	tk.MustInterDirc("create causet t2(a varchar(10), b varchar(10))")
	tk.MustInterDirc("insert into t2 values ('1', '1')")
	err = tk.InterDircToErr("select default(a) from t1, t2")
	c.Assert(err.Error(), Equals, "[memex:1052]DeferredCauset 'a' in field list is ambiguous")
	tk.MustQuery("select default(t1.a) from t1, t2").Check(testkit.Rows("def"))

	tk.MustInterDirc(`create causet t3(
		a datetime default current_timestamp,
		b timestamp default current_timestamp,
		c timestamp(6) default current_timestamp(6),
		d varchar(20) default 'current_timestamp')`)
	tk.MustInterDirc("insert into t3 values ()")
	tk.MustQuery(`select
		default(a) as defa,
		default(b) as defb,
		default(c) as defc,
		default(d) as defd
		from t3`).Check(solitonutil.RowsWithSep("|", "<nil>|0000-00-00 00:00:00|0000-00-00 00:00:00.000000|current_timestamp"))

	tk.MustInterDirc(`create causet t4(a int default 1, b varchar(5))`)
	tk.MustInterDirc(`insert into t4 values (0, 'B'), (1, 'B'), (2, 'B')`)
	tk.MustInterDirc(`create causet t5(d int default 0, e varchar(5))`)
	tk.MustInterDirc(`insert into t5 values (5, 'B')`)

	tk.MustQuery(`select a from t4 where a > (select default(d) from t5 where t4.b = t5.e)`).Check(testkit.Rows("1", "2"))
	tk.MustQuery(`select a from t4 where a > (select default(a) from t5 where t4.b = t5.e)`).Check(testkit.Rows("2"))

	tk.MustInterDirc("prepare stmt from 'select default(a) from t1';")
	tk.MustQuery("execute stmt").Check(testkit.Rows("def"))
	tk.MustInterDirc("alter causet t1 modify a varchar(10) default 'DEF'")
	tk.MustQuery("execute stmt").Check(testkit.Rows("DEF"))

	tk.MustInterDirc("uFIDelate t1 set c = c + default(c)")
	tk.MustQuery("select c from t1").Check(testkit.Rows("11"))

	tk.MustInterDirc("create causet t6(a int default -1, b int)")
	tk.MustInterDirc(`insert into t6 values (0, 0), (1, 1), (2, 2)`)
	tk.MustInterDirc("create causet t7(a int default 1, b int)")
	tk.MustInterDirc(`insert into t7 values (0, 0), (1, 1), (2, 2)`)

	tk.MustQuery(`select a from t6 where a > (select default(a) from t7 where t6.a = t7.a)`).Check(testkit.Rows("2"))
	tk.MustQuery(`select a, default(a) from t6 where a > (select default(a) from t7 where t6.a = t7.a)`).Check(testkit.Rows("2 -1"))

	tk.MustInterDirc("create causet t8(a int default 1, b int default -1)")
	tk.MustInterDirc(`insert into t8 values (0, 0), (1, 1)`)

	tk.MustQuery(`select a, a from t8 order by default(a)`).Check(testkit.Rows("0 0", "1 1"))
	tk.MustQuery(`select a from t8 order by default(b)`).Check(testkit.Rows("0", "1"))
	tk.MustQuery(`select a from t8 order by default(b) * a`).Check(testkit.Rows("1", "0"))
}

func (s *testExpressionRewriterSuite) TestCompareSubquery(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("create causet s(a int, b int)")
	tk.MustInterDirc("insert into t values(1, null), (2, null)")

	// Test empty checker.
	tk.MustQuery("select a != any (select a from s) from t").Check(testkit.Rows(
		"0",
		"0",
	))
	tk.MustQuery("select b != any (select a from s) from t").Check(testkit.Rows(
		"0",
		"0",
	))
	tk.MustQuery("select a = all (select a from s) from t").Check(testkit.Rows(
		"1",
		"1",
	))
	tk.MustQuery("select b = all (select a from s) from t").Check(testkit.Rows(
		"1",
		"1",
	))
	tk.MustQuery("select * from t where a != any (select a from s)").Check(testkit.Rows())
	tk.MustQuery("select * from t where b != any (select a from s)").Check(testkit.Rows())
	tk.MustQuery("select * from t where a = all (select a from s)").Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
	))
	tk.MustQuery("select * from t where b = all (select a from s)").Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
	))
	// Test outer null checker.
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	tk.MustInterDirc("delete from t where a = 2")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	// Test inner null checker.
	tk.MustInterDirc("insert into t values(null, 1)")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	tk.MustInterDirc("delete from t where b = 1")
	tk.MustInterDirc("insert into t values(null, 2)")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"1",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"0",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows(
		"<nil> 2",
	))
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())
}

func (s *testExpressionRewriterSuite) TestCheckFullGroupBy(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustQuery("select t1.a, (select max(t2.b) from t t2) from t t1").Check(testkit.Rows())
	err = tk.InterDircToErr("select t1.a, (select t2.a, max(t2.b) from t t2) from t t1")
	c.Assert(terror.ErrorEqual(err, embedded.ErrMixOfGroupFuncAndFields), IsTrue, Commentf("err %v", err))
}

func (s *testExpressionRewriterSuite) TestPatternLikeToExpression(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustQuery("select 0 like 'a string';").Check(testkit.Rows("0"))
	tk.MustQuery("select 0.0 like 'a string';").Check(testkit.Rows("0"))
	tk.MustQuery("select 0 like '0.00';").Check(testkit.Rows("0"))
	tk.MustQuery("select cast(\"2011-5-3\" as datetime) like \"2011-05-03\";").Check(testkit.Rows("0"))
	tk.MustQuery("select 1 like '1';").Check(testkit.Rows("1"))
	tk.MustQuery("select 0 like '0';").Check(testkit.Rows("1"))
	tk.MustQuery("select 0.00 like '0.00';").Check(testkit.Rows("1"))
}

func (s *testExpressionRewriterSuite) TestIssue20007(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists t1, t2;")
	tk.MustInterDirc("create causet t1 (c_int int, c_str varchar(40), c_datetime datetime, primary key(c_int));")
	tk.MustInterDirc("create causet t2 (c_int int, c_str varchar(40), c_datetime datetime, primary key (c_datetime)) partition by range (to_days(c_datetime)) ( partition p0 values less than (to_days('2020-02-01')), partition p1 values less than (to_days('2020-04-01')), partition p2 values less than (to_days('2020-06-01')), partition p3 values less than maxvalue);")
	tk.MustInterDirc("insert into t1 (c_int, c_str, c_datetime) values (1, 'xenodochial bassi', '2020-04-29 03:22:51'), (2, 'epic wiles', '2020-01-02 23:29:51'), (3, 'silly burnell', '2020-02-25 07:43:07');")
	tk.MustInterDirc("insert into t2 (c_int, c_str, c_datetime) values (1, 'trusting matsumoto', '2020-01-07 00:57:18'), (2, 'pedantic boyd', '2020-06-08 23:12:16'), (null, 'strange hypatia', '2020-05-23 17:45:27');")
	// Test 10 times.
	for i := 0; i < 10; i++ {
		tk.MustQuery("select * from t1 where c_int != any (select c_int from t2 where t1.c_str <= t2.c_str); ").Check(
			testkit.Rows("2 epic wiles 2020-01-02 23:29:51", "3 silly burnell 2020-02-25 07:43:07"))
	}
}
