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

package interlock_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

type testSuiteJoin1 struct {
	*baseTestSuite
}

type testSuiteJoin2 struct {
	*baseTestSuite
}

type testSuiteJoin3 struct {
	*baseTestSuite
}

type testSuiteJoinSerial struct {
	*baseTestSuite
}

func (s *testSuiteJoin1) TestJoinPanic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustInterDirc("drop causet if exists events")
	tk.MustInterDirc("create causet events (clock int, source int)")
	tk.MustQuery("SELECT * FROM events e JOIN (SELECT MAX(clock) AS clock FROM events e2 GROUP BY e2.source) e3 ON e3.clock=e.clock")
	err := tk.InterDircToErr("SELECT * FROM events e JOIN (SELECT clock FROM events e2 GROUP BY e2.source) e3 ON e3.clock=e.clock")
	c.Check(err, NotNil)

	// Test for PR 18983, use to detect race.
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists tpj1,tpj2;")
	tk.MustInterDirc("create causet tpj1 (id int, b int,  unique index (id));")
	tk.MustInterDirc("create causet tpj2 (id int, b int,  unique index (id));")
	tk.MustInterDirc("insert into tpj1 values  (1,1);")
	tk.MustInterDirc("insert into tpj2 values  (1,1);")
	tk.MustQuery("select tpj1.b,tpj2.b from tpj1 left join tpj2 on tpj1.id=tpj2.id where tpj1.id=1;").Check(testkit.Events("1 1"))
}

func (s *testSuite) TestJoinInDisk(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	sm := &mockStochastikManager1{
		PS: make([]*soliton.ProcessInfo, 0),
	}
	tk.Se.SetStochastikManager(sm)
	s.petri.ExpensiveQueryHandle().SetStochastikManager(sm)

	// TODO(fengliyuan): how to ensure that it is using disk really?
	tk.MustInterDirc("set @@milevadb_mem_quota_query=1;")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int, c2 int)")
	tk.MustInterDirc("create causet t1(c1 int, c2 int)")
	tk.MustInterDirc("insert into t values(1,1),(2,2)")
	tk.MustInterDirc("insert into t1 values(2,3),(4,4)")
	result := tk.MustQuery("select /*+ MilevaDB_HJ(t, t2) */ * from t, t1 where t.c1 = t1.c1")
	result.Check(testkit.Events("2 2 2 3"))
}

func (s *testSuiteJoin2) TestJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("set @@milevadb_index_lookup_join_concurrency = 200")
	c.Assert(tk.Se.GetStochastikVars().IndexLookupJoinConcurrency(), Equals, 200)

	tk.MustInterDirc("set @@milevadb_index_lookup_join_concurrency = 4")
	c.Assert(tk.Se.GetStochastikVars().IndexLookupJoinConcurrency(), Equals, 4)

	tk.MustInterDirc("set @@milevadb_index_lookup_size = 2")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c int)")
	tk.MustInterDirc("insert t values (1)")
	tests := []struct {
		allegrosql string
		result     [][]interface{}
	}{
		{
			"select 1 from t as a left join t as b on 0",
			testkit.Events("1"),
		},
		{
			"select 1 from t as a join t as b on 1",
			testkit.Events("1"),
		},
	}
	for _, tt := range tests {
		result := tk.MustQuery(tt.allegrosql)
		result.Check(tt.result)
	}

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int, c2 int)")
	tk.MustInterDirc("create causet t1(c1 int, c2 int)")
	tk.MustInterDirc("insert into t values(1,1),(2,2)")
	tk.MustInterDirc("insert into t1 values(2,3),(4,4)")
	result := tk.MustQuery("select * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Events("1 1 <nil> <nil>"))
	result = tk.MustQuery("select * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Events("<nil> <nil> 1 1"))
	result = tk.MustQuery("select * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Events())
	result = tk.MustQuery("select * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false")
	result.Check(testkit.Events())
	result = tk.MustQuery("select * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1")
	result.Check(testkit.Events("1 1 <nil> <nil>", "2 2 2 3"))
	result = tk.MustQuery("select t.c1, t1.c1 from t left outer join t1 on t.c1 = t1.c1 and t.c2 + t1.c2 <= 5")
	result.Check(testkit.Events("1 <nil>", "2 2"))

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("drop causet if exists t3")

	tk.MustInterDirc("create causet t1 (c1 int, c2 int)")
	tk.MustInterDirc("create causet t2 (c1 int, c2 int)")
	tk.MustInterDirc("create causet t3 (c1 int, c2 int)")

	tk.MustInterDirc("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustInterDirc("insert into t2 values (1,1), (3,3), (5,5)")
	tk.MustInterDirc("insert into t3 values (1,1), (5,5), (9,9)")

	result = tk.MustQuery("select * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;")
	result.Check(testkit.Events("<nil> <nil> <nil> <nil> 5 5", "<nil> <nil> <nil> <nil> 9 9", "1 1 1 1 1 1"))

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int)")
	tk.MustInterDirc("insert into t1 values (1), (1), (1)")
	result = tk.MustQuery("select * from t1 a join t1 b on a.c1 = b.c1;")
	result.Check(testkit.Events("1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int, index k(c1))")
	tk.MustInterDirc("create causet t1(c1 int)")
	tk.MustInterDirc("insert into t values (1),(2),(3),(4),(5),(6),(7)")
	tk.MustInterDirc("insert into t1 values (1),(2),(3),(4),(5),(6),(7)")
	result = tk.MustQuery("select a.c1 from t a , t1 b where a.c1 = b.c1 order by a.c1;")
	result.Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	// Test race.
	result = tk.MustQuery("select a.c1 from t a , t1 b where a.c1 = b.c1 and a.c1 + b.c1 > 5 order by b.c1")
	result.Check(testkit.Events("3", "4", "5", "6", "7"))
	result = tk.MustQuery("select a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 order by b.c1;")
	result.Check(testkit.Events("1", "2", "3"))

	tk.MustInterDirc("drop causet if exists t,t2,t1")
	tk.MustInterDirc("create causet t(c1 int)")
	tk.MustInterDirc("create causet t1(c1 int, c2 int)")
	tk.MustInterDirc("create causet t2(c1 int, c2 int)")
	tk.MustInterDirc("insert into t1 values(1,2),(2,3),(3,4)")
	tk.MustInterDirc("insert into t2 values(1,0),(2,0),(3,0)")
	tk.MustInterDirc("insert into t values(1),(2),(3)")
	result = tk.MustQuery("select * from t1 , t2 where t2.c1 = t1.c1 and t2.c2 = 0 and t1.c2 in (select * from t)")
	result.Sort().Check(testkit.Events("1 2 1 0", "2 3 2 0"))
	result = tk.MustQuery("select * from t1 , t2 where t2.c1 = t1.c1 and t2.c2 = 0 and t1.c1 = 1 order by t1.c2 limit 1")
	result.Sort().Check(testkit.Events("1 2 1 0"))
	tk.MustInterDirc("drop causet if exists t, t1")
	tk.MustInterDirc("create causet t(a int primary key, b int)")
	tk.MustInterDirc("create causet t1(a int, b int, key s(b))")
	tk.MustInterDirc("insert into t values(1, 1), (2, 2), (3, 3)")
	tk.MustInterDirc("insert into t1 values(1, 2), (1, 3), (1, 4), (3, 4), (4, 5)")

	// The physical plans of the two allegrosql are tested at physical_plan_test.go
	tk.MustQuery("select /*+ INL_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Check(testkit.Events("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Sort().Check(testkit.Events("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Check(testkit.Events("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Events("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Sort().Check(testkit.Events("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Events("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	// Test single index reader.
	tk.MustQuery("select /*+ INL_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Events("2", "3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Sort().Check(testkit.Events("2", "3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Events("2", "3"))
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Check(testkit.Events("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Check(testkit.Events("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Check(testkit.Events("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Events("1.5000"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Events("1.5000"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Events("1.5000"))

	// Test that two conflict hints will return warning.
	tk.MustInterDirc("select /*+ MilevaDB_INLJ(t) MilevaDB_SMJ(t) */ * from t join t1 on t.a=t1.a")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	tk.MustInterDirc("select /*+ MilevaDB_INLJ(t) MilevaDB_HJ(t) */ * from t join t1 on t.a=t1.a")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	tk.MustInterDirc("select /*+ MilevaDB_SMJ(t) MilevaDB_HJ(t) */ * from t join t1 on t.a=t1.a")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("insert into t values(1),(2), (3)")
	tk.MustQuery("select @a := @a + 1 from t, (select @a := 0) b;").Check(testkit.Events("1", "2", "3"))

	tk.MustInterDirc("drop causet if exists t, t1")
	tk.MustInterDirc("create causet t(a int primary key, b int, key s(b))")
	tk.MustInterDirc("create causet t1(a int, b int)")
	tk.MustInterDirc("insert into t values(1, 3), (2, 2), (3, 1)")
	tk.MustInterDirc("insert into t1 values(0, 0), (1, 2), (1, 3), (3, 4)")
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Events("1 3 1 2", "1 3 1 3", "3 1 3 4"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Events("1 3 1 2", "1 3 1 3", "3 1 3 4"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Events("1 3 1 2", "1 3 1 3", "3 1 3 4"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").Check(testkit.Events("3 1"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").Check(testkit.Events("3 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").Check(testkit.Events("3 1"))
	tk.MustQuery("select /*+ INL_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Events("1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Events("1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Events("1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"))

	// join reorder will disorganize the resulting schemaReplicant
	tk.MustInterDirc("drop causet if exists t, t1")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("create causet t1(a int, b int)")
	tk.MustInterDirc("insert into t values(1,2)")
	tk.MustInterDirc("insert into t1 values(3,4)")
	tk.MustQuery("select (select t1.a from t1 , t where t.a = s.a limit 2) from t as s").Check(testkit.Events("3"))

	// test index join bug
	tk.MustInterDirc("drop causet if exists t, t1")
	tk.MustInterDirc("create causet t(a int, b int, key s1(a,b), key s2(b))")
	tk.MustInterDirc("create causet t1(a int)")
	tk.MustInterDirc("insert into t values(1,2), (5,3), (6,4)")
	tk.MustInterDirc("insert into t1 values(1), (2), (3)")
	tk.MustQuery("select /*+ INL_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").Check(testkit.Events("3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").Check(testkit.Events("3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").Check(testkit.Events("3"))

	// test issue#4997
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc(`
	CREATE TABLE t1 (
  		pk int(11) NOT NULL AUTO_INCREMENT primary key,
  		a int(11) DEFAULT NULL,
  		b date DEFAULT NULL,
  		c varchar(1) DEFAULT NULL,
  		KEY a (a),
  		KEY b (b),
  		KEY c (c,a)
	)`)
	tk.MustInterDirc(`
	CREATE TABLE t2 (
  		pk int(11) NOT NULL AUTO_INCREMENT primary key,
  		a int(11) DEFAULT NULL,
  		b date DEFAULT NULL,
  		c varchar(1) DEFAULT NULL,
  		KEY a (a),
  		KEY b (b),
  		KEY c (c,a)
	)`)
	tk.MustInterDirc(`insert into t1 value(1,1,"2000-11-11", null);`)
	result = tk.MustQuery(`
	SELECT block2.b AS field2 FROM
	(
	  t1 AS block1  LEFT OUTER JOIN
		(SELECT tmp_t2.* FROM ( t2 AS tmp_t1 RIGHT JOIN t1 AS tmp_t2 ON (tmp_t2.a = tmp_t1.a))) AS block2
	  ON (block2.c = block1.c)
	) `)
	result.Check(testkit.Events("<nil>"))

	// test virtual rows are included (issue#5771)
	result = tk.MustQuery(`SELECT 1 FROM (SELECT 1) t1, (SELECT 1) t2`)
	result.Check(testkit.Events("1"))

	result = tk.MustQuery(`
		SELECT @NUM := @NUM + 1 as NUM FROM
		( SELECT 1 UNION ALL
			SELECT 2 UNION ALL
			SELECT 3
		) a
		INNER JOIN
		( SELECT 1 UNION ALL
			SELECT 2 UNION ALL
			SELECT 3
		) b,
		(SELECT @NUM := 0) d;
	`)
	result.Check(testkit.Events("1", "2", "3", "4", "5", "6", "7", "8", "9"))

	// This case is for testing:
	// when the main thread calls InterlockingDirectorate.Close() while the out data fetch worker and join workers are still working,
	// we need to stop the goroutines as soon as possible to avoid unexpected error.
	tk.MustInterDirc("set @@milevadb_hash_join_concurrency=5")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t(a int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into t value(1)")
	}
	result = tk.MustQuery("select /*+ MilevaDB_HJ(s, r) */ * from t as s join t as r on s.a = r.a limit 1;")
	result.Check(testkit.Events("1 1"))

	tk.MustInterDirc("drop causet if exists user, aa, bb")
	tk.MustInterDirc("create causet aa(id int)")
	tk.MustInterDirc("insert into aa values(1)")
	tk.MustInterDirc("create causet bb(id int)")
	tk.MustInterDirc("insert into bb values(1)")
	tk.MustInterDirc("create causet user(id int, name varchar(20))")
	tk.MustInterDirc("insert into user values(1, 'a'), (2, 'b')")
	tk.MustQuery("select user.id,user.name from user left join aa on aa.id = user.id left join bb on aa.id = bb.id where bb.id < 10;").Check(testkit.Events("1 a"))

	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t (a bigint);`)
	tk.MustInterDirc(`insert into t values (1);`)
	tk.MustQuery(`select t2.a, t1.a from t t1 inner join (select "1" as a) t2 on t2.a = t1.a;`).Check(testkit.Events("1 1"))
	tk.MustQuery(`select t2.a, t1.a from t t1 inner join (select "2" as b, "1" as a) t2 on t2.a = t1.a;`).Check(testkit.Events("1 1"))

	tk.MustInterDirc("drop causet if exists t1, t2, t3, t4")
	tk.MustInterDirc("create causet t1(a int, b int)")
	tk.MustInterDirc("create causet t2(a int, b int)")
	tk.MustInterDirc("create causet t3(a int, b int)")
	tk.MustInterDirc("create causet t4(a int, b int)")
	tk.MustInterDirc("insert into t1 values(1, 1)")
	tk.MustInterDirc("insert into t2 values(1, 1)")
	tk.MustInterDirc("insert into t3 values(1, 1)")
	tk.MustInterDirc("insert into t4 values(1, 1)")
	tk.MustQuery("select min(t2.b) from t1 right join t2 on t2.a=t1.a right join t3 on t2.a=t3.a left join t4 on t3.a=t4.a").Check(testkit.Events("1"))
}

func (s *testSuiteJoin2) TestJoinCast(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	var result *testkit.Result

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int)")
	tk.MustInterDirc("create causet t1(c1 int unsigned)")
	tk.MustInterDirc("insert into t values (1)")
	tk.MustInterDirc("insert into t1 values (1)")
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Events("1"))

	// int64(-1) != uint64(18446744073709551615)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 bigint)")
	tk.MustInterDirc("create causet t1(c1 bigint unsigned)")
	tk.MustInterDirc("insert into t values (-1)")
	tk.MustInterDirc("insert into t1 values (18446744073709551615)")
	result = tk.MustQuery("select * from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Events())

	// float(1) == double(1)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 float)")
	tk.MustInterDirc("create causet t1(c1 double)")
	tk.MustInterDirc("insert into t values (1.0)")
	tk.MustInterDirc("insert into t1 values (1.00)")
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Events("1"))

	// varchar("x") == char("x")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 varchar(1))")
	tk.MustInterDirc("create causet t1(c1 char(1))")
	tk.MustInterDirc(`insert into t values ("x")`)
	tk.MustInterDirc(`insert into t1 values ("x")`)
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Events("x"))

	// varchar("x") != char("y")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 varchar(1))")
	tk.MustInterDirc("create causet t1(c1 char(1))")
	tk.MustInterDirc(`insert into t values ("x")`)
	tk.MustInterDirc(`insert into t1 values ("y")`)
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Events())

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int,c2 double)")
	tk.MustInterDirc("create causet t1(c1 double,c2 int)")
	tk.MustInterDirc("insert into t values (1, 2), (1, NULL)")
	tk.MustInterDirc("insert into t1 values (1, 2), (1, NULL)")
	result = tk.MustQuery("select * from t a , t1 b where (a.c1, a.c2) = (b.c1, b.c2);")
	result.Check(testkit.Events("1 2 1 2"))

	/* Issue 11895 */
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t(c1 bigint unsigned);")
	tk.MustInterDirc("create causet t1(c1 bit(64));")
	tk.MustInterDirc("insert into t value(18446744073709551615);")
	tk.MustInterDirc("insert into t1 value(-1);")
	result = tk.MustQuery("select * from t, t1 where t.c1 = t1.c1;")
	c.Check(len(result.Events()), Equals, 1)

	/* Issues 11896 */
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t(c1 bigint);")
	tk.MustInterDirc("create causet t1(c1 bit(64));")
	tk.MustInterDirc("insert into t value(1);")
	tk.MustInterDirc("insert into t1 value(1);")
	result = tk.MustQuery("select * from t, t1 where t.c1 = t1.c1;")
	c.Check(len(result.Events()), Equals, 1)

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t(c1 bigint);")
	tk.MustInterDirc("create causet t1(c1 bit(64));")
	tk.MustInterDirc("insert into t value(-1);")
	tk.MustInterDirc("insert into t1 value(18446744073709551615);")
	result = tk.MustQuery("select * from t, t1 where t.c1 = t1.c1;")
	// TODO: MyALLEGROSQL will return one event, because c1 in t1 is 0xffffffff, which equals to -1.
	c.Check(len(result.Events()), Equals, 0)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t(c1 bigint)")
	tk.MustInterDirc("create causet t1(c1 bigint unsigned)")
	tk.MustInterDirc("create causet t2(c1 Date)")
	tk.MustInterDirc("insert into t value(20191111)")
	tk.MustInterDirc("insert into t1 value(20191111)")
	tk.MustInterDirc("insert into t2 value('2020-11-11')")
	result = tk.MustQuery("select * from t, t1, t2 where t.c1 = t2.c1 and t1.c1 = t2.c1")
	result.Check(testkit.Events("20191111 20191111 2020-11-11"))

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2;")
	tk.MustInterDirc("create causet t(c1 bigint);")
	tk.MustInterDirc("create causet t1(c1 bigint unsigned);")
	tk.MustInterDirc("create causet t2(c1 enum('a', 'b', 'c', 'd'));")
	tk.MustInterDirc("insert into t value(3);")
	tk.MustInterDirc("insert into t1 value(3);")
	tk.MustInterDirc("insert into t2 value('c');")
	result = tk.MustQuery("select * from t, t1, t2 where t.c1 = t2.c1 and t1.c1 = t2.c1;")
	result.Check(testkit.Events("3 3 c"))

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("drop causet if exists t2;")
	tk.MustInterDirc("create causet t(c1 bigint);")
	tk.MustInterDirc("create causet t1(c1 bigint unsigned);")
	tk.MustInterDirc("create causet t2 (c1 SET('a', 'b', 'c', 'd'));")
	tk.MustInterDirc("insert into t value(9);")
	tk.MustInterDirc("insert into t1 value(9);")
	tk.MustInterDirc("insert into t2 value('a,d');")
	result = tk.MustQuery("select * from t, t1, t2 where t.c1 = t2.c1 and t1.c1 = t2.c1;")
	result.Check(testkit.Events("9 9 a,d"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int)")
	tk.MustInterDirc("create causet t1(c1 decimal(4,2))")
	tk.MustInterDirc("insert into t values(0), (2)")
	tk.MustInterDirc("insert into t1 values(0), (9)")
	result = tk.MustQuery("select * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Events("0 0.00", "2 <nil>"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 decimal(4,1))")
	tk.MustInterDirc("create causet t1(c1 decimal(4,2))")
	tk.MustInterDirc("insert into t values(0), (2)")
	tk.MustInterDirc("insert into t1 values(0), (9)")
	result = tk.MustQuery("select * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Events("0.0 0.00", "2.0 <nil>"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 decimal(4,1))")
	tk.MustInterDirc("create causet t1(c1 decimal(4,2))")
	tk.MustInterDirc("create index k1 on t1(c1)")
	tk.MustInterDirc("insert into t values(0), (2)")
	tk.MustInterDirc("insert into t1 values(0), (9)")
	result = tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Events("0.0 0.00", "2.0 <nil>"))
	result = tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Events("0.0 0.00", "2.0 <nil>"))
	result = tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Events("0.0 0.00", "2.0 <nil>"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t(c1 char(10))")
	tk.MustInterDirc("create causet t1(c1 char(10))")
	tk.MustInterDirc("create causet t2(c1 char(10))")
	tk.MustInterDirc("insert into t values('abd')")
	tk.MustInterDirc("insert into t1 values('abc')")
	tk.MustInterDirc("insert into t2 values('abc')")
	result = tk.MustQuery("select * from (select * from t union all select * from t1) t1 join t2 on t1.c1 = t2.c1")
	result.Sort().Check(testkit.Events("abc abc"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a varchar(10), index idx(a))")
	tk.MustInterDirc("insert into t values('1'), ('2'), ('3')")
	tk.MustInterDirc("set @@milevadb_init_chunk_size=1")
	result = tk.MustQuery("select a from (select /*+ INL_JOIN(t1, t2) */ t1.a from t t1 join t t2 on t1.a=t2.a) t group by a")
	result.Sort().Check(testkit.Events("1", "2", "3"))
	result = tk.MustQuery("select a from (select /*+ INL_HASH_JOIN(t1, t2) */ t1.a from t t1 join t t2 on t1.a=t2.a) t group by a")
	result.Sort().Check(testkit.Events("1", "2", "3"))
	result = tk.MustQuery("select a from (select /*+ INL_MERGE_JOIN(t1, t2) */ t1.a from t t1 join t t2 on t1.a=t2.a) t group by a")
	result.Sort().Check(testkit.Events("1", "2", "3"))
	tk.MustInterDirc("set @@milevadb_init_chunk_size=32")
}

func (s *testSuiteJoin1) TestUsing(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2, t3, t4")
	tk.MustInterDirc("create causet t1 (a int, c int)")
	tk.MustInterDirc("create causet t2 (a int, d int)")
	tk.MustInterDirc("create causet t3 (a int)")
	tk.MustInterDirc("create causet t4 (a int)")
	tk.MustInterDirc("insert t1 values (2, 4), (1, 3)")
	tk.MustInterDirc("insert t2 values (2, 5), (3, 6)")
	tk.MustInterDirc("insert t3 values (1)")

	tk.MustQuery("select * from t1 join t2 using (a)").Check(testkit.Events("2 4 5"))
	tk.MustQuery("select t1.a, t2.a from t1 join t2 using (a)").Check(testkit.Events("2 2"))

	tk.MustQuery("select * from t1 right join t2 using (a) order by a").Check(testkit.Events("2 5 4", "3 6 <nil>"))
	tk.MustQuery("select t1.a, t2.a from t1 right join t2 using (a) order by t2.a").Check(testkit.Events("2 2", "<nil> 3"))

	tk.MustQuery("select * from t1 left join t2 using (a) order by a").Check(testkit.Events("1 3 <nil>", "2 4 5"))
	tk.MustQuery("select t1.a, t2.a from t1 left join t2 using (a) order by t1.a").Check(testkit.Events("1 <nil>", "2 2"))

	tk.MustQuery("select * from t1 join t2 using (a) right join t3 using (a)").Check(testkit.Events("1 <nil> <nil>"))
	tk.MustQuery("select * from t1 join t2 using (a) right join t3 on (t2.a = t3.a)").Check(testkit.Events("<nil> <nil> <nil> 1"))
	tk.MustQuery("select t2.a from t1 join t2 using (a) right join t3 on (t1.a = t3.a)").Check(testkit.Events("<nil>"))
	tk.MustQuery("select t1.a, t2.a, t3.a from t1 join t2 using (a) right join t3 using (a)").Check(testkit.Events("<nil> <nil> 1"))
	tk.MustQuery("select t1.c, t2.d from t1 join t2 using (a) right join t3 using (a)").Check(testkit.Events("<nil> <nil>"))

	tk.MustInterDirc("alter causet t1 add defCausumn b int default 1 after a")
	tk.MustInterDirc("alter causet t2 add defCausumn b int default 1 after a")
	tk.MustQuery("select * from t1 join t2 using (b, a)").Check(testkit.Events("2 1 4 5"))

	tk.MustInterDirc("select * from (t1 join t2 using (a)) join (t3 join t4 using (a)) on (t2.a = t4.a and t1.a = t3.a)")

	tk.MustInterDirc("drop causet if exists t, tt")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("create causet tt(b int, a int)")
	tk.MustInterDirc("insert into t (a, b) values(1, 1)")
	tk.MustInterDirc("insert into tt (a, b) values(1, 2)")
	tk.MustQuery("select * from t join tt using(a)").Check(testkit.Events("1 1 2"))

	tk.MustInterDirc("drop causet if exists t, tt")
	tk.MustInterDirc("create causet t(a float, b int)")
	tk.MustInterDirc("create causet tt(b bigint, a int)")
	// Check whether this allegrosql can execute successfully.
	tk.MustInterDirc("select * from t join tt using(a)")
}

func (s *testSuiteJoin1) TestNaturalJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1 (a int, b int)")
	tk.MustInterDirc("create causet t2 (a int, c int)")
	tk.MustInterDirc("insert t1 values (1, 2), (10, 20)")
	tk.MustInterDirc("insert t2 values (1, 3), (100, 200)")

	tk.MustQuery("select * from t1 natural join t2").Check(testkit.Events("1 2 3"))
	tk.MustQuery("select * from t1 natural left join t2 order by a").Check(testkit.Events("1 2 3", "10 20 <nil>"))
	tk.MustQuery("select * from t1 natural right join t2 order by a").Check(testkit.Events("1 3 2", "100 200 <nil>"))
}

func (s *testSuiteJoin3) TestMultiJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t35(a35 int primary key, b35 int, x35 int)")
	tk.MustInterDirc("create causet t40(a40 int primary key, b40 int, x40 int)")
	tk.MustInterDirc("create causet t14(a14 int primary key, b14 int, x14 int)")
	tk.MustInterDirc("create causet t42(a42 int primary key, b42 int, x42 int)")
	tk.MustInterDirc("create causet t15(a15 int primary key, b15 int, x15 int)")
	tk.MustInterDirc("create causet t7(a7 int primary key, b7 int, x7 int)")
	tk.MustInterDirc("create causet t64(a64 int primary key, b64 int, x64 int)")
	tk.MustInterDirc("create causet t19(a19 int primary key, b19 int, x19 int)")
	tk.MustInterDirc("create causet t9(a9 int primary key, b9 int, x9 int)")
	tk.MustInterDirc("create causet t8(a8 int primary key, b8 int, x8 int)")
	tk.MustInterDirc("create causet t57(a57 int primary key, b57 int, x57 int)")
	tk.MustInterDirc("create causet t37(a37 int primary key, b37 int, x37 int)")
	tk.MustInterDirc("create causet t44(a44 int primary key, b44 int, x44 int)")
	tk.MustInterDirc("create causet t38(a38 int primary key, b38 int, x38 int)")
	tk.MustInterDirc("create causet t18(a18 int primary key, b18 int, x18 int)")
	tk.MustInterDirc("create causet t62(a62 int primary key, b62 int, x62 int)")
	tk.MustInterDirc("create causet t4(a4 int primary key, b4 int, x4 int)")
	tk.MustInterDirc("create causet t48(a48 int primary key, b48 int, x48 int)")
	tk.MustInterDirc("create causet t31(a31 int primary key, b31 int, x31 int)")
	tk.MustInterDirc("create causet t16(a16 int primary key, b16 int, x16 int)")
	tk.MustInterDirc("create causet t12(a12 int primary key, b12 int, x12 int)")
	tk.MustInterDirc("insert into t35 values(1,1,1)")
	tk.MustInterDirc("insert into t40 values(1,1,1)")
	tk.MustInterDirc("insert into t14 values(1,1,1)")
	tk.MustInterDirc("insert into t42 values(1,1,1)")
	tk.MustInterDirc("insert into t15 values(1,1,1)")
	tk.MustInterDirc("insert into t7 values(1,1,1)")
	tk.MustInterDirc("insert into t64 values(1,1,1)")
	tk.MustInterDirc("insert into t19 values(1,1,1)")
	tk.MustInterDirc("insert into t9 values(1,1,1)")
	tk.MustInterDirc("insert into t8 values(1,1,1)")
	tk.MustInterDirc("insert into t57 values(1,1,1)")
	tk.MustInterDirc("insert into t37 values(1,1,1)")
	tk.MustInterDirc("insert into t44 values(1,1,1)")
	tk.MustInterDirc("insert into t38 values(1,1,1)")
	tk.MustInterDirc("insert into t18 values(1,1,1)")
	tk.MustInterDirc("insert into t62 values(1,1,1)")
	tk.MustInterDirc("insert into t4 values(1,1,1)")
	tk.MustInterDirc("insert into t48 values(1,1,1)")
	tk.MustInterDirc("insert into t31 values(1,1,1)")
	tk.MustInterDirc("insert into t16 values(1,1,1)")
	tk.MustInterDirc("insert into t12 values(1,1,1)")
	tk.MustInterDirc("insert into t35 values(7,7,7)")
	tk.MustInterDirc("insert into t40 values(7,7,7)")
	tk.MustInterDirc("insert into t14 values(7,7,7)")
	tk.MustInterDirc("insert into t42 values(7,7,7)")
	tk.MustInterDirc("insert into t15 values(7,7,7)")
	tk.MustInterDirc("insert into t7 values(7,7,7)")
	tk.MustInterDirc("insert into t64 values(7,7,7)")
	tk.MustInterDirc("insert into t19 values(7,7,7)")
	tk.MustInterDirc("insert into t9 values(7,7,7)")
	tk.MustInterDirc("insert into t8 values(7,7,7)")
	tk.MustInterDirc("insert into t57 values(7,7,7)")
	tk.MustInterDirc("insert into t37 values(7,7,7)")
	tk.MustInterDirc("insert into t44 values(7,7,7)")
	tk.MustInterDirc("insert into t38 values(7,7,7)")
	tk.MustInterDirc("insert into t18 values(7,7,7)")
	tk.MustInterDirc("insert into t62 values(7,7,7)")
	tk.MustInterDirc("insert into t4 values(7,7,7)")
	tk.MustInterDirc("insert into t48 values(7,7,7)")
	tk.MustInterDirc("insert into t31 values(7,7,7)")
	tk.MustInterDirc("insert into t16 values(7,7,7)")
	tk.MustInterDirc("insert into t12 values(7,7,7)")
	result := tk.MustQuery(`SELECT x4,x8,x38,x44,x31,x9,x57,x48,x19,x40,x14,x12,x7,x64,x37,x18,x62,x35,x42,x15,x16 FROM
t35,t40,t14,t42,t15,t7,t64,t19,t9,t8,t57,t37,t44,t38,t18,t62,t4,t48,t31,t16,t12
WHERE b48=a57
AND a4=b19
AND a14=b16
AND b37=a48
AND a40=b42
AND a31=7
AND a15=b40
AND a38=b8
AND b15=a31
AND b64=a18
AND b12=a44
AND b7=a8
AND b35=a16
AND a12=b14
AND a64=b57
AND b62=a7
AND a35=b38
AND b9=a19
AND a62=b18
AND b4=a37
AND b44=a42`)
	result.Check(testkit.Events("7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7"))
}

func (s *testSuiteJoin3) TestSubquerySameBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int)")
	tk.MustInterDirc("insert t values (1), (2)")
	result := tk.MustQuery("select a from t where exists(select 1 from t as x where x.a < t.a)")
	result.Check(testkit.Events("2"))
	result = tk.MustQuery("select a from t where not exists(select 1 from t as x where x.a < t.a)")
	result.Check(testkit.Events("1"))
}

func (s *testSuiteJoin3) TestSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("set @@milevadb_hash_join_concurrency=1")
	tk.MustInterDirc("set @@milevadb_hashagg_partial_concurrency=1")
	tk.MustInterDirc("set @@milevadb_hashagg_final_concurrency=1")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c int, d int)")
	tk.MustInterDirc("insert t values (1, 1)")
	tk.MustInterDirc("insert t values (2, 2)")
	tk.MustInterDirc("insert t values (3, 4)")
	tk.MustInterDirc("commit")

	tk.MustInterDirc("set sql_mode = 'STRICT_TRANS_TABLES'")

	result := tk.MustQuery("select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1)")
	result.Check(testkit.Events("1 1"))
	result = tk.MustQuery("select * from t where exists(select k.c, k.d from t k, t p where t.c = k.d)")
	result.Check(testkit.Events("1 1", "2 2"))
	result = tk.MustQuery("select 1 = (select count(*) from t where t.c = k.d) from t k")
	result.Check(testkit.Events("1", "1", "0"))
	result = tk.MustQuery("select 1 = (select count(*) from t where exists( select * from t m where t.c = k.d)) from t k")
	result.Sort().Check(testkit.Events("0", "1", "1"))
	result = tk.MustQuery("select t.c = any (select count(*) from t) from t")
	result.Sort().Check(testkit.Events("0", "0", "1"))
	result = tk.MustQuery("select * from t where (t.c, 6) = any (select count(*), sum(t.c) from t)")
	result.Check(testkit.Events("3 4"))
	result = tk.MustQuery("select t.c from t where (t.c) < all (select count(*) from t)")
	result.Check(testkit.Events("1", "2"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) = any (select * from t)")
	result.Sort().Check(testkit.Events("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) != all (select * from t)")
	result.Check(testkit.Events())
	result = tk.MustQuery("select (select count(*) from t where t.c = k.d) from t k")
	result.Sort().Check(testkit.Events("0", "1", "1"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) in (select * from t)")
	result.Sort().Check(testkit.Events("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) not in (select * from t)")
	result.Check(testkit.Events())
	result = tk.MustQuery("select * from t A inner join t B on A.c = B.c and A.c > 100")
	result.Check(testkit.Events())
	// = all empty set is true
	result = tk.MustQuery("select t.c from t where (t.c, t.d) != all (select * from t where d > 1000)")
	result.Sort().Check(testkit.Events("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c) < any (select c from t where d > 1000)")
	result.Check(testkit.Events())
	tk.MustInterDirc("insert t values (NULL, NULL)")
	result = tk.MustQuery("select (t.c) < any (select c from t) from t")
	result.Sort().Check(testkit.Events("1", "1", "<nil>", "<nil>"))
	result = tk.MustQuery("select (10) > all (select c from t) from t")
	result.Check(testkit.Events("<nil>", "<nil>", "<nil>", "<nil>"))
	result = tk.MustQuery("select (c) > all (select c from t) from t")
	result.Check(testkit.Events("0", "0", "0", "<nil>"))

	tk.MustInterDirc("drop causet if exists a")
	tk.MustInterDirc("create causet a (c int, d int)")
	tk.MustInterDirc("insert a values (1, 2)")
	tk.MustInterDirc("drop causet if exists b")
	tk.MustInterDirc("create causet b (c int, d int)")
	tk.MustInterDirc("insert b values (2, 1)")

	result = tk.MustQuery("select * from a b where c = (select d from b a where a.c = 2 and b.c = 1)")
	result.Check(testkit.Events("1 2"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(c int)")
	tk.MustInterDirc("insert t values(10), (8), (7), (9), (11)")
	result = tk.MustQuery("select * from t where 9 in (select c from t s where s.c < t.c limit 3)")
	result.Check(testkit.Events("10"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(id int, v int)")
	tk.MustInterDirc("insert into t values(1, 1), (2, 2), (3, 3)")
	result = tk.MustQuery("select * from t where v=(select min(t1.v) from t t1, t t2, t t3 where t1.id=t2.id and t2.id=t3.id and t1.id=t.id)")
	result.Check(testkit.Events("1 1", "2 2", "3 3"))

	result = tk.MustQuery("select exists (select t.id from t where s.id < 2 and t.id = s.id) from t s")
	result.Sort().Check(testkit.Events("0", "0", "1"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(c int)")
	result = tk.MustQuery("select exists(select count(*) from t)")
	result.Check(testkit.Events("1"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(id int primary key, v int)")
	tk.MustInterDirc("insert into t values(1, 1), (2, 2), (3, 3)")
	result = tk.MustQuery("select (select t.id from t where s.id < 2 and t.id = s.id) from t s")
	result.Sort().Check(testkit.Events("1", "<nil>", "<nil>"))
	rs, err := tk.InterDirc("select (select t.id from t where t.id = t.v and t.v != s.id) from t s")
	c.Check(err, IsNil)
	_, err = stochastik.GetEvents4Test(context.Background(), tk.Se, rs)
	c.Check(err, NotNil)
	c.Check(rs.Close(), IsNil)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet t(id int)")
	tk.MustInterDirc("create causet s(id int)")
	tk.MustInterDirc("insert into t values(1), (2)")
	tk.MustInterDirc("insert into s values(2), (2)")
	result = tk.MustQuery("select id from t where(select count(*) from s where s.id = t.id) > 0")
	result.Check(testkit.Events("2"))
	result = tk.MustQuery("select *, (select count(*) from s where id = t.id limit 1, 1) from t")
	result.Check(testkit.Events("1 <nil>", "2 <nil>"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet t(id int primary key)")
	tk.MustInterDirc("create causet s(id int)")
	tk.MustInterDirc("insert into t values(1), (2)")
	tk.MustInterDirc("insert into s values(2), (2)")
	result = tk.MustQuery("select *, (select count(id) from s where id = t.id) from t")
	result.Check(testkit.Events("1 0", "2 2"))
	result = tk.MustQuery("select *, 0 < any (select count(id) from s where id = t.id) from t")
	result.Check(testkit.Events("1 0", "2 1"))
	result = tk.MustQuery("select (select count(*) from t k where t.id = id) from s, t where t.id = s.id limit 1")
	result.Check(testkit.Events("1"))

	tk.MustInterDirc("drop causet if exists t, s")
	tk.MustInterDirc("create causet t(id int primary key)")
	tk.MustInterDirc("create causet s(id int, index k(id))")
	tk.MustInterDirc("insert into t values(1), (2)")
	tk.MustInterDirc("insert into s values(2), (2)")
	result = tk.MustQuery("select (select id from s where s.id = t.id order by s.id limit 1) from t")
	result.Check(testkit.Events("<nil>", "2"))

	tk.MustInterDirc("drop causet if exists t, s")
	tk.MustInterDirc("create causet t(id int)")
	tk.MustInterDirc("create causet s(id int)")
	tk.MustInterDirc("insert into t values(2), (2)")
	tk.MustInterDirc("insert into s values(2)")
	result = tk.MustQuery("select (select id from s where s.id = t.id order by s.id) from t")
	result.Check(testkit.Events("2", "2"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(dt datetime)")
	result = tk.MustQuery("select (select 1 from t where DATE_FORMAT(o.dt,'%Y-%m')) from t o")
	result.Check(testkit.Events())

	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(f1 int, f2 int)")
	tk.MustInterDirc("create causet t2(fa int, fb int)")
	tk.MustInterDirc("insert into t1 values (1,1),(1,1),(1,2),(1,2),(1,2),(1,3)")
	tk.MustInterDirc("insert into t2 values (1,1),(1,2),(1,3)")
	result = tk.MustQuery("select f1,f2 from t1 group by f1,f2 having count(1) >= all (select fb from t2 where fa = f1)")
	result.Check(testkit.Events("1 2"))

	tk.MustInterDirc("DROP TABLE IF EXISTS t1, t2")
	tk.MustInterDirc("CREATE TABLE t1(a INT)")
	tk.MustInterDirc("CREATE TABLE t2 (d BINARY(2), PRIMARY KEY (d(1)), UNIQUE KEY (d))")
	tk.MustInterDirc("INSERT INTO t1 values(1)")
	result = tk.MustQuery("SELECT 1 FROM test.t1, test.t2 WHERE 1 = (SELECT test.t2.d FROM test.t2 WHERE test.t1.a >= 1) and test.t2.d = 1;")
	result.Check(testkit.Events())

	tk.MustInterDirc("DROP TABLE IF EXISTS t1")
	tk.MustInterDirc("CREATE TABLE t1(a int, b int default 0)")
	tk.MustInterDirc("create index k1 on t1(a)")
	tk.MustInterDirc("INSERT INTO t1 (a) values(1), (2), (3), (4), (5)")
	result = tk.MustQuery("select (select /*+ INL_JOIN(x2) */ x2.a from t1 x1, t1 x2 where x1.a = t1.a and x1.a = x2.a) from t1")
	result.Check(testkit.Events("1", "2", "3", "4", "5"))
	result = tk.MustQuery("select (select /*+ INL_HASH_JOIN(x2) */ x2.a from t1 x1, t1 x2 where x1.a = t1.a and x1.a = x2.a) from t1")
	result.Check(testkit.Events("1", "2", "3", "4", "5"))
	result = tk.MustQuery("select (select /*+ INL_MERGE_JOIN(x2) */ x2.a from t1 x1, t1 x2 where x1.a = t1.a and x1.a = x2.a) from t1")
	result.Check(testkit.Events("1", "2", "3", "4", "5"))

	// test left outer semi join & anti left outer semi join
	tk.MustQuery("select 1 from (select t1.a in (select t1.a from t1) from t1) x;").Check(testkit.Events("1", "1", "1", "1", "1"))
	tk.MustQuery("select 1 from (select t1.a not in (select t1.a from t1) from t1) x;").Check(testkit.Events("1", "1", "1", "1", "1"))

	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int)")
	tk.MustInterDirc("create causet t2(b int)")
	tk.MustInterDirc("insert into t1 values(1)")
	tk.MustInterDirc("insert into t2 values(1)")
	tk.MustQuery("select * from t1 where a in (select a from t2)").Check(testkit.Events("1"))

	tk.MustInterDirc("set @@milevadb_hash_join_concurrency=5")
}

func (s *testSuiteJoin1) TestInSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int)")
	tk.MustInterDirc("insert t values (1, 1), (2, 1)")
	result := tk.MustQuery("select m1.a from t as m1 where m1.a in (select m2.b from t as m2)")
	result.Check(testkit.Events("1"))
	result = tk.MustQuery("select m1.a from t as m1 where (3, m1.b) not in (select * from t as m2)")
	result.Sort().Check(testkit.Events("1", "2"))
	result = tk.MustQuery("select m1.a from t as m1 where m1.a in (select m2.b+? from t as m2)", 1)
	result.Check(testkit.Events("2"))
	tk.MustInterDirc(`prepare stmt1 from 'select m1.a from t as m1 where m1.a in (select m2.b+? from t as m2)'`)
	tk.MustInterDirc("set @a = 1")
	result = tk.MustQuery(`execute stmt1 using @a;`)
	result.Check(testkit.Events("2"))
	tk.MustInterDirc("set @a = 0")
	result = tk.MustQuery(`execute stmt1 using @a;`)
	result.Check(testkit.Events("1"))

	result = tk.MustQuery("select m1.a from t as m1 where m1.a in (1, 3, 5)")
	result.Check(testkit.Events("1"))

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a float)")
	tk.MustInterDirc("insert t1 values (281.37)")
	tk.MustQuery("select a from t1 where (a in (select a from t1))").Check(testkit.Events("281.37"))

	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1 (a int, b int)")
	tk.MustInterDirc("insert into t1 values (0,0),(1,1),(2,2),(3,3),(4,4)")
	tk.MustInterDirc("create causet t2 (a int)")
	tk.MustInterDirc("insert into t2 values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
	result = tk.MustQuery("select a from t1 where (1,1) in (select * from t2 s , t2 t where t1.a = s.a and s.a = t.a limit 1)")
	result.Check(testkit.Events("1"))

	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1 (a int)")
	tk.MustInterDirc("create causet t2 (a int)")
	tk.MustInterDirc("insert into t1 values (1),(2)")
	tk.MustInterDirc("insert into t2 values (1),(2)")
	tk.MustInterDirc("set @@stochastik.milevadb_opt_insubq_to_join_and_agg = 0")
	result = tk.MustQuery("select * from t1 where a in (select * from t2)")
	result.Sort().Check(testkit.Events("1", "2"))
	result = tk.MustQuery("select * from t1 where a in (select * from t2 where false)")
	result.Check(testkit.Events())
	result = tk.MustQuery("select * from t1 where a not in (select * from t2 where false)")
	result.Sort().Check(testkit.Events("1", "2"))
	tk.MustInterDirc("set @@stochastik.milevadb_opt_insubq_to_join_and_agg = 1")
	result = tk.MustQuery("select * from t1 where a in (select * from t2)")
	result.Sort().Check(testkit.Events("1", "2"))
	result = tk.MustQuery("select * from t1 where a in (select * from t2 where false)")
	result.Check(testkit.Events())
	result = tk.MustQuery("select * from t1 where a not in (select * from t2 where false)")
	result.Sort().Check(testkit.Events("1", "2"))

	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1 (a int, key b (a))")
	tk.MustInterDirc("create causet t2 (a int, key b (a))")
	tk.MustInterDirc("insert into t1 values (1),(2),(2)")
	tk.MustInterDirc("insert into t2 values (1),(2),(2)")
	result = tk.MustQuery("select * from t1 where a in (select * from t2) order by a desc")
	result.Check(testkit.Events("2", "2", "1"))
	result = tk.MustQuery("select * from t1 where a in (select count(*) from t2 where t1.a = t2.a) order by a desc")
	result.Check(testkit.Events("2", "2", "1"))
}

func (s *testSuiteJoin1) TestJoinLeak(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("set @@milevadb_hash_join_concurrency=1")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (d int)")
	tk.MustInterDirc("begin")
	for i := 0; i < 1002; i++ {
		tk.MustInterDirc("insert t values (1)")
	}
	tk.MustInterDirc("commit")
	result, err := tk.InterDirc("select * from t t1 left join (select 1) t2 on 1")
	c.Assert(err, IsNil)
	req := result.NewChunk()
	err = result.Next(context.Background(), req)
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond)
	result.Close()

	tk.MustInterDirc("set @@milevadb_hash_join_concurrency=5")
}

func (s *testSuiteJoin1) TestHashJoinInterDircEncodeDecodeEvent(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1 (id int)")
	tk.MustInterDirc("create causet t2 (id int, name varchar(255), ts timestamp)")
	tk.MustInterDirc("insert into t1 values (1)")
	tk.MustInterDirc("insert into t2 values (1, 'xxx', '2003-06-09 10:51:26')")
	result := tk.MustQuery("select ts from t1 inner join t2 where t2.name = 'xxx'")
	result.Check(testkit.Events("2003-06-09 10:51:26"))
}

func (s *testSuiteJoin1) TestSubqueryInJoinOn(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1 (id int)")
	tk.MustInterDirc("create causet t2 (id int)")
	tk.MustInterDirc("insert into t1 values (1)")
	tk.MustInterDirc("insert into t2 values (1)")

	err := tk.InterDircToErr("SELECT * FROM t1 JOIN t2 on (t2.id < all (SELECT 1))")
	c.Check(err, NotNil)
}

func (s *testSuiteJoin1) TestIssue5255(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int, b date, c float, primary key(a, b))")
	tk.MustInterDirc("create causet t2(a int primary key)")
	tk.MustInterDirc("insert into t1 values(1, '2020-11-29', 2.2)")
	tk.MustInterDirc("insert into t2 values(1)")
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t1 join t2 on t1.a=t2.a").Check(testkit.Events("1 2020-11-29 2.2 1"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t1 join t2 on t1.a=t2.a").Check(testkit.Events("1 2020-11-29 2.2 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t1 join t2 on t1.a=t2.a").Check(testkit.Events("1 2020-11-29 2.2 1"))
}

func (s *testSuiteJoin1) TestIssue5278(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, tt")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("create causet tt(a varchar(10), b int)")
	tk.MustInterDirc("insert into t values(1, 1)")
	tk.MustQuery("select * from t left join tt on t.a=tt.a left join t ttt on t.a=ttt.a").Check(testkit.Events("1 1 <nil> <nil> 1 1"))
}

func (s *testSuiteJoin1) TestIssue15850JoinNullValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustQuery("SELECT * FROM (select null) v NATURAL LEFT JOIN (select null) v1;").Check(testkit.Events("<nil>"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0))

	tk.MustInterDirc("drop causet if exists t0;")
	tk.MustInterDirc("drop view if exists v0;")
	tk.MustInterDirc("CREATE TABLE t0(c0 TEXT);")
	tk.MustInterDirc("CREATE VIEW v0(c0) AS SELECT NULL;")
	tk.MustQuery("SELECT /*+ HASH_JOIN(v0) */ * FROM v0 NATURAL LEFT JOIN t0;").Check(testkit.Events("<nil>"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustQuery("SELECT /*+ MERGE_JOIN(v0) */ * FROM v0 NATURAL LEFT JOIN t0;").Check(testkit.Events("<nil>"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0))
}

func (s *testSuiteJoin1) TestIndexLookupJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("set @@milevadb_init_chunk_size=2")
	tk.MustInterDirc("DROP TABLE IF EXISTS t")
	tk.MustInterDirc("CREATE TABLE `t` (`a` int, pk integer auto_increment,`b` char (20),primary key (pk))")
	tk.MustInterDirc("CREATE INDEX idx_t_a ON t(`a`)")
	tk.MustInterDirc("CREATE INDEX idx_t_b ON t(`b`)")
	tk.MustInterDirc("INSERT INTO t VALUES (148307968, DEFAULT, 'nndsjofmFIDelxvhqv') ,  (-1327693824, DEFAULT, 'pnndsjofmFIDelxvhqvfny') ,  (-277544960, DEFAULT, 'fpnndsjo')")

	tk.MustInterDirc("DROP TABLE IF EXISTS s")
	tk.MustInterDirc("CREATE TABLE `s` (`a` int, `b` char (20))")
	tk.MustInterDirc("CREATE INDEX idx_s_a ON s(`a`)")
	tk.MustInterDirc("INSERT INTO s VALUES (-277544960, 'fpnndsjo') ,  (2, 'kfpnndsjof') ,  (2, 'vtdiockfpn'), (-277544960, 'fpnndsjo') ,  (2, 'kfpnndsjof') ,  (6, 'ckfp')")
	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t join s on t.a = s.a").Sort().Check(testkit.Events("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t join s on t.a = s.a").Sort().Check(testkit.Events("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t join s on t.a = s.a").Sort().Check(testkit.Events("-277544960", "-277544960"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t left join s on t.a = s.a").Sort().Check(testkit.Events("-1327693824", "-277544960", "-277544960", "148307968"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t left join s on t.a = s.a").Sort().Check(testkit.Events("-1327693824", "-277544960", "-277544960", "148307968"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t left join s on t.a = s.a").Sort().Check(testkit.Events("-1327693824", "-277544960", "-277544960", "148307968"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t left join s on t.a = s.a where t.a = -277544960").Sort().Check(testkit.Events("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t left join s on t.a = s.a where t.a = -277544960").Sort().Check(testkit.Events("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t left join s on t.a = s.a where t.a = -277544960").Sort().Check(testkit.Events("-277544960", "-277544960"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t right join s on t.a = s.a").Sort().Check(testkit.Events("-277544960", "-277544960", "<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t right join s on t.a = s.a").Sort().Check(testkit.Events("-277544960", "-277544960", "<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t right join s on t.a = s.a").Sort().Check(testkit.Events("-277544960", "-277544960", "<nil>", "<nil>", "<nil>", "<nil>"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t left join s on t.a = s.a order by t.a desc").Check(testkit.Events("148307968", "-277544960", "-277544960", "-1327693824"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t left join s on t.a = s.a order by t.a desc").Check(testkit.Events("148307968", "-277544960", "-277544960", "-1327693824"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t left join s on t.a = s.a order by t.a desc").Check(testkit.Events("148307968", "-277544960", "-277544960", "-1327693824"))

	tk.MustInterDirc("DROP TABLE IF EXISTS t;")
	tk.MustInterDirc("CREATE TABLE t(a BIGINT PRIMARY KEY, b BIGINT);")
	tk.MustInterDirc("INSERT INTO t VALUES(1, 2);")
	tk.MustQuery("SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a UNION ALL SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a;").Check(testkit.Events("1 2 1 2", "1 2 1 2"))
	tk.MustQuery("SELECT /*+ INL_HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a UNION ALL SELECT /*+ INL_HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a;").Check(testkit.Events("1 2 1 2", "1 2 1 2"))
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a UNION ALL SELECT /*+ INL_MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a;").Check(testkit.Events("1 2 1 2", "1 2 1 2"))

	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a decimal(6,2), index idx(a));`)
	tk.MustInterDirc(`insert into t values(1.01), (2.02), (NULL);`)
	tk.MustQuery(`select /*+ INL_JOIN(t2) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Events(
		`1.01`,
		`2.02`,
	))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Events(
		`1.01`,
		`2.02`,
	))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Events(
		`1.01`,
		`2.02`,
	))

	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a bigint, b bigint, unique key idx1(a, b));`)
	tk.MustInterDirc(`insert into t values(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6);`)
	tk.MustInterDirc(`set @@milevadb_init_chunk_size = 2;`)
	tk.MustQuery(`select /*+ INL_JOIN(t2) */ * from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b + 4;`).Check(testkit.Events(
		`1 1 <nil> <nil>`,
		`1 2 <nil> <nil>`,
		`1 3 <nil> <nil>`,
		`1 4 <nil> <nil>`,
		`1 5 1 1`,
		`1 6 1 2`,
	))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ * from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b + 4;`).Check(testkit.Events(
		`1 1 <nil> <nil>`,
		`1 2 <nil> <nil>`,
		`1 3 <nil> <nil>`,
		`1 4 <nil> <nil>`,
		`1 5 1 1`,
		`1 6 1 2`,
	))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ * from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b + 4;`).Check(testkit.Events(
		`1 1 <nil> <nil>`,
		`1 2 <nil> <nil>`,
		`1 3 <nil> <nil>`,
		`1 4 <nil> <nil>`,
		`1 5 1 1`,
		`1 6 1 2`,
	))

	tk.MustInterDirc(`drop causet if exists t1, t2, t3;`)
	tk.MustInterDirc("create causet t1(a int primary key, b int)")
	tk.MustInterDirc("insert into t1 values(1, 0), (2, null)")
	tk.MustInterDirc("create causet t2(a int primary key)")
	tk.MustInterDirc("insert into t2 values(0)")
	tk.MustQuery("select /*+ INL_JOIN(t2)*/ * from t1 left join t2 on t1.b = t2.a;").Sort().Check(testkit.Events(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t2)*/ * from t1 left join t2 on t1.b = t2.a;").Sort().Check(testkit.Events(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t2)*/ * from t1 left join t2 on t1.b = t2.a;").Sort().Check(testkit.Events(
		`1 0 0`,
		`2 <nil> <nil>`,
	))

	tk.MustInterDirc("create causet t3(a int, key(a))")
	tk.MustInterDirc("insert into t3 values(0)")
	tk.MustQuery("select /*+ INL_JOIN(t3)*/ * from t1 left join t3 on t1.b = t3.a;").Check(testkit.Events(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t3)*/ * from t1 left join t3 on t1.b = t3.a;").Check(testkit.Events(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t3)*/ * from t1 left join t3 on t1.b = t3.a;").Check(testkit.Events(
		`2 <nil> <nil>`,
		`1 0 0`,
	))

	tk.MustInterDirc("drop causet if exists t,s")
	tk.MustInterDirc("create causet t(a int primary key auto_increment, b time)")
	tk.MustInterDirc("create causet s(a int, b time)")
	tk.MustInterDirc("alter causet s add index idx(a,b)")
	tk.MustInterDirc("set @@milevadb_index_join_batch_size=4;set @@milevadb_init_chunk_size=1;set @@milevadb_max_chunk_size=32; set @@milevadb_index_lookup_join_concurrency=15;")
	tk.MustInterDirc("set @@stochastik.milevadb_interlock_concurrency = 4;")
	tk.MustInterDirc("set @@stochastik.milevadb_hash_join_concurrency = 5;")

	// insert 64 rows into `t`
	tk.MustInterDirc("insert into t values(0, '01:01:01')")
	for i := 0; i < 6; i++ {
		tk.MustInterDirc("insert into t select 0, b + 1 from t")
	}
	tk.MustInterDirc("insert into s select a, b - 1 from t")
	tk.MustInterDirc("analyze causet t;")
	tk.MustInterDirc("analyze causet s;")

	tk.MustQuery("desc select /*+ MilevaDB_INLJ(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events(
		"HashAgg_9 1.00 root  funcs:count(1)->DeferredCauset#6",
		"└─IndexJoin_16 64.00 root  inner join, inner:IndexReader_15, outer key:test.t.a, inner key:test.s.a, other cond:lt(test.s.b, test.t.b)",
		"  ├─BlockReader_26(Build) 64.00 root  data:Selection_25",
		"  │ └─Selection_25 64.00 cop[einsteindb]  not(isnull(test.t.b))",
		"  │   └─BlockFullScan_24 64.00 cop[einsteindb] causet:t keep order:false",
		"  └─IndexReader_15(Probe) 1.00 root  index:Selection_14",
		"    └─Selection_14 1.00 cop[einsteindb]  not(isnull(test.s.a)), not(isnull(test.s.b))",
		"      └─IndexRangeScan_13 1.00 cop[einsteindb] causet:s, index:idx(a, b) range: decided by [eq(test.s.a, test.t.a) lt(test.s.b, test.t.b)], keep order:false"))
	tk.MustQuery("select /*+ MilevaDB_INLJ(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events("64"))
	tk.MustInterDirc("set @@milevadb_index_lookup_join_concurrency=1;")
	tk.MustQuery("select /*+ MilevaDB_INLJ(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events("64"))

	tk.MustQuery("desc select /*+ INL_MERGE_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events(
		"HashAgg_9 1.00 root  funcs:count(1)->DeferredCauset#6",
		"└─IndexMergeJoin_23 64.00 root  inner join, inner:IndexReader_21, outer key:test.t.a, inner key:test.s.a, other cond:lt(test.s.b, test.t.b)",
		"  ├─BlockReader_26(Build) 64.00 root  data:Selection_25",
		"  │ └─Selection_25 64.00 cop[einsteindb]  not(isnull(test.t.b))",
		"  │   └─BlockFullScan_24 64.00 cop[einsteindb] causet:t keep order:false",
		"  └─IndexReader_21(Probe) 1.00 root  index:Selection_20",
		"    └─Selection_20 1.00 cop[einsteindb]  not(isnull(test.s.a)), not(isnull(test.s.b))",
		"      └─IndexRangeScan_19 1.00 cop[einsteindb] causet:s, index:idx(a, b) range: decided by [eq(test.s.a, test.t.a) lt(test.s.b, test.t.b)], keep order:true",
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events("64"))
	tk.MustInterDirc("set @@milevadb_index_lookup_join_concurrency=1;")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events("64"))

	tk.MustQuery("desc select /*+ INL_HASH_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events(
		"HashAgg_9 1.00 root  funcs:count(1)->DeferredCauset#6",
		"└─IndexHashJoin_18 64.00 root  inner join, inner:IndexReader_15, outer key:test.t.a, inner key:test.s.a, other cond:lt(test.s.b, test.t.b)",
		"  ├─BlockReader_26(Build) 64.00 root  data:Selection_25",
		"  │ └─Selection_25 64.00 cop[einsteindb]  not(isnull(test.t.b))",
		"  │   └─BlockFullScan_24 64.00 cop[einsteindb] causet:t keep order:false",
		"  └─IndexReader_15(Probe) 1.00 root  index:Selection_14",
		"    └─Selection_14 1.00 cop[einsteindb]  not(isnull(test.s.a)), not(isnull(test.s.b))",
		"      └─IndexRangeScan_13 1.00 cop[einsteindb] causet:s, index:idx(a, b) range: decided by [eq(test.s.a, test.t.a) lt(test.s.b, test.t.b)], keep order:false",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events("64"))
	tk.MustInterDirc("set @@milevadb_index_lookup_join_concurrency=1;")
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Events("64"))

	// issue15658
	tk.MustInterDirc("drop causet t1, t2")
	tk.MustInterDirc("create causet t1(id int primary key)")
	tk.MustInterDirc("create causet t2(a int, b int)")
	tk.MustInterDirc("insert into t1 values(1)")
	tk.MustInterDirc("insert into t2 values(1,1),(2,1)")
	tk.MustQuery("select /*+ inl_join(t1)*/ * from t1 join t2 on t2.b=t1.id and t2.a=t1.id;").Check(testkit.Events("1 1 1"))
	tk.MustQuery("select /*+ inl_hash_join(t1)*/ * from t1 join t2 on t2.b=t1.id and t2.a=t1.id;").Check(testkit.Events("1 1 1"))
	tk.MustQuery("select /*+ inl_merge_join(t1)*/ * from t1 join t2 on t2.b=t1.id and t2.a=t1.id;").Check(testkit.Events("1 1 1"))
}

func (s *testSuiteJoinSerial) TestIndexNestedLoopHashJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("set @@milevadb_init_chunk_size=2")
	tk.MustInterDirc("set @@milevadb_index_join_batch_size=10")
	tk.MustInterDirc("DROP TABLE IF EXISTS t, s")
	tk.MustInterDirc("set @@milevadb_enable_clustered_index=0;")
	tk.MustInterDirc("create causet t(pk int primary key, a int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into t values(%d, %d)", i, i))
	}
	tk.MustInterDirc("create causet s(a int primary key)")
	for i := 0; i < 100; i++ {
		if rand.Float32() < 0.3 {
			tk.MustInterDirc(fmt.Sprintf("insert into s values(%d)", i))
		} else {
			tk.MustInterDirc(fmt.Sprintf("insert into s values(%d)", i*100))
		}
	}
	tk.MustInterDirc("analyze causet t")
	tk.MustInterDirc("analyze causet s")
	// Test IndexNestedLoopHashJoin keepOrder.
	tk.MustQuery("explain select /*+ INL_HASH_JOIN(s) */ * from t left join s on t.a=s.a order by t.pk").Check(testkit.Events(
		"IndexHashJoin_27 100.00 root  left outer join, inner:BlockReader_22, outer key:test.t.a, inner key:test.s.a",
		"├─BlockReader_30(Build) 100.00 root  data:BlockFullScan_29",
		"│ └─BlockFullScan_29 100.00 cop[einsteindb] causet:t keep order:true",
		"└─BlockReader_22(Probe) 1.00 root  data:BlockRangeScan_21",
		"  └─BlockRangeScan_21 1.00 cop[einsteindb] causet:s range: decided by [test.t.a], keep order:false",
	))
	rs := tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ * from t left join s on t.a=s.a order by t.pk")
	for i, event := range rs.Events() {
		c.Assert(event[0].(string), Equals, fmt.Sprintf("%d", i))
	}

	// index hash join with semi join
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causet/embedded/MockOnlyEnableIndexHashJoin", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causet/embedded/MockOnlyEnableIndexHashJoin"), IsNil)
	}()
	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("CREATE TABLE `t` (	`l_orderkey` int(11) NOT NULL,`l_linenumber` int(11) NOT NULL,`l_partkey` int(11) DEFAULT NULL,`l_suppkey` int(11) DEFAULT NULL,PRIMARY KEY (`l_orderkey`,`l_linenumber`))")
	tk.MustInterDirc(`insert into t values(0,0,0,0);`)
	tk.MustInterDirc(`insert into t values(0,1,0,1);`)
	tk.MustInterDirc(`insert into t values(0,2,0,0);`)
	tk.MustInterDirc(`insert into t values(1,0,1,0);`)
	tk.MustInterDirc(`insert into t values(1,1,1,1);`)
	tk.MustInterDirc(`insert into t values(1,2,1,0);`)
	tk.MustInterDirc(`insert into t values(2,0,0,0);`)
	tk.MustInterDirc(`insert into t values(2,1,0,1);`)
	tk.MustInterDirc(`insert into t values(2,2,0,0);`)

	tk.MustInterDirc("analyze causet t")

	// test semi join
	tk.Se.GetStochastikVars().InitChunkSize = 2
	tk.Se.GetStochastikVars().MaxChunkSize = 2
	tk.MustInterDirc("set @@milevadb_index_join_batch_size=2")
	tk.MustQuery("desc select * from t l1 where exists ( select * from t l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) order by `l_orderkey`,`l_linenumber`;").Check(testkit.Events(
		"Sort_9 7.20 root  test.t.l_orderkey, test.t.l_linenumber",
		"└─IndexHashJoin_17 7.20 root  semi join, inner:IndexLookUp_15, outer key:test.t.l_orderkey, inner key:test.t.l_orderkey, other cond:ne(test.t.l_suppkey, test.t.l_suppkey)",
		"  ├─BlockReader_20(Build) 9.00 root  data:Selection_19",
		"  │ └─Selection_19 9.00 cop[einsteindb]  not(isnull(test.t.l_suppkey))",
		"  │   └─BlockFullScan_18 9.00 cop[einsteindb] causet:l1 keep order:false",
		"  └─IndexLookUp_15(Probe) 3.00 root  ",
		"    ├─IndexRangeScan_12(Build) 3.00 cop[einsteindb] causet:l2, index:PRIMARY(l_orderkey, l_linenumber) range: decided by [eq(test.t.l_orderkey, test.t.l_orderkey)], keep order:false",
		"    └─Selection_14(Probe) 3.00 cop[einsteindb]  not(isnull(test.t.l_suppkey))",
		"      └─BlockEventIDScan_13 3.00 cop[einsteindb] causet:l2 keep order:false"))
	tk.MustQuery("select * from t l1 where exists ( select * from t l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey )order by `l_orderkey`,`l_linenumber`;").Check(testkit.Events("0 0 0 0", "0 1 0 1", "0 2 0 0", "1 0 1 0", "1 1 1 1", "1 2 1 0", "2 0 0 0", "2 1 0 1", "2 2 0 0"))
	tk.MustQuery("desc select count(*) from t l1 where exists ( select * from t l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey );").Check(testkit.Events(
		"StreamAgg_14 1.00 root  funcs:count(1)->DeferredCauset#11",
		"└─IndexHashJoin_29 7.20 root  semi join, inner:IndexLookUp_27, outer key:test.t.l_orderkey, inner key:test.t.l_orderkey, other cond:ne(test.t.l_suppkey, test.t.l_suppkey)",
		"  ├─BlockReader_23(Build) 9.00 root  data:Selection_22",
		"  │ └─Selection_22 9.00 cop[einsteindb]  not(isnull(test.t.l_suppkey))",
		"  │   └─BlockFullScan_21 9.00 cop[einsteindb] causet:l1 keep order:false",
		"  └─IndexLookUp_27(Probe) 3.00 root  ",
		"    ├─IndexRangeScan_24(Build) 3.00 cop[einsteindb] causet:l2, index:PRIMARY(l_orderkey, l_linenumber) range: decided by [eq(test.t.l_orderkey, test.t.l_orderkey)], keep order:false",
		"    └─Selection_26(Probe) 3.00 cop[einsteindb]  not(isnull(test.t.l_suppkey))",
		"      └─BlockEventIDScan_25 3.00 cop[einsteindb] causet:l2 keep order:false"))
	tk.MustQuery("select count(*) from t l1 where exists ( select * from t l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey );").Check(testkit.Events("9"))
	tk.MustInterDirc("DROP TABLE IF EXISTS t, s")

	// issue16586
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists lineitem;")
	tk.MustInterDirc("drop causet if exists orders;")
	tk.MustInterDirc("drop causet if exists supplier;")
	tk.MustInterDirc("drop causet if exists nation;")
	tk.MustInterDirc("CREATE TABLE `lineitem` (`l_orderkey` int(11) NOT NULL,`l_linenumber` int(11) NOT NULL,`l_partkey` int(11) DEFAULT NULL,`l_suppkey` int(11) DEFAULT NULL,PRIMARY KEY (`l_orderkey`,`l_linenumber`)	);")
	tk.MustInterDirc("CREATE TABLE `supplier` (	`S_SUPPKEY` bigint(20) NOT NULL,`S_NATIONKEY` bigint(20) NOT NULL,PRIMARY KEY (`S_SUPPKEY`));")
	tk.MustInterDirc("CREATE TABLE `orders` (`O_ORDERKEY` bigint(20) NOT NULL,`O_ORDERSTATUS` char(1) NOT NULL,PRIMARY KEY (`O_ORDERKEY`));")
	tk.MustInterDirc("CREATE TABLE `nation` (`N_NATIONKEY` bigint(20) NOT NULL,`N_NAME` char(25) NOT NULL,PRIMARY KEY (`N_NATIONKEY`))")
	tk.MustInterDirc("insert into lineitem values(0,0,0,1)")
	tk.MustInterDirc("insert into lineitem values(0,1,1,1)")
	tk.MustInterDirc("insert into lineitem values(0,2,2,0)")
	tk.MustInterDirc("insert into lineitem values(0,3,3,3)")
	tk.MustInterDirc("insert into lineitem values(0,4,1,4)")
	tk.MustInterDirc("insert into supplier values(0, 4)")
	tk.MustInterDirc("insert into orders values(0, 'F')")
	tk.MustInterDirc("insert into nation values(0, 'EGYPT')")
	tk.MustInterDirc("insert into lineitem values(1,0,2,4)")
	tk.MustInterDirc("insert into lineitem values(1,1,1,0)")
	tk.MustInterDirc("insert into lineitem values(1,2,3,3)")
	tk.MustInterDirc("insert into lineitem values(1,3,1,0)")
	tk.MustInterDirc("insert into lineitem values(1,4,1,3)")
	tk.MustInterDirc("insert into supplier values(1, 1)")
	tk.MustInterDirc("insert into orders values(1, 'F')")
	tk.MustInterDirc("insert into nation values(1, 'EGYPT')")
	tk.MustInterDirc("insert into lineitem values(2,0,1,2)")
	tk.MustInterDirc("insert into lineitem values(2,1,3,4)")
	tk.MustInterDirc("insert into lineitem values(2,2,2,0)")
	tk.MustInterDirc("insert into lineitem values(2,3,3,1)")
	tk.MustInterDirc("insert into lineitem values(2,4,4,3)")
	tk.MustInterDirc("insert into supplier values(2, 3)")
	tk.MustInterDirc("insert into orders values(2, 'F')")
	tk.MustInterDirc("insert into nation values(2, 'EGYPT')")
	tk.MustInterDirc("insert into lineitem values(3,0,4,3)")
	tk.MustInterDirc("insert into lineitem values(3,1,4,3)")
	tk.MustInterDirc("insert into lineitem values(3,2,2,2)")
	tk.MustInterDirc("insert into lineitem values(3,3,0,0)")
	tk.MustInterDirc("insert into lineitem values(3,4,1,0)")
	tk.MustInterDirc("insert into supplier values(3, 1)")
	tk.MustInterDirc("insert into orders values(3, 'F')")
	tk.MustInterDirc("insert into nation values(3, 'EGYPT')")
	tk.MustInterDirc("insert into lineitem values(4,0,2,2)")
	tk.MustInterDirc("insert into lineitem values(4,1,4,2)")
	tk.MustInterDirc("insert into lineitem values(4,2,0,2)")
	tk.MustInterDirc("insert into lineitem values(4,3,0,1)")
	tk.MustInterDirc("insert into lineitem values(4,4,2,2)")
	tk.MustInterDirc("insert into supplier values(4, 4)")
	tk.MustInterDirc("insert into orders values(4, 'F')")
	tk.MustInterDirc("insert into nation values(4, 'EGYPT')")
	tk.MustQuery("select count(*) from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and  exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey < l1.l_suppkey ) and s_nationkey = n_nationkey and n_name = 'EGYPT' order by l1.l_orderkey, l1.l_linenumber;").Check(testkit.Events("18"))
	tk.MustInterDirc("drop causet lineitem")
	tk.MustInterDirc("drop causet nation")
	tk.MustInterDirc("drop causet supplier")
	tk.MustInterDirc("drop causet orders")
}

func (s *testSuiteJoin3) TestIssue15686(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, k;")
	tk.MustInterDirc("create causet k (a int, pk int primary key, index(a));")
	tk.MustInterDirc("create causet t (a int, pk int primary key, index(a));")
	tk.MustInterDirc("insert into k values(0,8),(0,23),(1,21),(1,33),(1,52),(2,17),(2,34),(2,39),(2,40),(2,66),(2,67),(3,9),(3,25),(3,41),(3,48),(4,4),(4,11),(4,15),(4,26),(4,27),(4,31),(4,35),(4,45),(4,47),(4,49);")
	tk.MustInterDirc("insert into t values(3,4),(3,5),(3,27),(3,29),(3,57),(3,58),(3,79),(3,84),(3,92),(3,95);")
	tk.MustQuery("select /*+ inl_join(t) */ count(*) from k left join t on k.a = t.a and k.pk > t.pk;").Check(testkit.Events("33"))
	tk.MustQuery("select /*+ inl_hash_join(t) */ count(*) from k left join t on k.a = t.a and k.pk > t.pk;").Check(testkit.Events("33"))
	tk.MustQuery("select /*+ inl_merge_join(t) */ count(*) from k left join t on k.a = t.a and k.pk > t.pk;").Check(testkit.Events("33"))
}

func (s *testSuiteJoin3) TestIssue13449(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, s;")
	tk.MustInterDirc("create causet t(a int, index(a));")
	tk.MustInterDirc("create causet s(a int, index(a));")
	for i := 1; i <= 128; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into t values(%d)", i))
	}
	tk.MustInterDirc("insert into s values(1), (128)")
	tk.MustInterDirc("set @@milevadb_max_chunk_size=32;")
	tk.MustInterDirc("set @@milevadb_index_lookup_join_concurrency=1;")
	tk.MustInterDirc("set @@milevadb_index_join_batch_size=32;")

	tk.MustQuery("desc select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a order by t.a;").Check(testkit.Events(
		"IndexHashJoin_30 12487.50 root  inner join, inner:IndexReader_27, outer key:test.t.a, inner key:test.s.a",
		"├─IndexReader_37(Build) 9990.00 root  index:IndexFullScan_36",
		"│ └─IndexFullScan_36 9990.00 cop[einsteindb] causet:t, index:a(a) keep order:true, stats:pseudo",
		"└─IndexReader_27(Probe) 1.25 root  index:Selection_26",
		"  └─Selection_26 1.25 cop[einsteindb]  not(isnull(test.s.a))",
		"    └─IndexRangeScan_25 1.25 cop[einsteindb] causet:s, index:a(a) range: decided by [eq(test.s.a, test.t.a)], keep order:false, stats:pseudo"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a order by t.a;").Check(testkit.Events("1 1", "128 128"))
}

func (s *testSuiteJoin3) TestMergejoinOrder(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2;")
	tk.MustInterDirc("create causet t1(a bigint primary key, b bigint);")
	tk.MustInterDirc("create causet t2(a bigint primary key, b bigint);")
	tk.MustInterDirc("insert into t1 values(1, 100), (2, 100), (3, 100), (4, 100), (5, 100);")
	tk.MustInterDirc("insert into t2 select a*100, b*100 from t1;")

	tk.MustQuery("explain select /*+ MilevaDB_SMJ(t2) */ * from t1 left outer join t2 on t1.a=t2.a and t1.a!=3 order by t1.a;").Check(testkit.Events(
		"MergeJoin_20 10000.00 root  left outer join, left key:test.t1.a, right key:test.t2.a, left cond:[ne(test.t1.a, 3)]",
		"├─BlockReader_14(Build) 6666.67 root  data:BlockRangeScan_13",
		"│ └─BlockRangeScan_13 6666.67 cop[einsteindb] causet:t2 range:[-inf,3), (3,+inf], keep order:true, stats:pseudo",
		"└─BlockReader_12(Probe) 10000.00 root  data:BlockFullScan_11",
		"  └─BlockFullScan_11 10000.00 cop[einsteindb] causet:t1 keep order:true, stats:pseudo",
	))

	tk.MustInterDirc("set @@milevadb_init_chunk_size=1")
	tk.MustQuery("select /*+ MilevaDB_SMJ(t2) */ * from t1 left outer join t2 on t1.a=t2.a and t1.a!=3 order by t1.a;").Check(testkit.Events(
		"1 100 <nil> <nil>",
		"2 100 <nil> <nil>",
		"3 100 <nil> <nil>",
		"4 100 <nil> <nil>",
		"5 100 <nil> <nil>",
	))

	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a bigint, b bigint, index idx_1(a,b));`)
	tk.MustInterDirc(`insert into t values(1, 1), (1, 2), (2, 1), (2, 2);`)
	tk.MustQuery(`select /*+ MilevaDB_SMJ(t1, t2) */ * from t t1 join t t2 on t1.b = t2.b and t1.a=t2.a;`).Check(testkit.Events(
		`1 1 1 1`,
		`1 2 1 2`,
		`2 1 2 1`,
		`2 2 2 2`,
	))

	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a decimal(6,2), index idx(a));`)
	tk.MustInterDirc(`insert into t values(1.01), (2.02), (NULL);`)
	tk.MustQuery(`select /*+ MilevaDB_SMJ(t1) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Events(
		`1.01`,
		`2.02`,
	))
}

func (s *testSuiteJoin1) TestEmbeddedOuterJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int, b int)")
	tk.MustInterDirc("create causet t2(a int, b int)")
	tk.MustInterDirc("insert into t1 values(1, 1)")
	tk.MustQuery("select * from (t1 left join t2 on t1.a = t2.a) left join (t2 t3 left join t2 t4 on t3.a = t4.a) on t2.b = 1").
		Check(testkit.Events("1 1 <nil> <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testSuiteJoin1) TestHashJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int, b int);")
	tk.MustInterDirc("create causet t2(a int, b int);")
	tk.MustInterDirc("insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);")
	tk.MustQuery("select count(*) from t1").Check(testkit.Events("5"))
	tk.MustQuery("select count(*) from t2").Check(testkit.Events("0"))
	tk.MustInterDirc("set @@milevadb_init_chunk_size=1;")
	result := tk.MustQuery("explain analyze select /*+ MilevaDB_HJ(t1, t2) */ * from t1 where exists (select a from t2 where t1.a = t2.a);")
	//   0                       1        2 3         4        5                                                                    6                                           7         8
	// 0 HashJoin_9              7992.00  0 root               time:959.436µs, loops:1, Concurrency:5, probe defCauslision:0, build:0s  semi join, equal:[eq(test.t1.a, test.t2.a)] 0 Bytes   0 Bytes
	// 1 ├─BlockReader_15(Build) 9990.00  0 root               time:583.499µs, loops:1, rpc num: 1, rpc time:563.325µs, proc keys:0 data:Selection_14                           141 Bytes N/A
	// 2 │ └─Selection_14        9990.00  0 cop[einsteindb]          time:53.674µs, loops:1                                               not(isnull(test.t2.a))                      N/A       N/A
	// 3 │   └─BlockFullScan_13  10000.00 0 cop[einsteindb] causet:t2 time:52.14µs, loops:1                                                keep order:false, stats:pseudo              N/A       N/A
	// 4 └─BlockReader_12(Probe) 9990.00  5 root               time:779.503µs, loops:1, rpc num: 1, rpc time:794.929µs, proc keys:0 data:Selection_11                           241 Bytes N/A
	// 5   └─Selection_11        9990.00  5 cop[einsteindb]          time:243.395µs, loops:6                                              not(isnull(test.t1.a))                      N/A       N/A
	// 6     └─BlockFullScan_10  10000.00 5 cop[einsteindb] causet:t1 time:206.273µs, loops:6                                              keep order:false, stats:pseudo              N/A       N/A
	event := result.Events()
	c.Assert(len(event), Equals, 7)
	innerActEvents := event[1][2].(string)
	c.Assert(innerActEvents, Equals, "0")
	outerActEvents := event[4][2].(string)
	// FIXME: revert this result to 1 after BlockReaderInterlockingDirectorate can handle initChunkSize.
	c.Assert(outerActEvents, Equals, "5")
}

func (s *testSuiteJoin1) TestJoinDifferentDecimals(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("Use test")
	tk.MustInterDirc("Drop causet if exists t1")
	tk.MustInterDirc("Create causet t1 (v int)")
	tk.MustInterDirc("Insert into t1 value (1)")
	tk.MustInterDirc("Insert into t1 value (2)")
	tk.MustInterDirc("Insert into t1 value (3)")
	tk.MustInterDirc("Drop causet if exists t2")
	tk.MustInterDirc("Create causet t2 (v decimal(12, 3))")
	tk.MustInterDirc("Insert into t2 value (1)")
	tk.MustInterDirc("Insert into t2 value (2.0)")
	tk.MustInterDirc("Insert into t2 value (000003.000000)")
	rst := tk.MustQuery("Select * from t1, t2 where t1.v = t2.v order by t1.v")
	event := rst.Events()
	c.Assert(len(event), Equals, 3)
	rst.Check(testkit.Events("1 1.000", "2 2.000", "3 3.000"))
}

func (s *testSuiteJoin2) TestNullEmptyAwareSemiJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int, index idx_a(a), index idb_b(b), index idx_c(c))")
	tk.MustInterDirc("insert into t values(null, 1, 0), (1, 2, 0)")
	tests := []struct {
		allegrosql string
	}{
		{
			"a, b from t t1 where a not in (select b from t t2)",
		},
		{
			"a, b from t t1 where a not in (select b from t t2 where t1.b = t2.a)",
		},
		{
			"a, b from t t1 where a not in (select a from t t2)",
		},
		{
			"a, b from t t1 where a not in (select a from t t2 where t1.b = t2.b)",
		},
		{
			"a, b from t t1 where a != all (select b from t t2)",
		},
		{
			"a, b from t t1 where a != all (select b from t t2 where t1.b = t2.a)",
		},
		{
			"a, b from t t1 where a != all (select a from t t2)",
		},
		{
			"a, b from t t1 where a != all (select a from t t2 where t1.b = t2.b)",
		},
		{
			"a, b from t t1 where not exists (select * from t t2 where t1.a = t2.b)",
		},
		{
			"a, b from t t1 where not exists (select * from t t2 where t1.a = t2.a)",
		},
	}
	results := []struct {
		result [][]interface{}
	}{
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 2"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 2"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("<nil> 1"),
		},
		{
			testkit.Events("<nil> 1"),
		},
	}
	hints := [5]string{
		"/*+ HASH_JOIN(t1, t2) */",
		"/*+ MERGE_JOIN(t1, t2) */",
		"/*+ INL_JOIN(t1, t2) */",
		"/*+ INL_HASH_JOIN(t1, t2) */",
		"/*+ INL_MERGE_JOIN(t1, t2) */",
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert into t values(1, null, 0), (2, 1, 0)")
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events("2 1"),
		},
		{
			testkit.Events(),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert into t values(1, null, 0), (2, 1, 0), (null, 2, 0)")
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events("1 <nil>"),
		},
		{
			testkit.Events("<nil> 2"),
		},
		{
			testkit.Events("<nil> 2"),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert into t values(1, null, 0), (2, null, 0)")
	tests = []struct {
		allegrosql string
	}{
		{
			"a, b from t t1 where b not in (select a from t t2)",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Events(),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert into t values(null, 1, 1), (2, 2, 2), (3, null, 3), (4, 4, 3)")
	tests = []struct {
		allegrosql string
	}{
		{
			"a, b, a not in (select b from t t2) from t t1 order by a",
		},
		{
			"a, c, a not in (select c from t t2) from t t1 order by a",
		},
		{
			"a, b, a in (select b from t t2) from t t1 order by a",
		},
		{
			"a, c, a in (select c from t t2) from t t1 order by a",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Events(
				"<nil> 1 <nil>",
				"2 2 0",
				"3 <nil> <nil>",
				"4 4 0",
			),
		},
		{
			testkit.Events(
				"<nil> 1 <nil>",
				"2 2 0",
				"3 3 0",
				"4 3 1",
			),
		},
		{
			testkit.Events(
				"<nil> 1 <nil>",
				"2 2 1",
				"3 <nil> <nil>",
				"4 4 1",
			),
		},
		{
			testkit.Events(
				"<nil> 1 <nil>",
				"2 2 1",
				"3 3 1",
				"4 3 0",
			),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet s(a int, b int)")
	tk.MustInterDirc("insert into s values(1, 2)")
	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert into t values(null, null, 0)")
	tests = []struct {
		allegrosql string
	}{
		{
			"a in (select b from t t2 where t2.a = t1.b) from s t1",
		},
		{
			"a in (select b from s t2 where t2.a = t1.b) from t t1",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Events("0"),
		},
		{
			testkit.Events("0"),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("truncate causet s")
	tk.MustInterDirc("insert into s values(2, 2)")
	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert into t values(null, 1, 0)")
	tests = []struct {
		allegrosql string
	}{
		{
			"a in (select a from s t2 where t2.b = t1.b) from t t1",
		},
		{
			"a in (select a from s t2 where t2.b < t1.b) from t t1",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Events("0"),
		},
		{
			testkit.Events("0"),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("truncate causet s")
	tk.MustInterDirc("insert into s values(null, 2)")
	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert into t values(1, 1, 0)")
	tests = []struct {
		allegrosql string
	}{
		{
			"a in (select a from s t2 where t2.b = t1.b) from t t1",
		},
		{
			"b in (select a from s t2) from t t1",
		},
		{
			"* from t t1 where a not in (select a from s t2 where t2.b = t1.b)",
		},
		{
			"* from t t1 where a not in (select a from s t2)",
		},
		{
			"* from s t1 where a not in (select a from t t2)",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Events("0"),
		},
		{
			testkit.Events("<nil>"),
		},
		{
			testkit.Events("1 1 0"),
		},
		{
			testkit.Events(),
		},
		{
			testkit.Events(),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			allegrosql := fmt.Sprintf("select %s %s", hint, tt.allegrosql)
			result := tk.MustQuery(allegrosql)
			result.Check(results[i].result)
		}
	}

	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int)")
	tk.MustInterDirc("create causet t2(a int)")
	tk.MustInterDirc("insert into t1 values(1),(2)")
	tk.MustInterDirc("insert into t2 values(1),(null)")
	tk.MustQuery("select * from t1 where a not in (select a from t2 where t1.a = t2.a)").Check(testkit.Events(
		"2",
	))
	tk.MustQuery("select * from t1 where a != all (select a from t2 where t1.a = t2.a)").Check(testkit.Events(
		"2",
	))
	tk.MustQuery("select * from t1 where a <> all (select a from t2 where t1.a = t2.a)").Check(testkit.Events(
		"2",
	))
}

func (s *testSuiteJoin1) TestScalarFuncNullSemiJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("insert into t values(null, 1), (1, 2)")
	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet s(a varchar(20), b varchar(20))")
	tk.MustInterDirc("insert into s values(null, '1')")
	tk.MustQuery("select a in (select a from s) from t").Check(testkit.Events("<nil>", "<nil>"))
	tk.MustInterDirc("drop causet s")
	tk.MustInterDirc("create causet s(a int, b int)")
	tk.MustInterDirc("insert into s values(null, 1)")
	tk.MustQuery("select a in (select a+b from s) from t").Check(testkit.Events("<nil>", "<nil>"))
}

func (s *testSuiteJoin1) TestInjectProjOnTopN(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1(a bigint, b bigint)")
	tk.MustInterDirc("create causet t2(a bigint, b bigint)")
	tk.MustInterDirc("insert into t1 values(1, 1)")
	tk.MustQuery("select t1.a+t1.b as result from t1 left join t2 on 1 = 0 order by result limit 20;").Check(testkit.Events(
		"2",
	))
}

func (s *testSuiteJoin1) TestIssue11544(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet 11544t(a int)")
	tk.MustInterDirc("create causet 11544tt(a int, b varchar(10), index idx(a, b(3)))")
	tk.MustInterDirc("insert into 11544t values(1)")
	tk.MustInterDirc("insert into 11544tt values(1, 'aaaaaaa'), (1, 'aaaabbb'), (1, 'aaaacccc')")
	tk.MustQuery("select /*+ INL_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and (tt.b = 'aaaaaaa' or tt.b = 'aaaabbb')").Check(testkit.Events("1 1 aaaaaaa", "1 1 aaaabbb"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and (tt.b = 'aaaaaaa' or tt.b = 'aaaabbb')").Check(testkit.Events("1 1 aaaaaaa", "1 1 aaaabbb"))
	// INL_MERGE_JOIN is invalid
	tk.MustQuery("select /*+ INL_MERGE_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and (tt.b = 'aaaaaaa' or tt.b = 'aaaabbb')").Sort().Check(testkit.Events("1 1 aaaaaaa", "1 1 aaaabbb"))

	tk.MustQuery("select /*+ INL_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and tt.b in ('aaaaaaa', 'aaaabbb', 'aaaacccc')").Check(testkit.Events("1 1 aaaaaaa", "1 1 aaaabbb", "1 1 aaaacccc"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and tt.b in ('aaaaaaa', 'aaaabbb', 'aaaacccc')").Check(testkit.Events("1 1 aaaaaaa", "1 1 aaaabbb", "1 1 aaaacccc"))
	// INL_MERGE_JOIN is invalid
	tk.MustQuery("select /*+ INL_MERGE_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and tt.b in ('aaaaaaa', 'aaaabbb', 'aaaacccc')").Sort().Check(testkit.Events("1 1 aaaaaaa", "1 1 aaaabbb", "1 1 aaaacccc"))
}

func (s *testSuiteJoin1) TestIssue11390(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet 11390t (k1 int unsigned, k2 int unsigned, key(k1, k2))")
	tk.MustInterDirc("insert into 11390t values(1, 1)")
	tk.MustQuery("select /*+ INL_JOIN(t1, t2) */ * from 11390t t1, 11390t t2 where t1.k2 > 0 and t1.k2 = t2.k2 and t2.k1=1;").Check(testkit.Events("1 1 1 1"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2) */ * from 11390t t1, 11390t t2 where t1.k2 > 0 and t1.k2 = t2.k2 and t2.k1=1;").Check(testkit.Events("1 1 1 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2) */ * from 11390t t1, 11390t t2 where t1.k2 > 0 and t1.k2 = t2.k2 and t2.k1=1;").Check(testkit.Events("1 1 1 1"))
}

func (s *testSuiteJoinSerial) TestOuterBlockBuildHashBlockIsuse13933(c *C) {
	causetembedded.ForceUseOuterBuild4Test = true
	defer func() { causetembedded.ForceUseOuterBuild4Test = false }()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, s")
	tk.MustInterDirc("create causet t (a int,b int)")
	tk.MustInterDirc("create causet s (a int,b int)")
	tk.MustInterDirc("insert into t values (11,11),(1,2)")
	tk.MustInterDirc("insert into s values (1,2),(2,1),(11,11)")
	tk.MustQuery("select * from t left join s on s.a > t.a").Sort().Check(testkit.Events("1 2 11 11", "1 2 2 1", "11 11 <nil> <nil>"))
	tk.MustQuery("explain select * from t left join s on s.a > t.a").Check(testkit.Events(
		"HashJoin_6 99900000.00 root  CARTESIAN left outer join, other cond:gt(test.s.a, test.t.a)",
		"├─BlockReader_8(Build) 10000.00 root  data:BlockFullScan_7",
		"│ └─BlockFullScan_7 10000.00 cop[einsteindb] causet:t keep order:false, stats:pseudo",
		"└─BlockReader_11(Probe) 9990.00 root  data:Selection_10",
		"  └─Selection_10 9990.00 cop[einsteindb]  not(isnull(test.s.a))",
		"    └─BlockFullScan_9 10000.00 cop[einsteindb] causet:s keep order:false, stats:pseudo"))
	tk.MustInterDirc("drop causet if exists t, s")
	tk.MustInterDirc("Create causet s (a int, b int, key(b))")
	tk.MustInterDirc("Create causet t (a int, b int, key(b))")
	tk.MustInterDirc("Insert into s values (1,2),(2,1),(11,11)")
	tk.MustInterDirc("Insert into t values (11,2),(1,2),(5,2)")
	tk.MustQuery("select /*+ INL_HASH_JOIN(s)*/ * from t left join s on s.b=t.b and s.a < t.a;").Sort().Check(testkit.Events("1 2 <nil> <nil>", "11 2 1 2", "5 2 1 2"))
	tk.MustQuery("explain select /*+ INL_HASH_JOIN(s)*/ * from t left join s on s.b=t.b and s.a < t.a;").Check(testkit.Events(
		"IndexHashJoin_14 12475.01 root  left outer join, inner:IndexLookUp_11, outer key:test.t.b, inner key:test.s.b, other cond:lt(test.s.a, test.t.a)",
		"├─BlockReader_24(Build) 10000.00 root  data:BlockFullScan_23",
		"│ └─BlockFullScan_23 10000.00 cop[einsteindb] causet:t keep order:false, stats:pseudo",
		"└─IndexLookUp_11(Probe) 1.25 root  ",
		"  ├─Selection_9(Build) 1.25 cop[einsteindb]  not(isnull(test.s.b))",
		"  │ └─IndexRangeScan_7 1.25 cop[einsteindb] causet:s, index:b(b) range: decided by [eq(test.s.b, test.t.b)], keep order:false, stats:pseudo",
		"  └─Selection_10(Probe) 1.25 cop[einsteindb]  not(isnull(test.s.a))",
		"    └─BlockEventIDScan_8 1.25 cop[einsteindb] causet:s keep order:false, stats:pseudo"))
}

func (s *testSuiteJoin1) TestIssue13177(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a varchar(20), b int, c int)")
	tk.MustInterDirc("create causet t2(a varchar(20), b int, c int, primary key(a, b))")
	tk.MustInterDirc("insert into t1 values(\"abcd\", 1, 1), (\"bacd\", 2, 2), (\"cbad\", 3, 3)")
	tk.MustInterDirc("insert into t2 values(\"bcd\", 1, 1), (\"acd\", 2, 2), (\"bad\", 3, 3)")
	tk.MustQuery("select /*+ inl_join(t1, t2) */ * from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Sort().Check(testkit.Events(
		"abcd 1 1 bcd 1 1",
		"bacd 2 2 acd 2 2",
		"cbad 3 3 bad 3 3",
	))
	tk.MustQuery("select /*+ inl_hash_join(t1, t2) */ * from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Sort().Check(testkit.Events(
		"abcd 1 1 bcd 1 1",
		"bacd 2 2 acd 2 2",
		"cbad 3 3 bad 3 3",
	))
	tk.MustQuery("select /*+ inl_merge_join(t1, t2) */ * from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Sort().Check(testkit.Events(
		"abcd 1 1 bcd 1 1",
		"bacd 2 2 acd 2 2",
		"cbad 3 3 bad 3 3",
	))
	tk.MustQuery("select /*+ inl_join(t1, t2) */ t1.* from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Sort().Check(testkit.Events(
		"abcd 1 1",
		"bacd 2 2",
		"cbad 3 3",
	))
	tk.MustQuery("select /*+ inl_hash_join(t1, t2) */ t1.* from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Sort().Check(testkit.Events(
		"abcd 1 1",
		"bacd 2 2",
		"cbad 3 3",
	))
	tk.MustQuery("select /*+ inl_merge_join(t1, t2) */ t1.* from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Sort().Check(testkit.Events(
		"abcd 1 1",
		"bacd 2 2",
		"cbad 3 3",
	))
}

func (s *testSuiteJoin1) TestIssue14514(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (pk varchar(14) primary key, a varchar(12));")
	tk.MustQuery("select * from (select t1.pk or '/' as c from t as t1 left join t as t2 on t1.a = t2.pk) as t where t.c = 1;").Check(testkit.Events())
}

func (s *testSuiteJoinSerial) TestOuterMatchStatusIssue14742(c *C) {
	causetembedded.ForceUseOuterBuild4Test = true
	defer func() { causetembedded.ForceUseOuterBuild4Test = false }()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists testjoin;")
	tk.MustInterDirc("create causet testjoin(a int);")
	tk.Se.GetStochastikVars().MaxChunkSize = 2

	tk.MustInterDirc("insert into testjoin values (NULL);")
	tk.MustInterDirc("insert into testjoin values (1);")
	tk.MustInterDirc("insert into testjoin values (2), (2), (2);")
	tk.MustQuery("SELECT * FROM testjoin t1 RIGHT JOIN testjoin t2 ON t1.a > t2.a order by t1.a, t2.a;").Check(testkit.Events(
		"<nil> <nil>",
		"<nil> 2",
		"<nil> 2",
		"<nil> 2",
		"2 1",
		"2 1",
		"2 1",
	))
}

func (s *testSuiteJoinSerial) TestInlineProjection4HashJoinIssue15316(c *C) {
	// Two necessary factors to reproduce this issue:
	// (1) taking HashLeftJoin, i.e., letting the probing tuple lay at the left side of joined tuples
	// (2) the projection only contains a part of defCausumns from the build side, i.e., pruning the same probe side
	causetembedded.ForcedHashLeftJoin4Test = true
	defer func() { causetembedded.ForcedHashLeftJoin4Test = false }()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet S (a int not null, b int, c int);")
	tk.MustInterDirc("create causet T (a int not null, b int, c int);")
	tk.MustInterDirc("insert into S values (0,1,2),(0,1,null),(0,1,2);")
	tk.MustInterDirc("insert into T values (0,10,2),(0,10,null),(1,10,2);")
	tk.MustQuery("select T.a,T.a,T.c from S join T on T.a = S.a where S.b<T.b order by T.a,T.c;").Check(testkit.Events(
		"0 0 <nil>",
		"0 0 <nil>",
		"0 0 <nil>",
		"0 0 2",
		"0 0 2",
		"0 0 2",
	))
	// NOTE: the HashLeftJoin should be kept
	tk.MustQuery("explain select T.a,T.a,T.c from S join T on T.a = S.a where S.b<T.b order by T.a,T.c;").Check(testkit.Events(
		"Sort_8 12487.50 root  test.t.a, test.t.c",
		"└─Projection_10 12487.50 root  test.t.a, test.t.a, test.t.c",
		"  └─HashJoin_11 12487.50 root  inner join, equal:[eq(test.s.a, test.t.a)], other cond:lt(test.s.b, test.t.b)",
		"    ├─BlockReader_17(Build) 9990.00 root  data:Selection_16",
		"    │ └─Selection_16 9990.00 cop[einsteindb]  not(isnull(test.t.b))",
		"    │   └─BlockFullScan_15 10000.00 cop[einsteindb] causet:T keep order:false, stats:pseudo",
		"    └─BlockReader_14(Probe) 9990.00 root  data:Selection_13",
		"      └─Selection_13 9990.00 cop[einsteindb]  not(isnull(test.s.b))",
		"        └─BlockFullScan_12 10000.00 cop[einsteindb] causet:S keep order:false, stats:pseudo"))
}

func (s *testSuiteJoinSerial) TestIssue18070(c *C) {
	config.GetGlobalConfig().OOMCausetAction = config.OOMCausetActionCancel
	defer func() { config.GetGlobalConfig().OOMCausetAction = config.OOMCausetActionLog }()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int, index(a))")
	tk.MustInterDirc("create causet t2(a int, index(a))")
	tk.MustInterDirc("insert into t1 values(1),(2)")
	tk.MustInterDirc("insert into t2 values(1),(1),(2),(2)")
	tk.MustInterDirc("set @@milevadb_mem_quota_query=1000")
	err := tk.QueryToErr("select /*+ inl_hash_join(t1)*/ * from t1 join t2 on t1.a = t2.a;")
	c.Assert(strings.Contains(err.Error(), "Out Of Memory Quota!"), IsTrue)

	fpName := "github.com/whtcorpsinc/milevadb/interlock/mocHoTTexMergeJoinOOMPanic"
	c.Assert(failpoint.Enable(fpName, `panic("ERROR 1105 (HY000): Out Of Memory Quota![conn_id=1]")`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable(fpName), IsNil)
	}()
	err = tk.QueryToErr("select /*+ inl_merge_join(t1)*/ * from t1 join t2 on t1.a = t2.a;")
	c.Assert(strings.Contains(err.Error(), "Out Of Memory Quota!"), IsTrue)
}

func (s *testSuiteJoin1) TestIssue18564(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int, b int, primary key(a), index idx(b,a));")
	tk.MustInterDirc("create causet t2(a int, b int, primary key(a), index idx(b,a));")
	tk.MustInterDirc("insert into t1 values(1, 1)")
	tk.MustInterDirc("insert into t2 values(1, 1)")
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t1 FORCE INDEX (idx) join t2 on t1.b=t2.b and t1.a = t2.a").Check(testkit.Events("1 1 1 1"))
}

func (s *testSuite9) TestIssue18572_1(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1(a int, b int, index idx(b));")
	tk.MustInterDirc("insert into t1 values(1, 1);")
	tk.MustInterDirc("insert into t1 select * from t1;")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinInnerWorkerErr", "return"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinInnerWorkerErr"), IsNil)
	}()

	rs, err := tk.InterDirc("select /*+ inl_hash_join(t1) */ * from t1 right join t1 t2 on t1.b=t2.b;")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(context.Background(), nil, rs)
	c.Assert(strings.Contains(err.Error(), "mocHoTTexHashJoinInnerWorkerErr"), IsTrue)
}

func (s *testSuite9) TestIssue18572_2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1(a int, b int, index idx(b));")
	tk.MustInterDirc("insert into t1 values(1, 1);")
	tk.MustInterDirc("insert into t1 select * from t1;")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinOuterWorkerErr", "return"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinOuterWorkerErr"), IsNil)
	}()

	rs, err := tk.InterDirc("select /*+ inl_hash_join(t1) */ * from t1 right join t1 t2 on t1.b=t2.b;")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(context.Background(), nil, rs)
	c.Assert(strings.Contains(err.Error(), "mocHoTTexHashJoinOuterWorkerErr"), IsTrue)
}

func (s *testSuite9) TestIssue18572_3(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1(a int, b int, index idx(b));")
	tk.MustInterDirc("insert into t1 values(1, 1);")
	tk.MustInterDirc("insert into t1 select * from t1;")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinBuildErr", "return"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/testIndexHashJoinBuildErr"), IsNil)
	}()

	rs, err := tk.InterDirc("select /*+ inl_hash_join(t1) */ * from t1 right join t1 t2 on t1.b=t2.b;")
	c.Assert(err, IsNil)
	_, err = stochastik.GetEvents4Test(context.Background(), nil, rs)
	c.Assert(strings.Contains(err.Error(), "mocHoTTexHashJoinBuildErr"), IsTrue)
}

func (s *testSuite9) TestIssue19112(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1 ( c_int int, c_decimal decimal(12, 6), key(c_int), unique key(c_decimal) )")
	tk.MustInterDirc("create causet t2 like t1")
	tk.MustInterDirc("insert into t1 (c_int, c_decimal) values (1, 4.064000), (2, 0.257000), (3, 1.010000)")
	tk.MustInterDirc("insert into t2 (c_int, c_decimal) values (1, 4.064000), (3, 1.010000)")
	tk.MustQuery("select /*+ HASH_JOIN(t1,t2) */  * from t1 join t2 on t1.c_decimal = t2.c_decimal order by t1.c_int").Check(testkit.Events(
		"1 4.064000 1 4.064000",
		"3 1.010000 3 1.010000"))
}

func (s *testSuiteJoin3) TestIssue11896(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	// compare bigint to bit(64)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 bigint)")
	tk.MustInterDirc("create causet t1(c1 bit(64))")
	tk.MustInterDirc("insert into t value(1)")
	tk.MustInterDirc("insert into t1 value(1)")
	tk.MustQuery("select * from t, t1 where t.c1 = t1.c1").Check(
		testkit.Events("1 \x00\x00\x00\x00\x00\x00\x00\x01"))

	// compare int to bit(32)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int)")
	tk.MustInterDirc("create causet t1(c1 bit(32))")
	tk.MustInterDirc("insert into t value(1)")
	tk.MustInterDirc("insert into t1 value(1)")
	tk.MustQuery("select * from t, t1 where t.c1 = t1.c1").Check(
		testkit.Events("1 \x00\x00\x00\x01"))

	// compare mediumint to bit(24)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 mediumint)")
	tk.MustInterDirc("create causet t1(c1 bit(24))")
	tk.MustInterDirc("insert into t value(1)")
	tk.MustInterDirc("insert into t1 value(1)")
	tk.MustQuery("select * from t, t1 where t.c1 = t1.c1").Check(
		testkit.Events("1 \x00\x00\x01"))

	// compare smallint to bit(16)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 smallint)")
	tk.MustInterDirc("create causet t1(c1 bit(16))")
	tk.MustInterDirc("insert into t value(1)")
	tk.MustInterDirc("insert into t1 value(1)")
	tk.MustQuery("select * from t, t1 where t.c1 = t1.c1").Check(
		testkit.Events("1 \x00\x01"))

	// compare tinyint to bit(8)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 tinyint)")
	tk.MustInterDirc("create causet t1(c1 bit(8))")
	tk.MustInterDirc("insert into t value(1)")
	tk.MustInterDirc("insert into t1 value(1)")
	tk.MustQuery("select * from t, t1 where t.c1 = t1.c1").Check(
		testkit.Events("1 \x01"))
}

func (s *testSuiteJoin3) TestIssue19498(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t1 (c_int int, primary key (c_int));")
	tk.MustInterDirc("insert into t1 values (1),(2),(3),(4)")
	tk.MustInterDirc("drop causet if exists t2;")
	tk.MustInterDirc("create causet t2 (c_str varchar(40));")
	tk.MustInterDirc("insert into t2 values ('zen sammet');")
	tk.MustInterDirc("insert into t2 values ('happy fermat');")
	tk.MustInterDirc("insert into t2 values ('happy archimedes');")
	tk.MustInterDirc("insert into t2 values ('happy hypatia');")

	tk.MustInterDirc("drop causet if exists t3;")
	tk.MustInterDirc("create causet t3 (c_int int, c_str varchar(40), primary key (c_int), key (c_str));")
	tk.MustInterDirc("insert into t3 values (1, 'sweet hoover');")
	tk.MustInterDirc("insert into t3 values (2, 'awesome elion');")
	tk.MustInterDirc("insert into t3 values (3, 'hungry khayyam');")
	tk.MustInterDirc("insert into t3 values (4, 'objective kapitsa');")

	rs := tk.MustQuery("select c_str, (select /*+ INL_JOIN(t1,t3) */ max(t1.c_int) from t1, t3 where t1.c_int = t3.c_int and t2.c_str > t3.c_str) q from t2 order by c_str;")
	rs.Check(testkit.Events("happy archimedes 2", "happy fermat 2", "happy hypatia 2", "zen sammet 4"))

	rs = tk.MustQuery("select c_str, (select /*+ INL_HASH_JOIN(t1,t3) */ max(t1.c_int) from t1, t3 where t1.c_int = t3.c_int and t2.c_str > t3.c_str) q from t2 order by c_str;")
	rs.Check(testkit.Events("happy archimedes 2", "happy fermat 2", "happy hypatia 2", "zen sammet 4"))

	rs = tk.MustQuery("select c_str, (select /*+ INL_MERGE_JOIN(t1,t3) */ max(t1.c_int) from t1, t3 where t1.c_int = t3.c_int and t2.c_str > t3.c_str) q from t2 order by c_str;")
	rs.Check(testkit.Events("happy archimedes 2", "happy fermat 2", "happy hypatia 2", "zen sammet 4"))
}

func (s *testSuiteJoin3) TestIssue19500(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t1 (c_int int, primary key (c_int));")
	tk.MustInterDirc("insert into t1 values (1),(2),(3),(4),(5);")
	tk.MustInterDirc("drop causet if exists t2;")
	tk.MustInterDirc("create causet t2 (c_int int unsigned, c_str varchar(40), primary key (c_int), key (c_str));")
	tk.MustInterDirc("insert into t2 values (1, 'dazzling panini'),(2, 'infallible perlman'),(3, 'recursing cannon'),(4, 'vigorous satoshi'),(5, 'vigilant gauss'),(6, 'nervous jackson');\n")
	tk.MustInterDirc("drop causet if exists t3;")
	tk.MustInterDirc("create causet t3 (c_int int, c_str varchar(40), key (c_str));")
	tk.MustInterDirc("insert into t3 values (1, 'sweet morse'),(2, 'reverent golick'),(3, 'clever rubin'),(4, 'flamboyant morse');")
	tk.MustQuery("select (select (select sum(c_int) from t3 where t3.c_str > t2.c_str) from t2 where t2.c_int > t1.c_int order by c_int limit 1) q from t1 order by q;").
		Check(testkit.Events("<nil>", "<nil>", "3", "3", "3"))
}

func (s *testSuiteJoinSerial) TestExplainAnalyzeJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1,t2;")
	tk.MustInterDirc("create causet t1 (a int, b int, unique index (a));")
	tk.MustInterDirc("create causet t2 (a int, b int, unique index (a))")
	tk.MustInterDirc("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustInterDirc("insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5)")
	// Test for index lookup join.
	rows := tk.MustQuery("explain analyze select /*+ INL_JOIN(t1, t2) */ * from t1,t2 where t1.a=t2.a;").Events()
	c.Assert(len(rows), Equals, 8)
	c.Assert(rows[0][0], Matches, "IndexJoin_.*")
	c.Assert(rows[0][5], Matches, "time:.*, loops:.*, inner:{total:.*, concurrency:.*, task:.*, construct:.*, fetch:.*, build:.*}, probe:.*")
	// Test for index lookup hash join.
	rows = tk.MustQuery("explain analyze select /*+ INL_HASH_JOIN(t1, t2) */ * from t1,t2 where t1.a=t2.a;").Events()
	c.Assert(len(rows), Equals, 8)
	c.Assert(rows[0][0], Matches, "IndexHashJoin.*")
	c.Assert(rows[0][5], Matches, "time:.*, loops:.*, inner:{total:.*, concurrency:.*, task:.*, construct:.*, fetch:.*, build:.*, join:.*}")
	// Test for hash join.
	rows = tk.MustQuery("explain analyze select /*+ HASH_JOIN(t1, t2) */ * from t1,t2 where t1.a=t2.a;").Events()
	c.Assert(len(rows), Equals, 7)
	c.Assert(rows[0][0], Matches, "HashJoin.*")
	c.Assert(rows[0][5], Matches, "time:.*, loops:.*, build_hash_block:{total:.*, fetch:.*, build:.*}, probe:{concurrency:5, total:.*, max:.*, probe:.*, fetch:.*}")
}
