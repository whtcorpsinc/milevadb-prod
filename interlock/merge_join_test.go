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
	"bytes"
	"fmt"
	"math/rand"
	"strings"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

const plan1 = `[[BlockScan_12 {
    "EDB": "test",
    "causet": "t1",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_17] [BlockScan_15 {
    "EDB": "test",
    "causet": "t2",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_17] [MergeJoin_17 {
    "eqCond": [
        "eq(test.t1.c1, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftCauset": "BlockScan_12",
    "rightCauset": "BlockScan_15",
    "desc": "false"
} MergeJoin_8] [BlockScan_22 {
    "EDB": "test",
    "causet": "t3",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_8] [MergeJoin_8 {
    "eqCond": [
        "eq(test.t2.c1, test.t3.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftCauset": "MergeJoin_17",
    "rightCauset": "BlockScan_22",
    "desc": "false"
} Sort_23] [Sort_23 {
    "exprs": [
        {
            "Expr": "test.t1.c1",
            "Desc": false
        }
    ],
    "limit": null,
    "child": "MergeJoin_8"
} ]]`

const plan2 = `[[BlockScan_12 {
    "EDB": "test",
    "causet": "t1",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_17] [BlockScan_15 {
    "EDB": "test",
    "causet": "t2",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_17] [MergeJoin_17 {
    "eqCond": [
        "eq(test.t1.c1, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftCauset": "BlockScan_12",
    "rightCauset": "BlockScan_15",
    "desc": "false"
} MergeJoin_8] [BlockScan_22 {
    "EDB": "test",
    "causet": "t3",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_8] [MergeJoin_8 {
    "eqCond": [
        "eq(test.t2.c1, test.t3.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftCauset": "MergeJoin_17",
    "rightCauset": "BlockScan_22",
    "desc": "false"
} Sort_23] [Sort_23 {
    "exprs": [
        {
            "Expr": "test.t1.c1",
            "Desc": false
        }
    ],
    "limit": null,
    "child": "MergeJoin_8"
} ]]`

const plan3 = `[[BlockScan_12 {
    "EDB": "test",
    "causet": "t1",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_9] [BlockScan_15 {
    "EDB": "test",
    "causet": "t2",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_9] [MergeJoin_9 {
    "eqCond": [
        "eq(test.t1.c1, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftCauset": "BlockScan_12",
    "rightCauset": "BlockScan_15",
    "desc": "false"
} Sort_16] [Sort_16 {
    "exprs": [
        {
            "Expr": "test.t1.c1",
            "Desc": false
        }
    ],
    "limit": null,
    "child": "MergeJoin_9"
} MergeJoin_8] [BlockScan_23 {
    "EDB": "test",
    "causet": "t3",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "causet filter conditions": null
    }
} MergeJoin_8] [MergeJoin_8 {
    "eqCond": [
        "eq(test.t1.c1, test.t3.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftCauset": "Sort_16",
    "rightCauset": "BlockScan_23",
    "desc": "false"
} ]]`

func checkMergeAndRun(tk *testkit.TestKit, c *C, allegrosql string) *testkit.Result {
	explainedALLEGROSQL := "explain " + allegrosql
	result := tk.MustQuery(explainedALLEGROSQL)
	resultStr := fmt.Sprintf("%v", result.Events())
	if !strings.ContainsAny(resultStr, "MergeJoin") {
		c.Error("Expected MergeJoin in plan.")
	}
	return tk.MustQuery(allegrosql)
}

func checkCausetAndRun(tk *testkit.TestKit, c *C, plan string, allegrosql string) *testkit.Result {
	explainedALLEGROSQL := "explain " + allegrosql
	tk.MustQuery(explainedALLEGROSQL)

	// TODO: Reopen it after refactoring explain.
	// resultStr := fmt.Sprintf("%v", result.Events())
	// if plan != resultStr {
	//     c.Errorf("Causet not match. Obtained:\n %s\nExpected:\n %s\n", resultStr, plan)
	// }
	return tk.MustQuery(allegrosql)
}

func (s *testSerialSuite1) TestMergeJoinInDisk(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/testMergeJoinEventContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/testMergeJoinEventContainerSpill"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	sm := &mockStochastikManager1{
		PS: make([]*soliton.ProcessInfo, 0),
	}
	tk.Se.SetStochastikManager(sm)
	s.petri.ExpensiveQueryHandle().SetStochastikManager(sm)

	tk.MustInterDirc("set @@milevadb_mem_quota_query=1;")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int, c2 int)")
	tk.MustInterDirc("create causet t1(c1 int, c2 int)")
	tk.MustInterDirc("insert into t values(1,1)")
	tk.MustInterDirc("insert into t1 values(1,3),(4,4)")

	result := checkMergeAndRun(tk, c, "select /*+ MilevaDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Events("1 3 1 1"))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.MaxConsumed(), Greater, int64(0))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.DiskTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.DiskTracker.MaxConsumed(), Greater, int64(0))
	return
}

func (s *testSuite2) TestMergeJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int, c2 int)")
	tk.MustInterDirc("create causet t1(c1 int, c2 int)")
	tk.MustInterDirc("insert into t values(1,1),(2,2)")
	tk.MustInterDirc("insert into t1 values(2,3),(4,4)")

	result := checkMergeAndRun(tk, c, "select /*+ MilevaDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Events("1 1 <nil> <nil>"))
	result = checkMergeAndRun(tk, c, "select /*+ MilevaDB_SMJ(t) */ * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Events("<nil> <nil> 1 1"))
	result = checkMergeAndRun(tk, c, "select /*+ MilevaDB_SMJ(t) */ * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Events())
	result = checkMergeAndRun(tk, c, "select /*+ MilevaDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false")
	result.Check(testkit.Events())
	result = checkMergeAndRun(tk, c, "select /*+ MilevaDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1")
	result.Check(testkit.Events("1 1 <nil> <nil>", "2 2 2 3"))

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("drop causet if exists t3")

	tk.MustInterDirc("create causet t1 (c1 int, c2 int)")
	tk.MustInterDirc("create causet t2 (c1 int, c2 int)")
	tk.MustInterDirc("create causet t3 (c1 int, c2 int)")

	tk.MustInterDirc("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustInterDirc("insert into t2 values (1,1), (3,3), (5,5)")
	tk.MustInterDirc("insert into t3 values (1,1), (5,5), (9,9)")

	result = tk.MustQuery("select /*+ MilevaDB_SMJ(t1,t2,t3) */ * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;")
	result.Check(testkit.Events("<nil> <nil> <nil> <nil> 5 5", "<nil> <nil> <nil> <nil> 9 9", "1 1 1 1 1 1"))

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int)")
	tk.MustInterDirc("insert into t1 values (1), (1), (1)")
	result = tk.MustQuery("select/*+ MilevaDB_SMJ(t) */  * from t1 a join t1 b on a.c1 = b.c1;")
	result.Check(testkit.Events("1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int, index k(c1))")
	tk.MustInterDirc("create causet t1(c1 int)")
	tk.MustInterDirc("insert into t values (1),(2),(3),(4),(5),(6),(7)")
	tk.MustInterDirc("insert into t1 values (1),(2),(3),(4),(5),(6),(7)")
	result = tk.MustQuery("select /*+ MilevaDB_SMJ(a,b) */ a.c1 from t a , t1 b where a.c1 = b.c1 order by a.c1;")
	result.Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	result = tk.MustQuery("select /*+ MilevaDB_SMJ(a, b) */ a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 order by b.c1;")
	result.Check(testkit.Events("1", "2", "3"))
	// Test LogicalSelection under LogicalJoin.
	result = tk.MustQuery("select /*+ MilevaDB_SMJ(a, b) */ a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 and b.c1 is not null order by b.c1;")
	result.Check(testkit.Events("1", "2", "3"))
	tk.MustInterDirc("begin;")
	// Test LogicalLock under LogicalJoin.
	result = tk.MustQuery("select /*+ MilevaDB_SMJ(a, b) */ a.c1 from t a , (select * from t1 for uFIDelate) b where a.c1 = b.c1 order by a.c1;")
	result.Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	// Test LogicalUnionScan under LogicalJoin.
	tk.MustInterDirc("insert into t1 values(8);")
	result = tk.MustQuery("select /*+ MilevaDB_SMJ(a, b) */ a.c1 from t a , t1 b where a.c1 = b.c1;")
	result.Check(testkit.Events("1", "2", "3", "4", "5", "6", "7"))
	tk.MustInterDirc("rollback;")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(c1 int)")
	tk.MustInterDirc("create causet t1(c1 int unsigned)")
	tk.MustInterDirc("insert into t values (1)")
	tk.MustInterDirc("insert into t1 values (1)")
	result = tk.MustQuery("select /*+ MilevaDB_SMJ(t,t1) */ t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Events("1"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index a(a), index b(b))")
	tk.MustInterDirc("insert into t values(1, 2)")
	tk.MustQuery("select /*+ MilevaDB_SMJ(t, t1) */ t.a, t1.b from t right join t t1 on t.a = t1.b order by t.a").Check(testkit.Events("<nil> 2"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet t(a int, b int, primary key(a, b))")
	tk.MustInterDirc("insert into t value(1,1),(1,2),(1,3),(1,4)")
	tk.MustInterDirc("create causet s(a int, primary key(a))")
	tk.MustInterDirc("insert into s value(1)")
	tk.MustQuery("select /*+ MilevaDB_SMJ(t, s) */ count(*) from t join s on t.a = s.a").Check(testkit.Events("4"))

	// Test MilevaDB_SMJ for cartesian product.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("insert into t value(1),(2)")
	tk.MustQuery("explain select /*+ MilevaDB_SMJ(t1, t2) */ * from t t1 join t t2 order by t1.a, t2.a").Check(testkit.Events(
		"Sort_6 100000000.00 root  test.t.a, test.t.a",
		"└─MergeJoin_9 100000000.00 root  inner join",
		"  ├─BlockReader_13(Build) 10000.00 root  data:BlockFullScan_12",
		"  │ └─BlockFullScan_12 10000.00 cop[einsteindb] causet:t2 keep order:false, stats:pseudo",
		"  └─BlockReader_11(Probe) 10000.00 root  data:BlockFullScan_10",
		"    └─BlockFullScan_10 10000.00 cop[einsteindb] causet:t1 keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ * from t t1 join t t2 order by t1.a, t2.a").Check(testkit.Events(
		"1 1",
		"1 2",
		"2 1",
		"2 2",
	))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("insert into t values(1,1),(1,2)")
	tk.MustInterDirc("create causet s(a int, b int)")
	tk.MustInterDirc("insert into s values(1,1)")
	tk.MustQuery("explain select /*+ MilevaDB_SMJ(t, s) */ a in (select a from s where s.b >= t.b) from t").Check(testkit.Events(
		"MergeJoin_8 10000.00 root  left outer semi join, other cond:eq(test.t.a, test.s.a), ge(test.s.b, test.t.b)",
		"├─BlockReader_12(Build) 10000.00 root  data:BlockFullScan_11",
		"│ └─BlockFullScan_11 10000.00 cop[einsteindb] causet:s keep order:false, stats:pseudo",
		"└─BlockReader_10(Probe) 10000.00 root  data:BlockFullScan_9",
		"  └─BlockFullScan_9 10000.00 cop[einsteindb] causet:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ MilevaDB_SMJ(t, s) */ a in (select a from s where s.b >= t.b) from t").Check(testkit.Events(
		"1",
		"0",
	))

	// Test MilevaDB_SMJ for join with order by desc, see https://github.com/whtcorpsinc/milevadb/issues/14483
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t (a int, key(a))")
	tk.MustInterDirc("create causet t1 (a int, key(a))")
	tk.MustInterDirc("insert into t values (1), (2), (3)")
	tk.MustInterDirc("insert into t1 values (1), (2), (3)")
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t.a from t, t1 where t.a = t1.a order by t1.a desc").Check(testkit.Events(
		"3", "2", "1"))
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int, key(a), key(b))")
	tk.MustInterDirc("insert into t values (1,1),(1,2),(1,3),(2,1),(2,2),(3,1),(3,2),(3,3)")
	tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ t1.a from t t1, t t2 where t1.a = t2.b order by t1.a desc").Check(testkit.Events(
		"3", "3", "3", "3", "3", "3",
		"2", "2", "2", "2", "2", "2",
		"1", "1", "1", "1", "1", "1", "1", "1", "1"))

	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet s (a int)")
	tk.MustInterDirc("insert into s values (4), (1), (3), (2)")
	tk.MustQuery("explain select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Events(
		"MergeJoin_28 12487.50 root  inner join, left key:test.s.a, right key:test.s.a",
		"├─Sort_31(Build) 9990.00 root  test.s.a:desc",
		"│ └─BlockReader_26 9990.00 root  data:Selection_25",
		"│   └─Selection_25 9990.00 cop[einsteindb]  not(isnull(test.s.a))",
		"│     └─BlockFullScan_24 10000.00 cop[einsteindb] causet:s keep order:false, stats:pseudo",
		"└─Sort_29(Probe) 9990.00 root  test.s.a:desc",
		"  └─BlockReader_21 9990.00 root  data:Selection_20",
		"    └─Selection_20 9990.00 cop[einsteindb]  not(isnull(test.s.a))",
		"      └─BlockFullScan_19 10000.00 cop[einsteindb] causet:s keep order:false, stats:pseudo",
	))
	tk.MustQuery("select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Events(
		"4", "3", "2", "1"))
}

func (s *testSuite2) Test3WaysMergeJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("drop causet if exists t3")
	tk.MustInterDirc("create causet t1(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustInterDirc("create causet t2(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustInterDirc("create causet t3(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustInterDirc("insert into t1 values(1,1),(2,2),(3,3)")
	tk.MustInterDirc("insert into t2 values(2,3),(3,4),(4,5)")
	tk.MustInterDirc("insert into t3 values(1,2),(2,4),(3,10)")
	result := checkCausetAndRun(tk, c, plan1, "select /*+ MilevaDB_SMJ(t1,t2,t3) */ * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Events("2 2 2 3 2 4", "3 3 3 4 3 10"))

	result = checkCausetAndRun(tk, c, plan2, "select /*+ MilevaDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Events("2 2 2 3 2 4", "3 3 3 4 3 10"))

	// In below case, t1 side filled with null when no matched join, so that order is not kept and sort appended
	// On the other hand, t1 order kept so no final sort appended
	result = checkCausetAndRun(tk, c, plan3, "select /*+ MilevaDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1 order by 1")
	result.Check(testkit.Events("2 2 2 3 2 4", "3 3 3 4 3 10"))
}

func (s *testSuite2) TestMergeJoinDifferentTypes(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("set @@stochastik.milevadb_interlock_concurrency = 4;")
	tk.MustInterDirc("set @@stochastik.milevadb_hash_join_concurrency = 5;")
	tk.MustInterDirc("set @@stochastik.milevadb_allegrosql_scan_concurrency = 15;")

	tk.MustInterDirc(`use test`)
	tk.MustInterDirc(`drop causet if exists t1;`)
	tk.MustInterDirc(`drop causet if exists t2;`)
	tk.MustInterDirc(`create causet t1(a bigint, b bit(1), index idx_a(a));`)
	tk.MustInterDirc(`create causet t2(a bit(1) not null, b bit(1), index idx_a(a));`)
	tk.MustInterDirc(`insert into t1 values(1, 1);`)
	tk.MustInterDirc(`insert into t2 values(1, 1);`)
	tk.MustQuery(`select hex(t1.a), hex(t2.a) from t1 inner join t2 on t1.a=t2.a;`).Check(testkit.Events(`1 1`))

	tk.MustInterDirc(`drop causet if exists t1;`)
	tk.MustInterDirc(`drop causet if exists t2;`)
	tk.MustInterDirc(`create causet t1(a float, b double, index idx_a(a));`)
	tk.MustInterDirc(`create causet t2(a double not null, b double, index idx_a(a));`)
	tk.MustInterDirc(`insert into t1 values(1, 1);`)
	tk.MustInterDirc(`insert into t2 values(1, 1);`)
	tk.MustQuery(`select t1.a, t2.a from t1 inner join t2 on t1.a=t2.a;`).Check(testkit.Events(`1 1`))

	tk.MustInterDirc(`drop causet if exists t1;`)
	tk.MustInterDirc(`drop causet if exists t2;`)
	tk.MustInterDirc(`create causet t1(a bigint signed, b bigint, index idx_a(a));`)
	tk.MustInterDirc(`create causet t2(a bigint unsigned, b bigint, index idx_a(a));`)
	tk.MustInterDirc(`insert into t1 values(-1, 0), (-1, 0), (0, 0), (0, 0), (pow(2, 63), 0), (pow(2, 63), 0);`)
	tk.MustInterDirc(`insert into t2 values(18446744073709551615, 0), (18446744073709551615, 0), (0, 0), (0, 0), (pow(2, 63), 0), (pow(2, 63), 0);`)
	tk.MustQuery(`select t1.a, t2.a from t1 join t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Events(
		`0 0`,
		`0 0`,
		`0 0`,
		`0 0`,
	))
}

// TestVectorizedMergeJoin is used to test vectorized merge join with some corner cases.
func (s *testSuiteJoin3) TestVectorizedMergeJoin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1 (a int, b int)")
	tk.MustInterDirc("create causet t2 (a int, b int)")
	runTest := func(t1, t2 []int) {
		tk.MustInterDirc("truncate causet t1")
		tk.MustInterDirc("truncate causet t2")
		insert := func(tName string, ts []int) {
			for i, n := range ts {
				if n == 0 {
					continue
				}
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("insert into %v values ", tName))
				for j := 0; j < n; j++ {
					if j > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(fmt.Sprintf("(%v, %v)", i, rand.Intn(10)))
				}
				tk.MustInterDirc(buf.String())
			}
		}
		insert("t1", t1)
		insert("t2", t2)

		tk.MustQuery("explain select /*+ MilevaDB_SMJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Check(testkit.Events(
			`MergeJoin_7 4150.01 root  inner join, left key:test.t1.a, right key:test.t2.a`,
			`├─Sort_15(Build) 3320.01 root  test.t2.a`,
			`│ └─BlockReader_14 3320.01 root  data:Selection_13`,
			`│   └─Selection_13 3320.01 cop[einsteindb]  lt(test.t2.b, 5), not(isnull(test.t2.a))`,
			`│     └─BlockFullScan_12 10000.00 cop[einsteindb] causet:t2 keep order:false, stats:pseudo`,
			`└─Sort_11(Probe) 3330.00 root  test.t1.a`,
			`  └─BlockReader_10 3330.00 root  data:Selection_9`,
			`    └─Selection_9 3330.00 cop[einsteindb]  gt(test.t1.b, 5), not(isnull(test.t1.a))`,
			`      └─BlockFullScan_8 10000.00 cop[einsteindb] causet:t1 keep order:false, stats:pseudo`,
		))
		tk.MustQuery("explain select /*+ MilevaDB_HJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Check(testkit.Events(
			`HashJoin_7 4150.01 root  inner join, equal:[eq(test.t1.a, test.t2.a)]`,
			`├─BlockReader_14(Build) 3320.01 root  data:Selection_13`,
			`│ └─Selection_13 3320.01 cop[einsteindb]  lt(test.t2.b, 5), not(isnull(test.t2.a))`,
			`│   └─BlockFullScan_12 10000.00 cop[einsteindb] causet:t2 keep order:false, stats:pseudo`,
			`└─BlockReader_11(Probe) 3330.00 root  data:Selection_10`,
			`  └─Selection_10 3330.00 cop[einsteindb]  gt(test.t1.b, 5), not(isnull(test.t1.a))`,
			`    └─BlockFullScan_9 10000.00 cop[einsteindb] causet:t1 keep order:false, stats:pseudo`,
		))

		r1 := tk.MustQuery("select /*+ MilevaDB_SMJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Sort()
		r2 := tk.MustQuery("select /*+ MilevaDB_HJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Sort()
		c.Assert(len(r1.Events()), Equals, len(r2.Events()))

		i := 0
		n := len(r1.Events())
		for i < n {
			c.Assert(len(r1.Events()[i]), Equals, len(r2.Events()[i]))
			for j := range r1.Events()[i] {
				c.Assert(r1.Events()[i][j], Equals, r2.Events()[i][j])
			}
			i += rand.Intn((n-i)/5+1) + 1 // just compare parts of results to speed up
		}
	}

	tk.Se.GetStochastikVars().MaxChunkSize = variable.DefInitChunkSize
	chunkSize := tk.Se.GetStochastikVars().MaxChunkSize
	cases := []struct {
		t1 []int
		t2 []int
	}{
		{[]int{0}, []int{chunkSize}},
		{[]int{0}, []int{chunkSize - 1}},
		{[]int{0}, []int{chunkSize + 1}},
		{[]int{1}, []int{chunkSize}},
		{[]int{1}, []int{chunkSize - 1}},
		{[]int{1}, []int{chunkSize + 1}},
		{[]int{chunkSize - 1}, []int{chunkSize}},
		{[]int{chunkSize - 1}, []int{chunkSize - 1}},
		{[]int{chunkSize - 1}, []int{chunkSize + 1}},
		{[]int{chunkSize}, []int{chunkSize}},
		{[]int{chunkSize}, []int{chunkSize - 1}},
		{[]int{chunkSize}, []int{chunkSize + 1}},
		{[]int{chunkSize + 1}, []int{chunkSize}},
		{[]int{chunkSize + 1}, []int{chunkSize - 1}},
		{[]int{chunkSize + 1}, []int{chunkSize + 1}},
		{[]int{1, 1, 1}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{0, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{chunkSize + 1, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
	}
	for _, ca := range cases {
		runTest(ca.t1, ca.t2)
		runTest(ca.t2, ca.t1)
	}
}

func (s *testSuite2) TestMergeJoinWithOtherConditions(c *C) {
	// more than one inner tuple should be filtered on other conditions
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test`)
	tk.MustInterDirc(`drop causet if exists R;`)
	tk.MustInterDirc(`drop causet if exists Y;`)
	tk.MustInterDirc(`create causet Y (a int primary key, b int, index id_b(b));`)
	tk.MustInterDirc(`insert into Y values (0,2),(2,2);`)
	tk.MustInterDirc(`create causet R (a int primary key, b int);`)
	tk.MustInterDirc(`insert into R values (2,2);`)
	// the max() limits the required rows at most one
	// TODO(fangzhuhe): specify Y as the build side using hints
	tk.MustQuery(`select /*+milevadb_smj(R)*/ max(Y.a) from R join Y  on R.a=Y.b where R.b <= Y.a;`).Check(testkit.Events(
		`2`,
	))
}
