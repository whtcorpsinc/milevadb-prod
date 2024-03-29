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
	"context"
	"fmt"
	"math"
	"strings"

	dto "github.com/prometheus/client_perceptron/go"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/ekvcache"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

var _ = SerialSuites(&testPointGetSuite{})

type testPointGetSuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	testData    solitonutil.TestData
}

func (s *testPointGetSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	s.dom = dom
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "point_get_plan")
	c.Assert(err, IsNil)
}

func (s *testPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPointGetSuite) TestPointGetCausetCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := embedded.PreparedCausetCacheEnabled()
	defer func() {
		embedded.SetPreparedCausetCache(orgEnable)
	}()
	embedded.SetPreparedCausetCache(true)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a bigint unsigned primary key, b int, c int, key idx_bc(b,c))")
	tk.MustInterDirc("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustQuery("explain select * from t where a = 1").Check(testkit.Rows(
		"Point_Get_1 1.00 root causet:t handle:1",
	))
	tk.MustQuery("explain select * from t where 1 = a").Check(testkit.Rows(
		"Point_Get_1 1.00 root causet:t handle:1",
	))
	tk.MustQuery("explain uFIDelate t set b=b+1, c=c+1 where a = 1").Check(testkit.Rows(
		"UFIDelate_2 N/A root  N/A",
		"└─Point_Get_1 1.00 root causet:t handle:1",
	))
	tk.MustQuery("explain delete from t where a = 1").Check(testkit.Rows(
		"Delete_2 N/A root  N/A",
		"└─Point_Get_1 1.00 root causet:t handle:1",
	))
	tk.MustQuery("explain select a from t where a = -1").Check(testkit.Rows(
		"BlockDual_5 0.00 root  rows:0",
	))
	tk.MustInterDirc(`prepare stmt0 from "select a from t where a = ?"`)
	tk.MustInterDirc("set @p0 = -1")
	tk.MustQuery("execute stmt0 using @p0").Check(testkit.Rows())
	metrics.ResetblockCausetCacheCounterFortTest = true
	metrics.CausetCacheCounter.Reset()
	counter := metrics.CausetCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	var hit float64
	// PointGetCauset for Select.
	tk.MustInterDirc(`prepare stmt1 from "select * from t where a = ?"`)
	tk.MustInterDirc(`prepare stmt2 from "select * from t where b = ? and c = ?"`)
	tk.MustInterDirc("set @param=1")
	tk.MustQuery("execute stmt1 using @param").Check(testkit.Rows("1 1 1"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(0))
	tk.MustInterDirc("set @param=2")
	tk.MustQuery("execute stmt1 using @param").Check(testkit.Rows("2 2 2"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(1))
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("2 2 2"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(1))
	tk.MustInterDirc("set @param=1")
	tk.MustQuery("execute stmt2 using @param, @param").Check(testkit.Rows("1 1 1"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	// PointGetCauset for UFIDelate.
	tk.MustInterDirc(`prepare stmt3 from "uFIDelate t set b=b+1, c=c+1 where a = ?"`)
	tk.MustInterDirc(`prepare stmt4 from "uFIDelate t set a=a+1 where b = ? and c = ?"`)
	tk.MustInterDirc("set @param=3")
	tk.MustInterDirc("execute stmt3 using @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"3 4 4",
	))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustInterDirc("set @param=4")
	tk.MustInterDirc("execute stmt4 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"4 4 4",
	))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	// PointGetCauset for Delete.
	tk.MustInterDirc(`prepare stmt5 from "delete from t where a = ?"`)
	tk.MustInterDirc(`prepare stmt6 from "delete from t where b = ? and c = ?"`)
	tk.MustInterDirc("execute stmt5 using @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
	))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustInterDirc("set @param=2")
	tk.MustInterDirc("execute stmt6 using @param, @param")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
	))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
	tk.MustInterDirc("insert into t (a, b, c) values (18446744073709551615, 4, 4)")
	tk.MustInterDirc("set @p1=-1")
	tk.MustInterDirc("set @p2=1")
	tk.MustInterDirc(`prepare stmt7 from "select a from t where a = ?"`)
	tk.MustQuery("execute stmt7 using @p1").Check(testkit.Rows())
	tk.MustQuery("execute stmt7 using @p2").Check(testkit.Rows("1"))
	counter.Write(pb)
	hit = pb.GetCounter().GetValue()
	c.Check(hit, Equals, float64(2))
}

func (s *testPointGetSuite) TestPointGetForUFIDelate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet fu (id int primary key, val int)")
	tk.MustInterDirc("insert into fu values (6, 6)")

	// In autocommit mode, outside a transaction, "for uFIDelate" doesn't take effect.
	checkUseForUFIDelate(tk, c, false)

	tk.MustInterDirc("begin")
	checkUseForUFIDelate(tk, c, true)
	tk.MustInterDirc("rollback")

	tk.MustInterDirc("set @@stochastik.autocommit = 0")
	checkUseForUFIDelate(tk, c, true)
	tk.MustInterDirc("rollback")
}

func checkUseForUFIDelate(tk *testkit.TestKit, c *C, expectLock bool) {
	res := tk.MustQuery("explain select * from fu where id = 6 for uFIDelate")
	// Point_Get_1	1.00	root	causet:fu, handle:6
	opInfo := res.Rows()[0][4]
	selectLock := strings.Contains(fmt.Sprintf("%s", opInfo), "dagger")
	c.Assert(selectLock, Equals, expectLock)

	tk.MustQuery("select * from fu where id = 6 for uFIDelate").Check(testkit.Rows("6 6"))
}

func (s *testPointGetSuite) TestWhereIn2BatchPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int primary key auto_increment not null, b int, c int, unique key idx_abc(a, b, c))")
	tk.MustInterDirc("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 5)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 1 1",
		"2 2 2",
		"3 3 3",
		"4 4 5",
	))
	tk.MustQuery("explain select * from t where a = 1 and b = 1 and c = 1").Check(testkit.Rows(
		"Point_Get_1 1.00 root causet:t, index:idx_abc(a, b, c) ",
	))
	tk.MustQuery("explain select * from t where 1 = a and 1 = b and 1 = c").Check(testkit.Rows(
		"Point_Get_1 1.00 root causet:t, index:idx_abc(a, b, c) ",
	))
	tk.MustQuery("explain select * from t where 1 = a and b = 1 and 1 = c").Check(testkit.Rows(
		"Point_Get_1 1.00 root causet:t, index:idx_abc(a, b, c) ",
	))
	tk.MustQuery("explain select * from t where (a, b, c) in ((1, 1, 1), (2, 2, 2))").Check(testkit.Rows(
		"Batch_Point_Get_1 2.00 root causet:t, index:idx_abc(a, b, c) keep order:false, desc:false",
	))

	tk.MustQuery("explain select * from t where a in (1, 2, 3, 4, 5)").Check(testkit.Rows(
		"Batch_Point_Get_1 5.00 root causet:t handle:[1 2 3 4 5], keep order:false, desc:false",
	))

	tk.MustQuery("explain select * from t where a in (1, 2, 3, 1, 2)").Check(testkit.Rows(
		"Batch_Point_Get_1 5.00 root causet:t handle:[1 2 3 1 2], keep order:false, desc:false",
	))

	tk.MustInterDirc("begin")
	tk.MustQuery("explain select * from t where a in (1, 2, 3, 1, 2) FOR UFIDelATE").Check(testkit.Rows(
		"Batch_Point_Get_1 5.00 root causet:t handle:[1 2 3 1 2], keep order:false, desc:false, dagger",
	))
	tk.MustInterDirc("rollback")

	tk.MustQuery("explain select * from t where (a) in ((1), (2), (3), (1), (2))").Check(testkit.Rows(
		"Batch_Point_Get_1 5.00 root causet:t handle:[1 2 3 1 2], keep order:false, desc:false",
	))

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int, unique key idx_ab(a, b))")
	tk.MustInterDirc("insert into t values(1, 2, 3), (2, 3, 4), (3, 4, 5), (4, 5, 6)")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"3 4 5",
		"4 5 6",
	))
	tk.MustQuery("explain select * from t where (a, b) in ((1, 2), (2, 3))").Check(testkit.Rows(
		"Batch_Point_Get_1 2.00 root causet:t, index:idx_ab(a, b) keep order:false, desc:false",
	))
	tk.MustQuery("select * from t where (a, b) in ((1, 2), (2, 3))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
	))
	tk.MustQuery("select * from t where (b, a) in ((1, 2), (2, 3))").Check(testkit.Rows())
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
	))
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2), (2, 1), (5, 4))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"4 5 6",
	))
	tk.MustQuery("select * from t where (b, a) in ((2, 1), (3, 2), (2, 1), (5, 4), (3, 4))").Check(testkit.Rows(
		"1 2 3",
		"2 3 4",
		"4 5 6",
	))

	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("explain select * from t where (a, b) in ((1, 2), (2, 3)) FOR UFIDelATE").Check(testkit.Rows(
		"Batch_Point_Get_1 2.00 root causet:t, index:idx_ab(a, b) keep order:false, desc:false, dagger",
	))
	tk.MustInterDirc("rollback")
}

// Test that the plan id will be reset before optimization every time.
func (s *testPointGetSuite) TestPointGetId(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c1 int primary key, c2 int)")
	defer tk.MustInterDirc("drop causet if exists t")
	pointGetQuery := "select c2 from t where c1 = 1"
	for i := 0; i < 2; i++ {
		ctx := tk.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(ctx, pointGetQuery)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := petri.GetPetri(ctx).SchemaReplicant()
		err = embedded.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, _, err := causet.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		// Test explain result is useless, plan id will be reset when running `explain`.
		c.Assert(p.ID(), Equals, 1)
	}
}

func (s *testPointGetSuite) TestCBOPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("set @@milevadb_enable_clustered_index=0")
	tk.MustInterDirc("create causet t (a varchar(20), b int, c int, d int, primary key(a), unique key(b, c))")
	tk.MustInterDirc("insert into t values('1',4,4,1), ('2',3,3,2), ('3',2,2,3), ('4',1,1,4)")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            []string
		Res               []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, allegrosql := range input {
		plan := tk.MustQuery("explain " + allegrosql)
		res := tk.MustQuery(allegrosql)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = allegrosql
			output[i].Causet = s.testData.ConvertRowsToStrings(plan.Rows())
			output[i].Res = s.testData.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Causet...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}

func (s *testPointGetSuite) TestBatchPointGetCausetCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := embedded.PreparedCausetCacheEnabled()
	defer func() {
		embedded.SetPreparedCausetCache(orgEnable)
	}()
	embedded.SetPreparedCausetCache(true)

	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int primary key, b int)")
	tk.MustInterDirc("insert into t values(1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustQuery("explain select * from t where a in (1, 2)").Check(testkit.Rows(
		"Batch_Point_Get_1 2.00 root causet:t handle:[1 2], keep order:false, desc:false",
	))
	tk.MustInterDirc("prepare stmt from 'select * from t where a in (?,?)'")
	tk.MustInterDirc("set @p1 = 1, @p2 = 2")
	tk.MustQuery("execute stmt using @p1, @p2;").Check(testkit.Rows(
		"1 1",
		"2 2",
	))
	tk.MustInterDirc("set @p1 = 3, @p2 = 4")
	tk.MustQuery("execute stmt using @p1, @p2;").Check(testkit.Rows(
		"3 3",
		"4 4",
	))
}

func (s *testPointGetSuite) TestBatchPointGetPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	orgEnable := embedded.PreparedCausetCacheEnabled()
	defer func() {
		embedded.SetPreparedCausetCache(orgEnable)
	}()
	embedded.SetPreparedCausetCache(true)

	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int primary key, b int) PARTITION BY HASH(a) PARTITIONS 4")
	tk.MustInterDirc("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustQuery("explain select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows(
		"Batch_Point_Get_1 4.00 root causet:t handle:[1 2 3 4], keep order:false, desc:false",
	))
	tk.MustQuery("select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows("1 1", "2 2", "3 3", "4 4"))

	tk.MustQuery("explain uFIDelate t set b = b + 1 where a in (1, 2, 3, 4)").Check(testkit.Rows(
		"UFIDelate_2 N/A root  N/A]\n[└─Batch_Point_Get_1 4.00 root causet:t handle:[1 2 3 4], keep order:false, desc:false",
	))
	tk.MustInterDirc("uFIDelate t set b = b + 1 where a in (1, 2, 3, 4)")
	tk.MustQuery("select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows("1 2", "2 3", "3 4", "4 5"))

	tk.MustQuery("explain delete from t where a in (1, 2, 3, 4)").Check(testkit.Rows(
		"Delete_2 N/A root  N/A]\n[└─Batch_Point_Get_1 4.00 root causet:t handle:[1 2 3 4], keep order:false, desc:false",
	))
	tk.MustInterDirc("delete from t where a in (1, 2, 3, 4)")
	tk.MustQuery("select * from t where a in (1, 2, 3, 4)").Check(testkit.Rows())

	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("create causet t(a int, b int, c int, primary key (a, b)) PARTITION BY HASH(a) PARTITIONS 4")
	tk.MustInterDirc("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)")
	tk.MustQuery("explain select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").Check(testkit.Rows(
		"Batch_Point_Get_1 4.00 root causet:t, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").
		Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3", "4 4 4"))

	tk.MustQuery("explain uFIDelate t set c = c + 1 where (a,b) in ((1,1),(2,2),(3,3),(4,4))").Check(testkit.Rows(
		"UFIDelate_2 N/A root  N/A]\n[└─Batch_Point_Get_1 4.00 root causet:t, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	tk.MustInterDirc("uFIDelate t set c = c + 1 where (a,b) in ((1,1),(2,2),(3,3),(4,4))")
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").Sort().
		Check(testkit.Rows("1 1 2", "2 2 3", "3 3 4", "4 4 5"))

	tk.MustQuery("explain delete from t where (a,b) in ((1,1),(2,2),(3,3),(4,4))").Check(testkit.Rows(
		"Delete_2 N/A root  N/A]\n[└─Batch_Point_Get_1 4.00 root causet:t, clustered index:PRIMARY(a, b) keep order:false, desc:false",
	))
	tk.MustInterDirc("delete from t where (a,b) in ((1,1),(2,2),(3,3),(4,4))")
	tk.MustQuery("select * from t where (a, b) in ((1, 1), (2, 2), (3, 3), (4, 4))").Check(testkit.Rows())
}

func (s *testPointGetSuite) TestIssue19141(c *C) {
	// For issue 19141, fix partition selection on batch point get.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t19141 (c_int int, primary key (c_int)) partition by hash ( c_int ) partitions 4")
	tk.MustInterDirc("insert into t19141 values (1), (2), (3), (4)")
	tk.MustQuery("select * from t19141 partition (p0)").Check(testkit.Rows("4"))
	tk.MustQuery("select * from t19141 partition (p0) where c_int = 1").Check(testkit.Rows())
	tk.MustInterDirc("uFIDelate t19141 partition (p0) set c_int = -c_int where c_int = 1") // BlockDual after partition selection.
	tk.MustQuery("select * from t19141 order by c_int").Check(testkit.Rows("1", "2", "3", "4"))

	// Bach point get
	tk.MustQuery("select * from t19141 partition (p0, p2) where c_int in (1,2,3)").Check(testkit.Rows("2"))
	tk.MustInterDirc("uFIDelate t19141 partition (p1) set c_int = -c_int where c_int in (2,3)") // No data changed
	tk.MustQuery("select * from t19141 order by c_int").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustInterDirc("delete from t19141 partition (p0) where c_int in (2,3)") // No data changed
	tk.MustQuery("select * from t19141 order by c_int").Check(testkit.Rows("1", "2", "3", "4"))
}
