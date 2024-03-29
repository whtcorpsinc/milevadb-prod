// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by aprettyPrintlicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded_test

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/israce"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

var _ = Suite(&testCausetNormalize{})

type testCausetNormalize struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri

	testData solitonutil.TestData
}

func (s *testCausetNormalize) SetUpSuite(c *C) {
	testleak.BeforeTest()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	s.dom = dom

	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "plan_normalized_suite")
	c.Assert(err, IsNil)
}

func (s *testCausetNormalize) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
}

func (s *testCausetNormalize) TestNormalizedCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1,t2")
	tk.MustInterDirc("create causet t1 (a int key,b int,c int, index (b));")
	tk.MustInterDirc("create causet t2 (a int key,b int,c int, index (b));")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		tk.Se.GetStochastikVars().CausetID = 0
		tk.MustInterDirc(tt)
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		p, ok := info.Causet.(embedded.Causet)
		c.Assert(ok, IsTrue)
		normalized, _ := embedded.NormalizeCauset(p)
		normalizedCauset, err := plancodec.DecodeNormalizedCauset(normalized)
		normalizedCausetRows := getCausetRows(normalizedCauset)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = normalizedCausetRows
		})
		compareStringSlice(c, normalizedCausetRows, output[i].Causet)
	}
}

func (s *testCausetNormalize) TestNormalizedCausetForDiffStore(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int, b int, c int, primary key(a))")
	tk.MustInterDirc("insert into t1 values(1,1,1), (2,2,2), (3,3,3)")

	tbl, err := s.dom.SchemaReplicant().BlockByName(perceptron.CIStr{O: "test", L: "test"}, perceptron.CIStr{O: "t1", L: "t1"})
	c.Assert(err, IsNil)
	// Set the reploged TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &perceptron.TiFlashReplicaInfo{Count: 1, Available: true}

	var input []string
	var output []struct {
		Digest string
		Causet []string
	}
	s.testData.GetTestCases(c, &input, &output)
	lastDigest := ""
	for i, tt := range input {
		tk.Se.GetStochastikVars().CausetID = 0
		tk.MustInterDirc(tt)
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		ep, ok := info.Causet.(*embedded.Explain)
		c.Assert(ok, IsTrue)
		normalized, digest := embedded.NormalizeCauset(ep.TargetCauset)
		normalizedCauset, err := plancodec.DecodeNormalizedCauset(normalized)
		normalizedCausetRows := getCausetRows(normalizedCauset)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].Digest = digest
			output[i].Causet = normalizedCausetRows
		})
		compareStringSlice(c, normalizedCausetRows, output[i].Causet)
		c.Assert(digest != lastDigest, IsTrue)
		lastDigest = digest
	}
}

func (s *testCausetNormalize) TestEncodeDecodeCauset(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1,t2")
	tk.MustInterDirc("create causet t1 (a int key,b int,c int, index (b));")
	tk.MustInterDirc("set milevadb_enable_collect_execution_info=1;")

	tk.Se.GetStochastikVars().CausetID = 0
	getCausetTree := func() string {
		info := tk.Se.ShowProcess()
		c.Assert(info, NotNil)
		p, ok := info.Causet.(embedded.Causet)
		c.Assert(ok, IsTrue)
		encodeStr := embedded.EncodeCauset(p)
		planTree, err := plancodec.DecodeCauset(encodeStr)
		c.Assert(err, IsNil)
		return planTree
	}
	tk.MustInterDirc("select max(a) from t1 where a>0;")
	planTree := getCausetTree()
	c.Assert(strings.Contains(planTree, "time"), IsTrue)
	c.Assert(strings.Contains(planTree, "loops"), IsTrue)

	tk.MustInterDirc("insert into t1 values (1,1,1);")
	planTree = getCausetTree()
	c.Assert(strings.Contains(planTree, "Insert"), IsTrue)
	c.Assert(strings.Contains(planTree, "time"), IsTrue)
	c.Assert(strings.Contains(planTree, "loops"), IsTrue)
}

func (s *testCausetNormalize) TestNormalizedDigest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1,t2, bmsql_order_line, bmsql_district,bmsql_stock")
	tk.MustInterDirc("create causet t1 (a int key,b int,c int, index (b));")
	tk.MustInterDirc("create causet t2 (a int key,b int,c int, index (b));")
	tk.MustInterDirc(`CREATE TABLE  bmsql_order_line  (
	   ol_w_id  int(11) NOT NULL,
	   ol_d_id  int(11) NOT NULL,
	   ol_o_id  int(11) NOT NULL,
	   ol_number  int(11) NOT NULL,
	   ol_i_id  int(11) NOT NULL,
	   ol_delivery_d  timestamp NULL DEFAULT NULL,
	   ol_amount  decimal(6,2) DEFAULT NULL,
	   ol_supply_w_id  int(11) DEFAULT NULL,
	   ol_quantity  int(11) DEFAULT NULL,
	   ol_dist_info  char(24) DEFAULT NULL,
	  PRIMARY KEY ( ol_w_id , ol_d_id , ol_o_id , ol_number )
	);`)
	tk.MustInterDirc(`CREATE TABLE  bmsql_district  (
	   d_w_id  int(11) NOT NULL,
	   d_id  int(11) NOT NULL,
	   d_ytd  decimal(12,2) DEFAULT NULL,
	   d_tax  decimal(4,4) DEFAULT NULL,
	   d_next_o_id  int(11) DEFAULT NULL,
	   d_name  varchar(10) DEFAULT NULL,
	   d_street_1  varchar(20) DEFAULT NULL,
	   d_street_2  varchar(20) DEFAULT NULL,
	   d_city  varchar(20) DEFAULT NULL,
	   d_state  char(2) DEFAULT NULL,
	   d_zip  char(9) DEFAULT NULL,
	  PRIMARY KEY ( d_w_id , d_id )
	);`)
	tk.MustInterDirc(`CREATE TABLE  bmsql_stock  (
	   s_w_id  int(11) NOT NULL,
	   s_i_id  int(11) NOT NULL,
	   s_quantity  int(11) DEFAULT NULL,
	   s_ytd  int(11) DEFAULT NULL,
	   s_order_cnt  int(11) DEFAULT NULL,
	   s_remote_cnt  int(11) DEFAULT NULL,
	   s_data  varchar(50) DEFAULT NULL,
	   s_dist_01  char(24) DEFAULT NULL,
	   s_dist_02  char(24) DEFAULT NULL,
	   s_dist_03  char(24) DEFAULT NULL,
	   s_dist_04  char(24) DEFAULT NULL,
	   s_dist_05  char(24) DEFAULT NULL,
	   s_dist_06  char(24) DEFAULT NULL,
	   s_dist_07  char(24) DEFAULT NULL,
	   s_dist_08  char(24) DEFAULT NULL,
	   s_dist_09  char(24) DEFAULT NULL,
	   s_dist_10  char(24) DEFAULT NULL,
	  PRIMARY KEY ( s_w_id , s_i_id )
	);`)
	normalizedDigestCases := []struct {
		sql1   string
		sql2   string
		isSame bool
	}{
		{
			sql1:   "select * from t1;",
			sql2:   "select * from t2;",
			isSame: false,
		},
		{ // test for blockReader and blockScan.
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a<2",
			isSame: true,
		},
		{
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a=2",
			isSame: false,
		},
		{ // test for point get.
			sql1:   "select * from t1 where a=3",
			sql2:   "select * from t1 where a=2",
			isSame: true,
		},
		{ // test for indexLookUp.
			sql1:   "select * from t1 use index(b) where b=3",
			sql2:   "select * from t1 use index(b) where b=1",
			isSame: true,
		},
		{ // test for indexReader.
			sql1:   "select a+1,b+2 from t1 use index(b) where b=3",
			sql2:   "select a+2,b+3 from t1 use index(b) where b=2",
			isSame: true,
		},
		{ // test for merge join.
			sql1:   "SELECT /*+ MilevaDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ MilevaDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>2;",
			isSame: true,
		},
		{ // test for indexLookUpJoin.
			sql1:   "SELECT /*+ MilevaDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ MilevaDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for hashJoin.
			sql1:   "SELECT /*+ MilevaDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ MilevaDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ MilevaDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ MilevaDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ MilevaDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ MilevaDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
		{ // test for apply.
			sql1:   "select * from t1 where t1.b > 0 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >1)",
			sql2:   "select * from t1 where t1.b > 1 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >0)",
			isSame: true,
		},
		{ // test for apply.
			sql1:   "select * from t1 where t1.b > 0 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >1)",
			sql2:   "select * from t1 where t1.b > 1 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null)",
			isSame: false,
		},
		{ // test for topN.
			sql1:   "SELECT * from t1 where a!=1 order by c limit 1",
			sql2:   "SELECT * from t1 where a!=2 order by c limit 2",
			isSame: true,
		},
		{ // test for union
			sql1:   "select count(1) as num,a from t1 where a=1 group by a union select count(1) as num,a from t1 where a=3 group by a;",
			sql2:   "select count(1) as num,a from t1 where a=2 group by a union select count(1) as num,a from t1 where a=4 group by a;",
			isSame: true,
		},
		{
			sql1: `SELECT  COUNT(*) AS low_stock
					FROM
					(
						SELECT  *
						FROM bmsql_stock
						WHERE s_w_id = 1
						AND s_quantity < 2
						AND s_i_id IN ( SELECT /*+ MilevaDB_INLJ(bmsql_order_line) */ ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 20 AND ol_o_id < d_next_o_id WHERE d_w_id = 1 AND d_id = 2 )
					) AS L;`,
			sql2: `SELECT  COUNT(*) AS low_stock
					FROM
					(
						SELECT  *
						FROM bmsql_stock
						WHERE s_w_id = 5
						AND s_quantity < 6
						AND s_i_id IN ( SELECT /*+ MilevaDB_INLJ(bmsql_order_line) */ ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 70 AND ol_o_id < d_next_o_id WHERE d_w_id = 5 AND d_id = 6 )
					) AS L;`,
			isSame: true,
		},
	}
	for _, testCase := range normalizedDigestCases {
		testNormalizeDigest(tk, c, testCase.sql1, testCase.sql2, testCase.isSame)
	}
}

func testNormalizeDigest(tk *testkit.TestKit, c *C, sql1, sql2 string, isSame bool) {
	tk.Se.GetStochastikVars().CausetID = 0
	tk.MustQuery(sql1)
	info := tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	physicalCauset, ok := info.Causet.(embedded.PhysicalCauset)
	c.Assert(ok, IsTrue)
	normalized1, digest1 := embedded.NormalizeCauset(physicalCauset)

	tk.Se.GetStochastikVars().CausetID = 0
	tk.MustQuery(sql2)
	info = tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	physicalCauset, ok = info.Causet.(embedded.PhysicalCauset)
	c.Assert(ok, IsTrue)
	normalized2, digest2 := embedded.NormalizeCauset(physicalCauset)
	comment := Commentf("sql1: %v, sql2: %v\n%v !=\n%v\n", sql1, sql2, normalized1, normalized2)
	if isSame {
		c.Assert(normalized1, Equals, normalized2, comment)
		c.Assert(digest1, Equals, digest2, comment)
	} else {
		c.Assert(normalized1 != normalized2, IsTrue, comment)
		c.Assert(digest1 != digest2, IsTrue, comment)
	}
}

func getCausetRows(planStr string) []string {
	planStr = strings.Replace(planStr, "\t", " ", -1)
	return strings.Split(planStr, "\n")
}

func compareStringSlice(c *C, ss1, ss2 []string) {
	c.Assert(len(ss1), Equals, len(ss2))
	for i, s := range ss1 {
		c.Assert(s, Equals, ss2[i])
	}
}

func (s *testCausetNormalize) TestNthCausetHint(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists tt")
	tk.MustInterDirc("create causet tt (a int,b int, index(a), index(b));")
	tk.MustInterDirc("insert into tt values (1, 1), (2, 2), (3, 4)")

	tk.MustInterDirc("explain select /*+nth_plan(4)*/ * from tt where a=1 and b=1;")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The parameter of nth_plan() is out of range."))

	// Test hints for nth_plan(x).
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int, c int, index(a), index(b), index(a,b))")
	tk.MustQuery("explain format='hint' select * from t where a=1 and b=1").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t` `a_2`)"))
	tk.MustQuery("explain format='hint' select /*+ nth_plan(1) */ * from t where a=1 and b=1").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t` ), nth_plan(1)"))
	tk.MustQuery("explain format='hint' select /*+ nth_plan(2) */ * from t where a=1 and b=1").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t` `a_2`), nth_plan(2)"))

	tk.MustInterDirc("explain format='hint' select /*+ nth_plan(3) */ * from t where a=1 and b=1")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The parameter of nth_plan() is out of range."))

	tk.MustInterDirc("explain format='hint' select /*+ nth_plan(500) */ * from t where a=1 and b=1")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The parameter of nth_plan() is out of range."))

	// Test warning for multiply hints.
	tk.MustQuery("explain format='hint' select /*+ nth_plan(1) nth_plan(2) */ * from t where a=1 and b=1").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t` `a_2`), nth_plan(1), nth_plan(2)"))
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN(2)",
		"Warning 1105 NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN(2)"))

	// Test the correctness of generated plans.
	tk.MustInterDirc("insert into t values (1,1,1)")
	tk.MustQuery("select  /*+ nth_plan(1) */ * from t where a=1 and b=1;").Check(testkit.Rows(
		"1 1 1"))
	tk.MustQuery("select  /*+ nth_plan(2) */ * from t where a=1 and b=1;").Check(testkit.Rows(
		"1 1 1"))
	tk.MustQuery("select  /*+ nth_plan(1) */ * from tt where a=1 and b=1;").Check(testkit.Rows(
		"1 1"))
	tk.MustQuery("select  /*+ nth_plan(2) */ * from tt where a=1 and b=1;").Check(testkit.Rows(
		"1 1"))
	tk.MustQuery("select  /*+ nth_plan(3) */ * from tt where a=1 and b=1;").Check(testkit.Rows(
		"1 1"))

	// Make sure nth_plan() doesn't affect separately executed subqueries by asserting there's only one warning.
	tk.MustInterDirc("select /*+ nth_plan(1000) */ count(1) from t where (select count(1) from t, tt) > 1;")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The parameter of nth_plan() is out of range."))
	tk.MustInterDirc("select /*+ nth_plan(1000) */ count(1) from t where exists (select count(1) from t, tt);")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The parameter of nth_plan() is out of range."))
}

func (s *testCausetNormalize) TestDecodeCausetPerformance(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a varchar(10) key,b int);")
	tk.MustInterDirc("set @@milevadb_slow_log_threshold=200000")

	// generate ALLEGROALLEGROSQL
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024*4))
	for i := 0; i < 50000; i++ {
		if i > 0 {
			buf.WriteString(" union ")
		}
		buf.WriteString(fmt.Sprintf("select count(1) as num,a from t where a='%v' group by a", i))
	}
	query := buf.String()
	tk.Se.GetStochastikVars().CausetID = 0
	tk.MustInterDirc(query)
	info := tk.Se.ShowProcess()
	c.Assert(info, NotNil)
	p, ok := info.Causet.(embedded.PhysicalCauset)
	c.Assert(ok, IsTrue)
	// TODO: optimize the encode plan performance when encode plan with runtimeStats
	tk.Se.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl = nil
	encodedCausetStr := embedded.EncodeCauset(p)
	start := time.Now()
	_, err := plancodec.DecodeCauset(encodedCausetStr)
	c.Assert(err, IsNil)
	c.Assert(time.Since(start).Seconds(), Less, 3.0)
}
