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

package bindinfo_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	dto "github.com/prometheus/client_perceptron/go"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/bindinfo"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/stmtsummary"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	cluster     cluster.Cluster
	causetstore ekv.CausetStorage
	petri       *petri.Petri
	*BerolinaSQL.BerolinaSQL
}

var mockEinsteinDB = flag.Bool("mockEinsteinDB", true, "use mock einsteindb causetstore in bind test")

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.BerolinaSQL = BerolinaSQL.New()
	flag.Lookup("mockEinsteinDB")
	useMockEinsteinDB := *mockEinsteinDB
	if useMockEinsteinDB {
		causetstore, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.causetstore = causetstore
		stochastik.SetSchemaLease(0)
		stochastik.DisableStats4Test()
	}
	bindinfo.Lease = 0
	d, err := stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	d.SetStatsUFIDelating(true)
	s.petri = d
}

func (s *testSuite) TearDownSuite(c *C) {
	s.petri.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
}

func (s *testSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		tk.MustInterDirc(fmt.Sprintf("drop causet %v", blockName))
	}
}

func (s *testSuite) cleanBindingEnv(tk *testkit.TestKit) {
	tk.MustInterDirc("truncate causet allegrosql.bind_info")
	s.petri.BindHandle().Clear()
}

func (s *testSuite) TestBindParse(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(i int)")
	tk.MustInterDirc("create index index_t on t(i)")

	originALLEGROSQL := "select * from t"
	bindALLEGROSQL := "select * from t use index(index_t)"
	defaultDb := "test"
	status := "using"
	charset := "utf8mb4"
	collation := "utf8mb4_bin"
	source := bindinfo.Manual
	allegrosql := fmt.Sprintf(`INSERT INTO allegrosql.bind_info(original_sql,bind_sql,default_db,status,create_time,uFIDelate_time,charset,collation,source) VALUES ('%s', '%s', '%s', '%s', NOW(), NOW(),'%s', '%s', '%s')`,
		originALLEGROSQL, bindALLEGROSQL, defaultDb, status, charset, collation, source)
	tk.MustInterDirc(allegrosql)
	bindHandle := bindinfo.NewBindHandle(tk.Se)
	err := bindHandle.UFIDelate(true)
	c.Check(err, IsNil)
	c.Check(bindHandle.Size(), Equals, 1)

	allegrosql, hash := BerolinaSQL.NormalizeDigest("select * from t")
	bindData := bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t")
	bind := bindData.Bindings[0]
	c.Check(bind.BindALLEGROSQL, Equals, "select * from t use index(index_t)")
	c.Check(bindData.EDB, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, Equals, "utf8mb4")
	c.Check(bind.DefCauslation, Equals, "utf8mb4_bin")
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UFIDelateTime, NotNil)
	dur, err := bind.SinceUFIDelateTime()
	c.Assert(err, IsNil)
	c.Assert(int64(dur), GreaterEqual, int64(0))

	// Test fields with quotes or slashes.
	allegrosql = `CREATE GLOBAL BINDING FOR  select * from t where i BETWEEN "a" and "b" USING select * from t use index(index_t) where i BETWEEN "a\nb\rc\td\0e" and 'x'`
	tk.MustInterDirc(allegrosql)
	tk.MustInterDirc(`DROP global binding for select * from t use index(idx) where i BETWEEN "a\nb\rc\td\0e" and "x"`)
}

func (s *testSuite) TestGlobalBinding(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(i int, s varchar(20))")
	tk.MustInterDirc("create causet t1(i int, s varchar(20))")
	tk.MustInterDirc("create index index_t on t(i,s)")

	metrics.BindTotalGauge.Reset()
	metrics.BindMemoryUsage.Reset()

	_, err := tk.InterDirc("create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))

	_, err = tk.InterDirc("create global binding for select * from t where i>99 using select * from t use index(index_t) where i>99")
	c.Assert(err, IsNil)

	pb := &dto.Metric{}
	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(1))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(97))

	allegrosql, hash := BerolinaSQL.NormalizeDigest("select * from t where i          >      30.0")

	bindData := s.petri.BindHandle().GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where i > ?")
	bind := bindData.Bindings[0]
	c.Check(bind.BindALLEGROSQL, Equals, "select * from t use index(index_t) where i>99")
	c.Check(bindData.EDB, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, NotNil)
	c.Check(bind.DefCauslation, NotNil)
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UFIDelateTime, NotNil)

	rs, err := tk.InterDirc("show global bindings")
	c.Assert(err, IsNil)
	chk := rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	c.Check(err, IsNil)
	c.Check(chk.NumRows(), Equals, 1)
	event := chk.GetRow(0)
	c.Check(event.GetString(0), Equals, "select * from t where i > ?")
	c.Check(event.GetString(1), Equals, "select * from t use index(index_t) where i>99")
	c.Check(event.GetString(2), Equals, "test")
	c.Check(event.GetString(3), Equals, "using")
	c.Check(event.GetTime(4), NotNil)
	c.Check(event.GetTime(5), NotNil)
	c.Check(event.GetString(6), NotNil)
	c.Check(event.GetString(7), NotNil)

	bindHandle := bindinfo.NewBindHandle(tk.Se)
	err = bindHandle.UFIDelate(true)
	c.Check(err, IsNil)
	c.Check(bindHandle.Size(), Equals, 1)

	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where i > ?")
	bind = bindData.Bindings[0]
	c.Check(bind.BindALLEGROSQL, Equals, "select * from t use index(index_t) where i>99")
	c.Check(bindData.EDB, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, NotNil)
	c.Check(bind.DefCauslation, NotNil)
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UFIDelateTime, NotNil)

	_, err = tk.InterDirc("DROP global binding for select * from t where i>100")
	c.Check(err, IsNil)
	bindData = s.petri.BindHandle().GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, IsNil)

	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	// From newly created global bind handle.
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(97))

	bindHandle = bindinfo.NewBindHandle(tk.Se)
	err = bindHandle.UFIDelate(true)
	c.Check(err, IsNil)
	c.Check(bindHandle.Size(), Equals, 0)

	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, IsNil)

	rs, err = tk.InterDirc("show global bindings")
	c.Assert(err, IsNil)
	chk = rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	c.Check(err, IsNil)
	c.Check(chk.NumRows(), Equals, 0)

	_, err = tk.InterDirc("delete from allegrosql.bind_info")
	c.Assert(err, IsNil)

	_, err = tk.InterDirc("create global binding for select * from t using select * from t1 use index for join(index_t)")
	c.Assert(err, NotNil, Commentf("err %v", err))
}

func (s *testSuite) TestStochastikBinding(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(i int, s varchar(20))")
	tk.MustInterDirc("create causet t1(i int, s varchar(20))")
	tk.MustInterDirc("create index index_t on t(i,s)")

	metrics.BindTotalGauge.Reset()
	metrics.BindMemoryUsage.Reset()

	_, err := tk.InterDirc("create stochastik binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))

	_, err = tk.InterDirc("create stochastik binding for select * from t where i>99 using select * from t use index(index_t) where i>99")
	c.Assert(err, IsNil)

	pb := &dto.Metric{}
	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeStochastik, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(1))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeStochastik, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(97))

	handle := tk.Se.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
	bindData := handle.GetBindRecord("select * from t where i > ?", "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where i > ?")
	bind := bindData.Bindings[0]
	c.Check(bind.BindALLEGROSQL, Equals, "select * from t use index(index_t) where i>99")
	c.Check(bindData.EDB, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, NotNil)
	c.Check(bind.DefCauslation, NotNil)
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UFIDelateTime, NotNil)

	rs, err := tk.InterDirc("show global bindings")
	c.Assert(err, IsNil)
	chk := rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	c.Check(err, IsNil)
	c.Check(chk.NumRows(), Equals, 0)

	rs, err = tk.InterDirc("show stochastik bindings")
	c.Assert(err, IsNil)
	chk = rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	c.Check(err, IsNil)
	c.Check(chk.NumRows(), Equals, 1)
	event := chk.GetRow(0)
	c.Check(event.GetString(0), Equals, "select * from t where i > ?")
	c.Check(event.GetString(1), Equals, "select * from t use index(index_t) where i>99")
	c.Check(event.GetString(2), Equals, "test")
	c.Check(event.GetString(3), Equals, "using")
	c.Check(event.GetTime(4), NotNil)
	c.Check(event.GetTime(5), NotNil)
	c.Check(event.GetString(6), NotNil)
	c.Check(event.GetString(7), NotNil)

	_, err = tk.InterDirc("drop stochastik binding for select * from t where i>99")
	c.Assert(err, IsNil)
	bindData = handle.GetBindRecord("select * from t where i > ?", "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where i > ?")
	c.Check(len(bindData.Bindings), Equals, 0)

	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeStochastik, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeStochastik, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
}

func (s *testSuite) TestGlobalAndStochastikBindingBothExist(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1(id int)")
	tk.MustInterDirc("create causet t2(id int)")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	c.Assert(tk.HasCauset("SELECT  /*+ MilevaDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustInterDirc("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ MilevaDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	// Test bindingUsage, which indicates how many times the binding is used.
	metrics.BindUsageCounter.Reset()
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	pb := &dto.Metric{}
	metrics.BindUsageCounter.WithLabelValues(metrics.ScopeGlobal).Write(pb)
	c.Assert(pb.GetCounter().GetValue(), Equals, float64(1))

	// Test 'milevadb_use_plan_baselines'
	tk.MustInterDirc("set @@milevadb_use_plan_baselines = 0")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	tk.MustInterDirc("set @@milevadb_use_plan_baselines = 1")

	// Test 'drop global binding'
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	tk.MustInterDirc("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)

	// Test the case when global and stochastik binding both exist
	// PART1 : stochastik binding should totally cover global binding
	// use merge join as stochastik binding here since the optimizer will choose hash join for this stmt in default
	tk.MustInterDirc("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ MilevaDB_HJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	tk.MustInterDirc("create binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ MilevaDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	tk.MustInterDirc("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	// PART2 : the dropped stochastik binding should continue to causet the effect of global binding
	tk.MustInterDirc("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ MilevaDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	tk.MustInterDirc("drop binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	tk.MustInterDirc("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
}

func (s *testSuite) TestExplain(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1(id int)")
	tk.MustInterDirc("create causet t2(id int)")

	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	c.Assert(tk.HasCauset("SELECT  /*+ MilevaDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustInterDirc("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ MilevaDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	c.Assert(tk.HasCauset("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustInterDirc("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
}

// TestBindingSymbolList tests allegrosql with "?, ?, ?, ?", fixes #13871
func (s *testSuite) TestBindingSymbolList(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustInterDirc("insert into t value(1, 1);")

	// before binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:ia")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"), IsTrue)

	tk.MustInterDirc(`create global binding for select a, b from t where a = 1 limit 0, 1 using select a, b from t use index (ib) where a = 1 limit 0, 1`)

	// after binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:ib")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ib(b)"), IsTrue)

	// Normalize
	allegrosql, hash := BerolinaSQL.NormalizeDigest("select a, b from t where a = 1 limit 0, 1")

	bindData := s.petri.BindHandle().GetBindRecord(hash, allegrosql, "test")
	c.Assert(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select a , b from t where a = ? limit ...")
	bind := bindData.Bindings[0]
	c.Check(bind.BindALLEGROSQL, Equals, "select a, b from t use index (ib) where a = 1 limit 0, 1")
	c.Check(bindData.EDB, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, NotNil)
	c.Check(bind.DefCauslation, NotNil)
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UFIDelateTime, NotNil)
}

func (s *testSuite) TestBestCausetInBaselines(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustInterDirc("insert into t value(1, 1);")

	// before binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:ia")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"), IsTrue)

	tk.MustQuery("select a, b from t where b = 3 limit 1, 100")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:ib")
	c.Assert(tk.MustUseIndex("select a, b from t where b = 3 limit 1, 100", "ib(b)"), IsTrue)

	tk.MustInterDirc(`create global binding for select a, b from t where a = 1 limit 0, 1 using select /*+ use_index(@sel_1 test.t, ia) */ a, b from t where a = 1 limit 0, 1`)
	tk.MustInterDirc(`create global binding for select a, b from t where b = 1 limit 0, 1 using select /*+ use_index(@sel_1 test.t, ib) */ a, b from t where b = 1 limit 0, 1`)

	allegrosql, hash := BerolinaSQL.NormalizeDigest("select a, b from t where a = 1 limit 0, 1")
	bindData := s.petri.BindHandle().GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select a , b from t where a = ? limit ...")
	bind := bindData.Bindings[0]
	c.Check(bind.BindALLEGROSQL, Equals, "select /*+ use_index(@sel_1 test.t, ia) */ a, b from t where a = 1 limit 0, 1")
	c.Check(bindData.EDB, Equals, "test")
	c.Check(bind.Status, Equals, "using")

	tk.MustQuery("select a, b from t where a = 3 limit 1, 10")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:ia")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"), IsTrue)

	tk.MustQuery("select a, b from t where b = 3 limit 1, 100")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:ib")
	c.Assert(tk.MustUseIndex("select a, b from t where b = 3 limit 1, 100", "ib(b)"), IsTrue)
}

func (s *testSuite) TestErrorBind(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustGetErrMsg("create global binding for select * from t using select * from t", "[schemaReplicant:1146]Block 'test.t' doesn't exist")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t(i int, s varchar(20))")
	tk.MustInterDirc("create causet t1(i int, s varchar(20))")
	tk.MustInterDirc("create index index_t on t(i,s)")

	_, err := tk.InterDirc("create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))

	allegrosql, hash := BerolinaSQL.NormalizeDigest("select * from t where i > ?")
	bindData := s.petri.BindHandle().GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where i > ?")
	bind := bindData.Bindings[0]
	c.Check(bind.BindALLEGROSQL, Equals, "select * from t use index(index_t) where i>100")
	c.Check(bindData.EDB, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, NotNil)
	c.Check(bind.DefCauslation, NotNil)
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UFIDelateTime, NotNil)

	tk.MustInterDirc("drop index index_t on t")
	_, err = tk.InterDirc("select * from t where i > 10")
	c.Check(err, IsNil)

	s.petri.BindHandle().DropInvalidBindRecord()

	rs, err := tk.InterDirc("show global bindings")
	c.Assert(err, IsNil)
	chk := rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	c.Check(err, IsNil)
	c.Check(chk.NumRows(), Equals, 0)
}

func (s *testSuite) TestPreparedStmt(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc(`prepare stmt1 from 'select * from t'`)
	tk.MustInterDirc("execute stmt1")
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustInterDirc("create binding for select * from t using select * from t use index(idx)")
	tk.MustInterDirc("execute stmt1")
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx")

	tk.MustInterDirc("drop binding for select * from t")
	tk.MustInterDirc("execute stmt1")
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 0)
}

func (s *testSuite) TestCaptureCausetBaseline(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustInterDirc(" set @@milevadb_capture_plan_baselines = on")
	defer func() {
		tk.MustInterDirc(" set @@milevadb_capture_plan_baselines = off")
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, t1")
	tk.MustInterDirc("create causet t(a int)")
	s.petri.BindHandle().CaptureBaselines()
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustInterDirc("select count(*) from t where a > 10")
	tk.MustInterDirc("select count(*) from t where a > 10")
	tk.MustInterDirc("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)

	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustInterDirc("select * from t where a > 10")
	tk.MustInterDirc("select * from t where a > 10")
	tk.MustInterDirc("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from t where a > ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `t` WHERE `a`>10")
}

func (s *testSuite) TestCaptureBaselinesDefaultDB(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustInterDirc(" set @@milevadb_capture_plan_baselines = on")
	defer func() {
		tk.MustInterDirc(" set @@milevadb_capture_plan_baselines = off")
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop database if exists spm")
	tk.MustInterDirc("create database spm")
	tk.MustInterDirc("create causet spm.t(a int, index idx_a(a))")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustInterDirc("select * from spm.t ignore index(idx_a) where a > 10")
	tk.MustInterDirc("select * from spm.t ignore index(idx_a) where a > 10")
	tk.MustInterDirc("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	// Default EDB should be "" when all columns have explicit database name.
	c.Assert(rows[0][2], Equals, "")
	c.Assert(rows[0][3], Equals, "using")
	tk.MustInterDirc("use spm")
	tk.MustInterDirc("select * from spm.t where a > 10")
	// Should use TableScan because of the "ignore index" binding.
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 0)
}

func (s *testSuite) TestDropSingleBindings(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int, index idx_a(a), index idx_b(b))")

	// Test drop stochastik bindings.
	tk.MustInterDirc("create binding for select * from t using select * from t use index(idx_a)")
	tk.MustInterDirc("create binding for select * from t using select * from t use index(idx_b)")
	rows := tk.MustQuery("show bindings").Rows()
	// The size of bindings is equal to one. Because for one normalized allegrosql,
	// the `create binding` clears all the origin bindings.
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "select * from t use index(idx_b)")
	tk.MustInterDirc("drop binding for select * from t using select * from t use index(idx_a)")
	rows = tk.MustQuery("show bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "select * from t use index(idx_b)")
	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("drop binding for select * from t using select * from t use index(idx_b)")
	rows = tk.MustQuery("show bindings").Rows()
	c.Assert(len(rows), Equals, 0)

	tk.MustInterDirc("create causet t(a int, b int, c int, index idx_a(a), index idx_b(b))")
	// Test drop global bindings.
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx_a)")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx_b)")
	rows = tk.MustQuery("show global bindings").Rows()
	// The size of bindings is equal to one. Because for one normalized allegrosql,
	// the `create binding` clears all the origin bindings.
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "select * from t use index(idx_b)")
	tk.MustInterDirc("drop global binding for select * from t using select * from t use index(idx_a)")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "select * from t use index(idx_b)")
	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("drop global binding for select * from t using select * from t use index(idx_b)")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testSuite) TestAddEvolveTasks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")
	tk.MustInterDirc("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustInterDirc("analyze causet t")
	tk.MustInterDirc("create global binding for select * from t where a >= 1 and b >= 1 and c = 0 using select * from t use index(idx_a) where a >= 1 and b >= 1 and c = 0")
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines=1")
	// It cannot choose causet path although it has lowest cost.
	tk.MustQuery("select * from t where a >= 4 and b >= 1 and c = 0")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")
	tk.MustInterDirc("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a`>=4 AND `b`>=1 AND `c`=0")
	c.Assert(rows[1][3], Equals, "pending verify")
	tk.MustInterDirc("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a`>=4 AND `b`>=1 AND `c`=0")
	status := rows[1][3].(string)
	c.Assert(status == "using" || status == "rejected", IsTrue)
}

func (s *testSuite) TestRuntimeHintsInEvolveTasks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines=1")
	tk.MustInterDirc("create causet t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")

	// these runtime hints which don't be contained by the original binding should be ignored
	tk.MustInterDirc("create global binding for select * from t where a >= 1 and b >= 1 and c = 0 using select * from t use index(idx_a) where a >= 1 and b >= 1 and c = 0")
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(5000) */* from t where a >= 4 and b >= 1 and c = 0")
	tk.MustInterDirc("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`)*/ * FROM `test`.`t` WHERE `a`>=4 AND `b`>=1 AND `c`=0") // MAX_EXECUTION_TIME is ignored

	s.cleanBindingEnv(tk)
	tk.MustInterDirc("create global binding for select * from t where a >= 1 and b >= 1 and c = 0 using select /*+ MAX_EXECUTION_TIME(5000) */* from t use index(idx_a) where a >= 1 and b >= 1 and c = 0")
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(5000) */* from t where a >= 4 and b >= 1 and c = 0")
	tk.MustInterDirc("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`), max_execution_time(5000)*/ * FROM `test`.`t` WHERE `a`>=4 AND `b`>=1 AND `c`=0")
}

func (s *testSuite) TestBindingCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx);")
	tk.MustInterDirc("create database tmp")
	tk.MustInterDirc("use tmp")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx);")

	c.Assert(s.petri.BindHandle().UFIDelate(false), IsNil)
	c.Assert(s.petri.BindHandle().UFIDelate(false), IsNil)
	res := tk.MustQuery("show global bindings")
	c.Assert(len(res.Rows()), Equals, 2)

	tk.MustInterDirc("drop global binding for select * from t;")
	c.Assert(s.petri.BindHandle().UFIDelate(false), IsNil)
	c.Assert(len(s.petri.BindHandle().GetAllBindRecord()), Equals, 1)
}

func (s *testSuite) TestDefaultStochastikVars(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustQuery(`show variables like "%baselines%"`).Sort().Check(testkit.Rows(
		"milevadb_capture_plan_baselines off",
		"milevadb_evolve_plan_baselines off",
		"milevadb_use_plan_baselines on"))
	tk.MustQuery(`show global variables like "%baselines%"`).Sort().Check(testkit.Rows(
		"milevadb_capture_plan_baselines off",
		"milevadb_evolve_plan_baselines off",
		"milevadb_use_plan_baselines on"))
}

func (s *testSuite) TestCaptureBaselinesScope(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk1)
	tk1.MustQuery(`show stochastik variables like "milevadb_capture_plan_baselines"`).Check(testkit.Rows(
		"milevadb_capture_plan_baselines off",
	))
	tk1.MustQuery(`show global variables like "milevadb_capture_plan_baselines"`).Check(testkit.Rows(
		"milevadb_capture_plan_baselines off",
	))
	tk1.MustQuery(`select @@stochastik.milevadb_capture_plan_baselines`).Check(testkit.Rows(
		"off",
	))
	tk1.MustQuery(`select @@global.milevadb_capture_plan_baselines`).Check(testkit.Rows(
		"off",
	))

	tk1.MustInterDirc("set @@stochastik.milevadb_capture_plan_baselines = on")
	defer func() {
		tk1.MustInterDirc(" set @@stochastik.milevadb_capture_plan_baselines = off")
	}()
	tk1.MustQuery(`show stochastik variables like "milevadb_capture_plan_baselines"`).Check(testkit.Rows(
		"milevadb_capture_plan_baselines on",
	))
	tk1.MustQuery(`show global variables like "milevadb_capture_plan_baselines"`).Check(testkit.Rows(
		"milevadb_capture_plan_baselines off",
	))
	tk1.MustQuery(`select @@stochastik.milevadb_capture_plan_baselines`).Check(testkit.Rows(
		"on",
	))
	tk1.MustQuery(`select @@global.milevadb_capture_plan_baselines`).Check(testkit.Rows(
		"off",
	))
	tk2.MustQuery(`show stochastik variables like "milevadb_capture_plan_baselines"`).Check(testkit.Rows(
		"milevadb_capture_plan_baselines on",
	))
	tk2.MustQuery(`show global variables like "milevadb_capture_plan_baselines"`).Check(testkit.Rows(
		"milevadb_capture_plan_baselines off",
	))
	tk2.MustQuery(`select @@stochastik.milevadb_capture_plan_baselines`).Check(testkit.Rows(
		"on",
	))
	tk2.MustQuery(`select @@global.milevadb_capture_plan_baselines`).Check(testkit.Rows(
		"off",
	))
}

func (s *testSuite) TestDuplicateBindings(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx);")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	createTime := rows[0][4]
	time.Sleep(1000000)
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(createTime == rows[0][4], Equals, false)

	tk.MustInterDirc("create stochastik binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show stochastik bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	createTime = rows[0][4]
	time.Sleep(1000000)
	tk.MustInterDirc("create stochastik binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show stochastik bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(createTime == rows[0][4], Equals, false)
}

func (s *testSuite) TestStmtHints(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from t using select /*+ MAX_EXECUTION_TIME(100), MEMORY_QUOTA(1 GB) */ * from t use index(idx)")
	tk.MustQuery("select * from t")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemQuotaQuery, Equals, int64(1073741824))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MaxInterDircutionTime, Equals, uint64(100))
	tk.MustQuery("select a, b from t")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemQuotaQuery, Equals, int64(0))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MaxInterDircutionTime, Equals, uint64(0))
}

func (s *testSuite) TestReloadBindings(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx)")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	rows = tk.MustQuery("select * from allegrosql.bind_info").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustInterDirc("truncate causet allegrosql.bind_info")
	c.Assert(s.petri.BindHandle().UFIDelate(false), IsNil)
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(s.petri.BindHandle().UFIDelate(true), IsNil)
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustInterDirc("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testSuite) TestDefaultDB(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from test.t using select * from test.t use index(idx)")
	tk.MustInterDirc("use allegrosql")
	tk.MustQuery("select * from test.t")
	// Even in another database, we could still use the bindings.
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx")
	tk.MustInterDirc("drop global binding for select * from test.t")
	tk.MustQuery("show global bindings").Check(testkit.Rows())

	tk.MustInterDirc("use test")
	tk.MustInterDirc("create stochastik binding for select * from test.t using select * from test.t use index(idx)")
	tk.MustInterDirc("use allegrosql")
	tk.MustQuery("select * from test.t")
	// Even in another database, we could still use the bindings.
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx")
	tk.MustInterDirc("drop stochastik binding for select * from test.t")
	tk.MustQuery("show stochastik bindings").Check(testkit.Rows())
}

func (s *testSuite) TestEvolveInvalidBindings(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx_a(a))")
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select /*+ USE_INDEX(t) */ * from t where a > 10")
	// Manufacture a rejected binding by reploging allegrosql.bind_info.
	tk.MustInterDirc("insert into allegrosql.bind_info values('select * from t where a > ?', 'select /*+ USE_INDEX(t,idx_a) */ * from t where a > 10', 'test', 'rejected', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustQuery("select bind_sql, status from allegrosql.bind_info").Sort().Check(testkit.Rows(
		"select /*+ USE_INDEX(t) */ * from t where a > 10 using",
		"select /*+ USE_INDEX(t,idx_a) */ * from t where a > 10 rejected",
	))
	// Reload cache from allegrosql.bind_info.
	s.petri.BindHandle().Clear()
	c.Assert(s.petri.BindHandle().UFIDelate(true), IsNil)

	tk.MustInterDirc("alter causet t drop index idx_a")
	tk.MustInterDirc("admin evolve bindings")
	c.Assert(s.petri.BindHandle().UFIDelate(false), IsNil)
	rows := tk.MustQuery("show global bindings").Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// Make sure this "using" binding is not overrided.
	c.Assert(rows[0][1], Equals, "select /*+ USE_INDEX(t) */ * from t where a > 10")
	status := rows[0][3].(string)
	c.Assert(status == "using", IsTrue)
	c.Assert(rows[1][1], Equals, "select /*+ USE_INDEX(t,idx_a) */ * from t where a > 10")
	status = rows[1][3].(string)
	c.Assert(status == "using" || status == "rejected", IsTrue)
}

func (s *testSuite) TestOutdatedSchemaReplicant(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx)")
	c.Assert(s.petri.BindHandle().UFIDelate(false), IsNil)
	tk.MustInterDirc("truncate causet allegrosql.bind_info")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx)")
}

func (s *testSuite) TestPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx(a))")
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx)")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustInterDirc("create user test@'%'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "test", Hostname: "%"}, nil, nil), IsTrue)
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testSuite) TestHintsSetEvolveTask(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, index idx_a(a))")
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select * from t ignore index(idx_a) where a > 10")
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines=1")
	tk.MustQuery("select * from t use index(idx_a) where a > 0")
	bindHandle := s.petri.BindHandle()
	bindHandle.SaveEvolveTasksToStore()
	// Verify the added Binding for evolution contains valid ID and Hint, otherwise, panic may happen.
	allegrosql, hash := BerolinaSQL.NormalizeDigest("select * from t where a > ?")
	bindData := bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 2)
	bind := bindData.Bindings[1]
	c.Assert(bind.Status, Equals, bindinfo.PendingVerify)
	c.Assert(bind.ID, Not(Equals), "")
	c.Assert(bind.Hint, NotNil)
}

func (s *testSuite) TestHintsSetID(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, index idx_a(a))")
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select /*+ use_index(test.t, idx_a) */ * from t where a > 10")
	bindHandle := s.petri.BindHandle()
	// Verify the added Binding contains ID with restored query causet.
	allegrosql, hash := BerolinaSQL.NormalizeDigest("select * from t where a > ?")
	bindData := bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind := bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select /*+ use_index(t, idx_a) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select /*+ use_index(@sel_1 t, idx_a) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select /*+ use_index(@qb1 t, idx_a) qb_name(qb1) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select /*+ use_index(T, IDX_A) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	err := tk.InterDircToErr("create global binding for select * from t using select /*+ non_exist_hint() */ * from t")
	c.Assert(terror.ErrorEqual(err, BerolinaSQL.ErrWarnOptimizerHintParseError), IsTrue)
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "")
}

func (s *testSuite) TestCaptureCausetBaselineIgnoreTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, key(a), key(b))")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustInterDirc("select * from t")
	tk.MustInterDirc("select * from t")
	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	// Here the plan is the TiFlash plan.
	rows := tk.MustQuery("explain select * from t").Rows()
	c.Assert(fmt.Sprintf("%v", rows[len(rows)-1][2]), Equals, "cop[tiflash]")

	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustInterDirc("admin capture bindings")
	// Don't have the TiFlash plan even we have TiFlash replica.
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from t")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `t`")
}

func (s *testSuite) TestNotEvolveCausetForReadStorageHint(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustInterDirc("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)")
	tk.MustInterDirc("analyze causet t")
	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	// Make sure the best plan of the ALLEGROALLEGROSQL is use EinsteinDB index.
	tk.MustInterDirc("set @@stochastik.milevadb_interlock_concurrency = 4;")
	rows := tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(fmt.Sprintf("%v", rows[len(rows)-1][2]), Equals, "cop[einsteindb]")

	tk.MustInterDirc("create global binding for select * from t where a >= 1 and b >= 1 using select /*+ read_from_storage(tiflash[t]) */ * from t where a >= 1 and b >= 1")
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines=1")

	// Even if index of EinsteinDB has lower cost, it chooses TiFlash.
	rows = tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(fmt.Sprintf("%v", rows[len(rows)-1][2]), Equals, "cop[tiflash]")

	tk.MustInterDirc("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	// None evolve task, because of the origin binding is a read_from_storage binding.
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "select /*+ read_from_storage(tiflash[t]) */ * from t where a >= 1 and b >= 1")
	c.Assert(rows[0][3], Equals, "using")
}

func (s *testSuite) TestBindingWithIsolationRead(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustInterDirc("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)")
	tk.MustInterDirc("analyze causet t")
	// Create virtual tiflash replica info.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustInterDirc("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_a) where a >= 1 and b >= 1")
	tk.MustInterDirc("set @@milevadb_use_plan_baselines = 1")
	rows := tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[einsteindb]")
	// Even if we build a binding use index for ALLEGROALLEGROSQL, but after we set the isolation read for TiFlash, it choose TiFlash instead of index of EinsteinDB.
	tk.MustInterDirc("set @@milevadb_isolation_read_engines = \"tiflash\"")
	rows = tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tiflash]")
}

func (s *testSuite) TestReCreateBindAfterEvolveCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")
	tk.MustInterDirc("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustInterDirc("analyze causet t")
	tk.MustInterDirc("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_a) where a >= 1 and b >= 1")
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines=1")

	// It cannot choose causet path although it has lowest cost.
	tk.MustQuery("select * from t where a >= 0 and b >= 0")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")

	tk.MustInterDirc("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[1][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a`>=0 AND `b`>=0")
	c.Assert(rows[1][3], Equals, "pending verify")

	tk.MustInterDirc("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_b) where a >= 1 and b >= 1")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustQuery("select * from t where a >= 4 and b >= 1")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx_b")
}

func (s *testSuite) TestInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, unique idx_a(a), index idx_b(b) invisible)")
	tk.MustGetErrMsg(
		"create global binding for select * from t using select * from t use index(idx_b) ",
		"[causet:1176]Key 'idx_b' doesn't exist in causet 't'")

	// Create bind using index
	tk.MustInterDirc("create global binding for select * from t using select * from t use index(idx_a) ")

	tk.MustQuery("select * from t")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")
	c.Assert(tk.MustUseIndex("select * from t", "idx_a(a)"), IsTrue)

	tk.MustInterDirc(`prepare stmt1 from 'select * from t'`)
	tk.MustInterDirc("execute stmt1")
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")

	// And then make this index invisible
	tk.MustInterDirc("alter causet t alter index idx_a invisible")
	tk.MustQuery("select * from t")
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustInterDirc("execute stmt1")
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustInterDirc("drop binding for select * from t")
}

func (s *testSuite) TestbindingSource(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.cleanBindingEnv(tk)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, index idx_a(a))")

	// Test Source for ALLEGROALLEGROSQL created allegrosql
	tk.MustInterDirc("create global binding for select * from t where a > 10 using select * from t ignore index(idx_a) where a > 10")
	bindHandle := s.petri.BindHandle()
	allegrosql, hash := BerolinaSQL.NormalizeDigest("select * from t where a > ?")
	bindData := bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind := bindData.Bindings[0]
	c.Assert(bind.Source, Equals, bindinfo.Manual)

	// Test Source for evolved allegrosql
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines=1")
	tk.MustQuery("select * from t where a > 10")
	bindHandle.SaveEvolveTasksToStore()
	allegrosql, hash = BerolinaSQL.NormalizeDigest("select * from t where a > ?")
	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a > ?")
	c.Assert(len(bindData.Bindings), Equals, 2)
	bind = bindData.Bindings[1]
	c.Assert(bind.Source, Equals, bindinfo.Evolve)
	tk.MustInterDirc("set @@milevadb_evolve_plan_baselines=0")

	// Test Source for captured sqls
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustInterDirc("set @@milevadb_capture_plan_baselines = on")
	defer func() {
		tk.MustInterDirc("set @@milevadb_capture_plan_baselines = off")
	}()
	tk.MustInterDirc("use test")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustInterDirc("select * from t ignore index(idx_a) where a < 10")
	tk.MustInterDirc("select * from t ignore index(idx_a) where a < 10")
	tk.MustInterDirc("admin capture bindings")
	bindHandle.CaptureBaselines()
	allegrosql, hash = BerolinaSQL.NormalizeDigest("select * from t where a < ?")
	bindData = bindHandle.GetBindRecord(hash, allegrosql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalALLEGROSQL, Equals, "select * from t where a < ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.Source, Equals, bindinfo.Capture)
}
