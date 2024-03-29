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
	"crypto/tls"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

func (s *testSuite1) TestPreparedNameResolver(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int, KEY id (id))")
	tk.MustInterDirc("prepare stmt from 'select * from t limit ? offset ?'")
	_, err := tk.InterDirc("prepare stmt from 'select b from t'")
	c.Assert(err.Error(), Equals, "[causet:1054]Unknown defCausumn 'b' in 'field list'")

	_, err = tk.InterDirc("prepare stmt from '(select * FROM t) union all (select * FROM t) order by a limit ?'")
	c.Assert(err.Error(), Equals, "[causet:1054]Unknown defCausumn 'a' in 'order clause'")
}

// a 'create causet' DBS memex should be accepted if it has no parameters.
func (s *testSuite1) TestPreparedDBS(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("prepare stmt from 'create causet t (id int, KEY id (id))'")
}

func (s *testSuite1) TestIgnoreCausetCache(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")

	tk.MustInterDirc("create causet t (id int primary key, num int)")
	tk.MustInterDirc("insert into t values (1, 1)")
	tk.MustInterDirc("insert into t values (2, 2)")
	tk.MustInterDirc("insert into t values (3, 3)")
	tk.MustInterDirc("prepare stmt from 'select /*+ IGNORE_PLAN_CACHE() */ * from t where id=?'")
	tk.MustInterDirc("set @ignore_plan_doma = 1")
	tk.MustInterDirc("execute stmt using @ignore_plan_doma")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.UseCache, IsFalse)
}

func (s *testSuite1) TestPrepareStmtAfterIsolationReadChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int)")
	// create virtual tiflash replica.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	EDB, exists := is.SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range EDB.Blocks {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &perceptron.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustInterDirc("set @@stochastik.milevadb_isolation_read_engines='einsteindb'")
	tk.MustInterDirc("prepare stmt from \"select * from t\"")
	tk.MustQuery("execute stmt")
	tkProcess := tk.Se.ShowProcess()
	ps := []*soliton.ProcessInfo{tkProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Events()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[einsteindb]")

	tk.MustInterDirc("set @@stochastik.milevadb_isolation_read_engines='tiflash'")
	tk.MustInterDirc("execute stmt")
	tkProcess = tk.Se.ShowProcess()
	ps = []*soliton.ProcessInfo{tkProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Events()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tiflash]")

	c.Assert(len(tk.Se.GetStochastikVars().PreparedStmts), Equals, 1)
	c.Assert(tk.Se.GetStochastikVars().PreparedStmts[1].(*causetembedded.CachedPrepareStmt).NormalizedALLEGROSQL, Equals, "select * from t")
	c.Assert(tk.Se.GetStochastikVars().PreparedStmts[1].(*causetembedded.CachedPrepareStmt).NormalizedCauset, Equals, "")
}

type mockStochastikManager2 struct {
	se     stochastik.Stochastik
	killed bool
}

func (sm *mockStochastikManager2) ShowProcessList() map[uint64]*soliton.ProcessInfo {
	pl := make(map[uint64]*soliton.ProcessInfo)
	if pi, ok := sm.GetProcessInfo(0); ok {
		pl[pi.ID] = pi
	}
	return pl
}

func (sm *mockStochastikManager2) GetProcessInfo(id uint64) (pi *soliton.ProcessInfo, notNil bool) {
	pi = sm.se.ShowProcess()
	if pi != nil {
		notNil = true
	}
	return
}
func (sm *mockStochastikManager2) Kill(connectionID uint64, query bool) {
	sm.killed = true
	atomic.StoreUint32(&sm.se.GetStochastikVars().Killed, 1)
}
func (sm *mockStochastikManager2) UFIDelateTLSConfig(cfg *tls.Config) {}

var _ = SerialSuites(&testSuite12{&baseTestSuite{}})

type testSuite12 struct {
	*baseTestSuite
}

func (s *testSuite12) TestPreparedStmtWithHint(c *C) {
	// see https://github.com/whtcorpsinc/milevadb/issues/18535
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		causetstore.Close()
		dom.Close()
	}()

	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	tk.Se = se

	sm := &mockStochastikManager2{
		se: se,
	}
	se.SetStochastikManager(sm)
	go dom.ExpensiveQueryHandle().SetStochastikManager(sm).Run()
	tk.MustInterDirc("prepare stmt from \"select /*+ max_execution_time(100) */ sleep(10)\"")
	tk.MustQuery("execute stmt").Check(testkit.Events("1"))
	c.Check(sm.killed, Equals, true)
}

func (s *testSuite9) TestCausetCacheClusterIndex(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	causetembedded.SetPreparedCausetCache(true)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("set @@milevadb_enable_clustered_index = 1")
	tk.MustInterDirc("create causet t1(a varchar(20), b varchar(20), c varchar(20), primary key(a, b))")
	tk.MustInterDirc("insert into t1 values('1','1','111'),('2','2','222'),('3','3','333')")

	// For causet scan
	tk.MustInterDirc(`prepare stmt1 from "select * from t1 where t1.a = ? and t1.b > ?"`)
	tk.MustInterDirc("set @v1 = 1")
	tk.MustInterDirc("set @v2 = 0")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Events("1 1 111"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Events("0"))
	tk.MustInterDirc("set @v1 = 2")
	tk.MustInterDirc("set @v2 = 1")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Events("2 2 222"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Events("1"))
	tk.MustInterDirc("set @v1 = 3")
	tk.MustInterDirc("set @v2 = 2")
	tk.MustQuery("execute stmt1 using @v1,@v2").Check(testkit.Events("3 3 333"))
	tkProcess := tk.Se.ShowProcess()
	ps := []*soliton.ProcessInfo{tkProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Events()
	c.Assert(strings.Index(rows[len(rows)-1][4].(string), `range:("3" "2","3" +inf]`), Equals, 0)

	// For point get
	tk.MustInterDirc(`prepare stmt2 from "select * from t1 where t1.a = ? and t1.b = ?"`)
	tk.MustInterDirc("set @v1 = 1")
	tk.MustInterDirc("set @v2 = 1")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Events("1 1 111"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Events("0"))
	tk.MustInterDirc("set @v1 = 2")
	tk.MustInterDirc("set @v2 = 2")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Events("2 2 222"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Events("1"))
	tk.MustInterDirc("set @v1 = 3")
	tk.MustInterDirc("set @v2 = 3")
	tk.MustQuery("execute stmt2 using @v1,@v2").Check(testkit.Events("3 3 333"))
	tkProcess = tk.Se.ShowProcess()
	ps = []*soliton.ProcessInfo{tkProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Events()
	c.Assert(strings.Index(rows[len(rows)-1][0].(string), `Point_Get`), Equals, 0)

	// For CBO point get and batch point get
	// case 1:
	tk.MustInterDirc(`drop causet if exists ta, tb`)
	tk.MustInterDirc(`create causet ta (a varchar(8) primary key, b int)`)
	tk.MustInterDirc(`insert ta values ('a', 1), ('b', 2)`)
	tk.MustInterDirc(`create causet tb (a varchar(8) primary key, b int)`)
	tk.MustInterDirc(`insert tb values ('a', 1), ('b', 2)`)
	tk.MustInterDirc(`prepare stmt1 from "select * from ta, tb where ta.a = tb.a and ta.a = ?"`)
	tk.MustInterDirc(`set @v1 = 'a', @v2 = 'b'`)
	tk.MustQuery(`execute stmt1 using @v1`).Check(testkit.Events("a 1 a 1"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Events("b 2 b 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Events("1"))

	// case 2:
	tk.MustInterDirc(`drop causet if exists ta, tb`)
	tk.MustInterDirc(`create causet ta (a varchar(10) primary key, b int not null)`)
	tk.MustInterDirc(`insert ta values ('a', 1), ('b', 2)`)
	tk.MustInterDirc(`create causet tb (b int primary key, c int)`)
	tk.MustInterDirc(`insert tb values (1, 1), (2, 2)`)
	tk.MustInterDirc(`prepare stmt1 from "select * from ta, tb where ta.b = tb.b and ta.a = ?"`)
	tk.MustInterDirc(`set @v1 = 'a', @v2 = 'b'`)
	tk.MustQuery(`execute stmt1 using @v1`).Check(testkit.Events("a 1 1 1"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Events("b 2 2 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Events("1"))
	tk.MustQuery(`execute stmt1 using @v2`).Check(testkit.Events("b 2 2 2"))
	tkProcess = tk.Se.ShowProcess()
	ps = []*soliton.ProcessInfo{tkProcess}
	tk.Se.SetStochastikManager(&mockStochastikManager1{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Events()
	c.Assert(strings.Index(rows[1][0].(string), `Point_Get`), Equals, 6)

	// case 3:
	tk.MustInterDirc(`drop causet if exists ta, tb`)
	tk.MustInterDirc(`create causet ta (a varchar(10), b varchar(10), c int, primary key (a, b))`)
	tk.MustInterDirc(`insert ta values ('a', 'a', 1), ('b', 'b', 2), ('c', 'c', 3)`)
	tk.MustInterDirc(`create causet tb (b int primary key, c int)`)
	tk.MustInterDirc(`insert tb values (1, 1), (2, 2), (3,3)`)
	tk.MustInterDirc(`prepare stmt1 from "select * from ta, tb where ta.c = tb.b and ta.a = ? and ta.b = ?"`)
	tk.MustInterDirc(`set @v1 = 'a', @v2 = 'b', @v3 = 'c'`)
	tk.MustQuery(`execute stmt1 using @v1, @v1`).Check(testkit.Events("a a 1 1 1"))
	tk.MustQuery(`execute stmt1 using @v2, @v2`).Check(testkit.Events("b b 2 2 2"))
	tk.MustInterDirc(`prepare stmt2 from "select * from ta, tb where ta.c = tb.b and (ta.a, ta.b) in ((?, ?), (?, ?))"`)
	tk.MustQuery(`execute stmt2 using @v1, @v1, @v2, @v2`).Check(testkit.Events("a a 1 1 1", "b b 2 2 2"))
	tk.MustQuery(`execute stmt2 using @v2, @v2, @v3, @v3`).Check(testkit.Events("b b 2 2 2", "c c 3 3 3"))

	// For issue 19002
	tk.MustInterDirc(`set @@milevadb_enable_clustered_index = 1`)
	tk.MustInterDirc(`drop causet if exists t1`)
	tk.MustInterDirc(`create causet t1(a int, b int, c int, primary key(a, b))`)
	tk.MustInterDirc(`insert into t1 values(1,1,111),(2,2,222),(3,3,333)`)
	// Point Get:
	tk.MustInterDirc(`prepare stmt1 from "select * from t1 where t1.a = ? and t1.b = ?"`)
	tk.MustInterDirc(`set @v1=1, @v2=1`)
	tk.MustQuery(`execute stmt1 using @v1,@v2`).Check(testkit.Events("1 1 111"))
	tk.MustInterDirc(`set @v1=2, @v2=2`)
	tk.MustQuery(`execute stmt1 using @v1,@v2`).Check(testkit.Events("2 2 222"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Events("1"))
	// Batch Point Get:
	tk.MustInterDirc(`prepare stmt2 from "select * from t1 where (t1.a,t1.b) in ((?,?),(?,?))"`)
	tk.MustInterDirc(`set @v1=1, @v2=1, @v3=2, @v4=2`)
	tk.MustQuery(`execute stmt2 using @v1,@v2,@v3,@v4`).Check(testkit.Events("1 1 111", "2 2 222"))
	tk.MustInterDirc(`set @v1=2, @v2=2, @v3=3, @v4=3`)
	tk.MustQuery(`execute stmt2 using @v1,@v2,@v3,@v4`).Check(testkit.Events("2 2 222", "3 3 333"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Events("1"))
}
