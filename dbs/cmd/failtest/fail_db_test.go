// Copyright 2020 WHTCORPS INC
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
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/dbs"
	dbsutil "github.com/whtcorpsinc/milevadb/dbs/soliton"
	"github.com/whtcorpsinc/milevadb/dbs/solitonutil"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	. "github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

var _ = SerialSuites(&testFailDBSuite{})

type testFailDBSuite struct {
	cluster     cluster.Cluster
	lease       time.Duration
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	se          stochastik.Stochastik
	p           *BerolinaSQL.BerolinaSQL

	CommonHandleSuite
}

func (s *testFailDBSuite) SetUpSuite(c *C) {
	s.lease = 200 * time.Millisecond
	dbs.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	var err error
	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	stochastik.SetSchemaLease(s.lease)
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	s.p = BerolinaSQL.New()
}

func (s *testFailDBSuite) TearDownSuite(c *C) {
	_, err := s.se.InterDircute(context.Background(), "drop database if exists test_db_state")
	c.Assert(err, IsNil)
	s.se.Close()
	s.dom.Close()
	s.causetstore.Close()
}

// TestHalfwayCancelOperations tests the case that the schemaReplicant is correct after the execution of operations are cancelled halfway.
func (s *testFailDBSuite) TestHalfwayCancelOperations(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/truncateBlockErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/truncateBlockErr"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database cancel_job_db")
	tk.MustInterDirc("use cancel_job_db")

	// test for truncating causet
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("insert into t values(1)")
	_, err := tk.InterDirc("truncate causet t")
	c.Assert(err, NotNil)

	// Make sure that the causet's data has not been deleted.
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	// InterDircute dbs memex reload schemaReplicant
	tk.MustInterDirc("alter causet t comment 'test1'")
	err = s.dom.DBS().GetHook().OnChanged(nil)
	c.Assert(err, IsNil)

	tk = testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use cancel_job_db")
	// Test schemaReplicant is correct.
	tk.MustInterDirc("select * from t")
	// test for renaming causet
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/renameBlockErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/renameBlockErr"), IsNil)
	}()
	tk.MustInterDirc("create causet tx(a int)")
	tk.MustInterDirc("insert into tx values(1)")
	_, err = tk.InterDirc("rename causet tx to ty")
	c.Assert(err, NotNil)
	// Make sure that the causet's data has not been deleted.
	tk.MustQuery("select * from tx").Check(testkit.Rows("1"))
	// InterDircute dbs memex reload schemaReplicant.
	tk.MustInterDirc("alter causet tx comment 'tx'")
	err = s.dom.DBS().GetHook().OnChanged(nil)
	c.Assert(err, IsNil)

	tk = testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use cancel_job_db")
	tk.MustInterDirc("select * from tx")
	// test for exchanging partition
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/exchangePartitionErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/exchangePartitionErr"), IsNil)
	}()
	tk.MustInterDirc("create causet pt(a int) partition by hash (a) partitions 2")
	tk.MustInterDirc("insert into pt values(1), (3), (5)")
	tk.MustInterDirc("create causet nt(a int)")
	tk.MustInterDirc("insert into nt values(7)")
	_, err = tk.InterDirc("alter causet pt exchange partition p1 with causet nt")
	c.Assert(err, NotNil)

	tk.MustQuery("select * from pt").Check(testkit.Rows("1", "3", "5"))
	tk.MustQuery("select * from nt").Check(testkit.Rows("7"))
	// InterDircute dbs memex reload schemaReplicant.
	tk.MustInterDirc("alter causet pt comment 'pt'")
	err = s.dom.DBS().GetHook().OnChanged(nil)
	c.Assert(err, IsNil)

	tk = testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use cancel_job_db")
	// Test schemaReplicant is correct.
	tk.MustInterDirc("select * from pt")

	// clean up
	tk.MustInterDirc("drop database cancel_job_db")
}

// TestInitializeOffsetAndState tests the case that the defCausumn's offset and state don't be initialized in the file of dbs_api.go when
// doing the operation of 'modify defCausumn'.
func (s *testFailDBSuite) TestInitializeOffsetAndState(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(a int, b int, c int)")
	defer tk.MustInterDirc("drop causet t")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/uninitializedOffsetAndState", `return(true)`), IsNil)
	tk.MustInterDirc("ALTER TABLE t MODIFY COLUMN b int FIRST;")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/uninitializedOffsetAndState"), IsNil)
}

func (s *testFailDBSuite) TestUFIDelateHandleFailed(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/errorUFIDelateReorgHandle", `1*return`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/errorUFIDelateReorgHandle"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test_handle_failed")
	defer tk.MustInterDirc("drop database test_handle_failed")
	tk.MustInterDirc("use test_handle_failed")
	tk.MustInterDirc("create causet t(a int primary key, b int)")
	tk.MustInterDirc("insert into t values(-1, 1)")
	tk.MustInterDirc("alter causet t add index idx_b(b)")
	result := tk.MustQuery("select count(*) from t use index(idx_b)")
	result.Check(testkit.Rows("1"))
	tk.MustInterDirc("admin check index t idx_b")
}

func (s *testFailDBSuite) TestAddIndexFailed(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/mockBackfillRunErr", `1*return`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/mockBackfillRunErr"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test_add_index_failed")
	defer tk.MustInterDirc("drop database test_add_index_failed")
	tk.MustInterDirc("use test_add_index_failed")

	tk.MustInterDirc("create causet t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get causet ID for split.
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test_add_index_failed"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the causet.
	s.cluster.SplitBlock(tblID, 100)

	tk.MustInterDirc("alter causet t add index idx_b(b)")
	tk.MustInterDirc("admin check index t idx_b")
	tk.MustInterDirc("admin check causet t")
}

// TestFailSchemaSyncer test when the schemaReplicant syncer is done,
// should prohibit DML executing until the syncer is restartd by loadSchemaInLoop.
func (s *testFailDBSuite) TestFailSchemaSyncer(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int)")
	defer tk.MustInterDirc("drop causet if exists t")
	originalRetryTimes := petri.SchemaOutOfDateRetryTimes
	petri.SchemaOutOfDateRetryTimes = 1
	defer func() {
		petri.SchemaOutOfDateRetryTimes = originalRetryTimes
	}()
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	mockSyncer, ok := s.dom.DBS().SchemaSyncer().(*dbs.MockSchemaSyncer)
	c.Assert(ok, IsTrue)

	// make reload failed.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/petri/ErrorMockReloadFailed", `return(true)`), IsNil)
	mockSyncer.CloseStochastik()
	// wait the schemaValidator is stopped.
	for i := 0; i < 50; i++ {
		if !s.dom.SchemaValidator.IsStarted() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	c.Assert(s.dom.SchemaValidator.IsStarted(), IsFalse)
	_, err := tk.InterDirc("insert into t values(1)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[petri:8027]Information schemaReplicant is out of date: schemaReplicant failed to uFIDelate in 1 lease, please make sure MilevaDB can connect to EinsteinDB")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/petri/ErrorMockReloadFailed"), IsNil)
	// wait the schemaValidator is started.
	for i := 0; i < 50; i++ {
		if s.dom.SchemaValidator.IsStarted() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(s.dom.SchemaValidator.IsStarted(), IsTrue)
	_, err = tk.InterDirc("insert into t values(1)")
	c.Assert(err, IsNil)
}

func (s *testFailDBSuite) TestGenGlobalIDFail(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/mockGenGlobalIDFail"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists gen_global_id_fail")
	tk.MustInterDirc("use gen_global_id_fail")

	sql1 := "create causet t1(a bigint PRIMARY KEY, b int)"
	sql2 := `create causet t2(a bigint PRIMARY KEY, b int) partition by range (a) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than maxvalue)`
	sql3 := `truncate causet t1`
	sql4 := `truncate causet t2`

	testcases := []struct {
		allegrosql string
		causet     string
		mockErr    bool
	}{
		{sql1, "t1", true},
		{sql2, "t2", true},
		{sql1, "t1", false},
		{sql2, "t2", false},
		{sql3, "t1", true},
		{sql4, "t2", true},
		{sql3, "t1", false},
		{sql4, "t2", false},
	}

	for idx, test := range testcases {
		if test.mockErr {
			c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/mockGenGlobalIDFail", `return(true)`), IsNil)
			_, err := tk.InterDirc(test.allegrosql)
			c.Assert(err, NotNil, Commentf("the %dth test case '%s' fail", idx, test.allegrosql))
		} else {
			c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/mockGenGlobalIDFail", `return(false)`), IsNil)
			tk.MustInterDirc(test.allegrosql)
			tk.MustInterDirc(fmt.Sprintf("insert into %s values (%d, 42)", test.causet, rand.Intn(65536)))
			tk.MustInterDirc(fmt.Sprintf("admin check causet %s", test.causet))
		}
	}
	tk.MustInterDirc("admin check causet t1")
	tk.MustInterDirc("admin check causet t2")
}

func batchInsert(tk *testkit.TestKit, tbl string, start, end int) {
	dml := fmt.Sprintf("insert into %s values", tbl)
	for i := start; i < end; i++ {
		dml += fmt.Sprintf("(%d, %d, %d)", i, i, i)
		if i != end-1 {
			dml += ","
		}
	}
	tk.MustInterDirc(dml)
}

func (s *testFailDBSuite) TestAddIndexWorkerNum(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test_db")
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists test_add_index")
	if s.IsCommonHandle {
		tk.MustInterDirc("set @@milevadb_enable_clustered_index = 1")
		tk.MustInterDirc("create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1, c3))")
	} else {
		tk.MustInterDirc("create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))")
	}

	done := make(chan error, 1)
	start := -10
	// first add some rows
	for i := start; i < 4090; i += 100 {
		batchInsert(tk, "test_add_index", i, i+100)
	}

	is := s.dom.SchemaReplicant()
	schemaName := perceptron.NewCIStr("test_db")
	blockName := perceptron.NewCIStr("test_add_index")
	tbl, err := is.BlockByName(schemaName, blockName)
	c.Assert(err, IsNil)

	splitCount := 100
	// Split causet to multi region.
	s.cluster.SplitBlock(tbl.Meta().ID, splitCount)

	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	originDBSAddIndexWorkerCnt := variable.GetDBSReorgWorkerCounter()
	lastSetWorkerCnt := originDBSAddIndexWorkerCnt
	atomic.StoreInt32(&dbs.TestCheckWorkerNumber, lastSetWorkerCnt)
	dbs.TestCheckWorkerNumber = lastSetWorkerCnt
	defer tk.MustInterDirc(fmt.Sprintf("set @@global.milevadb_dbs_reorg_worker_cnt=%d", originDBSAddIndexWorkerCnt))

	if !s.IsCommonHandle { // only enable failpoint once
		c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/checkBackfillWorkerNum", `return(true)`), IsNil)
		defer func() {
			c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/checkBackfillWorkerNum"), IsNil)
		}()
	}

	solitonutil.StochastikInterDircInGoroutine(c, s.causetstore, "create index c3_index on test_add_index (c3)", done)
	checkNum := 0

LOOP:
	for {
		select {
		case err = <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-dbs.TestCheckWorkerNumCh:
			lastSetWorkerCnt = int32(rand.Intn(8) + 8)
			tk.MustInterDirc(fmt.Sprintf("set @@global.milevadb_dbs_reorg_worker_cnt=%d", lastSetWorkerCnt))
			atomic.StoreInt32(&dbs.TestCheckWorkerNumber, lastSetWorkerCnt)
			checkNum++
		}
	}
	c.Assert(checkNum, Greater, 5)
	tk.MustInterDirc("admin check causet test_add_index")
	tk.MustInterDirc("drop causet test_add_index")

	s.RerunWithCommonHandleEnabled(c, s.TestAddIndexWorkerNum)
}

// TestRunDBSJobPanic tests recover panic when run dbs job panic.
func (s *testFailDBSuite) TestRunDBSJobPanic(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/mockPanicInRunDBSJob"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/mockPanicInRunDBSJob", `1*panic("panic test")`), IsNil)
	_, err := tk.InterDirc("create causet t(c1 int, c2 int)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
}

func (s *testFailDBSuite) TestPartitionAddIndexGC(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`create causet partition_add_idx (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p5 values less than (2008),
	partition p7 values less than (2020)
	);`)
	tk.MustInterDirc("insert into partition_add_idx values(1, '2010-01-01'), (2, '1990-01-01'), (3, '2001-01-01')")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/mockUFIDelateCachedSafePoint", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/mockUFIDelateCachedSafePoint"), IsNil)
	}()
	tk.MustInterDirc("alter causet partition_add_idx add index idx (id, hired)")
}

func (s *testFailDBSuite) TestModifyDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	enableChangeDeferredCausetType := tk.Se.GetStochastikVars().EnableChangeDeferredCausetType
	tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = true
	defer func() {
		tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = enableChangeDeferredCausetType
	}()

	tk.MustInterDirc("create causet t (a int not null default 1, b int default 2, c int not null default 0, primary key(c), index idx(b), index idx1(a), index idx2(b, c))")
	tk.MustInterDirc("insert into t values(1, 2, 3), (11, 22, 33)")
	_, err := tk.InterDirc("alter causet t change defCausumn c cc mediumint")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: milevadb_enable_change_defCausumn_type is true and this defCausumn has primary key flag")
	tk.MustInterDirc("alter causet t change defCausumn b bb mediumint first")
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defcaus := tbl.Meta().DeferredCausets
	defcausStr := ""
	idxsStr := ""
	for _, defCaus := range defcaus {
		defcausStr += defCaus.Name.L + " "
	}
	for _, idx := range tbl.Meta().Indices {
		idxsStr += idx.Name.L + " "
	}
	c.Assert(len(defcaus), Equals, 3)
	c.Assert(len(tbl.Meta().Indices), Equals, 3)
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1 3", "22 11 33"))
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `bb` mediumint(9) DEFAULT NULL,\n" +
		"  `a` int(11) NOT NULL DEFAULT 1,\n" +
		"  `c` int(11) NOT NULL DEFAULT 0,\n" +
		"  PRIMARY KEY (`c`),\n" +
		"  KEY `idx` (`bb`),\n" +
		"  KEY `idx1` (`a`),\n" +
		"  KEY `idx2` (`bb`,`c`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("admin check causet t")
	tk.MustInterDirc("insert into t values(111, 222, 333)")
	_, err = tk.InterDirc("alter causet t change defCausumn a aa tinyint after c")
	c.Assert(err.Error(), Equals, "[types:1690]constant 222 overflows tinyint")
	tk.MustInterDirc("alter causet t change defCausumn a aa mediumint after c")
	tk.MustQuery("show create causet t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `bb` mediumint(9) DEFAULT NULL,\n" +
		"  `c` int(11) NOT NULL DEFAULT 0,\n" +
		"  `aa` mediumint(9) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`c`),\n" +
		"  KEY `idx` (`bb`),\n" +
		"  KEY `idx1` (`aa`),\n" +
		"  KEY `idx2` (`bb`,`c`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("select * from t").Check(testkit.Rows("2 3 1", "22 33 11", "111 333 222"))
	tk.MustInterDirc("admin check causet t")

	// Test unsupport memexs.
	tk.MustInterDirc("create causet t1(a int) partition by hash (a) partitions 2")
	_, err = tk.InterDirc("alter causet t1 modify defCausumn a mediumint")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: milevadb_enable_change_defCausumn_type is true, causet is partition causet")
	tk.MustInterDirc("create causet t2(id int, a int, b int generated always as (abs(a)) virtual, c int generated always as (a+1) stored)")
	_, err = tk.InterDirc("alter causet t2 modify defCausumn b mediumint")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: milevadb_enable_change_defCausumn_type is true, newDefCaus IsGenerated false, oldDefCaus IsGenerated true")
	_, err = tk.InterDirc("alter causet t2 modify defCausumn c mediumint")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: milevadb_enable_change_defCausumn_type is true, newDefCaus IsGenerated false, oldDefCaus IsGenerated true")
	_, err = tk.InterDirc("alter causet t2 modify defCausumn a mediumint generated always as(id+1) stored")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported modify defCausumn: milevadb_enable_change_defCausumn_type is true, newDefCaus IsGenerated true, oldDefCaus IsGenerated false")

	// Test multiple rows of data.
	tk.MustInterDirc("create causet t3(a int not null default 1, b int default 2, c int not null default 0, primary key(c), index idx(b), index idx1(a), index idx2(b, c))")
	// Add some discrete rows.
	maxBatch := 20
	batchCnt := 100
	// Make sure there are no duplicate keys.
	defaultBatchSize := variable.DefMilevaDBDBSReorgBatchSize * variable.DefMilevaDBDBSReorgWorkerCount
	base := defaultBatchSize * 20
	for i := 1; i < batchCnt; i++ {
		n := base + i*defaultBatchSize + i
		for j := 0; j < rand.Intn(maxBatch); j++ {
			n += j
			allegrosql := fmt.Sprintf("insert into t3 values (%d, %d, %d)", n, n, n)
			tk.MustInterDirc(allegrosql)
		}
	}
	tk.MustInterDirc("alter causet t3 modify defCausumn a mediumint")
	tk.MustInterDirc("admin check causet t")

	// Test PointGet.
	tk.MustInterDirc("create causet t4(a bigint, b int, unique index idx(a));")
	tk.MustInterDirc("insert into t4 values (1,1),(2,2),(3,3),(4,4),(5,5);")
	tk.MustInterDirc("alter causet t4 modify a bigint unsigned;")
	tk.MustQuery("select * from t4 where a=1;").Check(testkit.Rows("1 1"))

	// Test changing null to not null.
	tk.MustInterDirc("create causet t5(a bigint, b int, unique index idx(a));")
	tk.MustInterDirc("insert into t5 values (1,1),(2,2),(3,3),(4,4),(5,5);")
	tk.MustInterDirc("alter causet t5 modify a int not null;")

	tk.MustInterDirc("drop causet t, t1, t2, t3, t4, t5")
}
