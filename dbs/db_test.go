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
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	BerolinaSQLtypes "github.com/whtcorpsinc/BerolinaSQL/types"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	testdbsutil "github.com/whtcorpsinc/milevadb/dbs/solitonutil"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/petriutil"
	"github.com/whtcorpsinc/milevadb/soliton/israce"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
)

const (
	// waitForCleanDataRound indicates how many times should we check data is cleaned or not.
	waitForCleanDataRound = 150
	// waitForCleanDataInterval is a min duration between 2 check for data clean.
	waitForCleanDataInterval = time.Millisecond * 100
)

var _ = Suite(&testDBSuite1{&testDBSuite{}})
var _ = Suite(&testDBSuite2{&testDBSuite{}})
var _ = Suite(&testDBSuite3{&testDBSuite{}})
var _ = Suite(&testDBSuite4{&testDBSuite{}})
var _ = Suite(&testDBSuite5{&testDBSuite{}})
var _ = Suite(&testDBSuite6{&testDBSuite{}})
var _ = Suite(&testDBSuite7{&testDBSuite{}})
var _ = SerialSuites(&testSerialDBSuite{&testDBSuite{}})

const defaultBatchSize = 1024

type testDBSuite struct {
	cluster    cluster.Cluster
	causetstore      ekv.CausetStorage
	dom        *petri.Petri
	schemaName string
	s          stochastik.Stochastik
	lease      time.Duration
	autoIDStep int64
}

func setUpSuite(s *testDBSuite, c *C) {
	var err error

	s.lease = 600 * time.Millisecond
	stochastik.SetSchemaLease(s.lease)
	stochastik.DisableStats4Test()
	s.schemaName = "test_db"
	s.autoIDStep = autoid.GetStep()
	dbs.SetWaitTimeWhenErrorOccurred(0)

	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)

	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.s, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	_, err = s.s.InterDircute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
	s.s.InterDircute(context.Background(), "set @@global.milevadb_max_delta_schema_count= 4096")
}

func tearDownSuite(s *testDBSuite, c *C) {
	s.s.InterDircute(context.Background(), "drop database if exists test_db")
	s.s.Close()
	s.dom.Close()
	s.causetstore.Close()
}

func (s *testDBSuite) SetUpSuite(c *C) {
	setUpSuite(s, c)
}

func (s *testDBSuite) TearDownSuite(c *C) {
	tearDownSuite(s, c)
}

type testDBSuite1 struct{ *testDBSuite }
type testDBSuite2 struct{ *testDBSuite }
type testDBSuite3 struct{ *testDBSuite }
type testDBSuite4 struct{ *testDBSuite }
type testDBSuite5 struct{ *testDBSuite }
type testDBSuite6 struct{ *testDBSuite }
type testDBSuite7 struct{ *testDBSuite }
type testSerialDBSuite struct{ *testDBSuite }

func testAddIndexWithPK(tk *testkit.TestKit, s *testSerialDBSuite, c *C) {
	tk.MustInterDirc("drop causet if exists test_add_index_with_pk")
	tk.MustInterDirc("create causet test_add_index_with_pk(a int not null, b int not null default '0', primary key(a))")
	tk.MustInterDirc("insert into test_add_index_with_pk values(1, 2)")
	tk.MustInterDirc("alter causet test_add_index_with_pk add index idx (a)")
	tk.MustQuery("select a from test_add_index_with_pk").Check(testkit.Rows("1"))
	tk.MustInterDirc("insert into test_add_index_with_pk values(2, 2)")
	tk.MustInterDirc("alter causet test_add_index_with_pk add index idx1 (a, b)")
	tk.MustQuery("select * from test_add_index_with_pk").Check(testkit.Rows("1 2", "2 2"))
	tk.MustInterDirc("drop causet if exists test_add_index_with_pk1")
	tk.MustInterDirc("create causet test_add_index_with_pk1(a int not null, b int not null default '0', c int, d int, primary key(c))")
	tk.MustInterDirc("insert into test_add_index_with_pk1 values(1, 1, 1, 1)")
	tk.MustInterDirc("alter causet test_add_index_with_pk1 add index idx (c)")
	tk.MustInterDirc("insert into test_add_index_with_pk1 values(2, 2, 2, 2)")
	tk.MustQuery("select * from test_add_index_with_pk1").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
	tk.MustInterDirc("drop causet if exists test_add_index_with_pk2")
	tk.MustInterDirc("create causet test_add_index_with_pk2(a int not null, b int not null default '0', c int unsigned, d int, primary key(c))")
	tk.MustInterDirc("insert into test_add_index_with_pk2 values(1, 1, 1, 1)")
	tk.MustInterDirc("alter causet test_add_index_with_pk2 add index idx (c)")
	tk.MustInterDirc("insert into test_add_index_with_pk2 values(2, 2, 2, 2)")
	tk.MustQuery("select * from test_add_index_with_pk2").Check(testkit.Rows("1 1 1 1", "2 2 2 2"))
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int, c int, primary key(a, b));")
	tk.MustInterDirc("insert into t values (1, 2, 3);")
	tk.MustInterDirc("create index idx on t (a, b);")
}

func (s *testSerialDBSuite) TestAddIndexWithPK(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})

	testAddIndexWithPK(tk, s, c)
	tk.MustInterDirc("set @@milevadb_enable_clustered_index = 1;")
	testAddIndexWithPK(tk, s, c)
}

func (s *testDBSuite5) TestAddIndexWithDupIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)

	err1 := dbs.ErrDupKeyName.GenWithStack("index already exist %s", "idx")
	err2 := dbs.ErrDupKeyName.GenWithStack("index already exist %s; "+
		"a background job is trying to add the same index, "+
		"please check by `ADMIN SHOW DBS JOBS`", "idx")

	// When there is already an duplicate index, show error message.
	tk.MustInterDirc("create causet test_add_index_with_dup (a int, key idx (a))")
	_, err := tk.InterDirc("alter causet test_add_index_with_dup add index idx (a)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)
	c.Assert(errors.Cause(err1).Error() == err.Error(), IsTrue)

	// When there is another stochastik adding duplicate index with state other than
	// StatePublic, show explicit error message.
	t := s.testGetBlock(c, "test_add_index_with_dup")
	indexInfo := t.Meta().FindIndexByName("idx")
	indexInfo.State = perceptron.StateNone
	_, err = tk.InterDirc("alter causet test_add_index_with_dup add index idx (a)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)
	c.Assert(errors.Cause(err2).Error() == err.Error(), IsTrue)

	tk.MustInterDirc("drop causet test_add_index_with_dup")
}

func (s *testDBSuite1) TestRenameIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("create causet t (pk int primary key, c int default 1, c1 int default 1, unique key k1(c), key k2(c1))")

	// Test rename success
	tk.MustInterDirc("alter causet t rename index k1 to k3")
	tk.MustInterDirc("admin check index t k3")

	// Test rename to the same name
	tk.MustInterDirc("alter causet t rename index k3 to k3")
	tk.MustInterDirc("admin check index t k3")

	// Test rename on non-exists keys
	tk.MustGetErrCode("alter causet t rename index x to x", errno.ErrKeyDoesNotExist)

	// Test rename on already-exists keys
	tk.MustGetErrCode("alter causet t rename index k3 to k2", errno.ErrDupKeyName)

	tk.MustInterDirc("alter causet t rename index k2 to K2")
	tk.MustGetErrCode("alter causet t rename key k3 to K2", errno.ErrDupKeyName)
}

func testGetBlockByName(c *C, ctx stochastikctx.Context, EDB, causet string) causet.Block {
	dom := petri.GetPetri(ctx)
	// Make sure the causet schemaReplicant is the new schemaReplicant.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(causet))
	c.Assert(err, IsNil)
	return tbl
}

func testGetSchemaByName(c *C, ctx stochastikctx.Context, EDB string) *perceptron.DBInfo {
	dom := petri.GetPetri(ctx)
	// Make sure the causet schemaReplicant is the new schemaReplicant.
	err := dom.Reload()
	c.Assert(err, IsNil)
	dbInfo, ok := dom.SchemaReplicant().SchemaByName(perceptron.NewCIStr(EDB))
	c.Assert(ok, IsTrue)
	return dbInfo
}

func (s *testDBSuite) testGetBlock(c *C, name string) causet.Block {
	ctx := s.s.(stochastikctx.Context)
	return testGetBlockByName(c, ctx, s.schemaName, name)
}

func (s *testDBSuite) testGetDB(c *C, dbName string) *perceptron.DBInfo {
	ctx := s.s.(stochastikctx.Context)
	dom := petri.GetPetri(ctx)
	// Make sure the causet schemaReplicant is the new schemaReplicant.
	err := dom.Reload()
	c.Assert(err, IsNil)
	EDB, ok := dom.SchemaReplicant().SchemaByName(perceptron.NewCIStr(dbName))
	c.Assert(ok, IsTrue)
	return EDB
}

func backgroundInterDirc(s ekv.CausetStorage, allegrosql string, done chan error) {
	se, err := stochastik.CreateStochastik4Test(s)
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	defer se.Close()
	_, err = se.InterDircute(context.Background(), "use test_db")
	if err != nil {
		done <- errors.Trace(err)
		return
	}
	_, err = se.InterDircute(context.Background(), allegrosql)
	done <- errors.Trace(err)
}

// TestAddPrimaryKeyRollback1 is used to test scenarios that will roll back when a duplicate primary key is encountered.
func (s *testDBSuite5) TestAddPrimaryKeyRollback1(c *C) {
	hasNullValsInKey := false
	idxName := "PRIMARY"
	addIdxALLEGROSQL := "alter causet t1 add primary key c3_index (c3);"
	errMsg := "[ekv:1062]Duplicate entry '' for key 'PRIMARY'"
	testAddIndexRollback(c, s.causetstore, s.lease, idxName, addIdxALLEGROSQL, errMsg, hasNullValsInKey)
}

// TestAddPrimaryKeyRollback2 is used to test scenarios that will roll back when a null primary key is encountered.
func (s *testDBSuite1) TestAddPrimaryKeyRollback2(c *C) {
	hasNullValsInKey := true
	idxName := "PRIMARY"
	addIdxALLEGROSQL := "alter causet t1 add primary key c3_index (c3);"
	errMsg := "[dbs:1138]Invalid use of NULL value"
	testAddIndexRollback(c, s.causetstore, s.lease, idxName, addIdxALLEGROSQL, errMsg, hasNullValsInKey)
}

func (s *testDBSuite2) TestAddUniqueIndexRollback(c *C) {
	hasNullValsInKey := false
	idxName := "c3_index"
	addIdxALLEGROSQL := "create unique index c3_index on t1 (c3)"
	errMsg := "[ekv:1062]Duplicate entry '' for key 'c3_index'"
	testAddIndexRollback(c, s.causetstore, s.lease, idxName, addIdxALLEGROSQL, errMsg, hasNullValsInKey)
}

func (s *testSerialDBSuite) TestAddExpressionIndexRollback(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int, c2 int, c3 int, unique key(c1))")
	tk.MustInterDirc("insert into t1 values (20, 20, 20), (40, 40, 40), (80, 80, 80), (160, 160, 160);")

	var checkErr error
	tk1 := testkit.NewTestKit(c, s.causetstore)
	_, checkErr = tk1.InterDirc("use test_db")

	d := s.dom.DBS()
	hook := &dbs.TestDBSCallback{}
	hook.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if job.SchemaState == perceptron.StateDeleteOnly {
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.InterDirc("delete from t1 where c1 = 40;")
		}
	}
	d.(dbs.DBSForTest).SetHook(hook)

	tk.MustGetErrMsg("alter causet t1 add index expr_idx ((pow(c1, c2)));", "[dbs:8202]Cannot decode index value, because [types:1690]DOUBLE value is out of range in 'pow(160, 160)'")
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select * from t1;").Check(testkit.Rows("20 20 20", "80 80 80", "160 160 160"))
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

func testAddIndexRollback(c *C, causetstore ekv.CausetStorage, lease time.Duration, idxName, addIdxALLEGROSQL, errMsg string, hasNullValsInKey bool) {
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int, c2 int, c3 int, unique key(c1))")
	// defaultBatchSize is equal to dbs.defaultBatchSize
	base := defaultBatchSize * 2
	count := base
	// add some rows
	batchInsert(tk, "t1", 0, count)
	// add some null rows
	if hasNullValsInKey {
		for i := count - 10; i < count; i++ {
			tk.MustInterDirc("insert into t1 values (?, ?, null)", i+10, i)
		}
	} else {
		// add some duplicate rows
		for i := count - 10; i < count; i++ {
			tk.MustInterDirc("insert into t1 values (?, ?, ?)", i+10, i, i)
		}
	}

	done := make(chan error, 1)
	go backgroundInterDirc(causetstore, addIdxALLEGROSQL, done)

	times := 0
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, errMsg, Commentf("err:%v", err))
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				tk.MustInterDirc("delete from t1 where c1 = ?", n)
				tk.MustInterDirc("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	ctx := tk.Se.(stochastikctx.Context)
	t := testGetBlockByName(c, ctx, "test_db", "t1")
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, idxName), IsFalse)
	}

	// delete duplicated/null rows, then add index
	for i := base - 10; i < base; i++ {
		tk.MustInterDirc("delete from t1 where c1 = ?", i+10)
	}
	stochastikInterDirc(c, causetstore, addIdxALLEGROSQL)
	tk.MustInterDirc("drop causet t1")
}

func (s *testDBSuite5) TestCancelAddPrimaryKey(c *C) {
	idxName := "primary"
	addIdxALLEGROSQL := "alter causet t1 add primary key idx_c2 (c2);"
	testCancelAddIndex(c, s.causetstore, s.dom.DBS(), s.lease, idxName, addIdxALLEGROSQL, "")

	// Check the defCausumn's flag when the "add primary key" failed.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	ctx := tk.Se.(stochastikctx.Context)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	t := testGetBlockByName(c, ctx, "test_db", "t1")
	defCaus1Flag := t.DefCauss()[1].Flag
	c.Assert(!allegrosql.HasNotNullFlag(defCaus1Flag) && !allegrosql.HasPreventNullInsertFlag(defCaus1Flag) && allegrosql.HasUnsignedFlag(defCaus1Flag), IsTrue)
	tk.MustInterDirc("drop causet t1")
}

func (s *testDBSuite3) TestCancelAddIndex(c *C) {
	idxName := "c3_index "
	addIdxALLEGROSQL := "create unique index c3_index on t1 (c3)"
	testCancelAddIndex(c, s.causetstore, s.dom.DBS(), s.lease, idxName, addIdxALLEGROSQL, "")

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet t1")
}

func testCancelAddIndex(c *C, causetstore ekv.CausetStorage, d dbs.DBS, lease time.Duration, idxName, addIdxALLEGROSQL, sqlModeALLEGROSQL string) {
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int, c2 int unsigned, c3 int, unique key(c1))")
	// defaultBatchSize is equal to dbs.defaultBatchSize
	count := defaultBatchSize * 32
	start := 0
	// add some rows
	if len(sqlModeALLEGROSQL) != 0 {
		// Insert some null values.
		tk.MustInterDirc(sqlModeALLEGROSQL)
		tk.MustInterDirc("insert into t1 set c1 = ?", 0)
		tk.MustInterDirc("insert into t1 set c2 = ?", 1)
		tk.MustInterDirc("insert into t1 set c3 = ?", 2)
		start = 3
	}
	for i := start; i < count; i += defaultBatchSize {
		batchInsert(tk, "t1", i, i+defaultBatchSize)
	}

	var c3IdxInfo *perceptron.IndexInfo
	hook := &dbs.TestDBSCallback{}
	originBatchSize := tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	// Set batch size to lower try to slow down add-index reorganization, This if for hook to cancel this dbs job.
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_batch_size = 32")
	defer tk.MustInterDirc(fmt.Sprintf("set @@global.milevadb_dbs_reorg_batch_size = %v", originBatchSize.Rows()[0][0]))
	// let hook.OnJobUFIDelatedExported has chance to cancel the job.
	// the hook.OnJobUFIDelatedExported is called when the job is uFIDelated, runReorgJob will wait dbs.ReorgWaitTimeout, then return the dbs.runDBSJob.
	// After that dbs call d.hook.OnJobUFIDelated(job), so that we can canceled the job in this test case.
	var checkErr error
	ctx := tk.Se.(stochastikctx.Context)
	hook.OnJobUFIDelatedExported, c3IdxInfo, checkErr = backgroundInterDircOnJobUFIDelatedExported(c, causetstore, ctx, hook, idxName)
	originalHook := d.GetHook()
	d.(dbs.DBSForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundInterDirc(causetstore, addIdxALLEGROSQL, done)

	times := 0
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				tk.MustInterDirc("delete from t1 where c1 = ?", n)
				tk.MustInterDirc("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	t := testGetBlockByName(c, ctx, "test_db", "t1")
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, idxName), IsFalse)
	}

	idx := blocks.NewIndex(t.Meta().ID, t.Meta(), c3IdxInfo)
	checkDelRangeDone(c, ctx, idx)
	d.(dbs.DBSForTest).SetHook(originalHook)
}

// TestCancelAddIndex1 tests canceling dbs job when the add index worker is not started.
func (s *testDBSuite4) TestCancelAddIndex1(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t")
	s.mustInterDirc(tk, c, "create causet t(c1 int, c2 int)")
	defer s.mustInterDirc(tk, c, "drop causet t;")

	for i := 0; i < 50; i++ {
		s.mustInterDirc(tk, c, "insert into t values (?, ?)", i, i)
	}

	var checkErr error
	hook := &dbs.TestDBSCallback{}
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionAddIndex && job.State == perceptron.JobStateRunning && job.SchemaState == perceptron.StateWriteReorganization && job.SnapshotVer == 0 {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}

			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}

			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	rs, err := tk.InterDirc("alter causet t add index idx_c2(c2)")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")

	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	t := s.testGetBlock(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c2"), IsFalse)
	}
	s.mustInterDirc(tk, c, "alter causet t add index idx_c2(c2)")
	s.mustInterDirc(tk, c, "alter causet t drop index idx_c2")
}

// TestCancelDropIndex tests cancel dbs job which type is drop primary key.
func (s *testDBSuite4) TestCancelDropPrimaryKey(c *C) {
	idxName := "primary"
	addIdxALLEGROSQL := "alter causet t add primary key idx_c2 (c2);"
	dropIdxALLEGROSQL := "alter causet t drop primary key;"
	testCancelDropIndex(c, s.causetstore, s.dom.DBS(), idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL)
}

// TestCancelDropIndex tests cancel dbs job which type is drop index.
func (s *testDBSuite5) TestCancelDropIndex(c *C) {
	idxName := "idx_c2"
	addIdxALLEGROSQL := "alter causet t add index idx_c2 (c2);"
	dropIdxALLEGROSQL := "alter causet t drop index idx_c2;"
	testCancelDropIndex(c, s.causetstore, s.dom.DBS(), idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL)
}

// testCancelDropIndex tests cancel dbs job which type is drop index.
func testCancelDropIndex(c *C, causetstore ekv.CausetStorage, d dbs.DBS, idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL string) {
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(c1 int, c2 int)")
	defer tk.MustInterDirc("drop causet t;")
	for i := 0; i < 5; i++ {
		tk.MustInterDirc("insert into t values (?, ?)", i, i)
	}
	testCases := []struct {
		needAddIndex   bool
		jobState       perceptron.JobState
		JobSchemaState perceptron.SchemaState
		cancelSucc     bool
	}{
		// perceptron.JobStateNone means the jobs is canceled before the first run.
		// if we cancel successfully, we need to set needAddIndex to false in the next test case. Otherwise, set needAddIndex to true.
		{true, perceptron.JobStateNone, perceptron.StateNone, true},
		{false, perceptron.JobStateRunning, perceptron.StateWriteOnly, false},
		{true, perceptron.JobStateRunning, perceptron.StateDeleteOnly, false},
		{true, perceptron.JobStateRunning, perceptron.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if (job.Type == perceptron.CausetActionDropIndex || job.Type == perceptron.CausetActionDropPrimaryKey) &&
			job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobID = job.ID
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = causetstore
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}

			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := d.GetHook()
	d.(dbs.DBSForTest).SetHook(hook)
	ctx := tk.Se.(stochastikctx.Context)
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddIndex {
			tk.MustInterDirc(addIdxALLEGROSQL)
		}
		rs, err := tk.InterDirc(dropIdxALLEGROSQL)
		if rs != nil {
			rs.Close()
		}
		t := testGetBlockByName(c, ctx, "test_db", "t")
		indexInfo := t.Meta().FindIndexByName(idxName)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
			c.Assert(indexInfo, NotNil)
			c.Assert(indexInfo.State, Equals, perceptron.StatePublic)
		} else {
			err1 := admin.ErrCannotCancelDBSJob.GenWithStackByArgs(jobID)
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, err1.Error())
			c.Assert(indexInfo, IsNil)
		}
	}
	d.(dbs.DBSForTest).SetHook(originalHook)
	tk.MustInterDirc(addIdxALLEGROSQL)
	tk.MustInterDirc(dropIdxALLEGROSQL)
}

// TestCancelTruncateBlock tests cancel dbs job which type is truncate causet.
func (s *testDBSuite5) TestCancelTruncateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "create database if not exists test_truncate_block")
	s.mustInterDirc(tk, c, "drop causet if exists t")
	s.mustInterDirc(tk, c, "create causet t(c1 int, c2 int)")
	defer s.mustInterDirc(tk, c, "drop causet t;")
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionTruncateBlock && job.State == perceptron.JobStateNone {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	_, err := tk.InterDirc("truncate causet t")
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
}

func (s *testDBSuite5) TestParallelDropSchemaAndDropBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "create database if not exists test_drop_schema_block")
	s.mustInterDirc(tk, c, "use test_drop_schema_block")
	s.mustInterDirc(tk, c, "create causet t(c1 int, c2 int)")
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	dbInfo := testGetSchemaByName(c, tk.Se, "test_drop_schema_block")
	done := false
	var wg sync.WaitGroup
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use test_drop_schema_block")
	hook.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionDropSchema && job.State == perceptron.JobStateRunning &&
			job.SchemaState == perceptron.StateWriteOnly && job.SchemaID == dbInfo.ID && done == false {
			wg.Add(1)
			done = true
			go func() {
				_, checkErr = tk2.InterDirc("drop causet t")
				wg.Done()
			}()
			time.Sleep(5 * time.Millisecond)
		}
	}
	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	s.mustInterDirc(tk, c, "drop database test_drop_schema_block")
	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	wg.Wait()
	c.Assert(done, IsTrue)
	c.Assert(checkErr, NotNil)
	// There are two possible assert result because:
	// 1: If drop-database is finished before drop-causet being put into the dbs job queue, it will return "unknown causet" error directly in the previous check.
	// 2: If drop-causet has passed the previous check and been put into the dbs job queue, then drop-database finished, it will return schemaReplicant change error.
	assertRes := checkErr.Error() == "[petri:8028]Information schemaReplicant is changed during the execution of the"+
		" memex(for example, causet definition may be uFIDelated by other DBS ran in parallel). "+
		"If you see this error often, try increasing `milevadb_max_delta_schema_count`. [try again later]" ||
		checkErr.Error() == "[schemaReplicant:1051]Unknown causet 'test_drop_schema_block.t'"

	c.Assert(assertRes, Equals, true)

	// Below behaviour is use to mock query `curl "http://$IP:10080/tiflash/replica"`
	fn := func(jobs []*perceptron.Job) (bool, error) {
		return interlock.GetDropOrTruncateBlockInfoFromJobs(jobs, 0, s.dom, func(job *perceptron.Job, info *perceptron.BlockInfo) (bool, error) {
			return false, nil
		})
	}
	err := tk.Se.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	err = admin.IterHistoryDBSJobs(txn, fn)
	c.Assert(err, IsNil)
}

// TestCancelRenameIndex tests cancel dbs job which type is rename index.
func (s *testDBSuite1) TestCancelRenameIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "create database if not exists test_rename_index")
	s.mustInterDirc(tk, c, "drop causet if exists t")
	s.mustInterDirc(tk, c, "create causet t(c1 int, c2 int)")
	defer s.mustInterDirc(tk, c, "drop causet t;")
	for i := 0; i < 100; i++ {
		s.mustInterDirc(tk, c, "insert into t values (?, ?)", i, i)
	}
	s.mustInterDirc(tk, c, "alter causet t add index idx_c2(c2)")
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionRenameIndex && job.State == perceptron.JobStateNone {
			jobIDs := []int64{job.ID}
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	rs, err := tk.InterDirc("alter causet t rename index idx_c2 to idx_c3")
	if rs != nil {
		rs.Close()
	}
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	t := s.testGetBlock(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c3"), IsFalse)
	}
	s.mustInterDirc(tk, c, "alter causet t rename index idx_c2 to idx_c3")
}

// TestCancelDropBlock tests cancel dbs job which type is drop causet.
func (s *testDBSuite2) TestCancelDropBlockAndSchema(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	testCases := []struct {
		needAddBlockOrDB bool
		action           perceptron.CausetActionType
		jobState         perceptron.JobState
		JobSchemaState   perceptron.SchemaState
		cancelSucc       bool
	}{
		// Check drop causet.
		// perceptron.JobStateNone means the jobs is canceled before the first run.
		{true, perceptron.CausetActionDropBlock, perceptron.JobStateNone, perceptron.StateNone, true},
		{false, perceptron.CausetActionDropBlock, perceptron.JobStateRunning, perceptron.StateWriteOnly, false},
		{true, perceptron.CausetActionDropBlock, perceptron.JobStateRunning, perceptron.StateDeleteOnly, false},

		// Check drop database.
		{true, perceptron.CausetActionDropSchema, perceptron.JobStateNone, perceptron.StateNone, true},
		{false, perceptron.CausetActionDropSchema, perceptron.JobStateRunning, perceptron.StateWriteOnly, false},
		{true, perceptron.CausetActionDropSchema, perceptron.JobStateRunning, perceptron.StateDeleteOnly, false},
	}
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	var jobID int64
	testCase := &testCases[0]
	s.mustInterDirc(tk, c, "create database if not exists test_drop_db")
	dbInfo := s.testGetDB(c, "test_drop_db")

	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == testCase.action && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState && job.SchemaID == dbInfo.ID {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originHook)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	var err error
	allegrosql := ""
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddBlockOrDB {
			s.mustInterDirc(tk, c, "create database if not exists test_drop_db")
			s.mustInterDirc(tk, c, "use test_drop_db")
			s.mustInterDirc(tk, c, "create causet if not exists t(c1 int, c2 int)")
		}

		dbInfo = s.testGetDB(c, "test_drop_db")

		if testCase.action == perceptron.CausetActionDropBlock {
			allegrosql = "drop causet t;"
		} else if testCase.action == perceptron.CausetActionDropSchema {
			allegrosql = "drop database test_drop_db;"
		}

		_, err = tk.InterDirc(allegrosql)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
			s.mustInterDirc(tk, c, "insert into t values (?, ?)", i, i)
		} else {
			c.Assert(err, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDBSJob.GenWithStackByArgs(jobID).Error())
			_, err = tk.InterDirc("insert into t values (?, ?)", i, i)
			c.Assert(err, NotNil)
		}
	}
}

func (s *testDBSuite3) TestAddAnonymousIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	s.mustInterDirc(tk, c, "create causet t_anonymous_index (c1 int, c2 int, C3 int)")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index add index (c1, c2)")
	// for dropping empty index
	_, err := tk.InterDirc("alter causet t_anonymous_index drop index")
	c.Assert(err, NotNil)
	// The index name is c1 when adding index (c1, c2).
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index drop index c1")
	t := s.testGetBlock(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 0)
	// for adding some indices that the first defCausumn name is c1
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index add index (c1)")
	_, err = tk.InterDirc("alter causet t_anonymous_index add index c1 (c2)")
	c.Assert(err, NotNil)
	t = s.testGetBlock(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 1)
	idx := t.Indices()[0].Meta().Name.L
	c.Assert(idx, Equals, "c1")
	// The MyALLEGROSQL will be a warning.
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index add index c1_3 (c1)")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index add index (c1, c2, C3)")
	// The MyALLEGROSQL will be a warning.
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index add index (c1)")
	t = s.testGetBlock(c, "t_anonymous_index")
	c.Assert(t.Indices(), HasLen, 4)
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index drop index c1")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index drop index c1_2")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index drop index c1_3")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index drop index c1_4")
	// for case insensitive
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index add index (C3)")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index drop index c3")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index add index c3 (C3)")
	s.mustInterDirc(tk, c, "alter causet t_anonymous_index drop index C3")
	// for anonymous index with defCausumn name `primary`
	s.mustInterDirc(tk, c, "create causet t_primary (`primary` int, b int, key (`primary`))")
	t = s.testGetBlock(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	s.mustInterDirc(tk, c, "alter causet t_primary add index (`primary`);")
	t = s.testGetBlock(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	s.mustInterDirc(tk, c, "alter causet t_primary add primary key(b);")
	t = s.testGetBlock(c, "t_primary")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	c.Assert(t.Indices()[2].Meta().Name.L, Equals, "primary")
	s.mustInterDirc(tk, c, "create causet t_primary_2 (`primary` int, key primary_2 (`primary`), key (`primary`))")
	t = s.testGetBlock(c, "t_primary_2")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
	s.mustInterDirc(tk, c, "create causet t_primary_3 (`primary_2` int, key(`primary_2`), `primary` int, key(`primary`));")
	t = s.testGetBlock(c, "t_primary_3")
	c.Assert(t.Indices()[0].Meta().Name.String(), Equals, "primary_2")
	c.Assert(t.Indices()[1].Meta().Name.String(), Equals, "primary_3")
}

func (s *testDBSuite4) TestAlterLock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	s.mustInterDirc(tk, c, "create causet t_index_lock (c1 int, c2 int, C3 int)")
	s.mustInterDirc(tk, c, "alter causet t_index_lock add index (c1, c2), dagger=none")
}

func (s *testDBSuite5) TestAddMultiDeferredCausetsIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)

	tk.MustInterDirc("drop database if exists milevadb;")
	tk.MustInterDirc("create database milevadb;")
	tk.MustInterDirc("use milevadb;")
	tk.MustInterDirc("create causet milevadb.test (a int auto_increment primary key, b int);")
	tk.MustInterDirc("insert milevadb.test values (1, 1);")
	tk.MustInterDirc("uFIDelate milevadb.test set b = b + 1 where a = 1;")
	tk.MustInterDirc("insert into milevadb.test values (2, 2);")
	// Test that the b value is nil.
	tk.MustInterDirc("insert into milevadb.test (a) values (3);")
	tk.MustInterDirc("insert into milevadb.test values (4, 4);")
	// Test that the b value is nil again.
	tk.MustInterDirc("insert into milevadb.test (a) values (5);")
	tk.MustInterDirc("insert milevadb.test values (6, 6);")
	tk.MustInterDirc("alter causet milevadb.test add index idx1 (a, b);")
	tk.MustInterDirc("admin check causet test")
}

func (s *testDBSuite6) TestAddMultiDeferredCausetsIndexClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_add_multi_defCaus_index_clustered;")
	tk.MustInterDirc("create database test_add_multi_defCaus_index_clustered;")
	tk.MustInterDirc("use test_add_multi_defCaus_index_clustered;")

	tk.MustInterDirc("set @@milevadb_enable_clustered_index = 1")
	tk.MustInterDirc("create causet t (a int, b varchar(10), c int, primary key (a, b));")
	tk.MustInterDirc("insert into t values (1, '1', 1), (2, '2', NULL), (3, '3', 3);")
	tk.MustInterDirc("create index idx on t (a, c);")

	tk.MustInterDirc("admin check index t idx;")
	tk.MustInterDirc("admin check causet t;")

	tk.MustInterDirc("insert into t values (5, '5', 5), (6, '6', NULL);")

	tk.MustInterDirc("admin check index t idx;")
	tk.MustInterDirc("admin check causet t;")
}

func (s *testDBSuite1) TestAddPrimaryKey1(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPlain,
		"create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, unique key(c1))", "primary")
}

func (s *testDBSuite2) TestAddPrimaryKey2(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPartition,
		`create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by range (c3) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "primary")
}

func (s *testDBSuite3) TestAddPrimaryKey3(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPartition,
		`create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by hash (c3) partitions 4;`, "primary")
}

func (s *testDBSuite4) TestAddPrimaryKey4(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPartition,
		`create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, key(c1))
			      partition by range defCausumns (c3) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "primary")
}

func (s *testDBSuite1) TestAddIndex1(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPlain,
		"create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))", "")
}

func (s *testDBSuite2) TestAddIndex2(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPartition,
		`create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by range (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func (s *testDBSuite3) TestAddIndex3(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPartition,
		`create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by hash (c1) partitions 4;`, "")
}

func (s *testDBSuite4) TestAddIndex4(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testPartition,
		`create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c1))
			      partition by range defCausumns (c1) (
			      partition p0 values less than (3440),
			      partition p1 values less than (61440),
			      partition p2 values less than (122880),
			      partition p3 values less than (204800),
			      partition p4 values less than maxvalue)`, "")
}

func (s *testDBSuite5) TestAddIndex5(c *C) {
	testAddIndex(c, s.causetstore, s.lease, testClusteredIndex,
		`create causet test_add_index (c1 bigint, c2 bigint, c3 bigint, primary key(c2, c3))`, "")
}

type testAddIndexType int8

const (
	testPlain testAddIndexType = iota
	testPartition
	testClusteredIndex
)

func testAddIndex(c *C, causetstore ekv.CausetStorage, lease time.Duration, tp testAddIndexType, createBlockALLEGROSQL, idxTp string) {
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test_db")
	switch tp {
	case testPartition:
		tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = '1';")
	case testClusteredIndex:
		tk.MustInterDirc("set @@milevadb_enable_clustered_index = 1")
	}
	tk.MustInterDirc("drop causet if exists test_add_index")
	tk.MustInterDirc(createBlockALLEGROSQL)

	done := make(chan error, 1)
	start := -10
	num := defaultBatchSize
	// first add some rows
	batchInsert(tk, "test_add_index", start, num)

	// Add some discrete rows.
	maxBatch := 20
	batchCnt := 100
	otherKeys := make([]int, 0, batchCnt*maxBatch)
	// Make sure there are no duplicate keys.
	base := defaultBatchSize * 20
	for i := 1; i < batchCnt; i++ {
		n := base + i*defaultBatchSize + i
		for j := 0; j < rand.Intn(maxBatch); j++ {
			n += j
			allegrosql := fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", n, n, n)
			tk.MustInterDirc(allegrosql)
			otherKeys = append(otherKeys, n)
		}
	}
	// Encounter the value of math.MaxInt64 in midbse of
	v := math.MaxInt64 - defaultBatchSize/2
	tk.MustInterDirc(fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", v, v, v))
	otherKeys = append(otherKeys, v)

	addIdxALLEGROSQL := fmt.Sprintf("alter causet test_add_index add %s key c3_index(c3)", idxTp)
	testdbsutil.StochastikInterDircInGoroutine(c, causetstore, addIdxALLEGROSQL, done)

	deletedKeys := make(map[int]struct{})

	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// When the server performance is particularly poor,
			// the adding index operation can not be completed.
			// So here is a limit to the number of rows inserted.
			if num > defaultBatchSize*10 {
				break
			}
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				deletedKeys[n] = struct{}{}
				allegrosql := fmt.Sprintf("delete from test_add_index where c1 = %d", n)
				tk.MustInterDirc(allegrosql)
				allegrosql = fmt.Sprintf("insert into test_add_index values (%d, %d, %d)", i, i, i)
				tk.MustInterDirc(allegrosql)
			}
			num += step
		}
	}

	// get exists keys
	keys := make([]int, 0, num)
	for i := start; i < num; i++ {
		if _, ok := deletedKeys[i]; ok {
			continue
		}
		keys = append(keys, i)
	}
	keys = append(keys, otherKeys...)

	// test index key
	expectedRows := make([][]interface{}, 0, len(keys))
	for _, key := range keys {
		expectedRows = append(expectedRows, []interface{}{key})
	}
	rows := tk.MustQuery(fmt.Sprintf("select c1 from test_add_index where c3 >= %d order by c1", start)).Rows()
	matchRows(c, rows, expectedRows)

	tk.MustInterDirc("admin check causet test_add_index")
	if tp == testPartition {
		return
	}

	// TODO: Support explain in future.
	// rows := s.mustQuery(c, "explain select c1 from test_add_index where c3 >= 100")

	// ay := dumpRows(c, rows)
	// c.Assert(strings.Contains(fmt.Sprintf("%v", ay), "c3_index"), IsTrue)

	// get all event handles
	ctx := tk.Se.(stochastikctx.Context)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	t := testGetBlockByName(c, ctx, "test_db", "test_add_index")
	handles := ekv.NewHandleMap()
	startKey := t.RecordKey(ekv.IntHandle(math.MinInt64))
	err := t.IterRecords(ctx, startKey, t.DefCauss(),
		func(h ekv.Handle, data []types.Causet, defcaus []*causet.DeferredCauset) (bool, error) {
			handles.Set(h, struct{}{})
			return true, nil
		})
	c.Assert(err, IsNil)

	// check in index
	var nidx causet.Index
	idxName := "c3_index"
	if len(idxTp) != 0 {
		idxName = "primary"
	}
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == idxName {
			nidx = tidx
			break
		}
	}
	// Make sure there is index with name c3_index.
	c.Assert(nidx, NotNil)
	c.Assert(nidx.Meta().ID, Greater, int64(0))
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	txn.Rollback()

	c.Assert(ctx.NewTxn(context.Background()), IsNil)

	it, err := nidx.SeekFirst(txn)
	c.Assert(err, IsNil)
	defer it.Close()

	for {
		_, h, err := it.Next()
		if terror.ErrorEqual(err, io.EOF) {
			break
		}

		c.Assert(err, IsNil)
		_, ok := handles.Get(h)
		c.Assert(ok, IsTrue)
		handles.Delete(h)
	}
	c.Assert(handles.Len(), Equals, 0)
	tk.MustInterDirc("drop causet test_add_index")
}

// TestCancelAddBlockAndDropBlockPartition tests cancel dbs job which type is add/drop causet partition.
func (s *testDBSuite1) TestCancelAddBlockAndDropBlockPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "create database if not exists test_partition_block")
	s.mustInterDirc(tk, c, "use test_partition_block")
	s.mustInterDirc(tk, c, "drop causet if exists t_part")
	s.mustInterDirc(tk, c, `create causet t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
	);`)
	defer s.mustInterDirc(tk, c, "drop causet t_part;")
	base := 10
	for i := 0; i < base; i++ {
		s.mustInterDirc(tk, c, "insert into t_part values (?)", i)
	}

	testCases := []struct {
		action         perceptron.CausetActionType
		jobState       perceptron.JobState
		JobSchemaState perceptron.SchemaState
		cancelSucc     bool
	}{
		{perceptron.CausetActionAddBlockPartition, perceptron.JobStateNone, perceptron.StateNone, true},
		{perceptron.CausetActionDropBlockPartition, perceptron.JobStateNone, perceptron.StateNone, true},
		// Add causet partition now can be cancelled in ReplicaOnly state.
		{perceptron.CausetActionAddBlockPartition, perceptron.JobStateRunning, perceptron.StateReplicaOnly, true},
	}
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	testCase := &testCases[0]
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == testCase.action && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.Background())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}
	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)

	var err error
	allegrosql := ""
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.action == perceptron.CausetActionAddBlockPartition {
			allegrosql = `alter causet t_part add partition (
				partition p2 values less than (30)
				);`
		} else if testCase.action == perceptron.CausetActionDropBlockPartition {
			allegrosql = "alter causet t_part drop partition p1;"
		}
		_, err = tk.InterDirc(allegrosql)
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
			s.mustInterDirc(tk, c, "insert into t_part values (?)", i+base)

			ctx := s.s.(stochastikctx.Context)
			is := petri.GetPetri(ctx).SchemaReplicant()
			tbl, err := is.BlockByName(perceptron.NewCIStr("test_partition_block"), perceptron.NewCIStr("t_part"))
			c.Assert(err, IsNil)
			partitionInfo := tbl.Meta().GetPartitionInfo()
			c.Assert(partitionInfo, NotNil)
			c.Assert(len(partitionInfo.AddingDefinitions), Equals, 0)
		} else {
			c.Assert(err, IsNil, Commentf("err:%v", err))
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDBSJob.GenWithStackByArgs(jobID).Error())
			_, err = tk.InterDirc("insert into t_part values (?)", i)
			c.Assert(err, NotNil)
		}
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
}

func (s *testDBSuite1) TestDropPrimaryKey(c *C) {
	idxName := "primary"
	createALLEGROSQL := "create causet test_drop_index (c1 int, c2 int, c3 int, unique key(c1), primary key(c3))"
	dropIdxALLEGROSQL := "alter causet test_drop_index drop primary key;"
	testDropIndex(c, s.causetstore, s.lease, createALLEGROSQL, dropIdxALLEGROSQL, idxName)
}

func (s *testDBSuite2) TestDropIndex(c *C) {
	idxName := "c3_index"
	createALLEGROSQL := "create causet test_drop_index (c1 int, c2 int, c3 int, unique key(c1), key c3_index(c3))"
	dropIdxALLEGROSQL := "alter causet test_drop_index drop index c3_index;"
	testDropIndex(c, s.causetstore, s.lease, createALLEGROSQL, dropIdxALLEGROSQL, idxName)
}

func testDropIndex(c *C, causetstore ekv.CausetStorage, lease time.Duration, createALLEGROSQL, dropIdxALLEGROSQL, idxName string) {
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists test_drop_index")
	tk.MustInterDirc(createALLEGROSQL)
	done := make(chan error, 1)
	tk.MustInterDirc("delete from test_drop_index")

	num := 100
	//  add some rows
	for i := 0; i < num; i++ {
		tk.MustInterDirc("insert into test_drop_index values (?, ?, ?)", i, i, i)
	}
	ctx := tk.Se.(stochastikctx.Context)
	t := testGetBlockByName(c, ctx, "test_db", "test_drop_index")
	var c3idx causet.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == idxName {
			c3idx = tidx
			break
		}
	}
	c.Assert(c3idx, NotNil)

	testdbsutil.StochastikInterDircInGoroutine(c, causetstore, dropIdxALLEGROSQL, done)

	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 5
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustInterDirc("uFIDelate test_drop_index set c2 = 1 where c1 = ?", n)
				tk.MustInterDirc("insert into test_drop_index values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	rows := tk.MustQuery("explain select c1 from test_drop_index where c3 >= 0")
	c.Assert(strings.Contains(fmt.Sprintf("%v", rows), idxName), IsFalse)

	// Check in index, it must be no index in KV.
	// Make sure there is no index with name c3_index.
	t = testGetBlockByName(c, ctx, "test_db", "test_drop_index")
	var nidx causet.Index
	for _, tidx := range t.Indices() {
		if tidx.Meta().Name.L == idxName {
			nidx = tidx
			break
		}
	}
	c.Assert(nidx, IsNil)

	idx := blocks.NewIndex(t.Meta().ID, t.Meta(), c3idx.Meta())
	checkDelRangeDone(c, ctx, idx)
	tk.MustInterDirc("drop causet test_drop_index")
}

// TestCancelDropDeferredCauset tests cancel dbs job which type is drop defCausumn.
func (s *testDBSuite3) TestCancelDropDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	s.mustInterDirc(tk, c, "drop causet if exists test_drop_defCausumn")
	s.mustInterDirc(tk, c, "create causet test_drop_defCausumn(c1 int, c2 int)")
	defer s.mustInterDirc(tk, c, "drop causet test_drop_defCausumn;")
	testCases := []struct {
		needAddDeferredCauset  bool
		jobState       perceptron.JobState
		JobSchemaState perceptron.SchemaState
		cancelSucc     bool
	}{
		{true, perceptron.JobStateNone, perceptron.StateNone, true},
		{false, perceptron.JobStateRunning, perceptron.StateWriteOnly, false},
		{true, perceptron.JobStateRunning, perceptron.StateDeleteOnly, false},
		{true, perceptron.JobStateRunning, perceptron.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionDropDeferredCauset && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}

	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	var err1 error
	var c3idx causet.Index
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddDeferredCauset {
			s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn add defCausumn c3 int")
			s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn add index idx_c3(c3)")
			tt := s.testGetBlock(c, "test_drop_defCausumn")
			for _, idx := range tt.Indices() {
				if strings.EqualFold(idx.Meta().Name.L, "idx_c3") {
					c3idx = idx
					break
				}
			}
		}
		_, err1 = tk.InterDirc("alter causet test_drop_defCausumn drop defCausumn c3")
		var defCaus1 *causet.DeferredCauset
		var idx1 causet.Index
		t := s.testGetBlock(c, "test_drop_defCausumn")
		for _, defCaus := range t.DefCauss() {
			if strings.EqualFold(defCaus.Name.L, "c3") {
				defCaus1 = defCaus
				break
			}
		}
		for _, idx := range t.Indices() {
			if strings.EqualFold(idx.Meta().Name.L, "idx_c3") {
				idx1 = idx
				break
			}
		}
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(defCaus1, NotNil)
			c.Assert(defCaus1.Name.L, Equals, "c3")
			c.Assert(idx1, NotNil)
			c.Assert(idx1.Meta().Name.L, Equals, "idx_c3")
			c.Assert(err1.Error(), Equals, "[dbs:8214]Cancelled DBS job")
		} else {
			c.Assert(defCaus1, IsNil)
			c.Assert(idx1, IsNil)
			c.Assert(err1, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDBSJob.GenWithStackByArgs(jobID).Error())
			// Check index is deleted
			ctx := s.s.(stochastikctx.Context)
			checkDelRangeDone(c, ctx, c3idx)
		}
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn add defCausumn c3 int")
	s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn drop defCausumn c3")
}

// TestCancelDropDeferredCausets tests cancel dbs job which type is drop multi-defCausumns.
func (s *testDBSuite3) TestCancelDropDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	s.mustInterDirc(tk, c, "drop causet if exists test_drop_defCausumn")
	s.mustInterDirc(tk, c, "create causet test_drop_defCausumn(c1 int, c2 int)")
	defer s.mustInterDirc(tk, c, "drop causet test_drop_defCausumn;")
	testCases := []struct {
		needAddDeferredCauset  bool
		jobState       perceptron.JobState
		JobSchemaState perceptron.SchemaState
		cancelSucc     bool
	}{
		{true, perceptron.JobStateNone, perceptron.StateNone, true},
		{false, perceptron.JobStateRunning, perceptron.StateWriteOnly, false},
		{true, perceptron.JobStateRunning, perceptron.StateDeleteOnly, false},
		{true, perceptron.JobStateRunning, perceptron.StateDeleteReorganization, false},
	}
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	var jobID int64
	testCase := &testCases[0]
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type == perceptron.CausetActionDropDeferredCausets && job.State == testCase.jobState && job.SchemaState == testCase.JobSchemaState {
			jobIDs := []int64{job.ID}
			jobID = job.ID
			hookCtx := mock.NewContext()
			hookCtx.CausetStore = s.causetstore
			err := hookCtx.NewTxn(context.TODO())
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			txn, err := hookCtx.Txn(true)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			errs, err := admin.CancelJobs(txn, jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return
			}
			if errs[0] != nil {
				checkErr = errors.Trace(errs[0])
				return
			}
			checkErr = txn.Commit(context.Background())
		}
	}

	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	var err1 error
	var c3idx causet.Index
	for i := range testCases {
		testCase = &testCases[i]
		if testCase.needAddDeferredCauset {
			s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn add defCausumn c3 int, add defCausumn c4 int")
			s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn add index idx_c3(c3)")
			tt := s.testGetBlock(c, "test_drop_defCausumn")
			for _, idx := range tt.Indices() {
				if strings.EqualFold(idx.Meta().Name.L, "idx_c3") {
					c3idx = idx
					break
				}
			}
		}
		_, err1 = tk.InterDirc("alter causet test_drop_defCausumn drop defCausumn c3, drop defCausumn c4")
		t := s.testGetBlock(c, "test_drop_defCausumn")
		defCaus3 := causet.FindDefCaus(t.DefCauss(), "c3")
		defCaus4 := causet.FindDefCaus(t.DefCauss(), "c4")
		var idx3 causet.Index
		for _, idx := range t.Indices() {
			if strings.EqualFold(idx.Meta().Name.L, "idx_c3") {
				idx3 = idx
				break
			}
		}
		if testCase.cancelSucc {
			c.Assert(checkErr, IsNil)
			c.Assert(defCaus3, NotNil)
			c.Assert(defCaus4, NotNil)
			c.Assert(idx3, NotNil)
			c.Assert(defCaus3.Name.L, Equals, "c3")
			c.Assert(defCaus4.Name.L, Equals, "c4")
			c.Assert(idx3.Meta().Name.L, Equals, "idx_c3")
			c.Assert(err1.Error(), Equals, "[dbs:8214]Cancelled DBS job")
		} else {
			c.Assert(defCaus3, IsNil)
			c.Assert(defCaus4, IsNil)
			c.Assert(idx3, IsNil)
			c.Assert(err1, IsNil)
			c.Assert(checkErr, NotNil)
			c.Assert(checkErr.Error(), Equals, admin.ErrCannotCancelDBSJob.GenWithStackByArgs(jobID).Error())
			// Check index is deleted
			ctx := s.s.(stochastikctx.Context)
			checkDelRangeDone(c, ctx, c3idx)
		}
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn add defCausumn c3 int, add defCausumn c4 int")
	s.mustInterDirc(tk, c, "alter causet test_drop_defCausumn drop defCausumn c3, drop defCausumn c4")
}

func checkDelRangeDone(c *C, ctx stochastikctx.Context, idx causet.Index) {
	startTime := time.Now()
	f := func() map[int64]struct{} {
		handles := make(map[int64]struct{})

		c.Assert(ctx.NewTxn(context.Background()), IsNil)
		txn, err := ctx.Txn(true)
		c.Assert(err, IsNil)
		defer txn.Rollback()

		txn, err = ctx.Txn(true)
		c.Assert(err, IsNil)
		it, err := idx.SeekFirst(txn)
		c.Assert(err, IsNil)
		defer it.Close()

		for {
			_, h, err := it.Next()
			if terror.ErrorEqual(err, io.EOF) {
				break
			}

			c.Assert(err, IsNil)
			handles[h.IntValue()] = struct{}{}
		}
		return handles
	}

	var handles map[int64]struct{}
	for i := 0; i < waitForCleanDataRound; i++ {
		handles = f()
		if len(handles) != 0 {
			time.Sleep(waitForCleanDataInterval)
		} else {
			break
		}
	}
	c.Assert(handles, HasLen, 0, Commentf("take time %v", time.Since(startTime)))
}

func (s *testDBSuite5) TestAlterPrimaryKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet test_add_pk(a int, b int unsigned , c varchar(255) default 'abc', d int as (a+b), e int as (a+1) stored, index idx(b))")
	defer tk.MustInterDirc("drop causet test_add_pk")

	// for generated defCausumns
	tk.MustGetErrCode("alter causet test_add_pk add primary key(d);", errno.ErrUnsupportedOnGeneratedDeferredCauset)
	// The primary key name is the same as the existing index name.
	tk.MustInterDirc("alter causet test_add_pk add primary key idx(e)")
	tk.MustInterDirc("drop index `primary` on test_add_pk")

	// for describing causet
	tk.MustInterDirc("create causet test_add_pk1(a int, index idx(a))")
	tk.MustQuery("desc test_add_pk1").Check(solitonutil.RowsWithSep(",", `a,int(11),YES,MUL,<nil>,`))
	tk.MustInterDirc("alter causet test_add_pk1 add primary key idx(a)")
	tk.MustQuery("desc test_add_pk1").Check(solitonutil.RowsWithSep(",", `a,int(11),NO,PRI,<nil>,`))
	tk.MustInterDirc("alter causet test_add_pk1 drop primary key")
	tk.MustQuery("desc test_add_pk1").Check(solitonutil.RowsWithSep(",", `a,int(11),NO,MUL,<nil>,`))
	tk.MustInterDirc("create causet test_add_pk2(a int, b int, index idx(a))")
	tk.MustInterDirc("alter causet test_add_pk2 add primary key idx(a, b)")
	tk.MustQuery("desc test_add_pk2").Check(solitonutil.RowsWithSep(",", ""+
		"a int(11) NO PRI <nil> ]\n"+
		"[b int(11) NO PRI <nil> "))
	tk.MustQuery("show create causet test_add_pk2").Check(solitonutil.RowsWithSep("|", ""+
		"test_add_pk2 CREATE TABLE `test_add_pk2` (\n"+
		"  `a` int(11) NOT NULL,\n"+
		"  `b` int(11) NOT NULL,\n"+
		"  KEY `idx` (`a`),\n"+
		"  PRIMARY KEY (`a`,`b`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("alter causet test_add_pk2 drop primary key")
	tk.MustQuery("desc test_add_pk2").Check(solitonutil.RowsWithSep(",", ""+
		"a int(11) NO MUL <nil> ]\n"+
		"[b int(11) NO  <nil> "))

	// Check if the primary key exists before checking the causet's pkIsHandle.
	tk.MustGetErrCode("alter causet test_add_pk drop primary key", errno.ErrCantDropFieldOrKey)

	// for the limit of name
	validName := strings.Repeat("a", allegrosql.MaxIndexIdentifierLen)
	invalidName := strings.Repeat("b", allegrosql.MaxIndexIdentifierLen+1)
	tk.MustGetErrCode("alter causet test_add_pk add primary key "+invalidName+"(a)", errno.ErrTooLongIdent)
	// for valid name
	tk.MustInterDirc("alter causet test_add_pk add primary key " + validName + "(a)")
	// for multiple primary key
	tk.MustGetErrCode("alter causet test_add_pk add primary key (a)", errno.ErrMultiplePriKey)
	tk.MustInterDirc("alter causet test_add_pk drop primary key")
	// for not existing primary key
	tk.MustGetErrCode("alter causet test_add_pk drop primary key", errno.ErrCantDropFieldOrKey)
	tk.MustGetErrCode("drop index `primary` on test_add_pk", errno.ErrCantDropFieldOrKey)

	// for too many key parts specified
	tk.MustGetErrCode("alter causet test_add_pk add primary key idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);",
		errno.ErrTooManyKeyParts)

	// for the limit of comment's length
	validComment := "'" + strings.Repeat("a", dbs.MaxCommentLength) + "'"
	invalidComment := "'" + strings.Repeat("b", dbs.MaxCommentLength+1) + "'"
	tk.MustGetErrCode("alter causet test_add_pk add primary key(a) comment "+invalidComment, errno.ErrTooLongIndexComment)
	// for empty sql_mode
	r := tk.MustQuery("select @@sql_mode")
	sqlMode := r.Rows()[0][0].(string)
	tk.MustInterDirc("set @@sql_mode=''")
	tk.MustInterDirc("alter causet test_add_pk add primary key(a) comment " + invalidComment)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|1688|Comment for index 'PRIMARY' is too long (max = 1024)"))
	tk.MustInterDirc("set @@sql_mode= '" + sqlMode + "'")
	tk.MustInterDirc("alter causet test_add_pk drop primary key")
	// for valid comment
	tk.MustInterDirc("alter causet test_add_pk add primary key(a, b, c) comment " + validComment)
	ctx := tk.Se.(stochastikctx.Context)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)
	t := testGetBlockByName(c, ctx, "test", "test_add_pk")
	defCaus1Flag := t.DefCauss()[0].Flag
	defCaus2Flag := t.DefCauss()[1].Flag
	defCaus3Flag := t.DefCauss()[2].Flag
	c.Assert(allegrosql.HasNotNullFlag(defCaus1Flag) && !allegrosql.HasPreventNullInsertFlag(defCaus1Flag), IsTrue)
	c.Assert(allegrosql.HasNotNullFlag(defCaus2Flag) && !allegrosql.HasPreventNullInsertFlag(defCaus2Flag) && allegrosql.HasUnsignedFlag(defCaus2Flag), IsTrue)
	c.Assert(allegrosql.HasNotNullFlag(defCaus3Flag) && !allegrosql.HasPreventNullInsertFlag(defCaus3Flag) && !allegrosql.HasNoDefaultValueFlag(defCaus3Flag), IsTrue)
	tk.MustInterDirc("alter causet test_add_pk drop primary key")

	// for null values in primary key
	tk.MustInterDirc("drop causet test_add_pk")
	tk.MustInterDirc("create causet test_add_pk(a int, b int unsigned , c varchar(255) default 'abc', index idx(b))")
	tk.MustInterDirc("insert into test_add_pk set a = 0, b = 0, c = 0")
	tk.MustInterDirc("insert into test_add_pk set a = 1")
	tk.MustGetErrCode("alter causet test_add_pk add primary key (b)", errno.ErrInvalidUseOfNull)
	tk.MustInterDirc("insert into test_add_pk set a = 2, b = 2")
	tk.MustGetErrCode("alter causet test_add_pk add primary key (a, b)", errno.ErrInvalidUseOfNull)
	tk.MustInterDirc("insert into test_add_pk set a = 3, c = 3")
	tk.MustGetErrCode("alter causet test_add_pk add primary key (c, b, a)", errno.ErrInvalidUseOfNull)
}

func (s *testDBSuite4) TestAddIndexWithDupDefCauss(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	err1 := schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs("b")
	err2 := schemareplicant.ErrDeferredCausetExists.GenWithStackByArgs("B")

	tk.MustInterDirc("create causet test_add_index_with_dup (a int, b int)")
	_, err := tk.InterDirc("create index c on test_add_index_with_dup(b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.InterDirc("create index c on test_add_index_with_dup(b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.InterDirc("alter causet test_add_index_with_dup add index c (b, a, b)")
	c.Check(errors.Cause(err1).(*terror.Error).Equal(err), Equals, true)

	_, err = tk.InterDirc("alter causet test_add_index_with_dup add index c (b, a, B)")
	c.Check(errors.Cause(err2).(*terror.Error).Equal(err), Equals, true)

	tk.MustInterDirc("drop causet test_add_index_with_dup")
}

// checkGlobalIndexRow reads one record from global index and check. Only support int handle.
func checkGlobalIndexRow(c *C, ctx stochastikctx.Context, tblInfo *perceptron.BlockInfo, indexInfo *perceptron.IndexInfo,
	pid int64, idxVals []types.Causet, rowVals []types.Causet) {
	ctx.NewTxn(context.Background())
	txn, err := ctx.Txn(true)
	sc := ctx.GetStochastikVars().StmtCtx
	c.Assert(err, IsNil)

	tblDefCausMap := make(map[int64]*types.FieldType, len(tblInfo.DeferredCausets))
	for _, defCaus := range tblInfo.DeferredCausets {
		tblDefCausMap[defCaus.ID] = &defCaus.FieldType
	}
	idxDefCausInfos := make([]rowcodec.DefCausInfo, 0, len(indexInfo.DeferredCausets))
	for _, idxDefCaus := range indexInfo.DeferredCausets {
		defCaus := tblInfo.DeferredCausets[idxDefCaus.Offset]
		idxDefCausInfos = append(idxDefCausInfos, rowcodec.DefCausInfo{
			ID:         defCaus.ID,
			IsPKHandle: tblInfo.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag),
			Ft:         rowcodec.FieldTypeFromPerceptronDeferredCauset(defCaus),
		})
	}

	// Check local index entry does not exist.
	localPrefix := blockcodec.EncodeBlockIndexPrefix(pid, indexInfo.ID)
	it, err := txn.Iter(localPrefix, nil)
	c.Assert(err, IsNil)
	// no local index entry.
	c.Assert(it.Valid() && it.Key().HasPrefix(localPrefix), IsFalse)
	it.Close()

	// Check global index entry.
	encodedValue, err := codec.EncodeKey(sc, nil, idxVals...)
	c.Assert(err, IsNil)
	key := blockcodec.EncodeIndexSeekKey(tblInfo.ID, indexInfo.ID, encodedValue)
	c.Assert(err, IsNil)
	value, err := txn.Get(context.Background(), key)
	c.Assert(err, IsNil)
	defCausVals, err := blockcodec.DecodeIndexKV(key, value, len(indexInfo.DeferredCausets),
		blockcodec.HandleDefault, idxDefCausInfos)
	c.Assert(err, IsNil)
	c.Assert(defCausVals, HasLen, len(idxVals)+2)
	for i, val := range idxVals {
		_, d, err := codec.DecodeOne(defCausVals[i])
		c.Assert(err, IsNil)
		c.Assert(d, DeepEquals, val)
	}
	_, d, err := codec.DecodeOne(defCausVals[len(idxVals)+1]) //pid
	c.Assert(err, IsNil)
	c.Assert(d.GetInt64(), Equals, pid)

	_, d, err = codec.DecodeOne(defCausVals[len(idxVals)]) //handle
	c.Assert(err, IsNil)
	h := ekv.IntHandle(d.GetInt64())
	rowKey := blockcodec.EncodeRowKey(pid, h.Encoded())
	rowValue, err := txn.Get(context.Background(), rowKey)
	c.Assert(err, IsNil)
	rowValueCausets, err := blockcodec.DecodeRowToCausetMap(rowValue, tblDefCausMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(rowValueCausets, NotNil)
	for i, val := range rowVals {
		c.Assert(rowValueCausets[tblInfo.DeferredCausets[i].ID], DeepEquals, val)
	}
}

func (s *testSerialDBSuite) TestAddGlobalIndex(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
		conf.EnableGlobalIndex = true
	})
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet test_t1 (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustInterDirc("insert test_t1 values (1, 1)")
	tk.MustInterDirc("alter causet test_t1 add unique index p_a (a);")
	tk.MustInterDirc("insert test_t1 values (2, 11)")
	t := s.testGetBlock(c, "test_t1")
	tblInfo := t.Meta()
	indexInfo := tblInfo.FindIndexByName("p_a")
	c.Assert(indexInfo, NotNil)
	c.Assert(indexInfo.Global, IsTrue)

	ctx := s.s.(stochastikctx.Context)
	ctx.NewTxn(context.Background())
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)

	// check event 1
	pid := tblInfo.Partition.Definitions[0].ID
	idxVals := []types.Causet{types.NewCauset(1)}
	rowVals := []types.Causet{types.NewCauset(1), types.NewCauset(1)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)

	// check event 2
	pid = tblInfo.Partition.Definitions[1].ID
	idxVals = []types.Causet{types.NewCauset(2)}
	rowVals = []types.Causet{types.NewCauset(2), types.NewCauset(11)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)
	txn.Commit(context.Background())

	// Test add global Primary Key index
	tk.MustInterDirc("create causet test_t2 (a int, b int) partition by range (b)" +
		" (partition p0 values less than (10), " +
		"  partition p1 values less than (maxvalue));")
	tk.MustInterDirc("insert test_t2 values (1, 1)")
	tk.MustInterDirc("alter causet test_t2 add primary key (a);")
	tk.MustInterDirc("insert test_t2 values (2, 11)")
	t = s.testGetBlock(c, "test_t2")
	tblInfo = t.Meta()
	indexInfo = t.Meta().FindIndexByName("primary")
	c.Assert(indexInfo, NotNil)
	c.Assert(indexInfo.Global, IsTrue)

	ctx.NewTxn(context.Background())
	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)

	// check event 1
	pid = tblInfo.Partition.Definitions[0].ID
	idxVals = []types.Causet{types.NewCauset(1)}
	rowVals = []types.Causet{types.NewCauset(1), types.NewCauset(1)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)

	// check event 2
	pid = tblInfo.Partition.Definitions[1].ID
	idxVals = []types.Causet{types.NewCauset(2)}
	rowVals = []types.Causet{types.NewCauset(2), types.NewCauset(11)}
	checkGlobalIndexRow(c, ctx, tblInfo, indexInfo, pid, idxVals, rowVals)

	txn.Commit(context.Background())
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = false
	})
}

func (s *testDBSuite) showDeferredCausets(tk *testkit.TestKit, c *C, blockName string) [][]interface{} {
	return s.mustQuery(tk, c, fmt.Sprintf("show defCausumns from %s", blockName))
}

func (s *testDBSuite5) TestCreateIndexType(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	allegrosql := `CREATE TABLE test_index (
		price int(5) DEFAULT '0' NOT NULL,
		area varchar(40) DEFAULT '' NOT NULL,
		type varchar(40) DEFAULT '' NOT NULL,
		transityes set('a','b'),
		shopsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		schoolsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		petsyes enum('Y','N') DEFAULT 'Y' NOT NULL,
		KEY price (price,area,type,transityes,shopsyes,schoolsyes,petsyes));`
	tk.MustInterDirc(allegrosql)
}

func (s *testDBSuite1) TestDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("create causet t2 (c1 int, c2 int, c3 int)")
	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	s.testAddDeferredCauset(tk, c)
	s.testDropDeferredCauset(tk, c)
	tk.MustInterDirc("drop causet t2")
}

func stochastikInterDirc(c *C, s ekv.CausetStorage, allegrosql string) {
	se, err := stochastik.CreateStochastik4Test(s)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test_db")
	c.Assert(err, IsNil)
	rs, err := se.InterDircute(context.Background(), allegrosql)
	c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
	c.Assert(rs, IsNil)
	se.Close()
}

func (s *testDBSuite) testAddDeferredCauset(tk *testkit.TestKit, c *C) {
	done := make(chan error, 1)

	num := defaultBatchSize + 10
	// add some rows
	batchInsert(tk, "t2", 0, num)

	testdbsutil.StochastikInterDircInGoroutine(c, s.causetstore, "alter causet t2 add defCausumn c4 int default -1", done)

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustInterDirc("begin")
				tk.MustInterDirc("delete from t2 where c1 = ?", n)
				tk.MustInterDirc("commit")

				// Make sure that memex of insert and show use the same schemaReplicant.
				tk.MustInterDirc("begin")
				_, err := tk.InterDirc("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// if err is failed, the defCausumn number must be 4 now.
					values := s.showDeferredCausets(tk, c, "t2")
					c.Assert(values, HasLen, 4, Commentf("err:%v", errors.ErrorStack(err)))
				}
				tk.MustInterDirc("commit")
			}
			num += step
		}
	}

	// add data, here c4 must exist
	for i := num; i < num+step; i++ {
		tk.MustInterDirc("insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	rows := s.mustQuery(tk, c, "select count(c4) from t2")
	c.Assert(rows, HasLen, 1)
	c.Assert(rows[0], HasLen, 1)
	count, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(count, Greater, int64(0))

	rows = s.mustQuery(tk, c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})

	for i := num; i < num+step; i++ {
		rows = s.mustQuery(tk, c, "select c4 from t2 where c4 = ?", i)
		matchRows(c, rows, [][]interface{}{{i}})
	}

	ctx := s.s.(stochastikctx.Context)
	t := s.testGetBlock(c, "t2")
	i := 0
	j := 0
	ctx.NewTxn(context.Background())
	defer func() {
		if txn, err1 := ctx.Txn(true); err1 == nil {
			txn.Rollback()
		}
	}()
	err = t.IterRecords(ctx, t.FirstKey(), t.DefCauss(),
		func(_ ekv.Handle, data []types.Causet, defcaus []*causet.DeferredCauset) (bool, error) {
			i++
			// c4 must be -1 or > 0
			v, err1 := data[3].ToInt64(ctx.GetStochastikVars().StmtCtx)
			c.Assert(err1, IsNil)
			if v == -1 {
				j++
			} else {
				c.Assert(v, Greater, int64(0))
			}
			return true, nil
		})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int(count))
	c.Assert(i, LessEqual, num+step)
	c.Assert(j, Equals, int(count)-step)

	// for modifying defCausumns after adding defCausumns
	tk.MustInterDirc("alter causet t2 modify c4 int default 11")
	for i := num + step; i < num+step+10; i++ {
		s.mustInterDirc(tk, c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}
	rows = s.mustQuery(tk, c, "select count(c4) from t2 where c4 = -1")
	matchRows(c, rows, [][]interface{}{{count - int64(step)}})

	// add timestamp type defCausumn
	s.mustInterDirc(tk, c, "create causet test_on_uFIDelate_c (c1 int, c2 timestamp);")
	defer tk.MustInterDirc("drop causet test_on_uFIDelate_c;")
	s.mustInterDirc(tk, c, "alter causet test_on_uFIDelate_c add defCausumn c3 timestamp null default '2020-02-11' on uFIDelate current_timestamp;")
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("test_on_uFIDelate_c"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	defCausC := tblInfo.DeferredCausets[2]
	c.Assert(defCausC.Tp, Equals, allegrosql.TypeTimestamp)
	hasNotNull := allegrosql.HasNotNullFlag(defCausC.Flag)
	c.Assert(hasNotNull, IsFalse)
	// add datetime type defCausumn
	s.mustInterDirc(tk, c, "create causet test_on_uFIDelate_d (c1 int, c2 datetime);")
	defer tk.MustInterDirc("drop causet test_on_uFIDelate_d;")
	s.mustInterDirc(tk, c, "alter causet test_on_uFIDelate_d add defCausumn c3 datetime on uFIDelate current_timestamp;")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("test_on_uFIDelate_d"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	defCausC = tblInfo.DeferredCausets[2]
	c.Assert(defCausC.Tp, Equals, allegrosql.TypeDatetime)
	hasNotNull = allegrosql.HasNotNullFlag(defCausC.Flag)
	c.Assert(hasNotNull, IsFalse)

	// add year type defCausumn
	s.mustInterDirc(tk, c, "create causet test_on_uFIDelate_e (c1 int);")
	defer tk.MustInterDirc("drop causet test_on_uFIDelate_e;")
	s.mustInterDirc(tk, c, "insert into test_on_uFIDelate_e (c1) values (0);")
	s.mustInterDirc(tk, c, "alter causet test_on_uFIDelate_e add defCausumn c2 year not null;")
	tk.MustQuery("select c2 from test_on_uFIDelate_e").Check(testkit.Rows("0"))

	// test add unsupported constraint
	s.mustInterDirc(tk, c, "create causet t_add_unsupported_constraint (a int);")
	_, err = tk.InterDirc("ALTER TABLE t_add_unsupported_constraint ADD id int AUTO_INCREMENT;")
	c.Assert(err.Error(), Equals, "[dbs:8200]unsupported add defCausumn 'id' constraint AUTO_INCREMENT when altering 'test_db.t_add_unsupported_constraint'")
	_, err = tk.InterDirc("ALTER TABLE t_add_unsupported_constraint ADD id int KEY;")
	c.Assert(err.Error(), Equals, "[dbs:8200]unsupported add defCausumn 'id' constraint PRIMARY KEY when altering 'test_db.t_add_unsupported_constraint'")
	_, err = tk.InterDirc("ALTER TABLE t_add_unsupported_constraint ADD id int UNIQUE;")
	c.Assert(err.Error(), Equals, "[dbs:8200]unsupported add defCausumn 'id' constraint UNIQUE KEY when altering 'test_db.t_add_unsupported_constraint'")
}

func (s *testDBSuite) testDropDeferredCauset(tk *testkit.TestKit, c *C) {
	done := make(chan error, 1)
	s.mustInterDirc(tk, c, "delete from t2")

	num := 100
	// add some rows
	for i := 0; i < num; i++ {
		s.mustInterDirc(tk, c, "insert into t2 values (?, ?, ?, ?)", i, i, i, i)
	}

	// get c4 defCausumn id
	testdbsutil.StochastikInterDircInGoroutine(c, s.causetstore, "alter causet t2 drop defCausumn c4", done)

	ticker := time.NewTicker(s.lease / 2)
	defer ticker.Stop()
	step := 10
LOOP:
	for {
		select {
		case err := <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			// delete some rows, and add some data
			for i := num; i < num+step; i++ {
				// Make sure that memex of insert and show use the same schemaReplicant.
				tk.MustInterDirc("begin")
				_, err := tk.InterDirc("insert into t2 values (?, ?, ?)", i, i, i)
				if err != nil {
					// If executing is failed, the defCausumn number must be 4 now.
					values := s.showDeferredCausets(tk, c, "t2")
					c.Assert(values, HasLen, 4, Commentf("err:%v", errors.ErrorStack(err)))
				}
				tk.MustInterDirc("commit")
			}
			num += step
		}
	}

	// add data, here c4 must not exist
	for i := num; i < num+step; i++ {
		s.mustInterDirc(tk, c, "insert into t2 values (?, ?, ?)", i, i, i)
	}

	rows := s.mustQuery(tk, c, "select count(*) from t2")
	c.Assert(rows, HasLen, 1)
	c.Assert(rows[0], HasLen, 1)
	count, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(count, Greater, int64(0))
}

// TestDropDeferredCauset is for inserting value with a to-be-dropped defCausumn when do drop defCausumn.
// DeferredCauset info from schemaReplicant in build-insert-plan should be public only,
// otherwise they will not be consist with Block.DefCaus(), then the server will panic.
func (s *testDBSuite6) TestDropDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database drop_defCaus_db")
	tk.MustInterDirc("use drop_defCaus_db")
	num := 25
	multiDBS := make([]string, 0, num)
	allegrosql := "create causet t2 (c1 int, c2 int, c3 int, "
	for i := 4; i < 4+num; i++ {
		multiDBS = append(multiDBS, fmt.Sprintf("alter causet t2 drop defCausumn c%d", i))

		if i != 3+num {
			allegrosql += fmt.Sprintf("c%d int, ", i)
		} else {
			allegrosql += fmt.Sprintf("c%d int)", i)
		}
	}
	tk.MustInterDirc(allegrosql)
	dmlDone := make(chan error, num)
	dbsDone := make(chan error, num)

	testdbsutil.InterDircMultiALLEGROSQLInGoroutine(c, s.causetstore, "drop_defCaus_db", multiDBS, dbsDone)
	for i := 0; i < num; i++ {
		testdbsutil.InterDircMultiALLEGROSQLInGoroutine(c, s.causetstore, "drop_defCaus_db", []string{"insert into t2 set c1 = 1, c2 = 1, c3 = 1, c4 = 1"}, dmlDone)
	}
	for i := 0; i < num; i++ {
		select {
		case err := <-dbsDone:
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		}
	}

	// Test for drop partition causet defCausumn.
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int,b int) partition by hash(a) partitions 4;")
	_, err := tk.InterDirc("alter causet t1 drop defCausumn a")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[memex:1054]Unknown defCausumn 'a' in 'memex'")

	tk.MustInterDirc("drop database drop_defCaus_db")
}

func (s *testDBSuite4) TestChangeDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)

	s.mustInterDirc(tk, c, "create causet t3 (a int default '0', b varchar(10), d int not null default '0')")
	s.mustInterDirc(tk, c, "insert into t3 set b = 'a'")
	tk.MustQuery("select a from t3").Check(testkit.Rows("0"))
	s.mustInterDirc(tk, c, "alter causet t3 change a aa bigint")
	s.mustInterDirc(tk, c, "insert into t3 set b = 'b'")
	tk.MustQuery("select aa from t3").Check(testkit.Rows("0", "<nil>"))
	// for no default flag
	s.mustInterDirc(tk, c, "alter causet t3 change d dd bigint not null")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	defCausD := tblInfo.DeferredCausets[2]
	hasNoDefault := allegrosql.HasNoDefaultValueFlag(defCausD.Flag)
	c.Assert(hasNoDefault, IsTrue)
	// for the following definitions: 'not null', 'null', 'default value' and 'comment'
	s.mustInterDirc(tk, c, "alter causet t3 change b b varchar(20) null default 'c' comment 'my comment'")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	defCausB := tblInfo.DeferredCausets[1]
	c.Assert(defCausB.Comment, Equals, "my comment")
	hasNotNull := allegrosql.HasNotNullFlag(defCausB.Flag)
	c.Assert(hasNotNull, IsFalse)
	s.mustInterDirc(tk, c, "insert into t3 set aa = 3, dd = 5")
	tk.MustQuery("select b from t3").Check(testkit.Rows("a", "b", "c"))
	// for timestamp
	s.mustInterDirc(tk, c, "alter causet t3 add defCausumn c timestamp not null")
	s.mustInterDirc(tk, c, "alter causet t3 change c c timestamp null default '2020-02-11' comment 'defCaus c comment' on uFIDelate current_timestamp")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("t3"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	defCausC := tblInfo.DeferredCausets[3]
	c.Assert(defCausC.Comment, Equals, "defCaus c comment")
	hasNotNull = allegrosql.HasNotNullFlag(defCausC.Flag)
	c.Assert(hasNotNull, IsFalse)
	// for enum
	s.mustInterDirc(tk, c, "alter causet t3 add defCausumn en enum('a', 'b', 'c') not null default 'a'")

	// for failing tests
	allegrosql := "alter causet t3 change aa a bigint default ''"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidDefault)
	allegrosql = "alter causet t3 change a testx.t3.aa bigint"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongDBName)
	allegrosql = "alter causet t3 change t.a aa bigint"
	tk.MustGetErrCode(allegrosql, errno.ErrWrongBlockName)
	s.mustInterDirc(tk, c, "create causet t4 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustInterDirc("insert into t4(c2) values (null);")
	allegrosql = "alter causet t4 change c1 a1 int not null;"
	tk.MustGetErrCode(allegrosql, errno.ErrInvalidUseOfNull)
	allegrosql = "alter causet t4 change c2 a bigint not null;"
	tk.MustGetErrCode(allegrosql, allegrosql.WarnDataTruncated)
	allegrosql = "alter causet t3 modify en enum('a', 'z', 'b', 'c') not null default 'a'"
	tk.MustGetErrCode(allegrosql, errno.ErrUnsupportedDBSOperation)
	// Rename to an existing defCausumn.
	s.mustInterDirc(tk, c, "alter causet t3 add defCausumn a bigint")
	allegrosql = "alter causet t3 change aa a bigint"
	tk.MustGetErrCode(allegrosql, errno.ErrDupFieldName)

	tk.MustInterDirc("drop causet t3")
}

func (s *testDBSuite5) TestRenameDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)

	assertDefCausNames := func(blockName string, defCausNames ...string) {
		defcaus := s.testGetBlock(c, blockName).DefCauss()
		c.Assert(len(defcaus), Equals, len(defCausNames), Commentf("number of defCausumns mismatch"))
		for i := range defcaus {
			c.Assert(defcaus[i].Name.L, Equals, strings.ToLower(defCausNames[i]))
		}
	}

	s.mustInterDirc(tk, c, "create causet test_rename_defCausumn (id int not null primary key auto_increment, defCaus1 int)")
	s.mustInterDirc(tk, c, "alter causet test_rename_defCausumn rename defCausumn defCaus1 to defCaus1")
	assertDefCausNames("test_rename_defCausumn", "id", "defCaus1")
	s.mustInterDirc(tk, c, "alter causet test_rename_defCausumn rename defCausumn defCaus1 to defCaus2")
	assertDefCausNames("test_rename_defCausumn", "id", "defCaus2")

	// Test renaming non-exist defCausumns.
	tk.MustGetErrCode("alter causet test_rename_defCausumn rename defCausumn non_exist_defCaus to defCaus3", errno.ErrBadField)

	// Test renaming to an exist defCausumn.
	tk.MustGetErrCode("alter causet test_rename_defCausumn rename defCausumn defCaus2 to id", errno.ErrDupFieldName)

	// Test renaming the defCausumn with foreign key.
	tk.MustInterDirc("drop causet test_rename_defCausumn")
	tk.MustInterDirc("create causet test_rename_defCausumn_base (base int)")
	tk.MustInterDirc("create causet test_rename_defCausumn (defCaus int, foreign key (defCaus) references test_rename_defCausumn_base(base))")

	tk.MustGetErrCode("alter causet test_rename_defCausumn rename defCausumn defCaus to defCaus1", errno.ErrFKIncompatibleDeferredCausets)

	tk.MustInterDirc("drop causet test_rename_defCausumn_base")

	// Test renaming generated defCausumns.
	tk.MustInterDirc("drop causet test_rename_defCausumn")
	tk.MustInterDirc("create causet test_rename_defCausumn (id int, defCaus1 int generated always as (id + 1))")

	s.mustInterDirc(tk, c, "alter causet test_rename_defCausumn rename defCausumn defCaus1 to defCaus2")
	assertDefCausNames("test_rename_defCausumn", "id", "defCaus2")
	s.mustInterDirc(tk, c, "alter causet test_rename_defCausumn rename defCausumn defCaus2 to defCaus1")
	assertDefCausNames("test_rename_defCausumn", "id", "defCaus1")
	tk.MustGetErrCode("alter causet test_rename_defCausumn rename defCausumn id to id1", errno.ErrBadField)

	// Test renaming view defCausumns.
	tk.MustInterDirc("drop causet test_rename_defCausumn")
	s.mustInterDirc(tk, c, "create causet test_rename_defCausumn (id int, defCaus1 int)")
	s.mustInterDirc(tk, c, "create view test_rename_defCausumn_view as select * from test_rename_defCausumn")

	s.mustInterDirc(tk, c, "alter causet test_rename_defCausumn rename defCausumn defCaus1 to defCaus2")
	tk.MustGetErrCode("select * from test_rename_defCausumn_view", errno.ErrViewInvalid)

	s.mustInterDirc(tk, c, "drop view test_rename_defCausumn_view")
	tk.MustInterDirc("drop causet test_rename_defCausumn")
}

func (s *testDBSuite7) TestSelectInViewFromAnotherDB(c *C) {
	_, _ = s.s.InterDircute(context.Background(), "create database test_db2")
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("use test_db2")
	tk.MustInterDirc("create allegrosql security invoker view v as select * from " + s.schemaName + ".t")
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("select test_db2.v.a from test_db2.v")
}

func (s *testDBSuite) mustInterDirc(tk *testkit.TestKit, c *C, query string, args ...interface{}) {
	tk.MustInterDirc(query, args...)
}

func (s *testDBSuite) mustQuery(tk *testkit.TestKit, c *C, query string, args ...interface{}) [][]interface{} {
	r := tk.MustQuery(query, args...)
	return r.Rows()
}

func matchRows(c *C, rows [][]interface{}, expected [][]interface{}) {
	c.Assert(len(rows), Equals, len(expected), Commentf("got %v, expected %v", rows, expected))
	for i := range rows {
		match(c, rows[i], expected[i]...)
	}
}

func match(c *C, event []interface{}, expected ...interface{}) {
	c.Assert(len(event), Equals, len(expected))
	for i := range event {
		got := fmt.Sprintf("%v", event[i])
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}

// TestCreateBlockWithLike2 tests create causet with like when refer causet have non-public defCausumn/index.
func (s *testSerialDBSuite) TestCreateBlockWithLike2(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t1,t2;")
	defer tk.MustInterDirc("drop causet if exists t1,t2;")
	tk.MustInterDirc("create causet t1 (a int, b int, c int, index idx1(c));")

	tbl1 := testGetBlockByName(c, s.s, "test_db", "t1")
	doneCh := make(chan error, 2)
	hook := &dbs.TestDBSCallback{}
	var onceChecker sync.Map
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type != perceptron.CausetActionAddDeferredCauset && job.Type != perceptron.CausetActionDropDeferredCauset &&
			job.Type != perceptron.CausetActionAddDeferredCausets && job.Type != perceptron.CausetActionDropDeferredCausets &&
			job.Type != perceptron.CausetActionAddIndex && job.Type != perceptron.CausetActionDropIndex {
			return
		}
		if job.BlockID != tbl1.Meta().ID {
			return
		}

		if job.SchemaState == perceptron.StateDeleteOnly {
			if _, ok := onceChecker.Load(job.ID); ok {
				return
			}

			onceChecker.CausetStore(job.ID, true)
			go backgroundInterDirc(s.causetstore, "create causet t2 like t1", doneCh)
		}
	}
	originalHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)

	// create causet when refer causet add defCausumn
	tk.MustInterDirc("alter causet t1 add defCausumn d int")
	checkTbl2 := func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		tk.MustInterDirc("alter causet t2 add defCausumn e int")
		t2Info := testGetBlockByName(c, s.s, "test_db", "t2")
		c.Assert(len(t2Info.Meta().DeferredCausets), Equals, len(t2Info.DefCauss()))
	}
	checkTbl2()

	// create causet when refer causet drop defCausumn
	tk.MustInterDirc("drop causet t2;")
	tk.MustInterDirc("alter causet t1 drop defCausumn b;")
	checkTbl2()

	// create causet when refer causet add index
	tk.MustInterDirc("drop causet t2;")
	tk.MustInterDirc("alter causet t1 add index idx2(a);")
	checkTbl2 = func() {
		err := <-doneCh
		c.Assert(err, IsNil)
		tk.MustInterDirc("alter causet t2 add defCausumn e int")
		tbl2 := testGetBlockByName(c, s.s, "test_db", "t2")
		c.Assert(len(tbl2.Meta().DeferredCausets), Equals, len(tbl2.DefCauss()))

		for i := 0; i < len(tbl2.Meta().Indices); i++ {
			c.Assert(tbl2.Meta().Indices[i].State, Equals, perceptron.StatePublic)
		}
	}
	checkTbl2()

	// create causet when refer causet drop index.
	tk.MustInterDirc("drop causet t2;")
	tk.MustInterDirc("alter causet t1 drop index idx2;")
	checkTbl2()

	// Test for causet has tiflash  replica.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	tk.MustInterDirc("drop causet if exists t1,t2;")
	tk.MustInterDirc("create causet t1 (a int) partition by hash(a) partitions 2;")
	tk.MustInterDirc("alter causet t1 set tiflash replica 3 location labels 'a','b';")
	t1 := testGetBlockByName(c, s.s, "test_db", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 2)
	err := petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	t1 = testGetBlockByName(c, s.s, "test_db", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})

	tk.MustInterDirc("create causet t2 like t1")
	t2 := testGetBlockByName(c, s.s, "test_db", "t2")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)
	// Test for not affecting the original causet.
	t1 = testGetBlockByName(c, s.s, "test_db", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})
}

func (s *testSerialDBSuite) TestCreateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	tk.MustInterDirc("CREATE TABLE IF NOT EXISTS `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	defcaus := tbl.DefCauss()

	c.Assert(len(defcaus), Equals, 1)
	defCaus := defcaus[0]
	c.Assert(defCaus.Name.L, Equals, "a")
	d, ok := defCaus.DefaultValue.(string)
	c.Assert(ok, IsTrue)
	c.Assert(d, Equals, "2.0")

	tk.MustInterDirc("drop causet t")

	tk.MustGetErrCode("CREATE TABLE `t` (`a` int) DEFAULT CHARSET=abcdefg", errno.ErrUnknownCharacterSet)

	tk.MustInterDirc("CREATE TABLE `defCauslateTest` (`a` int, `b` varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_slovak_ci")
	expects := "defCauslateTest CREATE TABLE `defCauslateTest` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8_slovak_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_slovak_ci"
	tk.MustQuery("show create causet defCauslateTest").Check(testkit.Rows(expects))

	tk.MustGetErrCode("CREATE TABLE `defCauslateTest2` (`a` int) CHARSET utf8 COLLATE utf8mb4_unicode_ci", errno.ErrDefCauslationCharsetMismatch)
	tk.MustGetErrCode("CREATE TABLE `defCauslateTest3` (`a` int) COLLATE utf8mb4_unicode_ci CHARSET utf8", errno.ErrConflictingDeclarations)

	tk.MustInterDirc("CREATE TABLE `defCauslateTest4` (`a` int) COLLATE utf8_uniCOde_ci")
	expects = "defCauslateTest4 CREATE TABLE `defCauslateTest4` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	tk.MustQuery("show create causet defCauslateTest4").Check(testkit.Rows(expects))

	tk.MustInterDirc("create database test2 default charset utf8 defCauslate utf8_general_ci")
	tk.MustInterDirc("use test2")
	tk.MustInterDirc("create causet dbDefCauslateTest (a varchar(10))")
	expects = "dbDefCauslateTest CREATE TABLE `dbDefCauslateTest` (\n  `a` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
	tk.MustQuery("show create causet dbDefCauslateTest").Check(testkit.Rows(expects))

	// test for enum defCausumn
	tk.MustInterDirc("use test")
	failALLEGROSQL := "create causet t_enum (a enum('e','e'));"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tk = testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	failALLEGROSQL = "create causet t_enum (a enum('e','E')) charset=utf8 defCauslate=utf8_general_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	failALLEGROSQL = "create causet t_enum (a enum('abc','Abc')) charset=utf8 defCauslate=utf8_general_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	failALLEGROSQL = "create causet t_enum (a enum('e','E')) charset=utf8 defCauslate=utf8_unicode_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	failALLEGROSQL = "create causet t_enum (a enum('ss','ß')) charset=utf8 defCauslate=utf8_unicode_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	// test for set defCausumn
	failALLEGROSQL = "create causet t_enum (a set('e','e'));"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	failALLEGROSQL = "create causet t_enum (a set('e','E')) charset=utf8 defCauslate=utf8_general_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	failALLEGROSQL = "create causet t_enum (a set('abc','Abc')) charset=utf8 defCauslate=utf8_general_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	_, err = tk.InterDirc("create causet t_enum (a enum('B','b')) charset=utf8 defCauslate=utf8_general_ci;")
	c.Assert(err.Error(), Equals, "[types:1291]DeferredCauset 'a' has duplicated value 'b' in ENUM")
	failALLEGROSQL = "create causet t_enum (a set('e','E')) charset=utf8 defCauslate=utf8_unicode_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	failALLEGROSQL = "create causet t_enum (a set('ss','ß')) charset=utf8 defCauslate=utf8_unicode_ci;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrDuplicatedValueInType)
	_, err = tk.InterDirc("create causet t_enum (a enum('ss','ß')) charset=utf8 defCauslate=utf8_unicode_ci;")
	c.Assert(err.Error(), Equals, "[types:1291]DeferredCauset 'a' has duplicated value 'ß' in ENUM")

	// test for causet option "union" not supported
	tk.MustInterDirc("use test")
	tk.MustInterDirc("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustInterDirc("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	failALLEGROSQL = "CREATE TABLE z (a INT) ENGINE = MERGE UNION = (x, y);"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrBlockOptionUnionUnsupported)
	failALLEGROSQL = "ALTER TABLE x UNION = (y);"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrBlockOptionUnionUnsupported)
	tk.MustInterDirc("drop causet x;")
	tk.MustInterDirc("drop causet y;")

	// test for causet option "insert method" not supported
	tk.MustInterDirc("use test")
	tk.MustInterDirc("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustInterDirc("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	failALLEGROSQL = "CREATE TABLE z (a INT) ENGINE = MERGE INSERT_METHOD=LAST;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrBlockOptionInsertMethodUnsupported)
	failALLEGROSQL = "ALTER TABLE x INSERT_METHOD=LAST;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrBlockOptionInsertMethodUnsupported)
	tk.MustInterDirc("drop causet x;")
	tk.MustInterDirc("drop causet y;")
}

func (s *testSerialDBSuite) TestRepairBlock(c *C) {
	// TODO: When AlterPrimaryKey is false, this test fails. Fix it later.
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = true
	})
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/repairFetchCreateBlock", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/repairFetchCreateBlock"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t, other_block, origin")

	// Test repair causet when MilevaDB is not in repair mode.
	tk.MustInterDirc("CREATE TABLE t (a int primary key, b varchar(10));")
	_, err := tk.InterDirc("admin repair causet t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: MilevaDB is not in REPAIR MODE")

	// Test repair causet when the repaired list is empty.
	petriutil.RepairInfo.SetRepairMode(true)
	_, err = tk.InterDirc("admin repair causet t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: repair list is empty")

	// Test repair causet when it's database isn't in repairInfo.
	petriutil.RepairInfo.SetRepairBlockList([]string{"test.other_block"})
	_, err = tk.InterDirc("admin repair causet t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: database test is not in repair")

	// Test repair causet when the causet isn't in repairInfo.
	tk.MustInterDirc("CREATE TABLE other_block (a int, b varchar(1), key using hash(b));")
	_, err = tk.InterDirc("admin repair causet t CREATE TABLE t (a float primary key, b varchar(5));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: causet t is not in repair")

	// Test user can't access to the repaired causet.
	_, err = tk.InterDirc("select * from other_block")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.other_block' doesn't exist")

	// Test create memex use the same name with what is in repaired.
	_, err = tk.InterDirc("CREATE TABLE other_block (a int);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:1103]Incorrect causet name 'other_block'%!(EXTRA string=this causet is in repair)")

	// Test defCausumn lost in repair causet.
	_, err = tk.InterDirc("admin repair causet other_block CREATE TABLE other_block (a int, c char(1));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: DeferredCauset c has lost")

	// Test defCausumn type should be the same.
	_, err = tk.InterDirc("admin repair causet other_block CREATE TABLE other_block (a bigint, b varchar(1), key using hash(b));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: DeferredCauset a type should be the same")

	// Test index lost in repair causet.
	_, err = tk.InterDirc("admin repair causet other_block CREATE TABLE other_block (a int unique);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: Index a has lost")

	// Test index type should be the same.
	_, err = tk.InterDirc("admin repair causet other_block CREATE TABLE other_block (a int, b varchar(2) unique)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: Index b type should be the same")

	// Test sub create memex in repair memex with the same name.
	_, err = tk.InterDirc("admin repair causet other_block CREATE TABLE other_block (a int);")
	c.Assert(err, IsNil)

	// Test whether repair causet name is case sensitive.
	petriutil.RepairInfo.SetRepairMode(true)
	petriutil.RepairInfo.SetRepairBlockList([]string{"test.other_block2"})
	tk.MustInterDirc("CREATE TABLE otHer_tAblE2 (a int, b varchar(1));")
	_, err = tk.InterDirc("admin repair causet otHer_tAblE2 CREATE TABLE otHeR_tAbLe (a int, b varchar(2));")
	c.Assert(err, IsNil)
	repairBlock := testGetBlockByName(c, s.s, "test", "otHeR_tAbLe")
	c.Assert(repairBlock.Meta().Name.O, Equals, "otHeR_tAbLe")

	// Test memory and system database is not for repair.
	petriutil.RepairInfo.SetRepairMode(true)
	petriutil.RepairInfo.SetRepairBlockList([]string{"test.xxx"})
	_, err = tk.InterDirc("admin repair causet performance_schema.xxx CREATE TABLE yyy (a int);")
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: memory or system database is not for repair")

	// Test the repair detail.
	turnRepairModeAndInit(true)
	defer turnRepairModeAndInit(false)
	// Petri reload the blockInfo and add it into repairInfo.
	tk.MustInterDirc("CREATE TABLE origin (a int primary key auto_increment, b varchar(10), c int);")
	// Repaired blockInfo has been filtered by `petri.SchemaReplicant()`, so get it in repairInfo.
	originBlockInfo, _ := petriutil.RepairInfo.GetRepairedBlockInfoByBlockName("test", "origin")

	hook := &dbs.TestDBSCallback{}
	var repairErr error
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.Type != perceptron.CausetActionRepairBlock {
			return
		}
		if job.BlockID != originBlockInfo.ID {
			repairErr = errors.New("causet id should be the same")
			return
		}
		if job.SchemaState != perceptron.StateNone {
			repairErr = errors.New("repair job state should be the none")
			return
		}
		// Test whether it's readable, when repaired causet is still stateNone.
		tkInternal := testkit.NewTestKitWithInit(c, s.causetstore)
		_, repairErr = tkInternal.InterDirc("select * from origin")
		// Repaired blockInfo has been filtered by `petri.SchemaReplicant()`, here will get an error cause user can't get access to it.
		if repairErr != nil && terror.ErrorEqual(repairErr, schemareplicant.ErrBlockNotExists) {
			repairErr = nil
		}
	}
	originalHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)

	// InterDirc the repair memex to override the blockInfo.
	tk.MustInterDirc("admin repair causet origin CREATE TABLE origin (a int primary key auto_increment, b varchar(5), c int);")
	c.Assert(repairErr, IsNil)

	// Check the repaired blockInfo is exactly the same with old one in blockID, indexID, defCausID.
	// testGetBlockByName will extract the Block from `petri.SchemaReplicant()` directly.
	repairBlock = testGetBlockByName(c, s.s, "test", "origin")
	c.Assert(repairBlock.Meta().ID, Equals, originBlockInfo.ID)
	c.Assert(len(repairBlock.Meta().DeferredCausets), Equals, 3)
	c.Assert(repairBlock.Meta().DeferredCausets[0].ID, Equals, originBlockInfo.DeferredCausets[0].ID)
	c.Assert(repairBlock.Meta().DeferredCausets[1].ID, Equals, originBlockInfo.DeferredCausets[1].ID)
	c.Assert(repairBlock.Meta().DeferredCausets[2].ID, Equals, originBlockInfo.DeferredCausets[2].ID)
	c.Assert(len(repairBlock.Meta().Indices), Equals, 1)
	c.Assert(repairBlock.Meta().Indices[0].ID, Equals, originBlockInfo.DeferredCausets[0].ID)
	c.Assert(repairBlock.Meta().AutoIncID, Equals, originBlockInfo.AutoIncID)

	c.Assert(repairBlock.Meta().DeferredCausets[0].Tp, Equals, allegrosql.TypeLong)
	c.Assert(repairBlock.Meta().DeferredCausets[1].Tp, Equals, allegrosql.TypeVarchar)
	c.Assert(repairBlock.Meta().DeferredCausets[1].Flen, Equals, 5)
	c.Assert(repairBlock.Meta().DeferredCausets[2].Tp, Equals, allegrosql.TypeLong)

	// InterDirc the show create causet memex to make sure new blockInfo has been set.
	result := tk.MustQuery("show create causet origin")
	c.Assert(result.Rows()[0][1], Equals, "CREATE TABLE `origin` (\n  `a` int(11) NOT NULL AUTO_INCREMENT,\n  `b` varchar(5) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

}

func turnRepairModeAndInit(on bool) {
	list := make([]string, 0)
	if on {
		list = append(list, "test.origin")
	}
	petriutil.RepairInfo.SetRepairMode(on)
	petriutil.RepairInfo.SetRepairBlockList(list)
}

func (s *testSerialDBSuite) TestRepairBlockWithPartition(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/repairFetchCreateBlock", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/repairFetchCreateBlock"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists origin")

	turnRepairModeAndInit(true)
	defer turnRepairModeAndInit(false)
	// Petri reload the blockInfo and add it into repairInfo.
	tk.MustInterDirc("create causet origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p70 values less than (70)," +
		"partition p90 values less than (90));")
	// Test for some old partition has lost.
	_, err := tk.InterDirc("admin repair causet origin create causet origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p90 values less than (90)," +
		"partition p100 values less than (100));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: Partition p100 has lost")

	// Test for some partition changed the condition.
	_, err = tk.InterDirc("admin repair causet origin create causet origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p20 values less than (25)," +
		"partition p50 values less than (50)," +
		"partition p90 values less than (90));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: Partition p20 has lost")

	// Test for some partition changed the partition name.
	_, err = tk.InterDirc("admin repair causet origin create causet origin (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition pNew values less than (50)," +
		"partition p90 values less than (90));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: Partition pnew has lost")

	originBlockInfo, _ := petriutil.RepairInfo.GetRepairedBlockInfoByBlockName("test", "origin")
	tk.MustInterDirc("admin repair causet origin create causet origin_rename (a int not null) partition by RANGE(a) (" +
		"partition p10 values less than (10)," +
		"partition p30 values less than (30)," +
		"partition p50 values less than (50)," +
		"partition p90 values less than (90));")
	repairBlock := testGetBlockByName(c, s.s, "test", "origin_rename")
	c.Assert(repairBlock.Meta().ID, Equals, originBlockInfo.ID)
	c.Assert(len(repairBlock.Meta().DeferredCausets), Equals, 1)
	c.Assert(repairBlock.Meta().DeferredCausets[0].ID, Equals, originBlockInfo.DeferredCausets[0].ID)
	c.Assert(len(repairBlock.Meta().Partition.Definitions), Equals, 4)
	c.Assert(repairBlock.Meta().Partition.Definitions[0].ID, Equals, originBlockInfo.Partition.Definitions[0].ID)
	c.Assert(repairBlock.Meta().Partition.Definitions[1].ID, Equals, originBlockInfo.Partition.Definitions[1].ID)
	c.Assert(repairBlock.Meta().Partition.Definitions[2].ID, Equals, originBlockInfo.Partition.Definitions[2].ID)
	c.Assert(repairBlock.Meta().Partition.Definitions[3].ID, Equals, originBlockInfo.Partition.Definitions[4].ID)

	// Test hash partition.
	tk.MustInterDirc("drop causet if exists origin")
	petriutil.RepairInfo.SetRepairMode(true)
	petriutil.RepairInfo.SetRepairBlockList([]string{"test.origin"})
	tk.MustInterDirc("create causet origin (a varchar(1), b int not null, c int, key idx(c)) partition by hash(b) partitions 30")

	// Test partition num in repair should be exactly same with old one, other wise will cause partition semantic problem.
	_, err = tk.InterDirc("admin repair causet origin create causet origin (a varchar(2), b int not null, c int, key idx(c)) partition by hash(b) partitions 20")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8215]Failed to repair causet: Hash partition num should be the same")

	originBlockInfo, _ = petriutil.RepairInfo.GetRepairedBlockInfoByBlockName("test", "origin")
	tk.MustInterDirc("admin repair causet origin create causet origin (a varchar(3), b int not null, c int, key idx(c)) partition by hash(b) partitions 30")
	repairBlock = testGetBlockByName(c, s.s, "test", "origin")
	c.Assert(repairBlock.Meta().ID, Equals, originBlockInfo.ID)
	c.Assert(len(repairBlock.Meta().Partition.Definitions), Equals, 30)
	c.Assert(repairBlock.Meta().Partition.Definitions[0].ID, Equals, originBlockInfo.Partition.Definitions[0].ID)
	c.Assert(repairBlock.Meta().Partition.Definitions[1].ID, Equals, originBlockInfo.Partition.Definitions[1].ID)
	c.Assert(repairBlock.Meta().Partition.Definitions[29].ID, Equals, originBlockInfo.Partition.Definitions[29].ID)
}

func (s *testDBSuite2) TestCreateBlockWithSetDefCaus(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t_set (a int, b set('e') default '');")
	tk.MustQuery("show create causet t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` set('e') DEFAULT ''\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("drop causet t_set")
	tk.MustInterDirc("create causet t_set (a set('a', 'b', 'c', 'd') default 'a,c,c');")
	tk.MustQuery("show create causet t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('a','b','c','d') DEFAULT 'a,c'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// It's for failure cases.
	// The type of default value is string.
	tk.MustInterDirc("drop causet t_set")
	failedALLEGROSQL := "create causet t_set (a set('1', '4', '10') default '3');"
	tk.MustGetErrCode(failedALLEGROSQL, errno.ErrInvalidDefault)
	failedALLEGROSQL = "create causet t_set (a set('1', '4', '10') default '1,4,11');"
	tk.MustGetErrCode(failedALLEGROSQL, errno.ErrInvalidDefault)
	failedALLEGROSQL = "create causet t_set (a set('1', '4', '10') default '1 ,4');"
	tk.MustGetErrCode(failedALLEGROSQL, errno.ErrInvalidDefault)
	// The type of default value is int.
	failedALLEGROSQL = "create causet t_set (a set('1', '4', '10') default 0);"
	tk.MustGetErrCode(failedALLEGROSQL, errno.ErrInvalidDefault)
	failedALLEGROSQL = "create causet t_set (a set('1', '4', '10') default 8);"
	tk.MustGetErrCode(failedALLEGROSQL, errno.ErrInvalidDefault)

	// The type of default value is int.
	// It's for successful cases
	tk.MustInterDirc("create causet t_set (a set('1', '4', '10', '21') default 1);")
	tk.MustQuery("show create causet t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("drop causet t_set")
	tk.MustInterDirc("create causet t_set (a set('1', '4', '10', '21') default 2);")
	tk.MustQuery("show create causet t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '4'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("drop causet t_set")
	tk.MustInterDirc("create causet t_set (a set('1', '4', '10', '21') default 3);")
	tk.MustQuery("show create causet t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1,4'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("drop causet t_set")
	tk.MustInterDirc("create causet t_set (a set('1', '4', '10', '21') default 15);")
	tk.MustQuery("show create causet t_set").Check(testkit.Rows("t_set CREATE TABLE `t_set` (\n" +
		"  `a` set('1','4','10','21') DEFAULT '1,4,10,21'\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustInterDirc("insert into t_set value()")
	tk.MustQuery("select * from t_set").Check(testkit.Rows("1,4,10,21"))
}

func (s *testDBSuite2) TestBlockForeignKey(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t1 (a int, b int);")
	// test create causet with foreign key.
	failALLEGROSQL := "create causet t2 (c int, foreign key (a) references t1(a));"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrKeyDeferredCausetDoesNotExits)
	// test add foreign key.
	tk.MustInterDirc("create causet t3 (a int, b int);")
	failALLEGROSQL = "alter causet t1 add foreign key (c) REFERENCES t3(a);"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrKeyDeferredCausetDoesNotExits)
	// test oreign key not match error
	failALLEGROSQL = "alter causet t1 add foreign key (a) REFERENCES t3(a, b);"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrWrongFkDef)
	// Test drop defCausumn with foreign key.
	tk.MustInterDirc("create causet t4 (c int,d int,foreign key (d) references t1 (b));")
	failALLEGROSQL = "alter causet t4 drop defCausumn d"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrFkDeferredCausetCannotDrop)
	// Test change defCausumn with foreign key.
	failALLEGROSQL = "alter causet t4 change defCausumn d e bigint;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrFKIncompatibleDeferredCausets)
	// Test modify defCausumn with foreign key.
	failALLEGROSQL = "alter causet t4 modify defCausumn d bigint;"
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrFKIncompatibleDeferredCausets)
	tk.MustQuery("select count(*) from information_schema.KEY_COLUMN_USAGE;")
	tk.MustInterDirc("alter causet t4 drop foreign key d")
	tk.MustInterDirc("alter causet t4 modify defCausumn d bigint;")
	tk.MustInterDirc("drop causet if exists t1,t2,t3,t4;")
}

func (s *testDBSuite3) TestFKOnGeneratedDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// test add foreign key to generated defCausumn

	// foreign key constraint cannot be defined on a virtual generated defCausumn.
	tk.MustInterDirc("create causet t1 (a int primary key);")
	tk.MustGetErrCode("create causet t2 (a int, b int as (a+1) virtual, foreign key (b) references t1(a));", errno.ErrCannotAddForeign)
	tk.MustInterDirc("create causet t2 (a int, b int generated always as (a+1) virtual);")
	tk.MustGetErrCode("alter causet t2 add foreign key (b) references t1(a);", errno.ErrCannotAddForeign)
	tk.MustInterDirc("drop causet t1, t2;")

	// foreign key constraint can be defined on a stored generated defCausumn.
	tk.MustInterDirc("create causet t2 (a int primary key);")
	tk.MustInterDirc("create causet t1 (a int, b int as (a+1) stored, foreign key (b) references t2(a));")
	tk.MustInterDirc("create causet t3 (a int, b int generated always as (a+1) stored);")
	tk.MustInterDirc("alter causet t3 add foreign key (b) references t2(a);")
	tk.MustInterDirc("drop causet t1, t2, t3;")

	// foreign key constraint can reference a stored generated defCausumn.
	tk.MustInterDirc("create causet t1 (a int, b int generated always as (a+1) stored primary key);")
	tk.MustInterDirc("create causet t2 (a int, foreign key (a) references t1(b));")
	tk.MustInterDirc("create causet t3 (a int);")
	tk.MustInterDirc("alter causet t3 add foreign key (a) references t1(b);")
	tk.MustInterDirc("drop causet t1, t2, t3;")

	// rejected FK options on stored generated defCausumns
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on uFIDelate set null);", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on uFIDelate cascade);", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on uFIDelate set default);", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on delete set null);", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on delete set default);", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustInterDirc("create causet t2 (a int primary key);")
	tk.MustInterDirc("create causet t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter causet t1 add foreign key (b) references t2(a) on uFIDelate set null;", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("alter causet t1 add foreign key (b) references t2(a) on uFIDelate cascade;", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("alter causet t1 add foreign key (b) references t2(a) on uFIDelate set default;", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("alter causet t1 add foreign key (b) references t2(a) on delete set null;", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustGetErrCode("alter causet t1 add foreign key (b) references t2(a) on delete set default;", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustInterDirc("drop causet t1, t2;")
	// defCausumn name with uppercase characters
	tk.MustGetErrCode("create causet t1 (A int, b int generated always as (a+1) stored, foreign key (b) references t2(a) on uFIDelate set null);", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustInterDirc("create causet t2 (a int primary key);")
	tk.MustInterDirc("create causet t1 (A int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter causet t1 add foreign key (b) references t2(a) on uFIDelate set null;", errno.ErrWrongFKOptionForGeneratedDeferredCauset)
	tk.MustInterDirc("drop causet t1, t2;")

	// special case: MilevaDB error different from MyALLEGROSQL 8.0
	// MyALLEGROSQL: ERROR 3104 (HY000): Cannot define foreign key with ON UFIDelATE SET NULL clause on a generated defCausumn.
	// MilevaDB:  ERROR 1146 (42S02): Block 'test.t2' doesn't exist
	tk.MustInterDirc("create causet t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter causet t1 add foreign key (b) references t2(a) on uFIDelate set null;", errno.ErrNoSuchBlock)
	tk.MustInterDirc("drop causet t1;")

	// allowed FK options on stored generated defCausumns
	tk.MustInterDirc("create causet t1 (a int primary key, b char(5));")
	tk.MustInterDirc("create causet t2 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on uFIDelate restrict);")
	tk.MustInterDirc("create causet t3 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on uFIDelate no action);")
	tk.MustInterDirc("create causet t4 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete restrict);")
	tk.MustInterDirc("create causet t5 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete cascade);")
	tk.MustInterDirc("create causet t6 (a int, b int generated always as (a % 10) stored, foreign key (b) references t1(a) on delete no action);")
	tk.MustInterDirc("drop causet t2,t3,t4,t5,t6;")
	tk.MustInterDirc("create causet t2 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t2 add foreign key (b) references t1(a) on uFIDelate restrict;")
	tk.MustInterDirc("create causet t3 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t3 add foreign key (b) references t1(a) on uFIDelate no action;")
	tk.MustInterDirc("create causet t4 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t4 add foreign key (b) references t1(a) on delete restrict;")
	tk.MustInterDirc("create causet t5 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t5 add foreign key (b) references t1(a) on delete cascade;")
	tk.MustInterDirc("create causet t6 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t6 add foreign key (b) references t1(a) on delete no action;")
	tk.MustInterDirc("drop causet t1,t2,t3,t4,t5,t6;")

	// rejected FK options on the base defCausumns of a stored generated defCausumns
	tk.MustInterDirc("create causet t2 (a int primary key);")
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on uFIDelate set null);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on uFIDelate cascade);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on uFIDelate set default);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete set null);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete cascade);", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("create causet t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) on delete set default);", errno.ErrCannotAddForeign)
	tk.MustInterDirc("create causet t1 (a int, b int generated always as (a+1) stored);")
	tk.MustGetErrCode("alter causet t1 add foreign key (a) references t2(a) on uFIDelate set null;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter causet t1 add foreign key (a) references t2(a) on uFIDelate cascade;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter causet t1 add foreign key (a) references t2(a) on uFIDelate set default;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter causet t1 add foreign key (a) references t2(a) on delete set null;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter causet t1 add foreign key (a) references t2(a) on delete cascade;", errno.ErrCannotAddForeign)
	tk.MustGetErrCode("alter causet t1 add foreign key (a) references t2(a) on delete set default;", errno.ErrCannotAddForeign)
	tk.MustInterDirc("drop causet t1, t2;")

	// allowed FK options on the base defCausumns of a stored generated defCausumns
	tk.MustInterDirc("create causet t1 (a int primary key, b char(5));")
	tk.MustInterDirc("create causet t2 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on uFIDelate restrict);")
	tk.MustInterDirc("create causet t3 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on uFIDelate no action);")
	tk.MustInterDirc("create causet t4 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on delete restrict);")
	tk.MustInterDirc("create causet t5 (a int, b int generated always as (a % 10) stored, foreign key (a) references t1(a) on delete no action);")
	tk.MustInterDirc("drop causet t2,t3,t4,t5")
	tk.MustInterDirc("create causet t2 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t2 add foreign key (a) references t1(a) on uFIDelate restrict;")
	tk.MustInterDirc("create causet t3 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t3 add foreign key (a) references t1(a) on uFIDelate no action;")
	tk.MustInterDirc("create causet t4 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t4 add foreign key (a) references t1(a) on delete restrict;")
	tk.MustInterDirc("create causet t5 (a int, b int generated always as (a % 10) stored);")
	tk.MustInterDirc("alter causet t5 add foreign key (a) references t1(a) on delete no action;")
	tk.MustInterDirc("drop causet t1,t2,t3,t4,t5;")
}

func (s *testSerialDBSuite) TestTruncateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet truncate_block (c1 int, c2 int)")
	tk.MustInterDirc("insert truncate_block values (1, 1), (2, 2)")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("truncate_block"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID

	tk.MustInterDirc("truncate causet truncate_block")

	tk.MustInterDirc("insert truncate_block values (3, 3), (4, 4)")
	tk.MustQuery("select * from truncate_block").Check(testkit.Rows("3 3", "4 4"))

	is = petri.GetPetri(ctx).SchemaReplicant()
	newTblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("truncate_block"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Greater, oldTblID)

	// Verify that the old causet data has been deleted by background worker.
	blockPrefix := blockcodec.EncodeBlockPrefix(oldTblID)
	hasOldBlockData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err = ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
			it, err1 := txn.Iter(blockPrefix, nil)
			if err1 != nil {
				return err1
			}
			if !it.Valid() {
				hasOldBlockData = false
			} else {
				hasOldBlockData = it.Key().HasPrefix(blockPrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldBlockData {
			break
		}
		time.Sleep(waitForCleanDataInterval)
	}
	c.Assert(hasOldBlockData, IsFalse)

	// Test for truncate causet should clear the tiflash available status.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t1 (a int);")
	tk.MustInterDirc("alter causet t1 set tiflash replica 3 location labels 'a','b';")
	t1 := testGetBlockByName(c, s.s, "test", "t1")
	// Mock for causet tiflash replica was available.
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, t1.Meta().ID, true)
	c.Assert(err, IsNil)
	t1 = testGetBlockByName(c, s.s, "test", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)

	tk.MustInterDirc("truncate causet t1")
	t2 := testGetBlockByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)

	// Test for truncate partition should clear the tiflash available status.
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t1 (a int) partition by hash(a) partitions 2;")
	tk.MustInterDirc("alter causet t1 set tiflash replica 3 location labels 'a','b';")
	t1 = testGetBlockByName(c, s.s, "test", "t1")
	// Mock for all partitions replica was available.
	partition := t1.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 2)
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	t1 = testGetBlockByName(c, s.s, "test", "t1")
	c.Assert(t1.Meta().TiFlashReplica, NotNil)
	c.Assert(t1.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(t1.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID})

	tk.MustInterDirc("alter causet t1 truncate partition p0")
	t2 = testGetBlockByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[1].ID})
	// Test for truncate twice.
	tk.MustInterDirc("alter causet t1 truncate partition p0")
	t2 = testGetBlockByName(c, s.s, "test", "t1")
	c.Assert(t2.Meta().TiFlashReplica.Count, Equals, t1.Meta().TiFlashReplica.Count)
	c.Assert(t2.Meta().TiFlashReplica.LocationLabels, DeepEquals, t1.Meta().TiFlashReplica.LocationLabels)
	c.Assert(t2.Meta().TiFlashReplica.Available, IsFalse)
	c.Assert(t2.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[1].ID})

}

func (s *testDBSuite4) TestRenameBlock(c *C) {
	isAlterBlock := false
	s.testRenameBlock(c, "rename causet %s to %s", isAlterBlock)
}

func (s *testDBSuite5) TestAlterBlockRenameBlock(c *C) {
	isAlterBlock := true
	s.testRenameBlock(c, "alter causet %s rename to %s", isAlterBlock)
}

func (s *testDBSuite) testRenameBlock(c *C, allegrosql string, isAlterBlock bool) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// for different databases
	tk.MustInterDirc("create causet t (c1 int, c2 int)")
	tk.MustInterDirc("insert t values (1, 1), (2, 2)")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	oldTblID := oldTblInfo.Meta().ID
	tk.MustInterDirc("create database test1")
	tk.MustInterDirc("use test1")
	tk.MustInterDirc(fmt.Sprintf(allegrosql, "test.t", "test1.t1"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	newTblInfo, err := is.BlockByName(perceptron.NewCIStr("test1"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1", "2 2"))
	tk.MustInterDirc("use test")

	// Make sure t doesn't exist.
	tk.MustInterDirc("create causet t (c1 int, c2 int)")
	tk.MustInterDirc("drop causet t")

	// for the same database
	tk.MustInterDirc("use test1")
	tk.MustInterDirc(fmt.Sprintf(allegrosql, "t1", "t2"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	newTblInfo, err = is.BlockByName(perceptron.NewCIStr("test1"), perceptron.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(newTblInfo.Meta().ID, Equals, oldTblID)
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 1", "2 2"))
	isExist := is.BlockExists(perceptron.NewCIStr("test1"), perceptron.NewCIStr("t1"))
	c.Assert(isExist, IsFalse)
	tk.MustQuery("show blocks").Check(testkit.Rows("t2"))

	// for failure case
	failALLEGROSQL := fmt.Sprintf(allegrosql, "test_not_exist.t", "test_not_exist.t")
	if isAlterBlock {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrNoSuchBlock)
	} else {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrFileNotFound)
	}
	failALLEGROSQL = fmt.Sprintf(allegrosql, "test.test_not_exist", "test.test_not_exist")
	if isAlterBlock {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrNoSuchBlock)
	} else {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrFileNotFound)
	}
	failALLEGROSQL = fmt.Sprintf(allegrosql, "test.t_not_exist", "test_not_exist.t")
	if isAlterBlock {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrNoSuchBlock)
	} else {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrFileNotFound)
	}
	failALLEGROSQL = fmt.Sprintf(allegrosql, "test1.t2", "test_not_exist.t")
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrErrorOnRename)

	tk.MustInterDirc("use test1")
	tk.MustInterDirc("create causet if not exists t_exist (c1 int, c2 int)")
	failALLEGROSQL = fmt.Sprintf(allegrosql, "test1.t2", "test1.t_exist")
	tk.MustGetErrCode(failALLEGROSQL, errno.ErrBlockExists)
	failALLEGROSQL = fmt.Sprintf(allegrosql, "test.t_not_exist", "test1.t_exist")
	if isAlterBlock {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrNoSuchBlock)
	} else {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrBlockExists)
	}
	failALLEGROSQL = fmt.Sprintf(allegrosql, "test_not_exist.t", "test1.t_exist")
	if isAlterBlock {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrNoSuchBlock)
	} else {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrBlockExists)
	}
	failALLEGROSQL = fmt.Sprintf(allegrosql, "test_not_exist.t", "test1.t_not_exist")
	if isAlterBlock {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrNoSuchBlock)
	} else {
		tk.MustGetErrCode(failALLEGROSQL, errno.ErrFileNotFound)
	}

	// for the same causet name
	tk.MustInterDirc("use test1")
	tk.MustInterDirc("create causet if not exists t (c1 int, c2 int)")
	tk.MustInterDirc("create causet if not exists t1 (c1 int, c2 int)")
	if isAlterBlock {
		tk.MustInterDirc(fmt.Sprintf(allegrosql, "test1.t", "t"))
		tk.MustInterDirc(fmt.Sprintf(allegrosql, "test1.t1", "test1.T1"))
	} else {
		tk.MustGetErrCode(fmt.Sprintf(allegrosql, "test1.t", "t"), errno.ErrBlockExists)
		tk.MustGetErrCode(fmt.Sprintf(allegrosql, "test1.t1", "test1.T1"), errno.ErrBlockExists)
	}

	// Test rename causet name too long.
	tk.MustGetErrCode("rename causet test1.t1 to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", errno.ErrTooLongIdent)
	tk.MustGetErrCode("alter  causet test1.t1 rename to test1.txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", errno.ErrTooLongIdent)

	tk.MustInterDirc("drop database test1")
}

func (s *testDBSuite1) TestRenameMultiBlocks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t1(id int)")
	tk.MustInterDirc("create causet t2(id int)")
	// Currently it will fail only.
	allegrosql := fmt.Sprintf("rename causet t1 to t3, t2 to t4")
	_, err := tk.InterDirc(allegrosql)
	c.Assert(err, NotNil)
	originErr := errors.Cause(err)
	c.Assert(originErr.Error(), Equals, "can't run multi schemaReplicant change")

	tk.MustInterDirc("drop causet t1, t2")
}

func (s *testDBSuite2) TestAddNotNullDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	// for different databases
	tk.MustInterDirc("create causet tnn (c1 int primary key auto_increment, c2 int)")
	tk.MustInterDirc("insert tnn (c2) values (0)" + strings.Repeat(",(0)", 99))
	done := make(chan error, 1)
	testdbsutil.StochastikInterDircInGoroutine(c, s.causetstore, "alter causet tnn add defCausumn c3 int not null default 3", done)
	uFIDelateCnt := 0
out:
	for {
		select {
		case err := <-done:
			c.Assert(err, IsNil)
			break out
		default:
			tk.MustInterDirc("uFIDelate tnn set c2 = c2 + 1 where c1 = 99")
			uFIDelateCnt++
		}
	}
	expected := fmt.Sprintf("%d %d", uFIDelateCnt, 3)
	tk.MustQuery("select c2, c3 from tnn where c1 = 99").Check(testkit.Rows(expected))

	tk.MustInterDirc("drop causet tnn")
}

func (s *testDBSuite3) TestGeneratedDeferredCausetDBS(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	// Check create causet with virtual and stored generated defCausumns.
	tk.MustInterDirc(`CREATE TABLE test_gv_dbs(a int, b int as (a+8) virtual, c int as (b + 2) stored)`)

	// Check desc causet with virtual and stored generated defCausumns.
	result := tk.MustQuery(`DESC test_gv_dbs`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	// Check show create causet with virtual and stored generated defCausumns.
	result = tk.MustQuery(`show create causet test_gv_dbs`)
	result.Check(testkit.Rows(
		"test_gv_dbs CREATE TABLE `test_gv_dbs` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) GENERATED ALWAYS AS (`a` + 8) VIRTUAL,\n  `c` int(11) GENERATED ALWAYS AS (`b` + 2) STORED\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	// Check generated memex with blanks.
	tk.MustInterDirc("create causet block_with_gen_defCaus_blanks (a int, b char(20) as (cast( \r\n\t a \r\n\tas  char)), c int as (a+100))")
	result = tk.MustQuery(`show create causet block_with_gen_defCaus_blanks`)
	result.Check(testkit.Rows("block_with_gen_defCaus_blanks CREATE TABLE `block_with_gen_defCaus_blanks` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` char(20) GENERATED ALWAYS AS (cast(`a` as char)) VIRTUAL,\n" +
		"  `c` int(11) GENERATED ALWAYS AS (`a` + 100) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Check generated memex with charset latin1 ("latin1" != allegrosql.DefaultCharset).
	tk.MustInterDirc("create causet block_with_gen_defCaus_latin1 (a int, b char(20) as (cast( \r\n\t a \r\n\tas  char charset latin1)), c int as (a+100))")
	result = tk.MustQuery(`show create causet block_with_gen_defCaus_latin1`)
	result.Check(testkit.Rows("block_with_gen_defCaus_latin1 CREATE TABLE `block_with_gen_defCaus_latin1` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` char(20) GENERATED ALWAYS AS (cast(`a` as char charset latin1)) VIRTUAL,\n" +
		"  `c` int(11) GENERATED ALWAYS AS (`a` + 100) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Check generated memex with string (issue 9457).
	tk.MustInterDirc("create causet block_with_gen_defCaus_string (first_name varchar(10), last_name varchar(10), full_name varchar(255) AS (CONCAT(first_name,' ',last_name)))")
	result = tk.MustQuery(`show create causet block_with_gen_defCaus_string`)
	result.Check(testkit.Rows("block_with_gen_defCaus_string CREATE TABLE `block_with_gen_defCaus_string` (\n" +
		"  `first_name` varchar(10) DEFAULT NULL,\n" +
		"  `last_name` varchar(10) DEFAULT NULL,\n" +
		"  `full_name` varchar(255) GENERATED ALWAYS AS (concat(`first_name`, ' ', `last_name`)) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustInterDirc("alter causet block_with_gen_defCaus_string modify defCausumn full_name varchar(255) GENERATED ALWAYS AS (CONCAT(last_name,' ' ,first_name) ) VIRTUAL")
	result = tk.MustQuery(`show create causet block_with_gen_defCaus_string`)
	result.Check(testkit.Rows("block_with_gen_defCaus_string CREATE TABLE `block_with_gen_defCaus_string` (\n" +
		"  `first_name` varchar(10) DEFAULT NULL,\n" +
		"  `last_name` varchar(10) DEFAULT NULL,\n" +
		"  `full_name` varchar(255) GENERATED ALWAYS AS (concat(`last_name`, ' ', `first_name`)) VIRTUAL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	genExprTests := []struct {
		stmt string
		err  int
	}{
		// Drop/rename defCausumns dependent by other defCausumn.
		{`alter causet test_gv_dbs drop defCausumn a`, errno.ErrDependentByGeneratedDeferredCauset},
		{`alter causet test_gv_dbs change defCausumn a anew int`, errno.ErrBadField},

		// Modify/change stored status of generated defCausumns.
		{`alter causet test_gv_dbs modify defCausumn b bigint`, errno.ErrUnsupportedOnGeneratedDeferredCauset},
		{`alter causet test_gv_dbs change defCausumn c cnew bigint as (a+100)`, errno.ErrUnsupportedOnGeneratedDeferredCauset},

		// Modify/change generated defCausumns breaking prior.
		{`alter causet test_gv_dbs modify defCausumn b int as (c+100)`, errno.ErrGeneratedDeferredCausetNonPrior},
		{`alter causet test_gv_dbs change defCausumn b bnew int as (c+100)`, errno.ErrGeneratedDeferredCausetNonPrior},

		// Refer not exist defCausumns in generation memex.
		{`create causet test_gv_dbs_bad (a int, b int as (c+8))`, errno.ErrBadField},

		// Refer generated defCausumns non prior.
		{`create causet test_gv_dbs_bad (a int, b int as (c+1), c int as (a+1))`, errno.ErrGeneratedDeferredCausetNonPrior},

		// Virtual generated defCausumns cannot be primary key.
		{`create causet test_gv_dbs_bad (a int, b int, c int as (a+b) primary key)`, errno.ErrUnsupportedOnGeneratedDeferredCauset},
		{`create causet test_gv_dbs_bad (a int, b int, c int as (a+b), primary key(c))`, errno.ErrUnsupportedOnGeneratedDeferredCauset},
		{`create causet test_gv_dbs_bad (a int, b int, c int as (a+b), primary key(a, c))`, errno.ErrUnsupportedOnGeneratedDeferredCauset},

		// Add stored generated defCausumn through alter causet.
		{`alter causet test_gv_dbs add defCausumn d int as (b+2) stored`, errno.ErrUnsupportedOnGeneratedDeferredCauset},
		{`alter causet test_gv_dbs modify defCausumn b int as (a + 8) stored`, errno.ErrUnsupportedOnGeneratedDeferredCauset},
	}
	for _, tt := range genExprTests {
		tk.MustGetErrCode(tt.stmt, tt.err)
	}

	// Check alter causet modify/change generated defCausumn.
	modStoredDefCausErrMsg := "[dbs:3106]'modifying a stored defCausumn' is not supported for generated defCausumns."
	_, err := tk.InterDirc(`alter causet test_gv_dbs modify defCausumn c bigint as (b+200) stored`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredDefCausErrMsg)

	result = tk.MustQuery(`DESC test_gv_dbs`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	tk.MustInterDirc(`alter causet test_gv_dbs change defCausumn b b bigint as (a+100) virtual`)
	result = tk.MustQuery(`DESC test_gv_dbs`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))

	tk.MustInterDirc(`alter causet test_gv_dbs change defCausumn c cnew bigint`)
	result = tk.MustQuery(`DESC test_gv_dbs`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b bigint(20) YES  <nil> VIRTUAL GENERATED`, `cnew bigint(20) YES  <nil> `))
}

func (s *testDBSuite4) TestComment(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("drop causet if exists ct, ct1")

	validComment := strings.Repeat("a", 1024)
	invalidComment := strings.Repeat("b", 1025)

	tk.MustInterDirc("create causet ct (c int, d int, e int, key (c) comment '" + validComment + "')")
	tk.MustInterDirc("create index i on ct (d) comment '" + validComment + "'")
	tk.MustInterDirc("alter causet ct add key (e) comment '" + validComment + "'")

	tk.MustGetErrCode("create causet ct1 (c int, key (c) comment '"+invalidComment+"')", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("create index i1 on ct (d) comment '"+invalidComment+"b"+"'", errno.ErrTooLongIndexComment)
	tk.MustGetErrCode("alter causet ct add key (e) comment '"+invalidComment+"'", errno.ErrTooLongIndexComment)

	tk.MustInterDirc("set @@sql_mode=''")
	tk.MustInterDirc("create causet ct1 (c int, d int, e int, key (c) comment '" + invalidComment + "')")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|1688|Comment for index 'c' is too long (max = 1024)"))
	tk.MustInterDirc("create index i1 on ct1 (d) comment '" + invalidComment + "b" + "'")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|1688|Comment for index 'i1' is too long (max = 1024)"))
	tk.MustInterDirc("alter causet ct1 add key (e) comment '" + invalidComment + "'")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|1688|Comment for index 'e' is too long (max = 1024)"))

	tk.MustInterDirc("drop causet if exists ct, ct1")
}

func (s *testSerialDBSuite) TestRebaseAutoID(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)

	tk.MustInterDirc("drop database if exists milevadb;")
	tk.MustInterDirc("create database milevadb;")
	tk.MustInterDirc("use milevadb;")
	tk.MustInterDirc("create causet milevadb.test (a int auto_increment primary key, b int);")
	tk.MustInterDirc("insert milevadb.test values (null, 1);")
	tk.MustQuery("select * from milevadb.test").Check(testkit.Rows("1 1"))
	tk.MustInterDirc("alter causet milevadb.test auto_increment = 6000;")
	tk.MustInterDirc("insert milevadb.test values (null, 1);")
	tk.MustQuery("select * from milevadb.test").Check(testkit.Rows("1 1", "6000 1"))
	tk.MustInterDirc("alter causet milevadb.test auto_increment = 5;")
	tk.MustInterDirc("insert milevadb.test values (null, 1);")
	tk.MustQuery("select * from milevadb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1"))

	// Current range for causet test is [11000, 15999].
	// Though it does not have a tuple "a = 15999", its global next auto increment id should be 16000.
	// Anyway it is not compatible with MyALLEGROSQL.
	tk.MustInterDirc("alter causet milevadb.test auto_increment = 12000;")
	tk.MustInterDirc("insert milevadb.test values (null, 1);")
	tk.MustQuery("select * from milevadb.test").Check(testkit.Rows("1 1", "6000 1", "11000 1", "16000 1"))

	tk.MustInterDirc("create causet milevadb.test2 (a int);")
	tk.MustGetErrCode("alter causet milevadb.test2 add defCausumn b int auto_increment key, auto_increment=10;", errno.ErrUnsupportedDBSOperation)
}

func (s *testDBSuite5) TestCheckDeferredCausetDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists text_default_text;")
	tk.MustGetErrCode("create causet text_default_text(c1 text not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create causet text_default_text(c1 text not null default 'scds');", errno.ErrBlobCantHaveDefault)

	tk.MustInterDirc("drop causet if exists text_default_json;")
	tk.MustGetErrCode("create causet text_default_json(c1 json not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create causet text_default_json(c1 json not null default 'dfew555');", errno.ErrBlobCantHaveDefault)

	tk.MustInterDirc("drop causet if exists text_default_blob;")
	tk.MustGetErrCode("create causet text_default_blob(c1 blob not null default '');", errno.ErrBlobCantHaveDefault)
	tk.MustGetErrCode("create causet text_default_blob(c1 blob not null default 'scds54');", errno.ErrBlobCantHaveDefault)

	tk.MustInterDirc("set sql_mode='';")
	tk.MustInterDirc("create causet text_default_text(c1 text not null default '');")
	tk.MustQuery(`show create causet text_default_text`).Check(solitonutil.RowsWithSep("|",
		"text_default_text CREATE TABLE `text_default_text` (\n"+
			"  `c1` text NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("text_default_text"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().DeferredCausets[0].DefaultValue, Equals, "")

	tk.MustInterDirc("create causet text_default_blob(c1 blob not null default '');")
	tk.MustQuery(`show create causet text_default_blob`).Check(solitonutil.RowsWithSep("|",
		"text_default_blob CREATE TABLE `text_default_blob` (\n"+
			"  `c1` blob NOT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = petri.GetPetri(ctx).SchemaReplicant()
	tblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("text_default_blob"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().DeferredCausets[0].DefaultValue, Equals, "")

	tk.MustInterDirc("create causet text_default_json(c1 json not null default '');")
	tk.MustQuery(`show create causet text_default_json`).Check(solitonutil.RowsWithSep("|",
		"text_default_json CREATE TABLE `text_default_json` (\n"+
			"  `c1` json NOT NULL DEFAULT 'null'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	is = petri.GetPetri(ctx).SchemaReplicant()
	tblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("text_default_json"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().DeferredCausets[0].DefaultValue, Equals, `null`)
}

func (s *testDBSuite1) TestCharacterSetInDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database varchar_test;")
	defer tk.MustInterDirc("drop database varchar_test;")
	tk.MustInterDirc("use varchar_test")
	tk.MustInterDirc("create causet t (c1 int, s1 varchar(10), s2 text)")
	tk.MustQuery("select count(*) from information_schema.defCausumns where block_schema = 'varchar_test' and character_set_name != 'utf8mb4'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from information_schema.defCausumns where block_schema = 'varchar_test' and character_set_name = 'utf8mb4'").Check(testkit.Rows("2"))

	tk.MustInterDirc("create causet t1(id int) charset=UTF8;")
	tk.MustInterDirc("create causet t2(id int) charset=BINARY;")
	tk.MustInterDirc("create causet t3(id int) charset=LATIN1;")
	tk.MustInterDirc("create causet t4(id int) charset=ASCII;")
	tk.MustInterDirc("create causet t5(id int) charset=UTF8MB4;")

	tk.MustInterDirc("create causet t11(id int) charset=utf8;")
	tk.MustInterDirc("create causet t12(id int) charset=binary;")
	tk.MustInterDirc("create causet t13(id int) charset=latin1;")
	tk.MustInterDirc("create causet t14(id int) charset=ascii;")
	tk.MustInterDirc("create causet t15(id int) charset=utf8mb4;")
}

func (s *testDBSuite2) TestAddNotNullDeferredCausetWhileInsertOnDupUFIDelate(c *C) {
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustInterDirc("use " + s.schemaName)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use " + s.schemaName)
	closeCh := make(chan bool)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	tk1.MustInterDirc("create causet nn (a int primary key, b int)")
	tk1.MustInterDirc("insert nn values (1, 1)")
	var tk2Err error
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closeCh:
				return
			default:
			}
			_, tk2Err = tk2.InterDirc("insert nn (a, b) values (1, 1) on duplicate key uFIDelate a = 1, b = values(b) + 1")
			if tk2Err != nil {
				return
			}
		}
	}()
	tk1.MustInterDirc("alter causet nn add defCausumn c int not null default 3 after a")
	close(closeCh)
	wg.Wait()
	c.Assert(tk2Err, IsNil)
	tk1.MustQuery("select * from nn").Check(testkit.Rows("1 3 2"))
}

func (s *testDBSuite3) TestDeferredCausetModifyingDefinition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists test2;")
	tk.MustInterDirc("create causet test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustInterDirc("alter causet test2 change c2 a int not null;")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	t, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("test2"))
	c.Assert(err, IsNil)
	var c2 *causet.DeferredCauset
	for _, defCaus := range t.DefCauss() {
		if defCaus.Name.L == "a" {
			c2 = defCaus
		}
	}
	c.Assert(allegrosql.HasNotNullFlag(c2.Flag), IsTrue)

	tk.MustInterDirc("drop causet if exists test2;")
	tk.MustInterDirc("create causet test2 (c1 int, c2 int, c3 int default 1, index (c1));")
	tk.MustInterDirc("insert into test2(c2) values (null);")
	tk.MustGetErrCode("alter causet test2 change c2 a int not null", errno.ErrInvalidUseOfNull)
	tk.MustGetErrCode("alter causet test2 change c1 a1 bigint not null;", allegrosql.WarnDataTruncated)
}

func (s *testDBSuite4) TestCheckTooBigFieldLength(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists tr_01;")
	tk.MustInterDirc("create causet tr_01 (id int, name varchar(20000), purchased date )  default charset=utf8 defCauslate=utf8_bin;")

	tk.MustInterDirc("drop causet if exists tr_02;")
	tk.MustInterDirc("create causet tr_02 (id int, name varchar(16000), purchased date )  default charset=utf8mb4 defCauslate=utf8mb4_bin;")

	tk.MustInterDirc("drop causet if exists tr_03;")
	tk.MustInterDirc("create causet tr_03 (id int, name varchar(65534), purchased date ) default charset=latin1;")

	tk.MustInterDirc("drop causet if exists tr_04;")
	tk.MustInterDirc("create causet tr_04 (a varchar(20000) ) default charset utf8;")
	tk.MustGetErrCode("alter causet tr_04 add defCausumn b varchar(20000) charset utf8mb4;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("alter causet tr_04 convert to character set utf8mb4;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create causet tr (id int, name varchar(30000), purchased date )  default charset=utf8 defCauslate=utf8_bin;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create causet tr (id int, name varchar(20000) charset utf8mb4, purchased date ) default charset=utf8 defCauslate=utf8_bin;", errno.ErrTooBigFieldlength)
	tk.MustGetErrCode("create causet tr (id int, name varchar(65536), purchased date ) default charset=latin1;", errno.ErrTooBigFieldlength)

	tk.MustInterDirc("drop causet if exists tr_05;")
	tk.MustInterDirc("create causet tr_05 (a varchar(16000) charset utf8);")
	tk.MustInterDirc("alter causet tr_05 modify defCausumn a varchar(16000) charset utf8;")
	tk.MustInterDirc("alter causet tr_05 modify defCausumn a varchar(16000) charset utf8mb4;")
}

func (s *testDBSuite5) TestCheckConvertToCharacter(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	defer tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("create causet t(a varchar(10) charset binary);")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	t, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(10) charset utf8 defCauslate utf8_bin", errno.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(10) charset utf8mb4 defCauslate utf8mb4_bin", errno.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t modify defCausumn a varchar(10) charset latin1 defCauslate latin1_bin", errno.ErrUnsupportedDBSOperation)
	c.Assert(t.DefCauss()[0].Charset, Equals, "binary")
}

func (s *testDBSuite5) TestModifyDeferredCausetRollBack(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t1")
	s.mustInterDirc(tk, c, "create causet t1 (c1 int, c2 int, c3 int default 1, index (c1));")

	var c2 *causet.DeferredCauset
	var checkErr error
	hook := &dbs.TestDBSCallback{}
	hook.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		if checkErr != nil {
			return
		}

		t := s.testGetBlock(c, "t1")
		for _, defCaus := range t.DefCauss() {
			if defCaus.Name.L == "c2" {
				c2 = defCaus
			}
		}
		if allegrosql.HasPreventNullInsertFlag(c2.Flag) {
			tk.MustGetErrCode("insert into t1(c2) values (null);", errno.ErrBadNull)
		}

		hookCtx := mock.NewContext()
		hookCtx.CausetStore = s.causetstore
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}

		jobIDs := []int64{job.ID}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		errs, err := admin.CancelJobs(txn, jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		// It only tests cancel one DBS job.
		if errs[0] != nil {
			checkErr = errors.Trace(errs[0])
			return
		}

		txn, err = hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}

	originalHook := s.dom.DBS().GetHook()
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundInterDirc(s.causetstore, "alter causet t1 change c2 c2 bigint not null;", done)

	err := <-done
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
	s.mustInterDirc(tk, c, "insert into t1(c2) values (null);")

	t := s.testGetBlock(c, "t1")
	for _, defCaus := range t.DefCauss() {
		if defCaus.Name.L == "c2" {
			c2 = defCaus
		}
	}
	c.Assert(allegrosql.HasNotNullFlag(c2.Flag), IsFalse)
	s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)
	s.mustInterDirc(tk, c, "drop causet t1")
}

func (s *testSerialDBSuite) TestModifyDeferredCausetNullToNotNullWithChangingVal2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	enableChangeDeferredCausetType := tk.Se.GetStochastikVars().EnableChangeDeferredCausetType
	tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = true
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/mockInsertValueAfterCheckNull", `return("insert into test.tt values (NULL, NULL)")`), IsNil)
	defer func() {
		tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = enableChangeDeferredCausetType
		failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/mockInsertValueAfterCheckNull")
	}()

	tk.MustInterDirc(`create causet tt (a bigint, b int, unique index idx(a));`)
	tk.MustInterDirc("insert into tt values (1,1),(2,2),(3,3);")
	_, err := tk.InterDirc("alter causet tt modify a int not null;")
	c.Assert(err.Error(), Equals, "[dbs:1265]Data truncated for defCausumn 'a' at event 1")
	tk.MustInterDirc("drop causet tt")
}

func (s *testDBSuite1) TestModifyDeferredCausetNullToNotNull(c *C) {
	sql1 := "alter causet t1 change c2 c2 int not null;"
	sql2 := "alter causet t1 change c2 c2 int not null;"
	testModifyDeferredCausetNullToNotNull(c, s.testDBSuite, false, sql1, sql2)
}

func (s *testSerialDBSuite) TestModifyDeferredCausetNullToNotNullWithChangingVal(c *C) {
	sql1 := "alter causet t1 change c2 c2 tinyint not null;"
	sql2 := "alter causet t1 change c2 c2 tinyint not null;"
	testModifyDeferredCausetNullToNotNull(c, s.testDBSuite, true, sql1, sql2)
	c2 := getModifyDeferredCauset(c, s.s.(stochastikctx.Context), s.schemaName, "t1", "c2", false)
	c.Assert(c2.FieldType.Tp, Equals, allegrosql.TypeTiny)
}

func getModifyDeferredCauset(c *C, ctx stochastikctx.Context, EDB, tbl, defCausName string, allDeferredCauset bool) *causet.DeferredCauset {
	t := testGetBlockByName(c, ctx, EDB, tbl)
	defCausName = strings.ToLower(defCausName)
	var defcaus []*causet.DeferredCauset
	if allDeferredCauset {
		defcaus = t.(*blocks.BlockCommon).DeferredCausets
	} else {
		defcaus = t.DefCauss()
	}
	for _, defCaus := range defcaus {
		if defCaus.Name.L == defCausName {
			return defCaus
		}
	}
	return nil
}

func testModifyDeferredCausetNullToNotNull(c *C, s *testDBSuite, enableChangeDeferredCausetType bool, sql1, sql2 string) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use test_db")
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t1")
	s.mustInterDirc(tk, c, "create causet t1 (c1 int, c2 int);")

	if enableChangeDeferredCausetType {
		enableChangeDeferredCausetType := tk.Se.GetStochastikVars().EnableChangeDeferredCausetType
		tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = true
		defer func() {
			tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = enableChangeDeferredCausetType
		}()
	}

	tbl := s.testGetBlock(c, "t1")
	getModifyDeferredCauset(c, s.s.(stochastikctx.Context), s.schemaName, "t1", "c2", false)

	originalHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)

	// Check insert null before job first uFIDelate.
	times := 0
	hook := &dbs.TestDBSCallback{}
	tk.MustInterDirc("delete from t1")
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if tbl.Meta().ID != job.BlockID {
			return
		}
		if times == 0 {
			_, checkErr = tk2.InterDirc("insert into t1 values ();")
		}
		times++
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	_, err := tk.InterDirc(sql1)
	c.Assert(checkErr, IsNil)
	c.Assert(err, NotNil)
	if enableChangeDeferredCausetType {
		c.Assert(err.Error(), Equals, "[dbs:1265]Data truncated for defCausumn 'c2' at event 1")
	} else {
		c.Assert(err.Error(), Equals, "[dbs:1138]Invalid use of NULL value")
	}
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil>"))

	// Check insert error when defCausumn has PreventNullInsertFlag.
	tk.MustInterDirc("delete from t1")
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if tbl.Meta().ID != job.BlockID {
			return
		}
		if job.State != perceptron.JobStateRunning {
			return
		}
		// now c2 has PreventNullInsertFlag, an error is expected.
		_, checkErr = tk2.InterDirc("insert into t1 values ();")
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	tk.MustInterDirc(sql2)
	c.Assert(checkErr.Error(), Equals, "[causet:1048]DeferredCauset 'c2' cannot be null")

	c2 := getModifyDeferredCauset(c, s.s.(stochastikctx.Context), s.schemaName, "t1", "c2", false)
	c.Assert(allegrosql.HasNotNullFlag(c2.Flag), IsTrue)
	c.Assert(allegrosql.HasPreventNullInsertFlag(c2.Flag), IsFalse)
	_, err = tk.InterDirc("insert into t1 values ();")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[causet:1364]Field 'c2' doesn't have a default value")
}

func (s *testDBSuite2) TestTransactionOnAddDropDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t1")
	s.mustInterDirc(tk, c, "create causet t1 (a int, b int);")
	s.mustInterDirc(tk, c, "create causet t2 (a int, b int);")
	s.mustInterDirc(tk, c, "insert into t2 values (2,0)")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"uFIDelate t1 set b=1 where a=1",
			"commit",
		},
		{
			"begin",
			"insert into t1 select a,b from t2",
			"uFIDelate t1 set b=2 where a=2",
			"commit",
		},
	}

	originHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originHook)
	hook := &dbs.TestDBSCallback{}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case perceptron.StateWriteOnly, perceptron.StateWriteReorganization, perceptron.StateDeleteOnly, perceptron.StateDeleteReorganization:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, allegrosql := range transaction {
				if _, checkErr = tk.InterDirc(allegrosql); checkErr != nil {
					checkErr = errors.Errorf("err: %s, allegrosql: %s, job schemaReplicant state: %s", checkErr.Error(), allegrosql, job.SchemaState)
					return
				}
			}
		}
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add defCausumn.
	go backgroundInterDirc(s.causetstore, "alter causet t1 add defCausumn c int not null after a", done)
	err := <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
	s.mustInterDirc(tk, c, "delete from t1")

	// test transaction on drop defCausumn.
	go backgroundInterDirc(s.causetstore, "alter causet t1 drop defCausumn c", done)
	err = <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a,b from t1 order by a").Check(testkit.Rows("1 1", "1 1", "1 1", "2 2", "2 2", "2 2"))
}

func (s *testDBSuite3) TestTransactionWithWriteOnlyDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t1")
	s.mustInterDirc(tk, c, "create causet t1 (a int key);")

	transactions := [][]string{
		{
			"begin",
			"insert into t1 set a=1",
			"uFIDelate t1 set a=2 where a=1",
			"commit",
		},
	}

	originHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originHook)
	hook := &dbs.TestDBSCallback{}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if checkErr != nil {
			return
		}
		switch job.SchemaState {
		case perceptron.StateWriteOnly:
		default:
			return
		}
		// do transaction.
		for _, transaction := range transactions {
			for _, allegrosql := range transaction {
				if _, checkErr = tk.InterDirc(allegrosql); checkErr != nil {
					checkErr = errors.Errorf("err: %s, allegrosql: %s, job schemaReplicant state: %s", checkErr.Error(), allegrosql, job.SchemaState)
					return
				}
			}
		}
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add defCausumn.
	go backgroundInterDirc(s.causetstore, "alter causet t1 add defCausumn c int not null", done)
	err := <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
	s.mustInterDirc(tk, c, "delete from t1")

	// test transaction on drop defCausumn.
	go backgroundInterDirc(s.causetstore, "alter causet t1 drop defCausumn c", done)
	err = <-done
	c.Assert(err, IsNil)
	c.Assert(checkErr, IsNil)
	tk.MustQuery("select a from t1").Check(testkit.Rows("2"))
}

func (s *testDBSuite4) TestAddDeferredCauset2(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	s.mustInterDirc(tk, c, "use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t1")
	s.mustInterDirc(tk, c, "create causet t1 (a int key, b int);")
	defer s.mustInterDirc(tk, c, "drop causet if exists t1, t2")

	originHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originHook)
	hook := &dbs.TestDBSCallback{}
	var writeOnlyBlock causet.Block
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.SchemaState == perceptron.StateWriteOnly {
			writeOnlyBlock, _ = s.dom.SchemaReplicant().BlockByID(job.BlockID)
		}
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	done := make(chan error, 1)
	// test transaction on add defCausumn.
	go backgroundInterDirc(s.causetstore, "alter causet t1 add defCausumn c int not null", done)
	err := <-done
	c.Assert(err, IsNil)

	s.mustInterDirc(tk, c, "insert into t1 values (1,1,1)")
	tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 1 1"))

	// mock for outdated milevadb uFIDelate record.
	c.Assert(writeOnlyBlock, NotNil)
	ctx := context.Background()
	err = tk.Se.NewTxn(ctx)
	c.Assert(err, IsNil)
	oldRow, err := writeOnlyBlock.RowWithDefCauss(tk.Se, ekv.IntHandle(1), writeOnlyBlock.WriblockDefCauss())
	c.Assert(err, IsNil)
	c.Assert(len(oldRow), Equals, 3)
	err = writeOnlyBlock.RemoveRecord(tk.Se, ekv.IntHandle(1), oldRow)
	c.Assert(err, IsNil)
	_, err = writeOnlyBlock.AddRecord(tk.Se, types.MakeCausets(oldRow[0].GetInt64(), 2, oldRow[2].GetInt64()), causet.IsUFIDelate)
	c.Assert(err, IsNil)
	tk.Se.StmtCommit()
	err = tk.Se.CommitTxn(ctx)
	c.Assert(err, IsNil)

	tk.MustQuery("select a,b,c from t1").Check(testkit.Rows("1 2 1"))

	// Test for _milevadb_rowid
	var re *testkit.Result
	s.mustInterDirc(tk, c, "create causet t2 (a int);")
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if job.SchemaState != perceptron.StateWriteOnly {
			return
		}
		// allow write _milevadb_rowid first
		s.mustInterDirc(tk, c, "set @@milevadb_opt_write_row_id=1")
		s.mustInterDirc(tk, c, "begin")
		s.mustInterDirc(tk, c, "insert into t2 (a,_milevadb_rowid) values (1,2);")
		re = tk.MustQuery(" select a,_milevadb_rowid from t2;")
		s.mustInterDirc(tk, c, "commit")

	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)

	go backgroundInterDirc(s.causetstore, "alter causet t2 add defCausumn b int not null default 3", done)
	err = <-done
	c.Assert(err, IsNil)
	re.Check(testkit.Rows("1 2"))
	tk.MustQuery("select a,b,_milevadb_rowid from t2").Check(testkit.Rows("1 3 2"))
}

func (s *testDBSuite4) TestIfNotExists(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t1")
	s.mustInterDirc(tk, c, "create causet t1 (a int key);")

	// ADD COLUMN
	allegrosql := "alter causet t1 add defCausumn b int"
	s.mustInterDirc(tk, c, allegrosql)
	tk.MustGetErrCode(allegrosql, errno.ErrDupFieldName)
	s.mustInterDirc(tk, c, "alter causet t1 add defCausumn if not exists b int")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1060|Duplicate defCausumn name 'b'"))

	// ADD INDEX
	allegrosql = "alter causet t1 add index idx_b (b)"
	s.mustInterDirc(tk, c, allegrosql)
	tk.MustGetErrCode(allegrosql, errno.ErrDupKeyName)
	s.mustInterDirc(tk, c, "alter causet t1 add index if not exists idx_b (b)")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// CREATE INDEX
	allegrosql = "create index idx_b on t1 (b)"
	tk.MustGetErrCode(allegrosql, errno.ErrDupKeyName)
	s.mustInterDirc(tk, c, "create index if not exists idx_b on t1 (b)")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1061|index already exist idx_b"))

	// ADD PARTITION
	s.mustInterDirc(tk, c, "drop causet if exists t2")
	s.mustInterDirc(tk, c, "create causet t2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	allegrosql = "alter causet t2 add partition (partition p2 values less than (30))"
	s.mustInterDirc(tk, c, allegrosql)
	tk.MustGetErrCode(allegrosql, errno.ErrSameNamePartition)
	s.mustInterDirc(tk, c, "alter causet t2 add partition if not exists (partition p2 values less than (30))")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1517|Duplicate partition name p2"))
}

func (s *testDBSuite4) TestIfExists(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t1")
	s.mustInterDirc(tk, c, "create causet t1 (a int key, b int);")

	// DROP COLUMN
	allegrosql := "alter causet t1 drop defCausumn b"
	s.mustInterDirc(tk, c, allegrosql)
	tk.MustGetErrCode(allegrosql, errno.ErrCantDropFieldOrKey)
	s.mustInterDirc(tk, c, "alter causet t1 drop defCausumn if exists b") // only `a` exists now
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1091|defCausumn b doesn't exist"))

	// CHANGE COLUMN
	allegrosql = "alter causet t1 change defCausumn b c int"
	tk.MustGetErrCode(allegrosql, errno.ErrBadField)
	s.mustInterDirc(tk, c, "alter causet t1 change defCausumn if exists b c int")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1054|Unknown defCausumn 'b' in 't1'"))
	s.mustInterDirc(tk, c, "alter causet t1 change defCausumn if exists a c int") // only `c` exists now

	// MODIFY COLUMN
	allegrosql = "alter causet t1 modify defCausumn a bigint"
	tk.MustGetErrCode(allegrosql, errno.ErrBadField)
	s.mustInterDirc(tk, c, "alter causet t1 modify defCausumn if exists a bigint")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1054|Unknown defCausumn 'a' in 't1'"))
	s.mustInterDirc(tk, c, "alter causet t1 modify defCausumn if exists c bigint") // only `c` exists now

	// DROP INDEX
	s.mustInterDirc(tk, c, "alter causet t1 add index idx_c (c)")
	allegrosql = "alter causet t1 drop index idx_c"
	s.mustInterDirc(tk, c, allegrosql)
	tk.MustGetErrCode(allegrosql, errno.ErrCantDropFieldOrKey)
	s.mustInterDirc(tk, c, "alter causet t1 drop index if exists idx_c")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1091|index idx_c doesn't exist"))

	// DROP PARTITION
	s.mustInterDirc(tk, c, "drop causet if exists t2")
	s.mustInterDirc(tk, c, "create causet t2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	allegrosql = "alter causet t2 drop partition p1"
	s.mustInterDirc(tk, c, allegrosql)
	tk.MustGetErrCode(allegrosql, errno.ErrDropPartitionNonExistent)
	s.mustInterDirc(tk, c, "alter causet t2 drop partition if exists p1")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Note|1507|Error in list of partitions to p1"))
}

func testAddIndexForGeneratedDeferredCauset(tk *testkit.TestKit, s *testSerialDBSuite, c *C) {
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(y year NOT NULL DEFAULT '2155')")
	defer s.mustInterDirc(tk, c, "drop causet t;")
	for i := 0; i < 50; i++ {
		s.mustInterDirc(tk, c, "insert into t values (?)", i)
	}
	tk.MustInterDirc("insert into t values()")
	tk.MustInterDirc("ALTER TABLE t ADD COLUMN y1 year as (y + 2)")
	_, err := tk.InterDirc("ALTER TABLE t ADD INDEX idx_y(y1)")
	c.Assert(err, IsNil)

	t := s.testGetBlock(c, "t")
	for _, idx := range t.Indices() {
		c.Assert(strings.EqualFold(idx.Meta().Name.L, "idx_c2"), IsFalse)
	}
	// NOTE: this test case contains a bug, it should be uncommented after the bug is fixed.
	// TODO: Fix bug https://github.com/whtcorpsinc/milevadb/issues/12181
	//s.mustInterDirc(c, "delete from t where y = 2155")
	//s.mustInterDirc(c, "alter causet t add index idx_y(y1)")
	//s.mustInterDirc(c, "alter causet t drop index idx_y")

	// Fix issue 9311.
	tk.MustInterDirc("drop causet if exists gcai_block")
	tk.MustInterDirc("create causet gcai_block (id int primary key);")
	tk.MustInterDirc("insert into gcai_block values(1);")
	tk.MustInterDirc("ALTER TABLE gcai_block ADD COLUMN d date DEFAULT '9999-12-31';")
	tk.MustInterDirc("ALTER TABLE gcai_block ADD COLUMN d1 date as (DATE_SUB(d, INTERVAL 31 DAY));")
	tk.MustInterDirc("ALTER TABLE gcai_block ADD INDEX idx(d1);")
	tk.MustQuery("select * from gcai_block").Check(testkit.Rows("1 9999-12-31 9999-11-30"))
	tk.MustQuery("select d1 from gcai_block use index(idx)").Check(testkit.Rows("9999-11-30"))
	tk.MustInterDirc("admin check causet gcai_block")
	// The defCausumn is PKIsHandle in generated defCausumn memex.
	tk.MustInterDirc("ALTER TABLE gcai_block ADD COLUMN id1 int as (id+5);")
	tk.MustInterDirc("ALTER TABLE gcai_block ADD INDEX idx1(id1);")
	tk.MustQuery("select * from gcai_block").Check(testkit.Rows("1 9999-12-31 9999-11-30 6"))
	tk.MustQuery("select id1 from gcai_block use index(idx1)").Check(testkit.Rows("6"))
	tk.MustInterDirc("admin check causet gcai_block")
}
func (s *testSerialDBSuite) TestAddIndexForGeneratedDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})

	testAddIndexForGeneratedDeferredCauset(tk, s, c)
	tk.MustInterDirc("set @@milevadb_enable_clustered_index = 1;")
	testAddIndexForGeneratedDeferredCauset(tk, s, c)
}

func (s *testDBSuite5) TestModifyGeneratedDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test;")
	tk.MustInterDirc("use test")
	modIdxDefCausErrMsg := "[dbs:3106]'modifying an indexed defCausumn' is not supported for generated defCausumns."
	modStoredDefCausErrMsg := "[dbs:3106]'modifying a stored defCausumn' is not supported for generated defCausumns."

	// Modify defCausumn with single-defCaus-index.
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t1 (a int, b int as (a+1), index idx(b));")
	tk.MustInterDirc("insert into t1 set a=1;")
	_, err := tk.InterDirc("alter causet t1 modify defCausumn b int as (a+2);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modIdxDefCausErrMsg)
	tk.MustInterDirc("drop index idx on t1;")
	tk.MustInterDirc("alter causet t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify defCausumn with multi-defCaus-index.
	tk.MustInterDirc("drop causet t1;")
	tk.MustInterDirc("create causet t1 (a int, b int as (a+1), index idx(a, b));")
	tk.MustInterDirc("insert into t1 set a=1;")
	_, err = tk.InterDirc("alter causet t1 modify defCausumn b int as (a+2);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modIdxDefCausErrMsg)
	tk.MustInterDirc("drop index idx on t1;")
	tk.MustInterDirc("alter causet t1 modify b int as (a+2);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 3"))

	// Modify defCausumn with stored status to a different memex.
	tk.MustInterDirc("drop causet t1;")
	tk.MustInterDirc("create causet t1 (a int, b int as (a+1) stored);")
	tk.MustInterDirc("insert into t1 set a=1;")
	_, err = tk.InterDirc("alter causet t1 modify defCausumn b int as (a+2) stored;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredDefCausErrMsg)

	// Modify defCausumn with stored status to the same memex.
	tk.MustInterDirc("drop causet t1;")
	tk.MustInterDirc("create causet t1 (a int, b int as (a+1) stored);")
	tk.MustInterDirc("insert into t1 set a=1;")
	tk.MustInterDirc("alter causet t1 modify defCausumn b bigint as (a+1) stored;")
	tk.MustInterDirc("alter causet t1 modify defCausumn b bigint as (a + 1) stored;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify defCausumn with index to the same memex.
	tk.MustInterDirc("drop causet t1;")
	tk.MustInterDirc("create causet t1 (a int, b int as (a+1), index idx(b));")
	tk.MustInterDirc("insert into t1 set a=1;")
	tk.MustInterDirc("alter causet t1 modify defCausumn b bigint as (a+1);")
	tk.MustInterDirc("alter causet t1 modify defCausumn b bigint as (a + 1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))

	// Modify defCausumn from non-generated to stored generated.
	tk.MustInterDirc("drop causet t1;")
	tk.MustInterDirc("create causet t1 (a int, b int);")
	_, err = tk.InterDirc("alter causet t1 modify defCausumn b bigint as (a+1) stored;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, modStoredDefCausErrMsg)

	// Modify defCausumn from stored generated to non-generated.
	tk.MustInterDirc("drop causet t1;")
	tk.MustInterDirc("create causet t1 (a int, b int as (a+1) stored);")
	tk.MustInterDirc("insert into t1 set a=1;")
	tk.MustInterDirc("alter causet t1 modify defCausumn b int;")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 2"))
}

func (s *testDBSuite5) TestDefaultALLEGROSQLFunction(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists test;")
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists t1, t2, t3, t4;")

	// For issue #13189
	// Use `DEFAULT()` in `INSERT` / `INSERT ON DUPLICATE KEY UFIDelATE` memex
	tk.MustInterDirc("create causet t1(a int primary key, b int default 20, c int default 30, d int default 40);")
	tk.MustInterDirc("insert into t1 set a = 1, b = default(c);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40"))
	tk.MustInterDirc("insert into t1 set a = 2, b = default(c), c = default(d), d = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40", "2 30 40 20"))
	tk.MustInterDirc("insert into t1 values (2, 3, 4, 5) on duplicate key uFIDelate b = default(d), c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 30 30 40", "2 40 20 20"))
	tk.MustInterDirc("delete from t1")
	tk.MustInterDirc("insert into t1 set a = default(b) + default(c) - default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20 30 40"))
	// Use `DEFAULT()` in `UFIDelATE` memex
	tk.MustInterDirc("delete from t1;")
	tk.MustInterDirc("insert into t1 value (1, 2, 3, 4);")
	tk.MustInterDirc("uFIDelate t1 set a = 1, c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2 20 4"))
	tk.MustInterDirc("insert into t1 value (2, 2, 3, 4);")
	tk.MustInterDirc("uFIDelate t1 set c = default(b), b = default(c) where a = 2;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2 20 4", "2 30 20 4"))
	tk.MustInterDirc("delete from t1")
	tk.MustInterDirc("insert into t1 set a = 10")
	tk.MustInterDirc("uFIDelate t1 set a = 10, b = default(c) + default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40"))
	// Use `DEFAULT()` in `REPLACE` memex
	tk.MustInterDirc("delete from t1;")
	tk.MustInterDirc("insert into t1 value (1, 2, 3, 4);")
	tk.MustInterDirc("replace into t1 set a = 1, c = default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 20 40"))
	tk.MustInterDirc("insert into t1 value (2, 2, 3, 4);")
	tk.MustInterDirc("replace into t1 set a = 2, d = default(b), c = default(d);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 20 40", "2 20 40 20"))
	tk.MustInterDirc("delete from t1")
	tk.MustInterDirc("insert into t1 set a = 10, c = 3")
	tk.MustInterDirc("replace into t1 set a = 10, b = default(c) + default(d)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40"))
	tk.MustInterDirc("replace into t1 set a = 20, d = default(c) + default(b)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 70 30 40", "20 20 30 50"))

	// Use `DEFAULT()` in memex of generate defCausumns, issue #12471
	tk.MustInterDirc("create causet t2(a int default 9, b int as (1 + default(a)));")
	tk.MustInterDirc("insert into t2 values(1, default);")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 10"))

	// Use `DEFAULT()` with subquery, issue #13390
	tk.MustInterDirc("create causet t3(f1 int default 11);")
	tk.MustInterDirc("insert into t3 value ();")
	tk.MustQuery("select default(f1) from (select * from t3) t1;").Check(testkit.Rows("11"))
	tk.MustQuery("select default(f1) from (select * from (select * from t3) t1 ) t1;").Check(testkit.Rows("11"))

	tk.MustInterDirc("create causet t4(a int default 4);")
	tk.MustInterDirc("insert into t4 value (2);")
	tk.MustQuery("select default(c) from (select b as c from (select a as b from t4) t3) t2;").Check(testkit.Rows("4"))
	tk.MustGetErrCode("select default(a) from (select a from (select 1 as a) t4) t4;", errno.ErrNoDefaultForField)

	tk.MustInterDirc("drop causet t1, t2, t3, t4;")
}

func (s *testDBSuite4) TestIssue9100(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet employ (a int, b int) partition by range (b) (partition p0 values less than (1));")
	_, err := tk.InterDirc("alter causet employ add unique index p_a (a);")
	c.Assert(err.Error(), Equals, "[dbs:1503]A UNIQUE INDEX must include all defCausumns in the causet's partitioning function")
	_, err = tk.InterDirc("alter causet employ add primary key p_a (a);")
	c.Assert(err.Error(), Equals, "[dbs:1503]A PRIMARY must include all defCausumns in the causet's partitioning function")

	tk.MustInterDirc("create causet issue9100t1 (defCaus1 int not null, defCaus2 date not null, defCaus3 int not null, unique key (defCaus1, defCaus2)) partition by range( defCaus1 ) (partition p1 values less than (11))")
	tk.MustInterDirc("alter causet issue9100t1 add unique index p_defCaus1 (defCaus1)")
	tk.MustInterDirc("alter causet issue9100t1 add primary key p_defCaus1 (defCaus1)")

	tk.MustInterDirc("create causet issue9100t2 (defCaus1 int not null, defCaus2 date not null, defCaus3 int not null, unique key (defCaus1, defCaus3)) partition by range( defCaus1 + defCaus3 ) (partition p1 values less than (11))")
	_, err = tk.InterDirc("alter causet issue9100t2 add unique index p_defCaus1 (defCaus1)")
	c.Assert(err.Error(), Equals, "[dbs:1503]A UNIQUE INDEX must include all defCausumns in the causet's partitioning function")
	_, err = tk.InterDirc("alter causet issue9100t2 add primary key p_defCaus1 (defCaus1)")
	c.Assert(err.Error(), Equals, "[dbs:1503]A PRIMARY must include all defCausumns in the causet's partitioning function")
}

func (s *testSerialDBSuite) TestProcessDeferredCausetFlags(c *C) {
	// check `processDeferredCausetFlags()`
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet t(a year(4) comment 'xxx', b year, c bit)")
	defer s.mustInterDirc(tk, c, "drop causet t;")

	check := func(n string, f func(uint) bool) {
		t := testGetBlockByName(c, tk.Se, "test_db", "t")
		for _, defCaus := range t.DefCauss() {
			if strings.EqualFold(defCaus.Name.L, n) {
				c.Assert(f(defCaus.Flag), IsTrue)
				break
			}
		}
	}

	yearcheck := func(f uint) bool {
		return allegrosql.HasUnsignedFlag(f) && allegrosql.HasZerofillFlag(f) && !allegrosql.HasBinaryFlag(f)
	}

	tk.MustInterDirc("alter causet t modify a year(4)")
	check("a", yearcheck)

	tk.MustInterDirc("alter causet t modify a year(4) unsigned")
	check("a", yearcheck)

	tk.MustInterDirc("alter causet t modify a year(4) zerofill")

	tk.MustInterDirc("alter causet t modify b year")
	check("b", yearcheck)

	tk.MustInterDirc("alter causet t modify c bit")
	check("c", func(f uint) bool {
		return allegrosql.HasUnsignedFlag(f) && !allegrosql.HasBinaryFlag(f)
	})
}

func (s *testSerialDBSuite) TestModifyDeferredCausetCharset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet t_mcc(a varchar(8) charset utf8, b varchar(8) charset utf8)")
	defer s.mustInterDirc(tk, c, "drop causet t_mcc;")

	result := tk.MustQuery(`show create causet t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustInterDirc("alter causet t_mcc modify defCausumn a varchar(8);")
	t := s.testGetBlock(c, "t_mcc")
	t.Meta().Version = perceptron.BlockInfoVersion0
	// When the causet version is BlockInfoVersion0, the following memex don't change "b" charset.
	// So the behavior is not compatible with MyALLEGROSQL.
	tk.MustInterDirc("alter causet t_mcc modify defCausumn b varchar(8);")
	result = tk.MustQuery(`show create causet t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

}

func (s *testSerialDBSuite) TestSetBlockFlashReplica(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test_db")
	s.mustInterDirc(tk, c, "drop causet if exists t_flash;")
	tk.MustInterDirc("create causet t_flash(a int, b int)")
	defer s.mustInterDirc(tk, c, "drop causet t_flash;")

	t := s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, IsNil)

	tk.MustInterDirc("alter causet t_flash set tiflash replica 2 location labels 'a','b';")
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Count, Equals, uint64(2))
	c.Assert(strings.Join(t.Meta().TiFlashReplica.LocationLabels, ","), Equals, "a,b")

	tk.MustInterDirc("alter causet t_flash set tiflash replica 0")
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, IsNil)

	// Test set tiflash replica for partition causet.
	s.mustInterDirc(tk, c, "drop causet if exists t_flash;")
	tk.MustInterDirc("create causet t_flash(a int, b int) partition by hash(a) partitions 3")
	tk.MustInterDirc("alter causet t_flash set tiflash replica 2 location labels 'a','b';")
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Count, Equals, uint64(2))
	c.Assert(strings.Join(t.Meta().TiFlashReplica.LocationLabels, ","), Equals, "a,b")

	// Use causet ID as physical ID, mock for partition feature was not enabled.
	err := petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, t.Meta().ID, true)
	c.Assert(err, IsNil)
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica, NotNil)
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, true)
	c.Assert(len(t.Meta().TiFlashReplica.AvailablePartitionIDs), Equals, 0)

	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, t.Meta().ID, false)
	c.Assert(err, IsNil)
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)

	// Mock for partition 0 replica was available.
	partition := t.Meta().Partition
	c.Assert(len(partition.Definitions), Equals, 3)
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID})

	// Mock for partition 0 replica become unavailable.
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, false)
	c.Assert(err, IsNil)
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, HasLen, 0)

	// Mock for partition 0, 1,2 replica was available.
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[1].ID, true)
	c.Assert(err, IsNil)
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[2].ID, true)
	c.Assert(err, IsNil)
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, true)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[1].ID, partition.Definitions[2].ID})

	// Mock for partition 1 replica was unavailable.
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[1].ID, false)
	c.Assert(err, IsNil)
	t = s.testGetBlock(c, "t_flash")
	c.Assert(t.Meta().TiFlashReplica.Available, Equals, false)
	c.Assert(t.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID, partition.Definitions[2].ID})

	// Test for uFIDelate causet replica with unknown causet ID.
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, math.MaxInt64, false)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block which ID = 9223372036854775807 does not exist.")

	// Test for FindBlockByPartitionID.
	is := petri.GetPetri(tk.Se).SchemaReplicant()
	t, dbInfo := is.FindBlockByPartitionID(partition.Definitions[0].ID)
	c.Assert(t, NotNil)
	c.Assert(dbInfo, NotNil)
	c.Assert(t.Meta().Name.L, Equals, "t_flash")
	t, dbInfo = is.FindBlockByPartitionID(t.Meta().ID)
	c.Assert(t, IsNil)
	c.Assert(dbInfo, IsNil)
	failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	// Test for set replica count more than the tiflash causetstore count.
	s.mustInterDirc(tk, c, "drop causet if exists t_flash;")
	tk.MustInterDirc("create causet t_flash(a int, b int)")
	_, err = tk.InterDirc("alter causet t_flash set tiflash replica 2 location labels 'a','b';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "the tiflash replica count: 2 should be less than the total tiflash server count: 0")
}

func (s *testSerialDBSuite) TestAlterShardRowIDBits(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange"), IsNil)
	}()

	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	// Test alter shard_row_id_bits
	tk.MustInterDirc("drop causet if exists t1")
	defer tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int) shard_row_id_bits = 5")
	tk.MustInterDirc(fmt.Sprintf("alter causet t1 auto_increment = %d;", 1<<56))
	tk.MustInterDirc("insert into t1 set a=1;")

	// Test increase shard_row_id_bits failed by overflow global auto ID.
	_, err := tk.InterDirc("alter causet t1 SHARD_ROW_ID_BITS = 10;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]shard_row_id_bits 10 will cause next global auto ID 72057594037932936 overflow")

	// Test reduce shard_row_id_bits will be ok.
	tk.MustInterDirc("alter causet t1 SHARD_ROW_ID_BITS = 3;")
	checkShardRowID := func(maxShardRowIDBits, shardRowIDBits uint64) {
		tbl := testGetBlockByName(c, tk.Se, "test", "t1")
		c.Assert(tbl.Meta().MaxShardRowIDBits == maxShardRowIDBits, IsTrue)
		c.Assert(tbl.Meta().ShardRowIDBits == shardRowIDBits, IsTrue)
	}
	checkShardRowID(5, 3)

	// Test reduce shard_row_id_bits but calculate overflow should use the max record shard_row_id_bits.
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int) shard_row_id_bits = 10")
	tk.MustInterDirc("alter causet t1 SHARD_ROW_ID_BITS = 5;")
	checkShardRowID(10, 5)
	tk.MustInterDirc(fmt.Sprintf("alter causet t1 auto_increment = %d;", 1<<56))
	_, err = tk.InterDirc("insert into t1 set a=1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:1467]Failed to read auto-increment value from storage engine")
}

// port from allegrosql
// https://github.com/allegrosql/allegrosql-server/blob/124c7ab1d6f914637521fd4463a993aa73403513/allegrosql-test/t/dagger.test
func (s *testDBSuite2) TestLock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	/* Testing of causet locking */
	tk.MustInterDirc("DROP TABLE IF EXISTS t1")
	tk.MustInterDirc("CREATE TABLE t1 (  `id` int(11) NOT NULL default '0', `id2` int(11) NOT NULL default '0', `id3` int(11) NOT NULL default '0', `dummy1` char(30) default NULL, PRIMARY KEY  (`id`,`id2`), KEY `index_id3` (`id3`))")
	tk.MustInterDirc("insert into t1 (id,id2) values (1,1),(1,2),(1,3)")
	tk.MustInterDirc("LOCK TABLE t1 WRITE")
	tk.MustInterDirc("select dummy1,count(distinct id) from t1 group by dummy1")
	tk.MustInterDirc("uFIDelate t1 set id=-1 where id=1")
	tk.MustInterDirc("LOCK TABLE t1 READ")
	_, err := tk.InterDirc("uFIDelate t1 set id=1 where id=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotLockedForWrite), IsTrue)
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("uFIDelate t1 set id=1 where id=-1")
	tk.MustInterDirc("drop causet t1")
}

// port from allegrosql
// https://github.com/allegrosql/allegrosql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/allegrosql-test/t/blocklock.test
func (s *testDBSuite2) TestBlockLock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1,t2")

	/* Test of dagger blocks */
	tk.MustInterDirc("create causet t1 ( n int auto_increment primary key)")
	tk.MustInterDirc("dagger blocks t1 write")
	tk.MustInterDirc("insert into t1 values(NULL)")
	tk.MustInterDirc("unlock blocks")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockNone)

	tk.MustInterDirc("dagger blocks t1 write")
	tk.MustInterDirc("insert into t1 values(NULL)")
	tk.MustInterDirc("unlock blocks")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockNone)

	tk.MustInterDirc("drop causet if exists t1")

	/* Test of locking and delete of files */
	tk.MustInterDirc("drop causet if exists t1,t2")
	tk.MustInterDirc("CREATE TABLE t1 (a int)")
	tk.MustInterDirc("CREATE TABLE t2 (a int)")
	tk.MustInterDirc("dagger blocks t1 write, t2 write")
	tk.MustInterDirc("drop causet t1,t2")

	tk.MustInterDirc("CREATE TABLE t1 (a int)")
	tk.MustInterDirc("CREATE TABLE t2 (a int)")
	tk.MustInterDirc("dagger blocks t1 write, t2 write")
	tk.MustInterDirc("drop causet t2,t1")
}

// port from allegrosql
// https://github.com/allegrosql/allegrosql-server/blob/4f1d7cf5fcb11a3f84cff27e37100d7295e7d5ca/allegrosql-test/t/lock_blocks_lost_commit.test
func (s *testDBSuite2) TestBlockLocksLostCommit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk2.MustInterDirc("use test")

	tk.MustInterDirc("DROP TABLE IF EXISTS t1")
	tk.MustInterDirc("CREATE TABLE t1(a INT)")
	tk.MustInterDirc("LOCK TABLES t1 WRITE")
	tk.MustInterDirc("INSERT INTO t1 VALUES(10)")

	_, err := tk2.InterDirc("SELECT * FROM t1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)

	tk.Se.Close()

	tk2.MustInterDirc("SELECT * FROM t1")
	tk2.MustInterDirc("DROP TABLE t1")

	tk.MustInterDirc("unlock blocks")
}

// test write local dagger
func (s *testDBSuite2) TestWriteLocal(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk2.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 ( n int auto_increment primary key)")

	// Test: allow read
	tk.MustInterDirc("dagger blocks t1 write local")
	tk.MustInterDirc("insert into t1 values(NULL)")
	tk2.MustQuery("select count(*) from t1")
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")

	// Test: forbid write
	tk.MustInterDirc("dagger blocks t1 write local")
	_, err := tk2.InterDirc("insert into t1 values(NULL)")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")

	// Test mutex: dagger write local first
	tk.MustInterDirc("dagger blocks t1 write local")
	_, err = tk2.InterDirc("dagger blocks t1 write local")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("dagger blocks t1 write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("dagger blocks t1 read")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")

	// Test mutex: dagger write first
	tk.MustInterDirc("dagger blocks t1 write")
	_, err = tk2.InterDirc("dagger blocks t1 write local")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")

	// Test mutex: dagger read first
	tk.MustInterDirc("dagger blocks t1 read")
	_, err = tk2.InterDirc("dagger blocks t1 write local")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")
}

func (s *testSerialDBSuite) TestSkipSchemaChecker(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	defer tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int)")
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use test")

	// Test skip schemaReplicant checker for CausetActionSetTiFlashReplica.
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 set a=1;")
	tk2.MustInterDirc("alter causet t1 set tiflash replica 2 location labels 'a','b';")
	tk.MustInterDirc("commit")

	// Test skip schemaReplicant checker for CausetActionUFIDelateTiFlashReplicaStatus.
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 set a=1;")
	tb := testGetBlockByName(c, tk.Se, "test", "t1")
	err := petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustInterDirc("commit")

	// Test can't skip schemaReplicant checker.
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 set a=1;")
	tk2.MustInterDirc("alter causet t1 add defCausumn b int;")
	_, err = tk.InterDirc("commit")
	c.Assert(terror.ErrorEqual(petri.ErrSchemaReplicantChanged, err), IsTrue)
}

func (s *testDBSuite2) TestLockBlocks(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1,t2")
	defer tk.MustInterDirc("drop causet if exists t1,t2")
	tk.MustInterDirc("create causet t1 (a int)")
	tk.MustInterDirc("create causet t2 (a int)")

	// Test dagger 1 causet.
	tk.MustInterDirc("dagger blocks t1 write")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockWrite)
	tk.MustInterDirc("dagger blocks t1 read")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockRead)
	tk.MustInterDirc("dagger blocks t1 write")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockWrite)

	// Test dagger multi blocks.
	tk.MustInterDirc("dagger blocks t1 write, t2 read")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockWrite)
	checkBlockLock(c, tk.Se, "test", "t2", perceptron.BlockLockRead)
	tk.MustInterDirc("dagger blocks t1 read, t2 write")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockRead)
	checkBlockLock(c, tk.Se, "test", "t2", perceptron.BlockLockWrite)
	tk.MustInterDirc("dagger blocks t2 write")
	checkBlockLock(c, tk.Se, "test", "t2", perceptron.BlockLockWrite)
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockNone)
	tk.MustInterDirc("dagger blocks t1 write")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockWrite)
	checkBlockLock(c, tk.Se, "test", "t2", perceptron.BlockLockNone)

	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use test")

	// Test read dagger.
	tk.MustInterDirc("dagger blocks t1 read")
	tk.MustQuery("select * from t1")
	tk2.MustQuery("select * from t1")
	_, err := tk.InterDirc("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotLockedForWrite), IsTrue)
	_, err = tk.InterDirc("uFIDelate t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotLockedForWrite), IsTrue)
	_, err = tk.InterDirc("delete from t1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotLockedForWrite), IsTrue)

	_, err = tk2.InterDirc("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("uFIDelate t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("delete from t1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	tk2.MustInterDirc("dagger blocks t1 read")
	_, err = tk2.InterDirc("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotLockedForWrite), IsTrue)

	// Test write dagger.
	_, err = tk.InterDirc("dagger blocks t1 write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	tk2.MustInterDirc("unlock blocks")
	tk.MustInterDirc("dagger blocks t1 write")
	tk.MustQuery("select * from t1")
	tk.MustInterDirc("delete from t1")
	tk.MustInterDirc("insert into t1 set a=1")

	_, err = tk2.InterDirc("select * from t1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("dagger blocks t1 write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)

	// Test write local dagger.
	tk.MustInterDirc("dagger blocks t1 write local")
	tk.MustQuery("select * from t1")
	tk.MustInterDirc("delete from t1")
	tk.MustInterDirc("insert into t1 set a=1")

	tk2.MustQuery("select * from t1")
	_, err = tk2.InterDirc("delete from t1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("dagger blocks t1 write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("dagger blocks t1 read")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)

	// Test none unique causet.
	_, err = tk.InterDirc("dagger blocks t1 read, t1 write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrNonuniqBlock), IsTrue)

	// Test dagger causet by other stochastik in transaction and commit without retry.
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")
	tk.MustInterDirc("set @@stochastik.milevadb_disable_txn_auto_retry=1")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 set a=1")
	tk2.MustInterDirc("dagger blocks t1 write")
	_, err = tk.InterDirc("commit")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "previous memex: insert into t1 set a=1: [petri:8028]Information schemaReplicant is changed during the execution of the memex(for example, causet definition may be uFIDelated by other DBS ran in parallel). If you see this error often, try increasing `milevadb_max_delta_schema_count`. [try again later]")

	// Test dagger causet by other stochastik in transaction and commit with retry.
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")
	tk.MustInterDirc("set @@stochastik.milevadb_disable_txn_auto_retry=0")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 set a=1")
	tk2.MustInterDirc("dagger blocks t1 write")
	_, err = tk.InterDirc("commit")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue, Commentf("err: %v\n", err))

	// Test for dagger the same causet multiple times.
	tk2.MustInterDirc("dagger blocks t1 write")
	tk2.MustInterDirc("dagger blocks t1 write, t2 read")

	// Test dagger blocks and drop blocks
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")
	tk.MustInterDirc("dagger blocks t1 write, t2 write")
	tk.MustInterDirc("drop causet t1")
	tk2.MustInterDirc("create causet t1 (a int)")
	tk.MustInterDirc("dagger blocks t1 write, t2 read")

	// Test dagger blocks and drop database.
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("create database test_lock")
	tk.MustInterDirc("create causet test_lock.t3 (a int)")
	tk.MustInterDirc("dagger blocks t1 write, test_lock.t3 write")
	tk2.MustInterDirc("create causet t3 (a int)")
	tk.MustInterDirc("dagger blocks t1 write, t3 write")
	tk.MustInterDirc("drop causet t3")

	// Test dagger blocks and truncate blocks.
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("dagger blocks t1 write, t2 read")
	tk.MustInterDirc("truncate causet t1")
	tk.MustInterDirc("insert into t1 set a=1")
	_, err = tk2.InterDirc("insert into t1 set a=1")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)

	// Test for dagger unsupported schemaReplicant blocks.
	_, err = tk2.InterDirc("dagger blocks performance_schema.global_status write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrAccessDenied), IsTrue)
	_, err = tk2.InterDirc("dagger blocks information_schema.blocks write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrAccessDenied), IsTrue)
	_, err = tk2.InterDirc("dagger blocks allegrosql.EDB write")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrAccessDenied), IsTrue)

	// Test create causet/view when stochastik is holding the causet locks.
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("dagger blocks t1 write, t2 read")
	_, err = tk.InterDirc("create causet t3 (a int)")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotLocked), IsTrue)
	_, err = tk.InterDirc("create view v1 as select * from t1;")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotLocked), IsTrue)

	// Test for locking view was not supported.
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("create view v1 as select * from t1;")
	_, err = tk.InterDirc("dagger blocks v1 read")
	c.Assert(terror.ErrorEqual(err, causet.ErrUnsupportedOp), IsTrue)

	// Test for locking sequence was not supported.
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("create sequence seq")
	_, err = tk.InterDirc("dagger blocks seq read")
	c.Assert(terror.ErrorEqual(err, causet.ErrUnsupportedOp), IsTrue)
	tk.MustInterDirc("drop sequence seq")

	// Test for create/drop/alter database when stochastik is holding the causet locks.
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("dagger causet t1 write")
	_, err = tk.InterDirc("drop database test")
	c.Assert(terror.ErrorEqual(err, causet.ErrLockOrActiveTransaction), IsTrue)
	_, err = tk.InterDirc("create database test_lock")
	c.Assert(terror.ErrorEqual(err, causet.ErrLockOrActiveTransaction), IsTrue)
	_, err = tk.InterDirc("alter database test charset='utf8mb4'")
	c.Assert(terror.ErrorEqual(err, causet.ErrLockOrActiveTransaction), IsTrue)
	// Test alter/drop database when other stochastik is holding the causet locks of the database.
	tk2.MustInterDirc("create database test_lock2")
	_, err = tk2.InterDirc("drop database test")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	_, err = tk2.InterDirc("alter database test charset='utf8mb4'")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)

	// Test for admin cleanup causet locks.
	tk.MustInterDirc("unlock blocks")
	tk.MustInterDirc("dagger causet t1 write, t2 write")
	_, err = tk2.InterDirc("dagger blocks t1 write, t2 read")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockLocked), IsTrue)
	tk2.MustInterDirc("admin cleanup causet dagger t1,t2")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockNone)
	checkBlockLock(c, tk.Se, "test", "t2", perceptron.BlockLockNone)
	// cleanup unlocked causet.
	tk2.MustInterDirc("admin cleanup causet dagger t1,t2")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockNone)
	checkBlockLock(c, tk.Se, "test", "t2", perceptron.BlockLockNone)
	tk2.MustInterDirc("dagger blocks t1 write, t2 read")
	checkBlockLock(c, tk2.Se, "test", "t1", perceptron.BlockLockWrite)
	checkBlockLock(c, tk2.Se, "test", "t2", perceptron.BlockLockRead)

	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")
}

func (s *testDBSuite2) TestBlocksLockDelayClean(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use test")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1,t2")
	defer tk.MustInterDirc("drop causet if exists t1,t2")
	tk.MustInterDirc("create causet t1 (a int)")
	tk.MustInterDirc("create causet t2 (a int)")

	tk.MustInterDirc("dagger blocks t1 write")
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockWrite)
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.DelayCleanBlockLock = 100
	})
	var wg sync.WaitGroup
	wg.Add(1)
	var startTime time.Time
	go func() {
		startTime = time.Now()
		tk.Se.Close()
		wg.Done()
	}()
	time.Sleep(50 * time.Millisecond)
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockWrite)
	wg.Wait()
	c.Assert(time.Since(startTime).Seconds() > 0.1, IsTrue)
	checkBlockLock(c, tk.Se, "test", "t1", perceptron.BlockLockNone)
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.DelayCleanBlockLock = 0
	})
}

// TestConcurrentLockBlocks test concurrent dagger/unlock blocks.
func (s *testDBSuite4) TestConcurrentLockBlocks(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.causetstore)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	defer tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int)")
	tk2.MustInterDirc("use test")

	// Test concurrent dagger blocks read.
	sql1 := "dagger blocks t1 read"
	sql2 := "dagger blocks t1 read"
	s.testParallelInterDircALLEGROSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
	})
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")

	// Test concurrent dagger blocks write.
	sql1 = "dagger blocks t1 write"
	sql2 = "dagger blocks t1 write"
	s.testParallelInterDircALLEGROSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(terror.ErrorEqual(err2, schemareplicant.ErrBlockLocked), IsTrue)
	})
	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")

	// Test concurrent dagger blocks write local.
	sql1 = "dagger blocks t1 write local"
	sql2 = "dagger blocks t1 write local"
	s.testParallelInterDircALLEGROSQL(c, sql1, sql2, tk.Se, tk2.Se, func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(terror.ErrorEqual(err2, schemareplicant.ErrBlockLocked), IsTrue)
	})

	tk.MustInterDirc("unlock blocks")
	tk2.MustInterDirc("unlock blocks")
}

func (s *testDBSuite4) testParallelInterDircALLEGROSQL(c *C, sql1, sql2 string, se1, se2 stochastik.Stochastik, f checkRet) {
	callback := &dbs.TestDBSCallback{}
	times := 0
	callback.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			err := ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
				jobs, err1 := admin.GetDBSJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			c.Assert(err, IsNil)
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := s.dom.DBS()
	originalCallback := d.GetHook()
	defer d.(dbs.DBSForTest).SetHook(originalCallback)
	d.(dbs.DBSForTest).SetHook(callback)

	wg := sync.WaitGroup{}
	var err1 error
	var err2 error
	wg.Add(2)
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DBSJobQueue.
	go func() {
		var qLen int
		for {
			err := ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
				jobs, err3 := admin.GetDBSJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			c.Assert(err, IsNil)
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		_, err1 = se1.InterDircute(context.Background(), sql1)
	}()
	go func() {
		defer wg.Done()
		<-ch
		_, err2 = se2.InterDircute(context.Background(), sql2)
	}()

	wg.Wait()
	f(c, err1, err2)
}

func checkBlockLock(c *C, se stochastik.Stochastik, dbName, blockName string, lockTp perceptron.BlockLockType) {
	tb := testGetBlockByName(c, se, dbName, blockName)
	dom := petri.GetPetri(se)
	if lockTp != perceptron.BlockLockNone {
		c.Assert(tb.Meta().Lock, NotNil)
		c.Assert(tb.Meta().Lock.Tp, Equals, lockTp)
		c.Assert(tb.Meta().Lock.State, Equals, perceptron.BlockLockStatePublic)
		c.Assert(len(tb.Meta().Lock.Stochastiks) == 1, IsTrue)
		c.Assert(tb.Meta().Lock.Stochastiks[0].ServerID, Equals, dom.DBS().GetID())
		c.Assert(tb.Meta().Lock.Stochastiks[0].StochastikID, Equals, se.GetStochastikVars().ConnectionID)
	} else {
		c.Assert(tb.Meta().Lock, IsNil)
	}
}

func (s *testDBSuite2) TestDBSWithInvalidBlockInfo(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	defer tk.MustInterDirc("drop causet if exists t")
	// Test create with invalid memex.
	_, err := tk.InterDirc(`CREATE TABLE t (
		c0 int(11) ,
  		c1 int(11),
    	c2 decimal(16,4) GENERATED ALWAYS AS ((case when (c0 = 0) then 0when (c0 > 0) then (c1 / c0) end))
	);`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[BerolinaSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use line 4 defCausumn 88 near \"then (c1 / c0) end))\n\t);\" ")

	tk.MustInterDirc("create causet t (a bigint, b int, c int generated always as (b+1)) partition by hash(a) partitions 4;")
	// Test drop partition defCausumn.
	_, err = tk.InterDirc("alter causet t drop defCausumn a;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[memex:1054]Unknown defCausumn 'a' in 'memex'")
	// Test modify defCausumn with invalid memex.
	_, err = tk.InterDirc("alter causet t modify defCausumn c int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[BerolinaSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use line 1 defCausumn 97 near \"then (b / a) end));\" ")
	// Test add defCausumn with invalid memex.
	_, err = tk.InterDirc("alter causet t add defCausumn d int GENERATED ALWAYS AS ((case when (a = 0) then 0when (a > 0) then (b / a) end));")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[BerolinaSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use line 1 defCausumn 94 near \"then (b / a) end));\" ")
}

func (s *testDBSuite4) TestDeferredCausetCheck(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("drop causet if exists defCausumn_check")
	tk.MustInterDirc("create causet defCausumn_check (pk int primary key, a int check (a > 1))")
	defer tk.MustInterDirc("drop causet if exists defCausumn_check")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|8231|DeferredCauset check is not supported"))
}

func (s *testDBSuite5) TestAlterCheck(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("drop causet if exists alter_check")
	tk.MustInterDirc("create causet alter_check (pk int primary key)")
	defer tk.MustInterDirc("drop causet if exists alter_check")
	tk.MustInterDirc("alter causet alter_check alter check crcn ENFORCED")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|8231|ALTER CHECK is not supported"))
}

func (s *testDBSuite6) TestDropCheck(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("drop causet if exists drop_check")
	tk.MustInterDirc("create causet drop_check (pk int primary key)")
	defer tk.MustInterDirc("drop causet if exists drop_check")
	tk.MustInterDirc("alter causet drop_check drop check crcn")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|8231|DROP CHECK is not supported"))
}

func (s *testDBSuite7) TestAddConstraintCheck(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("drop causet if exists add_constraint_check")
	tk.MustInterDirc("create causet add_constraint_check (pk int primary key, a int)")
	defer tk.MustInterDirc("drop causet if exists add_constraint_check")
	tk.MustInterDirc("alter causet add_constraint_check add constraint crn check (a > 1)")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|8231|ADD CONSTRAINT CHECK is not supported"))
}

func (s *testDBSuite6) TestAlterOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use " + s.schemaName)
	tk.MustInterDirc("create causet ob (pk int primary key, c int default 1, c1 int default 1, KEY cl(c1))")

	// Test order by with primary key
	tk.MustInterDirc("alter causet ob order by c")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|1105|ORDER BY ignored as there is a user-defined clustered index in the causet 'ob'"))

	// Test order by with no primary key
	tk.MustInterDirc("drop causet if exists ob")
	tk.MustInterDirc("create causet ob (c int default 1, c1 int default 1, KEY cl(c1))")
	tk.MustInterDirc("alter causet ob order by c")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(0))
	tk.MustInterDirc("drop causet if exists ob")
}

func (s *testSerialDBSuite) TestDBSJobErrorCount(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists dbs_error_block, new_dbs_error_block")
	tk.MustInterDirc("create causet dbs_error_block(a int)")
	is := s.dom.SchemaReplicant()
	schemaName := perceptron.NewCIStr("test")
	blockName := perceptron.NewCIStr("dbs_error_block")
	schemaReplicant, ok := is.SchemaByName(schemaName)
	c.Assert(ok, IsTrue)
	tbl, err := is.BlockByName(schemaName, blockName)
	c.Assert(err, IsNil)

	newBlockName := perceptron.NewCIStr("new_dbs_error_block")
	job := &perceptron.Job{
		SchemaID:   schemaReplicant.ID,
		BlockID:    tbl.Meta().ID,
		SchemaName: schemaReplicant.Name.L,
		Type:       perceptron.CausetActionRenameBlock,
		BinlogInfo: &perceptron.HistoryInfo{},
		Args:       []interface{}{schemaReplicant.ID, newBlockName},
	}

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/mockErrEntrySizeTooLarge", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/mockErrEntrySizeTooLarge"), IsNil)
	}()

	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	t := spacetime.NewMeta(txn)
	job.ID, err = t.GenGlobalID()
	c.Assert(err, IsNil)
	job.Version = 1
	job.StartTS = txn.StartTS()

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
		c.Assert(historyJob.ErrorCount, Equals, int64(1), Commentf("%v", historyJob))
		ekv.ErrEntryTooLarge.Equal(historyJob.Error)
		break
	}
}

func (s *testDBSuite1) TestAlterBlockWithValidation(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	defer tk.MustInterDirc("drop causet if exists t1")

	tk.MustInterDirc("create causet t1 (c1 int, c2 int as (c1 + 1));")

	// Test for alter causet with validation.
	tk.MustInterDirc("alter causet t1 with validation")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|8200|ALTER TABLE WITH VALIDATION is currently unsupported"))

	// Test for alter causet without validation.
	tk.MustInterDirc("alter causet t1 without validation")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|8200|ALTER TABLE WITHOUT VALIDATION is currently unsupported"))
}

func (s *testSerialDBSuite) TestCommitTxnWithIndexChange(c *C) {
	// Prepare work.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_db")
	tk.MustInterDirc("create database test_db")
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
	tk.MustInterDirc("insert t1 values (1, 10, 100), (2, 20, 200)")
	tk.MustInterDirc("alter causet t1 add index k2(c2)")
	tk.MustInterDirc("alter causet t1 drop index k2")
	tk.MustInterDirc("alter causet t1 add index k2(c2)")
	tk.MustInterDirc("alter causet t1 drop index k2")
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use test_db")

	// tkALLEGROSQLs are the allegrosql memexs for the pessimistic transaction.
	// tk2DBS are the dbs memexs executed before the pessimistic transaction.
	// idxDBS is the DBS memex executed between pessimistic transaction begin and commit.
	// failCommit means the pessimistic transaction commit should fail not.
	type caseUnit struct {
		tkALLEGROSQLs     []string
		tk2DBS     []string
		idxDBS     string
		checkALLEGROSQLs  []string
		rowsExps   [][]string
		failCommit bool
		stateEnd   perceptron.SchemaState
	}

	cases := []caseUnit{
		// Test secondary index
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t2 values(11, 11, 11)"},
			[]string{"alter causet t1 add index k2(c2)",
				"alter causet t1 drop index k2",
				"alter causet t1 add index kk2(c2, c1)",
				"alter causet t1 add index k2(c2)",
				"alter causet t1 drop index k2"},
			"alter causet t1 add index k2(c2)",
			[]string{"select c3, c2 from t1 use index(k2) where c2 = 20",
				"select c3, c2 from t1 use index(k2) where c2 = 10",
				"select * from t1",
				"select * from t2 where c1 = 11"},
			[][]string{{"200 20"},
				{"100 10"},
				{"1 10 100", "2 20 200", "3 30 300"},
				{"11 11 11"}},
			false,
			perceptron.StateNone},
		// Test secondary index
		{[]string{"insert into t2 values(5, 50, 500)",
			"insert into t2 values(11, 11, 11)",
			"delete from t2 where c2 = 11",
			"uFIDelate t2 set c2 = 110 where c1 = 11"},
			//"uFIDelate t2 set c1 = 10 where c3 = 100"},
			[]string{"alter causet t1 add index k2(c2)",
				"alter causet t1 drop index k2",
				"alter causet t1 add index kk2(c2, c1)",
				"alter causet t1 add index k2(c2)",
				"alter causet t1 drop index k2"},
			"alter causet t1 add index k2(c2)",
			[]string{"select c3, c2 from t1 use index(k2) where c2 = 20",
				"select c3, c2 from t1 use index(k2) where c2 = 10",
				"select * from t1",
				"select * from t2 where c1 = 11",
				"select * from t2 where c3 = 100"},
			[][]string{{"200 20"},
				{"100 10"},
				{"1 10 100", "2 20 200"},
				{},
				{"1 10 100"}},
			false,
			perceptron.StateNone},
		// Test unique index
		/* TODO unique index is not supported now.
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t1 values(4, 40, 400)",
			"insert into t2 values(11, 11, 11)",
			"insert into t2 values(12, 12, 11)"},
			[]string{"alter causet t1 add unique index uk3(c3)",
				"alter causet t1 drop index uk3",
				"alter causet t2 add unique index ukc1c3(c1, c3)",
				"alter causet t2 add unique index ukc3(c3)",
				"alter causet t2 drop index ukc1c3",
				"alter causet t2 drop index ukc3",
				"alter causet t2 add index kc3(c3)"},
			"alter causet t1 add unique index uk3(c3)",
			[]string{"select c3, c2 from t1 use index(uk3) where c3 = 200",
				"select c3, c2 from t1 use index(uk3) where c3 = 300",
				"select c3, c2 from t1 use index(uk3) where c3 = 400",
				"select * from t1",
				"select * from t2"},
			[][]string{{"200 20"},
				{"300 30"},
				{"400 40"},
				{"1 10 100", "2 20 200", "3 30 300", "4 40 400"},
				{"1 10 100", "2 20 200", "11 11 11", "12 12 11"}},
			false, perceptron.StateNone},
		// Test unique index fail to commit, this case needs the new index could be inserted
		{[]string{"insert into t1 values(3, 30, 300)",
			"insert into t1 values(4, 40, 300)",
			"insert into t2 values(11, 11, 11)",
			"insert into t2 values(12, 11, 12)"},
			//[]string{"alter causet t1 add unique index uk3(c3)", "alter causet t1 drop index uk3"},
			[]string{},
			"alter causet t1 add unique index uk3(c3)",
			[]string{"select c3, c2 from t1 use index(uk3) where c3 = 200",
				"select c3, c2 from t1 use index(uk3) where c3 = 300",
				"select c3, c2 from t1 where c1 = 4",
				"select * from t1",
				"select * from t2"},
			[][]string{{"200 20"},
				{},
				{},
				{"1 10 100", "2 20 200"},
				{"1 10 100", "2 20 200"}},
			true,
			perceptron.StateWriteOnly},
		*/
	}
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))

	// Test add index state change
	do := s.dom.DBS()
	startStates := []perceptron.SchemaState{perceptron.StateNone, perceptron.StateDeleteOnly}
	for _, startState := range startStates {
		endStatMap := stochastik.ConstOpAddIndex[startState]
		var endStates []perceptron.SchemaState
		for st := range endStatMap {
			endStates = append(endStates, st)
		}
		sort.Slice(endStates, func(i, j int) bool { return endStates[i] < endStates[j] })
		for _, endState := range endStates {
			for _, curCase := range cases {
				if endState < curCase.stateEnd {
					break
				}
				tk2.MustInterDirc("drop causet if exists t1")
				tk2.MustInterDirc("drop causet if exists t2")
				tk2.MustInterDirc("create causet t1 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
				tk2.MustInterDirc("create causet t2 (c1 int primary key, c2 int, c3 int, index ok2(c2))")
				tk2.MustInterDirc("insert t1 values (1, 10, 100), (2, 20, 200)")
				tk2.MustInterDirc("insert t2 values (1, 10, 100), (2, 20, 200)")
				tk2.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))
				tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10 100", "2 20 200"))
				tk.MustQuery("select * from t2;").Check(testkit.Rows("1 10 100", "2 20 200"))

				for _, DBSALLEGROSQL := range curCase.tk2DBS {
					tk2.MustInterDirc(DBSALLEGROSQL)
				}
				hook := &dbs.TestDBSCallback{}
				prepared := false
				committed := false
				hook.OnJobUFIDelatedExported = func(job *perceptron.Job) {
					if job.SchemaState == startState {
						if !prepared {
							tk.MustInterDirc("begin pessimistic")
							for _, tkALLEGROSQL := range curCase.tkALLEGROSQLs {
								tk.MustInterDirc(tkALLEGROSQL)
							}
							prepared = true
						}
					} else if job.SchemaState == endState {
						if !committed {
							if curCase.failCommit {
								_, err := tk.InterDirc("commit")
								c.Assert(err, NotNil)
							} else {
								tk.MustInterDirc("commit")
							}
						}
						committed = true
					}
				}
				originalCallback := do.GetHook()
				do.(dbs.DBSForTest).SetHook(hook)
				tk2.MustInterDirc(curCase.idxDBS)
				do.(dbs.DBSForTest).SetHook(originalCallback)
				tk2.MustInterDirc("admin check causet t1")
				for i, checkALLEGROSQL := range curCase.checkALLEGROSQLs {
					if len(curCase.rowsExps[i]) > 0 {
						tk2.MustQuery(checkALLEGROSQL).Check(testkit.Rows(curCase.rowsExps[i]...))
					} else {
						tk2.MustQuery(checkALLEGROSQL).Check(nil)
					}
				}
			}
		}
	}
	tk.MustInterDirc("admin check causet t1")
}

// TestAddIndexFailOnCaseWhenCanExit is used to close #19325.
func (s *testSerialDBSuite) TestAddIndexFailOnCaseWhenCanExit(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/MockCaseWhenParseFailure", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/MockCaseWhenParseFailure"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("insert into t values(1, 1)")
	_, err := tk.InterDirc("alter causet t add index idx(b)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:-1]DBS job rollback, error msg: job.ErrCount:512, mock unknown type: ast.whenClause.")
	tk.MustInterDirc("drop causet if exists t")
}

func init() {
	// Make sure it will only be executed once.
	petri.SchemaOutOfDateRetryInterval = int64(50 * time.Millisecond)
	petri.SchemaOutOfDateRetryTimes = int32(50)
}

func (s *testSerialDBSuite) TestCreateBlockWithIntegerLengthWaring(c *C) {
	// Inject the strict-integer-display-width variable in BerolinaSQL directly.
	BerolinaSQLtypes.MilevaDBStrictIntegerDisplayWidth = true
	defer func() {
		BerolinaSQLtypes.MilevaDBStrictIntegerDisplayWidth = false
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")

	tk.MustInterDirc("create causet t(a tinyint(1))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a smallint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a mediumint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a bigint(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a integer(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int1(1))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int2(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int3(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int4(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int8(2))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use [BerolinaSQL:1681]Integer display width is deprecated and will be removed in a future release."))

	tk.MustInterDirc("drop causet if exists t")
}
