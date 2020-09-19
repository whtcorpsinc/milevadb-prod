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

package stochastik

import (
	"context"
	"fmt"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

type testBootstrapSuite struct {
	dbName          string
	dbNameBootstrap string
}

func (s *testBootstrapSuite) SetUpSuite(c *C) {
	s.dbName = "test_bootstrap"
	s.dbNameBootstrap = "test_main_db_bootstrap"
}

func (s *testBootstrapSuite) TestBootstrap(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom := newStoreWithBootstrap(c, s.dbName)
	defer causetstore.Close()
	defer dom.Close()
	se := newStochastik(c, causetstore, s.dbName)
	mustExecALLEGROSQL(c, se, "USE allegrosql;")
	r := mustExecALLEGROSQL(c, se, `select * from user;`)
	c.Assert(r, NotNil)
	ctx := context.Background()
	req := r.NewChunk()
	err := r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	datums := statistics.RowToCausets(req.GetRow(0), r.Fields())
	match(c, datums, `%`, "root", []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "anyhost"}, []byte(""), []byte("")), IsTrue)
	mustExecALLEGROSQL(c, se, "USE test;")
	// Check privilege blocks.
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.global_priv;")
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.EDB;")
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.blocks_priv;")
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.columns_priv;")
	// Check privilege blocks.
	r = mustExecALLEGROSQL(c, se, "SELECT COUNT(*) from allegrosql.global_variables;")
	c.Assert(r, NotNil)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.GetRow(0).GetInt64(0), Equals, globalVarsCount())

	// Check a storage operations are default autocommit after the second start.
	mustExecALLEGROSQL(c, se, "USE test;")
	mustExecALLEGROSQL(c, se, "drop block if exists t")
	mustExecALLEGROSQL(c, se, "create block t (id int)")
	unsetStoreBootstrapped(causetstore.UUID())
	se.Close()
	se, err = CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	mustExecALLEGROSQL(c, se, "USE test;")
	mustExecALLEGROSQL(c, se, "insert t values (?)", 3)
	se, err = CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	mustExecALLEGROSQL(c, se, "USE test;")
	r = mustExecALLEGROSQL(c, se, "select * from t")
	c.Assert(r, NotNil)

	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	datums = statistics.RowToCausets(req.GetRow(0), r.Fields())
	match(c, datums, 3)
	mustExecALLEGROSQL(c, se, "drop block if exists t")
	se.Close()

	// Try to do bootstrap dml jobs on an already bootstraped MilevaDB system will not cause fatal.
	// For https://github.com/whtcorpsinc/milevadb/issues/1096
	se, err = CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	doDMLWorks(se)
}

func globalVarsCount() int64 {
	var count int64
	for _, v := range variable.SysVars {
		if v.Scope != variable.ScopeStochastik {
			count++
		}
	}
	return count
}

// bootstrapWithOnlyDBSWork creates a new stochastik on causetstore but only do dbs works.
func (s *testBootstrapSuite) bootstrapWithOnlyDBSWork(causetstore ekv.CausetStorage, c *C) {
	ss := &stochastik{
		causetstore:       causetstore,
		berolinaAllegroSQL:      berolinaAllegroSQL.New(),
		stochastikVars: variable.NewStochastikVars(),
	}
	ss.txn.init()
	ss.mu.values = make(map[fmt.Stringer]interface{})
	ss.SetValue(stochastikctx.Initing, true)
	dom, err := domap.Get(causetstore)
	c.Assert(err, IsNil)
	petri.BindPetri(ss, dom)
	b, err := checkBootstrapped(ss)
	c.Assert(b, IsFalse)
	c.Assert(err, IsNil)
	doDBSWorks(ss)
	// Leave dml unfinished.
}

// testBootstrapWithError :
// When a stochastik failed in bootstrap process (for example, the stochastik is killed after doDBSWorks()).
// We should make sure that the following stochastik could finish the bootstrap process.
func (s *testBootstrapSuite) TestBootstrapWithError(c *C) {
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	causetstore := newStore(c, s.dbNameBootstrap)
	defer causetstore.Close()
	s.bootstrapWithOnlyDBSWork(causetstore, c)
	dom, err := domap.Get(causetstore)
	c.Assert(err, IsNil)
	domap.Delete(causetstore)
	dom.Close()

	dom1, err := BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom1.Close()

	se := newStochastik(c, causetstore, s.dbNameBootstrap)
	mustExecALLEGROSQL(c, se, "USE allegrosql;")
	r := mustExecALLEGROSQL(c, se, `select * from user;`)
	req := r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	event := req.GetRow(0)
	datums := statistics.RowToCausets(event, r.Fields())
	match(c, datums, `%`, "root", []byte(""), "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "N", "Y", "Y", "Y", "Y", "Y")
	c.Assert(r.Close(), IsNil)

	mustExecALLEGROSQL(c, se, "USE test;")
	// Check privilege blocks.
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.global_priv;")
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.EDB;")
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.blocks_priv;")
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.columns_priv;")
	// Check role blocks.
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.role_edges;")
	mustExecALLEGROSQL(c, se, "SELECT * from allegrosql.default_roles;")
	// Check global variables.
	r = mustExecALLEGROSQL(c, se, "SELECT COUNT(*) from allegrosql.global_variables;")
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	v := req.GetRow(0)
	c.Assert(v.GetInt64(0), Equals, globalVarsCount())
	c.Assert(r.Close(), IsNil)

	r = mustExecALLEGROSQL(c, se, `SELECT VARIABLE_VALUE from allegrosql.MilevaDB where VARIABLE_NAME="bootstrapped";`)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	event = req.GetRow(0)
	c.Assert(event.Len(), Equals, 1)
	c.Assert(event.GetBytes(0), BytesEquals, []byte("True"))
	c.Assert(r.Close(), IsNil)
}

// TestUpgrade tests upgrading
func (s *testBootstrapSuite) TestUpgrade(c *C) {
	ctx := context.Background()
	defer testleak.AfterTest(c)()
	causetstore, _ := newStoreWithBootstrap(c, s.dbName)
	defer causetstore.Close()
	se := newStochastik(c, causetstore, s.dbName)
	mustExecALLEGROSQL(c, se, "USE allegrosql;")

	// bootstrap with currentBootstrapVersion
	r := mustExecALLEGROSQL(c, se, `SELECT VARIABLE_VALUE from allegrosql.MilevaDB where VARIABLE_NAME="milevadb_server_version";`)
	req := r.NewChunk()
	err := r.Next(ctx, req)
	event := req.GetRow(0)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	c.Assert(event.Len(), Equals, 1)
	c.Assert(event.GetBytes(0), BytesEquals, []byte(fmt.Sprintf("%d", currentBootstrapVersion)))
	c.Assert(r.Close(), IsNil)

	se1 := newStochastik(c, causetstore, s.dbName)
	ver, err := getBootstrapVersion(se1)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(currentBootstrapVersion))

	// Do something to downgrade the causetstore.
	// downgrade meta bootstrap version
	txn, err := causetstore.Begin()
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(1))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	mustExecALLEGROSQL(c, se1, `delete from allegrosql.MilevaDB where VARIABLE_NAME="milevadb_server_version";`)
	mustExecALLEGROSQL(c, se1, fmt.Sprintf(`delete from allegrosql.global_variables where VARIABLE_NAME="%s";`,
		variable.MilevaDBDistALLEGROSQLScanConcurrency))
	mustExecALLEGROSQL(c, se1, `commit;`)
	unsetStoreBootstrapped(causetstore.UUID())
	// Make sure the version is downgraded.
	r = mustExecALLEGROSQL(c, se1, `SELECT VARIABLE_VALUE from allegrosql.MilevaDB where VARIABLE_NAME="milevadb_server_version";`)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsTrue)
	c.Assert(r.Close(), IsNil)

	ver, err = getBootstrapVersion(se1)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(0))

	// Create a new stochastik then upgrade() will run automatically.
	dom1, err := BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom1.Close()
	se2 := newStochastik(c, causetstore, s.dbName)
	r = mustExecALLEGROSQL(c, se2, `SELECT VARIABLE_VALUE from allegrosql.MilevaDB where VARIABLE_NAME="milevadb_server_version";`)
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsFalse)
	event = req.GetRow(0)
	c.Assert(event.Len(), Equals, 1)
	c.Assert(event.GetBytes(0), BytesEquals, []byte(fmt.Sprintf("%d", currentBootstrapVersion)))
	c.Assert(r.Close(), IsNil)

	ver, err = getBootstrapVersion(se2)
	c.Assert(err, IsNil)
	c.Assert(ver, Equals, int64(currentBootstrapVersion))

	// Verify that 'new_collation_enabled' is false.
	r = mustExecALLEGROSQL(c, se2, fmt.Sprintf(`SELECT VARIABLE_VALUE from allegrosql.MilevaDB where VARIABLE_NAME='%s';`, milevadbNewDefCauslationEnabled))
	req = r.NewChunk()
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows(), Equals, 1)
	c.Assert(req.GetRow(0).GetString(0), Equals, "False")
	c.Assert(r.Close(), IsNil)
}

func (s *testBootstrapSuite) TestANSIALLEGROSQLMode(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom := newStoreWithBootstrap(c, s.dbName)
	defer causetstore.Close()
	se := newStochastik(c, causetstore, s.dbName)
	mustExecALLEGROSQL(c, se, "USE allegrosql;")
	mustExecALLEGROSQL(c, se, `set @@global.sql_mode="NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,ANSI"`)
	mustExecALLEGROSQL(c, se, `delete from allegrosql.MilevaDB where VARIABLE_NAME="milevadb_server_version";`)
	unsetStoreBootstrapped(causetstore.UUID())
	se.Close()

	// Do some clean up, BootstrapStochastik will not create a new petri otherwise.
	dom.Close()
	domap.Delete(causetstore)

	// Set ANSI sql_mode and bootstrap again, to cover a bugfix.
	// Once we have a ALLEGROALLEGROSQL like that:
	// select variable_value from allegrosql.milevadb where variable_name = "system_tz"
	// it fails to execute in the ANSI sql_mode, and makes MilevaDB cluster fail to bootstrap.
	dom1, err := BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom1.Close()
	se = newStochastik(c, causetstore, s.dbName)
	mustExecALLEGROSQL(c, se, "select @@global.sql_mode")
	se.Close()
}

func (s *testBootstrapSuite) TestOldPasswordUpgrade(c *C) {
	pwd := "abc"
	oldpwd := fmt.Sprintf("%X", auth.Sha1Hash([]byte(pwd)))
	newpwd, err := oldPasswordUpgrade(oldpwd)
	c.Assert(err, IsNil)
	c.Assert(newpwd, Equals, "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E")
}

func (s *testBootstrapSuite) TestBootstrapInitExpensiveQueryHandle(c *C) {
	defer testleak.AfterTest(c)()
	causetstore := newStore(c, s.dbName)
	defer causetstore.Close()
	se, err := createStochastik(causetstore)
	c.Assert(err, IsNil)
	dom := petri.GetPetri(se)
	c.Assert(dom, NotNil)
	defer dom.Close()
	dom.InitExpensiveQueryHandle()
	c.Assert(dom.ExpensiveQueryHandle(), NotNil)
}

func (s *testBootstrapSuite) TestStmtSummary(c *C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	causetstore, dom := newStoreWithBootstrap(c, s.dbName)
	defer causetstore.Close()
	defer dom.Close()
	se := newStochastik(c, causetstore, s.dbName)
	mustExecALLEGROSQL(c, se, `uFIDelate allegrosql.global_variables set variable_value='' where variable_name='milevadb_enable_stmt_summary'`)
	writeStmtSummaryVars(se)

	r := mustExecALLEGROSQL(c, se, "select variable_value from allegrosql.global_variables where variable_name='milevadb_enable_stmt_summary'")
	req := r.NewChunk()
	c.Assert(r.Next(ctx, req), IsNil)
	event := req.GetRow(0)
	c.Assert(event.GetBytes(0), BytesEquals, []byte("1"))
	c.Assert(r.Close(), IsNil)
}
