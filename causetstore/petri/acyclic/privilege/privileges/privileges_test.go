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

package privileges_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testPrivilegeSuite{})

type testPrivilegeSuite struct {
	causetstore ekv.CausetStorage
	dom         *petri.Petri
	dbName      string

	createDBALLEGROSQL                      string
	createDB1ALLEGROSQL                     string
	droFIDelBALLEGROSQL                     string
	useDBALLEGROSQL                         string
	createBlockALLEGROSQL                   string
	createSystemDBALLEGROSQL                string
	createUserBlockALLEGROSQL               string
	createDBPrivBlockALLEGROSQL             string
	createBlockPrivBlockALLEGROSQL          string
	createDeferredCausetPrivBlockALLEGROSQL string
}

func (s *testPrivilegeSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test"
	s.dom, s.causetstore = newStore(c, s.dbName)
}

func (s *testPrivilegeSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
}

func (s *testPrivilegeSuite) SetUpTest(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	s.createDBALLEGROSQL = fmt.Sprintf("create database if not exists %s;", s.dbName)
	s.createDB1ALLEGROSQL = fmt.Sprintf("create database if not exists %s1;", s.dbName)
	s.droFIDelBALLEGROSQL = fmt.Sprintf("drop database if exists %s;", s.dbName)
	s.useDBALLEGROSQL = fmt.Sprintf("use %s;", s.dbName)
	s.createBlockALLEGROSQL = `CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`

	mustInterDirc(c, se, s.createDBALLEGROSQL)
	mustInterDirc(c, se, s.createDB1ALLEGROSQL) // create database test1
	mustInterDirc(c, se, s.useDBALLEGROSQL)
	mustInterDirc(c, se, s.createBlockALLEGROSQL)

	s.createSystemDBALLEGROSQL = fmt.Sprintf("create database if not exists %s;", allegrosql.SystemDB)
	s.createUserBlockALLEGROSQL = stochastik.CreateUserBlock
	s.createDBPrivBlockALLEGROSQL = stochastik.CreateDBPrivBlock
	s.createBlockPrivBlockALLEGROSQL = stochastik.CreateBlockPrivBlock
	s.createDeferredCausetPrivBlockALLEGROSQL = stochastik.CreateDeferredCausetPrivBlock

	mustInterDirc(c, se, s.createSystemDBALLEGROSQL)
	mustInterDirc(c, se, s.createUserBlockALLEGROSQL)
	mustInterDirc(c, se, s.createDBPrivBlockALLEGROSQL)
	mustInterDirc(c, se, s.createBlockPrivBlockALLEGROSQL)
	mustInterDirc(c, se, s.createDeferredCausetPrivBlockALLEGROSQL)
}

func (s *testPrivilegeSuite) TearDownTest(c *C) {
	// drop EDB
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, s.droFIDelBALLEGROSQL)
}

func (s *testPrivilegeSuite) TestCheckDBPrivilege(c *C) {
	rootSe := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `CREATE USER 'testcheck'@'localhost';`)
	mustInterDirc(c, rootSe, `CREATE USER 'testcheck_tmp'@'localhost';`)

	se := newStochastik(c, s.causetstore, s.dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testcheck", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.SelectPriv), IsFalse)

	mustInterDirc(c, rootSe, `GRANT SELECT ON *.* TO  'testcheck'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.UFIDelatePriv), IsFalse)

	mustInterDirc(c, rootSe, `GRANT UFIDelate ON test.* TO  'testcheck'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.UFIDelatePriv), IsTrue)

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "testcheck", Hostname: "localhost"})
	mustInterDirc(c, rootSe, `GRANT 'testcheck'@'localhost' TO 'testcheck_tmp'@'localhost';`)
	se2 := newStochastik(c, s.causetstore, s.dbName)
	c.Assert(se2.Auth(&auth.UserIdentity{Username: "testcheck_tmp", Hostname: "localhost"}, nil, nil), IsTrue)
	pc = privilege.GetPrivilegeManager(se2)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.UFIDelatePriv), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckPointGetDBPrivilege(c *C) {
	rootSe := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `CREATE USER 'tester'@'localhost';`)
	mustInterDirc(c, rootSe, `GRANT SELECT,UFIDelATE ON test.* TO  'tester'@'localhost';`)
	mustInterDirc(c, rootSe, `flush privileges;`)
	mustInterDirc(c, rootSe, `create database test2`)
	mustInterDirc(c, rootSe, `create causet test2.t(id int, v int, primary key(id))`)
	mustInterDirc(c, rootSe, `insert into test2.t(id, v) values(1, 1)`)

	se := newStochastik(c, s.causetstore, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tester", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `use test;`)
	_, err := se.InterDircute(context.Background(), `select * from test2.t where id = 1`)
	c.Assert(terror.ErrorEqual(err, embedded.ErrBlockaccessDenied), IsTrue)
	_, err = se.InterDircute(context.Background(), "uFIDelate test2.t set v = 2 where id = 1")
	c.Assert(terror.ErrorEqual(err, embedded.ErrBlockaccessDenied), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckBlockPrivilege(c *C) {
	rootSe := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `CREATE USER 'test1'@'localhost';`)
	mustInterDirc(c, rootSe, `CREATE USER 'test1_tmp'@'localhost';`)

	se := newStochastik(c, s.causetstore, s.dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test1", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", allegrosql.SelectPriv), IsFalse)

	mustInterDirc(c, rootSe, `GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", allegrosql.UFIDelatePriv), IsFalse)

	mustInterDirc(c, rootSe, `GRANT UFIDelate ON test.* TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", allegrosql.UFIDelatePriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", allegrosql.IndexPriv), IsFalse)

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "test1", Hostname: "localhost"})
	se2 := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `GRANT 'test1'@'localhost' TO 'test1_tmp'@'localhost';`)
	c.Assert(se2.Auth(&auth.UserIdentity{Username: "test1_tmp", Hostname: "localhost"}, nil, nil), IsTrue)
	pc2 := privilege.GetPrivilegeManager(se2)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", allegrosql.UFIDelatePriv), IsTrue)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", allegrosql.IndexPriv), IsFalse)

	mustInterDirc(c, rootSe, `GRANT Index ON test.test TO  'test1'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "test", "", allegrosql.IndexPriv), IsTrue)
	c.Assert(pc2.RequestVerification(activeRoles, "test", "test", "", allegrosql.IndexPriv), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckViewPrivilege(c *C) {
	rootSe := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `CREATE USER 'vuser'@'localhost';`)
	mustInterDirc(c, rootSe, `CREATE VIEW v AS SELECT * FROM test;`)

	se := newStochastik(c, s.causetstore, s.dbName)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "vuser", Hostname: "localhost"}, nil, nil), IsTrue)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", allegrosql.SelectPriv), IsFalse)

	mustInterDirc(c, rootSe, `GRANT SELECT ON test.v TO 'vuser'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", allegrosql.ShowViewPriv), IsFalse)

	mustInterDirc(c, rootSe, `GRANT SHOW VIEW ON test.v TO 'vuser'@'localhost';`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "v", "", allegrosql.ShowViewPriv), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckPrivilegeWithRoles(c *C) {
	rootSe := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `CREATE USER 'test_role'@'localhost';`)
	mustInterDirc(c, rootSe, `CREATE ROLE r_1, r_2, r_3;`)
	mustInterDirc(c, rootSe, `GRANT r_1, r_2, r_3 TO 'test_role'@'localhost';`)

	se := newStochastik(c, s.causetstore, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_role", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `SET ROLE r_1, r_2;`)
	mustInterDirc(c, rootSe, `SET DEFAULT ROLE r_1 TO 'test_role'@'localhost';`)

	mustInterDirc(c, rootSe, `GRANT SELECT ON test.* TO r_1;`)
	pc := privilege.GetPrivilegeManager(se)
	activeRoles := se.GetStochastikVars().ActiveRoles
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.UFIDelatePriv), IsFalse)
	mustInterDirc(c, rootSe, `GRANT UFIDelATE ON test.* TO r_2;`)
	c.Assert(pc.RequestVerification(activeRoles, "test", "", "", allegrosql.UFIDelatePriv), IsTrue)

	mustInterDirc(c, se, `SET ROLE NONE;`)
	c.Assert(len(se.GetStochastikVars().ActiveRoles), Equals, 0)
	mustInterDirc(c, se, `SET ROLE DEFAULT;`)
	c.Assert(len(se.GetStochastikVars().ActiveRoles), Equals, 1)
	mustInterDirc(c, se, `SET ROLE ALL;`)
	c.Assert(len(se.GetStochastikVars().ActiveRoles), Equals, 3)
	mustInterDirc(c, se, `SET ROLE ALL EXCEPT r_1, r_2;`)
	c.Assert(len(se.GetStochastikVars().ActiveRoles), Equals, 1)
}

func (s *testPrivilegeSuite) TestShowGrants(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	ctx, _ := se.(stochastikctx.Context)
	mustInterDirc(c, se, `CREATE USER 'show'@'localhost' identified by '123';`)
	mustInterDirc(c, se, `GRANT Index ON *.* TO  'show'@'localhost';`)
	pc := privilege.GetPrivilegeManager(se)

	gs, err := pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Index ON *.* TO 'show'@'localhost'`)

	mustInterDirc(c, se, `GRANT Select ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,Index ON *.* TO 'show'@'localhost'`)

	// The order of privs is the same with AllGlobalPrivs
	mustInterDirc(c, se, `GRANT UFIDelate ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT Select,UFIDelate,Index ON *.* TO 'show'@'localhost'`)

	// All privileges
	mustInterDirc(c, se, `GRANT ALL ON *.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`)

	// All privileges with grant option
	mustInterDirc(c, se, `GRANT ALL ON *.* TO 'show'@'localhost' WITH GRANT OPTION;`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost' WITH GRANT OPTION`)

	// Revoke grant option
	mustInterDirc(c, se, `REVOKE GRANT OPTION ON *.* FROM 'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`)

	// Add EDB scope privileges
	mustInterDirc(c, se, `GRANT Select ON test.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	expected := []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`}
	c.Assert(solitonutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustInterDirc(c, se, `GRANT Index ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT Index ON test1.* TO 'show'@'localhost'`}
	c.Assert(solitonutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	mustInterDirc(c, se, `GRANT ALL ON test1.* TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`}
	c.Assert(solitonutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Add causet scope privileges
	mustInterDirc(c, se, `GRANT UFIDelate ON test.test TO  'show'@'localhost';`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 4)
	expected = []string{`GRANT ALL PRIVILEGES ON *.* TO 'show'@'localhost'`,
		`GRANT Select ON test.* TO 'show'@'localhost'`,
		`GRANT ALL PRIVILEGES ON test1.* TO 'show'@'localhost'`,
		`GRANT UFIDelate ON test.test TO 'show'@'localhost'`}
	c.Assert(solitonutil.CompareUnorderedStringSlice(gs, expected), IsTrue)

	// Expected behavior: Usage still exists after revoking all privileges
	mustInterDirc(c, se, `REVOKE ALL PRIVILEGES ON *.* FROM 'show'@'localhost'`)
	mustInterDirc(c, se, `REVOKE Select on test.* FROM 'show'@'localhost'`)
	mustInterDirc(c, se, `REVOKE ALL ON test1.* FROM 'show'@'localhost'`)
	mustInterDirc(c, se, `REVOKE UFIDelATE on test.test FROM 'show'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 1)
	c.Assert(gs[0], Equals, `GRANT USAGE ON *.* TO 'show'@'localhost'`)

	// Usage should not exist after dropping the user
	// Which we need privileges to do so!
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustInterDirc(c, se, `DROP USER 'show'@'localhost'`)

	// This should now return an error
	_, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "show", Hostname: "localhost"}, nil)
	c.Assert(err, NotNil)
	// cant show grants for non-existent
	c.Assert(terror.ErrorEqual(err, privileges.ErrNonexistingGrant), IsTrue)

	// Test SHOW GRANTS with USING roles.
	mustInterDirc(c, se, `CREATE ROLE 'r1', 'r2'`)
	mustInterDirc(c, se, `GRANT SELECT ON test.* TO 'r1'`)
	mustInterDirc(c, se, `GRANT INSERT, UFIDelATE ON test.* TO 'r2'`)
	mustInterDirc(c, se, `CREATE USER 'testrole'@'localhost' IDENTIFIED BY 'u1pass'`)
	mustInterDirc(c, se, `GRANT 'r1', 'r2' TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	roles := make([]*auth.RoleIdentity, 0)
	roles = append(roles, &auth.RoleIdentity{Username: "r2", Hostname: "%"})
	mustInterDirc(c, se, `GRANT DELETE ON test.* TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	roles = append(roles, &auth.RoleIdentity{Username: "r1", Hostname: "%"})
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
	mustInterDirc(c, se, `GRANT INSERT, DELETE ON test.test TO 'r2'`)
	mustInterDirc(c, se, `create causet test.b (id int)`)
	mustInterDirc(c, se, `GRANT UFIDelATE ON test.b TO 'testrole'@'localhost'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 5)
	mustInterDirc(c, se, `DROP ROLE 'r1', 'r2'`)
	mustInterDirc(c, se, `DROP USER 'testrole'@'localhost'`)
	mustInterDirc(c, se, `CREATE ROLE 'r1', 'r2'`)
	mustInterDirc(c, se, `GRANT SELECT ON test.* TO 'r2'`)
	mustInterDirc(c, se, `CREATE USER 'testrole'@'localhost' IDENTIFIED BY 'u1pass'`)
	mustInterDirc(c, se, `GRANT 'r1' TO 'testrole'@'localhost'`)
	mustInterDirc(c, se, `GRANT 'r2' TO 'r1'`)
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, nil)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 2)
	roles = make([]*auth.RoleIdentity, 0)
	roles = append(roles, &auth.RoleIdentity{Username: "r1", Hostname: "%"})
	gs, err = pc.ShowGrants(se, &auth.UserIdentity{Username: "testrole", Hostname: "localhost"}, roles)
	c.Assert(err, IsNil)
	c.Assert(gs, HasLen, 3)
}

func (s *testPrivilegeSuite) TestShowDeferredCausetGrants(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `USE test`)
	mustInterDirc(c, se, `CREATE USER 'column'@'%'`)
	mustInterDirc(c, se, `CREATE TABLE column_block (a int, b int, c int)`)
	mustInterDirc(c, se, `GRANT Select(a),UFIDelate(a,b),Insert(c) ON test.column_block TO  'column'@'%'`)

	pc := privilege.GetPrivilegeManager(se)
	gs, err := pc.ShowGrants(se, &auth.UserIdentity{Username: "column", Hostname: "%"}, nil)
	c.Assert(err, IsNil)
	c.Assert(strings.Join(gs, " "), Equals, "GRANT USAGE ON *.* TO 'column'@'%' GRANT Select(a), Insert(c), UFIDelate(a, b) ON test.column_block TO 'column'@'%'")
}

func (s *testPrivilegeSuite) TestDropBlockPriv(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	ctx, _ := se.(stochastikctx.Context)
	mustInterDirc(c, se, `CREATE TABLE todrop(c int);`)
	// ctx.GetStochastikVars().User = "root@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `CREATE USER 'drop'@'localhost';`)
	mustInterDirc(c, se, `GRANT Select ON test.todrop TO  'drop'@'localhost';`)

	// ctx.GetStochastikVars().User = "drop@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "drop", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `SELECT * FROM todrop;`)
	_, err := se.InterDircute(context.Background(), "DROP TABLE todrop;")
	c.Assert(err, NotNil)

	se = newStochastik(c, s.causetstore, s.dbName)
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustInterDirc(c, se, `GRANT Drop ON test.todrop TO  'drop'@'localhost';`)

	se = newStochastik(c, s.causetstore, s.dbName)
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "drop", Hostname: "localhost"}
	mustInterDirc(c, se, `DROP TABLE todrop;`)
}

func (s *testPrivilegeSuite) TestSetPasswdStmt(c *C) {

	se := newStochastik(c, s.causetstore, s.dbName)

	// high privileged user setting password for other user (passes)
	mustInterDirc(c, se, "CREATE USER 'superuser'")
	mustInterDirc(c, se, "CREATE USER 'nobodyuser'")
	mustInterDirc(c, se, "GRANT ALL ON *.* TO 'superuser'")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "superuser", Hostname: "localhost", AuthUsername: "superuser", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "SET PASSWORD for 'nobodyuser' = 'newpassword'")
	mustInterDirc(c, se, "SET PASSWORD for 'nobodyuser' = ''")

	// low privileged user trying to set password for other user (fails)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nobodyuser", Hostname: "localhost", AuthUsername: "nobodyuser", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), "SET PASSWORD for 'superuser' = 'newpassword'")
	c.Assert(err, NotNil)
}

func (s *testPrivilegeSuite) TestSelectViewSecurity(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	ctx, _ := se.(stochastikctx.Context)
	mustInterDirc(c, se, `CREATE TABLE viewsecurity(c int);`)
	// ctx.GetStochastikVars().User = "root@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `CREATE USER 'selectusr'@'localhost';`)
	mustInterDirc(c, se, `GRANT CREATE VIEW ON test.* TO  'selectusr'@'localhost';`)
	mustInterDirc(c, se, `GRANT SELECT ON test.viewsecurity TO  'selectusr'@'localhost';`)

	// ctx.GetStochastikVars().User = "selectusr@localhost"
	c.Assert(se.Auth(&auth.UserIdentity{Username: "selectusr", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `SELECT * FROM test.viewsecurity;`)
	mustInterDirc(c, se, `CREATE ALGORITHM = UNDEFINED ALLEGROALLEGROSQL SECURITY DEFINER VIEW test.selectviewsecurity as select * FROM test.viewsecurity;`)

	se = newStochastik(c, s.causetstore, s.dbName)
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	mustInterDirc(c, se, "SELECT * FROM test.selectviewsecurity")
	mustInterDirc(c, se, `REVOKE Select ON test.viewsecurity FROM  'selectusr'@'localhost';`)
	_, err := se.InterDircute(context.Background(), "select * from test.selectviewsecurity")
	c.Assert(err.Error(), Equals, embedded.ErrViewInvalid.GenWithStackByArgs("test", "selectviewsecurity").Error())
}

func (s *testPrivilegeSuite) TestRoleAdminSecurity(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER 'ar1'@'localhost';`)
	mustInterDirc(c, se, `CREATE USER 'ar2'@'localhost';`)
	mustInterDirc(c, se, `GRANT ALL ON *.* to ar1@localhost`)
	defer func() {
		c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
		mustInterDirc(c, se, "drop user 'ar1'@'localhost'")
		mustInterDirc(c, se, "drop user 'ar2'@'localhost'")
	}()

	c.Assert(se.Auth(&auth.UserIdentity{Username: "ar1", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `create role r_test1@localhost`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "ar2", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), `create role r_test2@localhost`)
	c.Assert(terror.ErrorEqual(err, embedded.ErrSpecificAccessDenied), IsTrue)
}

func (s *testPrivilegeSuite) TestCheckCertBasedAuth(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER 'r1'@'localhost';`)
	mustInterDirc(c, se, `CREATE USER 'r2'@'localhost' require none;`)
	mustInterDirc(c, se, `CREATE USER 'r3'@'localhost' require ssl;`)
	mustInterDirc(c, se, `CREATE USER 'r4'@'localhost' require x509;`)
	mustInterDirc(c, se, `CREATE USER 'r5'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=WHTCORPS INC/OU=MilevaDB/CN=MilevaDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=WHTCORPS INC.Inc/OU=MilevaDB/CN=tester1' cipher 'TLS_AES_128_GCM_SHA256'`)
	mustInterDirc(c, se, `CREATE USER 'r6'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=WHTCORPS INC/OU=MilevaDB/CN=MilevaDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=WHTCORPS INC.Inc/OU=MilevaDB/CN=tester1'`)
	mustInterDirc(c, se, `CREATE USER 'r7_issuer_only'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=WHTCORPS INC/OU=MilevaDB/CN=MilevaDB admin'`)
	mustInterDirc(c, se, `CREATE USER 'r8_subject_only'@'localhost' require subject '/C=ZH/ST=Beijing/L=Haidian/O=WHTCORPS INC.Inc/OU=MilevaDB/CN=tester1'`)
	mustInterDirc(c, se, `CREATE USER 'r9_subject_disorder'@'localhost' require subject '/ST=Beijing/C=ZH/L=Haidian/O=WHTCORPS INC.Inc/OU=MilevaDB/CN=tester1'`)
	mustInterDirc(c, se, `CREATE USER 'r10_issuer_disorder'@'localhost' require issuer '/ST=California/C=US/L=San Francisco/O=WHTCORPS INC/OU=MilevaDB/CN=MilevaDB admin'`)
	mustInterDirc(c, se, `CREATE USER 'r11_cipher_only'@'localhost' require cipher 'TLS_AES_256_GCM_SHA384'`)
	mustInterDirc(c, se, `CREATE USER 'r12_old_milevadb_user'@'localhost'`)
	mustInterDirc(c, se, "DELETE FROM allegrosql.global_priv WHERE `user` = 'r12_old_milevadb_user' and `host` = 'localhost'")
	mustInterDirc(c, se, `CREATE USER 'r13_broken_user'@'localhost'require issuer '/C=US/ST=California/L=San Francisco/O=WHTCORPS INC/OU=MilevaDB/CN=MilevaDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=WHTCORPS INC.Inc/OU=MilevaDB/CN=tester1'`)
	mustInterDirc(c, se, "UFIDelATE allegrosql.global_priv set priv = 'abc' where `user` = 'r13_broken_user' and `host` = 'localhost'")
	mustInterDirc(c, se, `CREATE USER 'r14_san_only_pass'@'localhost' require san 'URI:spiffe://mesh.whtcorpsinc.com/ns/timesh/sa/me1'`)
	mustInterDirc(c, se, `CREATE USER 'r15_san_only_fail'@'localhost' require san 'URI:spiffe://mesh.whtcorpsinc.com/ns/timesh/sa/me2'`)
	mustInterDirc(c, se, "flush privileges")

	defer func() {
		c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
		mustInterDirc(c, se, "drop user 'r1'@'localhost'")
		mustInterDirc(c, se, "drop user 'r2'@'localhost'")
		mustInterDirc(c, se, "drop user 'r3'@'localhost'")
		mustInterDirc(c, se, "drop user 'r4'@'localhost'")
		mustInterDirc(c, se, "drop user 'r5'@'localhost'")
		mustInterDirc(c, se, "drop user 'r6'@'localhost'")
		mustInterDirc(c, se, "drop user 'r7_issuer_only'@'localhost'")
		mustInterDirc(c, se, "drop user 'r8_subject_only'@'localhost'")
		mustInterDirc(c, se, "drop user 'r9_subject_disorder'@'localhost'")
		mustInterDirc(c, se, "drop user 'r10_issuer_disorder'@'localhost'")
		mustInterDirc(c, se, "drop user 'r11_cipher_only'@'localhost'")
		mustInterDirc(c, se, "drop user 'r12_old_milevadb_user'@'localhost'")
		mustInterDirc(c, se, "drop user 'r13_broken_user'@'localhost'")
		mustInterDirc(c, se, "drop user 'r14_san_only_pass'@'localhost'")
		mustInterDirc(c, se, "drop user 'r15_san_only_fail'@'localhost'")
	}()

	// test without ssl or ca
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test use ssl without ca
	se.GetStochastikVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: nil}
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test use ssl with signed but info wrong ca.
	se.GetStochastikVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test a all pass case
	se.GetStochastikVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "US"),
				soliton.MockPkixAttribute(soliton.Province, "California"),
				soliton.MockPkixAttribute(soliton.Locality, "San Francisco"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "MilevaDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "ZH"),
				soliton.MockPkixAttribute(soliton.Province, "Beijing"),
				soliton.MockPkixAttribute(soliton.Locality, "Haidian"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC.Inc"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256, func(c *x509.Certificate) {
			var url url.URL
			url.UnmarshalBinary([]byte("spiffe://mesh.whtcorpsinc.com/ns/timesh/sa/me1"))
			c.URIs = append(c.URIs, &url)
		})
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r14_san_only_pass", Hostname: "localhost"}, nil, nil), IsTrue)

	// test require but give nothing
	se.GetStochastikVars().TLSConnectionState = nil
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)

	// test mismatch cipher
	se.GetStochastikVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "US"),
				soliton.MockPkixAttribute(soliton.Province, "California"),
				soliton.MockPkixAttribute(soliton.Locality, "San Francisco"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "MilevaDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "ZH"),
				soliton.MockPkixAttribute(soliton.Province, "Beijing"),
				soliton.MockPkixAttribute(soliton.Locality, "Haidian"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC.Inc"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_256_GCM_SHA384)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r6", Hostname: "localhost"}, nil, nil), IsTrue) // not require cipher
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r11_cipher_only", Hostname: "localhost"}, nil, nil), IsTrue)

	// test only subject or only issuer
	se.GetStochastikVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "US"),
				soliton.MockPkixAttribute(soliton.Province, "California"),
				soliton.MockPkixAttribute(soliton.Locality, "San Francisco"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "MilevaDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "AZ"),
				soliton.MockPkixAttribute(soliton.Province, "Beijing"),
				soliton.MockPkixAttribute(soliton.Locality, "Shijingshang"),
				soliton.MockPkixAttribute(soliton.Organization, "CAPPing.Inc"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "tester2"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r7_issuer_only", Hostname: "localhost"}, nil, nil), IsTrue)
	se.GetStochastikVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "AU"),
				soliton.MockPkixAttribute(soliton.Province, "California"),
				soliton.MockPkixAttribute(soliton.Locality, "San Francisco"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "MilevaDB admin2"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "ZH"),
				soliton.MockPkixAttribute(soliton.Province, "Beijing"),
				soliton.MockPkixAttribute(soliton.Locality, "Haidian"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC.Inc"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r8_subject_only", Hostname: "localhost"}, nil, nil), IsTrue)

	// test disorder issuer or subject
	se.GetStochastikVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "ZH"),
				soliton.MockPkixAttribute(soliton.Province, "Beijing"),
				soliton.MockPkixAttribute(soliton.Locality, "Haidian"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC.Inc"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r9_subject_disorder", Hostname: "localhost"}, nil, nil), IsFalse)
	se.GetStochastikVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				soliton.MockPkixAttribute(soliton.Country, "US"),
				soliton.MockPkixAttribute(soliton.Province, "California"),
				soliton.MockPkixAttribute(soliton.Locality, "San Francisco"),
				soliton.MockPkixAttribute(soliton.Organization, "WHTCORPS INC"),
				soliton.MockPkixAttribute(soliton.OrganizationalUnit, "MilevaDB"),
				soliton.MockPkixAttribute(soliton.CommonName, "MilevaDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{},
		},
		tls.TLS_AES_128_GCM_SHA256)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r10_issuer_disorder", Hostname: "localhost"}, nil, nil), IsFalse)

	// test mismatch san
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r15_san_only_fail", Hostname: "localhost"}, nil, nil), IsFalse)

	// test old data and broken data
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r12_old_milevadb_user", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r13_broken_user", Hostname: "localhost"}, nil, nil), IsFalse)

}

func connectionState(issuer, subject pkix.Name, cipher uint16, opt ...func(c *x509.Certificate)) *tls.ConnectionState {
	cert := &x509.Certificate{Issuer: issuer, Subject: subject}
	for _, o := range opt {
		o(cert)
	}
	return &tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{cert}},
		CipherSuite:    cipher,
	}
}

func (s *testPrivilegeSuite) TestCheckAuthenticate(c *C) {

	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER 'u1'@'localhost';`)
	mustInterDirc(c, se, `CREATE USER 'u2'@'localhost' identified by 'abc';`)
	mustInterDirc(c, se, `CREATE USER 'u3@example.com'@'localhost';`)
	mustInterDirc(c, se, `CREATE USER u4@localhost;`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil), IsFalse)
	salt := []byte{85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82}
	authentication := []byte{24, 180, 183, 225, 166, 6, 81, 102, 70, 248, 199, 143, 91, 204, 169, 9, 161, 171, 203, 33}
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, authentication, salt), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil), IsTrue)

	se1 := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se1, "drop user 'u1'@'localhost'")
	mustInterDirc(c, se1, "drop user 'u2'@'localhost'")
	mustInterDirc(c, se1, "drop user 'u3@example.com'@'localhost'")
	mustInterDirc(c, se1, "drop user u4@localhost")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil), IsFalse)

	se2 := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se2, "create role 'r1'@'localhost'")
	mustInterDirc(c, se2, "create role 'r2'@'localhost'")
	mustInterDirc(c, se2, "create role 'r3@example.com'@'localhost'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil), IsFalse)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "r3@example.com", Hostname: "localhost"}, nil, nil), IsFalse)

	mustInterDirc(c, se1, "drop user 'r1'@'localhost'")
	mustInterDirc(c, se1, "drop user 'r2'@'localhost'")
	mustInterDirc(c, se1, "drop user 'r3@example.com'@'localhost'")
}

func (s *testPrivilegeSuite) TestUseDB(c *C) {

	se := newStochastik(c, s.causetstore, s.dbName)
	// high privileged user
	mustInterDirc(c, se, "CREATE USER 'usesuper'")
	mustInterDirc(c, se, "CREATE USER 'usenobody'")
	mustInterDirc(c, se, "GRANT ALL ON *.* TO 'usesuper'")
	//without grant option
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	_, e := se.InterDircute(context.Background(), "GRANT SELECT ON allegrosql.* TO 'usenobody'")
	c.Assert(e, NotNil)
	//with grant option
	se = newStochastik(c, s.causetstore, s.dbName)
	// high privileged user
	mustInterDirc(c, se, "GRANT ALL ON *.* TO 'usesuper' WITH GRANT OPTION")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "use allegrosql")
	// low privileged user
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), "use allegrosql")
	c.Assert(err, NotNil)

	// try again after privilege granted
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "GRANT SELECT ON allegrosql.* TO 'usenobody'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.InterDircute(context.Background(), "use allegrosql")
	c.Assert(err, IsNil)

	// test `use EDB` for role.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `CREATE DATABASE app_db`)
	mustInterDirc(c, se, `CREATE ROLE 'app_developer'`)
	mustInterDirc(c, se, `GRANT ALL ON app_db.* TO 'app_developer'`)
	mustInterDirc(c, se, `CREATE USER 'dev'@'localhost'`)
	mustInterDirc(c, se, `GRANT 'app_developer' TO 'dev'@'localhost'`)
	mustInterDirc(c, se, `SET DEFAULT ROLE 'app_developer' TO 'dev'@'localhost'`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "dev", Hostname: "localhost", AuthUsername: "dev", AuthHostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.InterDircute(context.Background(), "use app_db")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use allegrosql")
	c.Assert(err, NotNil)
}

func (s *testPrivilegeSuite) TestRevokePrivileges(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, "CREATE USER 'hasgrant'")
	mustInterDirc(c, se, "CREATE USER 'withoutgrant'")
	mustInterDirc(c, se, "GRANT ALL ON *.* TO 'hasgrant'")
	mustInterDirc(c, se, "GRANT ALL ON allegrosql.* TO 'withoutgrant'")
	// Without grant option
	c.Assert(se.Auth(&auth.UserIdentity{Username: "hasgrant", Hostname: "localhost", AuthUsername: "hasgrant", AuthHostname: "%"}, nil, nil), IsTrue)
	_, e := se.InterDircute(context.Background(), "REVOKE SELECT ON allegrosql.* FROM 'withoutgrant'")
	c.Assert(e, NotNil)
	// With grant option
	se = newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, "GRANT ALL ON *.* TO 'hasgrant' WITH GRANT OPTION")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "hasgrant", Hostname: "localhost", AuthUsername: "hasgrant", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "REVOKE SELECT ON allegrosql.* FROM 'withoutgrant'")
	mustInterDirc(c, se, "REVOKE ALL ON allegrosql.* FROM withoutgrant")
}

func (s *testPrivilegeSuite) TestSetGlobal(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER setglobal_a@localhost`)
	mustInterDirc(c, se, `CREATE USER setglobal_b@localhost`)
	mustInterDirc(c, se, `GRANT SUPER ON *.* to setglobal_a@localhost`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "setglobal_a", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `set global innodb_commit_concurrency=16`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "setglobal_b", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), `set global innodb_commit_concurrency=16`)
	c.Assert(terror.ErrorEqual(err, embedded.ErrSpecificAccessDenied), IsTrue)
}

func (s *testPrivilegeSuite) TestCreateDropUser(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER tcd1, tcd2`)
	mustInterDirc(c, se, `GRANT ALL ON *.* to tcd2 WITH GRANT OPTION`)

	// should fail
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthUsername: "tcd1", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), `CREATE USER acdc`)
	c.Assert(terror.ErrorEqual(err, embedded.ErrSpecificAccessDenied), IsTrue)
	_, err = se.InterDircute(context.Background(), `DROP USER tcd2`)
	c.Assert(terror.ErrorEqual(err, embedded.ErrSpecificAccessDenied), IsTrue)

	// should pass
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd2", Hostname: "localhost", AuthUsername: "tcd2", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `DROP USER tcd1`)
	mustInterDirc(c, se, `CREATE USER tcd1`)

	// should pass
	mustInterDirc(c, se, `GRANT tcd2 TO tcd1`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthUsername: "tcd1", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `SET ROLE tcd2;`)
	mustInterDirc(c, se, `CREATE USER tcd3`)
	mustInterDirc(c, se, `DROP USER tcd3`)
}

func (s *testPrivilegeSuite) TestConfigPrivilege(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `DROP USER IF EXISTS tcd1`)
	mustInterDirc(c, se, `CREATE USER tcd1`)
	mustInterDirc(c, se, `GRANT ALL ON *.* to tcd1`)
	mustInterDirc(c, se, `DROP USER IF EXISTS tcd2`)
	mustInterDirc(c, se, `CREATE USER tcd2`)
	mustInterDirc(c, se, `GRANT ALL ON *.* to tcd2`)
	mustInterDirc(c, se, `REVOKE CONFIG ON *.* FROM tcd2`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthHostname: "tcd1", AuthUsername: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `SET CONFIG EinsteinDB testkey="testval"`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tcd2", Hostname: "localhost", AuthHostname: "tcd2", AuthUsername: "%"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), `SET CONFIG EinsteinDB testkey="testval"`)
	c.Assert(err, ErrorMatches, ".*you need \\(at least one of\\) the CONFIG privilege\\(s\\) for this operation")
	mustInterDirc(c, se, `DROP USER tcd1, tcd2`)
}

func (s *testPrivilegeSuite) TestShowCreateBlock(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER tsct1, tsct2`)
	mustInterDirc(c, se, `GRANT select ON allegrosql.* to tsct2`)

	// should fail
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tsct1", Hostname: "localhost", AuthUsername: "tsct1", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), `SHOW CREATE TABLE allegrosql.user`)
	c.Assert(terror.ErrorEqual(err, embedded.ErrBlockaccessDenied), IsTrue)

	// should pass
	c.Assert(se.Auth(&auth.UserIdentity{Username: "tsct2", Hostname: "localhost", AuthUsername: "tsct2", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `SHOW CREATE TABLE allegrosql.user`)
}

func (s *testPrivilegeSuite) TestAnalyzeBlock(c *C) {

	se := newStochastik(c, s.causetstore, s.dbName)
	// high privileged user
	mustInterDirc(c, se, "CREATE USER 'asuper'")
	mustInterDirc(c, se, "CREATE USER 'anobody'")
	mustInterDirc(c, se, "GRANT ALL ON *.* TO 'asuper' WITH GRANT OPTION")
	mustInterDirc(c, se, "CREATE DATABASE atest")
	mustInterDirc(c, se, "use atest")
	mustInterDirc(c, se, "CREATE TABLE t1 (a int)")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "analyze causet allegrosql.user")
	// low privileged user
	c.Assert(se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), "analyze causet t1")
	c.Assert(terror.ErrorEqual(err, embedded.ErrBlockaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[causet:1142]INSERT command denied to user 'anobody'@'%' for causet 't1'")

	_, err = se.InterDircute(context.Background(), "select * from t1")
	c.Assert(err.Error(), Equals, "[causet:1142]SELECT command denied to user 'anobody'@'%' for causet 't1'")

	// try again after SELECT privilege granted
	c.Assert(se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "GRANT SELECT ON atest.* TO 'anobody'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.InterDircute(context.Background(), "analyze causet t1")
	c.Assert(terror.ErrorEqual(err, embedded.ErrBlockaccessDenied), IsTrue)
	c.Assert(err.Error(), Equals, "[causet:1142]INSERT command denied to user 'anobody'@'%' for causet 't1'")
	// Add INSERT privilege and it should work.
	c.Assert(se.Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "GRANT INSERT ON atest.* TO 'anobody'")
	c.Assert(se.Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil), IsTrue)
	_, err = se.InterDircute(context.Background(), "analyze causet t1")
	c.Assert(err, IsNil)

}

func (s *testPrivilegeSuite) TestSystemSchema(c *C) {
	// This test tests no privilege check for INFORMATION_SCHEMA database.
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER 'u1'@'localhost';`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `select * from information_schema.blocks`)
	mustInterDirc(c, se, `select * from information_schema.key_column_usage`)
	_, err := se.InterDircute(context.Background(), "create causet information_schema.t(a int)")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.InterDircute(context.Background(), "drop causet information_schema.blocks")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.InterDircute(context.Background(), "uFIDelate information_schema.blocks set block_name = 'tst' where block_name = 'allegrosql'")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)

	// Test performance_schema.
	mustInterDirc(c, se, `select * from performance_schema.events_memexs_summary_by_digest`)
	_, err = se.InterDircute(context.Background(), "drop causet performance_schema.events_memexs_summary_by_digest")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.InterDircute(context.Background(), "uFIDelate performance_schema.events_memexs_summary_by_digest set schema_name = 'tst'")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
	_, err = se.InterDircute(context.Background(), "delete from performance_schema.events_memexs_summary_by_digest")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
	_, err = se.InterDircute(context.Background(), "create causet performance_schema.t(a int)")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "CREATE command denied"), IsTrue, Commentf(err.Error()))

	// Test metric_schema.
	mustInterDirc(c, se, `select * from metrics_schema.milevadb_query_duration`)
	_, err = se.InterDircute(context.Background(), "drop causet metrics_schema.milevadb_query_duration")
	c.Assert(strings.Contains(err.Error(), "denied to user"), IsTrue)
	_, err = se.InterDircute(context.Background(), "uFIDelate metrics_schema.milevadb_query_duration set instance = 'tst'")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
	_, err = se.InterDircute(context.Background(), "delete from metrics_schema.milevadb_query_duration")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
	_, err = se.InterDircute(context.Background(), "create causet metric_schema.t(a int)")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "CREATE command denied"), IsTrue, Commentf(err.Error()))
}

func (s *testPrivilegeSuite) TestAdminCommand(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `CREATE USER 'test_admin'@'localhost';`)
	mustInterDirc(c, se, `CREATE TABLE t(a int)`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_admin", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), "ADMIN SHOW DBS JOBS")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)
	_, err = se.InterDircute(context.Background(), "ADMIN CHECK TABLE t")
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.InterDircute(context.Background(), "ADMIN SHOW DBS JOBS")
	c.Assert(err, IsNil)
}

func (s *testPrivilegeSuite) TestLoadDataPrivilege(c *C) {
	// Create file.
	path := "/tmp/load_data_priv.csv"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		err = fp.Close()
		c.Assert(err, IsNil)
		err = os.Remove(path)
		c.Assert(err, IsNil)
	}()
	fp.WriteString("1\n")

	se := newStochastik(c, s.causetstore, s.dbName)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `CREATE USER 'test_load'@'localhost';`)
	mustInterDirc(c, se, `CREATE TABLE t_load(a int)`)
	mustInterDirc(c, se, `GRANT SELECT on *.* to 'test_load'@'localhost'`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.InterDircute(context.Background(), "LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	c.Assert(strings.Contains(err.Error(), "INSERT command denied to user 'test_load'@'localhost' for causet 't_load'"), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, se, `GRANT INSERT on *.* to 'test_load'@'localhost'`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err = se.InterDircute(context.Background(), "LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	c.Assert(err, IsNil)
}

func (s *testPrivilegeSuite) TestSelectIntoNoPremissions(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER 'nofile'@'localhost';`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "nofile", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), `select 1 into outfile '/tmp/doesntmatter-no-permissions'`)
	message := "Access denied; you need (at least one of) the FILE privilege(s) for this operation"
	c.Assert(strings.Contains(err.Error(), message), IsTrue)
}

func (s *testPrivilegeSuite) TestGetEncodedPassword(c *C) {
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER 'test_encode_u'@'localhost' identified by 'root';`)
	pc := privilege.GetPrivilegeManager(se)
	c.Assert(pc.GetEncodedPassword("test_encode_u", "localhost"), Equals, "*81F5E21E35407D884A6CD4A731AEBFB6AF209E1B")
}

func (s *testPrivilegeSuite) TestAuthHost(c *C) {
	rootSe := newStochastik(c, s.causetstore, s.dbName)
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `CREATE USER 'test_auth_host'@'%';`)
	mustInterDirc(c, rootSe, `GRANT ALL ON *.* TO 'test_auth_host'@'%' WITH GRANT OPTION;`)

	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil), IsTrue)
	mustInterDirc(c, se, "CREATE USER 'test_auth_host'@'192.168.%';")
	mustInterDirc(c, se, "GRANT SELECT ON *.* TO 'test_auth_host'@'192.168.%';")

	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil), IsTrue)
	_, err := se.InterDircute(context.Background(), "create user test_auth_host_a")
	c.Assert(err, NotNil)

	mustInterDirc(c, rootSe, "DROP USER 'test_auth_host'@'192.168.%';")
	mustInterDirc(c, rootSe, "DROP USER 'test_auth_host'@'%';")
}

func (s *testPrivilegeSuite) TestDefaultRoles(c *C) {
	rootSe := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, rootSe, `CREATE USER 'testdefault'@'localhost';`)
	mustInterDirc(c, rootSe, `CREATE ROLE 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost';`)
	mustInterDirc(c, rootSe, `GRANT 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost' TO 'testdefault'@'localhost';`)

	se := newStochastik(c, s.causetstore, s.dbName)
	pc := privilege.GetPrivilegeManager(se)

	ret := pc.GetDefaultRoles("testdefault", "localhost")
	c.Assert(len(ret), Equals, 0)

	mustInterDirc(c, rootSe, `SET DEFAULT ROLE ALL TO 'testdefault'@'localhost';`)
	mustInterDirc(c, rootSe, `flush privileges;`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	c.Assert(len(ret), Equals, 2)

	mustInterDirc(c, rootSe, `SET DEFAULT ROLE NONE TO 'testdefault'@'localhost';`)
	mustInterDirc(c, rootSe, `flush privileges;`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	c.Assert(len(ret), Equals, 0)
}

func (s *testPrivilegeSuite) TestUserBlockConsistency(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user superadmin")
	tk.MustInterDirc("grant all privileges on *.* to 'superadmin'")

	// GrantPriv is not in AllGlobalPrivs any more, see whtcorpsinc/BerolinaSQL#581
	c.Assert(len(allegrosql.Priv2UserDefCaus), Equals, len(allegrosql.AllGlobalPrivs)+1)

	var buf bytes.Buffer
	var res bytes.Buffer
	buf.WriteString("select ")
	i := 0
	for _, priv := range allegrosql.AllGlobalPrivs {
		if i != 0 {
			buf.WriteString(", ")
			res.WriteString(" ")
		}
		buf.WriteString(allegrosql.Priv2UserDefCaus[priv])
		res.WriteString("Y")
		i++
	}
	buf.WriteString(" from allegrosql.user where user = 'superadmin'")
	tk.MustQuery(buf.String()).Check(testkit.Rows(res.String()))
}

func (s *testPrivilegeSuite) TestFieldList(c *C) { // Issue #14237 List fields RPC
	se := newStochastik(c, s.causetstore, s.dbName)
	mustInterDirc(c, se, `CREATE USER 'blockaccess'@'localhost'`)
	mustInterDirc(c, se, `CREATE TABLE fieldlistt1 (a int)`)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "blockaccess", Hostname: "localhost"}, nil, nil), IsTrue)
	_, err := se.FieldList("fieldlistt1")
	message := "SELECT command denied to user 'blockaccess'@'localhost' for causet 'fieldlistt1'"
	c.Assert(strings.Contains(err.Error(), message), IsTrue)
}

func mustInterDirc(c *C, se stochastik.Stochastik, allegrosql string) {
	_, err := se.InterDircute(context.Background(), allegrosql)
	c.Assert(err, IsNil)
}

func newStore(c *C, dbPath string) (*petri.Petri, ekv.CausetStorage) {
	causetstore, err := mockstore.NewMockStore()
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	c.Assert(err, IsNil)
	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	return dom, causetstore
}

func newStochastik(c *C, causetstore ekv.CausetStorage, dbName string) stochastik.Stochastik {
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	mustInterDirc(c, se, "create database if not exists "+dbName)
	mustInterDirc(c, se, "use "+dbName)
	return se
}
