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

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

func (s *testSuite3) TestCharsetDatabase(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	testALLEGROSQL := `create database if not exists cd_test_utf8 CHARACTER SET utf8 COLLATE utf8_bin;`
	tk.MustInterDirc(testALLEGROSQL)

	testALLEGROSQL = `create database if not exists cd_test_latin1 CHARACTER SET latin1 COLLATE latin1_swedish_ci;`
	tk.MustInterDirc(testALLEGROSQL)

	testALLEGROSQL = `use cd_test_utf8;`
	tk.MustInterDirc(testALLEGROSQL)
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Events("utf8"))
	tk.MustQuery(`select @@defCauslation_database;`).Check(testkit.Events("utf8_bin"))

	testALLEGROSQL = `use cd_test_latin1;`
	tk.MustInterDirc(testALLEGROSQL)
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Events("latin1"))
	tk.MustQuery(`select @@defCauslation_database;`).Check(testkit.Events("latin1_swedish_ci"))
}

func (s *testSuite3) TestDo(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("do 1, @a:=1")
	tk.MustQuery("select @a").Check(testkit.Events("1"))
}

func (s *testSuite3) TestSetRoleAllCorner(c *C) {
	// For user with no role, `SET ROLE ALL` should active
	// a empty slice, rather than nil.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user set_role_all")
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "set_role_all", Hostname: "localhost"}, nil, nil), IsTrue)
	ctx := context.Background()
	_, err = se.InterDircute(ctx, `set role all`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `select current_role`)
	c.Assert(err, IsNil)
}

func (s *testSuite3) TestCreateRole(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user testCreateRole;")
	tk.MustInterDirc("grant CREATE USER on *.* to testCreateRole;")
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testCreateRole", Hostname: "localhost"}, nil, nil), IsTrue)

	ctx := context.Background()
	_, err = se.InterDircute(ctx, `create role test_create_role;`)
	c.Assert(err, IsNil)
	tk.MustInterDirc("revoke CREATE USER on *.* from testCreateRole;")
	tk.MustInterDirc("drop role test_create_role;")
	tk.MustInterDirc("grant CREATE ROLE on *.* to testCreateRole;")
	_, err = se.InterDircute(ctx, `create role test_create_role;`)
	c.Assert(err, IsNil)
	tk.MustInterDirc("drop role test_create_role;")
	_, err = se.InterDircute(ctx, `create user test_create_role;`)
	c.Assert(err, NotNil)
	tk.MustInterDirc("drop user testCreateRole;")
}

func (s *testSuite3) TestDropRole(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user testCreateRole;")
	tk.MustInterDirc("create user test_create_role;")
	tk.MustInterDirc("grant CREATE USER on *.* to testCreateRole;")
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testCreateRole", Hostname: "localhost"}, nil, nil), IsTrue)

	ctx := context.Background()
	_, err = se.InterDircute(ctx, `drop role test_create_role;`)
	c.Assert(err, IsNil)
	tk.MustInterDirc("revoke CREATE USER on *.* from testCreateRole;")
	tk.MustInterDirc("create role test_create_role;")
	tk.MustInterDirc("grant DROP ROLE on *.* to testCreateRole;")
	_, err = se.InterDircute(ctx, `drop role test_create_role;`)
	c.Assert(err, IsNil)
	tk.MustInterDirc("create user test_create_role;")
	_, err = se.InterDircute(ctx, `drop user test_create_role;`)
	c.Assert(err, NotNil)
	tk.MustInterDirc("drop user testCreateRole;")
	tk.MustInterDirc("drop user test_create_role;")
}

func (s *testSuite3) TestTransaction(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("begin")
	ctx := tk.Se.(stochastikctx.Context)
	c.Assert(inTxn(ctx), IsTrue)
	tk.MustInterDirc("commit")
	c.Assert(inTxn(ctx), IsFalse)
	tk.MustInterDirc("begin")
	c.Assert(inTxn(ctx), IsTrue)
	tk.MustInterDirc("rollback")
	c.Assert(inTxn(ctx), IsFalse)

	// Test that begin implicitly commits previous transaction.
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet txn (a int)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert txn values (1)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("rollback")
	tk.MustQuery("select * from txn").Check(testkit.Events("1"))

	// Test that DBS implicitly commits previous transaction.
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert txn values (2)")
	tk.MustInterDirc("create causet txn2 (a int)")
	tk.MustInterDirc("rollback")
	tk.MustQuery("select * from txn").Check(testkit.Events("1", "2"))
}

func inTxn(ctx stochastikctx.Context) bool {
	return (ctx.GetStochastikVars().Status & allegrosql.ServerStatusInTrans) > 0
}

func (s *testSuite6) TestRole(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Make sure user test not in allegrosql.User.
	result := tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)

	// Test for DROP ROLE.
	createRoleALLEGROSQL := `CREATE ROLE 'test'@'localhost';`
	tk.MustInterDirc(createRoleALLEGROSQL)
	// Make sure user test in allegrosql.User.
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("")))
	// Insert relation into allegrosql.role_edges
	tk.MustInterDirc("insert into allegrosql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('localhost','test','%','root')")
	tk.MustInterDirc("insert into allegrosql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('localhost','test1','localhost','test1')")
	// Insert relation into allegrosql.default_roles
	tk.MustInterDirc("insert into allegrosql.default_roles (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) values ('%','root','localhost','test')")
	tk.MustInterDirc("insert into allegrosql.default_roles (HOST,USER,DEFAULT_ROLE_HOST,DEFAULT_ROLE_USER) values ('localhost','test','%','test1')")

	dropUserALLEGROSQL := `DROP ROLE IF EXISTS 'test'@'localhost' ;`
	_, err := tk.InterDirc(dropUserALLEGROSQL)
	c.Check(err, IsNil)

	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM allegrosql.role_edges WHERE TO_USER="test" and TO_HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM allegrosql.role_edges WHERE FROM_USER="test" and FROM_HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM allegrosql.default_roles WHERE USER="test" and HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM allegrosql.default_roles WHERE DEFAULT_ROLE_USER="test" and DEFAULT_ROLE_HOST="localhost"`)
	result.Check(nil)

	// Test for GRANT ROLE
	createRoleALLEGROSQL = `CREATE ROLE 'r_1'@'localhost', 'r_2'@'localhost', 'r_3'@'localhost';`
	tk.MustInterDirc(createRoleALLEGROSQL)
	grantRoleALLEGROSQL := `GRANT 'r_1'@'localhost' TO 'r_2'@'localhost';`
	tk.MustInterDirc(grantRoleALLEGROSQL)
	result = tk.MustQuery(`SELECT TO_USER FROM allegrosql.role_edges WHERE FROM_USER="r_1" and FROM_HOST="localhost"`)
	result.Check(testkit.Events("r_2"))

	grantRoleALLEGROSQL = `GRANT 'r_1'@'localhost' TO 'r_3'@'localhost', 'r_4'@'localhost';`
	_, err = tk.InterDirc(grantRoleALLEGROSQL)
	c.Check(err, NotNil)

	// Test grant role for current_user();
	stochastikVars := tk.Se.GetStochastikVars()
	originUser := stochastikVars.User
	stochastikVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "%"}
	tk.MustInterDirc("grant 'r_1'@'localhost' to current_user();")
	tk.MustInterDirc("revoke 'r_1'@'localhost' from 'root'@'%';")
	stochastikVars.User = originUser

	result = tk.MustQuery(`SELECT FROM_USER FROM allegrosql.role_edges WHERE TO_USER="r_3" and TO_HOST="localhost"`)
	result.Check(nil)

	dropRoleALLEGROSQL := `DROP ROLE IF EXISTS 'r_1'@'localhost' ;`
	tk.MustInterDirc(dropRoleALLEGROSQL)
	dropRoleALLEGROSQL = `DROP ROLE IF EXISTS 'r_2'@'localhost' ;`
	tk.MustInterDirc(dropRoleALLEGROSQL)
	dropRoleALLEGROSQL = `DROP ROLE IF EXISTS 'r_3'@'localhost' ;`
	tk.MustInterDirc(dropRoleALLEGROSQL)

	// Test for revoke role
	createRoleALLEGROSQL = `CREATE ROLE 'test'@'localhost', r_1, r_2;`
	tk.MustInterDirc(createRoleALLEGROSQL)
	tk.MustInterDirc("insert into allegrosql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('localhost','test','%','root')")
	tk.MustInterDirc("insert into allegrosql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_1','%','root')")
	tk.MustInterDirc("insert into allegrosql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_2','%','root')")
	tk.MustInterDirc("flush privileges")
	tk.MustInterDirc("SET DEFAULT ROLE r_1, r_2 TO root")
	_, err = tk.InterDirc("revoke test@localhost, r_1 from root;")
	c.Check(err, IsNil)
	_, err = tk.InterDirc("revoke `r_2`@`%` from root, u_2;")
	c.Check(err, NotNil)
	_, err = tk.InterDirc("revoke `r_2`@`%` from root;")
	c.Check(err, IsNil)
	_, err = tk.InterDirc("revoke `r_1`@`%` from root;")
	c.Check(err, IsNil)
	result = tk.MustQuery(`SELECT * FROM allegrosql.default_roles WHERE DEFAULT_ROLE_USER="test" and DEFAULT_ROLE_HOST="localhost"`)
	result.Check(nil)
	result = tk.MustQuery(`SELECT * FROM allegrosql.default_roles WHERE USER="root" and HOST="%"`)
	result.Check(nil)
	dropRoleALLEGROSQL = `DROP ROLE 'test'@'localhost', r_1, r_2;`
	tk.MustInterDirc(dropRoleALLEGROSQL)

	ctx := tk.Se.(stochastikctx.Context)
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "test1", Hostname: "localhost"}
	c.Assert(tk.InterDircToErr("SET ROLE role1, role2"), NotNil)
	tk.MustInterDirc("SET ROLE ALL")
	tk.MustInterDirc("SET ROLE ALL EXCEPT role1, role2")
	tk.MustInterDirc("SET ROLE DEFAULT")
	tk.MustInterDirc("SET ROLE NONE")
}

func (s *testSuite3) TestRoleAdmin(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("CREATE USER 'testRoleAdmin';")
	tk.MustInterDirc("CREATE ROLE 'targetRole';")

	// Create a new stochastik.
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testRoleAdmin", Hostname: "localhost"}, nil, nil), IsTrue)

	ctx := context.Background()
	_, err = se.InterDircute(ctx, "GRANT `targetRole` TO `testRoleAdmin`;")
	c.Assert(err, NotNil)

	tk.MustInterDirc("GRANT SUPER ON *.* TO `testRoleAdmin`;")
	_, err = se.InterDircute(ctx, "GRANT `targetRole` TO `testRoleAdmin`;")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, "REVOKE `targetRole` FROM `testRoleAdmin`;")
	c.Assert(err, IsNil)

	tk.MustInterDirc("DROP USER 'testRoleAdmin';")
	tk.MustInterDirc("DROP ROLE 'targetRole';")
}

func (s *testSuite3) TestDefaultRole(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	createRoleALLEGROSQL := `CREATE ROLE r_1, r_2, r_3, u_1;`
	tk.MustInterDirc(createRoleALLEGROSQL)

	tk.MustInterDirc("insert into allegrosql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_1','%','u_1')")
	tk.MustInterDirc("insert into allegrosql.role_edges (FROM_HOST,FROM_USER,TO_HOST,TO_USER) values ('%','r_2','%','u_1')")

	tk.MustInterDirc("flush privileges;")

	setRoleALLEGROSQL := `SET DEFAULT ROLE r_3 TO u_1;`
	_, err := tk.InterDirc(setRoleALLEGROSQL)
	c.Check(err, NotNil)

	setRoleALLEGROSQL = `SET DEFAULT ROLE r_1 TO u_1000;`
	_, err = tk.InterDirc(setRoleALLEGROSQL)
	c.Check(err, NotNil)

	setRoleALLEGROSQL = `SET DEFAULT ROLE r_1, r_3 TO u_1;`
	_, err = tk.InterDirc(setRoleALLEGROSQL)
	c.Check(err, NotNil)

	setRoleALLEGROSQL = `SET DEFAULT ROLE r_1 TO u_1;`
	_, err = tk.InterDirc(setRoleALLEGROSQL)
	c.Check(err, IsNil)
	result := tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM allegrosql.default_roles WHERE USER="u_1"`)
	result.Check(testkit.Events("r_1"))
	setRoleALLEGROSQL = `SET DEFAULT ROLE r_2 TO u_1;`
	_, err = tk.InterDirc(setRoleALLEGROSQL)
	c.Check(err, IsNil)
	result = tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM allegrosql.default_roles WHERE USER="u_1"`)
	result.Check(testkit.Events("r_2"))

	setRoleALLEGROSQL = `SET DEFAULT ROLE ALL TO u_1;`
	_, err = tk.InterDirc(setRoleALLEGROSQL)
	c.Check(err, IsNil)
	result = tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM allegrosql.default_roles WHERE USER="u_1"`)
	result.Check(testkit.Events("r_1", "r_2"))

	setRoleALLEGROSQL = `SET DEFAULT ROLE NONE TO u_1;`
	_, err = tk.InterDirc(setRoleALLEGROSQL)
	c.Check(err, IsNil)
	result = tk.MustQuery(`SELECT DEFAULT_ROLE_USER FROM allegrosql.default_roles WHERE USER="u_1"`)
	result.Check(nil)

	dropRoleALLEGROSQL := `DROP USER r_1, r_2, r_3, u_1;`
	tk.MustInterDirc(dropRoleALLEGROSQL)
}

func (s *testSuite7) TestSetDefaultRoleAll(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user test_all;")
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "test_all", Hostname: "localhost"}, nil, nil), IsTrue)

	ctx := context.Background()
	_, err = se.InterDircute(ctx, "set default role all to test_all;")
	c.Assert(err, IsNil)
}

func (s *testSuite7) TestUser(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Make sure user test not in allegrosql.User.
	result := tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test" and Host="localhost"`)
	result.Check(nil)
	// Create user test.
	createUserALLEGROSQL := `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	// Make sure user test in allegrosql.User.
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("123")))
	// Create duplicate user with IfNotExists will be success.
	createUserALLEGROSQL = `CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)

	// Create duplicate user without IfNotExists will cause error.
	createUserALLEGROSQL = `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustGetErrCode(createUserALLEGROSQL, allegrosql.ErrCannotUser)
	createUserALLEGROSQL = `CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|3163|User 'test'@'localhost' already exists."))
	dropUserALLEGROSQL := `DROP USER IF EXISTS 'test'@'localhost' ;`
	tk.MustInterDirc(dropUserALLEGROSQL)
	// Create user test.
	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost';`
	tk.MustInterDirc(createUserALLEGROSQL)
	// Make sure user test in allegrosql.User.
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("")))
	dropUserALLEGROSQL = `DROP USER IF EXISTS 'test1'@'localhost' ;`
	tk.MustInterDirc(dropUserALLEGROSQL)

	// Test alter user.
	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost' IDENTIFIED BY '123', 'test2'@'localhost' IDENTIFIED BY '123', 'test3'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	alterUserALLEGROSQL := `ALTER USER 'test1'@'localhost' IDENTIFIED BY '111';`
	tk.MustInterDirc(alterUserALLEGROSQL)
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("111")))
	alterUserALLEGROSQL = `ALTER USER 'test_not_exist'@'localhost' IDENTIFIED BY '111';`
	tk.MustGetErrCode(alterUserALLEGROSQL, allegrosql.ErrCannotUser)
	alterUserALLEGROSQL = `ALTER USER 'test1'@'localhost' IDENTIFIED BY '222', 'test_not_exist'@'localhost' IDENTIFIED BY '111';`
	tk.MustGetErrCode(alterUserALLEGROSQL, allegrosql.ErrCannotUser)
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("222")))

	alterUserALLEGROSQL = `ALTER USER IF EXISTS 'test2'@'localhost' IDENTIFIED BY '222', 'test_not_exist'@'localhost' IDENTIFIED BY '1';`
	tk.MustInterDirc(alterUserALLEGROSQL)
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|3162|User 'test_not_exist'@'localhost' does not exist."))
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test2" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("222")))
	alterUserALLEGROSQL = `ALTER USER IF EXISTS'test_not_exist'@'localhost' IDENTIFIED BY '1', 'test3'@'localhost' IDENTIFIED BY '333';`
	tk.MustInterDirc(alterUserALLEGROSQL)
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|3162|User 'test_not_exist'@'localhost' does not exist."))
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test3" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("333")))

	// Test alter user user().
	alterUserALLEGROSQL = `ALTER USER USER() IDENTIFIED BY '1';`
	_, err := tk.InterDirc(alterUserALLEGROSQL)
	c.Check(terror.ErrorEqual(err, errors.New("Stochastik user is empty")), IsTrue, Commentf("err %v", err))
	tk.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	ctx := tk.Se.(stochastikctx.Context)
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "test1", Hostname: "localhost", AuthHostname: "localhost"}
	tk.MustInterDirc(alterUserALLEGROSQL)
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="test1" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("1")))
	dropUserALLEGROSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	tk.MustInterDirc(dropUserALLEGROSQL)

	// Test drop user if exists.
	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustInterDirc(createUserALLEGROSQL)
	dropUserALLEGROSQL = `DROP USER IF EXISTS 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost' ;`
	tk.MustInterDirc(dropUserALLEGROSQL)
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|3162|User test2@localhost does not exist."))

	// Test negative cases without IF EXISTS.
	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustInterDirc(createUserALLEGROSQL)
	dropUserALLEGROSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	tk.MustGetErrCode(dropUserALLEGROSQL, allegrosql.ErrCannotUser)
	dropUserALLEGROSQL = `DROP USER 'test3'@'localhost';`
	tk.MustInterDirc(dropUserALLEGROSQL)
	dropUserALLEGROSQL = `DROP USER 'test1'@'localhost';`
	tk.MustInterDirc(dropUserALLEGROSQL)
	// Test positive cases without IF EXISTS.
	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustInterDirc(createUserALLEGROSQL)
	dropUserALLEGROSQL = `DROP USER 'test1'@'localhost', 'test3'@'localhost';`
	tk.MustInterDirc(dropUserALLEGROSQL)

	// Test 'identified by password'
	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost' identified by password 'xxx';`
	_, err = tk.InterDirc(createUserALLEGROSQL)
	c.Assert(terror.ErrorEqual(interlock.ErrPasswordFormat, err), IsTrue, Commentf("err %v", err))
	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost' identified by password '*3D56A309CD04FA2EEF181462E59011F075C89548';`
	tk.MustInterDirc(createUserALLEGROSQL)
	dropUserALLEGROSQL = `DROP USER 'test1'@'localhost';`
	tk.MustInterDirc(dropUserALLEGROSQL)

	// Test drop user meet error
	_, err = tk.InterDirc(dropUserALLEGROSQL)
	c.Assert(terror.ErrorEqual(err, interlock.ErrCannotUser.GenWithStackByArgs("DROP USER", "")), IsTrue, Commentf("err %v", err))

	createUserALLEGROSQL = `CREATE USER 'test1'@'localhost'`
	tk.MustInterDirc(createUserALLEGROSQL)
	createUserALLEGROSQL = `CREATE USER 'test2'@'localhost'`
	tk.MustInterDirc(createUserALLEGROSQL)

	dropUserALLEGROSQL = `DROP USER 'test1'@'localhost', 'test2'@'localhost', 'test3'@'localhost';`
	_, err = tk.InterDirc(dropUserALLEGROSQL)
	c.Assert(terror.ErrorEqual(err, interlock.ErrCannotUser.GenWithStackByArgs("DROP USER", "")), IsTrue, Commentf("err %v", err))

	// Close issue #17639
	dropUserALLEGROSQL = `DROP USER if exists test3@'%'`
	tk.MustInterDirc(dropUserALLEGROSQL)
	createUserALLEGROSQL = `create user test3@'%' IDENTIFIED WITH 'mysql_native_password' AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';`
	tk.MustInterDirc(createUserALLEGROSQL)
	queryALLEGROSQL := `select authentication_string from allegrosql.user where user="test3" ;`
	tk.MustQuery(queryALLEGROSQL).Check(testkit.Events("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9"))
	alterUserALLEGROSQL = `alter user test3@'%' IDENTIFIED WITH 'mysql_native_password' AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';`
	tk.MustInterDirc(alterUserALLEGROSQL)
	tk.MustQuery(queryALLEGROSQL).Check(testkit.Events("*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9"))
}

func (s *testSuite3) TestSetPwd(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	createUserALLEGROSQL := `CREATE USER 'testpwd'@'localhost' IDENTIFIED BY '';`
	tk.MustInterDirc(createUserALLEGROSQL)
	result := tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Events(""))

	// set password for
	tk.MustInterDirc(`SET PASSWORD FOR 'testpwd'@'localhost' = 'password';`)
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("password")))

	// set password
	setPwdALLEGROSQL := `SET PASSWORD = 'pwd'`
	// Stochastik user is empty.
	_, err := tk.InterDirc(setPwdALLEGROSQL)
	c.Check(err, NotNil)
	tk.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	ctx := tk.Se.(stochastikctx.Context)
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "testpwd1", Hostname: "localhost", AuthUsername: "testpwd1", AuthHostname: "localhost"}
	// Stochastik user doesn't exist.
	_, err = tk.InterDirc(setPwdALLEGROSQL)
	c.Check(terror.ErrorEqual(err, interlock.ErrPasswordNoMatch), IsTrue, Commentf("err %v", err))
	// normal
	ctx.GetStochastikVars().User = &auth.UserIdentity{Username: "testpwd", Hostname: "localhost", AuthUsername: "testpwd", AuthHostname: "localhost"}
	tk.MustInterDirc(setPwdALLEGROSQL)
	result = tk.MustQuery(`SELECT authentication_string FROM allegrosql.User WHERE User="testpwd" and Host="localhost"`)
	result.Check(testkit.Events(auth.EncodePassword("pwd")))

}

func (s *testSuite3) TestKillStmt(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("kill 1")

	result := tk.MustQuery("show warnings")
	result.Check(testkit.Events("Warning 1105 Invalid operation. Please use 'KILL MilevaDB [CONNECTION | QUERY] connectionID' instead"))
}

func (s *testSuite3) TestFlushPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc(`CREATE USER 'testflush'@'localhost' IDENTIFIED BY '';`)
	tk.MustInterDirc(`UFIDelATE allegrosql.User SET Select_priv='Y' WHERE User="testflush" and Host="localhost"`)

	// Create a new stochastik.
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "testflush", Hostname: "localhost"}, nil, nil), IsTrue)

	ctx := context.Background()
	// Before flush.
	_, err = se.InterDircute(ctx, `SELECT authentication_string FROM allegrosql.User WHERE User="testflush" and Host="localhost"`)
	c.Check(err, NotNil)

	tk.MustInterDirc("FLUSH PRIVILEGES")

	// After flush.
	_, err = se.InterDircute(ctx, `SELECT authentication_string FROM allegrosql.User WHERE User="testflush" and Host="localhost"`)
	c.Check(err, IsNil)

}

type testFlushSuite struct{}

func (s *testFlushSuite) TestFlushPrivilegesPanic(c *C) {
	// Run in a separate suite because this test need to set SkipGrantBlock config.
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()

	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.Security.SkipGrantBlock = true
	})

	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom.Close()

	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("FLUSH PRIVILEGES")
}

func (s *testSuite3) TestDropStats(c *C) {
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 int, c2 int)")
	do := petri.GetPetri(testKit.Se)
	is := do.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	h := do.StatsHandle()
	h.Clear()
	testKit.MustInterDirc("analyze causet t")
	statsTbl := h.GetBlockStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustInterDirc("drop stats t")
	c.Assert(h.UFIDelate(is), IsNil)
	statsTbl = h.GetBlockStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)

	testKit.MustInterDirc("analyze causet t")
	statsTbl = h.GetBlockStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	h.SetLease(1)
	testKit.MustInterDirc("drop stats t")
	c.Assert(h.UFIDelate(is), IsNil)
	statsTbl = h.GetBlockStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	h.SetLease(0)
}

func (s *testSuite3) TestDropStatsFromKV(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t (c1 varchar(20), c2 varchar(20))")
	tk.MustInterDirc(`insert into t values("1","1"),("2","2"),("3","3"),("4","4")`)
	tk.MustInterDirc("insert into t select * from t")
	tk.MustInterDirc("insert into t select * from t")
	tk.MustInterDirc("analyze causet t")
	tblID := tk.MustQuery(`select milevadb_block_id from information_schema.blocks where block_name = "t" and block_schema = "test"`).Events()[0][0].(string)
	tk.MustQuery("select modify_count, count from allegrosql.stats_spacetime where block_id = " + tblID).Check(
		testkit.Events("0 16"))
	tk.MustQuery("select hist_id from allegrosql.stats_histograms where block_id = " + tblID).Check(
		testkit.Events("1", "2"))
	tk.MustQuery("select hist_id, bucket_id from allegrosql.stats_buckets where block_id = " + tblID).Check(
		testkit.Events("1 0",
			"1 1",
			"1 2",
			"1 3",
			"2 0",
			"2 1",
			"2 2",
			"2 3"))
	tk.MustQuery("select hist_id from allegrosql.stats_top_n where block_id = " + tblID).Check(
		testkit.Events("1", "1", "1", "1", "2", "2", "2", "2"))

	tk.MustInterDirc("drop stats t")
	tk.MustQuery("select modify_count, count from allegrosql.stats_spacetime where block_id = " + tblID).Check(
		testkit.Events("0 16"))
	tk.MustQuery("select hist_id from allegrosql.stats_histograms where block_id = " + tblID).Check(
		testkit.Events())
	tk.MustQuery("select hist_id, bucket_id from allegrosql.stats_buckets where block_id = " + tblID).Check(
		testkit.Events())
	tk.MustQuery("select hist_id from allegrosql.stats_top_n where block_id = " + tblID).Check(
		testkit.Events())
}

func (s *testSuite3) TestFlushBlocks(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	_, err := tk.InterDirc("FLUSH TABLES")
	c.Check(err, IsNil)

	_, err = tk.InterDirc("FLUSH TABLES WITH READ LOCK")
	c.Check(err, NotNil)

}

func (s *testSuite3) TestUseDB(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	_, err := tk.InterDirc("USE test")
	c.Check(err, IsNil)

	_, err = tk.InterDirc("USE ``")
	c.Assert(terror.ErrorEqual(embedded.ErrNoDB, err), IsTrue, Commentf("err %v", err))
}

func (s *testSuite3) TestStmtAutoNewTxn(c *C) {
	// Some memexs are like DBS, they commit the previous txn automically.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	// Fix issue https://github.com/whtcorpsinc/milevadb/issues/10705
	tk.MustInterDirc("begin")
	tk.MustInterDirc("create user 'xxx'@'%';")
	tk.MustInterDirc("grant all privileges on *.* to 'xxx'@'%';")

	tk.MustInterDirc("create causet auto_new (id int)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into auto_new values (1)")
	tk.MustInterDirc("revoke all privileges on *.* from 'xxx'@'%'")
	tk.MustInterDirc("rollback") // insert memex has already committed
	tk.MustQuery("select * from auto_new").Check(testkit.Events("1"))

	// Test the behavior when autocommit is false.
	tk.MustInterDirc("set autocommit = 0")
	tk.MustInterDirc("insert into auto_new values (2)")
	tk.MustInterDirc("create user 'yyy'@'%'")
	tk.MustInterDirc("rollback")
	tk.MustQuery("select * from auto_new").Check(testkit.Events("1", "2"))

	tk.MustInterDirc("drop user 'yyy'@'%'")
	tk.MustInterDirc("insert into auto_new values (3)")
	tk.MustInterDirc("rollback")
	tk.MustQuery("select * from auto_new").Check(testkit.Events("1", "2"))
}

func (s *testSuite3) TestIssue9111(c *C) {
	// CREATE USER / DROP USER fails if admin doesn't have insert privilege on `allegrosql.user` causet.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user 'user_admin'@'localhost';")
	tk.MustInterDirc("grant create user on *.* to 'user_admin'@'localhost';")

	// Create a new stochastik.
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "user_admin", Hostname: "localhost"}, nil, nil), IsTrue)

	ctx := context.Background()
	_, err = se.InterDircute(ctx, `create user test_create_user`)
	c.Check(err, IsNil)
	_, err = se.InterDircute(ctx, `drop user test_create_user`)
	c.Check(err, IsNil)

	tk.MustInterDirc("revoke create user on *.* from 'user_admin'@'localhost';")
	tk.MustInterDirc("grant insert, delete on allegrosql.user to 'user_admin'@'localhost';")

	_, err = se.InterDircute(ctx, `create user test_create_user`)
	c.Check(err, IsNil)
	_, err = se.InterDircute(ctx, `drop user test_create_user`)
	c.Check(err, IsNil)

	_, err = se.InterDircute(ctx, `create role test_create_user`)
	c.Check(err, IsNil)
	_, err = se.InterDircute(ctx, `drop role test_create_user`)
	c.Check(err, IsNil)

	tk.MustInterDirc("drop user 'user_admin'@'localhost';")
}

func (s *testSuite3) TestRoleAtomic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("create role r2;")
	_, err := tk.InterDirc("create role r1, r2, r3")
	c.Check(err, NotNil)
	// Check atomic create role.
	result := tk.MustQuery(`SELECT user FROM allegrosql.User WHERE user in ('r1', 'r2', 'r3')`)
	result.Check(testkit.Events("r2"))
	// Check atomic drop role.
	_, err = tk.InterDirc("drop role r1, r2, r3")
	c.Check(err, NotNil)
	result = tk.MustQuery(`SELECT user FROM allegrosql.User WHERE user in ('r1', 'r2', 'r3')`)
	result.Check(testkit.Events("r2"))
	tk.MustInterDirc("drop role r2;")
}

func (s *testSuite3) TestExtendedStatsPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int)")
	tk.MustInterDirc("create user 'u1'@'%'")
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Check(err, IsNil)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil), IsTrue)
	ctx := context.Background()
	_, err = se.InterDircute(ctx, "create statistics s1(correlation) on test.t(a,b)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[causet:1142]CREATE STATISTICS command denied to user 'u1'@'%' for causet 't'")
	tk.MustInterDirc("grant select on test.* to 'u1'@'%'")
	_, err = se.InterDircute(ctx, "create statistics s1(correlation) on test.t(a,b)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[causet:1142]CREATE STATISTICS command denied to user 'u1'@'%' for causet 'stats_extended'")
	tk.MustInterDirc("grant insert on allegrosql.stats_extended to 'u1'@'%'")
	_, err = se.InterDircute(ctx, "create statistics s1(correlation) on test.t(a,b)")
	c.Assert(err, IsNil)

	_, err = se.InterDircute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, "drop statistics s1")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[causet:1142]DROP STATISTICS command denied to user 'u1'@'%' for causet 'stats_extended'")
	tk.MustInterDirc("grant uFIDelate on allegrosql.stats_extended to 'u1'@'%'")
	_, err = se.InterDircute(ctx, "drop statistics s1")
	c.Assert(err, IsNil)
	tk.MustInterDirc("drop user 'u1'@'%'")
}

func (s *testSuite3) TestIssue17247(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user 'issue17247'")
	tk.MustInterDirc("grant CREATE USER on *.* to 'issue17247'")

	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustInterDirc("use test")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "issue17247", Hostname: "%"}, nil, nil), IsTrue)
	tk1.MustInterDirc("ALTER USER USER() IDENTIFIED BY 'xxx'")
	tk1.MustInterDirc("ALTER USER CURRENT_USER() IDENTIFIED BY 'yyy'")
	tk1.MustInterDirc("ALTER USER CURRENT_USER IDENTIFIED BY 'zzz'")
	tk.MustInterDirc("ALTER USER 'issue17247'@'%' IDENTIFIED BY 'kkk'")
	tk.MustInterDirc("ALTER USER 'issue17247'@'%' IDENTIFIED BY PASSWORD '*B50FBDB37F1256824274912F2A1CE648082C3F1F'")
	// Wrong grammar
	_, err := tk1.InterDirc("ALTER USER USER() IDENTIFIED BY PASSWORD '*B50FBDB37F1256824274912F2A1CE648082C3F1F'")
	c.Assert(err, NotNil)
}
