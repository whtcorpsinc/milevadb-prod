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
	"fmt"
	"strings"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuiteP1) TestGrantGlobal(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	createUserALLEGROSQL := `CREATE USER 'testGlobal'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	// Make sure all the global privs for new user is "N".
	for _, v := range allegrosql.AllDBPrivs {
		allegrosql := fmt.Sprintf("SELECT %s FROM allegrosql.User WHERE User=\"testGlobal\" and host=\"localhost\";", allegrosql.Priv2UserDefCaus[v])
		r := tk.MustQuery(allegrosql)
		r.Check(testkit.Events("N"))
	}

	// Grant each priv to the user.
	for _, v := range allegrosql.AllGlobalPrivs {
		allegrosql := fmt.Sprintf("GRANT %s ON *.* TO 'testGlobal'@'localhost';", allegrosql.Priv2Str[v])
		tk.MustInterDirc(allegrosql)
		allegrosql = fmt.Sprintf("SELECT %s FROM allegrosql.User WHERE User=\"testGlobal\" and host=\"localhost\"", allegrosql.Priv2UserDefCaus[v])
		tk.MustQuery(allegrosql).Check(testkit.Events("Y"))
	}

	// Create a new user.
	createUserALLEGROSQL = `CREATE USER 'testGlobal1'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	tk.MustInterDirc("GRANT ALL ON *.* TO 'testGlobal1'@'localhost';")
	// Make sure all the global privs for granted user is "Y".
	for _, v := range allegrosql.AllGlobalPrivs {
		allegrosql := fmt.Sprintf("SELECT %s FROM allegrosql.User WHERE User=\"testGlobal1\" and host=\"localhost\"", allegrosql.Priv2UserDefCaus[v])
		tk.MustQuery(allegrosql).Check(testkit.Events("Y"))
	}
	//with grant option
	tk.MustInterDirc("GRANT ALL ON *.* TO 'testGlobal1'@'localhost' WITH GRANT OPTION;")
	for _, v := range allegrosql.AllGlobalPrivs {
		allegrosql := fmt.Sprintf("SELECT %s FROM allegrosql.User WHERE User=\"testGlobal1\" and host=\"localhost\"", allegrosql.Priv2UserDefCaus[v])
		tk.MustQuery(allegrosql).Check(testkit.Events("Y"))
	}
}

func (s *testSuite3) TestGrantDBScope(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	createUserALLEGROSQL := `CREATE USER 'testDB'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	// Make sure all the EDB privs for new user is empty.
	allegrosql := fmt.Sprintf("SELECT * FROM allegrosql.EDB WHERE User=\"testDB\" and host=\"localhost\"")
	tk.MustQuery(allegrosql).Check(testkit.Events())

	// Grant each priv to the user.
	for _, v := range allegrosql.AllDBPrivs {
		allegrosql := fmt.Sprintf("GRANT %s ON test.* TO 'testDB'@'localhost';", allegrosql.Priv2Str[v])
		tk.MustInterDirc(allegrosql)
		allegrosql = fmt.Sprintf("SELECT %s FROM allegrosql.EDB WHERE User=\"testDB\" and host=\"localhost\" and EDB=\"test\"", allegrosql.Priv2UserDefCaus[v])
		tk.MustQuery(allegrosql).Check(testkit.Events("Y"))
	}

	// Create a new user.
	createUserALLEGROSQL = `CREATE USER 'testDB1'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	tk.MustInterDirc("USE test;")
	tk.MustInterDirc("GRANT ALL ON * TO 'testDB1'@'localhost';")
	// Make sure all the EDB privs for granted user is "Y".
	for _, v := range allegrosql.AllDBPrivs {
		allegrosql := fmt.Sprintf("SELECT %s FROM allegrosql.EDB WHERE User=\"testDB1\" and host=\"localhost\" and EDB=\"test\";", allegrosql.Priv2UserDefCaus[v])
		tk.MustQuery(allegrosql).Check(testkit.Events("Y"))
	}
}

func (s *testSuite3) TestWithGrantOption(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	createUserALLEGROSQL := `CREATE USER 'testWithGrant'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	// Make sure all the EDB privs for new user is empty.
	allegrosql := fmt.Sprintf("SELECT * FROM allegrosql.EDB WHERE User=\"testWithGrant\" and host=\"localhost\"")
	tk.MustQuery(allegrosql).Check(testkit.Events())

	// Grant select priv to the user, with grant option.
	tk.MustInterDirc("GRANT select ON test.* TO 'testWithGrant'@'localhost' WITH GRANT OPTION;")
	tk.MustQuery("SELECT grant_priv FROM allegrosql.EDB WHERE User=\"testWithGrant\" and host=\"localhost\" and EDB=\"test\"").Check(testkit.Events("Y"))

	tk.MustInterDirc("CREATE USER 'testWithGrant1'")
	tk.MustQuery("SELECT grant_priv FROM allegrosql.user WHERE User=\"testWithGrant1\"").Check(testkit.Events("N"))
	tk.MustInterDirc("GRANT ALL ON *.* TO 'testWithGrant1'")
	tk.MustQuery("SELECT grant_priv FROM allegrosql.user WHERE User=\"testWithGrant1\"").Check(testkit.Events("N"))
	tk.MustInterDirc("GRANT ALL ON *.* TO 'testWithGrant1' WITH GRANT OPTION")
	tk.MustQuery("SELECT grant_priv FROM allegrosql.user WHERE User=\"testWithGrant1\"").Check(testkit.Events("Y"))
}

func (s *testSuiteP1) TestBlockScope(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	createUserALLEGROSQL := `CREATE USER 'testTbl'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	tk.MustInterDirc(`CREATE TABLE test.test1(c1 int);`)
	// Make sure all the causet privs for new user is empty.
	tk.MustQuery(`SELECT * FROM allegrosql.Blocks_priv WHERE User="testTbl" and host="localhost" and EDB="test" and Block_name="test1"`).Check(testkit.Events())

	// Grant each priv to the user.
	for _, v := range allegrosql.AllBlockPrivs {
		allegrosql := fmt.Sprintf("GRANT %s ON test.test1 TO 'testTbl'@'localhost';", allegrosql.Priv2Str[v])
		tk.MustInterDirc(allegrosql)
		rows := tk.MustQuery(`SELECT Block_priv FROM allegrosql.Blocks_priv WHERE User="testTbl" and host="localhost" and EDB="test" and Block_name="test1";`).Events()
		c.Assert(rows, HasLen, 1)
		event := rows[0]
		c.Assert(event, HasLen, 1)
		p := fmt.Sprintf("%v", event[0])
		c.Assert(strings.Index(p, allegrosql.Priv2SetStr[v]), Greater, -1)
	}
	// Create a new user.
	createUserALLEGROSQL = `CREATE USER 'testTbl1'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	tk.MustInterDirc("USE test;")
	tk.MustInterDirc(`CREATE TABLE test2(c1 int);`)
	// Grant all causet scope privs.
	tk.MustInterDirc("GRANT ALL ON test2 TO 'testTbl1'@'localhost' WITH GRANT OPTION;")
	// Make sure all the causet privs for granted user are in the Block_priv set.
	for _, v := range allegrosql.AllBlockPrivs {
		rows := tk.MustQuery(`SELECT Block_priv FROM allegrosql.Blocks_priv WHERE User="testTbl1" and host="localhost" and EDB="test" and Block_name="test2";`).Events()
		c.Assert(rows, HasLen, 1)
		event := rows[0]
		c.Assert(event, HasLen, 1)
		p := fmt.Sprintf("%v", event[0])
		c.Assert(strings.Index(p, allegrosql.Priv2SetStr[v]), Greater, -1)
	}
}

func (s *testSuite3) TestDeferredCausetScope(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Create a new user.
	createUserALLEGROSQL := `CREATE USER 'testDefCaus'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	tk.MustInterDirc(`CREATE TABLE test.test3(c1 int, c2 int);`)

	// Make sure all the defCausumn privs for new user is empty.
	tk.MustQuery(`SELECT * FROM allegrosql.DeferredCausets_priv WHERE User="testDefCaus" and host="localhost" and EDB="test" and Block_name="test3" and DeferredCauset_name="c1"`).Check(testkit.Events())
	tk.MustQuery(`SELECT * FROM allegrosql.DeferredCausets_priv WHERE User="testDefCaus" and host="localhost" and EDB="test" and Block_name="test3" and DeferredCauset_name="c2"`).Check(testkit.Events())

	// Grant each priv to the user.
	for _, v := range allegrosql.AllDeferredCausetPrivs {
		allegrosql := fmt.Sprintf("GRANT %s(c1) ON test.test3 TO 'testDefCaus'@'localhost';", allegrosql.Priv2Str[v])
		tk.MustInterDirc(allegrosql)
		rows := tk.MustQuery(`SELECT DeferredCauset_priv FROM allegrosql.DeferredCausets_priv WHERE User="testDefCaus" and host="localhost" and EDB="test" and Block_name="test3" and DeferredCauset_name="c1";`).Events()
		c.Assert(rows, HasLen, 1)
		event := rows[0]
		c.Assert(event, HasLen, 1)
		p := fmt.Sprintf("%v", event[0])
		c.Assert(strings.Index(p, allegrosql.Priv2SetStr[v]), Greater, -1)
	}

	// Create a new user.
	createUserALLEGROSQL = `CREATE USER 'testDefCaus1'@'localhost' IDENTIFIED BY '123';`
	tk.MustInterDirc(createUserALLEGROSQL)
	tk.MustInterDirc("USE test;")
	// Grant all defCausumn scope privs.
	tk.MustInterDirc("GRANT ALL(c2) ON test3 TO 'testDefCaus1'@'localhost';")
	// Make sure all the defCausumn privs for granted user are in the DeferredCauset_priv set.
	for _, v := range allegrosql.AllDeferredCausetPrivs {
		rows := tk.MustQuery(`SELECT DeferredCauset_priv FROM allegrosql.DeferredCausets_priv WHERE User="testDefCaus1" and host="localhost" and EDB="test" and Block_name="test3" and DeferredCauset_name="c2";`).Events()
		c.Assert(rows, HasLen, 1)
		event := rows[0]
		c.Assert(event, HasLen, 1)
		p := fmt.Sprintf("%v", event[0])
		c.Assert(strings.Index(p, allegrosql.Priv2SetStr[v]), Greater, -1)
	}
}

func (s *testSuite3) TestIssue2456(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("CREATE USER 'dduser'@'%' IDENTIFIED by '123456';")
	tk.MustInterDirc("CREATE DATABASE `dddb_%`;")
	tk.MustInterDirc("CREATE causet `dddb_%`.`te%` (id int);")
	tk.MustInterDirc("GRANT ALL PRIVILEGES ON `dddb_%`.* TO 'dduser'@'%';")
	tk.MustInterDirc("GRANT ALL PRIVILEGES ON `dddb_%`.`te%` to 'dduser'@'%';")
}

func (s *testSuite3) TestNoAutoCreateUser(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`DROP USER IF EXISTS 'test'@'%'`)
	tk.MustInterDirc(`SET sql_mode='NO_AUTO_CREATE_USER'`)
	_, err := tk.InterDirc(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%' IDENTIFIED BY 'xxx'`)
	c.Check(err, NotNil)
	c.Assert(terror.ErrorEqual(err, interlock.ErrCantCreateUserWithGrant), IsTrue)
}

func (s *testSuite3) TestCreateUserWhenGrant(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`DROP USER IF EXISTS 'test'@'%'`)
	// This only applies to sql_mode:NO_AUTO_CREATE_USER off
	tk.MustInterDirc(`SET ALLEGROSQL_MODE=''`)
	tk.MustInterDirc(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%' IDENTIFIED BY 'xxx'`)
	// Make sure user is created automatically when grant to a non-exists one.
	tk.MustQuery(`SELECT user FROM allegrosql.user WHERE user='test' and host='%'`).Check(
		testkit.Events("test"),
	)
	tk.MustInterDirc(`DROP USER IF EXISTS 'test'@'%'`)
}

func (s *testSuite3) TestGrantPrivilegeAtomic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`drop role if exists r1, r2, r3, r4;`)
	tk.MustInterDirc(`create role r1, r2, r3;`)
	tk.MustInterDirc(`create causet test.testatomic(x int);`)

	_, err := tk.InterDirc(`grant uFIDelate, select, insert, delete on *.* to r1, r2, r4;`)
	c.Assert(terror.ErrorEqual(err, interlock.ErrCantCreateUserWithGrant), IsTrue)
	tk.MustQuery(`select UFIDelate_priv, Select_priv, Insert_priv, Delete_priv from allegrosql.user where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Events(
		"N N N N",
		"N N N N",
		"N N N N",
	))
	tk.MustInterDirc(`grant uFIDelate, select, insert, delete on *.* to r1, r2, r3;`)
	_, err = tk.InterDirc(`revoke all on *.* from r1, r2, r4, r3;`)
	c.Check(err, NotNil)
	tk.MustQuery(`select UFIDelate_priv, Select_priv, Insert_priv, Delete_priv from allegrosql.user where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Events(
		"Y Y Y Y",
		"Y Y Y Y",
		"Y Y Y Y",
	))

	_, err = tk.InterDirc(`grant uFIDelate, select, insert, delete on test.* to r1, r2, r4;`)
	c.Assert(terror.ErrorEqual(err, interlock.ErrCantCreateUserWithGrant), IsTrue)
	tk.MustQuery(`select UFIDelate_priv, Select_priv, Insert_priv, Delete_priv from allegrosql.EDB where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Events())
	tk.MustInterDirc(`grant uFIDelate, select, insert, delete on test.* to r1, r2, r3;`)
	_, err = tk.InterDirc(`revoke all on *.* from r1, r2, r4, r3;`)
	c.Check(err, NotNil)
	tk.MustQuery(`select UFIDelate_priv, Select_priv, Insert_priv, Delete_priv from allegrosql.EDB where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Events(
		"Y Y Y Y",
		"Y Y Y Y",
		"Y Y Y Y",
	))

	_, err = tk.InterDirc(`grant uFIDelate, select, insert, delete on test.testatomic to r1, r2, r4;`)
	c.Assert(terror.ErrorEqual(err, interlock.ErrCantCreateUserWithGrant), IsTrue)
	tk.MustQuery(`select Block_priv from allegrosql.blocks_priv where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Events())
	tk.MustInterDirc(`grant uFIDelate, select, insert, delete on test.testatomic to r1, r2, r3;`)
	_, err = tk.InterDirc(`revoke all on *.* from r1, r2, r4, r3;`)
	c.Check(err, NotNil)
	tk.MustQuery(`select Block_priv from allegrosql.blocks_priv where user in ('r1', 'r2', 'r3', 'r4') and host = "%";`).Check(testkit.Events(
		"Select,Insert,UFIDelate,Delete",
		"Select,Insert,UFIDelate,Delete",
		"Select,Insert,UFIDelate,Delete",
	))

	tk.MustInterDirc(`drop role if exists r1, r2, r3, r4;`)
	tk.MustInterDirc(`drop causet test.testatomic;`)

}

func (s *testSuite3) TestIssue2654(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`DROP USER IF EXISTS 'test'@'%'`)
	tk.MustInterDirc(`CREATE USER 'test'@'%' IDENTIFIED BY 'test'`)
	tk.MustInterDirc("GRANT SELECT ON test.* to 'test'")
	rows := tk.MustQuery(`SELECT user,host FROM allegrosql.user WHERE user='test' and host='%'`)
	rows.Check(testkit.Events(`test %`))
}

func (s *testSuite3) TestGrantUnderANSIQuotes(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// Fix a bug that the GrantInterDirc fails in ANSI_QUOTES allegrosql mode
	// The bug is caused by the improper usage of double quotes like:
	// INSERT INTO allegrosql.user ... VALUES ("..", "..", "..")
	tk.MustInterDirc(`SET ALLEGROSQL_MODE='ANSI_QUOTES'`)
	tk.MustInterDirc(`GRANT ALL PRIVILEGES ON video_ulimit.* TO web@'%' IDENTIFIED BY 'eDrkrhZ>l2sV'`)
	tk.MustInterDirc(`REVOKE ALL PRIVILEGES ON video_ulimit.* FROM web@'%';`)
	tk.MustInterDirc(`DROP USER IF EXISTS 'web'@'%'`)
}

func (s *testSuite3) TestMaintainRequire(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// test create with require
	tk.MustInterDirc(`CREATE USER 'ssl_auser'@'%' require issuer '/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustInterDirc(`CREATE USER 'ssl_buser'@'%' require subject '/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustInterDirc(`CREATE USER 'ssl_cuser'@'%' require cipher 'AES128-GCM-SHA256'`)
	tk.MustInterDirc(`CREATE USER 'ssl_duser'@'%'`)
	tk.MustInterDirc(`CREATE USER 'ssl_euser'@'%' require none`)
	tk.MustInterDirc(`CREATE USER 'ssl_fuser'@'%' require ssl`)
	tk.MustInterDirc(`CREATE USER 'ssl_guser'@'%' require x509`)
	tk.MustQuery("select * from allegrosql.global_priv where `user` like 'ssl_%'").Check(testkit.Events(
		"% ssl_auser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_issuer\":\"/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH\"}",
		"% ssl_buser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_subject\":\"/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH\"}",
		"% ssl_cuser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}",
		"% ssl_duser {}",
		"% ssl_euser {}",
		"% ssl_fuser {\"ssl_type\":1}",
		"% ssl_guser {\"ssl_type\":2}",
	))

	// test grant with require
	tk.MustInterDirc("CREATE USER 'u1'@'%'")
	tk.MustInterDirc("GRANT ALL ON *.* TO 'u1'@'%' require issuer '/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US' and subject '/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH'") // add new require.
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Events("{\"ssl_type\":3,\"x509_issuer\":\"/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH\"}"))
	tk.MustInterDirc("GRANT ALL ON *.* TO 'u1'@'%' require cipher 'AES128-GCM-SHA256'") // modify always overwrite.
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Events("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}"))
	tk.MustInterDirc("GRANT select ON *.* TO 'u1'@'%'") // modify without require should not modify old require.
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Events("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}"))
	tk.MustInterDirc("GRANT ALL ON *.* TO 'u1'@'%' require none") // use require none to clean up require.
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Events("{}"))

	// test alter with require
	tk.MustInterDirc("CREATE USER 'u2'@'%'")
	tk.MustInterDirc("alter user 'u2'@'%' require ssl")
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Events("{\"ssl_type\":1}"))
	tk.MustInterDirc("alter user 'u2'@'%' require x509")
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Events("{\"ssl_type\":2}"))
	tk.MustInterDirc("alter user 'u2'@'%' require issuer '/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'")
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Events("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_issuer\":\"/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH\"}"))
	tk.MustInterDirc("alter user 'u2'@'%' require none")
	tk.MustQuery("select priv from allegrosql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Events("{}"))

	// test show create user
	tk.MustInterDirc(`CREATE USER 'u3'@'%' require issuer '/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustQuery("show create user 'u3'").Check(testkit.Events("CREATE USER 'u3'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE CIPHER 'AES128-GCM-SHA256' ISSUER '/CN=MilevaDB admin/OU=MilevaDB/O=WHTCORPS INC/L=San Francisco/ST=California/C=US' SUBJECT '/CN=tester1/OU=MilevaDB/O=WHTCORPS INC.Inc/L=Haidian/ST=Beijing/C=ZH' PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	// check issuer/subject/cipher value
	_, err := tk.InterDirc(`CREATE USER 'u4'@'%' require issuer 'CN=MilevaDB,OU=WHTCORPS INC'`)
	c.Assert(err, NotNil)
	_, err = tk.InterDirc(`CREATE USER 'u5'@'%' require subject '/CN=MilevaDB\OU=WHTCORPS INC'`)
	c.Assert(err, NotNil)
	_, err = tk.InterDirc(`CREATE USER 'u6'@'%' require subject '/CN=MilevaDB\NC=WHTCORPS INC'`)
	c.Assert(err, NotNil)
	_, err = tk.InterDirc(`CREATE USER 'u7'@'%' require cipher 'AES128-GCM-SHA1'`)
	c.Assert(err, NotNil)
	_, err = tk.InterDirc(`CREATE USER 'u8'@'%' require subject '/CN'`)
	c.Assert(err, NotNil)
	_, err = tk.InterDirc(`CREATE USER 'u9'@'%' require cipher 'TLS_AES_256_GCM_SHA384' cipher 'RC4-SHA'`)
	c.Assert(err.Error(), Equals, "Duplicate require CIPHER clause")
	_, err = tk.InterDirc(`CREATE USER 'u9'@'%' require issuer 'CN=MilevaDB,OU=WHTCORPS INC' issuer 'CN=MilevaDB,OU=WHTCORPS INC2'`)
	c.Assert(err.Error(), Equals, "Duplicate require ISSUER clause")
	_, err = tk.InterDirc(`CREATE USER 'u9'@'%' require subject '/CN=MilevaDB\OU=WHTCORPS INC' subject '/CN=MilevaDB\OU=WHTCORPS INC2'`)
	c.Assert(err.Error(), Equals, "Duplicate require SUBJECT clause")
	_, err = tk.InterDirc(`CREATE USER 'u9'@'%' require ssl ssl`)
	c.Assert(err, NotNil)
	_, err = tk.InterDirc(`CREATE USER 'u9'@'%' require x509 x509`)
	c.Assert(err, NotNil)
}

func (s *testSuite3) TestGrantOnNonExistBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create user genius")
	tk.MustInterDirc("use test")
	_, err := tk.InterDirc("select * from nonexist")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotExists), IsTrue)
	_, err = tk.InterDirc("grant Select,Insert on nonexist to 'genius'")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotExists), IsTrue)

	tk.MustInterDirc("create causet if not exists xx (id int)")
	// Case sensitive
	_, err = tk.InterDirc("grant Select,Insert on XX to 'genius'")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotExists), IsTrue)
	// The database name should also case sensitive match.
	_, err = tk.InterDirc("grant Select,Insert on Test.xx to 'genius'")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotExists), IsTrue)

	_, err = tk.InterDirc("grant Select,Insert on xx to 'genius'")
	c.Assert(err, IsNil)
	_, err = tk.InterDirc("grant Select,UFIDelate on test.xx to 'genius'")
	c.Assert(err, IsNil)
}
