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
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causet"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/dbs"
	dbsutil "github.com/whtcorpsinc/milevadb/dbs/soliton"
	dbssolitonutil "github.com/whtcorpsinc/milevadb/dbs/solitonutil"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *testSuite6) TestTruncateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`drop causet if exists truncate_test;`)
	tk.MustInterDirc(`create causet truncate_test (a int)`)
	tk.MustInterDirc(`insert truncate_test values (1),(2),(3)`)
	result := tk.MustQuery("select * from truncate_test")
	result.Check(testkit.Events("1", "2", "3"))
	tk.MustInterDirc("truncate causet truncate_test")
	result = tk.MustQuery("select * from truncate_test")
	result.Check(nil)
}

// TestInTxnInterDircDBSFail tests the following case:
//  1. InterDircute the ALLEGROALLEGROSQL of "begin";
//  2. A ALLEGROALLEGROSQL that will fail to execute;
//  3. InterDircute DBS.
func (s *testSuite6) TestInTxnInterDircDBSFail(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t (i int key);")
	tk.MustInterDirc("insert into t values (1);")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t values (1);")
	_, err := tk.InterDirc("truncate causet t;")
	c.Assert(err.Error(), Equals, "[ekv:1062]Duplicate entry '1' for key 'PRIMARY'")
	result := tk.MustQuery("select count(*) from t")
	result.Check(testkit.Events("1"))
}

func (s *testSuite6) TestCreateBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// Test create an exist database
	_, err := tk.InterDirc("CREATE database test")
	c.Assert(err, NotNil)

	// Test create an exist causet
	tk.MustInterDirc("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	_, err = tk.InterDirc("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	c.Assert(err, NotNil)

	// Test "if not exist"
	tk.MustInterDirc("CREATE TABLE if not exists test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// Testcase for https://github.com/whtcorpsinc/milevadb/issues/312
	tk.MustInterDirc(`create causet issue312_1 (c float(24));`)
	tk.MustInterDirc(`create causet issue312_2 (c float(25));`)
	rs, err := tk.InterDirc(`desc issue312_1`)
	c.Assert(err, IsNil)
	ctx := context.Background()
	req := rs.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		c.Assert(err1, IsNil)
		if req.NumEvents() == 0 {
			break
		}
		for event := it.Begin(); event != it.End(); event = it.Next() {
			c.Assert(event.GetString(1), Equals, "float")
		}
	}
	rs, err = tk.InterDirc(`desc issue312_2`)
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	it = chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		c.Assert(err1, IsNil)
		if req.NumEvents() == 0 {
			break
		}
		for event := it.Begin(); event != it.End(); event = it.Next() {
			c.Assert(req.GetEvent(0).GetString(1), Equals, "double")
		}
	}

	// test multiple defCauslate specified in defCausumn when create.
	tk.MustInterDirc("drop causet if exists test_multiple_defCausumn_defCauslate;")
	tk.MustInterDirc("create causet test_multiple_defCausumn_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	t, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("test_multiple_defCausumn_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8_general_ci")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	tk.MustInterDirc("drop causet if exists test_multiple_defCausumn_defCauslate;")
	tk.MustInterDirc("create causet test_multiple_defCausumn_defCauslate (a char(1) charset utf8 defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("test_multiple_defCausumn_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8_general_ci")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	// test Err case for multiple defCauslate specified in defCausumn when create.
	tk.MustInterDirc("drop causet if exists test_err_multiple_defCauslate;")
	_, err = tk.InterDirc("create causet test_err_multiple_defCauslate (a char(1) charset utf8mb4 defCauslate utf8_unicode_ci defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8_unicode_ci", "utf8mb4").Error())

	tk.MustInterDirc("drop causet if exists test_err_multiple_defCauslate;")
	_, err = tk.InterDirc("create causet test_err_multiple_defCauslate (a char(1) defCauslate utf8_unicode_ci defCauslate utf8mb4_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8mb4_general_ci", "utf8").Error())

	// causet option is auto-increment
	tk.MustInterDirc("drop causet if exists create_auto_increment_test;")
	tk.MustInterDirc("create causet create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 999;")
	tk.MustInterDirc("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustInterDirc("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustInterDirc("insert into create_auto_increment_test (name) values ('cc')")
	r := tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Events("999 aa", "1000 bb", "1001 cc"))
	tk.MustInterDirc("drop causet create_auto_increment_test")
	tk.MustInterDirc("create causet create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 1999;")
	tk.MustInterDirc("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustInterDirc("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustInterDirc("insert into create_auto_increment_test (name) values ('cc')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Events("1999 aa", "2000 bb", "2001 cc"))
	tk.MustInterDirc("drop causet create_auto_increment_test")
	tk.MustInterDirc("create causet create_auto_increment_test (id int not null auto_increment, name varchar(255), key(id)) auto_increment = 1000;")
	tk.MustInterDirc("insert into create_auto_increment_test (name) values ('aa')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Events("1000 aa"))

	// Test for `drop causet if exists`.
	tk.MustInterDirc("drop causet if exists t_if_exists;")
	tk.MustQuery("show warnings;").Check(testkit.Events("Note 1051 Unknown causet 'test.t_if_exists'"))
	tk.MustInterDirc("create causet if not exists t1_if_exists(c int)")
	tk.MustInterDirc("drop causet if exists t1_if_exists,t2_if_exists,t3_if_exists")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|1051|Unknown causet 'test.t2_if_exists'", "Note|1051|Unknown causet 'test.t3_if_exists'"))
}

func (s *testSuite6) TestCreateView(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	//create an source causet
	tk.MustInterDirc("CREATE TABLE source_block (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	//test create a exist view
	tk.MustInterDirc("CREATE VIEW view_t AS select id , name from source_block")
	defer tk.MustInterDirc("DROP VIEW IF EXISTS view_t")
	_, err := tk.InterDirc("CREATE VIEW view_t AS select id , name from source_block")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1050]Block 'test.view_t' already exists")
	//create view on nonexistent causet
	_, err = tk.InterDirc("create view v1 (c,d) as select a,b from t1")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.t1' doesn't exist")
	//simple view
	tk.MustInterDirc("create causet t1 (a int ,b int)")
	tk.MustInterDirc("insert into t1 values (1,2), (1,3), (2,4), (2,5), (3,10)")
	//view with defCausList and SelectFieldExpr
	tk.MustInterDirc("create view v1 (c) as select b+1 from t1")
	//view with SelectFieldExpr
	tk.MustInterDirc("create view v2 as select b+1 from t1")
	//view with SelectFieldExpr and AsName
	tk.MustInterDirc("create view v3 as select b+1 as c from t1")
	//view with defCausList , SelectField and AsName
	tk.MustInterDirc("create view v4 (c) as select b+1 as d from t1")
	//view with select wild card
	tk.MustInterDirc("create view v5 as select * from t1")
	tk.MustInterDirc("create view v6 (c,d) as select * from t1")
	_, err = tk.InterDirc("create view v7 (c,d,e) as select * from t1")
	c.Assert(err.Error(), Equals, dbs.ErrViewWrongList.Error())
	//drop multiple views in a memex
	tk.MustInterDirc("drop view v1,v2,v3,v4,v5,v6")
	//view with variable
	tk.MustInterDirc("create view v1 (c,d) as select a,b+@@global.max_user_connections from t1")
	_, err = tk.InterDirc("create view v1 (c,d) as select a,b from t1 where a = @@global.max_user_connections")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1050]Block 'test.v1' already exists")
	tk.MustInterDirc("drop view v1")
	//view with different defCaus counts
	_, err = tk.InterDirc("create view v1 (c,d,e) as select a,b from t1 ")
	c.Assert(err.Error(), Equals, dbs.ErrViewWrongList.Error())
	_, err = tk.InterDirc("create view v1 (c) as select a,b from t1 ")
	c.Assert(err.Error(), Equals, dbs.ErrViewWrongList.Error())
	//view with or_replace flag
	tk.MustInterDirc("drop view if exists v1")
	tk.MustInterDirc("create view v1 (c,d) as select a,b from t1")
	tk.MustInterDirc("create or replace view v1 (c,d) as select a,b from t1 ")
	tk.MustInterDirc("create causet if not exists t1 (a int ,b int)")
	_, err = tk.InterDirc("create or replace view t1 as select * from t1")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "t1", "VIEW").Error())
	// create view using prepare
	tk.MustInterDirc(`prepare stmt from "create view v10 (x) as select 1";`)
	tk.MustInterDirc("execute stmt")

	// create view on union
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("drop view if exists v")
	_, err = tk.InterDirc("create view v as select * from t1 union select * from t2")
	c.Assert(terror.ErrorEqual(err, schemareplicant.ErrBlockNotExists), IsTrue)
	tk.MustInterDirc("create causet t1(a int, b int)")
	tk.MustInterDirc("create causet t2(a int, b int)")
	tk.MustInterDirc("insert into t1 values(1,2), (1,1), (1,2)")
	tk.MustInterDirc("insert into t2 values(1,1),(1,3)")
	tk.MustInterDirc("create definer='root'@'localhost' view v as select * from t1 union select * from t2")
	tk.MustQuery("select * from v").Sort().Check(testkit.Events("1 1", "1 2", "1 3"))
	tk.MustInterDirc("alter causet t1 drop defCausumn a")
	_, err = tk.InterDirc("select * from v")
	c.Assert(terror.ErrorEqual(err, causetembedded.ErrViewInvalid), IsTrue)
	tk.MustInterDirc("alter causet t1 add defCausumn a int")
	tk.MustQuery("select * from v").Sort().Check(testkit.Events("1 1", "1 3", "<nil> 1", "<nil> 2"))
	tk.MustInterDirc("alter causet t1 drop defCausumn a")
	tk.MustInterDirc("alter causet t2 drop defCausumn b")
	_, err = tk.InterDirc("select * from v")
	c.Assert(terror.ErrorEqual(err, causetembedded.ErrViewInvalid), IsTrue)
	tk.MustInterDirc("drop view v")

	tk.MustInterDirc("create view v as (select * from t1)")
	tk.MustInterDirc("drop view v")
	tk.MustInterDirc("create view v as (select * from t1 union select * from t2)")
	tk.MustInterDirc("drop view v")

	// Test for `drop view if exists`.
	tk.MustInterDirc("drop view if exists v_if_exists;")
	tk.MustQuery("show warnings;").Check(testkit.Events("Note 1051 Unknown causet 'test.v_if_exists'"))
	tk.MustInterDirc("create view v1_if_exists as (select * from t1)")
	tk.MustInterDirc("drop view if exists v1_if_exists,v2_if_exists,v3_if_exists")
	tk.MustQuery("show warnings").Check(solitonutil.EventsWithSep("|", "Note|1051|Unknown causet 'test.v2_if_exists'", "Note|1051|Unknown causet 'test.v3_if_exists'"))

	// Test for create nested view.
	tk.MustInterDirc("create causet test_v_nested(a int)")
	tk.MustInterDirc("create definer='root'@'localhost' view v_nested as select * from test_v_nested")
	tk.MustInterDirc("create definer='root'@'localhost' view v_nested2 as select * from v_nested")
	_, err = tk.InterDirc("create or replace definer='root'@'localhost' view v_nested as select * from v_nested2")
	c.Assert(terror.ErrorEqual(err, causetembedded.ErrNoSuchBlock), IsTrue)
	tk.MustInterDirc("drop causet test_v_nested")
	tk.MustInterDirc("drop view v_nested, v_nested2")
}

func (s *testSuite6) TestIssue16250(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet if not exists t(a int)")
	tk.MustInterDirc("create view view_issue16250 as select * from t")
	_, err := tk.InterDirc("truncate causet view_issue16250")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.view_issue16250' doesn't exist")
}

func (s testSuite6) TestTruncateSequence(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create sequence if not exists seq")
	_, err := tk.InterDirc("truncate causet seq")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.seq' doesn't exist")
	tk.MustInterDirc("create sequence if not exists seq1 start 10 increment 2 maxvalue 10000 cycle")
	_, err = tk.InterDirc("truncate causet seq1")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.seq1' doesn't exist")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop sequence if exists seq1")
}

func (s *testSuite6) TestCreateViewWithOverlongDefCausName(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(a int)")
	defer tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("create view v as select distinct'" + strings.Repeat("a", 65) + "', " +
		"max('" + strings.Repeat("b", 65) + "'), " +
		"'cccccccccc', '" + strings.Repeat("d", 65) + "';")
	resultCreateStmt := "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v` (`name_exp_1`, `name_exp_2`, `cccccccccc`, `name_exp_4`) AS SELECT DISTINCT '" + strings.Repeat("a", 65) + "',MAX('" + strings.Repeat("b", 65) + "'),'cccccccccc','" + strings.Repeat("d", 65) + "'"
	tk.MustQuery("select * from v")
	tk.MustQuery("select name_exp_1, name_exp_2, cccccccccc, name_exp_4 from v")
	tk.MustQuery("show create view v").Check(testkit.Events("v " + resultCreateStmt + "  "))
	tk.MustInterDirc("drop view v;")
	tk.MustInterDirc(resultCreateStmt)

	tk.MustInterDirc("drop view v ")
	tk.MustInterDirc("create definer='root'@'localhost' view v as select 'a', '" + strings.Repeat("b", 65) + "' from t " +
		"union select '" + strings.Repeat("c", 65) + "', " +
		"count(distinct '" + strings.Repeat("b", 65) + "', " +
		"'c');")
	resultCreateStmt = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v` (`a`, `name_exp_2`) AS SELECT 'a','" + strings.Repeat("b", 65) + "' FROM `test`.`t` UNION SELECT '" + strings.Repeat("c", 65) + "',COUNT(DISTINCT '" + strings.Repeat("b", 65) + "', 'c')"
	tk.MustQuery("select * from v")
	tk.MustQuery("select a, name_exp_2 from v")
	tk.MustQuery("show create view v").Check(testkit.Events("v " + resultCreateStmt + "  "))
	tk.MustInterDirc("drop view v;")
	tk.MustInterDirc(resultCreateStmt)

	tk.MustInterDirc("drop view v ")
	tk.MustInterDirc("create definer='root'@'localhost' view v as select 'a' as '" + strings.Repeat("b", 65) + "' from t;")
	tk.MustQuery("select * from v")
	tk.MustQuery("select name_exp_1 from v")
	resultCreateStmt = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` ALLEGROALLEGROSQL SECURITY DEFINER VIEW `v` (`name_exp_1`) AS SELECT 'a' AS `" + strings.Repeat("b", 65) + "` FROM `test`.`t`"
	tk.MustQuery("show create view v").Check(testkit.Events("v " + resultCreateStmt + "  "))
	tk.MustInterDirc("drop view v;")
	tk.MustInterDirc(resultCreateStmt)

	tk.MustInterDirc("drop view v ")
	err := tk.InterDircToErr("create view v(`" + strings.Repeat("b", 65) + "`) as select a from t;")
	c.Assert(err.Error(), Equals, "[dbs:1059]Identifier name 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' is too long")
}

func (s *testSuite6) TestCreateDroFIDelatabase(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("create database if not exists drop_test;")
	tk.MustInterDirc("drop database if exists drop_test;")
	tk.MustInterDirc("create database drop_test;")
	tk.MustInterDirc("use drop_test;")
	tk.MustInterDirc("drop database drop_test;")
	_, err := tk.InterDirc("drop causet t;")
	c.Assert(err.Error(), Equals, causetembedded.ErrNoDB.Error())
	err = tk.InterDircToErr("select * from t;")
	c.Assert(err.Error(), Equals, causetembedded.ErrNoDB.Error())

	_, err = tk.InterDirc("drop database allegrosql")
	c.Assert(err, NotNil)

	tk.MustInterDirc("create database charset_test charset ascii;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET ascii */",
	))
	tk.MustInterDirc("drop database charset_test;")
	tk.MustInterDirc("create database charset_test charset binary;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET binary */",
	))
	tk.MustInterDirc("drop database charset_test;")
	tk.MustInterDirc("create database charset_test defCauslate utf8_general_ci;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci */",
	))
	tk.MustInterDirc("drop database charset_test;")
	tk.MustInterDirc("create database charset_test charset utf8 defCauslate utf8_general_ci;")
	tk.MustQuery("show create database charset_test;").Check(solitonutil.EventsWithSep("|",
		"charset_test|CREATE DATABASE `charset_test` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci */",
	))
	tk.MustGetErrMsg("create database charset_test charset utf8 defCauslate utf8mb4_unicode_ci;", "[dbs:1253]COLLATION 'utf8mb4_unicode_ci' is not valid for CHARACTER SET 'utf8'")
}

func (s *testSuite6) TestCreateDropBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet if not exists drop_test (a int)")
	tk.MustInterDirc("drop causet if exists drop_test")
	tk.MustInterDirc("create causet drop_test (a int)")
	tk.MustInterDirc("drop causet drop_test")

	_, err := tk.InterDirc("drop causet allegrosql.gc_delete_range")
	c.Assert(err, NotNil)
}

func (s *testSuite6) TestCreateDropView(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create or replace view drop_test as select 1,2")

	_, err := tk.InterDirc("drop causet drop_test")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1051]Unknown causet 'test.drop_test'")

	_, err = tk.InterDirc("drop view if exists drop_test")
	c.Assert(err, IsNil)

	_, err = tk.InterDirc("drop view allegrosql.gc_delete_range")
	c.Assert(err.Error(), Equals, "Drop milevadb system causet 'allegrosql.gc_delete_range' is forbidden")

	_, err = tk.InterDirc("drop view drop_test")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1051]Unknown causet 'test.drop_test'")

	tk.MustInterDirc("create causet t_v(a int)")
	_, err = tk.InterDirc("drop view t_v")
	c.Assert(err.Error(), Equals, "[dbs:1347]'test.t_v' is not VIEW")

	tk.MustInterDirc("create causet t_v1(a int, b int);")
	tk.MustInterDirc("create causet t_v2(a int, b int);")
	tk.MustInterDirc("create view v as select * from t_v1;")
	tk.MustInterDirc("create or replace view v  as select * from t_v2;")
	tk.MustQuery("select * from information_schema.views where block_name ='v';").Check(
		testkit.Events("def test v SELECT `test`.`t_v2`.`a`,`test`.`t_v2`.`b` FROM `test`.`t_v2` CASCADED NO @ DEFINER utf8mb4 utf8mb4_bin"))
}

func (s *testSuite6) TestCreateDropIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet if not exists drop_test (a int)")
	tk.MustInterDirc("create index idx_a on drop_test (a)")
	tk.MustInterDirc("drop index idx_a on drop_test")
	tk.MustInterDirc("drop causet drop_test")
}

func (s *testSuite6) TestAlterBlockAddDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet if not exists alter_test (c1 int)")
	tk.MustInterDirc("insert into alter_test values(1)")
	tk.MustInterDirc("alter causet alter_test add defCausumn c2 timestamp default current_timestamp")
	time.Sleep(1 * time.Millisecond)
	now := time.Now().Add(-1 * time.Millisecond).Format(types.TimeFormat)
	r, err := tk.InterDirc("select c2 from alter_test")
	c.Assert(err, IsNil)
	req := r.NewChunk()
	err = r.Next(context.Background(), req)
	c.Assert(err, IsNil)
	event := req.GetEvent(0)
	c.Assert(event.Len(), Equals, 1)
	c.Assert(now, GreaterEqual, event.GetTime(0).String())
	r.Close()
	tk.MustInterDirc("alter causet alter_test add defCausumn c3 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustQuery("select c3 from alter_test").Check(testkit.Events("CURRENT_TIMESTAMP"))
	tk.MustInterDirc("create or replace view alter_view as select c1,c2 from alter_test")
	_, err = tk.InterDirc("alter causet alter_view add defCausumn c4 varchar(50)")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustInterDirc("drop view alter_view")
	tk.MustInterDirc("create sequence alter_seq")
	_, err = tk.InterDirc("alter causet alter_seq add defCausumn c int")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error())
	tk.MustInterDirc("drop sequence alter_seq")
}

func (s *testSuite6) TestAlterBlockAddDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet if not exists alter_test (c1 int)")
	tk.MustInterDirc("insert into alter_test values(1)")
	tk.MustInterDirc("alter causet alter_test add defCausumn c2 timestamp default current_timestamp, add defCausumn c8 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustInterDirc("alter causet alter_test add defCausumn (c7 timestamp default current_timestamp, c3 varchar(50) default 'CURRENT_TIMESTAMP')")
	r, err := tk.InterDirc("select c2 from alter_test")
	c.Assert(err, IsNil)
	req := r.NewChunk()
	err = r.Next(context.Background(), req)
	c.Assert(err, IsNil)
	event := req.GetEvent(0)
	c.Assert(event.Len(), Equals, 1)
	r.Close()
	tk.MustQuery("select c3 from alter_test").Check(testkit.Events("CURRENT_TIMESTAMP"))
	tk.MustInterDirc("create or replace view alter_view as select c1,c2 from alter_test")
	_, err = tk.InterDirc("alter causet alter_view add defCausumn (c4 varchar(50), c5 varchar(50))")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustInterDirc("drop view alter_view")
	tk.MustInterDirc("create sequence alter_seq")
	_, err = tk.InterDirc("alter causet alter_seq add defCausumn (c1 int, c2 varchar(10))")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error())
	tk.MustInterDirc("drop sequence alter_seq")
}

func (s *testSuite6) TestAddNotNullDeferredCausetNoDefault(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet nn (c1 int)")
	tk.MustInterDirc("insert nn values (1), (2)")
	tk.MustInterDirc("alter causet nn add defCausumn c2 int not null")

	tbl, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("nn"))
	c.Assert(err, IsNil)
	defCaus2 := tbl.Meta().DeferredCausets[1]
	c.Assert(defCaus2.DefaultValue, IsNil)
	c.Assert(defCaus2.OriginDefaultValue, Equals, "0")

	tk.MustQuery("select * from nn").Check(testkit.Events("1 0", "2 0"))
	_, err = tk.InterDirc("insert nn (c1) values (3)")
	c.Check(err, NotNil)
	tk.MustInterDirc("set sql_mode=''")
	tk.MustInterDirc("insert nn (c1) values (3)")
	tk.MustQuery("select * from nn").Check(testkit.Events("1 0", "2 0", "3 0"))
}

func (s *testSuite6) TestAlterBlockModifyDeferredCauset(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists mc")
	tk.MustInterDirc("create causet mc(c1 int, c2 varchar(10), c3 bit)")
	_, err := tk.InterDirc("alter causet mc modify defCausumn c1 short")
	c.Assert(err, NotNil)
	tk.MustInterDirc("alter causet mc modify defCausumn c1 bigint")

	_, err = tk.InterDirc("alter causet mc modify defCausumn c2 blob")
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("alter causet mc modify defCausumn c2 varchar(8)")
	c.Assert(err, NotNil)
	tk.MustInterDirc("alter causet mc modify defCausumn c2 varchar(11)")
	tk.MustInterDirc("alter causet mc modify defCausumn c2 text(13)")
	tk.MustInterDirc("alter causet mc modify defCausumn c2 text")
	tk.MustInterDirc("alter causet mc modify defCausumn c3 bit")
	result := tk.MustQuery("show create causet mc")
	createALLEGROSQL := result.Events()[0][1]
	expected := "CREATE TABLE `mc` (\n  `c1` bigint(20) DEFAULT NULL,\n  `c2` text DEFAULT NULL,\n  `c3` bit(1) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createALLEGROSQL, Equals, expected)
	tk.MustInterDirc("create or replace view alter_view as select c1,c2 from mc")
	_, err = tk.InterDirc("alter causet alter_view modify defCausumn c2 text")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustInterDirc("drop view alter_view")
	tk.MustInterDirc("create sequence alter_seq")
	_, err = tk.InterDirc("alter causet alter_seq modify defCausumn c int")
	c.Assert(err.Error(), Equals, dbs.ErrWrongObject.GenWithStackByArgs("test", "alter_seq", "BASE TABLE").Error())
	tk.MustInterDirc("drop sequence alter_seq")

	// test multiple defCauslate modification in defCausumn.
	tk.MustInterDirc("drop causet if exists modify_defCausumn_multiple_defCauslate")
	tk.MustInterDirc("create causet modify_defCausumn_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.InterDirc("alter causet modify_defCausumn_multiple_defCauslate modify defCausumn a char(1) defCauslate utf8mb4_bin;")
	c.Assert(err, IsNil)
	t, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("modify_defCausumn_multiple_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8mb4")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8mb4_bin")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	tk.MustInterDirc("drop causet if exists modify_defCausumn_multiple_defCauslate;")
	tk.MustInterDirc("create causet modify_defCausumn_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.InterDirc("alter causet modify_defCausumn_multiple_defCauslate modify defCausumn a char(1) charset utf8mb4 defCauslate utf8mb4_bin;")
	c.Assert(err, IsNil)
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("modify_defCausumn_multiple_defCauslate"))
	c.Assert(err, IsNil)
	c.Assert(t.DefCauss()[0].Charset, Equals, "utf8mb4")
	c.Assert(t.DefCauss()[0].DefCauslate, Equals, "utf8mb4_bin")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().DefCauslate, Equals, "utf8mb4_bin")

	// test Err case for multiple defCauslate modification in defCausumn.
	tk.MustInterDirc("drop causet if exists err_modify_multiple_defCauslate;")
	tk.MustInterDirc("create causet err_modify_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.InterDirc("alter causet err_modify_multiple_defCauslate modify defCausumn a char(1) charset utf8mb4 defCauslate utf8_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8_bin", "utf8mb4").Error())

	tk.MustInterDirc("drop causet if exists err_modify_multiple_defCauslate;")
	tk.MustInterDirc("create causet err_modify_multiple_defCauslate (a char(1) defCauslate utf8_bin defCauslate utf8_general_ci) charset utf8mb4 defCauslate utf8mb4_bin")
	_, err = tk.InterDirc("alter causet err_modify_multiple_defCauslate modify defCausumn a char(1) defCauslate utf8_bin defCauslate utf8mb4_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, dbs.ErrDefCauslationCharsetMismatch.GenWithStackByArgs("utf8mb4_bin", "utf8").Error())

}

func (s *testSuite6) TestDefaultDBAfterDropCurDB(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	testALLEGROSQL := `create database if not exists test_db CHARACTER SET latin1 COLLATE latin1_swedish_ci;`
	tk.MustInterDirc(testALLEGROSQL)

	testALLEGROSQL = `use test_db;`
	tk.MustInterDirc(testALLEGROSQL)
	tk.MustQuery(`select database();`).Check(testkit.Events("test_db"))
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Events("latin1"))
	tk.MustQuery(`select @@defCauslation_database;`).Check(testkit.Events("latin1_swedish_ci"))

	testALLEGROSQL = `drop database test_db;`
	tk.MustInterDirc(testALLEGROSQL)
	tk.MustQuery(`select database();`).Check(testkit.Events("<nil>"))
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Events(allegrosql.DefaultCharset))
	tk.MustQuery(`select @@defCauslation_database;`).Check(testkit.Events(allegrosql.DefaultDefCauslationName))
}

func (s *testSuite6) TestDeferredCausetCharsetAndDefCauslate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	dbName := "defCaus_charset_defCauslate"
	tk.MustInterDirc("create database " + dbName)
	tk.MustInterDirc("use " + dbName)
	tests := []struct {
		defCausType     string
		charset         string
		defCauslates    string
		exptCharset     string
		exptDefCauslate string
		errMsg          string
	}{
		{
			defCausType:     "varchar(10)",
			charset:         "charset utf8",
			defCauslates:    "defCauslate utf8_bin",
			exptCharset:     "utf8",
			exptDefCauslate: "utf8_bin",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset utf8mb4",
			defCauslates:    "",
			exptCharset:     "utf8mb4",
			exptDefCauslate: "utf8mb4_bin",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset utf16",
			defCauslates:    "",
			exptCharset:     "",
			exptDefCauslate: "",
			errMsg:          "Unknown charset utf16",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset latin1",
			defCauslates:    "",
			exptCharset:     "latin1",
			exptDefCauslate: "latin1_bin",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset binary",
			defCauslates:    "",
			exptCharset:     "binary",
			exptDefCauslate: "binary",
			errMsg:          "",
		},
		{
			defCausType:     "varchar(10)",
			charset:         "charset ascii",
			defCauslates:    "",
			exptCharset:     "ascii",
			exptDefCauslate: "ascii_bin",
			errMsg:          "",
		},
	}
	sctx := tk.Se.(stochastikctx.Context)
	dm := petri.GetPetri(sctx)
	for i, tt := range tests {
		tblName := fmt.Sprintf("t%d", i)
		allegrosql := fmt.Sprintf("create causet %s (a %s %s %s)", tblName, tt.defCausType, tt.charset, tt.defCauslates)
		if tt.errMsg == "" {
			tk.MustInterDirc(allegrosql)
			is := dm.SchemaReplicant()
			c.Assert(is, NotNil)

			tb, err := is.BlockByName(perceptron.NewCIStr(dbName), perceptron.NewCIStr(tblName))
			c.Assert(err, IsNil)
			c.Assert(tb.Meta().DeferredCausets[0].Charset, Equals, tt.exptCharset, Commentf(allegrosql))
			c.Assert(tb.Meta().DeferredCausets[0].DefCauslate, Equals, tt.exptDefCauslate, Commentf(allegrosql))
		} else {
			_, err := tk.InterDirc(allegrosql)
			c.Assert(err, NotNil, Commentf(allegrosql))
		}
	}
	tk.MustInterDirc("drop database " + dbName)
}

func (s *testSuite6) TestTooLargeIdentifierLength(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	// for database.
	dbName1, dbName2 := strings.Repeat("a", allegrosql.MaxDatabaseNameLength), strings.Repeat("a", allegrosql.MaxDatabaseNameLength+1)
	tk.MustInterDirc(fmt.Sprintf("create database %s", dbName1))
	tk.MustInterDirc(fmt.Sprintf("drop database %s", dbName1))
	_, err := tk.InterDirc(fmt.Sprintf("create database %s", dbName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", dbName2))

	// for causet.
	tk.MustInterDirc("use test")
	blockName1, blockName2 := strings.Repeat("b", allegrosql.MaxBlockNameLength), strings.Repeat("b", allegrosql.MaxBlockNameLength+1)
	tk.MustInterDirc(fmt.Sprintf("create causet %s(c int)", blockName1))
	tk.MustInterDirc(fmt.Sprintf("drop causet %s", blockName1))
	_, err = tk.InterDirc(fmt.Sprintf("create causet %s(c int)", blockName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", blockName2))

	// for defCausumn.
	tk.MustInterDirc("drop causet if exists t;")
	defCausumnName1, defCausumnName2 := strings.Repeat("c", allegrosql.MaxDeferredCausetNameLength), strings.Repeat("c", allegrosql.MaxDeferredCausetNameLength+1)
	tk.MustInterDirc(fmt.Sprintf("create causet t(%s int)", defCausumnName1))
	tk.MustInterDirc("drop causet t")
	_, err = tk.InterDirc(fmt.Sprintf("create causet t(%s int)", defCausumnName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", defCausumnName2))

	// for index.
	tk.MustInterDirc("create causet t(c int);")
	indexName1, indexName2 := strings.Repeat("d", allegrosql.MaxIndexIdentifierLen), strings.Repeat("d", allegrosql.MaxIndexIdentifierLen+1)
	tk.MustInterDirc(fmt.Sprintf("create index %s on t(c)", indexName1))
	tk.MustInterDirc(fmt.Sprintf("drop index %s on t", indexName1))
	_, err = tk.InterDirc(fmt.Sprintf("create index %s on t(c)", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", indexName2))

	// for create causet with index.
	tk.MustInterDirc("drop causet t;")
	_, err = tk.InterDirc(fmt.Sprintf("create causet t(c int, index %s(c));", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[dbs:1059]Identifier name '%s' is too long", indexName2))
}

func (s *testSuite8) TestShardEventIDBits(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t (a int) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into t values (?)", i)
	}

	dom := petri.GetPetri(tk.Se)
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)

	assertCountAndShard := func(t causet.Block, expectCount int) {
		var hasShardedID bool
		var count int
		c.Assert(tk.Se.NewTxn(context.Background()), IsNil)
		err = t.IterRecords(tk.Se, t.FirstKey(), nil, func(h ekv.Handle, rec []types.Causet, defcaus []*causet.DeferredCauset) (more bool, err error) {
			c.Assert(h.IntValue(), GreaterEqual, int64(0))
			first8bits := h.IntValue() >> 56
			if first8bits > 0 {
				hasShardedID = true
			}
			count++
			return true, nil
		})
		c.Assert(err, IsNil)
		c.Assert(count, Equals, expectCount)
		c.Assert(hasShardedID, IsTrue)
	}

	assertCountAndShard(tbl, 100)

	// After PR 10759, shard_row_id_bits is supported with blocks with auto_increment defCausumn.
	tk.MustInterDirc("create causet auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustInterDirc("alter causet auto shard_row_id_bits = 5")
	tk.MustInterDirc("drop causet auto")
	tk.MustInterDirc("create causet auto (id int not null auto_increment unique) shard_row_id_bits = 0")
	tk.MustInterDirc("alter causet auto shard_row_id_bits = 5")
	tk.MustInterDirc("drop causet auto")
	tk.MustInterDirc("create causet auto (id int not null auto_increment unique)")
	tk.MustInterDirc("alter causet auto shard_row_id_bits = 5")
	tk.MustInterDirc("drop causet auto")
	tk.MustInterDirc("create causet auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustInterDirc("alter causet auto shard_row_id_bits = 0")
	tk.MustInterDirc("drop causet auto")

	// After PR 10759, shard_row_id_bits is not supported with pk_is_handle blocks.
	err = tk.InterDircToErr("create causet auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 4")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported shard_row_id_bits for causet with primary key as event id")
	tk.MustInterDirc("create causet auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 0")
	err = tk.InterDircToErr("alter causet auto shard_row_id_bits = 5")
	c.Assert(err.Error(), Equals, "[dbs:8200]Unsupported shard_row_id_bits for causet with primary key as event id")
	tk.MustInterDirc("alter causet auto shard_row_id_bits = 0")

	// Hack an existing causet with shard_row_id_bits and primary key as handle
	EDB, ok := dom.SchemaReplicant().SchemaByName(perceptron.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	tbl, err = dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("auto"))
	tblInfo := tbl.Meta()
	tblInfo.ShardEventIDBits = 5
	tblInfo.MaxShardEventIDBits = 5

	ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
		m := spacetime.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UFIDelateBlock(EDB.ID, tblInfo), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustInterDirc("insert auto(b) values (1), (3), (5)")
	tk.MustQuery("select id from auto order by id").Check(testkit.Events("1", "2", "3"))

	tk.MustInterDirc("alter causet auto shard_row_id_bits = 0")
	tk.MustInterDirc("drop causet auto")

	// Test shard_row_id_bits with auto_increment defCausumn
	tk.MustInterDirc("create causet auto (a int, b int auto_increment unique) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into auto(a) values (?)", i)
	}
	tbl, err = dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("auto"))
	assertCountAndShard(tbl, 100)
	prevB, err := strconv.Atoi(tk.MustQuery("select b from auto where a=0").Events()[0][0].(string))
	c.Assert(err, IsNil)
	for i := 1; i < 100; i++ {
		b, err := strconv.Atoi(tk.MustQuery(fmt.Sprintf("select b from auto where a=%d", i)).Events()[0][0].(string))
		c.Assert(err, IsNil)
		c.Assert(b, Greater, prevB)
		prevB = b
	}

	// Test overflow
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int) shard_row_id_bits = 15")
	defer tk.MustInterDirc("drop causet if exists t1")

	tbl, err = dom.SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	maxID := 1<<(64-15-1) - 1
	err = tbl.RebaseAutoID(tk.Se, int64(maxID)-1, false, autoid.EventIDAllocType)
	c.Assert(err, IsNil)
	tk.MustInterDirc("insert into t1 values(1)")

	// continue inserting will fail.
	_, err = tk.InterDirc("insert into t1 values(2)")
	c.Assert(autoid.ErrAutoincReadFailed.Equal(err), IsTrue, Commentf("err:%v", err))
	_, err = tk.InterDirc("insert into t1 values(3)")
	c.Assert(autoid.ErrAutoincReadFailed.Equal(err), IsTrue, Commentf("err:%v", err))
}

type testAutoRandomSuite struct {
	*baseTestSuite
}

func (s *testAutoRandomSuite) SetUpTest(c *C) {
	solitonutil.ConfigTestUtils.SetupAutoRandomTestConfig()
}

func (s *testAutoRandomSuite) TearDownTest(c *C) {
	solitonutil.ConfigTestUtils.RestoreAutoRandomTestConfig()
}

func (s *testAutoRandomSuite) TestAutoRandomBitsData(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("create database if not exists test_auto_random_bits")
	defer tk.MustInterDirc("drop database if exists test_auto_random_bits")
	tk.MustInterDirc("use test_auto_random_bits")
	tk.MustInterDirc("drop causet if exists t")

	extractAllHandles := func() []int64 {
		allHds, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test_auto_random_bits", "t")
		c.Assert(err, IsNil)
		return allHds
	}

	tk.MustInterDirc("set @@allow_auto_random_explicit_insert = true")

	tk.MustInterDirc("create causet t (a bigint primary key auto_random(15), b int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into t(b) values (?)", i)
	}
	allHandles := extractAllHandles()
	tk.MustInterDirc("drop causet t")

	// Test auto random id number.
	c.Assert(len(allHandles), Equals, 100)
	// Test the handles are not all zero.
	allZero := true
	for _, h := range allHandles {
		allZero = allZero && (h>>(64-16)) == 0
	}
	c.Assert(allZero, IsFalse)
	// Test non-shard-bits part of auto random id is monotonic increasing and continuous.
	orderedHandles := solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 15, allegrosql.TypeLonglong)
	size := int64(len(allHandles))
	for i := int64(1); i <= size; i++ {
		c.Assert(i, Equals, orderedHandles[i-1])
	}

	// Test explicit insert.
	autoRandBitsUpperBound := 2<<47 - 1
	tk.MustInterDirc("create causet t (a bigint primary key auto_random(15), b int)")
	for i := -10; i < 10; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert into t values(%d, %d)", i+autoRandBitsUpperBound, i))
	}
	_, err := tk.InterDirc("insert into t (b) values (0)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustInterDirc("drop causet t")

	// Test overflow.
	tk.MustInterDirc("create causet t (a bigint primary key auto_random(15), b int)")
	// Here we cannot fill the all values for a `bigint` defCausumn,
	// so firstly we rebase auto_rand to the position before overflow.
	tk.MustInterDirc(fmt.Sprintf("insert into t values (%d, %d)", autoRandBitsUpperBound, 1))
	_, err = tk.InterDirc("insert into t (b) values (0)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustInterDirc("drop causet t")

	tk.MustInterDirc("create causet t (a bigint primary key auto_random(15), b int)")
	tk.MustInterDirc("insert into t values (1, 2)")
	tk.MustInterDirc(fmt.Sprintf("uFIDelate t set a = %d where a = 1", autoRandBitsUpperBound))
	_, err = tk.InterDirc("insert into t (b) values (0)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, autoid.ErrAutoRandReadFailed.GenWithStackByArgs().Error())
	tk.MustInterDirc("drop causet t")

	// Test insert negative integers explicitly won't trigger rebase.
	tk.MustInterDirc("create causet t (a bigint primary key auto_random(15), b int)")
	for i := 1; i <= 100; i++ {
		tk.MustInterDirc("insert into t(b) values (?)", i)
		tk.MustInterDirc("insert into t(a, b) values (?, ?)", -i, i)
	}
	// orderedHandles should be [-100, -99, ..., -2, -1, 1, 2, ..., 99, 100]
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(extractAllHandles(), 15, allegrosql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < 100; i++ {
		c.Assert(orderedHandles[i], Equals, i-100)
	}
	for i := int64(100); i < size; i++ {
		c.Assert(orderedHandles[i], Equals, i-99)
	}
	tk.MustInterDirc("drop causet t")

	// Test signed/unsigned types.
	tk.MustInterDirc("create causet t (a bigint primary key auto_random(10), b int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into t (b) values(?)", i)
	}
	for _, h := range extractAllHandles() {
		// Sign bit should be reserved.
		c.Assert(h > 0, IsTrue)
	}
	tk.MustInterDirc("drop causet t")

	tk.MustInterDirc("create causet t (a bigint unsigned primary key auto_random(10), b int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into t (b) values(?)", i)
	}
	signBitUnused := true
	for _, h := range extractAllHandles() {
		signBitUnused = signBitUnused && (h > 0)
	}
	// Sign bit should be used for shard.
	c.Assert(signBitUnused, IsFalse)
	tk.MustInterDirc("drop causet t;")

	// Test rename causet does not affect incremental part of auto_random ID.
	tk.MustInterDirc("create database test_auto_random_bits_rename;")
	tk.MustInterDirc("create causet t (a bigint auto_random primary key);")
	for i := 0; i < 10; i++ {
		tk.MustInterDirc("insert into t values ();")
	}
	tk.MustInterDirc("alter causet t rename to test_auto_random_bits_rename.t1;")
	for i := 0; i < 10; i++ {
		tk.MustInterDirc("insert into test_auto_random_bits_rename.t1 values ();")
	}
	tk.MustInterDirc("alter causet test_auto_random_bits_rename.t1 rename to t;")
	for i := 0; i < 10; i++ {
		tk.MustInterDirc("insert into t values ();")
	}
	uniqueHandles := make(map[int64]struct{})
	for _, h := range extractAllHandles() {
		uniqueHandles[h&((1<<(63-5))-1)] = struct{}{}
	}
	c.Assert(len(uniqueHandles), Equals, 30)
	tk.MustInterDirc("drop database test_auto_random_bits_rename;")
	tk.MustInterDirc("drop causet t;")
}

func (s *testAutoRandomSuite) TestAutoRandomBlockOption(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	// test causet option is auto-random
	tk.MustInterDirc("drop causet if exists auto_random_block_option")
	tk.MustInterDirc("create causet auto_random_block_option (a bigint auto_random(5) key) auto_random_base = 1000")
	t, err := petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("auto_random_block_option"))
	c.Assert(err, IsNil)
	c.Assert(t.Meta().AutoRandID, Equals, int64(1000))
	tk.MustInterDirc("insert into auto_random_block_option values (),(),(),(),()")
	allHandles, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "auto_random_block_option")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 5)
	// Test non-shard-bits part of auto random id is monotonic increasing and continuous.
	orderedHandles := solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	size := int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		c.Assert(i+1000, Equals, orderedHandles[i])
	}

	tk.MustInterDirc("drop causet if exists alter_block_auto_random_option")
	tk.MustInterDirc("create causet alter_block_auto_random_option (a bigint primary key auto_random(4), b int)")
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("alter_block_auto_random_option"))
	c.Assert(err, IsNil)
	c.Assert(t.Meta().AutoRandID, Equals, int64(0))
	tk.MustInterDirc("insert into alter_block_auto_random_option values(),(),(),(),()")
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "alter_block_auto_random_option")
	c.Assert(err, IsNil)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		c.Assert(orderedHandles[i], Equals, i+1)
	}
	tk.MustInterDirc("delete from alter_block_auto_random_option")

	// alter causet to change the auto_random option (it will dismiss the local allocator cache)
	// To avoid the new base is in the range of local cache, which will leading the next
	// value is not what we rebased, because the local cache is dropped, here we choose
	// a quite big value to do this.
	tk.MustInterDirc("alter causet alter_block_auto_random_option auto_random_base = 3000000")
	t, err = petri.GetPetri(tk.Se).SchemaReplicant().BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("alter_block_auto_random_option"))
	c.Assert(err, IsNil)
	c.Assert(t.Meta().AutoRandID, Equals, int64(3000000))
	tk.MustInterDirc("insert into alter_block_auto_random_option values(),(),(),(),()")
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "alter_block_auto_random_option")
	c.Assert(err, IsNil)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	size = int64(len(allHandles))
	for i := int64(0); i < size; i++ {
		c.Assert(orderedHandles[i], Equals, i+3000000)
	}
	tk.MustInterDirc("drop causet alter_block_auto_random_option")

	// Alter auto_random_base on non auto_random causet.
	tk.MustInterDirc("create causet alter_auto_random_normal (a int)")
	_, err = tk.InterDirc("alter causet alter_auto_random_normal auto_random_base = 100")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), autoid.AutoRandomRebaseNotApplicable), IsTrue, Commentf(err.Error()))
}

// Test filter different HoTT of allocators.
// In special dbs type, for example:
// 1: CausetActionRenameBlock             : it will abandon all the old allocators.
// 2: CausetActionRebaseAutoID            : it will drop event-id-type allocator.
// 3: CausetActionModifyBlockAutoIdCache  : it will drop event-id-type allocator.
// 3: CausetActionRebaseAutoRandomBase    : it will drop auto-rand-type allocator.
func (s *testAutoRandomSuite) TestFilterDifferentSlabPredictors(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")

	tk.MustInterDirc("create causet t(a bigint auto_random(5) key, b int auto_increment unique)")
	tk.MustInterDirc("insert into t values()")
	tk.MustQuery("select b from t").Check(testkit.Events("1"))
	allHandles, err := dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles := solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Equals, int64(1))
	tk.MustInterDirc("delete from t")

	// Test rebase auto_increment.
	tk.MustInterDirc("alter causet t auto_increment 3000000")
	tk.MustInterDirc("insert into t values()")
	tk.MustQuery("select b from t").Check(testkit.Events("3000000"))
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Equals, int64(2))
	tk.MustInterDirc("delete from t")

	// Test rebase auto_random.
	tk.MustInterDirc("alter causet t auto_random_base 3000000")
	tk.MustInterDirc("insert into t values()")
	tk.MustQuery("select b from t").Check(testkit.Events("3000001"))
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Equals, int64(3000000))
	tk.MustInterDirc("delete from t")

	// Test rename causet.
	tk.MustInterDirc("rename causet t to t1")
	tk.MustInterDirc("insert into t1 values()")
	res := tk.MustQuery("select b from t1")
	strInt64, err := strconv.ParseInt(res.Events()[0][0].(string), 10, 64)
	c.Assert(err, IsNil)
	c.Assert(strInt64, Greater, int64(3000002))
	allHandles, err = dbssolitonutil.ExtractAllBlockHandles(tk.Se, "test", "t1")
	c.Assert(err, IsNil)
	c.Assert(len(allHandles), Equals, 1)
	orderedHandles = solitonutil.ConfigTestUtils.MaskSortHandles(allHandles, 5, allegrosql.TypeLonglong)
	c.Assert(orderedHandles[0], Greater, int64(3000001))
}

func (s *testSuite6) TestMaxHandleAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(a bigint PRIMARY KEY, b int)")
	tk.MustInterDirc(fmt.Sprintf("insert into t values(%v, 1)", math.MaxInt64))
	tk.MustInterDirc(fmt.Sprintf("insert into t values(%v, 1)", math.MinInt64))
	tk.MustInterDirc("alter causet t add index idx_b(b)")
	tk.MustInterDirc("admin check causet t")

	tk.MustInterDirc("create causet t1(a bigint UNSIGNED PRIMARY KEY, b int)")
	tk.MustInterDirc(fmt.Sprintf("insert into t1 values(%v, 1)", uint64(math.MaxUint64)))
	tk.MustInterDirc(fmt.Sprintf("insert into t1 values(%v, 1)", 0))
	tk.MustInterDirc("alter causet t1 add index idx_b(b)")
	tk.MustInterDirc("admin check causet t1")
}

func (s *testSuite6) TestSetDBSReorgWorkerCnt(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	err := dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(variable.DefMilevaDBDBSReorgWorkerCount))
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_worker_cnt = 1")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(1))
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.InterDirc("set @@global.milevadb_dbs_reorg_worker_cnt = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.InterDirc("set @@global.milevadb_dbs_reorg_worker_cnt = -1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	res := tk.MustQuery("select @@global.milevadb_dbs_reorg_worker_cnt")
	res.Check(testkit.Events("100"))

	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_worker_cnt")
	res.Check(testkit.Events("100"))
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_worker_cnt = 100")
	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_worker_cnt")
	res.Check(testkit.Events("100"))
}

func (s *testSuite6) TestSetDBSReorgBatchSize(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	err := dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, int32(variable.DefMilevaDBDBSReorgBatchSize))

	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_batch_size = 1")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect milevadb_dbs_reorg_batch_size value: '1'"))
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, variable.MinDBSReorgBatchSize)
	tk.MustInterDirc(fmt.Sprintf("set @@global.milevadb_dbs_reorg_batch_size = %v", variable.MaxDBSReorgBatchSize+1))
	tk.MustQuery("show warnings;").Check(testkit.Events(fmt.Sprintf("Warning 1292 Truncated incorrect milevadb_dbs_reorg_batch_size value: '%d'", variable.MaxDBSReorgBatchSize+1)))
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, variable.MaxDBSReorgBatchSize)
	_, err = tk.InterDirc("set @@global.milevadb_dbs_reorg_batch_size = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_batch_size = 100")
	err = dbsutil.LoadDBSReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSReorgBatchSize(), Equals, int32(100))
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_batch_size = -1")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect milevadb_dbs_reorg_batch_size value: '-1'"))

	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_batch_size = 100")
	res := tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	res.Check(testkit.Events("100"))

	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	res.Check(testkit.Events(fmt.Sprintf("%v", 100)))
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_batch_size = 1000")
	res = tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	res.Check(testkit.Events("1000"))
}

func (s *testSuite6) TestIllegalFunctionCall4GeneratedDeferredCausets(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// Test create an exist database
	_, err := tk.InterDirc("CREATE database test")
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create causet t1 (b double generated always as (rand()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.InterDirc("create causet t1 (a varchar(64), b varchar(1024) generated always as (load_file(a)) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.InterDirc("create causet t1 (a datetime generated always as (curdate()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.InterDirc("create causet t1 (a datetime generated always as (current_time()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.InterDirc("create causet t1 (a datetime generated always as (current_timestamp()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.InterDirc("create causet t1 (a datetime, b varchar(10) generated always as (localtime()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.InterDirc("create causet t1 (a varchar(1024) generated always as (uuid()) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.InterDirc("create causet t1 (a varchar(1024), b varchar(1024) generated always as (is_free_lock(a)) virtual);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	tk.MustInterDirc("create causet t1 (a bigint not null primary key auto_increment, b bigint, c bigint as (b + 1));")

	_, err = tk.InterDirc("alter causet t1 add defCausumn d varchar(1024) generated always as (database());")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("d").Error())

	tk.MustInterDirc("alter causet t1 add defCausumn d bigint generated always as (b + 1); ")

	_, err = tk.InterDirc("alter causet t1 modify defCausumn d bigint generated always as (connection_id());")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("d").Error())

	_, err = tk.InterDirc("alter causet t1 change defCausumn c cc bigint generated always as (connection_id());")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetFunctionIsNotAllowed.GenWithStackByArgs("cc").Error())
}

func (s *testSuite6) TestGeneratedDeferredCausetRelatedDBS(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// Test create an exist database
	_, err := tk.InterDirc("CREATE database test")
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create causet t1 (a bigint not null primary key auto_increment, b bigint as (a + 1));")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs("b").Error())

	tk.MustInterDirc("create causet t1 (a bigint not null primary key auto_increment, b bigint, c bigint as (b + 1));")

	_, err = tk.InterDirc("alter causet t1 add defCausumn d bigint generated always as (a + 1);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs("d").Error())

	tk.MustInterDirc("alter causet t1 add defCausumn d bigint generated always as (b + 1);")

	_, err = tk.InterDirc("alter causet t1 modify defCausumn d bigint generated always as (a + 1);")
	c.Assert(err.Error(), Equals, dbs.ErrGeneratedDeferredCausetRefAutoInc.GenWithStackByArgs("d").Error())

	_, err = tk.InterDirc("alter causet t1 add defCausumn e bigint as (z + 1);")
	c.Assert(err.Error(), Equals, dbs.ErrBadField.GenWithStackByArgs("z", "generated defCausumn function").Error())

	tk.MustInterDirc("drop causet t1;")
}

func (s *testSuite6) TestSetDBSErrorCountLimit(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	err := dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(variable.DefMilevaDBDBSErrorCountLimit))

	tk.MustInterDirc("set @@global.milevadb_dbs_error_count_limit = -1")
	tk.MustQuery("show warnings;").Check(testkit.Events("Warning 1292 Truncated incorrect milevadb_dbs_error_count_limit value: '-1'"))
	err = dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(0))
	tk.MustInterDirc(fmt.Sprintf("set @@global.milevadb_dbs_error_count_limit = %v", uint64(math.MaxInt64)+1))
	tk.MustQuery("show warnings;").Check(testkit.Events(fmt.Sprintf("Warning 1292 Truncated incorrect milevadb_dbs_error_count_limit value: '%d'", uint64(math.MaxInt64)+1)))
	err = dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(math.MaxInt64))
	_, err = tk.InterDirc("set @@global.milevadb_dbs_error_count_limit = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustInterDirc("set @@global.milevadb_dbs_error_count_limit = 100")
	err = dbsutil.LoadDBSVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDBSErrorCountLimit(), Equals, int64(100))
	res := tk.MustQuery("select @@global.milevadb_dbs_error_count_limit")
	res.Check(testkit.Events("100"))
}

// Test issue #9205, fix the precision problem for time type default values
// See https://github.com/whtcorpsinc/milevadb/issues/9205 for details
func (s *testSuite6) TestIssue9205(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(c time DEFAULT '12:12:12.8');`)
	tk.MustQuery("show create causet `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustInterDirc(`alter causet t add defCausumn c1 time default '12:12:12.000000';`)
	tk.MustQuery("show create causet `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '12:12:12'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustInterDirc(`alter causet t alter defCausumn c1 set default '2020-02-01 12:12:10.4';`)
	tk.MustQuery("show create causet `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '12:12:10'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustInterDirc(`alter causet t modify c1 time DEFAULT '770:12:12.000000';`)
	tk.MustQuery("show create causet `t`").Check(solitonutil.EventsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '770:12:12'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

func (s *testSuite6) TestCheckDefaultFsp(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`drop causet if exists t;`)

	_, err := tk.InterDirc("create causet t (  tt timestamp default now(1));")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.InterDirc("create causet t (  tt timestamp(1) default current_timestamp);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.InterDirc("create causet t (  tt timestamp(1) default now(2));")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	tk.MustInterDirc("create causet t (  tt timestamp(1) default now(1));")
	tk.MustInterDirc("create causet t2 (  tt timestamp default current_timestamp());")
	tk.MustInterDirc("create causet t3 (  tt timestamp default current_timestamp(0));")

	_, err = tk.InterDirc("alter causet t add defCausumn ttt timestamp default now(2);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'ttt'")

	_, err = tk.InterDirc("alter causet t add defCausumn ttt timestamp(5) default current_timestamp;")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'ttt'")

	_, err = tk.InterDirc("alter causet t add defCausumn ttt timestamp(5) default now(2);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'ttt'")

	_, err = tk.InterDirc("alter causet t modify defCausumn tt timestamp(1) default now();")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.InterDirc("alter causet t modify defCausumn tt timestamp(4) default now(5);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tt'")

	_, err = tk.InterDirc("alter causet t change defCausumn tt tttt timestamp(4) default now(5);")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tttt'")

	_, err = tk.InterDirc("alter causet t change defCausumn tt tttt timestamp(1) default now();")
	c.Assert(err.Error(), Equals, "[dbs:1067]Invalid default value for 'tttt'")
}

func (s *testSuite6) TestTimestampMinDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists tdv;")
	tk.MustInterDirc("create causet tdv(a int);")
	tk.MustInterDirc("ALTER TABLE tdv ADD COLUMN ts timestamp DEFAULT '1970-01-01 08:00:01';")
}

// this test will change the fail-point `mockAutoIDChange`, so we move it to the `testRecoverBlock` suite
func (s *testRecoverBlock) TestRenameBlock(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/spacetime/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("create database rename1")
	tk.MustInterDirc("create database rename2")
	tk.MustInterDirc("create database rename3")
	tk.MustInterDirc("create causet rename1.t (a int primary key auto_increment)")
	tk.MustInterDirc("insert rename1.t values ()")
	tk.MustInterDirc("rename causet rename1.t to rename2.t")
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustInterDirc("drop database rename1")
	tk.MustInterDirc("insert rename2.t values ()")
	tk.MustInterDirc("rename causet rename2.t to rename3.t")
	tk.MustInterDirc("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Events("1", "5001", "10001"))
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustInterDirc("drop database rename2")
	tk.MustInterDirc("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Events("1", "5001", "10001", "10002"))
	tk.MustInterDirc("drop database rename3")

	tk.MustInterDirc("create database rename1")
	tk.MustInterDirc("create database rename2")
	tk.MustInterDirc("create causet rename1.t (a int primary key auto_increment)")
	tk.MustInterDirc("rename causet rename1.t to rename2.t1")
	tk.MustInterDirc("insert rename2.t1 values ()")
	result := tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Events("1"))
	// Make sure the drop old database doesn't affect the t1's operations.
	tk.MustInterDirc("drop database rename1")
	tk.MustInterDirc("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Events("1", "2"))
	// Rename a causet to another causet in the same database.
	tk.MustInterDirc("rename causet rename2.t1 to rename2.t2")
	tk.MustInterDirc("insert rename2.t2 values ()")
	result = tk.MustQuery("select * from rename2.t2")
	result.Check(testkit.Events("1", "2", "5001"))
	tk.MustInterDirc("drop database rename2")

	tk.MustInterDirc("create database rename1")
	tk.MustInterDirc("create database rename2")
	tk.MustInterDirc("create causet rename1.t (a int primary key auto_increment)")
	tk.MustInterDirc("insert rename1.t values ()")
	tk.MustInterDirc("rename causet rename1.t to rename2.t1")
	// Make sure the value is greater than autoid.step.
	tk.MustInterDirc("insert rename2.t1 values (100000)")
	tk.MustInterDirc("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Events("1", "100000", "100001"))
	_, err := tk.InterDirc("insert rename1.t values ()")
	c.Assert(err, NotNil)
	tk.MustInterDirc("drop database rename1")
	tk.MustInterDirc("drop database rename2")
}
