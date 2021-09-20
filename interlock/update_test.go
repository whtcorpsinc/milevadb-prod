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
	"flag"
	"fmt"

	"github.com/whtcorpsinc/BerolinaSQL"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

type testUFIDelateSuite struct {
	cluster     cluster.Cluster
	causetstore ekv.CausetStorage
	petri       *petri.Petri
	*BerolinaSQL.BerolinaSQL
	ctx *mock.Context
}

func (s *testUFIDelateSuite) SetUpSuite(c *C) {
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
	d, err := stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	d.SetStatsUFIDelating(true)
	s.petri = d
}

func (s *testUFIDelateSuite) TearDownSuite(c *C) {
	s.petri.Close()
	s.causetstore.Close()
}

func (s *testUFIDelateSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Events() {
		blockName := tb[0]
		tk.MustInterDirc(fmt.Sprintf("drop causet %v", blockName))
	}
}

func (s *testUFIDelateSuite) TestUFIDelateGenDefCausInTxn(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`create causet t(a bigint, b bigint as (a+1));`)
	tk.MustInterDirc(`begin;`)
	tk.MustInterDirc(`insert into t(a) values(1);`)
	err := tk.InterDircToErr(`uFIDelate t set b=6 where b=2;`)
	c.Assert(err.Error(), Equals, "[causet:3105]The value specified for generated defCausumn 'b' in causet 't' is not allowed.")
	tk.MustInterDirc(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Events(
		`1 2`))
}

func (s *testUFIDelateSuite) TestUFIDelateWithAutoidSchema(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test`)
	tk.MustInterDirc(`create causet t1(id int primary key auto_increment, n int);`)
	tk.MustInterDirc(`create causet t2(id int primary key, n float auto_increment, key I_n(n));`)
	tk.MustInterDirc(`create causet t3(id int primary key, n double auto_increment, key I_n(n));`)

	tests := []struct {
		exec   string
		query  string
		result [][]interface{}
	}{
		{
			`insert into t1 set n = 1`,
			`select * from t1 where id = 1`,
			testkit.Events(`1 1`),
		},
		{
			`uFIDelate t1 set id = id+1`,
			`select * from t1 where id = 2`,
			testkit.Events(`2 1`),
		},
		{
			`insert into t1 set n = 2`,
			`select * from t1 where id = 3`,
			testkit.Events(`3 2`),
		},
		{
			`uFIDelate t1 set id = id + '1.1' where id = 3`,
			`select * from t1 where id = 4`,
			testkit.Events(`4 2`),
		},
		{
			`insert into t1 set n = 3`,
			`select * from t1 where id = 5`,
			testkit.Events(`5 3`),
		},
		{
			`uFIDelate t1 set id = id + '0.5' where id = 5`,
			`select * from t1 where id = 6`,
			testkit.Events(`6 3`),
		},
		{
			`insert into t1 set n = 4`,
			`select * from t1 where id = 7`,
			testkit.Events(`7 4`),
		},
		{
			`insert into t2 set id = 1`,
			`select * from t2 where id = 1`,
			testkit.Events(`1 1`),
		},
		{
			`uFIDelate t2 set n = n+1`,
			`select * from t2 where id = 1`,
			testkit.Events(`1 2`),
		},
		{
			`insert into t2 set id = 2`,
			`select * from t2 where id = 2`,
			testkit.Events(`2 3`),
		},
		{
			`uFIDelate t2 set n = n + '2.2'`,
			`select * from t2 where id = 2`,
			testkit.Events(`2 5.2`),
		},
		{
			`insert into t2 set id = 3`,
			`select * from t2 where id = 3`,
			testkit.Events(`3 6`),
		},
		{
			`uFIDelate t2 set n = n + '0.5' where id = 3`,
			`select * from t2 where id = 3`,
			testkit.Events(`3 6.5`),
		},
		{
			`insert into t2 set id = 4`,
			`select * from t2 where id = 4`,
			testkit.Events(`4 7`),
		},
		{
			`insert into t3 set id = 1`,
			`select * from t3 where id = 1`,
			testkit.Events(`1 1`),
		},
		{
			`uFIDelate t3 set n = n+1`,
			`select * from t3 where id = 1`,
			testkit.Events(`1 2`),
		},
		{
			`insert into t3 set id = 2`,
			`select * from t3 where id = 2`,
			testkit.Events(`2 3`),
		},
		{
			`uFIDelate t3 set n = n + '3.3'`,
			`select * from t3 where id = 2`,
			testkit.Events(`2 6.3`),
		},
		{
			`insert into t3 set id = 3`,
			`select * from t3 where id = 3`,
			testkit.Events(`3 7`),
		},
		{
			`uFIDelate t3 set n = n + '0.5' where id = 3`,
			`select * from t3 where id = 3`,
			testkit.Events(`3 7.5`),
		},
		{
			`insert into t3 set id = 4`,
			`select * from t3 where id = 4`,
			testkit.Events(`4 8`),
		},
	}

	for _, tt := range tests {
		tk.MustInterDirc(tt.exec)
		tk.MustQuery(tt.query).Check(tt.result)
	}
}

func (s *testUFIDelateSuite) TestUFIDelateSchemaChange(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`create causet t(a bigint, b bigint as (a+1));`)
	tk.MustInterDirc(`begin;`)
	tk.MustInterDirc(`insert into t(a) values(1);`)
	err := tk.InterDircToErr(`uFIDelate t set b=6 where b=2;`)
	c.Assert(err.Error(), Equals, "[causet:3105]The value specified for generated defCausumn 'b' in causet 't' is not allowed.")
	tk.MustInterDirc(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Events(
		`1 2`))
}

func (s *testUFIDelateSuite) TestUFIDelateMultiDatabaseBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop database if exists test2")
	tk.MustInterDirc("create database test2")
	tk.MustInterDirc("create causet t(a int, b int generated always  as (a+1) virtual)")
	tk.MustInterDirc("create causet test2.t(a int, b int generated always  as (a+1) virtual)")
	tk.MustInterDirc("uFIDelate t, test2.t set test.t.a=1")
}

var _ = SerialSuites(&testSuite11{&baseTestSuite{}})

type testSuite11 struct {
	*baseTestSuite
}

func (s *testSuite11) TestUFIDelateClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`set @@milevadb_enable_clustered_index=true`)
	tk.MustInterDirc(`use test`)

	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`create causet t(id varchar(200) primary key, v int)`)
	tk.MustInterDirc(`insert into t(id, v) values ('abc', 233)`)
	tk.MustQuery(`select id, v from t where id = 'abc'`).Check(testkit.Events("abc 233"))
	tk.MustInterDirc(`uFIDelate t set id = 'dfg' where id = 'abc'`)
	tk.MustQuery(`select * from t`).Check(testkit.Events("dfg 233"))
	tk.MustInterDirc(`uFIDelate t set id = 'aaa', v = 333 where id = 'dfg'`)
	tk.MustQuery(`select * from t where id = 'aaa'`).Check(testkit.Events("aaa 333"))
	tk.MustInterDirc(`uFIDelate t set v = 222 where id = 'aaa'`)
	tk.MustQuery(`select * from t where id = 'aaa'`).Check(testkit.Events("aaa 222"))
	tk.MustInterDirc(`insert into t(id, v) values ('bbb', 111)`)
	tk.MustGetErrCode(`uFIDelate t set id = 'bbb' where id = 'aaa'`, errno.ErrDupEntry)

	tk.MustInterDirc(`drop causet if exists ut3pk`)
	tk.MustInterDirc(`create causet ut3pk(id1 varchar(200), id2 varchar(200), v int, id3 int, primary key(id1, id2, id3))`)
	tk.MustInterDirc(`insert into ut3pk(id1, id2, v, id3) values ('aaa', 'bbb', 233, 111)`)
	tk.MustQuery(`select id1, id2, id3, v from ut3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`).Check(testkit.Events("aaa bbb 111 233"))
	tk.MustInterDirc(`uFIDelate ut3pk set id1 = 'abc', id2 = 'bbb2', id3 = 222, v = 555 where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`)
	tk.MustQuery(`select id1, id2, id3, v from ut3pk where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`).Check(testkit.Events("abc bbb2 222 555"))
	tk.MustQuery(`select id1, id2, id3, v from ut3pk`).Check(testkit.Events("abc bbb2 222 555"))
	tk.MustInterDirc(`uFIDelate ut3pk set v = 666 where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`)
	tk.MustQuery(`select id1, id2, id3, v from ut3pk`).Check(testkit.Events("abc bbb2 222 666"))
	tk.MustInterDirc(`insert into ut3pk(id1, id2, id3, v) values ('abc', 'bbb3', 222, 777)`)
	tk.MustGetErrCode(`uFIDelate ut3pk set id2 = 'bbb3' where id1 = 'abc' and id2 = 'bbb2' and id3 = 222`, errno.ErrDupEntry)

	tk.MustInterDirc(`drop causet if exists ut1pku`)
	tk.MustInterDirc(`create causet ut1pku(id varchar(200) primary key, uk int, v int, unique key ukk(uk))`)
	tk.MustInterDirc(`insert into ut1pku(id, uk, v) values('a', 1, 2), ('b', 2, 3)`)
	tk.MustQuery(`select * from ut1pku`).Check(testkit.Events("a 1 2", "b 2 3"))
	tk.MustInterDirc(`uFIDelate ut1pku set uk = 3 where id = 'a'`)
	tk.MustQuery(`select * from ut1pku`).Check(testkit.Events("a 3 2", "b 2 3"))
	tk.MustGetErrCode(`uFIDelate ut1pku set uk = 2 where id = 'a'`, errno.ErrDupEntry)
	tk.MustQuery(`select * from ut1pku`).Check(testkit.Events("a 3 2", "b 2 3"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a char(10) primary key, b char(10));")
	tk.MustInterDirc("insert into t values('a', 'b');")
	tk.MustInterDirc("uFIDelate t set a='c' where t.a='a' and b='b';")
	tk.MustQuery("select * from t").Check(testkit.Events("c b"))

	tk.MustInterDirc("drop causet if exists s")
	tk.MustInterDirc("create causet s (a int, b int, c int, primary key (a, b))")
	tk.MustInterDirc("insert s values (3, 3, 3), (5, 5, 5)")
	tk.MustInterDirc("uFIDelate s set c = 10 where a = 3")
	tk.MustQuery("select * from s").Check(testkit.Events("3 3 10", "5 5 5"))
}

func (s *testSuite11) TestDeleteClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`set @@milevadb_enable_clustered_index=true`)
	tk.MustInterDirc(`use test`)

	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`create causet t(id varchar(200) primary key, v int)`)
	tk.MustInterDirc(`insert into t(id, v) values ('abc', 233)`)
	tk.MustInterDirc(`delete from t where id = 'abc'`)
	tk.MustQuery(`select * from t`).Check(testkit.Events())
	tk.MustQuery(`select * from t where id = 'abc'`).Check(testkit.Events())

	tk.MustInterDirc(`drop causet if exists it3pk`)
	tk.MustInterDirc(`create causet it3pk(id1 varchar(200), id2 varchar(200), v int, id3 int, primary key(id1, id2, id3))`)
	tk.MustInterDirc(`insert into it3pk(id1, id2, v, id3) values ('aaa', 'bbb', 233, 111)`)
	tk.MustInterDirc(`delete from it3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`)
	tk.MustQuery(`select * from it3pk`).Check(testkit.Events())
	tk.MustQuery(`select * from it3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`).Check(testkit.Events())
	tk.MustInterDirc(`insert into it3pk(id1, id2, v, id3) values ('aaa', 'bbb', 433, 111)`)
	tk.MustQuery(`select * from it3pk where id1 = 'aaa' and id2 = 'bbb' and id3 = 111`).Check(testkit.Events("aaa bbb 433 111"))

	tk.MustInterDirc(`drop causet if exists dt3pku`)
	tk.MustInterDirc(`create causet dt3pku(id varchar(200) primary key, uk int, v int, unique key uuk(uk))`)
	tk.MustInterDirc(`insert into dt3pku(id, uk, v) values('a', 1, 2)`)
	tk.MustInterDirc(`delete from dt3pku where id = 'a'`)
	tk.MustQuery(`select * from dt3pku`).Check(testkit.Events())
	tk.MustInterDirc(`insert into dt3pku(id, uk, v) values('a', 1, 2)`)

	tk.MustInterDirc("drop causet if exists s1")
	tk.MustInterDirc("create causet s1 (a int, b int, c int, primary key (a, b))")
	tk.MustInterDirc("insert s1 values (3, 3, 3), (5, 5, 5)")
	tk.MustInterDirc("delete from s1 where a = 3")
	tk.MustQuery("select * from s1").Check(testkit.Events("5 5 5"))
}

func (s *testSuite11) TestReplaceClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`set @@milevadb_enable_clustered_index=true`)
	tk.MustInterDirc(`use test`)

	tk.MustInterDirc(`drop causet if exists rt1pk`)
	tk.MustInterDirc(`create causet rt1pk(id varchar(200) primary key, v int)`)
	tk.MustInterDirc(`replace into rt1pk(id, v) values('abc', 1)`)
	tk.MustQuery(`select * from rt1pk`).Check(testkit.Events("abc 1"))
	tk.MustInterDirc(`replace into rt1pk(id, v) values('bbb', 233), ('abc', 2)`)
	tk.MustQuery(`select * from rt1pk`).Check(testkit.Events("abc 2", "bbb 233"))

	tk.MustInterDirc(`drop causet if exists rt3pk`)
	tk.MustInterDirc(`create causet rt3pk(id1 timestamp, id2 time, v int, id3 year, primary key(id1, id2, id3))`)
	tk.MustInterDirc(`replace into rt3pk(id1, id2,id3, v) values('2020-01-01 11:11:11', '22:22:22', '2020', 1)`)
	tk.MustQuery(`select * from rt3pk`).Check(testkit.Events("2020-01-01 11:11:11 22:22:22 1 2020"))
	tk.MustInterDirc(`replace into rt3pk(id1, id2, id3, v) values('2020-01-01 11:11:11', '22:22:22', '2020', 2)`)
	tk.MustQuery(`select * from rt3pk`).Check(testkit.Events("2020-01-01 11:11:11 22:22:22 2 2020"))

	tk.MustInterDirc(`drop causet if exists rt1pk1u`)
	tk.MustInterDirc(`create causet rt1pk1u(id varchar(200) primary key, uk int, v int, unique key uuk(uk))`)
	tk.MustInterDirc(`replace into rt1pk1u(id, uk, v) values("abc", 2, 1)`)
	tk.MustQuery(`select * from rt1pk1u`).Check(testkit.Events("abc 2 1"))
	tk.MustInterDirc(`replace into rt1pk1u(id, uk, v) values("aaa", 2, 11)`)
	tk.MustQuery(`select * from rt1pk1u`).Check(testkit.Events("aaa 2 11"))
}

func (s *testSuite11) TestPessimisticUFIDelatePKLazyCheck(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	s.testUFIDelatePKLazyCheck(c, tk, true)
	s.testUFIDelatePKLazyCheck(c, tk, false)
}

func (s *testSuite11) testUFIDelatePKLazyCheck(c *C, tk *testkit.TestKit, clusteredIndex bool) {
	tk.MustInterDirc(fmt.Sprintf(`set @@milevadb_enable_clustered_index=%v`, clusteredIndex))
	tk.MustInterDirc(`drop causet if exists upk`)
	tk.MustInterDirc(`create causet upk (a int, b int, c int, primary key (a, b))`)
	tk.MustInterDirc(`insert upk values (1, 1, 1), (2, 2, 2), (3, 3, 3)`)
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate upk set b = b + 1 where a between 1 and 2")
	c.Assert(getPresumeExistsCount(c, tk.Se), Equals, 2)
	_, err := tk.InterDirc("uFIDelate upk set a = 3, b = 3 where a between 1 and 2")
	c.Assert(ekv.ErrKeyExists.Equal(err), IsTrue)
	tk.MustInterDirc("commit")
}

func getPresumeExistsCount(c *C, se stochastik.Stochastik) int {
	txn, err := se.Txn(false)
	c.Assert(err, IsNil)
	buf := txn.GetMemBuffer()
	it, err := buf.Iter(nil, nil)
	c.Assert(err, IsNil)
	presumeNotExistsCnt := 0
	for it.Valid() {
		flags, err1 := buf.GetFlags(it.Key())
		c.Assert(err1, IsNil)
		err = it.Next()
		c.Assert(err, IsNil)
		if flags.HasPresumeKeyNotExists() {
			presumeNotExistsCnt++
		}
	}
	return presumeNotExistsCnt
}
