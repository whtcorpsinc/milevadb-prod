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
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
)

type testPointGetSuite struct {
	causetstore    ekv.CausetStorage
	dom      *petri.Petri
	cli      *checkRequestClient
	testData solitonutil.TestData
}

func (s *testPointGetSuite) SetUpSuite(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c einsteindb.Client) einsteindb.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.causetstore, err = mockstore.NewMockStore(
		mockstore.WithClientHijacker(hijackClient),
	)
	c.Assert(err, IsNil)
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	h := s.dom.StatsHandle()
	h.SetLease(0)
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "point_get_suite")
	c.Assert(err, IsNil)
}

func (s *testPointGetSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testPointGetSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Events() {
		blockName := tb[0]
		tk.MustInterDirc(fmt.Sprintf("drop causet %v", blockName))
	}
}

func (s *testPointGetSuite) TestPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustInterDirc("insert point values (1, 1, 'a')")
	tk.MustInterDirc("insert point values (2, 2, 'b')")
	tk.MustQuery("select * from point where id = 1 and c = 0").Check(testkit.Events())
	tk.MustQuery("select * from point where id < 0 and c = 1 and d = 'b'").Check(testkit.Events())
	result, err := tk.InterDirc("select id as ident from point where id = 1")
	c.Assert(err, IsNil)
	fields := result.Fields()
	c.Assert(fields[0].DeferredCausetAsName.O, Equals, "ident")
	result.Close()

	tk.MustInterDirc("CREATE TABLE tab3(pk INTEGER PRIMARY KEY, defCaus0 INTEGER, defCaus1 FLOAT, defCaus2 TEXT, defCaus3 INTEGER, defCaus4 FLOAT, defCaus5 TEXT);")
	tk.MustInterDirc("CREATE UNIQUE INDEX idx_tab3_0 ON tab3 (defCaus4);")
	tk.MustInterDirc("INSERT INTO tab3 VALUES(0,854,111.96,'mguub',711,966.36,'snwlo');")
	tk.MustQuery("SELECT ALL * FROM tab3 WHERE defCaus4 = 85;").Check(testkit.Events())

	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a bigint primary key, b bigint, c bigint);`)
	tk.MustInterDirc(`insert into t values(1, NULL, NULL), (2, NULL, 2), (3, 3, NULL), (4, 4, 4), (5, 6, 7);`)
	tk.MustQuery(`select * from t where a = 1;`).Check(testkit.Events(
		`1 <nil> <nil>`,
	))
	tk.MustQuery(`select * from t where a = 2;`).Check(testkit.Events(
		`2 <nil> 2`,
	))
	tk.MustQuery(`select * from t where a = 3;`).Check(testkit.Events(
		`3 3 <nil>`,
	))
	tk.MustQuery(`select * from t where a = 4;`).Check(testkit.Events(
		`4 4 4`,
	))
	tk.MustQuery(`select a, a, b, a, b, c, b, c, c from t where a = 5;`).Check(testkit.Events(
		`5 5 6 5 6 7 6 7 7`,
	))
	tk.MustQuery(`select b, b from t where a = 1`).Check(testkit.Events(
		"<nil> <nil>"))
}

func (s *testPointGetSuite) TestPointGetOverflow(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t0")
	tk.MustInterDirc("CREATE TABLE t0(c1 BOOL UNIQUE)")
	tk.MustInterDirc("INSERT INTO t0(c1) VALUES (-128)")
	tk.MustInterDirc("INSERT INTO t0(c1) VALUES (127)")
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=-129").Check(testkit.Events()) // no result
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=-128").Check(testkit.Events("-128"))
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=128").Check(testkit.Events())
	tk.MustQuery("SELECT t0.c1 FROM t0 WHERE t0.c1=127").Check(testkit.Events("127"))
}

func (s *testPointGetSuite) TestPointGetCharPK(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a char(4) primary key, b char(4));`)
	tk.MustInterDirc(`insert into t values("aa", "bb");`)

	// Test CHAR type.
	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Events())

	tk.MustInterDirc(`truncate causet t;`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Events(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Events())

	// Test CHAR BINARY.
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a char(2) binary primary key, b char(2));`)
	tk.MustInterDirc(`insert into t values("  ", "  ");`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Events(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Events())
	tk.MustBlockDual(`select * from t where a = "a  ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "";`).Check(testkit.Events(` `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Events())
	tk.MustBlockDual(`select * from t where a = "   ";`).Check(testkit.Events())

}

func (s *testPointGetSuite) TestPointGetAliasBlockCharPK(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a char(2) primary key, b char(2));`)
	tk.MustInterDirc(`insert into t values("aa", "bb");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustBlockDual(`select * from t tmp where a = "aab";`).Check(testkit.Events())

	tk.MustInterDirc(`truncate causet t;`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Events(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Events())
	tk.MustBlockDual(`select * from t tmp where a = "a  ";`).Check(testkit.Events())

	// Test CHAR BINARY.
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a char(2) binary primary key, b char(2));`)
	tk.MustInterDirc(`insert into t values("  ", "  ");`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Events(`a b`))
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Events())
	tk.MustBlockDual(`select * from t tmp where a = "a  ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t tmp where a = "";`).Check(testkit.Events(` `))
	tk.MustPointGet(`select * from t tmp where a = "  ";`).Check(testkit.Events())
	tk.MustBlockDual(`select * from t tmp where a = "   ";`).Check(testkit.Events())

	// Test both wildcard and defCausumn name exist in select field list
	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a char(2) primary key, b char(2));`)
	tk.MustInterDirc(`insert into t values("aa", "bb");`)
	tk.MustPointGet(`select *, a from t tmp where a = "aa";`).Check(testkit.Events(`aa bb aa`))

	// Test using causet alias in field list
	tk.MustPointGet(`select tmp.* from t tmp where a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustPointGet(`select tmp.a, tmp.b from t tmp where a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustPointGet(`select tmp.*, tmp.a, tmp.b from t tmp where a = "aa";`).Check(testkit.Events(`aa bb aa bb`))
	tk.MustBlockDual(`select tmp.* from t tmp where a = "aab";`).Check(testkit.Events())
	tk.MustBlockDual(`select tmp.a, tmp.b from t tmp where a = "aab";`).Check(testkit.Events())
	tk.MustBlockDual(`select tmp.*, tmp.a, tmp.b from t tmp where a = "aab";`).Check(testkit.Events())

	// Test using causet alias in where clause
	tk.MustPointGet(`select * from t tmp where tmp.a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustPointGet(`select a, b from t tmp where tmp.a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustPointGet(`select *, a, b from t tmp where tmp.a = "aa";`).Check(testkit.Events(`aa bb aa bb`))

	// Unknown causet name in where clause and field list
	err := tk.InterDircToErr(`select a from t where xxxxx.a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown defCausumn 'xxxxx.a' in 'where clause'")
	err = tk.InterDircToErr(`select xxxxx.a from t where a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown defCausumn 'xxxxx.a' in 'field list'")

	// When an alias is provided, it completely hides the actual name of the causet.
	err = tk.InterDircToErr(`select a from t tmp where t.a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown defCausumn 't.a' in 'where clause'")
	err = tk.InterDircToErr(`select t.a from t tmp where a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown defCausumn 't.a' in 'field list'")
	err = tk.InterDircToErr(`select t.* from t tmp where a = "aa"`)
	c.Assert(err, ErrorMatches, ".*Unknown causet 't'")
}

func (s *testPointGetSuite) TestIndexLookupChar(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a char(2), b char(2), index idx_1(a));`)
	tk.MustInterDirc(`insert into t values("aa", "bb");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustIndexLookup(`select * from t where a = "aab";`).Check(testkit.Events())

	// Test query with causet alias
	tk.MustIndexLookup(`select * from t tmp where a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustIndexLookup(`select * from t tmp where a = "aab";`).Check(testkit.Events())

	tk.MustInterDirc(`truncate causet t;`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Events(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Events())

	// Test CHAR BINARY.
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a char(2) binary, b char(2), index idx_1(a));`)
	tk.MustInterDirc(`insert into t values("  ", "  ");`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Events(`a b`))
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "";`).Check(testkit.Events(` `))
	tk.MustIndexLookup(`select * from t where a = " ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "  ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "   ";`).Check(testkit.Events())

}

func (s *testPointGetSuite) TestPointGetVarcharPK(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a varchar(2) primary key, b varchar(2));`)
	tk.MustInterDirc(`insert into t values("aa", "bb");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Events(`aa bb`))
	tk.MustBlockDual(`select * from t where a = "aab";`).Check(testkit.Events())

	tk.MustInterDirc(`truncate causet t;`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustBlockDual(`select * from t where a = "a  ";`).Check(testkit.Events())

	// // Test VARCHAR BINARY.
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a varchar(2) binary primary key, b varchar(2));`)
	tk.MustInterDirc(`insert into t values("  ", "  ");`)
	tk.MustInterDirc(`insert into t values("a ", "b ");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustBlockDual(`select * from t where a = "a  ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Events(`     `))
	tk.MustBlockDual(`select * from t where a = "   ";`).Check(testkit.Events())

}

func (s *testPointGetSuite) TestPointGetBinaryPK(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a binary(2) primary key, b binary(2));`)
	tk.MustInterDirc(`insert into t values("a", "b");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a\0";`).Check(testkit.Events("a\x00 b\x00"))

	tk.MustInterDirc(`insert into t values("a ", "b ");`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Events())

	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Events())
}

func (s *testPointGetSuite) TestPointGetAliasBlockBinaryPK(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a binary(2) primary key, b binary(2));`)
	tk.MustInterDirc(`insert into t values("a", "b");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t tmp where a = "a\0";`).Check(testkit.Events("a\x00 b\x00"))

	tk.MustInterDirc(`insert into t values("a ", "b ");`)
	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Events())

	tk.MustPointGet(`select * from t tmp where a = "a";`).Check(testkit.Events())
	tk.MustPointGet(`select * from t tmp where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustPointGet(`select * from t tmp where a = "a  ";`).Check(testkit.Events())
}

func (s *testPointGetSuite) TestIndexLookupBinary(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a binary(2), b binary(2), index idx_1(a));`)
	tk.MustInterDirc(`insert into t values("a", "b");`)

	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "a\0";`).Check(testkit.Events("a\x00 b\x00"))

	// Test query with causet alias
	tk.MustInterDirc(`set @@sql_mode="";`)
	tk.MustIndexLookup(`select * from t tmp where a = "a";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t tmp where a = "a ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t tmp where a = "a  ";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t tmp where a = "a\0";`).Check(testkit.Events("a\x00 b\x00"))

	tk.MustInterDirc(`insert into t values("a ", "b ");`)
	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Events())

	tk.MustIndexLookup(`select * from t where a = "a";`).Check(testkit.Events())
	tk.MustIndexLookup(`select * from t where a = "a ";`).Check(testkit.Events(`a  b `))
	tk.MustIndexLookup(`select * from t where a = "a  ";`).Check(testkit.Events())

}

func (s *testPointGetSuite) TestOverflowOrTruncated(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t6 (id bigint, a bigint, primary key(id), unique key(a));")
	tk.MustInterDirc("insert into t6 values(9223372036854775807, 9223372036854775807);")
	tk.MustInterDirc("insert into t6 values(1, 1);")
	var nilVal []string
	// for unique key
	tk.MustQuery("select * from t6 where a = 9223372036854775808").Check(testkit.Events(nilVal...))
	tk.MustQuery("select * from t6 where a = '1.123'").Check(testkit.Events(nilVal...))
	// for primary key
	tk.MustQuery("select * from t6 where id = 9223372036854775808").Check(testkit.Events(nilVal...))
	tk.MustQuery("select * from t6 where id = '1.123'").Check(testkit.Events(nilVal...))
}

func (s *testPointGetSuite) TestIssue10448(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(pk int1 primary key)")
	tk.MustInterDirc("insert into t values(125)")
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 128").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(pk int8 primary key)")
	tk.MustInterDirc("insert into t values(9223372036854775807)")
	tk.MustQuery("select * from t where pk = 9223372036854775807").Check(testkit.Events("9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Events("Point_Get_1 1.00 root causet:t handle:9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(pk int1 unsigned primary key)")
	tk.MustInterDirc("insert into t values(255)")
	tk.MustQuery("select * from t where pk = 255").Check(testkit.Events("255"))
	tk.MustQuery("desc select * from t where pk = 256").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(pk int8 unsigned primary key)")
	tk.MustInterDirc("insert into t value(18446744073709551615)")
	tk.MustQuery("desc select * from t where pk = 18446744073709551615").Check(testkit.Events("Point_Get_1 1.00 root causet:t handle:18446744073709551615"))
	tk.MustQuery("select * from t where pk = 18446744073709551615").Check(testkit.Events("18446744073709551615"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775807").Check(testkit.Events("Point_Get_1 1.00 root causet:t handle:9223372036854775807"))
	tk.MustQuery("desc select * from t where pk = 18446744073709551616").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("desc select * from t where pk = 9223372036854775808").Check(testkit.Events("Point_Get_1 1.00 root causet:t handle:9223372036854775808"))
}

func (s *testPointGetSuite) TestIssue10677(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(pk int1 primary key)")
	tk.MustInterDirc("insert into t values(1)")
	tk.MustQuery("desc select * from t where pk = 1.1").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("select * from t where pk = 1.1").Check(testkit.Events())
	tk.MustQuery("desc select * from t where pk = '1.1'").Check(testkit.Events("BlockDual_2 0.00 root  rows:0"))
	tk.MustQuery("select * from t where pk = '1.1'").Check(testkit.Events())
	tk.MustQuery("desc select * from t where pk = 1").Check(testkit.Events("Point_Get_1 1.00 root causet:t handle:1"))
	tk.MustQuery("select * from t where pk = 1").Check(testkit.Events("1"))
	tk.MustQuery("desc select * from t where pk = '1'").Check(testkit.Events("Point_Get_1 1.00 root causet:t handle:1"))
	tk.MustQuery("select * from t where pk = '1'").Check(testkit.Events("1"))
	tk.MustQuery("desc select * from t where pk = '1.0'").Check(testkit.Events("Point_Get_1 1.00 root causet:t handle:1"))
	tk.MustQuery("select * from t where pk = '1.0'").Check(testkit.Events("1"))
}

func (s *testPointGetSuite) TestForUFIDelateRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.InterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(pk int primary key, c int)")
	tk.MustInterDirc("insert into t values (1, 1), (2, 2)")
	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("begin")
	tk.MustQuery("select * from t where pk = 1 for uFIDelate")
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustInterDirc("uFIDelate t set c = c + 1 where pk = 1")
	tk.MustInterDirc("uFIDelate t set c = c + 1 where pk = 2")
	_, err := tk.InterDirc("commit")
	c.Assert(stochastik.ErrForUFIDelateCantRetry.Equal(err), IsTrue)
}

func (s *testPointGetSuite) TestPointGetByEventID(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a varchar(20), b int)")
	tk.MustInterDirc("insert into t values(\"aaa\", 12)")
	tk.MustQuery("explain select * from t where t._milevadb_rowid = 1").Check(testkit.Events(
		"Point_Get_1 1.00 root causet:t handle:1"))
	tk.MustQuery("select * from t where t._milevadb_rowid = 1").Check(testkit.Events("aaa 12"))
}

func (s *testPointGetSuite) TestSelectCheckVisibility(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a varchar(10) key, b int,index idx(b))")
	tk.MustInterDirc("insert into t values('1',1)")
	tk.MustInterDirc("begin")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	ts := txn.StartTS()
	causetstore := tk.Se.GetStore().(einsteindb.CausetStorage)
	// UFIDelate gc safe time for check data visibility.
	causetstore.UFIDelateSPCache(ts+1, time.Now())
	checkSelectResultError := func(allegrosql string, expectErr *terror.Error) {
		re, err := tk.InterDirc(allegrosql)
		c.Assert(err, IsNil)
		_, err = stochastik.ResultSetToStringSlice(context.Background(), tk.Se, re)
		c.Assert(err, NotNil)
		c.Assert(expectErr.Equal(err), IsTrue)
	}
	// Test point get.
	checkSelectResultError("select * from t where a='1'", einsteindb.ErrGCTooEarly)
	// Test batch point get.
	checkSelectResultError("select * from t where a in ('1','2')", einsteindb.ErrGCTooEarly)
	// Test Index look up read.
	checkSelectResultError("select * from t where b > 0 ", einsteindb.ErrGCTooEarly)
	// Test Index read.
	checkSelectResultError("select b from t where b > 0 ", einsteindb.ErrGCTooEarly)
	// Test causet read.
	checkSelectResultError("select * from t", einsteindb.ErrGCTooEarly)
}

func (s *testPointGetSuite) TestReturnValues(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("set @@milevadb_enable_clustered_index=0;")
	tk.MustInterDirc("create causet t (a varchar(64) primary key, b int)")
	tk.MustInterDirc("insert t values ('a', 1), ('b', 2), ('c', 3)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from t where a = 'b' for uFIDelate").Check(testkit.Events("b 2"))
	tid := tk.GetBlockID("t")
	idxVal, err := codec.EncodeKey(tk.Se.GetStochastikVars().StmtCtx, nil, types.NewStringCauset("b"))
	c.Assert(err, IsNil)
	pk := blockcodec.EncodeIndexSeekKey(tid, 1, idxVal)
	txnCtx := tk.Se.GetStochastikVars().TxnCtx
	val, ok := txnCtx.GetKeyInPessimisticLockCache(pk)
	c.Assert(ok, IsTrue)
	handle, err := blockcodec.DecodeHandleInUniqueIndexValue(val, false)
	c.Assert(err, IsNil)
	rowKey := blockcodec.EncodeEventKeyWithHandle(tid, handle)
	_, ok = txnCtx.GetKeyInPessimisticLockCache(rowKey)
	c.Assert(ok, IsTrue)
	tk.MustInterDirc("rollback")
}

func (s *testPointGetSuite) TestClusterIndexPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`set @@milevadb_enable_clustered_index=true`)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists pgt")
	tk.MustInterDirc("create causet pgt (a varchar(64), b varchar(64), uk int, v int, primary key(a, b), unique key uuk(uk))")
	tk.MustInterDirc("insert pgt values ('a', 'a1', 1, 11), ('b', 'b1', 2, 22), ('c', 'c1', 3, 33)")
	tk.MustQuery(`select * from pgt where (a, b) in (('a', 'a1'), ('c', 'c1'))`).Check(testkit.Events("a a1 1 11", "c c1 3 33"))
	tk.MustQuery(`select * from pgt where a = 'b' and b = 'b1'`).Check(testkit.Events("b b1 2 22"))
	tk.MustQuery(`select * from pgt where uk = 1`).Check(testkit.Events("a a1 1 11"))
	tk.MustQuery(`select * from pgt where uk in (1, 2, 3)`).Check(testkit.Events("a a1 1 11", "b b1 2 22", "c c1 3 33"))
	tk.MustInterDirc(`admin check causet pgt`)

	tk.MustInterDirc(`drop causet if exists snp`)
	tk.MustInterDirc(`create causet snp(id1 int, id2 int, v int, primary key(id1, id2))`)
	tk.MustInterDirc(`insert snp values (1, 1, 1), (2, 2, 2), (2, 3, 3)`)
	tk.MustQuery(`explain select * from snp where id1 = 1`).Check(testkit.Events("BlockReader_6 10.00 root  data:BlockRangeScan_5",
		"└─BlockRangeScan_5 10.00 cop[einsteindb] causet:snp range:[1,1], keep order:false, stats:pseudo"))
	tk.MustQuery(`explain select * from snp where id1 in (1, 100)`).Check(testkit.Events("BlockReader_6 20.00 root  data:BlockRangeScan_5",
		"└─BlockRangeScan_5 20.00 cop[einsteindb] causet:snp range:[1,1], [100,100], keep order:false, stats:pseudo"))
	tk.MustQuery("select * from snp where id1 = 2").Check(testkit.Events("2 2 2", "2 3 3"))
}

func (s *testPointGetSuite) TestClusterIndexCBOPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`set @@milevadb_enable_clustered_index=true`)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc(`create causet t1 (a int, b decimal(10,0), c int, primary key(a,b))`)
	tk.MustInterDirc(`create causet t2 (a varchar(20), b int, primary key(a), unique key(b))`)
	tk.MustInterDirc(`insert into t1 values(1,1,1),(2,2,2),(3,3,3)`)
	tk.MustInterDirc(`insert into t2 values('111',1),('222',2),('333',3)`)
	tk.MustInterDirc("analyze causet t1")
	tk.MustInterDirc("analyze causet t2")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL  string
		Causet []string
		Res  []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		plan := tk.MustQuery("explain " + tt)
		res := tk.MustQuery(tt).Sort()
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = s.testData.ConvertEventsToStrings(plan.Events())
			output[i].Res = s.testData.ConvertEventsToStrings(res.Events())
		})
		plan.Check(testkit.Events(output[i].Causet...))
		res.Check(testkit.Events(output[i].Res...))
	}
}
