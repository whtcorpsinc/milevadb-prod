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

package types_test

import (
	"context"
	"flag"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

var _ = Suite(&testMyALLEGROSQLConstSuite{})

type testMyALLEGROSQLConstSuite struct {
	cluster   cluster.Cluster
	mvccStore mockeinsteindb.MVCCStore
	causetstore     ekv.CausetStorage
	dom       *petri.Petri
	*BerolinaSQL.BerolinaSQL
}

var mockEinsteinDB = flag.Bool("mockEinsteinDB", true, "use mock einsteindb causetstore in interlock test")

func (s *testMyALLEGROSQLConstSuite) SetUpSuite(c *C) {
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
	var err error
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testMyALLEGROSQLConstSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
}

func (s *testMyALLEGROSQLConstSuite) TestGetALLEGROSQLMode(c *C) {
	positiveCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE"},
		{",,NO_ZERO_DATE"},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE"},
		{""},
		{", "},
		{","},
	}

	for _, t := range positiveCases {
		_, err := allegrosql.GetALLEGROSQLMode(allegrosql.FormatALLEGROSQLModeStr(t.arg))
		c.Assert(err, IsNil)
	}

	negativeCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE, NO_ZERO_IN_DATE"},
		{"NO_ZERO_DATE,adfadsdfasdfads"},
		{", ,NO_ZERO_DATE"},
		{" ,"},
	}

	for _, t := range negativeCases {
		_, err := allegrosql.GetALLEGROSQLMode(allegrosql.FormatALLEGROSQLModeStr(t.arg))
		c.Assert(err, NotNil)
	}
}

func (s *testMyALLEGROSQLConstSuite) TestALLEGROSQLMode(c *C) {
	tests := []struct {
		arg                           string
		hasNoZeroDateMode             bool
		hasNoZeroInDateMode           bool
		hasErrorForDivisionByZeroMode bool
	}{
		{"NO_ZERO_DATE", true, false, false},
		{"NO_ZERO_IN_DATE", false, true, false},
		{"ERROR_FOR_DIVISION_BY_ZERO", false, false, true},
		{"NO_ZERO_IN_DATE,NO_ZERO_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", true, true, true},
		{"NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", false, true, true},
		{"", false, false, false},
	}

	for _, t := range tests {
		sqlMode, _ := allegrosql.GetALLEGROSQLMode(t.arg)
		c.Assert(sqlMode.HasNoZeroDateMode(), Equals, t.hasNoZeroDateMode)
		c.Assert(sqlMode.HasNoZeroInDateMode(), Equals, t.hasNoZeroInDateMode)
		c.Assert(sqlMode.HasErrorForDivisionByZeroMode(), Equals, t.hasErrorForDivisionByZeroMode)
	}
}

func (s *testMyALLEGROSQLConstSuite) TestRealAsFloatMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a real);")
	result := tk.MustQuery("desc t")
	c.Check(result.Rows(), HasLen, 1)
	event := result.Rows()[0]
	c.Assert(event[1], Equals, "double")

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("set sql_mode='REAL_AS_FLOAT'")
	tk.MustInterDirc("create causet t (a real)")
	result = tk.MustQuery("desc t")
	c.Check(result.Rows(), HasLen, 1)
	event = result.Rows()[0]
	c.Assert(event[1], Equals, "float")
}

func (s *testMyALLEGROSQLConstSuite) TestPipesAsConcatMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("SET sql_mode='PIPES_AS_CONCAT';")
	r := tk.MustQuery(`SELECT 'hello' || 'world';`)
	r.Check(testkit.Rows("helloworld"))
}

func (s *testMyALLEGROSQLConstSuite) TestNoUnsignedSubtractionMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	ctx := context.Background()
	tk.MustInterDirc("set sql_mode='NO_UNSIGNED_SUBTRACTION'")
	r := tk.MustQuery("SELECT CAST(0 as UNSIGNED) - 1;")
	r.Check(testkit.Rows("-1"))
	rs, _ := tk.InterDirc("SELECT CAST(18446744073709551615 as UNSIGNED) - 1;")
	_, err := stochastik.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.InterDirc("SELECT 1 - CAST(18446744073709551615 as UNSIGNED);")
	_, err = stochastik.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.InterDirc("SELECT CAST(-1 as UNSIGNED) - 1")
	_, err = stochastik.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.InterDirc("SELECT CAST(9223372036854775808 as UNSIGNED) - 1")
	_, err = stochastik.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
}

func (s *testMyALLEGROSQLConstSuite) TestHighNotPrecedenceMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int);")
	tk.MustInterDirc("insert into t1 values (0),(1),(NULL);")
	r := tk.MustQuery(`SELECT * FROM t1 WHERE NOT a BETWEEN 2 AND 3;`)
	r.Check(testkit.Rows("0", "1"))
	r = tk.MustQuery(`SELECT NOT 1 BETWEEN -5 AND 5;`)
	r.Check(testkit.Rows("0"))
	tk.MustInterDirc("set sql_mode='high_not_precedence';")
	r = tk.MustQuery(`SELECT * FROM t1 WHERE NOT a BETWEEN 2 AND 3;`)
	r.Check(testkit.Rows())
	r = tk.MustQuery(`SELECT NOT 1 BETWEEN -5 AND 5;`)
	r.Check(testkit.Rows("1"))
}

func (s *testMyALLEGROSQLConstSuite) TestIgnoreSpaceMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("set sql_mode=''")
	tk.MustInterDirc("CREATE TABLE COUNT (a bigint);")
	tk.MustInterDirc("DROP TABLE COUNT;")
	tk.MustInterDirc("CREATE TABLE `COUNT` (a bigint);")
	tk.MustInterDirc("DROP TABLE COUNT;")
	_, err := tk.InterDirc("CREATE TABLE COUNT(a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE test.COUNT(a bigint);")
	tk.MustInterDirc("DROP TABLE COUNT;")

	tk.MustInterDirc("CREATE TABLE BIT_AND (a bigint);")
	tk.MustInterDirc("DROP TABLE BIT_AND;")
	tk.MustInterDirc("CREATE TABLE `BIT_AND` (a bigint);")
	tk.MustInterDirc("DROP TABLE BIT_AND;")
	_, err = tk.InterDirc("CREATE TABLE BIT_AND(a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE test.BIT_AND(a bigint);")
	tk.MustInterDirc("DROP TABLE BIT_AND;")

	tk.MustInterDirc("CREATE TABLE NOW (a bigint);")
	tk.MustInterDirc("DROP TABLE NOW;")
	tk.MustInterDirc("CREATE TABLE `NOW` (a bigint);")
	tk.MustInterDirc("DROP TABLE NOW;")
	_, err = tk.InterDirc("CREATE TABLE NOW(a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE test.NOW(a bigint);")
	tk.MustInterDirc("DROP TABLE NOW;")

	tk.MustInterDirc("set sql_mode='IGNORE_SPACE'")
	_, err = tk.InterDirc("CREATE TABLE COUNT (a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE `COUNT` (a bigint);")
	tk.MustInterDirc("DROP TABLE COUNT;")
	_, err = tk.InterDirc("CREATE TABLE COUNT(a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE test.COUNT(a bigint);")
	tk.MustInterDirc("DROP TABLE COUNT;")

	_, err = tk.InterDirc("CREATE TABLE BIT_AND (a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE `BIT_AND` (a bigint);")
	tk.MustInterDirc("DROP TABLE BIT_AND;")
	_, err = tk.InterDirc("CREATE TABLE BIT_AND(a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE test.BIT_AND(a bigint);")
	tk.MustInterDirc("DROP TABLE BIT_AND;")

	_, err = tk.InterDirc("CREATE TABLE NOW (a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE `NOW` (a bigint);")
	tk.MustInterDirc("DROP TABLE NOW;")
	_, err = tk.InterDirc("CREATE TABLE NOW(a bigint);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("CREATE TABLE test.NOW(a bigint);")
	tk.MustInterDirc("DROP TABLE NOW;")

}

func (s *testMyALLEGROSQLConstSuite) TestNoBackslashEscapesMode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("set sql_mode=''")
	r := tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\"))
	tk.MustInterDirc("set sql_mode='NO_BACKSLASH_ESCAPES'")
	r = tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\\\"))
}

func (s *testMyALLEGROSQLConstSuite) TestServerStatus(c *C) {
	tests := []struct {
		arg            uint16
		IsCursorExists bool
	}{
		{0, false},
		{allegrosql.ServerStatusInTrans | allegrosql.ServerStatusNoBackslashEscaped, false},
		{allegrosql.ServerStatusCursorExists, true},
		{allegrosql.ServerStatusCursorExists | allegrosql.ServerStatusLastRowSend, true},
	}

	for _, t := range tests {
		ret := allegrosql.HasCursorExistsFlag(t.arg)
		c.Assert(ret, Equals, t.IsCursorExists)
	}
}
