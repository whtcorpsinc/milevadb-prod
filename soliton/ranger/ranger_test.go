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

package ranger_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/whtcorpsinc/BerolinaSQL"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&testRangerSuite{})

type testRangerSuite struct {
	*BerolinaSQL.BerolinaSQL
	testData solitonutil.TestData
}

func (s *testRangerSuite) SetUpSuite(c *C) {
	s.BerolinaSQL = BerolinaSQL.New()
	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "ranger_suite")
	c.Assert(err, IsNil)
}

func (s *testRangerSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func newPetriStoreWithBootstrap(c *C) (*petri.Petri, ekv.CausetStorage, error) {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	dom, err := stochastik.BootstrapStochastik(causetstore)
	return dom, causetstore, errors.Trace(err)
}

func (s *testRangerSuite) TestBlockRange(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("create causet t(a int, b int, c int unsigned)")

	tests := []struct {
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			exprStr:     "a = 1",
			accessConds: "[eq(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
		},
		{
			exprStr:     "1 = a",
			accessConds: "[eq(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
		},
		{
			exprStr:     "a != 1",
			accessConds: "[ne(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
		},
		{
			exprStr:     "1 != a",
			accessConds: "[ne(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
		},
		{
			exprStr:     "a > 1",
			accessConds: "[gt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
		},
		{
			exprStr:     "1 < a",
			accessConds: "[lt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
		},
		{
			exprStr:     "a >= 1",
			accessConds: "[ge(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
		},
		{
			exprStr:     "1 <= a",
			accessConds: "[le(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
		},
		{
			exprStr:     "a < 1",
			accessConds: "[lt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			exprStr:     "1 > a",
			accessConds: "[gt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			exprStr:     "a <= 1",
			accessConds: "[le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
		},
		{
			exprStr:     "1 >= test.t.a",
			accessConds: "[ge(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
		},
		{
			exprStr:     "(a)",
			accessConds: "[test.t.a]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			exprStr:     "a in (1, 3, NULL, 2)",
			accessConds: "[in(test.t.a, 1, 3, <nil>, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3]]",
		},
		{
			exprStr:     `a IN (8,8,81,45)`,
			accessConds: "[in(test.t.a, 8, 8, 81, 45)]",
			filterConds: "[]",
			resultStr:   `[[8,8] [45,45] [81,81]]`,
		},
		{
			exprStr:     "a between 1 and 2",
			accessConds: "[ge(test.t.a, 1) le(test.t.a, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,2]]",
		},
		{
			exprStr:     "a not between 1 and 2",
			accessConds: "[or(lt(test.t.a, 1), gt(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (2,+inf]]",
		},
		{
			exprStr:     "a between 2 and 1",
			accessConds: "[ge(test.t.a, 2) le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			exprStr:     "a not between 2 and 1",
			accessConds: "[or(lt(test.t.a, 2), gt(test.t.a, 1))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			exprStr:     "a IS NULL",
			accessConds: "[isnull(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			exprStr:     "a IS NOT NULL",
			accessConds: "[not(isnull(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
		},
		{
			exprStr:     "a IS TRUE",
			accessConds: "[istrue(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			exprStr:     "a IS NOT TRUE",
			accessConds: "[not(istrue(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[0,0]]",
		},
		{
			exprStr:     "a IS FALSE",
			accessConds: "[isfalse(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[0,0]]",
		},
		{
			exprStr:     "a IS NOT FALSE",
			accessConds: "[not(isfalse(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
		},
		{
			exprStr:     "a = 1 or a = 3 or a = 4 or (a > 1 and (a = -1 or a = 5))",
			accessConds: "[or(or(eq(test.t.a, 1), eq(test.t.a, 3)), or(eq(test.t.a, 4), and(gt(test.t.a, 1), or(eq(test.t.a, -1), eq(test.t.a, 5)))))]",
			filterConds: "[]",
			resultStr:   "[[1,1] [3,3] [4,4] [5,5]]",
		},
		{
			exprStr:     "(a = 1 and b = 1) or (a = 2 and b = 2)",
			accessConds: "[or(eq(test.t.a, 1), eq(test.t.a, 2))]",
			filterConds: "[or(and(eq(test.t.a, 1), eq(test.t.b, 1)), and(eq(test.t.a, 2), eq(test.t.b, 2)))]",
			resultStr:   "[[1,1] [2,2]]",
		},
		{
			exprStr:     "a = 1 or a = 3 or a = 4 or (b > 1 and (a = -1 or a = 5))",
			accessConds: "[or(or(eq(test.t.a, 1), eq(test.t.a, 3)), or(eq(test.t.a, 4), or(eq(test.t.a, -1), eq(test.t.a, 5))))]",
			filterConds: "[or(or(or(eq(test.t.a, 1), eq(test.t.a, 3)), eq(test.t.a, 4)), and(gt(test.t.b, 1), or(eq(test.t.a, -1), eq(test.t.a, 5))))]",
			resultStr:   "[[-1,-1] [1,1] [3,3] [4,4] [5,5]]",
		},
		{
			exprStr:     "a in (1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)",
			accessConds: "[in(test.t.a, 1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3] [4,4]]",
		},
		{
			exprStr:     "a not in (1, 2, 3)",
			accessConds: "[not(in(test.t.a, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (3,+inf]]",
		},
		{
			exprStr:     "a > 9223372036854775807",
			accessConds: "[gt(test.t.a, 9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			exprStr:     "a >= 9223372036854775807",
			accessConds: "[ge(test.t.a, 9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[[9223372036854775807,+inf]]",
		},
		{
			exprStr:     "a < -9223372036854775807",
			accessConds: "[lt(test.t.a, -9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[[-inf,-9223372036854775807)]",
		},
		{
			exprStr:     "a < -9223372036854775808",
			accessConds: "[lt(test.t.a, -9223372036854775808)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		allegrosql := "select * from t where " + tt.exprStr
		sctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(sctx, allegrosql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := petri.GetPetri(sctx).SchemaReplicant()
		err = causetembedded.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, _, err := causetembedded.BuildLogicalCauset(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(causetembedded.LogicalCauset).Children()[0].(*causetembedded.LogicalSelection)
		conds := make([]memex.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = memex.PushDownNot(sctx, cond)
		}
		tbl := selection.Children()[0].(*causetembedded.DataSource).BlockInfo()
		defCaus := memex.DefCausInfo2DefCaus(selection.Schema().DeferredCausets, tbl.DeferredCausets[0])
		c.Assert(defCaus, NotNil)
		var filter []memex.Expression
		conds, filter = ranger.DetachCondsForDeferredCauset(sctx, conds, defCaus)
		c.Assert(fmt.Sprintf("%s", conds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		c.Assert(fmt.Sprintf("%s", filter), Equals, tt.filterConds, Commentf("wrong filter conditions for expr: %s", tt.exprStr))
		result, err := ranger.BuildBlockRange(conds, new(stmtctx.StatementContext), defCaus.RetType)
		c.Assert(err, IsNil, Commentf("failed to build causet range for expr %s", tt.exprStr))
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

func (s *testRangerSuite) TestIndexRange(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc(`
create causet t(
	a varchar(50),
	b int,
	c double,
	d varchar(10),
	e binary(10),
	f varchar(10) defCauslate utf8mb4_general_ci,
	index idx_ab(a(50), b),
	index idx_cb(c, a),
	index idx_d(d(2)),
	index idx_e(e(2)),
	index idx_f(f)
)`)

	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    0,
			exprStr:     `a LIKE 'abc%'`,
			accessConds: `[like(test.t.a, abc%, 92)]`,
			filterConds: "[]",
			resultStr:   "[[\"abc\",\"abd\")]",
		},
		{
			indexPos:    0,
			exprStr:     "a LIKE 'abc_'",
			accessConds: "[like(test.t.a, abc_, 92)]",
			filterConds: "[like(test.t.a, abc_, 92)]",
			resultStr:   "[(\"abc\",\"abd\")]",
		},
		{
			indexPos:    0,
			exprStr:     "a LIKE 'abc'",
			accessConds: "[eq(test.t.a, abc)]",
			filterConds: "[]",
			resultStr:   "[[\"abc\",\"abc\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "ab\_c"`,
			accessConds: "[eq(test.t.a, ab_c)]",
			filterConds: "[]",
			resultStr:   "[[\"ab_c\",\"ab_c\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE '%'`,
			accessConds: "[]",
			filterConds: `[like(test.t.a, %, 92)]`,
			resultStr:   "[[NULL,+inf]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE '\%a'`,
			accessConds: "[eq(test.t.a, %a)]",
			filterConds: "[]",
			resultStr:   `[["%a","%a"]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "\\"`,
			accessConds: "[eq(test.t.a, \\)]",
			filterConds: "[]",
			resultStr:   "[[\"\\\",\"\\\"]]",
		},
		{
			indexPos:    0,
			exprStr:     `a LIKE "\\\\a%"`,
			accessConds: `[like(test.t.a, \\a%, 92)]`,
			filterConds: "[]",
			resultStr:   "[[\"\\a\",\"\\b\")]",
		},
		{
			indexPos:    0,
			exprStr:     `a > NULL`,
			accessConds: "[gt(test.t.a, <nil>)]",
			filterConds: "[]",
			resultStr:   `[]`,
		},
		{
			indexPos:    0,
			exprStr:     `a = 'a' and b in (1, 2, 3)`,
			accessConds: "[eq(test.t.a, a) in(test.t.b, 1, 2, 3)]",
			filterConds: "[]",
			resultStr:   "[[\"a\" 1,\"a\" 1] [\"a\" 2,\"a\" 2] [\"a\" 3,\"a\" 3]]",
		},
		{
			indexPos:    0,
			exprStr:     `a = 'a' and b not in (1, 2, 3)`,
			accessConds: "[eq(test.t.a, a) not(in(test.t.b, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[(\"a\" NULL,\"a\" 1) (\"a\" 3,\"a\" +inf]]",
		},
		{
			indexPos:    0,
			exprStr:     `a in ('a') and b in ('1', 2.0, NULL)`,
			accessConds: "[eq(test.t.a, a) in(test.t.b, 1, 2, <nil>)]",
			filterConds: "[]",
			resultStr:   `[["a" 1,"a" 1] ["a" 2,"a" 2]]`,
		},
		{
			indexPos:    1,
			exprStr:     `c in ('1.1', 1, 1.1) and a in ('1', 'a', NULL)`,
			accessConds: "[in(test.t.c, 1.1, 1, 1.1) in(test.t.a, 1, a, <nil>)]",
			filterConds: "[]",
			resultStr:   "[[1 \"1\",1 \"1\"] [1 \"a\",1 \"a\"] [1.1 \"1\",1.1 \"1\"] [1.1 \"a\",1.1 \"a\"]]",
		},
		{
			indexPos:    1,
			exprStr:     "c in (1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)",
			accessConds: "[in(test.t.c, 1, 1, 1, 1, 1, 1, 2, 1, 2, 3, 2, 3, 4, 4, 1, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3] [4,4]]",
		},
		{
			indexPos:    1,
			exprStr:     "c not in (1, 2, 3)",
			accessConds: "[not(in(test.t.c, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[(NULL,1) (1,2) (2,3) (3,+inf]]",
		},
		{
			indexPos:    1,
			exprStr:     "c in (1, 2) and c in (1, 3)",
			accessConds: "[eq(test.t.c, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
		},
		{
			indexPos:    1,
			exprStr:     "c = 1 and c = 2",
			accessConds: "[]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "a in (NULL)",
			accessConds: "[eq(test.t.a, <nil>)]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "a not in (NULL, '1', '2', '3')",
			accessConds: "[not(in(test.t.a, <nil>, 1, 2, 3))]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "not (a not in (NULL, '1', '2', '3') and a > '2')",
			accessConds: "[or(in(test.t.a, <nil>, 1, 2, 3), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,\"2\"] [\"3\",\"3\"]]",
		},
		{
			indexPos:    0,
			exprStr:     "not (a not in (NULL) and a > '2')",
			accessConds: "[or(eq(test.t.a, <nil>), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,\"2\"]]",
		},
		{
			indexPos:    0,
			exprStr:     "not (a not in (NULL) or a > '2')",
			accessConds: "[and(eq(test.t.a, <nil>), le(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'b' and a < 'bbb') or (a < 'cb' and a > 'a')",
			accessConds: "[or(and(gt(test.t.a, b), lt(test.t.a, bbb)), and(lt(test.t.a, cb), gt(test.t.a, a)))]",
			filterConds: "[]",
			resultStr:   "[(\"a\",\"cb\")]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a' and a < 'b') or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[]",
			resultStr:   "[(\"a\",\"c\")]",
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a' and a < 'b' and b < 1) or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(gt(test.t.a, a), lt(test.t.a, b)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[or(and(and(gt(test.t.a, a), lt(test.t.a, b)), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			resultStr:   "[(\"a\",\"c\")]",
		},
		{
			indexPos:    0,
			exprStr:     "(a in ('a', 'b') and b < 1) or (a >= 'b' and a < 'c')",
			accessConds: "[or(and(in(test.t.a, a, b), lt(test.t.b, 1)), and(ge(test.t.a, b), lt(test.t.a, c)))]",
			filterConds: "[]",
			resultStr:   `[["a" -inf,"a" 1) ["b","c")]`,
		},
		{
			indexPos:    0,
			exprStr:     "(a > 'a') or (c > 1)",
			accessConds: "[]",
			filterConds: "[or(gt(test.t.a, a), gt(test.t.c, 1))]",
			resultStr:   "[[NULL,+inf]]",
		},
		{
			indexPos:    2,
			exprStr:     `d = "你好啊"`,
			accessConds: "[eq(test.t.d, 你好啊)]",
			filterConds: "[eq(test.t.d, 你好啊)]",
			resultStr:   "[[\"你好\",\"你好\"]]",
		},
		{
			indexPos:    3,
			exprStr:     `e = "你好啊"`,
			accessConds: "[eq(test.t.e, 你好啊)]",
			filterConds: "[eq(test.t.e, 你好啊)]",
			resultStr:   "[[\"[228 189]\",\"[228 189]\"]]",
		},
		{
			indexPos:    2,
			exprStr:     `d in ("你好啊", "再见")`,
			accessConds: "[in(test.t.d, 你好啊, 再见)]",
			filterConds: "[in(test.t.d, 你好啊, 再见)]",
			resultStr:   "[[\"你好\",\"你好\"] [\"再见\",\"再见\"]]",
		},
		{
			indexPos:    2,
			exprStr:     `d not in ("你好啊")`,
			accessConds: "[]",
			filterConds: "[ne(test.t.d, 你好啊)]",
			resultStr:   "[[NULL,+inf]]",
		},
		{
			indexPos:    2,
			exprStr:     `d < "你好" || d > "你好"`,
			accessConds: "[or(lt(test.t.d, 你好), gt(test.t.d, 你好))]",
			filterConds: "[or(lt(test.t.d, 你好), gt(test.t.d, 你好))]",
			resultStr:   "[[-inf,\"你好\") (\"你好\",+inf]]",
		},
		{
			indexPos:    2,
			exprStr:     `not(d < "你好" || d > "你好")`,
			accessConds: "[and(ge(test.t.d, 你好), le(test.t.d, 你好))]",
			filterConds: "[and(ge(test.t.d, 你好), le(test.t.d, 你好))]",
			resultStr:   "[[\"你好\",\"你好\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f >= 'a' and f <= 'B'",
			accessConds: "[ge(test.t.f, a) le(test.t.f, B)]",
			filterConds: "[]",
			resultStr:   "[[\"a\",\"B\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f in ('a', 'B')",
			accessConds: "[in(test.t.f, a, B)]",
			filterConds: "[]",
			resultStr:   "[[\"a\",\"a\"] [\"B\",\"B\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f = 'a' and f = 'B' defCauslate utf8mb4_bin",
			accessConds: "[eq(test.t.f, a)]",
			filterConds: "[eq(test.t.f, B)]",
			resultStr:   "[[\"a\",\"a\"]]",
		},
		{
			indexPos:    4,
			exprStr:     "f like '@%' defCauslate utf8mb4_bin",
			accessConds: "[]",
			filterConds: "[like(test.t.f, @%, 92)]",
			resultStr:   "[[NULL,+inf]]",
		},
	}

	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer func() { defCauslate.SetNewDefCauslationEnabledForTest(false) }()
	ctx := context.Background()
	for _, tt := range tests {
		allegrosql := "select * from t where " + tt.exprStr
		sctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(sctx, allegrosql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := petri.GetPetri(sctx).SchemaReplicant()
		err = causetembedded.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, _, err := causetembedded.BuildLogicalCauset(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(causetembedded.LogicalCauset).Children()[0].(*causetembedded.LogicalSelection)
		tbl := selection.Children()[0].(*causetembedded.DataSource).BlockInfo()
		c.Assert(selection, NotNil, Commentf("expr:%v", tt.exprStr))
		conds := make([]memex.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = memex.PushDownNot(sctx, cond)
		}
		defcaus, lengths := memex.IndexInfo2PrefixDefCauss(tbl.DeferredCausets, selection.Schema().DeferredCausets, tbl.Indices[tt.indexPos])
		c.Assert(defcaus, NotNil)
		res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, conds, defcaus, lengths)
		c.Assert(err, IsNil)
		c.Assert(fmt.Sprintf("%s", res.AccessConds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		c.Assert(fmt.Sprintf("%s", res.RemainedConds), Equals, tt.filterConds, Commentf("wrong filter conditions for expr: %s", tt.exprStr))
		got := fmt.Sprintf("%v", res.Ranges)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

// for issue #6661
func (s *testRangerSuite) TestIndexRangeForUnsignedInt(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("create causet t (a smallint(5) unsigned,key (a) )")

	tests := []struct {
		indexPos    int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
	}{
		{
			indexPos:    0,
			exprStr:     `a not in (0, 1, 2)`,
			accessConds: "[not(in(test.t.a, 0, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,0) (2,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (-1, 1, 2)`,
			accessConds: "[not(in(test.t.a, -1, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,1) (2,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (-2, -1, 1, 2)`,
			accessConds: "[not(in(test.t.a, -2, -1, 1, 2))]",
			filterConds: "[]",
			resultStr:   `[(NULL,1) (2,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (111)`,
			accessConds: "[ne(test.t.a, 111)]",
			filterConds: "[]",
			resultStr:   `[[-inf,111) (111,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a not in (1, 2, 9223372036854775810)`,
			accessConds: "[not(in(test.t.a, 1, 2, 9223372036854775810))]",
			filterConds: "[]",
			resultStr:   `[(NULL,1) (2,9223372036854775810) (9223372036854775810,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a >= -2147483648`,
			accessConds: "[ge(test.t.a, -2147483648)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a > -2147483648`,
			accessConds: "[gt(test.t.a, -2147483648)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			indexPos:    0,
			exprStr:     `a != -2147483648`,
			accessConds: "[ne(test.t.a, -2147483648)]",
			filterConds: "[]",
			resultStr:   `[[0,+inf]]`,
		},
		{
			exprStr:     "a < -1 or a < 1",
			accessConds: "[or(lt(test.t.a, -1), lt(test.t.a, 1))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
		},
		{
			exprStr:     "a < -1 and a < 1",
			accessConds: "[]",
			filterConds: "[]",
			resultStr:   "[]",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		allegrosql := "select * from t where " + tt.exprStr
		sctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(sctx, allegrosql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := petri.GetPetri(sctx).SchemaReplicant()
		err = causetembedded.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, _, err := causetembedded.BuildLogicalCauset(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(causetembedded.LogicalCauset).Children()[0].(*causetembedded.LogicalSelection)
		tbl := selection.Children()[0].(*causetembedded.DataSource).BlockInfo()
		c.Assert(selection, NotNil, Commentf("expr:%v", tt.exprStr))
		conds := make([]memex.Expression, len(selection.Conditions))
		for i, cond := range selection.Conditions {
			conds[i] = memex.PushDownNot(sctx, cond)
		}
		defcaus, lengths := memex.IndexInfo2PrefixDefCauss(tbl.DeferredCausets, selection.Schema().DeferredCausets, tbl.Indices[tt.indexPos])
		c.Assert(defcaus, NotNil)
		res, err := ranger.DetachCondAndBuildRangeForIndex(sctx, conds, defcaus, lengths)
		c.Assert(err, IsNil)
		c.Assert(fmt.Sprintf("%s", res.AccessConds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		c.Assert(fmt.Sprintf("%s", res.RemainedConds), Equals, tt.filterConds, Commentf("wrong filter conditions for expr: %s", tt.exprStr))
		got := fmt.Sprintf("%v", res.Ranges)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s", tt.exprStr))
	}
}

func (s *testRangerSuite) TestDeferredCausetRange(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("create causet t(a int, b double, c float(3, 2), d varchar(3), e bigint unsigned)")

	tests := []struct {
		defCausPos  int
		exprStr     string
		accessConds string
		filterConds string
		resultStr   string
		length      int
	}{
		{
			defCausPos:  0,
			exprStr:     "a = 1 and b > 1",
			accessConds: "[eq(test.t.a, 1)]",
			filterConds: "[gt(test.t.b, 1)]",
			resultStr:   "[[1,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  1,
			exprStr:     "b > 1",
			accessConds: "[gt(test.t.b, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "1 = a",
			accessConds: "[eq(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a != 1",
			accessConds: "[ne(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "1 != a",
			accessConds: "[ne(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a > 1",
			accessConds: "[gt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "1 < a",
			accessConds: "[lt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[(1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a >= 1",
			accessConds: "[ge(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "1 <= a",
			accessConds: "[le(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[1,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a < 1",
			accessConds: "[lt(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "1 > a",
			accessConds: "[gt(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1)]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a <= 1",
			accessConds: "[le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "1 >= a",
			accessConds: "[ge(1, test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "(a)",
			accessConds: "[test.t.a]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a in (1, 3, NULL, 2)",
			accessConds: "[in(test.t.a, 1, 3, <nil>, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2,2] [3,3]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     `a IN (8,8,81,45)`,
			accessConds: "[in(test.t.a, 8, 8, 81, 45)]",
			filterConds: "[]",
			resultStr:   `[[8,8] [45,45] [81,81]]`,
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a between 1 and 2",
			accessConds: "[ge(test.t.a, 1) le(test.t.a, 2)]",
			filterConds: "[]",
			resultStr:   "[[1,2]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a not between 1 and 2",
			accessConds: "[or(lt(test.t.a, 1), gt(test.t.a, 2))]",
			filterConds: "[]",
			resultStr:   "[[-inf,1) (2,+inf]]",
			length:      types.UnspecifiedLength,
		},
		//{
		// `a > null` will be converted to `castAsString(a) > null` which can not be extracted as access condition.
		//	exprStr:   "a not between null and 0",
		//	resultStr[(0,+inf]]
		//},
		{
			defCausPos:  0,
			exprStr:     "a between 2 and 1",
			accessConds: "[ge(test.t.a, 2) le(test.t.a, 1)]",
			filterConds: "[]",
			resultStr:   "[]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a not between 2 and 1",
			accessConds: "[or(lt(test.t.a, 2), gt(test.t.a, 1))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a IS NULL",
			accessConds: "[isnull(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a IS NOT NULL",
			accessConds: "[not(isnull(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[-inf,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a IS TRUE",
			accessConds: "[istrue(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[-inf,0) (0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a IS NOT TRUE",
			accessConds: "[not(istrue(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[NULL,NULL] [0,0]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a IS FALSE",
			accessConds: "[isfalse(test.t.a)]",
			filterConds: "[]",
			resultStr:   "[[0,0]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a IS NOT FALSE",
			accessConds: "[not(isfalse(test.t.a))]",
			filterConds: "[]",
			resultStr:   "[[NULL,0) (0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  1,
			exprStr:     `b in (1, '2.1')`,
			accessConds: "[in(test.t.b, 1, 2.1)]",
			filterConds: "[]",
			resultStr:   "[[1,1] [2.1,2.1]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     `a > 9223372036854775807`,
			accessConds: "[gt(test.t.a, 9223372036854775807)]",
			filterConds: "[]",
			resultStr:   "[(9223372036854775807,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  2,
			exprStr:     `c > 111.11111111`,
			accessConds: "[gt(test.t.c, 111.11111111)]",
			filterConds: "[]",
			resultStr:   "[[111.111115,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  3,
			exprStr:     `d > 'aaaaaaaaaaaaaa'`,
			accessConds: "[gt(test.t.d, aaaaaaaaaaaaaa)]",
			filterConds: "[]",
			resultStr:   "[(\"aaaaaaaaaaaaaa\",+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  4,
			exprStr:     `e > 18446744073709500000`,
			accessConds: "[gt(test.t.e, 18446744073709500000)]",
			filterConds: "[]",
			resultStr:   "[(18446744073709500000,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  4,
			exprStr:     `e > -2147483648`,
			accessConds: "[gt(test.t.e, -2147483648)]",
			filterConds: "[]",
			resultStr:   "[[0,+inf]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  3,
			exprStr:     "d = 'aab' or d = 'aac'",
			accessConds: "[or(eq(test.t.d, aab), eq(test.t.d, aac))]",
			filterConds: "[]",
			resultStr:   "[[\"a\",\"a\"]]",
			length:      1,
		},
		// This test case cannot be simplified to [1, 3] otherwise the index join will executes wrongly.
		{
			defCausPos:  0,
			exprStr:     "a in (1, 2, 3)",
			accessConds: "[in(test.t.a, 1, 2, 3)]",
			filterConds: "",
			resultStr:   "[[1,1] [2,2] [3,3]]",
			length:      types.UnspecifiedLength,
		},
		// test cases for nulleq
		{
			defCausPos:  0,
			exprStr:     "a <=> 1",
			accessConds: "[nulleq(test.t.a, 1)]",
			filterConds: "",
			resultStr:   "[[1,1]]",
			length:      types.UnspecifiedLength,
		},
		{
			defCausPos:  0,
			exprStr:     "a <=> null",
			accessConds: "[nulleq(test.t.a, <nil>)]",
			filterConds: "",
			resultStr:   "[[NULL,NULL]]",
			length:      types.UnspecifiedLength,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		allegrosql := "select * from t where " + tt.exprStr
		sctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(sctx, allegrosql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := petri.GetPetri(sctx).SchemaReplicant()
		err = causetembedded.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, _, err := causetembedded.BuildLogicalCauset(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		sel := p.(causetembedded.LogicalCauset).Children()[0].(*causetembedded.LogicalSelection)
		ds, ok := sel.Children()[0].(*causetembedded.DataSource)
		c.Assert(ok, IsTrue, Commentf("expr:%v", tt.exprStr))
		conds := make([]memex.Expression, len(sel.Conditions))
		for i, cond := range sel.Conditions {
			conds[i] = memex.PushDownNot(sctx, cond)
		}
		defCaus := memex.DefCausInfo2DefCaus(sel.Schema().DeferredCausets, ds.BlockInfo().DeferredCausets[tt.defCausPos])
		c.Assert(defCaus, NotNil)
		conds = ranger.ExtractAccessConditionsForDeferredCauset(conds, defCaus.UniqueID)
		c.Assert(fmt.Sprintf("%s", conds), Equals, tt.accessConds, Commentf("wrong access conditions for expr: %s", tt.exprStr))
		result, err := ranger.BuildDeferredCausetRange(conds, new(stmtctx.StatementContext), defCaus.RetType, tt.length)
		c.Assert(err, IsNil)
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, tt.resultStr, Commentf("different for expr %s, defCaus: %v", tt.exprStr, defCaus))
	}
}

func (s *testRangerSuite) TestIndexRangeElimininatedProjection(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("set @@milevadb_enable_clustered_index=0")
	testKit.MustInterDirc("create causet t(a int not null, b int not null, primary key(a,b))")
	testKit.MustInterDirc("insert into t values(1,2)")
	testKit.MustInterDirc("analyze causet t")
	testKit.MustQuery("explain select * from (select * from t union all select ifnull(a,b), b from t) sub where a > 0").Check(testkit.Rows(
		"Union_11 2.00 root  ",
		"├─IndexReader_14 1.00 root  index:IndexRangeScan_13",
		"│ └─IndexRangeScan_13 1.00 cop[einsteindb] causet:t, index:PRIMARY(a, b) range:(0,+inf], keep order:false",
		"└─IndexReader_17 1.00 root  index:IndexRangeScan_16",
		"  └─IndexRangeScan_16 1.00 cop[einsteindb] causet:t, index:PRIMARY(a, b) range:(0,+inf], keep order:false",
	))
	testKit.MustQuery("select * from (select * from t union all select ifnull(a,b), b from t) sub where a > 0").Check(testkit.Rows(
		"1 2",
		"1 2",
	))
}

func (s *testRangerSuite) TestCompIndexInExprCorrDefCaus(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("create causet t(a int primary key, b int, c int, d int, e int, index idx(b,c,d))")
	testKit.MustInterDirc("insert into t values(1,1,1,1,2),(2,1,2,1,0)")
	testKit.MustInterDirc("analyze causet t")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testRangerSuite) TestIndexStringIsTrueRange(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t0")
	testKit.MustInterDirc("CREATE TABLE t0(c0 TEXT(10));")
	testKit.MustInterDirc("INSERT INTO t0(c0) VALUES (1);")
	testKit.MustInterDirc("CREATE INDEX i0 ON t0(c0(10));")
	testKit.MustInterDirc("analyze causet t0;")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Result = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testRangerSuite) TestCompIndexDNFMatch(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("create causet t(a int, b int, c int, key(a,b,c));")
	testKit.MustInterDirc("insert into t values(1,2,2)")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = s.testData.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Causet...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testRangerSuite) TestCompIndexMultiDefCausDNF1(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("create causet t(a int, b int, c int, primary key(a,b));")
	testKit.MustInterDirc("insert into t values(1,1,1),(2,2,3)")
	testKit.MustInterDirc("analyze causet t")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = s.testData.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Causet...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testRangerSuite) TestCompIndexMultiDefCausDNF2(c *C) {
	defer testleak.AfterTest(c)()
	dom, causetstore, err := newPetriStoreWithBootstrap(c)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("drop causet if exists t")
	testKit.MustInterDirc("create causet t(a int, b int, c int, primary key(a,b,c));")
	testKit.MustInterDirc("insert into t values(1,1,1),(2,2,3)")
	testKit.MustInterDirc("analyze causet t")

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            []string
		Result            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = s.testData.ConvertRowsToStrings(testKit.MustQuery("explain " + tt).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Causet...))
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}
