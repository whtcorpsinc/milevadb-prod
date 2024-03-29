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

package embedded_test

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

var _ = Suite(&testCausetSuite{})
var _ = SerialSuites(&testCausetSerialSuite{})

type testCausetSuiteBase struct {
	*BerolinaSQL.BerolinaSQL
	is schemareplicant.SchemaReplicant
}

func (s *testCausetSuiteBase) SetUpSuite(c *C) {
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{embedded.MockSignedBlock(), embedded.MockUnsignedBlock()})
	s.BerolinaSQL = BerolinaSQL.New()
	s.BerolinaSQL.EnableWindowFunc(true)
}

type testCausetSerialSuite struct {
	testCausetSuiteBase
}

type testCausetSuite struct {
	testCausetSuiteBase

	testData solitonutil.TestData
}

func (s *testCausetSuite) SetUpSuite(c *C) {
	s.testCausetSuiteBase.SetUpSuite(c)

	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "plan_suite")
	c.Assert(err, IsNil)
}

func (s *testCausetSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testCausetSuite) TestPosetDagCausetBuilderSimpleCase(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderJoin(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	ctx := se.(stochastikctx.Context)
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.InterlockingDirectorateConcurrency = 4
	stochastikVars.SetDistALLEGROSQLScanConcurrency(15)
	stochastikVars.SetHashJoinConcurrency(5)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderSubquery(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.InterDircute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	ctx := se.(stochastikctx.Context)
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.SetHashAggFinalConcurrency(1)
	stochastikVars.SetHashAggPartialConcurrency(1)
	stochastikVars.SetHashJoinConcurrency(5)
	stochastikVars.SetDistALLEGROSQLScanConcurrency(15)
	stochastikVars.InterlockingDirectorateConcurrency = 4
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testCausetSuite) TestPosetDagCausetTopN(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderBasePhysicalCauset(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)

	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		embedded.Preprocess(se, stmt, s.is)
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p))
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
		c.Assert(hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p)), Equals, output[i].Hints, Commentf("for %s", tt))
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderUnion(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		// Make txn not read only.
		txn, err := se.Txn(true)
		c.Assert(err, IsNil)
		txn.Set(ekv.Key("AAA"), []byte("BBB"))
		se.StmtCommit()
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderAgg(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	se.InterDircute(context.Background(), "use test")
	se.InterDircute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	ctx := se.(stochastikctx.Context)
	stochastikVars := ctx.GetStochastikVars()
	stochastikVars.SetHashAggFinalConcurrency(1)
	stochastikVars.SetHashAggPartialConcurrency(1)
	stochastikVars.SetDistALLEGROSQLScanConcurrency(15)
	stochastikVars.InterlockingDirectorateConcurrency = 4

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

func (s *testCausetSuite) TestRefine(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(stochastikctx.Context).GetStochastikVars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testCausetSuite) TestAggEliminator(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.InterDircute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("for %s", tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(stochastikctx.Context).GetStochastikVars().StmtCtx
		sc.IgnoreTruncate = false
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, Commentf("for %s", tt))
	}
}

type overrideStore struct{ ekv.CausetStorage }

func (causetstore overrideStore) GetClient() ekv.Client {
	cli := causetstore.CausetStorage.GetClient()
	return overrideClient{cli}
}

type overrideClient struct{ ekv.Client }

func (cli overrideClient) IsRequestTypeSupported(reqType, subType int64) bool {
	return false
}

func (s *testCausetSuite) TestRequestTypeSupportedOff(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(overrideStore{causetstore})
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	allegrosql := "select * from t where a in (1, 10, 20)"
	expect := "BlockReader(Block(t))->Sel([in(test.t.a, 1, 10, 20)])"

	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)
	p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(embedded.ToString(p), Equals, expect, Commentf("for %s", allegrosql))
}

func (s *testCausetSuite) TestIndexJoinUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustInterDirc("use test")
	var input [][]string
	var output []struct {
		ALLEGROALLEGROSQL []string
		Causet            []string
	}
	tk.MustInterDirc("create causet t (a int primary key, b int, index idx(a))")
	tk.MustInterDirc("create causet tt (a int primary key) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))")

	tk.MustInterDirc(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		tk.MustInterDirc("begin")
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustInterDirc(tt)
			}
			s.testData.OnRecord(func() {
				output[i].ALLEGROALLEGROSQL = ts
				if j == len(ts)-1 {
					output[i].Causet = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Causet...))
			}
		}
		tk.MustInterDirc("rollback")
	}
}

func (s *testCausetSuite) TestDoSubquery(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		allegrosql string
		best       string
	}{
		{
			allegrosql: "do 1 in (select a from t)",
			best:       "LeftHashJoin{Dual->PointGet(Handle(t.a)1)}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(embedded.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testCausetSuite) TestIndexLookupCartesianJoin(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	allegrosql := "select /*+ MilevaDB_INLJ(t1, t2) */ * from t t1 join t t2"
	stmt, err := s.ParseOneStmt(allegrosql, "", "")
	c.Assert(err, IsNil)
	p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(embedded.ToString(p), Equals, "LeftHashJoin{BlockReader(Block(t))->BlockReader(Block(t))}")
	warnings := se.GetStochastikVars().StmtCtx.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	err = embedded.ErrInternal.GenWithStack("MilevaDB_INLJ hint is inapplicable without column equal ON condition")
	c.Assert(terror.ErrorEqual(err, lastWarn.Err), IsTrue)
}

func (s *testCausetSuite) TestSemiJoinToInner(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil)
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best)
	}
}

func (s *testCausetSuite) TestUnmatchedBlockInHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Warning           string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, test := range input {
		se.GetStochastikVars().StmtCtx.SetWarnings(nil)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil)
		_, _, err = causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetStochastikVars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning)
		}
	}
}

func (s *testCausetSuite) TestHintScope(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(context.Background(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best)

		warnings := se.GetStochastikVars().StmtCtx.GetWarnings()
		c.Assert(warnings, HasLen, 0, comment)
	}
}

func (s *testCausetSuite) TestJoinHints(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		Warning           string
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		se.GetStochastikVars().StmtCtx.SetWarnings(nil)
		p, _, err := causet.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetStochastikVars().StmtCtx.GetWarnings()

		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = embedded.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
			output[i].Hints = hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p))
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best)
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1, Commentf("%v", warnings))
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning)
		}
		c.Assert(hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testCausetSuite) TestAggregationHints(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	stochastikVars := se.(stochastikctx.Context).GetStochastikVars()
	stochastikVars.SetHashAggFinalConcurrency(1)
	stochastikVars.SetHashAggPartialConcurrency(1)

	var input []struct {
		ALLEGROALLEGROSQL string
		AggPushDown       bool
	}
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		Warning           string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		se.GetStochastikVars().StmtCtx.SetWarnings(nil)
		se.GetStochastikVars().AllowAggPushDown = test.AggPushDown

		stmt, err := s.ParseOneStmt(test.ALLEGROALLEGROSQL, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetStochastikVars().StmtCtx.GetWarnings()

		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test.ALLEGROALLEGROSQL
			output[i].Best = embedded.ToString(p)
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testCausetSuite) TestAggToCopHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists ta")
	tk.MustInterDirc("create causet ta(a int, b int, index(a))")

	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Best              string
			Warning           string
		}
	)
	s.testData.GetTestCases(c, &input, &output)

	ctx := context.Background()
	is := petri.GetPetri(tk.Se).SchemaReplicant()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
		})
		c.Assert(test, Equals, output[i].ALLEGROALLEGROSQL, comment)

		tk.Se.GetStochastikVars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(ctx, tk.Se, stmt, is)
		c.Assert(err, IsNil)
		planString := embedded.ToString(p)
		s.testData.OnRecord(func() {
			output[i].Best = planString
		})
		c.Assert(planString, Equals, output[i].Best, comment)

		warnings := tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testCausetSuite) TestLimitToCopHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists tn")
	tk.MustInterDirc("create causet tn(a int, b int, c int, d int, key (a, b, c, d))")

	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Causet            []string
			Warning           string
		}
	)

	s.testData.GetTestCases(c, &input, &output)

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Causet = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Causet...))

		comment := Commentf("case:%v allegrosql:%s", i, ts)
		warnings := tk.Se.GetStochastikVars().StmtCtx.GetWarnings()
		s.testData.OnRecord(func() {
			if len(warnings) > 0 {
				output[i].Warning = warnings[0].Err.Error()
			}
		})
		if output[i].Warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, output[i].Warning, comment)
		}
	}
}

func (s *testCausetSuite) TestPushdownDistinctEnable(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Causet            []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 1", variable.MilevaDBOptDistinctAggPushDown),
		"set stochastik milevadb_opt_agg_push_down = 1",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testCausetSuite) TestPushdownDistinctDisable(c *C) {
	defer testleak.AfterTest(c)()
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Causet            []string
			Result            []string
		}
	)

	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 0", variable.MilevaDBOptDistinctAggPushDown),
		"set stochastik milevadb_opt_agg_push_down = 1",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testCausetSuite) TestPushdownDistinctEnableAggPushDownDisable(c *C) {
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Causet            []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		fmt.Sprintf("set @@stochastik.%s = 1", variable.MilevaDBOptDistinctAggPushDown),
		"set stochastik milevadb_opt_agg_push_down = 0",
	}
	s.doTestPushdownDistinct(c, vars, input, output)
}

func (s *testCausetSuite) doTestPushdownDistinct(c *C, vars, input []string, output []struct {
	ALLEGROALLEGROSQL string
	Causet            []string
	Result            []string
}) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int, index(c))")
	tk.MustInterDirc("insert into t values (1, 1, 1), (1, 1, 3), (1, 2, 3), (2, 1, 3), (1, 2, NULL);")

	tk.MustInterDirc("drop causet if exists pt")
	tk.MustInterDirc(`CREATE TABLE pt (a int, b int) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (2),
		PARTITION p1 VALUES LESS THAN (100)
	);`)

	tk.MustInterDirc("drop causet if exists ta")
	tk.MustInterDirc("create causet ta(a int);")
	tk.MustInterDirc("insert into ta values(1), (1);")
	tk.MustInterDirc("drop causet if exists tb")
	tk.MustInterDirc("create causet tb(a int);")
	tk.MustInterDirc("insert into tb values(1), (1);")

	tk.MustInterDirc("set stochastik sql_mode=''")
	tk.MustInterDirc(fmt.Sprintf("set stochastik %s=1", variable.MilevaDBHashAggPartialConcurrency))
	tk.MustInterDirc(fmt.Sprintf("set stochastik %s=1", variable.MilevaDBHashAggFinalConcurrency))

	tk.MustInterDirc(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	for _, v := range vars {
		tk.MustInterDirc(v)
	}

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Causet = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Causet...))
		tk.MustQuery(ts).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testCausetSuite) TestGroupConcatOrderby(c *C) {
	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL string
			Causet            []string
			Result            []string
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists test;")
	tk.MustInterDirc("create causet test(id int, name int)")
	tk.MustInterDirc("insert into test values(1, 10);")
	tk.MustInterDirc("insert into test values(1, 20);")
	tk.MustInterDirc("insert into test values(1, 30);")
	tk.MustInterDirc("insert into test values(2, 20);")
	tk.MustInterDirc("insert into test values(3, 200);")
	tk.MustInterDirc("insert into test values(3, 500);")

	tk.MustInterDirc("drop causet if exists ptest;")
	tk.MustInterDirc("CREATE TABLE ptest (id int,name int) PARTITION BY RANGE ( id ) " +
		"(PARTITION `p0` VALUES LESS THAN (2), PARTITION `p1` VALUES LESS THAN (11))")
	tk.MustInterDirc("insert into ptest select * from test;")
	tk.MustInterDirc(fmt.Sprintf("set stochastik milevadb_opt_distinct_agg_push_down = %v", 1))
	tk.MustInterDirc(fmt.Sprintf("set stochastik milevadb_opt_agg_push_down = %v", 1))

	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Causet = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Sort().Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Causet...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testCausetSuite) TestHintAlias(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	tests := []struct {
		sql1 string
		sql2 string
	}{
		{
			sql1: "select /*+ MilevaDB_SMJ(t1) */ t1.a, t1.b from t t1, (select /*+ MilevaDB_INLJ(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ MERGE_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ INL_JOIN(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ MilevaDB_HJ(t1) */ t1.a, t1.b from t t1, (select /*+ MilevaDB_SMJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ HASH_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ MERGE_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ MilevaDB_INLJ(t1) */ t1.a, t1.b from t t1, (select /*+ MilevaDB_HJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ INL_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ HASH_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql1:%s sql2:%s", i, tt.sql1, tt.sql2)
		stmt1, err := s.ParseOneStmt(tt.sql1, "", "")
		c.Assert(err, IsNil, comment)
		stmt2, err := s.ParseOneStmt(tt.sql2, "", "")
		c.Assert(err, IsNil, comment)

		p1, _, err := causet.Optimize(ctx, se, stmt1, s.is)
		c.Assert(err, IsNil)
		p2, _, err := causet.Optimize(ctx, se, stmt2, s.is)
		c.Assert(err, IsNil)

		c.Assert(embedded.ToString(p1), Equals, embedded.ToString(p2))
	}
}

func (s *testCausetSuite) TestIndexHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		HasWarn           bool
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		se.GetStochastikVars().StmtCtx.SetWarnings(nil)

		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = embedded.ToString(p)
			output[i].HasWarn = len(se.GetStochastikVars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p))
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
		warnings := se.GetStochastikVars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
		c.Assert(hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testCausetSuite) TestIndexMergeHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
		HasWarn           bool
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.Background()
	for i, test := range input {
		comment := Commentf("case:%v allegrosql:%s", i, test)
		se.GetStochastikVars().StmtCtx.SetWarnings(nil)
		stmt, err := s.ParseOneStmt(test, "", "")
		c.Assert(err, IsNil, comment)
		sctx := se.(stochastikctx.Context)
		err = interlock.ResetContextOfStmt(sctx, stmt)
		c.Assert(err, IsNil)
		p, _, err := causet.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = test
			output[i].Best = embedded.ToString(p)
			output[i].HasWarn = len(se.GetStochastikVars().StmtCtx.GetWarnings()) > 0
			output[i].Hints = hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p))
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
		warnings := se.GetStochastikVars().StmtCtx.GetWarnings()
		if output[i].HasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
		c.Assert(hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testCausetSuite) TestQueryBlockHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            string
		Hints             string
	}
	s.testData.GetTestCases(c, &input, &output)
	ctx := context.TODO()
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = embedded.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p))
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Causet, comment)
		c.Assert(hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testCausetSuite) TestInlineProjection(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.InterDircute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `drop causet if exists test.t1, test.t2;`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `create causet test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `create causet test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            string
		Hints             string
	}
	is := petri.GetPetri(se).SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		p, _, err := causet.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = embedded.ToString(p)
			output[i].Hints = hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p))
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Causet, comment)
		c.Assert(hint.RestoreOptimizerHints(embedded.GenHintsFromPhysicalCauset(p)), Equals, output[i].Hints, comment)
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderSplitAvg(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		allegrosql string
		plan       string
	}{
		{
			allegrosql: "select avg(a),avg(b),avg(c) from t",
			plan:       "BlockReader(Block(t)->StreamAgg)->StreamAgg",
		},
		{
			allegrosql: "select /*+ HASH_AGG() */ avg(a),avg(b),avg(c) from t",
			plan:       "BlockReader(Block(t)->HashAgg)->HashAgg",
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.allegrosql)
		stmt, err := s.ParseOneStmt(tt.allegrosql, "", "")
		c.Assert(err, IsNil, comment)

		embedded.Preprocess(se, stmt, s.is)
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		c.Assert(embedded.ToString(p), Equals, tt.plan, comment)
		root, ok := p.(embedded.PhysicalCauset)
		if !ok {
			continue
		}
		testPosetDagCausetBuilderSplitAvg(c, root)
	}
}

func testPosetDagCausetBuilderSplitAvg(c *C, root embedded.PhysicalCauset) {
	if p, ok := root.(*embedded.PhysicalBlockReader); ok {
		if p.BlockCausets != nil {
			baseAgg := p.BlockCausets[len(p.BlockCausets)-1]
			if agg, ok := baseAgg.(*embedded.PhysicalHashAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().DeferredCausets[i].RetType, Equals, aggfunc.RetTp)
				}
			}
			if agg, ok := baseAgg.(*embedded.PhysicalStreamAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().DeferredCausets[i].RetType, Equals, aggfunc.RetTp)
				}
			}
		}
	}

	childs := root.Children()
	if childs == nil {
		return
	}
	for _, son := range childs {
		testPosetDagCausetBuilderSplitAvg(c, son)
	}
}

func (s *testCausetSuite) TestIndexJoinHint(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.InterDircute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `drop causet if exists test.t1, test.t2, test.t;`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `create causet test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `create causet test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, "CREATE TABLE `t` ( `a` bigint(20) NOT NULL, `b` tinyint(1) DEFAULT NULL, `c` datetime DEFAULT NULL, `d` int(10) unsigned DEFAULT NULL, `e` varchar(20) DEFAULT NULL, `f` double DEFAULT NULL, `g` decimal(30,5) DEFAULT NULL, `h` float DEFAULT NULL, `i` date DEFAULT NULL, `j` timestamp NULL DEFAULT NULL, PRIMARY KEY (`a`), UNIQUE KEY `b` (`b`), KEY `c` (`c`,`d`,`e`), KEY `f` (`f`), KEY `g` (`g`,`h`), KEY `g_2` (`g`), UNIQUE KEY `g_3` (`g`), KEY `i` (`i`) );")
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            string
	}
	is := petri.GetPetri(se).SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := causet.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Causet, comment)
	}
}

func (s *testCausetSuite) TestPosetDagCausetBuilderWindow(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		"set @@stochastik.milevadb_window_concurrency = 1",
	}
	s.doTestPosetDagCausetBuilderWindow(c, vars, input, output)
}

func (s *testCausetSuite) TestPosetDagCausetBuilderWindowParallel(c *C) {
	defer testleak.AfterTest(c)()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Best              string
	}
	s.testData.GetTestCases(c, &input, &output)
	vars := []string{
		"set @@stochastik.milevadb_window_concurrency = 4",
	}
	s.doTestPosetDagCausetBuilderWindow(c, vars, input, output)
}

func (s *testCausetSuite) doTestPosetDagCausetBuilderWindow(c *C, vars, input []string, output []struct {
	ALLEGROALLEGROSQL string
	Best              string
}) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.InterDircute(ctx, "use test")
	c.Assert(err, IsNil)

	for _, v := range vars {
		_, err = se.InterDircute(ctx, v)
		c.Assert(err, IsNil)
	}

	for i, tt := range input {
		comment := Commentf("case:%v allegrosql:%s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, _, err := causet.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Best = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Best, comment)
	}
}

func (s *testCausetSuite) TestNominalSort(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            []string
		Result            []string
	}
	tk.MustInterDirc("create causet t (a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustInterDirc("insert into t values(1, 1)")
	tk.MustInterDirc("insert into t values(1, 2)")
	tk.MustInterDirc("insert into t values(2, 4)")
	tk.MustInterDirc("insert into t values(3, 5)")
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Causet = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
			output[i].Result = s.testData.ConvertRowsToStrings(tk.MustQuery(ts).Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Causet...))
		tk.MustQuery(ts).Check(testkit.Rows(output[i].Result...))
	}
}

func (s *testCausetSuite) TestHintFromDiffDatabase(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.InterDircute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `drop causet if exists test.t1`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `create causet test.t1(a bigint, index idx_a(a));`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `create causet test.t2(a bigint, index idx_a(a));`)
	c.Assert(err, IsNil)

	_, err = se.InterDircute(ctx, "drop database if exists test2")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, "create database test2")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, "use test2")
	c.Assert(err, IsNil)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            string
	}
	is := petri.GetPetri(se).SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		p, _, err := causet.Optimize(ctx, se, stmt, is)
		c.Assert(err, IsNil, comment)
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Causet = embedded.ToString(p)
		})
		c.Assert(embedded.ToString(p), Equals, output[i].Causet, comment)
	}
}

func (s *testCausetSuite) TestNthCausetHintWithExplain(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = se.InterDircute(ctx, "use test")
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `drop causet if exists test.tt`)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(ctx, `create causet test.tt (a int,b int, index(a), index(b));`)
	c.Assert(err, IsNil)

	_, err = se.InterDircute(ctx, "insert into tt values (1, 1), (2, 2), (3, 4)")
	c.Assert(err, IsNil)

	tk.MustInterDirc(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)

	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		Causet            []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = ts
			output[i].Causet = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + ts).Rows())
		})
		tk.MustQuery("explain " + ts).Check(testkit.Rows(output[i].Causet...))
	}

	// This assert makes sure a query with or without nth_plan() hint output exactly the same plan(including plan ID).
	// The query below is the same as queries in the testdata except for nth_plan() hint.
	// Currently its output is the same as the second test case in the testdata, which is `output[1]`. If this doesn't
	// hold in the future, you may need to modify this.
	tk.MustQuery("explain select * from test.tt where a=1 and b=1").Check(testkit.Rows(output[1].Causet...))
}
