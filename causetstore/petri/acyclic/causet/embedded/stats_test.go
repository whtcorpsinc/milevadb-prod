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

	"github.com/whtcorpsinc/BerolinaSQL"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

var _ = Suite(&testStatsSuite{})

type testStatsSuite struct {
	*BerolinaSQL.BerolinaSQL
	testData solitonutil.TestData
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	s.BerolinaSQL = BerolinaSQL.New()
	s.BerolinaSQL.EnableWindowFunc(true)

	var err error
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "stats_suite")
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testStatsSuite) TestGroupNDVs(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int not null, b int not null, key(a,b))")
	tk.MustInterDirc("insert into t1 values(1,1),(1,2),(2,1),(2,2),(1,1)")
	tk.MustInterDirc("create causet t2(a int not null, b int not null, key(a,b))")
	tk.MustInterDirc("insert into t2 values(1,1),(1,2),(1,3),(2,1),(2,2),(2,3),(3,1),(3,2),(3,3),(1,1)")
	tk.MustInterDirc("analyze causet t1")
	tk.MustInterDirc("analyze causet t2")

	ctx := context.Background()
	var input []string
	var output []struct {
		ALLEGROALLEGROSQL string
		AggInput          string
		JoinInput         string
	}
	is := dom.SchemaReplicant()
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		comment := Commentf("case:%v allegrosql: %s", i, tt)
		stmt, err := s.ParseOneStmt(tt, "", "")
		c.Assert(err, IsNil, comment)
		embedded.Preprocess(tk.Se, stmt, is)
		builder := embedded.NewCausetBuilder(tk.Se, is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil, comment)
		p, err = embedded.LogicalOptimize(ctx, builder.GetOptFlag(), p.(embedded.LogicalCauset))
		c.Assert(err, IsNil, comment)
		lp := p.(embedded.LogicalCauset)
		_, err = embedded.RecursiveDeriveStats4Test(lp)
		c.Assert(err, IsNil, comment)
		var agg *embedded.LogicalAggregation
		var join *embedded.LogicalJoin
		stack := make([]embedded.LogicalCauset, 0, 2)
		traversed := false
		for !traversed {
			switch v := lp.(type) {
			case *embedded.LogicalAggregation:
				agg = v
				lp = lp.Children()[0]
			case *embedded.LogicalJoin:
				join = v
				lp = v.Children()[0]
				stack = append(stack, v.Children()[1])
			case *embedded.LogicalApply:
				lp = lp.Children()[0]
				stack = append(stack, v.Children()[1])
			case *embedded.LogicalUnionAll:
				lp = lp.Children()[0]
				for i := 1; i < len(v.Children()); i++ {
					stack = append(stack, v.Children()[i])
				}
			case *embedded.DataSource:
				if len(stack) == 0 {
					traversed = true
				} else {
					lp = stack[0]
					stack = stack[1:]
				}
			default:
				lp = lp.Children()[0]
			}
		}
		aggInput := ""
		joinInput := ""
		if agg != nil {
			s := embedded.GetStats4Test(agg.Children()[0])
			aggInput = property.ToString(s.GroupNDVs)
		}
		if join != nil {
			l := embedded.GetStats4Test(join.Children()[0])
			r := embedded.GetStats4Test(join.Children()[1])
			joinInput = property.ToString(l.GroupNDVs) + ";" + property.ToString(r.GroupNDVs)
		}
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].AggInput = aggInput
			output[i].JoinInput = joinInput
		})
		c.Assert(aggInput, Equals, output[i].AggInput, comment)
		c.Assert(joinInput, Equals, output[i].JoinInput, comment)
	}
}
