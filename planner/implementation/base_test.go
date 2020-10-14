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

package implementation

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/planner/memo"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testImplSuite{})

type testImplSuite struct {
	*BerolinaSQL.BerolinaSQL
	is   schemareplicant.SchemaReplicant
	sctx stochastikctx.Context
}

func (s *testImplSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{plannercore.MockSignedBlock()})
	s.sctx = plannercore.MockContext()
	s.BerolinaSQL = BerolinaSQL.New()
}

func (s *testImplSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testImplSuite) TestBaseImplementation(c *C) {
	p := plannercore.PhysicalLimit{}.Init(s.sctx, nil, 0, nil)
	impl := &baseImpl{plan: p}
	c.Assert(impl.GetPlan(), Equals, p)

	cost := impl.CalcCost(10, []memo.Implementation{}...)
	c.Assert(cost, Equals, 0.0)
	c.Assert(impl.GetCost(), Equals, 0.0)

	impl.SetCost(6.0)
	c.Assert(impl.GetCost(), Equals, 6.0)
}
