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

package memo

import (
	. "github.com/whtcorpsinc/check"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
)

func (s *testMemoSuite) TestGetOperand(c *C) {
	c.Assert(GetOperand(&causetcore.LogicalJoin{}), Equals, OperandJoin)
	c.Assert(GetOperand(&causetcore.LogicalAggregation{}), Equals, OperanPosetDaggregation)
	c.Assert(GetOperand(&causetcore.LogicalProjection{}), Equals, OperandProjection)
	c.Assert(GetOperand(&causetcore.LogicalSelection{}), Equals, OperandSelection)
	c.Assert(GetOperand(&causetcore.LogicalApply{}), Equals, OperandApply)
	c.Assert(GetOperand(&causetcore.LogicalMaxOneRow{}), Equals, OperandMaxOneRow)
	c.Assert(GetOperand(&causetcore.LogicalBlockDual{}), Equals, OperandBlockDual)
	c.Assert(GetOperand(&causetcore.DataSource{}), Equals, OperandDataSource)
	c.Assert(GetOperand(&causetcore.LogicalUnionScan{}), Equals, OperandUnionScan)
	c.Assert(GetOperand(&causetcore.LogicalUnionAll{}), Equals, OperandUnionAll)
	c.Assert(GetOperand(&causetcore.LogicalSort{}), Equals, OperandSort)
	c.Assert(GetOperand(&causetcore.LogicalTopN{}), Equals, OperandTopN)
	c.Assert(GetOperand(&causetcore.LogicalLock{}), Equals, OperandLock)
	c.Assert(GetOperand(&causetcore.LogicalLimit{}), Equals, OperandLimit)
}

func (s *testMemoSuite) TestOperandMatch(c *C) {
	c.Assert(OperandAny.Match(OperandLimit), IsTrue)
	c.Assert(OperandAny.Match(OperandSelection), IsTrue)
	c.Assert(OperandAny.Match(OperandJoin), IsTrue)
	c.Assert(OperandAny.Match(OperandMaxOneRow), IsTrue)
	c.Assert(OperandAny.Match(OperandAny), IsTrue)

	c.Assert(OperandLimit.Match(OperandAny), IsTrue)
	c.Assert(OperandSelection.Match(OperandAny), IsTrue)
	c.Assert(OperandJoin.Match(OperandAny), IsTrue)
	c.Assert(OperandMaxOneRow.Match(OperandAny), IsTrue)
	c.Assert(OperandAny.Match(OperandAny), IsTrue)

	c.Assert(OperandLimit.Match(OperandLimit), IsTrue)
	c.Assert(OperandSelection.Match(OperandSelection), IsTrue)
	c.Assert(OperandJoin.Match(OperandJoin), IsTrue)
	c.Assert(OperandMaxOneRow.Match(OperandMaxOneRow), IsTrue)
	c.Assert(OperandAny.Match(OperandAny), IsTrue)

	c.Assert(OperandLimit.Match(OperandSelection), IsFalse)
	c.Assert(OperandLimit.Match(OperandJoin), IsFalse)
	c.Assert(OperandLimit.Match(OperandMaxOneRow), IsFalse)
}

func (s *testMemoSuite) TestNewPattern(c *C) {
	p := NewPattern(OperandAny, EngineAll)
	c.Assert(p.Operand, Equals, OperandAny)
	c.Assert(p.Children, IsNil)

	p = NewPattern(OperandJoin, EngineAll)
	c.Assert(p.Operand, Equals, OperandJoin)
	c.Assert(p.Children, IsNil)
}

func (s *testMemoSuite) TestPatternSetChildren(c *C) {
	p := NewPattern(OperandAny, EngineAll)
	p.SetChildren(NewPattern(OperandLimit, EngineAll))
	c.Assert(len(p.Children), Equals, 1)
	c.Assert(p.Children[0].Operand, Equals, OperandLimit)
	c.Assert(p.Children[0].Children, IsNil)

	p = NewPattern(OperandJoin, EngineAll)
	p.SetChildren(NewPattern(OperandProjection, EngineAll), NewPattern(OperandSelection, EngineAll))
	c.Assert(len(p.Children), Equals, 2)
	c.Assert(p.Children[0].Operand, Equals, OperandProjection)
	c.Assert(p.Children[0].Children, IsNil)
	c.Assert(p.Children[1].Operand, Equals, OperandSelection)
	c.Assert(p.Children[1].Children, IsNil)
}
