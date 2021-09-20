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

package embedded

import (
	"fmt"
	"math"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

var _ = Suite(&testFindBestTaskSuite{})

type testFindBestTaskSuite struct {
	ctx stochastikctx.Context
}

func (s *testFindBestTaskSuite) SetUpSuite(c *C) {
	s.ctx = MockContext()
}

type mockDataSource struct {
	baseLogicalCauset
}

func (ds mockDataSource) Init(ctx stochastikctx.Context) *mockDataSource {
	ds.baseLogicalCauset = newBaseLogicalCauset(ctx, "mockDS", &ds, 0)
	return &ds
}

func (ds *mockDataSource) findBestTask(prop *property.PhysicalProperty, planCounter *CausetCounterTp) (task, int64, error) {
	// It can satisfy any of the property!
	// Just use a BlockDual for convenience.
	p := PhysicalBlockDual{}.Init(ds.ctx, &property.StatsInfo{RowCount: 1}, 0)
	task := &rootTask{
		p:   p,
		cst: 10000,
	}
	planCounter.Dec(1)
	return task, 1, nil
}

// mockLogicalCauset4Test is a LogicalCauset which is used for unit test.
// The basic assumption:
// 1. mockLogicalCauset4Test can generate tow HoTTs of physical plan: physicalCauset1 and
//    physicalCauset2. physicalCauset1 can pass the property only when they are the same
//    order; while physicalCauset2 cannot match any of the property(in other words, we can
//    generate it only when then property is empty).
// 2. We have a hint for physicalCauset2.
// 3. If the property is empty, we still need to check `canGenerateCauset2` to decide
//    whether it can generate physicalCauset2.
type mockLogicalCauset4Test struct {
	baseLogicalCauset
	// hasHintForCauset2 indicates whether this mockCauset contains hint.
	// This hint is used to generate physicalCauset2. See the implementation
	// of exhaustPhysicalCausets().
	hasHintForCauset2 bool
	// canGenerateCauset2 indicates whether this plan can generate physicalCauset2.
	canGenerateCauset2 bool
	// costOverflow indicates whether this plan will generate physical plan whose cost is overflowed.
	costOverflow bool
}

func (p mockLogicalCauset4Test) Init(ctx stochastikctx.Context) *mockLogicalCauset4Test {
	p.baseLogicalCauset = newBaseLogicalCauset(ctx, "mockCauset", &p, 0)
	return &p
}

func (p *mockLogicalCauset4Test) getPhysicalCauset1(prop *property.PhysicalProperty) PhysicalCauset {
	physicalCauset1 := mockPhysicalCauset4Test{planType: 1, costOverflow: p.costOverflow}.Init(p.ctx)
	physicalCauset1.stats = &property.StatsInfo{RowCount: 1}
	physicalCauset1.childrenReqProps = make([]*property.PhysicalProperty, 1)
	physicalCauset1.childrenReqProps[0] = prop.Clone()
	return physicalCauset1
}

func (p *mockLogicalCauset4Test) getPhysicalCauset2(prop *property.PhysicalProperty) PhysicalCauset {
	physicalCauset2 := mockPhysicalCauset4Test{planType: 2, costOverflow: p.costOverflow}.Init(p.ctx)
	physicalCauset2.stats = &property.StatsInfo{RowCount: 1}
	physicalCauset2.childrenReqProps = make([]*property.PhysicalProperty, 1)
	physicalCauset2.childrenReqProps[0] = property.NewPhysicalProperty(prop.TaskTp, nil, false, prop.ExpectedCnt, false)
	return physicalCauset2
}

func (p *mockLogicalCauset4Test) exhaustPhysicalCausets(prop *property.PhysicalProperty) ([]PhysicalCauset, bool) {
	plan1 := make([]PhysicalCauset, 0, 1)
	plan2 := make([]PhysicalCauset, 0, 1)
	if prop.IsEmpty() && p.canGenerateCauset2 {
		// Generate PhysicalCauset2 when the property is empty.
		plan2 = append(plan2, p.getPhysicalCauset2(prop))
		if p.hasHintForCauset2 {
			return plan2, true
		}
	}
	if all, _ := prop.AllSameOrder(); all {
		// Generate PhysicalCauset1 when properties are the same order.
		plan1 = append(plan1, p.getPhysicalCauset1(prop))
	}
	if p.hasHintForCauset2 {
		// The hint cannot work.
		if prop.IsEmpty() {
			p.ctx.GetStochastikVars().StmtCtx.AppendWarning(fmt.Errorf("the hint is inapplicable for plan2"))
		}
		return plan1, false
	}
	return append(plan1, plan2...), true
}

type mockPhysicalCauset4Test struct {
	basePhysicalCauset
	// 1 or 2 for physicalCauset1 or physicalCauset2.
	// See the comment of mockLogicalCauset4Test.
	planType     int
	costOverflow bool
}

func (p mockPhysicalCauset4Test) Init(ctx stochastikctx.Context) *mockPhysicalCauset4Test {
	p.basePhysicalCauset = newBasePhysicalCauset(ctx, "mockCauset", &p, 0)
	return &p
}

func (p *mockPhysicalCauset4Test) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	attachCauset2Task(p, t)
	if p.costOverflow {
		t.addCost(math.MaxFloat64)
	} else {
		t.addCost(1)
	}
	return t
}

func (s *testFindBestTaskSuite) TestCostOverflow(c *C) {
	ctx := MockContext()
	// Causet Tree: mockCauset -> mockDataSource
	mockCauset := mockLogicalCauset4Test{costOverflow: true}.Init(ctx)
	mockDS := mockDataSource{}.Init(ctx)
	mockCauset.SetChildren(mockDS)
	// An empty property is enough for this test.
	prop := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
	t, _, err := mockCauset.findBestTask(prop, &CausetCounterDisabled)
	c.Assert(err, IsNil)
	// The cost should be overflowed, but the task shouldn't be invalid.
	c.Assert(t.invalid(), IsFalse)
	c.Assert(t.cost(), Equals, math.MaxFloat64)
}

func (s *testFindBestTaskSuite) TestEnforcedProperty(c *C) {
	ctx := MockContext()
	// CausetTree : mockLogicalCauset -> mockDataSource
	mockCauset := mockLogicalCauset4Test{}.Init(ctx)
	mockDS := mockDataSource{}.Init(ctx)
	mockCauset.SetChildren(mockDS)

	col0 := &memex.DeferredCauset{UniqueID: 1}
	col1 := &memex.DeferredCauset{UniqueID: 2}
	// Use different order, so that mockLogicalCauset cannot generate any of the
	// physical plans.
	item0 := property.Item{DefCaus: col0, Desc: false}
	item1 := property.Item{DefCaus: col1, Desc: true}
	items := []property.Item{item0, item1}

	prop0 := &property.PhysicalProperty{
		Items:    items,
		Enforced: false,
	}
	// should return invalid task because no physical plan can match this property.
	task, _, err := mockCauset.findBestTask(prop0, &CausetCounterDisabled)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsTrue)

	prop1 := &property.PhysicalProperty{
		Items:    items,
		Enforced: true,
	}
	// should return the valid task when the property is enforced.
	task, _, err = mockCauset.findBestTask(prop1, &CausetCounterDisabled)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
}

func (s *testFindBestTaskSuite) TestHintCannotFitProperty(c *C) {
	ctx := MockContext()
	// CausetTree : mockLogicalCauset -> mockDataSource
	mockCauset0 := mockLogicalCauset4Test{
		hasHintForCauset2:  true,
		canGenerateCauset2: true,
	}.Init(ctx)
	mockDS := mockDataSource{}.Init(ctx)
	mockCauset0.SetChildren(mockDS)

	col0 := &memex.DeferredCauset{UniqueID: 1}
	item0 := property.Item{DefCaus: col0}
	items := []property.Item{item0}
	// case 1, The property is not empty and enforced, should enforce a sort.
	prop0 := &property.PhysicalProperty{
		Items:    items,
		Enforced: true,
	}
	task, _, err := mockCauset0.findBestTask(prop0, &CausetCounterDisabled)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	_, enforcedSort := task.plan().(*PhysicalSort)
	c.Assert(enforcedSort, IsTrue)
	plan2 := task.plan().Children()[0]
	mockPhysicalCauset, ok := plan2.(*mockPhysicalCauset4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalCauset.planType, Equals, 2)

	// case 2, The property is not empty but not enforced, still need to enforce a sort
	// to ensure the hint can work
	prop1 := &property.PhysicalProperty{
		Items:    items,
		Enforced: false,
	}
	task, _, err = mockCauset0.findBestTask(prop1, &CausetCounterDisabled)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	_, enforcedSort = task.plan().(*PhysicalSort)
	c.Assert(enforcedSort, IsTrue)
	plan2 = task.plan().Children()[0]
	mockPhysicalCauset, ok = plan2.(*mockPhysicalCauset4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalCauset.planType, Equals, 2)

	// case 3, The hint cannot work even if the property is empty, should return a warning
	// and generate physicalCauset1.
	prop2 := &property.PhysicalProperty{
		Items:    items,
		Enforced: false,
	}
	mockCauset1 := mockLogicalCauset4Test{
		hasHintForCauset2:  true,
		canGenerateCauset2: false,
	}.Init(ctx)
	mockCauset1.SetChildren(mockDS)
	task, _, err = mockCauset1.findBestTask(prop2, &CausetCounterDisabled)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	c.Assert(ctx.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	// Because physicalCauset1 can match the property, so we should get it.
	mockPhysicalCauset, ok = task.plan().(*mockPhysicalCauset4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalCauset.planType, Equals, 1)

	// case 4, Similar to case 3, but the property is enforced now. Ths result should be
	// the same with case 3.
	ctx.GetStochastikVars().StmtCtx.SetWarnings(nil)
	prop3 := &property.PhysicalProperty{
		Items:    items,
		Enforced: true,
	}
	task, _, err = mockCauset1.findBestTask(prop3, &CausetCounterDisabled)
	c.Assert(err, IsNil)
	c.Assert(task.invalid(), IsFalse)
	c.Assert(ctx.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	// Because physicalCauset1 can match the property, so we don't need to enforce a sort.
	mockPhysicalCauset, ok = task.plan().(*mockPhysicalCauset4Test)
	c.Assert(ok, IsTrue)
	c.Assert(mockPhysicalCauset.planType, Equals, 1)
}
