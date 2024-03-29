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
	"context"
	"testing"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMemoSuite{})

type testMemoSuite struct {
	*BerolinaSQL.BerolinaSQL
	is              schemareplicant.SchemaReplicant
	schemaReplicant *memex.Schema
	sctx            stochastikctx.Context
}

func (s *testMemoSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.is = schemareplicant.MockSchemaReplicant([]*perceptron.BlockInfo{causetembedded.MockSignedBlock()})
	s.sctx = causetembedded.MockContext()
	s.BerolinaSQL = BerolinaSQL.New()
	s.schemaReplicant = memex.NewSchema()
}

func (s *testMemoSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testMemoSuite) TestNewGroup(c *C) {
	p := &causetembedded.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schemaReplicant)

	c.Assert(g.Equivalents.Len(), Equals, 1)
	c.Assert(g.Equivalents.Front().Value.(*GroupExpr), Equals, expr)
	c.Assert(len(g.Fingerprints), Equals, 1)
	c.Assert(g.Explored(0), IsFalse)
}

func (s *testMemoSuite) TestGroupInsert(c *C) {
	p := &causetembedded.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schemaReplicant)
	c.Assert(g.Insert(expr), IsFalse)
	expr.selfFingerprint = "1"
	c.Assert(g.Insert(expr), IsTrue)
}

func (s *testMemoSuite) TestGrouFIDelelete(c *C) {
	p := &causetembedded.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schemaReplicant)
	c.Assert(g.Equivalents.Len(), Equals, 1)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)

	g.Delete(expr)
	c.Assert(g.Equivalents.Len(), Equals, 0)
}

func (s *testMemoSuite) TestGrouFIDeleleteAll(c *C) {
	expr := NewGroupExpr(causetembedded.LogicalSelection{}.Init(s.sctx, 0))
	g := NewGroupWithSchema(expr, s.schemaReplicant)
	c.Assert(g.Insert(NewGroupExpr(causetembedded.LogicalLimit{}.Init(s.sctx, 0))), IsTrue)
	c.Assert(g.Insert(NewGroupExpr(causetembedded.LogicalProjection{}.Init(s.sctx, 0))), IsTrue)
	c.Assert(g.Equivalents.Len(), Equals, 3)
	c.Assert(g.GetFirstElem(OperandProjection), NotNil)
	c.Assert(g.Exists(expr), IsTrue)

	g.DeleteAll()
	c.Assert(g.Equivalents.Len(), Equals, 0)
	c.Assert(g.GetFirstElem(OperandProjection), IsNil)
	c.Assert(g.Exists(expr), IsFalse)
}

func (s *testMemoSuite) TestGroupExists(c *C) {
	p := &causetembedded.LogicalLimit{}
	expr := NewGroupExpr(p)
	g := NewGroupWithSchema(expr, s.schemaReplicant)
	c.Assert(g.Exists(expr), IsTrue)

	g.Delete(expr)
	c.Assert(g.Exists(expr), IsFalse)
}

func (s *testMemoSuite) TestGroupFingerPrint(c *C) {
	stmt1, err := s.ParseOneStmt("select * from t where a > 1 and a < 100", "", "")
	c.Assert(err, IsNil)
	p1, _, err := causetembedded.BuildLogicalCauset(context.Background(), s.sctx, stmt1, s.is)
	c.Assert(err, IsNil)
	logic1, ok := p1.(causetembedded.LogicalCauset)
	c.Assert(ok, IsTrue)
	// Causet tree should be: DataSource -> Selection -> Projection
	proj, ok := logic1.(*causetembedded.LogicalProjection)
	c.Assert(ok, IsTrue)
	sel, ok := logic1.Children()[0].(*causetembedded.LogicalSelection)
	c.Assert(ok, IsTrue)
	group1 := Convert2Group(logic1)
	oldGroupExpr := group1.Equivalents.Front().Value.(*GroupExpr)

	// Insert a GroupExpr with the same ExprNode.
	newGroupExpr := NewGroupExpr(proj)
	newGroupExpr.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr)
	c.Assert(group1.Equivalents.Len(), Equals, 1)

	// Insert a GroupExpr with different children。
	newGroupExpr2 := NewGroupExpr(proj)
	newGroup := NewGroupWithSchema(oldGroupExpr, group1.Prop.Schema)
	newGroupExpr2.SetChildren(newGroup)
	group1.Insert(newGroupExpr2)
	c.Assert(group1.Equivalents.Len(), Equals, 2)

	// Insert a GroupExpr with different ExprNode.
	limit := causetembedded.LogicalLimit{}.Init(proj.SCtx(), 0)
	newGroupExpr3 := NewGroupExpr(limit)
	newGroupExpr3.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr3)
	c.Assert(group1.Equivalents.Len(), Equals, 3)

	// Insert two LogicalSelections with same conditions but different order.
	c.Assert(len(sel.Conditions), Equals, 2)
	newSelection := causetembedded.LogicalSelection{
		Conditions: make([]memex.Expression, 2)}.Init(sel.SCtx(), sel.SelectBlockOffset())
	newSelection.Conditions[0], newSelection.Conditions[1] = sel.Conditions[1], sel.Conditions[0]
	newGroupExpr4 := NewGroupExpr(sel)
	newGroupExpr5 := NewGroupExpr(newSelection)
	newGroupExpr4.SetChildren(oldGroupExpr.Children[0])
	newGroupExpr5.SetChildren(oldGroupExpr.Children[0])
	group1.Insert(newGroupExpr4)
	c.Assert(group1.Equivalents.Len(), Equals, 4)
	group1.Insert(newGroupExpr5)
	c.Assert(group1.Equivalents.Len(), Equals, 4)
}

func (s *testMemoSuite) TestGroupGetFirstElem(c *C) {
	expr0 := NewGroupExpr(causetembedded.LogicalProjection{}.Init(s.sctx, 0))
	expr1 := NewGroupExpr(causetembedded.LogicalLimit{}.Init(s.sctx, 0))
	expr2 := NewGroupExpr(causetembedded.LogicalProjection{}.Init(s.sctx, 0))
	expr3 := NewGroupExpr(causetembedded.LogicalLimit{}.Init(s.sctx, 0))
	expr4 := NewGroupExpr(causetembedded.LogicalProjection{}.Init(s.sctx, 0))

	g := NewGroupWithSchema(expr0, s.schemaReplicant)
	g.Insert(expr1)
	g.Insert(expr2)
	g.Insert(expr3)
	g.Insert(expr4)

	c.Assert(g.GetFirstElem(OperandProjection).Value.(*GroupExpr), Equals, expr0)
	c.Assert(g.GetFirstElem(OperandLimit).Value.(*GroupExpr), Equals, expr1)
	c.Assert(g.GetFirstElem(OperandAny).Value.(*GroupExpr), Equals, expr0)
}

type fakeImpl struct {
	cost float64
	plan causetembedded.PhysicalCauset
}

func (impl *fakeImpl) CalcCost(float64, ...Implementation) float64     { return 0 }
func (impl *fakeImpl) SetCost(float64)                                 {}
func (impl *fakeImpl) GetCost() float64                                { return 0 }
func (impl *fakeImpl) GetCauset() causetembedded.PhysicalCauset        { return impl.plan }
func (impl *fakeImpl) AttachChildren(...Implementation) Implementation { return nil }
func (impl *fakeImpl) GetCostLimit(float64, ...Implementation) float64 { return 0 }
func (s *testMemoSuite) TestGetInsertGroupImpl(c *C) {
	g := NewGroupWithSchema(NewGroupExpr(causetembedded.LogicalLimit{}.Init(s.sctx, 0)), s.schemaReplicant)
	emptyProp := &property.PhysicalProperty{}
	orderProp := &property.PhysicalProperty{Items: []property.Item{{DefCaus: &memex.DeferredCauset{}}}}

	impl := g.GetImpl(emptyProp)
	c.Assert(impl, IsNil)

	impl = &fakeImpl{plan: &causetembedded.PhysicalLimit{}}
	g.InsertImpl(emptyProp, impl)

	newImpl := g.GetImpl(emptyProp)
	c.Assert(newImpl, Equals, impl)

	newImpl = g.GetImpl(orderProp)
	c.Assert(newImpl, IsNil)
}

func (s *testMemoSuite) TestEngineTypeSet(c *C) {
	c.Assert(EngineAll.Contains(EngineMilevaDB), IsTrue)
	c.Assert(EngineAll.Contains(EngineEinsteinDB), IsTrue)
	c.Assert(EngineAll.Contains(EngineTiFlash), IsTrue)

	c.Assert(EngineMilevaDBOnly.Contains(EngineMilevaDB), IsTrue)
	c.Assert(EngineMilevaDBOnly.Contains(EngineEinsteinDB), IsFalse)
	c.Assert(EngineMilevaDBOnly.Contains(EngineTiFlash), IsFalse)

	c.Assert(EngineEinsteinDBOnly.Contains(EngineMilevaDB), IsFalse)
	c.Assert(EngineEinsteinDBOnly.Contains(EngineEinsteinDB), IsTrue)
	c.Assert(EngineEinsteinDBOnly.Contains(EngineTiFlash), IsFalse)

	c.Assert(EngineTiFlashOnly.Contains(EngineMilevaDB), IsFalse)
	c.Assert(EngineTiFlashOnly.Contains(EngineEinsteinDB), IsFalse)
	c.Assert(EngineTiFlashOnly.Contains(EngineTiFlash), IsTrue)

	c.Assert(EngineEinsteinDBOrTiFlash.Contains(EngineMilevaDB), IsFalse)
	c.Assert(EngineEinsteinDBOrTiFlash.Contains(EngineEinsteinDB), IsTrue)
	c.Assert(EngineEinsteinDBOrTiFlash.Contains(EngineTiFlash), IsTrue)
}

func (s *testMemoSuite) TestFirstElemAfterDelete(c *C) {
	oldExpr := NewGroupExpr(causetembedded.LogicalLimit{Count: 10}.Init(s.sctx, 0))
	g := NewGroupWithSchema(oldExpr, s.schemaReplicant)
	newExpr := NewGroupExpr(causetembedded.LogicalLimit{Count: 20}.Init(s.sctx, 0))
	g.Insert(newExpr)
	c.Assert(g.GetFirstElem(OperandLimit), NotNil)
	c.Assert(g.GetFirstElem(OperandLimit).Value, Equals, oldExpr)
	g.Delete(oldExpr)
	c.Assert(g.GetFirstElem(OperandLimit), NotNil)
	c.Assert(g.GetFirstElem(OperandLimit).Value, Equals, newExpr)
	g.Delete(newExpr)
	c.Assert(g.GetFirstElem(OperandLimit), IsNil)
}

func (s *testMemoSuite) TestBuildKeyInfo(c *C) {
	// case 1: primary key has constant constraint
	stmt1, err := s.ParseOneStmt("select a from t where a = 10", "", "")
	c.Assert(err, IsNil)
	p1, _, err := causetembedded.BuildLogicalCauset(context.Background(), s.sctx, stmt1, s.is)
	c.Assert(err, IsNil)
	logic1, ok := p1.(causetembedded.LogicalCauset)
	c.Assert(ok, IsTrue)
	group1 := Convert2Group(logic1)
	group1.BuildKeyInfo()
	c.Assert(group1.Prop.MaxOneRow, IsTrue)
	c.Assert(len(group1.Prop.Schema.Keys), Equals, 1)

	// case 2: group by column is key
	stmt2, err := s.ParseOneStmt("select b, sum(a) from t group by b", "", "")
	c.Assert(err, IsNil)
	p2, _, err := causetembedded.BuildLogicalCauset(context.Background(), s.sctx, stmt2, s.is)
	c.Assert(err, IsNil)
	logic2, ok := p2.(causetembedded.LogicalCauset)
	c.Assert(ok, IsTrue)
	group2 := Convert2Group(logic2)
	group2.BuildKeyInfo()
	c.Assert(group2.Prop.MaxOneRow, IsFalse)
	c.Assert(len(group2.Prop.Schema.Keys), Equals, 1)

	// case 3: build key info for new Group
	newSel := causetembedded.LogicalSelection{}.Init(s.sctx, 0)
	newExpr1 := NewGroupExpr(newSel)
	newExpr1.SetChildren(group2)
	newGroup1 := NewGroupWithSchema(newExpr1, group2.Prop.Schema)
	newGroup1.BuildKeyInfo()
	c.Assert(len(newGroup1.Prop.Schema.Keys), Equals, 1)

	// case 4: build maxOneRow for new Group
	newLimit := causetembedded.LogicalLimit{Count: 1}.Init(s.sctx, 0)
	newExpr2 := NewGroupExpr(newLimit)
	newExpr2.SetChildren(group2)
	newGroup2 := NewGroupWithSchema(newExpr2, group2.Prop.Schema)
	newGroup2.BuildKeyInfo()
	c.Assert(newGroup2.Prop.MaxOneRow, IsTrue)
}

func (s *testMemoSuite) TestExploreMark(c *C) {
	mark := ExploreMark(0)
	c.Assert(mark.Explored(0), IsFalse)
	c.Assert(mark.Explored(1), IsFalse)
	mark.SetExplored(0)
	mark.SetExplored(1)
	c.Assert(mark.Explored(0), IsTrue)
	c.Assert(mark.Explored(1), IsTrue)
	mark.SetUnexplored(1)
	c.Assert(mark.Explored(0), IsTrue)
	c.Assert(mark.Explored(1), IsFalse)
}
