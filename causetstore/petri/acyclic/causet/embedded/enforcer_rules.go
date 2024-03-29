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

package cascades

import (
	"math"

	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causet/implementation"
	"github.com/whtcorpsinc/milevadb/causet/memo"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
)

// Enforcer defines the interface for enforcer rules.
type Enforcer interface {
	// NewProperty generates relaxed property with the help of enforcer.
	NewProperty(prop *property.PhysicalProperty) (newProp *property.PhysicalProperty)
	// OnEnforce adds physical operators on top of child implementation to satisfy
	// required physical property.
	OnEnforce(reqProp *property.PhysicalProperty, child memo.Implementation) (impl memo.Implementation)
	// GetEnforceCost calculates cost of enforcing required physical property.
	GetEnforceCost(g *memo.Group) float64
}

// GetEnforcerMemrules gets all candidate enforcer rules based
// on required physical property.
func GetEnforcerMemrules(g *memo.Group, prop *property.PhysicalProperty) (enforcers []Enforcer) {
	if g.EngineType != memo.EngineMilevaDB {
		return
	}
	if !prop.IsEmpty() {
		enforcers = append(enforcers, orderEnforcer)
	}
	return
}

// OrderEnforcer enforces order property on child implementation.
type OrderEnforcer struct {
}

var orderEnforcer = &OrderEnforcer{}

// NewProperty removes order property from required physical property.
func (e *OrderEnforcer) NewProperty(prop *property.PhysicalProperty) (newProp *property.PhysicalProperty) {
	// Order property cannot be empty now.
	newProp = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	return
}

// OnEnforce adds sort operator to satisfy required order property.
func (e *OrderEnforcer) OnEnforce(reqProp *property.PhysicalProperty, child memo.Implementation) (impl memo.Implementation) {
	childCauset := child.GetCauset()
	sort := causetembedded.PhysicalSort{
		ByItems: make([]*soliton.ByItems, 0, len(reqProp.Items)),
	}.Init(childCauset.SCtx(), childCauset.Stats(), childCauset.SelectBlockOffset(), &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	for _, item := range reqProp.Items {
		item := &soliton.ByItems{
			Expr: item.DefCaus,
			Desc: item.Desc,
		}
		sort.ByItems = append(sort.ByItems, item)
	}
	impl = implementation.NewSortImpl(sort).AttachChildren(child)
	return
}

// GetEnforceCost calculates cost of sort operator.
func (e *OrderEnforcer) GetEnforceCost(g *memo.Group) float64 {
	// We need a StochastikCtx to calculate the cost of a sort.
	sctx := g.Equivalents.Front().Value.(*memo.GroupExpr).ExprNode.SCtx()
	sort := causetembedded.PhysicalSort{}.Init(sctx, g.Prop.Stats, 0, nil)
	cost := sort.GetCost(g.Prop.Stats.RowCount, g.Prop.Schema)
	return cost
}
