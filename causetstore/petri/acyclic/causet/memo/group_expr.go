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
	"encoding/binary"
	"reflect"

	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/memex"
)

// GroupExpr is used to causetstore all the logically equivalent memexs which
// have the same root operator. Different from a normal memex, the
// Children of a Group memex are memex Groups, not memexs.
// Another property of Group memex is that the child Group references will
// never be changed once the Group memex is created.
type GroupExpr struct {
	ExprNode causetembedded.LogicalCauset
	Children []*Group
	Group    *Group

	// ExploreMark is uses to mark whether this GroupExpr has been fully
	// explored by a transformation rule batch in a certain round.
	ExploreMark

	selfFingerprint string
	// appliedMemruleSet saves transformation rules which have been applied to this
	// GroupExpr, and will not be applied again. Use `uint64` which should be the
	// id of a Transformation instead of `Transformation` itself to avoid import cycle.
	appliedMemruleSet map[uint64]struct{}
}

// NewGroupExpr creates a GroupExpr based on a logical plan node.
func NewGroupExpr(node causetembedded.LogicalCauset) *GroupExpr {
	return &GroupExpr{
		ExprNode:          node,
		Children:          nil,
		appliedMemruleSet: make(map[uint64]struct{}),
	}
}

// FingerPrint gets the unique fingerprint of the Group memex.
func (e *GroupExpr) FingerPrint() string {
	if len(e.selfFingerprint) == 0 {
		planHash := e.ExprNode.HashCode()
		buffer := make([]byte, 2, 2+len(e.Children)*8+len(planHash))
		binary.BigEndian.PutUint16(buffer, uint16(len(e.Children)))
		for _, child := range e.Children {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(reflect.ValueOf(child).Pointer()))
			buffer = append(buffer, buf[:]...)
		}
		buffer = append(buffer, planHash...)
		e.selfFingerprint = string(buffer)
	}
	return e.selfFingerprint
}

// SetChildren sets Children of the GroupExpr.
func (e *GroupExpr) SetChildren(children ...*Group) {
	e.Children = children
}

// Schema gets GroupExpr's Schema.
func (e *GroupExpr) Schema() *memex.Schema {
	return e.Group.Prop.Schema
}

// AddAppliedMemrule adds a rule into the appliedMemruleSet.
func (e *GroupExpr) AddAppliedMemrule(rule interface{}) {
	ruleID := reflect.ValueOf(rule).Pointer()
	e.appliedMemruleSet[uint64(ruleID)] = struct{}{}
}

// HasAppliedMemrule returns if the rule has been applied.
func (e *GroupExpr) HasAppliedMemrule(rule interface{}) bool {
	ruleID := reflect.ValueOf(rule).Pointer()
	_, ok := e.appliedMemruleSet[uint64(ruleID)]
	return ok
}
