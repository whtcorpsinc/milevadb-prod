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

	. "github.com/whtcorpsinc/check"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/memex"
)

func (s *testMemoSuite) TestNewGroupExpr(c *C) {
	p := &causetembedded.LogicalLimit{}
	expr := NewGroupExpr(p)
	c.Assert(expr.ExprNode, Equals, p)
	c.Assert(expr.Children, IsNil)
	c.Assert(expr.Explored(0), IsFalse)
}

func (s *testMemoSuite) TestGroupExprFingerprint(c *C) {
	p := &causetembedded.LogicalLimit{Count: 3}
	expr := NewGroupExpr(p)
	childGroup := NewGroupWithSchema(nil, memex.NewSchema())
	expr.SetChildren(childGroup)
	// ChildNum(2 bytes) + ChildPointer(8 bytes) + LogicalLimit HashCode
	planHash := p.HashCode()
	buffer := make([]byte, 10+len(planHash))
	binary.BigEndian.PutUint16(buffer, 1)
	binary.BigEndian.PutUint64(buffer[2:], uint64(reflect.ValueOf(childGroup).Pointer()))
	copy(buffer[10:], planHash)
	c.Assert(expr.FingerPrint(), Equals, string(buffer))
}
