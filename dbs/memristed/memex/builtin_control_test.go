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

package memex

import (
	"errors"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
)

func (s *testEvaluatorSuite) TestCaseWhen(c *C) {
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{true, 1, true, 2, 3}, 1},
		{[]interface{}{false, 1, true, 2, 3}, 2},
		{[]interface{}{nil, 1, true, 2, 3}, 2},
		{[]interface{}{false, 1, false, 2, 3}, 3},
		{[]interface{}{nil, 1, nil, 2, 3}, 3},
		{[]interface{}{false, 1, nil, 2, 3}, 3},
		{[]interface{}{nil, 1, false, 2, 3}, 3},
		{[]interface{}{1, jsonInt.GetMysqlJSON(), nil}, 3},
		{[]interface{}{0, jsonInt.GetMysqlJSON(), nil}, nil},
		{[]interface{}{0.1, 1, 2}, 1},
		{[]interface{}{0.0, 1, 0.1, 2}, 2},
	}
	fc := funcs[ast.Case]
	for _, t := range tbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(t.Arg...)))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(d, solitonutil.CausetEquals, types.NewCauset(t.Ret))
	}
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(errors.New("can't convert string to bool"), 1, true)))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestIf(c *C) {
	stmtCtx := s.ctx.GetStochastikVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()
	tbl := []struct {
		Arg1 interface{}
		Arg2 interface{}
		Arg3 interface{}
		Ret  interface{}
	}{
		{1, 1, 2, 1},
		{nil, 1, 2, 2},
		{0, 1, 2, 2},
		{"abc", 1, 2, 2},
		{"1abc", 1, 2, 1},
		{tm, 1, 2, 1},
		{duration, 1, 2, 1},
		{types.Duration{Duration: time.Duration(0)}, 1, 2, 2},
		{types.NewDecFromStringForTest("1.2"), 1, 2, 1},
		{jsonInt.GetMysqlJSON(), 1, 2, 1},
		{0.1, 1, 2, 1},
		{0.0, 1, 2, 2},
		{types.NewDecFromStringForTest("0.1"), 1, 2, 1},
		{types.NewDecFromStringForTest("0.0"), 1, 2, 2},
		{"0.1", 1, 2, 1},
		{"0.0", 1, 2, 2},
	}

	fc := funcs[ast.If]
	for _, t := range tbl {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(t.Arg1, t.Arg2, t.Arg3)))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil)
		c.Assert(d, solitonutil.CausetEquals, types.NewCauset(t.Ret))
	}
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(errors.New("must error"), 1, 2)))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Event{})
	c.Assert(err, NotNil)
	_, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(1, 2)))
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestIfNull(c *C) {
	tbl := []struct {
		arg1     interface{}
		arg2     interface{}
		expected interface{}
		isNil    bool
		getErr   bool
	}{
		{1, 2, int64(1), false, false},
		{nil, 2, int64(2), false, false},
		{nil, nil, nil, true, false},
		{tm, nil, tm, false, false},
		{nil, duration, duration, false, false},
		{nil, types.NewDecFromFloatForTest(123.123), types.NewDecFromFloatForTest(123.123), false, false},
		{nil, types.NewBinaryLiteralFromUint(0x01, -1), uint64(1), false, false},
		{nil, types.Set{Value: 1, Name: "abc"}, "abc", false, false},
		{nil, jsonInt.GetMysqlJSON(), jsonInt.GetMysqlJSON(), false, false},
		{"abc", nil, "abc", false, false},
		{errors.New(""), nil, "", true, true},
	}

	for _, t := range tbl {
		f, err := newFunctionForTest(s.ctx, ast.Ifnull, s.primitiveValsToConstants([]interface{}{t.arg1, t.arg2})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Event{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.HoTT(), Equals, types.HoTTNull)
			} else {
				c.Assert(d.GetValue(), DeepEquals, t.expected)
			}
		}
	}

	_, err := funcs[ast.Ifnull].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Ifnull].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, NotNil)
}
