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
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
)

func (s *testEvaluatorSuite) TestLike(c *C) {
	tests := []struct {
		input   string
		pattern string
		match   int
	}{
		{"a", "", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "Aa", 0},
		{"aAb", `Aa%`, 0},
		{"aAb", "aA_", 1},
		{"baab", "b_%b", 1},
		{"baab", "b%_b", 1},
		{"bab", "b_%b", 1},
		{"bab", "b%_b", 1},
		{"bb", "b_%b", 0},
		{"bb", "b%_b", 0},
		{"baabccc", "b_%b%", 1},
	}

	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(tt.input, tt.pattern, 0)))
		c.Assert(err, IsNil, commentf)
		r, err := evalBuiltinFuncConcurrent(f, chunk.Event{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, solitonutil.CausetEquals, types.NewCauset(tt.match), commentf)
	}
}

func (s *testEvaluatorSerialSuites) TestCILike(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	tests := []struct {
		input        string
		pattern      string
		generalMatch int
		unicodeMatch int
	}{
		{"a", "", 0, 0},
		{"a", "a", 1, 1},
		{"a", "á", 1, 1},
		{"a", "b", 0, 0},
		{"aA", "Aa", 1, 1},
		{"áAb", `Aa%`, 1, 1},
		{"áAb", `%ab%`, 1, 1},
		{"áAb", `%ab`, 1, 1},
		{"ÀAb", "aA_", 1, 1},
		{"áééá", "a_%a", 1, 1},
		{"áééá", "a%_a", 1, 1},
		{"áéá", "a_%a", 1, 1},
		{"áéá", "a%_a", 1, 1},
		{"áá", "a_%a", 0, 0},
		{"áá", "a%_a", 0, 0},
		{"áééáííí", "a_%a%", 1, 1},

		// performs matching on a per-character basis
		// https://dev.allegrosql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
		{"ß", "s%", 1, 0},
		{"ß", "%s", 1, 0},
		{"ß", "ss", 0, 0},
		{"ß", "s", 1, 0},
		{"ss", "%ß%", 1, 0},
		{"ß", "_", 1, 1},
		{"ß", "__", 0, 0},
	}
	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		inputs := s.datumsToConstants(types.MakeCausets(tt.input, tt.pattern, 0))
		f, err := fc.getFunction(s.ctx, inputs)
		c.Assert(err, IsNil, commentf)
		f.setDefCauslator(defCauslate.GetDefCauslator("utf8mb4_general_ci"))
		r, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, solitonutil.CausetEquals, types.NewCauset(tt.generalMatch), commentf)
	}

	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		inputs := s.datumsToConstants(types.MakeCausets(tt.input, tt.pattern, 0))
		f, err := fc.getFunction(s.ctx, inputs)
		c.Assert(err, IsNil, commentf)
		f.setDefCauslator(defCauslate.GetDefCauslator("utf8mb4_unicode_ci"))
		r, err := evalBuiltinFunc(f, chunk.Event{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, solitonutil.CausetEquals, types.NewCauset(tt.unicodeMatch), commentf)
	}
}

func (s *testEvaluatorSuite) TestRegexp(c *C) {
	tests := []struct {
		pattern string
		input   string
		match   int64
		err     error
	}{
		{"^$", "a", 0, nil},
		{"a", "a", 1, nil},
		{"a", "b", 0, nil},
		{"aA", "aA", 1, nil},
		{".", "a", 1, nil},
		{"^.$", "ab", 0, nil},
		{"..", "b", 0, nil},
		{".ab", "aab", 1, nil},
		{".*", "abcd", 1, nil},
		{"(", "", 0, ErrRegexp},
		{"(*", "", 0, ErrRegexp},
		{"[a", "", 0, ErrRegexp},
		{"\\", "", 0, ErrRegexp},
	}
	for _, tt := range tests {
		fc := funcs[ast.Regexp]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeCausets(tt.input, tt.pattern)))
		c.Assert(err, IsNil)
		match, err := evalBuiltinFunc(f, chunk.Event{})
		if tt.err == nil {
			c.Assert(err, IsNil)
			c.Assert(match, solitonutil.CausetEquals, types.NewCauset(tt.match), Commentf("%v", tt))
		} else {
			c.Assert(terror.ErrorEqual(err, tt.err), IsTrue)
		}
	}
}
