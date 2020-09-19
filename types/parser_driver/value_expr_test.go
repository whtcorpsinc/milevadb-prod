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

package driver

import (
	"strings"
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/format"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testValueExprRestoreSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testValueExprRestoreSuite struct {
}

func (s *testValueExprRestoreSuite) TestValueExprRestore(c *C) {
	testCases := []struct {
		causet  types.Causet
		expect string
	}{
		{types.NewCauset(nil), "NULL"},
		{types.NewIntCauset(1), "1"},
		{types.NewIntCauset(-1), "-1"},
		{types.NewUintCauset(1), "1"},
		{types.NewFloat32Causet(1.1), "1.1e+00"},
		{types.NewFloat64Causet(1.1), "1.1e+00"},
		{types.NewStringCauset("test `s't\"r."), "'test `s''t\"r.'"},
		{types.NewBytesCauset([]byte("test `s't\"r.")), "'test `s''t\"r.'"},
		{types.NewBinaryLiteralCauset([]byte("test `s't\"r.")), "b'11101000110010101110011011101000010000001100000011100110010011101110100001000100111001000101110'"},
		{types.NewDecimalCauset(types.NewDecFromInt(321)), "321"},
		{types.NewDurationCauset(types.ZeroDuration), "'00:00:00'"},
		{types.NewTimeCauset(types.ZeroDatetime), "'0000-00-00 00:00:00'"},
	}
	// Run Test
	var sb strings.Builder
	for _, testCase := range testCases {
		sb.Reset()
		expr := &ValueExpr{Causet: testCase.causet}
		err := expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		c.Assert(err, IsNil)
		c.Assert(sb.String(), Equals, testCase.expect, Commentf("Causet: %#v", testCase.causet))
	}
}
