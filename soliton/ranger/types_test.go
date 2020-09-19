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

package ranger_test

import (
	"math"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
)

var _ = Suite(&testRangeSuite{})

type testRangeSuite struct {
}

func (s *testRangeSuite) TestRange(c *C) {
	simpleTests := []struct {
		ran ranger.Range
		str string
	}{
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewIntCauset(1)},
				HighVal: []types.Causet{types.NewIntCauset(1)},
			},
			str: "[1,1]",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Causet{types.NewIntCauset(1)},
				HighVal:     []types.Causet{types.NewIntCauset(1)},
				HighExclude: true,
			},
			str: "[1,1)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Causet{types.NewIntCauset(1)},
				HighVal:     []types.Causet{types.NewIntCauset(2)},
				LowExclude:  true,
				HighExclude: true,
			},
			str: "(1,2)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Causet{types.NewFloat64Causet(1.1)},
				HighVal:     []types.Causet{types.NewFloat64Causet(1.9)},
				HighExclude: true,
			},
			str: "[1.1,1.9)",
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Causet{types.MinNotNullCauset()},
				HighVal:     []types.Causet{types.NewIntCauset(1)},
				HighExclude: true,
			},
			str: "[-inf,1)",
		},
	}
	for _, t := range simpleTests {
		c.Assert(t.ran.String(), Equals, t.str)
	}

	isPointTests := []struct {
		ran     ranger.Range
		isPoint bool
	}{
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewIntCauset(1)},
				HighVal: []types.Causet{types.NewIntCauset(1)},
			},
			isPoint: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewStringCauset("abc")},
				HighVal: []types.Causet{types.NewStringCauset("abc")},
			},
			isPoint: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewIntCauset(1)},
				HighVal: []types.Causet{types.NewIntCauset(1), types.NewIntCauset(1)},
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:     []types.Causet{types.NewIntCauset(1)},
				HighVal:    []types.Causet{types.NewIntCauset(1)},
				LowExclude: true,
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:      []types.Causet{types.NewIntCauset(1)},
				HighVal:     []types.Causet{types.NewIntCauset(1)},
				HighExclude: true,
			},
			isPoint: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewIntCauset(1)},
				HighVal: []types.Causet{types.NewIntCauset(2)},
			},
			isPoint: false,
		},
	}
	sc := new(stmtctx.StatementContext)
	for _, t := range isPointTests {
		c.Assert(t.ran.IsPoint(sc), Equals, t.isPoint)
	}
}

func (s *testRangeSuite) TestIsFullRange(c *C) {
	nullCauset := types.MinNotNullCauset()
	nullCauset.SetNull()
	isFullRangeTests := []struct {
		ran         ranger.Range
		isFullRange bool
	}{
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewIntCauset(math.MinInt64)},
				HighVal: []types.Causet{types.NewIntCauset(math.MaxInt64)},
			},
			isFullRange: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewIntCauset(math.MaxInt64)},
				HighVal: []types.Causet{types.NewIntCauset(math.MinInt64)},
			},
			isFullRange: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.NewIntCauset(1)},
				HighVal: []types.Causet{types.NewUintCauset(math.MaxUint64)},
			},
			isFullRange: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{*nullCauset.Clone()},
				HighVal: []types.Causet{types.NewUintCauset(math.MaxUint64)},
			},
			isFullRange: true,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{*nullCauset.Clone()},
				HighVal: []types.Causet{*nullCauset.Clone()},
			},
			isFullRange: false,
		},
		{
			ran: ranger.Range{
				LowVal:  []types.Causet{types.MinNotNullCauset()},
				HighVal: []types.Causet{types.MaxValueCauset()},
			},
			isFullRange: true,
		},
	}
	for _, t := range isFullRangeTests {
		c.Assert(t.ran.IsFullRange(), Equals, t.isFullRange)
	}
}
