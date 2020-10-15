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

package aggfuncs_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/interlock/aggfuncs"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *testSuite) TestLeadLag(c *C) {
	zero := memex.NewZero()
	one := memex.NewOne()
	two := &memex.Constant{
		Value:   types.NewCauset(2),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}
	three := &memex.Constant{
		Value:   types.NewCauset(3),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}
	million := &memex.Constant{
		Value:   types.NewCauset(1000000),
		RetType: types.NewFieldType(allegrosql.TypeLong),
	}
	defaultArg := &memex.DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong), Index: 0}

	numEvents := 3
	tests := []windowTest{
		// lag(field0, N)
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{zero}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{one}, 0, numEvents, nil, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{two}, 0, numEvents, nil, nil, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{three}, 0, numEvents, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{million}, 0, numEvents, nil, nil, nil),
		// lag(field0, N, 1000000)
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{zero, million}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{one, million}, 0, numEvents, 1000000, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{two, million}, 0, numEvents, 1000000, 1000000, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{three, million}, 0, numEvents, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{million, million}, 0, numEvents, 1000000, 1000000, 1000000),
		// lag(field0, N, field0)
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{zero, defaultArg}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{one, defaultArg}, 0, numEvents, 0, 0, 1),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{two, defaultArg}, 0, numEvents, 0, 1, 0),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{three, defaultArg}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{million, defaultArg}, 0, numEvents, 0, 1, 2),

		// lead(field0, N)
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{zero}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{one}, 0, numEvents, 1, 2, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{two}, 0, numEvents, 2, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{three}, 0, numEvents, nil, nil, nil),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{million}, 0, numEvents, nil, nil, nil),
		// lead(field0, N, 1000000)
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{zero, million}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{one, million}, 0, numEvents, 1, 2, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{two, million}, 0, numEvents, 2, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{three, million}, 0, numEvents, 1000000, 1000000, 1000000),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{million, million}, 0, numEvents, 1000000, 1000000, 1000000),
		// lead(field0, N, field0)
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{zero, defaultArg}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{one, defaultArg}, 0, numEvents, 1, 2, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{two, defaultArg}, 0, numEvents, 2, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{three, defaultArg}, 0, numEvents, 0, 1, 2),
		buildWindowTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{million, defaultArg}, 0, numEvents, 0, 1, 2),
	}
	for _, test := range tests {
		s.testWindowFunc(c, test)
	}

}

func (s *testSuite) TestMemLeadLag(c *C) {
	zero := memex.NewZero()
	one := memex.NewOne()
	two := &memex.Constant{
		Value:   types.NewCauset(2),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}
	three := &memex.Constant{
		Value:   types.NewCauset(3),
		RetType: types.NewFieldType(allegrosql.TypeTiny),
	}
	million := &memex.Constant{
		Value:   types.NewCauset(1000000),
		RetType: types.NewFieldType(allegrosql.TypeLong),
	}

	numEvents := 3
	tests := []windowMemTest{
		// lag(field0, N)
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{zero}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{one}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{two}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{three}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLag, allegrosql.TypeLonglong,
			[]memex.Expression{million}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),

		// lead(field0, N)
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{zero}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{one}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{two}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{three}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
		buildWindowMemTesterWithArgs(ast.WindowFuncLead, allegrosql.TypeLonglong,
			[]memex.Expression{million}, 0, numEvents, aggfuncs.DefPartialResult4LeadLagSize, rowMemDeltaGens),
	}

	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}

}
