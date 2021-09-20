// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain col1 copy of the License at
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

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = SerialSuites(&testUnitTestSuit{})

type testUnitTestSuit struct {
	ctx stochastikctx.Context
}

func (s *testUnitTestSuit) SetUpSuite(c *C) {
	s.ctx = MockContext()
}

func (s *testUnitTestSuit) newTypeWithFlen(typeByte byte, flen int) *types.FieldType {
	tp := types.NewFieldType(typeByte)
	tp.Flen = flen
	return tp
}

func (s *testUnitTestSuit) SubstituteDefCaus2CorDefCaus(expr memex.Expression, colIDs map[int64]struct{}) (memex.Expression, error) {
	switch x := expr.(type) {
	case *memex.ScalarFunction:
		newArgs := make([]memex.Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg, err := s.SubstituteDefCaus2CorDefCaus(arg, colIDs)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newArgs = append(newArgs, newArg)
		}
		newSf, err := memex.NewFunction(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
		return newSf, errors.Trace(err)
	case *memex.DeferredCauset:
		if _, ok := colIDs[x.UniqueID]; ok {
			return &memex.CorrelatedDeferredCauset{DeferredCauset: *x}, nil
		}
	}
	return expr, nil
}

func (s *testUnitTestSuit) TestIndexPathSplitCorDefCausCond(c *C) {
	defer testleak.AfterTest(c)()
	totalSchema := memex.NewSchema()
	totalSchema.Append(&memex.DeferredCauset{
		UniqueID: 1,
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
	})
	totalSchema.Append(&memex.DeferredCauset{
		UniqueID: 2,
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
	})
	totalSchema.Append(&memex.DeferredCauset{
		UniqueID: 3,
		RetType:  s.newTypeWithFlen(allegrosql.TypeVarchar, 10),
	})
	totalSchema.Append(&memex.DeferredCauset{
		UniqueID: 4,
		RetType:  s.newTypeWithFlen(allegrosql.TypeVarchar, 10),
	})
	totalSchema.Append(&memex.DeferredCauset{
		UniqueID: 5,
		RetType:  types.NewFieldType(allegrosql.TypeLonglong),
	})
	names := make(types.NameSlice, 0, 5)
	names = append(names, &types.FieldName{DefCausName: perceptron.NewCIStr("col1")})
	names = append(names, &types.FieldName{DefCausName: perceptron.NewCIStr("col2")})
	names = append(names, &types.FieldName{DefCausName: perceptron.NewCIStr("col3")})
	names = append(names, &types.FieldName{DefCausName: perceptron.NewCIStr("col4")})
	names = append(names, &types.FieldName{DefCausName: perceptron.NewCIStr("col5")})
	testCases := []struct {
		expr           string
		corDefCausIDs  []int64
		idxDefCausIDs  []int64
		idxDefCausLens []int
		access         string
		remained       string
	}{
		{
			expr:           "col1 = col2",
			corDefCausIDs:  []int64{2},
			idxDefCausIDs:  []int64{1},
			idxDefCausLens: []int{types.UnspecifiedLength},
			access:         "[eq(DeferredCauset#1, DeferredCauset#2)]",
			remained:       "[]",
		},
		{
			expr:           "col1 = col5 and col2 = 1",
			corDefCausIDs:  []int64{5},
			idxDefCausIDs:  []int64{1, 2},
			idxDefCausLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:         "[eq(DeferredCauset#1, DeferredCauset#5) eq(DeferredCauset#2, 1)]",
			remained:       "[]",
		},
		{
			expr:           "col1 = col5 and col2 = 1",
			corDefCausIDs:  []int64{5},
			idxDefCausIDs:  []int64{2, 1},
			idxDefCausLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:         "[eq(DeferredCauset#2, 1) eq(DeferredCauset#1, DeferredCauset#5)]",
			remained:       "[]",
		},
		{
			expr:           "col1 = col5 and col2 = 1",
			corDefCausIDs:  []int64{5},
			idxDefCausIDs:  []int64{1},
			idxDefCausLens: []int{types.UnspecifiedLength},
			access:         "[eq(DeferredCauset#1, DeferredCauset#5)]",
			remained:       "[eq(DeferredCauset#2, 1)]",
		},
		{
			expr:           "col2 = 1 and col1 = col5",
			corDefCausIDs:  []int64{5},
			idxDefCausIDs:  []int64{1},
			idxDefCausLens: []int{types.UnspecifiedLength},
			access:         "[eq(DeferredCauset#1, DeferredCauset#5)]",
			remained:       "[eq(DeferredCauset#2, 1)]",
		},
		{
			expr:           "col1 = col2 and col3 = col4 and col5 = 1",
			corDefCausIDs:  []int64{2, 4},
			idxDefCausIDs:  []int64{1, 3},
			idxDefCausLens: []int{types.UnspecifiedLength, types.UnspecifiedLength},
			access:         "[eq(DeferredCauset#1, DeferredCauset#2) eq(DeferredCauset#3, DeferredCauset#4)]",
			remained:       "[eq(DeferredCauset#5, 1)]",
		},
		{
			expr:           "col1 = col2 and col3 = col4 and col5 = 1",
			corDefCausIDs:  []int64{2, 4},
			idxDefCausIDs:  []int64{1, 3},
			idxDefCausLens: []int{types.UnspecifiedLength, 2},
			access:         "[eq(DeferredCauset#1, DeferredCauset#2) eq(DeferredCauset#3, DeferredCauset#4)]",
			remained:       "[eq(DeferredCauset#3, DeferredCauset#4) eq(DeferredCauset#5, 1)]",
		},
		{
			expr:           `col1 = col5 and col3 = "col1" and col2 = col5`,
			corDefCausIDs:  []int64{5},
			idxDefCausIDs:  []int64{1, 2, 3},
			idxDefCausLens: []int{types.UnspecifiedLength, types.UnspecifiedLength, types.UnspecifiedLength},
			access:         "[eq(DeferredCauset#1, DeferredCauset#5) eq(DeferredCauset#2, DeferredCauset#5) eq(DeferredCauset#3, col1)]",
			remained:       "[]",
		},
		{
			expr:           "col3 = CHAR(1 COLLATE 'binary')",
			corDefCausIDs:  []int64{},
			idxDefCausIDs:  []int64{3},
			idxDefCausLens: []int{types.UnspecifiedLength},
			access:         "[]",
			remained:       "[eq(DeferredCauset#3, \x01)]",
		},
	}
	collate.SetNewDefCauslationEnabledForTest(true)
	for _, tt := range testCases {
		comment := Commentf("failed at case:\nexpr: %v\ncorDefCausIDs: %v\nidxDefCausIDs: %v\nidxDefCausLens: %v\naccess: %v\nremained: %v\n", tt.expr, tt.corDefCausIDs, tt.idxDefCausIDs, tt.idxDefCausLens, tt.access, tt.remained)
		filters, err := memex.ParseSimpleExprsWithNames(s.ctx, tt.expr, totalSchema, names)
		c.Assert(err, IsNil, comment)
		if sf, ok := filters[0].(*memex.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			filters = memex.FlattenCNFConditions(sf)
		}
		trueFilters := make([]memex.Expression, 0, len(filters))
		idMap := make(map[int64]struct{})
		for _, id := range tt.corDefCausIDs {
			idMap[id] = struct{}{}
		}
		for _, filter := range filters {
			trueFilter, err := s.SubstituteDefCaus2CorDefCaus(filter, idMap)
			c.Assert(err, IsNil, comment)
			trueFilters = append(trueFilters, trueFilter)
		}
		path := soliton.AccessPath{
			EqCondCount:    0,
			BlockFilters:   trueFilters,
			IdxDefCauss:    memex.FindPrefixOfIndex(totalSchema.DeferredCausets, tt.idxDefCausIDs),
			IdxDefCausLens: tt.idxDefCausLens,
		}

		access, remained := path.SplitCorDefCausAccessCondFromFilters(s.ctx, path.EqCondCount)
		c.Assert(fmt.Sprintf("%s", access), Equals, tt.access, comment)
		c.Assert(fmt.Sprintf("%s", remained), Equals, tt.remained, comment)
	}
	collate.SetNewDefCauslationEnabledForTest(false)
}
