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

// Code generated by go generate in memex/generator; DO NOT EDIT.

package memex

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/types"
)

var vecGeneratedBuiltinStringCases = map[string][]vecExprBenchCase{
	ast.Field: {

		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt, types.ETInt, types.ETInt}},

		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETReal, types.ETReal, types.ETReal, types.ETReal}},

		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETString, types.ETString, types.ETString, types.ETString}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedGeneratedBuiltinStringEvalOneVec(c *C) {
	testVectorizedEvalOneVec(c, vecGeneratedBuiltinStringCases)
}

func (s *testEvaluatorSuite) TestVectorizedGeneratedBuiltinStringFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecGeneratedBuiltinStringCases)
}

func BenchmarkVectorizedGeneratedBuiltinStringEvalOneVec(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecGeneratedBuiltinStringCases)
}

func BenchmarkVectorizedGeneratedBuiltinStringFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecGeneratedBuiltinStringCases)
}
