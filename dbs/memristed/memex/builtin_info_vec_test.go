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
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/types"
)

type milevadbKeyGener struct {
	inner *defaultGener
}

func (g *milevadbKeyGener) gen() interface{} {
	blockID := g.inner.gen().(int64)
	var result []byte
	if rand.Intn(2) == 1 {
		// Generate a record key
		handle := g.inner.gen().(int64)
		result = blockcodec.EncodeEventKeyWithHandle(blockID, ekv.IntHandle(handle))
	} else {
		// Generate an index key
		idx := g.inner.gen().(int64)
		result = blockcodec.EncodeBlockIndexPrefix(blockID, idx)
	}
	return hex.EncodeToString(result)
}

var vecBuiltinInfoCases = map[string][]vecExprBenchCase{
	ast.Version: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.MilevaDBVersion: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.CurrentUser: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.FoundEvents: {
		{retEvalType: types.ETInt},
	},
	ast.Database: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.User: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.MilevaDBDecodeKey: {
		{
			retEvalType:   types.ETString,
			childrenTypes: []types.EvalType{types.ETString},
			geners: []dataGenerator{&milevadbKeyGener{
				inner: newDefaultGener(0, types.ETInt),
			}},
		},
	},
	ast.EventCount: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{}},
	},
	ast.CurrentRole: {
		{retEvalType: types.ETString, childrenTypes: []types.EvalType{}},
	},
	ast.MilevaDBIsDBSTenant: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{}},
	},
	ast.ConnectionID: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{}},
	},
	ast.LastInsertId: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt}},
	},
	ast.Benchmark: {
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			constants: []*Constant{{Value: types.NewIntCauset(10), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETReal},
			constants: []*Constant{{Value: types.NewIntCauset(11), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETDecimal},
			constants: []*Constant{{Value: types.NewIntCauset(12), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETString},
			constants: []*Constant{{Value: types.NewIntCauset(13), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETDatetime},
			constants: []*Constant{{Value: types.NewIntCauset(14), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETTimestamp},
			constants: []*Constant{{Value: types.NewIntCauset(15), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETDuration},
			constants: []*Constant{{Value: types.NewIntCauset(16), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETJson},
			constants: []*Constant{{Value: types.NewIntCauset(17), RetType: types.NewFieldType(allegrosql.TypeLonglong)}, nil}},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinInfoFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinInfoCases)
}

func BenchmarkVectorizedBuiltinInfoFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinInfoCases)
}
