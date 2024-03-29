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

package aggregation

import (
	"testing"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
)

func BenchmarkCreateContext(b *testing.B) {
	defCaus := &memex.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []memex.Expression{defCaus}, false)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.CreateContext(ctx.GetStochastikVars().StmtCtx)
	}
	b.ReportAllocs()
}

func BenchmarkResetContext(b *testing.B) {
	defCaus := &memex.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []memex.Expression{defCaus}, false)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	evalCtx := fun.CreateContext(ctx.GetStochastikVars().StmtCtx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.ResetContext(ctx.GetStochastikVars().StmtCtx, evalCtx)
	}
	b.ReportAllocs()
}

func BenchmarkCreateDistinctContext(b *testing.B) {
	defCaus := &memex.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []memex.Expression{defCaus}, true)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.CreateContext(ctx.GetStochastikVars().StmtCtx)
	}
	b.ReportAllocs()
}

func BenchmarkResetDistinctContext(b *testing.B) {
	defCaus := &memex.DeferredCauset{
		Index:   0,
		RetType: types.NewFieldType(allegrosql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(ctx, ast.AggFuncAvg, []memex.Expression{defCaus}, true)
	if err != nil {
		b.Fatal(err)
	}
	fun := desc.GetAggFunc(ctx)
	evalCtx := fun.CreateContext(ctx.GetStochastikVars().StmtCtx)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		fun.ResetContext(ctx.GetStochastikVars().StmtCtx, evalCtx)
	}
	b.ReportAllocs()
}
