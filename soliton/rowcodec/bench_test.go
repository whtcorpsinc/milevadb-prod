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

package rowcodec_test

import (
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
)

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeCausets(1, "abc", 1.1)
	var xb rowcodec.Encoder
	var buf []byte
	defCausIDs := []int64{1, 2, 3}
	var err error
	for i := 0; i < b.N; i++ {
		buf, err = xb.Encode(nil, defCausIDs, oldRow, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFromOldRow(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeCausets(1, "abc", 1.1)
	oldRowData, err := blockcodec.EncodeOldRow(new(stmtctx.StatementContext), oldRow, []int64{1, 2, 3}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	var xb rowcodec.Encoder
	var buf []byte
	for i := 0; i < b.N; i++ {
		buf, err = rowcodec.EncodeFromOldRow(&xb, nil, oldRowData, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	b.ReportAllocs()
	oldRow := types.MakeCausets(1, "abc", 1.1)
	defCausIDs := []int64{-1, 2, 3}
	tps := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeString),
		types.NewFieldType(allegrosql.TypeDouble),
	}
	var xb rowcodec.Encoder
	xRowData, err := xb.Encode(nil, defCausIDs, oldRow, nil)
	if err != nil {
		b.Fatal(err)
	}
	defcaus := make([]rowcodec.DefCausInfo, len(tps))
	for i, tp := range tps {
		defcaus[i] = rowcodec.DefCausInfo{
			ID: defCausIDs[i],
			Ft: tp,
		}
	}
	decoder := rowcodec.NewChunkDecoder(defcaus, []int64{-1}, nil, time.Local)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	for i := 0; i < b.N; i++ {
		chk.Reset()
		err = decoder.DecodeToChunk(xRowData, ekv.IntHandle(1), chk)
		if err != nil {
			b.Fatal(err)
		}
	}
}
