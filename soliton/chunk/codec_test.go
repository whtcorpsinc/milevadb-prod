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

package chunk

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

var _ = check.Suite(&testCodecSuite{})

type testCodecSuite struct{}

func (s *testCodecSuite) TestCodec(c *check.C) {
	if runtime.Version() >= "go1.14" {
		// TODO: fix https://github.com/whtcorpsinc/milevadb/issues/15154
		c.Skip("cannot pass checkptr, TODO to fix https://github.com/whtcorpsinc/milevadb/issues/15154")
	}
	numDefCauss := 6
	numRows := 10

	defCausTypes := make([]*types.FieldType, 0, numDefCauss)
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeNewDecimal})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeJSON})

	oldChk := NewChunkWithCapacity(defCausTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
		oldChk.AppendString(3, str)
		oldChk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		oldChk.AppendJSON(5, json.CreateBinary(str))
	}

	codec := NewCodec(defCausTypes)
	buffer := codec.Encode(oldChk)

	newChk := NewChunkWithCapacity(defCausTypes, numRows)
	remained := codec.DecodeToChunk(buffer, newChk)

	c.Assert(len(remained), check.Equals, 0)
	c.Assert(newChk.NumDefCauss(), check.Equals, numDefCauss)
	c.Assert(newChk.NumRows(), check.Equals, numRows)
	for i := 0; i < numRows; i++ {
		event := newChk.GetRow(i)
		str := fmt.Sprintf("%d.12345", i)
		c.Assert(event.IsNull(0), check.IsTrue)
		c.Assert(event.IsNull(1), check.IsFalse)
		c.Assert(event.IsNull(2), check.IsFalse)
		c.Assert(event.IsNull(3), check.IsFalse)
		c.Assert(event.IsNull(4), check.IsFalse)
		c.Assert(event.IsNull(5), check.IsFalse)

		c.Assert(event.GetInt64(1), check.Equals, int64(i))
		c.Assert(event.GetString(2), check.Equals, str)
		c.Assert(event.GetString(3), check.Equals, str)
		c.Assert(event.GetMyDecimal(4).String(), check.Equals, str)
		c.Assert(string(event.GetJSON(5).GetString()), check.Equals, str)
	}
}

func (s *testCodecSuite) TestEstimateTypeWidth(c *check.C) {
	var defCausType *types.FieldType

	defCausType = &types.FieldType{Tp: allegrosql.TypeLonglong}
	c.Assert(EstimateTypeWidth(defCausType), check.Equals, 8) // fixed-witch type

	defCausType = &types.FieldType{Tp: allegrosql.TypeString, Flen: 31}
	c.Assert(EstimateTypeWidth(defCausType), check.Equals, 31) // defCausLen <= 32

	defCausType = &types.FieldType{Tp: allegrosql.TypeString, Flen: 999}
	c.Assert(EstimateTypeWidth(defCausType), check.Equals, 515) // defCausLen < 1000

	defCausType = &types.FieldType{Tp: allegrosql.TypeString, Flen: 2000}
	c.Assert(EstimateTypeWidth(defCausType), check.Equals, 516) // defCausLen < 1000

	defCausType = &types.FieldType{Tp: allegrosql.TypeString}
	c.Assert(EstimateTypeWidth(defCausType), check.Equals, 32) // value after guessing
}

func BenchmarkEncodeChunk(b *testing.B) {
	numDefCauss := 4
	numRows := 1024

	chk := &Chunk{defCausumns: make([]*DeferredCauset, numDefCauss)}
	for i := 0; i < numDefCauss; i++ {
		chk.defCausumns[i] = &DeferredCauset{
			length:     numRows,
			nullBitmap: make([]byte, numRows/8+1),
			data:       make([]byte, numRows*8),
		}
	}

	codec := &Codec{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Encode(chk)
	}
}

func BenchmarkDecode(b *testing.B) {
	numDefCauss := 4
	numRows := 1024

	defCausTypes := make([]*types.FieldType, numDefCauss)
	chk := &Chunk{defCausumns: make([]*DeferredCauset, numDefCauss)}
	for i := 0; i < numDefCauss; i++ {
		chk.defCausumns[i] = &DeferredCauset{
			length:     numRows,
			nullBitmap: make([]byte, numRows/8+1),
			data:       make([]byte, numRows*8),
		}
		defCausTypes[i] = &types.FieldType{
			Tp: allegrosql.TypeLonglong,
		}
	}
	codec := &Codec{defCausTypes}
	buffer := codec.Encode(chk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Decode(buffer)
	}
}

func BenchmarkDecodeToChunk(b *testing.B) {
	numDefCauss := 4
	numRows := 1024

	defCausTypes := make([]*types.FieldType, numDefCauss)
	chk := &Chunk{
		defCausumns: make([]*DeferredCauset, numDefCauss),
	}
	for i := 0; i < numDefCauss; i++ {
		chk.defCausumns[i] = &DeferredCauset{
			length:     numRows,
			nullBitmap: make([]byte, numRows/8+1),
			data:       make([]byte, numRows*8),
			elemBuf:    make([]byte, 8),
		}
		defCausTypes[i] = &types.FieldType{
			Tp: allegrosql.TypeLonglong,
		}
	}
	codec := &Codec{defCausTypes}
	buffer := codec.Encode(chk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.DecodeToChunk(buffer, chk)
	}
}

func BenchmarkDecodeToChunkWithVariableType(b *testing.B) {
	numDefCauss := 6
	numRows := 1024

	defCausTypes := make([]*types.FieldType, 0, numDefCauss)
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeNewDecimal})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeJSON})

	chk := NewChunkWithCapacity(defCausTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		chk.AppendNull(0)
		chk.AppendInt64(1, int64(i))
		chk.AppendString(2, str)
		chk.AppendString(3, str)
		chk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		chk.AppendJSON(5, json.CreateBinary(str))
	}
	codec := &Codec{defCausTypes}
	buffer := codec.Encode(chk)

	chk.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.DecodeToChunk(buffer, chk)
	}
}
