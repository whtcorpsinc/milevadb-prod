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
	"testing"
	"time"

	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

func (s *testChunkSuite) TestMutRow(c *check.C) {
	mutRow := MutRowFromTypes(allTypes)
	event := mutRow.ToRow()
	sc := new(stmtctx.StatementContext)
	for i := 0; i < event.Len(); i++ {
		val := zeroValForType(allTypes[i])
		d := event.GetCauset(i, allTypes[i])
		d2 := types.NewCauset(val)
		cmp, err := d.CompareCauset(sc, &d2)
		c.Assert(err, check.IsNil)
		c.Assert(cmp, check.Equals, 0)
	}

	mutRow = MutRowFromValues("abc", 123)
	c.Assert(event.IsNull(0), check.IsFalse)
	c.Assert(mutRow.ToRow().GetString(0), check.Equals, "abc")
	c.Assert(event.IsNull(1), check.IsFalse)
	c.Assert(mutRow.ToRow().GetInt64(1), check.Equals, int64(123))
	mutRow.SetValues("abcd", 456)
	event = mutRow.ToRow()
	c.Assert(event.GetString(0), check.Equals, "abcd")
	c.Assert(event.IsNull(0), check.IsFalse)
	c.Assert(event.GetInt64(1), check.Equals, int64(456))
	c.Assert(event.IsNull(1), check.IsFalse)
	mutRow.SetCausets(types.NewStringCauset("defgh"), types.NewIntCauset(33))
	c.Assert(event.IsNull(0), check.IsFalse)
	c.Assert(event.GetString(0), check.Equals, "defgh")
	c.Assert(event.IsNull(1), check.IsFalse)
	c.Assert(event.GetInt64(1), check.Equals, int64(33))

	mutRow.SetRow(MutRowFromValues("foobar", nil).ToRow())
	event = mutRow.ToRow()
	c.Assert(event.IsNull(0), check.IsFalse)
	c.Assert(event.IsNull(1), check.IsTrue)

	nRow := MutRowFromValues(nil, 111).ToRow()
	c.Assert(nRow.IsNull(0), check.IsTrue)
	c.Assert(nRow.IsNull(1), check.IsFalse)
	mutRow.SetRow(nRow)
	event = mutRow.ToRow()
	c.Assert(event.IsNull(0), check.IsTrue)
	c.Assert(event.IsNull(1), check.IsFalse)

	j, err := json.ParseBinaryFromString("true")
	t := types.NewTime(types.FromDate(2000, 1, 1, 1, 0, 0, 0), allegrosql.TypeDatetime, types.MaxFsp)
	c.Assert(err, check.IsNil)
	mutRow = MutRowFromValues(j, t)
	event = mutRow.ToRow()
	c.Assert(event.GetJSON(0), check.DeepEquals, j)
	c.Assert(event.GetTime(1), check.DeepEquals, t)

	retTypes := []*types.FieldType{types.NewFieldType(allegrosql.TypeDuration)}
	chk := New(retTypes, 1, 1)
	dur, err := types.ParseDuration(sc, "01:23:45", 0)
	c.Assert(err, check.IsNil)
	chk.AppendDuration(0, dur)
	mutRow = MutRowFromTypes(retTypes)
	mutRow.SetValue(0, dur)
	c.Assert(chk.defCausumns[0].data, check.BytesEquals, mutRow.c.defCausumns[0].data)
	mutRow.SetCauset(0, types.NewDurationCauset(dur))
	c.Assert(chk.defCausumns[0].data, check.BytesEquals, mutRow.c.defCausumns[0].data)
}

func BenchmarkMutRowSetRow(b *testing.B) {
	b.ReportAllocs()
	rowChk := newChunk(8, 0)
	rowChk.AppendInt64(0, 1)
	rowChk.AppendString(1, "abcd")
	event := rowChk.GetRow(0)
	mutRow := MutRowFromValues(1, "abcd")
	for i := 0; i < b.N; i++ {
		mutRow.SetRow(event)
	}
}

func BenchmarkMutRowSetCausets(b *testing.B) {
	b.ReportAllocs()
	mutRow := MutRowFromValues(1, "abcd")
	datums := []types.Causet{types.NewCauset(1), types.NewCauset("abcd")}
	for i := 0; i < b.N; i++ {
		mutRow.SetCausets(datums...)
	}
}

func BenchmarkMutRowSetValues(b *testing.B) {
	b.ReportAllocs()
	mutRow := MutRowFromValues(1, "abcd")
	for i := 0; i < b.N; i++ {
		mutRow.SetValues(1, "abcd")
	}
}

func BenchmarkMutRowFromTypes(b *testing.B) {
	b.ReportAllocs()
	tps := []*types.FieldType{
		types.NewFieldType(allegrosql.TypeLonglong),
		types.NewFieldType(allegrosql.TypeVarchar),
	}
	for i := 0; i < b.N; i++ {
		MutRowFromTypes(tps)
	}
}

func BenchmarkMutRowFromCausets(b *testing.B) {
	b.ReportAllocs()
	datums := []types.Causet{types.NewCauset(1), types.NewCauset("abc")}
	for i := 0; i < b.N; i++ {
		MutRowFromCausets(datums)
	}
}

func BenchmarkMutRowFromValues(b *testing.B) {
	b.ReportAllocs()
	values := []interface{}{1, "abc"}
	for i := 0; i < b.N; i++ {
		MutRowFromValues(values)
	}
}

func (s *testChunkSuite) TestMutRowShallowCopyPartialRow(c *check.C) {
	defCausTypes := make([]*types.FieldType, 0, 3)
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarString})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeTimestamp})

	mutRow := MutRowFromTypes(defCausTypes)
	event := MutRowFromValues("abc", 123, types.ZeroTimestamp).ToRow()
	mutRow.ShallowCopyPartialRow(0, event)
	c.Assert(event.GetString(0), check.Equals, mutRow.ToRow().GetString(0))
	c.Assert(event.GetInt64(1), check.Equals, mutRow.ToRow().GetInt64(1))
	c.Assert(event.GetTime(2), check.DeepEquals, mutRow.ToRow().GetTime(2))

	event.c.Reset()
	d := types.NewStringCauset("dfg")
	event.c.AppendCauset(0, &d)
	d = types.NewIntCauset(567)
	event.c.AppendCauset(1, &d)
	d = types.NewTimeCauset(types.NewTime(types.FromGoTime(time.Now()), allegrosql.TypeTimestamp, 6))
	event.c.AppendCauset(2, &d)

	c.Assert(d.GetMysqlTime(), check.DeepEquals, mutRow.ToRow().GetTime(2))
	c.Assert(event.GetString(0), check.Equals, mutRow.ToRow().GetString(0))
	c.Assert(event.GetInt64(1), check.Equals, mutRow.ToRow().GetInt64(1))
	c.Assert(event.GetTime(2), check.DeepEquals, mutRow.ToRow().GetTime(2))
}

var rowsNum = 1024

func BenchmarkMutRowShallowCopyPartialRow(b *testing.B) {
	b.ReportAllocs()
	defCausTypes := make([]*types.FieldType, 0, 8)
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarString})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeVarString})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	defCausTypes = append(defCausTypes, &types.FieldType{Tp: allegrosql.TypeDatetime})

	mutRow := MutRowFromTypes(defCausTypes)
	event := MutRowFromValues("abc", "abcdefg", 123, 456, types.ZeroDatetime).ToRow()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < rowsNum; j++ {
			mutRow.ShallowCopyPartialRow(0, event)
		}
	}
}

func BenchmarkChunkAppendPartialRow(b *testing.B) {
	b.ReportAllocs()
	chk := newChunkWithInitCap(rowsNum, 0, 0, 8, 8, sizeTime)
	event := MutRowFromValues("abc", "abcdefg", 123, 456, types.ZeroDatetime).ToRow()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chk.Reset()
		for j := 0; j < rowsNum; j++ {
			chk.AppendPartialRow(0, event)
		}
	}
}
