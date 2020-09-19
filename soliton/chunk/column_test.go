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
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

func equalDeferredCauset(c1, c2 *DeferredCauset) bool {
	if c1.length != c2.length ||
		c1.nullCount() != c2.nullCount() {
		return false
	}
	if len(c1.nullBitmap) != len(c2.nullBitmap) ||
		len(c1.offsets) != len(c2.offsets) ||
		len(c1.data) != len(c2.data) ||
		len(c1.elemBuf) != len(c2.elemBuf) {
		return false
	}
	for i := range c1.nullBitmap {
		if c1.nullBitmap[i] != c2.nullBitmap[i] {
			return false
		}
	}
	for i := range c1.offsets {
		if c1.offsets[i] != c2.offsets[i] {
			return false
		}
	}
	for i := range c1.data {
		if c1.data[i] != c2.data[i] {
			return false
		}
	}
	for i := range c1.elemBuf {
		if c1.elemBuf[i] != c2.elemBuf[i] {
			return false
		}
	}
	return true
}

func (s *testChunkSuite) TestDeferredCausetCopy(c *check.C) {
	defCaus := newFixedLenDeferredCauset(8, 10)
	for i := 0; i < 10; i++ {
		defCaus.AppendInt64(int64(i))
	}

	c1 := defCaus.CopyConstruct(nil)
	c.Check(equalDeferredCauset(defCaus, c1), check.IsTrue)

	c2 := newFixedLenDeferredCauset(8, 10)
	c2 = defCaus.CopyConstruct(c2)
	c.Check(equalDeferredCauset(defCaus, c2), check.IsTrue)
}

func (s *testChunkSuite) TestDeferredCausetCopyReconstructFixedLen(c *check.C) {
	defCaus := NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), 1024)
	results := make([]int64, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			defCaus.AppendNull()
			nulls = append(nulls, true)
			results = append(results, 0)
			continue
		}

		v := rand.Int63()
		defCaus.AppendInt64(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	defCaus = defCaus.CopyReconstruct(sel, nil)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(defCaus.IsNull(n), check.Equals, true)
		} else {
			c.Assert(defCaus.GetInt64(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, defCaus.nullCount())
	c.Assert(defCaus.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			defCaus.AppendNull()
		} else {
			defCaus.AppendInt64(int64(i * i * i))
		}
	}

	c.Assert(defCaus.length, check.Equals, len(sel)+128)
	c.Assert(defCaus.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(defCaus.GetInt64(len(sel)+i), check.Equals, int64(i*i*i))
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, false)
		}
	}
}

func (s *testChunkSuite) TestDeferredCausetCopyReconstructVarLen(c *check.C) {
	defCaus := NewDeferredCauset(types.NewFieldType(allegrosql.TypeVarString), 1024)
	results := make([]string, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			defCaus.AppendNull()
			nulls = append(nulls, true)
			results = append(results, "")
			continue
		}

		v := fmt.Sprintf("%v", rand.Int63())
		defCaus.AppendString(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	defCaus = defCaus.CopyReconstruct(sel, nil)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(defCaus.IsNull(n), check.Equals, true)
		} else {
			c.Assert(defCaus.GetString(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, defCaus.nullCount())
	c.Assert(defCaus.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			defCaus.AppendNull()
		} else {
			defCaus.AppendString(fmt.Sprintf("%v", i*i*i))
		}
	}

	c.Assert(defCaus.length, check.Equals, len(sel)+128)
	c.Assert(defCaus.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(defCaus.GetString(len(sel)+i), check.Equals, fmt.Sprintf("%v", i*i*i))
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, false)
		}
	}
}

func (s *testChunkSuite) TestLargeStringDeferredCausetOffset(c *check.C) {
	numRows := 1
	defCaus := newVarLenDeferredCauset(numRows, nil)
	defCaus.offsets[0] = 6 << 30
	c.Check(defCaus.offsets[0], check.Equals, int64(6<<30)) // test no overflow.
}

func (s *testChunkSuite) TestI64DeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendInt64(int64(i))
	}

	i64s := defCaus.Int64s()
	for i := 0; i < 1024; i++ {
		c.Assert(i64s[i], check.Equals, int64(i))
		i64s[i]++
	}

	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		c.Assert(event.GetInt64(0), check.Equals, int64(i+1))
		c.Assert(defCaus.GetInt64(i), check.Equals, int64(i+1))
		i++
	}
}

func (s *testChunkSuite) TestF64DeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDouble)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendFloat64(float64(i))
	}

	f64s := defCaus.Float64s()
	for i := 0; i < 1024; i++ {
		c.Assert(f64s[i], check.Equals, float64(i))
		f64s[i] /= 2
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for event := it.Begin(); event != it.End(); event = it.Next() {
		c.Assert(event.GetFloat64(0), check.Equals, float64(i)/2)
		c.Assert(defCaus.GetFloat64(int(i)), check.Equals, float64(i)/2)
		i++
	}
}

func (s *testChunkSuite) TestF32DeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeFloat)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendFloat32(float32(i))
	}

	f32s := defCaus.Float32s()
	for i := 0; i < 1024; i++ {
		c.Assert(f32s[i], check.Equals, float32(i))
		f32s[i] /= 2
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for event := it.Begin(); event != it.End(); event = it.Next() {
		c.Assert(event.GetFloat32(0), check.Equals, float32(i)/2)
		c.Assert(defCaus.GetFloat32(int(i)), check.Equals, float32(i)/2)
		i++
	}
}

func (s *testChunkSuite) TestDurationSliceDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDuration)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendDuration(types.Duration{Duration: time.Duration(i)})
	}

	ds := defCaus.GoDurations()
	for i := 0; i < 1024; i++ {
		c.Assert(ds[i], check.Equals, time.Duration(i))
		d := types.Duration{Duration: ds[i]}
		d, _ = d.Add(d)
		ds[i] = d.Duration
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for event := it.Begin(); event != it.End(); event = it.Next() {
		c.Assert(event.GetDuration(0, 0).Duration, check.Equals, time.Duration(i)*2)
		c.Assert(defCaus.GetDuration(int(i), 0).Duration, check.Equals, time.Duration(i)*2)
		i++
	}
}

func (s *testChunkSuite) TestMyDecimal(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeNewDecimal)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		d := new(types.MyDecimal)
		if err := d.FromFloat64(float64(i) * 1.1); err != nil {
			c.Fatal(err)
		}
		defCaus.AppendMyDecimal(d)
	}

	ds := defCaus.Decimals()
	for i := 0; i < 1024; i++ {
		d := new(types.MyDecimal)
		if err := d.FromFloat64(float64(i) * 1.1); err != nil {
			c.Fatal(err)
		}
		c.Assert(d.Compare(&ds[i]), check.Equals, 0)

		if err := types.DecimalAdd(&ds[i], d, &ds[i]); err != nil {
			c.Fatal(err)
		}
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for event := it.Begin(); event != it.End(); event = it.Next() {
		d := new(types.MyDecimal)
		if err := d.FromFloat64(float64(i) * 1.1 * 2); err != nil {
			c.Fatal(err)
		}

		delta := new(types.MyDecimal)
		if err := types.DecimalSub(d, event.GetMyDecimal(0), delta); err != nil {
			c.Fatal(err)
		}

		fDelta, err := delta.ToFloat64()
		if err != nil {
			c.Fatal(err)
		}
		if fDelta > 0.0001 || fDelta < -0.0001 {
			c.Fatal()
		}

		i++
	}
}

func (s *testChunkSuite) TestStringDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeVarString)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendString(fmt.Sprintf("%v", i*i))
	}

	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		c.Assert(event.GetString(0), check.Equals, fmt.Sprintf("%v", i*i))
		c.Assert(defCaus.GetString(i), check.Equals, fmt.Sprintf("%v", i*i))
		i++
	}
}

func (s *testChunkSuite) TestSetDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeSet)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendSet(types.Set{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}

	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		s1 := defCaus.GetSet(i)
		s2 := event.GetSet(0)
		c.Assert(s1.Name, check.Equals, s2.Name)
		c.Assert(s1.Value, check.Equals, s2.Value)
		c.Assert(s1.Name, check.Equals, fmt.Sprintf("%v", i))
		c.Assert(s1.Value, check.Equals, uint64(i))
		i++
	}
}

func (s *testChunkSuite) TestJSONDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeJSON)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		j := new(json.BinaryJSON)
		if err := j.UnmarshalJSON([]byte(fmt.Sprintf(`{"%v":%v}`, i, i))); err != nil {
			c.Fatal(err)
		}
		defCaus.AppendJSON(*j)
	}

	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		j1 := defCaus.GetJSON(i)
		j2 := event.GetJSON(0)
		c.Assert(j1.String(), check.Equals, j2.String())
		i++
	}
}

func (s *testChunkSuite) TestTimeDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDatetime)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendTime(types.CurrentTime(allegrosql.TypeDatetime))
		time.Sleep(time.Millisecond / 10)
	}

	it := NewIterator4Chunk(chk)
	ts := defCaus.Times()
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		j1 := defCaus.GetTime(i)
		j2 := event.GetTime(0)
		j3 := ts[i]
		c.Assert(j1.Compare(j2), check.Equals, 0)
		c.Assert(j1.Compare(j3), check.Equals, 0)
		i++
	}
}

func (s *testChunkSuite) TestDurationDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDuration)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendDuration(types.Duration{Duration: time.Second * time.Duration(i)})
	}

	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		j1 := defCaus.GetDuration(i, 0)
		j2 := event.GetDuration(0, 0)
		c.Assert(j1.Compare(j2), check.Equals, 0)
		i++
	}
}

func (s *testChunkSuite) TestEnumDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeEnum)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendEnum(types.Enum{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}

	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		s1 := defCaus.GetEnum(i)
		s2 := event.GetEnum(0)
		c.Assert(s1.Name, check.Equals, s2.Name)
		c.Assert(s1.Value, check.Equals, s2.Value)
		c.Assert(s1.Name, check.Equals, fmt.Sprintf("%v", i))
		c.Assert(s1.Value, check.Equals, uint64(i))
		i++
	}
}

func (s *testChunkSuite) TestNullsDeferredCauset(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		if i%2 == 0 {
			defCaus.AppendNull()
			continue
		}
		defCaus.AppendInt64(int64(i))
	}

	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		if i%2 == 0 {
			c.Assert(event.IsNull(0), check.Equals, true)
			c.Assert(defCaus.IsNull(i), check.Equals, true)
		} else {
			c.Assert(event.GetInt64(0), check.Equals, int64(i))
		}
		i++
	}
}

func (s *testChunkSuite) TestReconstructFixedLen(c *check.C) {
	defCaus := NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), 1024)
	results := make([]int64, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			defCaus.AppendNull()
			nulls = append(nulls, true)
			results = append(results, 0)
			continue
		}

		v := rand.Int63()
		defCaus.AppendInt64(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	defCaus.reconstruct(sel)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(defCaus.IsNull(n), check.Equals, true)
		} else {
			c.Assert(defCaus.GetInt64(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, defCaus.nullCount())
	c.Assert(defCaus.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			defCaus.AppendNull()
		} else {
			defCaus.AppendInt64(int64(i * i * i))
		}
	}

	c.Assert(defCaus.length, check.Equals, len(sel)+128)
	c.Assert(defCaus.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(defCaus.GetInt64(len(sel)+i), check.Equals, int64(i*i*i))
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, false)
		}
	}
}

func (s *testChunkSuite) TestReconstructVarLen(c *check.C) {
	defCaus := NewDeferredCauset(types.NewFieldType(allegrosql.TypeVarString), 1024)
	results := make([]string, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			defCaus.AppendNull()
			nulls = append(nulls, true)
			results = append(results, "")
			continue
		}

		v := fmt.Sprintf("%v", rand.Int63())
		defCaus.AppendString(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	defCaus.reconstruct(sel)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(defCaus.IsNull(n), check.Equals, true)
		} else {
			c.Assert(defCaus.GetString(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, defCaus.nullCount())
	c.Assert(defCaus.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			defCaus.AppendNull()
		} else {
			defCaus.AppendString(fmt.Sprintf("%v", i*i*i))
		}
	}

	c.Assert(defCaus.length, check.Equals, len(sel)+128)
	c.Assert(defCaus.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(defCaus.GetString(len(sel)+i), check.Equals, fmt.Sprintf("%v", i*i*i))
			c.Assert(defCaus.IsNull(len(sel)+i), check.Equals, false)
		}
	}
}

func (s *testChunkSuite) TestPreAllocInt64(c *check.C) {
	defCaus := NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), 128)
	defCaus.ResizeInt64(256, true)
	i64s := defCaus.Int64s()
	c.Assert(len(i64s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(defCaus.IsNull(i), check.Equals, true)
	}
	defCaus.AppendInt64(2333)
	c.Assert(defCaus.IsNull(256), check.Equals, false)
	c.Assert(len(defCaus.Int64s()), check.Equals, 257)
	c.Assert(defCaus.Int64s()[256], check.Equals, int64(2333))
}

func (s *testChunkSuite) TestPreAllocUint64(c *check.C) {
	tll := types.NewFieldType(allegrosql.TypeLonglong)
	tll.Flag |= allegrosql.UnsignedFlag
	defCaus := NewDeferredCauset(tll, 128)
	defCaus.ResizeUint64(256, true)
	u64s := defCaus.Uint64s()
	c.Assert(len(u64s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(defCaus.IsNull(i), check.Equals, true)
	}
	defCaus.AppendUint64(2333)
	c.Assert(defCaus.IsNull(256), check.Equals, false)
	c.Assert(len(defCaus.Uint64s()), check.Equals, 257)
	c.Assert(defCaus.Uint64s()[256], check.Equals, uint64(2333))
}

func (s *testChunkSuite) TestPreAllocFloat32(c *check.C) {
	defCaus := newFixedLenDeferredCauset(sizeFloat32, 128)
	defCaus.ResizeFloat32(256, true)
	f32s := defCaus.Float32s()
	c.Assert(len(f32s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(defCaus.IsNull(i), check.Equals, true)
	}
	defCaus.AppendFloat32(2333)
	c.Assert(defCaus.IsNull(256), check.Equals, false)
	c.Assert(len(defCaus.Float32s()), check.Equals, 257)
	c.Assert(defCaus.Float32s()[256], check.Equals, float32(2333))
}

func (s *testChunkSuite) TestPreAllocFloat64(c *check.C) {
	defCaus := newFixedLenDeferredCauset(sizeFloat64, 128)
	defCaus.ResizeFloat64(256, true)
	f64s := defCaus.Float64s()
	c.Assert(len(f64s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(defCaus.IsNull(i), check.Equals, true)
	}
	defCaus.AppendFloat64(2333)
	c.Assert(defCaus.IsNull(256), check.Equals, false)
	c.Assert(len(defCaus.Float64s()), check.Equals, 257)
	c.Assert(defCaus.Float64s()[256], check.Equals, float64(2333))
}

func (s *testChunkSuite) TestPreAllocDecimal(c *check.C) {
	defCaus := newFixedLenDeferredCauset(sizeMyDecimal, 128)
	defCaus.ResizeDecimal(256, true)
	ds := defCaus.Decimals()
	c.Assert(len(ds), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(defCaus.IsNull(i), check.Equals, true)
	}
	defCaus.AppendMyDecimal(new(types.MyDecimal))
	c.Assert(defCaus.IsNull(256), check.Equals, false)
	c.Assert(len(defCaus.Float64s()), check.Equals, 257)
}

func (s *testChunkSuite) TestPreAllocTime(c *check.C) {
	defCaus := newFixedLenDeferredCauset(sizeTime, 128)
	defCaus.ResizeTime(256, true)
	ds := defCaus.Times()
	c.Assert(len(ds), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(defCaus.IsNull(i), check.Equals, true)
	}
	defCaus.AppendTime(types.ZeroDatetime)
	c.Assert(defCaus.IsNull(256), check.Equals, false)
	c.Assert(len(defCaus.Times()), check.Equals, 257)
}

func (s *testChunkSuite) TestNull(c *check.C) {
	defCaus := newFixedLenDeferredCauset(sizeFloat64, 32)
	defCaus.ResizeFloat64(1024, true)
	c.Assert(defCaus.nullCount(), check.Equals, 1024)

	notNulls := make(map[int]struct{})
	for i := 0; i < 512; i++ {
		idx := rand.Intn(1024)
		notNulls[idx] = struct{}{}
		defCaus.SetNull(idx, false)
	}

	c.Assert(defCaus.nullCount(), check.Equals, 1024-len(notNulls))
	for idx := range notNulls {
		c.Assert(defCaus.IsNull(idx), check.Equals, false)
	}

	defCaus.ResizeFloat64(8, true)
	defCaus.SetNulls(0, 8, true)
	defCaus.SetNull(7, false)
	c.Assert(defCaus.nullCount(), check.Equals, 7)

	defCaus.ResizeFloat64(8, true)
	defCaus.SetNulls(0, 8, true)
	c.Assert(defCaus.nullCount(), check.Equals, 8)

	defCaus.ResizeFloat64(9, true)
	defCaus.SetNulls(0, 9, true)
	defCaus.SetNull(8, false)
	c.Assert(defCaus.nullCount(), check.Equals, 8)
}

func (s *testChunkSuite) TestSetNulls(c *check.C) {
	defCaus := newFixedLenDeferredCauset(sizeFloat64, 32)
	defCaus.ResizeFloat64(1024, true)
	c.Assert(defCaus.nullCount(), check.Equals, 1024)

	defCaus.SetNulls(0, 1024, false)
	c.Assert(defCaus.nullCount(), check.Equals, 0)

	nullMap := make(map[int]struct{})
	for i := 0; i < 100; i++ {
		begin := rand.Intn(1024)
		l := rand.Intn(37)
		end := begin + l
		if end > 1024 {
			end = 1024
		}
		for i := begin; i < end; i++ {
			nullMap[i] = struct{}{}
		}
		defCaus.SetNulls(begin, end, true)

		c.Assert(defCaus.nullCount(), check.Equals, len(nullMap))
		for k := range nullMap {
			c.Assert(defCaus.IsNull(k), check.Equals, true)
		}
	}
}

func (s *testChunkSuite) TestResizeReserve(c *check.C) {
	cI64s := newFixedLenDeferredCauset(sizeInt64, 0)
	c.Assert(cI64s.length, check.Equals, 0)
	for i := 0; i < 100; i++ {
		t := rand.Intn(1024)
		cI64s.ResizeInt64(t, true)
		c.Assert(cI64s.length, check.Equals, t)
		c.Assert(len(cI64s.Int64s()), check.Equals, t)
	}
	cI64s.ResizeInt64(0, true)
	c.Assert(cI64s.length, check.Equals, 0)
	c.Assert(len(cI64s.Int64s()), check.Equals, 0)

	cStrs := newVarLenDeferredCauset(0, nil)
	for i := 0; i < 100; i++ {
		t := rand.Intn(1024)
		cStrs.ReserveString(t)
		c.Assert(cStrs.length, check.Equals, 0)
	}
	cStrs.ReserveString(0)
	c.Assert(cStrs.length, check.Equals, 0)
}

func (s *testChunkSuite) TestGetRaw(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeFloat)}, 1024)
	defCaus := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendFloat32(float32(i))
	}
	it := NewIterator4Chunk(chk)
	var i int
	for event := it.Begin(); event != it.End(); event = it.Next() {
		f := float32(i)
		b := (*[unsafe.Sizeof(f)]byte)(unsafe.Pointer(&f))[:]
		c.Assert(event.GetRaw(0), check.DeepEquals, b)
		c.Assert(defCaus.GetRaw(i), check.DeepEquals, b)
		i++
	}

	chk = NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeVarString)}, 1024)
	defCaus = chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus.AppendString(fmt.Sprint(i))
	}
	it = NewIterator4Chunk(chk)
	i = 0
	for event := it.Begin(); event != it.End(); event = it.Next() {
		c.Assert(event.GetRaw(0), check.DeepEquals, []byte(fmt.Sprint(i)))
		c.Assert(defCaus.GetRaw(i), check.DeepEquals, []byte(fmt.Sprint(i)))
		i++
	}
}

func (s *testChunkSuite) TestResize(c *check.C) {
	defCaus := NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), 1024)
	for i := 0; i < 1024; i++ {
		defCaus.AppendInt64(int64(i))
	}
	defCaus.ResizeInt64(1024, false)
	for i := 0; i < 1024; i++ {
		c.Assert(defCaus.Int64s()[i], check.Equals, int64(0))
	}

	defCaus = NewDeferredCauset(types.NewFieldType(allegrosql.TypeFloat), 1024)
	for i := 0; i < 1024; i++ {
		defCaus.AppendFloat32(float32(i))
	}
	defCaus.ResizeFloat32(1024, false)
	for i := 0; i < 1024; i++ {
		c.Assert(defCaus.Float32s()[i], check.Equals, float32(0))
	}

	defCaus = NewDeferredCauset(types.NewFieldType(allegrosql.TypeDouble), 1024)
	for i := 0; i < 1024; i++ {
		defCaus.AppendFloat64(float64(i))
	}
	defCaus.ResizeFloat64(1024, false)
	for i := 0; i < 1024; i++ {
		c.Assert(defCaus.Float64s()[i], check.Equals, float64(0))
	}

	defCaus = NewDeferredCauset(types.NewFieldType(allegrosql.TypeNewDecimal), 1024)
	for i := 0; i < 1024; i++ {
		defCaus.AppendMyDecimal(new(types.MyDecimal).FromInt(int64(i)))
	}
	defCaus.ResizeDecimal(1024, false)
	for i := 0; i < 1024; i++ {
		var d types.MyDecimal
		c.Assert(defCaus.Decimals()[i], check.Equals, d)
	}

	defCaus = NewDeferredCauset(types.NewFieldType(allegrosql.TypeDuration), 1024)
	for i := 0; i < 1024; i++ {
		defCaus.AppendDuration(types.Duration{Duration: time.Duration(i), Fsp: int8(i)})
	}
	defCaus.ResizeGoDuration(1024, false)
	for i := 0; i < 1024; i++ {
		c.Assert(defCaus.GoDurations()[i], check.Equals, time.Duration(0))
	}

	defCaus = NewDeferredCauset(types.NewFieldType(allegrosql.TypeDatetime), 1024)
	for i := 0; i < 1024; i++ {
		gt := types.FromDate(rand.Intn(2200), rand.Intn(10)+1, rand.Intn(20)+1, rand.Intn(12), rand.Intn(60), rand.Intn(60), rand.Intn(1000000))
		t := types.NewTime(gt, 0, 0)
		defCaus.AppendTime(t)
	}
	defCaus.ResizeTime(1024, false)
	for i := 0; i < 1024; i++ {
		var t types.Time
		c.Assert(defCaus.Times()[i], check.Equals, t)
	}
}

func BenchmarkDurationRow(b *testing.B) {
	chk1 := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDuration)}, 1024)
	defCaus1 := chk1.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus1.AppendDuration(types.Duration{Duration: time.Second * time.Duration(i)})
	}
	chk2 := chk1.CopyConstruct()
	result := chk1.CopyConstruct()

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		result.Reset()
		it1 := NewIterator4Chunk(chk1)
		it2 := NewIterator4Chunk(chk2)
		for r1, r2 := it1.Begin(), it2.Begin(); r1 != it1.End() && r2 != it2.End(); r1, r2 = it1.Next(), it2.Next() {
			d1 := r1.GetDuration(0, 0)
			d2 := r2.GetDuration(0, 0)
			r, err := d1.Add(d2)
			if err != nil {
				b.Fatal(err)
			}
			result.AppendDuration(0, r)
		}
	}
}

func BenchmarkDurationVec(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDuration)}, 1024)
	defCaus1 := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus1.AppendDuration(types.Duration{Duration: time.Second * time.Duration(i)})
	}
	defCaus2 := defCaus1.CopyConstruct(nil)
	result := defCaus1.CopyConstruct(nil)

	ds1 := defCaus1.GoDurations()
	ds2 := defCaus2.GoDurations()
	rs := result.GoDurations()

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		result.ResizeGoDuration(1024, true)
		for i := 0; i < 1024; i++ {
			d1 := types.Duration{Duration: ds1[i]}
			d2 := types.Duration{Duration: ds2[i]}
			r, err := d1.Add(d2)
			if err != nil {
				b.Fatal(err)
			}
			rs[i] = r.Duration
		}
	}
}

func BenchmarkTimeRow(b *testing.B) {
	chk1 := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDate)}, 1024)
	defCaus1 := chk1.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus1.AppendTime(types.ZeroDate)
	}
	chk2 := chk1.CopyConstruct()
	result := chk1.CopyConstruct()

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		result.Reset()
		it1 := NewIterator4Chunk(chk1)
		it2 := NewIterator4Chunk(chk2)
		for r1, r2 := it1.Begin(), it2.Begin(); r1 != it1.End() && r2 != it2.End(); r1, r2 = it1.Next(), it2.Next() {
			d1 := r1.GetTime(0)
			d2 := r2.GetTime(0)
			if r := d1.Compare(d2); r > 0 {
				result.AppendTime(0, d1)
			} else {
				result.AppendTime(0, d2)
			}
		}
	}
}

func BenchmarkTimeVec(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeDate)}, 1024)
	defCaus1 := chk.DeferredCauset(0)
	for i := 0; i < 1024; i++ {
		defCaus1.AppendTime(types.ZeroDate)
	}
	defCaus2 := defCaus1.CopyConstruct(nil)
	result := defCaus1.CopyConstruct(nil)

	ds1 := defCaus1.Times()
	ds2 := defCaus2.Times()
	rs := result.Times()

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		result.ResizeTime(1024, true)
		for i := 0; i < 1024; i++ {
			if r := ds1[i].Compare(ds2[i]); r > 0 {
				rs[i] = ds1[i]
			} else {
				rs[i] = ds2[i]
			}
		}
	}
}

func genNullDefCauss(n int) []*DeferredCauset {
	defcaus := make([]*DeferredCauset, n)
	for i := range defcaus {
		defcaus[i] = NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), 1024)
		defcaus[i].ResizeInt64(1024, false)
		for j := 0; j < 1024; j++ {
			if rand.Intn(10) < 5 {
				defcaus[i].SetNull(j, true)
			}
		}
	}
	return defcaus
}

func (s *testChunkSuite) TestVectorizedNulls(c *check.C) {
	for i := 0; i < 256; i++ {
		defcaus := genNullDefCauss(4)
		lDefCaus, rDefCaus := defcaus[0], defcaus[1]
		vecResult, rowResult := defcaus[2], defcaus[3]
		vecResult.SetNulls(0, 1024, false)
		rowResult.SetNulls(0, 1024, false)
		vecResult.MergeNulls(lDefCaus, rDefCaus)
		for i := 0; i < 1024; i++ {
			rowResult.SetNull(i, lDefCaus.IsNull(i) || rDefCaus.IsNull(i))
		}

		for i := 0; i < 1024; i++ {
			c.Assert(rowResult.IsNull(i), check.Equals, vecResult.IsNull(i))
		}
	}
}

func (s *testChunkSuite) TestResetDeferredCauset(c *check.C) {
	defCaus0 := NewDeferredCauset(types.NewFieldType(allegrosql.TypeVarString), 0)
	defCaus1 := NewDeferredCauset(types.NewFieldType(allegrosql.TypeLonglong), 0)

	// using defCaus0.reset() here will cause panic since it doesn't reset the elemBuf field which
	// is used by MergeNulls.
	defCaus0.Reset(types.ETInt)
	defCaus0.MergeNulls(defCaus1)

	defCaus := NewDeferredCauset(types.NewFieldType(allegrosql.TypeDatetime), 0)
	defCaus.Reset(types.ETDuration)
	defCaus.AppendDuration(types.Duration{})
	// using defCaus.reset() above will let this assertion fail since the length of initialized elemBuf
	// is sizeTime.
	c.Assert(len(defCaus.data), check.Equals, sizeGoDuration)
}

func BenchmarkMergeNullsVectorized(b *testing.B) {
	defcaus := genNullDefCauss(3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		defcaus[0].MergeNulls(defcaus[1:]...)
	}
}

func BenchmarkMergeNullsNonVectorized(b *testing.B) {
	defcaus := genNullDefCauss(3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1024; i++ {
			defcaus[0].SetNull(i, defcaus[1].IsNull(i) || defcaus[2].IsNull(i))
		}
	}
}
