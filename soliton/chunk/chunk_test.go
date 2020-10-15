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
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

func TestT(t *testing.T) {
	path, _ := ioutil.TemFIDelir("", "oom-use-tmp-storage")
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = path
	})
	_ = os.RemoveAll(path) // clean the uncleared temp file during the last run.
	_ = os.MkdirAll(path, 0755)
	check.TestingT(t)
}

var _ = check.Suite(&testChunkSuite{})

type testChunkSuite struct{}

func (s *testChunkSuite) TestChunk(c *check.C) {
	numDefCauss := 6
	numRows := 10
	chk := newChunk(8, 8, 0, 0, 40, 0)
	strFmt := "%d.12345"
	for i := 0; i < numRows; i++ {
		chk.AppendNull(0)
		chk.AppendInt64(1, int64(i))
		str := fmt.Sprintf(strFmt, i)
		chk.AppendString(2, str)
		chk.AppendBytes(3, []byte(str))
		chk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		chk.AppendJSON(5, json.CreateBinary(str))
	}
	c.Assert(chk.NumDefCauss(), check.Equals, numDefCauss)
	c.Assert(chk.NumRows(), check.Equals, numRows)
	for i := 0; i < numRows; i++ {
		event := chk.GetRow(i)
		c.Assert(event.GetInt64(0), check.Equals, int64(0))
		c.Assert(event.IsNull(0), check.IsTrue)
		c.Assert(event.GetInt64(1), check.Equals, int64(i))
		str := fmt.Sprintf(strFmt, i)
		c.Assert(event.IsNull(2), check.IsFalse)
		c.Assert(event.GetString(2), check.Equals, str)
		c.Assert(event.IsNull(3), check.IsFalse)
		c.Assert(event.GetBytes(3), check.BytesEquals, []byte(str))
		c.Assert(event.IsNull(4), check.IsFalse)
		c.Assert(event.GetMyDecimal(4).String(), check.Equals, str)
		c.Assert(event.IsNull(5), check.IsFalse)
		c.Assert(string(event.GetJSON(5).GetString()), check.Equals, str)
	}

	chk2 := newChunk(8, 8, 0, 0, 40, 0)
	for i := 0; i < numRows; i++ {
		event := chk.GetRow(i)
		chk2.AppendRow(event)
	}
	for i := 0; i < numDefCauss; i++ {
		defCaus2, defCaus1 := chk2.defCausumns[i], chk.defCausumns[i]
		defCaus2.elemBuf, defCaus1.elemBuf = nil, nil
		c.Assert(defCaus2, check.DeepEquals, defCaus1)
	}

	chk = newChunk(4, 8, 16, 16, 0, 0)
	f32Val := float32(1.2)
	chk.AppendFloat32(0, f32Val)
	f64Val := 1.3
	chk.AppendFloat64(1, f64Val)
	tVal := types.TimeFromDays(1)
	chk.AppendTime(2, tVal)
	durVal := types.Duration{Duration: time.Hour, Fsp: 6}
	chk.AppendDuration(3, durVal)
	enumVal := types.Enum{Name: "abc", Value: 100}
	chk.AppendEnum(4, enumVal)
	setVal := types.Set{Name: "def", Value: 101}
	chk.AppendSet(5, setVal)

	event := chk.GetRow(0)
	c.Assert(event.GetFloat32(0), check.Equals, f32Val)
	c.Assert(event.GetTime(2).Compare(tVal), check.Equals, 0)
	// fsp is no longer maintained in chunk
	c.Assert(event.GetDuration(3, 0).Duration, check.DeepEquals, durVal.Duration)
	c.Assert(event.GetEnum(4), check.DeepEquals, enumVal)
	c.Assert(event.GetSet(5), check.DeepEquals, setVal)

	// AppendPartialRow can be different number of defCausumns, useful for join.
	chk = newChunk(8, 8)
	chk2 = newChunk(8)
	chk2.AppendInt64(0, 1)
	chk2.AppendInt64(0, -1)
	chk.AppendPartialRow(0, chk2.GetRow(0))
	chk.AppendPartialRow(1, chk2.GetRow(0))
	c.Assert(chk.GetRow(0).GetInt64(0), check.Equals, int64(1))
	c.Assert(chk.GetRow(0).GetInt64(1), check.Equals, int64(1))
	c.Assert(chk.NumRows(), check.Equals, 1)

	// AppendRowByDefCausIdxs and AppendPartialRowByDefCausIdxs can do projection from event.
	chk = newChunk(8, 8)
	event = MutRowFromValues(0, 1, 2, 3).ToRow()
	chk.AppendRowByDefCausIdxs(event, []int{3})
	chk.AppendRowByDefCausIdxs(event, []int{1})
	chk.AppendRowByDefCausIdxs(event, []int{})
	c.Assert(chk.DeferredCauset(0).Int64s(), check.DeepEquals, []int64{3, 1})
	c.Assert(chk.numVirtualRows, check.Equals, 3)
	chk.AppendPartialRowByDefCausIdxs(1, event, []int{2})
	chk.AppendPartialRowByDefCausIdxs(1, event, []int{0})
	chk.AppendPartialRowByDefCausIdxs(0, event, []int{1, 3})
	c.Assert(chk.DeferredCauset(0).Int64s(), check.DeepEquals, []int64{3, 1, 1})
	c.Assert(chk.DeferredCauset(1).Int64s(), check.DeepEquals, []int64{2, 0, 3})
	c.Assert(chk.numVirtualRows, check.Equals, 3)

	// Test Reset.
	chk = newChunk(0)
	chk.AppendString(0, "abcd")
	chk.Reset()
	chk.AppendString(0, "def")
	c.Assert(chk.GetRow(0).GetString(0), check.Equals, "def")

	// Test float32
	chk = newChunk(4)
	chk.AppendFloat32(0, 1)
	chk.AppendFloat32(0, 1)
	chk.AppendFloat32(0, 1)
	c.Assert(chk.GetRow(2).GetFloat32(0), check.Equals, float32(1))
}

func (s *testChunkSuite) TestAppend(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeJSON})

	jsonObj, err := json.ParseBinaryFromString("{\"k1\":\"v1\"}")
	c.Assert(err, check.IsNil)

	src := NewChunkWithCapacity(fieldTypes, 32)
	dst := NewChunkWithCapacity(fieldTypes, 32)

	src.AppendFloat32(0, 12.8)
	src.AppendString(1, "abc")
	src.AppendJSON(2, jsonObj)
	src.AppendNull(0)
	src.AppendNull(1)
	src.AppendNull(2)

	dst.Append(src, 0, 2)
	dst.Append(src, 0, 2)
	dst.Append(src, 0, 2)
	dst.Append(src, 0, 2)
	dst.Append(dst, 2, 6)
	dst.Append(dst, 6, 6)

	c.Assert(len(dst.defCausumns), check.Equals, 3)

	c.Assert(dst.defCausumns[0].length, check.Equals, 12)
	c.Assert(dst.defCausumns[0].nullCount(), check.Equals, 6)
	c.Assert(string(dst.defCausumns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(len(dst.defCausumns[0].offsets), check.Equals, 0)
	c.Assert(len(dst.defCausumns[0].data), check.Equals, 4*12)
	c.Assert(len(dst.defCausumns[0].elemBuf), check.Equals, 4)

	c.Assert(dst.defCausumns[1].length, check.Equals, 12)
	c.Assert(dst.defCausumns[1].nullCount(), check.Equals, 6)
	c.Assert(string(dst.defCausumns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(fmt.Sprintf("%v", dst.defCausumns[1].offsets), check.Equals, fmt.Sprintf("%v", []int64{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}))
	c.Assert(string(dst.defCausumns[1].data), check.Equals, "abcabcabcabcabcabc")
	c.Assert(len(dst.defCausumns[1].elemBuf), check.Equals, 0)

	c.Assert(dst.defCausumns[2].length, check.Equals, 12)
	c.Assert(dst.defCausumns[2].nullCount(), check.Equals, 6)
	c.Assert(string(dst.defCausumns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(len(dst.defCausumns[2].offsets), check.Equals, 13)
	c.Assert(len(dst.defCausumns[2].data), check.Equals, 150)
	c.Assert(len(dst.defCausumns[2].elemBuf), check.Equals, 0)
	for i := 0; i < 12; i += 2 {
		jsonElem := dst.GetRow(i).GetJSON(2)
		cmpRes := json.CompareBinary(jsonElem, jsonObj)
		c.Assert(cmpRes, check.Equals, 0)
	}
}

func (s *testChunkSuite) TestTruncateTo(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 3)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeJSON})

	jsonObj, err := json.ParseBinaryFromString("{\"k1\":\"v1\"}")
	c.Assert(err, check.IsNil)

	src := NewChunkWithCapacity(fieldTypes, 32)

	for i := 0; i < 8; i++ {
		src.AppendFloat32(0, 12.8)
		src.AppendString(1, "abc")
		src.AppendJSON(2, jsonObj)
		src.AppendNull(0)
		src.AppendNull(1)
		src.AppendNull(2)
	}

	src.TruncateTo(16)
	src.TruncateTo(16)
	src.TruncateTo(14)
	src.TruncateTo(12)
	c.Assert(len(src.defCausumns), check.Equals, 3)

	c.Assert(src.defCausumns[0].length, check.Equals, 12)
	c.Assert(src.defCausumns[0].nullCount(), check.Equals, 6)
	c.Assert(string(src.defCausumns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(len(src.defCausumns[0].offsets), check.Equals, 0)
	c.Assert(len(src.defCausumns[0].data), check.Equals, 4*12)
	c.Assert(len(src.defCausumns[0].elemBuf), check.Equals, 4)

	c.Assert(src.defCausumns[1].length, check.Equals, 12)
	c.Assert(src.defCausumns[1].nullCount(), check.Equals, 6)
	c.Assert(string(src.defCausumns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(fmt.Sprintf("%v", src.defCausumns[1].offsets), check.Equals, fmt.Sprintf("%v", []int64{0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18}))
	c.Assert(string(src.defCausumns[1].data), check.Equals, "abcabcabcabcabcabc")
	c.Assert(len(src.defCausumns[1].elemBuf), check.Equals, 0)

	c.Assert(src.defCausumns[2].length, check.Equals, 12)
	c.Assert(src.defCausumns[2].nullCount(), check.Equals, 6)
	c.Assert(string(src.defCausumns[0].nullBitmap), check.Equals, string([]byte{0x55, 0x05}))
	c.Assert(len(src.defCausumns[2].offsets), check.Equals, 13)
	c.Assert(len(src.defCausumns[2].data), check.Equals, 150)
	c.Assert(len(src.defCausumns[2].elemBuf), check.Equals, 0)
	for i := 0; i < 12; i += 2 {
		event := src.GetRow(i)
		jsonElem := event.GetJSON(2)
		cmpRes := json.CompareBinary(jsonElem, jsonObj)
		c.Assert(cmpRes, check.Equals, 0)
	}
	chk := NewChunkWithCapacity(fieldTypes[:1], 1)
	chk.AppendFloat32(0, 1.0)
	chk.AppendFloat32(0, 1.0)
	chk.TruncateTo(1)
	chk.AppendNull(0)
	c.Assert(chk.GetRow(1).IsNull(0), check.IsTrue)
}

func (s *testChunkSuite) TestChunkSizeControl(c *check.C) {
	maxChunkSize := 10
	chk := New([]*types.FieldType{types.NewFieldType(allegrosql.TypeLong)}, maxChunkSize, maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, maxChunkSize)

	for i := 0; i < maxChunkSize; i++ {
		chk.AppendInt64(0, 1)
	}
	maxChunkSize += maxChunkSize / 3
	chk.GrowAndReset(maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, maxChunkSize)

	maxChunkSize2 := maxChunkSize + maxChunkSize/3
	chk2 := Renew(chk, maxChunkSize2)
	c.Assert(chk2.RequiredRows(), check.Equals, maxChunkSize2)

	chk.Reset()
	for i := 1; i < maxChunkSize*2; i++ {
		chk.SetRequiredRows(i, maxChunkSize)
		c.Assert(chk.RequiredRows(), check.Equals, mathutil.Min(maxChunkSize, i))
	}

	chk.SetRequiredRows(1, maxChunkSize).
		SetRequiredRows(2, maxChunkSize).
		SetRequiredRows(3, maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, 3)

	chk.SetRequiredRows(-1, maxChunkSize)
	c.Assert(chk.RequiredRows(), check.Equals, maxChunkSize)

	chk.SetRequiredRows(5, maxChunkSize)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	c.Assert(chk.NumRows(), check.Equals, 4)
	c.Assert(chk.IsFull(), check.IsFalse)

	chk.AppendInt64(0, 1)
	c.Assert(chk.NumRows(), check.Equals, 5)
	c.Assert(chk.IsFull(), check.IsTrue)

	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	chk.AppendInt64(0, 1)
	c.Assert(chk.NumRows(), check.Equals, 8)
	c.Assert(chk.IsFull(), check.IsTrue)

	chk.SetRequiredRows(maxChunkSize, maxChunkSize)
	c.Assert(chk.NumRows(), check.Equals, 8)
	c.Assert(chk.IsFull(), check.IsFalse)
}

// newChunk creates a new chunk and initialize defCausumns with element length.
// 0 adds an varlen DeferredCauset, positive len add a fixed length DeferredCauset, negative len adds a interface DeferredCauset.
func newChunk(elemLen ...int) *Chunk {
	chk := &Chunk{}
	for _, l := range elemLen {
		if l > 0 {
			chk.defCausumns = append(chk.defCausumns, newFixedLenDeferredCauset(l, 0))
		} else {
			chk.defCausumns = append(chk.defCausumns, newVarLenDeferredCauset(0, nil))
		}
	}
	return chk
}

func newChunkWithInitCap(cap int, elemLen ...int) *Chunk {
	chk := &Chunk{}
	for _, l := range elemLen {
		if l > 0 {
			chk.defCausumns = append(chk.defCausumns, newFixedLenDeferredCauset(l, cap))
		} else {
			chk.defCausumns = append(chk.defCausumns, newVarLenDeferredCauset(cap, nil))
		}
	}
	return chk
}

var allTypes = []*types.FieldType{
	types.NewFieldType(allegrosql.TypeTiny),
	types.NewFieldType(allegrosql.TypeShort),
	types.NewFieldType(allegrosql.TypeInt24),
	types.NewFieldType(allegrosql.TypeLong),
	types.NewFieldType(allegrosql.TypeLonglong),
	{
		Tp:      allegrosql.TypeLonglong,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    allegrosql.UnsignedFlag,
	},
	types.NewFieldType(allegrosql.TypeYear),
	types.NewFieldType(allegrosql.TypeFloat),
	types.NewFieldType(allegrosql.TypeDouble),
	types.NewFieldType(allegrosql.TypeString),
	types.NewFieldType(allegrosql.TypeVarString),
	types.NewFieldType(allegrosql.TypeVarchar),
	types.NewFieldType(allegrosql.TypeBlob),
	types.NewFieldType(allegrosql.TypeTinyBlob),
	types.NewFieldType(allegrosql.TypeMediumBlob),
	types.NewFieldType(allegrosql.TypeLongBlob),
	types.NewFieldType(allegrosql.TypeDate),
	types.NewFieldType(allegrosql.TypeDatetime),
	types.NewFieldType(allegrosql.TypeTimestamp),
	types.NewFieldType(allegrosql.TypeDuration),
	types.NewFieldType(allegrosql.TypeNewDecimal),
	{
		Tp:      allegrosql.TypeSet,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    allegrosql.UnsignedFlag,
		Elems:   []string{"a", "b"},
	},
	{
		Tp:      allegrosql.TypeEnum,
		Flen:    types.UnspecifiedLength,
		Decimal: types.UnspecifiedLength,
		Flag:    allegrosql.UnsignedFlag,
		Elems:   []string{"a", "b"},
	},
	types.NewFieldType(allegrosql.TypeBit),
	types.NewFieldType(allegrosql.TypeJSON),
}

func (s *testChunkSuite) TestCompare(c *check.C) {
	chunk := NewChunkWithCapacity(allTypes, 32)
	for i := 0; i < len(allTypes); i++ {
		chunk.AppendNull(i)
	}
	for i := 0; i < len(allTypes); i++ {
		switch allTypes[i].Tp {
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
			if allegrosql.HasUnsignedFlag(allTypes[i].Flag) {
				chunk.AppendUint64(i, 0)
			} else {
				chunk.AppendInt64(i, -1)
			}
		case allegrosql.TypeFloat:
			chunk.AppendFloat32(i, 0)
		case allegrosql.TypeDouble:
			chunk.AppendFloat64(i, 0)
		case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar,
			allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
			chunk.AppendString(i, "0")
		case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
			chunk.AppendTime(i, types.TimeFromDays(2000*365))
		case allegrosql.TypeDuration:
			chunk.AppendDuration(i, types.ZeroDuration)
		case allegrosql.TypeNewDecimal:
			chunk.AppendMyDecimal(i, types.NewDecFromInt(0))
		case allegrosql.TypeSet:
			chunk.AppendSet(i, types.Set{Name: "a", Value: 0})
		case allegrosql.TypeEnum:
			chunk.AppendEnum(i, types.Enum{Name: "a", Value: 0})
		case allegrosql.TypeBit:
			chunk.AppendBytes(i, []byte{0})
		case allegrosql.TypeJSON:
			chunk.AppendJSON(i, json.CreateBinary(int64(0)))
		default:
			c.FailNow()
		}
	}
	for i := 0; i < len(allTypes); i++ {
		switch allTypes[i].Tp {
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
			if allegrosql.HasUnsignedFlag(allTypes[i].Flag) {
				chunk.AppendUint64(i, math.MaxUint64)
			} else {
				chunk.AppendInt64(i, 1)
			}
		case allegrosql.TypeFloat:
			chunk.AppendFloat32(i, 1)
		case allegrosql.TypeDouble:
			chunk.AppendFloat64(i, 1)
		case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar,
			allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
			chunk.AppendString(i, "1")
		case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
			chunk.AppendTime(i, types.TimeFromDays(2001*365))
		case allegrosql.TypeDuration:
			chunk.AppendDuration(i, types.Duration{Duration: time.Second})
		case allegrosql.TypeNewDecimal:
			chunk.AppendMyDecimal(i, types.NewDecFromInt(1))
		case allegrosql.TypeSet:
			chunk.AppendSet(i, types.Set{Name: "b", Value: 1})
		case allegrosql.TypeEnum:
			chunk.AppendEnum(i, types.Enum{Name: "b", Value: 1})
		case allegrosql.TypeBit:
			chunk.AppendBytes(i, []byte{1})
		case allegrosql.TypeJSON:
			chunk.AppendJSON(i, json.CreateBinary(int64(1)))
		default:
			c.FailNow()
		}
	}
	rowNull := chunk.GetRow(0)
	rowSmall := chunk.GetRow(1)
	rowBig := chunk.GetRow(2)
	for i := 0; i < len(allTypes); i++ {
		cmpFunc := GetCompareFunc(allTypes[i])
		c.Assert(cmpFunc(rowNull, i, rowNull, i), check.Equals, 0)
		c.Assert(cmpFunc(rowNull, i, rowSmall, i), check.Equals, -1)
		c.Assert(cmpFunc(rowSmall, i, rowNull, i), check.Equals, 1)
		c.Assert(cmpFunc(rowSmall, i, rowSmall, i), check.Equals, 0)
		c.Assert(cmpFunc(rowSmall, i, rowBig, i), check.Equals, -1, check.Commentf("%d", allTypes[i].Tp))
		c.Assert(cmpFunc(rowBig, i, rowSmall, i), check.Equals, 1)
		c.Assert(cmpFunc(rowBig, i, rowBig, i), check.Equals, 0)
	}
}

func (s *testChunkSuite) TestCopyTo(c *check.C) {
	chunk := NewChunkWithCapacity(allTypes, 101)
	for i := 0; i < len(allTypes); i++ {
		chunk.AppendNull(i)
	}
	for k := 0; k < 100; k++ {
		for i := 0; i < len(allTypes); i++ {
			switch allTypes[i].Tp {
			case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
				if allegrosql.HasUnsignedFlag(allTypes[i].Flag) {
					chunk.AppendUint64(i, uint64(k))
				} else {
					chunk.AppendInt64(i, int64(k))
				}
			case allegrosql.TypeFloat:
				chunk.AppendFloat32(i, float32(k))
			case allegrosql.TypeDouble:
				chunk.AppendFloat64(i, float64(k))
			case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar,
				allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
				chunk.AppendString(i, fmt.Sprintf("%v", k))
			case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
				chunk.AppendTime(i, types.TimeFromDays(2000*365+int64(k)))
			case allegrosql.TypeDuration:
				chunk.AppendDuration(i, types.Duration{Duration: time.Second * time.Duration(k), Fsp: types.DefaultFsp})
			case allegrosql.TypeNewDecimal:
				chunk.AppendMyDecimal(i, types.NewDecFromInt(int64(k)))
			case allegrosql.TypeSet:
				chunk.AppendSet(i, types.Set{Name: "a", Value: uint64(k)})
			case allegrosql.TypeEnum:
				chunk.AppendEnum(i, types.Enum{Name: "a", Value: uint64(k)})
			case allegrosql.TypeBit:
				chunk.AppendBytes(i, []byte{byte(k)})
			case allegrosql.TypeJSON:
				chunk.AppendJSON(i, json.CreateBinary(int64(k)))
			default:
				c.FailNow()
			}
		}
	}

	ck1 := chunk.CopyConstruct()

	for k := 0; k < 101; k++ {
		event := chunk.GetRow(k)
		r1 := ck1.GetRow(k)
		for i := 0; i < len(allTypes); i++ {
			cmpFunc := GetCompareFunc(allTypes[i])
			c.Assert(cmpFunc(event, i, r1, i), check.Equals, 0)
		}

	}
}

func (s *testChunkSuite) TestGetDecimalCauset(c *check.C) {
	causet := types.NewCauset(1.01)
	decType := types.NewFieldType(allegrosql.TypeNewDecimal)
	decType.Flen = 4
	decType.Decimal = 2
	sc := new(stmtctx.StatementContext)
	decCauset, err := causet.ConvertTo(sc, decType)
	c.Assert(err, check.IsNil)
	chk := NewChunkWithCapacity([]*types.FieldType{decType}, 32)
	chk.AppendMyDecimal(0, decCauset.GetMysqlDecimal())
	decFromChk := chk.GetRow(0).GetCauset(0, decType)
	c.Assert(decCauset.Length(), check.Equals, decFromChk.Length())
	c.Assert(decCauset.Frac(), check.Equals, decFromChk.Frac())
}

func (s *testChunkSuite) TestChunkMemoryUsage(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 5)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeJSON})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeDuration})

	initCap := 10
	chk := NewChunkWithCapacity(fieldTypes, initCap)

	//cap(c.nullBitmap) + cap(c.offsets)*4 + cap(c.data) + cap(c.elemBuf)
	defCausUsage := make([]int, len(fieldTypes))
	defCausUsage[0] = (initCap+7)>>3 + 0 + initCap*4 + 4
	defCausUsage[1] = (initCap+7)>>3 + (initCap+1)*4 + initCap*8 + 0
	defCausUsage[2] = (initCap+7)>>3 + (initCap+1)*4 + initCap*8 + 0
	defCausUsage[3] = (initCap+7)>>3 + 0 + initCap*sizeTime + sizeTime
	defCausUsage[4] = (initCap+7)>>3 + 0 + initCap*8 + 8

	expectedUsage := 0
	for i := range defCausUsage {
		expectedUsage += defCausUsage[i] + int(unsafe.Sizeof(*chk.defCausumns[i]))
	}
	memUsage := chk.MemoryUsage()
	c.Assert(memUsage, check.Equals, int64(expectedUsage))

	jsonObj, err := json.ParseBinaryFromString("1")
	c.Assert(err, check.IsNil)
	timeObj := types.NewTime(types.FromGoTime(time.Now()), allegrosql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	chk.AppendFloat32(0, 12.4)
	chk.AppendString(1, "123")
	chk.AppendJSON(2, jsonObj)
	chk.AppendTime(3, timeObj)
	chk.AppendDuration(4, durationObj)

	memUsage = chk.MemoryUsage()
	c.Assert(memUsage, check.Equals, int64(expectedUsage))

	chk.AppendFloat32(0, 12.4)
	chk.AppendString(1, "123111111111111111111111111111111111111111111111")
	chk.AppendJSON(2, jsonObj)
	chk.AppendTime(3, timeObj)
	chk.AppendDuration(4, durationObj)

	memUsage = chk.MemoryUsage()
	defCausUsage[1] = (initCap+7)>>3 + (initCap+1)*4 + cap(chk.defCausumns[1].data) + 0
	expectedUsage = 0
	for i := range defCausUsage {
		expectedUsage += defCausUsage[i] + int(unsafe.Sizeof(*chk.defCausumns[i]))
	}
	c.Assert(memUsage, check.Equals, int64(expectedUsage))
}

func (s *testChunkSuite) TestSwapDeferredCauset(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 2)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})

	// chk1: defCausumn1 refers to defCausumn0
	chk1 := NewChunkWithCapacity(fieldTypes, 1)
	chk1.AppendFloat32(0, 1)
	chk1.MakeRef(0, 1)
	chk1.AppendFloat32(2, 3)

	// chk2: defCausumn1 refers to defCausumn0
	chk2 := NewChunkWithCapacity(fieldTypes, 1)
	chk2.AppendFloat32(0, 1)
	chk2.MakeRef(0, 1)
	chk2.AppendFloat32(2, 3)

	c.Assert(chk1.defCausumns[0] == chk1.defCausumns[1], check.IsTrue)
	c.Assert(chk2.defCausumns[0] == chk2.defCausumns[1], check.IsTrue)

	checkRef := func() {
		c.Assert(chk1.defCausumns[0] == chk1.defCausumns[1], check.IsTrue)
		c.Assert(chk1.defCausumns[0] == chk2.defCausumns[0], check.IsFalse)
		c.Assert(chk2.defCausumns[0] == chk2.defCausumns[1], check.IsTrue)
	}

	chk1.SwapDeferredCauset(0, chk2, 0)
	checkRef()

	chk1.SwapDeferredCauset(0, chk2, 1)
	checkRef()

	chk2.SwapDeferredCauset(1, chk2, 0)
	checkRef()

	chk2.SwapDeferredCauset(1, chk2, 1)
	checkRef()

	chk2.SwapDeferredCauset(1, chk2, 2)
	checkRef()

	chk2.SwapDeferredCauset(2, chk2, 0)
	checkRef()
}

func (s *testChunkSuite) TestPreAlloc4RowAndInsert(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeLonglong})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeNewDecimal})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})

	srcChk := NewChunkWithCapacity(fieldTypes, 10)
	for i := int64(0); i < 10; i++ {
		srcChk.AppendFloat32(0, float32(i))
		srcChk.AppendInt64(1, i)
		srcChk.AppendMyDecimal(2, types.NewDecFromInt(i))
		srcChk.AppendString(3, strings.Repeat(strconv.FormatInt(i, 10), int(i)))
	}

	destChk := NewChunkWithCapacity(fieldTypes, 3)

	// Test Chunk.PreAlloc.
	for i := 0; i < srcChk.NumRows(); i++ {
		c.Assert(destChk.NumRows(), check.Equals, i)
		destChk.preAlloc(srcChk.GetRow(i))
	}
	for i, srcDefCaus := range srcChk.defCausumns {
		destDefCaus := destChk.defCausumns[i]
		c.Assert(len(srcDefCaus.elemBuf), check.Equals, len(destDefCaus.elemBuf))
		c.Assert(len(srcDefCaus.data), check.Equals, len(destDefCaus.data))
		c.Assert(len(srcDefCaus.offsets), check.Equals, len(destDefCaus.offsets))
		c.Assert(len(srcDefCaus.nullBitmap), check.Equals, len(destDefCaus.nullBitmap))
		c.Assert(srcDefCaus.length, check.Equals, destDefCaus.length)
		c.Assert(srcDefCaus.nullCount(), check.Equals, destDefCaus.nullCount())

		for _, val := range destDefCaus.data {
			c.Assert(val == 0, check.IsTrue)
		}
		for j, val := range srcDefCaus.offsets {
			c.Assert(val, check.Equals, destDefCaus.offsets[j])
		}
		for j, val := range srcDefCaus.nullBitmap {
			c.Assert(val, check.Equals, destDefCaus.nullBitmap[j])
		}
		for _, val := range destDefCaus.elemBuf {
			c.Assert(val == 0, check.IsTrue)
		}
	}

	// Test Chunk.Insert.
	for i := srcChk.NumRows() - 1; i >= 0; i-- {
		destChk.insert(i, srcChk.GetRow(i))
	}
	for i, srcDefCaus := range srcChk.defCausumns {
		destDefCaus := destChk.defCausumns[i]

		for j, val := range srcDefCaus.data {
			c.Assert(val, check.Equals, destDefCaus.data[j])
		}
		for j, val := range srcDefCaus.offsets {
			c.Assert(val, check.Equals, destDefCaus.offsets[j])
		}
		for j, val := range srcDefCaus.nullBitmap {
			c.Assert(val, check.Equals, destDefCaus.nullBitmap[j])
		}
		for _, val := range destDefCaus.elemBuf {
			c.Assert(val == 0, check.IsTrue)
		}
	}

	// Test parallel Chunk.Insert.
	destChk.Reset()
	startWg, endWg := &sync.WaitGroup{}, &sync.WaitGroup{}
	startWg.Add(1)
	for i := 0; i < srcChk.NumRows(); i++ {
		destChk.preAlloc(srcChk.GetRow(i))
		endWg.Add(1)
		go func(rowIdx int) {
			defer func() {
				endWg.Done()
			}()
			startWg.Wait()
			destChk.insert(rowIdx, srcChk.GetRow(rowIdx))
		}(i)
	}
	startWg.Done()
	endWg.Wait()
	for i, srcDefCaus := range srcChk.defCausumns {
		destDefCaus := destChk.defCausumns[i]

		for j, val := range srcDefCaus.data {
			c.Assert(val, check.Equals, destDefCaus.data[j])
		}
		for j, val := range srcDefCaus.offsets {
			c.Assert(val, check.Equals, destDefCaus.offsets[j])
		}
		for j, val := range srcDefCaus.nullBitmap {
			c.Assert(val, check.Equals, destDefCaus.nullBitmap[j])
		}
		for _, val := range destDefCaus.elemBuf {
			c.Assert(val == 0, check.IsTrue)
		}
	}
}

func (s *testChunkSuite) TestAppendSel(c *check.C) {
	tll := &types.FieldType{Tp: allegrosql.TypeLonglong}
	chk := NewChunkWithCapacity([]*types.FieldType{tll}, 1024)
	sel := make([]int, 0, 1024/2)
	for i := 0; i < 1024/2; i++ {
		chk.AppendInt64(0, int64(i))
		if i%2 == 0 {
			sel = append(sel, i)
		}
	}
	chk.SetSel(sel)
	c.Assert(chk.NumRows(), check.Equals, 1024/2/2)
	chk.AppendInt64(0, int64(1))
	c.Assert(chk.NumRows(), check.Equals, 1024/2/2+1)
	sel = chk.Sel()
	c.Assert(sel[len(sel)-1], check.Equals, 1024/2)
}

func (s *testChunkSuite) TestMakeRefTo(c *check.C) {
	fieldTypes := make([]*types.FieldType, 0, 2)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})

	chk1 := NewChunkWithCapacity(fieldTypes, 1)
	chk1.AppendFloat32(0, 1)
	chk1.AppendFloat32(1, 3)

	chk2 := NewChunkWithCapacity(fieldTypes, 1)
	chk2.MakeRefTo(0, chk1, 1)
	chk2.MakeRefTo(1, chk1, 0)

	c.Assert(chk2.defCausumns[0] == chk1.defCausumns[1], check.IsTrue)
	c.Assert(chk2.defCausumns[1] == chk1.defCausumns[0], check.IsTrue)
}

func BenchmarkAppendInt(b *testing.B) {
	b.ReportAllocs()
	chk := newChunk(8)
	for i := 0; i < b.N; i++ {
		appendInt(chk)
	}
}

func appendInt(chk *Chunk) {
	chk.Reset()
	for i := 0; i < 1000; i++ {
		chk.AppendInt64(0, int64(i))
	}
}

func BenchmarkAppendString(b *testing.B) {
	b.ReportAllocs()
	chk := newChunk(0)
	for i := 0; i < b.N; i++ {
		appendString(chk)
	}
}

func appendString(chk *Chunk) {
	chk.Reset()
	for i := 0; i < 1000; i++ {
		chk.AppendString(0, "abcd")
	}
}

func BenchmarkAppendRow(b *testing.B) {
	b.ReportAllocs()
	rowChk := newChunk(8, 8, 0, 0)
	rowChk.AppendNull(0)
	rowChk.AppendInt64(1, 1)
	rowChk.AppendString(2, "abcd")
	rowChk.AppendBytes(3, []byte("abcd"))

	chk := newChunk(8, 8, 0, 0)
	for i := 0; i < b.N; i++ {
		appendRow(chk, rowChk.GetRow(0))
	}
}

func appendRow(chk *Chunk, event Row) {
	chk.Reset()
	for i := 0; i < 1000; i++ {
		chk.AppendRow(event)
	}
}

func BenchmarkAppendBytes1024(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 1024)
	}
}

func BenchmarkAppendBytes512(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 512)
	}
}

func BenchmarkAppendBytes256(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 256)
	}
}

func BenchmarkAppendBytes128(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 128)
	}
}

func BenchmarkAppendBytes64(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 64)
	}
}

func BenchmarkAppendBytes32(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 32)
	}
}

func BenchmarkAppendBytes16(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 16)
	}
}

func BenchmarkAppendBytes8(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 8)
	}
}

func BenchmarkAppendBytes4(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 4)
	}
}

func BenchmarkAppendBytes2(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 2)
	}
}

func BenchmarkAppendBytes1(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(allegrosql.TypeString)}, 32)
	var bs = make([]byte, 256)
	for i := 0; i < b.N; i++ {
		appendBytes(chk, bs, 1)
	}
}

func appendBytes(chk *Chunk, bs []byte, times int) {
	chk.Reset()
	for i := 0; i < times; i++ {
		chk.AppendBytes(0, bs)
	}
}

func BenchmarkAccess(b *testing.B) {
	b.StopTimer()
	rowChk := newChunk(8)
	for i := 0; i < 8192; i++ {
		rowChk.AppendInt64(0, math.MaxUint16)
	}
	b.StartTimer()
	var sum int64
	for i := 0; i < b.N; i++ {
		for j := 0; j < 8192; j++ {
			sum += rowChk.GetRow(j).GetInt64(0)
		}
	}
	fmt.Println(sum)
}

func BenchmarkChunkMemoryUsage(b *testing.B) {
	fieldTypes := make([]*types.FieldType, 0, 4)
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeFloat})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeVarchar})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeDatetime})
	fieldTypes = append(fieldTypes, &types.FieldType{Tp: allegrosql.TypeDuration})

	initCap := 10
	chk := NewChunkWithCapacity(fieldTypes, initCap)
	timeObj := types.NewTime(types.FromGoTime(time.Now()), allegrosql.TypeDatetime, 0)
	durationObj := types.Duration{Duration: math.MaxInt64, Fsp: 0}

	for i := 0; i < initCap; i++ {
		chk.AppendFloat64(0, 123.123)
		chk.AppendString(1, "123")
		chk.AppendTime(2, timeObj)
		chk.AppendDuration(3, durationObj)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chk.MemoryUsage()
	}
}

type seqNumberGenerateInterDirc struct {
	seq          int
	genCountSize int
}

func (x *seqNumberGenerateInterDirc) Next(chk *Chunk, resize bool) {
	if resize {
		chk.GrowAndReset(1024)
	} else {
		chk.Reset()
	}
	for chk.NumRows() < chk.Capacity() {
		x.seq++
		if x.seq > x.genCountSize {
			break
		}
		chk.AppendInt64(0, 1)
	}
}

type benchChunkGrowCase struct {
	tag        string
	reuse      bool
	newReset   bool
	cntPerCall int
	initCap    int
	maxCap     int
}

func (b *benchChunkGrowCase) String() string {
	var buff bytes.Buffer
	if b.reuse {
		buff.WriteString("renew,")
	} else {
		buff.WriteString("reset,")
	}
	buff.WriteString("cntPerCall:" + strconv.Itoa(b.cntPerCall) + ",")
	buff.WriteString("cap from:" + strconv.Itoa(b.initCap) + " to " + strconv.Itoa(b.maxCap) + ",")
	if b.tag != "" {
		buff.WriteString("[" + b.tag + "]")
	}
	return buff.String()
}

func BenchmarkChunkGrowSuit(b *testing.B) {
	tests := []benchChunkGrowCase{
		{reuse: true, newReset: false, cntPerCall: 10000000, initCap: 1024, maxCap: 1024},
		{reuse: true, newReset: false, cntPerCall: 10000000, initCap: 32, maxCap: 32},
		{reuse: true, newReset: true, cntPerCall: 10000000, initCap: 32, maxCap: 1024, tag: "grow"},
		{reuse: false, newReset: false, cntPerCall: 10000000, initCap: 1024, maxCap: 1024},
		{reuse: false, newReset: false, cntPerCall: 10000000, initCap: 32, maxCap: 32},
		{reuse: false, newReset: true, cntPerCall: 10000000, initCap: 32, maxCap: 1024, tag: "grow"},
		{reuse: true, newReset: false, cntPerCall: 10, initCap: 1024, maxCap: 1024},
		{reuse: true, newReset: false, cntPerCall: 10, initCap: 32, maxCap: 32},
		{reuse: true, newReset: true, cntPerCall: 10, initCap: 32, maxCap: 1024, tag: "grow"},
		{reuse: false, newReset: false, cntPerCall: 10, initCap: 1024, maxCap: 1024},
		{reuse: false, newReset: false, cntPerCall: 10, initCap: 32, maxCap: 32},
		{reuse: false, newReset: true, cntPerCall: 10, initCap: 32, maxCap: 1024, tag: "grow"},
	}
	for _, test := range tests {
		b.Run(test.String(), benchmarkChunkGrow(test))
	}
}

func benchmarkChunkGrow(t benchChunkGrowCase) func(b *testing.B) {
	return func(b *testing.B) {
		b.ReportAllocs()
		chk := New([]*types.FieldType{{Tp: allegrosql.TypeLong}}, t.initCap, t.maxCap)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := &seqNumberGenerateInterDirc{genCountSize: t.cntPerCall}
			for {
				e.Next(chk, t.newReset)
				if chk.NumRows() == 0 {
					break
				}
				if !t.reuse {
					if t.newReset {
						chk = Renew(chk, t.maxCap)
					} else {
						chk = New([]*types.FieldType{{Tp: allegrosql.TypeLong}}, t.initCap, t.maxCap)
					}
				}
			}
		}
	}
}
