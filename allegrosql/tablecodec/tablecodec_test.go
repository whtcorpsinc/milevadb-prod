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

package blockcodec

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&testTableCodecSuite{})

type testTableCodecSuite struct{}

// TestTableCodec  tests some functions in package blockcodec
// TODO: add more tests.
func (s *testTableCodecSuite) TestTableCodec(c *C) {
	defer testleak.AfterTest(c)()
	key := EncodeRowKey(1, codec.EncodeInt(nil, 2))
	h, err := DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(2))

	key = EncodeRowKeyWithHandle(1, ekv.IntHandle(2))
	h, err = DecodeRowKey(key)
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(2))
}

// column is a structure used for test
type column struct {
	id int64
	tp *types.FieldType
}

func (s *testTableCodecSuite) TestRowCodec(c *C) {
	defer testleak.AfterTest(c)()

	c1 := &column{id: 1, tp: types.NewFieldType(allegrosql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(allegrosql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(allegrosql.TypeNewDecimal)}
	c4 := &column{id: 4, tp: &types.FieldType{Tp: allegrosql.TypeEnum, Elems: []string{"a"}}}
	c5 := &column{id: 5, tp: &types.FieldType{Tp: allegrosql.TypeSet, Elems: []string{"a"}}}
	c6 := &column{id: 6, tp: &types.FieldType{Tp: allegrosql.TypeBit, Flen: 8}}
	defcaus := []*column{c1, c2, c3, c4, c5, c6}

	event := make([]types.Causet, 6)
	event[0] = types.NewIntCauset(100)
	event[1] = types.NewBytesCauset([]byte("abc"))
	event[2] = types.NewDecimalCauset(types.NewDecFromInt(1))
	event[3] = types.NewMysqlEnumCauset(types.Enum{Name: "a", Value: 1})
	event[4] = types.NewCauset(types.Set{Name: "a", Value: 1})
	event[5] = types.NewCauset(types.BinaryLiteral{100})
	// Encode
	colIDs := make([]int64, 0, len(event))
	for _, col := range defcaus {
		colIDs = append(colIDs, col.id)
	}
	rd := rowcodec.CausetEncoder{Enable: true}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	bs, err := EncodeRow(sc, event, colIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, len(event))
	for _, col := range defcaus {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRowToCausetMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(event))
	// Compare decoded event and original event
	for i, col := range defcaus {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareCauset(sc, &event[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0, Commentf("expect: %v, got %v", event[i], v))
	}

	// colMap may contains more columns than encoded event.
	//colMap[4] = types.NewFieldType(allegrosql.TypeFloat)
	r, err = DecodeRowToCausetMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(event))
	for i, col := range defcaus {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareCauset(sc, &event[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// colMap may contains less columns than encoded event.
	delete(colMap, 3)
	delete(colMap, 4)
	r, err = DecodeRowToCausetMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(event)-2)
	for i, col := range defcaus {
		if i > 1 {
			break
		}
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareCauset(sc, &event[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// Make sure empty event return not nil value.
	bs, err = EncodeOldRow(sc, []types.Causet{}, []int64{}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, HasLen, 1)

	r, err = DecodeRowToCausetMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)
}

func (s *testTableCodecSuite) TestDecodeDeferredCausetValue(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}

	// test timestamp
	d := types.NewTimeCauset(types.NewTime(types.FromGoTime(time.Now()), allegrosql.TypeTimestamp, types.DefaultFsp))
	bs, err := EncodeOldRow(sc, []types.Causet{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp := types.NewFieldType(allegrosql.TypeTimestamp)
	d1, err := DecodeDeferredCausetValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err := d1.CompareCauset(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// test set
	elems := []string{"a", "b", "c", "d", "e"}
	e, _ := types.ParseSetValue(elems, uint64(1))
	d = types.NewMysqlSetCauset(e, "")
	bs, err = EncodeOldRow(sc, []types.Causet{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp = types.NewFieldType(allegrosql.TypeSet)
	tp.Elems = elems
	d1, err = DecodeDeferredCausetValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err = d1.CompareCauset(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// test bit
	d = types.NewMysqlBitCauset(types.NewBinaryLiteralFromUint(3223600, 3))
	bs, err = EncodeOldRow(sc, []types.Causet{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp = types.NewFieldType(allegrosql.TypeBit)
	tp.Flen = 24
	d1, err = DecodeDeferredCausetValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err = d1.CompareCauset(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// test empty enum
	d = types.NewMysqlEnumCauset(types.Enum{})
	bs, err = EncodeOldRow(sc, []types.Causet{d}, []int64{1}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	_, bs, err = codec.CutOne(bs) // ignore colID
	c.Assert(err, IsNil)
	tp = types.NewFieldType(allegrosql.TypeEnum)
	d1, err = DecodeDeferredCausetValue(bs, tp, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err = d1.CompareCauset(sc, &d)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}

func (s *testTableCodecSuite) TestUnflattenCausets(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	input := types.MakeCausets(int64(1))
	tps := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	output, err := UnflattenCausets(input, tps, sc.TimeZone)
	c.Assert(err, IsNil)
	cmp, err := input[0].CompareCauset(sc, &output[0])
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}

func (s *testTableCodecSuite) TestTimeCodec(c *C) {
	defer testleak.AfterTest(c)()

	c1 := &column{id: 1, tp: types.NewFieldType(allegrosql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(allegrosql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(allegrosql.TypeTimestamp)}
	c4 := &column{id: 4, tp: types.NewFieldType(allegrosql.TypeDuration)}
	defcaus := []*column{c1, c2, c3, c4}
	colLen := len(defcaus)

	event := make([]types.Causet, colLen)
	event[0] = types.NewIntCauset(100)
	event[1] = types.NewBytesCauset([]byte("abc"))
	ts, err := types.ParseTimestamp(&stmtctx.StatementContext{TimeZone: time.UTC},
		"2020-06-23 11:30:45")
	c.Assert(err, IsNil)
	event[2] = types.NewCauset(ts)
	du, err := types.ParseDuration(nil, "12:59:59.999999", 6)
	c.Assert(err, IsNil)
	event[3] = types.NewCauset(du)

	// Encode
	colIDs := make([]int64, 0, colLen)
	for _, col := range defcaus {
		colIDs = append(colIDs, col.id)
	}
	rd := rowcodec.CausetEncoder{Enable: true}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	bs, err := EncodeRow(sc, event, colIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, colLen)
	for _, col := range defcaus {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRowToCausetMap(bs, colMap, time.UTC)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, colLen)
	// Compare decoded event and original event
	for i, col := range defcaus {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareCauset(sc, &event[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}
}

func (s *testTableCodecSuite) TestCutRow(c *C) {
	defer testleak.AfterTest(c)()

	var err error
	c1 := &column{id: 1, tp: types.NewFieldType(allegrosql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(allegrosql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(allegrosql.TypeNewDecimal)}
	defcaus := []*column{c1, c2, c3}

	event := make([]types.Causet, 3)
	event[0] = types.NewIntCauset(100)
	event[1] = types.NewBytesCauset([]byte("abc"))
	event[2] = types.NewDecimalCauset(types.NewDecFromInt(1))

	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	data := make([][]byte, 3)
	data[0], err = EncodeValue(sc, nil, event[0])
	c.Assert(err, IsNil)
	data[1], err = EncodeValue(sc, nil, event[1])
	c.Assert(err, IsNil)
	data[2], err = EncodeValue(sc, nil, event[2])
	c.Assert(err, IsNil)
	// Encode
	colIDs := make([]int64, 0, 3)
	for _, col := range defcaus {
		colIDs = append(colIDs, col.id)
	}
	bs, err := EncodeOldRow(sc, event, colIDs, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]int, 3)
	for i, col := range defcaus {
		colMap[col.id] = i
	}
	r, err := CutRowNew(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, 3)
	// Compare cut event and original event
	for i := range colIDs {
		c.Assert(r[i], DeepEquals, data[i])
	}
	bs = []byte{codec.NilFlag}
	r, err = CutRowNew(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	bs = nil
	r, err = CutRowNew(bs, colMap)
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
}

func (s *testTableCodecSuite) TestCutKeyNew(c *C) {
	values := []types.Causet{types.NewIntCauset(1), types.NewBytesCauset([]byte("abc")), types.NewFloat64Causet(5.5)}
	handle := types.NewIntCauset(100)
	values = append(values, handle)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	c.Assert(err, IsNil)
	blockID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(blockID, indexID, encodedValue)
	valuesBytes, handleBytes, err := CutIndexKeyNew(indexKey, 3)
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		valueBytes := valuesBytes[i]
		var val types.Causet
		_, val, _ = codec.DecodeOne(valueBytes)
		c.Assert(val, DeepEquals, values[i])
	}
	_, handleVal, _ := codec.DecodeOne(handleBytes)
	c.Assert(handleVal, DeepEquals, types.NewIntCauset(100))
}

func (s *testTableCodecSuite) TestCutKey(c *C) {
	colIDs := []int64{1, 2, 3}
	values := []types.Causet{types.NewIntCauset(1), types.NewBytesCauset([]byte("abc")), types.NewFloat64Causet(5.5)}
	handle := types.NewIntCauset(100)
	values = append(values, handle)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	c.Assert(err, IsNil)
	blockID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(blockID, indexID, encodedValue)
	valuesMap, handleBytes, err := CutIndexKey(indexKey, colIDs)
	c.Assert(err, IsNil)
	for i, colID := range colIDs {
		valueBytes := valuesMap[colID]
		var val types.Causet
		_, val, _ = codec.DecodeOne(valueBytes)
		c.Assert(val, DeepEquals, values[i])
	}
	_, handleVal, _ := codec.DecodeOne(handleBytes)
	c.Assert(handleVal, DeepEquals, types.NewIntCauset(100))
}

func (s *testTableCodecSuite) TestDecodeBadDecical(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/soliton/codec/errorInDecodeDecimal", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/soliton/codec/errorInDecodeDecimal"), IsNil)
	}()
	dec := types.NewDecFromStringForTest("0.111")
	b, err := codec.EncodeDecimal(nil, dec, 0, 0)
	c.Assert(err, IsNil)
	// Expect no panic.
	_, _, err = codec.DecodeOne(b)
	c.Assert(err, NotNil)
}

func (s *testTableCodecSuite) TestIndexKey(c *C) {
	blockID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(blockID, indexID, []byte{})
	tTableID, tIndexID, isRecordKey, err := DecodeKeyHead(indexKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, blockID)
	c.Assert(tIndexID, Equals, indexID)
	c.Assert(isRecordKey, IsFalse)
}

func (s *testTableCodecSuite) TestRecordKey(c *C) {
	blockID := int64(55)
	blockKey := EncodeRowKeyWithHandle(blockID, ekv.IntHandle(math.MaxUint32))
	tTableID, _, isRecordKey, err := DecodeKeyHead(blockKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, blockID)
	c.Assert(isRecordKey, IsTrue)

	encodedHandle := codec.EncodeInt(nil, math.MaxUint32)
	rowKey := EncodeRowKey(blockID, encodedHandle)
	c.Assert([]byte(blockKey), BytesEquals, []byte(rowKey))
	tTableID, handle, err := DecodeRecordKey(rowKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, blockID)
	c.Assert(handle.IntValue(), Equals, int64(math.MaxUint32))

	recordPrefix := GenTableRecordPrefix(blockID)
	rowKey = EncodeRecordKey(recordPrefix, ekv.IntHandle(math.MaxUint32))
	c.Assert([]byte(blockKey), BytesEquals, []byte(rowKey))

	_, _, err = DecodeRecordKey(nil)
	c.Assert(err, NotNil)
	_, _, err = DecodeRecordKey([]byte("abcdefghijklmnopqrstuvwxyz"))
	c.Assert(err, NotNil)
	c.Assert(DecodeTableID(nil), Equals, int64(0))
}

func (s *testTableCodecSuite) TestPrefix(c *C) {
	const blockID int64 = 66
	key := EncodeTablePrefix(blockID)
	tTableID := DecodeTableID(key)
	c.Assert(tTableID, Equals, blockID)

	c.Assert(TablePrefix(), BytesEquals, blockPrefix)

	blockPrefix1 := GenTablePrefix(blockID)
	c.Assert([]byte(blockPrefix1), BytesEquals, []byte(key))

	indexPrefix := EncodeTableIndexPrefix(blockID, math.MaxUint32)
	tTableID, indexID, isRecordKey, err := DecodeKeyHead(indexPrefix)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, blockID)
	c.Assert(indexID, Equals, int64(math.MaxUint32))
	c.Assert(isRecordKey, IsFalse)

	prefixKey := GenTableIndexPrefix(blockID)
	c.Assert(DecodeTableID(prefixKey), Equals, blockID)

	c.Assert(TruncateToRowKeyLen(append(indexPrefix, "xyz"...)), HasLen, RecordRowKeyLen)
	c.Assert(TruncateToRowKeyLen(key), HasLen, len(key))
}

func (s *testTableCodecSuite) TestDecodeIndexKey(c *C) {
	blockID := int64(4)
	indexID := int64(5)
	values := []types.Causet{
		types.NewIntCauset(1),
		types.NewBytesCauset([]byte("abc")),
		types.NewFloat64Causet(123.45),
		// MysqlTime is not supported.
		// types.NewTimeCauset(types.Time{
		// 	Time: types.FromGoTime(time.Now()),
		// 	Fsp:  6,
		// 	Type: allegrosql.TypeTimestamp,
		// }),
	}
	valueStrs := make([]string, 0, len(values))
	for _, v := range values {
		str, err := v.ToString()
		if err != nil {
			str = fmt.Sprintf("%d-%v", v.HoTT(), v.GetValue())
		}
		valueStrs = append(valueStrs, str)
	}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	encodedValue, err := codec.EncodeKey(sc, nil, values...)
	c.Assert(err, IsNil)
	indexKey := EncodeIndexSeekKey(blockID, indexID, encodedValue)

	decodeTableID, decodeIndexID, decodeValues, err := DecodeIndexKey(indexKey)
	c.Assert(err, IsNil)
	c.Assert(decodeTableID, Equals, blockID)
	c.Assert(decodeIndexID, Equals, indexID)
	c.Assert(decodeValues, DeepEquals, valueStrs)
}

func (s *testTableCodecSuite) TestCutPrefix(c *C) {
	key := EncodeTableIndexPrefix(42, 666)
	res := CutRowKeyPrefix(key)
	c.Assert(res, BytesEquals, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x9a})
	res = CutIndexPrefix(key)
	c.Assert(res, BytesEquals, []byte{})
}

func (s *testTableCodecSuite) TestRange(c *C) {
	s1, e1 := GetTableHandleKeyRange(22)
	s2, e2 := GetTableHandleKeyRange(23)
	c.Assert(s1, Less, e1)
	c.Assert(e1, Less, s2)
	c.Assert(s2, Less, e2)

	s1, e1 = GetTableIndexKeyRange(42, 666)
	s2, e2 = GetTableIndexKeyRange(42, 667)
	c.Assert(s1, Less, e1)
	c.Assert(e1, Less, s2)
	c.Assert(s2, Less, e2)
}

func (s *testTableCodecSuite) TestDecodeAutoIDMeta(c *C) {
	keyBytes := []byte{0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe}
	key, field, err := DecodeMetaKey(keyBytes)
	c.Assert(err, IsNil)
	c.Assert(string(key), Equals, "EDB:56")
	c.Assert(string(field), Equals, "TID:108")
}

func BenchmarkHasTablePrefix(b *testing.B) {
	k := ekv.Key("foobar")
	for i := 0; i < b.N; i++ {
		hasTablePrefix(k)
	}
}

func BenchmarkHasTablePrefixBuiltin(b *testing.B) {
	k := ekv.Key("foobar")
	for i := 0; i < b.N; i++ {
		k.HasPrefix(blockPrefix)
	}
}

// Bench result:
// BenchmarkEncodeValue      5000000           368 ns/op
func BenchmarkEncodeValue(b *testing.B) {
	event := make([]types.Causet, 7)
	event[0] = types.NewIntCauset(100)
	event[1] = types.NewBytesCauset([]byte("abc"))
	event[2] = types.NewDecimalCauset(types.NewDecFromInt(1))
	event[3] = types.NewMysqlEnumCauset(types.Enum{Name: "a", Value: 0})
	event[4] = types.NewCauset(types.Set{Name: "a", Value: 0})
	event[5] = types.NewCauset(types.BinaryLiteral{100})
	event[6] = types.NewFloat32Causet(1.5)
	b.ResetTimer()
	encodedDefCaus := make([]byte, 0, 16)
	for i := 0; i < b.N; i++ {
		for _, d := range event {
			encodedDefCaus = encodedDefCaus[:0]
			EncodeValue(nil, encodedDefCaus, d)
		}
	}
}

func (s *testTableCodecSuite) TestError(c *C) {
	ekvErrs := []*terror.Error{
		errInvalidKey,
		errInvalidRecordKey,
		errInvalidIndexKey,
	}
	for _, err := range ekvErrs {
		code := terror.ToALLEGROSQLError(err).Code
		c.Assert(code != allegrosql.ErrUnknown && code == uint16(err.Code()), IsTrue, Commentf("err: %v", err))
	}
}
