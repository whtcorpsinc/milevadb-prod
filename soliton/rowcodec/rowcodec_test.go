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
	"math"
	"strings"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

type testData struct {
	id     int64
	ft     *types.FieldType
	dt     types.Causet
	bt     types.Causet
	def    *types.Causet
	handle bool
}

func (s *testSuite) TestEncodeLargeSmallReuseBug(c *C) {
	// reuse one rowcodec.Encoder.
	var encoder rowcodec.Encoder
	defCausFt := types.NewFieldType(allegrosql.TypeString)

	largeDefCausID := int64(300)
	b, err := encoder.Encode(&stmtctx.StatementContext{}, []int64{largeDefCausID}, []types.Causet{types.NewBytesCauset([]byte(""))}, nil)
	c.Assert(err, IsNil)

	bDecoder := rowcodec.NewCausetMaFIDelecoder([]rowcodec.DefCausInfo{
		{
			ID:         largeDefCausID,
			Ft:         defCausFt,
			IsPKHandle: false,
		},
	}, nil)
	m, err := bDecoder.DecodeToCausetMap(b, nil)
	c.Assert(err, IsNil)
	v := m[largeDefCausID]

	defCausFt = types.NewFieldType(allegrosql.TypeLonglong)
	smallDefCausID := int64(1)
	b, err = encoder.Encode(&stmtctx.StatementContext{}, []int64{smallDefCausID}, []types.Causet{types.NewIntCauset(2)}, nil)
	c.Assert(err, IsNil)

	bDecoder = rowcodec.NewCausetMaFIDelecoder([]rowcodec.DefCausInfo{
		{
			ID:         smallDefCausID,
			Ft:         defCausFt,
			IsPKHandle: false,
		},
	}, nil)
	m, err = bDecoder.DecodeToCausetMap(b, nil)
	c.Assert(err, IsNil)
	v = m[smallDefCausID]
	c.Assert(v.GetInt64(), Equals, int64(2))
}

func (s *testSuite) TestDecodeRowWithHandle(c *C) {
	handleID := int64(-1)
	handleValue := int64(10000)

	encodeAndDecodeHandle := func(c *C, testData []testData) {
		// transform test data into input.
		defCausIDs := make([]int64, 0, len(testData))
		dts := make([]types.Causet, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		defcaus := make([]rowcodec.DefCausInfo, 0, len(testData))
		handleDefCausFtMap := make(map[int64]*types.FieldType)
		for i := range testData {
			t := testData[i]
			if t.handle {
				handleDefCausFtMap[handleID] = t.ft
			} else {
				defCausIDs = append(defCausIDs, t.id)
				dts = append(dts, t.dt)
			}
			fts = append(fts, t.ft)
			defcaus = append(defcaus, rowcodec.DefCausInfo{
				ID:         t.id,
				IsPKHandle: t.handle,
				Ft:         t.ft,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, defCausIDs, dts, nil)
		c.Assert(err, IsNil)

		// decode to causet map.
		mDecoder := rowcodec.NewCausetMaFIDelecoder(defcaus, sc.TimeZone)
		dm, err := mDecoder.DecodeToCausetMap(newRow, nil)
		c.Assert(err, IsNil)
		dm, err = blockcodec.DecodeHandleToCausetMap(ekv.IntHandle(handleValue),
			[]int64{handleID}, handleDefCausFtMap, sc.TimeZone, dm)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			c.Assert(exists, IsTrue)
			c.Assert(d, DeepEquals, t.dt)
		}

		// decode to chunk.
		cDecoder := rowcodec.NewChunkDecoder(defcaus, []int64{-1}, nil, sc.TimeZone)
		chk := chunk.New(fts, 1, 1)
		err = cDecoder.DecodeToChunk(newRow, ekv.IntHandle(handleValue), chk)
		c.Assert(err, IsNil)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetCausetRow(fts)
		for i, t := range testData {
			d := cdt[i]
			if d.HoTT() == types.HoTTMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
			}
		}

		// decode to old event bytes.
		defCausOffset := make(map[int64]int)
		for i, t := range testData {
			defCausOffset[t.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(defcaus, []int64{-1}, nil, nil)
		oldRow, err := bDecoder.DecodeToBytes(defCausOffset, ekv.IntHandle(handleValue), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			c.Assert(err, IsNil)
			c.Assert(len(remain), Equals, 0)
			if d.HoTT() == types.HoTTMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
			}
		}
	}

	// encode & decode signed int.
	testDataSigned := []testData{
		{
			handleID,
			types.NewFieldType(allegrosql.TypeLonglong),
			types.NewIntCauset(handleValue),
			types.NewIntCauset(handleValue),
			nil,
			true,
		},
		{
			10,
			types.NewFieldType(allegrosql.TypeLonglong),
			types.NewIntCauset(1),
			types.NewIntCauset(1),
			nil,
			false,
		},
	}
	encodeAndDecodeHandle(c, testDataSigned)

	// encode & decode unsigned int.
	testDataUnsigned := []testData{
		{
			handleID,
			withUnsigned(types.NewFieldType(allegrosql.TypeLonglong)),
			types.NewUintCauset(uint64(handleValue)),
			types.NewUintCauset(uint64(handleValue)), // decode as bytes will uint if unsigned.
			nil,
			true,
		},
		{
			10,
			types.NewFieldType(allegrosql.TypeLonglong),
			types.NewIntCauset(1),
			types.NewIntCauset(1),
			nil,
			false,
		},
	}
	encodeAndDecodeHandle(c, testDataUnsigned)
}

func (s *testSuite) TestEncodeHoTTNullCauset(c *C) {
	var encoder rowcodec.Encoder
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
	defCausIDs := []int64{
		1,
		2,
	}
	var nilDt types.Causet
	nilDt.SetNull()
	dts := []types.Causet{nilDt, types.NewIntCauset(2)}
	ft := types.NewFieldType(allegrosql.TypeLonglong)
	fts := []*types.FieldType{ft, ft}
	newRow, err := encoder.Encode(sc, defCausIDs, dts, nil)
	c.Assert(err, IsNil)

	defcaus := []rowcodec.DefCausInfo{{
		ID: 1,
		Ft: ft,
	},
		{
			ID: 2,
			Ft: ft,
		}}
	cDecoder := rowcodec.NewChunkDecoder(defcaus, []int64{-1}, nil, sc.TimeZone)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, ekv.IntHandle(-1), chk)
	c.Assert(err, IsNil)
	chkRow := chk.GetRow(0)
	cdt := chkRow.GetCausetRow(fts)
	c.Assert(cdt[0].IsNull(), Equals, true)
	c.Assert(cdt[1].GetInt64(), Equals, int64(2))
}

func (s *testSuite) TestDecodeDecimalFspNotMatch(c *C) {
	var encoder rowcodec.Encoder
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = time.UTC
	defCausIDs := []int64{
		1,
	}
	dec := withFrac(4)(withLen(6)(types.NewDecimalCauset(types.NewDecFromStringForTest("11.9900"))))
	dts := []types.Causet{dec}
	ft := types.NewFieldType(allegrosql.TypeNewDecimal)
	ft.Decimal = 4
	fts := []*types.FieldType{ft}
	newRow, err := encoder.Encode(sc, defCausIDs, dts, nil)
	c.Assert(err, IsNil)

	// decode to chunk.
	ft = types.NewFieldType(allegrosql.TypeNewDecimal)
	ft.Decimal = 3
	defcaus := make([]rowcodec.DefCausInfo, 0)
	defcaus = append(defcaus, rowcodec.DefCausInfo{
		ID: 1,
		Ft: ft,
	})
	cDecoder := rowcodec.NewChunkDecoder(defcaus, []int64{-1}, nil, sc.TimeZone)
	chk := chunk.New(fts, 1, 1)
	err = cDecoder.DecodeToChunk(newRow, ekv.IntHandle(-1), chk)
	c.Assert(err, IsNil)
	chkRow := chk.GetRow(0)
	cdt := chkRow.GetCausetRow(fts)
	dec = withFrac(3)(withLen(6)(types.NewDecimalCauset(types.NewDecFromStringForTest("11.990"))))
	c.Assert(cdt[0].GetMysqlDecimal().String(), DeepEquals, dec.GetMysqlDecimal().String())
}

func (s *testSuite) TestTypesNewRowCodec(c *C) {
	getJSONCauset := func(value string) types.Causet {
		j, err := json.ParseBinaryFromString(value)
		c.Assert(err, IsNil)
		var d types.Causet
		d.SetMysqlJSON(j)
		return d
	}
	getSetCauset := func(name string, value uint64) types.Causet {
		var d types.Causet
		d.SetMysqlSet(types.Set{Name: name, Value: value}, allegrosql.DefaultDefCauslationName)
		return d
	}
	getTime := func(value string) types.Time {
		t, err := types.ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, value, allegrosql.TypeTimestamp, 6)
		c.Assert(err, IsNil)
		return t
	}

	var encoder rowcodec.Encoder
	encodeAndDecode := func(c *C, testData []testData) {
		// transform test data into input.
		defCausIDs := make([]int64, 0, len(testData))
		dts := make([]types.Causet, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		defcaus := make([]rowcodec.DefCausInfo, 0, len(testData))
		for i := range testData {
			t := testData[i]
			defCausIDs = append(defCausIDs, t.id)
			dts = append(dts, t.dt)
			fts = append(fts, t.ft)
			defcaus = append(defcaus, rowcodec.DefCausInfo{
				ID:         t.id,
				IsPKHandle: t.handle,
				Ft:         t.ft,
			})
		}

		// test encode input.
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, defCausIDs, dts, nil)
		c.Assert(err, IsNil)

		// decode to causet map.
		mDecoder := rowcodec.NewCausetMaFIDelecoder(defcaus, sc.TimeZone)
		dm, err := mDecoder.DecodeToCausetMap(newRow, nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			c.Assert(exists, IsTrue)
			c.Assert(d, DeepEquals, t.dt)
		}

		// decode to chunk.
		cDecoder := rowcodec.NewChunkDecoder(defcaus, []int64{-1}, nil, sc.TimeZone)
		chk := chunk.New(fts, 1, 1)
		err = cDecoder.DecodeToChunk(newRow, ekv.IntHandle(-1), chk)
		c.Assert(err, IsNil)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetCausetRow(fts)
		for i, t := range testData {
			d := cdt[i]
			if d.HoTT() == types.HoTTMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.dt)
			}
		}

		// decode to old event bytes.
		defCausOffset := make(map[int64]int)
		for i, t := range testData {
			defCausOffset[t.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(defcaus, []int64{-1}, nil, nil)
		oldRow, err := bDecoder.DecodeToBytes(defCausOffset, ekv.IntHandle(-1), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			c.Assert(err, IsNil)
			c.Assert(len(remain), Equals, 0)
			if d.HoTT() == types.HoTTMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
			}
		}
	}

	testData := []testData{
		{
			1,
			types.NewFieldType(allegrosql.TypeLonglong),
			types.NewIntCauset(1),
			types.NewIntCauset(1),
			nil,
			false,
		},
		{
			22,
			withUnsigned(types.NewFieldType(allegrosql.TypeShort)),
			types.NewUintCauset(1),
			types.NewUintCauset(1),
			nil,
			false,
		},
		{
			3,
			types.NewFieldType(allegrosql.TypeDouble),
			types.NewFloat64Causet(2),
			types.NewFloat64Causet(2),
			nil,
			false,
		},
		{
			24,
			types.NewFieldType(allegrosql.TypeBlob),
			types.NewBytesCauset([]byte("abc")),
			types.NewBytesCauset([]byte("abc")),
			nil,
			false,
		},
		{
			25,
			&types.FieldType{Tp: allegrosql.TypeString, DefCauslate: allegrosql.DefaultDefCauslationName},
			types.NewStringCauset("ab"),
			types.NewBytesCauset([]byte("ab")),
			nil,
			false,
		},
		{
			5,
			withFsp(6)(types.NewFieldType(allegrosql.TypeTimestamp)),
			types.NewTimeCauset(getTime("2011-11-10 11:11:11.999999")),
			types.NewUintCauset(1840446893366133311),
			nil,
			false,
		},
		{
			16,
			withFsp(0)(types.NewFieldType(allegrosql.TypeDuration)),
			types.NewDurationCauset(getDuration("4:00:00")),
			types.NewIntCauset(14400000000000),
			nil,
			false,
		},
		{
			8,
			types.NewFieldType(allegrosql.TypeNewDecimal),
			withFrac(4)(withLen(6)(types.NewDecimalCauset(types.NewDecFromStringForTest("11.9900")))),
			withFrac(4)(withLen(6)(types.NewDecimalCauset(types.NewDecFromStringForTest("11.9900")))),
			nil,
			false,
		},
		{
			12,
			types.NewFieldType(allegrosql.TypeYear),
			types.NewIntCauset(1999),
			types.NewIntCauset(1999),
			nil,
			false,
		},
		{
			9,
			withEnumElems("y", "n")(types.NewFieldTypeWithDefCauslation(allegrosql.TypeEnum, allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)),
			types.NewMysqlEnumCauset(types.Enum{Name: "n", Value: 2}),
			types.NewUintCauset(2),
			nil,
			false,
		},
		{
			14,
			types.NewFieldType(allegrosql.TypeJSON),
			getJSONCauset(`{"a":2}`),
			getJSONCauset(`{"a":2}`),
			nil,
			false,
		},
		{
			11,
			types.NewFieldType(allegrosql.TypeNull),
			types.NewCauset(nil),
			types.NewCauset(nil),
			nil,
			false,
		},
		{
			2,
			types.NewFieldType(allegrosql.TypeNull),
			types.NewCauset(nil),
			types.NewCauset(nil),
			nil,
			false,
		},
		{
			100,
			types.NewFieldType(allegrosql.TypeNull),
			types.NewCauset(nil),
			types.NewCauset(nil),
			nil,
			false,
		},
		{
			116,
			types.NewFieldType(allegrosql.TypeFloat),
			types.NewFloat32Causet(6),
			types.NewFloat64Causet(6),
			nil,
			false,
		},
		{
			117,
			withEnumElems("n1", "n2")(types.NewFieldTypeWithDefCauslation(allegrosql.TypeSet, allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)),
			getSetCauset("n1", 1),
			types.NewUintCauset(1),
			nil,
			false,
		},
		{
			118,
			withFlen(24)(types.NewFieldType(allegrosql.TypeBit)), // 3 bit
			types.NewMysqlBitCauset(types.NewBinaryLiteralFromUint(3223600, 3)),
			types.NewUintCauset(3223600),
			nil,
			false,
		},
		{
			119,
			&types.FieldType{Tp: allegrosql.TypeVarString, DefCauslate: allegrosql.DefaultDefCauslationName},
			types.NewStringCauset(""),
			types.NewBytesCauset([]byte("")),
			nil,
			false,
		},
	}

	// test small
	encodeAndDecode(c, testData)

	// test large defCausID
	testData[0].id = 300
	encodeAndDecode(c, testData)
	testData[0].id = 1

	// test large data
	testData[3].dt = types.NewBytesCauset([]byte(strings.Repeat("a", math.MaxUint16+1)))
	testData[3].bt = types.NewBytesCauset([]byte(strings.Repeat("a", math.MaxUint16+1)))
	encodeAndDecode(c, testData)
}

func (s *testSuite) TestNilAndDefault(c *C) {
	encodeAndDecode := func(c *C, testData []testData) {
		// transform test data into input.
		defCausIDs := make([]int64, 0, len(testData))
		dts := make([]types.Causet, 0, len(testData))
		defcaus := make([]rowcodec.DefCausInfo, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		for i := range testData {
			t := testData[i]
			if t.def == nil {
				defCausIDs = append(defCausIDs, t.id)
				dts = append(dts, t.dt)
			}
			fts = append(fts, t.ft)
			defcaus = append(defcaus, rowcodec.DefCausInfo{
				ID:         t.id,
				IsPKHandle: t.handle,
				Ft:         t.ft,
			})
		}
		ddf := func(i int, chk *chunk.Chunk) error {
			t := testData[i]
			if t.def == nil {
				chk.AppendNull(i)
				return nil
			}
			chk.AppendCauset(i, t.def)
			return nil
		}
		bdf := func(i int) ([]byte, error) {
			t := testData[i]
			if t.def == nil {
				return nil, nil
			}
			return getOldCausetByte(*t.def), nil
		}
		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, defCausIDs, dts, nil)
		c.Assert(err, IsNil)

		// decode to causet map.
		mDecoder := rowcodec.NewCausetMaFIDelecoder(defcaus, sc.TimeZone)
		dm, err := mDecoder.DecodeToCausetMap(newRow, nil)
		c.Assert(err, IsNil)
		for _, t := range testData {
			d, exists := dm[t.id]
			if t.def != nil {
				// for causet should not fill default value.
				c.Assert(exists, IsFalse)
			} else {
				c.Assert(exists, IsTrue)
				c.Assert(d, DeepEquals, t.bt)
			}
		}

		//decode to chunk.
		chk := chunk.New(fts, 1, 1)
		cDecoder := rowcodec.NewChunkDecoder(defcaus, []int64{-1}, ddf, sc.TimeZone)
		err = cDecoder.DecodeToChunk(newRow, ekv.IntHandle(-1), chk)
		c.Assert(err, IsNil)
		chkRow := chk.GetRow(0)
		cdt := chkRow.GetCausetRow(fts)
		for i, t := range testData {
			d := cdt[i]
			if d.HoTT() == types.HoTTMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
			}
		}

		chk = chunk.New(fts, 1, 1)
		cDecoder = rowcodec.NewChunkDecoder(defcaus, []int64{-1}, nil, sc.TimeZone)
		err = cDecoder.DecodeToChunk(newRow, ekv.IntHandle(-1), chk)
		c.Assert(err, IsNil)
		chkRow = chk.GetRow(0)
		cdt = chkRow.GetCausetRow(fts)
		for i := range testData {
			if i == 0 {
				continue
			}
			d := cdt[i]
			c.Assert(d.IsNull(), Equals, true)
		}

		// decode to old event bytes.
		defCausOffset := make(map[int64]int)
		for i, t := range testData {
			defCausOffset[t.id] = i
		}
		bDecoder := rowcodec.NewByteDecoder(defcaus, []int64{-1}, bdf, sc.TimeZone)
		oldRow, err := bDecoder.DecodeToBytes(defCausOffset, ekv.IntHandle(-1), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			remain, d, err := codec.DecodeOne(oldRow[i])
			c.Assert(err, IsNil)
			c.Assert(len(remain), Equals, 0)
			if d.HoTT() == types.HoTTMysqlDecimal {
				c.Assert(d.GetMysqlDecimal(), DeepEquals, t.bt.GetMysqlDecimal())
			} else {
				c.Assert(d, DeepEquals, t.bt)
			}
		}
	}
	dtNilData := []testData{
		{
			1,
			types.NewFieldType(allegrosql.TypeLonglong),
			types.NewIntCauset(1),
			types.NewIntCauset(1),
			nil,
			false,
		},
		{
			2,
			withUnsigned(types.NewFieldType(allegrosql.TypeLonglong)),
			types.NewUintCauset(1),
			types.NewUintCauset(9),
			getCausetPoint(types.NewUintCauset(9)),
			false,
		},
	}
	encodeAndDecode(c, dtNilData)
}

func (s *testSuite) TestVarintCompatibility(c *C) {
	encodeAndDecodeByte := func(c *C, testData []testData) {
		// transform test data into input.
		defCausIDs := make([]int64, 0, len(testData))
		dts := make([]types.Causet, 0, len(testData))
		fts := make([]*types.FieldType, 0, len(testData))
		defcaus := make([]rowcodec.DefCausInfo, 0, len(testData))
		for i := range testData {
			t := testData[i]
			defCausIDs = append(defCausIDs, t.id)
			dts = append(dts, t.dt)
			fts = append(fts, t.ft)
			defcaus = append(defcaus, rowcodec.DefCausInfo{
				ID:         t.id,
				IsPKHandle: t.handle,
				Ft:         t.ft,
			})
		}

		// test encode input.
		var encoder rowcodec.Encoder
		sc := new(stmtctx.StatementContext)
		sc.TimeZone = time.UTC
		newRow, err := encoder.Encode(sc, defCausIDs, dts, nil)
		c.Assert(err, IsNil)
		decoder := rowcodec.NewByteDecoder(defcaus, []int64{-1}, nil, sc.TimeZone)
		// decode to old event bytes.
		defCausOffset := make(map[int64]int)
		for i, t := range testData {
			defCausOffset[t.id] = i
		}
		oldRow, err := decoder.DecodeToBytes(defCausOffset, ekv.IntHandle(1), newRow, nil)
		c.Assert(err, IsNil)
		for i, t := range testData {
			oldVarint, err := blockcodec.EncodeValue(nil, nil, t.bt) // blockcodec will encode as varint/varuint
			c.Assert(err, IsNil)
			c.Assert(oldVarint, DeepEquals, oldRow[i])
		}
	}

	testDataValue := []testData{
		{
			1,
			types.NewFieldType(allegrosql.TypeLonglong),
			types.NewIntCauset(1),
			types.NewIntCauset(1),
			nil,
			false,
		},
		{
			2,
			withUnsigned(types.NewFieldType(allegrosql.TypeLonglong)),
			types.NewUintCauset(1),
			types.NewUintCauset(1),
			nil,
			false,
		},
	}
	encodeAndDecodeByte(c, testDataValue)
}

func (s *testSuite) TestCodecUtil(c *C) {
	defCausIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(allegrosql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(allegrosql.TypeNull)
	sc := new(stmtctx.StatementContext)
	oldRow, err := blockcodec.EncodeOldRow(sc, types.MakeCausets(1, 2, 3, nil), defCausIDs, nil, nil)
	c.Check(err, IsNil)
	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRow(&rb, nil, oldRow, nil)
	c.Assert(err, IsNil)
	c.Assert(rowcodec.IsNewFormat(newRow), IsTrue)
	c.Assert(rowcodec.IsNewFormat(oldRow), IsFalse)

	// test stringer for decoder.
	var defcaus []rowcodec.DefCausInfo
	for i, ft := range tps {
		defcaus = append(defcaus, rowcodec.DefCausInfo{
			ID:         defCausIDs[i],
			IsPKHandle: false,
			Ft:         ft,
		})
	}
	d := rowcodec.NewDecoder(defcaus, []int64{-1}, nil)

	// test DeferredCausetIsNull
	isNil, err := d.DeferredCausetIsNull(newRow, 4, nil)
	c.Assert(err, IsNil)
	c.Assert(isNil, IsTrue)
	isNil, err = d.DeferredCausetIsNull(newRow, 1, nil)
	c.Assert(err, IsNil)
	c.Assert(isNil, IsFalse)
	isNil, err = d.DeferredCausetIsNull(newRow, 5, nil)
	c.Assert(err, IsNil)
	c.Assert(isNil, IsTrue)
	isNil, err = d.DeferredCausetIsNull(newRow, 5, []byte{1})
	c.Assert(err, IsNil)
	c.Assert(isNil, IsFalse)

	// test isRowKey
	c.Assert(rowcodec.IsRowKey([]byte{'b', 't'}), IsFalse)
	c.Assert(rowcodec.IsRowKey([]byte{'t', 'r'}), IsFalse)
}

func (s *testSuite) TestOldRowCodec(c *C) {
	defCausIDs := []int64{1, 2, 3, 4}
	tps := make([]*types.FieldType, 4)
	for i := 0; i < 3; i++ {
		tps[i] = types.NewFieldType(allegrosql.TypeLonglong)
	}
	tps[3] = types.NewFieldType(allegrosql.TypeNull)
	sc := new(stmtctx.StatementContext)
	oldRow, err := blockcodec.EncodeOldRow(sc, types.MakeCausets(1, 2, 3, nil), defCausIDs, nil, nil)
	c.Check(err, IsNil)

	var (
		rb     rowcodec.Encoder
		newRow []byte
	)
	newRow, err = rowcodec.EncodeFromOldRow(&rb, nil, oldRow, nil)
	c.Check(err, IsNil)
	defcaus := make([]rowcodec.DefCausInfo, len(tps))
	for i, tp := range tps {
		defcaus[i] = rowcodec.DefCausInfo{
			ID: defCausIDs[i],
			Ft: tp,
		}
	}
	rd := rowcodec.NewChunkDecoder(defcaus, []int64{-1}, nil, time.Local)
	chk := chunk.NewChunkWithCapacity(tps, 1)
	err = rd.DecodeToChunk(newRow, ekv.IntHandle(-1), chk)
	c.Assert(err, IsNil)
	event := chk.GetRow(0)
	for i := 0; i < 3; i++ {
		c.Assert(event.GetInt64(i), Equals, int64(i)+1)
	}
}

func (s *testSuite) Test65535Bug(c *C) {
	defCausIds := []int64{1}
	tps := make([]*types.FieldType, 1)
	tps[0] = types.NewFieldType(allegrosql.TypeString)
	sc := new(stmtctx.StatementContext)
	text65535 := strings.Repeat("a", 65535)
	encode := rowcodec.Encoder{}
	bd, err := encode.Encode(sc, defCausIds, []types.Causet{types.NewStringCauset(text65535)}, nil)
	c.Check(err, IsNil)

	defcaus := make([]rowcodec.DefCausInfo, 1)
	defcaus[0] = rowcodec.DefCausInfo{
		ID: 1,
		Ft: tps[0],
	}
	dc := rowcodec.NewCausetMaFIDelecoder(defcaus, nil)
	result, err := dc.DecodeToCausetMap(bd, nil)
	c.Check(err, IsNil)
	rs := result[1]
	c.Check(rs.GetString(), Equals, text65535)
}

var (
	withUnsigned = func(ft *types.FieldType) *types.FieldType {
		ft.Flag = ft.Flag | allegrosql.UnsignedFlag
		return ft
	}
	withEnumElems = func(elem ...string) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.Elems = elem
			return ft
		}
	}
	withFsp = func(fsp int) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.Decimal = fsp
			return ft
		}
	}
	withFlen = func(flen int) func(ft *types.FieldType) *types.FieldType {
		return func(ft *types.FieldType) *types.FieldType {
			ft.Flen = flen
			return ft
		}
	}
	getDuration = func(value string) types.Duration {
		dur, _ := types.ParseDuration(nil, value, 0)
		return dur
	}
	getOldCausetByte = func(d types.Causet) []byte {
		b, err := blockcodec.EncodeValue(nil, nil, d)
		if err != nil {
			panic(err)
		}
		return b
	}
	getCausetPoint = func(d types.Causet) *types.Causet {
		return &d
	}
	withFrac = func(f int) func(d types.Causet) types.Causet {
		return func(d types.Causet) types.Causet {
			d.SetFrac(f)
			return d
		}
	}
	withLen = func(len int) func(d types.Causet) types.Causet {
		return func(d types.Causet) types.Causet {
			d.SetLength(len)
			return d
		}
	}
)
