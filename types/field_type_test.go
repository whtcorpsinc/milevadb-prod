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

package types

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

var _ = Suite(&testFieldTypeSuite{})

type testFieldTypeSuite struct {
}

func (s *testFieldTypeSuite) TestFieldType(c *C) {
	defer testleak.AfterTest(c)()
	ft := NewFieldType(allegrosql.TypeDuration)
	c.Assert(ft.Flen, Equals, UnspecifiedLength)
	c.Assert(ft.Decimal, Equals, UnspecifiedLength)
	ft.Decimal = 5
	c.Assert(ft.String(), Equals, "time(5)")

	ft = NewFieldType(allegrosql.TypeLong)
	ft.Flen = 5
	ft.Flag = allegrosql.UnsignedFlag | allegrosql.ZerofillFlag
	c.Assert(ft.String(), Equals, "int(5) UNSIGNED ZEROFILL")
	c.Assert(ft.SchemaReplicantStr(), Equals, "int(5) unsigned")

	ft = NewFieldType(allegrosql.TypeFloat)
	ft.Flen = 12   // Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "float(12,3)")
	ft = NewFieldType(allegrosql.TypeFloat)
	ft.Flen = 12    // Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "float")
	ft = NewFieldType(allegrosql.TypeFloat)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "float")
	ft = NewFieldType(allegrosql.TypeFloat)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "float(7,3)")

	ft = NewFieldType(allegrosql.TypeDouble)
	ft.Flen = 22   // Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "double(22,3)")
	ft = NewFieldType(allegrosql.TypeDouble)
	ft.Flen = 22    // Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "double")
	ft = NewFieldType(allegrosql.TypeDouble)
	ft.Flen = 5     // Not Default
	ft.Decimal = -1 // Default
	c.Assert(ft.String(), Equals, "double")
	ft = NewFieldType(allegrosql.TypeDouble)
	ft.Flen = 7    // Not Default
	ft.Decimal = 3 // Not Default
	c.Assert(ft.String(), Equals, "double(7,3)")

	ft = NewFieldType(allegrosql.TypeBlob)
	ft.Flen = 10
	ft.Charset = "UTF8"
	ft.DefCauslate = "UTF8_UNICODE_GI"
	c.Assert(ft.String(), Equals, "text CHARACTER SET UTF8 COLLATE UTF8_UNICODE_GI")

	ft = NewFieldType(allegrosql.TypeVarchar)
	ft.Flen = 10
	ft.Flag |= allegrosql.BinaryFlag
	c.Assert(ft.String(), Equals, "varchar(10) BINARY COLLATE utf8mb4_bin")

	ft = NewFieldType(allegrosql.TypeString)
	ft.Charset = charset.DefCauslationBin
	ft.Flag |= allegrosql.BinaryFlag
	c.Assert(ft.String(), Equals, "binary(1) COLLATE utf8mb4_bin")

	ft = NewFieldType(allegrosql.TypeEnum)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "enum('a','b')")

	ft = NewFieldType(allegrosql.TypeEnum)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "enum('''a''','''b''')")

	ft = NewFieldType(allegrosql.TypeEnum)
	ft.Elems = []string{"a\nb", "a\tb", "a\rb"}
	c.Assert(ft.String(), Equals, "enum('a\\nb','a\tb','a\\rb')")

	ft = NewFieldType(allegrosql.TypeEnum)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	c.Assert(ft.String(), Equals, "enum('a\\nb','a''	\\r\\nb','a\\rb')")

	ft = NewFieldType(allegrosql.TypeSet)
	ft.Elems = []string{"a", "b"}
	c.Assert(ft.String(), Equals, "set('a','b')")

	ft = NewFieldType(allegrosql.TypeSet)
	ft.Elems = []string{"'a'", "'b'"}
	c.Assert(ft.String(), Equals, "set('''a''','''b''')")

	ft = NewFieldType(allegrosql.TypeSet)
	ft.Elems = []string{"a\nb", "a'\t\r\nb", "a\rb"}
	c.Assert(ft.String(), Equals, "set('a\\nb','a''	\\r\\nb','a\\rb')")

	ft = NewFieldType(allegrosql.TypeSet)
	ft.Elems = []string{"a'\nb", "a'b\tc"}
	c.Assert(ft.String(), Equals, "set('a''\\nb','a''b	c')")

	ft = NewFieldType(allegrosql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "timestamp(2)")
	ft = NewFieldType(allegrosql.TypeTimestamp)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "timestamp")

	ft = NewFieldType(allegrosql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "datetime(2)")
	ft = NewFieldType(allegrosql.TypeDatetime)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "datetime")

	ft = NewFieldType(allegrosql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "date")
	ft = NewFieldType(allegrosql.TypeDate)
	ft.Flen = 8
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "date")

	ft = NewFieldType(allegrosql.TypeYear)
	ft.Flen = 4
	ft.Decimal = 0
	c.Assert(ft.String(), Equals, "year(4)")
	ft = NewFieldType(allegrosql.TypeYear)
	ft.Flen = 2
	ft.Decimal = 2
	c.Assert(ft.String(), Equals, "year(2)") // Note: Invalid year.
}

func (s *testFieldTypeSuite) TestDefaultTypeForValue(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		value     interface{}
		tp        byte
		flen      int
		decimal   int
		charset   string
		collation string
		flag      uint
	}{
		{nil, allegrosql.TypeNull, 0, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{1, allegrosql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{0, allegrosql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{432, allegrosql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{4321, allegrosql.TypeLonglong, 4, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{1234567, allegrosql.TypeLonglong, 7, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{12345678, allegrosql.TypeLonglong, 8, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{12345678901234567, allegrosql.TypeLonglong, 17, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{-42, allegrosql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{uint64(1), allegrosql.TypeLonglong, 1, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag | allegrosql.UnsignedFlag},
		{uint64(123), allegrosql.TypeLonglong, 3, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag | allegrosql.UnsignedFlag},
		{uint64(1234), allegrosql.TypeLonglong, 4, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag | allegrosql.UnsignedFlag},
		{uint64(1234567), allegrosql.TypeLonglong, 7, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag | allegrosql.UnsignedFlag},
		{uint64(12345678), allegrosql.TypeLonglong, 8, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag | allegrosql.UnsignedFlag},
		{uint64(12345678901234567), allegrosql.TypeLonglong, 17, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag | allegrosql.UnsignedFlag},
		{"abc", allegrosql.TypeVarString, 3, UnspecifiedLength, charset.CharsetUTF8MB4, charset.DefCauslationUTF8MB4, 0},
		{1.1, allegrosql.TypeDouble, 3, -1, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{[]byte("abc"), allegrosql.TypeBlob, 3, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{HexLiteral{}, allegrosql.TypeVarString, 0, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag | allegrosql.UnsignedFlag},
		{BitLiteral{}, allegrosql.TypeVarString, 0, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{NewTime(ZeroCoreTime, allegrosql.TypeDatetime, DefaultFsp), allegrosql.TypeDatetime, 19, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{NewTime(FromDate(2020, 12, 12, 12, 59, 59, 0), allegrosql.TypeDatetime, 3), allegrosql.TypeDatetime, 23, 3, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{Duration{}, allegrosql.TypeDuration, 8, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{&MyDecimal{}, allegrosql.TypeNewDecimal, 1, 0, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{Enum{Name: "a", Value: 1}, allegrosql.TypeEnum, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
		{Set{Name: "a", Value: 1}, allegrosql.TypeSet, 1, UnspecifiedLength, charset.CharsetBin, charset.CharsetBin, allegrosql.BinaryFlag},
	}
	for _, tt := range tests {
		var ft FieldType
		DefaultTypeForValue(tt.value, &ft, allegrosql.DefaultCharset, allegrosql.DefaultDefCauslationName)
		c.Assert(ft.Tp, Equals, tt.tp, Commentf("%v %v", ft.Tp, tt.tp))
		c.Assert(ft.Flen, Equals, tt.flen, Commentf("%v %v", ft.Flen, tt.flen))
		c.Assert(ft.Charset, Equals, tt.charset, Commentf("%v %v", ft.Charset, tt.charset))
		c.Assert(ft.Decimal, Equals, tt.decimal, Commentf("%v %v", ft.Decimal, tt.decimal))
		c.Assert(ft.DefCauslate, Equals, tt.collation, Commentf("%v %v", ft.DefCauslate, tt.collation))
		c.Assert(ft.Flag, Equals, tt.flag, Commentf("%v %v", ft.Flag, tt.flag))
	}
}

func (s *testFieldTypeSuite) TestAggFieldType(c *C) {
	defer testleak.AfterTest(c)()
	fts := []*FieldType{
		NewFieldType(allegrosql.TypeUnspecified),
		NewFieldType(allegrosql.TypeTiny),
		NewFieldType(allegrosql.TypeShort),
		NewFieldType(allegrosql.TypeLong),
		NewFieldType(allegrosql.TypeFloat),
		NewFieldType(allegrosql.TypeDouble),
		NewFieldType(allegrosql.TypeNull),
		NewFieldType(allegrosql.TypeTimestamp),
		NewFieldType(allegrosql.TypeLonglong),
		NewFieldType(allegrosql.TypeInt24),
		NewFieldType(allegrosql.TypeDate),
		NewFieldType(allegrosql.TypeDuration),
		NewFieldType(allegrosql.TypeDatetime),
		NewFieldType(allegrosql.TypeYear),
		NewFieldType(allegrosql.TypeNewDate),
		NewFieldType(allegrosql.TypeVarchar),
		NewFieldType(allegrosql.TypeBit),
		NewFieldType(allegrosql.TypeJSON),
		NewFieldType(allegrosql.TypeNewDecimal),
		NewFieldType(allegrosql.TypeEnum),
		NewFieldType(allegrosql.TypeSet),
		NewFieldType(allegrosql.TypeTinyBlob),
		NewFieldType(allegrosql.TypeMediumBlob),
		NewFieldType(allegrosql.TypeLongBlob),
		NewFieldType(allegrosql.TypeBlob),
		NewFieldType(allegrosql.TypeVarString),
		NewFieldType(allegrosql.TypeString),
		NewFieldType(allegrosql.TypeGeometry),
	}

	for i := range fts {
		aggTp := AggFieldType(fts[i : i+1])
		c.Assert(aggTp.Tp, Equals, fts[i].Tp)

		aggTp = AggFieldType([]*FieldType{fts[i], fts[i]})
		switch fts[i].Tp {
		case allegrosql.TypeDate:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeDate)
		case allegrosql.TypeJSON:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeJSON)
		case allegrosql.TypeEnum, allegrosql.TypeSet, allegrosql.TypeVarString:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeVarchar)
		case allegrosql.TypeUnspecified:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeNewDecimal)
		default:
			c.Assert(aggTp.Tp, Equals, fts[i].Tp)
		}

		aggTp = AggFieldType([]*FieldType{fts[i], NewFieldType(allegrosql.TypeLong)})
		switch fts[i].Tp {
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeLong,
			allegrosql.TypeYear, allegrosql.TypeInt24, allegrosql.TypeNull:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeLong)
		case allegrosql.TypeLonglong:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeLonglong)
		case allegrosql.TypeFloat, allegrosql.TypeDouble:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeDouble)
		case allegrosql.TypeTimestamp, allegrosql.TypeDate, allegrosql.TypeDuration,
			allegrosql.TypeDatetime, allegrosql.TypeNewDate, allegrosql.TypeVarchar,
			allegrosql.TypeBit, allegrosql.TypeJSON, allegrosql.TypeEnum, allegrosql.TypeSet,
			allegrosql.TypeVarString, allegrosql.TypeGeometry:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeVarchar)
		case allegrosql.TypeString:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeString)
		case allegrosql.TypeUnspecified, allegrosql.TypeNewDecimal:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeNewDecimal)
		case allegrosql.TypeTinyBlob:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeTinyBlob)
		case allegrosql.TypeBlob:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeBlob)
		case allegrosql.TypeMediumBlob:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeMediumBlob)
		case allegrosql.TypeLongBlob:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeLongBlob)
		}

		aggTp = AggFieldType([]*FieldType{fts[i], NewFieldType(allegrosql.TypeJSON)})
		switch fts[i].Tp {
		case allegrosql.TypeJSON, allegrosql.TypeNull:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeJSON)
		case allegrosql.TypeLongBlob, allegrosql.TypeMediumBlob, allegrosql.TypeTinyBlob, allegrosql.TypeBlob:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeLongBlob)
		case allegrosql.TypeString:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeString)
		default:
			c.Assert(aggTp.Tp, Equals, allegrosql.TypeVarchar)
		}
	}
}
func (s *testFieldTypeSuite) TestAggFieldTypeForTypeFlag(c *C) {
	types := []*FieldType{
		NewFieldType(allegrosql.TypeLonglong),
		NewFieldType(allegrosql.TypeLonglong),
	}

	aggTp := AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, allegrosql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, uint(0))

	types[0].Flag = allegrosql.NotNullFlag
	aggTp = AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, allegrosql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, uint(0))

	types[0].Flag = 0
	types[1].Flag = allegrosql.NotNullFlag
	aggTp = AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, allegrosql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, uint(0))

	types[0].Flag = allegrosql.NotNullFlag
	aggTp = AggFieldType(types)
	c.Assert(aggTp.Tp, Equals, allegrosql.TypeLonglong)
	c.Assert(aggTp.Flag, Equals, allegrosql.NotNullFlag)
}

func (s *testFieldTypeSuite) TestAggregateEvalType(c *C) {
	defer testleak.AfterTest(c)()
	fts := []*FieldType{
		NewFieldType(allegrosql.TypeUnspecified),
		NewFieldType(allegrosql.TypeTiny),
		NewFieldType(allegrosql.TypeShort),
		NewFieldType(allegrosql.TypeLong),
		NewFieldType(allegrosql.TypeFloat),
		NewFieldType(allegrosql.TypeDouble),
		NewFieldType(allegrosql.TypeNull),
		NewFieldType(allegrosql.TypeTimestamp),
		NewFieldType(allegrosql.TypeLonglong),
		NewFieldType(allegrosql.TypeInt24),
		NewFieldType(allegrosql.TypeDate),
		NewFieldType(allegrosql.TypeDuration),
		NewFieldType(allegrosql.TypeDatetime),
		NewFieldType(allegrosql.TypeYear),
		NewFieldType(allegrosql.TypeNewDate),
		NewFieldType(allegrosql.TypeVarchar),
		NewFieldType(allegrosql.TypeBit),
		NewFieldType(allegrosql.TypeJSON),
		NewFieldType(allegrosql.TypeNewDecimal),
		NewFieldType(allegrosql.TypeEnum),
		NewFieldType(allegrosql.TypeSet),
		NewFieldType(allegrosql.TypeTinyBlob),
		NewFieldType(allegrosql.TypeMediumBlob),
		NewFieldType(allegrosql.TypeLongBlob),
		NewFieldType(allegrosql.TypeBlob),
		NewFieldType(allegrosql.TypeVarString),
		NewFieldType(allegrosql.TypeString),
		NewFieldType(allegrosql.TypeGeometry),
	}

	for i := range fts {
		var flag uint
		aggregatedEvalType := AggregateEvalType(fts[i:i+1], &flag)
		switch fts[i].Tp {
		case allegrosql.TypeUnspecified, allegrosql.TypeNull, allegrosql.TypeTimestamp, allegrosql.TypeDate,
			allegrosql.TypeDuration, allegrosql.TypeDatetime, allegrosql.TypeNewDate, allegrosql.TypeVarchar,
			allegrosql.TypeJSON, allegrosql.TypeEnum, allegrosql.TypeSet, allegrosql.TypeTinyBlob,
			allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob, allegrosql.TypeBlob,
			allegrosql.TypeVarString, allegrosql.TypeString, allegrosql.TypeGeometry:
			c.Assert(aggregatedEvalType.IsStringHoTT(), IsTrue)
			c.Assert(flag, Equals, uint(0))
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeBit,
			allegrosql.TypeInt24, allegrosql.TypeYear:
			c.Assert(aggregatedEvalType, Equals, ETInt)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		case allegrosql.TypeFloat, allegrosql.TypeDouble:
			c.Assert(aggregatedEvalType, Equals, ETReal)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		case allegrosql.TypeNewDecimal:
			c.Assert(aggregatedEvalType, Equals, ETDecimal)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		}

		flag = 0
		aggregatedEvalType = AggregateEvalType([]*FieldType{fts[i], fts[i]}, &flag)
		switch fts[i].Tp {
		case allegrosql.TypeUnspecified, allegrosql.TypeNull, allegrosql.TypeTimestamp, allegrosql.TypeDate,
			allegrosql.TypeDuration, allegrosql.TypeDatetime, allegrosql.TypeNewDate, allegrosql.TypeVarchar,
			allegrosql.TypeJSON, allegrosql.TypeEnum, allegrosql.TypeSet, allegrosql.TypeTinyBlob,
			allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob, allegrosql.TypeBlob,
			allegrosql.TypeVarString, allegrosql.TypeString, allegrosql.TypeGeometry:
			c.Assert(aggregatedEvalType.IsStringHoTT(), IsTrue)
			c.Assert(flag, Equals, uint(0))
		case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeBit,
			allegrosql.TypeInt24, allegrosql.TypeYear:
			c.Assert(aggregatedEvalType, Equals, ETInt)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		case allegrosql.TypeFloat, allegrosql.TypeDouble:
			c.Assert(aggregatedEvalType, Equals, ETReal)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		case allegrosql.TypeNewDecimal:
			c.Assert(aggregatedEvalType, Equals, ETDecimal)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		}
		flag = 0
		aggregatedEvalType = AggregateEvalType([]*FieldType{fts[i], NewFieldType(allegrosql.TypeLong)}, &flag)
		switch fts[i].Tp {
		case allegrosql.TypeTimestamp, allegrosql.TypeDate, allegrosql.TypeDuration,
			allegrosql.TypeDatetime, allegrosql.TypeNewDate, allegrosql.TypeVarchar, allegrosql.TypeJSON,
			allegrosql.TypeEnum, allegrosql.TypeSet, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob,
			allegrosql.TypeLongBlob, allegrosql.TypeBlob, allegrosql.TypeVarString,
			allegrosql.TypeString, allegrosql.TypeGeometry:
			c.Assert(aggregatedEvalType.IsStringHoTT(), IsTrue)
			c.Assert(flag, Equals, uint(0))
		case allegrosql.TypeUnspecified, allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeLong, allegrosql.TypeNull, allegrosql.TypeBit,
			allegrosql.TypeLonglong, allegrosql.TypeYear, allegrosql.TypeInt24:
			c.Assert(aggregatedEvalType, Equals, ETInt)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		case allegrosql.TypeFloat, allegrosql.TypeDouble:
			c.Assert(aggregatedEvalType, Equals, ETReal)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		case allegrosql.TypeNewDecimal:
			c.Assert(aggregatedEvalType, Equals, ETDecimal)
			c.Assert(flag, Equals, allegrosql.BinaryFlag)
		}
	}
}
