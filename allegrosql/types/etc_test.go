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
	"io"
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTypeEtcSuite{})

type testTypeEtcSuite struct {
}

func testIsTypeBlob(c *C, tp byte, expect bool) {
	v := IsTypeBlob(tp)
	c.Assert(v, Equals, expect)
}

func testIsTypeChar(c *C, tp byte, expect bool) {
	v := IsTypeChar(tp)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestIsType(c *C) {
	defer testleak.AfterTest(c)()
	testIsTypeBlob(c, allegrosql.TypeTinyBlob, true)
	testIsTypeBlob(c, allegrosql.TypeMediumBlob, true)
	testIsTypeBlob(c, allegrosql.TypeBlob, true)
	testIsTypeBlob(c, allegrosql.TypeLongBlob, true)
	testIsTypeBlob(c, allegrosql.TypeInt24, false)

	testIsTypeChar(c, allegrosql.TypeString, true)
	testIsTypeChar(c, allegrosql.TypeVarchar, true)
	testIsTypeChar(c, allegrosql.TypeLong, false)
}

func testTypeStr(c *C, tp byte, expect string) {
	v := TypeStr(tp)
	c.Assert(v, Equals, expect)
}

func testTypeToStr(c *C, tp byte, charset string, expect string) {
	v := TypeToStr(tp, charset)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestTypeToStr(c *C) {
	defer testleak.AfterTest(c)()
	testTypeStr(c, allegrosql.TypeYear, "year")
	testTypeStr(c, 0xdd, "")

	testTypeToStr(c, allegrosql.TypeBlob, "utf8", "text")
	testTypeToStr(c, allegrosql.TypeLongBlob, "utf8", "longtext")
	testTypeToStr(c, allegrosql.TypeTinyBlob, "utf8", "tinytext")
	testTypeToStr(c, allegrosql.TypeMediumBlob, "utf8", "mediumtext")
	testTypeToStr(c, allegrosql.TypeVarchar, "binary", "varbinary")
	testTypeToStr(c, allegrosql.TypeString, "binary", "binary")
	testTypeToStr(c, allegrosql.TypeTiny, "binary", "tinyint")
	testTypeToStr(c, allegrosql.TypeBlob, "binary", "blob")
	testTypeToStr(c, allegrosql.TypeLongBlob, "binary", "longblob")
	testTypeToStr(c, allegrosql.TypeTinyBlob, "binary", "tinyblob")
	testTypeToStr(c, allegrosql.TypeMediumBlob, "binary", "mediumblob")
	testTypeToStr(c, allegrosql.TypeVarchar, "utf8", "varchar")
	testTypeToStr(c, allegrosql.TypeString, "utf8", "char")
	testTypeToStr(c, allegrosql.TypeShort, "binary", "smallint")
	testTypeToStr(c, allegrosql.TypeInt24, "binary", "mediumint")
	testTypeToStr(c, allegrosql.TypeLong, "binary", "int")
	testTypeToStr(c, allegrosql.TypeLonglong, "binary", "bigint")
	testTypeToStr(c, allegrosql.TypeFloat, "binary", "float")
	testTypeToStr(c, allegrosql.TypeDouble, "binary", "double")
	testTypeToStr(c, allegrosql.TypeYear, "binary", "year")
	testTypeToStr(c, allegrosql.TypeDuration, "binary", "time")
	testTypeToStr(c, allegrosql.TypeDatetime, "binary", "datetime")
	testTypeToStr(c, allegrosql.TypeDate, "binary", "date")
	testTypeToStr(c, allegrosql.TypeTimestamp, "binary", "timestamp")
	testTypeToStr(c, allegrosql.TypeNewDecimal, "binary", "decimal")
	testTypeToStr(c, allegrosql.TypeUnspecified, "binary", "unspecified")
	testTypeToStr(c, 0xdd, "binary", "")
	testTypeToStr(c, allegrosql.TypeBit, "binary", "bit")
	testTypeToStr(c, allegrosql.TypeEnum, "binary", "enum")
	testTypeToStr(c, allegrosql.TypeSet, "binary", "set")
}

func (s *testTypeEtcSuite) TestEOFAsNil(c *C) {
	defer testleak.AfterTest(c)()
	err := EOFAsNil(io.EOF)
	c.Assert(err, IsNil)
	err = EOFAsNil(errors.New("test"))
	c.Assert(err, ErrorMatches, "test")
}

func (s *testTypeEtcSuite) TestMaxFloat(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Flen    int
		Decimal int
		Expect  float64
	}{
		{3, 2, 9.99},
		{5, 2, 999.99},
		{10, 1, 999999999.9},
		{5, 5, 0.99999},
	}

	for _, t := range tbl {
		f := GetMaxFloat(t.Flen, t.Decimal)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestRoundFloat(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  float64
		Expect float64
	}{
		{2.5, 3},
		{1.5, 2},
		{0.5, 1},
		{0.49999999999999997, 0},
		{0, 0},
		{-0.49999999999999997, 0},
		{-0.5, -1},
		{-2.5, -3},
		{-1.5, -2},
	}

	for _, t := range tbl {
		f := RoundFloat(t.Input)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestRound(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  float64
		Dec    int
		Expect float64
	}{
		{-1.23, 0, -1},
		{-1.58, 0, -2},
		{1.58, 0, 2},
		{1.298, 1, 1.3},
		{1.298, 0, 1},
		{23.298, -1, 20},
	}

	for _, t := range tbl {
		f := Round(t.Input, t.Dec)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestTruncate(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input   float64
		Flen    int
		Decimal int
		Expect  float64
		Err     error
	}{
		{100.114, 10, 2, 100.11, nil},
		{100.115, 10, 2, 100.12, nil},
		{100.1156, 10, 3, 100.116, nil},
		{100.1156, 3, 1, 99.9, ErrOverflow},
		{1.36, 10, 2, 1.36, nil},
	}
	for _, t := range tbl {
		f, err := TruncateFloat(t.Input, t.Flen, t.Decimal)
		c.Assert(f, Equals, t.Expect)
		c.Assert(terror.ErrorEqual(err, t.Err), IsTrue, Commentf("err %v", err))
	}
}

func (s *testTypeEtcSuite) TestIsTypeTemporal(c *C) {
	defer testleak.AfterTest(c)()
	res := IsTypeTemporal(allegrosql.TypeDuration)
	c.Assert(res, Equals, true)
	res = IsTypeTemporal(allegrosql.TypeDatetime)
	c.Assert(res, Equals, true)
	res = IsTypeTemporal(allegrosql.TypeTimestamp)
	c.Assert(res, Equals, true)
	res = IsTypeTemporal(allegrosql.TypeDate)
	c.Assert(res, Equals, true)
	res = IsTypeTemporal(allegrosql.TypeNewDate)
	c.Assert(res, Equals, true)
	res = IsTypeTemporal('t')
	c.Assert(res, Equals, false)
}

func (s *testTypeEtcSuite) TestIsBinaryStr(c *C) {
	defer testleak.AfterTest(c)()
	in := FieldType{
		Tp:      allegrosql.TypeBit,
		Flag:    allegrosql.UnsignedFlag,
		Flen:    1,
		Decimal: 0,
		Charset: charset.CharsetUTF8,
		DefCauslate: charset.DefCauslationUTF8,
	}
	in.DefCauslate = charset.DefCauslationUTF8
	res := IsBinaryStr(&in)
	c.Assert(res, Equals, false)

	in.DefCauslate = charset.DefCauslationBin
	res = IsBinaryStr(&in)
	c.Assert(res, Equals, false)

	in.Tp = allegrosql.TypeBlob
	res = IsBinaryStr(&in)
	c.Assert(res, Equals, true)
}

func (s *testTypeEtcSuite) TestIsNonBinaryStr(c *C) {
	defer testleak.AfterTest(c)()
	in := FieldType{
		Tp:      allegrosql.TypeBit,
		Flag:    allegrosql.UnsignedFlag,
		Flen:    1,
		Decimal: 0,
		Charset: charset.CharsetUTF8,
		DefCauslate: charset.DefCauslationUTF8,
	}

	in.DefCauslate = charset.DefCauslationBin
	res := IsBinaryStr(&in)
	c.Assert(res, Equals, false)

	in.DefCauslate = charset.DefCauslationUTF8
	res = IsBinaryStr(&in)
	c.Assert(res, Equals, false)

	in.Tp = allegrosql.TypeBlob
	res = IsBinaryStr(&in)
	c.Assert(res, Equals, false)
}

func (s *testTypeEtcSuite) TestIsTemporalWithDate(c *C) {
	defer testleak.AfterTest(c)()

	res := IsTemporalWithDate(allegrosql.TypeDatetime)
	c.Assert(res, Equals, true)

	res = IsTemporalWithDate(allegrosql.TypeDate)
	c.Assert(res, Equals, true)

	res = IsTemporalWithDate(allegrosql.TypeTimestamp)
	c.Assert(res, Equals, true)

	res = IsTemporalWithDate('t')
	c.Assert(res, Equals, false)
}

func (s *testTypeEtcSuite) TestIsTypePrefixable(c *C) {
	defer testleak.AfterTest(c)()

	res := IsTypePrefixable('t')
	c.Assert(res, Equals, false)

	res = IsTypePrefixable(allegrosql.TypeBlob)
	c.Assert(res, Equals, true)
}

func (s *testTypeEtcSuite) TestIsTypeFractionable(c *C) {
	defer testleak.AfterTest(c)()

	res := IsTypeFractionable(allegrosql.TypeDatetime)
	c.Assert(res, Equals, true)

	res = IsTypeFractionable(allegrosql.TypeDuration)
	c.Assert(res, Equals, true)

	res = IsTypeFractionable(allegrosql.TypeTimestamp)
	c.Assert(res, Equals, true)

	res = IsTypeFractionable('t')
	c.Assert(res, Equals, false)
}

func (s *testTypeEtcSuite) TestIsTypeNumeric(c *C) {
	defer testleak.AfterTest(c)()

	res := IsTypeNumeric(allegrosql.TypeBit)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeTiny)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeInt24)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeLong)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeLonglong)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeNewDecimal)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeUnspecified)
	c.Assert(res, Equals, false)

	res = IsTypeNumeric(allegrosql.TypeFloat)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeDouble)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric(allegrosql.TypeShort)
	c.Assert(res, Equals, true)

	res = IsTypeNumeric('t')
	c.Assert(res, Equals, false)
}
