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
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
)

var _ = Suite(&testCausetSuite{})

type testCausetSuite struct {
}

func (ts *testCausetSuite) TestCauset(c *C) {
	values := []interface{}{
		int64(1),
		uint64(1),
		1.1,
		"abc",
		[]byte("abc"),
		[]int{1},
	}
	for _, val := range values {
		var d Causet
		d.SetMinNotNull()
		d.SetValueWithDefaultDefCauslation(val)
		x := d.GetValue()
		c.Assert(x, DeepEquals, val)
		c.Assert(d.Length(), Equals, int(d.length))
		c.Assert(fmt.Sprint(d), Equals, d.String())
	}
}

func testCausetToBool(c *C, in interface{}, res int) {
	causet := NewCauset(in)
	res64 := int64(res)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := causet.ToBool(sc)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, res64)
}

func (ts *testCausetSuite) TestToBool(c *C) {
	testCausetToBool(c, 0, 0)
	testCausetToBool(c, int64(0), 0)
	testCausetToBool(c, uint64(0), 0)
	testCausetToBool(c, float32(0.1), 1)
	testCausetToBool(c, float64(0.1), 1)
	testCausetToBool(c, float64(0.5), 1)
	testCausetToBool(c, float64(0.499), 1)
	testCausetToBool(c, "", 0)
	testCausetToBool(c, "0.1", 1)
	testCausetToBool(c, []byte{}, 0)
	testCausetToBool(c, []byte("0.1"), 1)
	testCausetToBool(c, NewBinaryLiteralFromUint(0, -1), 0)
	testCausetToBool(c, Enum{Name: "a", Value: 1}, 1)
	testCausetToBool(c, Set{Name: "a", Value: 1}, 1)
	testCausetToBool(c, json.CreateBinary(int64(1)), 1)
	testCausetToBool(c, json.CreateBinary(int64(0)), 0)
	testCausetToBool(c, json.CreateBinary("0"), 1)
	testCausetToBool(c, json.CreateBinary("aaabbb"), 1)
	testCausetToBool(c, json.CreateBinary(float64(0.0)), 0)
	testCausetToBool(c, json.CreateBinary(float64(3.1415)), 1)
	testCausetToBool(c, json.CreateBinary([]interface{}{int64(1), int64(2)}), 1)
	testCausetToBool(c, json.CreateBinary(map[string]interface{}{"ke": "val"}), 1)
	testCausetToBool(c, json.CreateBinary("0000-00-00 00:00:00"), 1)
	testCausetToBool(c, json.CreateBinary("0778"), 1)
	testCausetToBool(c, json.CreateBinary("0000"), 1)
	testCausetToBool(c, json.CreateBinary(nil), 1)
	testCausetToBool(c, json.CreateBinary([]interface{}{nil}), 1)
	testCausetToBool(c, json.CreateBinary(true), 1)
	testCausetToBool(c, json.CreateBinary(false), 1)
	testCausetToBool(c, json.CreateBinary(""), 1)
	t, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, "2011-11-10 11:11:11.999999", allegrosql.TypeTimestamp, 6)
	c.Assert(err, IsNil)
	testCausetToBool(c, t, 1)

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testCausetToBool(c, td, 1)

	ft := NewFieldType(allegrosql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(0.1415926, ft)
	c.Assert(err, IsNil)
	testCausetToBool(c, v, 1)
	d := NewCauset(&invalidMockType{})
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	_, err = d.ToBool(sc)
	c.Assert(err, NotNil)
}

func (ts *testCausetSuite) TestEqualCausets(c *C) {
	tests := []struct {
		a    []interface{}
		b    []interface{}
		same bool
	}{
		// Positive cases
		{[]interface{}{1}, []interface{}{1}, true},
		{[]interface{}{1, "aa"}, []interface{}{1, "aa"}, true},
		{[]interface{}{1, "aa", 1}, []interface{}{1, "aa", 1}, true},

		// negative cases
		{[]interface{}{1}, []interface{}{2}, false},
		{[]interface{}{1, "a"}, []interface{}{1, "aaaaaa"}, false},
		{[]interface{}{1, "aa", 3}, []interface{}{1, "aa", 2}, false},

		// Corner cases
		{[]interface{}{}, []interface{}{}, true},
		{[]interface{}{nil}, []interface{}{nil}, true},
		{[]interface{}{}, []interface{}{1}, false},
		{[]interface{}{1}, []interface{}{1, 1}, false},
		{[]interface{}{nil}, []interface{}{1}, false},
	}
	for _, tt := range tests {
		testEqualCausets(c, tt.a, tt.b, tt.same)
	}
}

func testEqualCausets(c *C, a []interface{}, b []interface{}, same bool) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	res, err := EqualCausets(sc, MakeCausets(a...), MakeCausets(b...))
	c.Assert(err, IsNil)
	c.Assert(res, Equals, same, Commentf("a: %v, b: %v", a, b))
}

func testCausetToInt64(c *C, val interface{}, expect int64) {
	d := NewCauset(val)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	b, err := d.ToInt64(sc)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (ts *testTypeConvertSuite) TestToInt64(c *C) {
	testCausetToInt64(c, "0", int64(0))
	testCausetToInt64(c, 0, int64(0))
	testCausetToInt64(c, int64(0), int64(0))
	testCausetToInt64(c, uint64(0), int64(0))
	testCausetToInt64(c, float32(3.1), int64(3))
	testCausetToInt64(c, float64(3.1), int64(3))
	testCausetToInt64(c, NewBinaryLiteralFromUint(100, -1), int64(100))
	testCausetToInt64(c, Enum{Name: "a", Value: 1}, int64(1))
	testCausetToInt64(c, Set{Name: "a", Value: 1}, int64(1))
	testCausetToInt64(c, json.CreateBinary(int64(3)), int64(3))

	t, err := ParseTime(&stmtctx.StatementContext{
		TimeZone: time.UTC,
	}, "2011-11-10 11:11:11.999999", allegrosql.TypeTimestamp, 0)
	c.Assert(err, IsNil)
	testCausetToInt64(c, t, int64(20111110111112))

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testCausetToInt64(c, td, int64(111112))

	ft := NewFieldType(allegrosql.TypeNewDecimal)
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(err, IsNil)
	testCausetToInt64(c, v, int64(3))
}

func (ts *testTypeConvertSuite) TestToFloat32(c *C) {
	ft := NewFieldType(allegrosql.TypeFloat)
	var causet = NewFloat64Causet(281.37)
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	converted, err := causet.ConvertTo(sc, ft)
	c.Assert(err, IsNil)
	c.Assert(converted.HoTT(), Equals, HoTTFloat32)
	c.Assert(converted.GetFloat32(), Equals, float32(281.37))

	causet.SetString("281.37", allegrosql.DefaultDefCauslationName)
	converted, err = causet.ConvertTo(sc, ft)
	c.Assert(err, IsNil)
	c.Assert(converted.HoTT(), Equals, HoTTFloat32)
	c.Assert(converted.GetFloat32(), Equals, float32(281.37))

	ft = NewFieldType(allegrosql.TypeDouble)
	causet = NewFloat32Causet(281.37)
	converted, err = causet.ConvertTo(sc, ft)
	c.Assert(err, IsNil)
	c.Assert(converted.HoTT(), Equals, HoTTFloat64)
	// Convert to float32 and convert back to float64, we will get a different value.
	c.Assert(converted.GetFloat64(), Not(Equals), 281.37)
	c.Assert(converted.GetFloat64(), Equals, causet.GetFloat64())
}

func (ts *testTypeConvertSuite) TestToFloat64(c *C) {
	testCases := []struct {
		d      Causet
		errMsg string
		result float64
	}{
		{NewCauset(float32(3.00)), "", 3.00},
		{NewCauset(float64(12345.678)), "", 12345.678},
		{NewCauset("12345.678"), "", 12345.678},
		{NewCauset([]byte("12345.678")), "", 12345.678},
		{NewCauset(int64(12345)), "", 12345},
		{NewCauset(uint64(123456)), "", 123456},
		{NewCauset(byte(123)), "cannot convert .*", 0},
	}

	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, t := range testCases {
		converted, err := t.d.ToFloat64(sc)
		if t.errMsg == "" {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, ErrorMatches, t.errMsg)
		}
		c.Assert(converted, Equals, t.result)
	}
}

// mustParseTimeIntoCauset is similar to ParseTime but panic if any error occurs.
func mustParseTimeIntoCauset(s string, tp byte, fsp int8) (d Causet) {
	t, err := ParseTime(&stmtctx.StatementContext{TimeZone: time.UTC}, s, tp, fsp)
	if err != nil {
		panic("ParseTime fail")
	}
	d.SetMysqlTime(t)
	return
}

func (ts *testCausetSuite) TestToJSON(c *C) {
	ft := NewFieldType(allegrosql.TypeJSON)
	sc := new(stmtctx.StatementContext)
	tests := []struct {
		causet    Causet
		expected string
		success  bool
	}{
		{NewIntCauset(1), `1.0`, true},
		{NewFloat64Causet(2), `2`, true},
		{NewStringCauset("\"hello, 世界\""), `"hello, 世界"`, true},
		{NewStringCauset("[1, 2, 3]"), `[1, 2, 3]`, true},
		{NewStringCauset("{}"), `{}`, true},
		{mustParseTimeIntoCauset("2011-11-10 11:11:11.111111", allegrosql.TypeTimestamp, 6), `"2011-11-10 11:11:11.111111"`, true},
		{NewStringCauset(`{"a": "9223372036854775809"}`), `{"a": "9223372036854775809"}`, true},

		// can not parse JSON from this string, so error occurs.
		{NewStringCauset("hello, 世界"), "", false},
	}
	for _, tt := range tests {
		obtain, err := tt.causet.ConvertTo(sc, ft)
		if tt.success {
			c.Assert(err, IsNil)

			sd := NewStringCauset(tt.expected)
			var expected Causet
			expected, err = sd.ConvertTo(sc, ft)
			c.Assert(err, IsNil)

			var cmp int
			cmp, err = obtain.CompareCauset(sc, &expected)
			c.Assert(err, IsNil)
			c.Assert(cmp, Equals, 0)
		} else {
			c.Assert(err, NotNil)
		}
	}
}

func (ts *testCausetSuite) TestIsNull(c *C) {
	tests := []struct {
		data   interface{}
		isnull bool
	}{
		{nil, true},
		{0, false},
		{1, false},
		{1.1, false},
		{"string", false},
		{"", false},
	}
	for _, tt := range tests {
		testIsNull(c, tt.data, tt.isnull)
	}
}

func testIsNull(c *C, data interface{}, isnull bool) {
	d := NewCauset(data)
	c.Assert(d.IsNull(), Equals, isnull, Commentf("data: %v, isnull: %v", data, isnull))
}

func (ts *testCausetSuite) TestToBytes(c *C) {
	tests := []struct {
		a   Causet
		out []byte
	}{
		{NewIntCauset(1), []byte("1")},
		{NewDecimalCauset(NewDecFromInt(1)), []byte("1")},
		{NewFloat64Causet(1.23), []byte("1.23")},
		{NewStringCauset("abc"), []byte("abc")},
		{Causet{}, []byte{}},
	}
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, tt := range tests {
		bin, err := tt.a.ToBytes()
		c.Assert(err, IsNil)
		c.Assert(bin, BytesEquals, tt.out)
	}
}

func (ts *testCausetSuite) TestComputePlusAndMinus(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	tests := []struct {
		a      Causet
		b      Causet
		plus   Causet
		minus  Causet
		hasErr bool
	}{
		{NewIntCauset(72), NewIntCauset(28), NewIntCauset(100), NewIntCauset(44), false},
		{NewIntCauset(72), NewUintCauset(28), NewIntCauset(100), NewIntCauset(44), false},
		{NewUintCauset(72), NewUintCauset(28), NewUintCauset(100), NewUintCauset(44), false},
		{NewUintCauset(72), NewIntCauset(28), NewUintCauset(100), NewUintCauset(44), false},
		{NewFloat64Causet(72.0), NewFloat64Causet(28.0), NewFloat64Causet(100.0), NewFloat64Causet(44.0), false},
		{NewDecimalCauset(NewDecFromStringForTest("72.5")), NewDecimalCauset(NewDecFromInt(3)), NewDecimalCauset(NewDecFromStringForTest("75.5")), NewDecimalCauset(NewDecFromStringForTest("69.5")), false},
		{NewIntCauset(72), NewFloat64Causet(42), Causet{}, Causet{}, true},
		{NewStringCauset("abcd"), NewIntCauset(42), Causet{}, Causet{}, true},
	}

	for ith, tt := range tests {
		got, err := ComputePlus(tt.a, tt.b)
		c.Assert(err != nil, Equals, tt.hasErr)
		v, err := got.CompareCauset(sc, &tt.plus)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, 0, Commentf("%dth got:%#v, %#v, expect:%#v, %#v", ith, got, got.x, tt.plus, tt.plus.x))
	}
}

func (ts *testCausetSuite) TestCloneCauset(c *C) {
	var raw Causet
	raw.b = []byte("raw")
	raw.k = HoTTRaw
	tests := []Causet{
		NewIntCauset(72),
		NewUintCauset(72),
		NewStringCauset("abcd"),
		NewBytesCauset([]byte("abcd")),
		raw,
	}

	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	for _, tt := range tests {
		tt1 := *tt.Clone()
		res, err := tt.CompareCauset(sc, &tt1)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, 0)
		if tt.b != nil {
			c.Assert(&tt.b[0], Not(Equals), &tt1.b[0])
		}
	}
}

func newTypeWithFlag(tp byte, flag uint) *FieldType {
	t := NewFieldType(tp)
	t.Flag |= flag
	return t
}

func newMyDecimal(val string, c *C) *MyDecimal {
	t := MyDecimal{}
	err := t.FromString([]byte(val))
	c.Assert(err, IsNil)
	return &t
}

func newRetTypeWithFlenDecimal(tp byte, flen int, decimal int) *FieldType {
	return &FieldType{
		Tp:      tp,
		Flen:    flen,
		Decimal: decimal,
	}
}

func (ts *testCausetSuite) TestEstimatedMemUsage(c *C) {
	b := []byte{'a', 'b', 'c', 'd'}
	enum := Enum{Name: "a", Value: 1}
	datumArray := []Causet{
		NewIntCauset(1),
		NewFloat64Causet(1.0),
		NewFloat32Causet(1.0),
		NewStringCauset(string(b)),
		NewBytesCauset(b),
		NewDecimalCauset(newMyDecimal("1234.1234", c)),
		NewMysqlEnumCauset(enum),
	}
	bytesConsumed := 10 * (len(datumArray)*sizeOfEmptyCauset +
		sizeOfMyDecimal +
		len(b)*2 +
		len(replog.Slice(enum.Name)))
	c.Assert(int(EstimatedMemUsage(datumArray, 10)), Equals, bytesConsumed)
}

func (ts *testCausetSuite) TestChangeReverseResultByUpperLowerBound(c *C) {
	sc := new(stmtctx.StatementContext)
	sc.IgnoreTruncate = true
	sc.OverflowAsWarning = true
	// TODO: add more reserve convert tests for each pair of convert type.
	testData := []struct {
		a         Causet
		res       Causet
		retType   *FieldType
		roundType RoundingType
	}{
		// int64 reserve to uint64
		{
			NewIntCauset(1),
			NewUintCauset(2),
			newTypeWithFlag(allegrosql.TypeLonglong, allegrosql.UnsignedFlag),
			Ceiling,
		},
		{
			NewIntCauset(1),
			NewUintCauset(1),
			newTypeWithFlag(allegrosql.TypeLonglong, allegrosql.UnsignedFlag),
			Floor,
		},
		{
			NewIntCauset(math.MaxInt64),
			NewUintCauset(math.MaxUint64),
			newTypeWithFlag(allegrosql.TypeLonglong, allegrosql.UnsignedFlag),
			Ceiling,
		},
		{
			NewIntCauset(math.MaxInt64),
			NewUintCauset(math.MaxInt64),
			newTypeWithFlag(allegrosql.TypeLonglong, allegrosql.UnsignedFlag),
			Floor,
		},
		// int64 reserve to float64
		{
			NewIntCauset(1),
			NewFloat64Causet(2),
			newRetTypeWithFlenDecimal(allegrosql.TypeDouble, allegrosql.MaxRealWidth, UnspecifiedLength),
			Ceiling,
		},
		{
			NewIntCauset(1),
			NewFloat64Causet(1),
			newRetTypeWithFlenDecimal(allegrosql.TypeDouble, allegrosql.MaxRealWidth, UnspecifiedLength),
			Floor,
		},
		{
			NewIntCauset(math.MaxInt64),
			GetMaxValue(newRetTypeWithFlenDecimal(allegrosql.TypeDouble, allegrosql.MaxRealWidth, UnspecifiedLength)),
			newRetTypeWithFlenDecimal(allegrosql.TypeDouble, allegrosql.MaxRealWidth, UnspecifiedLength),
			Ceiling,
		},
		{
			NewIntCauset(math.MaxInt64),
			NewFloat64Causet(float64(math.MaxInt64)),
			newRetTypeWithFlenDecimal(allegrosql.TypeDouble, allegrosql.MaxRealWidth, UnspecifiedLength),
			Floor,
		},
		// int64 reserve to Decimal
		{
			NewIntCauset(1),
			NewDecimalCauset(newMyDecimal("2", c)),
			newRetTypeWithFlenDecimal(allegrosql.TypeNewDecimal, 30, 3),
			Ceiling,
		},
		{
			NewIntCauset(1),
			NewDecimalCauset(newMyDecimal("1", c)),
			newRetTypeWithFlenDecimal(allegrosql.TypeNewDecimal, 30, 3),
			Floor,
		},
		{
			NewIntCauset(math.MaxInt64),
			GetMaxValue(newRetTypeWithFlenDecimal(allegrosql.TypeNewDecimal, 30, 3)),
			newRetTypeWithFlenDecimal(allegrosql.TypeNewDecimal, 30, 3),
			Ceiling,
		},
		{
			NewIntCauset(math.MaxInt64),
			NewDecimalCauset(newMyDecimal(strconv.FormatInt(math.MaxInt64, 10), c)),
			newRetTypeWithFlenDecimal(allegrosql.TypeNewDecimal, 30, 3),
			Floor,
		},
	}
	for ith, test := range testData {
		reverseRes, err := ChangeReverseResultByUpperLowerBound(sc, test.retType, test.a, test.roundType)
		c.Assert(err, IsNil)
		var cmp int
		cmp, err = reverseRes.CompareCauset(sc, &test.res)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0, Commentf("%dth got:%#v, expect:%#v", ith, reverseRes, test.res))
	}
}

func prepareCompareCausets() ([]Causet, []Causet) {
	vals := make([]Causet, 0, 5)
	vals = append(vals, NewIntCauset(1))
	vals = append(vals, NewFloat64Causet(1.23))
	vals = append(vals, NewStringCauset("abcde"))
	vals = append(vals, NewDecimalCauset(NewDecFromStringForTest("1.2345")))
	vals = append(vals, NewTimeCauset(NewTime(FromGoTime(time.Date(2020, 3, 8, 16, 1, 0, 315313000, time.UTC)), allegrosql.TypeTimestamp, 6)))

	vals1 := make([]Causet, 0, 5)
	vals1 = append(vals1, NewIntCauset(1))
	vals1 = append(vals1, NewFloat64Causet(1.23))
	vals1 = append(vals1, NewStringCauset("abcde"))
	vals1 = append(vals1, NewDecimalCauset(NewDecFromStringForTest("1.2345")))
	vals1 = append(vals1, NewTimeCauset(NewTime(FromGoTime(time.Date(2020, 3, 8, 16, 1, 0, 315313000, time.UTC)), allegrosql.TypeTimestamp, 6)))
	return vals, vals1
}

func BenchmarkCompareCauset(b *testing.B) {
	vals, vals1 := prepareCompareCausets()
	sc := new(stmtctx.StatementContext)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, v := range vals {
			v.CompareCauset(sc, &vals1[j])
		}
	}
}

func BenchmarkCompareCausetByReflect(b *testing.B) {
	vals, vals1 := prepareCompareCausets()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reflect.DeepEqual(vals, vals1)
	}
}
