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

package statistics

import (
	"math"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
)

const eps = 1e-9

func getDecimal(value float64) *types.MyDecimal {
	dec := &types.MyDecimal{}
	dec.FromFloat64(value)
	return dec
}

func getDuration(value string) types.Duration {
	dur, _ := types.ParseDuration(nil, value, 0)
	return dur
}

func getTime(year, month, day int, timeType byte) types.Time {
	ret := types.NewTime(types.FromDate(year, month, day, 0, 0, 0, 0), timeType, types.DefaultFsp)
	return ret
}

func getTimeStamp(hour, min, sec int, timeType byte) types.Time {
	ret := types.NewTime(types.FromDate(2020, 1, 1, hour, min, sec, 0), timeType, 0)
	return ret
}

func getBinaryLiteral(value string) types.BinaryLiteral {
	b, _ := types.ParseBitStr(value)
	return b
}

func getUnsignedFieldType() *types.FieldType {
	tp := types.NewFieldType(allegrosql.TypeLonglong)
	tp.Flag |= allegrosql.UnsignedFlag
	return tp
}

func (s *testStatisticsSuite) TestCalcFraction(c *C) {
	tests := []struct {
		lower    types.Causet
		upper    types.Causet
		value    types.Causet
		fraction float64
		tp       *types.FieldType
	}{
		{
			lower:    types.NewIntCauset(0),
			upper:    types.NewIntCauset(4),
			value:    types.NewIntCauset(1),
			fraction: 0.25,
			tp:       types.NewFieldType(allegrosql.TypeLonglong),
		},
		{
			lower:    types.NewIntCauset(0),
			upper:    types.NewIntCauset(4),
			value:    types.NewIntCauset(4),
			fraction: 1,
			tp:       types.NewFieldType(allegrosql.TypeLonglong),
		},
		{
			lower:    types.NewIntCauset(0),
			upper:    types.NewIntCauset(4),
			value:    types.NewIntCauset(-1),
			fraction: 0,
			tp:       types.NewFieldType(allegrosql.TypeLonglong),
		},
		{
			lower:    types.NewUintCauset(0),
			upper:    types.NewUintCauset(4),
			value:    types.NewUintCauset(1),
			fraction: 0.25,
			tp:       getUnsignedFieldType(),
		},
		{
			lower:    types.NewFloat64Causet(0),
			upper:    types.NewFloat64Causet(4),
			value:    types.NewFloat64Causet(1),
			fraction: 0.25,
			tp:       types.NewFieldType(allegrosql.TypeDouble),
		},
		{
			lower:    types.NewFloat32Causet(0),
			upper:    types.NewFloat32Causet(4),
			value:    types.NewFloat32Causet(1),
			fraction: 0.25,
			tp:       types.NewFieldType(allegrosql.TypeFloat),
		},
		{
			lower:    types.NewDecimalCauset(getDecimal(0)),
			upper:    types.NewDecimalCauset(getDecimal(4)),
			value:    types.NewDecimalCauset(getDecimal(1)),
			fraction: 0.25,
			tp:       types.NewFieldType(allegrosql.TypeNewDecimal),
		},
		{
			lower:    types.NewMysqlBitCauset(getBinaryLiteral("0b0")),
			upper:    types.NewMysqlBitCauset(getBinaryLiteral("0b100")),
			value:    types.NewMysqlBitCauset(getBinaryLiteral("0b1")),
			fraction: 0.5,
			tp:       types.NewFieldType(allegrosql.TypeBit),
		},
		{
			lower:    types.NewDurationCauset(getDuration("0:00:00")),
			upper:    types.NewDurationCauset(getDuration("4:00:00")),
			value:    types.NewDurationCauset(getDuration("1:00:00")),
			fraction: 0.25,
			tp:       types.NewFieldType(allegrosql.TypeDuration),
		},
		{
			lower:    types.NewTimeCauset(getTime(2020, 1, 1, allegrosql.TypeTimestamp)),
			upper:    types.NewTimeCauset(getTime(2020, 4, 1, allegrosql.TypeTimestamp)),
			value:    types.NewTimeCauset(getTime(2020, 2, 1, allegrosql.TypeTimestamp)),
			fraction: 0.34444444444444444,
			tp:       types.NewFieldType(allegrosql.TypeTimestamp),
		},
		{
			lower:    types.NewTimeCauset(getTime(2020, 1, 1, allegrosql.TypeDatetime)),
			upper:    types.NewTimeCauset(getTime(2020, 4, 1, allegrosql.TypeDatetime)),
			value:    types.NewTimeCauset(getTime(2020, 2, 1, allegrosql.TypeDatetime)),
			fraction: 0.34444444444444444,
			tp:       types.NewFieldType(allegrosql.TypeDatetime),
		},
		{
			lower:    types.NewTimeCauset(getTime(2020, 1, 1, allegrosql.TypeDate)),
			upper:    types.NewTimeCauset(getTime(2020, 4, 1, allegrosql.TypeDate)),
			value:    types.NewTimeCauset(getTime(2020, 2, 1, allegrosql.TypeDate)),
			fraction: 0.34444444444444444,
			tp:       types.NewFieldType(allegrosql.TypeDate),
		},
		{
			lower:    types.NewStringCauset("aasad"),
			upper:    types.NewStringCauset("addad"),
			value:    types.NewStringCauset("abfsd"),
			fraction: 0.32280253984063745,
			tp:       types.NewFieldType(allegrosql.TypeString),
		},
		{
			lower:    types.NewBytesCauset([]byte("aasad")),
			upper:    types.NewBytesCauset([]byte("asdff")),
			value:    types.NewBytesCauset([]byte("abfsd")),
			fraction: 0.0529216802217269,
			tp:       types.NewFieldType(allegrosql.TypeBlob),
		},
	}
	for _, test := range tests {
		hg := NewHistogram(0, 0, 0, 0, test.tp, 1, 0)
		hg.AppendBucket(&test.lower, &test.upper, 0, 0)
		hg.PreCalculateScalar()
		fraction := hg.calcFraction(0, &test.value)
		c.Check(math.Abs(fraction-test.fraction) < eps, IsTrue)
	}
}

func (s *testStatisticsSuite) TestEnumRangeValues(c *C) {
	tests := []struct {
		low         types.Causet
		high        types.Causet
		lowExclude  bool
		highExclude bool
		res         string
	}{
		{
			low:         types.NewIntCauset(0),
			high:        types.NewIntCauset(5),
			lowExclude:  false,
			highExclude: true,
			res:         "(0, 1, 2, 3, 4)",
		},
		{
			low:         types.NewIntCauset(math.MinInt64),
			high:        types.NewIntCauset(math.MaxInt64),
			lowExclude:  false,
			highExclude: false,
			res:         "",
		},
		{
			low:         types.NewUintCauset(0),
			high:        types.NewUintCauset(5),
			lowExclude:  false,
			highExclude: true,
			res:         "(0, 1, 2, 3, 4)",
		},
		{
			low:         types.NewDurationCauset(getDuration("0:00:00")),
			high:        types.NewDurationCauset(getDuration("0:00:05")),
			lowExclude:  false,
			highExclude: true,
			res:         "(00:00:00, 00:00:01, 00:00:02, 00:00:03, 00:00:04)",
		},
		{
			low:         types.NewDurationCauset(getDuration("0:00:00")),
			high:        types.NewDurationCauset(getDuration("0:00:05")),
			lowExclude:  false,
			highExclude: true,
			res:         "(00:00:00, 00:00:01, 00:00:02, 00:00:03, 00:00:04)",
		},
		{
			low:         types.NewTimeCauset(getTime(2020, 1, 1, allegrosql.TypeDate)),
			high:        types.NewTimeCauset(getTime(2020, 1, 5, allegrosql.TypeDate)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04)",
		},
		{
			low:         types.NewTimeCauset(getTimeStamp(0, 0, 0, allegrosql.TypeTimestamp)),
			high:        types.NewTimeCauset(getTimeStamp(0, 0, 5, allegrosql.TypeTimestamp)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2020-01-01 00:00:00, 2020-01-01 00:00:01, 2020-01-01 00:00:02, 2020-01-01 00:00:03, 2020-01-01 00:00:04)",
		},
		{
			low:         types.NewTimeCauset(getTimeStamp(0, 0, 0, allegrosql.TypeDatetime)),
			high:        types.NewTimeCauset(getTimeStamp(0, 0, 5, allegrosql.TypeDatetime)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2020-01-01 00:00:00, 2020-01-01 00:00:01, 2020-01-01 00:00:02, 2020-01-01 00:00:03, 2020-01-01 00:00:04)",
		},
		// fix issue 11610
		{
			low:         types.NewIntCauset(math.MinInt64),
			high:        types.NewIntCauset(0),
			lowExclude:  false,
			highExclude: false,
			res:         "",
		},
	}
	for _, t := range tests {
		vals := enumRangeValues(t.low, t.high, t.lowExclude, t.highExclude)
		str, err := types.CausetsToString(vals, true)
		c.Assert(err, IsNil)
		c.Assert(str, Equals, t.res)
	}
}
