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

package ranger

import (
	"fmt"
	"math"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// Range represents a range generated in physical plan building phase.
type Range struct {
	LowVal  []types.Causet
	HighVal []types.Causet

	LowExclude  bool // Low value is exclusive.
	HighExclude bool // High value is exclusive.
}

// Clone clones a Range.
func (ran *Range) Clone() *Range {
	newRange := &Range{
		LowVal:      make([]types.Causet, 0, len(ran.LowVal)),
		HighVal:     make([]types.Causet, 0, len(ran.HighVal)),
		LowExclude:  ran.LowExclude,
		HighExclude: ran.HighExclude,
	}
	for i, length := 0, len(ran.LowVal); i < length; i++ {
		newRange.LowVal = append(newRange.LowVal, ran.LowVal[i])
	}
	for i, length := 0, len(ran.HighVal); i < length; i++ {
		newRange.HighVal = append(newRange.HighVal, ran.HighVal[i])
	}
	return newRange
}

// IsPoint returns if the range is a point.
func (ran *Range) IsPoint(sc *stmtctx.StatementContext) bool {
	if len(ran.LowVal) != len(ran.HighVal) {
		return false
	}
	for i := range ran.LowVal {
		a := ran.LowVal[i]
		b := ran.HighVal[i]
		if a.HoTT() == types.HoTTMinNotNull || b.HoTT() == types.HoTTMaxValue {
			return false
		}
		cmp, err := a.CompareCauset(sc, &b)
		if err != nil {
			return false
		}
		if cmp != 0 {
			return false
		}

		if a.IsNull() {
			return false
		}
	}
	return !ran.LowExclude && !ran.HighExclude
}

// IsPointNullable returns if the range is a point.
func (ran *Range) IsPointNullable(sc *stmtctx.StatementContext) bool {
	if len(ran.LowVal) != len(ran.HighVal) {
		return false
	}
	for i := range ran.LowVal {
		a := ran.LowVal[i]
		b := ran.HighVal[i]
		if a.HoTT() == types.HoTTMinNotNull || b.HoTT() == types.HoTTMaxValue {
			return false
		}
		cmp, err := a.CompareCauset(sc, &b)
		if err != nil {
			return false
		}
		if cmp != 0 {
			return false
		}

		if a.IsNull() {
			if !b.IsNull() {
				return false
			}
		}
	}
	return !ran.LowExclude && !ran.HighExclude
}

//IsFullRange check if the range is full scan range
func (ran *Range) IsFullRange() bool {
	if len(ran.LowVal) != len(ran.HighVal) {
		return false
	}
	for i := range ran.LowVal {
		lowValRawString := formatCauset(ran.LowVal[i], true)
		highValRawString := formatCauset(ran.HighVal[i], false)
		if ("-inf" != lowValRawString && "NULL" != lowValRawString) ||
			("+inf" != highValRawString && "NULL" != highValRawString) ||
			("NULL" == lowValRawString && "NULL" == highValRawString) {
			return false
		}
	}
	return true
}

// String implements the Stringer interface.
func (ran *Range) String() string {
	lowStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.LowVal {
		lowStrs = append(lowStrs, formatCauset(d, true))
	}
	highStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.HighVal {
		highStrs = append(highStrs, formatCauset(d, false))
	}
	l, r := "[", "]"
	if ran.LowExclude {
		l = "("
	}
	if ran.HighExclude {
		r = ")"
	}
	return l + strings.Join(lowStrs, " ") + "," + strings.Join(highStrs, " ") + r
}

// Encode encodes the range to its encoded value.
func (ran *Range) Encode(sc *stmtctx.StatementContext, lowBuffer, highBuffer []byte) ([]byte, []byte, error) {
	var err error
	lowBuffer, err = codec.EncodeKey(sc, lowBuffer[:0], ran.LowVal...)
	if err != nil {
		return nil, nil, err
	}
	if ran.LowExclude {
		lowBuffer = ekv.Key(lowBuffer).PrefixNext()
	}
	highBuffer, err = codec.EncodeKey(sc, highBuffer[:0], ran.HighVal...)
	if err != nil {
		return nil, nil, err
	}
	if !ran.HighExclude {
		highBuffer = ekv.Key(highBuffer).PrefixNext()
	}
	return lowBuffer, highBuffer, nil
}

// PrefixEqualLen tells you how long the prefix of the range is a point.
// e.g. If this range is (1 2 3, 1 2 +inf), then the return value is 2.
func (ran *Range) PrefixEqualLen(sc *stmtctx.StatementContext) (int, error) {
	// Here, len(ran.LowVal) always equal to len(ran.HighVal)
	for i := 0; i < len(ran.LowVal); i++ {
		cmp, err := ran.LowVal[i].CompareCauset(sc, &ran.HighVal[i])
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp != 0 {
			return i, nil
		}
	}
	return len(ran.LowVal), nil
}

func formatCauset(d types.Causet, isLeftSide bool) string {
	switch d.HoTT() {
	case types.HoTTNull:
		return "NULL"
	case types.HoTTMinNotNull:
		return "-inf"
	case types.HoTTMaxValue:
		return "+inf"
	case types.HoTTInt64:
		switch d.GetInt64() {
		case math.MinInt64:
			if isLeftSide {
				return "-inf"
			}
		case math.MaxInt64:
			if !isLeftSide {
				return "+inf"
			}
		}
	case types.HoTTUint64:
		if d.GetUint64() == math.MaxUint64 && !isLeftSide {
			return "+inf"
		}
	case types.HoTTString, types.HoTTBytes, types.HoTTMysqlEnum, types.HoTTMysqlSet,
		types.HoTTMysqlJSON, types.HoTTBinaryLiteral, types.HoTTMysqlBit:
		return fmt.Sprintf("\"%v\"", d.GetValue())
	}
	return fmt.Sprintf("%v", d.GetValue())
}
