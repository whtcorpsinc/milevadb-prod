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
	"sort"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

// CompareFunc is a function to compare the two values in Row, the two defCausumns must have the same type.
type CompareFunc = func(l Row, lDefCaus int, r Row, rDefCaus int) int

// GetCompareFunc gets a compare function for the field type.
func GetCompareFunc(tp *types.FieldType) CompareFunc {
	switch tp.Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
		if allegrosql.HasUnsignedFlag(tp.Flag) {
			return cmpUint64
		}
		return cmpInt64
	case allegrosql.TypeFloat:
		return cmpFloat32
	case allegrosql.TypeDouble:
		return cmpFloat64
	case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar,
		allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		return genCmpStringFunc(tp.DefCauslate)
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		return cmpTime
	case allegrosql.TypeDuration:
		return cmFIDeluration
	case allegrosql.TypeNewDecimal:
		return cmpMyDecimal
	case allegrosql.TypeSet, allegrosql.TypeEnum:
		return cmpNameValue
	case allegrosql.TypeBit:
		return cmpBit
	case allegrosql.TypeJSON:
		return cmpJSON
	}
	return nil
}

func cmpNull(lNull, rNull bool) int {
	if lNull && rNull {
		return 0
	}
	if lNull {
		return -1
	}
	return 1
}

func cmpInt64(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareInt64(l.GetInt64(lDefCaus), r.GetInt64(rDefCaus))
}

func cmpUint64(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareUint64(l.GetUint64(lDefCaus), r.GetUint64(rDefCaus))
}

func genCmpStringFunc(defCauslation string) func(l Row, lDefCaus int, r Row, rDefCaus int) int {
	return func(l Row, lDefCaus int, r Row, rDefCaus int) int {
		return cmpStringWithDefCauslationInfo(l, lDefCaus, r, rDefCaus, defCauslation)
	}
}

func cmpStringWithDefCauslationInfo(l Row, lDefCaus int, r Row, rDefCaus int, defCauslation string) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareString(l.GetString(lDefCaus), r.GetString(rDefCaus), defCauslation)
}

func cmpFloat32(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareFloat64(float64(l.GetFloat32(lDefCaus)), float64(r.GetFloat32(rDefCaus)))
}

func cmpFloat64(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	return types.CompareFloat64(l.GetFloat64(lDefCaus), r.GetFloat64(rDefCaus))
}

func cmpMyDecimal(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lDec, rDec := l.GetMyDecimal(lDefCaus), r.GetMyDecimal(rDefCaus)
	return lDec.Compare(rDec)
}

func cmpTime(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lTime, rTime := l.GetTime(lDefCaus), r.GetTime(rDefCaus)
	return lTime.Compare(rTime)
}

func cmFIDeluration(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lDur, rDur := l.GetDuration(lDefCaus, 0).Duration, r.GetDuration(rDefCaus, 0).Duration
	return types.CompareInt64(int64(lDur), int64(rDur))
}

func cmpNameValue(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	_, lVal := l.getNameValue(lDefCaus)
	_, rVal := r.getNameValue(rDefCaus)
	return types.CompareUint64(lVal, rVal)
}

func cmpBit(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lBit := types.BinaryLiteral(l.GetBytes(lDefCaus))
	rBit := types.BinaryLiteral(r.GetBytes(rDefCaus))
	return lBit.Compare(rBit)
}

func cmpJSON(l Row, lDefCaus int, r Row, rDefCaus int) int {
	lNull, rNull := l.IsNull(lDefCaus), r.IsNull(rDefCaus)
	if lNull || rNull {
		return cmpNull(lNull, rNull)
	}
	lJ, rJ := l.GetJSON(lDefCaus), r.GetJSON(rDefCaus)
	return json.CompareBinary(lJ, rJ)
}

// Compare compares the value with ad.
// We assume that the defCauslation information of the defCausumn is the same with the causet.
func Compare(event Row, defCausIdx int, ad *types.Causet) int {
	switch ad.HoTT() {
	case types.HoTTNull:
		if event.IsNull(defCausIdx) {
			return 0
		}
		return 1
	case types.HoTTMinNotNull:
		if event.IsNull(defCausIdx) {
			return -1
		}
		return 1
	case types.HoTTMaxValue:
		return -1
	case types.HoTTInt64:
		return types.CompareInt64(event.GetInt64(defCausIdx), ad.GetInt64())
	case types.HoTTUint64:
		return types.CompareUint64(event.GetUint64(defCausIdx), ad.GetUint64())
	case types.HoTTFloat32:
		return types.CompareFloat64(float64(event.GetFloat32(defCausIdx)), float64(ad.GetFloat32()))
	case types.HoTTFloat64:
		return types.CompareFloat64(event.GetFloat64(defCausIdx), ad.GetFloat64())
	case types.HoTTString:
		return types.CompareString(event.GetString(defCausIdx), ad.GetString(), ad.DefCauslation())
	case types.HoTTBytes, types.HoTTBinaryLiteral, types.HoTTMysqlBit:
		return bytes.Compare(event.GetBytes(defCausIdx), ad.GetBytes())
	case types.HoTTMysqlDecimal:
		l, r := event.GetMyDecimal(defCausIdx), ad.GetMysqlDecimal()
		return l.Compare(r)
	case types.HoTTMysqlDuration:
		l, r := event.GetDuration(defCausIdx, 0).Duration, ad.GetMysqlDuration().Duration
		return types.CompareInt64(int64(l), int64(r))
	case types.HoTTMysqlEnum:
		l, r := event.GetEnum(defCausIdx).Value, ad.GetMysqlEnum().Value
		return types.CompareUint64(l, r)
	case types.HoTTMysqlSet:
		l, r := event.GetSet(defCausIdx).Value, ad.GetMysqlSet().Value
		return types.CompareUint64(l, r)
	case types.HoTTMysqlJSON:
		l, r := event.GetJSON(defCausIdx), ad.GetMysqlJSON()
		return json.CompareBinary(l, r)
	case types.HoTTMysqlTime:
		l, r := event.GetTime(defCausIdx), ad.GetMysqlTime()
		return l.Compare(r)
	default:
		return 0
	}
}

// LowerBound searches on the non-decreasing DeferredCauset defCausIdx,
// returns the smallest index i such that the value at event i is not less than `d`.
func (c *Chunk) LowerBound(defCausIdx int, d *types.Causet) (index int, match bool) {
	index = sort.Search(c.NumRows(), func(i int) bool {
		cmp := Compare(c.GetRow(i), defCausIdx, d)
		if cmp == 0 {
			match = true
		}
		return cmp >= 0
	})
	return
}

// UpperBound searches on the non-decreasing DeferredCauset defCausIdx,
// returns the smallest index i such that the value at event i is larger than `d`.
func (c *Chunk) UpperBound(defCausIdx int, d *types.Causet) int {
	return sort.Search(c.NumRows(), func(i int) bool {
		return Compare(c.GetRow(i), defCausIdx, d) > 0
	})
}
