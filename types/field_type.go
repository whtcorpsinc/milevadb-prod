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
	"strconv"

	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	ast "github.com/whtcorpsinc/BerolinaSQL/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	utilMath "github.com/whtcorpsinc/milevadb/soliton/math"
)

// UnspecifiedLength is unspecified length.
const UnspecifiedLength = -1

// ErrorLength is error length for blob or text.
const ErrorLength = 0

// FieldType records field type information.
type FieldType = ast.FieldType

// NewFieldType returns a FieldType,
// with a type and other information about field type.
func NewFieldType(tp byte) *FieldType {
	ft := &FieldType{
		Tp:      tp,
		Flen:    UnspecifiedLength,
		Decimal: UnspecifiedLength,
	}
	if tp != allegrosql.TypeVarchar && tp != allegrosql.TypeVarString && tp != allegrosql.TypeString {
		ft.DefCauslate = charset.DefCauslationBin
	} else {
		ft.DefCauslate = allegrosql.DefaultDefCauslationName
	}
	// TODO: use DefaultCharsetForType to set charset and collate
	return ft
}

// NewFieldTypeWithDefCauslation returns a FieldType,
// with a type and other information about field type.
func NewFieldTypeWithDefCauslation(tp byte, collation string, length int) *FieldType {
	return &FieldType{
		Tp:      tp,
		Flen:    length,
		Decimal: UnspecifiedLength,
		DefCauslate: collation,
	}
}

// AggFieldType aggregates field types for a multi-argument function like `IF`, `IFNULL`, `COALESCE`
// whose return type is determined by the arguments' FieldTypes.
// Aggregation is performed by MergeFieldType function.
func AggFieldType(tps []*FieldType) *FieldType {
	var currType FieldType
	for i, t := range tps {
		if i == 0 && currType.Tp == allegrosql.TypeUnspecified {
			currType = *t
			continue
		}
		mtp := MergeFieldType(currType.Tp, t.Tp)
		currType.Tp = mtp
		currType.Flag = mergeTypeFlag(currType.Flag, t.Flag)
	}

	return &currType
}

// AggregateEvalType aggregates arguments' EvalType of a multi-argument function.
func AggregateEvalType(fts []*FieldType, flag *uint) EvalType {
	var (
		aggregatedEvalType = ETString
		unsigned           bool
		gotFirst           bool
		gotBinString       bool
	)
	lft := fts[0]
	for _, ft := range fts {
		if ft.Tp == allegrosql.TypeNull {
			continue
		}
		et := ft.EvalType()
		rft := ft
		if (IsTypeBlob(ft.Tp) || IsTypeVarchar(ft.Tp) || IsTypeChar(ft.Tp)) && allegrosql.HasBinaryFlag(ft.Flag) {
			gotBinString = true
		}
		if !gotFirst {
			gotFirst = true
			aggregatedEvalType = et
			unsigned = allegrosql.HasUnsignedFlag(ft.Flag)
		} else {
			aggregatedEvalType = mergeEvalType(aggregatedEvalType, et, lft, rft, unsigned, allegrosql.HasUnsignedFlag(ft.Flag))
			unsigned = unsigned && allegrosql.HasUnsignedFlag(ft.Flag)
		}
		lft = rft
	}
	setTypeFlag(flag, allegrosql.UnsignedFlag, unsigned)
	setTypeFlag(flag, allegrosql.BinaryFlag, !aggregatedEvalType.IsStringHoTT() || gotBinString)
	return aggregatedEvalType
}

func mergeEvalType(lhs, rhs EvalType, lft, rft *FieldType, isLHSUnsigned, isRHSUnsigned bool) EvalType {
	if lft.Tp == allegrosql.TypeUnspecified || rft.Tp == allegrosql.TypeUnspecified {
		if lft.Tp == rft.Tp {
			return ETString
		}
		if lft.Tp == allegrosql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringHoTT() || rhs.IsStringHoTT() {
		return ETString
	} else if lhs == ETReal || rhs == ETReal {
		return ETReal
	} else if lhs == ETDecimal || rhs == ETDecimal || isLHSUnsigned != isRHSUnsigned {
		return ETDecimal
	}
	return ETInt
}

func setTypeFlag(flag *uint, flagItem uint, on bool) {
	if on {
		*flag |= flagItem
	} else {
		*flag &= ^flagItem
	}
}

// DefaultParamTypeForValue returns the default FieldType for the parameterized value.
func DefaultParamTypeForValue(value interface{}, tp *FieldType) {
	switch value.(type) {
	case nil:
		tp.Tp = allegrosql.TypeVarString
		tp.Flen = UnspecifiedLength
		tp.Decimal = UnspecifiedLength
	default:
		DefaultTypeForValue(value, tp, allegrosql.DefaultCharset, allegrosql.DefaultDefCauslationName)
		if hasVariantFieldLength(tp) {
			tp.Flen = UnspecifiedLength
		}
		if tp.Tp == allegrosql.TypeUnspecified {
			tp.Tp = allegrosql.TypeVarString
		}
	}
}

func hasVariantFieldLength(tp *FieldType) bool {
	switch tp.Tp {
	case allegrosql.TypeLonglong, allegrosql.TypeVarString, allegrosql.TypeDouble, allegrosql.TypeBlob,
		allegrosql.TypeBit, allegrosql.TypeDuration, allegrosql.TypeNewDecimal, allegrosql.TypeEnum, allegrosql.TypeSet:
		return true
	}
	return false
}

// DefaultTypeForValue returns the default FieldType for the value.
func DefaultTypeForValue(value interface{}, tp *FieldType, char string, collate string) {
	switch x := value.(type) {
	case nil:
		tp.Tp = allegrosql.TypeNull
		tp.Flen = 0
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case bool:
		tp.Tp = allegrosql.TypeLonglong
		tp.Flen = 1
		tp.Decimal = 0
		tp.Flag |= allegrosql.IsBooleanFlag
		SetBinChsClnFlag(tp)
	case int:
		tp.Tp = allegrosql.TypeLonglong
		tp.Flen = utilMath.StrLenOfInt64Fast(int64(x))
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case int64:
		tp.Tp = allegrosql.TypeLonglong
		tp.Flen = utilMath.StrLenOfInt64Fast(x)
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case uint64:
		tp.Tp = allegrosql.TypeLonglong
		tp.Flag |= allegrosql.UnsignedFlag
		tp.Flen = utilMath.StrLenOfUint64Fast(x)
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case string:
		tp.Tp = allegrosql.TypeVarString
		// TODO: tp.Flen should be len(x) * 3 (max bytes length of CharsetUTF8)
		tp.Flen = len(x)
		tp.Decimal = UnspecifiedLength
		tp.Charset, tp.DefCauslate = char, collate
	case float32:
		tp.Tp = allegrosql.TypeFloat
		s := strconv.FormatFloat(float64(x), 'f', -1, 32)
		tp.Flen = len(s)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case float64:
		tp.Tp = allegrosql.TypeDouble
		s := strconv.FormatFloat(x, 'f', -1, 64)
		tp.Flen = len(s)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case []byte:
		tp.Tp = allegrosql.TypeBlob
		tp.Flen = len(x)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case BitLiteral:
		tp.Tp = allegrosql.TypeVarString
		tp.Flen = len(x)
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
	case HexLiteral:
		tp.Tp = allegrosql.TypeVarString
		tp.Flen = len(x) * 3
		tp.Decimal = 0
		tp.Flag |= allegrosql.UnsignedFlag
		SetBinChsClnFlag(tp)
	case BinaryLiteral:
		tp.Tp = allegrosql.TypeBit
		tp.Flen = len(x) * 8
		tp.Decimal = 0
		SetBinChsClnFlag(tp)
		tp.Flag &= ^allegrosql.BinaryFlag
		tp.Flag |= allegrosql.UnsignedFlag
	case Time:
		tp.Tp = x.Type()
		switch x.Type() {
		case allegrosql.TypeDate:
			tp.Flen = allegrosql.MaxDateWidth
			tp.Decimal = UnspecifiedLength
		case allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
			tp.Flen = allegrosql.MaxDatetimeWidthNoFsp
			if x.Fsp() > DefaultFsp { // consider point('.') and the fractional part.
				tp.Flen += int(x.Fsp()) + 1
			}
			tp.Decimal = int(x.Fsp())
		}
		SetBinChsClnFlag(tp)
	case Duration:
		tp.Tp = allegrosql.TypeDuration
		tp.Flen = len(x.String())
		if x.Fsp > DefaultFsp { // consider point('.') and the fractional part.
			tp.Flen = int(x.Fsp) + 1
		}
		tp.Decimal = int(x.Fsp)
		SetBinChsClnFlag(tp)
	case *MyDecimal:
		tp.Tp = allegrosql.TypeNewDecimal
		tp.Flen = len(x.ToString())
		tp.Decimal = int(x.digitsFrac)
		SetBinChsClnFlag(tp)
	case Enum:
		tp.Tp = allegrosql.TypeEnum
		tp.Flen = len(x.Name)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case Set:
		tp.Tp = allegrosql.TypeSet
		tp.Flen = len(x.Name)
		tp.Decimal = UnspecifiedLength
		SetBinChsClnFlag(tp)
	case json.BinaryJSON:
		tp.Tp = allegrosql.TypeJSON
		tp.Flen = UnspecifiedLength
		tp.Decimal = 0
		tp.Charset = charset.CharsetUTF8MB4
		tp.DefCauslate = charset.DefCauslationUTF8MB4
	default:
		tp.Tp = allegrosql.TypeUnspecified
		tp.Flen = UnspecifiedLength
		tp.Decimal = UnspecifiedLength
	}
}

// DefaultCharsetForType returns the default charset/collation for allegrosql type.
func DefaultCharsetForType(tp byte) (string, string) {
	switch tp {
	case allegrosql.TypeVarString, allegrosql.TypeString, allegrosql.TypeVarchar:
		// Default charset for string types is utf8mb4.
		return allegrosql.DefaultCharset, allegrosql.DefaultDefCauslationName
	}
	return charset.CharsetBin, charset.DefCauslationBin
}

// MergeFieldType merges two MyALLEGROSQL type to a new type.
// This is used in hybrid field type expression.
// For example "select case c when 1 then 2 when 2 then 'milevadb' from t;"
// The result field type of the case expression is the merged type of the two when clause.
// See https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql/field.cc#L1042
func MergeFieldType(a byte, b byte) byte {
	ia := getFieldTypeIndex(a)
	ib := getFieldTypeIndex(b)
	return fieldTypeMergeMemrules[ia][ib]
}

// mergeTypeFlag merges two MyALLEGROSQL type flag to a new one
// currently only NotNullFlag is checked
// todo more flag need to be checked, for example: UnsignedFlag
func mergeTypeFlag(a, b uint) uint {
	return a & (b&allegrosql.NotNullFlag | ^allegrosql.NotNullFlag)
}

func getFieldTypeIndex(tp byte) int {
	itp := int(tp)
	if itp < fieldTypeTearFrom {
		return itp
	}
	return fieldTypeTearFrom + itp - fieldTypeTearTo - 1
}

const (
	fieldTypeTearFrom = int(allegrosql.TypeBit) + 1
	fieldTypeTearTo   = int(allegrosql.TypeJSON) - 1
	fieldTypeNum      = fieldTypeTearFrom + (255 - fieldTypeTearTo)
)

var fieldTypeMergeMemrules = [fieldTypeNum][fieldTypeNum]byte{
	/* allegrosql.TypeUnspecified -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeUnspecified, allegrosql.TypeUnspecified,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeTiny -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeTiny,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeShort, allegrosql.TypeLong,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeFloat, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeTiny, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLonglong, allegrosql.TypeInt24,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeTiny,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeShort -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeShort,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeShort, allegrosql.TypeLong,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeFloat, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeShort, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLonglong, allegrosql.TypeInt24,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeShort,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeLong -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeLong,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeLong, allegrosql.TypeLong,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeLong, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLonglong, allegrosql.TypeLong,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeLong,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeFloat -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeDouble, allegrosql.TypeFloat,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeFloat, allegrosql.TypeDouble,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeFloat, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeFloat, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeFloat, allegrosql.TypeFloat,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeFloat,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeDouble, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeDouble -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeDouble, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeDouble,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeDouble, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeNull -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeTiny,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeShort, allegrosql.TypeLong,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeFloat, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeNull, allegrosql.TypeTimestamp,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLonglong, allegrosql.TypeLonglong,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeDate, allegrosql.TypeDuration,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeDatetime, allegrosql.TypeYear,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeBit,
		//allegrosql.TypeJSON
		allegrosql.TypeJSON,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeEnum,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeSet, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeGeometry,
	},
	/* allegrosql.TypeTimestamp -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeTimestamp, allegrosql.TypeTimestamp,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeDatetime, allegrosql.TypeDatetime,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeDatetime, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeLonglong -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeLonglong,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeLonglong, allegrosql.TypeLonglong,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeLonglong, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLonglong, allegrosql.TypeLong,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeLonglong,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeInt24 -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeInt24,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeInt24, allegrosql.TypeLong,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeFloat, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeInt24, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLonglong, allegrosql.TypeInt24,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeInt24,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal    allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeDate -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeDate, allegrosql.TypeDatetime,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeDate, allegrosql.TypeDatetime,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeDatetime, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeTime -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeDuration, allegrosql.TypeDatetime,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeDatetime, allegrosql.TypeDuration,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeDatetime, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeDatetime -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeDatetime, allegrosql.TypeDatetime,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeDatetime, allegrosql.TypeDatetime,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeDatetime, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeYear -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeUnspecified, allegrosql.TypeTiny,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeShort, allegrosql.TypeLong,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeFloat, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeYear, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLonglong, allegrosql.TypeInt24,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeYear,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeNewDate -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeNewDate, allegrosql.TypeDatetime,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeNewDate, allegrosql.TypeDatetime,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeDatetime, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeNewDate, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeVarchar -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeBit -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeBit, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeBit,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeJSON -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewFloat     allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeJSON, allegrosql.TypeVarchar,
		//allegrosql.TypeLongLONG     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         MYALLEGROSQL_TYPE_TIME
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     MYALLEGROSQL_TYPE_YEAR
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeJSON,
		//allegrosql.TypeNewDecimal   MYALLEGROSQL_TYPE_ENUM
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeLongBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeLongBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       MYALLEGROSQL_TYPE_GEOMETRY
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeNewDecimal -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeDouble, allegrosql.TypeDouble,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeNewDecimal, allegrosql.TypeNewDecimal,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeNewDecimal,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeNewDecimal, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeEnum -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeEnum, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeSet -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeSet, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeTinyBlob -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeTinyBlob,
		//allegrosql.TypeJSON
		allegrosql.TypeLongBlob,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeTinyBlob,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeTinyBlob, allegrosql.TypeTinyBlob,
	},
	/* allegrosql.TypeMediumBlob -> */
	{
		//allegrosql.TypeUnspecified    allegrosql.TypeTiny
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeMediumBlob,
		//allegrosql.TypeJSON
		allegrosql.TypeLongBlob,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeMediumBlob, allegrosql.TypeMediumBlob,
	},
	/* allegrosql.TypeLongBlob -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeLongBlob,
		//allegrosql.TypeJSON
		allegrosql.TypeLongBlob,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeLongBlob, allegrosql.TypeLongBlob,
	},
	/* allegrosql.TypeBlob -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeBlob,
		//allegrosql.TypeJSON
		allegrosql.TypeLongBlob,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeBlob,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeBlob, allegrosql.TypeBlob,
	},
	/* allegrosql.TypeVarString -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
	},
	/* allegrosql.TypeString -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeString, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeString,
		//allegrosql.TypeJSON
		allegrosql.TypeString,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeString, allegrosql.TypeString,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeString, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeString,
	},
	/* allegrosql.TypeGeometry -> */
	{
		//allegrosql.TypeUnspecified  allegrosql.TypeTiny
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeShort        allegrosql.TypeLong
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeFloat        allegrosql.TypeDouble
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNull         allegrosql.TypeTimestamp
		allegrosql.TypeGeometry, allegrosql.TypeVarchar,
		//allegrosql.TypeLonglong     allegrosql.TypeInt24
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDate         allegrosql.TypeTime
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeDatetime     allegrosql.TypeYear
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeNewDate      allegrosql.TypeVarchar
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeBit          <16>-<244>
		allegrosql.TypeVarchar,
		//allegrosql.TypeJSON
		allegrosql.TypeVarchar,
		//allegrosql.TypeNewDecimal   allegrosql.TypeEnum
		allegrosql.TypeVarchar, allegrosql.TypeVarchar,
		//allegrosql.TypeSet          allegrosql.TypeTinyBlob
		allegrosql.TypeVarchar, allegrosql.TypeTinyBlob,
		//allegrosql.TypeMediumBlob  allegrosql.TypeLongBlob
		allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		//allegrosql.TypeBlob         allegrosql.TypeVarString
		allegrosql.TypeBlob, allegrosql.TypeVarchar,
		//allegrosql.TypeString       allegrosql.TypeGeometry
		allegrosql.TypeString, allegrosql.TypeGeometry,
	},
}

// SetBinChsClnFlag sets charset, collation as 'binary' and adds binaryFlag to FieldType.
func SetBinChsClnFlag(ft *FieldType) {
	ft.Charset = charset.CharsetBin
	ft.DefCauslate = charset.DefCauslationBin
	ft.Flag |= allegrosql.BinaryFlag
}

// VarStorageLen indicates this column is a variable length column.
const VarStorageLen = ast.VarStorageLen
