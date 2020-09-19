// Copyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	ast "github.com/whtcorpsinc/berolinaAllegroSQL/types"
)

// IsTypeBlob returns a boolean indicating whether the tp is a blob type.
var IsTypeBlob = ast.IsTypeBlob

// IsTypeChar returns a boolean indicating
// whether the tp is the char type like a string type or a varchar type.
var IsTypeChar = ast.IsTypeChar

// IsTypeVarchar returns a boolean indicating
// whether the tp is the varchar type like a varstring type or a varchar type.
func IsTypeVarchar(tp byte) bool {
	return tp == allegrosql.TypeVarString || tp == allegrosql.TypeVarchar
}

// IsTypeUnspecified returns a boolean indicating whether the tp is the Unspecified type.
func IsTypeUnspecified(tp byte) bool {
	return tp == allegrosql.TypeUnspecified
}

// IsTypePrefixable returns a boolean indicating
// whether an index on a column with the tp can be defined with a prefix.
func IsTypePrefixable(tp byte) bool {
	return IsTypeBlob(tp) || IsTypeChar(tp)
}

// IsTypeFractionable returns a boolean indicating
// whether the tp can has time fraction.
func IsTypeFractionable(tp byte) bool {
	return tp == allegrosql.TypeDatetime || tp == allegrosql.TypeDuration || tp == allegrosql.TypeTimestamp
}

// IsTypeTime returns a boolean indicating
// whether the tp is time type like datetime, date or timestamp.
func IsTypeTime(tp byte) bool {
	return tp == allegrosql.TypeDatetime || tp == allegrosql.TypeDate || tp == allegrosql.TypeTimestamp
}

// IsTypeNumeric returns a boolean indicating whether the tp is numeric type.
func IsTypeNumeric(tp byte) bool {
	switch tp {
	case allegrosql.TypeBit, allegrosql.TypeTiny, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeNewDecimal,
		allegrosql.TypeFloat, allegrosql.TypeDouble, allegrosql.TypeShort:
		return true
	}
	return false
}

// IsTemporalWithDate returns a boolean indicating
// whether the tp is time type with date.
func IsTemporalWithDate(tp byte) bool {
	return IsTypeTime(tp)
}

// IsBinaryStr returns a boolean indicating
// whether the field type is a binary string type.
func IsBinaryStr(ft *FieldType) bool {
	return ft.DefCauslate == charset.DefCauslationBin && IsString(ft.Tp)
}

// IsNonBinaryStr returns a boolean indicating
// whether the field type is a non-binary string type.
func IsNonBinaryStr(ft *FieldType) bool {
	if ft.DefCauslate != charset.DefCauslationBin && IsString(ft.Tp) {
		return true
	}
	return false
}

// IsString returns a boolean indicating
// whether the field type is a string type.
func IsString(tp byte) bool {
	return IsTypeChar(tp) || IsTypeBlob(tp) || IsTypeVarchar(tp) || IsTypeUnspecified(tp)
}

var HoTT2Str = map[byte]string{
	HoTTNull:          "null",
	HoTTInt64:         "bigint",
	HoTTUint64:        "unsigned bigint",
	HoTTFloat32:       "float",
	HoTTFloat64:       "double",
	HoTTString:        "char",
	HoTTBytes:         "bytes",
	HoTTBinaryLiteral: "bit/hex literal",
	HoTTMysqlDecimal:  "decimal",
	HoTTMysqlDuration: "time",
	HoTTMysqlEnum:     "enum",
	HoTTMysqlBit:      "bit",
	HoTTMysqlSet:      "set",
	HoTTMysqlTime:     "datetime",
	HoTTInterface:     "interface",
	HoTTMinNotNull:    "min_not_null",
	HoTTMaxValue:      "max_value",
	HoTTRaw:           "raw",
	HoTTMysqlJSON:     "json",
}

// TypeStr converts tp to a string.
var TypeStr = ast.TypeStr

// HoTTStr converts HoTT to a string.
func HoTTStr(HoTT byte) (r string) {
	return HoTT2Str[HoTT]
}

// TypeToStr converts a field to a string.
// It is used for converting Text to Blob,
// or converting Char to Binary.
// Args:
//	tp: type enum
//	cs: charset
var TypeToStr = ast.TypeToStr

// EOFAsNil filtrates errors,
// If err is equal to io.EOF returns nil.
func EOFAsNil(err error) error {
	if terror.ErrorEqual(err, io.EOF) {
		return nil
	}
	return errors.Trace(err)
}

// InvOp2 returns an invalid operation error.
func InvOp2(x, y interface{}, o opcode.Op) (interface{}, error) {
	return nil, errors.Errorf("Invalid operation: %v %v %v (mismatched types %T and %T)", x, o, y, x, y)
}

// overflow returns an overflowed error.
func overflow(v interface{}, tp byte) error {
	return ErrOverflow.GenWithStack("constant %v overflows %s", v, TypeStr(tp))
}

// IsTypeTemporal checks if a type is a temporal type.
func IsTypeTemporal(tp byte) bool {
	switch tp {
	case allegrosql.TypeDuration, allegrosql.TypeDatetime, allegrosql.TypeTimestamp,
		allegrosql.TypeDate, allegrosql.TypeNewDate:
		return true
	}
	return false
}
