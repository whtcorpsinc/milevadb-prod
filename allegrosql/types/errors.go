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
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	BerolinaSQL_types "github.com/whtcorpsinc/BerolinaSQL/types"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

// const strings for ErrWrongValue
const (
	DateTimeStr = "datetime"
	TimeStr     = "time"
)

var (
	// ErrInvalidDefault is returned when meet a invalid default value.
	ErrInvalidDefault = BerolinaSQL_types.ErrInvalidDefault
	// ErrDataTooLong is returned when converts a string value that is longer than field type length.
	ErrDataTooLong = terror.ClassTypes.New(allegrosql.ErrDataTooLong, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDataTooLong])
	// ErrIllegalValueForType is returned when value of type is illegal.
	ErrIllegalValueForType = terror.ClassTypes.New(allegrosql.ErrIllegalValueForType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrIllegalValueForType])
	// ErrTruncated is returned when data has been truncated during conversion.
	ErrTruncated = terror.ClassTypes.New(allegrosql.WarnDataTruncated, allegrosql.MyALLEGROSQLErrName[allegrosql.WarnDataTruncated])
	// ErrOverflow is returned when data is out of range for a field type.
	ErrOverflow = terror.ClassTypes.New(allegrosql.ErrDataOutOfRange, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDataOutOfRange])
	// ErrDivByZero is return when do division by 0.
	ErrDivByZero = terror.ClassTypes.New(allegrosql.ErrDivisionByZero, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDivisionByZero])
	// ErrTooBigDisplayWidth is return when display width out of range for column.
	ErrTooBigDisplayWidth = terror.ClassTypes.New(allegrosql.ErrTooBigDisplaywidth, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooBigDisplaywidth])
	// ErrTooBigFieldLength is return when column length too big for column.
	ErrTooBigFieldLength = terror.ClassTypes.New(allegrosql.ErrTooBigFieldlength, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooBigFieldlength])
	// ErrTooBigSet is returned when too many strings for column.
	ErrTooBigSet = terror.ClassTypes.New(allegrosql.ErrTooBigSet, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooBigSet])
	// ErrTooBigScale is returned when type DECIMAL/NUMERIC scale is bigger than allegrosql.MaxDecimalScale.
	ErrTooBigScale = terror.ClassTypes.New(allegrosql.ErrTooBigScale, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooBigScale])
	// ErrTooBigPrecision is returned when type DECIMAL/NUMERIC precision is bigger than allegrosql.MaxDecimalWidth
	ErrTooBigPrecision = terror.ClassTypes.New(allegrosql.ErrTooBigPrecision, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTooBigPrecision])
	// ErrBadNumber is return when parsing an invalid binary decimal number.
	ErrBadNumber = terror.ClassTypes.New(allegrosql.ErrBadNumber, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadNumber])
	// ErrInvalidFieldSize is returned when the precision of a column is out of range.
	ErrInvalidFieldSize = terror.ClassTypes.New(allegrosql.ErrInvalidFieldSize, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidFieldSize])
	// ErrMBiggerThanD is returned when precision less than the scale.
	ErrMBiggerThanD = terror.ClassTypes.New(allegrosql.ErrMBiggerThanD, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrMBiggerThanD])
	// ErrWarnDataOutOfRange is returned when the value in a numeric column that is outside the permissible range of the column data type.
	// See https://dev.allegrosql.com/doc/refman/5.5/en/out-of-range-and-overflow.html for details
	ErrWarnDataOutOfRange = terror.ClassTypes.New(allegrosql.ErrWarnDataOutOfRange, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWarnDataOutOfRange])
	// ErrDuplicatedValueInType is returned when enum column has duplicated value.
	ErrDuplicatedValueInType = terror.ClassTypes.New(allegrosql.ErrDuplicatedValueInType, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDuplicatedValueInType])
	// ErrDatetimeFunctionOverflow is returned when the calculation in datetime function cause overflow.
	ErrDatetimeFunctionOverflow = terror.ClassTypes.New(allegrosql.ErrDatetimeFunctionOverflow, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDatetimeFunctionOverflow])
	// ErrCastAsSignedOverflow is returned when positive out-of-range integer, and convert to it's negative complement.
	ErrCastAsSignedOverflow = terror.ClassTypes.New(allegrosql.ErrCastAsSignedOverflow, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCastAsSignedOverflow])
	// ErrCastNegIntAsUnsigned is returned when a negative integer be casted to an unsigned int.
	ErrCastNegIntAsUnsigned = terror.ClassTypes.New(allegrosql.ErrCastNegIntAsUnsigned, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCastNegIntAsUnsigned])
	// ErrInvalidYearFormat is returned when the input is not a valid year format.
	ErrInvalidYearFormat = terror.ClassTypes.New(allegrosql.ErrInvalidYearFormat, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidYearFormat])
	// ErrInvalidYear is returned when the input value is not a valid year.
	ErrInvalidYear = terror.ClassTypes.New(allegrosql.ErrInvalidYear, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidYear])
	// ErrTruncatedWrongVal is returned when data has been truncated during conversion.
	ErrTruncatedWrongVal = terror.ClassTypes.New(allegrosql.ErrTruncatedWrongValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTruncatedWrongValue])
	// ErrInvalidWeekModeFormat is returned when the week mode is wrong.
	ErrInvalidWeekModeFormat = terror.ClassTypes.New(allegrosql.ErrInvalidWeekModeFormat, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidWeekModeFormat])
	// ErrWrongValue is returned when the input value is in wrong format.
	ErrWrongValue = terror.ClassTypes.New(allegrosql.ErrTruncatedWrongValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongValue])
)
