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

package variable

import (
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

// Error instances.
var (
	errCantGetValidID              = terror.ClassVariable.New(allegrosql.ErrCantGetValidID, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantGetValidID])
	errWarnDeprecatedSyntax        = terror.ClassVariable.New(allegrosql.ErrWarnDeprecatedSyntax, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWarnDeprecatedSyntax])
	ErrCantSetToNull               = terror.ClassVariable.New(allegrosql.ErrCantSetToNull, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantSetToNull])
	ErrSnapshotTooOld              = terror.ClassVariable.New(allegrosql.ErrSnapshotTooOld, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSnapshotTooOld])
	ErrUnsupportedValueForVar      = terror.ClassVariable.New(allegrosql.ErrUnsupportedValueForVar, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedValueForVar])
	ErrUnknownSystemVar            = terror.ClassVariable.New(allegrosql.ErrUnknownSystemVariable, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownSystemVariable])
	ErrIncorrectScope              = terror.ClassVariable.New(allegrosql.ErrIncorrectGlobalLocalVar, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrIncorrectGlobalLocalVar])
	ErrUnknownTimeZone             = terror.ClassVariable.New(allegrosql.ErrUnknownTimeZone, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownTimeZone])
	ErrReadOnly                    = terror.ClassVariable.New(allegrosql.ErrVariableIsReadonly, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrVariableIsReadonly])
	ErrWrongValueForVar            = terror.ClassVariable.New(allegrosql.ErrWrongValueForVar, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongValueForVar])
	ErrWrongTypeForVar             = terror.ClassVariable.New(allegrosql.ErrWrongTypeForVar, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongTypeForVar])
	ErrTruncatedWrongValue         = terror.ClassVariable.New(allegrosql.ErrTruncatedWrongValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTruncatedWrongValue])
	ErrMaxPreparedStmtCountReached = terror.ClassVariable.New(allegrosql.ErrMaxPreparedStmtCountReached, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrMaxPreparedStmtCountReached])
	ErrUnsupportedIsolationLevel   = terror.ClassVariable.New(allegrosql.ErrUnsupportedIsolationLevel, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedIsolationLevel])
)
