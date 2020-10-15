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

package interlock

import (
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

// Error instances.
var (
	ErrGetStartTS      = terror.ClassInterlockingDirectorate.New(allegrosql.ErrGetStartTS, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrGetStartTS])
	ErrUnknownCauset     = terror.ClassInterlockingDirectorate.New(allegrosql.ErrUnknownCauset, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownCauset])
	ErrPrepareMulti    = terror.ClassInterlockingDirectorate.New(allegrosql.ErrPrepareMulti, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPrepareMulti])
	ErrPrepareDBS      = terror.ClassInterlockingDirectorate.New(allegrosql.ErrPrepareDBS, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPrepareDBS])
	ErrResultIsEmpty   = terror.ClassInterlockingDirectorate.New(allegrosql.ErrResultIsEmpty, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrResultIsEmpty])
	ErrBuildInterlockingDirectorate   = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBuildInterlockingDirectorate, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBuildInterlockingDirectorate])
	ErrBatchInsertFail = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBatchInsertFail, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBatchInsertFail])

	ErrCantCreateUserWithGrant     = terror.ClassInterlockingDirectorate.New(allegrosql.ErrCantCreateUserWithGrant, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantCreateUserWithGrant])
	ErrPasswordNoMatch             = terror.ClassInterlockingDirectorate.New(allegrosql.ErrPasswordNoMatch, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPasswordNoMatch])
	ErrCannotUser                  = terror.ClassInterlockingDirectorate.New(allegrosql.ErrCannotUser, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCannotUser])
	ErrPasswordFormat              = terror.ClassInterlockingDirectorate.New(allegrosql.ErrPasswordFormat, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPasswordFormat])
	ErrCantChangeTxCharacteristics = terror.ClassInterlockingDirectorate.New(allegrosql.ErrCantChangeTxCharacteristics, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantChangeTxCharacteristics])
	ErrPsManyParam                 = terror.ClassInterlockingDirectorate.New(allegrosql.ErrPsManyParam, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrPsManyParam])
	ErrAdminCheckBlock             = terror.ClassInterlockingDirectorate.New(allegrosql.ErrAdminCheckBlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrAdminCheckBlock])
	ErrDBaccessDenied              = terror.ClassInterlockingDirectorate.New(allegrosql.ErrDBaccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDBaccessDenied])
	ErrBlockaccessDenied           = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBlockaccessDenied, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockaccessDenied])
	ErrBadDB                       = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBadDB, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadDB])
	ErrWrongObject                 = terror.ClassInterlockingDirectorate.New(allegrosql.ErrWrongObject, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWrongObject])
	ErrRoleNotGranted              = terror.ClassPrivilege.New(allegrosql.ErrRoleNotGranted, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRoleNotGranted])
	ErrDeadlock                    = terror.ClassInterlockingDirectorate.New(allegrosql.ErrLockDeadlock, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrLockDeadlock])
	ErrQueryInterrupted            = terror.ClassInterlockingDirectorate.New(allegrosql.ErrQueryInterrupted, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrQueryInterrupted])

	ErrBRIEBackupFailed  = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBRIEBackupFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIEBackupFailed])
	ErrBRIERestoreFailed = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBRIERestoreFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIERestoreFailed])
	ErrBRIEImportFailed  = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBRIEImportFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIEImportFailed])
	ErrBRIEExportFailed  = terror.ClassInterlockingDirectorate.New(allegrosql.ErrBRIEExportFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBRIEExportFailed])
)
