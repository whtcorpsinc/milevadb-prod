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

package causet

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
)

var _ = Suite(&testBlockSuite{})

type testBlockSuite struct{}

func (t *testBlockSuite) TestErrorCode(c *C) {
	c.Assert(int(terror.ToALLEGROSQLError(ErrDeferredCausetCantNull).Code), Equals, allegrosql.ErrBadNull)
	c.Assert(int(terror.ToALLEGROSQLError(ErrUnknownDeferredCauset).Code), Equals, allegrosql.ErrBadField)
	c.Assert(int(terror.ToALLEGROSQLError(errDuplicateDeferredCauset).Code), Equals, allegrosql.ErrFieldSpecifiedTwice)
	c.Assert(int(terror.ToALLEGROSQLError(errGetDefaultFailed).Code), Equals, allegrosql.ErrFieldGetDefaultFailed)
	c.Assert(int(terror.ToALLEGROSQLError(ErrNoDefaultValue).Code), Equals, allegrosql.ErrNoDefaultForField)
	c.Assert(int(terror.ToALLEGROSQLError(ErrIndexOutBound).Code), Equals, allegrosql.ErrIndexOutBound)
	c.Assert(int(terror.ToALLEGROSQLError(ErrUnsupportedOp).Code), Equals, allegrosql.ErrUnsupportedOp)
	c.Assert(int(terror.ToALLEGROSQLError(ErrRowNotFound).Code), Equals, allegrosql.ErrRowNotFound)
	c.Assert(int(terror.ToALLEGROSQLError(ErrBlockStateCantNone).Code), Equals, allegrosql.ErrBlockStateCantNone)
	c.Assert(int(terror.ToALLEGROSQLError(ErrDeferredCausetStateCantNone).Code), Equals, allegrosql.ErrDeferredCausetStateCantNone)
	c.Assert(int(terror.ToALLEGROSQLError(ErrDeferredCausetStateNonPublic).Code), Equals, allegrosql.ErrDeferredCausetStateNonPublic)
	c.Assert(int(terror.ToALLEGROSQLError(ErrIndexStateCantNone).Code), Equals, allegrosql.ErrIndexStateCantNone)
	c.Assert(int(terror.ToALLEGROSQLError(ErrInvalidRecordKey).Code), Equals, allegrosql.ErrInvalidRecordKey)
	c.Assert(int(terror.ToALLEGROSQLError(ErrTruncatedWrongValueForField).Code), Equals, allegrosql.ErrTruncatedWrongValueForField)
	c.Assert(int(terror.ToALLEGROSQLError(ErrUnknownPartition).Code), Equals, allegrosql.ErrUnknownPartition)
	c.Assert(int(terror.ToALLEGROSQLError(ErrNoPartitionForGivenValue).Code), Equals, allegrosql.ErrNoPartitionForGivenValue)
	c.Assert(int(terror.ToALLEGROSQLError(ErrLockOrActiveTransaction).Code), Equals, allegrosql.ErrLockOrActiveTransaction)
}
