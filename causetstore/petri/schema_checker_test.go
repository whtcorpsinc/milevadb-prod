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

package petri

import (
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
)

func (s *testSuite) TestSchemaCheckerSimple(c *C) {
	lease := 5 * time.Millisecond
	validator := NewSchemaValidator(lease, nil)
	checker := &SchemaChecker{SchemaValidator: validator}

	// Add some schemaReplicant versions and delta causet IDs.
	ts := uint64(time.Now().UnixNano())
	validator.UFIDelate(ts, 0, 2, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{1}, CausetActionTypes: []uint64{1}})
	validator.UFIDelate(ts, 2, 4, &einsteindb.RelatedSchemaChange{PhyTblIDS: []int64{2}, CausetActionTypes: []uint64{2}})

	// checker's schemaReplicant version is the same as the current schemaReplicant version.
	checker.schemaVer = 4
	_, err := checker.Check(ts)
	c.Assert(err, IsNil)

	// checker's schemaReplicant version is less than the current schemaReplicant version, and it doesn't exist in validator's items.
	// checker's related causet ID isn't in validator's changed causet IDs.
	checker.schemaVer = 2
	checker.relatedBlockIDs = []int64{3}
	_, err = checker.Check(ts)
	c.Assert(err, IsNil)
	// The checker's schemaReplicant version isn't in validator's items.
	checker.schemaVer = 1
	checker.relatedBlockIDs = []int64{3}
	_, err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, ErrSchemaReplicantChanged), IsTrue)
	// checker's related causet ID is in validator's changed causet IDs.
	checker.relatedBlockIDs = []int64{2}
	_, err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, ErrSchemaReplicantChanged), IsTrue)

	// validator's latest schemaReplicant version is expired.
	time.Sleep(lease + time.Microsecond)
	checker.schemaVer = 4
	checker.relatedBlockIDs = []int64{3}
	_, err = checker.Check(ts)
	c.Assert(err, IsNil)
	nowTS := uint64(time.Now().UnixNano())
	// Use checker.SchemaValidator.Check instead of checker.Check here because backoff make CI slow.
	_, result := checker.SchemaValidator.Check(nowTS, checker.schemaVer, checker.relatedBlockIDs)
	c.Assert(result, Equals, ResultUnknown)
}
