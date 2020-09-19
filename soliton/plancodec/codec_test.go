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

package plancodec

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/ekv"
)

var _ = Suite(&testPlanCodecSuite{})

type testPlanCodecSuite struct{}

type encodeTaskTypeCase struct {
	IsRoot     bool
	StoreType  ekv.StoreType
	EncodedStr string
	DecodedStr string
}

func (s *testPlanCodecSuite) TestEncodeTaskType(c *C) {
	cases := []encodeTaskTypeCase{
		{true, ekv.UnSpecified, "0", "root"},
		{false, ekv.EinsteinDB, "1_0", "cop[einsteindb]"},
		{false, ekv.TiFlash, "1_1", "cop[tiflash]"},
		{false, ekv.MilevaDB, "1_2", "cop[milevadb]"},
	}
	for _, cas := range cases {
		c.Assert(EncodeTaskType(cas.IsRoot, cas.StoreType), Equals, cas.EncodedStr)
		str, err := decodeTaskType(cas.EncodedStr)
		c.Assert(err, IsNil)
		c.Assert(str, Equals, cas.DecodedStr)
	}

	str, err := decodeTaskType("1")
	c.Assert(err, IsNil)
	c.Assert(str, Equals, "cop")

	_, err = decodeTaskType("1_x")
	c.Assert(err, NotNil)
}
