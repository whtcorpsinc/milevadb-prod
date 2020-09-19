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

package handle

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/statistics"
)

var _ = Suite(&testUFIDelateListSuite{})

type testUFIDelateListSuite struct {
}

func (s *testUFIDelateListSuite) TestInsertAndDelete(c *C) {
	h := Handle{
		listHead: &StochastikStatsDefCauslector{mapper: make(blockDeltaMap)},
		feedback: statistics.NewQueryFeedbackMap(),
	}
	var items []*StochastikStatsDefCauslector
	for i := 0; i < 5; i++ {
		items = append(items, h.NewStochastikStatsDefCauslector())
	}
	items[0].Delete() // delete tail
	items[2].Delete() // delete midbse
	items[4].Delete() // delete head
	h.sweepList()

	c.Assert(h.listHead.next, Equals, items[3])
	c.Assert(items[3].next, Equals, items[1])
	c.Assert(items[1].next, IsNil)

	// delete rest
	items[1].Delete()
	items[3].Delete()
	h.sweepList()
	c.Assert(h.listHead.next, IsNil)
}
