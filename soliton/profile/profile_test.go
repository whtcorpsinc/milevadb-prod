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

package profile_test

import (
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/soliton/profile"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

type profileSuite struct {
	causetstore ekv.CausetStorage
	dom   *petri.Petri
}

var _ = Suite(&profileSuite{})

func (s *profileSuite) SetUpSuite(c *C) {
	var err error
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	stochastik.DisableStats4Test()
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *profileSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
}

func (s *profileSuite) TestProfiles(c *C) {
	oldValue := profile.CPUProfileInterval
	profile.CPUProfileInterval = 2 * time.Second
	defer func() {
		profile.CPUProfileInterval = oldValue
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("select * from performance_schema.milevadb_profile_cpu")
	tk.MustInterDirc("select * from performance_schema.milevadb_profile_memory")
	tk.MustInterDirc("select * from performance_schema.milevadb_profile_allocs")
	tk.MustInterDirc("select * from performance_schema.milevadb_profile_mutex")
	tk.MustInterDirc("select * from performance_schema.milevadb_profile_block")
	tk.MustInterDirc("select * from performance_schema.milevadb_profile_goroutines")
}
