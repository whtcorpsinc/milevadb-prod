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

package memex_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testIntegrationSuite) TestFoldIfNull(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc(`use test;`)
	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a bigint, b bigint);`)
	tk.MustInterDirc(`insert into t values(1, 1);`)
	tk.MustQuery(`desc select ifnull("aaaa", a) from t;`).Check(testkit.Events(
		`Projection_3 10000.00 root  aaaa->DeferredCauset#4`,
		`└─BlockReader_5 10000.00 root  data:BlockFullScan_4`,
		`  └─BlockFullScan_4 10000.00 cop[einsteindb] causet:t keep order:false, stats:pseudo`,
	))
	tk.MustQuery(`show warnings;`).Check(testkit.Events())
	tk.MustQuery(`select ifnull("aaaa", a) from t;`).Check(testkit.Events("aaaa"))
	tk.MustQuery(`show warnings;`).Check(testkit.Events())
}
