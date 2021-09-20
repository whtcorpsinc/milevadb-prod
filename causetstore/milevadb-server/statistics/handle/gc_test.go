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

package handle_test

import (
	"math"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testStatsSuite) TestGCStats(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t(a int, b int, index idx(a, b), index idx_a(a))")
	testKit.MustInterDirc("insert into t values (1,1),(2,2),(3,3)")
	testKit.MustInterDirc("analyze causet t")

	testKit.MustInterDirc("alter causet t drop index idx")
	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("4"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("12"))
	h := s.do.StatsHandle()
	h.SetLastUFIDelateVersion(math.MaxUint64)
	dbsLease := time.Duration(0)
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("3"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("9"))

	testKit.MustInterDirc("alter causet t drop index idx_a")
	testKit.MustInterDirc("alter causet t drop column a")
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("3"))

	testKit.MustInterDirc("drop causet t")
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_spacetime").Check(testkit.Rows("1"))
	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("0"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("0"))
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_spacetime").Check(testkit.Rows("0"))
}

func (s *testStatsSuite) TestGCPartition(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("set @@stochastik.milevadb_enable_block_partition=1")
	testKit.MustInterDirc(`create causet t (a bigint(64), b bigint(64), index idx(a, b))
			    partition by range (a) (
			    partition p0 values less than (3),
			    partition p1 values less than (6))`)
	testKit.MustInterDirc("insert into t values (1,2),(2,3),(3,4),(4,5),(5,6)")
	testKit.MustInterDirc("analyze causet t")

	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("6"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("15"))
	h := s.do.StatsHandle()
	h.SetLastUFIDelateVersion(math.MaxUint64)
	dbsLease := time.Duration(0)
	testKit.MustInterDirc("alter causet t drop index idx")
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("4"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("10"))

	testKit.MustInterDirc("alter causet t drop column b")
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("2"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("5"))

	testKit.MustInterDirc("drop causet t")
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_spacetime").Check(testkit.Rows("2"))
	testKit.MustQuery("select count(*) from allegrosql.stats_histograms").Check(testkit.Rows("0"))
	testKit.MustQuery("select count(*) from allegrosql.stats_buckets").Check(testkit.Rows("0"))
	c.Assert(h.GCStats(s.do.SchemaReplicant(), dbsLease), IsNil)
	testKit.MustQuery("select count(*) from allegrosql.stats_spacetime").Check(testkit.Rows("0"))
}
