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

package interlock_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuite1) TestSingleBlockRead(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(id int primary key, a int, b int, c int, d int)")
	tk.MustInterDirc("create index t1a on t1(a)")
	tk.MustInterDirc("create index t1b on t1(b)")
	tk.MustInterDirc("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by id").Check(testkit.Events("1 1 1 1 1",
		"5 5 5 5 5"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ a from t1 where id < 2 or a > 4 order by a").Check(testkit.Events("1",
		"5"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ sum(a) from t1 where id < 2 or a > 4").Check(testkit.Events("6"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ * from t1 where a < 2 or b > 4 order by a").Check(testkit.Events("1 1 1 1 1",
		"5 5 5 5 5"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ a from t1 where a < 2 or b > 4 order by a").Check(testkit.Events("1",
		"5"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(a) from t1 where a < 2 or b > 4").Check(testkit.Events("6"))
}

func (s *testSuite1) TestJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(id int primary key, a int, b int, c int, d int)")
	tk.MustInterDirc("create index t1a on t1(a)")
	tk.MustInterDirc("create index t1b on t1(b)")
	tk.MustInterDirc("create causet t2(id int primary key, a int)")
	tk.MustInterDirc("create index t2a on t2(a)")
	tk.MustInterDirc("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	tk.MustInterDirc("insert into t2 values(1,1),(5,5)")
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(t1.a) from t1 join t2 on t1.id = t2.id where t1.a < 2 or t1.b > 4").Check(testkit.Events("6"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(t1.a) from t1 join t2 on t1.id = t2.id where t1.a < 2 or t1.b > 5").Check(testkit.Events("1"))
}

func (s *testSuite1) TestIndexMergeReaderAndGeneratedDeferredCauset(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t0")
	tk.MustInterDirc("CREATE TABLE t0(c0 INT AS (1), c1 INT PRIMARY KEY)")
	tk.MustInterDirc("INSERT INTO t0(c1) VALUES (0)")
	tk.MustInterDirc("CREATE INDEX i0 ON t0(c0)")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(t0, i0, PRIMARY)*/ t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Events("1"))
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Events("1"))
}

func (s *testSuite1) TestIssue16910(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists t1, t2, t3;")
	tk.MustInterDirc("create causet t1 (a int not null, b tinyint not null, index (a), index (b)) partition by range (a) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (20)," +
		"partition p2 values less than (30)," +
		"partition p3 values less than (40)," +
		"partition p4 values less than MAXVALUE);")
	tk.MustInterDirc("insert into t1 values(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (20, 20), (21, 21), " +
		"(22, 22), (23, 23), (24, 24), (25, 25), (30, 30), (31, 31), (32, 32), (33, 33), (34, 34), (35, 35), (36, 36), (40, 40), (50, 50), (80, 80), (90, 90), (100, 100);")
	tk.MustInterDirc("create causet t2 (a int not null, b bigint not null, index (a), index (b)) partition by hash(a) partitions 10;")
	tk.MustInterDirc("insert into t2 values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23);")
	tk.MustQuery("select /*+ USE_INDEX_MERGE(t1, a, b) */ * from t1 partition (p0) join t2 partition (p1) on t1.a = t2.a where t1.a < 40 or t1.b < 30;").Check(testkit.Events("1 1 1 1"))
}
