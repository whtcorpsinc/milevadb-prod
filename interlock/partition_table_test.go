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
	"context"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *partitionBlockSuite) TestFourReader(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists pt")
	tk.MustInterDirc(`create causet pt (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustInterDirc("insert into pt values (0, 0), (2, 2), (4, 4), (6, 6), (7, 7), (9, 9), (null, null)")

	// Block reader
	tk.MustQuery("select * from pt").Sort().Check(testkit.Events("0 0", "2 2", "4 4", "6 6", "7 7", "9 9", "<nil> <nil>"))
	// Block reader: causet dual
	tk.MustQuery("select * from pt where c > 10").Check(testkit.Events())
	// Block reader: one partition
	tk.MustQuery("select * from pt where c > 8").Check(testkit.Events("9 9"))
	// Block reader: more than one partition
	tk.MustQuery("select * from pt where c < 2 or c >= 9").Check(testkit.Events("0 0", "9 9"))

	// Index reader
	tk.MustQuery("select c from pt").Sort().Check(testkit.Events("0", "2", "4", "6", "7", "9", "<nil>"))
	tk.MustQuery("select c from pt where c > 10").Check(testkit.Events())
	tk.MustQuery("select c from pt where c > 8").Check(testkit.Events("9"))
	tk.MustQuery("select c from pt where c < 2 or c >= 9").Check(testkit.Events("0", "9"))

	// Index lookup
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt").Sort().Check(testkit.Events("0 0", "2 2", "4 4", "6 6", "7 7", "9 9", "<nil> <nil>"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 4 and c > 10").Check(testkit.Events())
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c > 8").Check(testkit.Events("9 9"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c < 2 or c >= 9").Check(testkit.Events("0 0", "9 9"))

	// Index Merge
	tk.MustInterDirc("set @@milevadb_enable_index_merge = 1")
	tk.MustQuery("select /*+ use_index(i_c, i_id) */ * from pt where id = 4 or c < 7").Check(testkit.Events("0 0", "2 2", "4 4", "6 6"))
}

func (s *partitionBlockSuite) TestPartitionIndexJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists p, t")
	tk.MustInterDirc(`create causet p (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustInterDirc("create causet t (id int)")
	tk.MustInterDirc("insert into p values (3,3), (4,4), (6,6), (9,9)")
	tk.MustInterDirc("insert into t values (4), (9)")

	// Build indexLookUp in index join
	tk.MustQuery("select /*+ INL_JOIN(p) */ * from p, t where p.id = t.id").Sort().Check(testkit.Events("4 4 4", "9 9 9"))
	// Build index reader in index join
	tk.MustQuery("select /*+ INL_JOIN(p) */ p.id from p, t where p.id = t.id").Check(testkit.Events("4", "9"))
}

func (s *partitionBlockSuite) TestPartitionUnionScanIndexJoin(c *C) {
	// For issue https://github.com/whtcorpsinc/milevadb/issues/19152
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1  (c_int int, c_str varchar(40), primary key (c_int)) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustInterDirc("create causet t2  (c_int int, c_str varchar(40), primary key (c_int, c_str)) partition by hash (c_int) partitions 4")
	tk.MustInterDirc("insert into t1 values (10, 'interesting neumann')")
	tk.MustInterDirc("insert into t2 select * from t1")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t2 values (11, 'hopeful hoover');")
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */  * from t1 join t2 on t1.c_int = t2.c_int and t1.c_str = t2.c_str where t1.c_int in (10, 11)").Check(testkit.Events("10 interesting neumann 10 interesting neumann"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1,t2) */  * from t1 join t2 on t1.c_int = t2.c_int and t1.c_str = t2.c_str where t1.c_int in (10, 11)").Check(testkit.Events("10 interesting neumann 10 interesting neumann"))
	tk.MustInterDirc("commit")
}

func (s *partitionBlockSuite) TestPosetDagBlockID(c *C) {
	// This test checks the causet ID in the PosetDag is changed to partition ID in the nextPartition function.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet employees (id int,store_id int not null)partition by hash(store_id) partitions 4;")
	allegrosql := "select * from test.employees"
	rs, err := tk.InterDirc(allegrosql)
	c.Assert(err, IsNil)

	m := make(map[int64]struct{})
	ctx := context.WithValue(context.Background(), "nextPartitionUFIDelatePosetDagReq", m)
	tk.ResultSetToResultWithCtx(ctx, rs, Commentf("allegrosql:%s, args:%v", allegrosql))
	// Check causet ID is changed to partition ID for each partition.
	c.Assert(m, HasLen, 4)
}

func (s *partitionBlockSuite) TestPartitionReaderUnderApply(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")

	// For issue 19458.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(c_int int)")
	tk.MustInterDirc("insert into t values(1), (2), (3), (4), (5), (6), (7), (8), (9)")
	tk.MustInterDirc("DROP TABLE IF EXISTS `t1`")
	tk.MustInterDirc(`CREATE TABLE t1 (
		  c_int int NOT NULL,
		  c_str varchar(40) NOT NULL,
		  c_datetime datetime NOT NULL,
		  c_timestamp timestamp NULL DEFAULT NULL,
		  c_double double DEFAULT NULL,
		  c_decimal decimal(12,6) DEFAULT NULL,
		  PRIMARY KEY (c_int,c_str,c_datetime)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
		 PARTITION BY RANGE (c_int)
		(PARTITION p0 VALUES LESS THAN (2) ENGINE = InnoDB,
		 PARTITION p1 VALUES LESS THAN (4) ENGINE = InnoDB,
		 PARTITION p2 VALUES LESS THAN (6) ENGINE = InnoDB,
		 PARTITION p3 VALUES LESS THAN (8) ENGINE = InnoDB,
		 PARTITION p4 VALUES LESS THAN (10) ENGINE = InnoDB,
		 PARTITION p5 VALUES LESS THAN (20) ENGINE = InnoDB,
		 PARTITION p6 VALUES LESS THAN (50) ENGINE = InnoDB,
		 PARTITION p7 VALUES LESS THAN (1000000000) ENGINE = InnoDB)`)
	tk.MustInterDirc("INSERT INTO `t1` VALUES (19,'nifty feistel','2020-02-28 04:01:28','2020-02-04 06:11:57',32.430079,1.284000),(20,'objective snyder','2020-04-15 17:55:04','2020-05-30 22:04:13',37.690874,9.372000)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 values (22, 'wizardly saha', '2020-05-03 16:35:22', '2020-05-03 02:18:42', 96.534810, 0.088)")
	tk.MustQuery("select c_int from t where (select min(t1.c_int) from t1 where t1.c_int > t.c_int) > (select count(*) from t1 where t1.c_int > t.c_int) order by c_int").Check(testkit.Events(
		"1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustInterDirc("rollback")

	// For issue 19450.
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1  (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key (c_int))")
	tk.MustInterDirc("create causet t2  (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key (c_int)) partition by hash (c_int) partitions 4")
	tk.MustInterDirc("insert into t1 values (1, 'romantic robinson', 4.436), (2, 'stoic chaplygin', 9.826), (3, 'vibrant shamir', 6.300), (4, 'hungry wilson', 4.900), (5, 'naughty swartz', 9.524)")
	tk.MustInterDirc("insert into t2 select * from t1")
	tk.MustQuery("select * from t1 where c_decimal in (select c_decimal from t2 where t1.c_int = t2.c_int or t1.c_int = t2.c_int and t1.c_str > t2.c_str)").Check(testkit.Events(
		"1 romantic robinson 4.436000",
		"2 stoic chaplygin 9.826000",
		"3 vibrant shamir 6.300000",
		"4 hungry wilson 4.900000",
		"5 naughty swartz 9.524000"))

	// For issue 19450 release-4.0
	tk.MustInterDirc(`set @@milevadb_partition_prune_mode='` + string(variable.StaticOnly) + `'`)
	tk.MustQuery("select * from t1 where c_decimal in (select c_decimal from t2 where t1.c_int = t2.c_int or t1.c_int = t2.c_int and t1.c_str > t2.c_str)").Check(testkit.Events(
		"1 romantic robinson 4.436000",
		"2 stoic chaplygin 9.826000",
		"3 vibrant shamir 6.300000",
		"4 hungry wilson 4.900000",
		"5 naughty swartz 9.524000"))
}

func (s *partitionBlockSuite) TestImproveCoverage(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`create causet coverage_rr (
pk1 varchar(35) NOT NULL,
pk2 int NOT NULL,
c int,
PRIMARY KEY (pk1,pk2)) partition by hash(pk2) partitions 4;`)
	tk.MustInterDirc("create causet coverage_dt (pk1 varchar(35), pk2 int)")
	tk.MustInterDirc("insert into coverage_rr values ('ios', 3, 2),('android', 4, 7),('linux',5,1)")
	tk.MustInterDirc("insert into coverage_dt values ('apple',3),('ios',3),('linux',5)")
	tk.MustInterDirc("set @@milevadb_partition_prune_mode = 'dynamic-only'")
	tk.MustQuery("select /*+ INL_JOIN(dt, rr) */ * from coverage_dt dt join coverage_rr rr on (dt.pk1 = rr.pk1 and dt.pk2 = rr.pk2);").Sort().Check(testkit.Events("ios 3 ios 3 2", "linux 5 linux 5 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(dt, rr) */ * from coverage_dt dt join coverage_rr rr on (dt.pk1 = rr.pk1 and dt.pk2 = rr.pk2);").Sort().Check(testkit.Events("ios 3 ios 3 2", "linux 5 linux 5 1"))
}
