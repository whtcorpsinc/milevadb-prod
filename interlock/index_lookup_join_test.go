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
	"fmt"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuite1) TestIndexLookupJoinHang(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet idxJoinOuter (a int unsigned)")
	tk.MustInterDirc("create causet idxJoinInner (a int unsigned unique)")
	tk.MustInterDirc("insert idxJoinOuter values (1), (1), (1), (1), (1)")
	tk.MustInterDirc("insert idxJoinInner values (1)")
	tk.Se.GetStochastikVars().IndexJoinBatchSize = 1
	tk.Se.GetStochastikVars().SetIndexLookupJoinConcurrency(1)

	rs, err := tk.InterDirc("select /*+ INL_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	for i := 0; i < 5; i++ {
		rs.Next(context.Background(), req)
	}
	rs.Close()

	rs, err = tk.InterDirc("select /*+ INL_HASH_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	for i := 0; i < 5; i++ {
		rs.Next(context.Background(), req)
	}
	rs.Close()

	rs, err = tk.InterDirc("select /*+ INL_MERGE_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	for i := 0; i < 5; i++ {
		rs.Next(context.Background(), req)
	}
	rs.Close()
}

func (s *testSuite1) TestIndexJoinUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t1(id int primary key, a int)")
	tk.MustInterDirc("create causet t2(id int primary key, a int, b int, key idx_a(a))")
	tk.MustInterDirc("insert into t2 values (1,1,1),(4,2,4)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 values(2,2)")
	tk.MustInterDirc("insert into t2 values(2,2,2), (3,3,3)")
	// BlockScan below UnionScan
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Events(
		"2 2 2 2 2",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Events(
		"2 2 2 2 2",
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Events(
		"2 2 2 2 2",
	))
	// IndexLookUp below UnionScan
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Events(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Events(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	// INL_MERGE_JOIN is invalid
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Sort().Check(testkit.Events(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	// IndexScan below UnionScan
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Events(
		"2 2",
		"2 2",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Events(
		"2 2",
		"2 2",
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Events(
		"2 2",
		"2 2",
	))
	tk.MustInterDirc("rollback")
}

func (s *testSuite1) TestBatchIndexJoinUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t1(id int primary key, a int)")
	tk.MustInterDirc("create causet t2(id int primary key, a int, key idx_a(a))")
	tk.MustInterDirc("set @@stochastik.milevadb_init_chunk_size=1")
	tk.MustInterDirc("set @@stochastik.milevadb_index_join_batch_size=1")
	tk.MustInterDirc("set @@stochastik.milevadb_index_lookup_join_concurrency=4")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 values(1,1),(2,1),(3,1),(4,1)")
	tk.MustInterDirc("insert into t2 values(1,1)")
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Events("4"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Events("4"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Events("4"))
	tk.MustInterDirc("rollback")
}

func (s *testSuite1) TestInapplicableIndexJoinHint(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc(`drop causet if exists t1, t2;`)
	tk.MustInterDirc(`create causet t1(a bigint, b bigint);`)
	tk.MustInterDirc(`create causet t2(a bigint, b bigint);`)
	tk.MustQuery(`select /*+ MilevaDB_INLJ(t1, t2) */ * from t1, t2;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1, t2) */ or /*+ MilevaDB_INLJ(t1, t2) */ is inapplicable without defCausumn equal ON condition`))
	tk.MustQuery(`select /*+ MilevaDB_INLJ(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1, t2) */ or /*+ MilevaDB_INLJ(t1, t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1, t2) */ * from t1, t2;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t1, t2) */ is inapplicable without defCausumn equal ON condition`))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t1, t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1, t2;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t1, t2) */ is inapplicable without defCausumn equal ON condition`))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t1, t2) */ is inapplicable`))

	tk.MustInterDirc(`drop causet if exists t1, t2;`)
	tk.MustInterDirc(`create causet t1(a bigint, b bigint, index idx_a(a));`)
	tk.MustInterDirc(`create causet t2(a bigint, b bigint);`)
	tk.MustQuery(`select /*+ MilevaDB_INLJ(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1) */ or /*+ MilevaDB_INLJ(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ MilevaDB_INLJ(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t2) */ or /*+ MilevaDB_INLJ(t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Events())
	tk.MustQuery(`show warnings;`).Check(testkit.Events(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t2) */ is inapplicable`))
}

func (s *testSuite) TestIndexJoinOverflow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc(`drop causet if exists t1, t2`)
	tk.MustInterDirc(`create causet t1(a int)`)
	tk.MustInterDirc(`insert into t1 values (-1)`)
	tk.MustInterDirc(`create causet t2(a int unsigned, index idx(a));`)
	tk.MustQuery(`select /*+ INL_JOIN(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Events())
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Events())
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Events())
}

func (s *testSuite5) TestIssue11061(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(c varchar(30), index ix_c(c(10)))")
	tk.MustInterDirc("insert into t1 (c) values('7_chars'), ('13_characters')")
	tk.MustQuery("SELECT /*+ INL_JOIN(t1) */ SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)").Check(testkit.Events("20"))
	tk.MustQuery("SELECT /*+ INL_HASH_JOIN(t1) */ SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)").Check(testkit.Events("20"))
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1) */ SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)").Check(testkit.Events("20"))
}

func (s *testSuite5) TestIndexJoinPartitionBlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int not null, c int, key idx(c)) partition by hash(b) partitions 30")
	tk.MustInterDirc("insert into t values(1, 27, 2)")
	tk.MustQuery("SELECT /*+ INL_JOIN(t1) */ count(1) FROM t t1 INNER JOIN (SELECT a, max(c) AS c FROM t WHERE b = 27 AND a = 1 GROUP BY a) t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.b = 27").Check(testkit.Events("1"))
	tk.MustQuery("SELECT /*+ INL_HASH_JOIN(t1) */ count(1) FROM t t1 INNER JOIN (SELECT a, max(c) AS c FROM t WHERE b = 27 AND a = 1 GROUP BY a) t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.b = 27").Check(testkit.Events("1"))
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1) */ count(1) FROM t t1 INNER JOIN (SELECT a, max(c) AS c FROM t WHERE b = 27 AND a = 1 GROUP BY a) t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.b = 27").Check(testkit.Events("1"))
}

func (s *testSuite5) TestIndexJoinMultiCondition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1, t2")
	tk.MustInterDirc("create causet t1(a int not null, b int not null, key idx_a_b(a,b))")
	tk.MustInterDirc("create causet t2(a int not null, b int not null)")
	tk.MustInterDirc("insert into t1 values (0,1), (0,2), (0,3)")
	tk.MustInterDirc("insert into t2 values (0,1), (0,2), (0,3)")
	tk.MustQuery("select /*+ MilevaDB_INLJ(t1) */ count(*) from t1, t2 where t1.a = t2.a and t1.b < t2.b").Check(testkit.Events("3"))
}

func (s *testSuite5) TestIssue16887(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists admin_roles, admin_role_has_permissions")
	tk.MustInterDirc("CREATE TABLE `admin_role_has_permissions` (`permission_id` bigint(20) unsigned NOT NULL, `role_id` bigint(20) unsigned NOT NULL, PRIMARY KEY (`permission_id`,`role_id`), KEY `admin_role_has_permissions_role_id_foreign` (`role_id`))")
	tk.MustInterDirc("CREATE TABLE `admin_roles` (`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL COMMENT '角色名称', `created_at` timestamp NULL DEFAULT NULL, `uFIDelated_at` timestamp NULL DEFAULT NULL, PRIMARY KEY (`id`))")
	tk.MustInterDirc("INSERT INTO `admin_roles` (`id`, `name`, `created_at`, `uFIDelated_at`) VALUES(1, 'admin','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(2, 'developer','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(3, 'analyst','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(4, 'channel_admin','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(5, 'test','2020-04-27 02:40:08', '2020-04-27 02:40:08')")
	tk.MustInterDirc("INSERT INTO `admin_role_has_permissions` (`permission_id`, `role_id`) VALUES(1, 1),(2, 1),(3, 1),(4, 1),(5, 1),(6, 1),(7, 1),(8, 1),(9, 1),(10, 1),(11, 1),(12, 1),(13, 1),(14, 1),(15, 1),(16, 1),(17, 1),(18, 1),(19, 1),(20, 1),(21, 1),(22, 1),(23, 1),(24, 1),(25, 1),(26, 1),(27, 1),(28, 1),(29, 1),(30, 1),(31, 1),(32, 1),(33, 1),(34, 1),(35, 1),(36, 1),(37, 1),(38, 1),(39, 1),(40, 1),(41, 1),(42, 1),(43, 1),(44, 1),(45, 1),(46, 1),(47, 1),(48, 1),(49, 1),(50, 1),(51, 1),(52, 1),(53, 1),(54, 1),(55, 1),(56, 1),(57, 1),(58, 1),(59, 1),(60, 1),(61, 1),(62, 1),(63, 1),(64, 1),(65, 1),(66, 1),(67, 1),(68, 1),(69, 1),(70, 1),(71, 1),(72, 1),(73, 1),(74, 1),(75, 1),(76, 1),(77, 1),(78, 1),(79, 1),(80, 1),(81, 1),(82, 1),(83, 1),(5, 4),(6, 4),(7, 4),(84, 5),(85, 5),(86, 5)")
	rows := tk.MustQuery("SELECT /*+ inl_merge_join(admin_role_has_permissions) */ `admin_roles`.* FROM `admin_roles` INNER JOIN `admin_role_has_permissions` ON `admin_roles`.`id` = `admin_role_has_permissions`.`role_id` WHERE `admin_role_has_permissions`.`permission_id`\n IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67)").Events()
	c.Assert(len(rows), Equals, 70)
	rows = tk.MustQuery("show warnings").Events()
	c.Assert(len(rows) > 0, Equals, true)
}

func (s *testSuite5) TestIndexJoinEnumSetIssue19233(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("drop causet if exists i;")
	tk.MustInterDirc("drop causet if exists p1;")
	tk.MustInterDirc("drop causet if exists p2;")
	tk.MustInterDirc(`CREATE TABLE p1 (type enum('HOST_PORT') NOT NULL, UNIQUE KEY (type)) ;`)
	tk.MustInterDirc(`CREATE TABLE p2 (type set('HOST_PORT') NOT NULL, UNIQUE KEY (type)) ;`)
	tk.MustInterDirc(`CREATE TABLE i (objectType varchar(64) NOT NULL);`)
	tk.MustInterDirc(`insert into i values ('SWITCH');`)
	tk.MustInterDirc(`create causet t like i;`)
	tk.MustInterDirc(`insert into t values ('HOST_PORT');`)
	tk.MustInterDirc(`insert into t select * from t;`)
	tk.MustInterDirc(`insert into t select * from t;`)
	tk.MustInterDirc(`insert into t select * from t;`)
	tk.MustInterDirc(`insert into t select * from t;`)
	tk.MustInterDirc(`insert into t select * from t;`)
	tk.MustInterDirc(`insert into t select * from t;`)

	tk.MustInterDirc(`insert into i select * from t;`)

	tk.MustInterDirc(`insert into p1 values('HOST_PORT');`)
	tk.MustInterDirc(`insert into p2 values('HOST_PORT');`)
	for _, causet := range []string{"p1", "p2"} {
		for _, hint := range []string{"INL_HASH_JOIN", "INL_MERGE_JOIN", "INL_JOIN"} {
			allegrosql := fmt.Sprintf(`select /*+ %s(%s) */ * from i, %s where i.objectType = %s.type;`, hint, causet, causet, causet)
			rows := tk.MustQuery(allegrosql).Events()
			c.Assert(len(rows), Equals, 64)
			for i := 0; i < len(rows); i++ {
				c.Assert(fmt.Sprint(rows[i][0]), Equals, "HOST_PORT")
			}
			rows = tk.MustQuery("show warnings").Events()
			c.Assert(len(rows), Equals, 0)
		}
	}
}

func (s *testSuite5) TestIssue19411(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t1  (c_int int, primary key (c_int))")
	tk.MustInterDirc("create causet t2  (c_int int, primary key (c_int)) partition by hash (c_int) partitions 4")
	tk.MustInterDirc("insert into t1 values (1)")
	tk.MustInterDirc("insert into t2 values (1)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 values (2)")
	tk.MustInterDirc("insert into t2 values (2)")
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */ * from t1 left join t2 on t1.c_int = t2.c_int").Check(testkit.Events(
		"1 1",
		"2 2"))
	tk.MustInterDirc("commit")
}
