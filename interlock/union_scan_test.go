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

func (s *testSuite7) TestDirtyTransaction(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("set @@stochastik.milevadb_interlock_concurrency = 4;")
	tk.MustInterDirc("set @@stochastik.milevadb_hash_join_concurrency = 5;")
	tk.MustInterDirc("set @@stochastik.milevadb_allegrosql_scan_concurrency = 15;")

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int primary key, b int, index idx_b (b));")
	tk.MustInterDirc("insert t value (2, 3), (4, 8), (6, 8)")
	tk.MustInterDirc("begin")
	tk.MustQuery("select * from t").Check(testkit.Events("2 3", "4 8", "6 8"))
	tk.MustInterDirc("insert t values (1, 5), (3, 4), (7, 6)")
	tk.MustQuery("select * from information_schema.defCausumns")
	tk.MustQuery("select * from t").Check(testkit.Events("1 5", "2 3", "3 4", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t where a = 1").Check(testkit.Events("1 5"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Events("7 6", "6 8", "4 8", "3 4", "2 3", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Events("2 3", "3 4", "1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Events("6 8", "4 8", "7 6", "1 5", "3 4", "2 3"))
	tk.MustQuery("select b from t where b = 8 order by b desc").Check(testkit.Events("8", "8"))
	// Delete a snapshot event and a dirty event.
	tk.MustInterDirc("delete from t where a = 2 or a = 3")
	tk.MustQuery("select * from t").Check(testkit.Events("1 5", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Events("7 6", "6 8", "4 8", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Events("1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Events("6 8", "4 8", "7 6", "1 5"))
	// Add deleted event back.
	tk.MustInterDirc("insert t values (2, 3), (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Events("1 5", "2 3", "3 4", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Events("7 6", "6 8", "4 8", "3 4", "2 3", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Events("2 3", "3 4", "1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Events("6 8", "4 8", "7 6", "1 5", "3 4", "2 3"))
	// Truncate Block
	tk.MustInterDirc("truncate causet t")
	tk.MustQuery("select * from t").Check(testkit.Events())
	tk.MustInterDirc("insert t values (1, 2)")
	tk.MustQuery("select * from t").Check(testkit.Events("1 2"))
	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert t values (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Events("3 4"))
	tk.MustInterDirc("commit")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int)")
	tk.MustInterDirc("insert t values (2, 3), (4, 5), (6, 7)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert t values (0, 1)")
	tk.MustQuery("select * from t where b = 3").Check(testkit.Events("2 3"))
	tk.MustInterDirc("commit")

	tk.MustInterDirc(`drop causet if exists t;`)
	tk.MustInterDirc(`create causet t(a json, b bigint);`)
	tk.MustInterDirc(`begin;`)
	tk.MustInterDirc(`insert into t values("\"1\"", 1);`)
	tk.MustQuery(`select * from t`).Check(testkit.Events(`"1" 1`))
	tk.MustInterDirc(`commit;`)

	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc("create causet t(a int, b int, c int, d int, index idx(c, d))")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values(1, 2, 3, 4)")
	tk.MustQuery("select * from t use index(idx) where c > 1 and d = 4").Check(testkit.Events("1 2 3 4"))
	tk.MustInterDirc("commit")

	// Test partitioned causet use wrong causet ID.
	tk.MustInterDirc(`drop causet if exists t`)
	tk.MustInterDirc(`CREATE TABLE t (c1 smallint(6) NOT NULL, c2 char(5) DEFAULT NULL) PARTITION BY RANGE ( c1 ) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN (20),
			PARTITION p2 VALUES LESS THAN (30),
			PARTITION p3 VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values (1, 1)")
	tk.MustQuery("select * from t").Check(testkit.Events("1 1"))
	tk.MustQuery("select * from t where c1 < 5").Check(testkit.Events("1 1"))
	tk.MustQuery("select c2 from t").Check(testkit.Events("1"))
	tk.MustInterDirc("commit")

	// Test general virtual defCausumn
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a int, b int as (a+1), c int as (b+1), index(c));")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t values (1, default, default), (2, default, default), (3, default, default);")
	// BlockReader
	tk.MustQuery("select * from t;").Check(testkit.Events("1 2 3", "2 3 4", "3 4 5"))
	tk.MustQuery("select b from t;").Check(testkit.Events("2", "3", "4"))
	tk.MustQuery("select c from t;").Check(testkit.Events("3", "4", "5"))
	tk.MustQuery("select a from t;").Check(testkit.Events("1", "2", "3"))
	// IndexReader
	tk.MustQuery("select c from t where c > 3;").Check(testkit.Events("4", "5"))
	tk.MustQuery("select c from t order by c;").Check(testkit.Events("3", "4", "5"))
	// IndexLookup
	tk.MustQuery("select * from t where c > 3;").Check(testkit.Events("2 3 4", "3 4 5"))
	tk.MustQuery("select a, b from t use index(c) where c > 3;").Check(testkit.Events("2 3", "3 4"))
	tk.MustQuery("select a, c from t use index(c) where c > 3;").Check(testkit.Events("2 4", "3 5"))
	tk.MustQuery("select b, c from t use index(c) where c > 3;").Check(testkit.Events("3 4", "4 5"))
	// Delete and uFIDelate some data
	tk.MustInterDirc("delete from t where c > 4;")
	tk.MustQuery("select * from t;").Check(testkit.Events("1 2 3", "2 3 4"))
	tk.MustInterDirc("uFIDelate t set a = 3 where b > 1;")
	tk.MustQuery("select * from t;").Check(testkit.Events("3 4 5", "3 4 5"))
	tk.MustInterDirc("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Events("3 4 5", "3 4 5"))
	// Again with non-empty causet
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t values (1, default, default), (2, default, default), (3, default, default);")
	// BlockReader
	tk.MustQuery("select * from t;").Check(testkit.Events("3 4 5", "3 4 5", "1 2 3", "2 3 4", "3 4 5"))
	tk.MustQuery("select b from t;").Check(testkit.Events("4", "4", "2", "3", "4"))
	tk.MustQuery("select c from t;").Check(testkit.Events("3", "4", "5", "5", "5"))
	tk.MustQuery("select a from t;").Check(testkit.Events("3", "3", "1", "2", "3"))
	// IndexReader
	tk.MustQuery("select c from t where c > 3;").Check(testkit.Events("4", "5", "5", "5"))
	tk.MustQuery("select c from t order by c;").Check(testkit.Events("3", "4", "5", "5", "5"))
	// IndexLookup
	tk.MustQuery("select * from t where c > 3;").Check(testkit.Events("3 4 5", "3 4 5", "2 3 4", "3 4 5"))
	tk.MustQuery("select a, b from t use index(c) where c > 3;").Check(testkit.Events("2 3", "3 4", "3 4", "3 4"))
	tk.MustQuery("select a, c from t use index(c) where c > 3;").Check(testkit.Events("2 4", "3 5", "3 5", "3 5"))
	tk.MustQuery("select b, c from t use index(c) where c > 3;").Check(testkit.Events("3 4", "4 5", "4 5", "4 5"))
	// Delete and uFIDelate some data
	tk.MustInterDirc("delete from t where c > 4;")
	tk.MustQuery("select * from t;").Check(testkit.Events("1 2 3", "2 3 4"))
	tk.MustInterDirc("uFIDelate t set a = 3 where b > 2;")
	tk.MustQuery("select * from t;").Check(testkit.Events("1 2 3", "3 4 5"))
	tk.MustInterDirc("commit;")
}

func (s *testSuite7) TestUnionScanWithCastCondition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet ta (a varchar(20))")
	tk.MustInterDirc("insert ta values ('1'), ('2')")
	tk.MustInterDirc("create causet tb (a varchar(20))")
	tk.MustInterDirc("begin")
	tk.MustQuery("select * from ta where a = 1").Check(testkit.Events("1"))
	tk.MustInterDirc("insert tb values ('0')")
	tk.MustQuery("select * from ta where a = 1").Check(testkit.Events("1"))
	tk.MustInterDirc("rollback")
}

func (s *testSuite7) TestUnionScanForMemBufferReader(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int,b int, index idx(b))")
	tk.MustInterDirc("insert t values (1,1),(2,2)")

	// Test for delete in union scan
	tk.MustInterDirc("begin")
	tk.MustInterDirc("delete from t")
	tk.MustQuery("select * from t").Check(testkit.Events())
	tk.MustInterDirc("insert t values (1,1)")
	tk.MustQuery("select a,b from t").Check(testkit.Events("1 1"))
	tk.MustQuery("select a,b from t use index(idx)").Check(testkit.Events("1 1"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")

	// Test uFIDelate with untouched index defCausumns.
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert t values (1,1),(2,2)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate t set a=a+1")
	tk.MustQuery("select * from t").Check(testkit.Events("2 1", "3 2"))
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Events("2 1", "3 2"))
	tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Events("3 2", "2 1"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")

	// Test uFIDelate with index defCausumn.
	tk.MustQuery("select * from t").Check(testkit.Events("2 1", "3 2"))
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate t set b=b+1 where a=2")
	tk.MustQuery("select * from t").Check(testkit.Events("2 2", "3 2"))
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Events("2 2", "3 2"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")

	// Test index reader order.
	tk.MustQuery("select * from t").Check(testkit.Events("2 2", "3 2"))
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert t values (3,3),(1,1),(4,4),(-1,-1);")
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Events("-1 -1", "1 1", "2 2", "3 2", "3 3", "4 4"))
	tk.MustQuery("select b from t use index (idx) order by b desc").Check(testkit.Events("4", "3", "2", "2", "1", "-1"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")

	// test for uFIDelate unique index.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int,b int, unique index idx(b))")
	tk.MustInterDirc("insert t values (1,1),(2,2)")
	tk.MustInterDirc("begin")
	_, err := tk.InterDirc("uFIDelate t set b=b+1")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ekv:1062]Duplicate entry '2' for key 'idx'")
	// uFIDelate with unchange index defCausumn.
	tk.MustInterDirc("uFIDelate t set a=a+1")
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Events("2 1", "3 2"))
	tk.MustQuery("select b from t use index (idx)").Check(testkit.Events("1", "2"))
	tk.MustInterDirc("uFIDelate t set b=b+2 where a=2")
	tk.MustQuery("select * from t").Check(testkit.Events("2 3", "3 2"))
	tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Events("2 3", "3 2"))
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Events("3 2", "2 3"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")

	// Test for getMissIndexEventsByHandle return nil.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int,b int, index idx(a))")
	tk.MustInterDirc("insert into t values (1,1),(2,2),(3,3)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate t set b=0 where a=2")
	tk.MustQuery("select * from t ignore index (idx) where a>0 and b>0;").Check(testkit.Events("1 1", "3 3"))
	tk.MustQuery("select * from t use index (idx) where a>0 and b>0;").Check(testkit.Events("1 1", "3 3"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")

	// Test index lookup reader corner case.
	tk.MustInterDirc("drop causet if exists tt")
	tk.MustInterDirc("create causet tt (a bigint, b int,c int,primary key (a,b));")
	tk.MustInterDirc("insert into tt set a=1,b=1;")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("uFIDelate tt set c=1;")
	tk.MustQuery("select * from tt use index (PRIMARY) where c is not null;").Check(testkit.Events("1 1 1"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet tt")

	// Test index reader corner case.
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int,b int,primary key(a,b));")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t1 values(1, 1);")
	tk.MustQuery("select * from t1 use index(primary) where a=1;").Check(testkit.Events("1 1"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t1;")

	// Test index reader with pk handle.
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int unsigned key,b int,c varchar(10), index idx(b,a,c));")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t1 (a,b) values (0, 0), (1, 1);")
	tk.MustQuery("select a,b from t1 use index(idx) where b>0;").Check(testkit.Events("1 1"))
	tk.MustQuery("select a,b,c from t1 ignore index(idx) where a>=1 order by a desc").Check(testkit.Events("1 1 <nil>"))
	tk.MustInterDirc("insert into t1 values (2, 2, null), (3, 3, 'a');")
	tk.MustQuery("select a,b from t1 use index(idx) where b>1 and c is not null;").Check(testkit.Events("3 3"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t1;")

	// Test insert and uFIDelate with untouched index.
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int,b int,c int,index idx(b));")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t1 values (1, 1, 1), (2, 2, 2);")
	tk.MustInterDirc("uFIDelate t1 set c=c+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx);").Check(testkit.Events("1 1 2", "2 2 2"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t1;")

	// Test insert and uFIDelate with untouched unique index.
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int,b int,c int,unique index idx(b));")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t1 values (1, 1, 1), (2, 2, 2);")
	tk.MustInterDirc("uFIDelate t1 set c=c+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx);").Check(testkit.Events("1 1 2", "2 2 2"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t1;")

	// Test uFIDelate with 2 index, one untouched, the other index is touched.
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (a int,b int,c int,unique index idx1(a), index idx2(b));")
	tk.MustInterDirc("insert into t1 values (1, 1, 1);")
	tk.MustInterDirc("uFIDelate t1 set b=b+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx2);").Check(testkit.Events("1 2 1"))
	tk.MustInterDirc("admin check causet t1;")
}

func (s *testSuite7) TestForUFIDelateUntouchedIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")

	checkFunc := func() {
		tk.MustInterDirc("begin")
		tk.MustInterDirc("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key uFIDelate b = b + 1;")
		tk.MustInterDirc("commit")
		tk.MustInterDirc("admin check causet t")

		// Test for autocommit
		tk.MustInterDirc("set autocommit=0")
		tk.MustInterDirc("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key uFIDelate b = b + 1;")
		tk.MustInterDirc("set autocommit=1")
		tk.MustInterDirc("admin check causet t")
	}

	// Test for primary key.
	tk.MustInterDirc("create causet t (a varchar(10) primary key,b int)")
	checkFunc()

	// Test for unique key.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a varchar(10),b int, unique index(a))")
	checkFunc()

	// Test for on duplicate uFIDelate also conflict too.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int,b int, unique index(a))")
	tk.MustInterDirc("begin")
	_, err := tk.InterDirc("insert into t values (1, 1), (2, 2), (1, 3) on duplicate key uFIDelate a = a + 1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ekv:1062]Duplicate entry '2' for key 'a'")
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")
}

func (s *testSuite7) TestUFIDelateScanningHandles(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t(a int primary key, b int);")
	tk.MustInterDirc("begin")
	for i := 2; i < 100000; i++ {
		tk.MustInterDirc("insert into t values (?, ?)", i, i)
	}
	tk.MustInterDirc("commit;")

	tk.MustInterDirc("set milevadb_allegrosql_scan_concurrency = 1;")
	tk.MustInterDirc("set milevadb_index_lookup_join_concurrency = 1;")
	tk.MustInterDirc("set milevadb_projection_concurrency=1;")
	tk.MustInterDirc("set milevadb_init_chunk_size=1;")
	tk.MustInterDirc("set milevadb_max_chunk_size=32;")

	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values (1, 1);")
	tk.MustInterDirc("uFIDelate /*+ INL_JOIN(t1) */ t t1, (select a, b from t) t2 set t1.b = t2.b where t1.a = t2.a + 1000;")
	result := tk.MustQuery("select a, a-b from t where a > 1000 and a - b != 1000;")
	c.Assert(result.Events(), HasLen, 0)
	tk.MustInterDirc("rollback;")
}

// See https://github.com/whtcorpsinc/milevadb/issues/19136
func (s *testSuite7) TestForApplyAndUnionScan(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")

	tk.MustInterDirc("create causet t ( c_int int, c_str varchar(40), primary key(c_int, c_str) )")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values (1, 'amazing almeida'), (2, 'boring bardeen'), (3, 'busy wescoff')")
	tk.MustQuery("select c_int, (select t1.c_int from t t1 where t1.c_int = 3 and t1.c_int > t.c_int order by t1.c_int limit 1) x from t").Check(testkit.Events("1 3", "2 3", "3 <nil>"))
	tk.MustInterDirc("commit")
	tk.MustQuery("select c_int, (select t1.c_int from t t1 where t1.c_int = 3 and t1.c_int > t.c_int order by t1.c_int limit 1) x from t").Check(testkit.Events("1 3", "2 3", "3 <nil>"))

	// See https://github.com/whtcorpsinc/milevadb/issues/19435
	tk.MustInterDirc("drop causet if exists t, t1")
	tk.MustInterDirc("create causet t1(c_int int)")
	tk.MustInterDirc("create causet t(c_int int)")
	tk.MustInterDirc("insert into t values(1),(2),(3),(4),(5),(6),(7),(8),(9)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t1 values(18)")
	tk.MustQuery("select (select min(t1.c_int) from t1 where t1.c_int > t.c_int), (select max(t1.c_int) from t1 where t1.c_int> t.c_int), (select sum(t1.c_int) from t1 where t1.c_int> t.c_int) from t").Check(testkit.Events("18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18", "18 18 18"))
	tk.MustInterDirc("rollback")

	// See https://github.com/whtcorpsinc/milevadb/issues/19431
	tk.MustInterDirc("DROP TABLE IF EXISTS `t`")
	tk.MustInterDirc("CREATE TABLE `t` ( `c_int` int(11) NOT NULL, `c_str` varchar(40) NOT NULL, `c_datetime` datetime NOT NULL, PRIMARY KEY (`c_int`,`c_str`,`c_datetime`), KEY `c_str` (`c_str`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin; /*!40101 SET character_set_client = @saved_cs_client */")
	tk.MustInterDirc("INSERT INTO `t` VALUES (1,'cool pasteur','2020-04-21 19:01:04'),(3,'friendly stonebraker','2020-06-09 18:58:00'),(5,'happy shannon','2020-02-29 21:39:08'),(6,'competent torvalds','2020-05-24 04:18:45'),(7,'fervent kapitsa','2020-05-21 16:58:12'),(8,'quirky jennings','2020-03-12 12:52:58'),(9,'adoring swartz','2020-04-19 02:20:32'),(14,'intelligent keller','2020-01-08 09:47:42'),(15,'vibrant zhukovsky','2020-04-15 15:15:55'),(18,'keen chatterjee','2020-02-09 06:39:31'),(20,'elastic gauss','2020-03-01 13:34:06'),(21,'affectionate margulis','2020-06-20 10:20:29'),(27,'busy keldysh','2020-05-21 09:10:45'),(31,'flamboyant banach','2020-03-04 21:28:44'),(39,'keen banach','2020-06-09 03:07:57'),(41,'nervous gagarin','2020-06-12 23:43:04'),(47,'wonderful chebyshev','2020-04-15 14:51:17'),(50,'reverent brahmagupta','2020-06-25 21:50:52'),(52,'suspicious elbakyan','2020-05-28 04:55:34'),(55,'epic lichterman','2020-05-16 19:24:09'),(57,'determined taussig','2020-06-18 22:51:37')")
	tk.MustInterDirc("DROP TABLE IF EXISTS `t1`")
	tk.MustInterDirc("CREATE TABLE `t1` ( `c_int` int(11) DEFAULT NULL, `c_str` varchar(40) NOT NULL, `c_datetime` datetime DEFAULT NULL, PRIMARY KEY (`c_str`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustInterDirc("INSERT INTO `t1` VALUES (19,'nervous johnson','2020-05-04 13:15:19'),(22,'pedantic tu','2020-02-19 09:32:44'),(24,'wizardly robinson','2020-02-03 18:39:36'),(33,'eager stonebraker','2020-05-03 08:20:54'),(34,'zen taussig','2020-06-29 01:18:48'),(36,'epic ganguly','2020-04-23 17:25:13'),(38,'objective euclid','2020-05-21 01:04:27'),(40,'infallible hodgkin','2020-05-07 03:52:52'),(43,'wizardly hellman','2020-04-11 20:20:05'),(46,'inspiring hoover','2020-06-28 14:47:34'),(48,'amazing cerf','2020-05-15 08:04:32'),(49,'objective hermann','2020-04-25 18:01:06'),(51,'upbeat spence','2020-01-27 21:59:54'),(53,'hardembedded nightingale','2020-01-20 18:57:37'),(54,'silly hellman','2020-06-24 00:22:47'),(56,'elastic drisdefCausl','2020-02-27 22:46:57'),(58,'nifty buck','2020-03-12 03:56:16')")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values (59, 'suspicious feistel', '2020-01-29 19:52:14')")
	tk.MustInterDirc("insert into t1 values (60, 'practical thompson', '2020-03-25 04:33:10')")
	tk.MustQuery("select c_int, c_str from t where (select count(*) from t1 where t1.c_int in (t.c_int, t.c_int + 2, t.c_int + 10)) > 2").Check(testkit.Events())
	tk.MustInterDirc("rollback")
}
