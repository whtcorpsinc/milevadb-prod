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

package core_test

import (
	"context"
	"math"
	"strconv"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/kvcache"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_perceptron/go"
)

var _ = Suite(&testPrepareSuite{})
var _ = SerialSuites(&testPrepareSerialSuite{})

type testPrepareSuite struct {
}

type testPrepareSerialSuite struct {
}

func (s *testPrepareSerialSuite) TestPrepareCache(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int primary key, b int, c int, index idx1(b, a), index idx2(b))")
	tk.MustInterDirc("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 1, 2)")
	tk.MustInterDirc(`prepare stmt1 from "select * from t use index(idx1) where a = ? and b = ?"`)
	tk.MustInterDirc(`prepare stmt2 from "select a, b from t use index(idx2) where b = ?"`)
	tk.MustInterDirc(`prepare stmt3 from "select * from t where a = ?"`)
	tk.MustInterDirc("set @a=1, @b=1")
	// When executing one memex at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt2 using @b").Check(testkit.Rows("1 1", "6 1"))
	tk.MustQuery("execute stmt2 using @b").Check(testkit.Rows("1 1", "6 1"))
	tk.MustQuery("execute stmt3 using @a").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt3 using @a").Check(testkit.Rows("1 1 1"))
	tk.MustInterDirc(`prepare stmt4 from "select * from t where a > ?"`)
	tk.MustInterDirc("set @a=3")
	tk.MustQuery("execute stmt4 using @a").Check(testkit.Rows("4 4 4", "5 5 5", "6 1 2"))
	tk.MustQuery("execute stmt4 using @a").Check(testkit.Rows("4 4 4", "5 5 5", "6 1 2"))
	tk.MustInterDirc(`prepare stmt5 from "select c from t order by c"`)
	tk.MustQuery("execute stmt5").Check(testkit.Rows("1", "2", "2", "3", "4", "5"))
	tk.MustQuery("execute stmt5").Check(testkit.Rows("1", "2", "2", "3", "4", "5"))
	tk.MustInterDirc(`prepare stmt6 from "select distinct a from t order by a"`)
	tk.MustQuery("execute stmt6").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
	tk.MustQuery("execute stmt6").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))

	// test privilege change
	rootSe := tk.Se
	tk.MustInterDirc("drop causet if exists tp")
	tk.MustInterDirc(`create causet tp(c1 int, c2 int, primary key (c1))`)
	tk.MustInterDirc(`insert into tp values(1, 1), (2, 2), (3, 3)`)

	tk.MustInterDirc(`create user 'u_tp'@'localhost'`)
	tk.MustInterDirc(`grant select on test.tp to u_tp@'localhost';`)

	// user u_tp
	userSess := newStochastik(c, causetstore, "test")
	c.Assert(userSess.Auth(&auth.UserIdentity{Username: "u_tp", Hostname: "localhost"}, nil, nil), IsTrue)
	mustInterDirc(c, userSess, `prepare ps_stp_r from 'select * from tp where c1 > ?'`)
	mustInterDirc(c, userSess, `set @p2 = 2`)
	tk.Se = userSess
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))

	// root revoke
	tk.Se = rootSe
	tk.MustInterDirc(`revoke all on test.tp from 'u_tp'@'localhost';`)

	// user u_tp
	tk.Se = userSess
	_, err = tk.InterDirc(`execute ps_stp_r using @p2`)
	c.Assert(err, NotNil)

	// grant again
	tk.Se = rootSe
	tk.MustInterDirc(`grant select on test.tp to u_tp@'localhost';`)

	// user u_tp
	tk.Se = userSess
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))
	tk.MustQuery(`execute ps_stp_r using @p2`).Check(testkit.Rows("3 3"))

	// restore
	tk.Se = rootSe
	tk.MustInterDirc("drop causet if exists tp")
	tk.MustInterDirc(`DROP USER 'u_tp'@'localhost';`)
}

func (s *testPrepareSerialSuite) TestPrepareCacheIndexScan(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int, primary key (a, b))")
	tk.MustInterDirc("insert into t values(1, 1, 2), (1, 2, 3), (1, 3, 3), (2, 1, 2), (2, 2, 3), (2, 3, 3)")
	tk.MustInterDirc(`prepare stmt1 from "select a, c from t where a = ? and c = ?"`)
	tk.MustInterDirc("set @a=1, @b=3")
	// When executing one memex at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 3", "1 3"))
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 3", "1 3"))
}

func (s *testCausetSerialSuite) TestPrepareCacheDeferredFunction(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (id int PRIMARY KEY, c1 TIMESTAMP(3) NOT NULL DEFAULT '2020-01-14 10:43:20', KEY idx1 (c1))")
	tk.MustInterDirc("prepare sel1 from 'select id, c1 from t1 where c1 < now(3)'")

	sql1 := "execute sel1"
	expectedPattern := `IndexReader\(Index\(t1.idx1\)\[\[-inf,[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9].[0-9][0-9][0-9]\)\]\)`

	var cnt [2]float64
	var planStr [2]string
	metrics.ResetblockCausetCacheCounterFortTest = true
	metrics.CausetCacheCounter.Reset()
	counter := metrics.CausetCacheCounter.WithLabelValues("prepare")
	ctx := context.TODO()
	for i := 0; i < 2; i++ {
		stmt, err := s.ParseOneStmt(sql1, "", "")
		c.Check(err, IsNil)
		is := tk.Se.GetStochastikVars().TxnCtx.SchemaReplicant.(schemareplicant.SchemaReplicant)
		builder := core.NewCausetBuilder(tk.Se, is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Check(err, IsNil)
		execCauset, ok := p.(*core.InterDircute)
		c.Check(ok, IsTrue)
		interlock.ResetContextOfStmt(tk.Se, stmt)
		err = execCauset.OptimizePreparedCauset(ctx, tk.Se, is)
		c.Check(err, IsNil)
		planStr[i] = core.ToString(execCauset.Causet)
		c.Check(planStr[i], Matches, expectedPattern, Commentf("for %dth %s", i, sql1))
		pb := &dto.Metric{}
		counter.Write(pb)
		cnt[i] = pb.GetCounter().GetValue()
		c.Check(cnt[i], Equals, float64(i))
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(planStr[0] < planStr[1], IsTrue, Commentf("plan 1: %v, plan 2: %v", planStr[0], planStr[1]))
}

func (s *testPrepareSerialSuite) TestPrepareCacheNow(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc(`prepare stmt1 from "select now(), current_timestamp(), utc_timestamp(), unix_timestamp(), sleep(0.1), now(), current_timestamp(), utc_timestamp(), unix_timestamp()"`)
	// When executing one memex at the first time, we don't usTestPrepareCacheDeferredFunctione cache, so we need to execute it at least twice to test the cache.
	_ = tk.MustQuery("execute stmt1").Rows()
	rs := tk.MustQuery("execute stmt1").Rows()
	c.Assert(rs[0][0].(string), Equals, rs[0][5].(string))
	c.Assert(rs[0][1].(string), Equals, rs[0][6].(string))
	c.Assert(rs[0][2].(string), Equals, rs[0][7].(string))
	c.Assert(rs[0][3].(string), Equals, rs[0][8].(string))
}

func (s *testPrepareSerialSuite) TestPrepareOverMaxPreparedStmtCount(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")

	// test prepare and deallocate.
	prePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	tk.MustInterDirc(`prepare stmt1 from "select 1"`)
	onePrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared+1, Equals, onePrepared)
	tk.MustInterDirc(`deallocate prepare stmt1`)
	deallocPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared, Equals, deallocPrepared)

	// test change global limit and make it affected in test stochastik.
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("-1"))
	tk.MustInterDirc("set @@global.max_prepared_stmt_count = 2")
	tk.MustQuery("select @@global.max_prepared_stmt_count").Check(testkit.Rows("2"))

	// Disable global variable cache, so load global stochastik variable take effect immediate.
	dom.GetGlobalVarsCache().Disable()

	// test close stochastik to give up all prepared stmt
	tk.MustInterDirc(`prepare stmt2 from "select 1"`)
	prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
	tk.Se.Close()
	drawPrepared := readGaugeInt(metrics.PreparedStmtGauge)
	c.Assert(prePrepared-1, Equals, drawPrepared)

	// test meet max limit.
	tk.Se = nil
	tk.MustQuery("select @@max_prepared_stmt_count").Check(testkit.Rows("2"))
	for i := 1; ; i++ {
		prePrepared = readGaugeInt(metrics.PreparedStmtGauge)
		if prePrepared >= 2 {
			_, err = tk.InterDirc(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
			c.Assert(terror.ErrorEqual(err, variable.ErrMaxPreparedStmtCountReached), IsTrue)
			break
		}
		tk.InterDirc(`prepare stmt` + strconv.Itoa(i) + ` from "select 1"`)
	}
}

// unit test for issue https://github.com/whtcorpsinc/milevadb/issues/8518
func (s *testPrepareSerialSuite) TestPrepareBlockAsNameOnGroupByWithCache(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)

	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc(`create causet t1 (
		id int(11) unsigned not null primary key auto_increment,
		partner_id varchar(35) not null,
		t1_status_id int(10) unsigned
	  );`)
	tk.MustInterDirc(`insert into t1 values ("1", "partner1", "10"), ("2", "partner2", "10"), ("3", "partner3", "10"), ("4", "partner4", "10");`)
	tk.MustInterDirc("drop causet if exists t3")
	tk.MustInterDirc(`create causet t3 (
		id int(11) not null default '0',
		preceding_id int(11) not null default '0',
		primary key  (id,preceding_id)
	  );`)
	tk.MustInterDirc(`prepare stmt from 'SELECT DISTINCT t1.partner_id
	FROM t1
		LEFT JOIN t3 ON t1.id = t3.id
		LEFT JOIN t1 pp ON pp.id = t3.preceding_id
	GROUP BY t1.id ;'`)
	tk.MustQuery("execute stmt").Sort().Check(testkit.Rows("partner1", "partner2", "partner3", "partner4"))
}

func readGaugeInt(g prometheus.Gauge) int {
	ch := make(chan prometheus.Metric, 1)
	g.DefCauslect(ch)
	m := <-ch
	mm := &dto.Metric{}
	m.Write(mm)
	return int(mm.GetGauge().GetValue())
}

// unit test for issue https://github.com/whtcorpsinc/milevadb/issues/9478
func (s *testPrepareSuite) TestPrepareWithWindowFunction(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("set @@milevadb_enable_window_function = 1")
	defer func() {
		tk.MustInterDirc("set @@milevadb_enable_window_function = 0")
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet window_prepare(a int, b double)")
	tk.MustInterDirc("insert into window_prepare values(1, 1.1), (2, 1.9)")
	tk.MustInterDirc("prepare stmt1 from 'select row_number() over() from window_prepare';")
	// Test the unnamed window can be executed successfully.
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1", "2"))
	// Test the stmt can be prepared successfully.
	tk.MustInterDirc("prepare stmt2 from 'select count(a) over (order by a rows between ? preceding and ? preceding) from window_prepare'")
	tk.MustInterDirc("set @a=0, @b=1;")
	tk.MustQuery("execute stmt2 using @a, @b").Check(testkit.Rows("0", "0"))
}

func (s *testPrepareSuite) TestPrepareForGroupByItems(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(id int, v int)")
	tk.MustInterDirc("insert into t(id, v) values(1, 2),(1, 2),(2, 3);")
	tk.MustInterDirc("prepare s1 from 'select max(v) from t group by floor(id/?)';")
	tk.MustInterDirc("set @a=2;")
	tk.MustQuery("execute s1 using @a;").Sort().Check(testkit.Rows("2", "3"))

	tk.MustInterDirc("prepare s1 from 'select max(v) from t group by ?';")
	tk.MustInterDirc("set @a=2;")
	err = tk.InterDircToErr("execute s1 using @a;")
	c.Assert(err.Error(), Equals, "Unknown column '2' in 'group memex'")
	tk.MustInterDirc("set @a=2.0;")
	tk.MustQuery("execute s1 using @a;").Check(testkit.Rows("3"))
}

func (s *testPrepareSerialSuite) TestPrepareCacheForPartition(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)

	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	for _, val := range []string{string(variable.StaticOnly), string(variable.DynamicOnly)} {
		tk.MustInterDirc("set @@milevadb_partition_prune_mode = '" + val + "'")
		// Test for PointGet and IndexRead.
		tk.MustInterDirc("drop causet if exists t_index_read")
		tk.MustInterDirc("create causet t_index_read (id int, k int, c varchar(10), primary key (id, k)) partition by hash(id+k) partitions 10")
		tk.MustInterDirc("insert into t_index_read values (1, 2, 'abc'), (3, 4, 'def'), (5, 6, 'xyz')")
		tk.MustInterDirc("prepare stmt1 from 'select c from t_index_read where id = ? and k = ?;'")
		tk.MustInterDirc("set @id=1, @k=2")
		// When executing one memex at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
		tk.MustQuery("execute stmt1 using @id, @k").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt1 using @id, @k").Check(testkit.Rows("abc"))
		tk.MustInterDirc("set @id=5, @k=6")
		tk.MustQuery("execute stmt1 using @id, @k").Check(testkit.Rows("xyz"))
		tk.MustInterDirc("prepare stmt2 from 'select c from t_index_read where id = ? and k = ? and 1 = 1;'")
		tk.MustInterDirc("set @id=1, @k=2")
		tk.MustQuery("execute stmt2 using @id, @k").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt2 using @id, @k").Check(testkit.Rows("abc"))
		tk.MustInterDirc("set @id=5, @k=6")
		tk.MustQuery("execute stmt2 using @id, @k").Check(testkit.Rows("xyz"))
		// Test for BlockScan.
		tk.MustInterDirc("drop causet if exists t_block_read")
		tk.MustInterDirc("create causet t_block_read (id int, k int, c varchar(10), primary key(id)) partition by hash(id) partitions 10")
		tk.MustInterDirc("insert into t_block_read values (1, 2, 'abc'), (3, 4, 'def'), (5, 6, 'xyz')")
		tk.MustInterDirc("prepare stmt3 from 'select c from t_index_read where id = ?;'")
		tk.MustInterDirc("set @id=1")
		// When executing one memex at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
		tk.MustQuery("execute stmt3 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt3 using @id").Check(testkit.Rows("abc"))
		tk.MustInterDirc("set @id=5")
		tk.MustQuery("execute stmt3 using @id").Check(testkit.Rows("xyz"))
		tk.MustInterDirc("prepare stmt4 from 'select c from t_index_read where id = ? and k = ?'")
		tk.MustInterDirc("set @id=1, @k=2")
		tk.MustQuery("execute stmt4 using @id, @k").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt4 using @id, @k").Check(testkit.Rows("abc"))
		tk.MustInterDirc("set @id=5, @k=6")
		tk.MustQuery("execute stmt4 using @id, @k").Check(testkit.Rows("xyz"))
		// Query on range partition blocks should not raise error.
		tk.MustInterDirc("drop causet if exists t_range_index")
		tk.MustInterDirc("create causet t_range_index (id int, k int, c varchar(10), primary key(id)) partition by range(id) ( PARTITION p0 VALUES LESS THAN (4), PARTITION p1 VALUES LESS THAN (14),PARTITION p2 VALUES LESS THAN (20) )")
		tk.MustInterDirc("insert into t_range_index values (1, 2, 'abc'), (5, 4, 'def'), (13, 6, 'xyz'), (17, 6, 'hij')")
		tk.MustInterDirc("prepare stmt5 from 'select c from t_range_index where id = ?'")
		tk.MustInterDirc("set @id=1")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("abc"))
		tk.MustInterDirc("set @id=5")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("def"))
		tk.MustInterDirc("set @id=13")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("xyz"))
		tk.MustInterDirc("set @id=17")
		tk.MustQuery("execute stmt5 using @id").Check(testkit.Rows("hij"))

		tk.MustInterDirc("drop causet if exists t_range_block")
		tk.MustInterDirc("create causet t_range_block (id int, k int, c varchar(10)) partition by range(id) ( PARTITION p0 VALUES LESS THAN (4), PARTITION p1 VALUES LESS THAN (14),PARTITION p2 VALUES LESS THAN (20) )")
		tk.MustInterDirc("insert into t_range_block values (1, 2, 'abc'), (5, 4, 'def'), (13, 6, 'xyz'), (17, 6, 'hij')")
		tk.MustInterDirc("prepare stmt6 from 'select c from t_range_block where id = ?'")
		tk.MustInterDirc("set @id=1")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("abc"))
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("abc"))
		tk.MustInterDirc("set @id=5")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("def"))
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("def"))
		tk.MustInterDirc("set @id=13")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("xyz"))
		tk.MustInterDirc("set @id=17")
		tk.MustQuery("execute stmt6 using @id").Check(testkit.Rows("hij"))
	}
}

func newStochastik(c *C, causetstore ekv.CausetStorage, dbName string) stochastik.Stochastik {
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	mustInterDirc(c, se, "create database if not exists "+dbName)
	mustInterDirc(c, se, "use "+dbName)
	return se
}

func mustInterDirc(c *C, se stochastik.Stochastik, allegrosql string) {
	_, err := se.InterDircute(context.Background(), allegrosql)
	c.Assert(err, IsNil)
}

func (s *testPrepareSerialSuite) TestConstPropAndPFIDelWithCache(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)

	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a varchar(8) not null, b varchar(8) not null)")
	tk.MustInterDirc("insert into t values('1','1')")

	tk.MustInterDirc(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t2.b = ? and t2.b = ?"`)
	tk.MustInterDirc("set @p0 = '1', @p1 = '2';")
	tk.MustQuery("execute stmt using @p0, @p1").Check(testkit.Rows(
		"0",
	))
	tk.MustInterDirc("set @p0 = '1', @p1 = '1'")
	tk.MustQuery("execute stmt using @p0, @p1").Check(testkit.Rows(
		"1",
	))

	tk.MustInterDirc(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and ?"`)
	tk.MustInterDirc("set @p0 = 0")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustInterDirc("set @p0 = 1")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))

	tk.MustInterDirc(`prepare stmt from "select count(1) from t t1, t t2 where ?"`)
	tk.MustInterDirc("set @p0 = 0")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustInterDirc("set @p0 = 1")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))

	tk.MustInterDirc(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t2.b = '1' and t2.b = ?"`)
	tk.MustInterDirc("set @p0 = '1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))
	tk.MustInterDirc("set @p0 = '2'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))

	tk.MustInterDirc(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t1.a > ?"`)
	tk.MustInterDirc("set @p0 = '1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustInterDirc("set @p0 = '0'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"1",
	))

	tk.MustInterDirc(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t1.b > ? and t1.b > ?"`)
	tk.MustInterDirc("set @p0 = '0', @p1 = '0'")
	tk.MustQuery("execute stmt using @p0,@p1").Check(testkit.Rows(
		"1",
	))
	tk.MustInterDirc("set @p0 = '0', @p1 = '1'")
	tk.MustQuery("execute stmt using @p0,@p1").Check(testkit.Rows(
		"0",
	))

	tk.MustInterDirc(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t1.b > ? and t1.b > '1'"`)
	tk.MustInterDirc("set @p0 = '1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
	tk.MustInterDirc("set @p0 = '0'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows(
		"0",
	))
}

func (s *testCausetSerialSuite) TestCausetCacheUnionScan(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	pb := &dto.Metric{}
	metrics.ResetblockCausetCacheCounterFortTest = true
	metrics.CausetCacheCounter.Reset()
	counter := metrics.CausetCacheCounter.WithLabelValues("prepare")

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("drop causet if exists t2")
	tk.MustInterDirc("create causet t1(a int not null)")
	tk.MustInterDirc("create causet t2(a int not null)")
	tk.MustInterDirc("prepare stmt1 from 'select * from t1 where a > ?'")
	tk.MustInterDirc("set @p0 = 0")
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	tk.MustInterDirc("begin")
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt := pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(1))
	tk.MustInterDirc("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(1))
	tk.MustInterDirc("insert into t2 values(1)")
	// Cached plan is chosen, modification on t2 does not impact plan of t1.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows(
		"1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(2))
	tk.MustInterDirc("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt1 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(3))

	tk.MustInterDirc("prepare stmt2 from 'select * from t1 left join t2 on true where t1.a > ?'")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	tk.MustInterDirc("begin")
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	tk.MustInterDirc("insert into t1 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 <nil>",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	tk.MustInterDirc("insert into t2 values(1)")
	// Cached plan is invalid now, it is not chosen and removed.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(4))
	// Cached plan is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows(
		"1 1",
	))
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(5))
	tk.MustInterDirc("rollback")
	// Though cached plan contains UnionScan, it does not impact correctness, so it is reused.
	tk.MustQuery("execute stmt2 using @p0").Check(testkit.Rows())
	counter.Write(pb)
	cnt = pb.GetCounter().GetValue()
	c.Check(cnt, Equals, float64(6))
}

func (s *testCausetSerialSuite) TestCausetCacheHitInfo(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)

	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(id int)")
	tk.MustInterDirc("insert into t values (1),(2),(3),(4)")
	tk.MustInterDirc("prepare stmt from 'select * from t where id=?'")
	tk.MustInterDirc("prepare stmt2 from 'select /*+ ignore_plan_cache() */ * from t where id=?'")
	tk.MustInterDirc("set @doma = 1")
	// Test if last_plan_from_cache is appropriately initialized.
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	// Test if last_plan_from_cache is uFIDelated after a plan cache hit.
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt2 using @doma").Check(testkit.Rows("1"))
	// Test if last_plan_from_cache is uFIDelated after a plan cache miss caused by a prepared memex.
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	// Test if last_plan_from_cache is uFIDelated after a plan cache miss caused by a usual memex.
	tk.MustQuery("execute stmt using @doma").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where id=1").Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func (s *testCausetSerialSuite) TestCausetCacheUnsignedHandleOverflow(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)

	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a bigint unsigned primary key)")
	tk.MustInterDirc("insert into t values(18446744073709551615)")
	tk.MustInterDirc("prepare stmt from 'select a from t where a=?'")
	tk.MustInterDirc("set @p = 1")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustInterDirc("set @p = 18446744073709551615")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func (s *testCausetSerialSuite) TestIssue18066(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	orgEnable := core.PreparedCausetCacheEnabled()
	defer func() {
		dom.Close()
		causetstore.Close()
		core.SetPreparedCausetCache(orgEnable)
	}()
	core.SetPreparedCausetCache(true)

	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(causetstore, &stochastik.Opt{
		PreparedCausetCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)
	tk.GetConnectionID()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("prepare stmt from 'select * from t'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.memexs_summary where digest_text='select * from t'").Check(
		testkit.Rows("1 0 0"))
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.memexs_summary where digest_text='select * from t'").Check(
		testkit.Rows("2 1 1"))
	tk.MustInterDirc("prepare stmt from 'select * from t'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.memexs_summary where digest_text='select * from t'").Check(
		testkit.Rows("3 1 0"))
}

func (s *testPrepareSuite) TestPrepareForGroupByMultiItems(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, b int, c int , index idx(a));")
	tk.MustInterDirc("insert into t values(1,2, -1), (1,2, 1), (1,2, -1), (4,4,3);")
	tk.MustInterDirc("set @a=1")
	tk.MustInterDirc("set @b=3")
	tk.MustInterDirc(`set sql_mode=""`)
	tk.MustInterDirc(`prepare stmt from "select a, sum(b), c from t group by ?, ? order by ?, ?"`)
	tk.MustQuery("select a, sum(b), c from t group by 1,3 order by 1,3;").Check(testkit.Rows("1 4 -1", "1 2 1", "4 4 3"))
	tk.MustQuery(`execute stmt using @a, @b, @a, @b`).Check(testkit.Rows("1 4 -1", "1 2 1", "4 4 3"))

	tk.MustInterDirc("set @c=10")
	err = tk.InterDircToErr("execute stmt using @a, @c, @a, @c")
	c.Assert(err.Error(), Equals, "Unknown column '10' in 'group memex'")

	tk.MustInterDirc("set @v1=1.0")
	tk.MustInterDirc("set @v2=3.0")
	tk.MustInterDirc(`prepare stmt2 from "select sum(b) from t group by ?, ?"`)
	tk.MustQuery(`execute stmt2 using @v1, @v2`).Check(testkit.Rows("10"))
}

func (s *testPrepareSuite) TestInvisibleIndex(c *C) {
	defer testleak.AfterTest(c)()
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int, unique idx_a(a))")
	tk.MustInterDirc("insert into t values(1)")
	tk.MustInterDirc(`prepare stmt1 from "select a from t order by a"`)

	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")

	tk.MustInterDirc("alter causet t alter index idx_a invisible")
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustInterDirc("alter causet t alter index idx_a visible")
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt1").Check(testkit.Rows("1"))
	c.Assert(len(tk.Se.GetStochastikVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")
}
