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
	"crypto/tls"
	"math"
	"time"

	dto "github.com/prometheus/client_perceptron/go"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	. "github.com/whtcorpsinc/check"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/ekvcache"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/types"
)

func (s *seqTestSuite) TestPrepared(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()

	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		var err error
		causetembedded.SetPreparedCausetCache(flag)

		tk := testkit.NewTestKit(c, s.causetstore)
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists prepare_test")
		tk.MustInterDirc("create causet prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustInterDirc("insert prepare_test (c1) values (1),(2),(NULL)")

		tk.MustInterDirc(`prepare stmt_test_1 from 'select id from prepare_test where id > ?'; set @a = 1; execute stmt_test_1 using @a;`)
		tk.MustInterDirc(`prepare stmt_test_2 from 'select 1'`)
		// Prepare multiple memex is not allowed.
		_, err = tk.InterDirc(`prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'`)
		c.Assert(interlock.ErrPrepareMulti.Equal(err), IsTrue)
		// The variable count does not match.
		_, err = tk.InterDirc(`prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?'; set @a = 1; execute stmt_test_4 using @a;`)
		c.Assert(causetembedded.ErrWrongParamCount.Equal(err), IsTrue)
		// Prepare and deallocate prepared memex immediately.
		tk.MustInterDirc(`prepare stmt_test_5 from 'select id from prepare_test where id > ?'; deallocate prepare stmt_test_5;`)

		// Statement not found.
		_, err = tk.InterDirc("deallocate prepare stmt_test_5")
		c.Assert(causetembedded.ErrStmtNotFound.Equal(err), IsTrue)

		// incorrect ALLEGROSQLs in prepare. issue #3738, ALLEGROALLEGROSQL in prepare stmt is parsed in DoPrepare.
		_, err = tk.InterDirc(`prepare p from "delete from t where a = 7 or 1=1/*' and b = 'p'";`)
		c.Assert(err.Error(), Equals, `[BerolinaSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use near '/*' and b = 'p'' at line 1`)

		// The `stmt_test5` should not be found.
		_, err = tk.InterDirc(`set @a = 1; execute stmt_test_5 using @a;`)
		c.Assert(causetembedded.ErrStmtNotFound.Equal(err), IsTrue)

		// Use parameter marker with argument will run prepared memex.
		result := tk.MustQuery("select distinct c1, c2 from prepare_test where c1 = ?", 1)
		result.Check(testkit.Events("1 <nil>"))

		// Call Stochastik PrepareStmt directly to get stmtID.
		query := "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err := tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		rs, err := tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("1 <nil>"))

		tk.MustInterDirc("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)

		tk1 := testkit.NewTestKit(c, s.causetstore)
		tk1.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk1.MustInterDirc("use test")
		tk1.MustInterDirc("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("3"))

		tk.MustInterDirc("delete from prepare_test")
		query = "select c1 from prepare_test where c1 = (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		rs, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		c.Assert(rs.Close(), IsNil)
		tk1.MustInterDirc("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("3"))

		tk.MustInterDirc("delete from prepare_test")
		query = "select c1 from prepare_test where c1 in (select c1 from prepare_test where c1 = ?)"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		rs, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		c.Assert(rs.Close(), IsNil)
		tk1.MustInterDirc("insert prepare_test (c1) values (3)")
		rs, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(3)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("3"))

		tk.MustInterDirc("begin")
		tk.MustInterDirc("insert prepare_test (c1) values (4)")
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		tk.MustInterDirc("rollback")
		rs, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(4)})
		c.Assert(err, IsNil)
		tk.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events())

		// Check that ast.Statement created by interlock.CompileInterDircutePreparedStmt has query text.
		stmt, err := interlock.CompileInterDircutePreparedStmt(context.TODO(), tk.Se, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
		c.Assert(stmt.OriginText(), Equals, query)

		// Check that rebuild plan works.
		tk.Se.PrepareTxnCtx(ctx)
		_, err = stmt.RebuildCauset(ctx)
		c.Assert(err, IsNil)
		rs, err = stmt.InterDirc(ctx)
		c.Assert(err, IsNil)
		req := rs.NewChunk()
		err = rs.Next(ctx, req)
		c.Assert(err, IsNil)
		c.Assert(rs.Close(), IsNil)

		// Make schemaReplicant change.
		tk.MustInterDirc("drop causet if exists prepare2")
		tk.InterDirc("create causet prepare2 (a int)")

		// Should success as the changed schemaReplicant do not affect the prepared memex.
		_, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)

		// Drop a defCausumn so the prepared memex become invalid.
		query = "select c1, c2 from prepare_test where c1 = ?"
		stmtID, _, _, err = tk.Se.PrepareStmt(query)
		c.Assert(err, IsNil)
		tk.MustInterDirc("alter causet prepare_test drop defCausumn c2")

		_, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(causetembedded.ErrUnknownDeferredCauset.Equal(err), IsTrue)

		tk.MustInterDirc("drop causet prepare_test")
		_, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(causetembedded.ErrSchemaChanged.Equal(err), IsTrue)

		// issue 3381
		tk.MustInterDirc("drop causet if exists prepare3")
		tk.MustInterDirc("create causet prepare3 (a decimal(1))")
		tk.MustInterDirc("prepare stmt from 'insert into prepare3 value(123)'")
		_, err = tk.InterDirc("execute stmt")
		c.Assert(err, NotNil)

		_, _, fields, err := tk.Se.PrepareStmt("select a from prepare3")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "prepare3")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select a from prepare3 where ?")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "prepare3")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select (1,1) in (select 1,1)")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "")
		c.Assert(fields[0].BlockAsName.L, Equals, "")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "(1,1) in (select 1,1)")

		_, _, fields, err = tk.Se.PrepareStmt("select a from prepare3 where a = (" +
			"select a from prepare2 where a = ?)")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "prepare3")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("select * from prepare3 as t1 join prepare3 as t2")
		c.Assert(err, IsNil)
		c.Assert(fields[0].DBName.L, Equals, "test")
		c.Assert(fields[0].BlockAsName.L, Equals, "t1")
		c.Assert(fields[0].DeferredCausetAsName.L, Equals, "a")
		c.Assert(fields[1].DBName.L, Equals, "test")
		c.Assert(fields[1].BlockAsName.L, Equals, "t2")
		c.Assert(fields[1].DeferredCausetAsName.L, Equals, "a")

		_, _, fields, err = tk.Se.PrepareStmt("uFIDelate prepare3 set a = ?")
		c.Assert(err, IsNil)
		c.Assert(len(fields), Equals, 0)

		// issue 8074
		tk.MustInterDirc("drop causet if exists prepare1;")
		tk.MustInterDirc("create causet prepare1 (a decimal(1))")
		tk.MustInterDirc("insert into prepare1 values(1);")
		_, err = tk.InterDirc("prepare stmt FROM @sql1")
		c.Assert(err.Error(), Equals, "[BerolinaSQL:1064]You have an error in your ALLEGROALLEGROSQL syntax; check the manual that corresponds to your MilevaDB version for the right syntax to use line 1 defCausumn 4 near \"NULL\" ")
		tk.MustInterDirc("SET @allegrosql = 'uFIDelate prepare1 set a=5 where a=?';")
		_, err = tk.InterDirc("prepare stmt FROM @allegrosql")
		c.Assert(err, IsNil)
		tk.MustInterDirc("set @var=1;")
		_, err = tk.InterDirc("execute stmt using @var")
		c.Assert(err, IsNil)
		tk.MustQuery("select a from prepare1;").Check(testkit.Events("5"))

		// issue 19371
		tk.MustInterDirc("SET @allegrosql = 'uFIDelate prepare1 set a=a+1';")
		_, err = tk.InterDirc("prepare stmt FROM @ALLEGROALLEGROSQL")
		c.Assert(err, IsNil)
		_, err = tk.InterDirc("execute stmt")
		c.Assert(err, IsNil)
		tk.MustQuery("select a from prepare1;").Check(testkit.Events("6"))
		_, err = tk.InterDirc("prepare stmt FROM @Sql")
		c.Assert(err, IsNil)
		_, err = tk.InterDirc("execute stmt")
		c.Assert(err, IsNil)
		tk.MustQuery("select a from prepare1;").Check(testkit.Events("7"))

		// Coverage.
		exec := &interlock.InterDircuteInterDirc{}
		exec.Next(ctx, nil)
		exec.Close()

		// issue 8065
		stmtID, _, _, err = tk.Se.PrepareStmt("select ? from dual")
		c.Assert(err, IsNil)
		_, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
		stmtID, _, _, err = tk.Se.PrepareStmt("uFIDelate prepare1 set a = ? where a = ?")
		c.Assert(err, IsNil)
		_, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1), types.NewCauset(1)})
		c.Assert(err, IsNil)
	}
}

func (s *seqTestSuite) TestPreparedLimitOffset(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	flags := []bool{false, true}
	ctx := context.Background()
	for _, flag := range flags {
		var err error
		causetembedded.SetPreparedCausetCache(flag)

		tk := testkit.NewTestKit(c, s.causetstore)
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists prepare_test")
		tk.MustInterDirc("create causet prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
		tk.MustInterDirc("insert prepare_test (c1) values (1),(2),(NULL)")
		tk.MustInterDirc(`prepare stmt_test_1 from 'select id from prepare_test limit ? offset ?'; set @a = 1, @b=1;`)
		r := tk.MustQuery(`execute stmt_test_1 using @a, @b;`)
		r.Check(testkit.Events("2"))

		tk.MustInterDirc(`set @a=1.1`)
		r = tk.MustQuery(`execute stmt_test_1 using @a, @b;`)
		r.Check(testkit.Events("2"))

		tk.MustInterDirc(`set @c="-1"`)
		_, err = tk.InterDirc("execute stmt_test_1 using @c, @c")
		c.Assert(causetembedded.ErrWrongArguments.Equal(err), IsTrue)

		stmtID, _, _, err := tk.Se.PrepareStmt("select id from prepare_test limit ?")
		c.Assert(err, IsNil)
		_, err = tk.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{types.NewCauset(1)})
		c.Assert(err, IsNil)
	}
}

func (s *seqTestSuite) TestPreparedNullParam(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists t")
		tk.MustInterDirc("create causet t (id int, KEY id (id))")
		tk.MustInterDirc("insert into t values (1), (2), (3)")
		tk.MustInterDirc(`prepare stmt from 'select * from t use index(id) where id = ?'`)

		r := tk.MustQuery(`execute stmt using @id;`)
		r.Check(nil)

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(nil)

		tk.MustInterDirc(`set @id="1"`)
		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))

		r = tk.MustQuery(`execute stmt using @id2;`)
		r.Check(nil)

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))
	}
}

func (s *seqTestSuite) TestPrepareWithAggregation(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists t")
		tk.MustInterDirc("create causet t (id int primary key)")
		tk.MustInterDirc("insert into t values (1), (2), (3)")
		tk.MustInterDirc(`prepare stmt from 'select sum(id) from t where id = ?'`)

		tk.MustInterDirc(`set @id="1"`)
		r := tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))

		r = tk.MustQuery(`execute stmt using @id;`)
		r.Check(testkit.Events("1"))
	}
}

func (s *seqTestSuite) TestPreparedIssue7579(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists t")
		tk.MustInterDirc("create causet t (a int, b int, index a_idx(a))")
		tk.MustInterDirc("insert into t values (1,1), (2,2), (null,3)")

		r := tk.MustQuery("select a, b from t order by b asc;")
		r.Check(testkit.Events("1 1", "2 2", "<nil> 3"))

		tk.MustInterDirc(`prepare stmt from 'select a, b from t where ? order by b asc'`)

		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)

		tk.MustInterDirc(`set @param = true`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 1", "2 2", "<nil> 3"))

		tk.MustInterDirc(`set @param = false`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)

		tk.MustInterDirc(`set @param = 1`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 1", "2 2", "<nil> 3"))

		tk.MustInterDirc(`set @param = 0`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(nil)
	}
}

func (s *seqTestSuite) TestPreparedInsert(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	metrics.ResetblockCausetCacheCounterFortTest = true
	metrics.CausetCacheCounter.Reset()
	counter := metrics.CausetCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists prepare_test")
		tk.MustInterDirc("create causet prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustInterDirc(`prepare stmt_insert from 'insert into prepare_test values (?, ?)'`)
		tk.MustInterDirc(`set @a=1,@b=1; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(0))
		}
		tk.MustInterDirc(`set @a=2,@b=2; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(1))
		}
		tk.MustInterDirc(`set @a=3,@b=3; execute stmt_insert using @a, @b;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(testkit.Events("1 1"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(testkit.Events("2 2"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(testkit.Events("3 3"))

		tk.MustInterDirc(`prepare stmt_insert_select from 'insert into prepare_test (id, c1) select id + 100, c1 + 100 from prepare_test where id = ?'`)
		tk.MustInterDirc(`set @a=1; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}
		tk.MustInterDirc(`set @a=2; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(3))
		}
		tk.MustInterDirc(`set @a=3; execute stmt_insert_select using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(4))
		}

		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 101)
		result.Check(testkit.Events("101 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 102)
		result.Check(testkit.Events("102 102"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 103)
		result.Check(testkit.Events("103 103"))
	}
}

func (s *seqTestSuite) TestPreparedUFIDelate(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	metrics.ResetblockCausetCacheCounterFortTest = true
	metrics.CausetCacheCounter.Reset()
	counter := metrics.CausetCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists prepare_test")
		tk.MustInterDirc("create causet prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustInterDirc(`insert into prepare_test values (1, 1)`)
		tk.MustInterDirc(`insert into prepare_test values (2, 2)`)
		tk.MustInterDirc(`insert into prepare_test values (3, 3)`)

		tk.MustInterDirc(`prepare stmt_uFIDelate from 'uFIDelate prepare_test set c1 = c1 + ? where id = ?'`)
		tk.MustInterDirc(`set @a=1,@b=100; execute stmt_uFIDelate using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}
		tk.MustInterDirc(`set @a=2,@b=200; execute stmt_uFIDelate using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(3))
		}
		tk.MustInterDirc(`set @a=3,@b=300; execute stmt_uFIDelate using @b,@a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(4))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(testkit.Events("1 101"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(testkit.Events("2 202"))
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(testkit.Events("3 303"))
	}
}

func (s *seqTestSuite) TestPreparedDelete(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	metrics.ResetblockCausetCacheCounterFortTest = true
	metrics.CausetCacheCounter.Reset()
	counter := metrics.CausetCacheCounter.WithLabelValues("prepare")
	pb := &dto.Metric{}
	flags := []bool{false, true}
	for _, flag := range flags {
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists prepare_test")
		tk.MustInterDirc("create causet prepare_test (id int PRIMARY KEY, c1 int)")
		tk.MustInterDirc(`insert into prepare_test values (1, 1)`)
		tk.MustInterDirc(`insert into prepare_test values (2, 2)`)
		tk.MustInterDirc(`insert into prepare_test values (3, 3)`)

		tk.MustInterDirc(`prepare stmt_delete from 'delete from prepare_test where id = ?'`)
		tk.MustInterDirc(`set @a=1; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(0))
		}
		tk.MustInterDirc(`set @a=2; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(1))
		}
		tk.MustInterDirc(`set @a=3; execute stmt_delete using @a;`)
		if flag {
			counter.Write(pb)
			hit := pb.GetCounter().GetValue()
			c.Check(hit, Equals, float64(2))
		}

		result := tk.MustQuery("select id, c1 from prepare_test where id = ?", 1)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 2)
		result.Check(nil)
		result = tk.MustQuery("select id, c1 from prepare_test where id = ?", 3)
		result.Check(nil)
	}
}

func (s *seqTestSuite) TestPrepareDealloc(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	causetembedded.SetPreparedCausetCache(true)

	tk := testkit.NewTestKit(c, s.causetstore)
	var err error
	tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
		PreparedCausetCache: ekvcache.NewSimpleLRUCache(3, 0.1, math.MaxUint64),
	})
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists prepare_test")
	tk.MustInterDirc("create causet prepare_test (id int PRIMARY KEY, c1 int)")

	c.Assert(tk.Se.PreparedCausetCache().Size(), Equals, 0)
	tk.MustInterDirc(`prepare stmt1 from 'select * from prepare_test'`)
	tk.MustInterDirc("execute stmt1")
	tk.MustInterDirc(`prepare stmt2 from 'select * from prepare_test'`)
	tk.MustInterDirc("execute stmt2")
	tk.MustInterDirc(`prepare stmt3 from 'select * from prepare_test'`)
	tk.MustInterDirc("execute stmt3")
	tk.MustInterDirc(`prepare stmt4 from 'select * from prepare_test'`)
	tk.MustInterDirc("execute stmt4")
	c.Assert(tk.Se.PreparedCausetCache().Size(), Equals, 3)

	tk.MustInterDirc("deallocate prepare stmt1")
	c.Assert(tk.Se.PreparedCausetCache().Size(), Equals, 3)
	tk.MustInterDirc("deallocate prepare stmt2")
	tk.MustInterDirc("deallocate prepare stmt3")
	tk.MustInterDirc("deallocate prepare stmt4")
	c.Assert(tk.Se.PreparedCausetCache().Size(), Equals, 0)
}

func (s *seqTestSuite) TestPreparedIssue8153(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		var err error
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists t")
		tk.MustInterDirc("create causet t (a int, b int)")
		tk.MustInterDirc("insert into t (a, b) values (1,3), (2,2), (3,1)")

		tk.MustInterDirc(`prepare stmt from 'select * from t order by ? asc'`)
		r := tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 3", "2 2", "3 1"))

		tk.MustInterDirc(`set @param = 1`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 3", "2 2", "3 1"))

		tk.MustInterDirc(`set @param = 2`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("3 1", "2 2", "1 3"))

		tk.MustInterDirc(`set @param = 3`)
		_, err = tk.InterDirc(`execute stmt using @param;`)
		c.Assert(err.Error(), Equals, "[causet:1054]Unknown defCausumn '?' in 'order clause'")

		tk.MustInterDirc(`set @param = '##'`)
		r = tk.MustQuery(`execute stmt using @param;`)
		r.Check(testkit.Events("1 3", "2 2", "3 1"))

		tk.MustInterDirc("insert into t (a, b) values (1,1), (1,2), (2,1), (2,3), (3,2), (3,3)")
		tk.MustInterDirc(`prepare stmt from 'select ?, sum(a) from t group by ?'`)

		tk.MustInterDirc(`set @a=1,@b=1`)
		r = tk.MustQuery(`execute stmt using @a,@b;`)
		r.Check(testkit.Events("1 18"))

		tk.MustInterDirc(`set @a=1,@b=2`)
		_, err = tk.InterDirc(`execute stmt using @a,@b;`)
		c.Assert(err.Error(), Equals, "[causet:1056]Can't group on 'sum(a)'")
	}
}

func (s *seqTestSuite) TestPreparedIssue8644(c *C) {
	orgEnable := causetembedded.PreparedCausetCacheEnabled()
	defer func() {
		causetembedded.SetPreparedCausetCache(orgEnable)
	}()
	flags := []bool{false, true}
	for _, flag := range flags {
		causetembedded.SetPreparedCausetCache(flag)
		tk := testkit.NewTestKit(c, s.causetstore)
		var err error
		tk.Se, err = stochastik.CreateStochastik4TestWithOpt(s.causetstore, &stochastik.Opt{
			PreparedCausetCache: ekvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
		})
		c.Assert(err, IsNil)

		tk.MustInterDirc("use test")
		tk.MustInterDirc("drop causet if exists t")
		tk.MustInterDirc("create causet t(data mediumblob)")

		tk.MustInterDirc(`prepare stmt1 from 'insert t (data) values (?)'`)

		tk.MustInterDirc(`set @a = 'a'`)
		tk.MustInterDirc(`execute stmt1 using @a;`)

		tk.MustInterDirc(`set @b = 'aaaaaaaaaaaaaaaaaa'`)
		tk.MustInterDirc(`execute stmt1 using @b;`)

		r := tk.MustQuery(`select * from t`)
		r.Check(testkit.Events("a", "aaaaaaaaaaaaaaaaaa"))
	}
}

// mockStochastikManager is a mocked stochastik manager which is used for test.
type mockStochastikManager1 struct {
	Se stochastik.Stochastik
}

// ShowProcessList implements the StochastikManager.ShowProcessList interface.
func (msm *mockStochastikManager1) ShowProcessList() map[uint64]*soliton.ProcessInfo {
	ret := make(map[uint64]*soliton.ProcessInfo)
	return ret
}

func (msm *mockStochastikManager1) GetProcessInfo(id uint64) (*soliton.ProcessInfo, bool) {
	pi := msm.Se.ShowProcess()
	return pi, true
}

// Kill implements the StochastikManager.Kill interface.
func (msm *mockStochastikManager1) Kill(cid uint64, query bool) {}

func (msm *mockStochastikManager1) UFIDelateTLSConfig(cfg *tls.Config) {}

func (s *seqTestSuite) TestPreparedIssue17419(c *C) {
	ctx := context.Background()
	tk := testkit.NewTestKit(c, s.causetstore)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int)")
	tk.MustInterDirc("insert into t (a) values (1), (2), (3)")

	tk1 := testkit.NewTestKit(c, s.causetstore)

	var err error
	tk1.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	tk1.GetConnectionID()

	query := "select * from test.t"
	stmtID, _, _, err := tk1.Se.PrepareStmt(query)
	c.Assert(err, IsNil)

	sm := &mockStochastikManager1{
		Se: tk1.Se,
	}
	tk1.Se.SetStochastikManager(sm)
	s.petri.ExpensiveQueryHandle().SetStochastikManager(sm)

	rs, err := tk1.Se.InterDircutePreparedStmt(ctx, stmtID, []types.Causet{})
	c.Assert(err, IsNil)
	tk1.ResultSetToResult(rs, Commentf("%v", rs)).Check(testkit.Events("1", "2", "3"))
	tk1.Se.SetProcessInfo("", time.Now(), allegrosql.ComStmtInterDircute, 0)

	s.petri.ExpensiveQueryHandle().LogOnQueryExceedMemQuota(tk.Se.GetStochastikVars().ConnectionID)

	// After entirely fixing https://github.com/whtcorpsinc/milevadb/issues/17419
	// c.Assert(tk1.Se.ShowProcess().Causet, NotNil)
	// _, ok := tk1.Se.ShowProcess().Causet.(*causetembedded.InterDircute)
	// c.Assert(ok, IsTrue)
}
