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
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testStatsSuite) TestDBSAfterLoad(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("create block t (c1 int, c2 int)")
	testKit.MustExec("analyze block t")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustExec("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustExec("analyze block t")
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	// add column
	testKit.MustExec("alter block t add column c10 int")
	is = do.SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()

	sc := new(stmtctx.StatementContext)
	count := statsTbl.DeferredCausetGreaterRowCount(sc, types.NewCauset(recordCount+1), blockInfo.DeferredCausets[0].ID)
	c.Assert(count, Equals, 0.0)
	count = statsTbl.DeferredCausetGreaterRowCount(sc, types.NewCauset(recordCount+1), blockInfo.DeferredCausets[2].ID)
	c.Assert(int(count), Equals, 333)
}

func (s *testStatsSuite) TestDBSTable(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("create block t (c1 int, c2 int)")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	h := do.StatsHandle()
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	statsTbl := h.GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("create block t1 (c1 int, c2 int, index idx(c1))")
	is = do.SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	statsTbl = h.GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("truncate block t1")
	is = do.SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	statsTbl = h.GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
}

func (s *testStatsSuite) TestDBSHistogram(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	do := s.do
	h := do.StatsHandle()

	testKit.MustExec("use test")
	testKit.MustExec("create block t (c1 int, c2 int)")
	<-h.DBSEventCh()
	testKit.MustExec("insert into t values(1,2),(3,4)")
	testKit.MustExec("analyze block t")

	testKit.MustExec("alter block t add column c_null int")
	err := h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	is := do.SchemaReplicant()
	c.Assert(h.UFIDelate(is), IsNil)
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	c.Check(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].NullCount, Equals, int64(2))
	c.Check(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].NDV, Equals, int64(0))

	testKit.MustExec("alter block t add column c3 int NOT NULL")
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	is = do.SchemaReplicant()
	c.Assert(h.UFIDelate(is), IsNil)
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	sc := new(stmtctx.StatementContext)
	count, err := statsTbl.DeferredCausetEqualRowCount(sc, types.NewIntCauset(0), blockInfo.DeferredCausets[3].ID)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(2))
	count, err = statsTbl.DeferredCausetEqualRowCount(sc, types.NewIntCauset(1), blockInfo.DeferredCausets[3].ID)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(0))

	testKit.MustExec("alter block t add column c4 datetime NOT NULL default CURRENT_TIMESTAMP")
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	is = do.SchemaReplicant()
	c.Assert(h.UFIDelate(is), IsNil)
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	// If we don't use original default value, we will get a pseudo block.
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("alter block t add column c5 varchar(15) DEFAULT '123'")
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	is = do.SchemaReplicant()
	c.Assert(h.UFIDelate(is), IsNil)
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	c.Check(statsTbl.DeferredCausets[blockInfo.DeferredCausets[5].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 3.0)

	testKit.MustExec("alter block t add column c6 varchar(15) DEFAULT '123', add column c7 varchar(15) DEFAULT '123'")
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	is = do.SchemaReplicant()
	c.Assert(h.UFIDelate(is), IsNil)
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	testKit.MustExec("create index i on t(c2, c1)")
	testKit.MustExec("analyze block t")
	rs := testKit.MustQuery("select count(*) from allegrosql.stats_histograms where block_id = ? and hist_id = 1 and is_index =1", blockInfo.ID)
	rs.Check(testkit.Rows("1"))
	rs = testKit.MustQuery("select count(*) from allegrosql.stats_buckets where block_id = ? and hist_id = 1 and is_index = 1", blockInfo.ID)
	rs.Check(testkit.Rows("2"))
}

func (s *testStatsSuite) TestDBSPartition(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	createTable := `CREATE TABLE t (a int, b int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	testKit.MustExec(createTable)
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	h := do.StatsHandle()
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	pi := blockInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(blockInfo, def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
	}

	testKit.MustExec("insert into t values (1,2),(6,2),(11,2),(16,2)")
	testKit.MustExec("analyze block t")
	testKit.MustExec("alter block t add column c varchar(15) DEFAULT '123'")
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	is = do.SchemaReplicant()
	c.Assert(h.UFIDelate(is), IsNil)
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	pi = blockInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(blockInfo, def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
		c.Check(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 3.0)
	}

	addPartition := "alter block t add partition (partition p4 values less than (26))"
	testKit.MustExec(addPartition)
	is = s.do.SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	pi = blockInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(blockInfo, def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
	}

	truncatePartition := "alter block t truncate partition p4"
	testKit.MustExec(truncatePartition)
	is = s.do.SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
	pi = blockInfo.GetPartitionInfo()
	for _, def := range pi.Definitions {
		statsTbl := h.GetPartitionStats(blockInfo, def.ID)
		c.Assert(statsTbl.Pseudo, IsFalse)
	}
}
