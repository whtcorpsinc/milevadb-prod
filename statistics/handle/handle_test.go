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
	"fmt"
	"math"
	"testing"
	"time"
	"unsafe"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func cleanEnv(c *C, causetstore ekv.CausetStorage, do *petri.Petri) {
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustInterDirc("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		tk.MustInterDirc(fmt.Sprintf("drop causet %v", blockName))
	}
	tk.MustInterDirc("delete from allegrosql.stats_spacetime")
	tk.MustInterDirc("delete from allegrosql.stats_histograms")
	tk.MustInterDirc("delete from allegrosql.stats_buckets")
	tk.MustInterDirc("delete from allegrosql.stats_extended")
	do.StatsHandle().Clear()
}

func (s *testStatsSuite) TestStatsCache(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 int, c2 int)")
	testKit.MustInterDirc("insert into t values(1, 2)")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustInterDirc("analyze causet t")
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	testKit.MustInterDirc("create index idx_t on t(c1)")
	do.SchemaReplicant()
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	// If index is build, but stats is not uFIDelated. statsTbl can also work.
	c.Assert(statsTbl.Pseudo, IsFalse)
	// But the added index will not work.
	c.Assert(statsTbl.Indices[int64(1)], IsNil)

	testKit.MustInterDirc("analyze causet t")
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	// If the new schemaReplicant drop a column, the causet stats can still work.
	testKit.MustInterDirc("alter causet t drop column c2")
	is = do.SchemaReplicant()
	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schemaReplicant add a column, the causet stats can still work.
	testKit.MustInterDirc("alter causet t add column c10 int")
	is = do.SchemaReplicant()

	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
}

func (s *testStatsSuite) TestStatsCacheMemTracker(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 int, c2 int,c3 int)")
	testKit.MustInterDirc("insert into t values(1, 2, 3)")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsTrue)
	testKit.MustInterDirc("analyze causet t")

	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.MemoryUsage() > 0, IsTrue)
	c.Assert(do.StatsHandle().GetAllTableStatsMemUsage(), Equals, do.StatsHandle().GetMemConsumed())

	statsTbl = do.StatsHandle().GetTableStats(blockInfo)

	c.Assert(statsTbl.Pseudo, IsFalse)
	testKit.MustInterDirc("create index idx_t on t(c1)")
	do.SchemaReplicant()
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)

	// If index is build, but stats is not uFIDelated. statsTbl can also work.
	c.Assert(statsTbl.Pseudo, IsFalse)
	// But the added index will not work.
	c.Assert(statsTbl.Indices[int64(1)], IsNil)

	testKit.MustInterDirc("analyze causet t")
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)

	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schemaReplicant drop a column, the causet stats can still work.
	testKit.MustInterDirc("alter causet t drop column c2")
	is = do.SchemaReplicant()
	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)

	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.MemoryUsage() > 0, IsTrue)
	c.Assert(do.StatsHandle().GetAllTableStatsMemUsage(), Equals, do.StatsHandle().GetMemConsumed())
	c.Assert(statsTbl.Pseudo, IsFalse)

	// If the new schemaReplicant add a column, the causet stats can still work.
	testKit.MustInterDirc("alter causet t add column c10 int")
	is = do.SchemaReplicant()

	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.Pseudo, IsFalse)
	c.Assert(do.StatsHandle().GetAllTableStatsMemUsage(), Equals, do.StatsHandle().GetMemConsumed())
}

func assertTableEqual(c *C, a *statistics.Block, b *statistics.Block) {
	c.Assert(a.Count, Equals, b.Count)
	c.Assert(a.ModifyCount, Equals, b.ModifyCount)
	c.Assert(len(a.DeferredCausets), Equals, len(b.DeferredCausets))
	for i := range a.DeferredCausets {
		c.Assert(a.DeferredCausets[i].Count, Equals, b.DeferredCausets[i].Count)
		c.Assert(statistics.HistogramEqual(&a.DeferredCausets[i].Histogram, &b.DeferredCausets[i].Histogram, false), IsTrue)
		if a.DeferredCausets[i].CMSketch == nil {
			c.Assert(b.DeferredCausets[i].CMSketch, IsNil)
		} else {
			c.Assert(a.DeferredCausets[i].CMSketch.Equal(b.DeferredCausets[i].CMSketch), IsTrue)
		}
	}
	c.Assert(len(a.Indices), Equals, len(b.Indices))
	for i := range a.Indices {
		c.Assert(statistics.HistogramEqual(&a.Indices[i].Histogram, &b.Indices[i].Histogram, false), IsTrue)
		if a.DeferredCausets[i].CMSketch == nil {
			c.Assert(b.DeferredCausets[i].CMSketch, IsNil)
		} else {
			c.Assert(a.DeferredCausets[i].CMSketch.Equal(b.DeferredCausets[i].CMSketch), IsTrue)
		}
	}
}

func (s *testStatsSuite) TestStatsStoreAndLoad(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 int, c2 int)")
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		testKit.MustInterDirc("insert into t values (?, ?)", i, i+1)
	}
	testKit.MustInterDirc("create index idx_t on t(c2)")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()

	testKit.MustInterDirc("analyze causet t")
	statsTbl1 := do.StatsHandle().GetTableStats(blockInfo)

	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)
	statsTbl2 := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	c.Assert(statsTbl2.Count, Equals, int64(recordCount))
	assertTableEqual(c, statsTbl1, statsTbl2)
}

func (s *testStatsSuite) TestEmptyTable(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 int, c2 int, key cc1(c1), key cc2(c2))")
	testKit.MustInterDirc("analyze causet t")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	sc := new(stmtctx.StatementContext)
	count := statsTbl.DeferredCausetGreaterRowCount(sc, types.NewCauset(1), blockInfo.DeferredCausets[0].ID)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsSuite) TestDeferredCausetIDs(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 int, c2 int)")
	testKit.MustInterDirc("insert into t values(1, 2)")
	testKit.MustInterDirc("analyze causet t")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	sc := new(stmtctx.StatementContext)
	count := statsTbl.DeferredCausetLessRowCount(sc, types.NewCauset(2), blockInfo.DeferredCausets[0].ID)
	c.Assert(count, Equals, float64(1))

	// Drop a column and the offset changed,
	testKit.MustInterDirc("alter causet t drop column c1")
	is = do.SchemaReplicant()
	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	// At that time, we should get c2's stats instead of c1's.
	count = statsTbl.DeferredCausetLessRowCount(sc, types.NewCauset(2), blockInfo.DeferredCausets[0].ID)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsSuite) TestAvgDefCausLen(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 int, c2 varchar(100), c3 float, c4 datetime, c5 varchar(100))")
	testKit.MustInterDirc("insert into t values(1, '1234567', 12.3, '2020-03-07 19:00:57', NULL)")
	testKit.MustInterDirc("analyze causet t")
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[0].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 1.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[0].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[0].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, 8.0)

	// The size of varchar type is LEN + BYTE, here is 1 + 7 = 8
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[1].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[3].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[1].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, 8.0-3)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[3].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[1].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, 8.0-3+8)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[3].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[4].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[4].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, 0.0)
	testKit.MustInterDirc("insert into t values(132, '123456789112', 1232.3, '2020-03-07 19:17:29', NULL)")
	testKit.MustInterDirc("analyze causet t")
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[0].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 1.5)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[1].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 10.5)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[3].ID].AvgDefCausSize(statsTbl.Count, false), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[0].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[1].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, math.Round((10.5-math.Log2(10.5))*100)/100)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[3].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[0].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[1].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, math.Round((10.5-math.Log2(10.5))*100)/100+8)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[2].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(float32(12.3))))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[3].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, float64(unsafe.Sizeof(types.ZeroTime)))
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[4].ID].AvgDefCausSizeChunkFormat(statsTbl.Count), Equals, 8.0)
	c.Assert(statsTbl.DeferredCausets[blockInfo.DeferredCausets[4].ID].AvgDefCausSizeListInDisk(statsTbl.Count), Equals, 0.0)
}

func (s *testStatsSuite) TestDurationToTS(c *C) {
	tests := []time.Duration{time.Millisecond, time.Second, time.Minute, time.Hour}
	for _, t := range tests {
		ts := handle.DurationToTS(t)
		c.Assert(oracle.ExtractPhysical(ts)*int64(time.Millisecond), Equals, int64(t))
	}
}

func (s *testStatsSuite) TestVersion(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t1 (c1 int, c2 int)")
	testKit.MustInterDirc("analyze causet t1")
	do := s.do
	is := do.SchemaReplicant()
	tbl1, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t1"))
	c.Assert(err, IsNil)
	blockInfo1 := tbl1.Meta()
	h := handle.NewHandle(testKit.Se, time.Millisecond)
	unit := oracle.ComposeTS(1, 0)
	testKit.MustInterDirc("uFIDelate allegrosql.stats_spacetime set version = ? where block_id = ?", 2*unit, blockInfo1.ID)

	c.Assert(h.UFIDelate(is), IsNil)
	c.Assert(h.LastUFIDelateVersion(), Equals, 2*unit)
	statsTbl1 := h.GetTableStats(blockInfo1)
	c.Assert(statsTbl1.Pseudo, IsFalse)

	testKit.MustInterDirc("create causet t2 (c1 int, c2 int)")
	testKit.MustInterDirc("analyze causet t2")
	is = do.SchemaReplicant()
	tbl2, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t2"))
	c.Assert(err, IsNil)
	blockInfo2 := tbl2.Meta()
	// A smaller version write, and we can still read it.
	testKit.MustInterDirc("uFIDelate allegrosql.stats_spacetime set version = ? where block_id = ?", unit, blockInfo2.ID)
	c.Assert(h.UFIDelate(is), IsNil)
	c.Assert(h.LastUFIDelateVersion(), Equals, 2*unit)
	statsTbl2 := h.GetTableStats(blockInfo2)
	c.Assert(statsTbl2.Pseudo, IsFalse)

	testKit.MustInterDirc("insert t1 values(1,2)")
	testKit.MustInterDirc("analyze causet t1")
	offset := 3 * unit
	testKit.MustInterDirc("uFIDelate allegrosql.stats_spacetime set version = ? where block_id = ?", offset+4, blockInfo1.ID)
	c.Assert(h.UFIDelate(is), IsNil)
	c.Assert(h.LastUFIDelateVersion(), Equals, offset+uint64(4))
	statsTbl1 = h.GetTableStats(blockInfo1)
	c.Assert(statsTbl1.Count, Equals, int64(1))

	testKit.MustInterDirc("insert t2 values(1,2)")
	testKit.MustInterDirc("analyze causet t2")
	// A smaller version write, and we can still read it.
	testKit.MustInterDirc("uFIDelate allegrosql.stats_spacetime set version = ? where block_id = ?", offset+3, blockInfo2.ID)
	c.Assert(h.UFIDelate(is), IsNil)
	c.Assert(h.LastUFIDelateVersion(), Equals, offset+uint64(4))
	statsTbl2 = h.GetTableStats(blockInfo2)
	c.Assert(statsTbl2.Count, Equals, int64(1))

	testKit.MustInterDirc("insert t2 values(1,2)")
	testKit.MustInterDirc("analyze causet t2")
	// A smaller version write, and we cannot read it. Because at this time, lastThree Version is 4.
	testKit.MustInterDirc("uFIDelate allegrosql.stats_spacetime set version = 1 where block_id = ?", blockInfo2.ID)
	c.Assert(h.UFIDelate(is), IsNil)
	c.Assert(h.LastUFIDelateVersion(), Equals, offset+uint64(4))
	statsTbl2 = h.GetTableStats(blockInfo2)
	c.Assert(statsTbl2.Count, Equals, int64(1))

	// We add an index and analyze it, but DBS doesn't load.
	testKit.MustInterDirc("alter causet t2 add column c3 int")
	testKit.MustInterDirc("analyze causet t2")
	// load it with old schemaReplicant.
	c.Assert(h.UFIDelate(is), IsNil)
	statsTbl2 = h.GetTableStats(blockInfo2)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	c.Assert(statsTbl2.DeferredCausets[int64(3)], IsNil)
	// Next time DBS uFIDelated.
	is = do.SchemaReplicant()
	c.Assert(h.UFIDelate(is), IsNil)
	statsTbl2 = h.GetTableStats(blockInfo2)
	c.Assert(statsTbl2.Pseudo, IsFalse)
	// We can read it without analyze again! Thanks for PrevLastVersion.
	c.Assert(statsTbl2.DeferredCausets[int64(3)], NotNil)
}

func (s *testStatsSuite) TestLoadHist(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t (c1 varchar(12), c2 char(12))")
	do := s.do
	h := do.StatsHandle()
	err := h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	rowCount := 10
	for i := 0; i < rowCount; i++ {
		testKit.MustInterDirc("insert into t values('a','ddd')")
	}
	testKit.MustInterDirc("analyze causet t")
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	oldStatsTbl := h.GetTableStats(blockInfo)
	for i := 0; i < rowCount; i++ {
		testKit.MustInterDirc("insert into t values('bb','sdfga')")
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	h.UFIDelate(do.SchemaReplicant())
	newStatsTbl := h.GetTableStats(blockInfo)
	// The stats causet is uFIDelated.
	c.Assert(oldStatsTbl == newStatsTbl, IsFalse)
	// Only the TotDefCausSize of histograms is uFIDelated.
	for id, hist := range oldStatsTbl.DeferredCausets {
		c.Assert(hist.TotDefCausSize, Less, newStatsTbl.DeferredCausets[id].TotDefCausSize)

		temp := hist.TotDefCausSize
		hist.TotDefCausSize = newStatsTbl.DeferredCausets[id].TotDefCausSize
		c.Assert(statistics.HistogramEqual(&hist.Histogram, &newStatsTbl.DeferredCausets[id].Histogram, false), IsTrue)
		hist.TotDefCausSize = temp

		c.Assert(hist.CMSketch.Equal(newStatsTbl.DeferredCausets[id].CMSketch), IsTrue)
		c.Assert(hist.Count, Equals, newStatsTbl.DeferredCausets[id].Count)
		c.Assert(hist.Info, Equals, newStatsTbl.DeferredCausets[id].Info)
	}
	// Add column c3, we only uFIDelate c3.
	testKit.MustInterDirc("alter causet t add column c3 int")
	err = h.HandleDBSEvent(<-h.DBSEventCh())
	c.Assert(err, IsNil)
	is = do.SchemaReplicant()
	tbl, err = is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo = tbl.Meta()
	c.Assert(h.UFIDelate(is), IsNil)
	newStatsTbl2 := h.GetTableStats(blockInfo)
	c.Assert(newStatsTbl2 == newStatsTbl, IsFalse)
	// The histograms is not uFIDelated.
	for id, hist := range newStatsTbl.DeferredCausets {
		c.Assert(hist, Equals, newStatsTbl2.DeferredCausets[id])
	}
	c.Assert(newStatsTbl2.DeferredCausets[int64(3)].LastUFIDelateVersion, Greater, newStatsTbl2.DeferredCausets[int64(1)].LastUFIDelateVersion)
}

func (s *testStatsSuite) TestInitStats(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustInterDirc("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,7,8)")
	testKit.MustInterDirc("analyze causet t")
	h := s.do.StatsHandle()
	is := s.do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	// `UFIDelate` will not use load by need strategy when `Lease` is 0, and `InitStats` is only called when
	// `Lease` is not 0, so here we just change it.
	h.SetLease(time.Millisecond)

	h.Clear()
	c.Assert(h.InitStats(is), IsNil)
	block0 := h.GetTableStats(tbl.Meta())
	defcaus := block0.DeferredCausets
	c.Assert(defcaus[1].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x36))
	c.Assert(defcaus[2].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x37))
	c.Assert(defcaus[3].LastAnalyzePos.GetBytes()[0], Equals, uint8(0x38))
	h.Clear()
	c.Assert(h.UFIDelate(is), IsNil)
	block1 := h.GetTableStats(tbl.Meta())
	assertTableEqual(c, block0, block1)
	h.SetLease(0)
}

func (s *testStatsSuite) TestLoadStats(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t(a int, b int, c int, primary key(a), key idx(b))")
	testKit.MustInterDirc("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := s.do.StatsHandle().Lease()
	s.do.StatsHandle().SetLease(1)
	defer func() {
		s.do.StatsHandle().SetLease(oriLease)
	}()
	testKit.MustInterDirc("analyze causet t")

	is := s.do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	h := s.do.StatsHandle()
	stat := h.GetTableStats(blockInfo)
	hg := stat.DeferredCausets[blockInfo.DeferredCausets[0].ID].Histogram
	c.Assert(hg.Len(), Greater, 0)
	cms := stat.DeferredCausets[blockInfo.DeferredCausets[0].ID].CMSketch
	c.Assert(cms, IsNil)
	hg = stat.Indices[blockInfo.Indices[0].ID].Histogram
	c.Assert(hg.Len(), Greater, 0)
	cms = stat.Indices[blockInfo.Indices[0].ID].CMSketch
	c.Assert(cms.TotalCount(), Greater, uint64(0))
	hg = stat.DeferredCausets[blockInfo.DeferredCausets[2].ID].Histogram
	c.Assert(hg.Len(), Equals, 0)
	cms = stat.DeferredCausets[blockInfo.DeferredCausets[2].ID].CMSketch
	c.Assert(cms, IsNil)
	_, err = stat.DeferredCausetEqualRowCount(testKit.Se.GetStochastikVars().StmtCtx, types.NewIntCauset(1), blockInfo.DeferredCausets[2].ID)
	c.Assert(err, IsNil)
	c.Assert(h.LoadNeededHistograms(), IsNil)
	stat = h.GetTableStats(blockInfo)
	hg = stat.DeferredCausets[blockInfo.DeferredCausets[2].ID].Histogram
	c.Assert(hg.Len(), Greater, 0)
	// Following test tests whether the LoadNeededHistograms would panic.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/statistics/handle/mockGetStatsReaderFail", `return(true)`), IsNil)
	err = h.LoadNeededHistograms()
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/statistics/handle/mockGetStatsReaderFail"), IsNil)
}

func newStoreWithBootstrap() (ekv.CausetStorage, *petri.Petri, error) {
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	petri.RunAutoAnalyze = false
	do, err := stochastik.BootstrapStochastik(causetstore)
	do.SetStatsUFIDelating(true)
	return causetstore, do, errors.Trace(err)
}

func (s *testStatsSuite) TestCorrelation(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustInterDirc("use test")
	testKit.MustInterDirc("create causet t(c1 int primary key, c2 int)")
	testKit.MustInterDirc("insert into t values(1,1),(3,12),(4,20),(2,7),(5,21)")
	testKit.MustInterDirc("analyze causet t")
	result := testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")
	testKit.MustInterDirc("insert into t values(8,18)")
	testKit.MustInterDirc("analyze causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "0.828571")

	testKit.MustInterDirc("truncate causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 0)
	testKit.MustInterDirc("insert into t values(1,21),(3,12),(4,7),(2,20),(5,1)")
	testKit.MustInterDirc("analyze causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "-1")
	testKit.MustInterDirc("insert into t values(8,4)")
	testKit.MustInterDirc("analyze causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "-0.942857")

	testKit.MustInterDirc("truncate causet t")
	testKit.MustInterDirc("insert into t values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),(11,1),(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,2),(21,2),(22,2),(23,2),(24,2),(25,2)")
	testKit.MustInterDirc("analyze causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")

	testKit.MustInterDirc("drop causet t")
	testKit.MustInterDirc("create causet t(c1 int, c2 int)")
	testKit.MustInterDirc("insert into t values(1,1),(2,7),(3,12),(4,20),(5,21),(8,18)")
	testKit.MustInterDirc("analyze causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "1")
	c.Assert(result.Rows()[1][9], Equals, "0.828571")

	testKit.MustInterDirc("truncate causet t")
	testKit.MustInterDirc("insert into t values(1,1),(2,7),(3,12),(8,18),(4,20),(5,21)")
	testKit.MustInterDirc("analyze causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't'").Sort()
	c.Assert(len(result.Rows()), Equals, 2)
	c.Assert(result.Rows()[0][9], Equals, "0.828571")
	c.Assert(result.Rows()[1][9], Equals, "1")

	testKit.MustInterDirc("drop causet t")
	testKit.MustInterDirc("create causet t(c1 int primary key, c2 int, c3 int, key idx_c2(c2))")
	testKit.MustInterDirc("insert into t values(1,1,1),(2,2,2),(3,3,3)")
	testKit.MustInterDirc("analyze causet t")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 0").Sort()
	c.Assert(len(result.Rows()), Equals, 3)
	c.Assert(result.Rows()[0][9], Equals, "0")
	c.Assert(result.Rows()[1][9], Equals, "1")
	c.Assert(result.Rows()[2][9], Equals, "1")
	result = testKit.MustQuery("show stats_histograms where Table_name = 't' and Is_index = 1").Sort()
	c.Assert(len(result.Rows()), Equals, 1)
	c.Assert(result.Rows()[0][9], Equals, "0")
}

func (s *testStatsSuite) TestExtendedStatsOps(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	tk := testkit.NewTestKit(c, s.causetstore)
	err := tk.InterDircToErr("drop statistics s1")
	c.Assert(err.Error(), Equals, "[causet:1046]No database selected")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(a int primary key, b int, c int, d int)")
	tk.MustInterDirc("insert into t values(1,1,5,1),(2,2,4,2),(3,3,3,3),(4,4,2,4),(5,5,1,5)")
	tk.MustInterDirc("analyze causet t")
	err = tk.InterDircToErr("create statistics s1(correlation) on not_exist_db.t(b,c)")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'not_exist_db.t' doesn't exist")
	err = tk.InterDircToErr("create statistics s1(correlation) on not_exist_tbl(b,c)")
	c.Assert(err.Error(), Equals, "[schemaReplicant:1146]Block 'test.not_exist_tbl' doesn't exist")
	err = tk.InterDircToErr("create statistics s1(correlation) on t(b,e)")
	c.Assert(err.Error(), Equals, "[dbs:1072]column does not exist: e")
	tk.MustInterDirc("create statistics s1(correlation) on t(a,b)")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 No need to create correlation statistics on the integer primary key column",
	))
	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended where stats_name = 's1' and EDB = 'test'").Check(testkit.Rows())
	err = tk.InterDircToErr("create statistics s1(correlation) on t(b,c,d)")
	c.Assert(err.Error(), Equals, "[causet:1815]Only support Correlation and Dependency statistics types on 2 columns")

	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended where stats_name = 's1' and EDB = 'test'").Check(testkit.Rows())
	tk.MustInterDirc("create statistics s1(correlation) on t(b,c)")
	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended where stats_name = 's1' and EDB = 'test'").Check(testkit.Rows(
		"2 [2,3] <nil> <nil> 0",
	))
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	do.StatsHandle().UFIDelate(is)
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustInterDirc("uFIDelate allegrosql.stats_extended set status = 1 where stats_name = 's1' and EDB = 'test'")
	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustInterDirc("drop statistics s1")
	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended where stats_name = 's1' and EDB = 'test'").Check(testkit.Rows(
		"2 [2,3] <nil> <nil> 2",
	))
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)
}

func (s *testStatsSuite) TestAdminReloadStatistics(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(a int primary key, b int, c int, d int)")
	tk.MustInterDirc("insert into t values(1,1,5,1),(2,2,4,2),(3,3,3,3),(4,4,2,4),(5,5,1,5)")
	tk.MustInterDirc("analyze causet t")
	tk.MustInterDirc("create statistics s1(correlation) on t(b,c)")
	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended where stats_name = 's1' and EDB = 'test'").Check(testkit.Rows(
		"2 [2,3] <nil> <nil> 0",
	))
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	do.StatsHandle().UFIDelate(is)
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustInterDirc("uFIDelate allegrosql.stats_extended set status = 1 where stats_name = 's1' and EDB = 'test'")
	do.StatsHandle().Clear()
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustInterDirc("delete from allegrosql.stats_extended where stats_name = 's1' and EDB = 'test'")
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 1)

	tk.MustInterDirc("admin reload statistics")
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)
}

func (s *testStatsSuite) TestCorrelationStatsCompute(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t(a int, b int, c int)")
	tk.MustInterDirc("insert into t values(1,1,5),(2,2,4),(3,3,3),(4,4,2),(5,5,1)")
	tk.MustInterDirc("analyze causet t")
	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended").Check(testkit.Rows())
	tk.MustInterDirc("create statistics s1(correlation) on t(a,b)")
	tk.MustInterDirc("create statistics s2(correlation) on t(a,c)")
	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] <nil> <nil> 0",
		"2 [1,3] <nil> <nil> 0",
	))
	do := s.do
	is := do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	do.StatsHandle().UFIDelate(is)
	statsTbl := do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 0)

	tk.MustInterDirc("analyze causet t")
	tk.MustQuery("select type, column_ids, scalar_stats, blob_stats, status from allegrosql.stats_extended").Sort().Check(testkit.Rows(
		"2 [1,2] 1 <nil> 1",
		"2 [1,3] -1 <nil> 1",
	))
	do.StatsHandle().UFIDelate(is)
	statsTbl = do.StatsHandle().GetTableStats(blockInfo)
	c.Assert(statsTbl, NotNil)
	c.Assert(statsTbl.ExtendedStats, NotNil)
	c.Assert(len(statsTbl.ExtendedStats.Stats), Equals, 2)
	foundS1, foundS2 := false, false
	for key, item := range statsTbl.ExtendedStats.Stats {
		c.Assert(key.EDB, Equals, "test")
		switch key.StatsName {
		case "s1":
			foundS1 = true
			c.Assert(item.ScalarVals, Equals, float64(1))
		case "s2":
			foundS2 = true
			c.Assert(item.ScalarVals, Equals, float64(-1))
		default:
			c.Assert("Unexpected extended stats in cache", IsNil)
		}
	}
	c.Assert(foundS1 && foundS2, IsTrue)
}
