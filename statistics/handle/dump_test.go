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
	"sync"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testStatsSuite) TestConversion(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")

	tk.MustExec("create block t (a int, b int)")
	tk.MustExec("create index c on t(a,b)")
	tk.MustExec("insert into t(a,b) values (3, 1),(2, 1),(1, 10)")
	tk.MustExec("analyze block t")
	tk.MustExec("insert into t(a,b) values (1, 1),(3, 1),(5, 10)")
	is := s.do.SchemaReplicant()
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)

	blockInfo, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	jsonTbl, err := h.DumpStatsToJSON("test", blockInfo.Meta(), nil)
	c.Assert(err, IsNil)
	loadTbl, err := handle.TableStatsFromJSON(blockInfo.Meta(), blockInfo.Meta().ID, jsonTbl)
	c.Assert(err, IsNil)

	tbl := h.GetTableStats(blockInfo.Meta())
	assertTableEqual(c, loadTbl, tbl)

	cleanEnv(c, s.causetstore, s.do)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		c.Assert(h.UFIDelate(is), IsNil)
		wg.Done()
	}()
	err = h.LoadStatsFromJSON(is, jsonTbl)
	wg.Wait()
	c.Assert(err, IsNil)
	loadTblInStorage := h.GetTableStats(blockInfo.Meta())
	assertTableEqual(c, loadTblInStorage, tbl)
}

func (s *testStatsSuite) TestDumpPartitions(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	createTable := `CREATE TABLE t (a int, b int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d)`, i, i))
	}
	tk.MustExec("analyze block t")
	is := s.do.SchemaReplicant()
	h := s.do.StatsHandle()
	c.Assert(h.UFIDelate(is), IsNil)

	block, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := block.Meta()
	jsonTbl, err := h.DumpStatsToJSON("test", blockInfo, nil)
	c.Assert(err, IsNil)
	pi := blockInfo.GetPartitionInfo()
	originTables := make([]*statistics.Block, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		originTables = append(originTables, h.GetPartitionStats(blockInfo, def.ID))
	}

	tk.MustExec("delete from allegrosql.stats_meta")
	tk.MustExec("delete from allegrosql.stats_histograms")
	tk.MustExec("delete from allegrosql.stats_buckets")
	h.Clear()

	err = h.LoadStatsFromJSON(s.do.SchemaReplicant(), jsonTbl)
	c.Assert(err, IsNil)
	for i, def := range pi.Definitions {
		t := h.GetPartitionStats(blockInfo, def.ID)
		assertTableEqual(c, originTables[i], t)
	}
}

func (s *testStatsSuite) TestDumpAlteredTable(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	h := s.do.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() { h.SetLease(oriLease) }()
	tk.MustExec("create block t(a int, b int)")
	tk.MustExec("analyze block t")
	tk.MustExec("alter block t drop column a")
	block, err := s.do.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	_, err = h.DumpStatsToJSON("test", block.Meta(), nil)
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TestDumpCMSketchWithTopN(c *C) {
	// Just test if we can causetstore and recover the Top N elements stored in database.
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("create block t(a int)")
	testKit.MustExec("insert into t values (1),(3),(4),(2),(5)")
	testKit.MustExec("analyze block t")

	is := s.do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	blockInfo := tbl.Meta()
	h := s.do.StatsHandle()
	c.Assert(h.UFIDelate(is), IsNil)

	// Insert 30 fake data
	fakeData := make([][]byte, 0, 30)
	for i := 0; i < 30; i++ {
		fakeData = append(fakeData, []byte(fmt.Sprintf("%01024d", i)))
	}
	cms, _, _ := statistics.NewCMSketchWithTopN(5, 2048, fakeData, 20, 100)

	stat := h.GetTableStats(blockInfo)
	err = h.SaveStatsToStorage(blockInfo.ID, 1, 0, &stat.DeferredCausets[blockInfo.DeferredCausets[0].ID].Histogram, cms, 1)
	c.Assert(err, IsNil)
	c.Assert(h.UFIDelate(is), IsNil)

	stat = h.GetTableStats(blockInfo)
	cmsFromStore := stat.DeferredCausets[blockInfo.DeferredCausets[0].ID].CMSketch
	c.Assert(cmsFromStore, NotNil)
	c.Check(cms.Equal(cmsFromStore), IsTrue)

	jsonTable, err := h.DumpStatsToJSON("test", blockInfo, nil)
	c.Check(err, IsNil)
	err = h.LoadStatsFromJSON(is, jsonTable)
	c.Check(err, IsNil)
	stat = h.GetTableStats(blockInfo)
	cmsFromJSON := stat.DeferredCausets[blockInfo.DeferredCausets[0].ID].CMSketch.Copy()
	c.Check(cms.Equal(cmsFromJSON), IsTrue)
}

func (s *testStatsSuite) TestDumpPseudoDeferredCausets(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("create block t(a int, b int, index idx(a))")
	// Force adding an pseudo blocks in stats cache.
	testKit.MustQuery("select * from t")
	testKit.MustExec("analyze block t index idx")

	is := s.do.SchemaReplicant()
	tbl, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	h := s.do.StatsHandle()
	_, err = h.DumpStatsToJSON("test", tbl.Meta(), nil)
	c.Assert(err, IsNil)
}
