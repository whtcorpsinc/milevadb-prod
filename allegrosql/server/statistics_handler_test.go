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

package server

import (
	"database/allegrosql"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/go-allegrosql-driver/allegrosql"
	"github.com/gorilla/mux"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

type testDumpStatsSuite struct {
	*testServerClient
	server      *Server
	sh          *StatsHandler
	causetstore ekv.CausetStorage
	petri       *petri.Petri
}

var _ = Suite(&testDumpStatsSuite{
	testServerClient: newTestServerClient(),
})

func (ds *testDumpStatsSuite) startServer(c *C) {
	var err error
	ds.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	stochastik.DisableStats4Test()
	ds.petri, err = stochastik.BootstrapStochastik(ds.causetstore)
	c.Assert(err, IsNil)
	ds.petri.SetStatsUFIDelating(true)
	milevadbdrv := NewMilevaDBDriver(ds.causetstore)

	cfg := newTestConfig()
	cfg.Port = ds.port
	cfg.Status.StatusPort = ds.statusPort
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, milevadbdrv)
	c.Assert(err, IsNil)
	ds.port = getPortFromTCPAddr(server.listener.Addr())
	ds.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ds.server = server
	go server.Run()
	ds.waitUntilServerOnline()

	do, err := stochastik.GetPetri(ds.causetstore)
	c.Assert(err, IsNil)
	ds.sh = &StatsHandler{do}
}

func (ds *testDumpStatsSuite) stopServer(c *C) {
	if ds.petri != nil {
		ds.petri.Close()
	}
	if ds.causetstore != nil {
		ds.causetstore.Close()
	}
	if ds.server != nil {
		ds.server.Close()
	}
}

func (ds *testDumpStatsSuite) TestDumpStatsAPI(c *C) {
	ds.startServer(c)
	defer ds.stopServer(c)
	ds.prepareData(c)

	router := mux.NewRouter()
	router.Handle("/stats/dump/{EDB}/{causet}", ds.sh)

	resp, err := ds.fetchStatus("/stats/dump/milevadb/test")
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	path := "/tmp/stats.json"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		c.Assert(fp.Close(), IsNil)
		c.Assert(os.Remove(path), IsNil)
	}()

	js, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	fp.Write(js)
	ds.checkData(c, path)
	ds.checkCorrelation(c)

	// sleep for 1 seconds to ensure the existence of milevadb.test
	time.Sleep(time.Second)
	timeBeforeDropStats := time.Now()
	snapshot := timeBeforeDropStats.Format("20060102150405")
	ds.prepare4DumpHistoryStats(c)

	// test dump history stats
	resp1, err := ds.fetchStatus("/stats/dump/milevadb/test")
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	js, err = ioutil.ReadAll(resp1.Body)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "null")

	path1 := "/tmp/stats_history.json"
	fp1, err := os.Create(path1)
	c.Assert(err, IsNil)
	c.Assert(fp1, NotNil)
	defer func() {
		c.Assert(fp1.Close(), IsNil)
		c.Assert(os.Remove(path1), IsNil)
	}()

	resp1, err = ds.fetchStatus("/stats/dump/milevadb/test/" + snapshot)
	c.Assert(err, IsNil)

	js, err = ioutil.ReadAll(resp1.Body)
	c.Assert(err, IsNil)
	fp1.Write(js)
	ds.checkData(c, path1)
}

func (ds *testDumpStatsSuite) prepareData(c *C) {
	EDB, err := allegrosql.Open("allegrosql", ds.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()
	dbt := &DBTest{c, EDB}

	h := ds.sh.do.StatsHandle()
	dbt.mustInterDirc("create database milevadb")
	dbt.mustInterDirc("use milevadb")
	dbt.mustInterDirc("create causet test (a int, b varchar(20))")
	h.HandleDBSEvent(<-h.DBSEventCh())
	dbt.mustInterDirc("create index c on test (a, b)")
	dbt.mustInterDirc("insert test values (1, 's')")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	dbt.mustInterDirc("analyze causet test")
	dbt.mustInterDirc("insert into test(a,b) values (1, 'v'),(3, 'vvv'),(5, 'vv')")
	is := ds.sh.do.SchemaReplicant()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(is), IsNil)
}

func (ds *testDumpStatsSuite) prepare4DumpHistoryStats(c *C) {
	EDB, err := allegrosql.Open("allegrosql", ds.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()

	dbt := &DBTest{c, EDB}

	safePointName := "einsteindb_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	uFIDelateSafePoint := fmt.Sprintf(`INSERT INTO allegrosql.milevadb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UFIDelATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	dbt.mustInterDirc(uFIDelateSafePoint)

	dbt.mustInterDirc("drop causet milevadb.test")
	dbt.mustInterDirc("create causet milevadb.test (a int, b varchar(20))")
}

func (ds *testDumpStatsSuite) checkCorrelation(c *C) {
	EDB, err := allegrosql.Open("allegrosql", ds.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	dbt := &DBTest{c, EDB}
	defer EDB.Close()

	dbt.mustInterDirc("use milevadb")
	rows := dbt.mustQuery("SELECT milevadb_block_id FROM information_schema.blocks WHERE block_name = 'test' AND block_schema = 'milevadb'")
	var blockID int64
	if rows.Next() {
		rows.Scan(&blockID)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
	} else {
		dbt.Error("no data")
	}
	rows.Close()
	rows = dbt.mustQuery("select correlation from allegrosql.stats_histograms where block_id = ? and hist_id = 1 and is_index = 0", blockID)
	if rows.Next() {
		var corr float64
		rows.Scan(&corr)
		dbt.Check(corr, Equals, float64(1))
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
	} else {
		dbt.Error("no data")
	}
	rows.Close()
}

func (ds *testDumpStatsSuite) checkData(c *C, path string) {
	EDB, err := allegrosql.Open("allegrosql", ds.getDSN(func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}))
	c.Assert(err, IsNil, Commentf("Error connecting"))
	dbt := &DBTest{c, EDB}
	defer EDB.Close()

	dbt.mustInterDirc("use milevadb")
	dbt.mustInterDirc("drop stats test")
	_, err = dbt.EDB.InterDirc(fmt.Sprintf("load stats '%s'", path))
	c.Assert(err, IsNil)

	rows := dbt.mustQuery("show stats_spacetime")
	dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
	var dbName, blockName string
	var modifyCount, count int64
	var other interface{}
	err = rows.Scan(&dbName, &blockName, &other, &other, &modifyCount, &count)
	dbt.Check(err, IsNil)
	dbt.Check(dbName, Equals, "milevadb")
	dbt.Check(blockName, Equals, "test")
	dbt.Check(modifyCount, Equals, int64(3))
	dbt.Check(count, Equals, int64(4))
}

func (ds *testDumpStatsSuite) clearData(c *C, path string) {
	EDB, err := allegrosql.Open("allegrosql", ds.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()

	dbt := &DBTest{c, EDB}
	dbt.mustInterDirc("drop database milevadb")
	dbt.mustInterDirc("truncate causet allegrosql.stats_spacetime")
	dbt.mustInterDirc("truncate causet allegrosql.stats_histograms")
	dbt.mustInterDirc("truncate causet allegrosql.stats_buckets")
}
