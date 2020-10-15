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

package binloginfo_test

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	pumpcli "github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/pump_client"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	binlog "github.com/whtcorpsinc/fidelpb/go-binlog"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	TestingT(t)
}

type mockBinlogPump struct {
	mu struct {
		sync.Mutex
		payloads [][]byte
		mockFail bool
	}
}

func (p *mockBinlogPump) WriteBinlog(ctx context.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.mockFail {
		return &binlog.WriteBinlogResp{}, errors.New("mock fail")
	}
	p.mu.payloads = append(p.mu.payloads, req.Payload)
	return &binlog.WriteBinlogResp{}, nil
}

// PullBinlogs implements PumpServer interface.
func (p *mockBinlogPump) PullBinlogs(req *binlog.PullBinlogReq, srv binlog.Pump_PullBinlogsServer) error {
	return nil
}

var _ = SerialSuites(&testBinlogSuite{})

type testBinlogSuite struct {
	causetstore    ekv.CausetStorage
	petri   *petri.Petri
	unixFile string
	serv     *grpc.Server
	pump     *mockBinlogPump
	client   *pumpcli.PumpsClient
	dbs      dbs.DBS
}

const maxRecvMsgSize = 64 * 1024

func (s *testBinlogSuite) SetUpSuite(c *C) {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	stochastik.SetSchemaLease(0)
	s.unixFile = "/tmp/mock-binlog-pump" + strconv.FormatInt(time.Now().UnixNano(), 10)
	l, err := net.Listen("unix", s.unixFile)
	c.Assert(err, IsNil)
	s.serv = grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvMsgSize))
	s.pump = new(mockBinlogPump)
	binlog.RegisterPumpServer(s.serv, s.pump)
	go s.serv.Serve(l)
	opt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	})
	clientCon, err := grpc.Dial(s.unixFile, opt, grpc.WithInsecure())
	c.Assert(err, IsNil)
	c.Assert(clientCon, NotNil)
	tk := testkit.NewTestKit(c, s.causetstore)
	s.petri, err = stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	tk.MustInterDirc("use test")
	stochastikPetri := petri.GetPetri(tk.Se.(stochastikctx.Context))
	s.dbs = stochastikPetri.DBS()

	s.client = binloginfo.MockPumpsClient(binlog.NewPumpClient(clientCon))
	s.dbs.SetBinlogClient(s.client)
}

func (s *testBinlogSuite) TearDownSuite(c *C) {
	s.dbs.Stop()
	s.serv.Stop()
	os.Remove(s.unixFile)
	s.petri.Close()
	s.causetstore.Close()
}

func (s *testBinlogSuite) TestBinlog(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.Se.GetStochastikVars().BinlogClient = s.client
	pump := s.pump
	tk.MustInterDirc("drop causet if exists local_binlog")
	dbsQuery := "create causet local_binlog (id int unique key, name varchar(10)) shard_row_id_bits=1"
	binlogDBSQuery := "create causet local_binlog (id int unique key, name varchar(10)) /*T! shard_row_id_bits=1 */"
	tk.MustInterDirc(dbsQuery)
	var matched bool // got matched pre DBS and commit DBS
	for i := 0; i < 10; i++ {
		preDBS, commitDBS, _ := getLatestDBSBinlog(c, pump, binlogDBSQuery)
		if preDBS != nil && commitDBS != nil {
			if preDBS.DdlJobId == commitDBS.DdlJobId {
				c.Assert(commitDBS.StartTs, Equals, preDBS.StartTs)
				c.Assert(commitDBS.CommitTs, Greater, commitDBS.StartTs)
				matched = true
				break
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(matched, IsTrue)

	tk.MustInterDirc("insert local_binlog values (1, 'abc'), (2, 'cde')")
	prewriteVal := getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.SchemaVersion, Greater, int64(0))
	c.Assert(prewriteVal.Mutations[0].BlockId, Greater, int64(0))
	expected := [][]types.Causet{
		{types.NewIntCauset(1), types.NewDefCauslationStringCauset("abc", allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)},
		{types.NewIntCauset(2), types.NewDefCauslationStringCauset("cde", allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)},
	}
	gotRows := mutationRowsToRows(c, prewriteVal.Mutations[0].InsertedRows, 2, 4)
	c.Assert(gotRows, DeepEquals, expected)

	tk.MustInterDirc("uFIDelate local_binlog set name = 'xyz' where id = 2")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	oldRow := [][]types.Causet{
		{types.NewIntCauset(2), types.NewDefCauslationStringCauset("cde", allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)},
	}
	newRow := [][]types.Causet{
		{types.NewIntCauset(2), types.NewDefCauslationStringCauset("xyz", allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)},
	}
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].UFIDelatedRows, 1, 3)
	c.Assert(gotRows, DeepEquals, oldRow)

	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].UFIDelatedRows, 7, 9)
	c.Assert(gotRows, DeepEquals, newRow)

	tk.MustInterDirc("delete from local_binlog where id = 1")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].DeletedRows, 1, 3)
	expected = [][]types.Causet{
		{types.NewIntCauset(1), types.NewDefCauslationStringCauset("abc", allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)},
	}
	c.Assert(gotRows, DeepEquals, expected)

	// Test causet primary key is not integer.
	tk.MustInterDirc("set @@milevadb_enable_clustered_index=0;")
	tk.MustInterDirc("create causet local_binlog2 (name varchar(64) primary key, age int)")
	tk.MustInterDirc("insert local_binlog2 values ('abc', 16), ('def', 18)")
	tk.MustInterDirc("delete from local_binlog2 where name = 'def'")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].Sequence[0], Equals, binlog.MutationType_DeleteRow)

	expected = [][]types.Causet{
		{types.NewStringCauset("def"), types.NewIntCauset(18), types.NewIntCauset(-1), types.NewIntCauset(2)},
	}
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].DeletedRows, 1, 3, 4, 5)
	c.Assert(gotRows, DeepEquals, expected)

	// Test Block don't have primary key.
	tk.MustInterDirc("create causet local_binlog3 (c1 int, c2 int)")
	tk.MustInterDirc("insert local_binlog3 values (1, 2), (1, 3), (2, 3)")
	tk.MustInterDirc("uFIDelate local_binlog3 set c1 = 3 where c1 = 2")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)

	// The encoded uFIDelate event is [oldDefCausID1, oldDefCausVal1, oldDefCausID2, oldDefCausVal2, -1, handle,
	// 		newDefCausID1, newDefCausVal2, newDefCausID2, newDefCausVal2, -1, handle]
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].UFIDelatedRows, 7, 9)
	expected = [][]types.Causet{
		{types.NewIntCauset(3), types.NewIntCauset(3)},
	}
	c.Assert(gotRows, DeepEquals, expected)
	expected = [][]types.Causet{
		{types.NewIntCauset(-1), types.NewIntCauset(3), types.NewIntCauset(-1), types.NewIntCauset(3)},
	}
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].UFIDelatedRows, 4, 5, 10, 11)
	c.Assert(gotRows, DeepEquals, expected)

	tk.MustInterDirc("delete from local_binlog3 where c1 = 3 and c2 = 3")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].Sequence[0], Equals, binlog.MutationType_DeleteRow)
	gotRows = mutationRowsToRows(c, prewriteVal.Mutations[0].DeletedRows, 1, 3, 4, 5)
	expected = [][]types.Causet{
		{types.NewIntCauset(3), types.NewIntCauset(3), types.NewIntCauset(-1), types.NewIntCauset(3)},
	}
	c.Assert(gotRows, DeepEquals, expected)

	// Test Mutation Sequence.
	tk.MustInterDirc("create causet local_binlog4 (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert local_binlog4 values (1, 1), (2, 2), (3, 2)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("delete from local_binlog4 where c1 = 1")
	tk.MustInterDirc("insert local_binlog4 values (1, 1)")
	tk.MustInterDirc("uFIDelate local_binlog4 set c2 = 3 where c1 = 3")
	tk.MustInterDirc("commit")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].Sequence, DeepEquals, []binlog.MutationType{
		binlog.MutationType_DeleteRow,
		binlog.MutationType_Insert,
		binlog.MutationType_UFIDelate,
	})

	// Test memex rollback.
	tk.MustInterDirc("create causet local_binlog5 (c1 int primary key)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into local_binlog5 value (1)")
	// This memex execute fail and should not write binlog.
	_, err := tk.InterDirc("insert into local_binlog5 value (4),(3),(1),(2)")
	c.Assert(err, NotNil)
	tk.MustInterDirc("commit")
	prewriteVal = getLatestBinlogPrewriteValue(c, pump)
	c.Assert(prewriteVal.Mutations[0].Sequence, DeepEquals, []binlog.MutationType{
		binlog.MutationType_Insert,
	})

	checkBinlogCount(c, pump)

	pump.mu.Lock()
	originBinlogLen := len(pump.mu.payloads)
	pump.mu.Unlock()
	tk.MustInterDirc("set @@global.autocommit = 0")
	tk.MustInterDirc("set @@global.autocommit = 1")
	pump.mu.Lock()
	newBinlogLen := len(pump.mu.payloads)
	pump.mu.Unlock()
	c.Assert(newBinlogLen, Equals, originBinlogLen)
}

func (s *testBinlogSuite) TestMaxRecvSize(c *C) {
	info := &binloginfo.BinlogInfo{
		Data: &binlog.Binlog{
			Tp:            binlog.BinlogType_Prewrite,
			PrewriteValue: make([]byte, maxRecvMsgSize+1),
		},
		Client: s.client,
	}
	binlogWR := info.WriteBinlog(1)
	err := binlogWR.GetError()
	c.Assert(err, NotNil)
	c.Assert(terror.ErrCritical.Equal(err), IsFalse, Commentf("%v", err))
}

func getLatestBinlogPrewriteValue(c *C, pump *mockBinlogPump) *binlog.PrewriteValue {
	var bin *binlog.Binlog
	pump.mu.Lock()
	for i := len(pump.mu.payloads) - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin = new(binlog.Binlog)
		bin.Unmarshal(payload)
		if bin.Tp == binlog.BinlogType_Prewrite {
			break
		}
	}
	pump.mu.Unlock()
	c.Assert(bin, NotNil)
	preVal := new(binlog.PrewriteValue)
	preVal.Unmarshal(bin.PrewriteValue)
	return preVal
}

func getLatestDBSBinlog(c *C, pump *mockBinlogPump, dbsQuery string) (preDBS, commitDBS *binlog.Binlog, offset int) {
	pump.mu.Lock()
	for i := len(pump.mu.payloads) - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin := new(binlog.Binlog)
		bin.Unmarshal(payload)
		if bin.Tp == binlog.BinlogType_Commit && bin.DdlJobId > 0 {
			commitDBS = bin
		}
		if bin.Tp == binlog.BinlogType_Prewrite && bin.DdlJobId != 0 {
			preDBS = bin
		}
		if preDBS != nil && commitDBS != nil {
			offset = i
			break
		}
	}
	pump.mu.Unlock()
	c.Assert(preDBS.DdlJobId, Greater, int64(0))
	c.Assert(preDBS.StartTs, Greater, int64(0))
	c.Assert(preDBS.CommitTs, Equals, int64(0))
	c.Assert(string(preDBS.DdlQuery), Equals, dbsQuery)
	return
}

func checkBinlogCount(c *C, pump *mockBinlogPump) {
	var bin *binlog.Binlog
	prewriteCount := 0
	dbsCount := 0
	pump.mu.Lock()
	length := len(pump.mu.payloads)
	for i := length - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin = new(binlog.Binlog)
		bin.Unmarshal(payload)
		if bin.Tp == binlog.BinlogType_Prewrite {
			if bin.DdlJobId != 0 {
				dbsCount++
			} else {
				prewriteCount++
			}
		}
	}
	pump.mu.Unlock()
	c.Assert(dbsCount, Greater, 0)
	match := false
	for i := 0; i < 10; i++ {
		pump.mu.Lock()
		length = len(pump.mu.payloads)
		pump.mu.Unlock()
		if (prewriteCount+dbsCount)*2 == length {
			match = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.Assert(match, IsTrue)
}

func mutationRowsToRows(c *C, mutationRows [][]byte, defCausumnValueOffsets ...int) [][]types.Causet {
	var rows = make([][]types.Causet, 0)
	for _, mutationRow := range mutationRows {
		datums, err := codec.Decode(mutationRow, 5)
		c.Assert(err, IsNil)
		for i := range datums {
			if datums[i].HoTT() == types.HoTTBytes {
				datums[i].SetBytesAsString(datums[i].GetBytes(), allegrosql.DefaultDefCauslationName, defCauslate.DefaultLen)
			}
		}
		event := make([]types.Causet, 0, len(defCausumnValueOffsets))
		for _, defCausOff := range defCausumnValueOffsets {
			event = append(event, datums[defCausOff])
		}
		rows = append(rows, event)
	}
	return rows
}

func (s *testBinlogSuite) TestBinlogForSequence(c *C) {
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/mockSyncBinlogCommit", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/mockSyncBinlogCommit"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	s.pump.mu.Lock()
	s.pump.mu.payloads = s.pump.mu.payloads[:0]
	s.pump.mu.Unlock()
	tk.Se.GetStochastikVars().BinlogClient = s.client

	tk.MustInterDirc("drop sequence if exists seq")
	// the default start = 1, increment = 1.
	tk.MustInterDirc("create sequence seq cache 3")
	// trigger the sequence cache allocation.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceBlock := testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok := sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round := tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(3))
	c.Assert(round, Equals, int64(0))

	// Check the sequence binlog.
	// Got matched pre DBS and commit DBS.
	ok = mustGetDBSBinlog(s, "select setval(`test`.`seq`, 3)", c)
	c.Assert(ok, IsTrue)

	// Invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	// trigger the next sequence cache allocation.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(8))
	c.Assert(round, Equals, int64(0))
	ok = mustGetDBSBinlog(s, "select setval(`test`.`seq`, 8)", c)
	c.Assert(ok, IsTrue)

	tk.MustInterDirc("create database test2")
	tk.MustInterDirc("use test2")
	tk.MustInterDirc("drop sequence if exists seq2")
	tk.MustInterDirc("create sequence seq2 start 1 increment -2 cache 3 minvalue -10 maxvalue 10 cycle")
	// trigger the sequence cache allocation.
	tk.MustQuery("select nextval(seq2)").Check(testkit.Rows("1"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test2", "seq2")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-3))
	c.Assert(round, Equals, int64(0))
	ok = mustGetDBSBinlog(s, "select setval(`test2`.`seq2`, -3)", c)
	c.Assert(ok, IsTrue)

	tk.MustQuery("select setval(seq2, -100)").Check(testkit.Rows("-100"))
	// trigger the sequence cache allocation.
	tk.MustQuery("select nextval(seq2)").Check(testkit.Rows("10"))
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(6))
	c.Assert(round, Equals, int64(1))
	ok = mustGetDBSBinlog(s, "select setval(`test2`.`seq2`, 6)", c)
	c.Assert(ok, IsTrue)

	// Test dml txn is independent from sequence txn.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq cache 3")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int default next value for seq)")
	// sequence txn commit first then the dml txn.
	tk.MustInterDirc("insert into t values(-1),(default),(-1),(default)")
	// binlog list like [... dbs prewrite(offset), dbs commit, dml prewrite, dml commit]
	_, _, offset := getLatestDBSBinlog(c, s.pump, "select setval(`test2`.`seq`, 3)")
	s.pump.mu.Lock()
	c.Assert(offset+3, Equals, len(s.pump.mu.payloads)-1)
	s.pump.mu.Unlock()
}

// Sometimes this test doesn't clean up fail, let the function name begin with 'Z'
// so it runs last and would not disrupt other tests.
func (s *testBinlogSuite) TestZIgnoreError(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.Se.GetStochastikVars().BinlogClient = s.client
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int)")

	binloginfo.SetIgnoreError(true)
	s.pump.mu.Lock()
	s.pump.mu.mockFail = true
	s.pump.mu.Unlock()

	tk.MustInterDirc("insert into t values (1)")
	tk.MustInterDirc("insert into t values (1)")

	// Clean up.
	s.pump.mu.Lock()
	s.pump.mu.mockFail = false
	s.pump.mu.Unlock()
	binloginfo.DisableSkipBinlogFlag()
	binloginfo.SetIgnoreError(false)
}

func (s *testBinlogSuite) TestPartitionedBlock(c *C) {
	// This test checks partitioned causet write binlog with causet ID, rather than partition ID.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.Se.GetStochastikVars().BinlogClient = s.client
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc(`create causet t (id int) partition by range (id) (
			partition p0 values less than (1),
			partition p1 values less than (4),
			partition p2 values less than (7),
			partition p3 values less than (10))`)
	tids := make([]int64, 0, 10)
	for i := 0; i < 10; i++ {
		tk.MustInterDirc("insert into t values (?)", i)
		prewriteVal := getLatestBinlogPrewriteValue(c, s.pump)
		tids = append(tids, prewriteVal.Mutations[0].BlockId)
	}
	c.Assert(len(tids), Equals, 10)
	for i := 1; i < 10; i++ {
		c.Assert(tids[i], Equals, tids[0])
	}
}

func (s *testBinlogSuite) TestDeleteSchema(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("CREATE TABLE `b1` (`id` int(11) NOT NULL AUTO_INCREMENT, `job_id` varchar(50) NOT NULL, `split_job_id` varchar(30) DEFAULT NULL, PRIMARY KEY (`id`), KEY `b1` (`job_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustInterDirc("CREATE TABLE `b2` (`id` int(11) NOT NULL AUTO_INCREMENT, `job_id` varchar(50) NOT NULL, `batch_class` varchar(20) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `bu` (`job_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	tk.MustInterDirc("insert into b2 (job_id, batch_class) values (2, 'TEST');")
	tk.MustInterDirc("insert into b1 (job_id) values (2);")

	// This test cover a bug that the final schemaReplicant and the binlog event inconsistent.
	// The final schemaReplicant of this ALLEGROALLEGROSQL should be the schemaReplicant of causet b1, rather than the schemaReplicant of join result.
	tk.MustInterDirc("delete from b1 where job_id in (select job_id from b2 where batch_class = 'TEST') or split_job_id in (select job_id from b2 where batch_class = 'TEST');")
	tk.MustInterDirc("delete b1 from b2 right join b1 on b1.job_id = b2.job_id and batch_class = 'TEST';")
}

func (s *testBinlogSuite) TestAddSpecialComment(c *C) {
	testCase := []struct {
		input  string
		result string
	}{
		{
			"create causet t1 (id int ) shard_row_id_bits=2;",
			"create causet t1 (id int ) /*T! shard_row_id_bits=2 */ ;",
		},
		{
			"create causet t1 (id int ) shard_row_id_bits=2 pre_split_regions=2;",
			"create causet t1 (id int ) /*T! shard_row_id_bits=2 pre_split_regions=2 */ ;",
		},
		{
			"create causet t1 (id int ) shard_row_id_bits=2     pre_split_regions=2;",
			"create causet t1 (id int ) /*T! shard_row_id_bits=2     pre_split_regions=2 */ ;",
		},

		{
			"create causet t1 (id int ) shard_row_id_bits=2 engine=innodb pre_split_regions=2;",
			"create causet t1 (id int ) /*T! shard_row_id_bits=2 pre_split_regions=2 */ engine=innodb ;",
		},
		{
			"create causet t1 (id int ) pre_split_regions=2 shard_row_id_bits=2;",
			"create causet t1 (id int ) /*T! shard_row_id_bits=2 pre_split_regions=2 */ ;",
		},
		{
			"create causet t6 (id int ) shard_row_id_bits=2 shard_row_id_bits=3 pre_split_regions=2;",
			"create causet t6 (id int ) /*T! shard_row_id_bits=2 shard_row_id_bits=3 pre_split_regions=2 */ ;",
		},
		{
			"create causet t1 (id int primary key auto_random(2));",
			"create causet t1 (id int primary key /*T![auto_rand] auto_random(2) */ );",
		},
		{
			"create causet t1 (id int primary key auto_random);",
			"create causet t1 (id int primary key /*T![auto_rand] auto_random */ );",
		},
		{
			"create causet t1 (id int auto_random ( 4 ) primary key);",
			"create causet t1 (id int /*T![auto_rand] auto_random ( 4 ) */ primary key);",
		},
		{
			"create causet t1 (id int  auto_random  (   4    ) primary key);",
			"create causet t1 (id int  /*T![auto_rand] auto_random  (   4    ) */ primary key);",
		},
		{
			"create causet t1 (id int auto_random ( 3 ) primary key) auto_random_base = 100;",
			"create causet t1 (id int /*T![auto_rand] auto_random ( 3 ) */ primary key) /*T![auto_rand_base] auto_random_base = 100 */ ;",
		},
		{
			"create causet t1 (id int auto_random primary key) auto_random_base = 50;",
			"create causet t1 (id int /*T![auto_rand] auto_random */ primary key) /*T![auto_rand_base] auto_random_base = 50 */ ;",
		},
		{
			"create causet t1 (id int auto_increment key) auto_id_cache 100;",
			"create causet t1 (id int auto_increment key) /*T![auto_id_cache] auto_id_cache 100 */ ;",
		},
		{
			"create causet t1 (id int auto_increment unique) auto_id_cache 10;",
			"create causet t1 (id int auto_increment unique) /*T![auto_id_cache] auto_id_cache 10 */ ;",
		},
		{
			"create causet t1 (id int) auto_id_cache = 5;",
			"create causet t1 (id int) /*T![auto_id_cache] auto_id_cache = 5 */ ;",
		},
		{
			"create causet t1 (id int) auto_id_cache=5;",
			"create causet t1 (id int) /*T![auto_id_cache] auto_id_cache=5 */ ;",
		},
		{
			"create causet t1 (id int) /*T![auto_id_cache] auto_id_cache=5 */ ;",
			"create causet t1 (id int) /*T![auto_id_cache] auto_id_cache=5 */ ;",
		},
	}
	for _, ca := range testCase {
		re := binloginfo.AddSpecialComment(ca.input)
		c.Assert(re, Equals, ca.result)
	}
}

func mustGetDBSBinlog(s *testBinlogSuite, dbsQuery string, c *C) (matched bool) {
	for i := 0; i < 10; i++ {
		preDBS, commitDBS, _ := getLatestDBSBinlog(c, s.pump, dbsQuery)
		if preDBS != nil && commitDBS != nil {
			if preDBS.DdlJobId == commitDBS.DdlJobId {
				c.Assert(commitDBS.StartTs, Equals, preDBS.StartTs)
				c.Assert(commitDBS.CommitTs, Greater, commitDBS.StartTs)
				matched = true
				break
			}
		}
		time.Sleep(time.Millisecond * 30)
	}
	return
}

func testGetBlockByName(c *C, ctx stochastikctx.Context, EDB, causet string) causet.Block {
	dom := petri.GetPetri(ctx)
	// Make sure the causet schemaReplicant is the new schemaReplicant.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.SchemaReplicant().BlockByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(causet))
	c.Assert(err, IsNil)
	return tbl
}
