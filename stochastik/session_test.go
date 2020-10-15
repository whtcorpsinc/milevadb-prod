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

package stochastik_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/fidelpb/go-binlog"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

var (
	withEinsteinDB        = flag.Bool("with-einsteindb", false, "run tests with EinsteinDB cluster started. (not use the mock server)")
	FIDelAddrs         = flag.String("fidel-addrs", "127.0.0.1:2379", "fidel addrs")
	FIDelAddrChan      chan string
	initFIDelAddrsOnce sync.Once
)

var _ = Suite(&testStochastikSuite{})
var _ = Suite(&testStochastikSuite2{})
var _ = Suite(&testStochastikSuite3{})
var _ = Suite(&testSchemaSuite{})
var _ = Suite(&testIsolationSuite{})
var _ = SerialSuites(&testSchemaSerialSuite{})
var _ = SerialSuites(&testStochastikSerialSuite{})
var _ = SerialSuites(&testBackupRestoreSuite{})

type testStochastikSuiteBase struct {
	cluster cluster.Cluster
	causetstore   ekv.CausetStorage
	dom     *petri.Petri
	FIDelAddr  string
}

type testStochastikSuite struct {
	testStochastikSuiteBase
}

type testStochastikSuite2 struct {
	testStochastikSuiteBase
}

type testStochastikSuite3 struct {
	testStochastikSuiteBase
}

type testStochastikSerialSuite struct {
	testStochastikSuiteBase
}

type testBackupRestoreSuite struct {
	testStochastikSuiteBase
}

func clearStorage(causetstore ekv.CausetStorage) error {
	txn, err := causetstore.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for iter.Valid() {
		txn.Delete(iter.Key())
		if err := iter.Next(); err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit(context.Background())
}

func clearETCD(ebd einsteindb.EtcdBackend) error {
	endpoints, err := ebd.EtcdAddrs()
	if err != nil {
		return err
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
		TLS: ebd.TLSConfig(),
	})
	if err != nil {
		return errors.Trace(err)
	}
	defer cli.Close()

	resp, err := cli.Get(context.Background(), "/milevadb", clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	for _, ekv := range resp.Kvs {
		if ekv.Lease != 0 {
			if _, err := cli.Revoke(context.Background(), clientv3.LeaseID(ekv.Lease)); err != nil {
				return errors.Trace(err)
			}
		}
	}
	_, err = cli.Delete(context.Background(), "/milevadb", clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func initFIDelAddrs() {
	initFIDelAddrsOnce.Do(func() {
		addrs := strings.Split(*FIDelAddrs, ",")
		FIDelAddrChan = make(chan string, len(addrs))
		for _, addr := range addrs {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				FIDelAddrChan <- addr
			}
		}
	})
}

func (s *testStochastikSuiteBase) SetUpSuite(c *C) {
	testleak.BeforeTest()

	if *withEinsteinDB {
		initFIDelAddrs()
		s.FIDelAddr = <-FIDelAddrChan
		var d einsteindb.Driver
		config.UFIDelateGlobal(func(conf *config.Config) {
			conf.TxnLocalLatches.Enabled = false
		})
		causetstore, err := d.Open(fmt.Sprintf("einsteindb://%s", s.FIDelAddr))
		c.Assert(err, IsNil)
		err = clearStorage(causetstore)
		c.Assert(err, IsNil)
		err = clearETCD(causetstore.(einsteindb.EtcdBackend))
		c.Assert(err, IsNil)
		stochastik.ResetStoreForWithEinsteinDBTest(causetstore)
		s.causetstore = causetstore
	} else {
		causetstore, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.causetstore = causetstore
		stochastik.DisableStats4Test()
	}

	var err error
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.dom.GetGlobalVarsCache().Disable()
}

func (s *testStochastikSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
	if *withEinsteinDB {
		FIDelAddrChan <- s.FIDelAddr
	}
}

func (s *testStochastikSuiteBase) TearDownTest(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	r := tk.MustQuery("show full blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		blockType := tb[1]
		if blockType == "VIEW" {
			tk.MustInterDirc(fmt.Sprintf("drop view %v", blockName))
		} else if blockType == "BASE TABLE" {
			tk.MustInterDirc(fmt.Sprintf("drop causet %v", blockName))
		} else {
			panic(fmt.Sprintf("Unexpected causet '%s' with type '%s'.", blockName, blockType))
		}
	}
}

type mockBinlogPump struct {
}

var _ binlog.PumpClient = &mockBinlogPump{}

func (p *mockBinlogPump) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq, opts ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return &binlog.WriteBinlogResp{}, nil
}

type mockPumpPullBinlogsClient struct {
	grpc.ClientStream
}

func (m mockPumpPullBinlogsClient) Recv() (*binlog.PullBinlogResp, error) {
	return nil, nil
}

func (p *mockBinlogPump) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq, opts ...grpc.CallOption) (binlog.Pump_PullBinlogsClient, error) {
	return mockPumpPullBinlogsClient{mockeinsteindb.MockGRPCClientStream()}, nil
}

func (s *testStochastikSuite) TestForCoverage(c *C) {
	// Just for test coverage.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int auto_increment, v int, index (id))")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("insert t values ()")

	// Normal request will not cover txn.Seek.
	tk.MustInterDirc("admin check causet t")

	// Cover dirty causet operations in StateTxn.
	tk.Se.GetStochastikVars().BinlogClient = binloginfo.MockPumpsClient(&mockBinlogPump{})
	tk.MustInterDirc("begin")
	tk.MustInterDirc("truncate causet t")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("delete from t where id = 2")
	tk.MustInterDirc("uFIDelate t set v = 5 where id = 2")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("rollback")

	c.Check(tk.Se.SetDefCauslation(allegrosql.DefaultDefCauslationID), IsNil)

	tk.MustInterDirc("show processlist")
	_, err := tk.Se.FieldList("t")
	c.Check(err, IsNil)
}

func (s *testStochastikSuite2) TestErrorRollback(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t_rollback")
	tk.MustInterDirc("create causet t_rollback (c1 int, c2 int, primary key(c1))")
	tk.MustInterDirc("insert into t_rollback values (0, 0)")

	var wg sync.WaitGroup
	cnt := 4
	wg.Add(cnt)
	num := 20

	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			localTk := testkit.NewTestKitWithInit(c, s.causetstore)
			localTk.MustInterDirc("set @@stochastik.milevadb_retry_limit = 100")
			for j := 0; j < num; j++ {
				localTk.InterDirc("insert into t_rollback values (1, 1)")
				localTk.MustInterDirc("uFIDelate t_rollback set c2 = c2 + 1 where c1 = 0")
			}
		}()
	}

	wg.Wait()
	tk.MustQuery("select c2 from t_rollback where c1 = 0").Check(testkit.Rows(fmt.Sprint(cnt * num)))
}

func (s *testStochastikSuite) TestQueryString(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("create causet mutil1 (a int);create causet multi2 (a int)")
	queryStr := tk.Se.Value(stochastikctx.QueryString)
	c.Assert(queryStr, Equals, "create causet multi2 (a int)")

	// Test execution of DBS through the "InterDircutePreparedStmt" interface.
	_, err := tk.Se.InterDircute(context.Background(), "use test;")
	c.Assert(err, IsNil)
	_, err = tk.Se.InterDircute(context.Background(), "CREATE TABLE t (id bigint PRIMARY KEY, age int)")
	c.Assert(err, IsNil)
	_, err = tk.Se.InterDircute(context.Background(), "show create causet t")
	c.Assert(err, IsNil)
	id, _, _, err := tk.Se.PrepareStmt("CREATE TABLE t2(id bigint PRIMARY KEY, age int)")
	c.Assert(err, IsNil)
	params := []types.Causet{}
	_, err = tk.Se.InterDircutePreparedStmt(context.Background(), id, params)
	c.Assert(err, IsNil)
	qs := tk.Se.Value(stochastikctx.QueryString)
	c.Assert(qs.(string), Equals, "CREATE TABLE t2(id bigint PRIMARY KEY, age int)")

	// Test execution of DBS through the "InterDircute" interface.
	_, err = tk.Se.InterDircute(context.Background(), "use test;")
	c.Assert(err, IsNil)
	_, err = tk.Se.InterDircute(context.Background(), "drop causet t2")
	c.Assert(err, IsNil)
	_, err = tk.Se.InterDircute(context.Background(), "prepare stmt from 'CREATE TABLE t2(id bigint PRIMARY KEY, age int)'")
	c.Assert(err, IsNil)
	_, err = tk.Se.InterDircute(context.Background(), "execute stmt")
	c.Assert(err, IsNil)
	qs = tk.Se.Value(stochastikctx.QueryString)
	c.Assert(qs.(string), Equals, "CREATE TABLE t2(id bigint PRIMARY KEY, age int)")
}

func (s *testStochastikSuite) TestAffectedRows(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(id TEXT)")
	tk.MustInterDirc(`INSERT INTO t VALUES ("a");`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustInterDirc(`INSERT INTO t VALUES ("b");`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustInterDirc(`UFIDelATE t set id = 'c' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustInterDirc(`UFIDelATE t set id = 'a' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)
	tk.MustQuery(`SELECT * from t`).Check(testkit.Rows("c", "b"))
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int, data int)")
	tk.MustInterDirc(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustInterDirc(`UFIDelATE t set id = 1 where data = 0;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int, c1 timestamp);")
	tk.MustInterDirc(`insert t values(1, 0);`)
	tk.MustInterDirc(`UFIDelATE t set id = 1 where id = 1;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)

	// With ON DUPLICATE KEY UFIDelATE, the affected-rows value per event is 1 if the event is inserted as a new event,
	// 2 if an existing event is uFIDelated, and 0 if an existing event is set to its current values.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c1 int PRIMARY KEY, c2 int);")
	tk.MustInterDirc(`insert t values(1, 1);`)
	tk.MustInterDirc(`insert into t values (1, 1) on duplicate key uFIDelate c2=2;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)
	tk.MustInterDirc(`insert into t values (1, 1) on duplicate key uFIDelate c2=2;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)
	tk.MustInterDirc("drop causet if exists test")
	createALLEGROSQL := `CREATE TABLE test (
	  id        VARCHAR(36) PRIMARY KEY NOT NULL,
	  factor    INTEGER                 NOT NULL                   DEFAULT 2);`
	tk.MustInterDirc(createALLEGROSQL)
	insertALLEGROSQL := `INSERT INTO test(id) VALUES('id') ON DUPLICATE KEY UFIDelATE factor=factor+3;`
	tk.MustInterDirc(insertALLEGROSQL)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.MustInterDirc(insertALLEGROSQL)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)
	tk.MustInterDirc(insertALLEGROSQL)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)

	tk.Se.SetClientCapability(allegrosql.ClientFoundRows)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int, data int)")
	tk.MustInterDirc(`INSERT INTO t VALUES (1, 0), (0, 0), (1, 1);`)
	tk.MustInterDirc(`UFIDelATE t set id = 1 where data = 0;`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 2)
}

func (s *testStochastikSuite3) TestLastMessage(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(id TEXT)")

	// Insert
	tk.MustInterDirc(`INSERT INTO t VALUES ("a");`)
	tk.CheckLastMessage("")
	tk.MustInterDirc(`INSERT INTO t VALUES ("b"), ("c");`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")

	// UFIDelate
	tk.MustInterDirc(`UFIDelATE t set id = 'c' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 1)
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustInterDirc(`UFIDelATE t set id = 'a' where id = 'a';`)
	c.Assert(int(tk.Se.AffectedRows()), Equals, 0)
	tk.CheckLastMessage("Rows matched: 0  Changed: 0  Warnings: 0")

	// Replace
	tk.MustInterDirc(`drop causet if exists t, t1;
        create causet t (c1 int PRIMARY KEY, c2 int);
        create causet t1 (a1 int, a2 int);`)
	tk.MustInterDirc(`INSERT INTO t VALUES (1,1)`)
	tk.MustInterDirc(`REPLACE INTO t VALUES (2,2)`)
	tk.CheckLastMessage("")
	tk.MustInterDirc(`INSERT INTO t1 VALUES (1,10), (3,30);`)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	tk.MustInterDirc(`REPLACE INTO t SELECT * from t1`)
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")

	// Check insert with CLIENT_FOUND_ROWS is set
	tk.Se.SetClientCapability(allegrosql.ClientFoundRows)
	tk.MustInterDirc(`drop causet if exists t, t1;
        create causet t (c1 int PRIMARY KEY, c2 int);
        create causet t1 (a1 int, a2 int);`)
	tk.MustInterDirc(`INSERT INTO t1 VALUES (1, 10), (2, 2), (3, 30);`)
	tk.MustInterDirc(`INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);`)
	tk.MustInterDirc(`INSERT INTO t SELECT * FROM t1 ON DUPLICATE KEY UFIDelATE c2=a2;`)
	tk.CheckLastMessage("Records: 6  Duplicates: 3  Warnings: 0")
}

// TestRowLock . See http://dev.allegrosql.com/doc/refman/5.7/en/commit.html.
func (s *testStochastikSuite) TestRowLock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists t")
	txn, err := tk.Se.Txn(true)
	c.Assert(ekv.ErrInvalidTxn.Equal(err), IsTrue)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustInterDirc("create causet t (c1 int, c2 int, c3 int)")
	tk.MustInterDirc("insert t values (11, 2, 3)")
	tk.MustInterDirc("insert t values (12, 2, 3)")
	tk.MustInterDirc("insert t values (13, 2, 3)")

	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate t set c2=21 where c1=11")

	tk2.MustInterDirc("begin")
	tk2.MustInterDirc("uFIDelate t set c2=211 where c1=11")
	tk2.MustInterDirc("commit")

	// tk1 will retry and the final value is 21
	tk1.MustInterDirc("commit")

	// Check the result is correct
	tk.MustQuery("select c2 from t where c1=11").Check(testkit.Rows("21"))

	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate t set c2=21 where c1=11")

	tk2.MustInterDirc("begin")
	tk2.MustInterDirc("uFIDelate t set c2=22 where c1=12")
	tk2.MustInterDirc("commit")

	tk1.MustInterDirc("commit")
}

// TestAutocommit . See https://dev.allegrosql.com/doc/internals/en/status-flags.html
func (s *testStochastikSuite) TestAutocommit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists t;")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)
	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)
	tk.MustInterDirc("insert t values ()")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)
	tk.MustInterDirc("begin")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)
	tk.MustInterDirc("insert t values ()")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)
	tk.MustInterDirc("drop causet if exists t")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)

	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)
	tk.MustInterDirc("set autocommit=0")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Equals, 0)
	tk.MustInterDirc("insert t values ()")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Equals, 0)
	tk.MustInterDirc("commit")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Equals, 0)
	tk.MustInterDirc("drop causet if exists t")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Equals, 0)
	tk.MustInterDirc("set autocommit='On'")
	c.Assert(int(tk.Se.Status()&allegrosql.ServerStatusAutocommit), Greater, 0)

	// When autocommit is 0, transaction start ts should be the first *valid*
	// memex, rather than *any* memex.
	tk.MustInterDirc("create causet t (id int)")
	tk.MustInterDirc("set @@autocommit = 0")
	tk.MustInterDirc("rollback")
	tk.MustInterDirc("set @@autocommit = 0")
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("insert into t select 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	// TODO: MyALLEGROSQL compatibility for setting global variable.
	// tk.MustInterDirc("begin")
	// tk.MustInterDirc("insert into t values (42)")
	// tk.MustInterDirc("set @@global.autocommit = 1")
	// tk.MustInterDirc("rollback")
	// tk.MustQuery("select count(*) from t where id = 42").Check(testkit.Rows("0"))
	// Even the transaction is rollbacked, the set memex succeed.
	// tk.MustQuery("select @@global.autocommit").Rows("1")
}

// TestTxnLazyInitialize tests that when autocommit = 0, not all memex starts
// a new transaction.
func (s *testStochastikSuite) TestTxnLazyInitialize(c *C) {
	testTxnLazyInitialize(s, c, false)
	testTxnLazyInitialize(s, c, true)
}

func testTxnLazyInitialize(s *testStochastikSuite, c *C, isPessimistic bool) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int)")
	if isPessimistic {
		tk.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	}

	tk.MustInterDirc("set @@autocommit = 0")
	_, err := tk.Se.Txn(true)
	c.Assert(ekv.ErrInvalidTxn.Equal(err), IsTrue)
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustQuery("select @@milevadb_current_ts").Check(testkit.Rows("0"))
	tk.MustQuery("select @@milevadb_current_ts").Check(testkit.Rows("0"))

	// Those memex should not start a new transaction automacally.
	tk.MustQuery("select 1")
	tk.MustQuery("select @@milevadb_current_ts").Check(testkit.Rows("0"))

	tk.MustInterDirc("set @@milevadb_general_log = 0")
	tk.MustQuery("select @@milevadb_current_ts").Check(testkit.Rows("0"))

	tk.MustQuery("explain select * from t")
	tk.MustQuery("select @@milevadb_current_ts").Check(testkit.Rows("0"))

	// Begin memex should start a new transaction.
	tk.MustInterDirc("begin")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("rollback")

	tk.MustInterDirc("select * from t")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("rollback")

	tk.MustInterDirc("insert into t values (1)")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("rollback")
}

func (s *testStochastikSuite) TestGlobalVarAccessor(c *C) {
	varName := "max_allowed_packet"
	varValue := "67108864" // This is the default value for max_allowed_packet
	varValue1 := "4194305"
	varValue2 := "4194306"

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	se := tk.Se.(variable.GlobalVarAccessor)
	// Get globalSysVar twice and get the same value
	v, err := se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue)
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue)
	// Set global var to another value
	err = se.SetGlobalSysVar(varName, varValue1)
	c.Assert(err, IsNil)
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue1)
	c.Assert(tk.Se.CommitTxn(context.TODO()), IsNil)

	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	se1 := tk1.Se.(variable.GlobalVarAccessor)
	v, err = se1.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue1)
	err = se1.SetGlobalSysVar(varName, varValue2)
	c.Assert(err, IsNil)
	v, err = se1.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue2)
	c.Assert(tk1.Se.CommitTxn(context.TODO()), IsNil)

	// Make sure the change is visible to any client that accesses that global variable.
	v, err = se.GetGlobalSysVar(varName)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, varValue2)

	// For issue 10955, make sure the new stochastik load `max_execution_time` into stochastikVars.
	s.dom.GetGlobalVarsCache().Disable()
	tk1.MustInterDirc("set @@global.max_execution_time = 100")
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	c.Assert(tk2.Se.GetStochastikVars().MaxInterDircutionTime, Equals, uint64(100))
	tk1.MustInterDirc("set @@global.max_execution_time = 0")

	result := tk.MustQuery("show global variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show stochastik variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	tk.MustInterDirc("set stochastik sql_select_limit=100000000000;")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show stochastik variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 100000000000"))
	tk.MustInterDirc("set @@global.sql_select_limit = 1")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 1"))
	tk.MustInterDirc("set @@global.sql_select_limit = default")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))

	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustInterDirc("set @@global.autocommit = 0;")
	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustInterDirc("set @@global.autocommit=1")

	_, err = tk.InterDirc("set global time_zone = 'timezone'")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, variable.ErrUnknownTimeZone), IsTrue)
}

func (s *testStochastikSuite) TestGetSysVariables(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	// Test ScopeStochastik
	tk.MustInterDirc("select @@warning_count")
	tk.MustInterDirc("select @@stochastik.warning_count")
	tk.MustInterDirc("select @@local.warning_count")
	_, err := tk.InterDirc("select @@global.warning_count")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))

	// Test ScopeGlobal
	tk.MustInterDirc("select @@max_connections")
	tk.MustInterDirc("select @@global.max_connections")
	_, err = tk.InterDirc("select @@stochastik.max_connections")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))
	_, err = tk.InterDirc("select @@local.max_connections")
	c.Assert(terror.ErrorEqual(err, variable.ErrIncorrectScope), IsTrue, Commentf("err %v", err))

	// Test ScopeNone
	tk.MustInterDirc("select @@performance_schema_max_mutex_classes")
	tk.MustInterDirc("select @@global.performance_schema_max_mutex_classes")
	// For issue 19524, test
	tk.MustInterDirc("select @@stochastik.performance_schema_max_mutex_classes")
	tk.MustInterDirc("select @@local.performance_schema_max_mutex_classes")
}

func (s *testStochastikSuite) TestRetryResetStmtCtx(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet retrytxn (a int unique, b int)")
	tk.MustInterDirc("insert retrytxn values (1, 1)")
	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate retrytxn set b = b + 1 where a = 1")

	// Make retryable error.
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("uFIDelate retrytxn set b = b + 1 where a = 1")

	err := tk.Se.CommitTxn(context.TODO())
	c.Assert(err, IsNil)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
}

func (s *testStochastikSuite) TestRetryCleanTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet retrytxn (a int unique, b int)")
	tk.MustInterDirc("insert retrytxn values (1, 1)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate retrytxn set b = b + 1 where a = 1")

	// Make retryable error.
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("uFIDelate retrytxn set b = b + 1 where a = 1")

	// Hijack retry history, add a memex that returns error.
	history := stochastik.GetHistory(tk.Se)
	stmtNode, err := BerolinaSQL.New().ParseOneStmt("insert retrytxn values (2, 'a')", "", "")
	c.Assert(err, IsNil)
	compiler := interlock.Compiler{Ctx: tk.Se}
	stmt, _ := compiler.Compile(context.TODO(), stmtNode)
	interlock.ResetContextOfStmt(tk.Se, stmtNode)
	history.Add(stmt, tk.Se.GetStochastikVars().StmtCtx)
	_, err = tk.InterDirc("commit")
	c.Assert(err, NotNil)
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsFalse)
	c.Assert(tk.Se.GetStochastikVars().InTxn(), IsFalse)
}

func (s *testStochastikSuite) TestReadOnlyNotInHistory(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet history (a int)")
	tk.MustInterDirc("insert history values (1), (2), (3)")
	tk.MustInterDirc("set @@autocommit = 0")
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")
	tk.MustQuery("select * from history")
	history := stochastik.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)

	tk.MustInterDirc("insert history values (4)")
	tk.MustInterDirc("insert history values (5)")
	c.Assert(history.Count(), Equals, 2)
	tk.MustInterDirc("commit")
	tk.MustQuery("select * from history")
	history = stochastik.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)
}

func (s *testStochastikSuite) TestRetryUnion(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet history (a int)")
	tk.MustInterDirc("insert history values (1), (2), (3)")
	tk.MustInterDirc("set @@autocommit = 0")
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")
	// UNION should't be in retry history.
	tk.MustQuery("(select * from history) union (select * from history)")
	history := stochastik.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)
	tk.MustQuery("(select * from history for uFIDelate) union (select * from history)")
	tk.MustInterDirc("uFIDelate history set a = a + 1")
	history = stochastik.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 2)

	// Make retryable error.
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("uFIDelate history set a = a + 1")

	_, err := tk.InterDirc("commit")
	c.Assert(err, ErrorMatches, ".*can not retry select for uFIDelate memex")
}

func (s *testStochastikSuite) TestRetryShow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("set @@autocommit = 0")
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")
	// UNION should't be in retry history.
	tk.MustQuery("show variables")
	tk.MustQuery("show databases")
	history := stochastik.GetHistory(tk.Se)
	c.Assert(history.Count(), Equals, 0)
}

func (s *testStochastikSuite) TestNoRetryForCurrentTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet history (a int)")
	tk.MustInterDirc("insert history values (1)")

	// Firstly, disable retry.
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 1")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate history set a = 2")
	// Enable retry now.
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")

	tk1.MustInterDirc("uFIDelate history set a = 3")
	c.Assert(tk.InterDircToErr("commit"), NotNil)
}

func (s *testStochastikSuite) TestRetryForCurrentTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet history (a int)")
	tk.MustInterDirc("insert history values (1)")

	// Firstly, enable retry.
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate history set a = 2")
	// Disable retry now.
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 1")

	tk1.MustInterDirc("uFIDelate history set a = 3")
	tk.MustInterDirc("commit")
	tk.MustQuery("select * from history").Check(testkit.Rows("2"))
}

// TestTruncateAlloc tests that the auto_increment ID does not reuse the old causet's allocator.
func (s *testStochastikSuite) TestTruncateAlloc(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet truncate_id (a int primary key auto_increment)")
	tk.MustInterDirc("insert truncate_id values (), (), (), (), (), (), (), (), (), ()")
	tk.MustInterDirc("truncate causet truncate_id")
	tk.MustInterDirc("insert truncate_id values (), (), (), (), (), (), (), (), (), ()")
	tk.MustQuery("select a from truncate_id where a > 11").Check(testkit.Rows())
}

func (s *testStochastikSuite) TestString(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("select 1")
	// here to check the panic bug in String() when txn is nil after committed.
	c.Log(tk.Se.String())
}

func (s *testStochastikSuite) TestDatabase(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	// Test database.
	tk.MustInterDirc("create database xxx")
	tk.MustInterDirc("drop database xxx")

	tk.MustInterDirc("drop database if exists xxx")
	tk.MustInterDirc("create database xxx")
	tk.MustInterDirc("create database if not exists xxx")
	tk.MustInterDirc("drop database if exists xxx")

	// Test schemaReplicant.
	tk.MustInterDirc("create schemaReplicant xxx")
	tk.MustInterDirc("drop schemaReplicant xxx")

	tk.MustInterDirc("drop schemaReplicant if exists xxx")
	tk.MustInterDirc("create schemaReplicant xxx")
	tk.MustInterDirc("create schemaReplicant if not exists xxx")
	tk.MustInterDirc("drop schemaReplicant if exists xxx")
}

func (s *testStochastikSuite) TestInterDircRestrictedALLEGROSQL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	r, _, err := tk.Se.(sqlexec.RestrictedALLEGROSQLInterlockingDirectorate).InterDircRestrictedALLEGROSQL("select 1;")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 1)
}

// TestInTrans . See https://dev.allegrosql.com/doc/internals/en/status-flags.html
func (s *testStochastikSuite) TestInTrans(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("begin")
	txn, err := tk.Se.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("drop causet if exists t;")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustInterDirc("insert t values ()")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustInterDirc("commit")
	tk.MustInterDirc("insert t values ()")

	tk.MustInterDirc("set autocommit=0")
	tk.MustInterDirc("begin")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("commit")
	c.Assert(txn.Valid(), IsFalse)
	tk.MustInterDirc("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("commit")
	c.Assert(txn.Valid(), IsFalse)

	tk.MustInterDirc("set autocommit=1")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustInterDirc("begin")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("insert t values ()")
	c.Assert(txn.Valid(), IsTrue)
	tk.MustInterDirc("rollback")
	c.Assert(txn.Valid(), IsFalse)
}

func (s *testStochastikSuite) TestRetryPreparedStmt(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists t")
	txn, err := tk.Se.Txn(true)
	c.Assert(ekv.ErrInvalidTxn.Equal(err), IsTrue)
	c.Assert(txn.Valid(), IsFalse)
	tk.MustInterDirc("create causet t (c1 int, c2 int, c3 int)")
	tk.MustInterDirc("insert t values (11, 2, 3)")

	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate t set c2=? where c1=11;", 21)

	tk2.MustInterDirc("begin")
	tk2.MustInterDirc("uFIDelate t set c2=? where c1=11", 22)
	tk2.MustInterDirc("commit")

	tk1.MustInterDirc("commit")

	tk.MustQuery("select c2 from t where c1=11").Check(testkit.Rows("21"))
}

func (s *testStochastikSuite) TestStochastik(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("ROLLBACK;")
	tk.Se.Close()
}

func (s *testStochastikSuite) TestStochastikAuth(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "Any not exist username with zero password!", Hostname: "anyhost"}, []byte(""), []byte("")), IsFalse)
}

func (s *testStochastikSerialSuite) TestSkipWithGrant(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	save2 := privileges.SkipWithGrant

	privileges.SkipWithGrant = false
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "user_not_exist"}, []byte("yyy"), []byte("zzz")), IsFalse)

	privileges.SkipWithGrant = true
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: `%`}, []byte("yyy"), []byte("zzz")), IsTrue)
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, []byte(""), []byte("")), IsTrue)
	tk.MustInterDirc("create causet t (id int)")
	tk.MustInterDirc("create role r_1")
	tk.MustInterDirc("grant r_1 to root")
	tk.MustInterDirc("set role all")
	tk.MustInterDirc("show grants for root")
	privileges.SkipWithGrant = save2
}

func (s *testStochastikSuite) TestLastInsertID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	// insert
	tk.MustInterDirc("create causet t (c1 int not null auto_increment, c2 int, PRIMARY KEY (c1))")
	tk.MustInterDirc("insert into t set c2 = 11")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("1"))

	tk.MustInterDirc("insert into t (c2) values (22), (33), (44)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("2"))

	tk.MustInterDirc("insert into t (c1, c2) values (10, 55)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("2"))

	// replace
	tk.MustInterDirc("replace t (c2) values(66)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 11", "2 22", "3 33", "4 44", "10 55", "11 66"))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("11"))

	// uFIDelate
	tk.MustInterDirc("uFIDelate t set c1=last_insert_id(c1 + 100)")
	tk.MustQuery("select * from t").Check(testkit.Rows("101 11", "102 22", "103 33", "104 44", "110 55", "111 66"))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("111"))
	tk.MustInterDirc("insert into t (c2) values (77)")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("112"))

	// drop
	tk.MustInterDirc("drop causet t")
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows("112"))

	tk.MustInterDirc("create causet t (c2 int, c3 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	tk.MustInterDirc("insert into t set c2 = 30")

	// insert values
	lastInsertID := tk.Se.LastInsertID()
	tk.MustInterDirc("prepare stmt1 from 'insert into t (c2) values (?)'")
	tk.MustInterDirc("set @v1=10")
	tk.MustInterDirc("set @v2=20")
	tk.MustInterDirc("execute stmt1 using @v1")
	tk.MustInterDirc("execute stmt1 using @v2")
	tk.MustInterDirc("deallocate prepare stmt1")
	currLastInsertID := tk.Se.GetStochastikVars().StmtCtx.PrevLastInsertID
	tk.MustQuery("select c1 from t where c2 = 20").Check(testkit.Rows(fmt.Sprint(currLastInsertID)))
	c.Assert(lastInsertID+2, Equals, currLastInsertID)
}

func (s *testStochastikSuite) TestPrepareZero(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(v timestamp)")
	tk.MustInterDirc("prepare s1 from 'insert into t (v) values (?)'")
	tk.MustInterDirc("set @v1='0'")
	_, rs := tk.InterDirc("execute s1 using @v1")
	c.Assert(rs, NotNil)
	tk.MustInterDirc("set @v2='" + types.ZeroDatetimeStr + "'")
	tk.MustInterDirc("execute s1 using @v2")
	tk.MustQuery("select v from t").Check(testkit.Rows("0000-00-00 00:00:00"))
}

func (s *testStochastikSuite) TestPrimaryKeyAutoIncrement(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name varchar(255) UNIQUE NOT NULL, status int)")
	tk.MustInterDirc("insert t (name) values (?)", "abc")
	id := tk.Se.LastInsertID()
	c.Check(id != 0, IsTrue)

	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc <nil>", id)))

	tk.MustInterDirc("uFIDelate t set name = 'abc', status = 1 where id = ?", id)
	tk1.MustQuery("select * from t").Check(testkit.Rows(fmt.Sprintf("%d abc 1", id)))

	// Check for pass bool param to milevadb prepared memex
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id tinyint)")
	tk.MustInterDirc("insert t values (?)", true)
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
}

func (s *testStochastikSuite) TestAutoIncrementID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("insert t values ()")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	tk.MustInterDirc("insert t values ()")
	lastID := tk.Se.LastInsertID()
	c.Assert(lastID, Less, uint64(4))
	tk.MustInterDirc("insert t () values ()")
	c.Assert(tk.Se.LastInsertID(), Greater, lastID)
	lastID = tk.Se.LastInsertID()
	tk.MustInterDirc("insert t values (100)")
	c.Assert(tk.Se.LastInsertID(), Equals, uint64(100))

	// If the auto_increment column value is given, it uses the value of the latest event.
	tk.MustInterDirc("insert t values (120), (112)")
	c.Assert(tk.Se.LastInsertID(), Equals, uint64(112))

	// The last_insert_id function only use last auto-generated id.
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows(fmt.Sprint(lastID)))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (i tinyint unsigned not null auto_increment, primary key (i));")
	tk.MustInterDirc("insert into t set i = 254;")
	tk.MustInterDirc("insert t values ()")

	// The last insert ID doesn't care about primary key, it is set even if its a normal index column.
	tk.MustInterDirc("create causet autoid (id int auto_increment, index (id))")
	tk.MustInterDirc("insert autoid values ()")
	c.Assert(tk.Se.LastInsertID(), Greater, uint64(0))
	tk.MustInterDirc("insert autoid values (100)")
	c.Assert(tk.Se.LastInsertID(), Equals, uint64(100))

	tk.MustQuery("select last_insert_id(20)").Check(testkit.Rows(fmt.Sprint(20)))
	tk.MustQuery("select last_insert_id()").Check(testkit.Rows(fmt.Sprint(20)))

	// Corner cases for unsigned bigint auto_increment DeferredCausets.
	tk.MustInterDirc("drop causet if exists autoid")
	tk.MustInterDirc("create causet autoid(`auto_inc_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustInterDirc("insert into autoid values(9223372036854775808);")
	tk.MustInterDirc("insert into autoid values();")
	tk.MustInterDirc("insert into autoid values();")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("9223372036854775808", "9223372036854775810", "9223372036854775812"))
	// In MilevaDB : _milevadb_rowid will also consume the autoID when the auto_increment column is not the primary key.
	// Using the MaxUint64 and MaxInt64 as the autoID upper limit like MyALLEGROSQL will cause _milevadb_rowid allocation fail here.
	_, err := tk.InterDirc("insert into autoid values(18446744073709551614)")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)
	_, err = tk.InterDirc("insert into autoid values()")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)
	// FixMe: MyALLEGROSQL works fine with the this allegrosql.
	_, err = tk.InterDirc("insert into autoid values(18446744073709551615)")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)

	tk.MustInterDirc("drop causet if exists autoid")
	tk.MustInterDirc("create causet autoid(`auto_inc_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustInterDirc("insert into autoid values()")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("1"))
	tk.MustInterDirc("insert into autoid values(5000)")
	tk.MustQuery("select * from autoid").Check(testkit.Rows("1", "5000"))
	_, err = tk.InterDirc("uFIDelate autoid set auto_inc_id = 8000")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	tk.MustInterDirc("uFIDelate autoid set auto_inc_id = 9000 where auto_inc_id=1")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000"))
	tk.MustInterDirc("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000", "9001"))

	// Corner cases for signed bigint auto_increment DeferredCausets.
	tk.MustInterDirc("drop causet if exists autoid")
	tk.MustInterDirc("create causet autoid(`auto_inc_id` bigint(20) NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	// In MilevaDB : _milevadb_rowid will also consume the autoID when the auto_increment column is not the primary key.
	// Using the MaxUint64 and MaxInt64 as autoID upper limit like MyALLEGROSQL will cause insert fail if the values is
	// 9223372036854775806. Because _milevadb_rowid will be allocated 9223372036854775807 at same time.
	tk.MustInterDirc("insert into autoid values(9223372036854775805);")
	tk.MustQuery("select auto_inc_id, _milevadb_rowid from autoid use index()").Check(testkit.Rows("9223372036854775805 9223372036854775806"))
	_, err = tk.InterDirc("insert into autoid values();")
	c.Assert(terror.ErrorEqual(err, autoid.ErrAutoincReadFailed), IsTrue)
	tk.MustQuery("select auto_inc_id, _milevadb_rowid from autoid use index()").Check(testkit.Rows("9223372036854775805 9223372036854775806"))
	tk.MustQuery("select auto_inc_id, _milevadb_rowid from autoid use index(auto_inc_id)").Check(testkit.Rows("9223372036854775805 9223372036854775806"))

	tk.MustInterDirc("drop causet if exists autoid")
	tk.MustInterDirc("create causet autoid(`auto_inc_id` bigint(20) NOT NULL AUTO_INCREMENT,UNIQUE KEY `auto_inc_id` (`auto_inc_id`))")
	tk.MustInterDirc("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1"))
	tk.MustInterDirc("insert into autoid values(5000)")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	_, err = tk.InterDirc("uFIDelate autoid set auto_inc_id = 8000")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("1", "5000"))
	tk.MustInterDirc("uFIDelate autoid set auto_inc_id = 9000 where auto_inc_id=1")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000"))
	tk.MustInterDirc("insert into autoid values()")
	tk.MustQuery("select * from autoid use index()").Check(testkit.Rows("9000", "5000", "9001"))
}

func (s *testStochastikSuite) TestAutoIncrementWithRetry(c *C) {
	// test for https://github.com/whtcorpsinc/milevadb/issues/827

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("create causet t (c2 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	tk.MustInterDirc("insert into t (c2) values (1), (2), (3), (4), (5)")

	// insert values
	lastInsertID := tk.Se.LastInsertID()
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t (c2) values (11), (12), (13)")
	tk.MustQuery("select c1 from t where c2 = 11").Check(testkit.Rows("6"))
	tk.MustInterDirc("uFIDelate t set c2 = 33 where c2 = 1")

	tk1.MustInterDirc("uFIDelate t set c2 = 22 where c2 = 1")

	tk.MustInterDirc("commit")

	tk.MustQuery("select c1 from t where c2 = 11").Check(testkit.Rows("6"))
	currLastInsertID := tk.Se.GetStochastikVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+5, Equals, currLastInsertID)

	// insert set
	lastInsertID = currLastInsertID
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t set c2 = 31")
	tk.MustQuery("select c1 from t where c2 = 31").Check(testkit.Rows("9"))
	tk.MustInterDirc("uFIDelate t set c2 = 44 where c2 = 2")

	tk1.MustInterDirc("uFIDelate t set c2 = 55 where c2 = 2")

	tk.MustInterDirc("commit")

	tk.MustQuery("select c1 from t where c2 = 31").Check(testkit.Rows("9"))
	currLastInsertID = tk.Se.GetStochastikVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// replace
	lastInsertID = currLastInsertID
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t (c2) values (21), (22), (23)")
	tk.MustQuery("select c1 from t where c2 = 21").Check(testkit.Rows("10"))
	tk.MustInterDirc("uFIDelate t set c2 = 66 where c2 = 3")

	tk1.MustInterDirc("uFIDelate t set c2 = 77 where c2 = 3")

	tk.MustInterDirc("commit")

	tk.MustQuery("select c1 from t where c2 = 21").Check(testkit.Rows("10"))
	currLastInsertID = tk.Se.GetStochastikVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+1, Equals, currLastInsertID)

	// uFIDelate
	lastInsertID = currLastInsertID
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t set c2 = 41")
	tk.MustInterDirc("uFIDelate t set c1 = 0 where c2 = 41")
	tk.MustQuery("select c1 from t where c2 = 41").Check(testkit.Rows("0"))
	tk.MustInterDirc("uFIDelate t set c2 = 88 where c2 = 4")

	tk1.MustInterDirc("uFIDelate t set c2 = 99 where c2 = 4")

	tk.MustInterDirc("commit")

	tk.MustQuery("select c1 from t where c2 = 41").Check(testkit.Rows("0"))
	currLastInsertID = tk.Se.GetStochastikVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// prepare
	lastInsertID = currLastInsertID
	tk.MustInterDirc("begin")
	tk.MustInterDirc("prepare stmt from 'insert into t (c2) values (?)'")
	tk.MustInterDirc("set @v1=100")
	tk.MustInterDirc("set @v2=200")
	tk.MustInterDirc("set @v3=300")
	tk.MustInterDirc("execute stmt using @v1")
	tk.MustInterDirc("execute stmt using @v2")
	tk.MustInterDirc("execute stmt using @v3")
	tk.MustInterDirc("deallocate prepare stmt")
	tk.MustQuery("select c1 from t where c2 = 12").Check(testkit.Rows("7"))
	tk.MustInterDirc("uFIDelate t set c2 = 111 where c2 = 5")

	tk1.MustInterDirc("uFIDelate t set c2 = 222 where c2 = 5")

	tk.MustInterDirc("commit")

	tk.MustQuery("select c1 from t where c2 = 12").Check(testkit.Rows("7"))
	currLastInsertID = tk.Se.GetStochastikVars().StmtCtx.PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)
}

func (s *testStochastikSuite) TestBinaryReadOnly(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (i int key)")
	id, _, _, err := tk.Se.PrepareStmt("select i from t where i = ?")
	c.Assert(err, IsNil)
	id2, _, _, err := tk.Se.PrepareStmt("insert into t values (?)")
	c.Assert(err, IsNil)
	tk.MustInterDirc("set autocommit = 0")
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")
	_, err = tk.Se.InterDircutePreparedStmt(context.Background(), id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 0)
	tk.MustInterDirc("insert into t values (1)")
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 1)
	_, err = tk.Se.InterDircutePreparedStmt(context.Background(), id2, []types.Causet{types.NewCauset(2)})
	c.Assert(err, IsNil)
	c.Assert(stochastik.GetHistory(tk.Se).Count(), Equals, 2)
	tk.MustInterDirc("commit")
}

func (s *testStochastikSuite) TestPrepare(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t(id TEXT)")
	tk.MustInterDirc(`INSERT INTO t VALUES ("id");`)
	id, ps, _, err := tk.Se.PrepareStmt("select id+? from t")
	ctx := context.Background()
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint32(1))
	c.Assert(ps, Equals, 1)
	tk.MustInterDirc(`set @a=1`)
	_, err = tk.Se.InterDircutePreparedStmt(ctx, id, []types.Causet{types.NewCauset("1")})
	c.Assert(err, IsNil)
	err = tk.Se.DropPreparedStmt(id)
	c.Assert(err, IsNil)

	tk.MustInterDirc("prepare stmt from 'select 1+?'")
	tk.MustInterDirc("set @v1=100")
	tk.MustQuery("execute stmt using @v1").Check(testkit.Rows("101"))

	tk.MustInterDirc("set @v2=200")
	tk.MustQuery("execute stmt using @v2").Check(testkit.Rows("201"))

	tk.MustInterDirc("set @v3=300")
	tk.MustQuery("execute stmt using @v3").Check(testkit.Rows("301"))
	tk.MustInterDirc("deallocate prepare stmt")

	// InterDircute prepared memexs for more than one time.
	tk.MustInterDirc("create causet multiexec (a int, b int)")
	tk.MustInterDirc("insert multiexec values (1, 1), (2, 2)")
	id, _, _, err = tk.Se.PrepareStmt("select a from multiexec where b = ? order by b")
	c.Assert(err, IsNil)
	rs, err := tk.Se.InterDircutePreparedStmt(ctx, id, []types.Causet{types.NewCauset(1)})
	c.Assert(err, IsNil)
	rs.Close()
	rs, err = tk.Se.InterDircutePreparedStmt(ctx, id, []types.Causet{types.NewCauset(2)})
	rs.Close()
	c.Assert(err, IsNil)
}

func (s *testStochastikSuite2) TestSpecifyIndexPrefixLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	_, err := tk.InterDirc("create causet t (c1 char, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create causet t (c1 int, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create causet t (c1 bit(10), index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	tk.MustInterDirc("create causet t (c1 char, c2 int, c3 bit(10));")

	_, err = tk.InterDirc("create index idx_c1 on t (c1(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create index idx_c1 on t (c2(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create index idx_c1 on t (c3(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	tk.MustInterDirc("drop causet if exists t;")

	_, err = tk.InterDirc("create causet t (c1 int, c2 blob, c3 varchar(64), index(c2));")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	c.Assert(err, NotNil)

	tk.MustInterDirc("create causet t (c1 int, c2 blob, c3 varchar(64));")
	_, err = tk.InterDirc("create index idx_c1 on t (c2);")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create index idx_c1 on t (c2(555555));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	_, err = tk.InterDirc("create index idx_c1 on t (c1(5))")
	// ERROR 1089 (HY000): Incorrect prefix key;
	// the used key part isn't a string, the used length is longer than the key part,
	// or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	tk.MustInterDirc("create index idx_c1 on t (c1);")
	tk.MustInterDirc("create index idx_c2 on t (c2(3));")
	tk.MustInterDirc("create unique index idx_c3 on t (c3(5));")

	tk.MustInterDirc("insert into t values (3, 'abc', 'def');")
	tk.MustQuery("select c2 from t where c2 = 'abc';").Check(testkit.Rows("abc"))

	tk.MustInterDirc("insert into t values (4, 'abcd', 'xxx');")
	tk.MustInterDirc("insert into t values (4, 'abcf', 'yyy');")
	tk.MustQuery("select c2 from t where c2 = 'abcf';").Check(testkit.Rows("abcf"))
	tk.MustQuery("select c2 from t where c2 = 'abcd';").Check(testkit.Rows("abcd"))

	tk.MustInterDirc("insert into t values (4, 'ignore', 'abcdeXXX');")
	_, err = tk.InterDirc("insert into t values (5, 'ignore', 'abcdeYYY');")
	// ERROR 1062 (23000): Duplicate entry 'abcde' for key 'idx_c3'
	c.Assert(err, NotNil)
	tk.MustQuery("select c3 from t where c3 = 'abcde';").Check(testkit.Rows())

	tk.MustInterDirc("delete from t where c3 = 'abcdeXXX';")
	tk.MustInterDirc("delete from t where c2 = 'abc';")

	tk.MustQuery("select c2 from t where c2 > 'abcd';").Check(testkit.Rows("abcf"))
	tk.MustQuery("select c2 from t where c2 < 'abcf';").Check(testkit.Rows("abcd"))
	tk.MustQuery("select c2 from t where c2 >= 'abcd';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 <= 'abcf';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 != 'abc';").Check(testkit.Rows("abcd", "abcf"))
	tk.MustQuery("select c2 from t where c2 != 'abcd';").Check(testkit.Rows("abcf"))

	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t1 (a int, b char(255), key(a, b(20)));")
	tk.MustInterDirc("insert into t1 values (0, '1');")
	tk.MustInterDirc("uFIDelate t1 set b = b + 1 where a = 0;")
	tk.MustQuery("select b from t1 where a = 0;").Check(testkit.Rows("2"))

	// test union index.
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a text, b text, c int, index (a(3), b(3), c));")
	tk.MustInterDirc("insert into t values ('abc', 'abcd', 1);")
	tk.MustInterDirc("insert into t values ('abcx', 'abcf', 2);")
	tk.MustInterDirc("insert into t values ('abcy', 'abcf', 3);")
	tk.MustInterDirc("insert into t values ('bbc', 'abcd', 4);")
	tk.MustInterDirc("insert into t values ('bbcz', 'abcd', 5);")
	tk.MustInterDirc("insert into t values ('cbck', 'abd', 6);")
	tk.MustQuery("select c from t where a = 'abc' and b <= 'abc';").Check(testkit.Rows())
	tk.MustQuery("select c from t where a = 'abc' and b <= 'abd';").Check(testkit.Rows("1"))
	tk.MustQuery("select c from t where a < 'cbc' and b > 'abcd';").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select c from t where a <= 'abd' and b > 'abc';").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select c from t where a < 'bbcc' and b = 'abcd';").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select c from t where a > 'bbcf';").Check(testkit.Rows("5", "6"))
}

func (s *testStochastikSuite) TestResultField(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (id int);")

	tk.MustInterDirc(`INSERT INTO t VALUES (1);`)
	tk.MustInterDirc(`INSERT INTO t VALUES (2);`)
	r, err := tk.InterDirc(`SELECT count(*) from t;`)
	c.Assert(err, IsNil)
	fields := r.Fields()
	c.Assert(err, IsNil)
	c.Assert(len(fields), Equals, 1)
	field := fields[0].DeferredCauset
	c.Assert(field.Tp, Equals, allegrosql.TypeLonglong)
	c.Assert(field.Flen, Equals, 21)
}

func (s *testStochastikSuite) TestResultType(c *C) {
	// Testcase for https://github.com/whtcorpsinc/milevadb/issues/325
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	rs, err := tk.InterDirc(`select cast(null as char(30))`)
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(req.GetRow(0).IsNull(0), IsTrue)
	c.Assert(rs.Fields()[0].DeferredCauset.FieldType.Tp, Equals, allegrosql.TypeVarString)
}

func (s *testStochastikSuite) TestFieldText(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (a int)")
	tests := []struct {
		allegrosql   string
		field string
	}{
		{"select distinct(a) from t", "a"},
		{"select (1)", "1"},
		{"select (1+1)", "(1+1)"},
		{"select a from t", "a"},
		{"select        ((a+1))     from t", "((a+1))"},
		{"select 1 /*!32301 +1 */;", "1  +1 "},
		{"select /*!32301 1  +1 */;", "1  +1 "},
		{"/*!32301 select 1  +1 */;", "1  +1 "},
		{"select 1 + /*!32301 1 +1 */;", "1 +  1 +1 "},
		{"select 1 /*!32301 + 1, 1 */;", "1  + 1"},
		{"select /*!32301 1, 1 +1 */;", "1"},
		{"select /*!32301 1 + 1, */ +1;", "1 + 1"},
	}
	for _, tt := range tests {
		result, err := tk.InterDirc(tt.allegrosql)
		c.Assert(err, IsNil)
		c.Assert(result.Fields()[0].DeferredCausetAsName.O, Equals, tt.field)
	}
}

func (s *testStochastikSuite3) TestIndexMaxLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create database test_index_max_length")
	tk.MustInterDirc("use test_index_max_length")

	// create simple index at causet creation
	tk.MustGetErrCode("create causet t (c1 varchar(3073), index(c1)) charset = ascii;", allegrosql.ErrTooLongKey)

	// create simple index after causet creation
	tk.MustInterDirc("create causet t (c1 varchar(3073)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1 on t(c1) ", allegrosql.ErrTooLongKey)
	tk.MustInterDirc("drop causet t;")

	// create compound index at causet creation
	tk.MustGetErrCode("create causet t (c1 varchar(3072), c2 varchar(1), index(c1, c2)) charset = ascii;", allegrosql.ErrTooLongKey)
	tk.MustGetErrCode("create causet t (c1 varchar(3072), c2 char(1), index(c1, c2)) charset = ascii;", allegrosql.ErrTooLongKey)
	tk.MustGetErrCode("create causet t (c1 varchar(3072), c2 char, index(c1, c2)) charset = ascii;", allegrosql.ErrTooLongKey)
	tk.MustGetErrCode("create causet t (c1 varchar(3072), c2 date, index(c1, c2)) charset = ascii;", allegrosql.ErrTooLongKey)
	tk.MustGetErrCode("create causet t (c1 varchar(3069), c2 timestamp(1), index(c1, c2)) charset = ascii;", allegrosql.ErrTooLongKey)

	tk.MustInterDirc("create causet t (c1 varchar(3068), c2 bit(26), index(c1, c2)) charset = ascii;") // 26 bit = 4 bytes
	tk.MustInterDirc("drop causet t;")
	tk.MustInterDirc("create causet t (c1 varchar(3068), c2 bit(32), index(c1, c2)) charset = ascii;") // 32 bit = 4 bytes
	tk.MustInterDirc("drop causet t;")
	tk.MustGetErrCode("create causet t (c1 varchar(3068), c2 bit(33), index(c1, c2)) charset = ascii;", allegrosql.ErrTooLongKey)

	// create compound index after causet creation
	tk.MustInterDirc("create causet t (c1 varchar(3072), c2 varchar(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", allegrosql.ErrTooLongKey)
	tk.MustInterDirc("drop causet t;")

	tk.MustInterDirc("create causet t (c1 varchar(3072), c2 char(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", allegrosql.ErrTooLongKey)
	tk.MustInterDirc("drop causet t;")

	tk.MustInterDirc("create causet t (c1 varchar(3072), c2 char) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", allegrosql.ErrTooLongKey)
	tk.MustInterDirc("drop causet t;")

	tk.MustInterDirc("create causet t (c1 varchar(3072), c2 date) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", allegrosql.ErrTooLongKey)
	tk.MustInterDirc("drop causet t;")

	tk.MustInterDirc("create causet t (c1 varchar(3069), c2 timestamp(1)) charset = ascii;")
	tk.MustGetErrCode("create index idx_c1_c2 on t(c1, c2);", allegrosql.ErrTooLongKey)
	tk.MustInterDirc("drop causet t;")

	// Test charsets other than `ascii`.
	assertCharsetLimit := func(charset string, bytesPerChar int) {
		base := 3072 / bytesPerChar
		tk.MustGetErrCode(fmt.Sprintf("create causet t (a varchar(%d) primary key) charset=%s", base+1, charset), allegrosql.ErrTooLongKey)
		tk.MustInterDirc(fmt.Sprintf("create causet t (a varchar(%d) primary key) charset=%s", base, charset))
		tk.MustInterDirc("drop causet if exists t")
	}
	assertCharsetLimit("binary", 1)
	assertCharsetLimit("latin1", 1)
	assertCharsetLimit("utf8", 3)
	assertCharsetLimit("utf8mb4", 4)

	// Test types bit length limit.
	assertTypeLimit := func(tp string, limitBitLength int) {
		base := 3072 - limitBitLength
		tk.MustGetErrCode(fmt.Sprintf("create causet t (a blob(10000), b %s, index idx(a(%d), b))", tp, base+1), allegrosql.ErrTooLongKey)
		tk.MustInterDirc(fmt.Sprintf("create causet t (a blob(10000), b %s, index idx(a(%d), b))", tp, base))
		tk.MustInterDirc("drop causet if exists t")
	}

	assertTypeLimit("tinyint", 1)
	assertTypeLimit("smallint", 2)
	assertTypeLimit("mediumint", 3)
	assertTypeLimit("int", 4)
	assertTypeLimit("integer", 4)
	assertTypeLimit("bigint", 8)
	assertTypeLimit("float", 4)
	assertTypeLimit("float(24)", 4)
	assertTypeLimit("float(25)", 8)
	assertTypeLimit("decimal(9)", 4)
	assertTypeLimit("decimal(10)", 5)
	assertTypeLimit("decimal(17)", 8)
	assertTypeLimit("year", 1)
	assertTypeLimit("date", 3)
	assertTypeLimit("time", 3)
	assertTypeLimit("datetime", 8)
	assertTypeLimit("timestamp", 4)
}

func (s *testStochastikSuite2) TestIndexDeferredCausetLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (c1 int, c2 blob);")
	tk.MustInterDirc("create index idx_c1 on t(c1);")
	tk.MustInterDirc("create index idx_c2 on t(c2(6));")

	is := s.dom.SchemaReplicant()
	tab, err2 := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err2, Equals, nil)

	idxC1DefCauss := blocks.FindIndexByDefCausName(tab, "c1").Meta().DeferredCausets
	c.Assert(idxC1DefCauss[0].Length, Equals, types.UnspecifiedLength)

	idxC2DefCauss := blocks.FindIndexByDefCausName(tab, "c2").Meta().DeferredCausets
	c.Assert(idxC2DefCauss[0].Length, Equals, 6)
}

func (s *testStochastikSuite2) TestIgnoreForeignKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	sqlText := `CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`
	tk.MustInterDirc(sqlText)
}

// TestISDeferredCausets tests information_schema.columns.
func (s *testStochastikSuite3) TestISDeferredCausets(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("select ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS;")
	tk.MustQuery("SELECT CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.CHARACTER_SETS WHERE CHARACTER_SET_NAME = 'utf8mb4'").Check(testkit.Rows("utf8mb4"))
}

func (s *testStochastikSuite2) TestRetry(c *C) {
	// For https://github.com/whtcorpsinc/milevadb/issues/571
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("begin")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c int)")
	tk.MustInterDirc("insert t values (1), (2), (3)")
	tk.MustInterDirc("commit")

	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3.MustInterDirc("SET SESSION autocommit=0;")
	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk2.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk3.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")

	var wg sync.WaitGroup
	wg.Add(3)
	f1 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			tk1.MustInterDirc("uFIDelate t set c = 1;")
		}
	}
	f2 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			tk2.MustInterDirc("uFIDelate t set c = ?;", 1)
		}
	}
	f3 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			tk3.MustInterDirc("begin")
			tk3.MustInterDirc("uFIDelate t set c = 1;")
			tk3.MustInterDirc("commit")
		}
	}
	go f1()
	go f2()
	go f3()
	wg.Wait()
}

func (s *testStochastikSuite3) TestMultiStmts(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1; create causet t1(id int ); insert into t1 values (1);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1"))
}

func (s *testStochastikSuite2) TestLastInterDircuteDBSFlag(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1(id int)")
	c.Assert(tk.Se.Value(stochastikctx.LastInterDircuteDBS), NotNil)
	tk.MustInterDirc("insert into t1 values (1)")
	c.Assert(tk.Se.Value(stochastikctx.LastInterDircuteDBS), IsNil)
}

func (s *testStochastikSuite3) TestDecimal(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a decimal unique);")
	tk.MustInterDirc("insert t values ('100');")
	_, err := tk.InterDirc("insert t values ('1e2');")
	c.Check(err, NotNil)
}

func (s *testStochastikSuite2) TestBerolinaSQL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	// test for https://github.com/whtcorpsinc/milevadb/pull/177
	tk.MustInterDirc("CREATE TABLE `t1` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	tk.MustInterDirc("CREATE TABLE `t2` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	tk.MustInterDirc(`INSERT INTO t1 VALUES (1,1,1);`)
	tk.MustInterDirc(`INSERT INTO t2 VALUES (1,1,1);`)
	tk.MustInterDirc(`PREPARE my_stmt FROM "SELECT t1.b, count(*) FROM t1 group by t1.b having count(*) > ALL (SELECT COUNT(*) FROM t2 WHERE t2.a=1 GROUP By t2.b)";`)
	tk.MustInterDirc(`EXECUTE my_stmt;`)
	tk.MustInterDirc(`EXECUTE my_stmt;`)
	tk.MustInterDirc(`deallocate prepare my_stmt;`)
	tk.MustInterDirc(`drop causet t1,t2;`)
}

func (s *testStochastikSuite3) TestOnDuplicate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	// test for https://github.com/whtcorpsinc/milevadb/pull/454
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int, c2 int, c3 int);")
	tk.MustInterDirc("insert into t1 set c1=1, c2=2, c3=1;")
	tk.MustInterDirc("create causet t (c1 int, c2 int, c3 int, primary key (c1));")
	tk.MustInterDirc("insert into t set c1=1, c2=4;")
	tk.MustInterDirc("insert into t select * from t1 limit 1 on duplicate key uFIDelate c3=3333;")
}

func (s *testStochastikSuite2) TestReplace(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	// test for https://github.com/whtcorpsinc/milevadb/pull/456
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int, c2 int, c3 int);")
	tk.MustInterDirc("replace into t1 set c1=1, c2=2, c3=1;")
	tk.MustInterDirc("create causet t (c1 int, c2 int, c3 int, primary key (c1));")
	tk.MustInterDirc("replace into t set c1=1, c2=4;")
	tk.MustInterDirc("replace into t select * from t1 limit 1;")
}

func (s *testStochastikSuite3) TestDelete(c *C) {
	// test for https://github.com/whtcorpsinc/milevadb/pull/1135

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustInterDirc("create database test1")
	tk1.MustInterDirc("use test1")
	tk1.MustInterDirc("create causet t (F1 VARCHAR(30));")
	tk1.MustInterDirc("insert into t (F1) values ('1'), ('4');")

	tk.MustInterDirc("create causet t (F1 VARCHAR(30));")
	tk.MustInterDirc("insert into t (F1) values ('1'), ('2');")
	tk.MustInterDirc("delete m1 from t m2,t m1 where m1.F1>1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (F1 VARCHAR(30));")
	tk.MustInterDirc("insert into t (F1) values ('1'), ('2');")
	tk.MustInterDirc("delete m1 from t m1,t m2 where true and m1.F1<2;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("2"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (F1 VARCHAR(30));")
	tk.MustInterDirc("insert into t (F1) values ('1'), ('2');")
	tk.MustInterDirc("delete m1 from t m1,t m2 where false;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1", "2"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (F1 VARCHAR(30));")
	tk.MustInterDirc("insert into t (F1) values ('1'), ('2');")
	tk.MustInterDirc("delete m1, m2 from t m1,t m2 where m1.F1>m2.F1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows())

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (F1 VARCHAR(30));")
	tk.MustInterDirc("insert into t (F1) values ('1'), ('2');")
	tk.MustInterDirc("delete test1.t from test1.t inner join test.t where test1.t.F1 > test.t.F1")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("1"))
}

func (s *testStochastikSuite2) TestResetCtx(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("create causet t (i int auto_increment not null key);")
	tk.MustInterDirc("insert into t values (1);")
	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t values (10);")
	tk.MustInterDirc("uFIDelate t set i = i + row_count();")
	tk.MustQuery("select * from t;").Check(testkit.Rows("2", "11"))

	tk1.MustInterDirc("uFIDelate t set i = 0 where i = 1;")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("0"))

	tk.MustInterDirc("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1", "11"))

	tk.MustInterDirc("delete from t where i = 11;")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into t values ();")
	tk.MustInterDirc("uFIDelate t set i = i + last_insert_id() + 1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("14", "25"))

	tk1.MustInterDirc("uFIDelate t set i = 0 where i = 1;")
	tk1.MustQuery("select * from t;").Check(testkit.Rows("0"))

	tk.MustInterDirc("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("13", "25"))
}

func (s *testStochastikSuite3) TestUnique(c *C) {
	// test for https://github.com/whtcorpsinc/milevadb/pull/461

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc(`CREATE TABLE test ( id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, val int UNIQUE, PRIMARY KEY (id)); `)
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into test(id, val) values(1, 1);")
	tk1.MustInterDirc("begin;")
	tk1.MustInterDirc("insert into test(id, val) values(2, 2);")
	tk2.MustInterDirc("begin;")
	tk2.MustInterDirc("insert into test(id, val) values(1, 2);")
	tk2.MustInterDirc("commit;")
	_, err := tk.InterDirc("commit")
	c.Assert(err, NotNil)
	// Check error type and error message
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue, Commentf("err %v", err))
	c.Assert(err.Error(), Equals, "previous memex: insert into test(id, val) values(1, 1);: [ekv:1062]Duplicate entry '1' for key 'PRIMARY'")

	_, err = tk1.InterDirc("commit")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue, Commentf("err %v", err))
	c.Assert(err.Error(), Equals, "previous memex: insert into test(id, val) values(2, 2);: [ekv:1062]Duplicate entry '2' for key 'val'")

	// Test for https://github.com/whtcorpsinc/milevadb/issues/463
	tk.MustInterDirc("drop causet test;")
	tk.MustInterDirc(`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val int UNIQUE,
			PRIMARY KEY (id)
		);`)
	tk.MustInterDirc("insert into test(id, val) values(1, 1);")
	_, err = tk.InterDirc("insert into test(id, val) values(2, 1);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("insert into test(id, val) values(2, 2);")

	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into test(id, val) values(3, 3);")
	_, err = tk.InterDirc("insert into test(id, val) values(4, 3);")
	c.Assert(err, NotNil)
	tk.MustInterDirc("insert into test(id, val) values(4, 4);")
	tk.MustInterDirc("commit;")

	tk1.MustInterDirc("begin;")
	tk1.MustInterDirc("insert into test(id, val) values(5, 6);")
	tk.MustInterDirc("begin;")
	tk.MustInterDirc("insert into test(id, val) values(20, 6);")
	tk.MustInterDirc("commit;")
	tk1.InterDirc("commit")
	tk1.MustInterDirc("insert into test(id, val) values(5, 5);")

	tk.MustInterDirc("drop causet test;")
	tk.MustInterDirc(`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val1 int UNIQUE,
			val2 int UNIQUE,
			PRIMARY KEY (id)
		);`)
	tk.MustInterDirc("insert into test(id, val1, val2) values(1, 1, 1);")
	tk.MustInterDirc("insert into test(id, val1, val2) values(2, 2, 2);")
	tk.InterDirc("uFIDelate test set val1 = 3, val2 = 2 where id = 1;")
	tk.MustInterDirc("insert into test(id, val1, val2) values(3, 3, 3);")
}

func (s *testStochastikSuite2) TestSet(c *C) {
	// Test for https://github.com/whtcorpsinc/milevadb/issues/1114

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("set @tmp = 0")
	tk.MustInterDirc("set @tmp := @tmp + 1")
	tk.MustQuery("select @tmp").Check(testkit.Rows("1"))
	tk.MustQuery("select @tmp1 = 1, @tmp2 := 2").Check(testkit.Rows("<nil> 2"))
	tk.MustQuery("select @tmp1 := 11, @tmp2").Check(testkit.Rows("11 2"))

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c int);")
	tk.MustInterDirc("insert into t values (1),(2);")
	tk.MustInterDirc("uFIDelate t set c = 3 WHERE c = @var:= 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("3", "2"))
	tk.MustQuery("select @tmp := count(*) from t").Check(testkit.Rows("2"))
	tk.MustQuery("select @tmp := c-2 from t where c=3").Check(testkit.Rows("1"))
}

func (s *testStochastikSuite3) TestMyALLEGROSQLTypes(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery(`select 0x01 + 1, x'4D7953514C' = "MyALLEGROSQL"`).Check(testkit.Rows("2 1"))
	tk.MustQuery(`select 0b01 + 1, 0b01000001 = "A"`).Check(testkit.Rows("2 1"))
}

func (s *testStochastikSuite2) TestIssue986(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	sqlText := `CREATE TABLE address (
 		id bigint(20) NOT NULL AUTO_INCREMENT,
 		PRIMARY KEY (id));`
	tk.MustInterDirc(sqlText)
	tk.MustInterDirc(`insert into address values ('10')`)
}

func (s *testStochastikSuite3) TestCast(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustQuery("select cast(0.5 as unsigned)")
	tk.MustQuery("select cast(-0.5 as signed)")
	tk.MustQuery("select hex(cast(0x10 as binary(2)))").Check(testkit.Rows("1000"))
}

func (s *testStochastikSuite2) TestTableInfoMeta(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	checkResult := func(affectedRows uint64, insertID uint64) {
		gotRows := tk.Se.AffectedRows()
		c.Assert(gotRows, Equals, affectedRows)

		gotID := tk.Se.LastInsertID()
		c.Assert(gotID, Equals, insertID)
	}

	// create causet
	tk.MustInterDirc("CREATE TABLE tbl_test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// insert data
	tk.MustInterDirc(`INSERT INTO tbl_test VALUES (1, "hello");`)
	checkResult(1, 0)

	tk.MustInterDirc(`INSERT INTO tbl_test VALUES (2, "hello");`)
	checkResult(1, 0)

	tk.MustInterDirc(`UFIDelATE tbl_test SET name = "abc" where id = 2;`)
	checkResult(1, 0)

	tk.MustInterDirc(`DELETE from tbl_test where id = 2;`)
	checkResult(1, 0)

	// select data
	tk.MustQuery("select * from tbl_test").Check(testkit.Rows("1 hello"))
}

func (s *testStochastikSuite3) TestCaseInsensitive(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("create causet T (a text, B int)")
	tk.MustInterDirc("insert t (A, b) values ('aaa', 1)")
	rs, _ := tk.InterDirc("select * from t")
	fields := rs.Fields()
	c.Assert(fields[0].DeferredCausetAsName.O, Equals, "a")
	c.Assert(fields[1].DeferredCausetAsName.O, Equals, "B")
	rs, _ = tk.InterDirc("select A, b from t")
	fields = rs.Fields()
	c.Assert(fields[0].DeferredCausetAsName.O, Equals, "A")
	c.Assert(fields[1].DeferredCausetAsName.O, Equals, "b")
	rs, _ = tk.InterDirc("select a as A from t where A > 0")
	fields = rs.Fields()
	c.Assert(fields[0].DeferredCausetAsName.O, Equals, "A")
	tk.MustInterDirc("uFIDelate T set b = B + 1")
	tk.MustInterDirc("uFIDelate T set B = b + 1")
	tk.MustQuery("select b from T").Check(testkit.Rows("3"))
}

// TestDeletePanic is for delete panic
func (s *testStochastikSuite2) TestDeletePanic(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (c int)")
	tk.MustInterDirc("insert into t values (1), (2), (3)")
	tk.MustInterDirc("delete from `t` where `c` = ?", 1)
	tk.MustInterDirc("delete from `t` where `c` = ?", 2)
}

func (s *testStochastikSuite2) TestInformationSchemaCreateTime(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (c int)")
	ret := tk.MustQuery("select create_time from information_schema.blocks where block_name='t';")
	// Make sure t1 is greater than t.
	time.Sleep(time.Second)
	tk.MustInterDirc("alter causet t modify c int default 11")
	ret1 := tk.MustQuery("select create_time from information_schema.blocks where block_name='t';")
	t, err := types.ParseDatetime(nil, ret.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	t1, err := types.ParseDatetime(nil, ret1.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	r := t1.Compare(t)
	c.Assert(r, Equals, 1)
}

type testSchemaSuiteBase struct {
	cluster cluster.Cluster
	causetstore   ekv.CausetStorage
	dom     *petri.Petri
}

type testSchemaSuite struct {
	testSchemaSuiteBase
}

type testSchemaSerialSuite struct {
	testSchemaSuiteBase
}

func (s *testSchemaSuiteBase) TearDownTest(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		tk.MustInterDirc(fmt.Sprintf("drop causet %v", blockName))
	}
}

func (s *testSchemaSuiteBase) SetUpSuite(c *C) {
	testleak.BeforeTest()
	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	stochastik.DisableStats4Test()
	dom, err := stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.dom = dom
}

func (s *testSchemaSuiteBase) TearDownSuite(c *C) {
	s.dom.Close()
	s.causetstore.Close()
	testleak.AfterTest(c)()
}

func (s *testSchemaSerialSuite) TestLoadSchemaFailed(c *C) {
	atomic.StoreInt32(&petri.SchemaOutOfDateRetryTimes, int32(3))
	atomic.StoreInt64(&petri.SchemaOutOfDateRetryInterval, int64(20*time.Millisecond))
	defer func() {
		atomic.StoreInt32(&petri.SchemaOutOfDateRetryTimes, 10)
		atomic.StoreInt64(&petri.SchemaOutOfDateRetryInterval, int64(500*time.Millisecond))
	}()

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("create causet t (a int);")
	tk.MustInterDirc("create causet t1 (a int);")
	tk.MustInterDirc("create causet t2 (a int);")

	tk1.MustInterDirc("begin")
	tk2.MustInterDirc("begin")

	// Make sure loading information schemaReplicant is failed and server is invalid.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/petri/ErrorMockReloadFailed", `return(true)`), IsNil)
	err := petri.GetPetri(tk.Se).Reload()
	c.Assert(err, NotNil)

	lease := petri.GetPetri(tk.Se).DBS().GetLease()
	time.Sleep(lease * 2)

	// Make sure executing insert memex is failed when server is invalid.
	_, err = tk.InterDirc("insert t values (100);")
	c.Check(err, NotNil)

	tk1.MustInterDirc("insert t1 values (100);")
	tk2.MustInterDirc("insert t2 values (100);")

	_, err = tk1.InterDirc("commit")
	c.Check(err, NotNil)

	ver, err := s.causetstore.CurrentVersion()
	c.Assert(err, IsNil)
	c.Assert(ver, NotNil)

	failpoint.Disable("github.com/whtcorpsinc/milevadb/petri/ErrorMockReloadFailed")
	time.Sleep(lease * 2)

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a int);")
	tk.MustInterDirc("insert t values (100);")
	// Make sure insert to causet t2 transaction executes.
	tk2.MustInterDirc("commit")
}

func (s *testSchemaSerialSuite) TestSchemaCheckerALLEGROSQL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)

	// create causet
	tk.MustInterDirc(`create causet t (id int, c int);`)
	tk.MustInterDirc(`create causet t1 (id int, c int);`)
	// insert data
	tk.MustInterDirc(`insert into t values(1, 1);`)

	// The schemaReplicant version is out of date in the first transaction, but the ALLEGROALLEGROSQL can be retried.
	tk.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc(`begin;`)
	tk1.MustInterDirc(`alter causet t add index idx(c);`)
	tk.MustInterDirc(`insert into t values(2, 2);`)
	tk.MustInterDirc(`commit;`)

	// The schemaReplicant version is out of date in the first transaction, and the ALLEGROALLEGROSQL can't be retried.
	atomic.StoreUint32(&stochastik.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&stochastik.SchemaChangedWithoutRetry, 0)
	}()
	tk.MustInterDirc(`begin;`)
	tk1.MustInterDirc(`alter causet t modify column c bigint;`)
	tk.MustInterDirc(`insert into t values(3, 3);`)
	_, err := tk.InterDirc(`commit;`)
	c.Assert(terror.ErrorEqual(err, petri.ErrSchemaReplicantChanged), IsTrue, Commentf("err %v", err))

	// But the transaction related causet IDs aren't in the uFIDelated causet IDs.
	tk.MustInterDirc(`begin;`)
	tk1.MustInterDirc(`alter causet t add index idx2(c);`)
	tk.MustInterDirc(`insert into t1 values(4, 4);`)
	tk.MustInterDirc(`commit;`)

	// Test for "select for uFIDelate".
	tk.MustInterDirc(`begin;`)
	tk1.MustInterDirc(`alter causet t add index idx3(c);`)
	tk.MustQuery(`select * from t for uFIDelate`)
	_, err = tk.InterDirc(`commit;`)
	c.Assert(err, NotNil)
}

func (s *testSchemaSuite) TestPrepareStmtCommitWhenSchemaChanged(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("create causet t (a int, b int)")
	tk1.MustInterDirc("prepare stmt from 'insert into t values (?, ?)'")
	tk1.MustInterDirc("set @a = 1")

	// Commit find unrelated schemaReplicant change.
	tk1.MustInterDirc("begin")
	tk.MustInterDirc("create causet t1 (id int)")
	tk1.MustInterDirc("execute stmt using @a, @a")
	tk1.MustInterDirc("commit")

	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustInterDirc("begin")
	tk.MustInterDirc("alter causet t drop column b")
	tk1.MustInterDirc("execute stmt using @a, @a")
	_, err := tk1.InterDirc("commit")
	c.Assert(terror.ErrorEqual(err, causetcore.ErrWrongValueCountOnRow), IsTrue, Commentf("err %v", err))
}

func (s *testSchemaSuite) TestCommitWhenSchemaChanged(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (a int, b int)")

	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("insert into t values (1, 1)")

	tk.MustInterDirc("alter causet t drop column b")

	// When tk1 commit, it will find schemaReplicant already changed.
	tk1.MustInterDirc("insert into t values (4, 4)")
	_, err := tk1.InterDirc("commit")
	c.Assert(terror.ErrorEqual(err, causetcore.ErrWrongValueCountOnRow), IsTrue, Commentf("err %v", err))
}

func (s *testSchemaSuite) TestRetrySchemaChangeForEmptyChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (i int)")
	tk.MustInterDirc("create causet t1 (i int)")
	tk.MustInterDirc("begin")
	tk1.MustInterDirc("alter causet t add j int")
	tk.MustInterDirc("select * from t for uFIDelate")
	tk.MustInterDirc("uFIDelate t set i = -i")
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t1 values (1)")
	tk.MustInterDirc("commit")

	tk.MustInterDirc("begin pessimistic")
	tk1.MustInterDirc("alter causet t add k int")
	tk.MustInterDirc("select * from t for uFIDelate")
	tk.MustInterDirc("uFIDelate t set i = -i")
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t1 values (1)")
	tk.MustInterDirc("commit")
}

func (s *testSchemaSuite) TestRetrySchemaChange(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (a int primary key, b int)")
	tk.MustInterDirc("insert into t values (1, 1)")

	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate t set b = 5 where a = 1")

	tk.MustInterDirc("alter causet t add index b_i (b)")

	run := false
	hook := func() {
		if !run {
			tk.MustInterDirc("uFIDelate t set b = 3 where a = 1")
			run = true
		}
	}

	// In order to cover a bug that memex history is not uFIDelated during retry.
	// See https://github.com/whtcorpsinc/milevadb/pull/5202
	// Step1: when tk1 commit, it find schemaReplicant changed and retry().
	// Step2: during retry, hook() is called, tk uFIDelate primary key.
	// Step3: tk1 continue commit in retry() meet a retryable error(write conflict), retry again.
	// Step4: tk1 retry() success, if it use the stale memex, data and index will inconsistent.
	fpName := "github.com/whtcorpsinc/milevadb/stochastik/preCommitHook"
	c.Assert(failpoint.Enable(fpName, "return"), IsNil)
	defer func() { c.Assert(failpoint.Disable(fpName), IsNil) }()

	ctx := context.WithValue(context.Background(), "__preCommitHook", hook)
	err := tk1.Se.CommitTxn(ctx)
	c.Assert(err, IsNil)
	tk.MustQuery("select * from t where t.b = 5").Check(testkit.Rows("1 5"))
}

func (s *testSchemaSuite) TestRetryMissingUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (a int primary key, b int unique, c int)")
	tk.MustInterDirc("insert into t values (1, 1, 1)")

	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 0")
	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate t set b = 1, c = 2 where b = 2")
	tk1.MustInterDirc("uFIDelate t set b = 1, c = 2 where a = 1")

	// Create a conflict to reproduces the bug that the second uFIDelate memex in retry
	// has a dirty causet but doesn't use UnionScan.
	tk.MustInterDirc("uFIDelate t set b = 2 where a = 1")

	tk1.MustInterDirc("commit")
}

func (s *testSchemaSuite) TestTableReaderChunk(c *C) {
	// Since normally a single region mock einsteindb only returns one partial result we need to manually split the
	// causet to test multiple chunks.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet chk (a int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert chk values (%d)", i))
	}
	tbl, err := petri.GetPetri(tk.Se).SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("chk"))
	c.Assert(err, IsNil)
	s.cluster.SplitTable(tbl.Meta().ID, 10)

	tk.Se.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(1)
	tk.MustInterDirc("set milevadb_init_chunk_size = 2")
	defer func() {
		tk.MustInterDirc(fmt.Sprintf("set milevadb_init_chunk_size = %d", variable.DefInitChunkSize))
	}()
	rs, err := tk.InterDirc("select * from chk")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	var count int
	var numChunks int
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			c.Assert(req.GetRow(i).GetInt64(0), Equals, int64(count))
			count++
		}
		numChunks++
	}
	c.Assert(count, Equals, 100)
	// FIXME: revert this result to new group value after allegrosql can handle initChunkSize.
	c.Assert(numChunks, Equals, 1)
	rs.Close()
}

func (s *testSchemaSuite) TestInsertInterDircChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet test1(a int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert test1 values (%d)", i))
	}
	tk.MustInterDirc("create causet test2(a int)")

	tk.Se.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(1)
	tk.MustInterDirc("insert into test2(a) select a from test1;")

	rs, err := tk.InterDirc("select * from test2")
	c.Assert(err, IsNil)
	var idx int
	for {
		req := rs.NewChunk()
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		if req.NumRows() == 0 {
			break
		}

		for rowIdx := 0; rowIdx < req.NumRows(); rowIdx++ {
			event := req.GetRow(rowIdx)
			c.Assert(event.GetInt64(0), Equals, int64(idx))
			idx++
		}
	}

	c.Assert(idx, Equals, 100)
	rs.Close()
}

func (s *testSchemaSuite) TestUFIDelateInterDircChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet chk(a int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Se.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(1)
	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("uFIDelate chk set a = a + 100 where a = %d", i))
	}

	rs, err := tk.InterDirc("select * from chk")
	c.Assert(err, IsNil)
	var idx int
	for {
		req := rs.NewChunk()
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		if req.NumRows() == 0 {
			break
		}

		for rowIdx := 0; rowIdx < req.NumRows(); rowIdx++ {
			event := req.GetRow(rowIdx)
			c.Assert(event.GetInt64(0), Equals, int64(idx+100))
			idx++
		}
	}

	c.Assert(idx, Equals, 100)
	rs.Close()
}

func (s *testSchemaSuite) TestDeleteInterDircChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet chk(a int)")

	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert chk values (%d)", i))
	}

	tk.Se.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(1)

	for i := 0; i < 99; i++ {
		tk.MustInterDirc(fmt.Sprintf("delete from chk where a = %d", i))
	}

	rs, err := tk.InterDirc("select * from chk")
	c.Assert(err, IsNil)

	req := rs.NewChunk()
	err = rs.Next(context.TODO(), req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows(), Equals, 1)

	event := req.GetRow(0)
	c.Assert(event.GetInt64(0), Equals, int64(99))
	rs.Close()
}

func (s *testSchemaSuite) TestDeleteMultiTableInterDircChunk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet chk1(a int)")
	tk.MustInterDirc("create causet chk2(a int)")

	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert chk1 values (%d)", i))
	}

	for i := 0; i < 50; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert chk2 values (%d)", i))
	}

	tk.Se.GetStochastikVars().SetDistALLEGROSQLScanConcurrency(1)

	tk.MustInterDirc("delete chk1, chk2 from chk1 inner join chk2 where chk1.a = chk2.a")

	rs, err := tk.InterDirc("select * from chk1")
	c.Assert(err, IsNil)

	var idx int
	for {
		req := rs.NewChunk()
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)

		if req.NumRows() == 0 {
			break
		}

		for i := 0; i < req.NumRows(); i++ {
			event := req.GetRow(i)
			c.Assert(event.GetInt64(0), Equals, int64(idx+50))
			idx++
		}
	}
	c.Assert(idx, Equals, 50)
	rs.Close()

	rs, err = tk.InterDirc("select * from chk2")
	c.Assert(err, IsNil)

	req := rs.NewChunk()
	err = rs.Next(context.TODO(), req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows(), Equals, 0)
	rs.Close()
}

func (s *testSchemaSuite) TestIndexLookUpReaderChunk(c *C) {
	// Since normally a single region mock einsteindb only returns one partial result we need to manually split the
	// causet to test multiple chunks.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists chk")
	tk.MustInterDirc("create causet chk (k int unique, c int)")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc(fmt.Sprintf("insert chk values (%d, %d)", i, i))
	}
	tbl, err := petri.GetPetri(tk.Se).SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("chk"))
	c.Assert(err, IsNil)
	s.cluster.SplitIndex(tbl.Meta().ID, tbl.Indices()[0].Meta().ID, 10)

	tk.Se.GetStochastikVars().IndexLookupSize = 10
	rs, err := tk.InterDirc("select * from chk order by k")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	var count int
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			c.Assert(req.GetRow(i).GetInt64(0), Equals, int64(count))
			c.Assert(req.GetRow(i).GetInt64(1), Equals, int64(count))
			count++
		}
	}
	c.Assert(count, Equals, 100)
	rs.Close()

	rs, err = tk.InterDirc("select k from chk where c < 90 order by k")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	count = 0
	for {
		err = rs.Next(context.TODO(), req)
		c.Assert(err, IsNil)
		numRows := req.NumRows()
		if numRows == 0 {
			break
		}
		for i := 0; i < numRows; i++ {
			c.Assert(req.GetRow(i).GetInt64(0), Equals, int64(count))
			count++
		}
	}
	c.Assert(count, Equals, 90)
	rs.Close()
}

func (s *testStochastikSuite2) TestStatementErrorInTransaction(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet memex_side_effect (c int primary key)")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into memex_side_effect values (1)")
	_, err := tk.InterDirc("insert into memex_side_effect value (2),(3),(4),(1)")
	c.Assert(err, NotNil)
	tk.MustQuery(`select * from memex_side_effect`).Check(testkit.Rows("1"))
	tk.MustInterDirc("commit")
	tk.MustQuery(`select * from memex_side_effect`).Check(testkit.Rows("1"))

	tk.MustInterDirc("drop causet if exists test;")
	tk.MustInterDirc(`create causet test (
 		  a int(11) DEFAULT NULL,
 		  b int(11) DEFAULT NULL
 	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)
	tk.MustInterDirc("insert into test values (1, 2), (1, 2), (1, 1), (1, 1);")

	tk.MustInterDirc("start transaction;")
	// In the transaction, memex error should not rollback the transaction.
	_, err = tk.InterDirc("uFIDelate tset set b=11 where a=1 and b=2;")
	c.Assert(err, NotNil)
	// Test for a bug that last line rollback and exit transaction, this line autocommit.
	tk.MustInterDirc("uFIDelate test set b = 11 where a = 1 and b = 2;")
	tk.MustInterDirc("rollback")
	tk.MustQuery("select * from test where a = 1 and b = 11").Check(testkit.Rows())
}

func (s *testStochastikSerialSuite) TestStatementCountLimit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet stmt_count_limit (id int)")
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.Performance.StmtCountLimit = 3
	})
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into stmt_count_limit values (1)")
	tk.MustInterDirc("insert into stmt_count_limit values (2)")
	_, err := tk.InterDirc("insert into stmt_count_limit values (3)")
	c.Assert(err, NotNil)

	// begin is counted into history but this one is not.
	tk.MustInterDirc("SET SESSION autocommit = false")
	tk.MustInterDirc("insert into stmt_count_limit values (1)")
	tk.MustInterDirc("insert into stmt_count_limit values (2)")
	tk.MustInterDirc("insert into stmt_count_limit values (3)")
	_, err = tk.InterDirc("insert into stmt_count_limit values (4)")
	c.Assert(err, NotNil)
}

func (s *testStochastikSerialSuite) TestBatchCommit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("set milevadb_batch_commit = 1")
	tk.MustInterDirc("set milevadb_disable_txn_auto_retry = 0")
	tk.MustInterDirc("create causet t (id int)")
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.Performance.StmtCountLimit = 3
	})
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("SET SESSION autocommit = 1")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values (1)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustInterDirc("insert into t values (2)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustInterDirc("rollback")
	tk1.MustQuery("select * from t").Check(testkit.Rows())

	// The above rollback will not make the stochastik in transaction.
	tk.MustInterDirc("insert into t values (1)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustInterDirc("delete from t")

	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into t values (5)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustInterDirc("insert into t values (6)")
	tk1.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustInterDirc("insert into t values (7)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))

	// The stochastik is still in transaction.
	tk.MustInterDirc("insert into t values (8)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustInterDirc("insert into t values (9)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustInterDirc("insert into t values (10)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7"))
	tk.MustInterDirc("commit")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7", "8", "9", "10"))

	// The above commit will not make the stochastik in transaction.
	tk.MustInterDirc("insert into t values (11)")
	tk1.MustQuery("select * from t").Check(testkit.Rows("5", "6", "7", "8", "9", "10", "11"))

	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("SET SESSION autocommit = 0")
	tk.MustInterDirc("insert into t values (1)")
	tk.MustInterDirc("insert into t values (2)")
	tk.MustInterDirc("insert into t values (3)")
	tk.MustInterDirc("rollback")
	tk1.MustInterDirc("insert into t values (4)")
	tk1.MustInterDirc("insert into t values (5)")
	tk.MustQuery("select * from t").Check(testkit.Rows("4", "5"))
}

func (s *testStochastikSuite3) TestCastTimeToDate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("set time_zone = '-8:00'")
	date := time.Now().In(time.FixedZone("", -8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format("2006-01-02")))

	tk.MustInterDirc("set time_zone = '+08:00'")
	date = time.Now().In(time.FixedZone("", 8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format("2006-01-02")))
}

func (s *testStochastikSuite) TestSetGlobalTZ(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("set time_zone = '+08:00'")
	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	tk.MustInterDirc("set global time_zone = '+00:00'")

	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	// Disable global variable cache, so load global stochastik variable take effect immediate.
	s.dom.GetGlobalVarsCache().Disable()
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +00:00"))
}

func (s *testStochastikSuite2) TestRollbackOnCompileError(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (a int)")
	tk.MustInterDirc("insert t values (1)")

	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustInterDirc("rename causet t to t2")

	var meetErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.InterDirc("insert t values (1)")
		if err != nil {
			meetErr = true
			break
		}
	}
	c.Assert(meetErr, IsTrue)
	tk.MustInterDirc("rename causet t2 to t")
	var recoverErr bool
	for i := 0; i < 100; i++ {
		_, err := tk2.InterDirc("insert t values (1)")
		if err == nil {
			recoverErr = true
			break
		}
	}
	c.Assert(recoverErr, IsTrue)
}

func (s *testStochastikSuite3) TestSetTransactionIsolationOneShot(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet t (k int, v int)")
	tk.MustInterDirc("insert t values (1, 42)")
	tk.MustInterDirc("set tx_isolation = 'read-committed'")
	tk.MustQuery("select @@tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustInterDirc("set tx_isolation = 'repeablock-read'")
	tk.MustInterDirc("set transaction isolation level read committed")
	tk.MustQuery("select @@tx_isolation_one_shot").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	// Check isolation level is set to read committed.
	ctx := context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *ekv.Request) {
		c.Assert(req.IsolationLevel, Equals, ekv.SI)
	})
	tk.Se.InterDircute(ctx, "select * from t where k = 1")

	// Check it just take effect for one time.
	ctx = context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *ekv.Request) {
		c.Assert(req.IsolationLevel, Equals, ekv.SI)
	})
	tk.Se.InterDircute(ctx, "select * from t where k = 1")

	// Can't change isolation level when it's inside a transaction.
	tk.MustInterDirc("begin")
	_, err := tk.Se.InterDircute(ctx, "set transaction isolation level read committed")
	c.Assert(err, NotNil)
}

func (s *testStochastikSuite2) TestDBUserNameLength(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet if not exists t (a int)")
	// Test user name length can be longer than 16.
	tk.MustInterDirc(`CREATE USER 'abcddfjakldfjaldddds'@'%' identified by ''`)
	tk.MustInterDirc(`grant all privileges on test.* to 'abcddfjakldfjaldddds'@'%'`)
	tk.MustInterDirc(`grant all privileges on test.t to 'abcddfjakldfjaldddds'@'%'`)
}

func (s *testStochastikSerialSuite) TestKVVars(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("set @@milevadb_backoff_lock_fast = 1")
	tk.MustInterDirc("set @@milevadb_backoff_weight = 100")
	tk.MustInterDirc("create causet if not exists kvvars (a int key)")
	tk.MustInterDirc("insert into kvvars values (1)")
	tk.MustInterDirc("begin")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	vars := txn.GetVars()
	c.Assert(vars.BackoffLockFast, Equals, 1)
	c.Assert(vars.BackOffWeight, Equals, 100)
	tk.MustInterDirc("rollback")
	tk.MustInterDirc("set @@milevadb_backoff_weight = 50")
	tk.MustInterDirc("set @@autocommit = 0")
	tk.MustInterDirc("select * from kvvars")
	c.Assert(tk.Se.GetStochastikVars().InTxn(), IsTrue)
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	vars = txn.GetVars()
	c.Assert(vars.BackOffWeight, Equals, 50)

	tk.MustInterDirc("set @@autocommit = 1")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/probeSetVars", `return(true)`), IsNil)
	tk.MustInterDirc("select * from kvvars where a = 1")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/probeSetVars"), IsNil)
	c.Assert(einsteindb.SetSuccess, IsTrue)
	einsteindb.SetSuccess = false
}

func (s *testStochastikSuite2) TestCommitRetryCount(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("create causet no_retry (id int)")
	tk1.MustInterDirc("insert into no_retry values (1)")
	tk1.MustInterDirc("set @@milevadb_retry_limit = 0")

	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate no_retry set id = 2")

	tk2.MustInterDirc("begin")
	tk2.MustInterDirc("uFIDelate no_retry set id = 3")
	tk2.MustInterDirc("commit")

	// No auto retry because retry limit is set to 0.
	_, err := tk1.Se.InterDircute(context.Background(), "commit")
	c.Assert(err, NotNil)
}

func (s *testStochastikSuite3) TestEnablePartition(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("set milevadb_enable_block_partition=off")
	tk.MustQuery("show variables like 'milevadb_enable_block_partition'").Check(testkit.Rows("milevadb_enable_block_partition off"))

	tk.MustInterDirc("set global milevadb_enable_block_partition = on")

	tk.MustQuery("show variables like 'milevadb_enable_block_partition'").Check(testkit.Rows("milevadb_enable_block_partition off"))
	tk.MustQuery("show global variables like 'milevadb_enable_block_partition'").Check(testkit.Rows("milevadb_enable_block_partition on"))

	// Disable global variable cache, so load global stochastik variable take effect immediate.
	s.dom.GetGlobalVarsCache().Disable()
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustQuery("show variables like 'milevadb_enable_block_partition'").Check(testkit.Rows("milevadb_enable_block_partition on"))
}

func (s *testStochastikSerialSuite) TestTxnRetryErrMsg(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("create causet no_retry (id int)")
	tk1.MustInterDirc("insert into no_retry values (1)")
	tk1.MustInterDirc("begin")
	tk2.MustInterDirc("uFIDelate no_retry set id = id + 1")
	tk1.MustInterDirc("uFIDelate no_retry set id = id + 1")
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/ErrMockRetryableOnly", `return(true)`), IsNil)
	_, err := tk1.Se.InterDircute(context.Background(), "commit")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/ErrMockRetryableOnly"), IsNil)
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrTxnRetryable.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), "mock retryable error"), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), ekv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
}

func (s *testSchemaSuite) TestDisableTxnAutoRetry(c *C) {
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("create causet no_retry (id int)")
	tk1.MustInterDirc("insert into no_retry values (1)")
	tk1.MustInterDirc("set @@milevadb_disable_txn_auto_retry = 1")

	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate no_retry set id = 2")

	tk2.MustInterDirc("begin")
	tk2.MustInterDirc("uFIDelate no_retry set id = 3")
	tk2.MustInterDirc("commit")

	// No auto retry because milevadb_disable_txn_auto_retry is set to 1.
	_, err := tk1.Se.InterDircute(context.Background(), "commit")
	c.Assert(err, NotNil)

	// stochastik 1 starts a transaction early.
	// execute a select memex to clear retry history.
	tk1.MustInterDirc("select 1")
	tk1.Se.PrepareTxnCtx(context.Background())
	// stochastik 2 uFIDelate the value.
	tk2.MustInterDirc("uFIDelate no_retry set id = 4")
	// AutoCommit uFIDelate will retry, so it would not fail.
	tk1.MustInterDirc("uFIDelate no_retry set id = 5")

	// RestrictedALLEGROSQL should retry.
	tk1.Se.GetStochastikVars().InRestrictedALLEGROSQL = true
	tk1.MustInterDirc("begin")

	tk2.MustInterDirc("uFIDelate no_retry set id = 6")

	tk1.MustInterDirc("uFIDelate no_retry set id = 7")
	tk1.MustInterDirc("commit")

	// test for disable transaction local latch
	tk1.Se.GetStochastikVars().InRestrictedALLEGROSQL = false
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = false
	})
	tk1.MustInterDirc("begin")
	tk1.MustInterDirc("uFIDelate no_retry set id = 9")

	tk2.MustInterDirc("uFIDelate no_retry set id = 8")

	_, err = tk1.Se.InterDircute(context.Background(), "commit")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), ekv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
	tk1.MustInterDirc("rollback")

	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.TxnLocalLatches.Enabled = true
	})
	tk1.MustInterDirc("begin")
	tk2.MustInterDirc("alter causet no_retry add index idx(id)")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk1.MustInterDirc("uFIDelate no_retry set id = 10")
	_, err = tk1.Se.InterDircute(context.Background(), "commit")
	c.Assert(err, NotNil)

	// set autocommit to begin and commit
	tk1.MustInterDirc("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("8"))
	tk2.MustInterDirc("uFIDelate no_retry set id = 11")
	tk1.MustInterDirc("uFIDelate no_retry set id = 12")
	_, err = tk1.Se.InterDircute(context.Background(), "set autocommit = 1")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), ekv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
	tk1.MustInterDirc("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("11"))

	tk1.MustInterDirc("set autocommit = 0")
	tk1.MustQuery("select * from no_retry").Check(testkit.Rows("11"))
	tk2.MustInterDirc("uFIDelate no_retry set id = 13")
	tk1.MustInterDirc("uFIDelate no_retry set id = 14")
	_, err = tk1.Se.InterDircute(context.Background(), "commit")
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrWriteConflict.Equal(err), IsTrue, Commentf("error: %s", err))
	c.Assert(strings.Contains(err.Error(), ekv.TxnRetryableMark), IsTrue, Commentf("error: %s", err))
	tk1.MustInterDirc("rollback")
	tk2.MustQuery("select * from no_retry").Check(testkit.Rows("13"))
}

// TestSetGroupConcatMaxLen is for issue #7034
func (s *testStochastikSuite2) TestSetGroupConcatMaxLen(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)

	// Normal case
	tk.MustInterDirc("set global group_concat_max_len = 100")
	tk.MustInterDirc("set @@stochastik.group_concat_max_len = 50")
	result := tk.MustQuery("show global variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 100"))

	result = tk.MustQuery("show stochastik variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 50"))

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@stochastik.group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	tk.MustInterDirc("set @@group_concat_max_len = 1024")

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@stochastik.group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	// Test value out of range
	tk.MustInterDirc("set @@group_concat_max_len=1")
	tk.MustQuery("show warnings").Check(solitonutil.RowsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("4"))

	_, err := tk.InterDirc("set @@group_concat_max_len = 18446744073709551616")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	// Test illegal type
	_, err = tk.InterDirc("set @@group_concat_max_len='hello'")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
}

func (s *testStochastikSuite2) TestUFIDelatePrivilege(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1, t2;")
	tk.MustInterDirc("create causet t1 (id int);")
	tk.MustInterDirc("create causet t2 (id int);")
	tk.MustInterDirc("insert into t1 values (1);")
	tk.MustInterDirc("insert into t2 values (2);")
	tk.MustInterDirc("create user xxx;")
	tk.MustInterDirc("grant all on test.t1 to xxx;")
	tk.MustInterDirc("grant select on test.t2 to xxx;")

	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: "localhost"},
		[]byte(""),
		[]byte("")), IsTrue)

	_, err := tk1.InterDirc("uFIDelate t2 set id = 666 where id = 1;")
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "privilege check fail"), IsTrue)

	// Cover a bug that t1 and t2 both require uFIDelate privilege.
	// In fact, the privlege check for t1 should be uFIDelate, and for t2 should be select.
	_, err = tk1.InterDirc("uFIDelate t1,t2 set t1.id = t2.id;")
	c.Assert(err, IsNil)

	// Fix issue 8911
	tk.MustInterDirc("create database weperk")
	tk.MustInterDirc("use weperk")
	tk.MustInterDirc("create causet tb_wehub_server (id int, active_count int, used_count int)")
	tk.MustInterDirc("create user 'weperk'")
	tk.MustInterDirc("grant all privileges on weperk.* to 'weperk'@'%'")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "weperk", Hostname: "%"},
		[]byte(""), []byte("")), IsTrue)
	tk1.MustInterDirc("use weperk")
	tk1.MustInterDirc("uFIDelate tb_wehub_server a set a.active_count=a.active_count+1,a.used_count=a.used_count+1 where id=1")

	tk.MustInterDirc("create database service")
	tk.MustInterDirc("create database report")
	tk.MustInterDirc(`CREATE TABLE service.t1 (
  id int(11) DEFAULT NULL,
  a bigint(20) NOT NULL,
  b text DEFAULT NULL,
  PRIMARY KEY (a)
)`)
	tk.MustInterDirc(`CREATE TABLE report.t2 (
  a bigint(20) DEFAULT NULL,
  c bigint(20) NOT NULL
)`)
	tk.MustInterDirc("grant all privileges on service.* to weperk")
	tk.MustInterDirc("grant all privileges on report.* to weperk")
	tk1.Se.GetStochastikVars().CurrentDB = ""
	tk1.MustInterDirc(`uFIDelate service.t1 s,
report.t2 t
set s.a = t.a
WHERE
s.a = t.a
and t.c >=  1 and t.c <= 10000
and s.b !='xx';`)

	// Fix issue 10028
	tk.MustInterDirc("create database ap")
	tk.MustInterDirc("create database tp")
	tk.MustInterDirc("grant all privileges on ap.* to xxx")
	tk.MustInterDirc("grant select on tp.* to xxx")
	tk.MustInterDirc("create causet tp.record( id int,name varchar(128),age int)")
	tk.MustInterDirc("insert into tp.record (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18)")
	tk.MustInterDirc("create causet ap.record( id int,name varchar(128),age int)")
	tk.MustInterDirc("insert into ap.record(id) values(1)")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: "localhost"},
		[]byte(""),
		[]byte("")), IsTrue)
	_, err2 := tk1.InterDirc("uFIDelate ap.record t inner join tp.record tt on t.id=tt.id  set t.name=tt.name")
	c.Assert(err2, IsNil)
}

func (s *testStochastikSuite2) TestTxnGoString(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists gostr;")
	tk.MustInterDirc("create causet gostr (id int);")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	str1 := fmt.Sprintf("%#v", txn)
	c.Assert(str1, Equals, "Txn{state=invalid}")
	tk.MustInterDirc("begin")
	txn, err = tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(fmt.Sprintf("%#v", txn), Equals, fmt.Sprintf("Txn{state=valid, txnStartTS=%d}", txn.StartTS()))

	tk.MustInterDirc("insert into gostr values (1)")
	c.Assert(fmt.Sprintf("%#v", txn), Equals, fmt.Sprintf("Txn{state=valid, txnStartTS=%d}", txn.StartTS()))

	tk.MustInterDirc("rollback")
	c.Assert(fmt.Sprintf("%#v", txn), Equals, "Txn{state=invalid}")
}

func (s *testStochastikSuite3) TestMaxInterDiructeTime(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet MaxInterDircTime( id int,name varchar(128),age int);")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into MaxInterDircTime (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18);")

	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) MAX_EXECUTION_TIME(500) */ * FROM MaxInterDircTime;")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(500)")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.HasMaxInterDircutionTime, Equals, true)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MaxInterDircutionTime, Equals, uint64(500))

	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) */ * FROM MaxInterDircTime;")

	tk.MustInterDirc("set @@global.MAX_EXECUTION_TIME = 300;")
	tk.MustQuery("select * FROM MaxInterDircTime;")

	tk.MustInterDirc("set @@MAX_EXECUTION_TIME = 150;")
	tk.MustQuery("select * FROM MaxInterDircTime;")

	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("300"))
	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("150"))

	tk.MustInterDirc("set @@global.MAX_EXECUTION_TIME = 0;")
	tk.MustInterDirc("set @@MAX_EXECUTION_TIME = 0;")
	tk.MustInterDirc("commit")
	tk.MustInterDirc("drop causet if exists MaxInterDircTime;")
}

func (s *testStochastikSuite2) TestGrantViewRelated(c *C) {
	tkRoot := testkit.NewTestKitWithInit(c, s.causetstore)
	tkUser := testkit.NewTestKitWithInit(c, s.causetstore)

	tkRoot.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkRoot.MustInterDirc("create causet if not exists t (a int)")
	tkRoot.MustInterDirc("create view v_version29 as select * from t")
	tkRoot.MustInterDirc("create user 'u_version29'@'%'")
	tkRoot.MustInterDirc("grant select on t to u_version29@'%'")

	tkUser.Se.Auth(&auth.UserIdentity{Username: "u_version29", Hostname: "localhost", CurrentUser: true, AuthUsername: "u_version29", AuthHostname: "%"}, nil, []byte("012345678901234567890"))

	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	err := tkUser.InterDircToErr("select * from test.v_version29;")
	c.Assert(err, NotNil)
	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	err = tkUser.InterDircToErr("create view v_version29_c as select * from t;")
	c.Assert(err, NotNil)

	tkRoot.MustInterDirc(`grant show view on v_version29 to 'u_version29'@'%'`)
	tkRoot.MustQuery("select block_priv from allegrosql.blocks_priv where host='%' and EDB='test' and user='u_version29' and block_name='v_version29'").Check(testkit.Rows("Show View"))

	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	tkUser.MustQuery("show create view v_version29;")
	err = tkUser.InterDircToErr("create view v_version29_c as select * from v_version29;")
	c.Assert(err, NotNil)

	tkRoot.MustInterDirc("create view v_version29_c as select * from v_version29;")
	tkRoot.MustInterDirc(`grant create view on v_version29_c to 'u_version29'@'%'`) // Can't grant privilege on a non-exist causet/view.
	tkRoot.MustQuery("select block_priv from allegrosql.blocks_priv where host='%' and EDB='test' and user='u_version29' and block_name='v_version29_c'").Check(testkit.Rows("Create View"))
	tkRoot.MustInterDirc("drop view v_version29_c")

	tkRoot.MustInterDirc(`grant select on v_version29 to 'u_version29'@'%'`)
	tkUser.MustQuery("select current_user();").Check(testkit.Rows("u_version29@%"))
	tkUser.MustInterDirc("create view v_version29_c as select * from v_version29;")
}

func (s *testStochastikSuite3) TestLoadClientInteractive(c *C) {
	var (
		err          error
		connectionID uint64
	)
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	id := atomic.AddUint64(&connectionID, 1)
	tk.Se.SetConnectionID(id)
	tk.Se.GetStochastikVars().ClientCapability = tk.Se.GetStochastikVars().ClientCapability | allegrosql.ClientInteractive
	tk.MustQuery("select @@wait_timeout").Check(testkit.Rows("28800"))
}

func (s *testStochastikSuite2) TestReplicaRead(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetStochastikVars().GetReplicaRead(), Equals, ekv.ReplicaReadLeader)
	tk.MustInterDirc("set @@milevadb_replica_read = 'follower';")
	c.Assert(tk.Se.GetStochastikVars().GetReplicaRead(), Equals, ekv.ReplicaReadFollower)
	tk.MustInterDirc("set @@milevadb_replica_read = 'leader';")
	c.Assert(tk.Se.GetStochastikVars().GetReplicaRead(), Equals, ekv.ReplicaReadLeader)
}

func (s *testStochastikSuite3) TestIsolationRead(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(len(tk.Se.GetStochastikVars().GetIsolationReadEngines()), Equals, 3)
	tk.MustInterDirc("set @@milevadb_isolation_read_engines = 'tiflash';")
	engines := tk.Se.GetStochastikVars().GetIsolationReadEngines()
	c.Assert(len(engines), Equals, 1)
	_, hasTiFlash := engines[ekv.TiFlash]
	_, hasEinsteinDB := engines[ekv.EinsteinDB]
	c.Assert(hasTiFlash, Equals, true)
	c.Assert(hasEinsteinDB, Equals, false)
}

func (s *testStochastikSuite2) TestStmtHints(c *C) {
	var err error
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.Se, err = stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)

	// Test MEMORY_QUOTA hint
	tk.MustInterDirc("select /*+ MEMORY_QUOTA(1 MB) */ 1;")
	val := int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	tk.MustInterDirc("select /*+ MEMORY_QUOTA(1 GB) */ 1;")
	val = int64(1) * 1024 * 1024 * 1024
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	tk.MustInterDirc("select /*+ MEMORY_QUOTA(1 GB), MEMORY_QUOTA(1 MB) */ 1;")
	val = int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	tk.MustInterDirc("select /*+ MEMORY_QUOTA(0 GB) */ 1;")
	val = int64(0)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "Setting the MEMORY_QUOTA to 0 means no memory limit")

	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t1(a int);")
	tk.MustInterDirc("insert /*+ MEMORY_QUOTA(1 MB) */ into t1 (a) values (1);")
	val = int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)

	tk.MustInterDirc("insert /*+ MEMORY_QUOTA(1 MB) */  into t1 select /*+ MEMORY_QUOTA(3 MB) */ * from t1;")
	val = int64(1) * 1024 * 1024
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.MemTracker.CheckBytesLimit(val), IsTrue)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "[soliton:3126]Hint MEMORY_QUOTA(`3145728`) is ignored as conflicting/duplicated.")

	// Test NO_INDEX_MERGE hint
	tk.Se.GetStochastikVars().SetEnableIndexMerge(true)
	tk.MustInterDirc("select /*+ NO_INDEX_MERGE() */ 1;")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.NoIndexMergeHint, IsTrue)
	tk.MustInterDirc("select /*+ NO_INDEX_MERGE(), NO_INDEX_MERGE() */ 1;")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().GetEnableIndexMerge(), IsTrue)

	// Test USE_TOJA hint
	tk.Se.GetStochastikVars().SetAllowInSubqToJoinAnPosetDagg(true)
	tk.MustInterDirc("select /*+ USE_TOJA(false) */ 1;")
	c.Assert(tk.Se.GetStochastikVars().GetAllowInSubqToJoinAnPosetDagg(), IsFalse)
	tk.Se.GetStochastikVars().SetAllowInSubqToJoinAnPosetDagg(false)
	tk.MustInterDirc("select /*+ USE_TOJA(true) */ 1;")
	c.Assert(tk.Se.GetStochastikVars().GetAllowInSubqToJoinAnPosetDagg(), IsTrue)
	tk.MustInterDirc("select /*+ USE_TOJA(false), USE_TOJA(true) */ 1;")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().GetAllowInSubqToJoinAnPosetDagg(), IsTrue)

	// Test USE_CASCADES hint
	tk.Se.GetStochastikVars().SetEnableCascadesCausetAppend(true)
	tk.MustInterDirc("select /*+ USE_CASCADES(false) */ 1;")
	c.Assert(tk.Se.GetStochastikVars().GetEnableCascadesCausetAppend(), IsFalse)
	tk.Se.GetStochastikVars().SetEnableCascadesCausetAppend(false)
	tk.MustInterDirc("select /*+ USE_CASCADES(true) */ 1;")
	c.Assert(tk.Se.GetStochastikVars().GetEnableCascadesCausetAppend(), IsTrue)
	tk.MustInterDirc("select /*+ USE_CASCADES(false), USE_CASCADES(true) */ 1;")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings()[0].Err.Error(), Equals, "USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(true)")
	c.Assert(tk.Se.GetStochastikVars().GetEnableCascadesCausetAppend(), IsTrue)

	// Test READ_CONSISTENT_REPLICA hint
	tk.Se.GetStochastikVars().SetReplicaRead(ekv.ReplicaReadLeader)
	tk.MustInterDirc("select /*+ READ_CONSISTENT_REPLICA() */ 1;")
	c.Assert(tk.Se.GetStochastikVars().GetReplicaRead(), Equals, ekv.ReplicaReadFollower)
	tk.MustInterDirc("select /*+ READ_CONSISTENT_REPLICA(), READ_CONSISTENT_REPLICA() */ 1;")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings(), HasLen, 1)
	c.Assert(tk.Se.GetStochastikVars().GetReplicaRead(), Equals, ekv.ReplicaReadFollower)
}

func (s *testStochastikSuite3) TestPessimisticLockOnPartition(c *C) {
	// This test checks that 'select ... for uFIDelate' locks the partition instead of the causet.
	// Cover a bug that causet ID is used to encode the dagger key mistakenly.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`create causet if not exists foruFIDelate_on_partition (
  age int not null primary key,
  nickname varchar(20) not null,
  gender int not null default 0,
  first_name varchar(30) not null default '',
  last_name varchar(20) not null default '',
  full_name varchar(60) as (concat(first_name, ' ', last_name)),
  index idx_nickname (nickname)
) partition by range (age) (
  partition child values less than (18),
  partition young values less than (30),
  partition midbse values less than (50),
  partition old values less than (123)
);`)
	tk.MustInterDirc("insert into foruFIDelate_on_partition (`age`, `nickname`) values (25, 'cosven');")

	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustInterDirc("use test")

	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from foruFIDelate_on_partition where age=25 for uFIDelate").Check(testkit.Rows("25 cosven 0    "))
	tk1.MustInterDirc("begin pessimistic")

	ch := make(chan int32, 5)
	go func() {
		tk1.MustInterDirc("uFIDelate foruFIDelate_on_partition set first_name='sw' where age=25")
		ch <- 0
		tk1.MustInterDirc("commit")
		ch <- 0
	}()

	// Leave 50ms for tk1 to run, tk1 should be blocked at the uFIDelate operation.
	time.Sleep(50 * time.Millisecond)
	ch <- 1

	tk.MustInterDirc("commit")
	// tk1 should be blocked until tk commit, check the order.
	c.Assert(<-ch, Equals, int32(1))
	c.Assert(<-ch, Equals, int32(0))
	<-ch // wait for goroutine to quit.

	// Once again...
	// This time, test for the uFIDelate-uFIDelate conflict.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate foruFIDelate_on_partition set first_name='sw' where age=25")
	tk1.MustInterDirc("begin pessimistic")

	go func() {
		tk1.MustInterDirc("uFIDelate foruFIDelate_on_partition set first_name = 'xxx' where age=25")
		ch <- 0
		tk1.MustInterDirc("commit")
		ch <- 0
	}()

	// Leave 50ms for tk1 to run, tk1 should be blocked at the uFIDelate operation.
	time.Sleep(50 * time.Millisecond)
	ch <- 1

	tk.MustInterDirc("commit")
	// tk1 should be blocked until tk commit, check the order.
	c.Assert(<-ch, Equals, int32(1))
	c.Assert(<-ch, Equals, int32(0))
	<-ch // wait for goroutine to quit.
}

func (s *testSchemaSuite) TestTxnSize(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists txn_size")
	tk.MustInterDirc("create causet txn_size (k int , v varchar(64))")
	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert txn_size values (1, 'dfaasdfsdf')")
	tk.MustInterDirc("insert txn_size values (2, 'dsdfaasdfsdf')")
	tk.MustInterDirc("insert txn_size values (3, 'abcdefghijkl')")
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	c.Assert(txn.Size() > 0, IsTrue)
}

func (s *testStochastikSuite2) TestPerStmtTaskID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("create causet task_id (v int)")

	tk.MustInterDirc("begin")
	tk.MustInterDirc("select * from task_id where v > 10")
	taskID1 := tk.Se.GetStochastikVars().StmtCtx.TaskID
	tk.MustInterDirc("select * from task_id where v < 5")
	taskID2 := tk.Se.GetStochastikVars().StmtCtx.TaskID
	tk.MustInterDirc("commit")

	c.Assert(taskID1 != taskID2, IsTrue)
}

func (s *testStochastikSerialSuite) TestDoDBSJobQuit(c *C) {
	// test https://github.com/whtcorpsinc/milevadb/issues/18714, imitate DM's use environment
	// use isolated causetstore, because in below failpoint we will cancel its context
	causetstore, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.MockEinsteinDB))
	c.Assert(err, IsNil)
	defer causetstore.Close()
	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom.Close()
	se, err := stochastik.CreateStochastik(causetstore)
	c.Assert(err, IsNil)
	defer se.Close()

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/dbs/storeCloseInLoop", `return`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/dbs/storeCloseInLoop")

	// this DBS call will enter deadloop before this fix
	err = dom.DBS().CreateSchema(se, perceptron.NewCIStr("testschema"), nil)
	c.Assert(err.Error(), Equals, "context canceled")
}

func (s *testBackupRestoreSuite) TestBackupAndRestore(c *C) {
	// only run BR ALLEGROALLEGROSQL integration test with einsteindb causetstore.
	if *withEinsteinDB {
		cfg := config.GetGlobalConfig()
		cfg.CausetStore = "einsteindb"
		cfg.Path = s.FIDelAddr
		config.StoreGlobalConfig(cfg)
		tk := testkit.NewTestKitWithInit(c, s.causetstore)
		tk.MustInterDirc("create database if not exists br")
		tk.MustInterDirc("use br")
		tk.MustInterDirc("create causet t1(v int)")
		tk.MustInterDirc("insert into t1 values (1)")
		tk.MustInterDirc("insert into t1 values (2)")
		tk.MustInterDirc("insert into t1 values (3)")
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))

		tk.MustInterDirc("create database if not exists br02")
		tk.MustInterDirc("use br02")
		tk.MustInterDirc("create causet t1(v int)")

		tmFIDelir := path.Join(os.TemFIDelir(), "bk1")
		os.RemoveAll(tmFIDelir)
		// backup database to tmp dir
		tk.MustQuery("backup database * to 'local://" + tmFIDelir + "'")

		// remove database for recovery
		tk.MustInterDirc("drop database br")
		tk.MustInterDirc("drop database br02")

		// restore database with backup data
		tk.MustQuery("restore database * from 'local://" + tmFIDelir + "'")
		tk.MustInterDirc("use br")
		tk.MustQuery("select count(*) from t1").Check(testkit.Rows("3"))
		tk.MustInterDirc("drop database br")
		tk.MustInterDirc("drop database br02")
	}
}
