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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
)

var _ = SerialSuites(&testPessimisticSuite{})

type testPessimisticSuite struct {
	testStochastikSuiteBase
}

func (s *testPessimisticSuite) SetUpSuite(c *C) {
	s.testStochastikSuiteBase.SetUpSuite(c)
	// Set it to 300ms for testing dagger resolve.
	atomic.StoreUint64(&einsteindb.ManagedLockTTL, 300)
	einsteindb.PrewriteMaxBackoff = 500
}

func (s *testPessimisticSuite) TearDownSuite(c *C) {
	s.testStochastikSuiteBase.TearDownSuite(c)
	einsteindb.PrewriteMaxBackoff = 20000
}

func (s *testPessimisticSuite) TestPessimisticTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	// Make the name has different indent for easier read.
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists pessimistic")
	tk.MustInterDirc("create causet pessimistic (k int, v int)")
	tk.MustInterDirc("insert into pessimistic values (1, 1)")

	// t1 dagger, t2 uFIDelate, t1 uFIDelate and retry memex.
	tk1.MustInterDirc("begin pessimistic")

	tk.MustInterDirc("uFIDelate pessimistic set v = 2 where v = 1")

	// UFIDelate can see the change, so this memex affects 0 rows.
	tk1.MustInterDirc("uFIDelate pessimistic set v = 3 where v = 1")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(0))
	c.Assert(stochastik.GetHistory(tk1.Se).Count(), Equals, 0)
	// select for uFIDelate can see the change of another transaction.
	tk1.MustQuery("select * from pessimistic for uFIDelate").Check(testkit.Rows("1 2"))
	// plain select can not see the change of another transaction.
	tk1.MustQuery("select * from pessimistic").Check(testkit.Rows("1 1"))
	tk1.MustInterDirc("uFIDelate pessimistic set v = 3 where v = 2")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(1))

	// pessimistic dagger doesn't causet read operation of other transactions.
	tk.MustQuery("select * from pessimistic").Check(testkit.Rows("1 2"))

	tk1.MustInterDirc("commit")
	tk1.MustQuery("select * from pessimistic").Check(testkit.Rows("1 3"))

	// t1 dagger, t1 select for uFIDelate, t2 wait t1.
	tk1.MustInterDirc("begin pessimistic")
	tk1.MustInterDirc("select * from pessimistic where k = 1 for uFIDelate")
	finishCh := make(chan struct{})
	go func() {
		tk.MustInterDirc("uFIDelate pessimistic set v = 5 where k = 1")
		finishCh <- struct{}{}
	}()
	time.Sleep(time.Millisecond * 10)
	tk1.MustInterDirc("uFIDelate pessimistic set v = 3 where k = 1")
	tk1.MustInterDirc("commit")
	<-finishCh
	tk.MustQuery("select * from pessimistic").Check(testkit.Rows("1 5"))
}

func (s *testPessimisticSuite) TestTxnMode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tests := []struct {
		beginStmt     string
		txnMode       string
		isPessimistic bool
	}{
		{"pessimistic", "pessimistic", true},
		{"pessimistic", "optimistic", true},
		{"pessimistic", "", true},
		{"optimistic", "pessimistic", false},
		{"optimistic", "optimistic", false},
		{"optimistic", "", false},
		{"", "pessimistic", true},
		{"", "optimistic", false},
		{"", "", false},
	}
	for _, tt := range tests {
		tk.MustInterDirc(fmt.Sprintf("set @@milevadb_txn_mode = '%s'", tt.txnMode))
		tk.MustInterDirc("begin " + tt.beginStmt)
		c.Check(tk.Se.GetStochastikVars().TxnCtx.IsPessimistic, Equals, tt.isPessimistic)
		tk.MustInterDirc("rollback")
	}

	tk.MustInterDirc("set @@autocommit = 0")
	tk.MustInterDirc("create causet if not exists txn_mode (a int)")
	tests2 := []struct {
		txnMode       string
		isPessimistic bool
	}{
		{"pessimistic", true},
		{"optimistic", false},
		{"", false},
	}
	for _, tt := range tests2 {
		tk.MustInterDirc(fmt.Sprintf("set @@milevadb_txn_mode = '%s'", tt.txnMode))
		tk.MustInterDirc("rollback")
		tk.MustInterDirc("insert txn_mode values (1)")
		c.Check(tk.Se.GetStochastikVars().TxnCtx.IsPessimistic, Equals, tt.isPessimistic)
		tk.MustInterDirc("rollback")
	}
	tk.MustInterDirc("set @@global.milevadb_txn_mode = 'pessimistic'")
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustQuery("select @@milevadb_txn_mode").Check(testkit.Rows("pessimistic"))
	tk1.MustInterDirc("set @@autocommit = 0")
	tk1.MustInterDirc("insert txn_mode values (2)")
	c.Check(tk1.Se.GetStochastikVars().TxnCtx.IsPessimistic, IsTrue)
	tk1.MustInterDirc("set @@milevadb_txn_mode = ''")
	tk1.MustInterDirc("rollback")
	tk1.MustInterDirc("insert txn_mode values (2)")
	c.Check(tk1.Se.GetStochastikVars().TxnCtx.IsPessimistic, IsFalse)
	tk1.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestDeadlock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists deadlock")
	tk.MustInterDirc("create causet deadlock (k int primary key, v int)")
	tk.MustInterDirc("insert into deadlock values (1, 1), (2, 1)")

	syncCh := make(chan error)
	go func() {
		tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
		tk1.MustInterDirc("begin pessimistic")
		tk1.MustInterDirc("uFIDelate deadlock set v = v + 1 where k = 2")
		syncCh <- nil
		_, err := tk1.InterDirc("uFIDelate deadlock set v = v + 1 where k = 1")
		syncCh <- err
	}()
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate deadlock set v = v + 1 where k = 1")
	<-syncCh
	_, err1 := tk.InterDirc("uFIDelate deadlock set v = v + 1 where k = 2")
	err2 := <-syncCh
	// Either err1 or err2 is deadlock error.
	var err error
	if err1 != nil {
		c.Assert(err2, IsNil)
		err = err1
	} else {
		err = err2
	}
	e, ok := errors.Cause(err).(*terror.Error)
	c.Assert(ok, IsTrue)
	c.Assert(int(e.Code()), Equals, allegrosql.ErrLockDeadlock)
}

func (s *testPessimisticSuite) TestSingleStatementRollback(c *C) {
	if *withEinsteinDB {
		c.Skip("skip with einsteindb because cluster manipulate is not available")
	}
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists pessimistic")
	tk.MustInterDirc("create causet single_memex (id int primary key, v int)")
	tk.MustInterDirc("insert into single_memex values (1, 1), (2, 1), (3, 1), (4, 1)")
	tblID := tk.GetBlockID("single_memex")
	s.cluster.SplitBlock(tblID, 2)
	region1Key := codec.EncodeBytes(nil, blockcodec.EncodeRowKeyWithHandle(tblID, ekv.IntHandle(1)))
	region1, _ := s.cluster.GetRegionByKey(region1Key)
	region1ID := region1.Id
	region2Key := codec.EncodeBytes(nil, blockcodec.EncodeRowKeyWithHandle(tblID, ekv.IntHandle(3)))
	region2, _ := s.cluster.GetRegionByKey(region2Key)
	region2ID := region2.Id

	syncCh := make(chan bool)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/SingleStmtDeadLockRetrySleep", "return"), IsNil)
	go func() {
		tk2.MustInterDirc("begin pessimistic")
		<-syncCh
		// tk2 will go first, so tk will meet deadlock and retry, tk2 will resolve pessimistic rollback
		// dagger on key 3 after dagger ttl
		s.cluster.ScheduleDelay(tk2.Se.GetStochastikVars().TxnCtx.StartTS, region2ID, time.Millisecond*3)
		tk2.MustInterDirc("uFIDelate single_memex set v = v + 1")
		tk2.MustInterDirc("commit")
		<-syncCh
	}()
	tk.MustInterDirc("begin pessimistic")
	syncCh <- true
	s.cluster.ScheduleDelay(tk.Se.GetStochastikVars().TxnCtx.StartTS, region1ID, time.Millisecond*10)
	tk.MustInterDirc("uFIDelate single_memex set v = v + 1")
	tk.MustInterDirc("commit")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/SingleStmtDeadLockRetrySleep"), IsNil)
	syncCh <- true
}

func (s *testPessimisticSuite) TestFirstStatementFail(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists first")
	tk.MustInterDirc("create causet first (k int unique)")
	tk.MustInterDirc("insert first values (1)")
	tk.MustInterDirc("begin pessimistic")
	_, err := tk.InterDirc("insert first values (1)")
	c.Assert(err, NotNil)
	tk.MustInterDirc("insert first values (2)")
	tk.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestKeyExistsCheck(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists chk")
	tk.MustInterDirc("create causet chk (k int primary key)")
	tk.MustInterDirc("insert chk values (1)")
	tk.MustInterDirc("delete from chk where k = 1")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert chk values (1)")
	tk.MustInterDirc("commit")

	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("begin optimistic")
	tk1.MustInterDirc("insert chk values (1), (2), (3)")
	_, err := tk1.InterDirc("commit")
	c.Assert(err, NotNil)

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert chk values (2)")
	tk.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestInsertOnDup(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists dup")
	tk.MustInterDirc("create causet dup (id int primary key, c int)")
	tk.MustInterDirc("begin pessimistic")

	tk2.MustInterDirc("insert dup values (1, 1)")
	tk.MustInterDirc("insert dup values (1, 1) on duplicate key uFIDelate c = c + 1")
	tk.MustInterDirc("commit")
	tk.MustQuery("select * from dup").Check(testkit.Rows("1 2"))
}

func (s *testPessimisticSuite) TestPointGetKeyLock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists point")
	tk.MustInterDirc("create causet point (id int primary key, u int unique, c int)")
	syncCh := make(chan struct{})

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate point set c = c + 1 where id = 1")
	tk.MustInterDirc("delete from point where u = 2")
	go func() {
		tk2.MustInterDirc("begin pessimistic")
		_, err1 := tk2.InterDirc("insert point values (1, 1, 1)")
		c.Check(ekv.ErrKeyExists.Equal(err1), IsTrue)
		_, err1 = tk2.InterDirc("insert point values (2, 2, 2)")
		c.Check(ekv.ErrKeyExists.Equal(err1), IsTrue)
		tk2.MustInterDirc("rollback")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustInterDirc("insert point values (1, 1, 1)")
	tk.MustInterDirc("insert point values (2, 2, 2)")
	tk.MustInterDirc("commit")
	syncCh <- struct{}{}

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from point where id = 3 for uFIDelate")
	tk.MustInterDirc("select * from point where u = 4 for uFIDelate")
	go func() {
		tk2.MustInterDirc("begin pessimistic")
		_, err1 := tk2.InterDirc("insert point values (3, 3, 3)")
		c.Check(ekv.ErrKeyExists.Equal(err1), IsTrue)
		_, err1 = tk2.InterDirc("insert point values (4, 4, 4)")
		c.Check(ekv.ErrKeyExists.Equal(err1), IsTrue)
		tk2.MustInterDirc("rollback")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustInterDirc("insert point values (3, 3, 3)")
	tk.MustInterDirc("insert point values (4, 4, 4)")
	tk.MustInterDirc("commit")
	syncCh <- struct{}{}
}

func (s *testPessimisticSuite) TestBankTransfer(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists accounts")
	tk.MustInterDirc("create causet accounts (id int primary key, c int)")
	tk.MustInterDirc("insert accounts values (1, 100), (2, 100), (3, 100)")
	syncCh := make(chan struct{})

	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from accounts where id = 1 for uFIDelate").Check(testkit.Rows("1 100"))
	go func() {
		tk2.MustInterDirc("begin pessimistic")
		tk2.MustInterDirc("select * from accounts where id = 2 for uFIDelate")
		<-syncCh
		tk2.MustInterDirc("select * from accounts where id = 3 for uFIDelate")
		tk2.MustInterDirc("uFIDelate accounts set c = 50 where id = 2")
		tk2.MustInterDirc("uFIDelate accounts set c = 150 where id = 3")
		tk2.MustInterDirc("commit")
		<-syncCh
	}()
	syncCh <- struct{}{}
	tk.MustQuery("select * from accounts where id = 2 for uFIDelate").Check(testkit.Rows("2 50"))
	tk.MustInterDirc("uFIDelate accounts set c = 50 where id = 1")
	tk.MustInterDirc("uFIDelate accounts set c = 100 where id = 2")
	tk.MustInterDirc("commit")
	syncCh <- struct{}{}
	tk.MustQuery("select sum(c) from accounts").Check(testkit.Rows("300"))
}

func (s *testPessimisticSuite) TestLockUnchangedRowKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists unchanged")
	tk.MustInterDirc("create causet unchanged (id int primary key, c int)")
	tk.MustInterDirc("insert unchanged values (1, 1), (2, 2)")

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate unchanged set c = 1 where id < 2")

	tk2.MustInterDirc("begin pessimistic")
	err := tk2.InterDircToErr("select * from unchanged where id = 1 for uFIDelate nowait")
	c.Assert(err, NotNil)

	tk.MustInterDirc("rollback")

	tk2.MustQuery("select * from unchanged where id = 1 for uFIDelate nowait")

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert unchanged values (2, 2) on duplicate key uFIDelate c = values(c)")

	err = tk2.InterDircToErr("select * from unchanged where id = 2 for uFIDelate nowait")
	c.Assert(err, NotNil)

	tk.MustInterDirc("commit")

	tk2.MustQuery("select * from unchanged where id = 1 for uFIDelate nowait")
	tk2.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestOptimisticConflicts(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists conflict")
	tk.MustInterDirc("create causet conflict (id int primary key, c int)")
	tk.MustInterDirc("insert conflict values (1, 1)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from conflict where id = 1 for uFIDelate")
	syncCh := make(chan struct{})
	go func() {
		tk2.MustInterDirc("uFIDelate conflict set c = 3 where id = 1")
		<-syncCh
	}()
	time.Sleep(time.Millisecond * 10)
	tk.MustInterDirc("uFIDelate conflict set c = 2 where id = 1")
	tk.MustInterDirc("commit")
	syncCh <- struct{}{}
	tk.MustQuery("select c from conflict where id = 1").Check(testkit.Rows("3"))

	// Check pessimistic dagger is not resolved.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate conflict set c = 4 where id = 1")
	tk2.MustInterDirc("begin optimistic")
	tk2.MustInterDirc("uFIDelate conflict set c = 5 where id = 1")
	_, err := tk2.InterDirc("commit")
	c.Check(err, NotNil)
	tk.MustInterDirc("rollback")

	// UFIDelate snapshotTS after a conflict, invalidate snapshot cache.
	tk.MustInterDirc("truncate causet conflict")
	tk.MustInterDirc("insert into conflict values (1, 2)")
	tk.MustInterDirc("begin pessimistic")
	// This ALLEGROALLEGROSQL use BatchGet and cache data in the txn snapshot.
	// It can be changed to other ALLEGROSQLs that use BatchGet.
	tk.MustInterDirc("insert ignore into conflict values (1, 2)")

	tk2.MustInterDirc("uFIDelate conflict set c = c - 1")

	// Make the txn uFIDelate its forUFIDelateTS.
	tk.MustQuery("select * from conflict where id = 1 for uFIDelate").Check(testkit.Rows("1 1"))
	// Cover a bug that the txn snapshot doesn't invalidate cache after ts change.
	tk.MustInterDirc("insert into conflict values (1, 999) on duplicate key uFIDelate c = c + 2")
	tk.MustInterDirc("commit")
	tk.MustQuery("select * from conflict").Check(testkit.Rows("1 3"))
}

func (s *testPessimisticSuite) TestSelectForUFIDelateNoWait(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("create causet tk (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")

	tk.MustInterDirc("set @@autocommit = 0")
	tk2.MustInterDirc("set @@autocommit = 0")
	tk3.MustInterDirc("set @@autocommit = 0")

	// point get with no autocommit
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from tk where c1 = 2 for uFIDelate") // dagger succ

	tk2.MustInterDirc("begin pessimistic")
	_, err := tk2.InterDirc("select * from tk where c1 = 2 for uFIDelate nowait")
	c.Check(err, NotNil)
	tk.MustInterDirc("commit")
	tk2.MustInterDirc("select * from tk where c1 = 2 for uFIDelate nowait") // dagger succ

	tk3.MustInterDirc("begin pessimistic")
	_, err = tk3.InterDirc("select * from tk where c1 = 2 for uFIDelate nowait")
	c.Check(err, NotNil)

	tk2.MustInterDirc("commit")
	tk3.MustInterDirc("select * from tk where c1 = 2 for uFIDelate")
	tk3.MustInterDirc("commit")
	tk.MustInterDirc("commit")

	tk3.MustInterDirc("begin pessimistic")
	tk3.MustInterDirc("uFIDelate tk set c2 = c2 + 1 where c1 = 3")
	tk2.MustInterDirc("begin pessimistic")
	_, err = tk2.InterDirc("select * from tk where c1 = 3 for uFIDelate nowait")
	c.Check(err, NotNil)
	tk3.MustInterDirc("commit")
	tk2.MustInterDirc("select * from tk where c1 = 3 for uFIDelate nowait")
	tk2.MustInterDirc("commit")

	tk.MustInterDirc("commit")
	tk2.MustInterDirc("commit")
	tk3.MustInterDirc("commit")

	// scan with no autocommit
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from tk where c1 >= 2 for uFIDelate")
	tk2.MustInterDirc("begin pessimistic")
	_, err = tk2.InterDirc("select * from tk where c1 = 2 for uFIDelate nowait")
	c.Check(err, NotNil)
	_, err = tk2.InterDirc("select * from tk where c1 > 3 for uFIDelate nowait")
	c.Check(err, NotNil)
	tk2.MustInterDirc("select * from tk where c1 = 1 for uFIDelate nowait")
	tk2.MustInterDirc("commit")
	tk.MustQuery("select * from tk where c1 >= 2 for uFIDelate").Check(testkit.Rows("2 2", "3 4", "4 4", "5 5"))
	tk.MustInterDirc("commit")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate tk set c2 = c2 + 10 where c1 > 3")
	tk3.MustInterDirc("begin pessimistic")
	_, err = tk3.InterDirc("select * from tk where c1 = 5 for uFIDelate nowait")
	c.Check(err, NotNil)
	tk3.MustInterDirc("select * from tk where c1 = 1 for uFIDelate nowait")
	tk.MustInterDirc("commit")
	tk3.MustQuery("select * from tk where c1 > 3 for uFIDelate nowait").Check(testkit.Rows("4 14", "5 15"))
	tk3.MustInterDirc("commit")

	//delete
	tk3.MustInterDirc("begin pessimistic")
	tk3.MustInterDirc("delete from tk where c1 <= 2")
	tk.MustInterDirc("begin pessimistic")
	_, err = tk.InterDirc("select * from tk where c1 = 1 for uFIDelate nowait")
	c.Check(err, NotNil)
	tk3.MustInterDirc("commit")
	tk.MustQuery("select * from tk where c1 > 1 for uFIDelate nowait").Check(testkit.Rows("3 4", "4 14", "5 15"))
	tk.MustInterDirc("uFIDelate tk set c2 = c2 + 1 where c1 = 5")
	tk2.MustInterDirc("begin pessimistic")
	_, err = tk2.InterDirc("select * from tk where c1 = 5 for uFIDelate nowait")
	c.Check(err, NotNil)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from tk where c1 = 5 for uFIDelate nowait").Check(testkit.Rows("5 16"))
	tk2.MustInterDirc("uFIDelate tk set c2 = c2 + 1 where c1 = 5")
	tk2.MustQuery("select * from tk where c1 = 5 for uFIDelate nowait").Check(testkit.Rows("5 17"))
	tk2.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestAsyncRollBackNoWait(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("create causet tk (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,17)")

	tk.MustInterDirc("set @@autocommit = 0")
	tk2.MustInterDirc("set @@autocommit = 0")
	tk3.MustInterDirc("set @@autocommit = 0")

	// test get ts failed for handlePessimisticLockError when using nowait
	// even though async rollback for pessimistic dagger may rollback later locked key if get ts failed from fidel
	// the txn correctness should be ensured
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/interlock/InterDircStmtGetTsError", "return"), IsNil)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/AsyncRollBackSleep", "return"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/interlock/InterDircStmtGetTsError"), IsNil)
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/AsyncRollBackSleep"), IsNil)
	}()
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from tk where c1 > 0 for uFIDelate nowait")
	tk2.MustInterDirc("begin pessimistic")
	// The dagger rollback of this memex is delayed by failpoint AsyncRollBackSleep.
	_, err := tk2.InterDirc("select * from tk where c1 > 0 for uFIDelate nowait")
	c.Check(err, NotNil)
	tk.MustInterDirc("commit")
	// This memex success for now, but its dagger will be rollbacked later by the
	// lingering rollback request, as forUFIDelateTS doesn't change.
	tk2.MustQuery("select * from tk where c1 > 0 for uFIDelate nowait")
	tk2.MustQuery("select * from tk where c1 = 5 for uFIDelate nowait").Check(testkit.Rows("5 17"))
	tk3.MustInterDirc("begin pessimistic")

	c.Skip("tk3 is blocking because tk2 didn't rollback itself")
	// tk3 succ because tk2 rollback itself.
	tk3.MustInterDirc("uFIDelate tk set c2 = 1 where c1 = 5")
	// This will not take effect because the dagger of tk2 was gone.
	tk2.MustInterDirc("uFIDelate tk set c2 = c2 + 100 where c1 > 0")
	_, err = tk2.InterDirc("commit")
	c.Check(err, NotNil) // txn abort because pessimistic dagger not found
	tk3.MustInterDirc("commit")
	tk3.MustInterDirc("begin pessimistic")
	tk3.MustQuery("select * from tk where c1 = 5 for uFIDelate nowait").Check(testkit.Rows("5 1"))
	tk3.MustQuery("select * from tk where c1 = 4 for uFIDelate nowait").Check(testkit.Rows("4 4"))
	tk3.MustQuery("select * from tk where c1 = 3 for uFIDelate nowait").Check(testkit.Rows("3 3"))
	tk3.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestWaitLockKill(c *C) {
	// Test kill command works on waiting pessimistic dagger.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists test_kill")
	tk.MustInterDirc("create causet test_kill (id int primary key, c int)")
	tk.MustInterDirc("insert test_kill values (1, 1)")
	tk.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("set innodb_lock_wait_timeout = 50")
	tk2.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from test_kill where id = 1 for uFIDelate")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		sessVars := tk2.Se.GetStochastikVars()
		succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
		c.Assert(succ, IsTrue)
		wg.Wait()
	}()
	_, err := tk2.InterDirc("uFIDelate test_kill set c = c + 1 where id = 1")
	wg.Done()
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, einsteindb.ErrQueryInterrupted), IsTrue)
	tk.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestKillStopTTLManager(c *C) {
	// Test killing an idle pessimistic stochastik stop its ttlManager.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists test_kill")
	tk.MustInterDirc("create causet test_kill (id int primary key, c int)")
	tk.MustInterDirc("insert test_kill values (1, 1)")
	tk.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from test_kill where id = 1 for uFIDelate")
	sessVars := tk.Se.GetStochastikVars()
	succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
	c.Assert(succ, IsTrue)

	// This query should success rather than returning a ResolveLock error.
	tk2.MustInterDirc("uFIDelate test_kill set c = c + 1 where id = 1")
}

func (s *testPessimisticSuite) TestConcurrentInsert(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("create causet tk (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert tk values (1, 1)")
	tk.MustInterDirc("create causet tk1 (c1 int, c2 int)")

	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("begin pessimistic")
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	forUFIDelateTsA := tk1.Se.GetStochastikVars().TxnCtx.GetForUFIDelateTS()
	tk1.MustQuery("select * from tk where c1 = 1 for uFIDelate")
	forUFIDelateTsB := tk1.Se.GetStochastikVars().TxnCtx.GetForUFIDelateTS()
	c.Assert(forUFIDelateTsA, Equals, forUFIDelateTsB)
	tk1.MustQuery("select * from tk where c1 > 0 for uFIDelate")
	forUFIDelateTsC := tk1.Se.GetStochastikVars().TxnCtx.GetForUFIDelateTS()
	c.Assert(forUFIDelateTsC, Greater, forUFIDelateTsB)

	tk2.MustInterDirc("insert tk values (2, 2)")
	tk1.MustQuery("select * from tk for uFIDelate").Check(testkit.Rows("1 1", "2 2"))
	tk2.MustInterDirc("insert tk values (3, 3)")
	tk1.MustInterDirc("uFIDelate tk set c2 = c2 + 1")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(3))
	tk2.MustInterDirc("insert tk values (4, 4)")
	tk1.MustInterDirc("delete from tk")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(4))
	tk2.MustInterDirc("insert tk values (5, 5)")
	tk1.MustInterDirc("insert into tk1 select * from tk")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(1))
	tk2.MustInterDirc("insert tk values (6, 6)")
	tk1.MustInterDirc("replace into tk1 select * from tk")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(2))
	tk2.MustInterDirc("insert tk values (7, 7)")
	// This test is used to test when the selectCauset is a PointGetCauset, and we didn't uFIDelate its forUFIDelateTS.
	tk1.MustInterDirc("insert into tk1 select * from tk where c1 = 7")
	c.Assert(tk1.Se.AffectedRows(), Equals, uint64(1))
	tk1.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestInnodbLockWaitTimeout(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("create causet tk (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	// tk set global
	tk.MustInterDirc("set global innodb_lock_wait_timeout = 3")
	tk.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 50"))

	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk2.MustInterDirc("set innodb_lock_wait_timeout = 2")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 2"))

	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk3.MustInterDirc("set innodb_lock_wait_timeout = 1")
	tk3.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	tk2.MustInterDirc("set @@autocommit = 0")
	tk3.MustInterDirc("set @@autocommit = 0")

	tk4 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk4.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 3"))
	tk4.MustInterDirc("set @@autocommit = 0")

	// tk2 dagger c1 = 1
	tk2.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("select * from tk where c1 = 1 for uFIDelate") // dagger succ c1 = 1

	// Parallel the blocking tests to accelerate CI.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// tk3 try dagger c1 = 1 timeout 1sec
		tk3.MustInterDirc("begin pessimistic")
		_, err := tk3.InterDirc("select * from tk where c1 = 1 for uFIDelate")
		c.Check(err.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())
		tk3.MustInterDirc("commit")
	}()

	go func() {
		defer wg.Done()
		// tk5 try dagger c1 = 1 timeout 2sec
		tk5 := testkit.NewTestKitWithInit(c, s.causetstore)
		tk5.MustInterDirc("set innodb_lock_wait_timeout = 2")
		tk5.MustInterDirc("begin pessimistic")
		_, err := tk5.InterDirc("uFIDelate tk set c2 = c2 - 1 where c1 = 1")
		c.Check(err.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())
		tk5.MustInterDirc("rollback")
	}()

	// tk4 dagger c1 = 2
	tk4.MustInterDirc("begin pessimistic")
	tk4.MustInterDirc("uFIDelate tk set c2 = c2 + 1 where c1 = 2") // dagger succ c1 = 2 by uFIDelate

	tk2.MustInterDirc("set innodb_lock_wait_timeout = 1")
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	start := time.Now()
	_, err := tk2.InterDirc("delete from tk where c1 = 2")
	c.Check(time.Since(start), GreaterEqual, 1000*time.Millisecond)
	c.Check(time.Since(start), Less, 3000*time.Millisecond) // unit test diff should not be too big
	c.Check(err.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())

	tk4.MustInterDirc("commit")

	tk.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 50"))
	tk.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 3")) // tk4 uFIDelate commit work, tk2 delete should be rollbacked

	// test stmtRollBack caused by timeout but not the whole transaction
	tk2.MustInterDirc("uFIDelate tk set c2 = c2 + 2 where c1 = 2")            // tk2 dagger succ c1 = 2 by uFIDelate
	tk2.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 5")) // tk2 uFIDelate c2 succ

	tk3.MustInterDirc("begin pessimistic")
	tk3.MustInterDirc("select * from tk where c1 = 3 for uFIDelate") // tk3  dagger c1 = 3 succ

	start = time.Now()
	_, err = tk2.InterDirc("delete from tk where c1 = 3") // tk2 tries to dagger c1 = 3 fail, this delete should be rollback, but previous uFIDelate should be keeped
	c.Check(time.Since(start), GreaterEqual, 1000*time.Millisecond)
	c.Check(time.Since(start), Less, 3000*time.Millisecond) // unit test diff should not be too big
	c.Check(err.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())

	tk2.MustInterDirc("commit")
	tk3.MustInterDirc("commit")

	tk.MustQuery(`select * from tk where c1 = 1`).Check(testkit.Rows("1 1"))
	tk.MustQuery(`select * from tk where c1 = 2`).Check(testkit.Rows("2 5")) // tk2 uFIDelate succ
	tk.MustQuery(`select * from tk where c1 = 3`).Check(testkit.Rows("3 3")) // tk2 delete should fail
	tk.MustQuery(`select * from tk where c1 = 4`).Check(testkit.Rows("4 4"))
	tk.MustQuery(`select * from tk where c1 = 5`).Check(testkit.Rows("5 5"))

	// clean
	tk.MustInterDirc("drop causet if exists tk")
	tk4.MustInterDirc("commit")

	wg.Wait()
}

func (s *testPessimisticSuite) TestPushConditionCheckForPessimisticTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	defer tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (i int key)")
	tk.MustInterDirc("insert into t values (1)")

	tk.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	tk.MustInterDirc("begin")
	tk1.MustInterDirc("delete from t where i = 1")
	tk.MustInterDirc("insert into t values (1) on duplicate key uFIDelate i = values(i)")
	tk.MustInterDirc("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
}

func (s *testPessimisticSuite) TestInnodbLockWaitTimeoutWaitStart(c *C) {
	// prepare work
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	defer tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("create causet tk (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustInterDirc("set global innodb_lock_wait_timeout = 1")

	// raise pessimistic transaction in tk2 and trigger failpoint returning ErrWriteConflict
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustQuery(`show variables like "innodb_lock_wait_timeout"`).Check(testkit.Rows("innodb_lock_wait_timeout 1"))

	// tk3 gets the pessimistic dagger
	tk3.MustInterDirc("begin pessimistic")
	tk3.MustQuery("select * from tk where c1 = 1 for uFIDelate")

	tk2.MustInterDirc("begin pessimistic")
	done := make(chan error)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/PessimisticLockErrWriteConflict", "return"), IsNil)
	var duration time.Duration
	go func() {
		var err error
		start := time.Now()
		defer func() {
			duration = time.Since(start)
			done <- err
		}()
		_, err = tk2.InterDirc("select * from tk where c1 = 1 for uFIDelate")
	}()
	time.Sleep(time.Millisecond * 100)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/PessimisticLockErrWriteConflict"), IsNil)
	waitErr := <-done
	c.Assert(waitErr, NotNil)
	c.Check(waitErr.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())
	c.Check(duration, GreaterEqual, 1000*time.Millisecond)
	c.Check(duration, LessEqual, 3000*time.Millisecond)
	tk2.MustInterDirc("rollback")
	tk3.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestBatchPointGetWriteConflict(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (i int primary key, c int);")
	tk.MustInterDirc("insert t values (1, 1), (2, 2), (3, 3)")
	tk1.MustInterDirc("begin pessimistic")
	tk1.MustQuery("select * from t where i = 1").Check(testkit.Rows("1 1"))
	tk.MustInterDirc("uFIDelate t set c = c + 1")
	tk1.MustQuery("select * from t where i in (1, 3) for uFIDelate").Check(testkit.Rows("1 2", "3 4"))
	tk1.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestPessimisticReadCommitted(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("use test")

	tk.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	tk1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	// test SI
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t(i int key);")
	tk.MustInterDirc("insert into t values (1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustInterDirc("begin;")
	tk1.MustInterDirc("begin;")
	tk.MustInterDirc("uFIDelate t set i = -i;")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tk1.MustInterDirc("uFIDelate t set i = -i;")
		wg.Done()
	}()
	tk.MustInterDirc("commit;")
	wg.Wait()

	tk1.MustInterDirc("commit;")

	// test RC
	tk.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	tk1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t(i int key);")
	tk.MustInterDirc("insert into t values (1);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustInterDirc("begin;")
	tk1.MustInterDirc("begin;")
	tk.MustInterDirc("uFIDelate t set i = -i;")

	wg.Add(1)
	go func() {
		tk1.MustInterDirc("uFIDelate t set i = -i;")
		wg.Done()
	}()
	tk.MustInterDirc("commit;")
	wg.Wait()

	tk1.MustInterDirc("commit;")

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t(i int key, j int unique key, k int, l int, m int, key (k));")
	tk.MustInterDirc("insert into t values (1, 1, 1, 1, 1);")

	// Set it back to RR to test set transaction memex.
	tk.MustInterDirc("set tx_isolation = 'REPEATABLE-READ'")
	tk1.MustInterDirc("set tx_isolation = 'REPEATABLE-READ'")

	// test one shot and some reads.
	tk.MustInterDirc("set transaction isolation level read committed")
	tk.MustInterDirc("begin;")

	// test causet reader
	tk.MustQuery("select l from t where l = 1").Check(testkit.Rows("1"))
	tk1.MustInterDirc("uFIDelate t set l = l + 1 where l = 1;")
	tk.MustQuery("select l from t where l = 2").Check(testkit.Rows("2"))
	tk1.MustInterDirc("uFIDelate t set l = l + 1 where l = 2;")
	tk.MustQuery("select l from t where l = 3").Check(testkit.Rows("3"))

	// test index reader
	tk.MustQuery("select k from t where k = 1").Check(testkit.Rows("1"))
	tk1.MustInterDirc("uFIDelate t set k = k + 1 where k = 1;")
	tk.MustQuery("select k from t where k = 2").Check(testkit.Rows("2"))
	tk1.MustInterDirc("uFIDelate t set k = k + 1 where k = 2;")
	tk.MustQuery("select k from t where k = 3").Check(testkit.Rows("3"))

	// test double read
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("1"))
	tk1.MustInterDirc("uFIDelate t set m = m + 1 where k = 3;")
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("2"))
	tk1.MustInterDirc("uFIDelate t set m = m + 1 where k = 3;")
	tk.MustQuery("select m from t where k = 3").Check(testkit.Rows("3"))

	// test point get plan
	tk.MustQuery("select m from t where i = 1").Check(testkit.Rows("3"))
	tk1.MustInterDirc("uFIDelate t set m = m + 1 where i = 1;")
	tk.MustQuery("select m from t where i = 1").Check(testkit.Rows("4"))
	tk1.MustInterDirc("uFIDelate t set m = m + 1 where j = 1;")
	tk.MustQuery("select m from t where j = 1").Check(testkit.Rows("5"))

	// test batch point get plan
	tk1.MustInterDirc("insert into t values (2, 2, 2, 2, 2);")
	tk.MustQuery("select m from t where i in (1, 2)").Check(testkit.Rows("5", "2"))
	tk1.MustInterDirc("uFIDelate t set m = m + 1 where i in (1, 2);")
	tk.MustQuery("select m from t where i in (1, 2)").Check(testkit.Rows("6", "3"))
	tk1.MustInterDirc("uFIDelate t set m = m + 1 where j in (1, 2);")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))

	tk.MustInterDirc("commit;")

	// test for NewTxn()
	tk.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	tk.MustInterDirc("begin optimistic;")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))
	tk.MustInterDirc("begin pessimistic;")
	tk.MustQuery("select m from t where j in (1, 2)").Check(testkit.Rows("7", "4"))
	tk.MustInterDirc("commit;")
}

func (s *testPessimisticSuite) TestPessimisticLockNonExistsKey(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (k int primary key, c int)")
	tk.MustInterDirc("insert t values (1, 1), (3, 3), (5, 5)")

	// verify that select with project and filter on a non exists key still locks the key.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert t values (8, 8)") // Make the transaction dirty.
	tk.MustQuery("select c + 1 from t where k = 2 and c = 2 for uFIDelate").Check(testkit.Rows())
	explainStr := tk.MustQuery("explain select c + 1 from t where k = 2 and c = 2 for uFIDelate").Rows()[0][0].(string)
	c.Assert(strings.Contains(explainStr, "UnionScan"), IsFalse)
	tk.MustQuery("select * from t where k in (4, 5, 7) for uFIDelate").Check(testkit.Rows("5 5"))

	tk1.MustInterDirc("begin pessimistic")
	err := tk1.InterDircToErr("select * from t where k = 2 for uFIDelate nowait")
	c.Check(einsteindb.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	err = tk1.InterDircToErr("select * from t where k = 4 for uFIDelate nowait")
	c.Check(einsteindb.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	err = tk1.InterDircToErr("select * from t where k = 7 for uFIDelate nowait")
	c.Check(einsteindb.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	tk.MustInterDirc("rollback")
	tk1.MustInterDirc("rollback")

	// verify uFIDelate and delete non exists keys still locks the key.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert t values (8, 8)") // Make the transaction dirty.
	tk.MustInterDirc("uFIDelate t set c = c + 1 where k in (2, 3, 4) and c > 0")
	tk.MustInterDirc("delete from t where k in (5, 6, 7) and c > 0")

	tk1.MustInterDirc("begin pessimistic")
	err = tk1.InterDircToErr("select * from t where k = 2 for uFIDelate nowait")
	c.Check(einsteindb.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	err = tk1.InterDircToErr("select * from t where k = 6 for uFIDelate nowait")
	c.Check(einsteindb.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	tk.MustInterDirc("rollback")
	tk1.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestPessimisticCommitReadLock(c *C) {
	// set dagger ttl to 3s, tk1 dagger wait timeout is 2s
	atomic.StoreUint64(&einsteindb.ManagedLockTTL, 3000)
	defer atomic.StoreUint64(&einsteindb.ManagedLockTTL, 300)
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("use test")

	tk.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	tk1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	tk1.MustInterDirc("set innodb_lock_wait_timeout = 2")

	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t(i int key primary key, j int);")
	tk.MustInterDirc("insert into t values (1, 2);")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))

	// tk dagger one event
	tk.MustInterDirc("begin;")
	tk.MustQuery("select * from t for uFIDelate").Check(testkit.Rows("1 2"))
	tk1.MustInterDirc("begin;")
	done := make(chan error)
	go func() {
		var err error
		defer func() {
			done <- err
		}()
		// let txn not found could be checked by dagger wait timeout utility
		err = failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnNotFoundRetTTL", "return")
		if err != nil {
			return
		}
		_, err = tk1.InterDirc("uFIDelate t set j = j + 1 where i = 1")
		if err != nil {
			return
		}
		err = failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnNotFoundRetTTL")
		if err != nil {
			return
		}
		_, err = tk1.InterDirc("commit")
	}()
	// let the dagger be hold for a while
	time.Sleep(time.Millisecond * 50)
	tk.MustInterDirc("commit")
	waitErr := <-done
	c.Assert(waitErr, IsNil)
}

func (s *testPessimisticSuite) TestPessimisticLockReadValue(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t(i int, j int, k int, unique key uk(j));")
	tk.MustInterDirc("insert into t values (1, 1, 1);")

	// tk1 will left op_lock record
	tk1 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk1.MustInterDirc("use test")
	tk1.MustInterDirc("begin optimistic")
	tk1.MustQuery("select * from t where j = 1 for uFIDelate").Check(testkit.Rows("1 1 1"))
	tk1.MustQuery("select * from t where j = 1 for uFIDelate").Check(testkit.Rows("1 1 1"))
	tk1.MustInterDirc("commit")

	// tk2 pessimistic dagger read value
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustInterDirc("begin pessimistic")
	tk2.MustQuery("select * from t where j = 1 for uFIDelate").Check(testkit.Rows("1 1 1"))
	tk2.MustQuery("select * from t where j = 1 for uFIDelate").Check(testkit.Rows("1 1 1"))
	tk2.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestRCWaitTSOTwice(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t (i int key)")
	tk.MustInterDirc("insert into t values (1)")
	tk.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	tk.MustInterDirc("set tx_isolation = 'read-committed'")
	tk.MustInterDirc("set autocommit = 0")
	tk.MustQuery("select * from t where i = 1").Check(testkit.Rows("1"))
	tk.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestNonAutoCommitWithPessimisticMode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert into t1 values (1, 1)")
	tk.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	tk.MustInterDirc("set autocommit = 0")
	tk.MustQuery("select * from t1 where c2 = 1 for uFIDelate").Check(testkit.Rows("1 1"))
	tk2.MustInterDirc("insert into t1 values(2, 1)")
	tk.MustQuery("select * from t1 where c2 = 1 for uFIDelate").Check(testkit.Rows("1 1", "2 1"))
	tk.MustInterDirc("commit")
	tk2.MustInterDirc("insert into t1 values(3, 1)")
	tk.MustInterDirc("set tx_isolation = 'read-committed'")
	tk.MustQuery("select * from t1 where c2 = 1 for uFIDelate").Check(testkit.Rows("1 1", "2 1", "3 1"))
	tk2.MustInterDirc("insert into t1 values(4, 1)")
	tk.MustQuery("select * from t1 where c2 = 1 for uFIDelate").Check(testkit.Rows("1 1", "2 1", "3 1", "4 1"))
	tk.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestBatchPointGetLocHoTTex(c *C) {
	atomic.StoreUint64(&einsteindb.ManagedLockTTL, 3000)
	defer atomic.StoreUint64(&einsteindb.ManagedLockTTL, 300)
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustInterDirc("use test")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustInterDirc("insert into t1 values (1, 1, 1)")
	tk.MustInterDirc("insert into t1 values (5, 5, 5)")
	tk.MustInterDirc("insert into t1 values (10, 10, 10)")
	tk.MustInterDirc("begin pessimistic")
	// the handle does not exist and the index key should be locked as point get interlock did
	tk.MustQuery("select * from t1 where c2 in (2, 3) for uFIDelate").Check(testkit.Rows())
	tk2.MustInterDirc("set innodb_lock_wait_timeout = 1")
	tk2.MustInterDirc("begin pessimistic")
	err := tk2.InterDircToErr("insert into t1 values(2, 2, 2)")
	c.Assert(err, NotNil)
	c.Assert(einsteindb.ErrLockWaitTimeout.Equal(err), IsTrue)
	err = tk2.InterDircToErr("select * from t1 where c2 = 3 for uFIDelate nowait")
	c.Assert(err, NotNil)
	c.Assert(einsteindb.ErrLockAcquireFailAndNoWaitSet.Equal(err), IsTrue)
	tk.MustInterDirc("rollback")
	tk2.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestLockGotKeysInRC(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk2.MustInterDirc("use test")
	tk.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	tk2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustInterDirc("insert into t1 values (1, 1, 1)")
	tk.MustInterDirc("insert into t1 values (5, 5, 5)")
	tk.MustInterDirc("insert into t1 values (10, 10, 10)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in (2, 3) for uFIDelate").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c2 in (2, 3) for uFIDelate").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c1 = 2 for uFIDelate").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where c2 = 2 for uFIDelate").Check(testkit.Rows())
	tk2.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("insert into t1 values(2, 2, 2)")
	tk2.MustInterDirc("select * from t1 where c1 = 3 for uFIDelate nowait")
	tk2.MustInterDirc("select * from t1 where c2 = 3 for uFIDelate nowait")
	tk.MustInterDirc("rollback")
	tk2.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestBatchPointGetAlreadyLocked(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c1 int, c2 int, c3 int, primary key(c1, c2))")
	tk.MustInterDirc("insert t values (1, 1, 1), (2, 2, 2)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from t where c1 > 1 for uFIDelate").Check(testkit.Rows("2 2 2"))
	tk.MustQuery("select * from t where (c1, c2) in ((2,2)) for uFIDelate").Check(testkit.Rows("2 2 2"))
	tk.MustInterDirc("commit")
}

func (s *testPessimisticSuite) TestRollbackWakeupBlockedTxn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustInterDirc("use test")
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	tk.MustInterDirc("insert into t1 values (1, 1, 1)")
	tk.MustInterDirc("insert into t1 values (5, 5, 5)")
	tk.MustInterDirc("insert into t1 values (10, 10, 10)")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnExpireRetTTL", "return"), IsNil)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/getTxnStatusDelay", "return"), IsNil)
	tk.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("set innodb_lock_wait_timeout = 1")
	tk2.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate t1 set c3 = c3 + 1")
	errCh := make(chan error)
	go func() {
		var err error
		defer func() {
			errCh <- err
		}()
		_, err = tk2.InterDirc("uFIDelate t1 set c3 = 100 where c1 = 1")
		if err != nil {
			return
		}
	}()
	time.Sleep(time.Millisecond * 30)
	tk.MustInterDirc("rollback")
	err := <-errCh
	c.Assert(err, IsNil)
	tk2.MustInterDirc("rollback")
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnExpireRetTTL"), IsNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/getTxnStatusDelay"), IsNil)
}

func (s *testPessimisticSuite) TestRCSubQuery(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t, t1")
	tk.MustInterDirc("create causet `t` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )")
	tk.MustInterDirc("insert into t values(1, 3)")
	tk.MustInterDirc("create causet `t1` ( `c1` int(11) not null, `c2` int(11) default null, primary key (`c1`) )")
	tk.MustInterDirc("insert into t1 values(1, 3)")
	tk.MustInterDirc("set transaction isolation level read committed")
	tk.MustInterDirc("begin pessimistic")

	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2.MustInterDirc("uFIDelate t1 set c2 = c2 + 1")

	tk.MustQuery("select * from t1 where c1 = (select 1) and 1=1;").Check(testkit.Rows("1 4"))
	tk.MustQuery("select * from t1 where c1 = (select c1 from t where c1 = 1) and 1=1;").Check(testkit.Rows("1 4"))
	tk.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestGenerateDefCausPointGet(c *C) {
	atomic.StoreUint64(&einsteindb.ManagedLockTTL, 3000)
	defer atomic.StoreUint64(&einsteindb.ManagedLockTTL, 300)
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	defer func() {
		tk.MustInterDirc(fmt.Sprintf("set global milevadb_row_format_version = %d", variable.DefMilevaDBRowFormatV2))
	}()
	tests2 := []int{variable.DefMilevaDBRowFormatV1, variable.DefMilevaDBRowFormatV2}
	for _, rowFormat := range tests2 {
		tk.MustInterDirc(fmt.Sprintf("set global milevadb_row_format_version = %d", rowFormat))
		tk.MustInterDirc("drop causet if exists tu")
		tk.MustInterDirc("CREATE TABLE `tu`(`x` int, `y` int, `z` int GENERATED ALWAYS AS (x + y) VIRTUAL, PRIMARY KEY (`x`), UNIQUE KEY `idz` (`z`))")
		tk.MustInterDirc("insert into tu(x, y) values(1, 2);")

		// test point get dagger
		tk.MustInterDirc("begin pessimistic")
		tk.MustQuery("select * from tu where z = 3 for uFIDelate").Check(testkit.Rows("1 2 3"))
		tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
		tk2.MustInterDirc("begin pessimistic")
		err := tk2.InterDircToErr("select * from tu where z = 3 for uFIDelate nowait")
		c.Assert(err, NotNil)
		c.Assert(terror.ErrorEqual(err, einsteindb.ErrLockAcquireFailAndNoWaitSet), IsTrue)
		tk.MustInterDirc("begin pessimistic")
		tk.MustInterDirc("insert into tu(x, y) values(2, 2);")
		err = tk2.InterDircToErr("select * from tu where z = 4 for uFIDelate nowait")
		c.Assert(err, NotNil)
		c.Assert(terror.ErrorEqual(err, einsteindb.ErrLockAcquireFailAndNoWaitSet), IsTrue)

		// test batch point get dagger
		tk.MustInterDirc("begin pessimistic")
		tk2.MustInterDirc("begin pessimistic")
		tk.MustQuery("select * from tu where z in (1, 3, 5) for uFIDelate").Check(testkit.Rows("1 2 3"))
		tk2.MustInterDirc("begin pessimistic")
		err = tk2.InterDircToErr("select x from tu where z in (3, 7, 9) for uFIDelate nowait")
		c.Assert(err, NotNil)
		c.Assert(terror.ErrorEqual(err, einsteindb.ErrLockAcquireFailAndNoWaitSet), IsTrue)
		tk.MustInterDirc("begin pessimistic")
		tk.MustInterDirc("insert into tu(x, y) values(5, 6);")
		err = tk2.InterDircToErr("select * from tu where z = 11 for uFIDelate nowait")
		c.Assert(err, NotNil)
		c.Assert(terror.ErrorEqual(err, einsteindb.ErrLockAcquireFailAndNoWaitSet), IsTrue)

		tk.MustInterDirc("commit")
		tk2.MustInterDirc("commit")
	}
}

func (s *testPessimisticSuite) TestTxnWithExpiredPessimisticLocks(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int, c3 int, unique key uk(c2))")
	defer tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("insert into t1 values (1, 1, 1)")
	tk.MustInterDirc("insert into t1 values (5, 5, 5)")

	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in(1, 5) for uFIDelate").Check(testkit.Rows("1 1 1", "5 5 5"))
	atomic.StoreUint32(&tk.Se.GetStochastikVars().TxnCtx.LockExpire, 1)
	err := tk.InterDircToErr("select * from t1 where c1 in(1, 5)")
	c.Assert(terror.ErrorEqual(err, einsteindb.ErrLockExpire), IsTrue)
	tk.MustInterDirc("commit")

	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("select * from t1 where c1 in(1, 5) for uFIDelate").Check(testkit.Rows("1 1 1", "5 5 5"))
	atomic.StoreUint32(&tk.Se.GetStochastikVars().TxnCtx.LockExpire, 1)
	err = tk.InterDircToErr("uFIDelate t1 set c2 = c2 + 1")
	c.Assert(terror.ErrorEqual(err, einsteindb.ErrLockExpire), IsTrue)
	atomic.StoreUint32(&tk.Se.GetStochastikVars().TxnCtx.LockExpire, 0)
	tk.MustInterDirc("uFIDelate t1 set c2 = c2 + 1")
	tk.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestKillWaitLockTxn(c *C) {
	// Test kill command works on waiting pessimistic dagger.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists test_kill")
	tk.MustInterDirc("create causet test_kill (id int primary key, c int)")
	tk.MustInterDirc("insert test_kill values (1, 1)")

	tk.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("begin pessimistic")

	tk.MustQuery("select * from test_kill where id = 1 for uFIDelate")
	errCh := make(chan error)
	go func() {
		var err error
		defer func() {
			errCh <- err
		}()
		time.Sleep(20 * time.Millisecond)
		_, err = tk2.InterDirc("uFIDelate test_kill set c = c + 1 where id = 1")
		if err != nil {
			return
		}
	}()
	time.Sleep(100 * time.Millisecond)
	sessVars := tk.Se.GetStochastikVars()
	// dagger query in tk is killed, the ttl manager will stop
	succ := atomic.CompareAndSwapUint32(&sessVars.Killed, 0, 1)
	c.Assert(succ, IsTrue)
	err := <-errCh
	c.Assert(err, IsNil)
	tk.InterDirc("rollback")
	// reset kill
	atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0)
	tk.MustInterDirc("rollback")
	tk2.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestDupLockInconsistency(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int, index b (b))")
	tk.MustInterDirc("insert t (a) values (1), (1)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate t, (select a from t) s set t.b = s.a")
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")
}

func (s *testPessimisticSuite) TestUseLockCacheInRCMode(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists test_kill")
	tk.MustInterDirc("CREATE TABLE SEQUENCE_VALUE_ITEM(SEQ_NAME varchar(60) NOT NULL, SEQ_ID decimal(18,0) DEFAULT NULL, " +
		"PRIMARY KEY (SEQ_NAME))")
	tk.MustInterDirc("create causet t1(c1 int, c2 int, unique key(c1))")
	tk.MustInterDirc(`insert into sequence_value_item values("OSCurrentStep", 0)`)
	tk.MustInterDirc("insert into t1 values(1, 1)")
	tk.MustInterDirc("insert into t1 values(2, 2)")

	// tk2 uses RC isolation level
	tk2.MustInterDirc("set @@tx_isolation='READ-COMMITTED'")
	tk2.MustInterDirc("set autocommit = 0")

	// test point get
	tk2.MustInterDirc("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UFIDelATE")
	tk2.MustInterDirc("UFIDelATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'")
	tk2.MustInterDirc("rollback")
	tk2.MustQuery("select * from t1 where c1 = 1 for uFIDelate").Check(testkit.Rows("1 1"))
	tk2.MustInterDirc("uFIDelate t1 set c2 = c2 + 10 where c1 = 1")
	tk2.MustQuery("select * from t1 where c1 in (1, 2) for uFIDelate").Check(testkit.Rows("1 11", "2 2"))
	tk2.MustInterDirc("uFIDelate t1 set c2 = c2 + 10 where c1 in (2)")
	tk2.MustQuery("select * from t1 where c1 in (1, 2) for uFIDelate").Check(testkit.Rows("1 11", "2 12"))
	tk2.MustInterDirc("commit")

	// tk3 uses begin with RC isolation level
	tk3.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 0"))
	tk3.MustInterDirc("set @@tx_isolation='READ-COMMITTED'")
	tk3.MustInterDirc("begin")
	tk3.MustInterDirc("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME='OSCurrentStep' FOR UFIDelATE")
	tk3.MustInterDirc("UFIDelATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME='OSCurrentStep'")
	tk3.MustQuery("select * from t1 where c1 = 1 for uFIDelate").Check(testkit.Rows("1 11"))
	tk3.MustInterDirc("uFIDelate t1 set c2 = c2 + 10 where c1 = 1")
	tk3.MustQuery("select * from t1 where c1 in (1, 2) for uFIDelate").Check(testkit.Rows("1 21", "2 12"))
	tk3.MustInterDirc("uFIDelate t1 set c2 = c2 + 10 where c1 in (2)")
	tk3.MustQuery("select * from t1 where c1 in (1, 2) for uFIDelate").Check(testkit.Rows("1 21", "2 22"))
	tk3.MustInterDirc("commit")

	// verify
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 100"))
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM where SEQ_ID = 100").Check(testkit.Rows("OSCurrentStep 100"))
	tk.MustQuery("select * from t1 where c1 = 2").Check(testkit.Rows("2 22"))
	tk.MustQuery("select * from t1 where c1 in (1, 2, 3)").Check(testkit.Rows("1 21", "2 22"))

	// test batch point get
	tk2.MustInterDirc("set autocommit = 1")
	tk2.MustInterDirc("set autocommit = 0")
	tk2.MustInterDirc("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UFIDelATE")
	tk2.MustInterDirc("UFIDelATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')")
	tk2.MustQuery("select * from t1 where c1 in (1, 2, 3, 4, 5) for uFIDelate").Check(testkit.Rows("1 21", "2 22"))
	tk2.MustInterDirc("uFIDelate t1 set c2 = c2 + 10 where c1 in (1, 2, 3, 4, 5)")
	tk2.MustQuery("select * from t1 where c1 in (1, 2, 3, 4, 5) for uFIDelate").Check(testkit.Rows("1 31", "2 32"))
	tk2.MustInterDirc("commit")
	tk2.MustInterDirc("SELECT SEQ_ID FROM SEQUENCE_VALUE_ITEM WHERE SEQ_NAME in ('OSCurrentStep') FOR UFIDelATE")
	tk2.MustInterDirc("UFIDelATE SEQUENCE_VALUE_ITEM SET SEQ_ID=SEQ_ID+100 WHERE SEQ_NAME in ('OSCurrentStep')")
	tk2.MustInterDirc("rollback")

	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM").Check(testkit.Rows("OSCurrentStep 200"))
	tk.MustQuery("select * from SEQUENCE_VALUE_ITEM where SEQ_NAME in ('OSCurrentStep')").Check(testkit.Rows("OSCurrentStep 200"))
	tk.MustQuery("select * from t1 where c1 in (1, 2, 3)").Check(testkit.Rows("1 31", "2 32"))
	tk.MustInterDirc("rollback")
	tk2.MustInterDirc("rollback")
	tk3.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestPointGetWithDeleteInMem(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists uk")
	tk.MustInterDirc("create causet uk (c1 int primary key, c2 int, unique key uk(c2))")
	tk.MustInterDirc("insert uk values (1, 77), (2, 88), (3, 99)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("delete from uk where c1 = 1")
	tk.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows())
	tk.MustQuery("select * from uk where c2 in(77, 88, 99)").Check(testkit.Rows("2 88", "3 99"))
	tk.MustQuery("select * from uk").Check(testkit.Rows("2 88", "3 99"))
	tk.MustQuery("select * from uk where c2 = 77 for uFIDelate").Check(testkit.Rows())
	tk.MustQuery("select * from uk where c2 in(77, 88, 99) for uFIDelate").Check(testkit.Rows("2 88", "3 99"))
	tk.MustInterDirc("rollback")
	tk2.MustQuery("select * from uk where c1 = 1").Check(testkit.Rows("1 77"))
	tk.MustInterDirc("begin")
	tk.MustInterDirc("uFIDelate uk set c1 = 10 where c1 = 1")
	tk.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows("10 77"))
	tk.MustQuery("select * from uk where c2 in(77, 88, 99)").Check(testkit.Rows("10 77", "2 88", "3 99"))
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from uk where c1 = 1").Check(testkit.Rows())
	tk2.MustQuery("select * from uk where c2 = 77").Check(testkit.Rows("10 77"))
	tk2.MustQuery("select * from uk where c1 = 10").Check(testkit.Rows("10 77"))
	tk.MustInterDirc("drop causet if exists uk")
}

func (s *testPessimisticSuite) TestPessimisticTxnWithDBSAddDropDeferredCauset(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert t1 values (1, 77), (2, 88)")
	tk.MustInterDirc("alter causet t1 add index k2(c2)")
	tk.MustInterDirc("alter causet t1 drop index k2")

	// tk2 starts a pessimistic transaction and make some changes on causet t1.
	// tk executes some dbs memexs add/drop column on causet t1.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate t1 set c2 = c1 * 10")
	tk2.MustInterDirc("alter causet t1 add column c3 int after c1")
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 <nil> 10", "2 <nil> 20"))

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into t1 values(5, 5, 5)")
	tk2.MustInterDirc("alter causet t1 drop column c3")
	tk2.MustInterDirc("alter causet t1 drop column c2")
	tk.MustInterDirc("commit")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1", "2", "5"))
}
func (s *testPessimisticSuite) TestPessimisticTxnWithDBSChangeDeferredCauset(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_db")
	tk.MustInterDirc("create database test_db")
	tk.MustInterDirc("use test_db")
	tk2.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1 (c1 int primary key, c2 int, c3 varchar(10))")
	tk.MustInterDirc("insert t1 values (1, 77, 'a'), (2, 88, 'b')")

	// Extend column field length is accepblock.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate t1 set c2 = c1 * 10")
	tk2.MustInterDirc("alter causet t1 modify column c2 bigint")
	tk.MustInterDirc("commit")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("uFIDelate t1 set c3 = 'aba'")
	tk2.MustInterDirc("alter causet t1 modify column c3 varchar(30)")
	tk.MustInterDirc("commit")
	tk2.MustInterDirc("admin check causet t1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 10 aba", "2 20 aba"))

	// Change column from nullable to not null is not allowed by now.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into t1(c1) values(100)")
	tk2.MustInterDirc("alter causet t1 change column c2 cc2 bigint not null")
	err := tk.InterDircToErr("commit")
	c.Assert(err, NotNil)

	// Change default value is rejected.
	tk2.MustInterDirc("create causet ta(a bigint primary key auto_random(3), b varchar(255) default 'old');")
	tk2.MustInterDirc("insert into ta(b) values('a')")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into ta values()")
	tk2.MustInterDirc("alter causet ta modify column b varchar(300) default 'new';")
	err = tk.InterDircToErr("commit")
	c.Assert(err, NotNil)
	tk2.MustQuery("select b from ta").Check(testkit.Rows("a"))

	// Change default value with add index. There is a new MultipleKeyFlag flag on the index key, and the column is changed,
	// the flag check will fail.
	tk2.MustInterDirc("insert into ta values()")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into ta(b) values('inserted_value')")
	tk.MustInterDirc("insert into ta values()")
	tk.MustInterDirc("insert into ta values()")
	tk2.MustInterDirc("alter causet ta add index i1(b)")
	tk2.MustInterDirc("alter causet ta change column b b varchar(301) default 'newest'")
	tk2.MustInterDirc("alter causet ta modify column b varchar(301) default 'new'")
	c.Assert(tk.InterDircToErr("commit"), NotNil)
	tk2.MustInterDirc("admin check causet ta")
	tk2.MustQuery("select count(b) from ta use index(i1) where b = 'new'").Check(testkit.Rows("1"))

	// Change default value to now().
	tk2.MustInterDirc("create causet tbl_time(c1 int, c_time timestamp)")
	tk2.MustInterDirc("insert into tbl_time(c1) values(1)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into tbl_time(c1) values(2)")
	tk2.MustInterDirc("alter causet tbl_time modify column c_time timestamp default now()")
	tk2.MustInterDirc("insert into tbl_time(c1) values(3)")
	tk2.MustInterDirc("insert into tbl_time(c1) values(4)")
	c.Assert(tk.InterDircToErr("commit"), NotNil)
	tk2.MustQuery("select count(1) from tbl_time where c_time is not null").Check(testkit.Rows("2"))

	tk2.MustInterDirc("drop database if exists test_db")
}

func (s *testPessimisticSuite) TestPessimisticUnionForUFIDelate(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(id int, v int, k int, primary key (id), key kk(k))")
	tk.MustInterDirc("insert into t select 1, 1, 1")
	tk.MustInterDirc("begin pessimistic")
	tk.MustQuery("(select * from t where id between 0 and 1 for uFIDelate) union all (select * from t where id between 0 and 1 for uFIDelate)")
	tk.MustInterDirc("uFIDelate t set k = 2 where k = 1")
	tk.MustInterDirc("commit")
	tk.MustInterDirc("admin check causet t")
}

func (s *testPessimisticSuite) TestInsertDupKeyAfterLock(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_db")
	tk.MustInterDirc("create database test_db")
	tk.MustInterDirc("use test_db")
	tk2.MustInterDirc("use test_db")
	tk2.MustInterDirc("drop causet if exists t1")
	tk2.MustInterDirc("create causet t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustInterDirc("insert into t1 values(1, 2, 3);")
	tk2.MustInterDirc("insert into t1 values(10, 20, 30);")

	// Test insert after dagger.
	tk.MustInterDirc("begin pessimistic")
	err := tk.InterDircToErr("uFIDelate t1 set c2 = 20 where c1 = 1;")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from t1 for uFIDelate")
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from t1 where c2 = 2 for uFIDelate")
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	// Test insert after insert.
	tk.MustInterDirc("begin pessimistic")
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("insert into t1 values(5, 6, 7)")
	err = tk.InterDircToErr("insert into t1 values(6, 6, 7);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 30"))

	// Test insert after delete.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("delete from t1 where c2 > 2")
	tk.MustInterDirc("insert into t1 values(10, 20, 500);")
	err = tk.InterDircToErr("insert into t1 values(20, 20, 30);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(1, 20, 30);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test range.
	tk.MustInterDirc("begin pessimistic")
	err = tk.InterDircToErr("uFIDelate t1 set c2 = 20 where c1 >= 1 and c1 < 5;")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("uFIDelate t1 set c2 = 20 where c1 >= 1 and c1 < 50;")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test select for uFIDelate after dml.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into t1 values(5, 6, 7)")
	tk.MustInterDirc("select * from t1 where c1 = 5 for uFIDelate")
	tk.MustInterDirc("select * from t1 where c1 = 6 for uFIDelate")
	tk.MustInterDirc("select * from t1 for uFIDelate")
	err = tk.InterDircToErr("insert into t1 values(7, 6, 7)")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(5, 8, 6)")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("select * from t1 where c1 = 5 for uFIDelate")
	tk.MustInterDirc("select * from t1 where c2 = 8 for uFIDelate")
	tk.MustInterDirc("select * from t1 for uFIDelate")
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 500"))

	// Test optimistic for uFIDelate.
	tk.MustInterDirc("begin optimistic")
	tk.MustQuery("select * from t1 where c1 = 1 for uFIDelate").Check(testkit.Rows("1 2 3"))
	tk.MustInterDirc("insert into t1 values(10, 10, 10)")
	err = tk.InterDircToErr("commit")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
}

func (s *testPessimisticSuite) TestInsertDupKeyAfterLockBatchPointGet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_db")
	tk.MustInterDirc("create database test_db")
	tk.MustInterDirc("use test_db")
	tk2.MustInterDirc("use test_db")
	tk2.MustInterDirc("drop causet if exists t1")
	tk2.MustInterDirc("create causet t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustInterDirc("insert into t1 values(1, 2, 3);")
	tk2.MustInterDirc("insert into t1 values(10, 20, 30);")

	// Test insert after dagger.
	tk.MustInterDirc("begin pessimistic")
	err := tk.InterDircToErr("uFIDelate t1 set c2 = 20 where c1 in (1);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from t1 for uFIDelate")
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("select * from t1 where c2 in (2) for uFIDelate")
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 30"))

	// Test insert after insert.
	tk.MustInterDirc("begin pessimistic")
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("insert into t1 values(5, 6, 7)")
	err = tk.InterDircToErr("insert into t1 values(6, 6, 7);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 30"))

	// Test insert after delete.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("delete from t1 where c2 > 2")
	tk.MustInterDirc("insert into t1 values(10, 20, 500);")
	err = tk.InterDircToErr("insert into t1 values(20, 20, 30);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(1, 20, 30);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test range.
	tk.MustInterDirc("begin pessimistic")
	err = tk.InterDircToErr("uFIDelate t1 set c2 = 20 where c1 >= 1 and c1 < 5;")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("uFIDelate t1 set c2 = 20 where c1 >= 1 and c1 < 50;")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(1, 15, 300);")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "10 20 500"))

	// Test select for uFIDelate after dml.
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into t1 values(5, 6, 7)")
	tk.MustInterDirc("select * from t1 where c1 in (5, 6) for uFIDelate")
	tk.MustInterDirc("select * from t1 where c1 = 6 for uFIDelate")
	tk.MustInterDirc("select * from t1 for uFIDelate")
	err = tk.InterDircToErr("insert into t1 values(7, 6, 7)")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	err = tk.InterDircToErr("insert into t1 values(5, 8, 6)")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
	tk.MustInterDirc("select * from t1 where c2 = 8 for uFIDelate")
	tk.MustInterDirc("select * from t1 where c1 in (5, 8) for uFIDelate")
	tk.MustInterDirc("select * from t1 for uFIDelate")
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 2 3", "5 6 7", "10 20 500"))

	// Test optimistic for uFIDelate.
	tk.MustInterDirc("begin optimistic")
	tk.MustQuery("select * from t1 where c1 in (1) for uFIDelate").Check(testkit.Rows("1 2 3"))
	tk.MustInterDirc("insert into t1 values(10, 10, 10)")
	err = tk.InterDircToErr("commit")
	c.Assert(terror.ErrorEqual(err, ekv.ErrKeyExists), IsTrue)
}

func (s *testPessimisticSuite) TestAmendTxnVariable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_db")
	tk.MustInterDirc("create database test_db")
	tk.MustInterDirc("use test_db")
	tk2.MustInterDirc("use test_db")
	tk2.MustInterDirc("drop causet if exists t1")
	tk2.MustInterDirc("create causet t1(c1 int primary key, c2 int, c3 int, unique key uk(c2));")
	tk2.MustInterDirc("insert into t1 values(1, 1, 1);")
	tk2.MustInterDirc("insert into t1 values(2, 2, 2);")
	tk3.MustInterDirc("use test_db")

	// Set off the stochastik variable.
	tk3.MustInterDirc("set milevadb_enable_amend_pessimistic_txn = 0;")
	tk3.MustInterDirc("begin pessimistic")
	tk3.MustInterDirc("insert into t1 values(3, 3, 3)")
	tk.MustInterDirc("begin pessimistic")
	tk.MustInterDirc("insert into t1 values(4, 4, 4)")
	tk2.MustInterDirc("alter causet t1 add column new_col int")
	err := tk3.InterDircToErr("commit")
	c.Assert(err, NotNil)
	tk.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>"))

	// Set off the global variable.
	tk2.MustInterDirc("set global milevadb_enable_amend_pessimistic_txn = 0;")
	tk4 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk4.MustQuery(`show variables like "milevadb_enable_amend_pessimistic_txn"`).Check(testkit.Rows("milevadb_enable_amend_pessimistic_txn 0"))
	tk4.MustInterDirc("use test_db")
	tk4.MustInterDirc("begin pessimistic")
	tk4.MustInterDirc("insert into t1 values(5, 5, 5, 5)")
	tk2.MustInterDirc("alter causet t1 drop column new_col")
	err = tk4.InterDircToErr("commit")
	c.Assert(err, NotNil)
	tk4.MustInterDirc("set milevadb_enable_amend_pessimistic_txn = 1;")
	tk4.MustInterDirc("begin pessimistic")
	tk4.MustInterDirc("insert into t1 values(5, 5, 5)")
	tk2.MustInterDirc("alter causet t1 add column new_col2 int")
	tk4.MustInterDirc("commit")
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 1 1 <nil>", "2 2 2 <nil>", "4 4 4 <nil>", "5 5 5 <nil>"))

	// Restore.
	tk2.MustInterDirc("set global milevadb_enable_amend_pessimistic_txn = 1;")
}

func (s *testPessimisticSuite) TestSelectForUFIDelateWaitSeconds(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("create causet tk (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert into tk values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk4 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk5 := testkit.NewTestKitWithInit(c, s.causetstore)

	// tk2 dagger c1 = 5
	tk2.MustInterDirc("begin pessimistic")
	tk3.MustInterDirc("begin pessimistic")
	tk4.MustInterDirc("begin pessimistic")
	tk5.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("select * from tk where c1 = 5 or c1 = 1 for uFIDelate")
	start := time.Now()
	errCh := make(chan error, 3)
	go func() {
		// tk3 try dagger c1 = 1 timeout 1sec, the default innodb_lock_wait_timeout value is 50s.
		err := tk3.InterDircToErr("select * from tk where c1 = 1 for uFIDelate wait 1")
		errCh <- err
	}()
	go func() {
		// Lock use selectLockInterDirc.
		err := tk4.InterDircToErr("select * from tk where c1 >= 1 for uFIDelate wait 1")
		errCh <- err
	}()
	go func() {
		// Lock use batchPointGetInterDirc.
		err := tk5.InterDircToErr("select c2 from tk where c1 in (1, 5) for uFIDelate wait 1")
		errCh <- err
	}()
	waitErr := <-errCh
	waitErr2 := <-errCh
	waitErr3 := <-errCh
	c.Assert(waitErr, NotNil)
	c.Check(waitErr.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())
	c.Assert(waitErr2, NotNil)
	c.Check(waitErr2.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())
	c.Assert(waitErr3, NotNil)
	c.Check(waitErr3.Error(), Equals, einsteindb.ErrLockWaitTimeout.Error())
	c.Assert(time.Since(start).Seconds(), Less, 45.0)
	tk2.MustInterDirc("commit")
	tk3.MustInterDirc("rollback")
	tk4.MustInterDirc("rollback")
	tk5.MustInterDirc("rollback")
}

func (s *testPessimisticSuite) TestSelectForUFIDelateConflictRetry(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EinsteinDBClient.EnableAsyncCommit = true
	})

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustInterDirc("drop causet if exists tk")
	tk.MustInterDirc("create causet tk (c1 int primary key, c2 int)")
	tk.MustInterDirc("insert into tk values(1,1),(2,2)")
	tk2 := testkit.NewTestKitWithInit(c, s.causetstore)
	tk3 := testkit.NewTestKitWithInit(c, s.causetstore)

	tk2.MustInterDirc("begin pessimistic")
	tk3.MustInterDirc("begin pessimistic")
	tk2.MustInterDirc("uFIDelate tk set c2 = c2 + 1 where c1 = 1")
	tk3.MustInterDirc("uFIDelate tk set c2 = c2 + 1 where c2 = 2")
	tsCh := make(chan uint64)
	go func() {
		tk3.MustInterDirc("uFIDelate tk set c2 = c2 + 1 where c1 = 1")
		lastTS, err := s.causetstore.GetOracle().GetLowResolutionTimestamp(context.Background())
		c.Assert(err, IsNil)
		tsCh <- lastTS
		tk3.MustInterDirc("commit")
	}()
	// tk2LastTS should be its forUFIDelateTS
	tk2LastTS, err := s.causetstore.GetOracle().GetLowResolutionTimestamp(context.Background())
	c.Assert(err, IsNil)
	tk2.MustInterDirc("commit")

	tk3LastTs := <-tsCh
	// it must get a new ts on pessimistic write conflict so the latest timestamp
	// should increase
	c.Assert(tk3LastTs, Greater, tk2LastTS)
}
