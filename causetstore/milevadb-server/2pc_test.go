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

package einsteindb

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
)

type testCommitterSuite struct {
	OneByOneSuite
	cluster     cluster.Cluster
	causetstore *einsteindbStore
}

var _ = SerialSuites(&testCommitterSuite{})

func (s *testCommitterSuite) SetUpSuite(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 3000) // 3s
	s.OneByOneSuite.SetUpSuite(c)
	atomic.StoreUint64(&CommitMaxBackoff, 1000)
}

func (s *testCommitterSuite) SetUpTest(c *C) {
	mvccStore, err := mockeinsteindb.NewMVCCLevelDB("")
	c.Assert(err, IsNil)
	cluster := mockeinsteindb.NewCluster(mvccStore)
	mockeinsteindb.BootstrapWithMultiRegions(cluster, []byte("a"), []byte("b"), []byte("c"))
	s.cluster = cluster
	client := mockeinsteindb.NewRPCClient(cluster, mvccStore)
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(cluster)}
	spekv := NewMockSafePointKV()
	causetstore, err := newEinsteinDBStore("mockeinsteindb-causetstore", FIDelCli, spekv, client, false, nil)
	causetstore.EnableTxnLocalLatches(1024000)
	c.Assert(err, IsNil)

	// TODO: make it possible
	// causetstore, err := mockstore.NewMockStore(
	// 	mockstore.WithStoreType(mockstore.MockEinsteinDB),
	// 	mockstore.WithClusterInspector(func(c cluster.Cluster) {
	// 		mockstore.BootstrapWithMultiRegions(c, []byte("a"), []byte("b"), []byte("c"))
	// 		s.cluster = c
	// 	}),
	// 	mockstore.WithFIDelClientHijacker(func(c fidel.Client) fidel.Client {
	// 		return &codecFIDelClient{c}
	// 	}),
	// 	mockstore.WithTxnLocalLatches(1024000),
	// )
	// c.Assert(err, IsNil)

	s.causetstore = causetstore
}

func (s *testCommitterSuite) TearDownSuite(c *C) {
	atomic.StoreUint64(&CommitMaxBackoff, 20000)
	s.causetstore.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testCommitterSuite) begin(c *C) *einsteindbTxn {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	return txn.(*einsteindbTxn)
}

func (s *testCommitterSuite) checkValues(c *C, m map[string]string) {
	txn := s.begin(c)
	for k, v := range m {
		val, err := txn.Get(context.TODO(), []byte(k))
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, v)
	}
}

func (s *testCommitterSuite) mustCommit(c *C, m map[string]string) {
	txn := s.begin(c)
	for k, v := range m {
		err := txn.Set([]byte(k), []byte(v))
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	s.checkValues(c, m)
}

func randKV(keyLen, valLen int) (string, string) {
	const letters = "abc"
	k, v := make([]byte, keyLen), make([]byte, valLen)
	for i := range k {
		k[i] = letters[rand.Intn(len(letters))]
	}
	for i := range v {
		v[i] = letters[rand.Intn(len(letters))]
	}
	return string(k), string(v)
}

func (s *testCommitterSuite) TestDeleteYourWritesTTL(c *C) {
	conf := *config.GetGlobalConfig()
	oldConf := conf
	defer config.StoreGlobalConfig(&oldConf)
	conf.EinsteinDBClient.TTLRefreshedTxnSize = 0
	config.StoreGlobalConfig(&conf)
	bo := NewBackofferWithVars(context.Background(), getMaxBackoff, nil)

	{
		txn := s.begin(c)
		err := txn.GetMemBuffer().SetWithFlags(ekv.Key("bb"), []byte{0}, ekv.SetPresumeKeyNotExists)
		c.Assert(err, IsNil)
		err = txn.Set(ekv.Key("ba"), []byte{1})
		c.Assert(err, IsNil)
		err = txn.Delete(ekv.Key("bb"))
		c.Assert(err, IsNil)
		committer, err := newTwoPhaseCommitterWithInit(txn, 0)
		c.Assert(err, IsNil)
		err = committer.prewriteMutations(bo, committer.mutations)
		c.Assert(err, IsNil)
		state := atomic.LoadUint32((*uint32)(&committer.ttlManager.state))
		c.Check(state, Equals, uint32(stateRunning))
	}

	{
		txn := s.begin(c)
		err := txn.GetMemBuffer().SetWithFlags(ekv.Key("dd"), []byte{0}, ekv.SetPresumeKeyNotExists)
		c.Assert(err, IsNil)
		err = txn.Set(ekv.Key("de"), []byte{1})
		c.Assert(err, IsNil)
		err = txn.Delete(ekv.Key("dd"))
		c.Assert(err, IsNil)
		committer, err := newTwoPhaseCommitterWithInit(txn, 0)
		c.Assert(err, IsNil)
		err = committer.prewriteMutations(bo, committer.mutations)
		c.Assert(err, IsNil)
		state := atomic.LoadUint32((*uint32)(&committer.ttlManager.state))
		c.Check(state, Equals, uint32(stateRunning))
	}
}

func (s *testCommitterSuite) TestCommitRollback(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a",
		"b": "b",
		"c": "c",
	})

	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a1"))
	txn.Set([]byte("b"), []byte("b1"))
	txn.Set([]byte("c"), []byte("c1"))

	s.mustCommit(c, map[string]string{
		"c": "c2",
	})

	err := txn.Commit(context.Background())
	c.Assert(err, NotNil)

	s.checkValues(c, map[string]string{
		"a": "a",
		"b": "b",
		"c": "c2",
	})
}

func (s *testCommitterSuite) TestPrewriteRollback(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a0",
		"b": "b0",
	})
	ctx := context.Background()
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn1, 0)
	c.Assert(err, IsNil)
	err = committer.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), committer.mutations)
	c.Assert(err, IsNil)

	txn2 := s.begin(c)
	v, err := txn2.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a0"))

	err = committer.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), committer.mutations)
	if err != nil {
		// Retry.
		txn1 = s.begin(c)
		err = txn1.Set([]byte("a"), []byte("a1"))
		c.Assert(err, IsNil)
		err = txn1.Set([]byte("b"), []byte("b1"))
		c.Assert(err, IsNil)
		committer, err = newTwoPhaseCommitterWithInit(txn1, 0)
		c.Assert(err, IsNil)
		err = committer.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), committer.mutations)
		c.Assert(err, IsNil)
	}
	committer.commitTS, err = s.causetstore.oracle.GetTimestamp(ctx)
	c.Assert(err, IsNil)
	err = committer.commitMutations(NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), CommitterMutations{keys: [][]byte{[]byte("a")}})
	c.Assert(err, IsNil)

	txn3 := s.begin(c)
	v, err = txn3.Get(context.TODO(), []byte("b"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("b1"))
}

func (s *testCommitterSuite) TestContextCancel(c *C) {
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn1, 0)
	c.Assert(err, IsNil)

	bo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil)
	backoffer, cancel := bo.Fork()
	cancel() // cancel the context
	err = committer.prewriteMutations(backoffer, committer.mutations)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

func (s *testCommitterSuite) TestContextCancel2(c *C) {
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b"))
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	cancel()
	// Secondary keys should not be canceled.
	time.Sleep(time.Millisecond * 20)
	c.Assert(s.isKeyLocked(c, []byte("b")), IsFalse)
}

func (s *testCommitterSuite) TestContextCancelRetryable(c *C) {
	txn1, txn2, txn3 := s.begin(c), s.begin(c), s.begin(c)
	// txn1 locks "b"
	err := txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn1, 0)
	c.Assert(err, IsNil)
	err = committer.prewriteMutations(NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil), committer.mutations)
	c.Assert(err, IsNil)
	// txn3 writes "c"
	err = txn3.Set([]byte("c"), []byte("c3"))
	c.Assert(err, IsNil)
	err = txn3.Commit(context.Background())
	c.Assert(err, IsNil)
	// txn2 writes "a"(PK), "b", "c" on different regions.
	// "c" will return a retryable error.
	// "b" will get a Locked error first, then the context must be canceled after backoff for dagger.
	err = txn2.Set([]byte("a"), []byte("a2"))
	c.Assert(err, IsNil)
	err = txn2.Set([]byte("b"), []byte("b2"))
	c.Assert(err, IsNil)
	err = txn2.Set([]byte("c"), []byte("c2"))
	c.Assert(err, IsNil)
	err = txn2.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(ekv.ErrWriteConflictInMilevaDB.Equal(err), IsTrue, Commentf("err: %s", err))
}

func (s *testCommitterSuite) mustGetRegionID(c *C, key []byte) uint64 {
	loc, err := s.causetstore.regionCache.LocateKey(NewBackofferWithVars(context.Background(), getMaxBackoff, nil), key)
	c.Assert(err, IsNil)
	return loc.Region.id
}

func (s *testCommitterSuite) isKeyLocked(c *C, key []byte) bool {
	ver, err := s.causetstore.CurrentVersion()
	c.Assert(err, IsNil)
	bo := NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdGet, &ekvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	loc, err := s.causetstore.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.causetstore.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := (resp.Resp.(*ekvrpcpb.GetResponse)).GetError()
	return keyErr.GetLocked() != nil
}

func (s *testCommitterSuite) TestPrewriteCancel(c *C) {
	// Setup region delays for key "b" and "c".
	delays := map[uint64]time.Duration{
		s.mustGetRegionID(c, []byte("b")): time.Millisecond * 10,
		s.mustGetRegionID(c, []byte("c")): time.Millisecond * 20,
	}
	s.causetstore.client = &slowClient{
		Client:       s.causetstore.client,
		regionDelays: delays,
	}

	txn1, txn2 := s.begin(c), s.begin(c)
	// txn2 writes "b"
	err := txn2.Set([]byte("b"), []byte("b2"))
	c.Assert(err, IsNil)
	err = txn2.Commit(context.Background())
	c.Assert(err, IsNil)
	// txn1 writes "a"(PK), "b", "c" on different regions.
	// "b" will return an error and cancel commit.
	err = txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = txn1.Commit(context.Background())
	c.Assert(err, NotNil)
	// "c" should be cleaned up in reasonable time.
	for i := 0; i < 50; i++ {
		if !s.isKeyLocked(c, []byte("c")) {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.Fail()
}

// slowClient wraps rpcClient and makes some regions respond with delay.
type slowClient struct {
	Client
	regionDelays map[uint64]time.Duration
}

func (c *slowClient) SendReq(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	for id, delay := range c.regionDelays {
		reqCtx := &req.Context
		if reqCtx.GetRegionId() == id {
			time.Sleep(delay)
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (s *testCommitterSuite) TestIllegalTso(c *C) {
	txn := s.begin(c)
	data := map[string]string{
		"name": "aa",
		"age":  "12",
	}
	for k, v := range data {
		err := txn.Set([]byte(k), []byte(v))
		c.Assert(err, IsNil)
	}
	// make start ts bigger.
	txn.startTS = uint64(math.MaxUint64)
	err := txn.Commit(context.Background())
	c.Assert(err, NotNil)
	errMsgMustContain(c, err, "invalid txnStartTS")
}

func errMsgMustContain(c *C, err error, msg string) {
	c.Assert(strings.Contains(err.Error(), msg), IsTrue)
}

func newTwoPhaseCommitterWithInit(txn *einsteindbTxn, connID uint64) (*twoPhaseCommitter, error) {
	c, err := newTwoPhaseCommitter(txn, connID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = c.initKeysAndMutations(); err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

func (s *testCommitterSuite) TestCommitBeforePrewrite(c *C) {
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	ctx := context.Background()
	err = committer.cleanupMutations(NewBackofferWithVars(ctx, cleanupMaxBackoff, nil), committer.mutations)
	c.Assert(err, IsNil)
	err = committer.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), committer.mutations)
	c.Assert(err, NotNil)
	errMsgMustContain(c, err, "already rolled back")
}

func (s *testCommitterSuite) TestPrewritePrimaryKeyFailed(c *C) {
	// commit (a,a1)
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Commit(context.Background())
	c.Assert(err, IsNil)

	// check a
	txn := s.begin(c)
	v, err := txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a1"))

	// set txn2's startTs before txn1's
	txn2 := s.begin(c)
	txn2.startTS = txn1.startTS - 1
	err = txn2.Set([]byte("a"), []byte("a2"))
	c.Assert(err, IsNil)
	err = txn2.Set([]byte("b"), []byte("b2"))
	c.Assert(err, IsNil)
	// prewrite:primary a failed, b success
	err = txn2.Commit(context.Background())
	c.Assert(err, NotNil)

	// txn2 failed with a rollback for record a.
	txn = s.begin(c)
	v, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a1"))
	_, err = txn.Get(context.TODO(), []byte("b"))
	errMsgMustContain(c, err, "key not exist")

	// clean again, shouldn't be failed when a rollback already exist.
	ctx := context.Background()
	committer, err := newTwoPhaseCommitterWithInit(txn2, 0)
	c.Assert(err, IsNil)
	err = committer.cleanupMutations(NewBackofferWithVars(ctx, cleanupMaxBackoff, nil), committer.mutations)
	c.Assert(err, IsNil)

	// check the data after rollback twice.
	txn = s.begin(c)
	v, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a1"))

	// uFIDelate data in a new txn, should be success.
	err = txn.Set([]byte("a"), []byte("a3"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	// check value
	txn = s.begin(c)
	v, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a3"))
}

func (s *testCommitterSuite) TestWrittenKeysOnConflict(c *C) {
	// This test checks that when there is a write conflict, written keys is collected,
	// so we can use it to clean up keys.
	region, _ := s.cluster.GetRegionByKey([]byte("x"))
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte("y"), []uint64{newPeerID}, newPeerID)
	var totalTime time.Duration
	for i := 0; i < 10; i++ {
		txn1 := s.begin(c)
		txn2 := s.begin(c)
		txn2.Set([]byte("x1"), []byte("1"))
		committer2, err := newTwoPhaseCommitterWithInit(txn2, 2)
		c.Assert(err, IsNil)
		err = committer2.execute(context.Background())
		c.Assert(err, IsNil)
		txn1.Set([]byte("x1"), []byte("1"))
		txn1.Set([]byte("y1"), []byte("2"))
		committer1, err := newTwoPhaseCommitterWithInit(txn1, 2)
		c.Assert(err, IsNil)
		err = committer1.execute(context.Background())
		c.Assert(err, NotNil)
		committer1.cleanWg.Wait()
		txn3 := s.begin(c)
		start := time.Now()
		txn3.Get(context.TODO(), []byte("y1"))
		totalTime += time.Since(start)
		txn3.Commit(context.Background())
	}
	c.Assert(totalTime, Less, time.Millisecond*200)
}

func (s *testCommitterSuite) TestPrewriteTxnSize(c *C) {
	// Prepare two regions first: (, 100) and [100, )
	region, _ := s.cluster.GetRegionByKey([]byte{50})
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte{100}, []uint64{newPeerID}, newPeerID)

	txn := s.begin(c)
	var val [1024]byte
	for i := byte(50); i < 120; i++ {
		err := txn.Set([]byte{i}, val[:])
		c.Assert(err, IsNil)
	}

	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = committer.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), committer.mutations)
	c.Assert(err, IsNil)

	// Check the written locks in the first region (50 keys)
	for i := byte(50); i < 100; i++ {
		dagger := s.getLockInfo(c, []byte{i})
		c.Assert(int(dagger.TxnSize), Equals, 50)
	}

	// Check the written locks in the second region (20 keys)
	for i := byte(100); i < 120; i++ {
		dagger := s.getLockInfo(c, []byte{i})
		c.Assert(int(dagger.TxnSize), Equals, 20)
	}
}

func (s *testCommitterSuite) TestRejectCommitTS(c *C) {
	txn := s.begin(c)
	c.Assert(txn.Set([]byte("x"), []byte("v")), IsNil)

	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)
	bo := NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	loc, err := s.causetstore.regionCache.LocateKey(bo, []byte("x"))
	c.Assert(err, IsNil)
	mutations := []*ekvrpcpb.Mutation{
		{
			Op:    committer.mutations.ops[0],
			Key:   committer.mutations.keys[0],
			Value: committer.mutations.values[0],
		},
	}
	prewrite := &ekvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  committer.primary(),
		StartVersion: committer.startTS,
		LockTtl:      committer.lockTTL,
		MinCommitTs:  committer.startTS + 100, // Set minCommitTS
	}
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPrewrite, prewrite)
	_, err = s.causetstore.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)

	// Make commitTS less than minCommitTS.
	committer.commitTS = committer.startTS + 1
	// Ensure that the new commit ts is greater than minCommitTS when retry
	time.Sleep(3 * time.Millisecond)
	err = committer.commitMutations(bo, committer.mutations)
	c.Assert(err, IsNil)

	// Use startTS+2 to read the data and get nothing.
	// Use max.Uint64 to read the data and success.
	// That means the final commitTS > startTS+2, it's not the one we provide.
	// So we cover the rety commitTS logic.
	txn1, err := s.causetstore.BeginWithStartTS(committer.startTS + 2)
	c.Assert(err, IsNil)
	_, err = txn1.Get(bo.ctx, []byte("x"))
	c.Assert(ekv.IsErrNotFound(err), IsTrue)

	txn2, err := s.causetstore.BeginWithStartTS(math.MaxUint64)
	c.Assert(err, IsNil)
	val, err := txn2.Get(bo.ctx, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(val, []byte("v")), IsTrue)
}

func (s *testCommitterSuite) TestPessimisticPrewriteRequest(c *C) {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic dagger.
	txn := s.begin(c)
	txn.SetOption(ekv.Pessimistic, true)
	err := txn.Set([]byte("t1"), []byte("v1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	committer.forUFIDelateTS = 100
	var batch batchMutations
	batch.mutations = committer.mutations.subRange(0, 1)
	batch.region = RegionVerID{1, 1, 1}
	req := committer.buildPrewriteRequest(batch, 1)
	c.Assert(len(req.Prewrite().IsPessimisticLock), Greater, 0)
	c.Assert(req.Prewrite().ForUFIDelateTs, Equals, uint64(100))
}

func (s *testCommitterSuite) TestUnsetPrimaryKey(c *C) {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic dagger.
	key := ekv.Key("key")
	txn := s.begin(c)
	c.Assert(txn.Set(key, key), IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	txn = s.begin(c)
	txn.SetOption(ekv.Pessimistic, true)
	_, _ = txn.us.Get(context.TODO(), key)
	c.Assert(txn.GetMemBuffer().SetWithFlags(key, key, ekv.SetPresumeKeyNotExists), IsNil)
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn.startTS, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, NotNil)
	c.Assert(txn.Delete(key), IsNil)
	key2 := ekv.Key("key2")
	c.Assert(txn.Set(key2, key2), IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testCommitterSuite) TestPessimisticLockedKeysDedup(c *C) {
	txn := s.begin(c)
	txn.SetOption(ekv.Pessimistic, true)
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: 100, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, ekv.Key("abc"), ekv.Key("def"))
	c.Assert(err, IsNil)
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: 100, WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, ekv.Key("abc"), ekv.Key("def"))
	c.Assert(err, IsNil)
	c.Assert(txn.collectLockedKeys(), HasLen, 2)
}

func (s *testCommitterSuite) TestPessimisticTTL(c *C) {
	key := ekv.Key("key")
	txn := s.begin(c)
	txn.SetOption(ekv.Pessimistic, true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn.startTS, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 100)
	key2 := ekv.Key("key2")
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn.startTS, WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, key2)
	c.Assert(err, IsNil)
	lockInfo := s.getLockInfo(c, key)
	msBeforeLockExpired := s.causetstore.GetOracle().UntilExpired(txn.StartTS(), lockInfo.LockTtl)
	c.Assert(msBeforeLockExpired, GreaterEqual, int64(100))

	lr := newLockResolver(s.causetstore)
	bo := NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	status, err := lr.getTxnStatus(bo, txn.startTS, key2, 0, txn.startTS, true)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, GreaterEqual, lockInfo.LockTtl)

	// Check primary dagger TTL is auto increasing while the pessimistic txn is ongoing.
	for i := 0; i < 50; i++ {
		lockInfoNew := s.getLockInfo(c, key)
		if lockInfoNew.LockTtl > lockInfo.LockTtl {
			currentTS, err := lr.causetstore.GetOracle().GetTimestamp(bo.ctx)
			c.Assert(err, IsNil)
			// Check that the TTL is uFIDelate to a reasonable range.
			expire := oracle.ExtractPhysical(txn.startTS) + int64(lockInfoNew.LockTtl)
			now := oracle.ExtractPhysical(currentTS)
			c.Assert(expire > now, IsTrue)
			c.Assert(uint64(expire-now) <= atomic.LoadUint64(&ManagedLockTTL), IsTrue)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(false, IsTrue, Commentf("uFIDelate pessimistic ttl fail"))
}

func (s *testCommitterSuite) TestPessimisticLockReturnValues(c *C) {
	key := ekv.Key("key")
	key2 := ekv.Key("key2")
	txn := s.begin(c)
	c.Assert(txn.Set(key, key), IsNil)
	c.Assert(txn.Set(key2, key2), IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	txn = s.begin(c)
	txn.SetOption(ekv.Pessimistic, true)
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn.startTS, WaitStartTime: time.Now()}
	lockCtx.ReturnValues = true
	lockCtx.Values = map[string]ekv.ReturnedValue{}
	c.Assert(txn.LockKeys(context.Background(), lockCtx, key, key2), IsNil)
	c.Assert(lockCtx.Values, HasLen, 2)
	c.Assert(lockCtx.Values[string(key)].Value, BytesEquals, []byte(key))
	c.Assert(lockCtx.Values[string(key2)].Value, BytesEquals, []byte(key2))
}

// TestElapsedTTL tests that elapsed time is correct even if ts physical time is greater than local time.
func (s *testCommitterSuite) TestElapsedTTL(c *C) {
	key := ekv.Key("key")
	txn := s.begin(c)
	txn.startTS = oracle.ComposeTS(oracle.GetPhysical(time.Now().Add(time.Second*10)), 1)
	txn.SetOption(ekv.Pessimistic, true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &ekv.LockCtx{
		ForUFIDelateTS: oracle.ComposeTS(oracle.ExtractPhysical(txn.startTS)+100, 1),
		WaitStartTime:  time.Now(),
	}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, IsNil)
	lockInfo := s.getLockInfo(c, key)
	c.Assert(lockInfo.LockTtl-atomic.LoadUint64(&ManagedLockTTL), GreaterEqual, uint64(100))
	c.Assert(lockInfo.LockTtl-atomic.LoadUint64(&ManagedLockTTL), Less, uint64(150))
}

func (s *testCommitterSuite) TestDeleteYourWriteCauseGhostPrimary(c *C) {
	s.cluster.SplitKeys(ekv.Key("d"), ekv.Key("a"), 4)
	k1 := ekv.Key("a") // insert but deleted key at first pos in txn1
	k2 := ekv.Key("b") // insert key at second pos in txn1
	k3 := ekv.Key("c") // insert key in txn1 and will be conflict read by txn2

	// insert k1, k2, k3 and delete k1
	txn1 := s.begin(c)
	txn1.DelOption(ekv.Pessimistic)
	txn1.causetstore.txnLatches = nil
	txn1.Get(context.Background(), k1)
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, ekv.SetPresumeKeyNotExists)
	txn1.Set(k2, []byte{1})
	txn1.Set(k3, []byte{2})
	txn1.Delete(k1)
	committer1, err := newTwoPhaseCommitter(txn1, 0)
	c.Assert(err, IsNil)
	// setup test knob in txn's committer
	committer1.testingKnobs.acAfterCommitPrimary = make(chan struct{})
	committer1.testingKnobs.bkAfterCommitPrimary = make(chan struct{})
	txn1.committer = committer1
	var txn1Done sync.WaitGroup
	txn1Done.Add(1)
	go func() {
		err1 := txn1.Commit(context.Background())
		c.Assert(err1, IsNil)
		txn1Done.Done()
	}()
	// resume after after primary key be committed
	<-txn1.committer.testingKnobs.acAfterCommitPrimary

	// start txn2 to read k3(prewrite success and primary should be committed)
	txn2 := s.begin(c)
	txn2.DelOption(ekv.Pessimistic)
	txn2.causetstore.txnLatches = nil
	v, err := txn2.Get(context.Background(), k3)
	c.Assert(err, IsNil) // should resolve dagger and read txn1 k3 result instead of rollback it.
	c.Assert(v[0], Equals, byte(2))
	txn1.committer.testingKnobs.bkAfterCommitPrimary <- struct{}{}
	txn1Done.Wait()
}

func (s *testCommitterSuite) TestDeleteAllYourWrites(c *C) {
	s.cluster.SplitKeys(ekv.Key("d"), ekv.Key("a"), 4)
	k1 := ekv.Key("a")
	k2 := ekv.Key("b")
	k3 := ekv.Key("c")

	// insert k1, k2, k3 and delete k1, k2, k3
	txn1 := s.begin(c)
	txn1.DelOption(ekv.Pessimistic)
	txn1.causetstore.txnLatches = nil
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, ekv.SetPresumeKeyNotExists)
	txn1.Delete(k1)
	txn1.GetMemBuffer().SetWithFlags(k2, []byte{1}, ekv.SetPresumeKeyNotExists)
	txn1.Delete(k2)
	txn1.GetMemBuffer().SetWithFlags(k3, []byte{2}, ekv.SetPresumeKeyNotExists)
	txn1.Delete(k3)
	err1 := txn1.Commit(context.Background())
	c.Assert(err1, IsNil)
}

func (s *testCommitterSuite) TestDeleteAllYourWritesWithSFU(c *C) {
	s.cluster.SplitKeys(ekv.Key("d"), ekv.Key("a"), 4)
	k1 := ekv.Key("a")
	k2 := ekv.Key("b")
	k3 := ekv.Key("c")

	// insert k1, k2, k2 and delete k1
	txn1 := s.begin(c)
	txn1.DelOption(ekv.Pessimistic)
	txn1.causetstore.txnLatches = nil
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, ekv.SetPresumeKeyNotExists)
	txn1.Delete(k1)
	err := txn1.LockKeys(context.Background(), &ekv.LockCtx{}, k2, k3) // select * from t where x in (k2, k3) for uFIDelate
	c.Assert(err, IsNil)

	committer1, err := newTwoPhaseCommitter(txn1, 0)
	c.Assert(err, IsNil)
	// setup test knob in txn's committer
	committer1.testingKnobs.acAfterCommitPrimary = make(chan struct{})
	committer1.testingKnobs.bkAfterCommitPrimary = make(chan struct{})
	txn1.committer = committer1
	var txn1Done sync.WaitGroup
	txn1Done.Add(1)
	go func() {
		err1 := txn1.Commit(context.Background())
		c.Assert(err1, IsNil)
		txn1Done.Done()
	}()
	// resume after after primary key be committed
	<-txn1.committer.testingKnobs.acAfterCommitPrimary
	// start txn2 to read k3
	txn2 := s.begin(c)
	txn2.DelOption(ekv.Pessimistic)
	txn2.causetstore.txnLatches = nil
	err = txn2.Set(k3, []byte{33})
	c.Assert(err, IsNil)
	var meetLocks []*Lock
	txn2.causetstore.lockResolver.testingKnobs.meetLock = func(locks []*Lock) {
		meetLocks = append(meetLocks, locks...)
	}
	err = txn2.Commit(context.Background())
	c.Assert(err, IsNil)
	txn1.committer.testingKnobs.bkAfterCommitPrimary <- struct{}{}
	txn1Done.Wait()
	c.Assert(meetLocks[0].Primary[0], Equals, k2[0])
}

// TestAcquireFalseTimeoutLock tests acquiring a key which is a secondary key of another transaction.
// The dagger's own TTL is expired but the primary key is still alive due to heartbeats.
func (s *testCommitterSuite) TestAcquireFalseTimeoutLock(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 1000)       // 1s
	defer atomic.StoreUint64(&ManagedLockTTL, 3000) // restore default test value

	// k1 is the primary dagger of txn1
	k1 := ekv.Key("k1")
	// k2 is a secondary dagger of txn1 and a key txn2 wants to dagger
	k2 := ekv.Key("k2")

	txn1 := s.begin(c)
	txn1.SetOption(ekv.Pessimistic, true)
	// dagger the primary key
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)
	// dagger the secondary key
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k2)
	c.Assert(err, IsNil)

	// Heartbeats will increase the TTL of the primary key

	// wait until secondary key exceeds its own TTL
	time.Sleep(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond)
	txn2 := s.begin(c)
	txn2.SetOption(ekv.Pessimistic, true)

	// test no wait
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn2.startTS, LockWaitTime: ekv.LockNoWait, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire dagger immediately thus error
	c.Assert(err.Error(), Equals, ErrLockAcquireFailAndNoWaitSet.Error())

	// test for wait limited time (200ms)
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn2.startTS, LockWaitTime: 200, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire dagger in time thus error
	c.Assert(err.Error(), Equals, ErrLockWaitTimeout.Error())
}

func (s *testCommitterSuite) getLockInfo(c *C, key []byte) *ekvrpcpb.LockInfo {
	txn := s.begin(c)
	err := txn.Set(key, key)
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)
	bo := NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	loc, err := s.causetstore.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	batch := batchMutations{region: loc.Region, mutations: committer.mutations.subRange(0, 1)}
	req := committer.buildPrewriteRequest(batch, 1)
	resp, err := s.causetstore.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErrs := (resp.Resp.(*ekvrpcpb.PrewriteResponse)).Errors
	c.Assert(keyErrs, HasLen, 1)
	locked := keyErrs[0].Locked
	c.Assert(locked, NotNil)
	return locked
}

func (s *testCommitterSuite) TestPkNotFound(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&ManagedLockTTL, 3000) // restore default value
	// k1 is the primary dagger of txn1
	k1 := ekv.Key("k1")
	// k2 is a secondary dagger of txn1 and a key txn2 wants to dagger
	k2 := ekv.Key("k2")
	k3 := ekv.Key("k3")

	txn1 := s.begin(c)
	txn1.SetOption(ekv.Pessimistic, true)
	// dagger the primary key
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)
	// dagger the secondary key
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k2)
	c.Assert(err, IsNil)

	// Stop txn ttl manager and remove primary key, like milevadb server crashes and the priamry key dagger does not exists actually,
	// while the secondary dagger operation succeeded
	bo := NewBackofferWithVars(context.Background(), pessimisticLockMaxBackoff, nil)
	txn1.committer.ttlManager.close()
	err = txn1.committer.pessimisticRollbackMutations(bo, CommitterMutations{keys: [][]byte{k1}})
	c.Assert(err, IsNil)

	// Txn2 tries to dagger the secondary key k2, dead loop if the left secondary dagger by txn1 not resolved
	txn2 := s.begin(c)
	txn2.SetOption(ekv.Pessimistic, true)
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn2.startTS, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	c.Assert(err, IsNil)

	// Using smaller forUFIDelateTS cannot rollback this dagger, other dagger will fail
	lockKey3 := &Lock{
		Key:                k3,
		Primary:            k1,
		TxnID:              txn1.startTS,
		TTL:                ManagedLockTTL,
		TxnSize:            txnCommitBatchSize,
		LockType:           ekvrpcpb.Op_PessimisticLock,
		LockForUFIDelateTS: txn1.startTS - 1,
	}
	cleanTxns := make(map[RegionVerID]struct{})
	err = s.causetstore.lockResolver.resolvePessimisticLock(bo, lockKey3, cleanTxns)
	c.Assert(err, IsNil)

	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k3)
	c.Assert(err, IsNil)
	txn3 := s.begin(c)
	txn3.SetOption(ekv.Pessimistic, true)
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn1.startTS - 1, WaitStartTime: time.Now(), LockWaitTime: ekv.LockNoWait}
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnNotFoundRetTTL", "return"), IsNil)
	err = txn3.LockKeys(context.Background(), lockCtx, k3)
	c.Assert(err.Error(), Equals, ErrLockAcquireFailAndNoWaitSet.Error())
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnNotFoundRetTTL"), IsNil)
}

func (s *testCommitterSuite) TestPessimisticLockPrimary(c *C) {
	// a is the primary dagger of txn1
	k1 := ekv.Key("a")
	// b is a secondary dagger of txn1 and a key txn2 wants to dagger, b is on another region
	k2 := ekv.Key("b")

	txn1 := s.begin(c)
	txn1.SetOption(ekv.Pessimistic, true)
	// txn1 dagger k1
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)

	// txn2 wants to dagger k1, k2, k1(pk) is blocked by txn1, pessimisticLockKeys has been changed to
	// dagger primary key first and then secondary keys concurrently, k2 should not be locked by txn2
	doneCh := make(chan error)
	go func() {
		txn2 := s.begin(c)
		txn2.SetOption(ekv.Pessimistic, true)
		lockCtx2 := &ekv.LockCtx{ForUFIDelateTS: txn2.startTS, WaitStartTime: time.Now(), LockWaitTime: 200}
		waitErr := txn2.LockKeys(context.Background(), lockCtx2, k1, k2)
		doneCh <- waitErr
	}()
	time.Sleep(50 * time.Millisecond)

	// txn3 should locks k2 successfully using no wait
	txn3 := s.begin(c)
	txn3.SetOption(ekv.Pessimistic, true)
	lockCtx3 := &ekv.LockCtx{ForUFIDelateTS: txn3.startTS, WaitStartTime: time.Now(), LockWaitTime: ekv.LockNoWait}
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnNotFoundRetTTL", "return"), IsNil)
	err = txn3.LockKeys(context.Background(), lockCtx3, k2)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/txnNotFoundRetTTL"), IsNil)
	c.Assert(err, IsNil)
	waitErr := <-doneCh
	c.Assert(ErrLockWaitTimeout.Equal(waitErr), IsTrue)
}

func (c *twoPhaseCommitter) mutationsOfKeys(keys [][]byte) CommitterMutations {
	var res CommitterMutations
	for i := range c.mutations.keys {
		for _, key := range keys {
			if bytes.Equal(c.mutations.keys[i], key) {
				res.Push(c.mutations.ops[i], c.mutations.keys[i], c.mutations.values[i], c.mutations.isPessimisticLock[i])
				break
			}
		}
	}
	return res
}

func (s *testCommitterSuite) TestCommitDeadLock(c *C) {
	// Split into two region and let k1 k2 in different regions.
	s.cluster.SplitKeys(ekv.Key("z"), ekv.Key("a"), 2)
	k1 := ekv.Key("a_deadlock_k1")
	k2 := ekv.Key("y_deadlock_k2")

	region1, _ := s.cluster.GetRegionByKey(k1)
	region2, _ := s.cluster.GetRegionByKey(k2)
	c.Assert(region1.Id != region2.Id, IsTrue)

	txn1 := s.begin(c)
	txn1.Set(k1, []byte("t1"))
	txn1.Set(k2, []byte("t1"))
	commit1, err := newTwoPhaseCommitterWithInit(txn1, 1)
	c.Assert(err, IsNil)
	commit1.primaryKey = k1
	commit1.txnSize = 1000 * 1024 * 1024
	commit1.lockTTL = txnLockTTL(txn1.startTime, commit1.txnSize)

	txn2 := s.begin(c)
	txn2.Set(k1, []byte("t2"))
	txn2.Set(k2, []byte("t2"))
	commit2, err := newTwoPhaseCommitterWithInit(txn2, 2)
	c.Assert(err, IsNil)
	commit2.primaryKey = k2
	commit2.txnSize = 1000 * 1024 * 1024
	commit2.lockTTL = txnLockTTL(txn1.startTime, commit2.txnSize)

	s.cluster.ScheduleDelay(txn2.startTS, region1.Id, 5*time.Millisecond)
	s.cluster.ScheduleDelay(txn1.startTS, region2.Id, 5*time.Millisecond)

	// Txn1 prewrites k1, k2 and txn2 prewrites k2, k1, the large txn
	// protocol run ttlManager and uFIDelate their TTL, cause dead dagger.
	ch := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ch <- commit2.execute(context.Background())
		wg.Done()
	}()
	ch <- commit1.execute(context.Background())
	wg.Wait()
	close(ch)

	res := 0
	for e := range ch {
		if e != nil {
			res++
		}
	}
	c.Assert(res, Equals, 1)
}

// TestPushPessimisticLock tests that push forward the minCommiTS of pessimistic locks.
func (s *testCommitterSuite) TestPushPessimisticLock(c *C) {
	// k1 is the primary key.
	k1, k2 := ekv.Key("a"), ekv.Key("b")
	ctx := context.Background()

	txn1 := s.begin(c)
	txn1.SetOption(ekv.Pessimistic, true)
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1, k2)
	c.Assert(err, IsNil)

	txn1.Set(k2, []byte("v2"))
	err = txn1.committer.initKeysAndMutations()
	c.Assert(err, IsNil)
	// Strip the prewrite of the primary key.
	txn1.committer.mutations = txn1.committer.mutations.subRange(1, 2)
	c.Assert(err, IsNil)
	err = txn1.committer.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), txn1.committer.mutations)
	c.Assert(err, IsNil)
	// The primary dagger is a pessimistic dagger and the secondary dagger is a optimistic dagger.
	lock1 := s.getLockInfo(c, k1)
	c.Assert(lock1.LockType, Equals, ekvrpcpb.Op_PessimisticLock)
	c.Assert(lock1.PrimaryLock, BytesEquals, []byte(k1))
	lock2 := s.getLockInfo(c, k2)
	c.Assert(lock2.LockType, Equals, ekvrpcpb.Op_Put)
	c.Assert(lock2.PrimaryLock, BytesEquals, []byte(k1))

	txn2 := s.begin(c)
	start := time.Now()
	_, err = txn2.Get(ctx, k2)
	elapsed := time.Since(start)
	// The optimistic dagger shouldn't causet reads.
	c.Assert(elapsed, Less, 500*time.Millisecond)
	c.Assert(ekv.IsErrNotFound(err), IsTrue)

	txn1.Rollback()
	txn2.Rollback()
}

// TestResolveMixed tests mixed resolve with left behind optimistic locks and pessimistic locks,
// using clean whole region resolve path
func (s *testCommitterSuite) TestResolveMixed(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&ManagedLockTTL, 3000) // restore default value
	ctx := context.Background()

	// pk is the primary dagger of txn1
	pk := ekv.Key("pk")
	secondaryLockkeys := make([]ekv.Key, 0, bigTxnThreshold)
	for i := 0; i < bigTxnThreshold; i++ {
		optimisticLock := ekv.Key(fmt.Sprintf("optimisticLockKey%d", i))
		secondaryLockkeys = append(secondaryLockkeys, optimisticLock)
	}
	pessimisticLockKey := ekv.Key("pessimisticLockKey")

	// make the optimistic and pessimistic dagger left with primary dagger not found
	txn1 := s.begin(c)
	txn1.SetOption(ekv.Pessimistic, true)
	// dagger the primary key
	lockCtx := &ekv.LockCtx{ForUFIDelateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, pk)
	c.Assert(err, IsNil)
	// dagger the optimistic keys
	for i := 0; i < bigTxnThreshold; i++ {
		txn1.Set(secondaryLockkeys[i], []byte(fmt.Sprintf("v%d", i)))
	}
	err = txn1.committer.initKeysAndMutations()
	c.Assert(err, IsNil)
	err = txn1.committer.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), txn1.committer.mutations)
	c.Assert(err, IsNil)
	// dagger the pessimistic keys
	err = txn1.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	c.Assert(err, IsNil)
	lock1 := s.getLockInfo(c, pessimisticLockKey)
	c.Assert(lock1.LockType, Equals, ekvrpcpb.Op_PessimisticLock)
	c.Assert(lock1.PrimaryLock, BytesEquals, []byte(pk))
	optimisticLockKey := secondaryLockkeys[0]
	lock2 := s.getLockInfo(c, optimisticLockKey)
	c.Assert(lock2.LockType, Equals, ekvrpcpb.Op_Put)
	c.Assert(lock2.PrimaryLock, BytesEquals, []byte(pk))

	// stop txn ttl manager and remove primary key, make the other keys left behind
	bo := NewBackofferWithVars(context.Background(), pessimisticLockMaxBackoff, nil)
	txn1.committer.ttlManager.close()
	err = txn1.committer.pessimisticRollbackMutations(bo, CommitterMutations{keys: [][]byte{pk}})
	c.Assert(err, IsNil)

	// try to resolve the left optimistic locks, use clean whole region
	cleanTxns := make(map[RegionVerID]struct{})
	time.Sleep(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond)
	optimisticLockInfo := s.getLockInfo(c, optimisticLockKey)
	dagger := NewLock(optimisticLockInfo)
	err = s.causetstore.lockResolver.resolveLock(NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, nil), dagger, TxnStatus{}, false, cleanTxns)
	c.Assert(err, IsNil)

	// txn2 tries to dagger the pessimisticLockKey, the dagger should has been resolved in clean whole region resolve
	txn2 := s.begin(c)
	txn2.SetOption(ekv.Pessimistic, true)
	lockCtx = &ekv.LockCtx{ForUFIDelateTS: txn2.startTS, WaitStartTime: time.Now(), LockWaitTime: ekv.LockNoWait}
	err = txn2.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	c.Assert(err, IsNil)

	err = txn1.Rollback()
	c.Assert(err, IsNil)
	err = txn2.Rollback()
	c.Assert(err, IsNil)
}

// TestSecondaryKeys tests that when async commit is enabled, each prewrite message includes an
// accurate list of secondary keys.
func (s *testCommitterSuite) TestPrewriteSecondaryKeys(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EinsteinDBClient.EnableAsyncCommit = true
	})

	// Prepare two regions first: (, 100) and [100, )
	region, _ := s.cluster.GetRegionByKey([]byte{50})
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte{100}, []uint64{newPeerID}, newPeerID)

	txn := s.begin(c)
	var val [1024]byte
	for i := byte(50); i < 120; i++ {
		err := txn.Set([]byte{i}, val[:])
		c.Assert(err, IsNil)
	}
	// Some duplicates.
	for i := byte(50); i < 120; i += 10 {
		err := txn.Set([]byte{i}, val[512:700])
		c.Assert(err, IsNil)
	}

	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)

	mock := mockClient{inner: s.causetstore.client}
	s.causetstore.client = &mock
	ctx := context.Background()
	// TODO remove this when minCommitTS is returned from mockStore prewrite response.
	committer.minCommitTS = committer.startTS + 10
	committer.testingKnobs.noFallBack = true
	err = committer.execute(ctx)
	c.Assert(err, IsNil)
	c.Assert(mock.seenPrimaryReq > 0, IsTrue)
	c.Assert(mock.seenSecondaryReq > 0, IsTrue)
}

func (s *testCommitterSuite) TestAsyncCommit(c *C) {
	defer config.RestoreFunc()()
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EinsteinDBClient.EnableAsyncCommit = true
	})

	ctx := context.Background()
	pk := ekv.Key("tpk")
	pkVal := []byte("pkVal")
	k1 := ekv.Key("tk1")
	k1Val := []byte("k1Val")
	txn1 := s.begin(c)
	err := txn1.Set(pk, pkVal)
	c.Assert(err, IsNil)
	err = txn1.Set(k1, k1Val)
	c.Assert(err, IsNil)

	committer, err := newTwoPhaseCommitterWithInit(txn1, 0)
	c.Assert(err, IsNil)
	committer.connID = 1
	committer.minCommitTS = txn1.startTS + 10
	err = committer.execute(ctx)
	c.Assert(err, IsNil)

	// TODO remove sleep when recovery logic is done
	time.Sleep(1 * time.Second)
	s.checkValues(c, map[string]string{
		string(pk): string(pkVal),
		string(k1): string(k1Val),
	})
}

type mockClient struct {
	inner            Client
	seenPrimaryReq   uint32
	seenSecondaryReq uint32
}

func (m *mockClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	// If we find a prewrite request, check if it satisfies our constraints.
	if pr, ok := req.Req.(*ekvrpcpb.PrewriteRequest); ok {
		if pr.UseAsyncCommit {
			if isPrimary(pr) {
				// The primary key should not be included, nor should there be any duplicates. All keys should be present.
				if !includesPrimary(pr) && allKeysNoDups(pr) {
					atomic.StoreUint32(&m.seenPrimaryReq, 1)
				}
			} else {
				// Secondaries should only be sent with the primary key
				if len(pr.Secondaries) == 0 {
					atomic.StoreUint32(&m.seenSecondaryReq, 1)
				}
			}
		}
	}
	return m.inner.SendRequest(ctx, addr, req, timeout)
}

func (m *mockClient) Close() error {
	return m.inner.Close()
}

func isPrimary(req *ekvrpcpb.PrewriteRequest) bool {
	for _, m := range req.Mutations {
		if bytes.Equal(req.PrimaryLock, m.Key) {
			return true
		}
	}

	return false
}

func includesPrimary(req *ekvrpcpb.PrewriteRequest) bool {
	for _, k := range req.Secondaries {
		if bytes.Equal(req.PrimaryLock, k) {
			return true
		}
	}

	return false
}

func allKeysNoDups(req *ekvrpcpb.PrewriteRequest) bool {
	check := make(map[string]bool)

	// Create the check map and check for duplicates.
	for _, k := range req.Secondaries {
		s := string(k)
		if check[s] {
			return false
		}
		check[s] = true
	}

	// Check every key is present.
	for i := byte(50); i < 120; i++ {
		k := []byte{i}
		if !bytes.Equal(req.PrimaryLock, k) && !check[string(k)] {
			return false
		}
	}
	return true
}
