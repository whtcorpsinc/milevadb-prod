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
	"sync/atomic"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/config"
)

type testAsyncCommitSuite struct {
	OneByOneSuite
	cluster     cluster.Cluster
	causetstore *einsteindbStore
	bo          *Backoffer
}

var _ = Suite(&testAsyncCommitSuite{})

func (s *testAsyncCommitSuite) SetUpTest(c *C) {
	client, clstr, FIDelClient, err := mockeinsteindb.NewEinsteinDBAndFIDelClient("")
	c.Assert(err, IsNil)
	mockeinsteindb.BootstrapWithSingleStore(clstr)
	s.cluster = clstr
	causetstore, err := NewTestEinsteinDBStore(client, FIDelClient, nil, nil, 0)
	c.Assert(err, IsNil)

	s.causetstore = causetstore.(*einsteindbStore)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testAsyncCommitSuite) putAlphabets(c *C) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV(c, []byte{ch}, []byte{ch})
	}
}

func (s *testAsyncCommitSuite) putKV(c *C, key, value []byte) (uint64, uint64) {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	return txn.StartTS(), txn.(*einsteindbTxn).commitTS
}

func (s *testAsyncCommitSuite) lockKeys(c *C, keys, values [][]byte, primaryKey, primaryValue []byte, commitPrimary bool) (uint64, uint64) {
	txn, err := newEinsteinDBTxn(s.causetstore)
	c.Assert(err, IsNil)
	for i, k := range keys {
		if len(values[i]) > 0 {
			err = txn.Set(k, values[i])
		} else {
			err = txn.Delete(k)
		}
		c.Assert(err, IsNil)
	}
	if len(primaryValue) > 0 {
		err = txn.Set(primaryKey, primaryValue)
	} else {
		err = txn.Delete(primaryKey)
	}
	c.Assert(err, IsNil)
	tpc, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	tpc.primaryKey = primaryKey

	ctx := context.Background()
	err = tpc.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), tpc.mutations)
	c.Assert(err, IsNil)

	if commitPrimary {
		tpc.commitTS, err = s.causetstore.oracle.GetTimestamp(ctx)
		c.Assert(err, IsNil)
		err = tpc.commitMutations(NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), tpc.mutationsOfKeys([][]byte{primaryKey}))
		c.Assert(err, IsNil)
	}
	return txn.startTS, tpc.commitTS
}

func (s *testAsyncCommitSuite) mustGetLock(c *C, key []byte) *Lock {
	ver, err := s.causetstore.CurrentVersion()
	c.Assert(err, IsNil)
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdGet, &ekvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	loc, err := s.causetstore.regionCache.LocateKey(s.bo, key)
	c.Assert(err, IsNil)
	resp, err := s.causetstore.SendReq(s.bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := resp.Resp.(*ekvrpcpb.GetResponse).GetError()
	c.Assert(keyErr, NotNil)
	dagger, err := extractLockFromKeyErr(keyErr)
	c.Assert(err, IsNil)
	return dagger
}

func (s *testAsyncCommitSuite) TestCheckSecondaries(c *C) {
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.EinsteinDBClient.EnableAsyncCommit = true
	})
	defer config.RestoreFunc()()

	s.putAlphabets(c)

	loc, err := s.causetstore.GetRegionCache().LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(loc.Region.id, newRegionID, []byte("e"), []uint64{peerID}, peerID)
	s.causetstore.GetRegionCache().InvalidateCachedRegion(loc.Region)

	// No locks to check, only primary key is locked, should be successful.
	s.lockKeys(c, [][]byte{}, [][]byte{}, []byte("z"), []byte("z"), false)
	dagger := s.mustGetLock(c, []byte("z"))
	dagger.UseAsyncCommit = true
	ts, err := s.causetstore.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	status := TxnStatus{primaryLock: &ekvrpcpb.LockInfo{Secondaries: [][]byte{}, UseAsyncCommit: true, MinCommitTs: ts}}

	err = s.causetstore.lockResolver.resolveLockAsync(s.bo, dagger, status)
	c.Assert(err, IsNil)
	currentTS, err := s.causetstore.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	status, err = s.causetstore.lockResolver.getTxnStatus(s.bo, dagger.TxnID, []byte("z"), currentTS, currentTS, true)
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, ts)

	// One key is committed (i), one key is locked (a). Should get committed.
	ts, err = s.causetstore.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	commitTs := ts + 10

	gotCheckA := int64(0)
	gotCheckB := int64(0)
	gotResolve := int64(0)
	gotOther := int64(0)
	mock := mockResolveClient{
		inner: s.causetstore.client,
		onCheckSecondaries: func(req *ekvrpcpb.CheckSecondaryLocksRequest) (*einsteindbrpc.Response, error) {
			if req.StartVersion != ts {
				return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
			}
			var resp ekvrpcpb.CheckSecondaryLocksResponse
			for _, k := range req.Keys {
				if bytes.Equal(k, []byte("a")) {
					atomic.StoreInt64(&gotCheckA, 1)

					resp = ekvrpcpb.CheckSecondaryLocksResponse{
						Locks:    []*ekvrpcpb.LockInfo{{Key: []byte("a"), PrimaryLock: []byte("z"), LockVersion: ts}},
						CommitTs: commitTs,
					}
				} else if bytes.Equal(k, []byte("i")) {
					atomic.StoreInt64(&gotCheckB, 1)

					resp = ekvrpcpb.CheckSecondaryLocksResponse{
						Locks:    []*ekvrpcpb.LockInfo{},
						CommitTs: commitTs,
					}
				} else {
					fmt.Printf("Got other key: %s\n", k)
					atomic.StoreInt64(&gotOther, 1)
				}
			}
			return &einsteindbrpc.Response{Resp: &resp}, nil
		},
		onResolveLock: func(req *ekvrpcpb.ResolveLockRequest) (*einsteindbrpc.Response, error) {
			if req.StartVersion != ts {
				return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
			}
			if req.CommitVersion != commitTs {
				return nil, errors.Errorf("Bad commit version: %d, expected: %d", req.CommitVersion, commitTs)
			}
			for _, k := range req.Keys {
				if bytes.Equal(k, []byte("a")) || bytes.Equal(k, []byte("z")) {
					atomic.StoreInt64(&gotResolve, 1)
				} else {
					atomic.StoreInt64(&gotOther, 1)
				}
			}
			resp := ekvrpcpb.ResolveLockResponse{}
			return &einsteindbrpc.Response{Resp: &resp}, nil
		},
	}
	s.causetstore.client = &mock

	status = TxnStatus{primaryLock: &ekvrpcpb.LockInfo{Secondaries: [][]byte{[]byte("a"), []byte("i")}, UseAsyncCommit: true}}
	dagger = &Lock{
		Key:            []byte("a"),
		Primary:        []byte("z"),
		TxnID:          ts,
		LockType:       ekvrpcpb.Op_Put,
		UseAsyncCommit: true,
		MinCommitTS:    ts + 5,
	}

	_, err = s.causetstore.Begin()
	c.Assert(err, IsNil)

	err = s.causetstore.lockResolver.resolveLockAsync(s.bo, dagger, status)
	c.Assert(err, IsNil)
	c.Assert(gotCheckA, Equals, int64(1))
	c.Assert(gotCheckB, Equals, int64(1))
	c.Assert(gotOther, Equals, int64(0))
	c.Assert(gotResolve, Equals, int64(1))

	// One key has been rolled back (b), one is locked (a). Should be rolled back.
	ts, err = s.causetstore.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	commitTs = ts + 10

	gotCheckA = int64(0)
	gotCheckB = int64(0)
	gotResolve = int64(0)
	gotOther = int64(0)
	mock.onResolveLock = func(req *ekvrpcpb.ResolveLockRequest) (*einsteindbrpc.Response, error) {
		if req.StartVersion != ts {
			return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
		}
		if req.CommitVersion != commitTs {
			return nil, errors.Errorf("Bad commit version: %d, expected: 0", req.CommitVersion)
		}
		for _, k := range req.Keys {
			if bytes.Equal(k, []byte("a")) || bytes.Equal(k, []byte("z")) {
				atomic.StoreInt64(&gotResolve, 1)
			} else {
				atomic.StoreInt64(&gotOther, 1)
			}
		}
		resp := ekvrpcpb.ResolveLockResponse{}
		return &einsteindbrpc.Response{Resp: &resp}, nil
	}

	dagger.TxnID = ts
	dagger.MinCommitTS = ts + 5

	err = s.causetstore.lockResolver.resolveLockAsync(s.bo, dagger, status)
	c.Assert(err, IsNil)
	c.Assert(gotCheckA, Equals, int64(1))
	c.Assert(gotCheckB, Equals, int64(1))
	c.Assert(gotResolve, Equals, int64(1))
	c.Assert(gotOther, Equals, int64(0))
}

type mockResolveClient struct {
	inner              Client
	onResolveLock      func(*ekvrpcpb.ResolveLockRequest) (*einsteindbrpc.Response, error)
	onCheckSecondaries func(*ekvrpcpb.CheckSecondaryLocksRequest) (*einsteindbrpc.Response, error)
}

func (m *mockResolveClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	// Intercept check secondary locks and resolve dagger messages if the callback is non-nil.
	// If the callback returns (nil, nil), forward to the inner client.
	if cr, ok := req.Req.(*ekvrpcpb.CheckSecondaryLocksRequest); ok && m.onCheckSecondaries != nil {
		result, err := m.onCheckSecondaries(cr)
		if result != nil || err != nil {
			return result, err
		}
	} else if rr, ok := req.Req.(*ekvrpcpb.ResolveLockRequest); ok && m.onResolveLock != nil {
		result, err := m.onResolveLock(rr)
		if result != nil || err != nil {
			return result, err
		}
	}
	return m.inner.SendRequest(ctx, addr, req, timeout)
}

func (m *mockResolveClient) Close() error {
	return m.inner.Close()
}
