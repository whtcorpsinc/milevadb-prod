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
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/mockeinsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/storeutil"
	"google.golang.org/grpc"
)

type testRegionRequestSuite struct {
	cluster             *mockeinsteindb.Cluster
	causetstore         uint64
	peer                uint64
	region              uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mockeinsteindb.MVCCStore
}

type testStoreLimitSuite struct {
	cluster             *mockeinsteindb.Cluster
	storeIDs            []uint64
	peerIDs             []uint64
	regionID            uint64
	leaderPeer          uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mockeinsteindb.MVCCStore
}

var _ = Suite(&testRegionRequestSuite{})
var _ = Suite(&testStoreLimitSuite{})

func (s *testRegionRequestSuite) SetUpTest(c *C) {
	s.cluster = mockeinsteindb.NewCluster(mockeinsteindb.MustNewMVCCStore())
	s.causetstore, s.peer, s.region = mockeinsteindb.BootstrapWithSingleStore(s.cluster)
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(s.cluster)}
	s.cache = NewRegionCache(FIDelCli)
	s.bo = NewNoopBackoff(context.Background())
	s.mvccStore = mockeinsteindb.MustNewMVCCStore()
	client := mockeinsteindb.NewRPCClient(s.cluster, s.mvccStore)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testStoreLimitSuite) SetUpTest(c *C) {
	s.cluster = mockeinsteindb.NewCluster(mockeinsteindb.MustNewMVCCStore())
	s.storeIDs, s.peerIDs, s.regionID, s.leaderPeer = mockeinsteindb.BootstrapWithMultiStores(s.cluster, 3)
	FIDelCli := &codecFIDelClient{mockeinsteindb.NewFIDelClient(s.cluster)}
	s.cache = NewRegionCache(FIDelCli)
	s.bo = NewNoopBackoff(context.Background())
	s.mvccStore = mockeinsteindb.MustNewMVCCStore()
	client := mockeinsteindb.NewRPCClient(s.cluster, s.mvccStore)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testRegionRequestSuite) TearDownTest(c *C) {
	s.cache.Close()
}

func (s *testStoreLimitSuite) TearDownTest(c *C) {
	s.cache.Close()
}

type fnClient struct {
	fn func(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error)
}

func (f *fnClient) Close() error {
	return nil
}

func (f *fnClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	return f.fn(ctx, addr, req, timeout)
}

func (s *testRegionRequestSuite) TestOnRegionError(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test stale command retry.
	func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (response *einsteindbrpc.Response, err error) {
			staleResp := &einsteindbrpc.Response{Resp: &ekvrpcpb.GetResponse{
				RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}},
			}}
			return staleResp, nil
		}}
		bo := NewBackofferWithVars(context.Background(), 5, nil)
		resp, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, NotNil)
		c.Assert(resp, IsNil)
	}()

}

func (s *testStoreLimitSuite) TestStoreTokenLimit(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPrewrite, &ekvrpcpb.PrewriteRequest{}, ekvrpcpb.Context{})
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	oldStoreLimit := storeutil.StoreLimit.Load()
	storeutil.StoreLimit.CausetStore(500)
	s.cache.getStoreByStoreID(s.storeIDs[0]).tokenCount.CausetStore(500)
	// cause there is only one region in this cluster, regionID maps this leader.
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(resp, IsNil)
	c.Assert(err.Error(), Equals, "[einsteindb:9008]CausetStore token is up to the limit, causetstore id = 1")
	storeutil.StoreLimit.CausetStore(oldStoreLimit)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithStoreRestart(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)

	// stop causetstore.
	s.cluster.StopStore(s.causetstore)
	_, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)

	// start causetstore.
	s.cluster.StartStore(s.causetstore)

	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithCloseKnownStoreThenUseNewOne(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})

	// add new store2 and make store2 as leader.
	store2 := s.cluster.AllocID()
	peer2 := s.cluster.AllocID()
	s.cluster.AddStore(store2, fmt.Sprintf("causetstore%d", store2))
	s.cluster.AddPeer(s.region, store2, peer2)
	s.cluster.ChangeLeader(s.region, peer2)

	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)

	// stop store2 and make store1 as new leader.
	s.cluster.StopStore(store2)
	s.cluster.ChangeLeader(s.region, s.peer)

	// send to store2 fail and send to new leader store1.
	bo2 := NewBackofferWithVars(context.Background(), 100, nil)
	resp, err = s.regionRequestSender.SendReq(bo2, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	regionErr, err := resp.GetRegionError()
	c.Assert(err, IsNil)
	c.Assert(regionErr, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestSuite) TestSendReqCtx(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, ctx, err := s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, ekv.EinsteinDB)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	c.Assert(ctx, NotNil)
	req.ReplicaRead = true
	resp, ctx, err = s.regionRequestSender.SendReqCtx(s.bo, req, region.Region, time.Second, ekv.EinsteinDB)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	c.Assert(ctx, NotNil)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithCancelled(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)

	// set causetstore to cancel state.
	s.cluster.CancelStore(s.causetstore)
	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	_, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	// set causetstore to normal state.
	s.cluster.UnCancelStore(s.causetstore)
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
}

func (s *testRegionRequestSuite) TestNoReloadRegionWhenCtxCanceled(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	sender := s.regionRequestSender
	bo, cancel := s.bo.Fork()
	cancel()
	// Call SendKVReq with a canceled context.
	_, err = sender.SendReq(bo, req, region.Region, time.Second)
	// Check this HoTT of error won't cause region cache drop.
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(sender.regionCache.getRegionByIDFromCache(s.region), NotNil)
}

// cancelContextClient wraps rpcClient and always cancels context before sending requests.
type cancelContextClient struct {
	Client
	redirectAddr string
}

func (c *cancelContextClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	childCtx, cancel := context.WithCancel(ctx)
	cancel()
	return c.Client.SendRequest(childCtx, c.redirectAddr, req, timeout)
}

// mockEinsteinDBGrpcServer mock a einsteindb gprc server for testing.
type mockEinsteinDBGrpcServer struct{}

// EkvGet commands with mvcc/txn supported.
func (s *mockEinsteinDBGrpcServer) EkvGet(context.Context, *ekvrpcpb.GetRequest) (*ekvrpcpb.GetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvScan(context.Context, *ekvrpcpb.ScanRequest) (*ekvrpcpb.ScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvPrewrite(context.Context, *ekvrpcpb.PrewriteRequest) (*ekvrpcpb.PrewriteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvCommit(context.Context, *ekvrpcpb.CommitRequest) (*ekvrpcpb.CommitResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvImport(context.Context, *ekvrpcpb.ImportRequest) (*ekvrpcpb.ImportResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvCleanup(context.Context, *ekvrpcpb.CleanupRequest) (*ekvrpcpb.CleanupResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvBatchGet(context.Context, *ekvrpcpb.BatchGetRequest) (*ekvrpcpb.BatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvBatchRollback(context.Context, *ekvrpcpb.BatchRollbackRequest) (*ekvrpcpb.BatchRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvScanLock(context.Context, *ekvrpcpb.ScanLockRequest) (*ekvrpcpb.ScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvResolveLock(context.Context, *ekvrpcpb.ResolveLockRequest) (*ekvrpcpb.ResolveLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvPessimisticLock(context.Context, *ekvrpcpb.PessimisticLockRequest) (*ekvrpcpb.PessimisticLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) KVPessimisticRollback(context.Context, *ekvrpcpb.PessimisticRollbackRequest) (*ekvrpcpb.PessimisticRollbackResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvCheckTxnStatus(ctx context.Context, in *ekvrpcpb.CheckTxnStatusRequest) (*ekvrpcpb.CheckTxnStatusResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvCheckSecondaryLocks(ctx context.Context, in *ekvrpcpb.CheckSecondaryLocksRequest) (*ekvrpcpb.CheckSecondaryLocksResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvTxnHeartBeat(ctx context.Context, in *ekvrpcpb.TxnHeartBeatRequest) (*ekvrpcpb.TxnHeartBeatResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvGC(context.Context, *ekvrpcpb.GCRequest) (*ekvrpcpb.GCResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) EkvDeleteRange(context.Context, *ekvrpcpb.DeleteRangeRequest) (*ekvrpcpb.DeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawGet(context.Context, *ekvrpcpb.RawGetRequest) (*ekvrpcpb.RawGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchGet(context.Context, *ekvrpcpb.RawBatchGetRequest) (*ekvrpcpb.RawBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawPut(context.Context, *ekvrpcpb.RawPutRequest) (*ekvrpcpb.RawPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchPut(context.Context, *ekvrpcpb.RawBatchPutRequest) (*ekvrpcpb.RawBatchPutResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawDelete(context.Context, *ekvrpcpb.RawDeleteRequest) (*ekvrpcpb.RawDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchDelete(context.Context, *ekvrpcpb.RawBatchDeleteRequest) (*ekvrpcpb.RawBatchDeleteResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawScan(context.Context, *ekvrpcpb.RawScanRequest) (*ekvrpcpb.RawScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawDeleteRange(context.Context, *ekvrpcpb.RawDeleteRangeRequest) (*ekvrpcpb.RawDeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RawBatchScan(context.Context, *ekvrpcpb.RawBatchScanRequest) (*ekvrpcpb.RawBatchScanResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) UnsafeDestroyRange(context.Context, *ekvrpcpb.UnsafeDestroyRangeRequest) (*ekvrpcpb.UnsafeDestroyRangeResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RegisterLockObserver(context.Context, *ekvrpcpb.RegisterLockObserverRequest) (*ekvrpcpb.RegisterLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) CheckLockObserver(context.Context, *ekvrpcpb.CheckLockObserverRequest) (*ekvrpcpb.CheckLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) RemoveLockObserver(context.Context, *ekvrpcpb.RemoveLockObserverRequest) (*ekvrpcpb.RemoveLockObserverResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) PhysicalScanLock(context.Context, *ekvrpcpb.PhysicalScanLockRequest) (*ekvrpcpb.PhysicalScanLockResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) Coprocessor(context.Context, *interlock.Request) (*interlock.Response, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) BatchCoprocessor(*interlock.BatchRequest, einsteindbpb.EinsteinDB_BatchCoprocessorServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) Raft(einsteindbpb.EinsteinDB_RaftServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) BatchRaft(einsteindbpb.EinsteinDB_BatchRaftServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) Snapshot(einsteindbpb.EinsteinDB_SnapshotServer) error {
	return errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) MvccGetByKey(context.Context, *ekvrpcpb.MvccGetByKeyRequest) (*ekvrpcpb.MvccGetByKeyResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) MvccGetByStartTs(context.Context, *ekvrpcpb.MvccGetByStartTsRequest) (*ekvrpcpb.MvccGetByStartTsResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mockEinsteinDBGrpcServer) SplitRegion(context.Context, *ekvrpcpb.SplitRegionRequest) (*ekvrpcpb.SplitRegionResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) CoprocessorStream(*interlock.Request, einsteindbpb.EinsteinDB_CoprocessorStreamServer) error {
	return errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) BatchCommands(einsteindbpb.EinsteinDB_BatchCommandsServer) error {
	return errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) ReadIndex(context.Context, *ekvrpcpb.ReadIndexRequest) (*ekvrpcpb.ReadIndexResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerGet(context.Context, *ekvrpcpb.VerGetRequest) (*ekvrpcpb.VerGetResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerBatchGet(context.Context, *ekvrpcpb.VerBatchGetRequest) (*ekvrpcpb.VerBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerMut(context.Context, *ekvrpcpb.VerMutRequest) (*ekvrpcpb.VerMutResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerBatchMut(context.Context, *ekvrpcpb.VerBatchMutRequest) (*ekvrpcpb.VerBatchMutResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerScan(context.Context, *ekvrpcpb.VerScanRequest) (*ekvrpcpb.VerScanResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mockEinsteinDBGrpcServer) VerDeleteRange(context.Context, *ekvrpcpb.VerDeleteRangeRequest) (*ekvrpcpb.VerDeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *testRegionRequestSuite) TestNoReloadRegionForGrpcWhenCtxCanceled(c *C) {
	// prepare a mock einsteindb grpc server
	addr := "localhost:56341"
	lis, err := net.Listen("tcp", addr)
	c.Assert(err, IsNil)
	server := grpc.NewServer()
	einsteindbpb.RegisterEinsteinDBServer(server, &mockEinsteinDBGrpcServer{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		server.Serve(lis)
		wg.Done()
	}()

	client := newRPCClient(config.Security{})
	sender := NewRegionRequestSender(s.cache, client)
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)

	bo, cancel := s.bo.Fork()
	cancel()
	_, err = sender.SendReq(bo, req, region.Region, 3*time.Second)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
	c.Assert(s.cache.getRegionByIDFromCache(s.region), NotNil)

	// Just for covering error code = codes.Canceled.
	client1 := &cancelContextClient{
		Client:       newRPCClient(config.Security{}),
		redirectAddr: addr,
	}
	sender = NewRegionRequestSender(s.cache, client1)
	sender.SendReq(s.bo, req, region.Region, 3*time.Second)

	// cleanup
	server.Stop()
	wg.Wait()
}

func (s *testRegionRequestSuite) TestOnMaxTimestampNotSyncedError(c *C) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdPrewrite, &ekvrpcpb.PrewriteRequest{})
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)

	// test retry for max timestamp not synced
	func() {
		oc := s.regionRequestSender.client
		defer func() {
			s.regionRequestSender.client = oc
		}()
		count := 0
		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (response *einsteindbrpc.Response, err error) {
			count += 1
			var resp *einsteindbrpc.Response
			if count < 3 {
				resp = &einsteindbrpc.Response{Resp: &ekvrpcpb.PrewriteResponse{
					RegionError: &errorpb.Error{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}},
				}}
			} else {
				resp = &einsteindbrpc.Response{Resp: &ekvrpcpb.PrewriteResponse{}}
			}
			return resp, nil
		}}
		bo := NewBackofferWithVars(context.Background(), 5, nil)
		resp, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
		c.Assert(err, IsNil)
		c.Assert(resp, NotNil)
	}()
}
