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

package entangledstore

import (
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	us "github.com/ngaut/entangledstore/einsteindb"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/debugpb"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"golang.org/x/net/context"
	"google.golang.org/grpc/spacetimedata"
)

// For gofail injection.
var undeterminedErr = terror.ErrResultUndetermined

// RPCClient sends ekv RPC calls to mock cluster. RPCClient mocks the behavior of
// a rpc client at einsteindb's side.
type RPCClient struct {
	usSvr      *us.Server
	cluster    *Cluster
	path       string
	rawHandler *rawHandler
	persistent bool
	closed     int32

	// rpcCli uses to redirects RPC request to MilevaDB rpc server, It is only use for test.
	// Mock MilevaDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
	// sync.Once uses to avoid concurrency initialize rpcCli.
	sync.Once
	rpcCli Client
}

// SendRequest sends a request to mock cluster.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	failpoint.Inject("rpcServerBusy", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(einsteindbrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}))
		}
	})

	if req.StoreTp == ekv.MilevaDB {
		return c.redirectRequestToRPCServer(ctx, addr, req, timeout)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if atomic.LoadInt32(&c.closed) != 0 {
		// Return `context.Canceled` can break Backoff.
		return nil, context.Canceled
	}

	resp := &einsteindbrpc.Response{}
	var err error
	switch req.Type {
	case einsteindbrpc.CmdGet:
		resp.Resp, err = c.usSvr.EkvGet(ctx, req.Get())
	case einsteindbrpc.CmdScan:
		resp.Resp, err = c.usSvr.EkvScan(ctx, req.Scan())
	case einsteindbrpc.CmdPrewrite:
		failpoint.Inject("rpcPrewriteResult", func(val failpoint.Value) {
			switch val.(string) {
			case "notLeader":
				failpoint.Return(&einsteindbrpc.Response{
					Resp: &ekvrpcpb.PrewriteResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			}
		})

		r := req.Prewrite()
		c.cluster.handleDelay(r.StartVersion, r.Context.RegionId)
		resp.Resp, err = c.usSvr.EkvPrewrite(ctx, r)
	case einsteindbrpc.CmdPessimisticLock:
		r := req.PessimisticLock()
		c.cluster.handleDelay(r.StartVersion, r.Context.RegionId)
		resp.Resp, err = c.usSvr.EkvPessimisticLock(ctx, r)
	case einsteindbrpc.CmdPessimisticRollback:
		resp.Resp, err = c.usSvr.KVPessimisticRollback(ctx, req.PessimisticRollback())
	case einsteindbrpc.CmdCommit:
		failpoint.Inject("rpcCommitResult", func(val failpoint.Value) {
			switch val.(string) {
			case "timeout":
				failpoint.Return(nil, errors.New("timeout"))
			case "notLeader":
				failpoint.Return(&einsteindbrpc.Response{
					Resp: &ekvrpcpb.CommitResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			case "keyError":
				failpoint.Return(&einsteindbrpc.Response{
					Resp: &ekvrpcpb.CommitResponse{Error: &ekvrpcpb.KeyError{}},
				}, nil)
			}
		})

		resp.Resp, err = c.usSvr.EkvCommit(ctx, req.Commit())

		failpoint.Inject("rpcCommitTimeout", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(nil, undeterminedErr)
			}
		})
	case einsteindbrpc.CmdCleanup:
		resp.Resp, err = c.usSvr.EkvCleanup(ctx, req.Cleanup())
	case einsteindbrpc.CmdCheckTxnStatus:
		resp.Resp, err = c.usSvr.EkvCheckTxnStatus(ctx, req.CheckTxnStatus())
	case einsteindbrpc.CmdCheckSecondaryLocks:
		resp.Resp, err = c.usSvr.EkvCheckSecondaryLocks(ctx, req.CheckSecondaryLocks())
	case einsteindbrpc.CmdTxnHeartBeat:
		resp.Resp, err = c.usSvr.EkvTxnHeartBeat(ctx, req.TxnHeartBeat())
	case einsteindbrpc.CmdBatchGet:
		resp.Resp, err = c.usSvr.EkvBatchGet(ctx, req.BatchGet())
	case einsteindbrpc.CmdBatchRollback:
		resp.Resp, err = c.usSvr.EkvBatchRollback(ctx, req.BatchRollback())
	case einsteindbrpc.CmdScanLock:
		resp.Resp, err = c.usSvr.EkvScanLock(ctx, req.ScanLock())
	case einsteindbrpc.CmdResolveLock:
		resp.Resp, err = c.usSvr.EkvResolveLock(ctx, req.ResolveLock())
	case einsteindbrpc.CmdGC:
		resp.Resp, err = c.usSvr.EkvGC(ctx, req.GC())
	case einsteindbrpc.CmdDeleteRange:
		resp.Resp, err = c.usSvr.EkvDeleteRange(ctx, req.DeleteRange())
	case einsteindbrpc.CmdRawGet:
		resp.Resp, err = c.rawHandler.RawGet(ctx, req.RawGet())
	case einsteindbrpc.CmdRawBatchGet:
		resp.Resp, err = c.rawHandler.RawBatchGet(ctx, req.RawBatchGet())
	case einsteindbrpc.CmdRawPut:
		resp.Resp, err = c.rawHandler.RawPut(ctx, req.RawPut())
	case einsteindbrpc.CmdRawBatchPut:
		resp.Resp, err = c.rawHandler.RawBatchPut(ctx, req.RawBatchPut())
	case einsteindbrpc.CmdRawDelete:
		resp.Resp, err = c.rawHandler.RawDelete(ctx, req.RawDelete())
	case einsteindbrpc.CmdRawBatchDelete:
		resp.Resp, err = c.rawHandler.RawBatchDelete(ctx, req.RawBatchDelete())
	case einsteindbrpc.CmdRawDeleteRange:
		resp.Resp, err = c.rawHandler.RawDeleteRange(ctx, req.RawDeleteRange())
	case einsteindbrpc.CmdRawScan:
		resp.Resp, err = c.rawHandler.RawScan(ctx, req.RawScan())
	case einsteindbrpc.CmdCop:
		resp.Resp, err = c.usSvr.Coprocessor(ctx, req.Causet())
	case einsteindbrpc.CmdCopStream:
		resp.Resp, err = c.handleCopStream(ctx, req.Causet())
	case einsteindbrpc.CmdMvccGetByKey:
		resp.Resp, err = c.usSvr.MvccGetByKey(ctx, req.MvccGetByKey())
	case einsteindbrpc.CmdMvccGetByStartTs:
		resp.Resp, err = c.usSvr.MvccGetByStartTs(ctx, req.MvccGetByStartTs())
	case einsteindbrpc.CmdSplitRegion:
		resp.Resp, err = c.usSvr.SplitRegion(ctx, req.SplitRegion())
	case einsteindbrpc.CmdDebugGetRegionProperties:
		resp.Resp, err = c.handleDebugGetRegionProperties(ctx, req.DebugGetRegionProperties())
		return resp, err
	default:
		err = errors.Errorf("unsupport this request type %v", req.Type)
	}
	if err != nil {
		return nil, err
	}
	regErr, err := resp.GetRegionError()
	if err != nil {
		return nil, err
	}
	if regErr != nil {
		if regErr.EpochNotMatch != nil {
			for i, newReg := range regErr.EpochNotMatch.CurrentRegions {
				regErr.EpochNotMatch.CurrentRegions[i] = proto.Clone(newReg).(*spacetimepb.Region)
			}
		}
	}
	return resp, nil
}

func (c *RPCClient) handleCopStream(ctx context.Context, req *interlock.Request) (*einsteindbrpc.CopStreamResponse, error) {
	copResp, err := c.usSvr.Coprocessor(ctx, req)
	if err != nil {
		return nil, err
	}
	return &einsteindbrpc.CopStreamResponse{
		EinsteinDB_CoprocessorStreamClient: new(mockCopStreamClient),
		Response:                           copResp,
	}, nil
}

func (c *RPCClient) handleDebugGetRegionProperties(ctx context.Context, req *debugpb.GetRegionPropertiesRequest) (*debugpb.GetRegionPropertiesResponse, error) {
	region := c.cluster.GetRegion(req.RegionId)
	_, start, err := codec.DecodeBytes(region.StartKey, nil)
	if err != nil {
		return nil, err
	}
	_, end, err := codec.DecodeBytes(region.EndKey, nil)
	if err != nil {
		return nil, err
	}
	scanResp, err := c.usSvr.EkvScan(ctx, &ekvrpcpb.ScanRequest{
		Context: &ekvrpcpb.Context{
			RegionId:    region.Id,
			RegionEpoch: region.RegionEpoch,
		},
		StartKey: start,
		EndKey:   end,
		Version:  math.MaxUint64,
		Limit:    math.MaxUint32,
	})
	if err != nil {
		return nil, err
	}
	if err := scanResp.GetRegionError(); err != nil {
		panic(err)
	}
	return &debugpb.GetRegionPropertiesResponse{
		Props: []*debugpb.Property{{
			Name:  "mvcc.num_rows",
			Value: strconv.Itoa(len(scanResp.Pairs)),
		}}}, nil
}

// Client is a client that sends RPC.
// This is same with einsteindb.Client, define again for avoid circle import.
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error)
}

// GRPCClientFactory is the GRPC client factory.
// Use global variable to avoid circle import.
// TODO: remove this global variable.
var GRPCClientFactory func() Client

// redirectRequestToRPCServer redirects RPC request to MilevaDB rpc server, It is only use for test.
// Mock MilevaDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
func (c *RPCClient) redirectRequestToRPCServer(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	c.Once.Do(func() {
		if GRPCClientFactory != nil {
			c.rpcCli = GRPCClientFactory()
		}
	})
	if c.rpcCli == nil {
		return nil, errors.Errorf("GRPCClientFactory is nil")
	}
	return c.rpcCli.SendRequest(ctx, addr, req, timeout)
}

// Close closes RPCClient and cleanup temporal resources.
func (c *RPCClient) Close() error {
	atomic.StoreInt32(&c.closed, 1)
	if c.usSvr != nil {
		c.usSvr.Stop()
	}
	if c.rpcCli != nil {
		err := c.rpcCli.Close()
		if err != nil {
			return err
		}
	}
	if !c.persistent && c.path != "" {
		err := os.RemoveAll(c.path)
		_ = err
	}
	return nil
}

type mockClientStream struct{}

// Header implements grpc.ClientStream interface
func (mockClientStream) Header() (spacetimedata.MD, error) { return nil, nil }

// Trailer implements grpc.ClientStream interface
func (mockClientStream) Trailer() spacetimedata.MD { return nil }

// CloseSend implements grpc.ClientStream interface
func (mockClientStream) CloseSend() error { return nil }

// Context implements grpc.ClientStream interface
func (mockClientStream) Context() context.Context { return nil }

// SendMsg implements grpc.ClientStream interface
func (mockClientStream) SendMsg(m interface{}) error { return nil }

// RecvMsg implements grpc.ClientStream interface
func (mockClientStream) RecvMsg(m interface{}) error { return nil }

type mockCopStreamClient struct {
	mockClientStream
}

func (mock *mockCopStreamClient) Recv() (*interlock.Response, error) {
	return nil, io.EOF
}
