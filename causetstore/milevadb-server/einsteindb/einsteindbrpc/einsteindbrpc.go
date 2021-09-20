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

package einsteindbrpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/ekvproto/pkg/debugpb"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdGet CmdType = 1 + iota
	CmdScan
	CmdPrewrite
	CmdCommit
	CmdCleanup
	CmdBatchGet
	CmdBatchRollback
	CmdScanLock
	CmdResolveLock
	CmdGC
	CmdDeleteRange
	CmdPessimisticLock
	CmdPessimisticRollback
	CmdTxnHeartBeat
	CmdCheckTxnStatus
	CmdCheckSecondaryLocks

	CmdRawGet CmdType = 256 + iota
	CmdRawBatchGet
	CmdRawPut
	CmdRawBatchPut
	CmdRawDelete
	CmdRawBatchDelete
	CmdRawDeleteRange
	CmdRawScan

	CmdUnsafeDestroyRange

	CmdRegisterLockObserver
	CmdCheckLockObserver
	CmdRemoveLockObserver
	CmdPhysicalScanLock

	CmdCop CmdType = 512 + iota
	CmdCopStream
	CmdBatchCop

	CmdMvccGetByKey CmdType = 1024 + iota
	CmdMvccGetByStartTs
	CmdSplitRegion

	CmdDebugGetRegionProperties CmdType = 2048 + iota

	CmdEmpty CmdType = 3072 + iota
)

func (t CmdType) String() string {
	switch t {
	case CmdGet:
		return "Get"
	case CmdScan:
		return "Scan"
	case CmdPrewrite:
		return "Prewrite"
	case CmdPessimisticLock:
		return "PessimisticLock"
	case CmdPessimisticRollback:
		return "PessimisticRollback"
	case CmdCommit:
		return "Commit"
	case CmdCleanup:
		return "Cleanup"
	case CmdBatchGet:
		return "BatchGet"
	case CmdBatchRollback:
		return "BatchRollback"
	case CmdScanLock:
		return "ScanLock"
	case CmdResolveLock:
		return "ResolveLock"
	case CmdGC:
		return "GC"
	case CmdDeleteRange:
		return "DeleteRange"
	case CmdRawGet:
		return "RawGet"
	case CmdRawBatchGet:
		return "RawBatchGet"
	case CmdRawPut:
		return "RawPut"
	case CmdRawBatchPut:
		return "RawBatchPut"
	case CmdRawDelete:
		return "RawDelete"
	case CmdRawBatchDelete:
		return "RawBatchDelete"
	case CmdRawDeleteRange:
		return "RawDeleteRange"
	case CmdRawScan:
		return "RawScan"
	case CmdUnsafeDestroyRange:
		return "UnsafeDestroyRange"
	case CmdRegisterLockObserver:
		return "RegisterLockObserver"
	case CmdCheckLockObserver:
		return "CheckLockObserver"
	case CmdRemoveLockObserver:
		return "RemoveLockObserver"
	case CmdPhysicalScanLock:
		return "PhysicalScanLock"
	case CmdCop:
		return "Causet"
	case CmdCopStream:
		return "CopStream"
	case CmdBatchCop:
		return "BatchCop"
	case CmdMvccGetByKey:
		return "MvccGetByKey"
	case CmdMvccGetByStartTs:
		return "MvccGetByStartTS"
	case CmdSplitRegion:
		return "SplitRegion"
	case CmdCheckTxnStatus:
		return "CheckTxnStatus"
	case CmdCheckSecondaryLocks:
		return "CheckSecondaryLocks"
	case CmdDebugGetRegionProperties:
		return "DebugGetRegionProperties"
	case CmdTxnHeartBeat:
		return "TxnHeartBeat"
	}
	return "Unknown"
}

// Request wraps all ekv/interlock requests.
type Request struct {
	Type CmdType
	Req  interface{}
	ekvrpcpb.Context
	ReplicaReadSeed *uint32 // pointer to follower read seed in snapshot/interlock
	StoreTp         ekv.StoreType
}

// NewRequest returns new ekv rpc request.
func NewRequest(typ CmdType, pointer interface{}, ctxs ...ekvrpcpb.Context) *Request {
	if len(ctxs) > 0 {
		return &Request{
			Type:    typ,
			Req:     pointer,
			Context: ctxs[0],
		}
	}
	return &Request{
		Type: typ,
		Req:  pointer,
	}
}

// NewReplicaReadRequest returns new ekv rpc request with replica read.
func NewReplicaReadRequest(typ CmdType, pointer interface{}, replicaReadType ekv.ReplicaReadType, replicaReadSeed *uint32, ctxs ...ekvrpcpb.Context) *Request {
	req := NewRequest(typ, pointer, ctxs...)
	req.ReplicaRead = replicaReadType.IsFollowerRead()
	req.ReplicaReadSeed = replicaReadSeed
	return req
}

// Get returns GetRequest in request.
func (req *Request) Get() *ekvrpcpb.GetRequest {
	return req.Req.(*ekvrpcpb.GetRequest)
}

// Scan returns ScanRequest in request.
func (req *Request) Scan() *ekvrpcpb.ScanRequest {
	return req.Req.(*ekvrpcpb.ScanRequest)
}

// Prewrite returns PrewriteRequest in request.
func (req *Request) Prewrite() *ekvrpcpb.PrewriteRequest {
	return req.Req.(*ekvrpcpb.PrewriteRequest)
}

// Commit returns CommitRequest in request.
func (req *Request) Commit() *ekvrpcpb.CommitRequest {
	return req.Req.(*ekvrpcpb.CommitRequest)
}

// Cleanup returns CleanupRequest in request.
func (req *Request) Cleanup() *ekvrpcpb.CleanupRequest {
	return req.Req.(*ekvrpcpb.CleanupRequest)
}

// BatchGet returns BatchGetRequest in request.
func (req *Request) BatchGet() *ekvrpcpb.BatchGetRequest {
	return req.Req.(*ekvrpcpb.BatchGetRequest)
}

// BatchRollback returns BatchRollbackRequest in request.
func (req *Request) BatchRollback() *ekvrpcpb.BatchRollbackRequest {
	return req.Req.(*ekvrpcpb.BatchRollbackRequest)
}

// ScanLock returns ScanLockRequest in request.
func (req *Request) ScanLock() *ekvrpcpb.ScanLockRequest {
	return req.Req.(*ekvrpcpb.ScanLockRequest)
}

// ResolveLock returns ResolveLockRequest in request.
func (req *Request) ResolveLock() *ekvrpcpb.ResolveLockRequest {
	return req.Req.(*ekvrpcpb.ResolveLockRequest)
}

// GC returns GCRequest in request.
func (req *Request) GC() *ekvrpcpb.GCRequest {
	return req.Req.(*ekvrpcpb.GCRequest)
}

// DeleteRange returns DeleteRangeRequest in request.
func (req *Request) DeleteRange() *ekvrpcpb.DeleteRangeRequest {
	return req.Req.(*ekvrpcpb.DeleteRangeRequest)
}

// RawGet returns RawGetRequest in request.
func (req *Request) RawGet() *ekvrpcpb.RawGetRequest {
	return req.Req.(*ekvrpcpb.RawGetRequest)
}

// RawBatchGet returns RawBatchGetRequest in request.
func (req *Request) RawBatchGet() *ekvrpcpb.RawBatchGetRequest {
	return req.Req.(*ekvrpcpb.RawBatchGetRequest)
}

// RawPut returns RawPutRequest in request.
func (req *Request) RawPut() *ekvrpcpb.RawPutRequest {
	return req.Req.(*ekvrpcpb.RawPutRequest)
}

// RawBatchPut returns RawBatchPutRequest in request.
func (req *Request) RawBatchPut() *ekvrpcpb.RawBatchPutRequest {
	return req.Req.(*ekvrpcpb.RawBatchPutRequest)
}

// RawDelete returns PrewriteRequest in request.
func (req *Request) RawDelete() *ekvrpcpb.RawDeleteRequest {
	return req.Req.(*ekvrpcpb.RawDeleteRequest)
}

// RawBatchDelete returns RawBatchDeleteRequest in request.
func (req *Request) RawBatchDelete() *ekvrpcpb.RawBatchDeleteRequest {
	return req.Req.(*ekvrpcpb.RawBatchDeleteRequest)
}

// RawDeleteRange returns RawDeleteRangeRequest in request.
func (req *Request) RawDeleteRange() *ekvrpcpb.RawDeleteRangeRequest {
	return req.Req.(*ekvrpcpb.RawDeleteRangeRequest)
}

// RawScan returns RawScanRequest in request.
func (req *Request) RawScan() *ekvrpcpb.RawScanRequest {
	return req.Req.(*ekvrpcpb.RawScanRequest)
}

// UnsafeDestroyRange returns UnsafeDestroyRangeRequest in request.
func (req *Request) UnsafeDestroyRange() *ekvrpcpb.UnsafeDestroyRangeRequest {
	return req.Req.(*ekvrpcpb.UnsafeDestroyRangeRequest)
}

// RegisterLockObserver returns RegisterLockObserverRequest in request.
func (req *Request) RegisterLockObserver() *ekvrpcpb.RegisterLockObserverRequest {
	return req.Req.(*ekvrpcpb.RegisterLockObserverRequest)
}

// CheckLockObserver returns CheckLockObserverRequest in request.
func (req *Request) CheckLockObserver() *ekvrpcpb.CheckLockObserverRequest {
	return req.Req.(*ekvrpcpb.CheckLockObserverRequest)
}

// RemoveLockObserver returns RemoveLockObserverRequest in request.
func (req *Request) RemoveLockObserver() *ekvrpcpb.RemoveLockObserverRequest {
	return req.Req.(*ekvrpcpb.RemoveLockObserverRequest)
}

// PhysicalScanLock returns PhysicalScanLockRequest in request.
func (req *Request) PhysicalScanLock() *ekvrpcpb.PhysicalScanLockRequest {
	return req.Req.(*ekvrpcpb.PhysicalScanLockRequest)
}

// Causet returns interlock request in request.
func (req *Request) Causet() *interlock.Request {
	return req.Req.(*interlock.Request)
}

// BatchCop returns interlock request in request.
func (req *Request) BatchCop() *interlock.BatchRequest {
	return req.Req.(*interlock.BatchRequest)
}

// MvccGetByKey returns MvccGetByKeyRequest in request.
func (req *Request) MvccGetByKey() *ekvrpcpb.MvccGetByKeyRequest {
	return req.Req.(*ekvrpcpb.MvccGetByKeyRequest)
}

// MvccGetByStartTs returns MvccGetByStartTsRequest in request.
func (req *Request) MvccGetByStartTs() *ekvrpcpb.MvccGetByStartTsRequest {
	return req.Req.(*ekvrpcpb.MvccGetByStartTsRequest)
}

// SplitRegion returns SplitRegionRequest in request.
func (req *Request) SplitRegion() *ekvrpcpb.SplitRegionRequest {
	return req.Req.(*ekvrpcpb.SplitRegionRequest)
}

// PessimisticLock returns PessimisticLockRequest in request.
func (req *Request) PessimisticLock() *ekvrpcpb.PessimisticLockRequest {
	return req.Req.(*ekvrpcpb.PessimisticLockRequest)
}

// PessimisticRollback returns PessimisticRollbackRequest in request.
func (req *Request) PessimisticRollback() *ekvrpcpb.PessimisticRollbackRequest {
	return req.Req.(*ekvrpcpb.PessimisticRollbackRequest)
}

// DebugGetRegionProperties returns GetRegionPropertiesRequest in request.
func (req *Request) DebugGetRegionProperties() *debugpb.GetRegionPropertiesRequest {
	return req.Req.(*debugpb.GetRegionPropertiesRequest)
}

// Empty returns BatchCommandsEmptyRequest in request.
func (req *Request) Empty() *einsteindbpb.BatchCommandsEmptyRequest {
	return req.Req.(*einsteindbpb.BatchCommandsEmptyRequest)
}

// CheckTxnStatus returns CheckTxnStatusRequest in request.
func (req *Request) CheckTxnStatus() *ekvrpcpb.CheckTxnStatusRequest {
	return req.Req.(*ekvrpcpb.CheckTxnStatusRequest)
}

// CheckSecondaryLocks returns CheckSecondaryLocksRequest in request.
func (req *Request) CheckSecondaryLocks() *ekvrpcpb.CheckSecondaryLocksRequest {
	return req.Req.(*ekvrpcpb.CheckSecondaryLocksRequest)
}

// TxnHeartBeat returns TxnHeartBeatRequest in request.
func (req *Request) TxnHeartBeat() *ekvrpcpb.TxnHeartBeatRequest {
	return req.Req.(*ekvrpcpb.TxnHeartBeatRequest)
}

// ToBatchCommandsRequest converts the request to an entry in BatchCommands request.
func (req *Request) ToBatchCommandsRequest() *einsteindbpb.BatchCommandsRequest_Request {
	switch req.Type {
	case CmdGet:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_Get{Get: req.Get()}}
	case CmdScan:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_Scan{Scan: req.Scan()}}
	case CmdPrewrite:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_Prewrite{Prewrite: req.Prewrite()}}
	case CmdCommit:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_Commit{Commit: req.Commit()}}
	case CmdCleanup:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_Cleanup{Cleanup: req.Cleanup()}}
	case CmdBatchGet:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_BatchGet{BatchGet: req.BatchGet()}}
	case CmdBatchRollback:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_BatchRollback{BatchRollback: req.BatchRollback()}}
	case CmdScanLock:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_ScanLock{ScanLock: req.ScanLock()}}
	case CmdResolveLock:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_ResolveLock{ResolveLock: req.ResolveLock()}}
	case CmdGC:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_GC{GC: req.GC()}}
	case CmdDeleteRange:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_DeleteRange{DeleteRange: req.DeleteRange()}}
	case CmdRawGet:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawGet{RawGet: req.RawGet()}}
	case CmdRawBatchGet:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawBatchGet{RawBatchGet: req.RawBatchGet()}}
	case CmdRawPut:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawPut{RawPut: req.RawPut()}}
	case CmdRawBatchPut:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawBatchPut{RawBatchPut: req.RawBatchPut()}}
	case CmdRawDelete:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawDelete{RawDelete: req.RawDelete()}}
	case CmdRawBatchDelete:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawBatchDelete{RawBatchDelete: req.RawBatchDelete()}}
	case CmdRawDeleteRange:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawDeleteRange{RawDeleteRange: req.RawDeleteRange()}}
	case CmdRawScan:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_RawScan{RawScan: req.RawScan()}}
	case CmdCop:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: req.Causet()}}
	case CmdPessimisticLock:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_PessimisticLock{PessimisticLock: req.PessimisticLock()}}
	case CmdPessimisticRollback:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_PessimisticRollback{PessimisticRollback: req.PessimisticRollback()}}
	case CmdEmpty:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_Empty{Empty: req.Empty()}}
	case CmdCheckTxnStatus:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_CheckTxnStatus{CheckTxnStatus: req.CheckTxnStatus()}}
	case CmdCheckSecondaryLocks:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_CheckSecondaryLocks{CheckSecondaryLocks: req.CheckSecondaryLocks()}}
	case CmdTxnHeartBeat:
		return &einsteindbpb.BatchCommandsRequest_Request{Cmd: &einsteindbpb.BatchCommandsRequest_Request_TxnHeartBeat{TxnHeartBeat: req.TxnHeartBeat()}}
	}
	return nil
}

// IsDebugReq check whether the req is debug req.
func (req *Request) IsDebugReq() bool {
	switch req.Type {
	case CmdDebugGetRegionProperties:
		return true
	}
	return false
}

// Response wraps all ekv/interlock responses.
type Response struct {
	Resp interface{}
}

// FromBatchCommandsResponse converts a BatchCommands response to Response.
func FromBatchCommandsResponse(res *einsteindbpb.BatchCommandsResponse_Response) (*Response, error) {
	if res.GetCmd() == nil {
		return nil, errors.New("Unknown command response")
	}
	switch res := res.GetCmd().(type) {
	case *einsteindbpb.BatchCommandsResponse_Response_Get:
		return &Response{Resp: res.Get}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_Scan:
		return &Response{Resp: res.Scan}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_Prewrite:
		return &Response{Resp: res.Prewrite}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_Commit:
		return &Response{Resp: res.Commit}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_Cleanup:
		return &Response{Resp: res.Cleanup}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_BatchGet:
		return &Response{Resp: res.BatchGet}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_BatchRollback:
		return &Response{Resp: res.BatchRollback}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_ScanLock:
		return &Response{Resp: res.ScanLock}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_ResolveLock:
		return &Response{Resp: res.ResolveLock}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_GC:
		return &Response{Resp: res.GC}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_DeleteRange:
		return &Response{Resp: res.DeleteRange}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawGet:
		return &Response{Resp: res.RawGet}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawBatchGet:
		return &Response{Resp: res.RawBatchGet}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawPut:
		return &Response{Resp: res.RawPut}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawBatchPut:
		return &Response{Resp: res.RawBatchPut}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawDelete:
		return &Response{Resp: res.RawDelete}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawBatchDelete:
		return &Response{Resp: res.RawBatchDelete}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawDeleteRange:
		return &Response{Resp: res.RawDeleteRange}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_RawScan:
		return &Response{Resp: res.RawScan}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_Coprocessor:
		return &Response{Resp: res.Coprocessor}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_PessimisticLock:
		return &Response{Resp: res.PessimisticLock}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_PessimisticRollback:
		return &Response{Resp: res.PessimisticRollback}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_Empty:
		return &Response{Resp: res.Empty}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_TxnHeartBeat:
		return &Response{Resp: res.TxnHeartBeat}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_CheckTxnStatus:
		return &Response{Resp: res.CheckTxnStatus}, nil
	case *einsteindbpb.BatchCommandsResponse_Response_CheckSecondaryLocks:
		return &Response{Resp: res.CheckSecondaryLocks}, nil
	}
	panic("unreachable")
}

// CopStreamResponse combinates einsteindbpb.EinsteinDB_CoprocessorStreamClient and the first Recv() result together.
// In streaming API, get grpc stream client may not involve any network packet, then region error have
// to be handled in Recv() function. This struct facilitates the error handling.
type CopStreamResponse struct {
	einsteindbpb.EinsteinDB_CoprocessorStreamClient
	*interlock.Response // The first result of Recv()
	Timeout             time.Duration
	Lease               // Shared by this object and a background goroutine.
}

// BatchCopStreamResponse comprises the BatchCoprocessorClient , the first result and timeout detector.
type BatchCopStreamResponse struct {
	einsteindbpb.EinsteinDB_BatchCoprocessorClient
	*interlock.BatchResponse
	Timeout time.Duration
	Lease   // Shared by this object and a background goroutine.
}

// SetContext set the Context field for the given req to the specified ctx.
func SetContext(req *Request, region *spacetimepb.Region, peer *spacetimepb.Peer) error {
	ctx := &req.Context
	if region != nil {
		ctx.RegionId = region.Id
		ctx.RegionEpoch = region.RegionEpoch
	}
	ctx.Peer = peer

	switch req.Type {
	case CmdGet:
		req.Get().Context = ctx
	case CmdScan:
		req.Scan().Context = ctx
	case CmdPrewrite:
		req.Prewrite().Context = ctx
	case CmdPessimisticLock:
		req.PessimisticLock().Context = ctx
	case CmdPessimisticRollback:
		req.PessimisticRollback().Context = ctx
	case CmdCommit:
		req.Commit().Context = ctx
	case CmdCleanup:
		req.Cleanup().Context = ctx
	case CmdBatchGet:
		req.BatchGet().Context = ctx
	case CmdBatchRollback:
		req.BatchRollback().Context = ctx
	case CmdScanLock:
		req.ScanLock().Context = ctx
	case CmdResolveLock:
		req.ResolveLock().Context = ctx
	case CmdGC:
		req.GC().Context = ctx
	case CmdDeleteRange:
		req.DeleteRange().Context = ctx
	case CmdRawGet:
		req.RawGet().Context = ctx
	case CmdRawBatchGet:
		req.RawBatchGet().Context = ctx
	case CmdRawPut:
		req.RawPut().Context = ctx
	case CmdRawBatchPut:
		req.RawBatchPut().Context = ctx
	case CmdRawDelete:
		req.RawDelete().Context = ctx
	case CmdRawBatchDelete:
		req.RawBatchDelete().Context = ctx
	case CmdRawDeleteRange:
		req.RawDeleteRange().Context = ctx
	case CmdRawScan:
		req.RawScan().Context = ctx
	case CmdUnsafeDestroyRange:
		req.UnsafeDestroyRange().Context = ctx
	case CmdRegisterLockObserver:
		req.RegisterLockObserver().Context = ctx
	case CmdCheckLockObserver:
		req.CheckLockObserver().Context = ctx
	case CmdRemoveLockObserver:
		req.RemoveLockObserver().Context = ctx
	case CmdPhysicalScanLock:
		req.PhysicalScanLock().Context = ctx
	case CmdCop:
		req.Causet().Context = ctx
	case CmdCopStream:
		req.Causet().Context = ctx
	case CmdBatchCop:
		req.BatchCop().Context = ctx
	case CmdMvccGetByKey:
		req.MvccGetByKey().Context = ctx
	case CmdMvccGetByStartTs:
		req.MvccGetByStartTs().Context = ctx
	case CmdSplitRegion:
		req.SplitRegion().Context = ctx
	case CmdEmpty:
		req.SplitRegion().Context = ctx
	case CmdTxnHeartBeat:
		req.TxnHeartBeat().Context = ctx
	case CmdCheckTxnStatus:
		req.CheckTxnStatus().Context = ctx
	case CmdCheckSecondaryLocks:
		req.CheckSecondaryLocks().Context = ctx
	default:
		return fmt.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	var p interface{}
	resp := &Response{}
	switch req.Type {
	case CmdGet:
		p = &ekvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		p = &ekvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		p = &ekvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdPessimisticLock:
		p = &ekvrpcpb.PessimisticLockResponse{
			RegionError: e,
		}
	case CmdPessimisticRollback:
		p = &ekvrpcpb.PessimisticRollbackResponse{
			RegionError: e,
		}
	case CmdCommit:
		p = &ekvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdCleanup:
		p = &ekvrpcpb.CleanupResponse{
			RegionError: e,
		}
	case CmdBatchGet:
		p = &ekvrpcpb.BatchGetResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		p = &ekvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdScanLock:
		p = &ekvrpcpb.ScanLockResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		p = &ekvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdGC:
		p = &ekvrpcpb.GCResponse{
			RegionError: e,
		}
	case CmdDeleteRange:
		p = &ekvrpcpb.DeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawGet:
		p = &ekvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawBatchGet:
		p = &ekvrpcpb.RawBatchGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		p = &ekvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawBatchPut:
		p = &ekvrpcpb.RawBatchPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		p = &ekvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdRawBatchDelete:
		p = &ekvrpcpb.RawBatchDeleteResponse{
			RegionError: e,
		}
	case CmdRawDeleteRange:
		p = &ekvrpcpb.RawDeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawScan:
		p = &ekvrpcpb.RawScanResponse{
			RegionError: e,
		}
	case CmdUnsafeDestroyRange:
		p = &ekvrpcpb.UnsafeDestroyRangeResponse{
			RegionError: e,
		}
	case CmdCop:
		p = &interlock.Response{
			RegionError: e,
		}
	case CmdCopStream:
		p = &CopStreamResponse{
			Response: &interlock.Response{
				RegionError: e,
			},
		}
	case CmdMvccGetByKey:
		p = &ekvrpcpb.MvccGetByKeyResponse{
			RegionError: e,
		}
	case CmdMvccGetByStartTs:
		p = &ekvrpcpb.MvccGetByStartTsResponse{
			RegionError: e,
		}
	case CmdSplitRegion:
		p = &ekvrpcpb.SplitRegionResponse{
			RegionError: e,
		}
	case CmdEmpty:
	case CmdTxnHeartBeat:
		p = &ekvrpcpb.TxnHeartBeatResponse{
			RegionError: e,
		}
	case CmdCheckTxnStatus:
		p = &ekvrpcpb.CheckTxnStatusResponse{
			RegionError: e,
		}
	case CmdCheckSecondaryLocks:
		p = &ekvrpcpb.CheckSecondaryLocksResponse{
			RegionError: e,
		}
	default:
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	resp.Resp = p
	return resp, nil
}

type getRegionError interface {
	GetRegionError() *errorpb.Error
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	if resp.Resp == nil {
		return nil, nil
	}
	err, ok := resp.Resp.(getRegionError)
	if !ok {
		if _, isEmpty := resp.Resp.(*einsteindbpb.BatchCommandsEmptyResponse); isEmpty {
			return nil, nil
		}
		return nil, fmt.Errorf("invalid response type %v", resp)
	}
	return err.GetRegionError(), nil
}

// CallRPC launches a rpc call.
// ch is needed to implement timeout for interlock streaing, the stream object's
// cancel function will be sent to the channel, together with a lease checked by a background goroutine.
func CallRPC(ctx context.Context, client einsteindbpb.EinsteinDBClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdGet:
		resp.Resp, err = client.EkvGet(ctx, req.Get())
	case CmdScan:
		resp.Resp, err = client.EkvScan(ctx, req.Scan())
	case CmdPrewrite:
		resp.Resp, err = client.EkvPrewrite(ctx, req.Prewrite())
	case CmdPessimisticLock:
		resp.Resp, err = client.EkvPessimisticLock(ctx, req.PessimisticLock())
	case CmdPessimisticRollback:
		resp.Resp, err = client.KVPessimisticRollback(ctx, req.PessimisticRollback())
	case CmdCommit:
		resp.Resp, err = client.EkvCommit(ctx, req.Commit())
	case CmdCleanup:
		resp.Resp, err = client.EkvCleanup(ctx, req.Cleanup())
	case CmdBatchGet:
		resp.Resp, err = client.EkvBatchGet(ctx, req.BatchGet())
	case CmdBatchRollback:
		resp.Resp, err = client.EkvBatchRollback(ctx, req.BatchRollback())
	case CmdScanLock:
		resp.Resp, err = client.EkvScanLock(ctx, req.ScanLock())
	case CmdResolveLock:
		resp.Resp, err = client.EkvResolveLock(ctx, req.ResolveLock())
	case CmdGC:
		resp.Resp, err = client.EkvGC(ctx, req.GC())
	case CmdDeleteRange:
		resp.Resp, err = client.EkvDeleteRange(ctx, req.DeleteRange())
	case CmdRawGet:
		resp.Resp, err = client.RawGet(ctx, req.RawGet())
	case CmdRawBatchGet:
		resp.Resp, err = client.RawBatchGet(ctx, req.RawBatchGet())
	case CmdRawPut:
		resp.Resp, err = client.RawPut(ctx, req.RawPut())
	case CmdRawBatchPut:
		resp.Resp, err = client.RawBatchPut(ctx, req.RawBatchPut())
	case CmdRawDelete:
		resp.Resp, err = client.RawDelete(ctx, req.RawDelete())
	case CmdRawBatchDelete:
		resp.Resp, err = client.RawBatchDelete(ctx, req.RawBatchDelete())
	case CmdRawDeleteRange:
		resp.Resp, err = client.RawDeleteRange(ctx, req.RawDeleteRange())
	case CmdRawScan:
		resp.Resp, err = client.RawScan(ctx, req.RawScan())
	case CmdUnsafeDestroyRange:
		resp.Resp, err = client.UnsafeDestroyRange(ctx, req.UnsafeDestroyRange())
	case CmdRegisterLockObserver:
		resp.Resp, err = client.RegisterLockObserver(ctx, req.RegisterLockObserver())
	case CmdCheckLockObserver:
		resp.Resp, err = client.CheckLockObserver(ctx, req.CheckLockObserver())
	case CmdRemoveLockObserver:
		resp.Resp, err = client.RemoveLockObserver(ctx, req.RemoveLockObserver())
	case CmdPhysicalScanLock:
		resp.Resp, err = client.PhysicalScanLock(ctx, req.PhysicalScanLock())
	case CmdCop:
		resp.Resp, err = client.Coprocessor(ctx, req.Causet())
	case CmdCopStream:
		var streamClient einsteindbpb.EinsteinDB_CoprocessorStreamClient
		streamClient, err = client.CoprocessorStream(ctx, req.Causet())
		resp.Resp = &CopStreamResponse{
			EinsteinDB_CoprocessorStreamClient: streamClient,
		}
	case CmdBatchCop:
		var streamClient einsteindbpb.EinsteinDB_BatchCoprocessorClient
		streamClient, err = client.BatchCoprocessor(ctx, req.BatchCop())
		resp.Resp = &BatchCopStreamResponse{
			EinsteinDB_BatchCoprocessorClient: streamClient,
		}
	case CmdMvccGetByKey:
		resp.Resp, err = client.MvccGetByKey(ctx, req.MvccGetByKey())
	case CmdMvccGetByStartTs:
		resp.Resp, err = client.MvccGetByStartTs(ctx, req.MvccGetByStartTs())
	case CmdSplitRegion:
		resp.Resp, err = client.SplitRegion(ctx, req.SplitRegion())
	case CmdEmpty:
		resp.Resp, err = &einsteindbpb.BatchCommandsEmptyResponse{}, nil
	case CmdCheckTxnStatus:
		resp.Resp, err = client.EkvCheckTxnStatus(ctx, req.CheckTxnStatus())
	case CmdCheckSecondaryLocks:
		resp.Resp, err = client.EkvCheckSecondaryLocks(ctx, req.CheckSecondaryLocks())
	case CmdTxnHeartBeat:
		resp.Resp, err = client.EkvTxnHeartBeat(ctx, req.TxnHeartBeat())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// CallDebugRPC launches a debug rpc call.
func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdDebugGetRegionProperties:
		resp.Resp, err = client.GetRegionProperties(ctx, req.DebugGetRegionProperties())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	return resp, err
}

// Lease is used to implement grpc stream timeout.
type Lease struct {
	Cancel   context.CancelFunc
	deadline int64 // A time.UnixNano value, if time.Now().UnixNano() > deadline, cancel() would be called.
}

// Recv overrides the stream client Recv() function.
func (resp *CopStreamResponse) Recv() (*interlock.Response, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.EinsteinDB_CoprocessorStreamClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.Trace(err)
}

// Close closes the CopStreamResponse object.
func (resp *CopStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// Recv overrides the stream client Recv() function.
func (resp *BatchCopStreamResponse) Recv() (*interlock.BatchResponse, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.EinsteinDB_BatchCoprocessorClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.Trace(err)
}

// Close closes the CopStreamResponse object.
func (resp *BatchCopStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// CheckStreamTimeoutLoop runs periodically to check is there any stream request timeouted.
// Lease is an object to track stream requests, call this function with "go CheckStreamTimeoutLoop()"
// It is not guaranteed to call every Lease.Cancel() putting into channel when exits.
// If grpc-go supports SetDeadline(https://github.com/grpc/grpc-go/issues/2917), we can stop using this method.
func CheckStreamTimeoutLoop(ch <-chan *Lease, done <-chan struct{}) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	array := make([]*Lease, 0, 1024)

	for {
		select {
		case <-done:
		drainLoop:
			// Try my best cleaning the channel to make SendRequest which is blocking by it continues.
			for {
				select {
				case <-ch:
				default:
					break drainLoop
				}
			}
			return
		case item := <-ch:
			array = append(array, item)
		case now := <-ticker.C:
			array = keepOnlyActive(array, now.UnixNano())
		}
	}
}

// keepOnlyActive removes completed items, call cancel function for timeout items.
func keepOnlyActive(array []*Lease, now int64) []*Lease {
	idx := 0
	for i := 0; i < len(array); i++ {
		item := array[i]
		deadline := atomic.LoadInt64(&item.deadline)
		if deadline == 0 || deadline > now {
			array[idx] = array[i]
			idx++
		} else {
			item.Cancel()
		}
	}
	return array[:idx]
}
