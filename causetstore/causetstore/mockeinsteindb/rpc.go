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

package mockeinsteindb

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/debugpb"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/errorpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/ekv"
)

// For gofail injection.
var undeterminedErr = terror.ErrResultUndetermined

const requestMaxSize = 8 * 1024 * 1024

func checkGoContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func convertToKeyError(err error) *ekvrpcpb.KeyError {
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		return &ekvrpcpb.KeyError{
			Locked: &ekvrpcpb.LockInfo{
				Key:                locked.Key.Raw(),
				PrimaryLock:        locked.Primary,
				LockVersion:        locked.StartTS,
				LockTtl:            locked.TTL,
				TxnSize:            locked.TxnSize,
				LockType:           locked.LockType,
				LockForUFIDelateTs: locked.ForUFIDelateTS,
			},
		}
	}
	if alreadyExist, ok := errors.Cause(err).(*ErrKeyAlreadyExist); ok {
		return &ekvrpcpb.KeyError{
			AlreadyExist: &ekvrpcpb.AlreadyExist{
				Key: alreadyExist.Key,
			},
		}
	}
	if writeConflict, ok := errors.Cause(err).(*ErrConflict); ok {
		return &ekvrpcpb.KeyError{
			Conflict: &ekvrpcpb.WriteConflict{
				Key:              writeConflict.Key,
				ConflictTs:       writeConflict.ConflictTS,
				ConflictCommitTs: writeConflict.ConflictCommitTS,
				StartTs:          writeConflict.StartTS,
			},
		}
	}
	if dead, ok := errors.Cause(err).(*ErrDeadlock); ok {
		return &ekvrpcpb.KeyError{
			Deadlock: &ekvrpcpb.Deadlock{
				LockTs:          dead.LockTS,
				LockKey:         dead.LockKey,
				DeadlockKeyHash: dead.DealockKeyHash,
			},
		}
	}
	if retryable, ok := errors.Cause(err).(ErrRetryable); ok {
		return &ekvrpcpb.KeyError{
			Retryable: retryable.Error(),
		}
	}
	if expired, ok := errors.Cause(err).(*ErrCommitTSExpired); ok {
		return &ekvrpcpb.KeyError{
			CommitTsExpired: &expired.CommitTsExpired,
		}
	}
	if tmp, ok := errors.Cause(err).(*ErrTxnNotFound); ok {
		return &ekvrpcpb.KeyError{
			TxnNotFound: &tmp.TxnNotFound,
		}
	}
	return &ekvrpcpb.KeyError{
		Abort: err.Error(),
	}
}

func convertToKeyErrors(errs []error) []*ekvrpcpb.KeyError {
	var keyErrors = make([]*ekvrpcpb.KeyError, 0)
	for _, err := range errs {
		if err != nil {
			keyErrors = append(keyErrors, convertToKeyError(err))
		}
	}
	return keyErrors
}

func convertToPbPairs(pairs []Pair) []*ekvrpcpb.EkvPair {
	ekvPairs := make([]*ekvrpcpb.EkvPair, 0, len(pairs))
	for _, p := range pairs {
		var ekvPair *ekvrpcpb.EkvPair
		if p.Err == nil {
			ekvPair = &ekvrpcpb.EkvPair{
				Key:   p.Key,
				Value: p.Value,
			}
		} else {
			ekvPair = &ekvrpcpb.EkvPair{
				Error: convertToKeyError(p.Err),
			}
		}
		ekvPairs = append(ekvPairs, ekvPair)
	}
	return ekvPairs
}

// rpcHandler mocks einsteindb's side handler behavior. In general, you may assume
// EinsteinDB just translate the logic from Go to Rust.
type rpcHandler struct {
	cluster   *Cluster
	mvccStore MVCCStore

	// storeID stores id for current request
	storeID uint64
	// startKey is used for handling normal request.
	startKey []byte
	endKey   []byte
	// rawStartKey is used for handling interlock request.
	rawStartKey []byte
	rawEndKey   []byte
	// isolationLevel is used for current request.
	isolationLevel ekvrpcpb.IsolationLevel
	resolvedLocks  []uint64
}

func isTiFlashStore(causetstore *spacetimepb.CausetStore) bool {
	for _, l := range causetstore.GetLabels() {
		if l.GetKey() == "engine" && l.GetValue() == "tiflash" {
			return true
		}
	}
	return false
}

func (h *rpcHandler) checkRequestContext(ctx *ekvrpcpb.Context) *errorpb.Error {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != h.storeID {
		return &errorpb.Error{
			Message:       *proto.String("causetstore not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	region, leaderID := h.cluster.GetRegion(ctx.GetRegionId())
	// No region found.
	if region == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	var storePeer, leaderPeer *spacetimepb.Peer
	for _, p := range region.Peers {
		if p.GetStoreId() == h.storeID {
			storePeer = p
		}
		if p.GetId() == leaderID {
			leaderPeer = p
		}
	}
	// The CausetStore does not contain a Peer of the Region.
	if storePeer == nil {
		return &errorpb.Error{
			Message: *proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// No leader.
	if leaderPeer == nil {
		return &errorpb.Error{
			Message: *proto.String("no leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// The Peer on the CausetStore is not leader. If it's tiflash causetstore , we pass this check.
	if storePeer.GetId() != leaderPeer.GetId() && !isTiFlashStore(h.cluster.GetStore(storePeer.GetStoreId())) {
		return &errorpb.Error{
			Message: *proto.String("not leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: *proto.Uint64(ctx.GetRegionId()),
				Leader:   leaderPeer,
			},
		}
	}
	// Region epoch does not match.
	if !proto.Equal(region.GetRegionEpoch(), ctx.GetRegionEpoch()) {
		nextRegion, _ := h.cluster.GetRegionByKey(region.GetEndKey())
		currentRegions := []*spacetimepb.Region{region}
		if nextRegion != nil {
			currentRegions = append(currentRegions, nextRegion)
		}
		return &errorpb.Error{
			Message: *proto.String("epoch not match"),
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: currentRegions,
			},
		}
	}
	h.startKey, h.endKey = region.StartKey, region.EndKey
	h.isolationLevel = ctx.IsolationLevel
	h.resolvedLocks = ctx.ResolvedLocks
	return nil
}

func (h *rpcHandler) checkRequestSize(size int) *errorpb.Error {
	// EinsteinDB has a limitation on raft log size.
	// mockeinsteindb has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (h *rpcHandler) checkRequest(ctx *ekvrpcpb.Context, size int) *errorpb.Error {
	if err := h.checkRequestContext(ctx); err != nil {
		return err
	}
	return h.checkRequestSize(size)
}

func (h *rpcHandler) checkKeyInRegion(key []byte) bool {
	return regionContains(h.startKey, h.endKey, NewMvccKey(key))
}

func (h *rpcHandler) handleEkvGet(req *ekvrpcpb.GetRequest) *ekvrpcpb.GetResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("EkvGet: key not in region")
	}

	val, err := h.mvccStore.Get(req.Key, req.GetVersion(), h.isolationLevel, req.Context.GetResolvedLocks())
	if err != nil {
		return &ekvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}
	}
	return &ekvrpcpb.GetResponse{
		Value: val,
	}
}

func (h *rpcHandler) handleEkvScan(req *ekvrpcpb.ScanRequest) *ekvrpcpb.ScanResponse {
	endKey := MvccKey(h.endKey).Raw()
	var pairs []Pair
	if !req.Reverse {
		if !h.checkKeyInRegion(req.GetStartKey()) {
			panic("EkvScan: startKey not in region")
		}
		if len(req.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.EndKey), h.endKey) < 0) {
			endKey = req.EndKey
		}
		pairs = h.mvccStore.Scan(req.GetStartKey(), endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	} else {
		// EinsteinDB use range [end_key, start_key) for reverse scan.
		// Should use the req.EndKey to check in region.
		if !h.checkKeyInRegion(req.GetEndKey()) {
			panic("EkvScan: startKey not in region")
		}

		// EinsteinDB use range [end_key, start_key) for reverse scan.
		// So the req.StartKey actually is the end_key.
		if len(req.StartKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.StartKey), h.endKey) < 0) {
			endKey = req.StartKey
		}

		pairs = h.mvccStore.ReverseScan(req.EndKey, endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	}

	return &ekvrpcpb.ScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleEkvPrewrite(req *ekvrpcpb.PrewriteRequest) *ekvrpcpb.PrewriteResponse {
	regionID := req.Context.RegionId
	h.cluster.handleDelay(req.StartVersion, regionID)

	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("EkvPrewrite: key not in region")
		}
	}
	errs := h.mvccStore.Prewrite(req)
	return &ekvrpcpb.PrewriteResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) handleEkvPessimisticLock(req *ekvrpcpb.PessimisticLockRequest) *ekvrpcpb.PessimisticLockResponse {
	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("EkvPessimisticLock: key not in region")
		}
	}
	startTS := req.StartVersion
	regionID := req.Context.RegionId
	h.cluster.handleDelay(startTS, regionID)
	return h.mvccStore.PessimisticLock(req)
}

func simulateServerSideWaitLock(errs []error) {
	for _, err := range errs {
		if _, ok := err.(*ErrLocked); ok {
			time.Sleep(time.Millisecond * 5)
			break
		}
	}
}

func (h *rpcHandler) handleEkvPessimisticRollback(req *ekvrpcpb.PessimisticRollbackRequest) *ekvrpcpb.PessimisticRollbackResponse {
	for _, key := range req.Keys {
		if !h.checkKeyInRegion(key) {
			panic("EkvPessimisticRollback: key not in region")
		}
	}
	errs := h.mvccStore.PessimisticRollback(req.Keys, req.StartVersion, req.ForUFIDelateTs)
	return &ekvrpcpb.PessimisticRollbackResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h *rpcHandler) handleEkvCommit(req *ekvrpcpb.CommitRequest) *ekvrpcpb.CommitResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("EkvCommit: key not in region")
		}
	}
	var resp ekvrpcpb.CommitResponse
	err := h.mvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (h *rpcHandler) handleEkvCleanup(req *ekvrpcpb.CleanupRequest) *ekvrpcpb.CleanupResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("EkvCleanup: key not in region")
	}
	var resp ekvrpcpb.CleanupResponse
	err := h.mvccStore.Cleanup(req.Key, req.GetStartVersion(), req.GetCurrentTs())
	if err != nil {
		if commitTS, ok := errors.Cause(err).(ErrAlreadyCommitted); ok {
			resp.CommitVersion = uint64(commitTS)
		} else {
			resp.Error = convertToKeyError(err)
		}
	}
	return &resp
}

func (h *rpcHandler) handleEkvCheckTxnStatus(req *ekvrpcpb.CheckTxnStatusRequest) *ekvrpcpb.CheckTxnStatusResponse {
	if !h.checkKeyInRegion(req.PrimaryKey) {
		panic("EkvCheckTxnStatus: key not in region")
	}
	var resp ekvrpcpb.CheckTxnStatusResponse
	ttl, commitTS, action, err := h.mvccStore.CheckTxnStatus(req.GetPrimaryKey(), req.GetLockTs(), req.GetCallerStartTs(), req.GetCurrentTs(), req.GetRollbackIfNotExist())
	if err != nil {
		resp.Error = convertToKeyError(err)
	} else {
		resp.LockTtl, resp.CommitVersion, resp.CausetAction = ttl, commitTS, action
	}
	return &resp
}

func (h *rpcHandler) handleTxnHeartBeat(req *ekvrpcpb.TxnHeartBeatRequest) *ekvrpcpb.TxnHeartBeatResponse {
	if !h.checkKeyInRegion(req.PrimaryLock) {
		panic("EkvTxnHeartBeat: key not in region")
	}
	var resp ekvrpcpb.TxnHeartBeatResponse
	ttl, err := h.mvccStore.TxnHeartBeat(req.PrimaryLock, req.StartVersion, req.AdviseLockTtl)
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	resp.LockTtl = ttl
	return &resp
}

func (h *rpcHandler) handleEkvBatchGet(req *ekvrpcpb.BatchGetRequest) *ekvrpcpb.BatchGetResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("EkvBatchGet: key not in region")
		}
	}
	pairs := h.mvccStore.BatchGet(req.Keys, req.GetVersion(), h.isolationLevel, req.Context.GetResolvedLocks())
	return &ekvrpcpb.BatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleMvccGetByKey(req *ekvrpcpb.MvccGetByKeyRequest) *ekvrpcpb.MvccGetByKeyResponse {
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		return &ekvrpcpb.MvccGetByKeyResponse{
			Error: "not implement",
		}
	}

	if !h.checkKeyInRegion(req.Key) {
		panic("MvccGetByKey: key not in region")
	}
	var resp ekvrpcpb.MvccGetByKeyResponse
	resp.Info = debugger.MvccGetByKey(req.Key)
	return &resp
}

func (h *rpcHandler) handleMvccGetByStartTS(req *ekvrpcpb.MvccGetByStartTsRequest) *ekvrpcpb.MvccGetByStartTsResponse {
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		return &ekvrpcpb.MvccGetByStartTsResponse{
			Error: "not implement",
		}
	}
	var resp ekvrpcpb.MvccGetByStartTsResponse
	resp.Info, resp.Key = debugger.MvccGetByStartTS(req.StartTs)
	return &resp
}

func (h *rpcHandler) handleEkvBatchRollback(req *ekvrpcpb.BatchRollbackRequest) *ekvrpcpb.BatchRollbackResponse {
	err := h.mvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &ekvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &ekvrpcpb.BatchRollbackResponse{}
}

func (h *rpcHandler) handleEkvScanLock(req *ekvrpcpb.ScanLockRequest) *ekvrpcpb.ScanLockResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	locks, err := h.mvccStore.ScanLock(startKey, endKey, req.GetMaxVersion())
	if err != nil {
		return &ekvrpcpb.ScanLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &ekvrpcpb.ScanLockResponse{
		Locks: locks,
	}
}

func (h *rpcHandler) handleEkvResolveLock(req *ekvrpcpb.ResolveLockRequest) *ekvrpcpb.ResolveLockResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.ResolveLock(startKey, endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &ekvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &ekvrpcpb.ResolveLockResponse{}
}

func (h *rpcHandler) handleEkvGC(req *ekvrpcpb.GCRequest) *ekvrpcpb.GCResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.GC(startKey, endKey, req.GetSafePoint())
	if err != nil {
		return &ekvrpcpb.GCResponse{
			Error: convertToKeyError(err),
		}
	}
	return &ekvrpcpb.GCResponse{}
}

func (h *rpcHandler) handleEkvDeleteRange(req *ekvrpcpb.DeleteRangeRequest) *ekvrpcpb.DeleteRangeResponse {
	if !h.checkKeyInRegion(req.StartKey) {
		panic("EkvDeleteRange: key not in region")
	}
	var resp ekvrpcpb.DeleteRangeResponse
	err := h.mvccStore.DeleteRange(req.StartKey, req.EndKey)
	if err != nil {
		resp.Error = err.Error()
	}
	return &resp
}

func (h *rpcHandler) handleEkvRawGet(req *ekvrpcpb.RawGetRequest) *ekvrpcpb.RawGetResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &ekvrpcpb.RawGetResponse{
			Error: "not implemented",
		}
	}
	return &ekvrpcpb.RawGetResponse{
		Value: rawKV.RawGet(req.GetKey()),
	}
}

func (h *rpcHandler) handleEkvRawBatchGet(req *ekvrpcpb.RawBatchGetRequest) *ekvrpcpb.RawBatchGetResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		// TODO should we add error ?
		return &ekvrpcpb.RawBatchGetResponse{
			RegionError: &errorpb.Error{
				Message: "not implemented",
			},
		}
	}
	values := rawKV.RawBatchGet(req.Keys)
	ekvPairs := make([]*ekvrpcpb.EkvPair, len(values))
	for i, key := range req.Keys {
		ekvPairs[i] = &ekvrpcpb.EkvPair{
			Key:   key,
			Value: values[i],
		}
	}
	return &ekvrpcpb.RawBatchGetResponse{
		Pairs: ekvPairs,
	}
}

func (h *rpcHandler) handleEkvRawPut(req *ekvrpcpb.RawPutRequest) *ekvrpcpb.RawPutResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &ekvrpcpb.RawPutResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawPut(req.GetKey(), req.GetValue())
	return &ekvrpcpb.RawPutResponse{}
}

func (h *rpcHandler) handleEkvRawBatchPut(req *ekvrpcpb.RawBatchPutRequest) *ekvrpcpb.RawBatchPutResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &ekvrpcpb.RawBatchPutResponse{
			Error: "not implemented",
		}
	}
	keys := make([][]byte, 0, len(req.Pairs))
	values := make([][]byte, 0, len(req.Pairs))
	for _, pair := range req.Pairs {
		keys = append(keys, pair.Key)
		values = append(values, pair.Value)
	}
	rawKV.RawBatchPut(keys, values)
	return &ekvrpcpb.RawBatchPutResponse{}
}

func (h *rpcHandler) handleEkvRawDelete(req *ekvrpcpb.RawDeleteRequest) *ekvrpcpb.RawDeleteResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &ekvrpcpb.RawDeleteResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawDelete(req.GetKey())
	return &ekvrpcpb.RawDeleteResponse{}
}

func (h *rpcHandler) handleEkvRawBatchDelete(req *ekvrpcpb.RawBatchDeleteRequest) *ekvrpcpb.RawBatchDeleteResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &ekvrpcpb.RawBatchDeleteResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawBatchDelete(req.Keys)
	return &ekvrpcpb.RawBatchDeleteResponse{}
}

func (h *rpcHandler) handleEkvRawDeleteRange(req *ekvrpcpb.RawDeleteRangeRequest) *ekvrpcpb.RawDeleteRangeResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &ekvrpcpb.RawDeleteRangeResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawDeleteRange(req.GetStartKey(), req.GetEndKey())
	return &ekvrpcpb.RawDeleteRangeResponse{}
}

func (h *rpcHandler) handleEkvRawScan(req *ekvrpcpb.RawScanRequest) *ekvrpcpb.RawScanResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		errStr := "not implemented"
		return &ekvrpcpb.RawScanResponse{
			RegionError: &errorpb.Error{
				Message: errStr,
			},
		}
	}

	var pairs []Pair
	if req.Reverse {
		lowerBound := h.startKey
		if bytes.Compare(req.EndKey, lowerBound) > 0 {
			lowerBound = req.EndKey
		}
		pairs = rawKV.RawReverseScan(
			req.StartKey,
			lowerBound,
			int(req.GetLimit()),
		)
	} else {
		upperBound := h.endKey
		if len(req.EndKey) > 0 && (len(upperBound) == 0 || bytes.Compare(req.EndKey, upperBound) < 0) {
			upperBound = req.EndKey
		}
		pairs = rawKV.RawScan(
			req.StartKey,
			upperBound,
			int(req.GetLimit()),
		)
	}

	return &ekvrpcpb.RawScanResponse{
		Ekvs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleSplitRegion(req *ekvrpcpb.SplitRegionRequest) *ekvrpcpb.SplitRegionResponse {
	keys := req.GetSplitKeys()
	resp := &ekvrpcpb.SplitRegionResponse{Regions: make([]*spacetimepb.Region, 0, len(keys)+1)}
	for i, key := range keys {
		k := NewMvccKey(key)
		region, _ := h.cluster.GetRegionByKey(k)
		if bytes.Equal(region.GetStartKey(), key) {
			continue
		}
		if i == 0 {
			// Set the leftmost region.
			resp.Regions = append(resp.Regions, region)
		}
		newRegionID, newPeerIDs := h.cluster.AllocID(), h.cluster.AllocIDs(len(region.Peers))
		newRegion := h.cluster.SplitRaw(region.GetId(), newRegionID, k, newPeerIDs, newPeerIDs[0])
		resp.Regions = append(resp.Regions, newRegion)
	}
	return resp
}

func drainRowsFromInterlockingDirectorate(ctx context.Context, e interlock, req *fidelpb.PosetDagRequest) (fidelpb.Chunk, error) {
	var chunk fidelpb.Chunk
	for {
		event, err := e.Next(ctx)
		if err != nil {
			return chunk, errors.Trace(err)
		}
		if event == nil {
			return chunk, nil
		}
		for _, offset := range req.OutputOffsets {
			chunk.RowsData = append(chunk.RowsData, event[offset]...)
		}
	}
}

func (h *rpcHandler) handleBatchCopRequest(ctx context.Context, req *interlock.BatchRequest) (*mockBatchCoFIDelataClient, error) {
	client := &mockBatchCoFIDelataClient{}
	for _, ri := range req.Regions {
		cop := interlock.Request{
			Tp:      ekv.ReqTypePosetDag,
			Data:    req.Data,
			StartTs: req.StartTs,
			Ranges:  ri.Ranges,
		}
		_, exec, posetPosetDagReq, err := h.buildPosetDagInterlockingDirectorate(&cop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunk, err := drainRowsFromInterlockingDirectorate(ctx, exec, posetPosetDagReq)
		if err != nil {
			return nil, errors.Trace(err)
		}
		client.chunks = append(client.chunks, chunk)
	}
	return client, nil
}

// Client is a client that sends RPC.
// This is same with einsteindb.Client, define again for avoid circle import.
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error)
}

// RPCClient sends ekv RPC calls to mock cluster. RPCClient mocks the behavior of
// a rpc client at einsteindb's side.
type RPCClient struct {
	Cluster       *Cluster
	MvccStore     MVCCStore
	streamTimeout chan *einsteindbrpc.Lease
	done          chan struct{}
	// rpcCli uses to redirects RPC request to MilevaDB rpc server, It is only use for test.
	// Mock MilevaDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
	// sync.Once uses to avoid concurrency initialize rpcCli.
	sync.Once
	rpcCli Client
}

// NewRPCClient creates an RPCClient.
// Note that close the RPCClient may close the underlying MvccStore.
func NewRPCClient(cluster *Cluster, mvccStore MVCCStore) *RPCClient {
	ch := make(chan *einsteindbrpc.Lease, 1024)
	done := make(chan struct{})
	go einsteindbrpc.CheckStreamTimeoutLoop(ch, done)
	return &RPCClient{
		Cluster:       cluster,
		MvccStore:     mvccStore,
		streamTimeout: ch,
		done:          done,
	}
}

func (c *RPCClient) getAndCheckStoreByAddr(addr string) (*spacetimepb.CausetStore, error) {
	causetstore, err := c.Cluster.GetAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	if causetstore == nil {
		return nil, errors.New("connect fail")
	}
	if causetstore.GetState() == spacetimepb.StoreState_Offline ||
		causetstore.GetState() == spacetimepb.StoreState_Tombstone {
		return nil, errors.New("connection refused")
	}
	return causetstore, nil
}

func (c *RPCClient) checkArgs(ctx context.Context, addr string) (*rpcHandler, error) {
	if err := checkGoContext(ctx); err != nil {
		return nil, err
	}

	causetstore, err := c.getAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	handler := &rpcHandler{
		cluster:   c.Cluster,
		mvccStore: c.MvccStore,
		// set causetstore id for current request
		storeID: causetstore.GetId(),
	}
	return handler, nil
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

// SendRequest sends a request to mock cluster.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *einsteindbrpc.Request, timeout time.Duration) (*einsteindbrpc.Response, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("RPCClient.SendRequest", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	failpoint.Inject("rpcServerBusy", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(einsteindbrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}))
		}
	})

	// increase coverage for mock einsteindb
	_ = req.Type.String()
	_ = req.ToBatchCommandsRequest()

	reqCtx := &req.Context
	resp := &einsteindbrpc.Response{}
	// When the causetstore type is MilevaDB, the request should handle over to MilevaDB rpc server to handle.
	if req.StoreTp == ekv.MilevaDB {
		return c.redirectRequestToRPCServer(ctx, addr, req, timeout)
	}

	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	switch req.Type {
	case einsteindbrpc.CmdGet:
		r := req.Get()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.GetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvGet(r)
	case einsteindbrpc.CmdScan:
		r := req.Scan()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.ScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvScan(r)

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
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.PrewriteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvPrewrite(r)
	case einsteindbrpc.CmdPessimisticLock:
		r := req.PessimisticLock()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.PessimisticLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvPessimisticLock(r)
	case einsteindbrpc.CmdPessimisticRollback:
		r := req.PessimisticRollback()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.PessimisticRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvPessimisticRollback(r)
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

		r := req.Commit()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.CommitResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvCommit(r)
		failpoint.Inject("rpcCommitTimeout", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(nil, undeterminedErr)
			}
		})
	case einsteindbrpc.CmdCleanup:
		r := req.Cleanup()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.CleanupResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvCleanup(r)
	case einsteindbrpc.CmdCheckTxnStatus:
		r := req.CheckTxnStatus()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.CheckTxnStatusResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvCheckTxnStatus(r)
	case einsteindbrpc.CmdTxnHeartBeat:
		r := req.TxnHeartBeat()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.TxnHeartBeatResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleTxnHeartBeat(r)
	case einsteindbrpc.CmdBatchGet:
		r := req.BatchGet()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.BatchGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvBatchGet(r)
	case einsteindbrpc.CmdBatchRollback:
		r := req.BatchRollback()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.BatchRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvBatchRollback(r)
	case einsteindbrpc.CmdScanLock:
		r := req.ScanLock()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.ScanLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvScanLock(r)
	case einsteindbrpc.CmdResolveLock:
		r := req.ResolveLock()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.ResolveLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvResolveLock(r)
	case einsteindbrpc.CmdGC:
		r := req.GC()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.GCResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvGC(r)
	case einsteindbrpc.CmdDeleteRange:
		r := req.DeleteRange()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.DeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvDeleteRange(r)
	case einsteindbrpc.CmdRawGet:
		r := req.RawGet()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvRawGet(r)
	case einsteindbrpc.CmdRawBatchGet:
		r := req.RawBatchGet()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawBatchGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvRawBatchGet(r)
	case einsteindbrpc.CmdRawPut:
		r := req.RawPut()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawPutResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvRawPut(r)
	case einsteindbrpc.CmdRawBatchPut:
		r := req.RawBatchPut()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawBatchPutResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvRawBatchPut(r)
	case einsteindbrpc.CmdRawDelete:
		r := req.RawDelete()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawDeleteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvRawDelete(r)
	case einsteindbrpc.CmdRawBatchDelete:
		r := req.RawBatchDelete()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawBatchDeleteResponse{RegionError: err}
		}
		resp.Resp = handler.handleEkvRawBatchDelete(r)
	case einsteindbrpc.CmdRawDeleteRange:
		r := req.RawDeleteRange()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawDeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvRawDeleteRange(r)
	case einsteindbrpc.CmdRawScan:
		r := req.RawScan()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.RawScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleEkvRawScan(r)
	case einsteindbrpc.CmdUnsafeDestroyRange:
		panic("unimplemented")
	case einsteindbrpc.CmdRegisterLockObserver:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdCheckLockObserver:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdRemoveLockObserver:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdPhysicalScanLock:
		return nil, errors.New("unimplemented")
	case einsteindbrpc.CmdCop:
		r := req.Causet()
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &interlock.Response{RegionError: err}
			return resp, nil
		}
		handler.rawStartKey = MvccKey(handler.startKey).Raw()
		handler.rawEndKey = MvccKey(handler.endKey).Raw()
		var res *interlock.Response
		switch r.GetTp() {
		case ekv.ReqTypePosetDag:
			res = handler.handleCoFIDelAGRequest(r)
		case ekv.ReqTypeAnalyze:
			res = handler.handleCopAnalyzeRequest(r)
		case ekv.ReqTypeChecksum:
			res = handler.handleCopChecksumRequest(r)
		default:
			panic(fmt.Sprintf("unknown interlock request type: %v", r.GetTp()))
		}
		resp.Resp = res
	case einsteindbrpc.CmdBatchCop:
		failpoint.Inject("BatchCopCancelled", func(value failpoint.Value) {
			if value.(bool) {
				failpoint.Return(nil, context.Canceled)
			}
		})

		failpoint.Inject("BatchCopRpcErr"+addr, func(value failpoint.Value) {
			if value.(string) == addr {
				failpoint.Return(nil, errors.New("rpc error"))
			}
		})
		r := req.BatchCop()
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &einsteindbrpc.BatchCopStreamResponse{
				EinsteinDB_BatchCoprocessorClient: &mockBathCopErrClient{Error: err},
				BatchResponse: &interlock.BatchResponse{
					OtherError: err.Message,
				},
			}
			return resp, nil
		}
		ctx1, cancel := context.WithCancel(ctx)
		batchCopStream, err := handler.handleBatchCopRequest(ctx1, r)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		batchResp := &einsteindbrpc.BatchCopStreamResponse{EinsteinDB_BatchCoprocessorClient: batchCopStream}
		batchResp.Lease.Cancel = cancel
		batchResp.Timeout = timeout
		c.streamTimeout <- &batchResp.Lease

		first, err := batchResp.Recv()
		if err != nil {
			return nil, errors.Trace(err)
		}
		batchResp.BatchResponse = first
		resp.Resp = batchResp
	case einsteindbrpc.CmdCopStream:
		r := req.Causet()
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &einsteindbrpc.CopStreamResponse{
				EinsteinDB_CoprocessorStreamClient: &mockCopStreamErrClient{Error: err},
				Response: &interlock.Response{
					RegionError: err,
				},
			}
			return resp, nil
		}
		handler.rawStartKey = MvccKey(handler.startKey).Raw()
		handler.rawEndKey = MvccKey(handler.endKey).Raw()
		ctx1, cancel := context.WithCancel(ctx)
		copStream, err := handler.handleCopStream(ctx1, r)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}

		streamResp := &einsteindbrpc.CopStreamResponse{
			EinsteinDB_CoprocessorStreamClient: copStream,
		}
		streamResp.Lease.Cancel = cancel
		streamResp.Timeout = timeout
		c.streamTimeout <- &streamResp.Lease

		first, err := streamResp.Recv()
		if err != nil {
			return nil, errors.Trace(err)
		}
		streamResp.Response = first
		resp.Resp = streamResp
	case einsteindbrpc.CmdMvccGetByKey:
		r := req.MvccGetByKey()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.MvccGetByKeyResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleMvccGetByKey(r)
	case einsteindbrpc.CmdMvccGetByStartTs:
		r := req.MvccGetByStartTs()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.MvccGetByStartTsResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleMvccGetByStartTS(r)
	case einsteindbrpc.CmdSplitRegion:
		r := req.SplitRegion()
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &ekvrpcpb.SplitRegionResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = handler.handleSplitRegion(r)
	// DebugGetRegionProperties is for fast analyze in mock einsteindb.
	case einsteindbrpc.CmdDebugGetRegionProperties:
		r := req.DebugGetRegionProperties()
		region, _ := c.Cluster.GetRegion(r.RegionId)
		var reqCtx ekvrpcpb.Context
		scanResp := handler.handleEkvScan(&ekvrpcpb.ScanRequest{
			Context:  &reqCtx,
			StartKey: MvccKey(region.StartKey).Raw(),
			EndKey:   MvccKey(region.EndKey).Raw(),
			Version:  math.MaxUint64,
			Limit:    math.MaxUint32})
		resp.Resp = &debugpb.GetRegionPropertiesResponse{
			Props: []*debugpb.Property{{
				Name:  "mvcc.num_rows",
				Value: strconv.Itoa(len(scanResp.Pairs)),
			}}}
	default:
		return nil, errors.Errorf("unsupported this request type %v", req.Type)
	}
	return resp, nil
}

// Close closes the client.
func (c *RPCClient) Close() error {
	close(c.done)

	var err error
	if c.MvccStore != nil {
		err = c.MvccStore.Close()
		if err != nil {
			return err
		}
	}

	if c.rpcCli != nil {
		err = c.rpcCli.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
