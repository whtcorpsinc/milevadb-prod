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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"go.uber.org/zap"
)

// batchCopTask comprises of multiple copTask that will send to same causetstore.
type batchCopTask struct {
	storeAddr string
	cmdType   einsteindbrpc.CmdType

	CausetTasks []copTaskAndRPCContext
}

type batchCopResponse struct {
	pbResp *interlock.BatchResponse
	detail *CopRuntimeStats

	// batch Causet Response is yet to return startKey. So batchCop cannot retry partially.
	startKey ekv.Key
	err      error
	respSize int64
	respTime time.Duration
}

// GetData implements the ekv.ResultSubset GetData interface.
func (rs *batchCopResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the ekv.ResultSubset GetStartKey interface.
func (rs *batchCopResponse) GetStartKey() ekv.Key {
	return rs.startKey
}

// GetInterDircDetails is unavailable currently, because TiFlash has not collected exec details for batch cop.
// TODO: Will fix in near future.
func (rs *batchCopResponse) GetCopRuntimeStats() *CopRuntimeStats {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *batchCopResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofInterDircDetails)
	}
	if rs.pbResp != nil {
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	return rs.respSize
}

func (rs *batchCopResponse) RespTime() time.Duration {
	return rs.respTime
}

type copTaskAndRPCContext struct {
	task *copTask
	ctx  *RPCContext
}

func buildBatchCausetTasks(bo *Backoffer, cache *RegionCache, ranges *copRanges, req *ekv.Request) ([]*batchCopTask, error) {
	start := time.Now()
	const cmdType = einsteindbrpc.CmdBatchCop
	rangesLen := ranges.len()
	for {
		var tasks []*copTask
		appendTask := func(regionWithRangeInfo *KeyLocation, ranges *copRanges) {
			tasks = append(tasks, &copTask{
				region:    regionWithRangeInfo.Region,
				ranges:    ranges,
				cmdType:   cmdType,
				storeType: req.StoreType,
			})
		}

		err := splitRanges(bo, cache, ranges, appendTask)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var batchTasks []*batchCopTask

		storeTaskMap := make(map[string]*batchCopTask)
		needRetry := false
		for _, task := range tasks {
			rpcCtx, err := cache.GetTiFlashRPCContext(bo, task.region)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We should retry and generate new tasks.
			if rpcCtx == nil {
				needRetry = true
				err = bo.Backoff(BoRegionMiss, errors.New("Cannot find region or TiFlash peer"))
				logutil.BgLogger().Info("retry for TiFlash peer or region missing", zap.Uint64("region id", task.region.GetID()))
				if err != nil {
					return nil, errors.Trace(err)
				}
				break
			}
			if batchCop, ok := storeTaskMap[rpcCtx.Addr]; ok {
				batchCop.CausetTasks = append(batchCop.CausetTasks, copTaskAndRPCContext{task: task, ctx: rpcCtx})
			} else {
				batchTask := &batchCopTask{
					storeAddr:   rpcCtx.Addr,
					cmdType:     cmdType,
					CausetTasks: []copTaskAndRPCContext{{task, rpcCtx}},
				}
				storeTaskMap[rpcCtx.Addr] = batchTask
			}
		}
		if needRetry {
			continue
		}
		for _, task := range storeTaskMap {
			batchTasks = append(batchTasks, task)
		}

		if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
			logutil.BgLogger().Warn("buildBatchCausetTasks takes too much time",
				zap.Duration("elapsed", elapsed),
				zap.Int("range len", rangesLen),
				zap.Int("task len", len(batchTasks)))
		}
		einsteindbTxnRegionsNumHistogramWithBatchCoprocessor.Observe(float64(len(batchTasks)))
		return batchTasks, nil
	}
}

func (c *CopClient) sendBatch(ctx context.Context, req *ekv.Request, vars *ekv.Variables) ekv.Response {
	if req.KeepOrder || req.Desc {
		return copErrorResponse{errors.New("batch interlock cannot prove keep order or desc property")}
	}
	ctx = context.WithValue(ctx, txnStartKey, req.StartTs)
	bo := NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, vars)
	tasks, err := buildBatchCausetTasks(bo, c.causetstore.regionCache, &copRanges{mid: req.KeyRanges}, req)
	if err != nil {
		return copErrorResponse{err}
	}
	it := &batchCopIterator{
		causetstore: c.causetstore,
		req:         req,
		finishCh:    make(chan struct{}),
		vars:        vars,
		memTracker:  req.MemTracker,
		clientHelper: clientHelper{
			LockResolver:      c.causetstore.lockResolver,
			RegionCache:       c.causetstore.regionCache,
			Client:            c.causetstore.client,
			minCommitTSPushed: &minCommitTSPushed{data: make(map[uint64]struct{}, 5)},
		},
		rpcCancel: NewRPCanceller(),
	}
	ctx = context.WithValue(ctx, RPCCancellerCtxKey{}, it.rpcCancel)
	it.tasks = tasks
	it.respChan = make(chan *batchCopResponse, 2048)
	go it.run(ctx)
	return it
}

type batchCopIterator struct {
	clientHelper

	causetstore *einsteindbStore
	req         *ekv.Request
	finishCh    chan struct{}

	tasks []*batchCopTask

	// Batch results are stored in respChan.
	respChan chan *batchCopResponse

	vars *ekv.Variables

	memTracker *memory.Tracker

	replicaReadSeed uint32

	rpcCancel *RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32
}

func (b *batchCopIterator) run(ctx context.Context) {
	// We run workers for every batch cop.
	for _, task := range b.tasks {
		b.wg.Add(1)
		bo := NewBackofferWithVars(ctx, copNextMaxBackoff, b.vars)
		go b.handleTask(ctx, bo, task)
	}
	b.wg.Wait()
	close(b.respChan)
}

// Next returns next interlock result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (b *batchCopIterator) Next(ctx context.Context) (ekv.ResultSubset, error) {
	var (
		resp   *batchCopResponse
		ok     bool
		closed bool
	)

	// Get next fetched resp from chan
	resp, ok, closed = b.recvFromRespCh(ctx)
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := b.causetstore.CheckVisibility(b.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (b *batchCopIterator) recvFromRespCh(ctx context.Context) (resp *batchCopResponse, ok bool, exit bool) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-b.respChan:
			return
		case <-ticker.C:
			if atomic.LoadUint32(b.vars.Killed) == 1 {
				resp = &batchCopResponse{err: ErrQueryInterrupted}
				ok = true
				return
			}
		case <-b.finishCh:
			exit = true
			return
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
				close(b.finishCh)
			}
			exit = true
			return
		}
	}
}

// Close releases the resource.
func (b *batchCopIterator) Close() error {
	if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		close(b.finishCh)
	}
	b.rpcCancel.CancelAll()
	b.wg.Wait()
	return nil
}

func (b *batchCopIterator) handleTask(ctx context.Context, bo *Backoffer, task *batchCopTask) {
	logutil.BgLogger().Debug("handle batch task")
	tasks := []*batchCopTask{task}
	for idx := 0; idx < len(tasks); idx++ {
		ret, err := b.handleTaskOnce(ctx, bo, tasks[idx])
		if err != nil {
			resp := &batchCopResponse{err: errors.Trace(err), detail: new(CopRuntimeStats)}
			b.sendToRespCh(resp)
			break
		}
		tasks = append(tasks, ret...)
	}
	b.wg.Done()
}

// Merge all ranges and request again.
func (b *batchCopIterator) retryBatchCopTask(ctx context.Context, bo *Backoffer, batchTask *batchCopTask) ([]*batchCopTask, error) {
	ranges := &copRanges{}
	for _, taskCtx := range batchTask.CausetTasks {
		taskCtx.task.ranges.do(func(ran *ekv.KeyRange) {
			ranges.mid = append(ranges.mid, *ran)
		})
	}
	return buildBatchCausetTasks(bo, b.RegionCache, ranges, b.req)
}

func (b *batchCopIterator) handleTaskOnce(ctx context.Context, bo *Backoffer, task *batchCopTask) ([]*batchCopTask, error) {
	logutil.BgLogger().Debug("handle batch task once")
	sender := NewRegionBatchRequestSender(b.causetstore.regionCache, b.causetstore.client)
	var regionInfos []*interlock.RegionInfo
	for _, task := range task.CausetTasks {
		regionInfos = append(regionInfos, &interlock.RegionInfo{
			RegionId: task.task.region.id,
			RegionEpoch: &spacetimepb.RegionEpoch{
				ConfVer: task.task.region.confVer,
				Version: task.task.region.ver,
			},
			Ranges: task.task.ranges.toPBRanges(),
		})
	}

	copReq := interlock.BatchRequest{
		Tp:        b.req.Tp,
		StartTs:   b.req.StartTs,
		Data:      b.req.Data,
		SchemaVer: b.req.SchemaVar,
		Regions:   regionInfos,
	}

	req := einsteindbrpc.NewRequest(task.cmdType, &copReq, ekvrpcpb.Context{
		IsolationLevel: pbIsolationLevel(b.req.IsolationLevel),
		Priority:       ekvPriorityToCommandPri(b.req.Priority),
		NotFillCache:   b.req.NotFillCache,
		HandleTime:     true,
		ScanDetail:     true,
		TaskId:         b.req.TaskID,
	})
	req.StoreTp = ekv.TiFlash

	logutil.BgLogger().Debug("send batch request to ", zap.String("req info", req.String()), zap.Int("cop task len", len(task.CausetTasks)))
	resp, retry, cancel, err := sender.sendStreamReqToAddr(bo, task.CausetTasks, req, ReadTimeoutUltraLong)
	// If there are causetstore errors, we should retry for all regions.
	if retry {
		return b.retryBatchCopTask(ctx, bo, task)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer cancel()
	return nil, b.handleStreamedBatchCopResponse(ctx, bo, resp.Resp.(*einsteindbrpc.BatchCopStreamResponse), task)
}

func (b *batchCopIterator) handleStreamedBatchCopResponse(ctx context.Context, bo *Backoffer, response *einsteindbrpc.BatchCopStreamResponse, task *batchCopTask) (err error) {
	defer response.Close()
	resp := response.BatchResponse
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return
	}
	for {
		err = b.handleBatchCopResponse(bo, resp, task)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err = response.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}

			if err1 := bo.Backoff(boEinsteinDBRPC, errors.Errorf("recv stream response error: %v, task causetstore addr: %s", err, task.storeAddr)); err1 != nil {
				return errors.Trace(err)
			}

			// No interlock.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return errors.Trace(err)
		}
	}
}

func (b *batchCopIterator) handleBatchCopResponse(bo *Backoffer, response *interlock.BatchResponse, task *batchCopTask) (err error) {
	if otherErr := response.GetOtherError(); otherErr != "" {
		err = errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", b.req.StartTs),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return errors.Trace(err)
	}

	resp := batchCopResponse{
		pbResp: response,
		detail: new(CopRuntimeStats),
	}

	resp.detail.BackoffTime = time.Duration(bo.totalSleep) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(bo.backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(bo.backoffTimes))
	for backoff := range bo.backoffTimes {
		backoffName := backoff.String()
		resp.detail.BackoffTimes[backoffName] = bo.backoffTimes[backoff]
		resp.detail.BackoffSleep[backoffName] = time.Duration(bo.backoffSleepMS[backoff]) * time.Millisecond
	}
	resp.detail.CalleeAddress = task.storeAddr

	b.sendToRespCh(&resp)

	return
}

func (b *batchCopIterator) sendToRespCh(resp *batchCopResponse) (exit bool) {
	select {
	case b.respChan <- resp:
	case <-b.finishCh:
		exit = true
	}
	return
}
