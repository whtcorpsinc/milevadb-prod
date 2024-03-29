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

	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/ekv"
)

// DeleteRangeTask is used to delete all keys in a range. After
// performing DeleteRange, it keeps how many ranges it affects and
// if the task was canceled or not.
type DeleteRangeTask struct {
	completedRegions int
	causetstore      CausetStorage
	startKey         []byte
	endKey           []byte
	notifyOnly       bool
	concurrency      int
}

// NewDeleteRangeTask creates a DeleteRangeTask. Deleting will be performed when `InterDircute` method is invoked.
// Be careful while using this API. This API doesn't keep recent MVCC versions, but will delete all versions of all keys
// in the range immediately. Also notice that frequent invocation to this API may cause performance problems to EinsteinDB.
func NewDeleteRangeTask(causetstore CausetStorage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask {
	return &DeleteRangeTask{
		completedRegions: 0,
		causetstore:      causetstore,
		startKey:         startKey,
		endKey:           endKey,
		notifyOnly:       false,
		concurrency:      concurrency,
	}
}

// NewNotifyDeleteRangeTask creates a task that sends delete range requests to all regions in the range, but with the
// flag `notifyOnly` set. EinsteinDB will not actually delete the range after receiving request, but it will be replicated via
// raft. This is used to notify the involved regions before sending UnsafeDestroyRange requests.
func NewNotifyDeleteRangeTask(causetstore CausetStorage, startKey []byte, endKey []byte, concurrency int) *DeleteRangeTask {
	task := NewDeleteRangeTask(causetstore, startKey, endKey, concurrency)
	task.notifyOnly = true
	return task
}

// getRunnerName returns a name for RangeTaskRunner.
func (t *DeleteRangeTask) getRunnerName() string {
	if t.notifyOnly {
		return "delete-range-notify"
	}
	return "delete-range"
}

// InterDircute performs the delete range operation.
func (t *DeleteRangeTask) InterDircute(ctx context.Context) error {
	runnerName := t.getRunnerName()

	runner := NewRangeTaskRunner(runnerName, t.causetstore, t.concurrency, t.sendReqOnRange)
	err := runner.RunOnRange(ctx, t.startKey, t.endKey)
	t.completedRegions = runner.CompletedRegions()

	return err
}

// InterDircute performs the delete range operation.
func (t *DeleteRangeTask) sendReqOnRange(ctx context.Context, r ekv.KeyRange) (RangeTaskStat, error) {
	startKey, rangeEndKey := r.StartKey, r.EndKey
	var stat RangeTaskStat
	for {
		select {
		case <-ctx.Done():
			return stat, errors.Trace(ctx.Err())
		default:
		}

		if bytes.Compare(startKey, rangeEndKey) >= 0 {
			break
		}

		bo := NewBackofferWithVars(ctx, deleteRangeOneRegionMaxBackoff, nil)
		loc, err := t.causetstore.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return stat, errors.Trace(err)
		}

		// Delete to the end of the region, except if it's the last region overlapping the range
		endKey := loc.EndKey
		// If it is the last region
		if loc.Contains(rangeEndKey) {
			endKey = rangeEndKey
		}

		req := einsteindbrpc.NewRequest(einsteindbrpc.CmdDeleteRange, &ekvrpcpb.DeleteRangeRequest{
			StartKey:   startKey,
			EndKey:     endKey,
			NotifyOnly: t.notifyOnly,
		})

		resp, err := t.causetstore.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			return stat, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return stat, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return stat, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return stat, errors.Trace(ErrBodyMissing)
		}
		deleteRangeResp := resp.Resp.(*ekvrpcpb.DeleteRangeResponse)
		if err := deleteRangeResp.GetError(); err != "" {
			return stat, errors.Errorf("unexpected delete range err: %v", err)
		}
		stat.CompletedRegions++
		startKey = endKey
	}

	return stat, nil
}

// CompletedRegions returns the number of regions that are affected by this delete range task
func (t *DeleteRangeTask) CompletedRegions() int {
	return t.completedRegions
}
