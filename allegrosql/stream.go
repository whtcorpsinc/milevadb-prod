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

package allegrosql

import (
	"context"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// streamResult implements the SelectResult interface.
type streamResult struct {
	label   string
	sqlType string

	resp       ekv.Response
	rowLen     int
	fieldTypes []*types.FieldType
	ctx        stochastikctx.Context

	// NOTE: curr == nil means stream finish, while len(curr.RowsData) == 0 doesn't.
	curr         *fidelpb.Chunk
	partialCount int64
	feedback     *statistics.QueryFeedback

	fetchDuration    time.Duration
	durationReported bool
}

func (r *streamResult) Fetch(context.Context) {}

func (r *streamResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for !chk.IsFull() {
		err := r.readDataIfNecessary(ctx)
		if err != nil {
			return err
		}
		if r.curr == nil {
			return nil
		}

		err = r.flushToChunk(chk)
		if err != nil {
			return err
		}
	}
	return nil
}

// readDataFromResponse read the data to result. Returns true means the resp is finished.
func (r *streamResult) readDataFromResponse(ctx context.Context, resp ekv.Response, result *fidelpb.Chunk) (bool, error) {
	startTime := time.Now()
	resultSubset, err := resp.Next(ctx)
	duration := time.Since(startTime)
	r.fetchDuration += duration
	if err != nil {
		return false, err
	}
	if resultSubset == nil {
		if !r.durationReported {
			// TODO: Add a label to distinguish between success or failure.
			// https://github.com/whtcorpsinc/milevadb/issues/11397
			metrics.DistALLEGROSQLQueryHistogram.WithLabelValues(r.label, r.sqlType).Observe(r.fetchDuration.Seconds())
			r.durationReported = true
		}
		return true, nil
	}

	var stream fidelpb.StreamResponse
	err = stream.Unmarshal(resultSubset.GetData())
	if err != nil {
		return false, errors.Trace(err)
	}
	if stream.Error != nil {
		return false, errors.Errorf("stream response error: [%d]%s\n", stream.Error.Code, stream.Error.Msg)
	}
	for _, warning := range stream.Warnings {
		r.ctx.GetStochastikVars().StmtCtx.AppendWarning(terror.ClassEinsteinDB.Synthesize(terror.ErrCode(warning.Code), warning.Msg))
	}

	err = result.Unmarshal(stream.Data)
	if err != nil {
		return false, errors.Trace(err)
	}
	r.feedback.UFIDelate(resultSubset.GetStartKey(), stream.OutputCounts)
	r.partialCount++

	hasStats, ok := resultSubset.(CopRuntimeStats)
	if ok {
		copStats := hasStats.GetCopRuntimeStats()
		if copStats != nil {
			copStats.CopTime = duration
			r.ctx.GetStochastikVars().StmtCtx.MergeInterDircDetails(&copStats.InterDircDetails, nil)
		}
	}
	return false, nil
}

// readDataIfNecessary ensures there are some data in current chunk. If no more data, r.curr == nil.
func (r *streamResult) readDataIfNecessary(ctx context.Context) error {
	if r.curr != nil && len(r.curr.RowsData) > 0 {
		return nil
	}

	tmp := new(fidelpb.Chunk)
	finish, err := r.readDataFromResponse(ctx, r.resp, tmp)
	if err != nil {
		return err
	}
	if finish {
		r.curr = nil
		return nil
	}
	r.curr = tmp
	return nil
}

func (r *streamResult) flushToChunk(chk *chunk.Chunk) (err error) {
	remainRowsData := r.curr.RowsData
	causetDecoder := codec.NewCausetDecoder(chk, r.ctx.GetStochastikVars().Location())
	for !chk.IsFull() && len(remainRowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			remainRowsData, err = causetDecoder.DecodeOne(remainRowsData, i, r.fieldTypes[i])
			if err != nil {
				return err
			}
		}
	}
	r.curr.RowsData = remainRowsData
	if len(remainRowsData) == 0 {
		r.curr = nil // Current chunk is finished.
	}
	return nil
}

func (r *streamResult) NextRaw(ctx context.Context) ([]byte, error) {
	r.partialCount++
	r.feedback.Invalidate()
	resultSubset, err := r.resp.Next(ctx)
	if resultSubset == nil || err != nil {
		return nil, err
	}
	return resultSubset.GetData(), err
}

func (r *streamResult) Close() error {
	if r.feedback.Actual() > 0 {
		metrics.DistALLEGROSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	metrics.DistALLEGROSQLPartialCountHistogram.Observe(float64(r.partialCount))
	return nil
}
