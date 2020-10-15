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
	"math"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// RequestBuilder is used to build a "ekv.Request".
// It is called before we issue a ekv request by "Select".
type RequestBuilder struct {
	ekv.Request
	err error
}

// Build builds a "ekv.Request".
func (builder *RequestBuilder) Build() (*ekv.Request, error) {
	return &builder.Request, builder.err
}

// SetMemTracker sets a memTracker for this request.
func (builder *RequestBuilder) SetMemTracker(tracker *memory.Tracker) *RequestBuilder {
	builder.Request.MemTracker = tracker
	return builder
}

// SetBlockRanges sets "KeyRanges" for "ekv.Request" by converting "blockRanges"
// to "KeyRanges" firstly.
func (builder *RequestBuilder) SetBlockRanges(tid int64, blockRanges []*ranger.Range, fb *statistics.QueryFeedback) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges = BlockRangesToKVRanges(tid, blockRanges, fb)
	}
	return builder
}

// SetIndexRanges sets "KeyRanges" for "ekv.Request" by converting index range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetIndexRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = IndexRangesToKVRanges(sc, tid, idxID, ranges, nil)
	}
	return builder
}

// SetCommonHandleRanges sets "KeyRanges" for "ekv.Request" by converting common handle range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetCommonHandleRanges(sc *stmtctx.StatementContext, tid int64, ranges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = CommonHandleRangesToKVRanges(sc, tid, ranges)
	}
	return builder
}

// SetBlockHandles sets "KeyRanges" for "ekv.Request" by converting causet handles
// "handles" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetBlockHandles(tid int64, handles []ekv.Handle) *RequestBuilder {
	builder.Request.KeyRanges = BlockHandlesToKVRanges(tid, handles)
	return builder
}

const estimatedRegionRowCount = 100000

// SetPosetDagRequest sets the request type to "ReqTypePosetDag" and construct request data.
func (builder *RequestBuilder) SetPosetDagRequest(posetPosetDag *fidelpb.PosetDagRequest) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = ekv.ReqTypePosetDag
		builder.Request.Cacheable = true
		builder.Request.Data, builder.err = posetPosetDag.Marshal()
	}
	// When the PosetDag is just simple scan and small limit, set concurrency to 1 would be sufficient.
	if len(posetPosetDag.InterlockingDirectorates) == 2 && posetPosetDag.InterlockingDirectorates[1].GetLimit() != nil {
		limit := posetPosetDag.InterlockingDirectorates[1].GetLimit()
		if limit != nil && limit.Limit < estimatedRegionRowCount {
			builder.Request.Concurrency = 1
		}
	}
	return builder
}

// SetAnalyzeRequest sets the request type to "ReqTypeAnalyze" and construct request data.
func (builder *RequestBuilder) SetAnalyzeRequest(ana *fidelpb.AnalyzeReq) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = ekv.ReqTypeAnalyze
		builder.Request.Data, builder.err = ana.Marshal()
		builder.Request.NotFillCache = true
		builder.Request.IsolationLevel = ekv.RC
		builder.Request.Priority = ekv.PriorityLow
	}

	return builder
}

// SetChecksumRequest sets the request type to "ReqTypeChecksum" and construct request data.
func (builder *RequestBuilder) SetChecksumRequest(checksum *fidelpb.ChecksumRequest) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = ekv.ReqTypeChecksum
		builder.Request.Data, builder.err = checksum.Marshal()
		builder.Request.NotFillCache = true
	}

	return builder
}

// SetKeyRanges sets "KeyRanges" for "ekv.Request".
func (builder *RequestBuilder) SetKeyRanges(keyRanges []ekv.KeyRange) *RequestBuilder {
	builder.Request.KeyRanges = keyRanges
	return builder
}

// SetStartTS sets "StartTS" for "ekv.Request".
func (builder *RequestBuilder) SetStartTS(startTS uint64) *RequestBuilder {
	builder.Request.StartTs = startTS
	return builder
}

// SetDesc sets "Desc" for "ekv.Request".
func (builder *RequestBuilder) SetDesc(desc bool) *RequestBuilder {
	builder.Request.Desc = desc
	return builder
}

// SetKeepOrder sets "KeepOrder" for "ekv.Request".
func (builder *RequestBuilder) SetKeepOrder(order bool) *RequestBuilder {
	builder.Request.KeepOrder = order
	return builder
}

// SetStoreType sets "StoreType" for "ekv.Request".
func (builder *RequestBuilder) SetStoreType(storeType ekv.StoreType) *RequestBuilder {
	builder.Request.StoreType = storeType
	return builder
}

// SetAllowBatchCop sets `BatchCop` property.
func (builder *RequestBuilder) SetAllowBatchCop(batchCop bool) *RequestBuilder {
	builder.Request.BatchCop = batchCop
	return builder
}

func (builder *RequestBuilder) getIsolationLevel() ekv.IsoLevel {
	switch builder.Tp {
	case ekv.ReqTypeAnalyze:
		return ekv.RC
	}
	return ekv.SI
}

func (builder *RequestBuilder) getKVPriority(sv *variable.StochastikVars) int {
	switch sv.StmtCtx.Priority {
	case allegrosql.NoPriority, allegrosql.DelayedPriority:
		return ekv.PriorityNormal
	case allegrosql.LowPriority:
		return ekv.PriorityLow
	case allegrosql.HighPriority:
		return ekv.PriorityHigh
	}
	return ekv.PriorityNormal
}

// SetFromStochastikVars sets the following fields for "ekv.Request" from stochastik variables:
// "Concurrency", "IsolationLevel", "NotFillCache", "ReplicaRead", "SchemaVar".
func (builder *RequestBuilder) SetFromStochastikVars(sv *variable.StochastikVars) *RequestBuilder {
	if builder.Request.Concurrency == 0 {
		// Concurrency may be set to 1 by SetPosetDagRequest
		builder.Request.Concurrency = sv.DistALLEGROSQLScanConcurrency()
	}
	builder.Request.IsolationLevel = builder.getIsolationLevel()
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	builder.Request.TaskID = sv.StmtCtx.TaskID
	builder.Request.Priority = builder.getKVPriority(sv)
	builder.Request.ReplicaRead = sv.GetReplicaRead()
	if sv.SnapshotschemaReplicant != nil {
		builder.Request.SchemaVar = schemareplicant.GetSchemaReplicantByStochastikVars(sv).SchemaMetaVersion()
	} else {
		builder.Request.SchemaVar = sv.TxnCtx.SchemaVersion
	}
	return builder
}

// SetStreaming sets "Streaming" flag for "ekv.Request".
func (builder *RequestBuilder) SetStreaming(streaming bool) *RequestBuilder {
	builder.Request.Streaming = streaming
	return builder
}

// SetConcurrency sets "Concurrency" for "ekv.Request".
func (builder *RequestBuilder) SetConcurrency(concurrency int) *RequestBuilder {
	builder.Request.Concurrency = concurrency
	return builder
}

// BlockRangesToKVRanges converts causet ranges to "KeyRange".
func BlockRangesToKVRanges(tid int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) []ekv.KeyRange {
	if fb == nil || fb.Hist == nil {
		return blockRangesToKVRangesWithoutSplit(tid, ranges)
	}
	krs := make([]ekv.KeyRange, 0, len(ranges))
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
		high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
		if ran.LowExclude {
			low = ekv.Key(low).PrefixNext()
		}
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PrefixNext` will make the
		// high value greater than upper bound, so we causetstore the range here.
		r := &ranger.Range{LowVal: []types.Causet{types.NewBytesCauset(low)},
			HighVal: []types.Causet{types.NewBytesCauset(high)}}
		feedbackRanges = append(feedbackRanges, r)

		if !ran.HighExclude {
			high = ekv.Key(high).PrefixNext()
		}
		startKey := blockcodec.EncodeRowKey(tid, low)
		endKey := blockcodec.EncodeRowKey(tid, high)
		krs = append(krs, ekv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	fb.StoreRanges(feedbackRanges)
	return krs
}

func blockRangesToKVRangesWithoutSplit(tid int64, ranges []*ranger.Range) []ekv.KeyRange {
	krs := make([]ekv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		low, high := encodeHandleKey(ran)
		startKey := blockcodec.EncodeRowKey(tid, low)
		endKey := blockcodec.EncodeRowKey(tid, high)
		krs = append(krs, ekv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

func encodeHandleKey(ran *ranger.Range) ([]byte, []byte) {
	low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
	high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
	if ran.LowExclude {
		low = ekv.Key(low).PrefixNext()
	}
	if !ran.HighExclude {
		high = ekv.Key(high).PrefixNext()
	}
	return low, high
}

// BlockHandlesToKVRanges converts sorted handle to ekv ranges.
// For continuous handles, we should merge them to a single key range.
func BlockHandlesToKVRanges(tid int64, handles []ekv.Handle) []ekv.KeyRange {
	krs := make([]ekv.KeyRange, 0, len(handles))
	i := 0
	for i < len(handles) {
		if commonHandle, ok := handles[i].(*ekv.CommonHandle); ok {
			ran := ekv.KeyRange{
				StartKey: blockcodec.EncodeRowKey(tid, commonHandle.Encoded()),
				EndKey:   blockcodec.EncodeRowKey(tid, ekv.Key(commonHandle.Encoded()).Next()),
			}
			krs = append(krs, ran)
			i++
			continue
		}
		j := i + 1
		for ; j < len(handles) && handles[j-1].IntValue() != math.MaxInt64; j++ {
			if handles[j].IntValue() != handles[j-1].IntValue()+1 {
				break
			}
		}
		low := codec.EncodeInt(nil, handles[i].IntValue())
		high := codec.EncodeInt(nil, handles[j-1].IntValue())
		high = ekv.Key(high).PrefixNext()
		startKey := blockcodec.EncodeRowKey(tid, low)
		endKey := blockcodec.EncodeRowKey(tid, high)
		krs = append(krs, ekv.KeyRange{StartKey: startKey, EndKey: endKey})
		i = j
	}
	return krs
}

// IndexRangesToKVRanges converts index ranges to "KeyRange".
func IndexRangesToKVRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]ekv.KeyRange, error) {
	if fb == nil || fb.Hist == nil {
		return indexRangesToKVWithoutSplit(sc, tid, idxID, ranges)
	}
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		feedbackRanges = append(feedbackRanges, &ranger.Range{LowVal: []types.Causet{types.NewBytesCauset(low)},
			HighVal: []types.Causet{types.NewBytesCauset(high)}, LowExclude: false, HighExclude: true})
	}
	feedbackRanges, ok := fb.Hist.SplitRange(sc, feedbackRanges, true)
	if !ok {
		fb.Invalidate()
	}
	krs := make([]ekv.KeyRange, 0, len(feedbackRanges))
	for _, ran := range feedbackRanges {
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			low = ekv.Key(low).PrefixNext()
		}
		ran.LowVal[0].SetBytes(low)
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PrefixNext` will make the
		// high value greater than upper bound, so we causetstore the high value here.
		ran.HighVal[0].SetBytes(high)
		if !ran.HighExclude {
			high = ekv.Key(high).PrefixNext()
		}
		startKey := blockcodec.EncodeIndexSeekKey(tid, idxID, low)
		endKey := blockcodec.EncodeIndexSeekKey(tid, idxID, high)
		krs = append(krs, ekv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	fb.StoreRanges(feedbackRanges)
	return krs, nil
}

// CommonHandleRangesToKVRanges converts common handle ranges to "KeyRange".
func CommonHandleRangesToKVRanges(sc *stmtctx.StatementContext, tid int64, ranges []*ranger.Range) ([]ekv.KeyRange, error) {
	rans := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		rans = append(rans, &ranger.Range{LowVal: []types.Causet{types.NewBytesCauset(low)},
			HighVal: []types.Causet{types.NewBytesCauset(high)}, LowExclude: false, HighExclude: true})
	}
	krs := make([]ekv.KeyRange, 0, len(rans))
	for _, ran := range rans {
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			low = ekv.Key(low).PrefixNext()
		}
		ran.LowVal[0].SetBytes(low)
		startKey := blockcodec.EncodeRowKey(tid, low)
		endKey := blockcodec.EncodeRowKey(tid, high)
		krs = append(krs, ekv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs, nil
}

func indexRangesToKVWithoutSplit(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range) ([]ekv.KeyRange, error) {
	krs := make([]ekv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		startKey := blockcodec.EncodeIndexSeekKey(tid, idxID, low)
		endKey := blockcodec.EncodeIndexSeekKey(tid, idxID, high)
		krs = append(krs, ekv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs, nil
}

func encodeIndexKey(sc *stmtctx.StatementContext, ran *ranger.Range) ([]byte, []byte, error) {
	low, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		return nil, nil, err
	}
	if ran.LowExclude {
		low = ekv.Key(low).PrefixNext()
	}
	high, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		return nil, nil, err
	}

	if !ran.HighExclude {
		high = ekv.Key(high).PrefixNext()
	}

	var hasNull bool
	for _, highVal := range ran.HighVal {
		if highVal.IsNull() {
			hasNull = true
			break
		}
	}

	if hasNull {
		// Append 0 to make unique-key range [null, null] to be a scan rather than point-get.
		high = ekv.Key(high).Next()
	}
	return low, high, nil
}
