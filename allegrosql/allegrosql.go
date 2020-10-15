// Copyright 2020 WHTCORPS INC.
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
	"unsafe"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/opentracing/opentracing-go"
)


func Select(ctx context.Context, sctx stochastikctx.Context, kvReq *ekv.Request, fieldTypes []*types.FieldType, fb *statistics.QueryFeedback) (SelectResult, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("allegrosql.Select", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// For testing purpose.
	if hook := ctx.Value("CheckSelectRequestHook"); hook != nil {
		hook.(func(*ekv.Request))(kvReq)
	}

	if !sctx.GetStochastikVars().EnableStreaming {
		kvReq.Streaming = false
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetStochastikVars().KVVars)
	if resp == nil {
		err := errors.New("client returns nil response")
		return nil, err
	}

	label := metrics.LblGeneral
	if sctx.GetStochastikVars().InRestrictedALLEGROSQL {
		label = metrics.LblInternal
	}

	// kvReq.MemTracker is used to trace and control memory usage in DistALLEGROSQL layer;
	// for streamResult, since it is a pipeline which has no buffer, it's not necessary to trace it;
	// for selectResult, we just use the kvReq.MemTracker prepared for co-processor
	// instead of creating a new one for simplification.
	if kvReq.Streaming {
		return &streamResult{
			label:      "posetPosetDag-stream",
			sqlType:    label,
			resp:       resp,
			rowLen:     len(fieldTypes),
			fieldTypes: fieldTypes,
			ctx:        sctx,
			feedback:   fb,
		}, nil
	}
	encodetype := fidelpb.EncodeType_TypeDefault
	if canUseChunkRPC(sctx) {
		encodetype = fidelpb.EncodeType_TypeChunk
	}
	return &selectResult{
		label:      "posetPosetDag",
		resp:       resp,
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        sctx,
		feedback:   fb,
		sqlType:    label,
		memTracker: kvReq.MemTracker,
		encodeType: encodetype,
	}, nil
}

// SelectWithRuntimeStats sends a PosetDag request, returns SelectResult.
// The difference from Select is that SelectWithRuntimeStats will set copCausetIDs into selectResult,
// which can help selectResult to defCauslect runtime stats.
func SelectWithRuntimeStats(ctx context.Context, sctx stochastikctx.Context, kvReq *ekv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copCausetIDs []int, rootCausetID int) (SelectResult, error) {
	sr, err := Select(ctx, sctx, kvReq, fieldTypes, fb)
	if err == nil {
		if selectResult, ok := sr.(*selectResult); ok {
			selectResult.copCausetIDs = copCausetIDs
			selectResult.rootCausetID = rootCausetID
		}
	}
	return sr, err
}

// Analyze do a analyze request.
func Analyze(ctx context.Context, client ekv.Client, kvReq *ekv.Request, vars *ekv.Variables,
	isRestrict bool) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	label := metrics.LblGeneral
	if isRestrict {
		label = metrics.LblInternal
	}
	result := &selectResult{
		label:      "analyze",
		resp:       resp,
		feedback:   statistics.NewQueryFeedback(0, nil, 0, false),
		sqlType:    label,
		encodeType: fidelpb.EncodeType_TypeDefault,
	}
	return result, nil
}

// Checksum sends a checksum request.
func Checksum(ctx context.Context, client ekv.Client, kvReq *ekv.Request, vars *ekv.Variables) (SelectResult, error) {
	resp := client.Send(ctx, kvReq, vars)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:      "checksum",
		resp:       resp,
		feedback:   statistics.NewQueryFeedback(0, nil, 0, false),
		sqlType:    metrics.LblGeneral,
		encodeType: fidelpb.EncodeType_TypeDefault,
	}
	return result, nil
}

// SetEncodeType sets the encoding method for the PosetDagRequest. The supported encoding
// methods are:
// 1. TypeChunk: the result is encoded using the Chunk format, refer soliton/chunk/chunk.go
// 2. TypeDefault: the result is encoded event by event
func SetEncodeType(ctx stochastikctx.Context, posetPosetDagReq *fidelpb.PosetDagRequest) {
	if canUseChunkRPC(ctx) {
		posetPosetDagReq.EncodeType = fidelpb.EncodeType_TypeChunk
		setChunkMemoryLayout(posetPosetDagReq)
	} else {
		posetPosetDagReq.EncodeType = fidelpb.EncodeType_TypeDefault
	}
}

func canUseChunkRPC(ctx stochastikctx.Context) bool {
	if !ctx.GetStochastikVars().EnableChunkRPC {
		return false
	}
	if ctx.GetStochastikVars().EnableStreaming {
		return false
	}
	if !checkAlignment() {
		return false
	}
	return true
}

var supportedAlignment = unsafe.Sizeof(types.MyDecimal{}) == 40

// checkAlignment checks the alignment in current system environment.
// The alignment is influenced by system, machine and Golang version.
// Using this function can guarantee the alignment is we want.
func checkAlignment() bool {
	return supportedAlignment
}

var systemEndian fidelpb.Endian

// setChunkMemoryLayout sets the chunk memory layout for the PosetDagRequest.
func setChunkMemoryLayout(posetPosetDagReq *fidelpb.PosetDagRequest) {
	posetPosetDagReq.ChunkMemoryLayout = &fidelpb.ChunkMemoryLayout{Endian: GetSystemEndian()}
}

// GetSystemEndian gets the system endian.
func GetSystemEndian() fidelpb.Endian {
	return systemEndian
}

func init() {
	i := 0x0100
	ptr := unsafe.Pointer(&i)
	if 0x01 == *(*byte)(ptr) {
		systemEndian = fidelpb.Endian_BigEndian
	} else {
		systemEndian = fidelpb.Endian_LittleEndian
	}
}
