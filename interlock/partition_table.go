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

package interlock

import (
	"context"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/errors"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// PartitionBlockInterlockingDirectorate is a InterlockingDirectorate for partitioned causet.
// It works by wrap the underlying BlockReader/IndexReader/IndexLookUpReader.
type PartitionBlockInterlockingDirectorate struct {
	baseInterlockingDirectorate

	nextPartition
	partitions []causet.PhysicalBlock
	cursor     int
	curr       InterlockingDirectorate
}

type nextPartition interface {
	nextPartition(context.Context, causet.PhysicalBlock) (InterlockingDirectorate, error)
}

type nextPartitionForBlockReader struct {
	exec *BlockReaderInterlockingDirectorate
}

func (n nextPartitionForBlockReader) nextPartition(ctx context.Context, tbl causet.PhysicalBlock) (InterlockingDirectorate, error) {
	n.exec.causet = tbl
	n.exec.kvRanges = n.exec.kvRanges[:0]
	if err := uFIDelatePosetDagRequestBlockID(ctx, n.exec.posetPosetDagPB, tbl.Meta().ID, tbl.GetPhysicalID()); err != nil {
		return nil, err
	}
	return n.exec, nil
}

type nextPartitionForIndexLookUp struct {
	exec *IndexLookUpInterlockingDirectorate
}

func (n nextPartitionForIndexLookUp) nextPartition(ctx context.Context, tbl causet.PhysicalBlock) (InterlockingDirectorate, error) {
	n.exec.causet = tbl
	return n.exec, nil
}

type nextPartitionForIndexReader struct {
	exec *IndexReaderInterlockingDirectorate
}

func (n nextPartitionForIndexReader) nextPartition(ctx context.Context, tbl causet.PhysicalBlock) (InterlockingDirectorate, error) {
	exec := n.exec
	exec.causet = tbl
	exec.physicalBlockID = tbl.GetPhysicalID()
	return exec, nil
}

type nextPartitionForIndexMerge struct {
	exec *IndexMergeReaderInterlockingDirectorate
}

func (n nextPartitionForIndexMerge) nextPartition(ctx context.Context, tbl causet.PhysicalBlock) (InterlockingDirectorate, error) {
	exec := n.exec
	exec.causet = tbl
	return exec, nil
}

type nextPartitionForUnionScan struct {
	b     *interlockBuilder
	us    *causetcore.PhysicalUnionScan
	child nextPartition
}

// nextPartition implements the nextPartition interface.
// For union scan on partitioned causet, the interlock should be PartitionBlock->UnionScan->BlockReader rather than
// UnionScan->PartitionBlock->BlockReader
func (n nextPartitionForUnionScan) nextPartition(ctx context.Context, tbl causet.PhysicalBlock) (InterlockingDirectorate, error) {
	childInterDirc, err := n.child.nextPartition(ctx, tbl)
	if err != nil {
		return nil, err
	}

	n.b.err = nil
	ret := n.b.buildUnionScanFromReader(childInterDirc, n.us)
	return ret, n.b.err
}

func nextPartitionWithTrace(ctx context.Context, n nextPartition, tbl causet.PhysicalBlock) (InterlockingDirectorate, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("nextPartition %d", tbl.GetPhysicalID()), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	return n.nextPartition(ctx, tbl)
}

// uFIDelatePosetDagRequestBlockID uFIDelate the causet ID in the PosetDag request to partition ID.
// EinsteinDB only use that causet ID for log, but TiFlash use it.
func uFIDelatePosetDagRequestBlockID(ctx context.Context, posetPosetDag *fidelpb.PosetDagRequest, blockID, partitionID int64) error {
	// TiFlash set RootInterlockingDirectorate field and ignore InterlockingDirectorates field.
	if posetPosetDag.RootInterlockingDirectorate != nil {
		return uFIDelateInterlockingDirectorateBlockID(ctx, posetPosetDag.RootInterlockingDirectorate, blockID, partitionID, true)
	}
	for i := 0; i < len(posetPosetDag.InterlockingDirectorates); i++ {
		exec := posetPosetDag.InterlockingDirectorates[i]
		err := uFIDelateInterlockingDirectorateBlockID(ctx, exec, blockID, partitionID, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func uFIDelateInterlockingDirectorateBlockID(ctx context.Context, exec *fidelpb.InterlockingDirectorate, blockID, partitionID int64, recursive bool) error {
	var child *fidelpb.InterlockingDirectorate
	switch exec.Tp {
	case fidelpb.InterDircType_TypeBlockScan:
		exec.TblScan.BlockId = partitionID
		// For test coverage.
		if tmp := ctx.Value("nextPartitionUFIDelatePosetDagReq"); tmp != nil {
			m := tmp.(map[int64]struct{})
			m[partitionID] = struct{}{}
		}
	case fidelpb.InterDircType_TypeIndexScan:
		exec.IdxScan.BlockId = partitionID
	case fidelpb.InterDircType_TypeSelection:
		child = exec.Selection.Child
	case fidelpb.InterDircType_TypeAggregation, fidelpb.InterDircType_TypeStreamAgg:
		child = exec.Aggregation.Child
	case fidelpb.InterDircType_TypeTopN:
		child = exec.TopN.Child
	case fidelpb.InterDircType_TypeLimit:
		child = exec.Limit.Child
	case fidelpb.InterDircType_TypeJoin:
		// TiFlash currently does not support Join on partition causet.
		// The causet should not generate this HoTT of plan.
		// So the code should never run here.
		return errors.New("wrong plan, join on partition causet is not supported on TiFlash")
	default:
		return errors.Trace(fmt.Errorf("unknown new fidelpb protodefCaus %d", exec.Tp))
	}
	if child != nil && recursive {
		return uFIDelateInterlockingDirectorateBlockID(ctx, child, blockID, partitionID, recursive)
	}
	return nil
}

// Open implements the InterlockingDirectorate interface.
func (e *PartitionBlockInterlockingDirectorate) Open(ctx context.Context) error {
	e.cursor = 0
	e.curr = nil
	return nil
}

// Next implements the InterlockingDirectorate interface.
func (e *PartitionBlockInterlockingDirectorate) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	var err error
	for e.cursor < len(e.partitions) {
		if e.curr == nil {
			n := e.nextPartition
			e.curr, err = nextPartitionWithTrace(ctx, n, e.partitions[e.cursor])
			if err != nil {
				return err
			}
			if err := e.curr.Open(ctx); err != nil {
				return err
			}
		}

		err = Next(ctx, e.curr, chk)
		if err != nil {
			return err
		}

		if chk.NumEvents() > 0 {
			break
		}

		err = e.curr.Close()
		if err != nil {
			return err
		}
		e.curr = nil
		e.cursor++
	}
	return nil
}

// Close implements the InterlockingDirectorate interface.
func (e *PartitionBlockInterlockingDirectorate) Close() error {
	var err error
	if e.curr != nil {
		err = e.curr.Close()
		e.curr = nil
	}
	return err
}
