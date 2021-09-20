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
	"runtime/trace"

	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/causet"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/types"
)

// UnionScanInterDirc merges the rows from dirty causet and the rows from allegrosql request.
type UnionScanInterDirc struct {
	baseInterlockingDirectorate

	memBuf     ekv.MemBuffer
	memBufSnap ekv.Getter

	// usedIndex is the defCausumn offsets of the index which Src interlock has used.
	usedIndex                []int
	desc                     bool
	conditions               []memex.Expression
	conditionsWithVirDefCaus []memex.Expression
	defCausumns              []*perceptron.DeferredCausetInfo
	causet                   causet.Block
	// belowHandleDefCauss is the handle's position of the below scan plan.
	belowHandleDefCauss causetembedded.HandleDefCauss

	addedEvents           [][]types.Causet
	cursor4AddEvents      int
	sortErr               error
	snapshotEvents        [][]types.Causet
	cursor4SnapshotEvents int
	snapshotChunkBuffer   *chunk.Chunk
	mublockEvent          chunk.MutEvent
	// virtualDeferredCausetIndex records all the indices of virtual defCausumns and sort them in definition
	// to make sure we can compute the virtual defCausumn in right order.
	virtualDeferredCausetIndex []int
}

// Open implements the InterlockingDirectorate Open interface.
func (us *UnionScanInterDirc) Open(ctx context.Context) error {
	if err := us.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	return us.open(ctx)
}

func (us *UnionScanInterDirc) open(ctx context.Context) error {
	var err error
	reader := us.children[0]

	// If the push-downed condition contains virtual defCausumn, we may build a selection upon reader. Since unionScanInterDirc
	// has already contained condition, we can ignore the selection.
	if sel, ok := reader.(*SelectionInterDirc); ok {
		reader = sel.children[0]
	}

	defer trace.StartRegion(ctx, "UnionScanBuildEvents").End()
	txn, err := us.ctx.Txn(false)
	if err != nil {
		return err
	}

	mb := txn.GetMemBuffer()
	mb.RLock()
	defer mb.RUnlock()

	us.memBuf = mb
	us.memBufSnap = mb.SnapshotGetter()

	// 1. select without virtual defCausumns
	// 2. build virtual defCausumns and select with virtual defCausumns
	switch x := reader.(type) {
	case *BlockReaderInterlockingDirectorate:
		us.addedEvents, err = buildMemBlockReader(us, x).getMemEvents()
	case *IndexReaderInterlockingDirectorate:
		us.addedEvents, err = buildMemIndexReader(us, x).getMemEvents()
	case *IndexLookUpInterlockingDirectorate:
		us.addedEvents, err = buildMemIndexLookUpReader(us, x).getMemEvents()
	default:
		err = fmt.Errorf("unexpected union scan children:%T", reader)
	}
	if err != nil {
		return err
	}
	us.snapshotChunkBuffer = newFirstChunk(us)
	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (us *UnionScanInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	us.memBuf.RLock()
	defer us.memBuf.RUnlock()
	req.GrowAndReset(us.maxChunkSize)
	mublockEvent := chunk.MutEventFromTypes(retTypes(us))
	for i, batchSize := 0, req.Capacity(); i < batchSize; i++ {
		event, err := us.getOneEvent(ctx)
		if err != nil {
			return err
		}
		// no more data.
		if event == nil {
			return nil
		}
		mublockEvent.SetCausets(event...)

		for _, idx := range us.virtualDeferredCausetIndex {
			causet, err := us.schemaReplicant.DeferredCausets[idx].EvalVirtualDeferredCauset(mublockEvent.ToEvent())
			if err != nil {
				return err
			}
			// Because the memex might return different type from
			// the generated defCausumn, we should wrap a CAST on the result.
			castCauset, err := causet.CastValue(us.ctx, causet, us.defCausumns[idx], false, true)
			if err != nil {
				return err
			}
			mublockEvent.SetCauset(idx, castCauset)
		}

		matched, _, err := memex.EvalBool(us.ctx, us.conditionsWithVirDefCaus, mublockEvent.ToEvent())
		if err != nil {
			return err
		}
		if matched {
			req.AppendEvent(mublockEvent.ToEvent())
		}
	}
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (us *UnionScanInterDirc) Close() error {
	us.cursor4AddEvents = 0
	us.cursor4SnapshotEvents = 0
	us.addedEvents = us.addedEvents[:0]
	us.snapshotEvents = us.snapshotEvents[:0]
	return us.children[0].Close()
}

// getOneEvent gets one result event from dirty causet or child.
func (us *UnionScanInterDirc) getOneEvent(ctx context.Context) ([]types.Causet, error) {
	snapshotEvent, err := us.getSnapshotEvent(ctx)
	if err != nil {
		return nil, err
	}
	addedEvent := us.getAddedEvent()

	var event []types.Causet
	var isSnapshotEvent bool
	if addedEvent == nil {
		event = snapshotEvent
		isSnapshotEvent = true
	} else if snapshotEvent == nil {
		event = addedEvent
	} else {
		isSnapshotEvent, err = us.shouldPickFirstEvent(snapshotEvent, addedEvent)
		if err != nil {
			return nil, err
		}
		if isSnapshotEvent {
			event = snapshotEvent
		} else {
			event = addedEvent
		}
	}
	if event == nil {
		return nil, nil
	}

	if isSnapshotEvent {
		us.cursor4SnapshotEvents++
	} else {
		us.cursor4AddEvents++
	}
	return event, nil
}

func (us *UnionScanInterDirc) getSnapshotEvent(ctx context.Context) ([]types.Causet, error) {
	if us.cursor4SnapshotEvents < len(us.snapshotEvents) {
		return us.snapshotEvents[us.cursor4SnapshotEvents], nil
	}
	var err error
	us.cursor4SnapshotEvents = 0
	us.snapshotEvents = us.snapshotEvents[:0]
	for len(us.snapshotEvents) == 0 {
		err = Next(ctx, us.children[0], us.snapshotChunkBuffer)
		if err != nil || us.snapshotChunkBuffer.NumEvents() == 0 {
			return nil, err
		}
		iter := chunk.NewIterator4Chunk(us.snapshotChunkBuffer)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			var snapshotHandle ekv.Handle
			snapshotHandle, err = us.belowHandleDefCauss.BuildHandle(event)
			if err != nil {
				return nil, err
			}
			checkKey := us.causet.RecordKey(snapshotHandle)
			if _, err := us.memBufSnap.Get(context.TODO(), checkKey); err == nil {
				// If src handle appears in added rows, it means there is conflict and the transaction will fail to
				// commit, but for simplicity, we don't handle it here.
				continue
			}
			us.snapshotEvents = append(us.snapshotEvents, event.GetCausetEvent(retTypes(us.children[0])))
		}
	}
	return us.snapshotEvents[0], nil
}

func (us *UnionScanInterDirc) getAddedEvent() []types.Causet {
	var addedEvent []types.Causet
	if us.cursor4AddEvents < len(us.addedEvents) {
		addedEvent = us.addedEvents[us.cursor4AddEvents]
	}
	return addedEvent
}

// shouldPickFirstEvent picks the suiblock event in order.
// The value returned is used to determine whether to pick the first input event.
func (us *UnionScanInterDirc) shouldPickFirstEvent(a, b []types.Causet) (bool, error) {
	var isFirstEvent bool
	addedCmpSrc, err := us.compare(a, b)
	if err != nil {
		return isFirstEvent, err
	}
	// Compare result will never be 0.
	if us.desc {
		if addedCmpSrc > 0 {
			isFirstEvent = true
		}
	} else {
		if addedCmpSrc < 0 {
			isFirstEvent = true
		}
	}
	return isFirstEvent, nil
}

func (us *UnionScanInterDirc) compare(a, b []types.Causet) (int, error) {
	sc := us.ctx.GetStochastikVars().StmtCtx
	for _, defCausOff := range us.usedIndex {
		aDeferredCauset := a[defCausOff]
		bDeferredCauset := b[defCausOff]
		cmp, err := aDeferredCauset.CompareCauset(sc, &bDeferredCauset)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return us.belowHandleDefCauss.Compare(a, b)
}
