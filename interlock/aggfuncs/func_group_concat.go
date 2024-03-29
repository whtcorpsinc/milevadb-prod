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

package aggfuncs

import (
	"bytes"
	"container/heap"
	"sort"
	"sync/atomic"

	"github.com/whtcorpsinc/BerolinaSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/set"
)

type baseGroupConcat4String struct {
	baseAggFunc
	byItems []*soliton.ByItems

	sep    string
	maxLen uint64
	// According to MyALLEGROSQL, a 'group_concat' function generates exactly one 'truncated' warning during its life time, no matter
	// how many group actually truncated. 'truncated' acts as a sentinel to indicate whether this warning has already been
	// generated.
	truncated *int32
}

func (e *baseGroupConcat4String) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4GroupConcat)(pr)
	if p.buffer == nil {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.buffer.String())
	return nil
}

func (e *baseGroupConcat4String) handleTruncateError(sctx stochastikctx.Context) (err error) {
	if atomic.CompareAndSwapInt32(e.truncated, 0, 1) {
		if !sctx.GetStochastikVars().StmtCtx.TruncateAsWarning {
			return memex.ErrCutValueGroupConcat.GenWithStackByArgs(e.args[0].String())
		}
		sctx.GetStochastikVars().StmtCtx.AppendWarning(memex.ErrCutValueGroupConcat.GenWithStackByArgs(e.args[0].String()))
	}
	return nil
}

func (e *baseGroupConcat4String) truncatePartialResultIfNeed(sctx stochastikctx.Context, buffer *bytes.Buffer) (err error) {
	if e.maxLen > 0 && uint64(buffer.Len()) > e.maxLen {
		buffer.Truncate(int(e.maxLen))
		return e.handleTruncateError(sctx)
	}
	return nil
}

type basePartialResult4GroupConcat struct {
	valsBuf *bytes.Buffer
	buffer  *bytes.Buffer
}

type partialResult4GroupConcat struct {
	basePartialResult4GroupConcat
}

type groupConcat struct {
	baseGroupConcat4String
}

func (e *groupConcat) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4GroupConcat)
	p.valsBuf = &bytes.Buffer{}
	return PartialResult(p), 0
}

func (e *groupConcat) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcat)(pr)
	p.buffer = nil
}

func (e *groupConcat) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcat)(pr)
	v, isNull := "", false
	for _, event := range rowsInGroup {
		p.valsBuf.Reset()
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, event)
			if err != nil {
				return 0, err
			}
			if isNull {
				break
			}
			p.valsBuf.WriteString(v)
		}
		if isNull {
			continue
		}
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
		} else {
			p.buffer.WriteString(e.sep)
		}
		p.buffer.WriteString(p.valsBuf.String())
	}
	if p.buffer != nil {
		return 0, e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return 0, nil
}

func (e *groupConcat) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4GroupConcat)(src), (*partialResult4GroupConcat)(dst)
	if p1.buffer == nil {
		return 0, nil
	}
	if p2.buffer == nil {
		p2.buffer = p1.buffer
		return 0, nil
	}
	p2.buffer.WriteString(e.sep)
	p2.buffer.WriteString(p1.buffer.String())
	return 0, e.truncatePartialResultIfNeed(sctx, p2.buffer)
}

// SetTruncated will be called in `interlockBuilder#buildHashAgg` with duck-type.
func (e *groupConcat) SetTruncated(t *int32) {
	e.truncated = t
}

// GetTruncated will be called in `interlockBuilder#buildHashAgg` with duck-type.
func (e *groupConcat) GetTruncated() *int32 {
	return e.truncated
}

type partialResult4GroupConcatDistinct struct {
	basePartialResult4GroupConcat
	valSet            set.StringSet
	encodeBytesBuffer []byte
}

type groupConcatDistinct struct {
	baseGroupConcat4String
}

func (e *groupConcatDistinct) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4GroupConcatDistinct)
	p.valsBuf = &bytes.Buffer{}
	p.valSet = set.NewStringSet()
	return PartialResult(p), 0
}

func (e *groupConcatDistinct) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	p.buffer, p.valSet = nil, set.NewStringSet()
}

func (e *groupConcatDistinct) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatDistinct)(pr)
	v, isNull := "", false
	for _, event := range rowsInGroup {
		p.valsBuf.Reset()
		p.encodeBytesBuffer = p.encodeBytesBuffer[:0]
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, event)
			if err != nil {
				return 0, err
			}
			if isNull {
				break
			}
			p.encodeBytesBuffer = codec.EncodeBytes(p.encodeBytesBuffer, replog.Slice(v))
			p.valsBuf.WriteString(v)
		}
		if isNull {
			continue
		}
		joinedVal := string(p.encodeBytesBuffer)
		if p.valSet.Exist(joinedVal) {
			continue
		}
		p.valSet.Insert(joinedVal)
		// write separator
		if p.buffer == nil {
			p.buffer = &bytes.Buffer{}
		} else {
			p.buffer.WriteString(e.sep)
		}
		// write values
		p.buffer.WriteString(p.valsBuf.String())
	}
	if p.buffer != nil {
		return 0, e.truncatePartialResultIfNeed(sctx, p.buffer)
	}
	return 0, nil
}

// SetTruncated will be called in `interlockBuilder#buildHashAgg` with duck-type.
func (e *groupConcatDistinct) SetTruncated(t *int32) {
	e.truncated = t
}

// GetTruncated will be called in `interlockBuilder#buildHashAgg` with duck-type.
func (e *groupConcatDistinct) GetTruncated() *int32 {
	return e.truncated
}

type sortEvent struct {
	buffer  *bytes.Buffer
	byItems []*types.Causet
}

type topNEvents struct {
	rows []sortEvent
	desc []bool
	sctx stochastikctx.Context
	err  error

	currSize  uint64
	limitSize uint64
	sepSize   uint64
}

func (h topNEvents) Len() int {
	return len(h.rows)
}

func (h topNEvents) Less(i, j int) bool {
	n := len(h.rows[i].byItems)
	for k := 0; k < n; k++ {
		ret, err := h.rows[i].byItems[k].CompareCauset(h.sctx.GetStochastikVars().StmtCtx, h.rows[j].byItems[k])
		if err != nil {
			h.err = err
			return false
		}
		if h.desc[k] {
			ret = -ret
		}
		if ret > 0 {
			return true
		}
		if ret < 0 {
			return false
		}
	}
	return false
}

func (h topNEvents) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *topNEvents) Push(x interface{}) {
	h.rows = append(h.rows, x.(sortEvent))
}

func (h *topNEvents) Pop() interface{} {
	n := len(h.rows)
	x := h.rows[n-1]
	h.rows = h.rows[:n-1]
	return x
}

func (h *topNEvents) tryToAdd(event sortEvent) (truncated bool) {
	h.currSize += uint64(event.buffer.Len())
	if len(h.rows) > 0 {
		h.currSize += h.sepSize
	}
	heap.Push(h, event)
	if h.currSize <= h.limitSize {
		return false
	}

	for h.currSize > h.limitSize {
		debt := h.currSize - h.limitSize
		if uint64(h.rows[0].buffer.Len()) > debt {
			h.currSize -= debt
			h.rows[0].buffer.Truncate(h.rows[0].buffer.Len() - int(debt))
		} else {
			h.currSize -= uint64(h.rows[0].buffer.Len()) + h.sepSize
			heap.Pop(h)
		}
	}
	return true
}

func (h *topNEvents) reset() {
	h.rows = h.rows[:0]
	h.err = nil
	h.currSize = 0
}

func (h *topNEvents) concat(sep string, truncated bool) string {
	buffer := new(bytes.Buffer)
	sort.Sort(sort.Reverse(h))
	for i, event := range h.rows {
		if i != 0 {
			buffer.WriteString(sep)
		}
		buffer.Write(event.buffer.Bytes())
	}
	if truncated && uint64(buffer.Len()) < h.limitSize {
		// append the last separator, because the last separator may be truncated in tryToAdd.
		buffer.WriteString(sep)
		buffer.Truncate(int(h.limitSize))
	}
	return buffer.String()
}

type partialResult4GroupConcatOrder struct {
	topN *topNEvents
}

type groupConcatOrder struct {
	baseGroupConcat4String
}

func (e *groupConcatOrder) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4GroupConcatOrder)(pr)
	if p.topN.Len() == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.topN.concat(e.sep, *e.truncated == 1))
	return nil
}

func (e *groupConcatOrder) AllocPartialResult() (pr PartialResult, memDelta int64) {
	desc := make([]bool, len(e.byItems))
	for i, byItem := range e.byItems {
		desc[i] = byItem.Desc
	}
	p := &partialResult4GroupConcatOrder{
		topN: &topNEvents{
			desc:      desc,
			currSize:  0,
			limitSize: e.maxLen,
			sepSize:   uint64(len(e.sep)),
		},
	}
	return PartialResult(p), 0
}

func (e *groupConcatOrder) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatOrder)(pr)
	p.topN.reset()
}

func (e *groupConcatOrder) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatOrder)(pr)
	p.topN.sctx = sctx
	v, isNull := "", false
	for _, event := range rowsInGroup {
		buffer := new(bytes.Buffer)
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, event)
			if err != nil {
				return 0, err
			}
			if isNull {
				break
			}
			buffer.WriteString(v)
		}
		if isNull {
			continue
		}
		sortEvent := sortEvent{
			buffer:  buffer,
			byItems: make([]*types.Causet, 0, len(e.byItems)),
		}
		for _, byItem := range e.byItems {
			d, err := byItem.Expr.Eval(event)
			if err != nil {
				return 0, err
			}
			sortEvent.byItems = append(sortEvent.byItems, d.Clone())
		}
		truncated := p.topN.tryToAdd(sortEvent)
		if p.topN.err != nil {
			return 0, p.topN.err
		}
		if truncated {
			if err := e.handleTruncateError(sctx); err != nil {
				return 0, err
			}
		}
	}
	return 0, nil
}

func (e *groupConcatOrder) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	// If order by exists, the parallel hash aggregation is forbidden in interlockBuilder.buildHashAgg.
	// So MergePartialResult will not be called.
	return 0, terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal]).GenWithStack("groupConcatOrder.MergePartialResult should not be called")
}

// SetTruncated will be called in `interlockBuilder#buildHashAgg` with duck-type.
func (e *groupConcatOrder) SetTruncated(t *int32) {
	e.truncated = t
}

// GetTruncated will be called in `interlockBuilder#buildHashAgg` with duck-type.
func (e *groupConcatOrder) GetTruncated() *int32 {
	return e.truncated
}

type partialResult4GroupConcatOrderDistinct struct {
	topN              *topNEvents
	valSet            set.StringSet
	encodeBytesBuffer []byte
}

type groupConcatDistinctOrder struct {
	baseGroupConcat4String
}

func (e *groupConcatDistinctOrder) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	if p.topN.Len() == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendString(e.ordinal, p.topN.concat(e.sep, *e.truncated == 1))
	return nil
}

func (e *groupConcatDistinctOrder) AllocPartialResult() (pr PartialResult, memDelta int64) {
	desc := make([]bool, len(e.byItems))
	for i, byItem := range e.byItems {
		desc[i] = byItem.Desc
	}
	p := &partialResult4GroupConcatOrderDistinct{
		topN: &topNEvents{
			desc:      desc,
			currSize:  0,
			limitSize: e.maxLen,
			sepSize:   uint64(len(e.sep)),
		},
		valSet: set.NewStringSet(),
	}
	return PartialResult(p), 0
}

func (e *groupConcatDistinctOrder) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	p.topN.reset()
	p.valSet = set.NewStringSet()
}

func (e *groupConcatDistinctOrder) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4GroupConcatOrderDistinct)(pr)
	p.topN.sctx = sctx
	v, isNull := "", false
	for _, event := range rowsInGroup {
		buffer := new(bytes.Buffer)
		p.encodeBytesBuffer = p.encodeBytesBuffer[:0]
		for _, arg := range e.args {
			v, isNull, err = arg.EvalString(sctx, event)
			if err != nil {
				return 0, err
			}
			if isNull {
				break
			}
			p.encodeBytesBuffer = codec.EncodeBytes(p.encodeBytesBuffer, replog.Slice(v))
			buffer.WriteString(v)
		}
		if isNull {
			continue
		}
		joinedVal := string(p.encodeBytesBuffer)
		if p.valSet.Exist(joinedVal) {
			continue
		}
		p.valSet.Insert(joinedVal)
		sortEvent := sortEvent{
			buffer:  buffer,
			byItems: make([]*types.Causet, 0, len(e.byItems)),
		}
		for _, byItem := range e.byItems {
			d, err := byItem.Expr.Eval(event)
			if err != nil {
				return 0, err
			}
			sortEvent.byItems = append(sortEvent.byItems, d.Clone())
		}
		truncated := p.topN.tryToAdd(sortEvent)
		if p.topN.err != nil {
			return 0, p.topN.err
		}
		if truncated {
			if err := e.handleTruncateError(sctx); err != nil {
				return 0, err
			}
		}
	}
	return 0, nil
}

func (e *groupConcatDistinctOrder) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	// If order by exists, the parallel hash aggregation is forbidden in interlockBuilder.buildHashAgg.
	// So MergePartialResult will not be called.
	return 0, terror.ClassOptimizer.New(allegrosql.ErrInternal, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInternal]).GenWithStack("groupConcatDistinctOrder.MergePartialResult should not be called")
}
