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
	"context"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
)

type aggCtxsMapper map[string][]*aggregation.AggEvaluateContext

var (
	_ interlock = &hashAggInterDirc{}
	_ interlock = &streamAggInterDirc{}
)

type hashAggInterDirc struct {
	evalCtx           *evalContext
	aggExprs          []aggregation.Aggregation
	aggCtxsMap        aggCtxsMapper
	groupByExprs      []memex.Expression
	relatedDefCausOffsets []int
	event               []types.Causet
	groups            map[string]struct{}
	groupKeys         [][]byte
	groupKeyRows      [][][]byte
	executed          bool
	currGroupIdx      int
	count             int64
	execDetail        *execDetail

	src interlock
}

func (e *hashAggInterDirc) InterDircDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.InterDircDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *hashAggInterDirc) SetSrcInterDirc(exec interlock) {
	e.src = exec
}

func (e *hashAggInterDirc) GetSrcInterDirc() interlock {
	return e.src
}

func (e *hashAggInterDirc) ResetCounts() {
	e.src.ResetCounts()
}

func (e *hashAggInterDirc) Counts() []int64 {
	return e.src.Counts()
}

func (e *hashAggInterDirc) innerNext(ctx context.Context) (bool, error) {
	values, err := e.src.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if values == nil {
		return false, nil
	}
	err = e.aggregate(values)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (e *hashAggInterDirc) Cursor() ([]byte, bool) {
	panic("don't not use interlock streaming API for hash aggregation!")
}

func (e *hashAggInterDirc) Next(ctx context.Context) (value [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.uFIDelate(begin, value)
	}(time.Now())
	e.count++
	if e.aggCtxsMap == nil {
		e.aggCtxsMap = make(aggCtxsMapper)
	}
	if !e.executed {
		for {
			hasMore, err := e.innerNext(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !hasMore {
				break
			}
		}
		e.executed = true
	}

	if e.currGroupIdx >= len(e.groups) {
		return nil, nil
	}
	gk := e.groupKeys[e.currGroupIdx]
	value = make([][]byte, 0, len(e.groupByExprs)+2*len(e.aggExprs))
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		partialResults := agg.GetPartialResult(aggCtxs[i])
		for _, result := range partialResults {
			data, err := codec.EncodeValue(e.evalCtx.sc, nil, result)
			if err != nil {
				return nil, errors.Trace(err)
			}
			value = append(value, data)
		}
	}
	value = append(value, e.groupKeyRows[e.currGroupIdx]...)
	e.currGroupIdx++

	return value, nil
}

func (e *hashAggInterDirc) getGroupKey() ([]byte, [][]byte, error) {
	length := len(e.groupByExprs)
	if length == 0 {
		return nil, nil, nil
	}
	bufLen := 0
	event := make([][]byte, 0, length)
	for _, item := range e.groupByExprs {
		v, err := item.Eval(chunk.MutRowFromCausets(e.event).ToRow())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		b, err := codec.EncodeValue(e.evalCtx.sc, nil, v)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		bufLen += len(b)
		event = append(event, b)
	}
	buf := make([]byte, 0, bufLen)
	for _, col := range event {
		buf = append(buf, col...)
	}
	return buf, event, nil
}

// aggregate uFIDelates aggregate functions with event.
func (e *hashAggInterDirc) aggregate(value [][]byte) error {
	err := e.evalCtx.decodeRelatedDeferredCausetVals(e.relatedDefCausOffsets, value, e.event)
	if err != nil {
		return errors.Trace(err)
	}
	// Get group key.
	gk, gbyKeyRow, err := e.getGroupKey()
	if err != nil {
		return errors.Trace(err)
	}
	if _, ok := e.groups[string(gk)]; !ok {
		e.groups[string(gk)] = struct{}{}
		e.groupKeys = append(e.groupKeys, gk)
		e.groupKeyRows = append(e.groupKeyRows, gbyKeyRow)
	}
	// UFIDelate aggregate memexs.
	aggCtxs := e.getContexts(gk)
	for i, agg := range e.aggExprs {
		err = agg.UFIDelate(aggCtxs[i], e.evalCtx.sc, chunk.MutRowFromCausets(e.event).ToRow())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *hashAggInterDirc) getContexts(groupKey []byte) []*aggregation.AggEvaluateContext {
	groupKeyString := string(groupKey)
	aggCtxs, ok := e.aggCtxsMap[groupKeyString]
	if !ok {
		aggCtxs = make([]*aggregation.AggEvaluateContext, 0, len(e.aggExprs))
		for _, agg := range e.aggExprs {
			aggCtxs = append(aggCtxs, agg.CreateContext(e.evalCtx.sc))
		}
		e.aggCtxsMap[groupKeyString] = aggCtxs
	}
	return aggCtxs
}

type streamAggInterDirc struct {
	evalCtx           *evalContext
	aggExprs          []aggregation.Aggregation
	aggCtxs           []*aggregation.AggEvaluateContext
	groupByExprs      []memex.Expression
	relatedDefCausOffsets []int
	event               []types.Causet
	tmpGroupByRow     []types.Causet
	currGroupByRow    []types.Causet
	nextGroupByRow    []types.Causet
	currGroupByValues [][]byte
	executed          bool
	hasData           bool
	count             int64
	execDetail        *execDetail

	src interlock
}

func (e *streamAggInterDirc) InterDircDetails() []*execDetail {
	var suffix []*execDetail
	if e.src != nil {
		suffix = e.src.InterDircDetails()
	}
	return append(suffix, e.execDetail)
}

func (e *streamAggInterDirc) SetSrcInterDirc(exec interlock) {
	e.src = exec
}

func (e *streamAggInterDirc) GetSrcInterDirc() interlock {
	return e.src
}

func (e *streamAggInterDirc) ResetCounts() {
	e.src.ResetCounts()
}

func (e *streamAggInterDirc) Counts() []int64 {
	return e.src.Counts()
}

func (e *streamAggInterDirc) getPartialResult() ([][]byte, error) {
	value := make([][]byte, 0, len(e.groupByExprs)+2*len(e.aggExprs))
	for i, agg := range e.aggExprs {
		partialResults := agg.GetPartialResult(e.aggCtxs[i])
		for _, result := range partialResults {
			data, err := codec.EncodeValue(e.evalCtx.sc, nil, result)
			if err != nil {
				return nil, errors.Trace(err)
			}
			value = append(value, data)
		}
		// Clear the aggregate context.
		e.aggCtxs[i] = agg.CreateContext(e.evalCtx.sc)
	}
	e.currGroupByValues = e.currGroupByValues[:0]
	for _, d := range e.currGroupByRow {
		buf, err := codec.EncodeValue(e.evalCtx.sc, nil, d)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.currGroupByValues = append(e.currGroupByValues, buf)
	}
	e.currGroupByRow = types.CloneRow(e.nextGroupByRow)
	return append(value, e.currGroupByValues...), nil
}

func (e *streamAggInterDirc) meetNewGroup(event [][]byte) (bool, error) {
	if len(e.groupByExprs) == 0 {
		return false, nil
	}

	e.tmpGroupByRow = e.tmpGroupByRow[:0]
	matched, firstGroup := true, false
	if e.nextGroupByRow == nil {
		matched, firstGroup = false, true
	}
	for i, item := range e.groupByExprs {
		d, err := item.Eval(chunk.MutRowFromCausets(e.event).ToRow())
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			c, err := d.CompareCauset(e.evalCtx.sc, &e.nextGroupByRow[i])
			if err != nil {
				return false, errors.Trace(err)
			}
			matched = c == 0
		}
		e.tmpGroupByRow = append(e.tmpGroupByRow, d)
	}
	if firstGroup {
		e.currGroupByRow = types.CloneRow(e.tmpGroupByRow)
	}
	if matched {
		return false, nil
	}
	e.nextGroupByRow = e.tmpGroupByRow
	return !firstGroup, nil
}

func (e *streamAggInterDirc) Cursor() ([]byte, bool) {
	panic("don't not use interlock streaming API for stream aggregation!")
}

func (e *streamAggInterDirc) Next(ctx context.Context) (retRow [][]byte, err error) {
	defer func(begin time.Time) {
		e.execDetail.uFIDelate(begin, retRow)
	}(time.Now())
	e.count++
	if e.executed {
		return nil, nil
	}

	for {
		values, err := e.src.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if values == nil {
			e.executed = true
			if !e.hasData && len(e.groupByExprs) > 0 {
				return nil, nil
			}
			return e.getPartialResult()
		}

		e.hasData = true
		err = e.evalCtx.decodeRelatedDeferredCausetVals(e.relatedDefCausOffsets, values, e.event)
		if err != nil {
			return nil, errors.Trace(err)
		}
		newGroup, err := e.meetNewGroup(values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if newGroup {
			retRow, err = e.getPartialResult()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		for i, agg := range e.aggExprs {
			err = agg.UFIDelate(e.aggCtxs[i], e.evalCtx.sc, chunk.MutRowFromCausets(e.event).ToRow())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if newGroup {
			return retRow, nil
		}
	}
}
