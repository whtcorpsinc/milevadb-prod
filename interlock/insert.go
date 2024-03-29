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
	"encoding/hex"
	"fmt"
	"runtime/trace"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// InsertInterDirc represents an insert interlock.
type InsertInterDirc struct {
	*InsertValues
	OnDuplicate    []*memex.Assignment
	evalBuffer4Dup chunk.MutEvent
	curInsertVals  chunk.MutEvent
	row4UFIDelate  []types.Causet

	Priority allegrosql.PriorityEnum
}

func (e *InsertInterDirc) exec(ctx context.Context, rows [][]types.Causet) error {
	defer trace.StartRegion(ctx, "InsertInterDirc").End()
	logutil.Eventf(ctx, "insert %d rows into causet `%s`", len(rows), stringutil.MemoizeStr(func() string {
		var tblName string
		if spacetime := e.Block.Meta(); spacetime != nil {
			tblName = spacetime.Name.L
		}
		return tblName
	}))
	// If milevadb_batch_insert is ON and not in a transaction, we could use BatchInsert mode.
	sessVars := e.ctx.GetStochastikVars()
	defer sessVars.CleanBuffers()
	ignoreErr := sessVars.StmtCtx.DupKeyAsWarning

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	txnSize := txn.Size()
	sessVars.StmtCtx.AddRecordEvents(uint64(len(rows)))
	// If you use the IGNORE keyword, duplicate-key error that occurs while executing the INSERT memex are ignored.
	// For example, without IGNORE, a event that duplicates an existing UNIQUE index or PRIMARY KEY value in
	// the causet causes a duplicate-key error and the memex is aborted. With IGNORE, the event is discarded and no error occurs.
	// However, if the `on duplicate uFIDelate` is also specified, the duplicated event will be uFIDelated.
	// Using BatchGet in insert ignore to mark rows as duplicated before we add records to the causet.
	// If `ON DUPLICATE KEY UFIDelATE` is specified, and no `IGNORE` keyword,
	// the to-be-insert rows will be check on duplicate keys and uFIDelate to the new rows.
	if len(e.OnDuplicate) > 0 {
		err := e.batchUFIDelateDupEvents(ctx, rows)
		if err != nil {
			return err
		}
	} else if ignoreErr {
		err := e.batchCheckAndInsert(ctx, rows, e.addRecord)
		if err != nil {
			return err
		}
	} else {
		for i, event := range rows {
			var err error
			sizeHintStep := int(sessVars.ShardAllocateStep)
			if i%sizeHintStep == 0 {
				sizeHint := sizeHintStep
				remain := len(rows) - i
				if sizeHint > remain {
					sizeHint = remain
				}
				err = e.addRecordWithAutoIDHint(ctx, event, sizeHint)
			} else {
				err = e.addRecord(ctx, event)
			}
			if err != nil {
				return err
			}
		}
	}
	e.memTracker.Consume(int64(txn.Size() - txnSize))
	return nil
}

func prefetchUniqueIndices(ctx context.Context, txn ekv.Transaction, rows []toBeCheckedEvent) (map[string][]byte, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("prefetchUniqueIndices", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	nKeys := 0
	for _, r := range rows {
		if r.handleKey != nil {
			nKeys++
		}
		nKeys += len(r.uniqueKeys)
	}
	batchKeys := make([]ekv.Key, 0, nKeys)
	for _, r := range rows {
		if r.handleKey != nil {
			batchKeys = append(batchKeys, r.handleKey.newKey)
		}
		for _, k := range r.uniqueKeys {
			batchKeys = append(batchKeys, k.newKey)
		}
	}
	return txn.BatchGet(ctx, batchKeys)
}

func prefetchConflictedOldEvents(ctx context.Context, txn ekv.Transaction, rows []toBeCheckedEvent, values map[string][]byte) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("prefetchConflictedOldEvents", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	batchKeys := make([]ekv.Key, 0, len(rows))
	for _, r := range rows {
		for _, uk := range r.uniqueKeys {
			if val, found := values[string(uk.newKey)]; found {
				handle, err := blockcodec.DecodeHandleInUniqueIndexValue(val, uk.commonHandle)
				if err != nil {
					return err
				}
				batchKeys = append(batchKeys, r.t.RecordKey(handle))
			}
		}
	}
	_, err := txn.BatchGet(ctx, batchKeys)
	return err
}

func prefetchDataCache(ctx context.Context, txn ekv.Transaction, rows []toBeCheckedEvent) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("prefetchDataCache", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	values, err := prefetchUniqueIndices(ctx, txn, rows)
	if err != nil {
		return err
	}
	return prefetchConflictedOldEvents(ctx, txn, rows, values)
}

// uFIDelateDupEvent uFIDelates a duplicate event to a new event.
func (e *InsertInterDirc) uFIDelateDupEvent(ctx context.Context, txn ekv.Transaction, event toBeCheckedEvent, handle ekv.Handle, onDuplicate []*memex.Assignment) error {
	oldEvent, err := getOldEvent(ctx, e.ctx, txn, event.t, handle, e.GenExprs)
	if err != nil {
		return err
	}

	err = e.doDupEventUFIDelate(ctx, handle, oldEvent, event.event, e.OnDuplicate)
	if e.ctx.GetStochastikVars().StmtCtx.DupKeyAsWarning && ekv.ErrKeyExists.Equal(err) {
		e.ctx.GetStochastikVars().StmtCtx.AppendWarning(err)
		return nil
	}
	return err
}

// batchUFIDelateDupEvents uFIDelates multi-rows in batch if they are duplicate with rows in causet.
func (e *InsertInterDirc) batchUFIDelateDupEvents(ctx context.Context, newEvents [][]types.Causet) error {
	// Get keys need to be checked.
	toBeCheckedEvents, err := getKeysNeedCheck(ctx, e.ctx, e.Block, newEvents)
	if err != nil {
		return err
	}

	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}

	if e.defCauslectRuntimeStatsEnabled() {
		if snapshot := txn.GetSnapshot(); snapshot != nil {
			snapshot.SetOption(ekv.DefCauslectRuntimeStats, e.stats.SnapshotRuntimeStats)
			defer snapshot.DelOption(ekv.DefCauslectRuntimeStats)
		}
	}

	// Use BatchGet to fill cache.
	// It's an optimization and could be removed without affecting correctness.
	if err = prefetchDataCache(ctx, txn, toBeCheckedEvents); err != nil {
		return err
	}

	for i, r := range toBeCheckedEvents {
		if r.handleKey != nil {
			handle, err := blockcodec.DecodeEventKey(r.handleKey.newKey)
			if err != nil {
				return err
			}

			err = e.uFIDelateDupEvent(ctx, txn, r, handle, e.OnDuplicate)
			if err == nil {
				continue
			}
			if !ekv.IsErrNotFound(err) {
				return err
			}
		}

		for _, uk := range r.uniqueKeys {
			val, err := txn.Get(ctx, uk.newKey)
			if err != nil {
				if ekv.IsErrNotFound(err) {
					continue
				}
				return err
			}
			handle, err := blockcodec.DecodeHandleInUniqueIndexValue(val, uk.commonHandle)
			if err != nil {
				return err
			}

			err = e.uFIDelateDupEvent(ctx, txn, r, handle, e.OnDuplicate)
			if err != nil {
				if ekv.IsErrNotFound(err) {
					// Data index inconsistent? A unique key provide the handle information, but the
					// handle points to nothing.
					logutil.BgLogger().Error("get old event failed when insert on dup",
						zap.String("uniqueKey", hex.EncodeToString(uk.newKey)),
						zap.Stringer("handle", handle),
						zap.String("toBeInsertedEvent", types.CausetsToStrNoErr(r.event)))
				}
				return err
			}

			newEvents[i] = nil
			break
		}

		// If event was checked with no duplicate keys,
		// we should do insert the event,
		// and key-values should be filled back to dupOldEventValues for the further event check,
		// due to there may be duplicate keys inside the insert memex.
		if newEvents[i] != nil {
			err := e.addRecord(ctx, newEvents[i])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *InsertInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if len(e.children) > 0 && e.children[0] != nil {
		return insertEventsFromSelect(ctx, e)
	}
	return insertEvents(ctx, e)
}

// Close implements the InterlockingDirectorate Close interface.
func (e *InsertInterDirc) Close() error {
	e.ctx.GetStochastikVars().CurrInsertValues = chunk.Event{}
	e.setMessage()
	if e.SelectInterDirc != nil {
		return e.SelectInterDirc.Close()
	}
	return nil
}

// Open implements the InterlockingDirectorate Open interface.
func (e *InsertInterDirc) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)

	if e.OnDuplicate != nil {
		e.initEvalBuffer4Dup()
	}
	if e.SelectInterDirc != nil {
		return e.SelectInterDirc.Open(ctx)
	}
	if !e.allAssignmentsAreConstant {
		e.initEvalBuffer()
	}
	return nil
}

func (e *InsertInterDirc) initEvalBuffer4Dup() {
	// Use public defCausumns for new event.
	numDefCauss := len(e.Block.DefCauss())
	// Use wriblock defCausumns for old event for uFIDelate.
	numWriblockDefCauss := len(e.Block.WriblockDefCauss())

	evalBufferTypes := make([]*types.FieldType, 0, numDefCauss+numWriblockDefCauss)

	// Append the old event before the new event, to be consistent with "Schema4OnDuplicate" in the "Insert" PhysicalCauset.
	for _, defCaus := range e.Block.WriblockDefCauss() {
		evalBufferTypes = append(evalBufferTypes, &defCaus.FieldType)
	}
	for _, defCaus := range e.Block.DefCauss() {
		evalBufferTypes = append(evalBufferTypes, &defCaus.FieldType)
	}
	if e.hasExtraHandle {
		evalBufferTypes = append(evalBufferTypes, types.NewFieldType(allegrosql.TypeLonglong))
	}
	e.evalBuffer4Dup = chunk.MutEventFromTypes(evalBufferTypes)
	e.curInsertVals = chunk.MutEventFromTypes(evalBufferTypes[numWriblockDefCauss:])
	e.row4UFIDelate = make([]types.Causet, 0, len(evalBufferTypes))
}

// doDupEventUFIDelate uFIDelates the duplicate event.
func (e *InsertInterDirc) doDupEventUFIDelate(ctx context.Context, handle ekv.Handle, oldEvent []types.Causet, newEvent []types.Causet,
	defcaus []*memex.Assignment) error {
	assignFlag := make([]bool, len(e.Block.WriblockDefCauss()))
	// See http://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	e.curInsertVals.SetCausets(newEvent...)
	e.ctx.GetStochastikVars().CurrInsertValues = e.curInsertVals.ToEvent()

	// NOTE: In order to execute the memex inside the defCausumn assignment,
	// we have to put the value of "oldEvent" before "newEvent" in "row4UFIDelate" to
	// be consistent with "Schema4OnDuplicate" in the "Insert" PhysicalCauset.
	e.row4UFIDelate = e.row4UFIDelate[:0]
	e.row4UFIDelate = append(e.row4UFIDelate, oldEvent...)
	e.row4UFIDelate = append(e.row4UFIDelate, newEvent...)

	// UFIDelate old event when the key is duplicated.
	e.evalBuffer4Dup.SetCausets(e.row4UFIDelate...)
	for _, defCaus := range defcaus {
		val, err1 := defCaus.Expr.Eval(e.evalBuffer4Dup.ToEvent())
		if err1 != nil {
			return err1
		}
		e.row4UFIDelate[defCaus.DefCaus.Index], err1 = causet.CastValue(e.ctx, val, defCaus.DefCaus.ToInfo(), false, false)
		if err1 != nil {
			return err1
		}
		e.evalBuffer4Dup.SetCauset(defCaus.DefCaus.Index, e.row4UFIDelate[defCaus.DefCaus.Index])
		assignFlag[defCaus.DefCaus.Index] = true
	}

	newData := e.row4UFIDelate[:len(oldEvent)]
	_, err := uFIDelateRecord(ctx, e.ctx, handle, oldEvent, newData, assignFlag, e.Block, true, e.memTracker)
	if err != nil {
		return err
	}
	return nil
}

// setMessage sets info message(ERR_INSERT_INFO) generated by INSERT memex
func (e *InsertInterDirc) setMessage() {
	stmtCtx := e.ctx.GetStochastikVars().StmtCtx
	numRecords := stmtCtx.RecordEvents()
	if e.SelectInterDirc != nil || numRecords > 1 {
		numWarnings := stmtCtx.WarningCount()
		var numDuplicates uint64
		if stmtCtx.DupKeyAsWarning {
			// if ignoreErr
			numDuplicates = numRecords - stmtCtx.CopiedEvents()
		} else {
			if e.ctx.GetStochastikVars().ClientCapability&allegrosql.ClientFoundEvents > 0 {
				numDuplicates = stmtCtx.TouchedEvents()
			} else {
				numDuplicates = stmtCtx.UFIDelatedEvents()
			}
		}
		msg := fmt.Sprintf(allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInsertInfo], numRecords, numDuplicates, numWarnings)
		stmtCtx.SetMessage(msg)
	}
}
