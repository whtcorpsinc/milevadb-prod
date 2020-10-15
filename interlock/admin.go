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
	"math"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/allegrosql"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"go.uber.org/zap"
)

var (
	_ InterlockingDirectorate = &ChecHoTTexRangeInterDirc{}
	_ InterlockingDirectorate = &RecoverIndexInterDirc{}
	_ InterlockingDirectorate = &CleanupIndexInterDirc{}
)

// ChecHoTTexRangeInterDirc outputs the index values which has handle between begin and end.
type ChecHoTTexRangeInterDirc struct {
	baseInterlockingDirectorate

	causet    *perceptron.BlockInfo
	index    *perceptron.IndexInfo
	is       schemareplicant.SchemaReplicant
	startKey []types.Causet

	handleRanges []ast.HandleRange
	srcChunk     *chunk.Chunk

	result allegrosql.SelectResult
	defcaus   []*perceptron.DeferredCausetInfo
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ChecHoTTexRangeInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	handleIdx := e.schemaReplicant.Len() - 1
	for {
		err := e.result.Next(ctx, e.srcChunk)
		if err != nil {
			return err
		}
		if e.srcChunk.NumEvents() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			handle := event.GetInt64(handleIdx)
			for _, hr := range e.handleRanges {
				if handle >= hr.Begin && handle < hr.End {
					req.AppendEvent(event)
					break
				}
			}
		}
		if req.NumEvents() > 0 {
			return nil
		}
	}
}

// Open implements the InterlockingDirectorate Open interface.
func (e *ChecHoTTexRangeInterDirc) Open(ctx context.Context) error {
	tDefCauss := e.causet.DefCauss()
	for _, ic := range e.index.DeferredCausets {
		defCaus := tDefCauss[ic.Offset]
		e.defcaus = append(e.defcaus, defCaus)
	}

	defCausTypeForHandle := e.schemaReplicant.DeferredCausets[len(e.defcaus)].RetType
	e.defcaus = append(e.defcaus, &perceptron.DeferredCausetInfo{
		ID:        perceptron.ExtraHandleID,
		Name:      perceptron.ExtraHandleName,
		FieldType: *defCausTypeForHandle,
	})

	e.srcChunk = newFirstChunk(e)
	posetPosetDagPB, err := e.buildPosetDagPB()
	if err != nil {
		return err
	}
	sc := e.ctx.GetStochastikVars().StmtCtx
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return nil
	}
	var builder allegrosql.RequestBuilder
	kvReq, err := builder.SetIndexRanges(sc, e.causet.ID, e.index.ID, ranger.FullRange()).
		SetPosetDagRequest(posetPosetDagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		Build()
	if err != nil {
		return err
	}

	e.result, err = allegrosql.Select(ctx, e.ctx, kvReq, e.retFieldTypes, statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return err
	}
	e.result.Fetch(ctx)
	return nil
}

func (e *ChecHoTTexRangeInterDirc) buildPosetDagPB() (*fidelpb.PosetDagRequest, error) {
	posetPosetDagReq := &fidelpb.PosetDagRequest{}
	posetPosetDagReq.TimeZoneName, posetPosetDagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetStochastikVars().Location())
	sc := e.ctx.GetStochastikVars().StmtCtx
	posetPosetDagReq.Flags = sc.PushDownFlags()
	for i := range e.schemaReplicant.DeferredCausets {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(i))
	}
	execPB := e.constructIndexScanPB()
	posetPosetDagReq.InterlockingDirectorates = append(posetPosetDagReq.InterlockingDirectorates, execPB)

	err := causetcore.SetPBDeferredCausetsDefaultValue(e.ctx, posetPosetDagReq.InterlockingDirectorates[0].IdxScan.DeferredCausets, e.defcaus)
	if err != nil {
		return nil, err
	}
	allegrosql.SetEncodeType(e.ctx, posetPosetDagReq)
	return posetPosetDagReq, nil
}

func (e *ChecHoTTexRangeInterDirc) constructIndexScanPB() *fidelpb.InterlockingDirectorate {
	idxInterDirc := &fidelpb.IndexScan{
		BlockId: e.causet.ID,
		IndexId: e.index.ID,
		DeferredCausets: soliton.DeferredCausetsToProto(e.defcaus, e.causet.PKIsHandle),
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeIndexScan, IdxScan: idxInterDirc}
}

// Close implements the InterlockingDirectorate Close interface.
func (e *ChecHoTTexRangeInterDirc) Close() error {
	return nil
}

// RecoverIndexInterDirc represents a recover index interlock.
// It is built from "admin recover index" memex, is used to backfill
// corrupted index.
type RecoverIndexInterDirc struct {
	baseInterlockingDirectorate

	done bool

	index      causet.Index
	causet      causet.Block
	physicalID int64
	batchSize  int

	defCausumns       []*perceptron.DeferredCausetInfo
	defCausFieldTypes []*types.FieldType
	srcChunk      *chunk.Chunk
	handleDefCauss    causetcore.HandleDefCauss

	// below buf is used to reduce allocations.
	recoverEvents []recoverEvents
	idxValsBufs [][]types.Causet
	idxKeyBufs  [][]byte
	batchKeys   []ekv.Key
}

func (e *RecoverIndexInterDirc) defCausumnsTypes() []*types.FieldType {
	if e.defCausFieldTypes != nil {
		return e.defCausFieldTypes
	}

	e.defCausFieldTypes = make([]*types.FieldType, 0, len(e.defCausumns))
	for _, defCaus := range e.defCausumns {
		e.defCausFieldTypes = append(e.defCausFieldTypes, &defCaus.FieldType)
	}
	return e.defCausFieldTypes
}

// Open implements the InterlockingDirectorate Open interface.
func (e *RecoverIndexInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}

	e.srcChunk = chunk.New(e.defCausumnsTypes(), e.initCap, e.maxChunkSize)
	e.batchSize = 2048
	e.recoverEvents = make([]recoverEvents, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Causet, e.batchSize)
	e.idxKeyBufs = make([][]byte, e.batchSize)
	return nil
}

func (e *RecoverIndexInterDirc) constructBlockScanPB(tblInfo *perceptron.BlockInfo, defCausInfos []*perceptron.DeferredCausetInfo) (*fidelpb.InterlockingDirectorate, error) {
	tblScan := blocks.BuildBlockScanFromInfos(tblInfo, defCausInfos)
	tblScan.BlockId = e.physicalID
	err := causetcore.SetPBDeferredCausetsDefaultValue(e.ctx, tblScan.DeferredCausets, defCausInfos)
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeBlockScan, TblScan: tblScan}, err
}

func (e *RecoverIndexInterDirc) constructLimitPB(count uint64) *fidelpb.InterlockingDirectorate {
	limitInterDirc := &fidelpb.Limit{
		Limit: count,
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeLimit, Limit: limitInterDirc}
}

func (e *RecoverIndexInterDirc) buildPosetDagPB(txn ekv.Transaction, limitCnt uint64) (*fidelpb.PosetDagRequest, error) {
	posetPosetDagReq := &fidelpb.PosetDagRequest{}
	posetPosetDagReq.TimeZoneName, posetPosetDagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetStochastikVars().Location())
	sc := e.ctx.GetStochastikVars().StmtCtx
	posetPosetDagReq.Flags = sc.PushDownFlags()
	for i := range e.defCausumns {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(i))
	}

	tblScanInterDirc, err := e.constructBlockScanPB(e.causet.Meta(), e.defCausumns)
	if err != nil {
		return nil, err
	}
	posetPosetDagReq.InterlockingDirectorates = append(posetPosetDagReq.InterlockingDirectorates, tblScanInterDirc)

	limitInterDirc := e.constructLimitPB(limitCnt)
	posetPosetDagReq.InterlockingDirectorates = append(posetPosetDagReq.InterlockingDirectorates, limitInterDirc)
	allegrosql.SetEncodeType(e.ctx, posetPosetDagReq)
	return posetPosetDagReq, nil
}

func (e *RecoverIndexInterDirc) buildBlockScan(ctx context.Context, txn ekv.Transaction, startHandle ekv.Handle, limitCnt uint64) (allegrosql.SelectResult, error) {
	posetPosetDagPB, err := e.buildPosetDagPB(txn, limitCnt)
	if err != nil {
		return nil, err
	}
	var builder allegrosql.RequestBuilder
	builder.KeyRanges, err = buildRecoverIndexKeyRanges(e.ctx.GetStochastikVars().StmtCtx, e.physicalID, startHandle)
	if err != nil {
		return nil, err
	}
	kvReq, err := builder.
		SetPosetDagRequest(posetPosetDagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		Build()
	if err != nil {
		return nil, err
	}

	// Actually, with limitCnt, the match quantum maybe only in one region, so let the concurrency to be 1,
	// avoid unnecessary region scan.
	kvReq.Concurrency = 1
	result, err := allegrosql.Select(ctx, e.ctx, kvReq, e.defCausumnsTypes(), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

// buildRecoverIndexKeyRanges build a KeyRange: (startHandle, unlimited).
func buildRecoverIndexKeyRanges(sctx *stmtctx.StatementContext, tid int64, startHandle ekv.Handle) ([]ekv.KeyRange, error) {
	var startKey []byte
	if startHandle == nil {
		startKey = blockcodec.EncodeEventKey(tid, []byte{codec.NilFlag})
	} else {
		startKey = blockcodec.EncodeEventKey(tid, startHandle.Next().Encoded())
	}
	maxVal, err := codec.EncodeKey(sctx, nil, types.MaxValueCauset())
	if err != nil {
		return nil, errors.Trace(err)
	}
	endKey := blockcodec.EncodeEventKey(tid, maxVal)
	return []ekv.KeyRange{{StartKey: startKey, EndKey: endKey}}, nil
}

type backfillResult struct {
	currentHandle ekv.Handle
	addedCount    int64
	scanEventCount  int64
}

func (e *RecoverIndexInterDirc) backfillIndex(ctx context.Context) (int64, int64, error) {
	var (
		currentHandle ekv.Handle = nil
		totalAddedCnt           = int64(0)
		totalScanCnt            = int64(0)
		lastLogCnt              = int64(0)
		result        backfillResult
	)
	for {
		errInTxn := ekv.RunInNewTxn(e.ctx.GetStore(), true, func(txn ekv.Transaction) error {
			var err error
			result, err = e.backfillIndexInTxn(ctx, txn, currentHandle)
			return err
		})
		if errInTxn != nil {
			return totalAddedCnt, totalScanCnt, errInTxn
		}
		totalAddedCnt += result.addedCount
		totalScanCnt += result.scanEventCount
		if totalScanCnt-lastLogCnt >= 50000 {
			lastLogCnt = totalScanCnt
			logutil.Logger(ctx).Info("recover index", zap.String("causet", e.causet.Meta().Name.O),
				zap.String("index", e.index.Meta().Name.O), zap.Int64("totalAddedCnt", totalAddedCnt),
				zap.Int64("totalScanCnt", totalScanCnt), zap.Stringer("currentHandle", result.currentHandle))
		}

		// no more rows
		if result.scanEventCount == 0 {
			break
		}
		currentHandle = result.currentHandle
	}
	return totalAddedCnt, totalScanCnt, nil
}

type recoverEvents struct {
	handle  ekv.Handle
	idxVals []types.Causet
	skip    bool
}

func (e *RecoverIndexInterDirc) fetchRecoverEvents(ctx context.Context, srcResult allegrosql.SelectResult, result *backfillResult) ([]recoverEvents, error) {
	e.recoverEvents = e.recoverEvents[:0]
	idxValLen := len(e.index.Meta().DeferredCausets)
	result.scanEventCount = 0

	for {
		err := srcResult.Next(ctx, e.srcChunk)
		if err != nil {
			return nil, err
		}

		if e.srcChunk.NumEvents() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(e.srcChunk)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			if result.scanEventCount >= int64(e.batchSize) {
				return e.recoverEvents, nil
			}
			handle, err := e.handleDefCauss.BuildHandle(event)
			if err != nil {
				return nil, err
			}
			idxVals := extractIdxVals(event, e.idxValsBufs[result.scanEventCount], e.defCausFieldTypes, idxValLen)
			e.idxValsBufs[result.scanEventCount] = idxVals
			e.recoverEvents = append(e.recoverEvents, recoverEvents{handle: handle, idxVals: idxVals, skip: false})
			result.scanEventCount++
			result.currentHandle = handle
		}
	}

	return e.recoverEvents, nil
}

func (e *RecoverIndexInterDirc) batchMarkDup(txn ekv.Transaction, rows []recoverEvents) error {
	if len(rows) == 0 {
		return nil
	}
	e.batchKeys = e.batchKeys[:0]
	sc := e.ctx.GetStochastikVars().StmtCtx
	distinctFlags := make([]bool, len(rows))
	for i, event := range rows {
		idxKey, distinct, err := e.index.GenIndexKey(sc, event.idxVals, event.handle, e.idxKeyBufs[i])
		if err != nil {
			return err
		}
		e.idxKeyBufs[i] = idxKey

		e.batchKeys = append(e.batchKeys, idxKey)
		distinctFlags[i] = distinct
	}

	values, err := txn.BatchGet(context.Background(), e.batchKeys)
	if err != nil {
		return err
	}

	// 1. unique-key is duplicate and the handle is equal, skip it.
	// 2. unique-key is duplicate and the handle is not equal, data is not consistent, log it and skip it.
	// 3. non-unique-key is duplicate, skip it.
	isCommonHandle := e.causet.Meta().IsCommonHandle
	for i, key := range e.batchKeys {
		if val, found := values[string(key)]; found {
			if distinctFlags[i] {
				handle, err1 := blockcodec.DecodeHandleInUniqueIndexValue(val, isCommonHandle)
				if err1 != nil {
					return err1
				}

				if handle.Compare(rows[i].handle) != 0 {
					logutil.BgLogger().Warn("recover index: the constraint of unique index is broken, handle in index is not equal to handle in causet",
						zap.String("index", e.index.Meta().Name.O), zap.ByteString("indexKey", key),
						zap.Stringer("handleInBlock", rows[i].handle), zap.Stringer("handleInIndex", handle))
				}
			}
			rows[i].skip = true
		}
	}
	return nil
}

func (e *RecoverIndexInterDirc) backfillIndexInTxn(ctx context.Context, txn ekv.Transaction, currentHandle ekv.Handle) (result backfillResult, err error) {
	srcResult, err := e.buildBlockScan(ctx, txn, currentHandle, uint64(e.batchSize))
	if err != nil {
		return result, err
	}
	defer terror.Call(srcResult.Close)

	rows, err := e.fetchRecoverEvents(ctx, srcResult, &result)
	if err != nil {
		return result, err
	}

	err = e.batchMarkDup(txn, rows)
	if err != nil {
		return result, err
	}

	// Constrains is already checked.
	e.ctx.GetStochastikVars().StmtCtx.BatchCheck = true
	for _, event := range rows {
		if event.skip {
			continue
		}

		recordKey := e.causet.RecordKey(event.handle)
		err := txn.LockKeys(ctx, new(ekv.LockCtx), recordKey)
		if err != nil {
			return result, err
		}

		_, err = e.index.Create(e.ctx, txn.GetUnionStore(), event.idxVals, event.handle)
		if err != nil {
			return result, err
		}
		result.addedCount++
	}
	return result, nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *RecoverIndexInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	recoveringClusteredIndex := e.index.Meta().Primary && e.causet.Meta().IsCommonHandle
	if recoveringClusteredIndex {
		req.AppendInt64(0, 0)
		req.AppendInt64(1, 0)
		e.done = true
		return nil
	}
	var totalAddedCnt, totalScanCnt int64
	var err error
	if tbl, ok := e.causet.(causet.PartitionedBlock); ok {
		pi := e.causet.Meta().GetPartitionInfo()
		for _, p := range pi.Definitions {
			e.causet = tbl.GetPartition(p.ID)
			e.index = blocks.GetWriblockIndexByName(e.index.Meta().Name.L, e.causet)
			e.physicalID = p.ID
			addedCnt, scanCnt, err := e.backfillIndex(ctx)
			totalAddedCnt += addedCnt
			totalScanCnt += scanCnt
			if err != nil {
				return err
			}
		}
	} else {
		totalAddedCnt, totalScanCnt, err = e.backfillIndex(ctx)
		if err != nil {
			return err
		}
	}

	req.AppendInt64(0, totalAddedCnt)
	req.AppendInt64(1, totalScanCnt)
	e.done = true
	return nil
}

// CleanupIndexInterDirc represents a cleanup index interlock.
// It is built from "admin cleanup index" memex, is used to delete
// dangling index data.
type CleanupIndexInterDirc struct {
	baseInterlockingDirectorate

	done      bool
	removeCnt uint64

	index      causet.Index
	causet      causet.Block
	physicalID int64

	defCausumns          []*perceptron.DeferredCausetInfo
	idxDefCausFieldTypes []*types.FieldType
	idxChunk         *chunk.Chunk
	handleDefCauss       causetcore.HandleDefCauss

	idxValues   *ekv.HandleMap // ekv.Handle -> [][]types.Causet
	batchSize   uint64
	batchKeys   []ekv.Key
	idxValsBufs [][]types.Causet
	lastIdxKey  []byte
	scanEventCnt  uint64
}

func (e *CleanupIndexInterDirc) getIdxDefCausTypes() []*types.FieldType {
	if e.idxDefCausFieldTypes != nil {
		return e.idxDefCausFieldTypes
	}
	e.idxDefCausFieldTypes = make([]*types.FieldType, 0, len(e.defCausumns))
	for _, defCaus := range e.defCausumns {
		e.idxDefCausFieldTypes = append(e.idxDefCausFieldTypes, &defCaus.FieldType)
	}
	return e.idxDefCausFieldTypes
}

func (e *CleanupIndexInterDirc) batchGetRecord(txn ekv.Transaction) (map[string][]byte, error) {
	e.idxValues.Range(func(h ekv.Handle, _ interface{}) bool {
		e.batchKeys = append(e.batchKeys, e.causet.RecordKey(h))
		return true
	})
	values, err := txn.BatchGet(context.Background(), e.batchKeys)
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (e *CleanupIndexInterDirc) deleteDanglingIdx(txn ekv.Transaction, values map[string][]byte) error {
	for _, k := range e.batchKeys {
		if _, found := values[string(k)]; !found {
			_, handle, err := blockcodec.DecodeRecordKey(k)
			if err != nil {
				return err
			}
			handleIdxValsGroup, ok := e.idxValues.Get(handle)
			if !ok {
				return errors.Trace(errors.Errorf("batch keys are inconsistent with handles"))
			}
			for _, handleIdxVals := range handleIdxValsGroup.([][]types.Causet) {
				if err := e.index.Delete(e.ctx.GetStochastikVars().StmtCtx, txn, handleIdxVals, handle); err != nil {
					return err
				}
				e.removeCnt++
				if e.removeCnt%e.batchSize == 0 {
					logutil.BgLogger().Info("clean up dangling index", zap.String("causet", e.causet.Meta().Name.String()),
						zap.String("index", e.index.Meta().Name.String()), zap.Uint64("count", e.removeCnt))
				}
			}
		}
	}
	return nil
}

func extractIdxVals(event chunk.Event, idxVals []types.Causet,
	fieldTypes []*types.FieldType, idxValLen int) []types.Causet {
	if cap(idxVals) < idxValLen {
		idxVals = make([]types.Causet, idxValLen)
	} else {
		idxVals = idxVals[:idxValLen]
	}

	for i := 0; i < idxValLen; i++ {
		defCausVal := event.GetCauset(i, fieldTypes[i])
		defCausVal.Copy(&idxVals[i])
	}
	return idxVals
}

func (e *CleanupIndexInterDirc) fetchIndex(ctx context.Context, txn ekv.Transaction) error {
	result, err := e.buildIndexScan(ctx, txn)
	if err != nil {
		return err
	}
	defer terror.Call(result.Close)

	sc := e.ctx.GetStochastikVars().StmtCtx
	idxDefCausLen := len(e.index.Meta().DeferredCausets)
	for {
		err := result.Next(ctx, e.idxChunk)
		if err != nil {
			return err
		}
		if e.idxChunk.NumEvents() == 0 {
			return nil
		}
		iter := chunk.NewIterator4Chunk(e.idxChunk)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			handle, err := e.handleDefCauss.BuildHandle(event)
			if err != nil {
				return err
			}
			idxVals := extractIdxVals(event, e.idxValsBufs[e.scanEventCnt], e.idxDefCausFieldTypes, idxDefCausLen)
			e.idxValsBufs[e.scanEventCnt] = idxVals
			existingIdxVals, ok := e.idxValues.Get(handle)
			if ok {
				uFIDelatedIdxVals := append(existingIdxVals.([][]types.Causet), idxVals)
				e.idxValues.Set(handle, uFIDelatedIdxVals)
			} else {
				e.idxValues.Set(handle, [][]types.Causet{idxVals})
			}
			idxKey, _, err := e.index.GenIndexKey(sc, idxVals, handle, nil)
			if err != nil {
				return err
			}
			e.scanEventCnt++
			e.lastIdxKey = idxKey
			if e.scanEventCnt >= e.batchSize {
				return nil
			}
		}
	}
}

// Next implements the InterlockingDirectorate Next interface.
func (e *CleanupIndexInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	cleaningClusteredPrimaryKey := e.causet.Meta().IsCommonHandle && e.index.Meta().Primary
	if cleaningClusteredPrimaryKey {
		e.done = true
		req.AppendUint64(0, 0)
		return nil
	}

	var err error
	if tbl, ok := e.causet.(causet.PartitionedBlock); ok {
		pi := e.causet.Meta().GetPartitionInfo()
		for _, p := range pi.Definitions {
			e.causet = tbl.GetPartition(p.ID)
			e.index = blocks.GetWriblockIndexByName(e.index.Meta().Name.L, e.causet)
			e.physicalID = p.ID
			err = e.init()
			if err != nil {
				return err
			}
			err = e.cleanBlockIndex(ctx)
			if err != nil {
				return err
			}
		}
	} else {
		err = e.cleanBlockIndex(ctx)
		if err != nil {
			return err
		}
	}
	e.done = true
	req.AppendUint64(0, e.removeCnt)
	return nil
}

func (e *CleanupIndexInterDirc) cleanBlockIndex(ctx context.Context) error {
	for {
		errInTxn := ekv.RunInNewTxn(e.ctx.GetStore(), true, func(txn ekv.Transaction) error {
			err := e.fetchIndex(ctx, txn)
			if err != nil {
				return err
			}
			values, err := e.batchGetRecord(txn)
			if err != nil {
				return err
			}
			err = e.deleteDanglingIdx(txn, values)
			if err != nil {
				return err
			}
			return nil
		})
		if errInTxn != nil {
			return errInTxn
		}
		if e.scanEventCnt == 0 {
			break
		}
		e.scanEventCnt = 0
		e.batchKeys = e.batchKeys[:0]
		e.idxValues.Range(func(h ekv.Handle, val interface{}) bool {
			e.idxValues.Delete(h)
			return true
		})
	}
	return nil
}

func (e *CleanupIndexInterDirc) buildIndexScan(ctx context.Context, txn ekv.Transaction) (allegrosql.SelectResult, error) {
	posetPosetDagPB, err := e.buildIdxPosetDagPB(txn)
	if err != nil {
		return nil, err
	}
	sc := e.ctx.GetStochastikVars().StmtCtx
	var builder allegrosql.RequestBuilder
	ranges := ranger.FullRange()
	kvReq, err := builder.SetIndexRanges(sc, e.physicalID, e.index.Meta().ID, ranges).
		SetPosetDagRequest(posetPosetDagPB).
		SetStartTS(txn.StartTS()).
		SetKeepOrder(true).
		SetFromStochastikVars(e.ctx.GetStochastikVars()).
		Build()
	if err != nil {
		return nil, err
	}

	kvReq.KeyRanges[0].StartKey = ekv.Key(e.lastIdxKey).PrefixNext()
	kvReq.Concurrency = 1
	result, err := allegrosql.Select(ctx, e.ctx, kvReq, e.getIdxDefCausTypes(), statistics.NewQueryFeedback(0, nil, 0, false))
	if err != nil {
		return nil, err
	}
	result.Fetch(ctx)
	return result, nil
}

// Open implements the InterlockingDirectorate Open interface.
func (e *CleanupIndexInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	return e.init()
}

func (e *CleanupIndexInterDirc) init() error {
	e.idxChunk = chunk.New(e.getIdxDefCausTypes(), e.initCap, e.maxChunkSize)
	e.idxValues = ekv.NewHandleMap()
	e.batchKeys = make([]ekv.Key, 0, e.batchSize)
	e.idxValsBufs = make([][]types.Causet, e.batchSize)
	sc := e.ctx.GetStochastikVars().StmtCtx
	idxKey, _, err := e.index.GenIndexKey(sc, []types.Causet{{}}, ekv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return err
	}
	e.lastIdxKey = idxKey
	return nil
}

func (e *CleanupIndexInterDirc) buildIdxPosetDagPB(txn ekv.Transaction) (*fidelpb.PosetDagRequest, error) {
	posetPosetDagReq := &fidelpb.PosetDagRequest{}
	posetPosetDagReq.TimeZoneName, posetPosetDagReq.TimeZoneOffset = timeutil.Zone(e.ctx.GetStochastikVars().Location())
	sc := e.ctx.GetStochastikVars().StmtCtx
	posetPosetDagReq.Flags = sc.PushDownFlags()
	for i := range e.defCausumns {
		posetPosetDagReq.OutputOffsets = append(posetPosetDagReq.OutputOffsets, uint32(i))
	}

	execPB := e.constructIndexScanPB()
	posetPosetDagReq.InterlockingDirectorates = append(posetPosetDagReq.InterlockingDirectorates, execPB)
	err := causetcore.SetPBDeferredCausetsDefaultValue(e.ctx, posetPosetDagReq.InterlockingDirectorates[0].IdxScan.DeferredCausets, e.defCausumns)
	if err != nil {
		return nil, err
	}

	limitInterDirc := e.constructLimitPB()
	posetPosetDagReq.InterlockingDirectorates = append(posetPosetDagReq.InterlockingDirectorates, limitInterDirc)
	allegrosql.SetEncodeType(e.ctx, posetPosetDagReq)
	return posetPosetDagReq, nil
}

func (e *CleanupIndexInterDirc) constructIndexScanPB() *fidelpb.InterlockingDirectorate {
	idxInterDirc := &fidelpb.IndexScan{
		BlockId:          e.physicalID,
		IndexId:          e.index.Meta().ID,
		DeferredCausets:          soliton.DeferredCausetsToProto(e.defCausumns, e.causet.Meta().PKIsHandle),
		PrimaryDeferredCausetIds: blocks.TryGetCommonPkDeferredCausetIds(e.causet.Meta()),
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeIndexScan, IdxScan: idxInterDirc}
}

func (e *CleanupIndexInterDirc) constructLimitPB() *fidelpb.InterlockingDirectorate {
	limitInterDirc := &fidelpb.Limit{
		Limit: e.batchSize,
	}
	return &fidelpb.InterlockingDirectorate{Tp: fidelpb.InterDircType_TypeLimit, Limit: limitInterDirc}
}

// Close implements the InterlockingDirectorate Close interface.
func (e *CleanupIndexInterDirc) Close() error {
	return nil
}
