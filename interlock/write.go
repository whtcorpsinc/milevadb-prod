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
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var (
	_ InterlockingDirectorate = &UFIDelateInterDirc{}
	_ InterlockingDirectorate = &DeleteInterDirc{}
	_ InterlockingDirectorate = &InsertInterDirc{}
	_ InterlockingDirectorate = &ReplaceInterDirc{}
	_ InterlockingDirectorate = &LoadDataInterDirc{}
)

// uFIDelateRecord uFIDelates the event specified by the handle `h`, from `oldData` to `newData`.
// `modified` means which defCausumns are really modified. It's used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WriblockDefCauss()`.
// The return values:
//     1. changed (bool) : does the uFIDelate really change the event values. e.g. uFIDelate set i = 1 where i = 1;
//     2. err (error) : error in the uFIDelate.
func uFIDelateRecord(ctx context.Context, sctx stochastikctx.Context, h ekv.Handle, oldData, newData []types.Causet, modified []bool, t causet.Block,
	onDup bool, memTracker *memory.Tracker) (bool, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("interlock.uFIDelateRecord", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	txn, err := sctx.Txn(false)
	if err != nil {
		return false, err
	}
	memUsageOfTxnState := txn.Size()
	defer memTracker.Consume(int64(txn.Size() - memUsageOfTxnState))
	sc := sctx.GetStochastikVars().StmtCtx
	changed, handleChanged := false, false
	// onUFIDelateSpecified is for "UFIDelATE SET ts_field = old_value", the
	// timestamp field is explicitly set, but not changed in fact.
	onUFIDelateSpecified := make(map[int]bool)
	var newHandle ekv.Handle

	// We can iterate on public defCausumns not wriblock defCausumns,
	// because all of them are sorted by their `Offset`, which
	// causes all wriblock defCausumns are after public defCausumns.

	// 1. Cast modified values.
	for i, defCaus := range t.DefCauss() {
		if modified[i] {
			// Cast changed fields with respective defCausumns.
			v, err := causet.CastValue(sctx, newData[i], defCaus.ToInfo(), false, false)
			if err != nil {
				return false, err
			}
			newData[i] = v
		}
	}

	// 2. Handle the bad null error.
	for i, defCaus := range t.DefCauss() {
		var err error
		if err = defCaus.HandleBadNull(&newData[i], sc); err != nil {
			return false, err
		}
	}

	// 3. Compare causet, then handle some flags.
	for i, defCaus := range t.DefCauss() {
		cmp, err := newData[i].CompareCauset(sc, &oldData[i])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			changed = true
			modified[i] = true
			// Rebase auto increment id if the field is changed.
			if allegrosql.HasAutoIncrementFlag(defCaus.Flag) {
				recordID, err := getAutoRecordID(newData[i], &defCaus.FieldType, false)
				if err != nil {
					return false, err
				}
				if err = t.RebaseAutoID(sctx, recordID, true, autoid.EventIDAllocType); err != nil {
					return false, err
				}
			}
			if defCaus.IsPKHandleDeferredCauset(t.Meta()) {
				handleChanged = true
				newHandle = ekv.IntHandle(newData[i].GetInt64())
				// Rebase auto random id if the field is changed.
				if err := rebaseAutoRandomValue(sctx, t, &newData[i], defCaus); err != nil {
					return false, err
				}
			}
			if defCaus.IsCommonHandleDeferredCauset(t.Meta()) {
				pkIdx := blocks.FindPrimaryIndex(t.Meta())
				handleChanged = true
				pkDts := make([]types.Causet, 0, len(pkIdx.DeferredCausets))
				for _, idxDefCaus := range pkIdx.DeferredCausets {
					pkDts = append(pkDts, newData[idxDefCaus.Offset])
				}
				blockcodec.TruncateIndexValues(t.Meta(), pkIdx, pkDts)
				handleBytes, err := codec.EncodeKey(sctx.GetStochastikVars().StmtCtx, nil, pkDts...)
				if err != nil {
					return false, err
				}
				newHandle, err = ekv.NewCommonHandle(handleBytes)
				if err != nil {
					return false, err
				}
			}
		} else {
			if allegrosql.HasOnUFIDelateNowFlag(defCaus.Flag) && modified[i] {
				// It's for "UFIDelATE t SET ts = ts" and ts is a timestamp.
				onUFIDelateSpecified[i] = true
			}
			modified[i] = false
		}
	}

	sc.AddTouchedEvents(1)
	// If no changes, nothing to do, return directly.
	if !changed {
		// See https://dev.allegrosql.com/doc/refman/5.7/en/allegrosql-real-connect.html  CLIENT_FOUND_ROWS
		if sctx.GetStochastikVars().ClientCapability&allegrosql.ClientFoundEvents > 0 {
			sc.AddAffectedEvents(1)
		}

		physicalID := t.Meta().ID
		if pt, ok := t.(causet.PartitionedBlock); ok {
			p, err := pt.GetPartitionByEvent(sctx, oldData)
			if err != nil {
				return false, err
			}
			physicalID = p.GetPhysicalID()
		}

		unchangedEventKey := blockcodec.EncodeEventKeyWithHandle(physicalID, h)
		txnCtx := sctx.GetStochastikVars().TxnCtx
		if txnCtx.IsPessimistic {
			txnCtx.AddUnchangedEventKey(unchangedEventKey)
		}
		return false, nil
	}

	// 4. Fill values into on-uFIDelate-now fields, only if they are really changed.
	for i, defCaus := range t.DefCauss() {
		if allegrosql.HasOnUFIDelateNowFlag(defCaus.Flag) && !modified[i] && !onUFIDelateSpecified[i] {
			if v, err := memex.GetTimeValue(sctx, strings.ToUpper(ast.CurrentTimestamp), defCaus.Tp, int8(defCaus.Decimal)); err == nil {
				newData[i] = v
				modified[i] = true
			} else {
				return false, err
			}
		}
	}

	// 5. If handle changed, remove the old then add the new record, otherwise uFIDelate the record.
	if handleChanged {
		if sc.DupKeyAsWarning {
			// For `UFIDelATE IGNORE`/`INSERT IGNORE ON DUPLICATE KEY UFIDelATE`
			// If the new handle exists, this will avoid to remove the record.
			err = blocks.CheckHandleExists(ctx, sctx, t, newHandle, newData)
			if err != nil {
				return false, err
			}
		}
		if err = t.RemoveRecord(sctx, h, oldData); err != nil {
			return false, err
		}
		// the `affectedEvents` is increased when adding new record.
		if sc.DupKeyAsWarning {
			_, err = t.AddRecord(sctx, newData, causet.IsUFIDelate, causet.SkipHandleCheck, causet.WithCtx(ctx))
		} else {
			_, err = t.AddRecord(sctx, newData, causet.IsUFIDelate, causet.WithCtx(ctx))
		}

		if err != nil {
			return false, err
		}
		if onDup {
			sc.AddAffectedEvents(1)
		}
	} else {
		// UFIDelate record to new value and uFIDelate index.
		if err = t.UFIDelateRecord(ctx, sctx, h, oldData, newData, modified); err != nil {
			return false, err
		}
		if onDup {
			sc.AddAffectedEvents(2)
		} else {
			sc.AddAffectedEvents(1)
		}
	}
	sc.AddUFIDelatedEvents(1)
	sc.AddCopiedEvents(1)

	return true, nil
}

func rebaseAutoRandomValue(sctx stochastikctx.Context, t causet.Block, newData *types.Causet, defCaus *causet.DeferredCauset) error {
	blockInfo := t.Meta()
	if !blockInfo.ContainsAutoRandomBits() {
		return nil
	}
	recordID, err := getAutoRecordID(*newData, &defCaus.FieldType, false)
	if err != nil {
		return err
	}
	if recordID < 0 {
		return nil
	}
	layout := autoid.NewAutoRandomIDLayout(&defCaus.FieldType, blockInfo.AutoRandomBits)
	// Set bits except incremental_bits to zero.
	recordID = recordID & (1<<layout.IncrementalBits - 1)
	return t.SlabPredictors(sctx).Get(autoid.AutoRandomType).Rebase(blockInfo.ID, recordID, true)
}

// resetErrDataTooLong reset ErrDataTooLong error msg.
// types.ErrDataTooLong is produced in types.ProduceStrWithSpecifiedTp, there is no defCausumn info in there,
// so we reset the error msg here, and wrap old err with errors.Wrap.
func resetErrDataTooLong(defCausName string, rowIdx int, err error) error {
	newErr := types.ErrDataTooLong.GenWithStack("Data too long for defCausumn '%v' at event %v", defCausName, rowIdx)
	return newErr
}
