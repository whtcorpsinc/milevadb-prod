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

package stochastik

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/whtcorpsinc/errors"
	pb "github.com/whtcorpsinc/ekvproto/pkg/kvrpcpb"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"go.uber.org/zap"
)

const amendableType = nonMemAmendType | memBufAmendType
const nonMemAmendType = (1 << perceptron.CausetActionAddDeferredCauset) | (1 << perceptron.CausetActionDropDeferredCauset) | (1 << perceptron.CausetActionDropIndex)
const memBufAmendType = uint64(1<<perceptron.CausetActionAddIndex) | (1 << perceptron.CausetActionModifyDeferredCauset)

// Amend operation types.
const (
	AmendNone int = iota

	// For add index.
	AmendNeedAddDelete
	AmendNeedAddDeleteAndInsert
	AmendNeedAddInsert
)

// ConstOpAddIndex is the possible dbs state changes, and related amend action types.
var ConstOpAddIndex = map[perceptron.SchemaState]map[perceptron.SchemaState]int{
	perceptron.StateNone: {
		perceptron.StateDeleteOnly:          AmendNeedAddDelete,
		perceptron.StateWriteOnly:           AmendNeedAddDeleteAndInsert,
		perceptron.StateWriteReorganization: AmendNeedAddDeleteAndInsert,
		perceptron.StatePublic:              AmendNeedAddDeleteAndInsert,
	},
	perceptron.StateDeleteOnly: {
		perceptron.StateWriteOnly:           AmendNeedAddInsert,
		perceptron.StateWriteReorganization: AmendNeedAddInsert,
		perceptron.StatePublic:              AmendNeedAddInsert,
	},
	perceptron.StateWriteOnly: {
		perceptron.StateWriteReorganization: AmendNone,
		perceptron.StatePublic:              AmendNone,
	},
	perceptron.StateWriteReorganization: {
		perceptron.StatePublic: AmendNone,
	},
}

type schemaAndDecoder struct {
	schemaReplicant  *expression.Schema
	decoder *rowcodec.ChunkDecoder
}

// amendDefCauslector collects all amend operations.
type amendDefCauslector struct {
	tblAmendOpMap map[int64][]amendOp
}

func newAmendDefCauslector() *amendDefCauslector {
	res := &amendDefCauslector{
		tblAmendOpMap: make(map[int64][]amendOp),
	}
	return res
}

func findIndexByID(tbl block.Block, ID int64) block.Index {
	for _, indexInfo := range tbl.Indices() {
		if indexInfo.Meta().ID == ID {
			return indexInfo
		}
	}
	return nil
}

func findDefCausByID(tbl block.Block, colID int64) *block.DeferredCauset {
	for _, colInfo := range tbl.DefCauss() {
		if colInfo.ID == colID {
			return colInfo
		}
	}
	return nil
}

func addIndexNeedRemoveOp(amendOp int) bool {
	if amendOp == AmendNeedAddDelete || amendOp == AmendNeedAddDeleteAndInsert {
		return true
	}
	return false
}

func addIndexNeedAddOp(amendOp int) bool {
	if amendOp == AmendNeedAddDeleteAndInsert || amendOp == AmendNeedAddInsert {
		return true
	}
	return false
}

func (a *amendDefCauslector) keyHasAmendOp(key []byte) bool {
	tblID := blockcodec.DecodeTableID(key)
	ops := a.tblAmendOpMap[tblID]
	return len(ops) > 0
}

func needDefCauslectIndexOps(actionType uint64) bool {
	return actionType&(1<<perceptron.CausetActionAddIndex) != 0
}

func needDefCauslectModifyDefCausOps(actionType uint64) bool {
	return actionType&(1<<perceptron.CausetActionModifyDeferredCauset) != 0
}

func fieldTypeDeepEquals(ft1 *types.FieldType, ft2 *types.FieldType) bool {
	if ft1.Tp == ft2.Tp &&
		ft1.Flag == ft2.Flag &&
		ft1.Flen == ft2.Flen &&
		ft1.Decimal == ft2.Decimal &&
		ft1.Charset == ft2.Charset &&
		ft1.DefCauslate == ft2.DefCauslate &&
		len(ft1.Elems) == len(ft2.Elems) {
		for i, elem := range ft1.Elems {
			if elem != ft2.Elems[i] {
				return false
			}
		}
		return true
	}
	return false
}

// colChangeAmendable checks whether the column change is amendable, now only increasing column field
// length is allowed for committing concurrent pessimistic transactions.
func colChangeAmendable(colAtStart *perceptron.DeferredCausetInfo, colAtCommit *perceptron.DeferredCausetInfo) error {
	// Modifying a stored generated column is not allowed by DBS, the generated related fields are not considered.
	if !fieldTypeDeepEquals(&colAtStart.FieldType, &colAtCommit.FieldType) {
		if colAtStart.FieldType.Flag != colAtCommit.FieldType.Flag {
			return errors.Trace(errors.Errorf("flag is not matched for column=%v, from=%v to=%v",
				colAtCommit.Name.String(), colAtStart.FieldType.Flag, colAtCommit.FieldType.Flag))
		}
		if colAtStart.Charset != colAtCommit.Charset || colAtStart.DefCauslate != colAtCommit.DefCauslate {
			return errors.Trace(errors.Errorf("charset or collate is not matched for column=%v", colAtCommit.Name.String()))
		}
		_, err := dbs.CheckModifyTypeCompatible(&colAtStart.FieldType, &colAtCommit.FieldType)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// TODO Default value change is not supported.
	if !reflect.DeepEqual(colAtStart.DefaultValue, colAtCommit.DefaultValue) {
		return errors.Trace(errors.Errorf("default value is not matched for column=%v, from=%v to=%v",
			colAtCommit.Name.String(), colAtStart.DefaultValue, colAtCommit.DefaultValue))
	}
	if !bytes.Equal(colAtStart.DefaultValueBit, colAtCommit.DefaultValueBit) {
		return errors.Trace(errors.Errorf("default value bits is not matched for column=%v, from=%v to=%v",
			colAtCommit.Name.String(), colAtStart.DefaultValueBit, colAtCommit.DefaultValueBit))
	}
	if colAtStart.Version != colAtCommit.Version {
		return errors.Trace(errors.Errorf("column version is not matched for column=%v, from=%v to=%v",
			colAtCommit.Name.String(), colAtStart.Version, colAtCommit.Version))
	}
	return nil
}

// collectModifyDefCausAmendOps is used to check if there is column change from nullable to not null by now.
// TODO allow column change from nullable to not null, and generate keys check operation.
func (a *amendDefCauslector) collectModifyDefCausAmendOps(tblAtStart, tblAtCommit block.Block) ([]amendOp, error) {
	for _, colAtCommit := range tblAtCommit.DefCauss() {
		colAtStart := findDefCausByID(tblAtStart, colAtCommit.ID)
		if colAtStart != nil {
			err := colChangeAmendable(colAtStart.DeferredCausetInfo, colAtCommit.DeferredCausetInfo)
			if err != nil {
				return nil, err
			}
		}
	}
	return nil, nil
}

func (a *amendDefCauslector) collectIndexAmendOps(sctx stochastikctx.Context, tblAtStart, tblAtCommit block.Block) ([]amendOp, error) {
	res := make([]amendOp, 0, 4)
	// Check index having state change, collect index column info.
	for _, idxInfoAtCommit := range tblAtCommit.Indices() {
		idxInfoAtStart := findIndexByID(tblAtStart, idxInfoAtCommit.Meta().ID)
		// Try to find index state change.
		var amendOpType int
		if idxInfoAtStart == nil {
			amendOpType = ConstOpAddIndex[perceptron.StateNone][idxInfoAtCommit.Meta().State]
		} else if idxInfoAtCommit.Meta().State > idxInfoAtStart.Meta().State {
			amendOpType = ConstOpAddIndex[idxInfoAtStart.Meta().State][idxInfoAtCommit.Meta().State]
		}
		if amendOpType != AmendNone {
			// TODO unique index amend is not supported by now.
			if idxInfoAtCommit.Meta().Unique {
				return nil, errors.Trace(errors.Errorf("amend unique index=%v for block=%v is not supported now",
					idxInfoAtCommit.Meta().Name, tblAtCommit.Meta().Name))
			}
			opInfo := &amendOperationAddIndexInfo{}
			opInfo.AmendOpType = amendOpType
			opInfo.tblInfoAtStart = tblAtStart
			opInfo.tblInfoAtCommit = tblAtCommit
			opInfo.indexInfoAtStart = idxInfoAtStart
			opInfo.indexInfoAtCommit = idxInfoAtCommit
			for _, idxDefCaus := range idxInfoAtCommit.Meta().DeferredCausets {
				colID := tblAtCommit.Meta().DeferredCausets[idxDefCaus.Offset].ID
				oldDefCausInfo := findDefCausByID(tblAtStart, colID)
				// TODO: now index column MUST be found in old block columns, generated column is not supported.
				if oldDefCausInfo == nil || oldDefCausInfo.IsGenerated() || oldDefCausInfo.Hidden {
					return nil, errors.Trace(errors.Errorf("amend index column=%v id=%v is not found or generated in block=%v",
						idxDefCaus.Name, colID, tblAtCommit.Meta().Name.String()))
				}
				opInfo.relatedOldIdxDefCauss = append(opInfo.relatedOldIdxDefCauss, oldDefCausInfo)
			}
			opInfo.schemaAndDecoder = newSchemaAndDecoder(sctx, tblAtStart.Meta())
			fieldTypes := make([]*types.FieldType, 0, len(tblAtStart.Meta().DeferredCausets))
			for _, col := range tblAtStart.Meta().DeferredCausets {
				fieldTypes = append(fieldTypes, &col.FieldType)
			}
			opInfo.chk = chunk.NewChunkWithCapacity(fieldTypes, 4)
			if addIndexNeedRemoveOp(amendOpType) {
				removeIndexOp := &amendOperationDeleteOldIndex{
					info: opInfo,
				}
				res = append(res, removeIndexOp)
			}
			if addIndexNeedAddOp(amendOpType) {
				addNewIndexOp := &amendOperationAddNewIndex{
					info: opInfo,
				}
				res = append(res, addNewIndexOp)
			}
		}
	}
	return res, nil
}

// collectTblAmendOps collects amend operations for each block using the schemaReplicant diff between startTS and commitTS.
func (a *amendDefCauslector) collectTblAmendOps(sctx stochastikctx.Context, phyTblID int64,
	tblInfoAtStart, tblInfoAtCommit block.Block, actionType uint64) error {
	if _, ok := a.tblAmendOpMap[phyTblID]; !ok {
		a.tblAmendOpMap[phyTblID] = make([]amendOp, 0, 4)
	}
	if needDefCauslectModifyDefCausOps(actionType) {
		_, err := a.collectModifyDefCausAmendOps(tblInfoAtStart, tblInfoAtCommit)
		if err != nil {
			return err
		}
	}
	if needDefCauslectIndexOps(actionType) {
		// TODO: currently only "add index" is considered.
		ops, err := a.collectIndexAmendOps(sctx, tblInfoAtStart, tblInfoAtCommit)
		if err != nil {
			return err
		}
		a.tblAmendOpMap[phyTblID] = append(a.tblAmendOpMap[phyTblID], ops...)
	}
	return nil
}

func isDeleteOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Del || keyOp == pb.Op_Put
}

func isInsertOp(keyOp pb.Op) bool {
	return keyOp == pb.Op_Put || keyOp == pb.Op_Insert
}

// amendOp is an amend operation for a specific schemaReplicant change, new mutations will be generated using input ones.
type amendOp interface {
	genMutations(ctx context.Context, sctx stochastikctx.Context, commitMutations einsteindb.CommitterMutations, kvMap *rowKvMap,
		resultMutations *einsteindb.CommitterMutations) error
}

// amendOperationAddIndex represents one amend operation related to a specific add index change.
type amendOperationAddIndexInfo struct {
	AmendOpType       int
	tblInfoAtStart    block.Block
	tblInfoAtCommit   block.Block
	indexInfoAtStart  block.Index
	indexInfoAtCommit block.Index
	relatedOldIdxDefCauss []*block.DeferredCauset

	schemaAndDecoder *schemaAndDecoder
	chk              *chunk.Chunk
}

// amendOperationDeleteOldIndex represents the remove operation will be performed on old key values for add index amend.
type amendOperationDeleteOldIndex struct {
	info *amendOperationAddIndexInfo
}

// amendOperationAddNewIndex represents the add operation will be performed on new key values for add index amend.
type amendOperationAddNewIndex struct {
	info *amendOperationAddIndexInfo
}

func (a *amendOperationAddIndexInfo) String() string {
	var colStr string
	colStr += "["
	for _, colInfo := range a.relatedOldIdxDefCauss {
		colStr += fmt.Sprintf(" %s ", colInfo.Name)
	}
	colStr += "]"
	res := fmt.Sprintf("AmenedOpType=%d phyTblID=%d idxID=%d columns=%v", a.AmendOpType, a.indexInfoAtCommit.Meta().ID,
		a.indexInfoAtCommit.Meta().ID, colStr)
	return res
}

func (a *amendOperationDeleteOldIndex) genMutations(ctx context.Context, sctx stochastikctx.Context,
	commitMutations einsteindb.CommitterMutations, kvMap *rowKvMap, resAddMutations *einsteindb.CommitterMutations) error {
	for i, key := range commitMutations.GetKeys() {
		keyOp := commitMutations.GetOps()[i]
		if blockcodec.IsIndexKey(key) || blockcodec.DecodeTableID(key) != a.info.tblInfoAtCommit.Meta().ID {
			continue
		}
		if !isDeleteOp(keyOp) {
			continue
		}
		err := a.processRowKey(ctx, sctx, key, kvMap.oldRowKvMap, resAddMutations)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *amendOperationAddNewIndex) genMutations(ctx context.Context, sctx stochastikctx.Context, commitMutations einsteindb.CommitterMutations,
	kvMap *rowKvMap, resAddMutations *einsteindb.CommitterMutations) error {
	for i, key := range commitMutations.GetKeys() {
		keyOp := commitMutations.GetOps()[i]
		if blockcodec.IsIndexKey(key) || blockcodec.DecodeTableID(key) != a.info.tblInfoAtCommit.Meta().ID {
			continue
		}
		if !isInsertOp(keyOp) {
			continue
		}
		err := a.processRowKey(ctx, sctx, key, kvMap.newRowKvMap, resAddMutations)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *amendOperationAddIndexInfo) genIndexKeyValue(ctx context.Context, sctx stochastikctx.Context, kvMap map[string][]byte,
	key []byte, kvHandle ekv.Handle, keyOnly bool) ([]byte, []byte, error) {
	chk := a.chk
	chk.Reset()
	val, ok := kvMap[string(key)]
	if !ok {
		// The Op_Put may not exist in old value ekv map.
		if keyOnly {
			return nil, nil, nil
		}
		return nil, nil, errors.Errorf("key=%v is not found in new event ekv map", ekv.Key(key).String())
	}
	err := executor.DecodeRowValToChunk(sctx, a.schemaAndDecoder.schemaReplicant, a.tblInfoAtStart.Meta(), kvHandle, val, chk, a.schemaAndDecoder.decoder)
	if err != nil {
		logutil.Logger(ctx).Warn("amend decode value to chunk failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	idxVals := make([]types.Causet, 0, len(a.indexInfoAtCommit.Meta().DeferredCausets))
	for _, oldDefCaus := range a.relatedOldIdxDefCauss {
		idxVals = append(idxVals, chk.GetRow(0).GetCauset(oldDefCaus.Offset, &oldDefCaus.FieldType))
	}

	// Generate index key buf.
	newIdxKey, distinct, err := blockcodec.GenIndexKey(sctx.GetStochastikVars().StmtCtx,
		a.tblInfoAtCommit.Meta(), a.indexInfoAtCommit.Meta(), a.tblInfoAtCommit.Meta().ID, idxVals, kvHandle, nil)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index key failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	if keyOnly {
		return newIdxKey, []byte{}, nil
	}

	// Generate index value buf.
	containsNonBinaryString := blocks.ContainsNonBinaryString(a.indexInfoAtCommit.Meta().DeferredCausets, a.tblInfoAtCommit.Meta().DeferredCausets)
	newIdxVal, err := blockcodec.GenIndexValue(sctx.GetStochastikVars().StmtCtx, a.tblInfoAtCommit.Meta(),
		a.indexInfoAtCommit.Meta(), containsNonBinaryString, distinct, false, idxVals, kvHandle)
	if err != nil {
		logutil.Logger(ctx).Warn("amend generate index values failed", zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	return newIdxKey, newIdxVal, nil
}

func (a *amendOperationAddNewIndex) processRowKey(ctx context.Context, sctx stochastikctx.Context, key []byte,
	kvMap map[string][]byte, resAddMutations *einsteindb.CommitterMutations) error {
	kvHandle, err := blockcodec.DecodeRowKey(key)
	if err != nil {
		logutil.Logger(ctx).Error("decode key error", zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return errors.Trace(err)
	}

	newIdxKey, newIdxValue, err := a.info.genIndexKeyValue(ctx, sctx, kvMap, key, kvHandle, false)
	if err != nil {
		return errors.Trace(err)
	}
	resAddMutations.Push(pb.Op_Put, newIdxKey, newIdxValue, false)
	return nil
}

func (a *amendOperationDeleteOldIndex) processRowKey(ctx context.Context, sctx stochastikctx.Context, key []byte,
	oldValKvMap map[string][]byte, resAddMutations *einsteindb.CommitterMutations) error {
	kvHandle, err := blockcodec.DecodeRowKey(key)
	if err != nil {
		logutil.Logger(ctx).Error("decode key error", zap.String("key", hex.EncodeToString(key)), zap.Error(err))
		return errors.Trace(err)
	}
	// Generated delete index key value.
	newIdxKey, emptyVal, err := a.info.genIndexKeyValue(ctx, sctx, oldValKvMap, key, kvHandle, true)
	if err != nil {
		return errors.Trace(err)
	}
	// For Op_Put the key may not exist in old key value map.
	if len(newIdxKey) > 0 {
		resAddMutations.Push(pb.Op_Del, newIdxKey, emptyVal, false)
	}
	return nil
}

// SchemaAmender is used to amend pessimistic transactions for schemaReplicant change.
type SchemaAmender struct {
	sess *stochastik
}

// NewSchemaAmenderForEinsteinDBTxn creates a schemaReplicant amender for einsteindbTxn type.
func NewSchemaAmenderForEinsteinDBTxn(sess *stochastik) *SchemaAmender {
	amender := &SchemaAmender{sess: sess}
	return amender
}

func (s *SchemaAmender) getAmendableKeys(commitMutations einsteindb.CommitterMutations, info *amendDefCauslector) ([]ekv.Key, []ekv.Key) {
	addKeys := make([]ekv.Key, 0, len(commitMutations.GetKeys()))
	removeKeys := make([]ekv.Key, 0, len(commitMutations.GetKeys()))
	for i, byteKey := range commitMutations.GetKeys() {
		if blockcodec.IsIndexKey(byteKey) || !info.keyHasAmendOp(byteKey) {
			continue
		}
		keyOp := commitMutations.GetOps()[i]
		if pb.Op_Put == keyOp {
			addKeys = append(addKeys, byteKey)
			removeKeys = append(removeKeys, byteKey)
		} else if pb.Op_Insert == keyOp {
			addKeys = append(addKeys, byteKey)
		} else if pb.Op_Del == keyOp {
			removeKeys = append(removeKeys, byteKey)
		} // else Do nothing.
	}
	return addKeys, removeKeys
}

type rowKvMap struct {
	oldRowKvMap map[string][]byte
	newRowKvMap map[string][]byte
}

func (s *SchemaAmender) prepareKvMap(ctx context.Context, commitMutations einsteindb.CommitterMutations, info *amendDefCauslector) (*rowKvMap, error) {
	// Get keys need to be considered for the amend operation, currently only event keys.
	addKeys, removeKeys := s.getAmendableKeys(commitMutations, info)

	// BatchGet the new key values, the Op_Put and Op_Insert type keys in memory buffer.
	txn, err := s.sess.Txn(true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newValKvMap, err := txn.BatchGet(ctx, addKeys)
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to batch get ekv new keys", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if len(newValKvMap) != len(addKeys) {
		logutil.Logger(ctx).Error("amend failed to batch get results invalid",
			zap.Int("addKeys len", len(addKeys)), zap.Int("newValKvMap", len(newValKvMap)))
		return nil, errors.Errorf("add keys has %v values but result kvMap has %v", len(addKeys), len(newValKvMap))
	}
	// BatchGet the old key values, the Op_Del and Op_Put types keys in storage using forUFIDelateTS, the Op_put type is for
	// event uFIDelate using the same event key, it may not exist.
	snapshot, err := s.sess.GetStore().GetSnapshot(ekv.Version{Ver: s.sess.stochastikVars.TxnCtx.GetForUFIDelateTS()})
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to get snapshot using forUFIDelateTS", zap.Error(err))
		return nil, errors.Trace(err)
	}
	oldValKvMap, err := snapshot.BatchGet(ctx, removeKeys)
	if err != nil {
		logutil.Logger(ctx).Warn("amend failed to batch get ekv old keys", zap.Error(err))
		return nil, errors.Trace(err)
	}

	res := &rowKvMap{
		oldRowKvMap: oldValKvMap,
		newRowKvMap: newValKvMap,
	}
	return res, nil
}

// genAllAmendMutations generates CommitterMutations for all blocks and related amend operations.
func (s *SchemaAmender) genAllAmendMutations(ctx context.Context, commitMutations einsteindb.CommitterMutations,
	info *amendDefCauslector) (*einsteindb.CommitterMutations, error) {
	rowKvMap, err := s.prepareKvMap(ctx, commitMutations, info)
	if err != nil {
		return nil, err
	}
	// Do generate add/remove mutations processing each key.
	resultNewMutations := einsteindb.NewCommiterMutations(32)
	for _, amendOps := range info.tblAmendOpMap {
		for _, curOp := range amendOps {
			err := curOp.genMutations(ctx, s.sess, commitMutations, rowKvMap, &resultNewMutations)
			if err != nil {
				return nil, err
			}
		}
	}
	return &resultNewMutations, nil
}

// AmendTxn does check and generate amend mutations based on input schemaReplicant and mutations, mutations need to prewrite
// are returned, the input commitMutations will not be changed.
func (s *SchemaAmender) AmendTxn(ctx context.Context, startSchemaReplicant einsteindb.SchemaVer, change *einsteindb.RelatedSchemaChange,
	commitMutations einsteindb.CommitterMutations) (*einsteindb.CommitterMutations, error) {
	// Get info schemaReplicant meta
	schemaReplicantAtStart := startSchemaReplicant.(schemareplicant.SchemaReplicant)
	schemaReplicantAtCheck := change.LatestSchemaReplicant.(schemareplicant.SchemaReplicant)

	// DefCauslect amend operations for each block by physical block ID.
	var needAmendMem bool
	amendDefCauslector := newAmendDefCauslector()
	for i, tblID := range change.PhyTblIDS {
		actionType := change.CausetActionTypes[i]
		// Check amendable flags, return if not supported flags exist.
		if actionType&(^amendableType) != 0 {
			logutil.Logger(ctx).Info("amend action type not supported for txn", zap.Int64("tblID", tblID), zap.Uint64("actionType", actionType))
			return nil, errors.Trace(block.ErrUnsupportedOp)
		}
		// Partition block is not supported now.
		tblInfoAtStart, ok := schemaReplicantAtStart.TableByID(tblID)
		if !ok {
			return nil, errors.Trace(errors.Errorf("blockID=%d is not found in schemaReplicant", tblID))
		}
		if tblInfoAtStart.Meta().Partition != nil {
			logutil.Logger(ctx).Info("Amend for partition block is not supported",
				zap.String("blockName", tblInfoAtStart.Meta().Name.String()), zap.Int64("blockID", tblID))
			return nil, errors.Trace(block.ErrUnsupportedOp)
		}
		tblInfoAtCommit, ok := schemaReplicantAtCheck.TableByID(tblID)
		if !ok {
			return nil, errors.Trace(errors.Errorf("blockID=%d is not found in schemaReplicant", tblID))
		}
		if actionType&(memBufAmendType) != 0 {
			needAmendMem = true
			err := amendDefCauslector.collectTblAmendOps(s.sess, tblID, tblInfoAtStart, tblInfoAtCommit, actionType)
			if err != nil {
				return nil, err
			}
		}
	}
	// After amend operations collect, generate related new mutations based on input commitMutations
	if needAmendMem {
		return s.genAllAmendMutations(ctx, commitMutations, amendDefCauslector)
	}
	return nil, nil
}

func newSchemaAndDecoder(ctx stochastikctx.Context, tbl *perceptron.TableInfo) *schemaAndDecoder {
	schemaReplicant := expression.NewSchema(make([]*expression.DeferredCauset, 0, len(tbl.DeferredCausets))...)
	for _, col := range tbl.DeferredCausets {
		colExpr := &expression.DeferredCauset{
			RetType: &col.FieldType,
			ID:      col.ID,
		}
		if col.IsGenerated() && !col.GeneratedStored {
			// This will not be used since generated column is rejected in collectIndexAmendOps.
			colExpr.VirtualExpr = &expression.Constant{}
		}
		schemaReplicant.Append(colExpr)
	}
	return &schemaAndDecoder{schemaReplicant, executor.NewRowDecoder(ctx, schemaReplicant, tbl)}
}
