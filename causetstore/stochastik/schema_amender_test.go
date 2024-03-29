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
	"sort"
	"strconv"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = SerialSuites(&testSchemaAmenderSuite{})

type testSchemaAmenderSuite struct {
}

func (s *testSchemaAmenderSuite) SetUpSuite(c *C) {
}

func (s *testSchemaAmenderSuite) TearDownSuite(c *C) {
}

func initTblDefCausIdxID(spacetimeInfo *perceptron.TableInfo) {
	for i, col := range spacetimeInfo.DeferredCausets {
		col.ID = int64(i + 1)
	}
	for i, idx := range spacetimeInfo.Indices {
		idx.ID = int64(i + 1)
		// TODO unique index is not supported now.
		idx.Unique = false
	}
	spacetimeInfo.ID = 1
	spacetimeInfo.State = perceptron.StatePublic
}

func mutationsEqual(res *einsteindb.CommitterMutations, expected *einsteindb.CommitterMutations, c *C) {
	c.Assert(len(res.GetKeys()), Equals, len(expected.GetKeys()))
	for i := 0; i < len(res.GetKeys()); i++ {
		foundIdx := -1
		for j := 0; j < len(expected.GetKeys()); j++ {
			if bytes.Equal(res.GetKeys()[i], expected.GetKeys()[j]) {
				foundIdx = j
				break
			}
		}
		c.Assert(foundIdx, GreaterEqual, 0)
		c.Assert(res.GetOps()[i], Equals, expected.GetOps()[foundIdx])
		c.Assert(res.GetPessimisticFlags()[i], Equals, expected.GetPessimisticFlags()[foundIdx])
		c.Assert(res.GetKeys()[i], BytesEquals, expected.GetKeys()[foundIdx])
		c.Assert(res.GetValues()[i], BytesEquals, expected.GetValues()[foundIdx])
	}
}

type data struct {
	ops      []ekvrpcpb.Op
	keys     [][]byte
	values   [][]byte
	rowValue [][]types.Causet
}

func prepareTestData(se *stochastik, mutations *einsteindb.CommitterMutations, oldTblInfo causet.Block, newTblInfo causet.Block,
	expecetedAmendOps []amendOp, c *C) (*data, *data, einsteindb.CommitterMutations) {
	var err error
	// Generated test data.
	colIds := make([]int64, len(oldTblInfo.Meta().DeferredCausets))
	basicRowValue := make([]types.Causet, len(oldTblInfo.Meta().DeferredCausets))
	for i, col := range oldTblInfo.Meta().DeferredCausets {
		colIds[i] = oldTblInfo.Meta().DeferredCausets[col.Offset].ID
		if col.FieldType.Tp == allegrosql.TypeLong {
			basicRowValue[i] = types.NewIntCauset(int64(col.Offset))
		} else {
			basicRowValue[i] = types.NewStringCauset(strconv.Itoa(col.Offset))
		}
	}
	KeyOps := []ekvrpcpb.Op{ekvrpcpb.Op_Put, ekvrpcpb.Op_Del, ekvrpcpb.Op_Lock, ekvrpcpb.Op_Insert, ekvrpcpb.Op_Put,
		ekvrpcpb.Op_Del, ekvrpcpb.Op_Insert, ekvrpcpb.Op_Lock}
	oldRowValues := make([][]types.Causet, len(KeyOps))
	newRowValues := make([][]types.Causet, len(KeyOps))
	rd := rowcodec.CausetEncoder{Enable: true}
	newData := &data{}
	oldData := &data{}
	expecteMutations := einsteindb.NewCommiterMutations(8)

	// Generate old data.
	for i := 0; i < len(KeyOps); i++ {
		keyOp := KeyOps[i]
		thisRowValue := make([]types.Causet, len(basicRowValue))
		copy(thisRowValue, basicRowValue)
		thisRowValue[0] = types.NewIntCauset(int64(i + 1))
		thisRowValue[4] = types.NewIntCauset(int64(i + 1 + 4))

		// Save old data.
		rowKey := blockcodec.EncodeRowKeyWithHandle(oldTblInfo.Meta().ID, ekv.IntHandle(i+1))
		var rowValue []byte
		rowValue, err = rd.Encode(se.stochastikVars.StmtCtx, colIds, thisRowValue, nil)
		c.Assert(err, IsNil)
		if keyOp == ekvrpcpb.Op_Del || keyOp == ekvrpcpb.Op_Put {
			// Skip the last Op_put, it has no old event value.
			if i == 4 {
				continue
			}
			oldData.keys = append(oldData.keys, rowKey)
			oldData.values = append(oldData.values, rowValue)
			oldData.ops = append(oldData.ops, keyOp)
			oldData.rowValue = append(oldData.rowValue, thisRowValue)
			if keyOp == ekvrpcpb.Op_Del {
				mutations.Push(keyOp, rowKey, nil, true)
			}
		}
		oldRowValues[i] = thisRowValue
	}

	// Generate new data.
	for i := 0; i < len(KeyOps); i++ {
		keyOp := KeyOps[i]
		thisRowValue := make([]types.Causet, len(basicRowValue))
		copy(thisRowValue, basicRowValue)
		thisRowValue[0] = types.NewIntCauset(int64(i + 1))
		// New column e value should be different from old event values.
		thisRowValue[4] = types.NewIntCauset(int64(i+1+4) * 20)

		var rowValue []byte
		// Save new data.
		rowKey := blockcodec.EncodeRowKeyWithHandle(oldTblInfo.Meta().ID, ekv.IntHandle(i+1))
		if keyOp == ekvrpcpb.Op_Insert {
			rowValue, err = blockcodec.EncodeOldRow(se.stochastikVars.StmtCtx, thisRowValue, colIds, nil, nil)
		} else {
			rowValue, err = rd.Encode(se.stochastikVars.StmtCtx, colIds, thisRowValue, nil)
		}
		if keyOp == ekvrpcpb.Op_Put || keyOp == ekvrpcpb.Op_Insert {
			newData.keys = append(newData.keys, rowKey)
			newData.values = append(newData.values, rowValue)
			newData.ops = append(newData.ops, keyOp)
			newData.rowValue = append(newData.rowValue, thisRowValue)
			mutations.Push(keyOp, rowKey, rowValue, true)
		} else if keyOp == ekvrpcpb.Op_Lock {
			mutations.Push(keyOp, rowKey, []byte{}, true)
		}
		newRowValues[i] = thisRowValue
	}

	// Prepare expected results.
	for _, op := range expecetedAmendOps {
		var oldOp *amendOperationDeleteOldIndex
		var newOp *amendOperationAddNewIndex
		var info *amendOperationAddIndexInfo
		var ok bool
		oldOp, ok = op.(*amendOperationDeleteOldIndex)
		if ok {
			info = oldOp.info
		} else {
			newOp = op.(*amendOperationAddNewIndex)
			info = newOp.info
		}
		var idxVal []byte
		genIndexKV := func(inputRow []types.Causet) ([]byte, []byte) {
			indexCausets := make([]types.Causet, len(info.relatedOldIdxDefCauss))
			for colIdx, col := range info.relatedOldIdxDefCauss {
				indexCausets[colIdx] = inputRow[col.Offset]
			}
			ekvHandle := ekv.IntHandle(inputRow[0].GetInt64())
			idxKey, _, err := blockcodec.GenIndexKey(se.stochastikVars.StmtCtx, newTblInfo.Meta(),
				info.indexInfoAtCommit.Meta(), newTblInfo.Meta().ID, indexCausets, ekvHandle, nil)
			c.Assert(err, IsNil)
			idxVal, err = blockcodec.GenIndexValue(se.stochastikVars.StmtCtx, newTblInfo.Meta(), info.indexInfoAtCommit.Meta(),
				false, info.indexInfoAtCommit.Meta().Unique, false, indexCausets, ekvHandle)
			c.Assert(err, IsNil)
			return idxKey, idxVal
		}
		_, ok = op.(*amendOperationDeleteOldIndex)
		if ok {
			c.Assert(addIndexNeedRemoveOp(info.AmendOpType), IsTrue)
			for i := range oldData.keys {
				if addIndexNeedRemoveOp(info.AmendOpType) && isDeleteOp(oldData.ops[i]) {
					thisRowValue := oldData.rowValue[i]
					idxKey, _ := genIndexKV(thisRowValue)
					expecteMutations.Push(ekvrpcpb.Op_Del, idxKey, []byte{}, false)
				}
			}
		}
		_, ok = op.(*amendOperationAddNewIndex)
		if ok {
			c.Assert(addIndexNeedAddOp(info.AmendOpType), IsTrue)
			for i := range newData.keys {
				if addIndexNeedAddOp(info.AmendOpType) && isInsertOp(newData.ops[i]) {
					thisRowValue := newData.rowValue[i]
					idxKey, idxVal := genIndexKV(thisRowValue)
					c.Assert(err, IsNil)
					mutOp := ekvrpcpb.Op_Put
					if info.indexInfoAtCommit.Meta().Unique {
						mutOp = ekvrpcpb.Op_Insert
					}
					expecteMutations.Push(mutOp, idxKey, idxVal, false)
				}
			}
		}
	}
	return newData, oldData, expecteMutations
}

func (s *testSchemaAmenderSuite) TestAmendDefCauslectAndGenMutations(c *C) {
	ctx := context.Background()
	causetstore := newStore(c, "test_schema_amender")
	defer causetstore.Close()
	se := &stochastik{
		causetstore:    causetstore,
		BerolinaSQL:    BerolinaSQL.New(),
		stochastikVars: variable.NewStochastikVars(),
	}
	startStates := []perceptron.SchemaState{perceptron.StateNone, perceptron.StateDeleteOnly}
	for _, startState := range startStates {
		endStatMap := ConstOpAddIndex[startState]
		var endStates []perceptron.SchemaState
		for st := range endStatMap {
			endStates = append(endStates, st)
		}
		sort.Slice(endStates, func(i, j int) bool { return endStates[i] < endStates[j] })
		for _, endState := range endStates {
			// column: a, b, c, d, e, c_str, d_str, e_str, f, g.
			// PK: a.
			// indices: c_d_e, e, f, g, f_g, c_d_e_str, c_d_e_str_prefix.
			oldTblMeta := embedded.MockSignedTable()
			initTblDefCausIdxID(oldTblMeta)
			// Indices[0] does not exist at the start.
			oldTblMeta.Indices = oldTblMeta.Indices[1:]
			oldTbInfo, err := causet.TableFromMeta(nil, oldTblMeta)
			c.Assert(err, IsNil)
			oldTblMeta.Indices[0].State = startState
			oldTblMeta.Indices[2].State = endState

			newTblMeta := embedded.MockSignedTable()
			initTblDefCausIdxID(newTblMeta)
			// colh is newly added.
			colh := &perceptron.DeferredCausetInfo{
				State:     perceptron.StatePublic,
				Offset:    12,
				Name:      perceptron.NewCIStr("b"),
				FieldType: *(types.NewFieldType(allegrosql.TypeLong)),
				ID:        13,
			}
			newTblMeta.DeferredCausets = append(newTblMeta.DeferredCausets, colh)
			// The last index "c_d_e_str_prefix is dropped.
			newTblMeta.Indices = newTblMeta.Indices[:len(newTblMeta.Indices)-1]
			newTblMeta.Indices[0].Unique = false
			newTblInfo, err := causet.TableFromMeta(nil, newTblMeta)
			c.Assert(err, IsNil)
			newTblMeta.Indices[0].State = endState
			// Indices[1] is newly created.
			newTblMeta.Indices[1].State = endState
			// Indices[3] is dropped
			newTblMeta.Indices[3].State = startState

			// Only the add index amend operations is collected in the results.
			collector := newAmendDefCauslector()
			tblID := int64(1)
			err = collector.collectTblAmendOps(se, tblID, oldTbInfo, newTblInfo, 1<<perceptron.CausetActionAddIndex)
			c.Assert(err, IsNil)
			c.Assert(len(collector.tblAmendOpMap[tblID]), GreaterEqual, 2)
			var expectedAmendOps []amendOp

			// For index 0.
			addIndexOpInfo := &amendOperationAddIndexInfo{
				AmendOpType:           ConstOpAddIndex[perceptron.StateNone][endState],
				tblInfoAtStart:        oldTbInfo,
				tblInfoAtCommit:       newTblInfo,
				indexInfoAtStart:      nil,
				indexInfoAtCommit:     newTblInfo.Indices()[0],
				relatedOldIdxDefCauss: []*causet.DeferredCauset{oldTbInfo.DefCauss()[2], oldTbInfo.DefCauss()[3], oldTbInfo.DefCauss()[4]},
			}
			if addIndexNeedRemoveOp(addIndexOpInfo.AmendOpType) {
				expectedAmendOps = append(expectedAmendOps, &amendOperationDeleteOldIndex{
					info: addIndexOpInfo,
				})
			}
			if addIndexNeedAddOp(addIndexOpInfo.AmendOpType) {
				expectedAmendOps = append(expectedAmendOps, &amendOperationAddNewIndex{
					info: addIndexOpInfo,
				})
			}

			// For index 1.
			addIndexOpInfo1 := &amendOperationAddIndexInfo{
				AmendOpType:           ConstOpAddIndex[startState][endState],
				tblInfoAtStart:        oldTbInfo,
				tblInfoAtCommit:       newTblInfo,
				indexInfoAtStart:      oldTbInfo.Indices()[0],
				indexInfoAtCommit:     newTblInfo.Indices()[1],
				relatedOldIdxDefCauss: []*causet.DeferredCauset{oldTbInfo.DefCauss()[4]},
			}
			if addIndexNeedRemoveOp(addIndexOpInfo1.AmendOpType) {
				expectedAmendOps = append(expectedAmendOps, &amendOperationDeleteOldIndex{
					info: addIndexOpInfo1,
				})
			}
			if addIndexNeedAddOp(addIndexOpInfo1.AmendOpType) {
				expectedAmendOps = append(expectedAmendOps, &amendOperationAddNewIndex{
					info: addIndexOpInfo1,
				})
			}
			// Check collect results.
			for i, amendOp := range collector.tblAmendOpMap[tblID] {
				oldOp, ok := amendOp.(*amendOperationDeleteOldIndex)
				var info *amendOperationAddIndexInfo
				var expectedInfo *amendOperationAddIndexInfo
				if ok {
					info = oldOp.info
					expectedOp, ok := expectedAmendOps[i].(*amendOperationDeleteOldIndex)
					c.Assert(ok, IsTrue)
					expectedInfo = expectedOp.info
				} else {
					newOp, ok := amendOp.(*amendOperationAddNewIndex)
					c.Assert(ok, IsTrue)
					info = newOp.info
					expectedOp, ok := expectedAmendOps[i].(*amendOperationAddNewIndex)
					c.Assert(ok, IsTrue)
					expectedInfo = expectedOp.info
				}
				c.Assert(info.AmendOpType, Equals, expectedInfo.AmendOpType)
				c.Assert(info.tblInfoAtStart, Equals, expectedInfo.tblInfoAtStart)
				c.Assert(info.tblInfoAtCommit, Equals, expectedInfo.tblInfoAtCommit)
				c.Assert(info.indexInfoAtStart, Equals, expectedInfo.indexInfoAtStart)
				c.Assert(info.indexInfoAtCommit, Equals, expectedInfo.indexInfoAtCommit)
				for j, col := range expectedInfo.relatedOldIdxDefCauss {
					c.Assert(col, Equals, expectedInfo.relatedOldIdxDefCauss[j])
				}
			}
			// Generated test data.
			mutations := einsteindb.NewCommiterMutations(8)
			newData, oldData, expectedMutations := prepareTestData(se, &mutations, oldTbInfo, newTblInfo, expectedAmendOps, c)
			// Prepare old data in causet.
			txnPrepare, err := se.causetstore.Begin()
			c.Assert(err, IsNil)
			for i, key := range oldData.keys {
				err = txnPrepare.Set(key, oldData.values[i])
				c.Assert(err, IsNil)
			}
			err = txnPrepare.Commit(ctx)
			c.Assert(err, IsNil)
			txnCheck, err := se.causetstore.Begin()
			c.Assert(err, IsNil)
			snaFIDelata, err := txnCheck.GetSnapshot().Get(ctx, oldData.keys[0])
			c.Assert(err, IsNil)
			c.Assert(oldData.values[0], BytesEquals, snaFIDelata)
			err = txnCheck.Rollback()
			c.Assert(err, IsNil)

			// Write data for this new transaction, its memory buffer will be used by schemaReplicant amender.
			txn, err := se.causetstore.Begin()
			c.Assert(err, IsNil)
			se.txn.changeInvalidToValid(txn)
			txn, err = se.Txn(true)
			c.Assert(err, IsNil)
			for i, key := range newData.keys {
				err = txn.Set(key, newData.values[i])
				c.Assert(err, IsNil)
			}
			var oldKeys []ekv.Key
			for i, key := range oldData.keys {
				if oldData.ops[i] == ekvrpcpb.Op_Del {
					err = txn.Delete(key)
					c.Assert(err, IsNil)
				}
				oldKeys = append(oldKeys, key)
			}
			curVer, err := se.causetstore.CurrentVersion()
			c.Assert(err, IsNil)
			se.stochastikVars.TxnCtx.SetForUFIDelateTS(curVer.Ver + 1)
			snap, err := se.causetstore.GetSnapshot(ekv.Version{Ver: se.stochastikVars.TxnCtx.GetForUFIDelateTS()})
			c.Assert(err, IsNil)
			oldVals, err := snap.BatchGet(ctx, oldKeys)
			c.Assert(err, IsNil)
			c.Assert(len(oldVals), Equals, len(oldKeys))

			schemaAmender := NewSchemaAmenderForEinsteinDBTxn(se)
			// Some noisy index key values.
			for i := 0; i < 4; i++ {
				idxValue := []byte("idxValue")
				idxKey := blockcodec.EncodeIndexSeekKey(oldTbInfo.Meta().ID, oldTbInfo.Indices()[2].Meta().ID, idxValue)
				err = txn.Set(idxKey, idxValue)
				c.Assert(err, IsNil)
				mutations.Push(ekvrpcpb.Op_Put, idxKey, idxValue, false)
			}

			res, err := schemaAmender.genAllAmendMutations(ctx, mutations, collector)
			c.Assert(err, IsNil)

			// Validate generated results.
			c.Assert(len(res.GetKeys()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetValues()), Equals, len(res.GetOps()))
			c.Assert(len(res.GetPessimisticFlags()), Equals, len(res.GetOps()))
			mutationsEqual(res, &expectedMutations, c)
			err = txn.Rollback()
			c.Assert(err, IsNil)
		}
	}
}
