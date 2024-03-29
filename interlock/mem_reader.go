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
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/allegrosql"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

type memIndexReader struct {
	ctx            stochastikctx.Context
	index          *perceptron.IndexInfo
	causet         *perceptron.BlockInfo
	ekvRanges      []ekv.KeyRange
	desc           bool
	conditions     []memex.Expression
	addedEvents    [][]types.Causet
	addedEventsLen int
	retFieldTypes  []*types.FieldType
	outputOffset   []int
	// belowHandleDefCauss is the handle's position of the below scan plan.
	belowHandleDefCauss causetembedded.HandleDefCauss
}

func buildMemIndexReader(us *UnionScanInterDirc, idxReader *IndexReaderInterlockingDirectorate) *memIndexReader {
	ekvRanges := idxReader.ekvRanges
	outputOffset := make([]int, 0, len(us.defCausumns))
	for _, defCaus := range idxReader.outputDeferredCausets {
		outputOffset = append(outputOffset, defCaus.Index)
	}
	return &memIndexReader{
		ctx:                 us.ctx,
		index:               idxReader.index,
		causet:              idxReader.causet.Meta(),
		ekvRanges:           ekvRanges,
		desc:                us.desc,
		conditions:          us.conditions,
		retFieldTypes:       retTypes(us),
		outputOffset:        outputOffset,
		belowHandleDefCauss: us.belowHandleDefCauss,
	}
}

func (m *memIndexReader) getMemEvents() ([][]types.Causet, error) {
	tps := make([]*types.FieldType, 0, len(m.index.DeferredCausets)+1)
	defcaus := m.causet.DeferredCausets
	for _, defCaus := range m.index.DeferredCausets {
		tps = append(tps, &defcaus[defCaus.Offset].FieldType)
	}
	switch {
	case m.causet.PKIsHandle:
		for _, defCaus := range m.causet.DeferredCausets {
			if allegrosql.HasPriKeyFlag(defCaus.Flag) {
				tps = append(tps, &defCaus.FieldType)
				break
			}
		}
	case m.causet.IsCommonHandle:
		pkIdx := blocks.FindPrimaryIndex(m.causet)
		for _, pkDefCaus := range pkIdx.DeferredCausets {
			defCausInfo := m.causet.DeferredCausets[pkDefCaus.Offset]
			tps = append(tps, &defCausInfo.FieldType)
		}
	default: // ExtraHandle DeferredCauset tp.
		tps = append(tps, types.NewFieldType(allegrosql.TypeLonglong))
	}

	mublockEvent := chunk.MutEventFromTypes(m.retFieldTypes)
	err := iterTxnMemBuffer(m.ctx, m.ekvRanges, func(key, value []byte) error {
		data, err := m.decodeIndexKeyValue(key, value, tps)
		if err != nil {
			return err
		}

		mublockEvent.SetCausets(data...)
		matched, _, err := memex.EvalBool(m.ctx, m.conditions, mublockEvent.ToEvent())
		if err != nil || !matched {
			return err
		}
		m.addedEvents = append(m.addedEvents, data)
		return nil
	})

	if err != nil {
		return nil, err
	}
	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseCausetSlice(m.addedEvents)
	}
	return m.addedEvents, nil
}

func (m *memIndexReader) decodeIndexKeyValue(key, value []byte, tps []*types.FieldType) ([]types.Causet, error) {
	hdStatus := blockcodec.HandleDefault
	if allegrosql.HasUnsignedFlag(tps[len(tps)-1].Flag) {
		hdStatus = blockcodec.HandleIsUnsigned
	}
	defCausInfos := make([]rowcodec.DefCausInfo, 0, len(m.index.DeferredCausets))
	for _, idxDefCaus := range m.index.DeferredCausets {
		defCaus := m.causet.DeferredCausets[idxDefCaus.Offset]
		defCausInfos = append(defCausInfos, rowcodec.DefCausInfo{
			ID:         defCaus.ID,
			IsPKHandle: m.causet.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag),
			Ft:         rowcodec.FieldTypeFromPerceptronDeferredCauset(defCaus),
		})
	}
	values, err := blockcodec.DecodeIndexKV(key, value, len(m.index.DeferredCausets), hdStatus, defCausInfos)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ds := make([]types.Causet, 0, len(m.outputOffset))
	for _, offset := range m.outputOffset {
		d, err := blockcodec.DecodeDeferredCausetValue(values[offset], tps[offset], m.ctx.GetStochastikVars().TimeZone)
		if err != nil {
			return nil, err
		}
		ds = append(ds, d)
	}
	return ds, nil
}

type memBlockReader struct {
	ctx           stochastikctx.Context
	causet        *perceptron.BlockInfo
	defCausumns   []*perceptron.DeferredCausetInfo
	ekvRanges     []ekv.KeyRange
	desc          bool
	conditions    []memex.Expression
	addedEvents   [][]types.Causet
	retFieldTypes []*types.FieldType
	defCausIDs    map[int64]int
	buffer        allocBuf
	pkDefCausIDs  []int64
}

type allocBuf struct {
	// cache for decode handle.
	handleBytes []byte
	rd          *rowcodec.BytesCausetDecoder
}

func buildMemBlockReader(us *UnionScanInterDirc, tblReader *BlockReaderInterlockingDirectorate) *memBlockReader {
	defCausIDs := make(map[int64]int, len(us.defCausumns))
	for i, defCaus := range us.defCausumns {
		defCausIDs[defCaus.ID] = i
	}

	defCausInfo := make([]rowcodec.DefCausInfo, 0, len(us.defCausumns))
	for i := range us.defCausumns {
		defCaus := us.defCausumns[i]
		defCausInfo = append(defCausInfo, rowcodec.DefCausInfo{
			ID:         defCaus.ID,
			IsPKHandle: us.causet.Meta().PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag),
			Ft:         rowcodec.FieldTypeFromPerceptronDeferredCauset(defCaus),
		})
	}

	pkDefCausIDs := blocks.TryGetCommonPkDeferredCausetIds(us.causet.Meta())
	if len(pkDefCausIDs) == 0 {
		pkDefCausIDs = []int64{-1}
	}
	rd := rowcodec.NewByteCausetDecoder(defCausInfo, pkDefCausIDs, nil, us.ctx.GetStochastikVars().TimeZone)
	return &memBlockReader{
		ctx:           us.ctx,
		causet:        us.causet.Meta(),
		defCausumns:   us.defCausumns,
		ekvRanges:     tblReader.ekvRanges,
		desc:          us.desc,
		conditions:    us.conditions,
		retFieldTypes: retTypes(us),
		defCausIDs:    defCausIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
		},
		pkDefCausIDs: pkDefCausIDs,
	}
}

// TODO: Try to make memXXXReader lazy, There is no need to decode many rows when parent operator only need 1 event.
func (m *memBlockReader) getMemEvents() ([][]types.Causet, error) {
	mublockEvent := chunk.MutEventFromTypes(m.retFieldTypes)
	err := iterTxnMemBuffer(m.ctx, m.ekvRanges, func(key, value []byte) error {
		event, err := m.decodeRecordKeyValue(key, value)
		if err != nil {
			return err
		}

		mublockEvent.SetCausets(event...)
		matched, _, err := memex.EvalBool(m.ctx, m.conditions, mublockEvent.ToEvent())
		if err != nil || !matched {
			return err
		}
		m.addedEvents = append(m.addedEvents, event)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// TODO: After refine `IterReverse`, remove below logic and use `IterReverse` when do reverse scan.
	if m.desc {
		reverseCausetSlice(m.addedEvents)
	}
	return m.addedEvents, nil
}

func (m *memBlockReader) decodeRecordKeyValue(key, value []byte) ([]types.Causet, error) {
	handle, err := blockcodec.DecodeEventKey(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m.decodeEventData(handle, value)
}

// decodeEventData uses to decode event data value.
func (m *memBlockReader) decodeEventData(handle ekv.Handle, value []byte) ([]types.Causet, error) {
	values, err := m.getEventData(handle, value)
	if err != nil {
		return nil, err
	}
	ds := make([]types.Causet, 0, len(m.defCausumns))
	for _, defCaus := range m.defCausumns {
		offset := m.defCausIDs[defCaus.ID]
		d, err := blockcodec.DecodeDeferredCausetValue(values[offset], &defCaus.FieldType, m.ctx.GetStochastikVars().TimeZone)
		if err != nil {
			return nil, err
		}
		ds = append(ds, d)
	}
	return ds, nil
}

// getEventData decodes raw byte slice to event data.
func (m *memBlockReader) getEventData(handle ekv.Handle, value []byte) ([][]byte, error) {
	defCausIDs := m.defCausIDs
	pkIsHandle := m.causet.PKIsHandle
	buffer := &m.buffer
	ctx := m.ctx.GetStochastikVars().StmtCtx
	if rowcodec.IsNewFormat(value) {
		return buffer.rd.DecodeToBytes(defCausIDs, handle, value, buffer.handleBytes)
	}
	values, err := blockcodec.CutEventNew(value, defCausIDs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if values == nil {
		values = make([][]byte, len(defCausIDs))
	}
	// Fill the handle and null defCausumns.
	for _, defCaus := range m.defCausumns {
		id := defCaus.ID
		offset := defCausIDs[id]
		if m.causet.IsCommonHandle {
			for i, defCausID := range m.pkDefCausIDs {
				if defCausID == defCaus.ID {
					values[offset] = handle.EncodedDefCaus(i)
					break
				}
			}
			continue
		}
		if (pkIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag)) || id == perceptron.ExtraHandleID {
			var handleCauset types.Causet
			if allegrosql.HasUnsignedFlag(defCaus.Flag) {
				// PK defCausumn is Unsigned.
				handleCauset = types.NewUintCauset(uint64(handle.IntValue()))
			} else {
				handleCauset = types.NewIntCauset(handle.IntValue())
			}
			handleData, err1 := codec.EncodeValue(ctx, buffer.handleBytes, handleCauset)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			values[offset] = handleData
			continue
		}
		if hasDefCausVal(values, defCausIDs, id) {
			continue
		}
		// no need to fill default value.
		values[offset] = []byte{codec.NilFlag}
	}

	return values, nil
}

func hasDefCausVal(data [][]byte, defCausIDs map[int64]int, id int64) bool {
	offset, ok := defCausIDs[id]
	if ok && data[offset] != nil {
		return true
	}
	return false
}

type processKVFunc func(key, value []byte) error

func iterTxnMemBuffer(ctx stochastikctx.Context, ekvRanges []ekv.KeyRange, fn processKVFunc) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	for _, rg := range ekvRanges {
		iter := txn.GetMemBuffer().SnapshotIter(rg.StartKey, rg.EndKey)
		for ; iter.Valid(); err = iter.Next() {
			if err != nil {
				return err
			}
			// check whether the key was been deleted.
			if len(iter.Value()) == 0 {
				continue
			}
			err = fn(iter.Key(), iter.Value())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func reverseCausetSlice(rows [][]types.Causet) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func (m *memIndexReader) getMemEventsHandle() ([]ekv.Handle, error) {
	handles := make([]ekv.Handle, 0, m.addedEventsLen)
	err := iterTxnMemBuffer(m.ctx, m.ekvRanges, func(key, value []byte) error {
		handle, err := blockcodec.DecodeIndexHandle(key, value, len(m.index.DeferredCausets))
		if err != nil {
			return err
		}
		handles = append(handles, handle)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if m.desc {
		for i, j := 0, len(handles)-1; i < j; i, j = i+1, j-1 {
			handles[i], handles[j] = handles[j], handles[i]
		}
	}
	return handles, nil
}

type memIndexLookUpReader struct {
	ctx           stochastikctx.Context
	index         *perceptron.IndexInfo
	defCausumns   []*perceptron.DeferredCausetInfo
	causet        causet.Block
	desc          bool
	conditions    []memex.Expression
	retFieldTypes []*types.FieldType

	idxReader *memIndexReader
}

func buildMemIndexLookUpReader(us *UnionScanInterDirc, idxLookUpReader *IndexLookUpInterlockingDirectorate) *memIndexLookUpReader {
	ekvRanges := idxLookUpReader.ekvRanges
	outputOffset := []int{len(idxLookUpReader.index.DeferredCausets)}
	memIdxReader := &memIndexReader{
		ctx:                 us.ctx,
		index:               idxLookUpReader.index,
		causet:              idxLookUpReader.causet.Meta(),
		ekvRanges:           ekvRanges,
		desc:                idxLookUpReader.desc,
		retFieldTypes:       retTypes(us),
		outputOffset:        outputOffset,
		belowHandleDefCauss: us.belowHandleDefCauss,
	}

	return &memIndexLookUpReader{
		ctx:           us.ctx,
		index:         idxLookUpReader.index,
		defCausumns:   idxLookUpReader.defCausumns,
		causet:        idxLookUpReader.causet,
		desc:          idxLookUpReader.desc,
		conditions:    us.conditions,
		retFieldTypes: retTypes(us),
		idxReader:     memIdxReader,
	}
}

func (m *memIndexLookUpReader) getMemEvents() ([][]types.Causet, error) {
	handles, err := m.idxReader.getMemEventsHandle()
	if err != nil || len(handles) == 0 {
		return nil, err
	}

	tblKVRanges := allegrosql.BlockHandlesToKVRanges(getPhysicalBlockID(m.causet), handles)
	defCausIDs := make(map[int64]int, len(m.defCausumns))
	for i, defCaus := range m.defCausumns {
		defCausIDs[defCaus.ID] = i
	}

	tblInfo := m.causet.Meta()
	defCausInfos := make([]rowcodec.DefCausInfo, 0, len(m.defCausumns))
	for i := range m.defCausumns {
		defCaus := m.defCausumns[i]
		defCausInfos = append(defCausInfos, rowcodec.DefCausInfo{
			ID:         defCaus.ID,
			IsPKHandle: tblInfo.PKIsHandle && allegrosql.HasPriKeyFlag(defCaus.Flag),
			Ft:         rowcodec.FieldTypeFromPerceptronDeferredCauset(defCaus),
		})
	}
	handleDefCausIDs := []int64{-1}
	if tblInfo.IsCommonHandle {
		handleDefCausIDs = handleDefCausIDs[:0]
		pkIdx := blocks.FindPrimaryIndex(tblInfo)
		for _, idxDefCaus := range pkIdx.DeferredCausets {
			defCausID := tblInfo.DeferredCausets[idxDefCaus.Offset].ID
			handleDefCausIDs = append(handleDefCausIDs, defCausID)
		}
	}
	rd := rowcodec.NewByteCausetDecoder(defCausInfos, handleDefCausIDs, nil, nil)
	memTblReader := &memBlockReader{
		ctx:           m.ctx,
		causet:        m.causet.Meta(),
		defCausumns:   m.defCausumns,
		ekvRanges:     tblKVRanges,
		conditions:    m.conditions,
		addedEvents:   make([][]types.Causet, 0, len(handles)),
		retFieldTypes: m.retFieldTypes,
		defCausIDs:    defCausIDs,
		buffer: allocBuf{
			handleBytes: make([]byte, 0, 16),
			rd:          rd,
		},
	}

	return memTblReader.getMemEvents()
}
