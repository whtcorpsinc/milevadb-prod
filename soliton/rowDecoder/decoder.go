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

package decoder

import (
	"sort"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/block/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
)

// DeferredCauset contains the info and generated expr of defCausumn.
type DeferredCauset struct {
	DefCaus     *block.DeferredCauset
	GenExpr expression.Expression
}

// RowDecoder decodes a byte slice into datums and eval the generated defCausumn value.
type RowDecoder struct {
	tbl           block.Block
	mutRow        chunk.MutRow
	defCausMap        map[int64]DeferredCauset
	defCausTypes      map[int64]*types.FieldType
	haveGenDeferredCauset bool
	defaultVals   []types.Causet
	defcaus          []*block.DeferredCauset
	pkDefCauss        []int64
}

// NewRowDecoder returns a new RowDecoder.
func NewRowDecoder(tbl block.Block, defcaus []*block.DeferredCauset, decodeDefCausMap map[int64]DeferredCauset) *RowDecoder {
	tblInfo := tbl.Meta()
	defCausFieldMap := make(map[int64]*types.FieldType, len(decodeDefCausMap))
	for id, defCaus := range decodeDefCausMap {
		defCausFieldMap[id] = &defCaus.DefCaus.DeferredCausetInfo.FieldType
	}

	tps := make([]*types.FieldType, len(defcaus))
	for _, defCaus := range defcaus {
		tps[defCaus.Offset] = &defCaus.FieldType
	}
	var pkDefCauss []int64
	switch {
	case tblInfo.IsCommonHandle:
		pkDefCauss = blocks.TryGetCommonPkDeferredCausetIds(tbl.Meta())
	case tblInfo.PKIsHandle:
		pkDefCauss = []int64{tblInfo.GetPkDefCausInfo().ID}
	}
	return &RowDecoder{
		tbl:         tbl,
		mutRow:      chunk.MutRowFromTypes(tps),
		defCausMap:      decodeDefCausMap,
		defCausTypes:    defCausFieldMap,
		defaultVals: make([]types.Causet, len(defcaus)),
		defcaus:        defcaus,
		pkDefCauss:      pkDefCauss,
	}
}

// DecodeAndEvalRowWithMap decodes a byte slice into datums and evaluates the generated defCausumn value.
func (rd *RowDecoder) DecodeAndEvalRowWithMap(ctx stochastikctx.Context, handle ekv.Handle, b []byte, decodeLoc, sysLoc *time.Location, event map[int64]types.Causet) (map[int64]types.Causet, error) {
	var err error
	if rowcodec.IsNewFormat(b) {
		event, err = blockcodec.DecodeRowWithMapNew(b, rd.defCausTypes, decodeLoc, event)
	} else {
		event, err = blockcodec.DecodeRowWithMap(b, rd.defCausTypes, decodeLoc, event)
	}
	if err != nil {
		return nil, err
	}
	event, err = blockcodec.DecodeHandleToCausetMap(handle, rd.pkDefCauss, rd.defCausTypes, decodeLoc, event)
	if err != nil {
		return nil, err
	}
	for _, dDefCaus := range rd.defCausMap {
		defCausInfo := dDefCaus.DefCaus.DeferredCausetInfo
		val, ok := event[defCausInfo.ID]
		if ok || dDefCaus.GenExpr != nil {
			rd.mutRow.SetValue(defCausInfo.Offset, val.GetValue())
			continue
		}
		if dDefCaus.DefCaus.ChangeStateInfo != nil {
			val, _, err = blocks.GetChangingDefCausVal(ctx, rd.defcaus, dDefCaus.DefCaus, event, rd.defaultVals)
		} else {
			// Get the default value of the defCausumn in the generated defCausumn expression.
			val, err = blocks.GetDefCausDefaultValue(ctx, dDefCaus.DefCaus, rd.defaultVals)
		}
		if err != nil {
			return nil, err
		}
		rd.mutRow.SetValue(defCausInfo.Offset, val.GetValue())
	}
	keys := make([]int, 0)
	ids := make(map[int]int)
	for k, defCaus := range rd.defCausMap {
		keys = append(keys, defCaus.DefCaus.Offset)
		ids[defCaus.DefCaus.Offset] = int(k)
	}
	sort.Ints(keys)
	for _, id := range keys {
		defCaus := rd.defCausMap[int64(ids[id])]
		if defCaus.GenExpr == nil {
			continue
		}
		// Eval the defCausumn value
		val, err := defCaus.GenExpr.Eval(rd.mutRow.ToRow())
		if err != nil {
			return nil, err
		}
		val, err = block.CastValue(ctx, val, defCaus.DefCaus.DeferredCausetInfo, false, true)
		if err != nil {
			return nil, err
		}

		if val.HoTT() == types.HoTTMysqlTime && sysLoc != time.UTC {
			t := val.GetMysqlTime()
			if t.Type() == allegrosql.TypeTimestamp {
				err := t.ConvertTimeZone(sysLoc, time.UTC)
				if err != nil {
					return nil, err
				}
				val.SetMysqlTime(t)
			}
		}
		rd.mutRow.SetValue(defCaus.DefCaus.Offset, val.GetValue())

		event[int64(ids[id])] = val
	}
	return event, nil
}

// BuildFullDecodeDefCausMap builds a map that contains [defCausumnID -> struct{*block.DeferredCauset, expression.Expression}] from all defCausumns.
func BuildFullDecodeDefCausMap(defcaus []*block.DeferredCauset, schemaReplicant *expression.Schema) map[int64]DeferredCauset {
	decodeDefCausMap := make(map[int64]DeferredCauset, len(defcaus))
	for _, defCaus := range defcaus {
		decodeDefCausMap[defCaus.ID] = DeferredCauset{
			DefCaus:     defCaus,
			GenExpr: schemaReplicant.DeferredCausets[defCaus.Offset].VirtualExpr,
		}
	}
	return decodeDefCausMap
}
