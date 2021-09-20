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

package rowcodec

import (
	"fmt"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

// causetDecoder contains base soliton for decode event.
type causetDecoder struct {
	event
	defCausumns      []DefCausInfo
	handleDefCausIDs []int64
	loc              *time.Location
}

// NewCausetDecoder creates a causetDecoder.
func NewCausetDecoder(defCausumns []DefCausInfo, handleDefCausIDs []int64, loc *time.Location) *causetDecoder {
	return &causetDecoder{
		defCausumns:      defCausumns,
		handleDefCausIDs: handleDefCausIDs,
		loc:              loc,
	}
}

// DefCausInfo is used as defCausumn spacetime info for event causetDecoder.
type DefCausInfo struct {
	ID                int64
	IsPKHandle        bool
	VirtualGenDefCaus bool
	Ft                *types.FieldType
}

// CausetFIDeliocoderdecodes the event to causet map.
type CausetFIDeliocoderstruct {
	causetDecoder
}

// NewCausetFIDeliocodercreates a CausetFIDelioecoder.
func NewCausetFIDelioecoder(defCausumns []DefCausInfo, loc *time.Location) *CausetFIDeliocoder{
	return &CausetFIDelioecoder{causetDecoder{
		defCausumns: defCausumns,
		loc:         loc,
	}}
}

// DecodeToCausetMap decodes byte slices to causet map.
func (causetDecoder *CausetFIDelioecoder) DecodeToCausetMap(rowData []byte, event map[int64]types.Causet) (map[int64]types.Causet, error) {
	if event == nil {
		event = make(map[int64]types.Causet, len(causetDecoder.defCausumns))
	}
	err := causetDecoder.fromBytes(rowData)
	if err != nil {
		return nil, err
	}
	for i := range causetDecoder.defCausumns {
		defCaus := &causetDecoder.defCausumns[i]
		idx, isNil, notFound := causetDecoder.event.findDefCausID(defCaus.ID)
		if !notFound && !isNil {
			defCausData := causetDecoder.getData(idx)
			d, err := causetDecoder.decodeDefCausCauset(defCaus, defCausData)
			if err != nil {
				return nil, err
			}
			event[defCaus.ID] = d
			continue
		}

		if isNil {
			var d types.Causet
			d.SetNull()
			event[defCaus.ID] = d
			continue
		}
	}
	return event, nil
}

func (causetDecoder *CausetFIDelioecoder) decodeDefCausCauset(defCaus *DefCausInfo, defCausData []byte) (types.Causet, error) {
	var d types.Causet
	switch defCaus.Ft.Tp {
	case allegrosql.TypeLonglong, allegrosql.TypeLong, allegrosql.TypeInt24, allegrosql.TypeShort, allegrosql.TypeTiny:
		if allegrosql.HasUnsignedFlag(defCaus.Ft.Flag) {
			d.SetUint64(decodeUint(defCausData))
		} else {
			d.SetInt64(decodeInt(defCausData))
		}
	case allegrosql.TypeYear:
		d.SetInt64(decodeInt(defCausData))
	case allegrosql.TypeFloat:
		_, fVal, err := codec.DecodeFloat(defCausData)
		if err != nil {
			return d, err
		}
		d.SetFloat32(float32(fVal))
	case allegrosql.TypeDouble:
		_, fVal, err := codec.DecodeFloat(defCausData)
		if err != nil {
			return d, err
		}
		d.SetFloat64(fVal)
	case allegrosql.TypeVarString, allegrosql.TypeVarchar, allegrosql.TypeString:
		d.SetString(string(defCausData), defCaus.Ft.DefCauslate)
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		d.SetBytes(defCausData)
	case allegrosql.TypeNewDecimal:
		_, dec, precision, frac, err := codec.DecodeDecimal(defCausData)
		if err != nil {
			return d, err
		}
		d.SetMysqlDecimal(dec)
		d.SetLength(precision)
		d.SetFrac(frac)
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		var t types.Time
		t.SetType(defCaus.Ft.Tp)
		t.SetFsp(int8(defCaus.Ft.Decimal))
		err := t.FromPackedUint(decodeUint(defCausData))
		if err != nil {
			return d, err
		}
		if defCaus.Ft.Tp == allegrosql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, causetDecoder.loc)
			if err != nil {
				return d, err
			}
		}
		d.SetMysqlTime(t)
	case allegrosql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(defCausData))
		dur.Fsp = int8(defCaus.Ft.Decimal)
		d.SetMysqlDuration(dur)
	case allegrosql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(defCaus.Ft.Elems, decodeUint(defCausData))
		if err != nil {
			enum = types.Enum{}
		}
		d.SetMysqlEnum(enum, defCaus.Ft.DefCauslate)
	case allegrosql.TypeSet:
		set, err := types.ParseSetValue(defCaus.Ft.Elems, decodeUint(defCausData))
		if err != nil {
			return d, err
		}
		d.SetMysqlSet(set, defCaus.Ft.DefCauslate)
	case allegrosql.TypeBit:
		byteSize := (defCaus.Ft.Flen + 7) >> 3
		d.SetMysqlBit(types.NewBinaryLiteralFromUint(decodeUint(defCausData), byteSize))
	case allegrosql.TypeJSON:
		var j json.BinaryJSON
		j.TypeCode = defCausData[0]
		j.Value = defCausData[1:]
		d.SetMysqlJSON(j)
	default:
		return d, errors.Errorf("unknown type %d", defCaus.Ft.Tp)
	}
	return d, nil
}

// ChunkCausetDecoder decodes the event to chunk.Chunk.
type ChunkCausetDecoder struct {
	causetDecoder
	defCauset func(i int, chk *chunk.Chunk) error
}

// NewChunkCausetDecoder creates a NewChunkCausetDecoder.
func NewChunkCausetDecoder(defCausumns []DefCausInfo, handleDefCausIDs []int64, defCauset func(i int, chk *chunk.Chunk) error, loc *time.Location) *ChunkCausetDecoder {
	return &ChunkCausetDecoder{
		causetDecoder: causetDecoder{
			defCausumns:      defCausumns,
			handleDefCausIDs: handleDefCausIDs,
			loc:              loc,
		},
		defCauset: defCauset,
	}
}

// DecodeToChunk decodes a event to chunk.
func (causetDecoder *ChunkCausetDecoder) DecodeToChunk(rowData []byte, handle ekv.Handle, chk *chunk.Chunk) error {
	err := causetDecoder.fromBytes(rowData)
	if err != nil {
		return err
	}

	for defCausIdx := range causetDecoder.defCausumns {
		defCaus := &causetDecoder.defCausumns[defCausIdx]
		// fill the virtual defCausumn value after event calculation
		if defCaus.VirtualGenDefCaus {
			chk.AppendNull(defCausIdx)
			continue
		}

		idx, isNil, notFound := causetDecoder.event.findDefCausID(defCaus.ID)
		if !notFound && !isNil {
			defCausData := causetDecoder.getData(idx)
			err := causetDecoder.decodeDefCausToChunk(defCausIdx, defCaus, defCausData, chk)
			if err != nil {
				return err
			}
			continue
		}

		if causetDecoder.tryAppendHandleDeferredCauset(defCausIdx, defCaus, handle, chk) {
			continue
		}

		if isNil {
			chk.AppendNull(defCausIdx)
			continue
		}

		if causetDecoder.defCauset == nil {
			chk.AppendNull(defCausIdx)
			continue
		}

		err := causetDecoder.defCauset(defCausIdx, chk)
		if err != nil {
			return err
		}
	}
	return nil
}

func (causetDecoder *ChunkCausetDecoder) tryAppendHandleDeferredCauset(defCausIdx int, defCaus *DefCausInfo, handle ekv.Handle, chk *chunk.Chunk) bool {
	if handle == nil {
		return false
	}
	if handle.IsInt() && defCaus.ID == causetDecoder.handleDefCausIDs[0] {
		chk.AppendInt64(defCausIdx, handle.IntValue())
		return true
	}
	for i, id := range causetDecoder.handleDefCausIDs {
		if defCaus.ID == id {
			coder := codec.NewCausetDecoder(chk, causetDecoder.loc)
			_, err := coder.DecodeOne(handle.EncodedDefCaus(i), defCausIdx, defCaus.Ft)
			if err != nil {
				return false
			}
			return true
		}
	}
	return false
}

func (causetDecoder *ChunkCausetDecoder) decodeDefCausToChunk(defCausIdx int, defCaus *DefCausInfo, defCausData []byte, chk *chunk.Chunk) error {
	switch defCaus.Ft.Tp {
	case allegrosql.TypeLonglong, allegrosql.TypeLong, allegrosql.TypeInt24, allegrosql.TypeShort, allegrosql.TypeTiny:
		if allegrosql.HasUnsignedFlag(defCaus.Ft.Flag) {
			chk.AppendUint64(defCausIdx, decodeUint(defCausData))
		} else {
			chk.AppendInt64(defCausIdx, decodeInt(defCausData))
		}
	case allegrosql.TypeYear:
		chk.AppendInt64(defCausIdx, decodeInt(defCausData))
	case allegrosql.TypeFloat:
		_, fVal, err := codec.DecodeFloat(defCausData)
		if err != nil {
			return err
		}
		chk.AppendFloat32(defCausIdx, float32(fVal))
	case allegrosql.TypeDouble:
		_, fVal, err := codec.DecodeFloat(defCausData)
		if err != nil {
			return err
		}
		chk.AppendFloat64(defCausIdx, fVal)
	case allegrosql.TypeVarString, allegrosql.TypeVarchar, allegrosql.TypeString,
		allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		chk.AppendBytes(defCausIdx, defCausData)
	case allegrosql.TypeNewDecimal:
		_, dec, _, frac, err := codec.DecodeDecimal(defCausData)
		if err != nil {
			return err
		}
		if defCaus.Ft.Decimal != types.UnspecifiedLength && frac > defCaus.Ft.Decimal {
			to := new(types.MyDecimal)
			err := dec.Round(to, defCaus.Ft.Decimal, types.ModeHalfEven)
			if err != nil {
				return errors.Trace(err)
			}
			dec = to
		}
		chk.AppendMyDecimal(defCausIdx, dec)
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		var t types.Time
		t.SetType(defCaus.Ft.Tp)
		t.SetFsp(int8(defCaus.Ft.Decimal))
		err := t.FromPackedUint(decodeUint(defCausData))
		if err != nil {
			return err
		}
		if defCaus.Ft.Tp == allegrosql.TypeTimestamp && causetDecoder.loc != nil && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, causetDecoder.loc)
			if err != nil {
				return err
			}
		}
		chk.AppendTime(defCausIdx, t)
	case allegrosql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(defCausData))
		dur.Fsp = int8(defCaus.Ft.Decimal)
		chk.AppendDuration(defCausIdx, dur)
	case allegrosql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(defCaus.Ft.Elems, decodeUint(defCausData))
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(defCausIdx, enum)
	case allegrosql.TypeSet:
		set, err := types.ParseSetValue(defCaus.Ft.Elems, decodeUint(defCausData))
		if err != nil {
			return err
		}
		chk.AppendSet(defCausIdx, set)
	case allegrosql.TypeBit:
		byteSize := (defCaus.Ft.Flen + 7) >> 3
		chk.AppendBytes(defCausIdx, types.NewBinaryLiteralFromUint(decodeUint(defCausData), byteSize))
	case allegrosql.TypeJSON:
		var j json.BinaryJSON
		j.TypeCode = defCausData[0]
		j.Value = defCausData[1:]
		chk.AppendJSON(defCausIdx, j)
	default:
		return errors.Errorf("unknown type %d", defCaus.Ft.Tp)
	}
	return nil
}

// BytesCausetDecoder decodes the event to old datums bytes.
type BytesCausetDecoder struct {
	causetDecoder
	defBytes func(i int) ([]byte, error)
}

// NewByteCausetDecoder creates a BytesCausetDecoder.
// defBytes: provided default value bytes in old causet format(flag+defCausData).
func NewByteCausetDecoder(defCausumns []DefCausInfo, handleDefCausIDs []int64, defBytes func(i int) ([]byte, error), loc *time.Location) *BytesCausetDecoder {
	return &BytesCausetDecoder{
		causetDecoder: causetDecoder{
			defCausumns:      defCausumns,
			handleDefCausIDs: handleDefCausIDs,
			loc:              loc,
		},
		defBytes: defBytes,
	}
}

func (causetDecoder *BytesCausetDecoder) decodeToBytesInternal(outputOffset map[int64]int, handle ekv.Handle, value []byte, cacheBytes []byte) ([][]byte, error) {
	var r event
	err := r.fromBytes(value)
	if err != nil {
		return nil, err
	}
	values := make([][]byte, len(outputOffset))
	for i := range causetDecoder.defCausumns {
		defCaus := &causetDecoder.defCausumns[i]
		tp := fieldType2Flag(defCaus.Ft.Tp, defCaus.Ft.Flag&allegrosql.UnsignedFlag == 0)
		defCausID := defCaus.ID
		offset := outputOffset[defCausID]
		if causetDecoder.tryDecodeHandle(values, offset, defCaus, handle, cacheBytes) {
			continue
		}

		idx, isNil, notFound := r.findDefCausID(defCausID)
		if !notFound && !isNil {
			val := r.getData(idx)
			values[offset] = causetDecoder.encodeOldCauset(tp, val)
			continue
		}

		if isNil {
			values[offset] = []byte{NilFlag}
			continue
		}

		if causetDecoder.defBytes != nil {
			defVal, err := causetDecoder.defBytes(i)
			if err != nil {
				return nil, err
			}
			if len(defVal) > 0 {
				values[offset] = defVal
				continue
			}
		}

		values[offset] = []byte{NilFlag}
	}
	return values, nil
}

func (causetDecoder *BytesCausetDecoder) tryDecodeHandle(values [][]byte, offset int, defCaus *DefCausInfo,
	handle ekv.Handle, cacheBytes []byte) bool {
	if handle == nil {
		return false
	}
	if defCaus.IsPKHandle || defCaus.ID == perceptron.ExtraHandleID {
		handleData := cacheBytes
		if allegrosql.HasUnsignedFlag(defCaus.Ft.Flag) {
			handleData = append(handleData, UintFlag)
			handleData = codec.EncodeUint(handleData, uint64(handle.IntValue()))
		} else {
			handleData = append(handleData, IntFlag)
			handleData = codec.EncodeInt(handleData, handle.IntValue())
		}
		values[offset] = handleData
		return true
	}
	var handleData []byte
	for i, hid := range causetDecoder.handleDefCausIDs {
		if defCaus.ID == hid {
			handleData = append(handleData, handle.EncodedDefCaus(i)...)
		}
	}
	if len(handleData) > 0 {
		values[offset] = handleData
		return true
	}
	return false
}

// DecodeToBytesNoHandle decodes raw byte slice to event dat without handle.
func (causetDecoder *BytesCausetDecoder) DecodeToBytesNoHandle(outputOffset map[int64]int, value []byte) ([][]byte, error) {
	return causetDecoder.decodeToBytesInternal(outputOffset, nil, value, nil)
}

// DecodeToBytes decodes raw byte slice to event data.
func (causetDecoder *BytesCausetDecoder) DecodeToBytes(outputOffset map[int64]int, handle ekv.Handle, value []byte, cacheBytes []byte) ([][]byte, error) {
	return causetDecoder.decodeToBytesInternal(outputOffset, handle, value, cacheBytes)
}

func (causetDecoder *BytesCausetDecoder) encodeOldCauset(tp byte, val []byte) []byte {
	var buf []byte
	switch tp {
	case BytesFlag:
		buf = append(buf, CompactBytesFlag)
		buf = codec.EncodeCompactBytes(buf, val)
	case IntFlag:
		buf = append(buf, VarintFlag)
		buf = codec.EncodeVarint(buf, decodeInt(val))
	case UintFlag:
		buf = append(buf, VaruintFlag)
		buf = codec.EncodeUvarint(buf, decodeUint(val))
	default:
		buf = append(buf, tp)
		buf = append(buf, val...)
	}
	return buf
}

// fieldType2Flag transforms field type into ekv type flag.
func fieldType2Flag(tp byte, signed bool) (flag byte) {
	switch tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		if signed {
			flag = IntFlag
		} else {
			flag = UintFlag
		}
	case allegrosql.TypeFloat, allegrosql.TypeDouble:
		flag = FloatFlag
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		allegrosql.TypeString, allegrosql.TypeVarchar, allegrosql.TypeVarString:
		flag = BytesFlag
	case allegrosql.TypeDatetime, allegrosql.TypeDate, allegrosql.TypeTimestamp:
		flag = UintFlag
	case allegrosql.TypeDuration:
		flag = IntFlag
	case allegrosql.TypeNewDecimal:
		flag = DecimalFlag
	case allegrosql.TypeYear:
		flag = IntFlag
	case allegrosql.TypeEnum, allegrosql.TypeBit, allegrosql.TypeSet:
		flag = UintFlag
	case allegrosql.TypeJSON:
		flag = JSONFlag
	case allegrosql.TypeNull:
		flag = NilFlag
	default:
		panic(fmt.Sprintf("unknown field type %d", tp))
	}
	return
}
