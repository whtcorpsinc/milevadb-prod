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

package chunk

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
)

// MutRow represents a mublock Row.
// The underlying defCausumns only contains one event and not exposed to the user.
type MutRow Row

// ToRow converts the MutRow to Row, so it can be used to read data.
func (mr MutRow) ToRow() Row {
	return Row(mr)
}

// Len returns the number of defCausumns.
func (mr MutRow) Len() int {
	return len(mr.c.defCausumns)
}

// Clone deep clone a MutRow.
func (mr MutRow) Clone() MutRow {
	newChk := mr.c
	if mr.c != nil {
		newChk = mr.c.CopyConstruct()
	}
	return MutRow{
		c:   newChk,
		idx: mr.idx,
	}
}

// MutRowFromValues creates a MutRow from a interface slice.
func MutRowFromValues(vals ...interface{}) MutRow {
	c := &Chunk{defCausumns: make([]*DeferredCauset, 0, len(vals))}
	for _, val := range vals {
		defCaus := makeMutRowDeferredCauset(val)
		c.defCausumns = append(c.defCausumns, defCaus)
	}
	return MutRow{c: c}
}

// MutRowFromCausets creates a MutRow from a causet slice.
func MutRowFromCausets(datums []types.Causet) MutRow {
	c := &Chunk{defCausumns: make([]*DeferredCauset, 0, len(datums))}
	for _, d := range datums {
		defCaus := makeMutRowDeferredCauset(d.GetValue())
		c.defCausumns = append(c.defCausumns, defCaus)
	}
	return MutRow{c: c, idx: 0}
}

// MutRowFromTypes creates a MutRow from a FieldType slice, each DeferredCauset is initialized to zero value.
func MutRowFromTypes(types []*types.FieldType) MutRow {
	c := &Chunk{defCausumns: make([]*DeferredCauset, 0, len(types))}
	for _, tp := range types {
		defCaus := makeMutRowDeferredCauset(zeroValForType(tp))
		c.defCausumns = append(c.defCausumns, defCaus)
	}
	return MutRow{c: c, idx: 0}
}

func zeroValForType(tp *types.FieldType) interface{} {
	switch tp.Tp {
	case allegrosql.TypeFloat:
		return float32(0)
	case allegrosql.TypeDouble:
		return float64(0)
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
		if allegrosql.HasUnsignedFlag(tp.Flag) {
			return uint64(0)
		}
		return int64(0)
	case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar:
		return ""
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		return []byte{}
	case allegrosql.TypeDuration:
		return types.ZeroDuration
	case allegrosql.TypeNewDecimal:
		return types.NewDecFromInt(0)
	case allegrosql.TypeDate:
		return types.ZeroDate
	case allegrosql.TypeDatetime:
		return types.ZeroDatetime
	case allegrosql.TypeTimestamp:
		return types.ZeroTimestamp
	case allegrosql.TypeBit:
		return types.BinaryLiteral{}
	case allegrosql.TypeSet:
		return types.Set{}
	case allegrosql.TypeEnum:
		return types.Enum{}
	case allegrosql.TypeJSON:
		return json.CreateBinary(nil)
	default:
		return nil
	}
}

func makeMutRowDeferredCauset(in interface{}) *DeferredCauset {
	switch x := in.(type) {
	case nil:
		defCaus := makeMutRowBytesDeferredCauset(nil)
		defCaus.nullBitmap[0] = 0
		return defCaus
	case int:
		return makeMutRowUint64DeferredCauset(uint64(x))
	case int64:
		return makeMutRowUint64DeferredCauset(uint64(x))
	case uint64:
		return makeMutRowUint64DeferredCauset(x)
	case float64:
		return makeMutRowUint64DeferredCauset(math.Float64bits(x))
	case float32:
		defCaus := newMutRowFixedLenDeferredCauset(4)
		*(*uint32)(unsafe.Pointer(&defCaus.data[0])) = math.Float32bits(x)
		return defCaus
	case string:
		return makeMutRowBytesDeferredCauset(replog.Slice(x))
	case []byte:
		return makeMutRowBytesDeferredCauset(x)
	case types.BinaryLiteral:
		return makeMutRowBytesDeferredCauset(x)
	case *types.MyDecimal:
		defCaus := newMutRowFixedLenDeferredCauset(types.MyDecimalStructSize)
		*(*types.MyDecimal)(unsafe.Pointer(&defCaus.data[0])) = *x
		return defCaus
	case types.Time:
		defCaus := newMutRowFixedLenDeferredCauset(sizeTime)
		*(*types.Time)(unsafe.Pointer(&defCaus.data[0])) = x
		return defCaus
	case json.BinaryJSON:
		defCaus := newMutRowVarLenDeferredCauset(len(x.Value) + 1)
		defCaus.data[0] = x.TypeCode
		copy(defCaus.data[1:], x.Value)
		return defCaus
	case types.Duration:
		defCaus := newMutRowFixedLenDeferredCauset(8)
		*(*int64)(unsafe.Pointer(&defCaus.data[0])) = int64(x.Duration)
		return defCaus
	case types.Enum:
		defCaus := newMutRowVarLenDeferredCauset(len(x.Name) + 8)
		copy(defCaus.data, (*[8]byte)(unsafe.Pointer(&x.Value))[:])
		copy(defCaus.data[8:], x.Name)
		return defCaus
	case types.Set:
		defCaus := newMutRowVarLenDeferredCauset(len(x.Name) + 8)
		copy(defCaus.data, (*[8]byte)(unsafe.Pointer(&x.Value))[:])
		copy(defCaus.data[8:], x.Name)
		return defCaus
	default:
		return nil
	}
}

func newMutRowFixedLenDeferredCauset(elemSize int) *DeferredCauset {
	buf := make([]byte, elemSize)
	defCaus := &DeferredCauset{
		length:     1,
		elemBuf:    buf,
		data:       buf,
		nullBitmap: make([]byte, 1),
	}
	defCaus.nullBitmap[0] = 1
	return defCaus
}

func newMutRowVarLenDeferredCauset(valSize int) *DeferredCauset {
	buf := make([]byte, valSize+1)
	defCaus := &DeferredCauset{
		length:     1,
		offsets:    []int64{0, int64(valSize)},
		data:       buf[:valSize],
		nullBitmap: buf[valSize:],
	}
	defCaus.nullBitmap[0] = 1
	return defCaus
}

func makeMutRowUint64DeferredCauset(val uint64) *DeferredCauset {
	defCaus := newMutRowFixedLenDeferredCauset(8)
	*(*uint64)(unsafe.Pointer(&defCaus.data[0])) = val
	return defCaus
}

func makeMutRowBytesDeferredCauset(bin []byte) *DeferredCauset {
	defCaus := newMutRowVarLenDeferredCauset(len(bin))
	copy(defCaus.data, bin)
	return defCaus
}

// SetRow sets the MutRow with Row.
func (mr MutRow) SetRow(event Row) {
	for defCausIdx, rDefCaus := range event.c.defCausumns {
		mrDefCaus := mr.c.defCausumns[defCausIdx]
		if rDefCaus.IsNull(event.idx) {
			mrDefCaus.nullBitmap[0] = 0
			continue
		}
		elemLen := len(rDefCaus.elemBuf)
		if elemLen > 0 {
			copy(mrDefCaus.data, rDefCaus.data[event.idx*elemLen:(event.idx+1)*elemLen])
		} else {
			setMutRowBytes(mrDefCaus, rDefCaus.data[rDefCaus.offsets[event.idx]:rDefCaus.offsets[event.idx+1]])
		}
		mrDefCaus.nullBitmap[0] = 1
	}
}

// SetValues sets the MutRow with values.
func (mr MutRow) SetValues(vals ...interface{}) {
	for i, v := range vals {
		mr.SetValue(i, v)
	}
}

// SetValue sets the MutRow with defCausIdx and value.
func (mr MutRow) SetValue(defCausIdx int, val interface{}) {
	defCaus := mr.c.defCausumns[defCausIdx]
	if val == nil {
		defCaus.nullBitmap[0] = 0
		return
	}
	switch x := val.(type) {
	case int:
		binary.LittleEndian.PutUint64(defCaus.data, uint64(x))
	case int64:
		binary.LittleEndian.PutUint64(defCaus.data, uint64(x))
	case uint64:
		binary.LittleEndian.PutUint64(defCaus.data, x)
	case float64:
		binary.LittleEndian.PutUint64(defCaus.data, math.Float64bits(x))
	case float32:
		binary.LittleEndian.PutUint32(defCaus.data, math.Float32bits(x))
	case string:
		setMutRowBytes(defCaus, replog.Slice(x))
	case []byte:
		setMutRowBytes(defCaus, x)
	case types.BinaryLiteral:
		setMutRowBytes(defCaus, x)
	case types.Duration:
		*(*int64)(unsafe.Pointer(&defCaus.data[0])) = int64(x.Duration)
	case *types.MyDecimal:
		*(*types.MyDecimal)(unsafe.Pointer(&defCaus.data[0])) = *x
	case types.Time:
		*(*types.Time)(unsafe.Pointer(&defCaus.data[0])) = x
	case types.Enum:
		setMutRowNameValue(defCaus, x.Name, x.Value)
	case types.Set:
		setMutRowNameValue(defCaus, x.Name, x.Value)
	case json.BinaryJSON:
		setMutRowJSON(defCaus, x)
	}
	defCaus.nullBitmap[0] = 1
}

// SetCausets sets the MutRow with causet slice.
func (mr MutRow) SetCausets(datums ...types.Causet) {
	for i, d := range datums {
		mr.SetCauset(i, d)
	}
}

// SetCauset sets the MutRow with defCausIdx and causet.
func (mr MutRow) SetCauset(defCausIdx int, d types.Causet) {
	defCaus := mr.c.defCausumns[defCausIdx]
	if d.IsNull() {
		defCaus.nullBitmap[0] = 0
		return
	}
	switch d.HoTT() {
	case types.HoTTInt64, types.HoTTUint64, types.HoTTFloat64:
		binary.LittleEndian.PutUint64(mr.c.defCausumns[defCausIdx].data, d.GetUint64())
	case types.HoTTFloat32:
		binary.LittleEndian.PutUint32(mr.c.defCausumns[defCausIdx].data, math.Float32bits(d.GetFloat32()))
	case types.HoTTString, types.HoTTBytes, types.HoTTBinaryLiteral:
		setMutRowBytes(defCaus, d.GetBytes())
	case types.HoTTMysqlTime:
		*(*types.Time)(unsafe.Pointer(&defCaus.data[0])) = d.GetMysqlTime()
	case types.HoTTMysqlDuration:
		*(*int64)(unsafe.Pointer(&defCaus.data[0])) = int64(d.GetMysqlDuration().Duration)
	case types.HoTTMysqlDecimal:
		*(*types.MyDecimal)(unsafe.Pointer(&defCaus.data[0])) = *d.GetMysqlDecimal()
	case types.HoTTMysqlJSON:
		setMutRowJSON(defCaus, d.GetMysqlJSON())
	case types.HoTTMysqlEnum:
		e := d.GetMysqlEnum()
		setMutRowNameValue(defCaus, e.Name, e.Value)
	case types.HoTTMysqlSet:
		s := d.GetMysqlSet()
		setMutRowNameValue(defCaus, s.Name, s.Value)
	default:
		mr.c.defCausumns[defCausIdx] = makeMutRowDeferredCauset(d.GetValue())
	}
	defCaus.nullBitmap[0] = 1
}

func setMutRowBytes(defCaus *DeferredCauset, bin []byte) {
	if len(defCaus.data) >= len(bin) {
		defCaus.data = defCaus.data[:len(bin)]
	} else {
		buf := make([]byte, len(bin)+1)
		defCaus.data = buf[:len(bin)]
		defCaus.nullBitmap = buf[len(bin):]
	}
	copy(defCaus.data, bin)
	defCaus.offsets[1] = int64(len(bin))
}

func setMutRowNameValue(defCaus *DeferredCauset, name string, val uint64) {
	dataLen := len(name) + 8
	if len(defCaus.data) >= dataLen {
		defCaus.data = defCaus.data[:dataLen]
	} else {
		buf := make([]byte, dataLen+1)
		defCaus.data = buf[:dataLen]
		defCaus.nullBitmap = buf[dataLen:]
	}
	binary.LittleEndian.PutUint64(defCaus.data, val)
	copy(defCaus.data[8:], name)
	defCaus.offsets[1] = int64(dataLen)
}

func setMutRowJSON(defCaus *DeferredCauset, j json.BinaryJSON) {
	dataLen := len(j.Value) + 1
	if len(defCaus.data) >= dataLen {
		defCaus.data = defCaus.data[:dataLen]
	} else {
		// In MutRow, there always exists 1 data in every DeferredCauset,
		// we should allocate one more byte for null bitmap.
		buf := make([]byte, dataLen+1)
		defCaus.data = buf[:dataLen]
		defCaus.nullBitmap = buf[dataLen:]
	}
	defCaus.data[0] = j.TypeCode
	copy(defCaus.data[1:], j.Value)
	defCaus.offsets[1] = int64(dataLen)
}

// ShallowCopyPartialRow shallow copies the data of `event` to MutRow.
func (mr MutRow) ShallowCopyPartialRow(defCausIdx int, event Row) {
	for i, srcDefCaus := range event.c.defCausumns {
		dstDefCaus := mr.c.defCausumns[defCausIdx+i]
		if !srcDefCaus.IsNull(event.idx) {
			// MutRow only contains one event, so we can directly set the whole byte.
			dstDefCaus.nullBitmap[0] = 1
		} else {
			dstDefCaus.nullBitmap[0] = 0
		}

		if srcDefCaus.isFixed() {
			elemLen := len(srcDefCaus.elemBuf)
			offset := event.idx * elemLen
			dstDefCaus.data = srcDefCaus.data[offset : offset+elemLen]
		} else {
			start, end := srcDefCaus.offsets[event.idx], srcDefCaus.offsets[event.idx+1]
			dstDefCaus.data = srcDefCaus.data[start:end]
			dstDefCaus.offsets[1] = int64(len(dstDefCaus.data))
		}
	}
}
