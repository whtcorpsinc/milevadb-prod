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
	"fmt"
	"math/bits"
	"reflect"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
)

// AppendDuration appends a duration value into this DeferredCauset.
func (c *DeferredCauset) AppendDuration(dur types.Duration) {
	c.AppendInt64(int64(dur.Duration))
}

// AppendMyDecimal appends a MyDecimal value into this DeferredCauset.
func (c *DeferredCauset) AppendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *DeferredCauset) appendNameValue(name string, val uint64) {
	var buf [8]byte
	copy(buf[:], (*[8]byte)(unsafe.Pointer(&val))[:])
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

// AppendJSON appends a BinaryJSON value into this DeferredCauset.
func (c *DeferredCauset) AppendJSON(j json.BinaryJSON) {
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

// AppendSet appends a Set value into this DeferredCauset.
func (c *DeferredCauset) AppendSet(set types.Set) {
	c.appendNameValue(set.Name, set.Value)
}

// DeferredCauset stores one defCausumn of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
type DeferredCauset struct {
	length     int
	nullBitmap []byte // bit 0 is null, 1 is not null
	offsets    []int64
	data       []byte
	elemBuf    []byte
}

// NewDeferredCauset creates a new defCausumn with the specific length and capacity.
func NewDeferredCauset(ft *types.FieldType, cap int) *DeferredCauset {
	return newDeferredCauset(getFixedLen(ft), cap)
}

func newDeferredCauset(typeSize, cap int) *DeferredCauset {
	var defCaus *DeferredCauset
	if typeSize == varElemLen {
		defCaus = newVarLenDeferredCauset(cap, nil)
	} else {
		defCaus = newFixedLenDeferredCauset(typeSize, cap)
	}
	return defCaus
}

func (c *DeferredCauset) typeSize() int {
	if len(c.elemBuf) > 0 {
		return len(c.elemBuf)
	}
	return varElemLen
}

func (c *DeferredCauset) isFixed() bool {
	return c.elemBuf != nil
}

// Reset resets this DeferredCauset according to the EvalType.
// Different from reset, Reset will reset the elemBuf.
func (c *DeferredCauset) Reset(eType types.EvalType) {
	switch eType {
	case types.ETInt:
		c.ResizeInt64(0, false)
	case types.ETReal:
		c.ResizeFloat64(0, false)
	case types.ETDecimal:
		c.ResizeDecimal(0, false)
	case types.ETString:
		c.ReserveString(0)
	case types.ETDatetime, types.ETTimestamp:
		c.ResizeTime(0, false)
	case types.ETDuration:
		c.ResizeGoDuration(0, false)
	case types.ETJson:
		c.ReserveJSON(0)
	default:
		panic(fmt.Sprintf("invalid EvalType %v", eType))
	}
}

// reset resets the underlying data of this DeferredCauset but doesn't modify its data type.
func (c *DeferredCauset) reset() {
	c.length = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

// IsNull returns if this event is null.
func (c *DeferredCauset) IsNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

// CopyConstruct copies this DeferredCauset to dst.
// If dst is nil, it creates a new DeferredCauset and returns it.
func (c *DeferredCauset) CopyConstruct(dst *DeferredCauset) *DeferredCauset {
	if dst != nil {
		dst.length = c.length
		dst.nullBitmap = append(dst.nullBitmap[:0], c.nullBitmap...)
		dst.offsets = append(dst.offsets[:0], c.offsets...)
		dst.data = append(dst.data[:0], c.data...)
		dst.elemBuf = append(dst.elemBuf[:0], c.elemBuf...)
		return dst
	}
	newDefCaus := &DeferredCauset{length: c.length}
	newDefCaus.nullBitmap = append(newDefCaus.nullBitmap, c.nullBitmap...)
	newDefCaus.offsets = append(newDefCaus.offsets, c.offsets...)
	newDefCaus.data = append(newDefCaus.data, c.data...)
	newDefCaus.elemBuf = append(newDefCaus.elemBuf, c.elemBuf...)
	return newDefCaus
}

func (c *DeferredCauset) appendNullBitmap(notNull bool) {
	idx := c.length >> 3
	if idx >= len(c.nullBitmap) {
		c.nullBitmap = append(c.nullBitmap, 0)
	}
	if notNull {
		pos := uint(c.length) & 7
		c.nullBitmap[idx] |= byte(1 << pos)
	}
}

// appendMultiSameNullBitmap appends multiple same bit value to `nullBitMap`.
// notNull means not null.
// num means the number of bits that should be appended.
func (c *DeferredCauset) appendMultiSameNullBitmap(notNull bool, num int) {
	numNewBytes := ((c.length + num + 7) >> 3) - len(c.nullBitmap)
	b := byte(0)
	if notNull {
		b = 0xff
	}
	for i := 0; i < numNewBytes; i++ {
		c.nullBitmap = append(c.nullBitmap, b)
	}
	if !notNull {
		return
	}
	// 1. Set all the remaining bits in the last slot of old c.numBitMap to 1.
	numRemainingBits := uint(c.length % 8)
	bitMask := byte(^((1 << numRemainingBits) - 1))
	c.nullBitmap[c.length/8] |= bitMask
	// 2. Set all the redundant bits in the last slot of new c.numBitMap to 0.
	numRedundantBits := uint(len(c.nullBitmap)*8 - c.length - num)
	bitMask = byte(1<<(8-numRedundantBits)) - 1
	c.nullBitmap[len(c.nullBitmap)-1] &= bitMask
}

// AppendNull appends a null value into this DeferredCauset.
func (c *DeferredCauset) AppendNull() {
	c.appendNullBitmap(false)
	if c.isFixed() {
		c.data = append(c.data, c.elemBuf...)
	} else {
		c.offsets = append(c.offsets, c.offsets[c.length])
	}
	c.length++
}

func (c *DeferredCauset) finishAppendFixed() {
	c.data = append(c.data, c.elemBuf...)
	c.appendNullBitmap(true)
	c.length++
}

// AppendInt64 appends an int64 value into this DeferredCauset.
func (c *DeferredCauset) AppendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i
	c.finishAppendFixed()
}

// AppendUint64 appends a uint64 value into this DeferredCauset.
func (c *DeferredCauset) AppendUint64(u uint64) {
	*(*uint64)(unsafe.Pointer(&c.elemBuf[0])) = u
	c.finishAppendFixed()
}

// AppendFloat32 appends a float32 value into this DeferredCauset.
func (c *DeferredCauset) AppendFloat32(f float32) {
	*(*float32)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

// AppendFloat64 appends a float64 value into this DeferredCauset.
func (c *DeferredCauset) AppendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *DeferredCauset) finishAppendVar() {
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int64(len(c.data)))
	c.length++
}

// AppendString appends a string value into this DeferredCauset.
func (c *DeferredCauset) AppendString(str string) {
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

// AppendBytes appends a byte slice into this DeferredCauset.
func (c *DeferredCauset) AppendBytes(b []byte) {
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

// AppendTime appends a time value into this DeferredCauset.
func (c *DeferredCauset) AppendTime(t types.Time) {
	*(*types.Time)(unsafe.Pointer(&c.elemBuf[0])) = t
	c.finishAppendFixed()
}

// AppendEnum appends a Enum value into this DeferredCauset.
func (c *DeferredCauset) AppendEnum(enum types.Enum) {
	c.appendNameValue(enum.Name, enum.Value)
}

const (
	sizeInt64      = int(unsafe.Sizeof(int64(0)))
	sizeUint64     = int(unsafe.Sizeof(uint64(0)))
	sizeFloat32    = int(unsafe.Sizeof(float32(0)))
	sizeFloat64    = int(unsafe.Sizeof(float64(0)))
	sizeMyDecimal  = int(unsafe.Sizeof(types.MyDecimal{}))
	sizeGoDuration = int(unsafe.Sizeof(time.Duration(0)))
	sizeTime       = int(unsafe.Sizeof(types.ZeroTime))
)

var (
	emptyBuf = make([]byte, 4*1024)
)

// resize resizes the defCausumn so that it contains n elements, only valid for fixed-length types.
func (c *DeferredCauset) resize(n, typeSize int, isNull bool) {
	sizeData := n * typeSize
	if cap(c.data) >= sizeData {
		(*reflect.SliceHeader)(unsafe.Pointer(&c.data)).Len = sizeData
	} else {
		c.data = make([]byte, sizeData)
	}
	if !isNull {
		for j := 0; j < sizeData; j += len(emptyBuf) {
			copy(c.data[j:], emptyBuf)
		}
	}

	newNulls := false
	sizeNulls := (n + 7) >> 3
	if cap(c.nullBitmap) >= sizeNulls {
		(*reflect.SliceHeader)(unsafe.Pointer(&c.nullBitmap)).Len = sizeNulls
	} else {
		c.nullBitmap = make([]byte, sizeNulls)
		newNulls = true
	}
	if !isNull || !newNulls {
		var nullVal byte
		if !isNull {
			nullVal = 0xFF
		}
		for i := range c.nullBitmap {
			c.nullBitmap[i] = nullVal
		}
	}

	if cap(c.elemBuf) >= typeSize {
		(*reflect.SliceHeader)(unsafe.Pointer(&c.elemBuf)).Len = typeSize
	} else {
		c.elemBuf = make([]byte, typeSize)
	}

	c.length = n
}

// reserve makes the defCausumn capacity be at least enough to contain n elements.
// this method is only valid for var-length types and estElemSize is the estimated size of this type.
func (c *DeferredCauset) reserve(n, estElemSize int) {
	sizeData := n * estElemSize
	if cap(c.data) >= sizeData {
		c.data = c.data[:0]
	} else {
		c.data = make([]byte, 0, sizeData)
	}

	sizeNulls := (n + 7) >> 3
	if cap(c.nullBitmap) >= sizeNulls {
		c.nullBitmap = c.nullBitmap[:0]
	} else {
		c.nullBitmap = make([]byte, 0, sizeNulls)
	}

	sizeOffs := n + 1
	if cap(c.offsets) >= sizeOffs {
		c.offsets = c.offsets[:1]
	} else {
		c.offsets = make([]int64, 1, sizeOffs)
	}

	c.elemBuf = nil
	c.length = 0
}

// SetNull sets the rowIdx to null.
func (c *DeferredCauset) SetNull(rowIdx int, isNull bool) {
	if isNull {
		c.nullBitmap[rowIdx>>3] &= ^(1 << uint(rowIdx&7))
	} else {
		c.nullBitmap[rowIdx>>3] |= 1 << uint(rowIdx&7)
	}
}

// SetNulls sets rows in [begin, end) to null.
func (c *DeferredCauset) SetNulls(begin, end int, isNull bool) {
	i := ((begin + 7) >> 3) << 3
	for ; begin < i && begin < end; begin++ {
		c.SetNull(begin, isNull)
	}
	var v uint8
	if !isNull {
		v = (1 << 8) - 1
	}
	for ; begin+8 <= end; begin += 8 {
		c.nullBitmap[begin>>3] = v
	}
	for ; begin < end; begin++ {
		c.SetNull(begin, isNull)
	}
}

// nullCount returns the number of nulls in this DeferredCauset.
func (c *DeferredCauset) nullCount() int {
	var cnt, i int
	for ; i+8 <= c.length; i += 8 {
		// 0 is null and 1 is not null
		cnt += 8 - bits.OnesCount8(c.nullBitmap[i>>3])
	}
	for ; i < c.length; i++ {
		if c.IsNull(i) {
			cnt++
		}
	}
	return cnt
}

// ResizeInt64 resizes the defCausumn so that it contains n int64 elements.
func (c *DeferredCauset) ResizeInt64(n int, isNull bool) {
	c.resize(n, sizeInt64, isNull)
}

// ResizeUint64 resizes the defCausumn so that it contains n uint64 elements.
func (c *DeferredCauset) ResizeUint64(n int, isNull bool) {
	c.resize(n, sizeUint64, isNull)
}

// ResizeFloat32 resizes the defCausumn so that it contains n float32 elements.
func (c *DeferredCauset) ResizeFloat32(n int, isNull bool) {
	c.resize(n, sizeFloat32, isNull)
}

// ResizeFloat64 resizes the defCausumn so that it contains n float64 elements.
func (c *DeferredCauset) ResizeFloat64(n int, isNull bool) {
	c.resize(n, sizeFloat64, isNull)
}

// ResizeDecimal resizes the defCausumn so that it contains n decimal elements.
func (c *DeferredCauset) ResizeDecimal(n int, isNull bool) {
	c.resize(n, sizeMyDecimal, isNull)
}

// ResizeGoDuration resizes the defCausumn so that it contains n duration elements.
func (c *DeferredCauset) ResizeGoDuration(n int, isNull bool) {
	c.resize(n, sizeGoDuration, isNull)
}

// ResizeTime resizes the defCausumn so that it contains n Time elements.
func (c *DeferredCauset) ResizeTime(n int, isNull bool) {
	c.resize(n, sizeTime, isNull)
}

// ReserveString changes the defCausumn capacity to causetstore n string elements and set the length to zero.
func (c *DeferredCauset) ReserveString(n int) {
	c.reserve(n, 8)
}

// ReserveBytes changes the defCausumn capacity to causetstore n bytes elements and set the length to zero.
func (c *DeferredCauset) ReserveBytes(n int) {
	c.reserve(n, 8)
}

// ReserveJSON changes the defCausumn capacity to causetstore n JSON elements and set the length to zero.
func (c *DeferredCauset) ReserveJSON(n int) {
	c.reserve(n, 8)
}

// ReserveSet changes the defCausumn capacity to causetstore n set elements and set the length to zero.
func (c *DeferredCauset) ReserveSet(n int) {
	c.reserve(n, 8)
}

// ReserveEnum changes the defCausumn capacity to causetstore n enum elements and set the length to zero.
func (c *DeferredCauset) ReserveEnum(n int) {
	c.reserve(n, 8)
}

func (c *DeferredCauset) castSliceHeader(header *reflect.SliceHeader, typeSize int) {
	header.Data = (*reflect.SliceHeader)(unsafe.Pointer(&c.data)).Data
	header.Len = c.length
	header.Cap = cap(c.data) / typeSize
}

// Int64s returns an int64 slice stored in this DeferredCauset.
func (c *DeferredCauset) Int64s() []int64 {
	var res []int64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeInt64)
	return res
}

// Uint64s returns a uint64 slice stored in this DeferredCauset.
func (c *DeferredCauset) Uint64s() []uint64 {
	var res []uint64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeUint64)
	return res
}

// Float32s returns a float32 slice stored in this DeferredCauset.
func (c *DeferredCauset) Float32s() []float32 {
	var res []float32
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeFloat32)
	return res
}

// Float64s returns a float64 slice stored in this DeferredCauset.
func (c *DeferredCauset) Float64s() []float64 {
	var res []float64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeFloat64)
	return res
}

// GoDurations returns a Golang time.Duration slice stored in this DeferredCauset.
// Different from the Row.GetDuration method, the argument Fsp is ignored, so the user should handle it outside.
func (c *DeferredCauset) GoDurations() []time.Duration {
	var res []time.Duration
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeGoDuration)
	return res
}

// Decimals returns a MyDecimal slice stored in this DeferredCauset.
func (c *DeferredCauset) Decimals() []types.MyDecimal {
	var res []types.MyDecimal
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeMyDecimal)
	return res
}

// Times returns a Time slice stored in this DeferredCauset.
func (c *DeferredCauset) Times() []types.Time {
	var res []types.Time
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeTime)
	return res
}

// GetInt64 returns the int64 in the specific event.
func (c *DeferredCauset) GetInt64(rowID int) int64 {
	return *(*int64)(unsafe.Pointer(&c.data[rowID*8]))
}

// GetUint64 returns the uint64 in the specific event.
func (c *DeferredCauset) GetUint64(rowID int) uint64 {
	return *(*uint64)(unsafe.Pointer(&c.data[rowID*8]))
}

// GetFloat32 returns the float32 in the specific event.
func (c *DeferredCauset) GetFloat32(rowID int) float32 {
	return *(*float32)(unsafe.Pointer(&c.data[rowID*4]))
}

// GetFloat64 returns the float64 in the specific event.
func (c *DeferredCauset) GetFloat64(rowID int) float64 {
	return *(*float64)(unsafe.Pointer(&c.data[rowID*8]))
}

// GetDecimal returns the decimal in the specific event.
func (c *DeferredCauset) GetDecimal(rowID int) *types.MyDecimal {
	return (*types.MyDecimal)(unsafe.Pointer(&c.data[rowID*types.MyDecimalStructSize]))
}

// GetString returns the string in the specific event.
func (c *DeferredCauset) GetString(rowID int) string {
	return string(replog.String(c.data[c.offsets[rowID]:c.offsets[rowID+1]]))
}

// GetJSON returns the JSON in the specific event.
func (c *DeferredCauset) GetJSON(rowID int) json.BinaryJSON {
	start := c.offsets[rowID]
	return json.BinaryJSON{TypeCode: c.data[start], Value: c.data[start+1 : c.offsets[rowID+1]]}
}

// GetBytes returns the byte slice in the specific event.
func (c *DeferredCauset) GetBytes(rowID int) []byte {
	return c.data[c.offsets[rowID]:c.offsets[rowID+1]]
}

// GetEnum returns the Enum in the specific event.
func (c *DeferredCauset) GetEnum(rowID int) types.Enum {
	name, val := c.getNameValue(rowID)
	return types.Enum{Name: name, Value: val}
}

// GetSet returns the Set in the specific event.
func (c *DeferredCauset) GetSet(rowID int) types.Set {
	name, val := c.getNameValue(rowID)
	return types.Set{Name: name, Value: val}
}

// GetTime returns the Time in the specific event.
func (c *DeferredCauset) GetTime(rowID int) types.Time {
	return *(*types.Time)(unsafe.Pointer(&c.data[rowID*sizeTime]))
}

// GetDuration returns the Duration in the specific event.
func (c *DeferredCauset) GetDuration(rowID int, fillFsp int) types.Duration {
	dur := *(*int64)(unsafe.Pointer(&c.data[rowID*8]))
	return types.Duration{Duration: time.Duration(dur), Fsp: int8(fillFsp)}
}

func (c *DeferredCauset) getNameValue(rowID int) (string, uint64) {
	start, end := c.offsets[rowID], c.offsets[rowID+1]
	if start == end {
		return "", 0
	}
	var val uint64
	copy((*[8]byte)(unsafe.Pointer(&val))[:], c.data[start:])
	return string(replog.String(c.data[start+8 : end])), val
}

// GetRaw returns the underlying raw bytes in the specific event.
func (c *DeferredCauset) GetRaw(rowID int) []byte {
	var data []byte
	if c.isFixed() {
		elemLen := len(c.elemBuf)
		data = c.data[rowID*elemLen : rowID*elemLen+elemLen]
	} else {
		data = c.data[c.offsets[rowID]:c.offsets[rowID+1]]
	}
	return data
}

// SetRaw sets the raw bytes for the rowIdx-th element.
// NOTE: Two conditions must be satisfied before calling this function:
// 1. The defCausumn should be stored with variable-length elements.
// 2. The length of the new element should be exactly the same as the old one.
func (c *DeferredCauset) SetRaw(rowID int, bs []byte) {
	copy(c.data[c.offsets[rowID]:c.offsets[rowID+1]], bs)
}

// reconstruct reconstructs this DeferredCauset by removing all filtered rows in it according to sel.
func (c *DeferredCauset) reconstruct(sel []int) {
	if sel == nil {
		return
	}
	if c.isFixed() {
		elemLen := len(c.elemBuf)
		for dst, src := range sel {
			idx := dst >> 3
			pos := uint16(dst & 7)
			if c.IsNull(src) {
				c.nullBitmap[idx] &= ^byte(1 << pos)
			} else {
				copy(c.data[dst*elemLen:dst*elemLen+elemLen], c.data[src*elemLen:src*elemLen+elemLen])
				c.nullBitmap[idx] |= byte(1 << pos)
			}
		}
		c.data = c.data[:len(sel)*elemLen]
	} else {
		tail := 0
		for dst, src := range sel {
			idx := dst >> 3
			pos := uint(dst & 7)
			if c.IsNull(src) {
				c.nullBitmap[idx] &= ^byte(1 << pos)
				c.offsets[dst+1] = int64(tail)
			} else {
				start, end := c.offsets[src], c.offsets[src+1]
				copy(c.data[tail:], c.data[start:end])
				tail += int(end - start)
				c.offsets[dst+1] = int64(tail)
				c.nullBitmap[idx] |= byte(1 << pos)
			}
		}
		c.data = c.data[:tail]
		c.offsets = c.offsets[:len(sel)+1]
	}
	c.length = len(sel)

	// clean nullBitmap
	c.nullBitmap = c.nullBitmap[:(len(sel)+7)>>3]
	idx := len(sel) >> 3
	if idx < len(c.nullBitmap) {
		pos := uint16(len(sel) & 7)
		c.nullBitmap[idx] &= byte((1 << pos) - 1)
	}
}

// CopyReconstruct copies this DeferredCauset to dst and removes unselected rows.
// If dst is nil, it creates a new DeferredCauset and returns it.
func (c *DeferredCauset) CopyReconstruct(sel []int, dst *DeferredCauset) *DeferredCauset {
	if sel == nil {
		return c.CopyConstruct(dst)
	}

	selLength := len(sel)
	if selLength == c.length {
		// The variable 'ascend' is used to check if the sel array is in ascending order
		ascend := true
		for i := 1; i < selLength; i++ {
			if sel[i] < sel[i-1] {
				ascend = false
				break
			}
		}
		if ascend {
			return c.CopyConstruct(dst)
		}
	}

	if dst == nil {
		dst = newDeferredCauset(c.typeSize(), len(sel))
	} else {
		dst.reset()
	}

	if c.isFixed() {
		elemLen := len(c.elemBuf)
		dst.elemBuf = make([]byte, elemLen)
		for _, i := range sel {
			dst.appendNullBitmap(!c.IsNull(i))
			dst.data = append(dst.data, c.data[i*elemLen:i*elemLen+elemLen]...)
			dst.length++
		}
	} else {
		dst.elemBuf = nil
		if len(dst.offsets) == 0 {
			dst.offsets = append(dst.offsets, 0)
		}
		for _, i := range sel {
			dst.appendNullBitmap(!c.IsNull(i))
			start, end := c.offsets[i], c.offsets[i+1]
			dst.data = append(dst.data, c.data[start:end]...)
			dst.offsets = append(dst.offsets, int64(len(dst.data)))
			dst.length++
		}
	}
	return dst
}

// MergeNulls merges these defCausumns' null bitmaps.
// For a event, if any defCausumn of it is null, the result is null.
// It works like: if defCaus1.IsNull || defCaus2.IsNull || defCaus3.IsNull.
// The caller should ensure that all these defCausumns have the same
// length, and data stored in the result defCausumn is fixed-length type.
func (c *DeferredCauset) MergeNulls(defcaus ...*DeferredCauset) {
	if !c.isFixed() {
		panic("result defCausumn should be fixed-length type")
	}
	for _, defCaus := range defcaus {
		if c.length != defCaus.length {
			panic(fmt.Sprintf("should ensure all defCausumns have the same length, expect %v, but got %v", c.length, defCaus.length))
		}
	}
	for _, defCaus := range defcaus {
		for i := range c.nullBitmap {
			// bit 0 is null, 1 is not null, so do AND operations here.
			c.nullBitmap[i] &= defCaus.nullBitmap[i]
		}
	}
}
