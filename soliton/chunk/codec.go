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
	"reflect"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
)

// Codec is used to:
// 1. encode a Chunk to a byte slice.
// 2. decode a Chunk from a byte slice.
type Codec struct {
	// defCausTypes is used to check whether a DeferredCauset is fixed sized and what the
	// fixed size for every element.
	// NOTE: It's only used for decoding.
	defCausTypes []*types.FieldType
}

// NewCodec creates a new Codec object for encode or decode a Chunk.
func NewCodec(defCausTypes []*types.FieldType) *Codec {
	return &Codec{defCausTypes}
}

// Encode encodes a Chunk to a byte slice.
func (c *Codec) Encode(chk *Chunk) []byte {
	buffer := make([]byte, 0, chk.MemoryUsage())
	for _, defCaus := range chk.defCausumns {
		buffer = c.encodeDeferredCauset(buffer, defCaus)
	}
	return buffer
}

func (c *Codec) encodeDeferredCauset(buffer []byte, defCaus *DeferredCauset) []byte {
	var lenBuffer [4]byte
	// encode length.
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(defCaus.length))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullCount.
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(defCaus.nullCount()))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullBitmap.
	if defCaus.nullCount() > 0 {
		numNullBitmapBytes := (defCaus.length + 7) / 8
		buffer = append(buffer, defCaus.nullBitmap[:numNullBitmapBytes]...)
	}

	// encode offsets.
	if !defCaus.isFixed() {
		numOffsetBytes := (defCaus.length + 1) * 8
		offsetBytes := i64SliceToBytes(defCaus.offsets)
		buffer = append(buffer, offsetBytes[:numOffsetBytes]...)
	}

	// encode data.
	buffer = append(buffer, defCaus.data...)
	return buffer
}

func i64SliceToBytes(i64s []int64) (b []byte) {
	if len(i64s) == 0 {
		return nil
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(i64s) * 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&i64s[0]))
	return b
}

// Decode decodes a Chunk from a byte slice, return the remained unused bytes.
func (c *Codec) Decode(buffer []byte) (*Chunk, []byte) {
	chk := &Chunk{}
	for ordinal := 0; len(buffer) > 0; ordinal++ {
		defCaus := &DeferredCauset{}
		buffer = c.decodeDeferredCauset(buffer, defCaus, ordinal)
		chk.defCausumns = append(chk.defCausumns, defCaus)
	}
	return chk, buffer
}

// DecodeToChunk decodes a Chunk from a byte slice, return the remained unused bytes.
func (c *Codec) DecodeToChunk(buffer []byte, chk *Chunk) (remained []byte) {
	for i := 0; i < len(chk.defCausumns); i++ {
		buffer = c.decodeDeferredCauset(buffer, chk.defCausumns[i], i)
	}
	return buffer
}

// decodeDeferredCauset decodes a DeferredCauset from a byte slice, return the remained unused bytes.
func (c *Codec) decodeDeferredCauset(buffer []byte, defCaus *DeferredCauset, ordinal int) (remained []byte) {
	// Todo(Shenghui Wu): Optimize all data is null.
	// decode length.
	defCaus.length = int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullCount.
	nullCount := int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullBitmap.
	if nullCount > 0 {
		numNullBitmapBytes := (defCaus.length + 7) / 8
		defCaus.nullBitmap = buffer[:numNullBitmapBytes:numNullBitmapBytes]
		buffer = buffer[numNullBitmapBytes:]
	} else {
		c.setAllNotNull(defCaus)
	}

	// decode offsets.
	numFixedBytes := getFixedLen(c.defCausTypes[ordinal])
	numDataBytes := int64(numFixedBytes * defCaus.length)
	if numFixedBytes == -1 {
		numOffsetBytes := (defCaus.length + 1) * 8
		defCaus.offsets = bytesToI64Slice(buffer[:numOffsetBytes:numOffsetBytes])
		buffer = buffer[numOffsetBytes:]
		numDataBytes = defCaus.offsets[defCaus.length]
	} else if cap(defCaus.elemBuf) < numFixedBytes {
		defCaus.elemBuf = make([]byte, numFixedBytes)
	}

	// decode data.
	defCaus.data = buffer[:numDataBytes:numDataBytes]
	return buffer[numDataBytes:]
}

var allNotNullBitmap [128]byte

func (c *Codec) setAllNotNull(defCaus *DeferredCauset) {
	numNullBitmapBytes := (defCaus.length + 7) / 8
	defCaus.nullBitmap = defCaus.nullBitmap[:0]
	for i := 0; i < numNullBitmapBytes; {
		numAppendBytes := mathutil.Min(numNullBitmapBytes-i, cap(allNotNullBitmap))
		defCaus.nullBitmap = append(defCaus.nullBitmap, allNotNullBitmap[:numAppendBytes]...)
		i += numAppendBytes
	}
}

func bytesToI64Slice(b []byte) (i64s []int64) {
	if len(b) == 0 {
		return nil
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&i64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return i64s
}

// varElemLen indicates this DeferredCauset is a variable length DeferredCauset.
const varElemLen = -1

func getFixedLen(defCausType *types.FieldType) int {
	switch defCausType.Tp {
	case allegrosql.TypeFloat:
		return 4
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong,
		allegrosql.TypeLonglong, allegrosql.TypeDouble, allegrosql.TypeYear, allegrosql.TypeDuration:
		return 8
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		return sizeTime
	case allegrosql.TypeNewDecimal:
		return types.MyDecimalStructSize
	default:
		return varElemLen
	}
}

// GetFixedLen get the memory size of a fixed-length type.
// if defCausType is not fixed-length, it returns varElemLen, aka -1.
func GetFixedLen(defCausType *types.FieldType) int {
	return getFixedLen(defCausType)
}

// EstimateTypeWidth estimates the average width of values of the type.
// This is used by the planner, which doesn't require absolutely correct results;
// it's OK (and expected) to guess if we don't know for sure.
//
// mostly study from https://github.com/postgres/postgres/blob/REL_12_STABLE/src/backend/utils/cache/lsyscache.c#L2356
func EstimateTypeWidth(defCausType *types.FieldType) int {
	defCausLen := getFixedLen(defCausType)
	// Easy if it's a fixed-width type
	if defCausLen != varElemLen {
		return defCausLen
	}

	defCausLen = defCausType.Flen
	if defCausLen > 0 {
		if defCausLen <= 32 {
			return defCausLen
		}
		if defCausLen < 1000 {
			return 32 + (defCausLen-32)/2 // assume 50%
		}
		/*
		 * Beyond 1000, assume we're looking at something like
		 * "varchar(10000)" where the limit isn't actually reached often, and
		 * use a fixed estimate.
		 */
		return 32 + (1000-32)/2
	}
	// Oops, we have no idea ... wild guess time.
	return 32
}

func init() {
	for i := 0; i < 128; i++ {
		allNotNullBitmap[i] = 0xFF
	}
}

// Decoder decodes the data returned from the interlock and stores the result in Chunk.
// How Decoder works:
// 1. Initialization phase: Decode a whole input byte slice to Decoder.intermChk(intermediate chunk) using Codec.Decode.
//    intermChk is introduced to simplify the implementation of decode phase. This phase uses pointer operations with
//    less CPU and memory cost.
// 2. Decode phase:
//    2.1 Set the number of rows to be decoded to a value that is a multiple of 8 and greater than
//        `chk.RequiredRows() - chk.NumRows()`. This reduces the overhead of copying the srcDefCaus.nullBitMap into
//        destDefCaus.nullBitMap.
//    2.2 Append srcDefCaus.offsets to destDefCaus.offsets when the elements is of var-length type. And further adjust the
//        offsets according to descDefCaus.offsets[destDefCaus.length]-srcDefCaus.offsets[0].
//    2.3 Append srcDefCaus.nullBitMap to destDefCaus.nullBitMap.
// 3. Go to step 1 when the input byte slice is consumed.
type Decoder struct {
	intermChk    *Chunk
	codec        *Codec
	remainedRows int
}

// NewDecoder creates a new Decoder object for decode a Chunk.
func NewDecoder(chk *Chunk, defCausTypes []*types.FieldType) *Decoder {
	return &Decoder{intermChk: chk, codec: NewCodec(defCausTypes), remainedRows: 0}
}

// Decode decodes multiple rows of Decoder.intermChk and stores the result in chk.
func (c *Decoder) Decode(chk *Chunk) {
	requiredRows := chk.RequiredRows() - chk.NumRows()
	// Set the requiredRows to a multiple of 8.
	requiredRows = (requiredRows + 7) >> 3 << 3
	if requiredRows > c.remainedRows {
		requiredRows = c.remainedRows
	}
	for i := 0; i < chk.NumDefCauss(); i++ {
		c.decodeDeferredCauset(chk, i, requiredRows)
	}
	c.remainedRows -= requiredRows
}

// Reset decodes data and causetstore the result in Decoder.intermChk. This decode phase uses pointer operations with less
// CPU and memory costs.
func (c *Decoder) Reset(data []byte) {
	c.codec.DecodeToChunk(data, c.intermChk)
	c.remainedRows = c.intermChk.NumRows()
}

// IsFinished indicates whether Decoder.intermChk has been dried up.
func (c *Decoder) IsFinished() bool {
	return c.remainedRows == 0
}

// RemainedRows indicates Decoder.intermChk has remained rows.
func (c *Decoder) RemainedRows() int {
	return c.remainedRows
}

// ReuseIntermChk swaps `Decoder.intermChk` with `chk` directly when `Decoder.intermChk.NumRows()` is no less
// than `chk.requiredRows * factor` where `factor` is 0.8 now. This can avoid the overhead of appending the
// data from `Decoder.intermChk` to `chk`. Moreover, the defCausumn.offsets needs to be further adjusted
// according to defCausumn.offset[0].
func (c *Decoder) ReuseIntermChk(chk *Chunk) {
	for i, defCaus := range c.intermChk.defCausumns {
		defCaus.length = c.remainedRows
		elemLen := getFixedLen(c.codec.defCausTypes[i])
		if elemLen == varElemLen {
			// For var-length types, we need to adjust the offsets before reuse.
			if deltaOffset := defCaus.offsets[0]; deltaOffset != 0 {
				for j := 0; j < len(defCaus.offsets); j++ {
					defCaus.offsets[j] -= deltaOffset
				}
			}
		}
	}
	chk.SwapDeferredCausets(c.intermChk)
	c.remainedRows = 0
}

func (c *Decoder) decodeDeferredCauset(chk *Chunk, ordinal int, requiredRows int) {
	elemLen := getFixedLen(c.codec.defCausTypes[ordinal])
	numDataBytes := int64(elemLen * requiredRows)
	srcDefCaus := c.intermChk.defCausumns[ordinal]
	destDefCaus := chk.defCausumns[ordinal]

	if elemLen == varElemLen {
		// For var-length types, we need to adjust the offsets after appending to destDefCaus.
		numDataBytes = srcDefCaus.offsets[requiredRows] - srcDefCaus.offsets[0]
		deltaOffset := destDefCaus.offsets[destDefCaus.length] - srcDefCaus.offsets[0]
		destDefCaus.offsets = append(destDefCaus.offsets, srcDefCaus.offsets[1:requiredRows+1]...)
		for i := destDefCaus.length + 1; i <= destDefCaus.length+requiredRows; i++ {
			destDefCaus.offsets[i] = destDefCaus.offsets[i] + deltaOffset
		}
		srcDefCaus.offsets = srcDefCaus.offsets[requiredRows:]
	}

	numNullBitmapBytes := (requiredRows + 7) >> 3
	if destDefCaus.length%8 == 0 {
		destDefCaus.nullBitmap = append(destDefCaus.nullBitmap, srcDefCaus.nullBitmap[:numNullBitmapBytes]...)
	} else {
		destDefCaus.appendMultiSameNullBitmap(false, requiredRows)
		bitMapLen := len(destDefCaus.nullBitmap)
		// bitOffset indicates the number of valid bits in destDefCaus.nullBitmap's last byte.
		bitOffset := destDefCaus.length % 8
		startIdx := (destDefCaus.length - 1) >> 3
		for i := 0; i < numNullBitmapBytes; i++ {
			destDefCaus.nullBitmap[startIdx+i] |= srcDefCaus.nullBitmap[i] << bitOffset
			// The high order 8-bitOffset bits in `srcDefCaus.nullBitmap[i]` should be appended to the low order of the next slot.
			if startIdx+i+1 < bitMapLen {
				destDefCaus.nullBitmap[startIdx+i+1] |= srcDefCaus.nullBitmap[i] >> (8 - bitOffset)
			}
		}
	}
	// Set all the redundant bits in the last slot of destDefCaus.nullBitmap to 0.
	numRedundantBits := uint(len(destDefCaus.nullBitmap)*8 - destDefCaus.length - requiredRows)
	bitMask := byte(1<<(8-numRedundantBits)) - 1
	destDefCaus.nullBitmap[len(destDefCaus.nullBitmap)-1] &= bitMask

	srcDefCaus.nullBitmap = srcDefCaus.nullBitmap[numNullBitmapBytes:]
	destDefCaus.length += requiredRows

	destDefCaus.data = append(destDefCaus.data, srcDefCaus.data[:numDataBytes]...)
	srcDefCaus.data = srcDefCaus.data[numDataBytes:]
}
