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
	"reflect"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

var msgErrSelNotNil = "The selection vector of Chunk is not nil. Please file a bug to the MilevaDB Team"

// Chunk stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/format/DeferredCausetar.html#physical-memory-layout
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
type Chunk struct {
	// sel indicates which rows are selected.
	// If it is nil, all rows are selected.
	sel []int

	defCausumns []*DeferredCauset
	// numVirtualRows indicates the number of virtual rows, which have zero DeferredCauset.
	// It is used only when this Chunk doesn't hold any data, i.e. "len(defCausumns)==0".
	numVirtualRows int
	// capacity indicates the max number of rows this chunk can hold.
	// TODO: replace all usages of capacity to requiredRows and remove this field
	capacity int

	// requiredRows indicates how many rows the parent interlock want.
	requiredRows int
}

// Capacity constants.
const (
	InitialCapacity = 32
	ZeroCapacity    = 0
)

// NewChunkWithCapacity creates a new chunk with field types and capacity.
func NewChunkWithCapacity(fields []*types.FieldType, cap int) *Chunk {
	return New(fields, cap, cap) //FIXME: in following PR.
}

// New creates a new chunk.
//  cap: the limit for the max number of rows.
//  maxChunkSize: the max limit for the number of rows.
func New(fields []*types.FieldType, cap, maxChunkSize int) *Chunk {
	chk := &Chunk{
		defCausumns:  make([]*DeferredCauset, 0, len(fields)),
		capacity: mathutil.Min(cap, maxChunkSize),
		// set the default value of requiredRows to maxChunkSize to let chk.IsFull() behave
		// like how we judge whether a chunk is full now, then the memex
		// "chk.NumRows() < maxChunkSize"
		// equals to "!chk.IsFull()".
		requiredRows: maxChunkSize,
	}

	for _, f := range fields {
		chk.defCausumns = append(chk.defCausumns, NewDeferredCauset(f, chk.capacity))
	}

	return chk
}

// renewWithCapacity creates a new Chunk based on an existing Chunk with capacity. The newly
// created Chunk has the same data schemaReplicant with the old Chunk.
func renewWithCapacity(chk *Chunk, cap, maxChunkSize int) *Chunk {
	newChk := new(Chunk)
	if chk.defCausumns == nil {
		return newChk
	}
	newChk.defCausumns = renewDeferredCausets(chk.defCausumns, cap)
	newChk.numVirtualRows = 0
	newChk.capacity = cap
	newChk.requiredRows = maxChunkSize
	return newChk
}

// Renew creates a new Chunk based on an existing Chunk. The newly created Chunk
// has the same data schemaReplicant with the old Chunk. The capacity of the new Chunk
// might be doubled based on the capacity of the old Chunk and the maxChunkSize.
//  chk: old chunk(often used in previous call).
//  maxChunkSize: the limit for the max number of rows.
func Renew(chk *Chunk, maxChunkSize int) *Chunk {
	newCap := reCalcCapacity(chk, maxChunkSize)
	return renewWithCapacity(chk, newCap, maxChunkSize)
}

// renewDeferredCausets creates the defCausumns of a Chunk. The capacity of the newly
// created defCausumns is equal to cap.
func renewDeferredCausets(oldDefCaus []*DeferredCauset, cap int) []*DeferredCauset {
	defCausumns := make([]*DeferredCauset, 0, len(oldDefCaus))
	for _, defCaus := range oldDefCaus {
		defCausumns = append(defCausumns, newDeferredCauset(defCaus.typeSize(), cap))
	}
	return defCausumns
}

// renewEmpty creates a new Chunk based on an existing Chunk
// but keep defCausumns empty.
func renewEmpty(chk *Chunk) *Chunk {
	newChk := &Chunk{
		defCausumns:        nil,
		numVirtualRows: chk.numVirtualRows,
		capacity:       chk.capacity,
		requiredRows:   chk.requiredRows,
	}
	if chk.sel != nil {
		newChk.sel = make([]int, len(chk.sel))
		copy(newChk.sel, chk.sel)
	}
	return newChk
}

// MemoryUsage returns the total memory usage of a Chunk in B.
// We ignore the size of DeferredCauset.length and DeferredCauset.nullCount
// since they have little effect of the total memory usage.
func (c *Chunk) MemoryUsage() (sum int64) {
	for _, defCaus := range c.defCausumns {
		curDefCausMemUsage := int64(unsafe.Sizeof(*defCaus)) + int64(cap(defCaus.nullBitmap)) + int64(cap(defCaus.offsets)*4) + int64(cap(defCaus.data)) + int64(cap(defCaus.elemBuf))
		sum += curDefCausMemUsage
	}
	return
}

// newFixedLenDeferredCauset creates a fixed length DeferredCauset with elemLen and initial data capacity.
func newFixedLenDeferredCauset(elemLen, cap int) *DeferredCauset {
	return &DeferredCauset{
		elemBuf:    make([]byte, elemLen),
		data:       make([]byte, 0, cap*elemLen),
		nullBitmap: make([]byte, 0, (cap+7)>>3),
	}
}

// newVarLenDeferredCauset creates a variable length DeferredCauset with initial data capacity.
func newVarLenDeferredCauset(cap int, old *DeferredCauset) *DeferredCauset {
	estimatedElemLen := 8
	// For varLenDeferredCauset (e.g. varchar), the accurate length of an element is unknown.
	// Therefore, in the first interlock.Next we use an experience value -- 8 (so it may make runtime.growslice)
	// but in the following Next call we estimate the length as AVG x 1.125 elemLen of the previous call.
	if old != nil && old.length != 0 {
		estimatedElemLen = (len(old.data) + len(old.data)/8) / old.length
	}
	return &DeferredCauset{
		offsets:    make([]int64, 1, cap+1),
		data:       make([]byte, 0, cap*estimatedElemLen),
		nullBitmap: make([]byte, 0, (cap+7)>>3),
	}
}

// RequiredRows returns how many rows is considered full.
func (c *Chunk) RequiredRows() int {
	return c.requiredRows
}

// SetRequiredRows sets the number of required rows.
func (c *Chunk) SetRequiredRows(requiredRows, maxChunkSize int) *Chunk {
	if requiredRows <= 0 || requiredRows > maxChunkSize {
		requiredRows = maxChunkSize
	}
	c.requiredRows = requiredRows
	return c
}

// IsFull returns if this chunk is considered full.
func (c *Chunk) IsFull() bool {
	return c.NumRows() >= c.requiredRows
}

// Prune creates a new Chunk according to `c` and prunes the defCausumns
// whose index is not in `usedDefCausIdxs`
func (c *Chunk) Prune(usedDefCausIdxs []int) *Chunk {
	chk := renewEmpty(c)
	chk.defCausumns = make([]*DeferredCauset, len(usedDefCausIdxs))
	for i, idx := range usedDefCausIdxs {
		chk.defCausumns[i] = c.defCausumns[idx]
	}
	return chk
}

// MakeRef makes DeferredCauset in "dstDefCausIdx" reference to DeferredCauset in "srcDefCausIdx".
func (c *Chunk) MakeRef(srcDefCausIdx, dstDefCausIdx int) {
	c.defCausumns[dstDefCausIdx] = c.defCausumns[srcDefCausIdx]
}

// MakeRefTo copies defCausumns `src.defCausumns[srcDefCausIdx]` to `c.defCausumns[dstDefCausIdx]`.
func (c *Chunk) MakeRefTo(dstDefCausIdx int, src *Chunk, srcDefCausIdx int) error {
	if c.sel != nil || src.sel != nil {
		return errors.New(msgErrSelNotNil)
	}
	c.defCausumns[dstDefCausIdx] = src.defCausumns[srcDefCausIdx]
	return nil
}

// SwapDeferredCauset swaps DeferredCauset "c.defCausumns[defCausIdx]" with DeferredCauset
// "other.defCausumns[otherIdx]". If there exists defCausumns refer to the DeferredCauset to be
// swapped, we need to re-build the reference.
func (c *Chunk) SwapDeferredCauset(defCausIdx int, other *Chunk, otherIdx int) error {
	if c.sel != nil || other.sel != nil {
		return errors.New(msgErrSelNotNil)
	}
	// Find the leftmost DeferredCauset of the reference which is the actual DeferredCauset to
	// be swapped.
	for i := 0; i < defCausIdx; i++ {
		if c.defCausumns[i] == c.defCausumns[defCausIdx] {
			defCausIdx = i
		}
	}
	for i := 0; i < otherIdx; i++ {
		if other.defCausumns[i] == other.defCausumns[otherIdx] {
			otherIdx = i
		}
	}

	// Find the defCausumns which refer to the actual DeferredCauset to be swapped.
	refDefCaussIdx := make([]int, 0, len(c.defCausumns)-defCausIdx)
	for i := defCausIdx; i < len(c.defCausumns); i++ {
		if c.defCausumns[i] == c.defCausumns[defCausIdx] {
			refDefCaussIdx = append(refDefCaussIdx, i)
		}
	}
	refDefCaussIdx4Other := make([]int, 0, len(other.defCausumns)-otherIdx)
	for i := otherIdx; i < len(other.defCausumns); i++ {
		if other.defCausumns[i] == other.defCausumns[otherIdx] {
			refDefCaussIdx4Other = append(refDefCaussIdx4Other, i)
		}
	}

	// Swap defCausumns from two chunks.
	c.defCausumns[defCausIdx], other.defCausumns[otherIdx] = other.defCausumns[otherIdx], c.defCausumns[defCausIdx]

	// Rebuild the reference.
	for _, i := range refDefCaussIdx {
		c.MakeRef(defCausIdx, i)
	}
	for _, i := range refDefCaussIdx4Other {
		other.MakeRef(otherIdx, i)
	}
	return nil
}

// SwapDeferredCausets swaps defCausumns with another Chunk.
func (c *Chunk) SwapDeferredCausets(other *Chunk) {
	c.sel, other.sel = other.sel, c.sel
	c.defCausumns, other.defCausumns = other.defCausumns, c.defCausumns
	c.numVirtualRows, other.numVirtualRows = other.numVirtualRows, c.numVirtualRows
}

// SetNumVirtualRows sets the virtual event number for a Chunk.
// It should only be used when there exists no DeferredCauset in the Chunk.
func (c *Chunk) SetNumVirtualRows(numVirtualRows int) {
	c.numVirtualRows = numVirtualRows
}

// Reset resets the chunk, so the memory it allocated can be reused.
// Make sure all the data in the chunk is not used anymore before you reuse this chunk.
func (c *Chunk) Reset() {
	c.sel = nil
	if c.defCausumns == nil {
		return
	}
	for _, defCaus := range c.defCausumns {
		defCaus.reset()
	}
	c.numVirtualRows = 0
}

// CopyConstruct creates a new chunk and copies this chunk's data into it.
func (c *Chunk) CopyConstruct() *Chunk {
	newChk := renewEmpty(c)
	newChk.defCausumns = make([]*DeferredCauset, len(c.defCausumns))
	for i := range c.defCausumns {
		newChk.defCausumns[i] = c.defCausumns[i].CopyConstruct(nil)
	}
	return newChk
}

// GrowAndReset resets the Chunk and doubles the capacity of the Chunk.
// The doubled capacity should not be larger than maxChunkSize.
// TODO: this method will be used in following PR.
func (c *Chunk) GrowAndReset(maxChunkSize int) {
	c.sel = nil
	if c.defCausumns == nil {
		return
	}
	newCap := reCalcCapacity(c, maxChunkSize)
	if newCap <= c.capacity {
		c.Reset()
		return
	}
	c.capacity = newCap
	c.defCausumns = renewDeferredCausets(c.defCausumns, newCap)
	c.numVirtualRows = 0
	c.requiredRows = maxChunkSize
}

// reCalcCapacity calculates the capacity for another Chunk based on the current
// Chunk. The new capacity is doubled only when the current Chunk is full.
func reCalcCapacity(c *Chunk, maxChunkSize int) int {
	if c.NumRows() < c.capacity {
		return c.capacity
	}
	return mathutil.Min(c.capacity*2, maxChunkSize)
}

// Capacity returns the capacity of the Chunk.
func (c *Chunk) Capacity() int {
	return c.capacity
}

// NumDefCauss returns the number of defCausumns in the chunk.
func (c *Chunk) NumDefCauss() int {
	return len(c.defCausumns)
}

// NumRows returns the number of rows in the chunk.
func (c *Chunk) NumRows() int {
	if c.sel != nil {
		return len(c.sel)
	}
	if c.NumDefCauss() == 0 {
		return c.numVirtualRows
	}
	return c.defCausumns[0].length
}

// GetRow gets the Row in the chunk with the event index.
func (c *Chunk) GetRow(idx int) Row {
	if c.sel != nil {
		// mapping the logical RowIdx to the actual physical RowIdx;
		// for example, if the Sel is [1, 5, 6], then
		//	logical 0 -> physical 1,
		//	logical 1 -> physical 5,
		//	logical 2 -> physical 6.
		// Then when we iterate this Chunk according to Row, only selected rows will be
		// accessed while all filtered rows will be ignored.
		return Row{c: c, idx: c.sel[idx]}
	}
	return Row{c: c, idx: idx}
}

// AppendRow appends a event to the chunk.
func (c *Chunk) AppendRow(event Row) {
	c.AppendPartialRow(0, event)
	c.numVirtualRows++
}

// AppendPartialRow appends a event to the chunk.
func (c *Chunk) AppendPartialRow(defCausOff int, event Row) {
	c.appendSel(defCausOff)
	for i, rowDefCaus := range event.c.defCausumns {
		chkDefCaus := c.defCausumns[defCausOff+i]
		appendCellByCell(chkDefCaus, rowDefCaus, event.idx)
	}
}

// AppendRowByDefCausIdxs appends a event by its defCausIdxs to the chunk.
// 1. every defCausumns are used if defCausIdxs is nil.
// 2. no defCausumns are used if defCausIdxs is not nil but the size of defCausIdxs is 0.
func (c *Chunk) AppendRowByDefCausIdxs(event Row, defCausIdxs []int) (wide int) {
	wide = c.AppendPartialRowByDefCausIdxs(0, event, defCausIdxs)
	c.numVirtualRows++
	return
}

// AppendPartialRowByDefCausIdxs appends a event by its defCausIdxs to the chunk.
// 1. every defCausumns are used if defCausIdxs is nil.
// 2. no defCausumns are used if defCausIdxs is not nil but the size of defCausIdxs is 0.
func (c *Chunk) AppendPartialRowByDefCausIdxs(defCausOff int, event Row, defCausIdxs []int) (wide int) {
	if defCausIdxs == nil {
		c.AppendPartialRow(defCausOff, event)
		return event.Len()
	}

	c.appendSel(defCausOff)
	for i, defCausIdx := range defCausIdxs {
		rowDefCaus := event.c.defCausumns[defCausIdx]
		chkDefCaus := c.defCausumns[defCausOff+i]
		appendCellByCell(chkDefCaus, rowDefCaus, event.idx)
	}
	return len(defCausIdxs)
}

// appendCellByCell appends the cell with rowIdx of src into dst.
func appendCellByCell(dst *DeferredCauset, src *DeferredCauset, rowIdx int) {
	dst.appendNullBitmap(!src.IsNull(rowIdx))
	if src.isFixed() {
		elemLen := len(src.elemBuf)
		offset := rowIdx * elemLen
		dst.data = append(dst.data, src.data[offset:offset+elemLen]...)
	} else {
		start, end := src.offsets[rowIdx], src.offsets[rowIdx+1]
		dst.data = append(dst.data, src.data[start:end]...)
		dst.offsets = append(dst.offsets, int64(len(dst.data)))
	}
	dst.length++
}

// preAlloc pre-allocates the memory space in a Chunk to causetstore the Row.
// NOTE: only used in test.
// 1. The Chunk must be empty or holds no useful data.
// 2. The schemaReplicant of the Row must be the same with the Chunk.
// 3. This API is paired with the `Insert()` function, which inserts all the
//    rows data into the Chunk after the pre-allocation.
// 4. We set the null bitmap here instead of in the Insert() function because
//    when the Insert() function is called parallelly, the data race on a byte
//    can not be avoided although the manipulated bits are different inside a
//    byte.
func (c *Chunk) preAlloc(event Row) (rowIdx uint32) {
	rowIdx = uint32(c.NumRows())
	for i, srcDefCaus := range event.c.defCausumns {
		dstDefCaus := c.defCausumns[i]
		dstDefCaus.appendNullBitmap(!srcDefCaus.IsNull(event.idx))
		elemLen := len(srcDefCaus.elemBuf)
		if !srcDefCaus.isFixed() {
			elemLen = int(srcDefCaus.offsets[event.idx+1] - srcDefCaus.offsets[event.idx])
			dstDefCaus.offsets = append(dstDefCaus.offsets, int64(len(dstDefCaus.data)+elemLen))
		}
		dstDefCaus.length++
		needCap := len(dstDefCaus.data) + elemLen
		if needCap <= cap(dstDefCaus.data) {
			(*reflect.SliceHeader)(unsafe.Pointer(&dstDefCaus.data)).Len = len(dstDefCaus.data) + elemLen
			continue
		}
		// Grow the capacity according to golang.growslice.
		// Implementation differences with golang:
		// 1. We double the capacity when `dstDefCaus.data < 1024*elemLen bytes` but
		// not `1024 bytes`.
		// 2. We expand the capacity to 1.5*originCap rather than 1.25*originCap
		// during the slow-increasing phase.
		newCap := cap(dstDefCaus.data)
		doubleCap := newCap << 1
		if needCap > doubleCap {
			newCap = needCap
		} else {
			avgElemLen := elemLen
			if !srcDefCaus.isFixed() {
				avgElemLen = len(dstDefCaus.data) / len(dstDefCaus.offsets)
			}
			// slowIncThreshold indicates the threshold exceeding which the
			// dstDefCaus.data capacity increase fold decreases from 2 to 1.5.
			slowIncThreshold := 1024 * avgElemLen
			if len(dstDefCaus.data) < slowIncThreshold {
				newCap = doubleCap
			} else {
				for 0 < newCap && newCap < needCap {
					newCap += newCap / 2
				}
				if newCap <= 0 {
					newCap = needCap
				}
			}
		}
		dstDefCaus.data = make([]byte, len(dstDefCaus.data)+elemLen, newCap)
	}
	return
}

// insert inserts `event` on the position specified by `rowIdx`.
// NOTE: only used in test.
// Note: Insert will cover the origin data, it should be called after
// PreAlloc.
func (c *Chunk) insert(rowIdx int, event Row) {
	for i, srcDefCaus := range event.c.defCausumns {
		if event.IsNull(i) {
			continue
		}
		dstDefCaus := c.defCausumns[i]
		var srcStart, srcEnd, destStart, destEnd int
		if srcDefCaus.isFixed() {
			srcElemLen, destElemLen := len(srcDefCaus.elemBuf), len(dstDefCaus.elemBuf)
			srcStart, destStart = event.idx*srcElemLen, rowIdx*destElemLen
			srcEnd, destEnd = srcStart+srcElemLen, destStart+destElemLen
		} else {
			srcStart, srcEnd = int(srcDefCaus.offsets[event.idx]), int(srcDefCaus.offsets[event.idx+1])
			destStart, destEnd = int(dstDefCaus.offsets[rowIdx]), int(dstDefCaus.offsets[rowIdx+1])
		}
		copy(dstDefCaus.data[destStart:destEnd], srcDefCaus.data[srcStart:srcEnd])
	}
}

// Append appends rows in [begin, end) in another Chunk to a Chunk.
func (c *Chunk) Append(other *Chunk, begin, end int) {
	for defCausID, src := range other.defCausumns {
		dst := c.defCausumns[defCausID]
		if src.isFixed() {
			elemLen := len(src.elemBuf)
			dst.data = append(dst.data, src.data[begin*elemLen:end*elemLen]...)
		} else {
			beginOffset, endOffset := src.offsets[begin], src.offsets[end]
			dst.data = append(dst.data, src.data[beginOffset:endOffset]...)
			for i := begin; i < end; i++ {
				dst.offsets = append(dst.offsets, dst.offsets[len(dst.offsets)-1]+src.offsets[i+1]-src.offsets[i])
			}
		}
		for i := begin; i < end; i++ {
			c.appendSel(defCausID)
			dst.appendNullBitmap(!src.IsNull(i))
			dst.length++
		}
	}
	c.numVirtualRows += end - begin
}

// TruncateTo truncates rows from tail to head in a Chunk to "numRows" rows.
func (c *Chunk) TruncateTo(numRows int) {
	c.Reconstruct()
	for _, defCaus := range c.defCausumns {
		if defCaus.isFixed() {
			elemLen := len(defCaus.elemBuf)
			defCaus.data = defCaus.data[:numRows*elemLen]
		} else {
			defCaus.data = defCaus.data[:defCaus.offsets[numRows]]
			defCaus.offsets = defCaus.offsets[:numRows+1]
		}
		defCaus.length = numRows
		bitmapLen := (defCaus.length + 7) / 8
		defCaus.nullBitmap = defCaus.nullBitmap[:bitmapLen]
		if defCaus.length%8 != 0 {
			// When we append null, we simply increment the nullCount,
			// so we need to clear the unused bits in the last bitmap byte.
			lastByte := defCaus.nullBitmap[bitmapLen-1]
			unusedBitsLen := 8 - uint(defCaus.length%8)
			lastByte <<= unusedBitsLen
			lastByte >>= unusedBitsLen
			defCaus.nullBitmap[bitmapLen-1] = lastByte
		}
	}
	c.numVirtualRows = numRows
}

// AppendNull appends a null value to the chunk.
func (c *Chunk) AppendNull(defCausIdx int) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendNull()
}

// AppendInt64 appends a int64 value to the chunk.
func (c *Chunk) AppendInt64(defCausIdx int, i int64) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendInt64(i)
}

// AppendUint64 appends a uint64 value to the chunk.
func (c *Chunk) AppendUint64(defCausIdx int, u uint64) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendUint64(u)
}

// AppendFloat32 appends a float32 value to the chunk.
func (c *Chunk) AppendFloat32(defCausIdx int, f float32) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendFloat32(f)
}

// AppendFloat64 appends a float64 value to the chunk.
func (c *Chunk) AppendFloat64(defCausIdx int, f float64) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendFloat64(f)
}

// AppendString appends a string value to the chunk.
func (c *Chunk) AppendString(defCausIdx int, str string) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendString(str)
}

// AppendBytes appends a bytes value to the chunk.
func (c *Chunk) AppendBytes(defCausIdx int, b []byte) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendBytes(b)
}

// AppendTime appends a Time value to the chunk.
func (c *Chunk) AppendTime(defCausIdx int, t types.Time) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendTime(t)
}

// AppendDuration appends a Duration value to the chunk.
func (c *Chunk) AppendDuration(defCausIdx int, dur types.Duration) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendDuration(dur)
}

// AppendMyDecimal appends a MyDecimal value to the chunk.
func (c *Chunk) AppendMyDecimal(defCausIdx int, dec *types.MyDecimal) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendMyDecimal(dec)
}

// AppendEnum appends an Enum value to the chunk.
func (c *Chunk) AppendEnum(defCausIdx int, enum types.Enum) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].appendNameValue(enum.Name, enum.Value)
}

// AppendSet appends a Set value to the chunk.
func (c *Chunk) AppendSet(defCausIdx int, set types.Set) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].appendNameValue(set.Name, set.Value)
}

// AppendJSON appends a JSON value to the chunk.
func (c *Chunk) AppendJSON(defCausIdx int, j json.BinaryJSON) {
	c.appendSel(defCausIdx)
	c.defCausumns[defCausIdx].AppendJSON(j)
}

func (c *Chunk) appendSel(defCausIdx int) {
	if defCausIdx == 0 && c.sel != nil { // use defCausumn 0 as standard
		c.sel = append(c.sel, c.defCausumns[0].length)
	}
}

// AppendCauset appends a causet into the chunk.
func (c *Chunk) AppendCauset(defCausIdx int, d *types.Causet) {
	switch d.HoTT() {
	case types.HoTTNull:
		c.AppendNull(defCausIdx)
	case types.HoTTInt64:
		c.AppendInt64(defCausIdx, d.GetInt64())
	case types.HoTTUint64:
		c.AppendUint64(defCausIdx, d.GetUint64())
	case types.HoTTFloat32:
		c.AppendFloat32(defCausIdx, d.GetFloat32())
	case types.HoTTFloat64:
		c.AppendFloat64(defCausIdx, d.GetFloat64())
	case types.HoTTString, types.HoTTBytes, types.HoTTBinaryLiteral, types.HoTTRaw, types.HoTTMysqlBit:
		c.AppendBytes(defCausIdx, d.GetBytes())
	case types.HoTTMysqlDecimal:
		c.AppendMyDecimal(defCausIdx, d.GetMysqlDecimal())
	case types.HoTTMysqlDuration:
		c.AppendDuration(defCausIdx, d.GetMysqlDuration())
	case types.HoTTMysqlEnum:
		c.AppendEnum(defCausIdx, d.GetMysqlEnum())
	case types.HoTTMysqlSet:
		c.AppendSet(defCausIdx, d.GetMysqlSet())
	case types.HoTTMysqlTime:
		c.AppendTime(defCausIdx, d.GetMysqlTime())
	case types.HoTTMysqlJSON:
		c.AppendJSON(defCausIdx, d.GetMysqlJSON())
	}
}

// DeferredCauset returns the specific defCausumn.
func (c *Chunk) DeferredCauset(defCausIdx int) *DeferredCauset {
	return c.defCausumns[defCausIdx]
}

// SetDefCaus sets the defCausIdx DeferredCauset to defCaus and returns the old DeferredCauset.
func (c *Chunk) SetDefCaus(defCausIdx int, defCaus *DeferredCauset) *DeferredCauset {
	if defCaus == c.defCausumns[defCausIdx] {
		return nil
	}
	old := c.defCausumns[defCausIdx]
	c.defCausumns[defCausIdx] = defCaus
	return old
}

// Sel returns Sel of this Chunk.
func (c *Chunk) Sel() []int {
	return c.sel
}

// SetSel sets a Sel for this Chunk.
func (c *Chunk) SetSel(sel []int) {
	c.sel = sel
}

// Reconstruct removes all filtered rows in this Chunk.
func (c *Chunk) Reconstruct() {
	if c.sel == nil {
		return
	}
	for _, defCaus := range c.defCausumns {
		defCaus.reconstruct(c.sel)
	}
	c.numVirtualRows = len(c.sel)
	c.sel = nil
}
