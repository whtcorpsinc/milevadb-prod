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
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/checksum"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/encrypt"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
)

// ListInDisk represents a slice of chunks storing in temporary disk.
type ListInDisk struct {
	fieldTypes []*types.FieldType
	// offsets stores the offsets in disk of all RowPtr,
	// the offset of one RowPtr is offsets[RowPtr.ChkIdx][RowPtr.RowIdx].
	offsets [][]int64
	// offWrite is the current offset for writing.
	offWrite int64

	disk          *os.File
	w             io.WriteCloser
	bufFlushMutex sync.RWMutex
	diskTracker   *disk.Tracker // track disk usage.
	numRowsInDisk int

	// ctrCipher stores the key and nonce using by aes encrypt io layer
	ctrCipher *encrypt.CtrCipher
}

var defaultChunkListInDiskPath = "chunk.ListInDisk"

// NewListInDisk creates a new ListInDisk with field types.
func NewListInDisk(fieldTypes []*types.FieldType) *ListInDisk {
	l := &ListInDisk{
		fieldTypes: fieldTypes,
		// TODO(fengliyuan): set the quota of disk usage.
		diskTracker: disk.NewTracker(memory.LabelForChunkListInDisk, -1),
	}
	return l
}

func (l *ListInDisk) initDiskFile() (err error) {
	err = disk.CheckAndInitTemFIDelir()
	if err != nil {
		return
	}
	l.disk, err = ioutil.TempFile(config.GetGlobalConfig().TempStoragePath, defaultChunkListInDiskPath+strconv.Itoa(l.diskTracker.Label()))
	if err != nil {
		return
	}
	var underlying io.WriteCloser = l.disk
	if config.GetGlobalConfig().Security.SpilledFileEncryptionMethod != config.SpilledFileEncryptionMethodPlaintext {
		// The possible values of SpilledFileEncryptionMethod are "plaintext", "aes128-ctr"
		l.ctrCipher, err = encrypt.NewCtrCipher()
		if err != nil {
			return
		}
		underlying = encrypt.NewWriter(l.disk, l.ctrCipher)
	}
	l.w = checksum.NewWriter(underlying)
	l.bufFlushMutex = sync.RWMutex{}
	return
}

// Len returns the number of rows in ListInDisk
func (l *ListInDisk) Len() int {
	return l.numRowsInDisk
}

// GetDiskTracker returns the memory tracker of this List.
func (l *ListInDisk) GetDiskTracker() *disk.Tracker {
	return l.diskTracker
}

// flush empties the write buffer, please call flush before read!
func (l *ListInDisk) flush() (err error) {
	// buffered is not zero only after Add and before GetRow, after the first flush, buffered will always be zero,
	// hence we use a RWLock to allow quicker quit.
	l.bufFlushMutex.RLock()
	checksumWriter := l.w
	l.bufFlushMutex.RUnlock()
	if checksumWriter == nil {
		return nil
	}
	l.bufFlushMutex.Lock()
	defer l.bufFlushMutex.Unlock()
	if l.w != nil {
		err = l.w.Close()
		if err != nil {
			return
		}
		l.w = nil
		// the l.disk is the underlying object of the l.w, it will be closed
		// after calling l.w.Close, we need to reopen it before reading rows.
		l.disk, err = os.Open(l.disk.Name())
		if err != nil {
			return
		}
	}
	return
}

// Add adds a chunk to the ListInDisk. Caller must make sure the input chk
// is not empty and not used any more and has the same field types.
// Warning: do not mix Add and GetRow (always use GetRow after you have added all the chunks), and do not use Add concurrently.
func (l *ListInDisk) Add(chk *Chunk) (err error) {
	if chk.NumRows() == 0 {
		return errors.New("chunk appended to List should have at least 1 event")
	}
	if l.disk == nil {
		err = l.initDiskFile()
		if err != nil {
			return
		}
	}
	chk2 := chunHoTTisk{Chunk: chk, offWrite: l.offWrite}
	n, err := chk2.WriteTo(l.w)
	l.offWrite += n
	if err != nil {
		return
	}
	l.offsets = append(l.offsets, chk2.getOffsetsOfRows())
	l.diskTracker.Consume(n)
	l.numRowsInDisk += chk.NumRows()
	return
}

// GetChunk gets a Chunk from the ListInDisk by chkIdx.
func (l *ListInDisk) GetChunk(chkIdx int) (*Chunk, error) {
	chk := NewChunkWithCapacity(l.fieldTypes, l.NumRowsOfChunk(chkIdx))
	offsets := l.offsets[chkIdx]
	for rowIdx := range offsets {
		event, err := l.GetRow(RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		if err != nil {
			return chk, err
		}
		chk.AppendRow(event)
	}
	return chk, nil
}

// GetRow gets a Row from the ListInDisk by RowPtr.
func (l *ListInDisk) GetRow(ptr RowPtr) (event Row, err error) {
	err = l.flush()
	if err != nil {
		return
	}
	off := l.offsets[ptr.ChkIdx][ptr.RowIdx]
	var underlying io.ReaderAt = l.disk
	if l.ctrCipher != nil {
		underlying = encrypt.NewReader(l.disk, l.ctrCipher)
	}
	r := io.NewSectionReader(checksum.NewReader(underlying), off, l.offWrite-off)
	format := rowInDisk{numDefCaus: len(l.fieldTypes)}
	_, err = format.ReadFrom(r)
	if err != nil {
		return event, err
	}
	event = format.toMutRow(l.fieldTypes).ToRow()
	return event, err
}

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (l *ListInDisk) NumRowsOfChunk(chkID int) int {
	return len(l.offsets[chkID])
}

// NumChunks returns the number of chunks in the ListInDisk.
func (l *ListInDisk) NumChunks() int {
	return len(l.offsets)
}

// Close releases the disk resource.
func (l *ListInDisk) Close() error {
	if l.disk != nil {
		l.diskTracker.Consume(-l.diskTracker.BytesConsumed())
		terror.Call(l.disk.Close)
		terror.Log(os.Remove(l.disk.Name()))
	}
	return nil
}

// chunHoTTisk represents a chunk in disk format. Each event of the chunk
// is serialized and in sequence ordered. The format of each event is like
// the struct diskFormatRow, put size of each defCausumn first, then the
// data of each defCausumn.
//
// For example, a chunk has 2 rows and 3 defCausumns, the disk format of the
// chunk is as follow:
//
// [size of row0 defCausumn0], [size of row0 defCausumn1], [size of row0 defCausumn2]
// [data of row0 defCausumn0], [data of row0 defCausumn1], [data of row0 defCausumn2]
// [size of row1 defCausumn0], [size of row1 defCausumn1], [size of row1 defCausumn2]
// [data of row1 defCausumn0], [data of row1 defCausumn1], [data of row1 defCausumn2]
//
// If a defCausumn of a event is null, the size of it is -1 and the data is empty.
type chunHoTTisk struct {
	*Chunk
	// offWrite is the current offset for writing.
	offWrite int64
	// offsetsOfRows stores the offset of each event.
	offsetsOfRows []int64
}

// WriteTo serializes the chunk into the format of chunHoTTisk, and
// writes to w.
func (chk *chunHoTTisk) WriteTo(w io.Writer) (written int64, err error) {
	var n int64
	numRows := chk.NumRows()
	chk.offsetsOfRows = make([]int64, 0, numRows)
	var format *diskFormatRow
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		format = convertFromRow(chk.GetRow(rowIdx), format)
		chk.offsetsOfRows = append(chk.offsetsOfRows, chk.offWrite+written)

		n, err = rowInDisk{diskFormatRow: *format}.WriteTo(w)
		written += n
		if err != nil {
			return
		}
	}
	return
}

// getOffsetsOfRows gets the offset of each event.
func (chk *chunHoTTisk) getOffsetsOfRows() []int64 { return chk.offsetsOfRows }

// rowInDisk represents a Row in format of diskFormatRow.
type rowInDisk struct {
	numDefCaus int
	diskFormatRow
}

// WriteTo serializes a event of the chunk into the format of
// diskFormatRow, and writes to w.
func (event rowInDisk) WriteTo(w io.Writer) (written int64, err error) {
	n, err := w.Write(i64SliceToBytes(event.sizesOfDeferredCausets))
	written += int64(n)
	if err != nil {
		return
	}
	for _, data := range event.cells {
		n, err = w.Write(data)
		written += int64(n)
		if err != nil {
			return
		}
	}
	return
}

// ReadFrom reads data of r, deserializes it from the format of diskFormatRow
// into Row.
func (event *rowInDisk) ReadFrom(r io.Reader) (n int64, err error) {
	b := make([]byte, 8*event.numDefCaus)
	var n1 int
	n1, err = io.ReadFull(r, b)
	n += int64(n1)
	if err != nil {
		return
	}
	event.sizesOfDeferredCausets = bytesToI64Slice(b)
	event.cells = make([][]byte, 0, event.numDefCaus)
	for _, size := range event.sizesOfDeferredCausets {
		if size == -1 {
			continue
		}
		cell := make([]byte, size)
		event.cells = append(event.cells, cell)
		n1, err = io.ReadFull(r, cell)
		n += int64(n1)
		if err != nil {
			return
		}
	}
	return
}

// diskFormatRow represents a event in a chunk in disk format. The disk format
// of a event is described in the doc of chunHoTTisk.
type diskFormatRow struct {
	// sizesOfDeferredCausets stores the size of each defCausumn in a event.
	// -1 means the value of this defCausumn is null.
	sizesOfDeferredCausets []int64 // -1 means null
	// cells represents raw data of not-null defCausumns in one event.
	// In convertFromRow, data from Row is shallow copied to cells.
	// In toMutRow, data in cells is shallow copied to MutRow.
	cells [][]byte
}

// convertFromRow serializes one event of chunk to diskFormatRow, then
// we can use diskFormatRow to write to disk.
func convertFromRow(event Row, reuse *diskFormatRow) (format *diskFormatRow) {
	numDefCauss := event.Chunk().NumDefCauss()
	if reuse != nil {
		format = reuse
		format.sizesOfDeferredCausets = format.sizesOfDeferredCausets[:0]
		format.cells = format.cells[:0]
	} else {
		format = &diskFormatRow{
			sizesOfDeferredCausets: make([]int64, 0, numDefCauss),
			cells:          make([][]byte, 0, numDefCauss),
		}
	}
	for defCausIdx := 0; defCausIdx < numDefCauss; defCausIdx++ {
		if event.IsNull(defCausIdx) {
			format.sizesOfDeferredCausets = append(format.sizesOfDeferredCausets, -1)
		} else {
			cell := event.GetRaw(defCausIdx)
			format.sizesOfDeferredCausets = append(format.sizesOfDeferredCausets, int64(len(cell)))
			format.cells = append(format.cells, cell)
		}
	}
	return
}

// toMutRow deserializes diskFormatRow to MutRow.
func (format *diskFormatRow) toMutRow(fields []*types.FieldType) MutRow {
	chk := &Chunk{defCausumns: make([]*DeferredCauset, 0, len(format.sizesOfDeferredCausets))}
	var cellOff int
	for defCausIdx, size := range format.sizesOfDeferredCausets {
		defCaus := &DeferredCauset{length: 1}
		elemSize := getFixedLen(fields[defCausIdx])
		if size == -1 { // isNull
			defCaus.nullBitmap = []byte{0}
			if elemSize == varElemLen {
				defCaus.offsets = []int64{0, 0}
			} else {
				buf := make([]byte, elemSize)
				defCaus.data = buf
				defCaus.elemBuf = buf
			}
		} else {
			defCaus.nullBitmap = []byte{1}
			defCaus.data = format.cells[cellOff]
			cellOff++
			if elemSize == varElemLen {
				defCaus.offsets = []int64{0, int64(len(defCaus.data))}
			} else {
				defCaus.elemBuf = defCaus.data
			}
		}
		chk.defCausumns = append(chk.defCausumns, defCaus)
	}
	return MutRow{c: chk}
}
