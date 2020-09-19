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

import "github.com/whtcorpsinc/errors"

// CopySelectedJoinRowsDirect directly copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined event was selected.
func CopySelectedJoinRowsDirect(src *Chunk, selected []bool, dst *Chunk) (bool, error) {
	if src.NumRows() == 0 {
		return false, nil
	}
	if src.sel != nil || dst.sel != nil {
		return false, errors.New(msgErrSelNotNil)
	}
	if len(src.defCausumns) == 0 {
		numSelected := 0
		for _, s := range selected {
			if s {
				numSelected++
			}
		}
		dst.numVirtualRows += numSelected
		return numSelected > 0, nil
	}

	oldLen := dst.defCausumns[0].length
	for j, srcDefCaus := range src.defCausumns {
		dstDefCaus := dst.defCausumns[j]
		if srcDefCaus.isFixed() {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstDefCaus.appendNullBitmap(!srcDefCaus.IsNull(i))
				dstDefCaus.length++

				elemLen := len(srcDefCaus.elemBuf)
				offset := i * elemLen
				dstDefCaus.data = append(dstDefCaus.data, srcDefCaus.data[offset:offset+elemLen]...)
			}
		} else {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstDefCaus.appendNullBitmap(!srcDefCaus.IsNull(i))
				dstDefCaus.length++

				start, end := srcDefCaus.offsets[i], srcDefCaus.offsets[i+1]
				dstDefCaus.data = append(dstDefCaus.data, srcDefCaus.data[start:end]...)
				dstDefCaus.offsets = append(dstDefCaus.offsets, int64(len(dstDefCaus.data)))
			}
		}
	}
	numSelected := dst.defCausumns[0].length - oldLen
	dst.numVirtualRows += numSelected
	return numSelected > 0, nil
}

// CopySelectedJoinRowsWithSameOuterRows copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined event was selected.
//
// NOTE: All the outer rows in the source Chunk should be the same.
func CopySelectedJoinRowsWithSameOuterRows(src *Chunk, innerDefCausOffset, innerDefCausLen, outerDefCausOffset, outerDefCausLen int, selected []bool, dst *Chunk) (bool, error) {
	if src.NumRows() == 0 {
		return false, nil
	}
	if src.sel != nil || dst.sel != nil {
		return false, errors.New(msgErrSelNotNil)
	}

	numSelected := copySelectedInnerRows(innerDefCausOffset, innerDefCausLen, src, selected, dst)
	copySameOuterRows(outerDefCausOffset, outerDefCausLen, src, numSelected, dst)
	dst.numVirtualRows += numSelected
	return numSelected > 0, nil
}

// copySelectedInnerRows copies the selected inner rows from the source Chunk
// to the destination Chunk.
// return the number of rows which is selected.
func copySelectedInnerRows(innerDefCausOffset, innerDefCausLen int, src *Chunk, selected []bool, dst *Chunk) int {
	srcDefCauss := src.defCausumns[innerDefCausOffset : innerDefCausOffset+innerDefCausLen]
	if len(srcDefCauss) == 0 {
		numSelected := 0
		for _, s := range selected {
			if s {
				numSelected++
			}
		}
		return numSelected
	}
	oldLen := dst.defCausumns[innerDefCausOffset].length
	for j, srcDefCaus := range srcDefCauss {
		dstDefCaus := dst.defCausumns[innerDefCausOffset+j]
		if srcDefCaus.isFixed() {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstDefCaus.appendNullBitmap(!srcDefCaus.IsNull(i))
				dstDefCaus.length++

				elemLen := len(srcDefCaus.elemBuf)
				offset := i * elemLen
				dstDefCaus.data = append(dstDefCaus.data, srcDefCaus.data[offset:offset+elemLen]...)
			}
		} else {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstDefCaus.appendNullBitmap(!srcDefCaus.IsNull(i))
				dstDefCaus.length++

				start, end := srcDefCaus.offsets[i], srcDefCaus.offsets[i+1]
				dstDefCaus.data = append(dstDefCaus.data, srcDefCaus.data[start:end]...)
				dstDefCaus.offsets = append(dstDefCaus.offsets, int64(len(dstDefCaus.data)))
			}
		}
	}
	return dst.defCausumns[innerDefCausOffset].length - oldLen
}

// copySameOuterRows copies the continuous 'numRows' outer rows in the source Chunk
// to the destination Chunk.
func copySameOuterRows(outerDefCausOffset, outerDefCausLen int, src *Chunk, numRows int, dst *Chunk) {
	if numRows <= 0 || outerDefCausLen <= 0 {
		return
	}
	event := src.GetRow(0)
	srcDefCauss := src.defCausumns[outerDefCausOffset : outerDefCausOffset+outerDefCausLen]
	for i, srcDefCaus := range srcDefCauss {
		dstDefCaus := dst.defCausumns[outerDefCausOffset+i]
		dstDefCaus.appendMultiSameNullBitmap(!srcDefCaus.IsNull(event.idx), numRows)
		dstDefCaus.length += numRows
		if srcDefCaus.isFixed() {
			elemLen := len(srcDefCaus.elemBuf)
			start := event.idx * elemLen
			end := start + numRows*elemLen
			dstDefCaus.data = append(dstDefCaus.data, srcDefCaus.data[start:end]...)
		} else {
			start, end := srcDefCaus.offsets[event.idx], srcDefCaus.offsets[event.idx+numRows]
			dstDefCaus.data = append(dstDefCaus.data, srcDefCaus.data[start:end]...)
			offsets := dstDefCaus.offsets
			elemLen := srcDefCaus.offsets[event.idx+1] - srcDefCaus.offsets[event.idx]
			for j := 0; j < numRows; j++ {
				offsets = append(offsets, offsets[len(offsets)-1]+elemLen)
			}
			dstDefCaus.offsets = offsets
		}
	}
}
