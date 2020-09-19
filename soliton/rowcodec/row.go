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
	"encoding/binary"
)

// event is the struct type used to access the a event.
type event struct {
	// small:  defCausID []byte, offsets []uint16, optimized for most cases.
	// large:  defCausID []uint32, offsets []uint32.
	large          bool
	numNotNullDefCauss uint16
	numNullDefCauss    uint16
	defCausIDs         []byte

	offsets []uint16
	data    []byte

	// for large event
	defCausIDs32  []uint32
	offsets32 []uint32
}

func (r *event) getData(i int) []byte {
	var start, end uint32
	if r.large {
		if i > 0 {
			start = r.offsets32[i-1]
		}
		end = r.offsets32[i]
	} else {
		if i > 0 {
			start = uint32(r.offsets[i-1])
		}
		end = uint32(r.offsets[i])
	}
	return r.data[start:end]
}

func (r *event) fromBytes(rowData []byte) error {
	if rowData[0] != CodecVer {
		return errInvalidCodecVer
	}
	r.large = rowData[1]&1 > 0
	r.numNotNullDefCauss = binary.LittleEndian.Uint16(rowData[2:])
	r.numNullDefCauss = binary.LittleEndian.Uint16(rowData[4:])
	cursor := 6
	if r.large {
		defCausIDsLen := int(r.numNotNullDefCauss+r.numNullDefCauss) * 4
		r.defCausIDs32 = bytesToU32Slice(rowData[cursor : cursor+defCausIDsLen])
		cursor += defCausIDsLen
		offsetsLen := int(r.numNotNullDefCauss) * 4
		r.offsets32 = bytesToU32Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	} else {
		defCausIDsLen := int(r.numNotNullDefCauss + r.numNullDefCauss)
		r.defCausIDs = rowData[cursor : cursor+defCausIDsLen]
		cursor += defCausIDsLen
		offsetsLen := int(r.numNotNullDefCauss) * 2
		r.offsets = bytes2U16Slice(rowData[cursor : cursor+offsetsLen])
		cursor += offsetsLen
	}
	r.data = rowData[cursor:]
	return nil
}

func (r *event) toBytes(buf []byte) []byte {
	buf = append(buf, CodecVer)
	flag := byte(0)
	if r.large {
		flag = 1
	}
	buf = append(buf, flag)
	buf = append(buf, byte(r.numNotNullDefCauss), byte(r.numNotNullDefCauss>>8))
	buf = append(buf, byte(r.numNullDefCauss), byte(r.numNullDefCauss>>8))
	if r.large {
		buf = append(buf, u32SliceToBytes(r.defCausIDs32)...)
		buf = append(buf, u32SliceToBytes(r.offsets32)...)
	} else {
		buf = append(buf, r.defCausIDs...)
		buf = append(buf, u16SliceToBytes(r.offsets)...)
	}
	buf = append(buf, r.data...)
	return buf
}

func (r *event) findDefCausID(defCausID int64) (idx int, isNil, notFound bool) {
	// Search the defCausumn in not-null defCausumns array.
	i, j := 0, int(r.numNotNullDefCauss)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large {
			v = int64(r.defCausIDs32[h])
		} else {
			v = int64(r.defCausIDs[h])
		}
		if v < defCausID {
			i = h + 1
		} else if v > defCausID {
			j = h
		} else {
			idx = h
			return
		}
	}

	// Search the defCausumn in null defCausumns array.
	i, j = int(r.numNotNullDefCauss), int(r.numNotNullDefCauss+r.numNullDefCauss)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		var v int64
		if r.large {
			v = int64(r.defCausIDs32[h])
		} else {
			v = int64(r.defCausIDs[h])
		}
		if v < defCausID {
			i = h + 1
		} else if v > defCausID {
			j = h
		} else {
			isNil = true
			return
		}
	}
	notFound = true
	return
}

// DeferredCausetIsNull returns if the defCausumn value is null. Mainly used for count defCausumn aggregation.
// this method will used in entangledstore.
func (r *event) DeferredCausetIsNull(rowData []byte, defCausID int64, defaultVal []byte) (bool, error) {
	err := r.fromBytes(rowData)
	if err != nil {
		return false, err
	}
	_, isNil, notFound := r.findDefCausID(defCausID)
	if notFound {
		return defaultVal == nil, nil
	}
	return isNil, nil
}

func (r *event) initDefCausIDs() {
	numDefCauss := int(r.numNotNullDefCauss + r.numNullDefCauss)
	if cap(r.defCausIDs) >= numDefCauss {
		r.defCausIDs = r.defCausIDs[:numDefCauss]
	} else {
		r.defCausIDs = make([]byte, numDefCauss)
	}
}

func (r *event) initDefCausIDs32() {
	numDefCauss := int(r.numNotNullDefCauss + r.numNullDefCauss)
	if cap(r.defCausIDs32) >= numDefCauss {
		r.defCausIDs32 = r.defCausIDs32[:numDefCauss]
	} else {
		r.defCausIDs32 = make([]uint32, numDefCauss)
	}
}

func (r *event) initOffsets() {
	if cap(r.offsets) >= int(r.numNotNullDefCauss) {
		r.offsets = r.offsets[:r.numNotNullDefCauss]
	} else {
		r.offsets = make([]uint16, r.numNotNullDefCauss)
	}
}

func (r *event) initOffsets32() {
	if cap(r.offsets32) >= int(r.numNotNullDefCauss) {
		r.offsets32 = r.offsets32[:r.numNotNullDefCauss]
	} else {
		r.offsets32 = make([]uint32, r.numNotNullDefCauss)
	}
}
