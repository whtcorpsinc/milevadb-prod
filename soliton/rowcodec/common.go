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
	"reflect"
	"unsafe"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/types"
)

// CodecVer is the constant number that represent the new event format.
const CodecVer = 128

var errInvalidCodecVer = errors.New("invalid codec version")

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag          byte = 0
	BytesFlag        byte = 1
	CompactBytesFlag byte = 2
	IntFlag          byte = 3
	UintFlag         byte = 4
	FloatFlag        byte = 5
	DecimalFlag      byte = 6
	VarintFlag       byte = 8
	VaruintFlag      byte = 9
	JSONFlag         byte = 10
)

func bytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytes2U16Slice(b []byte) []uint16 {
	if len(b) == 0 {
		return nil
	}
	var u16s []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u16s))
	hdr.Len = len(b) / 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u16s
}

func u16SliceToBytes(u16s []uint16) []byte {
	if len(u16s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u16s) * 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u16s[0]))
	return b
}

func u32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func encodeInt(buf []byte, iVal int64) []byte {
	var tmp [8]byte
	if int64(int8(iVal)) == iVal {
		buf = append(buf, byte(iVal))
	} else if int64(int16(iVal)) == iVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(iVal))
		buf = append(buf, tmp[:2]...)
	} else if int64(int32(iVal)) == iVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(iVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uint64(iVal))
		buf = append(buf, tmp[:8]...)
	}
	return buf
}

func decodeInt(val []byte) int64 {
	switch len(val) {
	case 1:
		return int64(int8(val[0]))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(val)))
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(val)))
	default:
		return int64(binary.LittleEndian.Uint64(val))
	}
}

func encodeUint(buf []byte, uVal uint64) []byte {
	var tmp [8]byte
	if uint64(uint8(uVal)) == uVal {
		buf = append(buf, byte(uVal))
	} else if uint64(uint16(uVal)) == uVal {
		binary.LittleEndian.PutUint16(tmp[:], uint16(uVal))
		buf = append(buf, tmp[:2]...)
	} else if uint64(uint32(uVal)) == uVal {
		binary.LittleEndian.PutUint32(tmp[:], uint32(uVal))
		buf = append(buf, tmp[:4]...)
	} else {
		binary.LittleEndian.PutUint64(tmp[:], uVal)
		buf = append(buf, tmp[:8]...)
	}
	return buf
}

func decodeUint(val []byte) uint64 {
	switch len(val) {
	case 1:
		return uint64(val[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(val))
	case 4:
		return uint64(binary.LittleEndian.Uint32(val))
	default:
		return binary.LittleEndian.Uint64(val)
	}
}

type largeNotNullSorter CausetEncoder

func (s *largeNotNullSorter) Less(i, j int) bool {
	return s.defCausIDs32[i] < s.defCausIDs32[j]
}

func (s *largeNotNullSorter) Len() int {
	return int(s.numNotNullDefCauss)
}

func (s *largeNotNullSorter) Swap(i, j int) {
	s.defCausIDs32[i], s.defCausIDs32[j] = s.defCausIDs32[j], s.defCausIDs32[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

type smallNotNullSorter CausetEncoder

func (s *smallNotNullSorter) Less(i, j int) bool {
	return s.defCausIDs[i] < s.defCausIDs[j]
}

func (s *smallNotNullSorter) Len() int {
	return int(s.numNotNullDefCauss)
}

func (s *smallNotNullSorter) Swap(i, j int) {
	s.defCausIDs[i], s.defCausIDs[j] = s.defCausIDs[j], s.defCausIDs[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

type smallNullSorter CausetEncoder

func (s *smallNullSorter) Less(i, j int) bool {
	nullDefCauss := s.defCausIDs[s.numNotNullDefCauss:]
	return nullDefCauss[i] < nullDefCauss[j]
}

func (s *smallNullSorter) Len() int {
	return int(s.numNullDefCauss)
}

func (s *smallNullSorter) Swap(i, j int) {
	nullDefCauss := s.defCausIDs[s.numNotNullDefCauss:]
	nullDefCauss[i], nullDefCauss[j] = nullDefCauss[j], nullDefCauss[i]
}

type largeNullSorter CausetEncoder

func (s *largeNullSorter) Less(i, j int) bool {
	nullDefCauss := s.defCausIDs32[s.numNotNullDefCauss:]
	return nullDefCauss[i] < nullDefCauss[j]
}

func (s *largeNullSorter) Len() int {
	return int(s.numNullDefCauss)
}

func (s *largeNullSorter) Swap(i, j int) {
	nullDefCauss := s.defCausIDs32[s.numNotNullDefCauss:]
	nullDefCauss[i], nullDefCauss[j] = nullDefCauss[j], nullDefCauss[i]
}

const (
	// Length of rowkey.
	rowKeyLen = 19
	// Index of record flag 'r' in rowkey used by milevadb-server.
	// The rowkey format is t{8 bytes id}_r{8 bytes handle}
	recordPrefixIdx = 10
)

// IsRowKey determine whether key is event key.
// this method will be used in entangledstore.
func IsRowKey(key []byte) bool {
	return len(key) >= rowKeyLen && key[0] == 't' && key[recordPrefixIdx] == 'r'
}

// IsNewFormat checks whether event data is in new-format.
func IsNewFormat(rowData []byte) bool {
	return rowData[0] == CodecVer
}

// FieldTypeFromPerceptronDeferredCauset creates a types.FieldType from perceptron.DeferredCausetInfo.
// export for test case and CDC.
func FieldTypeFromPerceptronDeferredCauset(defCaus *perceptron.DeferredCausetInfo) *types.FieldType {
	return &types.FieldType{
		Tp:      defCaus.Tp,
		Flag:    defCaus.Flag,
		Flen:    defCaus.Flen,
		Decimal: defCaus.Decimal,
		Elems:   defCaus.Elems,
		Charset: defCaus.Charset,
		DefCauslate: defCaus.DefCauslate,
	}
}
