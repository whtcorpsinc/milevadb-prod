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

package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
)

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag          byte = 0
	bytesFlag        byte = 1
	compactBytesFlag byte = 2
	intFlag          byte = 3
	uintFlag         byte = 4
	floatFlag        byte = 5
	decimalFlag      byte = 6
	durationFlag     byte = 7
	varintFlag       byte = 8
	uvarintFlag      byte = 9
	jsonFlag         byte = 10
	maxFlag          byte = 250
)

const (
	sizeUint64  = unsafe.Sizeof(uint64(0))
	sizeFloat64 = unsafe.Sizeof(float64(0))
)

func preRealloc(b []byte, vals []types.Causet, comparable bool) []byte {
	var size int
	for i := range vals {
		switch vals[i].HoTT() {
		case types.HoTTInt64, types.HoTTUint64, types.HoTTMysqlEnum, types.HoTTMysqlSet, types.HoTTMysqlBit, types.HoTTBinaryLiteral:
			size += sizeInt(comparable)
		case types.HoTTString, types.HoTTBytes:
			size += sizeBytes(vals[i].GetBytes(), comparable)
		case types.HoTTMysqlTime, types.HoTTMysqlDuration, types.HoTTFloat32, types.HoTTFloat64:
			size += 9
		case types.HoTTNull, types.HoTTMinNotNull, types.HoTTMaxValue:
			size += 1
		case types.HoTTMysqlJSON:
			size += 2 + len(vals[i].GetBytes())
		case types.HoTTMysqlDecimal:
			size += 1 + types.MyDecimalStructSize
		default:
			return b
		}
	}
	return reallocBytes(b, size)
}

// encode will encode a causet and append it to a byte slice. If comparable is true, the encoded bytes can be sorted as it's original order.
// If hash is true, the encoded bytes can be checked equal as it's original value.
func encode(sc *stmtctx.StatementContext, b []byte, vals []types.Causet, comparable bool) (_ []byte, err error) {
	b = preRealloc(b, vals, comparable)
	for i, length := 0, len(vals); i < length; i++ {
		switch vals[i].HoTT() {
		case types.HoTTInt64:
			b = encodeSignedInt(b, vals[i].GetInt64(), comparable)
		case types.HoTTUint64:
			b = encodeUnsignedInt(b, vals[i].GetUint64(), comparable)
		case types.HoTTFloat32, types.HoTTFloat64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, vals[i].GetFloat64())
		case types.HoTTString:
			b = encodeString(b, vals[i], comparable)
		case types.HoTTBytes:
			b = encodeBytes(b, vals[i].GetBytes(), comparable)
		case types.HoTTMysqlTime:
			b = append(b, uintFlag)
			b, err = EncodeMyALLEGROSQLTime(sc, vals[i].GetMysqlTime(), allegrosql.TypeUnspecified, b)
			if err != nil {
				return b, err
			}
		case types.HoTTMysqlDuration:
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(vals[i].GetMysqlDuration().Duration))
		case types.HoTTMysqlDecimal:
			b = append(b, decimalFlag)
			b, err = EncodeDecimal(b, vals[i].GetMysqlDecimal(), vals[i].Length(), vals[i].Frac())
			if terror.ErrorEqual(err, types.ErrTruncated) {
				err = sc.HandleTruncate(err)
			} else if terror.ErrorEqual(err, types.ErrOverflow) {
				err = sc.HandleOverflow(err, err)
			}
		case types.HoTTMysqlEnum:
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlEnum().ToNumber()), comparable)
		case types.HoTTMysqlSet:
			b = encodeUnsignedInt(b, uint64(vals[i].GetMysqlSet().ToNumber()), comparable)
		case types.HoTTMysqlBit, types.HoTTBinaryLiteral:
			// We don't need to handle errors here since the literal is ensured to be able to causetstore in uint64 in convertToMysqlBit.
			var val uint64
			val, err = vals[i].GetBinaryLiteral().ToInt(sc)
			terror.Log(errors.Trace(err))
			b = encodeUnsignedInt(b, val, comparable)
		case types.HoTTMysqlJSON:
			b = append(b, jsonFlag)
			j := vals[i].GetMysqlJSON()
			b = append(b, j.TypeCode)
			b = append(b, j.Value...)
		case types.HoTTNull:
			b = append(b, NilFlag)
		case types.HoTTMinNotNull:
			b = append(b, bytesFlag)
		case types.HoTTMaxValue:
			b = append(b, maxFlag)
		default:
			return b, errors.Errorf("unsupport encode type %d", vals[i].HoTT())
		}
	}

	return b, errors.Trace(err)
}

// EstimateValueSize uses to estimate the value  size of the encoded values.
func EstimateValueSize(sc *stmtctx.StatementContext, val types.Causet) (int, error) {
	l := 0
	switch val.HoTT() {
	case types.HoTTInt64:
		l = valueSizeOfSignedInt(val.GetInt64())
	case types.HoTTUint64:
		l = valueSizeOfUnsignedInt(val.GetUint64())
	case types.HoTTFloat32, types.HoTTFloat64, types.HoTTMysqlTime, types.HoTTMysqlDuration:
		l = 9
	case types.HoTTString, types.HoTTBytes:
		l = valueSizeOfBytes(val.GetBytes())
	case types.HoTTMysqlDecimal:
		l = valueSizeOfDecimal(val.GetMysqlDecimal(), val.Length(), val.Frac()) + 1
	case types.HoTTMysqlEnum:
		l = valueSizeOfUnsignedInt(uint64(val.GetMysqlEnum().ToNumber()))
	case types.HoTTMysqlSet:
		l = valueSizeOfUnsignedInt(uint64(val.GetMysqlSet().ToNumber()))
	case types.HoTTMysqlBit, types.HoTTBinaryLiteral:
		val, err := val.GetBinaryLiteral().ToInt(sc)
		terror.Log(errors.Trace(err))
		l = valueSizeOfUnsignedInt(val)
	case types.HoTTMysqlJSON:
		l = 2 + len(val.GetMysqlJSON().Value)
	case types.HoTTNull, types.HoTTMinNotNull, types.HoTTMaxValue:
		l = 1
	default:
		return l, errors.Errorf("unsupported encode type %d", val.HoTT())
	}
	return l, nil
}

// EncodeMyALLEGROSQLTime encodes causet of `HoTTMysqlTime` to []byte.
func EncodeMyALLEGROSQLTime(sc *stmtctx.StatementContext, t types.Time, tp byte, b []byte) (_ []byte, err error) {
	// Encoding timestamp need to consider timezone. If it's not in UTC, transform to UTC first.
	// This is compatible with `PBToExpr > convertTime`, and interlock assumes the passed timestamp is in UTC as well.
	if tp == allegrosql.TypeUnspecified {
		tp = t.Type()
	}
	if tp == allegrosql.TypeTimestamp && sc.TimeZone != time.UTC {
		err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
		if err != nil {
			return nil, err
		}
	}
	var v uint64
	v, err = t.ToPackedUint()
	if err != nil {
		return nil, err
	}
	b = EncodeUint(b, v)
	return b, nil
}

func encodeString(b []byte, val types.Causet, comparable bool) []byte {
	if defCauslate.NewDefCauslationEnabled() && comparable {
		return encodeBytes(b, defCauslate.GetDefCauslator(val.DefCauslation()).Key(val.GetString()), true)
	}
	return encodeBytes(b, val.GetBytes(), comparable)
}

func encodeBytes(b []byte, v []byte, comparable bool) []byte {
	if comparable {
		b = append(b, bytesFlag)
		b = EncodeBytes(b, v)
	} else {
		b = append(b, compactBytesFlag)
		b = EncodeCompactBytes(b, v)
	}
	return b
}

func valueSizeOfBytes(v []byte) int {
	return valueSizeOfSignedInt(int64(len(v))) + len(v)
}

func sizeBytes(v []byte, comparable bool) int {
	if comparable {
		reallocSize := (len(v)/encGroupSize + 1) * (encGroupSize + 1)
		return 1 + reallocSize
	}
	reallocSize := binary.MaxVarintLen64 + len(v)
	return 1 + reallocSize
}

func encodeSignedInt(b []byte, v int64, comparable bool) []byte {
	if comparable {
		b = append(b, intFlag)
		b = EncodeInt(b, v)
	} else {
		b = append(b, varintFlag)
		b = EncodeVarint(b, v)
	}
	return b
}

func valueSizeOfSignedInt(v int64) int {
	if v < 0 {
		v = 0 - v - 1
	}
	// Flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 6
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func encodeUnsignedInt(b []byte, v uint64, comparable bool) []byte {
	if comparable {
		b = append(b, uintFlag)
		b = EncodeUint(b, v)
	} else {
		b = append(b, uvarintFlag)
		b = EncodeUvarint(b, v)
	}
	return b
}

func valueSizeOfUnsignedInt(v uint64) int {
	// Flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 7
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func sizeInt(comparable bool) int {
	if comparable {
		return 9
	}
	return 1 + binary.MaxVarintLen64
}

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
// For Decimal type, causet must set causet's length and frac.
func EncodeKey(sc *stmtctx.StatementContext, b []byte, v ...types.Causet) ([]byte, error) {
	return encode(sc, b, v, true)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(sc *stmtctx.StatementContext, b []byte, v ...types.Causet) ([]byte, error) {
	return encode(sc, b, v, false)
}

func encodeHashChunkRowIdx(sc *stmtctx.StatementContext, event chunk.Row, tp *types.FieldType, idx int) (flag byte, b []byte, err error) {
	if event.IsNull(idx) {
		flag = NilFlag
		return
	}
	switch tp.Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
		flag = uvarintFlag
		if !allegrosql.HasUnsignedFlag(tp.Flag) && event.GetInt64(idx) < 0 {
			flag = varintFlag
		}
		b = event.GetRaw(idx)
	case allegrosql.TypeFloat:
		flag = floatFlag
		f := float64(event.GetFloat32(idx))
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = (*[unsafe.Sizeof(f)]byte)(unsafe.Pointer(&f))[:]
	case allegrosql.TypeDouble:
		flag = floatFlag
		f := event.GetFloat64(idx)
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = (*[unsafe.Sizeof(f)]byte)(unsafe.Pointer(&f))[:]
	case allegrosql.TypeVarchar, allegrosql.TypeVarString, allegrosql.TypeString, allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		flag = compactBytesFlag
		b = event.GetBytes(idx)
		b = ConvertByDefCauslation(b, tp)
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		flag = uintFlag
		t := event.GetTime(idx)
		// Encoding timestamp need to consider timezone.
		// If it's not in UTC, transform to UTC first.
		if t.Type() == allegrosql.TypeTimestamp && sc.TimeZone != time.UTC {
			err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return
			}
		}
		var v uint64
		v, err = t.ToPackedUint()
		if err != nil {
			return
		}
		b = (*[unsafe.Sizeof(v)]byte)(unsafe.Pointer(&v))[:]
	case allegrosql.TypeDuration:
		flag = durationFlag
		// duration may have negative value, so we cannot use String to encode directly.
		b = event.GetRaw(idx)
	case allegrosql.TypeNewDecimal:
		flag = decimalFlag
		// If hash is true, we only consider the original value of this decimal and ignore it's precision.
		dec := event.GetMyDecimal(idx)
		b, err = dec.ToHashKey()
		if err != nil {
			return
		}
	case allegrosql.TypeEnum:
		flag = compactBytesFlag
		v := uint64(event.GetEnum(idx).ToNumber())
		str := tp.Elems[v-1]
		b = ConvertByDefCauslation(replog.Slice(str), tp)
	case allegrosql.TypeSet:
		flag = compactBytesFlag
		v := uint64(event.GetSet(idx).ToNumber())
		str := tp.Elems[v-1]
		b = ConvertByDefCauslation(replog.Slice(str), tp)
	case allegrosql.TypeBit:
		// We don't need to handle errors here since the literal is ensured to be able to causetstore in uint64 in convertToMysqlBit.
		flag = uvarintFlag
		v, err1 := types.BinaryLiteral(event.GetBytes(idx)).ToInt(sc)
		terror.Log(errors.Trace(err1))
		b = (*[unsafe.Sizeof(v)]byte)(unsafe.Pointer(&v))[:]
	case allegrosql.TypeJSON:
		flag = jsonFlag
		b = event.GetBytes(idx)
	default:
		return 0, nil, errors.Errorf("unsupport defCausumn type for encode %d", tp.Tp)
	}
	return
}

// HashChunkDeferredCausets writes the encoded value of each event's defCausumn, which of index `defCausIdx`, to h.
func HashChunkDeferredCausets(sc *stmtctx.StatementContext, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, defCausIdx int, buf []byte, isNull []bool) (err error) {
	return HashChunkSelected(sc, h, chk, tp, defCausIdx, buf, isNull, nil, false)
}

// HashChunkSelected writes the encoded value of selected event's defCausumn, which of index `defCausIdx`, to h.
// sel indicates which rows are selected. If it is nil, all rows are selected.
func HashChunkSelected(sc *stmtctx.StatementContext, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, defCausIdx int, buf []byte,
	isNull, sel []bool, ignoreNull bool) (err error) {
	var b []byte
	defCausumn := chk.DeferredCauset(defCausIdx)
	rows := chk.NumRows()
	switch tp.Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear:
		i64s := defCausumn.Int64s()
		for i, v := range i64s {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = uvarintFlag
				if !allegrosql.HasUnsignedFlag(tp.Flag) && v < 0 {
					buf[0] = varintFlag
				}
				b = defCausumn.GetRaw(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeFloat:
		f32s := defCausumn.Float32s()
		for i, f := range f32s {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = floatFlag
				d := float64(f)
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if d == 0 {
					d = 0
				}
				b = (*[sizeFloat64]byte)(unsafe.Pointer(&d))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeDouble:
		f64s := defCausumn.Float64s()
		for i, f := range f64s {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = floatFlag
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if f == 0 {
					f = 0
				}
				b = (*[sizeFloat64]byte)(unsafe.Pointer(&f))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeVarchar, allegrosql.TypeVarString, allegrosql.TypeString, allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				b = defCausumn.GetBytes(i)
				b = ConvertByDefCauslation(b, tp)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		ts := defCausumn.Times()
		for i, t := range ts {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = uintFlag
				// Encoding timestamp need to consider timezone.
				// If it's not in UTC, transform to UTC first.
				if t.Type() == allegrosql.TypeTimestamp && sc.TimeZone != time.UTC {
					err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
					if err != nil {
						return
					}
				}
				var v uint64
				v, err = t.ToPackedUint()
				if err != nil {
					return
				}
				b = (*[sizeUint64]byte)(unsafe.Pointer(&v))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeDuration:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = durationFlag
				// duration may have negative value, so we cannot use String to encode directly.
				b = defCausumn.GetRaw(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeNewDecimal:
		ds := defCausumn.Decimals()
		for i, d := range ds {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = decimalFlag
				// If hash is true, we only consider the original value of this decimal and ignore it's precision.
				b, err = d.ToHashKey()
				if err != nil {
					return
				}
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeEnum:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				v := uint64(defCausumn.GetEnum(i).ToNumber())
				str := tp.Elems[v-1]
				b = ConvertByDefCauslation(replog.Slice(str), tp)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeSet:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				v := uint64(defCausumn.GetSet(i).ToNumber())
				str := tp.Elems[v-1]
				b = ConvertByDefCauslation(replog.Slice(str), tp)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeBit:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				// We don't need to handle errors here since the literal is ensured to be able to causetstore in uint64 in convertToMysqlBit.
				buf[0] = uvarintFlag
				v, err1 := types.BinaryLiteral(defCausumn.GetBytes(i)).ToInt(sc)
				terror.Log(errors.Trace(err1))
				b = (*[sizeUint64]byte)(unsafe.Pointer(&v))[:]
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeJSON:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			if defCausumn.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = jsonFlag
				b = defCausumn.GetBytes(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error..
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case allegrosql.TypeNull:
		for i := 0; i < rows; i++ {
			if sel != nil && !sel[i] {
				continue
			}
			isNull[i] = !ignoreNull
			buf[0] = NilFlag
			_, _ = h[i].Write(buf)
		}
	default:
		return errors.Errorf("unsupport defCausumn type for encode %d", tp.Tp)
	}
	return
}

// HashChunkRow writes the encoded values to w.
// If two rows are logically equal, it will generate the same bytes.
func HashChunkRow(sc *stmtctx.StatementContext, w io.Writer, event chunk.Row, allTypes []*types.FieldType, defCausIdx []int, buf []byte) (err error) {
	var b []byte
	for _, idx := range defCausIdx {
		buf[0], b, err = encodeHashChunkRowIdx(sc, event, allTypes[idx], idx)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = w.Write(buf)
		if err != nil {
			return
		}
		_, err = w.Write(b)
		if err != nil {
			return
		}
	}
	return err
}

// EqualChunkRow returns a boolean reporting whether row1 and row2
// with their types and defCausumn index are logically equal.
func EqualChunkRow(sc *stmtctx.StatementContext,
	row1 chunk.Row, allTypes1 []*types.FieldType, defCausIdx1 []int,
	row2 chunk.Row, allTypes2 []*types.FieldType, defCausIdx2 []int,
) (bool, error) {
	for i := range defCausIdx1 {
		idx1, idx2 := defCausIdx1[i], defCausIdx2[i]
		flag1, b1, err := encodeHashChunkRowIdx(sc, row1, allTypes1[idx1], idx1)
		if err != nil {
			return false, errors.Trace(err)
		}
		flag2, b2, err := encodeHashChunkRowIdx(sc, row2, allTypes2[idx2], idx2)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !(flag1 == flag2 && bytes.Equal(b1, b2)) {
			return false, nil
		}
	}
	return true, nil
}

// Decode decodes values from a byte slice generated with EncodeKey or EncodeValue
// before.
// size is the size of decoded causet slice.
func Decode(b []byte, size int) ([]types.Causet, error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}

	var (
		err    error
		values = make([]types.Causet, 0, size)
	)

	for len(b) > 0 {
		var d types.Causet
		b, d, err = DecodeOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}

		values = append(values, d)
	}

	return values, nil
}

// DecodeRange decodes the range values from a byte slice that generated by EncodeKey.
// It handles some special values like `MinNotNull` and `MaxValueCauset`.
// loc can be nil and only used in when the corresponding type is `allegrosql.TypeTimestamp`.
func DecodeRange(b []byte, size int, idxDeferredCausetTypes []byte, loc *time.Location) ([]types.Causet, []byte, error) {
	if len(b) < 1 {
		return nil, b, errors.New("invalid encoded key: length of key is zero")
	}

	var (
		err    error
		values = make([]types.Causet, 0, size)
	)

	i := 0
	for len(b) > 1 {
		var d types.Causet
		if idxDeferredCausetTypes == nil {
			b, d, err = DecodeOne(b)
		} else {
			if i >= len(idxDeferredCausetTypes) {
				return values, b, errors.New("invalid length of index's defCausumns")
			}
			if idxDeferredCausetTypes[i] == allegrosql.TypeDatetime || idxDeferredCausetTypes[i] == allegrosql.TypeTimestamp || idxDeferredCausetTypes[i] == allegrosql.TypeDate {
				b, d, err = DecodeAsDateTime(b, idxDeferredCausetTypes[i], loc)
			} else {
				b, d, err = DecodeOne(b)
			}
		}
		if err != nil {
			return values, b, errors.Trace(err)
		}
		values = append(values, d)
		i++
	}

	if len(b) == 1 {
		switch b[0] {
		case NilFlag:
			values = append(values, types.Causet{})
		case bytesFlag:
			values = append(values, types.MinNotNullCauset())
		// `maxFlag + 1` for PrefixNext
		case maxFlag, maxFlag + 1:
			values = append(values, types.MaxValueCauset())
		default:
			return values, b, errors.Errorf("invalid encoded key flag %v", b[0])
		}
	}
	return values, nil, nil
}

// DecodeOne decodes on causet from a byte slice generated with EncodeKey or EncodeValue.
func DecodeOne(b []byte) (remain []byte, d types.Causet, err error) {
	if len(b) < 1 {
		return nil, d, errors.New("invalid encoded key")
	}
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		var v int64
		b, v, err = DecodeInt(b)
		d.SetInt64(v)
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		d.SetUint64(v)
	case varintFlag:
		var v int64
		b, v, err = DecodeVarint(b)
		d.SetInt64(v)
	case uvarintFlag:
		var v uint64
		b, v, err = DecodeUvarint(b)
		d.SetUint64(v)
	case floatFlag:
		var v float64
		b, v, err = DecodeFloat(b)
		d.SetFloat64(v)
	case bytesFlag:
		var v []byte
		b, v, err = DecodeBytes(b, nil)
		d.SetBytes(v)
	case compactBytesFlag:
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		d.SetBytes(v)
	case decimalFlag:
		var (
			dec             *types.MyDecimal
			precision, frac int
		)
		b, dec, precision, frac, err = DecodeDecimal(b)
		if err == nil {
			d.SetMysqlDecimal(dec)
			d.SetLength(precision)
			d.SetFrac(frac)
		}
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err == nil {
			// use max fsp, let outer to do round manually.
			v := types.Duration{Duration: time.Duration(r), Fsp: types.MaxFsp}
			d.SetMysqlDuration(v)
		}
	case jsonFlag:
		var size int
		size, err = json.PeekBytesAsJSON(b)
		if err != nil {
			return b, d, err
		}
		j := json.BinaryJSON{TypeCode: b[0], Value: b[1:size]}
		d.SetMysqlJSON(j)
		b = b[size:]
	case NilFlag:
	default:
		return b, d, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return b, d, errors.Trace(err)
	}
	return b, d, nil
}

// DecodeAsDateTime decodes on causet from []byte of `HoTTMysqlTime`.
func DecodeAsDateTime(b []byte, tp byte, loc *time.Location) (remain []byte, d types.Causet, err error) {
	if len(b) < 1 {
		return nil, d, errors.New("invalid encoded key")
	}
	flag := b[0]
	b = b[1:]
	switch flag {
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		if err != nil {
			return b, d, err
		}
		t := types.NewTime(types.ZeroCoreTime, tp, 0)
		err = t.FromPackedUint(v)
		if err == nil {
			if tp == allegrosql.TypeTimestamp && !t.IsZero() && loc != nil {
				err = t.ConvertTimeZone(time.UTC, loc)
				if err != nil {
					return b, d, err
				}
			}
			d.SetMysqlTime(t)
		}
	default:
		return b, d, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return b, d, errors.Trace(err)
	}
	return b, d, nil
}

// CutOne cuts the first encoded value from b.
// It will return the first encoded item and the remains as byte slice.
func CutOne(b []byte) (data []byte, remain []byte, err error) {
	l, err := peek(b)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return b[:l], b[l:], nil
}

// CutDeferredCausetID cuts the defCausumn ID from b.
// It will return the remains as byte slice and defCausumn ID
func CutDeferredCausetID(b []byte) (remain []byte, n int64, err error) {
	if len(b) < 1 {
		return nil, 0, errors.New("invalid encoded key")
	}
	// skip the flag
	b = b[1:]
	return DecodeVarint(b)
}

// SetRawValues set raw causet values from a event data.
func SetRawValues(data []byte, values []types.Causet) error {
	for i := 0; i < len(values); i++ {
		l, err := peek(data)
		if err != nil {
			return errors.Trace(err)
		}
		values[i].SetRaw(data[:l:l])
		data = data[l:]
	}
	return nil
}

// peek peeks the first encoded value from b and returns its length.
func peek(b []byte) (length int, err error) {
	if len(b) < 1 {
		return 0, errors.New("invalid encoded key")
	}
	flag := b[0]
	length++
	b = b[1:]
	var l int
	switch flag {
	case NilFlag:
	case intFlag, uintFlag, floatFlag, durationFlag:
		// Those types are stored in 8 bytes.
		l = 8
	case bytesFlag:
		l, err = peekBytes(b)
	case compactBytesFlag:
		l, err = peekCompactBytes(b)
	case decimalFlag:
		l, err = types.DecimalPeak(b)
	case varintFlag:
		l, err = peekVarint(b)
	case uvarintFlag:
		l, err = peekUvarint(b)
	case jsonFlag:
		l, err = json.PeekBytesAsJSON(b)
	default:
		return 0, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	length += l
	return
}

func peekBytes(b []byte) (int, error) {
	offset := 0
	for {
		if len(b) < offset+encGroupSize+1 {
			return 0, errors.New("insufficient bytes to decode value")
		}
		// The byte slice is encoded into many groups.
		// For each group, there are 8 bytes for data and 1 byte for marker.
		marker := b[offset+encGroupSize]
		padCount := encMarker - marker
		offset += encGroupSize + 1
		// When padCount is not zero, it means we get the end of the byte slice.
		if padCount != 0 {
			break
		}
	}
	return offset, nil
}

func peekCompactBytes(b []byte) (int, error) {
	// Get length.
	v, n := binary.Varint(b)
	vi := int(v)
	if n < 0 {
		return 0, errors.New("value larger than 64 bits")
	} else if n == 0 {
		return 0, errors.New("insufficient bytes to decode value")
	}
	if len(b) < vi+n {
		return 0, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	return n + vi, nil
}

func peekVarint(b []byte) (int, error) {
	_, n := binary.Varint(b)
	if n < 0 {
		return 0, errors.New("value larger than 64 bits")
	}
	return n, nil
}

func peekUvarint(b []byte) (int, error) {
	_, n := binary.Uvarint(b)
	if n < 0 {
		return 0, errors.New("value larger than 64 bits")
	}
	return n, nil
}

// Decoder is used to decode value to chunk.
type Decoder struct {
	chk      *chunk.Chunk
	timezone *time.Location

	// buf is only used for DecodeBytes to avoid the cost of makeslice.
	buf []byte
}

// NewDecoder creates a Decoder.
func NewDecoder(chk *chunk.Chunk, timezone *time.Location) *Decoder {
	return &Decoder{
		chk:      chk,
		timezone: timezone,
	}
}

// DecodeOne decodes one value to chunk and returns the remained bytes.
func (decoder *Decoder) DecodeOne(b []byte, defCausIdx int, ft *types.FieldType) (remain []byte, err error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}
	chk := decoder.chk
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		var v int64
		b, v, err = DecodeInt(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		appendIntToChunk(v, chk, defCausIdx, ft)
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = appendUintToChunk(v, chk, defCausIdx, ft, decoder.timezone)
	case varintFlag:
		var v int64
		b, v, err = DecodeVarint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		appendIntToChunk(v, chk, defCausIdx, ft)
	case uvarintFlag:
		var v uint64
		b, v, err = DecodeUvarint(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = appendUintToChunk(v, chk, defCausIdx, ft, decoder.timezone)
	case floatFlag:
		var v float64
		b, v, err = DecodeFloat(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		appendFloatToChunk(v, chk, defCausIdx, ft)
	case bytesFlag:
		b, decoder.buf, err = DecodeBytes(b, decoder.buf)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendBytes(defCausIdx, decoder.buf)
	case compactBytesFlag:
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendBytes(defCausIdx, v)
	case decimalFlag:
		var dec *types.MyDecimal
		var frac int
		b, dec, _, frac, err = DecodeDecimal(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ft.Decimal != types.UnspecifiedLength && frac > ft.Decimal {
			to := new(types.MyDecimal)
			err := dec.Round(to, ft.Decimal, types.ModeHalfEven)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dec = to
		}
		chk.AppendMyDecimal(defCausIdx, dec)
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		v := types.Duration{Duration: time.Duration(r), Fsp: int8(ft.Decimal)}
		chk.AppendDuration(defCausIdx, v)
	case jsonFlag:
		var size int
		size, err = json.PeekBytesAsJSON(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chk.AppendJSON(defCausIdx, json.BinaryJSON{TypeCode: b[0], Value: b[1:size]})
		b = b[size:]
	case NilFlag:
		chk.AppendNull(defCausIdx)
	default:
		return nil, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b, nil
}

func appendIntToChunk(val int64, chk *chunk.Chunk, defCausIdx int, ft *types.FieldType) {
	switch ft.Tp {
	case allegrosql.TypeDuration:
		v := types.Duration{Duration: time.Duration(val), Fsp: int8(ft.Decimal)}
		chk.AppendDuration(defCausIdx, v)
	default:
		chk.AppendInt64(defCausIdx, val)
	}
}

func appendUintToChunk(val uint64, chk *chunk.Chunk, defCausIdx int, ft *types.FieldType, loc *time.Location) error {
	switch ft.Tp {
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.Tp, int8(ft.Decimal))
		var err error
		err = t.FromPackedUint(val)
		if err != nil {
			return errors.Trace(err)
		}
		if ft.Tp == allegrosql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return errors.Trace(err)
			}
		}
		chk.AppendTime(defCausIdx, t)
	case allegrosql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, val)
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(defCausIdx, enum)
	case allegrosql.TypeSet:
		set, err := types.ParseSetValue(ft.Elems, val)
		if err != nil {
			return errors.Trace(err)
		}
		chk.AppendSet(defCausIdx, set)
	case allegrosql.TypeBit:
		byteSize := (ft.Flen + 7) >> 3
		chk.AppendBytes(defCausIdx, types.NewBinaryLiteralFromUint(val, byteSize))
	default:
		chk.AppendUint64(defCausIdx, val)
	}
	return nil
}

func appendFloatToChunk(val float64, chk *chunk.Chunk, defCausIdx int, ft *types.FieldType) {
	if ft.Tp == allegrosql.TypeFloat {
		chk.AppendFloat32(defCausIdx, float32(val))
	} else {
		chk.AppendFloat64(defCausIdx, val)
	}
}

// HashGroupKey encodes each event of this defCausumn and append encoded data into buf.
// Only use in the aggregate executor.
func HashGroupKey(sc *stmtctx.StatementContext, n int, defCaus *chunk.DeferredCauset, buf [][]byte, ft *types.FieldType) ([][]byte, error) {
	var err error
	switch ft.EvalType() {
	case types.ETInt:
		i64s := defCaus.Int64s()
		for i := 0; i < n; i++ {
			if defCaus.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = encodeSignedInt(buf[i], i64s[i], false)
			}
		}
	case types.ETReal:
		f64s := defCaus.Float64s()
		for i := 0; i < n; i++ {
			if defCaus.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], floatFlag)
				buf[i] = EncodeFloat(buf[i], f64s[i])
			}
		}
	case types.ETDecimal:
		ds := defCaus.Decimals()
		for i := 0; i < n; i++ {
			if defCaus.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], decimalFlag)
				buf[i], err = EncodeDecimal(buf[i], &ds[i], ft.Flen, ft.Decimal)
				if terror.ErrorEqual(err, types.ErrTruncated) {
					err = sc.HandleTruncate(err)
				} else if terror.ErrorEqual(err, types.ErrOverflow) {
					err = sc.HandleOverflow(err, err)
				}
				if err != nil {
					return nil, err
				}
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		ts := defCaus.Times()
		for i := 0; i < n; i++ {
			if defCaus.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], uintFlag)
				buf[i], err = EncodeMyALLEGROSQLTime(sc, ts[i], allegrosql.TypeUnspecified, buf[i])
				if err != nil {
					return nil, err
				}
			}
		}
	case types.ETDuration:
		ds := defCaus.GoDurations()
		for i := 0; i < n; i++ {
			if defCaus.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], durationFlag)
				buf[i] = EncodeInt(buf[i], int64(ds[i]))
			}
		}
	case types.ETJson:
		for i := 0; i < n; i++ {
			if defCaus.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = append(buf[i], jsonFlag)
				j := defCaus.GetJSON(i)
				buf[i] = append(buf[i], j.TypeCode)
				buf[i] = append(buf[i], j.Value...)
			}
		}
	case types.ETString:
		for i := 0; i < n; i++ {
			if defCaus.IsNull(i) {
				buf[i] = append(buf[i], NilFlag)
			} else {
				buf[i] = encodeBytes(buf[i], ConvertByDefCauslation(defCaus.GetBytes(i), ft), false)
			}
		}
	default:
		return nil, errors.New(fmt.Sprintf("invalid eval type %v", ft.EvalType()))
	}
	return buf, nil
}

// ConvertByDefCauslation converts these bytes according to its defCauslation.
func ConvertByDefCauslation(raw []byte, tp *types.FieldType) []byte {
	defCauslator := defCauslate.GetDefCauslator(tp.DefCauslate)
	return defCauslator.Key(string(replog.String(raw)))
}

// ConvertByDefCauslationStr converts this string according to its defCauslation.
func ConvertByDefCauslationStr(str string, tp *types.FieldType) string {
	defCauslator := defCauslate.GetDefCauslator(tp.DefCauslate)
	return string(replog.String(defCauslator.Key(str)))
}
