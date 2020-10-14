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
	"math"
	"sort"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
)

// Encoder is used to encode a event.
type Encoder struct {
	event
	tempDefCausIDs []int64
	values     []*types.Causet
	// Enable indicates whether this encoder should be use.
	Enable bool
}

// Encode encodes a event from a datums slice.
func (encoder *Encoder) Encode(sc *stmtctx.StatementContext, defCausIDs []int64, values []types.Causet, buf []byte) ([]byte, error) {
	encoder.reset()
	encoder.appendDefCausVals(defCausIDs, values)
	numDefCauss, notNullIdx := encoder.reformatDefCauss()
	err := encoder.encodeRowDefCauss(sc, numDefCauss, notNullIdx)
	if err != nil {
		return nil, err
	}
	return encoder.event.toBytes(buf[:0]), nil
}

func (encoder *Encoder) reset() {
	encoder.large = false
	encoder.numNotNullDefCauss = 0
	encoder.numNullDefCauss = 0
	encoder.data = encoder.data[:0]
	encoder.tempDefCausIDs = encoder.tempDefCausIDs[:0]
	encoder.values = encoder.values[:0]
	encoder.offsets32 = encoder.offsets32[:0]
	encoder.offsets = encoder.offsets[:0]
}

func (encoder *Encoder) appendDefCausVals(defCausIDs []int64, values []types.Causet) {
	for i, defCausID := range defCausIDs {
		encoder.appendDefCausVal(defCausID, &values[i])
	}
}

func (encoder *Encoder) appendDefCausVal(defCausID int64, d *types.Causet) {
	if defCausID > 255 {
		encoder.large = true
	}
	if d.IsNull() {
		encoder.numNullDefCauss++
	} else {
		encoder.numNotNullDefCauss++
	}
	encoder.tempDefCausIDs = append(encoder.tempDefCausIDs, defCausID)
	encoder.values = append(encoder.values, d)
}

func (encoder *Encoder) reformatDefCauss() (numDefCauss, notNullIdx int) {
	r := &encoder.event
	numDefCauss = len(encoder.tempDefCausIDs)
	nullIdx := numDefCauss - int(r.numNullDefCauss)
	notNullIdx = 0
	if r.large {
		r.initDefCausIDs32()
		r.initOffsets32()
	} else {
		r.initDefCausIDs()
		r.initOffsets()
	}
	for i, defCausID := range encoder.tempDefCausIDs {
		if encoder.values[i].IsNull() {
			if r.large {
				r.defCausIDs32[nullIdx] = uint32(defCausID)
			} else {
				r.defCausIDs[nullIdx] = byte(defCausID)
			}
			nullIdx++
		} else {
			if r.large {
				r.defCausIDs32[notNullIdx] = uint32(defCausID)
			} else {
				r.defCausIDs[notNullIdx] = byte(defCausID)
			}
			encoder.values[notNullIdx] = encoder.values[i]
			notNullIdx++
		}
	}
	if r.large {
		largeNotNullSorter := (*largeNotNullSorter)(encoder)
		sort.Sort(largeNotNullSorter)
		if r.numNullDefCauss > 0 {
			largeNullSorter := (*largeNullSorter)(encoder)
			sort.Sort(largeNullSorter)
		}
	} else {
		smallNotNullSorter := (*smallNotNullSorter)(encoder)
		sort.Sort(smallNotNullSorter)
		if r.numNullDefCauss > 0 {
			smallNullSorter := (*smallNullSorter)(encoder)
			sort.Sort(smallNullSorter)
		}
	}
	return
}

func (encoder *Encoder) encodeRowDefCauss(sc *stmtctx.StatementContext, numDefCauss, notNullIdx int) error {
	r := &encoder.event
	for i := 0; i < notNullIdx; i++ {
		d := encoder.values[i]
		var err error
		r.data, err = encodeValueCauset(sc, d, r.data)
		if err != nil {
			return err
		}
		// handle convert to large
		if len(r.data) > math.MaxUint16 && !r.large {
			r.initDefCausIDs32()
			for j := 0; j < numDefCauss; j++ {
				r.defCausIDs32[j] = uint32(r.defCausIDs[j])
			}
			r.initOffsets32()
			for j := 0; j <= i; j++ {
				r.offsets32[j] = uint32(r.offsets[j])
			}
			r.large = true
		}
		if r.large {
			r.offsets32[i] = uint32(len(r.data))
		} else {
			r.offsets[i] = uint16(len(r.data))
		}
	}
	return nil
}

// encodeValueCauset encodes one event causet entry into bytes.
// due to encode as value, this method will flatten value type like blockcodec.flatten
func encodeValueCauset(sc *stmtctx.StatementContext, d *types.Causet, buffer []byte) (nBuffer []byte, err error) {
	switch d.HoTT() {
	case types.HoTTInt64:
		buffer = encodeInt(buffer, d.GetInt64())
	case types.HoTTUint64:
		buffer = encodeUint(buffer, d.GetUint64())
	case types.HoTTString, types.HoTTBytes:
		buffer = append(buffer, d.GetBytes()...)
	case types.HoTTMysqlTime:
		// for allegrosql datetime, timestamp and date type
		t := d.GetMysqlTime()
		if t.Type() == allegrosql.TypeTimestamp && sc != nil && sc.TimeZone != time.UTC {
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
		buffer = encodeUint(buffer, v)
	case types.HoTTMysqlDuration:
		buffer = encodeInt(buffer, int64(d.GetMysqlDuration().Duration))
	case types.HoTTMysqlEnum:
		buffer = encodeUint(buffer, d.GetMysqlEnum().Value)
	case types.HoTTMysqlSet:
		buffer = encodeUint(buffer, d.GetMysqlSet().Value)
	case types.HoTTBinaryLiteral, types.HoTTMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to causetstore in uint64 in convertToMysqlBit.
		var val uint64
		val, err = d.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			return
		}
		buffer = encodeUint(buffer, val)
	case types.HoTTFloat32, types.HoTTFloat64:
		buffer = codec.EncodeFloat(buffer, d.GetFloat64())
	case types.HoTTMysqlDecimal:
		buffer, err = codec.EncodeDecimal(buffer, d.GetMysqlDecimal(), d.Length(), d.Frac())
		if err != nil && sc != nil {
			if terror.ErrorEqual(err, types.ErrTruncated) {
				err = sc.HandleTruncate(err)
			} else if terror.ErrorEqual(err, types.ErrOverflow) {
				err = sc.HandleOverflow(err, err)
			}
		}
	case types.HoTTMysqlJSON:
		j := d.GetMysqlJSON()
		buffer = append(buffer, j.TypeCode)
		buffer = append(buffer, j.Value...)
	default:
		err = errors.Errorf("unsupport encode type %d", d.HoTT())
	}
	nBuffer = buffer
	return
}
