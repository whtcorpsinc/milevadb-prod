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
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
)

// Row represents a event of data, can be used to access values.
type Row struct {
	c   *Chunk
	idx int
}

// Chunk returns the Chunk which the event belongs to.
func (r Row) Chunk() *Chunk {
	return r.c
}

// IsEmpty returns true if the Row is empty.
func (r Row) IsEmpty() bool {
	return r == Row{}
}

// Idx returns the event index of Chunk.
func (r Row) Idx() int {
	return r.idx
}

// Len returns the number of values in the event.
func (r Row) Len() int {
	return r.c.NumDefCauss()
}

// GetInt64 returns the int64 value with the defCausIdx.
func (r Row) GetInt64(defCausIdx int) int64 {
	return r.c.defCausumns[defCausIdx].GetInt64(r.idx)
}

// GetUint64 returns the uint64 value with the defCausIdx.
func (r Row) GetUint64(defCausIdx int) uint64 {
	return r.c.defCausumns[defCausIdx].GetUint64(r.idx)
}

// GetFloat32 returns the float32 value with the defCausIdx.
func (r Row) GetFloat32(defCausIdx int) float32 {
	return r.c.defCausumns[defCausIdx].GetFloat32(r.idx)
}

// GetFloat64 returns the float64 value with the defCausIdx.
func (r Row) GetFloat64(defCausIdx int) float64 {
	return r.c.defCausumns[defCausIdx].GetFloat64(r.idx)
}

// GetString returns the string value with the defCausIdx.
func (r Row) GetString(defCausIdx int) string {
	return r.c.defCausumns[defCausIdx].GetString(r.idx)
}

// GetBytes returns the bytes value with the defCausIdx.
func (r Row) GetBytes(defCausIdx int) []byte {
	return r.c.defCausumns[defCausIdx].GetBytes(r.idx)
}

// GetTime returns the Time value with the defCausIdx.
func (r Row) GetTime(defCausIdx int) types.Time {
	return r.c.defCausumns[defCausIdx].GetTime(r.idx)
}

// GetDuration returns the Duration value with the defCausIdx.
func (r Row) GetDuration(defCausIdx int, fillFsp int) types.Duration {
	return r.c.defCausumns[defCausIdx].GetDuration(r.idx, fillFsp)
}

func (r Row) getNameValue(defCausIdx int) (string, uint64) {
	return r.c.defCausumns[defCausIdx].getNameValue(r.idx)
}

// GetEnum returns the Enum value with the defCausIdx.
func (r Row) GetEnum(defCausIdx int) types.Enum {
	return r.c.defCausumns[defCausIdx].GetEnum(r.idx)
}

// GetSet returns the Set value with the defCausIdx.
func (r Row) GetSet(defCausIdx int) types.Set {
	return r.c.defCausumns[defCausIdx].GetSet(r.idx)
}

// GetMyDecimal returns the MyDecimal value with the defCausIdx.
func (r Row) GetMyDecimal(defCausIdx int) *types.MyDecimal {
	return r.c.defCausumns[defCausIdx].GetDecimal(r.idx)
}

// GetJSON returns the JSON value with the defCausIdx.
func (r Row) GetJSON(defCausIdx int) json.BinaryJSON {
	return r.c.defCausumns[defCausIdx].GetJSON(r.idx)
}

// GetCausetRow converts chunk.Row to types.CausetRow.
// Keep in mind that GetCausetRow has a reference to r.c, which is a chunk,
// this function works only if the underlying chunk is valid or unchanged.
func (r Row) GetCausetRow(fields []*types.FieldType) []types.Causet {
	datumRow := make([]types.Causet, 0, r.c.NumDefCauss())
	for defCausIdx := 0; defCausIdx < r.c.NumDefCauss(); defCausIdx++ {
		causet := r.GetCauset(defCausIdx, fields[defCausIdx])
		datumRow = append(datumRow, causet)
	}
	return datumRow
}

// GetCauset implements the chunk.Row interface.
func (r Row) GetCauset(defCausIdx int, tp *types.FieldType) types.Causet {
	var d types.Causet
	switch tp.Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		if !r.IsNull(defCausIdx) {
			if allegrosql.HasUnsignedFlag(tp.Flag) {
				d.SetUint64(r.GetUint64(defCausIdx))
			} else {
				d.SetInt64(r.GetInt64(defCausIdx))
			}
		}
	case allegrosql.TypeYear:
		// FIXBUG: because insert type of TypeYear is definite int64, so we regardless of the unsigned flag.
		if !r.IsNull(defCausIdx) {
			d.SetInt64(r.GetInt64(defCausIdx))
		}
	case allegrosql.TypeFloat:
		if !r.IsNull(defCausIdx) {
			d.SetFloat32(r.GetFloat32(defCausIdx))
		}
	case allegrosql.TypeDouble:
		if !r.IsNull(defCausIdx) {
			d.SetFloat64(r.GetFloat64(defCausIdx))
		}
	case allegrosql.TypeVarchar, allegrosql.TypeVarString, allegrosql.TypeString:
		if !r.IsNull(defCausIdx) {
			d.SetString(r.GetString(defCausIdx), tp.DefCauslate)
		}
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		if !r.IsNull(defCausIdx) {
			d.SetBytes(r.GetBytes(defCausIdx))
		}
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		if !r.IsNull(defCausIdx) {
			d.SetMysqlTime(r.GetTime(defCausIdx))
		}
	case allegrosql.TypeDuration:
		if !r.IsNull(defCausIdx) {
			duration := r.GetDuration(defCausIdx, tp.Decimal)
			d.SetMysqlDuration(duration)
		}
	case allegrosql.TypeNewDecimal:
		if !r.IsNull(defCausIdx) {
			d.SetMysqlDecimal(r.GetMyDecimal(defCausIdx))
			d.SetLength(tp.Flen)
			// If tp.Decimal is unspecified(-1), we should set it to the real
			// fraction length of the decimal value, if not, the d.Frac will
			// be set to MAX_UINT16 which will cause unexpected BadNumber error
			// when encoding.
			if tp.Decimal == types.UnspecifiedLength {
				d.SetFrac(d.Frac())
			} else {
				d.SetFrac(tp.Decimal)
			}
		}
	case allegrosql.TypeEnum:
		if !r.IsNull(defCausIdx) {
			d.SetMysqlEnum(r.GetEnum(defCausIdx), tp.DefCauslate)
		}
	case allegrosql.TypeSet:
		if !r.IsNull(defCausIdx) {
			d.SetMysqlSet(r.GetSet(defCausIdx), tp.DefCauslate)
		}
	case allegrosql.TypeBit:
		if !r.IsNull(defCausIdx) {
			d.SetMysqlBit(r.GetBytes(defCausIdx))
		}
	case allegrosql.TypeJSON:
		if !r.IsNull(defCausIdx) {
			d.SetMysqlJSON(r.GetJSON(defCausIdx))
		}
	}
	return d
}

// GetRaw returns the underlying raw bytes with the defCausIdx.
func (r Row) GetRaw(defCausIdx int) []byte {
	return r.c.defCausumns[defCausIdx].GetRaw(r.idx)
}

// IsNull returns if the causet in the chunk.Row is null.
func (r Row) IsNull(defCausIdx int) bool {
	return r.c.defCausumns[defCausIdx].IsNull(r.idx)
}

// CopyConstruct creates a new event and copies this event's data into it.
func (r Row) CopyConstruct() Row {
	newChk := renewWithCapacity(r.c, 1, 1)
	newChk.AppendRow(r)
	return newChk.GetRow(0)
}
