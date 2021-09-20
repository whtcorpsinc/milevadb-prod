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

package blockcodec

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
	"unicode/utf8"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/structure"
	"github.com/whtcorpsinc/milevadb/types"
)

var (
	errInvalidKey       = terror.ClassXEval.New(errno.ErrInvalidKey, errno.MyALLEGROSQLErrName[errno.ErrInvalidKey])
	errInvalidRecordKey = terror.ClassXEval.New(errno.ErrInvalidRecordKey, errno.MyALLEGROSQLErrName[errno.ErrInvalidRecordKey])
	errInvalidIndexKey  = terror.ClassXEval.New(errno.ErrInvalidIndexKey, errno.MyALLEGROSQLErrName[errno.ErrInvalidIndexKey])
)

var (
	blockPrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
	spacetimePrefix = []byte{'m'}
)

const (
	idLen     = 8
	prefixLen = 1 + idLen /*blockID*/ + 2
	// RecordRowKeyLen is public for calculating avgerage event size.
	RecordRowKeyLen       = prefixLen + idLen /*handle*/
	blockPrefixLength     = 1
	recordPrefixSepLength = 2
	spacetimePrefixLength = 1
	// MaxOldEncodeValueLen is the maximum len of the old encoding of index value.
	MaxOldEncodeValueLen = 9

	// CommonHandleFlag is the flag used to decode the common handle in an unique index value.
	CommonHandleFlag byte = 127
	// PartitionIDFlag is the flag used to decode the partition ID in global index value.
	PartitionIDFlag byte = 126
	// RestoreDataFlag is the flag that RestoreData begin with.
	// See rowcodec.CausetEncoder.Encode and rowcodec.event.toBytes
	RestoreDataFlag byte = rowcodec.CodecVer
)

// TableSplitKeyLen is the length of key 't{block_id}' which is used for causet split.
const TableSplitKeyLen = 1 + idLen

// TablePrefix returns causet's prefix 't'.
func TablePrefix() []byte {
	return blockPrefix
}

// EncodeRowKey encodes the causet id and record handle into a ekv.Key
func EncodeRowKey(blockID int64, encodedHandle []byte) ekv.Key {
	buf := make([]byte, 0, prefixLen+len(encodedHandle))
	buf = appendTableRecordPrefix(buf, blockID)
	buf = append(buf, encodedHandle...)
	return buf
}

// EncodeRowKeyWithHandle encodes the causet id, event handle into a ekv.Key
func EncodeRowKeyWithHandle(blockID int64, handle ekv.Handle) ekv.Key {
	return EncodeRowKey(blockID, handle.Encoded())
}

// CutRowKeyPrefix cuts the event key prefix.
func CutRowKeyPrefix(key ekv.Key) []byte {
	return key[prefixLen:]
}

// EncodeRecordKey encodes the recordPrefix, event handle into a ekv.Key.
func EncodeRecordKey(recordPrefix ekv.Key, h ekv.Handle) ekv.Key {
	buf := make([]byte, 0, len(recordPrefix)+h.Len())
	buf = append(buf, recordPrefix...)
	buf = append(buf, h.Encoded()...)
	return buf
}

func hasTablePrefix(key ekv.Key) bool {
	return key[0] == blockPrefix[0]
}

func hasRecordPrefixSep(key ekv.Key) bool {
	return key[0] == recordPrefixSep[0] && key[1] == recordPrefixSep[1]
}

// DecodeRecordKey decodes the key and gets the blockID, handle.
func DecodeRecordKey(key ekv.Key) (blockID int64, handle ekv.Handle, err error) {
	if len(key) <= prefixLen {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}

	k := key
	if !hasTablePrefix(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[blockPrefixLength:]
	key, blockID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	if !hasRecordPrefixSep(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[recordPrefixSepLength:]
	if len(key) == 8 {
		var intHandle int64
		key, intHandle, err = codec.DecodeInt(key)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		return blockID, ekv.IntHandle(intHandle), nil
	}
	h, err := ekv.NewCommonHandle(key)
	if err != nil {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q %v", k, err)
	}
	return blockID, h, nil
}

// DecodeIndexKey decodes the key and gets the blockID, indexID, indexValues.
func DecodeIndexKey(key ekv.Key) (blockID int64, indexID int64, indexValues []string, err error) {
	k := key

	blockID, indexID, isRecord, err := DecodeKeyHead(key)
	if err != nil {
		return 0, 0, nil, errors.Trace(err)
	}
	if isRecord {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q", k)
		return 0, 0, nil, err
	}
	indexKey := key[prefixLen+idLen:]
	indexValues, err = DecodeValuesBytesToStrings(indexKey)
	if err != nil {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, err)
		return 0, 0, nil, err
	}
	return blockID, indexID, indexValues, nil
}

// DecodeValuesBytesToStrings decode the raw bytes to strings for each columns.
// FIXME: Without the schemaReplicant information, we can only decode the raw HoTT of
// the column. For instance, MysqlTime is internally saved as uint64.
func DecodeValuesBytesToStrings(b []byte) ([]string, error) {
	var datumValues []string
	for len(b) > 0 {
		remain, d, e := codec.DecodeOne(b)
		if e != nil {
			return nil, e
		}
		str, e1 := d.ToString()
		if e1 != nil {
			return nil, e
		}
		datumValues = append(datumValues, str)
		b = remain
	}
	return datumValues, nil
}

// DecodeMetaKey decodes the key and get the spacetime key and spacetime field.
func DecodeMetaKey(ek ekv.Key) (key []byte, field []byte, err error) {
	var tp uint64
	if !bytes.HasPrefix(ek, spacetimePrefix) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}
	ek = ek[spacetimePrefixLength:]
	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if structure.TypeFlag(tp) != structure.HashData {
		return nil, nil, errors.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}
	_, field, err = codec.DecodeBytes(ek, nil)
	return key, field, errors.Trace(err)
}

// DecodeKeyHead decodes the key's head and gets the blockID, indexID. isRecordKey is true when is a record key.
func DecodeKeyHead(key ekv.Key) (blockID int64, indexID int64, isRecordKey bool, err error) {
	isRecordKey = false
	k := key
	if !key.HasPrefix(blockPrefix) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(blockPrefix):]
	key, blockID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if key.HasPrefix(recordPrefixSep) {
		isRecordKey = true
		return
	}
	if !key.HasPrefix(indexPrefixSep) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(indexPrefixSep):]

	key, indexID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

// DecodeTableID decodes the causet ID of the key, if the key is not causet key, returns 0.
func DecodeTableID(key ekv.Key) int64 {
	if !key.HasPrefix(blockPrefix) {
		return 0
	}
	key = key[len(blockPrefix):]
	_, blockID, err := codec.DecodeInt(key)
	// TODO: return error.
	terror.Log(errors.Trace(err))
	return blockID
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key ekv.Key) (ekv.Handle, error) {
	if len(key) < RecordRowKeyLen || !hasTablePrefix(key) || !hasRecordPrefixSep(key[prefixLen-2:]) {
		return ekv.IntHandle(0), errInvalidKey.GenWithStack("invalid key - %q", key)
	}
	if len(key) == RecordRowKeyLen {
		u := binary.BigEndian.Uint64(key[prefixLen:])
		return ekv.IntHandle(codec.DecodeCmpUintToInt(u)), nil
	}
	return ekv.NewCommonHandle(key[prefixLen:])
}

// EncodeValue encodes a go value to bytes.
func EncodeValue(sc *stmtctx.StatementContext, b []byte, raw types.Causet) ([]byte, error) {
	var v types.Causet
	err := flatten(sc, raw, &v)
	if err != nil {
		return nil, err
	}
	return codec.EncodeValue(sc, b, v)
}

// EncodeRow encode event data and column ids into a slice of byte.
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
func EncodeRow(sc *stmtctx.StatementContext, event []types.Causet, colIDs []int64, valBuf []byte, values []types.Causet, e *rowcodec.CausetEncoder) ([]byte, error) {
	if len(event) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(event), len(colIDs))
	}
	if e.Enable {
		return e.Encode(sc, colIDs, event, valBuf)
	}
	return EncodeOldRow(sc, event, colIDs, valBuf, values)
}

// EncodeOldRow encode event data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
// valBuf and values pass by caller, for reducing EncodeOldRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeOldRow will allocate it.
func EncodeOldRow(sc *stmtctx.StatementContext, event []types.Causet, colIDs []int64, valBuf []byte, values []types.Causet) ([]byte, error) {
	if len(event) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(event), len(colIDs))
	}
	valBuf = valBuf[:0]
	if values == nil {
		values = make([]types.Causet, len(event)*2)
	}
	for i, c := range event {
		id := colIDs[i]
		values[2*i].SetInt64(id)
		err := flatten(sc, c, &values[2*i+1])
		if err != nil {
			return valBuf, errors.Trace(err)
		}
	}
	if len(values) == 0 {
		// We could not set nil value into ekv.
		return append(valBuf, codec.NilFlag), nil
	}
	return codec.EncodeValue(sc, valBuf, values...)
}

func flatten(sc *stmtctx.StatementContext, data types.Causet, ret *types.Causet) error {
	switch data.HoTT() {
	case types.HoTTMysqlTime:
		// for allegrosql datetime, timestamp and date type
		t := data.GetMysqlTime()
		if t.Type() == allegrosql.TypeTimestamp && sc.TimeZone != time.UTC {
			err := t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return errors.Trace(err)
			}
		}
		v, err := t.ToPackedUint()
		ret.SetUint64(v)
		return errors.Trace(err)
	case types.HoTTMysqlDuration:
		// for allegrosql time type
		ret.SetInt64(int64(data.GetMysqlDuration().Duration))
		return nil
	case types.HoTTMysqlEnum:
		ret.SetUint64(data.GetMysqlEnum().Value)
		return nil
	case types.HoTTMysqlSet:
		ret.SetUint64(data.GetMysqlSet().Value)
		return nil
	case types.HoTTBinaryLiteral, types.HoTTMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to causetstore in uint64 in convertToMysqlBit.
		val, err := data.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			return errors.Trace(err)
		}
		ret.SetUint64(val)
		return nil
	default:
		*ret = data
		return nil
	}
}

// DecodeDeferredCausetValue decodes data to a Causet according to the column info.
func DecodeDeferredCausetValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Causet, error) {
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Causet{}, errors.Trace(err)
	}
	colCauset, err := Unflatten(d, ft, loc)
	if err != nil {
		return types.Causet{}, errors.Trace(err)
	}
	return colCauset, nil
}

// DecodeRowWithMapNew decode a event to causet map.
func DecodeRowWithMapNew(b []byte, defcaus map[int64]*types.FieldType,
	loc *time.Location, event map[int64]types.Causet) (map[int64]types.Causet, error) {
	if event == nil {
		event = make(map[int64]types.Causet, len(defcaus))
	}
	if b == nil {
		return event, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return event, nil
	}

	reqDefCauss := make([]rowcodec.DefCausInfo, len(defcaus))
	var idx int
	for id, tp := range defcaus {
		reqDefCauss[idx] = rowcodec.DefCausInfo{
			ID: id,
			Ft: tp,
		}
		idx++
	}
	rd := rowcodec.NewCausetFIDelioecoder(reqDefCauss, loc)
	return rd.DecodeToCausetMap(b, event)
}

// DecodeRowWithMap decodes a byte slice into datums with a existing event map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRowWithMap(b []byte, defcaus map[int64]*types.FieldType, loc *time.Location, event map[int64]types.Causet) (map[int64]types.Causet, error) {
	if event == nil {
		event = make(map[int64]types.Causet, len(defcaus))
	}
	if b == nil {
		return event, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return event, nil
	}
	cnt := 0
	var (
		data []byte
		err  error
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := defcaus[id]
		if ok {
			_, v, err := codec.DecodeOne(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v, err = Unflatten(v, ft, loc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			event[id] = v
			cnt++
			if cnt == len(defcaus) {
				// Get enough data.
				break
			}
		}
	}
	return event, nil
}

// DecodeRowToCausetMap decodes a byte slice into datums.
// Row layout: colID1, value1, colID2, value2, .....
// Default value columns, generated columns and handle columns are unprocessed.
func DecodeRowToCausetMap(b []byte, defcaus map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Causet, error) {
	if !rowcodec.IsNewFormat(b) {
		return DecodeRowWithMap(b, defcaus, loc, nil)
	}
	return DecodeRowWithMapNew(b, defcaus, loc, nil)
}

// DecodeHandleToCausetMap decodes a handle into causet map.
func DecodeHandleToCausetMap(handle ekv.Handle, handleDefCausIDs []int64,
	defcaus map[int64]*types.FieldType, loc *time.Location, event map[int64]types.Causet) (map[int64]types.Causet, error) {
	if handle == nil || len(handleDefCausIDs) == 0 {
		return event, nil
	}
	if event == nil {
		event = make(map[int64]types.Causet, len(defcaus))
	}
	for id, ft := range defcaus {
		for idx, hid := range handleDefCausIDs {
			if id != hid {
				continue
			}
			d, err := decodeHandleToCauset(handle, ft, idx)
			if err != nil {
				return event, err
			}
			d, err = Unflatten(d, ft, loc)
			if err != nil {
				return event, err
			}
			if _, exists := event[id]; !exists {
				event[id] = d
			}
			break
		}
	}
	return event, nil
}

// decodeHandleToCauset decodes a handle to a specific column causet.
func decodeHandleToCauset(handle ekv.Handle, ft *types.FieldType, idx int) (types.Causet, error) {
	var d types.Causet
	var err error
	if handle.IsInt() {
		if allegrosql.HasUnsignedFlag(ft.Flag) {
			d = types.NewUintCauset(uint64(handle.IntValue()))
		} else {
			d = types.NewIntCauset(handle.IntValue())
		}
		return d, nil
	}
	// Decode common handle to Causet.
	_, d, err = codec.DecodeOne(handle.EncodedDefCaus(idx))
	return d, err
}

// CutRowNew cuts encoded event into byte slices and return columns' byte slice.
// Row layout: colID1, value1, colID2, value2, .....
func CutRowNew(data []byte, colIDs map[int64]int) ([][]byte, error) {
	if data == nil {
		return nil, nil
	}
	if len(data) == 1 && data[0] == codec.NilFlag {
		return nil, nil
	}

	var (
		cnt int
		b   []byte
		err error
		cid int64
	)
	event := make([][]byte, len(colIDs))
	for len(data) > 0 && cnt < len(colIDs) {
		// Get col id.
		data, cid, err = codec.CutDeferredCausetID(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Get col value.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		offset, ok := colIDs[cid]
		if ok {
			event[offset] = b
			cnt++
		}
	}
	return event, nil
}

// UnflattenCausets converts raw datums to column datums.
func UnflattenCausets(datums []types.Causet, fts []*types.FieldType, loc *time.Location) ([]types.Causet, error) {
	for i, causet := range datums {
		ft := fts[i]
		uCauset, err := Unflatten(causet, ft, loc)
		if err != nil {
			return datums, errors.Trace(err)
		}
		datums[i] = uCauset
	}
	return datums, nil
}

// Unflatten converts a raw causet to a column causet.
func Unflatten(causet types.Causet, ft *types.FieldType, loc *time.Location) (types.Causet, error) {
	if causet.IsNull() {
		return causet, nil
	}
	switch ft.Tp {
	case allegrosql.TypeFloat:
		causet.SetFloat32(float32(causet.GetFloat64()))
		return causet, nil
	case allegrosql.TypeVarchar, allegrosql.TypeString, allegrosql.TypeVarString:
		causet.SetString(causet.GetString(), ft.DefCauslate)
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeYear, allegrosql.TypeInt24,
		allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeDouble, allegrosql.TypeTinyBlob,
		allegrosql.TypeMediumBlob, allegrosql.TypeBlob, allegrosql.TypeLongBlob:
		return causet, nil
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.Tp, int8(ft.Decimal))
		var err error
		err = t.FromPackedUint(causet.GetUint64())
		if err != nil {
			return causet, errors.Trace(err)
		}
		if ft.Tp == allegrosql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return causet, errors.Trace(err)
			}
		}
		causet.SetUint64(0)
		causet.SetMysqlTime(t)
		return causet, nil
	case allegrosql.TypeDuration: // duration should read fsp from column spacetime data
		dur := types.Duration{Duration: time.Duration(causet.GetInt64()), Fsp: int8(ft.Decimal)}
		causet.SetMysqlDuration(dur)
		return causet, nil
	case allegrosql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, causet.GetUint64())
		if err != nil {
			enum = types.Enum{}
		}
		causet.SetMysqlEnum(enum, ft.DefCauslate)
		return causet, nil
	case allegrosql.TypeSet:
		set, err := types.ParseSetValue(ft.Elems, causet.GetUint64())
		if err != nil {
			return causet, errors.Trace(err)
		}
		causet.SetMysqlSet(set, ft.DefCauslate)
		return causet, nil
	case allegrosql.TypeBit:
		val := causet.GetUint64()
		byteSize := (ft.Flen + 7) >> 3
		causet.SetUint64(0)
		causet.SetMysqlBit(types.NewBinaryLiteralFromUint(val, byteSize))
	}
	return causet, nil
}

// EncodeIndexSeekKey encodes an index value to ekv.Key.
func EncodeIndexSeekKey(blockID int64, idxID int64, encodedValue []byte) ekv.Key {
	key := make([]byte, 0, RecordRowKeyLen+len(encodedValue))
	key = appendTableIndexPrefix(key, blockID)
	key = codec.EncodeInt(key, idxID)
	key = append(key, encodedValue...)
	return key
}

// CutIndexKey cuts encoded index key into colIDs to bytes slices map.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKey(key ekv.Key, colIDs []int64) (values map[int64][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make(map[int64][]byte, len(colIDs))
	for _, id := range colIDs {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values[id] = val
	}
	return
}

// CutIndexPrefix cuts the index prefix.
func CutIndexPrefix(key ekv.Key) []byte {
	return key[prefixLen+idLen:]
}

// CutIndexKeyNew cuts encoded index key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKeyNew(key ekv.Key, length int) (values [][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, val)
	}
	return
}

// CutCommonHandle cuts encoded common handle key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutCommonHandle(key ekv.Key, length int) (values [][]byte, b []byte, err error) {
	b = key[prefixLen:]
	values = make([][]byte, 0, length)
	for i := 0; i < length; i++ {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, val)
	}
	return
}

// HandleStatus is the handle status in index.
type HandleStatus int

const (
	// HandleDefault means decode handle value as int64 or bytes when DecodeIndexKV.
	HandleDefault HandleStatus = iota
	// HandleIsUnsigned means decode handle value as uint64 when DecodeIndexKV.
	HandleIsUnsigned
	// HandleNotNeeded means no need to decode handle value when DecodeIndexKV.
	HandleNotNeeded
)

// reEncodeHandle encodes the handle as a Causet so it can be properly decoded later.
// If it is common handle, it returns the encoded column values.
// If it is int handle, it is encoded as int Causet or uint Causet decided by the unsigned.
func reEncodeHandle(handle ekv.Handle, unsigned bool) ([][]byte, error) {
	if !handle.IsInt() {
		handleDefCausLen := handle.NumDefCauss()
		cHandleBytes := make([][]byte, 0, handleDefCausLen)
		for i := 0; i < handleDefCausLen; i++ {
			cHandleBytes = append(cHandleBytes, handle.EncodedDefCaus(i))
		}
		return cHandleBytes, nil
	}
	handleCauset := types.NewIntCauset(handle.IntValue())
	if unsigned {
		handleCauset.SetUint64(handleCauset.GetUint64())
	}
	intHandleBytes, err := codec.EncodeValue(nil, nil, handleCauset)
	return [][]byte{intHandleBytes}, err
}

func decodeRestoredValues(columns []rowcodec.DefCausInfo, restoredVal []byte) ([][]byte, error) {
	colIDs := make(map[int64]int, len(columns))
	for i, col := range columns {
		colIDs[col.ID] = i
	}
	// We don't need to decode handle here, and colIDs >= 0 always.
	rd := rowcodec.NewByteCausetDecoder(columns, []int64{-1}, nil, nil)
	resultValues, err := rd.DecodeToBytesNoHandle(colIDs, restoredVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resultValues, nil
}

func decodeIndexEkvOldDefCauslation(key, value []byte, defcausLen int, hdStatus HandleStatus) ([][]byte, error) {
	resultValues, b, err := CutIndexKeyNew(key, defcausLen)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}
	var handle ekv.Handle
	if len(b) > 0 {
		// non-unique index
		handle, err = decodeHandleInIndexKey(b)
		if err != nil {
			return nil, err
		}
		handleBytes, err := reEncodeHandle(handle, hdStatus == HandleIsUnsigned)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resultValues = append(resultValues, handleBytes...)
	} else {
		// In unique int handle index.
		handle = decodeIntHandleInIndexValue(value)
		handleBytes, err := reEncodeHandle(handle, hdStatus == HandleIsUnsigned)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resultValues = append(resultValues, handleBytes...)
	}
	return resultValues, nil
}

// DecodeIndexKV uses to decode index key values.
func DecodeIndexKV(key, value []byte, defcausLen int, hdStatus HandleStatus, columns []rowcodec.DefCausInfo) ([][]byte, error) {
	if len(value) <= MaxOldEncodeValueLen {
		return decodeIndexEkvOldDefCauslation(key, value, defcausLen, hdStatus)
	}
	return decodeIndexEkvGeneral(key, value, defcausLen, hdStatus, columns)
}

// DecodeIndexHandle uses to decode the handle from index key/value.
func DecodeIndexHandle(key, value []byte, defcausLen int) (ekv.Handle, error) {
	_, b, err := CutIndexKeyNew(key, defcausLen)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(b) > 0 {
		return decodeHandleInIndexKey(b)
	} else if len(value) >= 8 {
		return decodeHandleInIndexValue(value)
	}
	// Should never execute to here.
	return nil, errors.Errorf("no handle in index key: %v, value: %v", key, value)
}

func decodeHandleInIndexKey(keySuffix []byte) (ekv.Handle, error) {
	remain, d, err := codec.DecodeOne(keySuffix)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(remain) == 0 && d.HoTT() == types.HoTTInt64 {
		return ekv.IntHandle(d.GetInt64()), nil
	}
	return ekv.NewCommonHandle(keySuffix)
}

func decodeHandleInIndexValue(value []byte) (ekv.Handle, error) {
	if len(value) > MaxOldEncodeValueLen {
		tailLen := value[0]
		if tailLen >= 8 {
			return decodeIntHandleInIndexValue(value[len(value)-int(tailLen):]), nil
		}
		handleLen := uint16(value[2])<<8 + uint16(value[3])
		return ekv.NewCommonHandle(value[4 : 4+handleLen])
	}
	return decodeIntHandleInIndexValue(value), nil
}

// decodeIntHandleInIndexValue uses to decode index value as int handle id.
func decodeIntHandleInIndexValue(data []byte) ekv.Handle {
	return ekv.IntHandle(binary.BigEndian.Uint64(data))
}

// EncodeTableIndexPrefix encodes index prefix with blockID and idxID.
func EncodeTableIndexPrefix(blockID, idxID int64) ekv.Key {
	key := make([]byte, 0, prefixLen)
	key = appendTableIndexPrefix(key, blockID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// EncodeTablePrefix encodes causet prefix with causet ID.
func EncodeTablePrefix(blockID int64) ekv.Key {
	var key ekv.Key
	key = append(key, blockPrefix...)
	key = codec.EncodeInt(key, blockID)
	return key
}

// appendTableRecordPrefix appends causet record prefix  "t[blockID]_r".
func appendTableRecordPrefix(buf []byte, blockID int64) []byte {
	buf = append(buf, blockPrefix...)
	buf = codec.EncodeInt(buf, blockID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// appendTableIndexPrefix appends causet index prefix  "t[blockID]_i".
func appendTableIndexPrefix(buf []byte, blockID int64) []byte {
	buf = append(buf, blockPrefix...)
	buf = codec.EncodeInt(buf, blockID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

// GenTableRecordPrefix composes record prefix with blockID: "t[blockID]_r".
func GenTableRecordPrefix(blockID int64) ekv.Key {
	buf := make([]byte, 0, len(blockPrefix)+8+len(recordPrefixSep))
	return appendTableRecordPrefix(buf, blockID)
}

// GenTableIndexPrefix composes index prefix with blockID: "t[blockID]_i".
func GenTableIndexPrefix(blockID int64) ekv.Key {
	buf := make([]byte, 0, len(blockPrefix)+8+len(indexPrefixSep))
	return appendTableIndexPrefix(buf, blockID)
}

// IsIndexKey is used to check whether the key is an index key.
func IsIndexKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'i'
}

// IsUntouchedIndexKValue uses to check whether the key is index key, and the value is untouched,
// since the untouched index key/value is no need to commit.
func IsUntouchedIndexKValue(k, v []byte) bool {
	if !IsIndexKey(k) {
		return false
	}
	vLen := len(v)
	if vLen <= MaxOldEncodeValueLen {
		return (vLen == 1 || vLen == 9) && v[vLen-1] == ekv.UnCommitIndexKVFlag
	}
	// New index value format
	tailLen := int(v[0])
	if tailLen < 8 {
		// Non-unique index.
		return tailLen >= 1 && v[vLen-1] == ekv.UnCommitIndexKVFlag
	}
	// Unique index
	return tailLen == 9
}

// GenTablePrefix composes causet record and index prefix: "t[blockID]".
func GenTablePrefix(blockID int64) ekv.Key {
	buf := make([]byte, 0, len(blockPrefix)+8)
	buf = append(buf, blockPrefix...)
	buf = codec.EncodeInt(buf, blockID)
	return buf
}

// TruncateToRowKeyLen truncates the key to event key length if the key is longer than event key.
func TruncateToRowKeyLen(key ekv.Key) ekv.Key {
	if len(key) > RecordRowKeyLen {
		return key[:RecordRowKeyLen]
	}
	return key
}

// GetTableHandleKeyRange returns causet handle's key range with blockID.
func GetTableHandleKeyRange(blockID int64) (startKey, endKey []byte) {
	startKey = EncodeRowKeyWithHandle(blockID, ekv.IntHandle(math.MinInt64))
	endKey = EncodeRowKeyWithHandle(blockID, ekv.IntHandle(math.MaxInt64))
	return
}

// GetTableIndexKeyRange returns causet index's key range with blockID and indexID.
func GetTableIndexKeyRange(blockID, indexID int64) (startKey, endKey []byte) {
	startKey = EncodeIndexSeekKey(blockID, indexID, nil)
	endKey = EncodeIndexSeekKey(blockID, indexID, []byte{255})
	return
}

// GetIndexKeyBuf reuse or allocate buffer
func GetIndexKeyBuf(buf []byte, defaultCap int) []byte {
	if buf != nil {
		return buf[:0]
	}
	return make([]byte, 0, defaultCap)
}

// GenIndexKey generates index key using input physical causet id
func GenIndexKey(sc *stmtctx.StatementContext, tblInfo *perceptron.TableInfo, idxInfo *perceptron.IndexInfo,
	phyTblID int64, indexedValues []types.Causet, h ekv.Handle, buf []byte) (key []byte, distinct bool, err error) {
	if idxInfo.Unique {
		// See https://dev.allegrosql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new event with a key value that matches an existing event.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			if cv.IsNull() {
				distinct = false
				break
			}
		}
	}
	// For string columns, indexes can be created using only the leading part of column values,
	// using col_name(length) syntax to specify an index prefix length.
	TruncateIndexValues(tblInfo, idxInfo, indexedValues)
	key = GetIndexKeyBuf(buf, RecordRowKeyLen+len(indexedValues)*9+9)
	key = appendTableIndexPrefix(key, phyTblID)
	key = codec.EncodeInt(key, idxInfo.ID)
	key, err = codec.EncodeKey(sc, key, indexedValues...)
	if err != nil {
		return nil, false, err
	}
	if !distinct && h != nil {
		if h.IsInt() {
			key, err = codec.EncodeKey(sc, key, types.NewCauset(h.IntValue()))
		} else {
			key = append(key, h.Encoded()...)
		}
	}
	return
}

// GenIndexValue creates encoded index value and returns the result, only support local index
func GenIndexValue(sc *stmtctx.StatementContext, tblInfo *perceptron.TableInfo, idxInfo *perceptron.IndexInfo, containNonBinaryString bool,
	distinct bool, untouched bool, indexedValues []types.Causet, h ekv.Handle) ([]byte, error) {
	return GenIndexValueNew(sc, tblInfo, idxInfo, containNonBinaryString, distinct, untouched, indexedValues, h, 0)
}

// GenIndexValueNew create index value for both local and global index.
func GenIndexValueNew(sc *stmtctx.StatementContext, tblInfo *perceptron.TableInfo, idxInfo *perceptron.IndexInfo, containNonBinaryString bool,
	distinct bool, untouched bool, indexedValues []types.Causet, h ekv.Handle, partitionID int64) ([]byte, error) {
	idxVal := make([]byte, 1)
	newEncode := false
	tailLen := 0
	if !h.IsInt() && distinct {
		idxVal = encodeCommonHandle(idxVal, h)
		newEncode = true
	}
	if idxInfo.Global {
		idxVal = encodePartitionID(idxVal, partitionID)
		newEncode = true
	}
	if collate.NewDefCauslationEnabled() && containNonBinaryString {
		colIds := make([]int64, len(idxInfo.DeferredCausets))
		for i, col := range idxInfo.DeferredCausets {
			colIds[i] = tblInfo.DeferredCausets[col.Offset].ID
		}
		rd := rowcodec.CausetEncoder{Enable: true}
		rowRestoredValue, err := rd.Encode(sc, colIds, indexedValues, nil)
		if err != nil {
			return nil, err
		}
		idxVal = append(idxVal, rowRestoredValue...)
		newEncode = true
	}

	if newEncode {
		if h.IsInt() && distinct {
			// The len of the idxVal is always >= 10 since len (restoredValue) > 0.
			tailLen += 8
			idxVal = append(idxVal, EncodeHandleInUniqueIndexValue(h, false)...)
		} else if len(idxVal) < 10 {
			// Padding the len to 10
			paddingLen := 10 - len(idxVal)
			tailLen += paddingLen
			idxVal = append(idxVal, bytes.Repeat([]byte{0x0}, paddingLen)...)
		}
		if untouched {
			// If index is untouched and fetch here means the key is exists in EinsteinDB, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			tailLen += 1
			idxVal = append(idxVal, ekv.UnCommitIndexKVFlag)
		}
		idxVal[0] = byte(tailLen)
	} else {
		// Old index value encoding.
		idxVal = make([]byte, 0)
		if distinct {
			idxVal = EncodeHandleInUniqueIndexValue(h, untouched)
		}
		if untouched {
			// If index is untouched and fetch here means the key is exists in EinsteinDB, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			idxVal = append(idxVal, ekv.UnCommitIndexKVFlag)
		}
		if len(idxVal) == 0 {
			idxVal = []byte{'0'}
		}
	}
	return idxVal, nil
}

// TruncateIndexValues truncates the index values created using only the leading part of column values.
func TruncateIndexValues(tblInfo *perceptron.TableInfo, idxInfo *perceptron.IndexInfo, indexedValues []types.Causet) {
	for i := 0; i < len(indexedValues); i++ {
		v := &indexedValues[i]
		idxDefCaus := idxInfo.DeferredCausets[i]
		noPrefixIndex := idxDefCaus.Length == types.UnspecifiedLength
		if noPrefixIndex {
			continue
		}
		notStringType := v.HoTT() != types.HoTTString && v.HoTT() != types.HoTTBytes
		if notStringType {
			continue
		}

		colInfo := tblInfo.DeferredCausets[idxDefCaus.Offset]
		isUTF8Charset := colInfo.Charset == charset.CharsetUTF8 || colInfo.Charset == charset.CharsetUTF8MB4
		if isUTF8Charset && utf8.RuneCount(v.GetBytes()) > idxDefCaus.Length {
			rs := bytes.Runes(v.GetBytes())
			truncateStr := string(rs[:idxDefCaus.Length])
			// truncate value and limit its length
			v.SetString(truncateStr, colInfo.DefCauslate)
			if v.HoTT() == types.HoTTBytes {
				v.SetBytes(v.GetBytes())
			}
		} else if !isUTF8Charset && len(v.GetBytes()) > idxDefCaus.Length {
			v.SetBytes(v.GetBytes()[:idxDefCaus.Length])
			if v.HoTT() == types.HoTTString {
				v.SetString(v.GetString(), colInfo.DefCauslate)
			}
		}
	}
}

// EncodeHandleInUniqueIndexValue encodes handle in data.
func EncodeHandleInUniqueIndexValue(h ekv.Handle, isUntouched bool) []byte {
	if h.IsInt() {
		var data [8]byte
		binary.BigEndian.PutUint64(data[:], uint64(h.IntValue()))
		return data[:]
	}
	var untouchedFlag byte
	if isUntouched {
		untouchedFlag = 1
	}
	return encodeCommonHandle([]byte{untouchedFlag}, h)
}

func encodeCommonHandle(idxVal []byte, h ekv.Handle) []byte {
	idxVal = append(idxVal, CommonHandleFlag)
	hLen := uint16(len(h.Encoded()))
	idxVal = append(idxVal, byte(hLen>>8), byte(hLen))
	idxVal = append(idxVal, h.Encoded()...)
	return idxVal
}

// DecodeHandleInUniqueIndexValue decodes handle in data.
func DecodeHandleInUniqueIndexValue(data []byte, isCommonHandle bool) (ekv.Handle, error) {
	if !isCommonHandle {
		dLen := len(data)
		if dLen <= MaxOldEncodeValueLen {
			return ekv.IntHandle(int64(binary.BigEndian.Uint64(data))), nil
		}
		return ekv.IntHandle(int64(binary.BigEndian.Uint64(data[dLen-int(data[0]):]))), nil
	}
	tailLen := int(data[0])
	data = data[:len(data)-tailLen]
	handleLen := uint16(data[2])<<8 + uint16(data[3])
	handleEndOff := 4 + handleLen
	h, err := ekv.NewCommonHandle(data[4:handleEndOff])
	if err != nil {
		return nil, err
	}
	return h, nil
}

func encodePartitionID(idxVal []byte, partitionID int64) []byte {
	idxVal = append(idxVal, PartitionIDFlag)
	idxVal = codec.EncodeInt(idxVal, partitionID)
	return idxVal
}

type indexValueSegments struct {
	commonHandle   []byte
	partitionID    []byte
	restoredValues []byte
	intHandle      []byte
}

// splitIndexValue splits index value into segments.
func splitIndexValue(value []byte) (segs indexValueSegments) {
	tailLen := int(value[0])
	tail := value[len(value)-tailLen:]
	value = value[1 : len(value)-tailLen]
	if len(tail) >= 8 {
		segs.intHandle = tail[:8]
	}
	if len(value) > 0 && value[0] == CommonHandleFlag {
		handleLen := uint16(value[1])<<8 + uint16(value[2])
		handleEndOff := 3 + handleLen
		segs.commonHandle = value[3:handleEndOff]
		value = value[handleEndOff:]
	}
	if len(value) > 0 && value[0] == PartitionIDFlag {
		segs.partitionID = value[1:9]
		value = value[9:]
	}
	if len(value) > 0 && value[0] == RestoreDataFlag {
		segs.restoredValues = value
	}
	return
}

// decodeIndexEkvGeneral decodes index key value pair of new layout in an extensible way.
func decodeIndexEkvGeneral(key, value []byte, defcausLen int, hdStatus HandleStatus, columns []rowcodec.DefCausInfo) ([][]byte, error) {
	var resultValues [][]byte
	var keySuffix []byte
	var handle ekv.Handle
	var err error
	segs := splitIndexValue(value)
	resultValues, keySuffix, err = CutIndexKeyNew(key, defcausLen)
	if err != nil {
		return nil, err
	}
	if segs.restoredValues != nil { // new collation
		resultValues, err = decodeRestoredValues(columns[:defcausLen], segs.restoredValues)
		if err != nil {
			return nil, err
		}
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}

	if segs.intHandle != nil {
		// In unique int handle index.
		handle = decodeIntHandleInIndexValue(segs.intHandle)
	} else if segs.commonHandle != nil {
		// In unique common handle index.
		handle, err = decodeHandleInIndexKey(segs.commonHandle)
		if err != nil {
			return nil, err
		}
	} else {
		// In non-unique index, decode handle in keySuffix
		handle, err = decodeHandleInIndexKey(keySuffix)
		if err != nil {
			return nil, err
		}
	}
	handleBytes, err := reEncodeHandle(handle, hdStatus == HandleIsUnsigned)
	if err != nil {
		return nil, err
	}
	resultValues = append(resultValues, handleBytes...)
	if segs.partitionID != nil {
		_, pid, err := codec.DecodeInt(segs.partitionID)
		if err != nil {
			return nil, err
		}
		causet := types.NewIntCauset(pid)
		pidBytes, err := codec.EncodeValue(nil, nil, causet)
		if err != nil {
			return nil, err
		}
		resultValues = append(resultValues, pidBytes)
	}
	return resultValues, nil
}
