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

package types

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/BerolinaSQL/types"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types/json"
)

// HoTT constants.
const (
	HoTTNull          byte = 0
	HoTTInt64         byte = 1
	HoTTUint64        byte = 2
	HoTTFloat32       byte = 3
	HoTTFloat64       byte = 4
	HoTTString        byte = 5
	HoTTBytes         byte = 6
	HoTTBinaryLiteral byte = 7 // Used for BIT / HEX literals.
	HoTTMysqlDecimal  byte = 8
	HoTTMysqlDuration byte = 9
	HoTTMysqlEnum     byte = 10
	HoTTMysqlBit      byte = 11 // Used for BIT block defCausumn values.
	HoTTMysqlSet      byte = 12
	HoTTMysqlTime     byte = 13
	HoTTInterface     byte = 14
	HoTTMinNotNull    byte = 15
	HoTTMaxValue      byte = 16
	HoTTRaw           byte = 17
	HoTTMysqlJSON     byte = 18
)

// Causet is a data box holds different HoTT of data.
// It has better performance and is easier to use than `interface{}`.
type Causet struct {
	k         byte        // causet HoTT.
	decimal   uint16      // decimal can hold uint16 values.
	length    uint32      // length can hold uint32 values.
	i         int64       // i can hold int64 uint64 float64 values.
	defCauslation string      // defCauslation hold the defCauslation information for string value.
	b         []byte      // b can hold string or []byte values.
	x         interface{} // x hold all other types.
}

// Clone create a deep copy of the Causet.
func (d *Causet) Clone() *Causet {
	ret := new(Causet)
	d.Copy(ret)
	return ret
}

// Copy deep copies a Causet into destination.
func (d *Causet) Copy(dst *Causet) {
	*dst = *d
	if d.b != nil {
		dst.b = make([]byte, len(d.b))
		copy(dst.b, d.b)
	}
	switch dst.HoTT() {
	case HoTTMysqlDecimal:
		d := *d.GetMysqlDecimal()
		dst.SetMysqlDecimal(&d)
	case HoTTMysqlTime:
		dst.SetMysqlTime(d.GetMysqlTime())
	}
}

// HoTT gets the HoTT of the causet.
func (d *Causet) HoTT() byte {
	return d.k
}

// DefCauslation gets the defCauslation of the causet.
func (d *Causet) DefCauslation() string {
	return d.defCauslation
}

// Frac gets the frac of the causet.
func (d *Causet) Frac() int {
	return int(d.decimal)
}

// SetFrac sets the frac of the causet.
func (d *Causet) SetFrac(frac int) {
	d.decimal = uint16(frac)
}

// Length gets the length of the causet.
func (d *Causet) Length() int {
	return int(d.length)
}

// SetLength sets the length of the causet.
func (d *Causet) SetLength(l int) {
	d.length = uint32(l)
}

// IsNull checks if causet is null.
func (d *Causet) IsNull() bool {
	return d.k == HoTTNull
}

// GetInt64 gets int64 value.
func (d *Causet) GetInt64() int64 {
	return d.i
}

// SetInt64 sets int64 value.
func (d *Causet) SetInt64(i int64) {
	d.k = HoTTInt64
	d.i = i
}

// GetUint64 gets uint64 value.
func (d *Causet) GetUint64() uint64 {
	return uint64(d.i)
}

// SetUint64 sets uint64 value.
func (d *Causet) SetUint64(i uint64) {
	d.k = HoTTUint64
	d.i = int64(i)
}

// GetFloat64 gets float64 value.
func (d *Causet) GetFloat64() float64 {
	return math.Float64frombits(uint64(d.i))
}

// SetFloat64 sets float64 value.
func (d *Causet) SetFloat64(f float64) {
	d.k = HoTTFloat64
	d.i = int64(math.Float64bits(f))
}

// GetFloat32 gets float32 value.
func (d *Causet) GetFloat32() float32 {
	return float32(math.Float64frombits(uint64(d.i)))
}

// SetFloat32 sets float32 value.
func (d *Causet) SetFloat32(f float32) {
	d.k = HoTTFloat32
	d.i = int64(math.Float64bits(float64(f)))
}

// GetString gets string value.
func (d *Causet) GetString() string {
	return string(replog.String(d.b))
}

// SetString sets string value.
func (d *Causet) SetString(s string, defCauslation string) {
	d.k = HoTTString
	sink(s)
	d.b = replog.Slice(s)
	d.defCauslation = defCauslation
}

// sink prevents s from being allocated on the stack.
var sink = func(s string) {
}

// GetBytes gets bytes value.
func (d *Causet) GetBytes() []byte {
	return d.b
}

// SetBytes sets bytes value to causet.
func (d *Causet) SetBytes(b []byte) {
	d.k = HoTTBytes
	d.b = b
	d.defCauslation = charset.DefCauslationBin
}

// SetBytesAsString sets bytes value to causet as string type.
func (d *Causet) SetBytesAsString(b []byte, defCauslation string, length uint32) {
	d.k = HoTTString
	d.b = b
	d.length = length
	d.defCauslation = defCauslation
}

// GetInterface gets interface value.
func (d *Causet) GetInterface() interface{} {
	return d.x
}

// SetInterface sets interface to causet.
func (d *Causet) SetInterface(x interface{}) {
	d.k = HoTTInterface
	d.x = x
}

// SetNull sets causet to nil.
func (d *Causet) SetNull() {
	d.k = HoTTNull
	d.x = nil
}

// SetMinNotNull sets causet to minNotNull value.
func (d *Causet) SetMinNotNull() {
	d.k = HoTTMinNotNull
	d.x = nil
}

// GetBinaryLiteral gets Bit value
func (d *Causet) GetBinaryLiteral() BinaryLiteral {
	return d.b
}

// GetMysqlBit gets MysqlBit value
func (d *Causet) GetMysqlBit() BinaryLiteral {
	return d.GetBinaryLiteral()
}

// SetBinaryLiteral sets Bit value
func (d *Causet) SetBinaryLiteral(b BinaryLiteral) {
	d.k = HoTTBinaryLiteral
	d.b = b
}

// SetMysqlBit sets MysqlBit value
func (d *Causet) SetMysqlBit(b BinaryLiteral) {
	d.k = HoTTMysqlBit
	d.b = b
}

// GetMysqlDecimal gets Decimal value
func (d *Causet) GetMysqlDecimal() *MyDecimal {
	return d.x.(*MyDecimal)
}

// SetMysqlDecimal sets Decimal value
func (d *Causet) SetMysqlDecimal(b *MyDecimal) {
	d.k = HoTTMysqlDecimal
	d.x = b
}

// GetMysqlDuration gets Duration value
func (d *Causet) GetMysqlDuration() Duration {
	return Duration{Duration: time.Duration(d.i), Fsp: int8(d.decimal)}
}

// SetMysqlDuration sets Duration value
func (d *Causet) SetMysqlDuration(b Duration) {
	d.k = HoTTMysqlDuration
	d.i = int64(b.Duration)
	d.decimal = uint16(b.Fsp)
}

// GetMysqlEnum gets Enum value
func (d *Causet) GetMysqlEnum() Enum {
	str := string(replog.String(d.b))
	return Enum{Value: uint64(d.i), Name: str}
}

// SetMysqlEnum sets Enum value
func (d *Causet) SetMysqlEnum(b Enum, defCauslation string) {
	d.k = HoTTMysqlEnum
	d.i = int64(b.Value)
	sink(b.Name)
	d.defCauslation = defCauslation
	d.b = replog.Slice(b.Name)
}

// GetMysqlSet gets Set value
func (d *Causet) GetMysqlSet() Set {
	str := string(replog.String(d.b))
	return Set{Value: uint64(d.i), Name: str}
}

// SetMysqlSet sets Set value
func (d *Causet) SetMysqlSet(b Set, defCauslation string) {
	d.k = HoTTMysqlSet
	d.i = int64(b.Value)
	sink(b.Name)
	d.defCauslation = defCauslation
	d.b = replog.Slice(b.Name)
}

// GetMysqlJSON gets json.BinaryJSON value
func (d *Causet) GetMysqlJSON() json.BinaryJSON {
	return json.BinaryJSON{TypeCode: byte(d.i), Value: d.b}
}

// SetMysqlJSON sets json.BinaryJSON value
func (d *Causet) SetMysqlJSON(b json.BinaryJSON) {
	d.k = HoTTMysqlJSON
	d.i = int64(b.TypeCode)
	d.b = b.Value
}

// GetMysqlTime gets types.Time value
func (d *Causet) GetMysqlTime() Time {
	return d.x.(Time)
}

// SetMysqlTime sets types.Time value
func (d *Causet) SetMysqlTime(b Time) {
	d.k = HoTTMysqlTime
	d.x = b
}

// SetRaw sets raw value.
func (d *Causet) SetRaw(b []byte) {
	d.k = HoTTRaw
	d.b = b
}

// GetRaw gets raw value.
func (d *Causet) GetRaw() []byte {
	return d.b
}

// SetAutoID set the auto increment ID according to its int flag.
func (d *Causet) SetAutoID(id int64, flag uint) {
	if allegrosql.HasUnsignedFlag(flag) {
		d.SetUint64(uint64(id))
	} else {
		d.SetInt64(id)
	}
}

// String returns a human-readable description of Causet. It is intended only for debugging.
func (d Causet) String() string {
	var t string
	switch d.k {
	case HoTTNull:
		t = "HoTTNull"
	case HoTTInt64:
		t = "HoTTInt64"
	case HoTTUint64:
		t = "HoTTUint64"
	case HoTTFloat32:
		t = "HoTTFloat32"
	case HoTTFloat64:
		t = "HoTTFloat64"
	case HoTTString:
		t = "HoTTString"
	case HoTTBytes:
		t = "HoTTBytes"
	case HoTTMysqlDecimal:
		t = "HoTTMysqlDecimal"
	case HoTTMysqlDuration:
		t = "HoTTMysqlDuration"
	case HoTTMysqlEnum:
		t = "HoTTMysqlEnum"
	case HoTTBinaryLiteral:
		t = "HoTTBinaryLiteral"
	case HoTTMysqlBit:
		t = "HoTTMysqlBit"
	case HoTTMysqlSet:
		t = "HoTTMysqlSet"
	case HoTTMysqlJSON:
		t = "HoTTMysqlJSON"
	case HoTTMysqlTime:
		t = "HoTTMysqlTime"
	default:
		t = "Unknown"
	}
	v := d.GetValue()
	if b, ok := v.([]byte); ok && d.k == HoTTBytes {
		v = string(b)
	}
	return fmt.Sprintf("%v %v", t, v)
}

// GetValue gets the value of the causet of any HoTT.
func (d *Causet) GetValue() interface{} {
	switch d.k {
	case HoTTInt64:
		return d.GetInt64()
	case HoTTUint64:
		return d.GetUint64()
	case HoTTFloat32:
		return d.GetFloat32()
	case HoTTFloat64:
		return d.GetFloat64()
	case HoTTString:
		return d.GetString()
	case HoTTBytes:
		return d.GetBytes()
	case HoTTMysqlDecimal:
		return d.GetMysqlDecimal()
	case HoTTMysqlDuration:
		return d.GetMysqlDuration()
	case HoTTMysqlEnum:
		return d.GetMysqlEnum()
	case HoTTBinaryLiteral, HoTTMysqlBit:
		return d.GetBinaryLiteral()
	case HoTTMysqlSet:
		return d.GetMysqlSet()
	case HoTTMysqlJSON:
		return d.GetMysqlJSON()
	case HoTTMysqlTime:
		return d.GetMysqlTime()
	default:
		return d.GetInterface()
	}
}

// SetValueWithDefaultDefCauslation sets any HoTT of value.
func (d *Causet) SetValueWithDefaultDefCauslation(val interface{}) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		if x {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float32:
		d.SetFloat32(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x, allegrosql.DefaultDefCauslationName)
	case []byte:
		d.SetBytes(x)
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case Duration:
		d.SetMysqlDuration(x)
	case Enum:
		d.SetMysqlEnum(x, allegrosql.DefaultDefCauslationName)
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // CausetStore as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	case Set:
		d.SetMysqlSet(x, allegrosql.DefaultDefCauslationName)
	case json.BinaryJSON:
		d.SetMysqlJSON(x)
	case Time:
		d.SetMysqlTime(x)
	default:
		d.SetInterface(x)
	}
}

// SetValue sets any HoTT of value.
func (d *Causet) SetValue(val interface{}, tp *types.FieldType) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		if x {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float32:
		d.SetFloat32(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x, tp.DefCauslate)
	case []byte:
		d.SetBytes(x)
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case Duration:
		d.SetMysqlDuration(x)
	case Enum:
		d.SetMysqlEnum(x, tp.DefCauslate)
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // CausetStore as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	case Set:
		d.SetMysqlSet(x, tp.DefCauslate)
	case json.BinaryJSON:
		d.SetMysqlJSON(x)
	case Time:
		d.SetMysqlTime(x)
	default:
		d.SetInterface(x)
	}
}

// CompareCauset compares causet to another causet.
// TODO: return error properly.
func (d *Causet) CompareCauset(sc *stmtctx.StatementContext, ad *Causet) (int, error) {
	if d.k == HoTTMysqlJSON && ad.k != HoTTMysqlJSON {
		cmp, err := ad.CompareCauset(sc, d)
		return cmp * -1, errors.Trace(err)
	}
	switch ad.k {
	case HoTTNull:
		if d.k == HoTTNull {
			return 0, nil
		}
		return 1, nil
	case HoTTMinNotNull:
		if d.k == HoTTNull {
			return -1, nil
		} else if d.k == HoTTMinNotNull {
			return 0, nil
		}
		return 1, nil
	case HoTTMaxValue:
		if d.k == HoTTMaxValue {
			return 0, nil
		}
		return -1, nil
	case HoTTInt64:
		return d.compareInt64(sc, ad.GetInt64())
	case HoTTUint64:
		return d.compareUint64(sc, ad.GetUint64())
	case HoTTFloat32, HoTTFloat64:
		return d.compareFloat64(sc, ad.GetFloat64())
	case HoTTString:
		return d.compareString(sc, ad.GetString(), d.defCauslation)
	case HoTTBytes:
		return d.compareBytes(sc, ad.GetBytes())
	case HoTTMysqlDecimal:
		return d.compareMysqlDecimal(sc, ad.GetMysqlDecimal())
	case HoTTMysqlDuration:
		return d.compareMysqlDuration(sc, ad.GetMysqlDuration())
	case HoTTMysqlEnum:
		return d.compareMysqlEnum(sc, ad.GetMysqlEnum())
	case HoTTBinaryLiteral, HoTTMysqlBit:
		return d.compareBinaryLiteral(sc, ad.GetBinaryLiteral())
	case HoTTMysqlSet:
		return d.compareMysqlSet(sc, ad.GetMysqlSet())
	case HoTTMysqlJSON:
		return d.compareMysqlJSON(sc, ad.GetMysqlJSON())
	case HoTTMysqlTime:
		return d.compareMysqlTime(sc, ad.GetMysqlTime())
	default:
		return 0, nil
	}
}

func (d *Causet) compareInt64(sc *stmtctx.StatementContext, i int64) (int, error) {
	switch d.k {
	case HoTTMaxValue:
		return 1, nil
	case HoTTInt64:
		return CompareInt64(d.i, i), nil
	case HoTTUint64:
		if i < 0 || d.GetUint64() > math.MaxInt64 {
			return 1, nil
		}
		return CompareInt64(d.i, i), nil
	default:
		return d.compareFloat64(sc, float64(i))
	}
}

func (d *Causet) compareUint64(sc *stmtctx.StatementContext, u uint64) (int, error) {
	switch d.k {
	case HoTTMaxValue:
		return 1, nil
	case HoTTInt64:
		if d.i < 0 || u > math.MaxInt64 {
			return -1, nil
		}
		return CompareInt64(d.i, int64(u)), nil
	case HoTTUint64:
		return CompareUint64(d.GetUint64(), u), nil
	default:
		return d.compareFloat64(sc, float64(u))
	}
}

func (d *Causet) compareFloat64(sc *stmtctx.StatementContext, f float64) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTInt64:
		return CompareFloat64(float64(d.i), f), nil
	case HoTTUint64:
		return CompareFloat64(float64(d.GetUint64()), f), nil
	case HoTTFloat32, HoTTFloat64:
		return CompareFloat64(d.GetFloat64(), f), nil
	case HoTTString, HoTTBytes:
		fVal, err := StrToFloat(sc, d.GetString(), false)
		return CompareFloat64(fVal, f), errors.Trace(err)
	case HoTTMysqlDecimal:
		fVal, err := d.GetMysqlDecimal().ToFloat64()
		return CompareFloat64(fVal, f), errors.Trace(err)
	case HoTTMysqlDuration:
		fVal := d.GetMysqlDuration().Seconds()
		return CompareFloat64(fVal, f), nil
	case HoTTMysqlEnum:
		fVal := d.GetMysqlEnum().ToNumber()
		return CompareFloat64(fVal, f), nil
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(sc)
		fVal := float64(val)
		return CompareFloat64(fVal, f), errors.Trace(err)
	case HoTTMysqlSet:
		fVal := d.GetMysqlSet().ToNumber()
		return CompareFloat64(fVal, f), nil
	case HoTTMysqlTime:
		fVal, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return CompareFloat64(fVal, f), errors.Trace(err)
	default:
		return -1, nil
	}
}

func (d *Causet) compareString(sc *stmtctx.StatementContext, s string, retDefCauslation string) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTString, HoTTBytes:
		return CompareString(d.GetString(), s, d.defCauslation), nil
	case HoTTMysqlDecimal:
		dec := new(MyDecimal)
		err := sc.HandleTruncate(dec.FromString(replog.Slice(s)))
		return d.GetMysqlDecimal().Compare(dec), errors.Trace(err)
	case HoTTMysqlTime:
		dt, err := ParseDatetime(sc, s)
		return d.GetMysqlTime().Compare(dt), errors.Trace(err)
	case HoTTMysqlDuration:
		dur, err := ParseDuration(sc, s, MaxFsp)
		return d.GetMysqlDuration().Compare(dur), errors.Trace(err)
	case HoTTMysqlSet:
		return CompareString(d.GetMysqlSet().String(), s, d.defCauslation), nil
	case HoTTMysqlEnum:
		return CompareString(d.GetMysqlEnum().String(), s, d.defCauslation), nil
	case HoTTBinaryLiteral, HoTTMysqlBit:
		return CompareString(d.GetBinaryLiteral().ToString(), s, d.defCauslation), nil
	default:
		fVal, err := StrToFloat(sc, s, false)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.compareFloat64(sc, fVal)
	}
}

func (d *Causet) compareBytes(sc *stmtctx.StatementContext, b []byte) (int, error) {
	str := string(replog.String(b))
	return d.compareString(sc, str, d.defCauslation)
}

func (d *Causet) compareMysqlDecimal(sc *stmtctx.StatementContext, dec *MyDecimal) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTMysqlDecimal:
		return d.GetMysqlDecimal().Compare(dec), nil
	case HoTTString, HoTTBytes:
		dDec := new(MyDecimal)
		err := sc.HandleTruncate(dDec.FromString(d.GetBytes()))
		return dDec.Compare(dec), errors.Trace(err)
	default:
		dVal, err := d.ConvertTo(sc, NewFieldType(allegrosql.TypeNewDecimal))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return dVal.GetMysqlDecimal().Compare(dec), nil
	}
}

func (d *Causet) compareMysqlDuration(sc *stmtctx.StatementContext, dur Duration) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTMysqlDuration:
		return d.GetMysqlDuration().Compare(dur), nil
	case HoTTString, HoTTBytes:
		dDur, err := ParseDuration(sc, d.GetString(), MaxFsp)
		return dDur.Compare(dur), errors.Trace(err)
	default:
		return d.compareFloat64(sc, dur.Seconds())
	}
}

func (d *Causet) compareMysqlEnum(sc *stmtctx.StatementContext, enum Enum) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTString, HoTTBytes, HoTTMysqlEnum, HoTTMysqlSet:
		return CompareString(d.GetString(), enum.String(), d.defCauslation), nil
	default:
		return d.compareFloat64(sc, enum.ToNumber())
	}
}

func (d *Causet) compareBinaryLiteral(sc *stmtctx.StatementContext, b BinaryLiteral) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTString, HoTTBytes:
		return CompareString(d.GetString(), b.ToString(), d.defCauslation), nil
	case HoTTBinaryLiteral, HoTTMysqlBit:
		return CompareString(d.GetBinaryLiteral().ToString(), b.ToString(), d.defCauslation), nil
	default:
		val, err := b.ToInt(sc)
		if err != nil {
			return 0, errors.Trace(err)
		}
		result, err := d.compareFloat64(sc, float64(val))
		return result, errors.Trace(err)
	}
}

func (d *Causet) compareMysqlSet(sc *stmtctx.StatementContext, set Set) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTString, HoTTBytes, HoTTMysqlEnum, HoTTMysqlSet:
		return CompareString(d.GetString(), set.String(), d.defCauslation), nil
	default:
		return d.compareFloat64(sc, set.ToNumber())
	}
}

func (d *Causet) compareMysqlJSON(sc *stmtctx.StatementContext, target json.BinaryJSON) (int, error) {
	origin, err := d.ToMysqlJSON()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return json.CompareBinary(origin, target), nil
}

func (d *Causet) compareMysqlTime(sc *stmtctx.StatementContext, time Time) (int, error) {
	switch d.k {
	case HoTTNull, HoTTMinNotNull:
		return -1, nil
	case HoTTMaxValue:
		return 1, nil
	case HoTTString, HoTTBytes:
		dt, err := ParseDatetime(sc, d.GetString())
		return dt.Compare(time), errors.Trace(err)
	case HoTTMysqlTime:
		return d.GetMysqlTime().Compare(time), nil
	default:
		fVal, err := time.ToNumber().ToFloat64()
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.compareFloat64(sc, fVal)
	}
}

// ConvertTo converts a causet to the target field type.
// change this method need sync modification to type2HoTT in rowcodec/types.go
func (d *Causet) ConvertTo(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	if d.k == HoTTNull {
		return Causet{}, nil
	}
	switch target.Tp { // TODO: implement allegrosql types convert when "CAST() AS" syntax are supported.
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		unsigned := allegrosql.HasUnsignedFlag(target.Flag)
		if unsigned {
			return d.convertToUint(sc, target)
		}
		return d.convertToInt(sc, target)
	case allegrosql.TypeFloat, allegrosql.TypeDouble:
		return d.convertToFloat(sc, target)
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob,
		allegrosql.TypeString, allegrosql.TypeVarchar, allegrosql.TypeVarString:
		return d.convertToString(sc, target)
	case allegrosql.TypeTimestamp:
		return d.convertToMysqlTimestamp(sc, target)
	case allegrosql.TypeDatetime, allegrosql.TypeDate:
		return d.convertToMysqlTime(sc, target)
	case allegrosql.TypeDuration:
		return d.convertToMysqlDuration(sc, target)
	case allegrosql.TypeNewDecimal:
		return d.convertToMysqlDecimal(sc, target)
	case allegrosql.TypeYear:
		return d.convertToMysqlYear(sc, target)
	case allegrosql.TypeEnum:
		return d.convertToMysqlEnum(sc, target)
	case allegrosql.TypeBit:
		return d.convertToMysqlBit(sc, target)
	case allegrosql.TypeSet:
		return d.convertToMysqlSet(sc, target)
	case allegrosql.TypeJSON:
		return d.convertToMysqlJSON(sc, target)
	case allegrosql.TypeNull:
		return Causet{}, nil
	default:
		panic("should never happen")
	}
}

func (d *Causet) convertToFloat(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var (
		f   float64
		ret Causet
		err error
	)
	switch d.k {
	case HoTTNull:
		return ret, nil
	case HoTTInt64:
		f = float64(d.GetInt64())
	case HoTTUint64:
		f = float64(d.GetUint64())
	case HoTTFloat32, HoTTFloat64:
		f = d.GetFloat64()
	case HoTTString, HoTTBytes:
		f, err = StrToFloat(sc, d.GetString(), false)
	case HoTTMysqlTime:
		f, err = d.GetMysqlTime().ToNumber().ToFloat64()
	case HoTTMysqlDuration:
		f, err = d.GetMysqlDuration().ToNumber().ToFloat64()
	case HoTTMysqlDecimal:
		f, err = d.GetMysqlDecimal().ToFloat64()
	case HoTTMysqlSet:
		f = d.GetMysqlSet().ToNumber()
	case HoTTMysqlEnum:
		f = d.GetMysqlEnum().ToNumber()
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		f, err = float64(val), err1
	case HoTTMysqlJSON:
		f, err = ConvertJSONToFloat(sc, d.GetMysqlJSON())
	default:
		return invalidConv(d, target.Tp)
	}
	var err1 error
	f, err1 = ProduceFloatWithSpecifiedTp(f, target, sc)
	if err == nil && err1 != nil {
		err = err1
	}
	if target.Tp == allegrosql.TypeFloat {
		ret.SetFloat32(float32(f))
	} else {
		ret.SetFloat64(f)
	}
	return ret, errors.Trace(err)
}

// ProduceFloatWithSpecifiedTp produces a new float64 according to `flen` and `decimal`.
func ProduceFloatWithSpecifiedTp(f float64, target *FieldType, sc *stmtctx.StatementContext) (_ float64, err error) {
	// For float and following double type, we will only truncate it for float(M, D) format.
	// If no D is set, we will handle it like origin float whether M is set or not.
	if target.Flen != UnspecifiedLength && target.Decimal != UnspecifiedLength {
		f, err = TruncateFloat(f, target.Flen, target.Decimal)
		if err = sc.HandleOverflow(err, err); err != nil {
			return f, errors.Trace(err)
		}
	}
	if allegrosql.HasUnsignedFlag(target.Flag) && f < 0 {
		return 0, overflow(f, target.Tp)
	}
	return f, nil
}

func (d *Causet) convertToString(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var ret Causet
	var s string
	switch d.k {
	case HoTTInt64:
		s = strconv.FormatInt(d.GetInt64(), 10)
	case HoTTUint64:
		s = strconv.FormatUint(d.GetUint64(), 10)
	case HoTTFloat32:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 32)
	case HoTTFloat64:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case HoTTString, HoTTBytes:
		s = d.GetString()
	case HoTTMysqlTime:
		s = d.GetMysqlTime().String()
	case HoTTMysqlDuration:
		s = d.GetMysqlDuration().String()
	case HoTTMysqlDecimal:
		s = d.GetMysqlDecimal().String()
	case HoTTMysqlEnum:
		s = d.GetMysqlEnum().String()
	case HoTTMysqlSet:
		s = d.GetMysqlSet().String()
	case HoTTBinaryLiteral, HoTTMysqlBit:
		s = d.GetBinaryLiteral().ToString()
	case HoTTMysqlJSON:
		s = d.GetMysqlJSON().String()
	default:
		return invalidConv(d, target.Tp)
	}
	s, err := ProduceStrWithSpecifiedTp(s, target, sc, true)
	ret.SetString(s, target.DefCauslate)
	if target.Charset == charset.CharsetBin {
		ret.k = HoTTBytes
	}
	return ret, errors.Trace(err)
}

// ProduceStrWithSpecifiedTp produces a new string according to `flen` and `chs`. Param `padZero` indicates
// whether we should pad `\0` for `binary(flen)` type.
func ProduceStrWithSpecifiedTp(s string, tp *FieldType, sc *stmtctx.StatementContext, padZero bool) (_ string, err error) {
	flen, chs := tp.Flen, tp.Charset
	if flen >= 0 {
		// Flen is the rune length, not binary length, for UTF8 charset, we need to calculate the
		// rune count and truncate to Flen runes if it is too long.
		if chs == charset.CharsetUTF8 || chs == charset.CharsetUTF8MB4 {
			characterLen := utf8.RuneCountInString(s)
			if characterLen > flen {
				// 1. If len(s) is 0 and flen is 0, truncateLen will be 0, don't truncate s.
				//    CREATE TABLE t (a char(0));
				//    INSERT INTO t VALUES (``);
				// 2. If len(s) is 10 and flen is 0, truncateLen will be 0 too, but we still need to truncate s.
				//    SELECT 1, CAST(1234 AS CHAR(0));
				// So truncateLen is not a suiblock variable to determine to do truncate or not.
				var runeCount int
				var truncateLen int
				for i := range s {
					if runeCount == flen {
						truncateLen = i
						break
					}
					runeCount++
				}
				err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d, data len %d", flen, characterLen)
				s = truncateStr(s, truncateLen)
			}
		} else if len(s) > flen {
			err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d, data len %d", flen, len(s))
			s = truncateStr(s, flen)
		} else if tp.Tp == allegrosql.TypeString && IsBinaryStr(tp) && len(s) < flen && padZero {
			padding := make([]byte, flen-len(s))
			s = string(append([]byte(s), padding...))
		}
	}
	return s, errors.Trace(sc.HandleTruncate(err))
}

func (d *Causet) convertToInt(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	i64, err := d.toSignedInteger(sc, target.Tp)
	return NewIntCauset(i64), errors.Trace(err)
}

func (d *Causet) convertToUint(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	tp := target.Tp
	upperBound := IntergerUnsignedUpperBound(tp)
	var (
		val uint64
		err error
		ret Causet
	)
	switch d.k {
	case HoTTInt64:
		val, err = ConvertIntToUint(sc, d.GetInt64(), upperBound, tp)
	case HoTTUint64:
		val, err = ConvertUintToUint(d.GetUint64(), upperBound, tp)
	case HoTTFloat32, HoTTFloat64:
		val, err = ConvertFloatToUint(sc, d.GetFloat64(), upperBound, tp)
	case HoTTString, HoTTBytes:
		uval, err1 := StrToUint(sc, d.GetString(), false)
		if err1 != nil && ErrOverflow.Equal(err1) && !sc.ShouldIgnoreOverflowError() {
			return ret, errors.Trace(err1)
		}
		val, err = ConvertUintToUint(uval, upperBound, tp)
		if err != nil {
			return ret, errors.Trace(err)
		}
		err = err1
	case HoTTMysqlTime:
		dec := d.GetMysqlTime().ToNumber()
		err = dec.Round(dec, 0, ModeHalfEven)
		ival, err1 := dec.ToInt()
		if err == nil {
			err = err1
		}
		val, err1 = ConvertIntToUint(sc, ival, upperBound, tp)
		if err == nil {
			err = err1
		}
	case HoTTMysqlDuration:
		dec := d.GetMysqlDuration().ToNumber()
		err = dec.Round(dec, 0, ModeHalfEven)
		ival, err1 := dec.ToInt()
		if err1 == nil {
			val, err = ConvertIntToUint(sc, ival, upperBound, tp)
		}
	case HoTTMysqlDecimal:
		val, err = ConvertDecimalToUint(sc, d.GetMysqlDecimal(), upperBound, tp)
	case HoTTMysqlEnum:
		val, err = ConvertFloatToUint(sc, d.GetMysqlEnum().ToNumber(), upperBound, tp)
	case HoTTMysqlSet:
		val, err = ConvertFloatToUint(sc, d.GetMysqlSet().ToNumber(), upperBound, tp)
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err = d.GetBinaryLiteral().ToInt(sc)
		if err == nil {
			val, err = ConvertUintToUint(val, upperBound, tp)
		}
	case HoTTMysqlJSON:
		var i64 int64
		i64, err = ConvertJSONToInt(sc, d.GetMysqlJSON(), true)
		val = uint64(i64)
	default:
		return invalidConv(d, target.Tp)
	}
	ret.SetUint64(val)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Causet) convertToMysqlTimestamp(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var (
		ret Causet
		t   Time
		err error
	)
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		fsp = int8(target.Decimal)
	}
	switch d.k {
	case HoTTMysqlTime:
		t = d.GetMysqlTime()
		t, err = t.RoundFrac(sc, fsp)
	case HoTTMysqlDuration:
		t, err = d.GetMysqlDuration().ConvertToTime(sc, allegrosql.TypeTimestamp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(sc, fsp)
	case HoTTString, HoTTBytes:
		t, err = ParseTime(sc, d.GetString(), allegrosql.TypeTimestamp, fsp)
	case HoTTInt64:
		t, err = ParseTimeFromNum(sc, d.GetInt64(), allegrosql.TypeTimestamp, fsp)
	case HoTTMysqlDecimal:
		t, err = ParseTimeFromFloatString(sc, d.GetMysqlDecimal().String(), allegrosql.TypeTimestamp, fsp)
	case HoTTMysqlJSON:
		j := d.GetMysqlJSON()
		var s string
		s, err = j.Unquote()
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, err
		}
		t, err = ParseTime(sc, s, allegrosql.TypeTimestamp, fsp)
	default:
		return invalidConv(d, allegrosql.TypeTimestamp)
	}
	t.SetType(allegrosql.TypeTimestamp)
	ret.SetMysqlTime(t)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Causet) convertToMysqlTime(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	tp := target.Tp
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		fsp = int8(target.Decimal)
	}
	var (
		ret Causet
		t   Time
		err error
	)
	switch d.k {
	case HoTTMysqlTime:
		t, err = d.GetMysqlTime().Convert(sc, tp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(sc, fsp)
	case HoTTMysqlDuration:
		t, err = d.GetMysqlDuration().ConvertToTime(sc, tp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(sc, fsp)
	case HoTTMysqlDecimal:
		t, err = ParseTimeFromFloatString(sc, d.GetMysqlDecimal().String(), tp, fsp)
	case HoTTString, HoTTBytes:
		t, err = ParseTime(sc, d.GetString(), tp, fsp)
	case HoTTInt64:
		t, err = ParseTimeFromNum(sc, d.GetInt64(), tp, fsp)
	case HoTTMysqlJSON:
		j := d.GetMysqlJSON()
		var s string
		s, err = j.Unquote()
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, err
		}
		t, err = ParseTime(sc, s, tp, fsp)
	default:
		return invalidConv(d, tp)
	}
	if tp == allegrosql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		t.SetCoreTime(FromDate(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0))
	}
	ret.SetMysqlTime(t)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Causet) convertToMysqlDuration(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	tp := target.Tp
	fsp := DefaultFsp
	if target.Decimal != UnspecifiedLength {
		fsp = int8(target.Decimal)
	}
	var ret Causet
	switch d.k {
	case HoTTMysqlTime:
		dur, err := d.GetMysqlTime().ConvertToDuration()
		if err != nil {
			ret.SetMysqlDuration(dur)
			return ret, errors.Trace(err)
		}
		dur, err = dur.RoundFrac(fsp)
		ret.SetMysqlDuration(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case HoTTMysqlDuration:
		dur, err := d.GetMysqlDuration().RoundFrac(fsp)
		ret.SetMysqlDuration(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case HoTTInt64, HoTTFloat32, HoTTFloat64, HoTTMysqlDecimal:
		// TODO: We need a ParseDurationFromNum to avoid the cost of converting a num to string.
		timeStr, err := d.ToString()
		if err != nil {
			return ret, errors.Trace(err)
		}
		timeNum, err := d.ToInt64(sc)
		if err != nil {
			return ret, errors.Trace(err)
		}
		// For huge numbers(>'0001-00-00 00-00-00') try full DATETIME in ParseDuration.
		if timeNum > MaxDuration && timeNum < 10000000000 {
			// allegrosql return max in no strict allegrosql mode.
			ret.SetMysqlDuration(Duration{Duration: MaxTime, Fsp: 0})
			return ret, ErrWrongValue.GenWithStackByArgs(TimeStr, timeStr)
		}
		if timeNum < -MaxDuration {
			return ret, ErrWrongValue.GenWithStackByArgs(TimeStr, timeStr)
		}
		t, err := ParseDuration(sc, timeStr, fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case HoTTString, HoTTBytes:
		t, err := ParseDuration(sc, d.GetString(), fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case HoTTMysqlJSON:
		j := d.GetMysqlJSON()
		s, err := j.Unquote()
		if err != nil {
			return ret, errors.Trace(err)
		}
		t, err := ParseDuration(sc, s, fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	default:
		return invalidConv(d, tp)
	}
	return ret, nil
}

func (d *Causet) convertToMysqlDecimal(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var ret Causet
	ret.SetLength(target.Flen)
	ret.SetFrac(target.Decimal)
	var dec = &MyDecimal{}
	var err error
	switch d.k {
	case HoTTInt64:
		dec.FromInt(d.GetInt64())
	case HoTTUint64:
		dec.FromUint(d.GetUint64())
	case HoTTFloat32, HoTTFloat64:
		err = dec.FromFloat64(d.GetFloat64())
	case HoTTString, HoTTBytes:
		err = dec.FromString(d.GetBytes())
	case HoTTMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case HoTTMysqlTime:
		dec = d.GetMysqlTime().ToNumber()
	case HoTTMysqlDuration:
		dec = d.GetMysqlDuration().ToNumber()
	case HoTTMysqlEnum:
		err = dec.FromFloat64(d.GetMysqlEnum().ToNumber())
	case HoTTMysqlSet:
		err = dec.FromFloat64(d.GetMysqlSet().ToNumber())
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		err = err1
		dec.FromUint(val)
	case HoTTMysqlJSON:
		f, err1 := ConvertJSONToFloat(sc, d.GetMysqlJSON())
		if err1 != nil {
			return ret, errors.Trace(err1)
		}
		err = dec.FromFloat64(f)
	default:
		return invalidConv(d, target.Tp)
	}
	var err1 error
	dec, err1 = ProduceDecWithSpecifiedTp(dec, target, sc)
	if err == nil && err1 != nil {
		err = err1
	}
	if dec.negative && allegrosql.HasUnsignedFlag(target.Flag) {
		*dec = zeroMyDecimal
		if err == nil {
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", target.Flen, target.Decimal))
		}
	}
	ret.SetMysqlDecimal(dec)
	return ret, err
}

// ProduceDecWithSpecifiedTp produces a new decimal according to `flen` and `decimal`.
func ProduceDecWithSpecifiedTp(dec *MyDecimal, tp *FieldType, sc *stmtctx.StatementContext) (_ *MyDecimal, err error) {
	flen, decimal := tp.Flen, tp.Decimal
	if flen != UnspecifiedLength && decimal != UnspecifiedLength {
		if flen < decimal {
			return nil, ErrMBiggerThanD.GenWithStackByArgs("")
		}
		prec, frac := dec.PrecisionAndFrac()
		if !dec.IsZero() && prec-frac > flen-decimal {
			dec = NewMaxOrMinDec(dec.IsNegative(), flen, decimal)
			// select (cast 111 as decimal(1)) causes a warning in MyALLEGROSQL.
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", flen, decimal))
		} else if frac != decimal {
			old := *dec
			err = dec.Round(dec, decimal, ModeHalfEven)
			if err != nil {
				return nil, err
			}
			if !dec.IsZero() && frac > decimal && dec.Compare(&old) != 0 {
				if sc.InInsertStmt || sc.InUFIDelateStmt || sc.InDeleteStmt {
					// fix https://github.com/whtcorpsinc/milevadb/issues/3895
					// fix https://github.com/whtcorpsinc/milevadb/issues/5532
					sc.AppendWarning(ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", &old))
					err = nil
				} else {
					err = sc.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", &old))
				}
			}
		}
	}

	if ErrOverflow.Equal(err) {
		// TODO: warnErr need to be ErrWarnDataOutOfRange
		err = sc.HandleOverflow(err, err)
	}
	unsigned := allegrosql.HasUnsignedFlag(tp.Flag)
	if unsigned && dec.IsNegative() {
		dec = dec.FromUint(0)
	}
	return dec, err
}

func (d *Causet) convertToMysqlYear(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var (
		ret    Causet
		y      int64
		err    error
		adjust bool
	)
	switch d.k {
	case HoTTString, HoTTBytes:
		s := d.GetString()
		y, err = StrToInt(sc, s, false)
		if err != nil {
			ret.SetInt64(0)
			return ret, errors.Trace(err)
		}
		if len(s) != 4 && len(s) > 0 && s[0:1] == "0" {
			adjust = true
		}
	case HoTTMysqlTime:
		y = int64(d.GetMysqlTime().Year())
	case HoTTMysqlDuration:
		y = int64(time.Now().Year())
	default:
		ret, err = d.convertToInt(sc, NewFieldType(allegrosql.TypeLonglong))
		if err != nil {
			_, err = invalidConv(d, target.Tp)
			ret.SetInt64(0)
			return ret, err
		}
		y = ret.GetInt64()
	}
	y, err = AdjustYear(y, adjust)
	if err != nil {
		_, err = invalidConv(d, target.Tp)
	}
	ret.SetInt64(y)
	return ret, err
}

func (d *Causet) convertToMysqlBit(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var ret Causet
	var uintValue uint64
	var err error
	switch d.k {
	case HoTTString, HoTTBytes:
		uintValue, err = BinaryLiteral(d.b).ToInt(sc)
	case HoTTInt64:
		// if input HoTT is int64 (signed), when trans to bit, we need to treat it as unsigned
		d.k = HoTTUint64
		fallthrough
	default:
		uintCauset, err1 := d.convertToUint(sc, target)
		uintValue, err = uintCauset.GetUint64(), err1
	}
	if target.Flen < 64 && uintValue >= 1<<(uint64(target.Flen)) {
		return Causet{}, errors.Trace(ErrDataTooLong.GenWithStack("Data Too Long, field len %d", target.Flen))
	}
	byteSize := (target.Flen + 7) >> 3
	ret.SetMysqlBit(NewBinaryLiteralFromUint(uintValue, byteSize))
	return ret, errors.Trace(err)
}

func (d *Causet) convertToMysqlEnum(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var (
		ret Causet
		e   Enum
		err error
	)
	switch d.k {
	case HoTTString, HoTTBytes:
		e, err = ParseEnumName(target.Elems, d.GetString(), target.DefCauslate)
	case HoTTMysqlEnum:
		e, err = ParseEnumName(target.Elems, d.GetMysqlEnum().Name, target.DefCauslate)
	case HoTTMysqlSet:
		e, err = ParseEnumName(target.Elems, d.GetMysqlSet().Name, target.DefCauslate)
	default:
		var uintCauset Causet
		uintCauset, err = d.convertToUint(sc, target)
		if err == nil {
			e, err = ParseEnumValue(target.Elems, uintCauset.GetUint64())
		}
	}
	if err != nil {
		err = errors.Wrap(ErrTruncated, "convert to MyALLEGROSQL enum failed: "+err.Error())
	}
	ret.SetMysqlEnum(e, target.DefCauslate)
	return ret, err
}

func (d *Causet) convertToMysqlSet(sc *stmtctx.StatementContext, target *FieldType) (Causet, error) {
	var (
		ret Causet
		s   Set
		err error
	)
	switch d.k {
	case HoTTString, HoTTBytes:
		s, err = ParseSetName(target.Elems, d.GetString(), target.DefCauslate)
	case HoTTMysqlEnum:
		s, err = ParseSetName(target.Elems, d.GetMysqlEnum().Name, target.DefCauslate)
	case HoTTMysqlSet:
		s, err = ParseSetName(target.Elems, d.GetMysqlSet().Name, target.DefCauslate)
	default:
		var uintCauset Causet
		uintCauset, err = d.convertToUint(sc, target)
		if err == nil {
			s, err = ParseSetValue(target.Elems, uintCauset.GetUint64())
		}
	}
	if err != nil {
		err = errors.Wrap(ErrTruncated, "convert to MyALLEGROSQL set failed: "+err.Error())
	}
	ret.SetMysqlSet(s, target.DefCauslate)
	return ret, err
}

func (d *Causet) convertToMysqlJSON(sc *stmtctx.StatementContext, target *FieldType) (ret Causet, err error) {
	switch d.k {
	case HoTTString, HoTTBytes:
		var j json.BinaryJSON
		if j, err = json.ParseBinaryFromString(d.GetString()); err == nil {
			ret.SetMysqlJSON(j)
		}
	case HoTTInt64:
		i64 := d.GetInt64()
		ret.SetMysqlJSON(json.CreateBinary(i64))
	case HoTTUint64:
		u64 := d.GetUint64()
		ret.SetMysqlJSON(json.CreateBinary(u64))
	case HoTTFloat32, HoTTFloat64:
		f64 := d.GetFloat64()
		ret.SetMysqlJSON(json.CreateBinary(f64))
	case HoTTMysqlDecimal:
		var f64 float64
		if f64, err = d.GetMysqlDecimal().ToFloat64(); err == nil {
			ret.SetMysqlJSON(json.CreateBinary(f64))
		}
	case HoTTMysqlJSON:
		ret = *d
	default:
		var s string
		if s, err = d.ToString(); err == nil {
			// TODO: fix precision of MysqlTime. For example,
			// On MyALLEGROSQL 5.7 CAST(NOW() AS JSON) -> "2011-11-11 11:11:11.111111",
			// But now we can only return "2011-11-11 11:11:11".
			ret.SetMysqlJSON(json.CreateBinary(s))
		}
	}
	return ret, errors.Trace(err)
}

// ToBool converts to a bool.
// We will use 1 for true, and 0 for false.
func (d *Causet) ToBool(sc *stmtctx.StatementContext) (int64, error) {
	var err error
	isZero := false
	switch d.HoTT() {
	case HoTTInt64:
		isZero = d.GetInt64() == 0
	case HoTTUint64:
		isZero = d.GetUint64() == 0
	case HoTTFloat32:
		isZero = d.GetFloat64() == 0
	case HoTTFloat64:
		isZero = d.GetFloat64() == 0
	case HoTTString, HoTTBytes:
		iVal, err1 := StrToFloat(sc, d.GetString(), false)
		isZero, err = iVal == 0, err1

	case HoTTMysqlTime:
		isZero = d.GetMysqlTime().IsZero()
	case HoTTMysqlDuration:
		isZero = d.GetMysqlDuration().Duration == 0
	case HoTTMysqlDecimal:
		isZero = d.GetMysqlDecimal().IsZero()
	case HoTTMysqlEnum:
		isZero = d.GetMysqlEnum().ToNumber() == 0
	case HoTTMysqlSet:
		isZero = d.GetMysqlSet().ToNumber() == 0
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		isZero, err = val == 0, err1
	case HoTTMysqlJSON:
		val := d.GetMysqlJSON()
		isZero = val.IsZero()
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", d.GetValue(), d.GetValue())
	}
	var ret int64
	if isZero {
		ret = 0
	} else {
		ret = 1
	}
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

// ConvertCausetToDecimal converts causet to decimal.
func ConvertCausetToDecimal(sc *stmtctx.StatementContext, d Causet) (*MyDecimal, error) {
	dec := new(MyDecimal)
	var err error
	switch d.HoTT() {
	case HoTTInt64:
		dec.FromInt(d.GetInt64())
	case HoTTUint64:
		dec.FromUint(d.GetUint64())
	case HoTTFloat32:
		err = dec.FromFloat64(float64(d.GetFloat32()))
	case HoTTFloat64:
		err = dec.FromFloat64(d.GetFloat64())
	case HoTTString:
		err = sc.HandleTruncate(dec.FromString(d.GetBytes()))
	case HoTTMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case HoTTMysqlEnum:
		dec.FromUint(d.GetMysqlEnum().Value)
	case HoTTMysqlSet:
		dec.FromUint(d.GetMysqlSet().Value)
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(sc)
		dec.FromUint(val)
		err = err1
	case HoTTMysqlJSON:
		f, err1 := ConvertJSONToFloat(sc, d.GetMysqlJSON())
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		err = dec.FromFloat64(f)
	default:
		err = fmt.Errorf("can't convert %v to decimal", d.GetValue())
	}
	return dec, errors.Trace(err)
}

// ToDecimal converts to a decimal.
func (d *Causet) ToDecimal(sc *stmtctx.StatementContext) (*MyDecimal, error) {
	switch d.HoTT() {
	case HoTTMysqlTime:
		return d.GetMysqlTime().ToNumber(), nil
	case HoTTMysqlDuration:
		return d.GetMysqlDuration().ToNumber(), nil
	default:
		return ConvertCausetToDecimal(sc, *d)
	}
}

// ToInt64 converts to a int64.
func (d *Causet) ToInt64(sc *stmtctx.StatementContext) (int64, error) {
	return d.toSignedInteger(sc, allegrosql.TypeLonglong)
}

func (d *Causet) toSignedInteger(sc *stmtctx.StatementContext, tp byte) (int64, error) {
	lowerBound := IntergerSignedLowerBound(tp)
	upperBound := IntergerSignedUpperBound(tp)
	switch d.HoTT() {
	case HoTTInt64:
		return ConvertIntToInt(d.GetInt64(), lowerBound, upperBound, tp)
	case HoTTUint64:
		return ConvertUintToInt(d.GetUint64(), upperBound, tp)
	case HoTTFloat32:
		return ConvertFloatToInt(float64(d.GetFloat32()), lowerBound, upperBound, tp)
	case HoTTFloat64:
		return ConvertFloatToInt(d.GetFloat64(), lowerBound, upperBound, tp)
	case HoTTString, HoTTBytes:
		iVal, err := StrToInt(sc, d.GetString(), false)
		iVal, err2 := ConvertIntToInt(iVal, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return iVal, errors.Trace(err)
	case HoTTMysqlTime:
		// 2011-11-10 11:11:11.999999 -> 20111110111112
		// 2011-11-10 11:59:59.999999 -> 20111110120000
		t, err := d.GetMysqlTime().RoundFrac(sc, DefaultFsp)
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := t.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case HoTTMysqlDuration:
		// 11:11:11.999999 -> 111112
		// 11:59:59.999999 -> 120000
		dur, err := d.GetMysqlDuration().RoundFrac(DefaultFsp)
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := dur.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case HoTTMysqlDecimal:
		var to MyDecimal
		err := d.GetMysqlDecimal().Round(&to, 0, ModeHalfEven)
		ival, err1 := to.ToInt()
		if err == nil {
			err = err1
		}
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case HoTTMysqlEnum:
		fval := d.GetMysqlEnum().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case HoTTMysqlSet:
		fval := d.GetMysqlSet().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case HoTTMysqlJSON:
		return ConvertJSONToInt(sc, d.GetMysqlJSON(), false)
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := ConvertUintToInt(val, upperBound, tp)
		return ival, errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", d.GetValue(), d.GetValue())
	}
}

// ToFloat64 converts to a float64
func (d *Causet) ToFloat64(sc *stmtctx.StatementContext) (float64, error) {
	switch d.HoTT() {
	case HoTTInt64:
		return float64(d.GetInt64()), nil
	case HoTTUint64:
		return float64(d.GetUint64()), nil
	case HoTTFloat32:
		return float64(d.GetFloat32()), nil
	case HoTTFloat64:
		return d.GetFloat64(), nil
	case HoTTString:
		return StrToFloat(sc, d.GetString(), false)
	case HoTTBytes:
		return StrToFloat(sc, string(d.GetBytes()), false)
	case HoTTMysqlTime:
		f, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case HoTTMysqlDuration:
		f, err := d.GetMysqlDuration().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case HoTTMysqlDecimal:
		f, err := d.GetMysqlDecimal().ToFloat64()
		return f, errors.Trace(err)
	case HoTTMysqlEnum:
		return d.GetMysqlEnum().ToNumber(), nil
	case HoTTMysqlSet:
		return d.GetMysqlSet().ToNumber(), nil
	case HoTTBinaryLiteral, HoTTMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(sc)
		return float64(val), errors.Trace(err)
	case HoTTMysqlJSON:
		f, err := ConvertJSONToFloat(sc, d.GetMysqlJSON())
		return f, errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to float64", d.GetValue(), d.GetValue())
	}
}

// ToString gets the string representation of the causet.
func (d *Causet) ToString() (string, error) {
	switch d.HoTT() {
	case HoTTInt64:
		return strconv.FormatInt(d.GetInt64(), 10), nil
	case HoTTUint64:
		return strconv.FormatUint(d.GetUint64(), 10), nil
	case HoTTFloat32:
		return strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32), nil
	case HoTTFloat64:
		return strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64), nil
	case HoTTString:
		return d.GetString(), nil
	case HoTTBytes:
		return d.GetString(), nil
	case HoTTMysqlTime:
		return d.GetMysqlTime().String(), nil
	case HoTTMysqlDuration:
		return d.GetMysqlDuration().String(), nil
	case HoTTMysqlDecimal:
		return d.GetMysqlDecimal().String(), nil
	case HoTTMysqlEnum:
		return d.GetMysqlEnum().String(), nil
	case HoTTMysqlSet:
		return d.GetMysqlSet().String(), nil
	case HoTTMysqlJSON:
		return d.GetMysqlJSON().String(), nil
	case HoTTBinaryLiteral, HoTTMysqlBit:
		return d.GetBinaryLiteral().ToString(), nil
	case HoTTNull:
		return "", nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", d.GetValue(), d.GetValue())
	}
}

// ToBytes gets the bytes representation of the causet.
func (d *Causet) ToBytes() ([]byte, error) {
	switch d.k {
	case HoTTString, HoTTBytes:
		return d.GetBytes(), nil
	default:
		str, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return []byte(str), nil
	}
}

// ToMysqlJSON is similar to convertToMysqlJSON, except the
// latter parses from string, but the former uses it as primitive.
func (d *Causet) ToMysqlJSON() (j json.BinaryJSON, err error) {
	var in interface{}
	switch d.HoTT() {
	case HoTTMysqlJSON:
		j = d.GetMysqlJSON()
		return
	case HoTTInt64:
		in = d.GetInt64()
	case HoTTUint64:
		in = d.GetUint64()
	case HoTTFloat32, HoTTFloat64:
		in = d.GetFloat64()
	case HoTTMysqlDecimal:
		in, err = d.GetMysqlDecimal().ToFloat64()
	case HoTTString, HoTTBytes:
		in = d.GetString()
	case HoTTBinaryLiteral, HoTTMysqlBit:
		in = d.GetBinaryLiteral().ToString()
	case HoTTNull:
		in = nil
	default:
		in, err = d.ToString()
	}
	if err != nil {
		err = errors.Trace(err)
		return
	}
	j = json.CreateBinary(in)
	return
}

func invalidConv(d *Causet, tp byte) (Causet, error) {
	return Causet{}, errors.Errorf("cannot convert causet from %s to type %s.", HoTTStr(d.HoTT()), TypeStr(tp))
}

// NewCauset creates a new Causet from an interface{}.
func NewCauset(in interface{}) (d Causet) {
	switch x := in.(type) {
	case []interface{}:
		d.SetValueWithDefaultDefCauslation(MakeCausets(x...))
	default:
		d.SetValueWithDefaultDefCauslation(in)
	}
	return d
}

// NewIntCauset creates a new Causet from an int64 value.
func NewIntCauset(i int64) (d Causet) {
	d.SetInt64(i)
	return d
}

// NewUintCauset creates a new Causet from an uint64 value.
func NewUintCauset(i uint64) (d Causet) {
	d.SetUint64(i)
	return d
}

// NewBytesCauset creates a new Causet from a byte slice.
func NewBytesCauset(b []byte) (d Causet) {
	d.SetBytes(b)
	return d
}

// NewStringCauset creates a new Causet from a string.
func NewStringCauset(s string) (d Causet) {
	d.SetString(s, allegrosql.DefaultDefCauslationName)
	return d
}

// NewDefCauslationStringCauset creates a new Causet from a string with defCauslation and length info.
func NewDefCauslationStringCauset(s string, defCauslation string, length int) (d Causet) {
	d.SetString(s, defCauslation)
	return d
}

// NewFloat64Causet creates a new Causet from a float64 value.
func NewFloat64Causet(f float64) (d Causet) {
	d.SetFloat64(f)
	return d
}

// NewFloat32Causet creates a new Causet from a float32 value.
func NewFloat32Causet(f float32) (d Causet) {
	d.SetFloat32(f)
	return d
}

// NewDurationCauset creates a new Causet from a Duration value.
func NewDurationCauset(dur Duration) (d Causet) {
	d.SetMysqlDuration(dur)
	return d
}

// NewTimeCauset creates a new Time from a Time value.
func NewTimeCauset(t Time) (d Causet) {
	d.SetMysqlTime(t)
	return d
}

// NewDecimalCauset creates a new Causet from a MyDecimal value.
func NewDecimalCauset(dec *MyDecimal) (d Causet) {
	d.SetMysqlDecimal(dec)
	return d
}

// NewJSONCauset creates a new Causet from a BinaryJSON value
func NewJSONCauset(j json.BinaryJSON) (d Causet) {
	d.SetMysqlJSON(j)
	return d
}

// NewBinaryLiteralCauset creates a new BinaryLiteral Causet for a BinaryLiteral value.
func NewBinaryLiteralCauset(b BinaryLiteral) (d Causet) {
	d.SetBinaryLiteral(b)
	return d
}

// NewMysqlBitCauset creates a new MysqlBit Causet for a BinaryLiteral value.
func NewMysqlBitCauset(b BinaryLiteral) (d Causet) {
	d.SetMysqlBit(b)
	return d
}

// NewMysqlEnumCauset creates a new MysqlEnum Causet for a Enum value.
func NewMysqlEnumCauset(e Enum) (d Causet) {
	d.SetMysqlEnum(e, allegrosql.DefaultDefCauslationName)
	return d
}

// NewDefCauslateMysqlEnumCauset create a new MysqlEnum Causet for a Enum value with defCauslation information.
func NewDefCauslateMysqlEnumCauset(e Enum, defCauslation string) (d Causet) {
	d.SetMysqlEnum(e, defCauslation)
	return d
}

// NewMysqlSetCauset creates a new MysqlSet Causet for a Enum value.
func NewMysqlSetCauset(e Set, defCauslation string) (d Causet) {
	d.SetMysqlSet(e, defCauslation)
	return d
}

// MakeCausets creates causet slice from interfaces.
func MakeCausets(args ...interface{}) []Causet {
	datums := make([]Causet, len(args))
	for i, v := range args {
		datums[i] = NewCauset(v)
	}
	return datums
}

// MinNotNullCauset returns a causet represents minimum not null value.
func MinNotNullCauset() Causet {
	return Causet{k: HoTTMinNotNull}
}

// MaxValueCauset returns a causet represents max value.
func MaxValueCauset() Causet {
	return Causet{k: HoTTMaxValue}
}

// EqualCausets compare if a and b contains the same causet values.
func EqualCausets(sc *stmtctx.StatementContext, a []Causet, b []Causet) (bool, error) {
	if len(a) != len(b) {
		return false, nil
	}
	if a == nil && b == nil {
		return true, nil
	}
	if a == nil || b == nil {
		return false, nil
	}
	for i, ai := range a {
		v, err := ai.CompareCauset(sc, &b[i])
		if err != nil {
			return false, errors.Trace(err)
		}
		if v != 0 {
			return false, nil
		}
	}
	return true, nil
}

// SortCausets sorts a slice of causet.
func SortCausets(sc *stmtctx.StatementContext, datums []Causet) error {
	sorter := datumsSorter{datums: datums, sc: sc}
	sort.Sort(&sorter)
	return sorter.err
}

type datumsSorter struct {
	datums []Causet
	sc     *stmtctx.StatementContext
	err    error
}

func (ds *datumsSorter) Len() int {
	return len(ds.datums)
}

func (ds *datumsSorter) Less(i, j int) bool {
	cmp, err := ds.datums[i].CompareCauset(ds.sc, &ds.datums[j])
	if err != nil {
		ds.err = errors.Trace(err)
		return true
	}
	return cmp < 0
}

func (ds *datumsSorter) Swap(i, j int) {
	ds.datums[i], ds.datums[j] = ds.datums[j], ds.datums[i]
}

func handleTruncateError(sc *stmtctx.StatementContext, err error) error {
	if sc.IgnoreTruncate {
		return nil
	}
	if !sc.TruncateAsWarning {
		return err
	}
	sc.AppendWarning(err)
	return nil
}

// CausetsToString converts several datums to formatted string.
func CausetsToString(datums []Causet, handleSpecialValue bool) (string, error) {
	strs := make([]string, 0, len(datums))
	for _, causet := range datums {
		if handleSpecialValue {
			switch causet.HoTT() {
			case HoTTNull:
				strs = append(strs, "NULL")
				continue
			case HoTTMinNotNull:
				strs = append(strs, "-inf")
				continue
			case HoTTMaxValue:
				strs = append(strs, "+inf")
				continue
			}
		}
		str, err := causet.ToString()
		if err != nil {
			return "", errors.Trace(err)
		}
		strs = append(strs, str)
	}
	size := len(datums)
	if size > 1 {
		strs[0] = "(" + strs[0]
		strs[size-1] = strs[size-1] + ")"
	}
	return strings.Join(strs, ", "), nil
}

// CausetsToStrNoErr converts some datums to a formatted string.
// If an error occurs, it will print a log instead of returning an error.
func CausetsToStrNoErr(datums []Causet) string {
	str, err := CausetsToString(datums, true)
	terror.Log(errors.Trace(err))
	return str
}

// CloneEvent deep copies a Causet slice.
func CloneEvent(dr []Causet) []Causet {
	c := make([]Causet, len(dr))
	for i, d := range dr {
		d.Copy(&c[i])
	}
	return c
}

// GetMaxValue returns the max value causet for each type.
func GetMaxValue(ft *FieldType) (max Causet) {
	switch ft.Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		if allegrosql.HasUnsignedFlag(ft.Flag) {
			max.SetUint64(IntergerUnsignedUpperBound(ft.Tp))
		} else {
			max.SetInt64(IntergerSignedUpperBound(ft.Tp))
		}
	case allegrosql.TypeFloat:
		max.SetFloat32(float32(GetMaxFloat(ft.Flen, ft.Decimal)))
	case allegrosql.TypeDouble:
		max.SetFloat64(GetMaxFloat(ft.Flen, ft.Decimal))
	case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar:
		// codec.Encode HoTTMaxValue, to avoid import circle
		bytes := []byte{250}
		max.SetString(string(bytes), ft.DefCauslate)
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		// codec.Encode HoTTMaxValue, to avoid import circle
		bytes := []byte{250}
		max.SetBytes(bytes)
	case allegrosql.TypeNewDecimal:
		max.SetMysqlDecimal(NewMaxOrMinDec(false, ft.Flen, ft.Decimal))
	case allegrosql.TypeDuration:
		max.SetMysqlDuration(Duration{Duration: MaxTime})
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		if ft.Tp == allegrosql.TypeDate || ft.Tp == allegrosql.TypeDatetime {
			max.SetMysqlTime(NewTime(MaxDatetime, ft.Tp, 0))
		} else {
			max.SetMysqlTime(MaxTimestamp)
		}
	}
	return
}

// GetMinValue returns the min value causet for each type.
func GetMinValue(ft *FieldType) (min Causet) {
	switch ft.Tp {
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong:
		if allegrosql.HasUnsignedFlag(ft.Flag) {
			min.SetUint64(0)
		} else {
			min.SetInt64(IntergerSignedLowerBound(ft.Tp))
		}
	case allegrosql.TypeFloat:
		min.SetFloat32(float32(-GetMaxFloat(ft.Flen, ft.Decimal)))
	case allegrosql.TypeDouble:
		min.SetFloat64(-GetMaxFloat(ft.Flen, ft.Decimal))
	case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar:
		// codec.Encode HoTTMinNotNull, to avoid import circle
		bytes := []byte{1}
		min.SetString(string(bytes), ft.DefCauslate)
	case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		// codec.Encode HoTTMinNotNull, to avoid import circle
		bytes := []byte{1}
		min.SetBytes(bytes)
	case allegrosql.TypeNewDecimal:
		min.SetMysqlDecimal(NewMaxOrMinDec(true, ft.Flen, ft.Decimal))
	case allegrosql.TypeDuration:
		min.SetMysqlDuration(Duration{Duration: MinTime})
	case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		if ft.Tp == allegrosql.TypeDate || ft.Tp == allegrosql.TypeDatetime {
			min.SetMysqlTime(NewTime(MinDatetime, ft.Tp, 0))
		} else {
			min.SetMysqlTime(MinTimestamp)
		}
	}
	return
}

// RoundingType is used to indicate the rounding type for reversing evaluation.
type RoundingType uint8

const (
	// Ceiling means rounding up.
	Ceiling RoundingType = iota
	// Floor means rounding down.
	Floor
)

func getCausetBound(retType *FieldType, rType RoundingType) Causet {
	if rType == Ceiling {
		return GetMaxValue(retType)
	}
	return GetMinValue(retType)
}

// ChangeReverseResultByUpperLowerBound is for expression's reverse evaluation.
// Here is an example for what's effort for the function: CastRealAsInt(t.a),
// 		if the type of defCausumn `t.a` is allegrosql.TypeDouble, and there is a event that t.a == MaxFloat64
// 		then the cast function will arrive a result MaxInt64. But when we do the reverse evaluation,
//      if the result is MaxInt64, and the rounding type is ceiling. Then we should get the MaxFloat64
//      instead of float64(MaxInt64).
// Another example: cast(1.1 as signed) = 1,
// 		when we get the answer 1, we can only reversely evaluate 1.0 as the defCausumn value. So in this
// 		case, we should judge whether the rounding type are ceiling. If it is, then we should plus one for
// 		1.0 and get the reverse result 2.0.
func ChangeReverseResultByUpperLowerBound(
	sc *stmtctx.StatementContext,
	retType *FieldType,
	res Causet,
	rType RoundingType) (Causet, error) {
	d, err := res.ConvertTo(sc, retType)
	if terror.ErrorEqual(err, ErrOverflow) {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	resRetType := FieldType{}
	switch res.HoTT() {
	case HoTTInt64:
		resRetType.Tp = allegrosql.TypeLonglong
	case HoTTUint64:
		resRetType.Tp = allegrosql.TypeLonglong
		resRetType.Flag |= allegrosql.UnsignedFlag
	case HoTTFloat32:
		resRetType.Tp = allegrosql.TypeFloat
	case HoTTFloat64:
		resRetType.Tp = allegrosql.TypeDouble
	case HoTTMysqlDecimal:
		resRetType.Tp = allegrosql.TypeNewDecimal
		resRetType.Flen = int(res.GetMysqlDecimal().GetDigitsFrac() + res.GetMysqlDecimal().GetDigitsInt())
		resRetType.Decimal = int(res.GetMysqlDecimal().GetDigitsInt())
	}
	bound := getCausetBound(&resRetType, rType)
	cmp, err := d.CompareCauset(sc, &bound)
	if err != nil {
		return d, err
	}
	if cmp == 0 {
		d = getCausetBound(retType, rType)
	} else if rType == Ceiling {
		switch retType.Tp {
		case allegrosql.TypeShort:
			if allegrosql.HasUnsignedFlag(retType.Flag) {
				if d.GetUint64() != math.MaxUint16 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt16 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case allegrosql.TypeLong:
			if allegrosql.HasUnsignedFlag(retType.Flag) {
				if d.GetUint64() != math.MaxUint32 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt32 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case allegrosql.TypeLonglong:
			if allegrosql.HasUnsignedFlag(retType.Flag) {
				if d.GetUint64() != math.MaxUint64 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt64 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case allegrosql.TypeFloat:
			if d.GetFloat32() != math.MaxFloat32 {
				d.SetFloat32(d.GetFloat32() + 1.0)
			}
		case allegrosql.TypeDouble:
			if d.GetFloat64() != math.MaxFloat64 {
				d.SetFloat64(d.GetFloat64() + 1.0)
			}
		case allegrosql.TypeNewDecimal:
			if d.GetMysqlDecimal().Compare(NewMaxOrMinDec(false, retType.Flen, retType.Decimal)) != 0 {
				var decimalOne, newD MyDecimal
				one := decimalOne.FromInt(1)
				err = DecimalAdd(d.GetMysqlDecimal(), one, &newD)
				if err != nil {
					return d, err
				}
				d = NewDecimalCauset(&newD)
			}
		}
	}
	return d, nil
}

const (
	sizeOfEmptyCauset = int(unsafe.Sizeof(Causet{}))
	sizeOfMysqlTime   = int(unsafe.Sizeof(ZeroTime))
	sizeOfMyDecimal   = MyDecimalStructSize
)

// EstimatedMemUsage returns the estimated bytes consumed of a one-dimensional
// or two-dimensional causet array.
func EstimatedMemUsage(array []Causet, numOfEvents int) int64 {
	if numOfEvents == 0 {
		return 0
	}
	var bytesConsumed int
	for _, d := range array {
		switch d.HoTT() {
		case HoTTMysqlDecimal:
			bytesConsumed += sizeOfMyDecimal
		case HoTTMysqlTime:
			bytesConsumed += sizeOfMysqlTime
		default:
			bytesConsumed += len(d.b)
		}
	}
	bytesConsumed += len(array) * sizeOfEmptyCauset
	return int64(bytesConsumed * numOfEvents)
}
