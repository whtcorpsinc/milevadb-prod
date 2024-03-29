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

package memex

// This file contains benchmarks of our memex evaluation.

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/math"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
)

type benchHelper struct {
	ctx   stochastikctx.Context
	exprs []Expression

	inputTypes  []*types.FieldType
	outputTypes []*types.FieldType
	inputChunk  *chunk.Chunk
	outputChunk *chunk.Chunk
}

func (h *benchHelper) init() {
	numEvents := 4 * 1024

	h.ctx = mock.NewContext()
	h.ctx.GetStochastikVars().StmtCtx.TimeZone = time.Local
	h.ctx.GetStochastikVars().InitChunkSize = 32
	h.ctx.GetStochastikVars().MaxChunkSize = numEvents

	h.inputTypes = make([]*types.FieldType, 0, 10)
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      allegrosql.TypeLonglong,
		Flen:    allegrosql.MaxIntWidth,
		Decimal: 0,
		Flag:    allegrosql.BinaryFlag,
		Charset: charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	})
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      allegrosql.TypeDouble,
		Flen:    allegrosql.MaxRealWidth,
		Decimal: types.UnspecifiedLength,
		Flag:    allegrosql.BinaryFlag,
		Charset: charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	})
	h.inputTypes = append(h.inputTypes, &types.FieldType{
		Tp:      allegrosql.TypeNewDecimal,
		Flen:    11,
		Decimal: 0,
		Flag:    allegrosql.BinaryFlag,
		Charset: charset.CharsetBin,
		DefCauslate: charset.DefCauslationBin,
	})

	// Use 20 string defCausumns to show the cache performance.
	for i := 0; i < 20; i++ {
		h.inputTypes = append(h.inputTypes, &types.FieldType{
			Tp:      allegrosql.TypeVarString,
			Flen:    0,
			Decimal: types.UnspecifiedLength,
			Charset: charset.CharsetUTF8,
			DefCauslate: charset.DefCauslationUTF8,
		})
	}

	h.inputChunk = chunk.NewChunkWithCapacity(h.inputTypes, numEvents)
	for rowIdx := 0; rowIdx < numEvents; rowIdx++ {
		h.inputChunk.AppendInt64(0, 4)
		h.inputChunk.AppendFloat64(1, 2.019)
		h.inputChunk.AppendMyDecimal(2, types.NewDecFromFloatForTest(5.9101))
		for i := 0; i < 20; i++ {
			h.inputChunk.AppendString(3+i, `abcdefughasfjsaljal1321798273528791!&(*#&@&^%&%^&!)sadfashqwer`)
		}
	}

	defcaus := make([]*DeferredCauset, 0, len(h.inputTypes))
	for i := 0; i < len(h.inputTypes); i++ {
		defcaus = append(defcaus, &DeferredCauset{
			UniqueID: int64(i),
			RetType:  h.inputTypes[i],
			Index:    i,
		})
	}

	h.exprs = make([]Expression, 0, 10)
	if expr, err := NewFunction(h.ctx, ast.Substr, h.inputTypes[3], []Expression{defcaus[3], defcaus[2]}...); err != nil {
		panic("create SUBSTR function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.Plus, h.inputTypes[0], []Expression{defcaus[1], defcaus[2]}...); err != nil {
		panic("create PLUS function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{defcaus[11], defcaus[8]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{defcaus[19], defcaus[10]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{defcaus[17], defcaus[4]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.GT, h.inputTypes[2], []Expression{defcaus[18], defcaus[5]}...); err != nil {
		panic("create GT function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.LE, h.inputTypes[2], []Expression{defcaus[19], defcaus[4]}...); err != nil {
		panic("create LE function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}

	if expr, err := NewFunction(h.ctx, ast.EQ, h.inputTypes[2], []Expression{defcaus[20], defcaus[3]}...); err != nil {
		panic("create EQ function failed.")
	} else {
		h.exprs = append(h.exprs, expr)
	}
	h.exprs = append(h.exprs, defcaus[2])
	h.exprs = append(h.exprs, defcaus[2])

	h.outputTypes = make([]*types.FieldType, 0, len(h.exprs))
	for i := 0; i < len(h.exprs); i++ {
		h.outputTypes = append(h.outputTypes, h.exprs[i].GetType())
	}

	h.outputChunk = chunk.NewChunkWithCapacity(h.outputTypes, numEvents)
}

func BenchmarkVectorizedInterDircute(b *testing.B) {
	h := benchHelper{}
	h.init()
	inputIter := chunk.NewIterator4Chunk(h.inputChunk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.outputChunk.Reset()
		if err := VectorizedInterDircute(h.ctx, h.exprs, inputIter, h.outputChunk); err != nil {
			panic("errors happened during \"VectorizedInterDircute\"")
		}
	}
}

func BenchmarkScalarFunctionClone(b *testing.B) {
	defCaus := &DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeLonglong)}
	con1 := NewOne()
	con2 := NewZero()
	add := NewFunctionInternal(mock.NewContext(), ast.Plus, types.NewFieldType(allegrosql.TypeLonglong), defCaus, con1)
	sub := NewFunctionInternal(mock.NewContext(), ast.Plus, types.NewFieldType(allegrosql.TypeLonglong), add, con2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sub.Clone()
	}
	b.ReportAllocs()
}

func getRandomTime(r *rand.Rand) types.CoreTime {
	return types.FromDate(r.Intn(2200), r.Intn(10)+1, r.Intn(20)+1,
		r.Intn(12), r.Intn(60), r.Intn(60), r.Intn(1000000))

}

// dataGenerator is used to generate data for test.
type dataGenerator interface {
	gen() interface{}
}

type defaultRandGen struct {
	*rand.Rand
}

func newDefaultRandGen() *defaultRandGen {
	return &defaultRandGen{rand.New(rand.NewSource(int64(rand.Uint64())))}
}

type defaultGener struct {
	nullRation float64
	eType      types.EvalType
	randGen    *defaultRandGen
}

func newDefaultGener(nullRation float64, eType types.EvalType) *defaultGener {
	return &defaultGener{
		nullRation: nullRation,
		eType:      eType,
		randGen:    newDefaultRandGen(),
	}
}

func (g *defaultGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	switch g.eType {
	case types.ETInt:
		if g.randGen.Float64() < 0.5 {
			return -g.randGen.Int63()
		}
		return g.randGen.Int63()
	case types.ETReal:
		if g.randGen.Float64() < 0.5 {
			return -g.randGen.Float64() * 1000000
		}
		return g.randGen.Float64() * 1000000
	case types.ETDecimal:
		d := new(types.MyDecimal)
		var f float64
		if g.randGen.Float64() < 0.5 {
			f = g.randGen.Float64() * 100000
		} else {
			f = -g.randGen.Float64() * 100000
		}
		if err := d.FromFloat64(f); err != nil {
			panic(err)
		}
		return d
	case types.ETDatetime, types.ETTimestamp:
		gt := getRandomTime(g.randGen.Rand)
		t := types.NewTime(gt, convertETType(g.eType), 0)
		return t
	case types.ETDuration:
		d := types.Duration{
			// use rand.Int32() to make it not overflow when AddDuration
			Duration: time.Duration(g.randGen.Int31()),
		}
		return d
	case types.ETJson:
		j := new(json.BinaryJSON)
		if err := j.UnmarshalJSON([]byte(fmt.Sprintf(`{"key":%v}`, g.randGen.Int()))); err != nil {
			panic(err)
		}
		return *j
	case types.ETString:
		return randString(g.randGen.Rand)
	}
	return nil
}

// charInt64Gener is used to generate int which is equal to char's ascii
type charInt64Gener struct{}

func (g *charInt64Gener) gen() interface{} {
	rand := time.Now().Nanosecond()
	rand = rand % 1024
	return int64(rand)
}

// charsetStringGener is used to generate "ascii" or "gbk"
type charsetStringGener struct{}

func (g *charsetStringGener) gen() interface{} {
	rand := time.Now().Nanosecond() % 3
	if rand == 0 {
		return "ascii"
	}
	if rand == 1 {
		return "utf8"
	}
	return "gbk"
}

// selectStringGener select one string randomly from the candidates array
type selectStringGener struct {
	candidates []string
	randGen    *defaultRandGen
}

func newSelectStringGener(candidates []string) *selectStringGener {
	return &selectStringGener{candidates, newDefaultRandGen()}
}

func (g *selectStringGener) gen() interface{} {
	if len(g.candidates) == 0 {
		return nil
	}
	return g.candidates[g.randGen.Intn(len(g.candidates))]
}

// selectRealGener select one real number randomly from the candidates array
type selectRealGener struct {
	candidates []float64
	randGen    *defaultRandGen
}

func newSelectRealGener(candidates []float64) *selectRealGener {
	return &selectRealGener{candidates, newDefaultRandGen()}
}

func (g *selectRealGener) gen() interface{} {
	if len(g.candidates) == 0 {
		return nil
	}
	return g.candidates[g.randGen.Intn(len(g.candidates))]
}

type constJSONGener struct {
	jsonStr string
}

func (g *constJSONGener) gen() interface{} {
	j := new(json.BinaryJSON)
	if err := j.UnmarshalJSON([]byte(g.jsonStr)); err != nil {
		panic(err)
	}
	return *j
}

type decimalJSONGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func newDecimalJSONGener(nullRation float64) *decimalJSONGener {
	return &decimalJSONGener{nullRation, newDefaultRandGen()}
}

func (g *decimalJSONGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}

	var f float64
	if g.randGen.Float64() < 0.5 {
		f = g.randGen.Float64() * 100000
	} else {
		f = -g.randGen.Float64() * 100000
	}
	if err := (&types.MyDecimal{}).FromFloat64(f); err != nil {
		panic(err)
	}
	return json.CreateBinary(f)
}

type jsonStringGener struct {
	randGen *defaultRandGen
}

func newJSONStringGener() *jsonStringGener {
	return &jsonStringGener{newDefaultRandGen()}
}

func (g *jsonStringGener) gen() interface{} {
	j := new(json.BinaryJSON)
	if err := j.UnmarshalJSON([]byte(fmt.Sprintf(`{"key":%v}`, g.randGen.Int()))); err != nil {
		panic(err)
	}
	return j.String()
}

type decimalStringGener struct {
	randGen *defaultRandGen
}

func newDecimalStringGener() *decimalStringGener {
	return &decimalStringGener{newDefaultRandGen()}
}

func (g *decimalStringGener) gen() interface{} {
	temFIDelecimal := new(types.MyDecimal)
	if err := temFIDelecimal.FromFloat64(g.randGen.Float64()); err != nil {
		panic(err)
	}
	return temFIDelecimal.String()
}

type realStringGener struct {
	randGen *defaultRandGen
}

func newRealStringGener() *realStringGener {
	return &realStringGener{newDefaultRandGen()}
}

func (g *realStringGener) gen() interface{} {
	return fmt.Sprintf("%f", g.randGen.Float64())
}

type jsonTimeGener struct {
	randGen *defaultRandGen
}

func newJSONTimeGener() *jsonTimeGener {
	return &jsonTimeGener{newDefaultRandGen()}
}

func (g *jsonTimeGener) gen() interface{} {
	tm := types.NewTime(getRandomTime(g.randGen.Rand), allegrosql.TypeDatetime, types.DefaultFsp)
	return json.CreateBinary(tm.String())
}

type rangeDurationGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func newRangeDurationGener(nullRation float64) *rangeDurationGener {
	return &rangeDurationGener{nullRation, newDefaultRandGen()}
}

func (g *rangeDurationGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	tm := (math.Abs(g.randGen.Int63n(12))*3600 + math.Abs(g.randGen.Int63n(60))*60 + math.Abs(g.randGen.Int63n(60))) * 1000
	tu := (tm + math.Abs(g.randGen.Int63n(1000))) * 1000
	return types.Duration{
		Duration: time.Duration(tu * 1000)}
}

type timeFormatGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func newTimeFormatGener(nullRation float64) *timeFormatGener {
	return &timeFormatGener{nullRation, newDefaultRandGen()}
}

func (g *timeFormatGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	switch g.randGen.Uint32() % 4 {
	case 0:
		return "%H %i %S"
	case 1:
		return "%l %i %s"
	case 2:
		return "%p %i %s"
	case 3:
		return "%I %i %S %f"
	case 4:
		return "%T"
	default:
		return nil
	}
}

// rangeRealGener is used to generate float64 items in [begin, end].
type rangeRealGener struct {
	begin float64
	end   float64

	nullRation float64
	randGen    *defaultRandGen
}

func newRangeRealGener(begin, end, nullRation float64) *rangeRealGener {
	return &rangeRealGener{begin, end, nullRation, newDefaultRandGen()}
}

func (g *rangeRealGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	if g.end < g.begin {
		g.begin = -100
		g.end = 100
	}
	return g.randGen.Float64()*(g.end-g.begin) + g.begin
}

// rangeDecimalGener is used to generate decimal items in [begin, end].
type rangeDecimalGener struct {
	begin float64
	end   float64

	nullRation float64
	randGen    *defaultRandGen
}

func newRangeDecimalGener(begin, end, nullRation float64) *rangeDecimalGener {
	return &rangeDecimalGener{begin, end, nullRation, newDefaultRandGen()}
}

func (g *rangeDecimalGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	if g.end < g.begin {
		g.begin = -100000
		g.end = 100000
	}
	d := new(types.MyDecimal)
	f := g.randGen.Float64()*(g.end-g.begin) + g.begin
	if err := d.FromFloat64(f); err != nil {
		panic(err)
	}
	return d
}

// rangeInt64Gener is used to generate int64 items in [begin, end).
type rangeInt64Gener struct {
	begin   int
	end     int
	randGen *defaultRandGen
}

func newRangeInt64Gener(begin, end int) *rangeInt64Gener {
	return &rangeInt64Gener{begin, end, newDefaultRandGen()}
}

func (rig *rangeInt64Gener) gen() interface{} {
	return int64(rig.randGen.Intn(rig.end-rig.begin) + rig.begin)
}

// numStrGener is used to generate number strings.
type numStrGener struct {
	rangeInt64Gener
}

func (g *numStrGener) gen() interface{} {
	return fmt.Sprintf("%v", g.rangeInt64Gener.gen())
}

// ipv6StrGener is used to generate ipv6 strings.
type ipv6StrGener struct {
	randGen *defaultRandGen
}

func (g *ipv6StrGener) gen() interface{} {
	var ip net.IP = make([]byte, net.IPv6len)
	for i := range ip {
		ip[i] = uint8(g.randGen.Intn(256))
	}
	return ip.String()
}

// ipv4StrGener is used to generate ipv4 strings. For example 111.111.111.111
type ipv4StrGener struct {
	randGen *defaultRandGen
}

func (g *ipv4StrGener) gen() interface{} {
	var ip net.IP = make([]byte, net.IPv4len)
	for i := range ip {
		ip[i] = uint8(g.randGen.Intn(256))
	}
	return ip.String()
}

// ipv6ByteGener is used to generate ipv6 address in 16 bytes string.
type ipv6ByteGener struct {
	randGen *defaultRandGen
}

func (g *ipv6ByteGener) gen() interface{} {
	var ip = make([]byte, net.IPv6len)
	for i := range ip {
		ip[i] = uint8(g.randGen.Intn(256))
	}
	return string(ip[:net.IPv6len])
}

// ipv4ByteGener is used to generate ipv4 address in 4 bytes string.
type ipv4ByteGener struct {
	randGen *defaultRandGen
}

func (g *ipv4ByteGener) gen() interface{} {
	var ip = make([]byte, net.IPv4len)
	for i := range ip {
		ip[i] = uint8(g.randGen.Intn(256))
	}
	return string(ip[:net.IPv4len])
}

// ipv4Compat is used to generate ipv4 compatible ipv6 strings
type ipv4CompatByteGener struct {
	randGen *defaultRandGen
}

func (g *ipv4CompatByteGener) gen() interface{} {
	var ip = make([]byte, net.IPv6len)
	for i := range ip {
		if i < 12 {
			ip[i] = 0
		} else {
			ip[i] = uint8(g.randGen.Intn(256))
		}
	}
	return string(ip[:net.IPv6len])
}

// ipv4MappedByteGener is used to generate ipv4-mapped ipv6 bytes.
type ipv4MappedByteGener struct {
	randGen *defaultRandGen
}

func (g *ipv4MappedByteGener) gen() interface{} {
	var ip = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0, 0, 0}
	for i := 12; i < 16; i++ {
		ip[i] = uint8(g.randGen.Intn(256)) // reset the last 4 bytes
	}
	return string(ip[:net.IPv6len])
}

// randLenStrGener is used to generate strings whose lengths are in [lenBegin, lenEnd).
type randLenStrGener struct {
	lenBegin int
	lenEnd   int
	randGen  *defaultRandGen
}

func newRandLenStrGener(lenBegin, lenEnd int) *randLenStrGener {
	return &randLenStrGener{lenBegin, lenEnd, newDefaultRandGen()}
}

func (g *randLenStrGener) gen() interface{} {
	n := g.randGen.Intn(g.lenEnd-g.lenBegin) + g.lenBegin
	buf := make([]byte, n)
	for i := range buf {
		x := g.randGen.Intn(62)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else {
			buf[i] = byte('A' + x - 10 - 26)
		}
	}
	return string(buf)
}

type randHexStrGener struct {
	lenBegin int
	lenEnd   int
	randGen  *defaultRandGen
}

func newRandHexStrGener(lenBegin, lenEnd int) *randHexStrGener {
	return &randHexStrGener{lenBegin, lenEnd, newDefaultRandGen()}
}

func (g *randHexStrGener) gen() interface{} {
	n := g.randGen.Intn(g.lenEnd-g.lenBegin) + g.lenBegin
	buf := make([]byte, n)
	for i := range buf {
		x := g.randGen.Intn(16)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else {
			if x%2 == 0 {
				buf[i] = byte('a' + x - 10)
			} else {
				buf[i] = byte('A' + x - 10)
			}
		}
	}
	return string(buf)
}

// dateTimeGener is used to generate a dataTime
type dateTimeGener struct {
	Fsp     int
	Year    int
	Month   int
	Day     int
	randGen *defaultRandGen
}

func newDateTimeGener(fsp, year, month, day int) *dateTimeGener {
	return &dateTimeGener{fsp, year, month, day, newDefaultRandGen()}
}

func (g *dateTimeGener) gen() interface{} {
	if g.Year == 0 {
		g.Year = 1970 + g.randGen.Intn(100)
	}
	if g.Month == 0 {
		g.Month = g.randGen.Intn(10) + 1
	}
	if g.Day == 0 {
		g.Day = g.randGen.Intn(20) + 1
	}
	var gt types.CoreTime
	if g.Fsp > 0 && g.Fsp <= 6 {
		gt = types.FromDate(g.Year, g.Month, g.Day, g.randGen.Intn(12), g.randGen.Intn(60), g.randGen.Intn(60), g.randGen.Intn(1000000))
	} else {
		gt = types.FromDate(g.Year, g.Month, g.Day, g.randGen.Intn(12), g.randGen.Intn(60), g.randGen.Intn(60), 0)
	}
	t := types.NewTime(gt, allegrosql.TypeDatetime, types.DefaultFsp)
	return t
}

// dateTimeStrGener is used to generate strings which are dataTime format
type dateTimeStrGener struct {
	Fsp     int
	Year    int
	Month   int
	Day     int
	randGen *defaultRandGen
}

func (g *dateTimeStrGener) gen() interface{} {
	if g.Year == 0 {
		g.Year = 1970 + g.randGen.Intn(100)
	}
	if g.Month == 0 {
		g.Month = g.randGen.Intn(10) + 1
	}
	if g.Day == 0 {
		g.Day = g.randGen.Intn(20) + 1
	}
	hour := g.randGen.Intn(12)
	minute := g.randGen.Intn(60)
	second := g.randGen.Intn(60)
	dataTimeStr := fmt.Sprintf("%d-%d-%d %d:%d:%d",
		g.Year, g.Month, g.Day, hour, minute, second)
	if g.Fsp > 0 && g.Fsp <= 6 {
		microFmt := fmt.Sprintf(".%%0%dd", g.Fsp)
		return dataTimeStr + fmt.Sprintf(microFmt, g.randGen.Int()%(10^g.Fsp))
	}

	return dataTimeStr
}

// dateStrGener is used to generate strings which are date format
type dateStrGener struct {
	Year       int
	Month      int
	Day        int
	NullRation float64
	randGen    *defaultRandGen
}

func (g *dateStrGener) gen() interface{} {
	if g.NullRation > 1e-6 && g.randGen.Float64() < g.NullRation {
		return nil
	}

	if g.Year == 0 {
		g.Year = 1970 + g.randGen.Intn(100)
	}
	if g.Month == 0 {
		g.Month = g.randGen.Intn(10) + 1
	}
	if g.Day == 0 {
		g.Day = g.randGen.Intn(20) + 1
	}

	return fmt.Sprintf("%d-%d-%d", g.Year, g.Month, g.Day)
}

// timeStrGener is used to generate strings which are time format
type timeStrGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func (g *timeStrGener) gen() interface{} {
	if g.nullRation > 1e-6 && g.randGen.Float64() < g.nullRation {
		return nil
	}
	hour := g.randGen.Intn(12)
	minute := g.randGen.Intn(60)
	second := g.randGen.Intn(60)

	return fmt.Sprintf("%d:%d:%d", hour, minute, second)
}

type dateTimeIntGener struct {
	dateTimeGener
	nullRation float64
}

func (g *dateTimeIntGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}

	t := g.dateTimeGener.gen().(types.Time)
	num, err := t.ToNumber().ToInt()
	if err != nil {
		panic(err)
	}
	return num
}

// constStrGener always returns the given string
type constStrGener struct {
	s string
}

func (g *constStrGener) gen() interface{} {
	return g.s
}

type randDurInt struct {
	randGen *defaultRandGen
}

func newRandDurInt() *randDurInt {
	return &randDurInt{newDefaultRandGen()}
}

func (g *randDurInt) gen() interface{} {
	return int64(g.randGen.Intn(types.TimeMaxHour)*10000 + g.randGen.Intn(60)*100 + g.randGen.Intn(60))
}

type randDurReal struct {
	randGen *defaultRandGen
}

func newRandDurReal() *randDurReal {
	return &randDurReal{newDefaultRandGen()}
}

func (g *randDurReal) gen() interface{} {
	return float64(g.randGen.Intn(types.TimeMaxHour)*10000 + g.randGen.Intn(60)*100 + g.randGen.Intn(60))
}

type randDurDecimal struct {
	randGen *defaultRandGen
}

func newRandDurDecimal() *randDurDecimal {
	return &randDurDecimal{newDefaultRandGen()}
}

func (g *randDurDecimal) gen() interface{} {
	d := new(types.MyDecimal)
	return d.FromFloat64(float64(g.randGen.Intn(types.TimeMaxHour)*10000 + g.randGen.Intn(60)*100 + g.randGen.Intn(60)))
}

type randDurString struct{}

func (g *randDurString) gen() interface{} {
	return strconv.Itoa(rand.Intn(types.TimeMaxHour)*10000 + rand.Intn(60)*100 + rand.Intn(60))
}

// locationGener is used to generate location for the built-in function GetFormat.
type locationGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func newLocationGener(nullRation float64) *locationGener {
	return &locationGener{nullRation, newDefaultRandGen()}
}

func (g *locationGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	switch g.randGen.Uint32() % 5 {
	case 0:
		return usaLocation
	case 1:
		return jisLocation
	case 2:
		return isoLocation
	case 3:
		return eurLocation
	case 4:
		return internalLocation
	default:
		return nil
	}
}

// formatGener is used to generate a format for the built-in function GetFormat.
type formatGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func newFormatGener(nullRation float64) *formatGener {
	return &formatGener{nullRation, newDefaultRandGen()}
}

func (g *formatGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	switch g.randGen.Uint32() % 4 {
	case 0:
		return dateFormat
	case 1:
		return datetimeFormat
	case 2:
		return timestampFormat
	case 3:
		return timeFormat
	default:
		return nil
	}
}

type nullWrappedGener struct {
	nullRation float64
	inner      dataGenerator
	randGen    *defaultRandGen
}

func newNullWrappedGener(nullRation float64, inner dataGenerator) *nullWrappedGener {
	return &nullWrappedGener{nullRation, inner, newDefaultRandGen()}
}

func (g *nullWrappedGener) gen() interface{} {
	if g.randGen.Float64() < g.nullRation {
		return nil
	}
	return g.inner.gen()
}

type vecExprBenchCase struct {
	// retEvalType is the EvalType of the memex result.
	// This field is required.
	retEvalType types.EvalType
	// childrenTypes is the EvalTypes of the memex children(arguments).
	// This field is required.
	childrenTypes []types.EvalType
	// childrenFieldTypes is the field types of the memex children(arguments).
	// If childrenFieldTypes is not set, it will be converted from childrenTypes.
	// This field is optional.
	childrenFieldTypes []*types.FieldType
	// geners are used to generate data for children and geners[i] generates data for children[i].
	// If geners[i] is nil, the default dataGenerator will be used for its corresponding child.
	// The geners slice can be shorter than the children slice, if it has 3 children, then
	// geners[gen1, gen2] will be regarded as geners[gen1, gen2, nil].
	// This field is optional.
	geners []dataGenerator
	// aesModeAttr information, needed by encryption functions
	aesModes string
	// constants are used to generate constant data for children[i].
	constants []*Constant
	// chunkSize is used to specify the chunk size of children, the maximum is 1024.
	// This field is optional, 1024 by default.
	chunkSize int
}

type vecExprBenchCases map[string][]vecExprBenchCase

func fillDeferredCauset(eType types.EvalType, chk *chunk.Chunk, defCausIdx int, testCase vecExprBenchCase) {
	var gen dataGenerator
	if len(testCase.geners) > defCausIdx && testCase.geners[defCausIdx] != nil {
		gen = testCase.geners[defCausIdx]
	}
	fillDeferredCausetWithGener(eType, chk, defCausIdx, gen)
}

func fillDeferredCausetWithGener(eType types.EvalType, chk *chunk.Chunk, defCausIdx int, gen dataGenerator) {
	batchSize := chk.Capacity()
	if gen == nil {
		gen = newDefaultGener(0.2, eType)
	}

	defCaus := chk.DeferredCauset(defCausIdx)
	defCaus.Reset(eType)
	for i := 0; i < batchSize; i++ {
		v := gen.gen()
		if v == nil {
			defCaus.AppendNull()
			continue
		}
		switch eType {
		case types.ETInt:
			defCaus.AppendInt64(v.(int64))
		case types.ETReal:
			defCaus.AppendFloat64(v.(float64))
		case types.ETDecimal:
			defCaus.AppendMyDecimal(v.(*types.MyDecimal))
		case types.ETDatetime, types.ETTimestamp:
			defCaus.AppendTime(v.(types.Time))
		case types.ETDuration:
			defCaus.AppendDuration(v.(types.Duration))
		case types.ETJson:
			defCaus.AppendJSON(v.(json.BinaryJSON))
		case types.ETString:
			defCaus.AppendString(v.(string))
		}
	}
}

func randString(r *rand.Rand) string {
	n := 10 + r.Intn(10)
	buf := make([]byte, n)
	for i := range buf {
		x := r.Intn(62)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else {
			buf[i] = byte('A' + x - 10 - 26)
		}
	}
	return string(buf)
}

func eType2FieldType(eType types.EvalType) *types.FieldType {
	switch eType {
	case types.ETInt:
		return types.NewFieldType(allegrosql.TypeLonglong)
	case types.ETReal:
		return types.NewFieldType(allegrosql.TypeDouble)
	case types.ETDecimal:
		return types.NewFieldType(allegrosql.TypeNewDecimal)
	case types.ETDatetime, types.ETTimestamp:
		return types.NewFieldType(allegrosql.TypeDatetime)
	case types.ETDuration:
		return types.NewFieldType(allegrosql.TypeDuration)
	case types.ETJson:
		return types.NewFieldType(allegrosql.TypeJSON)
	case types.ETString:
		return types.NewFieldType(allegrosql.TypeVarString)
	default:
		panic(fmt.Sprintf("EvalType=%v is not supported.", eType))
	}
}

func genVecExprBenchCase(ctx stochastikctx.Context, funcName string, testCase vecExprBenchCase) (expr Expression, fts []*types.FieldType, input *chunk.Chunk, output *chunk.Chunk) {
	fts = make([]*types.FieldType, len(testCase.childrenTypes))
	for i := range fts {
		if i < len(testCase.childrenFieldTypes) && testCase.childrenFieldTypes[i] != nil {
			fts[i] = testCase.childrenFieldTypes[i]
		} else {
			fts[i] = eType2FieldType(testCase.childrenTypes[i])
		}
	}
	if testCase.chunkSize <= 0 || testCase.chunkSize > 1024 {
		testCase.chunkSize = 1024
	}
	defcaus := make([]Expression, len(testCase.childrenTypes))
	input = chunk.New(fts, testCase.chunkSize, testCase.chunkSize)
	input.NumEvents()
	for i, eType := range testCase.childrenTypes {
		fillDeferredCauset(eType, input, i, testCase)
		if i < len(testCase.constants) && testCase.constants[i] != nil {
			defcaus[i] = testCase.constants[i]
		} else {
			defcaus[i] = &DeferredCauset{Index: i, RetType: fts[i]}
		}
	}

	expr, err := NewFunction(ctx, funcName, eType2FieldType(testCase.retEvalType), defcaus...)
	if err != nil {
		panic(err)
	}

	output = chunk.New([]*types.FieldType{eType2FieldType(expr.GetType().EvalType())}, testCase.chunkSize, testCase.chunkSize)
	return expr, fts, input, output
}

// testVectorizedEvalOneVec is used to verify that the vectorized
// memex is evaluated correctly during projection
func testVectorizedEvalOneVec(c *C, vecExprCases vecExprBenchCases) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			expr, fts, input, output := genVecExprBenchCase(ctx, funcName, testCase)
			commentf := func(event int) CommentInterface {
				return Commentf("func: %v, case %+v, event: %v, rowData: %v", funcName, testCase, event, input.GetEvent(event).GetCausetEvent(fts))
			}
			output2 := output.CopyConstruct()
			c.Assert(evalOneVec(ctx, expr, input, output, 0), IsNil, Commentf("func: %v, case: %+v", funcName, testCase))
			it := chunk.NewIterator4Chunk(input)
			c.Assert(evalOneDeferredCauset(ctx, expr, it, output2, 0), IsNil, Commentf("func: %v, case: %+v", funcName, testCase))

			c1, c2 := output.DeferredCauset(0), output2.DeferredCauset(0)
			switch expr.GetType().EvalType() {
			case types.ETInt:
				for i := 0; i < input.NumEvents(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetInt64(i), Equals, c2.GetInt64(i), commentf(i))
					}
				}
			case types.ETReal:
				for i := 0; i < input.NumEvents(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetFloat64(i), Equals, c2.GetFloat64(i), commentf(i))
					}
				}
			case types.ETDecimal:
				for i := 0; i < input.NumEvents(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetDecimal(i), DeepEquals, c2.GetDecimal(i), commentf(i))
					}
				}
			case types.ETDatetime, types.ETTimestamp:
				for i := 0; i < input.NumEvents(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetTime(i), DeepEquals, c2.GetTime(i), commentf(i))
					}
				}
			case types.ETDuration:
				for i := 0; i < input.NumEvents(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetDuration(i, 0), Equals, c2.GetDuration(i, 0), commentf(i))
					}
				}
			case types.ETJson:
				for i := 0; i < input.NumEvents(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetJSON(i), DeepEquals, c2.GetJSON(i), commentf(i))
					}
				}
			case types.ETString:
				for i := 0; i < input.NumEvents(); i++ {
					c.Assert(c1.IsNull(i), Equals, c2.IsNull(i), commentf(i))
					if !c1.IsNull(i) {
						c.Assert(c1.GetString(i), Equals, c2.GetString(i), commentf(i))
					}
				}
			}
		}
	}
}

// benchmarkVectorizedEvalOneVec is used to get the effect of
// using the vectorized memex evaluations during projection
func benchmarkVectorizedEvalOneVec(b *testing.B, vecExprCases vecExprBenchCases) {
	ctx := mock.NewContext()
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			expr, _, input, output := genVecExprBenchCase(ctx, funcName, testCase)
			exprName := expr.String()
			if sf, ok := expr.(*ScalarFunction); ok {
				exprName = fmt.Sprintf("%v", reflect.TypeOf(sf.Function))
				tmp := strings.Split(exprName, ".")
				exprName = tmp[len(tmp)-1]
			}

			b.Run(exprName+"-EvalOneVec", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := evalOneVec(ctx, expr, input, output, 0); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(exprName+"-EvalOneDefCaus", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					it := chunk.NewIterator4Chunk(input)
					if err := evalOneDeferredCauset(ctx, expr, it, output, 0); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func genVecBuiltinFuncBenchCase(ctx stochastikctx.Context, funcName string, testCase vecExprBenchCase) (baseFunc builtinFunc, fts []*types.FieldType, input *chunk.Chunk, result *chunk.DeferredCauset) {
	childrenNumber := len(testCase.childrenTypes)
	fts = make([]*types.FieldType, childrenNumber)
	for i := range fts {
		if i < len(testCase.childrenFieldTypes) && testCase.childrenFieldTypes[i] != nil {
			fts[i] = testCase.childrenFieldTypes[i]
		} else {
			fts[i] = eType2FieldType(testCase.childrenTypes[i])
		}
	}
	defcaus := make([]Expression, childrenNumber)
	if testCase.chunkSize <= 0 || testCase.chunkSize > 1024 {
		testCase.chunkSize = 1024
	}
	input = chunk.New(fts, testCase.chunkSize, testCase.chunkSize)
	for i, eType := range testCase.childrenTypes {
		fillDeferredCauset(eType, input, i, testCase)
		if i < len(testCase.constants) && testCase.constants[i] != nil {
			defcaus[i] = testCase.constants[i]
		} else {
			defcaus[i] = &DeferredCauset{Index: i, RetType: fts[i]}
		}
	}
	if len(defcaus) == 0 {
		input.SetNumVirtualEvents(testCase.chunkSize)
	}

	var err error
	if funcName == ast.Cast {
		var fc functionClass
		tp := eType2FieldType(testCase.retEvalType)
		switch testCase.retEvalType {
		case types.ETInt:
			fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETDecimal:
			fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETReal:
			fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETDatetime, types.ETTimestamp:
			fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETDuration:
			fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETJson:
			fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		case types.ETString:
			fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		}
		baseFunc, err = fc.getFunction(ctx, defcaus)
	} else {
		baseFunc, err = funcs[funcName].getFunction(ctx, defcaus)
	}
	if err != nil {
		panic(err)
	}
	result = chunk.NewDeferredCauset(eType2FieldType(testCase.retEvalType), testCase.chunkSize)
	// Mess up the output to make sure vecEvalXXX to call ResizeXXX/ReserveXXX itself.
	result.AppendNull()
	return baseFunc, fts, input, result
}

// a replog way to calculate length of a chunk.DeferredCauset.
func getDeferredCausetLen(defCaus *chunk.DeferredCauset, eType types.EvalType) int {
	chk := chunk.New([]*types.FieldType{eType2FieldType(eType)}, 1024, 1024)
	chk.SetDefCaus(0, defCaus)
	return chk.NumEvents()
}

// removeTestOptions removes all not needed options like '-test.timeout=' from argument list
func removeTestOptions(args []string) []string {
	argList := args[:0]

	// args contains '-test.timeout=' option for example
	// excluding it to be able to run all tests
	for _, arg := range args {
		if strings.HasPrefix(arg, "builtin") || IsFunctionSupported(arg) {
			argList = append(argList, arg)
		}
	}
	return argList
}

// testVectorizedBuiltinFunc is used to verify that the vectorized
// memex is evaluated correctly
func testVectorizedBuiltinFunc(c *C, vecExprCases vecExprBenchCases) {
	testFunc := make(map[string]bool)
	argList := removeTestOptions(flag.Args())
	testAll := len(argList) == 0
	for _, arg := range argList {
		testFunc[arg] = true
	}
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			ctx := mock.NewContext()
			err := ctx.GetStochastikVars().SetSystemVar(variable.BlockEncryptionMode, testCase.aesModes)
			c.Assert(err, IsNil)
			if funcName == ast.CurrentUser || funcName == ast.User {
				ctx.GetStochastikVars().User = &auth.UserIdentity{
					Username:     "milevadb",
					Hostname:     "localhost",
					CurrentUser:  true,
					AuthHostname: "localhost",
					AuthUsername: "milevadb",
				}
			}
			if funcName == ast.GetParam {
				testTime := time.Now()
				ctx.GetStochastikVars().PreparedParams = []types.Causet{
					types.NewIntCauset(1),
					types.NewDecimalCauset(types.NewDecFromStringForTest("20170118123950.123")),
					types.NewTimeCauset(types.NewTime(types.FromGoTime(testTime), allegrosql.TypeTimestamp, 6)),
					types.NewDurationCauset(types.ZeroDuration),
					types.NewStringCauset("{}"),
					types.NewBinaryLiteralCauset([]byte{1}),
					types.NewBytesCauset([]byte{'b'}),
					types.NewFloat32Causet(1.1),
					types.NewFloat64Causet(2.1),
					types.NewUintCauset(100),
					types.NewMysqlBitCauset([]byte{1}),
					types.NewMysqlEnumCauset(types.Enum{Name: "n", Value: 2}),
				}
			}
			baseFunc, fts, input, output := genVecBuiltinFuncBenchCase(ctx, funcName, testCase)
			baseFuncName := fmt.Sprintf("%v", reflect.TypeOf(baseFunc))
			tmp := strings.Split(baseFuncName, ".")
			baseFuncName = tmp[len(tmp)-1]

			if !testAll && (testFunc[baseFuncName] != true && testFunc[funcName] != true) {
				continue
			}
			// do not forget to implement the vectorized method.
			c.Assert(baseFunc.vectorized(), IsTrue, Commentf("func: %v, case: %+v", baseFuncName, testCase))
			commentf := func(event int) CommentInterface {
				return Commentf("func: %v, case %+v, event: %v, rowData: %v", baseFuncName, testCase, event, input.GetEvent(event).GetCausetEvent(fts))
			}
			it := chunk.NewIterator4Chunk(input)
			i := 0
			var vecWarnCnt uint16
			switch testCase.retEvalType {
			case types.ETInt:
				err := baseFunc.vecEvalInt(input, output)
				c.Assert(err, IsNil, Commentf("func: %v, case: %+v", baseFuncName, testCase))
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				vecWarnCnt = ctx.GetStochastikVars().StmtCtx.WarningCount()
				i64s := output.Int64s()
				for event := it.Begin(); event != it.End(); event = it.Next() {
					val, isNull, err := baseFunc.evalInt(event)
					c.Assert(err, IsNil, commentf(i))
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, i64s[i], commentf(i))
					}
					i++
				}
			case types.ETReal:
				err := baseFunc.vecEvalReal(input, output)
				c.Assert(err, IsNil, Commentf("func: %v, case: %+v", baseFuncName, testCase))
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				vecWarnCnt = ctx.GetStochastikVars().StmtCtx.WarningCount()
				f64s := output.Float64s()
				for event := it.Begin(); event != it.End(); event = it.Next() {
					val, isNull, err := baseFunc.evalReal(event)
					c.Assert(err, IsNil, commentf(i))
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, f64s[i], commentf(i))
					}
					i++
				}
			case types.ETDecimal:
				err := baseFunc.vecEvalDecimal(input, output)
				c.Assert(err, IsNil, Commentf("func: %v, case: %+v", baseFuncName, testCase))
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				vecWarnCnt = ctx.GetStochastikVars().StmtCtx.WarningCount()
				d64s := output.Decimals()
				for event := it.Begin(); event != it.End(); event = it.Next() {
					val, isNull, err := baseFunc.evalDecimal(event)
					c.Assert(err, IsNil, commentf(i))
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(*val, Equals, d64s[i], commentf(i))
					}
					i++
				}
			case types.ETDatetime, types.ETTimestamp:
				err := baseFunc.vecEvalTime(input, output)
				c.Assert(err, IsNil, Commentf("func: %v, case: %+v", baseFuncName, testCase))
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				vecWarnCnt = ctx.GetStochastikVars().StmtCtx.WarningCount()
				t64s := output.Times()
				for event := it.Begin(); event != it.End(); event = it.Next() {
					val, isNull, err := baseFunc.evalTime(event)
					c.Assert(err, IsNil, commentf(i))
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, t64s[i], commentf(i))
					}
					i++
				}
			case types.ETDuration:
				err := baseFunc.vecEvalDuration(input, output)
				c.Assert(err, IsNil, Commentf("func: %v, case: %+v", baseFuncName, testCase))
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				vecWarnCnt = ctx.GetStochastikVars().StmtCtx.WarningCount()
				d64s := output.GoDurations()
				for event := it.Begin(); event != it.End(); event = it.Next() {
					val, isNull, err := baseFunc.evalDuration(event)
					c.Assert(err, IsNil, commentf(i))
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val.Duration, Equals, d64s[i], commentf(i))
					}
					i++
				}
			case types.ETJson:
				err := baseFunc.vecEvalJSON(input, output)
				c.Assert(err, IsNil, Commentf("func: %v, case: %+v", baseFuncName, testCase))
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				vecWarnCnt = ctx.GetStochastikVars().StmtCtx.WarningCount()
				for event := it.Begin(); event != it.End(); event = it.Next() {
					val, isNull, err := baseFunc.evalJSON(event)
					c.Assert(err, IsNil, commentf(i))
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						cmp := json.CompareBinary(val, output.GetJSON(i))
						c.Assert(cmp, Equals, 0, commentf(i))
					}
					i++
				}
			case types.ETString:
				err := baseFunc.vecEvalString(input, output)
				c.Assert(err, IsNil, Commentf("func: %v, case: %+v", baseFuncName, testCase))
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				vecWarnCnt = ctx.GetStochastikVars().StmtCtx.WarningCount()
				for event := it.Begin(); event != it.End(); event = it.Next() {
					val, isNull, err := baseFunc.evalString(event)
					c.Assert(err, IsNil, commentf(i))
					c.Assert(isNull, Equals, output.IsNull(i), commentf(i))
					if !isNull {
						c.Assert(val, Equals, output.GetString(i), commentf(i))
					}
					i++
				}
			default:
				c.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
			}

			// check warnings
			totalWarns := ctx.GetStochastikVars().StmtCtx.WarningCount()
			c.Assert(2*vecWarnCnt, Equals, totalWarns)
			warns := ctx.GetStochastikVars().StmtCtx.GetWarnings()
			for i := 0; i < int(vecWarnCnt); i++ {
				c.Assert(terror.ErrorEqual(warns[i].Err, warns[i+int(vecWarnCnt)].Err), IsTrue)
			}
		}
	}
}

// testVectorizedBuiltinFuncForRand is used to verify that the vectorized
// memex is evaluated correctly
func testVectorizedBuiltinFuncForRand(c *C, vecExprCases vecExprBenchCases) {
	for funcName, testCases := range vecExprCases {
		c.Assert(strings.EqualFold("rand", funcName), Equals, true)

		for _, testCase := range testCases {
			c.Assert(len(testCase.childrenTypes), Equals, 0)

			ctx := mock.NewContext()
			baseFunc, _, input, output := genVecBuiltinFuncBenchCase(ctx, funcName, testCase)
			baseFuncName := fmt.Sprintf("%v", reflect.TypeOf(baseFunc))
			tmp := strings.Split(baseFuncName, ".")
			baseFuncName = tmp[len(tmp)-1]
			// do not forget to implement the vectorized method.
			c.Assert(baseFunc.vectorized(), IsTrue, Commentf("func: %v", baseFuncName))
			switch testCase.retEvalType {
			case types.ETReal:
				err := baseFunc.vecEvalReal(input, output)
				c.Assert(err, IsNil)
				// do not forget to call ResizeXXX/ReserveXXX
				c.Assert(getDeferredCausetLen(output, testCase.retEvalType), Equals, input.NumEvents())
				// check result
				res := output.Float64s()
				for _, v := range res {
					c.Assert((0 <= v) && (v < 1), Equals, true)
				}
			default:
				c.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
			}
		}
	}
}

// benchmarkVectorizedBuiltinFunc is used to get the effect of
// using the vectorized memex evaluations
func benchmarkVectorizedBuiltinFunc(b *testing.B, vecExprCases vecExprBenchCases) {
	ctx := mock.NewContext()
	testFunc := make(map[string]bool)
	argList := removeTestOptions(flag.Args())
	testAll := len(argList) == 0
	for _, arg := range argList {
		testFunc[arg] = true
	}
	for funcName, testCases := range vecExprCases {
		for _, testCase := range testCases {
			err := ctx.GetStochastikVars().SetSystemVar(variable.BlockEncryptionMode, testCase.aesModes)
			if err != nil {
				panic(err)
			}
			if funcName == ast.CurrentUser || funcName == ast.User {
				ctx.GetStochastikVars().User = &auth.UserIdentity{
					Username:     "milevadb",
					Hostname:     "localhost",
					CurrentUser:  true,
					AuthHostname: "localhost",
					AuthUsername: "milevadb",
				}
			}
			if funcName == ast.GetParam {
				testTime := time.Now()
				ctx.GetStochastikVars().PreparedParams = []types.Causet{
					types.NewIntCauset(1),
					types.NewDecimalCauset(types.NewDecFromStringForTest("20170118123950.123")),
					types.NewTimeCauset(types.NewTime(types.FromGoTime(testTime), allegrosql.TypeTimestamp, 6)),
					types.NewDurationCauset(types.ZeroDuration),
					types.NewStringCauset("{}"),
					types.NewBinaryLiteralCauset([]byte{1}),
					types.NewBytesCauset([]byte{'b'}),
					types.NewFloat32Causet(1.1),
					types.NewFloat64Causet(2.1),
					types.NewUintCauset(100),
					types.NewMysqlBitCauset([]byte{1}),
					types.NewMysqlEnumCauset(types.Enum{Name: "n", Value: 2}),
				}
			}
			baseFunc, _, input, output := genVecBuiltinFuncBenchCase(ctx, funcName, testCase)
			baseFuncName := fmt.Sprintf("%v", reflect.TypeOf(baseFunc))
			tmp := strings.Split(baseFuncName, ".")
			baseFuncName = tmp[len(tmp)-1]

			if !testAll && testFunc[baseFuncName] != true && testFunc[funcName] != true {
				continue
			}

			b.Run(baseFuncName+"-VecBuiltinFunc", func(b *testing.B) {
				b.ResetTimer()
				switch testCase.retEvalType {
				case types.ETInt:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalInt(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETReal:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalReal(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETDecimal:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalDecimal(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETDatetime, types.ETTimestamp:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalTime(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETDuration:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalDuration(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETJson:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalJSON(input, output); err != nil {
							b.Fatal(err)
						}
					}
				case types.ETString:
					for i := 0; i < b.N; i++ {
						if err := baseFunc.vecEvalString(input, output); err != nil {
							b.Fatal(err)
						}
					}
				default:
					b.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
				}
			})
			b.Run(baseFuncName+"-NonVecBuiltinFunc", func(b *testing.B) {
				b.ResetTimer()
				it := chunk.NewIterator4Chunk(input)
				switch testCase.retEvalType {
				case types.ETInt:
					for i := 0; i < b.N; i++ {
						output.Reset(testCase.retEvalType)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							v, isNull, err := baseFunc.evalInt(event)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendInt64(v)
							}
						}
					}
				case types.ETReal:
					for i := 0; i < b.N; i++ {
						output.Reset(testCase.retEvalType)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							v, isNull, err := baseFunc.evalReal(event)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendFloat64(v)
							}
						}
					}
				case types.ETDecimal:
					for i := 0; i < b.N; i++ {
						output.Reset(testCase.retEvalType)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							v, isNull, err := baseFunc.evalDecimal(event)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendMyDecimal(v)
							}
						}
					}
				case types.ETDatetime, types.ETTimestamp:
					for i := 0; i < b.N; i++ {
						output.Reset(testCase.retEvalType)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							v, isNull, err := baseFunc.evalTime(event)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendTime(v)
							}
						}
					}
				case types.ETDuration:
					for i := 0; i < b.N; i++ {
						output.Reset(testCase.retEvalType)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							v, isNull, err := baseFunc.evalDuration(event)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendDuration(v)
							}
						}
					}
				case types.ETJson:
					for i := 0; i < b.N; i++ {
						output.Reset(testCase.retEvalType)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							v, isNull, err := baseFunc.evalJSON(event)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendJSON(v)
							}
						}
					}
				case types.ETString:
					for i := 0; i < b.N; i++ {
						output.Reset(testCase.retEvalType)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							v, isNull, err := baseFunc.evalString(event)
							if err != nil {
								b.Fatal(err)
							}
							if isNull {
								output.AppendNull()
							} else {
								output.AppendString(v)
							}
						}
					}
				default:
					b.Fatal(fmt.Sprintf("evalType=%v is not supported", testCase.retEvalType))
				}
			})
		}
	}
}

func genVecEvalBool(numDefCauss int, defCausTypes, eTypes []types.EvalType) (CNFExprs, *chunk.Chunk) {
	gens := make([]dataGenerator, 0, len(eTypes))
	for _, eType := range eTypes {
		if eType == types.ETString {
			gens = append(gens, &numStrGener{*newRangeInt64Gener(0, 10)})
		} else {
			gens = append(gens, newDefaultGener(0.05, eType))
		}
	}

	ts := make([]types.EvalType, 0, numDefCauss)
	gs := make([]dataGenerator, 0, numDefCauss)
	fts := make([]*types.FieldType, 0, numDefCauss)
	randGen := newDefaultRandGen()
	for i := 0; i < numDefCauss; i++ {
		idx := randGen.Intn(len(eTypes))
		if defCausTypes != nil {
			for j := range eTypes {
				if defCausTypes[i] == eTypes[j] {
					idx = j
					break
				}
			}
		}
		ts = append(ts, eTypes[idx])
		gs = append(gs, gens[idx])
		fts = append(fts, eType2FieldType(eTypes[idx]))
	}

	input := chunk.New(fts, 1024, 1024)
	exprs := make(CNFExprs, 0, numDefCauss)
	for i := 0; i < numDefCauss; i++ {
		fillDeferredCauset(ts[i], input, i, vecExprBenchCase{geners: gs})
		exprs = append(exprs, &DeferredCauset{Index: i, RetType: fts[i]})
	}
	return exprs, input
}

func generateRandomSel() []int {
	randGen := newDefaultRandGen()
	randGen.Seed(time.Now().UnixNano())
	var sel []int
	count := 0
	// Use constant 256 to make it faster to generate randomly arranged sel slices
	num := randGen.Intn(256) + 1
	existed := make([]bool, 1024)
	for i := 0; i < 1024; i++ {
		existed[i] = false
	}
	for count < num {
		val := randGen.Intn(1024)
		if !existed[val] {
			existed[val] = true
			count++
		}
	}
	for i := 0; i < 1024; i++ {
		if existed[i] {
			sel = append(sel, i)
		}
	}
	return sel
}

func (s *testVectorizeSuite2) TestVecEvalBool(c *C) {
	ctx := mock.NewContext()
	eTypes := []types.EvalType{types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	for numDefCauss := 1; numDefCauss <= 5; numDefCauss++ {
		for round := 0; round < 16; round++ {
			exprs, input := genVecEvalBool(numDefCauss, nil, eTypes)
			selected, nulls, err := VecEvalBool(ctx, exprs, input, nil, nil)
			c.Assert(err, IsNil)
			it := chunk.NewIterator4Chunk(input)
			i := 0
			for event := it.Begin(); event != it.End(); event = it.Next() {
				ok, null, err := EvalBool(mock.NewContext(), exprs, event)
				c.Assert(err, IsNil)
				c.Assert(null, Equals, nulls[i])
				c.Assert(ok, Equals, selected[i])
				i++
			}
		}
	}
}

func BenchmarkVecEvalBool(b *testing.B) {
	ctx := mock.NewContext()
	selected := make([]bool, 0, 1024)
	nulls := make([]bool, 0, 1024)
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for numDefCauss := 1; numDefCauss <= 2; numDefCauss++ {
		typeCombination := make([]types.EvalType, numDefCauss)
		var combFunc func(nDefCauss int)
		combFunc = func(nDefCauss int) {
			if nDefCauss == 0 {
				name := ""
				for _, t := range typeCombination {
					for i := range eTypes {
						if t == eTypes[i] {
							name += tNames[t] + "/"
						}
					}
				}
				exprs, input := genVecEvalBool(numDefCauss, typeCombination, eTypes)
				b.Run("Vec-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _, err := VecEvalBool(ctx, exprs, input, selected, nulls)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("Event-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						it := chunk.NewIterator4Chunk(input)
						for event := it.Begin(); event != it.End(); event = it.Next() {
							_, _, err := EvalBool(ctx, exprs, event)
							if err != nil {
								b.Fatal(err)
							}
						}
					}
				})
				return
			}
			for _, eType := range eTypes {
				typeCombination[nDefCauss-1] = eType
				combFunc(nDefCauss - 1)
			}
		}

		combFunc(numDefCauss)
	}
}

func (s *testVectorizeSuite2) TestEventBasedFilterAndVectorizedFilter(c *C) {
	ctx := mock.NewContext()
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	for numDefCauss := 1; numDefCauss <= 5; numDefCauss++ {
		for round := 0; round < 16; round++ {
			exprs, input := genVecEvalBool(numDefCauss, nil, eTypes)
			it := chunk.NewIterator4Chunk(input)
			isNull := make([]bool, it.Len())
			selected, nulls, err := rowBasedFilter(ctx, exprs, it, nil, isNull)
			c.Assert(err, IsNil)
			selected2, nulls2, err2 := vectorizedFilter(ctx, exprs, it, nil, isNull)
			c.Assert(err2, IsNil)
			length := it.Len()
			for i := 0; i < length; i++ {
				c.Assert(nulls2[i], Equals, nulls[i])
				c.Assert(selected2[i], Equals, selected[i])
			}
		}
	}
}

func BenchmarkEventBasedFilterAndVectorizedFilter(b *testing.B) {
	ctx := mock.NewContext()
	selected := make([]bool, 0, 1024)
	nulls := make([]bool, 0, 1024)
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for numDefCauss := 1; numDefCauss <= 2; numDefCauss++ {
		typeCombination := make([]types.EvalType, numDefCauss)
		var combFunc func(nDefCauss int)
		combFunc = func(nDefCauss int) {
			if nDefCauss == 0 {
				name := ""
				for _, t := range typeCombination {
					for i := range eTypes {
						if t == eTypes[i] {
							name += tNames[t] + "/"
						}
					}
				}
				exprs, input := genVecEvalBool(numDefCauss, typeCombination, eTypes)
				it := chunk.NewIterator4Chunk(input)
				b.Run("Vec-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _, err := vectorizedFilter(ctx, exprs, it, selected, nulls)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("Event-"+name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, _, err := rowBasedFilter(ctx, exprs, it, selected, nulls)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				return
			}
			for _, eType := range eTypes {
				typeCombination[nDefCauss-1] = eType
				combFunc(nDefCauss - 1)
			}
		}
		combFunc(numDefCauss)
	}

	// Add special case to prove when some calculations are added,
	// the vectorizedFilter for int types will be more faster than rowBasedFilter.
	funcName := ast.Least
	testCase := vecExprBenchCase{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}}
	expr, _, input, _ := genVecExprBenchCase(ctx, funcName, testCase)
	it := chunk.NewIterator4Chunk(input)

	b.Run("Vec-special case", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := vectorizedFilter(ctx, []Expression{expr}, it, selected, nulls)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("Event-special case", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := rowBasedFilter(ctx, []Expression{expr}, it, selected, nulls)
			if err != nil {
				panic(err)
			}
		}
	})
}

func (s *testVectorizeSuite2) TestVectorizedFilterConsiderNull(c *C) {
	ctx := mock.NewContext()
	dafaultEnableVectorizedExpressionVar := ctx.GetStochastikVars().EnableVectorizedExpression
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	for numDefCauss := 1; numDefCauss <= 5; numDefCauss++ {
		for round := 0; round < 16; round++ {
			exprs, input := genVecEvalBool(numDefCauss, nil, eTypes)
			it := chunk.NewIterator4Chunk(input)
			isNull := make([]bool, it.Len())
			ctx.GetStochastikVars().EnableVectorizedExpression = false
			selected, nulls, err := VectorizedFilterConsiderNull(ctx, exprs, it, nil, isNull)
			c.Assert(err, IsNil)
			ctx.GetStochastikVars().EnableVectorizedExpression = true
			selected2, nulls2, err2 := VectorizedFilterConsiderNull(ctx, exprs, it, nil, isNull)
			c.Assert(err2, IsNil)
			length := it.Len()
			for i := 0; i < length; i++ {
				c.Assert(nulls2[i], Equals, nulls[i])
				c.Assert(selected2[i], Equals, selected[i])
			}

			// add test which sel is not nil
			randomSel := generateRandomSel()
			input.SetSel(randomSel)
			it2 := chunk.NewIterator4Chunk(input)
			isNull = isNull[:0]
			ctx.GetStochastikVars().EnableVectorizedExpression = false
			selected3, nulls, err := VectorizedFilterConsiderNull(ctx, exprs, it2, nil, isNull)
			c.Assert(err, IsNil)
			ctx.GetStochastikVars().EnableVectorizedExpression = true
			selected4, nulls2, err2 := VectorizedFilterConsiderNull(ctx, exprs, it2, nil, isNull)
			c.Assert(err2, IsNil)
			for i := 0; i < length; i++ {
				c.Assert(nulls2[i], Equals, nulls[i])
				c.Assert(selected4[i], Equals, selected3[i])
			}

			unselected := make([]bool, length)
			// unselected[i] == false means that the i-th event is selected
			for i := 0; i < length; i++ {
				unselected[i] = true
			}
			for _, idx := range randomSel {
				unselected[idx] = false
			}
			for i := range selected2 {
				if selected2[i] && unselected[i] {
					selected2[i] = false
				}
			}
			for i := 0; i < length; i++ {
				c.Assert(selected2[i], Equals, selected4[i])
			}
		}
	}
	ctx.GetStochastikVars().EnableVectorizedExpression = dafaultEnableVectorizedExpressionVar
}
