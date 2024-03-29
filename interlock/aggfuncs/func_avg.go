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

package aggfuncs

import (
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/set"
)

const (
	// DefPartialResult4AvgDecimalSize is the size of partialResult4AvgDecimal
	DefPartialResult4AvgDecimalSize = int64(unsafe.Sizeof(partialResult4AvgDecimal{}))
	// DefPartialResult4AvgDistinctDecimalSize is the size of partialResult4AvgDistinctDecimal
	DefPartialResult4AvgDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4AvgDistinctDecimal{}))
	// DefPartialResult4AvgFloat64Size is the size of partialResult4AvgFloat64
	DefPartialResult4AvgFloat64Size = int64(unsafe.Sizeof(partialResult4AvgFloat64{}))
	// DefPartialResult4AvgDistinctFloat64Size is the size of partialResult4AvgDistinctFloat64
	DefPartialResult4AvgDistinctFloat64Size = int64(unsafe.Sizeof(partialResult4AvgDistinctFloat64{}))
)

// All the following avg function implementations return the decimal result,
// which causetstore the partial results in "partialResult4AvgDecimal".
//
// "baseAvgDecimal" is wrapped by:
// - "avgOriginal4Decimal"
// - "avgPartial4Decimal"
type baseAvgDecimal struct {
	baseAggFunc
}

type partialResult4AvgDecimal struct {
	sum   types.MyDecimal
	count int64
}

func (e *baseAvgDecimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(&partialResult4AvgDecimal{}), DefPartialResult4AvgDecimalSize
}

func (e *baseAvgDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDecimal)(pr)
	p.sum = *types.NewDecFromInt(0)
	p.count = int64(0)
}

func (e *baseAvgDecimal) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDecimal)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	decimalCount := types.NewDecFromInt(p.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		return err
	}
	// Make the decimal be the result of type inferring.
	frac := e.args[0].GetType().Decimal
	if len(e.args) == 2 {
		frac = e.args[1].GetType().Decimal
	}
	if frac == -1 {
		frac = allegrosql.MaxDecimalScale
	}
	err = finalResult.Round(finalResult, mathutil.Min(frac, allegrosql.MaxDecimalScale), types.ModeHalfEven)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

type avgOriginal4Decimal struct {
	baseAvgDecimal
}

func (e *avgOriginal4Decimal) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDecimal)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return 0, err
		}
		p.sum = *newSum
		p.count++
	}
	return 0, nil
}

func (e *avgOriginal4Decimal) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4AvgDecimal)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return err
		}
		p.sum = *newSum
		p.count++
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		newSum := new(types.MyDecimal)
		err = types.DecimalSub(&p.sum, input, newSum)
		if err != nil {
			return err
		}
		p.sum = *newSum
		p.count--
	}
	return nil
}

type avgPartial4Decimal struct {
	baseAvgDecimal
}

func (e *avgPartial4Decimal) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDecimal)(pr)
	for _, event := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalDecimal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, inputSum, newSum)
		if err != nil {
			return 0, err
		}
		p.sum = *newSum
		p.count += inputCount
	}
	return 0, nil
}

func (e *avgPartial4Decimal) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4AvgDecimal)(src), (*partialResult4AvgDecimal)(dst)
	if p1.count == 0 {
		return 0, nil
	}
	newSum := new(types.MyDecimal)
	err = types.DecimalAdd(&p1.sum, &p2.sum, newSum)
	if err != nil {
		return 0, err
	}
	p2.sum = *newSum
	p2.count += p1.count
	return 0, nil
}

type partialResult4AvgDistinctDecimal struct {
	partialResult4AvgDecimal
	valSet set.StringSet
}

type avgOriginal4DistinctDecimal struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctDecimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := &partialResult4AvgDistinctDecimal{
		valSet: set.NewStringSet(),
	}
	return PartialResult(p), DefPartialResult4AvgDistinctDecimalSize
}

func (e *avgOriginal4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDistinctDecimal)(pr)
	p.sum = *types.NewDecFromInt(0)
	p.count = int64(0)
	p.valSet = set.NewStringSet()
}

func (e *avgOriginal4DistinctDecimal) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDistinctDecimal)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, event)
		if err != nil {
			return memDelta, err
		}
		if isNull {
			continue
		}
		hash, err := input.ToHashKey()
		if err != nil {
			return memDelta, err
		}
		decStr := string(replog.String(hash))
		if p.valSet.Exist(decStr) {
			continue
		}
		p.valSet.Insert(decStr)
		memDelta += int64(len(decStr))
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.sum, input, newSum)
		if err != nil {
			return memDelta, err
		}
		p.sum = *newSum
		p.count++
	}
	return memDelta, nil
}

func (e *avgOriginal4DistinctDecimal) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDistinctDecimal)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	decimalCount := types.NewDecFromInt(p.count)
	finalResult := new(types.MyDecimal)
	err := types.DecimalDiv(&p.sum, decimalCount, finalResult, types.DivFracIncr)
	if err != nil {
		return err
	}
	// Make the decimal be the result of type inferring.
	frac := e.args[0].GetType().Decimal
	if frac == -1 {
		frac = allegrosql.MaxDecimalScale
	}
	err = finalResult.Round(finalResult, mathutil.Min(frac, allegrosql.MaxDecimalScale), types.ModeHalfEven)
	if err != nil {
		return err
	}
	chk.AppendMyDecimal(e.ordinal, finalResult)
	return nil
}

// All the following avg function implementations return the float64 result,
// which causetstore the partial results in "partialResult4AvgFloat64".
//
// "baseAvgFloat64" is wrapped by:
// - "avgOriginal4Float64"
// - "avgPartial4Float64"
type baseAvgFloat64 struct {
	baseAggFunc
}

type partialResult4AvgFloat64 struct {
	sum   float64
	count int64
}

func (e *baseAvgFloat64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return (PartialResult)(&partialResult4AvgFloat64{}), DefPartialResult4AvgFloat64Size
}

func (e *baseAvgFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgFloat64)(pr)
	p.sum = 0
	p.count = 0
}

func (e *baseAvgFloat64) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
	} else {
		chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
	}
	return nil
}

type avgOriginal4Float64HighPrecision struct {
	baseAvgFloat64
}

func (e *avgOriginal4Float64HighPrecision) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgFloat64)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		p.sum += input
		p.count++
	}
	return 0, nil
}

type avgOriginal4Float64 struct {
	avgOriginal4Float64HighPrecision
}

func (e *avgOriginal4Float64) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4AvgFloat64)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.sum += input
		p.count++
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.sum -= input
		p.count--
	}
	return nil
}

type avgPartial4Float64 struct {
	baseAvgFloat64
}

func (e *avgPartial4Float64) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgFloat64)(pr)
	for _, event := range rowsInGroup {
		inputSum, isNull, err := e.args[1].EvalReal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		inputCount, isNull, err := e.args[0].EvalInt(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.sum += inputSum
		p.count += inputCount
	}
	return 0, nil
}

func (e *avgPartial4Float64) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4AvgFloat64)(src), (*partialResult4AvgFloat64)(dst)
	p2.sum += p1.sum
	p2.count += p1.count
	return 0, nil
}

type partialResult4AvgDistinctFloat64 struct {
	partialResult4AvgFloat64
	valSet set.Float64Set
}

type avgOriginal4DistinctFloat64 struct {
	baseAggFunc
}

func (e *avgOriginal4DistinctFloat64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := &partialResult4AvgDistinctFloat64{
		valSet: set.NewFloat64Set(),
	}
	return PartialResult(p), DefPartialResult4AvgDistinctFloat64Size
}

func (e *avgOriginal4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	p.sum = float64(0)
	p.count = int64(0)
	p.valSet = set.NewFloat64Set()
}

func (e *avgOriginal4DistinctFloat64) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, event)
		if err != nil {
			return memDelta, err
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}

		p.sum += input
		p.count++
		p.valSet.Insert(input)
		memDelta += DefFloat64Size
	}
	return memDelta, nil
}

func (e *avgOriginal4DistinctFloat64) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4AvgDistinctFloat64)(pr)
	if p.count == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.sum/float64(p.count))
	return nil
}
