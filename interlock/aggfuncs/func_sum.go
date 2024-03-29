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

	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/set"
)

const (
	// DefPartialResult4SumFloat64Size is the size of partialResult4SumFloat64
	DefPartialResult4SumFloat64Size = int64(unsafe.Sizeof(partialResult4SumFloat64{}))
	// DefPartialResult4SumDecimalSize is the size of partialResult4SumDecimal
	DefPartialResult4SumDecimalSize = int64(unsafe.Sizeof(partialResult4SumDecimal{}))
	// DefPartialResult4SumDistinctFloat64Size is the size of partialResult4SumDistinctFloat64
	DefPartialResult4SumDistinctFloat64Size = int64(unsafe.Sizeof(partialResult4SumDistinctFloat64{}))
	// DefPartialResult4SumDistinctDecimalSize is the size of partialResult4SumDistinctDecimal
	DefPartialResult4SumDistinctDecimalSize = int64(unsafe.Sizeof(partialResult4SumDistinctDecimal{}))
)

type partialResult4SumFloat64 struct {
	val             float64
	notNullEventCount int64
}

type partialResult4SumDecimal struct {
	val             types.MyDecimal
	notNullEventCount int64
}

type partialResult4SumDistinctFloat64 struct {
	val    float64
	isNull bool
	valSet set.Float64Set
}

type partialResult4SumDistinctDecimal struct {
	val    types.MyDecimal
	isNull bool
	valSet set.StringSet
}

type baseSumAggFunc struct {
	baseAggFunc
}

type baseSum4Float64 struct {
	baseSumAggFunc
}

func (e *baseSum4Float64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumFloat64)
	return PartialResult(p), DefPartialResult4SumFloat64Size
}

func (e *baseSum4Float64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumFloat64)(pr)
	p.val = 0
	p.notNullEventCount = 0
}

func (e *baseSum4Float64) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumFloat64)(pr)
	if p.notNullEventCount == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

func (e *baseSum4Float64) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumFloat64)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		p.val += input
		p.notNullEventCount++
	}
	return 0, nil
}

func (e *baseSum4Float64) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4SumFloat64)(src), (*partialResult4SumFloat64)(dst)
	if p1.notNullEventCount == 0 {
		return 0, nil
	}
	p2.val += p1.val
	p2.notNullEventCount += p1.notNullEventCount
	return 0, nil
}

type sum4Float64 struct {
	baseSum4Float64
}

func (e *sum4Float64) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4SumFloat64)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.val += input
		p.notNullEventCount++
	}
	for i := uint64(0); i < shiftStart; i++ {
		input, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		p.val -= input
		p.notNullEventCount--
	}
	return nil
}

type sum4Float64HighPrecision struct {
	baseSum4Float64
}

type sum4Decimal struct {
	baseSumAggFunc
}

func (e *sum4Decimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumDecimal)
	return PartialResult(p), DefPartialResult4SumDecimalSize
}

func (e *sum4Decimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDecimal)(pr)
	p.notNullEventCount = 0
}

func (e *sum4Decimal) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDecimal)(pr)
	if p.notNullEventCount == 0 {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}

func (e *sum4Decimal) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDecimal)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalDecimal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}
		if p.notNullEventCount == 0 {
			p.val = *input
			p.notNullEventCount = 1
			continue
		}

		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.val, input, newSum)
		if err != nil {
			return 0, err
		}
		p.val = *newSum
		p.notNullEventCount++
	}
	return 0, nil
}

func (e *sum4Decimal) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4SumDecimal)(pr)
	for i := uint64(0); i < shiftEnd; i++ {
		input, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		if p.notNullEventCount == 0 {
			p.val = *input
			p.notNullEventCount = 1
			continue
		}
		newSum := new(types.MyDecimal)
		err = types.DecimalAdd(&p.val, input, newSum)
		if err != nil {
			return err
		}
		p.val = *newSum
		p.notNullEventCount++
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
		err = types.DecimalSub(&p.val, input, newSum)
		if err != nil {
			return err
		}
		p.val = *newSum
		p.notNullEventCount--
	}
	return nil
}

func (e *sum4Decimal) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4SumDecimal)(src), (*partialResult4SumDecimal)(dst)
	if p1.notNullEventCount == 0 {
		return 0, nil
	}
	newSum := new(types.MyDecimal)
	err = types.DecimalAdd(&p1.val, &p2.val, newSum)
	if err != nil {
		return 0, err
	}
	p2.val = *newSum
	p2.notNullEventCount += p1.notNullEventCount
	return 0, nil
}

type sum4DistinctFloat64 struct {
	baseSumAggFunc
}

func (e *sum4DistinctFloat64) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumDistinctFloat64)
	p.isNull = true
	p.valSet = set.NewFloat64Set()
	return PartialResult(p), DefPartialResult4SumDistinctFloat64Size
}

func (e *sum4DistinctFloat64) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctFloat64)(pr)
	p.isNull = true
	p.valSet = set.NewFloat64Set()
}

func (e *sum4DistinctFloat64) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDistinctFloat64)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalReal(sctx, event)
		if err != nil {
			return memDelta, err
		}
		if isNull || p.valSet.Exist(input) {
			continue
		}
		p.valSet.Insert(input)
		memDelta += DefFloat64Size
		if p.isNull {
			p.val = input
			p.isNull = false
			continue
		}
		p.val += input
	}
	return memDelta, nil
}

func (e *sum4DistinctFloat64) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctFloat64)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendFloat64(e.ordinal, p.val)
	return nil
}

type sum4DistinctDecimal struct {
	baseSumAggFunc
}

func (e *sum4DistinctDecimal) AllocPartialResult() (pr PartialResult, memDelta int64) {
	p := new(partialResult4SumDistinctDecimal)
	p.isNull = true
	p.valSet = set.NewStringSet()
	return PartialResult(p), DefPartialResult4SumDistinctDecimalSize
}

func (e *sum4DistinctDecimal) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4SumDistinctDecimal)(pr)
	p.isNull = true
	p.valSet = set.NewStringSet()
}

func (e *sum4DistinctDecimal) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4SumDistinctDecimal)(pr)
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
		if p.isNull {
			p.val = *input
			p.isNull = false
			continue
		}
		newSum := new(types.MyDecimal)
		if err = types.DecimalAdd(&p.val, input, newSum); err != nil {
			return memDelta, err
		}
		p.val = *newSum
	}
	return memDelta, nil
}

func (e *sum4DistinctDecimal) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4SumDistinctDecimal)(pr)
	if p.isNull {
		chk.AppendNull(e.ordinal)
		return nil
	}
	chk.AppendMyDecimal(e.ordinal, &p.val)
	return nil
}
