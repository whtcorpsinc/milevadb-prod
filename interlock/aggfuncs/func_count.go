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
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

const (
	// DefPartialResult4CountSize is the size of partialResult4Count
	DefPartialResult4CountSize = int64(unsafe.Sizeof(partialResult4Count(0)))
)

type baseCount struct {
	baseAggFunc
}

type partialResult4Count = int64

func (e *baseCount) AllocPartialResult() (pr PartialResult, memDelta int64) {
	return PartialResult(new(partialResult4Count)), DefPartialResult4CountSize
}

func (e *baseCount) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Count)(pr)
	*p = 0
}

func (e *baseCount) AppendFinalResult2Chunk(sctx stochastikctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Count)(pr)
	chk.AppendInt64(e.ordinal, *p)
	return nil
}

type countOriginal4Int struct {
	baseCount
}

func (e *countOriginal4Int) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, event := range rowsInGroup {
		_, isNull, err := e.args[0].EvalInt(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

func (e *countOriginal4Int) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalInt(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalInt(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Real struct {
	baseCount
}

func (e *countOriginal4Real) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, event := range rowsInGroup {
		_, isNull, err := e.args[0].EvalReal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

func (e *countOriginal4Real) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalReal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalReal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Decimal struct {
	baseCount
}

func (e *countOriginal4Decimal) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, event := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDecimal(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

func (e *countOriginal4Decimal) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalDecimal(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Time struct {
	baseCount
}

func (e *countOriginal4Time) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, event := range rowsInGroup {
		_, isNull, err := e.args[0].EvalTime(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

func (e *countOriginal4Time) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalTime(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalTime(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4Duration struct {
	baseCount
}

func (e *countOriginal4Duration) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, event := range rowsInGroup {
		_, isNull, err := e.args[0].EvalDuration(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

func (e *countOriginal4Duration) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalDuration(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalDuration(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4JSON struct {
	baseCount
}

func (e *countOriginal4JSON) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, event := range rowsInGroup {
		_, isNull, err := e.args[0].EvalJSON(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

func (e *countOriginal4JSON) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalJSON(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalJSON(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countOriginal4String struct {
	baseCount
}

func (e *countOriginal4String) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)

	for _, event := range rowsInGroup {
		_, isNull, err := e.args[0].EvalString(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p++
	}

	return 0, nil
}

func (e *countOriginal4String) Slide(sctx stochastikctx.Context, rows []chunk.Event, lastStart, lastEnd uint64, shiftStart, shiftEnd uint64, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for i := uint64(0); i < shiftStart; i++ {
		_, isNull, err := e.args[0].EvalString(sctx, rows[lastStart+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p--
	}
	for i := uint64(0); i < shiftEnd; i++ {
		_, isNull, err := e.args[0].EvalString(sctx, rows[lastEnd+i])
		if err != nil {
			return err
		}
		if isNull {
			continue
		}
		*p++
	}
	return nil
}

type countPartial struct {
	baseCount
}

func (e *countPartial) UFIDelatePartialResult(sctx stochastikctx.Context, rowsInGroup []chunk.Event, pr PartialResult) (memDelta int64, err error) {
	p := (*partialResult4Count)(pr)
	for _, event := range rowsInGroup {
		input, isNull, err := e.args[0].EvalInt(sctx, event)
		if err != nil {
			return 0, err
		}
		if isNull {
			continue
		}

		*p += input
	}
	return 0, nil
}

func (*countPartial) MergePartialResult(sctx stochastikctx.Context, src, dst PartialResult) (memDelta int64, err error) {
	p1, p2 := (*partialResult4Count)(src), (*partialResult4Count)(dst)
	*p2 += *p1
	return 0, nil
}
