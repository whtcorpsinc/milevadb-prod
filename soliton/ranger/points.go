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

package ranger

import (
	"fmt"
	"math"
	"sort"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
)

// Error instances.
var (
	ErrUnsupportedType = terror.ClassOptimizer.New(errno.ErrUnsupportedType, errno.MyALLEGROSQLErrName[errno.ErrUnsupportedType])
)

// RangeType is alias for int.
type RangeType int

// RangeType constants.
const (
	IntRangeType RangeType = iota
	DeferredCausetRangeType
	IndexRangeType
)

// Point is the end point of range interval.
type point struct {
	value types.Causet
	excl  bool // exclude
	start bool
}

func (rp point) String() string {
	val := rp.value.GetValue()
	if rp.value.HoTT() == types.HoTTMinNotNull {
		val = "-inf"
	} else if rp.value.HoTT() == types.HoTTMaxValue {
		val = "+inf"
	}
	if rp.start {
		symbol := "["
		if rp.excl {
			symbol = "("
		}
		return fmt.Sprintf("%s%v", symbol, val)
	}
	symbol := "]"
	if rp.excl {
		symbol = ")"
	}
	return fmt.Sprintf("%v%s", val, symbol)
}

type pointSorter struct {
	points []point
	err    error
	sc     *stmtctx.StatementContext
}

func (r *pointSorter) Len() int {
	return len(r.points)
}

func (r *pointSorter) Less(i, j int) bool {
	a := r.points[i]
	b := r.points[j]
	less, err := rangePointLess(r.sc, a, b)
	if err != nil {
		r.err = err
	}
	return less
}

func rangePointLess(sc *stmtctx.StatementContext, a, b point) (bool, error) {
	cmp, err := a.value.CompareCauset(sc, &b.value)
	if cmp != 0 {
		return cmp < 0, nil
	}
	return rangePointEqualValueLess(a, b), errors.Trace(err)
}

func rangePointEqualValueLess(a, b point) bool {
	if a.start && b.start {
		return !a.excl && b.excl
	} else if a.start {
		return !a.excl && !b.excl
	} else if b.start {
		return a.excl || b.excl
	}
	return a.excl && !b.excl
}

func (r *pointSorter) Swap(i, j int) {
	r.points[i], r.points[j] = r.points[j], r.points[i]
}

// fullRange is (-∞, +∞).
var fullRange = []point{
	{start: true},
	{value: types.MaxValueCauset()},
}

// FullIntRange is used for causet range. Since causet range cannot accept MaxValueCauset as the max value.
// So we need to set it to MaxInt64.
func FullIntRange(isUnsigned bool) []*Range {
	if isUnsigned {
		return []*Range{{LowVal: []types.Causet{types.NewUintCauset(0)}, HighVal: []types.Causet{types.NewUintCauset(math.MaxUint64)}}}
	}
	return []*Range{{LowVal: []types.Causet{types.NewIntCauset(math.MinInt64)}, HighVal: []types.Causet{types.NewIntCauset(math.MaxInt64)}}}
}

// FullRange is [null, +∞) for Range.
func FullRange() []*Range {
	return []*Range{{LowVal: []types.Causet{{}}, HighVal: []types.Causet{types.MaxValueCauset()}}}
}

// FullNotNullRange is (-∞, +∞) for Range.
func FullNotNullRange() []*Range {
	return []*Range{{LowVal: []types.Causet{types.MinNotNullCauset()}, HighVal: []types.Causet{types.MaxValueCauset()}}}
}

// NullRange is [null, null] for Range.
func NullRange() []*Range {
	return []*Range{{LowVal: []types.Causet{{}}, HighVal: []types.Causet{{}}}}
}

// builder is the range builder struct.
type builder struct {
	err error
	sc  *stmtctx.StatementContext
	ctx *stochastikctx.Context
}

func (r *builder) build(expr memex.Expression) []point {
	switch x := expr.(type) {
	case *memex.DeferredCauset:
		return r.buildFromDeferredCauset(x)
	case *memex.ScalarFunction:
		return r.buildFromScalarFunc(x)
	case *memex.Constant:
		return r.buildFromConstant(x)
	}

	return fullRange
}

func (r *builder) buildFromConstant(expr *memex.Constant) []point {
	dt, err := expr.Eval(chunk.Row{})
	if err != nil {
		r.err = err
		return nil
	}
	if dt.IsNull() {
		return nil
	}

	val, err := dt.ToBool(r.sc)
	if err != nil {
		r.err = err
		return nil
	}

	if val == 0 {
		return nil
	}
	return fullRange
}

func (r *builder) buildFromDeferredCauset(expr *memex.DeferredCauset) []point {
	// defCausumn name memex is equivalent to defCausumn name is true.
	startPoint1 := point{value: types.MinNotNullCauset(), start: true}
	endPoint1 := point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := point{value: types.MaxValueCauset()}
	return []point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *builder) buildFormBinOp(expr *memex.ScalarFunction) []point {
	// This has been checked that the binary operation is comparison operation, and one of
	// the operand is defCausumn name memex.
	var (
		op    string
		value types.Causet
		err   error
		ft    *types.FieldType
	)

	// refineValue refines the constant causet for string type since we may eval the constant to another defCauslation instead of its own defCauslation.
	refineValue := func(defCaus *memex.DeferredCauset, value *types.Causet) {
		if defCaus.RetType.EvalType() == types.ETString && value.HoTT() == types.HoTTString {
			value.SetString(value.GetString(), defCaus.RetType.DefCauslate)
		}
	}
	if defCaus, ok := expr.GetArgs()[0].(*memex.DeferredCauset); ok {
		ft = defCaus.RetType
		value, err = expr.GetArgs()[1].Eval(chunk.Row{})
		if err != nil {
			return nil
		}
		refineValue(defCaus, &value)
		op = expr.FuncName.L
	} else {
		defCaus, ok := expr.GetArgs()[1].(*memex.DeferredCauset)
		if !ok {
			return nil
		}
		ft = defCaus.RetType
		value, err = expr.GetArgs()[0].Eval(chunk.Row{})
		if err != nil {
			return nil
		}
		refineValue(defCaus, &value)

		switch expr.FuncName.L {
		case ast.GE:
			op = ast.LE
		case ast.GT:
			op = ast.LT
		case ast.LT:
			op = ast.GT
		case ast.LE:
			op = ast.GE
		default:
			op = expr.FuncName.L
		}
	}
	if op != ast.NullEQ && value.IsNull() {
		return nil
	}

	value, op, isValidRange := handleUnsignedIntDefCaus(ft, value, op)
	if !isValidRange {
		return nil
	}

	switch op {
	case ast.NullEQ:
		if value.IsNull() {
			return []point{{start: true}, {}} // [null, null]
		}
		fallthrough
	case ast.EQ:
		startPoint := point{value: value, start: true}
		endPoint := point{value: value}
		return []point{startPoint, endPoint}
	case ast.NE:
		startPoint1 := point{value: types.MinNotNullCauset(), start: true}
		endPoint1 := point{value: value, excl: true}
		startPoint2 := point{value: value, start: true, excl: true}
		endPoint2 := point{value: types.MaxValueCauset()}
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	case ast.LT:
		startPoint := point{value: types.MinNotNullCauset(), start: true}
		endPoint := point{value: value, excl: true}
		return []point{startPoint, endPoint}
	case ast.LE:
		startPoint := point{value: types.MinNotNullCauset(), start: true}
		endPoint := point{value: value}
		return []point{startPoint, endPoint}
	case ast.GT:
		startPoint := point{value: value, start: true, excl: true}
		endPoint := point{value: types.MaxValueCauset()}
		return []point{startPoint, endPoint}
	case ast.GE:
		startPoint := point{value: value, start: true}
		endPoint := point{value: types.MaxValueCauset()}
		return []point{startPoint, endPoint}
	}
	return nil
}

// handleUnsignedIntDefCaus handles the case when unsigned defCausumn meets negative integer value.
// The three returned values are: fixed constant value, fixed operator, and a boolean
// which indicates whether the range is valid or not.
func handleUnsignedIntDefCaus(ft *types.FieldType, val types.Causet, op string) (types.Causet, string, bool) {
	isUnsigned := allegrosql.HasUnsignedFlag(ft.Flag)
	isIntegerType := allegrosql.IsIntegerType(ft.Tp)
	isNegativeInteger := (val.HoTT() == types.HoTTInt64 && val.GetInt64() < 0)

	if !isUnsigned || !isIntegerType || !isNegativeInteger {
		return val, op, true
	}

	// If the operator is GT, GE or NE, the range should be [0, +inf].
	// Otherwise the value is out of valid range.
	if op == ast.GT || op == ast.GE || op == ast.NE {
		op = ast.GE
		val.SetUint64(0)
		return val, op, true
	}

	return val, op, false
}

func (r *builder) buildFromIsTrue(expr *memex.ScalarFunction, isNot int, keepNull bool) []point {
	if isNot == 1 {
		if keepNull {
			// Range is {[0, 0]}
			startPoint := point{start: true}
			startPoint.value.SetInt64(0)
			endPoint := point{}
			endPoint.value.SetInt64(0)
			return []point{startPoint, endPoint}
		}
		// NOT TRUE range is {[null null] [0, 0]}
		startPoint1 := point{start: true}
		endPoint1 := point{}
		startPoint2 := point{start: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := point{}
		endPoint2.value.SetInt64(0)
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// TRUE range is {[-inf 0) (0 +inf]}
	startPoint1 := point{value: types.MinNotNullCauset(), start: true}
	endPoint1 := point{excl: true}
	endPoint1.value.SetInt64(0)
	startPoint2 := point{excl: true, start: true}
	startPoint2.value.SetInt64(0)
	endPoint2 := point{value: types.MaxValueCauset()}
	return []point{startPoint1, endPoint1, startPoint2, endPoint2}
}

func (r *builder) buildFromIsFalse(expr *memex.ScalarFunction, isNot int) []point {
	if isNot == 1 {
		// NOT FALSE range is {[-inf, 0), (0, +inf], [null, null]}
		startPoint1 := point{start: true}
		endPoint1 := point{excl: true}
		endPoint1.value.SetInt64(0)
		startPoint2 := point{start: true, excl: true}
		startPoint2.value.SetInt64(0)
		endPoint2 := point{value: types.MaxValueCauset()}
		return []point{startPoint1, endPoint1, startPoint2, endPoint2}
	}
	// FALSE range is {[0, 0]}
	startPoint := point{start: true}
	startPoint.value.SetInt64(0)
	endPoint := point{}
	endPoint.value.SetInt64(0)
	return []point{startPoint, endPoint}
}

func (r *builder) buildFromIn(expr *memex.ScalarFunction) ([]point, bool) {
	list := expr.GetArgs()[1:]
	rangePoints := make([]point, 0, len(list)*2)
	hasNull := false
	defCausDefCauslate := expr.GetArgs()[0].GetType().DefCauslate
	for _, e := range list {
		v, ok := e.(*memex.Constant)
		if !ok {
			r.err = ErrUnsupportedType.GenWithStack("expr:%v is not constant", e)
			return fullRange, hasNull
		}
		dt, err := v.Eval(chunk.Row{})
		if err != nil {
			r.err = ErrUnsupportedType.GenWithStack("expr:%v is not evaluated", e)
			return fullRange, hasNull
		}
		if dt.IsNull() {
			hasNull = true
			continue
		}
		if dt.HoTT() == types.HoTTString {
			dt.SetString(dt.GetString(), defCausDefCauslate)
		}
		var startValue, endValue types.Causet
		dt.Copy(&startValue)
		dt.Copy(&endValue)
		startPoint := point{value: startValue, start: true}
		endPoint := point{value: endValue}
		rangePoints = append(rangePoints, startPoint, endPoint)
	}
	sorter := pointSorter{points: rangePoints, sc: r.sc}
	sort.Sort(&sorter)
	if sorter.err != nil {
		r.err = sorter.err
	}
	// check and remove duplicates
	curPos, frontPos := 0, 0
	for frontPos < len(rangePoints) {
		if rangePoints[curPos].start == rangePoints[frontPos].start {
			frontPos++
		} else {
			curPos++
			rangePoints[curPos] = rangePoints[frontPos]
			frontPos++
		}
	}
	if curPos > 0 {
		curPos++
	}
	return rangePoints[:curPos], hasNull
}

func (r *builder) newBuildFromPatternLike(expr *memex.ScalarFunction) []point {
	_, defCauslation := expr.CharsetAndDefCauslation(expr.GetCtx())
	if !defCauslate.CompatibleDefCauslate(expr.GetArgs()[0].GetType().DefCauslate, defCauslation) {
		return fullRange
	}
	FIDelt, err := expr.GetArgs()[1].(*memex.Constant).Eval(chunk.Row{})
	tpOfPattern := expr.GetArgs()[0].GetType()
	if err != nil {
		r.err = errors.Trace(err)
		return fullRange
	}
	pattern, err := FIDelt.ToString()
	if err != nil {
		r.err = errors.Trace(err)
		return fullRange
	}
	if pattern == "" {
		startPoint := point{value: types.NewStringCauset(""), start: true}
		endPoint := point{value: types.NewStringCauset("")}
		return []point{startPoint, endPoint}
	}
	lowValue := make([]byte, 0, len(pattern))
	edt, err := expr.GetArgs()[2].(*memex.Constant).Eval(chunk.Row{})
	if err != nil {
		r.err = errors.Trace(err)
		return fullRange
	}
	escape := byte(edt.GetInt64())
	var exclude bool
	isExactMatch := true
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == escape {
			i++
			if i < len(pattern) {
				lowValue = append(lowValue, pattern[i])
			} else {
				lowValue = append(lowValue, escape)
			}
			continue
		}
		if pattern[i] == '%' {
			// Get the prefix.
			isExactMatch = false
			break
		} else if pattern[i] == '_' {
			// Get the prefix, but exclude the prefix.
			// e.g., "abc_x", the start point exclude "abc",
			// because the string length is more than 3.
			exclude = true
			isExactMatch = false
			break
		}
		lowValue = append(lowValue, pattern[i])
	}
	if len(lowValue) == 0 {
		return []point{{value: types.MinNotNullCauset(), start: true}, {value: types.MaxValueCauset()}}
	}
	if isExactMatch {
		val := types.NewDefCauslationStringCauset(string(lowValue), tpOfPattern.DefCauslate, tpOfPattern.Flen)
		return []point{{value: val, start: true}, {value: val}}
	}
	startPoint := point{start: true, excl: exclude}
	startPoint.value.SetBytesAsString(lowValue, tpOfPattern.DefCauslate, uint32(tpOfPattern.Flen))
	highValue := make([]byte, len(lowValue))
	copy(highValue, lowValue)
	endPoint := point{excl: true}
	for i := len(highValue) - 1; i >= 0; i-- {
		// Make the end point value more than the start point value,
		// and the length of the end point value is the same as the length of the start point value.
		// e.g., the start point value is "abc", so the end point value is "abd".
		highValue[i]++
		if highValue[i] != 0 {
			endPoint.value.SetBytesAsString(highValue, tpOfPattern.DefCauslate, uint32(tpOfPattern.Flen))
			break
		}
		// If highValue[i] is 255 and highValue[i]++ is 0, then the end point value is max value.
		if i == 0 {
			endPoint.value = types.MaxValueCauset()
		}
	}
	return []point{startPoint, endPoint}
}

func (r *builder) buildFromNot(expr *memex.ScalarFunction) []point {
	switch n := expr.FuncName.L; n {
	case ast.IsTruthWithoutNull:
		return r.buildFromIsTrue(expr, 1, false)
	case ast.IsTruthWithNull:
		return r.buildFromIsTrue(expr, 1, true)
	case ast.IsFalsity:
		return r.buildFromIsFalse(expr, 1)
	case ast.In:
		var (
			isUnsignedIntDefCaus bool
			nonNegativePos   int
		)
		rangePoints, hasNull := r.buildFromIn(expr)
		if hasNull {
			return nil
		}
		if x, ok := expr.GetArgs()[0].(*memex.DeferredCauset); ok {
			isUnsignedIntDefCaus = allegrosql.HasUnsignedFlag(x.RetType.Flag) && allegrosql.IsIntegerType(x.RetType.Tp)
		}
		// negative ranges can be directly ignored for unsigned int defCausumns.
		if isUnsignedIntDefCaus {
			for nonNegativePos = 0; nonNegativePos < len(rangePoints); nonNegativePos += 2 {
				if rangePoints[nonNegativePos].value.HoTT() == types.HoTTUint64 || rangePoints[nonNegativePos].value.GetInt64() >= 0 {
					break
				}
			}
			rangePoints = rangePoints[nonNegativePos:]
		}
		retRangePoints := make([]point, 0, 2+len(rangePoints))
		previousValue := types.Causet{}
		for i := 0; i < len(rangePoints); i += 2 {
			retRangePoints = append(retRangePoints, point{value: previousValue, start: true, excl: true})
			retRangePoints = append(retRangePoints, point{value: rangePoints[i].value, excl: true})
			previousValue = rangePoints[i].value
		}
		// Append the interval (last element, max value].
		retRangePoints = append(retRangePoints, point{value: previousValue, start: true, excl: true})
		retRangePoints = append(retRangePoints, point{value: types.MaxValueCauset()})
		return retRangePoints
	case ast.Like:
		// Pattern not like is not supported.
		r.err = ErrUnsupportedType.GenWithStack("NOT LIKE is not supported.")
		return fullRange
	case ast.IsNull:
		startPoint := point{value: types.MinNotNullCauset(), start: true}
		endPoint := point{value: types.MaxValueCauset()}
		return []point{startPoint, endPoint}
	}
	return nil
}

func (r *builder) buildFromScalarFunc(expr *memex.ScalarFunction) []point {
	switch op := expr.FuncName.L; op {
	case ast.GE, ast.GT, ast.LT, ast.LE, ast.EQ, ast.NE, ast.NullEQ:
		return r.buildFormBinOp(expr)
	case ast.LogicAnd:
		return r.intersection(r.build(expr.GetArgs()[0]), r.build(expr.GetArgs()[1]))
	case ast.LogicOr:
		return r.union(r.build(expr.GetArgs()[0]), r.build(expr.GetArgs()[1]))
	case ast.IsTruthWithoutNull:
		return r.buildFromIsTrue(expr, 0, false)
	case ast.IsTruthWithNull:
		return r.buildFromIsTrue(expr, 0, true)
	case ast.IsFalsity:
		return r.buildFromIsFalse(expr, 0)
	case ast.In:
		retPoints, _ := r.buildFromIn(expr)
		return retPoints
	case ast.Like:
		return r.newBuildFromPatternLike(expr)
	case ast.IsNull:
		startPoint := point{start: true}
		endPoint := point{}
		return []point{startPoint, endPoint}
	case ast.UnaryNot:
		return r.buildFromNot(expr.GetArgs()[0].(*memex.ScalarFunction))
	}

	return nil
}

func (r *builder) intersection(a, b []point) []point {
	return r.merge(a, b, false)
}

func (r *builder) union(a, b []point) []point {
	return r.merge(a, b, true)
}

func (r *builder) mergeSorted(a, b []point) []point {
	ret := make([]point, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		less, err := rangePointLess(r.sc, a[i], b[j])
		if err != nil {
			r.err = err
			return nil
		}
		if less {
			ret = append(ret, a[i])
			i++
		} else {
			ret = append(ret, b[j])
			j++
		}
	}
	if i < len(a) {
		ret = append(ret, a[i:]...)
	} else if j < len(b) {
		ret = append(ret, b[j:]...)
	}
	return ret
}

func (r *builder) merge(a, b []point, union bool) []point {
	mergedPoints := r.mergeSorted(a, b)
	if r.err != nil {
		return nil
	}

	var (
		inRangeCount         int
		requiredInRangeCount int
	)
	if union {
		requiredInRangeCount = 1
	} else {
		requiredInRangeCount = 2
	}
	curTail := 0
	for _, val := range mergedPoints {
		if val.start {
			inRangeCount++
			if inRangeCount == requiredInRangeCount {
				// Just reached the required in range count, a new range started.
				mergedPoints[curTail] = val
				curTail++
			}
		} else {
			if inRangeCount == requiredInRangeCount {
				// Just about to leave the required in range count, the range is ended.
				mergedPoints[curTail] = val
				curTail++
			}
			inRangeCount--
		}
	}
	return mergedPoints[:curTail]
}
