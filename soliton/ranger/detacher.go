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
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
)

// detachDeferredCausetCNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is CNF form.
func detachDeferredCausetCNFConditions(sctx stochastikctx.Context, conditions []memex.Expression, checker *conditionChecker) ([]memex.Expression, []memex.Expression) {
	var accessConditions, filterConditions []memex.Expression
	for _, cond := range conditions {
		if sf, ok := cond.(*memex.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			dnfItems := memex.FlattenDNFConditions(sf)
			defCausumnDNFItems, hasResidual := detachDeferredCausetDNFConditions(sctx, dnfItems, checker)
			// If this CNF has memex that cannot be resolved as access condition, then the total DNF memex
			// should be also appended into filter condition.
			if hasResidual {
				filterConditions = append(filterConditions, cond)
			}
			if len(defCausumnDNFItems) == 0 {
				continue
			}
			rebuildDNF := memex.ComposeDNFCondition(sctx, defCausumnDNFItems...)
			accessConditions = append(accessConditions, rebuildDNF)
			continue
		}
		if !checker.check(cond) {
			filterConditions = append(filterConditions, cond)
			continue
		}
		accessConditions = append(accessConditions, cond)
		if checker.shouldReserve {
			filterConditions = append(filterConditions, cond)
			checker.shouldReserve = checker.length != types.UnspecifiedLength
		}
	}
	return accessConditions, filterConditions
}

// detachDeferredCausetDNFConditions detaches the condition for calculating range from the other conditions.
// Please make sure that the top level is DNF form.
func detachDeferredCausetDNFConditions(sctx stochastikctx.Context, conditions []memex.Expression, checker *conditionChecker) ([]memex.Expression, bool) {
	var (
		hasResidualConditions bool
		accessConditions      []memex.Expression
	)
	for _, cond := range conditions {
		if sf, ok := cond.(*memex.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := memex.FlattenCNFConditions(sf)
			defCausumnCNFItems, others := detachDeferredCausetCNFConditions(sctx, cnfItems, checker)
			if len(others) > 0 {
				hasResidualConditions = true
			}
			// If one part of DNF has no access condition. Then this DNF cannot get range.
			if len(defCausumnCNFItems) == 0 {
				return nil, true
			}
			rebuildCNF := memex.ComposeCNFCondition(sctx, defCausumnCNFItems...)
			accessConditions = append(accessConditions, rebuildCNF)
		} else if checker.check(cond) {
			accessConditions = append(accessConditions, cond)
			if checker.shouldReserve {
				hasResidualConditions = true
				checker.shouldReserve = checker.length != types.UnspecifiedLength
			}
		} else {
			return nil, true
		}
	}
	return accessConditions, hasResidualConditions
}

// getEqOrInDefCausOffset checks if the memex is a eq function that one side is constant and another is defCausumn or an
// in function which is `defCausumn in (constant list)`.
// If so, it will return the offset of this defCausumn in the slice, otherwise return -1 for not found.
func getEqOrInDefCausOffset(expr memex.Expression, defcaus []*memex.DeferredCauset) int {
	f, ok := expr.(*memex.ScalarFunction)
	if !ok {
		return -1
	}
	_, defCauslation := expr.CharsetAndDefCauslation(f.GetCtx())
	switch f.FuncName.L {
	case ast.LogicOr:
		dnfItems := memex.FlattenDNFConditions(f)
		offset := int(-1)
		for _, dnfItem := range dnfItems {
			curOffset := getEqOrInDefCausOffset(dnfItem, defcaus)
			if curOffset == -1 {
				return -1
			}
			if offset != -1 && curOffset != offset {
				return -1
			}
			offset = curOffset
		}
		return offset
	case ast.EQ, ast.NullEQ:
		if c, ok := f.GetArgs()[0].(*memex.DeferredCauset); ok {
			if c.RetType.EvalType() == types.ETString && !defCauslate.CompatibleDefCauslate(c.RetType.DefCauslate, defCauslation) {
				return -1
			}
			if constVal, ok := f.GetArgs()[1].(*memex.Constant); ok {
				val, err := constVal.Eval(chunk.Row{})
				if err != nil || val.IsNull() {
					// treat defCaus<=>null as range scan instead of point get to avoid incorrect results
					// when nullable unique index has multiple matches for filter x is null
					return -1
				}
				for i, defCaus := range defcaus {
					if defCaus.Equal(nil, c) {
						return i
					}
				}
			}
		}
		if c, ok := f.GetArgs()[1].(*memex.DeferredCauset); ok {
			if c.RetType.EvalType() == types.ETString && !defCauslate.CompatibleDefCauslate(c.RetType.DefCauslate, defCauslation) {
				return -1
			}
			if constVal, ok := f.GetArgs()[0].(*memex.Constant); ok {
				val, err := constVal.Eval(chunk.Row{})
				if err != nil || val.IsNull() {
					return -1
				}
				for i, defCaus := range defcaus {
					if defCaus.Equal(nil, c) {
						return i
					}
				}
			}
		}
	case ast.In:
		c, ok := f.GetArgs()[0].(*memex.DeferredCauset)
		if !ok {
			return -1
		}
		if c.RetType.EvalType() == types.ETString && !defCauslate.CompatibleDefCauslate(c.RetType.DefCauslate, defCauslation) {
			return -1
		}
		for _, arg := range f.GetArgs()[1:] {
			if _, ok := arg.(*memex.Constant); !ok {
				return -1
			}
		}
		for i, defCaus := range defcaus {
			if defCaus.Equal(nil, c) {
				return i
			}
		}
	}
	return -1
}

// extractIndexPointRangesForCNF extracts a CNF item from the input CNF memexs, such that the CNF item
// is totally composed of point range filters.
// e.g, for input CNF memexs ((a,b) in ((1,1),(2,2))) and a > 1 and ((a,b,c) in (1,1,1),(2,2,2))
// ((a,b,c) in (1,1,1),(2,2,2)) would be extracted.
func extractIndexPointRangesForCNF(sctx stochastikctx.Context, conds []memex.Expression, defcaus []*memex.DeferredCauset, lengths []int) (*DetachRangeResult, int, error) {
	if len(conds) < 2 {
		return nil, -1, nil
	}
	var r *DetachRangeResult
	maxNumDefCauss := int(0)
	offset := int(-1)
	for i, cond := range conds {
		tmpConds := []memex.Expression{cond}
		defCausSets := memex.ExtractDeferredCausetSet(tmpConds)
		origDefCausNum := defCausSets.Len()
		if origDefCausNum == 0 {
			continue
		}
		if l := len(defcaus); origDefCausNum > l {
			origDefCausNum = l
		}
		currDefCauss := defcaus[:origDefCausNum]
		currLengths := lengths[:origDefCausNum]
		res, err := DetachCondAndBuildRangeForIndex(sctx, tmpConds, currDefCauss, currLengths)
		if err != nil {
			return nil, -1, err
		}
		if len(res.Ranges) == 0 {
			return &DetachRangeResult{}, -1, nil
		}
		if len(res.AccessConds) == 0 || len(res.RemainedConds) > 0 {
			continue
		}
		sameLens, allPoints := true, true
		numDefCauss := int(0)
		for i, ran := range res.Ranges {
			if !ran.IsPoint(sctx.GetStochastikVars().StmtCtx) {
				allPoints = false
				break
			}
			if i == 0 {
				numDefCauss = len(ran.LowVal)
			} else if numDefCauss != len(ran.LowVal) {
				sameLens = false
				break
			}
		}
		if !allPoints || !sameLens {
			continue
		}
		if numDefCauss > maxNumDefCauss {
			r = res
			offset = i
			maxNumDefCauss = numDefCauss
		}
	}
	if r != nil {
		r.IsDNFCond = false
	}
	return r, offset, nil
}

// detachCNFCondAndBuildRangeForIndex will detach the index filters from causet filters. These conditions are connected with `and`
// It will first find the point query defCausumn and then extract the range query defCausumn.
// considerDNF is true means it will try to extract access conditions from the DNF memexs.
func (d *rangeDetacher) detachCNFCondAndBuildRangeForIndex(conditions []memex.Expression, tpSlice []*types.FieldType, considerDNF bool) (*DetachRangeResult, error) {
	var (
		eqCount int
		ranges  []*Range
		err     error
	)
	res := &DetachRangeResult{}

	accessConds, filterConds, newConditions, emptyRange := ExtractEqAndInCondition(d.sctx, conditions, d.defcaus, d.lengths)
	if emptyRange {
		return res, nil
	}
	for ; eqCount < len(accessConds); eqCount++ {
		if accessConds[eqCount].(*memex.ScalarFunction).FuncName.L != ast.EQ {
			break
		}
	}
	eqOrInCount := len(accessConds)
	res.EqCondCount = eqCount
	res.EqOrInCount = eqOrInCount
	ranges, err = d.buildCNFIndexRange(tpSlice, eqOrInCount, accessConds)
	if err != nil {
		return res, err
	}
	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	if eqOrInCount == len(d.defcaus) || len(newConditions) == 0 {
		res.RemainedConds = append(res.RemainedConds, newConditions...)
		return res, nil
	}
	checker := &conditionChecker{
		defCausUniqueID:   d.defcaus[eqOrInCount].UniqueID,
		length:        d.lengths[eqOrInCount],
		shouldReserve: d.lengths[eqOrInCount] != types.UnspecifiedLength,
	}
	if considerDNF {
		pointRes, offset, err := extractIndexPointRangesForCNF(d.sctx, conditions, d.defcaus, d.lengths)
		if err != nil {
			return nil, err
		}
		if pointRes != nil {
			if len(pointRes.Ranges) == 0 {
				return &DetachRangeResult{}, nil
			}
			if len(pointRes.Ranges[0].LowVal) > eqOrInCount {
				res = pointRes
				eqOrInCount = len(res.Ranges[0].LowVal)
				newConditions = newConditions[:0]
				newConditions = append(newConditions, conditions[:offset]...)
				newConditions = append(newConditions, conditions[offset+1:]...)
				if eqOrInCount == len(d.defcaus) || len(newConditions) == 0 {
					res.RemainedConds = append(res.RemainedConds, newConditions...)
					return res, nil
				}
			}
		}
		if eqOrInCount > 0 {
			newDefCauss := d.defcaus[eqOrInCount:]
			newLengths := d.lengths[eqOrInCount:]
			tailRes, err := DetachCondAndBuildRangeForIndex(d.sctx, newConditions, newDefCauss, newLengths)
			if err != nil {
				return nil, err
			}
			if len(tailRes.Ranges) == 0 {
				return &DetachRangeResult{}, nil
			}
			if len(tailRes.AccessConds) > 0 {
				res.Ranges = appendRanges2PointRanges(res.Ranges, tailRes.Ranges)
				res.AccessConds = append(res.AccessConds, tailRes.AccessConds...)
			}
			res.RemainedConds = append(res.RemainedConds, tailRes.RemainedConds...)
			// For cases like `((a = 1 and b = 1) or (a = 2 and b = 2)) and c = 1` on index (a,b,c), eqOrInCount is 2,
			// res.EqOrInCount is 0, and tailRes.EqOrInCount is 1. We should not set res.EqOrInCount to 1, otherwise,
			// `b = CorrelatedDeferredCauset` would be extracted as access conditions as well, which is not as expected at least for now.
			if res.EqOrInCount > 0 {
				if res.EqOrInCount == res.EqCondCount {
					res.EqCondCount = res.EqCondCount + tailRes.EqCondCount
				}
				res.EqOrInCount = res.EqOrInCount + tailRes.EqOrInCount
			}
			return res, nil
		}
		// `eqOrInCount` must be 0 when coming here.
		res.AccessConds, res.RemainedConds = detachDeferredCausetCNFConditions(d.sctx, newConditions, checker)
		ranges, err = d.buildCNFIndexRange(tpSlice, 0, res.AccessConds)
		if err != nil {
			return nil, err
		}
		res.Ranges = ranges
		return res, nil
	}
	for _, cond := range newConditions {
		if !checker.check(cond) {
			filterConds = append(filterConds, cond)
			continue
		}
		accessConds = append(accessConds, cond)
	}
	ranges, err = d.buildCNFIndexRange(tpSlice, eqOrInCount, accessConds)
	if err != nil {
		return nil, err
	}
	res.Ranges = ranges
	res.AccessConds = accessConds
	res.RemainedConds = filterConds
	return res, nil
}

// ExtractEqAndInCondition will split the given condition into three parts by the information of index defCausumns and their lengths.
// accesses: The condition will be used to build range.
// filters: filters is the part that some access conditions need to be evaluate again since it's only the prefix part of char defCausumn.
// newConditions: We'll simplify the given conditions if there're multiple in conditions or eq conditions on the same defCausumn.
//   e.g. if there're a in (1, 2, 3) and a in (2, 3, 4). This two will be combined to a in (2, 3) and pushed to newConditions.
// bool: indicate whether there's nil range when merging eq and in conditions.
func ExtractEqAndInCondition(sctx stochastikctx.Context, conditions []memex.Expression,
	defcaus []*memex.DeferredCauset, lengths []int) ([]memex.Expression, []memex.Expression, []memex.Expression, bool) {
	var filters []memex.Expression
	rb := builder{sc: sctx.GetStochastikVars().StmtCtx}
	accesses := make([]memex.Expression, len(defcaus))
	points := make([][]point, len(defcaus))
	mergedAccesses := make([]memex.Expression, len(defcaus))
	newConditions := make([]memex.Expression, 0, len(conditions))
	for _, cond := range conditions {
		offset := getEqOrInDefCausOffset(cond, defcaus)
		if offset == -1 {
			newConditions = append(newConditions, cond)
			continue
		}
		if accesses[offset] == nil {
			accesses[offset] = cond
			continue
		}
		// Multiple Eq/In conditions for one defCausumn in CNF, apply intersection on them
		// Lazily compute the points for the previously visited Eq/In
		if mergedAccesses[offset] == nil {
			mergedAccesses[offset] = accesses[offset]
			points[offset] = rb.build(accesses[offset])
		}
		points[offset] = rb.intersection(points[offset], rb.build(cond))
		// Early termination if false memex found
		if len(points[offset]) == 0 {
			return nil, nil, nil, true
		}
	}
	for i, ma := range mergedAccesses {
		if ma == nil {
			if accesses[i] != nil {
				newConditions = append(newConditions, accesses[i])
			}
			continue
		}
		accesses[i] = points2EqOrInCond(sctx, points[i], mergedAccesses[i])
		newConditions = append(newConditions, accesses[i])
	}
	for i, cond := range accesses {
		if cond == nil {
			accesses = accesses[:i]
			break
		}
		if lengths[i] != types.UnspecifiedLength {
			filters = append(filters, cond)
		}
	}
	// We should remove all accessConds, so that they will not be added to filter conditions.
	newConditions = removeAccessConditions(newConditions, accesses)
	return accesses, filters, newConditions, false
}

// detachDNFCondAndBuildRangeForIndex will detach the index filters from causet filters when it's a DNF.
// We will detach the conditions of every DNF items, then compose them to a DNF.
func (d *rangeDetacher) detachDNFCondAndBuildRangeForIndex(condition *memex.ScalarFunction, newTpSlice []*types.FieldType) ([]*Range, []memex.Expression, bool, error) {
	sc := d.sctx.GetStochastikVars().StmtCtx
	firstDeferredCausetChecker := &conditionChecker{
		defCausUniqueID:   d.defcaus[0].UniqueID,
		shouldReserve: d.lengths[0] != types.UnspecifiedLength,
		length:        d.lengths[0],
	}
	rb := builder{sc: sc}
	dnfItems := memex.FlattenDNFConditions(condition)
	newAccessItems := make([]memex.Expression, 0, len(dnfItems))
	var totalRanges []*Range
	hasResidual := false
	for _, item := range dnfItems {
		if sf, ok := item.(*memex.ScalarFunction); ok && sf.FuncName.L == ast.LogicAnd {
			cnfItems := memex.FlattenCNFConditions(sf)
			var accesses, filters []memex.Expression
			res, err := d.detachCNFCondAndBuildRangeForIndex(cnfItems, newTpSlice, true)
			if err != nil {
				return nil, nil, false, nil
			}
			ranges := res.Ranges
			accesses = res.AccessConds
			filters = res.RemainedConds
			if len(accesses) == 0 {
				return FullRange(), nil, true, nil
			}
			if len(filters) > 0 {
				hasResidual = true
			}
			totalRanges = append(totalRanges, ranges...)
			newAccessItems = append(newAccessItems, memex.ComposeCNFCondition(d.sctx, accesses...))
		} else if firstDeferredCausetChecker.check(item) {
			if firstDeferredCausetChecker.shouldReserve {
				hasResidual = true
				firstDeferredCausetChecker.shouldReserve = d.lengths[0] != types.UnspecifiedLength
			}
			points := rb.build(item)
			ranges, err := points2Ranges(sc, points, newTpSlice[0])
			if err != nil {
				return nil, nil, false, errors.Trace(err)
			}
			totalRanges = append(totalRanges, ranges...)
			newAccessItems = append(newAccessItems, item)
		} else {
			return FullRange(), nil, true, nil
		}
	}

	totalRanges, err := UnionRanges(sc, totalRanges, d.mergeConsecutive)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	return totalRanges, []memex.Expression{memex.ComposeDNFCondition(d.sctx, newAccessItems...)}, hasResidual, nil
}

// DetachRangeResult wraps up results when detaching conditions and builing ranges.
type DetachRangeResult struct {
	// Ranges is the ranges extracted and built from conditions.
	Ranges []*Range
	// AccessConds is the extracted conditions for access.
	AccessConds []memex.Expression
	// RemainedConds is the filter conditions which should be kept after access.
	RemainedConds []memex.Expression
	// EqCondCount is the number of equal conditions extracted.
	EqCondCount int
	// EqOrInCount is the number of equal/in conditions extracted.
	EqOrInCount int
	// IsDNFCond indicates if the top layer of conditions are in DNF.
	IsDNFCond bool
}

// DetachCondAndBuildRangeForIndex will detach the index filters from causet filters.
// The returned values are encapsulated into a struct DetachRangeResult, see its comments for explanation.
func DetachCondAndBuildRangeForIndex(sctx stochastikctx.Context, conditions []memex.Expression, defcaus []*memex.DeferredCauset,
	lengths []int) (*DetachRangeResult, error) {
	d := &rangeDetacher{
		sctx:             sctx,
		allConds:         conditions,
		defcaus:             defcaus,
		lengths:          lengths,
		mergeConsecutive: true,
	}
	return d.detachCondAndBuildRangeForDefCauss()
}

type rangeDetacher struct {
	sctx             stochastikctx.Context
	allConds         []memex.Expression
	defcaus             []*memex.DeferredCauset
	lengths          []int
	mergeConsecutive bool
}

func (d *rangeDetacher) detachCondAndBuildRangeForDefCauss() (*DetachRangeResult, error) {
	res := &DetachRangeResult{}
	newTpSlice := make([]*types.FieldType, 0, len(d.defcaus))
	for _, defCaus := range d.defcaus {
		newTpSlice = append(newTpSlice, newFieldType(defCaus.RetType))
	}
	if len(d.allConds) == 1 {
		if sf, ok := d.allConds[0].(*memex.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			ranges, accesses, hasResidual, err := d.detachDNFCondAndBuildRangeForIndex(sf, newTpSlice)
			if err != nil {
				return res, errors.Trace(err)
			}
			res.Ranges = ranges
			res.AccessConds = accesses
			res.IsDNFCond = true
			// If this DNF have something cannot be to calculate range, then all this DNF should be pushed as filter condition.
			if hasResidual {
				res.RemainedConds = d.allConds
				return res, nil
			}
			return res, nil
		}
	}
	return d.detachCNFCondAndBuildRangeForIndex(d.allConds, newTpSlice, true)
}

// DetachSimpleCondAndBuildRangeForIndex will detach the index filters from causet filters.
// It will find the point query defCausumn firstly and then extract the range query defCausumn.
func DetachSimpleCondAndBuildRangeForIndex(sctx stochastikctx.Context, conditions []memex.Expression,
	defcaus []*memex.DeferredCauset, lengths []int) ([]*Range, []memex.Expression, error) {
	newTpSlice := make([]*types.FieldType, 0, len(defcaus))
	for _, defCaus := range defcaus {
		newTpSlice = append(newTpSlice, newFieldType(defCaus.RetType))
	}
	d := &rangeDetacher{
		sctx:             sctx,
		allConds:         conditions,
		defcaus:             defcaus,
		lengths:          lengths,
		mergeConsecutive: true,
	}
	res, err := d.detachCNFCondAndBuildRangeForIndex(conditions, newTpSlice, false)
	return res.Ranges, res.AccessConds, err
}

func removeAccessConditions(conditions, accessConds []memex.Expression) []memex.Expression {
	filterConds := make([]memex.Expression, 0, len(conditions))
	for _, cond := range conditions {
		if !memex.Contains(accessConds, cond) {
			filterConds = append(filterConds, cond)
		}
	}
	return filterConds
}

// ExtractAccessConditionsForDeferredCauset extracts the access conditions used for range calculation. Since
// we don't need to return the remained filter conditions, it is much simpler than DetachCondsForDeferredCauset.
func ExtractAccessConditionsForDeferredCauset(conds []memex.Expression, uniqueID int64) []memex.Expression {
	checker := conditionChecker{
		defCausUniqueID: uniqueID,
		length:      types.UnspecifiedLength,
	}
	accessConds := make([]memex.Expression, 0, 8)
	return memex.Filter(accessConds, conds, checker.check)
}

// DetachCondsForDeferredCauset detaches access conditions for specified defCausumn from other filter conditions.
func DetachCondsForDeferredCauset(sctx stochastikctx.Context, conds []memex.Expression, defCaus *memex.DeferredCauset) (accessConditions, otherConditions []memex.Expression) {
	checker := &conditionChecker{
		defCausUniqueID: defCaus.UniqueID,
		length:      types.UnspecifiedLength,
	}
	return detachDeferredCausetCNFConditions(sctx, conds, checker)
}

// MergeDNFItems4DefCaus receives a slice of DNF conditions, merges some of them which can be built into ranges on a single defCausumn, then returns.
// For example, [a > 5, b > 6, c > 7, a = 1, b > 3] will become [a > 5 or a = 1, b > 6 or b > 3, c > 7].
func MergeDNFItems4DefCaus(ctx stochastikctx.Context, dnfItems []memex.Expression) []memex.Expression {
	mergedDNFItems := make([]memex.Expression, 0, len(dnfItems))
	defCaus2DNFItems := make(map[int64][]memex.Expression)
	for _, dnfItem := range dnfItems {
		defcaus := memex.ExtractDeferredCausets(dnfItem)
		// If this condition contains multiple defCausumns, we can't merge it.
		// If this defCausumn is _milevadb_rowid, we also can't merge it since Selectivity() doesn't handle it, or infinite recursion will happen.
		if len(defcaus) != 1 || defcaus[0].ID == perceptron.ExtraHandleID {
			mergedDNFItems = append(mergedDNFItems, dnfItem)
			continue
		}

		uniqueID := defcaus[0].UniqueID
		checker := &conditionChecker{
			defCausUniqueID: uniqueID,
			length:      types.UnspecifiedLength,
		}
		// If we can't use this condition to build range, we can't merge it.
		// Currently, we assume if every condition in a DNF memex can pass this check, then `Selectivity` must be able to
		// cover this entire DNF directly without recursively call `Selectivity`. If this doesn't hold in the future, this logic
		// may cause infinite recursion in `Selectivity`.
		if !checker.check(dnfItem) {
			mergedDNFItems = append(mergedDNFItems, dnfItem)
			continue
		}

		defCaus2DNFItems[uniqueID] = append(defCaus2DNFItems[uniqueID], dnfItem)
	}
	for _, items := range defCaus2DNFItems {
		mergedDNFItems = append(mergedDNFItems, memex.ComposeDNFCondition(ctx, items...))
	}
	return mergedDNFItems
}
