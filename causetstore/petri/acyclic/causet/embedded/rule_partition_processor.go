// Copyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/math"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// FullRange represent used all partitions.
const FullRange = -1

// partitionProcessor rewrites the ast for causet partition.
//
// create causet t (id int) partition by range (id)
//   (partition p1 values less than (10),
//    partition p2 values less than (20),
//    partition p3 values less than (30))
//
// select * from t is equal to
// select * from (union all
//      select * from p1 where id < 10
//      select * from p2 where id < 20
//      select * from p3 where id < 30)
//
// partitionProcessor is here because it's easier to prune partition after predicate push down.
type partitionProcessor struct{}

func (s *partitionProcessor) optimize(ctx context.Context, lp LogicalCauset) (LogicalCauset, error) {
	return s.rewriteDataSource(lp)
}

func (s *partitionProcessor) rewriteDataSource(lp LogicalCauset) (LogicalCauset, error) {
	// Assert there will not be sel -> sel in the ast.
	switch p := lp.(type) {
	case *DataSource:
		return s.prune(p)
	case *LogicalUnionScan:
		ds := p.Children()[0]
		ds, err := s.prune(ds.(*DataSource))
		if err != nil {
			return nil, err
		}
		if ua, ok := ds.(*LogicalPartitionUnionAll); ok {
			// Adjust the UnionScan->Union->DataSource1, DataSource2 ... to
			// Union->(UnionScan->DataSource1), (UnionScan->DataSource2)
			children := make([]LogicalCauset, 0, len(ua.Children()))
			for _, child := range ua.Children() {
				us := LogicalUnionScan{
					conditions:     p.conditions,
					handleDefCauss: p.handleDefCauss,
				}.Init(ua.ctx, ua.blockOffset)
				us.SetChildren(child)
				children = append(children, us)
			}
			ua.SetChildren(children...)
			return ua, nil
		}
		// Only one partition, no union all.
		p.SetChildren(ds)
		return p, nil
	default:
		children := lp.Children()
		for i, child := range children {
			newChild, err := s.rewriteDataSource(child)
			if err != nil {
				return nil, err
			}
			children[i] = newChild
		}
	}

	return lp, nil
}

// partitionBlock is for those blocks which implement partition.
type partitionBlock interface {
	PartitionExpr() (*blocks.PartitionExpr, error)
}

func generateHashPartitionExpr(ctx stochastikctx.Context, pi *perceptron.PartitionInfo, columns []*memex.DeferredCauset, names types.NameSlice) (memex.Expression, error) {
	schemaReplicant := memex.NewSchema(columns...)
	exprs, err := memex.ParseSimpleExprsWithNames(ctx, pi.Expr, schemaReplicant, names)
	if err != nil {
		return nil, err
	}
	exprs[0].HashCode(ctx.GetStochastikVars().StmtCtx)
	return exprs[0], nil
}

func (s *partitionProcessor) findUsedPartitions(ctx stochastikctx.Context, tbl causet.Block, partitionNames []perceptron.CIStr,
	conds []memex.Expression, columns []*memex.DeferredCauset, names types.NameSlice) ([]int, []memex.Expression, error) {
	pi := tbl.Meta().Partition
	pe, err := generateHashPartitionExpr(ctx, pi, columns, names)
	if err != nil {
		return nil, nil, err
	}
	partIdx := memex.ExtractDeferredCausets(pe)
	colLen := make([]int, 0, len(partIdx))
	for i := 0; i < len(partIdx); i++ {
		partIdx[i].Index = i
		colLen = append(colLen, types.UnspecifiedLength)
	}
	datchedResult, err := ranger.DetachCondAndBuildRangeForPartition(ctx, conds, partIdx, colLen)
	if err != nil {
		return nil, nil, err
	}
	ranges := datchedResult.Ranges
	used := make([]int, 0, len(ranges))
	for _, r := range ranges {
		if r.IsPointNullable(ctx.GetStochastikVars().StmtCtx) {
			if !r.HighVal[0].IsNull() {
				if len(r.HighVal) != len(partIdx) {
					used = []int{-1}
					break
				}
			}
			pos, isNull, err := pe.EvalInt(ctx, chunk.MutRowFromCausets(r.HighVal).ToRow())
			if err != nil {
				return nil, nil, err
			}
			if isNull {
				pos = 0
			}
			idx := math.Abs(pos % int64(pi.Num))
			if len(partitionNames) > 0 && !s.findByName(partitionNames, pi.Definitions[idx].Name.L) {
				continue
			}
			used = append(used, int(idx))
		} else {
			used = []int{FullRange}
			break
		}
	}
	if len(partitionNames) > 0 && len(used) == 1 && used[0] == FullRange {
		or := partitionRangeOR{partitionRange{0, len(pi.Definitions)}}
		return s.convertToIntSlice(or, pi, partitionNames), nil, nil
	}
	sort.Ints(used)
	ret := used[:0]
	for i := 0; i < len(used); i++ {
		if i == 0 || used[i] != used[i-1] {
			ret = append(ret, used[i])
		}
	}
	return ret, datchedResult.RemainedConds, nil
}

func (s *partitionProcessor) convertToIntSlice(or partitionRangeOR, pi *perceptron.PartitionInfo, partitionNames []perceptron.CIStr) []int {
	if len(or) == 1 && or[0].start == 0 && or[0].end == len(pi.Definitions) {
		if len(partitionNames) == 0 {
			return []int{FullRange}
		}
	}
	ret := make([]int, 0, len(or))
	for i := 0; i < len(or); i++ {
		for pos := or[i].start; pos < or[i].end; pos++ {
			if len(partitionNames) > 0 && !s.findByName(partitionNames, pi.Definitions[pos].Name.L) {
				continue
			}
			ret = append(ret, pos)
		}
	}
	return ret
}

func convertToRangeOr(used []int, pi *perceptron.PartitionInfo) partitionRangeOR {
	if len(used) == 1 && used[0] == -1 {
		return fullRange(len(pi.Definitions))
	}
	ret := make(partitionRangeOR, 0, len(used))
	for _, i := range used {
		ret = append(ret, partitionRange{i, i + 1})
	}
	return ret
}

func (s *partitionProcessor) pruneHashPartition(ctx stochastikctx.Context, tbl causet.Block, partitionNames []perceptron.CIStr,
	conds []memex.Expression, columns []*memex.DeferredCauset, names types.NameSlice) ([]int, error) {
	used, _, err := s.findUsedPartitions(ctx, tbl, partitionNames, conds, columns, names)
	if err != nil {
		return nil, err
	}
	return used, nil
}

func (s *partitionProcessor) processHashPartition(ds *DataSource, pi *perceptron.PartitionInfo) (LogicalCauset, error) {
	used, err := s.pruneHashPartition(ds.SCtx(), ds.causet, ds.partitionNames, ds.allConds, ds.TblDefCauss, ds.names)
	if err != nil {
		return nil, err
	}
	if used != nil {
		return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi))
	}
	blockDual := LogicalBlockDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
	blockDual.schemaReplicant = ds.Schema()
	return blockDual, nil
}

func (s *partitionProcessor) prune(ds *DataSource) (LogicalCauset, error) {
	pi := ds.blockInfo.GetPartitionInfo()
	if pi == nil {
		return ds, nil
	}
	// Try to locate partition directly for hash partition.
	if pi.Type == perceptron.PartitionTypeHash {
		return s.processHashPartition(ds, pi)
	}
	if pi.Type == perceptron.PartitionTypeRange {
		return s.processRangePartition(ds, pi)
	}

	// We haven't implement partition by list and so on.
	return s.makeUnionAllChildren(ds, pi, fullRange(len(pi.Definitions)))
}

// findByName checks whether object name exists in list.
func (s *partitionProcessor) findByName(partitionNames []perceptron.CIStr, partitionName string) bool {
	for _, s := range partitionNames {
		if s.L == partitionName {
			return true
		}
	}
	return false
}

func (*partitionProcessor) name() string {
	return "partition_processor"
}

type lessThanDataInt struct {
	data     []int64
	maxvalue bool
}

func (lt *lessThanDataInt) length() int {
	return len(lt.data)
}

func compareUnsigned(v1, v2 int64) int {
	switch {
	case uint64(v1) > uint64(v2):
		return 1
	case uint64(v1) == uint64(v2):
		return 0
	}
	return -1
}

func (lt *lessThanDataInt) compare(ith int, v int64, unsigned bool) int {
	if ith == len(lt.data)-1 {
		if lt.maxvalue {
			return 1
		}
	}
	if unsigned {
		return compareUnsigned(lt.data[ith], v)
	}
	switch {
	case lt.data[ith] > v:
		return 1
	case lt.data[ith] == v:
		return 0
	}
	return -1
}

// partitionRange represents [start, range)
type partitionRange struct {
	start int
	end   int
}

// partitionRangeOR represents OR(range1, range2, ...)
type partitionRangeOR []partitionRange

func fullRange(end int) partitionRangeOR {
	var reduceAllocation [3]partitionRange
	reduceAllocation[0] = partitionRange{0, end}
	return reduceAllocation[:1]
}

func (or partitionRangeOR) intersectionRange(start, end int) partitionRangeOR {
	// Let M = intersection, U = union, then
	// a M (b U c) == (a M b) U (a M c)
	ret := or[:0]
	for _, r1 := range or {
		newStart, newEnd := intersectionRange(r1.start, r1.end, start, end)
		// Exclude the empty one.
		if newEnd > newStart {
			ret = append(ret, partitionRange{newStart, newEnd})
		}
	}
	return ret
}

func (or partitionRangeOR) Len() int {
	return len(or)
}

func (or partitionRangeOR) Less(i, j int) bool {
	return or[i].start < or[j].start
}

func (or partitionRangeOR) Swap(i, j int) {
	or[i], or[j] = or[j], or[i]
}

func (or partitionRangeOR) union(x partitionRangeOR) partitionRangeOR {
	or = append(or, x...)
	return or.simplify()
}

func (or partitionRangeOR) simplify() partitionRangeOR {
	// Make the ranges order by start.
	sort.Sort(or)
	sorted := or

	// Iterate the sorted ranges, merge the adjacent two when their range overlap.
	// For example, [0, 1), [2, 7), [3, 5), ... => [0, 1), [2, 7) ...
	res := sorted[:1]
	for _, curr := range sorted[1:] {
		last := &res[len(res)-1]
		if curr.start > last.end {
			res = append(res, curr)
		} else {
			// Merge two.
			if curr.end > last.end {
				last.end = curr.end
			}
		}
	}
	return res
}

func (or partitionRangeOR) intersection(x partitionRangeOR) partitionRangeOR {
	if or.Len() == 1 {
		return x.intersectionRange(or[0].start, or[0].end)
	}
	if x.Len() == 1 {
		return or.intersectionRange(x[0].start, x[0].end)
	}

	// Rename to x, y where len(x) > len(y)
	var y partitionRangeOR
	if or.Len() > x.Len() {
		x, y = or, x
	} else {
		y = or
	}

	// (a U b) M (c U d) => (x M c) U (x M d), x = (a U b)
	res := make(partitionRangeOR, 0, len(y))
	for _, r := range y {
		// As intersectionRange modify the raw data, we have to make a copy.
		tmp := make(partitionRangeOR, len(x))
		copy(tmp, x)
		tmp = tmp.intersectionRange(r.start, r.end)
		res = append(res, tmp...)
	}
	return res.simplify()
}

// intersectionRange calculate the intersection of [start, end) and [newStart, newEnd)
func intersectionRange(start, end, newStart, newEnd int) (int, int) {
	var s, e int
	if start > newStart {
		s = start
	} else {
		s = newStart
	}

	if end < newEnd {
		e = end
	} else {
		e = newEnd
	}
	return s, e
}

func (s *partitionProcessor) pruneRangePartition(ctx stochastikctx.Context, pi *perceptron.PartitionInfo, tbl causet.PartitionedBlock, conds []memex.Expression,
	columns []*memex.DeferredCauset, names types.NameSlice) (partitionRangeOR, error) {
	partExpr, err := tbl.(partitionBlock).PartitionExpr()
	if err != nil {
		return nil, err
	}

	// Partition by range columns.
	if len(pi.DeferredCausets) > 0 {
		return s.pruneRangeDeferredCausetsPartition(ctx, conds, pi, partExpr, columns, names)
	}

	// Partition by range.
	col, fn, mono, err := makePartitionByFnDefCaus(ctx, columns, names, pi.Expr)
	if err != nil {
		return nil, err
	}
	result := fullRange(len(pi.Definitions))
	if col == nil {
		return result, nil
	}

	// Extract the partition column, if the column is not null, it's possible to prune.
	pruner := rangePruner{
		lessThan: lessThanDataInt{
			data:     partExpr.ForRangePruning.LessThan,
			maxvalue: partExpr.ForRangePruning.MaxValue,
		},
		col:        col,
		partFn:     fn,
		monotonous: mono,
	}
	result = partitionRangeForCNFExpr(ctx, conds, &pruner, result)
	return result, nil
}

func (s *partitionProcessor) processRangePartition(ds *DataSource, pi *perceptron.PartitionInfo) (LogicalCauset, error) {
	used, err := s.pruneRangePartition(ds.ctx, pi, ds.causet.(causet.PartitionedBlock), ds.allConds, ds.TblDefCauss, ds.names)
	if err != nil {
		return nil, err
	}
	return s.makeUnionAllChildren(ds, pi, used)
}

// makePartitionByFnDefCaus extracts the column and function information in 'partition by ... fn(col)'.
func makePartitionByFnDefCaus(sctx stochastikctx.Context, columns []*memex.DeferredCauset, names types.NameSlice, partitionExpr string) (*memex.DeferredCauset, *memex.ScalarFunction, bool, error) {
	schemaReplicant := memex.NewSchema(columns...)
	tmp, err := memex.ParseSimpleExprsWithNames(sctx, partitionExpr, schemaReplicant, names)
	if err != nil {
		return nil, nil, false, err
	}
	partExpr := tmp[0]
	var col *memex.DeferredCauset
	var fn *memex.ScalarFunction
	var monotonous bool
	switch raw := partExpr.(type) {
	case *memex.ScalarFunction:
		// Special handle for floor(unix_timestamp(ts)) as partition memex.
		// This pattern is so common for timestamp(3) column as partition memex that it deserve an optimization.
		if raw.FuncName.L == ast.Floor {
			if ut, ok := raw.GetArgs()[0].(*memex.ScalarFunction); ok && ut.FuncName.L == ast.UnixTimestamp {
				args := ut.GetArgs()
				if len(args) == 1 {
					if c, ok1 := args[0].(*memex.DeferredCauset); ok1 {
						return c, raw, true, nil
					}
				}
			}
		}

		fn = raw
		args := fn.GetArgs()
		if len(args) > 0 {
			arg0 := args[0]
			if c, ok1 := arg0.(*memex.DeferredCauset); ok1 {
				col = c
			}
		}
		_, monotonous = monotoneIncFuncs[raw.FuncName.L]
	case *memex.DeferredCauset:
		col = raw
	}
	return col, fn, monotonous, nil
}

func partitionRangeForCNFExpr(sctx stochastikctx.Context, exprs []memex.Expression,
	pruner partitionRangePruner, result partitionRangeOR) partitionRangeOR {
	for i := 0; i < len(exprs); i++ {
		result = partitionRangeForExpr(sctx, exprs[i], pruner, result)
	}
	return result
}

// partitionRangeForExpr calculate the partitions for the memex.
func partitionRangeForExpr(sctx stochastikctx.Context, expr memex.Expression,
	pruner partitionRangePruner, result partitionRangeOR) partitionRangeOR {
	// Handle AND, OR respectively.
	if op, ok := expr.(*memex.ScalarFunction); ok {
		if op.FuncName.L == ast.LogicAnd {
			return partitionRangeForCNFExpr(sctx, op.GetArgs(), pruner, result)
		} else if op.FuncName.L == ast.LogicOr {
			args := op.GetArgs()
			newRange := partitionRangeForOrExpr(sctx, args[0], args[1], pruner)
			return result.intersection(newRange)
		} else if op.FuncName.L == ast.In {
			if p, ok := pruner.(*rangePruner); ok {
				newRange := partitionRangeForInExpr(sctx, op.GetArgs(), p)
				return result.intersection(newRange)
			}
			return result
		}
	}

	// Handle a single memex.
	start, end, ok := pruner.partitionRangeForExpr(sctx, expr)
	if !ok {
		// Can't prune, return the whole range.
		return result
	}
	return result.intersectionRange(start, end)
}

type partitionRangePruner interface {
	partitionRangeForExpr(stochastikctx.Context, memex.Expression) (start, end int, succ bool)
	fullRange() partitionRangeOR
}

var _ partitionRangePruner = &rangePruner{}

// rangePruner is used by 'partition by range'.
type rangePruner struct {
	lessThan lessThanDataInt
	col      *memex.DeferredCauset
	partFn   *memex.ScalarFunction
	// If partFn is not nil, monotonous indicates partFn is monotonous or not.
	monotonous bool
}

func (p *rangePruner) partitionRangeForExpr(sctx stochastikctx.Context, expr memex.Expression) (int, int, bool) {
	if constExpr, ok := expr.(*memex.Constant); ok {
		if b, err := constExpr.Value.ToBool(sctx.GetStochastikVars().StmtCtx); err == nil && b == 0 {
			// A constant false memex.
			return 0, 0, true
		}
	}

	dataForPrune, ok := p.extractDataForPrune(sctx, expr)
	if !ok {
		return 0, 0, false
	}

	unsigned := allegrosql.HasUnsignedFlag(p.col.RetType.Flag)
	start, end := pruneUseBinarySearch(p.lessThan, dataForPrune, unsigned)
	return start, end, true
}

func (p *rangePruner) fullRange() partitionRangeOR {
	return fullRange(p.lessThan.length())
}

// partitionRangeForOrExpr calculate the partitions for or(expr1, expr2)
func partitionRangeForOrExpr(sctx stochastikctx.Context, expr1, expr2 memex.Expression,
	pruner partitionRangePruner) partitionRangeOR {
	tmp1 := partitionRangeForExpr(sctx, expr1, pruner, pruner.fullRange())
	tmp2 := partitionRangeForExpr(sctx, expr2, pruner, pruner.fullRange())
	return tmp1.union(tmp2)
}

func partitionRangeForInExpr(sctx stochastikctx.Context, args []memex.Expression,
	pruner *rangePruner) partitionRangeOR {
	col, ok := args[0].(*memex.DeferredCauset)
	if !ok || col.ID != pruner.col.ID {
		return pruner.fullRange()
	}

	var result partitionRangeOR
	unsigned := allegrosql.HasUnsignedFlag(col.RetType.Flag)
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*memex.Constant)
		if !ok {
			return pruner.fullRange()
		}
		switch constExpr.Value.HoTT() {
		case types.HoTTInt64, types.HoTTUint64:
		case types.HoTTNull:
			result = append(result, partitionRange{0, 1})
			continue
		default:
			return pruner.fullRange()
		}
		val, err := constExpr.Value.ToInt64(sctx.GetStochastikVars().StmtCtx)
		if err != nil {
			return pruner.fullRange()
		}

		start, end := pruneUseBinarySearch(pruner.lessThan, dataForPrune{op: ast.EQ, c: val}, unsigned)
		result = append(result, partitionRange{start, end})
	}
	return result.simplify()
}

// monotoneIncFuncs are those functions that for any x y, if x > y => f(x) > f(y)
var monotoneIncFuncs = map[string]struct{}{
	ast.ToDays:        {},
	ast.UnixTimestamp: {},
}

// f(x) op const, op is > = <
type dataForPrune struct {
	op string
	c  int64
}

// extractDataForPrune extracts data from the memex for pruning.
// The memex should have this form:  'f(x) op const', otherwise it can't be pruned.
func (p *rangePruner) extractDataForPrune(sctx stochastikctx.Context, expr memex.Expression) (dataForPrune, bool) {
	var ret dataForPrune
	op, ok := expr.(*memex.ScalarFunction)
	if !ok {
		return ret, false
	}
	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE:
		ret.op = op.FuncName.L
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*memex.DeferredCauset); ok && arg0.ID == p.col.ID {
			ret.op = ast.IsNull
			return ret, true
		}
		return ret, false
	default:
		return ret, false
	}

	var col *memex.DeferredCauset
	var con *memex.Constant
	if arg0, ok := op.GetArgs()[0].(*memex.DeferredCauset); ok && arg0.ID == p.col.ID {
		if arg1, ok := op.GetArgs()[1].(*memex.Constant); ok {
			col, con = arg0, arg1
		}
	} else if arg0, ok := op.GetArgs()[1].(*memex.DeferredCauset); ok && arg0.ID == p.col.ID {
		if arg1, ok := op.GetArgs()[0].(*memex.Constant); ok {
			ret.op = opposite(ret.op)
			col, con = arg0, arg1
		}
	}
	if col == nil || con == nil {
		return ret, false
	}

	// Current memex is 'col op const'
	var constExpr memex.Expression
	if p.partFn != nil {
		// If the partition function is not monotone, only EQ condition can be pruning.
		if !p.monotonous && ret.op != ast.EQ {
			return ret, false
		}

		// If the partition memex is fn(col), change constExpr to fn(constExpr).
		constExpr = replaceDeferredCausetWithConst(p.partFn, con)

		// Sometimes we need to relax the condition, < to <=, > to >=.
		// For example, the following case doesn't hold:
		// col < '2020-02-11 17:34:11' => to_days(col) < to_days(2020-02-11 17:34:11)
		// The correct transform should be:
		// col < '2020-02-11 17:34:11' => to_days(col) <= to_days(2020-02-11 17:34:11)
		ret.op = relaxOP(ret.op)
	} else {
		// If the partition memex is col, use constExpr.
		constExpr = con
	}
	c, isNull, err := constExpr.EvalInt(sctx, chunk.Row{})
	if err == nil && !isNull {
		ret.c = c
		return ret, true
	}
	return ret, false
}

// replaceDeferredCausetWithConst change fn(col) to fn(const)
func replaceDeferredCausetWithConst(partFn *memex.ScalarFunction, con *memex.Constant) *memex.ScalarFunction {
	args := partFn.GetArgs()
	// The partition function may be floor(unix_timestamp(ts)) instead of a simple fn(col).
	if partFn.FuncName.L == ast.Floor {
		ut := args[0].(*memex.ScalarFunction)
		if ut.FuncName.L == ast.UnixTimestamp {
			args = ut.GetArgs()
			args[0] = con
			return partFn
		}
	}

	// No 'copy on write' for the memex here, this is a dangerous operation.
	args[0] = con
	return partFn

}

// opposite turns > to <, >= to <= and so on.
func opposite(op string) string {
	switch op {
	case ast.EQ:
		return ast.EQ
	case ast.LT:
		return ast.GT
	case ast.GT:
		return ast.LT
	case ast.LE:
		return ast.GE
	case ast.GE:
		return ast.LE
	}
	panic("invalid input parameter" + op)
}

// relaxOP relax the op > to >= and < to <=
// Sometime we need to relax the condition, for example:
// col < const => f(col) <= const
// datetime < 2020-02-11 16:18:42 => to_days(datetime) <= to_days(2020-02-11)
// We can't say:
// datetime < 2020-02-11 16:18:42 => to_days(datetime) < to_days(2020-02-11)
func relaxOP(op string) string {
	switch op {
	case ast.LT:
		return ast.LE
	case ast.GT:
		return ast.GE
	}
	return op
}

func pruneUseBinarySearch(lessThan lessThanDataInt, data dataForPrune, unsigned bool) (start int, end int) {
	length := lessThan.length()
	switch data.op {
	case ast.EQ:
		// col = 66, lessThan = [4 7 11 14 17] => [5, 6)
		// col = 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col = 10, lessThan = [4 7 11 14 17] => [2, 3)
		// col = 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = pos, pos+1
	case ast.LT:
		// col < 66, lessThan = [4 7 11 14 17] => [0, 5)
		// col < 14, lessThan = [4 7 11 14 17] => [0, 4)
		// col < 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col < 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) >= 0 })
		start, end = 0, pos+1
	case ast.GE:
		// col >= 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col >= 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col >= 10, lessThan = [4 7 11 14 17] => [2, 5)
		// col >= 3, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = pos, length
	case ast.GT:
		// col > 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col > 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col > 10, lessThan = [4 7 11 14 17] => [3, 5)
		// col > 3, lessThan = [4 7 11 14 17] => [1, 5)
		// col > 2, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c+1, unsigned) > 0 })
		start, end = pos, length
	case ast.LE:
		// col <= 66, lessThan = [4 7 11 14 17] => [0, 6)
		// col <= 14, lessThan = [4 7 11 14 17] => [0, 5)
		// col <= 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col <= 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.c, unsigned) > 0 })
		start, end = 0, pos+1
	case ast.IsNull:
		start, end = 0, 1
	default:
		start, end = 0, length
	}

	if end > length {
		end = length
	}
	return start, end
}

func (s *partitionProcessor) resolveAccessPaths(ds *DataSource) error {
	possiblePaths, err := getPossibleAccessPaths(
		ds.ctx, &blockHintInfo{indexMergeHintList: ds.indexMergeHints, indexHintList: ds.IndexHints},
		ds.astIndexHints, ds.causet, ds.DBName, ds.blockInfo.Name)
	if err != nil {
		return err
	}
	possiblePaths, err = filterPathByIsolationRead(ds.ctx, possiblePaths, ds.DBName)
	if err != nil {
		return err
	}
	ds.possibleAccessPaths = possiblePaths
	return nil
}

func (s *partitionProcessor) resolveOptimizeHint(ds *DataSource, partitionName perceptron.CIStr) error {
	// index hint
	if len(ds.IndexHints) > 0 {
		newIndexHint := make([]indexHintInfo, 0, len(ds.IndexHints))
		for _, idxHint := range ds.IndexHints {
			if len(idxHint.partitions) == 0 {
				newIndexHint = append(newIndexHint, idxHint)
			} else {
				for _, p := range idxHint.partitions {
					if p.String() == partitionName.String() {
						newIndexHint = append(newIndexHint, idxHint)
						break
					}
				}
			}
		}
		ds.IndexHints = newIndexHint
	}

	// index merge hint
	if len(ds.indexMergeHints) > 0 {
		newIndexMergeHint := make([]indexHintInfo, 0, len(ds.indexMergeHints))
		for _, idxHint := range ds.indexMergeHints {
			if len(idxHint.partitions) == 0 {
				newIndexMergeHint = append(newIndexMergeHint, idxHint)
			} else {
				for _, p := range idxHint.partitions {
					if p.String() == partitionName.String() {
						newIndexMergeHint = append(newIndexMergeHint, idxHint)
						break
					}
				}
			}
		}
		ds.indexMergeHints = newIndexMergeHint
	}

	// read from storage hint
	if ds.preferStoreType&preferEinsteinDB > 0 {
		if len(ds.preferPartitions[preferEinsteinDB]) > 0 {
			ds.preferStoreType ^= preferEinsteinDB
			for _, p := range ds.preferPartitions[preferEinsteinDB] {
				if p.String() == partitionName.String() {
					ds.preferStoreType |= preferEinsteinDB
				}
			}
		}
	}
	if ds.preferStoreType&preferTiFlash > 0 {
		if len(ds.preferPartitions[preferTiFlash]) > 0 {
			ds.preferStoreType ^= preferTiFlash
			for _, p := range ds.preferPartitions[preferTiFlash] {
				if p.String() == partitionName.String() {
					ds.preferStoreType |= preferTiFlash
				}
			}
		}
	}
	if ds.preferStoreType&preferTiFlash != 0 && ds.preferStoreType&preferEinsteinDB != 0 {
		ds.ctx.GetStochastikVars().StmtCtx.AppendWarning(
			errors.New("hint `read_from_storage` has conflict storage type for the partition " + partitionName.L))
	}

	return s.resolveAccessPaths(ds)
}

func checkBlockHintsApplicableForPartition(partitions []perceptron.CIStr, partitionSet set.StringSet) []string {
	var unknownPartitions []string
	for _, p := range partitions {
		if !partitionSet.Exist(p.L) {
			unknownPartitions = append(unknownPartitions, p.L)
		}
	}
	return unknownPartitions
}

func appendWarnForUnknownPartitions(ctx stochastikctx.Context, hintName string, unknownPartitions []string) {
	if len(unknownPartitions) == 0 {
		return
	}
	ctx.GetStochastikVars().StmtCtx.AppendWarning(
		errors.New(fmt.Sprintf("Unknown partitions (%s) in optimizer hint %s",
			strings.Join(unknownPartitions, ","), hintName)))
}

func (s *partitionProcessor) checkHintsApplicable(ds *DataSource, partitionSet set.StringSet) {
	for _, idxHint := range ds.IndexHints {
		unknownPartitions := checkBlockHintsApplicableForPartition(idxHint.partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.ctx, restore2IndexHint(idxHint.hintTypeString(), idxHint), unknownPartitions)
	}
	for _, idxMergeHint := range ds.indexMergeHints {
		unknownPartitions := checkBlockHintsApplicableForPartition(idxMergeHint.partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.ctx, restore2IndexHint(HintIndexMerge, idxMergeHint), unknownPartitions)
	}
	unknownPartitions := checkBlockHintsApplicableForPartition(ds.preferPartitions[preferEinsteinDB], partitionSet)
	unknownPartitions = append(unknownPartitions,
		checkBlockHintsApplicableForPartition(ds.preferPartitions[preferTiFlash], partitionSet)...)
	appendWarnForUnknownPartitions(ds.ctx, HintReadFromStorage, unknownPartitions)
}

func (s *partitionProcessor) makeUnionAllChildren(ds *DataSource, pi *perceptron.PartitionInfo, or partitionRangeOR) (LogicalCauset, error) {
	children := make([]LogicalCauset, 0, len(pi.Definitions))
	partitionNameSet := make(set.StringSet)
	for _, r := range or {
		for i := r.start; i < r.end; i++ {
			// This is for `causet partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
			if len(ds.partitionNames) != 0 {
				if !s.findByName(ds.partitionNames, pi.Definitions[i].Name.L) {
					continue
				}
			}
			// Not a deep copy.
			newDataSource := *ds
			newDataSource.baseLogicalCauset = newBaseLogicalCauset(ds.SCtx(), plancodec.TypeBlockScan, &newDataSource, ds.blockOffset)
			newDataSource.schemaReplicant = ds.schemaReplicant.Clone()
			newDataSource.isPartition = true
			newDataSource.physicalBlockID = pi.Definitions[i].ID

			// There are many memex nodes in the plan tree use the original datasource
			// id as FromID. So we set the id of the newDataSource with the original one to
			// avoid traversing the whole plan tree to uFIDelate the references.
			newDataSource.id = ds.id
			newDataSource.statisticBlock = getStatsBlock(ds.SCtx(), ds.causet.Meta(), pi.Definitions[i].ID)
			err := s.resolveOptimizeHint(&newDataSource, pi.Definitions[i].Name)
			partitionNameSet.Insert(pi.Definitions[i].Name.L)
			if err != nil {
				return nil, err
			}
			children = append(children, &newDataSource)
		}
	}
	s.checkHintsApplicable(ds, partitionNameSet)

	if len(children) == 0 {
		// No result after causet pruning.
		blockDual := LogicalBlockDual{RowCount: 0}.Init(ds.SCtx(), ds.blockOffset)
		blockDual.schemaReplicant = ds.Schema()
		return blockDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		return children[0], nil
	}
	unionAll := LogicalPartitionUnionAll{}.Init(ds.SCtx(), ds.blockOffset)
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.schemaReplicant.Clone())
	return unionAll, nil
}

func (s *partitionProcessor) pruneRangeDeferredCausetsPartition(ctx stochastikctx.Context, conds []memex.Expression, pi *perceptron.PartitionInfo, pe *blocks.PartitionExpr, columns []*memex.DeferredCauset, names types.NameSlice) (partitionRangeOR, error) {
	result := fullRange(len(pi.Definitions))

	if len(pi.DeferredCausets) != 1 {
		return result, nil
	}

	pruner, err := makeRangeDeferredCausetPruner(columns, names, pi, pe.ForRangeDeferredCausetsPruning)
	if err == nil {
		result = partitionRangeForCNFExpr(ctx, conds, pruner, result)
	}
	return result, nil
}

var _ partitionRangePruner = &rangeDeferredCausetsPruner{}

// rangeDeferredCausetsPruner is used by 'partition by range columns'.
type rangeDeferredCausetsPruner struct {
	data        []memex.Expression
	partDefCaus *memex.DeferredCauset
	maxvalue    bool
}

func makeRangeDeferredCausetPruner(columns []*memex.DeferredCauset, names types.NameSlice, pi *perceptron.PartitionInfo, from *blocks.ForRangeDeferredCausetsPruning) (*rangeDeferredCausetsPruner, error) {
	schemaReplicant := memex.NewSchema(columns...)
	idx := memex.FindFieldNameIdxByDefCausName(names, pi.DeferredCausets[0].L)
	partDefCaus := schemaReplicant.DeferredCausets[idx]
	data := make([]memex.Expression, len(from.LessThan))
	for i := 0; i < len(from.LessThan); i++ {
		if from.LessThan[i] != nil {
			data[i] = from.LessThan[i].Clone()
		}
	}
	return &rangeDeferredCausetsPruner{data, partDefCaus, from.MaxValue}, nil
}

func (p *rangeDeferredCausetsPruner) fullRange() partitionRangeOR {
	return fullRange(len(p.data))
}

func (p *rangeDeferredCausetsPruner) partitionRangeForExpr(sctx stochastikctx.Context, expr memex.Expression) (int, int, bool) {
	op, ok := expr.(*memex.ScalarFunction)
	if !ok {
		return 0, len(p.data), false
	}

	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE:
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*memex.DeferredCauset); ok && arg0.ID == p.partDefCaus.ID {
			return 0, 1, true
		}
		return 0, len(p.data), false
	default:
		return 0, len(p.data), false
	}
	opName := op.FuncName.L

	var col *memex.DeferredCauset
	var con *memex.Constant
	if arg0, ok := op.GetArgs()[0].(*memex.DeferredCauset); ok && arg0.ID == p.partDefCaus.ID {
		if arg1, ok := op.GetArgs()[1].(*memex.Constant); ok {
			col, con = arg0, arg1
		}
	} else if arg0, ok := op.GetArgs()[1].(*memex.DeferredCauset); ok && arg0.ID == p.partDefCaus.ID {
		if arg1, ok := op.GetArgs()[0].(*memex.Constant); ok {
			opName = opposite(opName)
			col, con = arg0, arg1
		}
	}
	if col == nil || con == nil {
		return 0, len(p.data), false
	}

	start, end := p.pruneUseBinarySearch(sctx, opName, con)
	return start, end, true
}

func (p *rangeDeferredCausetsPruner) pruneUseBinarySearch(sctx stochastikctx.Context, op string, data *memex.Constant) (start int, end int) {
	var err error
	var isNull bool
	compare := func(ith int, op string, v *memex.Constant) bool {
		if ith == len(p.data)-1 {
			if p.maxvalue {
				return true
			}
		}
		var expr memex.Expression
		expr, err = memex.NewFunction(sctx, op, types.NewFieldType(allegrosql.TypeLonglong), p.data[ith], v)
		var val int64
		val, isNull, err = expr.EvalInt(sctx, chunk.Row{})
		return val > 0
	}

	length := len(p.data)
	switch op {
	case ast.EQ:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = pos, pos+1
	case ast.LT:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GE, data) })
		start, end = 0, pos+1
	case ast.GE, ast.GT:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = pos, length
	case ast.LE:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = 0, pos+1
	default:
		start, end = 0, length
	}

	// Something goes wrong, abort this prunning.
	if err != nil || isNull {
		return 0, len(p.data)
	}

	if end > length {
		end = length
	}
	return start, end
}
