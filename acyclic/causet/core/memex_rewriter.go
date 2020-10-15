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

package core

import (
	"context"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/opcode"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/types"
	driver "github.com/whtcorpsinc/milevadb/types/BerolinaSQL_driver"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
)

// EvalSubqueryFirstRow evaluates incorrelated subqueries once, and get first event.
var EvalSubqueryFirstRow func(ctx context.Context, p PhysicalCauset, is schemareplicant.SchemaReplicant, sctx stochastikctx.Context) (event []types.Causet, err error)

// evalAstExpr evaluates ast memex directly.
func evalAstExpr(sctx stochastikctx.Context, expr ast.ExprNode) (types.Causet, error) {
	if val, ok := expr.(*driver.ValueExpr); ok {
		return val.Causet, nil
	}
	newExpr, err := rewriteAstExpr(sctx, expr, nil, nil)
	if err != nil {
		return types.Causet{}, err
	}
	return newExpr.Eval(chunk.Row{})
}

// rewriteAstExpr rewrites ast memex directly.
func rewriteAstExpr(sctx stochastikctx.Context, expr ast.ExprNode, schemaReplicant *memex.Schema, names types.NameSlice) (memex.Expression, error) {
	var is schemareplicant.SchemaReplicant
	if sctx.GetStochastikVars().TxnCtx.SchemaReplicant != nil {
		is = sctx.GetStochastikVars().TxnCtx.SchemaReplicant.(schemareplicant.SchemaReplicant)
	}
	b := NewCausetBuilder(sctx, is, &hint.BlockHintProcessor{})
	fakeCauset := LogicalBlockDual{}.Init(sctx, 0)
	if schemaReplicant != nil {
		fakeCauset.schemaReplicant = schemaReplicant
		fakeCauset.names = names
	}
	newExpr, _, err := b.rewrite(context.TODO(), expr, fakeCauset, nil, true)
	if err != nil {
		return nil, err
	}
	return newExpr, nil
}

func (b *CausetBuilder) rewriteInsertOnDuplicateUFIDelate(ctx context.Context, exprNode ast.ExprNode, mockCauset LogicalCauset, insertCauset *Insert) (memex.Expression, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	rewriter := b.getExpressionRewriter(ctx, mockCauset)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		return nil, rewriter.err
	}

	rewriter.insertCauset = insertCauset
	rewriter.asScalar = true

	expr, _, err := b.rewriteExprNode(rewriter, exprNode, true)
	return expr, err
}

// rewrite function rewrites ast expr to memex.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schemaReplicant.
// asScalar means whether this memex must be treated as a scalar memex.
// And this function returns a result memex, a new plan that may have apply or semi-join.
func (b *CausetBuilder) rewrite(ctx context.Context, exprNode ast.ExprNode, p LogicalCauset, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (memex.Expression, LogicalCauset, error) {
	expr, resultCauset, err := b.rewriteWithPreprocess(ctx, exprNode, p, aggMapper, nil, asScalar, nil)
	return expr, resultCauset, err
}

// rewriteWithPreprocess is for handling the situation that we need to adjust the input ast tree
// before really using its node in `memexRewriter.Leave`. In that case, we first call
// er.preprocess(expr), which returns a new expr. Then we use the new expr in `Leave`.
func (b *CausetBuilder) rewriteWithPreprocess(
	ctx context.Context,
	exprNode ast.ExprNode,
	p LogicalCauset, aggMapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int,
	asScalar bool,
	preprocess func(ast.Node) ast.Node,
) (memex.Expression, LogicalCauset, error) {
	b.rewriterCounter++
	defer func() { b.rewriterCounter-- }()

	rewriter := b.getExpressionRewriter(ctx, p)
	// The rewriter maybe is obtained from "b.rewriterPool", "rewriter.err" is
	// not nil means certain previous procedure has not handled this error.
	// Here we give us one more chance to make a correct behavior by handling
	// this missed error.
	if rewriter.err != nil {
		return nil, nil, rewriter.err
	}

	rewriter.aggrMap = aggMapper
	rewriter.windowMap = windowMapper
	rewriter.asScalar = asScalar
	rewriter.preprocess = preprocess

	expr, resultCauset, err := b.rewriteExprNode(rewriter, exprNode, asScalar)
	return expr, resultCauset, err
}

func (b *CausetBuilder) getExpressionRewriter(ctx context.Context, p LogicalCauset) (rewriter *memexRewriter) {
	defer func() {
		if p != nil {
			rewriter.schemaReplicant = p.Schema()
			rewriter.names = p.OutputNames()
		}
	}()

	if len(b.rewriterPool) < b.rewriterCounter {
		rewriter = &memexRewriter{p: p, b: b, sctx: b.ctx, ctx: ctx}
		b.rewriterPool = append(b.rewriterPool, rewriter)
		return
	}

	rewriter = b.rewriterPool[b.rewriterCounter-1]
	rewriter.p = p
	rewriter.asScalar = false
	rewriter.aggrMap = nil
	rewriter.preprocess = nil
	rewriter.insertCauset = nil
	rewriter.disableFoldCounter = 0
	rewriter.tryFoldCounter = 0
	rewriter.ctxStack = rewriter.ctxStack[:0]
	rewriter.ctxNameStk = rewriter.ctxNameStk[:0]
	rewriter.ctx = ctx
	return
}

func (b *CausetBuilder) rewriteExprNode(rewriter *memexRewriter, exprNode ast.ExprNode, asScalar bool) (memex.Expression, LogicalCauset, error) {
	if rewriter.p != nil {
		curDefCausLen := rewriter.p.Schema().Len()
		defer func() {
			names := rewriter.p.OutputNames().Shallow()[:curDefCausLen]
			for i := curDefCausLen; i < rewriter.p.Schema().Len(); i++ {
				names = append(names, types.EmptyName)
			}
			// After rewriting finished, only old columns are visible.
			// e.g. select * from t where t.a in (select t1.a from t1);
			// The output columns before we enter the subquery are the columns from t.
			// But when we leave the subquery `t.a in (select t1.a from t1)`, we got a Apply operator
			// and the output columns become [t.*, t1.*]. But t1.* is used only inside the subquery. If there's another filter
			// which is also a subquery where t1 is involved. The name resolving will fail if we still expose the column from
			// the previous subquery.
			// So here we just reset the names to empty to avoid this situation.
			// TODO: implement ScalarSubQuery and resolve it during optimizing. In building phase, we will not change the plan's structure.
			rewriter.p.SetOutputNames(names)
		}()
	}
	exprNode.Accept(rewriter)
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	if !asScalar && len(rewriter.ctxStack) == 0 {
		return nil, rewriter.p, nil
	}
	if len(rewriter.ctxStack) != 1 {
		return nil, nil, errors.Errorf("context len %v is invalid", len(rewriter.ctxStack))
	}
	rewriter.err = memex.CheckArgsNotMultiDeferredCausetRow(rewriter.ctxStack[0])
	if rewriter.err != nil {
		return nil, nil, errors.Trace(rewriter.err)
	}
	return rewriter.ctxStack[0], rewriter.p, nil
}

type memexRewriter struct {
	ctxStack   []memex.Expression
	ctxNameStk []*types.FieldName
	p          LogicalCauset
	schemaReplicant     *memex.Schema
	names      []*types.FieldName
	err        error
	aggrMap    map[*ast.AggregateFuncExpr]int
	windowMap  map[*ast.WindowFuncExpr]int
	b          *CausetBuilder
	sctx       stochastikctx.Context
	ctx        context.Context

	// asScalar indicates the return value must be a scalar value.
	// NOTE: This value can be changed during memex rewritten.
	asScalar bool

	// preprocess is called for every ast.Node in Leave.
	preprocess func(ast.Node) ast.Node

	// insertCauset is only used to rewrite the memexs inside the assignment
	// of the "INSERT" memex.
	insertCauset *Insert

	// disableFoldCounter controls fold-disabled scope. If > 0, rewriter will NOT do constant folding.
	// Typically, during visiting AST, while entering the scope(disable), the counter will +1; while
	// leaving the scope(enable again), the counter will -1.
	// NOTE: This value can be changed during memex rewritten.
	disableFoldCounter int
	tryFoldCounter     int
}

func (er *memexRewriter) ctxStackLen() int {
	return len(er.ctxStack)
}

func (er *memexRewriter) ctxStackPop(num int) {
	l := er.ctxStackLen()
	er.ctxStack = er.ctxStack[:l-num]
	er.ctxNameStk = er.ctxNameStk[:l-num]
}

func (er *memexRewriter) ctxStackAppend(col memex.Expression, name *types.FieldName) {
	er.ctxStack = append(er.ctxStack, col)
	er.ctxNameStk = append(er.ctxNameStk, name)
}

// constructBinaryOpFunction converts binary operator functions
// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. Else constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
// 		IF ( isNull(a0 NE b0), Null,
// 			IF ( a1 NE b1, a1 op b1,
// 				IF ( isNull(a1 NE b1), Null, a2 op b2))))`
func (er *memexRewriter) constructBinaryOpFunction(l memex.Expression, r memex.Expression, op string) (memex.Expression, error) {
	lLen, rLen := memex.GetRowLen(l), memex.GetRowLen(r)
	if lLen == 1 && rLen == 1 {
		return er.newFunction(op, types.NewFieldType(allegrosql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, memex.ErrOperandDeferredCausets.GenWithStackByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]memex.Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = er.constructBinaryOpFunction(memex.GetFuncArg(l, i), memex.GetFuncArg(r, i), op)
			if err != nil {
				return nil, err
			}
		}
		if op == ast.NE {
			return memex.ComposeDNFCondition(er.sctx, funcs...), nil
		}
		return memex.ComposeCNFCondition(er.sctx, funcs...), nil
	default:
		larg0, rarg0 := memex.GetFuncArg(l, 0), memex.GetFuncArg(r, 0)
		var expr1, expr2, expr3, expr4, expr5 memex.Expression
		expr1 = memex.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), larg0, rarg0)
		expr2 = memex.NewFunctionInternal(er.sctx, op, types.NewFieldType(allegrosql.TypeTiny), larg0, rarg0)
		expr3 = memex.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), expr1)
		var err error
		l, err = memex.PopRowFirstArg(er.sctx, l)
		if err != nil {
			return nil, err
		}
		r, err = memex.PopRowFirstArg(er.sctx, r)
		if err != nil {
			return nil, err
		}
		expr4, err = er.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, err
		}
		expr5, err = er.newFunction(ast.If, types.NewFieldType(allegrosql.TypeTiny), expr3, memex.NewNull(), expr4)
		if err != nil {
			return nil, err
		}
		return er.newFunction(ast.If, types.NewFieldType(allegrosql.TypeTiny), expr1, expr2, expr5)
	}
}

func (er *memexRewriter) buildSubquery(ctx context.Context, subq *ast.SubqueryExpr) (LogicalCauset, error) {
	if er.schemaReplicant != nil {
		outerSchema := er.schemaReplicant.Clone()
		er.b.outerSchemas = append(er.b.outerSchemas, outerSchema)
		er.b.outerNames = append(er.b.outerNames, er.names)
		defer func() {
			er.b.outerSchemas = er.b.outerSchemas[0 : len(er.b.outerSchemas)-1]
			er.b.outerNames = er.b.outerNames[0 : len(er.b.outerNames)-1]
		}()
	}

	np, err := er.b.buildResultSetNode(ctx, subq.Query)
	if err != nil {
		return nil, err
	}
	// Pop the handle map generated by the subquery.
	er.b.handleHelper.popMap()
	return np, nil
}

// Enter implements Visitor interface.
func (er *memexRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		index, ok := -1, false
		if er.aggrMap != nil {
			index, ok = er.aggrMap[v]
		}
		if !ok {
			er.err = ErrInvalidGroupFuncUse
			return inNode, true
		}
		er.ctxStackAppend(er.schemaReplicant.DeferredCausets[index], er.names[index])
		return inNode, true
	case *ast.DeferredCausetNameExpr:
		if index, ok := er.b.colMapper[v]; ok {
			er.ctxStackAppend(er.schemaReplicant.DeferredCausets[index], er.names[index])
			return inNode, true
		}
	case *ast.CompareSubqueryExpr:
		return er.handleCompareSubquery(er.ctx, v)
	case *ast.ExistsSubqueryExpr:
		return er.handleExistSubquery(er.ctx, v)
	case *ast.PatternInExpr:
		if v.Sel != nil {
			return er.handleInSubquery(er.ctx, v)
		}
		if len(v.List) != 1 {
			break
		}
		// For 10 in ((select * from t)), the BerolinaSQL won't set v.Sel.
		// So we must process this case here.
		x := v.List[0]
		for {
			switch y := x.(type) {
			case *ast.SubqueryExpr:
				v.Sel = y
				return er.handleInSubquery(er.ctx, v)
			case *ast.ParenthesesExpr:
				x = y.Expr
			default:
				return inNode, false
			}
		}
	case *ast.SubqueryExpr:
		return er.handleScalarSubquery(er.ctx, v)
	case *ast.ParenthesesExpr:
	case *ast.ValuesExpr:
		schemaReplicant, names := er.schemaReplicant, er.names
		// NOTE: "er.insertCauset != nil" means that we are rewriting the
		// memexs inside the assignment of "INSERT" memex. we have to
		// use the "blockSchema" of that "insertCauset".
		if er.insertCauset != nil {
			schemaReplicant = er.insertCauset.blockSchema
			names = er.insertCauset.blockDefCausNames
		}
		idx, err := memex.FindFieldName(names, v.DeferredCauset.Name)
		if err != nil {
			er.err = err
			return inNode, false
		}
		if idx < 0 {
			er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.DeferredCauset.Name.OrigDefCausName(), "field list")
			return inNode, false
		}
		col := schemaReplicant.DeferredCausets[idx]
		er.ctxStackAppend(memex.NewValuesFunc(er.sctx, col.Index, col.RetType), types.EmptyName)
		return inNode, true
	case *ast.WindowFuncExpr:
		index, ok := -1, false
		if er.windowMap != nil {
			index, ok = er.windowMap[v]
		}
		if !ok {
			er.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.F))
			return inNode, true
		}
		er.ctxStackAppend(er.schemaReplicant.DeferredCausets[index], er.names[index])
		return inNode, true
	case *ast.FuncCallExpr:
		if _, ok := memex.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter++
		}
		if _, ok := memex.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter++
		}
	case *ast.CaseExpr:
		if _, ok := memex.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter++
		}
		if _, ok := memex.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter++
		}
	case *ast.SetDefCauslationExpr:
		// Do nothing
	default:
		er.asScalar = true
	}
	return inNode, false
}

func (er *memexRewriter) buildSemiApplyFromEqualSubq(np LogicalCauset, l, r memex.Expression, not bool) {
	var condition memex.Expression
	if rDefCaus, ok := r.(*memex.DeferredCauset); ok && (er.asScalar || not) {
		// If both input columns of `!= all / = any` memex are not null, we can treat the memex
		// as normal column equal condition.
		if lDefCaus, ok := l.(*memex.DeferredCauset); !ok || !allegrosql.HasNotNullFlag(lDefCaus.GetType().Flag) || !allegrosql.HasNotNullFlag(rDefCaus.GetType().Flag) {
			rDefCausCopy := *rDefCaus
			rDefCausCopy.InOperand = true
			r = &rDefCausCopy
		}
	}
	condition, er.err = er.constructBinaryOpFunction(l, r, ast.EQ)
	if er.err != nil {
		return
	}
	er.p, er.err = er.b.buildSemiApply(er.p, np, []memex.Expression{condition}, er.asScalar, not)
}

func (er *memexRewriter) handleCompareSubquery(ctx context.Context, v *ast.CompareSubqueryExpr) (ast.Node, bool) {
	v.L.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.R.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.R)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	// Only (a,b,c) = any (...) and (a,b,c) != all (...) can use event memex.
	canMultiDefCaus := (!v.All && v.Op == opcode.EQ) || (v.All && v.Op == opcode.NE)
	if !canMultiDefCaus && (memex.GetRowLen(lexpr) != 1 || np.Schema().Len() != 1) {
		er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return v, true
	}
	lLen := memex.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(lLen)
		return v, true
	}
	var rexpr memex.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().DeferredCausets[0]
	} else {
		args := make([]memex.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().DeferredCausets {
			args = append(args, col)
		}
		rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			return v, true
		}
	}
	switch v.Op {
	// Only EQ, NE and NullEQ can be composed with and.
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		if v.Op == opcode.EQ {
			if v.All {
				er.handleEQAll(lexpr, rexpr, np)
			} else {
				// `a = any(subq)` will be rewriten as `a in (subq)`.
				er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, false)
				if er.err != nil {
					return v, true
				}
			}
		} else if v.Op == opcode.NE {
			if v.All {
				// `a != all(subq)` will be rewriten as `a not in (subq)`.
				er.buildSemiApplyFromEqualSubq(np, lexpr, rexpr, true)
				if er.err != nil {
					return v, true
				}
			} else {
				er.handleNEAny(lexpr, rexpr, np)
			}
		} else {
			// TODO: Support this in future.
			er.err = errors.New("We don't support <=> all or <=> any now")
			return v, true
		}
	default:
		// When < all or > any , the agg function should use min.
		useMin := ((v.Op == opcode.LT || v.Op == opcode.LE) && v.All) || ((v.Op == opcode.GT || v.Op == opcode.GE) && !v.All)
		er.handleOtherComparableSubq(lexpr, rexpr, np, useMin, v.Op.String(), v.All)
	}
	if er.asScalar {
		// The parent memex only use the last column in schemaReplicant, which represents whether the condition is matched.
		er.ctxStack[len(er.ctxStack)-1] = er.p.Schema().DeferredCausets[er.p.Schema().Len()-1]
		er.ctxNameStk[len(er.ctxNameStk)-1] = er.p.OutputNames()[er.p.Schema().Len()-1]
	}
	return v, true
}

// handleOtherComparableSubq handles the queries like < any, < max, etc. For example, if the query is t.id < any (select s.id from s),
// it will be rewrote to t.id < (select max(s.id) from s).
func (er *memexRewriter) handleOtherComparableSubq(lexpr, rexpr memex.Expression, np LogicalCauset, useMin bool, cmpFunc string, all bool) {
	plan4Agg := LogicalAggregation{}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)

	// Create a "max" or "min" aggregation.
	funcName := ast.AggFuncMax
	if useMin {
		funcName = ast.AggFuncMin
	}
	funcMaxOrMin, err := aggregation.NewAggFuncDesc(er.sctx, funcName, []memex.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}

	// Create a column and append it to the schemaReplicant of that aggregation.
	colMaxOrMin := &memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  funcMaxOrMin.RetTp,
	}
	schemaReplicant := memex.NewSchema(colMaxOrMin)

	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	plan4Agg.SetSchema(schemaReplicant)
	plan4Agg.AggFuncs = []*aggregation.AggFuncDesc{funcMaxOrMin}

	cond := memex.NewFunctionInternal(er.sctx, cmpFunc, types.NewFieldType(allegrosql.TypeTiny), lexpr, colMaxOrMin)
	er.buildQuantifierCauset(plan4Agg, cond, lexpr, rexpr, all)
}

// buildQuantifierCauset adds extra condition for any / all subquery.
func (er *memexRewriter) buildQuantifierCauset(plan4Agg *LogicalAggregation, cond, lexpr, rexpr memex.Expression, all bool) {
	innerIsNull := memex.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), rexpr)
	outerIsNull := memex.NewFunctionInternal(er.sctx, ast.IsNull, types.NewFieldType(allegrosql.TypeTiny), lexpr)

	funcSum, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncSum, []memex.Expression{innerIsNull}, false)
	if err != nil {
		er.err = err
		return
	}
	colSum := &memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  funcSum.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcSum)
	plan4Agg.schemaReplicant.Append(colSum)
	innerHasNull := memex.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), colSum, memex.NewZero())

	// Build `count(1)` aggregation to check if subquery is empty.
	funcCount, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []memex.Expression{memex.NewOne()}, false)
	if err != nil {
		er.err = err
		return
	}
	colCount := &memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  funcCount.RetTp,
	}
	plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, funcCount)
	plan4Agg.schemaReplicant.Append(colCount)

	if all {
		// All of the inner record set should not contain null value. So for t.id < all(select s.id from s), it
		// should be rewrote to t.id < min(s.id) and if(sum(s.id is null) != 0, null, true).
		innerNullChecker := memex.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), innerHasNull, memex.NewNull(), memex.NewOne())
		cond = memex.ComposeCNFCondition(er.sctx, cond, innerNullChecker)
		// If the subquery is empty, it should always return true.
		emptyChecker := memex.NewFunctionInternal(er.sctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), colCount, memex.NewZero())
		// If outer key is null, and subquery is not empty, it should always return null, even when it is `null = all (1, 2)`.
		outerNullChecker := memex.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), outerIsNull, memex.NewNull(), memex.NewZero())
		cond = memex.ComposeDNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	} else {
		// For "any" memex, if the subquery has null and the cond returns false, the result should be NULL.
		// Specifically, `t.id < any (select s.id from s)` would be rewrote to `t.id < max(s.id) or if(sum(s.id is null) != 0, null, false)`
		innerNullChecker := memex.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), innerHasNull, memex.NewNull(), memex.NewZero())
		cond = memex.ComposeDNFCondition(er.sctx, cond, innerNullChecker)
		// If the subquery is empty, it should always return false.
		emptyChecker := memex.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), colCount, memex.NewZero())
		// If outer key is null, and subquery is not empty, it should return null.
		outerNullChecker := memex.NewFunctionInternal(er.sctx, ast.If, types.NewFieldType(allegrosql.TypeTiny), outerIsNull, memex.NewNull(), memex.NewOne())
		cond = memex.ComposeCNFCondition(er.sctx, cond, emptyChecker, outerNullChecker)
	}

	// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
	// plan4Agg.buildProjectionIfNecessary()
	if !er.asScalar {
		// For Semi LogicalApply without aux column, the result is no matter false or null. So we can add it to join predicate.
		er.p, er.err = er.b.buildSemiApply(er.p, plan4Agg, []memex.Expression{cond}, false, false)
		return
	}
	// If we treat the result as a scalar value, we will add a projection with a extra column to output true, false or null.
	outerSchemaLen := er.p.Schema().Len()
	er.p = er.b.buildApplyWithJoinType(er.p, plan4Agg, InnerJoin)
	joinSchema := er.p.Schema()
	proj := LogicalProjection{
		Exprs: memex.DeferredCauset2Exprs(joinSchema.Clone().DeferredCausets[:outerSchemaLen]),
	}.Init(er.sctx, er.b.getSelectOffset())
	proj.names = make([]*types.FieldName, outerSchemaLen, outerSchemaLen+1)
	copy(proj.names, er.p.OutputNames())
	proj.SetSchema(memex.NewSchema(joinSchema.Clone().DeferredCausets[:outerSchemaLen]...))
	proj.Exprs = append(proj.Exprs, cond)
	proj.schemaReplicant.Append(&memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  cond.GetType(),
	})
	proj.names = append(proj.names, types.EmptyName)
	proj.SetChildren(er.p)
	er.p = proj
}

// handleNEAny handles the case of != any. For example, if the query is t.id != any (select s.id from s), it will be rewrote to
// t.id != s.id or count(distinct s.id) > 1 or [any checker]. If there are two different values in s.id ,
// there must exist a s.id that doesn't equal to t.id.
func (er *memexRewriter) handleNEAny(lexpr, rexpr memex.Expression, np LogicalCauset) {
	// If there is NULL in s.id column, s.id should be the value that isn't null in condition t.id != s.id.
	// So use function max to filter NULL.
	maxFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncMax, []memex.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}
	countFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []memex.Expression{rexpr}, true)
	if err != nil {
		er.err = err
		return
	}
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{maxFunc, countFunc},
	}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)
	maxResultDefCaus := &memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  maxFunc.RetTp,
	}
	count := &memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.names = append(plan4Agg.names, types.EmptyName, types.EmptyName)
	plan4Agg.SetSchema(memex.NewSchema(maxResultDefCaus, count))
	gtFunc := memex.NewFunctionInternal(er.sctx, ast.GT, types.NewFieldType(allegrosql.TypeTiny), count, memex.NewOne())
	neCond := memex.NewFunctionInternal(er.sctx, ast.NE, types.NewFieldType(allegrosql.TypeTiny), lexpr, maxResultDefCaus)
	cond := memex.ComposeDNFCondition(er.sctx, gtFunc, neCond)
	er.buildQuantifierCauset(plan4Agg, cond, lexpr, rexpr, false)
}

// handleEQAll handles the case of = all. For example, if the query is t.id = all (select s.id from s), it will be rewrote to
// t.id = (select s.id from s having count(distinct s.id) <= 1 and [all checker]).
func (er *memexRewriter) handleEQAll(lexpr, rexpr memex.Expression, np LogicalCauset) {
	firstRowFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncFirstRow, []memex.Expression{rexpr}, false)
	if err != nil {
		er.err = err
		return
	}
	countFunc, err := aggregation.NewAggFuncDesc(er.sctx, ast.AggFuncCount, []memex.Expression{rexpr}, true)
	if err != nil {
		er.err = err
		return
	}
	plan4Agg := LogicalAggregation{
		AggFuncs: []*aggregation.AggFuncDesc{firstRowFunc, countFunc},
	}.Init(er.sctx, er.b.getSelectOffset())
	if hint := er.b.BlockHints(); hint != nil {
		plan4Agg.aggHints = hint.aggHints
	}
	plan4Agg.SetChildren(np)
	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	firstRowResultDefCaus := &memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  firstRowFunc.RetTp,
	}
	plan4Agg.names = append(plan4Agg.names, types.EmptyName)
	count := &memex.DeferredCauset{
		UniqueID: er.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
		RetType:  countFunc.RetTp,
	}
	plan4Agg.SetSchema(memex.NewSchema(firstRowResultDefCaus, count))
	leFunc := memex.NewFunctionInternal(er.sctx, ast.LE, types.NewFieldType(allegrosql.TypeTiny), count, memex.NewOne())
	eqCond := memex.NewFunctionInternal(er.sctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), lexpr, firstRowResultDefCaus)
	cond := memex.ComposeCNFCondition(er.sctx, leFunc, eqCond)
	er.buildQuantifierCauset(plan4Agg, cond, lexpr, rexpr, true)
}

func (er *memexRewriter) handleExistSubquery(ctx context.Context, v *ast.ExistsSubqueryExpr) (ast.Node, bool) {
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	np = er.popExistsSubCauset(np)
	if len(ExtractCorrelatedDefCauss4LogicalCauset(np)) > 0 {
		er.p, er.err = er.b.buildSemiApply(er.p, np, nil, er.asScalar, v.Not)
		if er.err != nil || !er.asScalar {
			return v, true
		}
		er.ctxStackAppend(er.p.Schema().DeferredCausets[er.p.Schema().Len()-1], er.p.OutputNames()[er.p.Schema().Len()-1])
	} else {
		// We don't want nth_plan hint to affect separately executed subqueries here, so disable nth_plan temporarily.
		NthCausetBackup := er.sctx.GetStochastikVars().StmtCtx.StmtHints.ForceNthCauset
		er.sctx.GetStochastikVars().StmtCtx.StmtHints.ForceNthCauset = -1
		physicalCauset, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
		er.sctx.GetStochastikVars().StmtCtx.StmtHints.ForceNthCauset = NthCausetBackup
		if err != nil {
			er.err = err
			return v, true
		}
		event, err := EvalSubqueryFirstRow(ctx, physicalCauset, er.b.is, er.b.ctx)
		if err != nil {
			er.err = err
			return v, true
		}
		if (event != nil && !v.Not) || (event == nil && v.Not) {
			er.ctxStackAppend(memex.NewOne(), types.EmptyName)
		} else {
			er.ctxStackAppend(memex.NewZero(), types.EmptyName)
		}
	}
	return v, true
}

// popExistsSubCauset will remove the useless plan in exist's child.
// See comments inside the method for more details.
func (er *memexRewriter) popExistsSubCauset(p LogicalCauset) LogicalCauset {
out:
	for {
		switch plan := p.(type) {
		// This can be removed when in exists clause,
		// e.g. exists(select count(*) from t order by a) is equal to exists t.
		case *LogicalProjection, *LogicalSort:
			p = p.Children()[0]
		case *LogicalAggregation:
			if len(plan.GroupByItems) == 0 {
				p = LogicalBlockDual{RowCount: 1}.Init(er.sctx, er.b.getSelectOffset())
				break out
			}
			p = p.Children()[0]
		default:
			break out
		}
	}
	return p
}

func (er *memexRewriter) handleInSubquery(ctx context.Context, v *ast.PatternInExpr) (ast.Node, bool) {
	asScalar := er.asScalar
	er.asScalar = true
	v.Expr.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.Sel)
		return v, true
	}
	np, err := er.buildSubquery(ctx, subq)
	if err != nil {
		er.err = err
		return v, true
	}
	lLen := memex.GetRowLen(lexpr)
	if lLen != np.Schema().Len() {
		er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(lLen)
		return v, true
	}
	var rexpr memex.Expression
	if np.Schema().Len() == 1 {
		rexpr = np.Schema().DeferredCausets[0]
		rDefCaus := rexpr.(*memex.DeferredCauset)
		// For AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemiJoin, we cannot treat `in` memex as
		// normal column equal condition, so we specially mark the inner operand here.
		if v.Not || asScalar {
			// If both input columns of `in` memex are not null, we can treat the memex
			// as normal column equal condition instead.
			if !allegrosql.HasNotNullFlag(lexpr.GetType().Flag) || !allegrosql.HasNotNullFlag(rDefCaus.GetType().Flag) {
				rDefCausCopy := *rDefCaus
				rDefCausCopy.InOperand = true
				rexpr = &rDefCausCopy
			}
		}
	} else {
		args := make([]memex.Expression, 0, np.Schema().Len())
		for _, col := range np.Schema().DeferredCausets {
			args = append(args, col)
		}
		rexpr, er.err = er.newFunction(ast.RowFunc, args[0].GetType(), args...)
		if er.err != nil {
			return v, true
		}
	}
	checkCondition, err := er.constructBinaryOpFunction(lexpr, rexpr, ast.EQ)
	if err != nil {
		er.err = err
		return v, true
	}

	// If the leftKey and the rightKey have different collations, don't convert the sub-query to an inner-join
	// since when converting we will add a distinct-agg upon the right child and this distinct-agg doesn't have the right collation.
	// To keep it simple, we forbid this converting if they have different collations.
	lt, rt := lexpr.GetType(), rexpr.GetType()
	collFlag := collate.CompatibleDefCauslate(lt.DefCauslate, rt.DefCauslate)

	// If it's not the form of `not in (SUBQUERY)`,
	// and has no correlated column from the current level plan(if the correlated column is from upper level,
	// we can treat it as constant, because the upper LogicalApply cannot be eliminated since current node is a join node),
	// and don't need to append a scalar value, we can rewrite it to inner join.
	if er.sctx.GetStochastikVars().GetAllowInSubqToJoinAnPosetDagg() && !v.Not && !asScalar && len(extractCorDeferredCausetsBySchema4LogicalCauset(np, er.p.Schema())) == 0 && collFlag {
		// We need to try to eliminate the agg and the projection produced by this operation.
		er.b.optFlag |= flagEliminateAgg
		er.b.optFlag |= flagEliminateProjection
		er.b.optFlag |= flagJoinReOrder
		// Build distinct for the inner query.
		agg, err := er.b.buildDistinct(np, np.Schema().Len())
		if err != nil {
			er.err = err
			return v, true
		}
		// Build inner join above the aggregation.
		join := LogicalJoin{JoinType: InnerJoin}.Init(er.sctx, er.b.getSelectOffset())
		join.SetChildren(er.p, agg)
		join.SetSchema(memex.MergeSchema(er.p.Schema(), agg.schemaReplicant))
		join.names = make([]*types.FieldName, er.p.Schema().Len()+agg.Schema().Len())
		copy(join.names, er.p.OutputNames())
		copy(join.names[er.p.Schema().Len():], agg.OutputNames())
		join.AttachOnConds(memex.SplitCNFItems(checkCondition))
		// Set join hint for this join.
		if er.b.BlockHints() != nil {
			join.setPreferredJoinType(er.b.BlockHints())
		}
		er.p = join
	} else {
		er.p, er.err = er.b.buildSemiApply(er.p, np, memex.SplitCNFItems(checkCondition), asScalar, v.Not)
		if er.err != nil {
			return v, true
		}
	}

	er.ctxStackPop(1)
	if asScalar {
		col := er.p.Schema().DeferredCausets[er.p.Schema().Len()-1]
		er.ctxStackAppend(col, er.p.OutputNames()[er.p.Schema().Len()-1])
	}
	return v, true
}

func (er *memexRewriter) handleScalarSubquery(ctx context.Context, v *ast.SubqueryExpr) (ast.Node, bool) {
	np, err := er.buildSubquery(ctx, v)
	if err != nil {
		er.err = err
		return v, true
	}
	np = er.b.buildMaxOneRow(np)
	if len(ExtractCorrelatedDefCauss4LogicalCauset(np)) > 0 {
		er.p = er.b.buildApplyWithJoinType(er.p, np, LeftOuterJoin)
		if np.Schema().Len() > 1 {
			newDefCauss := make([]memex.Expression, 0, np.Schema().Len())
			for _, col := range np.Schema().DeferredCausets {
				newDefCauss = append(newDefCauss, col)
			}
			expr, err1 := er.newFunction(ast.RowFunc, newDefCauss[0].GetType(), newDefCauss...)
			if err1 != nil {
				er.err = err1
				return v, true
			}
			er.ctxStackAppend(expr, types.EmptyName)
		} else {
			er.ctxStackAppend(er.p.Schema().DeferredCausets[er.p.Schema().Len()-1], er.p.OutputNames()[er.p.Schema().Len()-1])
		}
		return v, true
	}
	// We don't want nth_plan hint to affect separately executed subqueries here, so disable nth_plan temporarily.
	NthCausetBackup := er.sctx.GetStochastikVars().StmtCtx.StmtHints.ForceNthCauset
	er.sctx.GetStochastikVars().StmtCtx.StmtHints.ForceNthCauset = -1
	physicalCauset, _, err := DoOptimize(ctx, er.sctx, er.b.optFlag, np)
	er.sctx.GetStochastikVars().StmtCtx.StmtHints.ForceNthCauset = NthCausetBackup
	if err != nil {
		er.err = err
		return v, true
	}
	event, err := EvalSubqueryFirstRow(ctx, physicalCauset, er.b.is, er.b.ctx)
	if err != nil {
		er.err = err
		return v, true
	}
	if np.Schema().Len() > 1 {
		newDefCauss := make([]memex.Expression, 0, np.Schema().Len())
		for i, data := range event {
			newDefCauss = append(newDefCauss, &memex.Constant{
				Value:   data,
				RetType: np.Schema().DeferredCausets[i].GetType()})
		}
		expr, err1 := er.newFunction(ast.RowFunc, newDefCauss[0].GetType(), newDefCauss...)
		if err1 != nil {
			er.err = err1
			return v, true
		}
		er.ctxStackAppend(expr, types.EmptyName)
	} else {
		er.ctxStackAppend(&memex.Constant{
			Value:   event[0],
			RetType: np.Schema().DeferredCausets[0].GetType(),
		}, types.EmptyName)
	}
	return v, true
}

// Leave implements Visitor interface.
func (er *memexRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	if er.err != nil {
		return retNode, false
	}
	var inNode = originInNode
	if er.preprocess != nil {
		inNode = er.preprocess(inNode)
	}
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr, *ast.DeferredCausetNameExpr, *ast.ParenthesesExpr, *ast.WhenClause,
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr, *ast.ValuesExpr, *ast.WindowFuncExpr, *ast.BlockNameExpr:
	case *driver.ValueExpr:
		v.Causet.SetValue(v.Causet.GetValue(), &v.Type)
		value := &memex.Constant{Value: v.Causet, RetType: &v.Type}
		er.ctxStackAppend(value, types.EmptyName)
	case *driver.ParamMarkerExpr:
		var value memex.Expression
		value, er.err = memex.ParamMarkerExpression(er.sctx, v)
		if er.err != nil {
			return retNode, false
		}
		er.ctxStackAppend(value, types.EmptyName)
	case *ast.VariableExpr:
		er.rewriteVariable(v)
	case *ast.FuncCallExpr:
		if _, ok := memex.TryFoldFunctions[v.FnName.L]; ok {
			er.tryFoldCounter--
		}
		er.funcCallToExpression(v)
		if _, ok := memex.DisableFoldFunctions[v.FnName.L]; ok {
			er.disableFoldCounter--
		}
	case *ast.BlockName:
		er.toBlock(v)
	case *ast.DeferredCausetName:
		er.toDeferredCauset(v)
	case *ast.UnaryOperationExpr:
		er.unaryOpToExpression(v)
	case *ast.BinaryOperationExpr:
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		if _, ok := memex.TryFoldFunctions["case"]; ok {
			er.tryFoldCounter--
		}
		er.caseToExpression(v)
		if _, ok := memex.DisableFoldFunctions["case"]; ok {
			er.disableFoldCounter--
		}
	case *ast.FuncCastExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		er.err = memex.CheckArgsNotMultiDeferredCausetRow(arg)
		if er.err != nil {
			return retNode, false
		}

		// check the decimal precision of "CAST(AS TIME)".
		er.err = er.checkTimePrecision(v.Tp)
		if er.err != nil {
			return retNode, false
		}

		er.ctxStack[len(er.ctxStack)-1] = memex.BuildCastFunction(er.sctx, arg, v.Tp)
		er.ctxNameStk[len(er.ctxNameStk)-1] = types.EmptyName
	case *ast.PatternLikeExpr:
		er.patternLikeToExpression(v)
	case *ast.PatternRegexpExpr:
		er.regexpToScalarFunc(v)
	case *ast.RowExpr:
		er.rowToScalarFunc(v)
	case *ast.PatternInExpr:
		if v.Sel == nil {
			er.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *ast.PositionExpr:
		er.positionToScalarFunc(v)
	case *ast.IsNullExpr:
		er.isNullToExpression(v)
	case *ast.IsTruthExpr:
		er.isTrueToScalarFunc(v)
	case *ast.DefaultExpr:
		er.evalDefaultExpr(v)
	// TODO: Perhaps we don't need to transcode these back to generic integers/strings
	case *ast.TrimDirectionExpr:
		er.ctxStackAppend(&memex.Constant{
			Value:   types.NewIntCauset(int64(v.Direction)),
			RetType: types.NewFieldType(allegrosql.TypeTiny),
		}, types.EmptyName)
	case *ast.TimeUnitExpr:
		er.ctxStackAppend(&memex.Constant{
			Value:   types.NewStringCauset(v.Unit.String()),
			RetType: types.NewFieldType(allegrosql.TypeVarchar),
		}, types.EmptyName)
	case *ast.GetFormatSelectorExpr:
		er.ctxStackAppend(&memex.Constant{
			Value:   types.NewStringCauset(v.Selector.String()),
			RetType: types.NewFieldType(allegrosql.TypeVarchar),
		}, types.EmptyName)
	case *ast.SetDefCauslationExpr:
		arg := er.ctxStack[len(er.ctxStack)-1]
		if collate.NewDefCauslationEnabled() {
			var collInfo *charset.DefCauslation
			// TODO(bb7133): use charset.ValidCharsetAndDefCauslation when its bug is fixed.
			if collInfo, er.err = collate.GetDefCauslationByName(v.DefCauslate); er.err != nil {
				break
			}
			chs := arg.GetType().Charset
			if chs != "" && collInfo.CharsetName != chs {
				er.err = charset.ErrDefCauslationCharsetMismatch.GenWithStackByArgs(collInfo.Name, chs)
				break
			}
		}
		// SetDefCauslationExpr sets the collation explicitly, even when the evaluation type of the memex is non-string.
		if _, ok := arg.(*memex.DeferredCauset); ok {
			// Wrap a cast here to avoid changing the original FieldType of the column memex.
			exprType := arg.GetType().Clone()
			exprType.DefCauslate = v.DefCauslate
			casted := memex.BuildCastFunction(er.sctx, arg, exprType)
			er.ctxStackPop(1)
			er.ctxStackAppend(casted, types.EmptyName)
		} else {
			// For constant and scalar function, we can set its collate directly.
			arg.GetType().DefCauslate = v.DefCauslate
		}
		er.ctxStack[len(er.ctxStack)-1].SetCoercibility(memex.CoercibilityExplicit)
		er.ctxStack[len(er.ctxStack)-1].SetCharsetAndDefCauslation(arg.GetType().Charset, arg.GetType().DefCauslate)
	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	if er.err != nil {
		return retNode, false
	}
	return originInNode, true
}

// newFunction chooses which memex.NewFunctionImpl() will be used.
func (er *memexRewriter) newFunction(funcName string, retType *types.FieldType, args ...memex.Expression) (memex.Expression, error) {
	if er.disableFoldCounter > 0 {
		return memex.NewFunctionBase(er.sctx, funcName, retType, args...)
	}
	if er.tryFoldCounter > 0 {
		return memex.NewFunctionTryFold(er.sctx, funcName, retType, args...)
	}
	return memex.NewFunction(er.sctx, funcName, retType, args...)
}

func (er *memexRewriter) checkTimePrecision(ft *types.FieldType) error {
	if ft.EvalType() == types.ETDuration && ft.Decimal > int(types.MaxFsp) {
		return errTooBigPrecision.GenWithStackByArgs(ft.Decimal, "CAST", types.MaxFsp)
	}
	return nil
}

func (er *memexRewriter) useCache() bool {
	return er.sctx.GetStochastikVars().StmtCtx.UseCache
}

func (er *memexRewriter) rewriteVariable(v *ast.VariableExpr) {
	stkLen := len(er.ctxStack)
	name := strings.ToLower(v.Name)
	stochastikVars := er.b.ctx.GetStochastikVars()
	if !v.IsSystem {
		if v.Value != nil {
			er.ctxStack[stkLen-1], er.err = er.newFunction(ast.SetVar,
				er.ctxStack[stkLen-1].GetType(),
				memex.CausetToConstant(types.NewCauset(name), allegrosql.TypeString),
				er.ctxStack[stkLen-1])
			er.ctxNameStk[stkLen-1] = types.EmptyName
			return
		}
		f, err := er.newFunction(ast.GetVar,
			// TODO: Here is wrong, the stochastikVars should causetstore a name -> Causet map. Will fix it later.
			types.NewFieldType(allegrosql.TypeString),
			memex.CausetToConstant(types.NewStringCauset(name), allegrosql.TypeString))
		if err != nil {
			er.err = err
			return
		}
		f.SetCoercibility(memex.CoercibilityImplicit)
		er.ctxStackAppend(f, types.EmptyName)
		return
	}
	var val string
	var err error
	if v.ExplicitScope {
		err = variable.ValidateGetSystemVar(name, v.IsGlobal)
		if err != nil {
			er.err = err
			return
		}
	}
	sysVar := variable.SysVars[name]
	if sysVar == nil {
		er.err = variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
		return
	}
	// Variable is @@gobal.variable_name or variable is only global scope variable.
	if v.IsGlobal || sysVar.Scope == variable.ScopeGlobal {
		val, err = variable.GetGlobalSystemVar(stochastikVars, name)
	} else {
		val, err = variable.GetStochastikSystemVar(stochastikVars, name)
	}
	if err != nil {
		er.err = err
		return
	}
	e := memex.CausetToConstant(types.NewStringCauset(val), allegrosql.TypeVarString)
	e.GetType().Charset, _ = er.sctx.GetStochastikVars().GetSystemVar(variable.CharacterSetConnection)
	e.GetType().DefCauslate, _ = er.sctx.GetStochastikVars().GetSystemVar(variable.DefCauslationConnection)
	er.ctxStackAppend(e, types.EmptyName)
}

func (er *memexRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var op string
	switch v.Op {
	case opcode.Plus:
		// memex (+ a) is equal to a
		return
	case opcode.Minus:
		op = ast.UnaryMinus
	case opcode.BitNeg:
		op = ast.BitNeg
	case opcode.Not:
		op = ast.UnaryNot
	default:
		er.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	if memex.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	er.ctxStack[stkLen-1], er.err = er.newFunction(op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxNameStk[stkLen-1] = types.EmptyName
}

func (er *memexRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var function memex.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, er.err = er.constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			v.Op.String())
	default:
		lLen := memex.GetRowLen(er.ctxStack[stkLen-2])
		rLen := memex.GetRowLen(er.ctxStack[stkLen-1])
		if lLen != 1 || rLen != 1 {
			er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(1)
			return
		}
		function, er.err = er.newFunction(v.Op.String(), types.NewFieldType(allegrosql.TypeUnspecified), er.ctxStack[stkLen-2:]...)
	}
	if er.err != nil {
		return
	}
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *memexRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...memex.Expression) memex.Expression {
	opFunc, err := er.newFunction(op, tp, args...)
	if err != nil {
		er.err = err
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = er.newFunction(ast.UnaryNot, tp, opFunc)
	if err != nil {
		er.err = err
		return nil
	}
	return opFunc
}

func (er *memexRewriter) isNullToExpression(v *ast.IsNullExpr) {
	stkLen := len(er.ctxStack)
	if memex.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *memexRewriter) positionToScalarFunc(v *ast.PositionExpr) {
	pos := v.N
	str := strconv.Itoa(pos)
	if v.P != nil {
		stkLen := len(er.ctxStack)
		val := er.ctxStack[stkLen-1]
		intNum, isNull, err := memex.GetIntFromConstant(er.sctx, val)
		str = "?"
		if err == nil {
			if isNull {
				return
			}
			pos = intNum
			er.ctxStackPop(1)
		}
		er.err = err
	}
	if er.err == nil && pos > 0 && pos <= er.schemaReplicant.Len() {
		er.ctxStackAppend(er.schemaReplicant.DeferredCausets[pos-1], er.names[pos-1])
	} else {
		er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(str, clauseMsg[er.b.curClause])
	}
}

func (er *memexRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruthWithoutNull
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if memex.GetRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(1)
		return
	}
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStackPop(1)
	er.ctxStackAppend(function, types.EmptyName)
}

// inToExpression converts in memex to a scalar function. The argument lLen means the length of in list.
// The argument not means if the memex is not in. The tp stands for the memex type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (er *memexRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	stkLen := len(er.ctxStack)
	l := memex.GetRowLen(er.ctxStack[stkLen-lLen-1])
	for i := 0; i < lLen; i++ {
		if l != memex.GetRowLen(er.ctxStack[stkLen-lLen+i]) {
			er.err = memex.ErrOperandDeferredCausets.GenWithStackByArgs(l)
			return
		}
	}
	args := er.ctxStack[stkLen-lLen-1:]
	leftFt := args[0].GetType()
	leftEt, leftIsNull := leftFt.EvalType(), leftFt.Tp == allegrosql.TypeNull
	if leftIsNull {
		er.ctxStackPop(lLen + 1)
		er.ctxStackAppend(memex.NewNull(), types.EmptyName)
		return
	}
	if leftEt == types.ETInt {
		for i := 1; i < len(args); i++ {
			if c, ok := args[i].(*memex.Constant); ok {
				var isExceptional bool
				args[i], isExceptional = memex.RefineComparedConstant(er.sctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					args[i] = c
				}
			}
		}
	}
	allSameType := true
	for _, arg := range args[1:] {
		if arg.GetType().Tp != allegrosql.TypeNull && memex.GetAccurateCmpType(args[0], arg) != leftEt {
			allSameType = false
			break
		}
	}
	var function memex.Expression
	if allSameType && l == 1 && lLen > 1 {
		function = er.notToExpression(not, ast.In, tp, er.ctxStack[stkLen-lLen-1:]...)
	} else {
		eqFunctions := make([]memex.Expression, 0, lLen)
		for i := stkLen - lLen; i < stkLen; i++ {
			expr, err := er.constructBinaryOpFunction(args[0], er.ctxStack[i], ast.EQ)
			if err != nil {
				er.err = err
				return
			}
			eqFunctions = append(eqFunctions, expr)
		}
		function = memex.ComposeDNFCondition(er.sctx, eqFunctions...)
		if not {
			var err error
			function, err = er.newFunction(ast.UnaryNot, tp, function)
			if err != nil {
				er.err = err
				return
			}
		}
	}
	er.ctxStackPop(lLen + 1)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *memexRewriter) caseToExpression(v *ast.CaseExpr) {
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		argsLen++
	}
	er.err = memex.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[stkLen-argsLen:]...)
	if er.err != nil {
		return
	}

	// value                          -> ctxStack[stkLen-argsLen-1]
	// when clause(condition, result) -> ctxStack[stkLen-argsLen:stkLen-1];
	// else clause                    -> ctxStack[stkLen-1]
	var args []memex.Expression
	if v.Value != nil {
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		value := er.ctxStack[stkLen-argsLen-1]
		args = make([]memex.Expression, 0, argsLen)
		for i := stkLen - argsLen; i < stkLen-1; i += 2 {
			arg, err := er.newFunction(ast.EQ, types.NewFieldType(allegrosql.TypeTiny), value, er.ctxStack[i])
			if err != nil {
				er.err = err
				return
			}
			args = append(args, arg)
			args = append(args, er.ctxStack[i+1])
		}
		if v.ElseClause != nil {
			args = append(args, er.ctxStack[stkLen-1])
		}
		argsLen++ // for trimming the value element later
	} else {
		// args:  condition1, result1,
		//        condition2, result2,
		//        ...
		//        else clause
		args = er.ctxStack[stkLen-argsLen:]
	}
	function, err := er.newFunction(ast.Case, &v.Type, args...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackPop(argsLen)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *memexRewriter) patternLikeToExpression(v *ast.PatternLikeExpr) {
	l := len(er.ctxStack)
	er.err = memex.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}

	char, col := er.sctx.GetStochastikVars().GetCharsetInfo()
	var function memex.Expression
	fieldType := &types.FieldType{}
	isPatternExactMatch := false
	// Treat predicate 'like' the same way as predicate '=' when it is an exact match.
	if patExpression, ok := er.ctxStack[l-1].(*memex.Constant); ok {
		patString, isNull, err := patExpression.EvalString(nil, chunk.Row{})
		if err != nil {
			er.err = err
			return
		}
		if !isNull {
			patValue, patTypes := stringutil.CompilePattern(patString, v.Escape)
			if stringutil.IsExactMatch(patTypes) && er.ctxStack[l-2].GetType().EvalType() == types.ETString {
				op := ast.EQ
				if v.Not {
					op = ast.NE
				}
				types.DefaultTypeForValue(string(patValue), fieldType, char, col)
				function, er.err = er.constructBinaryOpFunction(er.ctxStack[l-2],
					&memex.Constant{Value: types.NewStringCauset(string(patValue)), RetType: fieldType},
					op)
				isPatternExactMatch = true
			}
		}
	}
	if !isPatternExactMatch {
		types.DefaultTypeForValue(int(v.Escape), fieldType, char, col)
		function = er.notToExpression(v.Not, ast.Like, &v.Type,
			er.ctxStack[l-2], er.ctxStack[l-1], &memex.Constant{Value: types.NewIntCauset(int64(v.Escape)), RetType: fieldType})
	}

	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *memexRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	l := len(er.ctxStack)
	er.err = memex.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[l-2:]...)
	if er.err != nil {
		return
	}
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStackPop(2)
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *memexRewriter) rowToScalarFunc(v *ast.RowExpr) {
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]memex.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		rows = append(rows, er.ctxStack[i])
	}
	er.ctxStackPop(length)
	function, err := er.newFunction(ast.RowFunc, rows[0].GetType(), rows...)
	if err != nil {
		er.err = err
		return
	}
	er.ctxStackAppend(function, types.EmptyName)
}

func (er *memexRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	er.err = memex.CheckArgsNotMultiDeferredCausetRow(er.ctxStack[stkLen-3:]...)
	if er.err != nil {
		return
	}

	expr, lexp, rexp := er.ctxStack[stkLen-3], er.ctxStack[stkLen-2], er.ctxStack[stkLen-1]

	if memex.GetCmpTp4MinMax([]memex.Expression{expr, lexp, rexp}) == types.ETDatetime {
		expr = memex.WrapWithCastAsTime(er.sctx, expr, types.NewFieldType(allegrosql.TypeDatetime))
		lexp = memex.WrapWithCastAsTime(er.sctx, lexp, types.NewFieldType(allegrosql.TypeDatetime))
		rexp = memex.WrapWithCastAsTime(er.sctx, rexp, types.NewFieldType(allegrosql.TypeDatetime))
	}

	var op string
	var l, r memex.Expression
	l, er.err = er.newFunction(ast.GE, &v.Type, expr, lexp)
	if er.err == nil {
		r, er.err = er.newFunction(ast.LE, &v.Type, expr, rexp)
	}
	op = ast.LogicAnd
	if er.err != nil {
		return
	}
	function, err := er.newFunction(op, &v.Type, l, r)
	if err != nil {
		er.err = err
		return
	}
	if v.Not {
		function, err = er.newFunction(ast.UnaryNot, &v.Type, function)
		if err != nil {
			er.err = err
			return
		}
	}
	er.ctxStackPop(3)
	er.ctxStackAppend(function, types.EmptyName)
}

// rewriteFuncCall handles a FuncCallExpr and generates a customized function.
// It should return true if for the given FuncCallExpr a rewrite is performed so that original behavior is skipped.
// Otherwise it should return false to indicate (the caller) that original behavior needs to be performed.
func (er *memexRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	switch v.FnName.L {
	// when column is not null, ifnull on such column is not necessary.
	case ast.Ifnull:
		if len(v.Args) != 2 {
			er.err = memex.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		arg1 := er.ctxStack[stackLen-2]
		col, isDeferredCauset := arg1.(*memex.DeferredCauset)
		// if expr1 is a column and column has not null flag, then we can eliminate ifnull on
		// this column.
		if isDeferredCauset && allegrosql.HasNotNullFlag(col.RetType.Flag) {
			name := er.ctxNameStk[stackLen-2]
			newDefCaus := col.Clone().(*memex.DeferredCauset)
			er.ctxStackPop(len(v.Args))
			er.ctxStackAppend(newDefCaus, name)
			return true
		}

		return false
	case ast.Nullif:
		if len(v.Args) != 2 {
			er.err = memex.ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		stackLen := len(er.ctxStack)
		param1 := er.ctxStack[stackLen-2]
		param2 := er.ctxStack[stackLen-1]
		// param1 = param2
		funcCompare, err := er.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			er.err = err
			return true
		}
		// NULL
		nullTp := types.NewFieldType(allegrosql.TypeNull)
		nullTp.Flen, nullTp.Decimal = allegrosql.GetDefaultFieldLengthAndDecimal(allegrosql.TypeNull)
		paramNull := &memex.Constant{
			Value:   types.NewCauset(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := er.newFunction(ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			er.err = err
			return true
		}
		er.ctxStackPop(len(v.Args))
		er.ctxStackAppend(funcIf, types.EmptyName)
		return true
	default:
		return false
	}
}

func (er *memexRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	args := er.ctxStack[stackLen-len(v.Args):]
	er.err = memex.CheckArgsNotMultiDeferredCausetRow(args...)
	if er.err != nil {
		return
	}

	if er.rewriteFuncCall(v) {
		return
	}

	var function memex.Expression
	er.ctxStackPop(len(v.Args))
	if _, ok := memex.DeferredFunctions[v.FnName.L]; er.useCache() && ok {
		// When the memex is unix_timestamp and the number of argument is not zero,
		// we deal with it as normal memex.
		if v.FnName.L == ast.UnixTimestamp && len(v.Args) != 0 {
			function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
			er.ctxStackAppend(function, types.EmptyName)
		} else {
			function, er.err = memex.NewFunctionBase(er.sctx, v.FnName.L, &v.Type, args...)
			c := &memex.Constant{Value: types.NewCauset(nil), RetType: function.GetType().Clone(), DeferredExpr: function}
			er.ctxStackAppend(c, types.EmptyName)
		}
	} else {
		function, er.err = er.newFunction(v.FnName.L, &v.Type, args...)
		er.ctxStackAppend(function, types.EmptyName)
	}
}

// Now BlockName in memex only used by sequence function like nextval(seq).
// The function arg should be evaluated as a causet name rather than normal column name like allegrosql does.
func (er *memexRewriter) toBlock(v *ast.BlockName) {
	fullName := v.Name.L
	if len(v.Schema.L) != 0 {
		fullName = v.Schema.L + "." + fullName
	}
	val := &memex.Constant{
		Value:   types.NewCauset(fullName),
		RetType: types.NewFieldType(allegrosql.TypeString),
	}
	er.ctxStackAppend(val, types.EmptyName)
}

func (er *memexRewriter) toDeferredCauset(v *ast.DeferredCausetName) {
	idx, err := memex.FindFieldName(er.names, v)
	if err != nil {
		er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
		return
	}
	if idx >= 0 {
		column := er.schemaReplicant.DeferredCausets[idx]
		if column.IsHidden {
			er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name, clauseMsg[er.b.curClause])
			return
		}
		er.ctxStackAppend(column, er.names[idx])
		return
	}
	for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
		outerSchema, outerName := er.b.outerSchemas[i], er.b.outerNames[i]
		idx, err = memex.FindFieldName(outerName, v)
		if idx >= 0 {
			column := outerSchema.DeferredCausets[idx]
			er.ctxStackAppend(&memex.CorrelatedDeferredCauset{DeferredCauset: *column, Data: new(types.Causet)}, outerName[idx])
			return
		}
		if err != nil {
			er.err = ErrAmbiguous.GenWithStackByArgs(v.Name, clauseMsg[fieldList])
			return
		}
	}
	if join, ok := er.p.(*LogicalJoin); ok && join.redundantSchema != nil {
		idx, err := memex.FindFieldName(join.redundantNames, v)
		if err != nil {
			er.err = err
			return
		}
		if idx >= 0 {
			er.ctxStackAppend(join.redundantSchema.DeferredCausets[idx], join.redundantNames[idx])
			return
		}
	}
	if _, ok := er.p.(*LogicalUnionAll); ok && v.Block.O != "" {
		er.err = ErrBlocknameNotAllowedHere.GenWithStackByArgs(v.Block.O, "SELECT", clauseMsg[er.b.curClause])
		return
	}
	if er.b.curClause == globalOrderByClause {
		er.b.curClause = orderByClause
	}
	er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.String(), clauseMsg[er.b.curClause])
}

func (er *memexRewriter) evalDefaultExpr(v *ast.DefaultExpr) {
	var name *types.FieldName
	// Here we will find the corresponding column for default function. At the same time, we need to consider the issue
	// of subquery and name space.
	// For example, we have two blocks t1(a int default 1, b int) and t2(a int default -1, c int). Consider the following ALLEGROALLEGROSQL:
	// 		select a from t1 where a > (select default(a) from t2)
	// Refer to the behavior of MyALLEGROSQL, we need to find column a in causet t2. If causet t2 does not have column a, then find it
	// in causet t1. If there are none, return an error message.
	// Based on the above description, we need to look in er.b.allNames from back to front.
	for i := len(er.b.allNames) - 1; i >= 0; i-- {
		idx, err := memex.FindFieldName(er.b.allNames[i], v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx >= 0 {
			name = er.b.allNames[i][idx]
			break
		}
	}
	if name == nil {
		idx, err := memex.FindFieldName(er.names, v.Name)
		if err != nil {
			er.err = err
			return
		}
		if idx < 0 {
			er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name.OrigDefCausName(), "field list")
			return
		}
		name = er.names[idx]
	}

	dbName := name.DBName
	if dbName.O == "" {
		// if database name is not specified, use current database name
		dbName = perceptron.NewCIStr(er.sctx.GetStochastikVars().CurrentDB)
	}
	if name.OrigTblName.O == "" {
		// column is evaluated by some memexs, for example:
		// `select default(c) from (select (a+1) as c from t) as t0`
		// in such case, a 'no default' error is returned
		er.err = causet.ErrNoDefaultValue.GenWithStackByArgs(name.DefCausName)
		return
	}
	var tbl causet.Block
	tbl, er.err = er.b.is.BlockByName(dbName, name.OrigTblName)
	if er.err != nil {
		return
	}
	colName := name.OrigDefCausName.O
	if colName == "" {
		// in some cases, OrigDefCausName is empty, use DefCausName instead
		colName = name.DefCausName.O
	}
	col := causet.FindDefCaus(tbl.DefCauss(), colName)
	if col == nil {
		er.err = ErrUnknownDeferredCauset.GenWithStackByArgs(v.Name, "field_list")
		return
	}
	isCurrentTimestamp := hasCurrentDatetimeDefault(col)
	var val *memex.Constant
	switch {
	case isCurrentTimestamp && col.Tp == allegrosql.TypeDatetime:
		// for DATETIME column with current_timestamp, use NULL to be compatible with MyALLEGROSQL 5.7
		val = memex.NewNull()
	case isCurrentTimestamp && col.Tp == allegrosql.TypeTimestamp:
		// for TIMESTAMP column with current_timestamp, use 0 to be compatible with MyALLEGROSQL 5.7
		zero := types.NewTime(types.ZeroCoreTime, allegrosql.TypeTimestamp, int8(col.Decimal))
		val = &memex.Constant{
			Value:   types.NewCauset(zero),
			RetType: types.NewFieldType(allegrosql.TypeTimestamp),
		}
	default:
		// for other columns, just use what it is
		val, er.err = er.b.getDefaultValue(col)
	}
	if er.err != nil {
		return
	}
	er.ctxStackAppend(val, types.EmptyName)
}

// hasCurrentDatetimeDefault checks if column has current_timestamp default value
func hasCurrentDatetimeDefault(col *causet.DeferredCauset) bool {
	x, ok := col.DefaultValue.(string)
	if !ok {
		return false
	}
	return strings.ToLower(x) == ast.CurrentTimestamp
}
