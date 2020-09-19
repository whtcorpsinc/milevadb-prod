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
	"fmt"
	"reflect"
	"strings"
	"unsafe"
	_ "unsafe" // required by go:linkname

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/expression/aggregation"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/planner/property"
	"github.com/whtcorpsinc/milevadb/planner/soliton"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
)

var _ = Suite(&testPlanBuilderSuite{})

func (s *testPlanBuilderSuite) SetUpSuite(c *C) {
}

type testPlanBuilderSuite struct {
}

func (s *testPlanBuilderSuite) TestShow(c *C) {
	node := &ast.ShowStmt{}
	tps := []ast.ShowStmtType{
		ast.ShowEngines,
		ast.ShowDatabases,
		ast.ShowBlocks,
		ast.ShowBlockStatus,
		ast.ShowDeferredCausets,
		ast.ShowWarnings,
		ast.ShowCharset,
		ast.ShowVariables,
		ast.ShowStatus,
		ast.ShowDefCauslation,
		ast.ShowCreateBlock,
		ast.ShowCreateUser,
		ast.ShowGrants,
		ast.ShowTriggers,
		ast.ShowProcedureStatus,
		ast.ShowIndex,
		ast.ShowProcessList,
		ast.ShowCreateDatabase,
		ast.ShowEvents,
		ast.ShowMasterStatus,
		ast.ShowBackups,
		ast.ShowRestores,
	}
	for _, tp := range tps {
		node.Tp = tp
		schemaReplicant, _ := buildShowSchema(node, false, false)
		for _, col := range schemaReplicant.DeferredCausets {
			c.Assert(col.RetType.Flen, Greater, 0)
		}
	}
}

func (s *testPlanBuilderSuite) TestGetPathByIndexName(c *C) {
	tblInfo := &perceptron.BlockInfo{
		Indices:    make([]*perceptron.IndexInfo, 0),
		PKIsHandle: true,
	}

	accessPath := []*soliton.AccessPath{
		{IsIntHandlePath: true},
		{Index: &perceptron.IndexInfo{Name: perceptron.NewCIStr("idx")}},
	}

	path := getPathByIndexName(accessPath, perceptron.NewCIStr("idx"), tblInfo)
	c.Assert(path, NotNil)
	c.Assert(path, Equals, accessPath[1])

	path = getPathByIndexName(accessPath, perceptron.NewCIStr("primary"), tblInfo)
	c.Assert(path, NotNil)
	c.Assert(path, Equals, accessPath[0])

	path = getPathByIndexName(accessPath, perceptron.NewCIStr("not exists"), tblInfo)
	c.Assert(path, IsNil)

	tblInfo = &perceptron.BlockInfo{
		Indices:    make([]*perceptron.IndexInfo, 0),
		PKIsHandle: false,
	}

	path = getPathByIndexName(accessPath, perceptron.NewCIStr("primary"), tblInfo)
	c.Assert(path, IsNil)
}

func (s *testPlanBuilderSuite) TestRewriterPool(c *C) {
	builder := NewPlanBuilder(MockContext(), nil, &hint.BlockHintProcessor{})

	// Make sure PlanBuilder.getExpressionRewriter() provides clean rewriter from pool.
	// First, pick one rewriter from the pool and make it dirty.
	builder.rewriterCounter++
	dirtyRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	dirtyRewriter.asScalar = true
	dirtyRewriter.aggrMap = make(map[*ast.AggregateFuncExpr]int)
	dirtyRewriter.preprocess = func(ast.Node) ast.Node { return nil }
	dirtyRewriter.insertPlan = &Insert{}
	dirtyRewriter.disableFoldCounter = 1
	dirtyRewriter.ctxStack = make([]expression.Expression, 2)
	dirtyRewriter.ctxNameStk = make([]*types.FieldName, 2)
	builder.rewriterCounter--
	// Then, pick again and check if it's cleaned up.
	builder.rewriterCounter++
	cleanRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	c.Assert(cleanRewriter, Equals, dirtyRewriter) // Rewriter should be reused.
	c.Assert(cleanRewriter.asScalar, Equals, false)
	c.Assert(cleanRewriter.aggrMap, IsNil)
	c.Assert(cleanRewriter.preprocess, IsNil)
	c.Assert(cleanRewriter.insertPlan, IsNil)
	c.Assert(cleanRewriter.disableFoldCounter, Equals, 0)
	c.Assert(len(cleanRewriter.ctxStack), Equals, 0)
	builder.rewriterCounter--
}

func (s *testPlanBuilderSuite) TestDisableFold(c *C) {
	// Functions like BENCHMARK() shall not be folded into result 0,
	// but normal outer function with constant args should be folded.
	// Types of expression and first layer of args will be validated.
	cases := []struct {
		ALLEGROALLEGROSQL      string
		Expected expression.Expression
		Args     []expression.Expression
	}{
		{`select sin(length("abc"))`, &expression.Constant{}, nil},
		{`select benchmark(3, sin(123))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
		{`select pow(length("abc"), benchmark(3, sin(123)))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
	}

	ctx := MockContext()
	for _, t := range cases {
		st, err := berolinaAllegroSQL.New().ParseOneStmt(t.ALLEGROALLEGROSQL, "", "")
		c.Assert(err, IsNil)
		stmt := st.(*ast.SelectStmt)
		expr := stmt.Fields.Fields[0].Expr

		builder := NewPlanBuilder(ctx, nil, &hint.BlockHintProcessor{})
		builder.rewriterCounter++
		rewriter := builder.getExpressionRewriter(context.TODO(), nil)
		c.Assert(rewriter, NotNil)
		c.Assert(rewriter.disableFoldCounter, Equals, 0)
		rewritenExpression, _, err := builder.rewriteExprNode(rewriter, expr, true)
		c.Assert(err, IsNil)
		c.Assert(rewriter.disableFoldCounter, Equals, 0) // Make sure the counter is reduced to 0 in the end.
		builder.rewriterCounter--

		c.Assert(rewritenExpression, FitsTypeOf, t.Expected)
		for i, expectedArg := range t.Args {
			rewritenArg := expression.GetFuncArg(rewritenExpression, i)
			c.Assert(rewritenArg, FitsTypeOf, expectedArg)
		}
	}
}

func (s *testPlanBuilderSuite) TestDeepClone(c *C) {
	tp := types.NewFieldType(allegrosql.TypeLonglong)
	expr := &expression.DeferredCauset{RetType: tp}
	byItems := []*soliton.ByItems{{Expr: expr}}
	sort1 := &PhysicalSort{ByItems: byItems}
	sort2 := &PhysicalSort{ByItems: byItems}
	checkDeepClone := func(p1, p2 PhysicalPlan) error {
		whiteList := []string{"*property.StatsInfo", "*stochastikctx.Context", "*mock.Context"}
		return checkDeepClonedCore(reflect.ValueOf(p1), reflect.ValueOf(p2), typeName(reflect.TypeOf(p1)), whiteList, nil)
	}
	c.Assert(checkDeepClone(sort1, sort2), ErrorMatches, "invalid slice pointers, path PhysicalSort.ByItems")

	byItems2 := []*soliton.ByItems{{Expr: expr}}
	sort2.ByItems = byItems2
	c.Assert(checkDeepClone(sort1, sort2), ErrorMatches, "same pointer, path PhysicalSort.ByItems.*Expression")

	expr2 := &expression.DeferredCauset{RetType: tp}
	byItems2[0].Expr = expr2
	c.Assert(checkDeepClone(sort1, sort2), ErrorMatches, "same pointer, path PhysicalSort.ByItems.*Expression.FieldType")

	expr2.RetType = types.NewFieldType(allegrosql.TypeString)
	c.Assert(checkDeepClone(sort1, sort2), ErrorMatches, "different values, path PhysicalSort.ByItems.*Expression.FieldType.uint8")

	expr2.RetType = types.NewFieldType(allegrosql.TypeLonglong)
	c.Assert(checkDeepClone(sort1, sort2), IsNil)
}

func (s *testPlanBuilderSuite) TestPhysicalPlanClone(c *C) {
	ctx := mock.NewContext()
	col, cst := &expression.DeferredCauset{RetType: types.NewFieldType(allegrosql.TypeString)}, &expression.Constant{RetType: types.NewFieldType(allegrosql.TypeLonglong)}
	stats := &property.StatsInfo{RowCount: 1000}
	schemaReplicant := expression.NewSchema(col)
	tblInfo := &perceptron.BlockInfo{}
	idxInfo := &perceptron.IndexInfo{}
	hist := &statistics.Histogram{Bounds: chunk.New(nil, 0, 0)}
	aggDesc1, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	aggDesc2, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{cst}, true)
	c.Assert(err, IsNil)
	aggDescs := []*aggregation.AggFuncDesc{aggDesc1, aggDesc2}

	// block scan
	blockScan := &PhysicalBlockScan{
		AccessCondition: []expression.Expression{col, cst},
		Block:           tblInfo,
		PkDefCauss:          []*expression.DeferredCauset{col},
		Hist:            hist,
	}
	blockScan = blockScan.Init(ctx, 0)
	blockScan.SetSchema(schemaReplicant)
	c.Assert(checkPhysicalPlanClone(blockScan), IsNil)

	// block reader
	blockReader := &PhysicalBlockReader{
		blockPlan:  blockScan,
		BlockPlans: []PhysicalPlan{blockScan},
		StoreType:  ekv.TiFlash,
	}
	blockReader = blockReader.Init(ctx, 0)
	c.Assert(checkPhysicalPlanClone(blockReader), IsNil)

	// index scan
	indexScan := &PhysicalIndexScan{
		AccessCondition:  []expression.Expression{col, cst},
		Block:            tblInfo,
		Index:            idxInfo,
		Hist:             hist,
		dataSourceSchema: schemaReplicant,
	}
	indexScan = indexScan.Init(ctx, 0)
	indexScan.SetSchema(schemaReplicant)
	c.Assert(checkPhysicalPlanClone(indexScan), IsNil)

	// index reader
	indexReader := &PhysicalIndexReader{
		indexPlan:     indexScan,
		IndexPlans:    []PhysicalPlan{indexScan},
		OutputDeferredCausets: []*expression.DeferredCauset{col, col},
	}
	indexReader = indexReader.Init(ctx, 0)
	c.Assert(checkPhysicalPlanClone(indexReader), IsNil)

	// index lookup
	indexLookup := &PhysicalIndexLookUpReader{
		IndexPlans:     []PhysicalPlan{indexReader},
		indexPlan:      indexScan,
		BlockPlans:     []PhysicalPlan{blockReader},
		blockPlan:      blockScan,
		ExtraHandleDefCaus: col,
		PushedLimit:    &PushedDownLimit{1, 2},
	}
	indexLookup = indexLookup.Init(ctx, 0)
	c.Assert(checkPhysicalPlanClone(indexLookup), IsNil)

	// selection
	sel := &PhysicalSelection{Conditions: []expression.Expression{col, cst}}
	sel = sel.Init(ctx, stats, 0)
	c.Assert(checkPhysicalPlanClone(sel), IsNil)

	// projection
	proj := &PhysicalProjection{Exprs: []expression.Expression{col, cst}}
	proj = proj.Init(ctx, stats, 0)
	c.Assert(checkPhysicalPlanClone(proj), IsNil)

	// limit
	lim := &PhysicalLimit{Count: 1, Offset: 2}
	lim = lim.Init(ctx, stats, 0)
	c.Assert(checkPhysicalPlanClone(lim), IsNil)

	// sort
	byItems := []*soliton.ByItems{{Expr: col}, {Expr: cst}}
	sort := &PhysicalSort{ByItems: byItems}
	sort = sort.Init(ctx, stats, 0)
	c.Assert(checkPhysicalPlanClone(sort), IsNil)

	// topN
	topN := &PhysicalTopN{ByItems: byItems, Offset: 2333, Count: 2333}
	topN = topN.Init(ctx, stats, 0)
	c.Assert(checkPhysicalPlanClone(topN), IsNil)

	// stream agg
	streamAgg := &PhysicalStreamAgg{basePhysicalAgg{
		AggFuncs:     aggDescs,
		GroupByItems: []expression.Expression{col, cst},
	}}
	streamAgg = streamAgg.initForStream(ctx, stats, 0)
	streamAgg.SetSchema(schemaReplicant)
	c.Assert(checkPhysicalPlanClone(streamAgg), IsNil)

	// hash agg
	hashAgg := &PhysicalHashAgg{basePhysicalAgg{
		AggFuncs:     aggDescs,
		GroupByItems: []expression.Expression{col, cst},
	}}
	hashAgg = hashAgg.initForHash(ctx, stats, 0)
	hashAgg.SetSchema(schemaReplicant)
	c.Assert(checkPhysicalPlanClone(hashAgg), IsNil)

	// hash join
	hashJoin := &PhysicalHashJoin{
		Concurrency:     4,
		UseOuterToBuild: true,
	}
	hashJoin = hashJoin.Init(ctx, stats, 0)
	hashJoin.SetSchema(schemaReplicant)
	c.Assert(checkPhysicalPlanClone(hashJoin), IsNil)

	// merge join
	mergeJoin := &PhysicalMergeJoin{
		CompareFuncs: []expression.CompareFunc{expression.CompareInt},
		Desc:         true,
	}
	mergeJoin = mergeJoin.Init(ctx, stats, 0)
	mergeJoin.SetSchema(schemaReplicant)
	c.Assert(checkPhysicalPlanClone(mergeJoin), IsNil)
}

//go:linkname valueInterface reflect.valueInterface
func valueInterface(v reflect.Value, safe bool) interface{}

func typeName(t reflect.Type) string {
	path := t.String()
	tmp := strings.Split(path, ".")
	return tmp[len(tmp)-1]
}

func checkPhysicalPlanClone(p PhysicalPlan) error {
	cloned, err := p.Clone()
	if err != nil {
		return err
	}
	whiteList := []string{"*property.StatsInfo", "*stochastikctx.Context", "*mock.Context", "*types.FieldType"}
	return checkDeepClonedCore(reflect.ValueOf(p), reflect.ValueOf(cloned), typeName(reflect.TypeOf(p)), whiteList, nil)
}

// checkDeepClonedCore is used to check if v2 is deep cloned from v1.
// It's modified from reflect.deepValueEqual. We cannot use reflect.DeepEqual here since they have different
// logic, for example, if two pointers point the same address, they will pass the DeepEqual check while failing in the DeepClone check.
func checkDeepClonedCore(v1, v2 reflect.Value, path string, whiteList []string, visited map[visit]bool) error {
	if !v1.IsValid() || !v2.IsValid() {
		if v1.IsValid() != v2.IsValid() {
			return errors.Errorf("invalid")
		}
		return nil
	}
	if v1.Type() != v2.Type() {
		return errors.Errorf("different type %v, %v, path %v", v1.Type(), v2.Type(), path)
	}

	if visited == nil {
		visited = make(map[visit]bool)
	}
	hard := func(k reflect.HoTT) bool {
		switch k {
		case reflect.Map, reflect.Slice, reflect.Ptr, reflect.Interface:
			return true
		}
		return false
	}
	if v1.CanAddr() && v2.CanAddr() && hard(v1.HoTT()) {
		addr1 := unsafe.Pointer(v1.UnsafeAddr())
		addr2 := unsafe.Pointer(v2.UnsafeAddr())
		if uintptr(addr1) > uintptr(addr2) {
			addr1, addr2 = addr2, addr1
		}
		typ := v1.Type()
		v := visit{addr1, addr2, typ}
		if visited[v] {
			return nil
		}
		visited[v] = true
	}

	switch v1.HoTT() {
	case reflect.Array:
		for i := 0; i < v1.Len(); i++ {
			if err := checkDeepClonedCore(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), whiteList, visited); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if (v1.IsNil() && v2.IsNil()) || (v1.Len() == 0 && v2.Len() == 0) {
			return nil
		}
		if v1.Len() != v2.Len() {
			return errors.Errorf("different slice lengths, len %v, %v, path %v", v1.Len(), v2.Len(), path)
		}
		if v1.IsNil() != v2.IsNil() {
			if v1.Len() == 0 && v2.Len() == 0 {
				return nil // nil and an empty slice are accepted
			}
			return errors.Errorf("different slices nil %v, %v, path %v", v1.IsNil(), v2.IsNil(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("invalid slice pointers, path %v", path)
		}
		for i := 0; i < v1.Len(); i++ {
			if err := checkDeepClonedCore(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), whiteList, visited); err != nil {
				return err
			}
		}
	case reflect.Interface:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.IsNil() != v2.IsNil() {
			return errors.Errorf("invalid interfaces, path %v", path)
		}
		return checkDeepClonedCore(v1.Elem(), v2.Elem(), path, whiteList, visited)
	case reflect.Ptr:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.Pointer() == v2.Pointer() {
			typeName := v1.Type().String()
			inWhiteList := false
			for _, whiteName := range whiteList {
				if whiteName == typeName {
					inWhiteList = true
					break
				}
			}
			if inWhiteList {
				return nil
			}
			return errors.Errorf("same pointer, path %v", path)
		}
		return checkDeepClonedCore(v1.Elem(), v2.Elem(), path, whiteList, visited)
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			if err := checkDeepClonedCore(v1.Field(i), v2.Field(i), fmt.Sprintf("%v.%v", path, typeName(v1.Field(i).Type())), whiteList, visited); err != nil {
				return err
			}
		}
	case reflect.Map:
		if (v1.IsNil() && v2.IsNil()) || (v1.Len() == 0 && v2.Len() == 0) {
			return nil
		}
		if v1.IsNil() != v2.IsNil() || v1.Len() != v2.Len() {
			return errors.Errorf("different maps nil: %v, %v, len: %v, %v, path: %v", v1.IsNil(), v2.IsNil(), v1.Len(), v2.Len(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("invalid map pointers, path %v", path)
		}
		if len(v1.MapKeys()) != len(v2.MapKeys()) {
			return errors.Errorf("invalid map")
		}
		for _, k := range v1.MapKeys() {
			val1 := v1.MapIndex(k)
			val2 := v2.MapIndex(k)
			if !val1.IsValid() || !val2.IsValid() {
				if err := checkDeepClonedCore(val1, val2, fmt.Sprintf("%v[%v]", path, typeName(k.Type())), whiteList, visited); err != nil {
					return err
				}
			}
		}
	case reflect.Func:
		if v1.IsNil() != v2.IsNil() {
			return errors.Errorf("invalid functions, path %v", path)
		}
		return nil // assume that these functions are stateless
	default:
		if valueInterface(v1, false) != valueInterface(v2, false) {
			return errors.Errorf("different values, path %v", path)
		}
	}
	return nil
}

type visit struct {
	a1  unsafe.Pointer
	a2  unsafe.Pointer
	typ reflect.Type
}
