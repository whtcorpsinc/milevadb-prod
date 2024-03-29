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

package embedded_test

import (
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/types/BerolinaSQL_driver"
)

var _ = Suite(&testCacheableSuite{})

type testCacheableSuite struct {
}

func (s *testCacheableSuite) TestCacheable(c *C) {
	causetstore, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, causetstore)
	defer func() {
		dom.Close()
		causetstore.Close()
	}()
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet t1(a int, b int) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustInterDirc("create causet t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustInterDirc("create causet t3(a int, b int)")
	tbl := &ast.BlockName{Schema: perceptron.NewCIStr("test"), Name: perceptron.NewCIStr("t3")}
	is := schemareplicant.GetSchemaReplicant(tk.Se)
	// test non-SelectStmt/-InsertStmt/-DeleteStmt/-UFIDelateStmt/-SetOprStmt
	var stmt ast.Node = &ast.ShowStmt{}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	stmt = &ast.LoadDataStmt{}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	// test SetOprStmt
	stmt = &ast.SetOprStmt{}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	blockRefsClause := &ast.BlockRefsClause{BlockRefs: &ast.Join{Left: &ast.BlockSource{Source: tbl}}}
	// test InsertStmt
	stmt = &ast.InsertStmt{Block: blockRefsClause}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	// test DeleteStmt
	whereExpr := &ast.FuncCallExpr{}
	stmt = &ast.DeleteStmt{
		BlockRefs: blockRefsClause,
		Where:     whereExpr,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	for funcName := range memex.UnCacheableFunctions {
		whereExpr.FnName = perceptron.NewCIStr(funcName)
		c.Assert(embedded.Cacheable(stmt, is), IsFalse)
	}

	whereExpr.FnName = perceptron.NewCIStr(ast.Rand)
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	stmt = &ast.DeleteStmt{
		BlockRefs: blockRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt := &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		BlockRefs: blockRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		BlockRefs: blockRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.DeleteStmt{
		BlockRefs: blockRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	stmt.(*ast.DeleteStmt).BlockHints = append(stmt.(*ast.DeleteStmt).BlockHints, &ast.BlockOptimizerHint{
		HintName: perceptron.NewCIStr(embedded.HintIgnoreCausetCache),
	})
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	// test UFIDelateStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.UFIDelateStmt{
		BlockRefs: blockRefsClause,
		Where:     whereExpr,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	for funcName := range memex.UnCacheableFunctions {
		whereExpr.FnName = perceptron.NewCIStr(funcName)
		c.Assert(embedded.Cacheable(stmt, is), IsFalse)
	}

	whereExpr.FnName = perceptron.NewCIStr(ast.Rand)
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	stmt = &ast.UFIDelateStmt{
		BlockRefs: blockRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UFIDelateStmt{
		BlockRefs: blockRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UFIDelateStmt{
		BlockRefs: blockRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.UFIDelateStmt{
		BlockRefs: blockRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	stmt.(*ast.UFIDelateStmt).BlockHints = append(stmt.(*ast.UFIDelateStmt).BlockHints, &ast.BlockOptimizerHint{
		HintName: perceptron.NewCIStr(embedded.HintIgnoreCausetCache),
	})
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	// test SelectStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.SelectStmt{
		Where: whereExpr,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	for funcName := range memex.UnCacheableFunctions {
		whereExpr.FnName = perceptron.NewCIStr(funcName)
		c.Assert(embedded.Cacheable(stmt, is), IsFalse)
	}

	whereExpr.FnName = perceptron.NewCIStr(ast.Rand)
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	stmt = &ast.SelectStmt{
		Where: &ast.ExistsSubqueryExpr{},
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	paramExpr := &driver.ParamMarkerExpr{}
	orderByClause := &ast.OrderByClause{Items: []*ast.ByItem{{Expr: paramExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	valExpr := &driver.ValueExpr{}
	orderByClause = &ast.OrderByClause{Items: []*ast.ByItem{{Expr: valExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)

	stmt.(*ast.SelectStmt).BlockHints = append(stmt.(*ast.SelectStmt).BlockHints, &ast.BlockOptimizerHint{
		HintName: perceptron.NewCIStr(embedded.HintIgnoreCausetCache),
	})
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	boundExpr := &ast.FrameBound{Expr: &driver.ParamMarkerExpr{}}
	c.Assert(embedded.Cacheable(boundExpr, is), IsFalse)

	// Partition causet can not be cached.
	join := &ast.Join{
		Left:  &ast.BlockName{Schema: perceptron.NewCIStr("test"), Name: perceptron.NewCIStr("t1")},
		Right: &ast.BlockName{Schema: perceptron.NewCIStr("test"), Name: perceptron.NewCIStr("t2")},
	}
	stmt = &ast.SelectStmt{
		From: &ast.BlockRefsClause{
			BlockRefs: join,
		},
	}
	c.Assert(embedded.Cacheable(stmt, is), IsFalse)

	join = &ast.Join{
		Left: &ast.BlockName{Schema: perceptron.NewCIStr("test"), Name: perceptron.NewCIStr("t3")},
	}
	stmt = &ast.SelectStmt{
		From: &ast.BlockRefsClause{
			BlockRefs: join,
		},
	}
	c.Assert(embedded.Cacheable(stmt, is), IsTrue)
}
