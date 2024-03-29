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

package embedded

import (
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/types/BerolinaSQL_driver"
	"go.uber.org/zap"
)

// Cacheable checks whether the input ast is cacheable.
// Handle "ignore_plan_cache()" hint
// If there are multiple hints, only one will take effect
func Cacheable(node ast.Node, is schemareplicant.SchemaReplicant) bool {
	_, isSelect := node.(*ast.SelectStmt)
	_, isUFIDelate := node.(*ast.UFIDelateStmt)
	_, isInsert := node.(*ast.InsertStmt)
	_, isDelete := node.(*ast.DeleteStmt)
	_, isSetOpr := node.(*ast.SetOprStmt)
	if !(isSelect || isUFIDelate || isInsert || isDelete || isSetOpr) {
		return false
	}
	checker := cacheableChecker{
		cacheable:       true,
		schemaReplicant: is,
	}
	node.Accept(&checker)
	return checker.cacheable
}

// cacheableChecker checks whether a query's plan can be cached, querys that:
//	 1. have ExistsSubqueryExpr, or
//	 2. have VariableExpr
// will not be cached currently.
// NOTE: we can add more rules in the future.
type cacheableChecker struct {
	cacheable       bool
	schemaReplicant schemareplicant.SchemaReplicant
}

// Enter implements Visitor interface.
func (checker *cacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		for _, hints := range node.BlockHints {
			if hints.HintName.L == HintIgnoreCausetCache {
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.DeleteStmt:
		for _, hints := range node.BlockHints {
			if hints.HintName.L == HintIgnoreCausetCache {
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.UFIDelateStmt:
		for _, hints := range node.BlockHints {
			if hints.HintName.L == HintIgnoreCausetCache {
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.VariableExpr, *ast.ExistsSubqueryExpr, *ast.SubqueryExpr:
		checker.cacheable = false
		return in, true
	case *ast.FuncCallExpr:
		if _, found := memex.UnCacheableFunctions[node.FnName.L]; found {
			checker.cacheable = false
			return in, true
		}
	case *ast.OrderByClause:
		for _, item := range node.Items {
			if _, isParamMarker := item.Expr.(*driver.ParamMarkerExpr); isParamMarker {
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.GroupByClause:
		for _, item := range node.Items {
			if _, isParamMarker := item.Expr.(*driver.ParamMarkerExpr); isParamMarker {
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.Limit:
		if node.Count != nil {
			if _, isParamMarker := node.Count.(*driver.ParamMarkerExpr); isParamMarker {
				checker.cacheable = false
				return in, true
			}
		}
		if node.Offset != nil {
			if _, isParamMarker := node.Offset.(*driver.ParamMarkerExpr); isParamMarker {
				checker.cacheable = false
				return in, true
			}
		}
	case *ast.FrameBound:
		if _, ok := node.Expr.(*driver.ParamMarkerExpr); ok {
			checker.cacheable = false
			return in, true
		}
	case *ast.BlockName:
		if checker.schemaReplicant != nil {
			if checker.isPartitionBlock(node) {
				checker.cacheable = false
				return in, true
			}
		}
	}
	return in, false
}

func (checker *cacheableChecker) isPartitionBlock(tn *ast.BlockName) bool {
	tb, err := checker.schemaReplicant.BlockByName(tn.Schema, tn.Name)
	if err != nil {
		logutil.BgLogger().Error("Error occur in checking cacheable", zap.Error(err))
		return false
	}
	if tb.Meta().Partition != nil {
		return true
	}
	return false
}

// Leave implements Visitor interface.
func (checker *cacheableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.cacheable
}
