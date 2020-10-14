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

package generatedexpr

import (
	"fmt"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/soliton"
)

// nameResolver is the visitor to resolve block name and defCausumn name.
// it combines BlockInfo and DeferredCausetInfo to a generation expression.
type nameResolver struct {
	blockInfo *perceptron.BlockInfo
	err       error
}

// Enter implements ast.Visitor interface.
func (nr *nameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (nr *nameResolver) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch v := inNode.(type) {
	case *ast.DeferredCausetNameExpr:
		for _, defCaus := range nr.blockInfo.DeferredCausets {
			if defCaus.Name.L == v.Name.Name.L {
				v.Refer = &ast.ResultField{
					DeferredCauset: defCaus,
					Block:  nr.blockInfo,
				}
				return inNode, true
			}
		}
		nr.err = errors.Errorf("can't find defCausumn %s in %s", v.Name.Name.O, nr.blockInfo.Name.O)
		return inNode, false
	}
	return inNode, true
}

// ParseExpression parses an ExprNode from a string.
// When MilevaDB loads schemareplicant from EinsteinDB, `GeneratedExprString`
// of `DeferredCausetInfo` is a string field, so we need to parse
// it into ast.ExprNode. This function is for that.
func ParseExpression(expr string) (node ast.ExprNode, err error) {
	expr = fmt.Sprintf("select %s", expr)
	charset, defCauslation := charset.GetDefaultCharsetAndDefCauslate()
	stmts, _, err := BerolinaSQL.New().Parse(expr, charset, defCauslation)
	if err == nil {
		node = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	return node, soliton.SyntaxError(err)
}

// SimpleResolveName resolves all defCausumn names in the expression node.
func SimpleResolveName(node ast.ExprNode, tblInfo *perceptron.BlockInfo) (ast.ExprNode, error) {
	nr := nameResolver{tblInfo, nil}
	if _, ok := node.Accept(&nr); !ok {
		return nil, errors.Trace(nr.err)
	}
	return node, nil
}
