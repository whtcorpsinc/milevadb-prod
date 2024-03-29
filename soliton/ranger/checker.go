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
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
)

// conditionChecker checks if this condition can be pushed to index causet.
type conditionChecker struct {
	defCausUniqueID   int64
	shouldReserve bool // check if a access condition should be reserved in filter conditions.
	length        int
}

func (c *conditionChecker) check(condition memex.Expression) bool {
	switch x := condition.(type) {
	case *memex.ScalarFunction:
		return c.checkScalarFunction(x)
	case *memex.DeferredCauset:
		if x.RetType.EvalType() == types.ETString {
			return false
		}
		return c.checkDeferredCauset(x)
	case *memex.Constant:
		return true
	}
	return false
}

func (c *conditionChecker) checkScalarFunction(scalar *memex.ScalarFunction) bool {
	_, defCauslation := scalar.CharsetAndDefCauslation(scalar.GetCtx())
	switch scalar.FuncName.L {
	case ast.LogicOr, ast.LogicAnd:
		return c.check(scalar.GetArgs()[0]) && c.check(scalar.GetArgs()[1])
	case ast.EQ, ast.NE, ast.GE, ast.GT, ast.LE, ast.LT, ast.NullEQ:
		if _, ok := scalar.GetArgs()[0].(*memex.Constant); ok {
			if c.checkDeferredCauset(scalar.GetArgs()[1]) {
				// Checks whether the scalar function is calculated use the defCauslation compatible with the defCausumn.
				if scalar.GetArgs()[1].GetType().EvalType() == types.ETString && !defCauslate.CompatibleDefCauslate(scalar.GetArgs()[1].GetType().DefCauslate, defCauslation) {
					return false
				}
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
		if _, ok := scalar.GetArgs()[1].(*memex.Constant); ok {
			if c.checkDeferredCauset(scalar.GetArgs()[0]) {
				// Checks whether the scalar function is calculated use the defCauslation compatible with the defCausumn.
				if scalar.GetArgs()[0].GetType().EvalType() == types.ETString && !defCauslate.CompatibleDefCauslate(scalar.GetArgs()[0].GetType().DefCauslate, defCauslation) {
					return false
				}
				return scalar.FuncName.L != ast.NE || c.length == types.UnspecifiedLength
			}
		}
	case ast.IsNull:
		return c.checkDeferredCauset(scalar.GetArgs()[0])
	case ast.IsTruthWithoutNull, ast.IsFalsity, ast.IsTruthWithNull:
		if s, ok := scalar.GetArgs()[0].(*memex.DeferredCauset); ok {
			if s.RetType.EvalType() == types.ETString {
				return false
			}
		}
		return c.checkDeferredCauset(scalar.GetArgs()[0])
	case ast.UnaryNot:
		// TODO: support "not like" convert to access conditions.
		if s, ok := scalar.GetArgs()[0].(*memex.ScalarFunction); ok {
			if s.FuncName.L == ast.Like {
				return false
			}
		} else {
			// "not defCausumn" or "not constant" can't lead to a range.
			return false
		}
		return c.check(scalar.GetArgs()[0])
	case ast.In:
		if !c.checkDeferredCauset(scalar.GetArgs()[0]) {
			return false
		}
		if scalar.GetArgs()[1].GetType().EvalType() == types.ETString && !defCauslate.CompatibleDefCauslate(scalar.GetArgs()[0].GetType().DefCauslate, defCauslation) {
			return false
		}
		for _, v := range scalar.GetArgs()[1:] {
			if _, ok := v.(*memex.Constant); !ok {
				return false
			}
		}
		return true
	case ast.Like:
		return c.checkLikeFunc(scalar)
	case ast.GetParam:
		return true
	}
	return false
}

func (c *conditionChecker) checkLikeFunc(scalar *memex.ScalarFunction) bool {
	_, defCauslation := scalar.CharsetAndDefCauslation(scalar.GetCtx())
	if !defCauslate.CompatibleDefCauslate(scalar.GetArgs()[0].GetType().DefCauslate, defCauslation) {
		return false
	}
	if !c.checkDeferredCauset(scalar.GetArgs()[0]) {
		return false
	}
	pattern, ok := scalar.GetArgs()[1].(*memex.Constant)
	if !ok {
		return false

	}
	if pattern.Value.IsNull() {
		return false
	}
	patternStr, err := pattern.Value.ToString()
	if err != nil {
		return false
	}
	if len(patternStr) == 0 {
		return true
	}
	escape := byte(scalar.GetArgs()[2].(*memex.Constant).Value.GetInt64())
	for i := 0; i < len(patternStr); i++ {
		if patternStr[i] == escape {
			i++
			if i < len(patternStr)-1 {
				continue
			}
			break
		}
		if i == 0 && (patternStr[i] == '%' || patternStr[i] == '_') {
			return false
		}
		if patternStr[i] == '%' {
			if i != len(patternStr)-1 {
				c.shouldReserve = true
			}
			break
		}
		if patternStr[i] == '_' {
			c.shouldReserve = true
			break
		}
	}
	return true
}

func (c *conditionChecker) checkDeferredCauset(expr memex.Expression) bool {
	defCaus, ok := expr.(*memex.DeferredCauset)
	if !ok {
		return false
	}
	return c.defCausUniqueID == defCaus.UniqueID
}
