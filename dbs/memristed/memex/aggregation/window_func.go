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

package aggregation

import (
	"strings"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// WindowFuncDesc describes a window function signature, only used in causet.
type WindowFuncDesc struct {
	baseFuncDesc
}

// NewWindowFuncDesc creates a window function signature descriptor.
func NewWindowFuncDesc(ctx stochastikctx.Context, name string, args []memex.Expression) (*WindowFuncDesc, error) {
	switch strings.ToLower(name) {
	case ast.WindowFuncNthValue:
		val, isNull, ok := memex.GetUint64FromConstant(args[1])
		// nth_value does not allow `0`, but allows `null`.
		if !ok || (val == 0 && !isNull) {
			return nil, nil
		}
	case ast.WindowFuncNtile:
		val, isNull, ok := memex.GetUint64FromConstant(args[0])
		// ntile does not allow `0`, but allows `null`.
		if !ok || (val == 0 && !isNull) {
			return nil, nil
		}
	case ast.WindowFuncLead, ast.WindowFuncLag:
		if len(args) < 2 {
			break
		}
		_, isNull, ok := memex.GetUint64FromConstant(args[1])
		if !ok || isNull {
			return nil, nil
		}
	}
	base, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &WindowFuncDesc{base}, nil
}

// noFrameWindowFuncs is the functions that operate on the entire partition,
// they should not have frame specifications.
var noFrameWindowFuncs = map[string]struct{}{
	ast.WindowFuncCumeDist:    {},
	ast.WindowFuncDenseRank:   {},
	ast.WindowFuncLag:         {},
	ast.WindowFuncLead:        {},
	ast.WindowFuncNtile:       {},
	ast.WindowFuncPercentRank: {},
	ast.WindowFuncRank:        {},
	ast.WindowFuncEventNumber:   {},
}

// NeedFrame checks if the function need frame specification.
func NeedFrame(name string) bool {
	_, ok := noFrameWindowFuncs[strings.ToLower(name)]
	return !ok
}
