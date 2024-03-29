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
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

type maxMinFunction struct {
	aggFunction
	isMax bool
}

// GetResult implements Aggregation interface.
func (mmf *maxMinFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Causet) {
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (mmf *maxMinFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Causet {
	return []types.Causet{mmf.GetResult(evalCtx)}
}

// UFIDelate implements Aggregation interface.
func (mmf *maxMinFunction) UFIDelate(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, event chunk.Event) error {
	a := mmf.Args[0]
	value, err := a.Eval(event)
	if err != nil {
		return err
	}
	if evalCtx.Value.IsNull() {
		value.Copy(&evalCtx.Value)
	}
	if value.IsNull() {
		return nil
	}
	var c int
	c, err = evalCtx.Value.CompareCauset(sc, &value)
	if err != nil {
		return err
	}
	if (mmf.isMax && c == -1) || (!mmf.isMax && c == 1) {
		value.Copy(&evalCtx.Value)
	}
	return nil
}
