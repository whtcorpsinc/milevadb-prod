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

package allegrosql

import (
	"context"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

func (s *testSuite) TestUFIDelateCopRuntimeStats(c *C) {
	ctx := mock.NewContext()
	ctx.GetStochastikVars().StmtCtx = new(stmtctx.StatementContext)
	sr := selectResult{ctx: ctx}
	c.Assert(ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl, IsNil)
	sr.rootCausetID = 1234
	sr.uFIDelateCopRuntimeStats(context.Background(), &einsteindb.CopRuntimeStats{InterDircDetails: execdetails.InterDircDetails{CalleeAddress: "a"}}, 0)

	ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl = execdetails.NewRuntimeStatsDefCausl()
	t := uint64(1)
	sr.selectResp = &fidelpb.SelectResponse{
		InterDircutionSummaries: []*fidelpb.InterlockingDirectorateInterDircutionSummary{
			{TimeProcessedNs: &t, NumProducedRows: &t, NumIterations: &t},
		},
	}
	c.Assert(len(sr.selectResp.GetInterDircutionSummaries()) != len(sr.copCausetIDs), IsTrue)
	sr.uFIDelateCopRuntimeStats(context.Background(), &einsteindb.CopRuntimeStats{InterDircDetails: execdetails.InterDircDetails{CalleeAddress: "callee"}}, 0)
	c.Assert(ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.ExistsCopStats(1234), IsFalse)

	sr.copCausetIDs = []int{sr.rootCausetID}
	c.Assert(ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl, NotNil)
	c.Assert(len(sr.selectResp.GetInterDircutionSummaries()), Equals, len(sr.copCausetIDs))
	sr.uFIDelateCopRuntimeStats(context.Background(), &einsteindb.CopRuntimeStats{InterDircDetails: execdetails.InterDircDetails{CalleeAddress: "callee"}}, 0)
	c.Assert(ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.GetCopStats(1234).String(), Equals, "time:1ns, loops:1")
}
