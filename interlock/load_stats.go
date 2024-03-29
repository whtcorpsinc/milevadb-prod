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

package interlock

import (
	"context"
	"encoding/json"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

var _ InterlockingDirectorate = &LoadStatsInterDirc{}

// LoadStatsInterDirc represents a load statistic interlock.
type LoadStatsInterDirc struct {
	baseInterlockingDirectorate
	info *LoadStatsInfo
}

// LoadStatsInfo saves the information of loading statistic operation.
type LoadStatsInfo struct {
	Path string
	Ctx  stochastikctx.Context
}

// loadStatsVarKeyType is a dummy type to avoid naming defCauslision in context.
type loadStatsVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadStatsVarKeyType) String() string {
	return "load_stats_var"
}

// LoadStatsVarKey is a variable key for load statistic.
const LoadStatsVarKey loadStatsVarKeyType = 0

// Next implements the InterlockingDirectorate Next interface.
func (e *LoadStatsInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if len(e.info.Path) == 0 {
		return errors.New("Load Stats: file path is empty")
	}
	val := e.ctx.Value(LoadStatsVarKey)
	if val != nil {
		e.ctx.SetValue(LoadStatsVarKey, nil)
		return errors.New("Load Stats: previous load stats option isn't closed normally")
	}
	e.ctx.SetValue(LoadStatsVarKey, e.info)
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *LoadStatsInterDirc) Close() error {
	return nil
}

// Open implements the InterlockingDirectorate Open interface.
func (e *LoadStatsInterDirc) Open(ctx context.Context) error {
	return nil
}

// UFIDelate uFIDelates the stats of the corresponding causet according to the data.
func (e *LoadStatsInfo) UFIDelate(data []byte) error {
	jsonTbl := &handle.JSONBlock{}
	if err := json.Unmarshal(data, jsonTbl); err != nil {
		return errors.Trace(err)
	}
	do := petri.GetPetri(e.Ctx)
	h := do.StatsHandle()
	if h == nil {
		return errors.New("Load Stats: handle is nil")
	}
	return h.LoadStatsFromJSON(schemareplicant.GetSchemaReplicant(e.Ctx), jsonTbl)
}
