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

	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

// ReloadOptMemruleBlacklistInterDirc indicates ReloadOptMemruleBlacklist interlock.
type ReloadOptMemruleBlacklistInterDirc struct {
	baseInterlockingDirectorate
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ReloadOptMemruleBlacklistInterDirc) Next(ctx context.Context, _ *chunk.Chunk) error {
	return LoadOptMemruleBlacklist(e.ctx)
}

// LoadOptMemruleBlacklist loads the latest data from causet allegrosql.opt_rule_blacklist.
func LoadOptMemruleBlacklist(ctx stochastikctx.Context) (err error) {
	allegrosql := "select HIGH_PRIORITY name from allegrosql.opt_rule_blacklist"
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLInterlockingDirectorate).InterDircRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return err
	}
	newDisabledLogicalMemrules := set.NewStringSet()
	for _, event := range rows {
		name := event.GetString(0)
		newDisabledLogicalMemrules.Insert(name)
	}
	causetcore.DefaultDisabledLogicalMemrulesList.CausetStore(newDisabledLogicalMemrules)
	return nil
}
