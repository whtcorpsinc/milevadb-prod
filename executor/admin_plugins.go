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

package executor

import (
	"context"

	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// AdminPluginsExec indicates AdminPlugins executor.
type AdminPluginsExec struct {
	baseExecutor
	CausetAction  core.AdminPluginsCausetAction
	Plugins []string
}

// Next implements the Executor Next interface.
func (e *AdminPluginsExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	switch e.CausetAction {
	case core.Enable:
		return e.changeDisableFlagAndFlush(false)
	case core.Disable:
		return e.changeDisableFlagAndFlush(true)
	}
	return nil
}

func (e *AdminPluginsExec) changeDisableFlagAndFlush(disabled bool) error {
	dom := petri.GetPetri(e.ctx)
	for _, pluginName := range e.Plugins {
		err := plugin.ChangeDisableFlagAndFlush(dom, pluginName, disabled)
		if err != nil {
			return err
		}
	}
	return nil
}
