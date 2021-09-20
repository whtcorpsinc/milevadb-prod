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

	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// AdminPluginsInterDirc indicates AdminPlugins interlock.
type AdminPluginsInterDirc struct {
	baseInterlockingDirectorate
	CausetAction embedded.AdminPluginsCausetAction
	Plugins      []string
}

// Next implements the InterlockingDirectorate Next interface.
func (e *AdminPluginsInterDirc) Next(ctx context.Context, _ *chunk.Chunk) error {
	switch e.CausetAction {
	case embedded.Enable:
		return e.changeDisableFlagAndFlush(false)
	case embedded.Disable:
		return e.changeDisableFlagAndFlush(true)
	}
	return nil
}

func (e *AdminPluginsInterDirc) changeDisableFlagAndFlush(disabled bool) error {
	dom := petri.GetPetri(e.ctx)
	for _, pluginName := range e.Plugins {
		err := plugin.ChangeDisableFlagAndFlush(dom, pluginName, disabled)
		if err != nil {
			return err
		}
	}
	return nil
}
