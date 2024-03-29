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

	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/telemetry"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// AdminShowTelemetryInterDirc is an interlock for ADMIN SHOW TELEMETRY.
type AdminShowTelemetryInterDirc struct {
	baseInterlockingDirectorate
	done bool
}

// Next implements the InterlockingDirectorate Next interface.
func (e *AdminShowTelemetryInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	dom := petri.GetPetri(e.ctx)
	id, err := telemetry.GetTrackingID(dom.GetEtcdClient())
	if err != nil {
		return err
	}
	status, err := telemetry.GetTelemetryStatus(dom.GetEtcdClient())
	if err != nil {
		return err
	}
	previewData, err := telemetry.PreviewUsageData(e.ctx, dom.GetEtcdClient())
	if err != nil {
		return err
	}
	req.AppendString(0, id)
	req.AppendString(1, status)
	req.AppendString(2, previewData)
	return nil
}

// AdminResetTelemetryIDInterDirc is an interlock for ADMIN RESET TELEMETRY_ID.
type AdminResetTelemetryIDInterDirc struct {
	baseInterlockingDirectorate
	done bool
}

// Next implements the InterlockingDirectorate Next interface.
func (e *AdminResetTelemetryIDInterDirc) Next(ctx context.Context, _ *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true
	dom := petri.GetPetri(e.ctx)
	_, err := telemetry.ResetTrackingID(dom.GetEtcdClient())
	return err
}
