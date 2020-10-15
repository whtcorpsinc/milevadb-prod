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

package execdetails

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	fidel "github.com/einsteindb/fidel/client"
)

var (
	_ fidel.Client   = &InterceptedFIDelClient{}
	_ fidel.TSFuture = &interceptedTsFuture{}
)

func recordFIDelWaitTime(ctx context.Context, start time.Time) {
	stmtInterDirc := ctx.Value(StmtInterDircDetailKey)
	if stmtInterDirc != nil {
		detail := stmtInterDirc.(*StmtInterDircDetails)
		atomic.AddInt64(&detail.WaitFIDelResFIDeluration, int64(time.Since(start)))
	}
}

// InterceptedFIDelClient is a FIDel's wrapper client to record stmt detail.
type InterceptedFIDelClient struct {
	fidel.Client
}

// interceptedTsFuture is a FIDel's wrapper future to record stmt detail.
type interceptedTsFuture struct {
	fidel.TSFuture
	ctx context.Context
}

// Wait implements fidel.Client#Wait.
func (m interceptedTsFuture) Wait() (int64, int64, error) {
	start := time.Now()
	physical, logical, err := m.TSFuture.Wait()
	recordFIDelWaitTime(m.ctx, start)
	return physical, logical, err
}

// GetTS implements fidel.Client#GetTS.
func (m InterceptedFIDelClient) GetTS(ctx context.Context) (int64, int64, error) {
	start := time.Now()
	physical, logical, err := m.Client.GetTS(ctx)
	recordFIDelWaitTime(ctx, start)
	return physical, logical, err
}

// GetTSAsync implements fidel.Client#GetTSAsync.
func (m InterceptedFIDelClient) GetTSAsync(ctx context.Context) fidel.TSFuture {
	start := time.Now()
	f := m.Client.GetTSAsync(ctx)
	recordFIDelWaitTime(ctx, start)
	return interceptedTsFuture{
		ctx:      ctx,
		TSFuture: f,
	}
}

// GetRegion implements fidel.Client#GetRegion.
func (m InterceptedFIDelClient) GetRegion(ctx context.Context, key []byte) (*fidel.Region, error) {
	start := time.Now()
	r, err := m.Client.GetRegion(ctx, key)
	recordFIDelWaitTime(ctx, start)
	return r, err
}

// GetPrevRegion implements fidel.Client#GetPrevRegion.
func (m InterceptedFIDelClient) GetPrevRegion(ctx context.Context, key []byte) (*fidel.Region, error) {
	start := time.Now()
	r, err := m.Client.GetRegion(ctx, key)
	recordFIDelWaitTime(ctx, start)
	return r, err
}

// GetRegionByID implements fidel.Client#GetRegionByID.
func (m InterceptedFIDelClient) GetRegionByID(ctx context.Context, regionID uint64) (*fidel.Region, error) {
	start := time.Now()
	r, err := m.Client.GetRegionByID(ctx, regionID)
	recordFIDelWaitTime(ctx, start)
	return r, err
}

// ScanRegions implements fidel.Client#ScanRegions.
func (m InterceptedFIDelClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*fidel.Region, error) {
	start := time.Now()
	r, err := m.Client.ScanRegions(ctx, key, endKey, limit)
	recordFIDelWaitTime(ctx, start)
	return r, err
}

// GetStore implements fidel.Client#GetStore.
func (m InterceptedFIDelClient) GetStore(ctx context.Context, storeID uint64) (*spacetimepb.CausetStore, error) {
	start := time.Now()
	s, err := m.Client.GetStore(ctx, storeID)
	recordFIDelWaitTime(ctx, start)
	return s, err
}
