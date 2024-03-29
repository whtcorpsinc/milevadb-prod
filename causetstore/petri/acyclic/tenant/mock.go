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

package tenant

import (
	"context"
	"sync/atomic"

	"github.com/whtcorpsinc/errors"
)

var _ Manager = &mockManager{}

// mockManager represents the structure which is used for electing tenant.
// It's used for local causetstore and testing.
// So this worker will always be the tenant.
type mockManager struct {
	tenant  int32
	id     string // id is the ID of manager.
	cancel context.CancelFunc
}

// NewMockManager creates a new mock Manager.
func NewMockManager(ctx context.Context, id string) Manager {
	_, cancelFunc := context.WithCancel(ctx)
	return &mockManager{
		id:     id,
		cancel: cancelFunc,
	}
}

// ID implements Manager.ID interface.
func (m *mockManager) ID() string {
	return m.id
}

// IsTenant implements Manager.IsTenant interface.
func (m *mockManager) IsTenant() bool {
	return atomic.LoadInt32(&m.tenant) == 1
}

func (m *mockManager) toBeTenant() {
	atomic.StoreInt32(&m.tenant, 1)
}

// RetireTenant implements Manager.RetireTenant interface.
func (m *mockManager) RetireTenant() {
	atomic.StoreInt32(&m.tenant, 0)
}

// Cancel implements Manager.Cancel interface.
func (m *mockManager) Cancel() {
	m.cancel()
}

// GetTenantID implements Manager.GetTenantID interface.
func (m *mockManager) GetTenantID(ctx context.Context) (string, error) {
	if m.IsTenant() {
		return m.ID(), nil
	}
	return "", errors.New("no tenant")
}

// CampaignTenant implements Manager.CampaignTenant interface.
func (m *mockManager) CampaignTenant() error {
	m.toBeTenant()
	return nil
}

// ResignTenant lets the tenant start a new election.
func (m *mockManager) ResignTenant(ctx context.Context) error {
	if m.IsTenant() {
		m.RetireTenant()
	}
	return nil
}
