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

package mock

import (
	"context"

	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/ekv"
)

// CausetStore implements ekv.CausetStorage interface.
type CausetStore struct {
	Client ekv.Client
}

// GetClient implements ekv.CausetStorage interface.
func (s *CausetStore) GetClient() ekv.Client { return s.Client }

// GetOracle implements ekv.CausetStorage interface.
func (s *CausetStore) GetOracle() oracle.Oracle { return nil }

// Begin implements ekv.CausetStorage interface.
func (s *CausetStore) Begin() (ekv.Transaction, error) { return nil, nil }

// BeginWithStartTS implements ekv.CausetStorage interface.
func (s *CausetStore) BeginWithStartTS(startTS uint64) (ekv.Transaction, error) { return s.Begin() }

// GetSnapshot implements ekv.CausetStorage interface.
func (s *CausetStore) GetSnapshot(ver ekv.Version) (ekv.Snapshot, error) { return nil, nil }

// Close implements ekv.CausetStorage interface.
func (s *CausetStore) Close() error { return nil }

// UUID implements ekv.CausetStorage interface.
func (s *CausetStore) UUID() string { return "mock" }

// CurrentVersion implements ekv.CausetStorage interface.
func (s *CausetStore) CurrentVersion() (ekv.Version, error) { return ekv.Version{}, nil }

// SupportDeleteRange implements ekv.CausetStorage interface.
func (s *CausetStore) SupportDeleteRange() bool { return false }

// Name implements ekv.CausetStorage interface.
func (s *CausetStore) Name() string { return "UtilMockStorage" }

// Describe implements ekv.CausetStorage interface.
func (s *CausetStore) Describe() string {
	return "UtilMockStorage is a mock CausetStore implementation, only for unittests in soliton package"
}

// ShowStatus implements ekv.CausetStorage interface.
func (s *CausetStore) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, nil
}
