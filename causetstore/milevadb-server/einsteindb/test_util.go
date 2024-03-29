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

package einsteindb

import (
	"github.com/einsteindb/fidel/client"
	"github.com/google/uuid"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
)

// NewTestEinsteinDBStore creates a test causetstore with Option
func NewTestEinsteinDBStore(client Client, FIDelClient fidel.Client, clientHijack func(Client) Client, FIDelClientHijack func(fidel.Client) fidel.Client, txnLocalLatches uint) (ekv.CausetStorage, error) {
	if clientHijack != nil {
		client = clientHijack(client)
	}

	FIDelCli := fidel.Client(&codecFIDelClient{FIDelClient})
	if FIDelClientHijack != nil {
		FIDelCli = FIDelClientHijack(FIDelCli)
	}

	// Make sure the uuid is unique.
	uid := uuid.New().String()
	spekv := NewMockSafePointKV()
	einsteindbStore, err := newEinsteinDBStore(uid, FIDelCli, spekv, client, false, nil)

	if txnLocalLatches > 0 {
		einsteindbStore.EnableTxnLocalLatches(txnLocalLatches)
	}

	einsteindbStore.mock = true
	return einsteindbStore, errors.Trace(err)
}
