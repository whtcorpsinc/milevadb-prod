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

package server

import (
	"context"
	"crypto/tls"

	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// IDriver opens IContext.
type IDriver interface {
	// OpenCtx opens an IContext with connection id, client capability, defCauslation, dbname and optionally the tls state.
	OpenCtx(connID uint64, capability uint32, defCauslation uint8, dbname string, tlsState *tls.ConnectionState) (*MilevaDBContext, error)
}

// PreparedStatement is the interface to use a prepared memex.
type PreparedStatement interface {
	// ID returns memex ID
	ID() int

	// InterDircute executes the memex.
	InterDircute(context.Context, []types.Causet) (ResultSet, error)

	// AppendParam appends parameter to the memex.
	AppendParam(paramID int, data []byte) error

	// NumParams returns number of parameters.
	NumParams() int

	// BoundParams returns bound parameters.
	BoundParams() [][]byte

	// SetParamsType sets type for parameters.
	SetParamsType([]byte)

	// GetParamsType returns the type for parameters.
	GetParamsType() []byte

	// StoreResultSet stores ResultSet for subsequent stmt fetching
	StoreResultSet(rs ResultSet)

	// GetResultSet gets ResultSet associated this memex
	GetResultSet() ResultSet

	// Reset removes all bound parameters.
	Reset()

	// Close closes the memex.
	Close() error
}

// ResultSet is the result set of an query.
type ResultSet interface {
	DeferredCausets() []*DeferredCausetInfo
	NewChunk() *chunk.Chunk
	Next(context.Context, *chunk.Chunk) error
	StoreFetchedRows(rows []chunk.Row)
	GetFetchedRows() []chunk.Row
	Close() error
}

// fetchNotifier represents notifier will be called in COM_FETCH.
type fetchNotifier interface {
	// OnFetchReturned be called when COM_FETCH returns.
	// it will be used in server-side cursor.
	OnFetchReturned()
}
