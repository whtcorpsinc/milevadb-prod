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

package sqlexec

import (
	"context"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// RestrictedALLEGROSQLInterlockingDirectorate is an interface provides executing restricted allegrosql memex.
// Why we need this interface?
// When we execute some management memexs, we need to operate system blocks.
// For example when executing create user memex, we need to check if the user already
// exists in the allegrosql.User causet and insert a new event if not exists. In this case, we need
// a convenience way to manipulate system blocks. The most simple way is executing allegrosql memex.
// In order to execute allegrosql memex in stmts package, we add this interface to solve dependence problem.
// And in the same time, we do not want this interface becomes a general way to run allegrosql memex.
// We hope this could be used with some restrictions such as only allowing system blocks as target,
// do not allowing recursion call.
// For more information please refer to the comments in stochastik.InterDircRestrictedALLEGROSQL().
// This is implemented in stochastik.go.
type RestrictedALLEGROSQLInterlockingDirectorate interface {
	// InterDircRestrictedALLEGROSQL run allegrosql memex in ctx with some restriction.
	InterDircRestrictedALLEGROSQL(allegrosql string) ([]chunk.Row, []*ast.ResultField, error)
	// InterDircRestrictedALLEGROSQLWithContext run allegrosql memex in ctx with some restriction.
	InterDircRestrictedALLEGROSQLWithContext(ctx context.Context, allegrosql string) ([]chunk.Row, []*ast.ResultField, error)
	// InterDircRestrictedALLEGROSQLWithSnapshot run allegrosql memex in ctx with some restriction and with snapshot.
	// If current stochastik sets the snapshot timestamp, then execute with this snapshot timestamp.
	// Otherwise, execute with the current transaction start timestamp if the transaction is valid.
	InterDircRestrictedALLEGROSQLWithSnapshot(allegrosql string) ([]chunk.Row, []*ast.ResultField, error)
}

// ALLEGROSQLInterlockingDirectorate is an interface provides executing normal allegrosql memex.
// Why we need this interface? To break circle dependence of packages.
// For example, privilege/privileges package need execute ALLEGROALLEGROSQL, if it use
// stochastik.Stochastik.InterDircute, then privilege/privileges and milevadb would become a circle.
type ALLEGROSQLInterlockingDirectorate interface {
	InterDircute(ctx context.Context, allegrosql string) ([]RecordSet, error)
	// InterDircuteInternal means execute allegrosql as the internal allegrosql.
	InterDircuteInternal(ctx context.Context, allegrosql string) ([]RecordSet, error)
}

// ALLEGROSQLBerolinaSQL is an interface provides parsing allegrosql memex.
// To parse a allegrosql memex, we could run BerolinaSQL.New() to get a BerolinaSQL object, and then run Parse method on it.
// But a stochastik already has a BerolinaSQL bind in it, so we define this interface and use stochastik as its implementation,
// thus avoid allocating new BerolinaSQL. See stochastik.ALLEGROSQLBerolinaSQL for more information.
type ALLEGROSQLBerolinaSQL interface {
	ParseALLEGROSQL(allegrosql, charset, defCauslation string) ([]ast.StmtNode, error)
}

// Statement is an interface for ALLEGROALLEGROSQL execution.
// NOTE: all Statement implementations must be safe for
// concurrent using by multiple goroutines.
// If the InterDirc method requires any InterDircution petri local data,
// they must be held out of the implementing instance.
type Statement interface {
	// OriginText gets the origin ALLEGROALLEGROSQL text.
	OriginText() string

	// GetTextToLog gets the desensitization ALLEGROALLEGROSQL text for logging.
	GetTextToLog() string

	// InterDirc executes ALLEGROALLEGROSQL and gets a Recordset.
	InterDirc(ctx context.Context) (RecordSet, error)

	// IsPrepared returns whether this memex is prepared memex.
	IsPrepared() bool

	// IsReadOnly returns if the memex is read only. For example: SelectStmt without dagger.
	IsReadOnly(vars *variable.StochastikVars) bool

	// RebuildCauset rebuilds the plan of the memex.
	RebuildCauset(ctx context.Context) (schemaVersion int64, err error)
}

// RecordSet is an abstract result set interface to help get data from Causet.
type RecordSet interface {
	// Fields gets result fields.
	Fields() []*ast.ResultField

	// Next reads records into chunk.
	Next(ctx context.Context, req *chunk.Chunk) error

	// NewChunk create a chunk.
	NewChunk() *chunk.Chunk

	// Close closes the underlying iterator, call Next after Close will
	// restart the iteration.
	Close() error
}

// MultiQueryNoDelayResult is an interface for one no-delay result for one memex in multi-queries.
type MultiQueryNoDelayResult interface {
	// AffectedRows return affected event for one memex in multi-queries.
	AffectedRows() uint64
	// LastMessage return last message for one memex in multi-queries.
	LastMessage() string
	// WarnCount return warn count for one memex in multi-queries.
	WarnCount() uint16
	// Status return status when executing one memex in multi-queries.
	Status() uint16
	// LastInsertID return last insert id for one memex in multi-queries.
	LastInsertID() uint64
}
