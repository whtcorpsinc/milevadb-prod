// Copyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package causet

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/ekv"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// Type , the type of causet, causetstore data in different ways.
type Type int16

const (
	// NormalBlock , causetstore data in einsteindb, mockeinsteindb and so on.
	NormalBlock Type = iota
	// VirtualBlock , causetstore no data, just extract data from the memory struct.
	VirtualBlock
	// ClusterBlock , contain the `VirtualBlock` in the all cluster milevadb nodes.
	ClusterBlock
)

// IsNormalBlock checks whether the causet is a normal causet type.
func (tp Type) IsNormalBlock() bool {
	return tp == NormalBlock
}

// IsVirtualBlock checks whether the causet is a virtual causet type.
func (tp Type) IsVirtualBlock() bool {
	return tp == VirtualBlock
}

// IsClusterBlock checks whether the causet is a cluster causet type.
func (tp Type) IsClusterBlock() bool {
	return tp == ClusterBlock
}

const (
	// DirtyBlockAddRow is the constant for dirty causet operation type.
	DirtyBlockAddRow = iota
	// DirtyBlockDeleteRow is the constant for dirty causet operation type.
	DirtyBlockDeleteRow
)

var (
	// ErrDeferredCausetCantNull is used for inserting null to a not null defCausumn.
	ErrDeferredCausetCantNull = terror.ClassBlock.New(allegrosql.ErrBadNull, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadNull])
	// ErrUnknownDeferredCauset is returned when accessing an unknown defCausumn.
	ErrUnknownDeferredCauset   = terror.ClassBlock.New(allegrosql.ErrBadField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBadField])
	errDuplicateDeferredCauset = terror.ClassBlock.New(allegrosql.ErrFieldSpecifiedTwice, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFieldSpecifiedTwice])

	errGetDefaultFailed = terror.ClassBlock.New(allegrosql.ErrFieldGetDefaultFailed, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrFieldGetDefaultFailed])

	// ErrNoDefaultValue is used when insert a event, the defCausumn value is not given, and the defCausumn has not null flag
	// and it doesn't have a default value.
	ErrNoDefaultValue = terror.ClassBlock.New(allegrosql.ErrNoDefaultForField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoDefaultForField])
	// ErrIndexOutBound returns for index defCausumn offset out of bound.
	ErrIndexOutBound = terror.ClassBlock.New(allegrosql.ErrIndexOutBound, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrIndexOutBound])
	// ErrUnsupportedOp returns for unsupported operation.
	ErrUnsupportedOp = terror.ClassBlock.New(allegrosql.ErrUnsupportedOp, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnsupportedOp])
	// ErrRowNotFound returns for event not found.
	ErrRowNotFound = terror.ClassBlock.New(allegrosql.ErrRowNotFound, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrRowNotFound])
	// ErrBlockStateCantNone returns for causet none state.
	ErrBlockStateCantNone = terror.ClassBlock.New(allegrosql.ErrBlockStateCantNone, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrBlockStateCantNone])
	// ErrDeferredCausetStateCantNone returns for defCausumn none state.
	ErrDeferredCausetStateCantNone = terror.ClassBlock.New(allegrosql.ErrDeferredCausetStateCantNone, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDeferredCausetStateCantNone])
	// ErrDeferredCausetStateNonPublic returns for defCausumn non-public state.
	ErrDeferredCausetStateNonPublic = terror.ClassBlock.New(allegrosql.ErrDeferredCausetStateNonPublic, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrDeferredCausetStateNonPublic])
	// ErrIndexStateCantNone returns for index none state.
	ErrIndexStateCantNone = terror.ClassBlock.New(allegrosql.ErrIndexStateCantNone, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrIndexStateCantNone])
	// ErrInvalidRecordKey returns for invalid record key.
	ErrInvalidRecordKey = terror.ClassBlock.New(allegrosql.ErrInvalidRecordKey, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidRecordKey])
	// ErrTruncatedWrongValueForField returns for truncate wrong value for field.
	ErrTruncatedWrongValueForField = terror.ClassBlock.New(allegrosql.ErrTruncatedWrongValueForField, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrTruncatedWrongValueForField])
	// ErrUnknownPartition returns unknown partition error.
	ErrUnknownPartition = terror.ClassBlock.New(allegrosql.ErrUnknownPartition, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrUnknownPartition])
	// ErrNoPartitionForGivenValue returns causet has no partition for value.
	ErrNoPartitionForGivenValue = terror.ClassBlock.New(allegrosql.ErrNoPartitionForGivenValue, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrNoPartitionForGivenValue])
	// ErrLockOrActiveTransaction returns when execute unsupported memex in a dagger stochastik or an active transaction.
	ErrLockOrActiveTransaction = terror.ClassBlock.New(allegrosql.ErrLockOrActiveTransaction, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrLockOrActiveTransaction])
	// ErrSequenceHasRunOut returns when sequence has run out.
	ErrSequenceHasRunOut = terror.ClassBlock.New(allegrosql.ErrSequenceRunOut, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrSequenceRunOut])
	// ErrRowDoesNotMatchGivenPartitionSet returns when the destination partition conflict with the partition selection.
	ErrRowDoesNotMatchGivenPartitionSet = terror.ClassBlock.NewStd(allegrosql.ErrRowDoesNotMatchGivenPartitionSet)
)

// RecordIterFunc is used for low-level record iteration.
type RecordIterFunc func(h ekv.Handle, rec []types.Causet, defcaus []*DeferredCauset) (more bool, err error)

// AddRecordOpt contains the options will be used when adding a record.
type AddRecordOpt struct {
	CreateIdxOpt
	IsUFIDelate   bool
	ReserveAutoID int
}

// AddRecordOption is defined for the AddRecord() method of the Block interface.
type AddRecordOption interface {
	ApplyOn(*AddRecordOpt)
}

// WithReserveAutoIDHint tells the AddRecord operation to reserve a batch of auto ID in the stmtctx.
type WithReserveAutoIDHint int

// ApplyOn implements the AddRecordOption interface.
func (n WithReserveAutoIDHint) ApplyOn(opt *AddRecordOpt) {
	opt.ReserveAutoID = int(n)
}

// ApplyOn implements the AddRecordOption interface, so any CreateIdxOptFunc
// can be passed as the optional argument to the causet.AddRecord method.
func (f CreateIdxOptFunc) ApplyOn(opt *AddRecordOpt) {
	f(&opt.CreateIdxOpt)
}

// IsUFIDelate is a defined value for AddRecordOptFunc.
var IsUFIDelate AddRecordOption = isUFIDelate{}

type isUFIDelate struct{}

func (i isUFIDelate) ApplyOn(opt *AddRecordOpt) {
	opt.IsUFIDelate = true
}

// Block is used to retrieve and modify rows in causet.
type Block interface {
	// IterRecords iterates records in the causet and calls fn.
	IterRecords(ctx stochastikctx.Context, startKey ekv.Key, defcaus []*DeferredCauset, fn RecordIterFunc) error

	// RowWithDefCauss returns a event that contains the given defcaus.
	RowWithDefCauss(ctx stochastikctx.Context, h ekv.Handle, defcaus []*DeferredCauset) ([]types.Causet, error)

	// Row returns a event for all defCausumns.
	Row(ctx stochastikctx.Context, h ekv.Handle) ([]types.Causet, error)

	// DefCauss returns the defCausumns of the causet which is used in select, including hidden defCausumns.
	DefCauss() []*DeferredCauset

	// VisibleDefCauss returns the defCausumns of the causet which is used in select, excluding hidden defCausumns.
	VisibleDefCauss() []*DeferredCauset

	// HiddenDefCauss returns the hidden defCausumns of the causet.
	HiddenDefCauss() []*DeferredCauset

	// WriblockDefCauss returns defCausumns of the causet in wriblock states.
	// Wriblock states includes Public, WriteOnly, WriteOnlyReorganization.
	WriblockDefCauss() []*DeferredCauset

	// FullHiddenDefCaussAndVisibleDefCauss returns hidden defCausumns in all states and unhidden defCausumns in public states.
	FullHiddenDefCaussAndVisibleDefCauss() []*DeferredCauset

	// Indices returns the indices of the causet.
	Indices() []Index

	// WriblockIndices returns write-only and public indices of the causet.
	WriblockIndices() []Index

	// DeleblockIndices returns delete-only, write-only and public indices of the causet.
	DeleblockIndices() []Index

	// RecordPrefix returns the record key prefix.
	RecordPrefix() ekv.Key

	// IndexPrefix returns the index key prefix.
	IndexPrefix() ekv.Key

	// FirstKey returns the first key.
	FirstKey() ekv.Key

	// RecordKey returns the key in KV storage for the event.
	RecordKey(h ekv.Handle) ekv.Key

	// AddRecord inserts a event which should contain only public defCausumns
	AddRecord(ctx stochastikctx.Context, r []types.Causet, opts ...AddRecordOption) (recordID ekv.Handle, err error)

	// UFIDelateRecord uFIDelates a event which should contain only wriblock defCausumns.
	UFIDelateRecord(ctx context.Context, sctx stochastikctx.Context, h ekv.Handle, currData, newData []types.Causet, touched []bool) error

	// RemoveRecord removes a event in the causet.
	RemoveRecord(ctx stochastikctx.Context, h ekv.Handle, r []types.Causet) error

	// SlabPredictors returns all allocators.
	SlabPredictors(ctx stochastikctx.Context) autoid.SlabPredictors

	// RebaseAutoID rebases the auto_increment ID base.
	// If allocIDs is true, it will allocate some IDs and save to the cache.
	// If allocIDs is false, it will not allocate IDs.
	RebaseAutoID(ctx stochastikctx.Context, newBase int64, allocIDs bool, tp autoid.SlabPredictorType) error

	// Meta returns BlockInfo.
	Meta() *perceptron.BlockInfo

	// Seek returns the handle greater or equal to h.
	Seek(ctx stochastikctx.Context, h ekv.Handle) (handle ekv.Handle, found bool, err error)

	// Type returns the type of causet
	Type() Type
}

// AllocAutoIncrementValue allocates an auto_increment value for a new event.
func AllocAutoIncrementValue(ctx context.Context, t Block, sctx stochastikctx.Context) (int64, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("causet.AllocAutoIncrementValue", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	increment := sctx.GetStochastikVars().AutoIncrementIncrement
	offset := sctx.GetStochastikVars().AutoIncrementOffset
	_, max, err := t.SlabPredictors(sctx).Get(autoid.RowIDAllocType).Alloc(t.Meta().ID, uint64(1), int64(increment), int64(offset))
	if err != nil {
		return 0, err
	}
	return max, err
}

// AllocBatchAutoIncrementValue allocates batch auto_increment value for rows, returning firstID, increment and err.
// The caller can derive the autoID by adding increment to firstID for N-1 times.
func AllocBatchAutoIncrementValue(ctx context.Context, t Block, sctx stochastikctx.Context, N int) (firstID int64, increment int64, err error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("causet.AllocBatchAutoIncrementValue", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	increment = int64(sctx.GetStochastikVars().AutoIncrementIncrement)
	offset := int64(sctx.GetStochastikVars().AutoIncrementOffset)
	min, max, err := t.SlabPredictors(sctx).Get(autoid.RowIDAllocType).Alloc(t.Meta().ID, uint64(N), increment, offset)
	if err != nil {
		return min, max, err
	}
	// SeekToFirstAutoIDUnSigned seeks to first autoID. Because AutoIncrement always allocate from 1,
	// signed and unsigned value can be unified as the unsigned handle.
	nr := int64(autoid.SeekToFirstAutoIDUnSigned(uint64(min), uint64(increment), uint64(offset)))
	return nr, increment, nil
}

// PhysicalBlock is an abstraction for two HoTTs of causet representation: partition or non-partitioned causet.
// PhysicalID is a ID that can be used to construct a key ranges, all the data in the key range belongs to the corresponding PhysicalBlock.
// For a non-partitioned causet, its PhysicalID equals to its BlockID; For a partition of a partitioned causet, its PhysicalID is the partition's ID.
type PhysicalBlock interface {
	Block
	GetPhysicalID() int64
}

// PartitionedBlock is a Block, and it has a GetPartition() method.
// GetPartition() gets the partition from a partition causet by a physical causet ID,
type PartitionedBlock interface {
	Block
	GetPartition(physicalID int64) PhysicalBlock
	GetPartitionByRow(stochastikctx.Context, []types.Causet) (PhysicalBlock, error)
}

// BlockFromMeta builds a causet.Block from *perceptron.BlockInfo.
// Currently, it is assigned to blocks.BlockFromMeta in milevadb package's init function.
var BlockFromMeta func(allocators autoid.SlabPredictors, tblInfo *perceptron.BlockInfo) (Block, error)

// MockBlockFromMeta only serves for test.
var MockBlockFromMeta func(blockInfo *perceptron.BlockInfo) Block
