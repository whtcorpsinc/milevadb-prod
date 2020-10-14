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

package structure

import (
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/ekv"
)

var (
	// ErrInvalidHashKeyFlag used by structure
	ErrInvalidHashKeyFlag = terror.ClassStructure.New(allegrosql.ErrInvalidHashKeyFlag, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidHashKeyFlag])
	// ErrInvalidListIndex used by structure
	ErrInvalidListIndex = terror.ClassStructure.New(allegrosql.ErrInvalidListIndex, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidListIndex])
	// ErrInvalidListMetaData used by structure
	ErrInvalidListMetaData = terror.ClassStructure.New(allegrosql.ErrInvalidListMetaData, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrInvalidListMetaData])
	// ErrWriteOnSnapshot used by structure
	ErrWriteOnSnapshot = terror.ClassStructure.New(allegrosql.ErrWriteOnSnapshot, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrWriteOnSnapshot])
)

// NewStructure creates a TxStructure with Retriever, RetrieverMutator and key prefix.
func NewStructure(reader ekv.Retriever, readWriter ekv.RetrieverMutator, prefix []byte) *TxStructure {
	return &TxStructure{
		reader:     reader,
		readWriter: readWriter,
		prefix:     prefix,
	}
}

// TxStructure supports some simple data structures like string, hash, list, etc... and
// you can use these in a transaction.
type TxStructure struct {
	reader     ekv.Retriever
	readWriter ekv.RetrieverMutator
	prefix     []byte
}
