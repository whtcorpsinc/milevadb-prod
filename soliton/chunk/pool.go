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

package chunk

import (
	"sync"

	"github.com/whtcorpsinc/milevadb/types"
)

// Pool is the DeferredCauset pool.
// NOTE: Pool is non-copyable.
type Pool struct {
	initCap int

	varLenDefCausPool   *sync.Pool
	fixLenDefCausPool4  *sync.Pool
	fixLenDefCausPool8  *sync.Pool
	fixLenDefCausPool16 *sync.Pool
	fixLenDefCausPool40 *sync.Pool
}

// NewPool creates a new Pool.
func NewPool(initCap int) *Pool {
	return &Pool{
		initCap:         initCap,
		varLenDefCausPool:   &sync.Pool{New: func() interface{} { return newVarLenDeferredCauset(initCap, nil) }},
		fixLenDefCausPool4:  &sync.Pool{New: func() interface{} { return newFixedLenDeferredCauset(4, initCap) }},
		fixLenDefCausPool8:  &sync.Pool{New: func() interface{} { return newFixedLenDeferredCauset(8, initCap) }},
		fixLenDefCausPool16: &sync.Pool{New: func() interface{} { return newFixedLenDeferredCauset(16, initCap) }},
		fixLenDefCausPool40: &sync.Pool{New: func() interface{} { return newFixedLenDeferredCauset(40, initCap) }},
	}
}

// GetChunk gets a Chunk from the Pool.
func (p *Pool) GetChunk(fields []*types.FieldType) *Chunk {
	chk := new(Chunk)
	chk.capacity = p.initCap
	chk.defCausumns = make([]*DeferredCauset, len(fields))
	for i, f := range fields {
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			chk.defCausumns[i] = p.varLenDefCausPool.Get().(*DeferredCauset)
		case 4:
			chk.defCausumns[i] = p.fixLenDefCausPool4.Get().(*DeferredCauset)
		case 8:
			chk.defCausumns[i] = p.fixLenDefCausPool8.Get().(*DeferredCauset)
		case 16:
			chk.defCausumns[i] = p.fixLenDefCausPool16.Get().(*DeferredCauset)
		case 40:
			chk.defCausumns[i] = p.fixLenDefCausPool40.Get().(*DeferredCauset)
		}
	}
	return chk
}

// PutChunk puts a Chunk back to the Pool.
func (p *Pool) PutChunk(fields []*types.FieldType, chk *Chunk) {
	for i, f := range fields {
		switch elemLen := getFixedLen(f); elemLen {
		case varElemLen:
			p.varLenDefCausPool.Put(chk.defCausumns[i])
		case 4:
			p.fixLenDefCausPool4.Put(chk.defCausumns[i])
		case 8:
			p.fixLenDefCausPool8.Put(chk.defCausumns[i])
		case 16:
			p.fixLenDefCausPool16.Put(chk.defCausumns[i])
		case 40:
			p.fixLenDefCausPool40.Put(chk.defCausumns[i])
		}
	}
	chk.defCausumns = nil // release the DeferredCauset references.
}
