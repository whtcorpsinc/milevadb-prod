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
	"testing"

	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = check.Suite(&poolTestSuite{})

type poolTestSuite struct{}

func (s *poolTestSuite) TestNewPool(c *check.C) {
	pool := NewPool(1024)
	c.Assert(pool.initCap, check.Equals, 1024)
	c.Assert(pool.varLenDefCausPool, check.NotNil)
	c.Assert(pool.fixLenDefCausPool4, check.NotNil)
	c.Assert(pool.fixLenDefCausPool8, check.NotNil)
	c.Assert(pool.fixLenDefCausPool16, check.NotNil)
	c.Assert(pool.fixLenDefCausPool40, check.NotNil)
}

func (s *poolTestSuite) TestPoolGetChunk(c *check.C) {
	initCap := 1024
	pool := NewPool(initCap)

	fieldTypes := []*types.FieldType{
		{Tp: allegrosql.TypeVarchar},
		{Tp: allegrosql.TypeJSON},
		{Tp: allegrosql.TypeFloat},
		{Tp: allegrosql.TypeNewDecimal},
		{Tp: allegrosql.TypeDouble},
		{Tp: allegrosql.TypeLonglong},
		//{Tp: allegrosql.TypeTimestamp},
		//{Tp: allegrosql.TypeDatetime},
	}

	chk := pool.GetChunk(fieldTypes)
	c.Assert(chk, check.NotNil)
	c.Assert(chk.NumDefCauss(), check.Equals, len(fieldTypes))
	c.Assert(chk.defCausumns[0].elemBuf, check.IsNil)
	c.Assert(chk.defCausumns[1].elemBuf, check.IsNil)
	c.Assert(len(chk.defCausumns[2].elemBuf), check.Equals, getFixedLen(fieldTypes[2]))
	c.Assert(len(chk.defCausumns[3].elemBuf), check.Equals, getFixedLen(fieldTypes[3]))
	c.Assert(len(chk.defCausumns[4].elemBuf), check.Equals, getFixedLen(fieldTypes[4]))
	c.Assert(len(chk.defCausumns[5].elemBuf), check.Equals, getFixedLen(fieldTypes[5]))
	//c.Assert(len(chk.defCausumns[6].elemBuf), check.Equals, getFixedLen(fieldTypes[6]))
	//c.Assert(len(chk.defCausumns[7].elemBuf), check.Equals, getFixedLen(fieldTypes[7]))

	c.Assert(cap(chk.defCausumns[2].data), check.Equals, initCap*getFixedLen(fieldTypes[2]))
	c.Assert(cap(chk.defCausumns[3].data), check.Equals, initCap*getFixedLen(fieldTypes[3]))
	c.Assert(cap(chk.defCausumns[4].data), check.Equals, initCap*getFixedLen(fieldTypes[4]))
	c.Assert(cap(chk.defCausumns[5].data), check.Equals, initCap*getFixedLen(fieldTypes[5]))
	//c.Assert(cap(chk.defCausumns[6].data), check.Equals, initCap*getFixedLen(fieldTypes[6]))
	//c.Assert(cap(chk.defCausumns[7].data), check.Equals, initCap*getFixedLen(fieldTypes[7]))
}

func (s *poolTestSuite) TestPoolPutChunk(c *check.C) {
	initCap := 1024
	pool := NewPool(initCap)

	fieldTypes := []*types.FieldType{
		{Tp: allegrosql.TypeVarchar},
		{Tp: allegrosql.TypeJSON},
		{Tp: allegrosql.TypeFloat},
		{Tp: allegrosql.TypeNewDecimal},
		{Tp: allegrosql.TypeDouble},
		{Tp: allegrosql.TypeLonglong},
		{Tp: allegrosql.TypeTimestamp},
		{Tp: allegrosql.TypeDatetime},
	}

	chk := pool.GetChunk(fieldTypes)
	pool.PutChunk(fieldTypes, chk)
	c.Assert(len(chk.defCausumns), check.Equals, 0)
}

func BenchmarkPoolChunkOperation(b *testing.B) {
	pool := NewPool(1024)

	fieldTypes := []*types.FieldType{
		{Tp: allegrosql.TypeVarchar},
		{Tp: allegrosql.TypeJSON},
		{Tp: allegrosql.TypeFloat},
		{Tp: allegrosql.TypeNewDecimal},
		{Tp: allegrosql.TypeDouble},
		{Tp: allegrosql.TypeLonglong},
		{Tp: allegrosql.TypeTimestamp},
		{Tp: allegrosql.TypeDatetime},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.PutChunk(fieldTypes, pool.GetChunk(fieldTypes))
		}
	})
}
