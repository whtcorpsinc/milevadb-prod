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

package statistics

import (
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

var _ = Suite(&testSampleSuite{})

type testSampleSuite struct {
	count int
	rs    sqlexec.RecordSet
}

func (s *testSampleSuite) SetUpSuite(c *C) {
	s.count = 10000
	rs := &recordSet{
		data:      make([]types.Causet, s.count),
		count:     s.count,
		cursor:    0,
		firstIsID: true,
	}
	rs.setFields(allegrosql.TypeLonglong, allegrosql.TypeLonglong)
	start := 1000 // 1000 values is null
	for i := start; i < rs.count; i++ {
		rs.data[i].SetInt64(int64(i))
	}
	for i := start; i < rs.count; i += 3 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 1)
	}
	for i := start; i < rs.count; i += 5 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 2)
	}
	s.rs = rs
}

func (s *testSampleSuite) TestDefCauslectDeferredCausetStats(c *C) {
	sc := mock.NewContext().GetStochastikVars().StmtCtx
	builder := SampleBuilder{
		Sc:              sc,
		RecordSet:       s.rs,
		DefCausLen:          1,
		PkBuilder:       NewSortedBuilder(sc, 256, 1, types.NewFieldType(allegrosql.TypeLonglong)),
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		DefCauslators:       make([]collate.DefCauslator, 1),
		DefCaussFieldType:   []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	collectors, pkBuilder, err := builder.DefCauslectDeferredCausetStats()
	c.Assert(err, IsNil)
	c.Assert(collectors[0].NullCount+collectors[0].Count, Equals, int64(s.count))
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(6232))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
	c.Assert(pkBuilder.Count, Equals, int64(s.count))
	c.Assert(pkBuilder.Hist().NDV, Equals, int64(s.count))
}

func (s *testSampleSuite) TestMergeSampleDefCauslector(c *C) {
	builder := SampleBuilder{
		Sc:              mock.NewContext().GetStochastikVars().StmtCtx,
		RecordSet:       s.rs,
		DefCausLen:          2,
		MaxSampleSize:   1000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		DefCauslators:       make([]collate.DefCauslator, 2),
		DefCaussFieldType:   []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong), types.NewFieldType(allegrosql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	collectors, pkBuilder, err := builder.DefCauslectDeferredCausetStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	c.Assert(len(collectors), Equals, 2)
	collectors[0].IsMerger = true
	collectors[0].MergeSampleDefCauslector(sc, collectors[1])
	c.Assert(collectors[0].FMSketch.NDV(), Equals, int64(9280))
	c.Assert(len(collectors[0].Samples), Equals, 1000)
	c.Assert(collectors[0].NullCount, Equals, int64(1000))
	c.Assert(collectors[0].Count, Equals, int64(19000))
	c.Assert(collectors[0].CMSketch.TotalCount(), Equals, uint64(collectors[0].Count))
}

func (s *testSampleSuite) TestDefCauslectorProtoConversion(c *C) {
	builder := SampleBuilder{
		Sc:              mock.NewContext().GetStochastikVars().StmtCtx,
		RecordSet:       s.rs,
		DefCausLen:          2,
		MaxSampleSize:   10000,
		MaxBucketSize:   256,
		MaxFMSketchSize: 1000,
		CMSketchWidth:   2048,
		CMSketchDepth:   8,
		DefCauslators:       make([]collate.DefCauslator, 2),
		DefCaussFieldType:   []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong), types.NewFieldType(allegrosql.TypeLonglong)},
	}
	c.Assert(s.rs.Close(), IsNil)
	collectors, pkBuilder, err := builder.DefCauslectDeferredCausetStats()
	c.Assert(err, IsNil)
	c.Assert(pkBuilder, IsNil)
	for _, collector := range collectors {
		p := SampleDefCauslectorToProto(collector)
		s := SampleDefCauslectorFromProto(p)
		c.Assert(collector.Count, Equals, s.Count)
		c.Assert(collector.NullCount, Equals, s.NullCount)
		c.Assert(collector.CMSketch.TotalCount(), Equals, s.CMSketch.TotalCount())
		c.Assert(collector.FMSketch.NDV(), Equals, s.FMSketch.NDV())
		c.Assert(collector.TotalSize, Equals, s.TotalSize)
		c.Assert(len(collector.Samples), Equals, len(s.Samples))
	}
}
