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
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
)

func (s *testStatisticsSuite) TestNewHistogramBySelectivity(c *C) {
	coll := &HistDefCausl{
		Count:   330,
		DeferredCausets: make(map[int64]*DeferredCauset),
		Indices: make(map[int64]*Index),
	}
	ctx := mock.NewContext()
	sc := ctx.GetStochastikVars().StmtCtx
	intDefCaus := &DeferredCauset{}
	intDefCaus.Histogram = *NewHistogram(1, 30, 30, 0, types.NewFieldType(allegrosql.TypeLonglong), chunk.InitialCapacity, 0)
	intDefCaus.IsHandle = true
	for i := 0; i < 10; i++ {
		intDefCaus.Bounds.AppendInt64(0, int64(i*3))
		intDefCaus.Bounds.AppendInt64(0, int64(i*3+2))
		intDefCaus.Buckets = append(intDefCaus.Buckets, Bucket{Repeat: 10, Count: int64(30*i + 30)})
	}
	coll.DeferredCausets[1] = intDefCaus
	node := &StatsNode{ID: 1, Tp: PkType, Selectivity: 0.56}
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeCausets(nil), HighVal: types.MakeCausets(nil)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: []types.Causet{types.MinNotNullCauset()}, HighVal: types.MakeCausets(2)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeCausets(5), HighVal: types.MakeCausets(6)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeCausets(8), HighVal: types.MakeCausets(10)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeCausets(13), HighVal: types.MakeCausets(13)})
	node.Ranges = append(node.Ranges, &ranger.Range{LowVal: types.MakeCausets(25), HighVal: []types.Causet{types.MaxValueCauset()}})
	intDefCausResult := `column:1 ndv:16 totDefCausSize:0
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10
num: 11 lower_bound: 6 upper_bound: 8 repeats: 0
num: 30 lower_bound: 9 upper_bound: 11 repeats: 0
num: 1 lower_bound: 12 upper_bound: 14 repeats: 0
num: 30 lower_bound: 27 upper_bound: 29 repeats: 0`

	stringDefCaus := &DeferredCauset{}
	stringDefCaus.Histogram = *NewHistogram(2, 15, 30, 0, types.NewFieldType(allegrosql.TypeString), chunk.InitialCapacity, 0)
	stringDefCaus.Bounds.AppendString(0, "a")
	stringDefCaus.Bounds.AppendString(0, "aaaabbbb")
	stringDefCaus.Buckets = append(stringDefCaus.Buckets, Bucket{Repeat: 10, Count: 60})
	stringDefCaus.Bounds.AppendString(0, "bbbb")
	stringDefCaus.Bounds.AppendString(0, "fdsfdsfds")
	stringDefCaus.Buckets = append(stringDefCaus.Buckets, Bucket{Repeat: 10, Count: 120})
	stringDefCaus.Bounds.AppendString(0, "kkkkk")
	stringDefCaus.Bounds.AppendString(0, "ooooo")
	stringDefCaus.Buckets = append(stringDefCaus.Buckets, Bucket{Repeat: 10, Count: 180})
	stringDefCaus.Bounds.AppendString(0, "oooooo")
	stringDefCaus.Bounds.AppendString(0, "sssss")
	stringDefCaus.Buckets = append(stringDefCaus.Buckets, Bucket{Repeat: 10, Count: 240})
	stringDefCaus.Bounds.AppendString(0, "ssssssu")
	stringDefCaus.Bounds.AppendString(0, "yyyyy")
	stringDefCaus.Buckets = append(stringDefCaus.Buckets, Bucket{Repeat: 10, Count: 300})
	stringDefCaus.PreCalculateScalar()
	coll.DeferredCausets[2] = stringDefCaus
	node2 := &StatsNode{ID: 2, Tp: DefCausType, Selectivity: 0.6}
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeCausets(nil), HighVal: types.MakeCausets(nil)})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: []types.Causet{types.MinNotNullCauset()}, HighVal: types.MakeCausets("aaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeCausets("aaaaaaaaaaa"), HighVal: types.MakeCausets("aaaaaaaaaaaaaa")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeCausets("bbb"), HighVal: types.MakeCausets("cccc")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeCausets("ddd"), HighVal: types.MakeCausets("fff")})
	node2.Ranges = append(node2.Ranges, &ranger.Range{LowVal: types.MakeCausets("ggg"), HighVal: []types.Causet{types.MaxValueCauset()}})
	stringDefCausResult := `column:2 ndv:9 totDefCausSize:0
num: 60 lower_bound: a upper_bound: aaaabbbb repeats: 0
num: 52 lower_bound: bbbb upper_bound: fdsfdsfds repeats: 0
num: 54 lower_bound: kkkkk upper_bound: ooooo repeats: 0
num: 60 lower_bound: oooooo upper_bound: sssss repeats: 0
num: 60 lower_bound: ssssssu upper_bound: yyyyy repeats: 0`

	newDefCausl := coll.NewHistDefCauslBySelectivity(sc, []*StatsNode{node, node2})
	c.Assert(newDefCausl.DeferredCausets[1].String(), Equals, intDefCausResult)
	c.Assert(newDefCausl.DeferredCausets[2].String(), Equals, stringDefCausResult)

	idx := &Index{Info: &perceptron.IndexInfo{DeferredCausets: []*perceptron.IndexDeferredCauset{{Name: perceptron.NewCIStr("a"), Offset: 0}}}}
	coll.Indices[0] = idx
	idx.Histogram = *NewHistogram(0, 15, 0, 0, types.NewFieldType(allegrosql.TypeBlob), 0, 0)
	for i := 0; i < 5; i++ {
		low, err1 := codec.EncodeKey(sc, nil, types.NewIntCauset(int64(i*3)))
		c.Assert(err1, IsNil, Commentf("Test failed: %v", err1))
		high, err2 := codec.EncodeKey(sc, nil, types.NewIntCauset(int64(i*3+2)))
		c.Assert(err2, IsNil, Commentf("Test failed: %v", err2))
		idx.Bounds.AppendBytes(0, low)
		idx.Bounds.AppendBytes(0, high)
		idx.Buckets = append(idx.Buckets, Bucket{Repeat: 10, Count: int64(30*i + 30)})
	}
	idx.PreCalculateScalar()
	node3 := &StatsNode{ID: 0, Tp: IndexType, Selectivity: 0.47}
	node3.Ranges = append(node3.Ranges, &ranger.Range{LowVal: types.MakeCausets(2), HighVal: types.MakeCausets(3)})
	node3.Ranges = append(node3.Ranges, &ranger.Range{LowVal: types.MakeCausets(10), HighVal: types.MakeCausets(13)})

	idxResult := `index:0 ndv:7
num: 30 lower_bound: 0 upper_bound: 2 repeats: 10
num: 30 lower_bound: 3 upper_bound: 5 repeats: 10
num: 30 lower_bound: 9 upper_bound: 11 repeats: 10
num: 30 lower_bound: 12 upper_bound: 14 repeats: 10`

	newDefCausl = coll.NewHistDefCauslBySelectivity(sc, []*StatsNode{node3})
	c.Assert(newDefCausl.Indices[0].String(), Equals, idxResult)
}

func (s *testStatisticsSuite) TestTruncateHistogram(c *C) {
	hist := NewHistogram(0, 0, 0, 0, types.NewFieldType(allegrosql.TypeLonglong), 1, 0)
	low, high := types.NewIntCauset(0), types.NewIntCauset(1)
	hist.AppendBucket(&low, &high, 0, 1)
	newHist := hist.TruncateHistogram(1)
	c.Assert(HistogramEqual(hist, newHist, true), IsTrue)
	newHist = hist.TruncateHistogram(0)
	c.Assert(newHist.Len(), Equals, 0)
}

func (s *testStatisticsSuite) TestValueToString4InvalidKey(c *C) {
	bytes, err := codec.EncodeKey(nil, nil, types.NewCauset(1), types.NewCauset(0.5))
	c.Assert(err, IsNil)
	// Append invalid flag.
	bytes = append(bytes, 20)
	causet := types.NewCauset(bytes)
	res, err := ValueToString(nil, &causet, 3, nil)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, "(1, 0.5, \x14)")
}
