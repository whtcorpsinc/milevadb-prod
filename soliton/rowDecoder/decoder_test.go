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

package causetDecoder_test

import (
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	_ "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/rowCausetDecoder"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCausetDecoderSuite{})

type testCausetDecoderSuite struct{}

func (s *testCausetDecoderSuite) TestRowCausetDecoder(c *C) {
	defer testleak.AfterTest(c)()
	c1 := &perceptron.DeferredCausetInfo{ID: 1, Name: perceptron.NewCIStr("c1"), State: perceptron.StatePublic, Offset: 0, FieldType: *types.NewFieldType(allegrosql.TypeLonglong)}
	c2 := &perceptron.DeferredCausetInfo{ID: 2, Name: perceptron.NewCIStr("c2"), State: perceptron.StatePublic, Offset: 1, FieldType: *types.NewFieldType(allegrosql.TypeVarchar)}
	c3 := &perceptron.DeferredCausetInfo{ID: 3, Name: perceptron.NewCIStr("c3"), State: perceptron.StatePublic, Offset: 2, FieldType: *types.NewFieldType(allegrosql.TypeNewDecimal)}
	c4 := &perceptron.DeferredCausetInfo{ID: 4, Name: perceptron.NewCIStr("c4"), State: perceptron.StatePublic, Offset: 3, FieldType: *types.NewFieldType(allegrosql.TypeTimestamp)}
	c5 := &perceptron.DeferredCausetInfo{ID: 5, Name: perceptron.NewCIStr("c5"), State: perceptron.StatePublic, Offset: 4, FieldType: *types.NewFieldType(allegrosql.TypeDuration), OriginDefaultValue: "02:00:02"}
	c6 := &perceptron.DeferredCausetInfo{ID: 6, Name: perceptron.NewCIStr("c6"), State: perceptron.StatePublic, Offset: 5, FieldType: *types.NewFieldType(allegrosql.TypeTimestamp), GeneratedExprString: "c4+c5"}
	c7 := &perceptron.DeferredCausetInfo{ID: 7, Name: perceptron.NewCIStr("c7"), State: perceptron.StatePublic, Offset: 6, FieldType: *types.NewFieldType(allegrosql.TypeLonglong)}
	c7.Flag |= allegrosql.PriKeyFlag

	defcaus := []*perceptron.DeferredCausetInfo{c1, c2, c3, c4, c5, c6, c7}

	tblInfo := &perceptron.BlockInfo{ID: 1, DeferredCausets: defcaus, PKIsHandle: true}
	tbl := blocks.MockBlockFromMeta(tblInfo)

	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	decodeDefCaussMap := make(map[int64]causetDecoder.DeferredCauset, len(defcaus))
	decodeDefCaussMap2 := make(map[int64]causetDecoder.DeferredCauset, len(defcaus))
	for _, defCaus := range tbl.DefCauss() {
		tpExpr := causetDecoder.DeferredCauset{
			DefCaus: defCaus,
		}
		decodeDefCaussMap2[defCaus.ID] = tpExpr
		if defCaus.GeneratedExprString != "" {
			expr, err := memex.ParseSimpleExprCastWithBlockInfo(ctx, defCaus.GeneratedExprString, tblInfo, &defCaus.FieldType)
			c.Assert(err, IsNil)
			tpExpr.GenExpr = expr
		}
		decodeDefCaussMap[defCaus.ID] = tpExpr
	}
	de := causetDecoder.NewRowCausetDecoder(tbl, tbl.DefCauss(), decodeDefCaussMap)
	deWithNoGenDefCauss := causetDecoder.NewRowCausetDecoder(tbl, tbl.DefCauss(), decodeDefCaussMap2)

	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)
	time1 := types.NewTime(types.FromDate(2020, 01, 01, 8, 01, 01, 0), allegrosql.TypeTimestamp, types.DefaultFsp)
	t1 := types.NewTimeCauset(time1)
	d1 := types.NewDurationCauset(types.Duration{
		Duration: time.Hour + time.Second,
	})

	time2, err := time1.Add(sc, d1.GetMysqlDuration())
	c.Assert(err, IsNil)
	err = time2.ConvertTimeZone(timeZoneIn8, time.UTC)
	c.Assert(err, IsNil)
	t2 := types.NewTimeCauset(time2)

	time3, err := time1.Add(sc, types.Duration{Duration: time.Hour*2 + time.Second*2})
	c.Assert(err, IsNil)
	err = time3.ConvertTimeZone(timeZoneIn8, time.UTC)
	c.Assert(err, IsNil)
	t3 := types.NewTimeCauset(time3)

	testRows := []struct {
		defcaus []int64
		input   []types.Causet
		output  []types.Causet
	}{
		{
			[]int64{defcaus[0].ID, defcaus[1].ID, defcaus[2].ID, defcaus[3].ID, defcaus[4].ID},
			[]types.Causet{types.NewIntCauset(100), types.NewBytesCauset([]byte("abc")), types.NewDecimalCauset(types.NewDecFromInt(1)), t1, d1},
			[]types.Causet{types.NewIntCauset(100), types.NewBytesCauset([]byte("abc")), types.NewDecimalCauset(types.NewDecFromInt(1)), t1, d1, t2},
		},
		{
			[]int64{defcaus[0].ID, defcaus[1].ID, defcaus[2].ID, defcaus[3].ID},
			[]types.Causet{types.NewIntCauset(100), types.NewBytesCauset([]byte("abc")), types.NewDecimalCauset(types.NewDecFromInt(1)), t1},
			[]types.Causet{types.NewIntCauset(100), types.NewBytesCauset([]byte("abc")), types.NewDecimalCauset(types.NewDecFromInt(1)), t1, types.NewCauset(nil), t3},
		},
		{
			[]int64{defcaus[0].ID, defcaus[1].ID, defcaus[2].ID, defcaus[3].ID, defcaus[4].ID},
			[]types.Causet{types.NewCauset(nil), types.NewCauset(nil), types.NewCauset(nil), types.NewCauset(nil), types.NewCauset(nil)},
			[]types.Causet{types.NewCauset(nil), types.NewCauset(nil), types.NewCauset(nil), types.NewCauset(nil), types.NewCauset(nil), types.NewCauset(nil)},
		},
	}
	rd := rowcodec.CausetEncoder{Enable: true}
	for i, event := range testRows {
		// test case for pk is unsigned.
		if i > 0 {
			c7.Flag |= allegrosql.UnsignedFlag
		}
		bs, err := blockcodec.EncodeRow(sc, event.input, event.defcaus, nil, nil, &rd)
		c.Assert(err, IsNil)
		c.Assert(bs, NotNil)

		r, err := de.DecodeAndEvalRowWithMap(ctx, ekv.IntHandle(i), bs, time.UTC, timeZoneIn8, nil)
		c.Assert(err, IsNil)
		// Last defCausumn is primary-key defCausumn, and the causet primary-key is handle, then the primary-key value won't be
		// stored in raw data, but causetstore in the raw key.
		// So when decode, we can't get the primary-key value from raw data, then ignore check the value of the
		// last primary key defCausumn.
		for i, defCaus := range defcaus[:len(defcaus)-1] {
			v, ok := r[defCaus.ID]
			if ok {
				equal, err1 := v.CompareCauset(sc, &event.output[i])
				c.Assert(err1, IsNil)
				c.Assert(equal, Equals, 0)
			} else {
				// use default value.
				c.Assert(defCaus.DefaultValue != "", IsTrue)
			}
		}
		// test decode with no generated defCausumn.
		r2, err := deWithNoGenDefCauss.DecodeAndEvalRowWithMap(ctx, ekv.IntHandle(i), bs, time.UTC, timeZoneIn8, nil)
		c.Assert(err, IsNil)
		for k, v := range r2 {
			v1, ok := r[k]
			c.Assert(ok, IsTrue)
			equal, err1 := v.CompareCauset(sc, &v1)
			c.Assert(err1, IsNil)
			c.Assert(equal, Equals, 0)
		}
	}
}

func (s *testCausetDecoderSuite) TestClusterIndexRowCausetDecoder(c *C) {
	c1 := &perceptron.DeferredCausetInfo{ID: 1, Name: perceptron.NewCIStr("c1"), State: perceptron.StatePublic, Offset: 0, FieldType: *types.NewFieldType(allegrosql.TypeLonglong)}
	c2 := &perceptron.DeferredCausetInfo{ID: 2, Name: perceptron.NewCIStr("c2"), State: perceptron.StatePublic, Offset: 1, FieldType: *types.NewFieldType(allegrosql.TypeVarchar)}
	c3 := &perceptron.DeferredCausetInfo{ID: 3, Name: perceptron.NewCIStr("c3"), State: perceptron.StatePublic, Offset: 2, FieldType: *types.NewFieldType(allegrosql.TypeNewDecimal)}
	c1.Flag |= allegrosql.PriKeyFlag
	c2.Flag |= allegrosql.PriKeyFlag
	pk := &perceptron.IndexInfo{ID: 1, Name: perceptron.NewCIStr("primary"), State: perceptron.StatePublic, Primary: true, DeferredCausets: []*perceptron.IndexDeferredCauset{
		{Name: perceptron.NewCIStr("c1"), Offset: 0},
		{Name: perceptron.NewCIStr("c2"), Offset: 1},
	}}

	defcaus := []*perceptron.DeferredCausetInfo{c1, c2, c3}

	tblInfo := &perceptron.BlockInfo{ID: 1, DeferredCausets: defcaus, Indices: []*perceptron.IndexInfo{pk}, IsCommonHandle: true}
	tbl := blocks.MockBlockFromMeta(tblInfo)

	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	decodeDefCaussMap := make(map[int64]causetDecoder.DeferredCauset, len(defcaus))
	for _, defCaus := range tbl.DefCauss() {
		tpExpr := causetDecoder.DeferredCauset{
			DefCaus: defCaus,
		}
		decodeDefCaussMap[defCaus.ID] = tpExpr
	}
	de := causetDecoder.NewRowCausetDecoder(tbl, tbl.DefCauss(), decodeDefCaussMap)

	timeZoneIn8, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)

	testRows := []struct {
		defcaus []int64
		input   []types.Causet
		output  []types.Causet
	}{
		{
			[]int64{defcaus[0].ID, defcaus[1].ID, defcaus[2].ID},
			[]types.Causet{types.NewIntCauset(100), types.NewBytesCauset([]byte("abc")), types.NewDecimalCauset(types.NewDecFromInt(1))},
			[]types.Causet{types.NewIntCauset(100), types.NewBytesCauset([]byte("abc")), types.NewDecimalCauset(types.NewDecFromInt(1))},
		},
	}
	rd := rowcodec.CausetEncoder{Enable: true}
	for _, event := range testRows {
		bs, err := blockcodec.EncodeRow(sc, event.input, event.defcaus, nil, nil, &rd)
		c.Assert(err, IsNil)
		c.Assert(bs, NotNil)

		r, err := de.DecodeAndEvalRowWithMap(ctx, solitonutil.MustNewCommonHandle(c, 100, "abc"), bs, time.UTC, timeZoneIn8, nil)
		c.Assert(err, IsNil)

		for i, defCaus := range defcaus {
			v, ok := r[defCaus.ID]
			c.Assert(ok, IsTrue)
			equal, err1 := v.CompareCauset(sc, &event.output[i])
			c.Assert(err1, IsNil)
			c.Assert(equal, Equals, 0)
		}
	}
}
