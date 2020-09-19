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

package statistics_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const eps = 1e-9

var _ = SerialSuites(&testStatsSuite{})

type testStatsSuite struct {
	causetstore    ekv.CausetStorage
	do       *petri.Petri
	hook     *logHook
	testData solitonutil.TestData
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// Add the hook here to avoid data race.
	s.registerHook()
	var err error
	s.causetstore, s.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.testData, err = solitonutil.LoadTestSuiteData("testdata", "stats_suite")
	c.Assert(err, IsNil)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	s.do.Close()
	c.Assert(s.causetstore.Close(), IsNil)
	testleak.AfterTest(c)()
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testStatsSuite) registerHook() {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	s.hook = &logHook{r.Core, ""}
	lg := zap.New(s.hook)
	log.ReplaceGlobals(lg, r)
}

type logHook struct {
	zapcore.Core
	results string
}

func (h *logHook) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	message := entry.Message
	if idx := strings.Index(message, "[stats"); idx != -1 {
		h.results = h.results + message
		for _, f := range fields {
			h.results = h.results + ", " + f.Key + "=" + h.field2String(f)
		}
	}
	return nil
}

func (h *logHook) field2String(field zapcore.Field) string {
	switch field.Type {
	case zapcore.StringType:
		return field.String
	case zapcore.Int64Type, zapcore.Int32Type, zapcore.Uint32Type:
		return fmt.Sprintf("%v", field.Integer)
	case zapcore.Float64Type:
		return fmt.Sprintf("%v", math.Float64frombits(uint64(field.Integer)))
	case zapcore.StringerType:
		return field.Interface.(fmt.Stringer).String()
	}
	return "not support"
}

func (h *logHook) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if h.Enabled(e.Level) {
		return ce.AddCore(e, h)
	}
	return ce
}

func newStoreWithBootstrap() (ekv.CausetStorage, *petri.Petri, error) {
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	petri.RunAutoAnalyze = false
	do, err := stochastik.BootstrapStochastik(causetstore)
	do.SetStatsUFIDelating(true)
	return causetstore, do, errors.Trace(err)
}

func cleanEnv(c *C, causetstore ekv.CausetStorage, do *petri.Petri) {
	tk := testkit.NewTestKit(c, causetstore)
	tk.MustExec("use test")
	r := tk.MustQuery("show blocks")
	for _, tb := range r.Rows() {
		blockName := tb[0]
		tk.MustExec(fmt.Sprintf("drop block %v", blockName))
	}
	tk.MustExec("delete from allegrosql.stats_meta")
	tk.MustExec("delete from allegrosql.stats_histograms")
	tk.MustExec("delete from allegrosql.stats_buckets")
	do.StatsHandle().Clear()
}

// generateIntCauset will generate a causet slice, every dimension is begin from 0, end with num - 1.
// If dimension is x, num is y, the total number of causet is y^x. And This slice is sorted.
func (s *testStatsSuite) generateIntCauset(dimension, num int) ([]types.Causet, error) {
	length := int(math.Pow(float64(num), float64(dimension)))
	ret := make([]types.Causet, length)
	if dimension == 1 {
		for i := 0; i < num; i++ {
			ret[i] = types.NewIntCauset(int64(i))
		}
	} else {
		sc := &stmtctx.StatementContext{TimeZone: time.Local}
		// In this way, we can guarantee the causet is in order.
		for i := 0; i < length; i++ {
			data := make([]types.Causet, dimension)
			j := i
			for k := 0; k < dimension; k++ {
				data[dimension-k-1].SetInt64(int64(j % num))
				j = j / num
			}
			bytes, err := codec.EncodeKey(sc, nil, data...)
			if err != nil {
				return nil, err
			}
			ret[i].SetBytes(bytes)
		}
	}
	return ret, nil
}

// mockStatsHistogram will create a statistics.Histogram, of which the data is uniform distribution.
func mockStatsHistogram(id int64, values []types.Causet, repeat int64, tp *types.FieldType) *statistics.Histogram {
	ndv := len(values)
	histogram := statistics.NewHistogram(id, int64(ndv), 0, 0, tp, ndv, 0)
	for i := 0; i < ndv; i++ {
		histogram.AppendBucket(&values[i], &values[i], repeat*int64(i+1), repeat)
	}
	return histogram
}

func mockStatsTable(tbl *perceptron.TableInfo, rowCount int64) *statistics.Block {
	histDefCausl := statistics.HistDefCausl{
		PhysicalID:     tbl.ID,
		HavePhysicalID: true,
		Count:          rowCount,
		DeferredCausets:        make(map[int64]*statistics.DeferredCauset, len(tbl.DeferredCausets)),
		Indices:        make(map[int64]*statistics.Index, len(tbl.Indices)),
	}
	statsTbl := &statistics.Block{
		HistDefCausl: histDefCausl,
	}
	return statsTbl
}

func (s *testStatsSuite) prepareSelectivity(testKit *testkit.TestKit, c *C) *statistics.Block {
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a int primary key, b int, c int, d int, e int, index idx_cd(c, d), index idx_de(d, e))")

	is := s.do.SchemaReplicant()
	tb, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tbl := tb.Meta()

	// mock the statistic block
	statsTbl := mockStatsTable(tbl, 540)

	// Set the value of columns' histogram.
	colValues, err := s.generateIntCauset(1, 54)
	c.Assert(err, IsNil)
	for i := 1; i <= 5; i++ {
		statsTbl.DeferredCausets[int64(i)] = &statistics.DeferredCauset{Histogram: *mockStatsHistogram(int64(i), colValues, 10, types.NewFieldType(allegrosql.TypeLonglong)), Info: tbl.DeferredCausets[i-1]}
	}

	// Set the value of two indices' histograms.
	idxValues, err := s.generateIntCauset(2, 3)
	c.Assert(err, IsNil)
	tp := types.NewFieldType(allegrosql.TypeBlob)
	statsTbl.Indices[1] = &statistics.Index{Histogram: *mockStatsHistogram(1, idxValues, 60, tp), Info: tbl.Indices[0]}
	statsTbl.Indices[2] = &statistics.Index{Histogram: *mockStatsHistogram(2, idxValues, 60, tp), Info: tbl.Indices[1]}
	return statsTbl
}

func (s *testStatsSuite) TestSelectivity(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	statsTbl := s.prepareSelectivity(testKit, c)
	is := s.do.SchemaReplicant()

	longExpr := "0 < a and a = 1 "
	for i := 1; i < 64; i++ {
		longExpr += fmt.Sprintf(" and a > %d ", i)
	}
	tests := []struct {
		exprs       string
		selectivity float64
	}{
		{
			exprs:       "a > 0 and a < 2",
			selectivity: 0.01851851851,
		},
		{
			exprs:       "a >= 1 and a < 2",
			selectivity: 0.01851851851,
		},
		{
			exprs:       "a >= 1 and b > 1 and a < 2",
			selectivity: 0.01783264746,
		},
		{
			exprs:       "a >= 1 and c > 1 and a < 2",
			selectivity: 0.00617283950,
		},
		{
			exprs:       "a >= 1 and c >= 1 and a < 2",
			selectivity: 0.01234567901,
		},
		{
			exprs:       "d = 0 and e = 1",
			selectivity: 0.11111111111,
		},
		{
			exprs:       "b > 1",
			selectivity: 0.96296296296,
		},
		{
			exprs:       "a > 1 and b < 2 and c > 3 and d < 4 and e > 5",
			selectivity: 0,
		},
		{
			exprs:       longExpr,
			selectivity: 0.001,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		allegrosql := "select * from t where " + tt.exprs
		comment := Commentf("for %s", tt.exprs)
		sctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(sctx, allegrosql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprs))
		c.Assert(stmts, HasLen, 1)

		err = plannercore.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, comment)
		p, _, err := plannercore.BuildLogicalPlan(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, tt.exprs))

		sel := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		ds := sel.Children()[0].(*plannercore.DataSource)

		histDefCausl := statsTbl.GenerateHistDefCauslFromDeferredCausetInfo(ds.DeferredCausets, ds.Schema().DeferredCausets)

		ratio, _, err := histDefCausl.Selectivity(sctx, sel.Conditions, nil)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))

		histDefCausl.Count *= 10
		ratio, _, err = histDefCausl.Selectivity(sctx, sel.Conditions, nil)
		c.Assert(err, IsNil, comment)
		c.Assert(math.Abs(ratio-tt.selectivity) < eps, IsTrue, Commentf("for %s, needed: %v, got: %v", tt.exprs, tt.selectivity, ratio))
	}
}

// TestDiscreteDistribution tests the estimation for discrete data distribution. This is more common when the index
// consists several columns, and the first column has small NDV.
func (s *testStatsSuite) TestDiscreteDistribution(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a char(10), b int, key idx(a, b))")
	for i := 0; i < 499; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values ('cn', %d)", i))
	}
	for i := 0; i < 10; i++ {
		testKit.MustExec("insert into t values ('tw', 0)")
	}
	testKit.MustExec("analyze block t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func (s *testStatsSuite) TestSelectCombinedLowBound(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(id int auto_increment, kid int, pid int, primary key(id), key(kid, pid))")
	testKit.MustExec("insert into t (kid, pid) values (1,2), (1,3), (1,4),(1, 11), (1, 12), (1, 13), (1, 14), (2, 2), (2, 3), (2, 4)")
	testKit.MustExec("analyze block t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func getRange(start, end int64) []*ranger.Range {
	ran := &ranger.Range{
		LowVal:  []types.Causet{types.NewIntCauset(start)},
		HighVal: []types.Causet{types.NewIntCauset(end)},
	}
	return []*ranger.Range{ran}
}

func (s *testStatsSuite) TestOutOfRangeEQEstimation(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a int)")
	for i := 0; i < 1000; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%v)", i/4)) // 0 ~ 249
	}
	testKit.MustExec("analyze block t")

	h := s.do.StatsHandle()
	block, err := s.do.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl := h.GetTableStats(block.Meta())
	sc := &stmtctx.StatementContext{}
	col := statsTbl.DeferredCausets[block.Meta().DeferredCausets[0].ID]
	count, err := col.GetDeferredCausetRowCount(sc, getRange(250, 250), 0, false)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, float64(0))

	for i := 0; i < 8; i++ {
		count, err := col.GetDeferredCausetRowCount(sc, getRange(250, 250), int64(i+1), false)
		c.Assert(err, IsNil)
		c.Assert(count, Equals, math.Min(float64(i+1), 4)) // estRows must be less than modifyCnt
	}
}

func (s *testStatsSuite) TestEstimationForUnknownValues(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a int, b int, key idx(a, b))")
	testKit.MustExec("analyze block t")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze block t")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i+10, i+10))
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.UFIDelate(s.do.SchemaReplicant()), IsNil)
	block, err := s.do.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl := h.GetTableStats(block.Meta())

	sc := &stmtctx.StatementContext{}
	colID := block.Meta().DeferredCausets[0].ID
	count, err := statsTbl.GetRowCountByDeferredCausetRanges(sc, colID, getRange(30, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.2)

	count, err = statsTbl.GetRowCountByDeferredCausetRanges(sc, colID, getRange(9, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2.4000000000000004)

	count, err = statsTbl.GetRowCountByDeferredCausetRanges(sc, colID, getRange(9, math.MaxInt64))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2.4000000000000004)

	idxID := block.Meta().Indices[0].ID
	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(30, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.2)

	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(9, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 2.2)

	testKit.MustExec("truncate block t")
	testKit.MustExec("insert into t values (null, null)")
	testKit.MustExec("analyze block t")
	block, err = s.do.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl = h.GetTableStats(block.Meta())

	colID = block.Meta().DeferredCausets[0].ID
	count, err = statsTbl.GetRowCountByDeferredCausetRanges(sc, colID, getRange(1, 30))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.0)

	testKit.MustExec("drop block t")
	testKit.MustExec("create block t(a int, b int, index idx(b))")
	testKit.MustExec("insert into t values (1,1)")
	testKit.MustExec("analyze block t")
	block, err = s.do.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl = h.GetTableStats(block.Meta())

	colID = block.Meta().DeferredCausets[0].ID
	count, err = statsTbl.GetRowCountByDeferredCausetRanges(sc, colID, getRange(2, 2))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.0)

	idxID = block.Meta().Indices[0].ID
	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(2, 2))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 0.0)
}

func (s *testStatsSuite) TestEstimationUniqueKeyEqualConds(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a int, b int, c int, unique key(b))")
	testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7)")
	testKit.MustExec("analyze block t with 4 cmsketch width, 1 cmsketch depth;")
	block, err := s.do.SchemaReplicant().TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	statsTbl := s.do.StatsHandle().GetTableStats(block.Meta())

	sc := &stmtctx.StatementContext{}
	idxID := block.Meta().Indices[0].ID
	count, err := statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(7, 7))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)

	count, err = statsTbl.GetRowCountByIndexRanges(sc, idxID, getRange(6, 6))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)

	colID := block.Meta().DeferredCausets[0].ID
	count, err = statsTbl.GetRowCountByIntDeferredCausetRanges(sc, colID, getRange(7, 7))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)

	count, err = statsTbl.GetRowCountByIntDeferredCausetRanges(sc, colID, getRange(6, 6))
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1.0)
}

func (s *testStatsSuite) TestPrimaryKeySelectivity(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("set @@milevadb_enable_clustered_index=0")
	testKit.MustExec("create block t(a char(10) primary key, b int)")
	var input, output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				testKit.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				if j == len(ts)-1 {
					output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
			}
		}
	}
}

func BenchmarkSelectivity(b *testing.B) {
	c := &C{}
	s := &testStatsSuite{}
	s.SetUpSuite(c)
	defer s.TearDownSuite(c)

	testKit := testkit.NewTestKit(c, s.causetstore)
	statsTbl := s.prepareSelectivity(testKit, c)
	is := s.do.SchemaReplicant()
	exprs := "a > 1 and b < 2 and c > 3 and d < 4 and e > 5"
	allegrosql := "select * from t where " + exprs
	comment := Commentf("for %s", exprs)
	sctx := testKit.Se.(stochastikctx.Context)
	stmts, err := stochastik.Parse(sctx, allegrosql)
	c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, exprs))
	c.Assert(stmts, HasLen, 1)
	err = plannercore.Preprocess(sctx, stmts[0], is)
	c.Assert(err, IsNil, comment)
	p, _, err := plannercore.BuildLogicalPlan(context.Background(), sctx, stmts[0], is)
	c.Assert(err, IsNil, Commentf("error %v, for building plan, expr %s", err, exprs))

	file, err := os.Create("cpu.profile")
	c.Assert(err, IsNil)
	defer file.Close()
	pprof.StartCPUProfile(file)

	b.Run("Selectivity", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := statsTbl.Selectivity(sctx, p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection).Conditions, nil)
			c.Assert(err, IsNil)
		}
		b.ReportAllocs()
	})
	pprof.StopCPUProfile()
}

func (s *testStatsSuite) TestDeferredCausetIndexNullEstimation(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a int, b int, c int, index idx_b(b), index idx_c_a(c, a))")
	testKit.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null);")
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze block t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < 5; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
	// Make sure column stats has been loaded.
	testKit.MustExec(`explain select * from t where a is null`)
	c.Assert(h.LoadNeededHistograms(), IsNil)
	for i := 5; i < len(input); i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func (s *testStatsSuite) TestUniqCompEqualEst(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)
	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a int, b int, primary key(a, b))")
	testKit.MustExec("insert into t values(1,1),(1,2),(1,3),(1,4),(1,5),(1,6),(1,7),(1,8),(1,9),(1,10)")
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze block t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < 1; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func (s *testStatsSuite) TestSelectivityGreedyAlgo(c *C) {
	nodes := make([]*statistics.StatsNode, 3)
	nodes[0] = statistics.MockStatsNode(1, 3, 2)
	nodes[1] = statistics.MockStatsNode(2, 5, 2)
	nodes[2] = statistics.MockStatsNode(3, 9, 2)

	// Sets should not overlap on mask, so only nodes[0] is chosen.
	usedSets := statistics.GetUsableSetsByGreedy(nodes)
	c.Assert(len(usedSets), Equals, 1)
	c.Assert(usedSets[0].ID, Equals, int64(1))

	nodes[0], nodes[1] = nodes[1], nodes[0]
	// Sets chosen should be sblock, so the returned node is still the one with ID 1.
	usedSets = statistics.GetUsableSetsByGreedy(nodes)
	c.Assert(len(usedSets), Equals, 1)
	c.Assert(usedSets[0].ID, Equals, int64(1))
}

func (s *testStatsSuite) TestDefCauslationDeferredCausetEstimate(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	tk := testkit.NewTestKit(c, s.causetstore)
	collate.SetNewDefCauslationEnabledForTest(true)
	defer collate.SetNewDefCauslationEnabledForTest(false)
	tk.MustExec("use test")
	tk.MustExec("drop block if exists t")
	tk.MustExec("create block t(a varchar(20) collate utf8mb4_general_ci)")
	tk.MustExec("insert into t values('aaa'), ('bbb'), ('AAA'), ('BBB')")
	h := s.do.StatsHandle()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	tk.MustExec("analyze block t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	c.Assert(h.LoadNeededHistograms(), IsNil)
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < len(input); i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

// TestDNFCondSelectivity tests selectivity calculation with DNF conditions covered by using independence assumption.
func (s *testStatsSuite) TestDNFCondSelectivity(c *C) {
	defer cleanEnv(c, s.causetstore, s.do)
	testKit := testkit.NewTestKit(c, s.causetstore)

	testKit.MustExec("use test")
	testKit.MustExec("drop block if exists t")
	testKit.MustExec("create block t(a int, b int, c int, d int)")
	testKit.MustExec("insert into t value(1,5,4,4),(3,4,1,8),(4,2,6,10),(6,7,2,5),(7,1,4,9),(8,9,8,3),(9,1,9,1),(10,6,6,2)")
	testKit.MustExec("alter block t add index (b)")
	testKit.MustExec("alter block t add index (d)")
	testKit.MustExec(`analyze block t`)

	ctx := context.Background()
	is := s.do.SchemaReplicant()
	h := s.do.StatsHandle()
	tb, err := is.TableByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblInfo := tb.Meta()
	statsTbl := h.GetTableStats(tblInfo)

	var (
		input  []string
		output []struct {
			ALLEGROALLEGROSQL         string
			Selectivity float64
		}
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		sctx := testKit.Se.(stochastikctx.Context)
		stmts, err := stochastik.Parse(sctx, tt)
		c.Assert(err, IsNil, Commentf("error %v, for allegrosql %s", err, tt))
		c.Assert(stmts, HasLen, 1)

		err = plannercore.Preprocess(sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for allegrosql %s", err, tt))
		p, _, err := plannercore.BuildLogicalPlan(ctx, sctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for building plan, allegrosql %s", err, tt))

		sel := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		ds := sel.Children()[0].(*plannercore.DataSource)

		histDefCausl := statsTbl.GenerateHistDefCauslFromDeferredCausetInfo(ds.DeferredCausets, ds.Schema().DeferredCausets)

		ratio, _, err := histDefCausl.Selectivity(sctx, sel.Conditions, nil)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt))
		s.testData.OnRecord(func() {
			output[i].ALLEGROALLEGROSQL = tt
			output[i].Selectivity = ratio
		})
		c.Assert(math.Abs(ratio-output[i].Selectivity) < eps, IsTrue,
			Commentf("for %s, needed: %v, got: %v", tt, output[i].Selectivity, ratio))
	}

	// Test issue 19981
	testKit.MustExec("select * from t where _milevadb_rowid is null or _milevadb_rowid > 7")
}
