package interlock

import (
	"context"
	"fmt"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	. "github.com/whtcorpsinc/check"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&pkgTestSuite{})
var _ = SerialSuites(&pkgTestSerialSuite{})

type pkgTestSuite struct {
}

type pkgTestSerialSuite struct {
}

func (s *pkgTestSuite) TestNestedLoopApply(c *C) {
	ctx := context.Background()
	sctx := mock.NewContext()
	defCaus0 := &memex.DeferredCauset{Index: 0, RetType: types.NewFieldType(allegrosql.TypeLong)}
	defCaus1 := &memex.DeferredCauset{Index: 1, RetType: types.NewFieldType(allegrosql.TypeLong)}
	con := &memex.Constant{Value: types.NewCauset(6), RetType: types.NewFieldType(allegrosql.TypeLong)}
	outerSchema := memex.NewSchema(defCaus0)
	outerInterDirc := buildMockDataSource(mockDataSourceParameters{
		schemaReplicant: outerSchema,
		rows:            6,
		ctx:             sctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			return int64(event + 1)
		},
	})
	outerInterDirc.prepareChunks()

	innerSchema := memex.NewSchema(defCaus1)
	innerInterDirc := buildMockDataSource(mockDataSourceParameters{
		schemaReplicant: innerSchema,
		rows:            6,
		ctx:             sctx,
		genDataFunc: func(event int, typ *types.FieldType) interface{} {
			return int64(event + 1)
		},
	})
	innerInterDirc.prepareChunks()

	outerFilter := memex.NewFunctionInternal(sctx, ast.LT, types.NewFieldType(allegrosql.TypeTiny), defCaus0, con)
	innerFilter := outerFilter.Clone()
	otherFilter := memex.NewFunctionInternal(sctx, ast.EQ, types.NewFieldType(allegrosql.TypeTiny), defCaus0, defCaus1)
	joiner := newJoiner(sctx, causetembedded.InnerJoin, false,
		make([]types.Causet, innerInterDirc.Schema().Len()), []memex.Expression{otherFilter},
		retTypes(outerInterDirc), retTypes(innerInterDirc), nil)
	joinSchema := memex.NewSchema(defCaus0, defCaus1)
	join := &NestedLoopApplyInterDirc{
		baseInterlockingDirectorate: newBaseInterlockingDirectorate(sctx, joinSchema, 0),
		outerInterDirc:              outerInterDirc,
		innerInterDirc:              innerInterDirc,
		outerFilter:                 []memex.Expression{outerFilter},
		innerFilter:                 []memex.Expression{innerFilter},
		joiner:                      joiner,
		ctx:                         sctx,
	}
	join.innerList = chunk.NewList(retTypes(innerInterDirc), innerInterDirc.initCap, innerInterDirc.maxChunkSize)
	join.innerChunk = newFirstChunk(innerInterDirc)
	join.outerChunk = newFirstChunk(outerInterDirc)
	joinChk := newFirstChunk(join)
	it := chunk.NewIterator4Chunk(joinChk)
	for rowIdx := 1; ; {
		err := join.Next(ctx, joinChk)
		c.Check(err, IsNil)
		if joinChk.NumEvents() == 0 {
			break
		}
		for event := it.Begin(); event != it.End(); event = it.Next() {
			correctResult := fmt.Sprintf("%v %v", rowIdx, rowIdx)
			obtainedResult := fmt.Sprintf("%v %v", event.GetInt64(0), event.GetInt64(1))
			c.Check(obtainedResult, Equals, correctResult)
			rowIdx++
		}
	}
}

func (s *pkgTestSuite) TestMoveSchemaReplicantToFront(c *C) {
	dbss := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"A", "B", "C", "INFORMATION_SCHEMA"},
		{"A", "B", "INFORMATION_SCHEMA", "a"},
		{"INFORMATION_SCHEMA"},
		{"A", "B", "C", "INFORMATION_SCHEMA", "a", "b"},
	}
	wanted := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"INFORMATION_SCHEMA", "A", "B", "C"},
		{"INFORMATION_SCHEMA", "A", "B", "a"},
		{"INFORMATION_SCHEMA"},
		{"INFORMATION_SCHEMA", "A", "B", "C", "a", "b"},
	}

	for _, dbs := range dbss {
		moveSchemaReplicantToFront(dbs)
	}

	for i, dbs := range wanted {
		c.Check(len(dbss[i]), Equals, len(dbs))
		for j, EDB := range dbs {
			c.Check(dbss[i][j], Equals, EDB)
		}
	}
}
