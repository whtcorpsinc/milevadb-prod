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

package core

import (
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// PBCausetBuilder uses to build physical plan from posetPosetDag protocol buffers.
type PBCausetBuilder struct {
	sctx stochastikctx.Context
	tps  []*types.FieldType
	is   schemareplicant.SchemaReplicant
}

// NewPBCausetBuilder creates a new pb plan builder.
func NewPBCausetBuilder(sctx stochastikctx.Context, is schemareplicant.SchemaReplicant) *PBCausetBuilder {
	return &PBCausetBuilder{sctx: sctx, is: is}
}

// Build builds physical plan from posetPosetDag protocol buffers.
func (b *PBCausetBuilder) Build(interlocks []*fidelpb.InterlockingDirectorate) (p PhysicalCauset, err error) {
	var src PhysicalCauset
	for i := 0; i < len(interlocks); i++ {
		curr, err := b.pbToPhysicalCauset(interlocks[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		curr.SetChildren(src)
		src = curr
	}
	_, src = b.predicatePushDown(src, nil)
	return src, nil
}

func (b *PBCausetBuilder) pbToPhysicalCauset(e *fidelpb.InterlockingDirectorate) (p PhysicalCauset, err error) {
	switch e.Tp {
	case fidelpb.InterDircType_TypeBlockScan:
		p, err = b.pbToBlockScan(e)
	case fidelpb.InterDircType_TypeSelection:
		p, err = b.pbToSelection(e)
	case fidelpb.InterDircType_TypeTopN:
		p, err = b.pbToTopN(e)
	case fidelpb.InterDircType_TypeLimit:
		p, err = b.pbToLimit(e)
	case fidelpb.InterDircType_TypeAggregation:
		p, err = b.pbToAgg(e, false)
	case fidelpb.InterDircType_TypeStreamAgg:
		p, err = b.pbToAgg(e, true)
	default:
		// TODO: Support other types.
		err = errors.Errorf("this exec type %v doesn't support yet.", e.GetTp())
	}
	return p, err
}

func (b *PBCausetBuilder) pbToBlockScan(e *fidelpb.InterlockingDirectorate) (PhysicalCauset, error) {
	tblScan := e.TblScan
	tbl, ok := b.is.BlockByID(tblScan.BlockId)
	if !ok {
		return nil, schemareplicant.ErrBlockNotExists.GenWithStack("Block which ID = %d does not exist.", tblScan.BlockId)
	}
	dbInfo, ok := b.is.SchemaByBlock(tbl.Meta())
	if !ok {
		return nil, schemareplicant.ErrDatabaseNotExists.GenWithStack("Database of causet ID = %d does not exist.", tblScan.BlockId)
	}
	// Currently only support cluster causet.
	if !tbl.Type().IsClusterBlock() {
		return nil, errors.Errorf("causet %s is not a cluster causet", tbl.Meta().Name.L)
	}
	columns, err := b.convertDeferredCausetInfo(tbl.Meta(), tblScan.DeferredCausets)
	if err != nil {
		return nil, err
	}
	schemaReplicant := b.buildBlockScanSchema(tbl.Meta(), columns)
	p := PhysicalMemBlock{
		DBName:  dbInfo.Name,
		Block:   tbl.Meta(),
		DeferredCausets: columns,
	}.Init(b.sctx, nil, 0)
	p.SetSchema(schemaReplicant)
	if strings.ToUpper(p.Block.Name.O) == schemareplicant.ClusterBlockSlowLog {
		p.Extractor = &SlowQueryExtractor{}
	}
	return p, nil
}

func (b *PBCausetBuilder) buildBlockScanSchema(tblInfo *perceptron.BlockInfo, columns []*perceptron.DeferredCausetInfo) *memex.Schema {
	schemaReplicant := memex.NewSchema(make([]*memex.DeferredCauset, 0, len(columns))...)
	for _, col := range tblInfo.DeferredCausets {
		for _, colInfo := range columns {
			if col.ID != colInfo.ID {
				continue
			}
			newDefCaus := &memex.DeferredCauset{
				UniqueID: b.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
				ID:       col.ID,
				RetType:  &col.FieldType,
			}
			schemaReplicant.Append(newDefCaus)
		}
	}
	return schemaReplicant
}

func (b *PBCausetBuilder) pbToSelection(e *fidelpb.InterlockingDirectorate) (PhysicalCauset, error) {
	conds, err := memex.PBToExprs(e.Selection.Conditions, b.tps, b.sctx.GetStochastikVars().StmtCtx)
	if err != nil {
		return nil, err
	}
	p := PhysicalSelection{
		Conditions: conds,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBCausetBuilder) pbToTopN(e *fidelpb.InterlockingDirectorate) (PhysicalCauset, error) {
	topN := e.TopN
	sc := b.sctx.GetStochastikVars().StmtCtx
	byItems := make([]*soliton.ByItems, 0, len(topN.OrderBy))
	for _, item := range topN.OrderBy {
		expr, err := memex.PBToExpr(item.Expr, b.tps, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		byItems = append(byItems, &soliton.ByItems{Expr: expr, Desc: item.Desc})
	}
	p := PhysicalTopN{
		ByItems: byItems,
		Count:   topN.Limit,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBCausetBuilder) pbToLimit(e *fidelpb.InterlockingDirectorate) (PhysicalCauset, error) {
	p := PhysicalLimit{
		Count: e.Limit.Limit,
	}.Init(b.sctx, nil, 0)
	return p, nil
}

func (b *PBCausetBuilder) pbToAgg(e *fidelpb.InterlockingDirectorate, isStreamAgg bool) (PhysicalCauset, error) {
	aggFuncs, groupBys, err := b.getAggInfo(e)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaReplicant := b.builPosetDaggSchema(aggFuncs, groupBys)
	baseAgg := basePhysicalAgg{
		AggFuncs:     aggFuncs,
		GroupByItems: groupBys,
	}
	baseAgg.schemaReplicant = schemaReplicant
	var partialAgg PhysicalCauset
	if isStreamAgg {
		partialAgg = baseAgg.initForStream(b.sctx, nil, 0)
	} else {
		partialAgg = baseAgg.initForHash(b.sctx, nil, 0)
	}
	return partialAgg, nil
}

func (b *PBCausetBuilder) builPosetDaggSchema(aggFuncs []*aggregation.AggFuncDesc, groupBys []memex.Expression) *memex.Schema {
	schemaReplicant := memex.NewSchema(make([]*memex.DeferredCauset, 0, len(aggFuncs)+len(groupBys))...)
	for _, agg := range aggFuncs {
		newDefCaus := &memex.DeferredCauset{
			UniqueID: b.sctx.GetStochastikVars().AllocCausetDeferredCausetID(),
			RetType:  agg.RetTp,
		}
		schemaReplicant.Append(newDefCaus)
	}
	return schemaReplicant
}

func (b *PBCausetBuilder) getAggInfo(interlock *fidelpb.InterlockingDirectorate) ([]*aggregation.AggFuncDesc, []memex.Expression, error) {
	var err error
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(interlock.Aggregation.AggFunc))
	for _, expr := range interlock.Aggregation.AggFunc {
		aggFunc, err := aggregation.PBExprToAggFuncDesc(b.sctx, expr, b.tps)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		aggFuncs = append(aggFuncs, aggFunc)
	}
	groupBys, err := memex.PBToExprs(interlock.Aggregation.GetGroupBy(), b.tps, b.sctx.GetStochastikVars().StmtCtx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return aggFuncs, groupBys, nil
}

func (b *PBCausetBuilder) convertDeferredCausetInfo(tblInfo *perceptron.BlockInfo, pbDeferredCausets []*fidelpb.DeferredCausetInfo) ([]*perceptron.DeferredCausetInfo, error) {
	columns := make([]*perceptron.DeferredCausetInfo, 0, len(pbDeferredCausets))
	tps := make([]*types.FieldType, 0, len(pbDeferredCausets))
	for _, col := range pbDeferredCausets {
		found := false
		for _, colInfo := range tblInfo.DeferredCausets {
			if col.DeferredCausetId == colInfo.ID {
				columns = append(columns, colInfo)
				tps = append(tps, colInfo.FieldType.Clone())
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("DeferredCauset ID %v of causet %v not found", col.DeferredCausetId, tblInfo.Name.L)
		}
	}
	b.tps = tps
	return columns, nil
}

func (b *PBCausetBuilder) predicatePushDown(p PhysicalCauset, predicates []memex.Expression) ([]memex.Expression, PhysicalCauset) {
	if p == nil {
		return predicates, p
	}
	switch p.(type) {
	case *PhysicalMemBlock:
		memBlock := p.(*PhysicalMemBlock)
		if memBlock.Extractor == nil {
			return predicates, p
		}
		names := make([]*types.FieldName, 0, len(memBlock.DeferredCausets))
		for _, col := range memBlock.DeferredCausets {
			names = append(names, &types.FieldName{
				TblName:     memBlock.Block.Name,
				DefCausName:     col.Name,
				OrigTblName: memBlock.Block.Name,
				OrigDefCausName: col.Name,
			})
		}
		// Set the memex column unique ID.
		// Since the memex is build from PB, It has not set the memex column ID yet.
		schemaDefCauss := memBlock.schemaReplicant.DeferredCausets
		defcaus := memex.ExtractDeferredCausetsFromExpressions([]*memex.DeferredCauset{}, predicates, nil)
		for i := range defcaus {
			defcaus[i].UniqueID = schemaDefCauss[defcaus[i].Index].UniqueID
		}
		predicates = memBlock.Extractor.Extract(b.sctx, memBlock.schemaReplicant, names, predicates)
		return predicates, memBlock
	case *PhysicalSelection:
		selection := p.(*PhysicalSelection)
		conditions, child := b.predicatePushDown(p.Children()[0], selection.Conditions)
		if len(conditions) > 0 {
			selection.Conditions = conditions
			selection.SetChildren(child)
			return predicates, selection
		}
		return predicates, child
	default:
		if children := p.Children(); len(children) > 0 {
			_, child := b.predicatePushDown(children[0], nil)
			p.SetChildren(child)
		}
		return predicates, p
	}
}
