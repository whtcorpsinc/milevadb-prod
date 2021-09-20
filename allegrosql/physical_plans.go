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

package embedded

import (
	"unsafe"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/property"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/memex/aggregation"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var (
	_ PhysicalCauset = &PhysicalSelection{}
	_ PhysicalCauset = &PhysicalProjection{}
	_ PhysicalCauset = &PhysicalTopN{}
	_ PhysicalCauset = &PhysicalMaxOneRow{}
	_ PhysicalCauset = &PhysicalBlockDual{}
	_ PhysicalCauset = &PhysicalUnionAll{}
	_ PhysicalCauset = &PhysicalSort{}
	_ PhysicalCauset = &NominalSort{}
	_ PhysicalCauset = &PhysicalLock{}
	_ PhysicalCauset = &PhysicalLimit{}
	_ PhysicalCauset = &PhysicalIndexScan{}
	_ PhysicalCauset = &PhysicalBlockScan{}
	_ PhysicalCauset = &PhysicalBlockReader{}
	_ PhysicalCauset = &PhysicalIndexReader{}
	_ PhysicalCauset = &PhysicalIndexLookUpReader{}
	_ PhysicalCauset = &PhysicalIndexMergeReader{}
	_ PhysicalCauset = &PhysicalHashAgg{}
	_ PhysicalCauset = &PhysicalStreamAgg{}
	_ PhysicalCauset = &PhysicalApply{}
	_ PhysicalCauset = &PhysicalIndexJoin{}
	_ PhysicalCauset = &PhysicalBroadCastJoin{}
	_ PhysicalCauset = &PhysicalHashJoin{}
	_ PhysicalCauset = &PhysicalMergeJoin{}
	_ PhysicalCauset = &PhysicalUnionScan{}
	_ PhysicalCauset = &PhysicalWindow{}
	_ PhysicalCauset = &PhysicalShuffle{}
	_ PhysicalCauset = &PhysicalShuffleDataSourceStub{}
	_ PhysicalCauset = &BatchPointGetCauset{}
)

// PhysicalBlockReader is the causet reader in milevadb.
type PhysicalBlockReader struct {
	physicalSchemaProducer

	// BlockCausets flats the blockCauset to construct interlock pb.
	BlockCausets []PhysicalCauset
	blockCauset  PhysicalCauset

	// StoreType indicates causet read from which type of causetstore.
	StoreType ekv.StoreType

	IsCommonHandle bool

	// Used by partition causet.
	PartitionInfo PartitionInfo
}

// PartitionInfo indicates partition helper info in physical plan.
type PartitionInfo struct {
	PruningConds        []memex.Expression
	PartitionNames      []perceptron.CIStr
	DeferredCausets     []*memex.DeferredCauset
	DeferredCausetNames types.NameSlice
}

// GetBlockCauset exports the blockCauset.
func (p *PhysicalBlockReader) GetBlockCauset() PhysicalCauset {
	return p.blockCauset
}

// GetBlockScan exports the blockScan that contained in blockCauset.
func (p *PhysicalBlockReader) GetBlockScan() *PhysicalBlockScan {
	curCauset := p.blockCauset
	for {
		chCnt := len(curCauset.Children())
		if chCnt == 0 {
			return curCauset.(*PhysicalBlockScan)
		} else if chCnt == 1 {
			curCauset = curCauset.Children()[0]
		} else {
			join := curCauset.(*PhysicalBroadCastJoin)
			curCauset = join.children[1-join.globalChildIndex]
		}
	}
}

// GetPhysicalBlockReader returns PhysicalBlockReader for logical EinsteinDBSingleGather.
func (sg *EinsteinDBSingleGather) GetPhysicalBlockReader(schemaReplicant *memex.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalBlockReader {
	reader := PhysicalBlockReader{}.Init(sg.ctx, sg.blockOffset)
	reader.PartitionInfo = PartitionInfo{
		PruningConds:        sg.Source.allConds,
		PartitionNames:      sg.Source.partitionNames,
		DeferredCausets:     sg.Source.TblDefCauss,
		DeferredCausetNames: sg.Source.names,
	}
	reader.stats = stats
	reader.SetSchema(schemaReplicant)
	reader.childrenReqProps = props
	return reader
}

// GetPhysicalIndexReader returns PhysicalIndexReader for logical EinsteinDBSingleGather.
func (sg *EinsteinDBSingleGather) GetPhysicalIndexReader(schemaReplicant *memex.Schema, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalIndexReader {
	reader := PhysicalIndexReader{}.Init(sg.ctx, sg.blockOffset)
	reader.stats = stats
	reader.SetSchema(schemaReplicant)
	reader.childrenReqProps = props
	return reader
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalBlockReader) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalBlockReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.StoreType = p.StoreType
	cloned.IsCommonHandle = p.IsCommonHandle
	if cloned.blockCauset, err = p.blockCauset.Clone(); err != nil {
		return nil, err
	}
	if cloned.BlockCausets, err = clonePhysicalCauset(p.BlockCausets); err != nil {
		return nil, err
	}
	return cloned, nil
}

// SetChildren overrides PhysicalCauset SetChildren interface.
func (p *PhysicalBlockReader) SetChildren(children ...PhysicalCauset) {
	p.blockCauset = children[0]
	p.BlockCausets = flattenPushDownCauset(p.blockCauset)
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalBlockReader) ExtractCorrelatedDefCauss() (corDefCauss []*memex.CorrelatedDeferredCauset) {
	for _, child := range p.BlockCausets {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
	}
	return corDefCauss
}

// PhysicalIndexReader is the index reader in milevadb.
type PhysicalIndexReader struct {
	physicalSchemaProducer

	// IndexCausets flats the indexCauset to construct interlock pb.
	IndexCausets []PhysicalCauset
	indexCauset  PhysicalCauset

	// OutputDeferredCausets represents the columns that index reader should return.
	OutputDeferredCausets []*memex.DeferredCauset

	// Used by partition causet.
	PartitionInfo PartitionInfo
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalIndexReader) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalIndexReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	if cloned.indexCauset, err = p.indexCauset.Clone(); err != nil {
		return nil, err
	}
	if cloned.IndexCausets, err = clonePhysicalCauset(p.IndexCausets); err != nil {
		return nil, err
	}
	cloned.OutputDeferredCausets = cloneDefCauss(p.OutputDeferredCausets)
	return cloned, err
}

// SetSchema overrides PhysicalCauset SetSchema interface.
func (p *PhysicalIndexReader) SetSchema(_ *memex.Schema) {
	if p.indexCauset != nil {
		p.IndexCausets = flattenPushDownCauset(p.indexCauset)
		switch p.indexCauset.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			p.schemaReplicant = p.indexCauset.Schema()
		default:
			is := p.IndexCausets[0].(*PhysicalIndexScan)
			p.schemaReplicant = is.dataSourceSchema
		}
		p.OutputDeferredCausets = p.schemaReplicant.Clone().DeferredCausets
	}
}

// SetChildren overrides PhysicalCauset SetChildren interface.
func (p *PhysicalIndexReader) SetChildren(children ...PhysicalCauset) {
	p.indexCauset = children[0]
	p.SetSchema(nil)
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalIndexReader) ExtractCorrelatedDefCauss() (corDefCauss []*memex.CorrelatedDeferredCauset) {
	for _, child := range p.IndexCausets {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
	}
	return corDefCauss
}

// PushedDownLimit is the limit operator pushed down into PhysicalIndexLookUpReader.
type PushedDownLimit struct {
	Offset uint64
	Count  uint64
}

// Clone clones this pushed-down list.
func (p *PushedDownLimit) Clone() *PushedDownLimit {
	cloned := new(PushedDownLimit)
	*cloned = *p
	return cloned
}

// PhysicalIndexLookUpReader is the index look up reader in milevadb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	physicalSchemaProducer

	// IndexCausets flats the indexCauset to construct interlock pb.
	IndexCausets []PhysicalCauset
	// BlockCausets flats the blockCauset to construct interlock pb.
	BlockCausets []PhysicalCauset
	indexCauset  PhysicalCauset
	blockCauset  PhysicalCauset

	ExtraHandleDefCaus *memex.DeferredCauset
	// PushedLimit is used to avoid unnecessary causet scan tasks of IndexLookUpReader.
	PushedLimit *PushedDownLimit

	CommonHandleDefCauss []*memex.DeferredCauset

	// Used by partition causet.
	PartitionInfo PartitionInfo
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalIndexLookUpReader) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalIndexLookUpReader)
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	if cloned.IndexCausets, err = clonePhysicalCauset(p.IndexCausets); err != nil {
		return nil, err
	}
	if cloned.BlockCausets, err = clonePhysicalCauset(p.BlockCausets); err != nil {
		return nil, err
	}
	if cloned.indexCauset, err = p.indexCauset.Clone(); err != nil {
		return nil, err
	}
	if cloned.blockCauset, err = p.blockCauset.Clone(); err != nil {
		return nil, err
	}
	if p.ExtraHandleDefCaus != nil {
		cloned.ExtraHandleDefCaus = p.ExtraHandleDefCaus.Clone().(*memex.DeferredCauset)
	}
	if p.PushedLimit != nil {
		cloned.PushedLimit = p.PushedLimit.Clone()
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalIndexLookUpReader) ExtractCorrelatedDefCauss() (corDefCauss []*memex.CorrelatedDeferredCauset) {
	for _, child := range p.BlockCausets {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
	}
	for _, child := range p.IndexCausets {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
	}
	return corDefCauss
}

// PhysicalIndexMergeReader is the reader using multiple indexes in milevadb.
type PhysicalIndexMergeReader struct {
	physicalSchemaProducer

	// PartialCausets flats the partialCausets to construct interlock pb.
	PartialCausets [][]PhysicalCauset
	// BlockCausets flats the blockCauset to construct interlock pb.
	BlockCausets []PhysicalCauset
	// partialCausets are the partial plans that have not been flatted. The type of each element is permitted PhysicalIndexScan or PhysicalBlockScan.
	partialCausets []PhysicalCauset
	// blockCauset is a PhysicalBlockScan to get the causet tuples. Current, it must be not nil.
	blockCauset PhysicalCauset

	// Used by partition causet.
	PartitionInfo PartitionInfo
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalIndexMergeReader) ExtractCorrelatedDefCauss() (corDefCauss []*memex.CorrelatedDeferredCauset) {
	for _, child := range p.BlockCausets {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
	}
	for _, child := range p.partialCausets {
		corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
	}
	for _, PartialCauset := range p.PartialCausets {
		for _, child := range PartialCauset {
			corDefCauss = append(corDefCauss, ExtractCorrelatedDefCauss4PhysicalCauset(child)...)
		}
	}
	return corDefCauss
}

// PhysicalIndexScan represents an index scan plan.
type PhysicalIndexScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []memex.Expression

	Block           *perceptron.BlockInfo
	Index           *perceptron.IndexInfo
	IdxDefCauss     []*memex.DeferredCauset
	IdxDefCausLens  []int
	Ranges          []*ranger.Range
	DeferredCausets []*perceptron.DeferredCausetInfo
	DBName          perceptron.CIStr

	BlockAsName *perceptron.CIStr

	// dataSourceSchema is the original schemaReplicant of DataSource. The schemaReplicant of index scan in KV and index reader in MilevaDB
	// will be different. The schemaReplicant of index scan will decode all columns of index but the MilevaDB only need some of them.
	dataSourceSchema *memex.Schema

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	rangeInfo string

	// The index scan may be on a partition.
	physicalBlockID int64

	GenExprs map[perceptron.BlockDeferredCausetID]memex.Expression

	isPartition bool
	Desc        bool
	KeepOrder   bool
	// DoubleRead means if the index interlock will read ekv two times.
	// If the query requires the columns that don't belong to index, DoubleRead will be true.
	DoubleRead bool

	NeedCommonHandle bool
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalIndexScan) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalIndexScan)
	*cloned = *p
	base, err := p.physicalSchemaProducer.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.AccessCondition = cloneExprs(p.AccessCondition)
	if p.Block != nil {
		cloned.Block = p.Block.Clone()
	}
	if p.Index != nil {
		cloned.Index = p.Index.Clone()
	}
	cloned.IdxDefCauss = cloneDefCauss(p.IdxDefCauss)
	cloned.IdxDefCausLens = make([]int, len(p.IdxDefCausLens))
	copy(cloned.IdxDefCausLens, p.IdxDefCausLens)
	cloned.Ranges = cloneRanges(p.Ranges)
	cloned.DeferredCausets = cloneDefCausInfos(p.DeferredCausets)
	if p.dataSourceSchema != nil {
		cloned.dataSourceSchema = p.dataSourceSchema.Clone()
	}
	if p.Hist != nil {
		cloned.Hist = p.Hist.Copy()
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalIndexScan) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.AccessCondition))
	for _, expr := range p.AccessCondition {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// PhysicalMemBlock reads memory causet.
type PhysicalMemBlock struct {
	physicalSchemaProducer

	DBName          perceptron.CIStr
	Block           *perceptron.BlockInfo
	DeferredCausets []*perceptron.DeferredCausetInfo
	Extractor       MemBlockPredicateExtractor
	QueryTimeRange  QueryTimeRange
}

// PhysicalBlockScan represents a causet scan plan.
type PhysicalBlockScan struct {
	physicalSchemaProducer

	// AccessCondition is used to calculate range.
	AccessCondition []memex.Expression
	filterCondition []memex.Expression

	Block           *perceptron.BlockInfo
	DeferredCausets []*perceptron.DeferredCausetInfo
	DBName          perceptron.CIStr
	Ranges          []*ranger.Range
	PkDefCauss      []*memex.DeferredCauset

	BlockAsName *perceptron.CIStr

	// Hist is the histogram when the query was issued.
	// It is used for query feedback.
	Hist *statistics.Histogram

	physicalBlockID int64

	rangeDecidedBy []*memex.DeferredCauset

	// HandleIdx is the index of handle, which is only used for admin check causet.
	HandleIdx      []int
	HandleDefCauss HandleDefCauss

	StoreType ekv.StoreType

	IsGlobalRead bool

	// The causet scan may be a partition, rather than a real causet.
	isPartition bool
	// KeepOrder is true, if sort data by scanning pkcol,
	KeepOrder bool
	Desc      bool

	isChildOfIndexLookUp bool
}

// Clone implements PhysicalCauset interface.
func (ts *PhysicalBlockScan) Clone() (PhysicalCauset, error) {
	clonedScan := new(PhysicalBlockScan)
	*clonedScan = *ts
	prod, err := ts.physicalSchemaProducer.cloneWithSelf(clonedScan)
	if err != nil {
		return nil, err
	}
	clonedScan.physicalSchemaProducer = *prod
	clonedScan.AccessCondition = cloneExprs(ts.AccessCondition)
	clonedScan.filterCondition = cloneExprs(ts.filterCondition)
	if ts.Block != nil {
		clonedScan.Block = ts.Block.Clone()
	}
	clonedScan.DeferredCausets = cloneDefCausInfos(ts.DeferredCausets)
	clonedScan.Ranges = cloneRanges(ts.Ranges)
	clonedScan.PkDefCauss = cloneDefCauss(ts.PkDefCauss)
	clonedScan.BlockAsName = ts.BlockAsName
	if ts.Hist != nil {
		clonedScan.Hist = ts.Hist.Copy()
	}
	clonedScan.rangeDecidedBy = cloneDefCauss(ts.rangeDecidedBy)
	return clonedScan, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (ts *PhysicalBlockScan) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(ts.AccessCondition)+len(ts.filterCondition))
	for _, expr := range ts.AccessCondition {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	for _, expr := range ts.filterCondition {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// IsPartition returns true and partition ID if it's actually a partition.
func (ts *PhysicalBlockScan) IsPartition() (bool, int64) {
	return ts.isPartition, ts.physicalBlockID
}

// ExpandVirtualDeferredCauset expands the virtual column's dependent columns to ts's schemaReplicant and column.
func ExpandVirtualDeferredCauset(columns []*perceptron.DeferredCausetInfo, schemaReplicant *memex.Schema,
	defcausInfo []*perceptron.DeferredCausetInfo) []*perceptron.DeferredCausetInfo {
	copyDeferredCauset := make([]*perceptron.DeferredCausetInfo, len(columns))
	copy(copyDeferredCauset, columns)
	var extraDeferredCauset *memex.DeferredCauset
	var extraDeferredCausetPerceptron *perceptron.DeferredCausetInfo
	if schemaReplicant.DeferredCausets[len(schemaReplicant.DeferredCausets)-1].ID == perceptron.ExtraHandleID {
		extraDeferredCauset = schemaReplicant.DeferredCausets[len(schemaReplicant.DeferredCausets)-1]
		extraDeferredCausetPerceptron = copyDeferredCauset[len(copyDeferredCauset)-1]
		schemaReplicant.DeferredCausets = schemaReplicant.DeferredCausets[:len(schemaReplicant.DeferredCausets)-1]
		copyDeferredCauset = copyDeferredCauset[:len(copyDeferredCauset)-1]
	}
	schemaDeferredCausets := schemaReplicant.DeferredCausets
	for _, col := range schemaDeferredCausets {
		if col.VirtualExpr == nil {
			continue
		}

		baseDefCauss := memex.ExtractDependentDeferredCausets(col.VirtualExpr)
		for _, baseDefCaus := range baseDefCauss {
			if !schemaReplicant.Contains(baseDefCaus) {
				schemaReplicant.DeferredCausets = append(schemaReplicant.DeferredCausets, baseDefCaus)
				copyDeferredCauset = append(copyDeferredCauset, FindDeferredCausetInfoByID(defcausInfo, baseDefCaus.ID))
			}
		}
	}
	if extraDeferredCauset != nil {
		schemaReplicant.DeferredCausets = append(schemaReplicant.DeferredCausets, extraDeferredCauset)
		copyDeferredCauset = append(copyDeferredCauset, extraDeferredCausetPerceptron)
	}
	return copyDeferredCauset
}

//SetIsChildOfIndexLookUp is to set the bool if is a child of IndexLookUpReader
func (ts *PhysicalBlockScan) SetIsChildOfIndexLookUp(isIsChildOfIndexLookUp bool) {
	ts.isChildOfIndexLookUp = isIsChildOfIndexLookUp
}

// PhysicalProjection is the physical operator of projection.
type PhysicalProjection struct {
	physicalSchemaProducer

	Exprs                        []memex.Expression
	CalculateNoDelay             bool
	AvoidDeferredCausetEvaluator bool
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalProjection) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalProjection)
	*cloned = *p
	base, err := p.basePhysicalCauset.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalCauset = *base
	cloned.Exprs = cloneExprs(p.Exprs)
	return cloned, err
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalProjection) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	return corDefCauss
}

// PhysicalTopN is the physical operator of topN.
type PhysicalTopN struct {
	basePhysicalCauset

	ByItems []*soliton.ByItems
	Offset  uint64
	Count   uint64
}

// Clone implements PhysicalCauset interface.
func (lt *PhysicalTopN) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalTopN)
	*cloned = *lt
	base, err := lt.basePhysicalCauset.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalCauset = *base
	cloned.ByItems = make([]*soliton.ByItems, 0, len(lt.ByItems))
	for _, it := range lt.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (lt *PhysicalTopN) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(lt.ByItems))
	for _, item := range lt.ByItems {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(item.Expr)...)
	}
	return corDefCauss
}

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	PhysicalHashJoin

	CanUseCache bool
	Concurrency int
	OuterSchema []*memex.CorrelatedDeferredCauset
}

// Clone implements PhysicalCauset interface.
func (la *PhysicalApply) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalApply)
	base, err := la.PhysicalHashJoin.Clone()
	if err != nil {
		return nil, err
	}
	hj := base.(*PhysicalHashJoin)
	cloned.PhysicalHashJoin = *hj
	cloned.CanUseCache = la.CanUseCache
	cloned.Concurrency = la.Concurrency
	for _, col := range la.OuterSchema {
		cloned.OuterSchema = append(cloned.OuterSchema, col.Clone().(*memex.CorrelatedDeferredCauset))
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (la *PhysicalApply) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := la.PhysicalHashJoin.ExtractCorrelatedDefCauss()
	for i := len(corDefCauss) - 1; i >= 0; i-- {
		if la.children[0].Schema().Contains(&corDefCauss[i].DeferredCauset) {
			corDefCauss = append(corDefCauss[:i], corDefCauss[i+1:]...)
		}
	}
	return corDefCauss
}

type basePhysicalJoin struct {
	physicalSchemaProducer

	JoinType JoinType

	LeftConditions  memex.CNFExprs
	RightConditions memex.CNFExprs
	OtherConditions memex.CNFExprs

	InnerChildIdx int
	OuterJoinKeys []*memex.DeferredCauset
	InnerJoinKeys []*memex.DeferredCauset
	LeftJoinKeys  []*memex.DeferredCauset
	RightJoinKeys []*memex.DeferredCauset
	IsNullEQ      []bool
	DefaultValues []types.Causet
}

func (p *basePhysicalJoin) cloneWithSelf(newSelf PhysicalCauset) (*basePhysicalJoin, error) {
	cloned := new(basePhysicalJoin)
	base, err := p.physicalSchemaProducer.cloneWithSelf(newSelf)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	cloned.JoinType = p.JoinType
	cloned.LeftConditions = cloneExprs(p.LeftConditions)
	cloned.RightConditions = cloneExprs(p.RightConditions)
	cloned.OtherConditions = cloneExprs(p.OtherConditions)
	cloned.InnerChildIdx = p.InnerChildIdx
	cloned.OuterJoinKeys = cloneDefCauss(p.OuterJoinKeys)
	cloned.InnerJoinKeys = cloneDefCauss(p.InnerJoinKeys)
	cloned.LeftJoinKeys = cloneDefCauss(p.LeftJoinKeys)
	cloned.RightJoinKeys = cloneDefCauss(p.RightJoinKeys)
	for _, d := range p.DefaultValues {
		cloned.DefaultValues = append(cloned.DefaultValues, *d.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *basePhysicalJoin) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.LeftConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.RightConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	return corDefCauss
}

// PhysicalHashJoin represents hash join implementation of LogicalJoin.
type PhysicalHashJoin struct {
	basePhysicalJoin

	Concurrency     uint
	EqualConditions []*memex.ScalarFunction

	// use the outer causet to build a hash causet when the outer causet is smaller.
	UseOuterToBuild bool
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalHashJoin) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalHashJoin)
	base, err := p.basePhysicalJoin.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalJoin = *base
	cloned.Concurrency = p.Concurrency
	cloned.UseOuterToBuild = p.UseOuterToBuild
	for _, c := range p.EqualConditions {
		cloned.EqualConditions = append(cloned.EqualConditions, c.Clone().(*memex.ScalarFunction))
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalHashJoin) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.RightConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(fun)...)
	}
	return corDefCauss
}

// NewPhysicalHashJoin creates a new PhysicalHashJoin from LogicalJoin.
func NewPhysicalHashJoin(p *LogicalJoin, innerIdx int, useOuterToBuild bool, newStats *property.StatsInfo, prop ...*property.PhysicalProperty) *PhysicalHashJoin {
	leftJoinKeys, rightJoinKeys, isNullEQ, _ := p.GetJoinKeys()
	baseJoin := basePhysicalJoin{
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		LeftJoinKeys:    leftJoinKeys,
		RightJoinKeys:   rightJoinKeys,
		IsNullEQ:        isNullEQ,
		JoinType:        p.JoinType,
		DefaultValues:   p.DefaultValues,
		InnerChildIdx:   innerIdx,
	}
	hashJoin := PhysicalHashJoin{
		basePhysicalJoin: baseJoin,
		EqualConditions:  p.EqualConditions,
		Concurrency:      uint(p.ctx.GetStochastikVars().HashJoinConcurrency()),
		UseOuterToBuild:  useOuterToBuild,
	}.Init(p.ctx, newStats, p.blockOffset, prop...)
	return hashJoin
}

// PhysicalIndexJoin represents the plan of index look up join.
type PhysicalIndexJoin struct {
	basePhysicalJoin

	outerSchema *memex.Schema
	innerTask   task

	// Ranges stores the IndexRanges when the inner plan is index scan.
	Ranges []*ranger.Range
	// KeyOff2IdxOff maps the offsets in join key to the offsets in the index.
	KeyOff2IdxOff []int
	// IdxDefCausLens stores the length of each index column.
	IdxDefCausLens []int
	// CompareFilters stores the filters for last column if those filters need to be evaluated during execution.
	// e.g. select * from t, t1 where t.a = t1.a and t.b > t1.b and t.b < t1.b+10
	//      If there's index(t.a, t.b). All the filters can be used to construct index range but t.b > t1.b and t.b < t1.b=10
	//      need to be evaluated after we fetch the data of t1.
	// This struct stores them and evaluate them to ranges.
	CompareFilters *DefCausWithCmpFuncManager
}

// PhysicalIndexMergeJoin represents the plan of index look up merge join.
type PhysicalIndexMergeJoin struct {
	PhysicalIndexJoin

	// KeyOff2KeyOffOrderByIdx maps the offsets in join keys to the offsets in join keys order by index.
	KeyOff2KeyOffOrderByIdx []int
	// CompareFuncs causetstore the compare functions for outer join keys and inner join key.
	CompareFuncs []memex.CompareFunc
	// OuterCompareFuncs causetstore the compare functions for outer join keys and outer join
	// keys, it's for outer rows sort's convenience.
	OuterCompareFuncs []memex.CompareFunc
	// NeedOuterSort means whether outer rows should be sorted to build range.
	NeedOuterSort bool
	// Desc means whether inner child keep desc order.
	Desc bool
}

// PhysicalIndexHashJoin represents the plan of index look up hash join.
type PhysicalIndexHashJoin struct {
	PhysicalIndexJoin
	// KeepOuterOrder indicates whether keeping the output result order as the
	// outer side.
	KeepOuterOrder bool
}

// PhysicalMergeJoin represents merge join implementation of LogicalJoin.
type PhysicalMergeJoin struct {
	basePhysicalJoin

	CompareFuncs []memex.CompareFunc
	// Desc means whether inner child keep desc order.
	Desc bool
}

// PhysicalBroadCastJoin only works for TiFlash Engine, which broadcast the small causet to every replica of probe side of blocks.
type PhysicalBroadCastJoin struct {
	basePhysicalJoin
	globalChildIndex int
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalMergeJoin) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalMergeJoin)
	base, err := p.basePhysicalJoin.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalJoin = *base
	for _, cf := range p.CompareFuncs {
		cloned.CompareFuncs = append(cloned.CompareFuncs, cf)
	}
	cloned.Desc = p.Desc
	return cloned, nil
}

// PhysicalLock is the physical operator of dagger, which is used for `select ... for uFIDelate` clause.
type PhysicalLock struct {
	basePhysicalCauset

	Lock *ast.SelectLockInfo

	TblID2Handle     map[int64][]HandleDefCauss
	PartitionedBlock []causet.PartitionedBlock
}

// PhysicalLimit is the physical operator of Limit.
type PhysicalLimit struct {
	basePhysicalCauset

	Offset uint64
	Count  uint64
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalLimit) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalLimit)
	*cloned = *p
	base, err := p.basePhysicalCauset.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalCauset = *base
	return cloned, nil
}

// PhysicalUnionAll is the physical operator of UnionAll.
type PhysicalUnionAll struct {
	physicalSchemaProducer
}

type basePhysicalAgg struct {
	physicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []memex.Expression
}

func (p *basePhysicalAgg) cloneWithSelf(newSelf PhysicalCauset) (*basePhysicalAgg, error) {
	cloned := new(basePhysicalAgg)
	base, err := p.physicalSchemaProducer.cloneWithSelf(newSelf)
	if err != nil {
		return nil, err
	}
	cloned.physicalSchemaProducer = *base
	for _, aggDesc := range p.AggFuncs {
		cloned.AggFuncs = append(cloned.AggFuncs, aggDesc.Clone())
	}
	cloned.GroupByItems = cloneExprs(p.GroupByItems)
	return cloned, nil
}

func (p *basePhysicalAgg) numDistinctFunc() (num int) {
	for _, fun := range p.AggFuncs {
		if fun.HasDistinct {
			num++
		}
	}
	return
}

func (p *basePhysicalAgg) getAggFuncCostFactor() (factor float64) {
	factor = 0.0
	for _, agg := range p.AggFuncs {
		if fac, ok := aggFuncFactor[agg.Name]; ok {
			factor += fac
		} else {
			factor += aggFuncFactor["default"]
		}
	}
	if factor == 0 {
		factor = 1.0
	}
	return
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *basePhysicalAgg) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.GroupByItems)+len(p.AggFuncs))
	for _, expr := range p.GroupByItems {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
	}
	for _, fun := range p.AggFuncs {
		for _, arg := range fun.Args {
			corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(arg)...)
		}
	}
	return corDefCauss
}

// PhysicalHashAgg is hash operator of aggregate.
type PhysicalHashAgg struct {
	basePhysicalAgg
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalHashAgg) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalHashAgg)
	base, err := p.basePhysicalAgg.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalAgg = *base
	return cloned, nil
}

// NewPhysicalHashAgg creates a new PhysicalHashAgg from a LogicalAggregation.
func NewPhysicalHashAgg(la *LogicalAggregation, newStats *property.StatsInfo, prop *property.PhysicalProperty) *PhysicalHashAgg {
	agg := basePhysicalAgg{
		GroupByItems: la.GroupByItems,
		AggFuncs:     la.AggFuncs,
	}.initForHash(la.ctx, newStats, la.blockOffset, prop)
	return agg
}

// PhysicalStreamAgg is stream operator of aggregate.
type PhysicalStreamAgg struct {
	basePhysicalAgg
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalStreamAgg) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalStreamAgg)
	base, err := p.basePhysicalAgg.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalAgg = *base
	return cloned, nil
}

// PhysicalSort is the physical operator of sort, which implements a memory sort.
type PhysicalSort struct {
	basePhysicalCauset

	ByItems []*soliton.ByItems
}

// Clone implements PhysicalCauset interface.
func (ls *PhysicalSort) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalSort)
	base, err := ls.basePhysicalCauset.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalCauset = *base
	for _, it := range ls.ByItems {
		cloned.ByItems = append(cloned.ByItems, it.Clone())
	}
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (ls *PhysicalSort) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(ls.ByItems))
	for _, item := range ls.ByItems {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(item.Expr)...)
	}
	return corDefCauss
}

// NominalSort asks sort properties for its child. It is a fake operator that will not
// appear in final physical operator tree. It will be eliminated or converted to Projection.
type NominalSort struct {
	basePhysicalCauset

	// These two fields are used to switch ScalarFunctions to Constants. For these
	// NominalSorts, we need to converted to Projections check if the ScalarFunctions
	// are out of bounds. (issue #11653)
	ByItems            []*soliton.ByItems
	OnlyDeferredCauset bool
}

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	basePhysicalCauset

	Conditions []memex.Expression

	HandleDefCauss HandleDefCauss
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalUnionScan) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0)
	for _, cond := range p.Conditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(cond)...)
	}
	return corDefCauss
}

// IsPartition returns true and partition ID if it works on a partition.
func (p *PhysicalIndexScan) IsPartition() (bool, int64) {
	return p.isPartition, p.physicalBlockID
}

// IsPointGetByUniqueKey checks whether is a point get by unique key.
func (p *PhysicalIndexScan) IsPointGetByUniqueKey(sc *stmtctx.StatementContext) bool {
	return len(p.Ranges) == 1 &&
		p.Index.Unique &&
		len(p.Ranges[0].LowVal) == len(p.Index.DeferredCausets) &&
		p.Ranges[0].IsPoint(sc)
}

// PhysicalSelection represents a filter.
type PhysicalSelection struct {
	basePhysicalCauset

	Conditions []memex.Expression
}

// Clone implements PhysicalCauset interface.
func (p *PhysicalSelection) Clone() (PhysicalCauset, error) {
	cloned := new(PhysicalSelection)
	base, err := p.basePhysicalCauset.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalCauset = *base
	cloned.Conditions = cloneExprs(p.Conditions)
	return cloned, nil
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalSelection) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.Conditions))
	for _, cond := range p.Conditions {
		corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(cond)...)
	}
	return corDefCauss
}

// PhysicalMaxOneRow is the physical operator of maxOneRow.
type PhysicalMaxOneRow struct {
	basePhysicalCauset
}

// PhysicalBlockDual is the physical operator of dual.
type PhysicalBlockDual struct {
	physicalSchemaProducer

	RowCount int

	// names is used for OutputNames() method. Dual may be inited when building point get plan.
	// So it needs to hold names for itself.
	names []*types.FieldName
}

// OutputNames returns the outputting names of each column.
func (p *PhysicalBlockDual) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PhysicalBlockDual) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// PhysicalWindow is the physical operator of window function.
type PhysicalWindow struct {
	physicalSchemaProducer

	WindowFuncDescs []*aggregation.WindowFuncDesc
	PartitionBy     []property.Item
	OrderBy         []property.Item
	Frame           *WindowFrame
}

// ExtractCorrelatedDefCauss implements PhysicalCauset interface.
func (p *PhysicalWindow) ExtractCorrelatedDefCauss() []*memex.CorrelatedDeferredCauset {
	corDefCauss := make([]*memex.CorrelatedDeferredCauset, 0, len(p.WindowFuncDescs))
	for _, windowFunc := range p.WindowFuncDescs {
		for _, arg := range windowFunc.Args {
			corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(arg)...)
		}
	}
	if p.Frame != nil {
		if p.Frame.Start != nil {
			for _, expr := range p.Frame.Start.CalcFuncs {
				corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
			}
		}
		if p.Frame.End != nil {
			for _, expr := range p.Frame.End.CalcFuncs {
				corDefCauss = append(corDefCauss, memex.ExtractCorDeferredCausets(expr)...)
			}
		}
	}
	return corDefCauss
}

// PhysicalShuffle represents a shuffle plan.
// `Tail` and `DataSource` are the last plan within and the first plan following the "shuffle", respectively,
//  to build the child interlocks chain.
// Take `Window` operator for example:
//  Shuffle -> Window -> Sort -> DataSource, will be separated into:
//    ==> Shuffle: for main thread
//    ==> Window -> Sort(:Tail) -> shuffleWorker: for workers
//    ==> DataSource: for `fetchDataAndSplit` thread
type PhysicalShuffle struct {
	basePhysicalCauset

	Concurrency int
	Tail        PhysicalCauset
	DataSource  PhysicalCauset

	SplitterType PartitionSplitterType
	HashByItems  []memex.Expression
}

// PartitionSplitterType is the type of `Shuffle` interlock splitter, which splits data source into partitions.
type PartitionSplitterType int

const (
	// PartitionHashSplitterType is the splitter splits by hash.
	PartitionHashSplitterType = iota
)

// PhysicalShuffleDataSourceStub represents a data source stub of `PhysicalShuffle`,
// and actually, is executed by `interlock.shuffleWorker`.
type PhysicalShuffleDataSourceStub struct {
	physicalSchemaProducer

	// Worker points to `interlock.shuffleWorker`.
	Worker unsafe.Pointer
}

// DefCauslectCausetStatsVersion uses to collect the statistics version of the plan.
func DefCauslectCausetStatsVersion(plan PhysicalCauset, statsInfos map[string]uint64) map[string]uint64 {
	for _, child := range plan.Children() {
		statsInfos = DefCauslectCausetStatsVersion(child, statsInfos)
	}
	switch copCauset := plan.(type) {
	case *PhysicalBlockReader:
		statsInfos = DefCauslectCausetStatsVersion(copCauset.blockCauset, statsInfos)
	case *PhysicalIndexReader:
		statsInfos = DefCauslectCausetStatsVersion(copCauset.indexCauset, statsInfos)
	case *PhysicalIndexLookUpReader:
		// For index loop up, only the indexCauset is necessary,
		// because they use the same stats and we do not set the stats info for blockCauset.
		statsInfos = DefCauslectCausetStatsVersion(copCauset.indexCauset, statsInfos)
	case *PhysicalIndexScan:
		statsInfos[copCauset.Block.Name.O] = copCauset.stats.StatsVersion
	case *PhysicalBlockScan:
		statsInfos[copCauset.Block.Name.O] = copCauset.stats.StatsVersion
	}

	return statsInfos
}

// PhysicalShow represents a show plan.
type PhysicalShow struct {
	physicalSchemaProducer

	ShowContents
}

// PhysicalShowDBSJobs is for showing DBS job list.
type PhysicalShowDBSJobs struct {
	physicalSchemaProducer

	JobNumber int64
}

// BuildMergeJoinCauset builds a PhysicalMergeJoin from the given fields. Currently, it is only used for test purpose.
func BuildMergeJoinCauset(ctx stochastikctx.Context, joinType JoinType, leftKeys, rightKeys []*memex.DeferredCauset) *PhysicalMergeJoin {
	baseJoin := basePhysicalJoin{
		JoinType:      joinType,
		DefaultValues: []types.Causet{types.NewCauset(1), types.NewCauset(1)},
		LeftJoinKeys:  leftKeys,
		RightJoinKeys: rightKeys,
	}
	return PhysicalMergeJoin{basePhysicalJoin: baseJoin}.Init(ctx, nil, 0)
}

// SafeClone clones this PhysicalCauset and handles its panic.
func SafeClone(v PhysicalCauset) (_ PhysicalCauset, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()
	return v.Clone()
}
