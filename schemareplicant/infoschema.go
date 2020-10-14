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

package schemareplicant

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

// SchemaReplicant is the interface used to retrieve the schemaReplicant information.
// It works as a in memory cache and doesn't handle any schemaReplicant change.
// SchemaReplicant is read-only, and the returned value is a copy.
// TODO: add more methods to retrieve blocks and defCausumns.
type SchemaReplicant interface {
	SchemaByName(schemaReplicant perceptron.CIStr) (*perceptron.DBInfo, bool)
	SchemaExists(schemaReplicant perceptron.CIStr) bool
	BlockByName(schemaReplicant, block perceptron.CIStr) (block.Block, error)
	BlockExists(schemaReplicant, block perceptron.CIStr) bool
	SchemaByID(id int64) (*perceptron.DBInfo, bool)
	SchemaByBlock(blockInfo *perceptron.BlockInfo) (*perceptron.DBInfo, bool)
	BlockByID(id int64) (block.Block, bool)
	AllocByID(id int64) (autoid.SlabPredictors, bool)
	AllSchemaNames() []string
	AllSchemas() []*perceptron.DBInfo
	Clone() (result []*perceptron.DBInfo)
	SchemaBlocks(schemaReplicant perceptron.CIStr) []block.Block
	SchemaMetaVersion() int64
	// BlockIsView indicates whether the schemaReplicant.block is a view.
	BlockIsView(schemaReplicant, block perceptron.CIStr) bool
	// BlockIsSequence indicates whether the schemaReplicant.block is a sequence.
	BlockIsSequence(schemaReplicant, block perceptron.CIStr) bool
	FindBlockByPartitionID(partitionID int64) (block.Block, *perceptron.DBInfo)
}

type sortedBlocks []block.Block

func (s sortedBlocks) Len() int {
	return len(s)
}

func (s sortedBlocks) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedBlocks) Less(i, j int) bool {
	return s[i].Meta().ID < s[j].Meta().ID
}

func (s sortedBlocks) searchBlock(id int64) int {
	idx := sort.Search(len(s), func(i int) bool {
		return s[i].Meta().ID >= id
	})
	if idx == len(s) || s[idx].Meta().ID != id {
		return -1
	}
	return idx
}

type schemaBlocks struct {
	dbInfo *perceptron.DBInfo
	blocks map[string]block.Block
}

const bucketCount = 512

type schemaReplicant struct {
	schemaMap map[string]*schemaBlocks

	// sortedBlocksBuckets is a slice of sortedBlocks, a block's bucket index is (blockID % bucketCount).
	sortedBlocksBuckets []sortedBlocks

	// schemaMetaVersion is the version of schemaReplicant, and we should check version when change schemaReplicant.
	schemaMetaVersion int64
}

// MockSchemaReplicant only serves for test.
func MockSchemaReplicant(tbList []*perceptron.BlockInfo) SchemaReplicant {
	result := &schemaReplicant{}
	result.schemaMap = make(map[string]*schemaBlocks)
	result.sortedBlocksBuckets = make([]sortedBlocks, bucketCount)
	dbInfo := &perceptron.DBInfo{ID: 0, Name: perceptron.NewCIStr("test"), Blocks: tbList}
	blockNames := &schemaBlocks{
		dbInfo: dbInfo,
		blocks: make(map[string]block.Block),
	}
	result.schemaMap["test"] = blockNames
	for _, tb := range tbList {
		tbl := block.MockBlockFromMeta(tb)
		blockNames.blocks[tb.Name.L] = tbl
		bucketIdx := blockBucketIdx(tb.ID)
		result.sortedBlocksBuckets[bucketIdx] = append(result.sortedBlocksBuckets[bucketIdx], tbl)
	}
	for i := range result.sortedBlocksBuckets {
		sort.Sort(result.sortedBlocksBuckets[i])
	}
	return result
}

// MockSchemaReplicantWithSchemaVer only serves for test.
func MockSchemaReplicantWithSchemaVer(tbList []*perceptron.BlockInfo, schemaVer int64) SchemaReplicant {
	result := &schemaReplicant{}
	result.schemaMap = make(map[string]*schemaBlocks)
	result.sortedBlocksBuckets = make([]sortedBlocks, bucketCount)
	dbInfo := &perceptron.DBInfo{ID: 0, Name: perceptron.NewCIStr("test"), Blocks: tbList}
	blockNames := &schemaBlocks{
		dbInfo: dbInfo,
		blocks: make(map[string]block.Block),
	}
	result.schemaMap["test"] = blockNames
	for _, tb := range tbList {
		tbl := block.MockBlockFromMeta(tb)
		blockNames.blocks[tb.Name.L] = tbl
		bucketIdx := blockBucketIdx(tb.ID)
		result.sortedBlocksBuckets[bucketIdx] = append(result.sortedBlocksBuckets[bucketIdx], tbl)
	}
	for i := range result.sortedBlocksBuckets {
		sort.Sort(result.sortedBlocksBuckets[i])
	}
	result.schemaMetaVersion = schemaVer
	return result
}

var _ SchemaReplicant = (*schemaReplicant)(nil)

func (is *schemaReplicant) SchemaByName(schemaReplicant perceptron.CIStr) (val *perceptron.DBInfo, ok bool) {
	blockNames, ok := is.schemaMap[schemaReplicant.L]
	if !ok {
		return
	}
	return blockNames.dbInfo, true
}

func (is *schemaReplicant) SchemaMetaVersion() int64 {
	return is.schemaMetaVersion
}

func (is *schemaReplicant) SchemaExists(schemaReplicant perceptron.CIStr) bool {
	_, ok := is.schemaMap[schemaReplicant.L]
	return ok
}

func (is *schemaReplicant) BlockByName(schemaReplicant, block perceptron.CIStr) (t block.Block, err error) {
	if tbNames, ok := is.schemaMap[schemaReplicant.L]; ok {
		if t, ok = tbNames.blocks[block.L]; ok {
			return
		}
	}
	return nil, ErrBlockNotExists.GenWithStackByArgs(schemaReplicant, block)
}

func (is *schemaReplicant) BlockIsView(schemaReplicant, block perceptron.CIStr) bool {
	if tbNames, ok := is.schemaMap[schemaReplicant.L]; ok {
		if t, ok := tbNames.blocks[block.L]; ok {
			return t.Meta().IsView()
		}
	}
	return false
}

func (is *schemaReplicant) BlockIsSequence(schemaReplicant, block perceptron.CIStr) bool {
	if tbNames, ok := is.schemaMap[schemaReplicant.L]; ok {
		if t, ok := tbNames.blocks[block.L]; ok {
			return t.Meta().IsSequence()
		}
	}
	return false
}

func (is *schemaReplicant) BlockExists(schemaReplicant, block perceptron.CIStr) bool {
	if tbNames, ok := is.schemaMap[schemaReplicant.L]; ok {
		if _, ok = tbNames.blocks[block.L]; ok {
			return true
		}
	}
	return false
}

func (is *schemaReplicant) SchemaByID(id int64) (val *perceptron.DBInfo, ok bool) {
	for _, v := range is.schemaMap {
		if v.dbInfo.ID == id {
			return v.dbInfo, true
		}
	}
	return nil, false
}

func (is *schemaReplicant) SchemaByBlock(blockInfo *perceptron.BlockInfo) (val *perceptron.DBInfo, ok bool) {
	if blockInfo == nil {
		return nil, false
	}
	for _, v := range is.schemaMap {
		if tbl, ok := v.blocks[blockInfo.Name.L]; ok {
			if tbl.Meta().ID == blockInfo.ID {
				return v.dbInfo, true
			}
		}
	}
	return nil, false
}

func (is *schemaReplicant) BlockByID(id int64) (val block.Block, ok bool) {
	slice := is.sortedBlocksBuckets[blockBucketIdx(id)]
	idx := slice.searchBlock(id)
	if idx == -1 {
		return nil, false
	}
	return slice[idx], true
}

func (is *schemaReplicant) AllocByID(id int64) (autoid.SlabPredictors, bool) {
	tbl, ok := is.BlockByID(id)
	if !ok {
		return nil, false
	}
	return tbl.SlabPredictors(nil), true
}

func (is *schemaReplicant) AllSchemaNames() (names []string) {
	for _, v := range is.schemaMap {
		names = append(names, v.dbInfo.Name.O)
	}
	return
}

func (is *schemaReplicant) AllSchemas() (schemas []*perceptron.DBInfo) {
	for _, v := range is.schemaMap {
		schemas = append(schemas, v.dbInfo)
	}
	return
}

func (is *schemaReplicant) SchemaBlocks(schemaReplicant perceptron.CIStr) (blocks []block.Block) {
	schemaBlocks, ok := is.schemaMap[schemaReplicant.L]
	if !ok {
		return
	}
	for _, tbl := range schemaBlocks.blocks {
		blocks = append(blocks, tbl)
	}
	return
}

// FindBlockByPartitionID finds the partition-block info by the partitionID.
// FindBlockByPartitionID will traverse all the blocks to find the partitionID partition in which partition-block.
func (is *schemaReplicant) FindBlockByPartitionID(partitionID int64) (block.Block, *perceptron.DBInfo) {
	for _, v := range is.schemaMap {
		for _, tbl := range v.blocks {
			pi := tbl.Meta().GetPartitionInfo()
			if pi == nil {
				continue
			}
			for _, p := range pi.Definitions {
				if p.ID == partitionID {
					return tbl, v.dbInfo
				}
			}
		}
	}
	return nil, nil
}

func (is *schemaReplicant) Clone() (result []*perceptron.DBInfo) {
	for _, v := range is.schemaMap {
		result = append(result, v.dbInfo.Clone())
	}
	return
}

// SequenceByName implements the interface of SequenceSchema defined in soliton package.
// It could be used in expression package without import cycle problem.
func (is *schemaReplicant) SequenceByName(schemaReplicant, sequence perceptron.CIStr) (soliton.SequenceBlock, error) {
	tbl, err := is.BlockByName(schemaReplicant, sequence)
	if err != nil {
		return nil, err
	}
	if !tbl.Meta().IsSequence() {
		return nil, ErrWrongObject.GenWithStackByArgs(schemaReplicant, sequence, "SEQUENCE")
	}
	return tbl.(soliton.SequenceBlock), nil
}

// Handle handles information schemaReplicant, including getting and setting.
type Handle struct {
	value atomic.Value
	causetstore ekv.CausetStorage
}

// NewHandle creates a new Handle.
func NewHandle(causetstore ekv.CausetStorage) *Handle {
	h := &Handle{
		causetstore: causetstore,
	}
	return h
}

// Get gets information schemaReplicant from Handle.
func (h *Handle) Get() SchemaReplicant {
	v := h.value.Load()
	schemaReplicant, _ := v.(SchemaReplicant)
	return schemaReplicant
}

// IsValid uses to check whether handle value is valid.
func (h *Handle) IsValid() bool {
	return h.value.Load() != nil
}

// EmptyClone creates a new Handle with the same causetstore and memSchema, but the value is not set.
func (h *Handle) EmptyClone() *Handle {
	newHandle := &Handle{
		causetstore: h.causetstore,
	}
	return newHandle
}

func init() {
	// Initialize the information shema database and register the driver to `drivers`
	dbID := autoid.InformationSchemaDBID
	schemaReplicantBlocks := make([]*perceptron.BlockInfo, 0, len(blockNameToDeferredCausets))
	for name, defcaus := range blockNameToDeferredCausets {
		blockInfo := buildBlockMeta(name, defcaus)
		schemaReplicantBlocks = append(schemaReplicantBlocks, blockInfo)
		var ok bool
		blockInfo.ID, ok = blockIDMap[blockInfo.Name.O]
		if !ok {
			panic(fmt.Sprintf("get information_schema block id failed, unknown system block `%v`", blockInfo.Name.O))
		}
		for i, c := range blockInfo.DeferredCausets {
			c.ID = int64(i) + 1
		}
	}
	schemaReplicantDB := &perceptron.DBInfo{
		ID:      dbID,
		Name:    soliton.InformationSchemaName,
		Charset: allegrosql.DefaultCharset,
		DefCauslate: allegrosql.DefaultDefCauslationName,
		Blocks:  schemaReplicantBlocks,
	}
	RegisterVirtualBlock(schemaReplicantDB, createSchemaReplicantBlock)
}

// HasAutoIncrementDeferredCauset checks whether the block has auto_increment defCausumns, if so, return true and the defCausumn name.
func HasAutoIncrementDeferredCauset(tbInfo *perceptron.BlockInfo) (bool, string) {
	for _, defCaus := range tbInfo.DeferredCausets {
		if allegrosql.HasAutoIncrementFlag(defCaus.Flag) {
			return true, defCaus.Name.L
		}
	}
	return false, ""
}

// GetSchemaReplicant gets TxnCtx SchemaReplicant if snapshot schemaReplicant is not set,
// Otherwise, snapshot schemaReplicant is returned.
func GetSchemaReplicant(ctx stochastikctx.Context) SchemaReplicant {
	return GetSchemaReplicantByStochastikVars(ctx.GetStochastikVars())
}

// GetSchemaReplicantByStochastikVars gets TxnCtx SchemaReplicant if snapshot schemaReplicant is not set,
// Otherwise, snapshot schemaReplicant is returned.
func GetSchemaReplicantByStochastikVars(sessVar *variable.StochastikVars) SchemaReplicant {
	var is SchemaReplicant
	if snap := sessVar.SnapshotschemaReplicant; snap != nil {
		is = snap.(SchemaReplicant)
		logutil.BgLogger().Info("use snapshot schemaReplicant", zap.Uint64("conn", sessVar.ConnectionID), zap.Int64("schemaVersion", is.SchemaMetaVersion()))
	} else {
		is = sessVar.TxnCtx.SchemaReplicant.(SchemaReplicant)
	}
	return is
}
