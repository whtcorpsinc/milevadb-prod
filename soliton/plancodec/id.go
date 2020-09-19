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

package plancodec

import "strconv"

const (
	// TypeSel is the type of Selection.
	TypeSel = "Selection"
	// TypeSet is the type of Set.
	TypeSet = "Set"
	// TypeProj is the type of Projection.
	TypeProj = "Projection"
	// TypeAgg is the type of Aggregation.
	TypeAgg = "Aggregation"
	// TypeStreamAgg is the type of StreamAgg.
	TypeStreamAgg = "StreamAgg"
	// TypeHashAgg is the type of HashAgg.
	TypeHashAgg = "HashAgg"
	// TypeShow is the type of show.
	TypeShow = "Show"
	// TypeJoin is the type of Join.
	TypeJoin = "Join"
	// TypeUnion is the type of Union.
	TypeUnion = "Union"
	// TypePartitionUnion is the type of PartitionUnion
	TypePartitionUnion = "PartitionUnion"
	// TypeBlockScan is the type of BlockScan.
	TypeBlockScan = "BlockScan"
	// TypeMemBlockScan is the type of BlockScan.
	TypeMemBlockScan = "MemBlockScan"
	// TypeUnionScan is the type of UnionScan.
	TypeUnionScan = "UnionScan"
	// TypeIdxScan is the type of IndexScan.
	TypeIdxScan = "IndexScan"
	// TypeSort is the type of Sort.
	TypeSort = "Sort"
	// TypeTopN is the type of TopN.
	TypeTopN = "TopN"
	// TypeLimit is the type of Limit.
	TypeLimit = "Limit"
	// TypeHashJoin is the type of hash join.
	TypeHashJoin = "HashJoin"
	// TypeBroadcastJoin is the type of broad cast join.
	TypeBroadcastJoin = "BroadcastJoin"
	// TypeMergeJoin is the type of merge join.
	TypeMergeJoin = "MergeJoin"
	// TypeIndexJoin is the type of index look up join.
	TypeIndexJoin = "IndexJoin"
	// TypeIndexMergeJoin is the type of index look up merge join.
	TypeIndexMergeJoin = "IndexMergeJoin"
	// TypeIndexHashJoin is the type of index nested loop hash join.
	TypeIndexHashJoin = "IndexHashJoin"
	// TypeApply is the type of Apply.
	TypeApply = "Apply"
	// TypeMaxOneRow is the type of MaxOneRow.
	TypeMaxOneRow = "MaxOneRow"
	// TypeExists is the type of Exists.
	TypeExists = "Exists"
	// TypeDual is the type of BlockDual.
	TypeDual = "BlockDual"
	// TypeLock is the type of SelectLock.
	TypeLock = "SelectLock"
	// TypeInsert is the type of Insert
	TypeInsert = "Insert"
	// TypeUFIDelate is the type of UFIDelate.
	TypeUFIDelate = "UFIDelate"
	// TypeDelete is the type of Delete.
	TypeDelete = "Delete"
	// TypeIndexLookUp is the type of IndexLookUp.
	TypeIndexLookUp = "IndexLookUp"
	// TypeBlockReader is the type of BlockReader.
	TypeBlockReader = "BlockReader"
	// TypeIndexReader is the type of IndexReader.
	TypeIndexReader = "IndexReader"
	// TypeWindow is the type of Window.
	TypeWindow = "Window"
	// TypeShuffle is the type of Shuffle.
	TypeShuffle = "Shuffle"
	// TypeShuffleDataSourceStub is the type of Shuffle.
	TypeShuffleDataSourceStub = "ShuffleDataSourceStub"
	// TypeEinsteinDBSingleGather is the type of EinsteinDBSingleGather.
	TypeEinsteinDBSingleGather = "EinsteinDBSingleGather"
	// TypeIndexMerge is the type of IndexMergeReader
	TypeIndexMerge = "IndexMerge"
	// TypePointGet is the type of PointGetPlan.
	TypePointGet = "Point_Get"
	// TypeShowDBSJobs is the type of show dbs jobs.
	TypeShowDBSJobs = "ShowDBSJobs"
	// TypeBatchPointGet is the type of BatchPointGetPlan.
	TypeBatchPointGet = "Batch_Point_Get"
	// TypeClusterMemBlockReader is the type of BlockReader.
	TypeClusterMemBlockReader = "ClusterMemBlockReader"
	// TypeDataSource is the type of DataSource.
	TypeDataSource = "DataSource"
)

// plan id.
// Attention: for compatibility of encode/decode plan, The plan id shouldn't be changed.
const (
	typeSelID                 int = 1
	typeSetID                 int = 2
	typeProjID                int = 3
	typeAggID                 int = 4
	typeStreamAggID           int = 5
	typeHashAggID             int = 6
	typeShowID                int = 7
	typeJoinID                int = 8
	typeUnionID               int = 9
	typeBlockScanID           int = 10
	typeMemBlockScanID        int = 11
	typeUnionScanID           int = 12
	typeIdxScanID             int = 13
	typeSortID                int = 14
	typeTopNID                int = 15
	typeLimitID               int = 16
	typeHashJoinID            int = 17
	typeMergeJoinID           int = 18
	typeIndexJoinID           int = 19
	typeIndexMergeJoinID      int = 20
	typeIndexHashJoinID       int = 21
	typeApplyID               int = 22
	typeMaxOneRowID           int = 23
	typeExistsID              int = 24
	typeDualID                int = 25
	typeLockID                int = 26
	typeInsertID              int = 27
	typeUFIDelateID              int = 28
	typeDeleteID              int = 29
	typeIndexLookUpID         int = 30
	typeBlockReaderID         int = 31
	typeIndexReaderID         int = 32
	typeWindowID              int = 33
	typeEinsteinDBSingleGatherID    int = 34
	typeIndexMergeID          int = 35
	typePointGet              int = 36
	typeShowDBSJobs           int = 37
	typeBatchPointGet         int = 38
	typeClusterMemBlockReader int = 39
	typeDataSourceID          int = 40
)

// TypeStringToPhysicalID converts the plan type string to plan id.
func TypeStringToPhysicalID(tp string) int {
	switch tp {
	case TypeSel:
		return typeSelID
	case TypeSet:
		return typeSetID
	case TypeProj:
		return typeProjID
	case TypeAgg:
		return typeAggID
	case TypeStreamAgg:
		return typeStreamAggID
	case TypeHashAgg:
		return typeHashAggID
	case TypeShow:
		return typeShowID
	case TypeJoin:
		return typeJoinID
	case TypeUnion:
		return typeUnionID
	case TypeBlockScan:
		return typeBlockScanID
	case TypeMemBlockScan:
		return typeMemBlockScanID
	case TypeUnionScan:
		return typeUnionScanID
	case TypeIdxScan:
		return typeIdxScanID
	case TypeSort:
		return typeSortID
	case TypeTopN:
		return typeTopNID
	case TypeLimit:
		return typeLimitID
	case TypeHashJoin:
		return typeHashJoinID
	case TypeMergeJoin:
		return typeMergeJoinID
	case TypeIndexJoin:
		return typeIndexJoinID
	case TypeIndexMergeJoin:
		return typeIndexMergeJoinID
	case TypeIndexHashJoin:
		return typeIndexHashJoinID
	case TypeApply:
		return typeApplyID
	case TypeMaxOneRow:
		return typeMaxOneRowID
	case TypeExists:
		return typeExistsID
	case TypeDual:
		return typeDualID
	case TypeLock:
		return typeLockID
	case TypeInsert:
		return typeInsertID
	case TypeUFIDelate:
		return typeUFIDelateID
	case TypeDelete:
		return typeDeleteID
	case TypeIndexLookUp:
		return typeIndexLookUpID
	case TypeBlockReader:
		return typeBlockReaderID
	case TypeIndexReader:
		return typeIndexReaderID
	case TypeWindow:
		return typeWindowID
	case TypeEinsteinDBSingleGather:
		return typeEinsteinDBSingleGatherID
	case TypeIndexMerge:
		return typeIndexMergeID
	case TypePointGet:
		return typePointGet
	case TypeShowDBSJobs:
		return typeShowDBSJobs
	case TypeBatchPointGet:
		return typeBatchPointGet
	case TypeClusterMemBlockReader:
		return typeClusterMemBlockReader
	case TypeDataSource:
		return typeDataSourceID
	}
	// Should never reach here.
	return 0
}

// PhysicalIDToTypeString converts the plan id to plan type string.
func PhysicalIDToTypeString(id int) string {
	switch id {
	case typeSelID:
		return TypeSel
	case typeSetID:
		return TypeSet
	case typeProjID:
		return TypeProj
	case typeAggID:
		return TypeAgg
	case typeStreamAggID:
		return TypeStreamAgg
	case typeHashAggID:
		return TypeHashAgg
	case typeShowID:
		return TypeShow
	case typeJoinID:
		return TypeJoin
	case typeUnionID:
		return TypeUnion
	case typeBlockScanID:
		return TypeBlockScan
	case typeMemBlockScanID:
		return TypeMemBlockScan
	case typeUnionScanID:
		return TypeUnionScan
	case typeIdxScanID:
		return TypeIdxScan
	case typeSortID:
		return TypeSort
	case typeTopNID:
		return TypeTopN
	case typeLimitID:
		return TypeLimit
	case typeHashJoinID:
		return TypeHashJoin
	case typeMergeJoinID:
		return TypeMergeJoin
	case typeIndexJoinID:
		return TypeIndexJoin
	case typeIndexMergeJoinID:
		return TypeIndexMergeJoin
	case typeIndexHashJoinID:
		return TypeIndexHashJoin
	case typeApplyID:
		return TypeApply
	case typeMaxOneRowID:
		return TypeMaxOneRow
	case typeExistsID:
		return TypeExists
	case typeDualID:
		return TypeDual
	case typeLockID:
		return TypeLock
	case typeInsertID:
		return TypeInsert
	case typeUFIDelateID:
		return TypeUFIDelate
	case typeDeleteID:
		return TypeDelete
	case typeIndexLookUpID:
		return TypeIndexLookUp
	case typeBlockReaderID:
		return TypeBlockReader
	case typeIndexReaderID:
		return TypeIndexReader
	case typeWindowID:
		return TypeWindow
	case typeEinsteinDBSingleGatherID:
		return TypeEinsteinDBSingleGather
	case typeIndexMergeID:
		return TypeIndexMerge
	case typePointGet:
		return TypePointGet
	case typeShowDBSJobs:
		return TypeShowDBSJobs
	case typeBatchPointGet:
		return TypeBatchPointGet
	case typeClusterMemBlockReader:
		return TypeClusterMemBlockReader
	}

	// Should never reach here.
	return "UnknownPlanID" + strconv.Itoa(id)
}
