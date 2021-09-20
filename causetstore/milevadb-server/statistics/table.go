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
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/atomic"
)

const (
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40
	pseudoDefCausSize = 8.0

	outOfRangeBetweenRate = 100
)

const (
	// PseudoVersion means the pseudo statistics version is 0.
	PseudoVersion uint64 = 0

	// PseudoRowCount export for other pkg to use.
	// When we haven't analyzed a causet, we use pseudo statistics to estimate costs.
	// It has event count 10000, equal condition selects 1/1000 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/40 of total rows.
	PseudoRowCount = 10000
)

// Block represents statistics for a causet.
type Block struct {
	HistDefCausl
	Version       uint64
	Name          string
	ExtendedStats *ExtendedStatsDefCausl
}

// ExtendedStatsKey is the key for cached item of a allegrosql.stats_extended record.
type ExtendedStatsKey struct {
	StatsName string
	EDB       string
}

// ExtendedStatsItem is the cached item of a allegrosql.stats_extended record.
type ExtendedStatsItem struct {
	DefCausIDs []int64
	Tp         uint8
	ScalarVals float64
	StringVals string
}

// ExtendedStatsDefCausl is a collection of cached items for allegrosql.stats_extended records.
type ExtendedStatsDefCausl struct {
	Stats                map[ExtendedStatsKey]*ExtendedStatsItem
	LastUFIDelateVersion uint64
}

// NewExtendedStatsDefCausl allocate an ExtendedStatsDefCausl struct.
func NewExtendedStatsDefCausl() *ExtendedStatsDefCausl {
	return &ExtendedStatsDefCausl{Stats: make(map[ExtendedStatsKey]*ExtendedStatsItem)}
}

// HistDefCausl is a collection of histogram. It collects enough information for plan to calculate the selectivity.
type HistDefCausl struct {
	PhysicalID      int64
	DeferredCausets map[int64]*DeferredCauset
	Indices         map[int64]*Index
	// Idx2DeferredCausetIDs maps the index id to its column ids. It's used to calculate the selectivity in causet.
	Idx2DeferredCausetIDs map[int64][]int64
	// DefCausID2IdxID maps the column id to index id whose first column is it. It's used to calculate the selectivity in causet.
	DefCausID2IdxID map[int64]int64
	Count           int64
	ModifyCount     int64 // Total modify count in a causet.

	// HavePhysicalID is true means this HistDefCausl is from single causet and have its ID's information.
	// The physical id is used when try to load column stats from storage.
	HavePhysicalID bool
	Pseudo         bool
}

// MemoryUsage returns the total memory usage of this Block.
// it will only calc the size of DeferredCausets and Indices stats data of causet.
// We ignore the size of other spacetimedata in Block
func (t *Block) MemoryUsage() (sum int64) {
	for _, col := range t.DeferredCausets {
		if col != nil {
			sum += col.MemoryUsage()
		}
	}
	for _, index := range t.Indices {
		if index != nil {
			sum += index.MemoryUsage()
		}
	}
	return
}

// Copy copies the current causet.
func (t *Block) Copy() *Block {
	newHistDefCausl := HistDefCausl{
		PhysicalID:      t.PhysicalID,
		HavePhysicalID:  t.HavePhysicalID,
		Count:           t.Count,
		DeferredCausets: make(map[int64]*DeferredCauset, len(t.DeferredCausets)),
		Indices:         make(map[int64]*Index, len(t.Indices)),
		Pseudo:          t.Pseudo,
		ModifyCount:     t.ModifyCount,
	}
	for id, col := range t.DeferredCausets {
		newHistDefCausl.DeferredCausets[id] = col
	}
	for id, idx := range t.Indices {
		newHistDefCausl.Indices[id] = idx
	}
	nt := &Block{
		HistDefCausl: newHistDefCausl,
		Version:      t.Version,
		Name:         t.Name,
	}
	if t.ExtendedStats != nil {
		newExtStatsDefCausl := &ExtendedStatsDefCausl{
			Stats:                make(map[ExtendedStatsKey]*ExtendedStatsItem),
			LastUFIDelateVersion: t.ExtendedStats.LastUFIDelateVersion,
		}
		for key, item := range t.ExtendedStats.Stats {
			newExtStatsDefCausl.Stats[key] = item
		}
		nt.ExtendedStats = newExtStatsDefCausl
	}
	return nt
}

// String implements Stringer interface.
func (t *Block) String() string {
	strs := make([]string, 0, len(t.DeferredCausets)+1)
	strs = append(strs, fmt.Sprintf("Block:%d Count:%d", t.PhysicalID, t.Count))
	defcaus := make([]*DeferredCauset, 0, len(t.DeferredCausets))
	for _, col := range t.DeferredCausets {
		defcaus = append(defcaus, col)
	}
	sort.Slice(defcaus, func(i, j int) bool { return defcaus[i].ID < defcaus[j].ID })
	for _, col := range defcaus {
		strs = append(strs, col.String())
	}
	idxs := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i].ID < idxs[j].ID })
	for _, idx := range idxs {
		strs = append(strs, idx.String())
	}
	// TODO: concat content of ExtendedStatsDefCausl
	return strings.Join(strs, "\n")
}

// IndexStartWithDeferredCauset finds the first index whose first column is the given column.
func (t *Block) IndexStartWithDeferredCauset(colName string) *Index {
	for _, index := range t.Indices {
		if index.Info.DeferredCausets[0].Name.L == colName {
			return index
		}
	}
	return nil
}

// DeferredCausetByName finds the statistics.DeferredCauset for the given column.
func (t *Block) DeferredCausetByName(colName string) *DeferredCauset {
	for _, c := range t.DeferredCausets {
		if c.Info.Name.L == colName {
			return c
		}
	}
	return nil
}

type blockDeferredCausetID struct {
	TableID          int64
	DeferredCausetID int64
}

type neededDeferredCausetMap struct {
	m       sync.Mutex
	defcaus map[blockDeferredCausetID]struct{}
}

func (n *neededDeferredCausetMap) AllDefCauss() []blockDeferredCausetID {
	n.m.Lock()
	keys := make([]blockDeferredCausetID, 0, len(n.defcaus))
	for key := range n.defcaus {
		keys = append(keys, key)
	}
	n.m.Unlock()
	return keys
}

func (n *neededDeferredCausetMap) insert(col blockDeferredCausetID) {
	n.m.Lock()
	n.defcaus[col] = struct{}{}
	n.m.Unlock()
}

func (n *neededDeferredCausetMap) Delete(col blockDeferredCausetID) {
	n.m.Lock()
	delete(n.defcaus, col)
	n.m.Unlock()
}

// RatioOfPseudoEstimate means if modifyCount / statsTblCount is greater than this ratio, we think the stats is invalid
// and use pseudo estimation.
var RatioOfPseudoEstimate = atomic.NewFloat64(0.7)

// IsOutdated returns true if the causet stats is outdated.
func (t *Block) IsOutdated() bool {
	if t.Count > 0 && float64(t.ModifyCount)/float64(t.Count) > RatioOfPseudoEstimate.Load() {
		return true
	}
	return false
}

// DeferredCausetGreaterRowCount estimates the event count where the column greater than value.
func (t *Block) DeferredCausetGreaterRowCount(sc *stmtctx.StatementContext, value types.Causet, colID int64) float64 {
	c, ok := t.DeferredCausets[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoLessRate
	}
	return c.greaterRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// DeferredCausetLessRowCount estimates the event count where the column less than value. Note that null values are not counted.
func (t *Block) DeferredCausetLessRowCount(sc *stmtctx.StatementContext, value types.Causet, colID int64) float64 {
	c, ok := t.DeferredCausets[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoLessRate
	}
	return c.lessRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// DeferredCausetBetweenRowCount estimates the event count where column greater or equal to a and less than b.
func (t *Block) DeferredCausetBetweenRowCount(sc *stmtctx.StatementContext, a, b types.Causet, colID int64) float64 {
	c, ok := t.DeferredCausets[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoBetweenRate
	}
	count := c.BetweenRowCount(a, b)
	if a.IsNull() {
		count += float64(c.NullCount)
	}
	return count * c.GetIncreaseFactor(t.Count)
}

// DeferredCausetEqualRowCount estimates the event count where the column equals to value.
func (t *Block) DeferredCausetEqualRowCount(sc *stmtctx.StatementContext, value types.Causet, colID int64) (float64, error) {
	c, ok := t.DeferredCausets[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoEqualRate, nil
	}
	result, err := c.equalRowCount(sc, value, t.ModifyCount)
	result *= c.GetIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIntDeferredCausetRanges estimates the event count by a slice of IntDeferredCausetRange.
func (coll *HistDefCausl) GetRowCountByIntDeferredCausetRanges(sc *stmtctx.StatementContext, colID int64, intRanges []*ranger.Range) (float64, error) {
	c, ok := coll.DeferredCausets[colID]
	if !ok || c.IsInvalid(sc, coll.Pseudo) {
		if len(intRanges) == 0 {
			return 0, nil
		}
		if intRanges[0].LowVal[0].HoTT() == types.HoTTInt64 {
			return getPseudoRowCountBySignedIntRanges(intRanges, float64(coll.Count)), nil
		}
		return getPseudoRowCountByUnsignedIntRanges(intRanges, float64(coll.Count)), nil
	}
	result, err := c.GetDeferredCausetRowCount(sc, intRanges, coll.ModifyCount, true)
	result *= c.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// GetRowCountByDeferredCausetRanges estimates the event count by a slice of Range.
func (coll *HistDefCausl) GetRowCountByDeferredCausetRanges(sc *stmtctx.StatementContext, colID int64, colRanges []*ranger.Range) (float64, error) {
	c, ok := coll.DeferredCausets[colID]
	if !ok || c.IsInvalid(sc, coll.Pseudo) {
		return GetPseudoRowCountByDeferredCausetRanges(sc, float64(coll.Count), colRanges, 0)
	}
	result, err := c.GetDeferredCausetRowCount(sc, colRanges, coll.ModifyCount, false)
	result *= c.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIndexRanges estimates the event count by a slice of Range.
func (coll *HistDefCausl) GetRowCountByIndexRanges(sc *stmtctx.StatementContext, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	idx := coll.Indices[idxID]
	if idx == nil || idx.IsInvalid(coll.Pseudo) {
		defcausLen := -1
		if idx != nil && idx.Info.Unique {
			defcausLen = len(idx.Info.DeferredCausets)
		}
		return getPseudoRowCountByIndexRanges(sc, indexRanges, float64(coll.Count), defcausLen)
	}
	var result float64
	var err error
	if idx.CMSketch != nil && idx.StatsVer == Version1 {
		result, err = coll.getIndexRowCount(sc, idxID, indexRanges)
	} else {
		result, err = idx.GetRowCount(sc, indexRanges, coll.ModifyCount)
	}
	result *= idx.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// PseudoAvgCountPerValue gets a pseudo average count if histogram not exists.
func (t *Block) PseudoAvgCountPerValue() float64 {
	return float64(t.Count) / pseudoEqualRate
}

// GetOrdinalOfRangeCond gets the ordinal of the position range condition,
// if not exist, it returns the end position.
func GetOrdinalOfRangeCond(sc *stmtctx.StatementContext, ran *ranger.Range) int {
	for i := range ran.LowVal {
		a, b := ran.LowVal[i], ran.HighVal[i]
		cmp, err := a.CompareCauset(sc, &b)
		if err != nil {
			return 0
		}
		if cmp != 0 {
			return i
		}
	}
	return len(ran.LowVal)
}

// ID2UniqueID generates a new HistDefCausl whose `DeferredCausets` is built from UniqueID of given columns.
func (coll *HistDefCausl) ID2UniqueID(columns []*memex.DeferredCauset) *HistDefCausl {
	defcaus := make(map[int64]*DeferredCauset)
	for _, col := range columns {
		colHist, ok := coll.DeferredCausets[col.ID]
		if ok {
			defcaus[col.UniqueID] = colHist
		}
	}
	newDefCausl := &HistDefCausl{
		PhysicalID:      coll.PhysicalID,
		HavePhysicalID:  coll.HavePhysicalID,
		Pseudo:          coll.Pseudo,
		Count:           coll.Count,
		ModifyCount:     coll.ModifyCount,
		DeferredCausets: defcaus,
	}
	return newDefCausl
}

// GenerateHistDefCauslFromDeferredCausetInfo generates a new HistDefCausl whose DefCausID2IdxID and IdxID2DefCausIDs is built from the given parameter.
func (coll *HistDefCausl) GenerateHistDefCauslFromDeferredCausetInfo(infos []*perceptron.DeferredCausetInfo, columns []*memex.DeferredCauset) *HistDefCausl {
	newDefCausHistMap := make(map[int64]*DeferredCauset)
	colInfoID2UniqueID := make(map[int64]int64, len(columns))
	colNames2UniqueID := make(map[string]int64)
	for _, col := range columns {
		colInfoID2UniqueID[col.ID] = col.UniqueID
	}
	for _, colInfo := range infos {
		uniqueID, ok := colInfoID2UniqueID[colInfo.ID]
		if ok {
			colNames2UniqueID[colInfo.Name.L] = uniqueID
		}
	}
	for id, colHist := range coll.DeferredCausets {
		uniqueID, ok := colInfoID2UniqueID[id]
		// DefCauslect the statistics by the given columns.
		if ok {
			newDefCausHistMap[uniqueID] = colHist
		}
	}
	newIdxHistMap := make(map[int64]*Index)
	idx2DeferredCausets := make(map[int64][]int64)
	colID2IdxID := make(map[int64]int64)
	for _, idxHist := range coll.Indices {
		ids := make([]int64, 0, len(idxHist.Info.DeferredCausets))
		for _, idxDefCaus := range idxHist.Info.DeferredCausets {
			uniqueID, ok := colNames2UniqueID[idxDefCaus.Name.L]
			if !ok {
				break
			}
			ids = append(ids, uniqueID)
		}
		// If the length of the id list is 0, this index won't be used in this query.
		if len(ids) == 0 {
			continue
		}
		colID2IdxID[ids[0]] = idxHist.ID
		newIdxHistMap[idxHist.ID] = idxHist
		idx2DeferredCausets[idxHist.ID] = ids
	}
	newDefCausl := &HistDefCausl{
		PhysicalID:            coll.PhysicalID,
		HavePhysicalID:        coll.HavePhysicalID,
		Pseudo:                coll.Pseudo,
		Count:                 coll.Count,
		ModifyCount:           coll.ModifyCount,
		DeferredCausets:       newDefCausHistMap,
		Indices:               newIdxHistMap,
		DefCausID2IdxID:       colID2IdxID,
		Idx2DeferredCausetIDs: idx2DeferredCausets,
	}
	return newDefCausl
}

// isSingleDefCausIdxNullRange checks if a range is [NULL, NULL] on a single-column index.
func isSingleDefCausIdxNullRange(idx *Index, ran *ranger.Range) bool {
	if len(idx.Info.DeferredCausets) > 1 {
		return false
	}
	l, h := ran.LowVal[0], ran.HighVal[0]
	if l.IsNull() && h.IsNull() {
		return true
	}
	return false
}

// outOfRangeEQSelectivity estimates selectivities for out-of-range values.
// It assumes all modifications are insertions and all new-inserted rows are uniformly distributed
// and has the same distribution with analyzed rows, which means each unique value should have the
// same number of rows(Tot/NDV) of it.
func outOfRangeEQSelectivity(ndv, modifyRows, totalRows int64) float64 {
	if modifyRows == 0 {
		return 0 // it must be 0 since the histogram contains the whole data
	}
	if ndv < outOfRangeBetweenRate {
		ndv = outOfRangeBetweenRate // avoid inaccurate selectivity caused by small NDV
	}
	selectivity := 1 / float64(ndv) // TODO: After extracting TopN from histograms, we can minus the TopN fraction here.
	if selectivity*float64(totalRows) > float64(modifyRows) {
		selectivity = float64(modifyRows) / float64(totalRows)
	}
	return selectivity
}

// getEqualCondSelectivity gets the selectivity of the equal conditions.
func (coll *HistDefCausl) getEqualCondSelectivity(idx *Index, bytes []byte, usedDefCaussLen int) float64 {
	coverAll := len(idx.Info.DeferredCausets) == usedDefCaussLen
	// In this case, the event count is at most 1.
	if idx.Info.Unique && coverAll {
		return 1.0 / float64(idx.TotalRowCount())
	}
	val := types.NewBytesCauset(bytes)
	if idx.outOfRange(val) {
		// When the value is out of range, we could not found this value in the CM Sketch,
		// so we use heuristic methods to estimate the selectivity.
		if idx.NDV > 0 && coverAll {
			return outOfRangeEQSelectivity(idx.NDV, coll.ModifyCount, int64(idx.TotalRowCount()))
		}
		// The equal condition only uses prefix columns of the index.
		colIDs := coll.Idx2DeferredCausetIDs[idx.ID]
		var ndv int64
		for i, colID := range colIDs {
			if i >= usedDefCaussLen {
				break
			}
			ndv = mathutil.MaxInt64(ndv, coll.DeferredCausets[colID].NDV)
		}
		return outOfRangeEQSelectivity(ndv, coll.ModifyCount, int64(idx.TotalRowCount()))
	}
	return float64(idx.CMSketch.QueryBytes(bytes)) / float64(idx.TotalRowCount())
}

func (coll *HistDefCausl) getIndexRowCount(sc *stmtctx.StatementContext, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	idx := coll.Indices[idxID]
	totalCount := float64(0)
	for _, ran := range indexRanges {
		rangePosition := GetOrdinalOfRangeCond(sc, ran)
		var rangeVals []types.Causet
		// Try to enum the last range values.
		if rangePosition != len(ran.LowVal) {
			rangeVals = enumRangeValues(ran.LowVal[rangePosition], ran.HighVal[rangePosition], ran.LowExclude, ran.HighExclude)
			if rangeVals != nil {
				rangePosition++
			}
		}
		// If first one is range, just use the previous way to estimate; if it is [NULL, NULL] range
		// on single-column index, use previous way as well, because CMSketch does not contain null
		// values in this case.
		if rangePosition == 0 || isSingleDefCausIdxNullRange(idx, ran) {
			count, err := idx.GetRowCount(sc, []*ranger.Range{ran}, coll.ModifyCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			totalCount += count
			continue
		}
		var selectivity float64
		// use CM Sketch to estimate the equal conditions
		if rangeVals == nil {
			bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity = coll.getEqualCondSelectivity(idx, bytes, rangePosition)
		} else {
			bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition-1]...)
			if err != nil {
				return 0, errors.Trace(err)
			}
			prefixLen := len(bytes)
			for _, val := range rangeVals {
				bytes = bytes[:prefixLen]
				bytes, err = codec.EncodeKey(sc, bytes, val)
				if err != nil {
					return 0, err
				}
				selectivity += coll.getEqualCondSelectivity(idx, bytes, rangePosition)
			}
		}
		// use histogram to estimate the range condition
		if rangePosition != len(ran.LowVal) {
			rang := ranger.Range{
				LowVal:      []types.Causet{ran.LowVal[rangePosition]},
				LowExclude:  ran.LowExclude,
				HighVal:     []types.Causet{ran.HighVal[rangePosition]},
				HighExclude: ran.HighExclude,
			}
			var count float64
			var err error
			colIDs := coll.Idx2DeferredCausetIDs[idxID]
			var colID int64
			if rangePosition >= len(colIDs) {
				colID = -1
			} else {
				colID = colIDs[rangePosition]
			}
			// prefer index stats over column stats
			if idx, ok := coll.DefCausID2IdxID[colID]; ok {
				count, err = coll.GetRowCountByIndexRanges(sc, idx, []*ranger.Range{&rang})
			} else {
				count, err = coll.GetRowCountByDeferredCausetRanges(sc, colID, []*ranger.Range{&rang})
			}
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity = selectivity * count / float64(idx.TotalRowCount())
		}
		totalCount += selectivity * float64(idx.TotalRowCount())
	}
	if totalCount > idx.TotalRowCount() {
		totalCount = idx.TotalRowCount()
	}
	return totalCount, nil
}

const fakePhysicalID int64 = -1

// PseudoTable creates a pseudo causet statistics.
func PseudoTable(tblInfo *perceptron.TableInfo) *Block {
	pseudoHistDefCausl := HistDefCausl{
		Count:           PseudoRowCount,
		PhysicalID:      tblInfo.ID,
		HavePhysicalID:  true,
		DeferredCausets: make(map[int64]*DeferredCauset, len(tblInfo.DeferredCausets)),
		Indices:         make(map[int64]*Index, len(tblInfo.Indices)),
		Pseudo:          true,
	}
	t := &Block{
		HistDefCausl: pseudoHistDefCausl,
	}
	for _, col := range tblInfo.DeferredCausets {
		if col.State == perceptron.StatePublic {
			t.DeferredCausets[col.ID] = &DeferredCauset{
				PhysicalID: fakePhysicalID,
				Info:       col,
				IsHandle:   tblInfo.PKIsHandle && allegrosql.HasPriKeyFlag(col.Flag),
				Histogram:  *NewHistogram(col.ID, 0, 0, 0, &col.FieldType, 0, 0),
			}
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == perceptron.StatePublic {
			t.Indices[idx.ID] = &Index{
				Info:      idx,
				Histogram: *NewHistogram(idx.ID, 0, 0, 0, types.NewFieldType(allegrosql.TypeBlob), 0, 0)}
		}
	}
	return t
}

func getPseudoRowCountByIndexRanges(sc *stmtctx.StatementContext, indexRanges []*ranger.Range,
	blockRowCount float64, defcausLen int) (float64, error) {
	if blockRowCount == 0 {
		return 0, nil
	}
	var totalCount float64
	for _, indexRange := range indexRanges {
		count := blockRowCount
		i, err := indexRange.PrefixEqualLen(sc)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if i == defcausLen && !indexRange.LowExclude && !indexRange.HighExclude {
			totalCount += 1.0
			continue
		}
		if i >= len(indexRange.LowVal) {
			i = len(indexRange.LowVal) - 1
		}
		rowCount, err := GetPseudoRowCountByDeferredCausetRanges(sc, blockRowCount, []*ranger.Range{indexRange}, i)
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / blockRowCount * rowCount
		// If the condition is a = 1, b = 1, c = 1, d = 1, we think every a=1, b=1, c=1 only filtrate 1/100 data,
		// so as to avoid collapsing too fast.
		for j := 0; j < i; j++ {
			count = count / float64(100)
		}
		totalCount += count
	}
	if totalCount > blockRowCount {
		totalCount = blockRowCount / 3.0
	}
	return totalCount, nil
}

// GetPseudoRowCountByDeferredCausetRanges calculate the event count by the ranges if there's no statistics information for this column.
func GetPseudoRowCountByDeferredCausetRanges(sc *stmtctx.StatementContext, blockRowCount float64, columnRanges []*ranger.Range, colIdx int) (float64, error) {
	var rowCount float64
	var err error
	for _, ran := range columnRanges {
		if ran.LowVal[colIdx].HoTT() == types.HoTTNull && ran.HighVal[colIdx].HoTT() == types.HoTTMaxValue {
			rowCount += blockRowCount
		} else if ran.LowVal[colIdx].HoTT() == types.HoTTMinNotNull {
			nullCount := blockRowCount / pseudoEqualRate
			if ran.HighVal[colIdx].HoTT() == types.HoTTMaxValue {
				rowCount += blockRowCount - nullCount
			} else if err == nil {
				lessCount := blockRowCount / pseudoLessRate
				rowCount += lessCount - nullCount
			}
		} else if ran.HighVal[colIdx].HoTT() == types.HoTTMaxValue {
			rowCount += blockRowCount / pseudoLessRate
		} else {
			compare, err1 := ran.LowVal[colIdx].CompareCauset(sc, &ran.HighVal[colIdx])
			if err1 != nil {
				return 0, errors.Trace(err1)
			}
			if compare == 0 {
				rowCount += blockRowCount / pseudoEqualRate
			} else {
				rowCount += blockRowCount / pseudoBetweenRate
			}
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	if rowCount > blockRowCount {
		rowCount = blockRowCount
	}
	return rowCount, nil
}

func getPseudoRowCountBySignedIntRanges(intRanges []*ranger.Range, blockRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetInt64()
		if rg.LowVal[0].HoTT() == types.HoTTNull || rg.LowVal[0].HoTT() == types.HoTTMinNotNull {
			low = math.MinInt64
		}
		high := rg.HighVal[0].GetInt64()
		if rg.HighVal[0].HoTT() == types.HoTTMaxValue {
			high = math.MaxInt64
		}
		if low == math.MinInt64 && high == math.MaxInt64 {
			cnt = blockRowCount
		} else if low == math.MinInt64 {
			cnt = blockRowCount / pseudoLessRate
		} else if high == math.MaxInt64 {
			cnt = blockRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal event count is at most one.
			} else {
				cnt = blockRowCount / pseudoBetweenRate
			}
		}
		if high-low > 0 && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > blockRowCount {
		rowCount = blockRowCount
	}
	return rowCount
}

func getPseudoRowCountByUnsignedIntRanges(intRanges []*ranger.Range, blockRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetUint64()
		if rg.LowVal[0].HoTT() == types.HoTTNull || rg.LowVal[0].HoTT() == types.HoTTMinNotNull {
			low = 0
		}
		high := rg.HighVal[0].GetUint64()
		if rg.HighVal[0].HoTT() == types.HoTTMaxValue {
			high = math.MaxUint64
		}
		if low == 0 && high == math.MaxUint64 {
			cnt = blockRowCount
		} else if low == 0 {
			cnt = blockRowCount / pseudoLessRate
		} else if high == math.MaxUint64 {
			cnt = blockRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal event count is at most one.
			} else {
				cnt = blockRowCount / pseudoBetweenRate
			}
		}
		if high > low && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > blockRowCount {
		rowCount = blockRowCount
	}
	return rowCount
}

// GetAvgRowSize computes average event size for given columns.
func (coll *HistDefCausl) GetAvgRowSize(ctx stochastikctx.Context, defcaus []*memex.DeferredCauset, isEncodedKey bool, isForScan bool) (size float64) {
	stochastikVars := ctx.GetStochastikVars()
	if coll.Pseudo || len(coll.DeferredCausets) == 0 || coll.Count == 0 {
		size = pseudoDefCausSize * float64(len(defcaus))
	} else {
		for _, col := range defcaus {
			colHist, ok := coll.DeferredCausets[col.UniqueID]
			// Normally this would not happen, it is for compatibility with old version stats which
			// does not include TotDefCausSize.
			if !ok || (!colHist.IsHandle && colHist.TotDefCausSize == 0 && (colHist.NullCount != coll.Count)) {
				size += pseudoDefCausSize
				continue
			}
			// We differentiate if the column is encoded as key or value, because the resulted size
			// is different.
			if stochastikVars.EnableChunkRPC && !isForScan {
				size += colHist.AvgDefCausSizeChunkFormat(coll.Count)
			} else {
				size += colHist.AvgDefCausSize(coll.Count, isEncodedKey)
			}
		}
	}
	if stochastikVars.EnableChunkRPC && !isForScan {
		// Add 1/8 byte for each column's nullBitMap byte.
		return size + float64(len(defcaus))/8
	}
	// Add 1 byte for each column's flag byte. See `encode` for details.
	return size + float64(len(defcaus))
}

// GetAvgRowSizeListInDisk computes average event size for given columns.
func (coll *HistDefCausl) GetAvgRowSizeListInDisk(defcaus []*memex.DeferredCauset) (size float64) {
	if coll.Pseudo || len(coll.DeferredCausets) == 0 || coll.Count == 0 {
		for _, col := range defcaus {
			size += float64(chunk.EstimateTypeWidth(col.GetType()))
		}
	} else {
		for _, col := range defcaus {
			colHist, ok := coll.DeferredCausets[col.UniqueID]
			// Normally this would not happen, it is for compatibility with old version stats which
			// does not include TotDefCausSize.
			if !ok || (!colHist.IsHandle && colHist.TotDefCausSize == 0 && (colHist.NullCount != coll.Count)) {
				size += float64(chunk.EstimateTypeWidth(col.GetType()))
				continue
			}
			size += colHist.AvgDefCausSizeListInDisk(coll.Count)
		}
	}
	// Add 8 byte for each column's size record. See `ListInDisk` for details.
	return size + float64(8*len(defcaus))
}

// GetTableAvgRowSize computes average event size for a causet scan, exclude the index key-value pairs.
func (coll *HistDefCausl) GetTableAvgRowSize(ctx stochastikctx.Context, defcaus []*memex.DeferredCauset, storeType ekv.StoreType, handleInDefCauss bool) (size float64) {
	size = coll.GetAvgRowSize(ctx, defcaus, false, true)
	switch storeType {
	case ekv.EinsteinDB:
		size += blockcodec.RecordRowKeyLen
		// The `defcaus` for EinsteinDB always contain the row_id, so prefix event size subtract its length.
		size -= 8
	case ekv.TiFlash:
		if !handleInDefCauss {
			size += 8 /* row_id length */
		}
	}
	return
}

// GetIndexAvgRowSize computes average event size for a index scan.
func (coll *HistDefCausl) GetIndexAvgRowSize(ctx stochastikctx.Context, defcaus []*memex.DeferredCauset, isUnique bool) (size float64) {
	size = coll.GetAvgRowSize(ctx, defcaus, true, true)
	// blockPrefix(1) + blockID(8) + indexPrefix(2) + indexID(8)
	// Because the defcaus for index scan always contain the handle, so we don't add the rowID here.
	size += 19
	if !isUnique {
		// add the len("_")
		size++
	}
	return
}
