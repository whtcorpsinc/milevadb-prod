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
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/collate"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

// Histogram represents statistics for a column or index.
type Histogram struct {
	ID        int64 // DeferredCauset ID.
	NDV       int64 // Number of distinct values.
	NullCount int64 // Number of null values.
	// LastUFIDelateVersion is the version that this histogram uFIDelated last time.
	LastUFIDelateVersion uint64

	Tp *types.FieldType

	// Histogram elements.
	//
	// A bucket bound is the smallest and greatest values stored in the bucket. The lower and upper bound
	// are stored in one column.
	//
	// A bucket count is the number of items stored in all previous buckets and the current bucket.
	// Bucket counts are always in increasing order.
	//
	// A bucket repeat is the number of repeats of the bucket value, it can be used to find popular values.
	Bounds  *chunk.Chunk
	Buckets []Bucket

	// Used for estimating fraction of the interval [lower, upper] that lies within the [lower, value].
	// For some types like `Int`, we do not build it because we can get them directly from `Bounds`.
	scalars []scalar
	// TotDefCausSize is the total column size for the histogram.
	// For unfixed-len types, it includes LEN and BYTE.
	TotDefCausSize int64

	// Correlation is the statistical correlation between physical event ordering and logical ordering of
	// the column values. This ranges from -1 to +1, and it is only valid for DeferredCauset histogram, not for
	// Index histogram.
	Correlation float64
}

// Bucket causetstore the bucket count and repeat.
type Bucket struct {
	Count  int64
	Repeat int64
}

type scalar struct {
	lower        float64
	upper        float64
	commonPfxLen int // commonPfxLen is the common prefix length of the lower bound and upper bound when the value type is HoTTString or HoTTBytes.
}

// NewHistogram creates a new histogram.
func NewHistogram(id, ndv, nullCount int64, version uint64, tp *types.FieldType, bucketSize int, totDefCausSize int64) *Histogram {
	return &Histogram{
		ID:                id,
		NDV:               ndv,
		NullCount:         nullCount,
		LastUFIDelateVersion: version,
		Tp:                tp,
		Bounds:            chunk.NewChunkWithCapacity([]*types.FieldType{tp}, 2*bucketSize),
		Buckets:           make([]Bucket, 0, bucketSize),
		TotDefCausSize:        totDefCausSize,
	}
}

// GetLower gets the lower bound of bucket `idx`.
func (hg *Histogram) GetLower(idx int) *types.Causet {
	d := hg.Bounds.GetRow(2*idx).GetCauset(0, hg.Tp)
	return &d
}

// GetUpper gets the upper bound of bucket `idx`.
func (hg *Histogram) GetUpper(idx int) *types.Causet {
	d := hg.Bounds.GetRow(2*idx+1).GetCauset(0, hg.Tp)
	return &d
}

// MemoryUsage returns the total memory usage of this Histogram.
// everytime changed the Histogram of the block, it will cost O(n)
// complexity so calculate the memoryUsage might cost little time.
// We ignore the size of other metadata in Histogram.
func (hg *Histogram) MemoryUsage() (sum int64) {
	if hg == nil {
		return
	}
	sum = hg.Bounds.MemoryUsage() + int64(cap(hg.Buckets)*int(unsafe.Sizeof(Bucket{}))) + int64(cap(hg.scalars)*int(unsafe.Sizeof(scalar{})))
	return
}

// AvgDefCausSize is the average column size of the histogram. These sizes are derived from function `encode`
// and `Causet::ConvertTo`, so we need to uFIDelate them if those 2 functions are changed.
func (c *DeferredCauset) AvgDefCausSize(count int64, isKey bool) float64 {
	if count == 0 {
		return 0
	}
	// Note that, if the handle column is encoded as value, instead of key, i.e,
	// when the handle column is in a unique index, the real column size may be
	// smaller than 8 because it is encoded using `EncodeVarint`. Since we don't
	// know the exact value size now, use 8 as approximation.
	if c.IsHandle {
		return 8
	}
	histCount := c.TotalRowCount()
	notNullRatio := 1.0
	if histCount > 0 {
		notNullRatio = 1.0 - float64(c.NullCount)/histCount
	}
	switch c.Histogram.Tp.Tp {
	case allegrosql.TypeFloat, allegrosql.TypeDouble, allegrosql.TypeDuration, allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		return 8 * notNullRatio
	case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong, allegrosql.TypeLonglong, allegrosql.TypeYear, allegrosql.TypeEnum, allegrosql.TypeBit, allegrosql.TypeSet:
		if isKey {
			return 8 * notNullRatio
		}
	}
	// Keep two decimal place.
	return math.Round(float64(c.TotDefCausSize)/float64(count)*100) / 100
}

// AvgDefCausSizeChunkFormat is the average column size of the histogram. These sizes are derived from function `Encode`
// and `DecodeToChunk`, so we need to uFIDelate them if those 2 functions are changed.
func (c *DeferredCauset) AvgDefCausSizeChunkFormat(count int64) float64 {
	if count == 0 {
		return 0
	}
	fixedLen := chunk.GetFixedLen(c.Histogram.Tp)
	if fixedLen != -1 {
		return float64(fixedLen)
	}
	// Keep two decimal place.
	// Add 8 bytes for unfixed-len type's offsets.
	// Minus Log2(avgSize) for unfixed-len type LEN.
	avgSize := float64(c.TotDefCausSize) / float64(count)
	if avgSize < 1 {
		return math.Round(avgSize*100)/100 + 8
	}
	return math.Round((avgSize-math.Log2(avgSize))*100)/100 + 8
}

// AvgDefCausSizeListInDisk is the average column size of the histogram. These sizes are derived
// from `chunk.ListInDisk` so we need to uFIDelate them if those 2 functions are changed.
func (c *DeferredCauset) AvgDefCausSizeListInDisk(count int64) float64 {
	if count == 0 {
		return 0
	}
	histCount := c.TotalRowCount()
	notNullRatio := 1.0
	if histCount > 0 {
		notNullRatio = 1.0 - float64(c.NullCount)/histCount
	}
	size := chunk.GetFixedLen(c.Histogram.Tp)
	if size != -1 {
		return float64(size) * notNullRatio
	}
	// Keep two decimal place.
	// Minus Log2(avgSize) for unfixed-len type LEN.
	avgSize := float64(c.TotDefCausSize) / float64(count)
	if avgSize < 1 {
		return math.Round((avgSize)*100) / 100
	}
	return math.Round((avgSize-math.Log2(avgSize))*100) / 100
}

// AppendBucket appends a bucket into `hg`.
func (hg *Histogram) AppendBucket(lower *types.Causet, upper *types.Causet, count, repeat int64) {
	hg.Buckets = append(hg.Buckets, Bucket{Count: count, Repeat: repeat})
	hg.Bounds.AppendCauset(0, lower)
	hg.Bounds.AppendCauset(0, upper)
}

func (hg *Histogram) uFIDelateLastBucket(upper *types.Causet, count, repeat int64) {
	len := hg.Len()
	hg.Bounds.TruncateTo(2*len - 1)
	hg.Bounds.AppendCauset(0, upper)
	hg.Buckets[len-1] = Bucket{Count: count, Repeat: repeat}
}

// DecodeTo decodes the histogram bucket values into `Tp`.
func (hg *Histogram) DecodeTo(tp *types.FieldType, timeZone *time.Location) error {
	oldIter := chunk.NewIterator4Chunk(hg.Bounds)
	hg.Bounds = chunk.NewChunkWithCapacity([]*types.FieldType{tp}, oldIter.Len())
	hg.Tp = tp
	for event := oldIter.Begin(); event != oldIter.End(); event = oldIter.Next() {
		causet, err := blockcodec.DecodeDeferredCausetValue(event.GetBytes(0), tp, timeZone)
		if err != nil {
			return errors.Trace(err)
		}
		hg.Bounds.AppendCauset(0, &causet)
	}
	return nil
}

// ConvertTo converts the histogram bucket values into `Tp`.
func (hg *Histogram) ConvertTo(sc *stmtctx.StatementContext, tp *types.FieldType) (*Histogram, error) {
	hist := NewHistogram(hg.ID, hg.NDV, hg.NullCount, hg.LastUFIDelateVersion, tp, hg.Len(), hg.TotDefCausSize)
	hist.Correlation = hg.Correlation
	iter := chunk.NewIterator4Chunk(hg.Bounds)
	for event := iter.Begin(); event != iter.End(); event = iter.Next() {
		d := event.GetCauset(0, hg.Tp)
		d, err := d.ConvertTo(sc, tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		hist.Bounds.AppendCauset(0, &d)
	}
	hist.Buckets = hg.Buckets
	return hist, nil
}

// Len is the number of buckets in the histogram.
func (hg *Histogram) Len() int {
	return len(hg.Buckets)
}

// HistogramEqual tests if two histograms are equal.
func HistogramEqual(a, b *Histogram, ignoreID bool) bool {
	if ignoreID {
		old := b.ID
		b.ID = a.ID
		defer func() { b.ID = old }()
	}
	return bytes.Equal([]byte(a.ToString(0)), []byte(b.ToString(0)))
}

// constants for stats version. These const can be used for solving compatibility issue.
const (
	CurStatsVersion = Version1
	Version1        = 1
)

// AnalyzeFlag is set when the statistics comes from analyze and has not been modified by feedback.
const AnalyzeFlag = 1

// IsAnalyzed checks whether this flag contains AnalyzeFlag.
func IsAnalyzed(flag int64) bool {
	return (flag & AnalyzeFlag) > 0
}

// ResetAnalyzeFlag resets the AnalyzeFlag because it has been modified by feedback.
func ResetAnalyzeFlag(flag int64) int64 {
	return flag &^ AnalyzeFlag
}

// ValueToString converts a possible encoded value to a formatted string. If the value is encoded, then
// idxDefCauss equals to number of origin values, else idxDefCauss is 0.
func ValueToString(vars *variable.StochastikVars, value *types.Causet, idxDefCauss int, idxDeferredCausetTypes []byte) (string, error) {
	if idxDefCauss == 0 {
		return value.ToString()
	}
	var loc *time.Location
	if vars != nil {
		loc = vars.Location()
	}
	// Ignore the error and treat remaining part that cannot decode successfully as bytes.
	decodedVals, remained, err := codec.DecodeRange(value.GetBytes(), idxDefCauss, idxDeferredCausetTypes, loc)
	// Ignore err explicit to pass errcheck.
	_ = err
	if len(remained) > 0 {
		decodedVals = append(decodedVals, types.NewBytesCauset(remained))
	}
	str, err := types.CausetsToString(decodedVals, true)
	return str, err
}

// BucketToString change the given bucket to string format.
func (hg *Histogram) BucketToString(bktID, idxDefCauss int) string {
	upperVal, err := ValueToString(nil, hg.GetUpper(bktID), idxDefCauss, nil)
	terror.Log(errors.Trace(err))
	lowerVal, err := ValueToString(nil, hg.GetLower(bktID), idxDefCauss, nil)
	terror.Log(errors.Trace(err))
	return fmt.Sprintf("num: %d lower_bound: %s upper_bound: %s repeats: %d", hg.bucketCount(bktID), lowerVal, upperVal, hg.Buckets[bktID].Repeat)
}

// ToString gets the string representation for the histogram.
func (hg *Histogram) ToString(idxDefCauss int) string {
	strs := make([]string, 0, hg.Len()+1)
	if idxDefCauss > 0 {
		strs = append(strs, fmt.Sprintf("index:%d ndv:%d", hg.ID, hg.NDV))
	} else {
		strs = append(strs, fmt.Sprintf("column:%d ndv:%d totDefCausSize:%d", hg.ID, hg.NDV, hg.TotDefCausSize))
	}
	for i := 0; i < hg.Len(); i++ {
		strs = append(strs, hg.BucketToString(i, idxDefCauss))
	}
	return strings.Join(strs, "\n")
}

// equalRowCount estimates the event count where the column equals to value.
func (hg *Histogram) equalRowCount(value types.Causet) float64 {
	index, match := hg.Bounds.LowerBound(0, &value)
	// Since we causetstore the lower and upper bound together, if the index is an odd number, then it points to a upper bound.
	if index%2 == 1 {
		if match {
			return float64(hg.Buckets[index/2].Repeat)
		}
		return hg.notNullCount() / float64(hg.NDV)
	}
	if match {
		cmp := chunk.GetCompareFunc(hg.Tp)
		if cmp(hg.Bounds.GetRow(index), 0, hg.Bounds.GetRow(index+1), 0) == 0 {
			return float64(hg.Buckets[index/2].Repeat)
		}
		return hg.notNullCount() / float64(hg.NDV)
	}
	return 0
}

// greaterRowCount estimates the event count where the column greater than value.
func (hg *Histogram) greaterRowCount(value types.Causet) float64 {
	gtCount := hg.notNullCount() - hg.lessRowCount(value) - hg.equalRowCount(value)
	return math.Max(0, gtCount)
}

// LessRowCountWithBktIdx estimates the event count where the column less than value.
func (hg *Histogram) LessRowCountWithBktIdx(value types.Causet) (float64, int) {
	// All the values are null.
	if hg.Bounds.NumRows() == 0 {
		return 0, 0
	}
	index, match := hg.Bounds.LowerBound(0, &value)
	if index == hg.Bounds.NumRows() {
		return hg.notNullCount(), hg.Len() - 1
	}
	// Since we causetstore the lower and upper bound together, so dividing the index by 2 will get the bucket index.
	bucketIdx := index / 2
	curCount, curRepeat := float64(hg.Buckets[bucketIdx].Count), float64(hg.Buckets[bucketIdx].Repeat)
	preCount := float64(0)
	if bucketIdx > 0 {
		preCount = float64(hg.Buckets[bucketIdx-1].Count)
	}
	if index%2 == 1 {
		if match {
			return curCount - curRepeat, bucketIdx
		}
		return preCount + hg.calcFraction(bucketIdx, &value)*(curCount-curRepeat-preCount), bucketIdx
	}
	return preCount, bucketIdx
}

func (hg *Histogram) lessRowCount(value types.Causet) float64 {
	result, _ := hg.LessRowCountWithBktIdx(value)
	return result
}

// BetweenRowCount estimates the event count where column greater or equal to a and less than b.
func (hg *Histogram) BetweenRowCount(a, b types.Causet) float64 {
	lessCountA := hg.lessRowCount(a)
	lessCountB := hg.lessRowCount(b)
	// If lessCountA is not less than lessCountB, it may be that they fall to the same bucket and we cannot estimate
	// the fraction, so we use `totalCount / NDV` to estimate the event count, but the result should not greater than
	// lessCountB or notNullCount-lessCountA.
	if lessCountA >= lessCountB && hg.NDV > 0 {
		result := math.Min(lessCountB, hg.notNullCount()-lessCountA)
		return math.Min(result, hg.notNullCount()/float64(hg.NDV))
	}
	return lessCountB - lessCountA
}

// TotalRowCount returns the total count of this histogram.
func (hg *Histogram) TotalRowCount() float64 {
	return hg.notNullCount() + float64(hg.NullCount)
}

// notNullCount indicates the count of non-null values in column histogram and single-column index histogram,
// for multi-column index histogram, since we cannot define null for the event, we treat all rows as non-null, that means,
// notNullCount would return same value as TotalRowCount for multi-column index histograms.
func (hg *Histogram) notNullCount() float64 {
	if hg.Len() == 0 {
		return 0
	}
	return float64(hg.Buckets[hg.Len()-1].Count)
}

// mergeBuckets is used to Merge every two neighbor buckets.
func (hg *Histogram) mergeBuckets(bucketIdx int) {
	curBuck := 0
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.Tp}, bucketIdx)
	for i := 0; i+1 <= bucketIdx; i += 2 {
		hg.Buckets[curBuck] = hg.Buckets[i+1]
		c.AppendCauset(0, hg.GetLower(i))
		c.AppendCauset(0, hg.GetUpper(i+1))
		curBuck++
	}
	if bucketIdx%2 == 0 {
		hg.Buckets[curBuck] = hg.Buckets[bucketIdx]
		c.AppendCauset(0, hg.GetLower(bucketIdx))
		c.AppendCauset(0, hg.GetUpper(bucketIdx))
		curBuck++
	}
	hg.Bounds = c
	hg.Buckets = hg.Buckets[:curBuck]
}

// GetIncreaseFactor will return a factor of data increasing after the last analysis.
func (hg *Histogram) GetIncreaseFactor(totalCount int64) float64 {
	columnCount := hg.TotalRowCount()
	if columnCount == 0 {
		// avoid dividing by 0
		return 1.0
	}
	return float64(totalCount) / columnCount
}

// validRange checks if the range is Valid, it is used by `SplitRange` to remove the invalid range,
// the possible types of range are index key range and handle key range.
func validRange(sc *stmtctx.StatementContext, ran *ranger.Range, encoded bool) bool {
	var low, high []byte
	if encoded {
		low, high = ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
	} else {
		var err error
		low, err = codec.EncodeKey(sc, nil, ran.LowVal[0])
		if err != nil {
			return false
		}
		high, err = codec.EncodeKey(sc, nil, ran.HighVal[0])
		if err != nil {
			return false
		}
	}
	if ran.LowExclude {
		low = ekv.Key(low).PrefixNext()
	}
	if !ran.HighExclude {
		high = ekv.Key(high).PrefixNext()
	}
	return bytes.Compare(low, high) < 0
}

func checkHoTT(vals []types.Causet, HoTT byte) bool {
	if HoTT == types.HoTTString {
		HoTT = types.HoTTBytes
	}
	for _, val := range vals {
		valHoTT := val.HoTT()
		if valHoTT == types.HoTTNull || valHoTT == types.HoTTMinNotNull || valHoTT == types.HoTTMaxValue {
			continue
		}
		if valHoTT == types.HoTTString {
			valHoTT = types.HoTTBytes
		}
		if valHoTT != HoTT {
			return false
		}
		// Only check the first non-null value.
		break
	}
	return true
}

func (hg *Histogram) typeMatch(ranges []*ranger.Range) bool {
	HoTT := hg.GetLower(0).HoTT()
	for _, ran := range ranges {
		if !checkHoTT(ran.LowVal, HoTT) || !checkHoTT(ran.HighVal, HoTT) {
			return false
		}
	}
	return true
}

// SplitRange splits the range according to the histogram lower bound. Note that we treat first bucket's lower bound
// as -inf and last bucket's upper bound as +inf, so all the split ranges will totally fall in one of the (-inf, l(1)),
// [l(1), l(2)),...[l(n-2), l(n-1)), [l(n-1), +inf), where n is the number of buckets, l(i) is the i-th bucket's lower bound.
func (hg *Histogram) SplitRange(sc *stmtctx.StatementContext, oldRanges []*ranger.Range, encoded bool) ([]*ranger.Range, bool) {
	if !hg.typeMatch(oldRanges) {
		return oldRanges, false
	}
	// Treat the only buckets as (-inf, +inf), so we do not need split it.
	if hg.Len() == 1 {
		return oldRanges, true
	}
	ranges := make([]*ranger.Range, 0, len(oldRanges))
	for _, ran := range oldRanges {
		ranges = append(ranges, ran.Clone())
	}
	split := make([]*ranger.Range, 0, len(ranges))
	for len(ranges) > 0 {
		// Find the first bound that greater than the LowVal.
		idx := hg.Bounds.UpperBound(0, &ranges[0].LowVal[0])
		// Treat last bucket's upper bound as +inf, so we do not need split any more.
		if idx >= hg.Bounds.NumRows()-1 {
			split = append(split, ranges...)
			break
		}
		// Treat first buckets's lower bound as -inf, just increase it to the next lower bound.
		if idx == 0 {
			idx = 2
		}
		// Get the next lower bound.
		if idx%2 == 1 {
			idx++
		}
		lowerBound := hg.Bounds.GetRow(idx)
		var i int
		// Find the first range that need to be split by the lower bound.
		for ; i < len(ranges); i++ {
			if chunk.Compare(lowerBound, 0, &ranges[i].HighVal[0]) <= 0 {
				break
			}
		}
		split = append(split, ranges[:i]...)
		ranges = ranges[i:]
		if len(ranges) == 0 {
			break
		}
		// Split according to the lower bound.
		cmp := chunk.Compare(lowerBound, 0, &ranges[0].LowVal[0])
		if cmp > 0 {
			lower := lowerBound.GetCauset(0, hg.Tp)
			newRange := &ranger.Range{
				LowExclude:  ranges[0].LowExclude,
				LowVal:      []types.Causet{ranges[0].LowVal[0]},
				HighVal:     []types.Causet{lower},
				HighExclude: true}
			if validRange(sc, newRange, encoded) {
				split = append(split, newRange)
			}
			ranges[0].LowVal[0] = lower
			ranges[0].LowExclude = false
			if !validRange(sc, ranges[0], encoded) {
				ranges = ranges[1:]
			}
		}
	}
	return split, true
}

func (hg *Histogram) bucketCount(idx int) int64 {
	if idx == 0 {
		return hg.Buckets[0].Count
	}
	return hg.Buckets[idx].Count - hg.Buckets[idx-1].Count
}

// HistogramToProto converts Histogram to its protobuf representation.
// Note that when this is used, the lower/upper bound in the bucket must be BytesCauset.
func HistogramToProto(hg *Histogram) *fidelpb.Histogram {
	protoHg := &fidelpb.Histogram{
		Ndv: hg.NDV,
	}
	for i := 0; i < hg.Len(); i++ {
		bkt := &fidelpb.Bucket{
			Count:      hg.Buckets[i].Count,
			LowerBound: hg.GetLower(i).GetBytes(),
			UpperBound: hg.GetUpper(i).GetBytes(),
			Repeats:    hg.Buckets[i].Repeat,
		}
		protoHg.Buckets = append(protoHg.Buckets, bkt)
	}
	return protoHg
}

// HistogramFromProto converts Histogram from its protobuf representation.
// Note that we will set BytesCauset for the lower/upper bound in the bucket, the decode will
// be after all histograms merged.
func HistogramFromProto(protoHg *fidelpb.Histogram) *Histogram {
	tp := types.NewFieldType(allegrosql.TypeBlob)
	hg := NewHistogram(0, protoHg.Ndv, 0, 0, tp, len(protoHg.Buckets), 0)
	for _, bucket := range protoHg.Buckets {
		lower, upper := types.NewBytesCauset(bucket.LowerBound), types.NewBytesCauset(bucket.UpperBound)
		hg.AppendBucket(&lower, &upper, bucket.Count, bucket.Repeats)
	}
	return hg
}

func (hg *Histogram) popFirstBucket() {
	hg.Buckets = hg.Buckets[1:]
	c := chunk.NewChunkWithCapacity([]*types.FieldType{hg.Tp, hg.Tp}, hg.Bounds.NumRows()-2)
	c.Append(hg.Bounds, 2, hg.Bounds.NumRows())
	hg.Bounds = c
}

// IsIndexHist checks whether current histogram is one for index.
func (hg *Histogram) IsIndexHist() bool {
	return hg.Tp.Tp == allegrosql.TypeBlob
}

// MergeHistograms merges two histograms.
func MergeHistograms(sc *stmtctx.StatementContext, lh *Histogram, rh *Histogram, bucketSize int) (*Histogram, error) {
	if lh.Len() == 0 {
		return rh, nil
	}
	if rh.Len() == 0 {
		return lh, nil
	}
	lh.NDV += rh.NDV
	lLen := lh.Len()
	cmp, err := lh.GetUpper(lLen-1).CompareCauset(sc, rh.GetLower(0))
	if err != nil {
		return nil, errors.Trace(err)
	}
	offset := int64(0)
	if cmp == 0 {
		lh.NDV--
		lh.uFIDelateLastBucket(rh.GetUpper(0), lh.Buckets[lLen-1].Count+rh.Buckets[0].Count, rh.Buckets[0].Repeat)
		offset = rh.Buckets[0].Count
		rh.popFirstBucket()
	}
	for lh.Len() > bucketSize {
		lh.mergeBuckets(lh.Len() - 1)
	}
	if rh.Len() == 0 {
		return lh, nil
	}
	for rh.Len() > bucketSize {
		rh.mergeBuckets(rh.Len() - 1)
	}
	lCount := lh.Buckets[lh.Len()-1].Count
	rCount := rh.Buckets[rh.Len()-1].Count - offset
	lAvg := float64(lCount) / float64(lh.Len())
	rAvg := float64(rCount) / float64(rh.Len())
	for lh.Len() > 1 && lAvg*2 <= rAvg {
		lh.mergeBuckets(lh.Len() - 1)
		lAvg *= 2
	}
	for rh.Len() > 1 && rAvg*2 <= lAvg {
		rh.mergeBuckets(rh.Len() - 1)
		rAvg *= 2
	}
	for i := 0; i < rh.Len(); i++ {
		lh.AppendBucket(rh.GetLower(i), rh.GetUpper(i), rh.Buckets[i].Count+lCount-offset, rh.Buckets[i].Repeat)
	}
	for lh.Len() > bucketSize {
		lh.mergeBuckets(lh.Len() - 1)
	}
	return lh, nil
}

// AvgCountPerNotNullValue gets the average event count per value by the data of histogram.
func (hg *Histogram) AvgCountPerNotNullValue(totalCount int64) float64 {
	factor := hg.GetIncreaseFactor(totalCount)
	totalNotNull := hg.notNullCount() * factor
	curNDV := float64(hg.NDV) * factor
	curNDV = math.Max(curNDV, 1)
	return totalNotNull / curNDV
}

func (hg *Histogram) outOfRange(val types.Causet) bool {
	if hg.Len() == 0 {
		return true
	}
	return chunk.Compare(hg.Bounds.GetRow(0), 0, &val) > 0 ||
		chunk.Compare(hg.Bounds.GetRow(hg.Bounds.NumRows()-1), 0, &val) < 0
}

// Copy deep copies the histogram.
func (hg *Histogram) Copy() *Histogram {
	newHist := *hg
	newHist.Bounds = hg.Bounds.CopyConstruct()
	newHist.Buckets = make([]Bucket, 0, len(hg.Buckets))
	newHist.Buckets = append(newHist.Buckets, hg.Buckets...)
	return &newHist
}

// RemoveUpperBound removes the upper bound from histogram.
// It is used when merge stats for incremental analyze.
func (hg *Histogram) RemoveUpperBound() *Histogram {
	hg.Buckets[hg.Len()-1].Count -= hg.Buckets[hg.Len()-1].Repeat
	hg.Buckets[hg.Len()-1].Repeat = 0
	return hg
}

// TruncateHistogram truncates the histogram to `numBkt` buckets.
func (hg *Histogram) TruncateHistogram(numBkt int) *Histogram {
	hist := hg.Copy()
	hist.Buckets = hist.Buckets[:numBkt]
	hist.Bounds.TruncateTo(numBkt * 2)
	return hist
}

// ErrorRate is the error rate of estimate event count by bucket and cm sketch.
type ErrorRate struct {
	ErrorTotal float64
	QueryTotal int64
}

// MaxErrorRate is the max error rate of estimate event count of a not pseudo column.
// If the block is pseudo, but the average error rate is less than MaxErrorRate,
// then the column is not pseudo.
const MaxErrorRate = 0.25

// NotAccurate is true when the total of query is zero or the average error
// rate is greater than MaxErrorRate.
func (e *ErrorRate) NotAccurate() bool {
	if e.QueryTotal == 0 {
		return true
	}
	return e.ErrorTotal/float64(e.QueryTotal) > MaxErrorRate
}

// UFIDelate uFIDelates the ErrorRate.
func (e *ErrorRate) UFIDelate(rate float64) {
	e.QueryTotal++
	e.ErrorTotal += rate
}

// Merge range merges two ErrorRate.
func (e *ErrorRate) Merge(rate *ErrorRate) {
	e.QueryTotal += rate.QueryTotal
	e.ErrorTotal += rate.ErrorTotal
}

// DeferredCauset represents a column histogram.
type DeferredCauset struct {
	Histogram
	*CMSketch
	PhysicalID int64
	Count      int64
	Info       *perceptron.DeferredCausetInfo
	IsHandle   bool
	ErrorRate
	Flag           int64
	LastAnalyzePos types.Causet
}

func (c *DeferredCauset) String() string {
	return c.Histogram.ToString(0)
}

// MemoryUsage returns the total memory usage of Histogram and CMSketch in DeferredCauset.
// We ignore the size of other metadata in DeferredCauset
func (c *DeferredCauset) MemoryUsage() (sum int64) {
	sum = c.Histogram.MemoryUsage()
	if c.CMSketch != nil {
		sum += c.CMSketch.MemoryUsage()
	}
	return
}

// HistogramNeededDeferredCausets stores the columns whose Histograms need to be loaded from physical ekv layer.
// Currently, we only load index/pk's Histogram from ekv automatically. DeferredCausets' are loaded by needs.
var HistogramNeededDeferredCausets = neededDeferredCausetMap{defcaus: map[blockDeferredCausetID]struct{}{}}

// IsInvalid checks if this column is invalid. If this column has histogram but not loaded yet, then we mark it
// as need histogram.
func (c *DeferredCauset) IsInvalid(sc *stmtctx.StatementContext, collPseudo bool) bool {
	if collPseudo && c.NotAccurate() {
		return true
	}
	if c.NDV > 0 && c.Len() == 0 && sc != nil {
		sc.SetHistogramsNotLoad()
		HistogramNeededDeferredCausets.insert(blockDeferredCausetID{TableID: c.PhysicalID, DeferredCausetID: c.Info.ID})
	}
	return c.TotalRowCount() == 0 || (c.NDV > 0 && c.Len() == 0)
}

func (c *DeferredCauset) equalRowCount(sc *stmtctx.StatementContext, val types.Causet, modifyCount int64) (float64, error) {
	if val.IsNull() {
		return float64(c.NullCount), nil
	}
	// All the values are null.
	if c.Histogram.Bounds.NumRows() == 0 {
		return 0.0, nil
	}
	if c.NDV > 0 && c.outOfRange(val) {
		return outOfRangeEQSelectivity(c.NDV, modifyCount, int64(c.TotalRowCount())) * c.TotalRowCount(), nil
	}
	if c.CMSketch != nil {
		count, err := c.CMSketch.queryValue(sc, val)
		return float64(count), errors.Trace(err)
	}
	return c.Histogram.equalRowCount(val), nil
}

// GetDeferredCausetRowCount estimates the event count by a slice of Range.
func (c *DeferredCauset) GetDeferredCausetRowCount(sc *stmtctx.StatementContext, ranges []*ranger.Range, modifyCount int64, pkIsHandle bool) (float64, error) {
	var rowCount float64
	for _, rg := range ranges {
		highVal := *rg.HighVal[0].Clone()
		lowVal := *rg.LowVal[0].Clone()
		if highVal.HoTT() == types.HoTTString {
			highVal.SetBytesAsString(collate.GetDefCauslator(
				highVal.DefCauslation()).Key(highVal.GetString()),
				highVal.DefCauslation(),
				uint32(highVal.Length()),
			)
		}
		if lowVal.HoTT() == types.HoTTString {
			lowVal.SetBytesAsString(collate.GetDefCauslator(
				lowVal.DefCauslation()).Key(lowVal.GetString()),
				lowVal.DefCauslation(),
				uint32(lowVal.Length()),
			)
		}
		cmp, err := lowVal.CompareCauset(sc, &highVal)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp == 0 {
			// the point case.
			if !rg.LowExclude && !rg.HighExclude {
				// In this case, the event count is at most 1.
				if pkIsHandle {
					rowCount += 1
					continue
				}
				var cnt float64
				cnt, err = c.equalRowCount(sc, lowVal, modifyCount)
				if err != nil {
					return 0, errors.Trace(err)
				}
				rowCount += cnt
			}
			continue
		}
		rangeVals := enumRangeValues(lowVal, highVal, rg.LowExclude, rg.HighExclude)
		// The small range case.
		if rangeVals != nil {
			for _, val := range rangeVals {
				cnt, err := c.equalRowCount(sc, val, modifyCount)
				if err != nil {
					return 0, err
				}
				rowCount += cnt
			}
			continue
		}
		// The interval case.
		cnt := c.BetweenRowCount(lowVal, highVal)
		if (c.outOfRange(lowVal) && !lowVal.IsNull()) || c.outOfRange(highVal) {
			cnt += outOfRangeEQSelectivity(outOfRangeBetweenRate, modifyCount, int64(c.TotalRowCount())) * c.TotalRowCount()
		}
		// `betweenRowCount` returns count for [l, h) range, we adjust cnt for boudaries here.
		// Note that, `cnt` does not include null values, we need specially handle cases
		// where null is the lower bound.
		if rg.LowExclude && !lowVal.IsNull() {
			lowCnt, err := c.equalRowCount(sc, lowVal, modifyCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			cnt -= lowCnt
		}
		if !rg.LowExclude && lowVal.IsNull() {
			cnt += float64(c.NullCount)
		}
		if !rg.HighExclude {
			highCnt, err := c.equalRowCount(sc, highVal, modifyCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			cnt += highCnt
		}
		rowCount += cnt
	}
	if rowCount > c.TotalRowCount() {
		rowCount = c.TotalRowCount()
	} else if rowCount < 0 {
		rowCount = 0
	}
	return rowCount, nil
}

// Index represents an index histogram.
type Index struct {
	Histogram
	*CMSketch
	ErrorRate
	StatsVer       int64 // StatsVer is the version of the current stats, used to maintain compatibility
	Info           *perceptron.IndexInfo
	Flag           int64
	LastAnalyzePos types.Causet
}

func (idx *Index) String() string {
	return idx.Histogram.ToString(len(idx.Info.DeferredCausets))
}

// IsInvalid checks if this index is invalid.
func (idx *Index) IsInvalid(collPseudo bool) bool {
	return (collPseudo && idx.NotAccurate()) || idx.TotalRowCount() == 0
}

// MemoryUsage returns the total memory usage of a Histogram and CMSketch in Index.
// We ignore the size of other metadata in Index.
func (idx *Index) MemoryUsage() (sum int64) {
	sum = idx.Histogram.MemoryUsage()
	if idx.CMSketch != nil {
		sum += idx.CMSketch.MemoryUsage()
	}
	return
}

var nullKeyBytes, _ = codec.EncodeKey(nil, nil, types.NewCauset(nil))

func (idx *Index) equalRowCount(sc *stmtctx.StatementContext, b []byte, modifyCount int64) (float64, error) {
	if len(idx.Info.DeferredCausets) == 1 {
		if bytes.Equal(b, nullKeyBytes) {
			return float64(idx.NullCount), nil
		}
	}
	val := types.NewBytesCauset(b)
	if idx.NDV > 0 && idx.outOfRange(val) {
		return outOfRangeEQSelectivity(idx.NDV, modifyCount, int64(idx.TotalRowCount())) * idx.TotalRowCount(), nil
	}
	if idx.CMSketch != nil {
		return float64(idx.CMSketch.QueryBytes(b)), nil
	}
	return idx.Histogram.equalRowCount(val), nil
}

// GetRowCount returns the event count of the given ranges.
// It uses the modifyCount to adjust the influence of modifications on the block.
func (idx *Index) GetRowCount(sc *stmtctx.StatementContext, indexRanges []*ranger.Range, modifyCount int64) (float64, error) {
	totalCount := float64(0)
	isSingleDefCaus := len(idx.Info.DeferredCausets) == 1
	for _, indexRange := range indexRanges {
		lb, err := codec.EncodeKey(sc, nil, indexRange.LowVal...)
		if err != nil {
			return 0, err
		}
		rb, err := codec.EncodeKey(sc, nil, indexRange.HighVal...)
		if err != nil {
			return 0, err
		}
		fullLen := len(indexRange.LowVal) == len(indexRange.HighVal) && len(indexRange.LowVal) == len(idx.Info.DeferredCausets)
		if bytes.Equal(lb, rb) {
			if indexRange.LowExclude || indexRange.HighExclude {
				continue
			}
			if fullLen {
				// At most 1 in this case.
				if idx.Info.Unique {
					totalCount += 1
					continue
				}
				count, err := idx.equalRowCount(sc, lb, modifyCount)
				if err != nil {
					return 0, err
				}
				totalCount += count
				continue
			}
		}
		if indexRange.LowExclude {
			lb = ekv.Key(lb).PrefixNext()
		}
		if !indexRange.HighExclude {
			rb = ekv.Key(rb).PrefixNext()
		}
		l := types.NewBytesCauset(lb)
		r := types.NewBytesCauset(rb)
		totalCount += idx.BetweenRowCount(l, r)
		lowIsNull := bytes.Equal(lb, nullKeyBytes)
		if (idx.outOfRange(l) && !(isSingleDefCaus && lowIsNull)) || idx.outOfRange(r) {
			totalCount += outOfRangeEQSelectivity(outOfRangeBetweenRate, modifyCount, int64(idx.TotalRowCount())) * idx.TotalRowCount()
		}
		if isSingleDefCaus && lowIsNull {
			totalCount += float64(idx.NullCount)
		}
	}
	if totalCount > idx.TotalRowCount() {
		totalCount = idx.TotalRowCount()
	}
	return totalCount, nil
}

type countByRangeFunc = func(*stmtctx.StatementContext, int64, []*ranger.Range) (float64, error)

// newHistogramBySelectivity fulfills the content of new histogram by the given selectivity result.
// TODO: Causet is not efficient, try to avoid using it here.
//  Also, there're redundant calculation with Selectivity(). We need to reduce it too.
func newHistogramBySelectivity(sc *stmtctx.StatementContext, histID int64, oldHist, newHist *Histogram, ranges []*ranger.Range, cntByRangeFunc countByRangeFunc) error {
	cntPerVal := int64(oldHist.AvgCountPerNotNullValue(int64(oldHist.TotalRowCount())))
	var totCnt int64
	for boundIdx, ranIdx, highRangeIdx := 0, 0, 0; boundIdx < oldHist.Bounds.NumRows() && ranIdx < len(ranges); boundIdx, ranIdx = boundIdx+2, highRangeIdx {
		for highRangeIdx < len(ranges) && chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx].HighVal[0]) >= 0 {
			highRangeIdx++
		}
		if boundIdx+2 >= oldHist.Bounds.NumRows() && highRangeIdx < len(ranges) && ranges[highRangeIdx].HighVal[0].HoTT() == types.HoTTMaxValue {
			highRangeIdx++
		}
		if ranIdx == highRangeIdx {
			continue
		}
		cnt, err := cntByRangeFunc(sc, histID, ranges[ranIdx:highRangeIdx])
		// This should not happen.
		if err != nil {
			return err
		}
		if cnt == 0 {
			continue
		}
		if int64(cnt) > oldHist.bucketCount(boundIdx/2) {
			cnt = float64(oldHist.bucketCount(boundIdx / 2))
		}
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx))
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx + 1))
		totCnt += int64(cnt)
		bkt := Bucket{Count: totCnt}
		if chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx-1].HighVal[0]) == 0 && !ranges[highRangeIdx-1].HighExclude {
			bkt.Repeat = cntPerVal
		}
		newHist.Buckets = append(newHist.Buckets, bkt)
		switch newHist.Tp.EvalType() {
		case types.ETString, types.ETDecimal, types.ETDatetime, types.ETTimestamp:
			newHist.scalars = append(newHist.scalars, oldHist.scalars[boundIdx/2])
		}
	}
	return nil
}

func (idx *Index) newIndexBySelectivity(sc *stmtctx.StatementContext, statsNode *StatsNode) (*Index, error) {
	var (
		ranLowEncode, ranHighEncode []byte
		err                         error
	)
	newIndexHist := &Index{Info: idx.Info, StatsVer: idx.StatsVer, CMSketch: idx.CMSketch}
	newIndexHist.Histogram = *NewHistogram(idx.ID, int64(float64(idx.NDV)*statsNode.Selectivity), 0, 0, types.NewFieldType(allegrosql.TypeBlob), chunk.InitialCapacity, 0)

	lowBucketIdx, highBucketIdx := 0, 0
	var totCnt int64

	// Bucket bound of index is encoded one, so we need to decode it if we want to calculate the fraction accurately.
	// TODO: enhance its calculation.
	// Now just remove the bucket that no range fell in.
	for _, ran := range statsNode.Ranges {
		lowBucketIdx = highBucketIdx
		ranLowEncode, ranHighEncode, err = ran.Encode(sc, ranLowEncode, ranHighEncode)
		if err != nil {
			return nil, err
		}
		for ; highBucketIdx < idx.Len(); highBucketIdx++ {
			// Encoded value can only go to its next quickly. So ranHighEncode is actually range.HighVal's PrefixNext value.
			// So the Bound should also go to its PrefixNext.
			bucketLowerEncoded := idx.Bounds.GetRow(highBucketIdx * 2).GetBytes(0)
			if bytes.Compare(ranHighEncode, ekv.Key(bucketLowerEncoded).PrefixNext()) < 0 {
				break
			}
		}
		for ; lowBucketIdx < highBucketIdx; lowBucketIdx++ {
			bucketUpperEncoded := idx.Bounds.GetRow(lowBucketIdx*2 + 1).GetBytes(0)
			if bytes.Compare(ranLowEncode, bucketUpperEncoded) <= 0 {
				break
			}
		}
		if lowBucketIdx >= idx.Len() {
			break
		}
		for i := lowBucketIdx; i < highBucketIdx; i++ {
			newIndexHist.Bounds.AppendRow(idx.Bounds.GetRow(i * 2))
			newIndexHist.Bounds.AppendRow(idx.Bounds.GetRow(i*2 + 1))
			totCnt += idx.bucketCount(i)
			newIndexHist.Buckets = append(newIndexHist.Buckets, Bucket{Repeat: idx.Buckets[i].Repeat, Count: totCnt})
			newIndexHist.scalars = append(newIndexHist.scalars, idx.scalars[i])
		}
	}
	return newIndexHist, nil
}

// NewHistDefCauslBySelectivity creates new HistDefCausl by the given statsNodes.
func (coll *HistDefCausl) NewHistDefCauslBySelectivity(sc *stmtctx.StatementContext, statsNodes []*StatsNode) *HistDefCausl {
	newDefCausl := &HistDefCausl{
		DeferredCausets:       make(map[int64]*DeferredCauset),
		Indices:       make(map[int64]*Index),
		Idx2DeferredCausetIDs: coll.Idx2DeferredCausetIDs,
		DefCausID2IdxID:   coll.DefCausID2IdxID,
		Count:         coll.Count,
	}
	for _, node := range statsNodes {
		if node.Tp == IndexType {
			idxHist, ok := coll.Indices[node.ID]
			if !ok {
				continue
			}
			newIdxHist, err := idxHist.newIndexBySelectivity(sc, node)
			if err != nil {
				logutil.BgLogger().Warn("[Histogram-in-plan]: something wrong happened when calculating event count, "+
					"failed to build histogram for index %v of block %v",
					zap.String("index", idxHist.Info.Name.O), zap.String("block", idxHist.Info.Block.O), zap.Error(err))
				continue
			}
			newDefCausl.Indices[node.ID] = newIdxHist
			continue
		}
		oldDefCaus, ok := coll.DeferredCausets[node.ID]
		if !ok {
			continue
		}
		newDefCaus := &DeferredCauset{
			PhysicalID: oldDefCaus.PhysicalID,
			Info:       oldDefCaus.Info,
			IsHandle:   oldDefCaus.IsHandle,
			CMSketch:   oldDefCaus.CMSketch,
		}
		newDefCaus.Histogram = *NewHistogram(oldDefCaus.ID, int64(float64(oldDefCaus.NDV)*node.Selectivity), 0, 0, oldDefCaus.Tp, chunk.InitialCapacity, 0)
		var err error
		splitRanges, ok := oldDefCaus.Histogram.SplitRange(sc, node.Ranges, false)
		if !ok {
			logutil.BgLogger().Warn("[Histogram-in-plan]: the type of histogram and ranges mismatch")
			continue
		}
		// Deal with some corner case.
		if len(splitRanges) > 0 {
			// Deal with NULL values.
			if splitRanges[0].LowVal[0].IsNull() {
				newDefCaus.NullCount = oldDefCaus.NullCount
				if splitRanges[0].HighVal[0].IsNull() {
					splitRanges = splitRanges[1:]
				} else {
					splitRanges[0].LowVal[0].SetMinNotNull()
				}
			}
		}
		if oldDefCaus.IsHandle {
			err = newHistogramBySelectivity(sc, node.ID, &oldDefCaus.Histogram, &newDefCaus.Histogram, splitRanges, coll.GetRowCountByIntDeferredCausetRanges)
		} else {
			err = newHistogramBySelectivity(sc, node.ID, &oldDefCaus.Histogram, &newDefCaus.Histogram, splitRanges, coll.GetRowCountByDeferredCausetRanges)
		}
		if err != nil {
			logutil.BgLogger().Warn("[Histogram-in-plan]: something wrong happened when calculating event count",
				zap.Error(err))
			continue
		}
		newDefCausl.DeferredCausets[node.ID] = newDefCaus
	}
	for id, idx := range coll.Indices {
		_, ok := newDefCausl.Indices[id]
		if !ok {
			newDefCausl.Indices[id] = idx
		}
	}
	for id, col := range coll.DeferredCausets {
		_, ok := newDefCausl.DeferredCausets[id]
		if !ok {
			newDefCausl.DeferredCausets[id] = col
		}
	}
	return newDefCausl
}

func (idx *Index) outOfRange(val types.Causet) bool {
	if idx.Histogram.Len() == 0 {
		return true
	}
	withInLowBoundOrPrefixMatch := chunk.Compare(idx.Bounds.GetRow(0), 0, &val) <= 0 ||
		matchPrefix(idx.Bounds.GetRow(0), 0, &val)
	withInHighBound := chunk.Compare(idx.Bounds.GetRow(idx.Bounds.NumRows()-1), 0, &val) >= 0
	return !withInLowBoundOrPrefixMatch || !withInHighBound
}

// matchPrefix checks whether ad is the prefix of value
func matchPrefix(event chunk.Row, colIdx int, ad *types.Causet) bool {
	switch ad.HoTT() {
	case types.HoTTString, types.HoTTBytes, types.HoTTBinaryLiteral, types.HoTTMysqlBit:
		return strings.HasPrefix(event.GetString(colIdx), ad.GetString())
	}
	return false
}

type dataCnt struct {
	data []byte
	cnt  uint64
}

func getIndexPrefixLens(data []byte, numDefCauss int) (prefixLens []int, err error) {
	prefixLens = make([]int, 0, numDefCauss)
	var colData []byte
	prefixLen := 0
	for len(data) > 0 {
		colData, data, err = codec.CutOne(data)
		if err != nil {
			return nil, err
		}
		prefixLen += len(colData)
		prefixLens = append(prefixLens, prefixLen)
	}
	return prefixLens, nil
}

// ExtractTopN extracts topn from histogram.
func (hg *Histogram) ExtractTopN(cms *CMSketch, numDefCauss int, numTopN uint32) error {
	if hg.Len() == 0 || cms == nil || numTopN == 0 {
		return nil
	}
	dataSet := make(map[string]struct{}, hg.Bounds.NumRows())
	dataCnts := make([]dataCnt, 0, hg.Bounds.NumRows())
	hg.PreCalculateScalar()
	// Set a limit on the frequency of boundary values to avoid extract values with low frequency.
	limit := hg.notNullCount() / float64(hg.Len())
	// Since our histogram are equal depth, they must occurs on the boundaries of buckets.
	for i := 0; i < hg.Bounds.NumRows(); i++ {
		data := hg.Bounds.GetRow(i).GetBytes(0)
		prefixLens, err := getIndexPrefixLens(data, numDefCauss)
		if err != nil {
			return err
		}
		for _, prefixLen := range prefixLens {
			prefixDefCausData := data[:prefixLen]
			_, ok := dataSet[string(prefixDefCausData)]
			if ok {
				continue
			}
			dataSet[string(prefixDefCausData)] = struct{}{}
			res := hg.BetweenRowCount(types.NewBytesCauset(prefixDefCausData), types.NewBytesCauset(ekv.Key(prefixDefCausData).PrefixNext()))
			if res >= limit {
				dataCnts = append(dataCnts, dataCnt{prefixDefCausData, uint64(res)})
			}
		}
	}
	sort.SliceSblock(dataCnts, func(i, j int) bool { return dataCnts[i].cnt >= dataCnts[j].cnt })
	if len(dataCnts) > int(numTopN) {
		dataCnts = dataCnts[:numTopN]
	}
	cms.topN = make(map[uint64][]*TopNMeta, len(dataCnts))
	for _, dataCnt := range dataCnts {
		h1, h2 := murmur3.Sum128(dataCnt.data)
		realCnt := cms.queryHashValue(h1, h2)
		cms.subValue(h1, h2, realCnt)
		cms.topN[h1] = append(cms.topN[h1], &TopNMeta{h2, dataCnt.data, realCnt})
	}
	return nil
}
