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

package stmtsummary

import (
	"container/list"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
)

var _ = Suite(&testStmtSummarySuite{})

type testStmtSummarySuite struct {
	ssMap *stmtSummaryByDigestMap
}

func emptyCausetGenerator() string {
	return ""
}

func fakeCausetDigestGenerator() string {
	return "point_get"
}

func (s *testStmtSummarySuite) SetUpSuite(c *C) {
	s.ssMap = newStmtSummaryByDigestMap()
	s.ssMap.SetEnabled("1", false)
	s.ssMap.SetRefreshInterval("1800", false)
	s.ssMap.SetHistorySize("24", false)
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

// Test stmtSummaryByDigest.AddStatement.
func (s *testStmtSummarySuite) TestAddStatement(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now + 60

	blocks := []stmtctx.BlockEntry{{EDB: "db1", Block: "tb1"}, {EDB: "db2", Block: "tb2"}}
	indexes := []string{"a", "b"}

	// first memex
	stmtInterDircInfo1 := generateAnyInterDircInfo()
	stmtInterDircInfo1.InterDircDetail.CommitDetail.Mu.BackoffTypes = make([]fmt.Stringer, 0)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo1.SchemaName,
		digest:     stmtInterDircInfo1.Digest,
		planDigest: stmtInterDircInfo1.CausetDigest,
	}
	expectedSummaryElement := stmtSummaryByDigestElement{
		beginTime:            now + 60,
		endTime:              now + 1860,
		sampleALLEGROSQL:            stmtInterDircInfo1.OriginalALLEGROSQL,
		sampleCauset:           stmtInterDircInfo1.CausetGenerator(),
		indexNames:           stmtInterDircInfo1.StmtCtx.IndexNames,
		execCount:            1,
		sumLatency:           stmtInterDircInfo1.TotalLatency,
		maxLatency:           stmtInterDircInfo1.TotalLatency,
		minLatency:           stmtInterDircInfo1.TotalLatency,
		sumParseLatency:      stmtInterDircInfo1.ParseLatency,
		maxParseLatency:      stmtInterDircInfo1.ParseLatency,
		sumCompileLatency:    stmtInterDircInfo1.CompileLatency,
		maxCompileLatency:    stmtInterDircInfo1.CompileLatency,
		sumNumCausetTasks:    int64(stmtInterDircInfo1.CausetTasks.NumCausetTasks),
		maxCopProcessTime:    stmtInterDircInfo1.CausetTasks.MaxProcessTime,
		maxCopProcessAddress: stmtInterDircInfo1.CausetTasks.MaxProcessAddress,
		maxCopWaitTime:       stmtInterDircInfo1.CausetTasks.MaxWaitTime,
		maxCopWaitAddress:    stmtInterDircInfo1.CausetTasks.MaxWaitAddress,
		sumProcessTime:       stmtInterDircInfo1.InterDircDetail.ProcessTime,
		maxProcessTime:       stmtInterDircInfo1.InterDircDetail.ProcessTime,
		sumWaitTime:          stmtInterDircInfo1.InterDircDetail.WaitTime,
		maxWaitTime:          stmtInterDircInfo1.InterDircDetail.WaitTime,
		sumBackoffTime:       stmtInterDircInfo1.InterDircDetail.BackoffTime,
		maxBackoffTime:       stmtInterDircInfo1.InterDircDetail.BackoffTime,
		sumTotalKeys:         stmtInterDircInfo1.InterDircDetail.TotalKeys,
		maxTotalKeys:         stmtInterDircInfo1.InterDircDetail.TotalKeys,
		sumProcessedKeys:     stmtInterDircInfo1.InterDircDetail.ProcessedKeys,
		maxProcessedKeys:     stmtInterDircInfo1.InterDircDetail.ProcessedKeys,
		sumGetCommitTsTime:   stmtInterDircInfo1.InterDircDetail.CommitDetail.GetCommitTsTime,
		maxGetCommitTsTime:   stmtInterDircInfo1.InterDircDetail.CommitDetail.GetCommitTsTime,
		sumPrewriteTime:      stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteTime,
		maxPrewriteTime:      stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteTime,
		sumCommitTime:        stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitTime,
		maxCommitTime:        stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitTime,
		sumLocalLatchTime:    stmtInterDircInfo1.InterDircDetail.CommitDetail.LocalLatchTime,
		maxLocalLatchTime:    stmtInterDircInfo1.InterDircDetail.CommitDetail.LocalLatchTime,
		sumCommitBackoffTime: stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitBackoffTime,
		maxCommitBackoffTime: stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitBackoffTime,
		sumResolveLockTime:   stmtInterDircInfo1.InterDircDetail.CommitDetail.ResolveLockTime,
		maxResolveLockTime:   stmtInterDircInfo1.InterDircDetail.CommitDetail.ResolveLockTime,
		sumWriteKeys:         int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteKeys),
		maxWriteKeys:         stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteKeys,
		sumWriteSize:         int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteSize),
		maxWriteSize:         stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteSize,
		sumPrewriteRegionNum: int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteRegionNum),
		maxPrewriteRegionNum: stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteRegionNum,
		sumTxnRetry:          int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.TxnRetry),
		maxTxnRetry:          stmtInterDircInfo1.InterDircDetail.CommitDetail.TxnRetry,
		backoffTypes:         make(map[fmt.Stringer]int),
		sumMem:               stmtInterDircInfo1.MemMax,
		maxMem:               stmtInterDircInfo1.MemMax,
		sumDisk:              stmtInterDircInfo1.DiskMax,
		maxDisk:              stmtInterDircInfo1.DiskMax,
		sumAffectedRows:      stmtInterDircInfo1.StmtCtx.AffectedRows(),
		firstSeen:            stmtInterDircInfo1.StartTime,
		lastSeen:             stmtInterDircInfo1.StartTime,
	}
	history := list.New()
	history.PushBack(&expectedSummaryElement)
	expectedSummary := stmtSummaryByDigest{
		schemaName:    stmtInterDircInfo1.SchemaName,
		stmtType:      stmtInterDircInfo1.StmtCtx.StmtType,
		digest:        stmtInterDircInfo1.Digest,
		normalizedALLEGROSQL: stmtInterDircInfo1.NormalizedALLEGROSQL,
		planDigest:    stmtInterDircInfo1.CausetDigest,
		blockNames:    "db1.tb1,db2.tb2",
		history:       history,
	}
	s.ssMap.AddStatement(stmtInterDircInfo1)
	summary, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

	// Second memex is similar with the first memex, and its values are
	// greater than that of the first memex.
	stmtInterDircInfo2 := &StmtInterDircInfo{
		SchemaName:     "schema_name",
		OriginalALLEGROSQL:    "original_sql2",
		NormalizedALLEGROSQL:  "normalized_sql",
		Digest:         "digest",
		CausetDigest:     "plan_digest",
		CausetGenerator:  emptyCausetGenerator,
		User:           "user2",
		TotalLatency:   20000,
		ParseLatency:   200,
		CompileLatency: 2000,
		CausetTasks: &stmtctx.CausetTasksDetails{
			NumCausetTasks:    20,
			AvgProcessTime:    2000,
			P90ProcessTime:    20000,
			MaxProcessAddress: "200",
			MaxProcessTime:    25000,
			AvgWaitTime:       200,
			P90WaitTime:       2000,
			MaxWaitAddress:    "201",
			MaxWaitTime:       2500,
		},
		InterDircDetail: &execdetails.InterDircDetails{
			CalleeAddress: "202",
			ProcessTime:   1500,
			WaitTime:      150,
			BackoffTime:   180,
			RequestCount:  20,
			TotalKeys:     6000,
			ProcessedKeys: 1500,
			CommitDetail: &execdetails.CommitDetails{
				GetCommitTsTime:   500,
				PrewriteTime:      50000,
				CommitTime:        5000,
				LocalLatchTime:    50,
				CommitBackoffTime: 1000,
				Mu: struct {
					sync.Mutex
					BackoffTypes []fmt.Stringer
				}{
					BackoffTypes: []fmt.Stringer{einsteindb.BoTxnLock},
				},
				ResolveLockTime:   10000,
				WriteKeys:         100000,
				WriteSize:         1000000,
				PrewriteRegionNum: 100,
				TxnRetry:          10,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Blocks:     blocks,
			IndexNames: indexes,
		},
		MemMax:    20000,
		DiskMax:   20000,
		StartTime: time.Date(2020, 1, 1, 10, 10, 20, 10, time.UTC),
		Succeed:   true,
	}
	stmtInterDircInfo2.StmtCtx.AddAffectedRows(200)
	expectedSummaryElement.execCount++
	expectedSummaryElement.sumLatency += stmtInterDircInfo2.TotalLatency
	expectedSummaryElement.maxLatency = stmtInterDircInfo2.TotalLatency
	expectedSummaryElement.sumParseLatency += stmtInterDircInfo2.ParseLatency
	expectedSummaryElement.maxParseLatency = stmtInterDircInfo2.ParseLatency
	expectedSummaryElement.sumCompileLatency += stmtInterDircInfo2.CompileLatency
	expectedSummaryElement.maxCompileLatency = stmtInterDircInfo2.CompileLatency
	expectedSummaryElement.sumNumCausetTasks += int64(stmtInterDircInfo2.CausetTasks.NumCausetTasks)
	expectedSummaryElement.maxCopProcessTime = stmtInterDircInfo2.CausetTasks.MaxProcessTime
	expectedSummaryElement.maxCopProcessAddress = stmtInterDircInfo2.CausetTasks.MaxProcessAddress
	expectedSummaryElement.maxCopWaitTime = stmtInterDircInfo2.CausetTasks.MaxWaitTime
	expectedSummaryElement.maxCopWaitAddress = stmtInterDircInfo2.CausetTasks.MaxWaitAddress
	expectedSummaryElement.sumProcessTime += stmtInterDircInfo2.InterDircDetail.ProcessTime
	expectedSummaryElement.maxProcessTime = stmtInterDircInfo2.InterDircDetail.ProcessTime
	expectedSummaryElement.sumWaitTime += stmtInterDircInfo2.InterDircDetail.WaitTime
	expectedSummaryElement.maxWaitTime = stmtInterDircInfo2.InterDircDetail.WaitTime
	expectedSummaryElement.sumBackoffTime += stmtInterDircInfo2.InterDircDetail.BackoffTime
	expectedSummaryElement.maxBackoffTime = stmtInterDircInfo2.InterDircDetail.BackoffTime
	expectedSummaryElement.sumTotalKeys += stmtInterDircInfo2.InterDircDetail.TotalKeys
	expectedSummaryElement.maxTotalKeys = stmtInterDircInfo2.InterDircDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtInterDircInfo2.InterDircDetail.ProcessedKeys
	expectedSummaryElement.maxProcessedKeys = stmtInterDircInfo2.InterDircDetail.ProcessedKeys
	expectedSummaryElement.sumGetCommitTsTime += stmtInterDircInfo2.InterDircDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.maxGetCommitTsTime = stmtInterDircInfo2.InterDircDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.sumPrewriteTime += stmtInterDircInfo2.InterDircDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.maxPrewriteTime = stmtInterDircInfo2.InterDircDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.sumCommitTime += stmtInterDircInfo2.InterDircDetail.CommitDetail.CommitTime
	expectedSummaryElement.maxCommitTime = stmtInterDircInfo2.InterDircDetail.CommitDetail.CommitTime
	expectedSummaryElement.sumLocalLatchTime += stmtInterDircInfo2.InterDircDetail.CommitDetail.LocalLatchTime
	expectedSummaryElement.maxLocalLatchTime = stmtInterDircInfo2.InterDircDetail.CommitDetail.LocalLatchTime
	expectedSummaryElement.sumCommitBackoffTime += stmtInterDircInfo2.InterDircDetail.CommitDetail.CommitBackoffTime
	expectedSummaryElement.maxCommitBackoffTime = stmtInterDircInfo2.InterDircDetail.CommitDetail.CommitBackoffTime
	expectedSummaryElement.sumResolveLockTime += stmtInterDircInfo2.InterDircDetail.CommitDetail.ResolveLockTime
	expectedSummaryElement.maxResolveLockTime = stmtInterDircInfo2.InterDircDetail.CommitDetail.ResolveLockTime
	expectedSummaryElement.sumWriteKeys += int64(stmtInterDircInfo2.InterDircDetail.CommitDetail.WriteKeys)
	expectedSummaryElement.maxWriteKeys = stmtInterDircInfo2.InterDircDetail.CommitDetail.WriteKeys
	expectedSummaryElement.sumWriteSize += int64(stmtInterDircInfo2.InterDircDetail.CommitDetail.WriteSize)
	expectedSummaryElement.maxWriteSize = stmtInterDircInfo2.InterDircDetail.CommitDetail.WriteSize
	expectedSummaryElement.sumPrewriteRegionNum += int64(stmtInterDircInfo2.InterDircDetail.CommitDetail.PrewriteRegionNum)
	expectedSummaryElement.maxPrewriteRegionNum = stmtInterDircInfo2.InterDircDetail.CommitDetail.PrewriteRegionNum
	expectedSummaryElement.sumTxnRetry += int64(stmtInterDircInfo2.InterDircDetail.CommitDetail.TxnRetry)
	expectedSummaryElement.maxTxnRetry = stmtInterDircInfo2.InterDircDetail.CommitDetail.TxnRetry
	expectedSummaryElement.sumBackoffTimes += 1
	expectedSummaryElement.backoffTypes[einsteindb.BoTxnLock] = 1
	expectedSummaryElement.sumMem += stmtInterDircInfo2.MemMax
	expectedSummaryElement.maxMem = stmtInterDircInfo2.MemMax
	expectedSummaryElement.sumDisk += stmtInterDircInfo2.DiskMax
	expectedSummaryElement.maxDisk = stmtInterDircInfo2.DiskMax
	expectedSummaryElement.sumAffectedRows += stmtInterDircInfo2.StmtCtx.AffectedRows()
	expectedSummaryElement.lastSeen = stmtInterDircInfo2.StartTime

	s.ssMap.AddStatement(stmtInterDircInfo2)
	summary, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

	// Third memex is similar with the first memex, and its values are
	// less than that of the first memex.
	stmtInterDircInfo3 := &StmtInterDircInfo{
		SchemaName:     "schema_name",
		OriginalALLEGROSQL:    "original_sql3",
		NormalizedALLEGROSQL:  "normalized_sql",
		Digest:         "digest",
		CausetDigest:     "plan_digest",
		CausetGenerator:  emptyCausetGenerator,
		User:           "user3",
		TotalLatency:   1000,
		ParseLatency:   50,
		CompileLatency: 500,
		CausetTasks: &stmtctx.CausetTasksDetails{
			NumCausetTasks:    2,
			AvgProcessTime:    100,
			P90ProcessTime:    300,
			MaxProcessAddress: "300",
			MaxProcessTime:    350,
			AvgWaitTime:       20,
			P90WaitTime:       200,
			MaxWaitAddress:    "301",
			MaxWaitTime:       250,
		},
		InterDircDetail: &execdetails.InterDircDetails{
			CalleeAddress: "302",
			ProcessTime:   150,
			WaitTime:      15,
			BackoffTime:   18,
			RequestCount:  2,
			TotalKeys:     600,
			ProcessedKeys: 150,
			CommitDetail: &execdetails.CommitDetails{
				GetCommitTsTime:   50,
				PrewriteTime:      5000,
				CommitTime:        500,
				LocalLatchTime:    5,
				CommitBackoffTime: 100,
				Mu: struct {
					sync.Mutex
					BackoffTypes []fmt.Stringer
				}{
					BackoffTypes: []fmt.Stringer{einsteindb.BoTxnLock},
				},
				ResolveLockTime:   1000,
				WriteKeys:         10000,
				WriteSize:         100000,
				PrewriteRegionNum: 10,
				TxnRetry:          1,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Blocks:     blocks,
			IndexNames: indexes,
		},
		MemMax:    200,
		DiskMax:   200,
		StartTime: time.Date(2020, 1, 1, 10, 10, 0, 10, time.UTC),
		Succeed:   true,
	}
	stmtInterDircInfo3.StmtCtx.AddAffectedRows(20000)
	expectedSummaryElement.execCount++
	expectedSummaryElement.sumLatency += stmtInterDircInfo3.TotalLatency
	expectedSummaryElement.minLatency = stmtInterDircInfo3.TotalLatency
	expectedSummaryElement.sumParseLatency += stmtInterDircInfo3.ParseLatency
	expectedSummaryElement.sumCompileLatency += stmtInterDircInfo3.CompileLatency
	expectedSummaryElement.sumNumCausetTasks += int64(stmtInterDircInfo3.CausetTasks.NumCausetTasks)
	expectedSummaryElement.sumProcessTime += stmtInterDircInfo3.InterDircDetail.ProcessTime
	expectedSummaryElement.sumWaitTime += stmtInterDircInfo3.InterDircDetail.WaitTime
	expectedSummaryElement.sumBackoffTime += stmtInterDircInfo3.InterDircDetail.BackoffTime
	expectedSummaryElement.sumTotalKeys += stmtInterDircInfo3.InterDircDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtInterDircInfo3.InterDircDetail.ProcessedKeys
	expectedSummaryElement.sumGetCommitTsTime += stmtInterDircInfo3.InterDircDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.sumPrewriteTime += stmtInterDircInfo3.InterDircDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.sumCommitTime += stmtInterDircInfo3.InterDircDetail.CommitDetail.CommitTime
	expectedSummaryElement.sumLocalLatchTime += stmtInterDircInfo3.InterDircDetail.CommitDetail.LocalLatchTime
	expectedSummaryElement.sumCommitBackoffTime += stmtInterDircInfo3.InterDircDetail.CommitDetail.CommitBackoffTime
	expectedSummaryElement.sumResolveLockTime += stmtInterDircInfo3.InterDircDetail.CommitDetail.ResolveLockTime
	expectedSummaryElement.sumWriteKeys += int64(stmtInterDircInfo3.InterDircDetail.CommitDetail.WriteKeys)
	expectedSummaryElement.sumWriteSize += int64(stmtInterDircInfo3.InterDircDetail.CommitDetail.WriteSize)
	expectedSummaryElement.sumPrewriteRegionNum += int64(stmtInterDircInfo3.InterDircDetail.CommitDetail.PrewriteRegionNum)
	expectedSummaryElement.sumTxnRetry += int64(stmtInterDircInfo3.InterDircDetail.CommitDetail.TxnRetry)
	expectedSummaryElement.sumBackoffTimes += 1
	expectedSummaryElement.backoffTypes[einsteindb.BoTxnLock] = 2
	expectedSummaryElement.sumMem += stmtInterDircInfo3.MemMax
	expectedSummaryElement.sumDisk += stmtInterDircInfo3.DiskMax
	expectedSummaryElement.sumAffectedRows += stmtInterDircInfo3.StmtCtx.AffectedRows()
	expectedSummaryElement.firstSeen = stmtInterDircInfo3.StartTime

	s.ssMap.AddStatement(stmtInterDircInfo3)
	summary, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

	// Fourth memex is in a different schemaReplicant.
	stmtInterDircInfo4 := stmtInterDircInfo1
	stmtInterDircInfo4.SchemaName = "schema2"
	stmtInterDircInfo4.InterDircDetail.CommitDetail = nil
	key = &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo4.SchemaName,
		digest:     stmtInterDircInfo4.Digest,
		planDigest: stmtInterDircInfo4.CausetDigest,
	}
	s.ssMap.AddStatement(stmtInterDircInfo4)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 2)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// Fifth memex has a different digest.
	stmtInterDircInfo5 := stmtInterDircInfo1
	stmtInterDircInfo5.Digest = "digest2"
	key = &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo5.SchemaName,
		digest:     stmtInterDircInfo5.Digest,
		planDigest: stmtInterDircInfo4.CausetDigest,
	}
	s.ssMap.AddStatement(stmtInterDircInfo5)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 3)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// Sixth memex has a different plan digest.
	stmtInterDircInfo6 := stmtInterDircInfo1
	stmtInterDircInfo6.CausetDigest = "plan_digest2"
	key = &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo6.SchemaName,
		digest:     stmtInterDircInfo6.Digest,
		planDigest: stmtInterDircInfo6.CausetDigest,
	}
	s.ssMap.AddStatement(stmtInterDircInfo6)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 4)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
}

func matchStmtSummaryByDigest(first, second *stmtSummaryByDigest) bool {
	if first.schemaName != second.schemaName ||
		first.digest != second.digest ||
		first.normalizedALLEGROSQL != second.normalizedALLEGROSQL ||
		first.planDigest != second.planDigest ||
		first.blockNames != second.blockNames ||
		!strings.EqualFold(first.stmtType, second.stmtType) {
		return false
	}
	if first.history.Len() != second.history.Len() {
		return false
	}
	ele1 := first.history.Front()
	ele2 := second.history.Front()
	for {
		if ele1 == nil {
			break
		}
		ssElement1 := ele1.Value.(*stmtSummaryByDigestElement)
		ssElement2 := ele2.Value.(*stmtSummaryByDigestElement)
		if ssElement1.beginTime != ssElement2.beginTime ||
			ssElement1.endTime != ssElement2.endTime ||
			ssElement1.sampleALLEGROSQL != ssElement2.sampleALLEGROSQL ||
			ssElement1.sampleCauset != ssElement2.sampleCauset ||
			ssElement1.prevALLEGROSQL != ssElement2.prevALLEGROSQL ||
			ssElement1.execCount != ssElement2.execCount ||
			ssElement1.sumErrors != ssElement2.sumErrors ||
			ssElement1.sumWarnings != ssElement2.sumWarnings ||
			ssElement1.sumLatency != ssElement2.sumLatency ||
			ssElement1.maxLatency != ssElement2.maxLatency ||
			ssElement1.minLatency != ssElement2.minLatency ||
			ssElement1.sumParseLatency != ssElement2.sumParseLatency ||
			ssElement1.maxParseLatency != ssElement2.maxParseLatency ||
			ssElement1.sumCompileLatency != ssElement2.sumCompileLatency ||
			ssElement1.maxCompileLatency != ssElement2.maxCompileLatency ||
			ssElement1.sumNumCausetTasks != ssElement2.sumNumCausetTasks ||
			ssElement1.maxCopProcessTime != ssElement2.maxCopProcessTime ||
			ssElement1.maxCopProcessAddress != ssElement2.maxCopProcessAddress ||
			ssElement1.maxCopWaitTime != ssElement2.maxCopWaitTime ||
			ssElement1.maxCopWaitAddress != ssElement2.maxCopWaitAddress ||
			ssElement1.sumProcessTime != ssElement2.sumProcessTime ||
			ssElement1.maxProcessTime != ssElement2.maxProcessTime ||
			ssElement1.sumWaitTime != ssElement2.sumWaitTime ||
			ssElement1.maxWaitTime != ssElement2.maxWaitTime ||
			ssElement1.sumBackoffTime != ssElement2.sumBackoffTime ||
			ssElement1.maxBackoffTime != ssElement2.maxBackoffTime ||
			ssElement1.sumTotalKeys != ssElement2.sumTotalKeys ||
			ssElement1.maxTotalKeys != ssElement2.maxTotalKeys ||
			ssElement1.sumProcessedKeys != ssElement2.sumProcessedKeys ||
			ssElement1.maxProcessedKeys != ssElement2.maxProcessedKeys ||
			ssElement1.sumGetCommitTsTime != ssElement2.sumGetCommitTsTime ||
			ssElement1.maxGetCommitTsTime != ssElement2.maxGetCommitTsTime ||
			ssElement1.sumPrewriteTime != ssElement2.sumPrewriteTime ||
			ssElement1.maxPrewriteTime != ssElement2.maxPrewriteTime ||
			ssElement1.sumCommitTime != ssElement2.sumCommitTime ||
			ssElement1.maxCommitTime != ssElement2.maxCommitTime ||
			ssElement1.sumLocalLatchTime != ssElement2.sumLocalLatchTime ||
			ssElement1.maxLocalLatchTime != ssElement2.maxLocalLatchTime ||
			ssElement1.sumCommitBackoffTime != ssElement2.sumCommitBackoffTime ||
			ssElement1.maxCommitBackoffTime != ssElement2.maxCommitBackoffTime ||
			ssElement1.sumResolveLockTime != ssElement2.sumResolveLockTime ||
			ssElement1.maxResolveLockTime != ssElement2.maxResolveLockTime ||
			ssElement1.sumWriteKeys != ssElement2.sumWriteKeys ||
			ssElement1.maxWriteKeys != ssElement2.maxWriteKeys ||
			ssElement1.sumWriteSize != ssElement2.sumWriteSize ||
			ssElement1.maxWriteSize != ssElement2.maxWriteSize ||
			ssElement1.sumPrewriteRegionNum != ssElement2.sumPrewriteRegionNum ||
			ssElement1.maxPrewriteRegionNum != ssElement2.maxPrewriteRegionNum ||
			ssElement1.sumTxnRetry != ssElement2.sumTxnRetry ||
			ssElement1.maxTxnRetry != ssElement2.maxTxnRetry ||
			ssElement1.sumBackoffTimes != ssElement2.sumBackoffTimes ||
			ssElement1.sumMem != ssElement2.sumMem ||
			ssElement1.maxMem != ssElement2.maxMem ||
			ssElement1.sumAffectedRows != ssElement2.sumAffectedRows ||
			ssElement1.firstSeen != ssElement2.firstSeen ||
			ssElement1.lastSeen != ssElement2.lastSeen {
			return false
		}
		if len(ssElement1.backoffTypes) != len(ssElement2.backoffTypes) {
			return false
		}
		for key, value1 := range ssElement1.backoffTypes {
			value2, ok := ssElement2.backoffTypes[key]
			if !ok || value1 != value2 {
				return false
			}
		}
		if len(ssElement1.indexNames) != len(ssElement2.indexNames) {
			return false
		}
		for key, value1 := range ssElement1.indexNames {
			if value1 != ssElement2.indexNames[key] {
				return false
			}
		}
		ele1 = ele1.Next()
		ele2 = ele2.Next()
	}
	return true
}

func match(c *C, event []types.Causet, expected ...interface{}) {
	c.Assert(len(event), Equals, len(expected))
	for i := range event {
		got := fmt.Sprintf("%v", event[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}

func generateAnyInterDircInfo() *StmtInterDircInfo {
	blocks := []stmtctx.BlockEntry{{EDB: "db1", Block: "tb1"}, {EDB: "db2", Block: "tb2"}}
	indexes := []string{"a"}
	stmtInterDircInfo := &StmtInterDircInfo{
		SchemaName:     "schema_name",
		OriginalALLEGROSQL:    "original_sql1",
		NormalizedALLEGROSQL:  "normalized_sql",
		Digest:         "digest",
		CausetDigest:     "plan_digest",
		CausetGenerator:  emptyCausetGenerator,
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		CausetTasks: &stmtctx.CausetTasksDetails{
			NumCausetTasks:    10,
			AvgProcessTime:    1000,
			P90ProcessTime:    10000,
			MaxProcessAddress: "127",
			MaxProcessTime:    15000,
			AvgWaitTime:       100,
			P90WaitTime:       1000,
			MaxWaitAddress:    "128",
			MaxWaitTime:       1500,
		},
		InterDircDetail: &execdetails.InterDircDetails{
			CalleeAddress: "129",
			ProcessTime:   500,
			WaitTime:      50,
			BackoffTime:   80,
			RequestCount:  10,
			TotalKeys:     1000,
			ProcessedKeys: 500,
			CommitDetail: &execdetails.CommitDetails{
				GetCommitTsTime:   100,
				PrewriteTime:      10000,
				CommitTime:        1000,
				LocalLatchTime:    10,
				CommitBackoffTime: 200,
				Mu: struct {
					sync.Mutex
					BackoffTypes []fmt.Stringer
				}{
					BackoffTypes: []fmt.Stringer{einsteindb.BoTxnLock},
				},
				ResolveLockTime:   2000,
				WriteKeys:         20000,
				WriteSize:         200000,
				PrewriteRegionNum: 20,
				TxnRetry:          2,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Blocks:     blocks,
			IndexNames: indexes,
		},
		MemMax:    10000,
		DiskMax:   10000,
		StartTime: time.Date(2020, 1, 1, 10, 10, 10, 10, time.UTC),
		Succeed:   true,
	}
	stmtInterDircInfo.StmtCtx.AddAffectedRows(10000)
	return stmtInterDircInfo
}

// Test stmtSummaryByDigest.ToCauset.
func (s *testStmtSummarySuite) TestToCauset(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	s.ssMap.AddStatement(stmtInterDircInfo1)
	datums := s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, 1)
	n := types.NewTime(types.FromGoTime(time.Unix(s.ssMap.beginTimeForCurInterval, 0)), allegrosql.TypeTimestamp, types.DefaultFsp)
	e := types.NewTime(types.FromGoTime(time.Unix(s.ssMap.beginTimeForCurInterval+1800, 0)), allegrosql.TypeTimestamp, types.DefaultFsp)
	t := types.NewTime(types.FromGoTime(stmtInterDircInfo1.StartTime), allegrosql.TypeTimestamp, types.DefaultFsp)
	expectedCauset := []interface{}{n, e, "Select", stmtInterDircInfo1.SchemaName, stmtInterDircInfo1.Digest, stmtInterDircInfo1.NormalizedALLEGROSQL,
		"db1.tb1,db2.tb2", "a", stmtInterDircInfo1.User, 1, 0, 0, int64(stmtInterDircInfo1.TotalLatency),
		int64(stmtInterDircInfo1.TotalLatency), int64(stmtInterDircInfo1.TotalLatency), int64(stmtInterDircInfo1.TotalLatency),
		int64(stmtInterDircInfo1.ParseLatency), int64(stmtInterDircInfo1.ParseLatency), int64(stmtInterDircInfo1.CompileLatency),
		int64(stmtInterDircInfo1.CompileLatency), stmtInterDircInfo1.CausetTasks.NumCausetTasks, int64(stmtInterDircInfo1.CausetTasks.MaxProcessTime),
		stmtInterDircInfo1.CausetTasks.MaxProcessAddress, int64(stmtInterDircInfo1.CausetTasks.MaxWaitTime),
		stmtInterDircInfo1.CausetTasks.MaxWaitAddress, int64(stmtInterDircInfo1.InterDircDetail.ProcessTime), int64(stmtInterDircInfo1.InterDircDetail.ProcessTime),
		int64(stmtInterDircInfo1.InterDircDetail.WaitTime), int64(stmtInterDircInfo1.InterDircDetail.WaitTime), int64(stmtInterDircInfo1.InterDircDetail.BackoffTime),
		int64(stmtInterDircInfo1.InterDircDetail.BackoffTime), stmtInterDircInfo1.InterDircDetail.TotalKeys, stmtInterDircInfo1.InterDircDetail.TotalKeys,
		stmtInterDircInfo1.InterDircDetail.ProcessedKeys, stmtInterDircInfo1.InterDircDetail.ProcessedKeys,
		int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteTime), int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteTime),
		int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitTime), int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitTime),
		int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.GetCommitTsTime), int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.GetCommitTsTime),
		stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitBackoffTime, stmtInterDircInfo1.InterDircDetail.CommitDetail.CommitBackoffTime,
		stmtInterDircInfo1.InterDircDetail.CommitDetail.ResolveLockTime, stmtInterDircInfo1.InterDircDetail.CommitDetail.ResolveLockTime,
		int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.LocalLatchTime), int64(stmtInterDircInfo1.InterDircDetail.CommitDetail.LocalLatchTime),
		stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteKeys, stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteKeys,
		stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteSize, stmtInterDircInfo1.InterDircDetail.CommitDetail.WriteSize,
		stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteRegionNum, stmtInterDircInfo1.InterDircDetail.CommitDetail.PrewriteRegionNum,
		stmtInterDircInfo1.InterDircDetail.CommitDetail.TxnRetry, stmtInterDircInfo1.InterDircDetail.CommitDetail.TxnRetry, 0, 0, 1,
		"txnLock:1", stmtInterDircInfo1.MemMax, stmtInterDircInfo1.MemMax, stmtInterDircInfo1.DiskMax, stmtInterDircInfo1.DiskMax, stmtInterDircInfo1.StmtCtx.AffectedRows(),
		t, t, 0, 0, stmtInterDircInfo1.OriginalALLEGROSQL, stmtInterDircInfo1.PrevALLEGROSQL, "plan_digest", ""}
	match(c, datums[0], expectedCauset...)
	datums = s.ssMap.ToHistoryCauset(nil, true)
	c.Assert(len(datums), Equals, 1)
	match(c, datums[0], expectedCauset...)
}

// Test AddStatement and ToCauset parallel.
func (s *testStmtSummarySuite) TestAddStatementParallel(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	addStmtFunc := func() {
		defer wg.Done()
		stmtInterDircInfo1 := generateAnyInterDircInfo()

		// Add 32 times with different digest.
		for i := 0; i < loops; i++ {
			stmtInterDircInfo1.Digest = fmt.Sprintf("digest%d", i)
			s.ssMap.AddStatement(stmtInterDircInfo1)
		}

		// There would be 32 summaries.
		datums := s.ssMap.ToCurrentCauset(nil, true)
		c.Assert(len(datums), Equals, loops)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	wg.Wait()

	datums := s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, loops)
}

// Test max number of memex count.
func (s *testStmtSummarySuite) TestMaxStmtCount(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	// Test the original value and modify it.
	maxStmtCount := s.ssMap.maxStmtCount()
	c.Assert(maxStmtCount, Equals, int(config.GetGlobalConfig().StmtSummary.MaxStmtCount))
	c.Assert(s.ssMap.SetMaxStmtCount("10", false), IsNil)
	c.Assert(s.ssMap.maxStmtCount(), Equals, 10)
	defer func() {
		c.Assert(s.ssMap.SetMaxStmtCount("", false), IsNil)
		c.Assert(s.ssMap.SetMaxStmtCount("", true), IsNil)
		c.Assert(maxStmtCount, Equals, int(config.GetGlobalConfig().StmtSummary.MaxStmtCount))
	}()

	// 100 digests
	stmtInterDircInfo1 := generateAnyInterDircInfo()
	loops := 100
	for i := 0; i < loops; i++ {
		stmtInterDircInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtInterDircInfo1)
	}

	// Summary count should be MaxStmtCount.
	sm := s.ssMap.summaryMap
	c.Assert(sm.Size(), Equals, 10)

	// LRU cache should work.
	for i := loops - 10; i < loops; i++ {
		key := &stmtSummaryByDigestKey{
			schemaName: stmtInterDircInfo1.SchemaName,
			digest:     fmt.Sprintf("digest%d", i),
			planDigest: stmtInterDircInfo1.CausetDigest,
		}
		_, ok := sm.Get(key)
		c.Assert(ok, IsTrue)
	}

	// Change to a bigger value.
	c.Assert(s.ssMap.SetMaxStmtCount("50", true), IsNil)
	for i := 0; i < loops; i++ {
		stmtInterDircInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtInterDircInfo1)
	}
	c.Assert(sm.Size(), Equals, 50)

	// Change to a smaller value.
	c.Assert(s.ssMap.SetMaxStmtCount("10", true), IsNil)
	for i := 0; i < loops; i++ {
		stmtInterDircInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtInterDircInfo1)
	}
	c.Assert(sm.Size(), Equals, 10)
}

// Test max length of normalized and sample ALLEGROALLEGROSQL.
func (s *testStmtSummarySuite) TestMaxALLEGROSQLLength(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	// Test the original value and modify it.
	maxALLEGROSQLLength := s.ssMap.maxALLEGROSQLLength()
	c.Assert(maxALLEGROSQLLength, Equals, int(config.GetGlobalConfig().StmtSummary.MaxALLEGROSQLLength))

	// Create a long ALLEGROALLEGROSQL
	length := maxALLEGROSQLLength * 10
	str := strings.Repeat("a", length)

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	stmtInterDircInfo1.OriginalALLEGROSQL = str
	stmtInterDircInfo1.NormalizedALLEGROSQL = str
	s.ssMap.AddStatement(stmtInterDircInfo1)

	key := &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo1.SchemaName,
		digest:     stmtInterDircInfo1.Digest,
		planDigest: stmtInterDircInfo1.CausetDigest,
		prevDigest: stmtInterDircInfo1.PrevALLEGROSQLDigest,
	}
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	expectedALLEGROSQL := fmt.Sprintf("%s(len:%d)", strings.Repeat("a", maxALLEGROSQLLength), length)
	summary := value.(*stmtSummaryByDigest)
	c.Assert(summary.normalizedALLEGROSQL, Equals, expectedALLEGROSQL)
	ssElement := summary.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.sampleALLEGROSQL, Equals, expectedALLEGROSQL)

	c.Assert(s.ssMap.SetMaxALLEGROSQLLength("100", false), IsNil)
	c.Assert(s.ssMap.maxALLEGROSQLLength(), Equals, 100)
	c.Assert(s.ssMap.SetMaxALLEGROSQLLength("10", true), IsNil)
	c.Assert(s.ssMap.maxALLEGROSQLLength(), Equals, 10)
	c.Assert(s.ssMap.SetMaxALLEGROSQLLength("", true), IsNil)
	c.Assert(s.ssMap.maxALLEGROSQLLength(), Equals, 100)
}

// Test AddStatement and SetMaxStmtCount parallel.
func (s *testStmtSummarySuite) TestSetMaxStmtCountParallel(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	threads := 8
	loops := 20
	wg := sync.WaitGroup{}
	wg.Add(threads + 1)

	addStmtFunc := func() {
		defer wg.Done()
		stmtInterDircInfo1 := generateAnyInterDircInfo()

		// Add 32 times with different digest.
		for i := 0; i < loops; i++ {
			stmtInterDircInfo1.Digest = fmt.Sprintf("digest%d", i)
			s.ssMap.AddStatement(stmtInterDircInfo1)
		}
	}
	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}

	defer c.Assert(s.ssMap.SetMaxStmtCount("", true), IsNil)
	setStmtCountFunc := func() {
		defer wg.Done()
		// Turn down MaxStmtCount one by one.
		for i := 10; i > 0; i-- {
			c.Assert(s.ssMap.SetMaxStmtCount(strconv.Itoa(i), true), IsNil)
		}
	}
	go setStmtCountFunc()

	wg.Wait()

	datums := s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, 1)
}

// Test setting EnableStmtSummary to 0.
func (s *testStmtSummarySuite) TestDisableStmtSummary(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()

	// Set false in global scope, it should work.
	s.ssMap.SetEnabled("0", false)
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	s.ssMap.AddStatement(stmtInterDircInfo1)
	datums := s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, 0)

	// Set true in stochastik scope, it will overwrite global scope.
	s.ssMap.SetEnabled("1", true)

	s.ssMap.AddStatement(stmtInterDircInfo1)
	datums = s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, 1)

	// Set false in global scope, it shouldn't work.
	s.ssMap.SetEnabled("0", false)
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtInterDircInfo2 := stmtInterDircInfo1
	stmtInterDircInfo2.OriginalALLEGROSQL = "original_sql2"
	stmtInterDircInfo2.NormalizedALLEGROSQL = "normalized_sql2"
	stmtInterDircInfo2.Digest = "digest2"
	s.ssMap.AddStatement(stmtInterDircInfo2)
	datums = s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, 2)

	// Unset in stochastik scope.
	s.ssMap.SetEnabled("", true)
	s.ssMap.beginTimeForCurInterval = now + 60
	s.ssMap.AddStatement(stmtInterDircInfo2)
	datums = s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, 0)

	// Unset in global scope.
	s.ssMap.SetEnabled("", false)
	s.ssMap.beginTimeForCurInterval = now + 60
	s.ssMap.AddStatement(stmtInterDircInfo1)
	datums = s.ssMap.ToCurrentCauset(nil, true)
	c.Assert(len(datums), Equals, 1)

	// Set back.
	s.ssMap.SetEnabled("1", false)
}

// Test disable and enable memex summary concurrently with adding memexs.
func (s *testStmtSummarySuite) TestEnableSummaryParallel(c *C) {
	s.ssMap.Clear()

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	addStmtFunc := func() {
		defer wg.Done()
		stmtInterDircInfo1 := generateAnyInterDircInfo()

		// Add 32 times with same digest.
		for i := 0; i < loops; i++ {
			// Sometimes enable it and sometimes disable it.
			s.ssMap.SetEnabled(fmt.Sprintf("%d", i%2), false)
			s.ssMap.AddStatement(stmtInterDircInfo1)
			// Try to read it.
			s.ssMap.ToHistoryCauset(nil, true)
		}
		s.ssMap.SetEnabled("1", false)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	// Ensure that there's no deadlocks.
	wg.Wait()

	// Ensure that it's enabled at last.
	c.Assert(s.ssMap.Enabled(), IsTrue)
}

// Test GetMoreThanOnceSelect.
func (s *testStmtSummarySuite) TestGetMoreThanOnceSelect(c *C) {
	s.ssMap.Clear()

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	stmtInterDircInfo1.OriginalALLEGROSQL = "insert 1"
	stmtInterDircInfo1.NormalizedALLEGROSQL = "insert ?"
	stmtInterDircInfo1.StmtCtx.StmtType = "Insert"
	s.ssMap.AddStatement(stmtInterDircInfo1)
	schemas, sqls := s.ssMap.GetMoreThanOnceSelect()
	c.Assert(len(schemas), Equals, 0)
	c.Assert(len(sqls), Equals, 0)

	stmtInterDircInfo1.NormalizedALLEGROSQL = "select ?"
	stmtInterDircInfo1.Digest = "digest1"
	stmtInterDircInfo1.StmtCtx.StmtType = "Select"
	s.ssMap.AddStatement(stmtInterDircInfo1)
	schemas, sqls = s.ssMap.GetMoreThanOnceSelect()
	c.Assert(len(schemas), Equals, 0)
	c.Assert(len(sqls), Equals, 0)

	s.ssMap.AddStatement(stmtInterDircInfo1)
	schemas, sqls = s.ssMap.GetMoreThanOnceSelect()
	c.Assert(len(schemas), Equals, 1)
	c.Assert(len(sqls), Equals, 1)
}

// Test `formatBackoffTypes`.
func (s *testStmtSummarySuite) TestFormatBackoffTypes(c *C) {
	backoffMap := make(map[fmt.Stringer]int)
	c.Assert(formatBackoffTypes(backoffMap), IsNil)

	backoffMap[einsteindb.BoFIDelRPC] = 1
	c.Assert(formatBackoffTypes(backoffMap), Equals, "FIDelRPC:1")

	backoffMap[einsteindb.BoTxnLock] = 2
	c.Assert(formatBackoffTypes(backoffMap), Equals, "txnLock:2,FIDelRPC:1")
}

// Test refreshing current memex summary periodically.
func (s *testStmtSummarySuite) TestRefreshCurrentSummary(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()

	s.ssMap.beginTimeForCurInterval = now + 10
	stmtInterDircInfo1 := generateAnyInterDircInfo()
	key := &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo1.SchemaName,
		digest:     stmtInterDircInfo1.Digest,
		planDigest: stmtInterDircInfo1.CausetDigest,
	}
	s.ssMap.AddStatement(stmtInterDircInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssElement := value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, s.ssMap.beginTimeForCurInterval)
	c.Assert(ssElement.execCount, Equals, int64(1))

	s.ssMap.beginTimeForCurInterval = now - 1900
	ssElement.beginTime = now - 1900
	s.ssMap.AddStatement(stmtInterDircInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(value.(*stmtSummaryByDigest).history.Len(), Equals, 2)
	ssElement = value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Greater, now-1900)
	c.Assert(ssElement.execCount, Equals, int64(1))

	s.ssMap.SetRefreshInterval("10", false)
	s.ssMap.beginTimeForCurInterval = now - 20
	ssElement.beginTime = now - 20
	s.ssMap.AddStatement(stmtInterDircInfo1)
	c.Assert(value.(*stmtSummaryByDigest).history.Len(), Equals, 3)
}

// Test expiring memex summary to history.
func (s *testStmtSummarySuite) TestSummaryHistory(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.SetRefreshInterval("10", false)
	s.ssMap.SetHistorySize("10", false)
	defer s.ssMap.SetRefreshInterval("1800", false)
	defer s.ssMap.SetHistorySize("24", false)

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	key := &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo1.SchemaName,
		digest:     stmtInterDircInfo1.Digest,
		planDigest: stmtInterDircInfo1.CausetDigest,
	}
	for i := 0; i < 11; i++ {
		s.ssMap.beginTimeForCurInterval = now + int64(i+1)*10
		s.ssMap.AddStatement(stmtInterDircInfo1)
		c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
		value, ok := s.ssMap.summaryMap.Get(key)
		c.Assert(ok, IsTrue)
		ssbd := value.(*stmtSummaryByDigest)
		if i < 10 {
			c.Assert(ssbd.history.Len(), Equals, i+1)
			ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			c.Assert(ssElement.beginTime, Equals, s.ssMap.beginTimeForCurInterval)
			c.Assert(ssElement.execCount, Equals, int64(1))
		} else {
			c.Assert(ssbd.history.Len(), Equals, 10)
			ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			c.Assert(ssElement.beginTime, Equals, s.ssMap.beginTimeForCurInterval)
			ssElement = ssbd.history.Front().Value.(*stmtSummaryByDigestElement)
			c.Assert(ssElement.beginTime, Equals, now+20)
		}
	}
	causet := s.ssMap.ToHistoryCauset(nil, true)
	c.Assert(len(causet), Equals, 10)

	s.ssMap.SetHistorySize("5", false)
	causet = s.ssMap.ToHistoryCauset(nil, true)
	c.Assert(len(causet), Equals, 5)
}

// Test summary when PrevALLEGROSQL is not empty.
func (s *testStmtSummarySuite) TestPrevALLEGROSQL(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	stmtInterDircInfo1.PrevALLEGROSQL = "prevALLEGROSQL"
	stmtInterDircInfo1.PrevALLEGROSQLDigest = "prevALLEGROSQLDigest"
	s.ssMap.AddStatement(stmtInterDircInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo1.SchemaName,
		digest:     stmtInterDircInfo1.Digest,
		planDigest: stmtInterDircInfo1.CausetDigest,
		prevDigest: stmtInterDircInfo1.PrevALLEGROSQLDigest,
	}
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	_, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// same prevALLEGROSQL
	s.ssMap.AddStatement(stmtInterDircInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)

	// different prevALLEGROSQL
	stmtInterDircInfo2 := stmtInterDircInfo1
	stmtInterDircInfo2.PrevALLEGROSQL = "prevALLEGROSQL1"
	stmtInterDircInfo2.PrevALLEGROSQLDigest = "prevALLEGROSQLDigest1"
	key.prevDigest = stmtInterDircInfo2.PrevALLEGROSQLDigest
	s.ssMap.AddStatement(stmtInterDircInfo2)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 2)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
}

func (s *testStmtSummarySuite) TestEndTime(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now - 100

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	s.ssMap.AddStatement(stmtInterDircInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo1.SchemaName,
		digest:     stmtInterDircInfo1.Digest,
		planDigest: stmtInterDircInfo1.CausetDigest,
	}
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssbd := value.(*stmtSummaryByDigest)
	ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, now-100)
	c.Assert(ssElement.endTime, Equals, now+1700)

	s.ssMap.SetRefreshInterval("3600", false)
	defer s.ssMap.SetRefreshInterval("1800", false)
	s.ssMap.AddStatement(stmtInterDircInfo1)
	c.Assert(ssbd.history.Len(), Equals, 1)
	ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, now-100)
	c.Assert(ssElement.endTime, Equals, now+3500)

	s.ssMap.SetRefreshInterval("60", false)
	s.ssMap.AddStatement(stmtInterDircInfo1)
	c.Assert(ssbd.history.Len(), Equals, 2)
	now2 := time.Now().Unix()
	ssElement = ssbd.history.Front().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, now-100)
	c.Assert(ssElement.endTime, GreaterEqual, now)
	c.Assert(ssElement.endTime, LessEqual, now2)
	ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, GreaterEqual, now-60)
	c.Assert(ssElement.beginTime, LessEqual, now2)
	c.Assert(ssElement.endTime-ssElement.beginTime, Equals, int64(60))
}

func (s *testStmtSummarySuite) TestPointGet(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now - 100

	stmtInterDircInfo1 := generateAnyInterDircInfo()
	stmtInterDircInfo1.CausetDigest = ""
	stmtInterDircInfo1.CausetDigestGen = fakeCausetDigestGenerator
	s.ssMap.AddStatement(stmtInterDircInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtInterDircInfo1.SchemaName,
		digest:     stmtInterDircInfo1.Digest,
		planDigest: "",
	}
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssbd := value.(*stmtSummaryByDigest)
	ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.execCount, Equals, int64(1))

	s.ssMap.AddStatement(stmtInterDircInfo1)
	c.Assert(ssElement.execCount, Equals, int64(2))
}

func (s *testStmtSummarySuite) TestAccessPrivilege(c *C) {
	s.ssMap.Clear()

	loops := 32
	stmtInterDircInfo1 := generateAnyInterDircInfo()

	for i := 0; i < loops; i++ {
		stmtInterDircInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtInterDircInfo1)
	}

	user := &auth.UserIdentity{Username: "user"}
	badUser := &auth.UserIdentity{Username: "bad_user"}

	datums := s.ssMap.ToCurrentCauset(user, false)
	c.Assert(len(datums), Equals, loops)
	datums = s.ssMap.ToCurrentCauset(badUser, false)
	c.Assert(len(datums), Equals, 0)
	datums = s.ssMap.ToCurrentCauset(badUser, true)
	c.Assert(len(datums), Equals, loops)

	datums = s.ssMap.ToHistoryCauset(user, false)
	c.Assert(len(datums), Equals, loops)
	datums = s.ssMap.ToHistoryCauset(badUser, false)
	c.Assert(len(datums), Equals, 0)
	datums = s.ssMap.ToHistoryCauset(badUser, true)
	c.Assert(len(datums), Equals, loops)
}
