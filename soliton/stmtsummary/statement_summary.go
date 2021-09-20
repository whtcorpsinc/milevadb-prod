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
	"bytes"
	"container/list"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/milevadb/soliton/ekvcache"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// stmtSummaryByDigestKey defines key for stmtSummaryByDigestMap.summaryMap.
type stmtSummaryByDigestKey struct {
	// Same memexs may appear in different schemaReplicant, but they refer to different blocks.
	schemaName string
	digest     string
	// The digest of the previous memex.
	prevDigest string
	// The digest of the plan of this ALLEGROALLEGROSQL.
	planDigest string
	// `hash` is the hash value of this object.
	hash []byte
}

// Hash implements SimpleLRUCache.Key.
// Only when current ALLEGROALLEGROSQL is `commit` do we record `prevALLEGROSQL`. Otherwise, `prevALLEGROSQL` is empty.
// `prevALLEGROSQL` is included in the key To distinguish different transactions.
func (key *stmtSummaryByDigestKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest)+len(key.prevDigest)+len(key.planDigest))
		key.hash = append(key.hash, replog.Slice(key.digest)...)
		key.hash = append(key.hash, replog.Slice(key.schemaName)...)
		key.hash = append(key.hash, replog.Slice(key.prevDigest)...)
		key.hash = append(key.hash, replog.Slice(key.planDigest)...)
	}
	return key.hash
}

// stmtSummaryByDigestMap is a LRU cache that stores memex summaries.
type stmtSummaryByDigestMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *ekvcache.SimpleLRUCache
	// beginTimeForCurInterval is the begin time for current summary.
	beginTimeForCurInterval int64

	// sysVars encapsulates system variables needed to control memex summary.
	sysVars *systemVars
}

// StmtSummaryByDigestMap is a global map containing all memex summaries.
var StmtSummaryByDigestMap = newStmtSummaryByDigestMap()

// stmtSummaryByDigest is the summary for each type of memexs.
type stmtSummaryByDigest struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	// Mutex is only used to dagger `history`.
	sync.Mutex
	initialized bool
	// Each element in history is a summary in one interval.
	history *list.List
	// Following fields are common for each summary element.
	// They won't change once this object is created, so locking is not needed.
	schemaName           string
	digest               string
	planDigest           string
	stmtType             string
	normalizedALLEGROSQL string
	blockNames           string
	isInternal           bool
}

// stmtSummaryByDigestElement is the summary for each type of memexs in current interval.
type stmtSummaryByDigestElement struct {
	sync.Mutex
	// Each summary is summarized between [beginTime, endTime).
	beginTime int64
	endTime   int64
	// basic
	sampleALLEGROSQL string
	prevALLEGROSQL   string
	sampleCauset     string
	indexNames       []string
	execCount        int64
	sumErrors        int
	sumWarnings      int
	// latency
	sumLatency        time.Duration
	maxLatency        time.Duration
	minLatency        time.Duration
	sumParseLatency   time.Duration
	maxParseLatency   time.Duration
	sumCompileLatency time.Duration
	maxCompileLatency time.Duration
	// interlock
	sumNumCausetTasks    int64
	maxCopProcessTime    time.Duration
	maxCopProcessAddress string
	maxCopWaitTime       time.Duration
	maxCopWaitAddress    string
	// EinsteinDB
	sumProcessTime   time.Duration
	maxProcessTime   time.Duration
	sumWaitTime      time.Duration
	maxWaitTime      time.Duration
	sumBackoffTime   time.Duration
	maxBackoffTime   time.Duration
	sumTotalKeys     int64
	maxTotalKeys     int64
	sumProcessedKeys int64
	maxProcessedKeys int64
	// txn
	commitCount            int64
	sumGetCommitTsTime     time.Duration
	maxGetCommitTsTime     time.Duration
	sumPrewriteTime        time.Duration
	maxPrewriteTime        time.Duration
	sumCommitTime          time.Duration
	maxCommitTime          time.Duration
	sumLocalLatchTime      time.Duration
	maxLocalLatchTime      time.Duration
	sumCommitBackoffTime   int64
	maxCommitBackoffTime   int64
	sumResolveLockTime     int64
	maxResolveLockTime     int64
	sumWriteKeys           int64
	maxWriteKeys           int
	sumWriteSize           int64
	maxWriteSize           int
	sumPrewriteRegionNum   int64
	maxPrewriteRegionNum   int32
	sumTxnRetry            int64
	maxTxnRetry            int
	sumInterDircRetryCount int64
	sumInterDircRetryTime  time.Duration
	sumBackoffTimes        int64
	backoffTypes           map[fmt.Stringer]int
	authUsers              map[string]struct{}
	// other
	sumMem          int64
	maxMem          int64
	sumDisk         int64
	maxDisk         int64
	sumAffectedRows uint64
	// The first time this type of ALLEGROALLEGROSQL executes.
	firstSeen time.Time
	// The last time this type of ALLEGROALLEGROSQL executes.
	lastSeen time.Time
	// plan cache
	planInCache   bool
	planCacheHits int64
	// pessimistic execution retry information.
	execRetryCount uint
	execRetryTime  time.Duration
}

// StmtInterDircInfo records execution information of each memex.
type StmtInterDircInfo struct {
	SchemaName           string
	OriginalALLEGROSQL   string
	NormalizedALLEGROSQL string
	Digest               string
	PrevALLEGROSQL       string
	PrevALLEGROSQLDigest string
	CausetGenerator      func() string
	CausetDigest         string
	CausetDigestGen      func() string
	User                 string
	TotalLatency         time.Duration
	ParseLatency         time.Duration
	CompileLatency       time.Duration
	StmtCtx              *stmtctx.StatementContext
	CausetTasks          *stmtctx.CausetTasksDetails
	InterDircDetail      *execdetails.InterDircDetails
	MemMax               int64
	DiskMax              int64
	StartTime            time.Time
	IsInternal           bool
	Succeed              bool
	CausetInCache        bool
	InterDircRetryCount  uint
	InterDircRetryTime   time.Duration
}

// newStmtSummaryByDigestMap creates an empty stmtSummaryByDigestMap.
func newStmtSummaryByDigestMap() *stmtSummaryByDigestMap {
	sysVars := newSysVars()
	maxStmtCount := uint(sysVars.getVariable(typeMaxStmtCount))
	return &stmtSummaryByDigestMap{
		summaryMap: ekvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
		sysVars:    sysVars,
	}
}

// AddStatement adds a memex to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtInterDircInfo) {
	// All times are counted in seconds.
	now := time.Now().Unix()

	intervalSeconds := ssMap.refreshInterval()
	historySize := ssMap.historySize()

	key := &stmtSummaryByDigestKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
		prevDigest: sei.PrevALLEGROSQLDigest,
		planDigest: sei.CausetDigest,
	}
	// Calculate hash value in advance, to reduce the time holding the dagger.
	key.Hash()

	// Enclose the causet in a function to ensure the dagger will always be released.
	summary, beginTime := func() (*stmtSummaryByDigest, int64) {
		ssMap.Lock()
		defer ssMap.Unlock()

		// Check again. Statements could be added before disabling the flag and after Clear().
		if !ssMap.Enabled() {
			return nil, 0
		}
		if sei.IsInternal && !ssMap.EnabledInternal() {
			return nil, 0
		}

		if ssMap.beginTimeForCurInterval+intervalSeconds <= now {
			// `beginTimeForCurInterval` is a multiple of intervalSeconds, so that when the interval is a multiple
			// of 60 (or 600, 1800, 3600, etc), begin time shows 'XX:XX:00', not 'XX:XX:01'~'XX:XX:59'.
			ssMap.beginTimeForCurInterval = now / intervalSeconds * intervalSeconds
		}

		beginTime := ssMap.beginTimeForCurInterval
		value, ok := ssMap.summaryMap.Get(key)
		var summary *stmtSummaryByDigest
		if !ok {
			// Lazy initialize it to release ssMap.mutex ASAP.
			summary = new(stmtSummaryByDigest)
			ssMap.summaryMap.Put(key, summary)
		} else {
			summary = value.(*stmtSummaryByDigest)
		}
		summary.isInternal = summary.isInternal && sei.IsInternal
		return summary, beginTime
	}()

	// Lock a single entry, not the whole cache.
	if summary != nil {
		summary.add(sei, beginTime, intervalSeconds, historySize)
	}
}

// Clear removes all memex summaries.
func (ssMap *stmtSummaryByDigestMap) Clear() {
	ssMap.Lock()
	defer ssMap.Unlock()

	ssMap.summaryMap.DeleteAll()
	ssMap.beginTimeForCurInterval = 0
}

// clearInternal removes all memex summaries which are internal summaries.
func (ssMap *stmtSummaryByDigestMap) clearInternal() {
	ssMap.Lock()
	defer ssMap.Unlock()

	for _, key := range ssMap.summaryMap.Keys() {
		summary, ok := ssMap.summaryMap.Get(key)
		if !ok {
			continue
		}
		if summary.(*stmtSummaryByDigest).isInternal {
			ssMap.summaryMap.Delete(key)
		}
	}
}

// ToCurrentCauset converts current memex summaries to causet.
func (ssMap *stmtSummaryByDigestMap) ToCurrentCauset(user *auth.UserIdentity, isSuper bool) [][]types.Causet {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	beginTime := ssMap.beginTimeForCurInterval
	ssMap.Unlock()

	rows := make([][]types.Causet, 0, len(values))
	for _, value := range values {
		record := value.(*stmtSummaryByDigest).toCurrentCauset(beginTime, user, isSuper)
		if record != nil {
			rows = append(rows, record)
		}
	}
	return rows
}

// ToHistoryCauset converts history memexs summaries to causet.
func (ssMap *stmtSummaryByDigestMap) ToHistoryCauset(user *auth.UserIdentity, isSuper bool) [][]types.Causet {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	historySize := ssMap.historySize()
	rows := make([][]types.Causet, 0, len(values)*historySize)
	for _, value := range values {
		records := value.(*stmtSummaryByDigest).toHistoryCauset(historySize, user, isSuper)
		rows = append(rows, records...)
	}
	return rows
}

// GetMoreThanOnceSelect gets users' select ALLEGROSQLs that occurred more than once.
func (ssMap *stmtSummaryByDigestMap) GetMoreThanOnceSelect() ([]string, []string) {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	schemas := make([]string, 0, len(values))
	sqls := make([]string, 0, len(values))
	for _, value := range values {
		ssbd := value.(*stmtSummaryByDigest)
		func() {
			ssbd.Lock()
			defer ssbd.Unlock()
			if ssbd.initialized && ssbd.stmtType == "Select" {
				if ssbd.history.Len() > 0 {
					ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
					ssElement.Lock()

					// Empty auth users means that it is an internal queries.
					if len(ssElement.authUsers) > 0 && (ssbd.history.Len() > 1 || ssElement.execCount > 1) {
						schemas = append(schemas, ssbd.schemaName)
						sqls = append(sqls, ssElement.sampleALLEGROSQL)
					}
					ssElement.Unlock()
				}
			}
		}()
	}
	return schemas, sqls
}

// SetEnabled enables or disables memex summary in global(cluster) or stochastik(server) scope.
func (ssMap *stmtSummaryByDigestMap) SetEnabled(value string, inStochastik bool) error {
	if err := ssMap.sysVars.setVariable(typeEnable, value, inStochastik); err != nil {
		return err
	}

	// Clear all summaries once memex summary is disabled.
	if ssMap.sysVars.getVariable(typeEnable) == 0 {
		ssMap.Clear()
	}
	return nil
}

// Enabled returns whether memex summary is enabled.
func (ssMap *stmtSummaryByDigestMap) Enabled() bool {
	return ssMap.sysVars.getVariable(typeEnable) > 0
}

// SetEnabledInternalQuery enables or disables internal memex summary in global(cluster) or stochastik(server) scope.
func (ssMap *stmtSummaryByDigestMap) SetEnabledInternalQuery(value string, inStochastik bool) error {
	if err := ssMap.sysVars.setVariable(typeEnableInternalQuery, value, inStochastik); err != nil {
		return err
	}

	// Clear all summaries once memex summary is disabled.
	if ssMap.sysVars.getVariable(typeEnableInternalQuery) == 0 {
		ssMap.clearInternal()
	}
	return nil
}

// EnabledInternal returns whether internal memex summary is enabled.
func (ssMap *stmtSummaryByDigestMap) EnabledInternal() bool {
	return ssMap.sysVars.getVariable(typeEnableInternalQuery) > 0
}

// SetRefreshInterval sets refreshing interval in ssMap.sysVars.
func (ssMap *stmtSummaryByDigestMap) SetRefreshInterval(value string, inStochastik bool) error {
	return ssMap.sysVars.setVariable(typeRefreshInterval, value, inStochastik)
}

// refreshInterval gets the refresh interval for summaries.
func (ssMap *stmtSummaryByDigestMap) refreshInterval() int64 {
	return ssMap.sysVars.getVariable(typeRefreshInterval)
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetHistorySize(value string, inStochastik bool) error {
	return ssMap.sysVars.setVariable(typeHistorySize, value, inStochastik)
}

// historySize gets the history size for summaries.
func (ssMap *stmtSummaryByDigestMap) historySize() int {
	return int(ssMap.sysVars.getVariable(typeHistorySize))
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetMaxStmtCount(value string, inStochastik bool) error {
	if err := ssMap.sysVars.setVariable(typeMaxStmtCount, value, inStochastik); err != nil {
		return err
	}
	capacity := ssMap.sysVars.getVariable(typeMaxStmtCount)

	ssMap.Lock()
	defer ssMap.Unlock()
	return ssMap.summaryMap.SetCapacity(uint(capacity))
}

func (ssMap *stmtSummaryByDigestMap) maxStmtCount() int {
	return int(ssMap.sysVars.getVariable(typeMaxStmtCount))
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetMaxALLEGROSQLLength(value string, inStochastik bool) error {
	return ssMap.sysVars.setVariable(typeMaxALLEGROSQLLength, value, inStochastik)
}

func (ssMap *stmtSummaryByDigestMap) maxALLEGROSQLLength() int {
	return int(ssMap.sysVars.getVariable(typeMaxALLEGROSQLLength))
}

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtInterDircInfo.
func (ssbd *stmtSummaryByDigest) init(sei *StmtInterDircInfo, beginTime int64, intervalSeconds int64, historySize int) {
	// Use "," to separate causet names to support FIND_IN_SET.
	var buffer bytes.Buffer
	for i, value := range sei.StmtCtx.Blocks {
		// In `create database` memex, EDB name is not empty but causet name is empty.
		if len(value.Block) == 0 {
			continue
		}
		buffer.WriteString(strings.ToLower(value.EDB))
		buffer.WriteString(".")
		buffer.WriteString(strings.ToLower(value.Block))
		if i < len(sei.StmtCtx.Blocks)-1 {
			buffer.WriteString(",")
		}
	}
	blockNames := buffer.String()

	planDigest := sei.CausetDigest
	if sei.CausetDigestGen != nil && len(planDigest) == 0 {
		// It comes here only when the plan is 'Point_Get'.
		planDigest = sei.CausetDigestGen()
	}
	ssbd.schemaName = sei.SchemaName
	ssbd.digest = sei.Digest
	ssbd.planDigest = planDigest
	ssbd.stmtType = sei.StmtCtx.StmtType
	ssbd.normalizedALLEGROSQL = formatALLEGROSQL(sei.NormalizedALLEGROSQL)
	ssbd.blockNames = blockNames
	ssbd.history = list.New()
	ssbd.initialized = true
}

func (ssbd *stmtSummaryByDigest) add(sei *StmtInterDircInfo, beginTime int64, intervalSeconds int64, historySize int) {
	// Enclose this causet in a function to ensure the dagger will always be released.
	ssElement, isElementNew := func() (*stmtSummaryByDigestElement, bool) {
		ssbd.Lock()
		defer ssbd.Unlock()

		if !ssbd.initialized {
			ssbd.init(sei, beginTime, intervalSeconds, historySize)
		}

		var ssElement *stmtSummaryByDigestElement
		isElementNew := true
		if ssbd.history.Len() > 0 {
			lastElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			if lastElement.beginTime >= beginTime {
				ssElement = lastElement
				isElementNew = false
			} else {
				// The last elements expires to the history.
				lastElement.onExpire(intervalSeconds)
			}
		}
		if isElementNew {
			// If the element is new created, `ssElement.add(sei)` should be done inside the dagger of `ssbd`.
			ssElement = newStmtSummaryByDigestElement(sei, beginTime, intervalSeconds)
			ssbd.history.PushBack(ssElement)
		}

		// `historySize` might be modified anytime, so check expiration every time.
		// Even if history is set to 0, current summary is still needed.
		for ssbd.history.Len() > historySize && ssbd.history.Len() > 1 {
			ssbd.history.Remove(ssbd.history.Front())
		}

		return ssElement, isElementNew
	}()

	// Lock a single entry, not the whole `ssbd`.
	if !isElementNew {
		ssElement.add(sei, intervalSeconds)
	}
}

func (ssbd *stmtSummaryByDigest) toCurrentCauset(beginTimeForCurInterval int64, user *auth.UserIdentity, isSuper bool) []types.Causet {
	var ssElement *stmtSummaryByDigestElement

	ssbd.Lock()
	if ssbd.initialized && ssbd.history.Len() > 0 {
		ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	}
	ssbd.Unlock()

	// `ssElement` is lazy expired, so expired elements could also be read.
	// `beginTime` won't change since `ssElement` is created, so locking is not needed here.
	isAuthed := true
	if user != nil && !isSuper {
		_, isAuthed = ssElement.authUsers[user.Username]
	}
	if ssElement == nil || ssElement.beginTime < beginTimeForCurInterval || !isAuthed {
		return nil
	}
	return ssElement.toCauset(ssbd)
}

func (ssbd *stmtSummaryByDigest) toHistoryCauset(historySize int, user *auth.UserIdentity, isSuper bool) [][]types.Causet {
	// DefCauslect all history summaries to an array.
	ssElements := ssbd.defCauslectHistorySummaries(historySize)

	rows := make([][]types.Causet, 0, len(ssElements))
	for _, ssElement := range ssElements {
		isAuthed := true
		if user != nil && !isSuper {
			_, isAuthed = ssElement.authUsers[user.Username]
		}
		if isAuthed {
			rows = append(rows, ssElement.toCauset(ssbd))
		}
	}
	return rows
}

// defCauslectHistorySummaries puts at most `historySize` summaries to an array.
func (ssbd *stmtSummaryByDigest) defCauslectHistorySummaries(historySize int) []*stmtSummaryByDigestElement {
	ssbd.Lock()
	defer ssbd.Unlock()

	if !ssbd.initialized {
		return nil
	}
	ssElements := make([]*stmtSummaryByDigestElement, 0, ssbd.history.Len())
	for listElement := ssbd.history.Front(); listElement != nil && len(ssElements) < historySize; listElement = listElement.Next() {
		ssElement := listElement.Value.(*stmtSummaryByDigestElement)
		ssElements = append(ssElements, ssElement)
	}
	return ssElements
}

func newStmtSummaryByDigestElement(sei *StmtInterDircInfo, beginTime int64, intervalSeconds int64) *stmtSummaryByDigestElement {
	// sampleALLEGROSQL / authUsers(sampleUser) / sampleCauset / prevALLEGROSQL / indexNames causetstore the values shown at the first time,
	// because it compacts performance to uFIDelate every time.
	ssElement := &stmtSummaryByDigestElement{
		beginTime:        beginTime,
		sampleALLEGROSQL: formatALLEGROSQL(sei.OriginalALLEGROSQL),
		// PrevALLEGROSQL is already truncated to cfg.Log.QueryLogMaxLen.
		prevALLEGROSQL: sei.PrevALLEGROSQL,
		// sampleCauset needs to be decoded so it can't be truncated.
		sampleCauset:  sei.CausetGenerator(),
		indexNames:    sei.StmtCtx.IndexNames,
		minLatency:    sei.TotalLatency,
		firstSeen:     sei.StartTime,
		lastSeen:      sei.StartTime,
		backoffTypes:  make(map[fmt.Stringer]int),
		authUsers:     make(map[string]struct{}),
		planInCache:   false,
		planCacheHits: 0,
	}
	ssElement.add(sei, intervalSeconds)
	return ssElement
}

// onExpire is called when this element expires to history.
func (ssElement *stmtSummaryByDigestElement) onExpire(intervalSeconds int64) {
	ssElement.Lock()
	defer ssElement.Unlock()

	// refreshInterval may change anytime, so we need to uFIDelate endTime.
	if ssElement.beginTime+intervalSeconds > ssElement.endTime {
		// // If interval changes to a bigger value, uFIDelate endTime to beginTime + interval.
		ssElement.endTime = ssElement.beginTime + intervalSeconds
	} else if ssElement.beginTime+intervalSeconds < ssElement.endTime {
		now := time.Now().Unix()
		// If interval changes to a smaller value and now > beginTime + interval, uFIDelate endTime to current time.
		if now > ssElement.beginTime+intervalSeconds {
			ssElement.endTime = now
		}
	}
}

func (ssElement *stmtSummaryByDigestElement) add(sei *StmtInterDircInfo, intervalSeconds int64) {
	ssElement.Lock()
	defer ssElement.Unlock()

	// add user to auth users set
	if len(sei.User) > 0 {
		ssElement.authUsers[sei.User] = struct{}{}
	}

	// refreshInterval may change anytime, uFIDelate endTime ASAP.
	ssElement.endTime = ssElement.beginTime + intervalSeconds
	ssElement.execCount++
	if !sei.Succeed {
		ssElement.sumErrors += 1
	}
	ssElement.sumWarnings += int(sei.StmtCtx.WarningCount())

	// latency
	ssElement.sumLatency += sei.TotalLatency
	if sei.TotalLatency > ssElement.maxLatency {
		ssElement.maxLatency = sei.TotalLatency
	}
	if sei.TotalLatency < ssElement.minLatency {
		ssElement.minLatency = sei.TotalLatency
	}
	ssElement.sumParseLatency += sei.ParseLatency
	if sei.ParseLatency > ssElement.maxParseLatency {
		ssElement.maxParseLatency = sei.ParseLatency
	}
	ssElement.sumCompileLatency += sei.CompileLatency
	if sei.CompileLatency > ssElement.maxCompileLatency {
		ssElement.maxCompileLatency = sei.CompileLatency
	}

	// interlock
	numCausetTasks := int64(sei.CausetTasks.NumCausetTasks)
	ssElement.sumNumCausetTasks += numCausetTasks
	if sei.CausetTasks.MaxProcessTime > ssElement.maxCopProcessTime {
		ssElement.maxCopProcessTime = sei.CausetTasks.MaxProcessTime
		ssElement.maxCopProcessAddress = sei.CausetTasks.MaxProcessAddress
	}
	if sei.CausetTasks.MaxWaitTime > ssElement.maxCopWaitTime {
		ssElement.maxCopWaitTime = sei.CausetTasks.MaxWaitTime
		ssElement.maxCopWaitAddress = sei.CausetTasks.MaxWaitAddress
	}

	// EinsteinDB
	ssElement.sumProcessTime += sei.InterDircDetail.ProcessTime
	if sei.InterDircDetail.ProcessTime > ssElement.maxProcessTime {
		ssElement.maxProcessTime = sei.InterDircDetail.ProcessTime
	}
	ssElement.sumWaitTime += sei.InterDircDetail.WaitTime
	if sei.InterDircDetail.WaitTime > ssElement.maxWaitTime {
		ssElement.maxWaitTime = sei.InterDircDetail.WaitTime
	}
	ssElement.sumBackoffTime += sei.InterDircDetail.BackoffTime
	if sei.InterDircDetail.BackoffTime > ssElement.maxBackoffTime {
		ssElement.maxBackoffTime = sei.InterDircDetail.BackoffTime
	}
	ssElement.sumTotalKeys += sei.InterDircDetail.TotalKeys
	if sei.InterDircDetail.TotalKeys > ssElement.maxTotalKeys {
		ssElement.maxTotalKeys = sei.InterDircDetail.TotalKeys
	}
	ssElement.sumProcessedKeys += sei.InterDircDetail.ProcessedKeys
	if sei.InterDircDetail.ProcessedKeys > ssElement.maxProcessedKeys {
		ssElement.maxProcessedKeys = sei.InterDircDetail.ProcessedKeys
	}

	// txn
	commitDetails := sei.InterDircDetail.CommitDetail
	if commitDetails != nil {
		ssElement.commitCount++
		ssElement.sumPrewriteTime += commitDetails.PrewriteTime
		if commitDetails.PrewriteTime > ssElement.maxPrewriteTime {
			ssElement.maxPrewriteTime = commitDetails.PrewriteTime
		}
		ssElement.sumCommitTime += commitDetails.CommitTime
		if commitDetails.CommitTime > ssElement.maxCommitTime {
			ssElement.maxCommitTime = commitDetails.CommitTime
		}
		ssElement.sumGetCommitTsTime += commitDetails.GetCommitTsTime
		if commitDetails.GetCommitTsTime > ssElement.maxGetCommitTsTime {
			ssElement.maxGetCommitTsTime = commitDetails.GetCommitTsTime
		}
		commitBackoffTime := atomic.LoadInt64(&commitDetails.CommitBackoffTime)
		ssElement.sumCommitBackoffTime += commitBackoffTime
		if commitBackoffTime > ssElement.maxCommitBackoffTime {
			ssElement.maxCommitBackoffTime = commitBackoffTime
		}
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
		ssElement.sumResolveLockTime += resolveLockTime
		if resolveLockTime > ssElement.maxResolveLockTime {
			ssElement.maxResolveLockTime = resolveLockTime
		}
		ssElement.sumLocalLatchTime += commitDetails.LocalLatchTime
		if commitDetails.LocalLatchTime > ssElement.maxLocalLatchTime {
			ssElement.maxLocalLatchTime = commitDetails.LocalLatchTime
		}
		ssElement.sumWriteKeys += int64(commitDetails.WriteKeys)
		if commitDetails.WriteKeys > ssElement.maxWriteKeys {
			ssElement.maxWriteKeys = commitDetails.WriteKeys
		}
		ssElement.sumWriteSize += int64(commitDetails.WriteSize)
		if commitDetails.WriteSize > ssElement.maxWriteSize {
			ssElement.maxWriteSize = commitDetails.WriteSize
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		ssElement.sumPrewriteRegionNum += int64(prewriteRegionNum)
		if prewriteRegionNum > ssElement.maxPrewriteRegionNum {
			ssElement.maxPrewriteRegionNum = prewriteRegionNum
		}
		ssElement.sumTxnRetry += int64(commitDetails.TxnRetry)
		if commitDetails.TxnRetry > ssElement.maxTxnRetry {
			ssElement.maxTxnRetry = commitDetails.TxnRetry
		}
		commitDetails.Mu.Lock()
		ssElement.sumBackoffTimes += int64(len(commitDetails.Mu.BackoffTypes))
		for _, backoffType := range commitDetails.Mu.BackoffTypes {
			ssElement.backoffTypes[backoffType] += 1
		}
		commitDetails.Mu.Unlock()
	}

	//plan cache
	if sei.CausetInCache {
		ssElement.planInCache = true
		ssElement.planCacheHits += 1
	} else {
		ssElement.planInCache = false
	}

	// other
	ssElement.sumAffectedRows += sei.StmtCtx.AffectedRows()
	ssElement.sumMem += sei.MemMax
	if sei.MemMax > ssElement.maxMem {
		ssElement.maxMem = sei.MemMax
	}
	ssElement.sumDisk += sei.DiskMax
	if sei.DiskMax > ssElement.maxDisk {
		ssElement.maxDisk = sei.DiskMax
	}
	if sei.StartTime.Before(ssElement.firstSeen) {
		ssElement.firstSeen = sei.StartTime
	}
	if ssElement.lastSeen.Before(sei.StartTime) {
		ssElement.lastSeen = sei.StartTime
	}
	if sei.InterDircRetryCount > 0 {
		ssElement.execRetryCount += sei.InterDircRetryCount
		ssElement.execRetryTime += sei.InterDircRetryTime
	}
}

func (ssElement *stmtSummaryByDigestElement) toCauset(ssbd *stmtSummaryByDigest) []types.Causet {
	ssElement.Lock()
	defer ssElement.Unlock()

	plan, err := plancodec.DecodeCauset(ssElement.sampleCauset)
	if err != nil {
		logutil.BgLogger().Error("decode plan in memex summary failed", zap.String("plan", ssElement.sampleCauset), zap.Error(err))
		plan = ""
	}

	sampleUser := ""
	for key := range ssElement.authUsers {
		sampleUser = key
		break
	}

	// Actually, there's a small chance that endTime is out of date, but it's hard to keep it up to date all the time.
	return types.MakeCausets(
		types.NewTime(types.FromGoTime(time.Unix(ssElement.beginTime, 0)), allegrosql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(time.Unix(ssElement.endTime, 0)), allegrosql.TypeTimestamp, 0),
		ssbd.stmtType,
		ssbd.schemaName,
		ssbd.digest,
		ssbd.normalizedALLEGROSQL,
		convertEmptyToNil(ssbd.blockNames),
		convertEmptyToNil(strings.Join(ssElement.indexNames, ",")),
		convertEmptyToNil(sampleUser),
		ssElement.execCount,
		ssElement.sumErrors,
		ssElement.sumWarnings,
		int64(ssElement.sumLatency),
		int64(ssElement.maxLatency),
		int64(ssElement.minLatency),
		avgInt(int64(ssElement.sumLatency), ssElement.execCount),
		avgInt(int64(ssElement.sumParseLatency), ssElement.execCount),
		int64(ssElement.maxParseLatency),
		avgInt(int64(ssElement.sumCompileLatency), ssElement.execCount),
		int64(ssElement.maxCompileLatency),
		ssElement.sumNumCausetTasks,
		int64(ssElement.maxCopProcessTime),
		convertEmptyToNil(ssElement.maxCopProcessAddress),
		int64(ssElement.maxCopWaitTime),
		convertEmptyToNil(ssElement.maxCopWaitAddress),
		avgInt(int64(ssElement.sumProcessTime), ssElement.execCount),
		int64(ssElement.maxProcessTime),
		avgInt(int64(ssElement.sumWaitTime), ssElement.execCount),
		int64(ssElement.maxWaitTime),
		avgInt(int64(ssElement.sumBackoffTime), ssElement.execCount),
		int64(ssElement.maxBackoffTime),
		avgInt(ssElement.sumTotalKeys, ssElement.execCount),
		ssElement.maxTotalKeys,
		avgInt(ssElement.sumProcessedKeys, ssElement.execCount),
		ssElement.maxProcessedKeys,
		avgInt(int64(ssElement.sumPrewriteTime), ssElement.commitCount),
		int64(ssElement.maxPrewriteTime),
		avgInt(int64(ssElement.sumCommitTime), ssElement.commitCount),
		int64(ssElement.maxCommitTime),
		avgInt(int64(ssElement.sumGetCommitTsTime), ssElement.commitCount),
		int64(ssElement.maxGetCommitTsTime),
		avgInt(ssElement.sumCommitBackoffTime, ssElement.commitCount),
		ssElement.maxCommitBackoffTime,
		avgInt(ssElement.sumResolveLockTime, ssElement.commitCount),
		ssElement.maxResolveLockTime,
		avgInt(int64(ssElement.sumLocalLatchTime), ssElement.commitCount),
		int64(ssElement.maxLocalLatchTime),
		avgFloat(ssElement.sumWriteKeys, ssElement.commitCount),
		ssElement.maxWriteKeys,
		avgFloat(ssElement.sumWriteSize, ssElement.commitCount),
		ssElement.maxWriteSize,
		avgFloat(ssElement.sumPrewriteRegionNum, ssElement.commitCount),
		int(ssElement.maxPrewriteRegionNum),
		avgFloat(ssElement.sumTxnRetry, ssElement.commitCount),
		ssElement.maxTxnRetry,
		int(ssElement.execRetryCount),
		int64(ssElement.execRetryTime),
		ssElement.sumBackoffTimes,
		formatBackoffTypes(ssElement.backoffTypes),
		avgInt(ssElement.sumMem, ssElement.execCount),
		ssElement.maxMem,
		avgInt(ssElement.sumDisk, ssElement.execCount),
		ssElement.maxDisk,
		avgFloat(int64(ssElement.sumAffectedRows), ssElement.execCount),
		types.NewTime(types.FromGoTime(ssElement.firstSeen), allegrosql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(ssElement.lastSeen), allegrosql.TypeTimestamp, 0),
		ssElement.planInCache,
		ssElement.planCacheHits,
		ssElement.sampleALLEGROSQL,
		ssElement.prevALLEGROSQL,
		ssbd.planDigest,
		plan,
	)
}

// Truncate ALLEGROALLEGROSQL to maxALLEGROSQLLength.
func formatALLEGROSQL(allegrosql string) string {
	maxALLEGROSQLLength := StmtSummaryByDigestMap.maxALLEGROSQLLength()
	length := len(allegrosql)
	if length > maxALLEGROSQLLength {
		allegrosql = fmt.Sprintf("%.*s(len:%d)", maxALLEGROSQLLength, allegrosql, length)
	}
	return allegrosql
}

// Format the backoffType map to a string or nil.
func formatBackoffTypes(backoffMap map[fmt.Stringer]int) interface{} {
	type backoffStat struct {
		backoffType fmt.Stringer
		count       int
	}

	size := len(backoffMap)
	if size == 0 {
		return nil
	}

	backoffArray := make([]backoffStat, 0, len(backoffMap))
	for backoffType, count := range backoffMap {
		backoffArray = append(backoffArray, backoffStat{backoffType, count})
	}
	sort.Slice(backoffArray, func(i, j int) bool {
		return backoffArray[i].count > backoffArray[j].count
	})

	var buffer bytes.Buffer
	for index, stat := range backoffArray {
		if _, err := fmt.Fprintf(&buffer, "%v:%d", stat.backoffType, stat.count); err != nil {
			return "FORMAT ERROR"
		}
		if index < len(backoffArray)-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
}

func avgInt(sum int64, count int64) int64 {
	if count > 0 {
		return sum / count
	}
	return 0
}

func avgFloat(sum int64, count int64) float64 {
	if count > 0 {
		return float64(sum) / float64(count)
	}
	return 0
}

func convertEmptyToNil(str string) interface{} {
	if str == "" {
		return nil
	}
	return str
}
