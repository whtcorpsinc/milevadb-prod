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

package stmtctx

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"go.uber.org/zap"
)

const (
	// WarnLevelError represents level "Error" for 'SHOW WARNINGS' syntax.
	WarnLevelError = "Error"
	// WarnLevelWarning represents level "Warning" for 'SHOW WARNINGS' syntax.
	WarnLevelWarning = "Warning"
	// WarnLevelNote represents level "Note" for 'SHOW WARNINGS' syntax.
	WarnLevelNote = "Note"
)

var taskIDAlloc uint64

// AllocateTaskID allocates a new unique ID for a statement execution
func AllocateTaskID() uint64 {
	return atomic.AddUint64(&taskIDAlloc, 1)
}

// ALLEGROSQLWarn relates a allegrosql warning and it's level.
type ALLEGROSQLWarn struct {
	Level string
	Err   error
}

// StatementContext contains variables for a statement.
// It should be reset before executing a statement.
type StatementContext struct {
	// Set the following variables before execution
	StmtHints

	// IsDBSJobInQueue is used to mark whether the DBS job is put into the queue.
	// If IsDBSJobInQueue is true, it means the DBS job is in the queue of storage, and it can be handled by the DBS worker.
	IsDBSJobInQueue        bool
	InInsertStmt           bool
	InUFIDelateStmt           bool
	InDeleteStmt           bool
	InSelectStmt           bool
	InLoadDataStmt         bool
	InExplainStmt          bool
	IgnoreTruncate         bool
	IgnoreZeroInDate       bool
	DupKeyAsWarning        bool
	BadNullAsWarning       bool
	DividedByZeroAsWarning bool
	TruncateAsWarning      bool
	OverflowAsWarning      bool
	InShowWarning          bool
	UseCache               bool
	BatchCheck             bool
	InNullRejectCheck      bool
	AllowInvalidDate       bool

	// mu struct holds variables that change during execution.
	mu struct {
		sync.Mutex

		affectedRows uint64
		foundRows    uint64

		/*
			following variables are ported from 'COPY_INFO' struct of MyALLEGROSQL server source,
			they are used to count rows for INSERT/REPLACE/UFIDelATE queries:
			  If a event is inserted then the copied variable is incremented.
			  If a event is uFIDelated by the INSERT ... ON DUPLICATE KEY UFIDelATE and the
			     new data differs from the old one then the copied and the uFIDelated
			     variables are incremented.
			  The touched variable is incremented if a event was touched by the uFIDelate part
			     of the INSERT ... ON DUPLICATE KEY UFIDelATE no matter whether the event
			     was actually changed or not.

			see https://github.com/allegrosql/allegrosql-server/blob/d2029238d6d9f648077664e4cdd611e231a6dc14/allegrosql/sql_data_change.h#L60 for more details
		*/
		records uint64
		uFIDelated uint64
		copied  uint64
		touched uint64

		message           string
		warnings          []ALLEGROSQLWarn
		errorCount        uint16
		histogramsNotLoad bool
		execDetails       execdetails.ExecDetails
		allExecDetails    []*execdetails.ExecDetails
	}
	// PrevAffectedRows is the affected-rows value(DBS is 0, DML is the number of affected rows).
	PrevAffectedRows int64
	// PrevLastInsertID is the last insert ID of previous statement.
	PrevLastInsertID uint64
	// LastInsertID is the auto-generated ID in the current statement.
	LastInsertID uint64
	// InsertID is the given insert ID of an auto_increment defCausumn.
	InsertID uint64

	BaseRowID int64
	MaxRowID  int64

	// Copied from StochastikVars.TimeZone.
	TimeZone         *time.Location
	Priority         allegrosql.PriorityEnum
	NotFillCache     bool
	MemTracker       *memory.Tracker
	DiskTracker      *disk.Tracker
	RuntimeStatsDefCausl *execdetails.RuntimeStatsDefCausl
	BlockIDs         []int64
	IndexNames       []string
	nowTs            time.Time // use this variable for now/current_timestamp calculation/cache for one stmt
	stmtTimeCached   bool
	StmtType         string
	OriginalALLEGROSQL      string
	digestMemo       struct {
		sync.Once
		normalized string
		digest     string
	}
	// planNormalized use for cache the normalized plan, avoid duplicate builds.
	planNormalized        string
	planDigest            string
	Blocks                []BlockEntry
	PointExec             bool  // for point uFIDelate cached execution, Constant expression need to set "paramMarker"
	lockWaitStartTime     int64 // LockWaitStartTime stores the pessimistic lock wait start time
	PessimisticLockWaited int32
	LockKeysDuration      int64
	LockKeysCount         int32
	TblInfo2UnionScan     map[*perceptron.BlockInfo]bool
	TaskID                uint64 // unique ID for an execution of a statement
	TaskMapBakTS          uint64 // counter for
}

// StmtHints are StochastikVars related allegrosql hints.
type StmtHints struct {
	// Hint Information
	MemQuotaQuery           int64
	ApplyCacheCapacity      int64
	MaxExecutionTime        uint64
	ReplicaRead             byte
	AllowInSubqToJoinAnPosetDagg bool
	NoIndexMergeHint        bool
	// EnableCascadesPlanner is use cascades planner for a single query only.
	EnableCascadesPlanner bool
	// ForceNthPlan indicates the PlanCounterTp number for finding physical plan.
	// -1 for disable.
	ForceNthPlan int64

	// Hint flags
	HasAllowInSubqToJoinAnPosetDaggHint bool
	HasMemQuotaHint                bool
	HasReplicaReadHint             bool
	HasMaxExecutionTime            bool
	HasEnableCascadesPlannerHint   bool
}

// TaskMapNeedBackUp indicates that whether we need to back up taskMap during physical optimizing.
func (sh *StmtHints) TaskMapNeedBackUp() bool {
	return sh.ForceNthPlan != -1
}

// GetNowTsCached getter for nowTs, if not set get now time and cache it
func (sc *StatementContext) GetNowTsCached() time.Time {
	if !sc.stmtTimeCached {
		now := time.Now()
		sc.nowTs = now
		sc.stmtTimeCached = true
	}
	return sc.nowTs
}

// ResetNowTs resetter for nowTs, clear cached time flag
func (sc *StatementContext) ResetNowTs() {
	sc.stmtTimeCached = false
}

// ALLEGROSQLDigest gets normalized and digest for provided allegrosql.
// it will cache result after first calling.
func (sc *StatementContext) ALLEGROSQLDigest() (normalized, sqlDigest string) {
	sc.digestMemo.Do(func() {
		sc.digestMemo.normalized, sc.digestMemo.digest = BerolinaSQL.NormalizeDigest(sc.OriginalALLEGROSQL)
	})
	return sc.digestMemo.normalized, sc.digestMemo.digest
}

// InitALLEGROSQLDigest sets the normalized and digest for allegrosql.
func (sc *StatementContext) InitALLEGROSQLDigest(normalized, digest string) {
	sc.digestMemo.Do(func() {
		sc.digestMemo.normalized, sc.digestMemo.digest = normalized, digest
	})
}

// GetPlanDigest gets the normalized plan and plan digest.
func (sc *StatementContext) GetPlanDigest() (normalized, planDigest string) {
	return sc.planNormalized, sc.planDigest
}

// SetPlanDigest sets the normalized plan and plan digest.
func (sc *StatementContext) SetPlanDigest(normalized, planDigest string) {
	sc.planNormalized, sc.planDigest = normalized, planDigest
}

// BlockEntry presents block in EDB.
type BlockEntry struct {
	EDB    string
	Block string
}

// AddAffectedRows adds affected rows.
func (sc *StatementContext) AddAffectedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.affectedRows += rows
	sc.mu.Unlock()
}

// AffectedRows gets affected rows.
func (sc *StatementContext) AffectedRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.affectedRows
	sc.mu.Unlock()
	return rows
}

// FoundRows gets found rows.
func (sc *StatementContext) FoundRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.foundRows
	sc.mu.Unlock()
	return rows
}

// AddFoundRows adds found rows.
func (sc *StatementContext) AddFoundRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.foundRows += rows
	sc.mu.Unlock()
}

// RecordRows is used to generate info message
func (sc *StatementContext) RecordRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.records
	sc.mu.Unlock()
	return rows
}

// AddRecordRows adds record rows.
func (sc *StatementContext) AddRecordRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.records += rows
	sc.mu.Unlock()
}

// UFIDelatedRows is used to generate info message
func (sc *StatementContext) UFIDelatedRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.uFIDelated
	sc.mu.Unlock()
	return rows
}

// AddUFIDelatedRows adds uFIDelated rows.
func (sc *StatementContext) AddUFIDelatedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.uFIDelated += rows
	sc.mu.Unlock()
}

// CopiedRows is used to generate info message
func (sc *StatementContext) CopiedRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.copied
	sc.mu.Unlock()
	return rows
}

// AddCopiedRows adds copied rows.
func (sc *StatementContext) AddCopiedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.copied += rows
	sc.mu.Unlock()
}

// TouchedRows is used to generate info message
func (sc *StatementContext) TouchedRows() uint64 {
	sc.mu.Lock()
	rows := sc.mu.touched
	sc.mu.Unlock()
	return rows
}

// AddTouchedRows adds touched rows.
func (sc *StatementContext) AddTouchedRows(rows uint64) {
	sc.mu.Lock()
	sc.mu.touched += rows
	sc.mu.Unlock()
}

// GetMessage returns the extra message of the last executed command, if there is no message, it returns empty string
func (sc *StatementContext) GetMessage() string {
	sc.mu.Lock()
	msg := sc.mu.message
	sc.mu.Unlock()
	return msg
}

// SetMessage sets the info message generated by some commands
func (sc *StatementContext) SetMessage(msg string) {
	sc.mu.Lock()
	sc.mu.message = msg
	sc.mu.Unlock()
}

// GetWarnings gets warnings.
func (sc *StatementContext) GetWarnings() []ALLEGROSQLWarn {
	sc.mu.Lock()
	warns := make([]ALLEGROSQLWarn, len(sc.mu.warnings))
	copy(warns, sc.mu.warnings)
	sc.mu.Unlock()
	return warns
}

// TruncateWarnings truncates wanrings begin from start and returns the truncated warnings.
func (sc *StatementContext) TruncateWarnings(start int) []ALLEGROSQLWarn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sz := len(sc.mu.warnings) - start
	if sz <= 0 {
		return nil
	}
	ret := make([]ALLEGROSQLWarn, sz)
	copy(ret, sc.mu.warnings[start:])
	sc.mu.warnings = sc.mu.warnings[:start]
	return ret
}

// WarningCount gets warning count.
func (sc *StatementContext) WarningCount() uint16 {
	if sc.InShowWarning {
		return 0
	}
	sc.mu.Lock()
	wc := uint16(len(sc.mu.warnings))
	sc.mu.Unlock()
	return wc
}

// NumErrorWarnings gets warning and error count.
func (sc *StatementContext) NumErrorWarnings() (ec uint16, wc int) {
	sc.mu.Lock()
	ec = sc.mu.errorCount
	wc = len(sc.mu.warnings)
	sc.mu.Unlock()
	return
}

// SetWarnings sets warnings.
func (sc *StatementContext) SetWarnings(warns []ALLEGROSQLWarn) {
	sc.mu.Lock()
	sc.mu.warnings = warns
	for _, w := range warns {
		if w.Level == WarnLevelError {
			sc.mu.errorCount++
		}
	}
	sc.mu.Unlock()
}

// AppendWarning appends a warning with level 'Warning'.
func (sc *StatementContext) AppendWarning(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, ALLEGROSQLWarn{WarnLevelWarning, warn})
	}
	sc.mu.Unlock()
}

// AppendWarnings appends some warnings.
func (sc *StatementContext) AppendWarnings(warns []ALLEGROSQLWarn) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, warns...)
	}
	sc.mu.Unlock()
}

// AppendNote appends a warning with level 'Note'.
func (sc *StatementContext) AppendNote(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, ALLEGROSQLWarn{WarnLevelNote, warn})
	}
	sc.mu.Unlock()
}

// AppendError appends a warning with level 'Error'.
func (sc *StatementContext) AppendError(warn error) {
	sc.mu.Lock()
	if len(sc.mu.warnings) < math.MaxUint16 {
		sc.mu.warnings = append(sc.mu.warnings, ALLEGROSQLWarn{WarnLevelError, warn})
		sc.mu.errorCount++
	}
	sc.mu.Unlock()
}

// SetHistogramsNotLoad sets histogramsNotLoad.
func (sc *StatementContext) SetHistogramsNotLoad() {
	sc.mu.Lock()
	sc.mu.histogramsNotLoad = true
	sc.mu.Unlock()
}

// HandleTruncate ignores or returns the error based on the StatementContext state.
func (sc *StatementContext) HandleTruncate(err error) error {
	// TODO: At present we have not checked whether the error can be ignored or treated as warning.
	// We will do that later, and then append WarnDataTruncated instead of the error itself.
	if err == nil {
		return nil
	}
	if sc.IgnoreTruncate {
		return nil
	}
	if sc.TruncateAsWarning {
		sc.AppendWarning(err)
		return nil
	}
	return err
}

// HandleOverflow treats ErrOverflow as warnings or returns the error based on the StmtCtx.OverflowAsWarning state.
func (sc *StatementContext) HandleOverflow(err error, warnErr error) error {
	if err == nil {
		return nil
	}

	if sc.OverflowAsWarning {
		sc.AppendWarning(warnErr)
		return nil
	}
	return err
}

// ResetForRetry resets the changed states during execution.
func (sc *StatementContext) ResetForRetry() {
	sc.mu.Lock()
	sc.mu.affectedRows = 0
	sc.mu.foundRows = 0
	sc.mu.records = 0
	sc.mu.uFIDelated = 0
	sc.mu.copied = 0
	sc.mu.touched = 0
	sc.mu.message = ""
	sc.mu.errorCount = 0
	sc.mu.warnings = nil
	sc.mu.execDetails = execdetails.ExecDetails{}
	sc.mu.allExecDetails = make([]*execdetails.ExecDetails, 0, 4)
	sc.mu.Unlock()
	sc.MaxRowID = 0
	sc.BaseRowID = 0
	sc.BlockIDs = sc.BlockIDs[:0]
	sc.IndexNames = sc.IndexNames[:0]
	sc.TaskID = AllocateTaskID()
}

// MergeExecDetails merges a single region execution details into self, used to print
// the information in slow query log.
func (sc *StatementContext) MergeExecDetails(details *execdetails.ExecDetails, commitDetails *execdetails.CommitDetails) {
	sc.mu.Lock()
	if details != nil {
		sc.mu.execDetails.CopTime += details.CopTime
		sc.mu.execDetails.ProcessTime += details.ProcessTime
		sc.mu.execDetails.WaitTime += details.WaitTime
		sc.mu.execDetails.BackoffTime += details.BackoffTime
		sc.mu.execDetails.RequestCount++
		sc.mu.execDetails.TotalKeys += details.TotalKeys
		sc.mu.execDetails.ProcessedKeys += details.ProcessedKeys
		sc.mu.allExecDetails = append(sc.mu.allExecDetails, details)
	}
	sc.mu.execDetails.CommitDetail = commitDetails
	sc.mu.Unlock()
}

// MergeLockKeysExecDetails merges lock keys execution details into self.
func (sc *StatementContext) MergeLockKeysExecDetails(lockKeys *execdetails.LockKeysDetails) {
	sc.mu.Lock()
	if sc.mu.execDetails.LockKeysDetail == nil {
		sc.mu.execDetails.LockKeysDetail = lockKeys
	} else {
		sc.mu.execDetails.LockKeysDetail.Merge(lockKeys)
	}
	sc.mu.Unlock()
}

// GetExecDetails gets the execution details for the statement.
func (sc *StatementContext) GetExecDetails() execdetails.ExecDetails {
	var details execdetails.ExecDetails
	sc.mu.Lock()
	details = sc.mu.execDetails
	details.LockKeysDuration = time.Duration(atomic.LoadInt64(&sc.LockKeysDuration))
	sc.mu.Unlock()
	return details
}

// ShouldClipToZero indicates whether values less than 0 should be clipped to 0 for unsigned integer types.
// This is the case for `insert`, `uFIDelate`, `alter block` and `load data infile` statements, when not in strict ALLEGROALLEGROSQL mode.
// see https://dev.allegrosql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
func (sc *StatementContext) ShouldClipToZero() bool {
	// TODO: Currently altering defCausumn of integer to unsigned integer is not supported.
	// If it is supported one day, that case should be added here.
	return sc.InInsertStmt || sc.InLoadDataStmt || sc.InUFIDelateStmt
}

// ShouldIgnoreOverflowError indicates whether we should ignore the error when type conversion overflows,
// so we can leave it for further processing like clipping values less than 0 to 0 for unsigned integer types.
func (sc *StatementContext) ShouldIgnoreOverflowError() bool {
	if (sc.InInsertStmt && sc.TruncateAsWarning) || sc.InLoadDataStmt {
		return true
	}
	return false
}

// PushDownFlags converts StatementContext to fidelpb.SelectRequest.Flags.
func (sc *StatementContext) PushDownFlags() uint64 {
	var flags uint64
	if sc.InInsertStmt {
		flags |= perceptron.FlagInInsertStmt
	} else if sc.InUFIDelateStmt || sc.InDeleteStmt {
		flags |= perceptron.FlagInUFIDelateOrDeleteStmt
	} else if sc.InSelectStmt {
		flags |= perceptron.FlagInSelectStmt
	}
	if sc.IgnoreTruncate {
		flags |= perceptron.FlagIgnoreTruncate
	} else if sc.TruncateAsWarning {
		flags |= perceptron.FlagTruncateAsWarning
	}
	if sc.OverflowAsWarning {
		flags |= perceptron.FlagOverflowAsWarning
	}
	if sc.IgnoreZeroInDate {
		flags |= perceptron.FlagIgnoreZeroInDate
	}
	if sc.DividedByZeroAsWarning {
		flags |= perceptron.FlagDividedByZeroAsWarning
	}
	if sc.InLoadDataStmt {
		flags |= perceptron.FlagInLoadDataStmt
	}
	return flags
}

// CausetTasksDetails returns some useful information of cop-tasks during execution.
func (sc *StatementContext) CausetTasksDetails() *CausetTasksDetails {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	n := len(sc.mu.allExecDetails)
	d := &CausetTasksDetails{
		NumCausetTasks:       n,
		MaxBackoffTime:    make(map[string]time.Duration),
		AvgBackoffTime:    make(map[string]time.Duration),
		P90BackoffTime:    make(map[string]time.Duration),
		TotBackoffTime:    make(map[string]time.Duration),
		TotBackoffTimes:   make(map[string]int),
		MaxBackoffAddress: make(map[string]string),
	}
	if n == 0 {
		return d
	}
	d.AvgProcessTime = sc.mu.execDetails.ProcessTime / time.Duration(n)
	d.AvgWaitTime = sc.mu.execDetails.WaitTime / time.Duration(n)

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		return sc.mu.allExecDetails[i].ProcessTime < sc.mu.allExecDetails[j].ProcessTime
	})
	d.P90ProcessTime = sc.mu.allExecDetails[n*9/10].ProcessTime
	d.MaxProcessTime = sc.mu.allExecDetails[n-1].ProcessTime
	d.MaxProcessAddress = sc.mu.allExecDetails[n-1].CalleeAddress

	sort.Slice(sc.mu.allExecDetails, func(i, j int) bool {
		return sc.mu.allExecDetails[i].WaitTime < sc.mu.allExecDetails[j].WaitTime
	})
	d.P90WaitTime = sc.mu.allExecDetails[n*9/10].WaitTime
	d.MaxWaitTime = sc.mu.allExecDetails[n-1].WaitTime
	d.MaxWaitAddress = sc.mu.allExecDetails[n-1].CalleeAddress

	// calculate backoff details
	type backoffItem struct {
		callee    string
		sleepTime time.Duration
		times     int
	}
	backoffInfo := make(map[string][]backoffItem)
	for _, ed := range sc.mu.allExecDetails {
		for backoff := range ed.BackoffTimes {
			backoffInfo[backoff] = append(backoffInfo[backoff], backoffItem{
				callee:    ed.CalleeAddress,
				sleepTime: ed.BackoffSleep[backoff],
				times:     ed.BackoffTimes[backoff],
			})
		}
	}
	for backoff, items := range backoffInfo {
		if len(items) == 0 {
			continue
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].sleepTime < items[j].sleepTime
		})
		n := len(items)
		d.MaxBackoffAddress[backoff] = items[n-1].callee
		d.MaxBackoffTime[backoff] = items[n-1].sleepTime
		d.P90BackoffTime[backoff] = items[n*9/10].sleepTime

		var totalTime time.Duration
		totalTimes := 0
		for _, it := range items {
			totalTime += it.sleepTime
			totalTimes += it.times
		}
		d.AvgBackoffTime[backoff] = totalTime / time.Duration(n)
		d.TotBackoffTime[backoff] = totalTime
		d.TotBackoffTimes[backoff] = totalTimes
	}
	return d
}

// SetFlagsFromPBFlag set the flag of StatementContext from a `fidelpb.SelectRequest.Flags`.
func (sc *StatementContext) SetFlagsFromPBFlag(flags uint64) {
	sc.IgnoreTruncate = (flags & perceptron.FlagIgnoreTruncate) > 0
	sc.TruncateAsWarning = (flags & perceptron.FlagTruncateAsWarning) > 0
	sc.InInsertStmt = (flags & perceptron.FlagInInsertStmt) > 0
	sc.InSelectStmt = (flags & perceptron.FlagInSelectStmt) > 0
	sc.OverflowAsWarning = (flags & perceptron.FlagOverflowAsWarning) > 0
	sc.IgnoreZeroInDate = (flags & perceptron.FlagIgnoreZeroInDate) > 0
	sc.DividedByZeroAsWarning = (flags & perceptron.FlagDividedByZeroAsWarning) > 0
}

// GetLockWaitStartTime returns the statement pessimistic lock wait start time
func (sc *StatementContext) GetLockWaitStartTime() time.Time {
	startTime := atomic.LoadInt64(&sc.lockWaitStartTime)
	if startTime == 0 {
		startTime = time.Now().UnixNano()
		atomic.StoreInt64(&sc.lockWaitStartTime, startTime)
	}
	return time.Unix(0, startTime)
}

//CausetTasksDetails defCauslects some useful information of cop-tasks during execution.
type CausetTasksDetails struct {
	NumCausetTasks int

	AvgProcessTime    time.Duration
	P90ProcessTime    time.Duration
	MaxProcessAddress string
	MaxProcessTime    time.Duration

	AvgWaitTime    time.Duration
	P90WaitTime    time.Duration
	MaxWaitAddress string
	MaxWaitTime    time.Duration

	MaxBackoffTime    map[string]time.Duration
	MaxBackoffAddress map[string]string
	AvgBackoffTime    map[string]time.Duration
	P90BackoffTime    map[string]time.Duration
	TotBackoffTime    map[string]time.Duration
	TotBackoffTimes   map[string]int
}

// ToZapFields wraps the CausetTasksDetails as zap.Fileds.
func (d *CausetTasksDetails) ToZapFields() (fields []zap.Field) {
	if d.NumCausetTasks == 0 {
		return
	}
	fields = make([]zap.Field, 0, 10)
	fields = append(fields, zap.Int("num_cop_tasks", d.NumCausetTasks))
	fields = append(fields, zap.String("process_avg_time", strconv.FormatFloat(d.AvgProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_p90_time", strconv.FormatFloat(d.P90ProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_time", strconv.FormatFloat(d.MaxProcessTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("process_max_addr", d.MaxProcessAddress))
	fields = append(fields, zap.String("wait_avg_time", strconv.FormatFloat(d.AvgWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_p90_time", strconv.FormatFloat(d.P90WaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_time", strconv.FormatFloat(d.MaxWaitTime.Seconds(), 'f', -1, 64)+"s"))
	fields = append(fields, zap.String("wait_max_addr", d.MaxWaitAddress))
	return fields
}
