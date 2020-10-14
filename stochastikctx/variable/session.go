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

package variable

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/cpuid"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	pumpcli "github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/pump_client"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/storeutil"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"github.com/twmb/murmur3"
)

var preparedStmtCount int64

// RetryInfo saves retry information.
type RetryInfo struct {
	Retrying               bool
	DroppedPreparedStmtIDs []uint32
	autoIncrementIDs       retryInfoAutoIDs
	autoRandomIDs          retryInfoAutoIDs
}

// Clean does some clean work.
func (r *RetryInfo) Clean() {
	r.autoIncrementIDs.clean()
	r.autoRandomIDs.clean()

	if len(r.DroppedPreparedStmtIDs) > 0 {
		r.DroppedPreparedStmtIDs = r.DroppedPreparedStmtIDs[:0]
	}
}

// ResetOffset resets the current retry offset.
func (r *RetryInfo) ResetOffset() {
	r.autoIncrementIDs.resetOffset()
	r.autoRandomIDs.resetOffset()
}

// AddAutoIncrementID adds id to autoIncrementIDs.
func (r *RetryInfo) AddAutoIncrementID(id int64) {
	r.autoIncrementIDs.autoIDs = append(r.autoIncrementIDs.autoIDs, id)
}

// GetCurrAutoIncrementID gets current autoIncrementID.
func (r *RetryInfo) GetCurrAutoIncrementID() (int64, error) {
	return r.autoIncrementIDs.getCurrent()
}

// AddAutoRandomID adds id to autoRandomIDs.
func (r *RetryInfo) AddAutoRandomID(id int64) {
	r.autoRandomIDs.autoIDs = append(r.autoRandomIDs.autoIDs, id)
}

// GetCurrAutoRandomID gets current AutoRandomID.
func (r *RetryInfo) GetCurrAutoRandomID() (int64, error) {
	return r.autoRandomIDs.getCurrent()
}

type retryInfoAutoIDs struct {
	currentOffset int
	autoIDs       []int64
}

func (r *retryInfoAutoIDs) resetOffset() {
	r.currentOffset = 0
}

func (r *retryInfoAutoIDs) clean() {
	r.currentOffset = 0
	if len(r.autoIDs) > 0 {
		r.autoIDs = r.autoIDs[:0]
	}
}

func (r *retryInfoAutoIDs) getCurrent() (int64, error) {
	if r.currentOffset >= len(r.autoIDs) {
		return 0, errCantGetValidID
	}
	id := r.autoIDs[r.currentOffset]
	r.currentOffset++
	return id, nil
}

// stmtFuture is used to async get timestamp for statement.
type stmtFuture struct {
	future   oracle.Future
	cachedTS uint64
}

// TransactionContext is used to causetstore variables that has transaction scope.
type TransactionContext struct {
	forUFIDelateTS   uint64
	stmtFuture    oracle.Future
	Binlog        interface{}
	SchemaReplicant    interface{}
	History       interface{}
	SchemaVersion int64
	StartTS       uint64

	// ShardStep indicates the max size of continuous rowid shard in one transaction.
	ShardStep    int
	shardRemain  int
	currentShard int64
	shardRand    *rand.Rand

	// BlockDeltaMap is used in the schemaReplicant validator for DBS changes in one block not to block others.
	// It's also used in the statistias uFIDelating.
	// Note: for the partitionted block, it stores all the partition IDs.
	BlockDeltaMap map[int64]BlockDelta

	// unchangedRowKeys is used to causetstore the unchanged rows that needs to lock for pessimistic transaction.
	unchangedRowKeys map[string]struct{}

	// pessimisticLockCache is the cache for pessimistic locked keys,
	// The value never changes during the transaction.
	pessimisticLockCache map[string][]byte
	PessimisticCacheHit  int

	// CreateTime For metrics.
	CreateTime     time.Time
	StatementCount int
	CouldRetry     bool
	IsPessimistic  bool
	Isolation      string
	LockExpire     uint32
	ForUFIDelate      uint32
}

// GetShard returns the shard prefix for the next `count` rowids.
func (tc *TransactionContext) GetShard(shardRowIDBits uint64, typeBitsLength uint64, reserveSignBit bool, count int) int64 {
	if shardRowIDBits == 0 {
		return 0
	}
	if tc.shardRand == nil {
		tc.shardRand = rand.New(rand.NewSource(int64(tc.StartTS)))
	}
	if tc.shardRemain <= 0 {
		tc.uFIDelateShard()
		tc.shardRemain = tc.ShardStep
	}
	tc.shardRemain -= count

	var signBitLength uint64
	if reserveSignBit {
		signBitLength = 1
	}
	return (tc.currentShard & (1<<shardRowIDBits - 1)) << (typeBitsLength - shardRowIDBits - signBitLength)
}

func (tc *TransactionContext) uFIDelateShard() {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], tc.shardRand.Uint64())
	tc.currentShard = int64(murmur3.Sum32(buf[:]))
}

// AddUnchangedRowKey adds an unchanged event key in uFIDelate statement for pessimistic lock.
func (tc *TransactionContext) AddUnchangedRowKey(key []byte) {
	if tc.unchangedRowKeys == nil {
		tc.unchangedRowKeys = map[string]struct{}{}
	}
	tc.unchangedRowKeys[string(key)] = struct{}{}
}

// DefCauslectUnchangedRowKeys defCauslects unchanged event keys for pessimistic lock.
func (tc *TransactionContext) DefCauslectUnchangedRowKeys(buf []ekv.Key) []ekv.Key {
	for key := range tc.unchangedRowKeys {
		buf = append(buf, ekv.Key(key))
	}
	tc.unchangedRowKeys = nil
	return buf
}

// UFIDelateDeltaForBlock uFIDelates the delta info for some block.
func (tc *TransactionContext) UFIDelateDeltaForBlock(physicalBlockID int64, delta int64, count int64, defCausSize map[int64]int64) {
	if tc.BlockDeltaMap == nil {
		tc.BlockDeltaMap = make(map[int64]BlockDelta)
	}
	item := tc.BlockDeltaMap[physicalBlockID]
	if item.DefCausSize == nil && defCausSize != nil {
		item.DefCausSize = make(map[int64]int64, len(defCausSize))
	}
	item.Delta += delta
	item.Count += count
	for key, val := range defCausSize {
		item.DefCausSize[key] += val
	}
	tc.BlockDeltaMap[physicalBlockID] = item
}

// GetKeyInPessimisticLockCache gets a key in pessimistic lock cache.
func (tc *TransactionContext) GetKeyInPessimisticLockCache(key ekv.Key) (val []byte, ok bool) {
	if tc.pessimisticLockCache == nil {
		return nil, false
	}
	val, ok = tc.pessimisticLockCache[string(key)]
	if ok {
		tc.PessimisticCacheHit++
	}
	return
}

// SetPessimisticLockCache sets a key value pair into pessimistic lock cache.
func (tc *TransactionContext) SetPessimisticLockCache(key ekv.Key, val []byte) {
	if tc.pessimisticLockCache == nil {
		tc.pessimisticLockCache = map[string][]byte{}
	}
	tc.pessimisticLockCache[string(key)] = val
}

// Cleanup clears up transaction info that no longer use.
func (tc *TransactionContext) Cleanup() {
	// tc.SchemaReplicant = nil; we cannot do it now, because some operation like handleFieldList depend on this.
	tc.Binlog = nil
	tc.History = nil
	tc.BlockDeltaMap = nil
	tc.pessimisticLockCache = nil
}

// ClearDelta clears the delta map.
func (tc *TransactionContext) ClearDelta() {
	tc.BlockDeltaMap = nil
}

// GetForUFIDelateTS returns the ts for uFIDelate.
func (tc *TransactionContext) GetForUFIDelateTS() uint64 {
	if tc.forUFIDelateTS > tc.StartTS {
		return tc.forUFIDelateTS
	}
	return tc.StartTS
}

// SetForUFIDelateTS sets the ts for uFIDelate.
func (tc *TransactionContext) SetForUFIDelateTS(forUFIDelateTS uint64) {
	if forUFIDelateTS > tc.forUFIDelateTS {
		tc.forUFIDelateTS = forUFIDelateTS
	}
}

// SetStmtFutureForRC sets the stmtFuture .
func (tc *TransactionContext) SetStmtFutureForRC(future oracle.Future) {
	tc.stmtFuture = future
}

// GetStmtFutureForRC gets the stmtFuture.
func (tc *TransactionContext) GetStmtFutureForRC() oracle.Future {
	return tc.stmtFuture
}

// WriteStmtBufs can be used by insert/replace/delete/uFIDelate statement.
// TODO: use a common memory pool to replace this.
type WriteStmtBufs struct {
	// RowValBuf is used by blockcodec.EncodeRow, to reduce runtime.growslice.
	RowValBuf []byte
	// AddRowValues use to causetstore temp insert rows value, to reduce memory allocations when importing data.
	AddRowValues []types.Causet

	// IndexValsBuf is used by index.FetchValues
	IndexValsBuf []types.Causet
	// IndexKeyBuf is used by index.GenIndexKey
	IndexKeyBuf []byte
}

func (ib *WriteStmtBufs) clean() {
	ib.RowValBuf = nil
	ib.AddRowValues = nil
	ib.IndexValsBuf = nil
	ib.IndexKeyBuf = nil
}

// BlockSnapshot represents a data snapshot of the block contained in `information_schema`.
type BlockSnapshot struct {
	Rows [][]types.Causet
	Err  error
}

type txnIsolationLevelOneShotState uint

// RewritePhaseInfo records some information about the rewrite phase
type RewritePhaseInfo struct {
	// DurationRewrite is the duration of rewriting the ALLEGROALLEGROSQL.
	DurationRewrite time.Duration

	// DurationPreprocessSubQuery is the duration of pre-processing sub-queries.
	DurationPreprocessSubQuery time.Duration

	// PreprocessSubQueries is the number of pre-processed sub-queries.
	PreprocessSubQueries int
}

// Reset resets all fields in RewritePhaseInfo.
func (r *RewritePhaseInfo) Reset() {
	r.DurationRewrite = 0
	r.DurationPreprocessSubQuery = 0
	r.PreprocessSubQueries = 0
}

const (
	// oneShotDef means default, that is tx_isolation_one_shot not set.
	oneShotDef txnIsolationLevelOneShotState = iota
	// oneShotSet means it's set in current transaction.
	oneShotSet
	// onsShotUse means it should be used in current transaction.
	oneShotUse
)

// StochastikVars is to handle user-defined or global variables in the current stochastik.
type StochastikVars struct {
	Concurrency
	MemQuota
	BatchSize
	// DMLBatchSize indicates the number of rows batch-committed for a statement.
	// It will be used when using LOAD DATA or BatchInsert or BatchDelete is on.
	DMLBatchSize        int
	RetryLimit          int64
	DisableTxnAutoRetry bool
	// UsersLock is a lock for user defined variables.
	UsersLock sync.RWMutex
	// Users are user defined variables.
	Users map[string]types.Causet
	// systems variables, don't modify it directly, use GetSystemVar/SetSystemVar method.
	systems map[string]string
	// SysWarningCount is the system variable "warning_count", because it is on the hot path, so we extract it from the systems
	SysWarningCount int
	// SysErrorCount is the system variable "error_count", because it is on the hot path, so we extract it from the systems
	SysErrorCount uint16
	// PreparedStmts stores prepared statement.
	PreparedStmts        map[uint32]interface{}
	PreparedStmtNameToID map[string]uint32
	// preparedStmtID is id of prepared statement.
	preparedStmtID uint32
	// PreparedParams params for prepared statements
	PreparedParams PreparedParams

	// ActiveRoles stores active roles for current user
	ActiveRoles []*auth.RoleIdentity

	RetryInfo *RetryInfo
	//  TxnCtx Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// KVVars is the variables for KV storage.
	KVVars *ekv.Variables

	// txnIsolationLevelOneShot is used to implements "set transaction isolation level ..."
	txnIsolationLevelOneShot struct {
		state txnIsolationLevelOneShotState
		value string
	}

	// Status stands for the stochastik status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	// ClientCapability is client's capability.
	ClientCapability uint32

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// ConnectionID is the connection id of the current stochastik.
	ConnectionID uint64

	// PlanID is the unique id of logical and physical plan.
	PlanID int

	// PlanDeferredCausetID is the unique id for defCausumn when building plan.
	PlanDeferredCausetID int64

	// User is the user identity with which the stochastik login.
	User *auth.UserIdentity

	// CurrentDB is the default database of this stochastik.
	CurrentDB string

	// CurrentDBChanged indicates if the CurrentDB has been uFIDelated, and if it is we should print it into
	// the slow log to make it be compatible with MyALLEGROSQL, https://github.com/whtcorpsinc/milevadb/issues/17846.
	CurrentDBChanged bool

	// StrictALLEGROSQLMode indicates if the stochastik is in strict mode.
	StrictALLEGROSQLMode bool

	// CommonGlobalLoaded indicates if common global variable has been loaded for this stochastik.
	CommonGlobalLoaded bool

	// InRestrictedALLEGROSQL indicates if the stochastik is handling restricted ALLEGROALLEGROSQL execution.
	InRestrictedALLEGROSQL bool

	// SnapshotTS is used for reading history data. For simplicity, SnapshotTS only supports allegrosql request.
	SnapshotTS uint64

	// SnapshotschemaReplicant is used with SnapshotTS, when the schemaReplicant version at snapshotTS less than current schemaReplicant
	// version, we load an old version schemaReplicant for query.
	SnapshotschemaReplicant interface{}

	// BinlogClient is used to write binlog.
	BinlogClient *pumpcli.PumpsClient

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// LastFoundRows is the number of found rows of last query statement
	LastFoundRows uint64

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	// AllowAggPushDown can be set to false to forbid aggregation push down.
	AllowAggPushDown bool

	// AllowBCJ means allow broadcast join.
	AllowBCJ bool
	// AllowDistinctAggPushDown can be set true to allow agg with distinct push down to einsteindb/tiflash.
	AllowDistinctAggPushDown bool

	// AllowWriteRowID can be set to false to forbid write data to _milevadb_rowid.
	// This variable is currently not recommended to be turned on.
	AllowWriteRowID bool

	// AllowBatchCop means if we should send batch interlock to TiFlash. Default value is 1, means to use batch cop in case of aggregation and join.
	// If value is set to 2 , which means to force to send batch cop for any query. Value is set to 0 means never use batch cop.
	AllowBatchCop int

	// MilevaDBAllowAutoRandExplicitInsert indicates whether explicit insertion on auto_random defCausumn is allowed.
	AllowAutoRandExplicitInsert bool

	// CorrelationThreshold is the guard to enable event count estimation using defCausumn order correlation.
	CorrelationThreshold float64

	// CorrelationExpFactor is used to control the heuristic approach of event count estimation when CorrelationThreshold is not met.
	CorrelationExpFactor int

	// CPUFactor is the CPU cost of processing one expression for one event.
	CPUFactor float64
	// CopCPUFactor is the CPU cost of processing one expression for one event in interlock.
	CopCPUFactor float64
	// CopTiFlashConcurrencyFactor is the concurrency number of computation in tiflash interlock.
	CopTiFlashConcurrencyFactor float64
	// NetworkFactor is the network cost of transferring 1 byte data.
	NetworkFactor float64
	// ScanFactor is the IO cost of scanning 1 byte data on EinsteinDB and TiFlash.
	ScanFactor float64
	// DescScanFactor is the IO cost of scanning 1 byte data on EinsteinDB and TiFlash in desc order.
	DescScanFactor float64
	// SeekFactor is the IO cost of seeking the start value of a range in EinsteinDB or TiFlash.
	SeekFactor float64
	// MemoryFactor is the memory cost of storing one tuple.
	MemoryFactor float64
	// DiskFactor is the IO cost of reading/writing one byte to temporary disk.
	DiskFactor float64
	// ConcurrencyFactor is the CPU cost of additional one goroutine.
	ConcurrencyFactor float64

	// CurrInsertValues is used to record current ValuesExpr's values.
	// See http://dev.allegrosql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
	CurrInsertValues chunk.Row

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the stochastik time_zone variable.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	ALLEGROSQLMode allegrosql.ALLEGROSQLMode

	// AutoIncrementIncrement and AutoIncrementOffset indicates the autoID's start value and increment.
	AutoIncrementIncrement int

	AutoIncrementOffset int

	/* MilevaDB system variables */

	// SkipASCIICheck check on input value.
	SkipASCIICheck bool

	// SkipUTF8Check check on input value.
	SkipUTF8Check bool

	// BatchInsert indicates if we should split insert data into multiple batches.
	BatchInsert bool

	// BatchDelete indicates if we should split delete data into multiple batches.
	BatchDelete bool

	// BatchCommit indicates if we should split the transaction into multiple batches.
	BatchCommit bool

	// IDSlabPredictor is provided by kvEncoder, if it is provided, we will use it to alloc auto id instead of using
	// Block.alloc.
	IDSlabPredictor autoid.SlabPredictor

	// OptimizerSelectivityLevel defines the level of the selectivity estimation in plan.
	OptimizerSelectivityLevel int

	// EnableBlockPartition enables block partition feature.
	EnableBlockPartition string

	// EnableCascadesPlanner enables the cascades planner.
	EnableCascadesPlanner bool

	// EnableWindowFunction enables the window function.
	EnableWindowFunction bool

	// EnableVectorizedExpression  enables the vectorized expression evaluation.
	EnableVectorizedExpression bool

	// DBSReorgPriority is the operation priority of adding indices.
	DBSReorgPriority int

	// EnableChangeDeferredCausetType is used to control whether to enable the change defCausumn type.
	EnableChangeDeferredCausetType bool

	// WaitSplitRegionFinish defines the split region behaviour is sync or async.
	WaitSplitRegionFinish bool

	// WaitSplitRegionTimeout defines the split region timeout.
	WaitSplitRegionTimeout uint64

	// EnableStreaming indicates whether the interlock request can use streaming API.
	// TODO: remove this after milevadb-server configuration "enable-streaming' removed.
	EnableStreaming bool

	// EnableChunkRPC indicates whether the interlock request can use chunk API.
	EnableChunkRPC bool

	writeStmtBufs WriteStmtBufs

	// L2CacheSize indicates the size of CPU L2 cache, using byte as unit.
	L2CacheSize int

	// EnableRadixJoin indicates whether to use radix hash join to execute
	// HashJoin.
	EnableRadixJoin bool

	// ConstraintCheckInPlace indicates whether to check the constraint when the ALLEGROALLEGROSQL executing.
	ConstraintCheckInPlace bool

	// CommandValue indicates which command current stochastik is doing.
	CommandValue uint32

	// MilevaDBOptJoinReorderThreshold defines the minimal number of join nodes
	// to use the greedy join reorder algorithm.
	MilevaDBOptJoinReorderThreshold int

	// SlowQueryFile indicates which slow query log file for SLOW_QUERY block to parse.
	SlowQueryFile string

	// EnableFastAnalyze indicates whether to take fast analyze.
	EnableFastAnalyze bool

	// TxnMode indicates should be pessimistic or optimistic.
	TxnMode string

	// LowResolutionTSO is used for reading data with low resolution TSO which is uFIDelated once every two seconds.
	LowResolutionTSO bool

	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the value is 0, timeouts are not enabled.
	// See https://dev.allegrosql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_execution_time
	MaxExecutionTime uint64

	// Killed is a flag to indicate that this query is killed.
	Killed uint32

	// ConnectionInfo indicates current connection info used by current stochastik, only be lazy assigned by plugin.
	ConnectionInfo *ConnectionInfo

	// use noop funcs or not
	EnableNoopFuncs bool

	// StartTime is the start time of the last query.
	StartTime time.Time

	// DurationParse is the duration of parsing ALLEGROALLEGROSQL string to AST of the last query.
	DurationParse time.Duration

	// DurationCompile is the duration of compiling AST to execution plan of the last query.
	DurationCompile time.Duration

	// RewritePhaseInfo records all information about the rewriting phase.
	RewritePhaseInfo

	// DurationOptimization is the duration of optimizing a query.
	DurationOptimization time.Duration

	// DurationWaitTS is the duration of waiting for a snapshot TS
	DurationWaitTS time.Duration

	// PrevStmt is used to causetstore the previous executed statement in the current stochastik.
	PrevStmt fmt.Stringer

	// prevStmtDigest is used to causetstore the digest of the previous statement in the current stochastik.
	prevStmtDigest string

	// AllowRemoveAutoInc indicates whether a user can drop the auto_increment defCausumn attribute or not.
	AllowRemoveAutoInc bool

	// UsePlanBaselines indicates whether we will use plan baselines to adjust plan.
	UsePlanBaselines bool

	// EvolvePlanBaselines indicates whether we will evolve the plan baselines.
	EvolvePlanBaselines bool

	// Unexported fields should be accessed and set through interfaces like GetReplicaRead() and SetReplicaRead().

	// allowInSubqToJoinAnPosetDagg can be set to false to forbid rewriting the semi join to inner join with agg.
	allowInSubqToJoinAnPosetDagg bool

	// EnableIndexMerge enables the generation of IndexMergePath.
	enableIndexMerge bool

	// replicaRead is used for reading data from replicas, only follower is supported at this time.
	replicaRead ekv.ReplicaReadType

	// IsolationReadEngines is used to isolation read, milevadb only read from the stores whose engine type is in the engines.
	IsolationReadEngines map[ekv.StoreType]struct{}

	PlannerSelectBlockAsName []ast.HintBlock

	// LockWaitTimeout is the duration waiting for pessimistic lock in milliseconds
	// negative value means nowait, 0 means default behavior, others means actual wait time
	LockWaitTimeout int64

	// MetricSchemaStep indicates the step when query metric schemaReplicant.
	MetricSchemaStep int64
	// MetricSchemaRangeDuration indicates the step when query metric schemaReplicant.
	MetricSchemaRangeDuration int64

	// Some data of cluster-level memory blocks will be retrieved many times in different inspection rules,
	// and the cost of retrieving some data is expensive. We use the `BlockSnapshot` to cache those data
	// and obtain them lazily, and provide a consistent view of inspection blocks for each inspection rules.
	// All cached snapshots will be released at the end of retrieving
	InspectionBlockCache map[string]BlockSnapshot

	// RowEncoder is reused in stochastik for encode event data.
	RowEncoder rowcodec.Encoder

	// SequenceState cache all sequence's latest value accessed by lastval() builtins. It's a stochastik scoped
	// variable, and all public methods of SequenceState are currently-safe.
	SequenceState *SequenceState

	// WindowingUseHighPrecision determines whether to compute window operations without loss of precision.
	// see https://dev.allegrosql.com/doc/refman/8.0/en/window-function-optimization.html for more details.
	WindowingUseHighPrecision bool

	// FoundInPlanCache indicates whether this statement was found in plan cache.
	FoundInPlanCache bool
	// PrevFoundInPlanCache indicates whether the last statement was found in plan cache.
	PrevFoundInPlanCache bool

	// OptimizerUseInvisibleIndexes indicates whether optimizer can use invisible index
	OptimizerUseInvisibleIndexes bool

	// SelectLimit limits the max counts of select statement's output
	SelectLimit uint64

	// EnableClusteredIndex indicates whether to enable clustered index when creating a new block.
	EnableClusteredIndex bool

	// PresumeKeyNotExists indicates lazy existence checking is enabled.
	PresumeKeyNotExists bool

	// EnableParallelApply indicates that thether to use parallel apply.
	EnableParallelApply bool

	// ShardAllocateStep indicates the max size of continuous rowid shard in one transaction.
	ShardAllocateStep int64

	// EnableAmendPessimisticTxn indicates if schemaReplicant change amend is enabled for pessimistic transactions.
	EnableAmendPessimisticTxn bool

	// LastTxnInfo keeps track the info of last committed transaction.
	LastTxnInfo ekv.TxnInfo

	// PartitionPruneMode indicates how and when to prune partitions.
	PartitionPruneMode PartitionPruneMode
}

// UseDynamicPartitionPrune indicates whether use new dynamic partition prune.
func (s *StochastikVars) UseDynamicPartitionPrune() bool {
	return s.PartitionPruneMode == DynamicOnly
}

// PartitionPruneMode presents the prune mode used.
type PartitionPruneMode string

const (
	// StaticOnly indicates only prune at plan phase.
	StaticOnly PartitionPruneMode = "static-only"
	// DynamicOnly indicates only prune at execute phase.
	DynamicOnly PartitionPruneMode = "dynamic-only"
	// StaticButPrepareDynamic indicates prune at plan phase but defCauslect stats need for dynamic prune.
	StaticButPrepareDynamic PartitionPruneMode = "static-defCauslect-dynamic"
)

// Valid indicate PruneMode is validated.
func (p PartitionPruneMode) Valid() bool {
	switch p {
	case StaticOnly, StaticButPrepareDynamic, DynamicOnly:
		return true
	default:
		return false
	}
}

// PreparedParams contains the parameters of the current prepared statement when executing it.
type PreparedParams []types.Causet

func (pps PreparedParams) String() string {
	if len(pps) == 0 {
		return ""
	}
	return " [arguments: " + types.CausetsToStrNoErr(pps) + "]"
}

// ConnectionInfo present connection used by audit.
type ConnectionInfo struct {
	ConnectionID      uint32
	ConnectionType    string
	Host              string
	ClientIP          string
	ClientPort        string
	ServerID          int
	ServerPort        int
	Duration          float64
	User              string
	ServerOSLoginUser string
	OSVersion         string
	ClientVersion     string
	ServerVersion     string
	SSLVersion        string
	PID               int
	EDB                string
}

// NewStochastikVars creates a stochastik vars object.
func NewStochastikVars() *StochastikVars {
	vars := &StochastikVars{
		Users:                       make(map[string]types.Causet),
		systems:                     make(map[string]string),
		PreparedStmts:               make(map[uint32]interface{}),
		PreparedStmtNameToID:        make(map[string]uint32),
		PreparedParams:              make([]types.Causet, 0, 10),
		TxnCtx:                      &TransactionContext{},
		RetryInfo:                   &RetryInfo{},
		ActiveRoles:                 make([]*auth.RoleIdentity, 0, 10),
		StrictALLEGROSQLMode:               true,
		AutoIncrementIncrement:      DefAutoIncrementIncrement,
		AutoIncrementOffset:         DefAutoIncrementOffset,
		Status:                      allegrosql.ServerStatusAutocommit,
		StmtCtx:                     new(stmtctx.StatementContext),
		AllowAggPushDown:            false,
		AllowBCJ:                    false,
		OptimizerSelectivityLevel:   DefMilevaDBOptimizerSelectivityLevel,
		RetryLimit:                  DefMilevaDBRetryLimit,
		DisableTxnAutoRetry:         DefMilevaDBDisableTxnAutoRetry,
		DBSReorgPriority:            ekv.PriorityLow,
		allowInSubqToJoinAnPosetDagg:     DefOptInSubqToJoinAnPosetDagg,
		CorrelationThreshold:        DefOptCorrelationThreshold,
		CorrelationExpFactor:        DefOptCorrelationExpFactor,
		CPUFactor:                   DefOptCPUFactor,
		CopCPUFactor:                DefOptCopCPUFactor,
		CopTiFlashConcurrencyFactor: DefOptTiFlashConcurrencyFactor,
		NetworkFactor:               DefOptNetworkFactor,
		ScanFactor:                  DefOptScanFactor,
		DescScanFactor:              DefOptDescScanFactor,
		SeekFactor:                  DefOptSeekFactor,
		MemoryFactor:                DefOptMemoryFactor,
		DiskFactor:                  DefOptDiskFactor,
		ConcurrencyFactor:           DefOptConcurrencyFactor,
		EnableRadixJoin:             false,
		EnableVectorizedExpression:  DefEnableVectorizedExpression,
		L2CacheSize:                 cpuid.CPU.Cache.L2,
		CommandValue:                uint32(allegrosql.ComSleep),
		MilevaDBOptJoinReorderThreshold: DefMilevaDBOptJoinReorderThreshold,
		SlowQueryFile:               config.GetGlobalConfig().Log.SlowQueryFile,
		WaitSplitRegionFinish:       DefMilevaDBWaitSplitRegionFinish,
		WaitSplitRegionTimeout:      DefWaitSplitRegionTimeout,
		enableIndexMerge:            false,
		EnableNoopFuncs:             DefMilevaDBEnableNoopFuncs,
		replicaRead:                 ekv.ReplicaReadLeader,
		AllowRemoveAutoInc:          DefMilevaDBAllowRemoveAutoInc,
		UsePlanBaselines:            DefMilevaDBUsePlanBaselines,
		EvolvePlanBaselines:         DefMilevaDBEvolvePlanBaselines,
		IsolationReadEngines:        make(map[ekv.StoreType]struct{}),
		LockWaitTimeout:             DefInnodbLockWaitTimeout * 1000,
		MetricSchemaStep:            DefMilevaDBMetricSchemaStep,
		MetricSchemaRangeDuration:   DefMilevaDBMetricSchemaRangeDuration,
		SequenceState:               NewSequenceState(),
		WindowingUseHighPrecision:   true,
		PrevFoundInPlanCache:        DefMilevaDBFoundInPlanCache,
		FoundInPlanCache:            DefMilevaDBFoundInPlanCache,
		SelectLimit:                 math.MaxUint64,
		AllowAutoRandExplicitInsert: DefMilevaDBAllowAutoRandExplicitInsert,
		EnableClusteredIndex:        DefMilevaDBEnableClusteredIndex,
		EnableParallelApply:         DefMilevaDBEnableParallelApply,
		ShardAllocateStep:           DefMilevaDBShardAllocateStep,
		EnableChangeDeferredCausetType:      DefMilevaDBChangeDeferredCausetType,
		EnableAmendPessimisticTxn:   DefMilevaDBEnableAmendPessimisticTxn,
	}
	vars.KVVars = ekv.NewVariables(&vars.Killed)
	vars.Concurrency = Concurrency{
		indexLookupConcurrency:     DefIndexLookupConcurrency,
		indexSerialScanConcurrency: DefIndexSerialScanConcurrency,
		indexLookupJoinConcurrency: DefIndexLookupJoinConcurrency,
		hashJoinConcurrency:        DefMilevaDBHashJoinConcurrency,
		projectionConcurrency:      DefMilevaDBProjectionConcurrency,
		distALLEGROSQLScanConcurrency:     DefDistALLEGROSQLScanConcurrency,
		hashAggPartialConcurrency:  DefMilevaDBHashAggPartialConcurrency,
		hashAggFinalConcurrency:    DefMilevaDBHashAggFinalConcurrency,
		windowConcurrency:          DefMilevaDBWindowConcurrency,
		ExecutorConcurrency:        DefExecutorConcurrency,
	}
	vars.MemQuota = MemQuota{
		MemQuotaQuery:               config.GetGlobalConfig().MemQuotaQuery,
		NestedLoopJoinCacheCapacity: config.GetGlobalConfig().NestedLoopJoinCacheCapacity,

		// The variables below do not take any effect anymore, it's remaining for compatibility.
		// TODO: remove them in v4.1
		MemQuotaHashJoin:          DefMilevaDBMemQuotaHashJoin,
		MemQuotaMergeJoin:         DefMilevaDBMemQuotaMergeJoin,
		MemQuotaSort:              DefMilevaDBMemQuotaSort,
		MemQuotaTopn:              DefMilevaDBMemQuotaTopn,
		MemQuotaIndexLookupReader: DefMilevaDBMemQuotaIndexLookupReader,
		MemQuotaIndexLookupJoin:   DefMilevaDBMemQuotaIndexLookupJoin,
		MemQuotaNestedLoopApply:   DefMilevaDBMemQuotaNestedLoopApply,
		MemQuotaDistALLEGROSQL:           DefMilevaDBMemQuotaDistALLEGROSQL,
	}
	vars.BatchSize = BatchSize{
		IndexJoinBatchSize: DefIndexJoinBatchSize,
		IndexLookupSize:    DefIndexLookupSize,
		InitChunkSize:      DefInitChunkSize,
		MaxChunkSize:       DefMaxChunkSize,
	}
	vars.DMLBatchSize = DefDMLBatchSize
	var enableStreaming string
	if config.GetGlobalConfig().EnableStreaming {
		enableStreaming = "1"
	} else {
		enableStreaming = "0"
	}
	terror.Log(vars.SetSystemVar(MilevaDBEnableStreaming, enableStreaming))

	vars.AllowBatchCop = DefMilevaDBAllowBatchCop

	var enableChunkRPC string
	if config.GetGlobalConfig().EinsteinDBClient.EnableChunkRPC {
		enableChunkRPC = "1"
	} else {
		enableChunkRPC = "0"
	}
	terror.Log(vars.SetSystemVar(MilevaDBEnableChunkRPC, enableChunkRPC))
	for _, engine := range config.GetGlobalConfig().IsolationRead.Engines {
		switch engine {
		case ekv.TiFlash.Name():
			vars.IsolationReadEngines[ekv.TiFlash] = struct{}{}
		case ekv.EinsteinDB.Name():
			vars.IsolationReadEngines[ekv.EinsteinDB] = struct{}{}
		case ekv.MilevaDB.Name():
			vars.IsolationReadEngines[ekv.MilevaDB] = struct{}{}
		}
	}
	return vars
}

// GetAllowInSubqToJoinAnPosetDagg get AllowInSubqToJoinAnPosetDagg from allegrosql hints and StochastikVars.allowInSubqToJoinAnPosetDagg.
func (s *StochastikVars) GetAllowInSubqToJoinAnPosetDagg() bool {
	if s.StmtCtx.HasAllowInSubqToJoinAnPosetDaggHint {
		return s.StmtCtx.AllowInSubqToJoinAnPosetDagg
	}
	return s.allowInSubqToJoinAnPosetDagg
}

// SetAllowInSubqToJoinAnPosetDagg set StochastikVars.allowInSubqToJoinAnPosetDagg.
func (s *StochastikVars) SetAllowInSubqToJoinAnPosetDagg(val bool) {
	s.allowInSubqToJoinAnPosetDagg = val
}

// GetEnableCascadesPlanner get EnableCascadesPlanner from allegrosql hints and StochastikVars.EnableCascadesPlanner.
func (s *StochastikVars) GetEnableCascadesPlanner() bool {
	if s.StmtCtx.HasEnableCascadesPlannerHint {
		return s.StmtCtx.EnableCascadesPlanner
	}
	return s.EnableCascadesPlanner
}

// SetEnableCascadesPlanner set StochastikVars.EnableCascadesPlanner.
func (s *StochastikVars) SetEnableCascadesPlanner(val bool) {
	s.EnableCascadesPlanner = val
}

// GetEnableIndexMerge get EnableIndexMerge from StochastikVars.enableIndexMerge.
func (s *StochastikVars) GetEnableIndexMerge() bool {
	return s.enableIndexMerge
}

// SetEnableIndexMerge set StochastikVars.enableIndexMerge.
func (s *StochastikVars) SetEnableIndexMerge(val bool) {
	s.enableIndexMerge = val
}

// GetReplicaRead get ReplicaRead from allegrosql hints and StochastikVars.replicaRead.
func (s *StochastikVars) GetReplicaRead() ekv.ReplicaReadType {
	if s.StmtCtx.HasReplicaReadHint {
		return ekv.ReplicaReadType(s.StmtCtx.ReplicaRead)
	}
	return s.replicaRead
}

// SetReplicaRead set StochastikVars.replicaRead.
func (s *StochastikVars) SetReplicaRead(val ekv.ReplicaReadType) {
	s.replicaRead = val
}

// GetWriteStmtBufs get pointer of StochastikVars.writeStmtBufs.
func (s *StochastikVars) GetWriteStmtBufs() *WriteStmtBufs {
	return &s.writeStmtBufs
}

// GetSplitRegionTimeout gets split region timeout.
func (s *StochastikVars) GetSplitRegionTimeout() time.Duration {
	return time.Duration(s.WaitSplitRegionTimeout) * time.Second
}

// GetIsolationReadEngines gets isolation read engines.
func (s *StochastikVars) GetIsolationReadEngines() map[ekv.StoreType]struct{} {
	return s.IsolationReadEngines
}

// CleanBuffers cleans the temporary bufs
func (s *StochastikVars) CleanBuffers() {
	s.GetWriteStmtBufs().clean()
}

// AllocPlanDeferredCausetID allocates defCausumn id for plan.
func (s *StochastikVars) AllocPlanDeferredCausetID() int64 {
	s.PlanDeferredCausetID++
	return s.PlanDeferredCausetID
}

// GetCharsetInfo gets charset and defCauslation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and defCauslation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// defCauslation_connection is important for comparisons of literal strings.
// For comparisons of strings with defCausumn values, defCauslation_connection does not matter because defCausumns
// have their own defCauslation, which has a higher defCauslation precedence.
// See https://dev.allegrosql.com/doc/refman/5.7/en/charset-connection.html
func (s *StochastikVars) GetCharsetInfo() (charset, defCauslation string) {
	charset = s.systems[CharacterSetConnection]
	defCauslation = s.systems[DefCauslationConnection]
	return
}

// SetUserVar set the value and defCauslation for user defined variable.
func (s *StochastikVars) SetUserVar(varName string, svalue string, defCauslation string) {
	if len(defCauslation) > 0 {
		s.Users[varName] = types.NewDefCauslationStringCauset(stringutil.Copy(svalue), defCauslation, defCauslate.DefaultLen)
	} else {
		_, defCauslation = s.GetCharsetInfo()
		s.Users[varName] = types.NewDefCauslationStringCauset(stringutil.Copy(svalue), defCauslation, defCauslate.DefaultLen)
	}
}

// SetLastInsertID saves the last insert id to the stochastik context.
// TODO: we may causetstore the result for last_insert_id sys var later.
func (s *StochastikVars) SetLastInsertID(insertID uint64) {
	s.StmtCtx.LastInsertID = insertID
}

// SetStatusFlag sets the stochastik server status variable.
// If on is ture sets the flag in stochastik status,
// otherwise removes the flag.
func (s *StochastikVars) SetStatusFlag(flag uint16, on bool) {
	if on {
		s.Status |= flag
		return
	}
	s.Status &= ^flag
}

// GetStatusFlag gets the stochastik server status variable, returns true if it is on.
func (s *StochastikVars) GetStatusFlag(flag uint16) bool {
	return s.Status&flag > 0
}

// InTxn returns if the stochastik is in transaction.
func (s *StochastikVars) InTxn() bool {
	return s.GetStatusFlag(allegrosql.ServerStatusInTrans)
}

// IsAutocommit returns if the stochastik is set to autocommit.
func (s *StochastikVars) IsAutocommit() bool {
	return s.GetStatusFlag(allegrosql.ServerStatusAutocommit)
}

// IsReadConsistencyTxn if true it means the transaction is an read consistency (read committed) transaction.
func (s *StochastikVars) IsReadConsistencyTxn() bool {
	if s.TxnCtx.Isolation != "" {
		return s.TxnCtx.Isolation == ast.ReadCommitted
	}
	if s.txnIsolationLevelOneShot.state == oneShotUse {
		s.TxnCtx.Isolation = s.txnIsolationLevelOneShot.value
	}
	if s.TxnCtx.Isolation == "" {
		s.TxnCtx.Isolation, _ = s.GetSystemVar(TxnIsolation)
	}
	return s.TxnCtx.Isolation == ast.ReadCommitted
}

// SetTxnIsolationLevelOneShotStateForNextTxn sets the txnIsolationLevelOneShot.state for next transaction.
func (s *StochastikVars) SetTxnIsolationLevelOneShotStateForNextTxn() {
	if isoLevelOneShot := &s.txnIsolationLevelOneShot; isoLevelOneShot.state != oneShotDef {
		switch isoLevelOneShot.state {
		case oneShotSet:
			isoLevelOneShot.state = oneShotUse
		case oneShotUse:
			isoLevelOneShot.state = oneShotDef
			isoLevelOneShot.value = ""
		}
	}
}

// IsPessimisticReadConsistency if true it means the statement is in an read consistency pessimistic transaction.
func (s *StochastikVars) IsPessimisticReadConsistency() bool {
	return s.TxnCtx.IsPessimistic && s.IsReadConsistencyTxn()
}

// GetNextPreparedStmtID generates and returns the next stochastik scope prepared statement id.
func (s *StochastikVars) GetNextPreparedStmtID() uint32 {
	s.preparedStmtID++
	return s.preparedStmtID
}

// Location returns the value of time_zone stochastik variable. If it is nil, then return time.Local.
func (s *StochastikVars) Location() *time.Location {
	loc := s.TimeZone
	if loc == nil {
		loc = timeutil.SystemLocation()
	}
	return loc
}

// GetSystemVar gets the string value of a system variable.
func (s *StochastikVars) GetSystemVar(name string) (string, bool) {
	if name == WarningCount {
		return strconv.Itoa(s.SysWarningCount), true
	} else if name == ErrorCount {
		return strconv.Itoa(int(s.SysErrorCount)), true
	}
	val, ok := s.systems[name]
	return val, ok
}

func (s *StochastikVars) setDBSReorgPriority(val string) {
	val = strings.ToLower(val)
	switch val {
	case "priority_low":
		s.DBSReorgPriority = ekv.PriorityLow
	case "priority_normal":
		s.DBSReorgPriority = ekv.PriorityNormal
	case "priority_high":
		s.DBSReorgPriority = ekv.PriorityHigh
	default:
		s.DBSReorgPriority = ekv.PriorityLow
	}
}

// AddPreparedStmt adds prepareStmt to current stochastik and count in global.
func (s *StochastikVars) AddPreparedStmt(stmtID uint32, stmt interface{}) error {
	if _, exists := s.PreparedStmts[stmtID]; !exists {
		valStr, _ := s.GetSystemVar(MaxPreparedStmtCount)
		maxPreparedStmtCount, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			maxPreparedStmtCount = DefMaxPreparedStmtCount
		}
		newPreparedStmtCount := atomic.AddInt64(&preparedStmtCount, 1)
		if maxPreparedStmtCount >= 0 && newPreparedStmtCount > maxPreparedStmtCount {
			atomic.AddInt64(&preparedStmtCount, -1)
			return ErrMaxPreparedStmtCountReached.GenWithStackByArgs(maxPreparedStmtCount)
		}
		metrics.PreparedStmtGauge.Set(float64(newPreparedStmtCount))
	}
	s.PreparedStmts[stmtID] = stmt
	return nil
}

// RemovePreparedStmt removes preparedStmt from current stochastik and decrease count in global.
func (s *StochastikVars) RemovePreparedStmt(stmtID uint32) {
	_, exists := s.PreparedStmts[stmtID]
	if !exists {
		return
	}
	delete(s.PreparedStmts, stmtID)
	afterMinus := atomic.AddInt64(&preparedStmtCount, -1)
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// WithdrawAllPreparedStmt remove all preparedStmt in current stochastik and decrease count in global.
func (s *StochastikVars) WithdrawAllPreparedStmt() {
	psCount := len(s.PreparedStmts)
	if psCount == 0 {
		return
	}
	afterMinus := atomic.AddInt64(&preparedStmtCount, -int64(psCount))
	metrics.PreparedStmtGauge.Set(float64(afterMinus))
}

// SetSystemVar sets the value of a system variable.
func (s *StochastikVars) SetSystemVar(name string, val string) error {
	switch name {
	case TxnIsolationOneShot:
		switch val {
		case "SERIALIZABLE", "READ-UNCOMMITTED":
			skipIsolationLevelCheck, err := GetStochastikSystemVar(s, MilevaDBSkipIsolationLevelCheck)
			returnErr := ErrUnsupportedIsolationLevel.GenWithStackByArgs(val)
			if err != nil {
				returnErr = err
			}
			if !MilevaDBOptOn(skipIsolationLevelCheck) || err != nil {
				return returnErr
			}
			//SET TRANSACTION ISOLATION LEVEL will affect two internal variables:
			// 1. tx_isolation
			// 2. transaction_isolation
			// The following if condition is used to deduplicate two same warnings.
			if name == "transaction_isolation" {
				s.StmtCtx.AppendWarning(returnErr)
			}
		}
		s.txnIsolationLevelOneShot.state = oneShotSet
		s.txnIsolationLevelOneShot.value = val
	case TimeZone:
		tz, err := parseTimeZone(val)
		if err != nil {
			return err
		}
		s.TimeZone = tz
	case ALLEGROSQLModeVar:
		val = allegrosql.FormatALLEGROSQLModeStr(val)
		// Modes is a list of different modes separated by commas.
		sqlMode, err2 := allegrosql.GetALLEGROSQLMode(val)
		if err2 != nil {
			return errors.Trace(err2)
		}
		s.StrictALLEGROSQLMode = sqlMode.HasStrictMode()
		s.ALLEGROSQLMode = sqlMode
		s.SetStatusFlag(allegrosql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
	case MilevaDBSnapshot:
		err := setSnapshotTS(s, val)
		if err != nil {
			return err
		}
	case AutoCommit:
		isAutocommit := MilevaDBOptOn(val)
		s.SetStatusFlag(allegrosql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			s.SetStatusFlag(allegrosql.ServerStatusInTrans, false)
		}
	case AutoIncrementIncrement:
		// AutoIncrementIncrement is valid in [1, 65535].
		s.AutoIncrementIncrement = milevadbOptPositiveInt32(val, DefAutoIncrementIncrement)
	case AutoIncrementOffset:
		// AutoIncrementOffset is valid in [1, 65535].
		s.AutoIncrementOffset = milevadbOptPositiveInt32(val, DefAutoIncrementOffset)
	case MaxExecutionTime:
		timeoutMS := milevadbOptPositiveInt32(val, 0)
		s.MaxExecutionTime = uint64(timeoutMS)
	case InnodbLockWaitTimeout:
		lockWaitSec := milevadbOptInt64(val, DefInnodbLockWaitTimeout)
		s.LockWaitTimeout = lockWaitSec * 1000
	case WindowingUseHighPrecision:
		s.WindowingUseHighPrecision = MilevaDBOptOn(val)
	case MilevaDBSkipUTF8Check:
		s.SkipUTF8Check = MilevaDBOptOn(val)
	case MilevaDBSkipASCIICheck:
		s.SkipASCIICheck = MilevaDBOptOn(val)
	case MilevaDBOptAggPushDown:
		s.AllowAggPushDown = MilevaDBOptOn(val)
	case MilevaDBOptBCJ:
		s.AllowBCJ = MilevaDBOptOn(val)
	case MilevaDBOptDistinctAggPushDown:
		s.AllowDistinctAggPushDown = MilevaDBOptOn(val)
	case MilevaDBOptWriteRowID:
		s.AllowWriteRowID = MilevaDBOptOn(val)
	case MilevaDBOptInSubqToJoinAnPosetDagg:
		s.SetAllowInSubqToJoinAnPosetDagg(MilevaDBOptOn(val))
	case MilevaDBOptCorrelationThreshold:
		s.CorrelationThreshold = milevadbOptFloat64(val, DefOptCorrelationThreshold)
	case MilevaDBOptCorrelationExpFactor:
		s.CorrelationExpFactor = int(milevadbOptInt64(val, DefOptCorrelationExpFactor))
	case MilevaDBOptCPUFactor:
		s.CPUFactor = milevadbOptFloat64(val, DefOptCPUFactor)
	case MilevaDBOptCopCPUFactor:
		s.CopCPUFactor = milevadbOptFloat64(val, DefOptCopCPUFactor)
	case MilevaDBOptTiFlashConcurrencyFactor:
		s.CopTiFlashConcurrencyFactor = milevadbOptFloat64(val, DefOptTiFlashConcurrencyFactor)
	case MilevaDBOptNetworkFactor:
		s.NetworkFactor = milevadbOptFloat64(val, DefOptNetworkFactor)
	case MilevaDBOptScanFactor:
		s.ScanFactor = milevadbOptFloat64(val, DefOptScanFactor)
	case MilevaDBOptDescScanFactor:
		s.DescScanFactor = milevadbOptFloat64(val, DefOptDescScanFactor)
	case MilevaDBOptSeekFactor:
		s.SeekFactor = milevadbOptFloat64(val, DefOptSeekFactor)
	case MilevaDBOptMemoryFactor:
		s.MemoryFactor = milevadbOptFloat64(val, DefOptMemoryFactor)
	case MilevaDBOptDiskFactor:
		s.DiskFactor = milevadbOptFloat64(val, DefOptDiskFactor)
	case MilevaDBOptConcurrencyFactor:
		s.ConcurrencyFactor = milevadbOptFloat64(val, DefOptConcurrencyFactor)
	case MilevaDBIndexLookupConcurrency:
		s.indexLookupConcurrency = milevadbOptPositiveInt32(val, ConcurrencyUnset)
	case MilevaDBIndexLookupJoinConcurrency:
		s.indexLookupJoinConcurrency = milevadbOptPositiveInt32(val, ConcurrencyUnset)
	case MilevaDBIndexJoinBatchSize:
		s.IndexJoinBatchSize = milevadbOptPositiveInt32(val, DefIndexJoinBatchSize)
	case MilevaDBAllowBatchCop:
		s.AllowBatchCop = int(milevadbOptInt64(val, DefMilevaDBAllowBatchCop))
	case MilevaDBIndexLookupSize:
		s.IndexLookupSize = milevadbOptPositiveInt32(val, DefIndexLookupSize)
	case MilevaDBHashJoinConcurrency:
		s.hashJoinConcurrency = milevadbOptPositiveInt32(val, ConcurrencyUnset)
	case MilevaDBProjectionConcurrency:
		s.projectionConcurrency = milevadbOptPositiveInt32(val, ConcurrencyUnset)
	case MilevaDBHashAggPartialConcurrency:
		s.hashAggPartialConcurrency = milevadbOptPositiveInt32(val, ConcurrencyUnset)
	case MilevaDBHashAggFinalConcurrency:
		s.hashAggFinalConcurrency = milevadbOptPositiveInt32(val, ConcurrencyUnset)
	case MilevaDBWindowConcurrency:
		s.windowConcurrency = milevadbOptPositiveInt32(val, ConcurrencyUnset)
	case MilevaDBDistALLEGROSQLScanConcurrency:
		s.distALLEGROSQLScanConcurrency = milevadbOptPositiveInt32(val, DefDistALLEGROSQLScanConcurrency)
	case MilevaDBIndexSerialScanConcurrency:
		s.indexSerialScanConcurrency = milevadbOptPositiveInt32(val, DefIndexSerialScanConcurrency)
	case MilevaDBExecutorConcurrency:
		s.ExecutorConcurrency = milevadbOptPositiveInt32(val, DefExecutorConcurrency)
	case MilevaDBBackoffLockFast:
		s.KVVars.BackoffLockFast = milevadbOptPositiveInt32(val, ekv.DefBackoffLockFast)
	case MilevaDBBackOffWeight:
		s.KVVars.BackOffWeight = milevadbOptPositiveInt32(val, ekv.DefBackOffWeight)
	case MilevaDBConstraintCheckInPlace:
		s.ConstraintCheckInPlace = MilevaDBOptOn(val)
	case MilevaDBBatchInsert:
		s.BatchInsert = MilevaDBOptOn(val)
	case MilevaDBBatchDelete:
		s.BatchDelete = MilevaDBOptOn(val)
	case MilevaDBBatchCommit:
		s.BatchCommit = MilevaDBOptOn(val)
	case MilevaDBDMLBatchSize:
		s.DMLBatchSize = int(milevadbOptInt64(val, DefOptCorrelationExpFactor))
	case MilevaDBCurrentTS, MilevaDBLastTxnInfo, MilevaDBConfig:
		return ErrReadOnly
	case MilevaDBMaxChunkSize:
		s.MaxChunkSize = milevadbOptPositiveInt32(val, DefMaxChunkSize)
	case MilevaDBInitChunkSize:
		s.InitChunkSize = milevadbOptPositiveInt32(val, DefInitChunkSize)
	case MilevaDBMemQuotaQuery:
		s.MemQuotaQuery = milevadbOptInt64(val, config.GetGlobalConfig().MemQuotaQuery)
	case MilevaDBNestedLoopJoinCacheCapacity:
		s.NestedLoopJoinCacheCapacity = milevadbOptInt64(val, config.GetGlobalConfig().NestedLoopJoinCacheCapacity)
	case MilevaDBMemQuotaHashJoin:
		s.MemQuotaHashJoin = milevadbOptInt64(val, DefMilevaDBMemQuotaHashJoin)
	case MilevaDBMemQuotaMergeJoin:
		s.MemQuotaMergeJoin = milevadbOptInt64(val, DefMilevaDBMemQuotaMergeJoin)
	case MilevaDBMemQuotaSort:
		s.MemQuotaSort = milevadbOptInt64(val, DefMilevaDBMemQuotaSort)
	case MilevaDBMemQuotaTopn:
		s.MemQuotaTopn = milevadbOptInt64(val, DefMilevaDBMemQuotaTopn)
	case MilevaDBMemQuotaIndexLookupReader:
		s.MemQuotaIndexLookupReader = milevadbOptInt64(val, DefMilevaDBMemQuotaIndexLookupReader)
	case MilevaDBMemQuotaIndexLookupJoin:
		s.MemQuotaIndexLookupJoin = milevadbOptInt64(val, DefMilevaDBMemQuotaIndexLookupJoin)
	case MilevaDBMemQuotaNestedLoopApply:
		s.MemQuotaNestedLoopApply = milevadbOptInt64(val, DefMilevaDBMemQuotaNestedLoopApply)
	case MilevaDBGeneralLog:
		atomic.StoreUint32(&ProcessGeneralLog, uint32(milevadbOptPositiveInt32(val, DefMilevaDBGeneralLog)))
	case MilevaDBPProfALLEGROSQLCPU:
		EnablePProfALLEGROSQLCPU.CausetStore(uint32(milevadbOptPositiveInt32(val, DefMilevaDBPProfALLEGROSQLCPU)) > 0)
	case MilevaDBDBSSlowOprThreshold:
		atomic.StoreUint32(&DBSSlowOprThreshold, uint32(milevadbOptPositiveInt32(val, DefMilevaDBDBSSlowOprThreshold)))
	case MilevaDBRetryLimit:
		s.RetryLimit = milevadbOptInt64(val, DefMilevaDBRetryLimit)
	case MilevaDBDisableTxnAutoRetry:
		s.DisableTxnAutoRetry = MilevaDBOptOn(val)
	case MilevaDBEnableStreaming:
		s.EnableStreaming = MilevaDBOptOn(val)
	case MilevaDBEnableChunkRPC:
		s.EnableChunkRPC = MilevaDBOptOn(val)
	case MilevaDBEnableCascadesPlanner:
		s.SetEnableCascadesPlanner(MilevaDBOptOn(val))
	case MilevaDBOptimizerSelectivityLevel:
		s.OptimizerSelectivityLevel = milevadbOptPositiveInt32(val, DefMilevaDBOptimizerSelectivityLevel)
	case MilevaDBEnableBlockPartition:
		s.EnableBlockPartition = val
	case MilevaDBDBSReorgPriority:
		s.setDBSReorgPriority(val)
	case MilevaDBForcePriority:
		atomic.StoreInt32(&ForcePriority, int32(allegrosql.Str2Priority(val)))
	case MilevaDBEnableRadixJoin:
		s.EnableRadixJoin = MilevaDBOptOn(val)
	case MilevaDBEnableWindowFunction:
		s.EnableWindowFunction = MilevaDBOptOn(val)
	case MilevaDBEnableVectorizedExpression:
		s.EnableVectorizedExpression = MilevaDBOptOn(val)
	case MilevaDBOptJoinReorderThreshold:
		s.MilevaDBOptJoinReorderThreshold = milevadbOptPositiveInt32(val, DefMilevaDBOptJoinReorderThreshold)
	case MilevaDBSlowQueryFile:
		s.SlowQueryFile = val
	case MilevaDBEnableFastAnalyze:
		s.EnableFastAnalyze = MilevaDBOptOn(val)
	case MilevaDBWaitSplitRegionFinish:
		s.WaitSplitRegionFinish = MilevaDBOptOn(val)
	case MilevaDBWaitSplitRegionTimeout:
		s.WaitSplitRegionTimeout = uint64(milevadbOptPositiveInt32(val, DefWaitSplitRegionTimeout))
	case MilevaDBExpensiveQueryTimeThreshold:
		atomic.StoreUint64(&ExpensiveQueryTimeThreshold, uint64(milevadbOptPositiveInt32(val, DefMilevaDBExpensiveQueryTimeThreshold)))
	case MilevaDBTxnMode:
		s.TxnMode = strings.ToUpper(val)
	case MilevaDBRowFormatVersion:
		formatVersion := int(milevadbOptInt64(val, DefMilevaDBRowFormatV1))
		if formatVersion == DefMilevaDBRowFormatV1 {
			s.RowEncoder.Enable = false
		} else if formatVersion == DefMilevaDBRowFormatV2 {
			s.RowEncoder.Enable = true
		}
	case MilevaDBLowResolutionTSO:
		s.LowResolutionTSO = MilevaDBOptOn(val)
	case MilevaDBEnableIndexMerge:
		s.SetEnableIndexMerge(MilevaDBOptOn(val))
	case MilevaDBEnableNoopFuncs:
		s.EnableNoopFuncs = MilevaDBOptOn(val)
	case MilevaDBReplicaRead:
		if strings.EqualFold(val, "follower") {
			s.SetReplicaRead(ekv.ReplicaReadFollower)
		} else if strings.EqualFold(val, "leader-and-follower") {
			s.SetReplicaRead(ekv.ReplicaReadMixed)
		} else if strings.EqualFold(val, "leader") || len(val) == 0 {
			s.SetReplicaRead(ekv.ReplicaReadLeader)
		}
	case MilevaDBAllowRemoveAutoInc:
		s.AllowRemoveAutoInc = MilevaDBOptOn(val)
	// It's a global variable, but it also wants to be cached in server.
	case MilevaDBMaxDeltaSchemaCount:
		SetMaxDeltaSchemaCount(milevadbOptInt64(val, DefMilevaDBMaxDeltaSchemaCount))
	case MilevaDBUsePlanBaselines:
		s.UsePlanBaselines = MilevaDBOptOn(val)
	case MilevaDBEvolvePlanBaselines:
		s.EvolvePlanBaselines = MilevaDBOptOn(val)
	case MilevaDBIsolationReadEngines:
		s.IsolationReadEngines = make(map[ekv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			switch engine {
			case ekv.EinsteinDB.Name():
				s.IsolationReadEngines[ekv.EinsteinDB] = struct{}{}
			case ekv.TiFlash.Name():
				s.IsolationReadEngines[ekv.TiFlash] = struct{}{}
			case ekv.MilevaDB.Name():
				s.IsolationReadEngines[ekv.MilevaDB] = struct{}{}
			}
		}
	case MilevaDBStoreLimit:
		storeutil.StoreLimit.CausetStore(milevadbOptInt64(val, DefMilevaDBStoreLimit))
	case MilevaDBMetricSchemaStep:
		s.MetricSchemaStep = milevadbOptInt64(val, DefMilevaDBMetricSchemaStep)
	case MilevaDBMetricSchemaRangeDuration:
		s.MetricSchemaRangeDuration = milevadbOptInt64(val, DefMilevaDBMetricSchemaRangeDuration)
	case DefCauslationConnection, DefCauslationDatabase, DefCauslationServer:
		if _, err := defCauslate.GetDefCauslationByName(val); err != nil {
			var ok bool
			var charsetVal string
			var err2 error
			if name == DefCauslationConnection {
				charsetVal, ok = s.systems[CharacterSetConnection]
			} else if name == DefCauslationDatabase {
				charsetVal, ok = s.systems[CharsetDatabase]
			} else {
				// DefCauslationServer
				charsetVal, ok = s.systems[CharacterSetServer]
			}
			if !ok {
				return err
			}
			val, err2 = charset.GetDefaultDefCauslation(charsetVal)
			if err2 != nil {
				return err2
			}
			logutil.BgLogger().Warn(err.Error())
		}
	case MilevaDBSlowLogThreshold:
		atomic.StoreUint64(&config.GetGlobalConfig().Log.SlowThreshold, uint64(milevadbOptInt64(val, logutil.DefaultSlowThreshold)))
	case MilevaDBRecordPlanInSlowLog:
		atomic.StoreUint32(&config.GetGlobalConfig().Log.RecordPlanInSlowLog, uint32(milevadbOptInt64(val, logutil.DefaultRecordPlanInSlowLog)))
	case MilevaDBEnableSlowLog:
		config.GetGlobalConfig().Log.EnableSlowLog = MilevaDBOptOn(val)
	case MilevaDBQueryLogMaxLen:
		atomic.StoreUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen, uint64(milevadbOptInt64(val, logutil.DefaultQueryLogMaxLen)))
	case MilevaDBCheckMb4ValueInUTF8:
		config.GetGlobalConfig().CheckMb4ValueInUTF8 = MilevaDBOptOn(val)
	case MilevaDBFoundInPlanCache:
		s.FoundInPlanCache = MilevaDBOptOn(val)
	case MilevaDBEnableDefCauslectExecutionInfo:
		config.GetGlobalConfig().EnableDefCauslectExecutionInfo = MilevaDBOptOn(val)
	case ALLEGROSQLSelectLimit:
		result, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		s.SelectLimit = result
	case MilevaDBAllowAutoRandExplicitInsert:
		s.AllowAutoRandExplicitInsert = MilevaDBOptOn(val)
	case MilevaDBEnableClusteredIndex:
		s.EnableClusteredIndex = MilevaDBOptOn(val)
	case MilevaDBPartitionPruneMode:
		s.PartitionPruneMode = PartitionPruneMode(strings.ToLower(strings.TrimSpace(val)))
	case MilevaDBEnableParallelApply:
		s.EnableParallelApply = MilevaDBOptOn(val)
	case MilevaDBSlowLogMasking, MilevaDBRedactLog:
		config.SetRedactLog(MilevaDBOptOn(val))
	case MilevaDBShardAllocateStep:
		s.ShardAllocateStep = milevadbOptInt64(val, DefMilevaDBShardAllocateStep)
	case MilevaDBEnableChangeDeferredCausetType:
		s.EnableChangeDeferredCausetType = MilevaDBOptOn(val)
	case MilevaDBEnableAmendPessimisticTxn:
		s.EnableAmendPessimisticTxn = MilevaDBOptOn(val)
	}
	s.systems[name] = val
	return nil
}

// GetReadableTxnMode returns the stochastik variable TxnMode but rewrites it to "OPTIMISTIC" when it's empty.
func (s *StochastikVars) GetReadableTxnMode() string {
	txnMode := s.TxnMode
	if txnMode == "" {
		txnMode = ast.Optimistic
	}
	return txnMode
}

func (s *StochastikVars) setTxnMode(val string) error {
	switch strings.ToUpper(val) {
	case ast.Pessimistic:
		s.TxnMode = ast.Pessimistic
	case ast.Optimistic:
		s.TxnMode = ast.Optimistic
	case "":
		s.TxnMode = ""
	default:
		return ErrWrongValueForVar.FastGenByArgs(MilevaDBTxnMode, val)
	}
	return nil
}

// SetPrevStmtDigest sets the digest of the previous statement.
func (s *StochastikVars) SetPrevStmtDigest(prevStmtDigest string) {
	s.prevStmtDigest = prevStmtDigest
}

// GetPrevStmtDigest returns the digest of the previous statement.
func (s *StochastikVars) GetPrevStmtDigest() string {
	// Because `prevStmt` may be truncated, so it's senseless to normalize it.
	// Even if `prevStmtDigest` is empty but `prevStmt` is not, just return it anyway.
	return s.prevStmtDigest
}

// LazyCheckKeyNotExists returns if we can lazy check key not exists.
func (s *StochastikVars) LazyCheckKeyNotExists() bool {
	return s.PresumeKeyNotExists || (s.TxnCtx.IsPessimistic && !s.StmtCtx.DupKeyAsWarning)
}

// SetLocalSystemVar sets values of the local variables which in "server" scope.
func SetLocalSystemVar(name string, val string) {
	switch name {
	case MilevaDBDBSReorgWorkerCount:
		SetDBSReorgWorkerCounter(int32(milevadbOptPositiveInt32(val, DefMilevaDBDBSReorgWorkerCount)))
	case MilevaDBDBSReorgBatchSize:
		SetDBSReorgBatchSize(int32(milevadbOptPositiveInt32(val, DefMilevaDBDBSReorgBatchSize)))
	case MilevaDBDBSErrorCountLimit:
		SetDBSErrorCountLimit(milevadbOptInt64(val, DefMilevaDBDBSErrorCountLimit))
	}
}

// special stochastik variables.
const (
	ALLEGROSQLModeVar           = "sql_mode"
	CharacterSetResults  = "character_set_results"
	MaxAllowedPacket     = "max_allowed_packet"
	TimeZone             = "time_zone"
	TxnIsolation         = "tx_isolation"
	TransactionIsolation = "transaction_isolation"
	TxnIsolationOneShot  = "tx_isolation_one_shot"
	MaxExecutionTime     = "max_execution_time"
)

// these variables are useless for MilevaDB, but still need to validate their values for some compatible issues.
// TODO: some more variables need to be added here.
const (
	serverReadOnly = "read_only"
)

var (
	// TxIsolationNames are the valid values of the variable "tx_isolation" or "transaction_isolation".
	TxIsolationNames = map[string]struct{}{
		"READ-UNCOMMITTED": {},
		"READ-COMMITTED":   {},
		"REPEATABLE-READ":  {},
		"SERIALIZABLE":     {},
	}
)

// BlockDelta stands for the changed count for one block or partition.
type BlockDelta struct {
	Delta    int64
	Count    int64
	DefCausSize  map[int64]int64
	InitTime time.Time // InitTime is the time that this delta is generated.
}

// ConcurrencyUnset means the value the of the concurrency related variable is unset.
const ConcurrencyUnset = -1

// Concurrency defines concurrency values.
type Concurrency struct {
	// indexLookupConcurrency is the number of concurrent index lookup worker.
	// indexLookupConcurrency is deprecated, use ExecutorConcurrency instead.
	indexLookupConcurrency int

	// indexLookupJoinConcurrency is the number of concurrent index lookup join inner worker.
	// indexLookupJoinConcurrency is deprecated, use ExecutorConcurrency instead.
	indexLookupJoinConcurrency int

	// distALLEGROSQLScanConcurrency is the number of concurrent dist ALLEGROALLEGROSQL scan worker.
	// distALLEGROSQLScanConcurrency is deprecated, use ExecutorConcurrency instead.
	distALLEGROSQLScanConcurrency int

	// hashJoinConcurrency is the number of concurrent hash join outer worker.
	// hashJoinConcurrency is deprecated, use ExecutorConcurrency instead.
	hashJoinConcurrency int

	// projectionConcurrency is the number of concurrent projection worker.
	// projectionConcurrency is deprecated, use ExecutorConcurrency instead.
	projectionConcurrency int

	// hashAggPartialConcurrency is the number of concurrent hash aggregation partial worker.
	// hashAggPartialConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggPartialConcurrency int

	// hashAggFinalConcurrency is the number of concurrent hash aggregation final worker.
	// hashAggFinalConcurrency is deprecated, use ExecutorConcurrency instead.
	hashAggFinalConcurrency int

	// windowConcurrency is the number of concurrent window worker.
	// windowConcurrency is deprecated, use ExecutorConcurrency instead.
	windowConcurrency int

	// indexSerialScanConcurrency is the number of concurrent index serial scan worker.
	indexSerialScanConcurrency int

	// ExecutorConcurrency is the number of concurrent worker for all executors.
	ExecutorConcurrency int
}

// SetIndexLookupConcurrency set the number of concurrent index lookup worker.
func (c *Concurrency) SetIndexLookupConcurrency(n int) {
	c.indexLookupConcurrency = n
}

// SetIndexLookupJoinConcurrency set the number of concurrent index lookup join inner worker.
func (c *Concurrency) SetIndexLookupJoinConcurrency(n int) {
	c.indexLookupJoinConcurrency = n
}

// SetDistALLEGROSQLScanConcurrency set the number of concurrent dist ALLEGROALLEGROSQL scan worker.
func (c *Concurrency) SetDistALLEGROSQLScanConcurrency(n int) {
	c.distALLEGROSQLScanConcurrency = n
}

// SetHashJoinConcurrency set the number of concurrent hash join outer worker.
func (c *Concurrency) SetHashJoinConcurrency(n int) {
	c.hashJoinConcurrency = n
}

// SetProjectionConcurrency set the number of concurrent projection worker.
func (c *Concurrency) SetProjectionConcurrency(n int) {
	c.projectionConcurrency = n
}

// SetHashAggPartialConcurrency set the number of concurrent hash aggregation partial worker.
func (c *Concurrency) SetHashAggPartialConcurrency(n int) {
	c.hashAggPartialConcurrency = n
}

// SetHashAggFinalConcurrency set the number of concurrent hash aggregation final worker.
func (c *Concurrency) SetHashAggFinalConcurrency(n int) {
	c.hashAggFinalConcurrency = n
}

// SetWindowConcurrency set the number of concurrent window worker.
func (c *Concurrency) SetWindowConcurrency(n int) {
	c.windowConcurrency = n
}

// SetIndexSerialScanConcurrency set the number of concurrent index serial scan worker.
func (c *Concurrency) SetIndexSerialScanConcurrency(n int) {
	c.indexSerialScanConcurrency = n
}

// IndexLookupConcurrency return the number of concurrent index lookup worker.
func (c *Concurrency) IndexLookupConcurrency() int {
	if c.indexLookupConcurrency != ConcurrencyUnset {
		return c.indexLookupConcurrency
	}
	return c.ExecutorConcurrency
}

// IndexLookupJoinConcurrency return the number of concurrent index lookup join inner worker.
func (c *Concurrency) IndexLookupJoinConcurrency() int {
	if c.indexLookupJoinConcurrency != ConcurrencyUnset {
		return c.indexLookupJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// DistALLEGROSQLScanConcurrency return the number of concurrent dist ALLEGROALLEGROSQL scan worker.
func (c *Concurrency) DistALLEGROSQLScanConcurrency() int {
	return c.distALLEGROSQLScanConcurrency
}

// HashJoinConcurrency return the number of concurrent hash join outer worker.
func (c *Concurrency) HashJoinConcurrency() int {
	if c.hashJoinConcurrency != ConcurrencyUnset {
		return c.hashJoinConcurrency
	}
	return c.ExecutorConcurrency
}

// ProjectionConcurrency return the number of concurrent projection worker.
func (c *Concurrency) ProjectionConcurrency() int {
	if c.projectionConcurrency != ConcurrencyUnset {
		return c.projectionConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggPartialConcurrency return the number of concurrent hash aggregation partial worker.
func (c *Concurrency) HashAggPartialConcurrency() int {
	if c.hashAggPartialConcurrency != ConcurrencyUnset {
		return c.hashAggPartialConcurrency
	}
	return c.ExecutorConcurrency
}

// HashAggFinalConcurrency return the number of concurrent hash aggregation final worker.
func (c *Concurrency) HashAggFinalConcurrency() int {
	if c.hashAggFinalConcurrency != ConcurrencyUnset {
		return c.hashAggFinalConcurrency
	}
	return c.ExecutorConcurrency
}

// WindowConcurrency return the number of concurrent window worker.
func (c *Concurrency) WindowConcurrency() int {
	if c.windowConcurrency != ConcurrencyUnset {
		return c.windowConcurrency
	}
	return c.ExecutorConcurrency
}

// IndexSerialScanConcurrency return the number of concurrent index serial scan worker.
// This option is not sync with ExecutorConcurrency since it's used by Analyze block.
func (c *Concurrency) IndexSerialScanConcurrency() int {
	return c.indexSerialScanConcurrency
}

// UnionConcurrency return the num of concurrent union worker.
func (c *Concurrency) UnionConcurrency() int {
	return c.ExecutorConcurrency
}

// MemQuota defines memory quota values.
type MemQuota struct {
	// MemQuotaQuery defines the memory quota for a query.
	MemQuotaQuery int64

	// NestedLoopJoinCacheCapacity defines the memory capacity for apply cache.
	NestedLoopJoinCacheCapacity int64

	// The variables below do not take any effect anymore, it's remaining for compatibility.
	// TODO: remove them in v4.1
	// MemQuotaHashJoin defines the memory quota for a hash join executor.
	MemQuotaHashJoin int64
	// MemQuotaMergeJoin defines the memory quota for a merge join executor.
	MemQuotaMergeJoin int64
	// MemQuotaSort defines the memory quota for a sort executor.
	MemQuotaSort int64
	// MemQuotaTopn defines the memory quota for a top n executor.
	MemQuotaTopn int64
	// MemQuotaIndexLookupReader defines the memory quota for a index lookup reader executor.
	MemQuotaIndexLookupReader int64
	// MemQuotaIndexLookupJoin defines the memory quota for a index lookup join executor.
	MemQuotaIndexLookupJoin int64
	// MemQuotaNestedLoopApply defines the memory quota for a nested loop apply executor.
	MemQuotaNestedLoopApply int64
	// MemQuotaDistALLEGROSQL defines the memory quota for all operators in DistALLEGROSQL layer like co-processor and selectResult.
	MemQuotaDistALLEGROSQL int64
}

// BatchSize defines batch size values.
type BatchSize struct {
	// IndexJoinBatchSize is the batch size of a index lookup join.
	IndexJoinBatchSize int

	// IndexLookupSize is the number of handles for an index lookup task in index double read executor.
	IndexLookupSize int

	// InitChunkSize defines init event count of a Chunk during query execution.
	InitChunkSize int

	// MaxChunkSize defines max event count of a Chunk during query execution.
	MaxChunkSize int
}

const (
	// SlowLogRowPrefixStr is slow log event prefix.
	SlowLogRowPrefixStr = "# "
	// SlowLogSpaceMarkStr is slow log space mark.
	SlowLogSpaceMarkStr = ": "
	// SlowLogALLEGROSQLSuffixStr is slow log suffix.
	SlowLogALLEGROSQLSuffixStr = ";"
	// SlowLogTimeStr is slow log field name.
	SlowLogTimeStr = "Time"
	// SlowLogStartPrefixStr is slow log start event prefix.
	SlowLogStartPrefixStr = SlowLogRowPrefixStr + SlowLogTimeStr + SlowLogSpaceMarkStr
	// SlowLogTxnStartTSStr is slow log field name.
	SlowLogTxnStartTSStr = "Txn_start_ts"
	// SlowLogUserAndHostStr is the user and host field name, which is compatible with MyALLEGROSQL.
	SlowLogUserAndHostStr = "User@Host"
	// SlowLogUserStr is slow log field name.
	SlowLogUserStr = "User"
	// SlowLogHostStr only for slow_query block usage.
	SlowLogHostStr = "Host"
	// SlowLogConnIDStr is slow log field name.
	SlowLogConnIDStr = "Conn_ID"
	// SlowLogQueryTimeStr is slow log field name.
	SlowLogQueryTimeStr = "Query_time"
	// SlowLogParseTimeStr is the parse allegrosql time.
	SlowLogParseTimeStr = "Parse_time"
	// SlowLogCompileTimeStr is the compile plan time.
	SlowLogCompileTimeStr = "Compile_time"
	// SlowLogRewriteTimeStr is the rewrite time.
	SlowLogRewriteTimeStr = "Rewrite_time"
	// SlowLogOptimizeTimeStr is the optimization time.
	SlowLogOptimizeTimeStr = "Optimize_time"
	// SlowLogWaitTSTimeStr is the time of waiting TS.
	SlowLogWaitTSTimeStr = "Wait_TS"
	// SlowLogPreprocSubQueriesStr is the number of pre-processed sub-queries.
	SlowLogPreprocSubQueriesStr = "Preproc_subqueries"
	// SlowLogPreProcSubQueryTimeStr is the total time of pre-processing sub-queries.
	SlowLogPreProcSubQueryTimeStr = "Preproc_subqueries_time"
	// SlowLogDBStr is slow log field name.
	SlowLogDBStr = "EDB"
	// SlowLogIsInternalStr is slow log field name.
	SlowLogIsInternalStr = "Is_internal"
	// SlowLogIndexNamesStr is slow log field name.
	SlowLogIndexNamesStr = "Index_names"
	// SlowLogDigestStr is slow log field name.
	SlowLogDigestStr = "Digest"
	// SlowLogQueryALLEGROSQLStr is slow log field name.
	SlowLogQueryALLEGROSQLStr = "Query" // use for slow log block, slow log will not print this field name but print allegrosql directly.
	// SlowLogStatsInfoStr is plan stats info.
	SlowLogStatsInfoStr = "Stats"
	// SlowLogNumCausetTasksStr is the number of cop-tasks.
	SlowLogNumCausetTasksStr = "Num_cop_tasks"
	// SlowLogCopProcAvg is the average process time of all cop-tasks.
	SlowLogCopProcAvg = "Cop_proc_avg"
	// SlowLogCopProcP90 is the p90 process time of all cop-tasks.
	SlowLogCopProcP90 = "Cop_proc_p90"
	// SlowLogCopProcMax is the max process time of all cop-tasks.
	SlowLogCopProcMax = "Cop_proc_max"
	// SlowLogCopProcAddr is the address of EinsteinDB where the cop-task which cost max process time run.
	SlowLogCopProcAddr = "Cop_proc_addr"
	// SlowLogCopWaitAvg is the average wait time of all cop-tasks.
	SlowLogCopWaitAvg = "Cop_wait_avg"
	// SlowLogCopWaitP90 is the p90 wait time of all cop-tasks.
	SlowLogCopWaitP90 = "Cop_wait_p90"
	// SlowLogCopWaitMax is the max wait time of all cop-tasks.
	SlowLogCopWaitMax = "Cop_wait_max"
	// SlowLogCopWaitAddr is the address of EinsteinDB where the cop-task which cost wait process time run.
	SlowLogCopWaitAddr = "Cop_wait_addr"
	// SlowLogCopBackoffPrefix contains backoff information.
	SlowLogCopBackoffPrefix = "Cop_backoff_"
	// SlowLogMemMax is the max number bytes of memory used in this statement.
	SlowLogMemMax = "Mem_max"
	// SlowLogDiskMax is the nax number bytes of disk used in this statement.
	SlowLogDiskMax = "Disk_max"
	// SlowLogPrepared is used to indicate whether this allegrosql execute in prepare.
	SlowLogPrepared = "Prepared"
	// SlowLogPlanFromCache is used to indicate whether this plan is from plan cache.
	SlowLogPlanFromCache = "Plan_from_cache"
	// SlowLogHasMoreResults is used to indicate whether this allegrosql has more following results.
	SlowLogHasMoreResults = "Has_more_results"
	// SlowLogSucc is used to indicate whether this allegrosql execute successfully.
	SlowLogSucc = "Succ"
	// SlowLogPrevStmt is used to show the previous executed statement.
	SlowLogPrevStmt = "Prev_stmt"
	// SlowLogPlan is used to record the query plan.
	SlowLogPlan = "Plan"
	// SlowLogPlanDigest is used to record the query plan digest.
	SlowLogPlanDigest = "Plan_digest"
	// SlowLogPlanPrefix is the prefix of the plan value.
	SlowLogPlanPrefix = ast.MilevaDBDecodePlan + "('"
	// SlowLogPlanSuffix is the suffix of the plan value.
	SlowLogPlanSuffix = "')"
	// SlowLogPrevStmtPrefix is the prefix of Prev_stmt in slow log file.
	SlowLogPrevStmtPrefix = SlowLogPrevStmt + SlowLogSpaceMarkStr
	// SlowLogKVTotal is the total time waiting for ekv.
	SlowLogKVTotal = "KV_total"
	// SlowLogFIDelTotal is the total time waiting for fidel.
	SlowLogFIDelTotal = "FIDel_total"
	// SlowLogBackoffTotal is the total time doing backoff.
	SlowLogBackoffTotal = "Backoff_total"
	// SlowLogWriteALLEGROSQLRespTotal is the total time used to write response to client.
	SlowLogWriteALLEGROSQLRespTotal = "Write_sql_response_total"
	// SlowLogExecRetryCount is the execution retry count.
	SlowLogExecRetryCount = "Exec_retry_count"
	// SlowLogExecRetryTime is the execution retry time.
	SlowLogExecRetryTime = "Exec_retry_time"
)

// SlowQueryLogItems is a defCauslection of items that should be included in the
// slow query log.
type SlowQueryLogItems struct {
	TxnTS             uint64
	ALLEGROALLEGROSQL               string
	Digest            string
	TimeTotal         time.Duration
	TimeParse         time.Duration
	TimeCompile       time.Duration
	TimeOptimize      time.Duration
	TimeWaitTS        time.Duration
	IndexNames        string
	StatsInfos        map[string]uint64
	CausetTasks          *stmtctx.CausetTasksDetails
	ExecDetail        execdetails.ExecDetails
	MemMax            int64
	DiskMax           int64
	Succ              bool
	Prepared          bool
	PlanFromCache     bool
	HasMoreResults    bool
	PrevStmt          string
	Plan              string
	PlanDigest        string
	RewriteInfo       RewritePhaseInfo
	KVTotal           time.Duration
	FIDelTotal           time.Duration
	BackoffTotal      time.Duration
	WriteALLEGROSQLRespTotal time.Duration
	ExecRetryCount    uint
	ExecRetryTime     time.Duration
}

// SlowLogFormat uses for formatting slow log.
// The slow log output is like below:
// # Time: 2020-04-28T15:24:04.309074+08:00
// # Txn_start_ts: 406315658548871171
// # User@Host: root[root] @ localhost [127.0.0.1]
// # Conn_ID: 6
// # Query_time: 4.895492
// # Process_time: 0.161 Request_count: 1 Total_keys: 100001 Processed_keys: 100000
// # EDB: test
// # Index_names: [t1.idx1,t2.idx2]
// # Is_internal: false
// # Digest: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
// # Stats: t1:1,t2:2
// # Num_cop_tasks: 10
// # Cop_process: Avg_time: 1s P90_time: 2s Max_time: 3s Max_addr: 10.6.131.78
// # Cop_wait: Avg_time: 10ms P90_time: 20ms Max_time: 30ms Max_Addr: 10.6.131.79
// # Memory_max: 4096
// # Disk_max: 65535
// # Succ: true
// # Prev_stmt: begin;
// select * from t_slim;
func (s *StochastikVars) SlowLogFormat(logItems *SlowQueryLogItems) string {
	var buf bytes.Buffer

	writeSlowLogItem(&buf, SlowLogTxnStartTSStr, strconv.FormatUint(logItems.TxnTS, 10))
	if s.User != nil {
		hostAddress := s.User.Hostname
		if s.ConnectionInfo != nil {
			hostAddress = s.ConnectionInfo.ClientIP
		}
		writeSlowLogItem(&buf, SlowLogUserAndHostStr, fmt.Sprintf("%s[%s] @ %s [%s]", s.User.Username, s.User.Username, s.User.Hostname, hostAddress))
	}
	if s.ConnectionID != 0 {
		writeSlowLogItem(&buf, SlowLogConnIDStr, strconv.FormatUint(s.ConnectionID, 10))
	}
	if logItems.ExecRetryCount > 0 {
		buf.WriteString(SlowLogRowPrefixStr)
		buf.WriteString(SlowLogExecRetryTime)
		buf.WriteString(SlowLogSpaceMarkStr)
		buf.WriteString(strconv.FormatFloat(logItems.ExecRetryTime.Seconds(), 'f', -1, 64))
		buf.WriteString(" ")
		buf.WriteString(SlowLogExecRetryCount)
		buf.WriteString(SlowLogSpaceMarkStr)
		buf.WriteString(strconv.Itoa(int(logItems.ExecRetryCount)))
		buf.WriteString("\n")
	}
	writeSlowLogItem(&buf, SlowLogQueryTimeStr, strconv.FormatFloat(logItems.TimeTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogParseTimeStr, strconv.FormatFloat(logItems.TimeParse.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogCompileTimeStr, strconv.FormatFloat(logItems.TimeCompile.Seconds(), 'f', -1, 64))

	buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v", SlowLogRewriteTimeStr,
		SlowLogSpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationRewrite.Seconds(), 'f', -1, 64)))
	if logItems.RewriteInfo.PreprocessSubQueries > 0 {
		buf.WriteString(fmt.Sprintf(" %v%v%v %v%v%v", SlowLogPreprocSubQueriesStr, SlowLogSpaceMarkStr, logItems.RewriteInfo.PreprocessSubQueries,
			SlowLogPreProcSubQueryTimeStr, SlowLogSpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationPreprocessSubQuery.Seconds(), 'f', -1, 64)))
	}
	buf.WriteString("\n")

	writeSlowLogItem(&buf, SlowLogOptimizeTimeStr, strconv.FormatFloat(logItems.TimeOptimize.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogWaitTSTimeStr, strconv.FormatFloat(logItems.TimeWaitTS.Seconds(), 'f', -1, 64))

	if execDetailStr := logItems.ExecDetail.String(); len(execDetailStr) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + execDetailStr + "\n")
	}

	if len(s.CurrentDB) > 0 {
		writeSlowLogItem(&buf, SlowLogDBStr, s.CurrentDB)
	}
	if len(logItems.IndexNames) > 0 {
		writeSlowLogItem(&buf, SlowLogIndexNamesStr, logItems.IndexNames)
	}

	writeSlowLogItem(&buf, SlowLogIsInternalStr, strconv.FormatBool(s.InRestrictedALLEGROSQL))
	if len(logItems.Digest) > 0 {
		writeSlowLogItem(&buf, SlowLogDigestStr, logItems.Digest)
	}
	if len(logItems.StatsInfos) > 0 {
		buf.WriteString(SlowLogRowPrefixStr + SlowLogStatsInfoStr + SlowLogSpaceMarkStr)
		firstComma := false
		vStr := ""
		for k, v := range logItems.StatsInfos {
			if v == 0 {
				vStr = "pseudo"
			} else {
				vStr = strconv.FormatUint(v, 10)

			}
			if firstComma {
				buf.WriteString("," + k + ":" + vStr)
			} else {
				buf.WriteString(k + ":" + vStr)
				firstComma = true
			}
		}
		buf.WriteString("\n")
	}
	if logItems.CausetTasks != nil {
		writeSlowLogItem(&buf, SlowLogNumCausetTasksStr, strconv.FormatInt(int64(logItems.CausetTasks.NumCausetTasks), 10))
		if logItems.CausetTasks.NumCausetTasks > 0 {
			// make the result sblock
			backoffs := make([]string, 0, 3)
			for backoff := range logItems.CausetTasks.TotBackoffTimes {
				backoffs = append(backoffs, backoff)
			}
			sort.Strings(backoffs)

			if logItems.CausetTasks.NumCausetTasks == 1 {
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v",
					SlowLogCopProcAvg, SlowLogSpaceMarkStr, logItems.CausetTasks.AvgProcessTime.Seconds(),
					SlowLogCopProcAddr, SlowLogSpaceMarkStr, logItems.CausetTasks.MaxProcessAddress) + "\n")
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v",
					SlowLogCopWaitAvg, SlowLogSpaceMarkStr, logItems.CausetTasks.AvgWaitTime.Seconds(),
					SlowLogCopWaitAddr, SlowLogSpaceMarkStr, logItems.CausetTasks.MaxWaitAddress) + "\n")
				for _, backoff := range backoffs {
					backoffPrefix := SlowLogCopBackoffPrefix + backoff + "_"
					buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v\n",
						backoffPrefix+"total_times", SlowLogSpaceMarkStr, logItems.CausetTasks.TotBackoffTimes[backoff],
						backoffPrefix+"total_time", SlowLogSpaceMarkStr, logItems.CausetTasks.TotBackoffTime[backoff].Seconds(),
					))
				}
			} else {
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v",
					SlowLogCopProcAvg, SlowLogSpaceMarkStr, logItems.CausetTasks.AvgProcessTime.Seconds(),
					SlowLogCopProcP90, SlowLogSpaceMarkStr, logItems.CausetTasks.P90ProcessTime.Seconds(),
					SlowLogCopProcMax, SlowLogSpaceMarkStr, logItems.CausetTasks.MaxProcessTime.Seconds(),
					SlowLogCopProcAddr, SlowLogSpaceMarkStr, logItems.CausetTasks.MaxProcessAddress) + "\n")
				buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v",
					SlowLogCopWaitAvg, SlowLogSpaceMarkStr, logItems.CausetTasks.AvgWaitTime.Seconds(),
					SlowLogCopWaitP90, SlowLogSpaceMarkStr, logItems.CausetTasks.P90WaitTime.Seconds(),
					SlowLogCopWaitMax, SlowLogSpaceMarkStr, logItems.CausetTasks.MaxWaitTime.Seconds(),
					SlowLogCopWaitAddr, SlowLogSpaceMarkStr, logItems.CausetTasks.MaxWaitAddress) + "\n")
				for _, backoff := range backoffs {
					backoffPrefix := SlowLogCopBackoffPrefix + backoff + "_"
					buf.WriteString(SlowLogRowPrefixStr + fmt.Sprintf("%v%v%v %v%v%v %v%v%v %v%v%v %v%v%v %v%v%v\n",
						backoffPrefix+"total_times", SlowLogSpaceMarkStr, logItems.CausetTasks.TotBackoffTimes[backoff],
						backoffPrefix+"total_time", SlowLogSpaceMarkStr, logItems.CausetTasks.TotBackoffTime[backoff].Seconds(),
						backoffPrefix+"max_time", SlowLogSpaceMarkStr, logItems.CausetTasks.MaxBackoffTime[backoff].Seconds(),
						backoffPrefix+"max_addr", SlowLogSpaceMarkStr, logItems.CausetTasks.MaxBackoffAddress[backoff],
						backoffPrefix+"avg_time", SlowLogSpaceMarkStr, logItems.CausetTasks.AvgBackoffTime[backoff].Seconds(),
						backoffPrefix+"p90_time", SlowLogSpaceMarkStr, logItems.CausetTasks.P90BackoffTime[backoff].Seconds(),
					))
				}
			}
		}
	}
	if logItems.MemMax > 0 {
		writeSlowLogItem(&buf, SlowLogMemMax, strconv.FormatInt(logItems.MemMax, 10))
	}
	if logItems.DiskMax > 0 {
		writeSlowLogItem(&buf, SlowLogDiskMax, strconv.FormatInt(logItems.DiskMax, 10))
	}

	writeSlowLogItem(&buf, SlowLogPrepared, strconv.FormatBool(logItems.Prepared))
	writeSlowLogItem(&buf, SlowLogPlanFromCache, strconv.FormatBool(logItems.PlanFromCache))
	writeSlowLogItem(&buf, SlowLogHasMoreResults, strconv.FormatBool(logItems.HasMoreResults))
	writeSlowLogItem(&buf, SlowLogKVTotal, strconv.FormatFloat(logItems.KVTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogFIDelTotal, strconv.FormatFloat(logItems.FIDelTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogBackoffTotal, strconv.FormatFloat(logItems.BackoffTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogWriteALLEGROSQLRespTotal, strconv.FormatFloat(logItems.WriteALLEGROSQLRespTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, SlowLogSucc, strconv.FormatBool(logItems.Succ))
	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, SlowLogPlan, logItems.Plan)
	}
	if len(logItems.PlanDigest) != 0 {
		writeSlowLogItem(&buf, SlowLogPlanDigest, logItems.PlanDigest)
	}

	if logItems.PrevStmt != "" {
		writeSlowLogItem(&buf, SlowLogPrevStmt, logItems.PrevStmt)
	}

	if s.CurrentDBChanged {
		buf.WriteString(fmt.Sprintf("use %s;\n", s.CurrentDB))
		s.CurrentDBChanged = false
	}

	buf.WriteString(logItems.ALLEGROALLEGROSQL)
	if len(logItems.ALLEGROALLEGROSQL) == 0 || logItems.ALLEGROALLEGROSQL[len(logItems.ALLEGROALLEGROSQL)-1] != ';' {
		buf.WriteString(";")
	}
	return buf.String()
}

// writeSlowLogItem writes a slow log item in the form of: "# ${key}:${value}"
func writeSlowLogItem(buf *bytes.Buffer, key, value string) {
	buf.WriteString(SlowLogRowPrefixStr + key + SlowLogSpaceMarkStr + value + "\n")
}
