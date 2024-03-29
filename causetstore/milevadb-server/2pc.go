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

package einsteindb

import (
	"bytes"
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	pb "github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/fidelpb/go-binlog"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"go.uber.org/zap"
)

type twoPhaseCommitCausetAction interface {
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, batchMutations) error
	EinsteinDBTxnRegionsNumHistogram() prometheus.Observer
	String() string
}

var (
	einsteindbSecondaryLockCleanupFailureCounterRollback = metrics.EinsteinDBSecondaryLockCleanupFailureCounter.WithLabelValues("rollback")
	EinsteinDBTxnHeartBeatHistogramOK                    = metrics.EinsteinDBTxnHeartBeatHistogram.WithLabelValues("ok")
	EinsteinDBTxnHeartBeatHistogramError                 = metrics.EinsteinDBTxnHeartBeatHistogram.WithLabelValues("err")
)

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

// metricsTag returns detail tag for metrics.
func metricsTag(action string) string {
	return "2pc_" + action
}

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	causetstore         *einsteindbStore
	txn                 *einsteindbTxn
	startTS             uint64
	mutations           CommitterMutations
	lockTTL             uint64
	commitTS            uint64
	priority            pb.CommandPri
	connID              uint64 // connID is used for log.
	cleanWg             sync.WaitGroup
	detail              unsafe.Pointer
	txnSize             int
	hasNoNeedCommitKeys bool

	primaryKey     []byte
	forUFIDelateTS uint64

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	syncLog bool
	// For pessimistic transaction
	isPessimistic bool
	isFirstLock   bool
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
	// Used by pessimistic transaction and large transaction.
	ttlManager

	testingKnobs struct {
		acAfterCommitPrimary chan struct{}
		bkAfterCommitPrimary chan struct{}
		noFallBack           bool
	}

	useAsyncCommit uint32
	minCommitTS    uint64
}

// CommitterMutations contains transaction operations.
type CommitterMutations struct {
	ops               []pb.Op
	keys              [][]byte
	values            [][]byte
	isPessimisticLock []bool
}

// NewCommiterMutations creates a CommitterMutations object with sizeHint reserved.
func NewCommiterMutations(sizeHint int) CommitterMutations {
	return CommitterMutations{
		ops:               make([]pb.Op, 0, sizeHint),
		keys:              make([][]byte, 0, sizeHint),
		values:            make([][]byte, 0, sizeHint),
		isPessimisticLock: make([]bool, 0, sizeHint),
	}
}

func (c *CommitterMutations) subRange(from, to int) CommitterMutations {
	var res CommitterMutations
	res.keys = c.keys[from:to]
	if c.ops != nil {
		res.ops = c.ops[from:to]
	}
	if c.values != nil {
		res.values = c.values[from:to]
	}
	if c.isPessimisticLock != nil {
		res.isPessimisticLock = c.isPessimisticLock[from:to]
	}
	return res
}

// Push another mutation into mutations.
func (c *CommitterMutations) Push(op pb.Op, key []byte, value []byte, isPessimisticLock bool) {
	c.ops = append(c.ops, op)
	c.keys = append(c.keys, key)
	c.values = append(c.values, value)
	c.isPessimisticLock = append(c.isPessimisticLock, isPessimisticLock)
}

func (c *CommitterMutations) len() int {
	return len(c.keys)
}

// GetKeys returns the keys.
func (c *CommitterMutations) GetKeys() [][]byte {
	return c.keys
}

// GetOps returns the key ops.
func (c *CommitterMutations) GetOps() []pb.Op {
	return c.ops
}

// GetValues returns the key values.
func (c *CommitterMutations) GetValues() [][]byte {
	return c.values
}

// GetPessimisticFlags returns the key pessimistic flags.
func (c *CommitterMutations) GetPessimisticFlags() []bool {
	return c.isPessimisticLock
}

// MergeMutations append input mutations into current mutations.
func (c *CommitterMutations) MergeMutations(mutations CommitterMutations) {
	c.ops = append(c.ops, mutations.ops...)
	c.keys = append(c.keys, mutations.keys...)
	c.values = append(c.values, mutations.values...)
	c.isPessimisticLock = append(c.isPessimisticLock, mutations.isPessimisticLock...)
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *einsteindbTxn, connID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		causetstore:   txn.causetstore,
		txn:           txn,
		startTS:       txn.StartTS(),
		connID:        connID,
		regionTxnSize: map[uint64]int{},
		ttlManager: ttlManager{
			ch: make(chan struct{}),
		},
		isPessimistic: txn.IsPessimistic(),
	}, nil
}

func (c *twoPhaseCommitter) extractKeyExistsErr(key ekv.Key) error {
	if !c.txn.us.HasPresumeKeyNotExists(key) {
		return errors.Errorf("conn %d, existErr for key:%s should not be nil", c.connID, key)
	}

	_, handle, err := blockcodec.DecodeRecordKey(key)
	if err == nil {
		if handle.IsInt() {
			return ekv.ErrKeyExists.FastGenByArgs(handle.String(), "PRIMARY")
		}
		trimLen := 0
		for i := 0; i < handle.NumDefCauss(); i++ {
			trimLen += len(handle.EncodedDefCaus(i))
		}
		values, err := blockcodec.DecodeValuesBytesToStrings(handle.Encoded()[:trimLen])
		if err == nil {
			return ekv.ErrKeyExists.FastGenByArgs(strings.Join(values, "-"), "PRIMARY")
		}
	}

	blockID, indexID, indexValues, err := blockcodec.DecodeIndexKey(key)
	if err == nil {
		return ekv.ErrKeyExists.FastGenByArgs(strings.Join(indexValues, "-"), c.txn.us.GetIndexName(blockID, indexID))
	}

	return ekv.ErrKeyExists.FastGenByArgs(key.String(), "UNKNOWN")
}

func (c *twoPhaseCommitter) initKeysAndMutations() error {
	var size, putCnt, delCnt, lockCnt, checkCnt int

	txn := c.txn
	memBuf := txn.GetMemBuffer()
	sizeHint := txn.us.GetMemBuffer().Len()
	mutations := NewCommiterMutations(sizeHint)
	c.isPessimistic = txn.IsPessimistic()

	var err error
	for it := memBuf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		key := it.Key()
		flags := it.Flags()
		var value []byte
		var op pb.Op

		if !it.HasValue() {
			if !flags.HasLocked() {
				continue
			}
			op = pb.Op_Lock
			lockCnt++
		} else {
			value = it.Value()
			if len(value) > 0 {
				if blockcodec.IsUntouchedIndexKValue(key, value) {
					continue
				}
				op = pb.Op_Put
				if flags.HasPresumeKeyNotExists() {
					op = pb.Op_Insert
				}
				putCnt++
			} else {
				if !txn.IsPessimistic() && flags.HasPresumeKeyNotExists() {
					// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
					// due to `Op_CheckNotExists` doesn't prewrite dagger, so mark those keys should not be used in commit-phase.
					op = pb.Op_CheckNotExists
					checkCnt++
					memBuf.UFIDelateFlags(key, ekv.SetNoNeedCommit)
				} else {
					// normal delete keys in optimistic txn can be delete without not exists checking
					// delete-your-writes keys in pessimistic txn can ensure must be no exists so can directly delete them
					op = pb.Op_Del
					delCnt++
				}
			}
		}

		var isPessimistic bool
		if flags.HasLocked() {
			isPessimistic = c.isPessimistic
		}
		mutations.Push(op, key, value, isPessimistic)
		size += len(key) + len(value)

		if len(c.primaryKey) == 0 && op != pb.Op_CheckNotExists {
			c.primaryKey = key
		}
	}

	if mutations.len() == 0 {
		return nil
	}
	c.txnSize = size

	if size > int(ekv.TxnTotalSizeLimit) {
		return ekv.ErrTxnTooLarge.GenWithStackByArgs(size)
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if mutations.len() > logEntryCount || size > logSize {
		blockID := blockcodec.DecodeTableID(mutations.keys[0])
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("con", c.connID),
			zap.Int64("causet ID", blockID),
			zap.Int("size", size),
			zap.Int("keys", mutations.len()),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Int("checks", checkCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	commitDetail := &execdetails.CommitDetails{WriteSize: size, WriteKeys: mutations.len()}
	metrics.EinsteinDBTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.EinsteinDBTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	c.hasNoNeedCommitKeys = checkCnt > 0
	c.mutations = mutations
	c.lockTTL = txnLockTTL(txn.startTime, size)
	c.priority = getTxnPriority(txn)
	c.syncLog = getTxnSyncLog(txn)
	c.setDetail(commitDetail)
	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		return c.mutations.keys[0]
	}
	return c.primaryKey
}

// asyncSecondaries returns all keys that must be checked in the recovery phase of an async commit.
func (c *twoPhaseCommitter) asyncSecondaries() [][]byte {
	secondaries := make([][]byte, 0, len(c.mutations.keys))
	for i, k := range c.mutations.keys {
		if bytes.Equal(k, c.primary()) || c.mutations.ops[i] == pb.Op_CheckNotExists {
			continue
		}
		secondaries = append(secondaries, k)
	}
	return secondaries
}

const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 4MiB, or 10MiB, ttl is 6s, 12s, 20s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > ManagedLockTTL {
			lockTTL = ManagedLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a dagger, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

var preSplitDetectThreshold uint32 = 100000
var preSplitSizeThreshold uint32 = 32 << 20

// doCausetActionOnMutations groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
func (c *twoPhaseCommitter) doCausetActionOnMutations(bo *Backoffer, action twoPhaseCommitCausetAction, mutations CommitterMutations) error {
	if mutations.len() == 0 {
		return nil
	}
	groups, err := c.groupMutations(bo, mutations)
	if err != nil {
		return errors.Trace(err)
	}

	return c.doCausetActionOnGroupMutations(bo, action, groups)
}

// groupMutations groups mutations by region, then checks for any large groups and in that case pre-splits the region.
func (c *twoPhaseCommitter) groupMutations(bo *Backoffer, mutations CommitterMutations) ([]groupedMutations, error) {
	groups, err := c.causetstore.regionCache.GroupSortedMutationsByRegion(bo, mutations)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Pre-split regions to avoid too much write workload into a single region.
	// In the large transaction case, this operation is important to avoid EinsteinDB 'server is busy' error.
	var didPreSplit bool
	preSplitDetectThresholdVal := atomic.LoadUint32(&preSplitDetectThreshold)
	for _, group := range groups {
		if uint32(group.mutations.len()) >= preSplitDetectThresholdVal {
			logutil.BgLogger().Info("2PC detect large amount of mutations on a single region",
				zap.Uint64("region", group.region.GetID()),
				zap.Int("mutations count", group.mutations.len()))
			// Use context.Background, this time should not add up to Backoffer.
			if c.causetstore.preSplitRegion(context.Background(), group) {
				didPreSplit = true
			}
		}
	}
	// Reload region cache again.
	if didPreSplit {
		groups, err = c.causetstore.regionCache.GroupSortedMutationsByRegion(bo, mutations)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return groups, nil
}

// doCausetActionOnGroupedMutations splits groups into batches (there is one group per region, and potentially many batches per group, but all mutations
// in a batch will belong to the same region).
func (c *twoPhaseCommitter) doCausetActionOnGroupMutations(bo *Backoffer, action twoPhaseCommitCausetAction, groups []groupedMutations) error {
	action.EinsteinDBTxnRegionsNumHistogram().Observe(float64(len(groups)))

	var sizeFunc = c.keySize

	switch act := action.(type) {
	case actionPrewrite:
		// Do not uFIDelate regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if len(bo.errors) == 0 {
			for _, group := range groups {
				c.regionTxnSize[group.region.id] = group.mutations.len()
			}
		}
		sizeFunc = c.keyValueSize
		atomic.AddInt32(&c.getDetail().PrewriteRegionNum, int32(len(groups)))
	case actionPessimisticLock:
		if act.LockCtx.Stats != nil {
			act.LockCtx.Stats.RegionNum = int32(len(groups))
		}
	}

	batchBuilder := newBatched(c.primary())
	for _, group := range groups {
		batchBuilder.appendBatchMutationsBySize(group.region, group.mutations, sizeFunc, txnCommitBatchSize)
	}
	firstIsPrimary := batchBuilder.setPrimary()

	actionCommit, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	_, actionIsPessimiticLock := action.(actionPessimisticLock)

	var err error
	failpoint.Inject("skipKeyReturnOK", func(val failpoint.Value) {
		valStr, ok := val.(string)
		if ok && c.connID > 0 {
			if firstIsPrimary && actionIsPessimiticLock {
				logutil.Logger(bo.ctx).Warn("pessimisticLock failpoint", zap.String("valStr", valStr))
				switch valStr {
				case "pessimisticLockSkipPrimary":
					err = c.doCausetActionOnBatches(bo, action, batchBuilder.allBatches())
					failpoint.Return(err)
				case "pessimisticLockSkipSecondary":
					err = c.doCausetActionOnBatches(bo, action, batchBuilder.primaryBatch())
					failpoint.Return(err)
				}
			}
		}
	})
	failpoint.Inject("pessimisticRollbackDoNth", func() {
		_, actionIsPessimisticRollback := action.(actionPessimisticRollback)
		if actionIsPessimisticRollback && c.connID > 0 {
			logutil.Logger(bo.ctx).Warn("pessimisticRollbackDoNth failpoint")
			failpoint.Return(nil)
		}
	})

	if firstIsPrimary &&
		((actionIsCommit && !c.isAsyncCommit()) || actionIsCleanup || actionIsPessimiticLock) {
		// primary should be committed(not async commit)/cleanup/pessimistically locked first
		err = c.doCausetActionOnBatches(bo, action, batchBuilder.primaryBatch())
		if err != nil {
			return errors.Trace(err)
		}
		if actionIsCommit && c.testingKnobs.bkAfterCommitPrimary != nil && c.testingKnobs.acAfterCommitPrimary != nil {
			c.testingKnobs.acAfterCommitPrimary <- struct{}{}
			<-c.testingKnobs.bkAfterCommitPrimary
		}
		batchBuilder.forgetPrimary()
	}
	// Already spawned a goroutine for async commit transaction.
	if actionIsCommit && !actionCommit.retry && !c.isAsyncCommit() {
		secondaryBo := NewBackofferWithVars(context.Background(), int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
		go func() {
			e := c.doCausetActionOnBatches(secondaryBo, action, batchBuilder.allBatches())
			if e != nil {
				logutil.BgLogger().Debug("2PC async doCausetActionOnBatches",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e))
				einsteindbSecondaryLockCleanupFailureCounterCommit.Inc()
			}
		}()
	} else {
		err = c.doCausetActionOnBatches(bo, action, batchBuilder.allBatches())
	}
	return errors.Trace(err)
}

// doCausetActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doCausetActionOnBatches(bo *Backoffer, action twoPhaseCommitCausetAction, batches []batchMutations) error {
	if len(batches) == 0 {
		return nil
	}

	noNeedFork := len(batches) == 1
	if !noNeedFork {
		if ac, ok := action.(actionCommit); ok && ac.retry {
			noNeedFork = true
		}
	}
	if noNeedFork {
		for _, b := range batches {
			e := action.handleSingleBatch(c, bo, b)
			if e != nil {
				logutil.BgLogger().Debug("2PC doCausetActionOnBatches failed",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e),
					zap.Uint64("txnStartTS", c.startTS))
				return errors.Trace(e)
			}
		}
		return nil
	}
	rateLim := len(batches)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, einsteindb will report service is busy.
	// If the rate limit is too low, we can't full utilize the einsteindb's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > config.GetGlobalConfig().Performance.CommitterConcurrency {
		rateLim = config.GetGlobalConfig().Performance.CommitterConcurrency
	}
	batchInterlockingDirectorate := newBatchInterlockingDirectorate(rateLim, c, action, bo)
	err := batchInterlockingDirectorate.process(batches)
	return errors.Trace(err)
}

func (c *twoPhaseCommitter) keyValueSize(key, value []byte) int {
	return len(key) + len(value)
}

func (c *twoPhaseCommitter) keySize(key, value []byte) int {
	return len(key)
}

type ttlManagerState uint32

const (
	stateUninitialized ttlManagerState = iota
	stateRunning
	stateClosed
)

type ttlManager struct {
	state   ttlManagerState
	ch      chan struct{}
	lockCtx *ekv.LockCtx
}

func (tm *ttlManager) run(c *twoPhaseCommitter, lockCtx *ekv.LockCtx) {
	// Run only once.
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateUninitialized), uint32(stateRunning)) {
		return
	}
	tm.lockCtx = lockCtx
	go tm.keepAlive(c)
}

func (tm *ttlManager) close() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateClosed)) {
		return
	}
	close(tm.ch)
}

func (tm *ttlManager) keepAlive(c *twoPhaseCommitter) {
	// Ticker is set to 1/2 of the ManagedLockTTL.
	ticker := time.NewTicker(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond / 2)
	defer ticker.Stop()
	for {
		select {
		case <-tm.ch:
			return
		case <-ticker.C:
			// If kill signal is received, the ttlManager should exit.
			if tm.lockCtx != nil && tm.lockCtx.Killed != nil && atomic.LoadUint32(tm.lockCtx.Killed) != 0 {
				return
			}
			bo := NewBackofferWithVars(context.Background(), pessimisticLockMaxBackoff, c.txn.vars)
			now, err := c.causetstore.GetOracle().GetTimestamp(bo.ctx)
			if err != nil {
				err1 := bo.Backoff(BoFIDelRPC, err)
				if err1 != nil {
					logutil.Logger(bo.ctx).Warn("keepAlive get tso fail",
						zap.Error(err))
					return
				}
				continue
			}

			uptime := uint64(oracle.ExtractPhysical(now) - oracle.ExtractPhysical(c.startTS))
			if uptime > config.GetGlobalConfig().Performance.MaxTxnTTL {
				// Checks maximum lifetime for the ttlManager, so when something goes wrong
				// the key will not be locked forever.
				logutil.Logger(bo.ctx).Info("ttlManager live up to its lifetime",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Uint64("uptime", uptime),
					zap.Uint64("maxTxnTTL", config.GetGlobalConfig().Performance.MaxTxnTTL))
				metrics.EinsteinDBTTLLifeTimeReachCounter.Inc()
				// the pessimistic locks may expire if the ttl manager has timed out, set `LockExpired` flag
				// so that this transaction could only commit or rollback with no more memex executions
				if c.isPessimistic && tm.lockCtx != nil && tm.lockCtx.LockExpired != nil {
					atomic.StoreUint32(tm.lockCtx.LockExpired, 1)
				}
				return
			}

			newTTL := uptime + atomic.LoadUint64(&ManagedLockTTL)
			logutil.Logger(bo.ctx).Info("send TxnHeartBeat",
				zap.Uint64("startTS", c.startTS), zap.Uint64("newTTL", newTTL))
			startTime := time.Now()
			_, err = sendTxnHeartBeat(bo, c.causetstore, c.primary(), c.startTS, newTTL)
			if err != nil {
				EinsteinDBTxnHeartBeatHistogramError.Observe(time.Since(startTime).Seconds())
				logutil.Logger(bo.ctx).Warn("send TxnHeartBeat failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				return
			}
			EinsteinDBTxnHeartBeatHistogramOK.Observe(time.Since(startTime).Seconds())
		}
	}
}

func sendTxnHeartBeat(bo *Backoffer, causetstore *einsteindbStore, primary []byte, startTS, ttl uint64) (uint64, error) {
	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdTxnHeartBeat, &pb.TxnHeartBeatRequest{
		PrimaryLock:   primary,
		StartVersion:  startTS,
		AdviseLockTtl: ttl,
	})
	for {
		loc, err := causetstore.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return 0, errors.Trace(err)
		}
		resp, err := causetstore.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return 0, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return 0, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*pb.TxnHeartBeatResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return 0, errors.Errorf("txn %d heartbeat fail, primary key = %v, err = %s", startTS, primary, keyErr.Abort)
		}
		return cmdResp.GetLockTtl(), nil
	}
}

// checkAsyncCommit checks if async commit protocol is available for current transaction commit, true is returned if possible.
func (c *twoPhaseCommitter) checkAsyncCommit() bool {
	// TODO the keys limit need more tests, this value makes the unit test pass by now.
	// Async commit is not compatible with Binlog because of the non unique timestamp issue.
	if c.connID > 0 && config.GetGlobalConfig().EinsteinDBClient.EnableAsyncCommit &&
		uint(len(c.mutations.keys)) <= config.GetGlobalConfig().EinsteinDBClient.AsyncCommitKeysLimit &&
		!c.shouldWriteBinlog() {
		return true
	}
	return false
}

func (c *twoPhaseCommitter) isAsyncCommit() bool {
	return atomic.LoadUint32(&c.useAsyncCommit) > 0
}

func (c *twoPhaseCommitter) setAsyncCommit(val bool) {
	if val {
		atomic.StoreUint32(&c.useAsyncCommit, 1)
	} else {
		atomic.StoreUint32(&c.useAsyncCommit, 0)
	}
}

func (c *twoPhaseCommitter) cleanup(ctx context.Context) {
	c.cleanWg.Add(1)
	go func() {
		cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
		err := c.cleanupMutations(NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
		if err != nil {
			einsteindbSecondaryLockCleanupFailureCounterRollback.Inc()
			logutil.Logger(ctx).Info("2PC cleanup failed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
		} else {
			logutil.Logger(ctx).Info("2PC clean up done",
				zap.Uint64("txnStartTS", c.startTS))
		}
		c.cleanWg.Done()
	}()
}

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	var binlogSkipped bool
	defer func() {
		if !c.isAsyncCommit() {
			// Always clean up all written keys if the txn does not commit.
			c.mu.RLock()
			committed := c.mu.committed
			undetermined := c.mu.undeterminedErr != nil
			c.mu.RUnlock()
			if !committed && !undetermined {
				c.cleanup(ctx)
			}
			c.txn.commitTS = c.commitTS
			if binlogSkipped {
				binloginfo.RemoveOneSkippedCommitter()
			} else {
				if err != nil {
					c.writeFinishBinlog(ctx, binlog.BinlogType_Rollback, 0)
				} else {
					c.writeFinishBinlog(ctx, binlog.BinlogType_Commit, int64(c.commitTS))
				}
			}
		} else {
			// The error means the async commit should not succeed.
			if err != nil {
				c.cleanup(ctx)
			}
		}
	}()

	// Check async commit is available or not.
	if c.checkAsyncCommit() {
		c.setAsyncCommit(true)
	}

	binlogChan := c.prewriteBinlog(ctx)
	prewriteBo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
	start := time.Now()
	err = c.prewriteMutations(prewriteBo, c.mutations)
	commitDetail := c.getDetail()
	commitDetail.PrewriteTime = time.Since(start)
	if prewriteBo.totalSleep > 0 {
		atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(prewriteBo.totalSleep)*int64(time.Millisecond))
		commitDetail.Mu.Lock()
		commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, prewriteBo.types...)
		commitDetail.Mu.Unlock()
	}
	if binlogChan != nil {
		startWaitBinlog := time.Now()
		binlogWriteResult := <-binlogChan
		commitDetail.WaitPrewriteBinlogTime = time.Since(startWaitBinlog)
		if binlogWriteResult != nil {
			binlogSkipped = binlogWriteResult.Skipped()
			binlogErr := binlogWriteResult.GetError()
			if binlogErr != nil {
				return binlogErr
			}
		}
	}
	if err != nil {
		logutil.Logger(ctx).Debug("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	// strip check_not_exists keys that no need to commit.
	c.stripNoNeedCommitKeys()

	var commitTS uint64
	if c.isAsyncCommit() {
		if c.minCommitTS == 0 {
			err = errors.Errorf("conn %d invalid minCommitTS for async commit protocol after prewrite, startTS=%v", c.connID, c.startTS)
			return errors.Trace(err)
		}
		commitTS = c.minCommitTS
	} else {
		start = time.Now()
		logutil.Event(ctx, "start get commit ts")
		commitTS, err = c.causetstore.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars))
		if err != nil {
			logutil.Logger(ctx).Warn("2PC get commitTS failed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		commitDetail.GetCommitTsTime = time.Since(start)
		logutil.Event(ctx, "finish get commit ts")
		logutil.SetTag(ctx, "commitTs", commitTS)
	}

	tryAmend := c.isPessimistic && c.connID > 0 && !c.isAsyncCommit() && c.txn.schemaAmender != nil
	if !tryAmend {
		_, _, err = c.checkSchemaValid(ctx, commitTS, c.txn.txnSchemaReplicant, false)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		relatedSchemaChange, memAmended, err := c.checkSchemaValid(ctx, commitTS, c.txn.txnSchemaReplicant, true)
		if err != nil {
			return errors.Trace(err)
		}
		if memAmended {
			// Get new commitTS and check schemaReplicant valid again.
			newCommitTS, err := c.getCommitTS(ctx, commitDetail)
			if err != nil {
				return errors.Trace(err)
			}
			// If schemaReplicant check failed between commitTS and newCommitTs, report schemaReplicant change error.
			_, _, err = c.checkSchemaValid(ctx, newCommitTS, relatedSchemaChange.LatestSchemaReplicant, false)
			if err != nil {
				return errors.Trace(err)
			}
			commitTS = newCommitTS
		}
	}
	c.commitTS = commitTS

	if c.causetstore.oracle.IsExpired(c.startTS, ekv.MaxTxnTimeUse) {
		err = errors.Errorf("conn %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return err
	}

	if c.connID > 0 {
		failpoint.Inject("beforeCommit", func() {})
	}

	if c.isAsyncCommit() {
		// For async commit protocol, the commit is considered success here.
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Info("2PC will use async commit protocol to commit this txn", zap.Uint64("startTS", c.startTS),
			zap.Uint64("commitTS", c.commitTS))
		go func() {
			failpoint.Inject("asyncCommitDoNothing", func() {
				failpoint.Return()
			})
			defer c.ttlManager.close()
			commitBo := NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
			err := c.commitMutations(commitBo, c.mutations)
			if err != nil {
				logutil.Logger(ctx).Warn("2PC async commit failed", zap.Uint64("connID", c.connID),
					zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS), zap.Error(err))
			}
		}()
		return nil
	}
	return c.commitTxn(ctx, commitDetail)
}

func (c *twoPhaseCommitter) commitTxn(ctx context.Context, commitDetail *execdetails.CommitDetails) error {
	c.mutations.values = nil
	c.txn.GetMemBuffer().DiscardValues()
	start := time.Now()

	commitBo := NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
	err := c.commitMutations(commitBo, c.mutations)
	commitDetail.CommitTime = time.Since(start)
	if commitBo.totalSleep > 0 {
		atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(commitBo.totalSleep)*int64(time.Millisecond))
		commitDetail.Mu.Lock()
		commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, commitBo.types...)
		commitDetail.Mu.Unlock()
	}
	if err != nil {
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			err = errors.Trace(terror.ErrResultUndetermined)
		}
		if !c.mu.committed {
			logutil.Logger(ctx).Debug("2PC failed on commit",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		logutil.Logger(ctx).Debug("got some exceptions, but 2PC was still successful",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

func (c *twoPhaseCommitter) stripNoNeedCommitKeys() {
	if !c.hasNoNeedCommitKeys {
		return
	}
	m := &c.mutations
	var newIdx int
	for oldIdx := range m.keys {
		key := m.keys[oldIdx]
		flags, err := c.txn.GetMemBuffer().GetFlags(key)
		if err == nil && flags.HasNoNeedCommit() {
			continue
		}
		m.keys[newIdx] = key
		if m.ops != nil {
			m.ops[newIdx] = m.ops[oldIdx]
		}
		if m.values != nil {
			m.values[newIdx] = m.values[oldIdx]
		}
		if m.isPessimisticLock != nil {
			m.isPessimisticLock[newIdx] = m.isPessimisticLock[oldIdx]
		}
		newIdx++
	}
	c.mutations = m.subRange(0, newIdx)
}

// SchemaVer is the schemaReplicant which will return the schemaReplicant version.
type SchemaVer interface {
	// SchemaMetaVersion returns the spacetime schemaReplicant version.
	SchemaMetaVersion() int64
}

type schemaLeaseChecker interface {
	// CheckBySchemaVer checks if the schemaReplicant has changed for the transaction related blocks between the startSchemaVer
	// and the schemaReplicant version at txnTS, all the related schemaReplicant changes will be returned.
	CheckBySchemaVer(txnTS uint64, startSchemaVer SchemaVer) (*RelatedSchemaChange, error)
}

// RelatedSchemaChange contains information about schemaReplicant diff between two schemaReplicant versions.
type RelatedSchemaChange struct {
	PhyTblIDS             []int64
	CausetActionTypes     []uint64
	LatestSchemaReplicant SchemaVer
	Amendable             bool
}

func (c *twoPhaseCommitter) tryAmendTxn(ctx context.Context, startSchemaReplicant SchemaVer, change *RelatedSchemaChange) (bool, error) {
	addMutations, err := c.txn.schemaAmender.AmendTxn(ctx, startSchemaReplicant, change, c.mutations)
	if err != nil {
		return false, err
	}
	// Prewrite new mutations.
	if addMutations != nil && len(addMutations.keys) > 0 {
		prewriteBo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
		err = c.prewriteMutations(prewriteBo, *addMutations)
		if err != nil {
			logutil.Logger(ctx).Warn("amend prewrite has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
			return false, err
		}
		logutil.Logger(ctx).Info("amend prewrite finished", zap.Uint64("txnStartTS", c.startTS))
		return true, nil
	}
	return false, nil
}

func (c *twoPhaseCommitter) getCommitTS(ctx context.Context, commitDetail *execdetails.CommitDetails) (uint64, error) {
	start := time.Now()
	logutil.Event(ctx, "start get commit ts")
	commitTS, err := c.causetstore.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return 0, errors.Trace(err)
	}
	commitDetail.GetCommitTsTime = time.Since(start)
	logutil.Event(ctx, "finish get commit ts")
	logutil.SetTag(ctx, "commitTS", commitTS)

	// Check commitTS.
	if commitTS <= c.startTS {
		err = errors.Errorf("conn %d invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return 0, errors.Trace(err)
	}
	return commitTS, nil
}

// checkSchemaValid checks if the schemaReplicant has changed, if tryAmend is set to true, committer will try to amend
// this transaction using the related schemaReplicant changes.
func (c *twoPhaseCommitter) checkSchemaValid(ctx context.Context, checkTS uint64, startSchemaReplicant SchemaVer,
	tryAmend bool) (*RelatedSchemaChange, bool, error) {
	checker, ok := c.txn.us.GetOption(ekv.SchemaChecker).(schemaLeaseChecker)
	if !ok {
		if c.connID > 0 {
			logutil.Logger(ctx).Warn("schemaLeaseChecker is not set for this transaction",
				zap.Uint64("connID", c.connID),
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("commitTS", checkTS))
		}
		return nil, false, nil
	}
	relatedChanges, err := checker.CheckBySchemaVer(checkTS, startSchemaReplicant)
	if err != nil {
		if tryAmend && relatedChanges != nil && relatedChanges.Amendable && c.txn.schemaAmender != nil {
			memAmended, amendErr := c.tryAmendTxn(ctx, startSchemaReplicant, relatedChanges)
			if amendErr != nil {
				logutil.BgLogger().Info("txn amend has failed", zap.Uint64("connID", c.connID),
					zap.Uint64("startTS", c.startTS), zap.Error(amendErr))
				return nil, false, err
			}
			logutil.Logger(ctx).Info("amend txn successfully for pessimistic commit",
				zap.Uint64("connID", c.connID), zap.Uint64("txn startTS", c.startTS), zap.Bool("memAmended", memAmended),
				zap.Uint64("checkTS", checkTS), zap.Int64("startSchemaReplicantVer", startSchemaReplicant.SchemaMetaVersion()),
				zap.Int64s("causet ids", relatedChanges.PhyTblIDS), zap.Uint64s("action types", relatedChanges.CausetActionTypes))
			return relatedChanges, memAmended, nil
		}
		return nil, false, errors.Trace(err)
	}
	return nil, false, nil
}

func (c *twoPhaseCommitter) prewriteBinlog(ctx context.Context) chan *binloginfo.WriteResult {
	if !c.shouldWriteBinlog() {
		return nil
	}
	ch := make(chan *binloginfo.WriteResult, 1)
	go func() {
		logutil.Eventf(ctx, "start prewrite binlog")
		binInfo := c.txn.us.GetOption(ekv.BinlogInfo).(*binloginfo.BinlogInfo)
		bin := binInfo.Data
		bin.StartTs = int64(c.startTS)
		if bin.Tp == binlog.BinlogType_Prewrite {
			bin.PrewriteKey = c.primary()
		}
		wr := binInfo.WriteBinlog(c.causetstore.clusterID)
		if wr.Skipped() {
			binInfo.Data.PrewriteValue = nil
			binloginfo.AddOneSkippedCommitter()
		}
		logutil.Eventf(ctx, "finish prewrite binlog")
		ch <- wr
	}()
	return ch
}

func (c *twoPhaseCommitter) writeFinishBinlog(ctx context.Context, tp binlog.BinlogType, commitTS int64) {
	if !c.shouldWriteBinlog() {
		return
	}
	binInfo := c.txn.us.GetOption(ekv.BinlogInfo).(*binloginfo.BinlogInfo)
	binInfo.Data.Tp = tp
	binInfo.Data.CommitTs = commitTS
	binInfo.Data.PrewriteValue = nil

	wg := sync.WaitGroup{}
	mock := false
	failpoint.Inject("mockSyncBinlogCommit", func(val failpoint.Value) {
		if val.(bool) {
			wg.Add(1)
			mock = true
		}
	})
	go func() {
		logutil.Eventf(ctx, "start write finish binlog")
		binlogWriteResult := binInfo.WriteBinlog(c.causetstore.clusterID)
		err := binlogWriteResult.GetError()
		if err != nil {
			logutil.BgLogger().Error("failed to write binlog",
				zap.Error(err))
		}
		logutil.Eventf(ctx, "finish write finish binlog")
		if mock {
			wg.Done()
		}
	}()
	if mock {
		wg.Wait()
	}
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	return c.txn.us.GetOption(ekv.BinlogInfo) != nil
}

// EinsteinDB recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

type batchMutations struct {
	region    RegionVerID
	mutations CommitterMutations
	isPrimary bool
}
type batched struct {
	batches    []batchMutations
	primaryIdx int
	primaryKey []byte
}

func newBatched(primaryKey []byte) *batched {
	return &batched{
		primaryIdx: -1,
		primaryKey: primaryKey,
	}
}

// appendBatchMutationsBySize appends mutations to b. It may split the keys to make
// sure each batch's size does not exceed the limit.
func (b *batched) appendBatchMutationsBySize(region RegionVerID, mutations CommitterMutations, sizeFn func(k, v []byte) int, limit int) {
	var start, end int
	for start = 0; start < mutations.len(); start = end {
		var size int
		for end = start; end < mutations.len() && size < limit; end++ {
			var k, v []byte
			k = mutations.keys[end]
			if end < len(mutations.values) {
				v = mutations.values[end]
			}
			size += sizeFn(k, v)
			if b.primaryIdx < 0 && bytes.Equal(k, b.primaryKey) {
				b.primaryIdx = len(b.batches)
			}
		}
		b.batches = append(b.batches, batchMutations{
			region:    region,
			mutations: mutations.subRange(start, end),
		})
	}
}

func (b *batched) setPrimary() bool {
	// If the batches include the primary key, put it to the first
	if b.primaryIdx >= 0 {
		if len(b.batches) > 0 {
			b.batches[b.primaryIdx].isPrimary = true
			b.batches[0], b.batches[b.primaryIdx] = b.batches[b.primaryIdx], b.batches[0]
			b.primaryIdx = 0
		}
		return true
	}

	return false
}

func (b *batched) allBatches() []batchMutations {
	return b.batches
}

// primaryBatch returns the batch containing the primary key.
// Precondition: `b.setPrimary() == true`
func (b *batched) primaryBatch() []batchMutations {
	return b.batches[:1]
}

func (b *batched) forgetPrimary() {
	if len(b.batches) == 0 {
		return
	}
	b.batches = b.batches[1:]
}

// batchInterlockingDirectorate is txn controller providing rate control like utils
type batchInterlockingDirectorate struct {
	rateLim           int                        // concurrent worker numbers
	rateLimiter       *rateLimit                 // rate limiter for concurrency control, maybe more strategies
	committer         *twoPhaseCommitter         // here maybe more different type committer in the future
	action            twoPhaseCommitCausetAction // the work action type
	backoffer         *Backoffer                 // Backoffer
	tokenWaitDuration time.Duration              // get token wait time
}

// newBatchInterlockingDirectorate create processor to handle concurrent batch works(prewrite/commit etc)
func newBatchInterlockingDirectorate(rateLimit int, committer *twoPhaseCommitter,
	action twoPhaseCommitCausetAction, backoffer *Backoffer) *batchInterlockingDirectorate {
	return &batchInterlockingDirectorate{rateLimit, nil, committer,
		action, backoffer, 1 * time.Millisecond}
}

// initUtils do initialize batchInterlockingDirectorate related policies like rateLimit soliton
func (batchInterDir *batchInterlockingDirectorate) initUtils() error {
	// init rateLimiter by injected rate limit number
	batchInterDir.rateLimiter = newRateLimit(batchInterDir.rateLim)
	return nil
}

// startWork concurrently do the work for each batch considering rate limit
func (batchInterDir *batchInterlockingDirectorate) startWorker(exitCh chan struct{}, ch chan error, batches []batchMutations) {
	for idx, batch1 := range batches {
		waitStart := time.Now()
		if exit := batchInterDir.rateLimiter.getToken(exitCh); !exit {
			batchInterDir.tokenWaitDuration += time.Since(waitStart)
			batch := batch1
			go func() {
				defer batchInterDir.rateLimiter.putToken()
				var singleBatchBackoffer *Backoffer
				if _, ok := batchInterDir.action.(actionCommit); ok {
					// Because the secondary batches of the commit actions are implemented to be
					// committed asynchronously in background goroutines, we should not
					// fork a child context and call cancel() while the foreground goroutine exits.
					// Otherwise the background goroutines will be canceled execeptionally.
					// Here we makes a new clone of the original backoffer for this goroutine
					// exclusively to avoid the data race when using the same backoffer
					// in concurrent goroutines.
					singleBatchBackoffer = batchInterDir.backoffer.Clone()
				} else {
					var singleBatchCancel context.CancelFunc
					singleBatchBackoffer, singleBatchCancel = batchInterDir.backoffer.Fork()
					defer singleBatchCancel()
				}
				beforeSleep := singleBatchBackoffer.totalSleep
				ch <- batchInterDir.action.handleSingleBatch(batchInterDir.committer, singleBatchBackoffer, batch)
				commitDetail := batchInterDir.committer.getDetail()
				if commitDetail != nil { // dagger operations of pessimistic-txn will let commitDetail be nil
					if delta := singleBatchBackoffer.totalSleep - beforeSleep; delta > 0 {
						atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(singleBatchBackoffer.totalSleep-beforeSleep)*int64(time.Millisecond))
						commitDetail.Mu.Lock()
						commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, singleBatchBackoffer.types...)
						commitDetail.Mu.Unlock()
					}
				}
			}()
		} else {
			logutil.Logger(batchInterDir.backoffer.ctx).Info("break startWorker",
				zap.Stringer("action", batchInterDir.action), zap.Int("batch size", len(batches)),
				zap.Int("index", idx))
			break
		}
	}
}

// process will start worker routine and collect results
func (batchInterDir *batchInterlockingDirectorate) process(batches []batchMutations) error {
	var err error
	err = batchInterDir.initUtils()
	if err != nil {
		logutil.Logger(batchInterDir.backoffer.ctx).Error("batchInterlockingDirectorate initUtils failed", zap.Error(err))
		return err
	}

	// For prewrite, stop sending other requests after receiving first error.
	backoffer := batchInterDir.backoffer
	var cancel context.CancelFunc
	if _, ok := batchInterDir.action.(actionPrewrite); ok {
		backoffer, cancel = batchInterDir.backoffer.Fork()
		defer cancel()
	}
	// concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	exitCh := make(chan struct{})
	go batchInterDir.startWorker(exitCh, ch, batches)
	// check results
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(backoffer.ctx).Debug("2PC doCausetActionOnBatch failed",
				zap.Uint64("conn", batchInterDir.committer.connID),
				zap.Stringer("action type", batchInterDir.action),
				zap.Error(e),
				zap.Uint64("txnStartTS", batchInterDir.committer.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				logutil.Logger(backoffer.ctx).Debug("2PC doCausetActionOnBatch to cancel other actions",
					zap.Uint64("conn", batchInterDir.committer.connID),
					zap.Stringer("action type", batchInterDir.action),
					zap.Uint64("txnStartTS", batchInterDir.committer.startTS))
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	close(exitCh)
	metrics.EinsteinDBTokenWaitDuration.Observe(batchInterDir.tokenWaitDuration.Seconds())
	return err
}

func getTxnPriority(txn *einsteindbTxn) pb.CommandPri {
	if pri := txn.us.GetOption(ekv.Priority); pri != nil {
		return ekvPriorityToCommandPri(pri.(int))
	}
	return pb.CommandPri_Normal
}

func getTxnSyncLog(txn *einsteindbTxn) bool {
	if syncOption := txn.us.GetOption(ekv.SyncLog); syncOption != nil {
		return syncOption.(bool)
	}
	return false
}

func ekvPriorityToCommandPri(pri int) pb.CommandPri {
	switch pri {
	case ekv.PriorityLow:
		return pb.CommandPri_Low
	case ekv.PriorityHigh:
		return pb.CommandPri_High
	default:
		return pb.CommandPri_Normal
	}
}

func (c *twoPhaseCommitter) setDetail(d *execdetails.CommitDetails) {
	atomic.StorePointer(&c.detail, unsafe.Pointer(d))
}

func (c *twoPhaseCommitter) getDetail() *execdetails.CommitDetails {
	return (*execdetails.CommitDetails)(atomic.LoadPointer(&c.detail))
}

func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.undeterminedErr = err
}

func (c *twoPhaseCommitter) getUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}
