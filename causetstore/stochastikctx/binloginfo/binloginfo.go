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

package binloginfo

import (
	"math"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-binlog"
	"github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/node"
	pumpcli "github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/pump_client"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	driver "github.com/whtcorpsinc/milevadb/types/BerolinaSQL_driver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func init() {
	grpc.EnableTracing = false
}

// pumpsClient is the client to write binlog, it is opened on server start and never close,
// shared by all stochastik.
var pumpsClient *pumpcli.PumpsClient
var pumpsClientLock sync.RWMutex
var shardPat = regexp.MustCompile(`SHARD_ROW_ID_BITS\s*=\s*\d+\s*`)
var preSplitPat = regexp.MustCompile(`PRE_SPLIT_REGIONS\s*=\s*\d+\s*`)
var autoRandomPat = regexp.MustCompile(`AUTO_RANDOM\s*\(\s*\d+\s*\)\s*`)

// BinlogInfo contains binlog data and binlog client.
type BinlogInfo struct {
	Data   *binlog.Binlog
	Client *pumpcli.PumpsClient
}

// BinlogStatus is the status of binlog
type BinlogStatus int

const (
	//BinlogStatusUnknown stands for unknown binlog status
	BinlogStatusUnknown BinlogStatus = iota
	//BinlogStatusOn stands for the binlog is enabled
	BinlogStatusOn
	//BinlogStatusOff stands for the binlog is disabled
	BinlogStatusOff
	//BinlogStatusSkipping stands for the binlog status
	BinlogStatusSkipping
)

// String implements String function in fmt.Stringer
func (s BinlogStatus) String() string {
	switch s {
	case BinlogStatusOn:
		return "On"
	case BinlogStatusOff:
		return "Off"
	case BinlogStatusSkipping:
		return "Skipping"
	}
	return "Unknown"
}

// GetPumpsClient gets the pumps client instance.
func GetPumpsClient() *pumpcli.PumpsClient {
	pumpsClientLock.RLock()
	client := pumpsClient
	pumpsClientLock.RUnlock()
	return client
}

// SetPumpsClient sets the pumps client instance.
func SetPumpsClient(client *pumpcli.PumpsClient) {
	pumpsClientLock.Lock()
	pumpsClient = client
	pumpsClientLock.Unlock()
}

// GetPrewriteValue gets binlog prewrite value in the context.
func GetPrewriteValue(ctx stochastikctx.Context, createIfNotExists bool) *binlog.PrewriteValue {
	vars := ctx.GetStochastikVars()
	v, ok := vars.TxnCtx.Binlog.(*binlog.PrewriteValue)
	if !ok && createIfNotExists {
		schemaVer := ctx.GetStochastikVars().TxnCtx.SchemaVersion
		v = &binlog.PrewriteValue{SchemaVersion: schemaVer}
		vars.TxnCtx.Binlog = v
	}
	return v
}

var skipBinlog uint32
var ignoreError uint32
var statusListener = func(_ BinlogStatus) error {
	return nil
}

// EnableSkipBinlogFlag enables the skipBinlog flag.
// NOTE: it is used *ONLY* for test.
func EnableSkipBinlogFlag() {
	atomic.StoreUint32(&skipBinlog, 1)
	logutil.BgLogger().Warn("[binloginfo] enable the skipBinlog flag")
}

// DisableSkipBinlogFlag disable the skipBinlog flag.
func DisableSkipBinlogFlag() {
	atomic.StoreUint32(&skipBinlog, 0)
	logutil.BgLogger().Warn("[binloginfo] disable the skipBinlog flag")
}

// IsBinlogSkipped gets the skipBinlog flag.
func IsBinlogSkipped() bool {
	return atomic.LoadUint32(&skipBinlog) > 0
}

// BinlogRecoverStatus is used for display the binlog recovered status after some operations.
type BinlogRecoverStatus struct {
	Skipped                 bool
	SkippedCommitterCounter int32
}

// GetBinlogStatus returns the binlog recovered status.
func GetBinlogStatus() *BinlogRecoverStatus {
	return &BinlogRecoverStatus{
		Skipped:                 IsBinlogSkipped(),
		SkippedCommitterCounter: SkippedCommitterCount(),
	}
}

var skippedCommitterCounter int32

// WaitBinlogRecover returns when all committing transaction finished.
func WaitBinlogRecover(timeout time.Duration) error {
	logutil.BgLogger().Warn("[binloginfo] start waiting for binlog recovering")
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	start := time.Now()
	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&skippedCommitterCounter) == 0 {
				logutil.BgLogger().Warn("[binloginfo] binlog recovered")
				return nil
			}
			if time.Since(start) > timeout {
				logutil.BgLogger().Warn("[binloginfo] waiting for binlog recovering timed out",
					zap.Duration("duration", timeout))
				return errors.New("timeout")
			}
		}
	}
}

// SkippedCommitterCount returns the number of alive committers whick skipped the binlog writing.
func SkippedCommitterCount() int32 {
	return atomic.LoadInt32(&skippedCommitterCounter)
}

// ResetSkippedCommitterCounter is used to reset the skippedCommitterCounter.
func ResetSkippedCommitterCounter() {
	atomic.StoreInt32(&skippedCommitterCounter, 0)
	logutil.BgLogger().Warn("[binloginfo] skippedCommitterCounter is reset to 0")
}

// AddOneSkippedCommitter adds one committer to skippedCommitterCounter.
func AddOneSkippedCommitter() {
	atomic.AddInt32(&skippedCommitterCounter, 1)
}

// RemoveOneSkippedCommitter removes one committer from skippedCommitterCounter.
func RemoveOneSkippedCommitter() {
	atomic.AddInt32(&skippedCommitterCounter, -1)
}

// SetIgnoreError sets the ignoreError flag, this function called when MilevaDB start
// up and find config.Binlog.IgnoreError is true.
func SetIgnoreError(on bool) {
	if on {
		atomic.StoreUint32(&ignoreError, 1)
	} else {
		atomic.StoreUint32(&ignoreError, 0)
	}
}

// GetStatus gets the status of binlog
func GetStatus() BinlogStatus {
	conf := config.GetGlobalConfig()
	if !conf.Binlog.Enable {
		return BinlogStatusOff
	}
	skip := atomic.LoadUint32(&skipBinlog)
	if skip > 0 {
		return BinlogStatusSkipping
	}
	return BinlogStatusOn
}

// RegisterStatusListener registers a listener function to watch binlog status
func RegisterStatusListener(listener func(BinlogStatus) error) {
	statusListener = listener
}

// WriteResult is used for the returned chan of WriteBinlog.
type WriteResult struct {
	skipped bool
	err     error
}

// Skipped if true stands for the binlog writing is skipped.
func (wr *WriteResult) Skipped() bool {
	return wr.skipped
}

// GetError gets the error of WriteBinlog.
func (wr *WriteResult) GetError() error {
	return wr.err
}

// WriteBinlog writes a binlog to Pump.
func (info *BinlogInfo) WriteBinlog(clusterID uint64) *WriteResult {
	skip := atomic.LoadUint32(&skipBinlog)
	if skip > 0 {
		metrics.CriticalErrorCounter.Add(1)
		return &WriteResult{true, nil}
	}

	if info.Client == nil {
		return &WriteResult{false, errors.New("pumps client is nil")}
	}

	// it will retry in PumpsClient if write binlog fail.
	err := info.Client.WriteBinlog(info.Data)
	if err != nil {
		logutil.BgLogger().Error("write binlog failed",
			zap.String("binlog_type", info.Data.Tp.String()),
			zap.Uint64("binlog_start_ts", uint64(info.Data.StartTs)),
			zap.Uint64("binlog_commit_ts", uint64(info.Data.CommitTs)),
			zap.Error(err))
		if atomic.LoadUint32(&ignoreError) == 1 {
			logutil.BgLogger().Error("write binlog fail but error ignored")
			metrics.CriticalErrorCounter.Add(1)
			// If error happens once, we'll stop writing binlog.
			swapped := atomic.CompareAndSwapUint32(&skipBinlog, skip, skip+1)
			if swapped && skip == 0 {
				if err := statusListener(BinlogStatusSkipping); err != nil {
					logutil.BgLogger().Warn("uFIDelate binlog status failed", zap.Error(err))
				}
			}
			return &WriteResult{true, nil}
		}

		if strings.Contains(err.Error(), "received message larger than max") {
			// This HoTT of error is not critical, return directly.
			return &WriteResult{false, errors.Errorf("binlog data is too large (%s)", err.Error())}
		}

		return &WriteResult{false, terror.ErrCritical.GenWithStackByArgs(err)}
	}

	return &WriteResult{false, nil}
}

// SetDBSBinlog sets DBS binlog in the ekv.Transaction.
func SetDBSBinlog(client *pumpcli.PumpsClient, txn ekv.Transaction, jobID int64, dbsSchemaState int32, dbsQuery string) {
	if client == nil {
		return
	}

	dbsQuery = AddSpecialComment(dbsQuery)
	info := &BinlogInfo{
		Data: &binlog.Binlog{
			Tp:             binlog.BinlogType_Prewrite,
			DdlJobId:       jobID,
			DdlSchemaState: dbsSchemaState,
			DdlQuery:       []byte(dbsQuery),
		},
		Client: client,
	}
	txn.SetOption(ekv.BinlogInfo, info)
}

const specialPrefix = `/*T! `

// AddSpecialComment uses to add comment for causet option in DBS query.
// Export for testing.
func AddSpecialComment(dbsQuery string) string {
	if strings.Contains(dbsQuery, specialPrefix) || strings.Contains(dbsQuery, driver.SpecialCommentVersionPrefix) {
		return dbsQuery
	}
	dbsQuery = addSpecialCommentByRegexps(dbsQuery, specialPrefix, shardPat, preSplitPat)
	for featureID, pattern := range driver.FeatureIDPatterns {
		dbsQuery = addSpecialCommentByRegexps(dbsQuery, driver.BuildSpecialCommentPrefix(featureID), pattern)
	}
	return dbsQuery
}

// addSpecialCommentByRegexps uses to add special comment for the worlds in the dbsQuery with match the regexps.
// addSpecialCommentByRegexps will merge multi pattern regs to one special comment.
func addSpecialCommentByRegexps(dbsQuery string, prefix string, regs ...*regexp.Regexp) string {
	upperQuery := strings.ToUpper(dbsQuery)
	var specialComments []string
	minIdx := math.MaxInt64
	for i := 0; i < len(regs); {
		reg := regs[i]
		loc := reg.FindStringIndex(upperQuery)
		if len(loc) < 2 {
			i++
			continue
		}
		specialComments = append(specialComments, dbsQuery[loc[0]:loc[1]])
		if loc[0] < minIdx {
			minIdx = loc[0]
		}
		dbsQuery = dbsQuery[:loc[0]] + dbsQuery[loc[1]:]
		upperQuery = upperQuery[:loc[0]] + upperQuery[loc[1]:]
	}
	if minIdx != math.MaxInt64 {
		query := dbsQuery[:minIdx] + prefix
		for _, comment := range specialComments {
			if query[len(query)-1] != ' ' {
				query += " "
			}
			query += comment
		}
		if query[len(query)-1] != ' ' {
			query += " "
		}
		query += "*/"
		if len(dbsQuery[minIdx:]) > 0 {
			return query + " " + dbsQuery[minIdx:]
		}
		return query
	}
	return dbsQuery
}

// MockPumpsClient creates a PumpsClient, used for test.
func MockPumpsClient(client binlog.PumpClient) *pumpcli.PumpsClient {
	nodeID := "pump-1"
	pump := &pumpcli.PumpStatus{
		Status: node.Status{
			NodeID: nodeID,
			State:  node.Online,
		},
		Client: client,
	}

	pumpInfos := &pumpcli.PumpInfos{
		Pumps:            make(map[string]*pumpcli.PumpStatus),
		AvaliablePumps:   make(map[string]*pumpcli.PumpStatus),
		UnAvaliablePumps: make(map[string]*pumpcli.PumpStatus),
	}
	pumpInfos.Pumps[nodeID] = pump
	pumpInfos.AvaliablePumps[nodeID] = pump

	pCli := &pumpcli.PumpsClient{
		ClusterID:          1,
		Pumps:              pumpInfos,
		Selector:           pumpcli.NewSelector(pumpcli.Range),
		BinlogWriteTimeout: time.Second,
	}
	pCli.Selector.SetPumps([]*pumpcli.PumpStatus{pump})

	return pCli
}
