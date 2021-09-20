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

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	causetDecoder "github.com/whtcorpsinc/milevadb/soliton/rowCausetDecoder"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

// DBSInfo is for DBS information.
type DBSInfo struct {
	SchemaVer   int64
	ReorgHandle ekv.Handle        // It's only used for DBS information.
	Jobs        []*perceptron.Job // It's the currently running jobs.
}

// GetDBSInfo returns DBS information.
func GetDBSInfo(txn ekv.Transaction) (*DBSInfo, error) {
	var err error
	info := &DBSInfo{}
	t := spacetime.NewMeta(txn)

	info.Jobs = make([]*perceptron.Job, 0, 2)
	job, err := t.GetDBSJobByIdx(0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if job != nil {
		info.Jobs = append(info.Jobs, job)
	}
	addIdxJob, err := t.GetDBSJobByIdx(0, spacetime.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if addIdxJob != nil {
		info.Jobs = append(info.Jobs, addIdxJob)
	}

	info.SchemaVer, err = t.GetSchemaVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if addIdxJob == nil {
		return info, nil
	}

	tbl, err := t.GetBlock(addIdxJob.SchemaID, addIdxJob.BlockID)
	if err != nil {
		return info, nil
	}
	info.ReorgHandle, _, _, err = t.GetDBSReorgHandle(addIdxJob, tbl.IsCommonHandle)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return info, nil
}

// IsJobRollbackable checks whether the job can be rollback.
func IsJobRollbackable(job *perceptron.Job) bool {
	switch job.Type {
	case perceptron.CausetActionDropIndex, perceptron.CausetActionDropPrimaryKey:
		// We can't cancel if index current state is in StateDeleteOnly or StateDeleteReorganization or StateWriteOnly, otherwise there will be an inconsistent issue between record and index.
		// In WriteOnly state, we can rollback for normal index but can't rollback for memex index(need to drop hidden defCausumn). Since we can't
		// know the type of index here, we consider all indices except primary index as non-rollbackable.
		// TODO: distinguish normal index and memex index so that we can rollback `DropIndex` for normal index in WriteOnly state.
		// TODO: make DropPrimaryKey rollbackable in WriteOnly, it need to deal with some tests.
		if job.SchemaState == perceptron.StateDeleteOnly ||
			job.SchemaState == perceptron.StateDeleteReorganization ||
			job.SchemaState == perceptron.StateWriteOnly {
			return false
		}
	case perceptron.CausetActionDropSchema, perceptron.CausetActionDropBlock, perceptron.CausetActionDropSequence:
		// To simplify the rollback logic, cannot be canceled in the following states.
		if job.SchemaState == perceptron.StateWriteOnly ||
			job.SchemaState == perceptron.StateDeleteOnly {
			return false
		}
	case perceptron.CausetActionAddBlockPartition:
		return job.SchemaState == perceptron.StateNone || job.SchemaState == perceptron.StateReplicaOnly
	case perceptron.CausetActionDropDeferredCauset, perceptron.CausetActionDropDeferredCausets, perceptron.CausetActionDropBlockPartition,
		perceptron.CausetActionRebaseAutoID, perceptron.CausetActionShardRowID,
		perceptron.CausetActionTruncateBlock, perceptron.CausetActionAddForeignKey,
		perceptron.CausetActionDropForeignKey, perceptron.CausetActionRenameBlock,
		perceptron.CausetActionModifyBlockCharsetAndDefCauslate, perceptron.CausetActionTruncateBlockPartition,
		perceptron.CausetActionModifySchemaCharsetAndDefCauslate, perceptron.CausetActionRepairBlock, perceptron.CausetActionModifyBlockAutoIdCache:
		return job.SchemaState == perceptron.StateNone
	}
	return true
}

// CancelJobs cancels the DBS jobs.
func CancelJobs(txn ekv.Transaction, ids []int64) ([]error, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	errs := make([]error, len(ids))
	t := spacetime.NewMeta(txn)
	generalJobs, err := getDBSJobsInQueue(t, spacetime.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDBSJobsInQueue(t, spacetime.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)

	for i, id := range ids {
		found := false
		for j, job := range jobs {
			if id != job.ID {
				logutil.BgLogger().Debug("the job that needs to be canceled isn't equal to current job",
					zap.Int64("need to canceled job ID", id),
					zap.Int64("current job ID", job.ID))
				continue
			}
			found = true
			// These states can't be cancelled.
			if job.IsDone() || job.IsSynced() {
				errs[i] = ErrCancelFinishedDBSJob.GenWithStackByArgs(id)
				continue
			}
			// If the state is rolling back, it means the work is cleaning the data after cancelling the job.
			if job.IsCancelled() || job.IsRollingback() || job.IsRollbackDone() {
				continue
			}
			if !IsJobRollbackable(job) {
				errs[i] = ErrCannotCancelDBSJob.GenWithStackByArgs(job.ID)
				continue
			}

			job.State = perceptron.JobStateCancelling
			// Make sure RawArgs isn't overwritten.
			err := json.Unmarshal(job.RawArgs, &job.Args)
			if err != nil {
				errs[i] = errors.Trace(err)
				continue
			}
			if job.Type == perceptron.CausetActionAddIndex || job.Type == perceptron.CausetActionAddPrimaryKey {
				offset := int64(j - len(generalJobs))
				err = t.UFIDelateDBSJob(offset, job, true, spacetime.AddIndexJobListKey)
			} else {
				err = t.UFIDelateDBSJob(int64(j), job, true)
			}
			if err != nil {
				errs[i] = errors.Trace(err)
			}
		}
		if !found {
			errs[i] = ErrDBSJobNotFound.GenWithStackByArgs(id)
		}
	}
	return errs, nil
}

func getDBSJobsInQueue(t *spacetime.Meta, jobListKey spacetime.JobListKeyType) ([]*perceptron.Job, error) {
	cnt, err := t.DBSJobQueueLen(jobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := make([]*perceptron.Job, cnt)
	for i := range jobs {
		jobs[i], err = t.GetDBSJobByIdx(int64(i), jobListKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return jobs, nil
}

// GetDBSJobs get all DBS jobs and sorts jobs by job.ID.
func GetDBSJobs(txn ekv.Transaction) ([]*perceptron.Job, error) {
	t := spacetime.NewMeta(txn)
	generalJobs, err := getDBSJobsInQueue(t, spacetime.DefaultJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	addIdxJobs, err := getDBSJobsInQueue(t, spacetime.AddIndexJobListKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs := append(generalJobs, addIdxJobs...)
	sort.Sort(jobArray(jobs))
	return jobs, nil
}

type jobArray []*perceptron.Job

func (v jobArray) Len() int {
	return len(v)
}

func (v jobArray) Less(i, j int) bool {
	return v[i].ID < v[j].ID
}

func (v jobArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

// MaxHistoryJobs is exported for testing.
const MaxHistoryJobs = 10

// DefNumHistoryJobs is default value of the default number of history job
const DefNumHistoryJobs = 10

// GetHistoryDBSJobs returns the DBS history jobs and an error.
// The maximum count of history jobs is num.
func GetHistoryDBSJobs(txn ekv.Transaction, maxNumJobs int) ([]*perceptron.Job, error) {
	t := spacetime.NewMeta(txn)
	jobs, err := t.GetLastNHistoryDBSJobs(maxNumJobs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

// IterHistoryDBSJobs iterates history DBS jobs until the `finishFn` return true or error.
func IterHistoryDBSJobs(txn ekv.Transaction, finishFn func([]*perceptron.Job) (bool, error)) error {
	txnMeta := spacetime.NewMeta(txn)
	iter, err := txnMeta.GetLastHistoryDBSJobsIterator()
	if err != nil {
		return err
	}
	cacheJobs := make([]*perceptron.Job, 0, DefNumHistoryJobs)
	for {
		cacheJobs, err = iter.GetLastJobs(DefNumHistoryJobs, cacheJobs)
		if err != nil || len(cacheJobs) == 0 {
			return err
		}
		finish, err := finishFn(cacheJobs)
		if err != nil || finish {
			return err
		}
	}
}

// IterAllDBSJobs will iterates running DBS jobs first, return directly if `finishFn` return true or error,
// then iterates history DBS jobs until the `finishFn` return true or error.
func IterAllDBSJobs(txn ekv.Transaction, finishFn func([]*perceptron.Job) (bool, error)) error {
	jobs, err := GetDBSJobs(txn)
	if err != nil {
		return err
	}

	finish, err := finishFn(jobs)
	if err != nil || finish {
		return err
	}
	return IterHistoryDBSJobs(txn, finishFn)
}

// RecordData is the record data composed of a handle and values.
type RecordData struct {
	Handle ekv.Handle
	Values []types.Causet
}

func getCount(ctx stochastikctx.Context, allegrosql string) (int64, error) {
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLInterlockingDirectorate).InterDircRestrictedALLEGROSQLWithSnapshot(allegrosql)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.Errorf("can not get count, allegrosql %s result rows %d", allegrosql, len(rows))
	}
	return rows[0].GetInt64(0), nil
}

// Count greater Types
const (
	// TblCntGreater means that the number of causet rows is more than the number of index rows.
	TblCntGreater byte = 1
	// IdxCntGreater means that the number of index rows is more than the number of causet rows.
	IdxCntGreater byte = 2
)

// ChecHoTTicesCount compares indices count with causet count.
// It returns the count greater type, the index offset and an error.
// It returns nil if the count from the index is equal to the count from the causet defCausumns,
// otherwise it returns an error and the corresponding index's offset.
func ChecHoTTicesCount(ctx stochastikctx.Context, dbName, blockName string, indices []string) (byte, int, error) {
	// Here we need check all indexes, includes invisible index
	ctx.GetStochastikVars().OptimizerUseInvisibleIndexes = true
	// Add `` for some names like `causet name`.
	allegrosql := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` USE INDEX()", dbName, blockName)
	tblCnt, err := getCount(ctx, allegrosql)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	for i, idx := range indices {
		allegrosql = fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` USE INDEX(`%s`)", dbName, blockName, idx)
		idxCnt, err := getCount(ctx, allegrosql)
		if err != nil {
			return 0, i, errors.Trace(err)
		}
		logutil.Logger(context.Background()).Info("check indices count",
			zap.String("causet", blockName), zap.Int64("cnt", tblCnt), zap.Reflect("index", idx), zap.Int64("cnt", idxCnt))
		if tblCnt == idxCnt {
			continue
		}

		var ret byte
		if tblCnt > idxCnt {
			ret = TblCntGreater
		} else if idxCnt > tblCnt {
			ret = IdxCntGreater
		}
		return ret, i, ErrAdminCheckBlock.GenWithStack("causet count %d != index(%s) count %d", tblCnt, idx, idxCnt)
	}
	return 0, 0, nil
}

// CheckRecordAndIndex is exported for testing.
func CheckRecordAndIndex(sessCtx stochastikctx.Context, txn ekv.Transaction, t causet.Block, idx causet.Index) error {
	sc := sessCtx.GetStochastikVars().StmtCtx
	defcaus := make([]*causet.DeferredCauset, len(idx.Meta().DeferredCausets))
	for i, defCaus := range idx.Meta().DeferredCausets {
		defcaus[i] = t.DefCauss()[defCaus.Offset]
	}

	startKey := t.RecordKey(ekv.IntHandle(math.MinInt64))
	filterFunc := func(h1 ekv.Handle, vals1 []types.Causet, defcaus []*causet.DeferredCauset) (bool, error) {
		for i, val := range vals1 {
			defCaus := defcaus[i]
			if val.IsNull() {
				if allegrosql.HasNotNullFlag(defCaus.Flag) && defCaus.ToInfo().OriginDefaultValue == nil {
					return false, errors.Errorf("DeferredCauset %v define as not null, but can't find the value where handle is %v", defCaus.Name, h1)
				}
				// NULL value is regarded as its default value.
				defCausDefVal, err := causet.GetDefCausOriginDefaultValue(sessCtx, defCaus.ToInfo())
				if err != nil {
					return false, errors.Trace(err)
				}
				vals1[i] = defCausDefVal
			}
		}
		isExist, h2, err := idx.Exist(sc, txn.GetUnionStore(), vals1, h1)
		if ekv.ErrKeyExists.Equal(err) {
			record1 := &RecordData{Handle: h1, Values: vals1}
			record2 := &RecordData{Handle: h2, Values: vals1}
			return false, ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", record2, record1)
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		if !isExist {
			record := &RecordData{Handle: h1, Values: vals1}
			return false, ErrDataInConsistent.GenWithStack("index:%#v != record:%#v", nil, record)
		}

		return true, nil
	}
	err := iterRecords(sessCtx, txn, t, startKey, defcaus, filterFunc)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func makeRowCausetDecoder(t causet.Block, sctx stochastikctx.Context) (*causetDecoder.RowCausetDecoder, error) {
	dbName := perceptron.NewCIStr(sctx.GetStochastikVars().CurrentDB)
	exprDefCauss, _, err := memex.DeferredCausetInfos2DeferredCausetsAndNames(sctx, dbName, t.Meta().Name, t.Meta().DefCauss(), t.Meta())
	if err != nil {
		return nil, err
	}
	mockSchema := memex.NewSchema(exprDefCauss...)
	decodeDefCaussMap := causetDecoder.BuildFullDecodeDefCausMap(t.DefCauss(), mockSchema)

	return causetDecoder.NewRowCausetDecoder(t, t.DefCauss(), decodeDefCaussMap), nil
}

func iterRecords(sessCtx stochastikctx.Context, retriever ekv.Retriever, t causet.Block, startKey ekv.Key, defcaus []*causet.DeferredCauset, fn causet.RecordIterFunc) error {
	prefix := t.RecordPrefix()
	keyUpperBound := prefix.PrefixNext()

	it, err := retriever.Iter(startKey, keyUpperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	logutil.BgLogger().Debug("record",
		zap.Stringer("startKey", startKey),
		zap.Stringer("key", it.Key()),
		zap.Binary("value", it.Value()))
	rowCausetDecoder, err := makeRowCausetDecoder(t, sessCtx)
	if err != nil {
		return err
	}
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first ekv pair is event dagger information.
		// TODO: check valid dagger
		// get event handle
		handle, err := blockcodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		rowMap, err := rowCausetDecoder.DecodeAndEvalRowWithMap(sessCtx, handle, it.Value(), sessCtx.GetStochastikVars().Location(), time.UTC, nil)
		if err != nil {
			return errors.Trace(err)
		}
		data := make([]types.Causet, 0, len(defcaus))
		for _, defCaus := range defcaus {
			data = append(data, rowMap[defCaus.ID])
		}
		more, err := fn(handle, data, defcaus)
		if !more || err != nil {
			return errors.Trace(err)
		}

		rk := t.RecordKey(handle)
		err = ekv.NextUntil(it, soliton.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

var (
	// ErrDataInConsistent indicate that meets inconsistent data.
	ErrDataInConsistent = terror.ClassAdmin.New(errno.ErrDataInConsistent, errno.MyALLEGROSQLErrName[errno.ErrDataInConsistent])
	// ErrDBSJobNotFound indicates the job id was not found.
	ErrDBSJobNotFound = terror.ClassAdmin.New(errno.ErrDBSJobNotFound, errno.MyALLEGROSQLErrName[errno.ErrDBSJobNotFound])
	// ErrCancelFinishedDBSJob returns when cancel a finished dbs job.
	ErrCancelFinishedDBSJob = terror.ClassAdmin.New(errno.ErrCancelFinishedDBSJob, errno.MyALLEGROSQLErrName[errno.ErrCancelFinishedDBSJob])
	// ErrCannotCancelDBSJob returns when cancel a almost finished dbs job, because cancel in now may cause data inconsistency.
	ErrCannotCancelDBSJob = terror.ClassAdmin.New(errno.ErrCannotCancelDBSJob, errno.MyALLEGROSQLErrName[errno.ErrCannotCancelDBSJob])
	// ErrAdminCheckBlock returns when the causet records is inconsistent with the index values.
	ErrAdminCheckBlock = terror.ClassAdmin.New(errno.ErrAdminCheckBlock, errno.MyALLEGROSQLErrName[errno.ErrAdminCheckBlock])
)
