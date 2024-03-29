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

package interlock

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

var (
	_ InterlockingDirectorate = &baseInterlockingDirectorate{}
	_ InterlockingDirectorate = &CheckBlockInterDirc{}
	_ InterlockingDirectorate = &HashAggInterDirc{}
	_ InterlockingDirectorate = &HashJoinInterDirc{}
	_ InterlockingDirectorate = &IndexLookUpInterlockingDirectorate{}
	_ InterlockingDirectorate = &IndexReaderInterlockingDirectorate{}
	_ InterlockingDirectorate = &LimitInterDirc{}
	_ InterlockingDirectorate = &MaxOneEventInterDirc{}
	_ InterlockingDirectorate = &MergeJoinInterDirc{}
	_ InterlockingDirectorate = &ProjectionInterDirc{}
	_ InterlockingDirectorate = &SelectionInterDirc{}
	_ InterlockingDirectorate = &SelectLockInterDirc{}
	_ InterlockingDirectorate = &ShowNextEventIDInterDirc{}
	_ InterlockingDirectorate = &ShowDBSInterDirc{}
	_ InterlockingDirectorate = &ShowDBSJobsInterDirc{}
	_ InterlockingDirectorate = &ShowDBSJobQueriesInterDirc{}
	_ InterlockingDirectorate = &SortInterDirc{}
	_ InterlockingDirectorate = &StreamAggInterDirc{}
	_ InterlockingDirectorate = &BlockDualInterDirc{}
	_ InterlockingDirectorate = &BlockReaderInterlockingDirectorate{}
	_ InterlockingDirectorate = &BlockScanInterDirc{}
	_ InterlockingDirectorate = &TopNInterDirc{}
	_ InterlockingDirectorate = &UnionInterDirc{}

	// GlobalMemoryUsageTracker is the ancestor of all the InterlockingDirectorates' memory tracker and GlobalMemory Tracker
	GlobalMemoryUsageTracker *memory.Tracker
	// GlobalDiskUsageTracker is the ancestor of all the InterlockingDirectorates' disk tracker
	GlobalDiskUsageTracker *disk.Tracker
)

type baseInterlockingDirectorate struct {
	ctx             stochastikctx.Context
	id              int
	schemaReplicant *memex.Schema // output schemaReplicant
	initCap         int
	maxChunkSize    int
	children        []InterlockingDirectorate
	retFieldTypes   []*types.FieldType
	runtimeStats    *execdetails.BasicRuntimeStats
}

const (
	// globalPanicStorageExceed represents the panic message when out of storage quota.
	globalPanicStorageExceed string = "Out Of Global CausetStorage Quota!"
	// globalPanicMemoryExceed represents the panic message when out of memory limit.
	globalPanicMemoryExceed string = "Out Of Global Memory Limit!"
)

// globalPanicOnExceed panics when GlobalDisTracker storage usage exceeds storage quota.
type globalPanicOnExceed struct {
	mutex sync.Mutex // For synchronization.
}

func init() {
	action := &globalPanicOnExceed{}
	GlobalMemoryUsageTracker = memory.NewGlobalTracker(memory.LabelForGlobalMemory, -1)
	GlobalMemoryUsageTracker.SetSuperCowOrNoCausetOnExceed(action)
	GlobalDiskUsageTracker = disk.NewGlobalTrcaker(memory.LabelForGlobalStorage, -1)
	GlobalDiskUsageTracker.SetSuperCowOrNoCausetOnExceed(action)
}

// SetLogHook sets a hook for PanicOnExceed.
func (a *globalPanicOnExceed) SetLogHook(hook func(uint64)) {}

// CausetAction panics when storage usage exceeds storage quota.
func (a *globalPanicOnExceed) CausetAction(t *memory.Tracker) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	msg := ""
	switch t.Label() {
	case memory.LabelForGlobalStorage:
		msg = globalPanicStorageExceed
	case memory.LabelForGlobalMemory:
		msg = globalPanicMemoryExceed
	default:
		msg = "Out of Unknown Resource Quota!"
	}
	panic(msg)
}

// SetFallback sets a fallback action.
func (a *globalPanicOnExceed) SetFallback(memory.SuperCowOrNoCausetOnExceed) {}

// base returns the baseInterlockingDirectorate of an interlock, don't override this method!
func (e *baseInterlockingDirectorate) base() *baseInterlockingDirectorate {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseInterlockingDirectorate) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes all interlocks and release all resources.
func (e *baseInterlockingDirectorate) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current baseInterlockingDirectorate's schemaReplicant. If it is nil, then create and return a new one.
func (e *baseInterlockingDirectorate) Schema() *memex.Schema {
	if e.schemaReplicant == nil {
		return memex.NewSchema()
	}
	return e.schemaReplicant
}

// newFirstChunk creates a new chunk to buffer current interlock's result.
func newFirstChunk(e InterlockingDirectorate) *chunk.Chunk {
	base := e.base()
	return chunk.New(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// newList creates a new List to buffer current interlock's result.
func newList(e InterlockingDirectorate) *chunk.List {
	base := e.base()
	return chunk.NewList(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// retTypes returns all output defCausumn types.
func retTypes(e InterlockingDirectorate) []*types.FieldType {
	base := e.base()
	return base.retFieldTypes
}

// Next fills multiple rows into a chunk.
func (e *baseInterlockingDirectorate) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

func newBaseInterlockingDirectorate(ctx stochastikctx.Context, schemaReplicant *memex.Schema, id int, children ...InterlockingDirectorate) baseInterlockingDirectorate {
	e := baseInterlockingDirectorate{
		children:        children,
		ctx:             ctx,
		id:              id,
		schemaReplicant: schemaReplicant,
		initCap:         ctx.GetStochastikVars().InitChunkSize,
		maxChunkSize:    ctx.GetStochastikVars().MaxChunkSize,
	}
	if ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl != nil {
		if e.id > 0 {
			e.runtimeStats = &execdetails.BasicRuntimeStats{}
			e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl.RegisterStats(id, e.runtimeStats)
		}
	}
	if schemaReplicant != nil {
		defcaus := schemaReplicant.DeferredCausets
		e.retFieldTypes = make([]*types.FieldType, len(defcaus))
		for i := range defcaus {
			e.retFieldTypes[i] = defcaus[i].RetType
		}
	}
	return e
}

// InterlockingDirectorate is the physical implementation of a algebra operator.
//
// In MilevaDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protodefCaus. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution perceptron, a "Next" function call in MilevaDB will
// return a batch of rows, other than a single event in Volcano.
// NOTE: InterlockingDirectorates must call "chk.Reset()" before appending their results to it.
type InterlockingDirectorate interface {
	base() *baseInterlockingDirectorate
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error
	Close() error
	Schema() *memex.Schema
}

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e InterlockingDirectorate, req *chunk.Chunk) error {
	base := e.base()
	if base.runtimeStats != nil {
		start := time.Now()
		defer func() { base.runtimeStats.Record(time.Since(start), req.NumEvents()) }()
	}
	sessVars := base.ctx.GetStochastikVars()
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		return ErrQueryInterrupted
	}
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan(fmt.Sprintf("%T.Next", e), opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	if trace.IsEnabled() {
		defer trace.StartRegion(ctx, fmt.Sprintf("%T.Next", e)).End()
	}
	err := e.Next(ctx, req)

	if err != nil {
		return err
	}
	// recheck whether the stochastik/query is killed during the Next()
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		err = ErrQueryInterrupted
	}
	return err
}

// CancelDBSJobsInterDirc represents a cancel DBS jobs interlock.
type CancelDBSJobsInterDirc struct {
	baseInterlockingDirectorate

	cursor int
	jobIDs []int64
	errs   []error
}

// Next implements the InterlockingDirectorate Next interface.
func (e *CancelDBSJobsInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendString(0, fmt.Sprintf("%d", e.jobIDs[i]))
		if e.errs[i] != nil {
			req.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			req.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// ShowNextEventIDInterDirc represents a show the next event ID interlock.
type ShowNextEventIDInterDirc struct {
	baseInterlockingDirectorate
	tblName *ast.BlockName
	done    bool
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ShowNextEventIDInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	is := petri.GetPetri(e.ctx).SchemaReplicant()
	tbl, err := is.BlockByName(e.tblName.Schema, e.tblName.Name)
	if err != nil {
		return err
	}
	tblMeta := tbl.Meta()

	allocators := tbl.SlabPredictors(e.ctx)
	for _, alloc := range allocators {
		nextGlobalID, err := alloc.NextGlobalAutoID(tblMeta.ID)
		if err != nil {
			return err
		}

		var defCausName, idType string
		switch alloc.GetType() {
		case autoid.EventIDAllocType, autoid.AutoIncrementType:
			idType = "AUTO_INCREMENT"
			if defCaus := tblMeta.GetAutoIncrementDefCausInfo(); defCaus != nil {
				defCausName = defCaus.Name.O
			} else {
				defCausName = perceptron.ExtraHandleName.O
			}
		case autoid.AutoRandomType:
			idType = "AUTO_RANDOM"
			defCausName = tblMeta.GetPkName().O
		case autoid.SequenceType:
			idType = "SEQUENCE"
			defCausName = ""
		default:
			return autoid.ErrInvalidSlabPredictorType.GenWithStackByArgs()
		}

		req.AppendString(0, e.tblName.Schema.O)
		req.AppendString(1, e.tblName.Name.O)
		req.AppendString(2, defCausName)
		req.AppendInt64(3, nextGlobalID)
		req.AppendString(4, idType)
	}

	e.done = true
	return nil
}

// ShowDBSInterDirc represents a show DBS interlock.
type ShowDBSInterDirc struct {
	baseInterlockingDirectorate

	dbsTenantID string
	selfID      string
	dbsInfo     *admin.DBSInfo
	done        bool
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ShowDBSInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	dbsJobs := ""
	query := ""
	l := len(e.dbsInfo.Jobs)
	for i, job := range e.dbsInfo.Jobs {
		dbsJobs += job.String()
		query += job.Query
		if i != l-1 {
			dbsJobs += "\n"
			query += "\n"
		}
	}

	serverInfo, err := infosync.GetServerInfoByID(ctx, e.dbsTenantID)
	if err != nil {
		return err
	}

	serverAddress := serverInfo.IP + ":" +
		strconv.FormatUint(uint64(serverInfo.Port), 10)

	req.AppendInt64(0, e.dbsInfo.SchemaVer)
	req.AppendString(1, e.dbsTenantID)
	req.AppendString(2, serverAddress)
	req.AppendString(3, dbsJobs)
	req.AppendString(4, e.selfID)
	req.AppendString(5, query)

	e.done = true
	return nil
}

// ShowDBSJobsInterDirc represent a show DBS jobs interlock.
type ShowDBSJobsInterDirc struct {
	baseInterlockingDirectorate
	DBSJobRetriever

	jobNumber int
	is        schemareplicant.SchemaReplicant
	done      bool
}

// DBSJobRetriever retrieve the DBSJobs.
type DBSJobRetriever struct {
	runningJobs    []*perceptron.Job
	historyJobIter *spacetime.LastJobIterator
	cursor         int
	is             schemareplicant.SchemaReplicant
	activeRoles    []*auth.RoleIdentity
	cacheJobs      []*perceptron.Job
}

func (e *DBSJobRetriever) initial(txn ekv.Transaction) error {
	jobs, err := admin.GetDBSJobs(txn)
	if err != nil {
		return err
	}
	m := spacetime.NewMeta(txn)
	e.historyJobIter, err = m.GetLastHistoryDBSJobsIterator()
	if err != nil {
		return err
	}
	e.runningJobs = jobs
	e.cursor = 0
	return nil
}

func (e *DBSJobRetriever) appendJobToChunk(req *chunk.Chunk, job *perceptron.Job, checker privilege.Manager) {
	schemaName := job.SchemaName
	blockName := ""
	finishTS := uint64(0)
	if job.BinlogInfo != nil {
		finishTS = job.BinlogInfo.FinishedTS
		if job.BinlogInfo.BlockInfo != nil {
			blockName = job.BinlogInfo.BlockInfo.Name.L
		}
		if len(schemaName) == 0 && job.BinlogInfo.DBInfo != nil {
			schemaName = job.BinlogInfo.DBInfo.Name.L
		}
	}
	// For compatibility, the old version of DBS Job wasn't causetstore the schemaReplicant name and causet name.
	if len(schemaName) == 0 {
		schemaName = getSchemaName(e.is, job.SchemaID)
	}
	if len(blockName) == 0 {
		blockName = getBlockName(e.is, job.BlockID)
	}

	startTime := ts2Time(job.StartTS)
	finishTime := ts2Time(finishTS)

	// Check the privilege.
	if checker != nil && !checker.RequestVerification(e.activeRoles, strings.ToLower(schemaName), strings.ToLower(blockName), "", allegrosql.AllPrivMask) {
		return
	}

	req.AppendInt64(0, job.ID)
	req.AppendString(1, schemaName)
	req.AppendString(2, blockName)
	req.AppendString(3, job.Type.String())
	req.AppendString(4, job.SchemaState.String())
	req.AppendInt64(5, job.SchemaID)
	req.AppendInt64(6, job.BlockID)
	req.AppendInt64(7, job.EventCount)
	req.AppendTime(8, startTime)
	if finishTS > 0 {
		req.AppendTime(9, finishTime)
	} else {
		req.AppendNull(9)
	}
	req.AppendString(10, job.State.String())
}

func ts2Time(timestamp uint64) types.Time {
	duration := time.Duration(math.Pow10(9-int(types.DefaultFsp))) * time.Nanosecond
	t := perceptron.TSConvert2Time(timestamp)
	t.Truncate(duration)
	return types.NewTime(types.FromGoTime(t), allegrosql.TypeDatetime, types.DefaultFsp)
}

// ShowDBSJobQueriesInterDirc represents a show DBS job queries interlock.
// The jobs id that is given by 'admin show dbs job queries' memex,
// only be searched in the latest 10 history jobs
type ShowDBSJobQueriesInterDirc struct {
	baseInterlockingDirectorate

	cursor int
	jobs   []*perceptron.Job
	jobIDs []int64
}

// Open implements the InterlockingDirectorate Open interface.
func (e *ShowDBSJobQueriesInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	jobs, err := admin.GetDBSJobs(txn)
	if err != nil {
		return err
	}
	historyJobs, err := admin.GetHistoryDBSJobs(txn, admin.DefNumHistoryJobs)
	if err != nil {
		return err
	}

	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)

	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ShowDBSJobQueriesInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if len(e.jobIDs) >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.jobs)-e.cursor)
	for _, id := range e.jobIDs {
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			if id == e.jobs[i].ID {
				req.AppendString(0, e.jobs[i].Query)
			}
		}
	}
	e.cursor += numCurBatch
	return nil
}

// Open implements the InterlockingDirectorate Open interface.
func (e *ShowDBSJobsInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	e.DBSJobRetriever.is = e.is
	if e.jobNumber == 0 {
		e.jobNumber = admin.DefNumHistoryJobs
	}
	err = e.DBSJobRetriever.initial(txn)
	if err != nil {
		return err
	}
	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ShowDBSJobsInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if (e.cursor - len(e.runningJobs)) >= e.jobNumber {
		return nil
	}
	count := 0

	// Append running dbs jobs.
	if e.cursor < len(e.runningJobs) {
		numCurBatch := mathutil.Min(req.Capacity(), len(e.runningJobs)-e.cursor)
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			e.appendJobToChunk(req, e.runningJobs[i], nil)
		}
		e.cursor += numCurBatch
		count += numCurBatch
	}

	// Append history dbs jobs.
	var err error
	if count < req.Capacity() {
		num := req.Capacity() - count
		remainNum := e.jobNumber - (e.cursor - len(e.runningJobs))
		num = mathutil.Min(num, remainNum)
		e.cacheJobs, err = e.historyJobIter.GetLastJobs(num, e.cacheJobs)
		if err != nil {
			return err
		}
		for _, job := range e.cacheJobs {
			e.appendJobToChunk(req, job, nil)
		}
		e.cursor += len(e.cacheJobs)
	}
	return nil
}

func getSchemaName(is schemareplicant.SchemaReplicant, id int64) string {
	var schemaName string
	DBInfo, ok := is.SchemaByID(id)
	if ok {
		schemaName = DBInfo.Name.O
		return schemaName
	}

	return schemaName
}

func getBlockName(is schemareplicant.SchemaReplicant, id int64) string {
	var blockName string
	causet, ok := is.BlockByID(id)
	if ok {
		blockName = causet.Meta().Name.O
		return blockName
	}

	return blockName
}

// CheckBlockInterDirc represents a check causet interlock.
// It is built from the "admin check causet" memex, and it checks if the
// index matches the records in the causet.
type CheckBlockInterDirc struct {
	baseInterlockingDirectorate

	dbName     string
	causet     causet.Block
	indexInfos []*perceptron.IndexInfo
	srcs       []*IndexLookUpInterlockingDirectorate
	done       bool
	is         schemareplicant.SchemaReplicant
	exitCh     chan struct{}
	retCh      chan error
	checHoTTex bool
}

// Open implements the InterlockingDirectorate Open interface.
func (e *CheckBlockInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	for _, src := range e.srcs {
		if err := src.Open(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	e.done = false
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *CheckBlockInterDirc) Close() error {
	var firstErr error
	for _, src := range e.srcs {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *CheckBlockInterDirc) checkBlockIndexHandle(ctx context.Context, idxInfo *perceptron.IndexInfo) error {
	// For partition causet, there will be multi same index indexLookUpReaders on different partitions.
	for _, src := range e.srcs {
		if src.index.Name.L == idxInfo.Name.L {
			err := e.checHoTTexHandle(ctx, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *CheckBlockInterDirc) checHoTTexHandle(ctx context.Context, src *IndexLookUpInterlockingDirectorate) error {
	defcaus := src.schemaReplicant.DeferredCausets
	retFieldTypes := make([]*types.FieldType, len(defcaus))
	for i := range defcaus {
		retFieldTypes[i] = defcaus[i].RetType
	}
	chk := chunk.New(retFieldTypes, e.initCap, e.maxChunkSize)

	var err error
	for {
		err = Next(ctx, src, chk)
		if err != nil {
			break
		}
		if chk.NumEvents() == 0 {
			break
		}

		select {
		case <-e.exitCh:
			return nil
		default:
		}
	}
	e.retCh <- errors.Trace(err)
	return errors.Trace(err)
}

func (e *CheckBlockInterDirc) handlePanic(r interface{}) {
	if r != nil {
		e.retCh <- errors.Errorf("%v", r)
	}
}

// Next implements the InterlockingDirectorate Next interface.
func (e *CheckBlockInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done || len(e.srcs) == 0 {
		return nil
	}
	defer func() { e.done = true }()

	idxNames := make([]string, 0, len(e.indexInfos))
	for _, idx := range e.indexInfos {
		idxNames = append(idxNames, idx.Name.O)
	}
	greater, idxOffset, err := admin.ChecHoTTicesCount(e.ctx, e.dbName, e.causet.Meta().Name.O, idxNames)
	if err != nil {
		// For admin check index memex, for speed up and compatibility, doesn't do below checks.
		if e.checHoTTex {
			return errors.Trace(err)
		}
		if greater == admin.IdxCntGreater {
			err = e.checkBlockIndexHandle(ctx, e.indexInfos[idxOffset])
		} else if greater == admin.TblCntGreater {
			err = e.checkBlockRecord(idxOffset)
		}
		if err != nil && admin.ErrDataInConsistent.Equal(err) {
			return ErrAdminCheckBlock.GenWithStack("%v err:%v", e.causet.Meta().Name, err)
		}
		return errors.Trace(err)
	}

	// The number of causet rows is equal to the number of index rows.
	// TODO: Make the value of concurrency adjusblock. And we can consider the number of records.
	concurrency := 3
	wg := sync.WaitGroup{}
	for i := range e.srcs {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			soliton.WithRecovery(func() {
				err1 := e.checHoTTexHandle(ctx, e.srcs[num])
				if err1 != nil {
					logutil.Logger(ctx).Info("check index handle failed", zap.Error(err1))
				}
			}, e.handlePanic)
		}(i)

		if (i+1)%concurrency == 0 {
			wg.Wait()
		}
	}

	for i := 0; i < len(e.srcs); i++ {
		err = <-e.retCh
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *CheckBlockInterDirc) checkBlockRecord(idxOffset int) error {
	idxInfo := e.indexInfos[idxOffset]
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	if e.causet.Meta().GetPartitionInfo() == nil {
		idx := blocks.NewIndex(e.causet.Meta().ID, e.causet.Meta(), idxInfo)
		return admin.CheckRecordAndIndex(e.ctx, txn, e.causet, idx)
	}

	info := e.causet.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := e.causet.(causet.PartitionedBlock).GetPartition(pid)
		idx := blocks.NewIndex(def.ID, e.causet.Meta(), idxInfo)
		if err := admin.CheckRecordAndIndex(e.ctx, txn, partition, idx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ShowSlowInterDirc represents the interlock of showing the slow queries.
// It is build from the "admin show slow" memex:
//	admin show slow top [internal | all] N
//	admin show slow recent N
type ShowSlowInterDirc struct {
	baseInterlockingDirectorate

	ShowSlow *ast.ShowSlow
	result   []*petri.SlowQueryInfo
	cursor   int
}

// Open implements the InterlockingDirectorate Open interface.
func (e *ShowSlowInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}

	dom := petri.GetPetri(e.ctx)
	e.result = dom.ShowSlowQuery(e.ShowSlow)
	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ShowSlowInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= len(e.result) {
		return nil
	}

	for e.cursor < len(e.result) && req.NumEvents() < e.maxChunkSize {
		slow := e.result[e.cursor]
		req.AppendString(0, slow.ALLEGROALLEGROSQL)
		req.AppendTime(1, types.NewTime(types.FromGoTime(slow.Start), allegrosql.TypeTimestamp, types.MaxFsp))
		req.AppendDuration(2, types.Duration{Duration: slow.Duration, Fsp: types.MaxFsp})
		req.AppendString(3, slow.Detail.String())
		if slow.Succ {
			req.AppendInt64(4, 1)
		} else {
			req.AppendInt64(4, 0)
		}
		req.AppendUint64(5, slow.ConnID)
		req.AppendUint64(6, slow.TxnTS)
		req.AppendString(7, slow.User)
		req.AppendString(8, slow.EDB)
		req.AppendString(9, slow.BlockIDs)
		req.AppendString(10, slow.IndexNames)
		if slow.Internal {
			req.AppendInt64(11, 1)
		} else {
			req.AppendInt64(11, 0)
		}
		req.AppendString(12, slow.Digest)
		e.cursor++
	}
	return nil
}

// SelectLockInterDirc represents a select dagger interlock.
// It is built from the "SELECT .. FOR UFIDelATE" or the "SELECT .. LOCK IN SHARE MODE" memex.
// For "SELECT .. FOR UFIDelATE" memex, it locks every event key from source InterlockingDirectorate.
// After the execution, the keys are buffered in transaction, and will be sent to KV
// when doing commit. If there is any key already locked by another transaction,
// the transaction will rollback and retry.
type SelectLockInterDirc struct {
	baseInterlockingDirectorate

	Lock *ast.SelectLockInfo
	keys []ekv.Key

	tblID2Handle     map[int64][]causetembedded.HandleDefCauss
	partitionedBlock []causet.PartitionedBlock

	// tblID2Block is cached to reduce cost.
	tblID2Block map[int64]causet.PartitionedBlock
}

// Open implements the InterlockingDirectorate Open interface.
func (e *SelectLockInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}

	if len(e.tblID2Handle) > 0 && len(e.partitionedBlock) > 0 {
		e.tblID2Block = make(map[int64]causet.PartitionedBlock, len(e.partitionedBlock))
		for id := range e.tblID2Handle {
			for _, p := range e.partitionedBlock {
				if id == p.Meta().ID {
					e.tblID2Block[id] = p
				}
			}
		}
	}

	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *SelectLockInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	// If there's no handle or it's not a `SELECT FOR UFIDelATE` memex.
	if len(e.tblID2Handle) == 0 || (!causetembedded.IsSelectForUFIDelateLockType(e.Lock.LockType)) {
		return nil
	}

	if req.NumEvents() > 0 {
		iter := chunk.NewIterator4Chunk(req)
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			for id, defcaus := range e.tblID2Handle {
				physicalID := id
				if pt, ok := e.tblID2Block[id]; ok {
					// On a partitioned causet, we have to use physical ID to encode the dagger key!
					p, err := pt.GetPartitionByEvent(e.ctx, event.GetCausetEvent(e.base().retFieldTypes))
					if err != nil {
						return err
					}
					physicalID = p.GetPhysicalID()
				}

				for _, defCaus := range defcaus {
					handle, err := defCaus.BuildHandle(event)
					if err != nil {
						return err
					}
					e.keys = append(e.keys, blockcodec.EncodeEventKeyWithHandle(physicalID, handle))
				}
			}
		}
		return nil
	}
	lockWaitTime := e.ctx.GetStochastikVars().LockWaitTimeout
	if e.Lock.LockType == ast.SelectLockForUFIDelateNoWait {
		lockWaitTime = ekv.LockNoWait
	} else if e.Lock.LockType == ast.SelectLockForUFIDelateWaitN {
		lockWaitTime = int64(e.Lock.WaitSec) * 1000
	}

	return doLockKeys(ctx, e.ctx, newLockCtx(e.ctx.GetStochastikVars(), lockWaitTime), e.keys...)
}

func newLockCtx(seVars *variable.StochastikVars, lockWaitTime int64) *ekv.LockCtx {
	return &ekv.LockCtx{
		Killed:                &seVars.Killed,
		ForUFIDelateTS:        seVars.TxnCtx.GetForUFIDelateTS(),
		LockWaitTime:          lockWaitTime,
		WaitStartTime:         seVars.StmtCtx.GetLockWaitStartTime(),
		PessimisticLockWaited: &seVars.StmtCtx.PessimisticLockWaited,
		LockKeysDuration:      &seVars.StmtCtx.LockKeysDuration,
		LockKeysCount:         &seVars.StmtCtx.LockKeysCount,
		LockExpired:           &seVars.TxnCtx.LockExpire,
	}
}

// doLockKeys is the main entry for pessimistic dagger keys
// waitTime means the dagger operation will wait in milliseconds if target key is already
// locked by others. used for (select for uFIDelate nowait) situation
// except 0 means alwaysWait 1 means nowait
func doLockKeys(ctx context.Context, se stochastikctx.Context, lockCtx *ekv.LockCtx, keys ...ekv.Key) error {
	sctx := se.GetStochastikVars().StmtCtx
	if !sctx.InUFIDelateStmt && !sctx.InDeleteStmt {
		atomic.StoreUint32(&se.GetStochastikVars().TxnCtx.ForUFIDelate, 1)
	}
	// Lock keys only once when finished fetching all results.
	txn, err := se.Txn(true)
	if err != nil {
		return err
	}
	var lockKeyStats *execdetails.LockKeysDetails
	ctx = context.WithValue(ctx, execdetails.LockKeysDetailCtxKey, &lockKeyStats)
	err = txn.LockKeys(stochastikctx.SetCommitCtx(ctx, se), lockCtx, keys...)
	if lockKeyStats != nil {
		sctx.MergeLockKeysInterDircDetails(lockKeyStats)
	}
	return err
}

// LimitInterDirc represents limit interlock
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitInterDirc struct {
	baseInterlockingDirectorate

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk
}

// Next implements the InterlockingDirectorate Next interface.
func (e *LimitInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		// transfer req's requiredEvents to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredEvents(req.RequiredEvents(), e.maxChunkSize)
		err := Next(ctx, e.children[0], e.adjustRequiredEvents(e.childResult))
		if err != nil {
			return err
		}
		batchSize := uint64(e.childResult.NumEvents())
		// no more data.
		if batchSize == 0 {
			return nil
		}
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				end = e.end - e.cursor
			}
			e.cursor += end
			if begin == end {
				break
			}
			req.Append(e.childResult, int(begin), int(end))
			return nil
		}
		e.cursor += batchSize
	}
	e.adjustRequiredEvents(req)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	batchSize := uint64(req.NumEvents())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		req.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize
	return nil
}

// Open implements the InterlockingDirectorate Open interface.
func (e *LimitInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	e.childResult = newFirstChunk(e.children[0])
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *LimitInterDirc) Close() error {
	e.childResult = nil
	return e.baseInterlockingDirectorate.Close()
}

func (e *LimitInterDirc) adjustRequiredEvents(chk *chunk.Chunk) *chunk.Chunk {
	// the limit of maximum number of rows the LimitInterDirc should read
	limitTotal := int(e.end - e.cursor)

	var limitRequired int
	if e.cursor < e.begin {
		// if cursor is less than begin, it have to read (begin-cursor) rows to ignore
		// and then read chk.RequiredEvents() rows to return,
		// so the limit is (begin-cursor)+chk.RequiredEvents().
		limitRequired = int(e.begin) - int(e.cursor) + chk.RequiredEvents()
	} else {
		// if cursor is equal or larger than begin, just read chk.RequiredEvents() rows to return.
		limitRequired = chk.RequiredEvents()
	}

	return chk.SetRequiredEvents(mathutil.Min(limitTotal, limitRequired), e.maxChunkSize)
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the interlock package because of the dependency cycle.
	// So we assign a function implemented in the interlock package to the plan package to avoid the dependency cycle.
	causetembedded.EvalSubqueryFirstEvent = func(ctx context.Context, p causetembedded.PhysicalCauset, is schemareplicant.SchemaReplicant, sctx stochastikctx.Context) ([]types.Causet, error) {
		defer func(begin time.Time) {
			s := sctx.GetStochastikVars()
			s.RewritePhaseInfo.PreprocessSubQueries++
			s.RewritePhaseInfo.DurationPreprocessSubQuery += time.Since(begin)
		}(time.Now())

		if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
			span1 := span.Tracer().StartSpan("interlock.EvalSubQuery", opentracing.ChildOf(span.Context()))
			defer span1.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span1)
		}

		e := &interlockBuilder{is: is, ctx: sctx}
		exec := e.build(p)
		if e.err != nil {
			return nil, e.err
		}
		err := exec.Open(ctx)
		defer terror.Call(exec.Close)
		if err != nil {
			return nil, err
		}
		chk := newFirstChunk(exec)
		for {
			err = Next(ctx, exec, chk)
			if err != nil {
				return nil, err
			}
			if chk.NumEvents() == 0 {
				return nil, nil
			}
			event := chk.GetEvent(0).GetCausetEvent(retTypes(exec))
			return event, err
		}
	}
}

// BlockDualInterDirc represents a dual causet interlock.
type BlockDualInterDirc struct {
	baseInterlockingDirectorate

	// numDualEvents can only be 0 or 1.
	numDualEvents int
	numReturned   int
}

// Open implements the InterlockingDirectorate Open interface.
func (e *BlockDualInterDirc) Open(ctx context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *BlockDualInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.numReturned >= e.numDualEvents {
		return nil
	}
	if e.Schema().Len() == 0 {
		req.SetNumVirtualEvents(1)
	} else {
		for i := range e.Schema().DeferredCausets {
			req.AppendNull(i)
		}
	}
	e.numReturned = e.numDualEvents
	return nil
}

// SelectionInterDirc represents a filter interlock.
type SelectionInterDirc struct {
	baseInterlockingDirectorate

	batched     bool
	filters     []memex.Expression
	selected    []bool
	inputIter   *chunk.Iterator4Chunk
	inputEvent  chunk.Event
	childResult *chunk.Chunk

	memTracker *memory.Tracker
}

// Open implements the InterlockingDirectorate Open interface.
func (e *SelectionInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	return e.open(ctx)
}

func (e *SelectionInterDirc) open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetStochastikVars().StmtCtx.MemTracker)
	e.childResult = newFirstChunk(e.children[0])
	e.memTracker.Consume(e.childResult.MemoryUsage())
	e.batched = memex.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputEvent = e.inputIter.End()
	return nil
}

// Close implements causetembedded.Causet Close interface.
func (e *SelectionInterDirc) Close() error {
	e.memTracker.Consume(-e.childResult.MemoryUsage())
	e.childResult = nil
	e.selected = nil
	return e.baseInterlockingDirectorate.Close()
}

// Next implements the InterlockingDirectorate Next interface.
func (e *SelectionInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)

	if !e.batched {
		return e.unBatchedNext(ctx, req)
	}

	for {
		for ; e.inputEvent != e.inputIter.End(); e.inputEvent = e.inputIter.Next() {
			if !e.selected[e.inputEvent.Idx()] {
				continue
			}
			if req.IsFull() {
				return nil
			}
			req.AppendEvent(e.inputEvent)
		}
		mSize := e.childResult.MemoryUsage()
		err := Next(ctx, e.children[0], e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		// no more data.
		if e.childResult.NumEvents() == 0 {
			return nil
		}
		e.selected, err = memex.VectorizedFilter(e.ctx, e.filters, e.inputIter, e.selected)
		if err != nil {
			return err
		}
		e.inputEvent = e.inputIter.Begin()
	}
}

// unBatchedNext filters input rows one by one and returns once an input event is selected.
// For allegrosql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionInterDirc) unBatchedNext(ctx context.Context, chk *chunk.Chunk) error {
	for {
		for ; e.inputEvent != e.inputIter.End(); e.inputEvent = e.inputIter.Next() {
			selected, _, err := memex.EvalBool(e.ctx, e.filters, e.inputEvent)
			if err != nil {
				return err
			}
			if selected {
				chk.AppendEvent(e.inputEvent)
				e.inputEvent = e.inputIter.Next()
				return nil
			}
		}
		mSize := e.childResult.MemoryUsage()
		err := Next(ctx, e.children[0], e.childResult)
		e.memTracker.Consume(e.childResult.MemoryUsage() - mSize)
		if err != nil {
			return err
		}
		e.inputEvent = e.inputIter.Begin()
		// no more data.
		if e.childResult.NumEvents() == 0 {
			return nil
		}
	}
}

// BlockScanInterDirc is a causet scan interlock without result fields.
type BlockScanInterDirc struct {
	baseInterlockingDirectorate

	t                     causet.Block
	defCausumns           []*perceptron.DeferredCausetInfo
	virtualBlockChunkList *chunk.List
	virtualBlockChunkIdx  int
}

// Next implements the InterlockingDirectorate Next interface.
func (e *BlockScanInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	return e.nextChunk4SchemaReplicant(ctx, req)
}

func (e *BlockScanInterDirc) nextChunk4SchemaReplicant(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.virtualBlockChunkList == nil {
		e.virtualBlockChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		defCausumns := make([]*causet.DeferredCauset, e.schemaReplicant.Len())
		for i, defCausInfo := range e.defCausumns {
			defCausumns[i] = causet.ToDeferredCauset(defCausInfo)
		}
		mublockEvent := chunk.MutEventFromTypes(retTypes(e))
		err := e.t.IterRecords(e.ctx, nil, defCausumns, func(_ ekv.Handle, rec []types.Causet, defcaus []*causet.DeferredCauset) (bool, error) {
			mublockEvent.SetCausets(rec...)
			e.virtualBlockChunkList.AppendEvent(mublockEvent.ToEvent())
			return true, nil
		})
		if err != nil {
			return err
		}
	}
	// no more data.
	if e.virtualBlockChunkIdx >= e.virtualBlockChunkList.NumChunks() {
		return nil
	}
	virtualBlockChunk := e.virtualBlockChunkList.GetChunk(e.virtualBlockChunkIdx)
	e.virtualBlockChunkIdx++
	chk.SwapDeferredCausets(virtualBlockChunk)
	return nil
}

// Open implements the InterlockingDirectorate Open interface.
func (e *BlockScanInterDirc) Open(ctx context.Context) error {
	e.virtualBlockChunkList = nil
	return nil
}

// MaxOneEventInterDirc checks if the number of rows that a query returns is at maximum one.
// It's built from subquery memex.
type MaxOneEventInterDirc struct {
	baseInterlockingDirectorate

	evaluated bool
}

// Open implements the InterlockingDirectorate Open interface.
func (e *MaxOneEventInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	e.evaluated = false
	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *MaxOneEventInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}

	if num := req.NumEvents(); num == 0 {
		for i := range e.schemaReplicant.DeferredCausets {
			req.AppendNull(i)
		}
		return nil
	} else if num != 1 {
		return errors.New("subquery returns more than 1 event")
	}

	childChunk := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], childChunk)
	if err != nil {
		return err
	}
	if childChunk.NumEvents() != 0 {
		return errors.New("subquery returns more than 1 event")
	}

	return nil
}

// UnionInterDirc pulls all it's children's result and returns to its parent directly.
// A "resultPuller" is started for every child to pull result from that child and push it to the "resultPool", the used
// "Chunk" is obtained from the corresponding "resourcePool". All resultPullers are running concurrently.
//                             +----------------+
//   +---> resourcePool 1 ---> | resultPuller 1 |-----+
//   |                         +----------------+     |
//   |                                                |
//   |                         +----------------+     v
//   +---> resourcePool 2 ---> | resultPuller 2 |-----> resultPool ---+
//   |                         +----------------+     ^               |
//   |                               ......           |               |
//   |                         +----------------+     |               |
//   +---> resourcePool n ---> | resultPuller n |-----+               |
//   |                         +----------------+                     |
//   |                                                                |
//   |                          +-------------+                       |
//   |--------------------------| main thread | <---------------------+
//                              +-------------+
type UnionInterDirc struct {
	baseInterlockingDirectorate
	concurrency int
	childIDChan chan int

	stopFetchData atomic.Value

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult

	results     []*chunk.Chunk
	wg          sync.WaitGroup
	initialized bool
}

// unionWorkerResult stores the result for a union worker.
// A "resultPuller" is started for every child to pull result from that child, unionWorkerResult is used to causetstore that pulled result.
// "src" is used for Chunk reuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
// enable the corresponding "resultPuller" continue to work.
type unionWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *UnionInterDirc) waitAllFinished() {
	e.wg.Wait()
	close(e.resultPool)
}

// Open implements the InterlockingDirectorate Open interface.
func (e *UnionInterDirc) Open(ctx context.Context) error {
	if err := e.baseInterlockingDirectorate.Open(ctx); err != nil {
		return err
	}
	e.stopFetchData.CausetStore(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionInterDirc) initialize(ctx context.Context) {
	if e.concurrency > len(e.children) {
		e.concurrency = len(e.children)
	}
	for i := 0; i < e.concurrency; i++ {
		e.results = append(e.results, newFirstChunk(e.children[0]))
	}
	e.resultPool = make(chan *unionWorkerResult, e.concurrency)
	e.resourcePools = make([]chan *chunk.Chunk, e.concurrency)
	e.childIDChan = make(chan int, len(e.children))
	for i := 0; i < e.concurrency; i++ {
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.results[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	for i := 0; i < len(e.children); i++ {
		e.childIDChan <- i
	}
	close(e.childIDChan)
	go e.waitAllFinished()
}

func (e *UnionInterDirc) resultPuller(ctx context.Context, workerID int) {
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[workerID],
	}
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("resultPuller panicked", zap.String("stack", string(buf)))
			result.err = errors.Errorf("%v", r)
			e.resultPool <- result
			e.stopFetchData.CausetStore(true)
		}
		e.wg.Done()
	}()
	for childID := range e.childIDChan {
		for {
			if e.stopFetchData.Load().(bool) {
				return
			}
			select {
			case <-e.finished:
				return
			case result.chk = <-e.resourcePools[workerID]:
			}
			result.err = Next(ctx, e.children[childID], result.chk)
			if result.err == nil && result.chk.NumEvents() == 0 {
				e.resourcePools[workerID] <- result.chk
				break
			}
			e.resultPool <- result
			if result.err != nil {
				e.stopFetchData.CausetStore(true)
				return
			}
		}
	}
}

// Next implements the InterlockingDirectorate Next interface.
func (e *UnionInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if !e.initialized {
		e.initialize(ctx)
		e.initialized = true
	}
	result, ok := <-e.resultPool
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}

	req.SwapDeferredCausets(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *UnionInterDirc) Close() error {
	if e.finished != nil {
		close(e.finished)
	}
	e.results = nil
	if e.resultPool != nil {
		for range e.resultPool {
		}
	}
	e.resourcePools = nil
	if e.childIDChan != nil {
		for range e.childIDChan {
		}
	}
	return e.baseInterlockingDirectorate.Close()
}

// ResetContextOfStmt resets the StmtContext and stochastik variables.
// Before every execution, we must clear memex context.
func ResetContextOfStmt(ctx stochastikctx.Context, s ast.StmtNode) (err error) {
	vars := ctx.GetStochastikVars()
	sc := &stmtctx.StatementContext{
		TimeZone:    vars.Location(),
		MemTracker:  memory.NewTracker(memory.LabelForALLEGROSQLText, vars.MemQuotaQuery),
		DiskTracker: disk.NewTracker(memory.LabelForALLEGROSQLText, -1),
		TaskID:      stmtctx.AllocateTaskID(),
	}
	sc.MemTracker.AttachToGlobalTracker(GlobalMemoryUsageTracker)
	globalConfig := config.GetGlobalConfig()
	if globalConfig.OOMUseTmpStorage && GlobalDiskUsageTracker != nil {
		sc.DiskTracker.AttachToGlobalTracker(GlobalDiskUsageTracker)
	}
	switch globalConfig.OOMCausetAction {
	case config.OOMCausetActionCancel:
		action := &memory.PanicOnExceed{ConnID: ctx.GetStochastikVars().ConnectionID}
		action.SetLogHook(petri.GetPetri(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetSuperCowOrNoCausetOnExceed(action)
	case config.OOMCausetActionLog:
		fallthrough
	default:
		action := &memory.RepLogCausetOnExceed{ConnID: ctx.GetStochastikVars().ConnectionID}
		action.SetLogHook(petri.GetPetri(ctx).ExpensiveQueryHandle().LogOnQueryExceedMemQuota)
		sc.MemTracker.SetSuperCowOrNoCausetOnExceed(action)
	}
	if execStmt, ok := s.(*ast.InterDircuteStmt); ok {
		s, err = causet.GetPreparedStmt(execStmt, vars)
		if err != nil {
			return
		}
	}
	// execute missed stmtID uses empty allegrosql
	sc.OriginalALLEGROSQL = s.Text()
	if explainStmt, ok := s.(*ast.ExplainStmt); ok {
		sc.InExplainStmt = true
		s = explainStmt.Stmt
	}
	if _, ok := s.(*ast.ExplainForStmt); ok {
		sc.InExplainStmt = true
	}
	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictALLEGROSQLMode) to avoid setting the same bool variables and
	// pushing them down to EinsteinDB as flags.
	switch stmt := s.(type) {
	case *ast.UFIDelateStmt:
		ResetUFIDelateStmtCtx(sc, stmt, vars)
	case *ast.DeleteStmt:
		sc.InDeleteStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert memex (not for uFIDelate memex), disabling the StrictALLEGROSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning or BadNullAsWarning,
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.CreateBlockStmt, *ast.AlterBlockStmt:
		// Make sure the sql_mode is strict when checking defCausumn default value.
	case *ast.LoadDataStmt:
		sc.DupKeyAsWarning = true
		sc.BadNullAsWarning = true
		sc.TruncateAsWarning = !vars.StrictALLEGROSQLMode
		sc.InLoadDataStmt = true
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// see https://dev.allegrosql.com/doc/refman/5.7/en/allegrosql-mode.html#allegrosql-mode-strict
		// said "For memexs such as SELECT that do not change data, invalid values
		// generate a warning in strict mode, not an error."
		// and https://dev.allegrosql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		sc.OverflowAsWarning = true

		// Return warning for truncate error in selection.
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.ALLEGROSQLCache
		}
	case *ast.SetOprStmt:
		sc.InSelectStmt = true
		sc.OverflowAsWarning = true
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
	case *ast.ShowStmt:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors {
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		sc.IgnoreTruncate = false
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
	default:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
	}
	vars.PreparedParams = vars.PreparedParams[:0]
	if priority := allegrosql.PriorityEnum(atomic.LoadInt32(&variable.ForcePriority)); priority != allegrosql.NoPriority {
		sc.Priority = priority
	}
	if vars.StmtCtx.LastInsertID > 0 {
		sc.PrevLastInsertID = vars.StmtCtx.LastInsertID
	} else {
		sc.PrevLastInsertID = vars.StmtCtx.PrevLastInsertID
	}
	sc.PrevAffectedEvents = 0
	if vars.StmtCtx.InUFIDelateStmt || vars.StmtCtx.InDeleteStmt || vars.StmtCtx.InInsertStmt {
		sc.PrevAffectedEvents = int64(vars.StmtCtx.AffectedEvents())
	} else if vars.StmtCtx.InSelectStmt {
		sc.PrevAffectedEvents = -1
	}
	if globalConfig.EnableDefCauslectInterDircutionInfo {
		sc.RuntimeStatsDefCausl = execdetails.NewRuntimeStatsDefCausl()
	}

	sc.TblInfo2UnionScan = make(map[*perceptron.BlockInfo]bool)
	errCount, warnCount := vars.StmtCtx.NumErrorWarnings()
	vars.SysErrorCount = errCount
	vars.SysWarningCount = warnCount
	vars.StmtCtx = sc
	vars.PrevFoundInCausetCache = vars.FoundInCausetCache
	vars.FoundInCausetCache = false
	return
}

// ResetUFIDelateStmtCtx resets memex context for UFIDelateStmt.
func ResetUFIDelateStmtCtx(sc *stmtctx.StatementContext, stmt *ast.UFIDelateStmt, vars *variable.StochastikVars) {
	sc.InUFIDelateStmt = true
	sc.DupKeyAsWarning = stmt.IgnoreErr
	sc.BadNullAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
	sc.TruncateAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
	sc.DividedByZeroAsWarning = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr
	sc.AllowInvalidDate = vars.ALLEGROSQLMode.HasAllowInvalidDatesMode()
	sc.IgnoreZeroInDate = !vars.StrictALLEGROSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
	sc.Priority = stmt.Priority
}

// FillVirtualDeferredCausetValue will calculate the virtual defCausumn value by evaluating generated
// memex using rows from a chunk, and then fill this value into the chunk
func FillVirtualDeferredCausetValue(virtualRetTypes []*types.FieldType, virtualDeferredCausetIndex []int,
	schemaReplicant *memex.Schema, defCausumns []*perceptron.DeferredCausetInfo, sctx stochastikctx.Context, req *chunk.Chunk) error {
	virDefCauss := chunk.NewChunkWithCapacity(virtualRetTypes, req.Capacity())
	iter := chunk.NewIterator4Chunk(req)
	for i, idx := range virtualDeferredCausetIndex {
		for event := iter.Begin(); event != iter.End(); event = iter.Next() {
			causet, err := schemaReplicant.DeferredCausets[idx].EvalVirtualDeferredCauset(event)
			if err != nil {
				return err
			}
			// Because the memex might return different type from
			// the generated defCausumn, we should wrap a CAST on the result.
			castCauset, err := causet.CastValue(sctx, causet, defCausumns[idx], false, true)
			if err != nil {
				return err
			}
			virDefCauss.AppendCauset(i, &castCauset)
		}
		req.SetDefCaus(idx, virDefCauss.DeferredCauset(i))
	}
	return nil
}
