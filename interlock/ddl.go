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
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/gcutil"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"go.uber.org/zap"
)

// DBSInterDirc represents a DBS interlock.
// It grabs a DBS instance from Petri, calling the DBS methods to do the work.
type DBSInterDirc struct {
	baseInterlockingDirectorate

	stmt ast.StmtNode
	is   schemareplicant.SchemaReplicant
	done bool
}

// toErr converts the error to the ErrSchemaReplicantChanged when the schemaReplicant is outdated.
func (e *DBSInterDirc) toErr(err error) error {
	// The err may be cause by schemaReplicant changed, here we distinguish the ErrSchemaReplicantChanged error from other errors.
	dom := petri.GetPetri(e.ctx)
	checker := petri.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil)
	txn, err1 := e.ctx.Txn(true)
	if err1 != nil {
		logutil.BgLogger().Error("active txn failed", zap.Error(err))
		return err1
	}
	_, schemaInfoErr := checker.Check(txn.StartTS())
	if schemaInfoErr != nil {
		return errors.Trace(schemaInfoErr)
	}
	return err
}

// Next implements the InterlockingDirectorate Next interface.
func (e *DBSInterDirc) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	// For each DBS, we should commit the previous transaction and create a new transaction.
	if err = e.ctx.NewTxn(ctx); err != nil {
		return err
	}
	defer func() { e.ctx.GetStochastikVars().StmtCtx.IsDBSJobInQueue = false }()

	switch x := e.stmt.(type) {
	case *ast.AlterDatabaseStmt:
		err = e.executeAlterDatabase(x)
	case *ast.AlterBlockStmt:
		err = e.executeAlterBlock(x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.CreateBlockStmt:
		err = e.executeCreateBlock(x)
	case *ast.CreateViewStmt:
		err = e.executeCreateView(x)
	case *ast.DropIndexStmt:
		err = e.executeDropIndex(x)
	case *ast.DroFIDelatabaseStmt:
		err = e.executeDroFIDelatabase(x)
	case *ast.DropBlockStmt:
		if x.IsView {
			err = e.executeDropView(x)
		} else {
			err = e.executeDropBlock(x)
		}
	case *ast.RecoverBlockStmt:
		err = e.executeRecoverBlock(x)
	case *ast.FlashBackBlockStmt:
		err = e.executeFlashbackBlock(x)
	case *ast.RenameBlockStmt:
		err = e.executeRenameBlock(x)
	case *ast.TruncateBlockStmt:
		err = e.executeTruncateBlock(x)
	case *ast.LockBlocksStmt:
		err = e.executeLockBlocks(x)
	case *ast.UnlockBlocksStmt:
		err = e.executeUnlockBlocks(x)
	case *ast.CleanupBlockLockStmt:
		err = e.executeCleanupBlockLock(x)
	case *ast.RepairBlockStmt:
		err = e.executeRepairBlock(x)
	case *ast.CreateSequenceStmt:
		err = e.executeCreateSequence(x)
	case *ast.DropSequenceStmt:
		err = e.executeDropSequence(x)

	}
	if err != nil {
		// If the tenant return ErrBlockNotExists error when running this DBS, it may be caused by schemaReplicant changed,
		// otherwise, ErrBlockNotExists can be returned before putting this DBS job to the job queue.
		if (e.ctx.GetStochastikVars().StmtCtx.IsDBSJobInQueue && schemareplicant.ErrBlockNotExists.Equal(err)) ||
			!e.ctx.GetStochastikVars().StmtCtx.IsDBSJobInQueue {
			return e.toErr(err)
		}
		return err

	}

	dom := petri.GetPetri(e.ctx)
	// UFIDelate SchemaReplicant in TxnCtx, so it will pass schemaReplicant check.
	is := dom.SchemaReplicant()
	txnCtx := e.ctx.GetStochastikVars().TxnCtx
	txnCtx.SchemaReplicant = is
	txnCtx.SchemaVersion = is.SchemaMetaVersion()
	// DBS will force commit old transaction, after DBS, in transaction status should be false.
	e.ctx.GetStochastikVars().SetStatusFlag(allegrosql.ServerStatusInTrans, false)
	return nil
}

func (e *DBSInterDirc) executeTruncateBlock(s *ast.TruncateBlockStmt) error {
	ident := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().TruncateBlock(e.ctx, ident)
	return err
}

func (e *DBSInterDirc) executeRenameBlock(s *ast.RenameBlockStmt) error {
	if len(s.BlockToBlocks) != 1 {
		// Now we only allow one schemaReplicant changing at the same time.
		return errors.Errorf("can't run multi schemaReplicant change")
	}
	oldIdent := ast.Ident{Schema: s.OldBlock.Schema, Name: s.OldBlock.Name}
	newIdent := ast.Ident{Schema: s.NewBlock.Schema, Name: s.NewBlock.Name}
	isAlterBlock := false
	err := petri.GetPetri(e.ctx).DBS().RenameBlock(e.ctx, oldIdent, newIdent, isAlterBlock)
	return err
}

func (e *DBSInterDirc) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	var opt *ast.CharsetOpt
	if len(s.Options) != 0 {
		opt = &ast.CharsetOpt{}
		for _, val := range s.Options {
			switch val.Tp {
			case ast.DatabaseOptionCharset:
				opt.Chs = val.Value
			case ast.DatabaseOptionDefCauslate:
				opt.DefCaus = val.Value
			}
		}
	}
	err := petri.GetPetri(e.ctx).DBS().CreateSchema(e.ctx, perceptron.NewCIStr(s.Name), opt)
	if err != nil {
		if schemareplicant.ErrDatabaseExists.Equal(err) && s.IfNotExists {
			err = nil
		}
	}
	return err
}

func (e *DBSInterDirc) executeAlterDatabase(s *ast.AlterDatabaseStmt) error {
	err := petri.GetPetri(e.ctx).DBS().AlterSchema(e.ctx, s)
	return err
}

func (e *DBSInterDirc) executeCreateBlock(s *ast.CreateBlockStmt) error {
	err := petri.GetPetri(e.ctx).DBS().CreateBlock(e.ctx, s)
	return err
}

func (e *DBSInterDirc) executeCreateView(s *ast.CreateViewStmt) error {
	err := petri.GetPetri(e.ctx).DBS().CreateView(e.ctx, s)
	return err
}

func (e *DBSInterDirc) executeCreateIndex(s *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().CreateIndex(e.ctx, ident, s.KeyType, perceptron.NewCIStr(s.IndexName),
		s.IndexPartSpecifications, s.IndexOption, s.IfNotExists)
	return err
}

func (e *DBSInterDirc) executeDroFIDelatabase(s *ast.DroFIDelatabaseStmt) error {
	dbName := perceptron.NewCIStr(s.Name)

	// Protect important system causet from been dropped by a mistake.
	// I can hardly find a case that a user really need to do this.
	if dbName.L == "allegrosql" {
		return errors.New("Drop 'allegrosql' database is forbidden")
	}

	err := petri.GetPetri(e.ctx).DBS().DropSchema(e.ctx, dbName)
	if schemareplicant.ErrDatabaseNotExists.Equal(err) {
		if s.IfExists {
			err = nil
		} else {
			err = schemareplicant.ErrDatabaseDropExists.GenWithStackByArgs(s.Name)
		}
	}
	stochastikVars := e.ctx.GetStochastikVars()
	if err == nil && strings.ToLower(stochastikVars.CurrentDB) == dbName.L {
		stochastikVars.CurrentDB = ""
		err = variable.SetStochastikSystemVar(stochastikVars, variable.CharsetDatabase, types.NewStringCauset(allegrosql.DefaultCharset))
		if err != nil {
			return err
		}
		err = variable.SetStochastikSystemVar(stochastikVars, variable.DefCauslationDatabase, types.NewStringCauset(allegrosql.DefaultDefCauslationName))
		if err != nil {
			return err
		}
	}
	return err
}

// If one drop those blocks by mistake, it's difficult to recover.
// In the worst case, the whole MilevaDB cluster fails to bootstrap, so we prevent user from dropping them.
var systemBlocks = map[string]struct{}{
	"milevadb":                 {},
	"gc_delete_range":      {},
	"gc_delete_range_done": {},
}

func isSystemBlock(schemaReplicant, causet string) bool {
	if schemaReplicant != "allegrosql" {
		return false
	}
	if _, ok := systemBlocks[causet]; ok {
		return true
	}
	return false
}

type objectType int

const (
	blockObject objectType = iota
	viewObject
	sequenceObject
)

func (e *DBSInterDirc) executeDropBlock(s *ast.DropBlockStmt) error {
	return e.dropBlockObject(s.Blocks, blockObject, s.IfExists)
}

func (e *DBSInterDirc) executeDropView(s *ast.DropBlockStmt) error {
	return e.dropBlockObject(s.Blocks, viewObject, s.IfExists)
}

func (e *DBSInterDirc) executeDropSequence(s *ast.DropSequenceStmt) error {
	return e.dropBlockObject(s.Sequences, sequenceObject, s.IfExists)
}

// dropBlockObject actually applies to `blockObject`, `viewObject` and `sequenceObject`.
func (e *DBSInterDirc) dropBlockObject(objects []*ast.BlockName, obt objectType, ifExists bool) error {
	var notExistBlocks []string
	for _, tn := range objects {
		fullti := ast.Ident{Schema: tn.Schema, Name: tn.Name}
		_, ok := e.is.SchemaByName(tn.Schema)
		if !ok {
			// TODO: we should return special error for causet not exist, checking "not exist" is not enough,
			// because some other errors may contain this error string too.
			notExistBlocks = append(notExistBlocks, fullti.String())
			continue
		}
		_, err := e.is.BlockByName(tn.Schema, tn.Name)
		if err != nil && schemareplicant.ErrBlockNotExists.Equal(err) {
			notExistBlocks = append(notExistBlocks, fullti.String())
			continue
		} else if err != nil {
			return err
		}

		// Protect important system causet from been dropped by a mistake.
		// I can hardly find a case that a user really need to do this.
		if isSystemBlock(tn.Schema.L, tn.Name.L) {
			return errors.Errorf("Drop milevadb system causet '%s.%s' is forbidden", tn.Schema.L, tn.Name.L)
		}

		if obt == blockObject && config.CheckBlockBeforeDrop {
			logutil.BgLogger().Warn("admin check causet before drop",
				zap.String("database", fullti.Schema.O),
				zap.String("causet", fullti.Name.O),
			)
			allegrosql := fmt.Sprintf("admin check causet `%s`.`%s`", fullti.Schema.O, fullti.Name.O)
			_, _, err = e.ctx.(sqlexec.RestrictedALLEGROSQLInterlockingDirectorate).InterDircRestrictedALLEGROSQL(allegrosql)
			if err != nil {
				return err
			}
		}
		switch obt {
		case blockObject:
			err = petri.GetPetri(e.ctx).DBS().DropBlock(e.ctx, fullti)
		case viewObject:
			err = petri.GetPetri(e.ctx).DBS().DropView(e.ctx, fullti)
		case sequenceObject:
			err = petri.GetPetri(e.ctx).DBS().DropSequence(e.ctx, fullti, ifExists)
		}
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err) {
			notExistBlocks = append(notExistBlocks, fullti.String())
		} else if err != nil {
			return err
		}
	}
	if len(notExistBlocks) > 0 && !ifExists {
		if obt == sequenceObject {
			return schemareplicant.ErrSequenceDropExists.GenWithStackByArgs(strings.Join(notExistBlocks, ","))
		}
		return schemareplicant.ErrBlockDropExists.GenWithStackByArgs(strings.Join(notExistBlocks, ","))
	}
	// We need add warning when use if exists.
	if len(notExistBlocks) > 0 && ifExists {
		for _, causet := range notExistBlocks {
			if obt == sequenceObject {
				e.ctx.GetStochastikVars().StmtCtx.AppendNote(schemareplicant.ErrSequenceDropExists.GenWithStackByArgs(causet))
			} else {
				e.ctx.GetStochastikVars().StmtCtx.AppendNote(schemareplicant.ErrBlockDropExists.GenWithStackByArgs(causet))
			}
		}
	}
	return nil
}

func (e *DBSInterDirc) executeDropIndex(s *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().DropIndex(e.ctx, ti, perceptron.NewCIStr(s.IndexName), s.IfExists)
	if (schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err)) && s.IfExists {
		err = nil
	}
	return err
}

func (e *DBSInterDirc) executeAlterBlock(s *ast.AlterBlockStmt) error {
	ti := ast.Ident{Schema: s.Block.Schema, Name: s.Block.Name}
	err := petri.GetPetri(e.ctx).DBS().AlterBlock(e.ctx, ti, s.Specs)
	return err
}

// executeRecoverBlock represents a recover causet interlock.
// It is built from "recover causet" memex,
// is used to recover the causet that deleted by mistake.
func (e *DBSInterDirc) executeRecoverBlock(s *ast.RecoverBlockStmt) error {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	t := spacetime.NewMeta(txn)
	dom := petri.GetPetri(e.ctx)
	var job *perceptron.Job
	var tblInfo *perceptron.BlockInfo
	if s.JobID != 0 {
		job, tblInfo, err = e.getRecoverBlockByJobID(s, t, dom)
	} else {
		job, tblInfo, err = e.getRecoverBlockByBlockName(s.Block)
	}
	if err != nil {
		return err
	}
	// Check the causet ID was not exists.
	tbl, ok := dom.SchemaReplicant().BlockByID(tblInfo.ID)
	if ok {
		return schemareplicant.ErrBlockExists.GenWithStack("Block '%-.192s' already been recover to '%-.192s', can't be recover repeatedly", s.Block.Name.O, tbl.Meta().Name.O)
	}

	autoIncID, autoRandID, err := e.getBlockAutoIDsFromSnapshot(job)
	if err != nil {
		return err
	}

	recoverInfo := &dbs.RecoverInfo{
		SchemaID:      job.SchemaID,
		BlockInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		CurAutoIncID:  autoIncID,
		CurAutoRandID: autoRandID,
	}
	// Call DBS RecoverBlock.
	err = petri.GetPetri(e.ctx).DBS().RecoverBlock(e.ctx, recoverInfo)
	return err
}

func (e *DBSInterDirc) getBlockAutoIDsFromSnapshot(job *perceptron.Job) (autoIncID, autoRandID int64, err error) {
	// Get causet original autoIDs before causet drop.
	dom := petri.GetPetri(e.ctx)
	m, err := dom.GetSnapshotMeta(job.StartTS)
	if err != nil {
		return 0, 0, err
	}
	autoIncID, err = m.GetAutoBlockID(job.SchemaID, job.BlockID)
	if err != nil {
		return 0, 0, errors.Errorf("recover block_id: %d, get original autoIncID from snapshot spacetime err: %s", job.BlockID, err.Error())
	}
	autoRandID, err = m.GetAutoRandomID(job.SchemaID, job.BlockID)
	if err != nil {
		return 0, 0, errors.Errorf("recover block_id: %d, get original autoRandID from snapshot spacetime err: %s", job.BlockID, err.Error())
	}
	return autoIncID, autoRandID, nil
}

func (e *DBSInterDirc) getRecoverBlockByJobID(s *ast.RecoverBlockStmt, t *spacetime.Meta, dom *petri.Petri) (*perceptron.Job, *perceptron.BlockInfo, error) {
	job, err := t.GetHistoryDBSJob(s.JobID)
	if err != nil {
		return nil, nil, err
	}
	if job == nil {
		return nil, nil, admin.ErrDBSJobNotFound.GenWithStackByArgs(s.JobID)
	}
	if job.Type != perceptron.CausetActionDropBlock && job.Type != perceptron.CausetActionTruncateBlock {
		return nil, nil, errors.Errorf("Job %v type is %v, not dropped/truncated causet", job.ID, job.Type)
	}

	// Check GC safe point for getting snapshot schemaReplicant.
	err = gcutil.ValidateSnapshot(e.ctx, job.StartTS)
	if err != nil {
		return nil, nil, err
	}

	// Get the snapshot schemaReplicant before drop causet.
	snapInfo, err := dom.GetSnapshotSchemaReplicant(job.StartTS)
	if err != nil {
		return nil, nil, err
	}
	// Get causet spacetime from snapshot schemaReplicant.
	causet, ok := snapInfo.BlockByID(job.BlockID)
	if !ok {
		return nil, nil, schemareplicant.ErrBlockNotExists.GenWithStackByArgs(
			fmt.Sprintf("(Schema ID %d)", job.SchemaID),
			fmt.Sprintf("(Block ID %d)", job.BlockID),
		)
	}
	return job, causet.Meta(), nil
}

// GetDropOrTruncateBlockInfoFromJobs gets the dropped/truncated causet information from DBS jobs,
// it will use the `start_ts` of DBS job as snapshot to get the dropped/truncated causet information.
func GetDropOrTruncateBlockInfoFromJobs(jobs []*perceptron.Job, gcSafePoint uint64, dom *petri.Petri, fn func(*perceptron.Job, *perceptron.BlockInfo) (bool, error)) (bool, error) {
	for _, job := range jobs {
		// Check GC safe point for getting snapshot schemaReplicant.
		err := gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return false, err
		}
		if job.Type != perceptron.CausetActionDropBlock && job.Type != perceptron.CausetActionTruncateBlock {
			continue
		}

		snapMeta, err := dom.GetSnapshotMeta(job.StartTS)
		if err != nil {
			return false, err
		}
		tbl, err := snapMeta.GetBlock(job.SchemaID, job.BlockID)
		if err != nil {
			if spacetime.ErrDBNotExists.Equal(err) {
				// The dropped/truncated DBS maybe execute failed that caused by the parallel DBS execution,
				// then can't find the causet from the snapshot info-schemaReplicant. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropBlock.
				continue
			}
			return false, err
		}
		if tbl == nil {
			// The dropped/truncated DBS maybe execute failed that caused by the parallel DBS execution,
			// then can't find the causet from the snapshot info-schemaReplicant. Should just ignore error here,
			// see more in TestParallelDropSchemaAndDropBlock.
			continue
		}
		finish, err := fn(job, tbl)
		if err != nil || finish {
			return finish, err
		}
	}
	return false, nil
}

func (e *DBSInterDirc) getRecoverBlockByBlockName(blockName *ast.BlockName) (*perceptron.Job, *perceptron.BlockInfo, error) {
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return nil, nil, err
	}
	schemaName := blockName.Schema.L
	if schemaName == "" {
		schemaName = strings.ToLower(e.ctx.GetStochastikVars().CurrentDB)
	}
	if schemaName == "" {
		return nil, nil, errors.Trace(core.ErrNoDB)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(e.ctx)
	if err != nil {
		return nil, nil, err
	}
	var jobInfo *perceptron.Job
	var blockInfo *perceptron.BlockInfo
	dom := petri.GetPetri(e.ctx)
	handleJobAndBlockInfo := func(job *perceptron.Job, tblInfo *perceptron.BlockInfo) (bool, error) {
		if tblInfo.Name.L != blockName.Name.L {
			return false, nil
		}
		schemaReplicant, ok := dom.SchemaReplicant().SchemaByID(job.SchemaID)
		if !ok {
			return false, nil
		}
		if schemaReplicant.Name.L == schemaName {
			blockInfo = tblInfo
			jobInfo = job
			return true, nil
		}
		return false, nil
	}
	fn := func(jobs []*perceptron.Job) (bool, error) {
		return GetDropOrTruncateBlockInfoFromJobs(jobs, gcSafePoint, dom, handleJobAndBlockInfo)
	}
	err = admin.IterHistoryDBSJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			return nil, nil, errors.Errorf("Can't find dropped/truncated causet '%s' in GC safe point %s", blockName.Name.O, perceptron.TSConvert2Time(gcSafePoint).String())
		}
		return nil, nil, err
	}
	if blockInfo == nil || jobInfo == nil {
		return nil, nil, errors.Errorf("Can't find dropped/truncated causet: %v in DBS history jobs", blockName.Name)
	}
	return jobInfo, blockInfo, nil
}

func (e *DBSInterDirc) executeFlashbackBlock(s *ast.FlashBackBlockStmt) error {
	job, tblInfo, err := e.getRecoverBlockByBlockName(s.Block)
	if err != nil {
		return err
	}
	if len(s.NewName) != 0 {
		tblInfo.Name = perceptron.NewCIStr(s.NewName)
	}
	// Check the causet ID was not exists.
	is := petri.GetPetri(e.ctx).SchemaReplicant()
	tbl, ok := is.BlockByID(tblInfo.ID)
	if ok {
		return schemareplicant.ErrBlockExists.GenWithStack("Block '%-.192s' already been flashback to '%-.192s', can't be flashback repeatedly", s.Block.Name.O, tbl.Meta().Name.O)
	}

	autoIncID, autoRandID, err := e.getBlockAutoIDsFromSnapshot(job)
	if err != nil {
		return err
	}
	recoverInfo := &dbs.RecoverInfo{
		SchemaID:      job.SchemaID,
		BlockInfo:     tblInfo,
		DropJobID:     job.ID,
		SnapshotTS:    job.StartTS,
		CurAutoIncID:  autoIncID,
		CurAutoRandID: autoRandID,
	}
	// Call DBS RecoverBlock.
	err = petri.GetPetri(e.ctx).DBS().RecoverBlock(e.ctx, recoverInfo)
	return err
}

func (e *DBSInterDirc) executeLockBlocks(s *ast.LockBlocksStmt) error {
	if !config.BlockLockEnabled() {
		return nil
	}
	return petri.GetPetri(e.ctx).DBS().LockBlocks(e.ctx, s)
}

func (e *DBSInterDirc) executeUnlockBlocks(s *ast.UnlockBlocksStmt) error {
	if !config.BlockLockEnabled() {
		return nil
	}
	lockedBlocks := e.ctx.GetAllBlockLocks()
	return petri.GetPetri(e.ctx).DBS().UnlockBlocks(e.ctx, lockedBlocks)
}

func (e *DBSInterDirc) executeCleanupBlockLock(s *ast.CleanupBlockLockStmt) error {
	return petri.GetPetri(e.ctx).DBS().CleanupBlockLock(e.ctx, s.Blocks)
}

func (e *DBSInterDirc) executeRepairBlock(s *ast.RepairBlockStmt) error {
	return petri.GetPetri(e.ctx).DBS().RepairBlock(e.ctx, s.Block, s.CreateStmt)
}

func (e *DBSInterDirc) executeCreateSequence(s *ast.CreateSequenceStmt) error {
	return petri.GetPetri(e.ctx).DBS().CreateSequence(e.ctx, s)
}
