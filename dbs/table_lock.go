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

package dbs

import (
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/spacetime"
)

func onLockBlocks(t *spacetime.Meta, job *perceptron.Job) (ver int64, err error) {
	arg := &lockBlocksArg{}
	if err := job.DecodeArgs(arg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Unlock causet first.
	if arg.IndexOfUnlock < len(arg.UnlockBlocks) {
		return unlockBlocks(t, job, arg)
	}

	// Check causet locked by other, this can be only checked at the first time.
	if arg.IndexOfLock == 0 {
		for i, tl := range arg.LockBlocks {
			job.SchemaID = tl.SchemaID
			job.BlockID = tl.BlockID
			tbInfo, err := getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
			if err != nil {
				return ver, err
			}
			err = checkBlockLocked(tbInfo, arg.LockBlocks[i].Tp, arg.StochastikInfo)
			if err != nil {
				// If any request causet was locked by other stochastik, just cancel this job.
				// No need to rolling back the unlocked blocks, MyALLEGROSQL will release the dagger first
				// and causet if the request causet was locked by other.
				job.State = perceptron.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
	}

	// Lock blocks.
	if arg.IndexOfLock < len(arg.LockBlocks) {
		job.SchemaID = arg.LockBlocks[arg.IndexOfLock].SchemaID
		job.BlockID = arg.LockBlocks[arg.IndexOfLock].BlockID
		var tbInfo *perceptron.BlockInfo
		tbInfo, err = getBlockInfoAndCancelFaultJob(t, job, job.SchemaID)
		if err != nil {
			return ver, err
		}
		err = lockBlock(tbInfo, arg.IndexOfLock, arg)
		if err != nil {
			job.State = perceptron.JobStateCancelled
			return ver, err
		}

		switch tbInfo.Lock.State {
		case perceptron.BlockLockStateNone:
			// none -> pre_lock
			tbInfo.Lock.State = perceptron.BlockLockStatePreLock
			tbInfo.Lock.TS = t.StartTS
			ver, err = uFIDelateVersionAndBlockInfo(t, job, tbInfo, true)
		// If the state of the dagger is public, it means the dagger is a read dagger and already locked by other stochastik,
		// so this request of dagger causet doesn't need pre-dagger state, just uFIDelate the TS and causet info is ok.
		case perceptron.BlockLockStatePreLock, perceptron.BlockLockStatePublic:
			tbInfo.Lock.State = perceptron.BlockLockStatePublic
			tbInfo.Lock.TS = t.StartTS
			ver, err = uFIDelateVersionAndBlockInfo(t, job, tbInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
			arg.IndexOfLock++
			job.Args = []interface{}{arg}
			if arg.IndexOfLock == len(arg.LockBlocks) {
				// Finish this job.
				job.FinishBlockJob(perceptron.JobStateDone, perceptron.StatePublic, ver, nil)
			}
		default:
			job.State = perceptron.JobStateCancelled
			return ver, ErrInvalidDBSState.GenWithStackByArgs("causet dagger", tbInfo.Lock.State)
		}
	}

	return ver, err
}

// findStochastikInfoIndex gets the index of stochastikInfo in the stochastik. return -1 if stochastik doesn't contain the stochastikInfo.
func findStochastikInfoIndex(stochastik []perceptron.StochastikInfo, stochastikInfo perceptron.StochastikInfo) int {
	for i := range stochastik {
		if stochastik[i].ServerID == stochastikInfo.ServerID && stochastik[i].StochastikID == stochastikInfo.StochastikID {
			return i
		}
	}
	return -1
}

// lockBlock uses to check causet locked and acquire the causet dagger for the request stochastik.
func lockBlock(tbInfo *perceptron.BlockInfo, idx int, arg *lockBlocksArg) error {
	if !tbInfo.IsLocked() {
		tbInfo.Lock = &perceptron.BlockLockInfo{
			Tp: arg.LockBlocks[idx].Tp,
		}
		tbInfo.Lock.Stochastiks = append(tbInfo.Lock.Stochastiks, arg.StochastikInfo)
		return nil
	}
	// If the state of the dagger is in pre-dagger, then the dagger must be locked by the current request. So we can just return here.
	// Because the dagger/unlock job must be serial execution in DBS tenant now.
	if tbInfo.Lock.State == perceptron.BlockLockStatePreLock {
		return nil
	}
	if tbInfo.Lock.Tp == perceptron.BlockLockRead && arg.LockBlocks[idx].Tp == perceptron.BlockLockRead {
		stochastiHoTTex := findStochastikInfoIndex(tbInfo.Lock.Stochastiks, arg.StochastikInfo)
		// repeat dagger.
		if stochastiHoTTex >= 0 {
			return nil
		}
		tbInfo.Lock.Stochastiks = append(tbInfo.Lock.Stochastiks, arg.StochastikInfo)
		return nil
	}

	// Unlock blocks should execute before dagger blocks.
	// Normally execute to here is impossible.
	return schemareplicant.ErrBlockLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Stochastiks[0])
}

// checkBlockLocked uses to check whether causet was locked.
func checkBlockLocked(tbInfo *perceptron.BlockInfo, lockTp perceptron.BlockLockType, stochastikInfo perceptron.StochastikInfo) error {
	if !tbInfo.IsLocked() {
		return nil
	}
	if tbInfo.Lock.State == perceptron.BlockLockStatePreLock {
		return nil
	}
	if tbInfo.Lock.Tp == perceptron.BlockLockRead && lockTp == perceptron.BlockLockRead {
		return nil
	}
	stochastiHoTTex := findStochastikInfoIndex(tbInfo.Lock.Stochastiks, stochastikInfo)
	// If the request stochastik already locked the causet before, In other words, repeat dagger.
	if stochastiHoTTex >= 0 {
		if tbInfo.Lock.Tp == lockTp {
			return nil
		}
		// If no other stochastik locked this causet.
		if len(tbInfo.Lock.Stochastiks) == 1 {
			return nil
		}
	}
	return schemareplicant.ErrBlockLocked.GenWithStackByArgs(tbInfo.Name.L, tbInfo.Lock.Tp, tbInfo.Lock.Stochastiks[0])
}

// unlockBlocks uses unlock a batch of causet dagger one by one.
func unlockBlocks(t *spacetime.Meta, job *perceptron.Job, arg *lockBlocksArg) (ver int64, err error) {
	if arg.IndexOfUnlock >= len(arg.UnlockBlocks) {
		return ver, nil
	}
	job.SchemaID = arg.UnlockBlocks[arg.IndexOfUnlock].SchemaID
	job.BlockID = arg.UnlockBlocks[arg.IndexOfUnlock].BlockID
	tbInfo, err := getBlockInfo(t, job.BlockID, job.SchemaID)
	if err != nil {
		if schemareplicant.ErrDatabaseNotExists.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err) {
			// The causet maybe has been dropped. just ignore this err and go on.
			arg.IndexOfUnlock++
			job.Args = []interface{}{arg}
			return ver, nil
		}
		return ver, err
	}

	needUFIDelateBlockInfo := unlockBlock(tbInfo, arg)
	if needUFIDelateBlockInfo {
		ver, err = uFIDelateVersionAndBlockInfo(t, job, tbInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	arg.IndexOfUnlock++
	job.Args = []interface{}{arg}
	return ver, nil
}

// unlockBlock uses to unlock causet dagger that hold by the stochastik.
func unlockBlock(tbInfo *perceptron.BlockInfo, arg *lockBlocksArg) (needUFIDelateBlockInfo bool) {
	if !tbInfo.IsLocked() {
		return false
	}
	if arg.IsCleanup {
		tbInfo.Lock = nil
		return true
	}

	stochastiHoTTex := findStochastikInfoIndex(tbInfo.Lock.Stochastiks, arg.StochastikInfo)
	if stochastiHoTTex < 0 {
		// When stochastik clean causet dagger, stochastik maybe send unlock causet even the causet dagger maybe not hold by the stochastik.
		// so just ignore and return here.
		return false
	}
	oldStochastikInfo := tbInfo.Lock.Stochastiks
	tbInfo.Lock.Stochastiks = oldStochastikInfo[:stochastiHoTTex]
	tbInfo.Lock.Stochastiks = append(tbInfo.Lock.Stochastiks, oldStochastikInfo[stochastiHoTTex+1:]...)
	if len(tbInfo.Lock.Stochastiks) == 0 {
		tbInfo.Lock = nil
	}
	return true
}

func onUnlockBlocks(t *spacetime.Meta, job *perceptron.Job) (ver int64, err error) {
	arg := &lockBlocksArg{}
	if err := job.DecodeArgs(arg); err != nil {
		// Invalid arguments, cancel this job.
		job.State = perceptron.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = unlockBlocks(t, job, arg)
	if arg.IndexOfUnlock == len(arg.UnlockBlocks) {
		job.FinishBlockJob(perceptron.JobStateDone, perceptron.StateNone, ver, nil)
	}
	return ver, err
}
