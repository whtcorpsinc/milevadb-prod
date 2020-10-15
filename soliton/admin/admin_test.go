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

package admin_test

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	. "github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	causetstore ekv.CausetStorage
	ctx   *mock.Context
}

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
	s.ctx.CausetStore = s.causetstore
}

func (s *testSuite) TearDownSuite(c *C) {
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
	testleak.AfterTest(c)()
}

func (s *testSuite) TestGetDBSInfo(c *C) {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	t := spacetime.NewMeta(txn)

	dbInfo2 := &perceptron.DBInfo{
		ID:    2,
		Name:  perceptron.NewCIStr("b"),
		State: perceptron.StateNone,
	}
	job := &perceptron.Job{
		SchemaID: dbInfo2.ID,
		Type:     perceptron.CausetActionCreateSchema,
		RowCount: 0,
	}
	job1 := &perceptron.Job{
		SchemaID: dbInfo2.ID,
		Type:     perceptron.CausetActionAddIndex,
		RowCount: 0,
	}
	err = t.EnQueueDBSJob(job)
	c.Assert(err, IsNil)
	info, err := GetDBSInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Jobs, HasLen, 1)
	c.Assert(info.Jobs[0], DeepEquals, job)
	c.Assert(info.ReorgHandle, Equals, nil)
	// Two jobs.
	t = spacetime.NewMeta(txn, spacetime.AddIndexJobListKey)
	err = t.EnQueueDBSJob(job1)
	c.Assert(err, IsNil)
	info, err = GetDBSInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(info.Jobs, HasLen, 2)
	c.Assert(info.Jobs[0], DeepEquals, job)
	c.Assert(info.Jobs[1], DeepEquals, job1)
	c.Assert(info.ReorgHandle, Equals, nil)
	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetDBSJobs(c *C) {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	t := spacetime.NewMeta(txn)
	cnt := 10
	jobs := make([]*perceptron.Job, cnt)
	var currJobs2 []*perceptron.Job
	for i := 0; i < cnt; i++ {
		jobs[i] = &perceptron.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     perceptron.CausetActionCreateBlock,
		}
		err = t.EnQueueDBSJob(jobs[i])
		c.Assert(err, IsNil)
		currJobs, err1 := GetDBSJobs(txn)
		c.Assert(err1, IsNil)
		c.Assert(currJobs, HasLen, i+1)
		currJobs2 = currJobs2[:0]
		err = IterAllDBSJobs(txn, func(jobs []*perceptron.Job) (b bool, e error) {
			for _, job := range jobs {
				if job.State == perceptron.JobStateNone {
					currJobs2 = append(currJobs2, job)
				} else {
					return true, nil
				}
			}
			return false, nil
		})
		c.Assert(err, IsNil)
		c.Assert(currJobs2, HasLen, i+1)
	}

	currJobs, err := GetDBSJobs(txn)
	c.Assert(err, IsNil)
	for i, job := range jobs {
		c.Assert(job.ID, Equals, currJobs[i].ID)
		c.Assert(job.SchemaID, Equals, int64(1))
		c.Assert(job.Type, Equals, perceptron.CausetActionCreateBlock)
	}
	c.Assert(currJobs, DeepEquals, currJobs2)

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func isJobsSorted(jobs []*perceptron.Job) bool {
	if len(jobs) <= 1 {
		return true
	}
	for i := 1; i < len(jobs); i++ {
		if jobs[i].ID <= jobs[i-1].ID {
			return false
		}
	}
	return true
}

func enQueueDBSJobs(c *C, t *spacetime.Meta, jobType perceptron.CausetActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &perceptron.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := t.EnQueueDBSJob(job)
		c.Assert(err, IsNil)
	}
}

func (s *testSuite) TestGetDBSJobsIsSort(c *C) {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)

	// insert 5 drop causet jobs to DefaultJobListKey queue
	t := spacetime.NewMeta(txn)
	enQueueDBSJobs(c, t, perceptron.CausetActionDropBlock, 10, 15)

	// insert 5 create causet jobs to DefaultJobListKey queue
	enQueueDBSJobs(c, t, perceptron.CausetActionCreateBlock, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	t = spacetime.NewMeta(txn, spacetime.AddIndexJobListKey)
	enQueueDBSJobs(c, t, perceptron.CausetActionAddIndex, 5, 10)

	currJobs, err := GetDBSJobs(txn)
	c.Assert(err, IsNil)
	c.Assert(currJobs, HasLen, 15)

	isSort := isJobsSorted(currJobs)
	c.Assert(isSort, Equals, true)

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestCancelJobs(c *C) {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	t := spacetime.NewMeta(txn)
	cnt := 10
	ids := make([]int64, cnt)
	for i := 0; i < cnt; i++ {
		job := &perceptron.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     perceptron.CausetActionCreateBlock,
		}
		if i == 0 {
			job.State = perceptron.JobStateDone
		}
		if i == 1 {
			job.State = perceptron.JobStateCancelled
		}
		ids[i] = int64(i)
		err = t.EnQueueDBSJob(job)
		c.Assert(err, IsNil)
	}

	errs, err := CancelJobs(txn, ids)
	c.Assert(err, IsNil)
	for i, err := range errs {
		if i == 0 {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
	}

	errs, err = CancelJobs(txn, []int64{})
	c.Assert(err, IsNil)
	c.Assert(errs, IsNil)
	errs, err = CancelJobs(txn, []int64{-1})
	c.Assert(err, IsNil)
	c.Assert(errs[0], NotNil)
	c.Assert(errs[0].Error(), Matches, "*DBS Job:-1 not found")

	// test cancel finish job.
	job := &perceptron.Job{
		ID:       100,
		SchemaID: 1,
		Type:     perceptron.CausetActionCreateBlock,
		State:    perceptron.JobStateDone,
	}
	err = t.EnQueueDBSJob(job)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{100})
	c.Assert(err, IsNil)
	c.Assert(errs[0], NotNil)
	c.Assert(errs[0].Error(), Matches, "*This job:100 is finished, so can't be cancelled")

	// test can't cancelable job.
	job.Type = perceptron.CausetActionDropIndex
	job.SchemaState = perceptron.StateWriteOnly
	job.State = perceptron.JobStateRunning
	job.ID = 101
	err = t.EnQueueDBSJob(job)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{101})
	c.Assert(err, IsNil)
	c.Assert(errs[0], NotNil)
	c.Assert(errs[0].Error(), Matches, "*This job:101 is almost finished, can't be cancelled now")

	// When both types of jobs exist in the DBS queue,
	// we first cancel the job with a larger ID.
	job = &perceptron.Job{
		ID:       1000,
		SchemaID: 1,
		BlockID:  2,
		Type:     perceptron.CausetActionAddIndex,
	}
	job1 := &perceptron.Job{
		ID:       1001,
		SchemaID: 1,
		BlockID:  2,
		Type:     perceptron.CausetActionAddDeferredCauset,
	}
	job2 := &perceptron.Job{
		ID:       1002,
		SchemaID: 1,
		BlockID:  2,
		Type:     perceptron.CausetActionAddIndex,
	}
	job3 := &perceptron.Job{
		ID:       1003,
		SchemaID: 1,
		BlockID:  2,
		Type:     perceptron.CausetActionRepairBlock,
	}
	err = t.EnQueueDBSJob(job, spacetime.AddIndexJobListKey)
	c.Assert(err, IsNil)
	err = t.EnQueueDBSJob(job1)
	c.Assert(err, IsNil)
	err = t.EnQueueDBSJob(job2, spacetime.AddIndexJobListKey)
	c.Assert(err, IsNil)
	err = t.EnQueueDBSJob(job3)
	c.Assert(err, IsNil)
	errs, err = CancelJobs(txn, []int64{job1.ID, job.ID, job2.ID, job3.ID})
	c.Assert(err, IsNil)
	for _, err := range errs {
		c.Assert(err, IsNil)
	}

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestGetHistoryDBSJobs(c *C) {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	t := spacetime.NewMeta(txn)
	cnt := 11
	jobs := make([]*perceptron.Job, cnt)
	for i := 0; i < cnt; i++ {
		jobs[i] = &perceptron.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     perceptron.CausetActionCreateBlock,
		}
		err = t.AddHistoryDBSJob(jobs[i], true)
		c.Assert(err, IsNil)
		historyJobs, err1 := GetHistoryDBSJobs(txn, DefNumHistoryJobs)
		c.Assert(err1, IsNil)
		if i+1 > MaxHistoryJobs {
			c.Assert(historyJobs, HasLen, MaxHistoryJobs)
		} else {
			c.Assert(historyJobs, HasLen, i+1)
		}
	}

	delta := cnt - MaxHistoryJobs
	historyJobs, err := GetHistoryDBSJobs(txn, DefNumHistoryJobs)
	c.Assert(err, IsNil)
	c.Assert(historyJobs, HasLen, MaxHistoryJobs)
	l := len(historyJobs) - 1
	for i, job := range historyJobs {
		c.Assert(job.ID, Equals, jobs[delta+l-i].ID)
		c.Assert(job.SchemaID, Equals, int64(1))
		c.Assert(job.Type, Equals, perceptron.CausetActionCreateBlock)
	}

	var historyJobs2 []*perceptron.Job
	err = IterHistoryDBSJobs(txn, func(jobs []*perceptron.Job) (b bool, e error) {
		for _, job := range jobs {
			historyJobs2 = append(historyJobs2, job)
			if len(historyJobs2) == DefNumHistoryJobs {
				return true, nil
			}
		}
		return false, nil
	})
	c.Assert(err, IsNil)
	c.Assert(historyJobs2, DeepEquals, historyJobs)

	err = txn.Rollback()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestIsJobRollbackable(c *C) {
	cases := []struct {
		tp     perceptron.CausetActionType
		state  perceptron.SchemaState
		result bool
	}{
		{perceptron.CausetActionDropIndex, perceptron.StateNone, true},
		{perceptron.CausetActionDropIndex, perceptron.StateDeleteOnly, false},
		{perceptron.CausetActionDropSchema, perceptron.StateDeleteOnly, false},
		{perceptron.CausetActionDropDeferredCauset, perceptron.StateDeleteOnly, false},
		{perceptron.CausetActionDropDeferredCausets, perceptron.StateDeleteOnly, false},
	}
	job := &perceptron.Job{}
	for _, ca := range cases {
		job.Type = ca.tp
		job.SchemaState = ca.state
		re := IsJobRollbackable(job)
		c.Assert(re == ca.result, IsTrue)
	}
}

func (s *testSuite) TestError(c *C) {
	kvErrs := []*terror.Error{
		ErrDataInConsistent,
		ErrDBSJobNotFound,
		ErrCancelFinishedDBSJob,
		ErrCannotCancelDBSJob,
	}
	for _, err := range kvErrs {
		code := terror.ToALLEGROSQLError(err).Code
		c.Assert(code != allegrosql.ErrUnknown && code == uint16(err.Code()), IsTrue, Commentf("err: %v", err))
	}
}
