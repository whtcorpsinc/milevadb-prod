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

package chunk

import (
	"errors"
	"time"

	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
)

var _ = check.Suite(&rowContainerTestSuite{})
var _ = check.SerialSuites(&rowContainerTestSerialSuite{})

type rowContainerTestSuite struct{}
type rowContainerTestSerialSuite struct{}

func (r *rowContainerTestSuite) TestNewRowContainer(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	rc := NewRowContainer(fields, 1024)
	c.Assert(rc, check.NotNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
}

func (r *rowContainerTestSuite) TestSel(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	sz := 4
	rc := NewRowContainer(fields, sz)
	c.Assert(rc, check.NotNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
	n := 64
	chk := NewChunkWithCapacity(fields, sz)
	numRows := 0
	for i := 0; i < n-sz; i++ {
		chk.AppendInt64(0, int64(i))
		if chk.NumRows() == sz {
			chk.SetSel([]int{0, 2})
			numRows += 2
			err := rc.Add(chk)
			c.Assert(err, check.IsNil)
			chk = NewChunkWithCapacity(fields, sz)
		}
	}
	c.Assert(rc.NumChunks(), check.Equals, numRows/2)
	c.Assert(rc.NumRow(), check.Equals, numRows)
	for i := n - sz; i < n; i++ {
		chk.AppendInt64(0, int64(i))
	}
	chk.SetSel([]int{0, 1, 2})

	checkByIter := func(it Iterator) {
		i := 0
		for event := it.Begin(); event != it.End(); event = it.Next() {
			c.Assert(event.GetInt64(0), check.Equals, int64(i))
			if i < n-sz {
				i += 2
			} else {
				i++
			}
		}
		c.Assert(i, check.Equals, n-1)
	}
	checkByIter(NewMultiIterator(NewIterator4RowContainer(rc), NewIterator4Chunk(chk)))
	rc.SpillToDisk()
	err := rc.m.spillError
	c.Assert(err, check.IsNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, true)
	checkByIter(NewMultiIterator(NewIterator4RowContainer(rc), NewIterator4Chunk(chk)))
	err = rc.Close()
	c.Assert(err, check.IsNil)
	c.Assert(rc.memTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(rc.memTracker.MaxConsumed(), check.Greater, int64(0))
}

func (r *rowContainerTestSuite) TestSpillCausetAction(c *check.C) {
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + 1)
	tracker.FallbackOldAndSetNewCausetAction(rc.CausetActionSpillForTest())
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	c.Assert(err, check.IsNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
	c.Assert(rc.GetMemTracker().BytesConsumed(), check.Equals, chk.MemoryUsage())
	// The following line is erroneous, since chk is already handled by rc, Add it again causes duplicated memory usage account.
	// It is only for test of spill, do not double-add a chunk elsewhere.
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	c.Assert(err, check.IsNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, true)
	err = rc.Reset()
	c.Assert(err, check.IsNil)
}

func (r *rowContainerTestSerialSuite) TestSpillCausetActionDeadLock(c *check.C) {
	// Maybe get deadlock if we use two RLock in one goroutine, for oom-action call stack.
	// Now the implement avoids the situation.
	// Goroutine 1: rc.Add() (RLock) -> list.Add() -> tracker.Consume() -> SpillDiskCausetAction -> rc.AlreadySpilledSafeForTest() (RLock)
	// Goroutine 2: ------------------> SpillDiskCausetAction -> new Goroutine to spill -> ------------------
	// new Goroutine created by 2: ---> rc.SpillToDisk (Lock)
	// In golang, RLock will be blocked after try to get Lock. So it will cause deadlock.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/soliton/chunk/testRowContainerDeadLock", "return(true)"), check.IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/soliton/chunk/testRowContainerDeadLock"), check.IsNil)
	}()
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(1)
	ac := rc.CausetActionSpillForTest()
	tracker.FallbackOldAndSetNewCausetAction(ac)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
	go func() {
		time.Sleep(200 * time.Millisecond)
		ac.CausetAction(tracker)
	}()
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	rc.actionSpill.WaitForTest()
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, true)
}

func (r *rowContainerTestSuite) TestNewSortedRowContainer(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	rc := NewSortedRowContainer(fields, 1024, nil, nil, nil)
	c.Assert(rc, check.NotNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
}

func (r *rowContainerTestSuite) TestSortedRowContainerSortSpillCausetAction(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	byItemsDesc := []bool{false}
	keyDeferredCausets := []int{0}
	keyCmpFuncs := []CompareFunc{cmpInt64}
	sz := 20
	rc := NewSortedRowContainer(fields, sz, byItemsDesc, keyDeferredCausets, keyCmpFuncs)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + 1)
	tracker.FallbackOldAndSetNewCausetAction(rc.CausetActionSpillForTest())
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	c.Assert(err, check.IsNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
	c.Assert(rc.GetMemTracker().BytesConsumed(), check.Equals, chk.MemoryUsage())
	// The following line is erroneous, since chk is already handled by rc, Add it again causes duplicated memory usage account.
	// It is only for test of spill, do not double-add a chunk elsewhere.
	err = rc.Add(chk)
	rc.actionSpill.WaitForTest()
	c.Assert(err, check.IsNil)
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, true)
	// The result has been sorted.
	for i := 0; i < sz*2; i++ {
		event, err := rc.GetSortedRow(i)
		if err != nil {
			c.Fatal(err)
		}
		c.Assert(event.GetInt64(0), check.Equals, int64(i/2))
	}
	// Can't insert records again.
	err = rc.Add(chk)
	c.Assert(err, check.NotNil)
	c.Assert(errors.Is(err, ErrCannotAddBecauseSorted), check.IsTrue)
	err = rc.Reset()
	c.Assert(err, check.IsNil)
}

func (r *rowContainerTestSerialSuite) TestCausetActionBlocked(c *check.C) {
	sz := 4
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	// Case 1, test Broadcast in CausetAction.
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(1450)
	ac := rc.CausetActionSpill()
	tracker.FallbackOldAndSetNewCausetAction(ac)
	for i := 0; i < 10; i++ {
		err = rc.Add(chk)
		c.Assert(err, check.IsNil)
	}

	ac.cond.L.Lock()
	for ac.cond.status == notSpilled ||
		ac.cond.status == spilling {
		ac.cond.Wait()
	}
	ac.cond.L.Unlock()
	ac.cond.L.Lock()
	c.Assert(ac.cond.status, check.Equals, spilledYet)
	ac.cond.L.Unlock()
	c.Assert(tracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(tracker.MaxConsumed(), check.Greater, int64(0))
	c.Assert(rc.GetDiskTracker().BytesConsumed(), check.Greater, int64(0))

	// Case 2, test CausetAction will causet when spilling.
	rc = NewRowContainer(fields, sz)
	tracker = rc.GetMemTracker()
	ac = rc.CausetActionSpill()
	starttime := time.Now()
	ac.setStatus(spilling)
	go func() {
		time.Sleep(200 * time.Millisecond)
		ac.setStatus(spilledYet)
		ac.cond.Broadcast()
	}()
	ac.CausetAction(tracker)
	c.Assert(time.Since(starttime), check.GreaterEqual, 200*time.Millisecond)
}

func (r *rowContainerTestSuite) TestRowContainerResetAndCausetAction(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(allegrosql.TypeLonglong)}
	sz := 20
	rc := NewRowContainer(fields, sz)

	chk := NewChunkWithCapacity(fields, sz)
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
	}
	var tracker *memory.Tracker
	var err error
	tracker = rc.GetMemTracker()
	tracker.SetBytesLimit(chk.MemoryUsage() + 1)
	tracker.FallbackOldAndSetNewCausetAction(rc.CausetActionSpillForTest())
	c.Assert(rc.AlreadySpilledSafeForTest(), check.Equals, false)
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetDiskTracker().BytesConsumed(), check.Equals, int64(0))
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	rc.actionSpill.WaitForTest()
	c.Assert(rc.GetDiskTracker().BytesConsumed(), check.Greater, int64(0))
	// Reset and Spill again.
	err = rc.Reset()
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetDiskTracker().BytesConsumed(), check.Equals, int64(0))
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetDiskTracker().BytesConsumed(), check.Equals, int64(0))
	err = rc.Add(chk)
	c.Assert(err, check.IsNil)
	rc.actionSpill.WaitForTest()
	c.Assert(rc.GetDiskTracker().BytesConsumed(), check.Greater, int64(0))
}
