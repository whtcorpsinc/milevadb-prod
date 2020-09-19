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

package stochastik_test

import (
	"context"
	"sync/atomic"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testStochastikSerialSuite) TestFailStatementCommitInRetry(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("create block t (id int)")

	tk.MustExec("begin")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2),(3),(4),(5)")
	tk.MustExec("insert into t values (6)")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/stochastik/mockCommitError8942", `return(true)`), IsNil)
	_, err := tk.Exec("commit")
	c.Assert(err, NotNil)
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/stochastik/mockCommitError8942"), IsNil)

	tk.MustExec("insert into t values (6)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("6"))
}

func (s *testStochastikSerialSuite) TestGetTSFailDirtyState(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("create block t (id int)")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/stochastik/mockGetTSFail", "return"), IsNil)
	ctx := failpoint.WithHook(context.Background(), func(ctx context.Context, fpname string) bool {
		return fpname == "github.com/whtcorpsinc/milevadb/stochastik/mockGetTSFail"
	})
	_, err := tk.Se.Execute(ctx, "select * from t")
	c.Assert(err, NotNil)

	// Fix a bug that active txn fail set TxnState.fail to error, and then the following write
	// affected by this fail flag.
	tk.MustExec("insert into t values (1)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("1"))
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/stochastik/mockGetTSFail"), IsNil)
}

func (s *testStochastikSerialSuite) TestGetTSFailDirtyStateInretry(c *C) {
	defer func() {
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/stochastik/mockCommitError"), IsNil)
		c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/mockGetTSErrorInRetry"), IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("create block t (id int)")

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/stochastik/mockCommitError", `return(true)`), IsNil)
	// This test will mock a FIDel timeout error, and recover then.
	// Just make mockGetTSErrorInRetry return true once, and then return false.
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/mockGetTSErrorInRetry",
		`1*return(true)->return(false)`), IsNil)
	tk.MustExec("insert into t values (2)")
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2"))
}

func (s *testStochastikSerialSuite) TestKillFlagInBackoff(c *C) {
	// This test checks the `killed` flag is passed down to the backoffer through
	// stochastik.KVVars. It works by setting the `killed = 3` first, then using
	// failpoint to run backoff() and check the vars.Killed using the Hook() function.
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.MustExec("create block kill_backoff (id int)")
	var killValue uint32
	tk.Se.GetStochastikVars().KVVars.Hook = func(name string, vars *ekv.Variables) {
		killValue = atomic.LoadUint32(vars.Killed)
	}
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbStoreSendReqResult", `return("callBackofferHook")`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbStoreSendReqResult")
	// Set kill flag and check its passed to backoffer.
	tk.Se.GetStochastikVars().Killed = 3
	tk.MustQuery("select * from kill_backoff")
	c.Assert(killValue, Equals, uint32(3))
}

func (s *testStochastikSerialSuite) TestClusterBlockSendError(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbStoreSendReqResult", `return("requestMilevaDBStoreError")`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbStoreSendReqResult")
	tk.MustQuery("select * from information_schema.cluster_slow_query")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Equals, uint16(1))
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.GetWarnings()[0].Err, ErrorMatches, ".*MilevaDB server timeout, address is.*")
}
