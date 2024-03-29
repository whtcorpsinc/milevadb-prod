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

package dbs_test

import (
	"errors"
	"time"

	BerolinaSQL_mysql "github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

var _ = SerialSuites(&testDeferredCausetTypeChangeSuite{})

type testDeferredCausetTypeChangeSuite struct {
	causetstore ekv.CausetStorage
	dbInfo      *perceptron.DBInfo
	dom         *petri.Petri
}

func (s *testDeferredCausetTypeChangeSuite) SetUpSuite(c *C) {
	var err error
	dbs.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	s.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.dom, err = stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
}

func (s *testDeferredCausetTypeChangeSuite) TearDownSuite(c *C) {
	s.dom.Close()
	c.Assert(s.causetstore.Close(), IsNil)
}

func (s *testDeferredCausetTypeChangeSuite) TestDeferredCausetTypeChangeBetweenInteger(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// Enable defCausumn change variable.
	tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = true
	defer func() {
		tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = false
	}()

	// Modify defCausumn from null to not null.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int null, b int null)")
	tk.MustInterDirc("alter causet t modify defCausumn b int not null")

	tk.MustInterDirc("insert into t(a, b) values (null, 1)")
	// Modify defCausumn from null to not null in same type will cause ErrInvalidUseOfNull
	tk.MustGetErrCode("alter causet t modify defCausumn a int not null", allegrosql.ErrInvalidUseOfNull)

	// Modify defCausumn from null to not null in different type will cause WarnDataTruncated.
	tk.MustGetErrCode("alter causet t modify defCausumn a tinyint not null", allegrosql.WarnDataTruncated)
	tk.MustGetErrCode("alter causet t modify defCausumn a bigint not null", allegrosql.WarnDataTruncated)

	// Modify defCausumn not null to null.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int not null, b int not null)")
	tk.MustInterDirc("alter causet t modify defCausumn b int null")

	tk.MustInterDirc("insert into t(a, b) values (1, null)")
	tk.MustInterDirc("alter causet t modify defCausumn a int null")

	// Modify defCausumn from unsigned to signed and from signed to unsigned.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int unsigned, b int signed)")
	tk.MustInterDirc("insert into t(a, b) values (1, 1)")
	tk.MustInterDirc("alter causet t modify defCausumn a int signed")
	tk.MustInterDirc("alter causet t modify defCausumn b int unsigned")

	// Modify defCausumn from small type to big type.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a tinyint)")
	tk.MustInterDirc("alter causet t modify defCausumn a smallint")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a tinyint)")
	tk.MustInterDirc("insert into t(a) values (127)")
	tk.MustInterDirc("alter causet t modify defCausumn a smallint")
	tk.MustInterDirc("alter causet t modify defCausumn a mediumint")
	tk.MustInterDirc("alter causet t modify defCausumn a int")
	tk.MustInterDirc("alter causet t modify defCausumn a bigint")

	// Modify defCausumn from big type to small type.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a bigint)")
	tk.MustInterDirc("alter causet t modify defCausumn a int")
	tk.MustInterDirc("alter causet t modify defCausumn a mediumint")
	tk.MustInterDirc("alter causet t modify defCausumn a smallint")
	tk.MustInterDirc("alter causet t modify defCausumn a tinyint")

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a bigint)")
	tk.MustInterDirc("insert into t(a) values (9223372036854775807)")
	tk.MustGetErrCode("alter causet t modify defCausumn a int", allegrosql.ErrDataOutOfRange)
	tk.MustGetErrCode("alter causet t modify defCausumn a mediumint", allegrosql.ErrDataOutOfRange)
	tk.MustGetErrCode("alter causet t modify defCausumn a smallint", allegrosql.ErrDataOutOfRange)
	tk.MustGetErrCode("alter causet t modify defCausumn a tinyint", allegrosql.ErrDataOutOfRange)
	_, err := tk.InterDirc("admin check causet t")
	c.Assert(err, IsNil)
}

func (s *testDeferredCausetTypeChangeSuite) TestDeferredCausetTypeChangeStateBetweenInteger(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c1 int, c2 int)")
	tk.MustInterDirc("insert into t(c1, c2) values (1, 1)")
	// Enable defCausumn change variable.
	tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = true
	defer func() {
		tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = false
	}()

	// use new stochastik to check spacetime in callback function.
	internalTK := testkit.NewTestKit(c, s.causetstore)
	internalTK.MustInterDirc("use test")

	tbl := testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.DefCauss()), Equals, 2)
	c.Assert(getModifyDeferredCauset(c, tk.Se.(stochastikctx.Context), "test", "t", "c2", false), NotNil)

	originalHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)

	hook := &dbs.TestDBSCallback{}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.BlockID {
			return
		}
		switch job.SchemaState {
		case perceptron.StateNone:
			tbl = testGetBlockByName(c, internalTK.Se, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.DefCauss()) != 2 {
				checkErr = errors.New("len(defcaus) is not right")
			}
		case perceptron.StateDeleteOnly, perceptron.StateWriteOnly, perceptron.StateWriteReorganization:
			tbl = testGetBlockByName(c, internalTK.Se, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.(*blocks.BlockCommon).DeferredCausets) != 3 {
				// changingDefCauss has been added into spacetime.
				checkErr = errors.New("len(defcaus) is not right")
			} else if getModifyDeferredCauset(c, internalTK.Se.(stochastikctx.Context), "test", "t", "c2", true).Flag&BerolinaSQL_mysql.PreventNullInsertFlag == uint(0) {
				checkErr = errors.New("old defCaus's flag is not right")
			} else if getModifyDeferredCauset(c, internalTK.Se.(stochastikctx.Context), "test", "t", "_DefCaus$_c2", true) == nil {
				checkErr = errors.New("changingDefCaus is nil")
			}
		}
	}
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	// Alter allegrosql will modify defCausumn c2 to tinyint not null.
	ALLEGROALLEGROSQL := "alter causet t modify defCausumn c2 tinyint not null"
	tk.MustInterDirc(ALLEGROALLEGROSQL)
	// Assert the checkErr in the job of every state.
	c.Assert(checkErr, IsNil)

	// Check the defCaus spacetime after the defCausumn type change.
	tbl = testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.DefCauss()), Equals, 2)
	defCaus := getModifyDeferredCauset(c, tk.Se.(stochastikctx.Context), "test", "t", "c2", false)
	c.Assert(defCaus, NotNil)
	c.Assert(BerolinaSQL_mysql.HasNotNullFlag(defCaus.Flag), Equals, true)
	c.Assert(defCaus.Flag&BerolinaSQL_mysql.NoDefaultValueFlag, Not(Equals), uint(0))
	c.Assert(defCaus.Tp, Equals, BerolinaSQL_mysql.TypeTiny)
	c.Assert(defCaus.ChangeStateInfo, IsNil)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

func (s *testDeferredCausetTypeChangeSuite) TestRollbackDeferredCausetTypeChangeBetweenInteger(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (c1 bigint, c2 bigint)")
	tk.MustInterDirc("insert into t(c1, c2) values (1, 1)")
	// Enable defCausumn change variable.
	tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = true
	defer func() {
		tk.Se.GetStochastikVars().EnableChangeDeferredCausetType = false
	}()

	tbl := testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.DefCauss()), Equals, 2)
	c.Assert(getModifyDeferredCauset(c, tk.Se.(stochastikctx.Context), "test", "t", "c2", false), NotNil)

	originalHook := s.dom.DBS().GetHook()
	defer s.dom.DBS().(dbs.DBSForTest).SetHook(originalHook)

	hook := &dbs.TestDBSCallback{}
	// Mock roll back at perceptron.StateNone.
	customizeHookRollbackAtState(hook, tbl, perceptron.StateNone)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	// Alter allegrosql will modify defCausumn c2 to bigint not null.
	ALLEGROALLEGROSQL := "alter causet t modify defCausumn c2 int not null"
	_, err := tk.InterDirc(ALLEGROALLEGROSQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:1]MockRollingBackInCallBack-none")
	assertRollBackedDefCausUnchanged(c, tk)

	// Mock roll back at perceptron.StateDeleteOnly.
	customizeHookRollbackAtState(hook, tbl, perceptron.StateDeleteOnly)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	_, err = tk.InterDirc(ALLEGROALLEGROSQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:1]MockRollingBackInCallBack-delete only")
	assertRollBackedDefCausUnchanged(c, tk)

	// Mock roll back at perceptron.StateWriteOnly.
	customizeHookRollbackAtState(hook, tbl, perceptron.StateWriteOnly)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	_, err = tk.InterDirc(ALLEGROALLEGROSQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:1]MockRollingBackInCallBack-write only")
	assertRollBackedDefCausUnchanged(c, tk)

	// Mock roll back at perceptron.StateWriteReorg.
	customizeHookRollbackAtState(hook, tbl, perceptron.StateWriteReorganization)
	s.dom.DBS().(dbs.DBSForTest).SetHook(hook)
	_, err = tk.InterDirc(ALLEGROALLEGROSQL)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:1]MockRollingBackInCallBack-write reorganization")
	assertRollBackedDefCausUnchanged(c, tk)
}

func customizeHookRollbackAtState(hook *dbs.TestDBSCallback, tbl causet.Block, state perceptron.SchemaState) {
	hook.OnJobRunBeforeExported = func(job *perceptron.Job) {
		if tbl.Meta().ID != job.BlockID {
			return
		}
		if job.SchemaState == state {
			job.State = perceptron.JobStateRollingback
			job.Error = mockTerrorMap[state.String()]
		}
	}
}

func assertRollBackedDefCausUnchanged(c *C, tk *testkit.TestKit) {
	tbl := testGetBlockByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(len(tbl.DefCauss()), Equals, 2)
	defCaus := getModifyDeferredCauset(c, tk.Se.(stochastikctx.Context), "test", "t", "c2", false)
	c.Assert(defCaus, NotNil)
	c.Assert(defCaus.Flag, Equals, uint(0))
	c.Assert(defCaus.Tp, Equals, BerolinaSQL_mysql.TypeLonglong)
	c.Assert(defCaus.ChangeStateInfo, IsNil)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

var mockTerrorMap = make(map[string]*terror.Error)

func init() {
	// Since terror new action will cause data race with other test suite (getTerrorCode) in parallel, we init it all here.
	mockTerrorMap[perceptron.StateNone.String()] = terror.ClassDBS.New(1, "MockRollingBackInCallBack-"+perceptron.StateNone.String())
	mockTerrorMap[perceptron.StateDeleteOnly.String()] = terror.ClassDBS.New(1, "MockRollingBackInCallBack-"+perceptron.StateDeleteOnly.String())
	mockTerrorMap[perceptron.StateWriteOnly.String()] = terror.ClassDBS.New(1, "MockRollingBackInCallBack-"+perceptron.StateWriteOnly.String())
	mockTerrorMap[perceptron.StateWriteReorganization.String()] = terror.ClassDBS.New(1, "MockRollingBackInCallBack-"+perceptron.StateWriteReorganization.String())
}
