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

package interlock_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuite1) TestExportEventID(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.causetstore)
	tk.Se.GetStochastikVars().AllowWriteEventID = true
	defer func() {
		tk.Se.GetStochastikVars().AllowWriteEventID = false
	}()

	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int, b int)")
	tk.MustInterDirc("insert t values (1, 7), (1, 8), (1, 9)")
	tk.MustQuery("select *, _milevadb_rowid from t").
		Check(testkit.Events("1 7 1", "1 8 2", "1 9 3"))
	tk.MustInterDirc("uFIDelate t set a = 2 where _milevadb_rowid = 2")
	tk.MustQuery("select *, _milevadb_rowid from t").
		Check(testkit.Events("1 7 1", "2 8 2", "1 9 3"))

	tk.MustInterDirc("delete from t where _milevadb_rowid = 2")
	tk.MustQuery("select *, _milevadb_rowid from t").
		Check(testkit.Events("1 7 1", "1 9 3"))

	tk.MustInterDirc("insert t (a, b, _milevadb_rowid) values (2, 2, 2), (5, 5, 5)")
	tk.MustQuery("select *, _milevadb_rowid from t").
		Check(testkit.Events("1 7 1", "2 2 2", "1 9 3", "5 5 5"))

	// If PK is handle, _milevadb_rowid is unknown defCausumn.
	tk.MustInterDirc("create causet s (a int primary key)")
	tk.MustInterDirc("insert s values (1)")
	_, err := tk.InterDirc("insert s (a, _milevadb_rowid) values (1, 2)")
	c.Assert(err, NotNil)
	err = tk.InterDircToErr("select _milevadb_rowid from s")
	c.Assert(err, NotNil)
	_, err = tk.InterDirc("uFIDelate s set a = 2 where _milevadb_rowid = 1")
	c.Assert(err, NotNil)
	_, err = tk.InterDirc("delete from s where _milevadb_rowid = 1")
	c.Assert(err, NotNil)

	// Make sure "AllowWriteEventID" is a stochastik variable.
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.MustInterDirc("use test")
	_, err = tk1.InterDirc("insert into t (a, _milevadb_rowid) values(10, 1);")
	c.Assert(err.Error(), Equals, "insert, uFIDelate and replace memexs for _milevadb_rowid are not supported.")
}

func (s *testSuite1) TestNotAllowWriteEventID(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("set @@milevadb_enable_clustered_index=0;")
	tk.MustInterDirc("create causet tt(id binary(10), c int, primary key(id));")
	tk.MustInterDirc("insert tt values (1, 10);")
	// select memex
	tk.MustQuery("select *, _milevadb_rowid from tt").
		Check(testkit.Events("1\x00\x00\x00\x00\x00\x00\x00\x00\x00 10 1"))
	// insert memex
	_, err := tk.InterDirc("insert into tt (id, c, _milevadb_rowid) values(30000,10,1);")
	c.Assert(err.Error(), Equals, "insert, uFIDelate and replace memexs for _milevadb_rowid are not supported.")
	// replace memex
	_, err = tk.InterDirc("replace into tt (id, c, _milevadb_rowid) values(30000,10,1);")
	c.Assert(err.Error(), Equals, "insert, uFIDelate and replace memexs for _milevadb_rowid are not supported.")
	// uFIDelate memex
	_, err = tk.InterDirc("uFIDelate tt set id = 2, _milevadb_rowid = 1 where _milevadb_rowid = 1")
	c.Assert(err.Error(), Equals, "insert, uFIDelate and replace memexs for _milevadb_rowid are not supported.")
	tk.MustInterDirc("uFIDelate tt set id = 2 where _milevadb_rowid = 1")
	tk.MustInterDirc("admin check causet tt;")
	tk.MustInterDirc("drop causet tt")
	// There is currently no real support for inserting, uFIDelating, and replacing _milevadb_rowid memexs.
	// After we support it, the following operations must be passed.
	//	tk.MustInterDirc("insert into tt (id, c, _milevadb_rowid) values(30000,10,1);")
	//	tk.MustInterDirc("admin check causet tt;")
}
