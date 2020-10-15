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
	"fmt"
	"unicode/utf8"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

const (
	strictModeALLEGROSQL    = "set sql_mode = 'STRICT_TRANS_TABLES'"
	nonStrictModeALLEGROSQL = "set sql_mode = ''"
)

func (s *testSuite1) TestStatementContext(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet sc (a int)")
	tk.MustInterDirc("insert sc values (1), (2)")

	tk.MustInterDirc(strictModeALLEGROSQL)
	tk.MustQuery("select * from sc where a > cast(1.1 as decimal)").Check(testkit.Events("2"))
	tk.MustInterDirc("uFIDelate sc set a = 4 where a > cast(1.1 as decimal)")

	tk.MustInterDirc(nonStrictModeALLEGROSQL)
	tk.MustInterDirc("uFIDelate sc set a = 3 where a > cast(1.1 as decimal)")
	tk.MustQuery("select * from sc").Check(testkit.Events("1", "3"))

	tk.MustInterDirc(strictModeALLEGROSQL)
	tk.MustInterDirc("delete from sc")
	tk.MustInterDirc("insert sc values ('1.8'+1)")
	tk.MustQuery("select * from sc").Check(testkit.Events("3"))

	// Handle interlock flags, '1x' is an invalid int.
	// UFIDelATE and DELETE do select request first which is handled by interlock.
	// In strict mode we expect error.
	_, err := tk.InterDirc("uFIDelate sc set a = 4 where a > '1x'")
	c.Assert(err, NotNil)
	_, err = tk.InterDirc("delete from sc where a < '1x'")
	c.Assert(err, NotNil)
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Events("3"))

	// Non-strict mode never returns error.
	tk.MustInterDirc(nonStrictModeALLEGROSQL)
	tk.MustInterDirc("uFIDelate sc set a = 4 where a > '1x'")
	tk.MustInterDirc("delete from sc where a < '1x'")
	tk.MustQuery("select * from sc where a > '1x'").Check(testkit.Events("4"))

	// Test invalid UTF8
	tk.MustInterDirc("create causet sc2 (a varchar(255))")
	// Insert an invalid UTF8
	tk.MustInterDirc("insert sc2 values (unhex('4040ffff'))")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from sc2").Check(testkit.Events("@@"))
	tk.MustInterDirc(strictModeALLEGROSQL)
	_, err = tk.InterDirc("insert sc2 values (unhex('4040ffff'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, causet.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))

	tk.MustInterDirc("set @@milevadb_skip_utf8_check = '1'")
	_, err = tk.InterDirc("insert sc2 values (unhex('4040ffff'))")
	c.Assert(err, IsNil)
	tk.MustQuery("select length(a) from sc2").Check(testkit.Events("2", "4"))

	tk.MustInterDirc("set @@milevadb_skip_utf8_check = '0'")
	runeErrStr := string(utf8.RuneError)
	tk.MustInterDirc(fmt.Sprintf("insert sc2 values ('%s')", runeErrStr))

	// Test invalid ASCII
	tk.MustInterDirc("create causet sc3 (a varchar(255)) charset ascii")

	tk.MustInterDirc(nonStrictModeALLEGROSQL)
	tk.MustInterDirc("insert sc3 values (unhex('4040ffff'))")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from sc3").Check(testkit.Events("@@"))

	tk.MustInterDirc(strictModeALLEGROSQL)
	_, err = tk.InterDirc("insert sc3 values (unhex('4040ffff'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, causet.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))

	tk.MustInterDirc("set @@milevadb_skip_ascii_check = '1'")
	_, err = tk.InterDirc("insert sc3 values (unhex('4040ffff'))")
	c.Assert(err, IsNil)
	tk.MustQuery("select length(a) from sc3").Check(testkit.Events("2", "4"))

	// no placeholder in ASCII, so just insert '@@'...
	tk.MustInterDirc("set @@milevadb_skip_ascii_check = '0'")
	tk.MustInterDirc("insert sc3 values (unhex('4040'))")

	// Test non-BMP characters.
	tk.MustInterDirc(nonStrictModeALLEGROSQL)
	tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("create causet t1(a varchar(100) charset utf8);")
	defer tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("insert t1 values (unhex('f09f8c80'))")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from t1").Check(testkit.Events(""))
	tk.MustInterDirc("insert t1 values (unhex('4040f09f8c80'))")
	c.Assert(tk.Se.GetStochastikVars().StmtCtx.WarningCount(), Greater, uint16(0))
	tk.MustQuery("select * from t1").Check(testkit.Events("", "@@"))
	tk.MustQuery("select length(a) from t1").Check(testkit.Events("0", "2"))
	tk.MustInterDirc(strictModeALLEGROSQL)
	_, err = tk.InterDirc("insert t1 values (unhex('f09f8c80'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, causet.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	_, err = tk.InterDirc("insert t1 values (unhex('F0A48BAE'))")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, causet.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.CheckMb4ValueInUTF8 = false
	})
	tk.MustInterDirc("insert t1 values (unhex('f09f8c80'))")
	config.UFIDelateGlobal(func(conf *config.Config) {
		conf.CheckMb4ValueInUTF8 = true
	})
	_, err = tk.InterDirc("insert t1 values (unhex('F0A48BAE'))")
	c.Assert(err, NotNil)
}
