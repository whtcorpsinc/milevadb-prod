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
	"os"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testSuite1) TestIndexAdvise(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)

	_, err := tk.InterDirc("index advise infile '/tmp/nonexistence.allegrosql'")
	c.Assert(err.Error(), Equals, "Index Advise: don't support load file without local field")
	_, err = tk.InterDirc("index advise local infile ''")
	c.Assert(err.Error(), Equals, "Index Advise: infile path is empty")
	_, err = tk.InterDirc("index advise local infile '/tmp/nonexistence.allegrosql' lines terminated by ''")
	c.Assert(err.Error(), Equals, "Index Advise: don't support advise index for ALLEGROALLEGROSQL terminated by nil")

	path := "/tmp/index_advise.allegrosql"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		err = fp.Close()
		c.Assert(err, IsNil)
		err = os.Remove(path)
		c.Assert(err, IsNil)
	}()

	_, err = fp.WriteString("\n" +
		"select * from t;\n" +
		"\n" +
		"select * from t where a > 1;\n" +
		"select a from t where a > 1 and a < 100;\n" +
		"\n" +
		"\n" +
		"select a,b from t1,t2 where t1.a = t2.b;\n" +
		"\n")
	c.Assert(err, IsNil)

	// TODO: Using "tastCase" to do more test when we finish the index advisor completely.
	tk.MustInterDirc("index advise local infile '/tmp/index_advise.allegrosql' max_minutes 3 max_idxnum per_block 4 per_db 5")
	ctx := tk.Se.(stochastikctx.Context)
	ia, ok := ctx.Value(interlock.IndexAdviseVarKey).(*interlock.IndexAdviseInfo)
	defer ctx.SetValue(interlock.IndexAdviseVarKey, nil)
	c.Assert(ok, IsTrue)
	c.Assert(ia.MaxMinutes, Equals, uint64(3))
	c.Assert(ia.MaxIndexNum.PerBlock, Equals, uint64(4))
	c.Assert(ia.MaxIndexNum.PerDB, Equals, uint64(5))

}
