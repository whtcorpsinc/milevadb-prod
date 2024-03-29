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

func (s *testSuite1) TestTraceInterDirc(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	testALLEGROSQL := `create causet trace (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustInterDirc(testALLEGROSQL)
	tk.MustInterDirc("trace insert into trace (c1, c2, c3) values (1, 2, 3)")
	rows := tk.MustQuery("trace select * from trace where id = 0;").Events()
	c.Assert(len(rows), GreaterEqual, 1)

	// +---------------------------+-----------------+------------+
	// | operation                 | startTS         | duration   |
	// +---------------------------+-----------------+------------+
	// | stochastik.getTxnFuture      | 22:08:38.247834 | 78.909µs   |
	// |   ├─stochastik.InterDircute       | 22:08:38.247829 | 1.478487ms |
	// |   ├─stochastik.ParseALLEGROSQL      | 22:08:38.248457 | 71.159µs   |
	// |   ├─interlock.Compile      | 22:08:38.248578 | 45.329µs   |
	// |   ├─stochastik.runStmt       | 22:08:38.248661 | 75.13µs    |
	// |   ├─stochastik.CommitTxn     | 22:08:38.248699 | 13.213µs   |
	// |   └─recordSet.Next        | 22:08:38.249340 | 155.317µs  |
	// +---------------------------+-----------------+------------+
	rows = tk.MustQuery("trace format='event' select * from trace where id = 0;").Events()
	c.Assert(len(rows) > 1, IsTrue)
	c.Assert(rowsOrdered(rows), IsTrue)

	rows = tk.MustQuery("trace format='event' delete from trace where id = 0").Events()
	c.Assert(len(rows) > 1, IsTrue)
	c.Assert(rowsOrdered(rows), IsTrue)

	tk.MustInterDirc("trace format='log' insert into trace (c1, c2, c3) values (1, 2, 3)")
	rows = tk.MustQuery("trace format='log' select * from trace where id = 0;").Events()
	c.Assert(len(rows), GreaterEqual, 1)
}

func rowsOrdered(rows [][]interface{}) bool {
	for idx := range rows {
		if _, ok := rows[idx][1].(string); !ok {
			return false
		}
		if idx == 0 {
			continue
		}
		if rows[idx-1][1].(string) > rows[idx][1].(string) {
			return false
		}
	}
	return true
}
