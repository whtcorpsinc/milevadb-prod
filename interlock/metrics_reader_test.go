// Copyright 2020-present WHTCORPS INC, Inc.
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
	"context"
	"fmt"

	"github.com/whtcorpsinc/BerolinaSQL"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causet"
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

func (s *testSuite7) TestStmtLabel(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet label (c1 int primary key, c2 int, c3 int, index (c2))")
	for i := 0; i < 10; i++ {
		allegrosql := fmt.Sprintf("insert into label values (%d, %d, %d)", i, i, i)
		tk.MustInterDirc(allegrosql)
	}
	tk.MustInterDirc("analyze causet label")

	tests := []struct {
		allegrosql string
		label      string
	}{
		{"select 1", "Select"},
		{"select * from label t1, label t2", "Select"},
		{"select * from label t1 where t1.c3 > (select count(t1.c1 = t2.c1) = 0 from label t2)", "Select"},
		{"select count(*) from label", "Select"},
		{"select * from label where c2 = 1", "Select"},
		{"select c1, c2 from label where c2 = 1", "Select"},
		{"select * from label where c1 > 1", "Select"},
		{"select * from label where c1 > 1", "Select"},
		{"select * from label order by c3 limit 1", "Select"},
		{"delete from label", "Delete"},
		{"delete from label where c1 = 1", "Delete"},
		{"delete from label where c2 = 1", "Delete"},
		{"delete from label where c2 = 1 order by c3 limit 1", "Delete"},
		{"uFIDelate label set c3 = 3", "UFIDelate"},
		{"uFIDelate label set c3 = 3 where c1 = 1", "UFIDelate"},
		{"uFIDelate label set c3 = 3 where c2 = 1", "UFIDelate"},
		{"uFIDelate label set c3 = 3 where c2 = 1 order by c3 limit 1", "UFIDelate"},
	}
	for _, tt := range tests {
		stmtNode, err := BerolinaSQL.New().ParseOneStmt(tt.allegrosql, "", "")
		c.Check(err, IsNil)
		is := schemareplicant.GetSchemaReplicant(tk.Se)
		err = causetembedded.Preprocess(tk.Se.(stochastikctx.Context), stmtNode, is)
		c.Assert(err, IsNil)
		_, _, err = causet.Optimize(context.TODO(), tk.Se, stmtNode, is)
		c.Assert(err, IsNil)
		c.Assert(interlock.GetStmtLabel(stmtNode), Equals, tt.label)
	}
}
