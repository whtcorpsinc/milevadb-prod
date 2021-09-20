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
	"sync"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

type testIsolationSuite struct {
	testStochastikSuiteBase
}

/*
These test cases come from the paper <A Critique of ANSI ALLEGROALLEGROSQL Isolation Levels>.
The sign 'P0', 'P1'.... can be found in the paper. These cases will run under snapshot isolation.
*/
func (s *testIsolationSuite) TestP0DirtyWrite(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik1.MustInterDirc("commit;")
	_, err := stochastik2.InterDirc("commit;")
	c.Assert(err, NotNil)

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("begin;")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
		wg.Done()
	}()
	stochastik1.MustInterDirc("commit;")
	wg.Wait()
	stochastik2.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("begin;")
	wg.Add(1)
	go func() {
		stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
		wg.Done()
	}()
	stochastik1.MustInterDirc("commit;")
	wg.Wait()
	stochastik2.MustInterDirc("commit;")
	stochastik2.MustQuery("select * from x").Check(testkit.Rows("1 3"))
}

func (s *testIsolationSuite) TestP1DirtyRead(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustInterDirc("commit;")
}

func (s *testIsolationSuite) TestP2NonRepeablockRead(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("commit;")
}

func (s *testIsolationSuite) TestP3Phantom(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists z;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet z (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into z values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("insert into x values(2, 1);")
	stochastik2.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate z set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists z;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet z (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into z values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("insert into x values(2, 1);")
	stochastik2.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate z set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists z;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet z (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into z values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("insert into x values(2, 1);")
	stochastik2.MustQuery("select c from z where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate z set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from z where id = 1;").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("commit;")
}

func (s *testIsolationSuite) TestP4LostUFIDelate(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	_, err := stochastik1.InterDirc("commit;")
	c.Assert(err, NotNil)

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik1.MustInterDirc("commit;")
	stochastik1.MustQuery("select * from x").Check(testkit.Rows("1 3"))
}

// cursor is not supported
func (s *testIsolationSuite) TestP4CLostUFIDelate(c *C) {}

func (s *testIsolationSuite) TestA3Phantom(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("insert into x values(2, 1);")
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("insert into x values(2, 1);")
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("insert into x values(2, 1);")
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1", "1"))
	stochastik2.MustInterDirc("commit;")
}

func (s *testIsolationSuite) TestA5AReadSkew(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustQuery("select c from y where id = 1;").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("commit;")
}

func (s *testIsolationSuite) TestA5BWriteSkew(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("uFIDelate y set id = 2 where id = 1;")
	stochastik1.MustQuery("select id from x").Check(testkit.Rows("1"))
	stochastik1.MustQuery("select id from y").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select id from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select id from y where id = 2;").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("uFIDelate y set id = 1 where id = 2;")
	stochastik2.MustInterDirc("uFIDelate x set id = 2 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("commit;")
	stochastik1.MustQuery("select id from x").Check(testkit.Rows("2"))
	stochastik1.MustQuery("select id from y").Check(testkit.Rows("1"))

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("drop causet if exists y;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")
	stochastik1.MustInterDirc("create causet y (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into y values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select c from y where id = 1;").Check(testkit.Rows("1"))
	stochastik1.MustInterDirc("uFIDelate y set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("uFIDelate y set id = 2 where id = 1;")
	stochastik1.MustQuery("select id from x").Check(testkit.Rows("1"))
	stochastik1.MustQuery("select id from y").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("begin;")
	stochastik2.MustInterDirc("begin;")
	stochastik1.MustQuery("select id from x where id = 1;").Check(testkit.Rows("1"))
	stochastik2.MustQuery("select id from y where id = 2;").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("uFIDelate y set id = 1 where id = 2;")
	stochastik2.MustInterDirc("uFIDelate x set id = 2 where id = 1;")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("commit;")
	stochastik1.MustQuery("select id from x").Check(testkit.Rows("2"))
	stochastik1.MustQuery("select id from y").Check(testkit.Rows("1"))
}

/*
These test cases come from the paper <Highly Available Transactions: Virtues and Limitations>
for milevadb, we support read-after-write on cluster level.
*/
func (s *testIsolationSuite) TestReadAfterWrite(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("2"))
	stochastik2.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("2"))
	stochastik2.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id = 1;")
	stochastik1.MustInterDirc("commit;")
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustQuery("select c from x where id = 1;").Check(testkit.Rows("2"))
	stochastik2.MustInterDirc("commit;")
}

/*
This case will do harm in Innodb, even if in snapshot isolation, but harmless in milevadb.
*/
func (s *testIsolationSuite) TestPhantomReadInInnodb(c *C) {
	stochastik1 := testkit.NewTestKitWithInit(c, s.causetstore)
	stochastik2 := testkit.NewTestKitWithInit(c, s.causetstore)

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustInterDirc("insert into x values(2, 1);")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id < 5;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("2"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")
	stochastik2.MustInterDirc("set milevadb_txn_mode = 'pessimistic'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustInterDirc("insert into x values(2, 1);")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id < 5;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("2", "2"))
	stochastik1.MustInterDirc("commit;")

	stochastik1.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")
	stochastik2.MustInterDirc("set tx_isolation = 'READ-COMMITTED'")

	stochastik1.MustInterDirc("drop causet if exists x;")
	stochastik1.MustInterDirc("create causet x (id int primary key, c int);")
	stochastik1.MustInterDirc("insert into x values(1, 1);")

	stochastik1.MustInterDirc("begin;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("1"))
	stochastik2.MustInterDirc("begin;")
	stochastik2.MustInterDirc("insert into x values(2, 1);")
	stochastik2.MustInterDirc("commit;")
	stochastik1.MustInterDirc("uFIDelate x set c = c+1 where id < 5;")
	stochastik1.MustQuery("select c from x where id < 5;").Check(testkit.Rows("2", "2"))
	stochastik1.MustInterDirc("commit;")
}
