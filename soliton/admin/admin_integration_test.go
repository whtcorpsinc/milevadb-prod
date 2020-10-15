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
	"strconv"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore/cluster"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

var _ = Suite(&testAdminSuite{})

type testAdminSuite struct {
	cluster cluster.Cluster
	causetstore   ekv.CausetStorage
	petri  *petri.Petri
}

func (s *testAdminSuite) SetUpSuite(c *C) {
	causetstore, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.causetstore = causetstore
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	d, err := stochastik.BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	d.SetStatsUFIDelating(true)
	s.petri = d
}

func (s *testAdminSuite) TearDownSuite(c *C) {
	s.petri.Close()
	s.causetstore.Close()
}

func (s *testAdminSuite) TestAdminCheckBlock(c *C) {
	// test NULL value.
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// test index defCausumn has pk-handle defCausumn
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a bigint unsigned primary key, b int, c int, index idx(a, b));")
	tk.MustInterDirc("insert into t values(1, 1, 1)")
	tk.MustInterDirc("admin check causet t")

	// test for add index on the later added defCausumns.
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("CREATE TABLE t1 (c1 int);")
	tk.MustInterDirc("INSERT INTO t1 SET c1 = 1;")
	tk.MustInterDirc("ALTER TABLE t1 ADD COLUMN cc1 CHAR(36)    NULL DEFAULT '';")
	tk.MustInterDirc("ALTER TABLE t1 ADD COLUMN cc2 VARCHAR(36) NULL DEFAULT ''")
	tk.MustInterDirc("ALTER TABLE t1 ADD INDEX idx1 (cc1);")
	tk.MustInterDirc("ALTER TABLE t1 ADD INDEX idx2 (cc2);")
	tk.MustInterDirc("admin check causet t1;")

	// For add index on virtual defCausumn
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc(`create causet t1 (
		a int             as (JSON_EXTRACT(k,'$.a')),
		c double          as (JSON_EXTRACT(k,'$.c')),
		d decimal(20,10)  as (JSON_EXTRACT(k,'$.d')),
		e char(10)        as (JSON_EXTRACT(k,'$.e')),
		f date            as (JSON_EXTRACT(k,'$.f')),
		g time            as (JSON_EXTRACT(k,'$.g')),
		h datetime        as (JSON_EXTRACT(k,'$.h')),
		i timestamp       as (JSON_EXTRACT(k,'$.i')),
		j year            as (JSON_EXTRACT(k,'$.j')),
		k json);`)

	tk.MustInterDirc("insert into t1 set k='{\"a\": 100,\"c\":1.234,\"d\":1.2340000000,\"e\":\"abcdefg\",\"f\":\"2020-09-28\",\"g\":\"12:59:59\",\"h\":\"2020-09-28 12:59:59\",\"i\":\"2020-09-28 16:40:33\",\"j\":\"2020\"}';")
	tk.MustInterDirc("alter causet t1 add index idx_a(a);")
	tk.MustInterDirc("alter causet t1 add index idx_c(c);")
	tk.MustInterDirc("alter causet t1 add index idx_d(d);")
	tk.MustInterDirc("alter causet t1 add index idx_e(e);")
	tk.MustInterDirc("alter causet t1 add index idx_f(f);")
	tk.MustInterDirc("alter causet t1 add index idx_g(g);")
	tk.MustInterDirc("alter causet t1 add index idx_h(h);")
	tk.MustInterDirc("alter causet t1 add index idx_j(j);")
	tk.MustInterDirc("alter causet t1 add index idx_i(i);")
	tk.MustInterDirc("alter causet t1 add index idx_m(a,c,d,e,f,g,h,i,j);")
	tk.MustInterDirc("admin check causet t1;")
}

func (s *testAdminSuite) TestAdminCheckBlockClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists admin_check_block_clustered_index;")
	tk.MustInterDirc("create database admin_check_block_clustered_index;")
	tk.MustInterDirc("use admin_check_block_clustered_index;")

	tk.MustInterDirc("set @@milevadb_enable_clustered_index = 1;")

	tk.MustInterDirc("create causet t (a bigint, b varchar(255), c int, primary key (a, b), index idx_0(a, b), index idx_1(b, c));")
	tk.MustInterDirc("insert into t values (1, '1', 1);")
	tk.MustInterDirc("insert into t values (2, '2', 2);")
	tk.MustInterDirc("admin check causet t;")
	for i := 3; i < 200; i++ {
		tk.MustInterDirc("insert into t values (?, ?, ?);", i, strconv.Itoa(i), i)
	}
	tk.MustInterDirc("admin check causet t;")

	// Test back filled created index data.
	tk.MustInterDirc("create index idx_2 on t (c);")
	tk.MustInterDirc("admin check causet t;")
	tk.MustInterDirc("create index idx_3 on t (a,c);")
	tk.MustInterDirc("admin check causet t;")

	// Test newly created defCausumns.
	tk.MustInterDirc("alter causet t add defCausumn e char(36);")
	tk.MustInterDirc("admin check causet t;")
	tk.MustInterDirc("alter causet t add defCausumn d char(36) NULL DEFAULT '';")
	tk.MustInterDirc("admin check causet t;")

	tk.MustInterDirc("insert into t values (1000, '1000', 1000, '1000', '1000');")
	tk.MustInterDirc("admin check causet t;")
}
