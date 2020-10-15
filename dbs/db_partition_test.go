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
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/dbs/solitonutil"
	"github.com/whtcorpsinc/milevadb/petri"
	tmysql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

func (s *testIntegrationSuite3) TestCreateBlockWithPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists tp;")
	tk.MustInterDirc(`CREATE TABLE tp (a int) PARTITION BY RANGE(a) (
	PARTITION p0 VALUES LESS THAN (10),
	PARTITION p1 VALUES LESS THAN (20),
	PARTITION p2 VALUES LESS THAN (MAXVALUE)
	);`)
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("tp"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`a`")
	for _, FIDelef := range part.Definitions {
		c.Assert(FIDelef.ID, Greater, int64(0))
	}
	c.Assert(part.Definitions, HasLen, 3)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name.L, Equals, "p0")
	c.Assert(part.Definitions[1].LessThan[0], Equals, "20")
	c.Assert(part.Definitions[1].Name.L, Equals, "p1")
	c.Assert(part.Definitions[2].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[2].Name.L, Equals, "p2")

	tk.MustInterDirc("drop causet if exists employees;")
	sql1 := `create causet employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p2 values less than (2001)
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrSameNamePartition)

	sql2 := `create causet employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql2, tmysql.ErrRangeNotIncreasing)

	sql3 := `create causet employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1998),
		partition p2 values less than maxvalue,
		partition p3 values less than (2001)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrPartitionMaxvalue)

	sql4 := `create causet t4 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than maxvalue,
		partition p2 values less than (1991),
		partition p3 values less than (1995)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrPartitionMaxvalue)

	_, err = tk.InterDirc(`CREATE TABLE rc (
		a INT NOT NULL,
		b INT NOT NULL,
		c INT NOT NULL
	)
	partition by range defCausumns(a,b,c) (
	partition p0 values less than (10,5,1),
	partition p2 values less than (50,maxvalue,10),
	partition p3 values less than (65,30,13),
	partition p4 values less than (maxvalue,30,40)
	);`)
	c.Assert(err, IsNil)

	sql6 := `create causet employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		 partition p0 values less than (6 , 10)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrTooManyValues)

	sql7 := `create causet t7 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than (1991),
		partition p2 values less than maxvalue,
		partition p3 values less than maxvalue,
		partition p4 values less than (1995),
		partition p5 values less than maxvalue
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrPartitionMaxvalue)

	sql18 := `create causet t8 (
	a int not null,
	b int not null
	)
	partition by range( a ) (
		partition p1 values less than (19xx91),
		partition p2 values less than maxvalue
	);`
	tk.MustGetErrCode(sql18, allegrosql.ErrBadField)

	sql9 := `create TABLE t9 (
	defCaus1 int
	)
	partition by range( case when defCaus1 > 0 then 10 else 20 end ) (
		partition p0 values less than (2),
		partition p1 values less than (6)
	);`
	tk.MustGetErrCode(sql9, tmysql.ErrPartitionFunctionIsNotAllowed)

	_, err = tk.InterDirc(`CREATE TABLE t9 (
		a INT NOT NULL,
		b INT NOT NULL,
		c INT NOT NULL
	)
	partition by range defCausumns(a) (
	partition p0 values less than (10),
	partition p2 values less than (20),
	partition p3 values less than (20)
	);`)
	c.Assert(dbs.ErrRangeNotIncreasing.Equal(err), IsTrue)

	tk.MustGetErrCode(`create TABLE t10 (c1 int,c2 int) partition by range(c1 / c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFunctionIsNotAllowed)

	tk.MustInterDirc(`create TABLE t11 (c1 int,c2 int) partition by range(c1 div c2 ) (partition p0 values less than (2));`)
	tk.MustInterDirc(`create TABLE t12 (c1 int,c2 int) partition by range(c1 + c2 ) (partition p0 values less than (2));`)
	tk.MustInterDirc(`create TABLE t13 (c1 int,c2 int) partition by range(c1 - c2 ) (partition p0 values less than (2));`)
	tk.MustInterDirc(`create TABLE t14 (c1 int,c2 int) partition by range(c1 * c2 ) (partition p0 values less than (2));`)
	tk.MustInterDirc(`create TABLE t15 (c1 int,c2 int) partition by range( abs(c1) ) (partition p0 values less than (2));`)
	tk.MustInterDirc(`create TABLE t16 (c1 int) partition by range( c1) (partition p0 values less than (10));`)

	tk.MustGetErrCode(`create TABLE t17 (c1 int,c2 float) partition by range(c1 + c2 ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	tk.MustGetErrCode(`create TABLE t18 (c1 int,c2 float) partition by range( floor(c2) ) (partition p0 values less than (2));`, tmysql.ErrPartitionFuncNotAllowed)
	tk.MustInterDirc(`create TABLE t19 (c1 int,c2 float) partition by range( floor(c1) ) (partition p0 values less than (2));`)

	tk.MustInterDirc(`create TABLE t20 (c1 int,c2 bit(10)) partition by range(c2) (partition p0 values less than (10));`)
	tk.MustInterDirc(`create TABLE t21 (c1 int,c2 year) partition by range( c2 ) (partition p0 values less than (2000));`)

	tk.MustGetErrCode(`create TABLE t24 (c1 float) partition by range( c1 ) (partition p0 values less than (2000));`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)

	// test check order. The allegrosql below have 2 problem: 1. ErrFieldTypeNotAllowedAsPartitionField  2. ErrPartitionMaxvalue , allegrosql will return ErrPartitionMaxvalue.
	tk.MustGetErrCode(`create TABLE t25 (c1 float) partition by range( c1 ) (partition p1 values less than maxvalue,partition p0 values less than (2000));`, tmysql.ErrPartitionMaxvalue)

	// Fix issue 7362.
	tk.MustInterDirc("create causet test_partition(id bigint, name varchar(255), primary key(id)) ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY RANGE  COLUMNS(id) (PARTITION p1 VALUES LESS THAN (10) ENGINE = InnoDB);")

	// 'Less than' in partition memex could be a constant memex, notice that
	// the SHOW result changed.
	tk.MustInterDirc(`create causet t26 (a date)
			  partition by range(to_seconds(a))(
			  partition p0 values less than (to_seconds('2004-01-01')),
			  partition p1 values less than (to_seconds('2005-01-01')));`)
	tk.MustQuery("show create causet t26").Check(
		testkit.Rows("t26 CREATE TABLE `t26` (\n  `a` date DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY RANGE ( TO_SECONDS(`a`) ) (\n  PARTITION `p0` VALUES LESS THAN (63240134400),\n  PARTITION `p1` VALUES LESS THAN (63271756800)\n)"))
	tk.MustInterDirc(`create causet t27 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000),
		  partition p4 values less than (18446744073709551614)
		);`)
	tk.MustInterDirc(`create causet t28 (a bigint unsigned not null)
		  partition by range(a) (
		  partition p0 values less than (10),
		  partition p1 values less than (100),
		  partition p2 values less than (1000),
		  partition p3 values less than (18446744073709551000 + 1),
		  partition p4 values less than (18446744073709551000 + 10)
		);`)

	tk.MustInterDirc("set @@milevadb_enable_block_partition = 1")
	tk.MustInterDirc("set @@milevadb_enable_block_partition = 1")
	tk.MustInterDirc(`create causet t30 (
		  a int,
		  b float,
		  c varchar(30))
		  partition by range defCausumns (a, b)
		  (partition p0 values less than (10, 10.0))`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8200 Unsupported partition type, treat as normal causet"))

	tk.MustGetErrCode(`create causet t31 (a int not null) partition by range( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create causet t32 (a int not null) partition by range defCausumns( a );`, tmysql.ErrPartitionsMustBeDefined)
	tk.MustGetErrCode(`create causet t33 (a int, b int) partition by hash(a) partitions 0;`, tmysql.ErrNoParts)
	tk.MustGetErrCode(`create causet t33 (a timestamp, b int) partition by hash(a) partitions 30;`, tmysql.ErrFieldTypeNotAllowedAsPartitionField)
	tk.MustGetErrCode(`CREATE TABLE t34 (c0 INT) PARTITION BY HASH((CASE WHEN 0 THEN 0 ELSE c0 END )) PARTITIONS 1;`, tmysql.ErrPartitionFunctionIsNotAllowed)
	tk.MustGetErrCode(`CREATE TABLE t0(c0 INT) PARTITION BY HASH((c0<CURRENT_USER())) PARTITIONS 1;`, tmysql.ErrPartitionFunctionIsNotAllowed)
	// TODO: fix this one
	// tk.MustGetErrCode(`create causet t33 (a timestamp, b int) partition by hash(unix_timestamp(a)) partitions 30;`, tmysql.ErrPartitionFuncNotAllowed)

	// Fix issue 8647
	tk.MustGetErrCode(`CREATE TABLE trb8 (
		id int(11) DEFAULT NULL,
		name varchar(50) DEFAULT NULL,
		purchased date DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
	PARTITION BY RANGE ( year(notexist.purchased) - 1 ) (
		PARTITION p0 VALUES LESS THAN (1990),
		PARTITION p1 VALUES LESS THAN (1995),
		PARTITION p2 VALUES LESS THAN (2000),
		PARTITION p3 VALUES LESS THAN (2005)
	);`, tmysql.ErrBadField)

	// Fix a timezone dependent check bug introduced in https://github.com/whtcorpsinc/milevadb/pull/10655
	tk.MustInterDirc(`create causet t34 (dt timestamp(3)) partition by range (floor(unix_timestamp(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`)

	tk.MustGetErrCode(`create causet t34 (dt timestamp(3)) partition by range (unix_timestamp(date(dt))) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, tmysql.ErrWrongExprInPartitionFunc)

	tk.MustGetErrCode(`create causet t34 (dt datetime) partition by range (unix_timestamp(dt)) (
		partition p0 values less than (unix_timestamp('2020-04-04 00:00:00')),
		partition p1 values less than (unix_timestamp('2020-04-05 00:00:00')));`, tmysql.ErrWrongExprInPartitionFunc)

	// Fix https://github.com/whtcorpsinc/milevadb/issues/16333
	tk.MustInterDirc(`create causet t35 (dt timestamp) partition by range (unix_timestamp(dt))
(partition p0 values less than (unix_timestamp('2020-04-15 00:00:00')));`)

	tk.MustInterDirc(`drop causet if exists too_long_identifier`)
	tk.MustGetErrCode(`create causet too_long_identifier(a int)
partition by range (a)
(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than (10));`, tmysql.ErrTooLongIdent)

	tk.MustInterDirc(`drop causet if exists too_long_identifier`)
	tk.MustInterDirc("create causet too_long_identifier(a int) partition by range(a) (partition p0 values less than(10))")
	tk.MustGetErrCode("alter causet too_long_identifier add partition "+
		"(partition p0pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp values less than(20))", tmysql.ErrTooLongIdent)

	tk.MustInterDirc(`create causet t36 (a date, b datetime) partition by range (EXTRACT(YEAR_MONTH FROM a)) (
    partition p0 values less than (200),
    partition p1 values less than (300),
    partition p2 values less than maxvalue)`)
}

func (s *testIntegrationSuite2) TestCreateBlockWithHashPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists employees;")
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustInterDirc(`
	create causet employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash(store_id) partitions 4;`)

	tk.MustInterDirc("drop causet if exists employees;")
	tk.MustInterDirc(`
	create causet employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash( year(hired) ) partitions 4;`)

	// This query makes milevadb OOM without partition count check.
	tk.MustGetErrCode(`CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL DEFAULT '1970-01-01',
    separated DATE NOT NULL DEFAULT '9999-12-31',
    job_code INT,
    store_id INT
) PARTITION BY HASH(store_id) PARTITIONS 102400000000;`, tmysql.ErrTooManyPartitions)

	tk.MustInterDirc("CREATE TABLE t_linear (a int, b varchar(128)) PARTITION BY LINEAR HASH(a) PARTITIONS 4")
	tk.MustGetErrCode("select * from t_linear partition (p0)", tmysql.ErrPartitionClauseOnNonpartitioned)

	tk.MustInterDirc(`CREATE TABLE t_sub (a int, b varchar(128)) PARTITION BY RANGE( a ) SUBPARTITION BY HASH( a )
                                   SUBPARTITIONS 2 (
                                       PARTITION p0 VALUES LESS THAN (100),
                                       PARTITION p1 VALUES LESS THAN (200),
                                       PARTITION p2 VALUES LESS THAN MAXVALUE)`)
	tk.MustGetErrCode("select * from t_sub partition (p0)", tmysql.ErrPartitionClauseOnNonpartitioned)

	// Fix create partition causet using extract() function as partition key.
	tk.MustInterDirc("create causet t2 (a date, b datetime) partition by hash (EXTRACT(YEAR_MONTH FROM a)) partitions 7")
}

func (s *testIntegrationSuite1) TestCreateBlockWithRangeDeferredCausetPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists log_message_1;")
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustInterDirc(`
create causet log_message_1 (
    add_time datetime not null default '2000-01-01 00:00:00',
    log_level int unsigned not null default '0',
    log_host varchar(32) not null,
    service_name varchar(32) not null,
    message varchar(2000)
) partition by range defCausumns(add_time)(
    partition p201403 values less than ('2020-04-01'),
    partition p201404 values less than ('2020-05-01'),
    partition p201405 values less than ('2020-06-01'),
    partition p201406 values less than ('2020-07-01'),
    partition p201407 values less than ('2020-08-01'),
    partition p201408 values less than ('2020-09-01'),
    partition p201409 values less than ('2020-10-01'),
    partition p201410 values less than ('2020-11-01')
)`)
	tk.MustInterDirc("drop causet if exists log_message_1;")
	tk.MustInterDirc(`
	create causet log_message_1 (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash( year(hired) ) partitions 4;`)

	tk.MustInterDirc("drop causet if exists t")

	type testCase struct {
		allegrosql string
		err *terror.Error
	}

	cases := []testCase{
		{
			"create causet t (id int) partition by range defCausumns (id);",
			ast.ErrPartitionsMustBeDefined,
		},
		{
			"create causet t (id int) partition by range defCausumns (id) (partition p0 values less than (1, 2));",
			ast.ErrPartitionDeferredCausetList,
		},
		{
			"create causet t (a int) partition by range defCausumns (b) (partition p0 values less than (1, 2));",
			ast.ErrPartitionDeferredCausetList,
		},
		{
			"create causet t (a int) partition by range defCausumns (b) (partition p0 values less than (1));",
			dbs.ErrFieldNotFoundPart,
		},
		{
			"create causet t (id timestamp) partition by range defCausumns (id) (partition p0 values less than ('2020-01-09 11:23:34'));",
			dbs.ErrNotAllowedTypeInPartition,
		},
		{
			`create causet t29 (
				a decimal
			)
			partition by range defCausumns (a)
			(partition p0 values less than (0));`,
			dbs.ErrNotAllowedTypeInPartition,
		},
		{
			"create causet t (id text) partition by range defCausumns (id) (partition p0 values less than ('abc'));",
			dbs.ErrNotAllowedTypeInPartition,
		},
		// create as normal causet, warning.
		//	{
		//		"create causet t (a int, b varchar(64)) partition by range defCausumns (a, b) (" +
		//			"partition p0 values less than (1, 'a')," +
		//			"partition p1 values less than (1, 'a'))",
		//		dbs.ErrRangeNotIncreasing,
		//	},
		{
			"create causet t (a int, b varchar(64)) partition by range defCausumns ( b) (" +
				"partition p0 values less than ( 'a')," +
				"partition p1 values less than ('a'))",
			dbs.ErrRangeNotIncreasing,
		},
		// create as normal causet, warning.
		//	{
		//		"create causet t (a int, b varchar(64)) partition by range defCausumns (a, b) (" +
		//			"partition p0 values less than (1, 'b')," +
		//			"partition p1 values less than (1, 'a'))",
		//		dbs.ErrRangeNotIncreasing,
		//	},
		{
			"create causet t (a int, b varchar(64)) partition by range defCausumns (b) (" +
				"partition p0 values less than ('b')," +
				"partition p1 values less than ('a'))",
			dbs.ErrRangeNotIncreasing,
		},
		// create as normal causet, warning.
		//		{
		//			"create causet t (a int, b varchar(64)) partition by range defCausumns (a, b) (" +
		//				"partition p0 values less than (1, maxvalue)," +
		//				"partition p1 values less than (1, 'a'))",
		//			dbs.ErrRangeNotIncreasing,
		//		},
		{
			"create causet t (a int, b varchar(64)) partition by range defCausumns ( b) (" +
				"partition p0 values less than (  maxvalue)," +
				"partition p1 values less than ('a'))",
			dbs.ErrRangeNotIncreasing,
		},
		{
			"create causet t (defCaus datetime not null default '2000-01-01')" +
				"partition by range defCausumns (defCaus) (" +
				"PARTITION p0 VALUES LESS THAN (20190905)," +
				"PARTITION p1 VALUES LESS THAN (20190906));",
			dbs.ErrWrongTypeDeferredCausetValue,
		},
	}
	for i, t := range cases {
		_, err := tk.InterDirc(t.allegrosql)
		c.Assert(t.err.Equal(err), IsTrue, Commentf(
			"case %d fail, allegrosql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
			i, t.allegrosql, t.err, err,
		))
	}

	tk.MustInterDirc("create causet t1 (a int, b char(3)) partition by range defCausumns (a, b) (" +
		"partition p0 values less than (1, 'a')," +
		"partition p1 values less than (2, maxvalue))")

	tk.MustInterDirc("create causet t2 (a int, b char(3)) partition by range defCausumns (b) (" +
		"partition p0 values less than ( 'a')," +
		"partition p1 values less than (maxvalue))")
}

func (s *testIntegrationSuite3) TestCreateBlockWithKeyPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists tm1;")
	tk.MustInterDirc(`create causet tm1
	(
		s1 char(32) primary key
	)
	partition by key(s1) partitions 10;`)

	tk.MustInterDirc(`drop causet if exists tm2`)
	tk.MustInterDirc(`create causet tm2 (a char(5), unique key(a(5))) partition by key() partitions 5;`)
}

func (s *testIntegrationSuite5) TestAlterBlockAddPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists employees;")
	tk.MustInterDirc(`create causet employees (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	tk.MustInterDirc(`alter causet employees add partition (
    partition p4 values less than (2010),
    partition p5 values less than MAXVALUE
	);`)

	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().Partition, NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)

	c.Assert(part.Expr, Equals, "YEAR(`hired`)")
	c.Assert(part.Definitions, HasLen, 5)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, perceptron.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, perceptron.NewCIStr("p2"))
	c.Assert(part.Definitions[2].LessThan[0], Equals, "2001")
	c.Assert(part.Definitions[2].Name, Equals, perceptron.NewCIStr("p3"))
	c.Assert(part.Definitions[3].LessThan[0], Equals, "2010")
	c.Assert(part.Definitions[3].Name, Equals, perceptron.NewCIStr("p4"))
	c.Assert(part.Definitions[4].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[4].Name, Equals, perceptron.NewCIStr("p5"))

	tk.MustInterDirc("drop causet if exists block1;")
	tk.MustInterDirc("create causet block1(a int)")
	sql1 := `alter causet block1 add partition (
		partition p1 values less than (2010),
		partition p2 values less than maxvalue
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)
	tk.MustInterDirc(`create causet block_MustBeDefined (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	sql2 := "alter causet block_MustBeDefined add partition"
	tk.MustGetErrCode(sql2, tmysql.ErrPartitionsMustBeDefined)
	tk.MustInterDirc("drop causet if exists block2;")
	tk.MustInterDirc(`create causet block2 (

	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than maxvalue
	);`)

	sql3 := `alter causet block2 add partition (
		partition p3 values less than (2010)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrPartitionMaxvalue)

	tk.MustInterDirc("drop causet if exists block3;")
	tk.MustInterDirc(`create causet block3 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p2 values less than (2001)
	);`)

	sql4 := `alter causet block3 add partition (
		partition p3 values less than (1993)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrRangeNotIncreasing)

	sql5 := `alter causet block3 add partition (
		partition p1 values less than (1993)
	);`
	tk.MustGetErrCode(sql5, tmysql.ErrSameNamePartition)

	sql6 := `alter causet block3 add partition (
		partition p1 values less than (1993),
		partition p1 values less than (1995)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrSameNamePartition)

	sql7 := `alter causet block3 add partition (
		partition p4 values less than (1993),
		partition p1 values less than (1995),
		partition p5 values less than maxvalue
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrSameNamePartition)

	sql8 := "alter causet block3 add partition (partition p6);"
	tk.MustGetErrCode(sql8, tmysql.ErrPartitionRequiresValues)

	sql9 := "alter causet block3 add partition (partition p7 values in (2020));"
	tk.MustGetErrCode(sql9, tmysql.ErrPartitionWrongValues)

	sql10 := "alter causet block3 add partition partitions 4;"
	tk.MustGetErrCode(sql10, tmysql.ErrPartitionsMustBeDefined)

	tk.MustInterDirc("alter causet block3 add partition (partition p3 values less than (2001 + 10))")

	// less than value can be negative or memex.
	tk.MustInterDirc(`CREATE TABLE tt5 (
		c3 bigint(20) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	PARTITION BY RANGE ( c3 ) (
		PARTITION p0 VALUES LESS THAN (-3),
		PARTITION p1 VALUES LESS THAN (-2)
	);`)
	tk.MustInterDirc(`ALTER TABLE tt5 add partition ( partition p2 values less than (-1) );`)
	tk.MustInterDirc(`ALTER TABLE tt5 add partition ( partition p3 values less than (5-1) );`)

	// Test add partition for the causet partition by range defCausumns.
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc("create causet t (a datetime) partition by range defCausumns (a) (partition p1 values less than ('2020-06-01'), partition p2 values less than ('2020-07-01'));")
	allegrosql := "alter causet t add partition ( partition p3 values less than ('2020-07-01'));"
	tk.MustGetErrCode(allegrosql, tmysql.ErrRangeNotIncreasing)
	tk.MustInterDirc("alter causet t add partition ( partition p3 values less than ('2020-08-01'));")

	// Add partition value's type should be the same with the defCausumn's type.
	tk.MustInterDirc("drop causet if exists t;")
	tk.MustInterDirc(`create causet t (
		defCaus date not null default '2000-01-01')
                partition by range defCausumns (defCaus) (
		PARTITION p0 VALUES LESS THAN ('20190905'),
		PARTITION p1 VALUES LESS THAN ('20190906'));`)
	allegrosql = "alter causet t add partition (partition p2 values less than (20190907));"
	tk.MustGetErrCode(allegrosql, tmysql.ErrWrongTypeDeferredCausetValue)
}

func (s *testIntegrationSuite5) TestAlterBlockDropPartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists employees")
	tk.MustInterDirc(`create causet employees (
	id int not null,
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)

	tk.MustInterDirc("alter causet employees drop partition p3;")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("employees"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part := tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`hired`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "1991")
	c.Assert(part.Definitions[0].Name, Equals, perceptron.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "1996")
	c.Assert(part.Definitions[1].Name, Equals, perceptron.NewCIStr("p2"))

	tk.MustInterDirc("drop causet if exists block1;")
	tk.MustInterDirc("create causet block1 (a int);")
	sql1 := "alter causet block1 drop partition p10;"
	tk.MustGetErrCode(sql1, tmysql.ErrPartitionMgmtOnNonpartitioned)

	tk.MustInterDirc("drop causet if exists block2;")
	tk.MustInterDirc(`create causet block2 (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001)
	);`)
	sql2 := "alter causet block2 drop partition p10;"
	tk.MustGetErrCode(sql2, tmysql.ErrDropPartitionNonExistent)

	tk.MustInterDirc("drop causet if exists block3;")
	tk.MustInterDirc(`create causet block3 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (1991)
	);`)
	sql3 := "alter causet block3 drop partition p1;"
	tk.MustGetErrCode(sql3, tmysql.ErrDropLastPartition)

	tk.MustInterDirc("drop causet if exists block4;")
	tk.MustInterDirc(`create causet block4 (
	id int not null
	)
	partition by range( id ) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than MAXVALUE
	);`)

	tk.MustInterDirc("alter causet block4 drop partition p2;")
	is = petri.GetPetri(ctx).SchemaReplicant()
	tbl, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("block4"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().GetPartitionInfo(), NotNil)
	part = tbl.Meta().Partition
	c.Assert(part.Type, Equals, perceptron.PartitionTypeRange)
	c.Assert(part.Expr, Equals, "`id`")
	c.Assert(part.Definitions, HasLen, 2)
	c.Assert(part.Definitions[0].LessThan[0], Equals, "10")
	c.Assert(part.Definitions[0].Name, Equals, perceptron.NewCIStr("p1"))
	c.Assert(part.Definitions[1].LessThan[0], Equals, "MAXVALUE")
	c.Assert(part.Definitions[1].Name, Equals, perceptron.NewCIStr("p3"))

	tk.MustInterDirc("drop causet if exists tr;")
	tk.MustInterDirc(` create causet tr(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	tk.MustInterDirc(`INSERT INTO tr VALUES
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2020-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result := tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows(`2 alarm clock 1997-11-05`, `10 lava lamp 1998-12-25`))
	tk.MustInterDirc("alter causet tr drop partition p2;")
	result = tk.MustQuery("select * from tr where purchased between '1995-01-01' and '1999-12-31';")
	result.Check(testkit.Rows())

	result = tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2020-12-31';")
	result.Check(testkit.Rows(`5 exercise bike 2020-05-09`, `7 espresso maker 2011-11-22`))
	tk.MustInterDirc("alter causet tr drop partition p5;")
	result = tk.MustQuery("select * from tr where purchased between '2010-01-01' and '2020-12-31';")
	result.Check(testkit.Rows())

	tk.MustInterDirc("alter causet tr drop partition p4;")
	result = tk.MustQuery("select * from tr where purchased between '2005-01-01' and '2009-12-31';")
	result.Check(testkit.Rows())

	tk.MustInterDirc("drop causet if exists block4;")
	tk.MustInterDirc(`create causet block4 (
		id int not null
	)
	partition by range( id ) (
		partition Par1 values less than (1991),
		partition pAR2 values less than (1992),
		partition Par3 values less than (1995),
		partition PaR5 values less than (1996)
	);`)
	tk.MustInterDirc("alter causet block4 drop partition Par2;")
	tk.MustInterDirc("alter causet block4 drop partition PAR5;")
	sql4 := "alter causet block4 drop partition PAR0;"
	tk.MustGetErrCode(sql4, tmysql.ErrDropPartitionNonExistent)

	tk.MustInterDirc("CREATE TABLE t1 (a int(11), b varchar(64)) PARTITION BY HASH(a) PARTITIONS 3")
	tk.MustGetErrCode("alter causet t1 drop partition p2", tmysql.ErrOnlyOnRangeListPartition)
}

func (s *testIntegrationSuite5) TestMultiPartitionDropAndTruncate(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists employees")
	tk.MustInterDirc(`create causet employees (
	hired int not null
	)
	partition by range( hired ) (
		partition p1 values less than (1991),
		partition p2 values less than (1996),
		partition p3 values less than (2001),
		partition p4 values less than (2006),
		partition p5 values less than (2011)
	);`)
	tk.MustInterDirc(`INSERT INTO employees VALUES (1990), (1995), (2000), (2005), (2010)`)

	tk.MustInterDirc("alter causet employees drop partition p1, p2;")
	result := tk.MustQuery("select * from employees;")
	result.Sort().Check(testkit.Rows(`2000`, `2005`, `2010`))

	tk.MustInterDirc("alter causet employees truncate partition p3, p4")
	result = tk.MustQuery("select * from employees;")
	result.Check(testkit.Rows(`2010`))
}

func (s *testIntegrationSuite7) TestAlterBlockExchangePartition(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists e")
	tk.MustInterDirc("drop causet if exists e2")
	tk.MustInterDirc(`CREATE TABLE e (
		id INT NOT NULL
	)
    PARTITION BY RANGE (id) (
        PARTITION p0 VALUES LESS THAN (50),
        PARTITION p1 VALUES LESS THAN (100),
        PARTITION p2 VALUES LESS THAN (150),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
	);`)
	tk.MustInterDirc(`CREATE TABLE e2 (
		id INT NOT NULL
	);`)
	tk.MustInterDirc(`INSERT INTO e VALUES (1669),(337),(16),(2005)`)
	tk.MustInterDirc("ALTER TABLE e EXCHANGE PARTITION p0 WITH TABLE e2")
	tk.MustQuery("select * from e2").Check(testkit.Rows("16"))
	tk.MustQuery("select * from e").Check(testkit.Rows("1669", "337", "2005"))
	// validation test for range partition
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p2 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p3 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)

	tk.MustInterDirc("drop causet if exists e3")

	tk.MustInterDirc(`CREATE TABLE e3 (
		id int not null
	) PARTITION BY HASH (id)
	PARTITIONS 4;`)
	tk.MustGetErrCode("ALTER TABLE e EXCHANGE PARTITION p1 WITH TABLE e3;", tmysql.ErrPartitionExchangePartBlock)
	tk.MustInterDirc("truncate causet e2")
	tk.MustInterDirc(`INSERT INTO e3 VALUES (1),(5)`)

	tk.MustInterDirc("ALTER TABLE e3 EXCHANGE PARTITION p1 WITH TABLE e2;")
	tk.MustQuery("select * from e3 partition(p0)").Check(testkit.Rows())
	tk.MustQuery("select * from e2").Check(testkit.Rows("1", "5"))

	// validation test for hash partition
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p0 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p2 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e3 EXCHANGE PARTITION p3 WITH TABLE e2", tmysql.ErrRowDoesNotMatchPartition)

	// without validation test
	tk.MustInterDirc("ALTER TABLE e3 EXCHANGE PARTITION p0 with TABLE e2 WITHOUT VALIDATION")

	tk.MustQuery("select * from e3 partition(p0)").Check(testkit.Rows("1", "5"))
	tk.MustQuery("select * from e2").Check(testkit.Rows())

	// more boundary test of range partition
	// for partition p0
	tk.MustInterDirc(`create causet e4 (a int) partition by range(a) (
		partition p0 values less than (3),
		partition p1 values less than (6),
        PARTITION p2 VALUES LESS THAN (9),
        PARTITION p3 VALUES LESS THAN (MAXVALUE)
		);`)
	tk.MustInterDirc(`create causet e5(a int);`)

	tk.MustInterDirc("insert into e5 values (1)")

	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustInterDirc("ALTER TABLE e4 EXCHANGE PARTITION p0 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p0)").Check(testkit.Rows("1"))

	// for partition p1
	tk.MustInterDirc("insert into e5 values (3)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p2 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustInterDirc("ALTER TABLE e4 EXCHANGE PARTITION p1 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p1)").Check(testkit.Rows("3"))

	// for partition p2
	tk.MustInterDirc("insert into e5 values (6)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p3 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustInterDirc("ALTER TABLE e4 EXCHANGE PARTITION p2 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p2)").Check(testkit.Rows("6"))

	// for partition p3
	tk.MustInterDirc("insert into e5 values (9)")
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p0 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("ALTER TABLE e4 EXCHANGE PARTITION p1 WITH TABLE e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustGetErrCode("alter causet e4 exchange partition p2 with causet e5", tmysql.ErrRowDoesNotMatchPartition)
	tk.MustInterDirc("ALTER TABLE e4 EXCHANGE PARTITION p3 with TABLE e5")
	tk.MustQuery("select * from e4 partition(p3)").Check(testkit.Rows("9"))

	// for defCausumns range partition
	tk.MustInterDirc(`create causet e6 (a varchar(3)) partition by range defCausumns (a) (
		partition p0 values less than ('3'),
		partition p1 values less than ('6')
	);`)
	tk.MustInterDirc(`create causet e7 (a varchar(3));`)
	tk.MustInterDirc(`insert into e6 values ('1');`)
	tk.MustInterDirc(`insert into e7 values ('2');`)
	tk.MustInterDirc("alter causet e6 exchange partition p0 with causet e7")

	tk.MustQuery("select * from e6 partition(p0)").Check(testkit.Rows("2"))
	tk.MustQuery("select * from e7").Check(testkit.Rows("1"))
	tk.MustGetErrCode("alter causet e6 exchange partition p1 with causet e7", tmysql.ErrRowDoesNotMatchPartition)

	// test exchange partition from different databases
	tk.MustInterDirc("create causet e8 (a int) partition by hash(a) partitions 2;")
	tk.MustInterDirc("create database if not exists exchange_partition")
	tk.MustInterDirc("insert into e8 values (1), (3), (5)")
	tk.MustInterDirc("use exchange_partition;")
	tk.MustInterDirc("create causet e9 (a int);")
	tk.MustInterDirc("insert into e9 values (7), (9)")
	tk.MustInterDirc("alter causet test.e8 exchange partition p1 with causet e9")

	tk.MustInterDirc("insert into e9 values (11)")
	tk.MustQuery("select * from e9").Check(testkit.Rows("1", "3", "5", "11"))
	tk.MustInterDirc("insert into test.e8 values (11)")
	tk.MustQuery("select * from test.e8").Check(testkit.Rows("7", "9", "11"))

	tk.MustInterDirc("use test")
	tk.MustInterDirc("create causet e10 (a int) partition by hash(a) partitions 2")
	tk.MustInterDirc("insert into e10 values (0), (2), (4)")
	tk.MustInterDirc("create causet e11 (a int)")
	tk.MustInterDirc("insert into e11 values (1), (3)")
	tk.MustInterDirc("alter causet e10 exchange partition p1 with causet e11")
	tk.MustInterDirc("insert into e11 values (5)")
	tk.MustQuery("select * from e11").Check(testkit.Rows("5"))
	tk.MustInterDirc("insert into e10 values (5), (6)")
	tk.MustQuery("select * from e10 partition(p0)").Check(testkit.Rows("0", "2", "4", "6"))
	tk.MustQuery("select * from e10 partition(p1)").Check(testkit.Rows("1", "3", "5"))

	// test for defCausumn id
	tk.MustInterDirc("create causet e12 (a int(1), b int, index (a)) partition by hash(a) partitions 3")
	tk.MustInterDirc("create causet e13 (a int(8), b int, index (a));")
	tk.MustInterDirc("alter causet e13 drop defCausumn b")
	tk.MustInterDirc("alter causet e13 add defCausumn b int")
	tk.MustGetErrCode("alter causet e12 exchange partition p0 with causet e13", tmysql.ErrPartitionExchangeDifferentOption)
	// test for index id
	tk.MustInterDirc("create causet e14 (a int, b int, index(a));")
	tk.MustInterDirc("alter causet e12 drop index a")
	tk.MustInterDirc("alter causet e12 add index (a);")
	tk.MustGetErrCode("alter causet e12 exchange partition p0 with causet e14", tmysql.ErrPartitionExchangeDifferentOption)

	// test for tiflash replica
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")

	tk.MustInterDirc("create causet e15 (a int) partition by hash(a) partitions 1;")
	tk.MustInterDirc("create causet e16 (a int)")
	tk.MustInterDirc("alter causet e15 set tiflash replica 1;")
	tk.MustInterDirc("alter causet e16 set tiflash replica 2;")

	e15 := testGetBlockByName(c, s.ctx, "test", "e15")
	partition := e15.Meta().Partition

	err := petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 := testGetBlockByName(c, s.ctx, "test", "e16")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustGetErrCode("alter causet e15 exchange partition p0 with causet e16", tmysql.ErrBlocksDifferentMetadata)
	tk.MustInterDirc("drop causet e15, e16")

	tk.MustInterDirc("create causet e15 (a int) partition by hash(a) partitions 1;")
	tk.MustInterDirc("create causet e16 (a int)")
	tk.MustInterDirc("alter causet e15 set tiflash replica 1;")
	tk.MustInterDirc("alter causet e16 set tiflash replica 1;")

	e15 = testGetBlockByName(c, s.ctx, "test", "e15")
	partition = e15.Meta().Partition

	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 = testGetBlockByName(c, s.ctx, "test", "e16")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustInterDirc("alter causet e15 exchange partition p0 with causet e16")

	e15 = testGetBlockByName(c, s.ctx, "test", "e15")

	partition = e15.Meta().Partition

	c.Assert(e15.Meta().TiFlashReplica, NotNil)
	c.Assert(e15.Meta().TiFlashReplica.Available, IsTrue)
	c.Assert(e15.Meta().TiFlashReplica.AvailablePartitionIDs, DeepEquals, []int64{partition.Definitions[0].ID})

	e16 = testGetBlockByName(c, s.ctx, "test", "e16")
	c.Assert(e16.Meta().TiFlashReplica, NotNil)
	c.Assert(e16.Meta().TiFlashReplica.Available, IsTrue)

	tk.MustInterDirc("drop causet e15, e16")
	tk.MustInterDirc("create causet e15 (a int) partition by hash(a) partitions 1;")
	tk.MustInterDirc("create causet e16 (a int)")
	tk.MustInterDirc("alter causet e16 set tiflash replica 1;")

	tk.MustInterDirc("alter causet e15 set tiflash replica 1 location labels 'a', 'b';")

	tk.MustGetErrCode("alter causet e15 exchange partition p0 with causet e16", tmysql.ErrBlocksDifferentMetadata)

	tk.MustInterDirc("alter causet e16 set tiflash replica 1 location labels 'a', 'b';")

	e15 = testGetBlockByName(c, s.ctx, "test", "e15")
	partition = e15.Meta().Partition

	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, partition.Definitions[0].ID, true)
	c.Assert(err, IsNil)

	e16 = testGetBlockByName(c, s.ctx, "test", "e16")
	err = petri.GetPetri(tk.Se).DBS().UFIDelateBlockReplicaInfo(tk.Se, e16.Meta().ID, true)
	c.Assert(err, IsNil)

	tk.MustInterDirc("alter causet e15 exchange partition p0 with causet e16")
}

func (s *testIntegrationSuite4) TestExchangePartitionBlockCompatiable(c *C) {
	type testCase struct {
		ptALLEGROSQL       string
		ntALLEGROSQL       string
		exchangeALLEGROSQL string
		err         *terror.Error
	}
	cases := []testCase{
		{
			"create causet pt (id int not null) partition by hash (id) partitions 4;",
			"create causet nt (id int(1) not null);",
			"alter causet pt exchange partition p0 with causet nt;",
			nil,
		},
		{
			"create causet pt1 (id int not null, fname varchar(3)) partition by hash (id) partitions 4;",
			"create causet nt1 (id int not null, fname varchar(4));",
			"alter causet pt1 exchange partition p0 with causet nt1;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt2 (id int not null, salary decimal) partition by hash(id) partitions 4;",
			"create causet nt2 (id int not null, salary decimal(3,2));",
			"alter causet pt2 exchange partition p0 with causet nt2;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt3 (id int not null, salary decimal) partition by hash(id) partitions 1;",
			"create causet nt3 (id int not null, salary decimal(10, 1));",
			"alter causet pt3 exchange partition p0 with causet nt3",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt4 (id int not null) partition by hash(id) partitions 1;",
			"create causet nt4 (id1 int not null);",
			"alter causet pt4 exchange partition p0 with causet nt4;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt5 (id int not null, primary key (id)) partition by hash(id) partitions 1;",
			"create causet nt5 (id int not null);",
			"alter causet pt5 exchange partition p0 with causet nt5;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt6 (id int not null, salary decimal, index idx (id, salary)) partition by hash(id) partitions 1;",
			"create causet nt6 (id int not null, salary decimal, index idx (salary, id));",
			"alter causet pt6 exchange partition p0 with causet nt6;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt7 (id int not null, index idx (id) invisible) partition by hash(id) partitions 1;",
			"create causet nt7 (id int not null, index idx (id));",
			"alter causet pt7 exchange partition p0 with causet nt7;",
			nil,
		},
		{
			"create causet pt8 (id int not null, index idx (id)) partition by hash(id) partitions 1;",
			"create causet nt8 (id int not null, index id_idx (id));",
			"alter causet pt8 exchange partition p0 with causet nt8;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// foreign key test
			// Partition causet doesn't support to add foreign keys in allegrosql
			"create causet pt9 (id int not null primary key auto_increment,t_id int not null) partition by hash(id) partitions 1;",
			"create causet nt9 (id int not null primary key auto_increment, t_id int not null,foreign key fk_id (t_id) references pt5(id));",
			"alter causet pt9 exchange partition p0 with causet nt9;",
			dbs.ErrPartitionExchangeForeignKey,
		},
		{
			// Generated defCausumn (virtual)
			"create causet pt10 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) virtual) partition by hash(id) partitions 1;",
			"create causet nt10 (id int not null, lname varchar(30), fname varchar(100));",
			"alter causet pt10 exchange partition p0 with causet nt10;",
			dbs.ErrUnsupportedOnGeneratedDeferredCauset,
		},
		{
			"create causet pt11 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create causet nt11 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter causet pt11 exchange partition p0 with causet nt11;",
			dbs.ErrUnsupportedOnGeneratedDeferredCauset,
		},
		{

			"create causet pt12 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname,' ')) stored) partition by hash(id) partitions 1;",
			"create causet nt12 (id int not null, lname varchar(30), fname varchar(100));",
			"alter causet pt12 exchange partition p0 with causet nt12;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt13 (id int not null, lname varchar(30), fname varchar(100)) partition by hash(id) partitions 1;",
			"create causet nt13 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored);",
			"alter causet pt13 exchange partition p0 with causet nt13;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt14 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual) partition by hash(id) partitions 1;",
			"create causet nt14 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter causet pt14 exchange partition p0 with causet nt14;",
			nil,
		},
		{
			// unique index
			"create causet pt15 (id int not null, unique index uk_id (id)) partition by hash(id) partitions 1;",
			"create causet nt15 (id int not null, index uk_id (id));",
			"alter causet pt15 exchange partition p0 with causet nt15",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// auto_increment
			"create causet pt16 (id int not null primary key auto_increment) partition by hash(id) partitions 1;",
			"create causet nt16 (id int not null primary key);",
			"alter causet pt16 exchange partition p0 with causet nt16;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// default
			"create causet pt17 (id int not null default 1) partition by hash(id) partitions 1;",
			"create causet nt17 (id int not null);",
			"alter causet pt17 exchange partition p0 with causet nt17;",
			nil,
		},
		{
			// view test
			"create causet pt18 (id int not null) partition by hash(id) partitions 1;",
			"create view nt18 as select id from nt17;",
			"alter causet pt18 exchange partition p0 with causet nt18",
			dbs.ErrCheckNoSuchBlock,
		},
		{
			"create causet pt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) stored) partition by hash(id) partitions 1;",
			"create causet nt19 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual);",
			"alter causet pt19 exchange partition p0 with causet nt19;",
			dbs.ErrUnsupportedOnGeneratedDeferredCauset,
		},
		{
			"create causet pt20 (id int not null) partition by hash(id) partitions 1;",
			"create causet nt20 (id int default null);",
			"alter causet pt20 exchange partition p0 with causet nt20;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// unsigned
			"create causet pt21 (id int unsigned) partition by hash(id) partitions 1;",
			"create causet nt21 (id int);",
			"alter causet pt21 exchange partition p0 with causet nt21;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			// zerofill
			"create causet pt22 (id int) partition by hash(id) partitions 1;",
			"create causet nt22 (id int zerofill);",
			"alter causet pt22 exchange partition p0 with causet nt22;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt23 (id int, lname varchar(10) charset binary) partition by hash(id) partitions 1;",
			"create causet nt23 (id int, lname varchar(10));",
			"alter causet pt23 exchange partition p0 with causet nt23;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt25 (id int, a datetime on uFIDelate current_timestamp) partition by hash(id) partitions 1;",
			"create causet nt25 (id int, a datetime);",
			"alter causet pt25 exchange partition p0 with causet nt25;",
			nil,
		},
		{
			"create causet pt26 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(lname, ' ')) virtual) partition by hash(id) partitions 1;",
			"create causet nt26 (id int not null, lname varchar(30), fname varchar(100) generated always as (concat(id, ' ')) virtual);",
			"alter causet pt26 exchange partition p0 with causet nt26;",
			dbs.ErrBlocksDifferentMetadata,
		},
		{
			"create causet pt27 (a int key, b int, index(a)) partition by hash(a) partitions 1;",
			"create causet nt27 (a int not null, b int, index(a));",
			"alter causet pt27 exchange partition p0 with causet nt27;",
			dbs.ErrBlocksDifferentMetadata,
		},
	}

	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	for i, t := range cases {
		tk.MustInterDirc(t.ptALLEGROSQL)
		tk.MustInterDirc(t.ntALLEGROSQL)
		if t.err != nil {
			_, err := tk.InterDirc(t.exchangeALLEGROSQL)
			c.Assert(terror.ErrorEqual(err, t.err), IsTrue, Commentf(
				"case %d fail, allegrosql = `%s`\nexpected error = `%v`\n  actual error = `%v`",
				i, t.exchangeALLEGROSQL, t.err, err,
			))
		} else {
			tk.MustInterDirc(t.exchangeALLEGROSQL)
		}
	}
}

func (s *testIntegrationSuite7) TestExchangePartitionExpressIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists pt1;")
	tk.MustInterDirc("create causet pt1(a int, b int, c int) PARTITION BY hash (a) partitions 1;")
	tk.MustInterDirc("alter causet pt1 add index idx((a+c));")

	tk.MustInterDirc("drop causet if exists nt1;")
	tk.MustInterDirc("create causet nt1(a int, b int, c int);")
	tk.MustGetErrCode("alter causet pt1 exchange partition p0 with causet nt1;", tmysql.ErrBlocksDifferentMetadata)

	tk.MustInterDirc("alter causet nt1 add defCausumn (`_V$_idx_0` bigint(20) generated always as (a+b) virtual);")
	tk.MustGetErrCode("alter causet pt1 exchange partition p0 with causet nt1;", tmysql.ErrBlocksDifferentMetadata)

	// test different memex index when memex returns same field type
	tk.MustInterDirc("alter causet nt1 drop defCausumn `_V$_idx_0`;")
	tk.MustInterDirc("alter causet nt1 add index idx((b-c));")
	tk.MustGetErrCode("alter causet pt1 exchange partition p0 with causet nt1;", tmysql.ErrBlocksDifferentMetadata)

	// test different memex index when memex returns different field type
	tk.MustInterDirc("alter causet nt1 drop index idx;")
	tk.MustInterDirc("alter causet nt1 add index idx((concat(a, b)));")
	tk.MustGetErrCode("alter causet pt1 exchange partition p0 with causet nt1;", tmysql.ErrBlocksDifferentMetadata)

	tk.MustInterDirc("drop causet if exists nt2;")
	tk.MustInterDirc("create causet nt2 (a int, b int, c int)")
	tk.MustInterDirc("alter causet nt2 add index idx((a+c))")
	tk.MustInterDirc("alter causet pt1 exchange partition p0 with causet nt2")

}

func (s *testIntegrationSuite4) TestAddPartitionTooManyPartitions(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	count := dbs.PartitionCountLimit
	tk.MustInterDirc("drop causet if exists p1;")
	sql1 := `create causet p1 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i <= count; i++ {
		sql1 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql1 += "partition p8193 values less than (8193) );"
	tk.MustGetErrCode(sql1, tmysql.ErrTooManyPartitions)

	tk.MustInterDirc("drop causet if exists p2;")
	sql2 := `create causet p2 (
		id int not null
	)
	partition by range( id ) (`
	for i := 1; i < count; i++ {
		sql2 += fmt.Sprintf("partition p%d values less than (%d),", i, i)
	}
	sql2 += "partition p8192 values less than (8192) );"

	tk.MustInterDirc(sql2)
	sql3 := `alter causet p2 add partition (
	partition p8193 values less than (8193)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrTooManyPartitions)
}

func checkPartitionDelRangeDone(c *C, s *testIntegrationSuite, partitionPrefix ekv.Key) bool {
	hasOldPartitionData := true
	for i := 0; i < waitForCleanDataRound; i++ {
		err := ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
			it, err := txn.Iter(partitionPrefix, nil)
			if err != nil {
				return err
			}
			if !it.Valid() {
				hasOldPartitionData = false
			} else {
				hasOldPartitionData = it.Key().HasPrefix(partitionPrefix)
			}
			it.Close()
			return nil
		})
		c.Assert(err, IsNil)
		if !hasOldPartitionData {
			break
		}
		time.Sleep(waitForCleanDataInterval)
	}
	return hasOldPartitionData
}

func (s *testIntegrationSuite4) TestTruncatePartitionAndDropBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	// Test truncate common causet.
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc("create causet t1 (id int(11));")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into t1 values (?)", i)
	}
	result := tk.MustQuery("select count(*) from t1;")
	result.Check(testkit.Rows("100"))
	tk.MustInterDirc("truncate causet t1;")
	result = tk.MustQuery("select count(*) from t1")
	result.Check(testkit.Rows("0"))

	// Test drop common causet.
	tk.MustInterDirc("drop causet if exists t2;")
	tk.MustInterDirc("create causet t2 (id int(11));")
	for i := 0; i < 100; i++ {
		tk.MustInterDirc("insert into t2 values (?)", i)
	}
	result = tk.MustQuery("select count(*) from t2;")
	result.Check(testkit.Rows("100"))
	tk.MustInterDirc("drop causet t2;")
	tk.MustGetErrCode("select * from t2;", tmysql.ErrNoSuchBlock)

	// Test truncate causet partition.
	tk.MustInterDirc("drop causet if exists t3;")
	tk.MustInterDirc(`create causet t3(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	tk.MustInterDirc(`insert into t3 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2020-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = tk.MustQuery("select count(*) from t3;")
	result.Check(testkit.Rows("10"))
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t3"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID := oldTblInfo.Meta().Partition.Definitions[0].ID
	tk.MustInterDirc("truncate causet t3;")
	partitionPrefix := blockcodec.EncodeBlockPrefix(oldPID)
	hasOldPartitionData := checkPartitionDelRangeDone(c, s.testIntegrationSuite, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)

	// Test drop causet partition.
	tk.MustInterDirc("drop causet if exists t4;")
	tk.MustInterDirc(`create causet t4(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	tk.MustInterDirc(`insert into t4 values
	(1, 'desk organiser', '2003-10-15'),
	(2, 'alarm clock', '1997-11-05'),
	(3, 'chair', '2009-03-10'),
	(4, 'bookcase', '1989-01-10'),
	(5, 'exercise bike', '2020-05-09'),
	(6, 'sofa', '1987-06-05'),
	(7, 'espresso maker', '2011-11-22'),
	(8, 'aquarium', '1992-08-04'),
	(9, 'study desk', '2006-09-16'),
	(10, 'lava lamp', '1998-12-25');`)
	result = tk.MustQuery("select count(*) from t4; ")
	result.Check(testkit.Rows("10"))
	is = petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t4"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	oldPID = oldTblInfo.Meta().Partition.Definitions[1].ID
	tk.MustInterDirc("drop causet t4;")
	partitionPrefix = blockcodec.EncodeBlockPrefix(oldPID)
	hasOldPartitionData = checkPartitionDelRangeDone(c, s.testIntegrationSuite, partitionPrefix)
	c.Assert(hasOldPartitionData, IsFalse)
	tk.MustGetErrCode("select * from t4;", tmysql.ErrNoSuchBlock)

	// Test truncate causet partition reassigns new partitionIDs.
	tk.MustInterDirc("drop causet if exists t5;")
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition=1;")
	tk.MustInterDirc(`create causet t5(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2020)
   	);`)
	is = petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t5"))
	c.Assert(err, IsNil)
	oldPID = oldTblInfo.Meta().Partition.Definitions[0].ID

	tk.MustInterDirc("truncate causet t5;")
	is = petri.GetPetri(ctx).SchemaReplicant()
	newTblInfo, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("t5"))
	c.Assert(err, IsNil)
	newPID := newTblInfo.Meta().Partition.Definitions[0].ID
	c.Assert(oldPID != newPID, IsTrue)

	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = 1;")
	tk.MustInterDirc("drop causet if exists clients;")
	tk.MustInterDirc(`create causet clients (
		id int,
		fname varchar(30),
		lname varchar(30),
		signed date
	)
	partition by hash( month(signed) )
	partitions 12;`)
	is = petri.GetPetri(ctx).SchemaReplicant()
	oldTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("clients"))
	c.Assert(err, IsNil)
	oldDefs := oldTblInfo.Meta().Partition.Definitions

	// Test truncate `hash partitioned causet` reassigns new partitionIDs.
	tk.MustInterDirc("truncate causet clients;")
	is = petri.GetPetri(ctx).SchemaReplicant()
	newTblInfo, err = is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("clients"))
	c.Assert(err, IsNil)
	newDefs := newTblInfo.Meta().Partition.Definitions
	for i := 0; i < len(oldDefs); i++ {
		c.Assert(oldDefs[i].ID != newDefs[i].ID, IsTrue)
	}
}

func (s *testIntegrationSuite5) TestPartitionUniqueKeyNeedAllFieldsInPf(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists part1;")
	tk.MustInterDirc(`create causet part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustInterDirc("drop causet if exists part2;")
	tk.MustInterDirc(`create causet part2 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2, defCaus3),
		unique key (defCaus3)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustInterDirc("drop causet if exists part3;")
	tk.MustInterDirc(`create causet part3 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2)
	)
	partition by range( defCaus1 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustInterDirc("drop causet if exists part4;")
	tk.MustInterDirc(`create causet part4 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2),
		unique key(defCaus2)
	)
	partition by range( year(defCaus2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustInterDirc("drop causet if exists part5;")
	tk.MustInterDirc(`create causet part5 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2, defCaus4),
		unique key(defCaus2, defCaus1)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`)

	tk.MustInterDirc("drop causet if exists Part1;")
	sql1 := `create causet Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql1, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustInterDirc("drop causet if exists Part1;")
	sql2 := `create causet Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1),
		unique key (defCaus3)
	)
	partition by range( defCaus1 + defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql2, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustInterDirc("drop causet if exists Part1;")
	sql3 := `create causet Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1),
		unique key (defCaus3)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql3, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustInterDirc("drop causet if exists Part1;")
	sql4 := `create causet Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		unique key (defCaus1, defCaus2, defCaus3),
		unique key (defCaus3)
	)
	partition by range( defCaus1 + defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql4, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustInterDirc("drop causet if exists Part1;")
	sql5 := `create causet Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus2)
	)
	partition by range( defCaus3 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql5, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustInterDirc("drop causet if exists Part1;")
	sql6 := `create causet Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus3),
		unique key(defCaus2)
	)
	partition by range( year(defCaus2)  ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql6, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustInterDirc("drop causet if exists Part1;")
	sql7 := `create causet Part1 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		primary key(defCaus1, defCaus3, defCaus4),
		unique key(defCaus2, defCaus1)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql7, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	tk.MustInterDirc("drop causet if exists part6;")
	sql8 := `create causet part6 (
		defCaus1 int not null,
		defCaus2 date not null,
		defCaus3 int not null,
		defCaus4 int not null,
		defCaus5 int not null,
		unique key(defCaus1, defCaus2),
		unique key(defCaus1, defCaus3)
	)
	partition by range( defCaus1 + defCaus2 ) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	);`
	tk.MustGetErrCode(sql8, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql9 := `create causet part7 (
		defCaus1 int not null,
		defCaus2 int not null,
		defCaus3 int not null unique,
		unique key(defCaus1, defCaus2)
	)
	partition by range (defCaus1 + defCaus2) (
	partition p1 values less than (11),
	partition p2 values less than (15)
	)`
	tk.MustGetErrCode(sql9, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql10 := `create causet part8 (
                 a int not null,
                 b int not null,
                 c int default null,
                 d int default null,
                 e int default null,
                 primary key (a, b),
                 unique key (c, d)
        )
        partition by range defCausumns (b) (
               partition p0 values less than (4),
               partition p1 values less than (7),
               partition p2 values less than (11)
        )`
	tk.MustGetErrCode(sql10, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql11 := `create causet part9 (
                 a int not null,
                 b int not null,
                 c int default null,
                 d int default null,
                 e int default null,
                 primary key (a, b),
                 unique key (b, c, d)
        )
        partition by range defCausumns (b, c) (
               partition p0 values less than (4, 5),
               partition p1 values less than (7, 9),
               partition p2 values less than (11, 22)
        )`
	tk.MustGetErrCode(sql11, tmysql.ErrUniqueKeyNeedAllFieldsInPf)

	sql12 := `create causet part12 (a varchar(20), b binary, unique index (a(5))) partition by range defCausumns (a) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`
	tk.MustGetErrCode(sql12, tmysql.ErrUniqueKeyNeedAllFieldsInPf)
	tk.MustInterDirc(`create causet part12 (a varchar(20), b binary) partition by range defCausumns (a) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`)
	tk.MustGetErrCode("alter causet part12 add unique index (a(5))", tmysql.ErrUniqueKeyNeedAllFieldsInPf)
	sql13 := `create causet part13 (a varchar(20), b varchar(10), unique index (a(5),b)) partition by range defCausumns (b) (
			partition p0 values less than ('aaaaa'),
			partition p1 values less than ('bbbbb'),
			partition p2 values less than ('ccccc'))`
	tk.MustInterDirc(sql13)
}

func (s *testIntegrationSuite2) TestPartitionDropPrimaryKey(c *C) {
	idxName := "primary"
	addIdxALLEGROSQL := "alter causet partition_drop_idx add primary key idx1 (c1);"
	dropIdxALLEGROSQL := "alter causet partition_drop_idx drop primary key;"
	testPartitionDropIndex(c, s.causetstore, s.lease, idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL)
}

func (s *testIntegrationSuite3) TestPartitionDropIndex(c *C) {
	idxName := "idx1"
	addIdxALLEGROSQL := "alter causet partition_drop_idx add index idx1 (c1);"
	dropIdxALLEGROSQL := "alter causet partition_drop_idx drop index idx1;"
	testPartitionDropIndex(c, s.causetstore, s.lease, idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL)
}

func testPartitionDropIndex(c *C, causetstore ekv.CausetStorage, lease time.Duration, idxName, addIdxALLEGROSQL, dropIdxALLEGROSQL string) {
	tk := testkit.NewTestKit(c, causetstore)
	done := make(chan error, 1)
	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists partition_drop_idx;")
	tk.MustInterDirc(`create causet partition_drop_idx (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (3),
    	partition p1 values less than (5),
    	partition p2 values less than (7),
    	partition p3 values less than (11),
    	partition p4 values less than (15),
    	partition p5 values less than (20),
		partition p6 values less than (maxvalue)
   	);`)

	num := 20
	for i := 0; i < num; i++ {
		tk.MustInterDirc("insert into partition_drop_idx values (?, ?, ?)", i, i, i)
	}
	tk.MustInterDirc(addIdxALLEGROSQL)

	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	t, err := is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)

	var idx1 causet.Index
	for _, pidx := range t.Indices() {
		if pidx.Meta().Name.L == idxName {
			idx1 = pidx
			break
		}
	}
	c.Assert(idx1, NotNil)

	solitonutil.StochastikInterDircInGoroutine(c, causetstore, dropIdxALLEGROSQL, done)
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err = <-done:
			if err == nil {
				break LOOP
			}
			c.Assert(err, IsNil, Commentf("err:%v", errors.ErrorStack(err)))
		case <-ticker.C:
			step := 10
			rand.Seed(time.Now().Unix())
			for i := num; i < num+step; i++ {
				n := rand.Intn(num)
				tk.MustInterDirc("uFIDelate partition_drop_idx set c2 = 1 where c1 = ?", n)
				tk.MustInterDirc("insert into partition_drop_idx values (?, ?, ?)", i, i, i)
			}
			num += step
		}
	}

	is = petri.GetPetri(ctx).SchemaReplicant()
	t, err = is.BlockByName(perceptron.NewCIStr("test_db"), perceptron.NewCIStr("partition_drop_idx"))
	c.Assert(err, IsNil)
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	var idxn causet.Index
	t.Indices()
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == idxName {
			idxn = idx
			break
		}
	}
	c.Assert(idxn, IsNil)
	idx := blocks.NewIndex(pid, t.Meta(), idx1.Meta())
	checkDelRangeDone(c, ctx, idx)
	tk.MustInterDirc("drop causet partition_drop_idx;")
}

func (s *testIntegrationSuite2) TestPartitionCancelAddPrimaryKey(c *C) {
	idxName := "primary"
	addIdxALLEGROSQL := "alter causet t1 add primary key c3_index (c1);"
	testPartitionCancelAddIndex(c, s.causetstore, s.dom.DBS(), s.lease, idxName, addIdxALLEGROSQL)
}

func (s *testIntegrationSuite4) TestPartitionCancelAddIndex(c *C) {
	idxName := "idx1"
	addIdxALLEGROSQL := "create unique index c3_index on t1 (c1)"
	testPartitionCancelAddIndex(c, s.causetstore, s.dom.DBS(), s.lease, idxName, addIdxALLEGROSQL)
}

func testPartitionCancelAddIndex(c *C, causetstore ekv.CausetStorage, d dbs.DBS, lease time.Duration, idxName, addIdxALLEGROSQL string) {
	tk := testkit.NewTestKit(c, causetstore)

	tk.MustInterDirc("use test_db")
	tk.MustInterDirc("drop causet if exists t1;")
	tk.MustInterDirc(`create causet t1 (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
    	partition p0 values less than (1024),
    	partition p1 values less than (2048),
    	partition p2 values less than (3072),
    	partition p3 values less than (4096),
		partition p4 values less than (maxvalue)
   	);`)
	count := defaultBatchSize * 32
	// add some rows
	for i := 0; i < count; i += defaultBatchSize {
		batchInsert(tk, "t1", i, i+defaultBatchSize)
	}

	var checkErr error
	var c3IdxInfo *perceptron.IndexInfo
	hook := &dbs.TestDBSCallback{}
	originBatchSize := tk.MustQuery("select @@global.milevadb_dbs_reorg_batch_size")
	// Set batch size to lower try to slow down add-index reorganization, This if for hook to cancel this dbs job.
	tk.MustInterDirc("set @@global.milevadb_dbs_reorg_batch_size = 32")
	ctx := tk.Se.(stochastikctx.Context)
	defer tk.MustInterDirc(fmt.Sprintf("set @@global.milevadb_dbs_reorg_batch_size = %v", originBatchSize.Rows()[0][0]))
	hook.OnJobUFIDelatedExported, c3IdxInfo, checkErr = backgroundInterDircOnJobUFIDelatedExported(c, causetstore, ctx, hook, idxName)
	originHook := d.GetHook()
	defer d.(dbs.DBSForTest).SetHook(originHook)
	d.(dbs.DBSForTest).SetHook(hook)
	done := make(chan error, 1)
	go backgroundInterDirc(causetstore, addIdxALLEGROSQL, done)

	times := 0
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case err := <-done:
			c.Assert(checkErr, IsNil)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[dbs:8214]Cancelled DBS job")
			break LOOP
		case <-ticker.C:
			if times >= 10 {
				break
			}
			step := 10
			rand.Seed(time.Now().Unix())
			// delete some rows, and add some data
			for i := count; i < count+step; i++ {
				n := rand.Intn(count)
				tk.MustInterDirc("delete from t1 where c1 = ?", n)
				tk.MustInterDirc("insert into t1 values (?, ?, ?)", i+10, i, i)
			}
			count += step
			times++
		}
	}

	t := testGetBlockByName(c, ctx, "test_db", "t1")
	// Only one partition id test is taken here.
	pid := t.Meta().Partition.Definitions[0].ID
	for _, tidx := range t.Indices() {
		c.Assert(strings.EqualFold(tidx.Meta().Name.L, "c3_index"), IsFalse)
	}

	idx := blocks.NewIndex(pid, t.Meta(), c3IdxInfo)
	checkDelRangeDone(c, ctx, idx)

	tk.MustInterDirc("drop causet t1")
}

func backgroundInterDircOnJobUFIDelatedExported(c *C, causetstore ekv.CausetStorage, ctx stochastikctx.Context, hook *dbs.TestDBSCallback, idxName string) (
	func(*perceptron.Job), *perceptron.IndexInfo, error) {
	var checkErr error
	first := true
	c3IdxInfo := &perceptron.IndexInfo{}
	hook.OnJobUFIDelatedExported = func(job *perceptron.Job) {
		addIndexNotFirstReorg := (job.Type == perceptron.CausetActionAddIndex || job.Type == perceptron.CausetActionAddPrimaryKey) &&
			job.SchemaState == perceptron.StateWriteReorganization && job.SnapshotVer != 0
		// If the action is adding index and the state is writing reorganization, it want to test the case of cancelling the job when backfilling indexes.
		// When the job satisfies this case of addIndexNotFirstReorg, the worker will start to backfill indexes.
		if !addIndexNotFirstReorg {
			// Get the index's spacetime.
			if c3IdxInfo != nil {
				return
			}
			t := testGetBlockByName(c, ctx, "test_db", "t1")
			for _, index := range t.WriblockIndices() {
				if index.Meta().Name.L == idxName {
					c3IdxInfo = index.Meta()
				}
			}
			return
		}
		// The job satisfies the case of addIndexNotFirst for the first time, the worker hasn't finished a batch of backfill indexes.
		if first {
			first = false
			return
		}
		if checkErr != nil {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.CausetStore = causetstore
		err := hookCtx.NewTxn(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		jobIDs := []int64{job.ID}
		txn, err := hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		errs, err := admin.CancelJobs(txn, jobIDs)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		// It only tests cancel one DBS job.
		if errs[0] != nil {
			checkErr = errors.Trace(errs[0])
			return
		}
		txn, err = hookCtx.Txn(true)
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		err = txn.Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
		}
	}
	return hook.OnJobUFIDelatedExported, c3IdxInfo, checkErr
}

func (s *testIntegrationSuite5) TestPartitionAddPrimaryKey(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	testPartitionAddIndexOrPK(c, tk, "primary key")
}

func (s *testIntegrationSuite1) TestPartitionAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	testPartitionAddIndexOrPK(c, tk, "index")
}

func testPartitionAddIndexOrPK(c *C, tk *testkit.TestKit, key string) {
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`create causet partition_add_idx (
	id int not null,
	hired date not null
	)
	partition by range( year(hired) ) (
	partition p1 values less than (1991),
	partition p3 values less than (2001),
	partition p4 values less than (2004),
	partition p5 values less than (2008),
	partition p6 values less than (2012),
	partition p7 values less than (2020)
	);`)
	testPartitionAddIndex(tk, c, key)

	// test hash partition causet.
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustInterDirc("drop causet if exists partition_add_idx")
	tk.MustInterDirc(`create causet partition_add_idx (
	id int not null,
	hired date not null
	) partition by hash( year(hired) ) partitions 4;`)
	testPartitionAddIndex(tk, c, key)

	// Test hash partition for pr 10475.
	tk.MustInterDirc("drop causet if exists t1")
	defer tk.MustInterDirc("drop causet if exists t1")
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = '1';")
	tk.MustInterDirc("create causet t1 (a int, b int, unique key(a)) partition by hash(a) partitions 5;")
	tk.MustInterDirc("insert into t1 values (0,0),(1,1),(2,2),(3,3);")
	tk.MustInterDirc(fmt.Sprintf("alter causet t1 add %s idx(a)", key))
	tk.MustInterDirc("admin check causet t1;")

	// Test range partition for pr 10475.
	tk.MustInterDirc("drop causet t1")
	tk.MustInterDirc("create causet t1 (a int, b int, unique key(a)) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20));")
	tk.MustInterDirc("insert into t1 values (0,0);")
	tk.MustInterDirc(fmt.Sprintf("alter causet t1 add %s idx(a)", key))
	tk.MustInterDirc("admin check causet t1;")
}

func testPartitionAddIndex(tk *testkit.TestKit, c *C, key string) {
	idxName1 := "idx1"

	f := func(end int, isPK bool) string {
		dml := fmt.Sprintf("insert into partition_add_idx values")
		for i := 0; i < end; i++ {
			dVal := 1988 + rand.Intn(30)
			if isPK {
				dVal = 1518 + i
			}
			dml += fmt.Sprintf("(%d, '%d-01-01')", i, dVal)
			if i != end-1 {
				dml += ","
			}
		}
		return dml
	}
	var dml string
	if key == "primary key" {
		idxName1 = "primary"
		// For the primary key, hired must be unique.
		dml = f(500, true)
	} else {
		dml = f(500, false)
	}
	tk.MustInterDirc(dml)

	tk.MustInterDirc(fmt.Sprintf("alter causet partition_add_idx add %s idx1 (hired)", key))
	tk.MustInterDirc("alter causet partition_add_idx add index idx2 (id, hired)")
	ctx := tk.Se.(stochastikctx.Context)
	is := petri.GetPetri(ctx).SchemaReplicant()
	t, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr("partition_add_idx"))
	c.Assert(err, IsNil)
	var idx1 causet.Index
	for _, idx := range t.Indices() {
		if idx.Meta().Name.L == idxName1 {
			idx1 = idx
			break
		}
	}
	c.Assert(idx1, NotNil)

	tk.MustQuery(fmt.Sprintf("select count(hired) from partition_add_idx use index(%s)", idxName1)).Check(testkit.Rows("500"))
	tk.MustQuery("select count(id) from partition_add_idx use index(idx2)").Check(testkit.Rows("500"))

	tk.MustInterDirc("admin check causet partition_add_idx")
	tk.MustInterDirc("drop causet partition_add_idx")
}

func (s *testIntegrationSuite5) TestDropSchemaWithPartitionBlock(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("drop database if exists test_db_with_partition")
	tk.MustInterDirc("create database test_db_with_partition")
	tk.MustInterDirc("use test_db_with_partition")
	tk.MustInterDirc(`create causet t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	tk.MustInterDirc("insert into t_part values (1),(2),(11),(12);")
	ctx := s.ctx
	tbl := testGetBlockByName(c, ctx, "test_db_with_partition", "t_part")

	// check records num before drop database.
	recordsNum := getPartitionBlockRecordsNum(c, ctx, tbl.(causet.PartitionedBlock))
	c.Assert(recordsNum, Equals, 4)

	tk.MustInterDirc("drop database if exists test_db_with_partition")

	// check job args.
	rs, err := tk.InterDirc("admin show dbs jobs")
	c.Assert(err, IsNil)
	rows, err := stochastik.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	event := rows[0]
	c.Assert(event.GetString(3), Equals, "drop schemaReplicant")
	jobID := event.GetInt64(0)
	ekv.RunInNewTxn(s.causetstore, false, func(txn ekv.Transaction) error {
		t := spacetime.NewMeta(txn)
		historyJob, err := t.GetHistoryDBSJob(jobID)
		c.Assert(err, IsNil)
		var blockIDs []int64
		err = historyJob.DecodeArgs(&blockIDs)
		c.Assert(err, IsNil)
		// There is 2 partitions.
		c.Assert(len(blockIDs), Equals, 3)
		return nil
	})

	// check records num after drop database.
	for i := 0; i < waitForCleanDataRound; i++ {
		recordsNum = getPartitionBlockRecordsNum(c, ctx, tbl.(causet.PartitionedBlock))
		if recordsNum != 0 {
			time.Sleep(waitForCleanDataInterval)
		} else {
			break
		}
	}
	c.Assert(recordsNum, Equals, 0)
}

func getPartitionBlockRecordsNum(c *C, ctx stochastikctx.Context, tbl causet.PartitionedBlock) int {
	num := 0
	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.(causet.PartitionedBlock).GetPartition(pid)
		startKey := partition.RecordKey(ekv.IntHandle(math.MinInt64))
		c.Assert(ctx.NewTxn(context.Background()), IsNil)
		err := partition.IterRecords(ctx, startKey, partition.DefCauss(),
			func(_ ekv.Handle, data []types.Causet, defcaus []*causet.DeferredCauset) (bool, error) {
				num++
				return true, nil
			})
		c.Assert(err, IsNil)
	}
	return num
}

func (s *testIntegrationSuite3) TestPartitionErrorCode(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// add partition
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustInterDirc("drop database if exists test_db_with_partition")
	tk.MustInterDirc("create database test_db_with_partition")
	tk.MustInterDirc("use test_db_with_partition")
	tk.MustInterDirc(`create causet employees (
		id int not null,
		fname varchar(30),
		lname varchar(30),
		hired date not null default '1970-01-01',
		separated date not null default '9999-12-31',
		job_code int,
		store_id int
	)
	partition by hash(store_id)
	partitions 4;`)
	_, err := tk.InterDirc("alter causet employees add partition partitions 8;")
	c.Assert(dbs.ErrUnsupportedAddPartition.Equal(err), IsTrue)

	_, err = tk.InterDirc("alter causet employees add partition (partition p5 values less than (42));")
	c.Assert(dbs.ErrUnsupportedAddPartition.Equal(err), IsTrue)

	// coalesce partition
	tk.MustInterDirc(`create causet clients (
		id int,
		fname varchar(30),
		lname varchar(30),
		signed date
	)
	partition by hash( month(signed) )
	partitions 12;`)
	_, err = tk.InterDirc("alter causet clients coalesce partition 4;")
	c.Assert(dbs.ErrUnsupportedCoalescePartition.Equal(err), IsTrue)

	tk.MustInterDirc(`create causet t_part (a int key)
		partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (20)
		);`)
	_, err = tk.InterDirc("alter causet t_part coalesce partition 4;")
	c.Assert(dbs.ErrCoalesceOnlyOnHashPartition.Equal(err), IsTrue)

	tk.MustGetErrCode(`alter causet t_part reorganize partition p0, p1 into (
			partition p0 values less than (1980));`, tmysql.ErrUnsupportedDBSOperation)

	tk.MustGetErrCode("alter causet t_part check partition p0, p1;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t_part optimize partition p0,p1;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t_part rebuild partition p0,p1;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t_part remove partitioning;", tmysql.ErrUnsupportedDBSOperation)
	tk.MustGetErrCode("alter causet t_part repair partition p1;", tmysql.ErrUnsupportedDBSOperation)
}

func (s *testIntegrationSuite5) TestConstAndTimezoneDepent(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// add partition
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustInterDirc("drop database if exists test_db_with_partition_const")
	tk.MustInterDirc("create database test_db_with_partition_const")
	tk.MustInterDirc("use test_db_with_partition_const")

	sql1 := `create causet t1 ( id int )
		partition by range(4) (
		partition p1 values less than (10)
		);`
	tk.MustGetErrCode(sql1, tmysql.ErrWrongExprInPartitionFunc)

	sql2 := `create causet t2 ( time_recorded timestamp )
		partition by range(TO_DAYS(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql2, tmysql.ErrWrongExprInPartitionFunc)

	sql3 := `create causet t3 ( id int )
		partition by range(DAY(id)) (
		partition p1 values less than (1)
		);`
	tk.MustGetErrCode(sql3, tmysql.ErrWrongExprInPartitionFunc)

	sql4 := `create causet t4 ( id int )
		partition by hash(4) partitions 4
		;`
	tk.MustGetErrCode(sql4, tmysql.ErrWrongExprInPartitionFunc)

	sql5 := `create causet t5 ( time_recorded timestamp )
		partition by range(to_seconds(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql5, tmysql.ErrWrongExprInPartitionFunc)

	sql6 := `create causet t6 ( id int )
		partition by range(to_seconds(id)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql6, tmysql.ErrWrongExprInPartitionFunc)

	sql7 := `create causet t7 ( time_recorded timestamp )
		partition by range(abs(time_recorded)) (
		partition p1 values less than (1559192604)
		);`
	tk.MustGetErrCode(sql7, tmysql.ErrWrongExprInPartitionFunc)

	sql8 := `create causet t2332 ( time_recorded time )
         partition by range(TO_DAYS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql8, tmysql.ErrWrongExprInPartitionFunc)

	sql9 := `create causet t1 ( id int )
		partition by hash(4) partitions 4;`
	tk.MustGetErrCode(sql9, tmysql.ErrWrongExprInPartitionFunc)

	sql10 := `create causet t1 ( id int )
		partition by hash(ed) partitions 4;`
	tk.MustGetErrCode(sql10, tmysql.ErrBadField)

	sql11 := `create causet t2332 ( time_recorded time )
         partition by range(TO_SECONDS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql11, tmysql.ErrWrongExprInPartitionFunc)

	sql12 := `create causet t2332 ( time_recorded time )
         partition by range(TO_SECONDS(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql12, tmysql.ErrWrongExprInPartitionFunc)

	sql13 := `create causet t2332 ( time_recorded time )
         partition by range(day(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql13, tmysql.ErrWrongExprInPartitionFunc)

	sql14 := `create causet t2332 ( time_recorded timestamp )
         partition by range(day(time_recorded)) (
  		 partition p0 values less than (1)
		);`
	tk.MustGetErrCode(sql14, tmysql.ErrWrongExprInPartitionFunc)
}

func (s *testIntegrationSuite5) TestConstAndTimezoneDepent2(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	// add partition
	tk.MustInterDirc("set @@stochastik.milevadb_enable_block_partition = 1")
	tk.MustInterDirc("drop database if exists test_db_with_partition_const")
	tk.MustInterDirc("create database test_db_with_partition_const")
	tk.MustInterDirc("use test_db_with_partition_const")

	tk.MustInterDirc(`create causet t1 ( time_recorded datetime )
	partition by range(TO_DAYS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustInterDirc(`create causet t2 ( time_recorded date )
	partition by range(TO_DAYS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustInterDirc(`create causet t3 ( time_recorded date )
	partition by range(TO_SECONDS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustInterDirc(`create causet t4 ( time_recorded date )
	partition by range(TO_SECONDS(time_recorded)) (
	partition p0 values less than (1));`)

	tk.MustInterDirc(`create causet t5 ( time_recorded timestamp )
	partition by range(unix_timestamp(time_recorded)) (
		partition p1 values less than (1559192604)
	);`)
}

func (s *testIntegrationSuite3) TestUnsupportedPartitionManagementDBSs(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test;")
	tk.MustInterDirc("drop causet if exists test_1465;")
	tk.MustInterDirc(`
		create causet test_1465 (a int)
		partition by range(a) (
			partition p1 values less than (10),
			partition p2 values less than (20),
			partition p3 values less than (30)
		);
	`)

	_, err := tk.InterDirc("alter causet test_1465 partition by hash(a)")
	c.Assert(err, ErrorMatches, ".*alter causet partition is unsupported")
}

func (s *testIntegrationSuite7) TestCommitWhenSchemaChange(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc(`create causet schema_change (a int, b timestamp)
			partition by range(a) (
			    partition p0 values less than (4),
			    partition p1 values less than (7),
			    partition p2 values less than (11)
			)`)
	tk2 := testkit.NewTestKit(c, s.causetstore)
	tk2.MustInterDirc("use test")

	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into schema_change values (1, '2020-12-25 13:27:42')")
	tk.MustInterDirc("insert into schema_change values (3, '2020-12-25 13:27:43')")

	tk2.MustInterDirc("alter causet schema_change add index idx(b)")

	tk.MustInterDirc("insert into schema_change values (5, '2020-12-25 13:27:43')")
	tk.MustInterDirc("insert into schema_change values (9, '2020-12-25 13:27:44')")
	atomic.StoreUint32(&stochastik.SchemaChangedWithoutRetry, 1)
	defer func() {
		atomic.StoreUint32(&stochastik.SchemaChangedWithoutRetry, 0)
	}()
	_, err := tk.Se.InterDircute(context.Background(), "commit")
	c.Assert(petri.ErrSchemaReplicantChanged.Equal(err), IsTrue)

	// Cover a bug that schemaReplicant validator does not prevent transaction commit when
	// the schemaReplicant has changed on the partitioned causet.
	// That bug will cause data and index inconsistency!
	tk.MustInterDirc("admin check causet schema_change")
	tk.MustQuery("select * from schema_change").Check(testkit.Rows())

	// Check inconsistency when exchanging partition
	tk.MustInterDirc(`drop causet if exists pt, nt;`)
	tk.MustInterDirc(`create causet pt (a int) partition by hash(a) partitions 2;`)
	tk.MustInterDirc(`create causet nt (a int);`)

	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into nt values (1), (3), (5);")
	tk2.MustInterDirc("alter causet pt exchange partition p1 with causet nt;")
	tk.MustInterDirc("insert into nt values (7), (9);")
	_, err = tk.Se.InterDircute(context.Background(), "commit")
	c.Assert(petri.ErrSchemaReplicantChanged.Equal(err), IsTrue)

	tk.MustInterDirc("admin check causet pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustInterDirc("admin check causet nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())

	tk.MustInterDirc("begin")
	tk.MustInterDirc("insert into pt values (1), (3), (5);")
	tk2.MustInterDirc("alter causet pt exchange partition p1 with causet nt;")
	tk.MustInterDirc("insert into pt values (7), (9);")
	_, err = tk.Se.InterDircute(context.Background(), "commit")
	c.Assert(petri.ErrSchemaReplicantChanged.Equal(err), IsTrue)

	tk.MustInterDirc("admin check causet pt")
	tk.MustQuery("select * from pt").Check(testkit.Rows())
	tk.MustInterDirc("admin check causet nt")
	tk.MustQuery("select * from nt").Check(testkit.Rows())
}
