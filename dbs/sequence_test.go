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
	"strconv"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/dbs"
	allegrosql "github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

var _ = Suite(&testSequenceSuite{&testDBSuite{}})

type testSequenceSuite struct{ *testDBSuite }

func (s *testSequenceSuite) TestCreateSequence(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustGetErrCode("create sequence `seq  `", allegrosql.ErrWrongBlockName)

	// increment should not be set as 0.
	tk.MustGetErrCode("create sequence seq increment 0", allegrosql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 2", allegrosql.ErrSequenceInvalidData)

	// maxvalue should be larger than minvalue.
	tk.MustGetErrCode("create sequence seq maxvalue 1 minvalue 1", allegrosql.ErrSequenceInvalidData)

	// maxvalue shouldn't be equal to MaxInt64.
	tk.MustGetErrCode("create sequence seq maxvalue 9223372036854775807 minvalue 1", allegrosql.ErrSequenceInvalidData)

	// TODO : minvalue shouldn't be equal to MinInt64.

	// maxvalue should be larger than start.
	tk.MustGetErrCode("create sequence seq maxvalue 1 start with 2", allegrosql.ErrSequenceInvalidData)

	// cacheVal should be less than (math.MaxInt64-maxIncrement)/maxIncrement.
	tk.MustGetErrCode("create sequence seq increment 100000 cache 922337203685477", allegrosql.ErrSequenceInvalidData)

	// test unsupported causet option in sequence.
	tk.MustGetErrCode("create sequence seq CHARSET=utf8", allegrosql.ErrSequenceUnsupportedBlockOption)

	_, err := tk.InterDirc("create sequence seq comment=\"test\"")
	c.Assert(err, IsNil)

	sequenceBlock := testGetBlockByName(c, s.s, "test", "seq")
	c.Assert(sequenceBlock.Meta().IsSequence(), Equals, true)
	c.Assert(sequenceBlock.Meta().Sequence.Increment, Equals, perceptron.DefaultSequenceIncrementValue)
	c.Assert(sequenceBlock.Meta().Sequence.Start, Equals, perceptron.DefaultPositiveSequenceStartValue)
	c.Assert(sequenceBlock.Meta().Sequence.MinValue, Equals, perceptron.DefaultPositiveSequenceMinValue)
	c.Assert(sequenceBlock.Meta().Sequence.MaxValue, Equals, perceptron.DefaultPositiveSequenceMaxValue)
	c.Assert(sequenceBlock.Meta().Sequence.Cache, Equals, true)
	c.Assert(sequenceBlock.Meta().Sequence.CacheValue, Equals, perceptron.DefaultSequenceCacheValue)
	c.Assert(sequenceBlock.Meta().Sequence.Cycle, Equals, false)

	// Test create privilege.
	tk.MustInterDirc("create user myuser@localhost")

	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil), IsTrue)
	tk1.Se = se

	// grant the myuser the access to database test.
	tk.MustInterDirc("grant select on test.* to 'myuser'@'localhost'")

	tk1.MustInterDirc("use test")
	_, err = tk1.InterDirc("create sequence my_seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[causet:1142]CREATE command denied to user 'myuser'@'localhost' for causet 'my_seq'")
}

func (s *testSequenceSuite) TestDropSequence(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop sequence if exists seq")

	// Test sequence is unknown.
	tk.MustGetErrCode("drop sequence seq", allegrosql.ErrUnknownSequence)

	// Test non-existed sequence can't drop successfully.
	tk.MustInterDirc("create sequence seq")
	_, err := tk.InterDirc("drop sequence seq, seq2")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:4139]Unknown SEQUENCE: 'test.seq2'")

	// Test the specified object is not sequence.
	tk.MustInterDirc("create causet seq3 (a int)")
	_, err = tk.InterDirc("drop sequence seq3")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, dbs.ErrWrongObject), IsTrue)

	// Test schemaReplicant is not exist.
	_, err = tk.InterDirc("drop sequence unknown.seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:4139]Unknown SEQUENCE: 'unknown.seq'")

	// Test drop sequence successfully.
	tk.MustInterDirc("create sequence seq")
	_, err = tk.InterDirc("drop sequence seq")
	c.Assert(err, IsNil)
	_, err = tk.InterDirc("drop sequence seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:4139]Unknown SEQUENCE: 'test.seq'")

	// Test drop causet when the object is a sequence.
	tk.MustInterDirc("create sequence seq")
	_, err = tk.InterDirc("drop causet seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1051]Unknown causet 'test.seq'")

	// Test drop view when the object is a sequence.
	_, err = tk.InterDirc("drop view seq")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, dbs.ErrWrongObject), IsTrue)
	tk.MustInterDirc("drop sequence seq")

	// Test drop privilege.
	tk.MustInterDirc("drop user if exists myuser@localhost")
	tk.MustInterDirc("create user myuser@localhost")

	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil), IsTrue)
	tk1.Se = se

	// grant the myuser the access to database test.
	tk.MustInterDirc("create sequence my_seq")
	tk.MustInterDirc("grant select on test.* to 'myuser'@'localhost'")

	tk1.MustInterDirc("use test")
	_, err = tk1.InterDirc("drop sequence my_seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[causet:1142]DROP command denied to user 'myuser'@'localhost' for causet 'my_seq'")

	// Test for `drop sequence if exists`.
	tk.MustInterDirc("drop sequence if exists seq_if_exists")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 4139 Unknown SEQUENCE: 'test.seq_if_exists'"))
}

func (s *testSequenceSuite) TestShowCreateSequence(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("create sequence seq")

	// Test show privilege.
	tk.MustInterDirc("drop user if exists myuser@localhost")
	tk.MustInterDirc("create user myuser@localhost")

	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil), IsTrue)
	tk1.Se = se

	// Grant the myuser the access to causet t in database test, but sequence seq.
	tk.MustInterDirc("grant select on test.t to 'myuser'@'localhost'")

	tk1.MustInterDirc("use test")
	tk1.MustInterDirc("show create causet t")
	_, err = tk1.InterDirc("show create sequence seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[causet:1142]SHOW command denied to user 'myuser'@'localhost' for causet 'seq'")

	// Grant the myuser the access to sequence seq in database test.
	tk.MustInterDirc("grant select on test.seq to 'myuser'@'localhost'")

	tk1.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	// Test show sequence detail.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq start 10")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 10 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq minvalue 0")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 0 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq maxvalue 100")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 100 increment by 1 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = -2")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with -1 minvalue -9223372036854775807 maxvalue -1 increment by -2 cache 1000 nocycle ENGINE=InnoDB"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq nocache")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 nocache nocycle ENGINE=InnoDB"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq cycle")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 cycle ENGINE=InnoDB"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq comment=\"ccc\"")
	tk.MustQuery("show create sequence seq").Check(testkit.Rows("seq CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB COMMENT='ccc'"))

	// Test show create sequence with a normal causet.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create causet seq (a int)")
	err = tk.QueryToErr("show create sequence seq")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[interlock:1347]'test.seq' is not SEQUENCE")
	tk.MustInterDirc("drop causet if exists seq")

	// Test use the show create sequence result to create sequence.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq")
	showString := tk.MustQuery("show create sequence seq").Rows()[0][1].(string)
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc(showString)
}

func (s *testSequenceSuite) TestSequenceAsDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq")

	// test the use sequence's nextval as default.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int not null default next value for seq key)")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int not null default nextval(seq), b int, primary key(a))")

	tk.MustInterDirc("create causet t1 (a int default next value for seq)")
	tk.MustGetErrMsg("create causet t2 (a char(1) default next value for seq)", "[dbs:8228]Unsupported sequence default value for defCausumn type 'a'")

	tk.MustInterDirc("create causet t3 (a int default nextval(seq))")

	tk.MustInterDirc("create causet t4 (a int)")
	tk.MustInterDirc("alter causet t4 alter defCausumn a set default (next value for seq)")
	tk.MustInterDirc("alter causet t4 alter defCausumn a set default (nextval(seq))")

	tk.MustInterDirc("create causet t5 (a char(1))")
	tk.MustGetErrMsg("alter causet t5 alter defCausumn a set default (next value for seq)", "[dbs:8228]Unsupported sequence default value for defCausumn type 'a'")

	tk.MustGetErrMsg("alter causet t5 alter defCausumn a set default (nextval(seq))", "[dbs:8228]Unsupported sequence default value for defCausumn type 'a'")

	// Specially, the new added defCausumn with sequence as it's default value is forbade.
	// But alter causet defCausumn with sequence as it's default value is allowed.
	tk.MustGetErrMsg("alter causet t5 add defCausumn c int default next value for seq", "[dbs:8230]Unsupported using sequence as default value in add defCausumn 'c'")

	tk.MustInterDirc("alter causet t5 add defCausumn c int default -1")
	// Alter with modify.
	tk.MustInterDirc("alter causet t5 modify defCausumn c int default next value for seq")
	// Alter with alter.
	tk.MustInterDirc("alter causet t5 alter defCausumn c set default (next value for seq)")
	// Alter with change.
	tk.MustInterDirc("alter causet t5 change defCausumn c c int default next value for seq")
}

func (s *testSequenceSuite) TestSequenceFunction(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop sequence if exists seq1")
	tk.MustInterDirc("create sequence seq")

	// test normal sequence function.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select next value for seq").Check(testkit.Rows("3"))
	tk.MustQuery("select next value for test.seq").Check(testkit.Rows("4"))

	// test sequence function error.
	tk.MustGetErrMsg("select nextval(seq1)", "[schemaReplicant:1146]Block 'test.seq1' doesn't exist")
	tk.MustInterDirc("create database test2")
	tk.MustInterDirc("use test2")
	tk.MustQuery("select nextval(test.seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select next value for test.seq").Check(testkit.Rows("6"))
	tk.MustGetErrMsg("select nextval(seq)", "[schemaReplicant:1146]Block 'test2.seq' doesn't exist")
	tk.MustGetErrMsg("select next value for seq", "[schemaReplicant:1146]Block 'test2.seq' doesn't exist")
	tk.MustInterDirc("use test")

	// test sequence nocache.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq nocache")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))

	// test sequence option logic.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = 5")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = 5 start = 3")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("13"))

	// minvalue should be specified lower than start (negative here), default 1 when increment > 0.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq minvalue -5 start = -2 increment = 5")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	// test sequence cycle.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = 5 start = 3 maxvalue = 12 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence maxvalue allocation.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = 4 start = 2 maxvalue = 10 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))

	// test sequence has run out.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = 5 start = 3 maxvalue = 12 nocycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))
	err := tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = 3 start = 3 maxvalue = 9 nocycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9"))
	err = tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")

	// test negative-growth sequence
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = -2 start = 3 minvalue -5 maxvalue = 12 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-3"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = -3 start = 2 minvalue -6 maxvalue = 11 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("11"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("8"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = -4 start = 6 minvalue -6 maxvalue = 11")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-6"))
	err = tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment = -3 start = 2 minvalue -2 maxvalue 10")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-1"))
	err = tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")

	// test sequence setval function.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("2"))
	// set value to a used value, will get NULL.
	tk.MustQuery("select setval(seq, 2)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	// set value to a unused value, will get itself.
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	// the next value will not be base on next value.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment 3 maxvalue 11")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	tk.MustQuery("select setval(seq, 3)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 4)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	tk.MustQuery("select setval(seq, 8)").Check(testkit.Rows("8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	err = tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")
	tk.MustQuery("select setval(seq, 11)").Check(testkit.Rows("11"))
	err = tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")
	// set value can be bigger than maxvalue.
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	err = tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")

	// test setval in second cache round.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment 10 start 5 maxvalue 100 cache 10 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("15"))
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	// the next value will not be base on next value.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("25"))
	sequenceBlock := testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok := sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round := tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(95))
	c.Assert(round, Equals, int64(0))
	// exhausted the sequence first round in cycle.
	tk.MustQuery("select setval(seq, 95)").Check(testkit.Rows("95"))
	// make sequence alloc the next batch.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(91))
	c.Assert(round, Equals, int64(1))
	tk.MustQuery("select setval(seq, 15)").Check(testkit.Rows("15"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("21"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("31"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment 2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("20"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-10"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-6))
	c.Assert(round, Equals, int64(1))

	// test setval in negative-growth sequence.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment -3 start 5 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("5"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-1))
	c.Assert(round, Equals, int64(0))
	// exhausted the sequence first cache batch.
	tk.MustQuery("select setval(seq, -2)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-4"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-10))
	c.Assert(round, Equals, int64(0))
	// exhausted the sequence second cache batch.
	tk.MustQuery("select setval(seq, -10)").Check(testkit.Rows("-10"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(4))
	c.Assert(round, Equals, int64(1))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("4"))
	// test the sequence negative rebase.
	tk.MustQuery("select setval(seq, 0)").Check(testkit.Rows("0"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment -2 start 0 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select setval(seq, 20)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select setval(seq, -20)").Check(testkit.Rows("-20"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(6))
	c.Assert(round, Equals, int64(1))

	// test sequence lastval function.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select next value for seq").Check(testkit.Rows("2"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	// setval won't change the last value.
	tk.MustQuery("select setval(seq, -1)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("7"))

	// test lastval in positive-growth sequence cycle and cache.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment 3 start 3 maxvalue 14 cache 3 cycle")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(9))
	c.Assert(round, Equals, int64(0))
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("3"))
	// trigger the next sequence cache.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("12"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(14))
	c.Assert(round, Equals, int64(0))
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 13)").Check(testkit.Rows("13"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("12"))
	// trigger the next sequence cache.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(7))
	c.Assert(round, Equals, int64(1))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))

	// test lastval in negative-growth sequence cycle and cache.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment -3 start -2 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-2"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-8))
	c.Assert(round, Equals, int64(0))
	// invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("-2"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("10"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(4))
	c.Assert(round, Equals, int64(1))
	tk.MustQuery("select lastval(seq)").Check(testkit.Rows("10"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment -1 start 1 maxvalue 10 minvalue -10 cache 3 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk.MustQuery("select setval(seq, -8)").Check(testkit.Rows("-8"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9"))
	sequenceBlock = testGetBlockByName(c, tk.Se, "test", "seq")
	tc, ok = sequenceBlock.(*blocks.BlockCommon)
	c.Assert(ok, Equals, true)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	c.Assert(end, Equals, int64(-10))
	c.Assert(round, Equals, int64(0))

	// Test the sequence seek formula will overflow Int64.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment 2 start -9223372036854775807 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775807"))
	tk.MustQuery("select setval(seq, 9223372036854775800)").Check(testkit.Rows("9223372036854775800"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775801"))

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq increment -2 start 9223372036854775806 maxvalue 9223372036854775806 minvalue -9223372036854775807 cache 2 cycle")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("9223372036854775806"))
	tk.MustQuery("select setval(seq, -9223372036854775800)").Check(testkit.Rows("-9223372036854775800"))
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("-9223372036854775802"))

	// Test sequence function with wrong object name.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop causet if exists seq")
	tk.MustInterDirc("drop view if exists seq")
	tk.MustInterDirc("drop sequence if exists seq1")
	tk.MustInterDirc("drop causet if exists seq1")
	tk.MustInterDirc("drop view if exists seq1")
	tk.MustInterDirc("create causet seq(a int)")
	_, err = tk.InterDirc("select nextval(seq)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.seq' is not SEQUENCE")
	_, err = tk.InterDirc("select lastval(seq)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.seq' is not SEQUENCE")
	_, err = tk.InterDirc("select setval(seq, 10)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.seq' is not SEQUENCE")

	tk.MustInterDirc("create view seq1 as select * from seq")
	_, err = tk.InterDirc("select nextval(seq1)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.seq1' is not SEQUENCE")
	_, err = tk.InterDirc("select lastval(seq1)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.seq1' is not SEQUENCE")
	_, err = tk.InterDirc("select setval(seq1, 10)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.seq1' is not SEQUENCE")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop causet if exists seq")
	tk.MustInterDirc("drop view if exists seq")
	tk.MustInterDirc("drop sequence if exists seq1")
	tk.MustInterDirc("drop causet if exists seq1")
	tk.MustInterDirc("drop view if exists seq1")

	// test a bug found in ticase.
	tk.MustInterDirc("create sequence seq")
	tk.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("<nil>"))
	tk.MustInterDirc("drop sequence seq")
	tk.MustInterDirc("create sequence seq increment=-1")
	tk.MustQuery("select setval(seq, -10)").Check(testkit.Rows("-10"))
	tk.MustQuery("select setval(seq, -5)").Check(testkit.Rows("<nil>"))
	tk.MustInterDirc("drop sequence seq")

	// test the current value already satisfied setval in other stochastik.
	tk.MustInterDirc("create sequence seq")
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	tk1 := testkit.NewTestKit(c, s.causetstore)
	tk1.Se = se
	tk1.MustInterDirc("use test")
	tk1.MustQuery("select setval(seq, 50)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("101"))
	tk1.MustQuery("select setval(seq, 100)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, 101)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, 102)").Check(testkit.Rows("102"))
	tk.MustInterDirc("drop sequence seq")

	tk.MustInterDirc("create sequence seq increment=-1")
	tk.MustQuery("select setval(seq, -100)").Check(testkit.Rows("-100"))
	tk1.MustQuery("select setval(seq, -50)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("-101"))
	tk1.MustQuery("select setval(seq, -100)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, -101)").Check(testkit.Rows("<nil>"))
	tk1.MustQuery("select setval(seq, -102)").Check(testkit.Rows("-102"))
	tk.MustInterDirc("drop sequence seq")

	// test the sequence name preprocess.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("insert into t values(1),(2)")
	tk.MustQuery("select nextval(seq), t.a from t").Check(testkit.Rows("1 1", "2 2"))
	_, err = tk.InterDirc("select nextval(t), t.a from t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.t' is not SEQUENCE")
	_, err = tk.InterDirc("select nextval(seq), nextval(t), t.a from t")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[schemaReplicant:1347]'test.t' is not SEQUENCE")
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("3"))
	tk.MustInterDirc("drop sequence seq")
	tk.MustInterDirc("drop causet t")
}

func (s *testSequenceSuite) TestInsertSequence(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop causet if exists t")

	// test insert with sequence default value.
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("create causet t (a int default next value for seq)")
	tk.MustInterDirc("insert into t values()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustInterDirc("insert into t values(),(),()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t values(-1),(default),(-1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1", "5", "-1"))

	// test insert with specified sequence value rather than default.
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (a int)")
	tk.MustInterDirc("insert into t values(next value for seq)")
	tk.MustQuery("select * from t").Check(testkit.Rows("6"))
	tk.MustInterDirc("insert into t values(next value for seq),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("6", "7", "8"))

	// test insert with sequence memex.
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t values(next value for seq + 1),(nextval(seq) * 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("10", "20"))
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t values((next value for seq - 1) / 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("5"))

	// test insert with user specified value.
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t values(-1),(next value for seq),(-1),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1", "12", "-1", "13"))

	// test insert with lastval & setval.
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t values(lastval(seq)),(-1),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("13", "-1", "14"))
	tk.MustInterDirc("delete from t")
	tk.MustQuery("select setval(seq, 100)").Check(testkit.Rows("100"))
	tk.MustInterDirc("insert into t values(lastval(seq)),(-1),(nextval(seq))")
	tk.MustQuery("select * from t").Check(testkit.Rows("14", "-1", "101"))

	// test insert with generated defCausumn.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t (id int default next value for seq, defCaus1 int generated always as (id + 1))")

	tk.MustInterDirc("insert into t values()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustInterDirc("insert into t values(),()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "2 3", "3 4"))
	tk.MustInterDirc("delete from t")
	tk.MustInterDirc("insert into t (id) values(-1),(default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 0", "4 5"))

	// test sequence run out (overflows MaxInt64).
	setALLEGROSQL := "select setval(seq," + strconv.FormatInt(perceptron.DefaultPositiveSequenceMaxValue+1, 10) + ")"
	tk.MustQuery(setALLEGROSQL).Check(testkit.Rows("9223372036854775807"))
	err := tk.QueryToErr("select nextval(seq)")
	c.Assert(err.Error(), Equals, "[causet:4135]Sequence 'test.seq' has run out")
}

func (s *testSequenceSuite) TestUnflodSequence(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	// test insert into select from.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop causet if exists t1,t2,t3")
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("create causet t1 (a int)")
	tk.MustInterDirc("create causet t2 (a int, b int)")
	tk.MustInterDirc("create causet t3 (a int, b int, c int)")
	tk.MustInterDirc("insert into t1 values(-1),(-1),(-1)")
	// test sequence function unfold.
	tk.MustQuery("select nextval(seq), a from t1").Check(testkit.Rows("1 -1", "2 -1", "3 -1"))
	tk.MustInterDirc("insert into t2 select nextval(seq), a from t1")
	tk.MustQuery("select * from t2").Check(testkit.Rows("4 -1", "5 -1", "6 -1"))
	tk.MustInterDirc("delete from t2")

	// if lastval is folded, the first result should be always 6.
	tk.MustQuery("select lastval(seq), nextval(seq), a from t1").Check(testkit.Rows("6 7 -1", "7 8 -1", "8 9 -1"))
	tk.MustInterDirc("insert into t3 select lastval(seq), nextval(seq), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("9 10 -1", "10 11 -1", "11 12 -1"))
	tk.MustInterDirc("delete from t3")

	// if setval is folded, the result should be "101 100 -1"...
	tk.MustQuery("select nextval(seq), setval(seq,100), a from t1").Check(testkit.Rows("13 100 -1", "101 <nil> -1", "102 <nil> -1"))
	tk.MustInterDirc("insert into t3 select nextval(seq), setval(seq,200), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("103 200 -1", "201 <nil> -1", "202 <nil> -1"))
	tk.MustInterDirc("delete from t3")

	// lastval should be evaluated after nextval in each event.
	tk.MustQuery("select nextval(seq), lastval(seq), a from t1").Check(testkit.Rows("203 203 -1", "204 204 -1", "205 205 -1"))
	tk.MustInterDirc("insert into t3 select nextval(seq), lastval(seq), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("206 206 -1", "207 207 -1", "208 208 -1"))
	tk.MustInterDirc("delete from t3")

	// double nextval should be also evaluated in each event.
	tk.MustQuery("select nextval(seq), nextval(seq), a from t1").Check(testkit.Rows("209 210 -1", "211 212 -1", "213 214 -1"))
	tk.MustInterDirc("insert into t3 select nextval(seq), nextval(seq), a from t1")
	tk.MustQuery("select * from t3").Check(testkit.Rows("215 216 -1", "217 218 -1", "219 220 -1"))
	tk.MustInterDirc("delete from t3")

	tk.MustQuery("select nextval(seq)+lastval(seq), a from t1").Check(testkit.Rows("442 -1", "444 -1", "446 -1"))
	tk.MustInterDirc("insert into t2 select nextval(seq)+lastval(seq), a from t1")
	tk.MustQuery("select * from t2").Check(testkit.Rows("448 -1", "450 -1", "452 -1"))
	tk.MustInterDirc("delete from t2")

	// sub-query contain sequence function.
	tk.MustQuery("select nextval(seq), b from (select nextval(seq) as b, a from t1) t2").Check(testkit.Rows("227 228", "229 230", "231 232"))
	tk.MustInterDirc("insert into t2 select nextval(seq), b from (select nextval(seq) as b, a from t1) t2")
	tk.MustQuery("select * from t2").Check(testkit.Rows("233 234", "235 236", "237 238"))
	tk.MustInterDirc("delete from t2")

	// For union operator like select1 union select2, select1 and select2 will be executed parallelly,
	// so sequence function in both select are evaluated without order. Besides, the upper union operator
	// will gather results through multi worker goroutine parallelly leading the results unordered.
	// Cases like:
	// `select nextval(seq), a from t1 union select lastval(seq), a from t2`
	// `select nextval(seq), a from t1 union select nextval(seq), a from t2`
	// The executing order of nextval and lastval is implicit, don't make any assumptions on it.
}

// before this PR:
// single insert consume: 50.498672ms
// after this PR:
// single insert consume: 33.213615ms
// Notice: use go test -check.b Benchmarkxxx to test it.
func (s *testSequenceSuite) BenchmarkInsertCacheDefaultExpr(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("create causet t(a int default next value for seq)")
	allegrosql := "insert into t values "
	for i := 0; i < 1000; i++ {
		if i == 0 {
			allegrosql += "()"
		} else {
			allegrosql += ",()"
		}
	}
	c.ResetTimer()
	for i := 0; i < c.N; i++ {
		tk.MustInterDirc(allegrosql)
	}
}

func (s *testSequenceSuite) TestSequenceFunctionPrivilege(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	// Test sequence function privilege.
	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create causet t(a int default next value for seq)")
	tk.MustInterDirc("drop user if exists myuser@localhost")
	tk.MustInterDirc("create user myuser@localhost")

	tk1 := testkit.NewTestKit(c, s.causetstore)
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil), IsTrue)
	tk1.Se = se

	// grant the myuser the create access to the sequence.
	tk.MustInterDirc("grant insert on test.t to 'myuser'@'localhost'")

	// INSERT privilege required to use nextval.
	tk1.MustInterDirc("use test")
	err = tk1.QueryToErr("select nextval(seq)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[memex:1142]INSERT command denied to user 'myuser'@'localhost' for causet 'seq'")

	_, err = tk1.InterDirc("insert into t values()")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[memex:1142]INSERT command denied to user 'myuser'@'localhost' for causet 'seq'")

	// SELECT privilege required to use lastval.
	err = tk1.QueryToErr("select lastval(seq)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[memex:1142]SELECT command denied to user 'myuser'@'localhost' for causet 'seq'")

	// INSERT privilege required to use setval.
	err = tk1.QueryToErr("select setval(seq, 10)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[memex:1142]INSERT command denied to user 'myuser'@'localhost' for causet 'seq'")

	// grant the myuser the SELECT & UFIDelATE access to sequence seq.
	tk.MustInterDirc("grant SELECT, INSERT on test.seq to 'myuser'@'localhost'")

	// SELECT privilege required to use nextval.
	tk1.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	tk1.MustQuery("select lastval(seq)").Check(testkit.Rows("1"))
	tk1.MustQuery("select setval(seq, 10)").Check(testkit.Rows("10"))
	tk1.MustInterDirc("insert into t values()")

	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("drop sequence seq")
	tk.MustInterDirc("drop user myuser@localhost")
}

// Background: the newly added defCausumn in MilevaDB won't fill the known rows with specific
// sequence next value immediately. Every time MilevaDB select the data from storage, kvDB
// will fill the originDefaultValue to these incomplete rows (but not causetstore).
//
// In sequence case, every time filling these rows, kvDB should eval the sequence
// expr for len(incomplete rows) times, and combine these event data together. That
// means the select result is not always the same.
//
// However, the altered defCausumn with sequence as it's default value can work well.
// Because this defCausumn has already been added before the alter action, which also
// means originDefaultValue should be something but nil, so the back filling in kvDB
// can work well.
//
// The new altered sequence default value for this defCausumn only take effect on the
// subsequent inserted rows.
//
// So under current situation, MilevaDB will
// [1]: forbid the new added defCausumn has sequence as it's default value.
// [2]: allow the altered defCausumn with sequence as default value.
func (s *testSequenceSuite) TestSequenceDefaultLogic(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("drop sequence if exists seq")
	tk.MustInterDirc("drop causet if exists t")
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("create causet t(a int)")

	// Alter causet to use sequence as default value is ok.
	tk.MustInterDirc("insert into t values(-1),(-1),(-1)")
	tk.MustInterDirc("alter causet t add column b int default -1")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 -1", "-1 -1", "-1 -1"))
	tk.MustInterDirc("alter causet t modify column b int default next value for seq")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 -1", "-1 -1", "-1 -1"))
	tk.MustInterDirc("insert into t(a) values(-1),(-1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 -1", "-1 -1", "-1 -1", "-1 1", "-1 2"))

	// Add column to set sequence as default value is forbade.
	tk.MustInterDirc("drop sequence seq")
	tk.MustInterDirc("drop causet t")
	tk.MustInterDirc("create sequence seq")
	tk.MustInterDirc("create causet t(a int)")
	tk.MustInterDirc("insert into t values(-1),(-1),(-1)")
	tk.MustGetErrMsg("alter causet t add column b int default next value for seq", "[dbs:8230]Unsupported using sequence as default value in add column 'b'")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1", "-1", "-1"))
}

// Close issue #17945, sequence cache shouldn't be negative.
func (s *testSequenceSuite) TestSequenceCacheShouldNotBeNegative(c *C) {
	tk := testkit.NewTestKit(c, s.causetstore)
	tk.MustInterDirc("use test")

	tk.MustInterDirc("drop sequence if exists seq")
	_, err := tk.InterDirc("create sequence seq cache -1")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:4136]Sequence 'test.seq' values are conflicting")

	_, err = tk.InterDirc("create sequence seq cache 0")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:4136]Sequence 'test.seq' values are conflicting")

	// This will error because
	// 1: maxvalue = -1 by default
	// 2: minvalue = -9223372036854775807 by default
	// 3: increment = -9223372036854775807 by user
	// `seqInfo.CacheValue < (math.MaxInt64-absIncrement)/absIncrement` will
	// ensure there is enough value for one cache allocation at least.
	_, err = tk.InterDirc("create sequence seq INCREMENT -9223372036854775807 cache 1")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[dbs:4136]Sequence 'test.seq' values are conflicting")

	tk.MustInterDirc("create sequence seq cache 1")
}
