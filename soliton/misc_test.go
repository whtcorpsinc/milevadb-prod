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

package soliton

import (
	"bytes"
	"crypto/x509/pkix"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/fastrand"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

var _ = Suite(&testMiscSuite{})

type testMiscSuite struct {
}

func (s *testMiscSuite) SetUpSuite(c *C) {
}

func (s *testMiscSuite) TearDownSuite(c *C) {
}

func (s *testMiscSuite) TestRunWithRetry(c *C) {
	defer testleak.AfterTest(c)()
	// Run succ.
	cnt := 0
	err := RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 2 {
			return true, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 2)

	// Run failed.
	cnt = 0
	err = RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 4 {
			return true, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, NotNil)
	c.Assert(cnt, Equals, 3)

	// Run failed.
	cnt = 0
	err = RunWithRetry(3, 1, func() (bool, error) {
		cnt++
		if cnt < 2 {
			return false, errors.New("err")
		}
		return true, nil
	})
	c.Assert(err, NotNil)
	c.Assert(cnt, Equals, 1)
}

func (s *testMiscSuite) TestCompatibleParseGCTime(c *C) {
	values := []string{
		"20181218-19:53:37 +0800 CST",
		"20181218-19:53:37 +0800 MST",
		"20181218-19:53:37 +0800 FOO",
		"20181218-19:53:37 +0800 +08",
		"20181218-19:53:37 +0800",
		"20181218-19:53:37 +0800 ",
		"20181218-11:53:37 +0000",
	}

	invalidValues := []string{
		"",
		" ",
		"foo",
		"20181218-11:53:37",
		"20181218-19:53:37 +0800CST",
		"20181218-19:53:37 +0800 FOO BAR",
		"20181218-19:53:37 +0800FOOOOOOO BAR",
		"20181218-19:53:37 ",
	}

	expectedTime := time.Date(2020, 12, 18, 11, 53, 37, 0, time.UTC)
	expectedTimeFormatted := "20181218-19:53:37 +0800"

	beijing, err := time.LoadLocation("Asia/Shanghai")
	c.Assert(err, IsNil)

	for _, value := range values {
		t, err := CompatibleParseGCTime(value)
		c.Assert(err, IsNil)
		c.Assert(t.Equal(expectedTime), Equals, true)

		formatted := t.In(beijing).Format(GCTimeFormat)
		c.Assert(formatted, Equals, expectedTimeFormatted)
	}

	for _, value := range invalidValues {
		_, err := CompatibleParseGCTime(value)
		c.Assert(err, NotNil)
	}
}

func (s *testMiscSuite) TestX509NameParseMatch(c *C) {
	check := pkix.Name{
		Names: []pkix.AttributeTypeAndValue{
			MockPkixAttribute(Country, "SE"),
			MockPkixAttribute(Province, "Stockholm2"),
			MockPkixAttribute(Locality, "Stockholm"),
			MockPkixAttribute(Organization, "MyALLEGROSQL demo client certificate"),
			MockPkixAttribute(OrganizationalUnit, "testUnit"),
			MockPkixAttribute(CommonName, "client"),
			MockPkixAttribute(Email, "client@example.com"),
		},
	}
	c.Assert(X509NameOnline(check), Equals, "/C=SE/ST=Stockholm2/L=Stockholm/O=MyALLEGROSQL demo client certificate/OU=testUnit/CN=client/emailAddress=client@example.com")
	check = pkix.Name{}
	c.Assert(X509NameOnline(check), Equals, "")
}

func (s *testMiscSuite) TestBasicFunc(c *C) {
	// Test for GetStack.
	b := GetStack()
	c.Assert(len(b) < 4096, IsTrue)

	// Test for WithRecovery.
	var recover interface{}
	WithRecovery(func() {
		panic("test")
	}, func(r interface{}) {
		recover = r
	})
	c.Assert(recover, Equals, "test")

	// Test for SyntaxError.
	c.Assert(SyntaxError(nil), IsNil)
	c.Assert(terror.ErrorEqual(SyntaxError(errors.New("test")), BerolinaSQL.ErrParse), IsTrue)
	c.Assert(terror.ErrorEqual(SyntaxError(BerolinaSQL.ErrSyntax.GenWithStackByArgs()), BerolinaSQL.ErrSyntax), IsTrue)

	// Test for SyntaxWarn.
	c.Assert(SyntaxWarn(nil), IsNil)
	c.Assert(terror.ErrorEqual(SyntaxWarn(errors.New("test")), BerolinaSQL.ErrParse), IsTrue)

	// Test for ProcessInfo.
	pi := ProcessInfo{
		ID:      1,
		User:    "test",
		Host:    "www",
		EDB:      "EDB",
		Command: allegrosql.ComSleep,
		Plan:    nil,
		Time:    time.Now(),
		State:   3,
		Info:    "test",
		StmtCtx: &stmtctx.StatementContext{
			MemTracker: memory.NewTracker(-1, -1),
		},
	}
	event := pi.ToRowForShow(false)
	row2 := pi.ToRowForShow(true)
	c.Assert(event, DeepEquals, row2)
	c.Assert(len(event), Equals, 8)
	c.Assert(event[0], Equals, pi.ID)
	c.Assert(event[1], Equals, pi.User)
	c.Assert(event[2], Equals, pi.Host)
	c.Assert(event[3], Equals, pi.EDB)
	c.Assert(event[4], Equals, "Sleep")
	c.Assert(event[5], Equals, uint64(0))
	c.Assert(event[6], Equals, "in transaction; autocommit")
	c.Assert(event[7], Equals, "test")

	row3 := pi.ToRow(time.UTC)
	c.Assert(row3[:8], DeepEquals, event)
	c.Assert(row3[9], Equals, int64(0))

	// Test for RandomBuf.
	buf := fastrand.Buf(5)
	c.Assert(len(buf), Equals, 5)
	c.Assert(bytes.Contains(buf, []byte("$")), IsFalse)
	c.Assert(bytes.Contains(buf, []byte{0}), IsFalse)
}

func (*testMiscSuite) TestToPB(c *C) {
	defCausumn := &perceptron.DeferredCausetInfo{
		ID:           1,
		Name:         perceptron.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	defCausumn.DefCauslate = "utf8mb4_general_ci"

	defCausumn2 := &perceptron.DeferredCausetInfo{
		ID:           1,
		Name:         perceptron.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	defCausumn2.DefCauslate = "utf8mb4_bin"

	c.Assert(DeferredCausetToProto(defCausumn).String(), Equals, "defCausumn_id:1 defCauslation:45 defCausumnLen:-1 decimal:-1 ")
	c.Assert(DeferredCausetsToProto([]*perceptron.DeferredCausetInfo{defCausumn, defCausumn2}, false)[0].String(), Equals, "defCausumn_id:1 defCauslation:45 defCausumnLen:-1 decimal:-1 ")
}
