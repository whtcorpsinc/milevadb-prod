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

package printer

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testPrinterSuite{})

type testPrinterSuite struct {
}

func (s *testPrinterSuite) TestPrintResult(c *C) {
	defer testleak.AfterTest(c)()
	defcaus := []string{"defCaus1", "defCaus2", "defCaus3"}
	quantum := [][]string{{"11"}, {"21", "22", "23"}}
	result, ok := GetPrintResult(defcaus, quantum)
	c.Assert(ok, IsFalse)
	c.Assert(result, Equals, "")

	quantum = [][]string{{"11", "12", "13"}, {"21", "22", "23"}}
	expect := `
+------+------+------+
| defCaus1 | defCaus2 | defCaus3 |
+------+------+------+
| 11   | 12   | 13   |
| 21   | 22   | 23   |
+------+------+------+
`
	result, ok = GetPrintResult(defcaus, quantum)
	c.Assert(ok, IsTrue)
	c.Assert(result, Equals, expect[1:])

	quantum = nil
	result, ok = GetPrintResult(defcaus, quantum)
	c.Assert(ok, IsFalse)
	c.Assert(result, Equals, "")

	defcaus = nil
	result, ok = GetPrintResult(defcaus, quantum)
	c.Assert(ok, IsFalse)
	c.Assert(result, Equals, "")
}
