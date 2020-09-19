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

package defCauslate

import (
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var (
	_ = SerialSuites(&testDefCauslateSuite{})
)

type testDefCauslateSuite struct {
}

type compareBlock struct {
	Left   string
	Right  string
	Expect int
}

type keyBlock struct {
	Str    string
	Expect []byte
}

func testCompareBlock(block []compareBlock, defCauslate string, c *C) {
	for i, t := range block {
		comment := Commentf("%d %v %v", i, t.Left, t.Right)
		c.Assert(GetDefCauslator(defCauslate).Compare(t.Left, t.Right), Equals, t.Expect, comment)
	}
}

func testKeyBlock(block []keyBlock, defCauslate string, c *C) {
	for i, t := range block {
		comment := Commentf("%d %s", i, t.Str)
		c.Assert(GetDefCauslator(defCauslate).Key(t.Str), DeepEquals, t.Expect, comment)
	}
}

func (s *testDefCauslateSuite) TestBinDefCauslator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewDefCauslationEnabledForTest(false)
	compareBlock := []compareBlock{
		{"a", "b", -1},
		{"a", "A", 1},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"a", "a ", -1},
		{"a ", "a  ", -1},
		{"a\t", "a", 1},
	}
	keyBlock := []keyBlock{
		{"a", []byte{0x61}},
		{"A", []byte{0x41}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0,
			0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78}},
		{"a ", []byte{0x61, 0x20}},
		{"a", []byte{0x61}},
	}
	testCompareBlock(compareBlock, "utf8mb4_bin", c)
	testKeyBlock(keyBlock, "utf8mb4_bin", c)
}

func (s *testDefCauslateSuite) TestBinPaddingDefCauslator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewDefCauslationEnabledForTest(true)
	defer SetNewDefCauslationEnabledForTest(false)
	compareBlock := []compareBlock{
		{"a", "b", -1},
		{"a", "A", 1},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"a", "a ", 0},
		{"a ", "a  ", 0},
		{"a\t", "a", 1},
	}
	keyBlock := []keyBlock{
		{"a", []byte{0x61}},
		{"A", []byte{0x41}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61,
			0x72, 0x20, 0xf0, 0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78}},
		{"a ", []byte{0x61}},
		{"a", []byte{0x61}},
	}
	testCompareBlock(compareBlock, "utf8mb4_bin", c)
	testKeyBlock(keyBlock, "utf8mb4_bin", c)
}

func (s *testDefCauslateSuite) TestGeneralCIDefCauslator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewDefCauslationEnabledForTest(true)
	defer SetNewDefCauslationEnabledForTest(false)
	compareBlock := []compareBlock{
		{"a", "b", -1},
		{"a", "A", 0},
		{"À", "A", 0},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"😜", "😃", 0},
		{"a ", "a  ", 0},
		{"a\t", "a", 1},
	}
	keyBlock := []keyBlock{
		{"a", []byte{0x0, 0x41}},
		{"A", []byte{0x0, 0x41}},
		{"😃", []byte{0xff, 0xfd}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x0, 0x46, 0x0, 0x4f, 0x0, 0x4f, 0x0, 0x20, 0x0, 0xa9, 0x0, 0x20, 0x0,
			0x42, 0x0, 0x41, 0x0, 0x52, 0x0, 0x20, 0xff, 0xfd, 0x0, 0x20, 0x0, 0x42, 0x0, 0x41, 0x0, 0x5a, 0x0, 0x20, 0x26,
			0x3, 0x0, 0x20, 0x0, 0x51, 0x0, 0x55, 0x0, 0x58}},
		{string([]byte{0x88, 0xe6}), []byte{0xff, 0xfd, 0xff, 0xfd}},
		{"a ", []byte{0x0, 0x41}},
		{"a", []byte{0x0, 0x41}},
	}
	testCompareBlock(compareBlock, "utf8mb4_general_ci", c)
	testKeyBlock(keyBlock, "utf8mb4_general_ci", c)
}

func (s *testDefCauslateSuite) TestUnicodeCIDefCauslator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewDefCauslationEnabledForTest(true)
	defer SetNewDefCauslationEnabledForTest(false)

	compareBlock := []compareBlock{
		{"a", "b", -1},
		{"a", "A", 0},
		{"abc", "abc", 0},
		{"abc", "ab", 1},
		{"a", "a ", 0},
		{"a ", "a  ", 0},
		{"😜", "😃", 0},
		{"a\t", "a", 1},
		{"ß", "s", 1},
		{"ß", "ss", 0},
	}
	keyBlock := []keyBlock{
		{"a", []byte{0x0E, 0x33}},
		{"A", []byte{0x0E, 0x33}},
		{"ß", []byte{0x0F, 0xEA, 0x0F, 0xEA}},
		{"Foo © bar 𝌆 baz ☃ qux", []byte{0x0E, 0xB9, 0x0F, 0x82, 0x0F, 0x82, 0x02, 0x09, 0x02,
			0xC5, 0x02, 0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x0F, 0xC0, 0x02, 0x09, 0xFF, 0xFD, 0x02,
			0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x10, 0x6A, 0x02, 0x09, 0x06, 0xFF, 0x02, 0x09, 0x0F,
			0xB4, 0x10, 0x1F, 0x10, 0x5A}},
		{"a ", []byte{0x0E, 0x33}},
		{"ﷻ", []byte{0x13, 0x5E, 0x13, 0xAB, 0x02, 0x09, 0x13, 0x5E, 0x13, 0xAB, 0x13, 0x50, 0x13, 0xAB, 0x13, 0xB7}},
	}

	testCompareBlock(compareBlock, "utf8mb4_unicode_ci", c)
	testKeyBlock(keyBlock, "utf8mb4_unicode_ci", c)
}

func (s *testDefCauslateSuite) TestSetNewDefCauslateEnabled(c *C) {
	defer SetNewDefCauslationEnabledForTest(false)

	SetNewDefCauslationEnabledForTest(true)
	c.Assert(NewDefCauslationEnabled(), Equals, true)
}

func (s *testDefCauslateSuite) TestRewriteAndRestoreDefCauslationID(c *C) {
	SetNewDefCauslationEnabledForTest(true)
	c.Assert(RewriteNewDefCauslationIDIfNeeded(5), Equals, int32(-5))
	c.Assert(RewriteNewDefCauslationIDIfNeeded(-5), Equals, int32(-5))
	c.Assert(RestoreDefCauslationIDIfNeeded(-5), Equals, int32(5))
	c.Assert(RestoreDefCauslationIDIfNeeded(5), Equals, int32(5))

	SetNewDefCauslationEnabledForTest(false)
	c.Assert(RewriteNewDefCauslationIDIfNeeded(5), Equals, int32(5))
	c.Assert(RewriteNewDefCauslationIDIfNeeded(-5), Equals, int32(-5))
	c.Assert(RestoreDefCauslationIDIfNeeded(5), Equals, int32(5))
	c.Assert(RestoreDefCauslationIDIfNeeded(-5), Equals, int32(-5))
}

func (s *testDefCauslateSuite) TestGetDefCauslator(c *C) {
	defer testleak.AfterTest(c)()
	SetNewDefCauslationEnabledForTest(true)
	defer SetNewDefCauslationEnabledForTest(false)
	c.Assert(GetDefCauslator("binary"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("utf8mb4_bin"), FitsTypeOf, &binPaddingDefCauslator{})
	c.Assert(GetDefCauslator("utf8_bin"), FitsTypeOf, &binPaddingDefCauslator{})
	c.Assert(GetDefCauslator("utf8mb4_general_ci"), FitsTypeOf, &generalCIDefCauslator{})
	c.Assert(GetDefCauslator("utf8_general_ci"), FitsTypeOf, &generalCIDefCauslator{})
	c.Assert(GetDefCauslator("utf8mb4_unicode_ci"), FitsTypeOf, &unicodeCIDefCauslator{})
	c.Assert(GetDefCauslator("utf8_unicode_ci"), FitsTypeOf, &unicodeCIDefCauslator{})
	c.Assert(GetDefCauslator("default_test"), FitsTypeOf, &binPaddingDefCauslator{})
	c.Assert(GetDefCauslatorByID(63), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(46), FitsTypeOf, &binPaddingDefCauslator{})
	c.Assert(GetDefCauslatorByID(83), FitsTypeOf, &binPaddingDefCauslator{})
	c.Assert(GetDefCauslatorByID(45), FitsTypeOf, &generalCIDefCauslator{})
	c.Assert(GetDefCauslatorByID(33), FitsTypeOf, &generalCIDefCauslator{})
	c.Assert(GetDefCauslatorByID(224), FitsTypeOf, &unicodeCIDefCauslator{})
	c.Assert(GetDefCauslatorByID(192), FitsTypeOf, &unicodeCIDefCauslator{})
	c.Assert(GetDefCauslatorByID(9999), FitsTypeOf, &binPaddingDefCauslator{})

	SetNewDefCauslationEnabledForTest(false)
	c.Assert(GetDefCauslator("binary"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("utf8mb4_bin"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("utf8_bin"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("utf8mb4_general_ci"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("utf8_general_ci"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("utf8mb4_unicode_ci"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("utf8_unicode_ci"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslator("default_test"), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(63), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(46), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(83), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(45), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(33), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(224), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(192), FitsTypeOf, &binDefCauslator{})
	c.Assert(GetDefCauslatorByID(9999), FitsTypeOf, &binDefCauslator{})
}
