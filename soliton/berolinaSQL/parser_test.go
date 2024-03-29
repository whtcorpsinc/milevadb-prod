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

package BerolinaSQL

import (
	"testing"

	. "github.com/whtcorpsinc/check"
)

var _ = Suite(&testBerolinaSQLSuite{})

type testBerolinaSQLSuite struct {
}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testBerolinaSQLSuite) TestSpace(c *C) {
	okBlock := []struct {
		Times    int
		Input    string
		Expected string
	}{
		{0, " 1", "1"},
		{0, "1", "1"},
		{1, "     1", "1"},
		{2, "  1", "1"},
	}
	for _, test := range okBlock {
		rest, err := Space(test.Input, test.Times)
		c.Assert(rest, Equals, test.Expected)
		c.Assert(err, IsNil)
	}

	errBlock := []struct {
		Times int
		Input string
	}{
		{1, "1"},
		{2, " 1"},
	}

	for _, test := range errBlock {
		rest, err := Space(test.Input, test.Times)
		c.Assert(rest, Equals, test.Input)
		c.Assert(err, NotNil)
	}
}

func (s *testBerolinaSQLSuite) TestDigit(c *C) {
	okBlock := []struct {
		Times          int
		Input          string
		ExpectedDigits string
		ExpectedRest   string
	}{
		{0, "123abc", "123", "abc"},
		{1, "123abc", "123", "abc"},
		{2, "123 @)@)", "123", " @)@)"},
		{3, "456 121", "456", " 121"},
	}

	for _, test := range okBlock {
		digits, rest, err := Digit(test.Input, test.Times)
		c.Assert(digits, Equals, test.ExpectedDigits)
		c.Assert(rest, Equals, test.ExpectedRest)
		c.Assert(err, IsNil)
	}

	errBlock := []struct {
		Times int
		Input string
	}{
		{1, "int"},
		{2, "1int"},
		{3, "12 int"},
	}

	for _, test := range errBlock {
		digits, rest, err := Digit(test.Input, test.Times)
		c.Assert(digits, Equals, "")
		c.Assert(rest, Equals, test.Input)
		c.Assert(err, NotNil)
	}
}

func (s *testBerolinaSQLSuite) TestNumber(c *C) {
	okBlock := []struct {
		Input        string
		ExpectedNum  int
		ExpectedRest string
	}{
		{"123abc", 123, "abc"},
		{"123abc", 123, "abc"},
		{"123 @)@)", 123, " @)@)"},
		{"456 121", 456, " 121"},
	}
	for _, test := range okBlock {
		digits, rest, err := Number(test.Input)
		c.Assert(digits, Equals, test.ExpectedNum)
		c.Assert(rest, Equals, test.ExpectedRest)
		c.Assert(err, IsNil)
	}

	errBlock := []struct {
		Input string
	}{
		{"int"},
		{"abcint"},
		{"@)@)int"},
	}

	for _, test := range errBlock {
		digits, rest, err := Number(test.Input)
		c.Assert(digits, Equals, 0)
		c.Assert(rest, Equals, test.Input)
		c.Assert(err, NotNil)
	}
}

func (s *testBerolinaSQLSuite) TestCharAndAnyChar(c *C) {
	okBlock := []struct {
		Char     byte
		Input    string
		Expected string
	}{
		{'i', "int", "nt"},
		{'1', "1int", "int"},
		{'1', "12 int", "2 int"},
	}

	for _, test := range okBlock {
		rest, err := Char(test.Input, test.Char)
		c.Assert(rest, Equals, test.Expected)
		c.Assert(err, IsNil)

		rest, err = AnyChar(test.Input)
		c.Assert(rest, Equals, test.Expected)
		c.Assert(err, IsNil)
	}

	errBlock := []struct {
		Char  byte
		Input string
	}{
		{'i', "xint"},
		{'1', "x1int"},
		{'1', "x12 int"},
	}

	for _, test := range errBlock {
		rest, err := Char(test.Input, test.Char)
		c.Assert(rest, Equals, test.Input)
		c.Assert(err, NotNil)
	}
}
