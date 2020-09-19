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

package filesort

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testFileSortSuite{})

type testFileSortSuite struct {
}

func nextRow(r *rand.Rand, keySize int, valSize int) (key []types.Causet, val []types.Causet, handle int64) {
	key = make([]types.Causet, keySize)
	for i := range key {
		key[i] = types.NewCauset(r.Int())
	}

	val = make([]types.Causet, valSize)
	for j := range val {
		val[j] = types.NewCauset(r.Int())
	}

	handle = r.Int63()
	return
}

func (s *testFileSortSuite) TestLessThan(c *C) {
	defer testleak.AfterTest(c)()

	sc := new(stmtctx.StatementContext)

	d0 := types.NewCauset(0)
	d1 := types.NewCauset(1)

	tblOneDeferredCauset := []struct {
		Arg1 []types.Causet
		Arg2 []types.Causet
		Arg3 []bool
		Ret  bool
	}{
		{[]types.Causet{d0}, []types.Causet{d0}, []bool{false}, false},
		{[]types.Causet{d0}, []types.Causet{d1}, []bool{false}, true},
		{[]types.Causet{d1}, []types.Causet{d0}, []bool{false}, false},
		{[]types.Causet{d0}, []types.Causet{d0}, []bool{true}, false},
		{[]types.Causet{d0}, []types.Causet{d1}, []bool{true}, false},
		{[]types.Causet{d1}, []types.Causet{d0}, []bool{true}, true},
	}

	for _, t := range tblOneDeferredCauset {
		ret, err := lessThan(sc, t.Arg1, t.Arg2, t.Arg3)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.Ret)
	}

	tblTwoDeferredCausets := []struct {
		Arg1 []types.Causet
		Arg2 []types.Causet
		Arg3 []bool
		Ret  bool
	}{
		{[]types.Causet{d0, d0}, []types.Causet{d1, d1}, []bool{false, false}, true},
		{[]types.Causet{d0, d1}, []types.Causet{d1, d1}, []bool{false, false}, true},
		{[]types.Causet{d0, d0}, []types.Causet{d1, d1}, []bool{false, false}, true},
		{[]types.Causet{d0, d0}, []types.Causet{d0, d1}, []bool{false, false}, true},
		{[]types.Causet{d0, d1}, []types.Causet{d0, d1}, []bool{false, false}, false},
		{[]types.Causet{d0, d1}, []types.Causet{d0, d0}, []bool{false, false}, false},
		{[]types.Causet{d1, d0}, []types.Causet{d0, d1}, []bool{false, false}, false},
		{[]types.Causet{d1, d1}, []types.Causet{d0, d1}, []bool{false, false}, false},
		{[]types.Causet{d1, d1}, []types.Causet{d0, d0}, []bool{false, false}, false},
	}

	for _, t := range tblTwoDeferredCausets {
		ret, err := lessThan(sc, t.Arg1, t.Arg2, t.Arg3)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testFileSortSuite) TestInMemory(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = r.Intn(2) == 0
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Causet
		key    []types.Causet
		tmFIDelir string
		ret    bool
	)

	tmFIDelir, err = ioutil.TemFIDelir("", "util_filesort_test")
	c.Assert(err, IsNil)

	fsBuilder := new(Builder)
	fs, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmFIDelir).Build()
	c.Assert(err, IsNil)
	defer fs.Close()

	nRows := r.Intn(bufSize-1) + 1 // random int in range [1, bufSize - 1]
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		c.Assert(err, IsNil)
	}

	pkey, _, _, err = fs.Output()
	c.Assert(err, IsNil)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		c.Assert(err, IsNil)
		ret, err = lessThan(sc, key, pkey, byDesc)
		c.Assert(err, IsNil)
		c.Assert(ret, IsFalse)
		pkey = key
	}
}

func (s *testFileSortSuite) TestMultipleFiles(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = r.Intn(2) == 0
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Causet
		key    []types.Causet
		tmFIDelir string
		ret    bool
	)

	tmFIDelir, err = ioutil.TemFIDelir("", "util_filesort_test")
	c.Assert(err, IsNil)

	fsBuilder := new(Builder)

	// Test for basic function.
	_, err = fsBuilder.Build()
	c.Assert(err.Error(), Equals, "StatementContext is nil")
	fsBuilder.SetSC(sc)
	_, err = fsBuilder.Build()
	c.Assert(err.Error(), Equals, "key size is not positive")
	fsBuilder.SetDesc(byDesc)
	_, err = fsBuilder.Build()
	c.Assert(err.Error(), Equals, "mismatch in key size and byDesc slice")
	fsBuilder.SetSchema(keySize, valSize)
	_, err = fsBuilder.Build()
	c.Assert(err.Error(), Equals, "buffer size is not positive")
	fsBuilder.SetBuf(bufSize)
	_, err = fsBuilder.Build()
	c.Assert(err.Error(), Equals, "tmFIDelir does not exist")
	fsBuilder.SetDir(tmFIDelir)

	fs, err = fsBuilder.SetWorkers(1).Build()
	c.Assert(err, IsNil)
	defer fs.Close()

	nRows := (r.Intn(bufSize) + 1) * (r.Intn(10) + 2)
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		c.Assert(err, IsNil)
	}

	pkey, _, _, err = fs.Output()
	c.Assert(err, IsNil)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		c.Assert(err, IsNil)
		ret, err = lessThan(sc, key, pkey, byDesc)
		c.Assert(err, IsNil)
		c.Assert(ret, IsFalse)
		pkey = key
	}
}

func (s *testFileSortSuite) TestMultipleWorkers(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := r.Intn(10) + 1 // random int in range [1, 10]
	valSize := r.Intn(20) + 1 // random int in range [1, 20]
	bufSize := 40             // hold up to 40 items per file
	byDesc := make([]bool, keySize)
	for i := range byDesc {
		byDesc[i] = r.Intn(2) == 0
	}

	var (
		err    error
		fs     *FileSorter
		pkey   []types.Causet
		key    []types.Causet
		tmFIDelir string
		ret    bool
	)

	tmFIDelir, err = ioutil.TemFIDelir("", "util_filesort_test")
	c.Assert(err, IsNil)

	fsBuilder := new(Builder)
	fs, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(4).SetDesc(byDesc).SetDir(tmFIDelir).Build()
	c.Assert(err, IsNil)
	defer fs.Close()

	nRows := (r.Intn(bufSize) + 1) * (r.Intn(10) + 2)
	for i := 1; i <= nRows; i++ {
		err = fs.Input(nextRow(r, keySize, valSize))
		c.Assert(err, IsNil)
	}

	pkey, _, _, err = fs.Output()
	c.Assert(err, IsNil)
	for i := 1; i < nRows; i++ {
		key, _, _, err = fs.Output()
		c.Assert(err, IsNil)
		ret, err = lessThan(sc, key, pkey, byDesc)
		c.Assert(err, IsNil)
		c.Assert(ret, IsFalse)
		pkey = key
	}
}

func (s *testFileSortSuite) TestClose(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := 2
	valSize := 2
	bufSize := 40
	byDesc := []bool{false, false}

	var (
		err     error
		fs0     *FileSorter
		fs1     *FileSorter
		tmFIDelir0 string
		tmFIDelir1 string
		errmsg  = "FileSorter has been closed"
	)

	// Prepare two FileSorter instances for tests
	fsBuilder := new(Builder)
	tmFIDelir0, err = ioutil.TemFIDelir("", "util_filesort_test")
	c.Assert(err, IsNil)
	fs0, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmFIDelir0).Build()
	c.Assert(err, IsNil)
	defer fs0.Close()

	tmFIDelir1, err = ioutil.TemFIDelir("", "util_filesort_test")
	c.Assert(err, IsNil)
	fs1, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmFIDelir1).Build()
	c.Assert(err, IsNil)
	defer fs1.Close()

	// 1. Close after some Input
	err = fs0.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	err = fs0.Close()
	c.Assert(err, IsNil)

	_, err = os.Stat(tmFIDelir0)
	c.Assert(os.IsNotExist(err), IsTrue)

	_, _, _, err = fs0.Output()
	c.Assert(err, ErrorMatches, errmsg)

	err = fs0.Input(nextRow(r, keySize, valSize))
	c.Assert(err, ErrorMatches, errmsg)

	err = fs0.Close()
	c.Assert(err, IsNil)

	// 2. Close after some Output
	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)
	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	_, _, _, err = fs1.Output()
	c.Assert(err, IsNil)

	err = fs1.Close()
	c.Assert(err, IsNil)

	_, err = os.Stat(tmFIDelir1)
	c.Assert(os.IsNotExist(err), IsTrue)

	_, _, _, err = fs1.Output()
	c.Assert(err, ErrorMatches, errmsg)

	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, ErrorMatches, errmsg)

	err = fs1.Close()
	c.Assert(err, IsNil)
}

func (s *testFileSortSuite) TestMismatchedUsage(c *C) {
	defer testleak.AfterTest(c)()

	seed := rand.NewSource(time.Now().UnixNano())
	r := rand.New(seed)

	sc := new(stmtctx.StatementContext)
	keySize := 2
	valSize := 2
	bufSize := 40
	byDesc := []bool{false, false}

	var (
		err    error
		fs0    *FileSorter
		fs1    *FileSorter
		key    []types.Causet
		tmFIDelir string
		errmsg = "call input after output"
	)

	// Prepare two FileSorter instances for tests
	fsBuilder := new(Builder)
	tmFIDelir, err = ioutil.TemFIDelir("", "util_filesort_test")
	c.Assert(err, IsNil)
	fs0, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmFIDelir).Build()
	c.Assert(err, IsNil)
	defer fs0.Close()

	tmFIDelir, err = ioutil.TemFIDelir("", "util_filesort_test")
	c.Assert(err, IsNil)
	fs1, err = fsBuilder.SetSC(sc).SetSchema(keySize, valSize).SetBuf(bufSize).SetWorkers(1).SetDesc(byDesc).SetDir(tmFIDelir).Build()
	c.Assert(err, IsNil)
	defer fs1.Close()

	// 1. call Output after fetched all rows
	err = fs0.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	key, _, _, err = fs0.Output()
	c.Assert(err, IsNil)
	c.Assert(key, NotNil)

	key, _, _, err = fs0.Output()
	c.Assert(err, IsNil)
	c.Assert(key, IsNil)

	// 2. call Input after Output
	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, IsNil)

	key, _, _, err = fs1.Output()
	c.Assert(err, IsNil)
	c.Assert(key, NotNil)

	err = fs1.Input(nextRow(r, keySize, valSize))
	c.Assert(err, ErrorMatches, errmsg)
}
