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

package soliton_test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

const (
	startIndex = 0
	testCount  = 12
	testPow    = 10
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testPrefixSuite{})

type testPrefixSuite struct {
	s ekv.CausetStorage
}

func (s *testPrefixSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.s = causetstore

}

func (s *testPrefixSuite) TearDownSuite(c *C) {
	err := s.s.Close()
	c.Assert(err, IsNil)
	testleak.AfterTest(c)()
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%d", n))
}

type MockContext struct {
	prefix int
	values map[fmt.Stringer]interface{}
	ekv.CausetStorage
	txn ekv.Transaction
}

func (c *MockContext) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

func (c *MockContext) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

func (c *MockContext) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

func (c *MockContext) GetTxn(forceNew bool) (ekv.Transaction, error) {
	var err error
	c.txn, err = c.Begin()
	if err != nil {
		return nil, err
	}

	return c.txn, nil
}

func (c *MockContext) fillTxn() error {
	if c.txn == nil {
		return nil
	}

	var err error
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i + (c.prefix * testPow))
		err = c.txn.Set(val, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *MockContext) CommitTxn() error {
	if c.txn == nil {
		return nil
	}
	return c.txn.Commit(context.Background())
}

func (s *testPrefixSuite) TestPrefix(c *C) {
	ctx := &MockContext{10000000, make(map[fmt.Stringer]interface{}), s.s, nil}
	ctx.fillTxn()
	txn, err := ctx.GetTxn(false)
	c.Assert(err, IsNil)
	err = soliton.DelKeyWithPrefix(txn, encodeInt(ctx.prefix))
	c.Assert(err, IsNil)
	err = ctx.CommitTxn()
	c.Assert(err, IsNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)
	k := []byte("key100jfowi878230")
	err = txn.Set(k, []byte(`val32dfaskli384757^*&%^`))
	c.Assert(err, IsNil)
	err = soliton.ScanMetaWithPrefix(txn, k, func(ekv.Key, []byte) bool {
		return true
	})
	c.Assert(err, IsNil)
	err = soliton.ScanMetaWithPrefix(txn, k, func(ekv.Key, []byte) bool {
		return false
	})
	c.Assert(err, IsNil)
	err = soliton.DelKeyWithPrefix(txn, []byte("key"))
	c.Assert(err, IsNil)
	_, err = txn.Get(context.TODO(), k)
	c.Assert(terror.ErrorEqual(ekv.ErrNotExist, err), IsTrue)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testPrefixSuite) TestPrefixFilter(c *C) {
	rowKey := []byte(`test@#$%l(le[0]..prefix) 2uio`)
	rowKey[8] = 0x00
	rowKey[9] = 0x00
	f := soliton.RowKeyPrefixFilter(rowKey)
	b := f(append(rowKey, []byte("akjdf3*(34")...))
	c.Assert(b, IsFalse)
	buf := f([]byte("sjfkdlsaf"))
	c.Assert(buf, IsTrue)
}
