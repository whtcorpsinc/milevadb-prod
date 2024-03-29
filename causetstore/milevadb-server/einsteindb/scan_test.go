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

package einsteindb

import (
	"bytes"
	"context"
	"fmt"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"go.uber.org/zap"
)

type testScanSuite struct {
	OneByOneSuite
	causetstore  *einsteindbStore
	recordPrefix []byte
	rowNums      []int
	ctx          context.Context
}

var _ = SerialSuites(&testScanSuite{})

func (s *testScanSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.causetstore = NewTestStore(c).(*einsteindbStore)
	s.recordPrefix = blockcodec.GenTableRecordPrefix(1)
	s.rowNums = append(s.rowNums, 1, scanBatchSize, scanBatchSize+1, scanBatchSize*3)
	// Avoid using async commit logic.
	s.ctx = context.WithValue(context.Background(), stochastikctx.ConnID, uint64(0))
}

func (s *testScanSuite) TearDownSuite(c *C) {
	txn := s.beginTxn(c)
	scanner, err := txn.Iter(s.recordPrefix, nil)
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit(s.ctx)
	c.Assert(err, IsNil)
	err = s.causetstore.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testScanSuite) beginTxn(c *C) *einsteindbTxn {
	txn, err := s.causetstore.Begin()
	c.Assert(err, IsNil)
	return txn.(*einsteindbTxn)
}

func (s *testScanSuite) TestScan(c *C) {
	check := func(c *C, scan ekv.Iterator, rowNum int, keyOnly bool) {
		for i := 0; i < rowNum; i++ {
			k := scan.Key()
			expectedKey := blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(i))
			if ok := bytes.Equal([]byte(k), []byte(expectedKey)); !ok {
				logutil.BgLogger().Error("bytes equal check fail",
					zap.Int("i", i),
					zap.Int("rowNum", rowNum),
					zap.Stringer("obtained key", k),
					zap.Stringer("obtained val", ekv.Key(scan.Value())),
					zap.Stringer("expected", expectedKey),
					zap.Bool("keyOnly", keyOnly))
			}
			c.Assert([]byte(k), BytesEquals, []byte(expectedKey))
			if !keyOnly {
				v := scan.Value()
				c.Assert(v, BytesEquals, genValueBytes(i))
			}
			// Because newScan return first item without calling scan.Next() just like go-hbase,
			// for-loop count will decrease 1.
			if i < rowNum-1 {
				scan.Next()
			}
		}
		scan.Next()
		c.Assert(scan.Valid(), IsFalse)
	}

	for _, rowNum := range s.rowNums {
		txn := s.beginTxn(c)
		for i := 0; i < rowNum; i++ {
			err := txn.Set(blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(i)), genValueBytes(i))
			c.Assert(err, IsNil)
		}
		err := txn.Commit(s.ctx)
		c.Assert(err, IsNil)
		mockTableID := int64(999)
		if rowNum > 123 {
			_, err = s.causetstore.SplitRegions(s.ctx, [][]byte{blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(123))}, false, &mockTableID)
			c.Assert(err, IsNil)
		}

		if rowNum > 456 {
			_, err = s.causetstore.SplitRegions(s.ctx, [][]byte{blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(456))}, false, &mockTableID)
			c.Assert(err, IsNil)
		}

		txn2 := s.beginTxn(c)
		val, err := txn2.Get(context.TODO(), blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(0)))
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, genValueBytes(0))
		// Test scan without upperBound
		scan, err := txn2.Iter(s.recordPrefix, nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, false)
		// Test scan with upperBound
		upperBound := rowNum / 2
		scan, err = txn2.Iter(s.recordPrefix, blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(upperBound)))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, false)

		txn3 := s.beginTxn(c)
		txn3.SetOption(ekv.KeyOnly, true)
		// Test scan without upper bound
		scan, err = txn3.Iter(s.recordPrefix, nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(s.recordPrefix, blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(upperBound)))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, true)

		// Restore KeyOnly to false
		txn3.SetOption(ekv.KeyOnly, false)
		scan, err = txn3.Iter(s.recordPrefix, nil)
		c.Assert(err, IsNil)
		check(c, scan, rowNum, true)
		// test scan with upper bound
		scan, err = txn3.Iter(s.recordPrefix, blockcodec.EncodeRecordKey(s.recordPrefix, ekv.IntHandle(upperBound)))
		c.Assert(err, IsNil)
		check(c, scan, upperBound, true)
	}
}

func genValueBytes(i int) []byte {
	var res = []byte{rowcodec.CodecVer}
	res = append(res, []byte(fmt.Sprintf("%d", i))...)
	return res
}
