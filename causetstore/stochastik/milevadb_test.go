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

package stochastik

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/auth"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
	"github.com/whtcorpsinc/milevadb/types"
)

func TestT(t *testing.T) {
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	CustomVerboseFlag = true
	SetSchemaLease(20 * time.Millisecond)
	TestingT(t)
}

var _ = Suite(&testMainSuite{})
var _ = SerialSuites(&testBootstrapSuite{})

type testMainSuite struct {
	dbName      string
	causetstore ekv.CausetStorage
	dom         *petri.Petri
}

func (s *testMainSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.dbName = "test_main_db"
	s.causetstore = newStore(c, s.dbName)
	dom, err := BootstrapStochastik(s.causetstore)
	c.Assert(err, IsNil)
	s.dom = dom
}

func (s *testMainSuite) TearDownSuite(c *C) {
	defer testleak.AfterTest(c)()
	s.dom.Close()
	err := s.causetstore.Close()
	c.Assert(err, IsNil)
	removeStore(c, s.dbName)
}

func (s *testMainSuite) TestSysStochastikPoolGoroutineLeak(c *C) {
	causetstore, dom := newStoreWithBootstrap(c, s.dbName+"goroutine_leak")
	defer causetstore.Close()
	defer dom.Close()
	se, err := createStochastik(causetstore)
	c.Assert(err, IsNil)

	// Test an issue that sysStochastikPool doesn't call stochastik's Close, cause
	// asyncGetTSWorker goroutine leak.
	count := 200
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(se *stochastik) {
			_, _, err := se.InterDircRestrictedALLEGROSQL("select * from allegrosql.user limit 1")
			c.Assert(err, IsNil)
			wg.Done()
		}(se)
	}
	wg.Wait()
}

func (s *testMainSuite) TestParseErrorWarn(c *C) {
	ctx := embedded.MockContext()

	nodes, err := Parse(ctx, "select /*+ adf */ 1")
	c.Assert(err, IsNil)
	c.Assert(len(nodes), Equals, 1)
	c.Assert(len(ctx.GetStochastikVars().StmtCtx.GetWarnings()), Equals, 1)

	_, err = Parse(ctx, "select")
	c.Assert(err, NotNil)
}

func newStore(c *C, dbPath string) ekv.CausetStorage {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	return causetstore
}

func newStoreWithBootstrap(c *C, dbPath string) (ekv.CausetStorage, *petri.Petri) {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	dom, err := BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	return causetstore, dom
}

var testConnID uint64

func newStochastik(c *C, causetstore ekv.CausetStorage, dbName string) Stochastik {
	se, err := CreateStochastik4Test(causetstore)
	id := atomic.AddUint64(&testConnID, 1)
	se.SetConnectionID(id)
	c.Assert(err, IsNil)
	se.Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, []byte("012345678901234567890"))
	mustInterDircALLEGROSQL(c, se, "create database if not exists "+dbName)
	mustInterDircALLEGROSQL(c, se, "use "+dbName)
	return se
}

func removeStore(c *C, dbPath string) {
	os.RemoveAll(dbPath)
}

func exec(se Stochastik, allegrosql string, args ...interface{}) (sqlexec.RecordSet, error) {
	ctx := context.Background()
	if len(args) == 0 {
		rs, err := se.InterDircute(ctx, allegrosql)
		if err == nil && len(rs) > 0 {
			return rs[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := se.PrepareStmt(allegrosql)
	if err != nil {
		return nil, err
	}
	params := make([]types.Causet, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewCauset(args[i])
	}
	rs, err := se.InterDircutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func mustInterDircALLEGROSQL(c *C, se Stochastik, allegrosql string, args ...interface{}) sqlexec.RecordSet {
	rs, err := exec(se, allegrosql, args...)
	c.Assert(err, IsNil)
	return rs
}

func match(c *C, event []types.Causet, expected ...interface{}) {
	c.Assert(len(event), Equals, len(expected))
	for i := range event {
		got := fmt.Sprintf("%v", event[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
	}
}

func (s *testMainSuite) TestKeysNeedLock(c *C) {
	rowKey := blockcodec.EncodeRowKeyWithHandle(1, ekv.IntHandle(1))
	indexKey := blockcodec.EncodeIndexSeekKey(1, 1, []byte{1})
	uniqueValue := make([]byte, 8)
	uniqueUntouched := append(uniqueValue, '1')
	nonUniqueVal := []byte{'0'}
	nonUniqueUntouched := []byte{'1'}
	var deleteVal []byte
	rowVal := []byte{'a', 'b', 'c'}
	tests := []struct {
		key  []byte
		val  []byte
		need bool
	}{
		{rowKey, rowVal, true},
		{rowKey, deleteVal, true},
		{indexKey, nonUniqueVal, false},
		{indexKey, nonUniqueUntouched, false},
		{indexKey, uniqueValue, true},
		{indexKey, uniqueUntouched, false},
		{indexKey, deleteVal, false},
	}
	for _, tt := range tests {
		c.Assert(keyNeedToLock(tt.key, tt.val, 0), Equals, tt.need)
	}
	flag := ekv.KeyFlags(1)
	c.Assert(flag.HasPresumeKeyNotExists(), IsTrue)
	c.Assert(keyNeedToLock(indexKey, deleteVal, flag), IsTrue)
}
