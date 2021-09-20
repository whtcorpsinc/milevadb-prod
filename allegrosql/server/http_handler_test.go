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

package server

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/allegrosql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/failpoint"
	zaplog "github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/helper"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/rowcodec"
	"github.com/whtcorpsinc/milevadb/soliton/versioninfo"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

type basicHTTPHandlerTestSuite struct {
	*testServerClient
	server      *Server
	causetstore ekv.CausetStorage
	petri       *petri.Petri
	milevadbdrv *MilevaDBDriver
}

type HTTPHandlerTestSuite struct {
	*basicHTTPHandlerTestSuite
}

type HTTPHandlerTestSerialSuite struct {
	*basicHTTPHandlerTestSuite
}

var _ = Suite(&HTTPHandlerTestSuite{&basicHTTPHandlerTestSuite{}})

var _ = SerialSuites(&HTTPHandlerTestSerialSuite{&basicHTTPHandlerTestSuite{}})

func (ts *basicHTTPHandlerTestSuite) SetUpSuite(c *C) {
	ts.testServerClient = newTestServerClient()
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRange(c *C) {
	sBlockID := int64(3)
	sIndex := int64(11)
	eBlockID := int64(9)
	recordID := int64(133)
	indexValues := []types.Causet{
		types.NewIntCauset(100),
		types.NewBytesCauset([]byte("foobar")),
		types.NewFloat64Causet(-100.25),
	}
	expectIndexValues := make([]string, 0, len(indexValues))
	for _, v := range indexValues {
		str, err := v.ToString()
		if err != nil {
			str = fmt.Sprintf("%d-%v", v.HoTT(), v.GetValue())
		}
		expectIndexValues = append(expectIndexValues, str)
	}
	encodedValue, err := codec.EncodeKey(&stmtctx.StatementContext{TimeZone: time.Local}, nil, indexValues...)
	c.Assert(err, IsNil)

	startKey := blockcodec.EncodeIndexSeekKey(sBlockID, sIndex, encodedValue)
	recordPrefix := blockcodec.GenBlockRecordPrefix(eBlockID)
	endKey := blockcodec.EncodeRecordKey(recordPrefix, ekv.IntHandle(recordID))

	region := &einsteindb.KeyLocation{
		Region:   einsteindb.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IndexID, Equals, sIndex)
	c.Assert(r.First.IsRecord, IsFalse)
	c.Assert(r.First.RecordID, Equals, int64(0))
	c.Assert(r.First.IndexValues, DeepEquals, expectIndexValues)
	c.Assert(r.Last.RecordID, Equals, recordID)
	c.Assert(r.Last.IndexValues, IsNil)

	testCases := []struct {
		blockID int64
		indexID int64
		isCover bool
	}{
		{2, 0, false},
		{3, 0, true},
		{9, 0, true},
		{10, 0, false},
		{2, 10, false},
		{3, 10, false},
		{3, 11, true},
		{3, 20, true},
		{9, 10, true},
		{10, 1, false},
	}
	for _, t := range testCases {
		var f *helper.FrameItem
		if t.indexID == 0 {
			f = r.GetRecordFrame(t.blockID, "", "", false)
		} else {
			f = r.GetIndexFrame(t.blockID, t.indexID, "", "", "")
		}
		if t.isCover {
			c.Assert(f, NotNil)
		} else {
			c.Assert(f, IsNil)
		}
	}
}

func (ts *HTTPHandlerTestSuite) TestRegionCommonHandleRange(c *C) {
	sBlockID := int64(3)
	indexValues := []types.Causet{
		types.NewIntCauset(100),
		types.NewBytesCauset([]byte("foobar")),
		types.NewFloat64Causet(-100.25),
	}
	expectIndexValues := make([]string, 0, len(indexValues))
	for _, v := range indexValues {
		str, err := v.ToString()
		if err != nil {
			str = fmt.Sprintf("%d-%v", v.HoTT(), v.GetValue())
		}
		expectIndexValues = append(expectIndexValues, str)
	}
	encodedValue, err := codec.EncodeKey(&stmtctx.StatementContext{TimeZone: time.Local}, nil, indexValues...)
	c.Assert(err, IsNil)

	startKey := blockcodec.EncodeRowKey(sBlockID, encodedValue)

	region := &einsteindb.KeyLocation{
		Region:   einsteindb.RegionVerID{},
		StartKey: startKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsTrue)
	c.Assert(r.First.RecordID, Equals, int64(0))
	c.Assert(r.First.IndexValues, DeepEquals, expectIndexValues)
	c.Assert(r.First.IndexName, Equals, "PRIMARY")
	c.Assert(r.Last.RecordID, Equals, int64(0))
	c.Assert(r.Last.IndexValues, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRangeWithEndNoLimit(c *C) {
	sBlockID := int64(15)
	startKey := blockcodec.GenBlockRecordPrefix(sBlockID)
	endKey := []byte("z_aaaaafdfd")
	region := &einsteindb.KeyLocation{
		Region:   einsteindb.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsTrue)
	c.Assert(r.Last.IsRecord, IsTrue)
	c.Assert(r.GetRecordFrame(300, "", "", false), NotNil)
	c.Assert(r.GetIndexFrame(200, 100, "", "", ""), NotNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRangeWithStartNoLimit(c *C) {
	eBlockID := int64(9)
	startKey := []byte("m_aaaaafdfd")
	endKey := blockcodec.GenBlockRecordPrefix(eBlockID)
	region := &einsteindb.KeyLocation{
		Region:   einsteindb.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsFalse)
	c.Assert(r.Last.IsRecord, IsTrue)
	c.Assert(r.GetRecordFrame(3, "", "", false), NotNil)
	c.Assert(r.GetIndexFrame(8, 1, "", "", ""), NotNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionsAPI(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	ts.prepareData(c)
	resp, err := ts.fetchStatus("/blocks/milevadb/t/regions")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	defer resp.Body.Close()
	causetDecoder := json.NewCausetDecoder(resp.Body)

	var data BlockRegions
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(len(data.RecordRegions) > 0, IsTrue)

	// list region
	for _, region := range data.RecordRegions {
		c.Assert(ts.regionContainsBlock(c, region.ID, data.BlockID), IsTrue)
	}
}

func (ts *HTTPHandlerTestSuite) TestRegionsAPIForClusterIndex(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	ts.prepareData(c)
	resp, err := ts.fetchStatus("/blocks/milevadb/t/regions")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	defer resp.Body.Close()
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var data BlockRegions
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(len(data.RecordRegions) > 0, IsTrue)
	// list region
	for _, region := range data.RecordRegions {
		resp, err := ts.fetchStatus(fmt.Sprintf("/regions/%d", region.ID))
		c.Assert(err, IsNil)
		c.Assert(resp.StatusCode, Equals, http.StatusOK)
		causetDecoder := json.NewCausetDecoder(resp.Body)
		var data RegionDetail
		err = causetDecoder.Decode(&data)
		c.Assert(err, IsNil)
		frameCnt := 0
		for _, f := range data.Frames {
			if f.DBName == "milevadb" && f.BlockName == "t" {
				frameCnt++
			}
		}
		// Primary index is as the record frame, so frame count is 1.
		c.Assert(frameCnt, Equals, 1)
		c.Assert(resp.Body.Close(), IsNil)
	}
}

func (ts *HTTPHandlerTestSuite) regionContainsBlock(c *C, regionID uint64, blockID int64) bool {
	resp, err := ts.fetchStatus(fmt.Sprintf("/regions/%d", regionID))
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	defer resp.Body.Close()
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var data RegionDetail
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	for _, index := range data.Frames {
		if index.BlockID == blockID {
			return true
		}
	}
	return false
}

func (ts *HTTPHandlerTestSuite) TestListBlockRegions(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	ts.prepareData(c)
	// Test list causet regions with error
	resp, err := ts.fetchStatus("/blocks/fdsfds/aaa/regions")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/blocks/milevadb/pt/regions")
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	var data []*BlockRegions
	dec := json.NewCausetDecoder(resp.Body)
	err = dec.Decode(&data)
	c.Assert(err, IsNil)

	region := data[1]
	_, err = ts.fetchStatus(fmt.Sprintf("/regions/%d", region.BlockID))
	c.Assert(err, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestGetRegionByIDWithError(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/regions/xxx")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	defer resp.Body.Close()
}

func (ts *HTTPHandlerTestSuite) TestBinlogRecover(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err := ts.fetchStatus("/binlog/recover")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Invalid operation will use the default operation.
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = ts.fetchStatus("/binlog/recover?op=abc")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	binloginfo.AddOneSkippedCommitter()
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)
	binloginfo.RemoveOneSkippedCommitter()

	binloginfo.AddOneSkippedCommitter()
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(1))
	resp, err = ts.fetchStatus("/binlog/recover?op=reset")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(0))

	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Only the first should work.
	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait&op=reset")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	resp, err = ts.fetchStatus("/binlog/recover?op=status")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

func (ts *HTTPHandlerTestSuite) TestRegionsFromMeta(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/regions/spacetime")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	// Verify the resp body.
	causetDecoder := json.NewCausetDecoder(resp.Body)
	spacetimes := make([]RegionMeta, 0)
	err = causetDecoder.Decode(&spacetimes)
	c.Assert(err, IsNil)
	for _, spacetime := range spacetimes {
		c.Assert(spacetime.ID != 0, IsTrue)
	}

	// test no panic
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/server/errGetRegionByIDEmpty", `return(true)`), IsNil)
	resp1, err := ts.fetchStatus("/regions/spacetime")
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	c.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/server/errGetRegionByIDEmpty"), IsNil)
}

func (ts *basicHTTPHandlerTestSuite) startServer(c *C) {
	var err error
	ts.causetstore, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	ts.petri, err = stochastik.BootstrapStochastik(ts.causetstore)
	c.Assert(err, IsNil)
	ts.milevadbdrv = NewMilevaDBDriver(ts.causetstore)

	cfg := newTestConfig()
	cfg.CausetStore = "einsteindb"
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	ts.port = getPortFromTCPAddr(server.listener.Addr())
	ts.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ts.server = server
	go server.Run()
	ts.waitUntilServerOnline()
}

func getPortFromTCPAddr(addr net.Addr) uint {
	return uint(addr.(*net.TCPAddr).Port)
}

func (ts *basicHTTPHandlerTestSuite) stopServer(c *C) {
	if ts.petri != nil {
		ts.petri.Close()
	}
	if ts.causetstore != nil {
		ts.causetstore.Close()
	}
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *basicHTTPHandlerTestSuite) prepareData(c *C) {
	EDB, err := allegrosql.Open("allegrosql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()
	dbt := &DBTest{c, EDB}

	dbt.mustInterDirc("create database milevadb;")
	dbt.mustInterDirc("use milevadb;")
	dbt.mustInterDirc("create causet milevadb.test (a int auto_increment primary key, b varchar(20));")
	dbt.mustInterDirc("insert milevadb.test values (1, 1);")
	txn1, err := dbt.EDB.Begin()
	c.Assert(err, IsNil)
	_, err = txn1.InterDirc("uFIDelate milevadb.test set b = b + 1 where a = 1;")
	c.Assert(err, IsNil)
	_, err = txn1.InterDirc("insert milevadb.test values (2, 2);")
	c.Assert(err, IsNil)
	_, err = txn1.InterDirc("insert milevadb.test (a) values (3);")
	c.Assert(err, IsNil)
	_, err = txn1.InterDirc("insert milevadb.test values (4, '');")
	c.Assert(err, IsNil)
	err = txn1.Commit()
	c.Assert(err, IsNil)
	dbt.mustInterDirc("alter causet milevadb.test add index idx1 (a, b);")
	dbt.mustInterDirc("alter causet milevadb.test add unique index idx2 (a, b);")

	dbt.mustInterDirc(`create causet milevadb.pt (a int primary key, b varchar(20), key idx(a, b))
partition by range (a)
(partition p0 values less than (256),
 partition p1 values less than (512),
 partition p2 values less than (1024))`)

	txn2, err := dbt.EDB.Begin()
	c.Assert(err, IsNil)
	txn2.InterDirc("insert into milevadb.pt values (42, '123')")
	txn2.InterDirc("insert into milevadb.pt values (256, 'b')")
	txn2.InterDirc("insert into milevadb.pt values (666, 'def')")
	err = txn2.Commit()
	c.Assert(err, IsNil)

	dbt.mustInterDirc("set @@milevadb_enable_clustered_index = 1")
	dbt.mustInterDirc("drop causet if exists t")
	dbt.mustInterDirc("create causet t (a double, b varchar(20), c int, primary key(a,b))")
	dbt.mustInterDirc("insert into t values(1.1,'111',1),(2.2,'222',2)")
}

func decodeKeyMvcc(closer io.ReadCloser, c *C, valid bool) {
	causetDecoder := json.NewCausetDecoder(closer)
	var data mvccKV
	err := causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	if valid {
		c.Assert(data.Value.Info, NotNil)
		c.Assert(len(data.Value.Info.Writes), Greater, 0)
	} else {
		c.Assert(data.Value.Info.Lock, IsNil)
		c.Assert(data.Value.Info.Writes, IsNil)
		c.Assert(data.Value.Info.Values, IsNil)
	}
}

func (ts *HTTPHandlerTestSuite) TestGetBlockMVCC(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	resp, err := ts.fetchStatus(fmt.Sprintf("/mvcc/key/milevadb/test/1"))
	c.Assert(err, IsNil)
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var data mvccKV
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Value, NotNil)
	info := data.Value.Info
	c.Assert(info, NotNil)
	c.Assert(len(info.Writes), Greater, 0)

	// TODO: EntangledStore will not return Op_Lock.
	// Use this workaround to support two backend, we can remove this replog after deprecated mockeinsteindb.
	var startTs uint64
	for _, w := range info.Writes {
		if w.Type == ekvrpcpb.Op_Lock {
			continue
		}
		startTs = w.StartTs
		break
	}

	resp, err = ts.fetchStatus(fmt.Sprintf("/mvcc/txn/%d/milevadb/test", startTs))
	c.Assert(err, IsNil)
	var p2 mvccKV
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&p2)
	c.Assert(err, IsNil)

	for i, expect := range info.Values {
		v2 := p2.Value.Info.Values[i].Value
		c.Assert(v2, BytesEquals, expect.Value)
	}

	hexKey := p2.Key
	resp, err = ts.fetchStatus("/mvcc/hex/" + hexKey)
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	var data2 mvccKV
	err = causetDecoder.Decode(&data2)
	c.Assert(err, IsNil)
	c.Assert(data2, DeepEquals, data)

	resp, err = ts.fetchStatus(fmt.Sprintf("/mvcc/key/milevadb/test/1?decode=true"))
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	var data3 map[string]interface{}
	err = causetDecoder.Decode(&data3)
	c.Assert(err, IsNil)
	c.Assert(data3["key"], NotNil)
	c.Assert(data3["info"], NotNil)
	c.Assert(data3["data"], NotNil)
	c.Assert(data3["decode_error"], IsNil)

	resp, err = ts.fetchStatus("/mvcc/key/milevadb/pt(p0)/42?decode=true")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	causetDecoder = json.NewCausetDecoder(resp.Body)
	var data4 map[string]interface{}
	err = causetDecoder.Decode(&data4)
	c.Assert(err, IsNil)
	c.Assert(data4["key"], NotNil)
	c.Assert(data4["info"], NotNil)
	c.Assert(data4["data"], NotNil)
	c.Assert(data4["decode_error"], IsNil)
}

func (ts *HTTPHandlerTestSuite) TestGetMVCCNotFound(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus(fmt.Sprintf("/mvcc/key/milevadb/test/1234"))
	c.Assert(err, IsNil)
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var data mvccKV
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Value.Info.Lock, IsNil)
	c.Assert(data.Value.Info.Writes, IsNil)
	c.Assert(data.Value.Info.Values, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestTiFlashReplica(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	EDB, err := allegrosql.Open("allegrosql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()
	dbt := &DBTest{c, EDB}

	defer func(originGC bool) {
		if originGC {
			dbs.EmulatorGCEnable()
		} else {
			dbs.EmulatorGCDisable()
		}
	}(dbs.IsEmulatorGCEnable())

	// Disable emulator GC.
	// Otherwise emulator GC will delete causet record as soon as possible after execute drop causet DBS.
	dbs.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointALLEGROSQL := `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('einsteindb_gc_safe_point', '%[1]s', ''),('einsteindb_gc_enable','true','')
			       ON DUPLICATE KEY
			       UFIDelATE variable_value = '%[1]s'`
	// Set GC safe point and enable GC.
	dbt.mustInterDirc(fmt.Sprintf(safePointALLEGROSQL, timeBeforeDrop))

	resp, err := ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var data []blockFlashReplicaInfo
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(len(data), Equals, 0)

	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/whtcorpsinc/milevadb/schemareplicant/mockTiFlashStoreCount")
	dbt.mustInterDirc("use milevadb")
	dbt.mustInterDirc("alter causet test set tiflash replica 2 location labels 'a','b';")

	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(len(data), Equals, 1)
	c.Assert(data[0].ReplicaCount, Equals, uint64(2))
	c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
	c.Assert(data[0].Available, Equals, false)

	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(`{"id":84,"region_count":3,"flash_region_count":3}`)))
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	body, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Equals, "[schemaReplicant:1146]Block which ID = 84 does not exist.")

	t, err := ts.petri.SchemaReplicant().BlockByName(perceptron.NewCIStr("milevadb"), perceptron.NewCIStr("test"))
	c.Assert(err, IsNil)
	req := fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, t.Meta().ID)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	body, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(body), Equals, "")

	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(len(data), Equals, 1)
	c.Assert(data[0].ReplicaCount, Equals, uint64(2))
	c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
	c.Assert(data[0].Available, Equals, true) // The status should be true now.

	// Should not take effect.
	dbt.mustInterDirc("alter causet test set tiflash replica 2 location labels 'a','b';")
	checkFunc := func() {
		resp, err = ts.fetchStatus("/tiflash/replica")
		c.Assert(err, IsNil)
		causetDecoder = json.NewCausetDecoder(resp.Body)
		err = causetDecoder.Decode(&data)
		c.Assert(err, IsNil)
		resp.Body.Close()
		c.Assert(len(data), Equals, 1)
		c.Assert(data[0].ReplicaCount, Equals, uint64(2))
		c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
		c.Assert(data[0].Available, Equals, true) // The status should be true now.
	}

	// Test for get dropped causet tiflash replica info.
	dbt.mustInterDirc("drop causet test")
	checkFunc()

	// Test unique causet id replica info.
	dbt.mustInterDirc("flashback causet test")
	checkFunc()
	dbt.mustInterDirc("drop causet test")
	checkFunc()
	dbt.mustInterDirc("flashback causet test")
	checkFunc()

	// Test for partition causet.
	dbt.mustInterDirc("alter causet pt set tiflash replica 2 location labels 'a','b';")
	dbt.mustInterDirc("alter causet test set tiflash replica 0;")
	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(len(data), Equals, 3)
	c.Assert(data[0].ReplicaCount, Equals, uint64(2))
	c.Assert(strings.Join(data[0].LocationLabels, ","), Equals, "a,b")
	c.Assert(data[0].Available, Equals, false)

	pid0 := data[0].ID
	pid1 := data[1].ID
	pid2 := data[2].ID

	// Mock for partition 1 replica was available.
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid1)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	resp.Body.Close()
	resp, err = ts.fetchStatus("/tiflash/replica")
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&data)
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(len(data), Equals, 3)
	c.Assert(data[0].Available, Equals, false)
	c.Assert(data[1].Available, Equals, true)
	c.Assert(data[2].Available, Equals, false)

	// Mock for partition 0,2 replica was available.
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid0)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	resp.Body.Close()
	req = fmt.Sprintf(`{"id":%d,"region_count":3,"flash_region_count":3}`, pid2)
	resp, err = ts.postStatus("/tiflash/replica", "application/json", bytes.NewBuffer([]byte(req)))
	c.Assert(err, IsNil)
	resp.Body.Close()
	checkFunc = func() {
		resp, err = ts.fetchStatus("/tiflash/replica")
		c.Assert(err, IsNil)
		causetDecoder = json.NewCausetDecoder(resp.Body)
		err = causetDecoder.Decode(&data)
		c.Assert(err, IsNil)
		resp.Body.Close()
		c.Assert(len(data), Equals, 3)
		c.Assert(data[0].Available, Equals, true)
		c.Assert(data[1].Available, Equals, true)
		c.Assert(data[2].Available, Equals, true)
	}

	// Test for get truncated causet tiflash replica info.
	dbt.mustInterDirc("truncate causet pt")
	dbt.mustInterDirc("alter causet pt set tiflash replica 0;")
	checkFunc()
}

func (ts *HTTPHandlerTestSuite) TestDecodeDeferredCausetValue(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	// defCausumn is a structure used for test
	type defCausumn struct {
		id int64
		tp *types.FieldType
	}
	// Backfill defCausumns.
	c1 := &defCausumn{id: 1, tp: types.NewFieldType(allegrosql.TypeLonglong)}
	c2 := &defCausumn{id: 2, tp: types.NewFieldType(allegrosql.TypeVarchar)}
	c3 := &defCausumn{id: 3, tp: types.NewFieldType(allegrosql.TypeNewDecimal)}
	c4 := &defCausumn{id: 4, tp: types.NewFieldType(allegrosql.TypeTimestamp)}
	defcaus := []*defCausumn{c1, c2, c3, c4}
	event := make([]types.Causet, len(defcaus))
	event[0] = types.NewIntCauset(100)
	event[1] = types.NewBytesCauset([]byte("abc"))
	event[2] = types.NewDecimalCauset(types.NewDecFromInt(1))
	event[3] = types.NewTimeCauset(types.NewTime(types.FromGoTime(time.Now()), allegrosql.TypeTimestamp, 6))

	// Encode the event.
	defCausIDs := make([]int64, 0, 3)
	for _, defCaus := range defcaus {
		defCausIDs = append(defCausIDs, defCaus.id)
	}
	rd := rowcodec.CausetEncoder{Enable: true}
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	bs, err := blockcodec.EncodeRow(sc, event, defCausIDs, nil, nil, &rd)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)
	bin := base64.StdEncoding.EncodeToString(bs)

	unitTest := func(defCaus *defCausumn) {
		path := fmt.Sprintf("/blocks/%d/%v/%d/%d?rowBin=%s", defCaus.id, defCaus.tp.Tp, defCaus.tp.Flag, defCaus.tp.Flen, bin)
		resp, err := ts.fetchStatus(path)
		c.Assert(err, IsNil, Commentf("url:%s", ts.statusURL(path)))
		causetDecoder := json.NewCausetDecoder(resp.Body)
		var data interface{}
		err = causetDecoder.Decode(&data)
		c.Assert(err, IsNil, Commentf("url:%v\ndata%v", ts.statusURL(path), data))
		defCausVal, err := types.CausetsToString([]types.Causet{event[defCaus.id-1]}, false)
		c.Assert(err, IsNil)
		c.Assert(data, Equals, defCausVal, Commentf("url:%v", ts.statusURL(path)))
	}

	for _, defCaus := range defcaus {
		unitTest(defCaus)
	}

	// Test bin has `+`.
	// 2020-03-08 16:01:00.315313
	bin = "CAIIyAEIBAIGYWJjCAYGAQCBCAgJsZ+TgISg1M8Z"
	event[3] = types.NewTimeCauset(types.NewTime(types.FromGoTime(time.Date(2020, 3, 8, 16, 1, 0, 315313000, time.UTC)), allegrosql.TypeTimestamp, 6))
	unitTest(defcaus[3])

	// Test bin has `/`.
	// 2020-03-08 02:44:46.409199
	bin = "CAIIyAEIBAIGYWJjCAYGAQCBCAgJ7/yY8LKF1M8Z"
	event[3] = types.NewTimeCauset(types.NewTime(types.FromGoTime(time.Date(2020, 3, 8, 2, 44, 46, 409199000, time.UTC)), allegrosql.TypeTimestamp, 6))
	unitTest(defcaus[3])
}

func (ts *HTTPHandlerTestSuite) TestGetIndexMVCC(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)

	// tests for normal index key
	resp, err := ts.fetchStatus("/mvcc/index/milevadb/test/idx1/1?a=1&b=2")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx2/1?a=1&b=2")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for index key which includes null
	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx1/3?a=3&b")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx2/3?a=3&b")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for index key which includes empty string
	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx1/4?a=4&b=")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx2/3?a=4&b=")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, true)

	// tests for wrong key
	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx1/5?a=5&b=1")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, false)

	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx2/5?a=5&b=1")
	c.Assert(err, IsNil)
	decodeKeyMvcc(resp.Body, c, false)

	// tests for missing defCausumn value
	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx1/1?a=1")
	c.Assert(err, IsNil)
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var data1 mvccKV
	err = causetDecoder.Decode(&data1)
	c.Assert(err, NotNil)

	resp, err = ts.fetchStatus("/mvcc/index/milevadb/test/idx2/1?a=1")
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	var data2 mvccKV
	err = causetDecoder.Decode(&data2)
	c.Assert(err, NotNil)

	resp, err = ts.fetchStatus("/mvcc/index/milevadb/pt(p2)/idx/666?a=666&b=def")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	decodeKeyMvcc(resp.Body, c, true)
}

func (ts *HTTPHandlerTestSuite) TestGetSettings(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/settings")
	c.Assert(err, IsNil)
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var settings *config.Config
	err = causetDecoder.Decode(&settings)
	c.Assert(err, IsNil)
	c.Assert(settings, DeepEquals, config.GetGlobalConfig())
}

func (ts *HTTPHandlerTestSuite) TestGetSchema(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/schemaReplicant")
	c.Assert(err, IsNil)
	causetDecoder := json.NewCausetDecoder(resp.Body)
	var dbs []*perceptron.DBInfo
	err = causetDecoder.Decode(&dbs)
	c.Assert(err, IsNil)
	expects := []string{"information_schema", "metrics_schema", "allegrosql", "performance_schema", "test", "milevadb"}
	names := make([]string, len(dbs))
	for i, v := range dbs {
		names[i] = v.Name.L
	}
	sort.Strings(names)
	c.Assert(names, DeepEquals, expects)

	resp, err = ts.fetchStatus("/schemaReplicant?block_id=5")
	c.Assert(err, IsNil)
	var t *perceptron.BlockInfo
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "user")

	_, err = ts.fetchStatus("/schemaReplicant?block_id=a")
	c.Assert(err, IsNil)

	_, err = ts.fetchStatus("/schemaReplicant?block_id=1")
	c.Assert(err, IsNil)

	_, err = ts.fetchStatus("/schemaReplicant?block_id=-1")
	c.Assert(err, IsNil)

	resp, err = ts.fetchStatus("/schemaReplicant/milevadb")
	c.Assert(err, IsNil)
	var lt []*perceptron.BlockInfo
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&lt)
	c.Assert(err, IsNil)
	c.Assert(len(lt), Greater, 0)

	_, err = ts.fetchStatus("/schemaReplicant/abc")
	c.Assert(err, IsNil)

	resp, err = ts.fetchStatus("/schemaReplicant/milevadb/test")
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "test")

	_, err = ts.fetchStatus("/schemaReplicant/milevadb/abc")
	c.Assert(err, IsNil)

	resp, err = ts.fetchStatus("/EDB-causet/5")
	c.Assert(err, IsNil)
	var dbtbl *dbBlockInfo
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&dbtbl)
	c.Assert(err, IsNil)
	c.Assert(dbtbl.BlockInfo.Name.L, Equals, "user")
	c.Assert(dbtbl.DBInfo.Name.L, Equals, "allegrosql")
	se, err := stochastik.CreateStochastik(ts.causetstore.(ekv.CausetStorage))
	c.Assert(err, IsNil)
	c.Assert(dbtbl.SchemaVersion, Equals, petri.GetPetri(se.(stochastikctx.Context)).SchemaReplicant().SchemaMetaVersion())

	EDB, err := allegrosql.Open("allegrosql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()
	dbt := &DBTest{c, EDB}

	dbt.mustInterDirc("create database if not exists test;")
	dbt.mustInterDirc("use test;")
	dbt.mustInterDirc(` create causet t1 (id int KEY)
		partition by range (id) (
		PARTITION p0 VALUES LESS THAN (3),
		PARTITION p1 VALUES LESS THAN (5),
		PARTITION p2 VALUES LESS THAN (7),
		PARTITION p3 VALUES LESS THAN (9))`)

	resp, err = ts.fetchStatus("/schemaReplicant/test/t1")
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&t)
	c.Assert(err, IsNil)
	c.Assert(t.Name.L, Equals, "t1")

	resp, err = ts.fetchStatus(fmt.Sprintf("/EDB-causet/%v", t.GetPartitionInfo().Definitions[0].ID))
	c.Assert(err, IsNil)
	causetDecoder = json.NewCausetDecoder(resp.Body)
	err = causetDecoder.Decode(&dbtbl)
	c.Assert(err, IsNil)
	c.Assert(dbtbl.BlockInfo.Name.L, Equals, "t1")
	c.Assert(dbtbl.DBInfo.Name.L, Equals, "test")
	c.Assert(dbtbl.BlockInfo, DeepEquals, t)
}

func (ts *HTTPHandlerTestSuite) TestAllHistory(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	_, err := ts.fetchStatus("/dbs/history/?limit=3")
	c.Assert(err, IsNil)
	_, err = ts.fetchStatus("/dbs/history/?limit=-1")
	c.Assert(err, IsNil)

	resp, err := ts.fetchStatus("/dbs/history")
	c.Assert(err, IsNil)
	causetDecoder := json.NewCausetDecoder(resp.Body)

	var jobs []*perceptron.Job
	s, _ := stochastik.CreateStochastik(ts.server.newEinsteinDBHandlerTool().CausetStore.(ekv.CausetStorage))
	defer s.Close()
	causetstore := petri.GetPetri(s.(stochastikctx.Context)).CausetStore()
	txn, _ := causetstore.Begin()
	txnMeta := spacetime.NewMeta(txn)
	txnMeta.GetAllHistoryDBSJobs()
	data, _ := txnMeta.GetAllHistoryDBSJobs()
	err = causetDecoder.Decode(&jobs)

	c.Assert(err, IsNil)
	c.Assert(jobs, DeepEquals, data)
}

func (ts *HTTPHandlerTestSuite) TestPostSettings(c *C) {
	ts.startServer(c)
	ts.prepareData(c)
	defer ts.stopServer(c)
	form := make(url.Values)
	form.Set("log_level", "error")
	form.Set("milevadb_general_log", "1")
	resp, err := ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(log.GetLevel(), Equals, log.ErrorLevel)
	c.Assert(zaplog.GetLevel(), Equals, zap.ErrorLevel)
	c.Assert(config.GetGlobalConfig().Log.Level, Equals, "error")
	c.Assert(atomic.LoadUint32(&variable.ProcessGeneralLog), Equals, uint32(1))
	form = make(url.Values)
	form.Set("log_level", "fatal")
	form.Set("milevadb_general_log", "0")
	resp, err = ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(atomic.LoadUint32(&variable.ProcessGeneralLog), Equals, uint32(0))
	c.Assert(log.GetLevel(), Equals, log.FatalLevel)
	c.Assert(zaplog.GetLevel(), Equals, zap.FatalLevel)
	c.Assert(config.GetGlobalConfig().Log.Level, Equals, "fatal")
	form.Set("log_level", os.Getenv("log_level"))

	// test dbs_slow_threshold
	form = make(url.Values)
	form.Set("dbs_slow_threshold", "200")
	resp, err = ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(atomic.LoadUint32(&variable.DBSSlowOprThreshold), Equals, uint32(200))

	// test check_mb4_value_in_utf8
	EDB, err := allegrosql.Open("allegrosql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()
	dbt := &DBTest{c, EDB}

	dbt.mustInterDirc("create database milevadb_test;")
	dbt.mustInterDirc("use milevadb_test;")
	dbt.mustInterDirc("drop causet if exists t2;")
	dbt.mustInterDirc("create causet t2(a varchar(100) charset utf8);")
	form.Set("check_mb4_value_in_utf8", "1")
	resp, err = ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, true)
	txn1, err := dbt.EDB.Begin()
	c.Assert(err, IsNil)
	_, err = txn1.InterDirc("insert t2 values (unhex('F0A48BAE'));")
	c.Assert(err, NotNil)
	txn1.Commit()

	// Disable CheckMb4ValueInUTF8.
	form = make(url.Values)
	form.Set("check_mb4_value_in_utf8", "0")
	resp, err = ts.formStatus("/settings", form)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(config.GetGlobalConfig().CheckMb4ValueInUTF8, Equals, false)
	dbt.mustInterDirc("insert t2 values (unhex('f09f8c80'));")
}

func (ts *HTTPHandlerTestSuite) TestPprof(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	retryTime := 100
	for retry := 0; retry < retryTime; retry++ {
		resp, err := ts.fetchStatus("/debug/pprof/heap")
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	zaplog.Fatal("failed to get profile for %d retries in every 10 ms", zap.Int("retryTime", retryTime))
}

func (ts *HTTPHandlerTestSuite) TestServerInfo(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/info")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	causetDecoder := json.NewCausetDecoder(resp.Body)

	info := serverInfo{}
	err = causetDecoder.Decode(&info)
	c.Assert(err, IsNil)

	cfg := config.GetGlobalConfig()
	c.Assert(info.IsTenant, IsTrue)
	c.Assert(info.IP, Equals, cfg.AdvertiseAddress)
	c.Assert(info.StatusPort, Equals, cfg.Status.StatusPort)
	c.Assert(info.Lease, Equals, cfg.Lease)
	c.Assert(info.Version, Equals, allegrosql.ServerVersion)
	c.Assert(info.GitHash, Equals, versioninfo.MilevaDBGitHash)

	causetstore := ts.server.newEinsteinDBHandlerTool().CausetStore.(ekv.CausetStorage)
	do, err := stochastik.GetPetri(causetstore.(ekv.CausetStorage))
	c.Assert(err, IsNil)
	dbs := do.DBS()
	c.Assert(info.ID, Equals, dbs.GetID())
}

func (ts *HTTPHandlerTestSerialSuite) TestAllServerInfo(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/info/all")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	causetDecoder := json.NewCausetDecoder(resp.Body)

	clusterInfo := clusterServerInfo{}
	err = causetDecoder.Decode(&clusterInfo)
	c.Assert(err, IsNil)

	c.Assert(clusterInfo.IsAllServerVersionConsistent, IsTrue)
	c.Assert(clusterInfo.ServersNum, Equals, 1)

	causetstore := ts.server.newEinsteinDBHandlerTool().CausetStore.(ekv.CausetStorage)
	do, err := stochastik.GetPetri(causetstore.(ekv.CausetStorage))
	c.Assert(err, IsNil)
	dbs := do.DBS()
	c.Assert(clusterInfo.TenantID, Equals, dbs.GetID())
	serverInfo, ok := clusterInfo.AllServersInfo[dbs.GetID()]
	c.Assert(ok, Equals, true)

	cfg := config.GetGlobalConfig()
	c.Assert(serverInfo.IP, Equals, cfg.AdvertiseAddress)
	c.Assert(serverInfo.StatusPort, Equals, cfg.Status.StatusPort)
	c.Assert(serverInfo.Lease, Equals, cfg.Lease)
	c.Assert(serverInfo.Version, Equals, allegrosql.ServerVersion)
	c.Assert(serverInfo.GitHash, Equals, versioninfo.MilevaDBGitHash)
	c.Assert(serverInfo.ID, Equals, dbs.GetID())
}

func (ts *HTTPHandlerTestSuite) TestHotRegionInfo(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/regions/hot")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
}

func (ts *HTTPHandlerTestSuite) TestDebugZip(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	resp, err := ts.fetchStatus("/debug/zip?seconds=1")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err := httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)
}

func (ts *HTTPHandlerTestSuite) TestCheckCN(c *C) {
	s := &Server{cfg: &config.Config{Security: config.Security{ClusterVerifyCN: []string{"a ", "b", "c"}}}}
	tlsConfig := &tls.Config{}
	s.setCNChecker(tlsConfig)
	c.Assert(tlsConfig.VerifyPeerCertificate, NotNil)
	err := tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "a"}}}})
	c.Assert(err, IsNil)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "b"}}}})
	c.Assert(err, IsNil)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "d"}}}})
	c.Assert(err, NotNil)
}

func (ts *HTTPHandlerTestSuite) TestZipInfoForALLEGROSQL(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)

	EDB, err := allegrosql.Open("allegrosql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()
	dbt := &DBTest{c, EDB}

	dbt.mustInterDirc("use test")
	dbt.mustInterDirc("create causet if not exists t (a int)")

	urlValues := url.Values{
		"allegrosql": {"select * from t"},
		"current_db": {"test"},
	}
	resp, err := ts.formStatus("/debug/sub-optimal-plan", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err := httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	resp, err = ts.formStatus("/debug/sub-optimal-plan?pprof_time=5&timeout=0", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err = httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	resp, err = ts.formStatus("/debug/sub-optimal-plan?pprof_time=5", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err = httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	resp, err = ts.formStatus("/debug/sub-optimal-plan?timeout=1", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err = httputil.DumpResponse(resp, true)
	c.Assert(err, IsNil)
	c.Assert(len(b), Greater, 0)
	c.Assert(resp.Body.Close(), IsNil)

	urlValues.Set("current_db", "non_exists_db")
	resp, err = ts.formStatus("/debug/sub-optimal-plan", urlValues)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusInternalServerError)
	b, err = ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(string(b), Equals, "use database non_exists_db failed, err: [schemaReplicant:1049]Unknown database 'non_exists_db'\n")
	c.Assert(resp.Body.Close(), IsNil)
}

func (ts *HTTPHandlerTestSuite) TestFailpointHandler(c *C) {
	defer ts.stopServer(c)

	// start server without enabling failpoint integration
	ts.startServer(c)
	resp, err := ts.fetchStatus("/fail/")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	ts.stopServer(c)

	// enable failpoint integration and start server
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/server/enableTestAPI", "return"), IsNil)
	ts.startServer(c)
	resp, err = ts.fetchStatus("/fail/")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	b, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(b), "github.com/whtcorpsinc/milevadb/server/enableTestAPI=return"), IsTrue)
	c.Assert(resp.Body.Close(), IsNil)
}

func (ts *HTTPHandlerTestSuite) TestTestHandler(c *C) {
	defer ts.stopServer(c)

	// start server without enabling failpoint integration
	ts.startServer(c)
	resp, err := ts.fetchStatus("/test")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusNotFound)
	ts.stopServer(c)

	// enable failpoint integration and start server
	c.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/server/enableTestAPI", "return"), IsNil)
	ts.startServer(c)

	resp, err = ts.fetchStatus("/test/gc/gc")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?safepoint=a")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?physical=1")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?physical=true")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)

	resp, err = ts.fetchStatus("/test/gc/resolvelock?safepoint=10000&physical=true")
	c.Assert(err, IsNil)
	resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}
