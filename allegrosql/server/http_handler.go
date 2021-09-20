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
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/ekvproto/pkg/spacetimepb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/causet"
	"github.com/whtcorpsinc/milevadb/causet/blocks"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/gcworker"
	"github.com/whtcorpsinc/milevadb/causetstore/helper"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/petri/infosync"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/FIDelapi"
	"github.com/whtcorpsinc/milevadb/soliton/admin"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/gcutil"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/spacetime"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"go.uber.org/zap"
)

const (
	FIDelBName          = "EDB"
	pHexKey             = "hexKey"
	pIndexName          = "index"
	pHandle             = "handle"
	pRegionID           = "regionID"
	pStartTS            = "startTS"
	pBlockName          = "causet"
	pBlockID            = "blockID"
	pDeferredCausetID   = "defCausID"
	pDeferredCausetTp   = "defCausTp"
	pDeferredCausetFlag = "defCausFlag"
	pDeferredCausetLen  = "defCausLen"
	pRowBin             = "rowBin"
	pSnapshot           = "snapshot"
)

// For query string
const (
	qBlockID   = "block_id"
	qLimit     = "limit"
	qOperation = "op"
	qSeconds   = "seconds"
)

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
	terror.Log(errors.Trace(err))
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		writeError(w, err)
		return
	}
	// write response
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	terror.Log(errors.Trace(err))
}

type einsteindbHandlerTool struct {
	helper.Helper
}

// newEinsteinDBHandlerTool checks and prepares for einsteindb handler.
// It would panic when any error happens.
func (s *Server) newEinsteinDBHandlerTool() *einsteindbHandlerTool {
	var einsteindbStore einsteindb.CausetStorage
	causetstore, ok := s.driver.(*MilevaDBDriver)
	if !ok {
		panic("Invalid EkvStore with illegal driver")
	}

	if einsteindbStore, ok = causetstore.causetstore.(einsteindb.CausetStorage); !ok {
		panic("Invalid EkvStore with illegal causetstore")
	}

	regionCache := einsteindbStore.GetRegionCache()

	return &einsteindbHandlerTool{
		helper.Helper{
			RegionCache: regionCache,
			CausetStore: einsteindbStore,
		},
	}
}

type mvccKV struct {
	Key      string                         `json:"key"`
	RegionID uint64                         `json:"region_id"`
	Value    *ekvrpcpb.MvccGetByKeyResponse `json:"value"`
}

func (t *einsteindbHandlerTool) getRegionIDByKey(encodedKey []byte) (uint64, error) {
	keyLocation, err := t.RegionCache.LocateKey(einsteindb.NewBackofferWithVars(context.Background(), 500, nil), encodedKey)
	if err != nil {
		return 0, err
	}
	return keyLocation.Region.GetID(), nil
}

func (t *einsteindbHandlerTool) getMvccByHandle(blockID, handle int64) (*mvccKV, error) {
	encodedKey := blockcodec.EncodeRowKeyWithHandle(blockID, ekv.IntHandle(handle))
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err := t.getRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return &mvccKV{Key: strings.ToUpper(hex.EncodeToString(encodedKey)), Value: data, RegionID: regionID}, err
}

func (t *einsteindbHandlerTool) getMvccByStartTs(startTS uint64, startKey, endKey ekv.Key) (*mvccKV, error) {
	bo := einsteindb.NewBackofferWithVars(context.Background(), 5000, nil)
	for {
		curRegion, err := t.RegionCache.LocateKey(bo, startKey)
		if err != nil {
			logutil.BgLogger().Error("get MVCC by startTS failed", zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey), zap.Error(err))
			return nil, errors.Trace(err)
		}

		einsteindbReq := einsteindbrpc.NewRequest(einsteindbrpc.CmdMvccGetByStartTs, &ekvrpcpb.MvccGetByStartTsRequest{
			StartTs: startTS,
		})
		einsteindbReq.Context.Priority = ekvrpcpb.CommandPri_Low
		ekvResp, err := t.CausetStore.SendReq(bo, einsteindbReq, curRegion.Region, time.Hour)
		if err != nil {
			logutil.BgLogger().Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion startKey", curRegion.StartKey),
				zap.Stringer("curRegion endKey", curRegion.EndKey),
				zap.Reflect("ekvResp", ekvResp),
				zap.Error(err))
			return nil, errors.Trace(err)
		}
		data := ekvResp.Resp.(*ekvrpcpb.MvccGetByStartTsResponse)
		if err := data.GetRegionError(); err != nil {
			logutil.BgLogger().Warn("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion startKey", curRegion.StartKey),
				zap.Stringer("curRegion endKey", curRegion.EndKey),
				zap.Reflect("ekvResp", ekvResp),
				zap.Stringer("error", err))
			continue
		}

		if len(data.GetError()) > 0 {
			logutil.BgLogger().Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion startKey", curRegion.StartKey),
				zap.Stringer("curRegion endKey", curRegion.EndKey),
				zap.Reflect("ekvResp", ekvResp),
				zap.String("error", data.GetError()))
			return nil, errors.New(data.GetError())
		}

		key := data.GetKey()
		if len(key) > 0 {
			resp := &ekvrpcpb.MvccGetByKeyResponse{Info: data.Info, RegionError: data.RegionError, Error: data.Error}
			return &mvccKV{Key: strings.ToUpper(hex.EncodeToString(key)), Value: resp, RegionID: curRegion.Region.GetID()}, nil
		}

		if len(endKey) > 0 && curRegion.Contains(endKey) {
			return nil, nil
		}
		if len(curRegion.EndKey) == 0 {
			return nil, nil
		}
		startKey = curRegion.EndKey
	}
}

func (t *einsteindbHandlerTool) getMvccByIdxValue(idx causet.Index, values url.Values, idxDefCauss []*perceptron.DeferredCausetInfo, handleStr string) (*mvccKV, error) {
	sc := new(stmtctx.StatementContext)
	// HTTP request is not a database stochastik, set timezone to UTC directly here.
	// See https://github.com/whtcorpsinc/milevadb/blob/master/docs/milevadb_http_api.md for more details.
	sc.TimeZone = time.UTC
	idxRow, err := t.formValue2CausetRow(sc, values, idxDefCauss)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle, err := strconv.ParseInt(handleStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	encodedKey, _, err := idx.GenIndexKey(sc, idxRow, ekv.IntHandle(handle), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err := t.getRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return &mvccKV{strings.ToUpper(hex.EncodeToString(encodedKey)), regionID, data}, err
}

// formValue2CausetRow converts URL query string to a Causet Row.
func (t *einsteindbHandlerTool) formValue2CausetRow(sc *stmtctx.StatementContext, values url.Values, idxDefCauss []*perceptron.DeferredCausetInfo) ([]types.Causet, error) {
	data := make([]types.Causet, len(idxDefCauss))
	for i, defCaus := range idxDefCauss {
		defCausName := defCaus.Name.String()
		vals, ok := values[defCausName]
		if !ok {
			return nil, errors.BadRequestf("Missing value for index defCausumn %s.", defCausName)
		}

		switch len(vals) {
		case 0:
			data[i].SetNull()
		case 1:
			bCauset := types.NewStringCauset(vals[0])
			cCauset, err := bCauset.ConvertTo(sc, &defCaus.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			data[i] = cCauset
		default:
			return nil, errors.BadRequestf("Invalid query form for defCausumn '%s', it's values are %v."+
				" DeferredCauset value should be unique for one index record.", defCausName, vals)
		}
	}
	return data, nil
}

func (t *einsteindbHandlerTool) getBlockID(dbName, blockName string) (int64, error) {
	tbl, err := t.getBlock(dbName, blockName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return tbl.GetPhysicalID(), nil
}

func (t *einsteindbHandlerTool) getBlock(dbName, blockName string) (causet.PhysicalBlock, error) {
	schemaReplicant, err := t.schemaReplicant()
	if err != nil {
		return nil, errors.Trace(err)
	}
	blockName, partitionName := extractBlockAndPartitionName(blockName)
	blockVal, err := schemaReplicant.BlockByName(perceptron.NewCIStr(dbName), perceptron.NewCIStr(blockName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.getPartition(blockVal, partitionName)
}

func (t *einsteindbHandlerTool) getPartition(blockVal causet.Block, partitionName string) (causet.PhysicalBlock, error) {
	if pt, ok := blockVal.(causet.PartitionedBlock); ok {
		if partitionName == "" {
			return blockVal.(causet.PhysicalBlock), errors.New("work on partitioned causet, please specify the causet name like this: causet(partition)")
		}
		tblInfo := pt.Meta()
		pid, err := blocks.FindPartitionByName(tblInfo, partitionName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return pt.GetPartition(pid), nil
	}
	if partitionName != "" {
		return nil, fmt.Errorf("%s is not a partitionted causet", blockVal.Meta().Name)
	}
	return blockVal.(causet.PhysicalBlock), nil
}

func (t *einsteindbHandlerTool) schemaReplicant() (schemareplicant.SchemaReplicant, error) {
	stochastik, err := stochastik.CreateStochastik(t.CausetStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return petri.GetPetri(stochastik.(stochastikctx.Context)).SchemaReplicant(), nil
}

func (t *einsteindbHandlerTool) handleMvccGetByHex(params map[string]string) (*mvccKV, error) {
	encodedKey, err := hex.DecodeString(params[pHexKey])
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionID, err := t.getRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return &mvccKV{Key: strings.ToUpper(params[pHexKey]), Value: data, RegionID: regionID}, nil
}

// settingsHandler is the handler for list milevadb server settings.
type settingsHandler struct {
}

// binlogRecover is used to recover binlog service.
// When config binlog IgnoreError, binlog service will stop after meeting the first error.
// It can be recovered using HTTP API.
type binlogRecover struct{}

// schemaHandler is the handler for list database or causet schemas.
type schemaHandler struct {
	*einsteindbHandlerTool
}

type dbBlockHandler struct {
	*einsteindbHandlerTool
}

type flashReplicaHandler struct {
	*einsteindbHandlerTool
}

// regionHandler is the common field for http handler. It contains
// some common functions for all handlers.
type regionHandler struct {
	*einsteindbHandlerTool
}

// blockHandler is the handler for list causet's regions.
type blockHandler struct {
	*einsteindbHandlerTool
	op string
}

// dbsHistoryJobHandler is the handler for list job history.
type dbsHistoryJobHandler struct {
	*einsteindbHandlerTool
}

// dbsResignTenantHandler is the handler for resigning dbs tenant.
type dbsResignTenantHandler struct {
	causetstore ekv.CausetStorage
}

type serverInfoHandler struct {
	*einsteindbHandlerTool
}

type allServerInfoHandler struct {
	*einsteindbHandlerTool
}

type profileHandler struct {
	*einsteindbHandlerTool
}

// valueHandler is the handler for get value.
type valueHandler struct {
}

const (
	opBlockRegions     = "regions"
	opBlockDiskUsage   = "disk-usage"
	opBlockScatter     = "scatter-causet"
	opStopBlockScatter = "stop-scatter-causet"
)

// mvccTxnHandler is the handler for txn debugger.
type mvccTxnHandler struct {
	*einsteindbHandlerTool
	op string
}

const (
	opMvccGetByHex = "hex"
	opMvccGetByKey = "key"
	opMvccGetByIdx = "idx"
	opMvccGetByTxn = "txn"
)

// ServeHTTP handles request of list a database or causet's schemas.
func (vh valueHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)

	defCausID, err := strconv.ParseInt(params[pDeferredCausetID], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	defCausTp, err := strconv.ParseInt(params[pDeferredCausetTp], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	defCausFlag, err := strconv.ParseUint(params[pDeferredCausetFlag], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	defCausLen, err := strconv.ParseInt(params[pDeferredCausetLen], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}

	// Get the unchanged binary.
	if req.URL == nil {
		err = errors.BadRequestf("Invalid URL")
		writeError(w, err)
		return
	}
	values := make(url.Values)
	shouldUnescape := false
	err = parseQuery(req.URL.RawQuery, values, shouldUnescape)
	if err != nil {
		writeError(w, err)
		return
	}
	if len(values[pRowBin]) != 1 {
		err = errors.BadRequestf("Invalid Query:%v", values[pRowBin])
		writeError(w, err)
		return
	}
	bin := values[pRowBin][0]
	valData, err := base64.StdEncoding.DecodeString(bin)
	if err != nil {
		writeError(w, err)
		return
	}
	// Construct field type.
	defaultDecimal := 6
	ft := &types.FieldType{
		Tp:      byte(defCausTp),
		Flag:    uint(defCausFlag),
		Flen:    int(defCausLen),
		Decimal: defaultDecimal,
	}
	// Decode a defCausumn.
	m := make(map[int64]*types.FieldType, 1)
	m[defCausID] = ft
	loc := time.UTC
	vals, err := blockcodec.DecodeRowToCausetMap(valData, m, loc)
	if err != nil {
		writeError(w, err)
		return
	}

	v := vals[defCausID]
	val, err := v.ToString()
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, val)
}

// BlockRegions is the response data for list causet's regions.
// It contains regions list for record and indices.
type BlockRegions struct {
	BlockName     string         `json:"name"`
	BlockID       int64          `json:"id"`
	RecordRegions []RegionMeta   `json:"record_regions"`
	Indices       []IndexRegions `json:"indices"`
}

// RegionMeta contains a region's peer detail
type RegionMeta struct {
	ID          uint64                   `json:"region_id"`
	Leader      *spacetimepb.Peer        `json:"leader"`
	Peers       []*spacetimepb.Peer      `json:"peers"`
	RegionEpoch *spacetimepb.RegionEpoch `json:"region_epoch"`
}

// IndexRegions is the region info for one index.
type IndexRegions struct {
	Name    string       `json:"name"`
	ID      int64        `json:"id"`
	Regions []RegionMeta `json:"regions"`
}

// RegionDetail is the response data for get region by ID
// it includes indices and records detail in current region.
type RegionDetail struct {
	RegionID uint64              `json:"region_id"`
	StartKey []byte              `json:"start_key"`
	EndKey   []byte              `json:"end_key"`
	Frames   []*helper.FrameItem `json:"frames"`
}

// addBlockInRange insert a causet into RegionDetail
// with index's id or record in the range if r.
func (rt *RegionDetail) addBlockInRange(dbName string, curBlock *perceptron.BlockInfo, r *helper.RegionFrameRange) {
	tName := curBlock.Name.String()
	tID := curBlock.ID
	pi := curBlock.GetPartitionInfo()
	isCommonHandle := curBlock.IsCommonHandle
	for _, index := range curBlock.Indices {
		if index.Primary && isCommonHandle {
			continue
		}
		if pi != nil {
			for _, def := range pi.Definitions {
				if f := r.GetIndexFrame(def.ID, index.ID, dbName, fmt.Sprintf("%s(%s)", tName, def.Name.O), index.Name.String()); f != nil {
					rt.Frames = append(rt.Frames, f)
				}
			}
		} else {
			if f := r.GetIndexFrame(tID, index.ID, dbName, tName, index.Name.String()); f != nil {
				rt.Frames = append(rt.Frames, f)
			}
		}

	}

	if pi != nil {
		for _, def := range pi.Definitions {
			if f := r.GetRecordFrame(def.ID, dbName, fmt.Sprintf("%s(%s)", tName, def.Name.O), isCommonHandle); f != nil {
				rt.Frames = append(rt.Frames, f)
			}
		}
	} else {
		if f := r.GetRecordFrame(tID, dbName, tName, isCommonHandle); f != nil {
			rt.Frames = append(rt.Frames, f)
		}
	}
}

// FrameItem includes a index's or record's spacetime data with causet's info.
type FrameItem struct {
	DBName      string   `json:"db_name"`
	BlockName   string   `json:"block_name"`
	BlockID     int64    `json:"block_id"`
	IsRecord    bool     `json:"is_record"`
	RecordID    int64    `json:"record_id,omitempty"`
	IndexName   string   `json:"index_name,omitempty"`
	IndexID     int64    `json:"index_id,omitempty"`
	IndexValues []string `json:"index_values,omitempty"`
}

// RegionFrameRange contains a frame range info which the region covered.
type RegionFrameRange struct {
	first  *FrameItem              // start frame of the region
	last   *FrameItem              // end frame of the region
	region *einsteindb.KeyLocation // the region
}

func (t *einsteindbHandlerTool) getRegionsMeta(regionIDs []uint64) ([]RegionMeta, error) {
	regions := make([]RegionMeta, len(regionIDs))
	for i, regionID := range regionIDs {
		region, err := t.RegionCache.FIDelClient().GetRegionByID(context.TODO(), regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		failpoint.Inject("errGetRegionByIDEmpty", func(val failpoint.Value) {
			if val.(bool) {
				region.Meta = nil
			}
		})

		if region.Meta == nil {
			return nil, errors.Errorf("region not found for regionID %q", regionID)
		}
		regions[i] = RegionMeta{
			ID:          regionID,
			Leader:      region.Leader,
			Peers:       region.Meta.Peers,
			RegionEpoch: region.Meta.RegionEpoch,
		}

	}
	return regions, nil
}

// ServeHTTP handles request of list milevadb server settings.
func (h settingsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		err := req.ParseForm()
		if err != nil {
			writeError(w, err)
			return
		}
		if levelStr := req.Form.Get("log_level"); levelStr != "" {
			err1 := logutil.SetLevel(levelStr)
			if err1 != nil {
				writeError(w, err1)
				return
			}

			l, err1 := log.ParseLevel(levelStr)
			if err1 != nil {
				writeError(w, err1)
				return
			}
			log.SetLevel(l)

			config.GetGlobalConfig().Log.Level = levelStr
		}
		if generalLog := req.Form.Get("milevadb_general_log"); generalLog != "" {
			switch generalLog {
			case "0":
				atomic.StoreUint32(&variable.ProcessGeneralLog, 0)
			case "1":
				atomic.StoreUint32(&variable.ProcessGeneralLog, 1)
			default:
				writeError(w, errors.New("illegal argument"))
				return
			}
		}
		if dbsSlowThreshold := req.Form.Get("dbs_slow_threshold"); dbsSlowThreshold != "" {
			threshold, err1 := strconv.Atoi(dbsSlowThreshold)
			if err1 != nil {
				writeError(w, err1)
				return
			}
			if threshold > 0 {
				atomic.StoreUint32(&variable.DBSSlowOprThreshold, uint32(threshold))
			}
		}
		if checkMb4ValueInUtf8 := req.Form.Get("check_mb4_value_in_utf8"); checkMb4ValueInUtf8 != "" {
			switch checkMb4ValueInUtf8 {
			case "0":
				config.GetGlobalConfig().CheckMb4ValueInUTF8 = false
			case "1":
				config.GetGlobalConfig().CheckMb4ValueInUTF8 = true
			default:
				writeError(w, errors.New("illegal argument"))
				return
			}
		}
	} else {
		writeData(w, config.GetGlobalConfig())
	}
}

// ServeHTTP recovers binlog service.
func (h binlogRecover) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	op := req.FormValue(qOperation)
	switch op {
	case "reset":
		binloginfo.ResetSkippedCommitterCounter()
	case "nowait":
		binloginfo.DisableSkipBinlogFlag()
	case "status":
	default:
		sec, err := strconv.ParseInt(req.FormValue(qSeconds), 10, 64)
		if sec <= 0 || err != nil {
			sec = 1800
		}
		binloginfo.DisableSkipBinlogFlag()
		timeout := time.Duration(sec) * time.Second
		err = binloginfo.WaitBinlogRecover(timeout)
		if err != nil {
			writeError(w, err)
			return
		}
	}
	writeData(w, binloginfo.GetBinlogStatus())
}

type blockFlashReplicaInfo struct {
	// Modifying the field name needs to negotiate with TiFlash defCausleague.
	ID             int64    `json:"id"`
	ReplicaCount   uint64   `json:"replica_count"`
	LocationLabels []string `json:"location_labels"`
	Available      bool     `json:"available"`
	HighPriority   bool     `json:"high_priority"`
}

func (h flashReplicaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		h.handleStatusReport(w, req)
		return
	}
	schemaReplicant, err := h.schemaReplicant()
	if err != nil {
		writeError(w, err)
		return
	}
	replicaInfos := make([]*blockFlashReplicaInfo, 0)
	allDBs := schemaReplicant.AllSchemas()
	for _, EDB := range allDBs {
		tbls := schemaReplicant.SchemaBlocks(EDB.Name)
		for _, tbl := range tbls {
			replicaInfos = h.getTiFlashReplicaInfo(tbl.Meta(), replicaInfos)
		}
	}
	dropedOrTruncateReplicaInfos, err := h.getDropOrTruncateBlockTiflash(schemaReplicant)
	if err != nil {
		writeError(w, err)
		return
	}
	replicaInfos = append(replicaInfos, dropedOrTruncateReplicaInfos...)
	writeData(w, replicaInfos)
}

func (h flashReplicaHandler) getTiFlashReplicaInfo(tblInfo *perceptron.BlockInfo, replicaInfos []*blockFlashReplicaInfo) []*blockFlashReplicaInfo {
	if tblInfo.TiFlashReplica == nil {
		return replicaInfos
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			replicaInfos = append(replicaInfos, &blockFlashReplicaInfo{
				ID:             p.ID,
				ReplicaCount:   tblInfo.TiFlashReplica.Count,
				LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
				Available:      tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID),
			})
		}
		for _, p := range pi.AddingDefinitions {
			replicaInfos = append(replicaInfos, &blockFlashReplicaInfo{
				ID:             p.ID,
				ReplicaCount:   tblInfo.TiFlashReplica.Count,
				LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
				Available:      tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID),
				HighPriority:   true,
			})
		}
		return replicaInfos
	}
	replicaInfos = append(replicaInfos, &blockFlashReplicaInfo{
		ID:             tblInfo.ID,
		ReplicaCount:   tblInfo.TiFlashReplica.Count,
		LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
		Available:      tblInfo.TiFlashReplica.Available,
	})
	return replicaInfos
}

func (h flashReplicaHandler) getDropOrTruncateBlockTiflash(currentSchema schemareplicant.SchemaReplicant) ([]*blockFlashReplicaInfo, error) {
	s, err := stochastik.CreateStochastik(h.CausetStore.(ekv.CausetStorage))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if s != nil {
		defer s.Close()
	}

	causetstore := petri.GetPetri(s).CausetStore()
	txn, err := causetstore.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(s)
	if err != nil {
		return nil, err
	}
	replicaInfos := make([]*blockFlashReplicaInfo, 0)
	uniqueIDMap := make(map[int64]struct{})
	handleJobAndBlockInfo := func(job *perceptron.Job, tblInfo *perceptron.BlockInfo) (bool, error) {
		// Avoid duplicate causet ID info.
		if _, ok := currentSchema.BlockByID(tblInfo.ID); ok {
			return false, nil
		}
		if _, ok := uniqueIDMap[tblInfo.ID]; ok {
			return false, nil
		}
		uniqueIDMap[tblInfo.ID] = struct{}{}
		replicaInfos = h.getTiFlashReplicaInfo(tblInfo, replicaInfos)
		return false, nil
	}
	dom := petri.GetPetri(s)
	fn := func(jobs []*perceptron.Job) (bool, error) {
		return interlock.GetDropOrTruncateBlockInfoFromJobs(jobs, gcSafePoint, dom, handleJobAndBlockInfo)
	}

	err = admin.IterAllDBSJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			// The err indicate that current dbs job and remain DBS jobs was been deleted by GC,
			// just ignore the error and return directly.
			return replicaInfos, nil
		}
		return nil, err
	}
	return replicaInfos, nil
}

type blockFlashReplicaStatus struct {
	// Modifying the field name needs to negotiate with TiFlash defCausleague.
	ID int64 `json:"id"`
	// RegionCount is the number of regions that need sync.
	RegionCount uint64 `json:"region_count"`
	// FlashRegionCount is the number of regions that already sync completed.
	FlashRegionCount uint64 `json:"flash_region_count"`
}

// checkBlockFlashReplicaAvailable uses to check the available status of causet flash replica.
func (tf *blockFlashReplicaStatus) checkBlockFlashReplicaAvailable() bool {
	return tf.FlashRegionCount == tf.RegionCount
}

func (h flashReplicaHandler) handleStatusReport(w http.ResponseWriter, req *http.Request) {
	var status blockFlashReplicaStatus
	err := json.NewCausetDecoder(req.Body).Decode(&status)
	if err != nil {
		writeError(w, err)
		return
	}
	do, err := stochastik.GetPetri(h.CausetStore.(ekv.CausetStorage))
	if err != nil {
		writeError(w, err)
		return
	}
	s, err := stochastik.CreateStochastik(h.CausetStore.(ekv.CausetStorage))
	if err != nil {
		writeError(w, err)
		return
	}
	available := status.checkBlockFlashReplicaAvailable()
	err = do.DBS().UFIDelateBlockReplicaInfo(s, status.ID, available)
	if err != nil {
		writeError(w, err)
	}
	if available {
		err = infosync.DeleteTiFlashBlockSyncProgress(status.ID)
	} else {
		err = infosync.UFIDelateTiFlashBlockSyncProgress(context.Background(), status.ID, float64(status.FlashRegionCount)/float64(status.RegionCount))
	}
	if err != nil {
		writeError(w, err)
	}

	logutil.BgLogger().Info("handle flash replica report", zap.Int64("causet ID", status.ID), zap.Uint64("region count",
		status.RegionCount),
		zap.Uint64("flash region count", status.FlashRegionCount),
		zap.Error(err))
}

// ServeHTTP handles request of list a database or causet's schemas.
func (h schemaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	schemaReplicant, err := h.schemaReplicant()
	if err != nil {
		writeError(w, err)
		return
	}

	// parse params
	params := mux.Vars(req)

	if dbName, ok := params[FIDelBName]; ok {
		cDBName := perceptron.NewCIStr(dbName)
		if blockName, ok := params[pBlockName]; ok {
			// causet schemaReplicant of a specified causet name
			cBlockName := perceptron.NewCIStr(blockName)
			data, err := schemaReplicant.BlockByName(cDBName, cBlockName)
			if err != nil {
				writeError(w, err)
				return
			}
			writeData(w, data.Meta())
			return
		}
		// all causet schemas in a specified database
		if schemaReplicant.SchemaExists(cDBName) {
			tbs := schemaReplicant.SchemaBlocks(cDBName)
			tbsInfo := make([]*perceptron.BlockInfo, len(tbs))
			for i := range tbsInfo {
				tbsInfo[i] = tbs[i].Meta()
			}
			writeData(w, tbsInfo)
			return
		}
		writeError(w, schemareplicant.ErrDatabaseNotExists.GenWithStackByArgs(dbName))
		return
	}

	if blockID := req.FormValue(qBlockID); len(blockID) > 0 {
		// causet schemaReplicant of a specified blockID
		tid, err := strconv.Atoi(blockID)
		if err != nil {
			writeError(w, err)
			return
		}
		if tid < 0 {
			writeError(w, schemareplicant.ErrBlockNotExists.GenWithStack("Block which ID = %s does not exist.", blockID))
			return
		}
		if data, ok := schemaReplicant.BlockByID(int64(tid)); ok {
			writeData(w, data.Meta())
			return
		}
		writeError(w, schemareplicant.ErrBlockNotExists.GenWithStack("Block which ID = %s does not exist.", blockID))
		return
	}

	// all databases' schemas
	writeData(w, schemaReplicant.AllSchemas())
}

// ServeHTTP handles causet related requests, such as causet's region information, disk usage.
func (h blockHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)
	dbName := params[FIDelBName]
	blockName := params[pBlockName]
	schemaReplicant, err := h.schemaReplicant()
	if err != nil {
		writeError(w, err)
		return
	}

	blockName, partitionName := extractBlockAndPartitionName(blockName)
	blockVal, err := schemaReplicant.BlockByName(perceptron.NewCIStr(dbName), perceptron.NewCIStr(blockName))
	if err != nil {
		writeError(w, err)
		return
	}
	switch h.op {
	case opBlockRegions:
		h.handleRegionRequest(schemaReplicant, blockVal, w, req)
	case opBlockDiskUsage:
		h.handleDiskUsageRequest(blockVal, w)
	case opBlockScatter:
		// supports partition causet, only get one physical causet, prevent too many scatter schedulers.
		ptbl, err := h.getPartition(blockVal, partitionName)
		if err != nil {
			writeError(w, err)
			return
		}
		h.handleScatterBlockRequest(schemaReplicant, ptbl, w, req)
	case opStopBlockScatter:
		ptbl, err := h.getPartition(blockVal, partitionName)
		if err != nil {
			writeError(w, err)
			return
		}
		h.handleStopScatterBlockRequest(schemaReplicant, ptbl, w, req)
	default:
		writeError(w, errors.New("method not found"))
	}
}

// ServeHTTP handles request of dbs jobs history.
func (h dbsHistoryJobHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if limitID := req.FormValue(qLimit); len(limitID) > 0 {
		lid, err := strconv.Atoi(limitID)

		if err != nil {
			writeError(w, err)
			return
		}

		if lid < 1 {
			writeError(w, errors.New("dbs history limit must be greater than 1"))
			return
		}

		jobs, err := h.getAllHistoryDBS()
		if err != nil {
			writeError(w, errors.New("dbs history not found"))
			return
		}

		jobsLen := len(jobs)
		if jobsLen > lid {
			start := jobsLen - lid
			jobs = jobs[start:]
		}

		writeData(w, jobs)
		return
	}
	jobs, err := h.getAllHistoryDBS()
	if err != nil {
		writeError(w, errors.New("dbs history not found"))
		return
	}
	writeData(w, jobs)
}

func (h dbsHistoryJobHandler) getAllHistoryDBS() ([]*perceptron.Job, error) {
	s, err := stochastik.CreateStochastik(h.CausetStore.(ekv.CausetStorage))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if s != nil {
		defer s.Close()
	}

	causetstore := petri.GetPetri(s.(stochastikctx.Context)).CausetStore()
	txn, err := causetstore.Begin()

	if err != nil {
		return nil, errors.Trace(err)
	}
	txnMeta := spacetime.NewMeta(txn)

	jobs, err := txnMeta.GetAllHistoryDBSJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func (h dbsResignTenantHandler) resignDBSTenant() error {
	dom, err := stochastik.GetPetri(h.causetstore)
	if err != nil {
		return errors.Trace(err)
	}

	tenantMgr := dom.DBS().TenantManager()
	err = tenantMgr.ResignTenant(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ServeHTTP handles request of resigning dbs tenant.
func (h dbsResignTenantHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, errors.Errorf("This api only support POST method."))
		return
	}

	err := h.resignDBSTenant()
	if err != nil {
		log.Error(err)
		writeError(w, err)
		return
	}

	writeData(w, "success!")
}

func (h blockHandler) getFIDelAddr() ([]string, error) {
	etcd, ok := h.CausetStore.(einsteindb.EtcdBackend)
	if !ok {
		return nil, errors.New("not implemented")
	}
	FIDelAddrs, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(FIDelAddrs) == 0 {
		return nil, errors.New("fidel unavailable")
	}
	return FIDelAddrs, nil
}

func (h blockHandler) addScatterSchedule(startKey, endKey []byte, name string) error {
	FIDelAddrs, err := h.getFIDelAddr()
	if err != nil {
		return err
	}
	input := map[string]string{
		"name":       "scatter-range",
		"start_key":  url.QueryEscape(string(startKey)),
		"end_key":    url.QueryEscape(string(endKey)),
		"range_name": name,
	}
	v, err := json.Marshal(input)
	if err != nil {
		return err
	}
	scheduleURL := fmt.Sprintf("%s://%s/fidel/api/v1/schedulers", soliton.InternalHTTPSchema(), FIDelAddrs[0])
	resp, err := soliton.InternalHTTPClient().Post(scheduleURL, "application/json", bytes.NewBuffer(v))
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error(err)
	}
	return nil
}

func (h blockHandler) deleteScatterSchedule(name string) error {
	FIDelAddrs, err := h.getFIDelAddr()
	if err != nil {
		return err
	}
	scheduleURL := fmt.Sprintf("%s://%s/fidel/api/v1/schedulers/scatter-range-%s", soliton.InternalHTTPSchema(), FIDelAddrs[0], name)
	req, err := http.NewRequest(http.MethodDelete, scheduleURL, nil)
	if err != nil {
		return err
	}
	resp, err := soliton.InternalHTTPClient().Do(req)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error(err)
	}
	return nil
}

func (h blockHandler) handleScatterBlockRequest(schemaReplicant schemareplicant.SchemaReplicant, tbl causet.PhysicalBlock, w http.ResponseWriter, req *http.Request) {
	// for record
	blockID := tbl.GetPhysicalID()
	startKey, endKey := blockcodec.GetBlockHandleKeyRange(blockID)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)
	blockName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), blockID)
	err := h.addScatterSchedule(startKey, endKey, blockName)
	if err != nil {
		writeError(w, errors.Annotate(err, "scatter record error"))
		return
	}
	// for indices
	for _, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indexName := index.Meta().Name.String()
		startKey, endKey := blockcodec.GetBlockIndexKeyRange(blockID, indexID)
		startKey = codec.EncodeBytes([]byte{}, startKey)
		endKey = codec.EncodeBytes([]byte{}, endKey)
		name := blockName + "-" + indexName
		err := h.addScatterSchedule(startKey, endKey, name)
		if err != nil {
			writeError(w, errors.Annotatef(err, "scatter index(%s) error", name))
			return
		}
	}
	writeData(w, "success!")
}

func (h blockHandler) handleStopScatterBlockRequest(schemaReplicant schemareplicant.SchemaReplicant, tbl causet.PhysicalBlock, w http.ResponseWriter, req *http.Request) {
	// for record
	blockName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), tbl.GetPhysicalID())
	err := h.deleteScatterSchedule(blockName)
	if err != nil {
		writeError(w, errors.Annotate(err, "stop scatter record error"))
		return
	}
	// for indices
	for _, index := range tbl.Indices() {
		indexName := index.Meta().Name.String()
		name := blockName + "-" + indexName
		err := h.deleteScatterSchedule(name)
		if err != nil {
			writeError(w, errors.Annotatef(err, "delete scatter index(%s) error", name))
			return
		}
	}
	writeData(w, "success!")
}

func (h blockHandler) handleRegionRequest(schemaReplicant schemareplicant.SchemaReplicant, tbl causet.Block, w http.ResponseWriter, req *http.Request) {
	pi := tbl.Meta().GetPartitionInfo()
	if pi != nil {
		// Partitioned causet.
		var data []*BlockRegions
		for _, def := range pi.Definitions {
			blockRegions, err := h.getRegionsByID(tbl, def.ID, def.Name.O)
			if err != nil {
				writeError(w, err)
				return
			}

			data = append(data, blockRegions)
		}
		writeData(w, data)
		return
	}

	spacetime := tbl.Meta()
	blockRegions, err := h.getRegionsByID(tbl, spacetime.ID, spacetime.Name.O)
	if err != nil {
		writeError(w, err)
		return
	}

	writeData(w, blockRegions)
}

func (h blockHandler) getRegionsByID(tbl causet.Block, id int64, name string) (*BlockRegions, error) {
	// for record
	startKey, endKey := blockcodec.GetBlockHandleKeyRange(id)
	ctx := context.Background()
	FIDelCli := h.RegionCache.FIDelClient()
	regions, err := FIDelCli.ScanRegions(ctx, startKey, endKey, -1)
	if err != nil {
		return nil, err
	}

	recordRegions := make([]RegionMeta, 0, len(regions))
	for _, region := range regions {
		spacetime := RegionMeta{
			ID:          region.Meta.Id,
			Leader:      region.Leader,
			Peers:       region.Meta.Peers,
			RegionEpoch: region.Meta.RegionEpoch,
		}
		recordRegions = append(recordRegions, spacetime)
	}

	// for indices
	indices := make([]IndexRegions, len(tbl.Indices()))
	for i, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indices[i].Name = index.Meta().Name.String()
		indices[i].ID = indexID
		startKey, endKey := blockcodec.GetBlockIndexKeyRange(id, indexID)
		regions, err := FIDelCli.ScanRegions(ctx, startKey, endKey, -1)
		if err != nil {
			return nil, err
		}
		indexRegions := make([]RegionMeta, 0, len(regions))
		for _, region := range regions {
			spacetime := RegionMeta{
				ID:          region.Meta.Id,
				Leader:      region.Leader,
				Peers:       region.Meta.Peers,
				RegionEpoch: region.Meta.RegionEpoch,
			}
			indexRegions = append(indexRegions, spacetime)
		}
		indices[i].Regions = indexRegions
	}

	return &BlockRegions{
		BlockName:     name,
		BlockID:       id,
		Indices:       indices,
		RecordRegions: recordRegions,
	}, nil
}

func (h blockHandler) handleDiskUsageRequest(tbl causet.Block, w http.ResponseWriter) {
	blockID := tbl.Meta().ID
	var stats helper.FIDelRegionStats
	err := h.GetFIDelRegionStats(blockID, &stats)
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, stats.StorageSize)
}

type hotRegion struct {
	helper.TblIndex
	helper.RegionMetric
}
type hotRegions []hotRegion

func (rs hotRegions) Len() int {
	return len(rs)
}

func (rs hotRegions) Less(i, j int) bool {
	return rs[i].MaxHotDegree > rs[j].MaxHotDegree || (rs[i].MaxHotDegree == rs[j].MaxHotDegree && rs[i].FlowBytes > rs[j].FlowBytes)
}

func (rs hotRegions) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// ServeHTTP handles request of get region by ID.
func (h regionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	params := mux.Vars(req)
	if _, ok := params[pRegionID]; !ok {
		router := mux.CurrentRoute(req).GetName()
		if router == "RegionsMeta" {
			startKey := []byte{'m'}
			endKey := []byte{'n'}

			recordRegionIDs, err := h.RegionCache.ListRegionIDsInKeyRange(einsteindb.NewBackofferWithVars(context.Background(), 500, nil), startKey, endKey)
			if err != nil {
				writeError(w, err)
				return
			}

			recordRegions, err := h.getRegionsMeta(recordRegionIDs)
			if err != nil {
				writeError(w, err)
				return
			}
			writeData(w, recordRegions)
			return
		}
		if router == "RegionHot" {
			schemaReplicant, err := h.schemaReplicant()
			if err != nil {
				writeError(w, err)
				return
			}
			hotRead, err := h.ScrapeHotInfo(FIDelapi.HotRead, schemaReplicant.AllSchemas())
			if err != nil {
				writeError(w, err)
				return
			}
			hotWrite, err := h.ScrapeHotInfo(FIDelapi.HotWrite, schemaReplicant.AllSchemas())
			if err != nil {
				writeError(w, err)
				return
			}
			writeData(w, map[string]interface{}{
				"write": hotWrite,
				"read":  hotRead,
			})
			return
		}
		return
	}

	regionIDInt, err := strconv.ParseInt(params[pRegionID], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	regionID := uint64(regionIDInt)

	// locate region
	region, err := h.RegionCache.LocateRegionByID(einsteindb.NewBackofferWithVars(context.Background(), 500, nil), regionID)
	if err != nil {
		writeError(w, err)
		return
	}

	frameRange, err := helper.NewRegionFrameRange(region)
	if err != nil {
		writeError(w, err)
		return
	}

	// create RegionDetail from RegionFrameRange
	regionDetail := &RegionDetail{
		RegionID: regionID,
		StartKey: region.StartKey,
		EndKey:   region.EndKey,
	}
	schemaReplicant, err := h.schemaReplicant()
	if err != nil {
		writeError(w, err)
		return
	}
	// Since we need a database's name for each frame, and a causet's database name can not
	// get from causet's ID directly. Above all, here do dot process like
	// 		`for id in [frameRange.firstBlockID,frameRange.endBlockID]`
	// on [frameRange.firstBlockID,frameRange.endBlockID] is small enough.
	for _, EDB := range schemaReplicant.AllSchemas() {
		if soliton.IsMemDB(EDB.Name.L) {
			continue
		}
		for _, blockVal := range EDB.Blocks {
			regionDetail.addBlockInRange(EDB.Name.String(), blockVal, frameRange)
		}
	}
	writeData(w, regionDetail)
}

// parseQuery is used to parse query string in URL with shouldUnescape, due to golang http package can not distinguish
// query like "?a=" and "?a". We rewrite it to separate these two queries. e.g.
// "?a=" which means that a is an empty string "";
// "?a"  which means that a is null.
// If shouldUnescape is true, we use QueryUnescape to handle keys and values that will be put in m.
// If shouldUnescape is false, we don't use QueryUnescap to handle.
func parseQuery(query string, m url.Values, shouldUnescape bool) error {
	var err error
	for query != "" {
		key := query
		if i := strings.IndexAny(key, "&;"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if key == "" {
			continue
		}
		if i := strings.Index(key, "="); i >= 0 {
			value := ""
			key, value = key[:i], key[i+1:]
			if shouldUnescape {
				key, err = url.QueryUnescape(key)
				if err != nil {
					return errors.Trace(err)
				}
				value, err = url.QueryUnescape(value)
				if err != nil {
					return errors.Trace(err)
				}
			}
			m[key] = append(m[key], value)
		} else {
			if shouldUnescape {
				key, err = url.QueryUnescape(key)
				if err != nil {
					return errors.Trace(err)
				}
			}
			if _, ok := m[key]; !ok {
				m[key] = nil
			}
		}
	}
	return errors.Trace(err)
}

// ServeHTTP handles request of list a causet's regions.
func (h mvccTxnHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var data interface{}
	params := mux.Vars(req)
	var err error
	switch h.op {
	case opMvccGetByHex:
		data, err = h.handleMvccGetByHex(params)
	case opMvccGetByIdx:
		if req.URL == nil {
			err = errors.BadRequestf("Invalid URL")
			break
		}
		values := make(url.Values)
		err = parseQuery(req.URL.RawQuery, values, true)
		if err == nil {
			data, err = h.handleMvccGetByIdx(params, values)
		}
	case opMvccGetByKey:
		decode := len(req.URL.Query().Get("decode")) > 0
		data, err = h.handleMvccGetByKey(params, decode)
	case opMvccGetByTxn:
		data, err = h.handleMvccGetByTxn(params)
	default:
		err = errors.NotSupportedf("Operation not supported.")
	}
	if err != nil {
		writeError(w, err)
	} else {
		writeData(w, data)
	}
}

func extractBlockAndPartitionName(str string) (string, string) {
	// extract causet name and partition name from this "causet(partition)":
	// A sane person would not let the the causet name or partition name contain '('.
	start := strings.IndexByte(str, '(')
	if start == -1 {
		return str, ""
	}
	end := strings.IndexByte(str, ')')
	if end == -1 {
		return str, ""
	}
	return str[:start], str[start+1 : end]
}

// handleMvccGetByIdx gets MVCC info by an index key.
func (h mvccTxnHandler) handleMvccGetByIdx(params map[string]string, values url.Values) (interface{}, error) {
	dbName := params[FIDelBName]
	blockName := params[pBlockName]
	handleStr := params[pHandle]

	t, err := h.getBlock(dbName, blockName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var idxDefCauss []*perceptron.DeferredCausetInfo
	var idx causet.Index
	for _, v := range t.Indices() {
		if strings.EqualFold(v.Meta().Name.String(), params[pIndexName]) {
			for _, c := range v.Meta().DeferredCausets {
				idxDefCauss = append(idxDefCauss, t.Meta().DeferredCausets[c.Offset])
			}
			idx = v
			break
		}
	}
	if idx == nil {
		return nil, errors.NotFoundf("Index %s not found!", params[pIndexName])
	}
	return h.getMvccByIdxValue(idx, values, idxDefCauss, handleStr)
}

func (h mvccTxnHandler) handleMvccGetByKey(params map[string]string, decodeData bool) (interface{}, error) {
	handle, err := strconv.ParseInt(params[pHandle], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tb, err := h.getBlock(params[FIDelBName], params[pBlockName])
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := h.getMvccByHandle(tb.GetPhysicalID(), handle)
	if err != nil {
		return nil, err
	}
	if !decodeData {
		return resp, nil
	}
	defCausMap := make(map[int64]*types.FieldType, 3)
	for _, defCaus := range tb.Meta().DeferredCausets {
		defCausMap[defCaus.ID] = &defCaus.FieldType
	}

	respValue := resp.Value
	var result interface{} = resp
	if respValue.Info != nil {
		quantum := make(map[string][]map[string]string)
		for _, w := range respValue.Info.Writes {
			if len(w.ShortValue) > 0 {
				quantum[strconv.FormatUint(w.StartTs, 10)], err = h.decodeMvccData(w.ShortValue, defCausMap, tb.Meta())
			}
		}

		for _, v := range respValue.Info.Values {
			if len(v.Value) > 0 {
				quantum[strconv.FormatUint(v.StartTs, 10)], err = h.decodeMvccData(v.Value, defCausMap, tb.Meta())
			}
		}

		if len(quantum) > 0 {
			re := map[string]interface{}{
				"key":  resp.Key,
				"info": respValue.Info,
				"data": quantum,
			}
			if err != nil {
				re["decode_error"] = err.Error()
			}
			result = re
		}
	}

	return result, nil
}

func (h mvccTxnHandler) decodeMvccData(bs []byte, defCausMap map[int64]*types.FieldType, tb *perceptron.BlockInfo) ([]map[string]string, error) {
	rs, err := blockcodec.DecodeRowToCausetMap(bs, defCausMap, time.UTC)
	var record []map[string]string
	for _, defCaus := range tb.DeferredCausets {
		if c, ok := rs[defCaus.ID]; ok {
			data := "nil"
			if !c.IsNull() {
				data, err = c.ToString()
			}
			record = append(record, map[string]string{defCaus.Name.O: data})
		}
	}
	return record, err
}

func (h *mvccTxnHandler) handleMvccGetByTxn(params map[string]string) (interface{}, error) {
	startTS, err := strconv.ParseInt(params[pStartTS], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	blockID, err := h.getBlockID(params[FIDelBName], params[pBlockName])
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey := blockcodec.EncodeBlockPrefix(blockID)
	endKey := blockcodec.EncodeRowKeyWithHandle(blockID, ekv.IntHandle(math.MaxInt64))
	return h.getMvccByStartTs(uint64(startTS), startKey, endKey)
}

// serverInfo is used to report the servers info when do http request.
type serverInfo struct {
	IsTenant bool `json:"is_tenant"`
	*infosync.ServerInfo
}

// ServeHTTP handles request of dbs server info.
func (h serverInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	do, err := stochastik.GetPetri(h.CausetStore.(ekv.CausetStorage))
	if err != nil {
		writeError(w, errors.New("create stochastik error"))
		log.Error(err)
		return
	}
	info := serverInfo{}
	info.ServerInfo, err = infosync.GetServerInfo()
	if err != nil {
		writeError(w, err)
		log.Error(err)
		return
	}
	info.IsTenant = do.DBS().TenantManager().IsTenant()
	writeData(w, info)
}

// clusterServerInfo is used to report cluster servers info when do http request.
type clusterServerInfo struct {
	ServersNum                   int                             `json:"servers_num,omitempty"`
	TenantID                     string                          `json:"tenant_id"`
	IsAllServerVersionConsistent bool                            `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffVersions       []infosync.ServerVersionInfo    `json:"all_servers_diff_versions,omitempty"`
	AllServersInfo               map[string]*infosync.ServerInfo `json:"all_servers_info,omitempty"`
}

// ServeHTTP handles request of all dbs servers info.
func (h allServerInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	do, err := stochastik.GetPetri(h.CausetStore.(ekv.CausetStorage))
	if err != nil {
		writeError(w, errors.New("create stochastik error"))
		log.Error(err)
		return
	}
	ctx := context.Background()
	allServersInfo, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		writeError(w, errors.New("dbs server information not found"))
		log.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	tenantID, err := do.DBS().TenantManager().GetTenantID(ctx)
	cancel()
	if err != nil {
		writeError(w, errors.New("dbs server information not found"))
		log.Error(err)
		return
	}
	allVersionsMap := map[infosync.ServerVersionInfo]struct{}{}
	allVersions := make([]infosync.ServerVersionInfo, 0, len(allServersInfo))
	for _, v := range allServersInfo {
		if _, ok := allVersionsMap[v.ServerVersionInfo]; ok {
			continue
		}
		allVersionsMap[v.ServerVersionInfo] = struct{}{}
		allVersions = append(allVersions, v.ServerVersionInfo)
	}
	clusterInfo := clusterServerInfo{
		ServersNum: len(allServersInfo),
		TenantID:   tenantID,
		// len(allVersions) = 1 indicates there has only 1 milevadb version in cluster, so all server versions are consistent.
		IsAllServerVersionConsistent: len(allVersions) == 1,
		AllServersInfo:               allServersInfo,
	}
	// if IsAllServerVersionConsistent is false, return the all milevadb servers version.
	if !clusterInfo.IsAllServerVersionConsistent {
		clusterInfo.AllServersDiffVersions = allVersions
	}
	writeData(w, clusterInfo)
}

// dbBlockInfo is used to report the database, causet information and the current schemaReplicant version.
type dbBlockInfo struct {
	DBInfo        *perceptron.DBInfo    `json:"db_info"`
	BlockInfo     *perceptron.BlockInfo `json:"block_info"`
	SchemaVersion int64                 `json:"schema_version"`
}

// ServeHTTP handles request of database information and causet information by blockID.
func (h dbBlockHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	blockID := params[pBlockID]
	physicalID, err := strconv.Atoi(blockID)
	if err != nil {
		writeError(w, errors.Errorf("Wrong blockID: %v", blockID))
		return
	}

	schemaReplicant, err := h.schemaReplicant()
	if err != nil {
		writeError(w, err)
		return
	}

	dbTblInfo := dbBlockInfo{
		SchemaVersion: schemaReplicant.SchemaMetaVersion(),
	}
	tbl, ok := schemaReplicant.BlockByID(int64(physicalID))
	if ok {
		dbTblInfo.BlockInfo = tbl.Meta()
		dbInfo, ok := schemaReplicant.SchemaByBlock(dbTblInfo.BlockInfo)
		if !ok {
			logutil.BgLogger().Error("can not find the database of the causet", zap.Int64("causet id", dbTblInfo.BlockInfo.ID), zap.String("causet name", dbTblInfo.BlockInfo.Name.L))
			writeError(w, schemareplicant.ErrBlockNotExists.GenWithStack("Block which ID = %s does not exist.", blockID))
			return
		}
		dbTblInfo.DBInfo = dbInfo
		writeData(w, dbTblInfo)
		return
	}
	// The physicalID maybe a partition ID of the partition-causet.
	tbl, dbInfo := schemaReplicant.FindBlockByPartitionID(int64(physicalID))
	if tbl == nil {
		writeError(w, schemareplicant.ErrBlockNotExists.GenWithStack("Block which ID = %s does not exist.", blockID))
		return
	}
	dbTblInfo.BlockInfo = tbl.Meta()
	dbTblInfo.DBInfo = dbInfo
	writeData(w, dbTblInfo)
}

// ServeHTTP handles request of MilevaDB metric profile.
func (h profileHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	sctx, err := stochastik.CreateStochastik(h.CausetStore)
	if err != nil {
		writeError(w, err)
		return
	}
	var start, end time.Time
	if req.FormValue("end") != "" {
		end, err = time.ParseInLocation(time.RFC3339, req.FormValue("end"), sctx.GetStochastikVars().Location())
		if err != nil {
			writeError(w, err)
			return
		}
	} else {
		end = time.Now()
	}
	if req.FormValue("start") != "" {
		start, err = time.ParseInLocation(time.RFC3339, req.FormValue("start"), sctx.GetStochastikVars().Location())
		if err != nil {
			writeError(w, err)
			return
		}
	} else {
		start = end.Add(-time.Minute * 10)
	}
	valueTp := req.FormValue("type")
	pb, err := interlock.NewProfileBuilder(sctx, start, end, valueTp)
	if err != nil {
		writeError(w, err)
		return
	}
	err = pb.DefCauslect()
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = w.Write(pb.Build())
	terror.Log(errors.Trace(err))
}

// testHandler is the handler for tests. It's convenient to provide some APIs for integration tests.
type testHandler struct {
	*einsteindbHandlerTool
	gcIsRunning uint32
}

// ServeHTTP handles test related requests.
func (h *testHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	mod := strings.ToLower(params["mod"])
	op := strings.ToLower(params["op"])

	switch mod {
	case "gc":
		h.handleGC(op, w, req)
	default:
		writeError(w, errors.NotSupportedf("module(%s)", mod))
	}
}

// Supported operations:
//   * resolvelock?safepoint={uint64}&physical={bool}:
//	   * safepoint: resolve all locks whose timestamp is less than the safepoint.
//	   * physical: whether it uses physical(green GC) mode to scan locks. Default is true.
func (h *testHandler) handleGC(op string, w http.ResponseWriter, req *http.Request) {
	if !atomic.CompareAndSwapUint32(&h.gcIsRunning, 0, 1) {
		writeError(w, errors.New("GC is running"))
		return
	}
	defer atomic.StoreUint32(&h.gcIsRunning, 0)

	switch op {
	case "resolvelock":
		h.handleGCResolveLocks(w, req)
	default:
		writeError(w, errors.NotSupportedf("operation(%s)", op))
	}
}

func (h *testHandler) handleGCResolveLocks(w http.ResponseWriter, req *http.Request) {
	s := req.FormValue("safepoint")
	safePoint, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		writeError(w, errors.Errorf("parse safePoint(%s) failed", s))
		return
	}
	usePhysical := true
	s = req.FormValue("physical")
	if s != "" {
		usePhysical, err = strconv.ParseBool(s)
		if err != nil {
			writeError(w, errors.Errorf("parse physical(%s) failed", s))
			return
		}
	}

	ctx := req.Context()
	logutil.Logger(ctx).Info("start resolving locks", zap.Uint64("safePoint", safePoint), zap.Bool("physical", usePhysical))
	physicalUsed, err := gcworker.RunResolveLocks(ctx, h.CausetStore, h.RegionCache.FIDelClient(), safePoint, "testGCWorker", 3, usePhysical)
	if err != nil {
		writeError(w, errors.Annotate(err, "resolveLocks failed"))
	} else {
		writeData(w, map[string]interface{}{
			"physicalUsed": physicalUsed,
		})
	}
}