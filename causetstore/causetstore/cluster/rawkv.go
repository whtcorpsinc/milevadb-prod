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
	"time"

	"github.com/einsteindb/fidel/client"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/einsteindbrpc"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/metrics"
)

var (
	// MaxRawKVScanLimit is the maximum scan limit for rawekv Scan.
	MaxRawKVScanLimit = 10240
	// ErrMaxScanLimitExceeded is returned when the limit for rawekv Scan is to large.
	ErrMaxScanLimitExceeded = errors.New("limit should be less than MaxRawKVScanLimit")
)

var (
	einsteindbRawekvCmdHistogramWithGet           = metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues("get")
	einsteindbRawekvCmdHistogramWithBatchGet      = metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues("batch_get")
	einsteindbRawekvCmdHistogramWithBatchPut      = metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues("batch_put")
	einsteindbRawekvCmdHistogramWithDelete        = metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues("delete")
	einsteindbRawekvCmdHistogramWithBatchDelete   = metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues("batch_delete")
	einsteindbRawekvCmdHistogramWithRawScan       = metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues("raw_scan")
	einsteindbRawekvCmdHistogramWithRawReversScan = metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues("raw_reverse_scan")

	einsteindbRawekvSizeHistogramWithKey   = metrics.EinsteinDBRawekvSizeHistogram.WithLabelValues("key")
	einsteindbRawekvSizeHistogramWithValue = metrics.EinsteinDBRawekvSizeHistogram.WithLabelValues("value")
)

const (
	// rawBatchPutSize is the maximum size limit for rawekv each batch put request.
	rawBatchPutSize = 16 * 1024
	// rawBatchPairCount is the maximum limit for rawekv each batch get/delete request.
	rawBatchPairCount = 512
)

// RawKVClient is a client of EinsteinDB server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type RawKVClient struct {
	clusterID   uint64
	regionCache *RegionCache
	FIDelClient fidel.Client
	rpcClient   Client
}

// NewRawKVClient creates a client with FIDel cluster addrs.
func NewRawKVClient(FIDelAddrs []string, security config.Security, opts ...fidel.ClientOption) (*RawKVClient, error) {
	FIDelCli, err := fidel.NewClient(FIDelAddrs, fidel.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	}, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RawKVClient{
		clusterID:   FIDelCli.GetClusterID(context.TODO()),
		regionCache: NewRegionCache(FIDelCli),
		FIDelClient: FIDelCli,
		rpcClient:   newRPCClient(security),
	}, nil
}

// Close closes the client.
func (c *RawKVClient) Close() error {
	if c.FIDelClient != nil {
		c.FIDelClient.Close()
	}
	if c.regionCache != nil {
		c.regionCache.Close()
	}
	if c.rpcClient == nil {
		return nil
	}
	return c.rpcClient.Close()
}

// ClusterID returns the EinsteinDB cluster ID.
func (c *RawKVClient) ClusterID() uint64 {
	return c.clusterID
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *RawKVClient) Get(key []byte) ([]byte, error) {
	start := time.Now()
	defer func() { einsteindbRawekvCmdHistogramWithGet.Observe(time.Since(start).Seconds()) }()

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawGet, &ekvrpcpb.RawGetRequest{Key: key})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.Resp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*ekvrpcpb.RawGetResponse)
	if cmdResp.GetError() != "" {
		return nil, errors.New(cmdResp.GetError())
	}
	if len(cmdResp.Value) == 0 {
		return nil, nil
	}
	return cmdResp.Value, nil
}

// BatchGet queries values with the keys.
func (c *RawKVClient) BatchGet(keys [][]byte) ([][]byte, error) {
	start := time.Now()
	defer func() {
		einsteindbRawekvCmdHistogramWithBatchGet.Observe(time.Since(start).Seconds())
	}()

	bo := NewBackofferWithVars(context.Background(), rawekvMaxBackoff, nil)
	resp, err := c.sendBatchReq(bo, keys, einsteindbrpc.CmdRawBatchGet)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp.Resp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*ekvrpcpb.RawBatchGetResponse)

	keyToValue := make(map[string][]byte, len(keys))
	for _, pair := range cmdResp.Pairs {
		keyToValue[string(pair.Key)] = pair.Value
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = keyToValue[string(key)]
	}
	return values, nil
}

// Put stores a key-value pair to EinsteinDB.
func (c *RawKVClient) Put(key, value []byte) error {
	start := time.Now()
	defer func() { einsteindbRawekvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds()) }()
	einsteindbRawekvSizeHistogramWithKey.Observe(float64(len(key)))
	einsteindbRawekvSizeHistogramWithValue.Observe(float64(len(value)))

	if len(value) == 0 {
		return errors.New("empty value is not supported")
	}

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawPut, &ekvrpcpb.RawPutRequest{
		Key:   key,
		Value: value,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*ekvrpcpb.RawPutResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchPut stores key-value pairs to EinsteinDB.
func (c *RawKVClient) BatchPut(keys, values [][]byte) error {
	start := time.Now()
	defer func() {
		einsteindbRawekvCmdHistogramWithBatchPut.Observe(time.Since(start).Seconds())
	}()

	if len(keys) != len(values) {
		return errors.New("the len of keys is not equal to the len of values")
	}
	for _, value := range values {
		if len(value) == 0 {
			return errors.New("empty value is not supported")
		}
	}
	bo := NewBackofferWithVars(context.Background(), rawekvMaxBackoff, nil)
	err := c.sendBatchPut(bo, keys, values)
	return errors.Trace(err)
}

// Delete deletes a key-value pair from EinsteinDB.
func (c *RawKVClient) Delete(key []byte) error {
	start := time.Now()
	defer func() { einsteindbRawekvCmdHistogramWithDelete.Observe(time.Since(start).Seconds()) }()

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawDelete, &ekvrpcpb.RawDeleteRequest{
		Key: key,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*ekvrpcpb.RawDeleteResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// BatchDelete deletes key-value pairs from EinsteinDB
func (c *RawKVClient) BatchDelete(keys [][]byte) error {
	start := time.Now()
	defer func() {
		einsteindbRawekvCmdHistogramWithBatchDelete.Observe(time.Since(start).Seconds())
	}()

	bo := NewBackofferWithVars(context.Background(), rawekvMaxBackoff, nil)
	resp, err := c.sendBatchReq(bo, keys, einsteindbrpc.CmdRawBatchDelete)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*ekvrpcpb.RawBatchDeleteResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

// DeleteRange deletes all key-value pairs in a range from EinsteinDB
func (c *RawKVClient) DeleteRange(startKey []byte, endKey []byte) error {
	start := time.Now()
	var err error
	defer func() {
		var label = "delete_range"
		if err != nil {
			label += "_error"
		}
		metrics.EinsteinDBRawekvCmdHistogram.WithLabelValues(label).Observe(time.Since(start).Seconds())
	}()

	// Process each affected region respectively
	for !bytes.Equal(startKey, endKey) {
		var resp *einsteindbrpc.Response
		var actualEndKey []byte
		resp, actualEndKey, err = c.sendDeleteRangeReq(startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*ekvrpcpb.RawDeleteRangeResponse)
		if cmdResp.GetError() != "" {
			return errors.New(cmdResp.GetError())
		}
		startKey = actualEndKey
	}

	return nil
}

// Scan queries continuous ekv pairs in range [startKey, endKey), up to limit pairs.
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, push a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Scan(push(startKey, '\0'), push(endKey, '\0'), limit)`.
func (c *RawKVClient) Scan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() { einsteindbRawekvCmdHistogramWithRawScan.Observe(time.Since(start).Seconds()) }()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit && (len(endKey) == 0 || bytes.Compare(startKey, endKey) < 0) {
		req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawScan, &ekvrpcpb.RawScanRequest{
			StartKey: startKey,
			EndKey:   endKey,
			Limit:    uint32(limit - len(keys)),
		})
		resp, loc, err := c.sendReq(startKey, req, false)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if resp.Resp == nil {
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*ekvrpcpb.RawScanResponse)
		for _, pair := range cmdResp.Ekvs {
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		startKey = loc.EndKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

// ReverseScan queries continuous ekv pairs in range [endKey, startKey), up to limit pairs.
// Direction is different from Scan, upper to lower.
// If endKey is empty, it means unbounded.
// If you want to include the startKey or exclude the endKey, push a '\0' to the key. For example, to scan
// (endKey, startKey], you can write:
// `ReverseScan(push(startKey, '\0'), push(endKey, '\0'), limit)`.
// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
func (c *RawKVClient) ReverseScan(startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	start := time.Now()
	defer func() {
		einsteindbRawekvCmdHistogramWithRawReversScan.Observe(time.Since(start).Seconds())
	}()

	if limit > MaxRawKVScanLimit {
		return nil, nil, errors.Trace(ErrMaxScanLimitExceeded)
	}

	for len(keys) < limit && bytes.Compare(startKey, endKey) > 0 {
		req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawScan, &ekvrpcpb.RawScanRequest{
			StartKey: startKey,
			EndKey:   endKey,
			Limit:    uint32(limit - len(keys)),
			Reverse:  true,
		})
		resp, loc, err := c.sendReq(startKey, req, true)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if resp.Resp == nil {
			return nil, nil, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*ekvrpcpb.RawScanResponse)
		for _, pair := range cmdResp.Ekvs {
			keys = append(keys, pair.Key)
			values = append(values, pair.Value)
		}
		startKey = loc.StartKey
		if len(startKey) == 0 {
			break
		}
	}
	return
}

func (c *RawKVClient) sendReq(key []byte, req *einsteindbrpc.Request, reverse bool) (*einsteindbrpc.Response, *KeyLocation, error) {
	bo := NewBackofferWithVars(context.Background(), rawekvMaxBackoff, nil)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		var loc *KeyLocation
		var err error
		if reverse {
			loc, err = c.regionCache.LocateEndKey(bo, key)
		} else {
			loc, err = c.regionCache.LocateKey(bo, key)
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, loc, nil
	}
}

func (c *RawKVClient) sendBatchReq(bo *Backoffer, keys [][]byte, cmdType einsteindbrpc.CmdType) (*einsteindbrpc.Response, error) { // split the keys
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var batches []batch
	for regionID, groupKeys := range groups {
		batches = appendKeyBatches(batches, regionID, groupKeys, rawBatchPairCount)
	}
	bo, cancel := bo.Fork()
	ches := make(chan singleBatchResp, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ches <- c.doBatchReq(singleBatchBackoffer, batch1, cmdType)
		}()
	}

	var firstError error
	var resp *einsteindbrpc.Response
	switch cmdType {
	case einsteindbrpc.CmdRawBatchGet:
		resp = &einsteindbrpc.Response{Resp: &ekvrpcpb.RawBatchGetResponse{}}
	case einsteindbrpc.CmdRawBatchDelete:
		resp = &einsteindbrpc.Response{Resp: &ekvrpcpb.RawBatchDeleteResponse{}}
	}
	for i := 0; i < len(batches); i++ {
		singleResp, ok := <-ches
		if ok {
			if singleResp.err != nil {
				cancel()
				if firstError == nil {
					firstError = singleResp.err
				}
			} else if cmdType == einsteindbrpc.CmdRawBatchGet {
				cmdResp := singleResp.resp.Resp.(*ekvrpcpb.RawBatchGetResponse)
				resp.Resp.(*ekvrpcpb.RawBatchGetResponse).Pairs = append(resp.Resp.(*ekvrpcpb.RawBatchGetResponse).Pairs, cmdResp.Pairs...)
			}
		}
	}

	return resp, firstError
}

func (c *RawKVClient) doBatchReq(bo *Backoffer, batch batch, cmdType einsteindbrpc.CmdType) singleBatchResp {
	var req *einsteindbrpc.Request
	switch cmdType {
	case einsteindbrpc.CmdRawBatchGet:
		req = einsteindbrpc.NewRequest(cmdType, &ekvrpcpb.RawBatchGetRequest{
			Keys: batch.keys,
		})
	case einsteindbrpc.CmdRawBatchDelete:
		req = einsteindbrpc.NewRequest(cmdType, &ekvrpcpb.RawBatchDeleteRequest{
			Keys: batch.keys,
		})
	}

	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, readTimeoutShort)

	batchResp := singleBatchResp{}
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	if regionErr != nil {
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			batchResp.err = errors.Trace(err)
			return batchResp
		}
		resp, err = c.sendBatchReq(bo, batch.keys, cmdType)
		batchResp.resp = resp
		batchResp.err = err
		return batchResp
	}

	switch cmdType {
	case einsteindbrpc.CmdRawBatchGet:
		batchResp.resp = resp
	case einsteindbrpc.CmdRawBatchDelete:
		if resp.Resp == nil {
			batchResp.err = errors.Trace(ErrBodyMissing)
			return batchResp
		}
		cmdResp := resp.Resp.(*ekvrpcpb.RawBatchDeleteResponse)
		if cmdResp.GetError() != "" {
			batchResp.err = errors.New(cmdResp.GetError())
			return batchResp
		}
		batchResp.resp = resp
	}
	return batchResp
}

// sendDeleteRangeReq sends a raw delete range request and returns the response and the actual endKey.
// If the given range spans over more than one regions, the actual endKey is the end of the first region.
// We can't use sendReq directly, because we need to know the end of the region before we send the request
// TODO: Is there any better way to avoid duplicating code with func `sendReq` ?
func (c *RawKVClient) sendDeleteRangeReq(startKey []byte, endKey []byte) (*einsteindbrpc.Response, []byte, error) {
	bo := NewBackofferWithVars(context.Background(), rawekvMaxBackoff, nil)
	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	for {
		loc, err := c.regionCache.LocateKey(bo, startKey)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		actualEndKey := endKey
		if len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, endKey) < 0 {
			actualEndKey = loc.EndKey
		}

		req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawDeleteRange, &ekvrpcpb.RawDeleteRangeRequest{
			StartKey: startKey,
			EndKey:   actualEndKey,
		})

		resp, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			continue
		}
		return resp, actualEndKey, nil
	}
}

func (c *RawKVClient) sendBatchPut(bo *Backoffer, keys, values [][]byte) error {
	keyToValue := make(map[string][]byte, len(keys))
	for i, key := range keys {
		keyToValue[string(key)] = values[i]
	}
	groups, _, err := c.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}
	var batches []batch
	// split the keys by size and RegionVerID
	for regionID, groupKeys := range groups {
		batches = appendBatches(batches, regionID, groupKeys, keyToValue, rawBatchPutSize)
	}
	bo, cancel := bo.Fork()
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		batch1 := batch
		go func() {
			singleBatchBackoffer, singleBatchCancel := bo.Fork()
			defer singleBatchCancel()
			ch <- c.doBatchPut(singleBatchBackoffer, batch1)
		}()
	}

	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			cancel()
			// catch the first error
			if err == nil {
				err = e
			}
		}
	}
	return errors.Trace(err)
}

func appendKeyBatches(batches []batch, regionID RegionVerID, groupKeys [][]byte, limit int) []batch {
	var keys [][]byte
	for start, count := 0, 0; start < len(groupKeys); start++ {
		if count > limit {
			batches = append(batches, batch{regionID: regionID, keys: keys})
			keys = make([][]byte, 0, limit)
			count = 0
		}
		keys = append(keys, groupKeys[start])
		count++
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys})
	}
	return batches
}

func appendBatches(batches []batch, regionID RegionVerID, groupKeys [][]byte, keyToValue map[string][]byte, limit int) []batch {
	var start, size int
	var keys, values [][]byte
	for start = 0; start < len(groupKeys); start++ {
		if size >= limit {
			batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
			keys = make([][]byte, 0)
			values = make([][]byte, 0)
			size = 0
		}
		key := groupKeys[start]
		value := keyToValue[string(key)]
		keys = append(keys, key)
		values = append(values, value)
		size += len(key)
		size += len(value)
	}
	if len(keys) != 0 {
		batches = append(batches, batch{regionID: regionID, keys: keys, values: values})
	}
	return batches
}

func (c *RawKVClient) doBatchPut(bo *Backoffer, batch batch) error {
	ekvPair := make([]*ekvrpcpb.EkvPair, 0, len(batch.keys))
	for i, key := range batch.keys {
		ekvPair = append(ekvPair, &ekvrpcpb.EkvPair{Key: key, Value: batch.values[i]})
	}

	req := einsteindbrpc.NewRequest(einsteindbrpc.CmdRawBatchPut, &ekvrpcpb.RawBatchPutRequest{Pairs: ekvPair})

	sender := NewRegionRequestSender(c.regionCache, c.rpcClient)
	resp, err := sender.SendReq(bo, req, batch.regionID, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// recursive call
		return c.sendBatchPut(bo, batch.keys, batch.values)
	}

	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*ekvrpcpb.RawBatchPutResponse)
	if cmdResp.GetError() != "" {
		return errors.New(cmdResp.GetError())
	}
	return nil
}

type batch struct {
	regionID RegionVerID
	keys     [][]byte
	values   [][]byte
}

type singleBatchResp struct {
	resp *einsteindbrpc.Response
	err  error
}
