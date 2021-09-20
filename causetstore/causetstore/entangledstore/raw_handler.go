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

package entangledstore

import (
	"bytes"
	"context"
	"sync"

	"github.com/ngaut/entangledstore/lockstore"
	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
)

type rawHandler struct {
	mu          sync.RWMutex
	causetstore *lockstore.MemStore
}

func newRawHandler() *rawHandler {
	return &rawHandler{
		causetstore: lockstore.NewMemStore(4096),
	}
}

func (h *rawHandler) RawGet(_ context.Context, req *ekvrpcpb.RawGetRequest) (*ekvrpcpb.RawGetResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	val := h.causetstore.Get(req.Key, nil)
	return &ekvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: len(val) == 0,
	}, nil
}

func (h *rawHandler) RawBatchGet(_ context.Context, req *ekvrpcpb.RawBatchGetRequest) (*ekvrpcpb.RawBatchGetResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	pairs := make([]*ekvrpcpb.EkvPair, len(req.Keys))
	for i, key := range req.Keys {
		pairs[i] = &ekvrpcpb.EkvPair{
			Key:   key,
			Value: h.causetstore.Get(key, nil),
		}
	}
	return &ekvrpcpb.RawBatchGetResponse{Pairs: pairs}, nil
}

func (h *rawHandler) RawPut(_ context.Context, req *ekvrpcpb.RawPutRequest) (*ekvrpcpb.RawPutResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.causetstore.Put(req.Key, req.Value)
	return &ekvrpcpb.RawPutResponse{}, nil
}

func (h *rawHandler) RawBatchPut(_ context.Context, req *ekvrpcpb.RawBatchPutRequest) (*ekvrpcpb.RawBatchPutResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, pair := range req.Pairs {
		h.causetstore.Put(pair.Key, pair.Value)
	}
	return &ekvrpcpb.RawBatchPutResponse{}, nil
}

func (h *rawHandler) RawDelete(_ context.Context, req *ekvrpcpb.RawDeleteRequest) (*ekvrpcpb.RawDeleteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.causetstore.Delete(req.Key)
	return &ekvrpcpb.RawDeleteResponse{}, nil
}

func (h *rawHandler) RawBatchDelete(_ context.Context, req *ekvrpcpb.RawBatchDeleteRequest) (*ekvrpcpb.RawBatchDeleteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, key := range req.Keys {
		h.causetstore.Delete(key)
	}
	return &ekvrpcpb.RawBatchDeleteResponse{}, nil
}

func (h *rawHandler) RawDeleteRange(_ context.Context, req *ekvrpcpb.RawDeleteRangeRequest) (*ekvrpcpb.RawDeleteRangeResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	it := h.causetstore.NewIterator()
	var keys [][]byte
	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), req.EndKey) >= 0 {
			break
		}
		keys = append(keys, safeCopy(it.Key()))
	}
	for _, key := range keys {
		h.causetstore.Delete(key)
	}
	return &ekvrpcpb.RawDeleteRangeResponse{}, nil
}

func (h *rawHandler) RawScan(_ context.Context, req *ekvrpcpb.RawScanRequest) (*ekvrpcpb.RawScanResponse, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	it := h.causetstore.NewIterator()
	var pairs []*ekvrpcpb.EkvPair
	if !req.Reverse {
		for it.Seek(req.StartKey); it.Valid(); it.Next() {
			if len(pairs) >= int(req.Limit) {
				break
			}
			if len(req.EndKey) > 0 && bytes.Compare(it.Key(), req.EndKey) >= 0 {
				break
			}
			pairs = h.appendPair(pairs, it)
		}
	} else {
		for it.SeekForPrev(req.StartKey); it.Valid(); it.Prev() {
			if bytes.Equal(it.Key(), req.StartKey) {
				continue
			}
			if len(pairs) >= int(req.Limit) {
				break
			}
			if bytes.Compare(it.Key(), req.EndKey) < 0 {
				break
			}
			pairs = h.appendPair(pairs, it)
		}
	}
	return &ekvrpcpb.RawScanResponse{Ekvs: pairs}, nil
}

func (h *rawHandler) appendPair(pairs []*ekvrpcpb.EkvPair, it *lockstore.Iterator) []*ekvrpcpb.EkvPair {
	pair := &ekvrpcpb.EkvPair{
		Key:   safeCopy(it.Key()),
		Value: safeCopy(it.Value()),
	}
	return append(pairs, pair)
}

func safeCopy(val []byte) []byte {
	return append([]byte{}, val...)
}
