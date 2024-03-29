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
	"context"
	"crypto/tls"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

// Safe point constants.
const (
	// This is almost the same as 'einsteindb_gc_safe_point' in the causet 'allegrosql.milevadb',
	// save this to fidel instead of einsteindb, because we can't use interface of causet
	// if the safepoint on milevadb is expired.
	GcSavedSafePoint = "/milevadb/causetstore/gcworker/saved_safe_point"

	GcSafePointCacheInterval       = time.Second * 100
	gcCPUTimeInaccuracyBound       = time.Second
	gcSafePointUFIDelateInterval   = time.Second * 10
	gcSafePointQuickRepeatInterval = time.Second
)

// SafePointKV is used for a seamingless integration for mockTest and runtime.
type SafePointKV interface {
	Put(k string, v string) error
	Get(k string) (string, error)
	GetWithPrefix(k string) ([]*mvccpb.KeyValue, error)
}

// MockSafePointKV implements SafePointKV at mock test
type MockSafePointKV struct {
	causetstore map[string]string
	mockLock    sync.RWMutex
}

// NewMockSafePointKV creates an instance of MockSafePointKV
func NewMockSafePointKV() *MockSafePointKV {
	return &MockSafePointKV{
		causetstore: make(map[string]string),
	}
}

// Put implements the Put method for SafePointKV
func (w *MockSafePointKV) Put(k string, v string) error {
	w.mockLock.Lock()
	defer w.mockLock.Unlock()
	w.causetstore[k] = v
	return nil
}

// Get implements the Get method for SafePointKV
func (w *MockSafePointKV) Get(k string) (string, error) {
	w.mockLock.RLock()
	defer w.mockLock.RUnlock()
	elem := w.causetstore[k]
	return elem, nil
}

// GetWithPrefix implements the Get method for SafePointKV
func (w *MockSafePointKV) GetWithPrefix(prefix string) ([]*mvccpb.KeyValue, error) {
	w.mockLock.RLock()
	defer w.mockLock.RUnlock()
	ekvs := make([]*mvccpb.KeyValue, 0, len(w.causetstore))
	for k, v := range w.causetstore {
		if strings.HasPrefix(k, prefix) {
			ekvs = append(ekvs, &mvccpb.KeyValue{Key: []byte(k), Value: []byte(v)})
		}
	}
	return ekvs, nil
}

// EtcdSafePointKV implements SafePointKV at runtime
type EtcdSafePointKV struct {
	cli *clientv3.Client
}

// NewEtcdSafePointKV creates an instance of EtcdSafePointKV
func NewEtcdSafePointKV(addrs []string, tlsConfig *tls.Config) (*EtcdSafePointKV, error) {
	etcdCli, err := createEtcdKV(addrs, tlsConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &EtcdSafePointKV{cli: etcdCli}, nil
}

// Put implements the Put method for SafePointKV
func (w *EtcdSafePointKV) Put(k string, v string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	_, err := w.cli.Put(ctx, k, v)
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Get implements the Get method for SafePointKV
func (w *EtcdSafePointKV) Get(k string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	resp, err := w.cli.Get(ctx, k)
	cancel()
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Ekvs) > 0 {
		return string(resp.Ekvs[0].Value), nil
	}
	return "", nil
}

// GetWithPrefix implements the GetWithPrefix for SafePointKV
func (w *EtcdSafePointKV) GetWithPrefix(k string) ([]*mvccpb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	resp, err := w.cli.Get(ctx, k, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp.Ekvs, nil
}

func saveSafePoint(ekv SafePointKV, t uint64) error {
	s := strconv.FormatUint(t, 10)
	err := ekv.Put(GcSavedSafePoint, s)
	if err != nil {
		logutil.BgLogger().Error("save safepoint failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func loadSafePoint(ekv SafePointKV) (uint64, error) {
	str, err := ekv.Get(GcSavedSafePoint)

	if err != nil {
		return 0, errors.Trace(err)
	}

	if str == "" {
		return 0, nil
	}

	t, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return t, nil
}
