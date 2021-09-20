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

package tenant

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	newStochastikRetryInterval = 200 * time.Millisecond
	logIntervalCnt             = int(3 * time.Second / newStochastikRetryInterval)
)

// Manager is used to campaign the tenant and manage the tenant information.
type Manager interface {
	// ID returns the ID of the manager.
	ID() string
	// IsTenant returns whether the tenantManager is the tenant.
	IsTenant() bool
	// RetireTenant make the manager to be a not tenant. It's exported for testing.
	RetireTenant()
	// GetTenantID gets the tenant ID.
	GetTenantID(ctx context.Context) (string, error)
	// CampaignTenant campaigns the tenant.
	CampaignTenant() error
	// ResignTenant lets the tenant start a new election.
	ResignTenant(ctx context.Context) error
	// Cancel cancels this etcd tenantManager campaign.
	Cancel()
}

const (
	NewStochastikDefaultRetryCnt = 3

	NewStochastikRetryUnlimited = math.MaxInt64
	keyOFIDelefaultTimeout      = 5 * time.Second
)

// DBSTenantChecker is used to check whether milevadb is tenant.
type DBSTenantChecker interface {
	// IsTenant returns whether the tenantManager is the tenant.
	IsTenant() bool
}

// tenantManager represents the structure which is used for electing tenant.
type tenantManager struct {
	id        string // id is the ID of the manager.
	key       string
	ctx       context.Context
	prompt    string
	logPrefix string
	logCtx    context.Context
	etcdCli   *clientv3.Client
	cancel    context.CancelFunc
	elec      unsafe.Pointer
	wg        sync.WaitGroup
}

// NewTenantManager creates a new Manager.
func NewTenantManager(ctx context.Context, etcdCli *clientv3.Client, prompt, id, key string) Manager {
	logPrefix := fmt.Sprintf("[%s] %s tenantManager %s", prompt, key, id)
	ctx, cancelFunc := context.WithCancel(ctx)
	return &tenantManager{
		etcdCli:   etcdCli,
		id:        id,
		key:       key,
		ctx:       ctx,
		prompt:    prompt,
		cancel:    cancelFunc,
		logPrefix: logPrefix,
		logCtx:    logutil.WithKeyValue(context.Background(), "tenant info", logPrefix),
	}
}

// ID implements Manager.ID interface.
func (m *tenantManager) ID() string {
	return m.id
}

// IsTenant implements Manager.IsTenant interface.
func (m *tenantManager) IsTenant() bool {
	return atomic.LoadPointer(&m.elec) != unsafe.Pointer(nil)
}

// Cancel implements Manager.Cancel interface.
func (m *tenantManager) Cancel() {
	m.cancel()
	m.wg.Wait()
}

// ManagerStochastikTTL is the etcd stochastik's TTL in seconds. It's exported for testing.
var ManagerStochastikTTL = 60

// setManagerStochastikTTL sets the ManagerStochastikTTL value, it's used for testing.
func setManagerStochastikTTL() error {
	ttlStr := os.Getenv("milevadb_manager_ttl")
	if len(ttlStr) == 0 {
		return nil
	}
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		return errors.Trace(err)
	}
	ManagerStochastikTTL = ttl
	return nil
}

// NewStochastik creates a new etcd stochastik.
func NewStochastik(ctx context.Context, logPrefix string, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Stochastik, error) {
	var err error

	var etcdStochastik *concurrency.Stochastik
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		if err = contextDone(ctx, err); err != nil {
			return etcdStochastik, errors.Trace(err)
		}

		failpoint.Inject("closeClient", func(val failpoint.Value) {
			if val.(bool) {
				if err := etcdCli.Close(); err != nil {
					failpoint.Return(etcdStochastik, errors.Trace(err))
				}
			}
		})

		failpoint.Inject("closeGrpc", func(val failpoint.Value) {
			if val.(bool) {
				if err := etcdCli.ActiveConnection().Close(); err != nil {
					failpoint.Return(etcdStochastik, errors.Trace(err))
				}
			}
		})

		startTime := time.Now()
		etcdStochastik, err = concurrency.NewStochastik(etcdCli,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		metrics.NewStochastikHistogram.WithLabelValues(logPrefix, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err == nil {
			break
		}
		if failedCnt%logIntervalCnt == 0 {
			logutil.BgLogger().Warn("failed to new stochastik to etcd", zap.String("tenantInfo", logPrefix), zap.Error(err))
		}

		time.Sleep(newStochastikRetryInterval)
		failedCnt++
	}
	return etcdStochastik, errors.Trace(err)
}

// CampaignTenant implements Manager.CampaignTenant interface.
func (m *tenantManager) CampaignTenant() error {
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	logutil.BgLogger().Info("start campaign tenant", zap.String("tenantInfo", logPrefix))
	stochastik, err := NewStochastik(m.ctx, logPrefix, m.etcdCli, NewStochastikDefaultRetryCnt, ManagerStochastikTTL)
	if err != nil {
		return errors.Trace(err)
	}
	m.wg.Add(1)
	go m.campaignLoop(stochastik)
	return nil
}

// ResignTenant lets the tenant start a new election.
func (m *tenantManager) ResignTenant(ctx context.Context) error {
	elec := (*concurrency.Election)(atomic.LoadPointer(&m.elec))
	if elec == nil {
		return errors.Errorf("This node is not a dbs tenant, can't be resigned.")
	}

	childCtx, cancel := context.WithTimeout(ctx, keyOFIDelefaultTimeout)
	err := elec.Resign(childCtx)
	cancel()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(m.logCtx).Warn("resign dbs tenant success")
	return nil
}

func (m *tenantManager) toBeTenant(elec *concurrency.Election) {
	atomic.StorePointer(&m.elec, unsafe.Pointer(elec))
}

// RetireTenant make the manager to be a not tenant.
func (m *tenantManager) RetireTenant() {
	atomic.StorePointer(&m.elec, nil)
}

func (m *tenantManager) campaignLoop(etcdStochastik *concurrency.Stochastik) {
	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(m.ctx)
	defer func() {
		cancel()
		if r := recover(); r != nil {
			buf := soliton.GetStack()
			logutil.BgLogger().Error("recover panic", zap.String("prompt", m.prompt), zap.Any("error", r), zap.String("buffer", string(buf)))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDBSTenant).Inc()
		}
		m.wg.Done()
	}()

	logPrefix := m.logPrefix
	logCtx := m.logCtx
	var err error
	for {
		if err != nil {
			metrics.CampaignTenantCounter.WithLabelValues(m.prompt, err.Error()).Inc()
		}

		select {
		case <-etcdStochastik.Done():
			logutil.Logger(logCtx).Info("etcd stochastik is done, creates a new one")
			leaseID := etcdStochastik.Lease()
			etcdStochastik, err = NewStochastik(ctx, logPrefix, m.etcdCli, NewStochastikRetryUnlimited, ManagerStochastikTTL)
			if err != nil {
				logutil.Logger(logCtx).Info("break campaign loop, NewStochastik failed", zap.Error(err))
				m.revokeStochastik(logPrefix, leaseID)
				return
			}
		case <-ctx.Done():
			logutil.Logger(logCtx).Info("break campaign loop, context is done")
			m.revokeStochastik(logPrefix, etcdStochastik.Lease())
			return
		default:
		}
		// If the etcd server turns clocks forward，the following case may occur.
		// The etcd server deletes this stochastik's lease ID, but etcd stochastik doesn't find it.
		// In this time if we do the campaign operation, the etcd server will return ErrLeaseNotFound.
		if terror.ErrorEqual(err, rpctypes.ErrLeaseNotFound) {
			if etcdStochastik != nil {
				err = etcdStochastik.Close()
				logutil.Logger(logCtx).Info("etcd stochastik encounters the error of lease not found, closes it", zap.Error(err))
			}
			continue
		}

		elec := concurrency.NewElection(etcdStochastik, m.key)
		err = elec.Campaign(ctx, m.id)
		if err != nil {
			logutil.Logger(logCtx).Info("failed to campaign", zap.Error(err))
			continue
		}

		tenantKey, err := GetTenantInfo(ctx, logCtx, elec, m.id)
		if err != nil {
			continue
		}

		m.toBeTenant(elec)
		m.watchTenant(ctx, etcdStochastik, tenantKey)
		m.RetireTenant()

		metrics.CampaignTenantCounter.WithLabelValues(m.prompt, metrics.NoLongerTenant).Inc()
		logutil.Logger(logCtx).Warn("is not the tenant")
	}
}

func (m *tenantManager) revokeStochastik(logPrefix string, leaseID clientv3.LeaseID) {
	// Revoke the stochastik lease.
	// If revoke takes longer than the ttl, lease is expired anyway.
	cancelCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(ManagerStochastikTTL)*time.Second)
	_, err := m.etcdCli.Revoke(cancelCtx, leaseID)
	cancel()
	logutil.Logger(m.logCtx).Info("revoke stochastik", zap.Error(err))
}

// GetTenantID implements Manager.GetTenantID interface.
func (m *tenantManager) GetTenantID(ctx context.Context) (string, error) {
	resp, err := m.etcdCli.Get(ctx, m.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Ekvs) == 0 {
		return "", concurrency.ErrElectionNoLeader
	}
	return string(resp.Ekvs[0].Value), nil
}

// GetTenantInfo gets the tenant information.
func GetTenantInfo(ctx, logCtx context.Context, elec *concurrency.Election, id string) (string, error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		// If no leader elected currently, it returns ErrElectionNoLeader.
		logutil.Logger(logCtx).Info("failed to get leader", zap.Error(err))
		return "", errors.Trace(err)
	}
	tenantID := string(resp.Ekvs[0].Value)
	logutil.Logger(logCtx).Info("get tenant", zap.String("tenantID", tenantID))
	if tenantID != id {
		logutil.Logger(logCtx).Warn("is not the tenant")
		return "", errors.New("tenantInfoNotMatch")
	}

	return string(resp.Ekvs[0].Key), nil
}

func (m *tenantManager) watchTenant(ctx context.Context, etcdStochastik *concurrency.Stochastik, key string) {
	logPrefix := fmt.Sprintf("[%s] tenantManager %s watch tenant key %v", m.prompt, m.id, key)
	logCtx := logutil.WithKeyValue(context.Background(), "tenant info", logPrefix)
	logutil.BgLogger().Debug(logPrefix)
	watchCh := m.etcdCli.Watch(ctx, key)
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				metrics.WatchTenantCounter.WithLabelValues(m.prompt, metrics.WatcherClosed).Inc()
				logutil.Logger(logCtx).Info("watcher is closed, no tenant")
				return
			}
			if resp.Canceled {
				metrics.WatchTenantCounter.WithLabelValues(m.prompt, metrics.Cancelled).Inc()
				logutil.Logger(logCtx).Info("watch canceled, no tenant")
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					metrics.WatchTenantCounter.WithLabelValues(m.prompt, metrics.Deleted).Inc()
					logutil.Logger(logCtx).Info("watch failed, tenant is deleted")
					return
				}
			}
		case <-etcdStochastik.Done():
			metrics.WatchTenantCounter.WithLabelValues(m.prompt, metrics.StochastikDone).Inc()
			return
		case <-ctx.Done():
			metrics.WatchTenantCounter.WithLabelValues(m.prompt, metrics.CtxDone).Inc()
			return
		}
	}
}

func init() {
	err := setManagerStochastikTTL()
	if err != nil {
		logutil.BgLogger().Warn("set manager stochastik TTL failed", zap.Error(err))
	}
}

func contextDone(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	// Sometime the ctx isn't closed, but the etcd client is closed,
	// we need to treat it as if context is done.
	// TODO: Make sure ctx is closed with etcd client.
	if terror.ErrorEqual(err, context.Canceled) ||
		terror.ErrorEqual(err, context.DeadlineExceeded) ||
		terror.ErrorEqual(err, grpc.ErrClientConnClosing) {
		return errors.Trace(err)
	}

	return nil
}
