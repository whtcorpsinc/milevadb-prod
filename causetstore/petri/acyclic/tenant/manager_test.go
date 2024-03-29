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

package tenant_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/whtcorpsinc/BerolinaSQL/terror"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	. "github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/tenant"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/integration"
	goctx "golang.org/x/net/context"
)

const testLease = 5 * time.Millisecond

func TestT(t *testing.T) {
	TestingT(t)
}

func checkTenant(d DBS, fbVal bool) (isTenant bool) {
	manager := d.TenantManager()
	// The longest to wait for 30 seconds to
	// make sure that campaigning tenants is completed.
	for i := 0; i < 6000; i++ {
		time.Sleep(5 * time.Millisecond)
		isTenant = manager.IsTenant()
		if isTenant == fbVal {
			break
		}
	}
	return
}

func TestSingle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		t.Fatal(err)
	}
	defer causetstore.Close()

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	cli := clus.RandClient()
	ctx := goctx.Background()
	d := NewDBS(
		ctx,
		WithEtcdClient(cli),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	defer d.Stop()

	isTenant := checkTenant(d, true)
	if !isTenant {
		t.Fatalf("expect true, got isTenant:%v", isTenant)
	}

	// test for newStochastik failed
	ctx, cancel := goctx.WithCancel(ctx)
	manager := tenant.NewTenantManager(ctx, cli, "dbs", "dbs_id", DBSTenantKey)
	cancel()
	err = manager.CampaignTenant()
	if !terror.ErrorEqual(err, goctx.Canceled) &&
		!terror.ErrorEqual(err, goctx.DeadlineExceeded) {
		t.Fatalf("campaigned result don't match, err %v", err)
	}
	isTenant = checkTenant(d, true)
	if !isTenant {
		t.Fatalf("expect true, got isTenant:%v", isTenant)
	}
	// The test is used to exit campaign loop.
	d.TenantManager().Cancel()
	isTenant = checkTenant(d, false)
	if isTenant {
		t.Fatalf("expect false, got isTenant:%v", isTenant)
	}
	time.Sleep(200 * time.Millisecond)
	tenantID, _ := manager.GetTenantID(goctx.Background())
	// The error is ok to be not nil since we canceled the manager.
	if tenantID != "" {
		t.Fatalf("tenant %s is not empty", tenantID)
	}
}

func TestCluster(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	tmpTTL := 3
	orignalTTL := tenant.ManagerStochastikTTL
	tenant.ManagerStochastikTTL = tmpTTL
	defer func() {
		tenant.ManagerStochastikTTL = orignalTTL
	}()
	causetstore, err := mockstore.NewMockStore()
	if err != nil {
		t.Fatal(err)
	}
	defer causetstore.Close()
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 4})
	defer clus.Terminate(t)

	cli := clus.Client(0)
	d := NewDBS(
		goctx.Background(),
		WithEtcdClient(cli),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	isTenant := checkTenant(d, true)
	if !isTenant {
		t.Fatalf("expect true, got isTenant:%v", isTenant)
	}
	cli1 := clus.Client(1)
	d1 := NewDBS(
		goctx.Background(),
		WithEtcdClient(cli1),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d1.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	isTenant = checkTenant(d1, false)
	if isTenant {
		t.Fatalf("expect false, got isTenant:%v", isTenant)
	}

	// Delete the leader key, the d1 become the tenant.
	cliRW := clus.Client(2)
	err = deleteLeader(cliRW, DBSTenantKey)
	if err != nil {
		t.Fatal(err)
	}
	isTenant = checkTenant(d, false)
	if isTenant {
		t.Fatalf("expect false, got isTenant:%v", isTenant)
	}
	d.Stop()

	// d3 (not tenant) stop
	cli3 := clus.Client(3)
	d3 := NewDBS(
		goctx.Background(),
		WithEtcdClient(cli3),
		WithStore(causetstore),
		WithLease(testLease),
	)
	err = d3.Start(nil)
	if err != nil {
		t.Fatalf("DBS start failed %v", err)
	}
	defer d3.Stop()
	isTenant = checkTenant(d3, false)
	if isTenant {
		t.Fatalf("expect false, got isTenant:%v", isTenant)
	}
	d3.Stop()

	// Cancel the tenant context, there is no tenant.
	d1.Stop()
	time.Sleep(time.Duration(tmpTTL+1) * time.Second)
	stochastik, err := concurrency.NewStochastik(cliRW)
	if err != nil {
		t.Fatalf("new stochastik failed %v", err)
	}
	elec := concurrency.NewElection(stochastik, DBSTenantKey)
	logPrefix := fmt.Sprintf("[dbs] %s tenantManager %s", DBSTenantKey, "useless id")
	logCtx := logutil.WithKeyValue(context.Background(), "tenant info", logPrefix)
	_, err = tenant.GetTenantInfo(goctx.Background(), logCtx, elec, "useless id")
	if !terror.ErrorEqual(err, concurrency.ErrElectionNoLeader) {
		t.Fatalf("get tenant info result don't match, err %v", err)
	}
}

func deleteLeader(cli *clientv3.Client, prefixKey string) error {
	stochastik, err := concurrency.NewStochastik(cli)
	if err != nil {
		return errors.Trace(err)
	}
	defer stochastik.Close()
	elec := concurrency.NewElection(stochastik, prefixKey)
	resp, err := elec.Leader(goctx.Background())
	if err != nil {
		return errors.Trace(err)
	}
	_, err = cli.Delete(goctx.Background(), string(resp.Ekvs[0].Key))
	return errors.Trace(err)
}
