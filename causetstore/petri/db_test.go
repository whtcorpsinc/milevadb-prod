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

package petri_test

import (
	"context"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

type dbTestSuite struct{}

var _ = Suite(&dbTestSuite{})

func (ts *dbTestSuite) TestIntegration(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	var err error
	lease := 50 * time.Millisecond
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()
	stochastik.SetSchemaLease(lease)
	petri, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer petri.Close()

	// for NotifyUFIDelatePrivilege
	createRoleALLEGROSQL := `CREATE ROLE 'test'@'localhost';`
	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	_, err = se.InterDircute(context.Background(), createRoleALLEGROSQL)
	c.Assert(err, IsNil)

	// for BindHandle
	se.InterDircute(context.Background(), "use test")
	se.InterDircute(context.Background(), "drop causet if exists t")
	se.InterDircute(context.Background(), "create causet t(i int, s varchar(20), index index_t(i, s))")
	_, err = se.InterDircute(context.Background(), "create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))
}
