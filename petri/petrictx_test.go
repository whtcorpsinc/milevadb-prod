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

package petri

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/soliton/testleak"
)

var _ = Suite(&testPetriCtxSuite{})

type testPetriCtxSuite struct {
}

func (s *testPetriCtxSuite) TestPetri(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()

	c.Assert(petriKey.String(), Not(Equals), "")

	BindPetri(ctx, nil)
	v := GetPetri(ctx)
	c.Assert(v, IsNil)

	ctx.ClearValue(petriKey)
	v = GetPetri(ctx)
	c.Assert(v, IsNil)
}
