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

package codec

import (
	"hash"
	"hash/crc32"
	"hash/fnv"
	"time"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
)

var _ = SerialSuites(&testDefCauslationSuite{})

type testDefCauslationSuite struct {
}

func prepareDefCauslationData() (int, *chunk.Chunk, *chunk.Chunk) {
	tp := types.NewFieldType(allegrosql.TypeString)
	tps := []*types.FieldType{tp}
	chk1 := chunk.New(tps, 3, 3)
	chk2 := chunk.New(tps, 3, 3)
	chk1.Reset()
	chk2.Reset()
	chk1.DeferredCauset(0).AppendString("aaa")
	chk2.DeferredCauset(0).AppendString("AAA")
	chk1.DeferredCauset(0).AppendString("😜")
	chk2.DeferredCauset(0).AppendString("😃")
	chk1.DeferredCauset(0).AppendString("À")
	chk2.DeferredCauset(0).AppendString("A")
	return 3, chk1, chk2
}

func (s *testDefCauslationSuite) TestHashGroupKeyDefCauslation(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	tp := types.NewFieldType(allegrosql.TypeString)
	n, chk1, chk2 := prepareDefCauslationData()

	tp.DefCauslate = "utf8_general_ci"
	buf1 := make([][]byte, n)
	buf2 := make([][]byte, n)
	buf1, err := HashGroupKey(sc, n, chk1.DeferredCauset(0), buf1, tp)
	c.Assert(err, IsNil)
	buf2, err = HashGroupKey(sc, n, chk2.DeferredCauset(0), buf2, tp)
	c.Assert(err, IsNil)
	for i := 0; i < n; i++ {
		c.Assert(len(buf1[i]), Equals, len(buf2[i]))
		for j := range buf1 {
			c.Assert(buf1[i][j], Equals, buf2[i][j])
		}
	}

	tp.DefCauslate = "utf8_unicode_ci"
	buf1 = make([][]byte, n)
	buf2 = make([][]byte, n)
	buf1, err = HashGroupKey(sc, n, chk1.DeferredCauset(0), buf1, tp)
	c.Assert(err, IsNil)
	buf2, err = HashGroupKey(sc, n, chk2.DeferredCauset(0), buf2, tp)
	c.Assert(err, IsNil)
	for i := 0; i < n; i++ {
		c.Assert(len(buf1[i]), Equals, len(buf2[i]))
		for j := range buf1 {
			c.Assert(buf1[i][j], Equals, buf2[i][j])
		}
	}
}

func (s *testDefCauslationSuite) TestHashChunkRowDefCauslation(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	tp := types.NewFieldType(allegrosql.TypeString)
	tps := []*types.FieldType{tp}
	n, chk1, chk2 := prepareDefCauslationData()
	defcaus := []int{0}
	buf := make([]byte, 1)

	tp.DefCauslate = "bin"
	for i := 0; i < n; i++ {
		h1 := crc32.NewIEEE()
		h2 := crc32.NewIEEE()
		c.Assert(HashChunkRow(sc, h1, chk1.GetRow(i), tps, defcaus, buf), IsNil)
		c.Assert(HashChunkRow(sc, h2, chk2.GetRow(i), tps, defcaus, buf), IsNil)
		c.Assert(h1.Sum32(), Not(Equals), h2.Sum32())
		h1.Reset()
		h2.Reset()
	}

	tp.DefCauslate = "utf8_general_ci"
	for i := 0; i < n; i++ {
		h1 := crc32.NewIEEE()
		h2 := crc32.NewIEEE()
		c.Assert(HashChunkRow(sc, h1, chk1.GetRow(i), tps, defcaus, buf), IsNil)
		c.Assert(HashChunkRow(sc, h2, chk2.GetRow(i), tps, defcaus, buf), IsNil)
		c.Assert(h1.Sum32(), Equals, h2.Sum32())
		h1.Reset()
		h2.Reset()
	}

	tp.DefCauslate = "utf8_unicode_ci"
	for i := 0; i < n; i++ {
		h1 := crc32.NewIEEE()
		h2 := crc32.NewIEEE()
		c.Assert(HashChunkRow(sc, h1, chk1.GetRow(i), tps, defcaus, buf), IsNil)
		c.Assert(HashChunkRow(sc, h2, chk2.GetRow(i), tps, defcaus, buf), IsNil)
		c.Assert(h1.Sum32(), Equals, h2.Sum32())
		h1.Reset()
		h2.Reset()
	}
}

func (s *testDefCauslationSuite) TestHashChunkDeferredCausetsDefCauslation(c *C) {
	defCauslate.SetNewDefCauslationEnabledForTest(true)
	defer defCauslate.SetNewDefCauslationEnabledForTest(false)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	tp := types.NewFieldType(allegrosql.TypeString)
	n, chk1, chk2 := prepareDefCauslationData()
	buf := make([]byte, 1)
	hasNull := []bool{false, false, false}
	h1s := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}
	h2s := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}

	tp.DefCauslate = "bin"
	c.Assert(HashChunkDeferredCausets(sc, h1s, chk1, tp, 0, buf, hasNull), IsNil)
	c.Assert(HashChunkDeferredCausets(sc, h2s, chk2, tp, 0, buf, hasNull), IsNil)
	for i := 0; i < n; i++ {
		c.Assert(h1s[i].Sum64(), Not(Equals), h2s[i].Sum64())
		h1s[i].Reset()
		h2s[i].Reset()
	}

	tp.DefCauslate = "utf8_general_ci"
	c.Assert(HashChunkDeferredCausets(sc, h1s, chk1, tp, 0, buf, hasNull), IsNil)
	c.Assert(HashChunkDeferredCausets(sc, h2s, chk2, tp, 0, buf, hasNull), IsNil)
	for i := 0; i < n; i++ {
		c.Assert(h1s[i].Sum64(), Equals, h2s[i].Sum64())
	}

	tp.DefCauslate = "utf8_unicode_ci"
	c.Assert(HashChunkDeferredCausets(sc, h1s, chk1, tp, 0, buf, hasNull), IsNil)
	c.Assert(HashChunkDeferredCausets(sc, h2s, chk2, tp, 0, buf, hasNull), IsNil)
	for i := 0; i < n; i++ {
		c.Assert(h1s[i].Sum64(), Equals, h2s[i].Sum64())
	}
}
