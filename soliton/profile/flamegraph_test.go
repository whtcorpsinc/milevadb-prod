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

package profile

import (
	"os"
	"testing"

	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/types"
)

type profileInternalSuite struct{}

var _ = Suite(&profileInternalSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *profileInternalSuite) TestProfileToCauset(c *C) {
	file, err := os.Open("testdata/test.pprof")
	c.Assert(err, IsNil)
	defer file.Close()

	data, err := (&DefCauslector{}).ProfileReaderToCausets(file)
	c.Assert(err, IsNil)

	datums := [][]types.Causet{
		types.MakeCausets(`root`, "100%", "100%", 0, 0, `root`),
		types.MakeCausets(`├─runtime.main`, "87.50%", "87.50%", 1, 1, `c:/go/src/runtime/proc.go:203`),
		types.MakeCausets(`│ └─main.main`, "87.50%", "100%", 1, 2, `Z:/main.go:46`),
		types.MakeCausets(`│   ├─main.defCauslatz`, "68.75%", "78.57%", 1, 3, `Z:/main.go:22`),
		types.MakeCausets(`│   │ └─crypto/cipher.(*ctr).XORKeyStream`, "68.75%", "100%", 1, 4, `c:/go/src/crypto/cipher/ctr.go:84`),
		types.MakeCausets(`│   │   ├─crypto/cipher.(*ctr).refill`, "62.50%", "90.91%", 1, 5, `c:/go/src/crypto/cipher/ctr.go:60`),
		types.MakeCausets(`│   │   │ ├─crypto/aes.(*aesCipherAsm).Encrypt`, "56.25%", "90.00%", 1, 6, `c:/go/src/crypto/aes/cipher_asm.go:68`),
		types.MakeCausets(`│   │   │ │ ├─crypto/aes.encryptBlockAsm`, "12.50%", "22.22%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:49`),
		types.MakeCausets(`│   │   │ │ ├─crypto/aes.encryptBlockAsm`, "6.25%", "11.11%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:45`),
		types.MakeCausets(`│   │   │ │ ├─crypto/aes.encryptBlockAsm`, "6.25%", "11.11%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:39`),
		types.MakeCausets(`│   │   │ │ ├─crypto/aes.encryptBlockAsm`, "6.25%", "11.11%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:37`),
		types.MakeCausets(`│   │   │ │ ├─crypto/aes.encryptBlockAsm`, "6.25%", "11.11%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:43`),
		types.MakeCausets(`│   │   │ │ ├─crypto/aes.encryptBlockAsm`, "6.25%", "11.11%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:41`),
		types.MakeCausets(`│   │   │ │ ├─crypto/aes.encryptBlockAsm`, "6.25%", "11.11%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:51`),
		types.MakeCausets(`│   │   │ │ └─crypto/aes.encryptBlockAsm`, "6.25%", "11.11%", 1, 7, `c:/go/src/crypto/aes/asm_amd64.s:11`),
		types.MakeCausets(`│   │   │ └─crypto/aes.(*aesCipherAsm).Encrypt`, "6.25%", "10.00%", 1, 6, `c:/go/src/crypto/aes/cipher_asm.go:58`),
		types.MakeCausets(`│   │   └─crypto/cipher.(*ctr).refill`, "6.25%", "9.09%", 1, 5, `c:/go/src/crypto/cipher/ctr.go:60`),
		types.MakeCausets(`│   ├─main.defCauslatz`, "12.50%", "14.29%", 1, 3, `Z:/main.go:30`),
		types.MakeCausets(`│   │ └─main.defCauslatz`, "12.50%", "100%", 1, 4, `Z:/main.go:22`),
		types.MakeCausets(`│   │   └─crypto/cipher.(*ctr).XORKeyStream`, "12.50%", "100%", 1, 5, `c:/go/src/crypto/cipher/ctr.go:84`),
		types.MakeCausets(`│   │     └─crypto/cipher.(*ctr).refill`, "12.50%", "100%", 1, 6, `c:/go/src/crypto/cipher/ctr.go:60`),
		types.MakeCausets(`│   │       ├─crypto/aes.(*aesCipherAsm).Encrypt`, "6.25%", "50.00%", 1, 7, `c:/go/src/crypto/aes/cipher_asm.go:68`),
		types.MakeCausets(`│   │       │ └─crypto/aes.encryptBlockAsm`, "6.25%", "100%", 1, 8, `c:/go/src/crypto/aes/asm_amd64.s:45`),
		types.MakeCausets(`│   │       └─crypto/aes.(*aesCipherAsm).Encrypt`, "6.25%", "50.00%", 1, 7, `c:/go/src/crypto/aes/cipher_asm.go:65`),
		types.MakeCausets(`│   │         └─crypto/internal/subtle.InexactOverlap`, "6.25%", "100%", 1, 8, `c:/go/src/crypto/internal/subtle/aliasing.go:33`),
		types.MakeCausets(`│   │           └─crypto/internal/subtle.AnyOverlap`, "6.25%", "100%", 1, 9, `c:/go/src/crypto/internal/subtle/aliasing.go:20`),
		types.MakeCausets(`│   └─main.defCauslatz`, "6.25%", "7.14%", 1, 3, `Z:/main.go:20`),
		types.MakeCausets(`│     └─runtime.memmove`, "6.25%", "100%", 1, 4, `c:/go/src/runtime/memmove_amd64.s:362`),
		types.MakeCausets(`├─runtime.mstart`, "6.25%", "6.25%", 2, 1, `c:/go/src/runtime/proc.go:1146`),
		types.MakeCausets(`│ └─runtime.systemstack`, "6.25%", "100%", 2, 2, `c:/go/src/runtime/asm_amd64.s:370`),
		types.MakeCausets(`│   └─runtime.bgscavenge.func2`, "6.25%", "100%", 2, 3, `c:/go/src/runtime/mgcscavenge.go:315`),
		types.MakeCausets(`│     └─runtime.(*mheap).scavengeLocked`, "6.25%", "100%", 2, 4, `c:/go/src/runtime/mheap.go:1446`),
		types.MakeCausets(`│       └─runtime.(*mspan).scavenge`, "6.25%", "100%", 2, 5, `c:/go/src/runtime/mheap.go:589`),
		types.MakeCausets(`│         └─runtime.sysUnused`, "6.25%", "100%", 2, 6, `c:/go/src/runtime/mem_windows.go:33`),
		types.MakeCausets(`│           └─runtime.stdcall3`, "6.25%", "100%", 2, 7, `c:/go/src/runtime/os_windows.go:837`),
		types.MakeCausets(`└─runtime.morestack`, "6.25%", "6.25%", 3, 1, `c:/go/src/runtime/asm_amd64.s:449`),
		types.MakeCausets(`  └─runtime.newstack`, "6.25%", "100%", 3, 2, `c:/go/src/runtime/stack.go:1038`),
		types.MakeCausets(`    └─runtime.gopreempt_m`, "6.25%", "100%", 3, 3, `c:/go/src/runtime/proc.go:2653`),
		types.MakeCausets(`      └─runtime.goschedImpl`, "6.25%", "100%", 3, 4, `c:/go/src/runtime/proc.go:2625`),
		types.MakeCausets(`        └─runtime.schedule`, "6.25%", "100%", 3, 5, `c:/go/src/runtime/proc.go:2524`),
		types.MakeCausets(`          └─runtime.findrunnable`, "6.25%", "100%", 3, 6, `c:/go/src/runtime/proc.go:2170`),
	}

	for i, event := range data {
		comment := Commentf("event %2d", i)
		rowStr, err := types.CausetsToString(event, true)
		c.Assert(err, IsNil, comment)
		expectStr, err := types.CausetsToString(datums[i], true)
		c.Assert(err, IsNil, comment)

		comment = Commentf("event %2d, actual (%s), expected (%s)", i, rowStr, expectStr)
		equal, err := types.EqualCausets(nil, event, datums[i])
		c.Assert(err, IsNil, comment)
		c.Assert(equal, IsTrue, comment)
	}
}
