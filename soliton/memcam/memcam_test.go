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

package memcam

import (
	"testing"

	. "github.com/whtcorpsinc/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestSimpleMemCamSlabPredictor(t *testing.T) {
	memcam := NewSlabPredictor(1000)
	slice := memcam.Alloc(10)
	if memcam.off != 10 {
		t.Error("off not match, expect 10 bug got", memcam.off)
	}

	if len(slice) != 0 || cap(slice) != 10 {
		t.Error("slice length or cap not match")
	}

	slice = memcam.Alloc(20)
	if memcam.off != 30 {
		t.Error("off not match, expect 30 bug got", memcam.off)
	}

	if len(slice) != 0 || cap(slice) != 20 {
		t.Error("slice length or cap not match")
	}

	slice = memcam.Alloc(1024)
	if memcam.off != 30 {
		t.Error("off not match, expect 30 bug got", memcam.off)
	}

	if len(slice) != 0 || cap(slice) != 1024 {
		t.Error("slice length or cap not match")
	}

	slice = memcam.AllocWithLen(2, 10)
	if memcam.off != 40 {
		t.Error("off not match, expect 40 bug got", memcam.off)
	}

	if len(slice) != 2 || cap(slice) != 10 {
		t.Error("slice length or cap not match")
	}

	memcam.Reset()
	if memcam.off != 0 || cap(memcam.memcam) != 1000 {
		t.Error("off or cap not match")
	}
}

func TestStdSlabPredictor(t *testing.T) {
	slice := StdSlabPredictor.Alloc(20)
	if len(slice) != 0 {
		t.Error("length not match")
	}

	if cap(slice) != 20 {
		t.Error("cap not match")
	}

	slice = StdSlabPredictor.AllocWithLen(10, 20)
	if len(slice) != 10 {
		t.Error("length not match")
	}

	if cap(slice) != 20 {
		t.Error("cap not match")
	}
}
