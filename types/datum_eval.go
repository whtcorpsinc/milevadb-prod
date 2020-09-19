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

package types

import (
	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/opcode"
)

// ComputePlus computes the result of a+b.
func ComputePlus(a, b Causet) (d Causet, err error) {
	switch a.HoTT() {
	case HoTTInt64:
		switch b.HoTT() {
		case HoTTInt64:
			r, err1 := AddInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case HoTTUint64:
			r, err1 := AddInteger(b.GetUint64(), a.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case HoTTUint64:
		switch b.HoTT() {
		case HoTTInt64:
			r, err1 := AddInteger(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case HoTTUint64:
			r, err1 := AddUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case HoTTFloat64:
		switch b.HoTT() {
		case HoTTFloat64:
			r := a.GetFloat64() + b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case HoTTMysqlDecimal:
		switch b.HoTT() {
		case HoTTMysqlDecimal:
			r := new(MyDecimal)
			err = DecimalAdd(a.GetMysqlDecimal(), b.GetMysqlDecimal(), r)
			d.SetMysqlDecimal(r)
			d.SetFrac(mathutil.Max(a.Frac(), b.Frac()))
			return d, err
		}
	}
	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Plus)
	return d, err
}
