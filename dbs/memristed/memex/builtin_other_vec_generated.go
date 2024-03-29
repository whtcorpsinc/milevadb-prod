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

// Code generated by go generate in memex/generator; DO NOT EDIT.

package memex

import (
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/types/json"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
)

func (b *builtinInIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf0, err := b.bufSlabPredictor.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufSlabPredictor.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf1)

	args0 := buf0.Int64s()
	result.ResizeInt64(n, true)
	r64s := result.Int64s()
	for i := 0; i < n; i++ {
		r64s[i] = 0
	}
	hasNull := make([]bool, n)
	if b.hasNull {
		for i := 0; i < n; i++ {
			hasNull[i] = true
		}
	}
	isUnsigned0 := allegrosql.HasUnsignedFlag(b.args[0].GetType().Flag)
	var compareResult int
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		for i := 0; i < n; i++ {
			if buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			if isUnsigned, ok := b.hashSet[arg0]; ok {
				if (isUnsigned0 && isUnsigned) || (!isUnsigned0 && !isUnsigned) {
					r64s[i] = 1
					result.SetNull(i, false)
				}
				if arg0 >= 0 {
					r64s[i] = 1
					result.SetNull(i, false)
				}
			}
		}
	}

	for j := 1; j < len(args); j++ {
		if err := args[j].VecEvalInt(b.ctx, input, buf1); err != nil {
			return err
		}
		isUnsigned := allegrosql.HasUnsignedFlag(args[j].GetType().Flag)
		args1 := buf1.Int64s()
		buf1.MergeNulls(buf0)
		for i := 0; i < n; i++ {
			if r64s[i] != 0 {
				continue
			}
			if buf1.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			arg1 := args1[i]
			compareResult = 1
			switch {
			case (isUnsigned0 && isUnsigned), (!isUnsigned0 && !isUnsigned):
				if arg1 == arg0 {
					compareResult = 0
				}
			case !isUnsigned0 && isUnsigned:
				if arg0 >= 0 && arg1 == arg0 {
					compareResult = 0
				}
			case isUnsigned0 && !isUnsigned:
				if arg1 >= 0 && arg1 == arg0 {
					compareResult = 0
				}
			}
			if compareResult == 0 {
				result.SetNull(i, false)
				r64s[i] = 1
			}
		} // for i
	} // for j
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			result.SetNull(i, hasNull[i])
		}
	}
	return nil
}

func (b *builtinInIntSig) vectorized() bool {
	return true
}

func (b *builtinInStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf0, err := b.bufSlabPredictor.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufSlabPredictor.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf1)

	result.ResizeInt64(n, true)
	r64s := result.Int64s()
	for i := 0; i < n; i++ {
		r64s[i] = 0
	}
	hasNull := make([]bool, n)
	if b.hasNull {
		for i := 0; i < n; i++ {
			hasNull[i] = true
		}
	}
	var compareResult int
	args := b.args
	if len(b.hashSet) != 0 {
		defCauslator := defCauslate.GetDefCauslator(b.defCauslation)
		args = b.nonConstArgs
		for i := 0; i < n; i++ {
			if buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := buf0.GetString(i)
			if _, ok := b.hashSet[string(defCauslator.Key(arg0))]; ok {
				r64s[i] = 1
				result.SetNull(i, false)
			}
		}
	}

	for j := 1; j < len(args); j++ {
		if err := args[j].VecEvalString(b.ctx, input, buf1); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if r64s[i] != 0 {
				continue
			}
			if buf1.IsNull(i) || buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := buf0.GetString(i)
			arg1 := buf1.GetString(i)
			compareResult = types.CompareString(arg0, arg1, b.defCauslation)
			if compareResult == 0 {
				result.SetNull(i, false)
				r64s[i] = 1
			}
		} // for i
	} // for j
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			result.SetNull(i, hasNull[i])
		}
	}
	return nil
}

func (b *builtinInStringSig) vectorized() bool {
	return true
}

func (b *builtinInDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf0, err := b.bufSlabPredictor.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf0)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufSlabPredictor.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf1)

	args0 := buf0.Decimals()
	result.ResizeInt64(n, true)
	r64s := result.Int64s()
	for i := 0; i < n; i++ {
		r64s[i] = 0
	}
	hasNull := make([]bool, n)
	if b.hasNull {
		for i := 0; i < n; i++ {
			hasNull[i] = true
		}
	}
	var compareResult int
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		for i := 0; i < n; i++ {
			if buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			key, err := arg0.ToHashKey()
			if err != nil {
				return err
			}
			if _, ok := b.hashSet[string(key)]; ok {
				r64s[i] = 1
				result.SetNull(i, false)
			}
		}
	}

	for j := 1; j < len(args); j++ {
		if err := args[j].VecEvalDecimal(b.ctx, input, buf1); err != nil {
			return err
		}
		args1 := buf1.Decimals()
		buf1.MergeNulls(buf0)
		for i := 0; i < n; i++ {
			if r64s[i] != 0 {
				continue
			}
			if buf1.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			arg1 := args1[i]
			compareResult = 1
			if arg0.Compare(&arg1) == 0 {
				compareResult = 0
			}
			if compareResult == 0 {
				result.SetNull(i, false)
				r64s[i] = 1
			}
		} // for i
	} // for j
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			result.SetNull(i, hasNull[i])
		}
	}
	return nil
}

func (b *builtinInDecimalSig) vectorized() bool {
	return true
}

func (b *builtinInRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf0, err := b.bufSlabPredictor.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf0)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufSlabPredictor.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf1)

	args0 := buf0.Float64s()
	result.ResizeInt64(n, true)
	r64s := result.Int64s()
	for i := 0; i < n; i++ {
		r64s[i] = 0
	}
	hasNull := make([]bool, n)
	if b.hasNull {
		for i := 0; i < n; i++ {
			hasNull[i] = true
		}
	}
	var compareResult int
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		for i := 0; i < n; i++ {
			if buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			if _, ok := b.hashSet[arg0]; ok {
				r64s[i] = 1
				result.SetNull(i, false)
			}
		}
	}

	for j := 1; j < len(args); j++ {
		if err := args[j].VecEvalReal(b.ctx, input, buf1); err != nil {
			return err
		}
		args1 := buf1.Float64s()
		buf1.MergeNulls(buf0)
		for i := 0; i < n; i++ {
			if r64s[i] != 0 {
				continue
			}
			if buf1.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			arg1 := args1[i]
			compareResult = types.CompareFloat64(arg0, arg1)
			if compareResult == 0 {
				result.SetNull(i, false)
				r64s[i] = 1
			}
		} // for i
	} // for j
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			result.SetNull(i, hasNull[i])
		}
	}
	return nil
}

func (b *builtinInRealSig) vectorized() bool {
	return true
}

func (b *builtinInTimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf0, err := b.bufSlabPredictor.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf0)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufSlabPredictor.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf1)

	args0 := buf0.Times()
	result.ResizeInt64(n, true)
	r64s := result.Int64s()
	for i := 0; i < n; i++ {
		r64s[i] = 0
	}
	hasNull := make([]bool, n)
	if b.hasNull {
		for i := 0; i < n; i++ {
			hasNull[i] = true
		}
	}
	var compareResult int
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		for i := 0; i < n; i++ {
			if buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			if _, ok := b.hashSet[arg0]; ok {
				r64s[i] = 1
				result.SetNull(i, false)
			}
		}
	}

	for j := 1; j < len(args); j++ {
		if err := args[j].VecEvalTime(b.ctx, input, buf1); err != nil {
			return err
		}
		args1 := buf1.Times()
		buf1.MergeNulls(buf0)
		for i := 0; i < n; i++ {
			if r64s[i] != 0 {
				continue
			}
			if buf1.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			arg1 := args1[i]
			compareResult = arg0.Compare(arg1)
			if compareResult == 0 {
				result.SetNull(i, false)
				r64s[i] = 1
			}
		} // for i
	} // for j
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			result.SetNull(i, hasNull[i])
		}
	}
	return nil
}

func (b *builtinInTimeSig) vectorized() bool {
	return true
}

func (b *builtinInDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf0, err := b.bufSlabPredictor.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf0)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufSlabPredictor.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf1)

	args0 := buf0.GoDurations()
	result.ResizeInt64(n, true)
	r64s := result.Int64s()
	for i := 0; i < n; i++ {
		r64s[i] = 0
	}
	hasNull := make([]bool, n)
	if b.hasNull {
		for i := 0; i < n; i++ {
			hasNull[i] = true
		}
	}
	var compareResult int
	args := b.args
	if len(b.hashSet) != 0 {
		args = b.nonConstArgs
		for i := 0; i < n; i++ {
			if buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			if _, ok := b.hashSet[arg0]; ok {
				r64s[i] = 1
				result.SetNull(i, false)
			}
		}
	}

	for j := 1; j < len(args); j++ {
		if err := args[j].VecEvalDuration(b.ctx, input, buf1); err != nil {
			return err
		}
		args1 := buf1.GoDurations()
		buf1.MergeNulls(buf0)
		for i := 0; i < n; i++ {
			if r64s[i] != 0 {
				continue
			}
			if buf1.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := args0[i]
			arg1 := args1[i]
			compareResult = types.CompareDuration(arg0, arg1)
			if compareResult == 0 {
				result.SetNull(i, false)
				r64s[i] = 1
			}
		} // for i
	} // for j
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			result.SetNull(i, hasNull[i])
		}
	}
	return nil
}

func (b *builtinInDurationSig) vectorized() bool {
	return true
}

func (b *builtinInJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	buf0, err := b.bufSlabPredictor.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf0)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufSlabPredictor.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(buf1)

	result.ResizeInt64(n, true)
	r64s := result.Int64s()
	for i := 0; i < n; i++ {
		r64s[i] = 0
	}
	hasNull := make([]bool, n)
	var compareResult int
	args := b.args

	for j := 1; j < len(args); j++ {
		if err := args[j].VecEvalJSON(b.ctx, input, buf1); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if r64s[i] != 0 {
				continue
			}
			if buf1.IsNull(i) || buf0.IsNull(i) {
				hasNull[i] = true
				continue
			}
			arg0 := buf0.GetJSON(i)
			arg1 := buf1.GetJSON(i)
			compareResult = json.CompareBinary(arg0, arg1)
			if compareResult == 0 {
				result.SetNull(i, false)
				r64s[i] = 1
			}
		} // for i
	} // for j
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			result.SetNull(i, hasNull[i])
		}
	}
	return nil
}

func (b *builtinInJSONSig) vectorized() bool {
	return true
}
