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

package memex

import (
	"regexp"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

func (b *builtinLikeSig) vectorized() bool {
	return true
}

func (b *builtinLikeSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	bufVal, err := b.bufSlabPredictor.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(bufVal)
	if err = b.args[0].VecEvalString(b.ctx, input, bufVal); err != nil {
		return err
	}
	bufPattern, err := b.bufSlabPredictor.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(bufPattern)
	if err = b.args[1].VecEvalString(b.ctx, input, bufPattern); err != nil {
		return err
	}

	bufEscape, err := b.bufSlabPredictor.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(bufEscape)
	if err = b.args[2].VecEvalInt(b.ctx, input, bufEscape); err != nil {
		return err
	}
	escapes := bufEscape.Int64s()

	// Must not use b.pattern to avoid data race
	pattern := b.defCauslator().Pattern()

	result.ResizeInt64(n, false)
	result.MergeNulls(bufVal, bufPattern, bufEscape)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		pattern.Compile(bufPattern.GetString(i), byte(escapes[i]))
		match := pattern.DoMatch(bufVal.GetString(i))
		i64s[i] = boolToInt64(match)
	}

	return nil
}

func (b *builtinRegexpSig) vectorized() bool {
	return true
}

func (b *builtinRegexpUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinRegexpSharedSig) isMemorizedRegexpInitialized() bool {
	return !(b.memorizedRegexp == nil && b.memorizedErr == nil)
}

func (b *builtinRegexpSharedSig) initMemoizedRegexp(patterns *chunk.DeferredCauset, n int) {
	// Precondition: patterns is generated from a constant memex
	if n == 0 {
		// If the input rownum is zero, the Regexp error shouldn't be generated.
		return
	}
	for i := 0; i < n; i++ {
		if patterns.IsNull(i) {
			continue
		}
		re, err := b.compile(patterns.GetString(i))
		b.memorizedRegexp = re
		b.memorizedErr = err
		break
	}
	if !b.isMemorizedRegexpInitialized() {
		b.memorizedErr = errors.New("No valid regexp pattern found")
	}
	if b.memorizedErr != nil {
		b.memorizedRegexp = nil
	}
}

func (b *builtinRegexpSharedSig) vecEvalInt(input *chunk.Chunk, result *chunk.DeferredCauset) error {
	n := input.NumEvents()
	bufExpr, err := b.bufSlabPredictor.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(bufExpr)
	if err := b.args[0].VecEvalString(b.ctx, input, bufExpr); err != nil {
		return err
	}

	bufPat, err := b.bufSlabPredictor.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufSlabPredictor.put(bufPat)
	if err := b.args[1].VecEvalString(b.ctx, input, bufPat); err != nil {
		return err
	}

	if b.args[1].ConstItem(b.ctx.GetStochastikVars().StmtCtx) && !b.isMemorizedRegexpInitialized() {
		b.initMemoizedRegexp(bufPat, n)
	}
	getRegexp := func(pat string) (*regexp.Regexp, error) {
		if b.isMemorizedRegexpInitialized() {
			return b.memorizedRegexp, b.memorizedErr
		}
		return b.compile(pat)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(bufExpr, bufPat)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		re, err := getRegexp(bufPat.GetString(i))
		if err != nil {
			return err
		}
		i64s[i] = boolToInt64(re.MatchString(bufExpr.GetString(i)))
	}
	return nil
}
