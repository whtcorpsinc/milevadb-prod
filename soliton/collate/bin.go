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

package defCauslate

import (
	"strings"

	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
)

type binDefCauslator struct {
}

// Compare implement DefCauslator interface.
func (bc *binDefCauslator) Compare(a, b string) int {
	return strings.Compare(a, b)
}

// Key implement DefCauslator interface.
func (bc *binDefCauslator) Key(str string) []byte {
	return []byte(str)
}

// Pattern implements DefCauslator interface.
func (bc *binDefCauslator) Pattern() WildcardPattern {
	return &binPattern{}
}

type binPaddingDefCauslator struct {
}

func (bpc *binPaddingDefCauslator) Compare(a, b string) int {
	return strings.Compare(truncateTailingSpace(a), truncateTailingSpace(b))
}

func (bpc *binPaddingDefCauslator) Key(str string) []byte {
	return []byte(truncateTailingSpace(str))
}

// Pattern implements DefCauslator interface.
// Notice that trailing spaces are significant.
func (bpc *binPaddingDefCauslator) Pattern() WildcardPattern {
	return &binPattern{}
}

type binPattern struct {
	patChars []byte
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *binPattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePattern(patternStr, escape)
}

// Compile implements WildcardPattern interface.
func (p *binPattern) DoMatch(str string) bool {
	return stringutil.DoMatch(str, p.patChars, p.patTypes)
}
