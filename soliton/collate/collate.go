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
	"sort"
	"sync/atomic"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/charset"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

var (
	newDefCauslatorMap      map[string]DefCauslator
	newDefCauslatorIDMap    map[int]DefCauslator
	newDefCauslationEnabled int32

	// binDefCauslatorInstance is a singleton used for all defCauslations when newDefCauslationEnabled is false.
	binDefCauslatorInstance = &binDefCauslator{}

	// ErrUnsupportedDefCauslation is returned when an unsupported defCauslation is specified.
	ErrUnsupportedDefCauslation = terror.ClassDBS.New(allegrosql.ErrUnknownDefCauslation, "Unsupported defCauslation when new defCauslation is enabled: '%-.64s'")
	// ErrIllegalMixDefCauslation is returned when illegal mix of defCauslations.
	ErrIllegalMixDefCauslation = terror.ClassExpression.New(allegrosql.ErrCantAggregateNdefCauslations, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantAggregateNdefCauslations])
	// ErrIllegalMix2DefCauslation is returned when illegal mix of 2 defCauslations.
	ErrIllegalMix2DefCauslation = terror.ClassExpression.New(allegrosql.ErrCantAggregate2defCauslations, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantAggregate2defCauslations])
	// ErrIllegalMix3DefCauslation is returned when illegal mix of 3 defCauslations.
	ErrIllegalMix3DefCauslation = terror.ClassExpression.New(allegrosql.ErrCantAggregate3defCauslations, allegrosql.MyALLEGROSQLErrName[allegrosql.ErrCantAggregate3defCauslations])
)

// DefaultLen is set for causet if the string causet don't know its length.
const (
	DefaultLen = 0
)

// DefCauslator provides functionality for comparing strings for a given
// defCauslation order.
type DefCauslator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string) int
	// Key returns the defCauslate key for str. If the defCauslation is padding, make sure the PadLen >= len(rune[]str) in opt.
	Key(str string) []byte
	// Pattern get a defCauslation-aware WildcardPattern.
	Pattern() WildcardPattern
}

// WildcardPattern is the interface used for wildcard pattern match.
type WildcardPattern interface {
	// Compile compiles the patternStr with specified escape character.
	Compile(patternStr string, escape byte)
	// DoMatch tries to match the str with compiled pattern, `Compile()` must be called before calling it.
	DoMatch(str string) bool
}

// EnableNewDefCauslations enables the new defCauslation.
func EnableNewDefCauslations() {
	SetNewDefCauslationEnabledForTest(true)
}

// SetNewDefCauslationEnabledForTest sets if the new defCauslation are enabled in test.
// Note: Be careful to use this function, if this functions is used in tests, make sure the tests are serial.
func SetNewDefCauslationEnabledForTest(flag bool) {
	if flag {
		atomic.StoreInt32(&newDefCauslationEnabled, 1)
		return
	}
	atomic.StoreInt32(&newDefCauslationEnabled, 0)
}

// NewDefCauslationEnabled returns if the new defCauslations are enabled.
func NewDefCauslationEnabled() bool {
	return atomic.LoadInt32(&newDefCauslationEnabled) == 1
}

// CompatibleDefCauslate checks whether the two defCauslate are the same.
func CompatibleDefCauslate(defCauslate1, defCauslate2 string) bool {
	if (defCauslate1 == "utf8mb4_general_ci" || defCauslate1 == "utf8_general_ci") && (defCauslate2 == "utf8mb4_general_ci" || defCauslate2 == "utf8_general_ci") {
		return true
	} else if (defCauslate1 == "utf8mb4_bin" || defCauslate1 == "utf8_bin") && (defCauslate2 == "utf8mb4_bin" || defCauslate2 == "utf8_bin") {
		return true
	} else if (defCauslate1 == "utf8mb4_unicode_ci" || defCauslate1 == "utf8_unicode_ci") && (defCauslate2 == "utf8mb4_unicode_ci" || defCauslate2 == "utf8_unicode_ci") {
		return true
	} else {
		return defCauslate1 == defCauslate2
	}
}

// RewriteNewDefCauslationIDIfNeeded rewrites a defCauslation id if the new defCauslations are enabled.
// When new defCauslations are enabled, we turn the defCauslation id to negative so that other the
// components of the cluster(for example, EinsteinDB) is able to aware of it without any change to
// the protodefCaus definition.
// When new defCauslations are not enabled, defCauslation id remains the same.
func RewriteNewDefCauslationIDIfNeeded(id int32) int32 {
	if atomic.LoadInt32(&newDefCauslationEnabled) == 1 {
		if id < 0 {
			logutil.BgLogger().Warn("Unexpected negative defCauslation ID for rewrite.", zap.Int32("ID", id))
		} else {
			return -id
		}
	}
	return id
}

// RestoreDefCauslationIDIfNeeded restores a defCauslation id if the new defCauslations are enabled.
func RestoreDefCauslationIDIfNeeded(id int32) int32 {
	if atomic.LoadInt32(&newDefCauslationEnabled) == 1 {
		if id > 0 {
			logutil.BgLogger().Warn("Unexpected positive defCauslation ID for restore.", zap.Int32("ID", id))
		} else {
			return -id
		}
	}
	return id
}

// GetDefCauslator get the defCauslator according to defCauslate, it will return the binary defCauslator if the corresponding defCauslator doesn't exist.
func GetDefCauslator(defCauslate string) DefCauslator {
	if atomic.LoadInt32(&newDefCauslationEnabled) == 1 {
		ctor, ok := newDefCauslatorMap[defCauslate]
		if !ok {
			logutil.BgLogger().Warn(
				"Unable to get defCauslator by name, use binDefCauslator instead.",
				zap.String("name", defCauslate),
				zap.Stack("stack"))
			return newDefCauslatorMap["utf8mb4_bin"]
		}
		return ctor
	}
	return binDefCauslatorInstance
}

// GetDefCauslatorByID get the defCauslator according to id, it will return the binary defCauslator if the corresponding defCauslator doesn't exist.
func GetDefCauslatorByID(id int) DefCauslator {
	if atomic.LoadInt32(&newDefCauslationEnabled) == 1 {
		ctor, ok := newDefCauslatorIDMap[id]
		if !ok {
			logutil.BgLogger().Warn(
				"Unable to get defCauslator by ID, use binDefCauslator instead.",
				zap.Int("ID", id),
				zap.Stack("stack"))
			return newDefCauslatorMap["utf8mb4_bin"]
		}
		return ctor
	}
	return binDefCauslatorInstance
}

// DefCauslationID2Name return the defCauslation name by the given id.
// If the id is not found in the map, the default defCauslation is returned.
func DefCauslationID2Name(id int32) string {
	name, ok := allegrosql.DefCauslations[uint8(id)]
	if !ok {
		// TODO(bb7133): fix repeating logs when the following code is uncommented.
		//logutil.BgLogger().Warn(
		//	"Unable to get defCauslation name from ID, use default defCauslation instead.",
		//	zap.Int32("ID", id),
		//	zap.Stack("stack"))
		return allegrosql.DefaultDefCauslationName
	}
	return name
}

// GetDefCauslationByName wraps charset.GetDefCauslationByName, it checks the defCauslation.
func GetDefCauslationByName(name string) (defCausl *charset.DefCauslation, err error) {
	if defCausl, err = charset.GetDefCauslationByName(name); err != nil {
		return nil, errors.Trace(err)
	}
	if atomic.LoadInt32(&newDefCauslationEnabled) == 1 {
		if _, ok := newDefCauslatorIDMap[defCausl.ID]; !ok {
			return nil, ErrUnsupportedDefCauslation.GenWithStackByArgs(name)
		}
	}
	return
}

// GetSupportedDefCauslations gets information for all defCauslations supported so far.
func GetSupportedDefCauslations() []*charset.DefCauslation {
	if atomic.LoadInt32(&newDefCauslationEnabled) == 1 {
		newSupportedDefCauslations := make([]*charset.DefCauslation, 0, len(newDefCauslatorMap))
		for name := range newDefCauslatorMap {
			if defCausl, err := charset.GetDefCauslationByName(name); err != nil {
				// Should never happens.
				terror.Log(err)
			} else {
				newSupportedDefCauslations = append(newSupportedDefCauslations, defCausl)
			}
		}
		sort.Slice(newSupportedDefCauslations, func(i int, j int) bool {
			return newSupportedDefCauslations[i].Name < newSupportedDefCauslations[j].Name
		})
		return newSupportedDefCauslations
	}
	return charset.GetSupportedDefCauslations()
}

func truncateTailingSpace(str string) string {
	byteLen := len(str)
	i := byteLen - 1
	for ; i >= 0; i-- {
		if str[i] != ' ' {
			break
		}
	}
	str = str[:i+1]
	return str
}

// IsCIDefCauslation returns if the defCauslation is case-sensitive
func IsCIDefCauslation(defCauslate string) bool {
	return defCauslate == "utf8_general_ci" || defCauslate == "utf8mb4_general_ci" ||
		defCauslate == "utf8_unicode_ci" || defCauslate == "utf8mb4_unicode_ci"
}

func init() {
	newDefCauslatorMap = make(map[string]DefCauslator)
	newDefCauslatorIDMap = make(map[int]DefCauslator)

	newDefCauslatorMap["binary"] = &binDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["binary"])] = &binDefCauslator{}
	newDefCauslatorMap["ascii_bin"] = &binPaddingDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["ascii_bin"])] = &binPaddingDefCauslator{}
	newDefCauslatorMap["latin1_bin"] = &binPaddingDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["latin1_bin"])] = &binPaddingDefCauslator{}
	newDefCauslatorMap["utf8mb4_bin"] = &binPaddingDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["utf8mb4_bin"])] = &binPaddingDefCauslator{}
	newDefCauslatorMap["utf8_bin"] = &binPaddingDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["utf8_bin"])] = &binPaddingDefCauslator{}
	newDefCauslatorMap["utf8mb4_general_ci"] = &generalCIDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["utf8mb4_general_ci"])] = &generalCIDefCauslator{}
	newDefCauslatorMap["utf8_general_ci"] = &generalCIDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["utf8_general_ci"])] = &generalCIDefCauslator{}
	newDefCauslatorMap["utf8mb4_unicode_ci"] = &unicodeCIDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["utf8mb4_unicode_ci"])] = &unicodeCIDefCauslator{}
	newDefCauslatorMap["utf8_unicode_ci"] = &unicodeCIDefCauslator{}
	newDefCauslatorIDMap[int(allegrosql.DefCauslationNames["utf8_unicode_ci"])] = &unicodeCIDefCauslator{}
}
