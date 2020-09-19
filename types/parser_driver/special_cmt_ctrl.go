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

package driver

import (
	"fmt"
	"regexp"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
)

// To add new features that needs to be downgrade-compatible,
// 1. Define a featureID below and make sure it is unique.
//    For example, `const FeatureIDMyFea = "my_fea"`.
// 2. Register the new featureID in init().
//    Only the registered berolinaAllegroSQL can parse the comment annotated with `my_fea`.
//    Now, the berolinaAllegroSQL treats `/*T![my_fea] what_ever */` and `what_ever` equivalent.
//    In other word, the berolinaAllegroSQL in old-version MilevaDB will ignores these comments.
// 3. [optional] Add a pattern into FeatureIDPatterns.
//    This is only required if the new feature is contained in DBS,
//    and we want to comment out this part of ALLEGROALLEGROSQL in binlog.
func init() {
	berolinaAllegroSQL.SpecialCommentsController.Register(string(FeatureIDAutoRandom))
	berolinaAllegroSQL.SpecialCommentsController.Register(string(FeatureIDAutoIDCache))
	berolinaAllegroSQL.SpecialCommentsController.Register(string(FeatureIDAutoRandomBase))
}

// SpecialCommentVersionPrefix is the prefix of MilevaDB execublock comments.
const SpecialCommentVersionPrefix = `/*T!`

// BuildSpecialCommentPrefix returns the prefix of `featureID` special comment.
// For some special feature in MilevaDB, we will refine dbs query with special comment,
// which may be useful when
// A: the downstream is directly MyALLEGROSQL instance (treat it as comment for compatibility).
// B: the downstream is lower version MilevaDB (ignore the unknown feature comment).
// C: the downstream is same/higher version MilevaDB (parse the feature syntax out).
func BuildSpecialCommentPrefix(featureID featureID) string {
	return fmt.Sprintf("%s[%s]", SpecialCommentVersionPrefix, featureID)
}

type featureID string

const (
	// FeatureIDAutoRandom is the `auto_random` feature.
	FeatureIDAutoRandom featureID = "auto_rand"
	// FeatureIDAutoIDCache is the `auto_id_cache` feature.
	FeatureIDAutoIDCache featureID = "auto_id_cache"
	// FeatureIDAutoRandomBase is the `auto_random_base` feature.
	FeatureIDAutoRandomBase featureID = "auto_rand_base"
)

// FeatureIDPatterns is used to record special comments patterns.
var FeatureIDPatterns = map[featureID]*regexp.Regexp{
	FeatureIDAutoRandom:     regexp.MustCompile(`(?i)AUTO_RANDOM\b\s*(\s*\(\s*\d+\s*\)\s*)?`),
	FeatureIDAutoIDCache:    regexp.MustCompile(`(?i)AUTO_ID_CACHE\s*=?\s*\d+\s*`),
	FeatureIDAutoRandomBase: regexp.MustCompile(`(?i)AUTO_RANDOM_BASE\s*=?\s*\d+\s*`),
}
