// Copyright 2020 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package soliton

import (
	"bytes"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
)

// ScanMetaWithPrefix scans spacetimedata with the prefix.
func ScanMetaWithPrefix(retriever ekv.Retriever, prefix ekv.Key, filter func(ekv.Key, []byte) bool) error {
	iter, err := retriever.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return errors.Trace(err)
	}
	defer iter.Close()

	for {
		if iter.Valid() && iter.Key().HasPrefix(prefix) {
			if !filter(iter.Key(), iter.Value()) {
				break
			}
			err = iter.Next()
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			break
		}
	}

	return nil
}

// DelKeyWithPrefix deletes keys with prefix.
func DelKeyWithPrefix(rm ekv.RetrieverMutator, prefix ekv.Key) error {
	var keys []ekv.Key
	iter, err := rm.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return errors.Trace(err)
	}

	defer iter.Close()
	for {
		if iter.Valid() && iter.Key().HasPrefix(prefix) {
			keys = append(keys, iter.Key().Clone())
			err = iter.Next()
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			break
		}
	}

	for _, key := range keys {
		err := rm.Delete(key)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// RowKeyPrefixFilter returns a function which checks whether currentKey has decoded rowKeyPrefix as prefix.
func RowKeyPrefixFilter(rowKeyPrefix ekv.Key) ekv.FnKeyCmp {
	return func(currentKey ekv.Key) bool {
		// Next until key without prefix of this record.
		return !bytes.HasPrefix(currentKey, rowKeyPrefix)
	}
}
