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
	"time"

	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/soliton/ekvcache"
	"go.uber.org/zap"
)

var defCaus = &DefCauslector{}

// HeapProfileForGlobalMemTracker record heap profile data into each global function memory tracker
func HeapProfileForGlobalMemTracker(d time.Duration) {
	log.Info("Mem Profile Tracker started")
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			err := heapProfileForGlobalMemTracker()
			if err != nil {
				log.Warn("profile memory into tracker failed", zap.Error(err))
			}
		}
	}
}

func heapProfileForGlobalMemTracker() error {
	bytes, err := defCaus.getFuncMemUsage(ekvcache.ProfileName)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			log.Error("GlobalLRUMemUsageTracker meet panic", zap.Any("panic", p), zap.Stack("stack"))
		}
	}()
	ekvcache.GlobalLRUMemUsageTracker.ReplaceBytesUsed(bytes)
	return nil
}
