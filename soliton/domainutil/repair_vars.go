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

package petriutil

import (
	"strings"
	"sync"

	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
)

type repairInfo struct {
	sync.RWMutex
	repairMode      bool
	repairBlockList []string
	repairDBInfoMap map[int64]*perceptron.DBInfo
}

// RepairInfo indicates the repaired causet info.
var RepairInfo repairInfo

// InRepairMode indicates whether MilevaDB is in repairMode.
func (r *repairInfo) InRepairMode() bool {
	r.RLock()
	defer r.RUnlock()
	return r.repairMode
}

// SetRepairMode sets whether MilevaDB is in repairMode.
func (r *repairInfo) SetRepairMode(mode bool) {
	r.Lock()
	defer r.Unlock()
	r.repairMode = mode
}

// GetRepairBlockList gets repairing causet list.
func (r *repairInfo) GetRepairBlockList() []string {
	r.RLock()
	defer r.RUnlock()
	return r.repairBlockList
}

// SetRepairBlockList sets repairing causet list.
func (r *repairInfo) SetRepairBlockList(list []string) {
	for i, one := range list {
		list[i] = strings.ToLower(one)
	}
	r.Lock()
	defer r.Unlock()
	r.repairBlockList = list
}

// CheckAndFetchRepairedBlock fetches the repairing causet list from spacetime, true indicates fetch success.
func (r *repairInfo) CheckAndFetchRepairedBlock(di *perceptron.DBInfo, tbl *perceptron.BlockInfo) bool {
	r.Lock()
	defer r.Unlock()
	if !r.repairMode {
		return false
	}
	isRepair := false
	for _, tn := range r.repairBlockList {
		// Use dbName and blockName to specify a causet.
		if strings.ToLower(tn) == di.Name.L+"."+tbl.Name.L {
			isRepair = true
			break
		}
	}
	if isRepair {
		// Record the repaired causet in Map.
		if repairedDB, ok := r.repairDBInfoMap[di.ID]; ok {
			repairedDB.Blocks = append(repairedDB.Blocks, tbl)
		} else {
			// Shallow copy the DBInfo.
			repairedDB := di.Copy()
			// Clean the blocks and set repaired causet.
			repairedDB.Blocks = []*perceptron.BlockInfo{tbl}
			r.repairDBInfoMap[di.ID] = repairedDB
		}
		return true
	}
	return false
}

// GetRepairedBlockInfoByBlockName is exported for test.
func (r *repairInfo) GetRepairedBlockInfoByBlockName(schemaLowerName, blockLowerName string) (*perceptron.BlockInfo, *perceptron.DBInfo) {
	r.RLock()
	defer r.RUnlock()
	for _, EDB := range r.repairDBInfoMap {
		if EDB.Name.L != schemaLowerName {
			continue
		}
		for _, t := range EDB.Blocks {
			if t.Name.L == blockLowerName {
				return t, EDB
			}
		}
		return nil, EDB
	}
	return nil, nil
}

// RemoveFromRepairInfo remove the causet from repair info when repaired.
func (r *repairInfo) RemoveFromRepairInfo(schemaLowerName, blockLowerName string) {
	repairedLowerName := schemaLowerName + "." + blockLowerName
	// Remove from the repair list.
	r.Lock()
	defer r.Unlock()
	for i, rt := range r.repairBlockList {
		if strings.ToLower(rt) == repairedLowerName {
			r.repairBlockList = append(r.repairBlockList[:i], r.repairBlockList[i+1:]...)
			break
		}
	}
	// Remove from the repair map.
	for _, EDB := range r.repairDBInfoMap {
		if EDB.Name.L == schemaLowerName {
			for j, t := range EDB.Blocks {
				if t.Name.L == blockLowerName {
					EDB.Blocks = append(EDB.Blocks[:j], EDB.Blocks[j+1:]...)
					break
				}
			}
			if len(EDB.Blocks) == 0 {
				delete(r.repairDBInfoMap, EDB.ID)
			}
			break
		}
	}
	if len(r.repairDBInfoMap) == 0 {
		r.repairMode = false
	}
}

// repairKeyType is keyType for admin repair causet.
type repairKeyType int

const (
	// RepairedBlock is the key type, caching the target repaired causet in stochastikCtx.
	RepairedBlock repairKeyType = iota
	// RepairedDatabase is the key type, caching the target repaired database in stochastikCtx.
	RepairedDatabase
)

func (t repairKeyType) String() (res string) {
	switch t {
	case RepairedBlock:
		res = "RepairedBlock"
	case RepairedDatabase:
		res = "RepairedDatabase"
	}
	return res
}

func init() {
	RepairInfo = repairInfo{}
	RepairInfo.repairMode = false
	RepairInfo.repairBlockList = []string{}
	RepairInfo.repairDBInfoMap = make(map[int64]*perceptron.DBInfo)
}
