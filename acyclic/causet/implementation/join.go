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

package implementation

import (
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/causet/memo"
)

// HashJoinImpl is the implementation for PhysicalHashJoin.
type HashJoinImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *HashJoinImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	hashJoin := impl.plan.(*causetcore.PhysicalHashJoin)
	// The children here are only used to calculate the cost.
	hashJoin.SetChildren(children[0].GetCauset(), children[1].GetCauset())
	selfCost := hashJoin.GetCost(children[0].GetCauset().StatsCount(), children[1].GetCauset().StatsCount())
	impl.cost = selfCost + children[0].GetCost() + children[1].GetCost()
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *HashJoinImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	hashJoin := impl.plan.(*causetcore.PhysicalHashJoin)
	hashJoin.SetChildren(children[0].GetCauset(), children[1].GetCauset())
	return impl
}

// NewHashJoinImpl creates a new HashJoinImpl.
func NewHashJoinImpl(hashJoin *causetcore.PhysicalHashJoin) *HashJoinImpl {
	return &HashJoinImpl{baseImpl{plan: hashJoin}}
}

// MergeJoinImpl is the implementation for PhysicalMergeJoin.
type MergeJoinImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *MergeJoinImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	mergeJoin := impl.plan.(*causetcore.PhysicalMergeJoin)
	// The children here are only used to calculate the cost.
	mergeJoin.SetChildren(children[0].GetCauset(), children[1].GetCauset())
	selfCost := mergeJoin.GetCost(children[0].GetCauset().StatsCount(), children[1].GetCauset().StatsCount())
	impl.cost = selfCost + children[0].GetCost() + children[1].GetCost()
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *MergeJoinImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	mergeJoin := impl.plan.(*causetcore.PhysicalMergeJoin)
	mergeJoin.SetChildren(children[0].GetCauset(), children[1].GetCauset())
	return impl
}

// NewMergeJoinImpl creates a new MergeJoinImpl.
func NewMergeJoinImpl(mergeJoin *causetcore.PhysicalMergeJoin) *MergeJoinImpl {
	return &MergeJoinImpl{baseImpl{plan: mergeJoin}}
}
