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

package embedded

import (
	"context"

	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// extractJoinGroup extracts all the join nodes connected with continuous
// InnerJoins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, LeftJoin(c, d)}.
func extractJoinGroup(p LogicalCauset) (group []LogicalCauset, eqEdges []*memex.ScalarFunction, otherConds []memex.Expression) {
	join, isJoin := p.(*LogicalJoin)
	if !isJoin || join.preferJoinType > uint(0) || join.JoinType != InnerJoin || join.StraightJoin {
		return []LogicalCauset{p}, nil, nil
	}

	lhsGroup, lhsEqualConds, lhsOtherConds := extractJoinGroup(join.children[0])
	rhsGroup, rhsEqualConds, rhsOtherConds := extractJoinGroup(join.children[1])

	group = append(group, lhsGroup...)
	group = append(group, rhsGroup...)
	eqEdges = append(eqEdges, join.EqualConditions...)
	eqEdges = append(eqEdges, lhsEqualConds...)
	eqEdges = append(eqEdges, rhsEqualConds...)
	otherConds = append(otherConds, join.OtherConditions...)
	otherConds = append(otherConds, lhsOtherConds...)
	otherConds = append(otherConds, rhsOtherConds...)
	return group, eqEdges, otherConds
}

type joinReOrderSolver struct {
}

type jrNode struct {
	p       LogicalCauset
	cumCost float64
}

func (s *joinReOrderSolver) optimize(ctx context.Context, p LogicalCauset) (LogicalCauset, error) {
	return s.optimizeRecursive(p.SCtx(), p)
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group.
func (s *joinReOrderSolver) optimizeRecursive(ctx stochastikctx.Context, p LogicalCauset) (LogicalCauset, error) {
	var err error
	curJoinGroup, eqEdges, otherConds := extractJoinGroup(p)
	if len(curJoinGroup) > 1 {
		for i := range curJoinGroup {
			curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i])
			if err != nil {
				return nil, err
			}
		}
		baseGroupSolver := &baseSingleGroupJoinOrderSolver{
			ctx:        ctx,
			otherConds: otherConds,
		}
		if len(curJoinGroup) > ctx.GetStochastikVars().MilevaDBOptJoinReorderThreshold {
			groupSolver := &joinReorderGreedySolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
				eqEdges:                        eqEdges,
			}
			p, err = groupSolver.solve(curJoinGroup)
		} else {
			dpSolver := &joinReorderDPSolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
			}
			dpSolver.newJoin = dpSolver.newJoinWithEdges
			p, err = dpSolver.solve(curJoinGroup, memex.ScalarFuncs2Exprs(eqEdges))
		}
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	newChildren := make([]LogicalCauset, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := s.optimizeRecursive(ctx, child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

type baseSingleGroupJoinOrderSolver struct {
	ctx          stochastikctx.Context
	curJoinGroup []*jrNode
	otherConds   []memex.Expression
}

// baseNodeCumCost calculate the cumulative cost of the node in the join group.
func (s *baseSingleGroupJoinOrderSolver) baseNodeCumCost(groupNode LogicalCauset) float64 {
	cost := groupNode.statsInfo().RowCount
	for _, child := range groupNode.Children() {
		cost += s.baseNodeCumCost(child)
	}
	return cost
}

// makeBushyJoin build bushy tree for the nodes which have no equal condition to connect them.
func (s *baseSingleGroupJoinOrderSolver) makeBushyJoin(cartesianJoinGroup []LogicalCauset) LogicalCauset {
	resultJoinGroup := make([]LogicalCauset, 0, (len(cartesianJoinGroup)+1)/2)
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup = resultJoinGroup[:0]
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			newJoin := s.newCartesianJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1])
			for i := len(s.otherConds) - 1; i >= 0; i-- {
				defcaus := memex.ExtractDeferredCausets(s.otherConds[i])
				if newJoin.schemaReplicant.DeferredCausetsIndices(defcaus) != nil {
					newJoin.OtherConditions = append(newJoin.OtherConditions, s.otherConds[i])
					s.otherConds = append(s.otherConds[:i], s.otherConds[i+1:]...)
				}
			}
			resultJoinGroup = append(resultJoinGroup, newJoin)
		}
		cartesianJoinGroup, resultJoinGroup = resultJoinGroup, cartesianJoinGroup
	}
	return cartesianJoinGroup[0]
}

func (s *baseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild LogicalCauset) *LogicalJoin {
	offset := lChild.SelectBlockOffset()
	if offset != rChild.SelectBlockOffset() {
		offset = -1
	}
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.Init(s.ctx, offset)
	join.SetSchema(memex.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}

func (s *baseSingleGroupJoinOrderSolver) newJoinWithEdges(lChild, rChild LogicalCauset, eqEdges []*memex.ScalarFunction, otherConds []memex.Expression) LogicalCauset {
	newJoin := s.newCartesianJoin(lChild, rChild)
	newJoin.EqualConditions = eqEdges
	newJoin.OtherConditions = otherConds
	return newJoin
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (s *baseSingleGroupJoinOrderSolver) calcJoinCumCost(join LogicalCauset, lNode, rNode *jrNode) float64 {
	return join.statsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

func (*joinReOrderSolver) name() string {
	return "join_reorder"
}
