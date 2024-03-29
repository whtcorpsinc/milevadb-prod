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
	"math"
	"sort"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/memex"
)

type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
	eqEdges []*memex.ScalarFunction
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//   For base node, its CumCount equals to the sum of the count of its subtree.
//   See baseNodeCumCost for more details.
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodeCausets []LogicalCauset) (LogicalCauset, error) {
	for _, node := range joinNodeCausets {
		_, err := node.recursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: s.baseNodeCumCost(node),
		})
	}
	sort.SliceSblock(s.curJoinGroup, func(i, j int) bool {
		return s.curJoinGroup[i].cumCost < s.curJoinGroup[j].cumCost
	})

	var cartesianGroup []LogicalCauset
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree()
		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []memex.Expression
		var bestJoin LogicalCauset
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
			if newJoin == nil {
				continue
			}
			_, err := newJoin.recursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			break
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftNode, rightNode LogicalCauset) (LogicalCauset, []memex.Expression) {
	var usedEdges []*memex.ScalarFunction
	remainOtherConds := make([]memex.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	for _, edge := range s.eqEdges {
		lDefCaus := edge.GetArgs()[0].(*memex.DeferredCauset)
		rDefCaus := edge.GetArgs()[1].(*memex.DeferredCauset)
		if leftNode.Schema().Contains(lDefCaus) && rightNode.Schema().Contains(rDefCaus) {
			usedEdges = append(usedEdges, edge)
		} else if rightNode.Schema().Contains(lDefCaus) && leftNode.Schema().Contains(rDefCaus) {
			newSf := memex.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rDefCaus, lDefCaus).(*memex.ScalarFunction)
			usedEdges = append(usedEdges, newSf)
		}
	}
	if len(usedEdges) == 0 {
		return nil, nil
	}
	var otherConds []memex.Expression
	mergedSchema := memex.MergeSchema(leftNode.Schema(), rightNode.Schema())
	remainOtherConds, otherConds = memex.FilterOutInPlace(remainOtherConds, func(expr memex.Expression) bool {
		return memex.ExprFromSchema(expr, mergedSchema)
	})
	return s.newJoinWithEdges(leftNode, rightNode, usedEdges, otherConds), remainOtherConds
}
