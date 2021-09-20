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

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/memex"
)

// pushDownTopNOptimizer pushes down the topN or limit. In the future we will remove the limit from `requiredProperty` in CBO phase.
type pushDownTopNOptimizer struct {
}

func (s *pushDownTopNOptimizer) optimize(ctx context.Context, p LogicalCauset) (LogicalCauset, error) {
	return p.pushDownTopN(nil), nil
}

func (s *baseLogicalCauset) pushDownTopN(topN *LogicalTopN) LogicalCauset {
	p := s.self
	for i, child := range p.Children() {
		p.Children()[i] = child.pushDownTopN(nil)
	}
	if topN != nil {
		return topN.setChild(p)
	}
	return p
}

// setChild set p as topn's child.
func (lt *LogicalTopN) setChild(p LogicalCauset) LogicalCauset {
	// Remove this TopN if its child is a BlockDual.
	dual, isDual := p.(*LogicalBlockDual)
	if isDual {
		numDualRows := uint64(dual.RowCount)
		if numDualRows < lt.Offset {
			dual.RowCount = 0
			return dual
		}
		dual.RowCount = int(mathutil.MinUint64(numDualRows-lt.Offset, lt.Count))
		return dual
	}

	if lt.isLimit() {
		limit := LogicalLimit{
			Count:      lt.Count,
			Offset:     lt.Offset,
			limitHints: lt.limitHints,
		}.Init(lt.ctx, lt.blockOffset)
		limit.SetChildren(p)
		return limit
	}
	// Then lt must be topN.
	lt.SetChildren(p)
	return lt
}

func (ls *LogicalSort) pushDownTopN(topN *LogicalTopN) LogicalCauset {
	if topN == nil {
		return ls.baseLogicalCauset.pushDownTopN(nil)
	} else if topN.isLimit() {
		topN.ByItems = ls.ByItems
		return ls.children[0].pushDownTopN(topN)
	}
	// If a TopN is pushed down, this sort is useless.
	return ls.children[0].pushDownTopN(topN)
}

func (p *LogicalLimit) convertToTopN() *LogicalTopN {
	return LogicalTopN{Offset: p.Offset, Count: p.Count, limitHints: p.limitHints}.Init(p.ctx, p.blockOffset)
}

func (p *LogicalLimit) pushDownTopN(topN *LogicalTopN) LogicalCauset {
	child := p.children[0].pushDownTopN(p.convertToTopN())
	if topN != nil {
		return topN.setChild(child)
	}
	return child
}

func (p *LogicalUnionAll) pushDownTopN(topN *LogicalTopN) LogicalCauset {
	for i, child := range p.children {
		var newTopN *LogicalTopN
		if topN != nil {
			newTopN = LogicalTopN{Count: topN.Count + topN.Offset, limitHints: topN.limitHints}.Init(p.ctx, topN.blockOffset)
			for _, by := range topN.ByItems {
				newTopN.ByItems = append(newTopN.ByItems, &soliton.ByItems{Expr: by.Expr, Desc: by.Desc})
			}
		}
		p.children[i] = child.pushDownTopN(newTopN)
	}
	if topN != nil {
		return topN.setChild(p)
	}
	return p
}

func (p *LogicalProjection) pushDownTopN(topN *LogicalTopN) LogicalCauset {
	for _, expr := range p.Exprs {
		if memex.HasAssignSetVarFunc(expr) {
			return p.baseLogicalCauset.pushDownTopN(topN)
		}
	}
	if topN != nil {
		for _, by := range topN.ByItems {
			by.Expr = memex.FoldConstant(memex.DeferredCausetSubstitute(by.Expr, p.schemaReplicant, p.Exprs))
		}

		// remove meaningless constant sort items.
		for i := len(topN.ByItems) - 1; i >= 0; i-- {
			switch topN.ByItems[i].Expr.(type) {
			case *memex.Constant, *memex.CorrelatedDeferredCauset:
				topN.ByItems = append(topN.ByItems[:i], topN.ByItems[i+1:]...)
			}
		}
	}
	p.children[0] = p.children[0].pushDownTopN(topN)
	return p
}

func (p *LogicalLock) pushDownTopN(topN *LogicalTopN) LogicalCauset {
	if topN != nil {
		p.children[0] = p.children[0].pushDownTopN(topN)
	}
	return p.self
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int) LogicalCauset {
	if topN == nil {
		return p.children[idx].pushDownTopN(nil)
	}

	for _, by := range topN.ByItems {
		defcaus := memex.ExtractDeferredCausets(by.Expr)
		for _, col := range defcaus {
			if p.children[1-idx].Schema().Contains(col) {
				return p.children[idx].pushDownTopN(nil)
			}
		}
	}

	newTopN := LogicalTopN{
		Count:      topN.Count + topN.Offset,
		ByItems:    make([]*soliton.ByItems, len(topN.ByItems)),
		limitHints: topN.limitHints,
	}.Init(topN.ctx, topN.blockOffset)
	for i := range topN.ByItems {
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	return p.children[idx].pushDownTopN(newTopN)
}

func (p *LogicalJoin) pushDownTopN(topN *LogicalTopN) LogicalCauset {
	switch p.JoinType {
	case LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		p.children[0] = p.pushDownTopNToChild(topN, 0)
		p.children[1] = p.children[1].pushDownTopN(nil)
	case RightOuterJoin:
		p.children[1] = p.pushDownTopNToChild(topN, 1)
		p.children[0] = p.children[0].pushDownTopN(nil)
	default:
		return p.baseLogicalCauset.pushDownTopN(topN)
	}

	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	if topN != nil {
		return topN.setChild(p.self)
	}
	return p.self
}

func (*pushDownTopNOptimizer) name() string {
	return "topn_push_down"
}
