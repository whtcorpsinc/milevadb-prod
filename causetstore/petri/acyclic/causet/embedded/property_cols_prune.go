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
	"github.com/whtcorpsinc/milevadb/causet/soliton"
	"github.com/whtcorpsinc/milevadb/memex"
)

// preparePossibleProperties traverses the plan tree by a post-order method,
// recursively calls LogicalCauset PreparePossibleProperties interface.
func preparePossibleProperties(lp LogicalCauset) [][]*memex.DeferredCauset {
	childrenProperties := make([][][]*memex.DeferredCauset, 0, len(lp.Children()))
	for _, child := range lp.Children() {
		childrenProperties = append(childrenProperties, preparePossibleProperties(child))
	}
	return lp.PreparePossibleProperties(lp.Schema(), childrenProperties...)
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (ds *DataSource) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	result := make([][]*memex.DeferredCauset, 0, len(ds.possibleAccessPaths))

	for _, path := range ds.possibleAccessPaths {
		if path.IsIntHandlePath {
			col := ds.getPKIsHandleDefCaus()
			if col != nil {
				result = append(result, []*memex.DeferredCauset{col})
			}
			continue
		}

		if len(path.IdxDefCauss) == 0 {
			continue
		}
		result = append(result, make([]*memex.DeferredCauset, len(path.IdxDefCauss)))
		copy(result[len(result)-1], path.IdxDefCauss)
		for i := 0; i < path.EqCondCount && i+1 < len(path.IdxDefCauss); i++ {
			result = append(result, make([]*memex.DeferredCauset, len(path.IdxDefCauss)-i-1))
			copy(result[len(result)-1], path.IdxDefCauss[i+1:])
		}
	}
	return result
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (ts *LogicalBlockScan) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	if ts.HandleDefCauss != nil {
		defcaus := make([]*memex.DeferredCauset, ts.HandleDefCauss.NumDefCauss())
		for i := 0; i < ts.HandleDefCauss.NumDefCauss(); i++ {
			defcaus[i] = ts.HandleDefCauss.GetDefCaus(i)
		}
		return [][]*memex.DeferredCauset{defcaus}
	}
	return nil
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (is *LogicalIndexScan) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	if len(is.IdxDefCauss) == 0 {
		return nil
	}
	result := make([][]*memex.DeferredCauset, 0, is.EqCondCount+1)
	for i := 0; i <= is.EqCondCount; i++ {
		result = append(result, make([]*memex.DeferredCauset, len(is.IdxDefCauss)-i))
		copy(result[i], is.IdxDefCauss[i:])
	}
	return result
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *EinsteinDBSingleGather) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	return childrenProperties[0]
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *LogicalSelection) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	return childrenProperties[0]
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *LogicalWindow) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	result := make([]*memex.DeferredCauset, 0, len(p.PartitionBy)+len(p.OrderBy))
	for i := range p.PartitionBy {
		result = append(result, p.PartitionBy[i].DefCaus)
	}
	for i := range p.OrderBy {
		result = append(result, p.OrderBy[i].DefCaus)
	}
	return [][]*memex.DeferredCauset{result}
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *LogicalSort) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	propDefCauss := getPossiblePropertyFromByItems(p.ByItems)
	if len(propDefCauss) == 0 {
		return nil
	}
	return [][]*memex.DeferredCauset{propDefCauss}
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *LogicalTopN) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	propDefCauss := getPossiblePropertyFromByItems(p.ByItems)
	if len(propDefCauss) == 0 {
		return nil
	}
	return [][]*memex.DeferredCauset{propDefCauss}
}

func getPossiblePropertyFromByItems(items []*soliton.ByItems) []*memex.DeferredCauset {
	defcaus := make([]*memex.DeferredCauset, 0, len(items))
	for _, item := range items {
		if col, ok := item.Expr.(*memex.DeferredCauset); ok {
			defcaus = append(defcaus, col)
		} else {
			break
		}
	}
	return defcaus
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *baseLogicalCauset) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	return nil
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *LogicalProjection) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	childProperties := childrenProperties[0]
	oldDefCauss := make([]*memex.DeferredCauset, 0, p.schemaReplicant.Len())
	newDefCauss := make([]*memex.DeferredCauset, 0, p.schemaReplicant.Len())
	for i, expr := range p.Exprs {
		if col, ok := expr.(*memex.DeferredCauset); ok {
			newDefCauss = append(newDefCauss, p.schemaReplicant.DeferredCausets[i])
			oldDefCauss = append(oldDefCauss, col)
		}
	}
	tmpSchema := memex.NewSchema(oldDefCauss...)
	for i := len(childProperties) - 1; i >= 0; i-- {
		for j, col := range childProperties[i] {
			pos := tmpSchema.DeferredCausetIndex(col)
			if pos >= 0 {
				childProperties[i][j] = newDefCauss[pos]
			} else {
				childProperties[i] = childProperties[i][:j]
				break
			}
		}
		if len(childProperties[i]) == 0 {
			childProperties = append(childProperties[:i], childProperties[i+1:]...)
		}
	}
	return childProperties
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (p *LogicalJoin) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	leftProperties := childrenProperties[0]
	rightProperties := childrenProperties[1]
	// TODO: We should consider properties propagation.
	p.leftProperties = leftProperties
	p.rightProperties = rightProperties
	if p.JoinType == LeftOuterJoin || p.JoinType == LeftOuterSemiJoin {
		rightProperties = nil
	} else if p.JoinType == RightOuterJoin {
		leftProperties = nil
	}
	resultProperties := make([][]*memex.DeferredCauset, len(leftProperties)+len(rightProperties))
	for i, defcaus := range leftProperties {
		resultProperties[i] = make([]*memex.DeferredCauset, len(defcaus))
		copy(resultProperties[i], defcaus)
	}
	leftLen := len(leftProperties)
	for i, defcaus := range rightProperties {
		resultProperties[leftLen+i] = make([]*memex.DeferredCauset, len(defcaus))
		copy(resultProperties[leftLen+i], defcaus)
	}
	return resultProperties
}

// PreparePossibleProperties implements LogicalCauset PreparePossibleProperties interface.
func (la *LogicalAggregation) PreparePossibleProperties(schemaReplicant *memex.Schema, childrenProperties ...[][]*memex.DeferredCauset) [][]*memex.DeferredCauset {
	childProps := childrenProperties[0]
	// If there's no group-by item, the stream aggregation could have no order property. So we can add an empty property
	// when its group-by item is empty.
	if len(la.GroupByItems) == 0 {
		la.possibleProperties = [][]*memex.DeferredCauset{nil}
		return nil
	}
	resultProperties := make([][]*memex.DeferredCauset, 0, len(childProps))
	for _, possibleChildProperty := range childProps {
		sortDefCausOffsets := getMaxSortPrefix(possibleChildProperty, la.groupByDefCauss)
		if len(sortDefCausOffsets) == len(la.groupByDefCauss) {
			resultProperties = append(resultProperties, possibleChildProperty[:len(la.groupByDefCauss)])
		}
	}
	la.possibleProperties = resultProperties
	return la.possibleProperties
}
