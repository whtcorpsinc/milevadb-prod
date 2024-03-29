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

package memo

import (
	causetembedded "github.com/whtcorpsinc/milevadb/causet/embedded"
)

// Operand is the node of a pattern tree, it represents a logical memex operator.
// Different from logical plan operator which holds the full information about an memex
// operator, Operand only stores the type information.
// An Operand may correspond to a concrete logical plan operator, or it can has special meaning,
// e.g, a placeholder for any logical plan operator.
type Operand int

const (
	// OperandAny is a placeholder for any Operand.
	OperandAny Operand = iota
	// OperandJoin is the operand for LogicalJoin.
	OperandJoin
	// OperanPosetDaggregation is the operand for LogicalAggregation.
	OperanPosetDaggregation
	// OperandProjection is the operand for LogicalProjection.
	OperandProjection
	// OperandSelection is the operand for LogicalSelection.
	OperandSelection
	// OperandApply is the operand for LogicalApply.
	OperandApply
	// OperandMaxOneRow is the operand for LogicalMaxOneRow.
	OperandMaxOneRow
	// OperandBlockDual is the operand for LogicalBlockDual.
	OperandBlockDual
	// OperandDataSource is the operand for DataSource.
	OperandDataSource
	// OperandUnionScan is the operand for LogicalUnionScan.
	OperandUnionScan
	// OperandUnionAll is the operand for LogicalUnionAll.
	OperandUnionAll
	// OperandSort is the operand for LogicalSort.
	OperandSort
	// OperandTopN is the operand for LogicalTopN.
	OperandTopN
	// OperandLock is the operand for LogicalLock.
	OperandLock
	// OperandLimit is the operand for LogicalLimit.
	OperandLimit
	// OperandEinsteinDBSingleGather is the operand for EinsteinDBSingleGather.
	OperandEinsteinDBSingleGather
	// OperandMemBlockScan is the operand for MemBlockScan.
	OperandMemBlockScan
	// OperandBlockScan is the operand for BlockScan.
	OperandBlockScan
	// OperandIndexScan is the operand for IndexScan.
	OperandIndexScan
	// OperandShow is the operand for Show.
	OperandShow
	// OperandWindow is the operand for window function.
	OperandWindow
	// OperandUnsupported is the operand for unsupported operators.
	OperandUnsupported
)

// GetOperand maps logical plan operator to Operand.
func GetOperand(p causetembedded.LogicalCauset) Operand {
	switch p.(type) {
	case *causetembedded.LogicalApply:
		return OperandApply
	case *causetembedded.LogicalJoin:
		return OperandJoin
	case *causetembedded.LogicalAggregation:
		return OperanPosetDaggregation
	case *causetembedded.LogicalProjection:
		return OperandProjection
	case *causetembedded.LogicalSelection:
		return OperandSelection
	case *causetembedded.LogicalMaxOneRow:
		return OperandMaxOneRow
	case *causetembedded.LogicalBlockDual:
		return OperandBlockDual
	case *causetembedded.DataSource:
		return OperandDataSource
	case *causetembedded.LogicalUnionScan:
		return OperandUnionScan
	case *causetembedded.LogicalUnionAll:
		return OperandUnionAll
	case *causetembedded.LogicalSort:
		return OperandSort
	case *causetembedded.LogicalTopN:
		return OperandTopN
	case *causetembedded.LogicalLock:
		return OperandLock
	case *causetembedded.LogicalLimit:
		return OperandLimit
	case *causetembedded.EinsteinDBSingleGather:
		return OperandEinsteinDBSingleGather
	case *causetembedded.LogicalBlockScan:
		return OperandBlockScan
	case *causetembedded.LogicalMemBlock:
		return OperandMemBlockScan
	case *causetembedded.LogicalIndexScan:
		return OperandIndexScan
	case *causetembedded.LogicalShow:
		return OperandShow
	case *causetembedded.LogicalWindow:
		return OperandWindow
	default:
		return OperandUnsupported
	}
}

// Match checks if current Operand matches specified one.
func (o Operand) Match(t Operand) bool {
	if o == OperandAny || t == OperandAny {
		return true
	}
	if o == t {
		return true
	}
	return false
}

// Pattern defines the match pattern for a rule. It's a tree-like structure
// which is a piece of a logical memex. Each node in the Pattern tree is
// defined by an Operand and EngineType pair.
type Pattern struct {
	Operand
	EngineTypeSet
	Children []*Pattern
}

// Match checks whether the EngineTypeSet contains the given EngineType
// and whether the two Operands match.
func (p *Pattern) Match(o Operand, e EngineType) bool {
	return p.EngineTypeSet.Contains(e) && p.Operand.Match(o)
}

// MatchOperandAny checks whether the pattern's Operand is OperandAny
// and the EngineTypeSet contains the given EngineType.
func (p *Pattern) MatchOperandAny(e EngineType) bool {
	return p.EngineTypeSet.Contains(e) && p.Operand == OperandAny
}

// NewPattern creates a pattern node according to the Operand and EngineType.
func NewPattern(operand Operand, engineTypeSet EngineTypeSet) *Pattern {
	return &Pattern{Operand: operand, EngineTypeSet: engineTypeSet}
}

// SetChildren sets the Children information for a pattern node.
func (p *Pattern) SetChildren(children ...*Pattern) {
	p.Children = children
}

// BuildPattern builds a Pattern from Operand, EngineType and child Patterns.
// Used in GetPattern() of Transformation interface to generate a Pattern.
func BuildPattern(operand Operand, engineTypeSet EngineTypeSet, children ...*Pattern) *Pattern {
	p := &Pattern{Operand: operand, EngineTypeSet: engineTypeSet}
	p.Children = children
	return p
}
