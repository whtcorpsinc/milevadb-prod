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
	"fmt"
	"sort"
	"strings"

	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/soliton/set"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// AggregateFuncExtractor visits Expr tree.
// It converts DefCausunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.SetOprStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	return n, true
}

// WindowFuncExtractor visits Expr tree.
// It converts DefCausunmNameExpr to WindowFuncExpr and collects WindowFuncExpr.
type WindowFuncExtractor struct {
	// WindowFuncs is the collected WindowFuncExprs.
	windowFuncs []*ast.WindowFuncExpr
}

// Enter implements Visitor interface.
func (a *WindowFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *WindowFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.WindowFuncExpr:
		a.windowFuncs = append(a.windowFuncs, v)
	}
	return n, true
}

// logicalSchemaProducer stores the schemaReplicant for the logical plans who can produce schemaReplicant directly.
type logicalSchemaProducer struct {
	schemaReplicant *memex.Schema
	names           types.NameSlice
	baseLogicalCauset
}

// Schema implements the Causet.Schema interface.
func (s *logicalSchemaProducer) Schema() *memex.Schema {
	if s.schemaReplicant == nil {
		s.schemaReplicant = memex.NewSchema()
	}
	return s.schemaReplicant
}

func (s *logicalSchemaProducer) OutputNames() types.NameSlice {
	return s.names
}

func (s *logicalSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// SetSchema implements the Causet.SetSchema interface.
func (s *logicalSchemaProducer) SetSchema(schemaReplicant *memex.Schema) {
	s.schemaReplicant = schemaReplicant
}

func (s *logicalSchemaProducer) setSchemaAndNames(schemaReplicant *memex.Schema, names types.NameSlice) {
	s.schemaReplicant = schemaReplicant
	s.names = names
}

// inlineProjection prunes unneeded columns inline a interlock.
func (s *logicalSchemaProducer) inlineProjection(parentUsedDefCauss []*memex.DeferredCauset) {
	used := memex.GetUsedList(parentUsedDefCauss, s.schemaReplicant)
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] {
			s.schemaReplicant.DeferredCausets = append(s.schemaReplicant.DeferredCausets[:i], s.schemaReplicant.DeferredCausets[i+1:]...)
		}
	}
}

// physicalSchemaProducer stores the schemaReplicant for the physical plans who can produce schemaReplicant directly.
type physicalSchemaProducer struct {
	schemaReplicant *memex.Schema
	basePhysicalCauset
}

func (s *physicalSchemaProducer) cloneWithSelf(newSelf PhysicalCauset) (*physicalSchemaProducer, error) {
	base, err := s.basePhysicalCauset.cloneWithSelf(newSelf)
	if err != nil {
		return nil, err
	}
	return &physicalSchemaProducer{
		basePhysicalCauset: *base,
		schemaReplicant:    s.schemaReplicant.Clone(),
	}, nil
}

// Schema implements the Causet.Schema interface.
func (s *physicalSchemaProducer) Schema() *memex.Schema {
	if s.schemaReplicant == nil {
		s.schemaReplicant = memex.NewSchema()
	}
	return s.schemaReplicant
}

// SetSchema implements the Causet.SetSchema interface.
func (s *physicalSchemaProducer) SetSchema(schemaReplicant *memex.Schema) {
	s.schemaReplicant = schemaReplicant
}

// baseSchemaProducer stores the schemaReplicant for the base plans who can produce schemaReplicant directly.
type baseSchemaProducer struct {
	schemaReplicant *memex.Schema
	names           types.NameSlice
	baseCauset
}

// OutputNames returns the outputting names of each column.
func (s *baseSchemaProducer) OutputNames() types.NameSlice {
	return s.names
}

func (s *baseSchemaProducer) SetOutputNames(names types.NameSlice) {
	s.names = names
}

// Schema implements the Causet.Schema interface.
func (s *baseSchemaProducer) Schema() *memex.Schema {
	if s.schemaReplicant == nil {
		s.schemaReplicant = memex.NewSchema()
	}
	return s.schemaReplicant
}

// SetSchema implements the Causet.SetSchema interface.
func (s *baseSchemaProducer) SetSchema(schemaReplicant *memex.Schema) {
	s.schemaReplicant = schemaReplicant
}

func (s *baseSchemaProducer) setSchemaAndNames(schemaReplicant *memex.Schema, names types.NameSlice) {
	s.schemaReplicant = schemaReplicant
	s.names = names
}

// Schema implements the Causet.Schema interface.
func (p *LogicalMaxOneRow) Schema() *memex.Schema {
	s := p.Children()[0].Schema().Clone()
	resetNotNullFlag(s, 0, s.Len())
	return s
}

func buildLogicalJoinSchema(joinType JoinType, join LogicalCauset) *memex.Schema {
	leftSchema := join.Children()[0].Schema()
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		return leftSchema.Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		newSchema := leftSchema.Clone()
		newSchema.Append(join.Schema().DeferredCausets[join.Schema().Len()-1])
		return newSchema
	}
	newSchema := memex.MergeSchema(leftSchema, join.Children()[1].Schema())
	if joinType == LeftOuterJoin {
		resetNotNullFlag(newSchema, leftSchema.Len(), newSchema.Len())
	} else if joinType == RightOuterJoin {
		resetNotNullFlag(newSchema, 0, leftSchema.Len())
	}
	return newSchema
}

// BuildPhysicalJoinSchema builds the schemaReplicant of PhysicalJoin from it's children's schemaReplicant.
func BuildPhysicalJoinSchema(joinType JoinType, join PhysicalCauset) *memex.Schema {
	switch joinType {
	case SemiJoin, AntiSemiJoin:
		return join.Children()[0].Schema().Clone()
	case LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		newSchema := join.Children()[0].Schema().Clone()
		newSchema.Append(join.Schema().DeferredCausets[join.Schema().Len()-1])
		return newSchema
	}
	return memex.MergeSchema(join.Children()[0].Schema(), join.Children()[1].Schema())
}

// GetStatsInfo gets the statistics info from a physical plan tree.
func GetStatsInfo(i interface{}) map[string]uint64 {
	if i == nil {
		// it's a workaround for https://github.com/whtcorpsinc/milevadb/issues/17419
		// To entirely fix this, uncomment the assertion in TestPreparedIssue17419
		return nil
	}
	p := i.(Causet)
	var physicalCauset PhysicalCauset
	switch x := p.(type) {
	case *Insert:
		physicalCauset = x.SelectCauset
	case *UFIDelate:
		physicalCauset = x.SelectCauset
	case *Delete:
		physicalCauset = x.SelectCauset
	case PhysicalCauset:
		physicalCauset = x
	}

	if physicalCauset == nil {
		return nil
	}

	statsInfos := make(map[string]uint64)
	statsInfos = DefCauslectCausetStatsVersion(physicalCauset, statsInfos)
	return statsInfos
}

// extractStringFromStringSet helps extract string info from set.StringSet
func extractStringFromStringSet(set set.StringSet) string {
	if len(set) < 1 {
		return ""
	}
	l := make([]string, 0, len(set))
	for k := range set {
		l = append(l, fmt.Sprintf(`"%s"`, k))
	}
	sort.Strings(l)
	return fmt.Sprintf("%s", strings.Join(l, ","))
}

func blockHasDirtyContent(ctx stochastikctx.Context, blockInfo *perceptron.BlockInfo) bool {
	pi := blockInfo.GetPartitionInfo()
	if pi == nil {
		return ctx.HasDirtyContent(blockInfo.ID)
	}
	// Currently, we add UnionScan on every partition even though only one partition's data is changed.
	// This is limited by current implementation of Partition Prune. It'll be uFIDelated once we modify that part.
	for _, partition := range pi.Definitions {
		if ctx.HasDirtyContent(partition.ID) {
			return true
		}
	}
	return false
}

func cloneExprs(exprs []memex.Expression) []memex.Expression {
	cloned := make([]memex.Expression, 0, len(exprs))
	for _, e := range exprs {
		cloned = append(cloned, e.Clone())
	}
	return cloned
}

func cloneDefCauss(defcaus []*memex.DeferredCauset) []*memex.DeferredCauset {
	cloned := make([]*memex.DeferredCauset, 0, len(defcaus))
	for _, c := range defcaus {
		cloned = append(cloned, c.Clone().(*memex.DeferredCauset))
	}
	return cloned
}

func cloneDefCausInfos(defcaus []*perceptron.DeferredCausetInfo) []*perceptron.DeferredCausetInfo {
	cloned := make([]*perceptron.DeferredCausetInfo, 0, len(defcaus))
	for _, c := range defcaus {
		cloned = append(cloned, c.Clone())
	}
	return cloned
}

func cloneRanges(ranges []*ranger.Range) []*ranger.Range {
	cloned := make([]*ranger.Range, 0, len(ranges))
	for _, r := range ranges {
		cloned = append(cloned, r.Clone())
	}
	return cloned
}

func clonePhysicalCauset(plans []PhysicalCauset) ([]PhysicalCauset, error) {
	cloned := make([]PhysicalCauset, 0, len(plans))
	for _, p := range plans {
		c, err := p.Clone()
		if err != nil {
			return nil, err
		}
		cloned = append(cloned, c)
	}
	return cloned, nil
}
