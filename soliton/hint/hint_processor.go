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

package hint

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/format"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"go.uber.org/zap"
)

var supportedHintNameForInsertStmt = map[string]struct{}{}

func init() {
	supportedHintNameForInsertStmt["memory_quota"] = struct{}{}
}

// HintsSet contains all hints of a query.
type HintsSet struct {
	blockHints [][]*ast.BlockOptimizerHint // Slice offset is the traversal order of `SelectStmt` in the ast.
	indexHints [][]*ast.IndexHint          // Slice offset is the traversal order of `BlockName` in the ast.
}

// GetFirstBlockHints gets the first causet hints.
func (hs *HintsSet) GetFirstBlockHints() []*ast.BlockOptimizerHint {
	if len(hs.blockHints) > 0 {
		return hs.blockHints[0]
	}
	return nil
}

// ContainBlockHint checks whether the causet hint set contains a hint.
func (hs *HintsSet) ContainBlockHint(hint string) bool {
	for _, blockHintsForBlock := range hs.blockHints {
		for _, blockHint := range blockHintsForBlock {
			if blockHint.HintName.String() == hint {
				return true
			}
		}
	}
	return false
}

// ExtractBlockHintsFromStmtNode extracts causet hints from this node.
func ExtractBlockHintsFromStmtNode(node ast.Node, sctx stochastikctx.Context) []*ast.BlockOptimizerHint {
	switch x := node.(type) {
	case *ast.SelectStmt:
		return x.BlockHints
	case *ast.UFIDelateStmt:
		return x.BlockHints
	case *ast.DeleteStmt:
		return x.BlockHints
	case *ast.InsertStmt:
		//check duplicated hints
		checkInsertStmtHintDuplicated(node, sctx)
		return x.BlockHints
	case *ast.ExplainStmt:
		return ExtractBlockHintsFromStmtNode(x.Stmt, sctx)
	default:
		return nil
	}
}

// checkInsertStmtHintDuplicated check whether existed the duplicated hints in both insertStmt and its selectStmt.
// If existed, it would send a warning message.
func checkInsertStmtHintDuplicated(node ast.Node, sctx stochastikctx.Context) {
	switch x := node.(type) {
	case *ast.InsertStmt:
		if len(x.BlockHints) > 0 {
			var supportedHint *ast.BlockOptimizerHint
			for _, hint := range x.BlockHints {
				if _, ok := supportedHintNameForInsertStmt[hint.HintName.L]; ok {
					supportedHint = hint
					break
				}
			}
			if supportedHint != nil {
				var duplicatedHint *ast.BlockOptimizerHint
				for _, hint := range ExtractBlockHintsFromStmtNode(x.Select, nil) {
					if hint.HintName.L == supportedHint.HintName.L {
						duplicatedHint = hint
						break
					}
				}
				if duplicatedHint != nil {
					hint := fmt.Sprintf("%s(`%v`)", duplicatedHint.HintName.O, duplicatedHint.HintData)
					err := terror.ClassUtil.New(errno.ErrWarnConflictingHint, fmt.Sprintf(errno.MyALLEGROSQLErrName[errno.ErrWarnConflictingHint], hint))
					sctx.GetStochastikVars().StmtCtx.AppendWarning(err)
				}
			}
		}
	default:
		return
	}
}

// RestoreOptimizerHints restores these hints.
func RestoreOptimizerHints(hints []*ast.BlockOptimizerHint) string {
	hintsStr := make([]string, 0, len(hints))
	hintsMap := make(map[string]struct{}, len(hints))
	for _, hint := range hints {
		hintStr := RestoreBlockOptimizerHint(hint)
		if _, ok := hintsMap[hintStr]; ok {
			continue
		}
		hintsMap[hintStr] = struct{}{}
		hintsStr = append(hintsStr, hintStr)
	}
	return strings.Join(hintsStr, ", ")
}

// RestoreBlockOptimizerHint returns string format of BlockOptimizerHint.
func RestoreBlockOptimizerHint(hint *ast.BlockOptimizerHint) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := hint.Restore(ctx)
	// There won't be any error for optimizer hint.
	if err != nil {
		logutil.BgLogger().Debug("restore BlockOptimizerHint failed", zap.Error(err))
	}
	return strings.ToLower(sb.String())
}

// RestoreIndexHint returns string format of IndexHint.
func RestoreIndexHint(hint *ast.IndexHint) (string, error) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := hint.Restore(ctx)
	if err != nil {
		logutil.BgLogger().Debug("restore IndexHint failed", zap.Error(err))
		return "", err
	}
	return strings.ToLower(sb.String()), nil
}

// Restore returns the string format of HintsSet.
func (hs *HintsSet) Restore() (string, error) {
	hintsStr := make([]string, 0, len(hs.blockHints)+len(hs.indexHints))
	for _, tblHints := range hs.blockHints {
		for _, tblHint := range tblHints {
			hintsStr = append(hintsStr, RestoreBlockOptimizerHint(tblHint))
		}
	}
	for _, idxHints := range hs.indexHints {
		for _, idxHint := range idxHints {
			str, err := RestoreIndexHint(idxHint)
			if err != nil {
				return "", err
			}
			hintsStr = append(hintsStr, str)
		}
	}
	return strings.Join(hintsStr, ", "), nil
}

type hintProcessor struct {
	*HintsSet
	// bindHint2Ast indicates the behavior of the processor, `true` for bind hint to ast, `false` for extract hint from ast.
	bindHint2Ast bool
	blockCounter int
	indexCounter int
}

func (hp *hintProcessor) Enter(in ast.Node) (ast.Node, bool) {
	switch v := in.(type) {
	case *ast.SelectStmt:
		if hp.bindHint2Ast {
			if hp.blockCounter < len(hp.blockHints) {
				v.BlockHints = hp.blockHints[hp.blockCounter]
			} else {
				v.BlockHints = nil
			}
			hp.blockCounter++
		} else {
			hp.blockHints = append(hp.blockHints, v.BlockHints)
		}
	case *ast.BlockName:
		if hp.bindHint2Ast {
			if hp.indexCounter < len(hp.indexHints) {
				v.IndexHints = hp.indexHints[hp.indexCounter]
			} else {
				v.IndexHints = nil
			}
			hp.indexCounter++
		} else {
			hp.indexHints = append(hp.indexHints, v.IndexHints)
		}
	}
	return in, false
}

func (hp *hintProcessor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// DefCauslectHint defCauslects hints for a memex.
func DefCauslectHint(in ast.StmtNode) *HintsSet {
	hp := hintProcessor{HintsSet: &HintsSet{blockHints: make([][]*ast.BlockOptimizerHint, 0, 4), indexHints: make([][]*ast.IndexHint, 0, 4)}}
	in.Accept(&hp)
	return hp.HintsSet
}

// BindHint will add hints for stmt according to the hints in `hintsSet`.
func BindHint(stmt ast.StmtNode, hintsSet *HintsSet) ast.StmtNode {
	hp := hintProcessor{HintsSet: hintsSet, bindHint2Ast: true}
	stmt.Accept(&hp)
	return stmt
}

// ParseHintsSet parses a ALLEGROALLEGROSQL string, then defCauslects and normalizes the HintsSet.
func ParseHintsSet(p *BerolinaSQL.BerolinaSQL, allegrosql, charset, defCauslation, EDB string) (*HintsSet, []error, error) {
	stmtNodes, warns, err := p.Parse(allegrosql, charset, defCauslation)
	if err != nil {
		return nil, nil, err
	}
	if len(stmtNodes) != 1 {
		return nil, nil, errors.New(fmt.Sprintf("bind_sql must be a single memex: %s", allegrosql))
	}
	hs := DefCauslectHint(stmtNodes[0])
	processor := &BlockHintProcessor{}
	stmtNodes[0].Accept(processor)
	for i, tblHints := range hs.blockHints {
		newHints := make([]*ast.BlockOptimizerHint, 0, len(tblHints))
		for _, tblHint := range tblHints {
			if tblHint.HintName.L == hintQBName {
				continue
			}
			offset := processor.GetHintOffset(tblHint.QBName, TypeSelect, i+1)
			if offset < 0 || !processor.checkBlockQBName(tblHint.Blocks, TypeSelect) {
				hintStr := RestoreBlockOptimizerHint(tblHint)
				return nil, nil, errors.New(fmt.Sprintf("Unknown query causet name in hint %s", hintStr))
			}
			tblHint.QBName = GenerateQBName(TypeSelect, offset)
			for i, tbl := range tblHint.Blocks {
				if tbl.DBName.String() == "" {
					tblHint.Blocks[i].DBName = perceptron.NewCIStr(EDB)
				}
			}
			newHints = append(newHints, tblHint)
		}
		hs.blockHints[i] = newHints
	}
	return hs, extractHintWarns(warns), nil
}

func extractHintWarns(warns []error) []error {
	for _, w := range warns {
		if BerolinaSQL.ErrWarnOptimizerHintUnsupportedHint.Equal(w) ||
			BerolinaSQL.ErrWarnOptimizerHintInvalidToken.Equal(w) ||
			BerolinaSQL.ErrWarnMemoryQuotaOverflow.Equal(w) ||
			BerolinaSQL.ErrWarnOptimizerHintParseError.Equal(w) ||
			BerolinaSQL.ErrWarnOptimizerHintInvalidInteger.Equal(w) {
			// Just one warning is enough, however we use a slice here to stop golint complaining
			// "error should be the last type when returning multiple items" for `ParseHintsSet`.
			return []error{w}
		}
	}
	return nil
}

// BlockHintProcessor processes hints at different level of allegrosql memex.
type BlockHintProcessor struct {
	QbNameMap        map[string]int                    // Map from query causet name to select stmt offset.
	QbHints          map[int][]*ast.BlockOptimizerHint // Group all hints at same query causet.
	Ctx              stochastikctx.Context
	selectStmtOffset int
}

// MaxSelectStmtOffset returns the current stmt offset.
func (p *BlockHintProcessor) MaxSelectStmtOffset() int {
	return p.selectStmtOffset
}

// Enter implements Visitor interface.
func (p *BlockHintProcessor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.UFIDelateStmt:
		p.checkQueryBlockHints(node.BlockHints, 0)
	case *ast.DeleteStmt:
		p.checkQueryBlockHints(node.BlockHints, 0)
	case *ast.SelectStmt:
		p.selectStmtOffset++
		node.QueryBlockOffset = p.selectStmtOffset
		p.checkQueryBlockHints(node.BlockHints, node.QueryBlockOffset)
	}
	return in, false
}

// Leave implements Visitor interface.
func (p *BlockHintProcessor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

const hintQBName = "qb_name"

// checkQueryBlockHints checks the validity of query blocks and records the map of query causet name to select offset.
func (p *BlockHintProcessor) checkQueryBlockHints(hints []*ast.BlockOptimizerHint, offset int) {
	var qbName string
	for _, hint := range hints {
		if hint.HintName.L != hintQBName {
			continue
		}
		if qbName != "" {
			if p.Ctx != nil {
				p.Ctx.GetStochastikVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("There are more than two query names in same query causet,, using the first one %s", qbName)))
			}
		} else {
			qbName = hint.QBName.L
		}
	}
	if qbName == "" {
		return
	}
	if p.QbNameMap == nil {
		p.QbNameMap = make(map[string]int)
	}
	if _, ok := p.QbNameMap[qbName]; ok {
		if p.Ctx != nil {
			p.Ctx.GetStochastikVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Duplicate query causet name %s, only the first one is effective", qbName)))
		}
	} else {
		p.QbNameMap[qbName] = offset
	}
}

const (
	defaultUFIDelateBlockName   = "uFIDel_1"
	defaultDeleteBlockName   = "del_1"
	defaultSelectBlockPrefix = "sel_"
)

// NodeType indicates if the node is for SELECT / UFIDelATE / DELETE.
type NodeType int

const (
	// TypeUFIDelate for UFIDelate.
	TypeUFIDelate NodeType = iota
	// TypeDelete for DELETE.
	TypeDelete
	// TypeSelect for SELECT.
	TypeSelect
)

// getBlockName finds the offset of query causet name. It use 0 as offset for top level uFIDelate or delete,
// -1 for invalid causet name.
func (p *BlockHintProcessor) getBlockOffset(blockName perceptron.CIStr, nodeType NodeType) int {
	if p.QbNameMap != nil {
		level, ok := p.QbNameMap[blockName.L]
		if ok {
			return level
		}
	}
	// Handle the default query causet name.
	if nodeType == TypeUFIDelate && blockName.L == defaultUFIDelateBlockName {
		return 0
	}
	if nodeType == TypeDelete && blockName.L == defaultDeleteBlockName {
		return 0
	}
	if nodeType == TypeSelect && strings.HasPrefix(blockName.L, defaultSelectBlockPrefix) {
		suffix := blockName.L[len(defaultSelectBlockPrefix):]
		level, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil || level > int64(p.selectStmtOffset) {
			return -1
		}
		return int(level)
	}
	return -1
}

// GetHintOffset gets the offset of stmt that the hints take effects.
func (p *BlockHintProcessor) GetHintOffset(qbName perceptron.CIStr, nodeType NodeType, currentOffset int) int {
	if qbName.L != "" {
		return p.getBlockOffset(qbName, nodeType)
	}
	return currentOffset
}

func (p *BlockHintProcessor) checkBlockQBName(blocks []ast.HintBlock, nodeType NodeType) bool {
	for _, causet := range blocks {
		if causet.QBName.L != "" && p.getBlockOffset(causet.QBName, nodeType) < 0 {
			return false
		}
	}
	return true
}

// GetCurrentStmtHints extracts all hints that take effects at current stmt.
func (p *BlockHintProcessor) GetCurrentStmtHints(hints []*ast.BlockOptimizerHint, nodeType NodeType, currentOffset int) []*ast.BlockOptimizerHint {
	if p.QbHints == nil {
		p.QbHints = make(map[int][]*ast.BlockOptimizerHint)
	}
	for _, hint := range hints {
		if hint.HintName.L == hintQBName {
			continue
		}
		offset := p.GetHintOffset(hint.QBName, nodeType, currentOffset)
		if offset < 0 || !p.checkBlockQBName(hint.Blocks, nodeType) {
			hintStr := RestoreBlockOptimizerHint(hint)
			p.Ctx.GetStochastikVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Hint %s is ignored due to unknown query causet name", hintStr)))
			continue
		}
		p.QbHints[offset] = append(p.QbHints[offset], hint)
	}
	return p.QbHints[currentOffset]
}

// GenerateQBName builds QBName from offset.
func GenerateQBName(nodeType NodeType, blockOffset int) perceptron.CIStr {
	if nodeType == TypeDelete && blockOffset == 0 {
		return perceptron.NewCIStr(defaultDeleteBlockName)
	} else if nodeType == TypeUFIDelate && blockOffset == 0 {
		return perceptron.NewCIStr(defaultUFIDelateBlockName)
	}
	return perceptron.NewCIStr(fmt.Sprintf("%s%d", defaultSelectBlockPrefix, blockOffset))
}
