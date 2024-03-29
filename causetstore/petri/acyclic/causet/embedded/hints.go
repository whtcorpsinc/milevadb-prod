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
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/ekv"
	utilhint "github.com/whtcorpsinc/milevadb/soliton/hint"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// GenHintsFromPhysicalCauset generates hints from physical plan.
func GenHintsFromPhysicalCauset(p Causet) []*ast.BlockOptimizerHint {
	var hints []*ast.BlockOptimizerHint
	switch pp := p.(type) {
	case *Explain:
		return GenHintsFromPhysicalCauset(pp.TargetCauset)
	case *UFIDelate:
		hints = genHintsFromPhysicalCauset(pp.SelectCauset, utilhint.TypeUFIDelate)
	case *Delete:
		hints = genHintsFromPhysicalCauset(pp.SelectCauset, utilhint.TypeDelete)
	case PhysicalCauset:
		hints = genHintsFromPhysicalCauset(pp, utilhint.TypeSelect)
	}
	return hints
}

func getBlockName(tblName perceptron.CIStr, asName *perceptron.CIStr) perceptron.CIStr {
	if asName != nil && asName.L != "" {
		return *asName
	}
	return tblName
}

func extractBlockAsName(p PhysicalCauset) (*perceptron.CIStr, *perceptron.CIStr) {
	if len(p.Children()) > 1 {
		return nil, nil
	}
	switch x := p.(type) {
	case *PhysicalBlockReader:
		ts := x.BlockCausets[0].(*PhysicalBlockScan)
		if ts.BlockAsName.L != "" {
			return &ts.DBName, ts.BlockAsName
		}
		return &ts.DBName, &ts.Block.Name
	case *PhysicalIndexReader:
		is := x.IndexCausets[0].(*PhysicalIndexScan)
		if is.BlockAsName.L != "" {
			return &is.DBName, is.BlockAsName
		}
		return &is.DBName, &is.Block.Name
	case *PhysicalIndexLookUpReader:
		is := x.IndexCausets[0].(*PhysicalIndexScan)
		if is.BlockAsName.L != "" {
			return &is.DBName, is.BlockAsName
		}
		return &is.DBName, &is.Block.Name
	}
	return nil, nil
}

func getJoinHints(sctx stochastikctx.Context, joinType string, parentOffset int, nodeType utilhint.NodeType, children ...PhysicalCauset) (res []*ast.BlockOptimizerHint) {
	for _, child := range children {
		blockOffset := child.SelectBlockOffset()
		if blockOffset == -1 {
			continue
		}
		var dbName, blockName *perceptron.CIStr
		if child.SelectBlockOffset() != parentOffset {
			hintBlock := sctx.GetStochastikVars().CausetAppendSelectBlockAsName[child.SelectBlockOffset()]
			// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select causet.
			dbName, blockName, blockOffset = &hintBlock.DBName, &hintBlock.BlockName, parentOffset
		} else {
			dbName, blockName = extractBlockAsName(child)
		}
		if blockName == nil {
			continue
		}
		res = append(res, &ast.BlockOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, blockOffset),
			HintName: perceptron.NewCIStr(joinType),
			Blocks:   []ast.HintBlock{{DBName: *dbName, BlockName: *blockName}},
		})
		break
	}
	return res
}

func genHintsFromPhysicalCauset(p PhysicalCauset, nodeType utilhint.NodeType) (res []*ast.BlockOptimizerHint) {
	for _, child := range p.Children() {
		res = append(res, genHintsFromPhysicalCauset(child, nodeType)...)
	}
	switch pp := p.(type) {
	case *PhysicalBlockReader:
		tbl := pp.BlockCausets[0].(*PhysicalBlockScan)
		res = append(res, &ast.BlockOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: perceptron.NewCIStr(HintUseIndex),
			Blocks:   []ast.HintBlock{{DBName: tbl.DBName, BlockName: getBlockName(tbl.Block.Name, tbl.BlockAsName)}},
		})
		if tbl.StoreType == ekv.TiFlash {
			res = append(res, &ast.BlockOptimizerHint{
				QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
				HintName: perceptron.NewCIStr(HintReadFromStorage),
				HintData: perceptron.NewCIStr(ekv.TiFlash.Name()),
				Blocks:   []ast.HintBlock{{DBName: tbl.DBName, BlockName: getBlockName(tbl.Block.Name, tbl.BlockAsName)}},
			})
		}
	case *PhysicalIndexLookUpReader:
		index := pp.IndexCausets[0].(*PhysicalIndexScan)
		res = append(res, &ast.BlockOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: perceptron.NewCIStr(HintUseIndex),
			Blocks:   []ast.HintBlock{{DBName: index.DBName, BlockName: getBlockName(index.Block.Name, index.BlockAsName)}},
			Indexes:  []perceptron.CIStr{index.Index.Name},
		})
	case *PhysicalIndexReader:
		index := pp.IndexCausets[0].(*PhysicalIndexScan)
		res = append(res, &ast.BlockOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: perceptron.NewCIStr(HintUseIndex),
			Blocks:   []ast.HintBlock{{DBName: index.DBName, BlockName: getBlockName(index.Block.Name, index.BlockAsName)}},
			Indexes:  []perceptron.CIStr{index.Index.Name},
		})
	case *PhysicalIndexMergeReader:
		Indexs := make([]perceptron.CIStr, 0, 2)
		var blockName perceptron.CIStr
		var blockAsName *perceptron.CIStr
		for _, partialCauset := range pp.PartialCausets {
			if index, ok := partialCauset[0].(*PhysicalIndexScan); ok {
				Indexs = append(Indexs, index.Index.Name)
				blockName = index.Block.Name
				blockAsName = index.BlockAsName
			} else {
				indexName := perceptron.NewCIStr("PRIMARY")
				Indexs = append(Indexs, indexName)
			}
		}
		res = append(res, &ast.BlockOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: perceptron.NewCIStr(HintIndexMerge),
			Blocks:   []ast.HintBlock{{BlockName: getBlockName(blockName, blockAsName)}},
			Indexes:  Indexs,
		})
	case *PhysicalHashAgg:
		res = append(res, &ast.BlockOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: perceptron.NewCIStr(HintHashAgg),
		})
	case *PhysicalStreamAgg:
		res = append(res, &ast.BlockOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: perceptron.NewCIStr(HintStreamAgg),
		})
	case *PhysicalMergeJoin:
		res = append(res, getJoinHints(p.SCtx(), HintSMJ, p.SelectBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalBroadCastJoin:
		res = append(res, getJoinHints(p.SCtx(), HintBCJ, p.SelectBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalHashJoin:
		res = append(res, getJoinHints(p.SCtx(), HintHJ, p.SelectBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalIndexJoin:
		res = append(res, getJoinHints(p.SCtx(), HintINLJ, p.SelectBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexMergeJoin:
		res = append(res, getJoinHints(p.SCtx(), HintINLMJ, p.SelectBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexHashJoin:
		res = append(res, getJoinHints(p.SCtx(), HintINLHJ, p.SelectBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	}
	return res
}
