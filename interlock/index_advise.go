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

package interlock

import (
	"context"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// IndexAdviseInterDirc represents a index advise interlock.
type IndexAdviseInterDirc struct {
	baseInterlockingDirectorate

	IsLocal         bool
	indexAdviseInfo *IndexAdviseInfo
}

// Next implements the InterlockingDirectorate Next interface.
func (e *IndexAdviseInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.IsLocal {
		return errors.New("Index Advise: don't support load file without local field")
	}
	if e.indexAdviseInfo.Path == "" {
		return errors.New("Index Advise: infile path is empty")
	}
	if len(e.indexAdviseInfo.LinesInfo.Terminated) == 0 {
		return errors.New("Index Advise: don't support advise index for ALLEGROALLEGROSQL terminated by nil")
	}

	if val := e.ctx.Value(IndexAdviseVarKey); val != nil {
		e.ctx.SetValue(IndexAdviseVarKey, nil)
		return errors.New("Index Advise: previous index advise option isn't closed normally")
	}
	e.ctx.SetValue(IndexAdviseVarKey, e.indexAdviseInfo)
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *IndexAdviseInterDirc) Close() error {
	return nil
}

// Open implements the InterlockingDirectorate Open interface.
func (e *IndexAdviseInterDirc) Open(ctx context.Context) error {
	return nil
}

// IndexAdviseInfo saves the information of index advise operation.
type IndexAdviseInfo struct {
	Path        string
	MaxMinutes  uint64
	MaxIndexNum *ast.MaxIndexNumClause
	LinesInfo   *ast.LinesClause
	Ctx         stochastikctx.Context
	StmtNodes   [][]ast.StmtNode
	Result      *IndexAdvice
}

func (e *IndexAdviseInfo) getStmtNodes(data []byte) error {
	str := string(data)
	sqls := strings.Split(str, e.LinesInfo.Terminated)

	j := 0
	for i, allegrosql := range sqls {
		if allegrosql != "\n" && allegrosql != "" && strings.HasPrefix(allegrosql, e.LinesInfo.Starting) {
			sqls[j] = sqls[i]
			j++
		}
	}
	sqls = sqls[:j]

	sv := e.Ctx.GetStochastikVars()
	e.StmtNodes = make([][]ast.StmtNode, len(sqls))
	sqlBerolinaSQL := BerolinaSQL.New()
	for i, allegrosql := range sqls {
		stmtNodes, warns, err := sqlBerolinaSQL.Parse(allegrosql, "", "")
		if err != nil {
			return err
		}
		for _, warn := range warns {
			sv.StmtCtx.AppendWarning(soliton.SyntaxWarn(warn))
		}
		curStmtNodes := make([]ast.StmtNode, len(stmtNodes))
		copy(curStmtNodes, stmtNodes)
		e.StmtNodes[i] = curStmtNodes
	}
	return nil
}

func (e *IndexAdviseInfo) prepareInfo(data []byte) error {
	if e.MaxMinutes == 0 {
		return errors.New("Index Advise: the maximum execution time limit should be greater than 0")
	}
	if e.MaxIndexNum != nil {
		if e.MaxIndexNum.PerBlock == 0 || e.MaxIndexNum.PerDB == 0 {
			return errors.New("Index Advise: the maximum number of indexes should be greater than 0")
		}
	}
	return e.getStmtNodes(data)
}

// GetIndexAdvice gets the index advice by workload file.
func (e *IndexAdviseInfo) GetIndexAdvice(ctx context.Context, data []byte) error {
	if err := e.prepareInfo(data); err != nil {
		return err
	}
	// TODO: Finish the index advise process. It will be done in another PR.
	return nil
}

// IndexAdvice represents the index advice.
type IndexAdvice struct {
	// TODO: Define index advice data structure and implements the ResultSet interface. It will be done in another PR
}

// IndexAdviseVarKeyType is a dummy type to avoid naming defCauslision in context.
type IndexAdviseVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k IndexAdviseVarKeyType) String() string {
	return "index_advise_var"
}

// IndexAdviseVarKey is a variable key for index advise.
const IndexAdviseVarKey IndexAdviseVarKeyType = 0
