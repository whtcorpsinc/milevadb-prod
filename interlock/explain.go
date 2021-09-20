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

	"github.com/cznic/mathutil"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// ExplainInterDirc represents an explain interlock.
type ExplainInterDirc struct {
	baseInterlockingDirectorate

	explain          *embedded.Explain
	analyzeInterDirc InterlockingDirectorate
	rows             [][]string
	cursor           int
}

// Open implements the InterlockingDirectorate Open interface.
func (e *ExplainInterDirc) Open(ctx context.Context) error {
	if e.analyzeInterDirc != nil {
		return e.analyzeInterDirc.Open(ctx)
	}
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (e *ExplainInterDirc) Close() error {
	e.rows = nil
	return nil
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ExplainInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		var err error
		e.rows, err = e.generateExplainInfo(ctx)
		if err != nil {
			return err
		}
	}

	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurEvents := mathutil.Min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurEvents; i++ {
		for j := range e.rows[i] {
			req.AppendString(j, e.rows[i][j])
		}
	}
	e.cursor += numCurEvents
	return nil
}

func (e *ExplainInterDirc) generateExplainInfo(ctx context.Context) (rows [][]string, err error) {
	closed := false
	defer func() {
		if !closed && e.analyzeInterDirc != nil {
			err = e.analyzeInterDirc.Close()
			closed = true
		}
	}()
	if e.analyzeInterDirc != nil {
		chk := newFirstChunk(e.analyzeInterDirc)
		var nextErr, closeErr error
		for {
			nextErr = Next(ctx, e.analyzeInterDirc, chk)
			if nextErr != nil || chk.NumEvents() == 0 {
				break
			}
		}
		closeErr = e.analyzeInterDirc.Close()
		closed = true
		if nextErr != nil {
			if closeErr != nil {
				err = errors.New(nextErr.Error() + ", " + closeErr.Error())
			} else {
				err = nextErr
			}
		} else if closeErr != nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	if err = e.explain.RenderResult(); err != nil {
		return nil, err
	}
	if e.analyzeInterDirc != nil {
		e.ctx.GetStochastikVars().StmtCtx.RuntimeStatsDefCausl = nil
	}
	return e.explain.Events, nil
}
