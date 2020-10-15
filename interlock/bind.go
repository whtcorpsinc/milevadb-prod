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

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb/bindinfo"
	"github.com/whtcorpsinc/milevadb/petri"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// ALLEGROSQLBindInterDirc represents a bind interlock.
type ALLEGROSQLBindInterDirc struct {
	baseInterlockingDirectorate

	sqlBindOp    causetcore.ALLEGROSQLBindOpType
	normdOrigALLEGROSQL string
	bindALLEGROSQL      string
	charset      string
	defCauslation    string
	EDB           string
	isGlobal     bool
	bindAst      ast.StmtNode
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ALLEGROSQLBindInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	switch e.sqlBindOp {
	case causetcore.OpALLEGROSQLBindCreate:
		return e.createALLEGROSQLBind()
	case causetcore.OpALLEGROSQLBindDrop:
		return e.dropALLEGROSQLBind()
	case causetcore.OpFlushBindings:
		return e.flushBindings()
	case causetcore.OpCaptureBindings:
		e.captureBindings()
	case causetcore.OpEvolveBindings:
		return e.evolveBindings()
	case causetcore.OpReloadBindings:
		return e.reloadBindings()
	default:
		return errors.Errorf("unsupported ALLEGROALLEGROSQL bind operation: %v", e.sqlBindOp)
	}
	return nil
}

func (e *ALLEGROSQLBindInterDirc) dropALLEGROSQLBind() error {
	var bindInfo *bindinfo.Binding
	if e.bindALLEGROSQL != "" {
		bindInfo = &bindinfo.Binding{
			BindALLEGROSQL:   e.bindALLEGROSQL,
			Charset:   e.charset,
			DefCauslation: e.defCauslation,
		}
	}
	if !e.isGlobal {
		handle := e.ctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
		return handle.DropBindRecord(e.normdOrigALLEGROSQL, e.EDB, bindInfo)
	}
	return petri.GetPetri(e.ctx).BindHandle().DropBindRecord(e.normdOrigALLEGROSQL, e.EDB, bindInfo)
}

func (e *ALLEGROSQLBindInterDirc) createALLEGROSQLBind() error {
	bindInfo := bindinfo.Binding{
		BindALLEGROSQL:   e.bindALLEGROSQL,
		Charset:   e.charset,
		DefCauslation: e.defCauslation,
		Status:    bindinfo.Using,
		Source:    bindinfo.Manual,
	}
	record := &bindinfo.BindRecord{
		OriginalALLEGROSQL: e.normdOrigALLEGROSQL,
		EDB:          e.EDB,
		Bindings:    []bindinfo.Binding{bindInfo},
	}
	if !e.isGlobal {
		handle := e.ctx.Value(bindinfo.StochastikBindInfoKeyType).(*bindinfo.StochastikHandle)
		return handle.CreateBindRecord(e.ctx, record)
	}
	return petri.GetPetri(e.ctx).BindHandle().CreateBindRecord(e.ctx, record)
}

func (e *ALLEGROSQLBindInterDirc) flushBindings() error {
	return petri.GetPetri(e.ctx).BindHandle().FlushBindings()
}

func (e *ALLEGROSQLBindInterDirc) captureBindings() {
	petri.GetPetri(e.ctx).BindHandle().CaptureBaselines()
}

func (e *ALLEGROSQLBindInterDirc) evolveBindings() error {
	return petri.GetPetri(e.ctx).BindHandle().HandleEvolveCausetTask(e.ctx, true)
}

func (e *ALLEGROSQLBindInterDirc) reloadBindings() error {
	return petri.GetPetri(e.ctx).BindHandle().ReloadBindings()
}
