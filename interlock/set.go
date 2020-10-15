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
	"fmt"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/defCauslate"
	"github.com/whtcorpsinc/milevadb/soliton/gcutil"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/stmtsummary"
	"github.com/whtcorpsinc/milevadb/soliton/stringutil"
	"go.uber.org/zap"
)

const (
	scopeGlobal  = "global"
	scopeStochastik = "stochastik"
)

// SetInterlockingDirectorate executes set memex.
type SetInterlockingDirectorate struct {
	baseInterlockingDirectorate

	vars []*memex.VarAssignment
	done bool
}

// Next implements the InterlockingDirectorate Next interface.
func (e *SetInterlockingDirectorate) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	stochastikVars := e.ctx.GetStochastikVars()
	for _, v := range e.vars {
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames || v.Name == ast.SetCharset {
			// This is set charset stmt.
			if v.IsDefault {
				err := e.setCharset(allegrosql.DefaultCharset, "", v.Name == ast.SetNames)
				if err != nil {
					return err
				}
				continue
			}
			dt, err := v.Expr.(*memex.Constant).Eval(chunk.Event{})
			if err != nil {
				return err
			}
			cs := dt.GetString()
			var co string
			if v.ExtendValue != nil {
				co = v.ExtendValue.Value.GetString()
			}
			err = e.setCharset(cs, co, v.Name == ast.SetNames)
			if err != nil {
				return err
			}
			continue
		}
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// Set user variable.
			value, err := v.Expr.Eval(chunk.Event{})
			if err != nil {
				return err
			}

			if value.IsNull() {
				delete(stochastikVars.Users, name)
			} else {
				svalue, err1 := value.ToString()
				if err1 != nil {
					return err1
				}

				stochastikVars.SetUserVar(name, stringutil.Copy(svalue), value.DefCauslation())
			}
			continue
		}

		syns := e.getSynonyms(name)
		// Set system variable
		for _, n := range syns {
			err := e.setSysVariable(n, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *SetInterlockingDirectorate) getSynonyms(varName string) []string {
	synonyms, ok := variable.SynonymsSysVariables[varName]
	if ok {
		return synonyms
	}

	synonyms = []string{varName}
	return synonyms
}

func (e *SetInterlockingDirectorate) setSysVariable(name string, v *memex.VarAssignment) error {
	stochastikVars := e.ctx.GetStochastikVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if sysVar.Scope == variable.ScopeNone {
		return errors.Errorf("Variable '%s' is a read only variable", name)
	}
	var valStr string
	var scopeStr string
	if v.IsGlobal {
		scopeStr = scopeGlobal
		// Set global scope system variable.
		if sysVar.Scope&variable.ScopeGlobal == 0 {
			return errors.Errorf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, sysVar)
		if err != nil {
			return err
		}
		if value.IsNull() {
			value.SetString("", allegrosql.DefaultDefCauslationName)
		}
		valStr, err = value.ToString()
		if err != nil {
			return err
		}
		err = stochastikVars.GlobalVarsAccessor.SetGlobalSysVar(name, valStr)
		if err != nil {
			return err
		}
		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			auditPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if auditPlugin.OnGlobalVariableEvent != nil {
				auditPlugin.OnGlobalVariableEvent(context.Background(), e.ctx.GetStochastikVars(), name, valStr)
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else {
		scopeStr = scopeStochastik
		// Set stochastik scope system variable.
		if sysVar.Scope&variable.ScopeStochastik == 0 {
			return errors.Errorf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, nil)
		if err != nil {
			return err
		}
		oldSnapshotTS := stochastikVars.SnapshotTS
		if name == variable.TxnIsolationOneShot && stochastikVars.InTxn() {
			return errors.Trace(ErrCantChangeTxCharacteristics)
		}
		if name == variable.MilevaDBFoundInCausetCache {
			stochastikVars.StmtCtx.AppendWarning(fmt.Errorf("Set operation for '%s' will not take effect", variable.MilevaDBFoundInCausetCache))
			return nil
		}
		err = variable.SetStochastikSystemVar(stochastikVars, name, value)
		if err != nil {
			return err
		}
		newSnapshotIsSet := stochastikVars.SnapshotTS > 0 && stochastikVars.SnapshotTS != oldSnapshotTS
		if newSnapshotIsSet {
			err = gcutil.ValidateSnapshot(e.ctx, stochastikVars.SnapshotTS)
			if err != nil {
				stochastikVars.SnapshotTS = oldSnapshotTS
				return err
			}
		}
		err = e.loadSnapshotSchemaReplicantIfNeeded(name)
		if err != nil {
			stochastikVars.SnapshotTS = oldSnapshotTS
			return err
		}
		if value.IsNull() {
			valStr = "NULL"
		} else {
			var err error
			valStr, err = value.ToString()
			terror.Log(err)
		}
	}
	if scopeStr == scopeGlobal {
		logutil.BgLogger().Info(fmt.Sprintf("set %s var", scopeStr), zap.Uint64("conn", stochastikVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	} else {
		// Clients are often noisy in setting stochastik variables such as
		// autocommit, timezone, query cache
		logutil.BgLogger().Debug(fmt.Sprintf("set %s var", scopeStr), zap.Uint64("conn", stochastikVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	}

	switch name {
	case variable.MilevaDBEnableStmtSummary:
		return stmtsummary.StmtSummaryByDigestMap.SetEnabled(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryInternalQuery:
		return stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryRefreshInterval:
		return stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryHistorySize:
		return stmtsummary.StmtSummaryByDigestMap.SetHistorySize(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryMaxStmtCount:
		return stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(valStr, !v.IsGlobal)
	case variable.MilevaDBStmtSummaryMaxALLEGROSQLLength:
		return stmtsummary.StmtSummaryByDigestMap.SetMaxALLEGROSQLLength(valStr, !v.IsGlobal)
	case variable.MilevaDBCaptureCausetBaseline:
		variable.CaptureCausetBaseline.Set(strings.ToLower(valStr), !v.IsGlobal)
	}

	return nil
}

func (e *SetInterlockingDirectorate) setCharset(cs, co string, isSetName bool) error {
	var err error
	if len(co) == 0 {
		if co, err = charset.GetDefaultDefCauslation(cs); err != nil {
			return err
		}
	} else {
		var defCausl *charset.DefCauslation
		if defCausl, err = defCauslate.GetDefCauslationByName(co); err != nil {
			return err
		}
		if defCausl.CharsetName != cs {
			return charset.ErrDefCauslationCharsetMismatch.GenWithStackByArgs(defCausl.Name, cs)
		}
	}
	stochastikVars := e.ctx.GetStochastikVars()
	if isSetName {
		for _, v := range variable.SetNamesVariables {
			if err = stochastikVars.SetSystemVar(v, cs); err != nil {
				return errors.Trace(err)
			}
		}
		return errors.Trace(stochastikVars.SetSystemVar(variable.DefCauslationConnection, co))
	}
	// Set charset memex, see also https://dev.allegrosql.com/doc/refman/8.0/en/set-character-set.html.
	for _, v := range variable.SetCharsetVariables {
		if err = stochastikVars.SetSystemVar(v, cs); err != nil {
			return errors.Trace(err)
		}
	}
	csDb, err := stochastikVars.GlobalVarsAccessor.GetGlobalSysVar(variable.CharsetDatabase)
	if err != nil {
		return err
	}
	coDb, err := stochastikVars.GlobalVarsAccessor.GetGlobalSysVar(variable.DefCauslationDatabase)
	if err != nil {
		return err
	}
	err = stochastikVars.SetSystemVar(variable.CharacterSetConnection, csDb)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(stochastikVars.SetSystemVar(variable.DefCauslationConnection, coDb))
}

func (e *SetInterlockingDirectorate) getVarValue(v *memex.VarAssignment, sysVar *variable.SysVar) (value types.Causet, err error) {
	if v.IsDefault {
		// To set a SESSION variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MyALLEGROSQL default value, use the DEFAULT keyword.
		// See http://dev.allegrosql.com/doc/refman/5.7/en/set-memex.html
		if sysVar != nil {
			value = types.NewStringCauset(sysVar.Value)
		} else {
			s, err1 := variable.GetGlobalSystemVar(e.ctx.GetStochastikVars(), v.Name)
			if err1 != nil {
				return value, err1
			}
			value = types.NewStringCauset(s)
		}
		return
	}
	value, err = v.Expr.Eval(chunk.Event{})
	return value, err
}

func (e *SetInterlockingDirectorate) loadSnapshotSchemaReplicantIfNeeded(name string) error {
	if name != variable.MilevaDBSnapshot {
		return nil
	}
	vars := e.ctx.GetStochastikVars()
	if vars.SnapshotTS == 0 {
		vars.SnapshotschemaReplicant = nil
		return nil
	}
	logutil.BgLogger().Info("load snapshot info schemaReplicant", zap.Uint64("conn", vars.ConnectionID), zap.Uint64("SnapshotTS", vars.SnapshotTS))
	dom := petri.GetPetri(e.ctx)
	snapInfo, err := dom.GetSnapshotSchemaReplicant(vars.SnapshotTS)
	if err != nil {
		return err
	}
	vars.SnapshotschemaReplicant = snapInfo
	return nil
}
