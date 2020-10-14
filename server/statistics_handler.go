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

package server

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/gcutil"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

// StatsHandler is the handler for dumping statistics.
type StatsHandler struct {
	do *petri.Petri
}

func (s *Server) newStatsHandler() *StatsHandler {
	causetstore, ok := s.driver.(*MilevaDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := stochastik.GetPetri(causetstore.causetstore)
	if err != nil {
		panic("Failed to get petri")
	}
	return &StatsHandler{do}
}

func (sh StatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)

	is := sh.do.SchemaReplicant()
	h := sh.do.StatsHandle()
	tbl, err := is.BlockByName(perceptron.NewCIStr(params[FIDelBName]), perceptron.NewCIStr(params[pBlockName]))
	if err != nil {
		writeError(w, err)
	} else {
		js, err := h.DumpStatsToJSON(params[FIDelBName], tbl.Meta(), nil)
		if err != nil {
			writeError(w, err)
		} else {
			writeData(w, js)
		}
	}
}

// StatsHistoryHandler is the handler for dumping statistics.
type StatsHistoryHandler struct {
	do *petri.Petri
}

func (s *Server) newStatsHistoryHandler() *StatsHistoryHandler {
	causetstore, ok := s.driver.(*MilevaDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := stochastik.GetPetri(causetstore.causetstore)
	if err != nil {
		panic("Failed to get petri")
	}
	return &StatsHistoryHandler{do}
}

func (sh StatsHistoryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(req)
	se, err := stochastik.CreateStochastik(sh.do.CausetStore())
	if err != nil {
		writeError(w, err)
		return
	}
	se.GetStochastikVars().StmtCtx.TimeZone = time.Local
	t, err := types.ParseTime(se.GetStochastikVars().StmtCtx, params[pSnapshot], allegrosql.TypeTimestamp, 6)
	if err != nil {
		writeError(w, err)
		return
	}
	t1, err := t.GoTime(time.Local)
	if err != nil {
		writeError(w, err)
		return
	}
	snapshot := variable.GoTimeToTS(t1)
	err = gcutil.ValidateSnapshot(se, snapshot)
	if err != nil {
		writeError(w, err)
		return
	}

	is, err := sh.do.GetSnapshotSchemaReplicant(snapshot)
	if err != nil {
		writeError(w, err)
		return
	}
	h := sh.do.StatsHandle()
	tbl, err := is.BlockByName(perceptron.NewCIStr(params[FIDelBName]), perceptron.NewCIStr(params[pBlockName]))
	if err != nil {
		writeError(w, err)
		return
	}
	se.GetStochastikVars().SnapshotschemaReplicant, se.GetStochastikVars().SnapshotTS = is, snapshot
	historyStatsExec := se.(sqlexec.RestrictedALLEGROSQLExecutor)
	js, err := h.DumpStatsToJSON(params[FIDelBName], tbl.Meta(), historyStatsExec)
	if err != nil {
		writeError(w, err)
	} else {
		writeData(w, js)
	}
}
