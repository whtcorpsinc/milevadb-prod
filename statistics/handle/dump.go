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

package handle

import (
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/statistics"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

// JSONTable is used for dumping statistics.
type JSONTable struct {
	DatabaseName string                 `json:"database_name"`
	TableName    string                 `json:"block_name"`
	DeferredCausets      map[string]*jsonDeferredCauset `json:"columns"`
	Indices      map[string]*jsonDeferredCauset `json:"indices"`
	Count        int64                  `json:"count"`
	ModifyCount  int64                  `json:"modify_count"`
	Partitions   map[string]*JSONTable  `json:"partitions"`
}

type jsonDeferredCauset struct {
	Histogram         *fidelpb.Histogram `json:"histogram"`
	CMSketch          *fidelpb.CMSketch  `json:"cm_sketch"`
	NullCount         int64           `json:"null_count"`
	TotDefCausSize        int64           `json:"tot_col_size"`
	LastUFIDelateVersion uint64          `json:"last_uFIDelate_version"`
	Correlation       float64         `json:"correlation"`
}

func dumpJSONDefCaus(hist *statistics.Histogram, CMSketch *statistics.CMSketch) *jsonDeferredCauset {
	jsonDefCaus := &jsonDeferredCauset{
		Histogram:         statistics.HistogramToProto(hist),
		NullCount:         hist.NullCount,
		TotDefCausSize:        hist.TotDefCausSize,
		LastUFIDelateVersion: hist.LastUFIDelateVersion,
		Correlation:       hist.Correlation,
	}
	if CMSketch != nil {
		jsonDefCaus.CMSketch = statistics.CMSketchToProto(CMSketch)
	}
	return jsonDefCaus
}

// DumpStatsToJSON dumps statistic to json.
func (h *Handle) DumpStatsToJSON(dbName string, blockInfo *perceptron.TableInfo, historyStatsExec sqlexec.RestrictedALLEGROSQLExecutor) (*JSONTable, error) {
	pi := blockInfo.GetPartitionInfo()
	if pi == nil {
		return h.blockStatsToJSON(dbName, blockInfo, blockInfo.ID, historyStatsExec)
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    blockInfo.Name.L,
		Partitions:   make(map[string]*JSONTable, len(pi.Definitions)),
	}
	for _, def := range pi.Definitions {
		tbl, err := h.blockStatsToJSON(dbName, blockInfo, def.ID, historyStatsExec)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if tbl == nil {
			continue
		}
		jsonTbl.Partitions[def.Name.L] = tbl
	}
	return jsonTbl, nil
}

func (h *Handle) blockStatsToJSON(dbName string, blockInfo *perceptron.TableInfo, physicalID int64, historyStatsExec sqlexec.RestrictedALLEGROSQLExecutor) (*JSONTable, error) {
	tbl, err := h.blockStatsFromStorage(blockInfo, physicalID, true, historyStatsExec)
	if err != nil || tbl == nil {
		return nil, err
	}
	tbl.Version, tbl.ModifyCount, tbl.Count, err = h.statsMetaByTableIDFromStorage(physicalID, historyStatsExec)
	if err != nil {
		return nil, err
	}
	jsonTbl := &JSONTable{
		DatabaseName: dbName,
		TableName:    blockInfo.Name.L,
		DeferredCausets:      make(map[string]*jsonDeferredCauset, len(tbl.DeferredCausets)),
		Indices:      make(map[string]*jsonDeferredCauset, len(tbl.Indices)),
		Count:        tbl.Count,
		ModifyCount:  tbl.ModifyCount,
	}

	for _, col := range tbl.DeferredCausets {
		sc := &stmtctx.StatementContext{TimeZone: time.UTC}
		hist, err := col.ConvertTo(sc, types.NewFieldType(allegrosql.TypeBlob))
		if err != nil {
			return nil, errors.Trace(err)
		}
		jsonTbl.DeferredCausets[col.Info.Name.L] = dumpJSONDefCaus(hist, col.CMSketch)
	}

	for _, idx := range tbl.Indices {
		jsonTbl.Indices[idx.Info.Name.L] = dumpJSONDefCaus(&idx.Histogram, idx.CMSketch)
	}
	return jsonTbl, nil
}

// LoadStatsFromJSON will load statistic from JSONTable, and save it to the storage.
func (h *Handle) LoadStatsFromJSON(is schemareplicant.SchemaReplicant, jsonTbl *JSONTable) error {
	block, err := is.TableByName(perceptron.NewCIStr(jsonTbl.DatabaseName), perceptron.NewCIStr(jsonTbl.TableName))
	if err != nil {
		return errors.Trace(err)
	}
	blockInfo := block.Meta()
	pi := blockInfo.GetPartitionInfo()
	if pi == nil {
		err := h.loadStatsFromJSON(blockInfo, blockInfo.ID, jsonTbl)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		if jsonTbl.Partitions == nil {
			return errors.New("No partition statistics")
		}
		for _, def := range pi.Definitions {
			tbl := jsonTbl.Partitions[def.Name.L]
			if tbl == nil {
				continue
			}
			err := h.loadStatsFromJSON(blockInfo, def.ID, tbl)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return errors.Trace(h.UFIDelate(is))
}

func (h *Handle) loadStatsFromJSON(blockInfo *perceptron.TableInfo, physicalID int64, jsonTbl *JSONTable) error {
	tbl, err := TableStatsFromJSON(blockInfo, physicalID, jsonTbl)
	if err != nil {
		return errors.Trace(err)
	}

	for _, col := range tbl.DeferredCausets {
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 0, &col.Histogram, col.CMSketch, 1)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range tbl.Indices {
		err = h.SaveStatsToStorage(tbl.PhysicalID, tbl.Count, 1, &idx.Histogram, idx.CMSketch, 1)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = h.SaveMetaToStorage(tbl.PhysicalID, tbl.Count, tbl.ModifyCount)
	return err
}

// TableStatsFromJSON loads statistic from JSONTable and return the Block of statistic.
func TableStatsFromJSON(blockInfo *perceptron.TableInfo, physicalID int64, jsonTbl *JSONTable) (*statistics.Block, error) {
	newHistDefCausl := statistics.HistDefCausl{
		PhysicalID:     physicalID,
		HavePhysicalID: true,
		Count:          jsonTbl.Count,
		ModifyCount:    jsonTbl.ModifyCount,
		DeferredCausets:        make(map[int64]*statistics.DeferredCauset, len(jsonTbl.DeferredCausets)),
		Indices:        make(map[int64]*statistics.Index, len(jsonTbl.Indices)),
	}
	tbl := &statistics.Block{
		HistDefCausl: newHistDefCausl,
	}
	for id, jsonIdx := range jsonTbl.Indices {
		for _, idxInfo := range blockInfo.Indices {
			if idxInfo.Name.L != id {
				continue
			}
			hist := statistics.HistogramFromProto(jsonIdx.Histogram)
			hist.ID, hist.NullCount, hist.LastUFIDelateVersion, hist.Correlation = idxInfo.ID, jsonIdx.NullCount, jsonIdx.LastUFIDelateVersion, jsonIdx.Correlation
			idx := &statistics.Index{
				Histogram: *hist,
				CMSketch:  statistics.CMSketchFromProto(jsonIdx.CMSketch),
				Info:      idxInfo,
			}
			tbl.Indices[idx.ID] = idx
		}
	}

	for id, jsonDefCaus := range jsonTbl.DeferredCausets {
		for _, colInfo := range blockInfo.DeferredCausets {
			if colInfo.Name.L != id {
				continue
			}
			hist := statistics.HistogramFromProto(jsonDefCaus.Histogram)
			count := int64(hist.TotalRowCount())
			sc := &stmtctx.StatementContext{TimeZone: time.UTC}
			hist, err := hist.ConvertTo(sc, &colInfo.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			hist.ID, hist.NullCount, hist.LastUFIDelateVersion, hist.TotDefCausSize, hist.Correlation = colInfo.ID, jsonDefCaus.NullCount, jsonDefCaus.LastUFIDelateVersion, jsonDefCaus.TotDefCausSize, jsonDefCaus.Correlation
			col := &statistics.DeferredCauset{
				PhysicalID: physicalID,
				Histogram:  *hist,
				CMSketch:   statistics.CMSketchFromProto(jsonDefCaus.CMSketch),
				Info:       colInfo,
				Count:      count,
				IsHandle:   blockInfo.PKIsHandle && allegrosql.HasPriKeyFlag(colInfo.Flag),
			}
			tbl.DeferredCausets[col.ID] = col
		}
	}
	return tbl, nil
}
