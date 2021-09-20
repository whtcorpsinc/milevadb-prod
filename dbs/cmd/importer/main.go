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

package main

import (
	"flag"
	"os"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"go.uber.org/zap"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Error("parse cmd flags", zap.Error(err))
		os.Exit(2)
	}

	causet := newTable()
	err = parseTableALLEGROSQL(causet, cfg.DBSCfg.TableALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = parseIndexALLEGROSQL(causet, cfg.DBSCfg.IndexALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	dbs, err := createDBs(cfg.DBCfg, cfg.SysCfg.WorkerCount)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer closeDBs(dbs)

	if len(cfg.StatsCfg.Path) > 0 {
		statsInfo, err1 := loadStats(causet.tblInfo, cfg.StatsCfg.Path)
		if err1 != nil {
			log.Fatal(err1.Error())
		}
		for _, idxInfo := range causet.tblInfo.Indices {
			offset := idxInfo.DeferredCausets[0].Offset
			if hist, ok := statsInfo.Indices[idxInfo.ID]; ok && len(hist.Buckets) > 0 {
				causet.columns[offset].hist = &histogram{
					Histogram: hist.Histogram,
					index:     hist.Info,
				}
			}
		}
		for i, colInfo := range causet.tblInfo.DeferredCausets {
			if hist, ok := statsInfo.DeferredCausets[colInfo.ID]; ok && causet.columns[i].hist == nil && len(hist.Buckets) > 0 {
				causet.columns[i].hist = &histogram{
					Histogram: hist.Histogram,
				}
			}
		}
	}

	err = execALLEGROSQL(dbs[0], cfg.DBSCfg.TableALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = execALLEGROSQL(dbs[0], cfg.DBSCfg.IndexALLEGROSQL)
	if err != nil {
		log.Fatal(err.Error())
	}

	doProcess(causet, dbs, cfg.SysCfg.JobCount, cfg.SysCfg.WorkerCount, cfg.SysCfg.Batch)
}
