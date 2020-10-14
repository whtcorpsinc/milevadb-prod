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
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/statistics/handle"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

type sqlInfoFetcher struct {
	causetstore einsteindb.CausetStorage
	do    *petri.Petri
	s     stochastik.Stochastik
}

type blockNamePair struct {
	DBName    string
	BlockName string
}

type blockNameExtractor struct {
	curDB string
	names map[blockNamePair]struct{}
}

func (tne *blockNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.BlockName); ok {
		return in, true
	}
	return in, false
}

func (tne *blockNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.BlockName); ok {
		tp := blockNamePair{DBName: t.Schema.L, BlockName: t.Name.L}
		if tp.DBName == "" {
			tp.DBName = tne.curDB
		}
		if _, ok := tne.names[tp]; !ok {
			tne.names[tp] = struct{}{}
		}
	}
	return in, true
}

func (sh *sqlInfoFetcher) zipInfoForALLEGROSQL(w http.ResponseWriter, r *http.Request) {
	var err error
	sh.s, err = stochastik.CreateStochastik(sh.causetstore)
	if err != nil {
		serveError(w, http.StatusInternalServerError, fmt.Sprintf("create stochastik failed, err: %v", err))
		return
	}
	defer sh.s.Close()
	sh.do = petri.GetPetri(sh.s)
	reqCtx := r.Context()
	allegrosql := r.FormValue("allegrosql")
	pprofTimeString := r.FormValue("pprof_time")
	timeoutString := r.FormValue("timeout")
	curDB := strings.ToLower(r.FormValue("current_db"))
	if curDB != "" {
		_, err = sh.s.Execute(reqCtx, fmt.Sprintf("use %v", curDB))
		if err != nil {
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("use database %v failed, err: %v", curDB, err))
			return
		}
	}
	var (
		pprofTime int
		timeout   int
	)
	if pprofTimeString != "" {
		pprofTime, err = strconv.Atoi(pprofTimeString)
		if err != nil {
			serveError(w, http.StatusBadRequest, "invalid value for pprof_time, please input a int value larger than 5")
			return
		}
	}
	if pprofTimeString != "" && pprofTime < 5 {
		serveError(w, http.StatusBadRequest, "pprof time is too short, please input a int value larger than 5")
	}
	if timeoutString != "" {
		timeout, err = strconv.Atoi(timeoutString)
		if err != nil {
			serveError(w, http.StatusBadRequest, "invalid value for timeout")
			return
		}
	}
	if timeout < pprofTime {
		timeout = pprofTime
	}
	pairs, err := sh.extractBlockNames(allegrosql, curDB)
	if err != nil {
		serveError(w, http.StatusBadRequest, fmt.Sprintf("invalid ALLEGROALLEGROSQL text, err: %v", err))
		return
	}
	zw := zip.NewWriter(w)
	defer func() {
		terror.Log(zw.Close())
	}()
	for pair := range pairs {
		jsonTbl, err := sh.getStatsForBlock(pair)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.BlockName), err)
			terror.Log(err)
			continue
		}
		statsFw, err := zw.Create(fmt.Sprintf("%v.%v.json", pair.DBName, pair.BlockName))
		if err != nil {
			terror.Log(err)
			continue
		}
		data, err := json.Marshal(jsonTbl)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.BlockName), err)
			terror.Log(err)
			continue
		}
		_, err = statsFw.Write(data)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.stats.err.txt", pair.DBName, pair.BlockName), err)
			terror.Log(err)
			continue
		}
	}
	for pair := range pairs {
		err = sh.getShowCreateBlock(pair, zw)
		if err != nil {
			err = sh.writeErrFile(zw, fmt.Sprintf("%v.%v.schemaReplicant.err.txt", pair.DBName, pair.BlockName), err)
			terror.Log(err)
			return
		}
	}
	// If we don't catch profile. We just get a explain result.
	if pprofTime == 0 {
		recordSets, err := sh.s.(sqlexec.ALLEGROSQLExecutor).Execute(reqCtx, fmt.Sprintf("explain %s", allegrosql))
		if len(recordSets) > 0 {
			defer terror.Call(recordSets[0].Close)
		}
		if err != nil {
			err = sh.writeErrFile(zw, "explain.err.txt", err)
			terror.Log(err)
			return
		}
		sRows, err := stochastik.ResultSetToStringSlice(reqCtx, sh.s, recordSets[0])
		if err != nil {
			err = sh.writeErrFile(zw, "explain.err.txt", err)
			terror.Log(err)
			return
		}
		fw, err := zw.Create("explain.txt")
		if err != nil {
			terror.Log(err)
			return
		}
		for _, event := range sRows {
			fmt.Fprintf(fw, "%s\n", strings.Join(event, "\t"))
		}
	} else {
		// Otherwise we catch a profile and run `EXPLAIN ANALYZE` result.
		ctx, cancelFunc := context.WithCancel(reqCtx)
		timer := time.NewTimer(time.Second * time.Duration(timeout))
		resultChan := make(chan *explainAnalyzeResult)
		go sh.getExplainAnalyze(ctx, allegrosql, resultChan)
		errChan := make(chan error)
		var buf bytes.Buffer
		go sh.catchCPUProfile(reqCtx, pprofTime, &buf, errChan)
		select {
		case result := <-resultChan:
			timer.Stop()
			cancelFunc()
			if result.err != nil {
				err = sh.writeErrFile(zw, "explain_analyze.err.txt", result.err)
				terror.Log(err)
				return
			}
			if len(result.rows) == 0 {
				break
			}
			fw, err := zw.Create("explain_analyze.txt")
			if err != nil {
				terror.Log(err)
				break
			}
			for _, event := range result.rows {
				fmt.Fprintf(fw, "%s\n", strings.Join(event, "\t"))
			}
		case <-timer.C:
			cancelFunc()
		}
		err = dumpCPUProfile(errChan, &buf, zw)
		if err != nil {
			err = sh.writeErrFile(zw, "profile.err.txt", err)
			terror.Log(err)
			return
		}
	}
}

func dumpCPUProfile(errChan chan error, buf *bytes.Buffer, zw *zip.Writer) error {
	err := <-errChan
	if err != nil {
		return err
	}
	fw, err := zw.Create("profile")
	if err != nil {
		return err
	}
	_, err = fw.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (sh *sqlInfoFetcher) writeErrFile(zw *zip.Writer, name string, err error) error {
	fw, err1 := zw.Create(name)
	if err1 != nil {
		return err1
	}
	fmt.Fprintf(fw, "error: %v", err)
	return nil
}

type explainAnalyzeResult struct {
	rows [][]string
	err  error
}

func (sh *sqlInfoFetcher) getExplainAnalyze(ctx context.Context, allegrosql string, resultChan chan<- *explainAnalyzeResult) {
	recordSets, err := sh.s.(sqlexec.ALLEGROSQLExecutor).Execute(ctx, fmt.Sprintf("explain analyze %s", allegrosql))
	if err != nil {
		resultChan <- &explainAnalyzeResult{err: err}
		return
	}
	rows, err := stochastik.ResultSetToStringSlice(ctx, sh.s, recordSets[0])
	if err != nil {
		terror.Log(err)
		return
	}
	if len(recordSets) > 0 {
		terror.Call(recordSets[0].Close)
	}
	resultChan <- &explainAnalyzeResult{rows: rows}
}

func (sh *sqlInfoFetcher) catchCPUProfile(ctx context.Context, sec int, buf *bytes.Buffer, errChan chan<- error) {
	if err := pprof.StartCPUProfile(buf); err != nil {
		errChan <- err
		return
	}
	sleepWithCtx(ctx, time.Duration(sec)*time.Second)
	pprof.StopCPUProfile()
	errChan <- nil
}

func (sh *sqlInfoFetcher) getStatsForBlock(pair blockNamePair) (*handle.JSONBlock, error) {
	is := sh.do.SchemaReplicant()
	h := sh.do.StatsHandle()
	tbl, err := is.BlockByName(perceptron.NewCIStr(pair.DBName), perceptron.NewCIStr(pair.BlockName))
	if err != nil {
		return nil, err
	}
	js, err := h.DumpStatsToJSON(pair.DBName, tbl.Meta(), nil)
	return js, err
}

func (sh *sqlInfoFetcher) getShowCreateBlock(pair blockNamePair, zw *zip.Writer) error {
	recordSets, err := sh.s.(sqlexec.ALLEGROSQLExecutor).Execute(context.TODO(), fmt.Sprintf("show create block `%v`.`%v`", pair.DBName, pair.BlockName))
	if len(recordSets) > 0 {
		defer terror.Call(recordSets[0].Close)
	}
	if err != nil {
		return err
	}
	sRows, err := stochastik.ResultSetToStringSlice(context.Background(), sh.s, recordSets[0])
	if err != nil {
		terror.Log(err)
		return nil
	}
	fw, err := zw.Create(fmt.Sprintf("%v.%v.schemaReplicant.txt", pair.DBName, pair.BlockName))
	if err != nil {
		terror.Log(err)
		return nil
	}
	for _, event := range sRows {
		fmt.Fprintf(fw, "%s\n", strings.Join(event, "\t"))
	}
	return nil
}

func (sh *sqlInfoFetcher) extractBlockNames(allegrosql, curDB string) (map[blockNamePair]struct{}, error) {
	p := BerolinaSQL.New()
	charset, defCauslation := sh.s.GetStochastikVars().GetCharsetInfo()
	stmts, _, err := p.Parse(allegrosql, charset, defCauslation)
	if err != nil {
		return nil, err
	}
	if len(stmts) > 1 {
		return nil, errors.Errorf("Only 1 statement is allowed")
	}
	extractor := &blockNameExtractor{
		curDB: curDB,
		names: make(map[blockNamePair]struct{}),
	}
	stmts[0].Accept(extractor)
	return extractor.names, nil
}
