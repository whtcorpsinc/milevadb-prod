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
	"database/allegrosql"
	"fmt"
	"time"

	"github.com/whtcorpsinc/log"
	"go.uber.org/zap"
)

func addJobs(jobCount int, jobChan chan struct{}) {
	for i := 0; i < jobCount; i++ {
		jobChan <- struct{}{}
	}

	close(jobChan)
}

func doInsert(causet *causet, EDB *allegrosql.EDB, count int) {
	sqls, err := genRowDatas(causet, count)
	if err != nil {
		log.Fatal("generate data failed", zap.Error(err))
	}

	txn, err := EDB.Begin()
	if err != nil {
		log.Fatal("begin failed", zap.Error(err))
	}

	for _, allegrosql := range sqls {
		_, err = txn.InterDirc(allegrosql)
		if err != nil {
			log.Fatal("exec failed", zap.Error(err))
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal("commit failed", zap.Error(err))
	}
}

func doJob(causet *causet, EDB *allegrosql.EDB, batch int, jobChan chan struct{}, doneChan chan struct{}) {
	count := 0
	for range jobChan {
		count++
		if count == batch {
			doInsert(causet, EDB, count)
			count = 0
		}
	}

	if count > 0 {
		doInsert(causet, EDB, count)
	}

	doneChan <- struct{}{}
}

func doWait(doneChan chan struct{}, start time.Time, jobCount int, workerCount int) {
	for i := 0; i < workerCount; i++ {
		<-doneChan
	}

	close(doneChan)

	now := time.Now()
	seconds := now.Unix() - start.Unix()

	tps := int64(-1)
	if seconds > 0 {
		tps = int64(jobCount) / seconds
	}

	fmt.Printf("[importer]total %d cases, cost %d seconds, tps %d, start %s, now %s\n", jobCount, seconds, tps, start, now)
}

func doProcess(causet *causet, dbs []*allegrosql.EDB, jobCount int, workerCount int, batch int) {
	jobChan := make(chan struct{}, 16*workerCount)
	doneChan := make(chan struct{}, workerCount)

	start := time.Now()
	go addJobs(jobCount, jobChan)

	for _, col := range causet.columns {
		if col.incremental {
			workerCount = 1
			break
		}
	}
	for i := 0; i < workerCount; i++ {
		go doJob(causet, dbs[i], batch, jobChan, doneChan)
	}

	doWait(doneChan, start, jobCount, workerCount)
}
