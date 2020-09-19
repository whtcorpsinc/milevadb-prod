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

package soliton

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/oracle"
)

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ID              uint64
	User            string
	Host            string
	EDB              string
	Digest          string
	Plan            interface{}
	PlanExplainRows [][]string
	Time            time.Time
	Info            string
	CurTxnStartTS   uint64
	StmtCtx         *stmtctx.StatementContext
	StatsInfo       func(interface{}) map[string]uint64
	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the query takes too long, kill it.
	MaxExecutionTime uint64

	State                     uint16
	Command                   byte
	ExceedExpensiveTimeThresh bool
}

// ToRowForShow returns []interface{} for the event data of "SHOW [FULL] PROCESSLIST".
func (pi *ProcessInfo) ToRowForShow(full bool) []interface{} {
	var info interface{}
	if len(pi.Info) > 0 {
		if full {
			info = pi.Info
		} else {
			info = fmt.Sprintf("%.100v", pi.Info)
		}
	}
	t := uint64(time.Since(pi.Time) / time.Second)
	var EDB interface{}
	if len(pi.EDB) > 0 {
		EDB = pi.EDB
	}
	return []interface{}{
		pi.ID,
		pi.User,
		pi.Host,
		EDB,
		allegrosql.Command2Str[pi.Command],
		t,
		serverStatus2Str(pi.State),
		info,
	}
}

func (pi *ProcessInfo) txnStartTs(tz *time.Location) (txnStart string) {
	if pi.CurTxnStartTS > 0 {
		physicalTime := oracle.GetTimeFromTS(pi.CurTxnStartTS)
		txnStart = fmt.Sprintf("%s(%d)", physicalTime.In(tz).Format("01-02 15:04:05.000"), pi.CurTxnStartTS)
	}
	return
}

// ToRow returns []interface{} for the event data of
// "SELECT * FROM INFORMATION_SCHEMA.PROCESSLIST".
func (pi *ProcessInfo) ToRow(tz *time.Location) []interface{} {
	bytesConsumed := int64(0)
	if pi.StmtCtx != nil && pi.StmtCtx.MemTracker != nil {
		bytesConsumed = pi.StmtCtx.MemTracker.BytesConsumed()
	}
	return append(pi.ToRowForShow(true), pi.Digest, bytesConsumed, pi.txnStartTs(tz))
}

// ascServerStatus is a slice of all defined server status in ascending order.
var ascServerStatus = []uint16{
	allegrosql.ServerStatusInTrans,
	allegrosql.ServerStatusAutocommit,
	allegrosql.ServerMoreResultsExists,
	allegrosql.ServerStatusNoGoodIndexUsed,
	allegrosql.ServerStatusNoIndexUsed,
	allegrosql.ServerStatusCursorExists,
	allegrosql.ServerStatusLastRowSend,
	allegrosql.ServerStatusDBDropped,
	allegrosql.ServerStatusNoBackslashEscaped,
	allegrosql.ServerStatusMetadataChanged,
	allegrosql.ServerStatusWasSlow,
	allegrosql.ServerPSOutParams,
}

// mapServerStatus2Str is the map for server status to string.
var mapServerStatus2Str = map[uint16]string{
	allegrosql.ServerStatusInTrans:            "in transaction",
	allegrosql.ServerStatusAutocommit:         "autocommit",
	allegrosql.ServerMoreResultsExists:        "more results exists",
	allegrosql.ServerStatusNoGoodIndexUsed:    "no good index used",
	allegrosql.ServerStatusNoIndexUsed:        "no index used",
	allegrosql.ServerStatusCursorExists:       "cursor exists",
	allegrosql.ServerStatusLastRowSend:        "last event send",
	allegrosql.ServerStatusDBDropped:          "EDB dropped",
	allegrosql.ServerStatusNoBackslashEscaped: "no backslash escaped",
	allegrosql.ServerStatusMetadataChanged:    "metadata changed",
	allegrosql.ServerStatusWasSlow:            "was slow",
	allegrosql.ServerPSOutParams:              "ps out params",
}

// serverStatus2Str convert server status to string.
// Param state is a bit-field. (e.g. 0x0003 = "in transaction; autocommit").
func serverStatus2Str(state uint16) string {
	// l defCauslect server status strings.
	var l []string
	// check each defined server status, if match, append to defCauslector.
	for _, s := range ascServerStatus {
		if state&s == 0 {
			continue
		}
		l = append(l, mapServerStatus2Str[s])
	}
	return strings.Join(l, "; ")
}

// StochastikManager is an interface for stochastik manage. Show processlist and
// kill statement rely on this interface.
type StochastikManager interface {
	ShowProcessList() map[uint64]*ProcessInfo
	GetProcessInfo(id uint64) (*ProcessInfo, bool)
	Kill(connectionID uint64, query bool)
	UFIDelateTLSConfig(cfg *tls.Config)
}
