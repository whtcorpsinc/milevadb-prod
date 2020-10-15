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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// InterlockingDirectorateCounter records the number of expensive interlocks.
	InterlockingDirectorateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "interlock",
			Name:      "expensive_total",
			Help:      "Counter of Expensive InterlockingDirectorates.",
		}, []string{LblType},
	)

	// StmtNodeCounter records the number of memex with the same type.
	StmtNodeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "interlock",
			Name:      "memex_total",
			Help:      "Counter of StmtNode.",
		}, []string{LblType})

	// DbStmtNodeCounter records the number of memex with the same type and EDB.
	DbStmtNodeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "milevadb",
			Subsystem: "interlock",
			Name:      "memex_db_total",
			Help:      "Counter of StmtNode by Database.",
		}, []string{LblDb, LblType})
)
