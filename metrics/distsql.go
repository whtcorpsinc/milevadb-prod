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

// distsql metrics.
var (
	DistALLEGROSQLQueryHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "distsql",
			Name:      "handle_query_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled queries.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblALLEGROSQLType})

	DistALLEGROSQLScanKeysPartialHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "distsql",
			Name:      "scan_keys_partial_num",
			Help:      "number of scanned keys for each partial result.",
		},
	)
	DistALLEGROSQLScanKeysHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "distsql",
			Name:      "scan_keys_num",
			Help:      "number of scanned keys for each query.",
		},
	)
	DistALLEGROSQLPartialCountHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "milevadb",
			Subsystem: "distsql",
			Name:      "partial_num",
			Help:      "number of partial results for each query.",
		},
	)
)
