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

package property

import (
	"github.com/whtcorpsinc/milevadb/memex"
)

// LogicalProperty stands for logical properties such as schemaReplicant of memex,
// or statistics of columns in schemaReplicant for output of Group.
// All group memexs in a group share same logical property.
type LogicalProperty struct {
	Stats     *StatsInfo
	Schema    *memex.Schema
	MaxOneRow bool
}
