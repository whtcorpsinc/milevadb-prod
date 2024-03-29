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

// TaskType is the type of execution task.
type TaskType int

const (
	// RootTaskType stands for the tasks that executed in the MilevaDB layer.
	RootTaskType TaskType = iota

	// CopSingleReadTaskType stands for the a BlockScan or IndexScan tasks
	// executed in the interlock layer.
	CopSingleReadTaskType

	// CoFIDeloubleReadTaskType stands for the a IndexLookup tasks executed in the
	// interlock layer.
	CoFIDeloubleReadTaskType

	// CopTiFlashLocalReadTaskType stands for flash interlock that read data locally,
	// and only a part of the data is read in one cop task, if the current task type is
	// CopTiFlashLocalReadTaskType, all its children prop's task type is CopTiFlashLocalReadTaskType
	CopTiFlashLocalReadTaskType

	// CopTiFlashGlobalReadTaskType stands for flash interlock that read data globally
	// and all the data of given causet will be read in one cop task, if the current task
	// type is CopTiFlashGlobalReadTaskType, all its children prop's task type is
	// CopTiFlashGlobalReadTaskType
	CopTiFlashGlobalReadTaskType
)

// String implements fmt.Stringer interface.
func (t TaskType) String() string {
	switch t {
	case RootTaskType:
		return "rootTask"
	case CopSingleReadTaskType:
		return "copSingleReadTask"
	case CoFIDeloubleReadTaskType:
		return "coFIDeloubleReadTask"
	case CopTiFlashLocalReadTaskType:
		return "copTiFlashLocalReadTask"
	case CopTiFlashGlobalReadTaskType:
		return "copTiFlashGlobalReadTask"
	}
	return "UnknownTaskType"
}
