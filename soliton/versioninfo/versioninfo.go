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

package versioninfo

const (
	// CommunityEdition is the default edition for building.
	CommunityEdition = "Community"
)

// Version information.
var (
	MilevaDBBuildTS   = "None"
	MilevaDBGitHash   = "None"
	MilevaDBGitBranch = "None"
	MilevaDBEdition   = CommunityEdition
	// EinsteinDBMinVersion is the minimum version of EinsteinDB that can be compatible with the current MilevaDB.
	EinsteinDBMinVersion = "v3.0.0-60965b006877ca7234adaced7890d7b029ed1306"
)
