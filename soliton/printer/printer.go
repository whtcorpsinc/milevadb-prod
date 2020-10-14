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

package printer

import (
	"bytes"
	"encoding/json"
	"fmt"
	_ "runtime" // import link package
	_ "unsafe"  // required by go:linkname

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/soliton/israce"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/versioninfo"
	"go.uber.org/zap"
)

// PrintMilevaDBInfo prints the MilevaDB version information.
func PrintMilevaDBInfo() {
	logutil.BgLogger().Info("Welcome to MilevaDB.",
		zap.String("Release Version", allegrosql.MilevaDBReleaseVersion),
		zap.String("Edition", versioninfo.MilevaDBEdition),
		zap.String("Git Commit Hash", versioninfo.MilevaDBGitHash),
		zap.String("Git Branch", versioninfo.MilevaDBGitBranch),
		zap.String("UTC Build Time", versioninfo.MilevaDBBuildTS),
		zap.String("GoVersion", buildVersion),
		zap.Bool("Race Enabled", israce.RaceEnabled),
		zap.Bool("Check Block Before Drop", config.CheckBlockBeforeDrop),
		zap.String("EinsteinDB Min Version", versioninfo.EinsteinDBMinVersion))
	configJSON, err := json.Marshal(config.GetGlobalConfig())
	if err != nil {
		panic(err)
	}
	logutil.BgLogger().Info("loaded config", zap.ByteString("config", configJSON))
}

// GetMilevaDBInfo returns the git hash and build time of this milevadb-server binary.
func GetMilevaDBInfo() string {
	return fmt.Sprintf("Release Version: %s\n"+
		"Edition: %s\n"+
		"Git Commit Hash: %s\n"+
		"Git Branch: %s\n"+
		"UTC Build Time: %s\n"+
		"GoVersion: %s\n"+
		"Race Enabled: %v\n"+
		"EinsteinDB Min Version: %s\n"+
		"Check Block Before Drop: %v",
		allegrosql.MilevaDBReleaseVersion,
		versioninfo.MilevaDBEdition,
		versioninfo.MilevaDBGitHash,
		versioninfo.MilevaDBGitBranch,
		versioninfo.MilevaDBBuildTS,
		buildVersion,
		israce.RaceEnabled,
		versioninfo.EinsteinDBMinVersion,
		config.CheckBlockBeforeDrop)
}

// checkValidity checks whether defcaus and every data have the same length.
func checkValidity(defcaus []string, quantum [][]string) bool {
	defCausLen := len(defcaus)
	if len(quantum) == 0 || defCausLen == 0 {
		return false
	}

	for _, data := range quantum {
		if defCausLen != len(data) {
			return false
		}
	}

	return true
}

func getMaxDeferCausetLen(defcaus []string, quantum [][]string) []int {
	maxDefCausLen := make([]int, len(defcaus))
	for i, defCaus := range defcaus {
		maxDefCausLen[i] = len(defCaus)
	}

	for _, data := range quantum {
		for i, v := range data {
			if len(v) > maxDefCausLen[i] {
				maxDefCausLen[i] = len(v)
			}
		}
	}

	return maxDefCausLen
}

func getPrintDivLine(maxDefCausLen []int) []byte {
	var value = make([]byte, 0)
	for _, v := range maxDefCausLen {
		value = append(value, '+')
		value = append(value, bytes.Repeat([]byte{'-'}, v+2)...)
	}
	value = append(value, '+')
	value = append(value, '\n')
	return value
}

func getPrintDefCaus(defcaus []string, maxDefCausLen []int) []byte {
	var value = make([]byte, 0)
	for i, v := range defcaus {
		value = append(value, '|')
		value = append(value, ' ')
		value = append(value, []byte(v)...)
		value = append(value, bytes.Repeat([]byte{' '}, maxDefCausLen[i]+1-len(v))...)
	}
	value = append(value, '|')
	value = append(value, '\n')
	return value
}

func getPrintEvent(data []string, maxDefCausLen []int) []byte {
	var value = make([]byte, 0)
	for i, v := range data {
		value = append(value, '|')
		value = append(value, ' ')
		value = append(value, []byte(v)...)
		value = append(value, bytes.Repeat([]byte{' '}, maxDefCausLen[i]+1-len(v))...)
	}
	value = append(value, '|')
	value = append(value, '\n')
	return value
}

func getPrintEvents(quantum [][]string, maxDefCausLen []int) []byte {
	var value = make([]byte, 0)
	for _, data := range quantum {
		value = append(value, getPrintEvent(data, maxDefCausLen)...)
	}
	return value
}

// GetPrintResult gets a result with a formatted string.
func GetPrintResult(defcaus []string, quantum [][]string) (string, bool) {
	if !checkValidity(defcaus, quantum) {
		return "", false
	}

	var value = make([]byte, 0)
	maxDefCausLen := getMaxDeferCausetLen(defcaus, quantum)

	value = append(value, getPrintDivLine(maxDefCausLen)...)
	value = append(value, getPrintDefCaus(defcaus, maxDefCausLen)...)
	value = append(value, getPrintDivLine(maxDefCausLen)...)
	value = append(value, getPrintEvents(quantum, maxDefCausLen)...)
	value = append(value, getPrintDivLine(maxDefCausLen)...)
	return string(value), true
}

//go:linkname buildVersion runtime.buildVersion
var buildVersion string
