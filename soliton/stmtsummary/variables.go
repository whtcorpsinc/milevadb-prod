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

package stmtsummary

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/config"
)

const (
	typeEnable = iota
	typeEnableInternalQuery
	typeRefreshInterval
	typeHistorySize
	typeMaxStmtCount
	typeMaxALLEGROSQLLength
	typesNum
)

type systemVars struct {
	sync.Mutex
	// This array itself won't be modified once created. Only its elements may be modified.
	variables []variable
}

type variable struct {
	stochastikValue string
	globalValue  string
	finalValue   int64
}

func newSysVars() *systemVars {
	s := &systemVars{
		variables: make([]variable, typesNum),
	}
	// Initialize these configurations by values in the config file.
	// They may be overwritten by system variables later.
	for varType := range s.variables {
		atomic.StoreInt64(&s.variables[varType].finalValue, getConfigValue(varType))
	}
	return s
}

func (s *systemVars) getVariable(varType int) int64 {
	return atomic.LoadInt64(&s.variables[varType].finalValue)
}

func (s *systemVars) setVariable(varType int, valueStr string, isStochastik bool) error {
	s.Lock()
	defer s.Unlock()

	v := &s.variables[varType]
	if isStochastik {
		v.stochastikValue = valueStr
	} else {
		v.globalValue = valueStr
	}
	stochastikValue := v.stochastikValue
	globalValue := v.globalValue

	var valueInt int64
	switch varType {
	case typeEnable, typeEnableInternalQuery:
		valueInt = getBoolFinalVariable(varType, stochastikValue, globalValue)
	case typeHistorySize, typeMaxALLEGROSQLLength:
		valueInt = getIntFinalVariable(varType, stochastikValue, globalValue, 0)
	case typeRefreshInterval, typeMaxStmtCount:
		valueInt = getIntFinalVariable(varType, stochastikValue, globalValue, 1)
	default:
		return errors.New(fmt.Sprintf("no such type of variable: %d", varType))
	}
	atomic.StoreInt64(&v.finalValue, valueInt)
	return nil
}

func getBoolFinalVariable(varType int, stochastikValue, globalValue string) int64 {
	var valueInt int64
	if len(stochastikValue) > 0 {
		valueInt = normalizeEnableValue(stochastikValue)
	} else if len(globalValue) > 0 {
		valueInt = normalizeEnableValue(globalValue)
	} else {
		valueInt = getConfigValue(varType)
	}
	return valueInt
}

// normalizeEnableValue converts 'ON' or '1' to 1 and 'OFF' or '0' to 0.
func normalizeEnableValue(value string) int64 {
	switch {
	case strings.EqualFold(value, "ON"):
		return 1
	case value == "1":
		return 1
	default:
		return 0
	}
}

func getIntFinalVariable(varType int, stochastikValue, globalValue string, minValue int64) int64 {
	valueInt := minValue - 1
	var err error
	if len(stochastikValue) > 0 {
		valueInt, err = strconv.ParseInt(stochastikValue, 10, 64)
		if err != nil {
			valueInt = minValue - 1
		}
	}
	if valueInt < minValue {
		valueInt, err = strconv.ParseInt(globalValue, 10, 64)
		if err != nil {
			valueInt = minValue - 1
		}
	}
	// If stochastik and global variables are both '', use the value in config.
	if valueInt < minValue {
		valueInt = getConfigValue(varType)
	}
	return valueInt
}

func getConfigValue(varType int) int64 {
	var valueInt int64
	stmtSummaryConfig := config.GetGlobalConfig().StmtSummary
	switch varType {
	case typeEnable:
		if stmtSummaryConfig.Enable {
			valueInt = 1
		}
	case typeEnableInternalQuery:
		if stmtSummaryConfig.EnableInternalQuery {
			valueInt = 1
		}
	case typeRefreshInterval:
		valueInt = int64(stmtSummaryConfig.RefreshInterval)
	case typeHistorySize:
		valueInt = int64(stmtSummaryConfig.HistorySize)
	case typeMaxStmtCount:
		valueInt = int64(stmtSummaryConfig.MaxStmtCount)
	case typeMaxALLEGROSQLLength:
		valueInt = int64(stmtSummaryConfig.MaxALLEGROSQLLength)
	default:
		panic(fmt.Sprintf("No such type of variable: %d", varType))
	}
	return valueInt
}
