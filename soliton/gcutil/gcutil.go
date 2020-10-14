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

package gcutil

import (
	"fmt"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
)

const (
	selectVariableValueALLEGROSQL = `SELECT HIGH_PRIORITY variable_value FROM allegrosql.milevadb WHERE variable_name='%s'`
	insertVariableValueALLEGROSQL = `INSERT HIGH_PRIORITY INTO allegrosql.milevadb VALUES ('%[1]s', '%[2]s', '%[3]s')
                              ON DUPLICATE KEY
			                  UFIDelATE variable_value = '%[2]s', comment = '%[3]s'`
)

// CheckGCEnable is use to check whether GC is enable.
func CheckGCEnable(ctx stochastikctx.Context) (enable bool, err error) {
	allegrosql := fmt.Sprintf(selectVariableValueALLEGROSQL, "einsteindb_gc_enable")
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(rows) != 1 {
		return false, errors.New("can not get 'einsteindb_gc_enable'")
	}
	return rows[0].GetString(0) == "true", nil
}

// DisableGC will disable GC enable variable.
func DisableGC(ctx stochastikctx.Context) error {
	allegrosql := fmt.Sprintf(insertVariableValueALLEGROSQL, "einsteindb_gc_enable", "false", "Current GC enable status")
	_, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	return errors.Trace(err)
}

// EnableGC will enable GC enable variable.
func EnableGC(ctx stochastikctx.Context) error {
	allegrosql := fmt.Sprintf(insertVariableValueALLEGROSQL, "einsteindb_gc_enable", "true", "Current GC enable status")
	_, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	return errors.Trace(err)
}

// ValidateSnapshot checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshot(ctx stochastikctx.Context, snapshotTS uint64) error {
	safePointTS, err := GetGCSafePoint(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(perceptron.TSConvert2Time(safePointTS).String())
	}
	return nil
}

// ValidateSnapshotWithGCSafePoint checks that the newly set snapshot time is after GC safe point time.
func ValidateSnapshotWithGCSafePoint(snapshotTS, safePointTS uint64) error {
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenWithStackByArgs(perceptron.TSConvert2Time(safePointTS).String())
	}
	return nil
}

// GetGCSafePoint loads GC safe point time from allegrosql.milevadb.
func GetGCSafePoint(ctx stochastikctx.Context) (uint64, error) {
	allegrosql := fmt.Sprintf(selectVariableValueALLEGROSQL, "einsteindb_gc_safe_point")
	rows, _, err := ctx.(sqlexec.RestrictedALLEGROSQLExecutor).ExecRestrictedALLEGROSQL(allegrosql)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(rows) != 1 {
		return 0, errors.New("can not get 'einsteindb_gc_safe_point'")
	}
	safePointString := rows[0].GetString(0)
	safePointTime, err := soliton.CompatibleParseGCTime(safePointString)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := variable.GoTimeToTS(safePointTime)
	return ts, nil
}
