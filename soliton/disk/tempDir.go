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

package disk

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/danjacques/gofslock/fslock"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var (
	temFIDelirLock fslock.Handle
	sf          singleflight.Group
)

// CheckAndInitTemFIDelir check whether the temp directory is existed.
// If not, initializes the temp directory.
func CheckAndInitTemFIDelir() (err error) {
	_, err, _ = sf.Do("temFIDelir", func() (value interface{}, err error) {
		if !checkTemFIDelirExist() {
			log.Info("Tmp-storage-path not found. Try to initialize TemFIDelir.")
			err = InitializeTemFIDelir()
		}
		return
	})
	return
}

func checkTemFIDelirExist() bool {
	temFIDelir := config.GetGlobalConfig().TempStoragePath
	_, err := os.Stat(temFIDelir)
	if err != nil && !os.IsExist(err) {
		return false
	}
	return true
}

// InitializeTemFIDelir initializes the temp directory.
func InitializeTemFIDelir() error {
	temFIDelir := config.GetGlobalConfig().TempStoragePath
	_, err := os.Stat(temFIDelir)
	if err != nil && !os.IsExist(err) {
		err = os.MkdirAll(temFIDelir, 0755)
		if err != nil {
			return err
		}
	}
	lockFile := "_dir.lock"
	temFIDelirLock, err = fslock.Lock(filepath.Join(temFIDelir, lockFile))
	if err != nil {
		switch err {
		case fslock.ErrLockHeld:
			log.Error("The current temporary storage dir has been occupied by another instance, "+
				"check tmp-storage-path config and make sure they are different.", zap.String("TempStoragePath", temFIDelir), zap.Error(err))
		default:
			log.Error("Failed to acquire exclusive lock on the temporary storage dir.", zap.String("TempStoragePath", temFIDelir), zap.Error(err))
		}
		return err
	}

	subDirs, err := ioutil.ReadDir(temFIDelir)
	if err != nil {
		return err
	}

	// If it exists others files except lock file, creates another goroutine to clean them.
	if len(subDirs) > 1 {
		go func() {
			for _, subDir := range subDirs {
				// Do not remove the lock file.
				if subDir.Name() == lockFile {
					continue
				}
				err := os.RemoveAll(filepath.Join(temFIDelir, subDir.Name()))
				if err != nil {
					log.Warn("Remove temporary file error",
						zap.String("tempStorageSubDir", filepath.Join(temFIDelir, subDir.Name())), zap.Error(err))
				}
			}
		}()
	}
	return nil
}

// CleanUp releases the directory lock when exiting MilevaDB.
func CleanUp() {
	if temFIDelirLock != nil {
		err := temFIDelirLock.Unlock()
		terror.Log(errors.Trace(err))
	}
}
