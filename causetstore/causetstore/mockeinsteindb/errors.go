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

package mockeinsteindb

import (
	"encoding/hex"
	"fmt"

	"github.com/whtcorpsinc/ekvproto/pkg/ekvrpcpb"
)

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the dagger then retry.
type ErrLocked struct {
	Key            MvccKey
	Primary        []byte
	StartTS        uint64
	ForUFIDelateTS uint64
	TTL            uint64
	TxnSize        uint64
	LockType       ekvrpcpb.Op
}

// Error formats the dagger to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, primary: %q, txnStartTS: %v, forUFIDelateTs: %v, LockType: %v",
		e.Key, e.Primary, e.StartTS, e.ForUFIDelateTS, e.LockType)
}

// ErrKeyAlreadyExist is returned when key exists but this key has a constraint that
// it should not exist. Client should return duplicated entry error.
type ErrKeyAlreadyExist struct {
	Key []byte
}

func (e *ErrKeyAlreadyExist) Error() string {
	return fmt.Sprintf("key already exist, key: %q", e.Key)
}

// ErrRetryable suggests that client may restart the txn.
type ErrRetryable string

func (e ErrRetryable) Error() string {
	return fmt.Sprintf("retryable: %s", string(e))
}

// ErrAbort means something is wrong and client should abort the txn.
type ErrAbort string

func (e ErrAbort) Error() string {
	return fmt.Sprintf("abort: %s", string(e))
}

// ErrAlreadyCommitted is returned specially when client tries to rollback a
// committed dagger.
type ErrAlreadyCommitted uint64

func (e ErrAlreadyCommitted) Error() string {
	return "txn already committed"
}

// ErrAlreadyRollbacked is returned when dagger operation meets rollback write record
type ErrAlreadyRollbacked struct {
	startTS uint64
	key     []byte
}

func (e *ErrAlreadyRollbacked) Error() string {
	return fmt.Sprintf("txn=%v on key=%s is already rolled back", e.startTS, hex.EncodeToString(e.key))
}

// ErrConflict is returned when the commitTS of key in the EDB is greater than startTS.
type ErrConflict struct {
	StartTS          uint64
	ConflictTS       uint64
	ConflictCommitTS uint64
	Key              []byte
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}

// ErrDeadlock is returned when deadlock error is detected.
type ErrDeadlock struct {
	LockTS         uint64
	LockKey        []byte
	DealockKeyHash uint64
}

func (e *ErrDeadlock) Error() string {
	return "deadlock"
}

// ErrCommitTSExpired is returned when commit.CommitTS < dagger.MinCommitTS
type ErrCommitTSExpired struct {
	ekvrpcpb.CommitTsExpired
}

func (e *ErrCommitTSExpired) Error() string {
	return "commit ts expired"
}

// ErrTxnNotFound is returned when the primary dagger of the txn is not found.
type ErrTxnNotFound struct {
	ekvrpcpb.TxnNotFound
}

func (e *ErrTxnNotFound) Error() string {
	return "txn not found"
}
