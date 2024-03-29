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

package dbs

import (
	"context"

	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// Interceptor is used for DBS.
type Interceptor interface {
	// OnGetSchemaReplicant is an intercept which is called in the function dbs.GetSchemaReplicant(). It is used in the tests.
	OnGetSchemaReplicant(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) schemareplicant.SchemaReplicant
}

// BaseInterceptor implements Interceptor.
type BaseInterceptor struct{}

// OnGetSchemaReplicant implements Interceptor.OnGetSchemaReplicant interface.
func (bi *BaseInterceptor) OnGetSchemaReplicant(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) schemareplicant.SchemaReplicant {
	return is
}

// Callback is used for DBS.
type Callback interface {
	// OnChanged is called after schemaReplicant is changed.
	OnChanged(err error) error
	// OnJobRunBefore is called before running job.
	OnJobRunBefore(job *perceptron.Job)
	// OnJobUFIDelated is called after the running job is uFIDelated.
	OnJobUFIDelated(job *perceptron.Job)
	// OnWatched is called after watching tenant is completed.
	OnWatched(ctx context.Context)
}

// BaseCallback implements Callback.OnChanged interface.
type BaseCallback struct {
}

// OnChanged implements Callback interface.
func (c *BaseCallback) OnChanged(err error) error {
	return err
}

// OnJobRunBefore implements Callback.OnJobRunBefore interface.
func (c *BaseCallback) OnJobRunBefore(job *perceptron.Job) {
	// Nothing to do.
}

// OnJobUFIDelated implements Callback.OnJobUFIDelated interface.
func (c *BaseCallback) OnJobUFIDelated(job *perceptron.Job) {
	// Nothing to do.
}

// OnWatched implements Callback.OnWatched interface.
func (c *BaseCallback) OnWatched(ctx context.Context) {
	// Nothing to do.
}
