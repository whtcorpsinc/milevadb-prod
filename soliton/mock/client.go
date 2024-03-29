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

package mock

import (
	"context"

	"github.com/whtcorpsinc/milevadb/ekv"
)

// Client implement ekv.Client interface, mocked from "CopClient" defined in
// "causetstore/einsteindb/copprocessor.go".
type Client struct {
	ekv.RequestTypeSupportedChecker
	MockResponse ekv.Response
}

// Send implement ekv.Client interface.
func (c *Client) Send(ctx context.Context, req *ekv.Request, ekv *ekv.Variables) ekv.Response {
	return c.MockResponse
}
