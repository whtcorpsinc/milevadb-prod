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

package interlock

import (
	"context"
	"strings"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/node"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// ChangeInterDirc represents a change interlock.
type ChangeInterDirc struct {
	baseInterlockingDirectorate
	*ast.ChangeStmt
}

// Next implements the InterlockingDirectorate Next interface.
func (e *ChangeInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	HoTT := strings.ToLower(e.NodeType)
	urls := config.GetGlobalConfig().Path
	registry, err := createRegistry(urls)
	if err != nil {
		return err
	}
	nodes, _, err := registry.Nodes(ctx, node.NodePrefix[HoTT])
	if err != nil {
		return err
	}
	state := e.State
	nodeID := e.NodeID
	for _, n := range nodes {
		if n.NodeID != nodeID {
			continue
		}
		switch state {
		case node.Online, node.Pausing, node.Paused, node.Closing, node.Offline:
			n.State = state
			return registry.UFIDelateNode(ctx, node.NodePrefix[HoTT], n)
		default:
			return errors.Errorf("state %s is illegal", state)
		}
	}
	return errors.NotFoundf("node %s, id %s from etcd %s", HoTT, nodeID, urls)
}
