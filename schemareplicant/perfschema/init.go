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

package perfschema

import (
	"fmt"
	"sync"

	"github.com/whtcorpsinc/berolinaAllegroSQL"
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/expression"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/meta/autoid"
	"github.com/whtcorpsinc/milevadb/soliton"
)

var once sync.Once

// Init register the PERFORMANCE_SCHEMA virtual blocks.
// It should be init(), and the ideal usage should be:
//
// import _ "github.com/whtcorpsinc/milevadb/perfschema"
//
// This function depends on plan/core.init(), which initialize the expression.EvalAstExpr function.
// The initialize order is a problem if init() is used as the function name.
func Init() {
	initOnce := func() {
		p := berolinaAllegroSQL.New()
		tbls := make([]*perceptron.BlockInfo, 0)
		dbID := autoid.PerformanceSchemaDBID
		for _, allegrosql := range perfSchemaBlocks {
			stmt, err := p.ParseOneStmt(allegrosql, "", "")
			if err != nil {
				panic(err)
			}
			meta, err := dbs.BuildBlockInfoFromAST(stmt.(*ast.CreateBlockStmt))
			if err != nil {
				panic(err)
			}
			tbls = append(tbls, meta)
			var ok bool
			meta.ID, ok = blockIDMap[meta.Name.O]
			if !ok {
				panic(fmt.Sprintf("get performance_schema block id failed, unknown system block `%v`", meta.Name.O))
			}
			for i, c := range meta.DeferredCausets {
				c.ID = int64(i) + 1
			}
		}
		dbInfo := &perceptron.DBInfo{
			ID:      dbID,
			Name:    soliton.PerformanceSchemaName,
			Charset: allegrosql.DefaultCharset,
			DefCauslate: allegrosql.DefaultDefCauslationName,
			Blocks:  tbls,
		}
		schemareplicant.RegisterVirtualBlock(dbInfo, blockFromMeta)
	}
	if expression.EvalAstExpr != nil {
		once.Do(initOnce)
	}
}
