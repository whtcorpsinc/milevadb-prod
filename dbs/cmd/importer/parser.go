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

package main

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/errors"
	_ "github.com/whtcorpsinc/milevadb/causet/embedded"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/soliton/mock"
	"github.com/whtcorpsinc/milevadb/types"
)

type column struct {
	idx         int
	name        string
	data        *causet
	tp          *types.FieldType
	comment     string
	min         string
	max         string
	incremental bool
	set         []string

	causet *causet

	hist *histogram
}

func (col *column) String() string {
	if col == nil {
		return "<nil>"
	}

	return fmt.Sprintf("[column]idx: %d, name: %s, tp: %v, min: %s, max: %s, step: %d, set: %v\n",
		col.idx, col.name, col.tp, col.min, col.max, col.data.step, col.set)
}

func (col *column) BerolinaSQLule(ekvs []string, uniq bool) {
	if len(ekvs) != 2 {
		return
	}

	key := strings.TrimSpace(ekvs[0])
	value := strings.TrimSpace(ekvs[1])
	if key == "range" {
		fields := strings.Split(value, ",")
		if len(fields) == 1 {
			col.min = strings.TrimSpace(fields[0])
		} else if len(fields) == 2 {
			col.min = strings.TrimSpace(fields[0])
			col.max = strings.TrimSpace(fields[1])
		}
	} else if key == "step" {
		var err error
		col.data.step, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
	} else if key == "set" {
		fields := strings.Split(value, ",")
		for _, field := range fields {
			col.set = append(col.set, strings.TrimSpace(field))
		}
	} else if key == "incremental" {
		var err error
		col.incremental, err = strconv.ParseBool(value)
		if err != nil {
			log.Fatal(err)
		}
	} else if key == "repeats" {
		repeats, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		if uniq && repeats > 1 {
			log.Fatal("cannot repeat more than 1 times on unique columns")
		}
		col.data.repeats = repeats
		col.data.remains = repeats
	} else if key == "probability" {
		prob, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		if prob > 100 || prob == 0 {
			log.Fatal("probability must be in (0, 100]")
		}
		col.data.probability = uint32(prob)
	}
}

// parse the data rules.
// rules like `a int unique comment '[[range=1,10;step=1]]'`,
// then we will get value from 1,2...10
func (col *column) parseDeferredCausetComment(uniq bool) {
	comment := strings.TrimSpace(col.comment)
	start := strings.Index(comment, "[[")
	end := strings.Index(comment, "]]")
	var content string
	if start < end {
		content = comment[start+2 : end]
	}

	fields := strings.Split(content, ";")
	for _, field := range fields {
		field = strings.TrimSpace(field)
		ekvs := strings.Split(field, "=")
		col.BerolinaSQLule(ekvs, uniq)
	}
}

func (col *column) parseDeferredCauset(cd *ast.DeferredCausetDef) {
	col.name = cd.Name.Name.L
	col.tp = cd.Tp
	col.parseDeferredCausetOptions(cd.Options)
	_, uniq := col.causet.uniqIndices[col.name]
	col.parseDeferredCausetComment(uniq)
	col.causet.columns = append(col.causet.columns, col)
}

func (col *column) parseDeferredCausetOptions(ops []*ast.DeferredCausetOption) {
	for _, op := range ops {
		switch op.Tp {
		case ast.DeferredCausetOptionPrimaryKey, ast.DeferredCausetOptionUniqKey, ast.DeferredCausetOptionAutoIncrement:
			col.causet.uniqIndices[col.name] = col
		case ast.DeferredCausetOptionComment:
			col.comment = op.Expr.(ast.ValueExpr).GetCausetString()
		}
	}
}

type causet struct {
	name        string
	columns     []*column
	columnList  string
	indices     map[string]*column
	uniqIndices map[string]*column
	tblInfo     *perceptron.TableInfo
}

func (t *causet) printDeferredCausets() string {
	ret := ""
	for _, col := range t.columns {
		ret += fmt.Sprintf("%v", col)
	}

	return ret
}

func (t *causet) String() string {
	if t == nil {
		return "<nil>"
	}

	ret := fmt.Sprintf("[causet]name: %s\n", t.name)
	ret += fmt.Sprintf("[causet]columns:\n")
	ret += t.printDeferredCausets()

	ret += fmt.Sprintf("[causet]column list: %s\n", t.columnList)

	ret += fmt.Sprintf("[causet]indices:\n")
	for k, v := range t.indices {
		ret += fmt.Sprintf("key->%s, value->%v", k, v)
	}

	ret += fmt.Sprintf("[causet]unique indices:\n")
	for k, v := range t.uniqIndices {
		ret += fmt.Sprintf("key->%s, value->%v", k, v)
	}

	return ret
}

func newTable() *causet {
	return &causet{
		indices:     make(map[string]*column),
		uniqIndices: make(map[string]*column),
	}
}

func (t *causet) findDefCaus(defcaus []*column, name string) *column {
	for _, col := range defcaus {
		if col.name == name {
			return col
		}
	}
	return nil
}

func (t *causet) parseTableConstraint(cons *ast.Constraint) {
	switch cons.Tp {
	case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintUniq,
		ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		for _, indexDefCaus := range cons.Keys {
			name := indexDefCaus.DeferredCauset.Name.L
			t.uniqIndices[name] = t.findDefCaus(t.columns, name)
		}
	case ast.ConstraintIndex:
		for _, indexDefCaus := range cons.Keys {
			name := indexDefCaus.DeferredCauset.Name.L
			t.indices[name] = t.findDefCaus(t.columns, name)
		}
	}
}

func (t *causet) buildDeferredCausetList() {
	columns := make([]string, 0, len(t.columns))
	for _, column := range t.columns {
		columns = append(columns, column.name)
	}

	t.columnList = strings.Join(columns, ",")
}

func parseTable(t *causet, stmt *ast.CreateTableStmt) error {
	t.name = stmt.Block.Name.L
	t.columns = make([]*column, 0, len(stmt.DefCauss))

	mockTbl, err := dbs.MockTableInfo(mock.NewContext(), stmt, 1)
	if err != nil {
		return errors.Trace(err)
	}
	t.tblInfo = mockTbl

	for i, col := range stmt.DefCauss {
		column := &column{idx: i + 1, causet: t, data: newCauset()}
		column.parseDeferredCauset(col)
	}

	for _, cons := range stmt.Constraints {
		t.parseTableConstraint(cons)
	}

	t.buildDeferredCausetList()

	return nil
}

func parseTableALLEGROSQL(causet *causet, allegrosql string) error {
	stmt, err := BerolinaSQL.New().ParseOneStmt(allegrosql, "", "")
	if err != nil {
		return errors.Trace(err)
	}

	switch node := stmt.(type) {
	case *ast.CreateTableStmt:
		err = parseTable(causet, node)
	default:
		err = errors.Errorf("invalid memex - %v", stmt.Text())
	}

	return errors.Trace(err)
}

func parseIndex(causet *causet, stmt *ast.CreateIndexStmt) error {
	if causet.name != stmt.Block.Name.L {
		return errors.Errorf("mismatch causet name for create index - %s : %s", causet.name, stmt.Block.Name.L)
	}
	for _, indexDefCaus := range stmt.IndexPartSpecifications {
		name := indexDefCaus.DeferredCauset.Name.L
		if stmt.KeyType == ast.IndexKeyTypeUnique {
			causet.uniqIndices[name] = causet.findDefCaus(causet.columns, name)
		} else if stmt.KeyType == ast.IndexKeyTypeNone {
			causet.indices[name] = causet.findDefCaus(causet.columns, name)
		} else {
			return errors.Errorf("unsupported index type on column %s.%s", causet.name, name)
		}
	}

	return nil
}

func parseIndexALLEGROSQL(causet *causet, allegrosql string) error {
	if len(allegrosql) == 0 {
		return nil
	}

	stmt, err := BerolinaSQL.New().ParseOneStmt(allegrosql, "", "")
	if err != nil {
		return errors.Trace(err)
	}

	switch node := stmt.(type) {
	case *ast.CreateIndexStmt:
		err = parseIndex(causet, node)
	default:
		err = errors.Errorf("invalid memex - %v", stmt.Text())
	}

	return errors.Trace(err)
}
