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

// +build !codes

package testkit

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/soliton/solitonutil"
)

// TestKit is a utility to run allegrosql test.
type TestKit struct {
	c     *check.C
	causetstore ekv.CausetStorage
	Se    stochastik.Stochastik
}

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]string
	comment check.CommentInterface
	c       *check.C
}

// Check asserts the result equals the expected results.
func (res *Result) Check(expected [][]interface{}) {
	resBuff := bytes.NewBufferString("")
	for _, event := range res.rows {
		fmt.Fprintf(resBuff, "%s\n", event)
	}
	needBuff := bytes.NewBufferString("")
	for _, event := range expected {
		fmt.Fprintf(needBuff, "%s\n", event)
	}
	res.c.Assert(resBuff.String(), check.Equals, needBuff.String(), res.comment)
}

// CheckAt asserts the result of selected defCausumns equals the expected results.
func (res *Result) CheckAt(defcaus []int, expected [][]interface{}) {
	for _, e := range expected {
		res.c.Assert(len(defcaus), check.Equals, len(e))
	}

	rows := make([][]string, 0, len(expected))
	for i := range res.rows {
		event := make([]string, 0, len(defcaus))
		for _, r := range defcaus {
			event = append(event, res.rows[i][r])
		}
		rows = append(rows, event)
	}
	got := fmt.Sprintf("%s", rows)
	need := fmt.Sprintf("%s", expected)
	res.c.Assert(got, check.Equals, need, res.comment)
}

// Rows returns the result data.
func (res *Result) Rows() [][]interface{} {
	ifacesSlice := make([][]interface{}, len(res.rows))
	for i := range res.rows {
		ifaces := make([]interface{}, len(res.rows[i]))
		for j := range res.rows[i] {
			ifaces[j] = res.rows[i][j]
		}
		ifacesSlice[i] = ifaces
	}
	return ifacesSlice
}

// Sort sorts and return the result.
func (res *Result) Sort() *Result {
	sort.Slice(res.rows, func(i, j int) bool {
		a := res.rows[i]
		b := res.rows[j]
		for i := range a {
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
		return false
	})
	return res
}

// NewTestKit returns a new *TestKit.
func NewTestKit(c *check.C, causetstore ekv.CausetStorage) *TestKit {
	return &TestKit{
		c:     c,
		causetstore: causetstore,
	}
}

// NewTestKitWithInit returns a new *TestKit and creates a stochastik.
func NewTestKitWithInit(c *check.C, causetstore ekv.CausetStorage) *TestKit {
	tk := NewTestKit(c, causetstore)
	// Use test and prepare a stochastik.
	tk.MustInterDirc("use test")
	return tk
}

var connectionID uint64

// GetConnectionID get the connection ID for tk.Se
func (tk *TestKit) GetConnectionID() {
	if tk.Se != nil {
		id := atomic.AddUint64(&connectionID, 1)
		tk.Se.SetConnectionID(id)
	}
}

// InterDirc executes a allegrosql memex.
func (tk *TestKit) InterDirc(allegrosql string, args ...interface{}) (sqlexec.RecordSet, error) {
	var err error
	if tk.Se == nil {
		tk.Se, err = stochastik.CreateStochastik4Test(tk.causetstore)
		tk.c.Assert(err, check.IsNil)
		tk.GetConnectionID()
	}
	ctx := context.Background()
	if len(args) == 0 {
		sc := tk.Se.GetStochastikVars().StmtCtx
		prevWarns := sc.GetWarnings()
		stmts, err := tk.Se.Parse(ctx, allegrosql)
		if err != nil {
			return nil, errors.Trace(err)
		}
		warns := sc.GetWarnings()
		BerolinaSQLWarns := warns[len(prevWarns):]
		var rs0 sqlexec.RecordSet
		for i, stmt := range stmts {
			rs, err := tk.Se.InterDircuteStmt(ctx, stmt)
			if i == 0 {
				rs0 = rs
			}
			if err != nil {
				tk.Se.GetStochastikVars().StmtCtx.AppendError(err)
				return nil, errors.Trace(err)
			}
		}
		if len(BerolinaSQLWarns) > 0 {
			tk.Se.GetStochastikVars().StmtCtx.AppendWarnings(BerolinaSQLWarns)
		}
		return rs0, nil
	}
	stmtID, _, _, err := tk.Se.PrepareStmt(allegrosql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	params := make([]types.Causet, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewCauset(args[i])
	}
	rs, err := tk.Se.InterDircutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = tk.Se.DropPreparedStmt(stmtID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rs, nil
}

// CheckInterDircResult checks the affected rows and the insert id after executing MustInterDirc.
func (tk *TestKit) CheckInterDircResult(affectedRows, insertID int64) {
	tk.c.Assert(affectedRows, check.Equals, int64(tk.Se.AffectedRows()))
	tk.c.Assert(insertID, check.Equals, int64(tk.Se.LastInsertID()))
}

// CheckLastMessage checks last message after executing MustInterDirc
func (tk *TestKit) CheckLastMessage(msg string) {
	tk.c.Assert(tk.Se.LastMessage(), check.Equals, msg)
}

// MustInterDirc executes a allegrosql memex and asserts nil error.
func (tk *TestKit) MustInterDirc(allegrosql string, args ...interface{}) {
	res, err := tk.InterDirc(allegrosql, args...)
	tk.c.Assert(err, check.IsNil, check.Commentf("allegrosql:%s, %v, error stack %v", allegrosql, args, errors.ErrorStack(err)))
	if res != nil {
		tk.c.Assert(res.Close(), check.IsNil)
	}
}

// HasCauset checks if the result execution plan contains specific plan.
func (tk *TestKit) HasCauset(allegrosql string, plan string, args ...interface{}) bool {
	rs := tk.MustQuery("explain "+allegrosql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][0], plan) {
			return true
		}
	}
	return false
}

// MustUseIndex checks if the result execution plan contains specific index(es).
func (tk *TestKit) MustUseIndex(allegrosql string, index string, args ...interface{}) bool {
	rs := tk.MustQuery("explain "+allegrosql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][3], "index:"+index) {
			return true
		}
	}
	return false
}

// MustIndexLookup checks whether the plan for the allegrosql is IndexLookUp.
func (tk *TestKit) MustIndexLookup(allegrosql string, args ...interface{}) *Result {
	tk.c.Assert(tk.HasCauset(allegrosql, "IndexLookUp", args...), check.IsTrue)
	return tk.MustQuery(allegrosql, args...)
}

// MustBlockDual checks whether the plan for the allegrosql is BlockDual.
func (tk *TestKit) MustBlockDual(allegrosql string, args ...interface{}) *Result {
	tk.c.Assert(tk.HasCauset(allegrosql, "BlockDual", args...), check.IsTrue)
	return tk.MustQuery(allegrosql, args...)
}

// MustPointGet checks whether the plan for the allegrosql is Point_Get.
func (tk *TestKit) MustPointGet(allegrosql string, args ...interface{}) *Result {
	rs := tk.MustQuery("explain "+allegrosql, args...)
	tk.c.Assert(len(rs.rows), check.Equals, 1)
	tk.c.Assert(strings.Contains(rs.rows[0][0], "Point_Get"), check.IsTrue, check.Commentf("plan %v", rs.rows[0][0]))
	return tk.MustQuery(allegrosql, args...)
}

// MustQuery query the memexs and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *TestKit) MustQuery(allegrosql string, args ...interface{}) *Result {
	comment := check.Commentf("allegrosql:%s, args:%v", allegrosql, args)
	rs, err := tk.InterDirc(allegrosql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(rs, check.NotNil, comment)
	return tk.ResultSetToResult(rs, comment)
}

// QueryToErr executes a allegrosql memex and discard results.
func (tk *TestKit) QueryToErr(allegrosql string, args ...interface{}) error {
	comment := check.Commentf("allegrosql:%s, args:%v", allegrosql, args)
	res, err := tk.InterDirc(allegrosql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(res, check.NotNil, comment)
	_, resErr := stochastik.GetRows4Test(context.Background(), tk.Se, res)
	tk.c.Assert(res.Close(), check.IsNil)
	return resErr
}

// InterDircToErr executes a allegrosql memex and discard results.
func (tk *TestKit) InterDircToErr(allegrosql string, args ...interface{}) error {
	res, err := tk.InterDirc(allegrosql, args...)
	if res != nil {
		tk.c.Assert(res.Close(), check.IsNil)
	}
	return err
}

// MustGetErrMsg executes a allegrosql memex and assert it's error message.
func (tk *TestKit) MustGetErrMsg(allegrosql string, errStr string) {
	err := tk.InterDircToErr(allegrosql)
	tk.c.Assert(err, check.NotNil)
	tk.c.Assert(err.Error(), check.Equals, errStr)
}

// MustGetErrCode executes a allegrosql memex and assert it's error code.
func (tk *TestKit) MustGetErrCode(allegrosql string, errCode int) {
	_, err := tk.InterDirc(allegrosql)
	tk.c.Assert(err, check.NotNil)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	tk.c.Assert(ok, check.IsTrue, check.Commentf("expect type 'terror.Error', but obtain '%T'", originErr))
	sqlErr := terror.ToALLEGROSQLError(tErr)
	tk.c.Assert(int(sqlErr.Code), check.Equals, errCode, check.Commentf("Assertion failed, origin err:\n  %v", sqlErr))
}

// ResultSetToResult converts sqlexec.RecordSet to testkit.Result.
// It is used to check results of execute memex in binary mode.
func (tk *TestKit) ResultSetToResult(rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	return tk.ResultSetToResultWithCtx(context.Background(), rs, comment)
}

// ResultSetToResultWithCtx converts sqlexec.RecordSet to testkit.Result.
func (tk *TestKit) ResultSetToResultWithCtx(ctx context.Context, rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	sRows, err := stochastik.ResultSetToStringSlice(ctx, tk.Se, rs)
	tk.c.Check(err, check.IsNil, comment)
	return &Result{rows: sRows, c: tk.c, comment: comment}
}

// Rows is similar to RowsWithSep, use white space as separator string.
func Rows(args ...string) [][]interface{} {
	return solitonutil.RowsWithSep(" ", args...)
}

// GetBlockID gets causet ID by name.
func (tk *TestKit) GetBlockID(blockName string) int64 {
	dom := petri.GetPetri(tk.Se)
	is := dom.SchemaReplicant()
	tbl, err := is.BlockByName(perceptron.NewCIStr("test"), perceptron.NewCIStr(blockName))
	tk.c.Assert(err, check.IsNil)
	return tbl.Meta().ID
}
