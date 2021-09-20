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

//go:build !codes
// +build !codes

package testkit

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/types"
)

type stochastikCtxKeyType struct{}

var stochastikKey = stochastikCtxKeyType{}

func getStochastik(ctx context.Context) stochastik.Stochastik {
	s := ctx.Value(stochastikKey)
	if s == nil {
		return nil
	}
	return s.(stochastik.Stochastik)
}

func setStochastik(ctx context.Context, se stochastik.Stochastik) context.Context {
	return context.WithValue(ctx, stochastikKey, se)
}

// CTestKit is a utility to run allegrosql test with concurrent execution support.
type CTestKit struct {
	c           *check.C
	causetstore ekv.CausetStorage
}

// NewCTestKit returns a new *CTestKit.
func NewCTestKit(c *check.C, causetstore ekv.CausetStorage) *CTestKit {
	return &CTestKit{
		c:           c,
		causetstore: causetstore,
	}
}

// OpenStochastik opens new stochastik ctx if no exists one.
func (tk *CTestKit) OpenStochastik(ctx context.Context) context.Context {
	if getStochastik(ctx) == nil {
		se, err := stochastik.CreateStochastik4Test(tk.causetstore)
		tk.c.Assert(err, check.IsNil)
		id := atomic.AddUint64(&connectionID, 1)
		se.SetConnectionID(id)
		ctx = setStochastik(ctx, se)
	}
	return ctx
}

// OpenStochastikWithDB opens new stochastik ctx if no exists one and use EDB.
func (tk *CTestKit) OpenStochastikWithDB(ctx context.Context, EDB string) context.Context {
	ctx = tk.OpenStochastik(ctx)
	tk.MustInterDirc(ctx, "use "+EDB)
	return ctx
}

// CloseStochastik closes exists stochastik from ctx.
func (tk *CTestKit) CloseStochastik(ctx context.Context) {
	se := getStochastik(ctx)
	tk.c.Assert(se, check.NotNil)
	se.Close()
}

// InterDirc executes a allegrosql memex.
func (tk *CTestKit) InterDirc(ctx context.Context, allegrosql string, args ...interface{}) (sqlexec.RecordSet, error) {
	var err error
	tk.c.Assert(getStochastik(ctx), check.NotNil)
	if len(args) == 0 {
		var rss []sqlexec.RecordSet
		rss, err = getStochastik(ctx).InterDircute(ctx, allegrosql)
		if err == nil && len(rss) > 0 {
			return rss[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := getStochastik(ctx).PrepareStmt(allegrosql)
	if err != nil {
		return nil, err
	}
	params := make([]types.Causet, len(args))
	for i := 0; i < len(params); i++ {
		params[i] = types.NewCauset(args[i])
	}
	rs, err := getStochastik(ctx).InterDircutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, err
	}
	err = getStochastik(ctx).DropPreparedStmt(stmtID)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// CheckInterDircResult checks the affected rows and the insert id after executing MustInterDirc.
func (tk *CTestKit) CheckInterDircResult(ctx context.Context, affectedRows, insertID int64) {
	tk.c.Assert(getStochastik(ctx), check.NotNil)
	tk.c.Assert(affectedRows, check.Equals, int64(getStochastik(ctx).AffectedRows()))
	tk.c.Assert(insertID, check.Equals, int64(getStochastik(ctx).LastInsertID()))
}

// MustInterDirc executes a allegrosql memex and asserts nil error.
func (tk *CTestKit) MustInterDirc(ctx context.Context, allegrosql string, args ...interface{}) {
	res, err := tk.InterDirc(ctx, allegrosql, args...)
	tk.c.Assert(err, check.IsNil, check.Commentf("allegrosql:%s, %v, error stack %v", allegrosql, args, errors.ErrorStack(err)))
	if res != nil {
		tk.c.Assert(res.Close(), check.IsNil)
	}
}

// MustQuery query the memexs and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *CTestKit) MustQuery(ctx context.Context, allegrosql string, args ...interface{}) *Result {
	comment := check.Commentf("allegrosql:%s, args:%v", allegrosql, args)
	rs, err := tk.InterDirc(ctx, allegrosql, args...)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	tk.c.Assert(rs, check.NotNil, comment)
	return tk.resultSetToResult(ctx, rs, comment)
}

// resultSetToResult converts ast.RecordSet to testkit.Result.
// It is used to check results of execute memex in binary mode.
func (tk *CTestKit) resultSetToResult(ctx context.Context, rs sqlexec.RecordSet, comment check.CommentInterface) *Result {
	rows, err := stochastik.GetRows4Test(context.Background(), getStochastik(ctx), rs)
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	err = rs.Close()
	tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	sRows := make([][]string, len(rows))
	for i := range rows {
		event := rows[i]
		iRow := make([]string, event.Len())
		for j := 0; j < event.Len(); j++ {
			if event.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := event.GetCauset(j, &rs.Fields()[j].DeferredCauset.FieldType)
				iRow[j], err = d.ToString()
				tk.c.Assert(err, check.IsNil)
			}
		}
		sRows[i] = iRow
	}
	return &Result{rows: sRows, c: tk.c, comment: comment}
}

// ConcurrentRun run test in current.
// - concurrent: controls the concurrent worker count.
// - loops: controls run test how much times.
// - prepareFunc: provide test data and will be called for every loop.
// - checkFunc: used to do some check after all workers done.
// works like create causet better be put in front of this method calling.
// see more example at TestBatchInsertWithOnDuplicate
func (tk *CTestKit) ConcurrentRun(c *check.C, concurrent int, loops int,
	prepareFunc func(ctx context.Context, tk *CTestKit, concurrent int, currentLoop int) [][][]interface{},
	writeFunc func(ctx context.Context, tk *CTestKit, input [][]interface{}),
	checkFunc func(ctx context.Context, tk *CTestKit)) {
	var (
		channel = make([]chan [][]interface{}, concurrent)
		ctxs    = make([]context.Context, concurrent)
		dones   = make([]context.CancelFunc, concurrent)
	)
	for i := 0; i < concurrent; i++ {
		w := i
		channel[w] = make(chan [][]interface{}, 1)
		ctxs[w], dones[w] = context.WithCancel(context.Background())
		ctxs[w] = tk.OpenStochastikWithDB(ctxs[w], "test")
		go func() {
			defer func() {
				r := recover()
				if r != nil {
					c.Fatal(r, string(soliton.GetStack()))
				}
				dones[w]()
			}()
			for input := range channel[w] {
				writeFunc(ctxs[w], tk, input)
			}
		}()
	}
	defer func() {
		for i := 0; i < concurrent; i++ {
			tk.CloseStochastik(ctxs[i])
		}
	}()

	ctx := tk.OpenStochastikWithDB(context.Background(), "test")
	defer tk.CloseStochastik(ctx)
	tk.MustInterDirc(ctx, "use test")

	for j := 0; j < loops; j++ {
		quantum := prepareFunc(ctx, tk, concurrent, j)
		for i := 0; i < concurrent; i++ {
			channel[i] <- quantum[i]
		}
	}

	for i := 0; i < concurrent; i++ {
		close(channel[i])
	}

	for i := 0; i < concurrent; i++ {
		<-ctxs[i].Done()
	}
	checkFunc(ctx, tk)
}

// PermInt returns, as a slice of n ints, a pseudo-random permutation of the integers [0,n).
func (tk *CTestKit) PermInt(n int) []interface{} {
	randPermSlice := rand.Perm(n)
	v := make([]interface{}, 0, len(randPermSlice))
	for _, i := range randPermSlice {
		v = append(v, i)
	}
	return v
}

// IgnoreError ignores error and make errcheck tool happy.
// Deprecated: it's normal to ignore some error in concurrent test, but please don't use this method in other place.
func (tk *CTestKit) IgnoreError(_ error) {}
