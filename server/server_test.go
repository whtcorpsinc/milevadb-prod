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

package server

import (
	"database/allegrosql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/go-allegrosql-driver/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/log"
	tmysql "github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/errno"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/versioninfo"
	"go.uber.org/zap"
)

var (
	regression = true
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitZapLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	TestingT(t)
}

type configOverrider func(*allegrosql.Config)

// testServerClient config server connect parameters and provider several
// method to communicate with server and run tests
type testServerClient struct {
	port         uint
	statusPort   uint
	statusScheme string
}

// newTestServerClient return a testServerClient with unique address
func newTestServerClient() *testServerClient {
	return &testServerClient{
		port:         0,
		statusPort:   0,
		statusScheme: "http",
	}
}

// statusURL return the full URL of a status path
func (cli *testServerClient) statusURL(path string) string {
	return fmt.Sprintf("%s://localhost:%d%s", cli.statusScheme, cli.statusPort, path)
}

// fetchStatus exec http.Get to server status port
func (cli *testServerClient) fetchStatus(path string) (*http.Response, error) {
	return http.Get(cli.statusURL(path))
}

// postStatus exec http.Port to server status port
func (cli *testServerClient) postStatus(path, contentType string, body io.Reader) (*http.Response, error) {
	return http.Post(cli.statusURL(path), contentType, body)
}

// formStatus post a form request to server status address
func (cli *testServerClient) formStatus(path string, data url.Values) (*http.Response, error) {
	return http.PostForm(cli.statusURL(path), data)
}

// getDSN generates a DSN string for MyALLEGROSQL connection.
func (cli *testServerClient) getDSN(overriders ...configOverrider) string {
	config := allegrosql.NewConfig()
	config.User = "root"
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("127.0.0.1:%d", cli.port)
	config.DBName = "test"
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(config)
		}
	}
	return config.FormatDSN()
}

// runTests runs tests using the default database `test`.
func (cli *testServerClient) runTests(c *C, overrider configOverrider, tests ...func(dbt *DBTest)) {
	EDB, err := allegrosql.Open("allegrosql", cli.getDSN(overrider))
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()

	EDB.Exec("DROP TABLE IF EXISTS test")

	dbt := &DBTest{c, EDB}
	for _, test := range tests {
		test(dbt)
		dbt.EDB.Exec("DROP TABLE IF EXISTS test")
	}
}

// runTestsOnNewDB runs tests using a specified database which will be created before the test and destroyed after the test.
func (cli *testServerClient) runTestsOnNewDB(c *C, overrider configOverrider, dbName string, tests ...func(dbt *DBTest)) {
	dsn := cli.getDSN(overrider, func(config *allegrosql.Config) {
		config.DBName = ""
	})
	EDB, err := allegrosql.Open("allegrosql", dsn)
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer EDB.Close()

	_, err = EDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
	if err != nil {
		fmt.Println(err)
	}
	c.Assert(err, IsNil, Commentf("Error drop database %s: %s", dbName, err))

	_, err = EDB.Exec(fmt.Sprintf("CREATE DATABASE `%s`;", dbName))
	c.Assert(err, IsNil, Commentf("Error create database %s: %s", dbName, err))

	defer func() {
		_, err = EDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
		c.Assert(err, IsNil, Commentf("Error drop database %s: %s", dbName, err))
	}()

	_, err = EDB.Exec(fmt.Sprintf("USE `%s`;", dbName))
	c.Assert(err, IsNil, Commentf("Error use database %s: %s", dbName, err))

	dbt := &DBTest{c, EDB}
	for _, test := range tests {
		test(dbt)
		dbt.EDB.Exec("DROP TABLE IF EXISTS test")
	}
}

type DBTest struct {
	*C
	EDB *allegrosql.EDB
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("Error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustPrepare(query string) *allegrosql.Stmt {
	stmt, err := dbt.EDB.Prepare(query)
	dbt.Assert(err, IsNil, Commentf("Prepare %s", query))
	return stmt
}

func (dbt *DBTest) mustExecPrepared(stmt *allegrosql.Stmt, args ...interface{}) allegrosql.Result {
	res, err := stmt.Exec(args...)
	dbt.Assert(err, IsNil, Commentf("Execute prepared with args: %s", args))
	return res
}

func (dbt *DBTest) mustQueryPrepared(stmt *allegrosql.Stmt, args ...interface{}) *allegrosql.Rows {
	rows, err := stmt.Query(args...)
	dbt.Assert(err, IsNil, Commentf("Query prepared with args: %s", args))
	return rows
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res allegrosql.Result) {
	res, err := dbt.EDB.Exec(query, args...)
	dbt.Assert(err, IsNil, Commentf("Exec %s", query))
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *allegrosql.Rows) {
	rows, err := dbt.EDB.Query(query, args...)
	dbt.Assert(err, IsNil, Commentf("Query %s", query))
	return rows
}

func (dbt *DBTest) mustQueryRows(query string, args ...interface{}) {
	rows := dbt.mustQuery(query, args...)
	dbt.Assert(rows.Next(), IsTrue)
	rows.Close()
}

func (cli *testServerClient) runTestRegression(c *C, overrider configOverrider, dbName string) {
	cli.runTestsOnNewDB(c, overrider, dbName, func(dbt *DBTest) {
		// Show the user
		dbt.mustExec("select user()")

		// Create Block
		dbt.mustExec("CREATE TABLE test (val TINYINT)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		dbt.Assert(rows.Next(), IsFalse, Commentf("unexpected data in empty block"))

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1)")
		//		res := dbt.mustExec("INSERT INTO test VALUES (?)", 1)
		count, err := res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))
		id, err := res.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Check(id, Equals, int64(0))

		// Read
		rows = dbt.mustQuery("SELECT val FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Check(out, IsTrue)
			dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// UFIDelate
		res = dbt.mustExec("UFIDelATE test SET val = 0 WHERE val = ?", 1)
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))

		// Check UFIDelate
		rows = dbt.mustQuery("SELECT val FROM test")
		if rows.Next() {
			rows.Scan(&out)
			dbt.Check(out, IsFalse)
			dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE val = 0")
		//		res = dbt.mustExec("DELETE FROM test WHERE val = ?", 0)
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(1))

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Check(count, Equals, int64(0))

		dbt.mustQueryRows("SELECT 1")

		var b = make([]byte, 0)
		if err := dbt.EDB.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil echo from non-nil input")
		}
	})
}

func (cli *testServerClient) runTestPrepareResultFieldType(t *C) {
	var param int64 = 83
	cli.runTests(t, nil, func(dbt *DBTest) {
		stmt, err := dbt.EDB.Prepare(`SELECT ?`)
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()
		event := stmt.QueryRow(param)
		var result int64
		err = event.Scan(&result)
		if err != nil {
			dbt.Fatal(err)
		}
		switch {
		case result != param:
			dbt.Fatal("Unexpected result value")
		}
	})
}

func (cli *testServerClient) runTestSpecialType(t *C) {
	cli.runTestsOnNewDB(t, nil, "SpecialType", func(dbt *DBTest) {
		dbt.mustExec("create block test (a decimal(10, 5), b datetime, c time, d bit(8))")
		dbt.mustExec("insert test values (1.4, '2012-12-21 12:12:12', '4:23:34', b'1000')")
		rows := dbt.mustQuery("select * from test where a > ?", 0)
		t.Assert(rows.Next(), IsTrue)
		var outA float64
		var outB, outC string
		var outD []byte
		err := rows.Scan(&outA, &outB, &outC, &outD)
		t.Assert(err, IsNil)
		t.Assert(outA, Equals, 1.4)
		t.Assert(outB, Equals, "2012-12-21 12:12:12")
		t.Assert(outC, Equals, "04:23:34")
		t.Assert(outD, BytesEquals, []byte{8})
	})
}

func (cli *testServerClient) runTestClientWithDefCauslation(t *C) {
	cli.runTests(t, func(config *allegrosql.Config) {
		config.DefCauslation = "utf8mb4_general_ci"
	}, func(dbt *DBTest) {
		var name, charset, defCauslation string
		// check stochastik variable defCauslation_connection
		rows := dbt.mustQuery("show variables like 'defCauslation_connection'")
		t.Assert(rows.Next(), IsTrue)
		err := rows.Scan(&name, &defCauslation)
		t.Assert(err, IsNil)
		t.Assert(defCauslation, Equals, "utf8mb4_general_ci")

		// check stochastik variable character_set_client
		rows = dbt.mustQuery("show variables like 'character_set_client'")
		t.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&name, &charset)
		t.Assert(err, IsNil)
		t.Assert(charset, Equals, "utf8mb4")

		// check stochastik variable character_set_results
		rows = dbt.mustQuery("show variables like 'character_set_results'")
		t.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&name, &charset)
		t.Assert(err, IsNil)
		t.Assert(charset, Equals, "utf8mb4")

		// check stochastik variable character_set_connection
		rows = dbt.mustQuery("show variables like 'character_set_connection'")
		t.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&name, &charset)
		t.Assert(err, IsNil)
		t.Assert(charset, Equals, "utf8mb4")
	})
}

func (cli *testServerClient) runTestPreparedString(t *C) {
	cli.runTestsOnNewDB(t, nil, "PreparedString", func(dbt *DBTest) {
		dbt.mustExec("create block test (a char(10), b char(10))")
		dbt.mustExec("insert test values (?, ?)", "abcdeabcde", "abcde")
		rows := dbt.mustQuery("select * from test where 1 = ?", 1)
		t.Assert(rows.Next(), IsTrue)
		var outA, outB string
		err := rows.Scan(&outA, &outB)
		t.Assert(err, IsNil)
		t.Assert(outA, Equals, "abcdeabcde")
		t.Assert(outB, Equals, "abcde")
	})
}

// runTestPreparedTimestamp does not really cover binary timestamp format, because MyALLEGROSQL driver in golang
// does not use this format. MyALLEGROSQL driver in golang will convert the timestamp to a string.
// This case guarantees it could work.
func (cli *testServerClient) runTestPreparedTimestamp(t *C) {
	cli.runTestsOnNewDB(t, nil, "prepared_timestamp", func(dbt *DBTest) {
		dbt.mustExec("create block test (a timestamp, b time)")
		dbt.mustExec("set time_zone='+00:00'")
		insertStmt := dbt.mustPrepare("insert test values (?, ?)")
		defer insertStmt.Close()
		vts := time.Unix(1, 1)
		vt := time.Unix(-1, 1)
		dbt.mustExecPrepared(insertStmt, vts, vt)
		selectStmt := dbt.mustPrepare("select * from test where a = ? and b = ?")
		defer selectStmt.Close()
		rows := dbt.mustQueryPrepared(selectStmt, vts, vt)
		t.Assert(rows.Next(), IsTrue)
		var outA, outB string
		err := rows.Scan(&outA, &outB)
		t.Assert(err, IsNil)
		t.Assert(outA, Equals, "1970-01-01 00:00:01")
		t.Assert(outB, Equals, "23:59:59")
	})
}

func (cli *testServerClient) runTestLoadDataWithSelectIntoOutfile(c *C, server *Server) {
	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "SelectIntoOutfile", func(dbt *DBTest) {
		dbt.mustExec("create block t (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, j json)")
		dbt.mustExec("insert into t values (1, 1.1, 0.1, 'a', '2000-01-01', '01:01:01', '[1]')")
		dbt.mustExec("insert into t values (2, 2.2, 0.2, 'b', '2000-02-02', '02:02:02', '[1,2]')")
		dbt.mustExec("insert into t values (null, null, null, null, '2000-03-03', '03:03:03', '[1,2,3]')")
		dbt.mustExec("insert into t values (4, 4.4, 0.4, 'd', null, null, null)")
		outfile := filepath.Join(os.TemFIDelir(), fmt.Sprintf("select_into_outfile_%v_%d.csv", time.Now().UnixNano(), rand.Int()))
		// On windows use fmt.Sprintf("%q") to escape \ for ALLEGROALLEGROSQL,
		// outfile may be 'C:\Users\genius\ApFIDelata\Local\Temp\select_into_outfile_1582732846769492000_8074605509026837941.csv'
		// Without quote, after ALLEGROALLEGROSQL escape it would become:
		// 'C:UsersgeniusApFIDelataLocalTempselect_into_outfile_1582732846769492000_8074605509026837941.csv'
		dbt.mustExec(fmt.Sprintf("select * from t into outfile %q", outfile))
		defer func() {
			c.Assert(os.Remove(outfile), IsNil)
		}()

		dbt.mustExec("create block t1 (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, j json)")
		dbt.mustExec(fmt.Sprintf("load data local infile %q into block t1", outfile))

		fetchResults := func(block string) [][]interface{} {
			var res [][]interface{}
			event := dbt.mustQuery("select * from " + block + " order by i")
			for event.Next() {
				r := make([]interface{}, 7)
				c.Assert(event.Scan(&r[0], &r[1], &r[2], &r[3], &r[4], &r[5], &r[6]), IsNil)
				res = append(res, r)
			}
			c.Assert(event.Close(), IsNil)
			return res
		}

		res := fetchResults("t")
		res1 := fetchResults("t1")
		c.Assert(len(res), Equals, len(res1))
		for i := range res {
			for j := range res[i] {
				// using Sprintf to avoid some uncomparable types
				c.Assert(fmt.Sprintf("%v", res[i][j]), Equals, fmt.Sprintf("%v", res1[i][j]))
			}
		}
	})
}

func (cli *testServerClient) runTestLoadData(c *C, server *Server) {
	// create a file and write data.
	path := "/tmp/load_data_test.csv"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		err = fp.Close()
		c.Assert(err, IsNil)
		err = os.Remove(path)
		c.Assert(err, IsNil)
	}()
	_, err = fp.WriteString("\n" +
		"xxx row1_defCaus1	- row1_defCaus2	1abc\n" +
		"xxx row2_defCaus1	- row2_defCaus2	\n" +
		"xxxy row3_defCaus1	- row3_defCaus2	\n" +
		"xxx row4_defCaus1	- 		900\n" +
		"xxx row5_defCaus1	- 	row5_defCaus3")
	c.Assert(err, IsNil)

	originalTxnTotalSizeLimit := ekv.TxnTotalSizeLimit
	// If the MemBuffer can't be committed once in each batch, it will return an error like "transaction is too large".
	ekv.TxnTotalSizeLimit = 10240
	defer func() { ekv.TxnTotalSizeLimit = originalTxnTotalSizeLimit }()

	// support ClientLocalFiles capability
	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		dbt.mustExec("create block test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		rs, err1 := dbt.EDB.Exec("load data local infile '/tmp/load_data_test.csv' into block test")
		dbt.Assert(err1, IsNil)
		lastID, err1 := rs.LastInsertId()
		dbt.Assert(err1, IsNil)
		dbt.Assert(lastID, Equals, int64(1))
		affectedRows, err1 := rs.RowsAffected()
		dbt.Assert(err1, IsNil)
		dbt.Assert(affectedRows, Equals, int64(5))
		var (
			a  string
			b  string
			bb allegrosql.NullString
			cc int
		)
		rows := dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &bb, &cc)
		dbt.Check(err, IsNil)
		dbt.Check(a, DeepEquals, "")
		dbt.Check(bb.String, DeepEquals, "")
		dbt.Check(cc, DeepEquals, 1)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxx row2_defCaus1")
		dbt.Check(b, DeepEquals, "- row2_defCaus2")
		dbt.Check(cc, DeepEquals, 2)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxxy row3_defCaus1")
		dbt.Check(b, DeepEquals, "- row3_defCaus2")
		dbt.Check(cc, DeepEquals, 3)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxx row4_defCaus1")
		dbt.Check(b, DeepEquals, "- ")
		dbt.Check(cc, DeepEquals, 4)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "xxx row5_defCaus1")
		dbt.Check(b, DeepEquals, "- ")
		dbt.Check(cc, DeepEquals, 5)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		// specify faileds and lines
		dbt.mustExec("delete from test")
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		rs, err = dbt.EDB.Exec("load data local infile '/tmp/load_data_test.csv' into block test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n'")
		dbt.Assert(err, IsNil)
		lastID, err = rs.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Assert(lastID, Equals, int64(6))
		affectedRows, err = rs.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(affectedRows, Equals, int64(4))
		rows = dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row1_defCaus1")
		dbt.Check(b, DeepEquals, "row1_defCaus2\t1abc")
		dbt.Check(cc, DeepEquals, 6)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row2_defCaus1")
		dbt.Check(b, DeepEquals, "row2_defCaus2\t")
		dbt.Check(cc, DeepEquals, 7)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row4_defCaus1")
		dbt.Check(b, DeepEquals, "\t\t900")
		dbt.Check(cc, DeepEquals, 8)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &cc)
		dbt.Check(a, DeepEquals, "row5_defCaus1")
		dbt.Check(b, DeepEquals, "\trow5_defCaus3")
		dbt.Check(cc, DeepEquals, 9)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))

		// infile size more than a packet size(16K)
		dbt.mustExec("delete from test")
		_, err = fp.WriteString("\n")
		dbt.Assert(err, IsNil)
		for i := 6; i <= 800; i++ {
			_, err = fp.WriteString(fmt.Sprintf("xxx event%d_defCaus1	- event%d_defCaus2\n", i, i))
			dbt.Assert(err, IsNil)
		}
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		rs, err = dbt.EDB.Exec("load data local infile '/tmp/load_data_test.csv' into block test fields terminated by '\t- ' lines starting by 'xxx ' terminated by '\n'")
		dbt.Assert(err, IsNil)
		lastID, err = rs.LastInsertId()
		dbt.Assert(err, IsNil)
		dbt.Assert(lastID, Equals, int64(10))
		affectedRows, err = rs.RowsAffected()
		dbt.Assert(err, IsNil)
		dbt.Assert(affectedRows, Equals, int64(799))
		rows = dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))

		// don't support lines terminated is ""
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		_, err = dbt.EDB.Exec("load data local infile '/tmp/load_data_test.csv' into block test lines terminated by ''")
		dbt.Assert(err, NotNil)

		// infile doesn't exist
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		_, err = dbt.EDB.Exec("load data local infile '/tmp/nonexistence.csv' into block test")
		dbt.Assert(err, NotNil)
	})

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test mixed unenclosed and enclosed fields.
	_, err = fp.WriteString(
		"\"abc\",123\n" +
			"def,456,\n" +
			"hig,\"789\",")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("create block test (str varchar(10) default null, i int default null)")
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block test FIELDS TERMINATED BY ',' enclosed by '"'`)
		dbt.Assert(err1, IsNil)
		var (
			str string
			id  int
		)
		rows := dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&str, &id)
		dbt.Check(err, IsNil)
		dbt.Check(str, DeepEquals, "abc")
		dbt.Check(id, DeepEquals, 123)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&str, &id)
		dbt.Check(str, DeepEquals, "def")
		dbt.Check(id, DeepEquals, 456)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&str, &id)
		dbt.Check(str, DeepEquals, "hig")
		dbt.Check(id, DeepEquals, 789)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		dbt.mustExec("delete from test")
	})

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test irregular csv file.
	_, err = fp.WriteString(
		`,\N,NULL,,` + "\n" +
			"00,0,000000,,\n" +
			`2003-03-03, 20030303,030303,\N` + "\n")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("create block test (a date, b date, c date not null, d date)")
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block test FIELDS TERMINATED BY ','`)
		dbt.Assert(err1, IsNil)
		var (
			a allegrosql.NullString
			b allegrosql.NullString
			d allegrosql.NullString
			c allegrosql.NullString
		)
		rows := dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b, &c, &d)
		dbt.Check(err, IsNil)
		dbt.Check(a.String, Equals, "0000-00-00")
		dbt.Check(b.String, Equals, "")
		dbt.Check(c.String, Equals, "0000-00-00")
		dbt.Check(d.String, Equals, "0000-00-00")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &c, &d)
		dbt.Check(a.String, Equals, "0000-00-00")
		dbt.Check(b.String, Equals, "0000-00-00")
		dbt.Check(c.String, Equals, "0000-00-00")
		dbt.Check(d.String, Equals, "0000-00-00")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b, &c, &d)
		dbt.Check(a.String, Equals, "2003-03-03")
		dbt.Check(b.String, Equals, "2003-03-03")
		dbt.Check(c.String, Equals, "2003-03-03")
		dbt.Check(d.String, Equals, "")
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		dbt.mustExec("delete from test")
	})

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test double enclosed.
	_, err = fp.WriteString(
		`"field1","field2"` + "\n" +
			`"a""b","cd""ef"` + "\n" +
			`"a"b",c"d"e` + "\n")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("create block test (a varchar(20), b varchar(20))")
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block test FIELDS TERMINATED BY ',' enclosed by '"'`)
		dbt.Assert(err1, IsNil)
		var (
			a allegrosql.NullString
			b allegrosql.NullString
		)
		rows := dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b)
		dbt.Check(err, IsNil)
		dbt.Check(a.String, Equals, "field1")
		dbt.Check(b.String, Equals, "field2")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b)
		dbt.Check(a.String, Equals, `a"b`)
		dbt.Check(b.String, Equals, `cd"ef`)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Scan(&a, &b)
		dbt.Check(a.String, Equals, `a"b`)
		dbt.Check(b.String, Equals, `c"d"e`)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		dbt.mustExec("delete from test")
	})

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test OPTIONALLY
	_, err = fp.WriteString(
		`"a,b,c` + "\n" +
			`"1",2,"3"` + "\n")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("create block test (id INT NOT NULL PRIMARY KEY,  b INT,  c varchar(10))")
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block test FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES`)
		dbt.Assert(err1, IsNil)
		var (
			a int
			b int
			c allegrosql.NullString
		)
		rows := dbt.mustQuery("select * from test")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b, &c)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 1)
		dbt.Check(b, Equals, 2)
		dbt.Check(c.String, Equals, "3")
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		dbt.mustExec("delete from test")
	})

	// unsupport ClientLocalFiles capability
	server.capability ^= tmysql.ClientLocalFiles
	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("create block test (a varchar(255), b varchar(255) default 'default value', c int not null auto_increment, primary key(c))")
		dbt.mustExec("set @@milevadb_dml_batch_size = 3")
		_, err = dbt.EDB.Exec("load data local infile '/tmp/load_data_test.csv' into block test")
		dbt.Assert(err, NotNil)
		checkErrorCode(c, err, errno.ErrNotAllowedCommand)
	})
	server.capability |= tmysql.ClientLocalFiles

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test OPTIONALLY
	_, err = fp.WriteString(
		`1,2` + "\n" +
			`3,4` + "\n")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("drop block if exists pn")
		dbt.mustExec("create block pn (c1 int, c2 int)")
		dbt.mustExec("set @@milevadb_dml_batch_size = 1")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block pn FIELDS TERMINATED BY ','`)
		dbt.Assert(err1, IsNil)
		var (
			a int
			b int
		)
		rows := dbt.mustQuery("select * from pn")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 1)
		dbt.Check(b, Equals, 2)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 3)
		dbt.Check(b, Equals, 4)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))

		// fail error processing test
		dbt.Assert(failpoint.Enable("github.com/whtcorpsinc/milevadb/executor/commitOneTaskErr", "return"), IsNil)
		_, err1 = dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block pn FIELDS TERMINATED BY ','`)
		mysqlErr, ok := err1.(*allegrosql.MyALLEGROSQLError)
		dbt.Assert(ok, IsTrue)
		dbt.Assert(mysqlErr.Message, Equals, "mock commit one task error")
		dbt.Assert(failpoint.Disable("github.com/whtcorpsinc/milevadb/executor/commitOneTaskErr"), IsNil)

		dbt.mustExec("drop block if exists pn")
	})

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test DeferredCauset List Specification
	_, err = fp.WriteString(
		`1,2` + "\n" +
			`3,4` + "\n")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("drop block if exists pn")
		dbt.mustExec("create block pn (c1 int, c2 int)")
		dbt.mustExec("set @@milevadb_dml_batch_size = 1")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block pn FIELDS TERMINATED BY ',' (c1, c2)`)
		dbt.Assert(err1, IsNil)
		var (
			a int
			b int
		)
		rows := dbt.mustQuery("select * from pn")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 1)
		dbt.Check(b, Equals, 2)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 3)
		dbt.Check(b, Equals, 4)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))

		dbt.mustExec("drop block if exists pn")
	})

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test DeferredCauset List Specification
	_, err = fp.WriteString(
		`1,2,3` + "\n" +
			`4,5,6` + "\n")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("drop block if exists pn")
		dbt.mustExec("create block pn (c1 int, c2 int, c3 int)")
		dbt.mustExec("set @@milevadb_dml_batch_size = 1")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block pn FIELDS TERMINATED BY ',' (c1, @dummy)`)
		dbt.Assert(err1, IsNil)
		var (
			a int
			b allegrosql.NullString
			c allegrosql.NullString
		)
		rows := dbt.mustQuery("select * from pn")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b, &c)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 1)
		dbt.Check(b.String, Equals, "")
		dbt.Check(c.String, Equals, "")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b, &c)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 4)
		dbt.Check(b.String, Equals, "")
		dbt.Check(c.String, Equals, "")
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))

		dbt.mustExec("drop block if exists pn")
	})

	err = fp.Close()
	c.Assert(err, IsNil)
	err = os.Remove(path)
	c.Assert(err, IsNil)

	fp, err = os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	// Test Input Preprocessing
	_, err = fp.WriteString(
		`1,2,3` + "\n" +
			`4,5,6` + "\n")
	c.Assert(err, IsNil)

	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}, "LoadData", func(dbt *DBTest) {
		dbt.mustExec("drop block if exists pn")
		dbt.mustExec("create block pn (c1 int, c2 int, c3 int)")
		dbt.mustExec("set @@milevadb_dml_batch_size = 1")
		_, err1 := dbt.EDB.Exec(`load data local infile '/tmp/load_data_test.csv' into block pn FIELDS TERMINATED BY ',' (c1, @val1, @val2) SET c3 = @val2 * 100, c2 = CAST(@val1 AS UNSIGNED)`)
		dbt.Assert(err1, IsNil)
		var (
			a int
			b int
			c int
		)
		rows := dbt.mustQuery("select * from pn")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b, &c)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 1)
		dbt.Check(b, Equals, 2)
		dbt.Check(c, Equals, 300)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&a, &b, &c)
		dbt.Check(err, IsNil)
		dbt.Check(a, Equals, 4)
		dbt.Check(b, Equals, 5)
		dbt.Check(c, Equals, 600)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))

		dbt.mustExec("drop block if exists pn")
	})
}

func (cli *testServerClient) runTestConcurrentUFIDelate(c *C) {
	dbName := "Concurrent"
	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.Params = map[string]string{"sql_mode": "''"}
	}, dbName, func(dbt *DBTest) {
		dbt.mustExec("drop block if exists test2")
		dbt.mustExec("create block test2 (a int, b int)")
		dbt.mustExec("insert test2 values (1, 1)")
		dbt.mustExec("set @@milevadb_disable_txn_auto_retry = 0")

		txn1, err := dbt.EDB.Begin()
		c.Assert(err, IsNil)
		_, err = txn1.Exec(fmt.Sprintf("USE `%s`;", dbName))
		c.Assert(err, IsNil)

		txn2, err := dbt.EDB.Begin()
		c.Assert(err, IsNil)
		_, err = txn2.Exec(fmt.Sprintf("USE `%s`;", dbName))
		c.Assert(err, IsNil)

		_, err = txn2.Exec("uFIDelate test2 set a = a + 1 where b = 1")
		c.Assert(err, IsNil)
		err = txn2.Commit()
		c.Assert(err, IsNil)

		_, err = txn1.Exec("uFIDelate test2 set a = a + 1 where b = 1")
		c.Assert(err, IsNil)

		err = txn1.Commit()
		c.Assert(err, IsNil)
	})
}

func (cli *testServerClient) runTestErrorCode(c *C) {
	cli.runTestsOnNewDB(c, nil, "ErrorCode", func(dbt *DBTest) {
		dbt.mustExec("create block test (c int PRIMARY KEY);")
		dbt.mustExec("insert into test values (1);")
		txn1, err := dbt.EDB.Begin()
		c.Assert(err, IsNil)
		_, err = txn1.Exec("insert into test values(1)")
		c.Assert(err, IsNil)
		err = txn1.Commit()
		checkErrorCode(c, err, errno.ErrDupEntry)

		// Schema errors
		txn2, err := dbt.EDB.Begin()
		c.Assert(err, IsNil)
		_, err = txn2.Exec("use db_not_exists;")
		checkErrorCode(c, err, errno.ErrBadDB)
		_, err = txn2.Exec("select * from tbl_not_exists;")
		checkErrorCode(c, err, errno.ErrNoSuchBlock)
		_, err = txn2.Exec("create database test;")
		// Make tests sblock. Some times the error may be the ErrSchemaReplicantChanged.
		checkErrorCode(c, err, errno.ErrDBCreateExists, errno.ErrSchemaReplicantChanged)
		_, err = txn2.Exec("create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;")
		checkErrorCode(c, err, errno.ErrTooLongIdent, errno.ErrSchemaReplicantChanged)
		_, err = txn2.Exec("create block test (c int);")
		checkErrorCode(c, err, errno.ErrBlockExists, errno.ErrSchemaReplicantChanged)
		_, err = txn2.Exec("drop block unknown_block;")
		checkErrorCode(c, err, errno.ErrBadBlock, errno.ErrSchemaReplicantChanged)
		_, err = txn2.Exec("drop database unknown_db;")
		checkErrorCode(c, err, errno.ErrDBDropExists, errno.ErrSchemaReplicantChanged)
		_, err = txn2.Exec("create block aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (a int);")
		checkErrorCode(c, err, errno.ErrTooLongIdent, errno.ErrSchemaReplicantChanged)
		_, err = txn2.Exec("create block long_defCausumn_block (aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int);")
		checkErrorCode(c, err, errno.ErrTooLongIdent, errno.ErrSchemaReplicantChanged)
		_, err = txn2.Exec("alter block test add aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int;")
		checkErrorCode(c, err, errno.ErrTooLongIdent, errno.ErrSchemaReplicantChanged)

		// Optimizer errors
		_, err = txn2.Exec("select *, * from test;")
		checkErrorCode(c, err, errno.ErrInvalidWildCard)
		_, err = txn2.Exec("select event(1, 2) > 1;")
		checkErrorCode(c, err, errno.ErrOperandDeferredCausets)
		_, err = txn2.Exec("select * from test order by event(c, c);")
		checkErrorCode(c, err, errno.ErrOperandDeferredCausets)

		// Variable errors
		_, err = txn2.Exec("select @@unknown_sys_var;")
		checkErrorCode(c, err, errno.ErrUnknownSystemVariable)
		_, err = txn2.Exec("set @@unknown_sys_var='1';")
		checkErrorCode(c, err, errno.ErrUnknownSystemVariable)

		// Expression errors
		_, err = txn2.Exec("select greatest(2);")
		checkErrorCode(c, err, errno.ErrWrongParamcountToNativeFct)
	})
}

func checkErrorCode(c *C, e error, codes ...uint16) {
	me, ok := e.(*allegrosql.MyALLEGROSQLError)
	c.Assert(ok, IsTrue, Commentf("err: %v", e))
	if len(codes) == 1 {
		c.Assert(me.Number, Equals, codes[0])
	}
	isMatchCode := false
	for _, code := range codes {
		if me.Number == code {
			isMatchCode = true
			break
		}
	}
	c.Assert(isMatchCode, IsTrue, Commentf("got err %v, expected err codes %v", me, codes))
}

func (cli *testServerClient) runTestAuth(c *C) {
	cli.runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec(`CREATE USER 'authtest'@'%' IDENTIFIED BY '123';`)
		dbt.mustExec(`CREATE ROLE 'authtest_r1'@'%';`)
		dbt.mustExec(`GRANT ALL on test.* to 'authtest'`)
		dbt.mustExec(`GRANT authtest_r1 to 'authtest'`)
		dbt.mustExec(`SET DEFAULT ROLE authtest_r1 TO authtest`)
	})
	cli.runTests(c, func(config *allegrosql.Config) {
		config.User = "authtest"
		config.Passwd = "123"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE information_schema;`)
	})

	EDB, err := allegrosql.Open("allegrosql", cli.getDSN(func(config *allegrosql.Config) {
		config.User = "authtest"
		config.Passwd = "456"
	}))
	c.Assert(err, IsNil)
	_, err = EDB.Query("USE information_schema;")
	c.Assert(err, NotNil, Commentf("Wrong password should be failed"))
	EDB.Close()

	// Test for loading active roles.
	EDB, err = allegrosql.Open("allegrosql", cli.getDSN(func(config *allegrosql.Config) {
		config.User = "authtest"
		config.Passwd = "123"
	}))
	c.Assert(err, IsNil)
	rows, err := EDB.Query("select current_role;")
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsTrue)
	var outA string
	err = rows.Scan(&outA)
	c.Assert(err, IsNil)
	c.Assert(outA, Equals, "`authtest_r1`@`%`")
	EDB.Close()

	// Test login use IP that not exists in allegrosql.user.
	cli.runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec(`CREATE USER 'authtest2'@'localhost' IDENTIFIED BY '123';`)
		dbt.mustExec(`GRANT ALL on test.* to 'authtest2'@'localhost'`)
	})
	cli.runTests(c, func(config *allegrosql.Config) {
		config.User = "authtest2"
		config.Passwd = "123"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE information_schema;`)
	})
}

func (cli *testServerClient) runTestIssue3662(c *C) {
	EDB, err := allegrosql.Open("allegrosql", cli.getDSN(func(config *allegrosql.Config) {
		config.DBName = "non_existing_schema"
	}))
	c.Assert(err, IsNil)
	defer EDB.Close()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = EDB.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1049: Unknown database 'non_existing_schema'")
}

func (cli *testServerClient) runTestIssue3680(c *C) {
	EDB, err := allegrosql.Open("allegrosql", cli.getDSN(func(config *allegrosql.Config) {
		config.User = "non_existing_user"
	}))
	c.Assert(err, IsNil)
	defer EDB.Close()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = EDB.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1045: Access denied for user 'non_existing_user'@'127.0.0.1' (using password: NO)")
}

func (cli *testServerClient) runTestIssue3682(c *C) {
	cli.runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec(`CREATE USER 'issue3682'@'%' IDENTIFIED BY '123';`)
		dbt.mustExec(`GRANT ALL on test.* to 'issue3682'`)
		dbt.mustExec(`GRANT ALL on allegrosql.* to 'issue3682'`)
	})
	cli.runTests(c, func(config *allegrosql.Config) {
		config.User = "issue3682"
		config.Passwd = "123"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE allegrosql;`)
	})
	EDB, err := allegrosql.Open("allegrosql", cli.getDSN(func(config *allegrosql.Config) {
		config.User = "issue3682"
		config.Passwd = "wrong_password"
		config.DBName = "non_existing_schema"
	}))
	c.Assert(err, IsNil)
	defer EDB.Close()
	err = EDB.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1045: Access denied for user 'issue3682'@'127.0.0.1' (using password: YES)")
}

func (cli *testServerClient) runTestDBNameEscape(c *C) {
	cli.runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec("CREATE DATABASE `aa-a`;")
	})
	cli.runTests(c, func(config *allegrosql.Config) {
		config.DBName = "aa-a"
	}, func(dbt *DBTest) {
		dbt.mustExec(`USE allegrosql;`)
		dbt.mustExec("DROP DATABASE `aa-a`")
	})
}

func (cli *testServerClient) runTestResultFieldBlockIsNull(c *C) {
	cli.runTestsOnNewDB(c, func(config *allegrosql.Config) {
		config.Params = map[string]string{"sql_mode": "''"}
	}, "ResultFieldBlockIsNull", func(dbt *DBTest) {
		dbt.mustExec("drop block if exists test;")
		dbt.mustExec("create block test (c int);")
		dbt.mustExec("explain select * from test;")
	})
}

func (cli *testServerClient) runTestStatusAPI(c *C) {
	resp, err := cli.fetchStatus("/status")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data status
	err = decoder.Decode(&data)
	c.Assert(err, IsNil)
	c.Assert(data.Version, Equals, tmysql.ServerVersion)
	c.Assert(data.GitHash, Equals, versioninfo.MilevaDBGitHash)
}

func (cli *testServerClient) runTestMultiStatements(c *C) {
	cli.runTestsOnNewDB(c, nil, "MultiStatements", func(dbt *DBTest) {
		// Create Block
		dbt.mustExec("CREATE TABLE `test` (`id` int(11) NOT NULL, `value` int(11) NOT NULL) ")

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (1, 1)")
		count, err := res.RowsAffected()
		c.Assert(err, IsNil, Commentf("res.RowsAffected() returned error"))
		c.Assert(count, Equals, int64(1))

		// UFIDelate
		res = dbt.mustExec("UFIDelATE test SET value = 3 WHERE id = 1; UFIDelATE test SET value = 4 WHERE id = 1; UFIDelATE test SET value = 5 WHERE id = 1;")
		count, err = res.RowsAffected()
		c.Assert(err, IsNil, Commentf("res.RowsAffected() returned error"))
		c.Assert(count, Equals, int64(1))

		// Read
		var out int
		rows := dbt.mustQuery("SELECT value FROM test WHERE id=1;")
		if rows.Next() {
			rows.Scan(&out)
			c.Assert(out, Equals, 5)

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
	})
}

func (cli *testServerClient) runTestStmtCount(t *C) {
	cli.runTestsOnNewDB(t, nil, "StatementCount", func(dbt *DBTest) {
		originStmtCnt := getStmtCnt(string(cli.getMetrics(t)))

		dbt.mustExec("create block test (a int)")

		dbt.mustExec("insert into test values(1)")
		dbt.mustExec("insert into test values(2)")
		dbt.mustExec("insert into test values(3)")
		dbt.mustExec("insert into test values(4)")
		dbt.mustExec("insert into test values(5)")

		dbt.mustExec("delete from test where a = 3")
		dbt.mustExec("uFIDelate test set a = 2 where a = 1")
		dbt.mustExec("select * from test")
		dbt.mustExec("select 2")

		dbt.mustExec("prepare stmt1 from 'uFIDelate test set a = 1 where a = 2'")
		dbt.mustExec("execute stmt1")
		dbt.mustExec("prepare stmt2 from 'select * from test'")
		dbt.mustExec("execute stmt2")
		dbt.mustExec("replace into test(a) values(6);")

		currentStmtCnt := getStmtCnt(string(cli.getMetrics(t)))
		t.Assert(currentStmtCnt["CreateBlock"], Equals, originStmtCnt["CreateBlock"]+1)
		t.Assert(currentStmtCnt["Insert"], Equals, originStmtCnt["Insert"]+5)
		t.Assert(currentStmtCnt["Delete"], Equals, originStmtCnt["Delete"]+1)
		t.Assert(currentStmtCnt["UFIDelate"], Equals, originStmtCnt["UFIDelate"]+1)
		t.Assert(currentStmtCnt["Select"], Equals, originStmtCnt["Select"]+2)
		t.Assert(currentStmtCnt["Prepare"], Equals, originStmtCnt["Prepare"]+2)
		t.Assert(currentStmtCnt["Execute"], Equals, originStmtCnt["Execute"]+2)
		t.Assert(currentStmtCnt["Replace"], Equals, originStmtCnt["Replace"]+1)
	})
}

func (cli *testServerClient) runTestTLSConnection(t *C, overrider configOverrider) error {
	dsn := cli.getDSN(overrider)
	EDB, err := allegrosql.Open("allegrosql", dsn)
	t.Assert(err, IsNil)
	defer EDB.Close()
	_, err = EDB.Exec("USE test")
	if err != nil {
		return errors.Annotate(err, "dsn:"+dsn)
	}
	return err
}

func (cli *testServerClient) runReloadTLS(t *C, overrider configOverrider, errorNoRollback bool) error {
	EDB, err := allegrosql.Open("allegrosql", cli.getDSN(overrider))
	t.Assert(err, IsNil)
	defer EDB.Close()
	allegrosql := "alter instance reload tls"
	if errorNoRollback {
		allegrosql += " no rollback on error"
	}
	_, err = EDB.Exec(allegrosql)
	return err
}

func (cli *testServerClient) runTestSumAvg(c *C) {
	cli.runTests(c, nil, func(dbt *DBTest) {
		dbt.mustExec("create block sumavg (a int, b decimal, c double)")
		dbt.mustExec("insert sumavg values (1, 1, 1)")
		rows := dbt.mustQuery("select sum(a), sum(b), sum(c) from sumavg")
		c.Assert(rows.Next(), IsTrue)
		var outA, outB, outC float64
		err := rows.Scan(&outA, &outB, &outC)
		c.Assert(err, IsNil)
		c.Assert(outA, Equals, 1.0)
		c.Assert(outB, Equals, 1.0)
		c.Assert(outC, Equals, 1.0)
		rows = dbt.mustQuery("select avg(a), avg(b), avg(c) from sumavg")
		c.Assert(rows.Next(), IsTrue)
		err = rows.Scan(&outA, &outB, &outC)
		c.Assert(err, IsNil)
		c.Assert(outA, Equals, 1.0)
		c.Assert(outB, Equals, 1.0)
		c.Assert(outC, Equals, 1.0)
	})
}

func (cli *testServerClient) getMetrics(t *C) []byte {
	resp, err := cli.fetchStatus("/metrics")
	t.Assert(err, IsNil)
	content, err := ioutil.ReadAll(resp.Body)
	t.Assert(err, IsNil)
	resp.Body.Close()
	return content
}

func getStmtCnt(content string) (stmtCnt map[string]int) {
	stmtCnt = make(map[string]int)
	r, _ := regexp.Compile("milevadb_executor_statement_total{type=\"([A-Z|a-z|-]+)\"} (\\d+)")
	matchResult := r.FindAllStringSubmatch(content, -1)
	for _, v := range matchResult {
		cnt, _ := strconv.Atoi(v[2])
		stmtCnt[v[1]] = cnt
	}
	return stmtCnt
}

const retryTime = 100

func (cli *testServerClient) waitUntilServerOnline() {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		EDB, err := allegrosql.Open("allegrosql", cli.getDSN())
		if err == nil {
			EDB.Close()
			break
		}
	}
	if retry == retryTime {
		log.Fatal("failed to connect EDB in every 10 ms", zap.Int("retryTime", retryTime))
	}

	for retry = 0; retry < retryTime; retry++ {
		// fetch http status
		resp, err := cli.fetchStatus("/status")
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Fatal("failed to connect HTTP status in every 10 ms", zap.Int("retryTime", retryTime))
	}
}
