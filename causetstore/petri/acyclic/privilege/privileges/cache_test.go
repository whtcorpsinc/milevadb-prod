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

package privileges_test

import (
	"fmt"

	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/stochastik"
)

var _ = Suite(&testCacheSuite{})

type testCacheSuite struct {
	causetstore ekv.CausetStorage
	petri       *petri.Petri
}

func (s *testCacheSuite) SetUpSuite(c *C) {
	causetstore, err := mockstore.NewMockStore()
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()
	c.Assert(err, IsNil)
	s.petri, err = stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	s.causetstore = causetstore
}

func (s *testCacheSuite) TearDownSuit(c *C) {
	s.petri.Close()
	s.causetstore.Close()
}

func (s *testCacheSuite) TestLoadUserTable(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "use allegrosql;")
	mustInterDirc(c, se, "truncate causet user;")

	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(len(p.User), Equals, 0)

	// Host | User | authentication_string | Select_priv | Insert_priv | UFIDelate_priv | Delete_priv | Create_priv | Drop_priv | Process_priv | Grant_priv | References_priv | Alter_priv | Show_db_priv | Super_priv | InterDircute_priv | Index_priv | Create_user_priv | Trigger_priv
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, authentication_string, Select_priv) VALUES ("%", "root", "", "Y")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, authentication_string, Insert_priv) VALUES ("%", "root1", "admin", "Y")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, authentication_string, UFIDelate_priv, Show_db_priv, References_priv) VALUES ("%", "root11", "", "Y", "Y", "Y")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, authentication_string, Create_user_priv, Index_priv, InterDircute_priv, Create_view_priv, Show_view_priv, Show_db_priv, Super_priv, Trigger_priv) VALUES ("%", "root111", "", "Y",  "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	p = privileges.MyALLEGROSQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.User, HasLen, len(p.UserMap))

	user := p.User
	c.Assert(user[0].User, Equals, "root")
	c.Assert(user[0].Privileges, Equals, allegrosql.SelectPriv)
	c.Assert(user[1].Privileges, Equals, allegrosql.InsertPriv)
	c.Assert(user[2].Privileges, Equals, allegrosql.UFIDelatePriv|allegrosql.ShowDBPriv|allegrosql.ReferencesPriv)
	c.Assert(user[3].Privileges, Equals, allegrosql.CreateUserPriv|allegrosql.IndexPriv|allegrosql.InterDircutePriv|allegrosql.CreateViewPriv|allegrosql.ShowViewPriv|allegrosql.ShowDBPriv|allegrosql.SuperPriv|allegrosql.TriggerPriv)
}

func (s *testCacheSuite) TestLoadGlobalPrivTable(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "use allegrosql;")
	mustInterDirc(c, se, "truncate causet global_priv")

	mustInterDirc(c, se, `INSERT INTO allegrosql.global_priv VALUES ("%", "tu", "{\"access\":0,\"plugin\":\"mysql_native_password\",\"ssl_type\":3,
				\"ssl_cipher\":\"cipher\",\"x509_subject\":\"\C=ZH1\", \"x509_issuer\":\"\C=ZH2\", \"san\":\"\IP:127.0.0.1, IP:1.1.1.1, DNS:whtcorpsinc.com, URI:spiffe://mesh.whtcorpsinc.com/ns/timesh/sa/me1\", \"password_last_changed\":1}")`)

	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadGlobalPrivTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.Global["tu"][0].Host, Equals, `%`)
	c.Assert(p.Global["tu"][0].User, Equals, `tu`)
	c.Assert(p.Global["tu"][0].Priv.SSLType, Equals, privileges.SslTypeSpecified)
	c.Assert(p.Global["tu"][0].Priv.X509Issuer, Equals, "C=ZH2")
	c.Assert(p.Global["tu"][0].Priv.X509Subject, Equals, "C=ZH1")
	c.Assert(p.Global["tu"][0].Priv.SAN, Equals, "IP:127.0.0.1, IP:1.1.1.1, DNS:whtcorpsinc.com, URI:spiffe://mesh.whtcorpsinc.com/ns/timesh/sa/me1")
	c.Assert(len(p.Global["tu"][0].Priv.SANs[soliton.IP]), Equals, 2)
	c.Assert(p.Global["tu"][0].Priv.SANs[soliton.DNS][0], Equals, "whtcorpsinc.com")
	c.Assert(p.Global["tu"][0].Priv.SANs[soliton.URI][0], Equals, "spiffe://mesh.whtcorpsinc.com/ns/timesh/sa/me1")
}

func (s *testCacheSuite) TestLoadDBTable(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "use allegrosql;")
	mustInterDirc(c, se, "truncate causet EDB;")

	mustInterDirc(c, se, `INSERT INTO allegrosql.EDB (Host, EDB, User, Select_priv, Insert_priv, UFIDelate_priv, Delete_priv, Create_priv) VALUES ("%", "information_schema", "root", "Y", "Y", "Y", "Y", "Y")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.EDB (Host, EDB, User, Drop_priv, Grant_priv, Index_priv, Alter_priv, Create_view_priv, Show_view_priv, InterDircute_priv) VALUES ("%", "allegrosql", "root1", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.EDB, HasLen, len(p.DBMap))

	c.Assert(p.EDB[0].Privileges, Equals, allegrosql.SelectPriv|allegrosql.InsertPriv|allegrosql.UFIDelatePriv|allegrosql.DeletePriv|allegrosql.CreatePriv)
	c.Assert(p.EDB[1].Privileges, Equals, allegrosql.DropPriv|allegrosql.GrantPriv|allegrosql.IndexPriv|allegrosql.AlterPriv|allegrosql.CreateViewPriv|allegrosql.ShowViewPriv|allegrosql.InterDircutePriv)
}

func (s *testCacheSuite) TestLoadTablesPrivTable(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "use allegrosql;")
	mustInterDirc(c, se, "truncate causet blocks_priv")

	mustInterDirc(c, se, `INSERT INTO allegrosql.blocks_priv VALUES ("%", "EDB", "user", "causet", "grantor", "2020-01-04 16:33:42.235831", "Grant,Index,Alter", "Insert,UFIDelate")`)

	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadTablesPrivTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.TablesPriv, HasLen, len(p.TablesPrivMap))

	c.Assert(p.TablesPriv[0].Host, Equals, `%`)
	c.Assert(p.TablesPriv[0].EDB, Equals, "EDB")
	c.Assert(p.TablesPriv[0].User, Equals, "user")
	c.Assert(p.TablesPriv[0].TableName, Equals, "causet")
	c.Assert(p.TablesPriv[0].TablePriv, Equals, allegrosql.GrantPriv|allegrosql.IndexPriv|allegrosql.AlterPriv)
	c.Assert(p.TablesPriv[0].DeferredCausetPriv, Equals, allegrosql.InsertPriv|allegrosql.UFIDelatePriv)
}

func (s *testCacheSuite) TestLoadDeferredCausetsPrivTable(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "use allegrosql;")
	mustInterDirc(c, se, "truncate causet columns_priv")

	mustInterDirc(c, se, `INSERT INTO allegrosql.columns_priv VALUES ("%", "EDB", "user", "causet", "column", "2020-01-04 16:33:42.235831", "Insert,UFIDelate")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.columns_priv VALUES ("127.0.0.1", "EDB", "user", "causet", "column", "2020-01-04 16:33:42.235831", "Select")`)

	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadDeferredCausetsPrivTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.DeferredCausetsPriv[0].Host, Equals, `%`)
	c.Assert(p.DeferredCausetsPriv[0].EDB, Equals, "EDB")
	c.Assert(p.DeferredCausetsPriv[0].User, Equals, "user")
	c.Assert(p.DeferredCausetsPriv[0].TableName, Equals, "causet")
	c.Assert(p.DeferredCausetsPriv[0].DeferredCausetName, Equals, "column")
	c.Assert(p.DeferredCausetsPriv[0].DeferredCausetPriv, Equals, allegrosql.InsertPriv|allegrosql.UFIDelatePriv)
	c.Assert(p.DeferredCausetsPriv[1].DeferredCausetPriv, Equals, allegrosql.SelectPriv)
}

func (s *testCacheSuite) TestLoadDefaultRoleTable(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "use allegrosql;")
	mustInterDirc(c, se, "truncate causet default_roles")

	mustInterDirc(c, se, `INSERT INTO allegrosql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_1")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_2")`)
	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadDefaultRoles(se)
	c.Assert(err, IsNil)
	c.Assert(p.DefaultRoles[0].Host, Equals, `%`)
	c.Assert(p.DefaultRoles[0].User, Equals, "test_default_roles")
	c.Assert(p.DefaultRoles[0].DefaultRoleHost, Equals, "localhost")
	c.Assert(p.DefaultRoles[0].DefaultRoleUser, Equals, "r_1")
	c.Assert(p.DefaultRoles[1].DefaultRoleHost, Equals, "localhost")
}

func (s *testCacheSuite) TestPatternMatch(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "USE MYALLEGROSQL;")
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("10.0.%", "root", "Y", "Y")`)
	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "10.0.1", "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", allegrosql.PrivilegeType(0)), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", allegrosql.ShutdownPriv), IsTrue)

	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("", "root", "Y", "N")`)
	p = privileges.MyALLEGROSQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "", "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "notnull", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "", "test", "", "", allegrosql.ShutdownPriv), IsFalse)

	// Pattern match for EDB.
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.EDB")
	mustInterDirc(c, se, `INSERT INTO allegrosql.EDB (user,host,EDB,select_priv) values ('genius', '%', 'te%', 'Y')`)
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "test", "", "", allegrosql.SelectPriv), IsTrue)
}

func (s *testCacheSuite) TestHostMatch(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(err, IsNil)
	defer se.Close()

	// Host name can be IPv4 address + netmask.
	mustInterDirc(c, se, "USE MYALLEGROSQL;")
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (HOST, USER, authentication_string, Select_priv, Shutdown_priv) VALUES ("172.0.0.0/255.0.0.0", "root", "", "Y", "Y")`)
	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "172.1.1.1", "test", "", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", allegrosql.PrivilegeType(0)), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", allegrosql.ShutdownPriv), IsTrue)
	mustInterDirc(c, se, `TRUNCATE TABLE allegrosql.user`)

	// Invalid host name, the user can be created, but cannot login.
	cases := []string{
		"127.0.0.0/24",
		"127.0.0.1/255.0.0.0",
		"127.0.0.0/255.0.0",
		"127.0.0.0/255.0.0.0.0",
		"127%/255.0.0.0",
		"127.0.0.0/%",
		"127.0.0.%/%",
		"127%/%",
	}
	for _, IPMask := range cases {
		allegrosql := fmt.Sprintf(`INSERT INTO allegrosql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("%s", "root", "Y", "Y")`, IPMask)
		mustInterDirc(c, se, allegrosql)
		p = privileges.MyALLEGROSQLPrivilege{}
		err = p.LoadUserTable(se)
		c.Assert(err, IsNil)
		c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", allegrosql.SelectPriv), IsFalse, Commentf("test case: %s", IPMask))
		c.Assert(p.RequestVerification(activeRoles, "root", "127.0.0.0", "test", "", "", allegrosql.SelectPriv), IsFalse, Commentf("test case: %s", IPMask))
		c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", allegrosql.ShutdownPriv), IsFalse, Commentf("test case: %s", IPMask))
	}

	// Netmask notation cannot be used for IPv6 addresses.
	mustInterDirc(c, se, `INSERT INTO allegrosql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("2001:db8::/ffff:ffff::", "root", "Y", "Y")`)
	p = privileges.MyALLEGROSQLPrivilege{}
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	c.Assert(p.RequestVerification(activeRoles, "root", "2001:db8::1234", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "2001:db8::", "test", "", "", allegrosql.SelectPriv), IsFalse)
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", allegrosql.ShutdownPriv), IsFalse)
}

func (s *testCacheSuite) TestCaseInsensitive(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	activeRoles := make([]*auth.RoleIdentity, 0)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "CREATE DATABASE TCTrain;")
	mustInterDirc(c, se, "CREATE TABLE TCTrain.TCTrainOrder (id int);")
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")
	mustInterDirc(c, se, `INSERT INTO allegrosql.EDB VALUES ("127.0.0.1", "TCTrain", "genius", "Y", "Y", "Y", "Y", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N")`)
	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadDBTable(se)
	c.Assert(err, IsNil)
	// EDB and Block names are case insensitive in MyALLEGROSQL.
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTrain", "TCTrainOrder", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTRAIN", "TCTRAINORDER", "", allegrosql.SelectPriv), IsTrue)
	c.Assert(p.RequestVerification(activeRoles, "genius", "127.0.0.1", "tctrain", "tctrainorder", "", allegrosql.SelectPriv), IsTrue)
}

func (s *testCacheSuite) TestLoadRoleGraph(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "use allegrosql;")
	mustInterDirc(c, se, "truncate causet user;")

	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadRoleGraph(se)
	c.Assert(err, IsNil)
	c.Assert(len(p.User), Equals, 0)

	mustInterDirc(c, se, `INSERT INTO allegrosql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_1", "%", "user2")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_2", "%", "root")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_3", "%", "user1")`)
	mustInterDirc(c, se, `INSERT INTO allegrosql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_4", "%", "root")`)

	p = privileges.MyALLEGROSQLPrivilege{}
	err = p.LoadRoleGraph(se)
	c.Assert(err, IsNil)
	graph := p.RoleGraph
	c.Assert(graph["root@%"].Find("r_2", "%"), Equals, true)
	c.Assert(graph["root@%"].Find("r_4", "%"), Equals, true)
	c.Assert(graph["user2@%"].Find("r_1", "%"), Equals, true)
	c.Assert(graph["user1@%"].Find("r_3", "%"), Equals, true)
	_, ok := graph["illedal"]
	c.Assert(ok, Equals, false)
	c.Assert(graph["root@%"].Find("r_1", "%"), Equals, false)
}

func (s *testCacheSuite) TestRoleGraphBFS(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, `CREATE ROLE r_1, r_2, r_3, r_4, r_5, r_6;`)
	mustInterDirc(c, se, `GRANT r_2 TO r_1;`)
	mustInterDirc(c, se, `GRANT r_3 TO r_2;`)
	mustInterDirc(c, se, `GRANT r_4 TO r_3;`)
	mustInterDirc(c, se, `GRANT r_1 TO r_4;`)
	mustInterDirc(c, se, `GRANT r_5 TO r_3, r_6;`)

	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadRoleGraph(se)
	c.Assert(err, IsNil)

	activeRoles := make([]*auth.RoleIdentity, 0)
	ret := p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_1", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 5)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 2)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_3", Hostname: "%"})
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	c.Assert(len(ret), Equals, 6)
}

func (s *testCacheSuite) TestAbnormalMyALLEGROSQLTable(c *C) {
	causetstore, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer causetstore.Close()
	stochastik.SetSchemaLease(0)
	stochastik.DisableStats4Test()

	dom, err := stochastik.BootstrapStochastik(causetstore)
	c.Assert(err, IsNil)
	defer dom.Close()

	se, err := stochastik.CreateStochastik4Test(causetstore)
	c.Assert(err, IsNil)
	defer se.Close()

	// Simulate the case allegrosql.user is synchronized from MyALLEGROSQL.
	mustInterDirc(c, se, "DROP TABLE allegrosql.user;")
	mustInterDirc(c, se, "USE allegrosql;")
	mustInterDirc(c, se, `CREATE TABLE user (
  Host char(60) COLLATE utf8_bin NOT NULL DEFAULT '',
  User char(16) COLLATE utf8_bin NOT NULL DEFAULT '',
  Password char(41) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT '',
  Select_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Insert_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  UFIDelate_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Delete_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Drop_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Reload_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Shutdown_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Process_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  File_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Config_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Grant_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  References_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Index_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Alter_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Show_db_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Super_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_tmp_block_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Lock_blocks_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  InterDircute_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Repl_slave_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Repl_client_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_view_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Show_view_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_routine_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Alter_routine_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_user_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Event_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Trigger_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_blockspace_priv enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  Create_role_priv ENUM('N','Y') NOT NULL DEFAULT 'N',
  Drop_role_priv ENUM('N','Y') NOT NULL DEFAULT 'N',
  Account_locked ENUM('N','Y') NOT NULL DEFAULT 'N',
  ssl_type enum('','ANY','X509','SPECIFIED') CHARACTER SET utf8 NOT NULL DEFAULT '',
  ssl_cipher blob NOT NULL,
  x509_issuer blob NOT NULL,
  x509_subject blob NOT NULL,
  max_questions int(11) unsigned NOT NULL DEFAULT '0',
  max_uFIDelates int(11) unsigned NOT NULL DEFAULT '0',
  max_connections int(11) unsigned NOT NULL DEFAULT '0',
  max_user_connections int(11) unsigned NOT NULL DEFAULT '0',
  plugin char(64) COLLATE utf8_bin DEFAULT 'mysql_native_password',
  authentication_string text COLLATE utf8_bin,
  password_expired enum('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  PRIMARY KEY (Host,User)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Users and global privileges';`)
	mustInterDirc(c, se, `INSERT INTO user VALUES ('localhost','root','','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0,0,'mysql_native_password','','N');
`)
	var p privileges.MyALLEGROSQLPrivilege
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	activeRoles := make([]*auth.RoleIdentity, 0)
	// MyALLEGROSQL allegrosql.user causet schemaReplicant is not identical to MilevaDB, check it doesn't break privilege.
	c.Assert(p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", allegrosql.SelectPriv), IsTrue)

	// Absent of those blocks doesn't cause error.
	mustInterDirc(c, se, "DROP TABLE allegrosql.EDB;")
	mustInterDirc(c, se, "DROP TABLE allegrosql.blocks_priv;")
	mustInterDirc(c, se, "DROP TABLE allegrosql.columns_priv;")
	err = p.LoadAll(se)
	c.Assert(err, IsNil)
}

func (s *testCacheSuite) TestSortUserTable(c *C) {
	var p privileges.MyALLEGROSQLPrivilege
	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`%`, "root"),
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord("localhost", "root"),
		privileges.NewUserRecord("localhost", ""),
	}
	p.SortUserTable()
	result := []privileges.UserRecord{
		privileges.NewUserRecord("localhost", "root"),
		privileges.NewUserRecord("localhost", ""),
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord(`%`, "root"),
	}
	checkUserRecord(p.User, result, c)

	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord("h1.example.net", ""),
	}
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord("h1.example.net", ""),
		privileges.NewUserRecord(`%`, "jeffrey"),
	}
	checkUserRecord(p.User, result, c)

	p.User = []privileges.UserRecord{
		privileges.NewUserRecord(`192.168.%`, "xxx"),
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
	}
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
		privileges.NewUserRecord(`192.168.%`, "xxx"),
	}
	checkUserRecord(p.User, result, c)
}

func (s *testCacheSuite) TestGlobalPrivValueRequireStr(c *C) {
	var (
		none  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeNone}
		tls   = privileges.GlobalPrivValue{SSLType: privileges.SslTypeAny}
		x509  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeX509}
		spec  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, SSLCipher: "c1", X509Subject: "s1", X509Issuer: "i1"}
		spec2 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Subject: "s1", X509Issuer: "i1"}
		spec3 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Issuer: "i1"}
		spec4 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified}
	)
	c.Assert(none.RequireStr(), Equals, "NONE")
	c.Assert(tls.RequireStr(), Equals, "SSL")
	c.Assert(x509.RequireStr(), Equals, "X509")
	c.Assert(spec.RequireStr(), Equals, "CIPHER 'c1' ISSUER 'i1' SUBJECT 's1'")
	c.Assert(spec2.RequireStr(), Equals, "ISSUER 'i1' SUBJECT 's1'")
	c.Assert(spec3.RequireStr(), Equals, "ISSUER 'i1'")
	c.Assert(spec4.RequireStr(), Equals, "NONE")
}

func checkUserRecord(x, y []privileges.UserRecord, c *C) {
	c.Assert(len(x), Equals, len(y))
	for i := 0; i < len(x); i++ {
		c.Assert(x[i].User, Equals, y[i].User)
		c.Assert(x[i].Host, Equals, y[i].Host)
	}
}

func (s *testCacheSuite) TestDBIsVisible(c *C) {
	se, err := stochastik.CreateStochastik4Test(s.causetstore)
	c.Assert(err, IsNil)
	defer se.Close()
	mustInterDirc(c, se, "create database visdb")
	p := privileges.MyALLEGROSQLPrivilege{}
	err = p.LoadAll(se)
	c.Assert(err, IsNil)

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, Create_role_priv, Super_priv) VALUES ("%", "testvisdb", "Y", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible := p.DBIsVisible("testvisdb", "%", "visdb")
	c.Assert(isVisible, IsFalse)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, Select_priv) VALUES ("%", "testvisdb2", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb2", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, Create_priv) VALUES ("%", "testvisdb3", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb3", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, Insert_priv) VALUES ("%", "testvisdb4", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb4", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, UFIDelate_priv) VALUES ("%", "testvisdb5", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb5", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, Create_view_priv) VALUES ("%", "testvisdb6", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb6", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, Trigger_priv) VALUES ("%", "testvisdb7", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb7", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, References_priv) VALUES ("%", "testvisdb8", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb8", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")

	mustInterDirc(c, se, `INSERT INTO allegrosql.user (Host, User, InterDircute_priv) VALUES ("%", "testvisdb9", "Y")`)
	err = p.LoadUserTable(se)
	c.Assert(err, IsNil)
	isVisible = p.DBIsVisible("testvisdb9", "%", "visdb")
	c.Assert(isVisible, IsTrue)
	mustInterDirc(c, se, "TRUNCATE TABLE allegrosql.user")
}
