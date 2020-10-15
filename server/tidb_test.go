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
// +build !race

package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-allegrosql-driver/allegrosql"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/errors"
	tmysql "github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/testkit"
)

type milevadbTestSuite struct {
	*milevadbTestSuiteBase
}

type milevadbTestSerialSuite struct {
	*milevadbTestSuiteBase
}

type milevadbTestSuiteBase struct {
	*testServerClient
	milevadbdrv *MilevaDBDriver
	server  *Server
	petri  *petri.Petri
	causetstore   ekv.CausetStorage
}

func newMilevaDBTestSuiteBase() *milevadbTestSuiteBase {
	return &milevadbTestSuiteBase{
		testServerClient: newTestServerClient(),
	}
}

var _ = Suite(&milevadbTestSuite{newMilevaDBTestSuiteBase()})
var _ = SerialSuites(&milevadbTestSerialSuite{newMilevaDBTestSuiteBase()})

func (ts *milevadbTestSuite) SetUpSuite(c *C) {
	metrics.RegisterMetrics()
	ts.milevadbTestSuiteBase.SetUpSuite(c)
}

func (ts *milevadbTestSuiteBase) SetUpSuite(c *C) {
	var err error
	ts.causetstore, err = mockstore.NewMockStore()
	stochastik.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.petri, err = stochastik.BootstrapStochastik(ts.causetstore)
	c.Assert(err, IsNil)
	ts.milevadbdrv = NewMilevaDBDriver(ts.causetstore)
	cfg := newTestConfig()
	cfg.Port = ts.port
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = ts.statusPort
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	ts.port = getPortFromTCPAddr(server.listener.Addr())
	ts.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ts.server = server
	go ts.server.Run()
	ts.waitUntilServerOnline()
}

func (ts *milevadbTestSuiteBase) TearDownSuite(c *C) {
	if ts.causetstore != nil {
		ts.causetstore.Close()
	}
	if ts.petri != nil {
		ts.petri.Close()
	}
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *milevadbTestSuite) TestRegression(c *C) {
	if regression {
		c.Parallel()
		ts.runTestRegression(c, nil, "Regression")
	}
}

func (ts *milevadbTestSuite) TestUint64(c *C) {
	ts.runTestPrepareResultFieldType(c)
}

func (ts *milevadbTestSuite) TestSpecialType(c *C) {
	c.Parallel()
	ts.runTestSpecialType(c)
}

func (ts *milevadbTestSuite) TestPreparedString(c *C) {
	c.Parallel()
	ts.runTestPreparedString(c)
}

func (ts *milevadbTestSuite) TestPreparedTimestamp(c *C) {
	c.Parallel()
	ts.runTestPreparedTimestamp(c)
}

// this test will change `ekv.TxnTotalSizeLimit` which may affect other test suites,
// so we must make it running in serial.
func (ts *milevadbTestSerialSuite) TestLoadData(c *C) {
	ts.runTestLoadData(c, ts.server)
	ts.runTestLoadDataWithSelectIntoOutfile(c, ts.server)
}

func (ts *milevadbTestSerialSuite) TestStmtCount(c *C) {
	ts.runTestStmtCount(c)
}

func (ts *milevadbTestSuite) TestConcurrentUFIDelate(c *C) {
	c.Parallel()
	ts.runTestConcurrentUFIDelate(c)
}

func (ts *milevadbTestSuite) TestErrorCode(c *C) {
	c.Parallel()
	ts.runTestErrorCode(c)
}

func (ts *milevadbTestSuite) TestAuth(c *C) {
	c.Parallel()
	ts.runTestAuth(c)
	ts.runTestIssue3682(c)
}

func (ts *milevadbTestSuite) TestIssues(c *C) {
	c.Parallel()
	ts.runTestIssue3662(c)
	ts.runTestIssue3680(c)
}

func (ts *milevadbTestSuite) TestDBNameEscape(c *C) {
	c.Parallel()
	ts.runTestDBNameEscape(c)
}

func (ts *milevadbTestSuite) TestResultFieldBlockIsNull(c *C) {
	c.Parallel()
	ts.runTestResultFieldBlockIsNull(c)
}

func (ts *milevadbTestSuite) TestStatusAPI(c *C) {
	c.Parallel()
	ts.runTestStatusAPI(c)
}

func (ts *milevadbTestSuite) TestStatusPort(c *C) {
	var err error
	ts.causetstore, err = mockstore.NewMockStore()
	stochastik.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.petri, err = stochastik.BootstrapStochastik(ts.causetstore)
	c.Assert(err, IsNil)
	ts.milevadbdrv = NewMilevaDBDriver(ts.causetstore)
	cfg := newTestConfig()
	cfg.Port = 0
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = ts.statusPort
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, NotNil)
	c.Assert(server, IsNil)
}

func (ts *milevadbTestSuite) TestStatusAPIWithTLS(c *C) {
	caCert, caKey, err := generateCert(0, "MilevaDB CA 2", nil, nil, "/tmp/ca-key-2.pem", "/tmp/ca-cert-2.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "milevadb-server-2", caCert, caKey, "/tmp/server-key-2.pem", "/tmp/server-cert-2.pem")
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key-2.pem")
		os.Remove("/tmp/ca-cert-2.pem")
		os.Remove("/tmp/server-key-2.pem")
		os.Remove("/tmp/server-cert-2.pem")
	}()

	cli := newTestServerClient()
	cli.statusScheme = "https"
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.StatusPort = cli.statusPort
	cfg.Security.ClusterSSLCA = "/tmp/ca-cert-2.pem"
	cfg.Security.ClusterSSLCert = "/tmp/server-cert-2.pem"
	cfg.Security.ClusterSSLKey = "/tmp/server-key-2.pem"
	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)

	// https connection should work.
	ts.runTestStatusAPI(c)

	// but plain http connection should fail.
	cli.statusScheme = "http"
	_, err = cli.fetchStatus("/status")
	c.Assert(err, NotNil)

	server.Close()
}

func (ts *milevadbTestSuite) TestStatusAPIWithTLSCNCheck(c *C) {
	caPath := filepath.Join(os.TemFIDelir(), "ca-cert-cn.pem")
	serverKeyPath := filepath.Join(os.TemFIDelir(), "server-key-cn.pem")
	serverCertPath := filepath.Join(os.TemFIDelir(), "server-cert-cn.pem")
	client1KeyPath := filepath.Join(os.TemFIDelir(), "client-key-cn-check-a.pem")
	client1CertPath := filepath.Join(os.TemFIDelir(), "client-cert-cn-check-a.pem")
	client2KeyPath := filepath.Join(os.TemFIDelir(), "client-key-cn-check-b.pem")
	client2CertPath := filepath.Join(os.TemFIDelir(), "client-cert-cn-check-b.pem")

	caCert, caKey, err := generateCert(0, "MilevaDB CA CN CHECK", nil, nil, filepath.Join(os.TemFIDelir(), "ca-key-cn.pem"), caPath)
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "milevadb-server-cn-check", caCert, caKey, serverKeyPath, serverCertPath)
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "milevadb-client-cn-check-a", caCert, caKey, client1KeyPath, client1CertPath, func(c *x509.Certificate) {
		c.Subject.CommonName = "milevadb-client-1"
	})
	c.Assert(err, IsNil)
	_, _, err = generateCert(3, "milevadb-client-cn-check-b", caCert, caKey, client2KeyPath, client2CertPath, func(c *x509.Certificate) {
		c.Subject.CommonName = "milevadb-client-2"
	})
	c.Assert(err, IsNil)

	cli := newTestServerClient()
	cli.statusScheme = "https"
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.StatusPort = cli.statusPort
	cfg.Security.ClusterSSLCA = caPath
	cfg.Security.ClusterSSLCert = serverCertPath
	cfg.Security.ClusterSSLKey = serverKeyPath
	cfg.Security.ClusterVerifyCN = []string{"milevadb-client-2"}
	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	cli.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)

	hc := newTLSHttpClient(c, caPath,
		client1CertPath,
		client1KeyPath,
	)
	_, err = hc.Get(cli.statusURL("/status"))
	c.Assert(err, NotNil)

	hc = newTLSHttpClient(c, caPath,
		client2CertPath,
		client2KeyPath,
	)
	_, err = hc.Get(cli.statusURL("/status"))
	c.Assert(err, IsNil)
}

func newTLSHttpClient(c *C, caFile, certFile, keyFile string) *http.Client {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	c.Assert(err, IsNil)
	caCert, err := ioutil.ReadFile(caFile)
	c.Assert(err, IsNil)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	}
	tlsConfig.BuildNameToCertificate()
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
}

func (ts *milevadbTestSuite) TestMultiStatements(c *C) {
	c.Parallel()
	ts.runTestMultiStatements(c)
}

func (ts *milevadbTestSuite) TestSocketForwarding(c *C) {
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = "/tmp/milevadbtest.sock"
	cfg.Port = cli.port
	os.Remove(cfg.Socket)
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	cli.runTestRegression(c, func(config *allegrosql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = "/tmp/milevadbtest.sock"
		config.DBName = "test"
		config.Params = map[string]string{"sql_mode": "'STRICT_ALL_TABLES'"}
	}, "SocketRegression")
}

func (ts *milevadbTestSuite) TestSocket(c *C) {
	cfg := newTestConfig()
	cfg.Socket = "/tmp/milevadbtest.sock"
	cfg.Port = 0
	os.Remove(cfg.Socket)
	cfg.Host = ""
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()

	//a fake server client, config is override, just used to run tests
	cli := newTestServerClient()
	cli.runTestRegression(c, func(config *allegrosql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = "/tmp/milevadbtest.sock"
		config.DBName = "test"
		config.Params = map[string]string{"sql_mode": "STRICT_ALL_TABLES"}
	}, "SocketRegression")

}

// generateCert generates a private key and a certificate in PEM format based on parameters.
// If parentCert and parentCertKey is specified, the new certificate will be signed by the parentCert.
// Otherwise, the new certificate will be self-signed and is a CA.
func generateCert(sn int, commonName string, parentCert *x509.Certificate, parentCertKey *rsa.PrivateKey, outKeyFile string, outCertFile string, opts ...func(c *x509.Certificate)) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 528)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	notBefore := time.Now().Add(-10 * time.Minute).UTC()
	notAfter := notBefore.Add(1 * time.Hour).UTC()

	template := x509.Certificate{
		SerialNumber:          big.NewInt(int64(sn)),
		Subject:               pkix.Name{CommonName: commonName, Names: []pkix.AttributeTypeAndValue{soliton.MockPkixAttribute(soliton.CommonName, commonName)}},
		DNSNames:              []string{commonName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	for _, opt := range opts {
		opt(&template)
	}

	var parent *x509.Certificate
	var priv *rsa.PrivateKey

	if parentCert == nil || parentCertKey == nil {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		parent = &template
		priv = privateKey
	} else {
		parent = parentCert
		priv = parentCertKey
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, parent, &privateKey.PublicKey, priv)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	certOut, err := os.Create(outCertFile)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	keyOut, err := os.OpenFile(outKeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	keyOut.Close()

	return cert, privateKey, nil
}

// registerTLSConfig registers a allegrosql client TLS config.
// See https://godoc.org/github.com/go-allegrosql-driver/allegrosql#RegisterTLSConfig for details.
func registerTLSConfig(configName string, caCertPath string, clientCertPath string, clientKeyPath string, serverName string, verifyServer bool) error {
	rootCertPool := x509.NewCertPool()
	data, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return err
	}
	if ok := rootCertPool.AppendCertsFromPEM(data); !ok {
		return errors.New("Failed to append PEM")
	}
	clientCert := make([]tls.Certificate, 0, 1)
	certs, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return err
	}
	clientCert = append(clientCert, certs)
	tlsConfig := &tls.Config{
		RootCAs:            rootCertPool,
		Certificates:       clientCert,
		ServerName:         serverName,
		InsecureSkipVerify: !verifyServer,
	}
	allegrosql.RegisterTLSConfig(configName, tlsConfig)
	return nil
}

func (ts *milevadbTestSuite) TestSystemTimeZone(c *C) {
	tk := testkit.NewTestKit(c, ts.causetstore)
	cfg := newTestConfig()
	cfg.Port, cfg.Status.StatusPort = 0, 0
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	defer server.Close()

	tz1 := tk.MustQuery("select variable_value from allegrosql.milevadb where variable_name = 'system_tz'").Rows()
	tk.MustQuery("select @@system_time_zone").Check(tz1)
}

func (ts *milevadbTestSerialSuite) TestTLS(c *C) {
	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "MilevaDB CA", nil, nil, "/tmp/ca-key.pem", "/tmp/ca-cert.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "milevadb-server", caCert, caKey, "/tmp/server-key.pem", "/tmp/server-cert.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "ALLEGROALLEGROSQL Client Certificate", caCert, caKey, "/tmp/client-key.pem", "/tmp/client-cert.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-certificate", "/tmp/ca-cert.pem", "/tmp/client-cert.pem", "/tmp/client-key.pem", "milevadb-server", true)
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key.pem")
		os.Remove("/tmp/ca-cert.pem")
		os.Remove("/tmp/server-key.pem")
		os.Remove("/tmp/server-cert.pem")
		os.Remove("/tmp/client-key.pem")
		os.Remove("/tmp/client-cert.pem")
	}()

	// Start the server without TLS.
	connOverrider := func(config *allegrosql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	err = cli.runTestTLSConnection(c, connOverrider) // We should get ErrNoTLS.
	c.Assert(err, NotNil)
	c.Assert(errors.Cause(err).Error(), Equals, allegrosql.ErrNoTLS.Error())
	server.Close()

	// Start the server with TLS but without CA, in this case the server will not verify client's certificate.
	connOverrider = func(config *allegrosql.Config) {
		config.TLSConfig = "skip-verify"
	}
	cli = newTestServerClient()
	cfg = newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCert: "/tmp/server-cert.pem",
		SSLKey:  "/tmp/server-key.pem",
	}
	server, err = NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	err = cli.runTestTLSConnection(c, connOverrider) // We should establish connection successfully.
	c.Assert(err, IsNil)
	cli.runTestRegression(c, connOverrider, "TLSRegression")
	// Perform server verification.
	connOverrider = func(config *allegrosql.Config) {
		config.TLSConfig = "client-certificate"
	}
	err = cli.runTestTLSConnection(c, connOverrider) // We should establish connection successfully.
	c.Assert(err, IsNil, Commentf("%v", errors.ErrorStack(err)))
	cli.runTestRegression(c, connOverrider, "TLSRegression")
	server.Close()

	// Start the server with TLS & CA, if the client presents its certificate, the certificate will be verified.
	cli = newTestServerClient()
	cfg = newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert.pem",
		SSLCert: "/tmp/server-cert.pem",
		SSLKey:  "/tmp/server-key.pem",
	}
	server, err = NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	// The client does not provide a certificate, the connection should succeed.
	err = cli.runTestTLSConnection(c, nil)
	c.Assert(err, IsNil)
	cli.runTestRegression(c, connOverrider, "TLSRegression")
	// The client provides a valid certificate.
	connOverrider = func(config *allegrosql.Config) {
		config.TLSConfig = "client-certificate"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)
	cli.runTestRegression(c, connOverrider, "TLSRegression")
	server.Close()

	c.Assert(soliton.IsTLSExpiredError(errors.New("unknown test")), IsFalse)
	c.Assert(soliton.IsTLSExpiredError(x509.CertificateInvalidError{Reason: x509.CANotAuthorizedForThisName}), IsFalse)
	c.Assert(soliton.IsTLSExpiredError(x509.CertificateInvalidError{Reason: x509.Expired}), IsTrue)

	_, err = soliton.LoadTLSCertificates("", "wrong key", "wrong cert")
	c.Assert(err, NotNil)
	_, err = soliton.LoadTLSCertificates("wrong ca", "/tmp/server-key.pem", "/tmp/server-cert.pem")
	c.Assert(err, NotNil)
}

func (ts *milevadbTestSerialSuite) TestReloadTLS(c *C) {
	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "MilevaDB CA", nil, nil, "/tmp/ca-key-reload.pem", "/tmp/ca-cert-reload.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "milevadb-server", caCert, caKey, "/tmp/server-key-reload.pem", "/tmp/server-cert-reload.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "ALLEGROALLEGROSQL Client Certificate", caCert, caKey, "/tmp/client-key-reload.pem", "/tmp/client-cert-reload.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-certificate-reload", "/tmp/ca-cert-reload.pem", "/tmp/client-cert-reload.pem", "/tmp/client-key-reload.pem", "milevadb-server", true)
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key-reload.pem")
		os.Remove("/tmp/ca-cert-reload.pem")

		os.Remove("/tmp/server-key-reload.pem")
		os.Remove("/tmp/server-cert-reload.pem")
		os.Remove("/tmp/client-key-reload.pem")
		os.Remove("/tmp/client-cert-reload.pem")
	}()

	// try old cert used in startup configuration.
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert-reload.pem",
		SSLCert: "/tmp/server-cert-reload.pem",
		SSLKey:  "/tmp/server-key-reload.pem",
	}
	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	// The client provides a valid certificate.
	connOverrider := func(config *allegrosql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)

	// try reload a valid cert.
	tlsCfg := server.getTLSConfig()
	cert, err := x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	c.Assert(err, IsNil)
	oldExpireTime := cert.NotAfter
	_, _, err = generateCert(1, "milevadb-server", caCert, caKey, "/tmp/server-key-reload2.pem", "/tmp/server-cert-reload2.pem", func(c *x509.Certificate) {
		c.NotBefore = time.Now().Add(-24 * time.Hour).UTC()
		c.NotAfter = time.Now().Add(1 * time.Hour).UTC()
	})
	c.Assert(err, IsNil)
	os.Rename("/tmp/server-key-reload2.pem", "/tmp/server-key-reload.pem")
	os.Rename("/tmp/server-cert-reload2.pem", "/tmp/server-cert-reload.pem")
	connOverrider = func(config *allegrosql.Config) {
		config.TLSConfig = "skip-verify"
	}
	err = cli.runReloadTLS(c, connOverrider, false)
	c.Assert(err, IsNil)
	connOverrider = func(config *allegrosql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)

	tlsCfg = server.getTLSConfig()
	cert, err = x509.ParseCertificate(tlsCfg.Certificates[0].Certificate[0])
	c.Assert(err, IsNil)
	newExpireTime := cert.NotAfter
	c.Assert(newExpireTime.After(oldExpireTime), IsTrue)

	// try reload a expired cert.
	_, _, err = generateCert(1, "milevadb-server", caCert, caKey, "/tmp/server-key-reload3.pem", "/tmp/server-cert-reload3.pem", func(c *x509.Certificate) {
		c.NotBefore = time.Now().Add(-24 * time.Hour).UTC()
		c.NotAfter = c.NotBefore.Add(1 * time.Hour).UTC()
	})
	c.Assert(err, IsNil)
	os.Rename("/tmp/server-key-reload3.pem", "/tmp/server-key-reload.pem")
	os.Rename("/tmp/server-cert-reload3.pem", "/tmp/server-cert-reload.pem")
	connOverrider = func(config *allegrosql.Config) {
		config.TLSConfig = "skip-verify"
	}
	err = cli.runReloadTLS(c, connOverrider, false)
	c.Assert(err, IsNil)
	connOverrider = func(config *allegrosql.Config) {
		config.TLSConfig = "client-certificate-reload"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, NotNil)
	c.Assert(soliton.IsTLSExpiredError(err), IsTrue, Commentf("real error is %+v", err))
	server.Close()
}

func (ts *milevadbTestSerialSuite) TestErrorNoRollback(c *C) {
	// Generate valid TLS certificates.
	caCert, caKey, err := generateCert(0, "MilevaDB CA", nil, nil, "/tmp/ca-key-rollback.pem", "/tmp/ca-cert-rollback.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(1, "milevadb-server", caCert, caKey, "/tmp/server-key-rollback.pem", "/tmp/server-cert-rollback.pem")
	c.Assert(err, IsNil)
	_, _, err = generateCert(2, "ALLEGROALLEGROSQL Client Certificate", caCert, caKey, "/tmp/client-key-rollback.pem", "/tmp/client-cert-rollback.pem")
	c.Assert(err, IsNil)
	err = registerTLSConfig("client-cert-rollback-test", "/tmp/ca-cert-rollback.pem", "/tmp/client-cert-rollback.pem", "/tmp/client-key-rollback.pem", "milevadb-server", true)
	c.Assert(err, IsNil)

	defer func() {
		os.Remove("/tmp/ca-key-rollback.pem")
		os.Remove("/tmp/ca-cert-rollback.pem")

		os.Remove("/tmp/server-key-rollback.pem")
		os.Remove("/tmp/server-cert-rollback.pem")
		os.Remove("/tmp/client-key-rollback.pem")
		os.Remove("/tmp/client-cert-rollback.pem")
	}()

	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = cli.port
	cfg.Status.ReportStatus = false

	cfg.Security = config.Security{
		RequireSecureTransport: true,
		SSLCA:                  "wrong path",
		SSLCert:                "wrong path",
		SSLKey:                 "wrong path",
	}
	_, err = NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, NotNil)

	// test reload tls fail with/without "error no rollback option"
	cfg.Security = config.Security{
		SSLCA:   "/tmp/ca-cert-rollback.pem",
		SSLCert: "/tmp/server-cert-rollback.pem",
		SSLKey:  "/tmp/server-key-rollback.pem",
	}
	server, err := NewServer(cfg, ts.milevadbdrv)
	c.Assert(err, IsNil)
	cli.port = getPortFromTCPAddr(server.listener.Addr())
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	connOverrider := func(config *allegrosql.Config) {
		config.TLSConfig = "client-cert-rollback-test"
	}
	err = cli.runTestTLSConnection(c, connOverrider)
	c.Assert(err, IsNil)
	os.Remove("/tmp/server-key-rollback.pem")
	err = cli.runReloadTLS(c, connOverrider, false)
	c.Assert(err, NotNil)
	tlsCfg := server.getTLSConfig()
	c.Assert(tlsCfg, NotNil)
	err = cli.runReloadTLS(c, connOverrider, true)
	c.Assert(err, IsNil)
	tlsCfg = server.getTLSConfig()
	c.Assert(tlsCfg, IsNil)
}

func (ts *milevadbTestSuite) TestClientWithDefCauslation(c *C) {
	c.Parallel()
	ts.runTestClientWithDefCauslation(c)
}

func (ts *milevadbTestSuite) TestCreateBlockFlen(c *C) {
	// issue #4540
	qctx, err := ts.milevadbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultDefCauslationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = InterDircute(context.Background(), qctx, "use test;")
	c.Assert(err, IsNil)

	ctx := context.Background()
	testALLEGROSQL := "CREATE TABLE `t1` (" +
		"`a` char(36) NOT NULL," +
		"`b` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`d` varchar(50) DEFAULT ''," +
		"`e` char(36) NOT NULL DEFAULT ''," +
		"`f` char(36) NOT NULL DEFAULT ''," +
		"`g` char(1) NOT NULL DEFAULT 'N'," +
		"`h` varchar(100) NOT NULL," +
		"`i` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`j` varchar(10) DEFAULT ''," +
		"`k` varchar(10) DEFAULT ''," +
		"`l` varchar(20) DEFAULT ''," +
		"`m` varchar(20) DEFAULT ''," +
		"`n` varchar(30) DEFAULT ''," +
		"`o` varchar(100) DEFAULT ''," +
		"`p` varchar(50) DEFAULT ''," +
		"`q` varchar(50) DEFAULT ''," +
		"`r` varchar(100) DEFAULT ''," +
		"`s` varchar(20) DEFAULT ''," +
		"`t` varchar(50) DEFAULT ''," +
		"`u` varchar(100) DEFAULT ''," +
		"`v` varchar(50) DEFAULT ''," +
		"`w` varchar(300) NOT NULL," +
		"`x` varchar(250) DEFAULT ''," +
		"`y` decimal(20)," +
		"`z` decimal(20, 4)," +
		"PRIMARY KEY (`a`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	_, err = InterDircute(ctx, qctx, testALLEGROSQL)
	c.Assert(err, IsNil)
	rs, err := InterDircute(ctx, qctx, "show create causet t1")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	defcaus := rs.DeferredCausets()
	c.Assert(err, IsNil)
	c.Assert(len(defcaus), Equals, 2)
	c.Assert(int(defcaus[0].DeferredCausetLength), Equals, 5*tmysql.MaxBytesOfCharacter)
	c.Assert(int(defcaus[1].DeferredCausetLength), Equals, len(req.GetRow(0).GetString(1))*tmysql.MaxBytesOfCharacter)

	// for issue#5246
	rs, err = InterDircute(ctx, qctx, "select y, z from t1")
	c.Assert(err, IsNil)
	defcaus = rs.DeferredCausets()
	c.Assert(len(defcaus), Equals, 2)
	c.Assert(int(defcaus[0].DeferredCausetLength), Equals, 21)
	c.Assert(int(defcaus[1].DeferredCausetLength), Equals, 22)
}

func InterDircute(ctx context.Context, qc *MilevaDBContext, allegrosql string) (ResultSet, error) {
	stmts, err := qc.Parse(ctx, allegrosql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		panic("wrong input for InterDircute: " + allegrosql)
	}
	return qc.InterDircuteStmt(ctx, stmts[0])
}

func (ts *milevadbTestSuite) TestShowBlocksFlen(c *C) {
	qctx, err := ts.milevadbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultDefCauslationID), "test", nil)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = InterDircute(ctx, qctx, "use test;")
	c.Assert(err, IsNil)

	testALLEGROSQL := "create causet abcdefghijklmnopqrstuvwxyz (i int)"
	_, err = InterDircute(ctx, qctx, testALLEGROSQL)
	c.Assert(err, IsNil)
	rs, err := InterDircute(ctx, qctx, "show blocks")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	defcaus := rs.DeferredCausets()
	c.Assert(err, IsNil)
	c.Assert(len(defcaus), Equals, 1)
	c.Assert(int(defcaus[0].DeferredCausetLength), Equals, 26*tmysql.MaxBytesOfCharacter)
}

func checkDefCausNames(c *C, defCausumns []*DeferredCausetInfo, names ...string) {
	for i, name := range names {
		c.Assert(defCausumns[i].Name, Equals, name)
		c.Assert(defCausumns[i].OrgName, Equals, name)
	}
}

func (ts *milevadbTestSuite) TestFieldList(c *C) {
	qctx, err := ts.milevadbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultDefCauslationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = InterDircute(context.Background(), qctx, "use test;")
	c.Assert(err, IsNil)

	ctx := context.Background()
	testALLEGROSQL := `create causet t (
		c_bit bit(10),
		c_int_d int,
		c_bigint_d bigint,
		c_float_d float,
		c_double_d double,
		c_decimal decimal(6, 3),
		c_datetime datetime(2),
		c_time time(3),
		c_date date,
		c_timestamp timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),
		c_char char(20),
		c_varchar varchar(20),
		c_text_d text,
		c_binary binary(20),
		c_blob_d blob,
		c_set set('a', 'b', 'c'),
		c_enum enum('a', 'b', 'c'),
		c_json JSON,
		c_year year
	)`
	_, err = InterDircute(ctx, qctx, testALLEGROSQL)
	c.Assert(err, IsNil)
	defCausInfos, err := qctx.FieldList("t")
	c.Assert(err, IsNil)
	c.Assert(len(defCausInfos), Equals, 19)

	checkDefCausNames(c, defCausInfos, "c_bit", "c_int_d", "c_bigint_d", "c_float_d",
		"c_double_d", "c_decimal", "c_datetime", "c_time", "c_date", "c_timestamp",
		"c_char", "c_varchar", "c_text_d", "c_binary", "c_blob_d", "c_set", "c_enum",
		"c_json", "c_year")

	for _, defcaus := range defCausInfos {
		c.Assert(defcaus.Schema, Equals, "test")
	}

	for _, defcaus := range defCausInfos {
		c.Assert(defcaus.Block, Equals, "t")
	}

	for i, defCaus := range defCausInfos {
		switch i {
		case 10, 11, 12, 15, 16:
			// c_char char(20), c_varchar varchar(20), c_text_d text,
			// c_set set('a', 'b', 'c'), c_enum enum('a', 'b', 'c')
			c.Assert(defCaus.Charset, Equals, uint16(tmysql.CharsetNameToID(tmysql.DefaultCharset)), Commentf("index %d", i))
			continue
		}

		c.Assert(defCaus.Charset, Equals, uint16(tmysql.CharsetNameToID("binary")), Commentf("index %d", i))
	}

	// c_decimal decimal(6, 3)
	c.Assert(defCausInfos[5].Decimal, Equals, uint8(3))

	// for issue#10513
	tooLongDeferredCausetAsName := "COALESCE(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)"
	defCausumnAsName := tooLongDeferredCausetAsName[:tmysql.MaxAliasIdentifierLen]

	rs, err := InterDircute(ctx, qctx, "select "+tooLongDeferredCausetAsName)
	c.Assert(err, IsNil)
	defcaus := rs.DeferredCausets()
	c.Assert(defcaus[0].OrgName, Equals, tooLongDeferredCausetAsName)
	c.Assert(defcaus[0].Name, Equals, defCausumnAsName)

	rs, err = InterDircute(ctx, qctx, "select c_bit as '"+tooLongDeferredCausetAsName+"' from t")
	c.Assert(err, IsNil)
	defcaus = rs.DeferredCausets()
	c.Assert(defcaus[0].OrgName, Equals, "c_bit")
	c.Assert(defcaus[0].Name, Equals, defCausumnAsName)
}

func (ts *milevadbTestSuite) TestSumAvg(c *C) {
	c.Parallel()
	ts.runTestSumAvg(c)
}

func (ts *milevadbTestSuite) TestNullFlag(c *C) {
	// issue #9689
	qctx, err := ts.milevadbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultDefCauslationID), "test", nil)
	c.Assert(err, IsNil)

	ctx := context.Background()
	rs, err := InterDircute(ctx, qctx, "select 1")
	c.Assert(err, IsNil)
	defcaus := rs.DeferredCausets()
	c.Assert(len(defcaus), Equals, 1)
	expectFlag := uint16(tmysql.NotNullFlag | tmysql.BinaryFlag)
	c.Assert(dumpFlag(defcaus[0].Type, defcaus[0].Flag), Equals, expectFlag)
}
