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
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	BerolinaSQLtypes "github.com/whtcorpsinc/BerolinaSQL/types"
	pumpcli "github.com/whtcorpsinc/milevadb-tools/milevadb-binlog/pump_client"
	"github.com/whtcorpsinc/milevadb/bindinfo"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/executor"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/privilege/privileges"
	"github.com/whtcorpsinc/milevadb/server"
	"github.com/whtcorpsinc/milevadb/stochastik"
	"github.com/whtcorpsinc/milevadb/stochastikctx/binloginfo"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/statistics"
	kvstore "github.com/whtcorpsinc/milevadb/causetstore"
	"github.com/whtcorpsinc/milevadb/causetstore/mockstore"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb/gcworker"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/soliton/disk"
	"github.com/whtcorpsinc/milevadb/soliton/petriutil"
	"github.com/whtcorpsinc/milevadb/soliton/kvcache"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/printer"
	"github.com/whtcorpsinc/milevadb/soliton/profile"
	"github.com/whtcorpsinc/milevadb/soliton/signal"
	"github.com/whtcorpsinc/milevadb/soliton/storeutil"
	"github.com/whtcorpsinc/milevadb/soliton/sys/linux"
	storageSys "github.com/whtcorpsinc/milevadb/soliton/sys/storage"
	"github.com/whtcorpsinc/milevadb/soliton/systimemon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	fidel "github.com/einsteindb/fidel/client"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
	"google.golang.org/grpc/grpclog"
)

// Flag Names
const (
	nmVersion                = "V"
	nmConfig                 = "config"
	nmConfigCheck            = "config-check"
	nmConfigStrict           = "config-strict"
	nmStore                  = "causetstore"
	nmStorePath              = "path"
	nmHost                   = "host"
	nmAdvertiseAddress       = "advertise-address"
	nmPort                   = "P"
	nmCors                   = "cors"
	nmSocket                 = "socket"
	nmEnableBinlog           = "enable-binlog"
	nmRunDBS                 = "run-dbs"
	nmLogLevel               = "L"
	nmLogFile                = "log-file"
	nmLogSlowQuery           = "log-slow-query"
	nmReportStatus           = "report-status"
	nmStatusHost             = "status-host"
	nmStatusPort             = "status"
	nmMetricsAddr            = "metrics-addr"
	nmMetricsInterval        = "metrics-interval"
	nmDdlLease               = "lease"
	nmTokenLimit             = "token-limit"
	nmPluginDir              = "plugin-dir"
	nmPluginLoad             = "plugin-load"
	nmRepairMode             = "repair-mode"
	nmRepairList             = "repair-list"
	nmRequireSecureTransport = "require-secure-transport"

	nmProxyProtocolNetworks      = "proxy-protocol-networks"
	nmProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
	nmAffinityCPU                = "affinity-cpus"
)

var (
	version      = flagBoolean(nmVersion, false, "print version information and exit")
	configPath   = flag.String(nmConfig, "", "config file path")
	configCheck  = flagBoolean(nmConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(nmConfigStrict, false, "enforce config file validity")

	// Base
	causetstore            = flag.String(nmStore, "entangledstore", "registered causetstore name, [einsteindb, mockeinsteindb, entangledstore]")
	storePath        = flag.String(nmStorePath, "/tmp/milevadb", "milevadb storage path")
	host             = flag.String(nmHost, "0.0.0.0", "milevadb server host")
	advertiseAddress = flag.String(nmAdvertiseAddress, "", "milevadb server advertise IP")
	port             = flag.String(nmPort, "4000", "milevadb server port")
	cors             = flag.String(nmCors, "", "milevadb server allow cors origin")
	socket           = flag.String(nmSocket, "", "The socket file to use for connection.")
	enableBinlog     = flagBoolean(nmEnableBinlog, false, "enable generate binlog")
	runDBS           = flagBoolean(nmRunDBS, true, "run dbs worker on this milevadb-server")
	dbsLease         = flag.String(nmDdlLease, "45s", "schemaReplicant lease duration, very dangerous to change only if you know what you do")
	tokenLimit       = flag.Int(nmTokenLimit, 1000, "the limit of concurrent executed stochastik")
	pluginDir        = flag.String(nmPluginDir, "/data/deploy/plugin", "the folder that hold plugin")
	pluginLoad       = flag.String(nmPluginLoad, "", "wait load plugin name(separated by comma)")
	affinityCPU      = flag.String(nmAffinityCPU, "", "affinity cpu (cpu-no. separated by comma, e.g. 1,2,3)")
	repairMode       = flagBoolean(nmRepairMode, false, "enable admin repair mode")
	repairList       = flag.String(nmRepairList, "", "admin repair block list")
	requireTLS       = flag.Bool(nmRequireSecureTransport, false, "require client use secure transport")

	// Log
	logLevel     = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile      = flag.String(nmLogFile, "", "log file path")
	logSlowQuery = flag.String(nmLogSlowQuery, "", "slow query file path")

	// Status
	reportStatus    = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusHost      = flag.String(nmStatusHost, "0.0.0.0", "milevadb server status host")
	statusPort      = flag.String(nmStatusPort, "10080", "milevadb server status port")
	metricsAddr     = flag.String(nmMetricsAddr, "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Uint(nmMetricsInterval, 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")

	// PROXY Protocol
	proxyProtocolNetworks      = flag.String(nmProxyProtocolNetworks, "", "proxy protocol networks allowed IP or *, empty mean disable proxy protocol support")
	proxyProtocolHeaderTimeout = flag.Uint(nmProxyProtocolHeaderTimeout, 5, "proxy protocol header read timeout, unit is second.")
)

var (
	storage  ekv.CausetStorage
	dom      *petri.Petri
	svr      *server.Server
	graceful bool
)

func main() {
	flag.Parse()
	if *version {
		fmt.Println(printer.GetMilevaDBInfo())
		os.Exit(0)
	}
	registerStores()
	registerMetrics()
	config.InitializeConfig(*configPath, *configCheck, *configStrict, reloadConfig, overrideConfig)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		config.GetGlobalConfig().UFIDelateTempStoragePath()
		err := disk.InitializeTemFIDelir()
		terror.MustNil(err)
		checkTempStorageQuota()
	}
	setGlobalVars()
	setCPUAffinity()
	setupLog()
	setHeapProfileTracker()
	setupTracing() // Should before createServer and after setup config.
	printInfo()
	setupBinlogClient()
	setupMetrics()
	createStoreAndPetri()
	createServer()
	signal.SetupSignalHandler(serverShutdown)
	runServer()
	cleanup()
	syncLog()
}

func exit() {
	syncLog()
	os.Exit(0)
}

func syncLog() {
	if err := log.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, "sync log err:", err)
		os.Exit(1)
	}
}

func checkTempStorageQuota() {
	// check capacity and the quota when OOMUseTmpStorage is enabled
	c := config.GetGlobalConfig()
	if c.TempStorageQuota < 0 {
		// means unlimited, do nothing
	} else {
		capacityByte, err := storageSys.GetTargetDirectoryCapacity(c.TempStoragePath)
		if err != nil {
			log.Fatal(err.Error())
		} else if capacityByte < uint64(c.TempStorageQuota) {
			log.Fatal(fmt.Sprintf("value of [tmp-storage-quota](%d byte) exceeds the capacity(%d byte) of the [%s] directory", c.TempStorageQuota, capacityByte, c.TempStoragePath))
		}
	}
}

func setCPUAffinity() {
	if affinityCPU == nil || len(*affinityCPU) == 0 {
		return
	}
	var cpu []int
	for _, af := range strings.Split(*affinityCPU, ",") {
		af = strings.TrimSpace(af)
		if len(af) > 0 {
			c, err := strconv.Atoi(af)
			if err != nil {
				fmt.Fprintf(os.Stderr, "wrong affinity cpu config: %s", *affinityCPU)
				exit()
			}
			cpu = append(cpu, c)
		}
	}
	err := linux.SetAffinity(cpu)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set cpu affinity failure: %v", err)
		exit()
	}
	runtime.GOMAXPROCS(len(cpu))
	metrics.MaxProcs.Set(float64(runtime.GOMAXPROCS(0)))
}

func setHeapProfileTracker() {
	c := config.GetGlobalConfig()
	d := parseDuration(c.Performance.MemProfileInterval)
	go profile.HeapProfileForGlobalMemTracker(d)
}

func registerStores() {
	err := kvstore.Register("einsteindb", einsteindb.Driver{})
	terror.MustNil(err)
	einsteindb.NewGCHandlerFunc = gcworker.NewGCWorker
	err = kvstore.Register("mockeinsteindb", mockstore.MockEinsteinDBDriver{})
	terror.MustNil(err)
	err = kvstore.Register("entangledstore", mockstore.EmbedEntangledStoreDriver{})
	terror.MustNil(err)
}

func registerMetrics() {
	metrics.RegisterMetrics()
}

func createStoreAndPetri() {
	cfg := config.GetGlobalConfig()
	fullPath := fmt.Sprintf("%s://%s", cfg.CausetStore, cfg.Path)
	var err error
	storage, err = kvstore.New(fullPath)
	terror.MustNil(err)
	// Bootstrap a stochastik to load information schemaReplicant.
	dom, err = stochastik.BootstrapStochastik(storage)
	terror.MustNil(err)
}

func setupBinlogClient() {
	cfg := config.GetGlobalConfig()
	if !cfg.Binlog.Enable {
		return
	}

	if cfg.Binlog.IgnoreError {
		binloginfo.SetIgnoreError(true)
	}

	var (
		client *pumpcli.PumpsClient
		err    error
	)

	securityOption := fidel.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}

	if len(cfg.Binlog.BinlogSocket) == 0 {
		client, err = pumpcli.NewPumpsClient(cfg.Path, cfg.Binlog.Strategy, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
	} else {
		client, err = pumpcli.NewLocalPumpsClient(cfg.Path, cfg.Binlog.BinlogSocket, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
	}

	terror.MustNil(err)

	err = pumpcli.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	binloginfo.SetPumpsClient(client)
	log.Info("milevadb-server", zap.Bool("create pumps client success, ignore binlog error", cfg.Binlog.IgnoreError))
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// pushMetric pushes metrics in background.
func pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Info("disable Prometheus push client")
		return
	}
	log.Info("start prometheus push client", zap.String("server addr", addr), zap.String("interval", interval.String()))
	go prometheusPushClient(addr, interval)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: MilevaDB do not have uniq name, so we use host+port to compose a name.
	job := "milevadb"
	pusher := push.New(addr, job)
	pusher = pusher.Gatherer(prometheus.DefaultGatherer)
	pusher = pusher.Grouping("instance", instanceName())
	for {
		err := pusher.Push()
		if err != nil {
			log.Error("could not push metrics to prometheus pushgateway", zap.String("err", err.Error()))
		}
		time.Sleep(interval)
	}
}

func instanceName() string {
	cfg := config.GetGlobalConfig()
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%d", hostname, cfg.Port)
}

// parseDuration parses lease argument string.
func parseDuration(lease string) time.Duration {
	dur, err := time.ParseDuration(lease)
	if err != nil {
		dur, err = time.ParseDuration(lease + "s")
	}
	if err != nil || dur < 0 {
		log.Fatal("invalid lease duration", zap.String("lease", lease))
	}
	return dur
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

func reloadConfig(nc, c *config.Config) {
	// Just a part of config items need to be reload explicitly.
	// Some of them like OOMCausetAction are always used by getting from global config directly
	// like config.GetGlobalConfig().OOMCausetAction.
	// These config items will become available naturally after the global config pointer
	// is uFIDelated in function ReloadGlobalConfig.
	if nc.Performance.ServerMemoryQuota != c.Performance.ServerMemoryQuota {
		plannercore.PreparedPlanCacheMaxMemory.CausetStore(nc.Performance.ServerMemoryQuota)
	}
	if nc.Performance.CrossJoin != c.Performance.CrossJoin {
		plannercore.AllowCartesianProduct.CausetStore(nc.Performance.CrossJoin)
	}
	if nc.Performance.FeedbackProbability != c.Performance.FeedbackProbability {
		statistics.FeedbackProbability.CausetStore(nc.Performance.FeedbackProbability)
	}
	if nc.Performance.QueryFeedbackLimit != c.Performance.QueryFeedbackLimit {
		statistics.MaxQueryFeedbackCount.CausetStore(int64(nc.Performance.QueryFeedbackLimit))
	}
	if nc.Performance.PseudoEstimateRatio != c.Performance.PseudoEstimateRatio {
		statistics.RatioOfPseudoEstimate.CausetStore(nc.Performance.PseudoEstimateRatio)
	}
	if nc.Performance.MaxProcs != c.Performance.MaxProcs {
		runtime.GOMAXPROCS(int(nc.Performance.MaxProcs))
		metrics.MaxProcs.Set(float64(runtime.GOMAXPROCS(0)))
	}
	if nc.EinsteinDBClient.StoreLimit != c.EinsteinDBClient.StoreLimit {
		storeutil.StoreLimit.CausetStore(nc.EinsteinDBClient.StoreLimit)
	}

	if nc.PreparedPlanCache.Enabled != c.PreparedPlanCache.Enabled {
		plannercore.SetPreparedPlanCache(nc.PreparedPlanCache.Enabled)
	}
	if nc.Log.Level != c.Log.Level {
		if err := logutil.SetLevel(nc.Log.Level); err != nil {
			logutil.BgLogger().Error("uFIDelate log level error", zap.Error(err))
		}
	}
}

// overrideConfig considers command arguments and overrides some config items in the Config.
func overrideConfig(cfg *config.Config) {
	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})

	// Base
	if actualFlags[nmHost] {
		cfg.Host = *host
	}
	if actualFlags[nmAdvertiseAddress] {
		cfg.AdvertiseAddress = *advertiseAddress
	}
	if len(cfg.AdvertiseAddress) == 0 {
		cfg.AdvertiseAddress = soliton.GetLocalIP()
	}
	if len(cfg.AdvertiseAddress) == 0 {
		cfg.AdvertiseAddress = cfg.Host
	}
	var err error
	if actualFlags[nmPort] {
		var p int
		p, err = strconv.Atoi(*port)
		terror.MustNil(err)
		cfg.Port = uint(p)
	}
	if actualFlags[nmCors] {
		fmt.Println(cors)
		cfg.Cors = *cors
	}
	if actualFlags[nmStore] {
		cfg.CausetStore = *causetstore
	}
	if actualFlags[nmStorePath] {
		cfg.Path = *storePath
	}
	if actualFlags[nmSocket] {
		cfg.Socket = *socket
	}
	if actualFlags[nmEnableBinlog] {
		cfg.Binlog.Enable = *enableBinlog
	}
	if actualFlags[nmRunDBS] {
		cfg.RunDBS = *runDBS
	}
	if actualFlags[nmDdlLease] {
		cfg.Lease = *dbsLease
	}
	if actualFlags[nmTokenLimit] {
		cfg.TokenLimit = uint(*tokenLimit)
	}
	if actualFlags[nmPluginLoad] {
		cfg.Plugin.Load = *pluginLoad
	}
	if actualFlags[nmPluginDir] {
		cfg.Plugin.Dir = *pluginDir
	}
	if actualFlags[nmRequireSecureTransport] {
		cfg.Security.RequireSecureTransport = *requireTLS
	}
	if actualFlags[nmRepairMode] {
		cfg.RepairMode = *repairMode
	}
	if actualFlags[nmRepairList] {
		if cfg.RepairMode {
			cfg.RepairTableList = stringToList(*repairList)
		}
	}

	// Log
	if actualFlags[nmLogLevel] {
		cfg.Log.Level = *logLevel
	}
	if actualFlags[nmLogFile] {
		cfg.Log.File.Filename = *logFile
	}
	if actualFlags[nmLogSlowQuery] {
		cfg.Log.SlowQueryFile = *logSlowQuery
	}

	// Status
	if actualFlags[nmReportStatus] {
		cfg.Status.ReportStatus = *reportStatus
	}
	if actualFlags[nmStatusHost] {
		cfg.Status.StatusHost = *statusHost
	}
	if actualFlags[nmStatusPort] {
		var p int
		p, err = strconv.Atoi(*statusPort)
		terror.MustNil(err)
		cfg.Status.StatusPort = uint(p)
	}
	if actualFlags[nmMetricsAddr] {
		cfg.Status.MetricsAddr = *metricsAddr
	}
	if actualFlags[nmMetricsInterval] {
		cfg.Status.MetricsInterval = *metricsInterval
	}

	// PROXY Protocol
	if actualFlags[nmProxyProtocolNetworks] {
		cfg.ProxyProtocol.Networks = *proxyProtocolNetworks
	}
	if actualFlags[nmProxyProtocolHeaderTimeout] {
		cfg.ProxyProtocol.HeaderTimeout = *proxyProtocolHeaderTimeout
	}
}

func setGlobalVars() {
	cfg := config.GetGlobalConfig()

	// Disable automaxprocs log
	nopLog := func(string, ...interface{}) {}
	_, err := maxprocs.Set(maxprocs.Logger(nopLog))
	terror.MustNil(err)
	// We should respect to user's settings in config file.
	// The default value of MaxProcs is 0, runtime.GOMAXPROCS(0) is no-op.
	runtime.GOMAXPROCS(int(cfg.Performance.MaxProcs))
	metrics.MaxProcs.Set(float64(runtime.GOMAXPROCS(0)))

	dbsLeaseDuration := parseDuration(cfg.Lease)
	stochastik.SetSchemaLease(dbsLeaseDuration)
	statsLeaseDuration := parseDuration(cfg.Performance.StatsLease)
	stochastik.SetStatsLease(statsLeaseDuration)
	indexUsageSyncLeaseDuration := parseDuration(cfg.Performance.IndexUsageSyncLease)
	stochastik.SetIndexUsageSyncLease(indexUsageSyncLeaseDuration)
	bindinfo.Lease = parseDuration(cfg.Performance.BindInfoLease)
	petri.RunAutoAnalyze = cfg.Performance.RunAutoAnalyze
	statistics.FeedbackProbability.CausetStore(cfg.Performance.FeedbackProbability)
	statistics.MaxQueryFeedbackCount.CausetStore(int64(cfg.Performance.QueryFeedbackLimit))
	statistics.RatioOfPseudoEstimate.CausetStore(cfg.Performance.PseudoEstimateRatio)
	dbs.RunWorker = cfg.RunDBS
	if cfg.SplitTable {
		atomic.StoreUint32(&dbs.EnableSplitTableRegion, 1)
	}
	plannercore.AllowCartesianProduct.CausetStore(cfg.Performance.CrossJoin)
	privileges.SkipWithGrant = cfg.Security.SkipGrantTable
	ekv.TxnTotalSizeLimit = cfg.Performance.TxnTotalSizeLimit
	if cfg.Performance.TxnEntrySizeLimit > 120*1024*1024 {
		log.Fatal("cannot set txn entry size limit larger than 120M")
	}
	ekv.TxnEntrySizeLimit = cfg.Performance.TxnEntrySizeLimit

	priority := allegrosql.Str2Priority(cfg.Performance.ForcePriority)
	variable.ForcePriority = int32(priority)
	variable.SysVars[variable.MilevaDBForcePriority].Value = allegrosql.Priority2Str[priority]
	variable.SysVars[variable.MilevaDBOptDistinctAggPushDown].Value = variable.BoolToIntStr(cfg.Performance.DistinctAggPushDown)

	variable.SysVars[variable.MilevaDBMemQuotaQuery].Value = strconv.FormatInt(cfg.MemQuotaQuery, 10)
	variable.SysVars["lower_case_block_names"].Value = strconv.Itoa(cfg.LowerCaseTableNames)
	variable.SysVars[variable.LogBin].Value = variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)

	variable.SysVars[variable.Port].Value = fmt.Sprintf("%d", cfg.Port)
	variable.SysVars[variable.Socket].Value = cfg.Socket
	variable.SysVars[variable.DataDir].Value = cfg.Path
	variable.SysVars[variable.MilevaDBSlowQueryFile].Value = cfg.Log.SlowQueryFile
	variable.SysVars[variable.MilevaDBIsolationReadEngines].Value = strings.Join(cfg.IsolationRead.Engines, ", ")

	// For CI environment we default enable prepare-plan-cache.
	plannercore.SetPreparedPlanCache(config.CheckTableBeforeDrop || cfg.PreparedPlanCache.Enabled)
	if plannercore.PreparedPlanCacheEnabled() {
		plannercore.PreparedPlanCacheCapacity = cfg.PreparedPlanCache.Capacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = cfg.PreparedPlanCache.MemoryGuardRatio
		if plannercore.PreparedPlanCacheMemoryGuardRatio < 0.0 || plannercore.PreparedPlanCacheMemoryGuardRatio > 1.0 {
			plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		}
		plannercore.PreparedPlanCacheMaxMemory.CausetStore(cfg.Performance.ServerMemoryQuota)
		total, err := memory.MemTotal()
		terror.MustNil(err)
		if plannercore.PreparedPlanCacheMaxMemory.Load() > total || plannercore.PreparedPlanCacheMaxMemory.Load() <= 0 {
			plannercore.PreparedPlanCacheMaxMemory.CausetStore(total)
		}
	}

	atomic.StoreUint64(&einsteindb.CommitMaxBackoff, uint64(parseDuration(cfg.EinsteinDBClient.CommitTimeout).Seconds()*1000))
	einsteindb.RegionCacheTTLSec = int64(cfg.EinsteinDBClient.RegionCacheTTL)
	petriutil.RepairInfo.SetRepairMode(cfg.RepairMode)
	petriutil.RepairInfo.SetRepairTableList(cfg.RepairTableList)
	c := config.GetGlobalConfig()
	executor.GlobalDiskUsageTracker.SetBytesLimit(c.TempStorageQuota)
	if c.Performance.ServerMemoryQuota < 1 {
		// If MaxMemory equals 0, it means unlimited
		executor.GlobalMemoryUsageTracker.SetBytesLimit(-1)
	} else {
		executor.GlobalMemoryUsageTracker.SetBytesLimit(int64(c.Performance.ServerMemoryQuota))
	}
	kvcache.GlobalLRUMemUsageTracker.AttachToGlobalTracker(executor.GlobalMemoryUsageTracker)

	t, err := time.ParseDuration(cfg.EinsteinDBClient.StoreLivenessTimeout)
	if err != nil {
		logutil.BgLogger().Fatal("invalid duration value for causetstore-liveness-timeout",
			zap.String("currentValue", config.GetGlobalConfig().EinsteinDBClient.StoreLivenessTimeout))
	}
	einsteindb.StoreLivenessTimeout = t
	BerolinaSQLtypes.MilevaDBStrictIntegerDisplayWidth = config.GetGlobalConfig().DeprecateIntegerDisplayWidth
}

func setupLog() {
	cfg := config.GetGlobalConfig()
	err := logutil.InitZapLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	err = logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	if len(os.Getenv("GRPC_DEBUG")) > 0 {
		grpclog.SetLoggerV2(grpclog.NewLoggerV2WithVerbosity(os.Stderr, os.Stderr, os.Stderr, 999))
	} else {
		grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, os.Stderr))
	}
	// trigger internal http(s) client init.
	soliton.InternalHTTPClient()
}

func printInfo() {
	// Make sure the MilevaDB info is always printed.
	level := log.GetLevel()
	log.SetLevel(zap.InfoLevel)
	printer.PrintMilevaDBInfo()
	log.SetLevel(level)
}

func createServer() {
	cfg := config.GetGlobalConfig()
	driver := server.NewMilevaDBDriver(storage)
	var err error
	svr, err = server.NewServer(cfg, driver)
	// Both petri and storage have started, so we have to clean them before exiting.
	terror.MustNil(err, closePetriAndStorage)
	svr.SetPetri(dom)
	go dom.ExpensiveQueryHandle().SetStochastikManager(svr).Run()
	dom.InfoSyncer().SetStochastikManager(svr)
}

func serverShutdown(isgraceful bool) {
	if isgraceful {
		graceful = true
	}
	svr.Close()
}

func setupMetrics() {
	cfg := config.GetGlobalConfig()
	// Enable the mutex profile, 1/10 of mutex blocking event sampling.
	runtime.SetMutexProfileFraction(10)
	systimeErrHandler := func() {
		metrics.TimeJumpBackCounter.Inc()
	}
	callBackCount := 0
	sucessCallBack := func() {
		callBackCount++
		// It is callback by monitor per second, we increase metrics.KeepAliveCounter per 5s.
		if callBackCount >= 5 {
			callBackCount = 0
			metrics.KeepAliveCounter.Inc()
		}
	}
	go systimemon.StartMonitor(time.Now, systimeErrHandler, sucessCallBack)

	pushMetric(cfg.Status.MetricsAddr, time.Duration(cfg.Status.MetricsInterval)*time.Second)
}

func setupTracing() {
	cfg := config.GetGlobalConfig()
	tracingCfg := cfg.OpenTracing.ToTracingConfig()
	tracingCfg.ServiceName = "MilevaDB"
	tracer, _, err := tracingCfg.NewTracer()
	if err != nil {
		log.Fatal("setup jaeger tracer failed", zap.String("error message", err.Error()))
	}
	opentracing.SetGlobalTracer(tracer)
}

func runServer() {
	err := svr.Run()
	terror.MustNil(err)
}

func closePetriAndStorage() {
	atomic.StoreUint32(&einsteindb.ShuttingDown, 1)
	dom.Close()
	err := storage.Close()
	terror.Log(errors.Trace(err))
}

func cleanup() {
	if graceful {
		svr.GracefulDown(context.Background(), nil)
	} else {
		svr.TryGracefulDown()
	}
	plugin.Shutdown(context.Background())
	closePetriAndStorage()
	disk.CleanUp()
}

func stringToList(repairString string) []string {
	if len(repairString) <= 0 {
		return []string{}
	}
	if repairString[0] == '[' && repairString[len(repairString)-1] == ']' {
		repairString = repairString[1 : len(repairString)-1]
	}
	return strings.FieldsFunc(repairString, func(r rune) bool {
		return r == ',' || r == ' ' || r == '"'
	})
}
