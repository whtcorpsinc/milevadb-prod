// Copyright 2020 The Go-MyALLEGROSQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2020 wandoulabs
// Copyright (c) 2020 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/auth"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	"github.com/whtcorpsinc/milevadb/petri"
	"github.com/whtcorpsinc/milevadb/interlock"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/metrics"
	causetcore "github.com/whtcorpsinc/milevadb/causet/core"
	"github.com/whtcorpsinc/milevadb/plugin"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"github.com/whtcorpsinc/milevadb/blockcodec"
	"github.com/whtcorpsinc/milevadb/soliton/memcam"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/memory"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)

var (
	queryTotalCountOk = [...]prometheus.Counter{
		allegrosql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "OK"),
		allegrosql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "OK"),
		allegrosql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "OK"),
		allegrosql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "OK"),
		allegrosql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "OK"),
		allegrosql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "OK"),
		allegrosql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "OK"),
		allegrosql.ComStmtInterDircute:      metrics.QueryTotalCounter.WithLabelValues("StmtInterDircute", "OK"),
		allegrosql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "OK"),
		allegrosql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "OK"),
		allegrosql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "OK"),
		allegrosql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "OK"),
		allegrosql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "OK"),
	}
	queryTotalCountErr = [...]prometheus.Counter{
		allegrosql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "Error"),
		allegrosql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "Error"),
		allegrosql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "Error"),
		allegrosql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "Error"),
		allegrosql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "Error"),
		allegrosql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "Error"),
		allegrosql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "Error"),
		allegrosql.ComStmtInterDircute:      metrics.QueryTotalCounter.WithLabelValues("StmtInterDircute", "Error"),
		allegrosql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "Error"),
		allegrosql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "Error"),
		allegrosql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "Error"),
		allegrosql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "Error"),
		allegrosql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "Error"),
	}

	queryDurationHistogramUse      = metrics.QueryDurationHistogram.WithLabelValues("Use")
	queryDurationHistogramShow     = metrics.QueryDurationHistogram.WithLabelValues("Show")
	queryDurationHistogramBegin    = metrics.QueryDurationHistogram.WithLabelValues("Begin")
	queryDurationHistogramCommit   = metrics.QueryDurationHistogram.WithLabelValues("Commit")
	queryDurationHistogramRollback = metrics.QueryDurationHistogram.WithLabelValues("Rollback")
	queryDurationHistogramInsert   = metrics.QueryDurationHistogram.WithLabelValues("Insert")
	queryDurationHistogramReplace  = metrics.QueryDurationHistogram.WithLabelValues("Replace")
	queryDurationHistogramDelete   = metrics.QueryDurationHistogram.WithLabelValues("Delete")
	queryDurationHistogramUFIDelate   = metrics.QueryDurationHistogram.WithLabelValues("UFIDelate")
	queryDurationHistogramSelect   = metrics.QueryDurationHistogram.WithLabelValues("Select")
	queryDurationHistogramInterDircute  = metrics.QueryDurationHistogram.WithLabelValues("InterDircute")
	queryDurationHistogramSet      = metrics.QueryDurationHistogram.WithLabelValues("Set")
	queryDurationHistogramGeneral  = metrics.QueryDurationHistogram.WithLabelValues(metrics.LblGeneral)

	disconnectNormal            = metrics.DisconnectionCounter.WithLabelValues(metrics.LblOK)
	disconnectByClientWithError = metrics.DisconnectionCounter.WithLabelValues(metrics.LblError)
	disconnectErrorUndetermined = metrics.DisconnectionCounter.WithLabelValues("undetermined")
)

// newClientConn creates a *clientConn object.
func newClientConn(s *Server) *clientConn {
	return &clientConn{
		server:       s,
		connectionID: atomic.AddUint32(&baseConnID, 1),
		defCauslation:    allegrosql.DefaultDefCauslationID,
		alloc:        memcam.NewSlabPredictor(32 * 1024),
		status:       connStatusDispatching,
	}
}

// clientConn represents a connection between server and client, it maintains connection specific state,
// handles client query.
type clientConn struct {
	pkt          *packetIO         // a helper to read and write data in packet format.
	bufReadConn  *bufferedReadConn // a buffered-read net.Conn or buffered-read tls.Conn.
	tlsConn      *tls.Conn         // TLS connection, nil if not TLS.
	server       *Server           // a reference of server instance.
	capability   uint32            // client capability affects the way server handles client request.
	connectionID uint32            // atomically allocated by a global variable, unique in process scope.
	user         string            // user of the client.
	dbname       string            // default database name.
	salt         []byte            // random bytes used for authentication.
	alloc        memcam.SlabPredictor   // an memory allocator for reducing memory allocation.
	lastPacket   []byte            // latest allegrosql query string, currently used for logging error.
	ctx          *MilevaDBContext      // an interface to execute allegrosql memexs.
	attrs        map[string]string // attributes parsed from client handshake response, not used for now.
	peerHost     string            // peer host
	peerPort     string            // peer port
	status       int32             // dispatching/reading/shutdown/waitshutdown
	lastCode     uint16            // last error code
	defCauslation    uint8             // defCauslation used by client, may be different from the defCauslation used by database.
}

func (cc *clientConn) String() string {
	defCauslationStr := allegrosql.DefCauslations[cc.defCauslation]
	return fmt.Sprintf("id:%d, addr:%s status:%b, defCauslation:%s, user:%s",
		cc.connectionID, cc.bufReadConn.RemoteAddr(), cc.ctx.Status(), defCauslationStr, cc.user,
	)
}

// authSwitchRequest is used when the client asked to speak something
// other than mysql_native_password. The server is allowed to ask
// the client to switch, so lets ask for mysql_native_password
// https://dev.allegrosql.com/doc/internals/en/connection-phase-packets.html#packet-ProtodefCaus::AuthSwitchRequest
func (cc *clientConn) authSwitchRequest(ctx context.Context) ([]byte, error) {
	enclen := 1 + len("mysql_native_password") + 1 + len(cc.salt) + 1
	data := cc.alloc.AllocWithLen(4, enclen)
	data = append(data, 0xfe) // switch request
	data = append(data, []byte("mysql_native_password")...)
	data = append(data, byte(0x00)) // requires null
	data = append(data, cc.salt...)
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return nil, err
	}
	err = cc.flush(ctx)
	if err != nil {
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return nil, err
	}
	resp, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Warn("authSwitchRequest response fail", zap.Error(err))
		}
		return nil, err
	}
	return resp, nil
}

// handshake works like TCP handshake, but in a higher level, it first writes initial packet to client,
// during handshake, client and server negotiate compatible features and do authentication.
// After handshake, client can send allegrosql query to server.
func (cc *clientConn) handshake(ctx context.Context) error {
	if err := cc.writeInitialHandshake(ctx); err != nil {
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("Could not send handshake due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("Write init handshake to client fail", zap.Error(errors.SuspendStack(err)))
		}
		return err
	}
	if err := cc.readOptionalSSLRequestAndHandshakeResponse(ctx); err != nil {
		err1 := cc.writeError(ctx, err)
		if err1 != nil {
			logutil.Logger(ctx).Debug("writeError failed", zap.Error(err1))
		}
		return err
	}
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, allegrosql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&allegrosql.ClientProtodefCaus41 > 0 {
		data = dumpUint16(data, allegrosql.ServerStatusAutocommit)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkt.sequence = 0
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("write response to client failed", zap.Error(err))
		return err
	}

	err = cc.flush(ctx)
	if err != nil {
		err = errors.SuspendStack(err)
		logutil.Logger(ctx).Debug("flush response to client failed", zap.Error(err))
		return err
	}
	return err
}

func (cc *clientConn) Close() error {
	cc.server.rwlock.Lock()
	delete(cc.server.clients, cc.connectionID)
	connections := len(cc.server.clients)
	cc.server.rwlock.Unlock()
	return closeConn(cc, connections)
}

func closeConn(cc *clientConn, connections int) error {
	metrics.ConnGauge.Set(float64(connections))
	err := cc.bufReadConn.Close()
	terror.Log(err)
	if cc.ctx != nil {
		return cc.ctx.Close()
	}
	return nil
}

func (cc *clientConn) closeWithoutLock() error {
	delete(cc.server.clients, cc.connectionID)
	return closeConn(cc, len(cc.server.clients))
}

// writeInitialHandshake sends server version, connection ID, server capability, defCauslation, server status
// and auth salt to the client.
func (cc *clientConn) writeInitialHandshake(ctx context.Context) error {
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, allegrosql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.connectionID), byte(cc.connectionID>>8), byte(cc.connectionID>>16), byte(cc.connectionID>>24))
	// auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability), byte(cc.server.capability>>8))
	// charset
	if cc.defCauslation == 0 {
		cc.defCauslation = uint8(allegrosql.DefaultDefCauslationID)
	}
	data = append(data, cc.defCauslation)
	// status
	data = dumpUint16(data, allegrosql.ServerStatusAutocommit)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(cc.server.capability>>16), byte(cc.server.capability>>24))
	// length of auth-plugin-data
	data = append(data, byte(len(cc.salt)+1))
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	data = append(data, 0)
	// auth-plugin name
	data = append(data, []byte("mysql_native_password")...)
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush(ctx)
}

func (cc *clientConn) readPacket() ([]byte, error) {
	return cc.pkt.readPacket()
}

func (cc *clientConn) writePacket(data []byte) error {
	failpoint.Inject("FakeClientConn", func() {
		if cc.pkt == nil {
			failpoint.Return(nil)
		}
	})
	return cc.pkt.writePacket(data)
}

// getStochastikVarsWaitTimeout get stochastik variable wait_timeout
func (cc *clientConn) getStochastikVarsWaitTimeout(ctx context.Context) uint64 {
	valStr, exists := cc.ctx.GetStochastikVars().GetSystemVar(variable.WaitTimeout)
	if !exists {
		return variable.DefWaitTimeout
	}
	waitTimeout, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		logutil.Logger(ctx).Warn("get sysval wait_timeout failed, use default value", zap.Error(err))
		// if get waitTimeout error, use default value
		return variable.DefWaitTimeout
	}
	return waitTimeout
}

type handshakeResponse41 struct {
	Capability uint32
	DefCauslation  uint8
	User       string
	DBName     string
	Auth       []byte
	AuthPlugin string
	Attrs      map[string]string
}

// parseOldHandshakeResponseHeader parses the old version handshake header HandshakeResponse320
func parseOldHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// https://dev.allegrosql.com/doc/internals/en/connection-phase-packets.html#packet-ProtodefCaus::HandshakeResponse320
	logutil.Logger(ctx).Debug("try to parse hanshake response as ProtodefCaus::HandshakeResponse320", zap.ByteString("packetData", data))
	if len(data) < 2+3 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, allegrosql.ErrMalformPacket
	}
	offset := 0
	// capability
	capability := binary.LittleEndian.Uint16(data[:2])
	packet.Capability = uint32(capability)

	// be compatible with ProtodefCaus::HandshakeResponse41
	packet.Capability = packet.Capability | allegrosql.ClientProtodefCaus41

	offset += 2
	// skip max packet size
	offset += 3
	// usa default CharsetID
	packet.DefCauslation = allegrosql.DefCauslationNames["utf8mb4_general_ci"]

	return offset, nil
}

// parseOldHandshakeResponseBody parse the HandshakeResponse for ProtodefCaus::HandshakeResponse320 (except the common header part).
func parseOldHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data), zap.Stack("stack"))
			err = allegrosql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&allegrosql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset = offset + idx + 1
		}
		if len(data[offset:]) > 0 {
			packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		}
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
	}

	return nil
}

// parseHandshakeResponseHeader parses the common header of SSLRequest and HandshakeResponse41.
func parseHandshakeResponseHeader(ctx context.Context, packet *handshakeResponse41, data []byte) (parsedBytes int, err error) {
	// Ensure there are enough data to read:
	// http://dev.allegrosql.com/doc/internals/en/connection-phase-packets.html#packet-ProtodefCaus::SSLRequest
	if len(data) < 4+4+1+23 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return 0, allegrosql.ErrMalformPacket
	}

	offset := 0
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	packet.Capability = capability
	offset += 4
	// skip max packet size
	offset += 4
	// charset, skip, if you want to use another charset, use set names
	packet.DefCauslation = data[offset]
	offset++
	// skip reserved 23[00]
	offset += 23

	return offset, nil
}

// parseHandshakeResponseBody parse the HandshakeResponse (except the common header part).
func parseHandshakeResponseBody(ctx context.Context, packet *handshakeResponse41, data []byte, offset int) (err error) {
	defer func() {
		// Check malformat packet cause out of range is disgusting, but don't panic!
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("handshake panic", zap.ByteString("packetData", data))
			err = allegrosql.ErrMalformPacket
		}
	}()
	// user name
	packet.User = string(data[offset : offset+bytes.IndexByte(data[offset:], 0)])
	offset += len(packet.User) + 1

	if packet.Capability&allegrosql.ClientPluginAuthLenencClientData > 0 {
		// MyALLEGROSQL client sets the wrong capability, it will set this bit even server doesn't
		// support ClientPluginAuthLenencClientData.
		// https://github.com/allegrosql/allegrosql-server/blob/5.7/allegrosql-common/client.c#L3478
		num, null, off := parseLengthEncodedInt(data[offset:])
		offset += off
		if !null {
			packet.Auth = data[offset : offset+int(num)]
			offset += int(num)
		}
	} else if packet.Capability&allegrosql.ClientSecureConnection > 0 {
		// auth length and auth
		authLen := int(data[offset])
		offset++
		packet.Auth = data[offset : offset+authLen]
		offset += authLen
	} else {
		packet.Auth = data[offset : offset+bytes.IndexByte(data[offset:], 0)]
		offset += len(packet.Auth) + 1
	}

	if packet.Capability&allegrosql.ClientConnectWithDB > 0 {
		if len(data[offset:]) > 0 {
			idx := bytes.IndexByte(data[offset:], 0)
			packet.DBName = string(data[offset : offset+idx])
			offset += idx + 1
		}
	}

	if packet.Capability&allegrosql.ClientPluginAuth > 0 {
		idx := bytes.IndexByte(data[offset:], 0)
		s := offset
		f := offset + idx
		if s < f { // handle unexpected bad packets
			packet.AuthPlugin = string(data[s:f])
		}
		offset += idx + 1
	}

	if packet.Capability&allegrosql.ClientConnectAtts > 0 {
		if len(data[offset:]) == 0 {
			// Defend some ill-formated packet, connection attribute is not important and can be ignored.
			return nil
		}
		if num, null, off := parseLengthEncodedInt(data[offset:]); !null {
			offset += off
			event := data[offset : offset+int(num)]
			attrs, err := parseAttrs(event)
			if err != nil {
				logutil.Logger(ctx).Warn("parse attrs failed", zap.Error(err))
				return nil
			}
			packet.Attrs = attrs
		}
	}

	return nil
}

func parseAttrs(data []byte) (map[string]string, error) {
	attrs := make(map[string]string)
	pos := 0
	for pos < len(data) {
		key, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off
		value, _, off, err := parseLengthEncodedBytes(data[pos:])
		if err != nil {
			return attrs, err
		}
		pos += off

		attrs[string(key)] = string(value)
	}
	return attrs, nil
}

func (cc *clientConn) readOptionalSSLRequestAndHandshakeResponse(ctx context.Context) error {
	// Read a packet. It may be a SSLRequest or HandshakeResponse.
	data, err := cc.readPacket()
	if err != nil {
		err = errors.SuspendStack(err)
		if errors.Cause(err) == io.EOF {
			logutil.Logger(ctx).Debug("wait handshake response fail due to connection has be closed by client-side")
		} else {
			logutil.Logger(ctx).Debug("wait handshake response fail", zap.Error(err))
		}
		return err
	}

	isOldVersion := false

	var resp handshakeResponse41
	var pos int

	if len(data) < 2 {
		logutil.Logger(ctx).Error("got malformed handshake response", zap.ByteString("packetData", data))
		return allegrosql.ErrMalformPacket
	}

	capability := uint32(binary.LittleEndian.Uint16(data[:2]))
	if capability&allegrosql.ClientProtodefCaus41 > 0 {
		pos, err = parseHandshakeResponseHeader(ctx, &resp, data)
	} else {
		pos, err = parseOldHandshakeResponseHeader(ctx, &resp, data)
		isOldVersion = true
	}

	if err != nil {
		terror.Log(err)
		return err
	}

	if resp.Capability&allegrosql.ClientSSL > 0 {
		tlsConfig := (*tls.Config)(atomic.LoadPointer(&cc.server.tlsConfig))
		if tlsConfig != nil {
			// The packet is a SSLRequest, let's switch to TLS.
			if err = cc.upgradeToTLS(tlsConfig); err != nil {
				return err
			}
			// Read the following HandshakeResponse packet.
			data, err = cc.readPacket()
			if err != nil {
				logutil.Logger(ctx).Warn("read handshake response failure after upgrade to TLS", zap.Error(err))
				return err
			}
			if isOldVersion {
				pos, err = parseOldHandshakeResponseHeader(ctx, &resp, data)
			} else {
				pos, err = parseHandshakeResponseHeader(ctx, &resp, data)
			}
			if err != nil {
				terror.Log(err)
				return err
			}
		}
	} else if config.GetGlobalConfig().Security.RequireSecureTransport {
		err := errSecureTransportRequired.FastGenByArgs()
		terror.Log(err)
		return err
	}

	// Read the remaining part of the packet.
	if isOldVersion {
		err = parseOldHandshakeResponseBody(ctx, &resp, data, pos)
	} else {
		err = parseHandshakeResponseBody(ctx, &resp, data, pos)
	}
	if err != nil {
		terror.Log(err)
		return err
	}

	// switching from other methods should work, but not tested
	if resp.AuthPlugin == "caching_sha2_password" {
		resp.Auth, err = cc.authSwitchRequest(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("attempt to send auth switch request packet failed", zap.Error(err))
			return err
		}
	}
	cc.capability = resp.Capability & cc.server.capability
	cc.user = resp.User
	cc.dbname = resp.DBName
	cc.defCauslation = resp.DefCauslation
	cc.attrs = resp.Attrs

	err = cc.openStochastikAndDoAuth(resp.Auth)
	if err != nil {
		logutil.Logger(ctx).Warn("open new stochastik failure", zap.Error(err))
	}
	return err
}

func (cc *clientConn) StochastikStatusToString() string {
	status := cc.ctx.Status()
	inTxn, autoCommit := 0, 0
	if status&allegrosql.ServerStatusInTrans > 0 {
		inTxn = 1
	}
	if status&allegrosql.ServerStatusAutocommit > 0 {
		autoCommit = 1
	}
	return fmt.Sprintf("inTxn:%d, autocommit:%d",
		inTxn, autoCommit,
	)
}

func (cc *clientConn) openStochastikAndDoAuth(authData []byte) error {
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	var err error
	cc.ctx, err = cc.server.driver.OpenCtx(uint64(cc.connectionID), cc.capability, cc.defCauslation, cc.dbname, tlsStatePtr)
	if err != nil {
		return err
	}

	if err = cc.server.checkConnectionCount(); err != nil {
		return err
	}
	hasPassword := "YES"
	if len(authData) == 0 {
		hasPassword = "NO"
	}
	host, err := cc.PeerHost(hasPassword)
	if err != nil {
		return err
	}
	if !cc.ctx.Auth(&auth.UserIdentity{Username: cc.user, Hostname: host}, authData, cc.salt) {
		return errAccessDenied.FastGenByArgs(cc.user, host, hasPassword)
	}
	if cc.dbname != "" {
		err = cc.useDB(context.Background(), cc.dbname)
		if err != nil {
			return err
		}
	}
	cc.ctx.SetStochastikManager(cc.server)
	return nil
}

func (cc *clientConn) PeerHost(hasPassword string) (host string, err error) {
	if len(cc.peerHost) > 0 {
		return cc.peerHost, nil
	}
	host = variable.DefHostname
	if cc.server.isUnixSocket() {
		cc.peerHost = host
		return
	}
	addr := cc.bufReadConn.RemoteAddr().String()
	var port string
	host, port, err = net.SplitHostPort(addr)
	if err != nil {
		err = errAccessDenied.GenWithStackByArgs(cc.user, addr, hasPassword)
		return
	}
	cc.peerHost = host
	cc.peerPort = port
	return
}

// Run reads client query and writes query result to client in for loop, if there is a panic during query handling,
// it will be recovered and log the panic error.
// This function returns and the connection is closed if there is an IO error or there is a panic.
func (cc *clientConn) Run(ctx context.Context) {
	const size = 4096
	defer func() {
		r := recover()
		if r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("connection running loop panic",
				zap.Stringer("lastALLEGROSQL", getLastStmtInConn{cc}),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.String("stack", string(buf)),
			)
			err := cc.writeError(ctx, errors.New(fmt.Sprintf("%v", r)))
			terror.Log(err)
			metrics.PanicCounter.WithLabelValues(metrics.LabelStochastik).Inc()
		}
		if atomic.LoadInt32(&cc.status) != connStatusShutdown {
			err := cc.Close()
			terror.Log(err)
		}
	}()
	// Usually, client connection status changes between [dispatching] <=> [reading].
	// When some event happens, server may notify this client connection by setting
	// the status to special values, for example: kill or graceful shutdown.
	// The client connection would detect the events when it fails to change status
	// by CAS operation, it would then take some actions accordingly.
	for {
		if !atomic.CompareAndSwapInt32(&cc.status, connStatusDispatching, connStatusReading) {
			return
		}

		cc.alloc.Reset()
		// close connection when idle time is more than wait_timeout
		waitTimeout := cc.getStochastikVarsWaitTimeout(ctx)
		cc.pkt.setReadTimeout(time.Duration(waitTimeout) * time.Second)
		start := time.Now()
		data, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				if netErr, isNetErr := errors.Cause(err).(net.Error); isNetErr && netErr.Timeout() {
					idleTime := time.Since(start)
					logutil.Logger(ctx).Info("read packet timeout, close this connection",
						zap.Duration("idle", idleTime),
						zap.Uint64("waitTimeout", waitTimeout),
						zap.Error(err),
					)
				} else {
					errStack := errors.ErrorStack(err)
					if !strings.Contains(errStack, "use of closed network connection") {
						logutil.Logger(ctx).Warn("read packet failed, close this connection",
							zap.Error(errors.SuspendStack(err)))
					}
				}
			}
			disconnectByClientWithError.Inc()
			return
		}

		if !atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusDispatching) {
			return
		}

		startTime := time.Now()
		if err = cc.dispatch(ctx, data); err != nil {
			if terror.ErrorEqual(err, io.EOF) {
				cc.addMetrics(data[0], startTime, nil)
				disconnectNormal.Inc()
				return
			} else if terror.ErrResultUndetermined.Equal(err) {
				logutil.Logger(ctx).Error("result undetermined, close this connection", zap.Error(err))
				disconnectErrorUndetermined.Inc()
				return
			} else if terror.ErrCritical.Equal(err) {
				metrics.CriticalErrorCounter.Add(1)
				logutil.Logger(ctx).Fatal("critical error, stop the server", zap.Error(err))
			}
			var txnMode string
			if cc.ctx != nil {
				txnMode = cc.ctx.GetStochastikVars().GetReadableTxnMode()
			}
			logutil.Logger(ctx).Info("command dispatched failed",
				zap.String("connInfo", cc.String()),
				zap.String("command", allegrosql.Command2Str[data[0]]),
				zap.String("status", cc.StochastikStatusToString()),
				zap.Stringer("allegrosql", getLastStmtInConn{cc}),
				zap.String("txn_mode", txnMode),
				zap.String("err", errStrForLog(err)),
			)
			err1 := cc.writeError(ctx, err)
			terror.Log(err1)
		}
		cc.addMetrics(data[0], startTime, err)
		cc.pkt.sequence = 0
	}
}

// ShutdownOrNotify will Shutdown this client connection, or do its best to notify.
func (cc *clientConn) ShutdownOrNotify() bool {
	if (cc.ctx.Status() & allegrosql.ServerStatusInTrans) > 0 {
		return false
	}
	// If the client connection status is reading, it's safe to shutdown it.
	if atomic.CompareAndSwapInt32(&cc.status, connStatusReading, connStatusShutdown) {
		return true
	}
	// If the client connection status is dispatching, we can't shutdown it immediately,
	// so set the status to WaitShutdown as a notification, the client will detect it
	// and then exit.
	atomic.StoreInt32(&cc.status, connStatusWaitShutdown)
	return false
}

func queryStrForLog(query string) string {
	const size = 4096
	if len(query) > size {
		return query[:size] + fmt.Sprintf("(len: %d)", len(query))
	}
	return query
}

func errStrForLog(err error) string {
	if ekv.ErrKeyExists.Equal(err) || BerolinaSQL.ErrParse.Equal(err) || schemareplicant.ErrBlockNotExists.Equal(err) {
		// Do not log stack for duplicated entry error.
		return err.Error()
	}
	return errors.ErrorStack(err)
}

func (cc *clientConn) addMetrics(cmd byte, startTime time.Time, err error) {
	if cmd == allegrosql.ComQuery && cc.ctx.Value(stochastikctx.LastInterDircuteDBS) != nil {
		// Don't take DBS execute time into account.
		// It's already recorded by other metrics in dbs package.
		return
	}

	var counter prometheus.Counter
	if err != nil && int(cmd) < len(queryTotalCountErr) {
		counter = queryTotalCountErr[cmd]
	} else if err == nil && int(cmd) < len(queryTotalCountOk) {
		counter = queryTotalCountOk[cmd]
	}
	if counter != nil {
		counter.Inc()
	} else {
		label := strconv.Itoa(int(cmd))
		if err != nil {
			metrics.QueryTotalCounter.WithLabelValues(label, "ERROR").Inc()
		} else {
			metrics.QueryTotalCounter.WithLabelValues(label, "OK").Inc()
		}
	}

	stmtType := cc.ctx.GetStochastikVars().StmtCtx.StmtType
	sqlType := metrics.LblGeneral
	if stmtType != "" {
		sqlType = stmtType
	}

	switch sqlType {
	case "Use":
		queryDurationHistogramUse.Observe(time.Since(startTime).Seconds())
	case "Show":
		queryDurationHistogramShow.Observe(time.Since(startTime).Seconds())
	case "Begin":
		queryDurationHistogramBegin.Observe(time.Since(startTime).Seconds())
	case "Commit":
		queryDurationHistogramCommit.Observe(time.Since(startTime).Seconds())
	case "Rollback":
		queryDurationHistogramRollback.Observe(time.Since(startTime).Seconds())
	case "Insert":
		queryDurationHistogramInsert.Observe(time.Since(startTime).Seconds())
	case "Replace":
		queryDurationHistogramReplace.Observe(time.Since(startTime).Seconds())
	case "Delete":
		queryDurationHistogramDelete.Observe(time.Since(startTime).Seconds())
	case "UFIDelate":
		queryDurationHistogramUFIDelate.Observe(time.Since(startTime).Seconds())
	case "Select":
		queryDurationHistogramSelect.Observe(time.Since(startTime).Seconds())
	case "InterDircute":
		queryDurationHistogramInterDircute.Observe(time.Since(startTime).Seconds())
	case "Set":
		queryDurationHistogramSet.Observe(time.Since(startTime).Seconds())
	case metrics.LblGeneral:
		queryDurationHistogramGeneral.Observe(time.Since(startTime).Seconds())
	default:
		metrics.QueryDurationHistogram.WithLabelValues(sqlType).Observe(time.Since(startTime).Seconds())
	}
}

// dispatch handles client request based on command which is the first byte of the data.
// It also gets a token from server which is used to limit the concurrently handling clients.
// The most frequently used command is ComQuery.
func (cc *clientConn) dispatch(ctx context.Context, data []byte) error {
	defer func() {
		// reset killed for each request
		atomic.StoreUint32(&cc.ctx.GetStochastikVars().Killed, 0)
	}()
	span := opentracing.StartSpan("server.dispatch")

	t := time.Now()
	cc.lastPacket = data
	cmd := data[0]
	data = data[1:]
	if variable.EnablePProfALLEGROSQLCPU.Load() {
		label := getLastStmtInConn{cc}.PProfLabel()
		if len(label) > 0 {
			defer pprof.SetGoroutineLabels(ctx)
			ctx = pprof.WithLabels(ctx, pprof.Labels("allegrosql", label))
			pprof.SetGoroutineLabels(ctx)
		}
	}
	if trace.IsEnabled() {
		lc := getLastStmtInConn{cc}
		sqlType := lc.PProfLabel()
		if len(sqlType) > 0 {
			var task *trace.Task
			ctx, task = trace.NewTask(ctx, sqlType)
			trace.Log(ctx, "allegrosql", lc.String())
			defer task.End()
		}
	}
	token := cc.server.getToken()
	defer func() {
		// if handleChangeUser failed, cc.ctx may be nil
		if cc.ctx != nil {
			cc.ctx.SetProcessInfo("", t, allegrosql.ComSleep, 0)
		}

		cc.server.releaseToken(token)
		span.Finish()
	}()

	vars := cc.ctx.GetStochastikVars()
	// reset killed for each request
	atomic.StoreUint32(&vars.Killed, 0)
	if cmd < allegrosql.ComEnd {
		cc.ctx.SetCommandValue(cmd)
	}

	dataStr := string(replog.String(data))
	switch cmd {
	case allegrosql.ComPing, allegrosql.ComStmtClose, allegrosql.ComStmtSendLongData, allegrosql.ComStmtReset,
		allegrosql.ComSetOption, allegrosql.ComChangeUser:
		cc.ctx.SetProcessInfo("", t, cmd, 0)
	case allegrosql.ComInitDB:
		cc.ctx.SetProcessInfo("use "+dataStr, t, cmd, 0)
	}

	switch cmd {
	case allegrosql.ComSleep:
		// TODO: According to allegrosql document, this command is supposed to be used only internally.
		// So it's just a temp fix, not sure if it's done right.
		// Investigate this command and write test case later.
		return nil
	case allegrosql.ComQuit:
		return io.EOF
	case allegrosql.ComInitDB:
		if err := cc.useDB(ctx, dataStr); err != nil {
			return err
		}
		return cc.writeOK(ctx)
	case allegrosql.ComQuery: // Most frequently used command.
		// For issue 1989
		// Input payload may end with byte '\0', we didn't find related allegrosql document about it, but allegrosql
		// implementation accept that case. So trim the last '\0' here as if the payload an EOF string.
		// See http://dev.allegrosql.com/doc/internals/en/com-query.html
		if len(data) > 0 && data[len(data)-1] == 0 {
			data = data[:len(data)-1]
			dataStr = string(replog.String(data))
		}
		return cc.handleQuery(ctx, dataStr)
	case allegrosql.ComFieldList:
		return cc.handleFieldList(ctx, dataStr)
	// ComCreateDB, ComDroFIDelB
	case allegrosql.ComRefresh:
		return cc.handleRefresh(ctx, data[0])
	case allegrosql.ComShutdown: // redirect to ALLEGROALLEGROSQL
		if err := cc.handleQuery(ctx, "SHUTDOWN"); err != nil {
			return err
		}
		return cc.writeOK(ctx)
	// ComStatistics, ComProcessInfo, ComConnect, ComProcessKill, ComDebug
	case allegrosql.ComPing:
		return cc.writeOK(ctx)
	// ComTime, ComDelayedInsert
	case allegrosql.ComChangeUser:
		return cc.handleChangeUser(ctx, data)
	// ComBinlogDump, ComBlockDump, ComConnectOut, ComRegisterSlave
	case allegrosql.ComStmtPrepare:
		return cc.handleStmtPrepare(ctx, dataStr)
	case allegrosql.ComStmtInterDircute:
		return cc.handleStmtInterDircute(ctx, data)
	case allegrosql.ComStmtSendLongData:
		return cc.handleStmtSendLongData(data)
	case allegrosql.ComStmtClose:
		return cc.handleStmtClose(data)
	case allegrosql.ComStmtReset:
		return cc.handleStmtReset(ctx, data)
	case allegrosql.ComSetOption:
		return cc.handleSetOption(ctx, data)
	case allegrosql.ComStmtFetch:
		return cc.handleStmtFetch(ctx, data)
	// ComDaemon, ComBinlogDumpGtid
	case allegrosql.ComResetConnection:
		return cc.handleResetConnection(ctx)
	// ComEnd
	default:
		return allegrosql.NewErrf(allegrosql.ErrUnknown, "command %d not supported now", cmd)
	}
}

func (cc *clientConn) useDB(ctx context.Context, EDB string) (err error) {
	// if input is "use `SELECT`", allegrosql client just send "SELECT"
	// so we add `` around EDB.
	stmts, err := cc.ctx.Parse(ctx, "use `"+EDB+"`")
	if err != nil {
		return err
	}
	_, err = cc.ctx.InterDircuteStmt(ctx, stmts[0])
	if err != nil {
		return err
	}
	cc.dbname = EDB
	return
}

func (cc *clientConn) flush(ctx context.Context) error {
	defer trace.StartRegion(ctx, "FlushClientConn").End()
	failpoint.Inject("FakeClientConn", func() {
		if cc.pkt == nil {
			failpoint.Return(nil)
		}
	})
	return cc.pkt.flush()
}

func (cc *clientConn) writeOK(ctx context.Context) error {
	msg := cc.ctx.LastMessage()
	return cc.writeOkWith(ctx, msg, cc.ctx.AffectedRows(), cc.ctx.LastInsertID(), cc.ctx.Status(), cc.ctx.WarningCount())
}

func (cc *clientConn) writeOkWith(ctx context.Context, msg string, affectedRows, lastInsertID uint64, status, warnCnt uint16) error {
	enclen := 0
	if len(msg) > 0 {
		enclen = lengthEncodedIntSize(uint64(len(msg))) + len(msg)
	}

	data := cc.alloc.AllocWithLen(4, 32+enclen)
	data = append(data, allegrosql.OKHeader)
	data = dumpLengthEncodedInt(data, affectedRows)
	data = dumpLengthEncodedInt(data, lastInsertID)
	if cc.capability&allegrosql.ClientProtodefCaus41 > 0 {
		data = dumpUint16(data, status)
		data = dumpUint16(data, warnCnt)
	}
	if enclen > 0 {
		// although MyALLEGROSQL manual says the info message is string<EOF>(https://dev.allegrosql.com/doc/internals/en/packet-OK_Packet.html),
		// it is actually string<lenenc>
		data = dumpLengthEncodedString(data, []byte(msg))
	}

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) writeError(ctx context.Context, e error) error {
	var (
		m  *allegrosql.ALLEGROSQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		m = terror.ToALLEGROSQLError(te)
	} else {
		e := errors.Cause(originErr)
		switch y := e.(type) {
		case *terror.Error:
			m = terror.ToALLEGROSQLError(y)
		default:
			m = allegrosql.NewErrf(allegrosql.ErrUnknown, "%s", e.Error())
		}
	}

	cc.lastCode = m.Code
	data := cc.alloc.AllocWithLen(4, 16+len(m.Message))
	data = append(data, allegrosql.ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&allegrosql.ClientProtodefCaus41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush(ctx)
}

// writeEOF writes an EOF packet.
// Note this function won't flush the stream because maybe there are more
// packets following it.
// serverStatus, a flag bit represents server information
// in the packet.
func (cc *clientConn) writeEOF(serverStatus uint16) error {
	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, allegrosql.EOFHeader)
	if cc.capability&allegrosql.ClientProtodefCaus41 > 0 {
		data = dumpUint16(data, cc.ctx.WarningCount())
		status := cc.ctx.Status()
		status |= serverStatus
		data = dumpUint16(data, status)
	}

	err := cc.writePacket(data)
	return err
}

func (cc *clientConn) writeReq(ctx context.Context, filePath string) error {
	data := cc.alloc.AllocWithLen(4, 5+len(filePath))
	data = append(data, allegrosql.LocalInFileHeader)
	data = append(data, filePath...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

func insertDataWithCommit(ctx context.Context, prevData,
	curData []byte, loadDataInfo *interlock.LoadDataInfo) ([]byte, error) {
	var err error
	var reachLimit bool
	for {
		prevData, reachLimit, err = loadDataInfo.InsertData(ctx, prevData, curData)
		if err != nil {
			return nil, err
		}
		if !reachLimit {
			break
		}
		// push into commit task queue
		err = loadDataInfo.EnqOneTask(ctx)
		if err != nil {
			return prevData, err
		}
		curData = prevData
		prevData = nil
	}
	return prevData, nil
}

// processStream process input stream from network
func processStream(ctx context.Context, cc *clientConn, loadDataInfo *interlock.LoadDataInfo, wg *sync.WaitGroup) {
	var err error
	var shouldBreak bool
	var prevData, curData []byte
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("process routine panicked",
				zap.Reflect("r", r),
				zap.Stack("stack"))
		}
		if err != nil || r != nil {
			loadDataInfo.ForceQuit()
		} else {
			loadDataInfo.CloseTaskQueue()
		}
		wg.Done()
	}()
	for {
		curData, err = cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				logutil.Logger(ctx).Error("read packet failed", zap.Error(err))
				break
			}
		}
		if len(curData) == 0 {
			loadDataInfo.Drained = true
			shouldBreak = true
			if len(prevData) == 0 {
				break
			}
		}
		select {
		case <-loadDataInfo.QuitCh:
			err = errors.New("processStream forced to quit")
		default:
		}
		if err != nil {
			break
		}
		// prepare batch and enqueue task
		prevData, err = insertDataWithCommit(ctx, prevData, curData, loadDataInfo)
		if err != nil {
			break
		}
		if shouldBreak {
			break
		}
	}
	if err != nil {
		logutil.Logger(ctx).Error("load data process stream error", zap.Error(err))
	} else {
		err = loadDataInfo.EnqOneTask(ctx)
		if err != nil {
			logutil.Logger(ctx).Error("load data process stream error", zap.Error(err))
		}
	}
}

// handleLoadData does the additional work after processing the 'load data' query.
// It sends client a file path, then reads the file content from client, inserts data into database.
func (cc *clientConn) handleLoadData(ctx context.Context, loadDataInfo *interlock.LoadDataInfo) error {
	// If the server handles the load data request, the client has to set the ClientLocalFiles capability.
	if cc.capability&allegrosql.ClientLocalFiles == 0 {
		return errNotAllowedCommand
	}
	if loadDataInfo == nil {
		return errors.New("load data info is empty")
	}

	err := cc.writeReq(ctx, loadDataInfo.Path)
	if err != nil {
		return err
	}

	loadDataInfo.InitQueues()
	loadDataInfo.SetMaxRowsInBatch(uint64(loadDataInfo.Ctx.GetStochastikVars().DMLBatchSize))
	loadDataInfo.StartStopWatcher()
	// let stop watcher goroutine quit
	defer loadDataInfo.ForceQuit()
	err = loadDataInfo.Ctx.NewTxn(ctx)
	if err != nil {
		return err
	}
	// processStream process input data, enqueue commit task
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go processStream(ctx, cc, loadDataInfo, wg)
	err = loadDataInfo.CommitWork(ctx)
	wg.Wait()
	if err != nil {
		if !loadDataInfo.Drained {
			logutil.Logger(ctx).Info("not drained yet, try reading left data from client connection")
		}
		// drain the data from client conn soliton empty packet received, otherwise the connection will be reset
		for !loadDataInfo.Drained {
			// check kill flag again, let the draining loop could quit if empty packet could not be received
			if atomic.CompareAndSwapUint32(&loadDataInfo.Ctx.GetStochastikVars().Killed, 1, 0) {
				logutil.Logger(ctx).Warn("receiving kill, stop draining data, connection may be reset")
				return interlock.ErrQueryInterrupted
			}
			curData, err1 := cc.readPacket()
			if err1 != nil {
				logutil.Logger(ctx).Error("drain reading left data encounter errors", zap.Error(err1))
				break
			}
			if len(curData) == 0 {
				loadDataInfo.Drained = true
				logutil.Logger(ctx).Info("draining finished for error", zap.Error(err))
				break
			}
		}
	}
	loadDataInfo.SetMessage()
	return err
}

// getDataFromPath gets file contents from file path.
func (cc *clientConn) getDataFromPath(ctx context.Context, path string) ([]byte, error) {
	err := cc.writeReq(ctx, path)
	if err != nil {
		return nil, err
	}
	var prevData, curData []byte
	for {
		curData, err = cc.readPacket()
		if err != nil && terror.ErrorNotEqual(err, io.EOF) {
			return nil, err
		}
		if len(curData) == 0 {
			break
		}
		prevData = append(prevData, curData...)
	}
	return prevData, nil
}

// handleLoadStats does the additional work after processing the 'load stats' query.
// It sends client a file path, then reads the file content from client, loads it into the storage.
func (cc *clientConn) handleLoadStats(ctx context.Context, loadStatsInfo *interlock.LoadStatsInfo) error {
	// If the server handles the load data request, the client has to set the ClientLocalFiles capability.
	if cc.capability&allegrosql.ClientLocalFiles == 0 {
		return errNotAllowedCommand
	}
	if loadStatsInfo == nil {
		return errors.New("load stats: info is empty")
	}
	data, err := cc.getDataFromPath(ctx, loadStatsInfo.Path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return loadStatsInfo.UFIDelate(data)
}

// handleIndexAdvise does the index advise work and returns the advise result for index.
func (cc *clientConn) handleIndexAdvise(ctx context.Context, indexAdviseInfo *interlock.IndexAdviseInfo) error {
	if cc.capability&allegrosql.ClientLocalFiles == 0 {
		return errNotAllowedCommand
	}
	if indexAdviseInfo == nil {
		return errors.New("Index Advise: info is empty")
	}

	data, err := cc.getDataFromPath(ctx, indexAdviseInfo.Path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return errors.New("Index Advise: infile is empty")
	}

	if err := indexAdviseInfo.GetIndexAdvice(ctx, data); err != nil {
		return err
	}

	// TODO: Write the rss []ResultSet. It will be done in another PR.
	return nil
}

// handleQuery executes the allegrosql query string and writes result set or result ok to the client.
// As the execution time of this function represents the performance of MilevaDB, we do time log and metrics here.
// There is a special query `load data` that does not return result, which is handled differently.
// Query `load stats` does not return result either.
func (cc *clientConn) handleQuery(ctx context.Context, allegrosql string) (err error) {
	defer trace.StartRegion(ctx, "handleQuery").End()
	sc := cc.ctx.GetStochastikVars().StmtCtx
	prevWarns := sc.GetWarnings()
	stmts, err := cc.ctx.Parse(ctx, allegrosql)
	if err != nil {
		metrics.InterDircuteErrorCounter.WithLabelValues(metrics.InterDircuteErrorToLabel(err)).Inc()
		return err
	}

	if len(stmts) == 0 {
		return cc.writeOK(ctx)
	}

	warns := sc.GetWarnings()
	BerolinaSQLWarns := warns[len(prevWarns):]

	var pointCausets []causetcore.Causet
	if len(stmts) > 1 {
		// Only pre-build point plans for multi-memex query
		pointCausets, err = cc.prefetchPointCausetKeys(ctx, stmts)
		if err != nil {
			return err
		}
	}
	if len(pointCausets) > 0 {
		defer cc.ctx.ClearValue(causetcore.PointCausetKey)
	}
	for i, stmt := range stmts {
		if len(pointCausets) > 0 {
			// Save the point plan in Stochastik so we don't need to build the point plan again.
			cc.ctx.SetValue(causetcore.PointCausetKey, causetcore.PointCausetVal{Causet: pointCausets[i]})
		}
		err = cc.handleStmt(ctx, stmt, BerolinaSQLWarns, i == len(stmts)-1)
		if err != nil {
			break
		}
	}
	if err != nil {
		metrics.InterDircuteErrorCounter.WithLabelValues(metrics.InterDircuteErrorToLabel(err)).Inc()
	}
	return err
}

// prefetchPointCausetKeys extracts the point keys in multi-memex query,
// use BatchGet to get the keys, so the values will be cached in the snapshot cache, save RPC call cost.
// For pessimistic transaction, the keys will be batch locked.
func (cc *clientConn) prefetchPointCausetKeys(ctx context.Context, stmts []ast.StmtNode) ([]causetcore.Causet, error) {
	txn, err := cc.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if !txn.Valid() {
		// Only prefetch in-transaction query for simplicity.
		// Later we can support out-transaction multi-memex query.
		return nil, nil
	}
	vars := cc.ctx.GetStochastikVars()
	if vars.TxnCtx.IsPessimistic {
		if vars.IsReadConsistencyTxn() {
			// TODO: to support READ-COMMITTED, we need to avoid getting new TS for each memex in the query.
			return nil, nil
		}
		if vars.TxnCtx.GetForUFIDelateTS() != vars.TxnCtx.StartTS {
			// Do not handle the case that ForUFIDelateTS is changed for simplicity.
			return nil, nil
		}
	}
	pointCausets := make([]causetcore.Causet, len(stmts))
	var idxKeys []ekv.Key
	var rowKeys []ekv.Key
	is := petri.GetPetri(cc.ctx).SchemaReplicant()
	sc := vars.StmtCtx
	for i, stmt := range stmts {
		// TODO: the preprocess is run twice, we should find some way to avoid do it again.
		if err = causetcore.Preprocess(cc.ctx, stmt, is); err != nil {
			return nil, err
		}
		p := causetcore.TryFastCauset(cc.ctx.Stochastik, stmt)
		pointCausets[i] = p
		if p == nil {
			continue
		}
		// Only support UFIDelate for now.
		// TODO: support other point plans.
		switch x := p.(type) {
		case *causetcore.UFIDelate:
			uFIDelateStmt := stmt.(*ast.UFIDelateStmt)
			if pp, ok := x.SelectCauset.(*causetcore.PointGetCauset); ok {
				if pp.PartitionInfo != nil {
					continue
				}
				if pp.IndexInfo != nil {
					interlock.ResetUFIDelateStmtCtx(sc, uFIDelateStmt, vars)
					idxKey, err1 := interlock.EncodeUniqueIndexKey(cc.ctx, pp.TblInfo, pp.IndexInfo, pp.IndexValues, pp.TblInfo.ID)
					if err1 != nil {
						return nil, err1
					}
					idxKeys = append(idxKeys, idxKey)
				} else {
					rowKeys = append(rowKeys, blockcodec.EncodeRowKeyWithHandle(pp.TblInfo.ID, pp.Handle))
				}
			}
		}
	}
	if len(idxKeys) == 0 && len(rowKeys) == 0 {
		return pointCausets, nil
	}
	snapshot := txn.GetSnapshot()
	idxVals, err1 := snapshot.BatchGet(ctx, idxKeys)
	if err1 != nil {
		return nil, err1
	}
	for idxKey, idxVal := range idxVals {
		h, err2 := blockcodec.DecodeHandleInUniqueIndexValue(idxVal, false)
		if err2 != nil {
			return nil, err2
		}
		tblID := blockcodec.DecodeBlockID(replog.Slice(idxKey))
		rowKeys = append(rowKeys, blockcodec.EncodeRowKeyWithHandle(tblID, h))
	}
	if vars.TxnCtx.IsPessimistic {
		allKeys := append(rowKeys, idxKeys...)
		err = interlock.LockKeys(ctx, cc.ctx, vars.LockWaitTimeout, allKeys...)
		if err != nil {
			// suppress the dagger error, we are not going to handle it here for simplicity.
			err = nil
			logutil.BgLogger().Warn("dagger keys error on prefetch", zap.Error(err))
		}
	} else {
		_, err = snapshot.BatchGet(ctx, rowKeys)
		if err != nil {
			return nil, err
		}
	}
	return pointCausets, nil
}

func (cc *clientConn) handleStmt(ctx context.Context, stmt ast.StmtNode, warns []stmtctx.ALLEGROSQLWarn, lastStmt bool) error {
	ctx = context.WithValue(ctx, execdetails.StmtInterDircDetailKey, &execdetails.StmtInterDircDetails{})
	reg := trace.StartRegion(ctx, "InterDircuteStmt")
	rs, err := cc.ctx.InterDircuteStmt(ctx, stmt)
	reg.End()
	// The stochastik tracker detachment from global tracker is solved in the `rs.Close` in most cases.
	// If the rs is nil, the detachment will be done in the `handleNoDelay`.
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return err
	}

	if lastStmt {
		cc.ctx.GetStochastikVars().StmtCtx.AppendWarnings(warns)
	}

	status := cc.ctx.Status()
	if !lastStmt {
		status |= allegrosql.ServerMoreResultsExists
	}

	if rs != nil {
		connStatus := atomic.LoadInt32(&cc.status)
		if connStatus == connStatusShutdown || connStatus == connStatusWaitShutdown {
			return interlock.ErrQueryInterrupted
		}

		err = cc.writeResultset(ctx, rs, false, status, 0)
		if err != nil {
			return err
		}
	} else {
		err = cc.handleQuerySpecial(ctx, status)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cc *clientConn) handleQuerySpecial(ctx context.Context, status uint16) error {
	loadDataInfo := cc.ctx.Value(interlock.LoadDataVarKey)
	if loadDataInfo != nil {
		defer cc.ctx.SetValue(interlock.LoadDataVarKey, nil)
		if err := cc.handleLoadData(ctx, loadDataInfo.(*interlock.LoadDataInfo)); err != nil {
			return err
		}
	}

	loadStats := cc.ctx.Value(interlock.LoadStatsVarKey)
	if loadStats != nil {
		defer cc.ctx.SetValue(interlock.LoadStatsVarKey, nil)
		if err := cc.handleLoadStats(ctx, loadStats.(*interlock.LoadStatsInfo)); err != nil {
			return err
		}
	}

	indexAdvise := cc.ctx.Value(interlock.IndexAdviseVarKey)
	if indexAdvise != nil {
		defer cc.ctx.SetValue(interlock.IndexAdviseVarKey, nil)
		if err := cc.handleIndexAdvise(ctx, indexAdvise.(*interlock.IndexAdviseInfo)); err != nil {
			return err
		}
	}
	return cc.writeOkWith(ctx, cc.ctx.LastMessage(), cc.ctx.AffectedRows(), cc.ctx.LastInsertID(), status, cc.ctx.WarningCount())
}

// handleFieldList returns the field list for a causet.
// The allegrosql string is composed of a causet name and a terminating character \x00.
func (cc *clientConn) handleFieldList(ctx context.Context, allegrosql string) (err error) {
	parts := strings.Split(allegrosql, "\x00")
	defCausumns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return err
	}
	data := cc.alloc.AllocWithLen(4, 1024)
	for _, defCausumn := range defCausumns {
		// Current we doesn't output defaultValue but reserve defaultValue length byte to make mariadb client happy.
		// https://dev.allegrosql.com/doc/internals/en/com-query-response.html#defCausumn-definition
		// TODO: fill the right DefaultValues.
		defCausumn.DefaultValueLength = 0
		defCausumn.DefaultValue = []byte{}

		data = data[0:4]
		data = defCausumn.Dump(data)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	if err := cc.writeEOF(0); err != nil {
		return err
	}
	return cc.flush(ctx)
}

// writeResultset writes data into a resultset and uses rs.Next to get event data back.
// If binary is true, the data would be encoded in BINARY format.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
func (cc *clientConn) writeResultset(ctx context.Context, rs ResultSet, binary bool, serverStatus uint16, fetchSize int) (runErr error) {
	defer func() {
		// close ResultSet when cursor doesn't exist
		r := recover()
		if r == nil {
			return
		}
		if str, ok := r.(string); !ok || !strings.HasPrefix(str, memory.PanicMemoryExceed) {
			panic(r)
		}
		// TODO(jianzhang.zj: add metrics here)
		runErr = errors.Errorf("%v", r)
		buf := make([]byte, 4096)
		stackSize := runtime.Stack(buf, false)
		buf = buf[:stackSize]
		logutil.Logger(ctx).Error("write query result panic", zap.Stringer("lastALLEGROSQL", getLastStmtInConn{cc}), zap.String("stack", string(buf)))
	}()
	var err error
	if allegrosql.HasCursorExistsFlag(serverStatus) {
		err = cc.writeChunksWithFetchSize(ctx, rs, serverStatus, fetchSize)
	} else {
		err = cc.writeChunks(ctx, rs, binary, serverStatus)
	}
	if err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) writeDeferredCausetInfo(defCausumns []*DeferredCausetInfo, serverStatus uint16) error {
	data := cc.alloc.AllocWithLen(4, 1024)
	data = dumpLengthEncodedInt(data, uint64(len(defCausumns)))
	if err := cc.writePacket(data); err != nil {
		return err
	}
	for _, v := range defCausumns {
		data = data[0:4]
		data = v.Dump(data)
		if err := cc.writePacket(data); err != nil {
			return err
		}
	}
	return cc.writeEOF(serverStatus)
}

// writeChunks writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information
func (cc *clientConn) writeChunks(ctx context.Context, rs ResultSet, binary bool, serverStatus uint16) error {
	data := cc.alloc.AllocWithLen(4, 1024)
	req := rs.NewChunk()
	gotDeferredCausetInfo := false
	var stmtDetail *execdetails.StmtInterDircDetails
	stmtDetailRaw := ctx.Value(execdetails.StmtInterDircDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = stmtDetailRaw.(*execdetails.StmtInterDircDetails)
	}

	for {
		// Here server.milevadbResultSet implements Next method.
		err := rs.Next(ctx, req)
		if err != nil {
			return err
		}
		if !gotDeferredCausetInfo {
			// We need to call Next before we get defCausumns.
			// Otherwise, we will get incorrect defCausumns info.
			defCausumns := rs.DeferredCausets()
			err = cc.writeDeferredCausetInfo(defCausumns, serverStatus)
			if err != nil {
				return err
			}
			gotDeferredCausetInfo = true
		}
		rowCount := req.NumRows()
		if rowCount == 0 {
			break
		}
		reg := trace.StartRegion(ctx, "WriteClientConn")
		start := time.Now()
		for i := 0; i < rowCount; i++ {
			data = data[0:4]
			if binary {
				data, err = dumpBinaryRow(data, rs.DeferredCausets(), req.GetRow(i))
			} else {
				data, err = dumpTextRow(data, rs.DeferredCausets(), req.GetRow(i))
			}
			if err != nil {
				reg.End()
				return err
			}
			if err = cc.writePacket(data); err != nil {
				reg.End()
				return err
			}
		}
		reg.End()
		if stmtDetail != nil {
			stmtDetail.WriteALLEGROSQLResFIDeluration += time.Since(start)
		}
	}
	return cc.writeEOF(serverStatus)
}

// writeChunksWithFetchSize writes data from a Chunk, which filled data by a ResultSet, into a connection.
// binary specifies the way to dump data. It throws any error while dumping data.
// serverStatus, a flag bit represents server information.
// fetchSize, the desired number of rows to be fetched each time when client uses cursor.
func (cc *clientConn) writeChunksWithFetchSize(ctx context.Context, rs ResultSet, serverStatus uint16, fetchSize int) error {
	fetchedRows := rs.GetFetchedRows()

	// if fetchedRows is not enough, getting data from recordSet.
	req := rs.NewChunk()
	for len(fetchedRows) < fetchSize {
		// Here server.milevadbResultSet implements Next method.
		err := rs.Next(ctx, req)
		if err != nil {
			return err
		}
		rowCount := req.NumRows()
		if rowCount == 0 {
			break
		}
		// filling fetchedRows with chunk
		for i := 0; i < rowCount; i++ {
			fetchedRows = append(fetchedRows, req.GetRow(i))
		}
		req = chunk.Renew(req, cc.ctx.GetStochastikVars().MaxChunkSize)
	}

	// tell the client COM_STMT_FETCH has finished by setting proper serverStatus,
	// and close ResultSet.
	if len(fetchedRows) == 0 {
		serverStatus &^= allegrosql.ServerStatusCursorExists
		serverStatus |= allegrosql.ServerStatusLastRowSend
		terror.Call(rs.Close)
		return cc.writeEOF(serverStatus)
	}

	// construct the rows sent to the client according to fetchSize.
	var curRows []chunk.Row
	if fetchSize < len(fetchedRows) {
		curRows = fetchedRows[:fetchSize]
		fetchedRows = fetchedRows[fetchSize:]
	} else {
		curRows = fetchedRows[:]
		fetchedRows = fetchedRows[:0]
	}
	rs.StoreFetchedRows(fetchedRows)

	data := cc.alloc.AllocWithLen(4, 1024)
	var stmtDetail *execdetails.StmtInterDircDetails
	stmtDetailRaw := ctx.Value(execdetails.StmtInterDircDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = stmtDetailRaw.(*execdetails.StmtInterDircDetails)
	}
	start := time.Now()
	var err error
	for _, event := range curRows {
		data = data[0:4]
		data, err = dumpBinaryRow(data, rs.DeferredCausets(), event)
		if err != nil {
			return err
		}
		if err = cc.writePacket(data); err != nil {
			return err
		}
	}
	if stmtDetail != nil {
		stmtDetail.WriteALLEGROSQLResFIDeluration += time.Since(start)
	}
	if cl, ok := rs.(fetchNotifier); ok {
		cl.OnFetchReturned()
	}
	return cc.writeEOF(serverStatus)
}

func (cc *clientConn) writeMultiResultset(ctx context.Context, rss []ResultSet, binary bool) error {
	for i, rs := range rss {
		lastRs := i == len(rss)-1
		if r, ok := rs.(*milevadbResultSet).recordSet.(sqlexec.MultiQueryNoDelayResult); ok {
			status := r.Status()
			if !lastRs {
				status |= allegrosql.ServerMoreResultsExists
			}
			if err := cc.writeOkWith(ctx, r.LastMessage(), r.AffectedRows(), r.LastInsertID(), status, r.WarnCount()); err != nil {
				return err
			}
			continue
		}
		status := uint16(0)
		if !lastRs {
			status |= allegrosql.ServerMoreResultsExists
		}
		if err := cc.writeResultset(ctx, rs, binary, status, 0); err != nil {
			return err
		}
	}
	return nil
}

func (cc *clientConn) setConn(conn net.Conn) {
	cc.bufReadConn = newBufferedReadConn(conn)
	if cc.pkt == nil {
		cc.pkt = newPacketIO(cc.bufReadConn)
	} else {
		// Preserve current sequence number.
		cc.pkt.setBufferedReadConn(cc.bufReadConn)
	}
}

func (cc *clientConn) upgradeToTLS(tlsConfig *tls.Config) error {
	// Important: read from buffered reader instead of the original net.Conn because it may contain data we need.
	tlsConn := tls.Server(cc.bufReadConn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	cc.setConn(tlsConn)
	cc.tlsConn = tlsConn
	return nil
}

func (cc *clientConn) handleChangeUser(ctx context.Context, data []byte) error {
	user, data := parseNullTermString(data)
	cc.user = string(replog.String(user))
	if len(data) < 1 {
		return allegrosql.ErrMalformPacket
	}
	passLen := int(data[0])
	data = data[1:]
	if passLen > len(data) {
		return allegrosql.ErrMalformPacket
	}
	pass := data[:passLen]
	data = data[passLen:]
	dbName, _ := parseNullTermString(data)
	cc.dbname = string(replog.String(dbName))

	err := cc.ctx.Close()
	if err != nil {
		logutil.Logger(ctx).Debug("close old context failed", zap.Error(err))
	}
	err = cc.openStochastikAndDoAuth(pass)
	if err != nil {
		return err
	}
	return cc.handleCommonConnectionReset(ctx)
}

func (cc *clientConn) handleResetConnection(ctx context.Context) error {
	user := cc.ctx.GetStochastikVars().User
	err := cc.ctx.Close()
	if err != nil {
		logutil.Logger(ctx).Debug("close old context failed", zap.Error(err))
	}
	var tlsStatePtr *tls.ConnectionState
	if cc.tlsConn != nil {
		tlsState := cc.tlsConn.ConnectionState()
		tlsStatePtr = &tlsState
	}
	cc.ctx, err = cc.server.driver.OpenCtx(uint64(cc.connectionID), cc.capability, cc.defCauslation, cc.dbname, tlsStatePtr)
	if err != nil {
		return err
	}
	if !cc.ctx.AuthWithoutVerification(user) {
		return errors.New("Could not reset connection")
	}
	if cc.dbname != "" { // Restore the current EDB
		err = cc.useDB(context.Background(), cc.dbname)
		if err != nil {
			return err
		}
	}
	cc.ctx.SetStochastikManager(cc.server)

	return cc.handleCommonConnectionReset(ctx)
}

func (cc *clientConn) handleCommonConnectionReset(ctx context.Context) error {
	if plugin.IsEnable(plugin.Audit) {
		cc.ctx.GetStochastikVars().ConnectionInfo = cc.connectInfo()
	}

	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		authPlugin := plugin.DeclareAuditManifest(p.Manifest)
		if authPlugin.OnConnectionEvent != nil {
			connInfo := cc.ctx.GetStochastikVars().ConnectionInfo
			err := authPlugin.OnConnectionEvent(context.Background(), plugin.ChangeUser, connInfo)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return cc.writeOK(ctx)
}

// safe to noop except 0x01 "FLUSH PRIVILEGES"
func (cc *clientConn) handleRefresh(ctx context.Context, subCommand byte) error {
	if subCommand == 0x01 {
		if err := cc.handleQuery(ctx, "FLUSH PRIVILEGES"); err != nil {
			return err
		}
	}
	return cc.writeOK(ctx)
}

var _ fmt.Stringer = getLastStmtInConn{}

type getLastStmtInConn struct {
	*clientConn
}

func (cc getLastStmtInConn) String() string {
	if len(cc.lastPacket) == 0 {
		return ""
	}
	cmd, data := cc.lastPacket[0], cc.lastPacket[1:]
	switch cmd {
	case allegrosql.ComInitDB:
		return "Use " + string(data)
	case allegrosql.ComFieldList:
		return "ListFields " + string(data)
	case allegrosql.ComQuery, allegrosql.ComStmtPrepare:
		allegrosql := string(replog.String(data))
		if config.RedactLogEnabled() {
			allegrosql, _ = BerolinaSQL.NormalizeDigest(allegrosql)
		}
		return queryStrForLog(allegrosql)
	case allegrosql.ComStmtInterDircute, allegrosql.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return queryStrForLog(cc.preparedStmt2String(stmtID))
	case allegrosql.ComStmtClose, allegrosql.ComStmtReset:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return allegrosql.Command2Str[cmd] + " " + strconv.Itoa(int(stmtID))
	default:
		if cmdStr, ok := allegrosql.Command2Str[cmd]; ok {
			return cmdStr
		}
		return string(replog.String(data))
	}
}

// PProfLabel return allegrosql label used to tag pprof.
func (cc getLastStmtInConn) PProfLabel() string {
	if len(cc.lastPacket) == 0 {
		return ""
	}
	cmd, data := cc.lastPacket[0], cc.lastPacket[1:]
	switch cmd {
	case allegrosql.ComInitDB:
		return "UseDB"
	case allegrosql.ComFieldList:
		return "ListFields"
	case allegrosql.ComStmtClose:
		return "CloseStmt"
	case allegrosql.ComStmtReset:
		return "ResetStmt"
	case allegrosql.ComQuery, allegrosql.ComStmtPrepare:
		return BerolinaSQL.Normalize(queryStrForLog(string(replog.String(data))))
	case allegrosql.ComStmtInterDircute, allegrosql.ComStmtFetch:
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		return queryStrForLog(cc.preparedStmt2StringNoArgs(stmtID))
	default:
		return ""
	}
}
