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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime/trace"
	"strconv"
	"time"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/milevadb/config"
	plannercore "github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/execdetails"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
)

func (cc *clientConn) handleStmtPrepare(ctx context.Context, allegrosql string) error {
	stmt, defCausumns, params, err := cc.ctx.Prepare(allegrosql)
	if err != nil {
		return err
	}
	data := make([]byte, 4, 128)

	//status ok
	data = append(data, 0)
	//stmt id
	data = dumpUint32(data, uint32(stmt.ID()))
	//number defCausumns
	data = dumpUint16(data, uint16(len(defCausumns)))
	//number params
	data = dumpUint16(data, uint16(len(params)))
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0) //TODO support warning count

	if err := cc.writePacket(data); err != nil {
		return err
	}

	if len(params) > 0 {
		for i := 0; i < len(params); i++ {
			data = data[0:4]
			data = params[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(0); err != nil {
			return err
		}
	}

	if len(defCausumns) > 0 {
		for i := 0; i < len(defCausumns); i++ {
			data = data[0:4]
			data = defCausumns[i].Dump(data)

			if err := cc.writePacket(data); err != nil {
				return err
			}
		}

		if err := cc.writeEOF(0); err != nil {
			return err
		}

	}
	return cc.flush(ctx)
}

func (cc *clientConn) handleStmtExecute(ctx context.Context, data []byte) (err error) {
	defer trace.StartRegion(ctx, "HandleStmtExecute").End()
	if len(data) < 9 {
		return allegrosql.ErrMalformPacket
	}
	pos := 0
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmt := cc.ctx.GetStatement(int(stmtID))
	if stmt == nil {
		return allegrosql.NewErr(allegrosql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_execute")
	}

	flag := data[pos]
	pos++
	// Please refer to https://dev.allegrosql.com/doc/internals/en/com-stmt-execute.html
	// The client indicates that it wants to use cursor by setting this flag.
	// 0x00 CURSOR_TYPE_NO_CURSOR
	// 0x01 CURSOR_TYPE_READ_ONLY
	// 0x02 CURSOR_TYPE_FOR_UFIDelATE
	// 0x04 CURSOR_TYPE_SCROLLABLE
	// Now we only support forward-only, read-only cursor.
	var useCursor bool
	switch flag {
	case 0:
		useCursor = false
	case 1:
		useCursor = true
	default:
		return allegrosql.NewErrf(allegrosql.ErrUnknown, "unsupported flag %d", flag)
	}

	// skip iteration-count, always 1
	pos += 4

	var (
		nullBitmaps []byte
		paramTypes  []byte
		paramValues []byte
	)
	numParams := stmt.NumParams()
	args := make([]types.Causet, numParams)
	if numParams > 0 {
		nullBitmapLen := (numParams + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return allegrosql.ErrMalformPacket
		}
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		// new param bound flag
		if data[pos] == 1 {
			pos++
			if len(data) < (pos + (numParams << 1)) {
				return allegrosql.ErrMalformPacket
			}

			paramTypes = data[pos : pos+(numParams<<1)]
			pos += numParams << 1
			paramValues = data[pos:]
			// Just the first StmtExecute packet contain parameters type,
			// we need save it for further use.
			stmt.SetParamsType(paramTypes)
		} else {
			paramValues = data[pos+1:]
		}

		err = parseExecArgs(cc.ctx.GetStochastikVars().StmtCtx, args, stmt.BoundParams(), nullBitmaps, stmt.GetParamsType(), paramValues)
		stmt.Reset()
		if err != nil {
			return errors.Annotate(err, cc.preparedStmt2String(stmtID))
		}
	}
	ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, &execdetails.StmtExecDetails{})
	rs, err := stmt.Execute(ctx, args)
	if err != nil {
		return errors.Annotate(err, cc.preparedStmt2String(stmtID))
	}
	if rs == nil {
		return cc.writeOK(ctx)
	}

	// if the client wants to use cursor
	// we should hold the ResultSet in PreparedStatement for next stmt_fetch, and only send back DeferredCausetInfo.
	// Tell the client cursor exists in server by setting proper serverStatus.
	if useCursor {
		stmt.StoreResultSet(rs)
		err = cc.writeDeferredCausetInfo(rs.DeferredCausets(), allegrosql.ServerStatusCursorExists)
		if err != nil {
			return err
		}
		if cl, ok := rs.(fetchNotifier); ok {
			cl.OnFetchReturned()
		}
		// explicitly flush defCausumnInfo to client.
		return cc.flush(ctx)
	}
	defer terror.Call(rs.Close)
	err = cc.writeResultset(ctx, rs, true, 0, 0)
	if err != nil {
		return errors.Annotate(err, cc.preparedStmt2String(stmtID))
	}
	return nil
}

// maxFetchSize constants
const (
	maxFetchSize = 1024
)

func (cc *clientConn) handleStmtFetch(ctx context.Context, data []byte) (err error) {
	cc.ctx.GetStochastikVars().StartTime = time.Now()

	stmtID, fetchSize, err := parseStmtFetchCmd(data)
	if err != nil {
		return err
	}

	stmt := cc.ctx.GetStatement(int(stmtID))
	if stmt == nil {
		return errors.Annotate(allegrosql.NewErr(allegrosql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_fetch"), cc.preparedStmt2String(stmtID))
	}
	allegrosql := ""
	if prepared, ok := cc.ctx.GetStatement(int(stmtID)).(*MilevaDBStatement); ok {
		allegrosql = prepared.allegrosql
	}
	cc.ctx.SetProcessInfo(allegrosql, time.Now(), allegrosql.ComStmtExecute, 0)
	rs := stmt.GetResultSet()
	if rs == nil {
		return errors.Annotate(allegrosql.NewErr(allegrosql.ErrUnknownStmtHandler,
			strconv.FormatUint(uint64(stmtID), 10), "stmt_fetch_rs"), cc.preparedStmt2String(stmtID))
	}

	err = cc.writeResultset(ctx, rs, true, allegrosql.ServerStatusCursorExists, int(fetchSize))
	if err != nil {
		return errors.Annotate(err, cc.preparedStmt2String(stmtID))
	}
	return nil
}

func parseStmtFetchCmd(data []byte) (uint32, uint32, error) {
	if len(data) != 8 {
		return 0, 0, allegrosql.ErrMalformPacket
	}
	// Please refer to https://dev.allegrosql.com/doc/internals/en/com-stmt-fetch.html
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	fetchSize := binary.LittleEndian.Uint32(data[4:8])
	if fetchSize > maxFetchSize {
		fetchSize = maxFetchSize
	}
	return stmtID, fetchSize, nil
}

func parseExecArgs(sc *stmtctx.StatementContext, args []types.Causet, boundParams [][]byte, nullBitmap, paramTypes, paramValues []byte) (err error) {
	pos := 0
	var (
		tmp    interface{}
		v      []byte
		n      int
		isNull bool
	)

	for i := 0; i < len(args); i++ {
		// if params had received via ComStmtSendLongData, use them directly.
		// ref https://dev.allegrosql.com/doc/internals/en/com-stmt-send-long-data.html
		// see clientConn#handleStmtSendLongData
		if boundParams[i] != nil {
			args[i] = types.NewBytesCauset(boundParams[i])
			continue
		}

		// check nullBitMap to determine the NULL arguments.
		// ref https://dev.allegrosql.com/doc/internals/en/com-stmt-execute.html
		// notice: some client(e.g. mariadb) will set nullBitMap even if data had be sent via ComStmtSendLongData,
		// so this check need place after boundParam's check.
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			var nilCauset types.Causet
			nilCauset.SetNull()
			args[i] = nilCauset
			continue
		}

		if (i<<1)+1 >= len(paramTypes) {
			return allegrosql.ErrMalformPacket
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case allegrosql.TypeNull:
			var nilCauset types.Causet
			nilCauset.SetNull()
			args[i] = nilCauset
			continue

		case allegrosql.TypeTiny:
			if len(paramValues) < (pos + 1) {
				err = allegrosql.ErrMalformPacket
				return
			}

			if isUnsigned {
				args[i] = types.NewUintCauset(uint64(paramValues[pos]))
			} else {
				args[i] = types.NewIntCauset(int64(int8(paramValues[pos])))
			}

			pos++
			continue

		case allegrosql.TypeShort, allegrosql.TypeYear:
			if len(paramValues) < (pos + 2) {
				err = allegrosql.ErrMalformPacket
				return
			}
			valU16 := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
			if isUnsigned {
				args[i] = types.NewUintCauset(uint64(valU16))
			} else {
				args[i] = types.NewIntCauset(int64(int16(valU16)))
			}
			pos += 2
			continue

		case allegrosql.TypeInt24, allegrosql.TypeLong:
			if len(paramValues) < (pos + 4) {
				err = allegrosql.ErrMalformPacket
				return
			}
			valU32 := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
			if isUnsigned {
				args[i] = types.NewUintCauset(uint64(valU32))
			} else {
				args[i] = types.NewIntCauset(int64(int32(valU32)))
			}
			pos += 4
			continue

		case allegrosql.TypeLonglong:
			if len(paramValues) < (pos + 8) {
				err = allegrosql.ErrMalformPacket
				return
			}
			valU64 := binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			if isUnsigned {
				args[i] = types.NewUintCauset(valU64)
			} else {
				args[i] = types.NewIntCauset(int64(valU64))
			}
			pos += 8
			continue

		case allegrosql.TypeFloat:
			if len(paramValues) < (pos + 4) {
				err = allegrosql.ErrMalformPacket
				return
			}

			args[i] = types.NewFloat32Causet(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case allegrosql.TypeDouble:
			if len(paramValues) < (pos + 8) {
				err = allegrosql.ErrMalformPacket
				return
			}

			args[i] = types.NewFloat64Causet(math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8])))
			pos += 8
			continue

		case allegrosql.TypeDate, allegrosql.TypeTimestamp, allegrosql.TypeDatetime:
			if len(paramValues) < (pos + 1) {
				err = allegrosql.ErrMalformPacket
				return
			}
			// See https://dev.allegrosql.com/doc/internals/en/binary-protodefCaus-value.html
			// for more details.
			length := paramValues[pos]
			pos++
			switch length {
			case 0:
				tmp = types.ZeroDatetimeStr
			case 4:
				pos, tmp = parseBinaryDate(pos, paramValues)
			case 7:
				pos, tmp = parseBinaryDateTime(pos, paramValues)
			case 11:
				pos, tmp = parseBinaryTimestamp(pos, paramValues)
			default:
				err = allegrosql.ErrMalformPacket
				return
			}
			args[i] = types.NewCauset(tmp) // FIXME: After check works!!!!!!
			continue

		case allegrosql.TypeDuration:
			if len(paramValues) < (pos + 1) {
				err = allegrosql.ErrMalformPacket
				return
			}
			// See https://dev.allegrosql.com/doc/internals/en/binary-protodefCaus-value.html
			// for more details.
			length := paramValues[pos]
			pos++
			switch length {
			case 0:
				tmp = "0"
			case 8:
				isNegative := paramValues[pos]
				if isNegative > 1 {
					err = allegrosql.ErrMalformPacket
					return
				}
				pos++
				pos, tmp = parseBinaryDuration(pos, paramValues, isNegative)
			case 12:
				isNegative := paramValues[pos]
				if isNegative > 1 {
					err = allegrosql.ErrMalformPacket
					return
				}
				pos++
				pos, tmp = parseBinaryDurationWithMS(pos, paramValues, isNegative)
			default:
				err = allegrosql.ErrMalformPacket
				return
			}
			args[i] = types.NewCauset(tmp)
			continue
		case allegrosql.TypeNewDecimal:
			if len(paramValues) < (pos + 1) {
				err = allegrosql.ErrMalformPacket
				return
			}

			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if isNull {
				args[i] = types.NewDecimalCauset(nil)
			} else {
				var dec types.MyDecimal
				err = sc.HandleTruncate(dec.FromString(v))
				if err != nil {
					return err
				}
				args[i] = types.NewDecimalCauset(&dec)
			}
			continue
		case allegrosql.TypeBlob, allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
			if len(paramValues) < (pos + 1) {
				err = allegrosql.ErrMalformPacket
				return
			}
			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if isNull {
				args[i] = types.NewBytesCauset(nil)
			} else {
				args[i] = types.NewBytesCauset(v)
			}
			continue
		case allegrosql.TypeUnspecified, allegrosql.TypeVarchar, allegrosql.TypeVarString, allegrosql.TypeString,
			allegrosql.TypeEnum, allegrosql.TypeSet, allegrosql.TypeGeometry, allegrosql.TypeBit:
			if len(paramValues) < (pos + 1) {
				err = allegrosql.ErrMalformPacket
				return
			}

			v, isNull, n, err = parseLengthEncodedBytes(paramValues[pos:])
			pos += n
			if err != nil {
				return
			}

			if !isNull {
				tmp = string(replog.String(v))
			} else {
				tmp = nil
			}
			args[i] = types.NewCauset(tmp)
			continue
		default:
			err = errUnknownFieldType.GenWithStack("stmt unknown field type %d", tp)
			return
		}
	}
	return
}

func parseBinaryDate(pos int, paramValues []byte) (int, string) {
	year := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
	pos += 2
	month := paramValues[pos]
	pos++
	day := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func parseBinaryDateTime(pos int, paramValues []byte) (int, string) {
	pos, date := parseBinaryDate(pos, paramValues)
	hour := paramValues[pos]
	pos++
	minute := paramValues[pos]
	pos++
	second := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func parseBinaryTimestamp(pos int, paramValues []byte) (int, string) {
	pos, dateTime := parseBinaryDateTime(pos, paramValues)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func parseBinaryDuration(pos int, paramValues []byte, isNegative uint8) (int, string) {
	sign := ""
	if isNegative == 1 {
		sign = "-"
	}
	days := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	hours := paramValues[pos]
	pos++
	minutes := paramValues[pos]
	pos++
	seconds := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s%d %02d:%02d:%02d", sign, days, hours, minutes, seconds)
}

func parseBinaryDurationWithMS(pos int, paramValues []byte,
	isNegative uint8) (int, string) {
	pos, dur := parseBinaryDuration(pos, paramValues, isNegative)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dur, microSecond)
}

func (cc *clientConn) handleStmtClose(data []byte) (err error) {
	if len(data) < 4 {
		return
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt != nil {
		return stmt.Close()
	}
	return
}

func (cc *clientConn) handleStmtSendLongData(data []byte) (err error) {
	if len(data) < 6 {
		return allegrosql.ErrMalformPacket
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))

	stmt := cc.ctx.GetStatement(stmtID)
	if stmt == nil {
		return allegrosql.NewErr(allegrosql.ErrUnknownStmtHandler,
			strconv.Itoa(stmtID), "stmt_send_longdata")
	}

	paramID := int(binary.LittleEndian.Uint16(data[4:6]))
	return stmt.AppendParam(paramID, data[6:])
}

func (cc *clientConn) handleStmtReset(ctx context.Context, data []byte) (err error) {
	if len(data) < 4 {
		return allegrosql.ErrMalformPacket
	}

	stmtID := int(binary.LittleEndian.Uint32(data[0:4]))
	stmt := cc.ctx.GetStatement(stmtID)
	if stmt == nil {
		return allegrosql.NewErr(allegrosql.ErrUnknownStmtHandler,
			strconv.Itoa(stmtID), "stmt_reset")
	}
	stmt.Reset()
	stmt.StoreResultSet(nil)
	return cc.writeOK(ctx)
}

// handleSetOption refer to https://dev.allegrosql.com/doc/internals/en/com-set-option.html
func (cc *clientConn) handleSetOption(ctx context.Context, data []byte) (err error) {
	if len(data) < 2 {
		return allegrosql.ErrMalformPacket
	}

	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		cc.capability |= allegrosql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	case 1:
		cc.capability &^= allegrosql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	default:
		return allegrosql.ErrMalformPacket
	}
	if err = cc.writeEOF(0); err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) preparedStmt2String(stmtID uint32) string {
	sv := cc.ctx.GetStochastikVars()
	if sv == nil {
		return ""
	}
	if config.RedactLogEnabled() {
		return cc.preparedStmt2StringNoArgs(stmtID)
	}
	return cc.preparedStmt2StringNoArgs(stmtID) + sv.PreparedParams.String()
}

func (cc *clientConn) preparedStmt2StringNoArgs(stmtID uint32) string {
	sv := cc.ctx.GetStochastikVars()
	if sv == nil {
		return ""
	}
	preparedPointer, ok := sv.PreparedStmts[stmtID]
	if !ok {
		return "prepared statement not found, ID: " + strconv.FormatUint(uint64(stmtID), 10)
	}
	preparedObj, ok := preparedPointer.(*plannercore.CachedPrepareStmt)
	if !ok {
		return "invalidate CachedPrepareStmt type, ID: " + strconv.FormatUint(uint64(stmtID), 10)
	}
	preparedAst := preparedObj.PreparedAst
	return preparedAst.Stmt.Text()
}
