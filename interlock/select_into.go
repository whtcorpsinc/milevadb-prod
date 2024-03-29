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

package interlock

import (
	"bufio"
	"bytes"
	"context"
	"math"
	"os"
	"strconv"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
)

// SelectIntoInterDirc represents a SelectInto interlock.
type SelectIntoInterDirc struct {
	baseInterlockingDirectorate
	intoOpt *ast.SelectIntoOption

	lineBuf   []byte
	realBuf   []byte
	fieldBuf  []byte
	escapeBuf []byte
	enclosed  bool
	writer    *bufio.Writer
	dstFile   *os.File
	chk       *chunk.Chunk
	started   bool
}

// Open implements the InterlockingDirectorate Open interface.
func (s *SelectIntoInterDirc) Open(ctx context.Context) error {
	// only 'select ... into outfile' is supported now
	if s.intoOpt.Tp != ast.SelectIntoOutfile {
		return errors.New("unsupported SelectInto type")
	}

	f, err := os.OpenFile(s.intoOpt.FileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return errors.Trace(err)
	}
	s.started = true
	s.dstFile = f
	s.writer = bufio.NewWriter(s.dstFile)
	s.chk = newFirstChunk(s.children[0])
	s.lineBuf = make([]byte, 0, 1024)
	s.fieldBuf = make([]byte, 0, 64)
	s.escapeBuf = make([]byte, 0, 64)
	return s.baseInterlockingDirectorate.Open(ctx)
}

// Next implements the InterlockingDirectorate Next interface.
func (s *SelectIntoInterDirc) Next(ctx context.Context, req *chunk.Chunk) error {
	for {
		if err := Next(ctx, s.children[0], s.chk); err != nil {
			return err
		}
		if s.chk.NumEvents() == 0 {
			break
		}
		if err := s.dumpToOutfile(); err != nil {
			return err
		}
	}
	return nil
}

func (s *SelectIntoInterDirc) considerEncloseOpt(et types.EvalType) bool {
	return et == types.ETString || et == types.ETDuration ||
		et == types.ETTimestamp || et == types.ETDatetime ||
		et == types.ETJson
}

func (s *SelectIntoInterDirc) escapeField(f []byte) []byte {
	if s.intoOpt.FieldsInfo.Escaped == 0 {
		return f
	}
	s.escapeBuf = s.escapeBuf[:0]
	for _, b := range f {
		escape := false
		switch {
		case b == 0:
			// we always escape 0
			escape = true
			b = '0'
		case b == s.intoOpt.FieldsInfo.Escaped || b == s.intoOpt.FieldsInfo.Enclosed:
			escape = true
		case !s.enclosed && len(s.intoOpt.FieldsInfo.Terminated) > 0 && b == s.intoOpt.FieldsInfo.Terminated[0]:
			// if field is enclosed, we only escape line terminator, otherwise both field and line terminator will be escaped
			escape = true
		case len(s.intoOpt.LinesInfo.Terminated) > 0 && b == s.intoOpt.LinesInfo.Terminated[0]:
			// we always escape line terminator
			escape = true
		}
		if escape {
			s.escapeBuf = append(s.escapeBuf, s.intoOpt.FieldsInfo.Escaped)
		}
		s.escapeBuf = append(s.escapeBuf, b)
	}
	return s.escapeBuf
}

func (s *SelectIntoInterDirc) dumpToOutfile() error {
	lineTerm := "\n"
	if s.intoOpt.LinesInfo.Terminated != "" {
		lineTerm = s.intoOpt.LinesInfo.Terminated
	}
	fieldTerm := "\t"
	if s.intoOpt.FieldsInfo.Terminated != "" {
		fieldTerm = s.intoOpt.FieldsInfo.Terminated
	}
	encloseFlag := false
	var encloseByte byte
	encloseOpt := false
	if s.intoOpt.FieldsInfo.Enclosed != byte(0) {
		encloseByte = s.intoOpt.FieldsInfo.Enclosed
		encloseFlag = true
		encloseOpt = s.intoOpt.FieldsInfo.OptEnclosed
	}
	nullTerm := []byte("\\N")
	if s.intoOpt.FieldsInfo.Escaped != byte(0) {
		nullTerm[0] = s.intoOpt.FieldsInfo.Escaped
	} else {
		nullTerm = []byte("NULL")
	}

	defcaus := s.children[0].Schema().DeferredCausets
	for i := 0; i < s.chk.NumEvents(); i++ {
		event := s.chk.GetEvent(i)
		s.lineBuf = s.lineBuf[:0]
		for j, defCaus := range defcaus {
			if j != 0 {
				s.lineBuf = append(s.lineBuf, fieldTerm...)
			}
			if event.IsNull(j) {
				s.lineBuf = append(s.lineBuf, nullTerm...)
				continue
			}
			et := defCaus.GetType().EvalType()
			if (encloseFlag && !encloseOpt) ||
				(encloseFlag && encloseOpt && s.considerEncloseOpt(et)) {
				s.lineBuf = append(s.lineBuf, encloseByte)
				s.enclosed = true
			} else {
				s.enclosed = false
			}
			s.fieldBuf = s.fieldBuf[:0]
			switch defCaus.GetType().Tp {
			case allegrosql.TypeTiny, allegrosql.TypeShort, allegrosql.TypeInt24, allegrosql.TypeLong:
				s.fieldBuf = strconv.AppendInt(s.fieldBuf, event.GetInt64(j), 10)
			case allegrosql.TypeLonglong:
				if allegrosql.HasUnsignedFlag(defCaus.GetType().Flag) {
					s.fieldBuf = strconv.AppendUint(s.fieldBuf, event.GetUint64(j), 10)
				} else {
					s.fieldBuf = strconv.AppendInt(s.fieldBuf, event.GetInt64(j), 10)
				}
			case allegrosql.TypeFloat, allegrosql.TypeDouble:
				s.realBuf, s.fieldBuf = DumpRealOutfile(s.realBuf, s.fieldBuf, event.GetFloat64(j), defCaus.RetType)
			case allegrosql.TypeNewDecimal:
				s.fieldBuf = append(s.fieldBuf, event.GetMyDecimal(j).String()...)
			case allegrosql.TypeString, allegrosql.TypeVarString, allegrosql.TypeVarchar,
				allegrosql.TypeTinyBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob, allegrosql.TypeBlob:
				s.fieldBuf = append(s.fieldBuf, event.GetBytes(j)...)
			case allegrosql.TypeBit:
				// bit value won't be escaped anyway (verified on MyALLEGROSQL, test case added)
				s.lineBuf = append(s.lineBuf, event.GetBytes(j)...)
			case allegrosql.TypeDate, allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
				s.fieldBuf = append(s.fieldBuf, event.GetTime(j).String()...)
			case allegrosql.TypeDuration:
				s.fieldBuf = append(s.fieldBuf, event.GetDuration(j, defCaus.GetType().Decimal).String()...)
			case allegrosql.TypeEnum:
				s.fieldBuf = append(s.fieldBuf, event.GetEnum(j).String()...)
			case allegrosql.TypeSet:
				s.fieldBuf = append(s.fieldBuf, event.GetSet(j).String()...)
			case allegrosql.TypeJSON:
				s.fieldBuf = append(s.fieldBuf, event.GetJSON(j).String()...)
			}
			s.lineBuf = append(s.lineBuf, s.escapeField(s.fieldBuf)...)
			if (encloseFlag && !encloseOpt) ||
				(encloseFlag && encloseOpt && s.considerEncloseOpt(et)) {
				s.lineBuf = append(s.lineBuf, encloseByte)
			}
		}
		s.lineBuf = append(s.lineBuf, lineTerm...)
		if _, err := s.writer.Write(s.lineBuf); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Close implements the InterlockingDirectorate Close interface.
func (s *SelectIntoInterDirc) Close() error {
	if !s.started {
		return nil
	}
	err1 := s.writer.Flush()
	err2 := s.dstFile.Close()
	err3 := s.baseInterlockingDirectorate.Close()
	if err1 != nil {
		return errors.Trace(err1)
	} else if err2 != nil {
		return errors.Trace(err2)
	}
	return err3
}

const (
	expFormatBig   = 1e15
	expFormatSmall = 1e-15
)

// DumpRealOutfile dumps a real number to lineBuf.
func DumpRealOutfile(realBuf, lineBuf []byte, v float64, tp *types.FieldType) ([]byte, []byte) {
	prec := types.UnspecifiedLength
	if tp.Decimal > 0 && tp.Decimal != allegrosql.NotFixedDec {
		prec = tp.Decimal
	}
	absV := math.Abs(v)
	if prec == types.UnspecifiedLength && (absV >= expFormatBig || (absV != 0 && absV < expFormatSmall)) {
		realBuf = strconv.AppendFloat(realBuf[:0], v, 'e', prec, 64)
		if idx := bytes.IndexByte(realBuf, '+'); idx != -1 {
			lineBuf = append(lineBuf, realBuf[:idx]...)
			lineBuf = append(lineBuf, realBuf[idx+1:]...)
		} else {
			lineBuf = append(lineBuf, realBuf...)
		}
	} else {
		lineBuf = strconv.AppendFloat(lineBuf, v, 'f', prec, 64)
	}
	return realBuf, lineBuf
}
