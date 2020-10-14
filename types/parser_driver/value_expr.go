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

package driver

import (
	"fmt"
	"io"
	"strconv"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/format"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/replog"
)

// The purpose of driver package is to decompose the dependency of the BerolinaSQL and
// types package.
// It provides the NewValueExpr function for the ast package, so the ast package
// do not depends on the concrete definition of `types.Causet`, thus get rid of
// the dependency of the types package.
// The BerolinaSQL package depends on the ast package, but not the types package.
// The whole relationship:
// ast imports []
// milevadb/types imports [BerolinaSQL/types]
// BerolinaSQL imports [ast, BerolinaSQL/types]
// driver imports [ast, milevadb/types]
// milevadb imports [BerolinaSQL, driver]

func init() {
	ast.NewValueExpr = newValueExpr
	ast.NewParamMarkerExpr = newParamMarkerExpr
	ast.NewDecimal = func(str string) (interface{}, error) {
		dec := new(types.MyDecimal)
		err := dec.FromString(replog.Slice(str))
		return dec, err
	}
	ast.NewHexLiteral = func(str string) (interface{}, error) {
		h, err := types.NewHexLiteral(str)
		return h, err
	}
	ast.NewBitLiteral = func(str string) (interface{}, error) {
		b, err := types.NewBitLiteral(str)
		return b, err
	}
}

var (
	_ ast.ParamMarkerExpr = &ParamMarkerExpr{}
	_ ast.ValueExpr       = &ValueExpr{}
)

// ValueExpr is the simple value expression.
type ValueExpr struct {
	ast.TexprNode
	types.Causet
	projectionOffset int
}

// SetValue implements interface of ast.ValueExpr.
func (n *ValueExpr) SetValue(res interface{}) {
	n.Causet.SetValueWithDefaultDefCauslation(res)
}

// Restore implements Node interface.
func (n *ValueExpr) Restore(ctx *format.RestoreCtx) error {
	switch n.HoTT() {
	case types.HoTTNull:
		ctx.WriteKeyWord("NULL")
	case types.HoTTInt64:
		if n.Type.Flag&allegrosql.IsBooleanFlag != 0 {
			if n.GetInt64() > 0 {
				ctx.WriteKeyWord("TRUE")
			} else {
				ctx.WriteKeyWord("FALSE")
			}
		} else {
			ctx.WritePlain(strconv.FormatInt(n.GetInt64(), 10))
		}
	case types.HoTTUint64:
		ctx.WritePlain(strconv.FormatUint(n.GetUint64(), 10))
	case types.HoTTFloat32:
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 32))
	case types.HoTTFloat64:
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64))
	case types.HoTTString:
		// TODO: Try other method to restore the character set introducer. For example, add a field in ValueExpr.
		ctx.WriteString(n.GetString())
	case types.HoTTBytes:
		ctx.WriteString(n.GetString())
	case types.HoTTMysqlDecimal:
		ctx.WritePlain(n.GetMysqlDecimal().String())
	case types.HoTTBinaryLiteral:
		if n.Type.Flag&allegrosql.UnsignedFlag != 0 {
			ctx.WritePlainf("x'%x'", n.GetBytes())
		} else {
			ctx.WritePlain(n.GetBinaryLiteral().ToBitLiteralString(true))
		}
	case types.HoTTMysqlDuration:
		ctx.WritePlainf("'%s'", n.GetMysqlDuration())
	case types.HoTTMysqlTime:
		ctx.WritePlainf("'%s'", n.GetMysqlTime())
	case types.HoTTMysqlEnum,
		types.HoTTMysqlBit, types.HoTTMysqlSet,
		types.HoTTInterface, types.HoTTMinNotNull, types.HoTTMaxValue,
		types.HoTTRaw, types.HoTTMysqlJSON:
		// TODO implement Restore function
		return errors.New("Not implemented")
	default:
		return errors.New("can't format to string")
	}
	return nil
}

// GetCausetString implements the ast.ValueExpr interface.
func (n *ValueExpr) GetCausetString() string {
	return n.GetString()
}

// Format the ExprNode into a Writer.
func (n *ValueExpr) Format(w io.Writer) {
	var s string
	switch n.HoTT() {
	case types.HoTTNull:
		s = "NULL"
	case types.HoTTInt64:
		if n.Type.Flag&allegrosql.IsBooleanFlag != 0 {
			if n.GetInt64() > 0 {
				s = "TRUE"
			} else {
				s = "FALSE"
			}
		} else {
			s = strconv.FormatInt(n.GetInt64(), 10)
		}
	case types.HoTTUint64:
		s = strconv.FormatUint(n.GetUint64(), 10)
	case types.HoTTFloat32:
		s = strconv.FormatFloat(n.GetFloat64(), 'e', -1, 32)
	case types.HoTTFloat64:
		s = strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64)
	case types.HoTTString, types.HoTTBytes:
		s = strconv.Quote(n.GetString())
	case types.HoTTMysqlDecimal:
		s = n.GetMysqlDecimal().String()
	case types.HoTTBinaryLiteral:
		if n.Type.Flag&allegrosql.UnsignedFlag != 0 {
			s = fmt.Sprintf("x'%x'", n.GetBytes())
		} else {
			s = n.GetBinaryLiteral().ToBitLiteralString(true)
		}
	default:
		panic("Can't format to string")
	}
	fmt.Fprint(w, s)
}

// newValueExpr creates a ValueExpr with value, and sets default field type.
func newValueExpr(value interface{}, charset string, collate string) ast.ValueExpr {
	if ve, ok := value.(*ValueExpr); ok {
		return ve
	}
	ve := &ValueExpr{}
	// We need to keep the ve.Type.DefCauslate equals to ve.Causet.collation.
	types.DefaultTypeForValue(value, &ve.Type, charset, collate)
	ve.Causet.SetValue(value, &ve.Type)
	ve.projectionOffset = -1
	return ve
}

// SetProjectionOffset sets ValueExpr.projectionOffset for logical plan builder.
func (n *ValueExpr) SetProjectionOffset(offset int) {
	n.projectionOffset = offset
}

// GetProjectionOffset returns ValueExpr.projectionOffset.
func (n *ValueExpr) GetProjectionOffset() int {
	return n.projectionOffset
}

// Accept implements Node interface.
func (n *ValueExpr) Accept(v ast.Visitor) (ast.Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ValueExpr)
	return v.Leave(n)
}

// ParamMarkerExpr expression holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarkerExpr struct {
	ValueExpr
	Offset    int
	Order     int
	InExecute bool
}

// Restore implements Node interface.
func (n *ParamMarkerExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain("?")
	return nil
}

func newParamMarkerExpr(offset int) ast.ParamMarkerExpr {
	return &ParamMarkerExpr{
		Offset: offset,
	}
}

// Format the ExprNode into a Writer.
func (n *ParamMarkerExpr) Format(w io.Writer) {
	panic("Not implemented")
}

// Accept implements Node Accept interface.
func (n *ParamMarkerExpr) Accept(v ast.Visitor) (ast.Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ParamMarkerExpr)
	return v.Leave(n)
}

// SetOrder implements the ast.ParamMarkerExpr interface.
func (n *ParamMarkerExpr) SetOrder(order int) {
	n.Order = order
}
