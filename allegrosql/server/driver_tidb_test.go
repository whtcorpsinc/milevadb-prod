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
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/charset"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/milevadb/types"
)

type milevadbResultSetTestSuite struct{}

var _ = Suite(milevadbResultSetTestSuite{})

func createDeferredCausetByTypeAndLen(tp byte, len uint32) *DeferredCausetInfo {
	return &DeferredCausetInfo{
		Schema:             "test",
		Block:              "dual",
		OrgBlock:           "",
		Name:               "a",
		OrgName:            "a",
		DeferredCausetLength:       len,
		Charset:            uint16(allegrosql.CharsetNameToID(charset.CharsetUTF8)),
		Flag:               uint16(allegrosql.UnsignedFlag),
		Decimal:            uint8(0),
		Type:               tp,
		DefaultValueLength: uint64(0),
		DefaultValue:       nil,
	}
}
func (ts milevadbResultSetTestSuite) TestConvertDeferredCausetInfo(c *C) {
	// Test "allegrosql.TypeBit", for: https://github.com/whtcorpsinc/milevadb/issues/5405.
	resultField := ast.ResultField{
		DeferredCauset: &perceptron.DeferredCausetInfo{
			Name:   perceptron.NewCIStr("a"),
			ID:     0,
			Offset: 0,
			FieldType: types.FieldType{
				Tp:      allegrosql.TypeBit,
				Flag:    allegrosql.UnsignedFlag,
				Flen:    1,
				Decimal: 0,
				Charset: charset.CharsetUTF8,
				DefCauslate: charset.DefCauslationUTF8,
			},
			Comment: "defCausumn a is the first defCausumn in causet dual",
		},
		DeferredCausetAsName: perceptron.NewCIStr("a"),
		BlockAsName:  perceptron.NewCIStr("dual"),
		DBName:       perceptron.NewCIStr("test"),
	}
	defCausInfo := convertDeferredCausetInfo(&resultField)
	c.Assert(defCausInfo, DeepEquals, createDeferredCausetByTypeAndLen(allegrosql.TypeBit, 1))

	// Test "allegrosql.TypeTiny", for: https://github.com/whtcorpsinc/milevadb/issues/5405.
	resultField = ast.ResultField{
		DeferredCauset: &perceptron.DeferredCausetInfo{
			Name:   perceptron.NewCIStr("a"),
			ID:     0,
			Offset: 0,
			FieldType: types.FieldType{
				Tp:      allegrosql.TypeTiny,
				Flag:    allegrosql.UnsignedFlag,
				Flen:    1,
				Decimal: 0,
				Charset: charset.CharsetUTF8,
				DefCauslate: charset.DefCauslationUTF8,
			},
			Comment: "defCausumn a is the first defCausumn in causet dual",
		},
		DeferredCausetAsName: perceptron.NewCIStr("a"),
		BlockAsName:  perceptron.NewCIStr("dual"),
		DBName:       perceptron.NewCIStr("test"),
	}
	defCausInfo = convertDeferredCausetInfo(&resultField)
	c.Assert(defCausInfo, DeepEquals, createDeferredCausetByTypeAndLen(allegrosql.TypeTiny, 1))

	resultField = ast.ResultField{
		DeferredCauset: &perceptron.DeferredCausetInfo{
			Name:   perceptron.NewCIStr("a"),
			ID:     0,
			Offset: 0,
			FieldType: types.FieldType{
				Tp:      allegrosql.TypeYear,
				Flag:    allegrosql.ZerofillFlag,
				Flen:    4,
				Decimal: 0,
				Charset: charset.CharsetBin,
				DefCauslate: charset.DefCauslationBin,
			},
			Comment: "defCausumn a is the first defCausumn in causet dual",
		},
		DeferredCausetAsName: perceptron.NewCIStr("a"),
		BlockAsName:  perceptron.NewCIStr("dual"),
		DBName:       perceptron.NewCIStr("test"),
	}
	defCausInfo = convertDeferredCausetInfo(&resultField)
	c.Assert(defCausInfo.DeferredCausetLength, Equals, uint32(4))
}
