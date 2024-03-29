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

package aggfuncs_test

import (
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"

	"github.com/whtcorpsinc/milevadb/interlock/aggfuncs"
)

func (s *testSuite) TestMemNtile(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncNtile, allegrosql.TypeLonglong, 1, 1, 1,
			aggfuncs.DefPartialResult4Ntile, defaultUFIDelateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNtile, allegrosql.TypeLonglong, 1, 3, 0,
			aggfuncs.DefPartialResult4Ntile, defaultUFIDelateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNtile, allegrosql.TypeLonglong, 1, 4, 1,
			aggfuncs.DefPartialResult4Ntile, defaultUFIDelateMemDeltaGens),
	}
	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}
}
