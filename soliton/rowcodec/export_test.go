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

package rowcodec

import (
	"github.com/whtcorpsinc/milevadb/stochastikctx/stmtctx"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
)

// EncodeFromOldRow encodes a event from an old-format event.
// this method will be used in test.
func EncodeFromOldRow(causetCausetEncoder *CausetEncoder, sc *stmtctx.StatementContext, oldRow, buf []byte) ([]byte, error) {
	if len(oldRow) > 0 && oldRow[0] == CodecVer {
		return oldRow, nil
	}
	causetCausetEncoder.reset()
	for len(oldRow) > 1 {
		var d types.Causet
		var err error
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		defCausID := d.GetInt64()
		oldRow, d, err = codec.DecodeOne(oldRow)
		if err != nil {
			return nil, err
		}
		causetCausetEncoder.appendDefCausVal(defCausID, &d)
	}
	numDefCauss, notNullIdx := causetCausetEncoder.reformatDefCauss()
	err := causetCausetEncoder.encodeRowDefCauss(sc, numDefCauss, notNullIdx)
	if err != nil {
		return nil, err
	}
	return causetCausetEncoder.event.toBytes(buf[:0]), nil
}
