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
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
)

const maxDeferredCausetNameSize = 256

// DeferredCausetInfo contains information of a defCausumn
type DeferredCausetInfo struct {
	Schema             string
	Block              string
	OrgBlock           string
	Name               string
	OrgName            string
	DeferredCausetLength       uint32
	Charset            uint16
	Flag               uint16
	Decimal            uint8
	Type               uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

// Dump dumps DeferredCausetInfo to bytes.
func (defCausumn *DeferredCausetInfo) Dump(buffer []byte) []byte {
	nameDump, orgnameDump := []byte(defCausumn.Name), []byte(defCausumn.OrgName)
	if len(nameDump) > maxDeferredCausetNameSize {
		nameDump = nameDump[0:maxDeferredCausetNameSize]
	}
	if len(orgnameDump) > maxDeferredCausetNameSize {
		orgnameDump = orgnameDump[0:maxDeferredCausetNameSize]
	}
	buffer = dumpLengthEncodedString(buffer, []byte("def"))
	buffer = dumpLengthEncodedString(buffer, []byte(defCausumn.Schema))
	buffer = dumpLengthEncodedString(buffer, []byte(defCausumn.Block))
	buffer = dumpLengthEncodedString(buffer, []byte(defCausumn.OrgBlock))
	buffer = dumpLengthEncodedString(buffer, nameDump)
	buffer = dumpLengthEncodedString(buffer, orgnameDump)

	buffer = append(buffer, 0x0c)

	buffer = dumpUint16(buffer, defCausumn.Charset)
	buffer = dumpUint32(buffer, defCausumn.DeferredCausetLength)
	buffer = append(buffer, dumpType(defCausumn.Type))
	buffer = dumpUint16(buffer, dumpFlag(defCausumn.Type, defCausumn.Flag))
	buffer = append(buffer, defCausumn.Decimal)
	buffer = append(buffer, 0, 0)

	if defCausumn.DefaultValue != nil {
		buffer = dumpUint64(buffer, uint64(len(defCausumn.DefaultValue)))
		buffer = append(buffer, defCausumn.DefaultValue...)
	}

	return buffer
}

func dumpFlag(tp byte, flag uint16) uint16 {
	switch tp {
	case allegrosql.TypeSet:
		return flag | uint16(allegrosql.SetFlag)
	case allegrosql.TypeEnum:
		return flag | uint16(allegrosql.EnumFlag)
	default:
		if allegrosql.HasBinaryFlag(uint(flag)) {
			return flag | uint16(allegrosql.NotNullFlag)
		}
		return flag
	}
}

func dumpType(tp byte) byte {
	switch tp {
	case allegrosql.TypeSet, allegrosql.TypeEnum:
		return allegrosql.TypeString
	default:
		return tp
	}
}
