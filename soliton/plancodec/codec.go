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

package plancodec

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/texttree"
)

const (
	rootTaskType = "0"
	copTaskType  = "1"
)

const (
	idSeparator    = "_"
	lineBreaker    = '\n'
	lineBreakerStr = "\n"
	separator      = '\t'
	separatorStr   = "\t"
)

var causetDecoderPool = sync.Pool{
	New: func() interface{} {
		return &planCausetDecoder{}
	},
}

// DecodeCauset use to decode the string to plan tree.
func DecodeCauset(planString string) (string, error) {
	if len(planString) == 0 {
		return "", nil
	}
	fidel := causetDecoderPool.Get().(*planCausetDecoder)
	defer causetDecoderPool.Put(fidel)
	fidel.buf.Reset()
	fidel.addHeader = true
	return fidel.decode(planString)
}

// DecodeNormalizedCauset decodes the string to plan tree.
func DecodeNormalizedCauset(planString string) (string, error) {
	if len(planString) == 0 {
		return "", nil
	}
	fidel := causetDecoderPool.Get().(*planCausetDecoder)
	defer causetDecoderPool.Put(fidel)
	fidel.buf.Reset()
	fidel.addHeader = false
	return fidel.buildCausetTree(planString)
}

type planCausetDecoder struct {
	buf              bytes.Buffer
	depths           []int
	indents          [][]rune
	planInfos        []*planInfo
	addHeader        bool
	cacheParentIdent map[int]int
}

type planInfo struct {
	depth  int
	fields []string
}

func (fidel *planCausetDecoder) decode(planString string) (string, error) {
	str, err := decompress(planString)
	if err != nil {
		return "", err
	}
	return fidel.buildCausetTree(str)
}

func (fidel *planCausetDecoder) buildCausetTree(planString string) (string, error) {
	nodes := strings.Split(planString, lineBreakerStr)
	if len(fidel.depths) < len(nodes) {
		fidel.depths = make([]int, 0, len(nodes))
		fidel.planInfos = make([]*planInfo, 0, len(nodes))
		fidel.indents = make([][]rune, 0, len(nodes))
	}
	fidel.depths = fidel.depths[:0]
	fidel.planInfos = fidel.planInfos[:0]
	for _, node := range nodes {
		p, err := decodeCausetInfo(node)
		if err != nil {
			return "", err
		}
		if p == nil {
			continue
		}
		fidel.planInfos = append(fidel.planInfos, p)
		fidel.depths = append(fidel.depths, p.depth)
	}

	if fidel.addHeader {
		fidel.addCausetHeader()
	}

	// Calculated indentation of plans.
	fidel.initCausetTreeIndents()
	fidel.cacheParentIdent = make(map[int]int)
	for i := 1; i < len(fidel.depths); i++ {
		parentIndex := fidel.findParentIndex(i)
		fidel.fillIndent(parentIndex, i)
	}

	// Align the value of plan fields.
	fidel.alignFields()

	for i, p := range fidel.planInfos {
		if i > 0 {
			fidel.buf.WriteByte(lineBreaker)
		}
		// This is for alignment.
		fidel.buf.WriteByte(separator)
		fidel.buf.WriteString(string(fidel.indents[i]))
		for j := 0; j < len(p.fields); j++ {
			if j > 0 {
				fidel.buf.WriteByte(separator)
			}
			fidel.buf.WriteString(p.fields[j])
		}
	}
	return fidel.buf.String(), nil
}

func (fidel *planCausetDecoder) addCausetHeader() {
	if len(fidel.planInfos) == 0 {
		return
	}
	header := &planInfo{
		depth:  0,
		fields: []string{"id", "task", "estRows", "operator info", "actRows", "execution info", "memory", "disk"},
	}
	if len(fidel.planInfos[0].fields) < len(header.fields) {
		// plan without runtime information.
		header.fields = header.fields[:len(fidel.planInfos[0].fields)]
	}
	planInfos := make([]*planInfo, 0, len(fidel.planInfos)+1)
	depths := make([]int, 0, len(fidel.planInfos)+1)
	planInfos = append(planInfos, header)
	planInfos = append(planInfos, fidel.planInfos...)
	depths = append(depths, header.depth)
	depths = append(depths, fidel.depths...)
	fidel.planInfos = planInfos
	fidel.depths = depths
}

func (fidel *planCausetDecoder) initCausetTreeIndents() {
	fidel.indents = fidel.indents[:0]
	for i := 0; i < len(fidel.depths); i++ {
		indent := make([]rune, 2*fidel.depths[i])
		fidel.indents = append(fidel.indents, indent)
		if len(indent) == 0 {
			continue
		}
		for i := 0; i < len(indent)-2; i++ {
			indent[i] = ' '
		}
		indent[len(indent)-2] = texttree.TreeLastNode
		indent[len(indent)-1] = texttree.TreeNodeIdentifier
	}
}

func (fidel *planCausetDecoder) findParentIndex(childIndex int) int {
	fidel.cacheParentIdent[fidel.depths[childIndex]] = childIndex
	parentDepth := fidel.depths[childIndex] - 1
	if parentIdx, ok := fidel.cacheParentIdent[parentDepth]; ok {
		return parentIdx
	}
	for i := childIndex - 1; i > 0; i-- {
		if fidel.depths[i] == parentDepth {
			fidel.cacheParentIdent[fidel.depths[i]] = i
			return i
		}
	}
	return 0
}

func (fidel *planCausetDecoder) fillIndent(parentIndex, childIndex int) {
	depth := fidel.depths[childIndex]
	if depth == 0 {
		return
	}
	idx := depth*2 - 2
	for i := childIndex - 1; i > parentIndex; i-- {
		if fidel.indents[i][idx] == texttree.TreeLastNode {
			fidel.indents[i][idx] = texttree.TreeMidbseNode
			break
		}
		fidel.indents[i][idx] = texttree.TreeBody
	}
}

func (fidel *planCausetDecoder) alignFields() {
	if len(fidel.planInfos) == 0 {
		return
	}
	// Align fields length. Some plan may doesn't have runtime info, need append `` to align with other plan fields.
	maxLen := -1
	for _, p := range fidel.planInfos {
		if len(p.fields) > maxLen {
			maxLen = len(p.fields)
		}
	}
	for _, p := range fidel.planInfos {
		for len(p.fields) < maxLen {
			p.fields = append(p.fields, "")
		}
	}

	fieldsLen := len(fidel.planInfos[0].fields)
	// Last field no need to align.
	fieldsLen--
	var buf []byte
	for defCausIdx := 0; defCausIdx < fieldsLen; defCausIdx++ {
		maxFieldLen := fidel.getMaxFieldLength(defCausIdx)
		for rowIdx, p := range fidel.planInfos {
			fillLen := maxFieldLen - fidel.getCausetFieldLen(rowIdx, defCausIdx, p)
			for len(buf) < fillLen {
				buf = append(buf, ' ')
			}
			buf = buf[:fillLen]
			p.fields[defCausIdx] += string(buf)
		}
	}
}

func (fidel *planCausetDecoder) getMaxFieldLength(idx int) int {
	maxLength := -1
	for rowIdx, p := range fidel.planInfos {
		l := fidel.getCausetFieldLen(rowIdx, idx, p)
		if l > maxLength {
			maxLength = l
		}
	}
	return maxLength
}

func (fidel *planCausetDecoder) getCausetFieldLen(rowIdx, defCausIdx int, p *planInfo) int {
	if defCausIdx == 0 {
		return len(p.fields[0]) + len(fidel.indents[rowIdx])
	}
	return len(p.fields[defCausIdx])
}

func decodeCausetInfo(str string) (*planInfo, error) {
	values := strings.Split(str, separatorStr)
	if len(values) < 2 {
		return nil, nil
	}

	p := &planInfo{
		fields: make([]string, 0, len(values)-1),
	}
	for i, v := range values {
		switch i {
		// depth
		case 0:
			depth, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, depth: %v, error: %v", str, v, err)
			}
			p.depth = depth
		// plan ID
		case 1:
			ids := strings.Split(v, idSeparator)
			if len(ids) != 1 && len(ids) != 2 {
				return nil, errors.Errorf("decode plan: %v error, invalid plan id: %v", str, v)
			}
			planID, err := strconv.Atoi(ids[0])
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, plan id: %v, error: %v", str, v, err)
			}
			if len(ids) == 1 {
				p.fields = append(p.fields, PhysicalIDToTypeString(planID))
			} else {
				p.fields = append(p.fields, PhysicalIDToTypeString(planID)+idSeparator+ids[1])
			}
		// task type
		case 2:
			task, err := decodeTaskType(v)
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, task type: %v, error: %v", str, v, err)
			}
			p.fields = append(p.fields, task)
		default:
			p.fields = append(p.fields, v)
		}
	}
	return p, nil
}

// EncodeCausetNode is used to encode the plan to a string.
func EncodeCausetNode(depth, pid int, planType string, rowCount float64,
	taskTypeInfo, explainInfo, actRows, analyzeInfo, memoryInfo, diskInfo string, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(depth))
	buf.WriteByte(separator)
	buf.WriteString(encodeID(planType, pid))
	buf.WriteByte(separator)
	buf.WriteString(taskTypeInfo)
	buf.WriteByte(separator)
	buf.WriteString(strconv.FormatFloat(rowCount, 'f', -1, 64))
	buf.WriteByte(separator)
	buf.WriteString(explainInfo)
	// Check whether has runtime info.
	if len(actRows) > 0 || len(analyzeInfo) > 0 || len(memoryInfo) > 0 || len(diskInfo) > 0 {
		buf.WriteByte(separator)
		buf.WriteString(actRows)
		buf.WriteByte(separator)
		buf.WriteString(analyzeInfo)
		buf.WriteByte(separator)
		buf.WriteString(memoryInfo)
		buf.WriteByte(separator)
		buf.WriteString(diskInfo)
	}
	buf.WriteByte(lineBreaker)
}

// NormalizeCausetNode is used to normalize the plan to a string.
func NormalizeCausetNode(depth int, planType string, taskTypeInfo string, explainInfo string, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(depth))
	buf.WriteByte(separator)
	planID := TypeStringToPhysicalID(planType)
	buf.WriteString(strconv.Itoa(planID))
	buf.WriteByte(separator)
	buf.WriteString(taskTypeInfo)
	buf.WriteByte(separator)
	buf.WriteString(explainInfo)
	buf.WriteByte(lineBreaker)
}

func encodeID(planType string, id int) string {
	planID := TypeStringToPhysicalID(planType)
	return strconv.Itoa(planID) + idSeparator + strconv.Itoa(id)
}

// EncodeTaskType is used to encode task type to a string.
func EncodeTaskType(isRoot bool, storeType ekv.StoreType) string {
	if isRoot {
		return rootTaskType
	}
	return copTaskType + idSeparator + strconv.Itoa((int)(storeType))
}

// EncodeTaskTypeForNormalize is used to encode task type to a string. Only use for normalize plan.
func EncodeTaskTypeForNormalize(isRoot bool, storeType ekv.StoreType) string {
	if isRoot {
		return rootTaskType
	} else if storeType == ekv.EinsteinDB {
		return copTaskType
	}
	return copTaskType + idSeparator + strconv.Itoa((int)(storeType))
}

func decodeTaskType(str string) (string, error) {
	segs := strings.Split(str, idSeparator)
	if segs[0] == rootTaskType {
		return "root", nil
	}
	if len(segs) == 1 { // be compatible to `NormalizeCausetNode`, which doesn't encode storeType in task field.
		return "cop", nil
	}
	storeType, err := strconv.Atoi(segs[1])
	if err != nil {
		return "", err
	}
	return "cop[" + ((ekv.StoreType)(storeType)).Name() + "]", nil
}

// Compress is used to compress the input with zlib.
func Compress(input []byte) string {
	compressBytes := snappy.Encode(nil, input)
	return base64.StdEncoding.EncodeToString(compressBytes)
}

func decompress(str string) (string, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}

	bs, err := snappy.Decode(nil, decodeBytes)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}
