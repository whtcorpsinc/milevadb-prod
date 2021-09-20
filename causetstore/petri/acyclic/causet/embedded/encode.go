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

package embedded

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"sync"

	"github.com/whtcorpsinc/failpoint"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/plancodec"
)

var causetCausetEncoderPool = sync.Pool{
	New: func() interface{} {
		return &planCausetEncoder{}
	},
}

type planCausetEncoder struct {
	buf            bytes.Buffer
	encodedCausets map[int]bool
}

// EncodeCauset is used to encodeCauset the plan to the plan tree with compressing.
func EncodeCauset(p Causet) string {
	pn := causetCausetEncoderPool.Get().(*planCausetEncoder)
	defer causetCausetEncoderPool.Put(pn)
	if p == nil || p.SCtx() == nil {
		return ""
	}
	selectCauset := getSelectCauset(p)
	if selectCauset != nil {
		failpoint.Inject("mockCausetRowCount", func(val failpoint.Value) {
			selectCauset.statsInfo().RowCount = float64(val.(int))
		})
	}
	return pn.encodeCausetTree(p)
}

func (pn *planCausetEncoder) encodeCausetTree(p Causet) string {
	pn.encodedCausets = make(map[int]bool)
	pn.buf.Reset()
	pn.encodeCauset(p, true, ekv.EinsteinDB, 0)
	return plancodec.Compress(pn.buf.Bytes())
}

func (pn *planCausetEncoder) encodeCauset(p Causet, isRoot bool, causetstore ekv.StoreType, depth int) {
	taskTypeInfo := plancodec.EncodeTaskType(isRoot, causetstore)
	actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfo(p.SCtx(), p)
	rowCount := 0.0
	if statsInfo := p.statsInfo(); statsInfo != nil {
		rowCount = p.statsInfo().RowCount
	}
	plancodec.EncodeCausetNode(depth, p.ID(), p.TP(), rowCount, taskTypeInfo, p.ExplainInfo(), actRows, analyzeInfo, memoryInfo, diskInfo, &pn.buf)
	pn.encodedCausets[p.ID()] = true
	depth++

	selectCauset := getSelectCauset(p)
	if selectCauset == nil {
		return
	}
	if !pn.encodedCausets[selectCauset.ID()] {
		pn.encodeCauset(selectCauset, isRoot, causetstore, depth)
		return
	}
	for _, child := range selectCauset.Children() {
		if pn.encodedCausets[child.ID()] {
			continue
		}
		pn.encodeCauset(child.(PhysicalCauset), isRoot, causetstore, depth)
	}
	switch copCauset := selectCauset.(type) {
	case *PhysicalBlockReader:
		pn.encodeCauset(copCauset.blockCauset, false, copCauset.StoreType, depth)
	case *PhysicalIndexReader:
		pn.encodeCauset(copCauset.indexCauset, false, causetstore, depth)
	case *PhysicalIndexLookUpReader:
		pn.encodeCauset(copCauset.indexCauset, false, causetstore, depth)
		pn.encodeCauset(copCauset.blockCauset, false, causetstore, depth)
	case *PhysicalIndexMergeReader:
		for _, p := range copCauset.partialCausets {
			pn.encodeCauset(p, false, causetstore, depth)
		}
		if copCauset.blockCauset != nil {
			pn.encodeCauset(copCauset.blockCauset, false, causetstore, depth)
		}
	}
}

var digesterPool = sync.Pool{
	New: func() interface{} {
		return &planDigester{
			hasher: sha256.New(),
		}
	},
}

type planDigester struct {
	buf            bytes.Buffer
	encodedCausets map[int]bool
	hasher         hash.Hash
}

// NormalizeCauset is used to normalize the plan and generate plan digest.
func NormalizeCauset(p Causet) (normalized, digest string) {
	selectCauset := getSelectCauset(p)
	if selectCauset == nil {
		return "", ""
	}
	d := digesterPool.Get().(*planDigester)
	defer digesterPool.Put(d)
	d.normalizeCausetTree(selectCauset)
	normalized = d.buf.String()
	d.hasher.Write(d.buf.Bytes())
	d.buf.Reset()
	digest = fmt.Sprintf("%x", d.hasher.Sum(nil))
	d.hasher.Reset()
	return
}

func (d *planDigester) normalizeCausetTree(p PhysicalCauset) {
	d.encodedCausets = make(map[int]bool)
	d.buf.Reset()
	d.normalizeCauset(p, true, ekv.EinsteinDB, 0)
}

func (d *planDigester) normalizeCauset(p PhysicalCauset, isRoot bool, causetstore ekv.StoreType, depth int) {
	taskTypeInfo := plancodec.EncodeTaskTypeForNormalize(isRoot, causetstore)
	plancodec.NormalizeCausetNode(depth, p.TP(), taskTypeInfo, p.ExplainNormalizedInfo(), &d.buf)
	d.encodedCausets[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if d.encodedCausets[child.ID()] {
			continue
		}
		d.normalizeCauset(child.(PhysicalCauset), isRoot, causetstore, depth)
	}
	switch x := p.(type) {
	case *PhysicalBlockReader:
		d.normalizeCauset(x.blockCauset, false, x.StoreType, depth)
	case *PhysicalIndexReader:
		d.normalizeCauset(x.indexCauset, false, causetstore, depth)
	case *PhysicalIndexLookUpReader:
		d.normalizeCauset(x.indexCauset, false, causetstore, depth)
		d.normalizeCauset(x.blockCauset, false, causetstore, depth)
	case *PhysicalIndexMergeReader:
		for _, p := range x.partialCausets {
			d.normalizeCauset(p, false, causetstore, depth)
		}
		if x.blockCauset != nil {
			d.normalizeCauset(x.blockCauset, false, causetstore, depth)
		}
	}
}

func getSelectCauset(p Causet) PhysicalCauset {
	var selectCauset PhysicalCauset
	if physicalCauset, ok := p.(PhysicalCauset); ok {
		selectCauset = physicalCauset
	} else {
		switch x := p.(type) {
		case *Delete:
			selectCauset = x.SelectCauset
		case *UFIDelate:
			selectCauset = x.SelectCauset
		case *Insert:
			selectCauset = x.SelectCauset
		}
	}
	return selectCauset
}
