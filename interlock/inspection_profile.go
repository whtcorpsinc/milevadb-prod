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
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/sqlexec"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

const (
	dateTimeFormat = "2006-01-02 15:04:05"
)

type profileBuilder struct {
	sctx            stochastikctx.Context
	idMap           map[string]uint64
	idSlabPredictor uint64
	totalValue      float64
	uniqueMap       map[string]struct{}
	buf             *bytes.Buffer
	start           time.Time
	end             time.Time
	valueTP         metricValueType
}

type metricNode struct {
	causet     string
	name       string
	label      []string
	condition  string
	labelValue map[string]*metricValue
	value      *metricValue
	unit       int64
	children   []*metricNode
	// isPartOfParent indicates the parent of this node not fully contain this node.
	isPartOfParent bool
	initialized    bool
}

type metricValue struct {
	sum     float64
	count   int
	avgP99  float64
	avgP90  float64
	avgP80  float64
	comment string
}

type metricValueType int

const (
	metricValueSum metricValueType = iota + 1
	metricValueAvg
	metricValueCnt
)

func (m metricValueType) String() string {
	switch m {
	case metricValueAvg:
		return "avg"
	case metricValueCnt:
		return "count"
	default:
		return "sum"
	}
}

func (m *metricValue) getValue(tp metricValueType) float64 {
	timeValue := 0.0
	switch tp {
	case metricValueCnt:
		return float64(m.count)
	case metricValueSum:
		timeValue = m.sum
	case metricValueAvg:
		if m.count == 0 {
			return 0.0
		}
		timeValue = m.sum / float64(m.count)
	default:
		panic("should never happen")
	}
	if math.IsNaN(timeValue) {
		return 0
	}
	return timeValue
}

func (m *metricValue) getComment() string {
	if m.count == 0 {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString(m.comment)
	buf.WriteString("\n\n")
	buf.WriteString("total_time: ")
	buf.WriteString(time.Duration(int64(m.sum * float64(time.Second))).String())
	buf.WriteByte('\n')
	buf.WriteString("total_count: ")
	buf.WriteString(strconv.Itoa(m.count))
	buf.WriteByte('\n')
	buf.WriteString("avg_time: ")
	buf.WriteString(time.Duration(int64(m.sum / float64(m.count) * float64(time.Second))).String())
	buf.WriteByte('\n')
	buf.WriteString("avgP99: ")
	buf.WriteString(time.Duration(int64(m.avgP99 * float64(time.Second))).String())
	buf.WriteByte('\n')
	buf.WriteString("avgP90: ")
	buf.WriteString(time.Duration(int64(m.avgP90 * float64(time.Second))).String())
	buf.WriteByte('\n')
	buf.WriteString("avgP80: ")
	buf.WriteString(time.Duration(int64(m.avgP80 * float64(time.Second))).String())
	return buf.String()
}

func (n *metricNode) getName(label string) string {
	name := n.causet
	if n.name != "" {
		name = n.name
	}
	if len(label) != 0 {
		name = name + "." + label
	}
	return name
}

func (n *metricNode) getValue(pb *profileBuilder) (*metricValue, error) {
	if !n.initialized {
		n.initialized = true
		err := n.initializeMetricValue(pb)
		if err != nil {
			return nil, err
		}
	}
	return n.value, nil
}

func (n *metricNode) getLabelValue(label string) *metricValue {
	value, ok := n.labelValue[label]
	if !ok {
		value = &metricValue{}
		n.labelValue[label] = value
	}
	return value
}

func (n *metricNode) queryEventsByLabel(pb *profileBuilder, query string, handleEventFn func(label string, v float64)) error {
	rows, _, err := pb.sctx.(sqlexec.RestrictedALLEGROSQLInterlockingDirectorate).InterDircRestrictedALLEGROSQLWithContext(context.Background(), query)
	if err != nil {
		return err
	}
	if len(rows) == 0 || rows[0].Len() == 0 {
		return nil
	}

	for _, event := range rows {
		v := event.GetFloat64(0)
		if n.unit != 0 {
			v = v / float64(n.unit)
		}
		label := ""
		for i := 1; i < event.Len(); i++ {
			if i > 1 {
				label += ","
			}
			label += event.GetString(i)
		}
		if label == "" && len(n.label) > 0 {
			continue
		}
		handleEventFn(label, v)
	}
	return nil
}

func (n *metricNode) initializeMetricValue(pb *profileBuilder) error {
	n.labelValue = make(map[string]*metricValue)
	n.value = &metricValue{}
	queryCondition := fmt.Sprintf("where time >= '%v' and time <= '%v' and value is not null and value>0",
		pb.start.Format(dateTimeFormat), pb.end.Format(dateTimeFormat))
	if n.condition != "" {
		queryCondition += (" and " + n.condition)
	}

	var query string
	// 1. Get total count value.
	if len(n.label) == 0 {
		query = fmt.Sprintf("select sum(value), '' from `metrics_schema`.`%v_total_count` %v", n.causet, queryCondition)
	} else {
		query = fmt.Sprintf("select sum(value), `%[3]s` from `metrics_schema`.`%[1]s_total_count` %[2]s group by `%[3]s` having sum(value) > 0",
			n.causet, queryCondition, strings.Join(n.label, "`,`"))
	}

	totalCount := 0.0
	err := n.queryEventsByLabel(pb, query, func(label string, v float64) {
		totalCount += v
		n.getLabelValue(label).count = int(v)
	})
	if err != nil {
		return err
	}
	// Return early if the metric doesn't has value
	if int(totalCount) == 0 {
		return nil
	}
	n.value.count = int(totalCount)

	// 2. Get total sum time.
	if len(n.label) == 0 {
		query = fmt.Sprintf("select sum(value), '' from `metrics_schema`.`%v_total_time` %v", n.causet, queryCondition)
	} else {
		query = fmt.Sprintf("select sum(value), `%[3]s` from `metrics_schema`.`%[1]s_total_time` %[2]s group by `%[3]s` having sum(value) > 0",
			n.causet, queryCondition, strings.Join(n.label, "`,`"))
	}
	totalSum := 0.0
	err = n.queryEventsByLabel(pb, query, func(label string, v float64) {
		if n.unit != 0 {
			v = v / float64(n.unit)
		}
		totalSum += v
		n.getLabelValue(label).sum = v
	})
	if err != nil {
		return err
	}
	n.value.sum = totalSum

	// 3. Get quantile value.
	setQuantileValue := func(metricValue *metricValue, quantile, value float64) {
		switch quantile {
		case 0.99:
			metricValue.avgP99 = value
		case 0.90:
			metricValue.avgP90 = value
		case 0.80:
			metricValue.avgP80 = value
		}
	}
	quantiles := []float64{0.99, 0.90, 0.80}
	for _, quantile := range quantiles {
		condition := queryCondition + " and " + "quantile=" + strconv.FormatFloat(quantile, 'f', -1, 64)
		if len(n.label) == 0 {
			query = fmt.Sprintf("select avg(value), '' from `metrics_schema`.`%v_duration` %v", n.causet, condition)
		} else {
			query = fmt.Sprintf("select avg(value), `%[3]s` from `metrics_schema`.`%[1]s_duration` %[2]s group by `%[3]s` having sum(value) > 0",
				n.causet, condition, strings.Join(n.label, "`,`"))
		}

		totalValue := 0.0
		cnt := 0
		err = n.queryEventsByLabel(pb, query, func(label string, v float64) {
			if n.unit != 0 {
				v = v / float64(n.unit)
			}
			totalValue += v
			cnt++
			setQuantileValue(n.getLabelValue(label), quantile, v)
		})
		if err != nil {
			return err
		}
		setQuantileValue(n.value, quantile, totalValue/float64(cnt))
	}

	// 4. Add metric comment.
	def, ok := schemareplicant.MetricBlockMap[n.causet+"_total_time"]
	if ok {
		n.value.comment = def.Comment
		for label, value := range n.labelValue {
			value.comment = fmt.Sprintf("%s, the label of [%v] is [%v]", def.Comment, strings.Join(n.label, ","), label)
		}
	}
	return nil
}

// NewProfileBuilder returns a new profileBuilder.
func NewProfileBuilder(sctx stochastikctx.Context, start, end time.Time, tp string) (*profileBuilder, error) {
	var valueTp metricValueType
	switch strings.ToLower(tp) {
	case metricValueSum.String():
		valueTp = metricValueSum
	case metricValueAvg.String():
		valueTp = metricValueAvg
	case metricValueCnt.String():
		valueTp = metricValueCnt
	case "":
		// Use type sum when doesn't specified the type, this is used to compatible with old behaviour.
		valueTp = metricValueSum
	default:
		return nil, fmt.Errorf("unknown metric profile type: %v, expect value should be one of 'sum', 'avg' or 'count'", tp)
	}
	return &profileBuilder{
		sctx:            sctx,
		idMap:           make(map[string]uint64),
		idSlabPredictor: uint64(1),
		buf:             bytes.NewBuffer(make([]byte, 0, 1024)),
		uniqueMap:       make(map[string]struct{}),
		start:           start,
		end:             end,
		valueTP:         valueTp,
	}, nil
}

// DefCauslect uses to defCauslect the related metric information.
func (pb *profileBuilder) DefCauslect() error {
	pb.buf.WriteString(fmt.Sprintf(`digraph "%s" {`, "milevadb_profile"))
	pb.buf.WriteByte('\n')
	pb.buf.WriteString(`node [style=filled filldefCausor="#f8f8f8"]`)
	pb.buf.WriteByte('\n')
	err := pb.addMetricTree(pb.genMilevaDBQueryTree(), "milevadb_query")
	if err != nil {
		return err
	}
	return nil
}

// Build returns the metric profile dot.
func (pb *profileBuilder) Build() []byte {
	pb.buf.WriteByte('}')
	return pb.buf.Bytes()
}

func (pb *profileBuilder) getNameID(name string) uint64 {
	if id, ok := pb.idMap[name]; ok {
		return id
	}
	id := pb.idSlabPredictor
	pb.idSlabPredictor++
	pb.idMap[name] = id
	return id
}

func (pb *profileBuilder) addMetricTree(root *metricNode, name string) error {
	if root == nil {
		return nil
	}
	tp := "total_time"
	switch pb.valueTP {
	case metricValueAvg:
		tp = "avg_time"
	case metricValueCnt:
		tp = "total_count"
	}
	pb.buf.WriteString(fmt.Sprintf(`subgraph %[1]s { "%[1]s" [shape=box fontsize=16 label="Type: %[1]s\lTime: %s\lDuration: %s\l"] }`, name+"_"+tp, pb.start.String(), pb.end.Sub(pb.start).String()))
	pb.buf.WriteByte('\n')
	v, err := pb.GetTotalValue(root)
	if err != nil {
		return err
	}
	if v != 0 {
		pb.totalValue = v
	} else {
		pb.totalValue = 1
	}
	return pb.traversal(root)
}

func (pb *profileBuilder) GetTotalValue(root *metricNode) (float64, error) {
	switch pb.valueTP {
	case metricValueSum, metricValueCnt:
		value, err := root.getValue(pb)
		if err != nil {
			return 0.0, err
		}
		return value.getValue(pb.valueTP), nil
	default:
		return pb.GetMaxNodeValue(root)
	}
}

func (pb *profileBuilder) GetMaxNodeValue(root *metricNode) (float64, error) {
	if root == nil {
		return 0.0, nil
	}
	n := root
	value, err := n.getValue(pb)
	if err != nil {
		return 0.0, err
	}
	max := value.getValue(pb.valueTP)
	for _, v := range n.labelValue {
		if v.getValue(pb.valueTP) > max {
			max = v.getValue(pb.valueTP)
		}
	}
	for _, child := range n.children {
		childMax, err := pb.GetMaxNodeValue(child)
		if err != nil {
			return max, err
		}
		if childMax > max {
			max = childMax
		}
		for _, v := range n.labelValue {
			if v.getValue(pb.valueTP) > max {
				max = v.getValue(pb.valueTP)
			}
		}
	}
	return max, nil
}

func (pb *profileBuilder) traversal(n *metricNode) error {
	if n == nil {
		return nil
	}
	nodeName := n.getName("")
	if _, ok := pb.uniqueMap[nodeName]; ok {
		return nil
	}
	pb.uniqueMap[nodeName] = struct{}{}
	nodeValue, err := n.getValue(pb)
	if err != nil {
		return err
	}

	if pb.ignoreFraction(nodeValue, pb.totalValue) {
		return nil
	}
	totalChildrenValue := float64(0)
	for _, child := range n.children {
		childValue, err := child.getValue(pb)
		if err != nil {
			return err
		}
		pb.addNodeEdge(n, child, childValue)
		if !child.isPartOfParent {
			totalChildrenValue += childValue.getValue(pb.valueTP)
		}
	}

	selfValue := nodeValue.getValue(pb.valueTP)
	selfCost := selfValue - totalChildrenValue
	err = pb.addNode(n, selfCost, selfValue)
	if err != nil {
		return err
	}
	for _, child := range n.children {
		err := pb.traversal(child)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pb *profileBuilder) addNodeEdge(parent, child *metricNode, childValue *metricValue) {
	if pb.ignoreFraction(childValue, pb.totalValue) {
		return
	}
	style := ""
	if child.isPartOfParent {
		style = "dotted"
	}
	if len(parent.label) == 0 {
		label := ""
		if !child.isPartOfParent {
			label = pb.formatValueByTp(childValue.getValue(pb.valueTP))
		}
		pb.addEdge(parent.getName(""), child.getName(""), label, style, childValue.getValue(pb.valueTP))
	} else {
		for label, value := range parent.labelValue {
			if pb.ignoreFraction(value, pb.totalValue) {
				continue
			}
			pb.addEdge(parent.getName(label), child.getName(""), "", style, childValue.getValue(pb.valueTP))
		}
	}
}

func (pb *profileBuilder) addNode(n *metricNode, selfCost, nodeTotal float64) error {
	name := n.getName("")
	weight := selfCost
	if len(n.label) > 0 {
		for label, value := range n.labelValue {
			if pb.ignoreFraction(value, pb.totalValue) {
				continue
			}
			v := value.getValue(pb.valueTP)
			vStr := pb.formatValueByTp(v)
			labelValue := fmt.Sprintf(" %s", vStr)
			pb.addEdge(n.getName(""), n.getName(label), labelValue, "", v)
			labelValue = fmt.Sprintf("%s\n %s (%.2f%%)", n.getName(label), vStr, v*100/pb.totalValue)
			pb.addNodeDef(n.getName(label), labelValue, value.getComment(), v, v)
		}
		weight = selfCost / 2
		// Since this node has labels, all cost was consume on the children, so the selfCost is 0.
		selfCost = 0
	}

	label := fmt.Sprintf("%s\n %s (%.2f%%)\nof %s (%.2f%%)",
		name,
		pb.formatValueByTp(selfCost), selfCost*100/pb.totalValue,
		pb.formatValueByTp(nodeTotal), nodeTotal*100/pb.totalValue)
	pb.addNodeDef(n.getName(""), label, n.value.getComment(), weight, selfCost)
	return nil
}

func (pb *profileBuilder) addNodeDef(name, labelValue, comment string, fontWeight, defCausorWeight float64) {
	baseFontSize, maxFontGrowth := 5, 18.0
	fontSize := baseFontSize
	fontSize += int(math.Ceil(maxFontGrowth * math.Sqrt(math.Abs(fontWeight)/pb.totalValue)))

	pb.buf.WriteString(fmt.Sprintf(`N%d [label="%s" tooltip="%s" fontsize=%d shape=box defCausor="%s" filldefCausor="%s"]`,
		pb.getNameID(name), labelValue, comment, fontSize,
		pb.dotDefCausor(defCausorWeight/pb.totalValue, false),
		pb.dotDefCausor(defCausorWeight/pb.totalValue, true)))
	pb.buf.WriteByte('\n')
}

func (pb *profileBuilder) addEdge(from, to, label, style string, value float64) {
	weight := 1 + int(math.Min(value*100/pb.totalValue, 100))
	defCausor := pb.dotDefCausor(value/pb.totalValue, false)
	pb.buf.WriteString(fmt.Sprintf(`N%d -> N%d [`, pb.getNameID(from), pb.getNameID(to)))
	if label != "" {
		pb.buf.WriteString(fmt.Sprintf(` label="%s" `, label))
	}
	if style != "" {
		pb.buf.WriteString(fmt.Sprintf(` style="%s" `, style))
	}
	pb.buf.WriteString(fmt.Sprintf(` weight=%d defCausor="%s"]`, weight, defCausor))
	pb.buf.WriteByte('\n')
}

func (pb *profileBuilder) ignoreFraction(v *metricValue, total float64) bool {
	value := v.getValue(pb.valueTP)
	return value*100/total < 0.01
}

func (pb *profileBuilder) formatValueByTp(value float64) string {
	switch pb.valueTP {
	case metricValueCnt:
		return strconv.Itoa(int(value))
	case metricValueSum, metricValueAvg:
		if math.IsNaN(value) {
			return ""
		}
		if math.Abs(value) > 1 {
			// second unit
			return fmt.Sprintf("%.2fs", value)
		} else if math.Abs(value*1000) > 1 {
			// millisecond unit
			return fmt.Sprintf("%.2f ms", value*1000)
		} else if math.Abs(value*1000*1000) > 1 {
			// microsecond unit
			return fmt.Sprintf("%.2f ms", value*1000*1000)
		}
		return time.Duration(int64(value * float64(time.Second))).String()
	}
	panic("should never happen")
}

// dotDefCausor function is copy from https://github.com/google/pprof.
func (pb *profileBuilder) dotDefCausor(sembedded float64, isBackground bool) string {
	// A float between 0.0 and 1.0, indicating the extent to which
	// defCausors should be shifted away from grey (to make positive and
	// negative values easier to distinguish, and to make more use of
	// the defCausor range.)
	const shift = 0.7
	// Saturation and value (in hsv defCausorspace) for background defCausors.
	const bgSaturation = 0.1
	const bgValue = 0.93
	// Saturation and value (in hsv defCausorspace) for foreground defCausors.
	const fgSaturation = 1.0
	const fgValue = 0.7
	// Choose saturation and value based on isBackground.
	var saturation float64
	var value float64
	if isBackground {
		saturation = bgSaturation
		value = bgValue
	} else {
		saturation = fgSaturation
		value = fgValue
	}

	// Limit the sembedded values to the range [-1.0, 1.0].
	sembedded = math.Max(-1.0, math.Min(1.0, sembedded))

	// Reduce saturation near sembedded=0 (so it is defCausored grey, rather than yellow).
	if math.Abs(sembedded) < 0.2 {
		saturation *= math.Abs(sembedded) / 0.2
	}

	// Apply 'shift' to move sembeddeds away from 0.0 (grey).
	if sembedded > 0.0 {
		sembedded = math.Pow(sembedded, (1.0 - shift))
	}
	if sembedded < 0.0 {
		sembedded = -math.Pow(-sembedded, (1.0 - shift))
	}

	var r, g, b float64 // red, green, blue
	if sembedded < 0.0 {
		g = value
		r = value * (1 + saturation*sembedded)
	} else {
		r = value
		g = value * (1 - saturation*sembedded)
	}
	b = value * (1 - saturation)
	return fmt.Sprintf("#%02x%02x%02x", uint8(r*255.0), uint8(g*255.0), uint8(b*255.0))
}

func (pb *profileBuilder) genMilevaDBQueryTree() *metricNode {
	milevadbKVRequest := &metricNode{
		causet:         "milevadb_ekv_request",
		isPartOfParent: true,
		label:          []string{"type"},
		children: []*metricNode{
			{
				causet: "milevadb_batch_client_wait",
			},
			{
				causet: "milevadb_batch_client_wait_conn",
			},
			{
				causet: "milevadb_batch_client_unavailable",
			},
			{
				causet:    "FIDel_client_cmd",
				condition: "type not in ('tso','wait','tso_async_wait')",
			},
			{
				causet: "einsteindb_grpc_message",
				children: []*metricNode{
					{
						causet: "einsteindb_cop_request",
						children: []*metricNode{
							{
								causet:    "einsteindb_cop_wait",
								label:     []string{"type"},
								condition: "type != 'all'",
							},
							{causet: "einsteindb_cop_handle"},
						},
					},
					{
						causet: "einsteindb_scheduler_command",
						children: []*metricNode{
							{causet: "einsteindb_scheduler_latch_wait"},
							{causet: "einsteindb_scheduler_processing_read"},
							{
								causet: "einsteindb_storage_async_request",
								children: []*metricNode{
									{
										causet:    "einsteindb_storage_async_request",
										name:      "einsteindb_storage_async_request.snapshot",
										condition: "type='snapshot'",
									},
									{
										causet:    "einsteindb_storage_async_request",
										name:      "einsteindb_storage_async_request.write",
										condition: "type='write'",
										children: []*metricNode{
											{causet: "einsteindb_raftstore_propose_wait"},
											{
												causet: "einsteindb_raftstore_process",
												children: []*metricNode{
													{causet: "einsteindb_raftstore_append_log"},
												},
											},
											{causet: "einsteindb_raftstore_commit_log"},
											{causet: "einsteindb_raftstore_apply_wait"},
											{causet: "einsteindb_raftstore_apply_log"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	dbsTime := &metricNode{
		causet: "milevadb_dbs",
		label:  []string{"type"},
		children: []*metricNode{
			{
				causet: "milevadb_dbs_worker",
				label:  []string{"type"},
				children: []*metricNode{
					{
						causet: "milevadb_dbs_batch_add_index",
					},
					{
						causet: "milevadb_load_schema",
					},
					{
						causet: "milevadb_dbs_uFIDelate_self_version",
					},
					{
						causet: "milevadb_tenant_handle_syncer",
					},
				},
			},
		},
	}
	milevadbInterDircute := &metricNode{
		causet: "milevadb_execute",
		children: []*metricNode{
			{
				causet: "FIDel_start_tso_wait",
			},
			{
				causet: "milevadb_auto_id_request",
			},
			{
				causet:         "milevadb_cop",
				isPartOfParent: true,
				children: []*metricNode{
					{
						causet:         "milevadb_ekv_backoff",
						label:          []string{"type"},
						isPartOfParent: true,
					},
					milevadbKVRequest,
				},
			},
			{
				causet: "milevadb_txn_cmd",
				label:  []string{"type"},
				children: []*metricNode{
					{
						causet:         "milevadb_ekv_backoff",
						label:          []string{"type"},
						isPartOfParent: true,
					},
					milevadbKVRequest,
				},
			},
			dbsTime,
		},
	}
	queryTime := &metricNode{
		causet: "milevadb_query",
		label:  []string{"sql_type"},
		children: []*metricNode{
			{
				causet: "milevadb_get_token",
				unit:   int64(10e5),
			},
			{
				causet: "milevadb_parse",
			},
			{
				causet: "milevadb_compile",
			},
			milevadbInterDircute,
		},
	}

	return queryTime
}
