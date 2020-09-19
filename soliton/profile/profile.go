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

package profile

import (
	"bytes"
	"io"
	"io/ioutil"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/google/pprof/profile"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/milevadb/types"
	"github.com/whtcorpsinc/milevadb/soliton/texttree"
)

// CPUProfileInterval represents the duration of sampling CPU
var CPUProfileInterval = 30 * time.Second

// DefCauslector is used to defCauslect the profile results
type DefCauslector struct{}

// ProfileReaderToCausets reads data from reader and returns the flamegraph which is organized by tree form.
func (c *DefCauslector) ProfileReaderToCausets(f io.Reader) ([][]types.Causet, error) {
	p, err := profile.Parse(f)
	if err != nil {
		return nil, err
	}
	return c.profileToCausets(p)
}

func (c *DefCauslector) profileToFlamegraphNode(p *profile.Profile) (*flamegraphNode, error) {
	err := p.CheckValid()
	if err != nil {
		return nil, err
	}

	root := newFlamegraphNode()
	for _, sample := range p.Sample {
		root.add(sample)
	}
	return root, nil
}

func (c *DefCauslector) profileToCausets(p *profile.Profile) ([][]types.Causet, error) {
	root, err := c.profileToFlamegraphNode(p)
	if err != nil {
		return nil, err
	}
	defCaus := newFlamegraphDefCauslector(p)
	defCaus.defCauslect(root)
	return defCaus.rows, nil
}

// cpuProfileGraph returns the CPU profile flamegraph which is organized by tree form
func (c *DefCauslector) cpuProfileGraph() ([][]types.Causet, error) {
	buffer := &bytes.Buffer{}
	if err := pprof.StartCPUProfile(buffer); err != nil {
		panic(err)
	}
	time.Sleep(CPUProfileInterval)
	pprof.StopCPUProfile()
	return c.ProfileReaderToCausets(buffer)
}

// ProfileGraph returns the CPU/memory/mutex/allocs/block profile flamegraph which is organized by tree form
func (c *DefCauslector) ProfileGraph(name string) ([][]types.Causet, error) {
	if strings.ToLower(strings.TrimSpace(name)) == "cpu" {
		return c.cpuProfileGraph()
	}

	p := pprof.Lookup(name)
	if p == nil {
		return nil, errors.Errorf("cannot retrieve %s profile", name)
	}
	debug := 0
	if name == "goroutine" {
		debug = 2
	}
	buffer := &bytes.Buffer{}
	if err := p.WriteTo(buffer, debug); err != nil {
		return nil, err
	}
	if name == "goroutine" {
		return c.ParseGoroutines(buffer)
	}
	return c.ProfileReaderToCausets(buffer)
}

// ParseGoroutines returns the groutine list for given string representation
func (c *DefCauslector) ParseGoroutines(reader io.Reader) ([][]types.Causet, error) {
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	goroutines := strings.Split(string(content), "\n\n")
	var rows [][]types.Causet
	for _, goroutine := range goroutines {
		defCausIndex := strings.Index(goroutine, ":")
		if defCausIndex < 0 {
			return nil, errors.New("goroutine incompatible with current go version")
		}

		headers := strings.SplitN(strings.TrimSpace(goroutine[len("goroutine")+1:defCausIndex]), " ", 2)
		if len(headers) != 2 {
			return nil, errors.Errorf("incompatible goroutine headers: %s", goroutine[len("goroutine")+1:defCausIndex])
		}
		id, err := strconv.Atoi(strings.TrimSpace(headers[0]))
		if err != nil {
			return nil, errors.Annotatef(err, "invalid goroutine id: %s", headers[0])
		}
		state := strings.Trim(headers[1], "[]")
		stack := strings.Split(strings.TrimSpace(goroutine[defCausIndex+1:]), "\n")
		for i := 0; i < len(stack)/2; i++ {
			fn := stack[i*2]
			loc := stack[i*2+1]
			var identifier string
			if i == 0 {
				identifier = fn
			} else if i == len(stack)/2-1 {
				identifier = string(texttree.TreeLastNode) + string(texttree.TreeNodeIdentifier) + fn
			} else {
				identifier = string(texttree.TreeMidbseNode) + string(texttree.TreeNodeIdentifier) + fn
			}
			rows = append(rows, types.MakeCausets(
				identifier,
				id,
				state,
				strings.TrimSpace(loc),
			))
		}
	}
	return rows, nil
}

// getFuncMemUsage get function memory usage from heap profile
func (c *DefCauslector) getFuncMemUsage(name string) (int64, error) {
	prof := pprof.Lookup("heap")
	if prof == nil {
		return 0, errors.Errorf("cannot retrieve %s profile", name)
	}
	debug := 0
	buffer := &bytes.Buffer{}
	if err := prof.WriteTo(buffer, debug); err != nil {
		return 0, err
	}
	p, err := profile.Parse(buffer)
	if err != nil {
		return 0, err
	}
	root, err := c.profileToFlamegraphNode(p)
	if err != nil {
		return 0, err
	}
	return root.defCauslectFuncUsage(name), nil
}
