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

package memcam

// SlabPredictor pre-allocates memory to reduce memory allocation cost.
// It is not thread-safe.
type SlabPredictor interface {
	// Alloc allocates memory with 0 len and capacity cap.
	Alloc(capacity int) []byte

	// AllocWithLen allocates memory with length and capacity.
	AllocWithLen(length int, capacity int) []byte

	// Reset resets memcam offset.
	// Make sure all the allocated memory are not used any more.
	Reset()
}

// SimpleSlabPredictor is a simple implementation of MemCamSlabPredictor.
type SimpleSlabPredictor struct {
	memcam []byte
	off   int
}

type stdSlabPredictor struct {
}

func (a *stdSlabPredictor) Alloc(capacity int) []byte {
	return make([]byte, 0, capacity)
}

func (a *stdSlabPredictor) AllocWithLen(length int, capacity int) []byte {
	return make([]byte, length, capacity)
}

func (a *stdSlabPredictor) Reset() {
}

var _ SlabPredictor = &stdSlabPredictor{}

// StdSlabPredictor implements SlabPredictor but do not pre-allocate memory.
var StdSlabPredictor = &stdSlabPredictor{}

// NewSlabPredictor creates an SlabPredictor with a specified capacity.
func NewSlabPredictor(capacity int) *SimpleSlabPredictor {
	return &SimpleSlabPredictor{memcam: make([]byte, 0, capacity)}
}

// Alloc implements SlabPredictor.AllocBytes interface.
func (s *SimpleSlabPredictor) Alloc(capacity int) []byte {
	if s.off+capacity < cap(s.memcam) {
		slice := s.memcam[s.off : s.off : s.off+capacity]
		s.off += capacity
		return slice
	}

	return make([]byte, 0, capacity)
}

// AllocWithLen implements SlabPredictor.AllocWithLen interface.
func (s *SimpleSlabPredictor) AllocWithLen(length int, capacity int) []byte {
	slice := s.Alloc(capacity)
	return slice[:length:capacity]
}

// Reset implements SlabPredictor.Reset interface.
func (s *SimpleSlabPredictor) Reset() {
	s.off = 0
}
