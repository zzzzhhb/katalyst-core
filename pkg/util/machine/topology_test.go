/*
Copyright 2022 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package machine

import (
	"reflect"
	"testing"
)

func TestMemoryDetailsEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		detail MemoryDetails
		want   MemoryDetails
		equal  bool
	}{
		{
			name:   "Equal Maps",
			detail: MemoryDetails{1: 100, 2: 200},
			want:   MemoryDetails{1: 100, 2: 200},
			equal:  true,
		},
		{
			name:   "Different Lengths",
			detail: MemoryDetails{1: 100},
			want:   MemoryDetails{1: 100, 2: 200},
			equal:  false,
		},
		{
			name:   "Different Values",
			detail: MemoryDetails{1: 100, 2: 200},
			want:   MemoryDetails{1: 100, 2: 300},
			equal:  false,
		},
		{
			name:   "Different Keys",
			detail: MemoryDetails{1: 100, 3: 300},
			want:   MemoryDetails{1: 100, 2: 300},
			equal:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.detail.Equal(tt.want); got != tt.equal {
				t.Errorf("MemoryDetails.Equal() = %v, want %v", got, tt.equal)
			}
		})
	}
}

func TestMemoryDetailsClone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		detail MemoryDetails
	}{
		{
			name:   "Empty Map",
			detail: MemoryDetails{},
		},
		{
			name:   "Single Element",
			detail: MemoryDetails{1: 100},
		},
		{
			name:   "Multiple Elements",
			detail: MemoryDetails{1: 100, 2: 200, 3: 300},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cloned := tt.detail.Clone()

			if !reflect.DeepEqual(cloned, tt.detail) {
				t.Errorf("Clone() = %v, want %v", cloned, tt.detail)
			}

			// Ensure that the clone is a different instance
			if &cloned == &tt.detail {
				t.Errorf("Clone() returned the same instance, want different instances")
			}
		})
	}
}

func TestMemoryDetailsFillNUMANodesWithZero(t *testing.T) {
	t.Parallel()

	// Define test cases for FillNUMANodesWithZero method
	tests := []struct {
		name     string
		detail   MemoryDetails
		allNUMAs CPUSet
		expected MemoryDetails
	}{
		{
			name:   "Existing NUMA Nodes",
			detail: MemoryDetails{0: 100, 1: 200},
			allNUMAs: CPUSet{
				Initialed: true,
				elems:     map[int]struct{}{0: {}, 1: {}, 2: {}},
			},
			expected: MemoryDetails{0: 100, 1: 200, 2: 0},
		},
		{
			name:   "Empty NUMA Nodes",
			detail: MemoryDetails{},
			allNUMAs: CPUSet{
				Initialed: true,
				elems:     map[int]struct{}{0: {}},
			},
			expected: MemoryDetails{0: 0},
		},
		{
			name:   "No Additional NUMA Nodes",
			detail: MemoryDetails{0: 100, 1: 200},
			allNUMAs: CPUSet{
				Initialed: true,
				elems:     map[int]struct{}{0: {}, 1: {}},
			},
			expected: MemoryDetails{0: 100, 1: 200},
		},
	}

	// Run the test cases
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Call the FillNUMANodesWithZero method and compare the result with expected outcome
			if got := tt.detail.FillNUMANodesWithZero(tt.allNUMAs); !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("MemoryDetails.FillNUMANodesWithZero() = %v, want %v", got, tt.expected)
			}
		})
	}
}
