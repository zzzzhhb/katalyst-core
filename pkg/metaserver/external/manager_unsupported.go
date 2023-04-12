//go:build !linux
// +build !linux

// Copyright 2022 The Katalyst Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"

	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/external/cgroupid"
	"github.com/kubewharf/katalyst-core/pkg/util/external/network"
	"github.com/kubewharf/katalyst-core/pkg/util/external/rdt"
)

type unsupportedExternalManager struct {
	cgroupid.CgroupIDManager

	network.NetworkManager
	rdt.RDTManager
}

// Run starts an unsupportedExternalManager
func (m *unsupportedExternalManager) Run(_ context.Context) {

}

// InitExternalManager initializes an externalManagerImpl
func InitExternalManager(podFetcher pod.PodFetcher) ExternalManager {
	return &unsupportedExternalManager{}
}
