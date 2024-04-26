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

package manager

import (
	"time"

	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/eventbus"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

func NewInstrumentedManager(manager Manager) *InstrumentedManager {
	return &InstrumentedManager{wrappedManager: manager}
}

type InstrumentedManager struct {
	wrappedManager Manager
}

func (i *InstrumentedManager) buildUnifiedDataEvent(startTime time.Time, absCgroupPath string, cgroupFileName string, data interface{}, eventType common.CGroupEventType) *common.CGroupEvent {
	return &common.CGroupEvent{
		BaseEventImpl: eventbus.BaseEventImpl{Time: startTime},
		Cost:          time.Now().Sub(startTime),
		Type:          eventType,
		CGroupPath:    absCgroupPath,
		CGroupFile:    cgroupFileName,
		Data:          data,
	}
}

func (i *InstrumentedManager) buildIORelatedEvent(startTime time.Time, absCgroupPath string, devID string, data interface{}, eventType common.CGroupEventType) *common.CGroupEvent {
	return &common.CGroupEvent{
		BaseEventImpl: eventbus.BaseEventImpl{Time: startTime},
		Cost:          time.Now().Sub(startTime),
		Type:          eventType,
		CGroupPath:    absCgroupPath,
		DevID:         devID,
		Data:          data,
	}
}

func (i *InstrumentedManager) buildEvent(startTime time.Time, absCgroupPath string, data interface{}, eventType common.CGroupEventType) *common.CGroupEvent {
	return &common.CGroupEvent{
		BaseEventImpl: eventbus.BaseEventImpl{Time: startTime},
		Type:          eventType,
		Cost:          time.Now().Sub(startTime),
		CGroupPath:    absCgroupPath,
		Data:          data,
	}
}

func (i *InstrumentedManager) sendApplyCGroupEvent(event *common.CGroupEvent) {
	if err := eventbus.GetDefaultEventBus().Publish(consts.TopicNameApplyCGroup, *event); err != nil {
		general.Warningf("publishing CGroupEvent failed: %v", err)
	}
}

func (i *InstrumentedManager) ApplyMemory(absCgroupPath string, data *common.MemoryData) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildEvent(startTime, absCgroupPath, *data, common.CGroupEventTypeMemory))

	return i.wrappedManager.ApplyMemory(absCgroupPath, data)
}

func (i *InstrumentedManager) ApplyCPU(absCgroupPath string, data *common.CPUData) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildEvent(startTime, absCgroupPath, *data, common.CGroupEventTypeCPU))

	return i.wrappedManager.ApplyCPU(absCgroupPath, data)
}

func (i *InstrumentedManager) ApplyCPUSet(absCgroupPath string, data *common.CPUSetData) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildEvent(startTime, absCgroupPath, *data, common.CGroupEventTypeCPUSet))

	return i.wrappedManager.ApplyCPUSet(absCgroupPath, data)
}

func (i *InstrumentedManager) ApplyNetCls(absCgroupPath string, data *common.NetClsData) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildEvent(startTime, absCgroupPath, *data, common.CGroupEventTypeNetCls))

	return i.wrappedManager.ApplyNetCls(absCgroupPath, data)
}

func (i *InstrumentedManager) ApplyIOCostQoS(absCgroupPath string, devID string, data *common.IOCostQoSData) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildIORelatedEvent(startTime, absCgroupPath, devID, *data, common.CGroupEventTypeIOCostQos))

	return i.wrappedManager.ApplyIOCostQoS(absCgroupPath, devID, data)
}

func (i *InstrumentedManager) ApplyIOCostModel(absCgroupPath string, devID string, data *common.IOCostModelData) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildIORelatedEvent(startTime, absCgroupPath, devID, *data, common.CGroupEventTypeIOCostModel))

	return i.wrappedManager.ApplyIOCostModel(absCgroupPath, devID, data)
}

func (i *InstrumentedManager) ApplyIOWeight(absCgroupPath string, devID string, weight uint64) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildIORelatedEvent(startTime, absCgroupPath, devID, weight, common.CGroupEventTypeIOWeight))

	return i.wrappedManager.ApplyIOWeight(absCgroupPath, devID, weight)
}

func (i *InstrumentedManager) ApplyUnifiedData(absCgroupPath, cgroupFileName, data string) error {
	startTime := time.Now()
	defer i.sendApplyCGroupEvent(i.buildIORelatedEvent(startTime, absCgroupPath, cgroupFileName, data, common.CGroupEventTypeUnifiedData))

	return i.wrappedManager.ApplyUnifiedData(absCgroupPath, cgroupFileName, data)
}

func (i *InstrumentedManager) GetMemory(absCgroupPath string) (*common.MemoryStats, error) {
	return i.wrappedManager.GetMemory(absCgroupPath)
}

func (i *InstrumentedManager) GetNumaMemory(absCgroupPath string) (map[int]*common.MemoryNumaMetrics, error) {
	return i.wrappedManager.GetNumaMemory(absCgroupPath)
}

func (i *InstrumentedManager) GetCPU(absCgroupPath string) (*common.CPUStats, error) {
	return i.wrappedManager.GetCPU(absCgroupPath)
}

func (i *InstrumentedManager) GetCPUSet(absCgroupPath string) (*common.CPUSetStats, error) {
	return i.wrappedManager.GetCPUSet(absCgroupPath)
}

func (i *InstrumentedManager) GetIOCostQoS(absCgroupPath string) (map[string]*common.IOCostQoSData, error) {
	return i.wrappedManager.GetIOCostQoS(absCgroupPath)
}

func (i *InstrumentedManager) GetIOCostModel(absCgroupPath string) (map[string]*common.IOCostModelData, error) {
	return i.wrappedManager.GetIOCostModel(absCgroupPath)
}

func (i *InstrumentedManager) GetDeviceIOWeight(absCgroupPath string, devID string) (uint64, bool, error) {
	return i.wrappedManager.GetDeviceIOWeight(absCgroupPath, devID)
}

func (i *InstrumentedManager) GetIOStat(absCgroupPath string) (map[string]map[string]string, error) {
	return i.wrappedManager.GetIOStat(absCgroupPath)
}

func (i *InstrumentedManager) GetMetrics(relCgroupPath string, subsystems map[string]struct{}) (*common.CgroupMetrics, error) {
	return i.wrappedManager.GetMetrics(relCgroupPath, subsystems)
}

func (i *InstrumentedManager) GetPids(absCgroupPath string) ([]string, error) {
	return i.wrappedManager.GetPids(absCgroupPath)
}

func (i *InstrumentedManager) GetTasks(absCgroupPath string) ([]string, error) {
	return i.wrappedManager.GetTasks(absCgroupPath)
}
