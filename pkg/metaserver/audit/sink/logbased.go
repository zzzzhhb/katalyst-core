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

package sink

import (
	"reflect"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/global"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	SinkNameLogBased = "log"
)

type LogBasedAuditSink struct {
	BaseAuditSink
	BufferSize int
}

func NewLogBasedAuditSink(conf *global.AuditConfiguration, _ metrics.MetricEmitter) Interface {
	return &LogBasedAuditSink{BufferSize: conf.BufferSize}
}

func (f *LogBasedAuditSink) ProcessEvent(event interface{}) error {
	if event == nil {
		general.Warningf("ignore nil event")
		return nil
	}

	switch e := event.(type) {
	case common.CGroupEvent:
		general.Infof("[audit log] cgroup event: %+v", e)
	default:
		general.Warningf("unsupported event type:%v", reflect.TypeOf(event))
	}

	return nil
}

func (f *LogBasedAuditSink) GetName() string {
	return SinkNameLogBased
}

func (f *LogBasedAuditSink) GetBufferSize() int {
	return f.BufferSize
}
