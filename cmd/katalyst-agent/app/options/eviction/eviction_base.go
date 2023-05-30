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

package eviction

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/errors"
	cliflag "k8s.io/component-base/cli/flag"

	evictionconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/eviction"
)

// GenericEvictionOptions holds the configurations for eviction manager.
type GenericEvictionOptions struct {
	InnerPlugins []string

	DryRunPlugins []string

	// ConditionTransitionPeriod is duration the eviction manager has to wait before transitioning out of a condition.
	ConditionTransitionPeriod time.Duration

	// EvictionManagerSyncPeriod is the interval duration that eviction manager fetches information from registered plugins
	EvictionManagerSyncPeriod time.Duration

	// those two variables are used to filter out eviction-free pods
	EvictionSkippedAnnotationKeys []string
	EvictionSkippedLabelKeys      []string

	// EvictionBurst limit the burst eviction counts
	EvictionBurst int
}

// NewGenericEvictionOptions creates a new Options with a default config.
func NewGenericEvictionOptions() *GenericEvictionOptions {
	return &GenericEvictionOptions{
		InnerPlugins:                  []string{},
		DryRunPlugins:                 []string{},
		ConditionTransitionPeriod:     5 * time.Minute,
		EvictionManagerSyncPeriod:     5 * time.Second,
		EvictionSkippedAnnotationKeys: []string{},
		EvictionSkippedLabelKeys:      []string{},
		EvictionBurst:                 3,
	}
}

// AddFlags adds flags  to the specified FlagSet.
func (o *GenericEvictionOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	fs := fss.FlagSet("eviction")

	fs.StringSliceVar(&o.InnerPlugins, "eviction-plugins", o.InnerPlugins, fmt.Sprintf(""+
		"A list of eviction plugins to enable. '*' enables all on-by-default eviction plugins, 'foo' enables the eviction plugin "+
		"named 'foo', '-foo' disables the eviction plugin named 'foo'"))

	fs.StringSliceVar(&o.DryRunPlugins, "dry-run-plugins", o.DryRunPlugins, fmt.Sprintf(" A list of "+
		"eviction plugins to dry run. If a plugin in this list, it will enter dry run mode"))

	fs.DurationVar(&o.ConditionTransitionPeriod, "eviction-condition-transition-period", o.ConditionTransitionPeriod,
		"duration the eviction manager has to wait before transitioning out of a condition")

	fs.DurationVar(&o.EvictionManagerSyncPeriod, "eviction-manager-sync-period", o.EvictionManagerSyncPeriod,
		"interval duration that eviction manager fetches information from registered plugins")

	fs.StringSliceVar(&o.EvictionSkippedAnnotationKeys, "eviction-skipped-annotation", o.EvictionSkippedAnnotationKeys,
		"A list of annotations to identify a bunch of pods that should be filtered out during eviction")
	fs.StringSliceVar(&o.EvictionSkippedLabelKeys, "eviction-skipped-labels", o.EvictionSkippedLabelKeys,
		"A list of labels to identify a bunch of pods that should be filtered out during eviction")

	fs.IntVar(&o.EvictionBurst, "eviction-burst", o.EvictionBurst,
		"The burst amount of pods to be evicted by edition manager")
}

// ApplyTo fills up config with options
func (o *GenericEvictionOptions) ApplyTo(c *evictionconfig.GenericEvictionConfiguration) error {
	c.InnerPlugins = o.InnerPlugins
	c.DryrunPlugins = o.DryRunPlugins
	c.ConditionTransitionPeriod = o.ConditionTransitionPeriod
	c.EvictionManagerSyncPeriod = o.EvictionManagerSyncPeriod
	c.EvictionSkippedAnnotationKeys.Insert(o.EvictionSkippedAnnotationKeys...)
	c.EvictionSkippedLabelKeys.Insert(o.EvictionSkippedLabelKeys...)
	c.EvictionBurst = o.EvictionBurst
	return nil
}

func (o *GenericEvictionOptions) Config() (*evictionconfig.GenericEvictionConfiguration, error) {
	c := evictionconfig.NewGenericEvictionConfiguration()
	if err := o.ApplyTo(c); err != nil {
		return nil, err
	}
	return c, nil
}

type EvictionPluginsOptions struct {
	*ReclaimedResourcesEvictionPluginOptions
	*MemoryPressureEvictionPluginOptions
	*CPUPressureEvictionPluginOptions
}

func NewEvictionPluginsOptions() *EvictionPluginsOptions {
	return &EvictionPluginsOptions{
		ReclaimedResourcesEvictionPluginOptions: NewReclaimedResourcesEvictionPluginOptions(),
		MemoryPressureEvictionPluginOptions:     NewMemoryPressureEvictionPluginOptions(),
		CPUPressureEvictionPluginOptions:        NewCPUPressureEvictionPluginOptions(),
	}
}

func (o *EvictionPluginsOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	o.ReclaimedResourcesEvictionPluginOptions.AddFlags(fss)
	o.MemoryPressureEvictionPluginOptions.AddFlags(fss)
	o.CPUPressureEvictionPluginOptions.AddFlags(fss)
}

// ApplyTo fills up config with options
func (o *EvictionPluginsOptions) ApplyTo(c *evictionconfig.EvictionPluginsConfiguration) error {
	var errList []error
	errList = append(errList,
		o.ReclaimedResourcesEvictionPluginOptions.ApplyTo(c.ReclaimedResourcesEvictionPluginConfiguration),
		o.MemoryPressureEvictionPluginOptions.ApplyTo(c.MemoryPressureEvictionPluginConfiguration),
		o.CPUPressureEvictionPluginOptions.ApplyTo(c.CPUPressureEvictionPluginConfiguration),
	)
	return errors.NewAggregate(errList)
}

func (o *EvictionPluginsOptions) Config() (*evictionconfig.EvictionPluginsConfiguration, error) {
	c := evictionconfig.NewEvictionPluginsConfiguration()
	if err := o.ApplyTo(c); err != nil {
		return nil, err
	}
	return c, nil
}
