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

package eventbus

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

type countSubscriber struct {
	name    string
	count   int
	msgKind reflect.Type
}

func (c *countSubscriber) OnMessage(message interface{}) error {
	if reflect.TypeOf(message).String() != c.msgKind.String() {
		return fmt.Errorf("expected message type %s, got %s", c.msgKind.String(), reflect.TypeOf(message).String())
	}

	c.count++
	general.Infof("subscriber %v received message:%v, message count:%v", c.name, message, c.count)
	return nil
}

type msg1 struct{}

type msg2 struct{}

func TestEventBus(t *testing.T) {
	t.Parallel()

	bufferSize := 20
	bus := NewEventBus(bufferSize)

	s1 := &countSubscriber{name: "s1", msgKind: reflect.TypeOf(msg1{})}
	s2 := &countSubscriber{name: "s2", msgKind: reflect.TypeOf(msg2{})}
	s3 := &countSubscriber{name: "s3", msgKind: reflect.TypeOf(msg1{})}

	topic1 := "topic1"
	topic2 := "topic2"
	topic3 := "topic3"

	err := bus.Subscribe(topic1, s1.name, bufferSize, s1.OnMessage)
	assert.NoError(t, err)
	err = bus.Subscribe(topic2, s2.name, bufferSize, s2.OnMessage)
	assert.NoError(t, err)
	err = bus.Subscribe(topic1, s3.name, bufferSize, s3.OnMessage)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		for i := 0; i < 400; i++ {
			_ = bus.Publish(topic1, msg1{})
			general.Infof("publish topic1")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 600; i++ {
			_ = bus.Publish(topic2, msg2{})
			general.Infof("publish topic2")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()

	assert.Equal(t, 400, s1.count)
	assert.Equal(t, 600, s2.count)
	assert.Equal(t, 400, s3.count)

	err = bus.Publish(topic3, msg1{})
	assert.NoError(t, err)
}
