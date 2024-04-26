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
	"sync"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	defaultBufferSize = 1024
)

var defaultEventBus = NewEventBus(defaultBufferSize)

func GetDefaultEventBus() EventBus {
	return defaultEventBus
}

type ConsumeFunc func(interface{}) error

// EventBus TODO: support event type check, unsubscribe, stop, statistics
type EventBus interface {
	Publish(topic string, event interface{}) error
	Subscribe(topic string, subscriber string, bufferSize int, handler ConsumeFunc) error
}

type eventHandler struct {
	name    string
	buffer  chan interface{}
	handler ConsumeFunc
}

func (e *eventHandler) Run() {
	for {
		select {
		case msg := <-e.buffer:
			if err := e.handler(msg); err != nil {
				general.Errorf("subscriber %v handling event err:%v", e.name, err)
			}
		}
	}
}

type topicContext struct {
	mutex         sync.RWMutex
	topic         string
	buffer        chan interface{}
	eventHandlers map[string]*eventHandler
}

func (t *topicContext) dispatch(event interface{}) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	for subscriber, handler := range t.eventHandlers {
		// non-blocking send
		select {
		case handler.buffer <- event:
		default:
			general.Warningf("topic %v subscriber %v buffer full, dropping event: %v", t.topic, subscriber, event)
		}
	}
}

func (t *topicContext) Run() {
	for {
		select {
		case event := <-t.buffer:
			t.dispatch(event)
		}
	}
}

func (t *topicContext) RegisterHandler(subscriber string, bufferSize int, handler ConsumeFunc) error {
	if t == nil {
		return fmt.Errorf("cannot register handler for a nil topic")
	}

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if _, exists := t.eventHandlers[subscriber]; exists {
		general.Warningf("subscriber: %v already subscribed topic %v", subscriber, t.topic)
	} else {
		e := &eventHandler{name: subscriber, handler: handler, buffer: make(chan interface{}, bufferSize)}
		go e.Run()
		t.eventHandlers[subscriber] = e
		general.Infof("register subscriber: %v for topic: %v", subscriber, t.topic)
	}

	return nil
}

type eventBus struct {
	mutex      sync.RWMutex
	bufferSize int
	topics     map[string]*topicContext
}

func NewEventBus(bufferSize int) EventBus {
	return &eventBus{
		topics:     make(map[string]*topicContext),
		bufferSize: bufferSize,
	}
}

func (e *eventBus) GetOrRegisterTopic(topic string) *topicContext {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if ctx, exists := e.topics[topic]; exists {
		return ctx
	} else {
		e.topics[topic] = &topicContext{
			mutex:         sync.RWMutex{},
			topic:         topic,
			buffer:        make(chan interface{}, e.bufferSize),
			eventHandlers: make(map[string]*eventHandler),
		}
		go e.topics[topic].Run()
		general.Infof("register new topic: %v", topic)
		return e.topics[topic]
	}
}

func (e *eventBus) Publish(topic string, event interface{}) error {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if ctx, ok := e.topics[topic]; ok {
		// non-blocking send
		select {
		case ctx.buffer <- event:
			return nil
		default:
			general.Warningf("topic %v buffer full, dropping event: %v", topic, event)
			return fmt.Errorf("buffer full")
		}
	} else {
		general.Warningf("no subscriber registered for topic %s", topic)
	}
	return nil
}

func (e *eventBus) Subscribe(topic string, subscriber string, bufferSize int, handler ConsumeFunc) error {
	return e.GetOrRegisterTopic(topic).RegisterHandler(subscriber, bufferSize, handler)
}
