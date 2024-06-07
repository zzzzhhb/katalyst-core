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
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	defaultBufferSize     = 1024
	defaultReportInterval = 30 * time.Second

	ErrTypeNoSubscriber = "NoSubscriber"
	ErrTypeBufferFull   = "BufferFull"

	MetricNameEventBusErr = "event_bus_err"
)

var defaultEventBus = NewEventBus(defaultBufferSize)

func GetDefaultEventBus() EventBus {
	defaultEventBus.EnableStatistic()
	return defaultEventBus
}

type ConsumeFunc func(interface{}) error

// EventBus TODO: support event type check, unsubscribe, stop, statistics
type EventBus interface {
	Publish(topic string, event interface{}) error
	Subscribe(topic string, subscriber string, bufferSize int, handler ConsumeFunc) error
	EnableStatistic()
	SetEmitter(emitter metrics.MetricEmitter)
}

type eventHandler struct {
	name    string
	buffer  chan interface{}
	handler ConsumeFunc
	emitter metrics.MetricEmitter
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
	errCounter    *sync.Map
}

func addCounter(counterMap *sync.Map, errType string, value uint64) {
	if counterMap == nil {
		return
	}

	counter, _ := counterMap.LoadOrStore(errType, new(uint64))
	atomic.AddUint64(counter.(*uint64), value)
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
	mutex         sync.RWMutex
	bufferSize    int
	topicSet      sets.String
	topics        map[string]*topicContext
	errorCounter  *sync.Map
	statisticOnce sync.Once
	emitter       metrics.MetricEmitter
	emitterMutex  sync.RWMutex
}

func NewEventBus(bufferSize int) EventBus {
	return &eventBus{
		topicSet:      sets.NewString(),
		topics:        make(map[string]*topicContext),
		bufferSize:    bufferSize,
		errorCounter:  &sync.Map{},
		statisticOnce: sync.Once{},
	}
}

func (e *eventBus) EnableStatistic() {
	e.statisticOnce.Do(func() {
		go wait.Forever(e.reportStatistic, defaultReportInterval)
	})
}

func (e *eventBus) SetEmitter(emitter metrics.MetricEmitter) {
	e.emitterMutex.Lock()
	defer e.emitterMutex.Unlock()

	e.emitter = emitter
}

func (e *eventBus) getEmitter() metrics.MetricEmitter {
	e.emitterMutex.RLock()
	defer e.emitterMutex.RUnlock()

	return e.emitter
}

func (e *eventBus) reportStatistic() {
	emitter := e.getEmitter()
	e.errorCounter.Range(func(key, value interface{}) bool {
		errType := key.(string)
		counter := value.(*uint64)
		general.Infof("eventbus error counter: %v, %v", errType, atomic.LoadUint64(counter))
		if emitter != nil {
			_ = emitter.StoreInt64(MetricNameEventBusErr, int64(atomic.LoadUint64(counter)), metrics.MetricTypeNameCount,
				metrics.MetricTag{Key: "errType", Val: errType},
			)
		}
		atomic.StoreUint64(counter, 0)
		return true
	})

	for topic := range e.topicSet {
		t := e.getTopicContext(topic)
		t.errCounter.Range(func(key, value interface{}) bool {
			errType := key.(string)
			counter := value.(*uint64)
			general.Infof("eventbus topic %v error counter: %v, %v", topic, errType, atomic.LoadUint64(counter))
			if emitter != nil {
				_ = emitter.StoreInt64(MetricNameEventBusErr, int64(atomic.LoadUint64(counter)), metrics.MetricTypeNameCount,
					metrics.MetricTag{Key: "errType", Val: errType},
					metrics.MetricTag{Key: "topic", Val: topic},
				)
			}
			atomic.StoreUint64(counter, 0)
			return true
		})
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
			errCounter:    &sync.Map{},
		}
		e.topicSet.Insert(topic)
		go e.topics[topic].Run()
		general.Infof("register new topic: %v", topic)
		return e.topics[topic]
	}
}

func (e *eventBus) getTopicContext(topic string) *topicContext {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.topics[topic]
}

func (e *eventBus) Publish(topic string, event interface{}) error {
	ctx := e.getTopicContext(topic)

	if ctx != nil {
		// non-blocking send
		select {
		case ctx.buffer <- event:
			return nil
		default:
			addCounter(ctx.errCounter, ErrTypeBufferFull, 1)
			return fmt.Errorf("buffer full")
		}
	} else {
		addCounter(e.errorCounter, ErrTypeNoSubscriber, 1)
	}
	return nil
}

func (e *eventBus) Subscribe(topic string, subscriber string, bufferSize int, handler ConsumeFunc) error {
	return e.GetOrRegisterTopic(topic).RegisterHandler(subscriber, bufferSize, handler)
}
