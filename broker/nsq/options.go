package nsq

import (
	"context"
	"time"

	"github.com/micro/go-micro/broker"
	nsq "github.com/nsqio/go-nsq"
	"github.com/scholar-ink/protobuf/ptypes/duration"
)

type concurrentHandlerKey struct{}
type maxInFlightKey struct{}
type maxAttemptsKey struct{}
type requeueDelayKey struct{}
type maxRequeueDelayKey struct{}
type asyncPublishKey struct{}
type deferredPublishKey struct{}
type lookupdAddrsKey struct{}
type consumerOptsKey struct{}

func WithConcurrentHandlers(n int) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, concurrentHandlerKey{}, n)
	}
}

func WithMaxInFlight(n int) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, maxInFlightKey{}, n)
	}
}

func WithMaxAttempts(n uint16) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, maxAttemptsKey{}, n)
	}
}

func WithRequeueDelay(time duration.Duration) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, requeueDelayKey{}, time)
	}
}

func WithMaxRequeueDelay(time duration.Duration) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, maxRequeueDelayKey{}, time)
	}
}

func WithAsyncPublish(doneChan chan *nsq.ProducerTransaction) broker.PublishOption {
	return func(o *broker.PublishOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, asyncPublishKey{}, doneChan)
	}
}

func WithDeferredPublish(delay time.Duration) broker.PublishOption {
	return func(o *broker.PublishOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, deferredPublishKey{}, delay)
	}
}

func WithLookupdAddrs(addrs []string) broker.Option {
	return func(o *broker.Options) {
		o.Context = context.WithValue(o.Context, lookupdAddrsKey{}, addrs)
	}
}

func WithConsumerOpts(consumerOpts []string) broker.Option {
	return func(o *broker.Options) {
		o.Context = context.WithValue(o.Context, consumerOptsKey{}, consumerOpts)
	}
}
