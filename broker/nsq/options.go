package nsq

import (
	"context"
	"time"

	"github.com/micro/go-micro/broker"
	nsq "github.com/nsqio/go-nsq"
)

type ConcurrentHandlerKey struct{}
type MaxInFlightKey struct{}
type MaxAttemptsKey struct{}
type RequeueDelayKey struct{}
type MaxRequeueDelayKey struct{}
type asyncPublishKey struct{}
type deferredPublishKey struct{}
type lookupdAddrsKey struct{}
type consumerOptsKey struct{}

func WithConcurrentHandlers(n int) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, ConcurrentHandlerKey{}, n)
	}
}

func WithMaxInFlight(n int) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, MaxInFlightKey{}, n)
	}
}

func WithMaxAttempts(n uint16) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, MaxAttemptsKey{}, n)
	}
}

func WithRequeueDelay(time time.Duration) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, RequeueDelayKey{}, time)
	}
}

func WithMaxRequeueDelay(time time.Duration) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, MaxRequeueDelayKey{}, time)
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
