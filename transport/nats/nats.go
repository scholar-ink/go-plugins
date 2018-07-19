// Package nats provides a NATS transport
package nats

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-micro/transport"
	"github.com/micro/go-micro/transport/codec/json"
	"github.com/nats-io/go-nats"
)

type ntport struct {
	addrs []string
	opts  transport.Options
	nopts nats.Options
}

type ntportClient struct {
	conn *nats.Conn
	addr string
	id   string
	sub  *nats.Subscription
	opts transport.Options
}

type ntportSocket struct {
	conn *nats.Conn
	m    *nats.Msg
	r    chan *nats.Msg

	close chan bool

	sync.Mutex
	bl []*nats.Msg

	opts transport.Options
}

type ntportListener struct {
	conn *nats.Conn
	addr string
	exit chan bool

	sync.RWMutex
	so map[string]*ntportSocket

	opts transport.Options
}

var (
	DefaultTimeout = time.Minute
)

func init() {
	cmd.DefaultTransports["nats"] = NewTransport
}

func setAddrs(addrs []string) []string {
	var cAddrs []string
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{nats.DefaultURL}
	}
	return cAddrs
}

func (n *ntportClient) Send(m *transport.Message) error {
	b, err := n.opts.Codec.Marshal(m)
	if err != nil {
		return err
	}

	// no deadline
	if n.opts.Timeout == time.Duration(0) {
		return n.conn.PublishRequest(n.addr, n.id, b)
	}

	// use the deadline
	ch := make(chan error, 1)

	go func() {
		ch <- n.conn.PublishRequest(n.addr, n.id, b)
	}()

	select {
	case err := <-ch:
		return err
	case <-time.After(n.opts.Timeout):
		return errors.New("deadline exceeded")
	}
}

func (n *ntportClient) Recv(m *transport.Message) error {
	timeout := time.Second * 10
	if n.opts.Timeout > time.Duration(0) {
		timeout = n.opts.Timeout
	}

	rsp, err := n.sub.NextMsg(timeout)
	if err != nil {
		return err
	}

	var mr transport.Message
	if err := n.opts.Codec.Unmarshal(rsp.Data, &mr); err != nil {
		return err
	}

	*m = mr
	return nil
}

func (n *ntportClient) Close() error {
	n.sub.Unsubscribe()
	n.conn.Close()
	return nil
}

func (n *ntportSocket) Recv(m *transport.Message) error {
	if m == nil {
		return errors.New("message passed in is nil")
	}

	var r *nats.Msg
	var ok bool

	// if there's a deadline we use it
	if n.opts.Timeout > time.Duration(0) {
		select {
		case r, ok = <-n.r:
		case <-time.After(n.opts.Timeout):
			return errors.New("deadline exceeded")
		}
	} else {
		r, ok = <-n.r
	}

	if !ok {
		return io.EOF
	}

	n.Lock()
	if len(n.bl) > 0 {
		select {
		case n.r <- n.bl[0]:
			n.bl = n.bl[1:]
		default:
		}
	}
	n.Unlock()

	if err := n.opts.Codec.Unmarshal(r.Data, m); err != nil {
		return err
	}
	return nil
}

func (n *ntportSocket) Send(m *transport.Message) error {
	b, err := n.opts.Codec.Marshal(m)
	if err != nil {
		return err
	}

	// no deadline
	if n.opts.Timeout == time.Duration(0) {
		return n.conn.Publish(n.m.Reply, b)
	}

	// use the deadline
	ch := make(chan error, 1)

	go func() {
		ch <- n.conn.Publish(n.m.Reply, b)
	}()

	select {
	case err := <-ch:
		return err
	case <-time.After(n.opts.Timeout):
		return errors.New("deadline exceeded")
	}
}

func (n *ntportSocket) Close() error {
	select {
	case <-n.close:
		return nil
	default:
		close(n.close)
	}
	return nil
}

func (n *ntportListener) Addr() string {
	return n.addr
}

func (n *ntportListener) Close() error {
	n.exit <- true
	n.conn.Close()
	return nil
}

func (n *ntportListener) Accept(fn func(transport.Socket)) error {
	s, err := n.conn.SubscribeSync(n.addr)
	if err != nil {
		return err
	}

	var lerr error

	go func() {
		<-n.exit
		lerr = s.Unsubscribe()
	}()

	for {
		m, err := s.NextMsg(time.Minute)
		if err != nil && err == nats.ErrTimeout {
			continue
		} else if err != nil {
			return err
		}

		n.RLock()
		sock, ok := n.so[m.Reply]
		n.RUnlock()

		if !ok {
			sock = &ntportSocket{
				conn:  n.conn,
				m:     m,
				r:     make(chan *nats.Msg, 1),
				close: make(chan bool),
				opts:  n.opts,
			}
			n.Lock()
			n.so[m.Reply] = sock
			n.Unlock()

			go func() {
				// TODO: think of a better error response strategy
				defer func() {
					if r := recover(); r != nil {
						sock.Close()
					}
				}()
				fn(sock)
			}()

			go func() {
				<-sock.close
				n.Lock()
				delete(n.so, sock.m.Reply)
				n.Unlock()
			}()
		}

		select {
		case <-sock.close:
			continue
		default:
		}

		sock.Lock()
		sock.bl = append(sock.bl, m)
		select {
		case sock.r <- sock.bl[0]:
			sock.bl = sock.bl[1:]
		default:
		}
		sock.Unlock()

	}
}

func (n *ntport) Dial(addr string, dialOpts ...transport.DialOption) (transport.Client, error) {
	dopts := transport.DialOptions{
		Timeout: transport.DefaultDialTimeout,
	}

	for _, o := range dialOpts {
		o(&dopts)
	}

	opts := n.nopts
	opts.Servers = n.addrs
	opts.Secure = n.opts.Secure
	opts.TLSConfig = n.opts.TLSConfig
	opts.Timeout = dopts.Timeout

	// secure might not be set
	if n.opts.TLSConfig != nil {
		opts.Secure = true
	}

	c, err := opts.Connect()
	if err != nil {
		return nil, err
	}

	id := nats.NewInbox()
	sub, err := c.SubscribeSync(id)
	if err != nil {
		return nil, err
	}

	return &ntportClient{
		conn: c,
		addr: addr,
		id:   id,
		sub:  sub,
		opts: n.opts,
	}, nil
}

func (n *ntport) Listen(addr string, listenOpts ...transport.ListenOption) (transport.Listener, error) {
	opts := n.nopts
	opts.Servers = n.addrs
	opts.Secure = n.opts.Secure
	opts.TLSConfig = n.opts.TLSConfig

	// secure might not be set
	if n.opts.TLSConfig != nil {
		opts.Secure = true
	}

	c, err := opts.Connect()
	if err != nil {
		return nil, err
	}

	// in case address has not been specifically set, create a new nats.Inbox()
	if addr == server.DefaultAddress {
		addr = nats.NewInbox()
	}

	// make sure addr subject is not empty
	if len(addr) == 0 {
		return nil, errors.New("addr (nats subject) must not be empty")
	}

	// since NATS implements a text based protocol, no space characters are
	// admitted in the addr (subject name)
	if strings.Contains(addr, " ") {
		return nil, errors.New("addr (nats subject) must not contain space characters")
	}

	return &ntportListener{
		addr: addr,
		conn: c,
		exit: make(chan bool, 1),
		so:   make(map[string]*ntportSocket),
		opts: n.opts,
	}, nil
}

func (n *ntport) String() string {
	return "nats"
}

func NewTransport(opts ...transport.Option) transport.Transport {

	options := transport.Options{
		// Default codec
		Codec:   json.NewCodec(),
		Timeout: DefaultTimeout,
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	natsOptions := nats.GetDefaultOptions()
	if n, ok := options.Context.Value(optionsKey{}).(nats.Options); ok {
		natsOptions = n
	}

	// transport.Options have higher priority than nats.Options
	// only if Addrs, Secure or TLSConfig were not set through a transport.Option
	// we read them from nats.Option
	if len(options.Addrs) == 0 {
		options.Addrs = natsOptions.Servers
	}

	if !options.Secure {
		options.Secure = natsOptions.Secure
	}

	if options.TLSConfig == nil {
		options.TLSConfig = natsOptions.TLSConfig
	}

	// check & add nats:// prefix (this makes also sure that the addresses
	// stored in natsRegistry.addrs and options.Addrs are identical)
	options.Addrs = setAddrs(options.Addrs)

	return &ntport{
		addrs: options.Addrs,
		opts:  options,
		nopts: natsOptions,
	}
}
