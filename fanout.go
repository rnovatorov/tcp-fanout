package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type fanout struct {
	connectAddr         string
	connectRetries      int
	connectIdle         time.Duration
	upstreamBufsize     uint
	upstreamReadTimeout time.Duration
	stopping            chan struct{}
	stopped             chan struct{}
	mu                  sync.Mutex
	subscriptions       map[int]subscription
}

type fanoutParams struct {
	connectAddr         string
	connectRetries      int
	connectIdle         time.Duration
	upstreamBufsize     uint
	upstreamReadTimeout time.Duration
}

func newFanout(p fanoutParams) *fanout {
	return &fanout{
		connectAddr:         p.connectAddr,
		connectRetries:      p.connectRetries,
		connectIdle:         p.connectIdle,
		upstreamBufsize:     p.upstreamBufsize,
		upstreamReadTimeout: p.upstreamReadTimeout,
		stopping:            make(chan struct{}),
		stopped:             make(chan struct{}),
		subscriptions:       make(map[int]subscription),
	}
}

func (fnt *fanout) start() <-chan error {
	errs := make(chan error, 1)
	go func() {
		defer close(fnt.stopped)
		for {
			conn, err := fnt.connect()
			if err != nil {
				errs <- fmt.Errorf("connect: %v", err)
				return
			}
			defer fnt.closeConn(conn)

			ups := &upstream{
				conn:        conn,
				fanout:      fnt,
				stopping:    fnt.stopping,
				bufsize:     fnt.upstreamBufsize,
				readTimeout: fnt.upstreamReadTimeout,
			}
			if err := ups.run(); err != nil {
				log.Printf("error, run upstream: %v", err)
			}
		}
	}()
	return errs
}

func (fnt *fanout) closeConn(conn net.Conn) {
	if err := conn.Close(); err != nil {
		log.Printf("error, close fanout conn: %v", err)
	}
}

func (fnt *fanout) stop() {
	select {
	case <-fnt.stopped:
	default:
		close(fnt.stopping)
		<-fnt.stopped
	}
}

func (fnt *fanout) connect() (net.Conn, error) {
	var conn net.Conn
	var lastErr error
	for i := 0; i < fnt.connectRetries; i++ {
		// FIXME: Dial with context.
		conn, lastErr = net.Dial("tcp", fnt.connectAddr)
		if lastErr != nil {
			log.Printf("error, %v", lastErr)
			select {
			case <-time.After(fnt.connectIdle):
				continue
			case <-fnt.stopping:
				return nil, lastErr
			}
		}
		log.Printf("info, connected %v->%v", conn.LocalAddr(), conn.RemoteAddr())
		return conn, nil
	}
	return nil, lastErr
}

func (fnt *fanout) pub(msg message) error {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	for _, sub := range fnt.subscriptions {
		select {
		case sub.stream <- msg:
		case <-sub.done:
		case <-fnt.stopping:
			return errors.New("stopped")
		}
	}
	return nil
}

func (fnt *fanout) sub(id int) subscription {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	sub := subscription{
		stream: make(chan message),
		done:   make(chan struct{}),
	}
	fnt.subscriptions[id] = sub
	return sub
}

func (fnt *fanout) unsub(id int) {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	delete(fnt.subscriptions, id)
}
