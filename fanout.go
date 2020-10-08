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
	addr    string
	retries int
	idle    time.Duration

	stp  chan struct{}
	stpd chan struct{}
	ups  *upstream

	mu   sync.Mutex
	subs map[int]subscription
}

func newFanout(addr string, retries int, idle time.Duration) *fanout {
	return &fanout{
		addr:    addr,
		retries: retries,
		idle:    idle,
		stp:     make(chan struct{}),
		stpd:    make(chan struct{}),
		subs:    make(map[int]subscription),
	}
}

func (fnt *fanout) start() <-chan error {
	errs := make(chan error, 1)
	go func() {
		defer close(fnt.stpd)
		for {
			conn, err := fnt.connect()
			if err != nil {
				errs <- fmt.Errorf("connect: %v", err)
				return
			}
			defer fnt.closeConn(conn)

			ups := &upstream{
				conn: conn,
				fnt:  fnt,
				stp:  fnt.stp,
				// FIXME: Hard-code.
				bufsize: 1 << 16,
				timeout: 5 * time.Second,
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
	case <-fnt.stpd:
	default:
		close(fnt.stp)
		<-fnt.stpd
	}
}

func (fnt *fanout) connect() (net.Conn, error) {
	var conn net.Conn
	var lastErr error
	for i := 0; i < fnt.retries; i++ {
		// FIXME: Dial with context.
		conn, lastErr = net.Dial("tcp", fnt.addr)
		if lastErr != nil {
			log.Printf("error, %v", lastErr)
			select {
			case <-time.After(fnt.idle):
				continue
			case <-fnt.stp:
				return nil, lastErr
			}
		}
		log.Printf("info, connected %v->%v", conn.LocalAddr(), conn.RemoteAddr())
		return conn, nil
	}
	return nil, fmt.Errorf("stopped after %d retries: %v", fnt.retries, lastErr)
}

func (fnt *fanout) pub(msg message) error {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	for _, sub := range fnt.subs {
		select {
		case sub.stream <- msg:
		case <-sub.done:
		case <-fnt.stp:
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
	fnt.subs[id] = sub
	return sub
}

func (fnt *fanout) unsub(id int) {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	delete(fnt.subs, id)
}
