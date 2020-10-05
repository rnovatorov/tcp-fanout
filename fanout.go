package main

import (
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

	mu      sync.Mutex
	clients map[int]chan message
}

func newFanout(addr string, retries int, idle time.Duration) *fanout {
	return &fanout{
		addr:    addr,
		retries: retries,
		idle:    idle,
		stp:     make(chan struct{}),
		stpd:    make(chan struct{}),
		clients: make(map[int]chan message),
	}
}

func (fnt *fanout) start() <-chan error {
	errch := make(chan error, 1)
	go func() {
		defer close(fnt.stpd)
		for {
			conn, err := fnt.connect()
			if err != nil {
				errch <- fmt.Errorf("connect: %v", err)
				return
			}
			defer fnt.closeConn(conn)

			ups, err := newUpstream(conn, fnt, fnt.stp)
			if err != nil {
				errch <- fmt.Errorf("new upstream: %v", err)
				return
			}
			if err := ups.run(); err != nil {
				log.Printf("error, run upstream: %v", err)
			}
		}
	}()
	return errch
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
			log.Printf("error, dial: %v", lastErr)
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

type message struct {
	fd int
	n  int
}

func (fnt *fanout) pub(msg message) {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	for _, stream := range fnt.clients {
		stream <- msg
	}
}

func (fnt *fanout) sub(id int) <-chan message {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	stream := make(chan message)
	fnt.clients[id] = stream
	return stream
}

func (fnt *fanout) unsub(id int) {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	delete(fnt.clients, id)
}
