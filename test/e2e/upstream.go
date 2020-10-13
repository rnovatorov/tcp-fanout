package e2e

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/errs"
)

type upstream struct {
	p        upstreamParams
	addr     net.Addr
	writable chan struct{}
	errc     chan error
	started  chan struct{}
	stopped  chan struct{}
	once     sync.Once
	stopping chan struct{}
}

type upstreamParams struct {
	data                string
	bufsize             int
	acceptTimeout       time.Duration
	waitWritableTimeout time.Duration
	writeTimeout        time.Duration
}

func startUpstream(p upstreamParams) (*upstream, error) {
	if p.bufsize <= 0 {
		return nil, fmt.Errorf("expected positive bufsize, got: %v", p.bufsize)
	}
	ups := &upstream{
		p:        p,
		errc:     make(chan error, 1),
		writable: make(chan struct{}),
		started:  make(chan struct{}),
		stopped:  make(chan struct{}),
		stopping: make(chan struct{}),
	}
	go func() {
		defer close(ups.stopped)
		ups.errc <- ups.run()
	}()
	select {
	case err := <-ups.errc:
		return nil, err
	case <-ups.started:
		return ups, nil
	}
}

func (ups *upstream) stop() error {
	ups.once.Do(func() { close(ups.stopping) })
	<-ups.stopped
	return <-ups.errc
}

func (ups *upstream) run() error {
	lsn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer lsn.Close()

	ups.addr = lsn.Addr()
	close(ups.started)

	conn, err := ups.accept(lsn)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = ups.handle(conn)
	<-ups.stopping
	return err
}

func (ups *upstream) accept(lsn net.Listener) (net.Conn, error) {
	var conn net.Conn
	var err error
	accepted := make(chan struct{})
	go func() {
		conn, err = lsn.Accept()
		close(accepted)
	}()
	select {
	case <-ups.stopping:
		return nil, errs.Stopping
	case <-time.After(ups.p.acceptTimeout):
		return nil, fmt.Errorf("accept: timed out after %v", ups.p.acceptTimeout)
	case <-accepted:
		return conn, err
	}
}

func (ups *upstream) handle(conn net.Conn) error {
	select {
	case <-ups.stopping:
		return errs.Stopping
	case <-time.After(ups.p.waitWritableTimeout):
		return fmt.Errorf("wait writable: timed out after %v", ups.p.waitWritableTimeout)
	case <-ups.writable:
		return ups.write(conn)
	}
}

func (ups *upstream) write(conn net.Conn) error {
	data := ups.p.data
	for len(data) > 0 {
		deadline := time.Now().Add(ups.p.writeTimeout)
		conn.SetWriteDeadline(deadline)

		size := ups.p.bufsize
		if size > len(data) {
			size = len(data)
		}
		buf := []byte(data[:size])
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("written 0 bytes: bufsize=%d", ups.p.bufsize)
		}
		data = data[n:]
	}
	return nil
}
