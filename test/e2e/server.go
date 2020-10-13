package e2e

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/errs"
)

type server struct {
	p        serverParams
	addr     net.Addr
	writable chan struct{}
	errc     chan error
	started  chan struct{}
	stopped  chan struct{}
	once     sync.Once
	stopping chan struct{}
}

type serverParams struct {
	data                string
	bufsize             int
	acceptTimeout       time.Duration
	waitWritableTimeout time.Duration
	writeTimeout        time.Duration
}

func startServer(p serverParams) (*server, error) {
	if p.bufsize <= 0 {
		return nil, fmt.Errorf("expected positive bufsize, got: %v", p.bufsize)
	}
	srv := &server{
		p:        p,
		errc:     make(chan error, 1),
		writable: make(chan struct{}),
		started:  make(chan struct{}),
		stopped:  make(chan struct{}),
		stopping: make(chan struct{}),
	}
	go func() {
		defer close(srv.stopped)
		srv.errc <- srv.run()
	}()
	select {
	case err := <-srv.errc:
		return nil, err
	case <-srv.started:
		return srv, nil
	}
}

func (srv *server) stop() error {
	srv.once.Do(func() { close(srv.stopping) })
	<-srv.stopped
	return <-srv.errc
}

func (srv *server) run() error {
	lsn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer lsn.Close()

	srv.addr = lsn.Addr()
	close(srv.started)

	conn, err := srv.accept(lsn)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = srv.handle(conn)
	<-srv.stopping
	return err
}

func (srv *server) accept(lsn net.Listener) (net.Conn, error) {
	var conn net.Conn
	var err error
	accepted := make(chan struct{})
	go func() {
		conn, err = lsn.Accept()
		close(accepted)
	}()
	select {
	case <-srv.stopping:
		return nil, errs.Stopping
	case <-time.After(srv.p.acceptTimeout):
		return nil, fmt.Errorf("accept: timed out after %v", srv.p.acceptTimeout)
	case <-accepted:
		return conn, err
	}
}

func (srv *server) handle(conn net.Conn) error {
	select {
	case <-srv.stopping:
		return errs.Stopping
	case <-time.After(srv.p.waitWritableTimeout):
		return fmt.Errorf("wait writable: timed out after %v", srv.p.waitWritableTimeout)
	case <-srv.writable:
		return srv.write(conn)
	}
}

func (srv *server) write(conn net.Conn) error {
	data := srv.p.data
	for len(data) > 0 {
		deadline := time.Now().Add(srv.p.writeTimeout)
		conn.SetWriteDeadline(deadline)

		size := srv.p.bufsize
		if size > len(data) {
			size = len(data)
		}
		buf := []byte(data[:size])
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("written 0 bytes: bufsize=%d", srv.p.bufsize)
		}
		data = data[n:]
	}
	return nil
}
