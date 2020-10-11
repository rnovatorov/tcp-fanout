package downstream

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/streaming"
)

type ServerParams struct {
	ListenAddr   string
	Fanout       *streaming.Fanout
	WriteTimeout time.Duration
}

type Server struct {
	ServerParams
	addr     net.Addr
	handlers sync.WaitGroup
	started  chan struct{}
	stopped  chan struct{}
	once     sync.Once
	stopping chan struct{}
}

func StartServer(params ServerParams) (*Server, error, <-chan error) {
	srv := &Server{
		ServerParams: params,
		started:      make(chan struct{}),
		stopped:      make(chan struct{}),
		stopping:     make(chan struct{}),
	}
	errc := make(chan error, 1)
	go func() {
		defer close(srv.stopped)
		if err := srv.run(); err != nil {
			errc <- err
		}
	}()
	select {
	case <-srv.started:
		return srv, nil, errc
	case err := <-errc:
		return nil, err, nil
	}
}

func (srv *Server) Stop() {
	srv.once.Do(func() { close(srv.stopping) })
	<-srv.stopped
}

func (srv *Server) Addr() net.Addr {
	<-srv.started
	return srv.addr
}

func (srv *Server) run() error {
	defer srv.handlers.Wait()

	lsn, err := net.Listen("tcp", srv.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %v", err)
	}
	defer lsn.Close()

	log.Printf("info, listening on %s", srv.ListenAddr)
	srv.addr = lsn.Addr()
	close(srv.started)

	conns, errc := srv.acceptConns(lsn)
	for id := 0; ; id++ {
		select {
		case <-srv.stopping:
			return errors.New("stopping")
		case err := <-errc:
			return err
		case conn := <-conns:
			srv.handlers.Add(1)
			go func() {
				defer srv.handlers.Done()
				defer closeConn(conn)
				srv.handle(id, conn)
			}()
		}
	}
}

func (srv *Server) handle(id int, conn net.Conn) {
	s := &session{
		id:           id,
		conn:         conn,
		fanout:       srv.Fanout,
		writeTimeout: srv.WriteTimeout,
		stopping:     srv.stopping,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.run(); err != nil {
			log.Printf("error, downstream session-%d: %v", id, err)
		} else {
			log.Printf("info, stopped downstream session-%d", id)
		}
	}()
	select {
	case <-done:
	case <-srv.stopping:
	}
}

func (srv *Server) acceptConns(lsn net.Listener) (<-chan net.Conn, <-chan error) {
	conns := make(chan net.Conn)
	errc := make(chan error, 1)
	go func() {
		for {
			conn, err := srv.acceptConn(lsn)
			if err != nil {
				errc <- err
				return
			}
			select {
			case conns <- conn:
			case <-srv.stopping:
				closeConn(conn)
				return
			}
		}
	}()
	return conns, errc
}

func (srv *Server) acceptConn(lsn net.Listener) (net.Conn, error) {
	conn, err := lsn.Accept()
	if err != nil {
		return nil, err
	}
	log.Printf("info, accept %v->%v", conn.LocalAddr(), conn.RemoteAddr())
	return conn, nil
}

func closeConn(conn net.Conn) {
	la, ra := conn.LocalAddr(), conn.RemoteAddr()
	if err := conn.Close(); err != nil {
		log.Printf("warn, close %v->%v: %v", la, ra, err)
		return
	}
	log.Printf("info, close %v->%v", la, ra)
}
