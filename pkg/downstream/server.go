package downstream

import (
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
	handlers sync.WaitGroup
	stopped  chan struct{}
	once     sync.Once
	stopping chan struct{}
}

func StartServer(params ServerParams) (*Server, <-chan error) {
	srv := &Server{
		ServerParams: params,
		stopped:      make(chan struct{}),
		stopping:     make(chan struct{}),
	}
	errs := make(chan error, 1)
	go func() {
		if err := srv.run(); err != nil {
			errs <- err
		}
	}()
	return srv, errs
}

func (srv *Server) Stop() {
	srv.once.Do(func() { close(srv.stopping) })
	<-srv.stopped
}

func (srv *Server) run() error {
	defer close(srv.stopped)
	if err := srv.serve(); err != nil {
		return fmt.Errorf("serve: %v", err)
	}
	return nil
}

func (srv *Server) serve() error {
	defer srv.handlers.Wait()

	listener, err := net.Listen("tcp", srv.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %v", err)
	}
	log.Printf("info, listening on %s", srv.ListenAddr)
	defer listener.Close()

	conns, errs := srv.acceptConns(listener)
	for id := 0; ; id++ {
		select {
		case <-srv.stopping:
			return nil
		case err := <-errs:
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
	if err := s.run(); err != nil {
		log.Printf("error, downstream session-%d: %v", id, err)
	} else {
		log.Printf("info, stopped downstream session-%d", id)
	}
}

func (srv *Server) acceptConns(lsn net.Listener) (<-chan net.Conn, <-chan error) {
	conns := make(chan net.Conn)
	errs := make(chan error, 1)
	go func() {
		for {
			conn, err := srv.acceptConn(lsn)
			if err != nil {
				errs <- err
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
	return conns, errs
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
