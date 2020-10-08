package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type server struct {
	fanout             *fanout
	listenAddr         string
	clientWriteTimeout time.Duration
	stopping           chan struct{}
	stopped            chan struct{}
	handlers           sync.WaitGroup
}

type serverParams struct {
	fanout             *fanout
	listenAddr         string
	clientWriteTimeout time.Duration
}

func newServer(p serverParams) *server {
	return &server{
		fanout:             p.fanout,
		listenAddr:         p.listenAddr,
		clientWriteTimeout: p.clientWriteTimeout,
		stopping:           make(chan struct{}),
		stopped:            make(chan struct{}),
	}
}

func (srv *server) start() <-chan error {
	errs := make(chan error, 1)
	go func() {
		defer close(srv.stopped)
		err := srv.serve()
		errs <- fmt.Errorf("serve: %v", err)
	}()
	return errs
}

func (srv *server) stop() {
	select {
	case <-srv.stopped:
	default:
		close(srv.stopping)
		<-srv.stopped
	}
}

func (srv *server) serve() error {
	defer srv.handlers.Wait()

	listener, err := net.Listen("tcp", srv.listenAddr)
	if err != nil {
		return fmt.Errorf("listen: %v", err)
	}
	log.Printf("info, listening on %s", srv.listenAddr)
	defer listener.Close()

	conns, errs := srv.acceptConns(listener)
	for id := 0; ; id++ {
		select {
		case <-srv.stopping:
			return nil
		case err := <-errs:
			return fmt.Errorf("accept conns: %v", err)
		case conn := <-conns:
			srv.handlers.Add(1)
			go srv.handle(id, conn)
		}
	}
}

func (srv *server) handle(id int, conn net.Conn) {
	defer srv.handlers.Done()
	defer srv.closeConn(conn)
	cli := &client{
		id:           id,
		conn:         conn,
		fanout:          srv.fanout,
		stopping:     srv.stopping,
		writeTimeout: srv.clientWriteTimeout,
	}
	if err := cli.run(); err != nil {
		log.Printf("error, run client-%d: %v", id, err)
	}
}

func (srv *server) acceptConns(lsn net.Listener) (<-chan net.Conn, <-chan error) {
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
				srv.closeConn(conn)
				return
			}
		}
	}()
	return conns, errs
}

func (srv *server) acceptConn(lsn net.Listener) (net.Conn, error) {
	conn, err := lsn.Accept()
	if err != nil {
		return nil, err
	}
	log.Printf("info, accept %v->%v", conn.LocalAddr(), conn.RemoteAddr())
	return conn, nil
}

func (srv *server) closeConn(conn net.Conn) {
	la, ra := conn.LocalAddr(), conn.RemoteAddr()
	if err := conn.Close(); err != nil {
		log.Printf("warn, close %v->%v: %v", la, ra, err)
		return
	}
	log.Printf("info, close %v->%v", la, ra)
}
