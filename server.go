package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type server struct {
	addr string
	fnt  *fanout
	stp  chan struct{}
	stpd chan struct{}
	wg   sync.WaitGroup
}

func newServer(addr string, fnt *fanout) *server {
	return &server{
		addr: addr,
		fnt:  fnt,
		stp:  make(chan struct{}),
		stpd: make(chan struct{}),
	}
}

func (srv *server) start() <-chan error {
	errch := make(chan error, 1)
	go func() {
		defer close(srv.stpd)
		err := srv.serve()
		errch <- fmt.Errorf("serve: %v", err)
	}()
	return errch
}

func (srv *server) stop() {
	select {
	case <-srv.stpd:
	default:
		close(srv.stp)
		<-srv.stpd
	}
}

func (srv *server) serve() error {
	defer srv.wg.Wait()

	lsn, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return fmt.Errorf("listen: %v", err)
	}
	log.Printf("info, listening on %s", srv.addr)
	defer lsn.Close()

	conns, errs := srv.acceptConns(lsn)
	for id := 0; ; id++ {
		select {
		case <-srv.stp:
			return nil
		case err := <-errs:
			return fmt.Errorf("accept conns: %v", err)
		case conn := <-conns:
			srv.wg.Add(1)
			go srv.handle(id, conn)
		}
	}
}

func (srv *server) handle(id int, conn net.Conn) {
	defer srv.wg.Done()
	defer srv.closeConn(conn)
	cli := &client{
		id:   id,
		conn: conn,
		fnt:  srv.fnt,
		stp:  srv.stp,
		// FIXME: Hard-code.
		timeout: 5 * time.Second,
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
			case <-srv.stp:
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
