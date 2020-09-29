package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"syscall"
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}

var connectAddr = flag.String("connect", "", "address to connect to")
var listenAddr = flag.String("listen", "", "address to listen to")

func run() error {
	fnt := newFanout(*connectAddr)
	ferr := fnt.start()
	defer fnt.stop()

	srv, err := newServer(*listenAddr, fnt)
	if err != nil {
		return fmt.Errorf("create server: %v", err)
	}
	serr := srv.start()
	defer srv.stop()

	select {
	case err = <-ferr:
		return fmt.Errorf("fanout error: %v", err)
	case err = <-serr:
		return fmt.Errorf("server error: %v", err)
	}
}

type fanout struct {
	addr string
	stp  chan struct{}
	stpd chan struct{}

	mu      sync.Mutex
	clients []*net.TCPConn
}

func newFanout(addr string) *fanout {
	return &fanout{
		addr: addr,
		stp:  make(chan struct{}),
		stpd: make(chan struct{}),
	}
}

func (fnt *fanout) start() <-chan error {
	errch := make(chan error, 1)
	go func() {
		defer close(fnt.stpd)
		conn, err := fnt.connect()
		if err != nil {
			errch <- fmt.Errorf("connect: %v", err)
			return
		}
		for {
			buf := make([]byte, 8)
			n, err := conn.Read(buf)
			log.Printf("err=%v, buf[:%d]=%v", err, n, buf[:n])
			fnt.send(buf[:n])
			if err != nil {
				errch <- fmt.Errorf("read: %v", err)
				return
			}
		}
		// 		err = conn.Control(func(fd uintptr) {
		// 			n, ok, sc, err := Splice(1, int(fd), remain)
		// 			log.Printf("splice: n=%d, ok=%v, sc=%s, err=%v", n, ok, sc, err)
		// 		})
		// 		errch <- err
	}()
	return errch
}

func (fnt *fanout) stop() {
	select {
	case <-fnt.stpd:
	default:
		close(fnt.stp)
		<-fnt.stpd
	}
}

func (fnt *fanout) send(buf []byte) error {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	for _, c := range fnt.clients {
		cbuf := buf[:]
		for len(cbuf) > 0 {
			n, err := c.Write(buf)
			if err != nil {
				return fmt.Errorf("write %s->%s: %v", c.LocalAddr(), c.RemoteAddr(), err)
			}
			cbuf = cbuf[n:]
		}
	}
	return nil
}

func (fnt *fanout) connect() (*net.TCPConn, error) {
	conn, err := net.Dial("tcp", fnt.addr)
	if err != nil {
		return nil, fmt.Errorf("net dial: %v", err)
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("cast to tcp conn: %v", conn)
	}
	return tcpConn, nil
}

func (fnt *fanout) attach(conn *net.TCPConn) {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	fnt.clients = append(fnt.clients, conn)
}

type server struct {
	addr string
	fnt  *fanout
	stp  chan struct{}
	stpd chan struct{}
}

func newServer(addr string, fnt *fanout) (*server, error) {
	srv := &server{
		addr: addr,
		fnt:  fnt,
		stp:  make(chan struct{}),
		stpd: make(chan struct{}),
	}
	return srv, nil
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
	lsn, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return fmt.Errorf("listen: %v", err)
	}
	defer lsn.Close()
	conns, aerrs := srv.accept(lsn)
	for {
		select {
		case <-srv.stp:
			return nil
		case err := <-aerrs:
			return fmt.Errorf("accept: %v", err)
		case conn := <-conns:
			go srv.handle(conn)
		}
	}
}

func (srv *server) accept(lsn net.Listener) (<-chan *net.TCPConn, <-chan error) {
	conns := make(chan *net.TCPConn)
	errs := make(chan error, 1)
	go func() {
		defer close(conns)
		defer close(errs)
		for {
			conn, err := lsn.Accept()
			if err != nil {
				errs <- fmt.Errorf("listener: %v", err)
				return
			}
			tcpConn, ok := conn.(*net.TCPConn)
			if !ok {
				errs <- fmt.Errorf("cast to tcp conn: %v", conn)
				return
			}
			conns <- tcpConn
		}
	}()
	return conns, errs
}

func (srv *server) handle(conn *net.TCPConn) {
	srv.fnt.attach(conn)
}

const (
	spliceNonblock = 0x2
	maxSpliceSize  = 4 << 20
	remain         = 1 << 62
)

func Splice(dst, src int, remain int64) (written int64, handled bool, sc string, err error) {
	prfd, pwfd, sc, err := newTempPipe()
	if err != nil {
		return 0, false, sc, err
	}
	defer destroyTempPipe(prfd, pwfd)
	var inPipe, n int
	for err == nil && remain > 0 {
		max := maxSpliceSize
		if int64(max) > remain {
			max = int(remain)
		}
		inPipe, err = spliceDrain(pwfd, src, max)
		handled = handled || (err != syscall.EINVAL)
		if err != nil || (inPipe == 0 && err == nil) {
			break
		}
		n, err = splicePump(dst, prfd, inPipe)
		if n > 0 {
			written += int64(n)
			remain -= int64(n)
		}
	}
	if err != nil {
		return written, handled, "splice", err
	}
	return written, true, "", nil
}

func spliceDrain(pipefd, sockfd int, max int) (int, error) {
	for {
		n, err := splice(pipefd, sockfd, max, spliceNonblock)
		if err == syscall.EINTR {
			continue
		}
		if err != syscall.EAGAIN {
			return n, err
		}
	}
}

func splicePump(sockfd, pipefd int, inPipe int) (int, error) {
	written := 0
	for inPipe > 0 {
		n, err := splice(sockfd, pipefd, inPipe, spliceNonblock)
		// Here, the condition n == 0 && err == nil should never be
		// observed, since Splice controls the write side of the pipe.
		if n > 0 {
			inPipe -= n
			written += n
			continue
		}
		if err != syscall.EAGAIN {
			return written, err
		}
	}
	return written, nil
}

func splice(out int, in int, max int, flags int) (int, error) {
	n, err := syscall.Splice(in, nil, out, nil, max, flags)
	return int(n), err
}

func newTempPipe() (prfd, pwfd int, sc string, err error) {
	var fds [2]int
	const flags = syscall.O_CLOEXEC | syscall.O_NONBLOCK
	if err := syscall.Pipe2(fds[:], flags); err != nil {
		return -1, -1, "pipe2", err
	}
	return fds[0], fds[1], "", nil
}

func destroyTempPipe(prfd, pwfd int) error {
	err := syscall.Close(prfd)
	err1 := syscall.Close(pwfd)
	if err == nil {
		return err1
	}
	return err
}
