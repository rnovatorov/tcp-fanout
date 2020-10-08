package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"syscall"
)

type upstream struct {
	conn syscall.RawConn
	fnt  *fanout
	stp  chan struct{}
}

func newUpstream(conn net.Conn, fnt *fanout, stp chan struct{}) (*upstream, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("cast to tcp conn: %v", conn)
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("make syscall conn: %v", err)
	}
	ups := &upstream{
		conn: rawConn,
		fnt:  fnt,
		stp:  stp,
	}
	return ups, nil
}

func (ups *upstream) run() error {
	buf, err := newPipebuf(2, true)
	if err != nil {
		return fmt.Errorf("new pipebuf: %v", err)
	}
	defer buf.close()

	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %v", err)
	}
	defer devNull.Close()
	devNullFd := devNull.Fd()

	sizes := make([]int, len(buf))
	for i := 0; ; i %= len(buf) {
		rfd, wfd := buf[i][0], buf[i][1]
		size := sizes[i]
		if err := ups.discard(int(devNullFd), int(rfd), size); err != nil {
			return fmt.Errorf("discard: %v", err)
		}
		n, err := ups.read(int(wfd))
		if err != nil {
			return fmt.Errorf("read: %v", err)
		}
		if n == 0 {
			return errors.New("eof")
		}
		if err := ups.fnt.pub(message{fd: int(rfd), n: n}); err != nil {
			return fmt.Errorf("pub: %v", err)
		}
		sizes[i] = n
		i++
	}
}

func (ups *upstream) read(dst int) (int, error) {
	var n int
	var rerr error
	err := ups.conn.Read(func(src uintptr) bool {
		n, rerr = splice(dst, int(src), spliceMaxSize, 0)
		log.Printf("debug, upstream.read: splice: n=%d, rerr=%v", n, rerr)
		return rerr != syscall.EINTR && rerr != syscall.EAGAIN
	})
	if err == nil {
		err = rerr
	}
	return n, err
}

func (ups *upstream) discard(dst, src, remain int) error {
	for remain > 0 {
		n, err := splice(dst, src, remain, 0)
		log.Printf("debug, upstream.discard: splice: n=%d, err=%v", n, err)
		if err == syscall.EINTR || err == syscall.EAGAIN {
			continue
		}
		if err != nil {
			return fmt.Errorf("splice: %v", err)
		}
		if n == 0 {
			return errors.New("eof")
		}
		remain -= n
	}
	return nil
}
