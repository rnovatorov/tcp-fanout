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
	pipes, err := ups.makePipes()
	if err != nil {
		return fmt.Errorf("make pipes: %v", err)
	}
	defer ups.destroyPipes(pipes)
	sizes := make([]int, len(pipes))

	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %v", err)
	}
	defer devNull.Close()
	devNullFd := devNull.Fd()

	for i := 0; ; i %= len(pipes) {
		// FIXME: Store pipe fds instead of calling Fd.
		rf, wf := pipes[i][0], pipes[i][1]
		rfd, wfd := rf.Fd(), wf.Fd()
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
		remain -= n
	}
	return nil
}

func (ups *upstream) makePipes() ([][2]*os.File, error) {
	pipes := make([][2]*os.File, 2)
	for i := range pipes {
		rf, wf, err := os.Pipe()
		if err != nil {
			for j := 0; j < i; j++ {
				pipes[j][0].Close()
				pipes[j][1].Close()
			}
			return nil, fmt.Errorf("make pipes[%d]: %v", i, err)
		}
		pipes[i][0], pipes[i][1] = rf, wf
	}
	return pipes, nil
}

func (ups *upstream) destroyPipes(pipes [][2]*os.File) {
	for i, pipe := range pipes {
		rf, wf := pipe[0], pipe[1]
		if err := rf.Close(); err != nil {
			log.Printf("error, ups: close pipes[%d] reader: %v", i, err)
		}
		if err := wf.Close(); err != nil {
			log.Printf("error, ups: close pipes[%d] writer: %v", i, err)
		}
	}
}
