package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"syscall"
)

type client struct {
	id   int
	conn syscall.RawConn
	fnt  *fanout
	stp  chan struct{}
}

func newClient(id int, conn net.Conn, fnt *fanout, stp chan struct{}) (*client, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("cast to tcp conn: %v", conn)
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("make syscall conn: %v", err)
	}
	cli := &client{
		id:   id,
		conn: rawConn,
		fnt:  fnt,
		stp:  stp,
	}
	return cli, nil
}

func (cli *client) run() error {
	rf, wf, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("make pipe: %v", err)
	}
	defer cli.closePipe(rf, wf)
	rfd, wfd := rf.Fd(), wf.Fd()

	sub := cli.fnt.sub(cli.id)
	defer cli.fnt.unsub(cli.id)
	defer close(sub.done)

	for {
		var msg message
		select {
		case <-cli.stp:
			return nil
		case msg = <-sub.stream:
		}
		if err := cli.copy(int(wfd), msg.fd, msg.n); err != nil {
			return fmt.Errorf("copy: %v", err)
		}
		if err := cli.write(int(rfd)); err != nil {
			return fmt.Errorf("write: %v", err)
		}
	}
}

func (cli *client) copy(dst, src, remain int) error {
	for remain > 0 {
		n, err := tee(dst, src, remain, 0)
		log.Printf("debug, client-%d.copy: tee: n=%d, err=%v", cli.id, n, err)
		if err == syscall.EINTR || err == syscall.EAGAIN {
			continue
		}
		if err != nil {
			return fmt.Errorf("tee: %v", err)
		}
		remain -= n
	}
	return nil
}

func (cli *client) write(src int) error {
	var n int
	var werr error
	err := cli.conn.Write(func(dst uintptr) bool {
		n, werr = splice(int(dst), src, spliceMaxSize, 0)
		log.Printf("debug, client-%d.write: splice: n=%d, werr=%v", cli.id, n, werr)
		return werr != syscall.EINTR && werr != syscall.EAGAIN
	})
	if err == nil {
		err = werr
	}
	return err
}

func (cli *client) closePipe(r *os.File, w *os.File) {
	if err := r.Close(); err != nil {
		log.Printf("error, client-%d: close pipe reader: %v", cli.id, err)
	}
	if err := w.Close(); err != nil {
		log.Printf("error, client-%d: close pipe writer: %v", cli.id, err)
	}
}
