package main

import (
	"net"
	"time"
)

type client struct {
	id      int
	conn    net.Conn
	fnt     *fanout
	stp     chan struct{}
	timeout time.Duration
}

func (cli *client) run() error {
	sub := cli.fnt.sub(cli.id)
	defer cli.fnt.unsub(cli.id)
	defer close(sub.done)
	for {
		select {
		case <-cli.stp:
			return nil
		case msg := <-sub.stream:
			if err := cli.write(msg); err != nil {
				return err
			}
		}
	}
}

func (cli *client) write(msg message) error {
	cli.setWriteTimeout()
	for len(msg) > 0 {
		n, err := cli.conn.Write(msg)
		if err != nil {
			return err
		}
		msg = msg[n:]
	}
	return nil
}

func (cli *client) setWriteTimeout() {
	deadline := time.Now().Add(cli.timeout)
	cli.conn.SetWriteDeadline(deadline)
}
