package tcpfanout

import (
	"net"
	"time"
)

type client struct {
	id           int
	conn         net.Conn
	fanout       *Fanout
	stopping     chan struct{}
	writeTimeout time.Duration
}

func (cli *client) run() error {
	sub := cli.fanout.sub(cli.id)
	defer cli.fanout.unsub(cli.id)
	defer close(sub.done)
	for {
		select {
		case <-cli.stopping:
			return nil
		case msg := <-sub.stream:
			if err := cli.write(msg); err != nil {
				return err
			}
		}
	}
}

func (cli *client) write(msg message) error {
	cli.setWriteDeadline()
	for len(msg) > 0 {
		n, err := cli.conn.Write(msg)
		if err != nil {
			return err
		}
		msg = msg[n:]
	}
	return nil
}

func (cli *client) setWriteDeadline() {
	deadline := time.Now().Add(cli.writeTimeout)
	cli.conn.SetWriteDeadline(deadline)
}
