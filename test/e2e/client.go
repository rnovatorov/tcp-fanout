package e2e

import (
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"time"
)

type client struct {
	p       clientParams
	errc    chan error
	started chan struct{}
	stopped chan struct{}
}

type clientParams struct {
	addr        string
	datalen     int
	checksum    string
	bufsize     int
	readTimeout time.Duration
	dialTimeout time.Duration
}

func startClient(p clientParams) (*client, error) {
	cli := &client{
		p:       p,
		errc:    make(chan error, 1),
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go func() {
		defer close(cli.stopped)
		cli.errc <- cli.run()
	}()
	select {
	case err := <-cli.errc:
		return nil, err
	case <-cli.started:
		return cli, nil
	}
}

func (cli *client) join() error {
	<-cli.stopped
	return <-cli.errc
}

func (cli *client) run() error {
	conn, err := net.DialTimeout("tcp", cli.p.addr, cli.p.dialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	close(cli.started)
	return cli.handle(conn)
}

func (cli *client) handle(conn net.Conn) error {
	data, err := cli.read(conn)
	if err != nil {
		return err
	}

	if sum := sha256.Sum256(data); string(sum[:]) != cli.p.checksum {
		return fmt.Errorf("checksum mismatch: expected=%x, actual=%x", cli.p.checksum, sum)
	}
	return nil
}

func (cli *client) read(conn net.Conn) ([]byte, error) {
	data := make([]byte, 0, cli.p.datalen)
	buf := make([]byte, cli.p.bufsize)

	defer func() {
		log.Printf("len(data) = %d", len(data))
	}()

	for len(data) < cli.p.datalen {
		deadline := time.Now().Add(cli.p.readTimeout)
		conn.SetReadDeadline(deadline)

		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}
		if n == 0 && err == nil {
			continue
		}
		chunk := buf[:n]
		data = append(data, chunk...)
	}

	return data, nil
}
