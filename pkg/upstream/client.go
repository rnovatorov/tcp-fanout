package upstream

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/errs"
	"github.com/rnovatorov/tcpfanout/pkg/streaming"
)

type ClientParams struct {
	ConnectAddr    string
	ConnectRetries uint
	ConnectIdle    time.Duration
	Fanout         *streaming.Fanout
	Bufsize        uint
	ReadTimeout    time.Duration
}

type Client struct {
	ClientParams
	stopped  chan struct{}
	once     sync.Once
	stopping chan struct{}
}

func StartClient(params ClientParams) (*Client, <-chan error) {
	cli := &Client{
		ClientParams: params,
		stopped:      make(chan struct{}),
		stopping:     make(chan struct{}),
	}
	errc := make(chan error, 1)
	go func() {
		defer close(cli.stopped)
		if err := cli.run(); err != nil {
			errc <- err
		}
	}()
	return cli, errc
}

func (cli *Client) Stop() {
	cli.once.Do(func() { close(cli.stopping) })
	<-cli.stopped
}

func (cli *Client) run() error {
	for {
		conn, err := cli.connect()
		if err != nil {
			return fmt.Errorf("connect: %v", err)
		}
		cli.handle(conn)
		closeConn(conn)
	}
}

func (cli *Client) connect() (net.Conn, error) {
	var conn net.Conn
	var lastErr error
	var i uint
	for i = 0; i < cli.ConnectRetries; i++ {
		// FIXME: Dial with context.
		conn, lastErr = net.Dial("tcp", cli.ConnectAddr)
		if lastErr != nil {
			log.Printf("error, %v", lastErr)
			select {
			case <-time.After(cli.ConnectIdle):
				continue
			case <-cli.stopping:
				if lastErr != nil {
					return nil, lastErr
				}
				return nil, errs.Stopping
			}
		}
		log.Printf("info, connect %v->%v", conn.LocalAddr(), conn.RemoteAddr())
		return conn, nil
	}
	return nil, lastErr
}

func (cli *Client) handle(conn net.Conn) {
	s := &session{
		conn:        conn,
		fanout:      cli.Fanout,
		bufsize:     cli.Bufsize,
		readTimeout: cli.ReadTimeout,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.run(); err != nil {
			log.Printf("error, upstream session: %v", err)
		} else {
			log.Printf("info, stopped upstream session")
		}
	}()
	select {
	case <-done:
	case <-cli.stopping:
	}
}

func closeConn(conn net.Conn) {
	la, ra := conn.LocalAddr(), conn.RemoteAddr()
	if err := conn.Close(); err != nil {
		log.Printf("warn, close %v->%v: %v", la, ra, err)
		return
	}
	log.Printf("info, close %v->%v", la, ra)
}
