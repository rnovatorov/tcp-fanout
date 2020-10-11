package upstream

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/errs"
	"github.com/rnovatorov/tcpfanout/pkg/lognet"
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
			return err
		}
		cli.handle(conn)
		conn.Close()
	}
}

func (cli *Client) connect() (net.Conn, error) {
	var conn net.Conn
	var lastErr error
	var i uint
	for i = 0; i < cli.ConnectRetries; i++ {
		conn, lastErr = cli.dial()
		if lastErr != nil {
			if !cli.shouldRetry(lastErr) {
				return nil, lastErr
			}
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
		return conn, nil
	}
	return nil, lastErr
}

func (cli *Client) dial() (net.Conn, error) {
	var conn net.Conn
	var err error
	ctx, cancel := context.WithCancel(context.TODO())
	done := make(chan struct{})
	go func() {
		var d lognet.Dialer
		conn, err = d.DialContext(ctx, "tcp", cli.ConnectAddr)
		close(done)
	}()
	select {
	case <-done:
		cancel()
	case <-cli.stopping:
		cancel()
		<-done
	}
	return conn, err
}

func (cli *Client) shouldRetry(err error) bool {
	if neterr, ok := err.(net.Error); ok && neterr.Temporary() {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	return false
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
