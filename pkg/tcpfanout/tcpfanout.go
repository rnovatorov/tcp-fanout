package tcpfanout

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/rnovatorov/tcpfanout/pkg/downstream"
	"github.com/rnovatorov/tcpfanout/pkg/errs"
	"github.com/rnovatorov/tcpfanout/pkg/streaming"
	"github.com/rnovatorov/tcpfanout/pkg/upstream"
)

type TCPFanout struct {
	cfg      Config
	fanout   *streaming.Fanout
	started  chan struct{}
	stopped  chan struct{}
	once     sync.Once
	stopping chan struct{}
}

func Start(cfg Config) (*TCPFanout, error, <-chan error) {
	tf := &TCPFanout{
		cfg:      cfg,
		fanout:   streaming.NewFanout(),
		started:  make(chan struct{}),
		stopped:  make(chan struct{}),
		stopping: make(chan struct{}),
	}
	errc := make(chan error, 1)
	go func() {
		defer close(tf.stopped)
		if err := tf.run(); err != nil {
			errc <- err
		}
	}()
	select {
	case <-tf.started:
		return tf, nil, errc
	case err := <-errc:
		return nil, err, nil
	}
}

func (tf *TCPFanout) Stop() {
	tf.once.Do(func() { close(tf.stopping) })
	<-tf.stopped
}

func (tf *TCPFanout) run() error {
	perr := startPprof(tf.cfg.PprofAddr)

	client, cerr := upstream.StartClient(upstream.ClientParams{
		ConnectAddr:    tf.cfg.ConnectAddr,
		ConnectRetries: tf.cfg.ConnectRetries,
		ConnectIdle:    tf.cfg.ConnectIdle,
		Fanout:         tf.fanout,
		Bufsize:        tf.cfg.Bufsize,
		ReadTimeout:    tf.cfg.ReadTimeout,
	})
	defer client.Stop()

	server, err, serr := downstream.StartServer(downstream.ServerParams{
		ListenAddr:   tf.cfg.ListenAddr,
		Fanout:       tf.fanout,
		WriteTimeout: tf.cfg.WriteTimeout,
	})
	if err != nil {
		return err
	}
	defer server.Stop()

	close(tf.started)

	select {
	case <-tf.stopping:
		return errs.Stopping
	case err := <-perr:
		return fmt.Errorf("pprof: %v", err)
	case err := <-cerr:
		return fmt.Errorf("client: %v", err)
	case err := <-serr:
		return fmt.Errorf("server: %v", err)
	}
}

func startPprof(addr string) <-chan error {
	if addr == "" {
		return nil
	}
	errc := make(chan error, 1)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			errc <- err
		}
	}()
	return errc
}
