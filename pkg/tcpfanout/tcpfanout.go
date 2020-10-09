package tcpfanout

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/rnovatorov/tcpfanout/pkg/downstream"
	"github.com/rnovatorov/tcpfanout/pkg/streaming"
	"github.com/rnovatorov/tcpfanout/pkg/upstream"
)

type TCPFanout struct {
	cfg     Config
	stopped chan struct{}

	once     sync.Once
	stopping chan struct{}
}

func Start(cfg Config) (*TCPFanout, <-chan error) {
	tf := &TCPFanout{
		cfg:      cfg,
		stopped:  make(chan struct{}),
		stopping: make(chan struct{}),
	}
	errs := make(chan error, 1)
	go func() {
		defer close(tf.stopped)
		if err := tf.run(); err != nil {
			errs <- err
		}
	}()
	return tf, errs
}

func (tf *TCPFanout) Stop() {
	tf.once.Do(func() { close(tf.stopping) })
	<-tf.stopped
}

func (tf *TCPFanout) run() error {
	perr := startPprof(tf.cfg.PprofAddr)

	fanout := streaming.NewFanout()

	client, cerr := upstream.StartClient(upstream.ClientParams{
		ConnectAddr:    tf.cfg.ConnectAddr,
		ConnectRetries: tf.cfg.ConnectRetries,
		ConnectIdle:    tf.cfg.ConnectIdle,
		Fanout:         fanout,
		Bufsize:        tf.cfg.Bufsize,
		ReadTimeout:    tf.cfg.ReadTimeout,
	})
	defer client.Stop()

	server, serr := downstream.StartServer(downstream.ServerParams{
		ListenAddr:   tf.cfg.ListenAddr,
		Fanout:       fanout,
		WriteTimeout: tf.cfg.WriteTimeout,
	})
	defer server.Stop()

	select {
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
	errs := make(chan error, 1)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			errs <- err
		}
	}()
	return errs
}
