package e2e

import (
	"log"
	"testing"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/errs"
	"github.com/rnovatorov/tcpfanout/pkg/tcpfanout"
)

func TestTCPFanout(t *testing.T) {
	if err := testTCPFanout(t); err != nil {
		log.Printf("error, test failed: %v", err)
		t.Fail()
	}
}

func testTCPFanout(t *testing.T) error {
	data, checksum := generateBytes(65536)

	srv, err := startServer(serverParams{
		data:                data,
		bufsize:             8192,
		acceptTimeout:       10 * time.Second,
		waitWritableTimeout: 10 * time.Second,
		writeTimeout:        10 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := srv.stop(); err != nil {
			log.Printf("error, test: stop server: %v", err)
			t.Fail()
		}
	}()

	tf, err, tferrc := tcpfanout.Start(tcpfanout.Config{
		ConnectAddr:    srv.addr.String(),
		ConnectRetries: 60,
		ConnectIdle:    1 * time.Second,
		ListenAddr:     "127.0.0.1:0",
		Bufsize:        2048,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() {
		tf.Stop()
		select {
		case err := <-tferrc:
			if err != errs.Stopping {
				log.Printf("error, test: tcpfanout: %v", err)
				t.Fail()
			}
		default:
		}
	}()

	var clients clientStore
	defer func() {
		if err := clients.join(); err != nil {
			log.Printf("error, test: join clients: %v", err)
			t.Fail()
		}
	}()
	const nclients = 8
	for i := 0; i < nclients; i++ {
		err := clients.add(clientParams{
			addr:        tf.ServerAddr().String(),
			datalen:     len(data),
			checksum:    checksum,
			bufsize:     1024,
			dialTimeout: 10 * time.Second,
			readTimeout: 10 * time.Second,
		})
		if err != nil {
			return err
		}
	}

	// FIXME: Remove sleep.
	// Waiting for accepted downstream client connections to subscribe to
	// streaming fanout.
	time.Sleep(time.Second)
	close(srv.writable)

	return nil
}
