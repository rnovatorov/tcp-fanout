package upstream

import (
	"fmt"
	"net"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/streaming"
)

type session struct {
	conn        net.Conn
	fanout      *streaming.Fanout
	bufsize     uint
	readTimeout time.Duration
}

func (s *session) run() error {
	bufs := s.newBufs()
	for i := 0; ; i = (i + 1) % len(bufs) {
		buf := bufs[i]
		n, readErr := s.read(buf)
		if pubErr := s.fanout.Pub(buf[:n]); pubErr != nil {
			return fmt.Errorf("pub: %v", pubErr)
		}
		if readErr != nil {
			return fmt.Errorf("read: %v", readErr)
		}
	}
}

func (s *session) newBufs() [2][]byte {
	var bufs [2][]byte
	for i := range bufs {
		bufs[i] = make([]byte, s.bufsize)
	}
	return bufs
}

func (s *session) read(buf []byte) (int, error) {
	s.setReadDeadline()
	for {
		n, err := s.conn.Read(buf[:])
		if n == 0 {
			if err == nil {
				continue
			}
			return 0, err
		}
		return n, err
	}
}

func (s *session) setReadDeadline() {
	deadline := time.Now().Add(s.readTimeout)
	s.conn.SetReadDeadline(deadline)
}
