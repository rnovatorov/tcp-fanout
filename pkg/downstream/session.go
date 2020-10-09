package downstream

import (
	"net"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/streaming"
)

type session struct {
	id           int
	conn         net.Conn
	fanout       *streaming.Fanout
	writeTimeout time.Duration
	stopping     chan struct{}
}

func (s *session) run() error {
	sub := s.fanout.Sub(s.id)
	defer s.fanout.Unsub(s.id)
	defer close(sub.Done)
	for {
		select {
		case <-s.stopping:
			return nil
		case msg := <-sub.Stream:
			if err := s.write(msg); err != nil {
				return err
			}
		}
	}
}

func (s *session) write(msg streaming.Message) error {
	s.setWriteDeadline()
	for len(msg) > 0 {
		n, err := s.conn.Write(msg)
		if err != nil {
			return err
		}
		msg = msg[n:]
	}
	return nil
}

func (s *session) setWriteDeadline() {
	deadline := time.Now().Add(s.writeTimeout)
	s.conn.SetWriteDeadline(deadline)
}
