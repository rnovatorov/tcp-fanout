package tcpfanout

import (
	"fmt"
	"net"
	"time"
)

type upstream struct {
	conn        net.Conn
	fanout      *Fanout
	stopping    chan struct{}
	bufsize     uint
	readTimeout time.Duration
}

func (ups *upstream) run() error {
	bufs := ups.newBufs()
	for i := 0; ; i = (i + 1) % len(bufs) {
		buf := bufs[i]
		n, readErr := ups.read(buf)
		if pubErr := ups.fanout.pub(buf[:n]); pubErr != nil {
			return fmt.Errorf("pub: %v", pubErr)
		}
		if readErr != nil {
			return fmt.Errorf("read: %v", readErr)
		}
	}
}

func (ups *upstream) newBufs() [2][]byte {
	var bufs [2][]byte
	for i := range bufs {
		bufs[i] = make([]byte, ups.bufsize)
	}
	return bufs
}

func (ups *upstream) read(buf []byte) (int, error) {
	ups.setReadDeadline()
	for {
		n, err := ups.conn.Read(buf[:])
		if n == 0 {
			if err == nil {
				continue
			}
			return 0, err
		}
		return n, err
	}
}

func (ups *upstream) setReadDeadline() {
	deadline := time.Now().Add(ups.readTimeout)
	ups.conn.SetReadDeadline(deadline)
}
