package tcpfanout

import (
	"time"
)

type Config struct {
	ConnectAddr    string
	ConnectRetries uint
	ConnectIdle    time.Duration
	ListenAddr     string
	PprofAddr      string
	Bufsize        uint
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}
