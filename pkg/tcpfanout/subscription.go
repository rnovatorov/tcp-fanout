package tcpfanout

type message []byte

type subscription struct {
	stream chan message
	done   chan struct{}
}
