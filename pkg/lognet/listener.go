package lognet

import (
	"log"
	"net"
)

func Listen(network, address string) (net.Listener, error) {
	lsn, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	log.Printf("info, net: listen on %v", lsn.Addr())
	return Listener{lsn}, nil
}

type Listener struct {
	net.Listener
}

func (l Listener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	log.Printf("info, net: accept %v->%v", conn.LocalAddr(), conn.RemoteAddr())
	return Conn{conn}, nil
}

func (l Listener) Close() error {
	err := l.Listener.Close()
	if err != nil {
		log.Printf("error, net: stop listening on %v: %v", l.Addr(), err)
	} else {
		log.Printf("info, net: stop listening on %v", l.Addr())
	}
	return err
}
