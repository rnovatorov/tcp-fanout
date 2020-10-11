package lognet

import (
	"log"
	"net"
)

type Conn struct {
	net.Conn
}

func (c Conn) Close() error {
	la, ra := c.Conn.LocalAddr(), c.Conn.RemoteAddr()
	err := c.Conn.Close()
	if err != nil {
		log.Printf("error, net: close %v->%v: %v", la, ra, err)
	} else {
		log.Printf("info, net: close %v->%v", la, ra)
	}
	return err
}
