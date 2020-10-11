package lognet

import (
	"context"
	"log"
	"net"
)

type Dialer struct {
	net.Dialer
}

func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		log.Printf("error, net: %v", err)
	} else {
		log.Printf("info, net: connect %v->%v", conn.LocalAddr(), conn.RemoteAddr())
	}
	return Conn{conn}, err
}
