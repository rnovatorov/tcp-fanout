package main

import (
	"log"
	"syscall"
)

const (
	spliceNonBlock = 0x2
	spliceMaxSize  = 4 << 20
	spliceRemain   = 1 << 62
)

func tee(out int, in int, max int, flags int) (int, error) {
	n, err := syscall.Tee(in, out, max, flags)
	if err != nil && n > 0 {
		log.Panicf("tee: n=%d, err=%v", n, err)
	}
	return int(n), err
}

func splice(out int, in int, max int, flags int) (int, error) {
	n, err := syscall.Splice(in, nil, out, nil, max, flags)
	if err != nil && n > 0 {
		log.Panicf("splice: n=%d, err=%v", n, err)
	}
	return int(n), err
}
