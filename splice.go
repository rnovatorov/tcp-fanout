package main

import "syscall"

const (
	spliceNonBlock = 0x2
	spliceMaxSize  = 4 << 20
	spliceRemain   = 1 << 62
)

func tee(out int, in int, max int, flags int) (int, error) {
	n, err := syscall.Tee(in, out, max, flags)
	return int(n), err
}

func splice(out int, in int, max int, flags int) (int, error) {
	n, err := syscall.Splice(in, nil, out, nil, max, flags)
	return int(n), err
}
