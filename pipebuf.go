package main

import (
	"fmt"
	"syscall"
)

type pipebuf [][2]int

func newPipebuf(n int, nonBlock bool) (pipebuf, error) {
	buf := pipebuf{}
	flags := syscall.O_CLOEXEC
	if nonBlock {
		flags |= syscall.O_NONBLOCK
	}
	for i := 0; i < n; i++ {
		if err := buf.extend(flags); err != nil {
			return nil, fmt.Errorf("extend: %v", err)
		}
	}
	return buf, nil
}

func (buf *pipebuf) extend(flags int) error {
	var pipe [2]int
	if err := syscall.Pipe2(pipe[:], flags); err != nil {
		return fmt.Errorf("pipe2: %v", err)
	}
	*buf = append(*buf, pipe)
	return nil
}

func (buf *pipebuf) close() error {
	var lastErr error
	for _, pipe := range *buf {
		for _, fd := range pipe {
			if err := syscall.Close(fd); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}
