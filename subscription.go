package main

type message struct {
	fd int
	n  int
}

type subscription struct {
	stream chan message
	done   chan struct{}
}
