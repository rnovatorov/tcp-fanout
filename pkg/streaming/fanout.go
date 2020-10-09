package streaming

import (
	"sync"
)

type Fanout struct {
	mu   sync.Mutex
	subs map[int]Subscription
}

func NewFanout() *Fanout {
	return &Fanout{
		subs: make(map[int]Subscription),
	}
}

func (fnt *Fanout) Pub(msg Message) error {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	for _, sub := range fnt.subs {
		select {
		case sub.Stream <- msg:
		case <-sub.Done:
		}
	}
	return nil
}

func (fnt *Fanout) Sub(id int) Subscription {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	sub := NewSubscription()
	fnt.subs[id] = sub
	return sub
}

func (fnt *Fanout) Unsub(id int) {
	fnt.mu.Lock()
	defer fnt.mu.Unlock()
	delete(fnt.subs, id)
}
