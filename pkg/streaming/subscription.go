package streaming

type Subscription struct {
	Stream Stream
	Done   chan struct{}
}

func NewSubscription() Subscription {
	return Subscription{
		Stream: make(Stream),
		Done:   make(chan struct{}),
	}
}
