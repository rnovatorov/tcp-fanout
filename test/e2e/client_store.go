package e2e

type clientStore []*client

func (s *clientStore) add(p clientParams) error {
	cli, err := startClient(p)
	if err != nil {
		return err
	}
	*s = append(*s, cli)
	return nil
}

func (s *clientStore) join() error {
	var lastErr error
	for _, cli := range *s {
		if err := cli.join(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
