package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func main() {
	args := parseArgs()
	if err := run(args); err != nil {
		log.Fatalf("fatal, %v", err)
	}
}

type parsedArgs struct {
	connectAddr    *string
	connectRetries *int
	connectIdle    *time.Duration
	listenAddr     *string
	pprofAddr      *string
}

func parseArgs() parsedArgs {
	args := parsedArgs{
		connectAddr:    flag.String("connect-addr", "", "address to connect to"),
		connectRetries: flag.Int("connect-retries", 8, "how many times to retry to connect"),
		connectIdle:    flag.Duration("connect-idle", time.Second, "interval between connect retries"),
		listenAddr:     flag.String("listen-addr", "", "address to listen to"),
		pprofAddr:      flag.String("pprof-addr", "", "pprof address"),
	}
	flag.Parse()
	return args
}

func run(args parsedArgs) error {
	perr := startPprof(*args.pprofAddr)

	fnt := newFanout(*args.connectAddr, *args.connectRetries, *args.connectIdle)
	ferr := fnt.start()
	defer fnt.stop()

	srv := newServer(*args.listenAddr, fnt)
	serr := srv.start()
	defer srv.stop()

	select {
	case err := <-perr:
		return fmt.Errorf("pprof: %v", err)
	case err := <-ferr:
		return fmt.Errorf("fanout: %v", err)
	case err := <-serr:
		return fmt.Errorf("server: %v", err)
	}
}

func startPprof(addr string) <-chan error {
	if addr == "" {
		return nil
	}
	errs := make(chan error, 1)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			errs <- err
		}
	}()
	return errs
}
