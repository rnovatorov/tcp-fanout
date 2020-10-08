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
		connectAddr:    flag.String("connect", "", "address to connect to"),
		connectRetries: flag.Int("retries", 8, "how many times to retry to connect"),
		connectIdle:    flag.Duration("interval", time.Second, "interval between connect retries"),
		listenAddr:     flag.String("listen", "", "address to listen to"),
		pprofAddr:      flag.String("pprof", "", "pprof address"),
	}
	flag.Parse()
	return args
}

func run(args parsedArgs) error {
	if *args.pprofAddr != "" {
		go func() {
			log.Println(http.ListenAndServe(*args.pprofAddr, nil))
		}()
	}

	fnt := newFanout(*args.connectAddr, *args.connectRetries, *args.connectIdle)
	ferr := fnt.start()
	defer fnt.stop()

	srv := newServer(*args.listenAddr, fnt)
	serr := srv.start()
	defer srv.stop()

	select {
	case err := <-ferr:
		return fmt.Errorf("fanout: %v", err)
	case err := <-serr:
		return fmt.Errorf("server: %v", err)
	}
}
