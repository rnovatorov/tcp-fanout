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
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalf("fatal, %v", err)
	}
}

var connectAddr = flag.String("connect", "", "address to connect to")
var connectRetries = flag.Int("retries", 8, "how many times to retry to connect")
var connectIdle = flag.Duration("interval", time.Second, "interval between connect retries")
var listenAddr = flag.String("listen", "", "address to listen to")
var pprofAddr = flag.String("pprof", "", "pprof address")

func run() error {
	if *pprofAddr != "" {
		go func() {
			log.Println(http.ListenAndServe(*pprofAddr, nil))
		}()
	}

	fnt := newFanout(*connectAddr, *connectRetries, *connectIdle)
	ferr := fnt.start()
	defer fnt.stop()

	srv := newServer(*listenAddr, fnt)
	serr := srv.start()
	defer srv.stop()

	select {
	case err := <-ferr:
		return fmt.Errorf("fanout: %v", err)
	case err := <-serr:
		return fmt.Errorf("server: %v", err)
	}
}
