package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/rnovatorov/tcpfanout/pkg/tcpfanout"
)

func main() {
	args := parseArgs()
	if err := run(args); err != nil {
		log.Fatalf("fatal, %v", err)
	}
}

type parsedArgs struct {
	connect  *string
	retries  *int
	idle     *time.Duration
	listen   *string
	pprof    *string
	bufsize  *uint
	rtimeout *time.Duration
	wtimeout *time.Duration
}

func parseArgs() parsedArgs {
	args := parsedArgs{
		connect:  flag.String("connect", "", "address to connect to"),
		retries:  flag.Int("retries", 8, "how many times to retry to connect"),
		idle:     flag.Duration("idle", time.Second, "interval between connect retries"),
		listen:   flag.String("listen", "", "address to listen to"),
		pprof:    flag.String("pprof", "", "address for pprof to listen to"),
		bufsize:  flag.Uint("bufsize", 1<<16, "size of upstream message buffer in bytes"),
		rtimeout: flag.Duration("rtimeout", 8*time.Second, "time before considering upstream socket dead"),
		wtimeout: flag.Duration("wtimeout", 8*time.Second, "time before considering client socket dead"),
	}
	flag.Parse()
	return args
}

func run(args parsedArgs) error {
	perr := startPprof(*args.pprof)

	fnt := tcpfanout.NewFanout(tcpfanout.FanoutParams{
		ConnectAddr:         *args.connect,
		ConnectRetries:      *args.retries,
		ConnectIdle:         *args.idle,
		UpstreamBufsize:     *args.bufsize,
		UpstreamReadTimeout: *args.rtimeout,
	})
	ferr := fnt.Start()
	defer fnt.Stop()

	srv := tcpfanout.NewServer(tcpfanout.ServerParams{
		Fanout:             fnt,
		ListenAddr:         *args.listen,
		ClientWriteTimeout: *args.wtimeout,
	})
	serr := srv.Start()
	defer srv.Stop()

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
