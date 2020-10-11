package main

import (
	"flag"
	"log"
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

func run(args parsedArgs) error {
	cfg := tcpfanout.Config{
		ConnectAddr:    *args.connect,
		ConnectRetries: *args.retries,
		ConnectIdle:    *args.idle,
		ListenAddr:     *args.listen,
		PprofAddr:      *args.pprof,
		Bufsize:        *args.bufsize,
		ReadTimeout:    *args.rtimeout,
		WriteTimeout:   *args.wtimeout,
	}
	tf, err, errs := tcpfanout.Start(cfg)
	if err != nil {
		return err
	}
	defer tf.Stop()
	return <-errs
}

type parsedArgs struct {
	connect  *string
	retries  *uint
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
		retries:  flag.Uint("retries", 8, "how many times to retry to connect"),
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
