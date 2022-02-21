//
// main.go
// Copyright (C) 2018 YanMing <yming0221@gmail.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"flag"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sharedis/config"
	"sharedis/server"
	"syscall"
	"time"

	"github.com/pingcap/log"
)

var (
	backend  string
	conf     string
	loglevel string
	logPath string
)

func init() {
	flag.StringVar(&backend, "backend", "", "tikv storage backend address")
	flag.StringVar(&conf, "conf", "", "config file")
	flag.StringVar(&loglevel, "loglevel", "info", "loglevel output, format:info/debug/warn")
}

func initTikvLog(c *config.Config) {
	tc := &log.Config{
		Level: c.Sharedis.LogLevel,
		File: log.FileLogConfig{
			Filename: c.Sharedis.LogPath,
			MaxSize: c.Sharedis.LogFileSizeMB,
		},
	}
	lg, r, err := log.InitLogger(tc)
	if nil != err {
		log.Fatal("init tikv log file failed", zap.Error(err))
	}
	log.ReplaceGlobals(lg, r)
}

func main() {
	flag.Parse()

	log.Info("server started")

	var (
		c   *config.Config
		err error
	)

	if conf != "" {
		c, err = config.LoadConfig(conf)
		if err != nil {
			return
		}
	} else {
		if c == nil && backend == "" {
			log.Fatal("backend argument must be assign")
		}
	}
	c = config.NewConfig(c, backend)

	config.FillWithDefaultConfig(c)

	initTikvLog(c)

	app := server.NewApp(c)

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go app.Run()

	s := <-quitCh
	log.Info("Program Exit... " + s.String())
	app.Close()
	time.Sleep(time.Second * 10)
}
