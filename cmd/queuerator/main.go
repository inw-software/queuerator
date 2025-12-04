package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"inwsoft.com/queuerator/internal/amqp"
	"inwsoft.com/queuerator/internal/config"
)

type baseConfig struct {
	Url      string `json:"url"`
	Protocol string `json:"protocol,omitempty"`
}

func main() {
	confPathPointer := flag.String("config", "", "Path to the config file (e.g. ./config.json)")
	flag.Parse()

	if confPathPointer == nil || *confPathPointer == "" {
		panic("missing config flag")
	}

	confData, err := os.ReadFile(*confPathPointer)
	if err != nil {
		panic(err)
	}

	var arr []json.RawMessage
	err = json.Unmarshal(confData, &arr)
	if err != nil {
		panic(err)
	}

	conf := make(config.Config, 0)

	amqpRegex := regexp.MustCompile("(?i)^amqp(?:s{0,1})*://")
	for _, a := range arr {
		var base baseConfig
		err := json.Unmarshal(a, &base)
		if err != nil {
			panic(err)
		}

		base.Protocol = strings.ToLower(strings.TrimSpace(base.Protocol))
		isAmqp := base.Protocol == "amqp" || base.Protocol == "" && amqpRegex.MatchString(base.Url)
		if isAmqp {
			var amqpConf amqp.AMQPDataSourceConfig
			raw, err := json.Marshal(a)
			if err != nil {
				panic(err)
			}

			err = json.Unmarshal(raw, &amqpConf)
			if err != nil {
				panic(err)
			}

			conf = append(conf, amqpConf)
		}
	}

	var s string
	if len(conf) == 1 {
		s = ""
	} else {
		s = "s"
	}

	fmt.Printf("Resolved %d data source%s.\n", len(conf), s)
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	for _, a := range conf {
		ds, err := a.CreateDataSource()
		if err != nil {
			fmt.Print(err)
		}

		go ds.Connect(ctx)
	}

	<-c
	cancel()
	time.Sleep(1000 * time.Millisecond)
}
