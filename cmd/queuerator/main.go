package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"inwsoft.com/queuerator/internal/amqp"
	"inwsoft.com/queuerator/internal/config"
	"inwsoft.com/queuerator/internal/mqtt"
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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	amqpRegex := regexp.MustCompile("(?i)^amqp(?:s{0,1})*://")
	mqttRegex := regexp.MustCompile("(?i)^mqtt(?:s{0,1})*://")
	for _, msg := range arr {
		var base baseConfig
		err := json.Unmarshal(msg, &base)
		if err != nil {
			panic(err)
		}

		base.Protocol = strings.ToLower(strings.TrimSpace(base.Protocol))
		isAmqp := base.Protocol == "amqp" || base.Protocol == "" && amqpRegex.MatchString(base.Url)
		if isAmqp {
			logger.Debug("found amqp connection in config", "url", base.Url)
			amqpConf, err := amqp.New(msg, logger)
			if err != nil {
				panic(err)
			}

			conf = append(conf, amqpConf)
		}

		isMqtt := base.Protocol == "mqtt" || base.Protocol == "" && mqttRegex.MatchString(base.Url)
		if isMqtt {
			logger.Debug("found mqtt connection in config", "url", base.Url)
			mqttConf, err := mqtt.New(msg)
			if err != nil {
				panic(err)
			}

			conf = append(conf, mqttConf)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	msg := make(chan json.RawMessage)
	for _, ds := range conf {
		go ds.Connect(ctx, msg)
	}

main:
	for {
		select {
		case <-exit:
			cancel()
			time.Sleep(1000 * time.Millisecond)
			break main
		case m := <-msg:
			_ = m // TODO: forward this somewhere
			// fmt.Printf("Curation yielded message: %s\n", m)
		}
	}
}
