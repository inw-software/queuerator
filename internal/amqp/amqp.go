package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqplib "github.com/Azure/go-amqp"
	"inwsoft.com/queuerator/internal/config"
)

type amqpConfig struct {
	Url      string               `json:"url"`
	Criteria config.CriteriaGroup `json:"criteria"`
	logger   *slog.Logger
}

type amqpDataSource struct {
	config amqpConfig
}

var _ config.DataSource = amqpDataSource{}

func New(a json.RawMessage, l *slog.Logger) (config.DataSource, error) {
	if l == nil {
		return nil, fmt.Errorf("logger is required")
	}

	src := amqpDataSource{}
	err := json.Unmarshal(a, &src.config)
	if err != nil {
		return nil, err
	}

	src.config.logger = l.With("src", src.config.Url)
	return src, nil
}

func (src amqpDataSource) Connect(ctx context.Context, msgChan chan json.RawMessage) error {
	src.config.logger.Debug("attempting net dial")
	conn, err := amqplib.Dial(ctx, src.config.Url, nil)
	if err != nil {
		return err
	}

	src.config.logger.Debug("net dial success")
	src.config.logger.Debug("starting new session")
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return err
	}

	src.config.logger.Debug("session started")
	queue := "test_queue"
	src.config.logger.Debug("creating receiver", "queue", queue)
	receiver, err := session.NewReceiver(ctx, "test_queue", nil)
	if err != nil {
		return err
	}

	src.config.logger.Debug("receiver created")
	for {
		receiveCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		msg, err := receiver.Receive(receiveCtx, nil)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				receiver.Close(ctx)
				session.Close(ctx)
				err = conn.Close()
				if err != nil {
					src.config.logger.Error("error while closing connection", "err", err.Error())
				}

				return nil
			} else {
				if err.Error() != "context deadline exceeded" {
					src.config.logger.Error("error while receiving new message", "err", err.Error())
				}

				continue
			}
		}

		ok, err := src.handleMessage(msg)
		if err != nil {
			src.config.logger.Error("error while handling new message", "err", err.Error())
			receiver.RejectMessage(ctx, msg, nil)
		} else {
			_ = receiver.AcceptMessage(ctx, msg)
			if ok {
				src.config.logger.Info("message satisfies criteria", "data", json.RawMessage(msg.GetData()))
				msgChan <- msg.GetData()
			}
		}
	}
}

func (src amqpDataSource) handleMessage(msg *amqplib.Message) (bool, error) {
	if msg == nil {
		return false, nil
	}

	data := msg.GetData()
	if data == nil {
		return false, fmt.Errorf("no data")
	}

	var rawJson any
	json.Unmarshal(data, &rawJson)
	if rawJson == nil {
		return false, fmt.Errorf("could not read data")
	}

	jsonData := rawJson.(map[string]any)
	res := src.config.Criteria.Evaluate(jsonData)
	return res, nil
}
