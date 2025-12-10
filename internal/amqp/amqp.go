package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqplib "github.com/Azure/go-amqp"
	"inwsoft.com/queuerator/internal/config"
)

type amqpConfig struct {
	Url      string               `json:"url"`
	Criteria config.CriteriaGroup `json:"criteria"`
}

type amqpDataSource struct {
	config amqpConfig
}

var _ config.DataSource = amqpDataSource{}

func New(a json.RawMessage) (config.DataSource, error) {
	src := amqpDataSource{}
	err := json.Unmarshal(a, &src.config)
	if err != nil {
		return nil, err
	}

	return src, nil
}

func (src amqpDataSource) Connect(ctx context.Context, msgChan chan json.RawMessage) error {
	conn, err := amqplib.Dial(ctx, src.config.Url, nil)
	if err != nil {
		return err
	}

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return err
	}

	receiver, err := session.NewReceiver(ctx, "test_queue", nil)
	if err != nil {
		return err
	}

	for {
		receiveCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		msg, err := receiver.Receive(receiveCtx, nil)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nClosing normally...")
				receiver.Close(ctx)
				session.Close(ctx)
				err = conn.Close()
				if err != nil {
					fmt.Println(err.Error())
				}
				return nil
			} else {
				// TODO: logging
				continue
			}
		}

		ok, err := src.handleMessage(msg)
		if err != nil {
			receiver.RejectMessage(ctx, msg, nil)
		} else {
			_ = receiver.AcceptMessage(ctx, msg)
			if ok {
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
