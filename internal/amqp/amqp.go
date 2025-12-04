package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqplib "github.com/Azure/go-amqp"
	"inwsoft.com/queuerator/internal/config"
)

type AMQPDataSourceConfig struct {
	Url      string               `json:"url"`
	Criteria config.CriteriaGroup `json:"criteria"`
}

var _ config.DataSourceConfig = (*AMQPDataSourceConfig)(nil)

func (c AMQPDataSourceConfig) CreateDataSource() (config.DataSource, error) {
	return amqpDataSource{
		url:      c.Url,
		criteria: c.Criteria,
	}, nil
}

type amqpDataSource struct {
	url      string
	criteria config.CriteriaGroup
}

var _ config.DataSource = amqpDataSource{}

func (src amqpDataSource) Connect(ctx context.Context) error {
	conn, err := amqplib.Dial(ctx, src.url, nil)
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
				continue
			}
		}

		err = src.handleMessage(msg)
		if err != nil {
			receiver.RejectMessage(ctx, msg, nil)
		} else {
			_ = receiver.AcceptMessage(ctx, msg)
		}
	}
}

func (src amqpDataSource) handleMessage(msg *amqplib.Message) error {
	if msg == nil {
		return nil
	}

	data := msg.GetData()
	if data == nil {
		return fmt.Errorf("no data")
	}

	var rawJson any
	json.Unmarshal(data, &rawJson)
	if rawJson == nil {
		return fmt.Errorf("could not read data")
	}

	jsonData := rawJson.(map[string]any)
	res := src.criteria.Evaluate(jsonData)
	fmt.Printf("Result: %t\n", res)
	return nil
}
