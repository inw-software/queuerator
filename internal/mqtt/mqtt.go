package mqtt

import (
	"context"
	"encoding/json"
	"log"

	"inwsoft.com/queuerator/internal/config"

	mqttlib3 "github.com/eclipse/paho.mqtt.golang"
)

type mqtt3Config struct {
	Url      string               `json:"url"`
	Criteria config.CriteriaGroup `json:"criteria"`
	ClientId string               `json:"clientId"`
	Topics   []string             `json:"topics"`
}

type mqtt3DataSource struct {
	config mqtt3Config
}

var _ config.DataSource = mqtt3DataSource{}

func New(a json.RawMessage) (config.DataSource, error) {
	src := mqtt3DataSource{}
	err := json.Unmarshal(a, &src.config)
	if err != nil {
		return nil, err
	}

	return src, nil
}

func (src mqtt3DataSource) Connect(ctx context.Context, msgChan chan json.RawMessage) error {
	l := log.Default()
	l.SetFlags(log.LstdFlags | log.Lmicroseconds)
	opts := mqttlib3.NewClientOptions()
	opts = opts.AddBroker(src.config.Url)
	opts = opts.SetClientID(src.config.ClientId)

	opts.SetConnectionNotificationHandler(func(c mqttlib3.Client, notification mqttlib3.ConnectionNotification) {
		switch n := notification.(type) {
		case mqttlib3.ConnectionNotificationConnected:
			l.Printf("[NOTIFICATION] connected\n")
			for _, topic := range src.config.Topics {
				token := c.Subscribe(topic, 0, nil)
				token.Wait()
				if token.Error() != nil {
					l.Printf("failed to subscribe to topic \"%s\"\n", topic)
					panic(token.Error())
				} else {
					l.Printf("subscribed to topic \"%s\"", topic)
				}
			}

		case mqttlib3.ConnectionNotificationConnecting:
			l.Printf("[NOTIFICATION] connecting (isReconnect=%t) [%d]\n", n.IsReconnect, n.Attempt)
		case mqttlib3.ConnectionNotificationFailed:
			l.Printf("[NOTIFICATION] connection failed: %v\n", n.Reason)
		case mqttlib3.ConnectionNotificationLost:
			l.Printf("[NOTIFICATION] connection lost: %v\n", n.Reason)
		case mqttlib3.ConnectionNotificationBroker:
			l.Printf("[NOTIFICATION] broker connection: %s\n", n.Broker.String())
		case mqttlib3.ConnectionNotificationBrokerFailed:
			l.Printf("[NOTIFICATION] broker connection failed: %v [%s]\n", n.Reason, n.Broker.String())
		}
	})

	queue := make(chan []byte)
	opts.SetDefaultPublishHandler(func(c mqttlib3.Client, msg mqttlib3.Message) {
		queue <- msg.Payload()
	})

	c := mqttlib3.NewClient(opts)
	token := c.Connect()
	token.Wait()
	if token.Error() != nil {
		panic(token.Error())
	}

	for {
		select {
		case <-ctx.Done():
			c.Disconnect(250)

		case data := <-queue:
			var jsonData map[string]any
			err := json.Unmarshal(data, &jsonData)
			if err != nil {
				continue
			}

			res := src.config.Criteria.Evaluate(jsonData)
			if res {
				msgChan <- data
			}
		}
	}
}
