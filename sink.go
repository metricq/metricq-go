package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Sink struct {
	*Agent
	connection *Connection
	channel    *amqp.Channel
}

type SinkSubscribeResponse struct {
	DataServerAddress string `json:"dataServerAddress"`
	DataQueue         string `json:"dataQueue"`
	// TODO metadata
}

func NewSink(token, server string) (*Sink, error) {
	agent, err := NewAgent(token, server)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent: %w", err)
	}
	return &Sink{Agent: agent}, nil
}

func (resp *SinkSubscribeResponse) parseDataServer(server *url.URL) (*url.URL, error) {
	dataServerAddress, err := url.Parse(resp.DataServerAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dataServerAddress: %w", err)
	}

	if dataServerAddress.Scheme == "vhost" {
		dataServerAddress.Scheme = server.Scheme
		dataServerAddress.Host = server.Host
	}

    if server.User != nil && dataServerAddress.User == nil {
        dataServerAddress.User = server.User
    }

	return dataServerAddress, nil
}

func (sink *Sink) Subscribe(ctx context.Context, metrics []string, expires time.Duration) (<-chan amqp.Delivery, error) {
	response, err := sink.Rpc(ctx, "metricq.management", "sink.subscribe", RpcMessage{"sink.subscribe"})
	if err != nil {
		return nil, fmt.Errorf("failed to send RPC: %w", err)
	}

	log.Printf("Received RPC response: %s", response)

	data := new(SinkSubscribeResponse)
	if err = json.Unmarshal(response, data); err != nil {
		return nil, fmt.Errorf("failed to parse RPC response: %w", err)
	}

	sink.connection = new(Connection)
	dataServer, err := data.parseDataServer(sink.Server)
	if err != nil {
		return nil, err
	}
	err = sink.connection.Connect(dataServer, fmt.Sprintf("data connection %s", sink.token))
	if err != nil {
		return nil, fmt.Errorf("failed to connect data connection: %w", err)
	}

	return nil, nil
}

func (sink *Sink) Close() error {
	if sink.channel != nil {
		return sink.channel.Close()
	}

	sink.connection.Close()

	return nil
}
