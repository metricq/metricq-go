package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type MetricDataPoint struct {
	Metric    string
	Timestamp time.Time
	Value     float64
}

type Sink struct {
	*Agent
	connection      *Connection
	channel         *amqp.Channel
	dataPointNotify chan<- MetricDataPoint
	mu              sync.Mutex
	subscribed      bool
	workerCtx       context.Context
	metrics         []string
	expires         time.Duration
}

type SinkSubscribeRequest struct {
	RpcMessage
	Metrics []string `json:"metrics"`
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
	sink := &Sink{Agent: agent}
	agent.RegisterReconnectHook("sink.subscribe", sink.reconnect)
	return sink, nil
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

func (sink *Sink) Subscribe(requestCtx context.Context, workerContext context.Context, metrics []string, expires time.Duration) error {
	sink.mu.Lock()
	sink.workerCtx = workerContext
	sink.metrics = append([]string(nil), metrics...)
	sink.expires = expires
	sink.mu.Unlock()

	return sink.subscribeAndConnect(requestCtx, workerContext, metrics)
}

func (sink *Sink) subscribeAndConnect(requestCtx context.Context, workerContext context.Context, metrics []string) error {
	response, err := sink.Rpc(requestCtx, "metricq.management", "sink.subscribe", SinkSubscribeRequest{RpcMessage{"sink.subscribe"}, metrics})
	if err != nil {
		return fmt.Errorf("failed to send RPC: %w", err)
	}

	log.Printf("Received RPC response: %s", response)

	data := new(SinkSubscribeResponse)
	if err = json.Unmarshal(response, data); err != nil {
		return fmt.Errorf("failed to parse RPC response: %w", err)
	}

	sink.connection = new(Connection)
	dataServer, err := data.parseDataServer(sink.Server)
	if err != nil {
		return err
	}
	err = sink.connection.Connect(dataServer, fmt.Sprintf("data connection %s", sink.token))
	if err != nil {
		return fmt.Errorf("failed to connect data connection: %w", err)
	}

	sink.mu.Lock()
	sink.subscribed = true
	sink.mu.Unlock()

	go func(ctx context.Context, queue string) {
		if err := sink.dataConsumeLoop(ctx, queue); err != nil {
			log.Panicf("failed to consume data messages: %v", err)
		}
	}(workerContext, data.DataQueue)

	return nil
}

func (sink *Sink) reconnect(ctx context.Context) error {
	sink.mu.Lock()
	subscribed := sink.subscribed
	workerCtx := sink.workerCtx
	metrics := append([]string(nil), sink.metrics...)
	sink.mu.Unlock()
	if !subscribed {
		return nil
	}
	if workerCtx == nil || workerCtx.Err() != nil {
		return nil
	}
	return sink.subscribeAndConnect(ctx, workerCtx, metrics)
}

func (sink *Sink) dataConsumeLoop(ctx context.Context, queue string) error {
	channel, err := sink.connection.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create data channel: %w", err)
	}
	defer channel.Close()

	log.Printf("starting data consume on queue %s", queue)

	consumer, err := channel.Consume(queue, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to start consuming on data queue: %w", err)
	}

ConsumeLoop:
	for {
		select {
		case packet := <-consumer:
			if channel.IsClosed() {
				return fmt.Errorf("Data Channel closed. Stopped data consume.")
			}

			chunk := DataChunk{}

			err = proto.Unmarshal(packet.Body, &chunk)
			if err != nil {
				log.Printf("failed to unmarshal DataChunk from message: %v", err)
				continue
			}

			var previous_time int64 = 0

			for i := range chunk.Value {
				sink.dataPointNotify <- MetricDataPoint{
					packet.RoutingKey,
					time.Unix(0, chunk.TimeDelta[i]+previous_time),
					chunk.Value[i],
				}

				previous_time += chunk.TimeDelta[i]
			}

			packet.Ack(false)

		case <-ctx.Done():
			break ConsumeLoop
		}
	}

	return nil
}

func (sink *Sink) NotifyDataPoint(channel chan<- MetricDataPoint) error {
	if sink.dataPointNotify != nil {
		return fmt.Errorf("failed to register notify handler: another channel was already set")
	}

	sink.dataPointNotify = channel

	return nil
}

func (sink *Sink) Close() error {
	if sink.channel != nil {
		return sink.channel.Close()
	}

	sink.connection.Close()

	return nil
}
