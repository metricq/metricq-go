package metricq

import (
	"context"
	"encoding/json"
    "net/url"
	"fmt"
	"log"
	"os"
	"time"

	uuid "github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rpcRequest struct {
	CorrelationId string
	Response      chan<- amqp.Delivery
	CleanUp       bool
}

func NewAgent(token, server string) (*Agent, error) {
    url, err := url.Parse(server)
    if err != nil {
        return nil, err
    }

	return &Agent{
		token: token, Server: url,
	}, nil
}

// Agent is the base client in a MetricQ cluster.
// It allows to send and receive RPC messages over the established connection.
// Make sure to call `Connect()` prior to `RPC()`
type Agent struct {
	token             string
	Server            *url.URL
	connection        *Connection
	rpcQueue          *amqp.Queue
	rpcRequestChannel chan<- rpcRequest
}

// RpcMessage is the base for RPC messages in MetricQ.
// It has the `function` member.
type RpcMessage struct {
	Function string `json:"function"`
}

// Connect established the management connection to the MetricQ cluster.
// This allows to use the MetricQ RPC protocol. A RPC receiver will run within
// the passed rpcCtx context; cancel this context to stop the receiver.
// It returns any errors.
func (agent *Agent) Connect(rpcCtx context.Context) error {
	if agent.token == "" {
		return fmt.Errorf("empty token is invalid")
	}

	agent.connection = new(Connection)
	err := agent.connection.Connect(agent.Server, fmt.Sprintf("management connection %s", agent.token))
	if err != nil {
		return err
	}

	queue, err := agent.connection.channel.QueueDeclare(agent.token+"-rpc",
		false, false, true, false, nil)
	if err != nil {
		return fmt.Errorf("failed to create RPC queue: %w", err)
	}
	agent.rpcQueue = &queue

	err = agent.connection.channel.QueueBind(agent.token+"-rpc", agent.token, "metricq.broadcast", false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind RPC queue to metricq.broadcast exchange: %w", err)
	}

	rpcRequestChannel := make(chan rpcRequest)
	agent.rpcRequestChannel = rpcRequestChannel

	go func(ctx context.Context, channel <-chan rpcRequest) {
		if err := agent.rpcConsumeLoop(ctx, channel); err != nil {
			log.Panicf("failed to consume RPC messages: %v", err)
		}
	}(rpcCtx, rpcRequestChannel)

	return nil
}

func (agent *Agent) rpcConsumeLoop(ctx context.Context, requests <-chan rpcRequest) error {
	channel, err := agent.connection.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create RPC channel: %w", err)
	}
	defer channel.Close()

	consumer, err := channel.Consume(agent.rpcQueue.Name, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to start consuming on rpc queue: %w", err)
	}

	log.Printf("starting RPC consume on queue %s", agent.rpcQueue.Name)

	handlers := make(map[string](rpcRequest))

ConsumeLoop:
	for {
		select {
		case packet := <-consumer:
			if channel.IsClosed() {
				return fmt.Errorf("RPC Channel closed. Stopped RPC consume.")
			}

			handler, ok := handlers[packet.CorrelationId]

			if ok {
				log.Printf("Received RPC response for %s from: %s", packet.CorrelationId, packet.AppId)

				handler.Response <- packet

				if handler.CleanUp {
					close(handler.Response)
					delete(handlers, packet.CorrelationId)
				}

				packet.Ack(false)
			} else {
				request := RpcMessage{}
				err := json.Unmarshal(packet.Body, &request)
				if err == nil {
					handler, ok := handlers[request.Function]
					if ok {
						handler.Response <- packet
						log.Printf("Received RPC request from: %s", packet.AppId)
						break
					}
				}

				log.Printf("Received unexpected RPC message (%s) from %s: %s", packet.CorrelationId, packet.AppId, packet.Body)
				packet.Nack(false, true)
			}

		case request := <-requests:
			// TODO clean up handlers after a timeout
			// TODO don't overwrite old handlers?
			handlers[request.CorrelationId] = request

		case <-ctx.Done():
			break ConsumeLoop
		}
	}

	return nil
}

// SendRpcResponse sends `payload` as response to the provided RPC request.
// `payload` will be Marshalled into JSON.
// It returns any errors.
func (agent *Agent) SendRpcResponse(ctx context.Context, rpcRequest amqp.Delivery, payload any) error {
	if agent.connection == nil {
		return fmt.Errorf("no connection established.")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	response := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: rpcRequest.CorrelationId,
		Body:          data,
		AppId:         agent.token,
	}

	return agent.connection.channel.PublishWithContext(ctx, "", rpcRequest.ReplyTo, true, false, response)
}

// HandleDiscover provides an opt-in to the default responds for discovery RPC
// requests. Run this method with `go` to opt-in.
// It will only return once the context is cancelled.
func (agent *Agent) HandleDiscover(ctx context.Context, version string) {
	start_time := time.Now()
	response_channel := make(chan amqp.Delivery)

	agent.NotifyRPC("discover", response_channel)

	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("failed to get hostname: %s", err)
		hostname = "[unknown]"
	}

HandlerLoop:
	for {
		select {
		case packet := <-response_channel:
			log.Printf("respond to discover RPC from: %s", packet.AppId)

			response := struct {
				Alive          bool      `json:"alive"`
				CurrentTime    time.Time `json:"currentTime"`
				StartingTime   time.Time `json:"startingTime"`
				MetricqVersion string    `json:"metricqVersion"`
				Version        string    `json:"version"`
				Hostname       string    `json:"hostname"`
			}{
				Alive:          true,
				CurrentTime:    time.Now(),
				StartingTime:   start_time,
				MetricqVersion: "metricq-go/0.0.1",
				Version:        version,
				Hostname:       hostname,
			}

			err := agent.SendRpcResponse(ctx, packet, response)

			if err != nil {
				log.Printf("failed to publish rpc response: %s", err)
			}
		case <-ctx.Done():
			break HandlerLoop
		}
	}
}

// makeCorrelationId returns a unique ID
func makeCorrelationId() string {
	return uuid.New().String()
}

// Rpc sends an `function` RPC request over the management connection using the
// `exchange` with `payload`.
// Payload will be marshalled into JSON.
// Returns the response body and any errors.
func (agent *Agent) Rpc(ctx context.Context, exchange, function string, payload any) ([]byte, error) {
	if agent.connection == nil || agent.rpcQueue == nil {
		return nil, fmt.Errorf("no connection established.")
	}

	log.Printf("Sending RPC message: %v", payload)

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	correlationId := makeCorrelationId()

	response := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: correlationId,
		ReplyTo:       agent.rpcQueue.Name,
		Body:          data,
		AppId:         agent.token,
	}

	rpc_request_chan := make(chan amqp.Delivery)

	rpc_request := rpcRequest{
		CorrelationId: correlationId,
		Response:      rpc_request_chan,
		CleanUp:       true,
	}

	agent.rpcRequestChannel <- rpc_request

	err = agent.connection.channel.PublishWithContext(ctx, exchange, function, true, false, response)
	if err != nil {
		return nil, fmt.Errorf("failed to publish RPC message: %w", err)
	}

	select {
	case response := <-rpc_request_chan:
		return response.Body, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout in RPC call. Function: %s", function)
	}
}

// NotifyRPC registers the given channel as the RPC handler for
// RPC messages with the given `function`. Upon receiving an RPC message that
// matches the given function, the message will be passed to the given channel.
func (agent *Agent) NotifyRPC(function string, rpc_response chan<- amqp.Delivery) {
	agent.rpcRequestChannel <- rpcRequest{
		CorrelationId: function,
		Response:      rpc_response,
		CleanUp:       false,
	}
}

// Close closes the connection.
func (agent *Agent) Close() {
	if agent.connection != nil {
		agent.connection.Close()
	}
}
