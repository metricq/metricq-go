package metrigo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	uuid "github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RpcResponse struct {
	Sender string
	Body   []byte
}

type rpcRequest struct {
	CorrelationId string
	Response      chan<- amqp.Delivery
	CleanUp       bool
}

type Agent struct {
	token      string
	Server     string
	connection *Connection
	rpc_queue  *amqp.Queue
	rpc_chan   chan<- rpcRequest
}

func NewAgent(token, server string) Agent {
	return Agent{
		token: token, Server: server,
	}
}

type RpcMessage struct {
	Function string `json:"function"`
}

func (con *Agent) Connect() {
	if con.token == "" {
		log.Panicf("Invalid token: %s", con.token)
	}

	con.connection = new(Connection)
	con.connection.Connect(con.Server)

	queue, err := con.connection.channel.QueueDeclare(con.token+"-rpc",
		false, false, true, false, nil)
	if err != nil {
		log.Panicf("Failed to create rpc queue: %s", err)
	}

	err = con.connection.channel.QueueBind(con.token+"-rpc", con.token, "metricq.broadcast", false, nil)
	if err != nil {
		log.Panicf("Failed to bind RPC queue to metricq.broadcast exchange: %s", err)
	}

	con.rpc_queue = &queue

	rpc_chan := make(chan rpcRequest)
	con.rpc_chan = rpc_chan

	rpc_channel, err := con.connection.connection.Channel()
	if err != nil {
		log.Panicf("Failed to create rpc channel: %s", err)
	}

	go func(channel *amqp.Channel, queue *amqp.Queue, requests <-chan rpcRequest) {
		consumer, err := channel.Consume(queue.Name, "", true, true, false, false, nil)
		if err != nil {
			log.Panicf("Failed to start consuming on rpc queue: %s", err)
		}

		log.Printf("Starting RPC consume on queue %s", queue.Name)

		handlers := make(map[string](rpcRequest))

	ConsumeLoop:
		for {
			select {
			case packet := <-consumer:
				// TODO technically speaking, if shit went wrong, we are supposed
				// to requeue these messages, but well ¯\_(ツ)_/¯

				if channel.IsClosed() {
					log.Print("RPC Channel closed. Stopped RPC consume.")
					break ConsumeLoop
				}

				handler, ok := handlers[packet.CorrelationId]

				if ok {
					log.Printf("Received RPC response for %s from: %s", packet.CorrelationId, packet.AppId)

					handler.Response <- packet

					if handler.CleanUp {
						close(handler.Response)
						delete(handlers, packet.CorrelationId)
					}
				} else {
					request := RpcMessage{}
					err := json.Unmarshal(packet.Body, &request)
					if err == nil {
						handler, ok := handlers[request.Function]
						if ok {
							handler.Response <- packet
							break
						}
					}

					log.Printf("Received unexpected RPC message (%s) from %s: %s", packet.CorrelationId, packet.AppId, packet.Body)
				}
			case request := <-requests:
				// TODO clean up handlers after a timeout
				// TODO don't overwrite old handlers?
				handlers[request.CorrelationId] = request
			}
		}
	}(rpc_channel, &queue, rpc_chan)
}

func (con *Agent) SendRpcResponse(ctx context.Context, packet amqp.Delivery, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("Failed to marshal JSON payload: %w", err)
	}

	response := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: packet.CorrelationId,
		Body:          data,
		AppId:         con.token,
	}

	return con.connection.channel.PublishWithContext(ctx, "", packet.ReplyTo, true, false, response)
}

func (con *Agent) HandleDiscover(ctx context.Context, version string) {
	start_time := time.Now()
	response_channel := make(chan amqp.Delivery)

	con.RegisterRpcHandler("discover", response_channel)

	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %s", err)
		hostname = "[unknown]"
	}

HandlerLoop:
	for {
		select {
		case packet := <-response_channel:
			log.Printf("Respond to discover RPC from: %s", packet.AppId)

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

			err := con.SendRpcResponse(ctx, packet, response)

			if err != nil {
				log.Panicf("Failed to publish rpc response: %s", err)
			}
		case <-ctx.Done():
			break HandlerLoop
		}
	}
}

func makeCorrelationId() string {
	return uuid.New().String()
}

func (con *Agent) Rpc(ctx context.Context, exchange, function string, payload any) ([]byte, error) {
	log.Printf("Sending RPC message: %v", payload)

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	correlationId := makeCorrelationId()

	response := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: correlationId,
		ReplyTo:       con.rpc_queue.Name,
		Body:          data,
		AppId:         con.token,
	}

	rpc_request_chan := make(chan amqp.Delivery)

	rpc_request := rpcRequest{
		CorrelationId: correlationId,
		Response:      rpc_request_chan,
		CleanUp:       true,
	}

	con.rpc_chan <- rpc_request

	err = con.connection.channel.PublishWithContext(ctx, exchange, function, true, false, response)
	if err != nil {
		log.Panicf("Failed to publish rpc message: %s", err)
	}

	select {
	case response := <-rpc_request_chan:
		return response.Body, nil
	case <-ctx.Done():
		return nil, errors.New("Timeout in RPC call. Function: " + function)
	}
}

func (con *Agent) RegisterRpcHandler(function string, rpc_response chan<- amqp.Delivery) {
	con.rpc_chan <- rpcRequest{
		CorrelationId: function,
		Response:      rpc_response,
		CleanUp:       false,
	}
}

func (con *Agent) Close() {
	if con.connection != nil {
		con.connection.Close()
	}
}
