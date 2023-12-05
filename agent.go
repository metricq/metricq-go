package metrigo

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	uuid "github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rpcRequest struct {
	CorrelationId string
	Response      chan<- []byte
}

type Agent struct {
	token      string
	Server     string
	connection *Connection
	rpc_queue  *amqp.Queue
	rpc_chan   chan<- rpcRequest
}

func NewAgent(token, server string) *Agent {
	return &Agent{
		token: token, Server: server,
	}
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

		handlers := make(map[string](chan<- []byte))

	ConsumeLoop:
		for {
			select {
			case packet := <-consumer:
				if channel.IsClosed() {
					log.Print("RPC Channel closed. Stopped RPC consume.")
					break ConsumeLoop
				}

				response_channel, ok := handlers[packet.CorrelationId]

				if ok {
					log.Printf("Received RPC response for %s from: %s", packet.CorrelationId, packet.AppId)

					response_channel <- packet.Body
					close(response_channel)
					delete(handlers, packet.CorrelationId)
				} else {
					log.Printf("Received unexpected RPC message (%s) from %s: %s", packet.CorrelationId, packet.AppId, packet.Body)
				}
			case request := <-requests:
				// we got a new request, just store the response channel
				// TODO clean up handlers after a timeout
				handlers[request.CorrelationId] = request.Response
			}
		}
	}(rpc_channel, &queue, rpc_chan)
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
	log.Printf("Body Contents: %s", data)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	correlationId := makeCorrelationId()

	publishing := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: correlationId,
		ReplyTo:       con.rpc_queue.Name,
		Body:          data,
		AppId:         con.token,
	}

	rpc_request_chan := make(chan []byte)

	rpc_request := rpcRequest{
		CorrelationId: correlationId,
		Response:      rpc_request_chan,
	}

	con.rpc_chan <- rpc_request

	err = con.connection.channel.PublishWithContext(ctx, exchange, function, true, false, publishing)
	if err != nil {
		log.Panicf("Failed to publish rpc message: %s", err)
	}

	select {
	case response := <-rpc_request_chan:
		return response, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("Timeout in RPC call. Function: " + function)
	}
}

func (con *Agent) Close() {
	con.connection.Close()
}
