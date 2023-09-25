package metrigo

import (
	"context"
	"encoding/json"
	"log"

	uuid "github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Agent struct {
	token        string
	Server       string
	connection   *Connection
	rpc_queue    *amqp.Queue
	rpc_handlers *map[string](chan []byte)
}

func New(token, server string) *Agent {
    return &Agent{token: token, Server: server}
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

	con.rpc_queue = &queue

	rpc_handlers := make(map[string](chan []byte))
	con.rpc_handlers = &rpc_handlers
	rpc_channel, err := con.connection.connection.Channel()
	if err != nil {
		log.Panicf("Failed to create rpc channel: %s", err)
	}

	go func(channel *amqp.Channel, queue *amqp.Queue, handlers *map[string](chan []byte)) {
		consumer, err := channel.Consume(queue.Name, "", true, true, false, false, nil)
		if err != nil {
			log.Panicf("Failed to start consuming on rpc queue: %s", err)
		}

		log.Printf("Starting RPC consume on queue %s", queue.Name)

		for packet := range consumer {
			log.Printf("Received RPC response for %s from: %s", packet.CorrelationId, packet.AppId)

			(*handlers)[packet.CorrelationId] <- packet.Body
			close((*handlers)[packet.CorrelationId])
		}
	}(rpc_channel, &queue, &rpc_handlers)
}

func makeCorrelationId() string {
	return uuid.New().String()
}

func (con *Agent) Rpc(ctx context.Context, exchange, function string, payload any) ([]byte, error) {
	log.Printf("Sending RPC message: %v", payload)

	data, err := json.Marshal(payload)
	log.Printf("Body Contents: %s", data)
	if err != nil {
		return nil, err
	}

	correlationId := makeCorrelationId()

	(*con.rpc_handlers)[correlationId] = make(chan []byte)

	publishing := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: correlationId,
		ReplyTo:       con.rpc_queue.Name,
		Body:          data,
		AppId:         con.token,
	}

	err = con.connection.channel.PublishWithContext(ctx, exchange, function, true, false, publishing)
	if err != nil {
		log.Panicf("Failed to publish rpc message: %s", err)
	}

	log.Print("Done")

	response := <-(*con.rpc_handlers)[correlationId]
	return response, nil
}

func (con *Agent) Close() {

}
