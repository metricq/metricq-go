package metricq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	uuid "github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewAgent(token, server string) (*Agent, error) {
	url, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	return &Agent{
		token:   token,
		Server:  url,
		pending: make(map[string]chan<- amqp.Delivery),
	}, nil
}

// Agent is the base client in a MetricQ cluster.
// It allows to send and receive RPC messages over the established connection.
// Make sure to call `Connect()` prior to `RPC()`
type Agent struct {
	token          string
	Server         *url.URL
	mu             sync.RWMutex
	connection     *Connection
	rpcQueue       *amqp.Queue
	pendingMu      sync.Mutex
	pending        map[string]chan<- amqp.Delivery
	reconnectHooks []reconnectHook
}

type reconnectHook struct {
	name string
	fn   func(context.Context) error
}

// RpcMessage is the base for RPC messages in MetricQ.
// It has the `function` member.
type RPCRequest interface {
	RPCFunction() string
}

type RpcMessage struct {
	Function string `json:"function"`
}

func (msg RpcMessage) RPCFunction() string {
	return msg.Function
}

func marshalRPCPayload(payload RPCRequest) ([]byte, error) {
	function := payload.RPCFunction()
	if function == "" {
		return nil, fmt.Errorf("rpc payload function must not be empty")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("rpc payload must marshal to a JSON object: %w", err)
	}

	if rawFunction, ok := object["function"]; ok {
		var payloadFunction string
		if err := json.Unmarshal(rawFunction, &payloadFunction); err != nil {
			return nil, fmt.Errorf("rpc payload function must be a string: %w", err)
		}
		if payloadFunction != "" && payloadFunction != function {
			return nil, fmt.Errorf("rpc payload function %q does not match request function %q", payloadFunction, function)
		}
	}

	functionData, err := json.Marshal(function)
	if err != nil {
		return nil, err
	}
	object["function"] = functionData

	return json.Marshal(object)
}

// Connect established the management connection to the MetricQ cluster.
// This allows to use the MetricQ RPC protocol. A RPC receiver will run within
// the passed rpcCtx context; cancel this context to stop the receiver.
// It returns any errors.
func (agent *Agent) Connect(rpcCtx context.Context) error {
	if agent.token == "" {
		return fmt.Errorf("empty token is invalid")
	}

	connection := new(Connection)
	err := connection.Connect(agent.Server, fmt.Sprintf("management connection %s", agent.token))
	if err != nil {
		return err
	}

	queue, err := agent.setupRPCQueue(connection)
	if err != nil {
		connection.Close()
		return err
	}

	agent.mu.Lock()
	agent.connection = connection
	agent.rpcQueue = queue
	agent.mu.Unlock()

	go agent.runRPCConsumeLoop(rpcCtx)

	return nil
}

func (agent *Agent) setupRPCQueue(connection *Connection) (*amqp.Queue, error) {
	queue, err := connection.channel.QueueDeclare(agent.token+"-rpc",
		false, false, true, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC queue: %w", err)
	}
	err = connection.channel.QueueBind(agent.token+"-rpc", agent.token, "metricq.broadcast", false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to bind RPC queue to metricq.broadcast exchange: %w", err)
	}
	return &queue, nil
}

func (agent *Agent) runRPCConsumeLoop(ctx context.Context) {
	backoff := 500 * time.Millisecond
	for {
		err := agent.rpcConsumeLoop(ctx)
		if ctx.Err() != nil {
			return
		}

		log.Printf("management RPC connection lost: %v", err)
		for {
			if ctx.Err() != nil {
				return
			}
			log.Printf("trying to reconnect management RPC...")
			reconnectErr := agent.reconnectManagement()
			if reconnectErr == nil {
				log.Printf("management RPC connection restored")
				go agent.runReconnectHooks(ctx)
				backoff = 500 * time.Millisecond
				break
			}

			log.Printf("management RPC reconnect failed: %v", reconnectErr)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 8*time.Second {
				backoff *= 2
			}
		}
	}
}

func (agent *Agent) reconnectManagement() error {
	connection := new(Connection)
	if err := connection.Connect(agent.Server, fmt.Sprintf("management connection %s", agent.token)); err != nil {
		return err
	}
	queue, err := agent.setupRPCQueue(connection)
	if err != nil {
		connection.Close()
		return err
	}

	agent.mu.Lock()
	old := agent.connection
	agent.connection = connection
	agent.rpcQueue = queue
	agent.mu.Unlock()

	if old != nil {
		old.Close()
	}
	return nil
}

func (agent *Agent) runReconnectHooks(parentCtx context.Context) {
	agent.mu.RLock()
	hooks := append([]reconnectHook(nil), agent.reconnectHooks...)
	agent.mu.RUnlock()

	for _, hook := range hooks {
		if hook.fn == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
		err := hook.fn(ctx)
		cancel()
		if err != nil {
			log.Printf("reconnect hook %q failed: %v", hook.name, err)
			continue
		}
		log.Printf("reconnect hook %q completed", hook.name)
	}
}

func (agent *Agent) RegisterReconnectHook(name string, fn func(context.Context) error) {
	if fn == nil {
		return
	}
	agent.mu.Lock()
	agent.reconnectHooks = append(agent.reconnectHooks, reconnectHook{
		name: name,
		fn:   fn,
	})
	agent.mu.Unlock()
}

func (agent *Agent) rpcConsumeLoop(ctx context.Context) error {
	agent.mu.RLock()
	connection := agent.connection
	rpcQueue := agent.rpcQueue
	agent.mu.RUnlock()
	if connection == nil || connection.connection == nil {
		return fmt.Errorf("management connection not available")
	}
	if rpcQueue == nil {
		return fmt.Errorf("rpc queue not initialized")
	}

	channel, err := connection.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create RPC channel: %w", err)
	}
	defer channel.Close()

	consumer, err := channel.Consume(rpcQueue.Name, "", false, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to start consuming on rpc queue: %w", err)
	}

	log.Printf("starting RPC consume on queue %s", rpcQueue.Name)

ConsumeLoop:
	for {
		select {
		case packet, ok := <-consumer:
			if !ok {
				return fmt.Errorf("RPC consume channel closed")
			}
			if channel.IsClosed() {
				return fmt.Errorf("RPC Channel closed. Stopped RPC consume.")
			}

			agent.pendingMu.Lock()
			ch, ok := agent.pending[packet.CorrelationId]
			agent.pendingMu.Unlock()

			if ok {
				log.Printf("Received RPC response for %s from: %s", packet.CorrelationId, packet.AppId)
				select {
				case ch <- packet:
				default:
					log.Printf("Dropping RPC response for %s: handler channel not ready", packet.CorrelationId)
				}
				if err := packet.Ack(false); err != nil {
					log.Printf("failed to ack rpc response %s: %v", packet.CorrelationId, err)
				}
				continue
			}

			// Try to dispatch as an incoming RPC request by function name.
			request := RpcMessage{}
			if err := json.Unmarshal(packet.Body, &request); err == nil {
				agent.pendingMu.Lock()
				ch, ok = agent.pending[request.Function]
				agent.pendingMu.Unlock()
				if ok {
					select {
					case ch <- packet:
						log.Printf("Received RPC request from: %s", packet.AppId)
					default:
						log.Printf("Dropping RPC request %s from %s: handler channel not ready", request.Function, packet.AppId)
					}
					if ackErr := packet.Ack(false); ackErr != nil {
						log.Printf("failed to ack rpc request %s: %v", request.Function, ackErr)
					}
					continue
				}
			}

			log.Printf("Received unexpected RPC message (%s) from %s; dropping", packet.CorrelationId, packet.AppId)
			if ackErr := packet.Ack(false); ackErr != nil {
				log.Printf("failed to ack unexpected rpc message %s: %v", packet.CorrelationId, ackErr)
			}

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
	agent.mu.RLock()
	connection := agent.connection
	agent.mu.RUnlock()
	if connection == nil {
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

	return connection.channel.PublishWithContext(ctx, "", rpcRequest.ReplyTo, true, false, response)
}

// ServeDiscover handles incoming discover RPC requests until ctx is cancelled.
// Call as a blocking function or with go for background operation.
func (agent *Agent) ServeDiscover(ctx context.Context, version string) {
	start_time := time.Now()
	response_channel := make(chan amqp.Delivery)

	agent.NotifyRPC("discover", response_channel)
	defer agent.UnregisterRPC("discover")

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

// Rpc sends the RPC request encoded in `payload` over the management connection using
// `exchange`.
// Payload will be marshalled into JSON.
// Returns the response body and any errors.
func (agent *Agent) Rpc(ctx context.Context, exchange string, payload RPCRequest) ([]byte, error) {
	function := payload.RPCFunction()
	if function == "" {
		return nil, fmt.Errorf("rpc payload function must not be empty")
	}
	agent.mu.RLock()
	connection := agent.connection
	rpcQueue := agent.rpcQueue
	agent.mu.RUnlock()

	if connection == nil || rpcQueue == nil {
		return nil, fmt.Errorf("no connection established.")
	}

	log.Printf("Sending RPC message: %v", payload)

	data, err := marshalRPCPayload(payload)
	if err != nil {
		return nil, err
	}

	correlationId := makeCorrelationId()
	responseCh := make(chan amqp.Delivery, 1)

	agent.pendingMu.Lock()
	agent.pending[correlationId] = responseCh
	agent.pendingMu.Unlock()
	defer func() {
		agent.pendingMu.Lock()
		delete(agent.pending, correlationId)
		agent.pendingMu.Unlock()
	}()

	msg := amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: correlationId,
		ReplyTo:       rpcQueue.Name,
		Body:          data,
		AppId:         agent.token,
	}

	err = connection.channel.PublishWithContext(ctx, exchange, function, true, false, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish RPC message: %w", err)
	}

	select {
	case response := <-responseCh:
		return response.Body, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout in RPC call. Function: %s", function)
	}
}

// NotifyRPC registers the given channel as the RPC handler for
// RPC messages with the given `function`. Upon receiving an RPC message that
// matches the given function, the message will be passed to the given channel.
func (agent *Agent) NotifyRPC(function string, rpc_response chan<- amqp.Delivery) {
	agent.pendingMu.Lock()
	agent.pending[function] = rpc_response
	agent.pendingMu.Unlock()
}

// UnregisterRPC removes the handler previously registered for function via NotifyRPC.
func (agent *Agent) UnregisterRPC(function string) {
	agent.pendingMu.Lock()
	delete(agent.pending, function)
	agent.pendingMu.Unlock()
}

// Close closes the connection.
func (agent *Agent) Close() {
	if agent.connection != nil {
		agent.connection.Close()
	}
}
