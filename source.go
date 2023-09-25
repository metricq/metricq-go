package metrigo

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"
)

type Source struct {
	connection *Connection
}

type SourceRegisterMessage struct {
	Function string `json:"function"`
}

type SourceRegisterResponse struct {
	DataServerAddress string          `json:"dataServerAddress"`
	DataExchange      string          `json:"dataExchange"`
	Config            json.RawMessage `json:"config"`
}

func (resp *SourceRegisterResponse) parseDataServer(server string) string {
	if strings.HasPrefix(resp.DataServerAddress, "vhost:") {
		return server + strings.TrimPrefix(resp.DataServerAddress, "vhost:")
	} else {
		return resp.DataServerAddress
	}
}

func (src *Source) Register(ctx context.Context, agent *Agent) json.RawMessage {
	rpcContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	response, err := agent.Rpc(rpcContext, "metricq.management", "source.register", SourceRegisterMessage{Function: "source.register"})
	if err != nil {
		log.Panicf("Failed to source.register RPC: %s", err)
	}

	log.Printf("Received RPC response: %s", response)

	data := new(SourceRegisterResponse)
	err = json.Unmarshal(response, data)
	if err != nil {
		log.Panicf("%s: %s", "Failed to parse RPC response", err)
	}

	src.connection = new(Connection)
	src.connection.Connect(data.parseDataServer(agent.Server))

	cancel()

	return data.Config
}
