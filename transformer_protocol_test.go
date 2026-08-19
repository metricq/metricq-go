package metricq

import (
	"encoding/json"
	"testing"
)

func TestTransformerSubscribeResponseReadsMetricsMetadata(t *testing.T) {
	response := []byte(`{
		"dataServerAddress":"vhost:/",
		"dataQueue":"transformer-input",
		"metrics":{
			"dummy.source":{"rate":1,"unit":"V"},
			"example.counter":{"rate":10}
		}
	}`)
	var decoded TransformerSubscribeResponse
	if err := json.Unmarshal(response, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Metadata) != 2 {
		t.Fatalf("metadata = %#v, want two metrics", decoded.Metadata)
	}
	if got := decoded.Metadata["dummy.source"]; got["unit"] != "V" || got["rate"] != float64(1) {
		t.Fatalf("dummy.source metadata = %#v", got)
	}
}
