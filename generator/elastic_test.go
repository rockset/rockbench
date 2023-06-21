package generator

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func NewElasticClient(result string) *Elastic {
	client := NewTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: 200,
			// Send response to be tested
			Body: ioutil.NopCloser(bytes.NewBufferString(result)),
			// Must be set to non-nil value or it panics
			Header: make(http.Header),
		}
	})

	return &Elastic{
		Auth:                "test",
		URL:                 "test",
		IndexName:           "test",
		Client:              client,
		GeneratorIdentifier: "test",
	}
}
func TestElastic_GetLatestTimestamp(t *testing.T) {
	expected := time.Now()
	r := NewElasticClient(fmt.Sprintf(`{"aggregations":{"max_event_time_for_identifier":{"value":%d}}}`,
		expected.UnixNano()/1000))

	t0, err := r.GetLatestTimestamp()
	assert.Nil(t, err)
	assert.Equal(t, expected.Unix(), t0.Unix())
}

func TestElastic_SendDocument(t *testing.T) {
	r := NewElasticClient("")
	spec := DocumentSpec{
		Destination:          "elastic",
		GeneratorIdentifier:  r.GeneratorIdentifier,
		BatchSize:            10,
		Mode:                 "add",
		IdMode:               "sequential",
		UpdatePercentage:     -1,
		NumClusters:          -1,
		HotClusterPercentage: -1,
	};

	docs, err := GenerateDocs(spec)
	assert.Nil(t, err)
	err = r.SendDocument(docs)
	assert.Nil(t, err)
}
