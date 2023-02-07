package generator

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
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
	r := NewElasticClient(fmt.Sprintf(`{"aggregations":{"max_event_time_for_identifier":{"max_event_time":{"value":%d}}}}`,
		expected.UnixNano()/1000))

	t0, err := r.GetLatestTimestamp()
	assert.Nil(t, err)
	assert.Equal(t, expected.Unix(), t0.Unix())
}

func TestElastic_SendDocument(t *testing.T) {
	r := NewElasticClient("")

	docs, err := GenerateDocs(10, "Elastic", r.GeneratorIdentifier, "sequential")
	assert.Nil(t, err)
	err = r.SendDocument(docs)
	assert.Nil(t, err)
}
