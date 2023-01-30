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

const defaultRocksetEndpoint = "https://api.rs2.usw2.rockset.com"

type RoundTripFunc func(req *http.Request) *http.Response

func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func NewTestClient(fn RoundTripFunc) *http.Client {
	return &http.Client{
		Transport: RoundTripFunc(fn),
	}
}

func NewRocksetClient(result string) *Rockset {
	client := NewTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: 200,
			// Send response to be tested
			Body: ioutil.NopCloser(bytes.NewBufferString(result)),
			// Must be set to non-nil value or it panics
			Header: make(http.Header),
		}
	})

	return &Rockset{
		APIKey:              "test",
		APIServer:           defaultRocksetEndpoint,
		CollectionPath:      "ws.test",
		Client:              client,
		GeneratorIdentifier: "test",
	}
}

func TestRockset_GetLatestTimestamp(t *testing.T) {
	expected := time.Now()
	r := NewRocksetClient(fmt.Sprintf(`{"results":[{"ts": %d}]}`,
		expected.UnixNano()/1000))

	t0, err := r.GetLatestTimestamp()
	assert.Nil(t, err)
	assert.Equal(t, expected.Unix(), t0.Unix())
}

func TestRockset_SendDocument(t *testing.T) {
	r := NewRocksetClient("")

	docs, err := GenerateDocs(10, "Rockset", r.GeneratorIdentifier, "uuid")
	assert.Nil(t, err)
	err = r.SendDocument(docs)
	assert.Nil(t, err)
}
