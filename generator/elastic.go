package generator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	guuid "github.com/google/uuid"
)

// Elastic contains all configurations needed to send documents to Elastic
type Elastic struct {
	Auth                string
	URL                 string
	IndexName           string
	Client              *http.Client
	GeneratorIdentifier string
}

func (j *Elastic) SendPatch(docs []interface{}) error {
	//TODO implement me
	panic("implement me")
}

// SendDocument sends a batch of documents to Rockset
func (e *Elastic) SendDocument(docs []any) error {
	numDocs := len(docs)
	numBytes := 0
	numEventIngested.Add(float64(numDocs))
	var builder bytes.Buffer
	for i := 0; i < len(docs); i++ {
		line, err := json.Marshal(docs[i])
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		index := make(map[string]interface{})
		index["_index"] = e.IndexName
		index["_id"] = guuid.New().String()
		ret := make(map[string]interface{})
		ret["index"] = index
		metaLine, err := json.Marshal(ret)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}

		builder.Write(metaLine)
		builder.WriteByte('\n')
		builder.Write(line)
		builder.WriteByte('\n')
		// Add bytes from doc payload
		numBytes += len(line)
	}

	body := builder.Bytes()
	bulkURL := e.URL + "/_bulk"
	elasticHTTPRequest, _ := http.NewRequest(http.MethodPost, bulkURL, bytes.NewBuffer(body))
	elasticHTTPRequest.Header.Add("Authorization", e.Auth)
	elasticHTTPRequest.Header.Add("Content-Type", "application/x-ndjson")

	resp, err := e.Client.Do(elasticHTTPRequest)
	if err != nil {
		recordWritesErrored(float64(numDocs))
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer deferredErrorCloser(resp.Body)

	if resp.StatusCode != http.StatusOK {
		recordWritesErrored(float64(numDocs))
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %w", err)
		}
		return fmt.Errorf("error code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}
	recordWritesCompleted(float64(numDocs))
	recordBytesSentAndCompleted(float64(numBytes))
	return nil
}

// GetLatestTimestamp returns the latest _event_time in Rockset
func (e *Elastic) GetLatestTimestamp() (time.Time, error) {
	searchURL := fmt.Sprintf("%s/%s/_search?size=0", e.URL, e.IndexName)

	// The identifier needs to be lowercased because by default, Elastic will index text in lowercase and the term query is case-sensitive
	// This can be avoided using the match query, but this is slightly slower than the term query
	jsonBody := fmt.Sprintf("{\"aggs\":{\"max_event_time_for_identifier\":{\"filter\":{\"term\":{\"generator_identifier\":\"%s\"}},\"aggs\":{\"max_event_time\":{\"max\":{\"field\":\"_event_time\"}}}}}}", strings.ToLower(e.GeneratorIdentifier))
	req, err := http.NewRequest(http.MethodPost, searchURL, bytes.NewBufferString(jsonBody))
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to create new request: %w", err)
	}

	req.Header.Add("Authorization", e.Auth)
	req.Header.Add("Content-Type", "application/json")

	resp, err := e.Client.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to perform request: %w", err)
	}
	defer deferredErrorCloser(resp.Body)

	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to read %s response body: %w", resp.Status, err)
		}
		return time.Time{}, fmt.Errorf("request failed: expected OK got %s: %s", resp.Status, string(bodyBytes))
	}

	// Received status 200. Result structure will look something like
	// {
	// 	...
	// 	"aggregations": {
	// 		"max_event_time_for_identifier": {
	// 			"doc_count": 201874000,
	// 			"max_event_time": {
	// 					"value": 1.677014840315018E15
	// 			}
	// 		}
	// 	}
	// }
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to read response body: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return time.Time{}, fmt.Errorf("failed to unmarshal reponse: %w", err)
	}

	// TODO: check type assertions
	result = result["aggregations"].(map[string]interface{})
	result = result["max_event_time_for_identifier"].(map[string]interface{})
	result = result["max_event_time"].(map[string]interface{})
	if result["value"] == nil {
		return time.Time{}, errors.New("malformed result, value is nil")
	}

	timeMicro := int64(result["value"].(float64))

	// Convert from microseconds to (secs, nanosecs)
	return time.Unix(timeMicro/1_000_000, (timeMicro%1_000_000)*1_000), nil
}

func (e *Elastic) ConfigureDestination() error {
	return nil
}
