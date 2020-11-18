package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	guuid "github.com/google/uuid"
)

// Elastic contains all configurations needed to send documents to Elastic
type Elastic struct {
	esAuth              string
	esURL               string
	esIndexName         string
	client              *http.Client
	generatorIdentifier string
}

// SendDocument sends a batch of documents to Rockset
func (e *Elastic) SendDocument(docs []interface{}) error {
	numDocs := len(docs)
	numEventIngested.Add(float64(numDocs))
	var builder bytes.Buffer
	for i := 0; i < len(docs); i++ {
		line, err := json.Marshal(docs[i])
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		index := make(map[string]interface{})
		index["_index"] = e.esIndexName
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
	}

	body := builder.Bytes()
	bulkURL := e.esURL + "/_bulk"
	elasticHTTPRequest, _ := http.NewRequest(http.MethodPost, bulkURL, bytes.NewBuffer(body))
	elasticHTTPRequest.Header.Add("Authorization", e.esAuth)
	elasticHTTPRequest.Header.Add("Content-Type", "application/x-ndjson")

	resp, err := e.client.Do(elasticHTTPRequest)
	if err != nil {
		recordWritesErrored(float64(numDocs))
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer deferredErrorCloser(resp.Body)

	if resp.StatusCode != http.StatusOK {
		recordWritesErrored(float64(numDocs))
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %w", err)
		}
		return fmt.Errorf("error code: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}
	recordWritesCompleted(float64(numDocs))
	return nil
}

// GetLatestTimestamp returns the latest _event_time in Rockset
func (e *Elastic) GetLatestTimestamp() (time.Time, error) {
	searchURL := fmt.Sprintf("%s/%s/_search?size=0", e.esURL, e.esIndexName)

	jsonBody := fmt.Sprintf("{\"aggs\":{\"max_event_time_for_identifier\":{\"filter\":{\"term\":{\"generator_identifier\":\"%s\"}},\"aggs\":{\"max_event_time\":{\"max\":{\"field\":\"_event_time\"}}}}}}", e.generatorIdentifier)
	req, err := http.NewRequest(http.MethodPost, searchURL, bytes.NewBufferString(jsonBody))
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to create new request: %w", err)
	}

	req.Header.Add("Authorization", e.esAuth)
	req.Header.Add("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to perform request: %w", err)
	}
	defer deferredErrorCloser(resp.Body)

	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to read %s response body: %w", resp.Status, err)
		}
		return time.Time{}, fmt.Errorf("request failed: expected OK got %s: %s", resp.Status, string(bodyBytes))
	}

	// Received status 200. Result structure will look something like
	// {
	// 	"aggregation" : {
	// 		"max_event_time_for_identifier": {
	//			"max_event_time" : {
	//				"value": 1000000
	//			}
	//      }
	// 	}
	// }
	bodyBytes, err := ioutil.ReadAll(resp.Body)
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
