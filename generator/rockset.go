package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// Rockset contains all configurations needed to send documents to Rockset
type Rockset struct {
	apiKey              string
	apiServer           string
	collection          string
	client              *http.Client
	generatorIdentifier string
}

// SendDocument sends a batch of documents to Rockset
func (r *Rockset) SendDocument(docs []interface{}) error {
	numDocs := len(docs)
	numEventIngested.Add(float64(numDocs))

	URL := fmt.Sprintf("%s/v1/orgs/self/ws/commons/collections/%s/docs", r.apiServer, r.collection)
	body := map[string][]interface{}{"data": docs}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, URL, bytes.NewBuffer(jsonBody))
	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", r.apiKey))
	req.Header.Add("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		recordWritesErrored(float64(numDocs))
		fmt.Println("Error during request!", err)
		return err
	}
	defer deferredErrorCloser(resp.Body)

	if resp.StatusCode == http.StatusOK {
		recordWritesCompleted(float64(numDocs))
		_, _ = io.Copy(ioutil.Discard, resp.Body)
	} else {
		recordWritesErrored(float64(numDocs))
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			return fmt.Errorf("error code: %d, body: %s", resp.StatusCode, bodyString)
		}
	}
	return nil
}

// GetLatestTimestamp returns the latest _event_time in Rockset
func (r *Rockset) GetLatestTimestamp() (time.Time, error) {
	url := fmt.Sprintf("%s/v1/orgs/self/queries", r.apiServer)
	query := fmt.Sprintf("select UNIX_MICROS(_event_time) from %s where generator_identifier = '%s' ORDER BY _event_time DESC limit 1", r.collection, r.generatorIdentifier)
	body := map[string]interface{}{"sql": map[string]interface{}{"query": query}}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to marshal document: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to create new request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", r.apiKey))
	req.Header.Add("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to execute request: %w", err)
	}

	defer deferredErrorCloser(resp.Body)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			fmt.Printf("Error code: %d, body: %s \n", resp.StatusCode, bodyString)
		}
		return time.Time{}, err
	}

	// Received status 200. Result structure will look something like
	// {
	// 	"results" : [{
	// 		"?UNIX_MICROS": 1000000
	// 	}]
	// }
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to read response body: %v", err)
	}
	var result map[string]interface{}
	if err = json.Unmarshal(bodyBytes, &result); err != nil {
		return time.Time{}, fmt.Errorf("failed to unmarshal response body: %v", err)
	}

	// TODO: check type assertions
	x := result["results"].([]interface{})
	if len(x) == 0 {
		return time.Time{}, fmt.Errorf("could not find the document")
	}

	x0 := x[0]
	y := x0.(map[string]interface{})
	yc := y["?UNIX_MICROS"]
	if yc == nil {
		return time.Time{}, fmt.Errorf("malformed result")
	}
	timeMicro := int64(yc.(float64))

	// Convert from microseconds to (secs, nanosecs)
	return time.Unix(timeMicro/1000000, (timeMicro%1000000)*1000), nil
}
