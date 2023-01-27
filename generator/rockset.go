package generator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Rockset contains all configurations needed to send documents to Rockset
type Rockset struct {
	APIKey              string
	APIServer           string
	CollectionPath      string
	Client              *http.Client
	GeneratorIdentifier string
}

// SendDocument sends a batch of documents to Rockset
func (r *Rockset) SendDocument(docs []any) error {
	numDocs := len(docs)
	numEventIngested.Add(float64(numDocs))

	rcollection := strings.Split(r.CollectionPath, ".") // this is already validated to have two components
	URL := fmt.Sprintf("%s/v1/orgs/self/ws/%s/collections/%s/docs", r.APIServer, rcollection[0], rcollection[1])
	body := map[string][]interface{}{"data": docs}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, URL, bytes.NewBuffer(jsonBody))
	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", r.APIKey))
	req.Header.Add("Content-Type", "application/json")
	resp, err := r.Client.Do(req)
	if err != nil {
		recordWritesErrored(float64(numDocs))
		fmt.Println("Error during request!", err)
		return err
	}
	defer deferredErrorCloser(resp.Body)

	if resp.StatusCode == http.StatusOK {
		recordWritesCompleted(float64(numDocs))
		_, _ = io.Copy(io.Discard, resp.Body)
	} else {
		recordWritesErrored(float64(numDocs))
		bodyBytes, err := io.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			return fmt.Errorf("error code: %d, body: %s", resp.StatusCode, bodyString)
		}
	}
	return nil
}

func (r *Rockset) SendPatch(docs []interface{}) error {
	numDocs := len(docs)
	rcollection := strings.Split(r.CollectionPath, ".") // this is already validated to have two components
	URL := fmt.Sprintf("%s/v1/orgs/self/ws/%s/collections/%s/docs", r.APIServer, rcollection[0], rcollection[1])
	body := map[string][]interface{}{"data": docs}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPatch, URL, bytes.NewBuffer(jsonBody))
	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", r.APIKey))
	req.Header.Add("Content-Type", "application/json")
	resp, err := r.Client.Do(req)
	if err != nil {
		fmt.Println("Error during request!", err)
		return err
	}
	defer deferredErrorCloser(resp.Body)

	if resp.StatusCode == http.StatusOK {
		recordPatchesCompleted(float64(numDocs))
		_, _ = io.Copy(io.Discard, resp.Body)
	} else {
		recordPatchesErrored(float64(numDocs))
		bodyBytes, err := io.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			return fmt.Errorf("error code: %d, body: %s", resp.StatusCode, bodyString)
		}
	}
	return nil
}

// GetLatestTimestamp returns the latest _event_time in Rockset
func (r *Rockset) GetLatestTimestamp() (time.Time, error) {

	url := fmt.Sprintf("%s/v1/orgs/self/queries", r.APIServer)
	rcollection := strings.Split(r.CollectionPath, ".") // this is already validated to have two components
	query := fmt.Sprintf("select _ts as ts from \"%s\".\"%s\" where generator_identifier = '%s' ORDER BY _ts DESC limit 1", rcollection[0], rcollection[1], r.GeneratorIdentifier)
	body := map[string]interface{}{"sql": map[string]interface{}{"query": query}}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to marshal document: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to create new request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", r.APIKey))
	req.Header.Add("Content-Type", "application/json")

	resp, err := r.Client.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to execute request: %w", err)
	}

	defer deferredErrorCloser(resp.Body)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			fmt.Printf("Error code: %d, body: %s \n", resp.StatusCode, bodyString)
		}
		return time.Time{}, err
	}

	// Received status 200. Result structure will look something like
	// {
	// 	"results" : [{
	// 		"ts": 1000000
	// 	}]
	// }
	bodyBytes, err := io.ReadAll(resp.Body)
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
	yc := y["ts"]
	if yc == nil {
		return time.Time{}, fmt.Errorf("malformed result")
	}
	timeMicro := int64(yc.(float64))

	// Convert from microseconds to (secs, nanosecs)
	return time.Unix(timeMicro/1000000, (timeMicro%1000000)*1000), nil
}

func (r *Rockset) ConfigureDestination() error {
	return nil
}
