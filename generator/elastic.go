package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
	guuid "github.com/google/uuid"
)

// contains all configurations needed to send documents to Elastic
type Elastic struct {
	esAuth              string
	esURL               string
	esIndexName         string
	client              *http.Client
	generatorIdentifier string
}

// SendDocument sends a batch of documents to Rockset
func (e *Elastic) SendDocument(docs []interface{}) error {
	var builder bytes.Buffer
	for i := 0; i < len(docs); i++ {
		line, err := json.Marshal(docs[i])
		if err != nil {
			return err
		}
		
		index := make(map[string]interface{})
		index["_index"] = e.esIndexName
		index["_id"] = guuid.New().String()
		ret := make(map[string]interface{})
		ret["index"] = index
		metaLine, err := json.Marshal(ret)
		if err != nil {
			fmt.Println("Error preparing request", err)
			return err
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
		fmt.Println("Error during request!", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			return errors.Errorf("Error code: %d, body: %s \n", resp.StatusCode, bodyString)
		}
	}
	return nil
}

// GetLatestTimestamp returns the latest _event_time in Rockset
func (e *Elastic) GetLatestTimestamp() (time.Time, error) {
	searchURL := fmt.Sprintf("%s/%s/_search?size=0", e.esURL, e.esIndexName)

	jsonBody := fmt.Sprintf("{\"aggs\":{\"max_event_time_for_identifier\":{\"filter\":{\"term\":{\"generator_identifier\":\"%s\"}},\"aggs\":{\"max_event_time\":{\"max\":{\"field\":\"_event_time\"}}}}}}", e.generatorIdentifier)
	req, _ := http.NewRequest(http.MethodPost, searchURL, bytes.NewBufferString(jsonBody))

	req.Header.Add("Authorization", e.esAuth)
	req.Header.Add("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		fmt.Println("Error during request!\n", err)
		return time.Now(), err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			fmt.Printf("Error code: %d, body: %s \n", resp.StatusCode, bodyString)
		}
		return time.Now(), err
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
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(bodyBytes, &result)
	result = result["aggregations"].(map[string]interface{})
	result = result["max_event_time_for_identifier"].(map[string]interface{})
	result = result["max_event_time"].(map[string]interface{})
	if result["value"] == nil {
		return time.Now(), errors.New("Malformed result")
	}

	timeMicro := int64(result["value"].(float64))

	// Convert from microseconds to (secs, nanosecs)
	return time.Unix(timeMicro/1000000, (timeMicro%1000000)*1000), nil
}
