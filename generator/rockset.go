package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	e2eLatenciesRockset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "e2e_latencies_rockset",
		Help: "The e2e latency between client and Rockset",
	})

	e2eRocksetLatenciesSummary = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "e2e_latencies_rockset_metric",
		Help:       "e2e latency in micro-seconds to Rockset",
		Objectives: objectiveMap,
	})
	numEventIngestedRockset = promauto.NewCounter(prometheus.CounterOpts{
		Name: "num_events_ingested_rockset",
		Help: "Number of events ingested to Rockset",
	})
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
	numEventIngestedRockset.Add(float64(numDocs))

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
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		recordWritesCompleted(float64(numDocs))
		_, _ = io.Copy(ioutil.Discard, resp.Body)
	} else {
		recordWritesErrored(float64(numDocs))
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			return errors.Errorf("Error code: %d, body: %s \n", resp.StatusCode, bodyString)
		}
	}
	return nil
}

// GetLatestTimestamp returns the latest _event_time in Rockset
func (r *Rockset) GetLatestTimestamp() (time.Time, error) {
	url := fmt.Sprintf("%s/v1/orgs/self/queries", r.apiServer)
	query := fmt.Sprintf("select UNIX_MICROS(_event_time) from %s where generator_identifier = '%s' ORDER BY _event_time DESC limit 1", r.collection, r.generatorIdentifier)
	body := map[string]interface{}{"sql": map[string]interface{}{"query": query}}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBody))
	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", r.apiKey))
	req.Header.Add("Content-Type", "application/json")

	resp, err := r.client.Do(req)
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
	// 	"results" : {
	// 		"?UNIX_MICROS": 1000000
	// 	}
	// }
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(bodyBytes, &result)
	x := result["results"].([]interface{})
	if len(x) == 0 {
		return time.Now(), errors.Errorf("Can't find the document")
	}

	x0 := x[0]
	y := x0.(map[string]interface{})
	yc := y["?UNIX_MICROS"]
	if yc == nil {
		return time.Now(), errors.New("Malformed result")
	}
	timeMicro := int64(yc.(float64))

	// Convert from microseconds to (secs, nanosecs)
	return time.Unix(timeMicro/1000000, (timeMicro%1000000)*1000), nil
}

func (r *Rockset) RecordE2ELatency(latency float64) {
	e2eLatenciesRockset.Set(latency)
	e2eRocksetLatenciesSummary.Observe(latency)
}
