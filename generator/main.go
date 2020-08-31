package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/bxcodec/faker/v3"
	guuid "github.com/google/uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	generatorIdentifier string

	// More info can found here: https://godoc.org/github.com/prometheus/client_golang/prometheus#NewSummary
	objectiveMap = map[float64]float64{0.5: 0.05, 0.95: 0.005}

	writesCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "writes_completed",
		Help: "The total number of writes completed",
	})

	writesErrored = promauto.NewCounter(prometheus.CounterOpts{
		Name: "writes_errored",
		Help: "The total number of writes errored",
	})

	e2eLatencies = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "e2e_latencies",
		Help: "The e2e latency between client and the Destination",
	})
	e2eLatenciesSummary = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "e2e_latencies_metric",
		Help:       "e2e latency in micro-seconds between client and the Destination",
		Objectives: objectiveMap,
	})
	numEventIngested = promauto.NewCounter(prometheus.CounterOpts{
		Name: "num_events_ingested",
		Help: "Number of events ingested to the Destination",
	})
)

func recordE2ELatency(latency float64) {
	e2eLatencies.Set(latency)
	e2eLatenciesSummary.Observe(latency)
}

func recordWritesCompleted(count float64) {
	writesCompleted.Add(count)
}

func recordWritesErrored(count float64) {
	writesErrored.Add(count)
}

func main() {
	wps := mustGetEnvInt("WPS")
	batchSize := mustGetEnvInt("BATCH_SIZE")
	destination := mustGetEnvString("DESTINATION")

	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("defaultRoundTripper not an *http.Transport"))
	}
	defaultTransport := defaultTransportPointer
	defaultTransport.MaxIdleConns = 100
	defaultTransport.MaxIdleConnsPerHost = 100
	client := &http.Client{Transport: defaultTransport}

	generatorIdentifier = generateRandomString(10)
	fmt.Println("Generator identifier: ", generatorIdentifier)

	var d Destination

	if destination == "Rockset" {
		apiKey := mustGetEnvString("ROCKSET_API_KEY")
		apiServer := getEnvDefault("ROCKSET_API_SERVER", "https://api.rs2.usw2.rockset.com")
		collection := mustGetEnvString("ROCKSET_COLLECTION")

		d = &Rockset{
			apiKey:              apiKey,
			apiServer:           apiServer,
			collection:          collection,
			client:              client,
			generatorIdentifier: generatorIdentifier,
		}
	} else if destination == "Elastic" {
		esAuth := mustGetEnvString("ELASTIC_AUTH")
		esURL := mustGetEnvString("ELASTIC_URL")
		esIndexName := mustGetEnvString("ELASTIC_INDEX")

		d = &Elastic{
			esAuth:              esAuth,
			esURL:               esURL,
			esIndexName:         esIndexName,
			client:              client,
			generatorIdentifier: generatorIdentifier,
		}
	} else {
		log.Fatal("Unsupported destination. Only supported one is Rockset.")
	}

	startMetricListener()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGKILL)
	exitChan := make(chan int)

	var done = false
	go func() {
		for {
			s := <-signalChan
			switch s {
			case syscall.SIGTERM:
				fmt.Printf("Receive SIGTERM\n")
				done = true
				exitChan <- 0
			case syscall.SIGKILL:
				fmt.Printf("Receive SIGKILL\n")
				done = true
				exitChan <- 0
			}
		}
	}()

	// Periodically read number of docs and log to output
	go func() {
		for {
			if done {
				break
			}
			now := time.Now()
			latestTimestamp, err := d.GetLatestTimestamp()
			latency := now.Sub(latestTimestamp)

			if err == nil {
				fmt.Printf("Latency: %s", latency)
				recordE2ELatency(float64(latency.Microseconds()))
			}

			time.Sleep(30 * time.Second)
		}
	}()

	// Write function
	for {
		if done {
			break
		}

		iterationStart := time.Now()
		for i := 0; i < wps; i++ {
			docs := generateDocs(batchSize, destination)
			go d.SendDocument(docs)
		}
		elapsed := time.Now().Sub(iterationStart)
		if elapsed > time.Second {
			fmt.Printf("Iteration time %s > 1s, invalid results!!!", elapsed)
		}
		sleepTime := time.Second - elapsed
		time.Sleep(sleepTime)
	}

	code := <-exitChan
	os.Exit(code)
}

type DocStruct struct {
	Guid       string
	IsActive   bool
	Balance    float64 `faker:"amount"`
	Picture    string
	Age        int `faker:"oneof: 15, 27, 61"`
	Name       NameStruct
	Company    string `faker:"oneof: facebook, google, rockset, tesla, uber, lyft"`
	Email      string `faker:"email"`
	Phone      string `faker:"phone_number"`
	Address    AddressStruct
	About      string `faker:"sentence"`
	Registered string `faker:"timestamp"`
	Tags       []string
	Friends    FriendStruct
	Greeting   string `faker:"paragraph"`
}

type NameStruct struct {
	First string `faker:"first_name"`
	Last  string `faker:"last_name"`
}

type AddressStruct struct {
	Street      string `faker:"oneof: 1st, 2nd, 3rd, 4th, 5th, 6th, 7th, 8th, 9th, 10th"`
	City        string `faker:"oneof: SF, San Mateo, San Jose, Mountain View, Menlo Park, Palo Alto"`
	ZipCode     int16
	Coordinates CoordinatesStruct
}

type CoordinatesStruct struct {
	Latitude  float32 `faker:"lat"`
	Longitude float32 `faker:"long"`
}

type FriendStruct struct {
	Friend1  FriendDetailsStruct
	Friend2  FriendDetailsStruct
	Friend3  FriendDetailsStruct
	Friend4  FriendDetailsStruct
	Friend5  FriendDetailsStruct
	Friend6  FriendDetailsStruct
	Friend7  FriendDetailsStruct
	Friend8  FriendDetailsStruct
	Friend9  FriendDetailsStruct
	Friend10 FriendDetailsStruct
}

type FriendDetailsStruct struct {
	Name NameStruct
	Age  int `faker:"oneof: 15, 27, 61"`
}

func generateDoc(destination string) interface{} {
	docStruct := DocStruct{}
	err := faker.FakeData(&docStruct)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	doc := make(map[string]interface{})
	j, _ := json.Marshal(docStruct)

	json.Unmarshal(j, &doc)

	if destination == "Rockset" {
		doc["_id"] = guuid.New().String()
	}

	doc["_event_time"] = getCurrentTimeMicros()
	doc["generator_identifier"] = generatorIdentifier

	return doc
}

func getCurrentTimeMicros() int64 {
	t := time.Now()
	return int64(time.Nanosecond) * t.UnixNano() / int64(time.Microsecond)
}

func generateDocs(batchSize int, destination string) []interface{} {
	var docs []interface{}

	for i := 0; i < batchSize; i++ {
		docs = append(docs, generateDoc(destination))
	}

	return docs
}

func generateRandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func getEnvDefault(env string, defaultValue string) string {
	v, found := os.LookupEnv(env)
	if !found {
		return defaultValue
	}
	return v
}

func mustGetEnvString(env string) string {
	v, found := os.LookupEnv(env)
	if !found {
		log.Fatalf("env %s must be set!", env)
	}
	return v
}

func mustGetEnvInt(env string) int {
	v, found := os.LookupEnv(env)
	if !found {
		log.Fatalf("env %s must be set!", env)
	}
	ret, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("env %s is not integer!", env)
	}
	return ret
}

// launch it asynchronously, as ListenAndServe is a blocking call
func startMetricListener() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9161", nil)
	}()
}
