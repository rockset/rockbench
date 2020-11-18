package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

const defaultRocksetEndpoint = "https://api.rs2.usw2.rockset.com"

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

	switch strings.ToLower(destination) {
	case "rockset":
		apiKey := mustGetEnvString("ROCKSET_API_KEY")
		apiServer := getEnvDefault("ROCKSET_API_SERVER", defaultRocksetEndpoint)
		collection := mustGetEnvString("ROCKSET_COLLECTION")

		d = &Rockset{
			apiKey:              apiKey,
			apiServer:           apiServer,
			collection:          collection,
			client:              client,
			generatorIdentifier: generatorIdentifier,
		}
	case "elastic":
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
	case "snowflake":
		account := mustGetEnvString("SNOWFLAKE_ACCOUNT")
		user := mustGetEnvString("SNOWFLAKE_USER")
		password := mustGetEnvString("SNOWFLAKE_PASSWORD")
		warehouse := mustGetEnvString("SNOWFLAKE_WAREHOUSE")
		database := mustGetEnvString("SNOWFLAKE_DATABASE")
		stageS3Bucket := mustGetEnvString("SNOWFLAKE_STAGES3BUCKETNAME")
		awsRegion := mustGetEnvString("AWS_REGION")
		d = &Snowflake{
			account:             account,
			user:                user,
			password:            password,
			warehouse:           warehouse,
			database:            database,
			generatorIdentifier: generatorIdentifier,
			stageS3BucketName:   stageS3Bucket,
			awsRegion:           awsRegion,
			schema:              "PUBLIC",
		}
		snowFlake := d.(*Snowflake)
		configErr := snowFlake.ConfigureDestination()
		if configErr != nil {
			log.Fatal("Unable to configure snowflake for sending documents: ", configErr)
		}
	case "null":
		d = &Null{}
	default:
		log.Fatal("Unsupported destination. Supported options are Rockset, Elastic & Null")
	}

	go metricListener()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Kill, os.Interrupt)

	var doneChan = make(chan struct{}, 1)

	go signalHandler(signalChan, doneChan)

	// Periodically read number of docs and log to output
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-doneChan:
				return
			case <-t.C:
				now := time.Now()
				latestTimestamp, err := d.GetLatestTimestamp()
				latency := now.Sub(latestTimestamp)

				if err == nil {
					fmt.Printf("Latency: %s\n", latency)
					recordE2ELatency(float64(latency.Microseconds()))
				} else {
					log.Printf("failed to get latest timespamp: %v", err)
				}
			}
		}
	}()

	// Write function
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		// when doneChan is closed, receive immediately returns the zero value
		case <-doneChan:
			log.Printf("done")
			os.Exit(0)
		case <-t.C:
			for i := 0; i < wps; i++ {
				// TODO: move doc generation out of this loop into a go routine that pre-generates them
				docs, err := generateDocs(batchSize, destination)
				if err != nil {
					log.Printf("document generation failed: %v", err)
					os.Exit(1)
				}
				go func(i int) {
					if err := d.SendDocument(docs); err != nil {
						log.Printf("failed to send document %d of %d: %v", i, wps, err)
					}
				}(i)
			}
			// TODO: this does not guarantee that the writes have finished
		}
	}
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

func generateDoc(destination string) (interface{}, error) {
	docStruct := DocStruct{}
	err := faker.FakeData(&docStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to generate fake document: %w", err)
	}

	doc := make(map[string]interface{})
	j, _ := json.Marshal(docStruct)

	if err = json.Unmarshal(j, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal document: %w", err)
	}

	if destination == "Rockset" {
		doc["_id"] = guuid.New().String()
	}

	doc["_event_time"] = getCurrentTimeMicros()
	doc["generator_identifier"] = generatorIdentifier

	return doc, nil
}

func getCurrentTimeMicros() int64 {
	t := time.Now()
	return int64(time.Nanosecond) * t.UnixNano() / int64(time.Microsecond)
}

func generateDocs(batchSize int, destination string) ([]interface{}, error) {
	var docs = make([]interface{}, batchSize, batchSize)

	for i := 0; i < batchSize; i++ {
		doc, err := generateDoc(destination)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}

	return docs, nil
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

// metricListener needs to be launched asynchronously, as ListenAndServe is a blocking call
func metricListener() {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":9161", nil)
	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("failed to start metrics listener: %v", err)
	}
}

func deferredErrorCloser(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("failed to close body: %v", err)
	}
}

func signalHandler(signalChan chan os.Signal, doneChan chan struct{}) {
	done := false
	for {
		s := <-signalChan
		if done {
			fmt.Printf("\nsecond signal received (%s), exiting\n", s)
			os.Exit(1)
		}
		fmt.Printf("\nsignal received: %s\n", s)
		done = true
		close(doneChan)
	}
}
