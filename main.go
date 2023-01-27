package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rockset/rockbench/generator"
)

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

	generatorIdentifier := generator.RandomString(10)
	fmt.Println("Generator identifier: ", generatorIdentifier)

	var d generator.Destination

	switch strings.ToLower(destination) {
	case "rockset":
		apiKey := mustGetEnvString("ROCKSET_API_KEY")
		apiServer := mustGetEnvString("ROCKSET_API_SERVER")
		collectionPath := mustGetEnvString("ROCKSET_COLLECTION")

		rcollection := strings.Split(collectionPath, ".")
		if len(rcollection) != 2 {
			panic(fmt.Sprintf("rockset collection path should have the format <workspace_name>.<collection_name>"))
		}

		d = &generator.Rockset{
			APIKey:              apiKey,
			APIServer:           apiServer,
			CollectionPath:      collectionPath,
			Client:              client,
			GeneratorIdentifier: generatorIdentifier,
		}
	case "elastic":
		esAuth := mustGetEnvString("ELASTIC_AUTH")
		esURL := mustGetEnvString("ELASTIC_URL")
		esIndexName := mustGetEnvString("ELASTIC_INDEX")

		d = &generator.Elastic{
			Auth:                esAuth,
			URL:                 esURL,
			IndexName:           esIndexName,
			Client:              client,
			GeneratorIdentifier: generatorIdentifier,
		}
	case "snowflake":
		account := mustGetEnvString("SNOWFLAKE_ACCOUNT")
		user := mustGetEnvString("SNOWFLAKE_USER")
		password := mustGetEnvString("SNOWFLAKE_PASSWORD")
		warehouse := mustGetEnvString("SNOWFLAKE_WAREHOUSE")
		database := mustGetEnvString("SNOWFLAKE_DATABASE")
		stageS3Bucket := mustGetEnvString("SNOWFLAKE_STAGES3BUCKETNAME")
		awsRegion := mustGetEnvString("AWS_REGION")
		d = &generator.Snowflake{
			Account:             account,
			User:                user,
			Password:            password,
			Warehouse:           warehouse,
			Database:            database,
			GeneratorIdentifier: generatorIdentifier,
			StageS3BucketName:   stageS3Bucket,
			AWSRegion:           awsRegion,
			Schema:              "PUBLIC",
		}
		configErr := d.ConfigureDestination()
		if configErr != nil {
			log.Fatal("Unable to configure snowflake for sending documents: ", configErr)
		}
	case "null":
		d = &generator.Null{}
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
					generator.RecordE2ELatency(float64(latency.Microseconds()))
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
				docs, err := generator.GenerateDocs(batchSize, destination, generatorIdentifier)
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