package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rockset/rockbench/generator"
)

func main() {
	// Seed so that values are random across replicas
	rand.Seed(time.Now().UnixNano())
	wps := mustGetEnvInt("WPS")
	batchSize := mustGetEnvInt("BATCH_SIZE")
	destination := mustGetEnvString("DESTINATION")
	numDocs := getEnvDefaultInt("NUM_DOCS", -1)
	mode := getEnvDefault("MODE", "add")
	idMode := getEnvDefault("ID_MODE", "uuid")
	patchMode := getEnvDefault("PATCH_MODE", "replace")
	exportMetrics := getEnvDefaultBool("EXPORT_METRICS", false)
	trackLatency := getEnvDefaultBool("TRACK_LATENCY", false)
	// Used to dynamically adjust the period between latency calculations to keep the number of queries roughly the same.
	// Ex. If we want 1 query per 30s and we have 2 replicas, the polling period should be 2 * 30s=60s.
	replicas := getEnvDefaultInt("REPLICAS", 2)

	if !(patchMode == "replace" || patchMode == "add") {
		panic("Invalid patch mode specified, expecting either 'replace' or 'add'")
	}
	if !(mode == "add" || mode == "patch" || mode == "add_then_patch") {
		panic("Invalid mode specified, expecting one of 'add', 'patch', 'add_then_patch'")
	}
	if !(idMode == "uuid" || idMode == "sequential") {
		panic("Invalid idMode specified, expecting 'uuid' or 'sequential'")
	}

	if mode == "patch" && idMode != "sequential" {
		panic("Patch mode supports ID_MODE `sequential` only")
	}

	if mode == "patch" && numDocs <= 0 {
		panic("Patch mode requires a positive number of docs to perform patches against. Please specify a number of documents via NUM_DOCS env var.")
	}

	pps := getEnvDefaultInt("PPS", wps)
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

	if exportMetrics {
		go metricListener()
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Kill, os.Interrupt, syscall.SIGTERM)

	var doneChan = make(chan struct{}, 1)

	go signalHandler(signalChan, doneChan)

	if trackLatency {
		go func() {
			// On average, send a request every 30s
			pollDuration := replicas * 30
			// Sleep a random amount to space requests out between each other
			sleepDuration := rand.Int31n(int32(pollDuration))
			fmt.Printf("Initial sleep of %ds and polling period of %ds\n", sleepDuration, pollDuration)
			timer := time.NewTimer(time.Duration(sleepDuration) * time.Second)
			defer timer.Stop()
			
			select {
			case <-doneChan:
				return
			case <-timer.C:
			}

			fmt.Printf("Sleep done. Now issuing requests to calculate e2e latency.\n")
			t := time.NewTicker(time.Duration(pollDuration) * time.Second)
			defer t.Stop()

			for {
				select {
				case <-doneChan:
					return
				case <-t.C:
					latestTimestamp, err := d.GetLatestTimestamp()
					now := time.Now()
					latency := now.Sub(latestTimestamp)

					if err == nil {
						fmt.Printf("Latency: %s\n", latency)
						generator.RecordE2ELatency(float64(latency.Microseconds()))
					} else {
						log.Printf("failed to get latest timestamp: %v", err)
					}
				}
			}
		}()
	}

	// Write function

	docs_written := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	if mode == "add_then_patch" || mode == "add" {
		for numDocs < 0 || docs_written < numDocs {
			select {
			// when doneChan is closed, receive immediately returns the zero value
			case <-doneChan:
				log.Printf("done")
				os.Exit(0)
			case <-t.C:
				for i := 0; i < wps; i++ {
					// TODO: move doc generation out of this loop into a go routine that pre-generates them
					docs, err := generator.GenerateDocs(batchSize, destination, generatorIdentifier, idMode)
					if err != nil {
						log.Printf("document generation failed: %v", err)
						os.Exit(1)
					}
					go func(i int) {
						if err := d.SendDocument(docs); err != nil {
							log.Printf("failed to send document batch %d of %d (wps): %v", i, wps, err)
						}
					}(i)
					docs_written = docs_written + batchSize
				}
				// TODO: this does not guarantee that the writes have finished
			}
		}
	}

	if mode == "add_then_patch" || mode == "patch" {
		if mode == "patch" {
			// must explicitly set number of docs so updates are applied evenly across document keys
			generator.SetMaxDoc(numDocs)
		}
		if destination != "Rockset" {
			panic("Patches can only be generated for Rockset at this time")
		}
		patchChannel := make(chan map[string]interface{}, 1)
		log.Printf("Sending patches in '%s' mode", patchMode)
		if patchMode == "replace" {
			go generator.RandomFieldReplace(patchChannel)
		} else {
			go generator.RandomFieldAdd(patchChannel)
		}
		for {
			select {
			// when doneChan is closed, receive immediately returns the zero value
			case <-doneChan:
				log.Printf("done")
				os.Exit(0)
			case <-t.C:
				for i := 0; i < pps; i++ {
					docs, err := generator.GeneratePatches(batchSize, patchChannel)
					if err != nil {
						log.Printf("patch generation failed: %v", err)
						os.Exit(1)
					}
					go func(i int) {
						if err := d.SendPatch(docs); err != nil {
							log.Printf("failed to send patch %d of %d: %v", i, pps, err)
						}
					}(i)
					docs_written = docs_written + batchSize
				}
			}

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

func getEnvDefaultInt(env string, defaultValue int) int {
	v, found := os.LookupEnv(env)
	if !found {
		return defaultValue
	}
	ret, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("env %s is not integer!", env)
	}
	return ret
}

func getEnvDefaultBool(env string, defaultValue bool) bool {
	v, found := os.LookupEnv(env)
	if !found {
		return defaultValue
	}

	ret, err := strconv.ParseBool(v)
	if err != nil {
		log.Fatalf("env %s is not bool!", env)
	}

	return ret
}

func getEnvDefault(env string, defaultValue string) string {
	v, found := os.LookupEnv(env)
	if !found {
		return defaultValue
	}
	return v
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
		if s == syscall.SIGTERM {
			os.Exit(0)
		}
		done = true
		close(doneChan)
	}
}
