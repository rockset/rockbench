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
)

var (
	generatorIdentifier string
)

func main() {
	apiKey := mustGetEnvString("ROCKSET_API_KEY")
	apiServer := getEnvDefault("ROCKSET_API_SERVER", "https://api.rs2.usw2.rockset.com")
	wps := mustGetEnvInt("WPS")
	collection := mustGetEnvString("ROCKSET_COLLECTION")
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

	// TODO: Add more types of destination
	var d Destination

	if destination == "Rockset" {
		d = &Rockset{
			apiKey:              apiKey,
			apiServer:           apiServer,
			collection:          collection,
			client:              client,
			generatorIdentifier: generatorIdentifier,
		}
	} else {
		log.Fatal("Unsupported destination. Only supported one is Rockset.")
	}

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

	generatorIdentifier = generateRandomString(10)
	fmt.Println("Generator identifier: ", generatorIdentifier)

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
			docs := generateDocs(batchSize)
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

func generateDoc() interface{} {
	docStruct := DocStruct{}
	err := faker.FakeData(&docStruct)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	doc := make(map[string]interface{})
	j, _ := json.Marshal(docStruct)

	json.Unmarshal(j, &doc)

	doc["_id"] = strconv.Itoa(rand.Intn(2000000000))
	doc["_event_time"] = getCurrentTimeMicros()
	doc["generator_identifier"] = generatorIdentifier

	return doc
}

func getCurrentTimeMicros() int64 {
	t := time.Now()
	return int64(time.Nanosecond) * t.UnixNano() / int64(time.Microsecond)
}

func generateDocs(batchSize int) []interface{} {
	var docs []interface{}

	for i := 0; i < batchSize; i++ {
		docs = append(docs, generateDoc())
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
