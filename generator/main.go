package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
	apiKey, _ := os.LookupEnv("TEST_API_KEY")
	apiServer, _ := os.LookupEnv("TEST_API_SERVER")
	qpsStr, _ := os.LookupEnv("TEST_WPS")
	collection, _ := os.LookupEnv("TEST_COLLECTION")

	wps, err := strconv.Atoi(qpsStr)
	if err != nil {
		log.Fatal("Invalid wps", qpsStr)
	}
	batch, found := os.LookupEnv("TEST_BATCH")
	if !found {
		log.Fatal("Must specify TEST_BATCH env var")
	}

	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("defaultRoundTripper not an *http.Transport"))
	}
	defaultTransport := *defaultTransportPointer
	defaultTransport.MaxIdleConns = 100
	defaultTransport.MaxIdleConnsPerHost = 100
	client := &http.Client{Transport: &defaultTransport}

	// Delete collection on cleanup.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGKILL)
	exitChan := make(chan int)

	var done = false
	go func() {
		// Delete the collection once receiving signal from k8s to stop this pod.
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

	url := fmt.Sprintf("%s/v1/orgs/self/ws/commons/collections/%s/docs", apiServer, collection)

	// Periodically read number of docs and log to output
	go func() {
		for {
			if done {
				break
			}
			getLatencyRockset(client, collection, apiServer, apiKey)

			time.Sleep(30 * time.Second)
		}
	}()

	batchSize, _ := strconv.Atoi(batch)
	// Write function
	for {
		if done {
			break
		}

		iterationStart := time.Now()
		for i := 0; i < wps; i++ {
			docs := generateDocs(batchSize)
			go sendDocuments(client, url, apiKey, docs)
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

func sendDocuments(client *http.Client, url string, apiKey string, docs []interface{}) {
	body := map[string][]interface{}{"data": docs}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBody))
	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", apiKey))
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error during request!", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
	} else {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			fmt.Printf("Error code: %d, body: %s \n", resp.StatusCode, bodyString)
		}
	}
}

func getLatencyRockset(client *http.Client, collection string, apiServer string, apiKey string) {
	url := fmt.Sprintf("%s/v1/orgs/self/queries", apiServer)
	query := fmt.Sprintf("select UNIX_MICROS(_event_time) from %s where generator_identifier = '%s' ORDER BY _event_time DESC limit 1", collection, generatorIdentifier)
	body := map[string]interface{}{"sql": map[string]interface{}{"query": query}}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonBody))
	req.Header.Add("Authorization", fmt.Sprintf("ApiKey %s", apiKey))
	req.Header.Add("Content-Type", "application/json")

	now := getCurrentTimeMicros()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error during request!\n", err)
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString := string(bodyBytes)
			fmt.Printf("Error code: %d, body: %s \n", resp.StatusCode, bodyString)
		}
		return
	} else {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		var result map[string]interface{}
		json.Unmarshal(bodyBytes, &result)
		// Below is pretty sad :(
		x := result["results"].([]interface{})
		if len(x) == 0 {
			return
		}

		x0 := x[0]
		y := x0.(map[string]interface{})
		yc := y["?UNIX_MICROS"]
		if yc == nil {
			return
		}

		latency := float64(now) - yc.(float64)
		if latency < 0 {
			// use query elapsed time as e2e latency
			end := getCurrentTimeMicros()
			latency = float64(end) - float64(now)
		}
		fmt.Printf("latency: %f\n", latency)
		return
	}
}

func generateRandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
