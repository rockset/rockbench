package generator

import (
	"encoding/json"
	"fmt"
	"github.com/go-faker/faker/v4"
	guuid "github.com/google/uuid"
	"math/rand"
	"time"
)

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

func GenerateDoc(destination, identifier string) (interface{}, error) {
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

	doc["_event_time"] = CurrentTimeMicros()
	doc["generator_identifier"] = identifier

	return doc, nil
}

func CurrentTimeMicros() int64 {
	t := time.Now()
	return int64(time.Nanosecond) * t.UnixNano() / int64(time.Microsecond)
}

func GenerateDocs(batchSize int, destination, identifier string) ([]interface{}, error) {
	var docs = make([]interface{}, batchSize, batchSize)

	for i := 0; i < batchSize; i++ {
		doc, err := GenerateDoc(destination, identifier)
		if err != nil {
			return nil, err
		}
		docs[i] = doc
	}

	return docs, nil
}

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
