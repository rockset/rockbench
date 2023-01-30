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
	Tags       []string `faker:"slice_len=10,len=7"`
	Friends    FriendStruct
	Greeting   string `faker:"sentence"`
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
}

type FriendDetailsStruct struct {
	Name NameStruct
	Age  int `faker:"oneof: 15, 27, 61"`
}

var doc_id = 0

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
		doc_id = doc_id + 1
	}

	doc["_event_time"] = CurrentTimeMicros()
	// Set _ts as _event_time is not mutable
	doc["_ts"] = CurrentTimeMicros()
	doc["generator_identifier"] = identifier

	return doc, nil
}

func getMaxDoc() int {
	// doc_ids are left padded monotonic integers,
	//this returns the highest exclusive doc id for purposes of issuing patches.
	return doc_id
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

func GeneratePatches(num_patch int, c chan map[string]interface{}) ([]interface{}, error) {
	ids_to_patch := genUniqueInRange(getMaxDoc(), num_patch)
	patches := make([]interface{}, 0)

	for _, id := range ids_to_patch {
		patch := generatePatch(id, <-c)
		patches = append(patches, patch)
	}

	return patches, nil
}

func RandomFieldAdd(c chan map[string]interface{}) {
	// Adding fields or array members
	for {
		options := []map[string]interface{}{{
			"op":    "add",
			"path":  "/" + faker.UUIDDigit(),
			"value": faker.Email(),
		},
			{
				"op":    "add",
				"path":  "/Tags/-",
				"value": faker.UUIDHyphenated(), // Append to tags array
			},
		}
		shuffleAndFillChannel(options, c)
	}

}

func RandomFieldReplace(c chan map[string]interface{}) {
	// Purely replacement of fields
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		options := []map[string]interface{}{{
			"op":    "replace",
			"path":  "/Email",
			"value": faker.Email(),
		},
			{
				"op":    "replace",
				"path":  "/About",
				"value": faker.Sentence(),
			},
			{
				"op":    "replace",
				"path":  "/Company",
				"value": faker.Word() + "-" + faker.Word(),
			},
			{
				"op":    "replace",
				"path":  "/Name/First",
				"value": faker.FirstName(),
			},
			{
				"op":    "replace",
				"path":  "/Name/Last",
				"value": faker.LastName(),
			},
			{
				"op":    "replace",
				"path":  "/Age",
				"value": random.Intn(100),
			},
			{
				"op":    "replace",
				"path":  "/Balance",
				"value": random.Float64(),
			},
			{
				"op":    "replace",
				"path":  "/Registered",
				"value": faker.Timestamp(),
			},
			{
				"op":    "replace",
				"path":  "/Phone",
				"value": faker.Phonenumber(),
			},
			{
				"op":    "replace",
				"path":  "/Picture",
				"value": faker.UUIDDigit(),
			},
			{
				"op":    "replace",
				"path":  "/Guid",
				"value": faker.UUIDHyphenated(),
			},
			{
				"op":    "replace",
				"path":  "/Greeting",
				"value": faker.Paragraph(),
			},
			{
				"op":    "replace",
				"path":  "/Address/ZipCode",
				"value": random.Intn(100000),
			},
			{
				"op":    "replace",
				"path":  "/Address/Coordinates/Longitude",
				"value": faker.Longitude(),
			},
			{
				"op":    "replace",
				"path":  "/Address/Coordinates/Latitude",
				"value": faker.Latitude(),
			},
			{
				"op":    "replace",
				"path":  "/Address/City",
				"value": faker.Word(),
			}}
		shuffleAndFillChannel(options, c)
	}
}

func genUniqueInRange(limit int, count int) []int {
	random := rand.New(rand.NewSource(CurrentTimeMicros()))
	ids_to_patch := make(map[int]struct{}, count)
	for len(ids_to_patch) < count {
		id := random.Intn(limit)
		_, exists := ids_to_patch[id]
		if !exists {
			ids_to_patch[id] = struct{}{}
		}
	}

	ids := make([]int, count)
	for k, _ := range ids_to_patch {
		ids = append(ids, k)
	}
	return ids
}

func generatePatch(id int, field_patch map[string]interface{}) map[string]interface{} {
	patch := make(map[string]interface{})
	patch["_id"] = formatDocId(id)
	add_op := []map[string]interface{}{field_patch, {"op": "add", "path": "/_ts", "value": CurrentTimeMicros()}}
	patch["patch"] = add_op
	return patch
}

func shuffleAndFillChannel(options []map[string]interface{}, c chan map[string]interface{}) {
	rand.Shuffle(len(options), func(i, j int) {
		options[i], options[j] = options[j], options[i]
	})
	for _, op := range options {
		c <- op

	}
}

func formatDocId(id int) string {
	return fmt.Sprintf("%024d", id)
}
