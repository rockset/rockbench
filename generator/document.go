package generator

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-faker/faker/v4"
	guuid "github.com/google/uuid"
)

type DocumentSpec struct {
	Destination          string
	GeneratorIdentifier  string
	BatchSize            int
	Mode                 string
	IdMode               string
	UpdatePercentage     int
	NumClusters          int
	HotClusterPercentage int
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
	About      string   `faker:"sentence"`
	Registered string   `faker:"timestamp"`
	Tags       []string `faker:"slice_len=9,len=14"`
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
	Friend1 FriendDetailsStruct
	Friend2 FriendDetailsStruct
	Friend3 FriendDetailsStruct
	Friend4 FriendDetailsStruct
	Friend5 FriendDetailsStruct
}

type FriendDetailsStruct struct {
	Name NameStruct
	Age  int `faker:"oneof: 15, 27, 61"`
}

var doc_id = 0
var max_doc_id = 0

func GenerateDoc(spec DocumentSpec) (interface{}, error) {
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

	if spec.Mode == "mixed" {
		// Randomly choose a number to decide whether to generate a doc with an existing doc id
		if rand.Intn(100) < spec.UpdatePercentage {
			// Choose random id from one already existing doc id
			doc["_id"] = formatDocId(rand.Intn(getMaxDoc()))
		} else {
			doc["_id"] = formatDocId(getMaxDoc())
			SetMaxDoc(getMaxDoc()+1)
		}
		doc_id = doc_id + 1
		// All other modes
	} else if spec.IdMode == "uuid" {
		doc["_id"] = guuid.New().String()
	} else if spec.IdMode == "sequential" {
		doc["_id"] = formatDocId(doc_id)
		doc_id = doc_id + 1
	} else {
		panic(fmt.Sprintf("Unsupported generateDoc case: %s", spec.IdMode))
	}

	if spec.NumClusters > 0 {
		doc["cluster1"] = getClusterKey(spec.NumClusters, spec.HotClusterPercentage)
	}

	doc["_event_time"] = CurrentTimeMicros()
	// Set _ts as _event_time is not mutable
	doc["_ts"] = CurrentTimeMicros()
	doc["generator_identifier"] = spec.GeneratorIdentifier

	return doc, nil
}

func getClusterKey(numClusters int, hotClusterPercentage int) string {
 	if hotClusterPercentage > 0 && rand.Intn(100) < hotClusterPercentage {
		return "0@gmail.com"
	} else {
		return fmt.Sprintf("%d@gmail.com", rand.Intn(numClusters))
	}
}

func getMaxDoc() int {
	// doc_ids are left padded monotonic integers,
	//this returns the highest exclusive doc id for purposes of issuing patches.
	return max_doc_id
}

func SetMaxDoc(maxDocId int) {
	// doc_id = maxDocId
	max_doc_id = maxDocId
}

func CurrentTimeMicros() int64 {
	t := time.Now()
	return int64(time.Nanosecond) * t.UnixNano() / int64(time.Microsecond)
}

func GenerateDocs(spec DocumentSpec) ([]interface{}, error) {
	var docs = make([]interface{}, spec.BatchSize, spec.BatchSize)

	for i := 0; i < spec.BatchSize; i++ {
		doc, err := GenerateDoc(spec)
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

func GeneratePatches(num_patch int, destination string, c chan map[string]interface{}) ([]interface{}, error) {
	patches := make([]interface{}, 0)

	ids_to_patch := genUniqueInRange(getMaxDoc(), num_patch)
	for _, id := range ids_to_patch {
		if (destination == "elastic") {
			patch := generateElasticPatch(id, <-c)
			patches = append(patches, patch)

		} else if (destination == "rockset") {
			patch := generateRocksetPatch(id, <-c)
			patches = append(patches, patch)
		}
	}
	return patches, nil
}

func RandomFieldAdd(destination string, c chan map[string]interface{}) {
	// Adding fields or array members
	for {
		if (destination == "rockset") {
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
		} else if (destination == "elastic") {
			options := []map[string]interface{}{{
					"doc": map[string]interface{}{
						faker.UUIDDigit(): faker.Email(),
						"_ts": CurrentTimeMicros(),
					},
				},
				{
					"script": map[string]interface{}{
						"source": "ctx._source.Tags.add(params.tag)",
						"params": map[string]interface{}{
							"tag": faker.UUIDHyphenated(),
							"_ts": CurrentTimeMicros(),
						},
					},
				},
			}
			shuffleAndFillChannel(options, c)
		}
	}
}

func RandomFieldReplace(destination string, c chan map[string]interface{}) {
	// Purely replacement of fields
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		if (destination == "rockset") {
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
		} else if (destination == "elastic") {
			options := []map[string]interface{}{{
				"doc": map[string]interface{}{
					"Email": faker.Email(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"About": faker.Sentence(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"Company": faker.Word() + "-" + faker.Word(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"script": map[string]interface{}{
					"source": "ctx._source.Name.First = params.updated_nested_first_name; ctx._source._ts = params.ts",
					"params" : map[string]interface{}{
						"updated_nested_first_name" : faker.FirstName(),
						"ts": CurrentTimeMicros(),
					},
				},
			},
			{
				"script": map[string]interface{}{
					"source": "ctx._source.Name.Last = params.updated_nested_last_name; ctx._source._ts = params.ts",
					"params" : map[string]interface{}{
						"updated_nested_last_name": faker.LastName(),
						"ts": CurrentTimeMicros(),
					},
				},
			},
			{
				"doc": map[string]interface{}{
					"Age": random.Intn(100),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"Balance": random.Float64(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"Registered": faker.Timestamp(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"Phone": faker.Phonenumber(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"Picture": faker.UUIDDigit(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"Guid": faker.UUIDHyphenated(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"doc": map[string]interface{}{
					"Greeting": faker.Paragraph(),
					"_ts": CurrentTimeMicros(),
				},
			},
			{
				"script": map[string]interface{}{
					"source": "ctx._source.Address.ZipCode = params.updated_nested_zcode; ctx._source._ts = params.ts",
					"params" : map[string]interface{}{
						"updated_nested_zcode": random.Intn(100000),
						"ts": CurrentTimeMicros(),
					},
				},
			},
			{
				"script": map[string]interface{}{
					"source": "ctx._source.Address.Coordinates.Longitude = params.updated_nested_coord_long; ctx._source._ts = params.ts",
					"params" : map[string]interface{}{
						"updated_nested_coord_long": faker.Longitude(),
						"ts": CurrentTimeMicros(),
					},
				},
			},
			{
				"script": map[string]interface{}{
					"source": "ctx._source.Address.Coordinates.Latitude = params.updated_nested_coord_lat; ctx._source._ts = params.ts",
					"params" : map[string]interface{}{
						"updated_nested_coord_lat": faker.Latitude(),
						"ts": CurrentTimeMicros(),
					},
				},
			},
			{
				"script": map[string]interface{}{
					"source": "ctx._source.Address.City = params.updated_nested_city; ctx._source._ts = params.ts",
					"params" : map[string]interface{}{
						"updated_nested_city": faker.Word(),
						"ts": CurrentTimeMicros(),
					},
				},
			}}
			shuffleAndFillChannel(options, c)
		}
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
	i := 0
	for k := range ids_to_patch {
		ids[i] = k
		i++
	}
	return ids
}

func generateRocksetPatch(id int, field_patch map[string]interface{}) map[string]interface{} {
	patch := make(map[string]interface{})
	patch["_id"] = formatDocId(id)
	add_op := []map[string]interface{}{field_patch, {"op": "add", "path": "/_ts", "value": CurrentTimeMicros()}}
	patch["patch"] = add_op
	return patch
}

func generateElasticPatch(id int, field_patch map[string]interface{}) map[string]interface{} {
	patch := make(map[string]interface{})
	patch["_id"] = formatDocId(id)
	patch["patch"] = field_patch
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
