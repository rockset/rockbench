package generator

import (
	"context"
	"os"
	"testing"
	"time"
)

func crateDbDocs() ([]any, error) {
	spec := DocumentSpec{
		Destination:          "elastic",
		GeneratorIdentifier:  "1",
		BatchSize:            10,
		Mode:                 "add",
		IdMode:               "sequential",
		UpdatePercentage:     -1,
		NumClusters:          -1,
		HotClusterPercentage: -1,
	}

	docs, err := GenerateDocs(spec)
	return docs, err
}

func findMaxTimestamp(rows []any) int64 {
	var maxTimestamp int64
	for _, row := range rows {
		timestamp := row.(ma)["_event_time"].(int64)
		if timestamp > maxTimestamp {
			maxTimestamp = timestamp
		}
	}

	return maxTimestamp
}

func TestTimestampsConversion(t *testing.T) {
	var given int64 = 1720004969682695

	t1 := time.Unix(given/1_000_000, (given%1_000_000)*1_000)
	back := t1.UnixMicro()

	t2 := time.Unix(back/1_000_000, (back%1_000_000)*1_000)
	output := t2.UnixMicro()

	if given != output {
		t.Fatalf("timestamps do not match: given=%d, output=%d", given, output)
	}
}

func TestNewCrateDB(t *testing.T) {
	uri := os.Getenv("CRATEDB_URI")
	if uri == "" {
		t.Skipf(`
CRATEDB_URI not set.

To run this test, make sure you run:
  docker compose -f dev/compose.yml up -d

And then run:
  CRATEDB_URI="postgres://crate:@localhost:5432/test?pool_max_conns=10&pool_min_conns=3" go test ./... 

`)
	}

	t.Logf("CRATEDB_URI: %s", uri)

	c, err := NewCrateDB(context.Background(), uri)
	if err != nil {
		t.Fatalf("NewCrateDB() error = %v", err)
	}

	if c == nil {
		t.Fatalf("NewCrateDB() = nil")
	}

	err = c.Reset(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Reset() error = %v", err)
	}

	err = c.Init(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Init() error = %v", err)
	}

	docs, err := crateDbDocs()
	if err != nil {
		t.Fatalf("crateDbDocs() error = %v", err)
	}

	err = c.SendDocument(docs)
	if err != nil {
		t.Fatalf("CrateDB.SendDocument() error = %v", err)
	}

	maxTimestamp := findMaxTimestamp(docs)

	timestamp, err := c.GetLatestTimestamp()
	if err != nil {
		t.Fatalf("CrateDB.GetLatestTimestamp() error = %v", err)
	}

	delta := maxTimestamp - timestamp.UnixMicro()
	epsilon := int64(100)
	if delta > epsilon {
		t.Errorf("CrateDB.GetLatestTimestamp() delta is %d, cannot be bigger than %d", delta, epsilon)
	}

	t.Logf("CrateDB.GetLatestTimestamp() = %s", timestamp.String())
	t.Logf("   timestamp = %d", timestamp.UnixMicro())
	t.Logf("maxTimestamp = %d", maxTimestamp)

	err = c.Reset(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Reset() error = %v", err)
	}

	err = c.Close(context.Background())
	if err != nil {
		t.Fatalf("CrateDB.Close() error = %v", err)
	}
}
