package generator

import (
	"io"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Destination is where to send the generated documents to
type Destination interface {
	// SendDocument sends a batch of documents to the destination.
	SendDocument(docs []any) error

	// Send a batch of patches to the destination.
	SendPatch(docs []any) error

	// GetLatestTimestamp get latest timestamp seen in the destination.
	GetLatestTimestamp() (time.Time, error)

	// ConfigureDestination is used to make any configuration changes to the destination that might be required for sending documents.
	ConfigureDestination() error
}

func deferredErrorCloser(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("failed to close body: %v", err)
	}
}

func RecordE2ELatency(latency float64) {
	e2eLatencies.Set(latency)
	e2eLatenciesSummary.Observe(latency)
}

func recordWritesCompleted(count float64) {
	writesCompleted.Add(count)
}

func recordWritesErrored(count float64) {
	writesErrored.Add(count)
}

func recordPatchesCompleted(count float64) {
	patchesCompleted.Add(count)
}

func recordPatchesErrored(count float64) {
	patchesErrored.Add(count)
}

var (
	// More info can found here: https://godoc.org/github.com/prometheus/client_golang/prometheus#NewSummary
	objectiveMap = map[float64]float64{0.5: 0.05, 0.95: 0.005, 0.99: 0.001}

	writesCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "writes_completed",
		Help: "The total number of writes completed",
	})

	writesErrored = promauto.NewCounter(prometheus.CounterOpts{
		Name: "writes_errored",
		Help: "The total number of writes errored",
	})

	patchesCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "patches_completed",
		Help: "The total number of patches completed",
	})

	patchesErrored = promauto.NewCounter(prometheus.CounterOpts{
		Name: "patches_errored",
		Help: "The total number of patches errored",
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
