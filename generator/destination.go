package main

import (
	"time"
)

// Destination is where to send the generated documents to
type Destination interface {
	// Send a batch of documents to the destination.
	SendDocument(docs []interface{}) error

	// Get latest timestamp seen in the destination.
	GetLatestTimestamp() (time.Time, error)
}
