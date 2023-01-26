package main

import (
	"time"
)

// Destination is where to send the generated documents to
type Destination interface {
	// SendDocument sends a batch of documents to the destination.
	SendDocument(docs []any) error

	// GetLatestTimestamp get latest timestamp seen in the destination.
	GetLatestTimestamp() (time.Time, error)

	// ConfigureDestination is used to make any configuration changes to the destination that might be required for sending documents.
	ConfigureDestination() error
}
