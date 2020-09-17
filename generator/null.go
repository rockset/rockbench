package main

import "time"

// Null destination for local testing
type Null struct{}

func (n *Null) SendDocument(docs []interface{}) error {
	recordWritesCompleted(float64(len(docs)))
	return nil
}

func (n *Null) GetLatestTimestamp() (time.Time, error) {
	return time.Now().Add(-10*time.Millisecond), nil
}