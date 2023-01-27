package generator

import "time"

// Null destination for local testing
type Null struct{}

func (n *Null) SendDocument(docs []any) error {

	recordWritesCompleted(float64(len(docs)))
	return nil
}

func (n *Null) SendPatch(docs []interface{}) error {
	return nil
}

func (n *Null) GetLatestTimestamp() (time.Time, error) {
	return time.Now().Add(-10 * time.Millisecond), nil
}

func (n *Null) ConfigureDestination() error {
	return nil
}
