package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewCrateDB(ctx context.Context, url string) (*CrateDB, error) {
	conn, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("cratedb:NewCrateDB: pool; %v", err)
	}

	conn2, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("cratedb:NewCrateDB: single; %v", err)
	}

	return &CrateDB{
		conn:  conn,
		conn2: conn2,
	}, nil
}

var _ Destination = (*CrateDB)(nil)

type ma = map[string]any

// CrateDB contains all configurations needed to send documents to CrateDB
type CrateDB struct {
	conn  *pgxpool.Pool
	conn2 *pgx.Conn
}

func (c *CrateDB) Init(ctx context.Context) error {
	table := `CREATE TABLE IF NOT EXISTS test (doc OBJECT)`

	_, err := c.conn.Exec(ctx, table)
	if err != nil {
		return fmt.Errorf("cratedb:Init: %v", err)
	}

	return nil
}

func (c *CrateDB) Reset(ctx context.Context) error {
	_, err := c.conn.Exec(ctx, "DROP TABLE IF EXISTS test")
	if err != nil {
		return fmt.Errorf("cratedb:Reset: %v", err)
	}

	return nil
}

func (c *CrateDB) SendDocument(docs []any) error {
	b := &pgx.Batch{}
	for _, doc := range docs {
		insert := `INSERT INTO test (doc) VALUES ($1);`

		//doc.(ma)["event_time"] = doc.(ma)["_event_time"]
		//doc.(ma)["ts"] = doc.(ma)["_ts"]
		//doc.(ma)["id"] = doc.(ma)["_id"]

		data, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("cratedb:SendDocument: %v", err)
		}
		b.Queue(insert, data)
	}

	br := c.conn.SendBatch(context.Background(), b)
	_, err := br.Exec()
	if err != nil {
		return fmt.Errorf("cratedb:SendDocument: exec; %v", err)
	}

	err = br.Close()
	if err != nil {
		return fmt.Errorf("cratedb:SendDocument: close; %v", err)
	}

	return nil
}

func (c *CrateDB) SendPatch(docs []any) error {
	//TODO implement me
	panic("implement me")
}

func (c *CrateDB) GetLatestTimestamp() (time.Time, error) {
	ctx := context.Background()
	_, err := c.conn.Exec(ctx, `REFRESH TABLE test;`)
	if err != nil {
		return time.Time{}, fmt.Errorf("cratedb:GetLatestTimestamp: %v", err)
	}

	query := `SELECT MAX(doc['_event_time'])::TIMESTAMP FROM test;`

	var ts time.Time
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return time.Time{}, fmt.Errorf("cratedb:GetLatestTimestamp: query; %v", err)
	}
	if !rows.Next() {
		return time.Time{}, fmt.Errorf("cratedb:GetLatestTimestamp: no rows")
	}
	err = rows.Scan(&ts)
	if err != nil {
		return time.Time{}, fmt.Errorf("cratedb:GetLatestTimestamp: scan; %v", err)
	}
	rows.Close()

	tsv := ts.UnixMicro()

	return time.Unix(0, tsv), nil
}

func (c *CrateDB) ConfigureDestination() error {
	//TODO implement me
	panic("implement me")
}

func (c *CrateDB) Close(ctx context.Context) error {
	c.conn.Close()
	return nil
}
