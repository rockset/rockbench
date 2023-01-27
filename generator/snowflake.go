package generator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"log"
	"strconv"
	"strings"
	"time"

	snowflake "github.com/snowflakedb/gosnowflake"
)

// Snowflake contains all configurations needed to send documents to Snowflake
type Snowflake struct {
	Account             string
	User                string
	Password            string
	Warehouse           string
	Database            string
	Schema              string
	GeneratorIdentifier string
	StageS3BucketName   string
	AWSRegion           string
	Table               string
	DBConnection        *sql.DB
}

// Snowflake has concept of stage & pipe:
//   Stage is a area where data is written by a client before it is loaded to a snowflake table.
//   Snowpipe (pipe) is a service which allows for bulk ingestion of data from stage to snowflake tables.
// The Approach rockbench uses for executing benchmark tests on snowflake:
//    It uses an AWS S3 bucket as stage and writes data to it.
//    It configures S3 bucket to trigger snowpipe to load data into snowflake table as soon as it is written to stage (s3 bucket).

// SendDocument sends a batch of documents to Snowflake
func (r *Snowflake) SendDocument(docs []any) error {
	ctx := context.TODO()
	numDocs := len(docs)
	numEventIngested.Add(float64(numDocs))

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(r.AWSRegion))
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %v", err)
	}

	// Create an uploader with the session and default options
	uploader := manager.NewUploader(s3.NewFromConfig(cfg))

	body := map[string][]interface{}{"data": docs}
	jsonBody, _ := json.Marshal(body)
	data := bytes.NewReader(jsonBody)

	// Upload the file to S3.
	result, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &r.StageS3BucketName,
		Key:    aws.String(time.Now().String()),
		Body:   data,
	})
	if err != nil {
		recordWritesErrored(float64(numDocs))
		return fmt.Errorf("failed to upload file, %v", err)
	}
	fmt.Printf("file uploaded to, %s\n", result.Location)
	recordWritesCompleted(float64(numDocs))

	return nil
}

// GetLatestTimestamp returns the latest _event_time in Snowflake
func (r *Snowflake) GetLatestTimestamp() (time.Time, error) {

	getLatestTimeStampQuery := "select JSONTEXT:data[0]._event_time AS unixtime from " + r.Table + " where JSONTEXT:data[0].generator_identifier = '" + r.GeneratorIdentifier + "' ORDER BY JSONTEXT:data[0]._event_time DESC limit 1"
	rows, err := r.DBConnection.Query(getLatestTimeStampQuery)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to run a query. %v, err: %v", getLatestTimeStampQuery, err)
	}
	var unixtime interface{}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Printf("failed to close rows: %v", err)
		}
	}()
	for rows.Next() {
		err := rows.Scan(&unixtime)
		if err != nil {
			return time.Time{}, fmt.Errorf("could not find the document %v", err)
		}
	}
	if unixtime != nil {
		unixtimeFloat, err := strconv.ParseFloat(unixtime.(string), 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("could not convert unixtime from string to float64 %w", err)
		}
		timeMicro := int64(unixtimeFloat)
		// Convert from microseconds to (secs, nanosecs)
		return time.Unix(timeMicro/1_000_000, (timeMicro%1_000_000)*1000), nil
	}
	return time.Time{}, errors.New("malformed result, value is nil")

}

// ConfigureDestination is used to make configuration changes to the Snowflake instance for sending documents.
func (r *Snowflake) ConfigureDestination() error {
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(r.AWSRegion))
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %v", err)
	}
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("unable retrieve credentials, %v", err)
	}

	snowflakeConfig := &snowflake.Config{
		Account:   r.Account,
		User:      r.User,
		Password:  r.Password,
		Database:  r.Database,
		Warehouse: r.Warehouse,
		Schema:    r.Schema,
	}

	// create DSN for snowflake
	dsn, err := snowflake.DSN(snowflakeConfig)

	if err != nil {
		return fmt.Errorf("failed to create DSN to connect snowflake: %w", err)
	}

	// open a connection with snowflake
	r.DBConnection, err = sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to open a connection with snowflake: %w", err)
	}

	// create stage
	stageName := "perfstage" + r.GeneratorIdentifier
	createStageQuery := "create stage " + stageName + " url='s3://" + r.StageS3BucketName + "' credentials = (AWS_KEY_ID = '" + creds.AccessKeyID + "' AWS_SECRET_KEY = '" + creds.SecretAccessKey + "' );"
	_, err = r.DBConnection.Query(createStageQuery)

	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", createStageQuery, err)
	}
	fmt.Println("created a stage named: ", stageName)

	// create table
	tableName := "perftable" + r.GeneratorIdentifier
	createTableQuery := "create table " + tableName + " ( jsontext variant );"
	_, err = r.DBConnection.Query(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", createTableQuery, err)
	}
	fmt.Println("created a table named: ", tableName)
	r.Table = tableName

	// create pipe which will ingest data from s3 to snowflake table
	pipeName := "perfpipe" + r.GeneratorIdentifier
	createPipeQuery := "create pipe " + pipeName + " auto_ingest=true as copy into " + tableName + " from @" + stageName + " file_format = (type = 'JSON');"
	_, err = r.DBConnection.Query(createPipeQuery)
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", createPipeQuery, err)
	}
	fmt.Println("created a pipe named: ", pipeName)

	// get the list of pipes and extract the notification channel for the pipe we created earlier
	showPipeQuery := "show pipes"
	rows, err := r.DBConnection.Query(showPipeQuery)
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", showPipeQuery, err)
	}
	var createdOn, name, databaseName, schemaName, owner, channel, comment, notificationChannel string
	var definition, integration, pattern sql.NullString
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Printf("failed to close rows: %v", err)
		}
	}()
	for rows.Next() {
		err := rows.Scan(&createdOn, &name, &databaseName, &schemaName, &definition, &owner, &channel, &comment, &integration, &pattern)
		if err != nil {
			return fmt.Errorf("failed to scan row to get notification channel info, err: %v", err)
		}
		if strings.ToLower(name) == strings.ToLower(pipeName) {
			notificationChannel = channel
			break
		}
	}
	// create an AWS session to configure s3 bucket used in stage
	svc := s3.NewFromConfig(cfg)
	input := &s3.PutBucketNotificationConfigurationInput{
		Bucket: &r.StageS3BucketName,
		NotificationConfiguration: &types.NotificationConfiguration{
			QueueConfigurations: []types.QueueConfiguration{
				{
					Id:       aws.String("snowflake-notifications"),
					Events:   []types.Event{"s3:ObjectCreated:*"},
					QueueArn: aws.String(notificationChannel),
				},
			},
		},
	}
	// configure s3 bucket to send notification to notification channel of the snowpipe on every object create event
	_, err = svc.PutBucketNotificationConfiguration(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to configure notfication on stage s3 bucket, %v", err)
	}
	fmt.Println("created event notification on ", r.StageS3BucketName)

	return nil
}
