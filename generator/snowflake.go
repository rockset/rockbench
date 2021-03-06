package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	snowflake "github.com/snowflakedb/gosnowflake"
)

// Snowflake contains all configurations needed to send documents to Snowflake
type Snowflake struct {
	account             string
	user                string
	password            string
	warehouse           string
	database            string
	schema              string
	generatorIdentifier string
	stageS3BucketName   string
	awsRegion           string
	table               string
	dbConnection        *sql.DB
}

// Snowflake has concept of stage & pipe:
//   Stage is a area where data is written by a client before it is loaded to a snowflake table.
//   Snowpipe (pipe) is a service which allows for bulk ingestion of data from stage to snowflake tables.
// The Approach rockbench uses for executing benchmark tests on snowflake:
//    It uses an AWS S3 bucket as stage and writes data to it.
//    It configures S3 bucket to trigger snowpipe to load data into snowflake table as soon as it is written to stage (s3 bucket).

// SendDocument sends a batch of documents to Snowflake
func (r *Snowflake) SendDocument(docs []interface{}) error {
	numDocs := len(docs)
	numEventIngested.Add(float64(numDocs))

	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
		})

	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Region:      &r.awsRegion,
	})
	if err != nil {
		recordWritesErrored(float64(numDocs))
		return fmt.Errorf("failed to create a session with AWS, %v", err)
	}

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	body := map[string][]interface{}{"data": docs}
	jsonBody, _ := json.Marshal(body)
	data := bytes.NewReader(jsonBody)

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(r.stageS3BucketName),
		Key:    aws.String(time.Now().String()),
		Body:   data,
	})
	if err != nil {
		recordWritesErrored(float64(numDocs))
		return fmt.Errorf("failed to upload file, %v", err)
	}
	fmt.Printf("file uploaded to, %s\n", aws.StringValue(&result.Location))
	recordWritesCompleted(float64(numDocs))
	return nil
}

// GetLatestTimestamp returns the latest _event_time in Snowflake
func (r *Snowflake) GetLatestTimestamp() (time.Time, error) {

	getLatestTimeStampQuery := "select JSONTEXT:data[0]._event_time AS unixtime from " + r.table + " where JSONTEXT:data[0].generator_identifier = '" + r.generatorIdentifier + "' ORDER BY JSONTEXT:data[0]._event_time DESC limit 1"
	rows, err := r.dbConnection.Query(getLatestTimeStampQuery)
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

	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
		})
	// get AWS_KEY_ID & AWS_SECRET_KEY from aws credentials
	credValue, err := creds.Get()
	if err != nil {
		return fmt.Errorf("failed to retrieve AWS credentials, %v", err)
	}
	snowflakeConfig := &snowflake.Config{
		Account:   r.account,
		User:      r.user,
		Password:  r.password,
		Database:  r.database,
		Warehouse: r.warehouse,
		Schema:    r.schema,
	}

	// create DSN for snowflake
	dsn, err := snowflake.DSN(snowflakeConfig)

	if err != nil {
		return fmt.Errorf("failed to create DSN to connect snowflake: %w", err)
	}

	// open a connection with snowflake
	r.dbConnection, err = sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to open a connection with snowflake: %w", err)
	}

	// create stage
	stageName := "perfstage" + r.generatorIdentifier
	createStageQuery := "create stage " + stageName + " url='s3://" + r.stageS3BucketName + "' credentials = (AWS_KEY_ID = '" + credValue.AccessKeyID + "' AWS_SECRET_KEY = '" + credValue.SecretAccessKey + "' );"
	_, err = r.dbConnection.Query(createStageQuery)
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", createStageQuery, err)
	}
	fmt.Println("created a stage named: ", stageName)

	// create table
	tableName := "perftable" + r.generatorIdentifier
	createTableQuery := "create table " + tableName + " ( jsontext variant );"
	_, err = r.dbConnection.Query(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", createTableQuery, err)
	}
	fmt.Println("created a table named: ", tableName)
	r.table = tableName

	// create pipe which will ingest data from s3 to snowflake table
	pipeName := "perfpipe" + r.generatorIdentifier
	createPipeQuery := "create pipe " + pipeName + " auto_ingest=true as copy into " + tableName + " from @" + stageName + " file_format = (type = 'JSON');"
	_, err = r.dbConnection.Query(createPipeQuery)
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", createPipeQuery, err)
	}
	fmt.Println("created a pipe named: ", pipeName)

	// get the list of pipes and extract the notification channel for the pipe we created earlier
	showPipeQuery := "show pipes"
	rows, err := r.dbConnection.Query(showPipeQuery)
	if err != nil {
		return fmt.Errorf("failed to run a query. %v, err: %v", showPipeQuery, err)
	}
	var created_on, name, database_name, schema_name, owner, notification_channel, comment, notificationChannel string
	var definition, integration, pattern sql.NullString
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Printf("failed to close rows: %v", err)
		}
	}()
	for rows.Next() {
		err := rows.Scan(&created_on, &name, &database_name, &schema_name, &definition, &owner, &notification_channel, &comment, &integration, &pattern)
		if err != nil {
			return fmt.Errorf("failed to scan row to get notification channel info, err: %v", err)
		}
		if strings.ToLower(name) == strings.ToLower(pipeName) {
			notificationChannel = notification_channel
			break
		}
	}
	// create an AWSsession to configure s3 bucket used in stage
	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Region:      &r.awsRegion,
	})
	if err != nil {
		return fmt.Errorf("failed to create a session with AWS, %v", err)
	}

	svc := s3.New(sess)
	input := &s3.PutBucketNotificationConfigurationInput{
		Bucket: &r.stageS3BucketName,
		NotificationConfiguration: &s3.NotificationConfiguration{
			QueueConfigurations: []*s3.QueueConfiguration{
				{
					Id: aws.String("snowflake-notifications"),
					Events: []*string{
						aws.String("s3:ObjectCreated:*"),
					},
					QueueArn: aws.String(notificationChannel),
				},
			},
		},
	}
	// configure s3 bucket to send notification to notification channel of the snowpipe on every object create event
	_, err = svc.PutBucketNotificationConfiguration(input)
	if err != nil {
		return fmt.Errorf("failed to configure notfication on stage s3 bucket, %v", err)
	}
	fmt.Println("created event notification on ", r.stageS3BucketName)

	return nil
}
