# RockBench
Benchmark used to measure ingest throughput of a realtime database.

## Usage
You can run this directly, or through Docker container.
* Clone the repository
```
git clone https://github.com/rockset/rockbench.git
cd rockbench/generator
```
* To run directly
```
# Build
go build

# Send to Rockset
ROCKSET_API_KEY=xxxx ROCKSET_COLLECTION=yyyy WPS=1 BATCH_SIZE=1 DESTINATION=Rockset ./generator

# Send to Elastic
ELASTIC_AUTH=xxxx ELASTIC_URL=https://... ELASTIC_INDEX=index_name WPS=1 BATCH_SIZE=1 DESTINATION=Elastic ./generator
```

* To run with Docker container
```
docker build -t data_generator .
docker run -e [env variable as above] data_generator
```

## Extend to use other databases

Implement the [Destination](https://github.com/rockset/rockbench/blob/master/generator/destination.go) interface and provide the appropriate configs required. Check [Rockset](https://github.com/rockset/rockbench/blob/master/generator/rockset.go) and [Elastic](https://github.com/rockset/rockbench/blob/master/generator/elastic.go) for reference. The interface has two methods:

* `SendDocument`: Method to send batch of documents to the destination
* `GetLatestTimestamp`: Fetch the latest timestamp from the database

Once the new source is implemented, handle it in [main.go](https://github.com/rockset/rockbench/blob/master/generator/main.go]).
