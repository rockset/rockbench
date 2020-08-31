# rockbench
Benchmark used to measure ingest throughput to various different sources.

## Manual
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
