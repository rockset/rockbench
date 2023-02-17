# RockBench

Benchmark to measure ingest throughput of a realtime database.

A real-time database is one that can sustain a high write rate of new incoming data, while at the same time allow
applications to make decisions based on fresh data. There could be a time lag between when the data is writtes to the
database and when it is visible in a query. This is called the data latency, or end-to-end latency, of the database. The
data latency is different from a query latency, which is what is typically used to measure the latency of querying a
database.

Data latency is one of the distinguishing factors that differentiates one real-time database from another. It is an
important measure for developers of low-latency applications, like real-time personalization, IoT automation and
security analytics, where speed is critical.

RockBench measures the data latency of any real-time database. It is designed to continuously stream documents in
batches of fixed size to a database and also calculate and report the data latency by querying the database at fixed
intervals.

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

# Send data to Rockset and report data latency
ROCKSET_API_KEY=xxxx ROCKSET_COLLECTION=yyyy WPS=1 BATCH_SIZE=50 DESTINATION=Rockset ./generator

# Send data to ElasticSearch and report data latency
ELASTIC_AUTH=xxxx ELASTIC_URL=https://... ELASTIC_INDEX=index_name WPS=1 BATCH_SIZE=50 DESTINATION=Elastic ./generator

# Send data to Snowflake and report data latency
SNOWFLAKE_ACCOUNT=xxxx SNOWFLAKE_USER=xxxx SNOWFLAKE_PASSWORD=xxxx SNOWFLAKE_WAREHOUSE=xxxx SNOWFLAKE_DATABASE=xxxx SNOWFLAKE_STAGES3BUCKETNAME=xxxx AWS_REGION=xxxx WPS=1 BATCH_SIZE=50 DESTINATION=Snowflake ./generator
```

* To run with Docker container

```
docker build -t rockset/write_generator .
docker run -e [env variable as above] rockset/write_generator
```

### Modes

RockBench can also measure the speed of patches.

| mode           | operation                                                |
|----------------|----------------------------------------------------------|
| add            | Perform strictly inserts (using either id scheme)        |
| patch          | Perform patches on id range specified from [0, NUM_DOCS) |
| add_then_patch | Perform add mode then patch mode                         |

Setting `NUM_DOCS` to a non-negative value will limit the number of writes made and then perform patches against that
document set.
Patch mode must be explicitly enabled via `MODE=patch` or `MODE=add_then_patch` and the patches per second is controlled
via `PPS`.
`PPS` == `WPS` unless `PPS` is specified.
`BATCH_SIZE` is used for both patching and inserting.
Each patch will update a timestamp field for latency detection and also one other field/array in the document.

Patches can take on various forms, currently

- replace: replaces random fields with roughly equivalent type and similar size
- add: Adds new top level fields and prepends entries into the top level tags array

Specify `PATCH_MODE` as either 'replace' or 'add'. Default will be 'replace'.

You can also specify the `_id` scheme for Rockset destination to be either `uuid` or `sequential` (increasing sequential
numbers) using `ID_MODE`

## How to extend RockBench to measure your favourite realtime database

Implement the [Destination](https://github.com/rockset/rockbench/blob/master/generator/destination.go) interface and
provide the appropriate configs required.
Check [Rockset](https://github.com/rockset/rockbench/blob/master/generator/rockset.go)
and [Elastic](https://github.com/rockset/rockbench/blob/master/generator/elastic.go) for reference. The interface has
two methods:

* `SendDocument`: Method to send batch of documents to the destination
* `GetLatestTimestamp`: Fetch the latest timestamp from the database

Once the new source is implemented, handle it
in [main.go](https://github.com/rockset/rockbench/blob/master/generator/main.go).
