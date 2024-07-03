#!/bin/bash

cwd=$(dirname "$0")
project_root=$(dirname "$cwd")

docker compose -f $cwd/compose.yml up -d
trap "docker compose -f $cwd/compose.yml down" EXIT

echo "Build rockbench"
go build .


echo "Wait for 15s for compose containers to start..."
sleep 15

echo "Benchmarking Elastic"
ELASTIC_AUTH="" ELASTIC_URL=http://localhost:9200 ELASTIC_INDEX=index_name WPS=1 BATCH_SIZE=50 DESTINATION=Elastic TRACK_LATENCY=true ./rockbench

Echo "Benchmarking CrateDB"
CRATEDB_URI="postgres://crate:@localhost:5432/test?pool_max_conns=10&pool_min_conns=3" WPS=1 BATCH_SIZE=50 DESTINATION=CrateDB TRACK_LATENCY=true ./rockbench
