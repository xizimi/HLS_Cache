#!/bin/bash
trap "rm server;kill 0" EXIT

go build -o server
./server -port=8004 & ./server -port=8002 & ./server -port=8003 -api=1 & wait


pkill -f './server -port='

curl -i  "http://localhost:9988/server_data/output/playlist.m3u8"
curl -i  "http://localhost:9988/server_data/output/segment_000.ts"
curl -i  "http://localhost:9988/server_data/output/segment_001.ts"

curl -i  "http://localhost:9988/server_data/vid_1769671677_9383_out/720p/index000.ts

go build -o server



sleep 2
echo ">>> start test"
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &

wait