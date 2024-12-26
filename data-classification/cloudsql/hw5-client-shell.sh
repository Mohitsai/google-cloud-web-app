#!/bin/bash

DOMAIN="35.184.82.220"
BUCKET="hw2-mohitsai"
WEBDIR="files/files"
NUM_REQUESTS=50000
MAX_FILE_INDEX=9999
PORT=8081
TIMEOUT=10
RANDOM=43

if [ -z "$1" ]; then
    echo "Usage: $0 <number_of_clients>"
    exit 1
fi

CLIENTS=$1

run_client() {
    echo "Starting client $1"
    python3 http-client.py -d "$DOMAIN" -n "$NUM_REQUESTS" -i "$MAX_FILE_INDEX" -p "$PORT" -b "$BUCKET" -w "$WEBDIR" -t "$TIMEOUT" -r "$RANDOM" &
}

for i in $(seq 1 $CLIENTS); do
    run_client $i
done

wait
echo "All clients finished."