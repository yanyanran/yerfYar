#!/bin/sh

set -e
set -x

killall yerkYar || true
sleep 0.1

cd $(dirname $0)

cd ../..
pwd
go install -v ./...

yerkYar -cluster MotherRussia -dirname ~/yerkYar-data/hah/ -instance Hah -listen 127.0.0.1:8081 -rotate-chunk-interval=10s &
yerkYar -cluster MotherRussia -dirname ~/yerkYar-data/moscow -instance Moscow -listen 127.0.0.1:8080 -rotate-chunk-interval=10s &

wait