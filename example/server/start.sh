#!/bin/sh

set -e
set -x

killall yerfYar || true
ssh z killall yerfYar || true
ssh g killall yerfYar || true
ssh a killall yerfYar || true
sleep 0.1

cd $(dirname $0)

cd ../..
pwd
go install -v ./...
GOARCH=arm go install -v ./...
scp ~/go/bin/yerfYar z:
scp ~/go/bin/yerfYar g:
scp ~/go/bin/linux_arm/yerfYar a:

COMMON_PARAMS="-cluster AllComrads -rotate-chunk-interval=10s"

# 所有节点都相等
yerfYar $COMMON_PARAMS -dirname ~/yerfYar-data/moscow -instance Moscow -listen 127.0.0.1:8080 &
yerfYar $COMMON_PARAMS -dirname ~/yerfYar-data/voronezh/ -instance Voronezh -listen 127.0.0.1:8081 &
ssh z ./yerfYar $COMMON_PARAMS -dirname ./yerfYar-data -instance Peking -listen 127.0.0.1:8082 &
ssh g ./yerfYar $COMMON_PARAMS -dirname ./yerfYar-data -instance Bengaluru -listen 127.0.0.1:8083 &
ssh a ./yerfYar $COMMON_PARAMS -dirname ./yerfYar-data -instance Phaenus -listen 127.0.0.1:8084 &

wait