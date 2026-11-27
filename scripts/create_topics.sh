#!/usr/bin/env bash 

set -euo pipefail

cd "$(dirname "$0")/.."

BOOTSTRAP="kafka:29092"            # internal listener: the command runs INSIDE the kafka container
HOUR_MS=$((60 * 60 * 1000))

create_topic(){
    local name=$1 partitions=$2 retention_hours=$3
    echo "--> $name (partitions=$partitions, retention=${retention_hours}h)"
    docker compose exec -T kafka kafka-topics \
        --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config retention.ms=$((retention_hours * HOUR_MS))
}

create_topic hfp.raw 3 48

create_topic hfp.dlq 1 168

echo 
docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic hfp.raw
docker compose exec -T kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic hfp.dlq