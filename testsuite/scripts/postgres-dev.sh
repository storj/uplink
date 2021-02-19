#!/usr/bin/env bash
set -ueo pipefail
set +x

# This is helper script to setup and run postgres docker image with
# all configuration needed to execute storj-sim integration tests locally
# Script is doing:
# * starts postgres docker container
# * waits for DB to be available
# * creates database for storj-sim

LOG_FILE=${STORJ_SIM_POSTGRES_LOG:-"storj-sim-postgres.log"}
CONTAINER_NAME=storj_sim_postgres

cleanup(){
  docker rm -f $CONTAINER_NAME
}
trap cleanup EXIT

docker run --rm -d -p 5433:5432 --name $CONTAINER_NAME -e POSTGRES_PASSWORD=tmppass postgres:12.3 -c log_min_duration_statement=0
docker logs -f $CONTAINER_NAME > $LOG_FILE 2>&1 &

STORJ_SIM_DATABASE=${STORJ_SIM_DATABASE:-"teststorj"}

RETRIES=10

until docker exec $CONTAINER_NAME psql -h localhost -U postgres -d postgres -c "select 1" > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "Waiting for postgres server, $((RETRIES--)) remaining attempts..."
  sleep 1
done

docker exec $CONTAINER_NAME psql -h localhost -U postgres -c "create database $STORJ_SIM_DATABASE;"

export STORJ_SIM_POSTGRES="postgres://postgres:tmppass@localhost:5433/$STORJ_SIM_DATABASE?sslmode=disable"