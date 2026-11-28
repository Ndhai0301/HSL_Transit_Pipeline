#!/usr/bin/env bash

set -euo pipefail

: "${PIPELINE_PASSWORD:?PIPELINE_PASSWORD must be set}"
: "${BI_READER_PASSWORD:?BI_READER_PASSWORD must be set}"
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v pipeline_pw="$PIPELINE_PASSWORD" \
     -v bi_pw="$BI_READER_PASSWORD" \
     -v airflow_pw="$AIRFLOW_DB_PASSWORD" <<'SQL'

-- Writer: Spark sinks, Airflow loaders, dbt
CREATE ROLE pipeline  LOGIN PASSWORD :'pipeline_pw';
-- Read-only: Grafana, Metabase
CREATE ROLE bi_reader LOGIN PASSWORD :'bi_pw';
-- Owner of the Airflow metadata database only
CREATE ROLE airflow   LOGIN PASSWORD :'airflow_pw';

-- Nobody connects to the warehouse unless granted
REVOKE CONNECT ON DATABASE :"DBNAME" FROM PUBLIC;
GRANT  CONNECT ON DATABASE :"DBNAME" TO pipeline, bi_reader;
-- dbt creates its own schemas (staging, intermediate, gold)
GRANT  CREATE  ON DATABASE :"DBNAME" TO pipeline;

-- Separate database so Airflow's tables never mix with pipeline data
CREATE DATABASE airflow OWNER airflow;
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
SQL