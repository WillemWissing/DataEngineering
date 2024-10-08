#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE metastore;
    CREATE USER hive WITH PASSWORD 'hivepassword';
    GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
EOSQL
