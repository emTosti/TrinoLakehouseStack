#!/usr/bin/env bash
set -e

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE lakekeeper
    WITH LOGIN
         PASSWORD '$LAKEKEEPER_DB_PASSWORD'
         NOSUPERUSER
         INHERIT
         NOCREATEDB
         NOCREATEROLE
         NOREPLICATION;

  CREATE ROLE hive
    WITH LOGIN
         PASSWORD '$HIVE_DB_PASSWORD'
         NOSUPERUSER
         INHERIT
         NOCREATEDB
         NOCREATEROLE
         NOREPLICATION;

  CREATE DATABASE lakekeeper OWNER lakekeeper;
  CREATE DATABASE metastore OWNER hive;

  GRANT ALL PRIVILEGES ON DATABASE lakekeeper TO lakekeeper;
  GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
EOSQL
