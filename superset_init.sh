#!/bin/bash

# Exit on any error
set -e

echo "Starting Superset initialization..."

# Wait for dependencies
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 1
done

echo "Waiting for Redis..."
while ! nc -z redis 6379; do
  sleep 1
done

echo "Waiting for ClickHouse..."
while ! nc -z clickhouse-server 8123; do
  sleep 1
done

echo "Installing ClickHouse driver..."
pip install --no-cache-dir clickhouse-sqlalchemy==0.2.4

echo "Installing additional dependencies..."
pip install --no-cache-dir SQLAlchemy==1.4.53

# Initialize Superset
echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin || echo "Admin user already exists"

echo "Upgrading database..."
superset db upgrade

echo "Initializing Superset..."
superset init

echo "Starting Superset server..."
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger