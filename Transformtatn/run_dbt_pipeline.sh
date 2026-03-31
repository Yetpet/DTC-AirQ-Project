#!/bin/bash

# Air Quality DBT Pipeline Runner
# Run this script to execute all DBT transformations

echo "Starting Air Quality DBT Pipeline..."

# Navigate to project directory
cd "$(dirname "$0")/air_quality_dbt"

# Run DBT transformations
echo "Running DBT connection test..."
uv run dbt debug

# Run DBT transformations
echo "Running DBT transformations..."
uv run dbt run

# Run tests
echo "Running data quality tests..."
uv run dbt test

# Generate documentation
echo "Generating documentation..."
uv run dbt docs generate

echo "🎉Pipeline complete! View docs at: uv run dbt docs serve"