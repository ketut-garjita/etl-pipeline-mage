FROM python:3.11-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-core and dbt-duckdb
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-duckdb

# Install dbt-clickhouse &  dbt-postgres
RUN pip install dbt-clickhouse dbt-postgres

# Set working directory
WORKDIR /app

# Set entrypoint
# ENTRYPOINT ["dbt", "--help"]
CMD ["/bin/sh", "-c", "while true; do sleep 10; done"]

