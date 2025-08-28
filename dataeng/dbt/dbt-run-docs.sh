docker exec -it dbt-duckdb bash -exec 'cd business_transformations && dbt run -s staging && dbt run -s dwh && dbt run -s marts && dbt docs generate && dbt docs serve --port 8080 --host dbt-duckdb'

