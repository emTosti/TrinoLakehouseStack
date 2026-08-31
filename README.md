# Data Lake Stack - Trino Edition

A federated query engine stack for exploring and analyzing data across multiple sources using Trino, including MinIO, PostgreSQL, Lakekeeper (Iceberg Catalog), and Kestra.

---

## Services Overview

- **minio**: S3-compatible object storage, accessible on ports `9000` (API) and `9001` (console).
- **prepare_buckets**: Initializes MinIO buckets and users for the stack.
- **lakekeeper**: Iceberg catalog for managing data lake metadata and tables.
- **migrate**: Runs Lakekeeper database migrations.
- **lakekeeper_prepare**: Bootstraps Lakekeeper with initial configuration.
- **postgres**: PostgreSQL database for Lakekeeper, Kestra, and Trino metadata.
- **trino-coordinator**: Trino query coordinator node - accepts queries and manages execution.
- **trino-worker**: Trino worker node - executes distributed queries.
- **kestra**: Workflow orchestration engine, configured to use MinIO and PostgreSQL.

All services are connected via the `data-stack-network` Docker network.

---

## Key Differences from StarRocks Stack

| Aspect | StarRocks | Trino |
|--------|-----------|-------|
| **Query Model** | Dedicated OLAP warehouse | Federated query engine |
| **Data Storage** | Stores data in S3 | Queries data without storing |
| **Multi-Source** | Single data source | Multiple data sources simultaneously |
| **Use Cases** | High-volume analytics | Ad-hoc queries, data exploration |

---

## Local Setup Requirements

1. **Docker & Docker Compose**  
   Ensure you have Docker and Docker Compose installed on your machine.

2. **Environment Variables**  
   A `.env` file is already provided with default settings. Adjust as needed.

3. **Initialize User Database**  
   Make the Postgres shell script executable:
   ```bash
   chmod +x postgres/init-user-db.sh
   ```

4. **Start the Data Stack**  
   ```bash
   docker-compose up -d
   ```

5. **Access Services**  
   - **MinIO Console:** [http://localhost:9001](http://localhost:9001) (minioadmin/minioadmin)  
   - **Trino UI:** [http://localhost:8080](http://localhost:8080)  
   - **Lakekeeper:** http://localhost:8181  
   - **PostgreSQL:** localhost:5432 (admin/admin)  
   - **Kestra:** [http://localhost:8888](http://localhost:8888)

---

## Available Catalogs in Trino

### 1. **iceberg** - Lakekeeper Iceberg Catalog
Query Iceberg tables managed by Lakekeeper through MinIO:
```sql
SELECT * FROM iceberg.datalake.table_name;
```

### 2. **postgresql** - PostgreSQL Database
Query tables directly from PostgreSQL:
```sql
SELECT * FROM postgresql.public.table_name;
```

### 3. **s3** - Raw S3 Parquet Files
Query Parquet files directly from MinIO:
```sql
SELECT * FROM s3."warehouse"."path/to/file.parquet";
```

---

## Example Queries

```sql
-- Query Iceberg table via Lakekeeper
SELECT * FROM iceberg.datalake.my_table;

-- Join Iceberg and PostgreSQL
SELECT 
  i.id, 
  i.name,
  p.description
FROM iceberg.datalake.iceberg_table i
JOIN postgresql.public.pg_table p ON i.id = p.id;

-- Query raw Parquet files
SELECT * FROM s3."warehouse"."path/to/file.parquet";
```

---

## Trino Features

✓ **Federated Queries** - Query multiple data sources in one SQL query  
✓ **Iceberg Integration** - Full support for Apache Iceberg tables via Lakekeeper  
✓ **SQL Compatibility** - ANSI SQL with familiar syntax  
✓ **Distributed Execution** - Coordinator + Worker nodes for horizontal scaling  
✓ **Multiple Connectors** - PostgreSQL, S3, Iceberg, and more  

---

## Configuration Files

- **docker-compose.yml** - Service definitions and networking
- **.env** - Environment variables and credentials
- **postgres/init-user-db.sh** - PostgreSQL initialization
- **lakekeeper/bootstrap-lk.py** - Lakekeeper setup script
- **trino/etc/config.properties** - Coordinator configuration
- **trino/etc/catalog/*.properties** - Data source connectors (Iceberg, PostgreSQL, S3)

---

## Stopping the Stack

```bash
docker-compose down
```

To remove all data:
```bash
docker-compose down -v
rm -rf volume/
```

---

## References

- [Trino Documentation](https://trino.io/docs/current/)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [Lakekeeper Documentation](https://github.com/lakekeeper/lakekeeper)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
