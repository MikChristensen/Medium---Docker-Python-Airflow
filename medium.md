# Docker-Based Python and Airflow: Building a Production-Like Data Pipeline

In this project, I build a small production-like data engineering
environment with Docker Compose. The goal is not to claim that a local
Docker setup is production ready, but to demonstrate how PostgreSQL,
containerized Python workloads and Apache Airflow can be separated into
services and connected into a reproducible pipeline.

The final architecture contains:

``` text
                         Airflow
                 ┌──────────┴──────────┐
                 │ scheduler/webserver │
                 └──────────┬──────────┘
                            │ DockerOperator
                            ▼
                  temporary Python jobs
                            │
                            ▼
                     postgres-prod

Airflow metadata ─────────► postgres-airflow
```

The project uses one Python image for several short-lived jobs. Airflow
decides which script to execute and Docker creates a temporary container
for each task.

## 1. Project structure

Create the following structure:

``` text
Medium---Docker-Python-Airflow/
├── .env
├── .gitignore
├── compose.yaml
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── load_data_dag.py
└── python/
    ├── Dockerfile
    ├── requirements.txt
    └── app/
        ├── generate_data.py
        ├── validate_data.py
        ├── load_data.py
        └── quality_check.py
```

## 2. Environment variables

Configuration is kept in a central `.env` file:

``` env
POSTGRES_DB=production_db
POSTGRES_USER=production_user
POSTGRES_PASSWORD=change_me

DB_HOST=postgres-prod
DB_PORT=5432

AIRFLOW_DB=airflow_db
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=airflow_password
```

Docker Compose reads these values and injects them into the containers.
This prevents database settings from being repeated throughout the
Compose file and Python code.

The values above are intentionally demo credentials. A real project
should not commit real passwords. A common approach is to ignore `.env`,
commit an `.env.example`, and supply real secrets through the deployment
environment or a secret manager.

The configuration flow is:

``` text
.env
  ↓
Docker Compose
  ├── postgres-prod
  ├── postgres-airflow
  ├── python-engine
  └── Airflow
          ↓
     DockerOperator
          ↓
   temporary Python job
```

## 3. Production PostgreSQL

The first service is the database containing application data:

``` yaml
postgres-prod:
  image: postgres:18
  container_name: postgres-prod
  environment:
    POSTGRES_DB: ${POSTGRES_DB}
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  ports:
    - "5432:5432"
  volumes:
    - postgres_prod_data:/var/lib/postgresql
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
    interval: 5s
    timeout: 5s
    retries: 10
  restart: unless-stopped
  networks:
    - data-platform
```

The named volume makes the database persistent when the container is
replaced. With PostgreSQL 18 the volume is mounted at
`/var/lib/postgresql`.

A healthcheck is also important. `depends_on` can control startup order,
but startup is not the same as readiness. `pg_isready` allows dependent
services to wait until PostgreSQL can actually accept connections.

Start the database:

``` bash
docker compose up -d postgres-prod
docker compose ps
```

A useful lesson from building this project was that a terminal message
saying a container was *started* did not necessarily mean it stayed
running. When something looks wrong, `docker compose ps -a` and
`docker compose logs <service>` are better sources of truth.

### Persistent storage is not a backup

The named volume protects data from ordinary container replacement, but
it is not a backup. If the underlying volume is lost or corrupted, the
database is lost as well. A separate backup and restore procedure is
added later.

## 4. The Python image

The Python workloads use one shared image:

``` dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

CMD ["python", "load_data.py"]
```

`python/requirements.txt` contains:

``` text
psycopg[binary]
```

The important line is:

``` dockerfile
COPY app/ .
```

All four Python applications are copied into the same image. We
therefore do **not** need four images. Airflow can start four different
containers from the same image and change only the command.

The Compose service gives the image a stable name:

``` yaml
python-engine:
  image: medium-python-engine:latest
  build:
    context: ./python
    dockerfile: Dockerfile
  environment:
    DB_HOST: postgres-prod
    DB_PORT: 5432
    POSTGRES_DB: ${POSTGRES_DB}
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  depends_on:
    postgres-prod:
      condition: service_healthy
  networks:
    - data-platform
```

Build it with:

``` bash
docker compose build python-engine
```

## 5. A four-step data pipeline

The Python image contains four small applications:

``` text
generate_data
      ↓
validate_data
      ↓
load_data
      ↓
quality_check
```

`generate_data.py` creates three demo orders in a staging table. Each
order has a stable `order_id`.

The important database operation is an upsert:

``` sql
INSERT INTO staging_orders (...)
VALUES (...)
ON CONFLICT (order_id)
DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    product_name = EXCLUDED.product_name,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    generated_at = EXCLUDED.generated_at;
```

`validate_data.py` checks the staging table before loading. The pipeline
fails if there is no data, if required text values are missing, or if
quantity or price is invalid.

`load_data.py` moves the validated data into `orders`. It also uses
`order_id` and `ON CONFLICT`, making repeated execution idempotent:

``` sql
INSERT INTO orders (...)
SELECT ...
FROM staging_orders
ON CONFLICT (order_id)
DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    product_name = EXCLUDED.product_name,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    loaded_at = EXCLUDED.loaded_at;
```

Finally, `quality_check.py` checks the production table after loading.
It verifies that rows exist, that the values remain valid, and that
duplicate business keys have not appeared.

The complete Python scripts are deliberately kept in the project
repository rather than reproduced line-by-line here. The architecture is
more important than repeating the same connection boilerplate four
times.

Before Airflow is introduced, each job can be tested independently:

``` bash
docker compose run --rm python-engine python generate_data.py
docker compose run --rm python-engine python validate_data.py
docker compose run --rm python-engine python load_data.py
docker compose run --rm python-engine python quality_check.py
```

The result can be verified with:

``` bash
docker exec -it postgres-prod psql -U production_user -d production_db
```

``` sql
SELECT * FROM orders ORDER BY order_id;
```

Running the four commands a second time should still leave three orders:

``` sql
SELECT COUNT(*) FROM orders;
```

``` text
3
```

This matters once retries are enabled. A retry should not create
duplicate production data.

## 6. Airflow metadata database

Airflow gets its own PostgreSQL database instead of storing metadata
together with application data:

``` yaml
postgres-airflow:
  image: postgres:18
  container_name: postgres-airflow
  environment:
    POSTGRES_DB: ${AIRFLOW_DB}
    POSTGRES_USER: ${AIRFLOW_DB_USER}
    POSTGRES_PASSWORD: ${AIRFLOW_DB_PASSWORD}
  volumes:
    - postgres_airflow_data:/var/lib/postgresql
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U ${AIRFLOW_DB_USER} -d ${AIRFLOW_DB}"]
    interval: 5s
    timeout: 5s
    retries: 10
  restart: unless-stopped
  networks:
    - data-platform
```

This database contains Airflow state such as DAG runs, task instances,
users and connections. It is operational metadata, not the production
order data.

## 7. Custom Airflow image

The standard Airflow image does not include everything required by
`DockerOperator`, so the project builds a small custom image.

`airflow/Dockerfile`:

``` dockerfile
FROM apache/airflow:2.10.5

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

`airflow/requirements.txt`:

``` text
apache-airflow-providers-docker==4.4.1
```

The shared Compose configuration uses `LocalExecutor` and points Airflow
at its metadata database:

``` yaml
x-airflow-common: &airflow-common
  build:
    context: ./airflow
    dockerfile: Dockerfile
  image: medium-airflow:2.10.5
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@postgres-airflow:5432/${AIRFLOW_DB}
    DB_HOST: postgres-prod
    DB_PORT: 5432
    POSTGRES_DB: ${POSTGRES_DB}
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  depends_on:
    postgres-airflow:
      condition: service_healthy
  networks:
    - data-platform
```

Initialize Airflow once:

``` bash
docker compose build airflow-init
docker compose run --rm airflow-init
```

For this demo an `admin/admin` account is created by the initialization
command. Real credentials should be handled differently in a real
deployment.

Then start the long-running Airflow services:

``` bash
docker compose up -d airflow-webserver airflow-scheduler
```

The UI is available on port `8080`.

## 8. Airflow orchestrates the Python containers

The scheduler mounts the Docker socket:

``` yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - /var/run/docker.sock:/var/run/docker.sock
```

This allows `DockerOperator` to ask the host Docker Engine to start a
temporary Python container.

The project also defines a stable network:

``` yaml
networks:
  data-platform:
    name: data-platform
```

This avoids coupling the DAG to a Compose-generated network name based
on the project directory.

The DAG uses the same image for every task:

``` python
IMAGE = "medium-python-engine:latest"
NETWORK = "data-platform"

def python_task(task_id, script):
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        command=f"python {script}",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        environment=COMMON_ENVIRONMENT,
        auto_remove="success",
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=5),
    )
```

The four tasks are then connected:

``` python
generate_data = python_task("generate_data", "generate_data.py")
validate_data = python_task("validate_data", "validate_data.py")
load_data = python_task("load_data", "load_data.py")
quality_check = python_task("quality_check", "quality_check.py")

generate_data >> validate_data >> load_data >> quality_check
```

The DAG has two retries with a one-minute delay and a five-minute
execution timeout per task. `max_active_runs=1` prevents overlapping
runs of this small demo pipeline.

The schedule is:

``` python
schedule="0 2 * * *"
```

with a timezone-aware start date using `Europe/Copenhagen`. The pipeline
therefore represents a daily 02:00 schedule while still handling local
timezone rules.

For the first test, it is useful to trigger the DAG manually in the
Airflow UI and confirm that all four tasks become successful before
relying on the schedule.

## 9. Troubleshooting Airflow and DockerOperator

One of the most useful parts of building the project was troubleshooting
a failed DockerOperator task. The Python container started successfully
but PostgreSQL rejected its credentials. That distinction immediately
narrowed the problem: Docker, the image and the network were working;
authentication was not.

A practical troubleshooting order is:

``` text
DAG missing?
    ↓
Check DAG file and import errors

DAG visible but not scheduled?
    ↓
Check scheduler and DAG state

Task failed?
    ↓
Open the Airflow task log

DockerOperator cannot start?
    ↓
Check image → Docker socket → network

Python starts but application fails?
    ↓
Check environment variables and application output

Database connection fails?
    ↓
Check hostname → port → database → username/password
```

Useful commands include:

``` bash
docker compose ps -a
docker compose logs airflow-scheduler
docker exec airflow-scheduler airflow dags list
docker exec airflow-scheduler ls -la /opt/airflow/dags
```

A second lesson is that persistent PostgreSQL volumes preserve the
credentials with which the database was originally initialized. Changing
`.env` later does not automatically rewrite an existing database user
password. In a disposable development environment the volume can be
recreated; with important data, credentials should instead be changed
deliberately inside PostgreSQL.

## 10. Backup and recovery

### Production database

Create a backup directory on the host:

``` bash
mkdir backups
```

Create a PostgreSQL custom-format backup inside the container and copy
it to the host:

``` bash
docker exec postgres-prod pg_dump -U production_user -d production_db -Fc -f /tmp/production_db.dump
docker cp postgres-prod:/tmp/production_db.dump backups/production_db.dump
```

A backup is only useful if restore has been considered. For a restore
test, copy the dump back into PostgreSQL and restore it into an
appropriate database:

``` bash
docker cp backups/production_db.dump postgres-prod:/tmp/production_db.dump
docker exec postgres-prod pg_restore -U production_user -d production_db --clean --if-exists /tmp/production_db.dump
```

In a real production environment backups would also need retention,
off-host storage, encryption, monitoring and regular restore tests.

### Airflow DAGs

DAGs are source code, so their primary recovery mechanism should be
version control rather than a database backup:

``` text
airflow/dags/
      ↓
     Git
      ↓
remote repository
```

The Airflow metadata database can also be backed up if preserving
operational history is important, but it should not replace version
control for DAG code.

## 11. Starting the complete environment from scratch

For a clean installation:

``` bash
docker compose build
docker compose up -d postgres-prod postgres-airflow
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
```

Build the Python image if it has not already been built:

``` bash
docker compose build python-engine
```

Open Airflow, trigger `production_data_pipeline`, and verify the final
database:

``` bash
docker exec -it postgres-prod psql -U production_user -d production_db
```

``` sql
SELECT * FROM orders ORDER BY order_id;
```

A successful run demonstrates the complete path:

``` text
Airflow scheduler
      ↓
DockerOperator
      ↓
temporary Python container
      ↓
PostgreSQL staging/production tables
      ↓
validation and quality checks
```

## 12. What makes this production-like rather than production ready?

This project demonstrates several production-oriented ideas: isolated
services, persistent database storage, healthchecks, environment-based
configuration, orchestration, retries, execution timeouts, idempotent
loading, data quality checks, backups and version-controlled DAGs.

It deliberately remains a local learning environment. A real production
deployment could additionally require managed secrets, TLS, role-based
access, centralized logs, monitoring and alerting, automated backup
retention, database redundancy, CI/CD and an orchestration platform such
as Kubernetes or managed Airflow.

One further limitation deserves special attention: mounting
`/var/run/docker.sock` gives the Airflow scheduler broad control over
the host Docker Engine. It is useful for this local DockerOperator
demonstration, but it is a significant security consideration and should
not be copied blindly into an untrusted or multi-user production
environment.

## Conclusion

The project started with a single PostgreSQL container and a manually
executed Python script. It ended as a small containerized data platform
where Airflow schedules and monitors separate Python workloads,
PostgreSQL provides persistent state, and Docker isolates each
component.

The most important lesson is not simply how to start several containers.
It is how the responsibilities are separated:

``` text
Docker      → runtime isolation
Python      → data processing
PostgreSQL  → persistent state
Airflow     → orchestration
Git         → source-code recovery
Backups     → database recovery
```

That separation is what turns a collection of containers into a
reproducible data engineering workflow.
