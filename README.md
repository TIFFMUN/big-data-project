# Big Data Project — AWS Infrastructure

End-to-end batch data pipeline on AWS, orchestrated by Apache Airflow (Dockerised), processed with PySpark on EMR, catalogued by Glue, and queried via Athena.

---

## Architecture

```
                          ┌─────────────────────────────┐
                          │         S3 Data Lake         │
                          │                             │
                          │  /raw/        ← source data │
┌───────────────┐         │  /scripts/    ← PySpark ETL │
│   Airflow      │────────▶│  /processed/  ← output     │
│   (Docker on   │         │  /athena-results/          │
│    EC2)        │         │  /emr-logs/                │
│                │         └──────────────┬─────────────┘
│  1. Create EMR │                        │
│  2. Submit job │                        │
│  3. Crawl data │                        │
└──────┬─────────┘                        │
       │                                  │
       ▼                                  ▼
┌───────────────┐                ┌───────────────┐
│     EMR        │  reads /raw/  │  Glue Crawler  │
│  1 Master      │  + /scripts/  │  + Data Catalog│
│  2 Core        │───────────────│                │
│  1 Task        │  writes       │  reads         │
│  (PySpark)     │  /processed/  │  /processed/   │
└───────────────┘                └───────┬───────┘
                                         │
                                         ▼
                                 ┌───────────────┐
                                 │    Athena      │
                                 │  (Workgroup)   │
                                 │  queries via   │
                                 │  Glue Catalog  │
                                 └───────────────┘
                                         │
                                         ▼
                                   [ Tableau ]
                                 (NOT provisioned)
```

---

## Project Structure

```
big-data-project/
│
├── bin/                              # Setup & utility scripts
│   ├── setup.sh / setup.ps1         #   Deploy everything (Mac/Linux / Windows)
│   ├── teardown.sh / teardown.ps1   #   Destroy everything
│   └── cost-check.sh / cost-check.ps1  # Check AWS spending
│
├── airflow/                          # Airflow (Dockerised)
│   ├── Dockerfile                    #   Custom image with boto3 + amazon provider
│   ├── docker-compose.yaml           #   Postgres · webserver · scheduler
│   ├── requirements.txt              #   Python deps baked into the image
│   ├── .env.example                  #   Template env vars (copy → .env)
│   ├── dags/
│   │   ├── emr_config.py             #   Shared config and helper functions
│   │   ├── project_dag_ingest.py     #   DAG: raw landing → EMR ingest → Glue
│   │   └── project_dag_main.py       #   DAG: main orchestration / triggers ingest
│   ├── logs/                         #   Container volume mount
│   └── plugins/                      #   Container volume mount
│
├── scripts/
│   ├── ingest.py                     # PySpark ingest (raw CSV → partitioned Parquet)
│   └── load_kaggle_raw_to_s3.py      # Kaggle raw landing helper (Kaggle → S3 /raw/)
│
├── terraform/                        # Infrastructure-as-Code
│   ├── main.tf                       #   Root module (provider + module wiring)
│   ├── variables.tf                  #   Input variables with defaults
│   ├── outputs.tf                    #   Terraform outputs
│   ├── .env.example                  #   Template AWS credentials
│   └── modules/
│       ├── s3/                       #   S3 bucket · prefixes · versioning
│       ├── iam/                      #   5 IAM roles (EC2, EMR×2, Glue, Athena)
│       ├── ec2_airflow/              #   EC2 instance · Docker bootstrap · SG · CW
│       ├── emr/                      #   EMR cluster (1M+2C+1T) · SGs · CW
│       ├── glue/                     #   Glue Crawler · Catalog DB · CW
│       ├── athena/                   #   Athena workgroup · named query
│
├── docs/
│   ├── architecture.png
│   └── Big Data Proposal Deck.pdf
│
└── README.md
```

---

## Prerequisites

| Tool             | Version          |
| ---------------- | ---------------- |
| Terraform        | ≥ 1.5            |
| AWS CLI          | v2               |
| Docker           | ≥ 24.0           |
| Docker Compose   | ≥ 2.20 (v2 plugin) |

You also need:

- An **AWS account** with permissions to create IAM, EC2, EMR, S3, Glue, Athena, and CloudWatch resources.
- AWS CLI **configured** — run `aws configure` or export `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION`.
- A **default VPC** in the target region (every AWS account has one by default).

---

## Quick Start

### Option A — One command (recommended)

**macOS / Linux:**
```bash
./bin/setup.sh
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy Bypass -File bin\setup.ps1
```

This single script will:
1. Check prerequisites (AWS CLI, Terraform, Docker)
2. Prompt for AWS credentials (if not configured)
3. Prompt for a unique S3 bucket name
4. Run `terraform init` → `plan` → `apply`
5. Upload the PySpark script to S3
6. Generate the Airflow `.env` file
7. Start Airflow with Docker Compose

To tear everything down:
- **macOS / Linux:** `./bin/teardown.sh`
- **Windows:** `powershell -ExecutionPolicy Bypass -File bin\teardown.ps1`

To check costs:
- **macOS / Linux:** `./bin/cost-check.sh`
- **Windows:** `powershell -ExecutionPolicy Bypass -File bin\cost-check.ps1`

---

### Option B — Manual steps

```bash
cd terraform/
cp example.tfvars terraform.tfvars
```

Edit **`terraform.tfvars`**:

| Variable        | What to change                                                   |
| --------------- | ---------------------------------------------------------------- |
| `bucket_name`   | Must be globally unique — e.g. `yourname-bigdata-data-lake`      |
| `allowed_cidr`  | Lock down to your IP — e.g. `"203.0.113.10/32"`                 |

### 2 — Deploy infrastructure

```bash
terraform init
terraform plan  -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

Save the outputs — you'll need `s3_bucket_name` and `airflow_public_ip`.

### 3 — Upload PySpark script

```bash
BUCKET=$(terraform output -raw s3_bucket_name)

aws s3 cp ../scripts/ingest.py "s3://${BUCKET}/scripts/ingest.py"
```

### 4 — Start Airflow with Docker

```bash
cd ../airflow/
cp .env.example .env          # edit: AWS creds, S3_BUCKET, and Kaggle creds for raw landing
docker compose up -d
```

Open **http://localhost:8080** — login **admin / admin**.

### 5 — Run the ingest pipeline

In the Airflow UI:

1. Find the **`project_dag_ingest`** DAG
2. **Unpause** it
3. Click **Trigger DAG**

This DAG will:
- land the Kaggle raw files into `s3://.../raw/airline_data/` if that prefix is empty
- upload `ingest.py` to `s3://.../scripts/`
- run the EMR PySpark ingest job
- refresh the Glue catalog after the Parquet output is written

If the raw prefix already contains files, the Kaggle landing step skips by default.

| Step | Task ID | What it does |
| ---- | ------- | ------------ |
| 1 | `land_raw_dataset` | Downloads the Kaggle airline files and lands them in S3 `/raw/airline_data/` when needed |
| 2 | `upload_ingest_script` | Uploads `ingest.py` → S3 `/scripts/` |
| 3 | `create_emr_cluster` | Spins up a transient EMR cluster (1M + 2C + 1T) |
| 4 | `wait_for_cluster_ready` | Polls until cluster reaches WAITING state |
| 5 | `submit_spark_step` | Submits `spark-submit` for `scripts/ingest.py` to EMR |
| 6 | `wait_for_step_completion` | Polls until step reaches COMPLETED |
| 7 | `terminate_emr_cluster` | Terminates the EMR cluster |
| 8 | `trigger_glue_crawler` | Starts the Glue Crawler on `/processed/airline_data/` |
| 9 | `wait_for_crawler` | Polls until crawler finishes cataloguing |

### 6 — Run the main pipeline

In the Airflow UI:

1. Find the **`big_data_pipeline`** DAG
2. **Unpause** it
3. Click **Trigger DAG**

This DAG is the orchestration wrapper. Right now it triggers `project_dag_ingest` and waits for it to finish.

| Step | Task ID | What it does |
| ---- | ------- | ------------ |
| 1 | `run_ingest_pipeline` | Triggers `project_dag_ingest` and waits for completion |

### 7 — Query with Athena

Once the crawler completes, open the **AWS Athena Console**:

- **Workgroup:** `bigdata-project-workgroup`
- **Database:** `bigdata_project_db`
- Run queries against the auto-discovered tables in `/processed/`

---

## Docker Compose Services

| Service             | Description                                    |
| ------------------- | ---------------------------------------------- |
| `postgres`          | Airflow metadata database (PostgreSQL 16)      |
| `airflow-init`      | One-shot — runs `db migrate` + creates admin user |
| `airflow-webserver` | Airflow UI on port **8080**                    |
| `airflow-scheduler` | Triggers and monitors DAG runs                 |

---

## Resources Created

| Service    | Resource                                     | Count |
| ---------- | -------------------------------------------- | ----- |
| S3         | Bucket (versioned, public access blocked)    | 1     |
| IAM        | Roles (EC2, EMR Service, EMR EC2, Glue, Athena) | 5 |
| IAM        | Instance Profiles (EC2, EMR EC2)             | 2     |
| IAM        | Policies                                     | 5     |
| EC2        | Airflow instance (`t3.micro`)                | 1     |
| EC2        | Security Groups (Airflow, EMR Master, EMR Core) | 3  |
| EMR        | Cluster — 1 Master + 2 Core + 1 Task         | 1     |
| Glue       | Catalog Database                             | 1     |
| Glue       | Crawler                                      | 1     |
| Athena     | Workgroup                                    | 1     |
| Athena     | Named Query (sample)                         | 1     |
| CloudWatch | Log Groups (EC2, EMR, Glue)                  | 3     |

---

## Tear Down

**macOS / Linux:**
```bash
./bin/teardown.sh
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy Bypass -File bin\teardown.ps1
```

Or manually:

```bash
cd terraform/
terraform destroy -var-file="terraform.tfvars"
```
