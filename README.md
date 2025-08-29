
## 🚀 RP Case – Data Pipeline (Airflow + Spark + Delta Lake + S3)

### 📌 Overview

This project implements a layered financial data pipeline using Apache Airflow for orchestration, Apache Spark + Delta Lake for scalable processing with ACID support, and Amazon S3 as a data lake.

**Objective:**

- Hipothetical scenario where CDC (Change Data Capture) data arrives as batch files, such as .parquet. In this case, the files would already be available in the raw zone.
Another scenario would involve fetching this data through an API, saving it into the raw zone, and then performing the necessary transformations to load it into the curated zone. 
- Ingest daily CDI rates and financial transactions into the **raw zone**.
- Process and consolidate this data in **Delta Lake format** in the **curated zone**.  
- Calculate daily balances and interest per client/account, and store the results in the **analytics zone**.

---
### 🗃️ Data Zones

#### 🔹 Raw Zone (`mzan-raw-rp-case`)
Stores raw, unprocessed data directly ingested from external sources.

**Contents:**
- Financial transactions in `.parquet` format
- Daily CDI rate in `.json` format, partitioned by date  
  `cdi/data=YYYY-MM-DD/*.json`

#### 🔹 Curated Zone (`mzan-curated-rp-case`)
Stores cleaned, structured, and deduplicated data in Delta Lake format.

**Tables:**
- `transactions_delta`: Financial transactions with schema validation and deduplication
- `cdi_delta`: Daily CDI rates normalized and partitioned by date

#### 🔹 Analytics Zone (`mzan-analytics-rp-case`)
Stores daily interest calculations and account summaries for analytical querying.

**Table: `payouts_delta`**  
This table includes per-user, per-account computed fields for each day.

---

### ⚙️ Pipeline (Steps)

#### 🔹 1. CDI Ingestion (`cdi_ingest_job`)
- Queries the official Central Bank API for the CDI rate.
- Stores the result as a `.json` file in the raw zone:  
  `mzan-raw-rp-case/cdi/data=YYYY-MM-DD/*.json`
- If no CDI is returned for the date, logs a warning (does not fail the job).

#### 🔹 2. Raw → Curated (`raw_to_curated_job`)
- Reads raw `.parquet` transaction files and `.json` CDI files.
- Filters input by `execution_date` (D-1).
- Transforms and writes data to curated zone using Delta Lake format.
- Ensures idempotency with `MERGE` operations into:
  - `transactions_delta`
  - `cdi_delta`
- If no data is found, logs and skips processing for the day.

#### 🔹 3. Curated → Analytics (`analytics_job`)
- Calculates interest and balances only for dates with valid CDI.
- Implements the following logic:

  - `saldo_a_mais_24_horas`: previous day's `saldo_final` from analytics.  
    If unavailable, it uses the sum of all transactions up to D-1.
  
  - `movimentacao_dia`: sum of transactions for date D.
  
  - `saldo_elegivel`: only set if `saldo_a_mais_24_horas ≥ 100`.
  
  - `juros_calculado`: `saldo_elegivel * CDI_dia`.
  
  - `saldo_final`: `saldo_elegivel + movimentacao_dia + juros_calculado`.

- Writes to `payouts_delta` (analytics zone) using idempotent `MERGE`.

---

### 📊 Analytics Table Structure

| Field                  | Description                                                   |
|------------------------|---------------------------------------------------------------|
| `user_id`              | Client identifier                                             |
| `account_id`           | Client account (a user may have multiple accounts)           |
| `saldo_a_mais_24_horas`| Balance held for 24 hours (used to calculate interest)       |
| `movimentacao_dia`     | Total amount of daily transactions                           |
| `saldo_elegivel`       | Eligible balance for interest (≥ 100)                         |
| `juros_calculado`      | Interest calculated using the CDI rate                        |
| `saldo_final`          | Final balance for the day (used in next day's calculation)    |
| `date`                 | Reference date                                                |

---

### 🗂️ Project Structure




---

### 🛠️ Technologies Used

- **Apache Airflow 2.9.0** (Python 3.10)
- **Apache Spark 3.5.0**
- **Delta Lake 2.x**
- **Amazon S3** (data lake: raw, curated, analytics zones)
- **Docker** and **Docker Compose**

---

### ▶️ How to Run

#### 1. Configure AWS Environment Variables

Make sure to set your AWS credentials (or configure them in Airflow using the `aws_default` connection). You need to create an IAM in AWS with read and write permissions for an S3 bucket and define the variables for this IAM in the .env file.

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```
#### 2. Configure and Execute the Terraform on the path /Infra - It's necessary adjust the name of the buckets
```bash
terraform init
terraform plan
terraform apply
```
#### 3. Start Docker
```bash
docker-compose build
docker-compose up
```

#### 4. Access Airflow UI:

Open your browser at: http://localhost:8080

#### 5. Trigger DAG rp-case:

Run over the interval: 2024-04-30 → 2024-10-31

The pipeline will process one day at a time (catchup=True)

###✅ Conclusion

This pipeline ensures:

- ✅ Reliable orchestration (Airflow)

- ✅ Scalable distributed processing (Spark)

- ✅ ACID transactions across all zones (Delta Lake)

- ✅ Reproducibility and idempotence via MERGE

- ✅ Business rules enforcement:

  - Minimum eligible balance

  - Interest based on CDI

  - 24h grace period for interest eligibility
