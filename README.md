
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

# Tabela de Informações Detalhadas

---
| **user_id**                           | **account_id**                       | **event_date**                  | **saldo_dia_anterior** | **movimentacao_dia** | **horario_saldo_elegivel**      | **event_time**                  | **valor_cdi**                 | **saldo_dia_anterior_corrigido** | **cdi_diario**               | **total**                    |
|---------------------------------------|--------------------------------------|---------------------------------|------------------------|----------------------|----------------------------------|----------------------------------|-------------------------------|----------------------------------|-----------------------------|------------------------------|
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-04-30T00:00:00.000-03:00 | 0.00                   | 43.75               |                                  | 2024-04-30T22:12:50.877-03:00   | 0.040168                     | 0.0                              | 0.0                         | 43.75                        |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-01T00:00:00.000-03:00 | 43.75                 | 31.80               | 2024-04-30T22:12:50.877-03:00   | 2024-05-01T22:15:42.856-03:00   | 0.0                          | 43.75                           | 0.0                         | 75.55                        |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-02T00:00:00.000-03:00 | 31.80                 | 52.60               | 2024-05-01T22:15:42.856-03:00   | 2024-05-02T10:14:38.527-03:00   | 0.040168                     | 75.55                           | 0.0                         | 128.15                       |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-03T00:00:00.000-03:00 | 52.60                 | 235.15              | 2024-05-02T10:14:38.527-03:00   | 2024-05-03T06:58:16.117-03:00   | 0.040168                     | 128.15                          | 0.0                         | 363.30                       |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-04T00:00:00.000-03:00 | 235.15                | -290.63             | 2024-05-03T06:58:16.117-03:00   | 2024-05-04T19:55:36.753-03:00   | 0.0                          | 363.30                          | 0.0                         | 72.67                        |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-05T00:00:00.000-03:00 | -290.63               | 32.85               | 2024-05-04T19:55:36.753-03:00   | 2024-05-05T22:05:47.548-03:00   | 0.0                          | 72.67                           | 0.0                         | 105.52                       |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-06T00:00:00.000-03:00 | 32.85                 | -244.82             | 2024-05-05T22:05:47.548-03:00   | 2024-05-06T18:14:08.838-03:00   | 0.040168                     | 105.52                          | 0.0                         | -139.30                      |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-07T00:00:00.000-03:00 | -244.82               | 313.48              | 2024-05-06T18:14:08.838-03:00   | 2024-05-07T23:06:06.852-03:00   | 0.040168                     | -139.30                         | 0.0                         | 174.18                       |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-08T00:00:00.000-03:00 | 313.48                | 39.73               | 2024-05-07T23:06:06.852-03:00   | 2024-05-08T19:47:11.777-03:00   | 0.040168                     | 174.18                          | 0.0                         | 213.91                       |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-09T00:00:00.000-03:00 | 39.73                 | -47.36              | 2024-05-08T19:47:11.777-03:00   | 2024-05-09T22:50:08.433-03:00   | 0.03927                      | 213.91                          | 0.084002                     | 166.63                       |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-10T00:00:00.000-03:00 | -47.36                | 733.29              | 2024-05-09T22:50:08.433-03:00   | 2024-05-10T22:19:16.695-03:00   | 0.03927                      | 166.63                          | 0.0                         | 899.92                       |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-11T00:00:00.000-03:00 | 733.29                 | 572.62               | 2024-05-10T22:19:16.695-03:00   | 2024-05-11T10:49:21.391-03:00   | 0.0                          | 899.9240024569999               | 0.0                         | 1472.5440024569998           |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-12T00:00:00.000-03:00 | 572.62                 | -811.04              | 2024-05-11T10:49:21.391-03:00   | 2024-05-12T23:44:20.200-03:00   | 0.0                          | 1472.5440024569998             | 0.0                         | 661.5040024569998            |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-13T00:00:00.000-03:00 | -811.04               | 193.90               | 2024-05-12T23:44:20.200-03:00   | 2024-05-13T23:59:53.935-03:00   | 0.03927                      | 661.5040024569998               | 0.2597726217648638           | 855.6637750787647            |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-14T00:00:00.000-03:00 | 193.90                | -503.31              | 2024-05-13T23:59:53.935-03:00   | 2024-05-14T18:53:01.821-03:00   | 0.03927                      | 855.6637750787647               | 0.0                         | 352.3537750787647            |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-15T00:00:00.000-03:00 | -503.31               | -243.70              | 2024-05-14T18:53:01.821-03:00   | 2024-05-15T22:15:27.070-03:00   | 0.03927                      | 352.3537750787647               | 0.13836932747343092          | 108.79214440623817           |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-16T00:00:00.000-03:00 | -243.70               | -6.86               | 2024-05-15T22:15:27.070-03:00   | 2024-05-16T14:05:47.442-03:00   | 0.03927                      | 108.79214440623817             | 0.0                         | 101.93214440623817           |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-17T00:00:00.000-03:00 | -6.86                 | 253.40              | 2024-05-16T14:05:47.442-03:00   | 2024-05-17T19:40:12.213-03:00   | 0.03927                      | 101.93214440623817             | 0.04002875310832973          | 355.3721731593465           |
| b7c5b9c6-48f5-5ead-bd92-54e699e8913f | 0386e6ab-62e2-558a-96e9-7ae11605bb66 | 2024-05-18T00:00:00.000-03:00 | 253.40                | -414.61             | 2024-05-17T19:40:12.213-03:00   | 2024-05-18T23:21:46.544-03:00   | 0.0                          | 355.3721731593465             | 0.0                         | -59.2378268406535           |

