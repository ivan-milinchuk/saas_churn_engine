# SaaS MRR & Churn Engine

Small data engineering project on top of the RavenStack SaaS dataset.

What it does:

- spins up PostgreSQL in Docker
- loads RavenStack CSVs into a `raw` schema
- builds `staging` tables
- builds a simple mart with monthly MRR and churn:
  - `mart.mart_mrr_over_time`

No Airflow here yet – just Docker + Python + SQL.

---

## Tech

- Docker / docker-compose
- PostgreSQL
- Python (pandas, SQLAlchemy, psycopg2)
- Plain SQL for modeling

---

## How to run

Clone the repo and go to the project folder:

```bash
git clone https://github.com/IM-commits/saas_churn_engine.git
cd saas_churn_engine
```

### 1. Start Postgres in Docker

```bash
docker-compose up -d
# or, with the new Docker CLI:
# docker compose up -d
```

This starts a Postgres container called `saas_pg` on port `5432`.

### 2. Python env and deps

Create and activate a virtual env, then install dependencies:

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows (PowerShell / cmd)
# .venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Put the data in place

Create `data/raw` (if it’s not there) and drop the RavenStack CSVs inside:

- `ravenstack_accounts.csv`
- `ravenstack_subscriptions.csv`
- `ravenstack_feature_usage.csv`
- `ravenstack_support_tickets.csv`
- `ravenstack_churn_events.csv`

So the paths look like:

```text
data/raw/ravenstack_accounts.csv
data/raw/ravenstack_subscriptions.csv
...
```

### 4. Run the pipeline

From the project root:

```bash
python -m scripts.rebuild_pipeline
```

This will:

1. create schemas `raw`, `staging`, `mart` and RAW tables in Postgres
2. load all RavenStack CSVs into `raw.*`
3. build the `staging.*` tables
4. build the mart `mart.mart_mrr_over_time`

If you want to quickly see the result, there’s a small debug script:

```bash
python -m scripts.debug_print_mrr
```

It prints the first few rows from `mart.mart_mrr_over_time`.

---

## Main output
Computes monthly MRR movements: new, expansion, contraction, churn, plus gross revenue churn rate (account-level month-over-month deltas for expansion/contraction).

Main mart:

- `mart.mart_mrr_over_time`

Columns:

- `month`
- `starting_mrr`
- `new_mrr`
- `churned_mrr`
- `net_new_mrr`
- `ending_mrr`
- `gross_revenue_churn_rate`

Example query:

```sql
SELECT *
FROM mart.mart_mrr_over_time
ORDER BY month;
```
---

## Data

This project uses the **SaaS Subscription & Churn Analytics** dataset (“RavenStack”) from Kaggle:

- Kaggle: https://www.kaggle.com/datasets/rivalytics/saas-subscription-and-churn-analytics-dataset

RavenStack is a fictional AI-powered collaboration platform, but the dataset is designed to look like a real B2B SaaS business, with accounts, subscriptions, feature usage, support tickets and churn events.

---

## Next ideas

Things I might add later:

- Airflow DAG on top of this pipeline
- cohorts by signup date
- MRR breakdown by industry / country / referral source
