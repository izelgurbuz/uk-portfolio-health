# UK Portfolio Health Pipeline

This is my end-to-end **data engineering project** that simulates what a real-world financial analytics pipeline would look like.  

This project ingests financial data every day, transforms it, calculates advanced metrics like **Sharpe Ratio**, **Sortino Ratio**, **Max Drawdown**, and serves it through a **Django REST API** and a lightweight dashboard.

It’s built to be **realistic and production-ready**, the way you’d do it at a fintech or hedge fund.

---

## **Why I Built This**

I wanted a project that:
1. **Showcases real-world data engineering skills** — not just toy examples.
2. Uses **modern tools** like Snowflake, Airflow, and Python in a way you'd see at work.
3. Lives in the **finance domain**, because finance data is messy, time-sensitive, and perfect for testing pipelines.
4. Combines **ETL + ELT**, orchestration, and an API layer, showing the full data lifecycle.

It’s a full system with:
- Daily automation
- Historical tracking
- Incremental loads
- Alerts when things go wrong

---

## **What It Does**

Here’s the flow, end-to-end:

- Alpha Vantage API (Equities, FX, Benchmark)
│
▼
- Airflow DAG (Python scripts fetch daily data)
│
▼
- Snowflake RAW schema <-- cleaned, structured data
│
▼
- Snowflake ANALYTICS schema <-- heavy lifting done here
(Views, Rolling Metrics, Portfolio Calculations)
│
▼
- Django REST API


It starts with raw data from Alpha Vantage, ends with a live API  where you can see your portfolio's health at a glance.

---

## **Core Features**

- **Daily portfolio updates** via Airflow
- **Incremental loads** (no duplicate data, historical accuracy preserved)
- **Snowflake transformations** for heavy calculations
- Advanced portfolio metrics like:
  - Sharpe Ratio
  - Sortino Ratio
  - Max Drawdown
  - Beta and Alpha
- **Data Quality Checks**:
  - Missing data
  - Duplicate records
  - Alerts sent straight to Slack
- **Read-only API** built with Django
---

## **How ETL and ELT Fit In**

This project mixes **ETL** and **ELT**, just like you'd see in real life.

- **ETL (Extract → Transform → Load)**:  
  Before data ever hits Snowflake, my Python scripts clean and standardize it.  
  For example:
  - Normalize timestamps
  - Ensure numeric types are valid
  - Fix symbol naming (`AAPL`, `MSFT` etc.)

  *Why?*  
  Because if your RAW data is garbage, everything downstream suffers.

---

- **ELT (Extract → Load → Transform)**:  
  Once the clean data lands in Snowflake, that's where the heavy stuff happens.  
  - Rolling volatility
  - Weighted portfolio returns
  - Value-at-Risk (VaR)
  - Sharpe, Sortino, Beta, Alpha
  - Max drawdown tracking

  *Why here?*  
  Snowflake is **built for this** — it's fast, scalable, and perfect for window functions and analytics queries.

---
<img width="1614" height="286" alt="Screenshot 2025-09-21 at 18 41 40" src="https://github.com/user-attachments/assets/7dfd694f-08bb-4a91-b095-04de20a8327a" />

## **Tech Stack**

| Part of the System | Tool |
|--------------------|------|
| Data Source        | Alpha Vantage API |
| Orchestration      | Apache Airflow |
| Storage            | Snowflake |
| Transformations    | Python + SQL |
| Monitoring         | Slack Alerts |
| API Layer          | Django REST Framework |
| Local Dev          | Docker |

---

## **Challenges I Ran Into**

Every real project has hurdles. Here’s what I faced and how I solved them:

| Problem | Why It Was Tough | How I Solved It |
|----------|-----------------|-----------------|
| Snowflake doesn’t have direct covariance/variance functions | Needed for Beta & Alpha calculations | Wrote manual rolling formulas using `SUM` and window functions |
| API limits  | Rate limits made daily loads tricky | Added retry logic and incremental fetching | 
| Incremental loads without losing data | Needed to avoid duplicates *and* keep full history | Created a `LOAD_METADATA` table and used `MERGE INTO` for idempotency |
| Missing FX data in early sources | Caused gaps in portfolio value calculations | Switched FX data entirely to Alpha Vantage |
| Django JSON errors on `NaN` | Django can’t serialize NaN values | Replaced NaN with `None` before sending response |

---

## **The Data Model**

The project uses two Snowflake schemas:

### **RAW**
The untouched, original source data — just cleaned enough to be consistent.

- `EQUITY_DAILY` – OHLC equity data  
- `FX_DAILY` – foreign exchange rates  
- `FACT_BENCHMARK` – S&P 500 benchmark index  
- `PORTFOLIO_TRANSACTIONS` – buys, sells, position changes reflected as deltas 

---

### **ANALYTICS**
Where the magic happens — transformed, ready-to-use data.

- `VIEW_PORTFOLIO_METRICS` – rolling portfolio values, returns, volatility
- `FACT_PORTFOLIO_ADV_METRICS` – Sharpe, Sortino, Max Drawdown, Beta, Alpha

---

## **APIs**

Once the data is flowing, it’s exposed via Django:

- `/api/portfolios/<id>/metrics`  
  → Daily returns, volatility, VaR

- `/api/portfolios/<id>/advanced-metrics`  
  → Sharpe, Sortino, Beta, Alpha, Max Drawdown

**Example Output:**
```json
[
  {
    "DATE": "2025-09-18",
    "PORTFOLIO_ID": "P1",
    "TOTAL_VALUE_GBP": 10456.32,
    "WEIGHTED_DAILY_RETURN": 0.0025,
    "ROLLING_30D_VOLATILITY": 0.0132,
    "DAILY_VAR_95": -0.023
  }
]
```

⚠️ Heads-up: This project needs a Snowflake account and Alpha Vantage API key.
If you don't have those, you can still browse the code and Airflow DAGs, but you won't see live data.

## How to Run This Project

1. Clone the repository and navigate to the project folder:
git clone https://github.com/
izelgurbuz/uk-portfolio-health.git
cd uk-portfolio-health

2. Create a .env file:
`cp .env.example .env`

3. Update the .env file with your Snowflake credentials, Alpha Vantage API key, and Slack webhook URL:
```
SNOWFLAKE_ACCOUNT=your_account_region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PORTFOLIO
SNOWFLAKE_SCHEMA_RAW=RAW
SNOWFLAKE_SCHEMA_ANALYTICS=ANALYTICS
ALPHA_VANTAGE_KEY=your_alpha_vantage_api_key
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
DJANGO_SECRET_KEY=your_secret_key
```


4. Create and activate a virtual environment:
`python -m venv .venv
source .venv/bin/activate # Mac/Linux
.venv\Scripts\activate # Windows`

5. Install dependencies:
`pip install -r requirements.txt`

6. Initialize Airflow:
`docker-compose -f docker-compose.airflow.yaml up airflow-init`

7. Start Airflow services:
`docker-compose -f docker-compose.airflow.yaml up`

8. Open Airflow UI in your browser:
http://localhost:8080

9. Trigger the DAG named:
etl_uk_portfolio_health

10. Run the Django API:
`python manage.py runserver`

11. Access the API and dashboard:
`http://127.0.0.1:8000/api/portfolios/P1/metrics`

`http://127.0.0.1:8000/api/portfolios/P1/advanced-metrics`

`http://127.0.0.1:8000/api/dashboard/P1/`






