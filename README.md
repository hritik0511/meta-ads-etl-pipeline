# 🚀 Meta Ads ETL Pipeline — Medallion Architecture on Databricks

## 📌 Overview
An end-to-end ETL pipeline that ingests, transforms, and analyzes Meta Ads campaign data using the **Medallion Architecture (Bronze → Silver → Gold)** on **Databricks**, helping businesses identify non-performing ads and reduce wasted ad spend.

> **Real-world impact:** Reduced client monthly Meta Ads spend by **30%** by surfacing non-performing ads through automated pipeline analysis.

---

## 🏗️ Architecture

```
Meta Ads API / Mock Data
        ↓
  ┌─────────────┐
  │   BRONZE    │  Raw ingestion → Delta Table
  │  (Raw Data) │  500+ ad records, 90-day window
  └─────┬───────┘
        ↓
  ┌─────────────┐
  │   SILVER    │  Cleaning + KPI computation
  │ (Cleaned)   │  CTR, CPC, CPM, Conversion Rate
  └─────┬───────┘
        ↓
  ┌─────────────┐
  │    GOLD     │  Business-ready aggregations
  │ (KPI Tables)│  Campaign Summary, Non-Performing Ads, Daily Trends
  └─────────────┘
        ↓
  Databricks Workflow (Scheduled Daily at 6 AM IST)
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **PySpark** | Distributed data processing |
| **Databricks** | Cloud compute & notebook environment |
| **Delta Lake** | ACID-compliant storage layer |
| **Databricks Workflows** | Pipeline orchestration & scheduling |
| **Python** | Mock data generation & scripting |

---

## 📁 Project Structure

```
meta_ads_pipeline/
├── bronze/
│   └── ingest_raw_meta_ads.py       # Raw data ingestion
├── silver/
│   └── transform_meta_ads.py        # Cleaning & KPI metrics
├── gold/
│   └── gold_kpi_tables.py           # Business aggregations
└── orchestration/
    └── databricks_workflow.json     # Workflow config
```

---

## 📊 KPI Metrics Computed

| Metric | Description |
|--------|-------------|
| **CTR** | Click-Through Rate (%) |
| **CPC** | Cost Per Click ($) |
| **CPM** | Cost Per 1000 Impressions ($) |
| **Conversion Rate** | Conversions / Clicks (%) |
| **Cost Per Conversion** | Spend / Conversions ($) |
| **is_non_performing** | Flag: CTR < 0.5%, zero conversions, spend > $50 |

---

## 🥇 Gold Layer Tables

| Table | Description |
|-------|-------------|
| `gold_campaign_summary` | Aggregated KPIs per campaign |
| `gold_non_performing_ads` | All flagged non-performing ads |
| `gold_daily_trend` | Day-by-day spend & performance |
| `gold_kpi_summary` | Single-row overall KPI snapshot |

---

## ⚙️ How to Run on Databricks

1. **Clone this repo** into your Databricks Workspace Repos
2. **Run notebooks in order:**
   - `bronze/ingest_raw_meta_ads`
   - `silver/transform_meta_ads`
   - `gold/gold_kpi_tables`
3. **Or import** `orchestration/databricks_workflow.json` into Databricks Workflows to schedule automatically

---

## 📈 Business Impact

- ✅ Automated identification of **non-performing ads**
- ✅ Estimated **wasted spend** surfaced per campaign
- ✅ Daily scheduled pipeline — **zero manual effort**
- ✅ Reduced client monthly ad spend by **30%**

---

## 👨‍💻 Author
**Hritik G.** — Data Engineer  
Skills: Python · SQL · PySpark · Databricks · Delta Lake · Airflow  
[Upwork Profile](https://www.upwork.com) | [LinkedIn](#)
