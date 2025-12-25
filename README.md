# Week 2 — Data Work (ETL + EDA)

This project was completed as part of the **AI Professionals Bootcamp**.
It covers building an ETL pipeline and performing exploratory data analysis (EDA)
using Python, Pandas, and Plotly.

---

## Project Overview

### Day 1 — Data Loading (ETL)
- Loaded raw CSV files (`orders`, `users`)
- Enforced schema (data types + basic cleaning)
- Saved processed data as Parquet
- Logged row counts and dtypes

### Day 2 — Data Quality & Cleaning
- Performed missing-value analysis
- Normalized order status values
- Added missing-value flags
- Wrote cleaned Parquet outputs

### Day 3 — Analytics Table
- Parsed datetime columns
- Added time features (year, month, day, hour)
- Handled outliers (winsorization)
- Safely joined orders with users
- Built an analytics-ready table

### EDA (Exploratory Data Analysis)
- Analyzed revenue by country
- Analyzed monthly revenue trends
- Visualized amount distribution (winsorized)
- Performed bootstrap comparison on refund rates
- Exported figures and summary report

---

## Setup

Create and activate a virtual environment, then install dependencies:

python -m venv .venv
source .venv/bin/activate   # Mac/Linux

# .venv\Scripts\activate    # Windows

pip install -r requirements.txt


## Run the scripts in order:
python scripts/run_day1_load.py
python scripts/run_day2_clean.py
python scripts/run_day3_build_analytics.py


## After running the pipeline and EDA, the following outputs are generated:
Processed Data
data/processed/analytics_table.parquet
data/processed/_run_meta.json
Reports & Figures
reports/figures/revenue_by_country.png
reports/figures/revenue_trend_monthly.png
reports/figures/amount_hist_winsor.png
reports/summary.md

## EDA Notebook
Run the exploratory analysis notebook:
notebooks/eda.ipynb

All figures are automatically saved to reports/figures/