# week2_data_work

Day 1 Data Work (ETL) – AI Professionals Bootcamp

## What I did
- Loaded raw CSV files (orders, users)
- Enforced schema (types + cleaning)
- Saved cleaned data as Parquet
- Logged row counts and dtypes

## How to run
python scripts/run_day1_load.py



## Day 2: Data Quality & Cleaning

Added basic data quality checks, missing-value analysis, and simple cleaning transforms.
The week2 Day 2 pipeline normalizes order status, adds missing flags, and writes cleaned Parquet outputs.
## How to run
python scripts/run_day2_clean.py


## Day 3 — Analytics Table 
Built an analytics-ready table by parsing datetimes, adding time features, handling outliers, and safely joining orders with users.
## Output
data/processed/analytics_table.parquet
## How to run
python scripts/run_day3_build_analytics.py

