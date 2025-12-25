# Week 2 Summary — ETL + EDA

## Key findings
- Total revenue is concentrated in a small number of countries, with clear differences in contribution by country.
- The number of orders varies noticeably across countries, indicating uneven customer activity.
- Monthly revenue shows limited variation over time, which is expected given the small dataset size.
- The distribution of order amounts (winsorized) shows a clear central tendency with reduced impact from extreme outliers.
- The estimated difference in refund rate between SA and AE is small, and the confidence interval includes zero, suggesting no strong evidence of a difference.

## Definitions
- Revenue = sum of `amount` across all orders.
- Average order value (AOV) = mean of `amount` per group.
- Refund rate = mean of `is_refund`, where `is_refund = 1` if `status_clean == "refund"`, otherwise 0.
- Time window = based on the `created_at` timestamp, aggregated at the monthly level.

## Data quality caveats
- Missingness: Some columns contain missing values (e.g., amount, quantity), which may affect aggregates.
- Duplicates: No explicit duplicate removal was performed beyond basic checks.
- Join coverage: Results depend on successful joins between orders and users; unmatched records may be excluded.
- Outliers: Extreme order amounts were winsorized to stabilize analysis, which may hide rare but real values.
- Sample size: The dataset is very small, so statistical results (including bootstrap confidence intervals) are highly uncertain.

## Next questions
- How do revenue and refund behavior change with a larger and more representative dataset?
- Are there differences in customer behavior by time of day or day of week?
- Can we segment users to better explain variation in order value and refunds?
