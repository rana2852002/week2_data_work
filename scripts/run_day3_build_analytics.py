from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import build_paths
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize
from bootcamp_data.joins import safe_left_join

try:
    from bootcamp_data.transforms import add_outlier_flag
    HAS_OUTLIER_FLAG = True
except Exception:
    HAS_OUTLIER_FLAG = False


def main() -> None:
    p = build_paths(ROOT)

    orders_path = p.processed / "orders_clean.parquet"
    users_path = p.processed / "users.parquet"

    orders = pd.read_parquet(orders_path)
    users = pd.read_parquet(users_path)

    print("loaded:", "orders_clean=", len(orders), "users=", len(users))

    require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders, name="orders_clean")
    assert_non_empty(users, name="users")

    assert_unique_key(users, "user_id")

    # time
    orders_t = parse_datetime(orders, "created_at", utc=True)
    missing_ts = int(orders_t["created_at"].isna().sum())
    print("missing created_at after parse:", missing_ts, "/", len(orders_t))

    orders_t = add_time_parts(orders_t, "created_at")

    # join
    before = len(orders_t)
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    after = len(joined)
    print("rows before join:", before, "after join:", after)

    assert after == before, "Row count changed after join (possible join explosion)"

    country_match_rate = 1.0 - float(joined["country"].isna().mean())
    print("country match rate:", round(country_match_rate, 3))

    # mini summary table (inside main!)
    summary = (
        joined
        .groupby("country", dropna=False)
        .agg(
            n_orders=("order_id", "size"),
            revenue=("amount", "sum"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
    )

    print("\nRevenue by country:")
    print(summary)

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(reports_dir / "revenue_by_country.csv", index=False)

    # outliers
    joined["amount_winsor"] = winsorize(joined["amount"])

    if HAS_OUTLIER_FLAG:
        joined = add_outlier_flag(joined, "amount", k=1.5)

    # write analytics table
    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)
    print("wrote:", out_path)


if __name__ == "__main__":
    main()
