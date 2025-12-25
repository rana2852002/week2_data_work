from __future__ import annotations

import sys
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import build_paths
from bootcamp_data.io import load_orders_csv, load_users_csv, save_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from bootcamp_data.quality import require_columns, assert_non_empty, assert_in_range

log = logging.getLogger("day2")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    p = build_paths(ROOT)

    orders_raw = load_orders_csv(p.raw / "orders.csv")
    users = load_users_csv(p.raw / "users.csv")
    log.info("Loaded rows: orders=%d users=%d", len(orders_raw), len(users))

    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, name="orders_raw")
    assert_non_empty(users, name="users")

    orders = enforce_schema(orders_raw)

    rep = missingness_report(orders)
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep_path = reports_dir / "missingness_orders.csv"
    rep.to_csv(rep_path)
    log.info("Wrote report: %s", rep_path)

    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = orders.copy()
    orders_clean["status_clean"] = status_clean
    orders_clean = add_missing_flags(orders_clean, ["amount", "quantity"])

    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")

    out_orders = p.processed / "orders_clean.parquet"
    save_parquet(orders_clean, out_orders)

    out_users = p.processed / "users.parquet"
    save_parquet(users, out_users)

    log.info("Wrote cleaned parquet: %s", out_orders)
    log.info("Wrote users parquet: %s", out_users)


if __name__ == "__main__":
    main()
