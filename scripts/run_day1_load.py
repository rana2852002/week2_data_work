from __future__ import annotations

from pathlib import Path
import sys
import logging
import json
from datetime import datetime, timezone

# Repo root (works no matter where you run the script from)
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC)) if str(SRC) not in sys.path else None

from bootcamp_data.config import build_paths
from bootcamp_data.io import load_orders_csv, load_users_csv, save_parquet
from bootcamp_data.transforms import enforce_schema

log = logging.getLogger("day1")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    p = build_paths(ROOT)

    orders_raw_path = p.raw / "orders.csv"
    users_raw_path = p.raw / "users.csv"

    orders = load_orders_csv(orders_raw_path)
    users = load_users_csv(users_raw_path)

    orders = enforce_schema(orders)

    log.info("Loaded rows → orders=%d, users=%d", len(orders), len(users))
    log.info("Orders dtypes:\n%s", orders.dtypes)
    log.info("Users dtypes:\n%s", users.dtypes)

    out_orders = p.processed / "orders.parquet"
    out_users = p.processed / "users.parquet"

    save_parquet(orders, out_orders)
    save_parquet(users, out_users)

    meta = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {"orders": str(orders_raw_path), "users": str(users_raw_path)},
        "rows": {"orders": int(len(orders)), "users": int(len(users))},
        "outputs": {"orders": str(out_orders), "users": str(out_users)},
    }
    meta_path = p.processed / "_run_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    log.info("Wrote outputs → %s", p.processed)
    log.info("Meta file → %s", meta_path)


if __name__ == "__main__":
    main()
