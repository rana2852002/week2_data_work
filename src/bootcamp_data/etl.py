from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import json
import logging
from dataclasses import asdict
from bootcamp_data.io import save_parquet

from bootcamp_data.io import load_orders_csv, load_users_csv
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import (
    enforce_schema,
    add_missing_flags,
    normalize_text,
    apply_mapping,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.joins import safe_left_join


@dataclass(frozen=True)
class ETLConfig:
    root: Path

    
    raw_orders: Path
    raw_users: Path

    out_orders_clean: Path
    out_users: Path
    out_analytics: Path

    run_meta: Path


def make_etl_config(root: Path) -> ETLConfig:
    root = Path(root)

    raw = root / "data" / "raw"
    processed = root / "data" / "processed"

    return ETLConfig(
        root=root,
        raw_orders=raw / "orders.csv",
        raw_users=raw / "users.csv",
        out_orders_clean=processed / "orders_clean.parquet",
        out_users=processed / "users.parquet",
        out_analytics=processed / "analytics_table.parquet",
        run_meta=processed / "_run_meta.json",
    )


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders_raw = load_orders_csv(cfg.raw_orders)
    users_raw = load_users_csv(cfg.raw_users)
    return orders_raw, users_raw


def transform(orders_raw: pd.DataFrame, users_raw: pd.DataFrame) -> pd.DataFrame:
   
    require_columns(
        orders_raw,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    require_columns(
        users_raw,
        ["user_id", "country", "signup_date"],
    )
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users_raw, "users_raw")
    assert_unique_key(users_raw, "user_id")

    orders = enforce_schema(orders_raw)
    users = users_raw.copy()

    status_norm = normalize_text(orders["status"])
    mapping = {
        "paid": "paid",
        "payment complete": "paid",
        "refund": "refund",
        "refunded": "refund",
    }
    orders = orders.assign(status_clean=apply_mapping(status_norm, mapping))

    orders = add_missing_flags(orders, cols=["amount", "quantity"])

    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    pre_join_rows = len(orders)
    joined = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    assert len(joined) == pre_join_rows, "Row count changed after join (join explosion?)"

    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, col="amount", k=1.5)

    return joined
log = logging.getLogger(__name__)


def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    cfg.out_analytics.parent.mkdir(parents=True, exist_ok=True)


    save_parquet(analytics, cfg.out_analytics)



def write_run_meta(
    cfg: ETLConfig,
    *,
    analytics: pd.DataFrame,
    orders_in: pd.DataFrame | None = None,
    users_in: pd.DataFrame | None = None,
) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = 1.0 - float(analytics["country"].isna().mean()) if "country" in analytics.columns else None

    meta = {
        # row counts
        "rows_in": {
            "orders": int(len(orders_in)) if orders_in is not None else None,
            "users": int(len(users_in)) if users_in is not None else None,
        },
        "rows_out": {
            "analytics": int(len(analytics)),
        },

       
        "missing_created_at": missing_created_at,

        "country_match_rate": country_match_rate,

        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    log.info("Loading inputs")
    orders_raw, users_raw = load_inputs(cfg)
    log.info("Rows in: orders=%s users=%s", len(orders_raw), len(users_raw))

    log.info("Transforming")
    analytics = transform(orders_raw, users_raw)
    log.info("Rows out: analytics=%s", len(analytics))

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics, users_raw, cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, analytics=analytics, orders_in=orders_raw, users_in=users_raw)