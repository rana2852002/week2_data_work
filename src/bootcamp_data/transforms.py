import pandas as pd

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()

    # IDs as strings
    clean["order_id"] = clean["order_id"].astype("string")
    clean["user_id"] = clean["user_id"].astype("string")

    # Numerics with safe coercion
    clean["amount"] = pd.to_numeric(clean["amount"], errors="coerce").astype("Float64")
    clean["quantity"] = pd.to_numeric(clean["quantity"], errors="coerce").astype("Int64")

    return clean



def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)

    records = []
    for col in df.columns:
        n_miss = int(df[col].isna().sum())
        records.append(
            {
                "column": col,
                "n_missing": n_miss,
                "p_missing": n_miss / total if total > 0 else 0.0,
            }
        )

    return (
        pd.DataFrame(records)
        .set_index("column")
        .sort_values("p_missing", ascending=False)
    )


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    flagged = df.copy()

    for col in cols:
        if col in flagged.columns:
            flagged[f"{col}_missing"] = flagged[col].isna()

    return flagged


def normalize_text(s: pd.Series) -> pd.Series:
    out = s.astype("string")

    out = out.str.strip()

    out = out.str.lower()

    out = out.str.split().str.join(" ")

    return out


def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    # replace only known keys, keep others as-is
    return s.where(~s.isin(mapping.keys()), s.map(mapping))



def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    data = df.copy()
    data = data.sort_values(ts_col)
    data = data.drop_duplicates(subset=key_cols, keep="last")
    data = data.reset_index(drop=True)

    return data
import pandas as pd


def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    out = df.copy()
    out[col] = pd.to_datetime(out[col], errors="coerce", utc=utc)
    return out


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    out = df.copy()
    ts = out[ts_col]

    # parts
    out["year"] = ts.dt.year
    out["month"] = ts.dt.month
    out["day"] = ts.dt.day
    out["hour"] = ts.dt.hour
    out["dow"] = ts.dt.day_name()

    return out


def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    clean = s.dropna()

    q1 = clean.quantile(0.25)
    q3 = clean.quantile(0.75)

    iqr = q3 - q1

    lower = q1 - k * iqr
    upper = q3 + k * iqr

    return float(lower), float(upper)


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    clean = s.dropna()

    low_val = clean.quantile(lo)
    high_val = clean.quantile(hi)

    return s.clip(lower=low_val, upper=high_val)

def add_outlier_flag(df: pd.DataFrame, col: str, k: float = 1.5) -> pd.DataFrame:
    out = df.copy()
    lo, hi = iqr_bounds(out[col], k=k)
    out[f"{col}_is_outlier"] = (out[col] < lo) | (out[col] > hi)
    return out