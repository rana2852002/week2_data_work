from pathlib import Path
import pandas as pd

NA_MARKERS = ["", "na", "n/a", "null", "none", "None"]

def load_orders_csv(file_path: Path) -> pd.DataFrame:
   
    return pd.read_csv(
        file_path,
        dtype={
            "order_id": "string",
            "user_id": "string",
        },
        na_values=NA_MARKERS,
        keep_default_na=True,
    )


def load_users_csv(file_path: Path) -> pd.DataFrame:
   
    return pd.read_csv(
        file_path,
        dtype={"user_id": "string"},
        na_values=NA_MARKERS,
        keep_default_na=True,
    )


def save_parquet(df: pd.DataFrame, output_path: Path) -> None:
    
    output_path.parent.mkdir(exist_ok=True, parents=True)
    df.to_parquet(output_path, index=False)


def load_parquet(input_path: Path) -> pd.DataFrame:
    
    return pd.read_parquet(input_path)
