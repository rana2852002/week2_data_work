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
