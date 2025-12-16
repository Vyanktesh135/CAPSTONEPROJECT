import re
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session
from model import DatabaseMetadata

def _clean_numeric_str(x):
    if isinstance(x, str):
        x = x.strip()
        x = re.sub(r"[\$,₹€£]", "", x)
        x = x.replace(",", "")
    return x

def infer_col_type(series: pd.Series, sample_size: int = 500) -> str:
    s = series.dropna()
    if len(s) == 0:
        return "string"

    s = s.head(sample_size)

    # normalize strings once
    low = s.astype(str).str.strip().str.lower()

    # boolean check (before numeric)
    if float(low.isin({"true","false","yes","no","y","n"}).mean()) >= 0.95:
        return "boolean"

    # numeric check
    cleaned = low.map(_clean_numeric_str)
    parsed_num = pd.to_numeric(cleaned, errors="coerce")
    num_ratio = float(parsed_num.notna().mean())

    if num_ratio >= 0.95:
        return "numeric"

    # date check (exclude pure numerics)
    parsed_date = pd.to_datetime(low, errors="coerce")
    date_ratio = float(parsed_date.notna().mean())

    if date_ratio >= 0.90:
        return "date"

    return "string"


def profile_df(df: pd.DataFrame, sample_size: int = 500) -> Dict[str, Any]:
    cols_meta = []
    for col in df.columns:
        ctype = infer_col_type(df[col], sample_size=sample_size)
        meta = {
            "name": col,
            "type": ctype,
            "distinct_count": int(df[col].nunique(dropna=True))
        }

        if ctype == "numeric":
            parsed = pd.to_numeric(df[col].map(_clean_numeric_str), errors="coerce").dropna()
            if len(parsed):
                meta["min"] = float(parsed.min())
                meta["max"] = float(parsed.max())

        elif ctype == "date":
            parsed = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True).dropna()
            if len(parsed):
                meta["min"] = parsed.min().date().isoformat()
                meta["max"] = parsed.max().date().isoformat()

        else:
            # small helpful preview
            vc = df[col].dropna().astype(str).value_counts().head(10)
            meta["top_values"] = vc.index.tolist()

        cols_meta.append(meta)

    return {"columns": cols_meta}


# -------------------------
# Helpers: role mapping
# -------------------------

ROLE_KEYWORDS = {
    "region": ["region", "territory", "market", "area"],
    "item_type": ["item_type", "item", "productline", "product_line", "category", "product"],
    "channel": ["channel", "sales_channel", "online", "offline"],
    "date": ["date", "orderdate", "order_date", "transactiondate", "transaction_date"],
    "units_sold": ["units_sold", "units", "quantity", "qty", "quantityordered", "quantity_ordered"],
    "revenue": ["revenue", "sales", "total_revenue", "amount", "total_sales", "turnover"],
    "avg_selling_price": ["avg_price", "average_price", "price", "priceeach", "unit_price", "unitprice"],
}

def score_name(col: str, keywords: List[str]) -> int:
    col_l = col.lower()
    score = 0
    for kw in keywords:
        kw_l = kw.lower()
        if col_l == kw_l:
            score += 5
        elif kw_l in col_l:
            score += 2
    return score

def build_type_lookup(columns_meta: List[Dict[str, Any]]) -> Dict[str, str]:
    return {c["name"]: c["type"] for c in columns_meta}

def infer_column_mapping(columns: List[str], type_lookup: Dict[str, str]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {k: None for k in ROLE_KEYWORDS.keys()}

    # candidates per role
    for role, kws in ROLE_KEYWORDS.items():
        best = None
        best_score = -1
        for col in columns:
            s = score_name(col, kws)
            # light type preference
            if role == "date" and type_lookup.get(col) == "date":
                s += 3
            if role in ("revenue", "units_sold", "avg_selling_price") and type_lookup.get(col) == "numeric":
                s += 2
            if role in ("region", "item_type", "channel") and type_lookup.get(col) == "string":
                s += 1

            if s > best_score:
                best_score = s
                best = col

        # accept only if score is meaningful, else None
        mapping[role] = best if best_score >= 2 else None

    # common conflict handling: revenue vs avg_selling_price if both numeric
    # If avg_selling_price got mapped to revenue column etc., you can refine using magnitude
    return mapping


# -------------------------
# DB helpers
# -------------------------

def fetch_sample_df(db: Session, table_name: str, limit: int = 2000) -> pd.DataFrame:
    # Pull small sample for profiling
    q = text(f'SELECT * FROM "{table_name}" LIMIT :limit')
    rows = db.execute(q, {"limit": limit}).mappings().all()
    return pd.DataFrame(rows)

def fetch_row_count(db: Session, table_name: str) -> int:
    q = text(f'SELECT COUNT(*) AS cnt FROM "{table_name}"')
    return int(db.execute(q).scalar_one())

def fetch_distinct_values(db: Session, table_name: str, col: str, limit: int = 2000) -> List[str]:
    # limit to keep metadata small
    q = text(f'SELECT DISTINCT "{col}" AS v FROM "{table_name}" WHERE "{col}" IS NOT NULL LIMIT :lim')
    vals = db.execute(q, {"lim": limit}).scalars().all()
    return [str(v) for v in vals if v is not None]

def fetch_min_max_date(db: Session, table_name: str, date_col: str) -> Tuple[Optional[str], Optional[str]]:
    # If column stored as TEXT, cast carefully
    q = text(f'''
        SELECT
          MIN(("{date_col}")::date) AS min_d,
          MAX(("{date_col}")::date) AS max_d
        FROM "{table_name}"
        WHERE "{date_col}" IS NOT NULL
    ''')
    row = db.execute(q).mappings().first()
    if not row:
        return None, None
    min_d = row["min_d"].isoformat() if row["min_d"] else None
    max_d = row["max_d"].isoformat() if row["max_d"] else None
    return min_d, max_d


# -------------------------
# Main background task
# -------------------------

def infer_and_store_metadata(db: Session, datasetid,file_name: str, table_name: str) -> Dict[str, Any]:
    """
    Run after upload.
    Reads from Postgres and updates metadata_table.table_metadata.
    """
    # 1) sample for profiling
    df_sample = fetch_sample_df(db, table_name, limit=2000)
    if df_sample.empty:
        meta = {"status": "error", "error": "Table is empty."}
        return meta

    # 2) profile types + per-column info
    prof = profile_df(df_sample, sample_size=500)
    type_lookup = build_type_lookup(prof["columns"])

    # 3) role mapping (merchandising roles)
    cols = list(df_sample.columns)
    column_mapping = infer_column_mapping(cols, type_lookup)

    # 4) dataset stats
    row_count = fetch_row_count(db, table_name)
    stats = {"row_count": row_count}

    date_col = column_mapping.get("date")
    if date_col:
        min_d, max_d = fetch_min_max_date(db, table_name, date_col)
        stats["min_date"] = min_d
        stats["max_date"] = max_d

    # 5) distinct values for core dims (if found)
    distinct_values = {"regions": [], "item_types": [], "channels": []}
    if column_mapping.get("region"):
        distinct_values["regions"] = fetch_distinct_values(db, table_name, column_mapping["region"])
    if column_mapping.get("item_type"):
        distinct_values["item_types"] = fetch_distinct_values(db, table_name, column_mapping["item_type"])
    if column_mapping.get("channel"):
        distinct_values["channels"] = fetch_distinct_values(db, table_name, column_mapping["channel"])

    meta: Dict[str, Any] = {
        "status": "ready",
        "file_name": file_name,
        "table_name": table_name,
        "column_mapping": column_mapping,
        "distinct_values": distinct_values,
        "stats": stats,
        "columns": prof["columns"],
        "version": 1
    }
    print("Metadata : ",meta)
    result = db.query(DatabaseMetadata).filter(DatabaseMetadata.id == datasetid).first()
    result.table_metadata = meta
    db.add(result)
    db.commit()
    db.refresh(result)
    print("Data has been added ..!")
    return meta
