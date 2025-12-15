# sql_generator.py
from typing import Dict, Any, List, Tuple
from sqlalchemy import text

ALLOWED_METRICS = {"revenue", "units_sold", "avg_selling_price"}
ALLOWED_GROUP_BY = {"region", "item_type", "channel"}  # keep MVP tight

def _col(mapping: Dict[str, Any], role: str) -> str:
    col = mapping.get(role)
    if not col:
        raise ValueError(f"Dataset does not support role '{role}' (column missing).")
    return col

def build_analytics_sql(
    table_name: str,
    table_metadata: Dict[str, Any],
    query_intent: Dict[str, Any]
) -> Tuple[Any, Dict[str, Any]]:
    """
    Returns (sqlalchemy.text(sql), params_dict).
    """
    mapping = table_metadata["column_mapping"]

    # ---- Validate metrics ----
    metrics = query_intent.get("metrics", [])
    for m in metrics:
        if m not in ALLOWED_METRICS:
            raise ValueError(f"Unsupported metric '{m}' in MVP.")

    # ---- Validate group_by ----
    group_by_roles = query_intent.get("group_by", [])
    for g in group_by_roles:
        if g not in ALLOWED_GROUP_BY:
            raise ValueError(f"Unsupported group_by '{g}' in MVP.")

    # ---- SELECT clause ----
    select_parts: List[str] = []
    group_parts: List[str] = []

    # group-by columns in SELECT
    for g in group_by_roles:
        c = _col(mapping, g)
        select_parts.append(f'"{c}" AS "{g}"')
        group_parts.append(f'"{c}"')

    # metrics in SELECT
    # Note: columns stored as TEXT in MVP; cast to numeric safely.
    if "revenue" in metrics:
        rev_col = _col(mapping, "revenue")
        select_parts.append(f'SUM(("{rev_col}")::numeric) AS revenue')
    if "units_sold" in metrics:
        u_col = _col(mapping, "units_sold")
        select_parts.append(f'SUM(("{u_col}")::numeric) AS units_sold')
    if "avg_selling_price" in metrics:
        # if price column exists, use AVG(price)
        # else compute revenue/units if both available
        p_col = mapping.get("avg_selling_price")
        if p_col:
            select_parts.append(f'AVG(("{p_col}")::numeric) AS avg_selling_price')
        else:
            # derived avg = revenue/units requires both
            rev_col = _col(mapping, "revenue")
            u_col = _col(mapping, "units_sold")
            select_parts.append(
                f'(SUM(("{rev_col}")::numeric) / NULLIF(SUM(("{u_col}")::numeric), 0)) AS avg_selling_price'
            )

    if not select_parts:
        raise ValueError("No select fields requested.")

    # ---- WHERE clause ----
    params: Dict[str, Any] = {}
    where_parts: List[str] = []

    filters = query_intent.get("filters", {})

    if filters.get("region"):
        c = _col(mapping, "region")
        where_parts.append(f'"{c}" = ANY(:regions)')
        params["regions"] = filters["region"]

    if filters.get("item_type"):
        c = _col(mapping, "item_type")
        where_parts.append(f'"{c}" = ANY(:item_types)')
        params["item_types"] = filters["item_type"]

    if filters.get("channel"):
        c = _col(mapping, "channel")
        where_parts.append(f'"{c}" = ANY(:channels)')
        params["channels"] = filters["channel"]

    if filters.get("date_range"):
        dcol = _col(mapping, "date")
        start, end = filters["date_range"]
        where_parts.append(f'("{dcol}")::date BETWEEN :start_date::date AND :end_date::date')
        params["start_date"] = start
        params["end_date"] = end

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    group_sql = (" GROUP BY " + ", ".join(group_parts)) if group_parts else ""

    sql = f'SELECT {", ".join(select_parts)} FROM "{table_name}"{where_sql}{group_sql};'
    return text(sql), params
