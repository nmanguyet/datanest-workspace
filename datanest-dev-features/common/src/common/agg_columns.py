import re
from typing import List, Optional, Tuple

_SUB_ENTROPY_SQL = "({column} * 1.0) / SUM({column}) OVER (PARTITION BY phone_number)"
_ENTROPY_SQL = "ROUND(-SUM(CASE WHEN {column} > 0 THEN {column} * LOG2({column}) ELSE 0 END), 6)"
_INTERSECTION_SQL = """
    ROUND(COALESCE(SIZE(ARRAY_INTERSECT({current_column}, {historical_column})) * 1.0
        /
        NULLIF(SIZE(ARRAY_UNION({current_column}, {historical_column})), 0),
    0), 6)
"""

_EXCEPT_SQL = """
    ROUND(COALESCE(SIZE(ARRAY_EXCEPT({current_column}, {historical_column})) * 1.0
        /
        NULLIF(SIZE(ARRAY_UNION({current_column}, {historical_column})), 0),
    0), 6)
"""


def parse_window(w: str) -> Optional[Tuple[int, str]]:
    if not w:
        return None
    m = re.search(r"last\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)", w.lower(), re.IGNORECASE)
    if not m:
        raise ValueError(f"Invalid window: {w!r}")
    value, unit = int(m.group(1)), m.group(2).upper()
    return value, unit if unit.endswith("S") else unit + "S"


def interval_from_window(w: Optional[Tuple[int, str]]) -> Optional[str]:
    if not w:
        return None
    value, unit = w
    return f"INTERVAL {value} {unit}"


def suffix_from_window(w: Optional[Tuple[int, str]]) -> Optional[str]:
    if not w:
        return None
    value, unit = w
    return f"l{value}{unit[0].lower()}"


def get_lxw_windows(window_weeks: List[int]) -> List[Tuple[str, str]]:
    return [(f"last {x} weeks", "") for x in window_weeks]


def get_lxw_overlap_windows(window_weeks: List[int]) -> List[Tuple[str, str]]:
    return (
        [(f"last {x} weeks", "") for x in window_weeks]
        + [(f"last {x * 2} weeks", f"last {x} weeks") for x in window_weeks]
    )


def get_feature_columns(df, exclude: Optional[list] = None) -> List[str]:
    exclude = exclude or []
    return [c for c in df.columns if "to" not in c and c not in exclude]


def build_groupby_window_query(
    agg_exprs: List[Tuple[str, str]],
    windows: List[Tuple[str, str]],
    snapshot_date: str,
    date_col: str = "date",
    group_by: Optional[List[str]] = None,
    source: Optional[str] = None,
) -> Tuple[str, str]:
    if isinstance(group_by, str):
        group_by = [group_by]
    group_by = group_by or []

    select_parts = list(group_by)

    for start_w, end_w in windows:
        start_parsed = parse_window(start_w)
        end_parsed = parse_window(end_w)
        start_i = interval_from_window(start_parsed)
        end_i = interval_from_window(end_parsed)

        parts = []
        if start_parsed:
            parts.append(suffix_from_window(start_parsed))
        if end_parsed:
            parts.append(suffix_from_window(end_parsed))
        win_suffix = "_to_".join(parts) or "current"

        if start_i and end_i:
            condition = (
                f"{date_col} > DATE('{snapshot_date}') - {start_i} "
                f"AND {date_col} <= DATE('{snapshot_date}') - {end_i}"
            )
        elif start_i:
            condition = f"{date_col} > DATE('{snapshot_date}') - {start_i}"
        elif end_i:
            condition = f"{date_col} <= DATE('{snapshot_date}') - {end_i}"
        else:
            condition = f"{date_col} <= DATE('{snapshot_date}')"

        for stat_expr, alias in agg_exprs:
            if "DISTINCT" in stat_expr.upper():
                m = re.match(r"(\w+)\s*\(\s*DISTINCT\s+(.+?)\s*\)", stat_expr, re.I)
                if not m:
                    raise ValueError(f"Invalid DISTINCT stat: {stat_expr!r}")
                func, col = m.groups()
                expr = f"{func}(DISTINCT CASE WHEN {condition} THEN {col} END)"
            else:
                expr = f"{stat_expr} FILTER (WHERE {condition})"

            select_parts.append(f"{expr} AS {alias}_{win_suffix}")

    select_sql = ",\n    ".join(select_parts)
    query = f"SELECT\n    {select_sql}"

    if source:
        query += f"\nFROM {source}"
    if group_by:
        query += f"\nGROUP BY {', '.join(group_by)}"

    return query, select_sql


def build_hist_column(col: str, window: str, multiplier: int = 2) -> str:
    return re.sub(r"_l\d+w$", f"_l{int(window) * multiplier}w_to_l{window}w", col)


def _resolve_stat_func(stat, prev_columns):
    if isinstance(stat, str):
        return stat, prev_columns
    func = stat["func"]
    cols = prev_columns
    if stat.get("include"):
        cols = [c for c in cols if any(p in c for p in stat["include"])]
    if stat.get("exclude"):
        cols = [c for c in cols if not any(p in c for p in stat["exclude"])]
    return func, cols

def build_aggregate_columns(
    prev_columns: List[str],
    stat_funcs: list,
    window_multiplier: int = 2,
    entropy_partition_col: str = "phone_number",
    entropy_sub_template: Optional[str] = None,
    entropy_template: Optional[str] = None,
    intersection_template: Optional[str] = None,
    except_template: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    entropy_sub_template = entropy_sub_template or _SUB_ENTROPY_SQL
    entropy_template = entropy_template or _ENTROPY_SQL
    intersection_template = intersection_template or _INTERSECTION_SQL
    except_template = except_template or _EXCEPT_SQL

    sub_entropy_filled = entropy_sub_template.replace(
        "SUM({column}) OVER (PARTITION BY phone_number)",
        f"SUM({{{column}}}) OVER (PARTITION BY {entropy_partition_col})",
    )

    columns = []
    sub_columns = []

    for stat in stat_funcs:
        func, cols = _resolve_stat_func(stat, prev_columns)
        if not cols:
            continue

        if func in ("intersection_ratio", "except_ratio"):
            for col in cols:
                match = re.search(r"_l(\d+)w$", col)
                if not match:
                    continue
                window = match.group(1)
                base = re.sub(r"_l\d+w$", "", col)
                hist = build_hist_column(col, window, window_multiplier)
                template = intersection_template if func == "intersection_ratio" else except_template
                expr = template.format(current_column=col, historical_column=hist)
                columns.append(f"{expr} AS {base}_{func}_l{window}w")

        elif func == "entropy":
            inputs = []
            for col in cols:
                match = re.search(r"_l(\d+)w$", col)
                if not match:
                    continue
                window = match.group(1)
                base = re.sub(r"_l\d+w$", "", col)
                sub_alias = f"{base}_proba_l{window}w"
                sub_expr = sub_entropy_filled.format(column=col)
                sub_columns.append(f"{sub_expr} AS {sub_alias}")
                inputs.append((sub_alias, window, base))

            for sub_alias, window, base in inputs:
                expr = entropy_template.format(column=sub_alias)
                columns.append(f"{expr} AS {base}_entropy_l{window}w")

        else:
            skipped = []
            for col in cols:
                if "_to_" in col:
                    skipped.append(col)
                    continue
                new_alias = re.sub(r"_l(\d+)w$", rf"_{func}_l\1w", col).strip()
                columns.append(f"{func}({col}) AS {new_alias}")
            if skipped:
                print(f"  [build_aggregate_columns] Skipped {func} on overlap columns: {skipped}")

    return columns, sub_columns