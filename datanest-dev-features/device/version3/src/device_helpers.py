from typing import List

def register_temp_view(df, view_name: str) -> str:
    df.createOrReplaceTempView(view_name)
    return view_name

def build_ratio_column(numerator: str, denominator: str, alias: str) -> str:
    return f"""
    case when {denominator} is not null and {denominator} > 0 and {numerator} is not null
        then round(cast({numerator} as double) / cast({denominator} as double), 6)
        else null
    end as {alias}
    """

def build_datediff_column(col1: str, col2: str, alias: str) -> str:
    return f"datediff({col1}, {col2}) as {alias}"

def build_days_since_column(column: str, alias: str, snapshot_date_str: str) -> str:
    return build_datediff_column(f"'{snapshot_date_str}'", column, alias)

def build_binary_flag_column(condition: str, alias: str) -> str:
    return f"case when {condition} then 1 else 0 end as {alias}"

def build_tac_columns(prefix: str, suffix: str = "l12w") -> List[str]:
    return [
        f"case when {prefix}_tac_{suffix} is null then null when {prefix}_brand_{suffix} is null then 'unmapped' else lower({prefix}_brand_{suffix}) end as {prefix}_brand_{suffix}",
        f"case when {prefix}_tac_{suffix} is null then null when {prefix}_model_{suffix} is null then 'unmapped' else lower({prefix}_model_{suffix}) end as {prefix}_model_{suffix}",
        f"""
        case
            when {prefix}_tac_{suffix} is null then null
            when {prefix}_brand_{suffix} is null then 'unmapped'
            when lower({prefix}_brand_{suffix}) = 'apple'
                or lower({prefix}_model_{suffix}) like '%iphone%'
                or lower({prefix}_model_{suffix}) like '%apple%'
                then 'ios'
            else 'android'
        end as {prefix}_os_{suffix}
        """,
    ]