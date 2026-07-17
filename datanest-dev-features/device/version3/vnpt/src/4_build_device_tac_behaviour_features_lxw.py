from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.import StorageLevel

import re
import sys
import time
import importlib
import pandas as pd

BASE_DIR = Path("/apps/jupyter/users/nguyetnguyen/workspace/feature")

sys.path[:0] = [str(path) for path in [
    BASE_DIR,
    BASE_DIR / "etl/vnpt/feature/device_v3",
]]

from etl.common import init_spark3
from agg_columns import (
    build_groupby_window_query,
    build_aggregate_columns,
    get_lxw_windows,
    get_lxw_overlap_windows,
    get_feature_columns,
)
from device_helpers import (
    register_temp_view,
    build_ratio_column,
    build_days_since_column,
    build_tac_columns,
    build_datediff_column
)

import config

importlib.reload(config)

spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 4,
        'executor.cores': 4,
        'executor.memory': '12g',
    },
    script_name="build_device_tac_behaviour_features"
)
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# =======================================================================
# SOURCE
# =======================================================================
def df_device_current_raw_weekly():
    return (
        spark.read.parquet(config.DEVICE_CURRENT_TAC_FEATURES_HC_SAMPLE_PATH)
        .filter(
            (F.col("date") > config.START_DATE_tac_BEHAVIOUR_FEATURES)
            & (F.col("date") <= config.SNAPSHOT_DATE_STR)
        )
        .selectExpr("phone_number", "date", f"{config.DEVICE_CURRENT_TAC_COLUMN} as device_current_l12w")
        .filter(F.col("phone_number").isNotNull() & F.col("device_current_l12w").isNotNull())
    )


def df_pn_last_activated_date():
    return (
        spark.read.parquet(config.LAST_ACTIVATED_PATH)
        .filter(
            (F.col("date") > config.START_DATE_LAST_ACTIVATED_DATE)
            & (F.col("date") <= config.SNAPSHOT_DATE_STR)
        )
        .groupBy("phone_number")
        .agg(F.max("last_activated_date").alias("start_date"))
    )


def df_device_current_weekly():
    return (
        df_device_current_raw_weekly()
        .join(df_pn_last_activated_date(), on="phone_number", how="inner")
        .where("date >= start_date")
        .drop("start_date")
        .repartition(200, "phone_number")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )


# =======================================================================
# BASE
# =======================================================================
def df_device_base_weekly():
    w_order = Window.partitionBy("phone_number").orderBy("date")
    w_prev = w_order.rowsBetween(Window.unboundedPreceding, -1)
    w_latest = Window.partitionBy("phone_number").orderBy(F.col("date").desc())

    df = (
        df_device_current_weekly()
        .withColumn("current_device", F.first("device_current_l12w", ignorenulls=True).over(w_latest))
        .withColumn("prev_device", F.lag("device_current_l12w").over(w_order))
        .withColumn("is_switch", 
            (F.col("device_current_l12w").isNotNull()
            & F.col("prev_device").isNotNull()
            & (F.col("device_current_l12w") != F.col("prev_device"))).cast("int"))
        .withColumn("switch_group", F.sum("is_switch").over(w_order))
        .withColumn("week_diff", F.expr(f"datediff('{config.SNAPSHOT_DATE_STR}', date) / 7").cast("int"))
        .withColumn("streak_length", F.count("*").over(Window.partitionBy("phone_number", "switch_group")))
    )

    for w in config.WINDOW_WEEKS:
        df = df.withColumn(f"is_l{w}w", F.col("week_diff") < w)

    for w in config.SWITCH_GAP_WINDOWS_WEEKS:
        sw_col = f"switch_week_l{w}w"
        gap_col = f"switch_tac_gap_l{w}w"
        prev_col = f"prev_switch_week_l{w}w"
        window_start = F.expr(f"date_sub('{config.SNAPSHOT_DATE_STR}', {w * 7})")

        df = (
            df
            .withColumn(sw_col,
                F.when((F.col("is_switch") == 1) & F.col(f"is_l{w}w"), F.col("date")).otherwise(F.lit(None)))
            .withColumn(prev_col, F.last(sw_col, ignorenulls=True).over(w_prev))
            .withColumn(gap_col,
                F.when((F.col("is_switch") == 1) & F.col(f"is_l{w}w"),
                    F.datediff(F.col("date"),
                        F.when(F.col(prev_col) < window_start, window_start)
                        .otherwise(F.col(prev_col))) / 7))
        )

    return df.persist(StorageLevel.MEMORY_AND_DISK)


def df_pn_tac_weekly_stats_lxw():
    source = register_temp_view(df_device_base_weekly(), "df_device_base")

    stats = [
        ("count(distinct date)", "active_weeks"),
        ("count(case when device_current_l12w = current_device then 1 end)", "device_current_tac_active_weeks"),
        ("count(distinct device_current_l12w)", "device_current_tac_distinct_count"),
        ("min(case when device_current_l12w = current_device then date end)", "current_tac_first_seen"),
        ("max(case when device_current_l12w = current_device then date end)", "current_tac_last_seen"),
    ]

    inner_query, _ = build_groupby_window_query(
        agg_exprs=stats,
        windows=get_lxw_windows(window_weeks=config.WINDOW_WEEKS),
        group_by=["phone_number"],
        source=source,
        snapshot_date=config.SNAPSHOT_DATE_STR
    )

    extra_cols = []
    for w in config.WINDOW_WEEKS:
        extra_cols.append(
            build_ratio_column(
                numerator=f"device_current_tac_active_weeks_l{w}w",
                denominator=f"active_weeks_l{w}w",
                alias=f"device_current_tac_active_ratio_l{w}w"
            )
        )
        first = f"current_tac_first_seen_l{w}w"
        last = f"current_tac_last_seen_l{w}w"
        extra_cols.append(build_datediff_column(last, first, f"device_current_tac_active_span_l{w}w"))
        # extra_cols.append(build_datediff_column(f"'{config.SNAPSHOT_DATE_STR}'", first, f"device_current_tac_days_since_first_seen_l{w}w"))
        extra_cols.append(build_datediff_column(f"'{config.SNAPSHOT_DATE_STR}'", last, f"device_current_tac_days_since_last_seen_l{w}w"))

    full_select = ",\n    ".join(extra_cols)
    query = f"SELECT *, {full_select} FROM ({inner_query})"
    return spark.sql(query)


def df_pn_tac_switch_stats_lxw():
    base_df = df_device_base_weekly()
    source = register_temp_view(base_df, "df_device_base")

    sum_query, _ = build_groupby_window_query(
        agg_exprs=[("sum(is_switch)", "device_switch_tac_count")],
        windows=get_lxw_windows(window_weeks=config.SWITCH_GAP_WINDOWS_WEEKS),
        group_by=["phone_number"],
        source=source,
        snapshot_date=config.SNAPSHOT_DATE_STR
    )
    sum_select = sum_query.split("\nFROM")[0].replace("SELECT\n    ", "")
    group_clause = "\nFROM" + sum_query.split("\nFROM")[1]

    gap_cols = [f"switch_tac_gap_l{w}w" for w in config.SWITCH_GAP_WINDOWS_WEEKS]
    gap_exprs, _ = build_aggregate_columns(
        gap_cols,
        [
            {"func": "min"},
            {"func": "max"},
            {"func": "avg"},
            {"func": "std"},
            {"func": "skewness", "exclude": ['device_switch_tac_gap']},
            {"func": "kurtosis", "exclude": ['device_switch_tac_gap']},
        ],
    )
    gap_exprs = [re.sub(r" AS (\w+)", r" AS device_\1", e) for e in gap_exprs]

    full_select = ",\n    ".join([sum_select] + list(gap_exprs))
    query = f"SELECT\n    {full_select}\n{group_clause}"
    return spark.sql(query)


def df_pn_tac_streak_features_lxw():
    source = register_temp_view(df_device_base_weekly(), "df_streak")

    inner_parts = ["phone_number", "switch_group"]
    inner_parts += [
        f"count(*) FILTER (WHERE is_l{w}w) AS streak_any_tac_week_l{w}w"
        for w in config.WINDOW_WEEKS
    ]
    inner_parts += [
        f"count(*) FILTER (WHERE is_l{w}w AND device_current_l12w = current_device) AS streak_current_tac_week_l{w}w"
        for w in config.WINDOW_WEEKS
    ]
    inner_sql = ",\n    ".join(inner_parts)

    outer_parts = ["phone_number"]
    outer_parts += [
        f"max(streak_any_tac_week_l{w}w) AS device_max_streak_any_tac_week_l{w}w"
        for w in config.WINDOW_WEEKS
    ]
    outer_parts += [
        f"max(streak_current_tac_week_l{w}w) AS device_max_streak_current_tac_week_l{w}w"
        for w in config.WINDOW_WEEKS
    ]
    outer_sql = ",\n    ".join(outer_parts)

    query = f"SELECT {outer_sql} FROM (SELECT {inner_sql} FROM {source} GROUP BY phone_number, switch_group) GROUP BY phone_number"
    return spark.sql(query)


def df_pn_tac_behaviour_features_lxw():
    weekly = df_pn_tac_weekly_stats_lxw()
    switch = df_pn_tac_switch_stats_lxw()
    streak = df_pn_tac_streak_features_lxw()

    def device_cols(df):
        return ["phone_number"] + [c for c in df.columns if c.startswith("device_")]

    df = (
        weekly.select(*device_cols(weekly))
        .join(switch.select(*device_cols(switch)), on="phone_number", how="left")
        .join(streak.select(*device_cols(streak)), on="phone_number", how="left")
    )

    return df.select([
        F.when(F.isnan(c), None).otherwise(F.col(c)).alias(c)
        for c in df.columns
    ])
    
def main():
    snapshot_date_str = sys.argv[1]
    start_time = time.time()

    config.configure(snapshot_date_str)

    print(datetime.now(), config.SNAPSHOT_DATE_STR)

    df_pn_tac_behaviour_features_lxw().write.mode('overwrite').parquet(
        f'{config.DEVICE_tac_BEHAVIOUR_FEATURES_HC_SAMPLE_PATH}/date={config.SNAPSHOT_DATE_STR}'
    )

    end_time = time.time()
    print(f'Done at: {datetime.now()} during {end_time - start_time}')
    print('-' * 50)


if __name__ == "__main__":
    main()
