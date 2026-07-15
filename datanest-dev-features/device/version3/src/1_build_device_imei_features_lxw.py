from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import functions as F

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
    register_temp_view
)

import config
importlib.reload(config)

spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 8,
        'executor.cores': 8,
        'executor.memory': '20g',
    },
    script_name="build_device_imei_features"
)
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# =====================
# DATA SOURCE BUILDERS
# =====================
def df_pn_imei_tac_raw_weekly():
    return (
        spark.read.parquet(config.IMEI_WEEKLY_HC_SAMPLE_PATH)
        .filter((F.col("date") > config.START_DATE_IMEI_FEATURES)
                & (F.col("date") <= config.SNAPSHOT_DATE_STR)
        )
    )


def df_pn_last_activated_date():
    return (
        spark.read.parquet(config.LAST_ACTIVATED_PATH)
        .filter((F.col("date") > config.START_DATE_LAST_ACTIVATED_DATE)
                & (F.col("date") <= config.SNAPSHOT_DATE_STR)
        )
        .groupBy("phone_number")
        .agg(F.max("last_activated_date").alias("last_activated_date"))
    )


def df_pn_imei_tac_weekly():
    return (
        df_pn_imei_tac_raw_weekly()
        .join(df_pn_last_activated_date(), "phone_number")
        .where("date > last_activated_date")
        .drop("last_activated_date")
    )

# =====================
# IMEI FEATURE BUILDERS
# =====================
def df_imei_shared_pn_counts_lxw():
    source = register_temp_view(df_pn_imei_tac_weekly(), "df_pn_imei_tac_weekly")
    stats = [("count(distinct phone_number)", "device_imei_distinct_pn_count")]
    query, _ = build_groupby_window_query(
        agg_exprs=stats,
        windows=get_lxw_windows(config.WINDOW_WEEKS),
        snapshot_date=config.SNAPSHOT_DATE_STR,
        group_by=["imei"],
        source=source,
    )
    return spark.sql(query)


def df_pn_by_imei_event_stats_lxw():
    source = register_temp_view(df_pn_imei_tac_weekly(), "df_pn_imei_tac_weekly")
    stats = [
        ("count(phone_number)", "device_pn_imei_events_count"),
        ("count(distinct date)", "device_pn_imei_distinct_weeks_count"),
        ("sum(device_num_day_l1w)", "device_pn_imei_distinct_days_count"),
    ]
    query, _ = build_groupby_window_query(
        agg_exprs=stats,
        windows=get_lxw_windows(config.WINDOW_WEEKS),
        snapshot_date=config.SNAPSHOT_DATE_STR,
        group_by=["phone_number", "imei"],
        source=source,
    )
    return spark.sql(query)

def df_pn_imei_distinct_counts_lxw():
    source = register_temp_view(df_pn_imei_tac_weekly(), "df_pn_imei_tac_weekly")
    stats = [
        ("count(distinct imei)", "device_pn_imei_distinct_count"),
    ]

    query, _ = build_groupby_window_query(
        agg_exprs=stats,
        windows=get_lxw_windows(config.WINDOW_WEEKS),
        snapshot_date=config.SNAPSHOT_DATE_STR,
        group_by=["phone_number"],
        source=source,
    )

    return spark.sql(query)

# =============================
# PHONE NUMBER IMEI OVERLAP FEATURES
# =============================
def df_pn_imei_overlap_features_lxw():
    def df_pn_imei_sets_lxw():
        source = register_temp_view(df_pn_imei_tac_weekly(), "df_pn_imei_tac_weekly")
        windows = get_lxw_overlap_windows(config.WINDOW_WEEKS[:-1])
        stats = [("collect_set(imei)", "device_imei_set")]
        query, _ = build_groupby_window_query(
            agg_exprs=stats, windows=windows, snapshot_date=config.SNAPSHOT_DATE_STR,
            group_by=["phone_number"], source=source,
        )
        return spark.sql(query)

    sets_df = df_pn_imei_sets_lxw()
    source = register_temp_view(sets_df, "df_pn_imei_sets_lxw")
    prev_columns = get_feature_columns(sets_df, exclude=["phone_number"])
    columns, _ = build_aggregate_columns(prev_columns, ["intersection_ratio", "except_ratio"])

    query = f"select phone_number, {', '.join(columns)} from {source}"
    return spark.sql(query)

# ==================================
# PHONE NUMBER DISTRIBUTION FEATURES
# ==================================
def df_pn_imei_distribution_lxw():
    df_join = (
        df_pn_by_imei_event_stats_lxw()
        .join(df_imei_shared_pn_counts_lxw(), on="imei", how="left")
    )

    source = register_temp_view(df_join, "df_join")
    prev_columns = get_feature_columns(df_join, exclude=["phone_number", "imei"])

    stat_config = [
        {"func": "min", "exclude": ['device_pn_imei_events_count', 'device_imei_distinct_pn_count']},
        {"func": "max"},
        {"func": "avg", "exclude": ['device_imei_distinct_pn_count']},
        {"func": "std"},
        {"func": "skewness"},
        {"func": "kurtosis"},
    ]
    columns, _ = build_aggregate_columns(prev_columns, stat_config)

    query = f"select phone_number, {', '.join(columns)} from {source} group by phone_number"
    return spark.sql(query)

# =============================
# PHONE NUMBER ENTROPY FEATURES
# =============================
def df_pn_imei_entropy_features_lxw():
    df_join = (
        df_pn_by_imei_event_stats_lxw()
        .join(df_imei_shared_pn_counts_lxw(), on="imei", how="left")
    )

    source = register_temp_view(df_join, "df_join")
    prev_columns = get_feature_columns(df_join, exclude=["phone_number", "imei"])
    columns, sub_columns = build_aggregate_columns(prev_columns, ["entropy"])

    query = f"""
    select phone_number, {', '.join(columns)}
    from (select phone_number, {', '.join(sub_columns)} from {source})
    group by phone_number
    """
    return spark.sql(query)


# ==============
# FINAL FEATURES
# ==============
def df_pn_imei_features_lxw():
    df = (
        df_pn_imei_distribution_lxw()
        .join(df_pn_imei_entropy_features_lxw(), on="phone_number", how="outer")
        .join(df_pn_imei_distinct_counts_lxw(), on="phone_number", how="outer")
        .join(df_pn_imei_overlap_features_lxw(), on="phone_number", how="outer")
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

    df_pn_imei_features_lxw().write.mode('overwrite').parquet(
        f'{config.DEVICE_IMEI_FEATURES_HC_SAMPLE_PATH}/date={config.SNAPSHOT_DATE_STR}'
    )

    end_time = time.time()
    print(f'Done at: {datetime.now()} during {end_time - start_time}')
    print('-' * 50)


if __name__ == "__main__":
    main()
