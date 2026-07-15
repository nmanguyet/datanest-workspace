from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.import StorageLevel

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
)

import config

importlib.reload(config)

spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 6,
        'executor.cores': 6,
        'executor.memory': '16g',
    },
    script_name="build_device_current_tac_features"
)
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# ==============================================================================
# SOURCE
# ==============================================================================
def df_pn_imei_tac_raw_weekly():
    return (
        spark.read.parquet(config.IMEI_WEEKLY_HC_SAMPLE_PATH)
        .filter((F.col("date") > config.START_DATE_CURRENT_TAC_FEATURES) & (F.col("date") <= config.SNAPSHOT_DATE_STR)
                & F.col("imei").isNotNull())
        .dropDuplicates()
    )

def df_tac_device_mapping():
    return F.broadcast(spark.read.parquet(config.TAC_MAPPING_PATH))

def df_pn_last_activated_date():
    return (
        spark.read.parquet(config.LAST_ACTIVATED_PATH)
        .filter((F.col("date") > config.START_DATE_LAST_ACTIVATED_DATE) & (F.col("date") <= config.SNAPSHOT_DATE_STR))
        .groupBy("phone_number")
        .agg(F.max("last_activated_date").alias("last_activated_date"))
    )

# ==============================================================================
# BASE
# ==============================================================================
def df_pn_imei_tac_l2w():
    df = (
        df_pn_imei_tac_raw_weekly()
        .join(df_pn_last_activated_date(), on="phone_number", how="inner")
        .where(F.col("date") >= F.col("last_activated_date"))
        .drop("last_activated_date")
    )
    max_date = F.max("date").over(Window.partitionBy("phone_number"))
    return (
        df.withColumn("max_date", max_date)
        .withColumn(
            "is_current",
            F.when(F.datediff(F.col("max_date"), F.col("date")) < config.CURRENT_TAC_WEEKS * 7, 1).otherwise(0),
        )
        .filter("is_current = 1")
        .drop("max_date", "is_current")
    )

# ==============================================================================
# RANKING
# ==============================================================================
def df_pn_by_tac_event_stats_ranked_l2w(tac_base=None):
    base = tac_base if tac_base is not None else df_pn_imei_tac_l2w()
    source = register_temp_view(base, "df_pn_imei_tac")
    
    stats = [
        ("count(*)", "device_tac_events_count"),
        ("count(distinct date)", "device_tac_active_days"),
        ("max(date)", "device_tac_last_seen_date"),
    ]
    
    query, _ = build_groupby_window_query(
        agg_exprs=stats,
        windows=get_lxw_windows([config.CURRENT_TAC_WEEKS]),
        snapshot_date=config.SNAPSHOT_DATE_STR,
        group_by=["phone_number", "tac"],
        source=source,
    )
    
    w = (
        Window.partitionBy("phone_number")
        .orderBy(
            F.col(f"device_tac_active_days_l{config.CURRENT_TAC_WEEKS}w").desc(),
            F.col(f"device_tac_last_seen_date_l{config.CURRENT_TAC_WEEKS}w").desc(),
            F.col("tac").asc(),
        )
    )
    return spark.sql(query).withColumn("rank", F.row_number().over(w))

# ==============================================================================
# CURRENT DEVICE
# ==============================================================================
def df_pn_current_tac_features_l2w():
    suffix = config.CURRENT_TAC_SUFFIX
    
    tac_base = df_pn_imei_tac_l2w().persist(StorageLevel.MEMORY_AND_DISK)
    
    df_total = tac_base.groupBy("phone_number").agg(
        F.count("imei").alias(f"device_active_events_{suffix}"),
        F.countDistinct("date").alias(f"device_active_days_{suffix}"),
    )
    
    ranked = df_pn_by_tac_event_stats_ranked_l2w(tac_base).persist(StorageLevel.MEMORY_AND_DISK)
    
    df_join = df_total
    for cfg in config.CURRENT_TAC_RANKS:
        subset = (
            ranked.filter(F.col("rank") == cfg["rank"])
            .select(
                "phone_number",
                F.col("tac").alias(f"{cfg['prefix']}_tac_{suffix}"),
                F.col(f"device_tac_active_days_l{config.CURRENT_TAC_WEEKS}w").alias(f"{cfg['prefix']}_tac_active_days_{suffix}"),
                F.col(f"device_tac_last_seen_date_l{config.CURRENT_TAC_WEEKS}w").alias(f"{cfg['prefix']}_tac_last_seen_date_{suffix}"),
                F.col(f"device_tac_events_count_l{config.CURRENT_TAC_WEEKS}w").alias(f"{cfg['prefix']}_tac_events_count_{suffix}"),
            )
        )
        df_join = df_join.join(subset, on="phone_number", how="left")
        
    tac_base.unpersist()
    ranked.unpersist()
    
    source = register_temp_view(df_join, "df_join")
    
    columns = ["*"]
    for cfg in config.CURRENT_TAC_RANKS:
        p = cfg["prefix"]
        columns += [
            build_ratio_column(
                numerator=f"{p}_tac_active_days_{suffix}",
                denominator=f"device_active_days_{suffix}",
                alias=f"{p}_dominant_day_ratio_{suffix}",
            ),
            build_days_since_column(
                column=f"{p}_tac_last_seen_date_{suffix}",
                alias=f"{p}_days_since_last_seen_{suffix}",
                snapshot_date_str=config.SNAPSHOT_DATE_STR,
            ),
            build_ratio_column(
                numerator=f"{p}_tac_events_count_{suffix}",
                denominator=f"device_active_events_{suffix}",
                alias=f"{p}_dominant_events_ratio_{suffix}",
            ),
        ]
        
    query = f"select {', '.join(columns)} from {source}"
    df_ratio = spark.sql(query)
    
    for cfg in config.CURRENT_TAC_RANKS:
        p = cfg["prefix"]
        df_ratio = df_ratio.join(
            df_tac_device_mapping().selectExpr(
                f"tac as {p}_tac_map",
                f"device_brand as {p}_brand_{suffix}",
                f"device_model as {p}_model_{suffix}",
            ),
            F.col(f"{p}_tac_{suffix}") == F.col(f"{p}_tac_map"),
            "left",
        )
        
    drop_cols = [item for cfg in config.CURRENT_TAC_RANKS
                      for item in [f"{cfg['prefix']}_brand_{suffix}", f"{cfg['prefix']}_model_{suffix}"]]
                      
    columns = [c for c in df_ratio.columns if c not in drop_cols] + [c for cfg in config.CURRENT_TAC_RANKS for c in build_tac_columns(cfg["prefix"], suffix)]
    
    source = register_temp_view(df_ratio, "df_final")
    query = f"select {', '.join(columns)} from {source}"
    
    df = spark.sql(query)
    
    return df.select([
        F.when(F.isnan(c), None).otherwise(F.col(c)).alias(c)
        for c in df.columns
    ])

def main():
    snapshot_date_str = sys.argv[1]
    start_time = time.time()

    config.configure(snapshot_date_str)

    print(datetime.now(), config.SNAPSHOT_DATE_STR)

    df_pn_current_tac_features_l2w().write.mode('overwrite').parquet(
        f'{config.DEVICE_CURRENT_TAC_FEATURES_HC_SAMPLE_PATH}/date={config.SNAPSHOT_DATE_STR}'
    )

    end_time = time.time()
    print(f'Done at: {datetime.now()} during {end_time - start_time}')
    print('-' * 50)


if __name__ == "__main__":
    main()
