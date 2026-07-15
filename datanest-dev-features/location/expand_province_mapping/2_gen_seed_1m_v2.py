from etl.common import init_spark3
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import subprocess, sys
from functools import reduce
from pyspark.sql import DataFrame
from .hdfs_helper import set_rep
from .config import BASE_PATH
import sys

exec_date = sys.argv[1]
exec_date_dt = datetime.strptime(exec_date, "%Y-%m-%d")

target_month = exec_date_dt.month - 1 if exec_date_dt.month > 1 else 12
target_year = exec_date_dt.year if exec_date_dt.month > 1 else exec_date_dt.year - 1

start_str = f"{target_year}-{target_month:02d}-01"
end_str = f"{exec_date_dt.year}-{exec_date_dt.month:02d}-01"

INPUT_PATH = f"{BASE_PATH}/daily/seed_prov_by_rank_top1_2/"
OUTPUT_STAT_PATH = f"{BASE_PATH}/montly/seed_stat_1m/month={exec_date_dt.year}-{exec_date_dt.month:02d}/"
OUTPUT_CELL_PN_PATH = f"{BASE_PATH}/montly/seed_unique_cell_pn/month={exec_date_dt.year}-{exec_date_dt.month:02d}/"
OUTPUT_CELL_REF_CELL_PATH = f"{BASE_PATH}/montly/seed_unique_cell_ref_cell/month={exec_date_dt.year}-{exec_date_dt.month:02d}/"
OUTPUT_CELL_COUNT_DATE = f"{BASE_PATH}/montly/seed_cell_count_date/month={exec_date_dt.year}-{exec_date_dt.month:02d}/"

# Initialize Spark
spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 16,
        'executor.cores': 5,
        'executor.memory': '20g',
    },
    script_name=f"gen_seed_stat_1M",
    cluster='datanest'
)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

df_month_raw = (
    spark.read.parquet(INPUT_PATH)
    .where(f"date >= '{start_str}' AND date < '{end_str}'")
    .withColumn("month", F.lit(f"{exec_date_dt.year}-{exec_date_dt.month:02d}"))
)

df_month_stat = (
    df_month_raw
    .groupBy(
        "cell_id",
        "ref_province_name",
        # "phone_number",
        # "ref_cell_id",
    )
    .agg(
        F.sum("sum_logs_count").alias("sum_logs_count"),
        F.sum("ref_sum_logs_count").alias("ref_sum_logs_count")
    )
)

df_count_date_cell = (
    df_month_raw
    .rollup("cell_id", "ref_province_name")
    .agg(
        F.countDistinct("date").alias("count_date")
    )
)

df_cell_pn = (
    df_month_raw
    .select("cell_id", "ref_province_name", "phone_number")
    .distinct()
)

df_cell_ref_cell = (
    df_month_raw
    .select("cell_id", "ref_province_name", "ref_cell_id")
    .distinct()
)

df_month_stat.write.mode("overwrite").parquet(OUTPUT_STAT_PATH)
df_cell_pn.write.mode("overwrite").parquet(OUTPUT_CELL_PN_PATH)
df_cell_ref_cell.write.mode("overwrite").parquet(OUTPUT_CELL_REF_CELL_PATH)
df_count_date_cell.write.mode("overwrite").parquet(OUTPUT_CELL_COUNT_DATE)

set_rep(OUTPUT_STAT_PATH, 2)
set_rep(OUTPUT_CELL_PN_PATH, 2)
set_rep(OUTPUT_CELL_REF_CELL_PATH, 2)