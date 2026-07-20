from etl.common import init_spark3
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import subprocess, sys
from functools import reduce
from pyspark.sql import DataFrame
from .config import BASE_PATH, TRAIN_CELL_MAP_PATH, TEST_CELL_MAP_PATH
from .hdfs_helper import set_rep

exec_date = sys.argv[1]
exec_date_dt = datetime.strptime(exec_date, "%Y-%m-%d")

# Check if the exec_date is not the first day of the year, skip
if exec_date_dt.month != 1 or exec_date_dt.day != 1:
    print(f"---- Skipping execution: {exec_date} is not the first day of the year.")
    sys.exit(99)

# Calculate previous month's boundary
# n1st_day_lst_year_str = exec_date_dt.replace(year = exec_date_dt.year - 1).strftime("%Y-%m-%d")
start_month = exec_date_dt.replace(year=exec_date_dt.year-1).strftime("%Y-%m")
end_month = exec_date_dt.strftime("%Y-%m")

# Initialize Spark
spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 16,
        'executor.cores': 5,
        'executor.memory': '20g',
    },
    script_name=f"agg_cell_stat_1y_{exec_date}",
    cluster='datanest'
)

INPUT_STAT_PATH = f"{BASE_PATH}/montly/seed_stat_1m"
OUTPUT_PATH = f"{BASE_PATH}/yearly/seed_stat_1y/"

# Load and filter data for exactly 1 previous month
df_seed_stat_1y_raw = (
    spark.read.parquet(INPUT_STAT_PATH)
    .where(f"month >= '{start_month}' AND month < '{end_month}'")
)

df_seed_stat_1y_agg_bse = (
    df_seed_stat_1y_raw
    .rollup("cell_id", "ref_province_name")
    .agg(
        # F.countDistinct("phone_number").alias("count_pn_cell_loc"),
        # F.countDistinct("ref_cell_id").alias("count_ref_cell_cell_loc"),
        F.sum("sum_logs_count").alias("sum_logs_count"),
        F.sum("ref_sum_logs_count").alias("ref_sum_logs_count"),
        F.countDistinct("month").alias("count_month")
    )
)

df_cell_prov_stat_agg = (
    df_seed_stat_1y_agg_bse
    .where("ref_province_name IS NOT NULL")
    # .withColumnRenamed("count_pn", "count_pn_cell_loc")
    # .withColumnRenamed("count_ref_cell", "count_ref_cell_cell_loc")
    .withColumnRenamed("sum_logs_count", "sum_logs_count_cell_prov")
    .withColumnRenamed("ref_sum_logs_count", "ref_sum_logs_count_cell_prov")
    # .withColumnRenamed("count_date", "count_date_cell_loc")
    .withColumnRenamed("count_month", "count_month_cell_prov")
)

df_cell_stat_agg = (
    df_seed_stat_1y_agg_bse
    .where("ref_province_name IS NULL")
    .drop("ref_province_name")
    # .withColumnRenamed("count_pn", "count_pn_cell")
    # .withColumnRenamed("count_ref_cell", "count_ref_cell_cell")
    .withColumnRenamed("sum_logs_count", "sum_logs_count_cell")
    .withColumnRenamed("ref_sum_logs_count", "ref_sum_logs_count_cell")
    # .withColumnRenamed("count_date", "count_date_cell")
    .withColumnRenamed("count_month", "count_month_cell")
    .drop("ref_province_name")
)

df_stat_1y_agg = (
    df_cell_prov_stat_agg
    .join(df_cell_stat_agg, "cell_id", "inner")
)

df_stat_1y_agg.write.mode("overwrite").format("parquet").save(OUTPUT_PATH)
# set_rep(OUTPUT_PATH, 1)