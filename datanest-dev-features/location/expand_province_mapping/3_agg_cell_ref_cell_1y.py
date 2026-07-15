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
    script_name=f"agg_ref_cell_1y_{exec_date}",
    cluster='datanest'
)

INPUT_CELL_REF_CELL_PATH = f"{BASE_PATH}/montly/seed_unique_cell_ref_cell"
OUTPUT_PATH = f"{BASE_PATH}/yearly/seed_unique_cell_ref_cell/"

df_seed_cell_ref_cell_1y_raw = (
    spark.read.parquet(INPUT_CELL_REF_CELL_PATH)
    .where(f"month >= '{start_month}' AND month < '{end_month}'")
)

# Agg count ref_cell distinct
df_seed_cell_ref_cell_1y_base = (
    df_seed_cell_ref_cell_1y_raw
    .rollup("cell_id", "ref_province_name")
    .agg(
        F.countDistinct("ref_cell_id").alias("count_ref_cell")
    )
)

df_cell_prov_count_ref_cell_agg = (
    df_seed_cell_ref_cell_1y_base
    .where("ref_province_name IS NOT NULL")
    .withColumnRenamed("count_ref_cell", "count_ref_cell_cell_prov")
)

df_cell_count_ref_cell_agg = (
    df_seed_cell_ref_cell_1y_base
    .where("ref_province_name IS NULL")
    .withColumnRenamed("count_ref_cell", "count_ref_cell_cell")
    .drop("ref_province_name")
)

df_count_ref_cell_1y_agg = (
    df_cell_prov_count_ref_cell_agg
    .join(df_cell_count_ref_cell_agg, "cell_id", "inner")
)

df_count_ref_cell_1y_agg.write.mode("overwrite").format("parquet").save(OUTPUT_PATH)
# set_rep(OUTPUT_PATH, 1)