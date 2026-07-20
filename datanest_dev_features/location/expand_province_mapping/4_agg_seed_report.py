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
    script_name=f"agg_seed_stat_report_1y_{exec_date}",
    cluster='datanest'
)

INPUT_STAT_PATH = f"{BASE_PATH}/yearly/seed_stat_1y"
INPUT_CELL_PN_PATH = f"{BASE_PATH}/yearly/seed_unique_cell_pn"
INPUT_CELL_REF_CELL_PATH = f"{BASE_PATH}/yearly/seed_unique_cell_ref_cell"
INPUT_CELL_COUNT_DATE_PATH = f"{BASE_PATH}/yearly/seed_cell_count_date/"

OUTPUT_PATH = f"{BASE_PATH}/yearly/seed_stat_report_1y/"

# Agg all
df_stat_1y_agg = (
    spark.read.parquet(INPUT_STAT_PATH)
)

df_count_pn_1y_agg = (
    spark.read.parquet(INPUT_CELL_PN_PATH)
)

df_count_ref_cell_1y_agg = (
    spark.read.parquet(INPUT_CELL_REF_CELL_PATH)
)

df_count_date = (
    spark.read.parquet(INPUT_CELL_COUNT_DATE_PATH)
)

df_1y_agg = (
    df_stat_1y_agg
    .join(df_count_pn_1y_agg, ["cell_id", "ref_province_name"], "inner")
    .join(df_count_ref_cell_1y_agg, ["cell_id", "ref_province_name"], "inner")
    .join(df_count_date, ["cell_id", "ref_province_name"], "inner")
)

df_1y_agg.write.mode("overwrite").format("parquet").save(OUTPUT_PATH)