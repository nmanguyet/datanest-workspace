from etl.common import init_spark3
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import subprocess, sys
from functools import reduce
from pyspark.sql import DataFrame
from .config import BASE_PATH, TRAIN_CELL_MAP_PATH
from .train_test_split import cell_train_test_split
from .hdfs_helper import set_rep

exec_date = sys.argv[1]
exec_date_dt = datetime.strptime(exec_date, "%Y-%m-%d")
lst_2date = (exec_date_dt - timedelta(2)).strftime("%Y-%m-%d")

spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 8,
        'executor.cores': 5,
        'executor.memory': '20g',
    },
    script_name="1_daily_select_top1_2_rank_logs_3d_{}".format(exec_date),
    cluster='datanest'
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

INPUT_PATH = "hdfs://datanest-ha/feature/daily/cell/"
# INPUT_PATH = "hdfs://datanest-ha/user/tridoan/location_v4/sample/daily/cell/"
OUTPUT_PATH = f"{BASE_PATH}/daily/seed_prov_by_rank_top1_2/date={exec_date}"
# Load train cell map
CELL_MAP_PATH = TRAIN_CELL_MAP_PATH
# CELL_MAP_PATH = "hdfs://cicdataha/data/processed/mapping/full_cell_id_mapping_province_district_20250630"

cell_train_test_split(spark)

# Load and process the cell map table
df_cell_map_raw = spark.read.format("parquet").load(CELL_MAP_PATH)
window_spec = Window.partitionBy("cell_id").orderBy(F.col("date").desc())

# Dedup base on date (get base on Latest date)
df_cell_map = (
    df_cell_map_raw
    .select("cell_id", "district_code", "district_name", "province_code", "province_name", "date")
    .withColumn("rn", F.row_number().over(window_spec))
    .where("rn = 1")
    .drop("rn")
    .selectExpr("cell_id", "district_code", "district_name", "province_code", "province_name", "date AS cell_upd_dt")
)

# Map and process the phone_cell with cell_map table
df_pn_cell_daily = spark.read.parquet(INPUT_PATH).where("phone_number IS NOT NULL")

df_3d = (
    df_pn_cell_daily
    .where(f"'{lst_2date}' <= date AND date <= '{exec_date}'")
    .join(F.broadcast(df_cell_map), "cell_id", "left")
    .cache()
)

if df_3d.select("date").distinct().count() < 3:
    print("Not enough 3 days, process next day")
    sys.exit(99)

df_pn_cell_curdate_agg = (
    df_3d
    .where(f"date = '{exec_date}'")
    .groupBy("cell_id", "phone_number", "province_name")
    .agg(
        F.sum("count").alias("sum_logs_count")
    )
)

df_pn_prov_logs_3d_agg = (
    df_3d
    .where("province_name IS NOT NULL")
    .groupBy("phone_number", "province_name")
    .agg(
        F.sum("count").alias("sum_logs_count_3d_prov")
    )
)

w_phone = Window.partitionBy("phone_number")

df_top1_2_logs_count_by_prov = (
    df_pn_prov_logs_3d_agg
    .withColumn("drank_logs_count", F.dense_rank().over(w_phone.orderBy(F.col("sum_logs_count_3d_prov").desc())))
    .where("drank_logs_count = 1 OR drank_logs_count = 2")
)

df_top1_top2_filter_91 = (
    df_top1_2_logs_count_by_prov
    .select("phone_number", "province_name", "drank_logs_count", "sum_logs_count_3d_prov")
    .withColumn("top1_val", F.max(F.expr("IF(drank_logs_count = 1, sum_logs_count_3d_prov, NULL)")).over(w_phone))
    .withColumn("top2_val", F.max(F.expr("IF(drank_logs_count = 2, sum_logs_count_3d_prov, NULL)")).over(w_phone))
    .withColumn("top1_count", F.sum(F.expr("IF(drank_logs_count = 1, 1, 0)")).over(w_phone))
    .where("(drank_logs_count = 1 AND top1_count = 1) AND (top2_val IS NULL OR top1_val >= top2_val*9)")
    .select("phone_number", "province_name", "sum_logs_count_3d_prov")
)

df_province_seed_stable = (
    df_top1_top2_filter_91
    .join(df_pn_cell_curdate_agg, ["phone_number", "province_name"], "inner")
    .selectExpr("phone_number", "province_name AS ref_province_name", "cell_id AS ref_cell_id", "sum_logs_count AS ref_sum_logs_count", "sum_logs_count_3d_prov as ref_sum_logs_count_3d_prov")
)

df_seed_ref_province_3d = (
    df_3d
    .select("phone_number", "cell_id")
    .where("province_name IS NULL")
    .dropDuplicates()
    .join(df_province_seed_stable, "phone_number")
    .join(df_pn_cell_curdate_agg, ["phone_number", "cell_id"], "inner")
    .select("phone_number", "cell_id", "sum_logs_count", "ref_cell_id", "ref_sum_logs_count", "ref_sum_logs_count_3d_prov", "ref_province_name")
    .cache()
)

df_3d.unpersist()

print(f"input: {INPUT_PATH}")
print(f"output: {OUTPUT_PATH}")

count_check = df_seed_ref_province_3d.count()
if count_check == 0:
    print("Empty output")
    df_seed_ref_province_3d.unpersist()
    sys.exit(1)
else:
    print(f"Number of records after processed: {count_check}")

df_seed_ref_province_3d.write.mode("overwrite").format("parquet").save(OUTPUT_PATH)
df_seed_ref_province_3d.unpersist()
set_rep(OUTPUT_PATH, 2)