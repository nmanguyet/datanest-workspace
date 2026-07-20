import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import subprocess, sys
from functools import reduce
from pyspark.sql import DataFrame

exec_date = sys.argv[1]

spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 8,
        'executor.cores': 8,
        'executor.memory': '16g',
    },
    script_name="sample_phone_cell_data_{}".format(exec_date),
    cluster='datanest'
)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

INPUT_PATH = "hdfs://datanest-ha/feature/daily/cell/date={}".format(exec_date)
SAMPLE_PATH = "hdfs://cicdataha/label/client=mafc"
OUTPUT_PATH = "hdfs://datanest-ha/user/tridoan/location_v4/sample/daily/cell/date={}".format(exec_date)

df_sample = (
    spark.read.parquet(SAMPLE_PATH)
    .select("phone_number")
    .distinct()
)

df_cell = spark.read.parquet(INPUT_PATH)

df_cell_sample = (
    df_sample
    .join(df_cell, "phone_number", "inner")
)

print(f"input: {INPUT_PATH}")
print(f"output: {OUTPUT_PATH}")

df_cell_sample.write.mode("overwrite").format("parquet").save(OUTPUT_PATH)