import os
import sys

import pyspark.sql.functions as F
import pyspark.sql.types as T

from etl.common import init_spark3
from etl.viettel.common.configs import read_config_service
from etl.common.utils import check_daily_count
from datetime import datetime

from etl.viettel.common.utils import check_enough_data
from etl.common.utils import log_io_file_path


def extract_data_daily_feature(in_dir, out_dir):
    # read raw data
    # df = spark.read.parquet(in_dir).dropDuplicates()
    df = spark.read.format('delta').load(in_dir).where('date="{}"'.format(current_date_str))
    check_enough_data(df, 1)

    # Add two dimensions
    df = df.withColumn("hour", F.hour("start_time")) \
        .withColumn("time_range",
                    F.expr("""
                        case
                            when hour >= 6 and hour < 18 then 'D'
                            else 'N'
                        end
                    """))

    # Compute features
    daily_fts = df.groupBy("phone_number").agg(
        F.sum("charge_amount").alias("data_amt_charge_total"),
        F.sum("up_data").alias("data_upload_size"),
        F.sum("down_data").alias("data_download_size"),
        F.avg("balance_remain").alias("data_blanace_remain_avg"),
        F.countDistinct("hour").alias("data_hour_distinct_num"),
        F.count("*").alias("data_txn_num"),
        F.max("start_time").alias("data_time_end"),
        F.min("start_time").alias("data_time_start"),

        F.sum(F.when(F.col("time_range") == 'D', F.col("charge_amount"))).alias("data_amt_charge_daytime_total"),
        F.sum(F.when(F.col("time_range") == 'D', F.col("up_data"))).alias("data_upload_size_daytime_total"),
        F.sum(F.when(F.col("time_range") == 'D', F.col("down_data"))).alias("data_download_size_daytime_total"),
        F.count(F.when(F.col("time_range") == 'D', 1)).alias("data_txn_daytime_num")
    )

    # Write to file
    daily_fts.coalesce(coalesce_value).write.mode("overwrite").parquet(out_dir)


current_date_str = sys.argv[1]
config = read_config_service.get_compute_features_config()['g22_data_charge']
job_cfg = config['daily']['job_config']

if not check_daily_count('g22', datetime.strptime(current_date_str, '%Y-%m-%d'), DAILY_COUNT_THRESHOLD=0.3):
    exit(1)

in_dir = config['path']['select_data_path']
coalesce_value = config["coalesce_value"]
# Initiate environment
script_name_str = "daily_data_charge_daily_feature_" + current_date_str
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name_str)
print("Starting script: ", script_name_str)

# Folders
out_dir = config["path"]["daily"]
out_dir = os.path.join(out_dir, "date={}".format(current_date_str))

extract_data_daily_feature(in_dir, out_dir)
spark.stop()
print("Finished task")

try:
    log_io_file_path(input_paths=[in_dir],
                     output_paths=[config["path"]["daily"]],
                     )
except Exception as e:
    print(f"Waring: unable to log data lineage! Err: {e}")
