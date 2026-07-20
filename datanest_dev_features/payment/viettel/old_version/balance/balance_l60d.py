import os
import sys
from datetime import datetime, timedelta
from datetime import datetime as dt

from etl.common import init_spark3
from etl.common import utils
from etl.viettel.common.configs import read_config_service
from etl.common.utils import check_weekly_count
from etl.viettel.common.utils import check_enough_data, check_enough_data_minumum
from etl.common.utils import log_io_file_path

curdate_str = sys.argv[1]
curdate = datetime.strptime(curdate_str, "%Y-%m-%d")
data_source = 'report_in'

if not check_weekly_count(data_source='report_in', curdate=curdate, DAILY_COUNT_THRESHOLD=0.2, MAX_DAY_MISSING=1):
    exit(1)

if curdate.weekday() != 6:
    print("It's not Sunday boy, try different date =))")
    sys.exit(0)

config = read_config_service.get_compute_features_config()['report_in']
weekly_config = config['weekly']
job_cfg = weekly_config['agg']['job_config']

script_name = utils.get_spark_script_name()
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name)

print("http://10.58.244.150:8088/proxy/{}".format(spark.sparkContext.applicationId))

from pyspark.sql.functions import *
from pyspark.sql.types import *

backward_days = 60
first_date = (curdate - timedelta(days=backward_days - 1))
first_date_str = dt.strftime(first_date, "%Y-%m-%d")

in_dir = config['path']['select_data_path']
out_dir = os.path.join("hdfs://datanest-ha/feature/lxw/balance/60days", "date={}".format(curdate_str))
print (first_date, curdate, in_dir, out_dir)

df = spark.read.format('delta').load(in_dir) \
    .withColumn(
    "BAL_other",
    col("BAL_10") / 10000.0 + col("BAL_11") / 10000.0 + \
    col("BAL_12") / 10000.0 + col("BAL_13") / 10000.0 + \
    col("BAL_15") / 10000.0 + col("BAL_14") / 10000.0 + \
    col("BAL_16") / 10000.0 + col("BAL_17") / 10000.0 + \
    col("BAL_18") / 10000.0 + col("BAL_21") / 10000.0 + \
    col("BAL_22") / 10000.0 + col("BAL_24") / 10000.0 + \
    col("BAL_25") / 10000.0 + col("BAL_38") / 10000.0 + \
    col("BAL_34") / 10000.0 + col("BAL_45") / 10000.0 + \
    col("BAL_46") / 10000.0
)

df = df.selectExpr("phone_number", "BAL_1/10000.0 balance", "BAL_other balance_promo", "date") \
    .where("date between '{}' and '{}'".format(first_date_str, curdate_str))
# check_enough_data(df, backward_days)
check_enough_data_minumum(df, backward_days-1)

df = df \
    .filter("balance >= 0") \
    .groupBy("phone_number", "date").agg(
    sum("balance").alias("balance"),
    sum("balance_promo").alias("balance_promo"),
) \
    .select("phone_number", "balance", "balance_promo") \
    .dropDuplicates()

ft_stat = df.groupBy("phone_number") \
    .agg(
    avg("balance").alias("balance_avg_per_day"),
    max("balance").alias("balance_max_per_day"),
    stddev("balance").alias("balance_sd_per_day"),
    avg("balance_promo").alias("balance_promo_avg_per_day"),
    max("balance_promo").alias("balance_promo_max_per_day"),
    stddev("balance_promo").alias("balance_promo_sd_per_day")
)

ft_balance = df \
    .withColumn("lt_1k", when((col("balance") > 0) & (col("balance") < 1000), 1).otherwise(0)) \
    .withColumn("lt_5k", when((col("balance") > 0) & (col("balance") < 5000), 1).otherwise(0)) \
    .withColumn("lt_10k", when((col("balance") > 0) & (col("balance") < 10000), 1).otherwise(0)) \
    .withColumn("is_zero", when(col("balance") == 0, 1).otherwise(0)) \
    .withColumn("ge_1k_lt_5k", when((col("balance") >= 1000) & (col("balance") < 5000), 1).otherwise(0)) \
    .withColumn("ge_5k_lt_10k", when((col("balance") >= 5000) & (col("balance") < 10000), 1).otherwise(0)) \
    .withColumn("ge_10k_lt_20k", when((col("balance") >= 10000) & (col("balance") < 20000), 1).otherwise(0)) \
    .withColumn("ge_20k_lt_5k", when((col("balance") >= 20000) & (col("balance") < 50000), 1).otherwise(0)) \
    .withColumn("ge_50k_lt_100k", when((col("balance") >= 50000) & (col("balance") < 100000), 1).otherwise(0)) \
    .withColumn("ge_100k", when(col("balance") >= 100000, 1).otherwise(0)) \
    .withColumn("lt_1k_promo", when((col("balance_promo") > 0) & (col("balance_promo") < 1000), 1).otherwise(0)) \
    .withColumn("lt_5k_promo", when((col("balance_promo") > 0) & (col("balance_promo") < 5000), 1).otherwise(0)) \
    .withColumn("lt_10k_promo", when((col("balance_promo") > 0) & (col("balance_promo") < 10000), 1).otherwise(0)) \
    .withColumn("is_zero_promo", when(col("balance_promo") == 0, 1).otherwise(0)) \
    .withColumn("ge_1k_lt_5k_promo", 
                when((col("balance_promo") >= 1000) & (col("balance_promo") < 5000), 1).otherwise(0)) \
    .withColumn("ge_5k_lt_10k_promo", 
                when((col("balance_promo") >= 5000) & (col("balance_promo") < 10000), 1).otherwise(0)) \
    .withColumn("ge_10k_lt_20k_promo", 
                when((col("balance_promo") >= 10000) & (col("balance_promo") < 20000), 1).otherwise(0)) \
    .withColumn("ge_20k_lt_50k_promo", 
                when((col("balance_promo") >= 20000) & (col("balance_promo") < 50000), 1).otherwise(0)) \
    .withColumn("ge_50k_lt_100k_promo", 
                when((col("balance_promo") >= 50000) & (col("balance_promo") < 100000), 1).otherwise(0)) \
    .withColumn("ge_100k_promo", when(col("balance_promo") >= 100000, 1).otherwise(0)) \
    .groupBy("phone_number") \
    .agg(
    mean("lt_1k").alias("balance_pct_day_lt_1k"),
    mean("lt_5k").alias("balance_pct_day_lt_5k"),
    mean("lt_10k").alias("balance_pct_day_lt_10k"),
    mean("is_zero").alias("balance_pct_day_zero"),
    mean("ge_1k_lt_5k").alias("balance_pct_day_1k_to_5k"),
    mean("ge_5k_lt_10k").alias("balance_pct_day_5k_to_10k"),
    mean("ge_10k_lt_20k").alias("balance_pct_day_10k_to_20k"),
    mean("ge_20k_lt_5k").alias("balance_pct_day_20k_to_5k"),
    mean("ge_50k_lt_100k").alias("balance_pct_day_50k_to_100k"),
    mean("ge_100k").alias("balance_pct_day_ge_100k"),
    mean("lt_1k_promo").alias("balance_promo_pct_day_lt_1k"),
    mean("lt_5k_promo").alias("balance_promo_pct_day_lt_5k"),
    mean("lt_10k_promo").alias("balance_promo_pct_day_lt_10k"),
    mean("is_zero_promo").alias("balance_promo_pct_day_zero"),
    mean("ge_1k_lt_5k_promo").alias("balance_promo_pct_day_1k_to_5k"),
    mean("ge_5k_lt_10k_promo").alias("balance_promo_pct_day_5k_to_10k"),
    mean("ge_10k_lt_20k_promo").alias("balance_promo_pct_day_10k_to_20k"),
    mean("ge_20k_lt_50k_promo").alias("balance_promo_pct_day_20k_to_50k"),
    mean("ge_50k_lt_100k_promo").alias("balance_promo_pct_day_50k_to_100k"),
    mean("ge_100k_promo").alias("balance_promo_pct_day_ge_100k")
)

ft = ft_stat.join(ft_balance, "phone_number", "outer").fillna(0)
ft.write.format("parquet") \
    .mode("overwrite") \
    .save(out_dir)

try:
    log_io_file_path(input_paths=[in_dir],
                     output_paths=['hdfs://datanest-ha/feature/lxw/balance/60days'],
                     )
except Exception as e:
    print(f'Waring: unable to log data lineage! Err: {e}')
