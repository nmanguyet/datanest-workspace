import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from etl.common.init_spark3 import setup, small_config
from etl.common.utils import check_multi_date_count
from etl.common.utils import get_script_name

if len(sys.argv) != 2:
    print('need processing date string: python {} YYYY-mm-dd'.format(sys.argv[0]), file=sys.stderr)
    exit(1)
curdate_str = sys.argv[1]
curdate = datetime.strptime(curdate_str, "%Y-%m-%d")
weekday = curdate.weekday()
this_sunday = curdate - timedelta(weekday - 6)

# if not check_multi_date_count('vasc', this_sunday, 60, DAILY_COUNT_THRESHOLD=1):
#      exit(1)

spark = setup(job_cfg=small_config(), script_name=get_script_name(sys.argv[0]))

in_dir = "/data/vnpt_v2/vasc"
out_dir = "/data/processed/credit_score_v1.2/features/vasc/agg/60days"
lad_dir = "/data/processed/credit_score_v1.2/features/sub/last_activated_date/weekly"

# jupyter +3
def compute(agg_date, in_dir, out_dir, lad_dir, backward_days=60):
    start_date = (agg_date - timedelta(days=backward_days - 1))
    out_file = os.path.join(out_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    lad_file = os.path.join(lad_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    print(start_date, agg_date, out_file, lad_file)
    lad_df = spark.read.parquet(lad_file)
    df = spark.read.format('delta').load(in_dir).dropDuplicates() \
        .drop("date") \
        .withColumn("date", F.to_date("transaction_time")) \
        .drop("transaction_time") \
        .filter((F.col("date") > start_date) & (F.col("date") <= agg_date))

    from etl.common.utils import check_data_date
    check_data_date(spark, df, int((agg_date - start_date).days),
                    path=in_dir, start_date=start_date + timedelta(days=1), end_date=agg_date)

    df = df.join(lad_df, "phone_number") \
        .filter("date >= last_activated_date")
    df.cache()
    df.count()

    ungtien_df = df.filter(F.col("service_name") != "2FRIEND") \
        .groupBy("phone_number", "date") \
        .agg(
            F.sum("amount").alias("amount"),
            F.count("amount").alias("count")
        ).groupBy("phone_number") \
        .agg(
            F.count("count").alias("vasc_num_days_uses_UNGTIEN"),
            F.sum("count").alias("vasc_num_times_uses_UNGTIEN"),
            F.sum("amount").alias("vasc_amount_expence_UNGTIEN")
        )

    friend_df = df.filter(F.col("service_name") == "2FRIEND") \
        .groupBy("phone_number", "date") \
        .agg(
            F.sum("amount").alias("amount"),
            F.count("amount").alias("count")
        ).groupBy("phone_number") \
        .agg(
            F.count("count").alias("vasc_num_days_uses_2FRIEND"),
            F.sum("count").alias("vasc_num_times_uses_2FRIEND"),
            F.sum("amount").alias("vasc_amount_expence_2FRIEND")
        )

    df = ungtien_df.join(friend_df, "phone_number", "outer").fillna(0)
    #    df.show(100, False)
    df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save(out_file)

compute(this_sunday, in_dir, out_dir, lad_dir)
