import os
import sys
from datetime import timedelta, datetime
from datetime import datetime as dt
from pyspark.sql import functions as F

if len(sys.argv) != 2:
    print('need processing date string: python {} YYYY-mm-dd'.format(sys.argv[0]))
    exit(1)
curdate_str = sys.argv[1]
curdate = datetime.strptime(curdate_str, "%Y-%m-%d")
weekday = curdate.weekday()
this_sunday = curdate - timedelta(weekday - 6)

# if not check_multi_date_count('air', this_sunday, 60, DAILY_COUNT_THRESHOLD=0.55):
#     exit(1)

spark = setup(job_cfg=small_config(), script_name="airflow_agg_air60d")

in_dir = "/data/vnpt_v2/air"
promo_file = "/data/DS/trung/processed/air/promo_dates"
out_dir = "/data/processed/credit_score_v1.2/features/air/agg/60days"
lad_dir = "/data/processed/credit_score_v1.2/features/sub/last_activated_date/weekly"


def find_promotion_dates(in_dir, out_file):
    df = spark.read.format('delta').load(in_dir)
    df = df.select("phone_number", "recharge_time") \
        .withColumn("date", F.to_date("recharge_time")) \
        .groupBy("date").agg(F.count("phone_number").alias("count"))

    df.withColumn("promo_date", (F.col("count") > 1000000).cast("integer")) \
        .orderBy("date").coalesce(1) \
        .write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save(out_file)


def compute(agg_date, in_dir, out_dir, lad_dir, promo_date, backward_days=60):
    start_date = (agg_date - timedelta(days=backward_days - 1))
    out_file = os.path.join(out_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    lad_file = os.path.join(lad_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    print(start_date, agg_date, lad_file)

    lad_df = spark.read.parquet(lad_file)
    df = spark.read.format('delta').load(in_dir)
    df = df.filter("recharge_amount > 0") \
        .filter((F.col("date") > start_date) & (F.col("date") <= agg_date)) \

    from etl.common.utils import check_data_date
    check_data_date(spark, df, int((agg_date - start_date).days),
                    path=in_dir, start_date=start_date + timedelta(days=1), end_date=agg_date)

    df = df.dropDuplicates() \
        .join(lad_df, "phone_number") \
        .filter("recharge_time >= last_activated_date")
    df.cache()
    df.count()

    stats_df = df.select("phone_number", "recharge_amount") \
        .withColumnRenamed("recharge_amount", "amount") \
        .withColumn("le_10k", F.when(F.col("amount") < 10000, 1).otherwise(0)) \
        .withColumn("gt_10k_le_20k", F.when((F.col("amount") > 10000) & (F.col("amount") <= 20000), 1).otherwise(0)) \
        .withColumn("gt_20k_le_50k", F.when((F.col("amount") > 20000) & (F.col("amount") <= 50000), 1).otherwise(0)) \
        .withColumn("gt_50k_le_100k", F.when((F.col("amount") > 50000) & (F.col("amount") <= 100000), 1).otherwise(0)) \
        .withColumn("gt_100k", F.when(F.col("amount") > 100000, 1).otherwise(0)) \
        .groupBy("phone_number") \
        .agg(F.mean("le_10k").alias("air_pct_le_10k"),
             F.mean("gt_10k_le_20k").alias("air_pct_gt_10k_le_20k"),
             F.mean("gt_20k_le_50k").alias("air_pct_gt_20k_le_50k"),
             F.mean("gt_50k_le_100k").alias("air_pct_gt_50k_le_100k"),
             F.mean("gt_100k").alias("air_pct_gt_100k")
             )
    stats_df.cache()
    stats_df.count()

    promo_df = df.join(promo_date, "date") \
        .groupBy("phone_number") \
        .agg(
        F.sum("recharge_amount").alias("air_promo_recharge_amount"),
        F.count("recharge_amount").alias("air_promo_recharge_times")
    )

    total_df = df.groupBy("phone_number") \
        .agg(
        F.sum("recharge_amount").alias("air_total_recharge_amount"),
        F.count("recharge_amount").alias("air_total_recharge_times")
    )

    promo_df = total_df.join(promo_df, "phone_number", "outer").fillna(0) \
        .withColumn('air_pct_amount_recharge_promo',
                    F.col("air_promo_recharge_amount") / (F.col("air_total_recharge_amount") + 0.01)) \
        .withColumn('air_pct_times_recharge_promo',
                    F.col("air_promo_recharge_times") / (F.col("air_total_recharge_times") + 0.01))
    promo_df.cache()
    promo_df.count()
    ft = stats_df.join(promo_df, "phone_number", "outer").fillna(0)

    ft.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save(out_file)


find_promotion_dates(in_dir, promo_file)
promo_date = spark.read.parquet(promo_file) \
    .filter("promo_date == 1").select("date")

compute(this_sunday, in_dir, out_dir, lad_dir, promo_date)
