import os
import sys
from datetime import datetime, timedelta

from etl.common import init_spark3
import pyspark.sql.functions as F

curdate_str = sys.argv[1]
curdate = datetime.strptime(curdate_str, "%Y-%m-%d")

if curdate.weekday() != 6:
    print("Not Sunday, no need to run")
    exit(0)

job_cfg = {
    "executor.instances": 4,
    "executor.cores": 10,
    "executor.memory": "20g"
}

script_name = f"bsv3_balance_weekly_feature_{curdate_str}"
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name)

def extract_weekly_future_value(in_dir1, in_dir2, out_dir):
    start_date = curdate - timedelta(days=6)
    end_date = curdate
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    print(f"balance in: {in_dir2}")
    print(f"date between {start_date_str} - {end_date_str}")

    raw_df = spark.read.format("delta").load(in_dir2) \
        .where("date between '" + start_date_str + "' and '" + end_date_str + "'")

    sub_file = os.path.join(in_dir1, f"date={curdate_str}")
    print(f"sub in: {sub_file}")
    sub_df = spark.read.parquet(sub_file)
    sub_df = sub_df.selectExpr("phone_number", "billing_type")

    weekly_df = raw_df.join(sub_df, on='phone_number')

    # Prepay subscriber for all accounts
    weekly_df1 = weekly_df.where("billing_type == 'PRE'") \
        .groupBy("phone_number", "date") \
        .agg(F.sum("balance").alias("balance")) \
        .groupBy("phone_number") \
        .agg(
            F.min("balance").alias("balance_pre_amt_min_l1w"),
            F.avg("balance").alias("balance_pre_amt_avg_l1w"),
            F.max("balance").alias("balance_pre_amt_max_l1w"),
            F.stddev("balance").alias("balance_pre_amt_std_l1w"),
            F.countDistinct(F.when(F.col("balance") == 0, F.col("date"))).alias("balance_pre_zero_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 5000, F.col("date"))).alias("balance_pre_leq5k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 10000, F.col("date"))).alias("balance_pre_leq10k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 20000, F.col("date"))).alias("balance_pre_leq20k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 50000, F.col("date"))).alias("balance_pre_leq50k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 100000, F.col("date"))).alias("balance_pre_leq100k_day_num_l1w")
        )

    weekly_df2 = weekly_df.where("billing_type == 'PRE' and account_code = 0") \
        .groupBy("phone_number") \
        .agg(
            F.min("balance").alias("balance_pre_main_amt_min_l1w"),
            F.avg("balance").alias("balance_pre_main_amt_avg_l1w"),
            F.max("balance").alias("balance_pre_main_amt_max_l1w"),
            F.stddev("balance").alias("balance_pre_main_amt_std_l1w"),
            F.countDistinct(F.when(F.col("balance") == 0, F.col("date"))).alias("balance_pre_main_zero_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 5000, F.col("date"))).alias("balance_pre_main_leq5k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 10000, F.col("date"))).alias("balance_pre_main_leq10k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 20000, F.col("date"))).alias("balance_pre_main_leq20k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 50000, F.col("date"))).alias("balance_pre_main_leq50k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 100000, F.col("date"))).alias("balance_pre_main_leq100k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") <= 200000, F.col("date"))).alias("balance_pre_main_leq200k_day_num_l1w")
        )

    # Postpay subscriber for all accounts
    weekly_df3 = weekly_df.where("billing_type == 'POST'") \
        .groupBy("phone_number", "date") \
        .agg(F.sum("balance").alias("balance")) \
        .groupBy("phone_number") \
        .agg(
            F.min("balance").alias("balance_post_amt_min_l1w"),
            F.avg("balance").alias("balance_post_amt_avg_l1w"),
            F.max("balance").alias("balance_post_amt_max_l1w"),
            F.stddev("balance").alias("balance_post_amt_std_l1w"),
            F.countDistinct(F.when(F.col("balance") >= 0, F.col("date"))).alias("balance_post_gt0_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -5000, F.col("date"))).alias("balance_post_geq_5k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -10000, F.col("date"))).alias("balance_post_geq_10k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -20000, F.col("date"))).alias("balance_post_geq_20k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -50000, F.col("date"))).alias("balance_post_geq_50k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -100000, F.col("date"))).alias("balance_post_geq_100k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -200000, F.col("date"))).alias("balance_post_geq_200k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -500000, F.col("date"))).alias("balance_post_geq_500k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -1000000, F.col("date"))).alias("balance_post_geq_1000k_day_num_l1w")
        )

    weekly_df4 = weekly_df.where("billing_type == 'POST' and account_code = 0") \
        .groupBy("phone_number") \
        .agg(
            F.min("balance").alias("balance_post_main_amt_min_l1w"),
            F.avg("balance").alias("balance_post_main_amt_avg_l1w"),
            F.max("balance").alias("balance_post_main_amt_max_l1w"),
            F.stddev("balance").alias("balance_post_main_amt_std_l1w"),
            F.countDistinct(F.when(F.col("balance") >= 0, F.col("date"))).alias("balance_post_main_gt0_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -5000, F.col("date"))).alias("balance_post_main_geq_5k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -10000, F.col("date"))).alias("balance_post_main_geq_10k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -20000, F.col("date"))).alias("balance_post_main_geq_20k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -50000, F.col("date"))).alias("balance_post_main_geq_50k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -100000, F.col("date"))).alias("balance_post_main_geq_100k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -200000, F.col("date"))).alias("balance_post_main_geq_200k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -500000, F.col("date"))).alias("balance_post_main_geq_500k_day_num_l1w"),
            F.countDistinct(F.when(F.col("balance") >= -1000000, F.col("date"))).alias("balance_post_main_geq_1000k_day_num_l1w")
        )

    weekly_df = weekly_df1.join(weekly_df2, on=['phone_number'], how='outer')
    weekly_df = weekly_df.join(weekly_df3, on=['phone_number'], how='outer')
    weekly_df = weekly_df.join(weekly_df4, on=['phone_number'], how='outer')

    # Write output
    out_file = os.path.join(out_dir, f"date={curdate_str}")
    weekly_df.write.mode("overwrite").parquet(out_file)

in_dir1 = "/data/DS/project/bs_rnd/202107/feature_value/sub/weekly_feature"
in_dir2 = "/data/vnpt_v2/balance"
out_dir = "/data/DS/project/bs_rnd/202107/whole_population/feature_value/balance/weekly_feature"

extract_weekly_future_value(in_dir1, in_dir2, out_dir)

from etl.common.data_lineage import data_lineage
dl = data_lineage.DataLineage()
dl.log_io(input_paths=[in_dir1, in_dir2], output_paths=[out_dir], script=__file__)
