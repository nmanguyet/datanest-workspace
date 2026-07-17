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

script_name = f"bsv3_balance_lxw_feature_{curdate_str}"
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name)

def extract_lxw_future_value(in_dir, out_dir, lxw):
    start_date = curdate - timedelta(days=lxw * 7 - 1)
    end_date = curdate
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    lxw_str = f"l{lxw}w"
    print("\n\n")
    print("===" + lxw_str + "===")

    print(f"in_dir: {in_dir}")
    print(f"date between {start_date_str} and {end_date_str}")
    df = spark.read.parquet(in_dir) \
        .where("date between '" + start_date_str + "' and '" + end_date_str + "'")

    lxw_df = df.groupBy("phone_number") \
        .agg(
            F.min('balance_pre_amt_min_l1w').alias('balance_pre_amt_min_' + lxw_str),
            F.avg('balance_pre_amt_avg_l1w').alias('balance_pre_amt_avg_' + lxw_str),
            F.max('balance_pre_amt_max_l1w').alias('balance_pre_amt_max_' + lxw_str),
            F.sum('balance_pre_zero_day_num_l1w').alias('balance_pre_zero_day_num_' + lxw_str),
            F.sum('balance_pre_leq5k_day_num_l1w').alias('balance_pre_leq5k_day_num_' + lxw_str),
            F.sum('balance_pre_leq10k_day_num_l1w').alias('balance_pre_leq10k_day_num_' + lxw_str),
            F.sum('balance_pre_leq20k_day_num_l1w').alias('balance_pre_leq20k_day_num_' + lxw_str),
            F.sum('balance_pre_leq5k_day_num_l1w').alias('balance_pre_leq5k_day_num_' + lxw_str),
            F.sum('balance_pre_leq50k_day_num_l1w').alias('balance_pre_leq50k_day_num_' + lxw_str),
            F.sum('balance_pre_leq100k_day_num_l1w').alias('balance_pre_leq100k_day_num_' + lxw_str),
            F.min('balance_pre_main_amt_min_l1w').alias('balance_pre_main_amt_min_' + lxw_str),
            F.avg('balance_pre_main_amt_avg_l1w').alias('balance_pre_main_amt_avg_' + lxw_str),
            F.max('balance_pre_main_amt_max_l1w').alias('balance_pre_main_amt_max_' + lxw_str),
            F.sum('balance_pre_main_zero_day_num_l1w').alias('balance_pre_main_zero_day_num_' + lxw_str),
            F.sum('balance_pre_main_leq5k_day_num_l1w').alias('balance_pre_main_leq5k_day_num_' + lxw_str),
            F.sum('balance_pre_main_leq10k_day_num_l1w').alias('balance_pre_main_leq10k_day_num_' + lxw_str),
            F.sum('balance_pre_main_leq20k_day_num_l1w').alias('balance_pre_main_leq20k_day_num_' + lxw_str),
            F.sum('balance_pre_main_leq5k_day_num_l1w').alias('balance_pre_main_leq5k_day_num_' + lxw_str),
            F.sum('balance_pre_main_leq50k_day_num_l1w').alias('balance_pre_main_leq50k_day_num_' + lxw_str),
            F.sum('balance_pre_main_leq100k_day_num_l1w').alias('balance_pre_main_leq100k_day_num_' + lxw_str),
            F.sum('balance_pre_main_leq200k_day_num_l1w').alias('balance_pre_main_leq200k_day_num_' + lxw_str),
            F.min('balance_post_amt_min_l1w').alias('balance_post_amt_min_' + lxw_str),
            F.avg('balance_post_amt_avg_l1w').alias('balance_post_amt_avg_' + lxw_str),
            F.max('balance_post_amt_max_l1w').alias('balance_post_amt_max_' + lxw_str),
            F.sum('balance_post_gt0_day_num_l1w').alias('balance_post_gt0_day_num_' + lxw_str),
            F.sum('balance_post_geq_5k_day_num_l1w').alias('balance_post_geq_5k_day_num_' + lxw_str),
            F.sum('balance_post_geq_10k_day_num_l1w').alias('balance_post_geq_10k_day_num_' + lxw_str),
            F.sum('balance_post_geq_20k_day_num_l1w').alias('balance_post_geq_20k_day_num_' + lxw_str),
            F.sum('balance_post_geq_50k_day_num_l1w').alias('balance_post_geq_50k_day_num_' + lxw_str),
            F.sum('balance_post_geq_100k_day_num_l1w').alias('balance_post_geq_100k_day_num_' + lxw_str),
            F.sum('balance_post_geq_200k_day_num_l1w').alias('balance_post_geq_200k_day_num_' + lxw_str),
            F.sum('balance_post_geq_500k_day_num_l1w').alias('balance_post_geq_500k_day_num_' + lxw_str),
            F.sum('balance_post_geq_1000k_day_num_l1w').alias('balance_post_geq_1000k_day_num_' + lxw_str),
            F.min('balance_post_main_amt_min_l1w').alias('balance_post_main_amt_min_' + lxw_str),
            F.avg('balance_post_main_amt_avg_l1w').alias('balance_post_main_amt_avg_' + lxw_str),
            F.max('balance_post_main_amt_max_l1w').alias('balance_post_main_amt_max_' + lxw_str),
            F.sum('balance_post_main_gt0_day_num_l1w').alias('balance_post_main_gt0_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_5k_day_num_l1w').alias('balance_post_main_geq_5k_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_10k_day_num_l1w').alias('balance_post_main_geq_10k_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_20k_day_num_l1w').alias('balance_post_main_geq_20k_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_50k_day_num_l1w').alias('balance_post_main_geq_50k_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_100k_day_num_l1w').alias('balance_post_main_geq_100k_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_200k_day_num_l1w').alias('balance_post_main_geq_200k_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_500k_day_num_l1w').alias('balance_post_main_geq_500k_day_num_' + lxw_str),
            F.sum('balance_post_main_geq_1000k_day_num_l1w').alias('balance_post_main_geq_1000k_day_num_' + lxw_str)
        )

    out_file = out_dir + "/" + lxw_str + "/" + f"date={curdate_str}"
    print(f"out_file: {out_file}")
    lxw_df.write.mode("overwrite").parquet(out_file)

in_dir = "/data/DS/project/bs_rnd/202107/whole_population/feature_value/balance/weekly_feature"
out_dir = "/data/DS/project/bs_rnd/202107/whole_population/feature_value/balance/"

lxw_list = [2, 4, 12]
for lxw in lxw_list:
    extract_lxw_future_value(in_dir, out_dir, lxw)

from etl.common.data_lineage import data_lineage
dl = data_lineage.DataLineage()
dl.log_io(input_paths=[in_dir], output_paths=[out_dir], script=__file__)
