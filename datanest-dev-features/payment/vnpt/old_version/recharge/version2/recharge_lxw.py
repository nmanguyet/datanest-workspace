import os
import sys
from datetime import timedelta, datetime
from datetime import datetime as dt
from pyspark.sql import functions as F

from etl.common.init_spark3 import setup, small_config
from etl.vnpt.spark_config import medium_config
from etl.vnpt.hdfs import check_dir_exist
from dateutil.relativedelta import relative_delta

PY_NORM_FDATE = "%Y-%m-%d"
SPARK_NORM_FDATE = "yyyy-MM-dd"
from pyspark.sql import functions as F

if len(sys.argv) != 2:
    print('need processing date string: python {} YYYY-mm-dd'.format(sys.argv[0]), file=sys.stderr)
    exit(1)
curdate_str = sys.argv[1]
curdate = dt.strptime(curdate_str, "%Y-%m-%d")
(year, week, dow) = curdate.isocalendar()

spark = setup(job_cfg=medium_config(), script_name="airflow_recharge_agg_" + curdate_str)

#in_dir = "/data/DS/namdoan/vnpt_new/05_AIR_with_bal"
in_dir = "/feature/daily/recharge/05_AIR_with_bal"

#out_dir = "/data/processed/inventory/features/recharge/agg/3m"
out_dir = "/feature/inventory/recharge"

in_file = "{}/date={}".format(in_dir, curdate_str)
out_file = "{}/date={}".format(out_dir, curdate_str)
print(in_file, out_file)

l1m_str = dt.strftime(curdate - relativedelta(months=1), PY_NORM_FDATE)
l2m_str = dt.strftime(curdate - relativedelta(months=2), PY_NORM_FDATE)
l3m_str = dt.strftime(curdate - relativedelta(months=3), PY_NORM_FDATE)
l1m_suffix = "_l1m"
l2m_suffix = "_l2m"
l3m_suffix = "_l3m"

recharge = spark.read.parquet(in_dir) \
    .where("recharge_amount > 0")
# todo: update promote date
promote_date_df = spark.read.parquet("/data/DS/trung/processed/air/promo_dates") \
    .withColumn("is_promote", F.lit(1))

# jupyter +3
def cal_recharge_ft(recharge, promote_date_df, start_date_str, end_date_str, suffix):
    recharge = recharge \
        .where("date > '{}' and date <= '{}'".format(start_date_str, end_date_str))
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    num_days = (end_date - start_date).days

    if recharge.select("date").distinct().count() != num_days:
        print("not enough data")
        print(recharge.select("date").distinct().orderBy("date").show(100))
        exit(1)

    ft_agg = recharge\
        .join(promote_date_df, "date", "left") \
        .groupBy("phone_number") \
        .agg(
        F.sum(F.expr("recharge_amount")) \
            .alias("recharge_amount_total{}".format(suffix)),
        F.count(F.expr("recharge_amount")) \
            .alias("recharge_count{}".format(suffix)),
        F.avg(F.expr("recharge_amount")) \
            .alias("recharge_amount_avg{}".format(suffix)),
        F.countDistinct("date") \
            .alias("recharge_day_count{}".format(suffix)),

        (F.avg(F.expr("if(pre_balance < 1000, 1, 0)")) * 100.0) \
            .alias("recharge_prebal_lt_1k_pct{}".format(suffix)),
        (F.avg(F.expr("if(pre_balance < 10000, 1, 0)")) * 100.0) \
            .alias("recharge_prebal_lt_10k_pct{}".format(suffix)),
        (F.avg(F.expr("if(pre_balance >= 10000 and pre_balance < 20000, 1, 0)")) * 100.0) \
            .alias("recharge_prebal_10k_20k_pct{}".format(suffix)),
        (F.avg(F.expr("if(pre_balance >= 20000 and pre_balance < 50000, 1, 0)")) * 100.0) \
            .alias("recharge_prebal_20k_50k_pct{}".format(suffix)),
        (F.avg(F.expr("if(pre_balance >= 50000, 1, 0)")) * 100.0) \
            .alias("recharge_prebal_gt_50k_pct{}".format(suffix)),
        F.sum(F.expr("if(pre_balance < 1000, 1, 0)")) \
            .alias("recharge_prebal_lt_1k_count{}".format(suffix)),
        F.sum(F.expr("if(pre_balance < 10000, 1, 0)")) \
            .alias("recharge_prebal_lt_10k_count{}".format(suffix)),
        F.sum(F.expr("if(pre_balance >= 10000 and pre_balance < 20000, 1, 0)")) \
            .alias("recharge_prebal_10k_20k_count{}".format(suffix)),
        F.sum(F.expr("if(pre_balance >= 20000 and pre_balance < 50000, 1, 0)")) \
            .alias("recharge_prebal_20k_50k_count{}".format(suffix)),
        F.sum(F.expr("if(pre_balance >= 50000, 1, 0)")) \
            .alias("recharge_prebal_gt_50k_count{}".format(suffix)),

        (F.avg(F.expr("if(recharge_amount < 10000,1,0)")) * 100.0) \
            .alias("recharge_lt_10k_pct{}".format(suffix)),
        (F.avg(F.expr("if(recharge_amount >= 10000 and recharge_amount <=20000,1,0)")) * 100.0) \
            .alias("recharge_amount_10k_20k_pct{}".format(suffix)),
        (F.avg(F.expr("if(recharge_amount > 20000 and recharge_amount <=50000,1,0)")) * 100.0) \
            .alias("recharge_amount_20k_50k_pct{}".format(suffix)),
        (F.avg(F.expr("if(recharge_amount > 50000 and recharge_amount <=100000,1,0)")) * 100.0) \
            .alias("recharge_amount_50k_100k_pct{}".format(suffix)),
        (F.avg(F.expr("if(recharge_amount > 100000,1,0)")) * 100.0) \
            .alias("recharge_amount_gt_100k_pct{}".format(suffix)),

        F.sum(F.expr("if(recharge_amount < 10000,1,0)")) \
            .alias("recharge_lt_10k_count{}".format(suffix)),
        F.sum(F.expr("if(recharge_amount >= 10000 and recharge_amount <=20000,1,0)")) \
            .alias("recharge_amount_10k_20k_count{}".format(suffix)),
        F.sum(F.expr("if(recharge_amount > 20000 and recharge_amount <=50000,1,0)")) \
            .alias("recharge_amount_20k_50k_count{}".format(suffix)),
        F.sum(F.expr("if(recharge_amount > 50000 and recharge_amount <=100000,1,0)")) \
            .alias("recharge_amount_50k_100k_count{}".format(suffix)),
        F.sum(F.expr("if(recharge_amount > 100000,1,0)")) \
            .alias("recharge_amount_gt_100k_count{}".format(suffix)),

        F.sum(F.expr("if(is_promote =1, recharge_amount, 0)")) \
            .alias("recharge_promote_amount_total{}".format(suffix)),
        F.sum(F.expr("if(is_promote =1, 1, 0)")) \
            .alias("recharge_promote_count{}".format(suffix)),
    ) \
    .withColumn("recharge_promote_amount_pct{}".format(suffix),
                (F.col("recharge_promote_amount_total{}".format(suffix)) * 100.0) / F.col(
                    "recharge_amount_total{}".format(suffix))) \
    .withColumn("recharge_promote_count_pct{}".format(suffix),
                (F.col("recharge_promote_count{}".format(suffix)) * 100.0) / F.col(
                    "recharge_count{}".format(suffix)))

    return ft_agg

ft_l1m = cal_recharge_ft(recharge, promote_date_df, l1m_str, curdate_str, l1m_suffix)
ft_l2m = cal_recharge_ft(recharge, promote_date_df, l2m_str, curdate_str, l2m_suffix)
ft_l3m = cal_recharge_ft(recharge, promote_date_df, l3m_str, curdate_str, l3m_suffix)
ratio_list = ["recharge_amount_total", "recharge_count", "recharge_amount_avg",
              "recharge_day_count",
              "recharge_prebal_lt_1k_count", "recharge_prebal_lt_10k_count",
              "recharge_prebal_10k_20k_count", "recharge_prebal_20k_50k_count",
              "recharge_prebal_gt_50k_count", "recharge_lt_10k_count",
              "recharge_amount_10k_20k_count", "recharge_amount_20k_50k_count",
              "recharge_amount_50k_100k_count", "recharge_amount_gt_100k_count",
              "recharge_promote_amount_total", "recharge_promote_count"]

ep = 0.0001
ft = ft_l1m \
    .join(ft_l2m, "phone_number", "outer") \
    .join(ft_l3m, "phone_number", "outer") \
    .fillna(0.0) \
    .fillna(0)
for col_name in ratio_list:
    print(col_name)
    ft = ft \
        .withColumn("{0}{1}_vs{2}".format(col_name, l1m_suffix, l3m_suffix),
                    (F.col("{}{}".format(col_name, l1m_suffix)) + ep) / (
                        F.col("{}{}".format(col_name, l3m_suffix)) + ep))
ft.write.mode("overwrite").parquet(out_file)
