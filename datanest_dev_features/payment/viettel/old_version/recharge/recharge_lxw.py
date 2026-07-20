import sys
from datetime import datetime as dt

import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta

from etl.common import init_spark3
from etl.viettel.common.configs import read_config_service
from etl.viettel.common.time_util import PY_NORM_DATE, is_sunday

current_date_str = sys.argv[1]
curdate = dt.strptime(current_date_str, PY_NORM_DATE)

# if not check_weekly_count('recharge', curdate, DAILY_COUNT_THRESHOLD=0.55):
#     exit(1)

if not is_sunday(curdate):
    print("only run in sunday")
    exit(0)

config = read_config_service.get_compute_features_config()['recharge']
weekly_config = config['weekly']
job_cfg = weekly_config['agg']['job_config']
script_name = 'recharge_feature_{}'.format(sys.argv[1])
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name)

print("http://master01.cicdata.io:8088/proxy/{}".format(spark.sparkContext.applicationId))

in_dir = 'hdfs://cicdataha/project/cs_hc_generic_v3/sample_data/data/viettel_v4/recharge'
out_dir = 'hdfs://cicdataha/project/cs_hc_generic_v3/feature/lxw/recharge/12m'

out_file = "{}/date={}".format(out_dir, current_date_str)
print(out_file)

l3m_date_str = dt.strftime(curdate - relativedelta(months=3), PY_NORM_DATE)
l6m_date_str = dt.strftime(curdate - relativedelta(months=6), PY_NORM_DATE)
l12m_date_str = dt.strftime(curdate - relativedelta(months=12), PY_NORM_DATE)
l3m_suffix = "_l3m"
l6m_suffix = "_l6m"
l12m_suffix = "_l12m"

recharge = spark.read.parquet(in_dir).where('recharge_amount > 0')

# join with sub active day
df_lad = (spark.read.parquet(
    f'hdfs://cicdataha/project/cs_hc_generic_v3/sample_data/data/processed/inventory/features/sub/lad_union_bal/weekly/date={current_date_str}')
    .withColumnRenamed("start_date", "last_activated_date"))

recharge = (recharge.join(df_lad, 'phone_number')
            .where('date >= last_activated_date')
           )

# todo: update promote theshold
thres_promote = 3000000

def cal_recharge_ft(recharge, start_date_str, end_date_str, suffix):
    """
    calculate feature recharge from (start_date, end_date]
    @param recharge: recharge dataframe.
    @param start_date_str: yyyy-MM-dd string
    @param end_date_str: yyyy-MM-dd string
    @param suffix:
    @return:
    """
    
    # calculate promote dataframe.
    promote_date_df = recharge \
        .where("date > '{}' and date <= '{}'".format(start_date_str, end_date_str)) \
        .groupBy("date").count() \
        .where("count >= {}".format(thres_promote)) \
        .selectExpr("date", "1 is_promote")

    ft_agg = recharge \
        .where("date > '{}' and date <= '{}'".format(start_date_str, end_date_str)) \
        .join(promote_date_df, "date", "left") \
        .groupBy("phone_number") \
        .agg(
            F.sum(F.expr("if(trade_method = 'V', recharge_amount, 0)")).alias("recharge_amount_v_total{}".format(suffix)),
            F.sum(F.expr("if(trade_method = 'V', 1, 0)")).alias("recharge_count_v{}".format(suffix)),
            F.sum(F.expr("if(trade_method = 'C', recharge_amount, 0)")).alias("recharge_amount_c_total{}".format(suffix)),
            F.sum(F.expr("if(trade_method = 'C', 1, 0)")).alias("recharge_count_c{}".format(suffix)),
            (F.sum(F.expr("if(trade_method = 'V', 1, 0)")) * 100.0 / F.count("recharge_amount")).alias("recharge_count_v_pct{}".format(suffix)),
            (F.sum(F.expr("if(trade_method = 'V', recharge_amount, 0)")) * 100.0 / F.sum("recharge_amount")).alias("recharge_amount_v_pct{}".format(suffix)),
            F.countDistinct("trade_method").alias("recharge_method_count{}".format(suffix)),
            F.sum(F.expr("recharge_amount")).alias("recharge_amount_total{}".format(suffix)),
            F.count(F.expr("recharge_amount")).alias("recharge_count{}".format(suffix)),
            F.avg(F.expr("recharge_amount")).alias("recharge_amount_avg{}".format(suffix)),
            F.countDistinct("date").alias("recharge_day_count{}".format(suffix)),
            
            (F.avg(F.expr("if(pre_balance < 1000, 1, 0)")) * 100.0).alias("recharge_prebal_lt_1k_pct{}".format(suffix)),
            (F.avg(F.expr("if(pre_balance < 10000, 1, 0)")) * 100.0).alias("recharge_prebal_lt_10k_pct{}".format(suffix)),
            (F.avg(F.expr("if(pre_balance >= 10000 and pre_balance < 20000, 1, 0)")) * 100.0).alias("recharge_prebal_10k_20k_pct{}".format(suffix)),
            (F.avg(F.expr("if(pre_balance >= 20000 and pre_balance < 50000, 1, 0)")) * 100.0).alias("recharge_prebal_20k_50k_pct{}".format(suffix)),
            (F.avg(F.expr("if(pre_balance >= 50000, 1, 0)")) * 100.0).alias("recharge_prebal_gt_50k_pct{}".format(suffix)),
            
            F.sum(F.expr("if(pre_balance < 1000, 1, 0)")).alias("recharge_prebal_lt_1k_count{}".format(suffix)),
            F.sum(F.expr("if(pre_balance < 10000, 1, 0)")).alias("recharge_prebal_lt_10k_count{}".format(suffix)),
            F.sum(F.expr("if(pre_balance >= 10000 and pre_balance < 20000, 1, 0)")).alias("recharge_prebal_10k_20k_count{}".format(suffix)),
            F.sum(F.expr("if(pre_balance >= 20000 and pre_balance < 50000, 1, 0)")).alias("recharge_prebal_20k_50k_count{}".format(suffix)),
            F.sum(F.expr("if(pre_balance >= 50000, 1, 0)")).alias("recharge_prebal_gt_50k_count{}".format(suffix)),
            
            (F.avg(F.expr("if(recharge_amount < 10000, 1, 0)")) * 100.0).alias("recharge_lt_10k_pct{}".format(suffix)),
            (F.avg(F.expr("if(recharge_amount >= 10000 and recharge_amount <= 20000, 1, 0)")) * 100.0).alias("recharge_amount_10k_20k_pct{}".format(suffix)),
            (F.avg(F.expr("if(recharge_amount > 20000 and recharge_amount <= 50000, 1, 0)")) * 100.0).alias("recharge_amount_20k_50k_pct{}".format(suffix)),
            (F.avg(F.expr("if(recharge_amount > 50000 and recharge_amount <= 100000, 1, 0)")) * 100.0).alias("recharge_amount_50k_100k_pct{}".format(suffix)),
            (F.avg(F.expr("if(recharge_amount > 100000, 1, 0)")) * 100.0).alias("recharge_amount_gt_100k_pct{}".format(suffix)),
            
            F.sum(F.expr("if(recharge_amount < 10000, 1, 0)")).alias("recharge_lt_10k_count{}".format(suffix)),
            F.sum(F.expr("if(recharge_amount >= 10000 and recharge_amount <= 20000, 1, 0)")).alias("recharge_amount_10k_20k_count{}".format(suffix)),
            F.sum(F.expr("if(recharge_amount > 20000 and recharge_amount <= 50000, 1, 0)")).alias("recharge_amount_20k_50k_count{}".format(suffix)),
            F.sum(F.expr("if(recharge_amount > 50000 and recharge_amount <= 100000, 1, 0)")).alias("recharge_amount_50k_100k_count{}".format(suffix)),
            F.sum(F.expr("if(recharge_amount > 100000, 1, 0)")).alias("recharge_amount_gt_100k_count{}".format(suffix)),
            
            F.sum(F.expr("if(is_promote = 1, recharge_amount, 0)")).alias("recharge_promote_amount_total{}".format(suffix)),
            F.sum(F.expr("if(is_promote = 1, 1, 0)")).alias("recharge_promote_count{}".format(suffix))
        ) \
        .withColumn("recharge_promote_amount_pct{}".format(suffix), 
                    (F.col("recharge_promote_amount_total{}".format(suffix)) * 100.0) / F.col("recharge_amount_total{}".format(suffix))) \
        .withColumn("recharge_promote_count_pct{}".format(suffix), 
                    (F.col("recharge_promote_count{}".format(suffix)) * 100.0) / F.col("recharge_count{}".format(suffix)))

    return ft_agg

ft_l3m = cal_recharge_ft(recharge, l3m_date_str, current_date_str, l3m_suffix)
ft_l6m = cal_recharge_ft(recharge, l6m_date_str, current_date_str, l6m_suffix)
ft_l12m = cal_recharge_ft(recharge, l12m_date_str, current_date_str, l12m_suffix)

ratio_list = [
    "recharge_amount_total", "recharge_count", "recharge_amount_avg",
    "recharge_day_count",
    "recharge_prebal_lt_1k_count", "recharge_prebal_lt_10k_count",
    "recharge_prebal_10k_20k_count", "recharge_prebal_20k_50k_count",
    "recharge_prebal_gt_50k_count", "recharge_lt_10k_count",
    "recharge_amount_10k_20k_count", "recharge_amount_20k_50k_count",
    "recharge_amount_50k_100k_count", "recharge_amount_gt_100k_count",
    "recharge_promote_amount_total", "recharge_promote_count"
]

ep = 0.0001
ft = ft_l3m \
    .join(ft_l6m, "phone_number", "outer") \
    .join(ft_l12m, "phone_number", "outer") \
    .fillna(0.0) \
    .fillna(0)

for col_name in ratio_list:
    print(col_name)
    ft = ft \
        .withColumn(f"{col_name}{l3m_suffix}_vs{l12m_suffix}",
                    (F.col(f"{col_name}{l3m_suffix}") + ep) / (F.col(f"{col_name}{l12m_suffix}") + ep))

ft.write.mode("overwrite").parquet(out_file)
