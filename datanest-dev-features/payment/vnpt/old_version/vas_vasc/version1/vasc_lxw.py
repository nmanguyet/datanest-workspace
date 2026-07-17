import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from etl.common.init_spark3 import init_spark3

curdate_str = sys.argv[1]
curdate = datetime.strptime(curdate_str, "%Y-%m-%d")

if curdate.weekday() != 6:
    print("Not Sunday, no need to run")
    exit(0)

job_cfg = {
    "executor.instances": 4,
    "executor.cores": 10,
    "executor.memory": "10g"
}

script_name = f"bsv3_vas_weekly_feature_{curdate_str}"
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name)

def extract_lxw_future_value(in_dir, out_dir, lxw):
    start_date = curdate - timedelta(days=lxw*7 - 1)
    end_date = curdate
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    lxw_str = "l" + str(lxw) + "w"
    print("\n\n")
    print(f"lxw: {lxw_str}")
    print(f"in_dir: {in_dir}")
    print(f"date between {start_date_str} and {end_date_str}")

    df = spark.read.format("delta").load(in_dir) \
        .where("date between '" + start_date_str + "' and '" + end_date_str + "'")

    lxw_df1 = df.groupBy("phone_number") \
        .agg(F.count("*").alias("vasc_txn_num_" + lxw_str),
             F.countDistinct("date").alias("vasc_day_num_" + lxw_str))

    tmp1 = df.where("service_name != '2FRIEND'") \
        .selectExpr("phone_number", "amount", "date")

    tmp2 = df.where("service_name = '2FRIEND'") \
        .selectExpr("receive_phone_number", "amount", "date") \
        .withColumnRenamed("receive_phone_number", "phone_number")

    tmp3 = tmp2.unionByName(tmp1)

    lxw_df2 = tmp3.groupBy("phone_number") \
        .agg(F.count("*").alias("vasc_credit_txn_num_" + lxw_str),
             F.sum("amount").alias("vasc_credit_amt_" + lxw_str),
             F.countDistinct("date").alias("vasc_credit_day_num_" + lxw_str))

    lxw_df3 = df.where("service_name == '2FRIEND'") \
        .selectExpr("receive_phone_number", "amount", "date") \
        .withColumnRenamed("receive_phone_number", "phone_number") \
        .groupBy("phone_number") \
        .agg(F.count("*").alias("vasc_2friend_num_" + lxw_str),
             F.sum("amount").alias("vasc_2friend_amt_" + lxw_str),
             F.countDistinct("date").alias("vasc_2friend_day_num_" + lxw_str))

    lxw_df = lxw_df1.join(lxw_df2, on="phone_number", how='outer') \
        .join(lxw_df3, on="phone_number", how='outer')

    # write
    out_file = os.path.join(out_dir + "/" + lxw_str + "/date=" + curdate_str)
    print(f"out_file: {out_file}")
    lxw_df.write.mode("overwrite").parquet(out_file)

in_dir = "/data/vnpt_v2/vasc"
out_dir = "/data/DS/project/bs_rnd/202107/whole_population/feature_value/vasc/"

lxw_lst = [1, 2, 4, 12]
for lxw in lxw_lst:
    extract_lxw_future_value(in_dir, out_dir, lxw)

from etl.common.data_lineage import data_lineage
dl = data_lineage.DataLineage()
dl.log_io(input_paths=[in_dir], output_paths=[out_dir], script=__file__)
