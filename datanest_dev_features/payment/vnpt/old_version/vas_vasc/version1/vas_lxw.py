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
    "executor.memory": "10g"
}

script_name = f"bsv3_vas_weekly_feature_{curdate_str}"
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name)

# Chương Thế Kiệt
def extract_lxw_future_value(in_dir, out_dir, lxw):
    start_date = curdate - timedelta(days=7*lxw - 1)
    end_date = curdate
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    print(f"in_dir: {in_dir}")
    print(f"date between {start_date_str} and {end_date_str}")

    raw_df = spark.read.parquet(in_dir) \
        .where("date between '" + start_date_str + "' and '" + end_date_str + "'")

    lxw_str = "l" + str(lxw) + "w"
    lxw_df = raw_df.groupBy("phone_number") \
        .agg(F.sum('vas_txn_num_l1w').alias("vas_txn_num_" + lxw_str),
             F.sum('vas_txn_amt_l1w').alias("vas_txn_amt_" + lxw_str),
             F.max('vas_txn_max_l1w').alias("vas_txn_max_" + lxw_str),
             F.sum('vas_day_num_l1w').alias("vas_day_num_" + lxw_str),
             F.sum('vas_reg_txn_num_l1w').alias("vas_reg_txn_num_" + lxw_str),
             F.sum('vas_reg_txn_amt_l1w').alias("vas_reg_txn_amt_" + lxw_str),
             F.max('vas_reg_txn_max_l1w').alias("vas_reg_txn_max_" + lxw_str),
             F.sum('vas_reg_day_num_l1w').alias("vas_reg_day_num_" + lxw_str),
             F.sum('vas_unreg_txn_num_l1w').alias("vas_unreg_txn_num_" + lxw_str),
             F.sum('vas_unreg_txn_amt_l1w').alias("vas_unreg_txn_amt_" + lxw_str),
             F.sum('vas_unreg_day_num_l1w').alias("vas_unreg_day_num_" + lxw_str),
             F.sum('vas_content_txn_num_l1w').alias("vas_content_txn_num_" + lxw_str),
             F.sum('vas_content_txn_amt_l1w').alias("vas_content_txn_amt_" + lxw_str),
             F.max('vas_content_txn_max_l1w').alias("vas_content_txn_max_" + lxw_str),
             F.sum('vas_content_day_num_l1w').alias("vas_content_day_num_" + lxw_str)
        )

    # Write data
    out_dir = os.path.join(out_dir + "/" + lxw_str + "/date=" + curdate_str)
    print(f"out_dir: {out_dir}")
    lxw_df.write.mode("overwrite").parquet(out_dir)

in_dir = "/data/DS/project/bs_rnd/202107/whole_population/feature_value/vas/weekly_feature"
out_dir = "/data/DS/project/bs_rnd/202107/whole_population/feature_value/vas"

lxw_list = [2, 4, 8]

for lxw in lxw_list:
    extract_lxw_future_value(in_dir, out_dir, lxw)

from etl.common.data_lineage import data_lineage
dl = data_lineage.DataLineage()
dl.log_io(input_paths=[in_dir], output_paths=[out_dir], script=__file__)
