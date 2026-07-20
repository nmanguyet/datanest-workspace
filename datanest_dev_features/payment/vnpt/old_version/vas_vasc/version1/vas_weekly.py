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

def extract_week_future_value(in_dir, out_dir):
    start_date = curdate - timedelta(days=6)
    end_date = curdate
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Read data
    print(f"in_dir: {in_dir}")
    print(f"date between {start_date_str} and {end_date_str}")

    raw_df = spark.read.format("delta").load(in_dir) \
        .where("date between '" + start_date_str + "' and '" + end_date_str + "'")

    week_df1 = raw_df.groupBy("phone_number") \
        .agg(F.count("*").alias("vas_txn_num_l1w"),
             F.sum("charge_amount").alias("vas_txn_amt_l1w"),
             F.max("charge_amount").alias("vas_txn_max_l1w"),
             F.countDistinct("date").alias("vas_day_num_l1w")
        )

    week_df2 = raw_df.where("transaction_type == 'REG'") \
        .groupBy("phone_number") \
        .agg(F.count("*").alias("vas_reg_txn_num_l1w"),
             F.sum("charge_amount").alias("vas_reg_txn_amt_l1w"),
             F.max("charge_amount").alias("vas_reg_txn_max_l1w"),
             F.countDistinct("date").alias("vas_reg_day_num_l1w")
        )

    week_df3 = raw_df.where("transaction_type == 'UNREG'") \
        .groupBy("phone_number") \
        .agg(F.count("*").alias("vas_unreg_txn_num_l1w"),
             F.sum("charge_amount").alias("vas_unreg_txn_amt_l1w"),
             F.countDistinct("date").alias("vas_unreg_day_num_l1w")
        )

    week_df4 = raw_df.where("transaction_type == 'CONTENT'") \
        .groupBy("phone_number") \
        .agg(F.count("*").alias("vas_content_txn_num_l1w"),
             F.sum("charge_amount").alias("vas_content_txn_amt_l1w"),
             F.max("charge_amount").alias("vas_content_txn_max_l1w"),
             F.countDistinct("date").alias("vas_content_day_num_l1w")
        )

    week_df = week_df1.join(week_df2, on='phone_number', how='outer') \
        .join(week_df3, on='phone_number', how='outer') \
        .join(week_df4, on='phone_number', how='outer')

    out_file = os.path.join(out_dir, f"date={curdate_str}")
    print(f"out_file: {out_file}")
    week_df.write.mode("overwrite").parquet(out_file)

in_dir = "/data/vnpt_v2/vas"
out_dir = "/data/DS/project/bs_rnd/202107/whole_population/feature_value/vas/weekly_feature"

extract_week_future_value(in_dir, out_dir)

from etl.common.data_lineage import data_lineage
dl = data_lineage.DataLineage()
dl.log_io(input_paths=[in_dir], output_paths=[out_dir], script=__file__)
