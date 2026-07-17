import sys
from datetime import datetime as dt

from etl.common.init_spark3 import setup
from etl.vnpt.spark_config import medium_config

PY_NORM_FDATE = "%Y-%m-%d"
SPARK_NORM_FDATE = "yyyy-MM-dd"
from pyspark.sql import functions as F
from etl.common.utils import check_daily_count


# if len(sys.argv) != 2:
#     print('need processing date string: python {} YYYY-mm-dd'.format(sys.argv[0]), file=sys.stderr)
#     exit(1)
curdate_str = sys.argv[1]
curdate = dt.strptime(curdate_str, "%Y-%m-%d")

# if not check_daily_count('balance', curdate) or not check_daily_count('air', curdate, DAILY_COUNT_THRESHOLD=0.3):
#     exit(1)

spark = setup(job_cfg=medium_config(), script_name="airflow_recharge_daily_" + curdate_str)

balance_dir = "/data/vnpt_v2/balance"
air_dir = "/data/vnpt_v2/air"

out_dir = "/feature/daily/recharge/05_AIR_with_bal"
air_file = "{}/date={}".format(air_dir, curdate_str)
balance_file = "{}/date={}".format(balance_dir, curdate_str)
out_file = "{}/date={}".format(out_dir, curdate_str)
print(air_file, balance_file, out_file)

air = spark.read.format("delta").load(air_dir).where(f'date="{curdate_str}"')
balance = spark.read.format("delta").load(balance_dir).where(f'date="{curdate_str}"')
balance_select = balance\
    .join(air.select("phone_number").distinct(), "phone_number")\
    .filter(~(balance.account_code.startswith('3')))\
    .filter(~(balance.account_code.contains("142")))\
    .filter("balance >= 0")\
    .groupBy("phone_number").agg(F.sum("balance").alias("pre_balance"))

air_bal = air.join(balance_select, "phone_number", "left")
air_bal.write.mode("overwrite").parquet(out_file)
