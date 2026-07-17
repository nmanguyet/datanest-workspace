# kernel python3 with pyspark3.
import sys
from datetime import datetime

from dateutil.relativedelta import relative_delta
from pyspark.sql import functions as F

from etl.common import init_spark3
from etl.common import utils
from etl.common.spark_config import large_config

# Init spark
script_name = utils.get_spark_script_name()
spark = init_spark3.setup(job_cfg=large_config(), script_name=script_name)

curdate_str = sys.argv[1]
curdate = datetime.strptime(curdate_str, '%Y-%m-%d')


def get_n_month_first_dates(date_str, n):
    ref_date = datetime.strptime(date_str, '%Y-%m-%d')
    
    first_dates = [
        (ref_date - relative_delta(months=i)).replace(day=1).strftime('%Y-%m-%d') for i in range(n)
    ]
    return first_dates


list_input_date = get_n_month_first_dates(curdate_str, 6)
print(f'list_input_date: {list_input_date}')

usage_pre = (
    spark.read.format('delta').load('/data/viettel_v5/usage_pre')
    .where(f'date in {tuple(list_input_date)}')
    .select('phone_number', 't_tot_cost', 'date')
)

date_data_count = usage_pre.select('date').distinct().count()
if date_data_count != 6:
    print(f'not enough usage_pre data date: {date_data_count}')
    exit(1)

usage_pos = (
    spark.read.format('delta').load('/data/viettel_v5/usage_pos')
    .where(f'date in {tuple(list_input_date)}')
    .withColumn('t_tot_cost', F.expr('t_org_cost + v_int_org_vost + v_ext_org_cost + v_intn_org_cost'))
    .select('phone_number', 't_tot_cost', 'date')
)

date_data_count = usage_pos.select('date').distinct().count()
if date_data_count != 6:
    print(f'not enough usage_pos data date: {date_data_count}')
    exit(1)

usage_combine = (
    usage_pre.unionByName(usage_pos)
    .groupBy('phone_number')
    .agg(
        F.avg('t_tot_cost').alias('avg_total_cost_l6m')
    )
)

out_dir = 'hdfs://datanest-ha/data/processed/inventory/features/usage/avg_total_cost_l6m'
out_file = f'{out_dir}/date={curdate_str}'

usage_combine.write.mode('overwrite').parquet(out_file)