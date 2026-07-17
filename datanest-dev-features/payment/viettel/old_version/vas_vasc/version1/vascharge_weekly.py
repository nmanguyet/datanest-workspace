import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F

from etl.common import date_util
from etl.common import init_spark3
from etl.common.date_util import DATE_FORMAT

extract_date_str = sys.argv[1]
date_util.check_is_sunday(extract_date_str)

spark = init_spark3.setup(
    job_cfg={
        "executor.instances": 8,
        "executor.cores": 8,
        "executor.memory": '20g',
        "driver.memory": '4g',
        "driver.maxResultSize": '4g',
    },
    script_name=f'1_weekly_fts_vas_charge_{extract_date_str}'
)

## input file
vascharge_daily_dir = 'hdfs://cicdataha/data/viettel_v5/vas_charge'
### output file
out_dir = 'hdfs://datanest-ha/feature/weekly/vascharge_fts_agg_l1w'

print("extraction date", extract_date_str)
current_date = datetime.strptime(extract_date_str, DATE_FORMAT)
last_1_week_date_str = (current_date - timedelta(days=6)).strftime('%Y-%m-%d')

lxw_str = "l" + str(1) + "w"
print(f"date '{extract_date_str}'")

remove_service_name = [
    '972981754',
    'BLOGRADIO',
    'GAME9029_NEW_FTECH',
    'GAME9029_NEW_TDP',
    'GAME9029_NEW_THNK',
    'GAME9029_NEW_VTECH',
    'GAME9029_NONOLIVE',
    'HAIVIP',
    'MECALL',
    'MPLAY_TRUYENTRANH',
    'YOCLIP',
    'EKIDS',
    'GIAODUC_HOCMAI'
]

sdf_vascharge = (
    spark
    .read
    .parquet(vascharge_daily_dir)
    .where(f"date >= '{last_1_week_date_str}' and date <= '{extract_date_str}' ")
    .dropDuplicates(['phone_number', 'date', 'price', 'service_name'])
)

sdf_vascharge_agg = (
    sdf_vascharge
    .groupBy('phone_number', 'service_name')
    .agg(
        F.countDistinct('date').alias(f'vas_charge_dist_date_service_{lxw_str}'),
        F.sum('price').alias(f'vas_charge_price_per_pn_service_{lxw_str}'),
    )
)

print("writing to file")
sdf_vascharge_agg.write.mode('overwrite').parquet(f'{out_dir}/date={extract_date_str}')
