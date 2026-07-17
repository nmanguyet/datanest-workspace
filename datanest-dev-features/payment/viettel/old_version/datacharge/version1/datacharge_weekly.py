import pyspark.sql.functions as F

from etl.common import date_util
from etl.common import init_spark3
from etl.common.date_util import DATE_FORMAT

extract_date_str = sys.argv[1]
date_util.check_is_sunday(extract_date_str)

spark = init_spark3.setup(
    job_cfg={
        "executor.instances": 10,
        "executor.cores": 8,
        "executor.memory": '20g',
        'driver.memory': '4g',
        "driver.maxResultSize": '4g',
    },
    script_name=f'2_2_weekly_agg_fts_data_charge_{extract_date_str}'
)

## 1.a data_charge
data_charge_dir = 'hdfs://datanest-ha/feature/daily/data_charge_fts_agg_daily'

### output file
out_dir = 'hdfs://datanest-ha/feature/weekly/data_charge_fts_agg_l1w'
### a. weekly

print("extraction date", extract_date_str)
current_date = datetime.strptime(extract_date_str, DATE_FORMAT)
last_1_week_date_str = (current_date - timedelta(days=6)).strftime('%Y-%m-%d')

lxw_str = "l" + str(1) + "w"
print(f"date '{extract_date_str}'")

sdf_data_charge = (
    spark
    .read
    .parquet(data_charge_dir)
    .where(f"date >= '{last_1_week_date_str}' and date <= '{extract_date_str}' ")
    .groupBy('phone_number', 'is_weekend', 'hour')
    .agg(
        F.sum('up_data').alias(f'data_charge_updata_hour_{lxw_str}'),
        F.sum('down_data').alias(f'data_charge_downdata_hour_{lxw_str}'),
        F.countDistinct('date').alias(f'data_charge_num_date_per_hour_{lxw_str}'),
        F.sum('num_time').alias(f'data_charge_num_per_hour_{lxw_str}'),
        F.sum('num_time_upload_per_hour').alias(f'data_charge_num_upload_per_hour_{lxw_str}'),
        F.sum('num_time_download_per_hour').alias(f'data_charge_num_download_per_hour_{lxw_str}'),
    )
    .dropDuplicates()
)

print("writing to file")
sdf_data_charge.write.mode('overwrite').parquet(f'{out_dir}/date={extract_date_str}')
print(datetime.now())
