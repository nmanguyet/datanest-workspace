import sys
from datetime import datetime

import pyspark.sql.functions as F

from etl.common import init_spark3
from etl.common.date_util import DATE_FORMAT

extract_date_str = sys.argv[1]

spark = init_spark3.setup(
    job_cfg={
        "executor.instances": 4,
        "executor.cores": 8,
        "executor.memory": '16g',
        'driver.memory': '4g',
        "driver.maxResultSize": '4g',
    },
    script_name=f'2_1_daily_agg_fts_data_charge_{extract_date_str}'
)

## 1.a data_charge
## input file
data_charge_dir = 'hdfs://cicdataha/data/viettel_v5/data_charge'
### output file
out_dir = 'hdfs://datanest-ha/feature/daily/data_charge_fts_agg_daily'
### a. daily

print("extraction date", extract_date_str)
current_date = datetime.strptime(extract_date_str, DATE_FORMAT)

sdf_data_charge = (
    spark
    .read
    .parquet(f'{data_charge_dir}/date={extract_date_str}')
)

sdf_data_charge_agg = (
    sdf_data_charge
    .withColumn('hour', F.hour('start_time'))
    .withColumn("day_of_week", F.dayofweek(F.lit(extract_date_str)))
    .withColumn("is_weekend",
                F.expr("""
                    case when day_of_week>=2 and day_of_week <=6 then 'weekday'
                         else 'weekend'
                    end
                """)
    )
    .groupBy('phone_number', 'is_weekend', 'hour')
    .agg(F.sum('up_data').alias('up_data'),
         F.sum('down_data').alias('down_data'),
         F.countDistinct('start_time').alias('num_time'),
         F.countDistinct(F.when(F.col('up_data').isNotNull(), F.col('start_time'))).alias('num_time_upload_per_hour'),
         F.countDistinct(F.when(F.col('down_data').isNotNull(), F.col('start_time'))).alias(
             'num_time_download_per_hour'),
    )
    .dropDuplicates()
)

print("writing to file", f'{out_dir}/date={extract_date_str}')
sdf_data_charge_agg.write.mode('overwrite').parquet(f'{out_dir}/date={extract_date_str}')
