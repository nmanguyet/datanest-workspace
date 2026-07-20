import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F

from etl.common import date_util
from etl.common import init_spark3

extract_date_str = sys.argv[1]
date_util.check_is_sunday(extract_date_str)

spark = init_spark3.setup(
    job_cfg={
        "executor.instances": 8,
        "executor.cores": 8,
        "executor.memory": '20g',
        'driver.memory': '4g',
        "driver.maxResultSize": '4g',
    },
    script_name=f'2_4_24week_fts_data_charge_by_hour_{extract_date_str}'
)

input_dir = 'hdfs://datanest-ha/feature/weekly/data_charge_fts_agg_l1w'

out_dir = 'hdfs://datanest-ha/feature/lxm/data_charge_fts_agg_by_hour_l24w'

### a. weekly
print("extraction date", extract_date_str)
extract_date_dt = datetime.strptime(extract_date_str, "%Y-%m-%d")

lb_date_dt = extract_date_dt - timedelta(days=23 * 7)
lb_date_str = datetime.strftime(lb_date_dt, "%Y-%m-%d")

lxw_str = "l" + str(24) + "w"
print(f"date from '{lb_date_str}' and '{extract_date_str}'")

data_charge_df_l24w = (
    spark.read.parquet(input_dir)
    .where(f"date between '{lb_date_str}' and '{extract_date_str}'")
    .dropDuplicates()
)

fts_agg_l24w = (
    data_charge_df_l24w.groupBy('phone_number', 'is_weekend', 'hour')
    .agg(
        F.avg('data_charge_updata_hour_l1w').alias(f'data_charge_avg_updata_per_hour_wk_{lxw_str}'),  ##used
        F.avg('data_charge_downdata_hour_l1w').alias(f'data_charge_avg_downdata_per_hour_wk_{lxw_str}'),  ## used
        F.sum('data_charge_num_date_per_hour_l1w').alias(f'data_charge_num_date_{lxw_str}'),  ##used
        F.sum('data_charge_num_upload_per_hour_l1w').alias(f'data_charge_num_upload_per_hour_{lxw_str}'),  ###used
    )
)

range_hour = [0, 1, 5, 6, 7, 8, 9, 10, 12, 19, 20, 21, 22]
fts_agg_l24w_hour = (
    fts_agg_l24w.groupBy('phone_number')
    .agg(
        *(F.sum(F.when(F.col('hour') == i, (F.col(f'data_charge_avg_updata_per_hour_wk_{lxw_str}'))))
          .alias(f'data_charge_updata_at_{i}_per_wk_{lxw_str}') for i in set(range_hour)),  ##used
        *(F.sum(F.when(F.col('hour') == hour, (F.col(f'data_charge_avg_downdata_per_hour_wk_{lxw_str}'))))
          .alias(f'data_charge_downdata_at_{hour}_{lxw_str}') for hour in set(range_hour)),  ### used
        *(F.sum(F.when(F.col('hour') == hour, (F.col(f'data_charge_num_date_{lxw_str}'))))
          .alias(f'data_charge_num_date_at_{hour}_{lxw_str}') for hour in set(range_hour)),  ###used
        *(F.sum(F.when(F.col('hour') == hour, (F.col(f'data_charge_num_upload_per_hour_{lxw_str}'))))
          .alias(f'data_charge_num_upload_at_{hour}_{lxw_str}') for hour in set(range_hour)),  ### used
    )
)

# print
print("writing to file")
fts_agg_l24w_hour.write.mode('overwrite').parquet(f'{out_dir}/date={extract_date_str}')
print(datetime.now())
