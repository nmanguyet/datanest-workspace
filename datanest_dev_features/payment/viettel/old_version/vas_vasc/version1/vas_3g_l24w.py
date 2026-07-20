import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F

from etl.common import date_util
from etl.common import init_spark3

extract_date_str = sys.argv[1]
date_util.check_is_sunday(extract_date_str)

spark = init_spark3.setup(
    job_cfg={
        "executor.instances": 2,
        "executor.cores": 4,
        "executor.memory": '8g',
    },
    script_name=f'2_24week_fts_vas3g_{extract_date_str}'
)

input_dir = 'hdfs://datanest-ha/feature/weekly/vas3g_fts_agg_l1w'
# map_dir = '/data/dimension/age/vas3g/mapping_target_encoding_vas3g_2021_2022'
out_dir = 'hdfs://datanest-ha/feature/lxm/vas3g_fts_agg_l24w'

### a. weekly
print("extraction date", extract_date_str)
extract_date_dt = datetime.strptime(extract_date_str, "%Y-%m-%d")

lb_date_dt = extract_date_dt - timedelta(days=23 * 7)
lb_date_str = datetime.strftime(lb_date_dt, "%Y-%m-%d")

lxw_str = "l" + str(6) + "m"
print(f"date from '{lb_date_str}' and '{extract_date_str}'")

vas3g_df_l6m = (
    spark.read.parquet(input_dir)
    .where(f"date between '{lb_date_str}' and '{extract_date_str}'")
    .dropDuplicates()
)

# package_name_del = ['GOI_KID_DINH_VI', 'KMCAR1', 'KMCAR', 'VTR']
# map_target_encoding_df = (spark.read.parquet(map_dir)
#                               .filter(~F.col('package_name_update').isin(package_name_del))
#                               .select('package_name_update', 'mean_age', 'std_age').dropna()
#                          )
# print('map_target_encoding_df', map_target_encoding_df.cache().count())

fts_agg_l24w = (
    vas3g_df_l6m  # .join(map_target_encoding_df, on='package_name_update')
    .repartition('phone_number').groupBy('phone_number')
    .agg(
        F.countDistinct('package_name_update').alias(f'vas3g_dist_package_{lxw_str}'),
        F.sum('vas3g_dist_date_per_pack_l1w').alias(f'vas3g_dist_date_{lxw_str}'),
        F.sum('vas3g_charge_amount_per_pn_pack_l1w').alias(f'vas3g_charge_amount_{lxw_str}')
    )
    .withColumn(
        f'vas3g_charge_amount_wrt_date_{lxw_str}',
        F.col(f'vas3g_charge_amount_{lxw_str}') / F.col(f'vas3g_dist_date_{lxw_str}')
    )
)

# print
print("writing to file")
fts_agg_l24w.write.mode('overwrite').parquet(f'{out_dir}/date={extract_date_str}')
print(datetime.now())
