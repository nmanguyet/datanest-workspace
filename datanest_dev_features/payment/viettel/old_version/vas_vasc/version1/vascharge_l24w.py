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
        "driver.memory": '4g',
        "driver.maxResultSize": '4g',
    },
    script_name=f'2_24week_fts_vas_charge_{extract_date_str}'
)

input_dir = 'hdfs://datanest-ha/feature/weekly/vascharge_fts_agg_l1w'
# map_dir = '/user/thuynguyen/mapping_target_encoding_vascharge_2021_2022_20220308_gt15_oldlabel'
map_dir = '/project/demographic/age/mapping_table_vascharge/mapping_target_encoding_vascharge_2021_2022_20220308_gt15_oldlabel'
out_dir = 'hdfs://datanest-ha/feature/lxm/vascharge_fts_target_encoding_l24w'

### a. weekly
print("extraction date", extract_date_str)
extract_date_dt = datetime.strptime(extract_date_str, "%Y-%m-%d")

lb_date_dt = extract_date_dt - timedelta(days=23 * 7)
lb_date_str = datetime.strftime(lb_date_dt, "%Y-%m-%d")

lxw_str = "l" + str(6) + "m"
print(f"date from '{lb_date_str}' and '{extract_date_str}'")

vascharge_df_l6m = (
    spark.read.parquet(input_dir)
    .where(f"date between '{lb_date_str}' and '{extract_date_str}'")
    .dropDuplicates()
)

map_target_encoding_df = (
    spark.read.parquet(map_dir)
    .select('service_name', 'mean_age_group', 'std_age_group', 'median_age')
    .dropDuplicates()
)

fts_agg_l24w = (
    vascharge_df_l6m
    .join(map_target_encoding_df, on='service_name')
    .withColumn("multip_num_date_and_mean_age", F.col('vas_charge_dist_date_service_l1w') * F.col('mean_age_group'))
    .repartition('phone_number').groupBy('phone_number')
    .agg(
        F.sum('multip_num_date_and_mean_age').alias(f'vas_charge_sum_date_star_mean_age_{lxw_str}'),
        F.sum('vas_charge_dist_date_service_l1w').alias(f'vas_charge_dist_date_{lxw_str}'),
        F.sum('vas_charge_price_per_pn_service_l1w').alias(f'vas_charge_price_{lxw_str}'),
    )
)

# print
print("writing to file")
fts_agg_l24w.write.mode('overwrite').parquet(f'{out_dir}/date={extract_date_str}')
print(datetime.now())
