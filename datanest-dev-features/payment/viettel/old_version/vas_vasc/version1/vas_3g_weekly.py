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
    script_name=f'1_weekly_fts_vas3g_{extract_date_str}'
)

## input file
vas3g_daily_dir = '/data/viettel_v5/vas_2g3g'
### output file
out_dir = 'hdfs://datanest-ha/feature/weekly/vas3g_fts_agg_l1w'
### a. weekly

print("extraction date", extract_date_str)
current_date = datetime.strptime(extract_date_str, DATE_FORMAT)
last_1_week_date_str = (current_date - timedelta(days=6)).strftime('%Y-%m-%d')

lxw_str = "l" + str(1) + "w"
print(f"date '{extract_date_str}'")

sdf_vas3g = (
    spark
    .read
    .parquet(vas3g_daily_dir)
    .where(f"date >= '{last_1_week_date_str}' and date <= '{extract_date_str}' ")
    .withColumn('package_name_update', F.expr("""
        case when (package_name like "%X") and (package_name != 'MIMAX') then substring(package_name,0,length(package_name)-1)
             when package_name like "%_60" then  regexp_replace(package_name,"_60","")
             when package_name like "%_TS" then  regexp_replace(package_name,"_TS","")
             
             when package_name like "%VOICE%" then regexp_replace(package_name,"-VOICE|VOICE|_VOICE1","")
             when package_name like "%DATA%" then regexp_replace(package_name,"-DATA|_DATA","")
             when package_name like "VTR%" then  substring(package_name,0,3)
             
             when package_name like "%_NEW"  then  substring(package_name,0,length(package_name)-4)
             
             when package_name like "%_KDS"  then "GOI_CUOC_DOANH_NGH_TANG/BAN"
             when (package_name like "%KID%") or (package_name like "KID%")  then  "GOI_KID_DINH_VI"
             
             when package_name like "%_4G%"  then  regexp_replace(package_name,"_4G|_4G_","")
             when package_name = "%4G1"  then regexp_replace(package_name,"1","")
             when package_name = "%4GN"  then regexp_replace(package_name,"N","")
             when package_name like "%_4"  then regexp_replace(package_name,"_4","")
             when package_name like "%_1"  then regexp_replace(package_name,"_1","")
             when (package_name != "%CAMAU_BACLIEU%") and (package_name != "CAMAU_BACLIEU") and (package_name like "%U") 
             then substring(package_name,0,length(package_name)-1)
             when (package_name != "%CAMAU_BACLIEU%") and (package_name != "CAMAU_BACLIEU") and (package_name like "%U") 
             then substring(package_name,0,length(package_name)-1)
        else package_name
        end
    """))
    .where(
        "(package_name_update not like 'VTR%') OR (package_name_update not like '%CAR%') OR (package_name_update != 'GOI_KID_DINH_VI')"
    )
    .dropDuplicates(['phone_number', 'date', 'charge_amount', 'package_name_update'])
)

sdf_vas3g_agg = (
    sdf_vas3g
    .groupBy('phone_number', 'package_name_update')
    .agg(
        F.countDistinct('date').alias(f'vas3g_dist_date_per_pack_{lxw_str}'),
        F.sum('charge_amount').alias(f'vas3g_charge_amount_per_pn_pack_{lxw_str}'),
    )
)

print("writing to file")
sdf_vas3g_agg.write.mode('overwrite').parquet(f'{out_dir}/date={extract_date_str}')
