from etl.common import init_spark3
import pyspark.sql.functions as F
import pyspark.sql.types as T

import subprocess, sys
from datetime import datetime, timedelta, date
from etl.viettel.common.configs import read_config_service
from etl.common.utils import log_io_file_path

###################################################################################################
#####
###################################################################################################

def extract_data_weekly_feature(extract_date_str, in_dir, out_dir):
    '''
    
    '''
    try:
        daily_df = spark.read.parquet(in_dir)
        extract_date_dt = datetime.strptime(extract_date_str, "%Y-%m-%d").date()
        daily_df = daily_df.filter((F.col("date") <= extract_date_dt) & (F.col("date") > (extract_date_dt - timedelta(7))))

        ### Fail if not enough 7 day data
        distinct_number_of_date = daily_df.select("date").distinct().count()
        if distinct_number_of_date != 7:
            print('not enough 7 days data: {}'.format(distinct_number_of_date))
            exit(1)

        #Clean this data by replacing null by 0
        daily_df = daily_df.fillna(0)

        # Add weekend/weekday dimension
        daily_df = daily_df\
            .withColumn("is_weekend", F.when((F.dayofweek(F.col("date")) >= 2 )\
                                            & (F.dayofweek(F.col("date")) <= 6), 0).otherwise(1))\
            .withColumn("usage_time_interval", F.col("data_time_end")\
                                            .cast("long") - F.col("data_time_start").cast("long"))\
            .withColumn("data_upload_vs_download_size", F.col("data_upload_size") / F.col("data_download_size"))\
            .withColumn("data_upload_daytime_vs_wholeday_size",\
                        F.col("data_upload_size_daytime_total") / F.col("data_upload_size"))\
            .withColumn("data_download_daytime_vs_wholeday_size",\
                        F.col("data_download_size_daytime_total") / F.col("data_download_size"))\
            .withColumn("data_daytime_vs_wholeday_size",\
                        (F.col("data_download_size_daytime_total") + F.col("data_upload_size_daytime_total")) / \
                        (F.col("data_download_size") + F.col("data_upload_size")))

        #Extract weekly features
        lxw_str = "_l1w"
        weekly_fts_df = daily_df.groupBy("phone_number")\
            .agg(\
                F.sum("data_amt_charge_total").alias("data_amt_charge_total" + lxw_str),\
                F.sum("data_upload_size").alias("data_upload_size" + lxw_str),\
                F.sum("data_download_size").alias("data_download_size" + lxw_str),\
                F.avg("data_blanace_remain_avg").alias("data_balance_remain_davg" + lxw_str),\
                F.avg("data_hour_distinct_num").alias("data_hour_distinct_num_davg" + lxw_str),\
                F.sum("data_txn_num").alias("data_txn_num" + lxw_str),\
                F.avg("usage_time_interval").alias("data_usage_time_interval_davg" + lxw_str),\
                #
                F.sum("data_amt_charge_daytime_total").alias("data_amt_charge_daytime_total" + lxw_str),\
                F.sum("data_upload_size_daytime_total").alias("data_upload_size_daytime" + lxw_str),\
                F.sum("data_download_size_daytime_total").alias("data_download_size_daytime_total" + lxw_str),\
                F.sum("data_txn_daytime_num").alias("data_txn_daytime_num" + lxw_str),\
                #
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_amt_charge_total") ))\
                    .alias("data_amt_charge_weekend_total" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_upload_size") ))\
                    .alias("data_upload_weekend_size" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_download_size") ))\
                    .alias("data_download_weekend_size" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_blanace_remain_avg") ))\
                    .alias("data_blanace_remain_weekend_avg" + lxw_str),\
                F.avg(F.when(F.col("is_weekend") == 1, F.col("data_hour_distinct_num") ))\
                    .alias("data_hour_distinct_weekend_num_davg" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_txn_num") ))\
                    .alias("data_txn_weekend_num" + lxw_str),\
                F.avg(F.when(F.col("is_weekend") == 1, F.col("usage_time_interval") ))\
                    .alias("data_usage_time_interval_weekend_davg" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_amt_charge_daytime_total") ))\
                    .alias("data_amt_charge_daytime_weekend_total" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_upload_size_daytime_total") ))\
                    .alias("data_upload_size_daytime_weekend_total" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_download_size_daytime_total") ))\
                    .alias("data_download_size_daytime_weekend_total" + lxw_str),\
                F.sum(F.when(F.col("is_weekend") == 1, F.col("data_txn_daytime_num") ))\
                    .alias("data_txn_weekend_daytime_num" + lxw_str),\
                F.count(F.when(F.col("data_txn_num") >= 1, True))\
                    .alias("data_active_day_num" + lxw_str),\
                F.avg("data_upload_daytime_vs_wholeday_size")\
                    .alias("data_upload_daytime_vs_wholeday_size_davg" + lxw_str),\
                F.avg("data_download_daytime_vs_wholeday_size")\
                    .alias("data_download_daytime_vs_wholeday_size_davg" + lxw_str),\
                F.avg("data_daytime_vs_wholeday_size")\
                    .alias("data_daytime_vs_wholeday_size_davg" + lxw_str),\
                #
                F.avg("data_upload_vs_download_size")\
                    .alias("data_upload_vs_download_size_davg" + lxw_str)
            )

        # Write to file
        print("write to file")
        weekly_fts_df.write.mode("overwrite").parquet(out_dir + "/date=" + extract_date_str)

    except Exception as e:
        print(e)

###################################################################################################
# start and end dates
extract_date_str = sys.argv[1]
extract_date = datetime.strptime(extract_date_str, '%Y-%m-%d')
if extract_date.weekday() != 6:
    print('Not Sunday, no need to run')
    exit(0)

# Initiate environment
config = read_config_service.get_compute_features_config()['g22_data_charge']
weekly_config = config['weekly']
job_cfg = weekly_config['weekly']['job_config']

script_name_str = "weekly_data_charge_weekly_feature_" + extract_date_str
print("Starting script: ", script_name_str)
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name_str)

# Folders
in_dir = config['path']['daily']
out_dir = config['path']['weekly']

extract_data_weekly_feature(extract_date_str, in_dir, out_dir)

print("Finished task")
