from etl.common import init_spark3
import pyspark.sql.functions as F
import pyspark.sql.types as T

import subprocess, sys
from datetime import datetime, timedelta, date
from etl.viettel.common.configs import read_config_service
from etl.common.utils import log_io_file_path
import os

###################################################################################################
#####
###################################################################################################

def extract_data_agg_lxw_feature(extract_date_str, lxw, in_dir, out_dir):
    '''
    
    '''
    try:
        weekly_df = spark.read.parquet(in_dir)
        extract_date_dt = datetime.strptime(extract_date_str, "%Y-%m-%d").date()
        weekly_df = weekly_df.filter((F.col("date") <= extract_date_dt) & (F.col("date") > (extract_date_dt - timedelta(7*lxw))))\
        
        ##################################
        # fail if not enough lxw week data
        ##################################
        distinct_number_of_date = weekly_df.select('date').distinct().count()
        if distinct_number_of_date != lxw:
            print('not enough {} weeks data: {}'.format(lxw, distinct_number_of_date))
            exit(1)
            
        #Get distinct phone number set
        phone_number_df = weekly_df.select("phone_number")\
            .distinct()\
            .coalesce(40)
            
        #Get list of dates
        date_df = weekly_df.select("date")\
            .distinct()\
            .coalesce(20)
            
        #Make data frame with all phone numbers and dates
        phone_number_date_df = phone_number_df.crossJoin(date_df)
        
        #Create daily feature data
        weekly_df = phone_number_date_df.join(weekly_df, on = (["phone_number", "date"]), how = "left")\
        
        #Clean this data by replacing null by 0
        weekly_df = weekly_df.fillna(0)
        
        lxw_str = "_l" + str(lxw) + "w"
        agg_lxw_fts_df = weekly_df.groupBy("phone_number")\
            .agg(\
                #
                F.avg("data_amt_charge_total_l1w")\
                    .alias("data_amt_charge_total_wavg" + lxw_str),\
                F.avg("data_upload_size_l1w")\
                    .alias("data_upload_size_wavg" + lxw_str),\
                F.avg("data_download_size_l1w")\
                    .alias("data_download_size_wavg" + lxw_str),\
                F.avg("data_balance_remain_davg_l1w")\
                    .alias("data_balance_remain_davg" + lxw_str),\
                F.avg("data_hour_distinct_num_davg_l1w")\
                    .alias("data_hour_distinct_num_davg" + lxw_str),\
                F.avg("data_txn_num_l1w")\
                    .alias("data_txn_num_davg" + lxw_str),\
                F.avg("data_usage_time_interval_davg_l1w")\
                    .alias("data_usage_time_interval_davg" + lxw_str),\
                #
                F.avg("data_amt_charge_daytime_total_l1w")\
                    .alias("data_amt_charge_daytime_total_wavg" + lxw_str),\
                F.avg("data_upload_daytime_size_l1w")\
                    .alias("data_upload_daytime_size_wavg" + lxw_str),\
                F.avg("data_download_size_daytime_total_l1w")\
                    .alias("data_download_size_daytime_size_wavg" + lxw_str),\
                F.avg("data_txn_daytime_num_l1w")\
                    .alias("data_txn_daytime_num_wavg" + lxw_str),\
                #
                F.avg("data_amt_charge_weekend_total_l1w")\
                    .alias("data_amt_charge_weekend_total_wavg" + lxw_str),\
                F.avg("data_upload_weekend_size_l1w")\
                    .alias("data_upload_weekend_size_wavg" + lxw_str),\
                F.avg("data_download_weekend_size_l1w")\
                    .alias("data_download_weekend_size_wavg" + lxw_str),\
                F.avg("data_blanace_remain_weekend_avg_l1w")\
                    .alias("data_blanace_remain_weekend_avg_total_wavg" + lxw_str),\
                F.avg("data_hour_distinct_weekend_num_davg_l1w")\
                    .alias("data_hour_distinct_weekend_num_davg" + lxw_str),\
                F.avg("data_txn_weekend_num_l1w")\
                    .alias("data_txn_weekend_num_wavg" + lxw_str),\
                F.avg("usage_time_interval_weekend_davg_l1w")\
                    .alias("usage_time_interval_weekend_davg" + lxw_str),\
                F.avg("data_amt_charge_daytime_weekend_total_l1w")\
                    .alias("data_amt_charge_daytime_weekend_total_wavq" + lxw_str),\
                F.avg("data_upload_size_daytime_weekend_total_l1w")\
                    .alias("data_upload_size_daytime_weekend_total_wavg" + lxw_str),\
                F.avg("data_download_size_daytime_weekend_total_l1w")\
                    .alias("data_download_size_daytime_weekend_total_wavq" + lxw_str),\
                F.avg("data_txn_daytime_weekend_num_l1w")\
                    .alias("data_txn_daytime_weekend_num_wavg" + lxw_str),\
                F.avg("data_active_day_num_l1w")\
                    .alias("data_active_day_num_wavg" + lxw_str),\
                #
                F.avg("data_upload_daytime_vs_wholeday_size_davg_l1w")\
                    .alias("data_upload_daytime_vs_wholeday_size_davg" + lxw_str),\
                F.avg("data_download_daytime_vs_wholeday_size_davg_l1w")\
                    .alias("data_download_daytime_vs_wholeday_size_davg" + lxw_str),\
                F.avg("data_daytime_vs_wholeday_size_davg_l1w")\
                    .alias("data_daytime_vs_wholeday_size_davg" + lxw_str),\
                #
                F.avg("data_upload_vs_download_size_davg_l1w")\
                    .alias("data_upload_vs_download_size_davg" + lxw_str),\
                F.stddev("data_amt_charge_total_l1w")\
                    .alias("data_amt_charge_total_wstd" + lxw_str),\
                F.stddev("data_upload_size_l1w")\
                    .alias("data_upload_size_wstd" + lxw_str),\
                F.stddev("data_download_size_l1w")\
                    .alias("data_download_size_wstd" + lxw_str),\
                F.stddev("data_balance_remain_davg_l1w")\
                    .alias("data_balance_remain_wstd" + lxw_str),\
                F.stddev("data_hour_distinct_num_davg_l1w")\
                    .alias("data_hour_distinct_num_wstd" + lxw_str),\
                F.stddev("data_txn_num_l1w")\
                    .alias("data_txn_num_wstd" + lxw_str),\
                F.stddev("data_usage_time_interval_davg_l1w")\
                    .alias("data_usage_time_interval_wstd" + lxw_str),\
                #
                F.stddev("data_amt_charge_daytime_total_l1w")\
                    .alias("data_amt_charge_daytime_total_wstd" + lxw_str),\
                F.stddev("data_upload_daytime_size_l1w")\
                    .alias("data_upload_daytime_size_wstd" + lxw_str),\
                F.stddev("data_download_size_daytime_total_l1w")\
                    .alias("data_download_size_daytime_wstd" + lxw_str),\
                F.stddev("data_txn_daytime_num_l1w")\
                    .alias("data_txn_daytime_num_wstd" + lxw_str),\
                #
                F.stddev("data_amt_charge_weekend_total_l1w")\
                    .alias("data_amt_charge_weekend_total_wstd" + lxw_str),\
                F.stddev("data_upload_weekend_size_l1w")\
                    .alias("data_upload_weekend_size_wstd" + lxw_str),\
                F.stddev("data_download_weekend_size_l1w")\
                    .alias("data_download_weekend_size_wstd" + lxw_str),\
                F.stddev("data_blanace_remain_weekend_avg_l1w")\
                    .alias("data_blanace_remain_weekend_avg_total_wstd" + lxw_str),\
                F.stddev("data_hour_distinct_weekend_num_davg_l1w")\
                    .alias("data_hour_distinct_weekend_num_wstd" + lxw_str),\
                F.stddev("data_txn_weekend_num_l1w")\
                    .alias("data_txn_weekend_num_wstd" + lxw_str),\
                F.stddev("data_usage_time_interval_weekend_davg_l1w")\
                    .alias("data_usage_time_interval_weekend_wstd" + lxw_str),\
                F.stddev("data_amt_charge_daytime_weekend_total_l1w")\
                    .alias("data_amt_charge_daytime_weekend_total_wstd" + lxw_str),\
                F.stddev("data_upload_size_daytime_weekend_total_l1w")\
                    .alias("data_upload_size_daytime_weekend_total_wstd" + lxw_str),\
                F.stddev("data_download_size_daytime_weekend_total_l1w")\
                    .alias("data_download_size_daytime_weekend_total_wstd" + lxw_str),\
                F.stddev("data_txn_daytime_weekend_num_l1w")\
                    .alias("data_txn_daytime_weekend_num_wstd" + lxw_str),\
                F.stddev("data_active_day_num_l1w")\
                    .alias("data_active_day_num_wstd" + lxw_str),\
                #
                F.stddev("data_upload_daytime_vs_wholeday_size_davg_l1w")\
                    .alias("data_upload_daytime_vs_wholeday_size_wstd" + lxw_str),\
                F.stddev("data_download_daytime_vs_wholeday_size_davg_l1w")\
                    .alias("data_download_daytime_vs_wholeday_size_wstd" + lxw_str),\
                F.stddev("data_daytime_vs_wholeday_size_davg_l1w")\
                    .alias("data_daytime_vs_wholeday_size_wstd" + lxw_str),\
                #
                F.stddev("data_upload_vs_download_size_davg_l1w")\
                    .alias("data_upload_vs_download_size_wstd" + lxw_str)
            )
            
        # Write to file
        print("write to file")
        agg_lxw_fts_df.write.mode("overwrite").parquet(out_dir + "/date=" + extract_date_str)
        
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
job_cfg = weekly_config['agg']['job_config']

script_name_str = "weekly_data_charge_lxw_feature_" + extract_date_str
print("Starting script: ", script_name_str)
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name_str)

# Folders
in_dir = config['path']['weekly']
out_dir = config['path']['agg']

lxw_lst = [4,8,12]
for lxw in lxw_lst:
    extract_data_agg_lxw_feature(extract_date_str, lxw, in_dir, out_dir + "/agg_l" + str(lxw) + "w")
    
print("Finished task")
# Stop spark
spark.stop()
