# Import important modules
import base64
import dateutil.relativedelta
import pyspark.sql.functions as F
import pyspark.sql.types as T
import struct
import sys
from datetime import datetime, timedelta, date

from etl.common import init_spark3
from etl.viettel.common.configs import read_config_service
from etl.common.utils import log_io_file_path
import os

####################################################################################################
##### Extact feature from export_account_book/balance_transaction
####################################################################################################

def extract_agg_lxm_feature_lxm(extract_date, lxm, daily_feature_folder_name, phone_number_col):
    """
    
    """
    
    ################################################################################
    ## Prepare raw data for last x months
    ################################################################################
    
    # Read daily feature data
    raw_vas_daily_fst_df = spark.read.parquet(daily_feature_folder_name)
    
    # Calculate start date
    extract_date_dt = datetime.strptime(extract_date, "%Y-%m-%d").date()
    start_date_dt = extract_date_dt + dateutil.relativedelta.relativedelta(months=-1 * lxm)
    
    # Read data
    raw_vas_daily_fst_df = raw_vas_daily_fst_df.filter(
        (F.col("date") <= extract_date_dt) & (F.col("date") > start_date_dt)
    )
    
    # Drop date column
    raw_vas_daily_fst_df = raw_vas_daily_fst_df.drop("date")
    
    # Clean this data by replacing null by 0
    vas_fst_df_lxm = raw_vas_daily_fst_df.fillna(0)
    
    ################################################################################
    # Extract data
    ################################################################################
    lxm_str = "_l" + str(lxm) + "m"
    vas_monthly_fts_df = vas_fst_df_lxm.groupBy(phone_number_col) \
        .agg(
            # Number of vas transaction in last lxw month
            F.sum("vas_txn_num").alias("vas_txn_num" + lxm_str),\
            # Number of cash-out vas transactions in last lxw month
            F.sum("vas_txn_cash_out_num").alias("vas_txn_cash_out_num" + lxm_str),\
            # Number of cash-in vas transactions in last lxw month
            F.sum("vas_txn_cash_in_num").alias("vas_txn_cash_in_num" + lxm_str),\
            
            # Avg of numbers of vas transactions in last lxw month
            F.avg("vas_txn_num").alias("vas_txn_num_mavg" + lxm_str),\
            # Avg of numbers of cash-out vas transactions in last lxw month
            F.avg("vas_txn_cash_out_num").alias("vas_txn_cash_out_num_mavg" + lxm_str),\
            # Avg of numbers of cash-in vas transactions in last lxw month
            F.avg("vas_txn_cash_in_num").alias("vas_txn_cash_in_num_mavg" + lxm_str),\
            
            # Avg of amount of balance change in last lxw month
            F.sum("vas_balance_change_amt").alias("vas_bal_change_amt" + lxm_str),\
            # Avg of absoluted amount of balance change in last lxw month
            F.sum(F.abs(F.col("vas_balance_change_amt"))).alias("vas_bal_change_abs_amt" + lxm_str),\
            # Std of absoluted amount of balance change in last lxw month
            F.stddev(F.abs(F.col("vas_balance_change_amt"))).alias("vas_bal_change_abs_amt_daily_std" + lxm_str),\
            
            # Avg of amount of all cash-in transactions in last lxw month
            F.sum("vas_amt_cash_in_total").alias("vas_amt_cash_in_total" + lxm_str),\
            # Amount of all cash-out transactions in last lxw month
            F.sum("vas_amt_cash_out_total").alias("vas_amt_cash_out_total" + lxm_str),\
            
            # Avg of numbers of cash-advance transactions in last lxw month
            F.sum("vas_txn_cash_advance_num").alias("vas_txn_cash_advance_num" + lxm_str),\
            # Avg of amount of cash-advance transactions in last lxw month
            F.sum("vas_amt_cash_advance_total").alias("vas_amt_cash_advance_total" + lxm_str)
        )
        
    return vas_monthly_fts_df


def extract_agg_lxm_feature(extract_date, in_dir, out_dir, lxm_lst, phone_number_col):
    """
    
    """
    monthly_fst_df = None
    
    for lxm in lxm_lst:
        print("lxm ", lxm)
        monthly_fst_df = extract_agg_lxm_feature_lxm(extract_date, lxm, in_dir, phone_number_col)
        
        # Save to file
        monthly_fst_df.write.mode("overwrite").parquet(out_dir + "/c_agg_l" + str(lxm) + "m/date=" + extract_date)


# Start and end dates
extract_date_str = sys.argv[1]
extract_date = datetime.strptime(extract_date_str, '%Y-%m-%d')
if extract_date.weekday() != 6:
    print('Not Sunday, no need to run')
    exit(0)

# Initiate environment
config = read_config_service.get_compute_features_config()['balance_transactions']
weekly_config = config['weekly']
job_cfg = weekly_config['agg']['job_config']

spark = init_spark3.setup(job_cfg=job_cfg, script_name="Extract_vas_balance_transaction_lxm_feature")

### Whole population
in_dir = config['path']['daily']
out_dir = config['path']['agg']

# parameters
lxm_lst = [1, 2, 3]
extract_agg_lxm_feature(extract_date_str, in_dir, out_dir, lxm_lst, phone_number_col='phone_number')

# Stop spark
spark.stop()
