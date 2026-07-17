# Import important modules
import sys
from datetime import datetime, timedelta

import dateutil.relativedelta
import pyspark.sql.functions as F
from etl.common.utils import log_io_file_path
import os
from etl.common import init_spark3
from etl.vittel.common.configs import read_config_service

####################################################################################################
##### Extact feature from vas_charge
####################################################################################################

def vas_vas_charge_agg_lxm_feature_extraction(extract_date_str, lxm, in_dir, out_dir):
    """
    
    """
    
    # Calculate start date
    extract_date_dt = datetime.strptime(extract_date_str, "%Y-%m-%d").date()
    start_date_dt = extract_date_dt + dateutil.relativedelta.relativedelta(months=-1*lxm)
    
    # Read daily feature data
    daily_fst_df = spark.read.parquet(in_dir)
    daily_fst_df = daily_fst_df.filter((F.col("date") <= extract_date_dt) & (F.col("date") > start_date_dt))
    
    # Clean this data by replacing null by 0
    daily_fst_df = daily_fst_df.fillna(0)
    
    ################################################################################
    # Extract feature
    ################################################################################
    lxm_str = "_l" + str(lxm) + "m"
    monthly_feature_df = daily_fst_df\
        .groupBy("phone_number")\
        .agg(\
            F.sum("vas_charge_amt_total").alias("vas_charge_amt_total" + lxm_str),\
            F.sum("vas_charge_txn_num").alias("vas_charge_txn_num" + lxm_str),\
            F.sum("vas_charge_service_num").alias("vas_charge_service_num_davg_total" + lxm_str),\
            F.sum("vas_charge_amt_lottery_total").alias("vas_charge_amt_lottery_total" + lxm_str),\
            F.sum("vas_charge_amt_phongthuy_total").alias("vas_charge_amt_phongthuy_total" + lxm_str),\
            F.sum("vas_charge_amt_kid_total").alias("vas_charge_amt_kid_total" + lxm_str),\
            F.sum("vas_charge_amt_monthly_total").alias("vas_charge_amt_monthly_total" + lxm_str),\
            F.sum("vas_charge_amt_weekly_total").alias("vas_charge_amt_weekly_total" + lxm_str),\
            F.sum("vas_charge_amt_daily_total").alias("vas_charge_amt_daily_total" + lxm_str)
        )
        
    # Write to file
    monthly_feature_df.write.mode("overwrite").parquet(out_dir + "/date=" + extract_date_str)

# start and end dates
start_date_str = sys.argv[1]
end_date_str = datetime.strftime(datetime.strptime(start_date_str, "%Y-%m-%d") + timedelta(days=1), "%Y-%m-%d")

start_date_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date_dt = datetime.strptime(end_date_str, "%Y-%m-%d")

if start_date_dt.weekday() != 6:
    print("Not Sunday, no need to run")
    exit(0)

# Initiate environment
config = read_config_service.get_compute_features_config()['vas_charge']
weekly_config = config['weekly']
job_cfg = weekly_config['agg']['job_config']
script_name_str = "extract_vas_charge_agg_feature"
print("Starting script: ", script_name_str)
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name_str)

# Folders
in_dir = config['path']['daily']
out_dir = config['path']['agg']
lxm_lst = [1, 2, 3]

# Start extraction
extract_date_dt = start_date_dt
duration = (end_date_dt - start_date_dt).days

for i in range(duration):
    extract_date_dt = start_date_dt + timedelta(i)
    
    if extract_date_dt.weekday() == 6:
        # Get extract date in string format
        extract_date_str = datetime.strftime(extract_date_dt, "%Y-%m-%d")
        print(extract_date_str)
        
        # Extract daily feature
        try:
            for lxm in lxm_lst:
                vas_vas_charge_agg_lxm_feature_extraction(extract_date_str, lxm, in_dir, out_dir + "/agg_l" + str(lxm) + "m")
        except Exception as e:
            print(e)

print("Finished task")
# Stop spark
spark.stop()

try:
    _output = [os.path.join(out_dir, f'agg_l{lxm}m') for lxm in lxm_lst]
    log_io_file_path(input_paths=[in_dir],
                      output_paths=[*_output],
                      )
except Exception as e:
    print(f"Error log io: {e}")
