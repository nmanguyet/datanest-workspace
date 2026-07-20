import sys

import pyspark.sql.functions as F

from etl.common import init_spark3
from datetime import datetime
from etl.viettel.common.configs import read_config_service
from etl.common.utils import check_daily_count
from etl.common.utils import log_io_file_path
# matplotlib inline

# Initiate environment
from etl.viettel.common.utils import check_enough_data

config = read_config_service.get_compute_features_config()['balance_transactions']
daily_config = config['daily']
job_cfg = daily_config['job_config']
script_name = "balance_transactions daily feature extract"
spark = init_spark3.setup(job_cfg=job_cfg, script_name=script_name)


def vas_daily_feature_extraction(extract_date_str, raw_vas_folder, feature_folder_name):
    """
    
    """
    
    print("Read data")
    # Column name
    amount_col = 'amount'
    service_col = 'reason'
    phone_number_col = 'phone_number'
    
    #### Read raw data
    # raw_vas_df = spark.read.parquet(raw_vas_folder + "/date=" + extract_date_str)
    # raw_vas_df.dropDuplicates()
    print("raw_vas_folder:", raw_vas_folder)
    print("extract_date:", extract_date_str)
    raw_vas_df = spark.read.format('delta').load(raw_vas_folder).where(f"date = '{extract_date_str}' ")
    check_enough_data(raw_vas_df, data_point=1)
    
    # Add dimension is_cash_in
    print("add dimension is_cash_in")
    transform_vas_df = raw_vas_df.withColumn("is_cash_in",
                                             F.expr(f"""
                                                case 
                                                    when ({amount_col} > 0) then 0
                                                    when ({amount_col} < 0) then 1
                                                    when ({amount_col} == 0) then -1
                                                end
                                             """))
                                             
    # Add dimension is_cash_advance
    print("add dimension is_cash_advance")
    transform_vas_df = transform_vas_df.withColumn("is_cash_advance",
                                                   F.expr(f"""
                                                      case
                                                          when ({service_col} like '%PROVISIONING_LOAN%' or 
                                                                {service_col} like '%AIRTIME_UNG%') then 1
                                                          else 0
                                                      end
                                                   """))
                                                   
    # Extract features
    print("Extract features")
    vas_daily_fts_df = transform_vas_df.groupBy(phone_number_col) \
        .agg(
            # GROUP: NUMBER OF VAS transactions
            # Number of vas transaction in last day
            F.count("*").alias("vas_txn_num"),
            # Number of cash-out vas transaction in last day
            F.count(F.when(F.col("is_cash_in") == 0, True)).alias("vas_txn_cash_out_num"),
            # Number of cash-in vas transaction in last day
            F.count(F.when(F.col("is_cash_in") == 1, True)).alias("vas_txn_cash_in_num"),
            # GROUP: Transaction amount
            # Amount of balance change
            F.sum(amount_col).alias("vas_balance_change_amt"),
            # Amount of all cash-in transactions
            F.sum(F.when(F.col("is_cash_in") == 1, F.col(amount_col))).alias("vas_amt_cash_in_total"),
            # Amount of all cash-out transactions
            F.sum(F.when(F.col("is_cash_in") == 0, F.col(amount_col))).alias("vas_amt_cash_out_total"),
            # GROUP: Transaction related to cash advace service
            # Number of cash-advance transactions
            F.count(F.when(F.col("is_cash_advance") == 1, True)).alias("vas_txn_cash_advance_num"),
            # Amount of cash-advance transactions
            F.sum(F.when(F.col("is_cash_advance") == 1, F.col(amount_col))).alias("vas_amt_cash_advance_total")
        )
        
    # Add column date
    vas_daily_fts_df = vas_daily_fts_df.withColumn("date", F.to_date(F.lit(extract_date_str), 'yyyy-MM-dd'))
    print("Start writing")
    # Write to file
    vas_daily_fts_df.write.mode("overwrite").parquet(feature_folder_name + "/date=" + extract_date_str)


##################################################
# Extraction date
extract_date_str = sys.argv[1]
print("Extract date :", extract_date_str)

if not check_daily_count(data_source='balance_transactions', datetime=datetime.strptime(extract_date_str, '%Y-%m-%d'), DAILY_COUNT_THRESHOLD=0.2):
    exit(1)
    
# Folders
raw_vas_folder = config['path']['select_data_path']
feature_folder_name = config['path']['daily']
# feature_folder_name = "hdfs://cicdataha/data/project/modelx/feature/daily/vas/balance_transactions"

# Extract daily feature
vas_daily_feature_extraction(extract_date_str, raw_vas_folder, feature_folder_name)
