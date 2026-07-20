import sys
from datetime import datetime, timedelta

import dateutil.relativedelta
import pyspark.sql.functions as F

from etl.common import init_spark3

###################################################################################################
# jupyter +2
def extract_vasc_monthly_feature(extract_date_str, in_dir, hour_dim_dir, out_dir, lxm):
    '''
    '''
    # Read hour dimension map
    hour_dim_df = spark.read.parquet(hour_dim_dir).drop("date")
    
    # Read raw data
    extract_date_dt = datetime.strptime(extract_date_str, "%Y-%m-%d").date()
    start_date_dt = extract_date_dt - dateutil.relativedelta.relativedelta(months=lxm)
    start_date_dt = start_date_dt - timedelta(1)
    start_date_str = datetime.strftime(start_date_dt, '%Y-%m-%d')
    
    raw_df = spark.read.format("delta").load(in_dir) \
        .where("date between '{}' and '{}'".format(start_date_str, extract_date_str))
        
    # Drop duplicated rows
    clean_df = raw_df.dropDuplicates()
    clean_df = clean_df.withColumn("service_name", F.upper(F.col("service_name")))
    
    # Add time dimension
    transform_df = clean_df.withColumn("hour", F.hour("transaction_time"))
    transform_df = transform_df.join(hour_dim_df, on="hour", how="left")
    
    # Add day dimension
    transform_df = transform_df.withColumn("day_of_week", F.dayofweek("date")) \
        .withColumn("is_weekend",
            F.expr("""
                case
                    when day_of_week >=2 and day_of_week <= 6 then 0
                    else 1
                end
            """))
            
    # Special dimensions
    transform_df = transform_df.withColumn("is_le5k_service",
        F.expr("""
            case
                when amount <= 5000.0 then 1
                else 0
            end
        """))
        
    transform_df = transform_df.withColumn("is_le10k_service",
        F.expr("""
            case
                when amount <= 10000.0 then 1
                else 0
            end
        """))
        
    transform_df = transform_df.withColumn("is_le17k_service",
        F.expr("""
            case
                when amount <= 17000.0 then 1
                else 0
            end
        """))
        
    transform_df = transform_df.withColumn("is_le27k_service",
        F.expr("""
            case
                when amount <= 27000.0 then 1
                else 0
            end
        """))
        
    transform_df = transform_df.withColumn("is_le50k_service",
        F.expr("""
            case
                when amount <= 5000.0 then 1
                else 0
            end
        """))
        
    transform_df = transform_df.withColumn("is_ge100k_service",
        F.expr("""
            case
                when amount >= 100000.0 then 1
                else 0
            end
        """))
        
    # Extract features
    lxm_str = "_l" + str(lxm) + "m"
    daily_fts_df1 = transform_df.groupBy("phone_number").agg( \
    
        ###########################################################################################
        # fact: number of transactions
        F.count("*").alias("vasc_txn_num" + lxm_str),
        
        # dim: daytime/nighttime
        F.count(F.when(F.col("hour_range_level2") == 'daytime',
            True)).alias("vasc_txn_daytime_num" + lxm_str),
            
        # dim: weekend/weekday
        F.count(F.when(F.col("is_weekend") == 1,
            True)).alias("vasc_txn_weekend_num" + lxm_str),
            
        # dim: type of services
        F.count(F.when(F.col("service_name") == 'UTN',
            True)).alias("vasc_txn_utn_num" + lxm_str),
        F.count(F.when(F.col("service_name") == '2FRIEND',
            True)).alias("vasc_txn_2friend_num" + lxm_str),
        F.count(F.when(F.col("service_name") == 'UDV',
            True)).alias("vasc_txn_udv_num" + lxm_str),
            
        # dim: range of amount
        F.count(F.when(F.col("is_le5k_service") == 1,
            True)).alias("vasc_txn_le5k_service_num" + lxm_str),
        F.count(F.when(F.col("is_le10k_service") == 1,
            True)).alias("vasc_txn_le10k_service_num" + lxm_str),
        F.count(F.when(F.col("is_le17k_service") == 1,
            True)).alias("vasc_txn_le17k_service_num" + lxm_str),
        F.count(F.when(F.col("is_le27k_service") == 1,
            True)).alias("vasc_txn_le27k_service_num" + lxm_str),
        F.count(F.when(F.col("is_le50k_service") == 1,
            True)).alias("vasc_txn_le50k_service_num" + lxm_str),
        F.count(F.when(F.col("is_ge100k_service") == 1,
            True)).alias("vasc_txn_ge100k_service_num" + lxm_str),
            
        ###########################################################################################
        # fact: amount
        F.sum("amount").alias("vasc_amt_total" + lxm_str),
        
        # dim: daytime/nighttime
        F.sum(F.when(F.col("hour_range_level2") == 'daytime',
            F.col("amount"))).alias("vasc_amt_daytime_total" + lxm_str),
        # dim: weekend/weekday
        F.sum(F.when(F.col("is_weekend") == 1,
            F.col("amount"))).alias("vasc_amt_weekend_total" + lxm_str),
            
        # dim: types of services
        F.sum(F.when(F.col("service_name") == 'UTN',
            F.col("amount"))).alias("vasc_amt_utn_total" + lxm_str),
        F.sum(F.when(F.col("service_name") == '2FRIEND',
            F.col("amount"))).alias("vasc_amt_2friend_total" + lxm_str),
        F.sum(F.when(F.col("service_name") == 'UDV',
            F.col("amount"))).alias("vasc_amt_udv_total" + lxm_str),
            
        # dim: range of amounts
        F.sum(F.when(F.col("is_le5k_service") == 1,
            F.col("amount"))).alias("vasc_amt_le5k_service_total" + lxm_str),
        F.sum(F.when(F.col("is_le10k_service") == 1,
            F.col("amount"))).alias("vasc_amt_le10k_service_total" + lxm_str),
        F.sum(F.when(F.col("is_le17k_service") == 1,
            F.col("amount"))).alias("vasc_amt_le17k_service_total" + lxm_str),
        F.sum(F.when(F.col("is_le27k_service") == 1,
            F.col("amount"))).alias("vasc_amt_le27k_service_total" + lxm_str),
        F.sum(F.when(F.col("is_le50k_service") == 1,
            F.col("amount"))).alias("vasc_amt_le50k_service_total" + lxm_str),
        F.sum(F.when(F.col("is_ge100k_service") == 1,
            F.col("amount"))).alias("vasc_amt_ge100k_service_total" + lxm_str),
            
        ###########################################################################################
        # Others
        F.countDistinct("service_name").alias("vasc_service_num" + lxm_str),
        F.countDistinct(F.when(F.col("service_name") == '2FRIEND',
            F.col('receive_phone_number'))).alias("vasc_receiver2f_num" + lxm_str),
        F.countDistinct("date").alias("vasc_active_day_num" + lxm_str),
        F.countDistinct(F.when(F.col("service_name") == 'UTN',
            F.col("date"))).alias("vasc_utn_active_day_num" + lxm_str),
        F.countDistinct(F.when(F.col("service_name") == 'UDV',
            F.col("date"))).alias("vasc_udv_active_day_num" + lxm_str),
        F.countDistinct(F.when(F.col("service_name") == '2FRIEND',
            F.col("date"))).alias("vasc_2friend_active_day_num" + lxm_str)
    )

    ###############################################################################################
    daily_fts_df2 = transform_df.where("service_name == '2FRIEND' ") \
        .groupBy("receive_phone_number").agg( \
        
        F.count("*").alias("vasc_txn_receivce2f_num" + lxm_str),
        F.countDistinct("phone_number").alias("vasc_sender2f_num" + lxm_str),
        F.sum("amount").alias("vasc_amt_receiver2f_total" + lxm_str),
        
        F.count(F.when(F.col("hour_range_level2") == 'daytime',
            True)).alias("vasc_txn_receivce2f_daytime_num" + lxm_str),
        F.countDistinct(F.when(F.col("hour_range_level2") == 'daytime',
            F.col("phone_number"))).alias("vasc_receivcer2f_daytime" + lxm_str),
        F.sum(F.when(F.col("hour_range_level2") == 'daytime',
            F.col("amount"))).alias("vasc_amt_receivce2f_daytime_total" + lxm_str),
            
        F.count(F.when(F.col("is_weekend") == 1,
            True)).alias("vasc_txn_receivce2f_weekend_num" + lxm_str),
        F.countDistinct(F.when(F.col("is_weekend") == 1,
            F.col("phone_number"))).alias("vasc_receivcer2f_weekend_num" + lxm_str),
        F.sum(F.when(F.col("is_weekend") == 1,
            F.col("amount"))).alias("vasc_amt_receivce2f_weekend_total" + lxm_str),
        F.countDistinct("date").alias("vasc_receivce2f_active_day_num" + lxm_str)
    )

    daily_fts_df = daily_fts_df1.join(daily_fts_df2,
                                     daily_fts_df1.phone_number == daily_fts_df2.receive_phone_number,
                                     how='outer')
                                     
    daily_fts_df = daily_fts_df.withColumn("phone_number",
        F.expr("""
            case
                when phone_number is null then receive_phone_number
                else phone_number
            end
        """))
        
    # Adding more features
    daily_fts_df = daily_fts_df \
        .withColumn("vasc_txn_nighttime_num" + lxm_str,
            F.col("vasc_txn_num" + lxm_str) - F.col("vasc_txn_daytime_num" + lxm_str)) \
        .withColumn("vasc_txn_weekday_num" + lxm_str,
            F.col("vasc_txn_num" + lxm_str) - F.col("vasc_txn_weekend_num" + lxm_str)) \
        .withColumn("vasc_amt_nighttime_total" + lxm_str,
            F.col("vasc_amt_total" + lxm_str) - F.col("vasc_amt_daytime_total" + lxm_str)) \
        .withColumn("vasc_amt_weekday_total" + lxm_str,
            F.col("vasc_amt_total" + lxm_str) - F.col("vasc_amt_weekend_total" + lxm_str))
            
    # write to file
    daily_fts_df.write.mode("overwrite").parquet(out_dir + "/agg" + lxm_str + "/date=" + extract_date_str)
    
    print("Finished task")


###################################################################################################
# Folders
in_dir = "/data/vnpt_v2/vasc"
out_dir = "/feature/lxm/vasc"
hour_dim_dir = "/data/project/modelx/map_data/hour_dimension/"

# Extract daily feature
extract_date_str = sys.argv[1]
extract_date = datetime.strptime(extract_date_str, '%Y-%m-%d')
if extract_date.weekday() != 6:
    print('Not Sunday, no need to run')
    exit(0)

# Initiate environment
spark = init_spark3.setup(job_cfg={"executor.instances": 2,
                                   "executor.cores": 8,
                                   "executor.memory": '2g'},
                         script_name="trung_vas_monthly_feature_" + extract_date_str)

print("extract date :", extract_date_str)
lxm_lst = [1, 2, 3]

for lxm in lxm_lst:
    extract_vasc_monthly_feature(extract_date_str, in_dir, hour_dim_dir, out_dir, lxm)

print("Finished task")
# Stop spark
spark.stop()

# Log data lineage #############################################
from etl.common.data_lineage import data_lineage
dl = data_lineage.DataLineage()
dl.log_io(input_paths=[in_dir, hour_dim_dir], output_paths=[out_dir], script=__file__)
