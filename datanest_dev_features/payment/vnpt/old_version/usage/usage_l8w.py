import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import functions as F

if len(sys.argv) != 2:
    print('need processing date string: python {} YYYY-mm-dd'.format(sys.argv[0]), file=sys.stderr)
    exit(1)

curdate_str = sys.argv[1]
curdate = datetime.strptime(curdate_str, "%Y-%m-%d")
weekday = curdate.weekday()
this_sunday = curdate - timedelta(weekday - 6)

# if not check_multi_date_count('usage', this_sunday, 60, DAILY_COUNT_THRESHOLD=0.8):
#     exit(1)

spark = setup(job_cfg=medium_config(), script_name=get_script_name(sys.argv[0]))

in_dir = "/data/vnpt_v2/usage"
usage_weekly_dir = "/data/processed/credit_score_v1.2/features/usage/usage_by_phone/weekly"
out_dir = "/data/processed/credit_score_v1.2/features/usage/agg/8weeks"
lad_dir = "/data/processed/credit_score_v1.2/features/sub/last_activated_date/weekly"


def compute_usage_weekly(agg_date, in_dir, out_dir, lad_dir):
    agg_week = int(agg_date.isocalendar()[1])
    out_file = os.path.join(out_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    lad_file = os.path.join(lad_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    start_date = agg_date - timedelta(7)
    end_date = agg_date + timedelta(0)
    print(start_date, end_date, out_file, lad_file)
    
    lad_df = spark.read.parquet(lad_file)
    df = spark.read.format('delta').load(in_dir) \
        .select("phone_number", "service_type", "charge_amount", "start_time", "date") \
        .filter((F.col("date") > start_date) & (F.col("date") <= end_date))
    
    if df.select("date").distinct().count() != 7:
        print("!!! not enough date data")
        print(df.select("date").distinct().orderBy("date").show(100, False))
        exit(1)
        
    df = df \
        .withColumn("week_of_year", F.weekofyear("start_time")) \
        .filter("week_of_year == {}".format(agg_week)) \
        .join(lad_df, "phone_number") \
        .filter("date >= last_activated_date") \
        .dropDuplicates()
    
    df.cache()
    df.count()
    
    sms_df = df.filter("service_type = 'SMS'") \
        .withColumn("free", F.when(F.col("charge_amount") == 0, 1).otherwise(0)) \
        .withColumn("paid", F.when(F.col("charge_amount") != 0, 1).otherwise(0)) \
        .groupBy("phone_number").agg(
            F.sum("charge_amount").alias("sms_charge_amount"),
            F.count("charge_amount").alias("sms_num"),
            F.sum("free").alias("sms_num_free"),
            F.sum("paid").alias("sms_num_paid"),
        ).withColumn("sms_pct_num_free", F.col("sms_num_free") / F.col("sms_num")) \
        .withColumn("sms_pct_num_paid", F.col("sms_num_paid") / F.col("sms_num")) \
        .withColumn("sms_avg_charge_amount", F.col("sms_charge_amount") / (F.col("sms_num_paid") + 0.01))
        
    sms_df.cache()
    sms_df.count()
    
    voice_df = df.filter("service_type = 'VOICE'") \
        .withColumn("free", F.when(F.col("charge_amount") == 0, 1).otherwise(0)) \
        .withColumn("paid", F.when(F.col("charge_amount") != 0, 1).otherwise(0)) \
        .groupBy("phone_number").agg(
            F.sum("charge_amount").alias("voice_charge_amount"),
            F.count("charge_amount").alias("voice_num"),
            F.sum("free").alias("voice_num_free"),
            F.sum("paid").alias("voice_num_paid"),
        ).withColumn("voice_pct_num_free", F.col("voice_num_free") / F.col("voice_num")) \
        .withColumn("voice_pct_num_paid", F.col("voice_num_paid") / F.col("voice_num")) \
        .withColumn("voice_avg_charge_amount", F.col("voice_charge_amount") / (F.col("voice_num_paid") + 0.01))
        
    voice_df.cache()
    voice_df.count()
    
    data_df = df.filter("service_type = 'DATA'") \
        .withColumn("free", F.when(F.col("charge_amount") == 0, 1).otherwise(0)) \
        .withColumn("paid", F.when(F.col("charge_amount") != 0, 1).otherwise(0)) \
        .groupBy("phone_number").agg(
            F.sum("charge_amount").alias("data_charge_amount"),
            F.count("charge_amount").alias("data_num"),
            F.sum("free").alias("data_num_free"),
            F.sum("paid").alias("data_num_paid"),
        ).withColumn("data_pct_num_free", F.col("data_num_free") / F.col("data_num")) \
        .withColumn("data_pct_num_paid", F.col("data_num_paid") / F.col("data_num")) \
        .withColumn("data_avg_charge_amount", F.col("data_charge_amount") / (F.col("data_num_paid") + 0.01))
        
    data_df.cache()
    data_df.count()
    
    other_df = df.filter("service_type = 'OTHER'") \
        .withColumn("free", F.when(F.col("charge_amount") == 0, 1).otherwise(0)) \
        .withColumn("paid", F.when(F.col("charge_amount") != 0, 1).otherwise(0)) \
        .groupBy("phone_number").agg(
            F.sum("charge_amount").alias("other_charge_amount"),
            F.count("charge_amount").alias("other_num"),
            F.sum("free").alias("other_num_free"),
            F.sum("paid").alias("other_num_paid"),
        ).withColumn("other_pct_num_free", F.col("other_num_free") / F.col("other_num")) \
        .withColumn("other_pct_num_paid", F.col("other_num_paid") / F.col("other_num")) \
        .withColumn("other_avg_charge_amount", F.col("other_charge_amount") / (F.col("other_num_paid") + 0.01))
        
    other_df.cache()
    other_df.count()
    
    df = sms_df.join(voice_df, "phone_number", "outer") \
        .join(data_df, "phone_number", "outer") \
        .join(other_df, "phone_number", "outer").fillna(0)
        
    df = df.withColumn("all_charge_amount",
                       F.col("sms_charge_amount") + F.col("voice_charge_amount") + F.col("data_charge_amount") + F.col("other_charge_amount")) \
        .withColumn("all_num",
                    F.col("sms_num") + F.col("voice_num") + F.col("data_num") + F.col("other_num")) \
        .withColumn("all_num_free",
                    F.col("sms_num_free") + F.col("voice_num_free") + F.col("data_num_free") + F.col("other_num_free")) \
        .withColumn("all_num_paid",
                    F.col("sms_num_paid") + F.col("voice_num_paid") + F.col("data_num_paid") + F.col("other_num_paid")) \
        .withColumn("all_pct_num_free", F.col("all_num_free") / F.col("all_num")) \
        .withColumn("all_pct_num_paid", F.col("all_num_paid") / F.col("all_num")) \
        .withColumn("all_avg_charge_amount", F.col("all_charge_amount") / (F.col("all_num") + 0.01))
        
    df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save(out_file)
        
    df.unpersist()
    sms_df.unpersist()
    voice_df.unpersist()
    data_df.unpersist()
    other_df.unpersist()


def compute_period_features(agg_date, in_dir, out_dir, lad_dir, backward_days=56):
    start_date = (agg_date - timedelta(days=backward_days - 1))
    out_file = os.path.join(out_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    lad_file = os.path.join(lad_dir, "date={}".format(agg_date.strftime("%Y-%m-%d")))
    print(start_date, agg_date, out_file, lad_file)
    
    lad_df = spark.read.parquet(lad_file)
    df = spark.read.parquet(in_dir) \
        .filter((F.col("date") >= start_date) & (F.col("date") <= agg_date)) \
        .join(lad_df, "phone_number") \
        .filter("date >= last_activated_date")
        
    df.cache()
    df.count()
    
    df = df.groupBy("phone_number") \
        .agg(
            F.stddev("all_charge_amount").alias("usage_sd_week_expense_8weeks"),
            F.mean("all_charge_amount").alias("usage_avg_weeks_expense"),
            F.sum("all_charge_amount").alias("usage_sum_weeks_expense"),
            F.stddev("sms_charge_amount").alias("usage_sms_sd_week_expense_8weeks"),
            F.mean("sms_charge_amount").alias("usage_sms_avg_weeks_expense"),
            F.sum("sms_charge_amount").alias("usage_sms_sum_weeks_expense"),
            F.stddev("voice_charge_amount").alias("usage_voice_sd_week_expense_8weeks"),
            F.mean("voice_charge_amount").alias("usage_voice_avg_weeks_expense"),
            F.sum("voice_charge_amount").alias("usage_voice_sum_weeks_expense"),
            F.stddev("data_charge_amount").alias("usage_data_sd_week_expense_8weeks"),
            F.mean("data_charge_amount").alias("usage_data_avg_weeks_expense"),
            F.sum("data_charge_amount").alias("usage_data_sum_weeks_expense"),
            F.stddev("other_charge_amount").alias("usage_other_sd_week_expense_8weeks"),
            F.mean("other_charge_amount").alias("usage_other_avg_weeks_expense"),
            F.sum("other_charge_amount").alias("usage_other_sum_weeks_expense"),
            F.sum("all_num").alias("usage_all_num"),
            F.sum("voice_num").alias("usage_voice_num"),
            F.sum("sms_num").alias("usage_sms_num"),
            F.sum("data_num").alias("usage_data_num"),
            F.sum("other_num").alias("usage_other_num"),
            F.sum("all_num_free").alias("usage_all_num_free"),
            F.sum("voice_num_free").alias("usage_voice_num_free"),
            F.sum("data_num_free").alias("usage_data_num_free"),
            F.sum("other_num_free").alias("usage_other_num_free"),
            F.sum("all_num_paid").alias("usage_all_num_paid"),
            F.sum("voice_num_paid").alias("usage_voice_num_paid"),
            F.sum("sms_num_paid").alias("usage_sms_num_paid"),
            F.sum("data_num_paid").alias("usage_data_num_paid"),
            F.sum("other_num_paid").alias("usage_other_num_paid"),
            F.avg("all_pct_num_free").alias("usage_all_pct_num_free"),
            F.avg("voice_pct_num_free").alias("usage_voice_pct_num_free"),
            F.avg("sms_pct_num_free").alias("usage_sms_pct_num_free"),
            F.avg("data_pct_num_free").alias("usage_data_pct_num_free"),
            F.avg("other_pct_num_free").alias("usage_other_pct_num_free"),
            F.avg("all_pct_num_paid").alias("usage_all_pct_num_paid"),
            F.avg("voice_pct_num_paid").alias("usage_voice_pct_num_paid"),
            F.avg("sms_pct_num_paid").alias("usage_sms_pct_num_paid"),
            F.avg("data_pct_num_paid").alias("usage_data_pct_num_paid"),
            F.avg("other_pct_num_paid").alias("usage_other_pct_num_paid"),
            F.avg("all_avg_charge_amount").alias("usage_all_avg_charge_amount"),
            F.avg("voice_avg_charge_amount").alias("usage_voice_avg_charge_amount"),
            F.avg("sms_avg_charge_amount").alias("usage_sms_avg_charge_amount"),
            F.avg("data_avg_charge_amount").alias("usage_data_avg_charge_amount"),
            F.avg("other_avg_charge_amount").alias("usage_other_avg_charge_amount"),
            F.avg("sms_pct_num_free").alias("usage_sms_pct_num_free"),
            F.avg("data_pct_num_free").alias("usage_data_pct_num_free"),
            F.avg("other_pct_num_free").alias("usage_other_pct_num_free"),
            F.avg("all_pct_num_paid").alias("usage_all_pct_num_paid"),
            F.avg("voice_pct_num_paid").alias("usage_voice_pct_num_paid"),
            F.avg("sms_pct_num_paid").alias("usage_sms_pct_num_paid"),
            F.avg("data_pct_num_paid").alias("usage_data_pct_num_paid"),
            F.avg("other_pct_num_paid").alias("usage_other_pct_num_paid"),
            F.avg("all_avg_charge_amount").alias("usage_all_avg_charge_amount"),
            F.avg("voice_avg_charge_amount").alias("usage_voice_avg_charge_amount"),
            F.avg("sms_avg_charge_amount").alias("usage_sms_avg_charge_amount"),
            F.avg("data_avg_charge_amount").alias("usage_data_avg_charge_amount"),
            F.avg("other_avg_charge_amount").alias("usage_other_avg_charge_amount"),
        ).withColumn("usage_sd_week_expense_8weeks_ge_25k",
                     (F.col("usage_sd_week_expense_8weeks") >= 20000).cast('integer')) \
        .withColumn("usage_sd_week_expense_8weeks_le_2500",
                    (F.col("usage_sd_week_expense_8weeks") < 2000).cast('integer')).fillna(0)

    df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save(out_file)
