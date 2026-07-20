import os
import sys

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from etl.common import init_spark3


def is_keeped(column, suffixes):
    return any(
        [column.endswith(suffix) for suffix in suffixes]
    )


def is_dropped(column, phrases):
    return any([
        phrase in column for phrase in phrases
    ])

last_activate_dir="/data/processed/credit_score_v1.2/features/sub/last_activated_date/weekly"
usage_data_path="/data/processed/credit_score_v1.2/features/usage/usage_by_phone/weekly/"


def get_usage_features(
    spark,
    date_from,
    date_to,
    suffix,
    usage_data_path=usage_data_path,
    last_activate_dir=last_activate_dir
):
    lad_file = os.path.join(last_activate_dir, "date={}".format(date_to))
    lad_df = spark.read.parquet(lad_file).select('phone_number', 'last_activated_date')
    
    usage_df = (
        spark.read.parquet(usage_data_path)
        .where('date > "{}" and date <= "{}"'.format(date_from, date_to))
        .join(lad_df, 'phone_number', 'inner')
        .filter("date >= last_activated_date")
        .drop("date")
        .dropDuplicates()
    )
    
    columns = usage_df.columns
    to_be_keeped_suffixes = ['_charge_amount', '_num', '_num_paid']
    to_be_dropped_phrases = ['pct', 'avg']
    to_be_aggregated_columns = [
        column for column in columns if (
            is_keeped(column, to_be_keeped_suffixes) and not is_dropped(column, to_be_dropped_phrases)
        )
    ]
    
    aggregations = [
        F.sum(column).alias('usage_{}_sum_{}'.format(column, suffix)) for column in to_be_aggregated_columns
    ]
    
    aggregations.extend([
        F.min(column).alias('usage_{}_min_{}'.format(column, suffix)) for column in to_be_aggregated_columns
    ])
    
    aggregations.extend([
        F.stddev(column).alias('usage_{}_std_{}'.format(column, suffix)) for column in to_be_aggregated_columns
    ])
    
    return usage_df.groupBy('phone_number').agg(*aggregations)


def get_spark_instance(current_date_str):
    spark = init_spark3.setup(
        job_cfg = {
            "executor.instances": 6,
            "executor.cores": 4,
            "executor.memory": '12g'
        },
        script_name='dat_usage_features_{}'.format(current_date_str)
    )
    
    return spark


def main():
    current_date_str = sys.argv[1]
    current_date = datetime.strptime(current_date_str, '%Y-%m-%d')
    weekday = current_date.weekday()
    this_sunday = current_date - timedelta(weekday-6)
    this_sunday_str = this_sunday.strftime('%Y-%m-%d')
    spark = get_spark_instance(this_sunday_str)
    
    date_from = this_sunday - timedelta(days=(7 * 24)) # 24 weeks
    date_from_str = date_from.strftime('%Y-%m-%d')
    
    df = spark.read.parquet("/data/processed/credit_score_v1.2/features/usage/usage_by_phone/weekly/")\
    .where('date > "{}" and date <= "{}"'.format(date_from_str, this_sunday_str ))
    
    if df.select("date").distinct().count() != 24:
        print("!!! not enough date data")
        print(df.select("date").distinct().orderBy("date").show(100, False))
        exit(1)
        
    usage_df = get_usage_features(spark, date_from_str, this_sunday_str, 'l24w')
    usage_df.orderBy('phone_number').repartition(10).write.mode('overwrite').parquet(
        '/feature/inventory/usage/24w/date={}'.format(this_sunday_str)
    )
