import sys
import pandas as pd, numpy as np, os, shutil
import pyspark.sql.types as T

from pathlib import Path
from etl.common import init_spark3
from pyspark.sql import functions as F
from pyspark.sql.window import window
from datetime import datetime, timedelta

BASE_DIR = Path("/apps/jupyter/users/nguyetnguyen/features")

sys.path[:0] = [str(path) for path in [
                    BASE_DIR / "common" / "src",
                    BASE_DIR / "device_refactored",
                ]
               ]

from common.agg_columns import (
    build_groupby_window_query,
    build_aggregate_columns,
    get_lxw_windows
)
from common.evaluate_new_features import NewFeatureEvalConfig, evaluate_features_in_bulk

spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 8,
        'executor.cores': 8,
        'executor.memory': '20g',
    },
    script_name= "build_holiday_features"
)

# -------------------------------------------------------------

import pandas as pd
import numpy as np
from datetime import timedelta

HOLIDAY_CSV = None
OUTPUT_PATH = None
TET_KEYWORDS = ["lunar new year"]


def load_holidays(csv_path=None):
    path = csv_path or HOLIDAY_CSV
    pdf = pd.read_csv(path)
    pdf = pdf[pdf["scope"] == "national"].copy()
    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf["end_date"] = pd.to_datetime(pdf["end_date"])
    pdf["is_tet"] = pdf["holiday"].str.lower().str.contains("|".join(TET_KEYWORDS))
    
    rows = []
    for _, r in pdf.iterrows():
        for d in pd.date_range(r["date"], r["end_date"]):
            rows.append({"date": d, "holiday": r["holiday"],
                         "is_national_holiday": 1, "is_tet": int(r["is_tet"])})
            
    result = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    result["year"] = result["date"].dt.year
    result["month"] = result["date"].dt.month
    result["week"] = result["date"].dt.isocalendar().week.astype(int)
    return result


def generate_holiday_features(holidays, start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq="D")
    cal = pd.DataFrame({"date": dates})
    cal["year"] = cal["date"].dt.year
    cal["month"] = cal["date"].dt.month
    cal["week"] = cal["date"].dt.isocalendar().week.astype(int)
    cal["dow"] = cal["date"].dt.dayofweek
    
    holiday_agg = holidays.groupby("date", as_index=False).agg(
        is_national_holiday=("is_national_holiday", "max"),
        is_tet=("is_tet", "max")
    )
    cal = cal.merge(holiday_agg, on="date", how="left").fillna({"is_national_holiday": 0, "is_tet": 0})
    
    cal["is_national_holiday"] = cal["is_national_holiday"].astype(int)
    cal["is_tet"] = cal["is_tet"].astype(int)
    
    hdays = holidays[holidays["is_national_holiday"] == 1]["date"].drop_duplicates().sort_values()
    tdays = holidays[holidays["is_tet"] == 1]["date"].drop_duplicates().sort_values()
    
    h_arr = hdays.dt.date.values.astype("datetime64[ns]")
    t_arr = tdays.dt.date.values.astype("datetime64[ns]")
    
    def nearest_forward(dates_arr, target):
        target = np.datetime64(target.date())
        idx = dates_arr.searchsorted(target)
        if idx < len(dates_arr):
            return (dates_arr[idx] - target).astype("timedelta64[D]").astype(int)
        return 0
        
    def nearest_backward(dates_arr, target):
        target = np.datetime64(target.date())
        idx = dates_arr.searchsorted(target, side="right") - 1
        if idx >= 0:
            return (target - dates_arr[idx]).astype("timedelta64[D]").astype(int)
        return 0
        
    cal["days_to_next_national_holiday"] = cal["date"].apply(lambda d: nearest_forward(h_arr, d))
    cal["days_since_last_national_holiday"] = cal["date"].apply(lambda d: nearest_backward(h_arr, d))
    cal["days_to_tet"] = cal["date"].apply(lambda d: nearest_forward(t_arr, d))
    cal["days_since_tet"] = cal["date"].apply(lambda d: nearest_backward(t_arr, d))
    
    holiday_dates = set(cal[cal["is_national_holiday"] == 1]["date"].dt.date)
    nat_holidays = holidays[holidays["is_national_holiday"] == 1][["date", "holiday"]].drop_duplicates()
    
    def count_day_in_future(target, days_ahead):
        end = target.date() + timedelta(days=days_ahead)
        target_d = target.date()
        return sum(1 for d in holiday_dates if target_d < d <= end)
        
    def count_day_in_past(target, days_back):
        start = target.date() - timedelta(days=days_back)
        target_d = target.date()
        return sum(1 for d in holiday_dates if start <= d < target_d)
        
    def count_event_in_future(target, days_ahead):
        target_d = target.date()
        end = target_d + timedelta(days=days_ahead)
        mask = nat_holidays["date"].apply(lambda d: target_d < d.date() <= end)
        return nat_holidays.loc[mask, "holiday"].nunique()
        
    def count_event_in_past(target, days_back):
        target_d = target.date()
        start = target_d - timedelta(days=days_back)
        mask = nat_holidays["date"].apply(lambda d: start <= d.date() < target_d)
        return nat_holidays.loc[mask, "holiday"].nunique()
        
    cal["holiday_day_next_7d"] = cal["date"].apply(lambda d: count_day_in_future(d, 7))
    cal["holiday_day_last_7d"] = cal["date"].apply(lambda d: count_day_in_past(d, 7))
    cal["holiday_event_next_7d"] = cal["date"].apply(lambda d: count_event_in_future(d, 7))
    cal["holiday_event_last_7d"] = cal["date"].apply(lambda d: count_event_in_past(d, 7))
    cal["holiday_day_next_30d"] = cal["date"].apply(lambda d: count_day_in_future(d, 30))
    cal["holiday_day_last_30d"] = cal["date"].apply(lambda d: count_day_in_past(d, 30))
    cal["holiday_event_next_30d"] = cal["date"].apply(lambda d: count_event_in_future(d, 30))
    cal["holiday_event_last_30d"] = cal["date"].apply(lambda d: count_event_in_past(d, 30))
    
    month_counts = cal[cal["is_national_holiday"] == 1].groupby(["year", "month"]).size().reset_index(name="holidays_this_month")
    cal = cal.merge(month_counts, on=["year", "month"], how="left")
    cal["holidays_this_month"] = cal["holidays_this_month"].fillna(0).astype(int)
    
    tet_counts = cal[cal["is_tet"] == 1].groupby(["year", "month"]).size().reset_index(name="tet_this_month")
    cal = cal.merge(tet_counts, on=["year", "month"], how="left")
    cal["tet_this_month"] = cal["tet_this_month"].fillna(0).astype(int)
    
    week_counts = cal[cal["is_national_holiday"] == 1].groupby(["year", "week"]).size().reset_index(name="holidays_this_week")
    cal = cal.merge(week_counts, on=["year", "week"], how="left")
    cal["holidays_this_week"] = cal["holidays_this_week"].fillna(0).astype(int)
    
    cal["is_holiday_week"] = (cal["holidays_this_week"] > 0).astype(int)
    cal["is_weekend"] = (cal["dow"] >= 5).astype(int)
    
    return cal


def run(csv_path=None, output_path=None, start_date="2020-01-01", end_date="2026-12-31", weekly=True):
    path = csv_path or HOLIDAY_CSV
    out = output_path or OUTPUT_PATH
    holidays = load_holidays(path)
    result = generate_holiday_features(holidays, start_date, end_date)
    
    if weekly:
        result = result[result["dow"] == 6].reset_index(drop=True)
        
    cols = ["date", "days_to_tet", "days_since_tet",
            "holiday_day_next_7d", "holiday_day_last_7d",
            "holiday_event_next_7d", "holiday_event_last_7d",
            "holiday_day_next_30d", "holiday_day_last_30d",
            "holiday_event_next_30d", "holiday_event_last_30d"]
    result = result[cols]
    
    if out:
        result.to_csv(out, index=False)
        print(f"Saved {len(result)} rows to {out}")
        
    freq = "weekly (Sunday)" if weekly else "daily"
    print(f"Holiday features: {len(result)} {freq} snapshots, {len(cols)} columns")
    print(f"  Range: {result['date'].min().date()} -> {result['date'].max().date()}")
    return result


path = '/apps/jupyter/users/nguyetnguyen/features/external_sources/data/vietnam_holidays_2020_2025.csv'
result = run(csv_path=path,
             output_path='',
             start_date='2020-01-01',
             end_date='2025-12-31')
df_result = spark.createDataFrame(result).withColumn('date', F.to_date('date'))
df_result.show()

df_result.write.mode('overwrite').partitionBy('date').parquet('/user/nguyetnguyen/features/external_sources/calendar_sources/holiday_features')

@F.udf(T.DateType())
def get_sunday(d):
    weekday = d.weekday()
    if weekday in [0, 1, 2]:
        return d - timedelta(weekday + 1 + 7)
    else:
        return d - timedelta(weekday + 1)

df_label = spark.read.parquet('/label/client=hc/source_code=20250417_hc_train_credit_score_81k').select('phone_number', 'disburse_date', 'label_value', 'product').dropDuplicates()
df_label.show(1)

df_fts = spark.read.parquet('/user/nguyetnguyen/features/external_sources/calendar_sources/holiday_features')
df_fts.show(1)

(
    df_label
    .join(
        df_label
        .select('phone_number', 'disburse_date')
        .withColumn('date', get_sunday(F.col('disburse_date')))
        .dropDuplicates()
        .join(df_fts, 'date')
        , ['phone_number', 'disburse_date'], 'left')
#   .write.mode('overwrite').parquet('/user/nguyetnguyen/features/hc_2025/external_source_holiday_merge_all')
)

cfg = NewFeatureEvalConfig.from_json(path='/apps/jupyter/users/nguyetnguyen/features/external_sources/configs/experiment_config_holiday.json')
cfg
df_result_holidays = evaluate_features_in_bulk(spark, cfg)

# Merge fts to build credit score model 
@F.udf(T.DateType())
def get_sunday(d):
    weekday = d.weekday()
    if weekday in [0, 1, 2]:
        return d - timedelta(weekday + 1 + 7)
    else:
        return d - timedelta(weekday + 1)

df_label = spark.read.parquet('/project/vnpt_cs_mafc_generic_v1/label/phone_date/hc_hdbank_kredivo_mafc_random_seabank_tnex_tpbank_tpdico_425k_pn_date')

fts_paths = [
    '/user/nguyetnguyen/features/external_sources/calendar_sources/holiday_features',
    '/user/nguyetnguyen/features/external_sources/macro_sources/btc_features',
    '/user/nguyetnguyen/features/external_sources/macro_sources/cpi_retail_sale_features',
    '/user/nguyetnguyen/features/external_sources/macro_sources/gold_features',
    '/user/nguyetnguyen/features/external_sources/macro_sources/oil_features',
    '/user/nguyetnguyen/features/external_sources/macro_sources/re_features',
    '/user/nguyetnguyen/features/external_sources/macro_sources/vnd_features'
]

df_fts = spark.read.parquet(fts_paths[0])
for path in fts_paths[1:]:
    fts = spark.read.parquet(path)
    df_fts = df_fts.join(fts, on='date', how='outer')

(
    df_label
    .join(
        df_label
        .select('phone_number', 'disburse_date')
        .withColumn('date', get_sunday(F.col('disburse_date')))
        .dropDuplicates()
        .join(df_fts, 'date')
        , ['phone_number', 'disburse_date'], 'left')
#   .write.mode('overwrite').parquet('/project/vnpt_cs_mafc_generic_v1/feature/join_feature/external_source_feature/hc_hdbank_kredivo_mafc_random_seabank_tnex_tpbank_tpdico_425k_pn_date')
)

spark.read.parquet('/project/vnpt_cs_mafc_generic_v1/feature/join_feature/external_source_feature/hc_hdbank_kredivo_mafc_random_seabank_tnex_tpbank_tpdico_425k_pn_date')\
.select(F.count('*').alias('count'),
        F.countDistinct('phone_number').alias('pn_date'),
        F.countDistinct('phone_number').alias('pn'),
       ).show()

len(spark.read.parquet('/project/vnpt_cs_mafc_generic_v1/feature/join_feature/external_source_feature/hc_hdbank_kredivo_mafc_random_seabank_tnex_tpbank_tpdico_425k_pn_date').columns)

df_label.select(F.count('*').alias('count'),
                F.countDistinct('phone_number').alias('pn_date'),
                F.countDistinct('phone_number').alias('pn'),
               ).show()
