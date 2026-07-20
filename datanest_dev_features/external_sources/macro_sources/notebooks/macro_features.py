import sys
import pandas as pd, numpy as np, os, shutil
import pyspark.sql.types as T

from pathlib import Path
from etl.common import init_spark3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
    script_name= "adhoc"
)

# -------------------------------------------------------------

DATA_DIR = '/apps/jupyter/users/nguyetnguyen/features/external_sources/data'
RAW_DIR = '/user/nguyetnguyen/features/external_sources/data'
FEAT_DIR = '/user/nguyetnguyen/features/external_sources/macro_sources'
WINDOWS = [1, 4, 24]
PART_COL = 'date'

TRADING_DAYS = {
    "btc": 365, "gold": 252, "oil": 252, "eur": 252, "vnd": 252,
    "sp100": 252, "dji": 252, "nasdaq": 252, "re": 252, "vn30": 252,
}

SHOCK_THRESHOLD = 0.05
EXTREME_LOSS_THRESHOLD = -0.03
VAR_ALPHA = 0.05
CROSS_PAIRS = [(1, 4), (4, 24)]
CIRCLE_PERIODS = [4, 12, 52]

COL_MAP = {"Date": "date", "Price": "close", "Open": "open",
           "High": "high", "Low": "low", "Vol.": "volume", "Change %": "change_pct"}
DATE_FMT = "%d/%m/%Y"

ASSETS = [
    {"ticker": "btc",  "csv": "btc_usd_raw_202001_202512.csv"},
    {"ticker": "gold", "csv": "gold_usd_raw_202001_202512.csv"},
    {"ticker": "oil",  "csv": "oil_usd_raw_202001_202512.csv"},
    {"ticker": "vnd",  "csv": "vnd_usd_raw_202001_202512.csv"},
    {"ticker": "re",   "csv": "real_estate_usd_raw_202001_202512.csv"},
]

def parse_number(val):
    if isinstance(val, (int, float)): return float(val)
    return float(str(val).replace(",", ""))

# -------------------------------------------------------------

def parse_volume(val):
    if pd.isna(val): return 0.0
    val = str(val).replace(",", "").strip()
    if not val: return 0.0
    for suffix, mult in [('K', 1_000), ('M', 1_000_000), ('B', 1_000_000_000)]:
        if suffix in val:
            return float(val.replace(suffix, '')) * mult
    return float(val)

def list_partitions(path):
    if not os.path.exists(path): return set()
    return {n.split('=', 1)[1] for n in os.listdir(path) if n.startswith(f"{PART_COL}=")}

def win_suffix(w):
    return f"{w}w"

def load_raw(spark, store):
    df = spark.read.parquet(store)
    if not df.head(1): return df
    bounds = df.agg(F.min("date"), F.max("date")).collect()[0]
    min_date, max_date = bounds[0], bounds[1]
    calendar = spark.sql(f"""
        SELECT explode(sequence(date'{min_date}', date'{max_date}', interval 1 day)) AS date
    """)
    result = calendar.join(df, "date", "left")
    w = Window.orderBy("date")
    result = result.withColumn("daily_return",
        (F.col("close") - F.lag("close", 1).over(w)) / F.lag("close", 1).over(w))
    return result

# -------------------------------------------------------------

def ingest_csv(spark, csv_path, raw_store, col_map, date_fmt):
    df = pd.read_csv(csv_path, skipinitialspace=True)
    bom_col = [c for c in df.columns if c.startswith('\ufeff')]
    if bom_col:
        og = bom_col[0]
        df = df.rename(columns={og: og.replace('\ufeff', '')})
    df = df.rename(columns=col_map)
    df["date"] = pd.to_datetime(df["date"], format=date_fmt, errors='coerce')
    for c in ["close", "open", "high", "low"]:
        df[c] = df[c].apply(parse_number)
    df["volume"] = df["volume"].apply(parse_volume)
    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    existing = list_partitions(raw_store)
    new = df[~df["date"].dt.strftime("%Y-%m-%d").isin(existing)]
    if new.empty:
        print(f"  Ingested 0 date(s)")
        return 0
    n = len(new)
    (
        spark.createDataFrame(new)
        .withColumn('date', F.to_date('date'))
        .write.mode("append").partitionBy(PART_COL).parquet(raw_store)
    )
    print(f"  Ingested {n} date(s)")
    return n

# -------------------------------------------------------------

def _compute_complex_features(joined, sundays, ticker, windows):
    p = lambda c: f"{ticker}_{c}"
    result = sundays.select("snap")
    
    for w in windows:
        w_days = w * 7
        suff = win_suffix(w)
        wd = joined.filter(F.col("r.date") > F.col("s.snap") - F.expr(f"INTERVAL {w_days} DAYS"))
        
        ws = Window.partitionBy("s.snap").orderBy("r.date")
        fws = ws.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        
        wd = (wd
              .withColumn("running_max_close", F.max("r.close").over(ws.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
              .withColumn("f_close", F.first("r.close", ignorenulls=True).over(fws))
              .withColumn("l_close", F.last("r.close", ignorenulls=True).over(fws)))
              
        aggs = [
            F.max("l_close").alias(p(f"last_close_{suff}")),
            F.max("f_close").alias(p(f"first_close_{suff}")),
            F.stddev("r.close").alias(p(f"std_close_{suff}")),
            F.min((F.col("r.close") / F.col("running_max_close")) - 1).alias(p(f"max_drawdown_{suff}")),
            F.expr(f"percentile_approx(r.daily_return, {VAR_ALPHA}, 100)").alias(p(f"return_var95_{suff}")),
            F.avg("r.daily_return").alias(p(f"return_mean_{suff}")),
            F.stddev("r.daily_return").alias(p(f"return_std_{suff}")),
            F.min("r.daily_return").alias(p(f"return_min_{suff}")),
            F.max("r.daily_return").alias(p(f"return_max_{suff}")),
            F.avg("r.volume").alias(p(f"volume_mean_{suff}")),
            F.stddev("r.volume").alias(p(p(f"volume_std_{suff}"))),
            F.skewness("r.volume").alias(p(f"volume_skew_{suff}")),
            F.max("r.volume").alias(p(f"volume_max_{suff}")),
            F.min("r.close").alias(p(f"price_min_{suff}")),
            F.max("r.close").alias(p(f"price_max_{suff}")),
        ]
        
        agg = wd.groupBy("s.snap").agg(*aggs).withColumnRenamed("s.snap", "snap")
        
        var_c = p(f"return_var95_{suff}")
        cvar_c = p(f"return_cvar95_{suff}")
        cvar_df = (
            wd.join(agg.select("snap", F.col(var_c).alias("_var")), "snap")
            .filter(F.col("r.daily_return").isNotNull() & (F.col("r.daily_return") <= F.col("_var")))
            .groupBy("s.snap")
            .agg(F.avg("r.daily_return").alias(cvar_c))
            .withColumnRenamed("s.snap", "snap"))
            
        agg = agg.join(cvar_df, "snap", "left")
        
        result = result.join(agg, "snap", "left")
        
    return result

# -------------------------------------------------------------

def _add_derived_features(df, ticker, windows, trading_days):
    p = lambda c: f"{ticker}_{c}"
    for w in windows:
        suff = win_suffix(w)
        rs = F.col(p(f"return_std_{suff}"))
        rm = F.col(p(f"return_mean_{suff}"))
        df = (df
              .withColumn(p(f"distance_to_high_{suff}"),
                          F.col(p(f"last_close_{suff}")) / F.col(p(f"price_max_{suff}")) - 1)
              .withColumn(p(f"distance_to_low_{suff}"),
                          F.when(F.col(p(f"price_min_{suff}")) > 0,
                                 F.col(p(f"last_close_{suff}")) / F.col(p(f"price_min_{suff}")) - 1))
              .withColumn(p(f"price_percentile_{suff}"),
                          F.when(F.col(p(f"price_max_{suff}")) != F.col(p(f"price_min_{suff}")),
                                 (F.col(p(f"last_close_{suff}")) - F.col(p(f"price_min_{suff}"))) /
                                 (F.col(p(f"price_max_{suff}")) - F.col(p(f"price_min_{suff}")))))
              .withColumn(p(f"price_return_{suff}"),
                          F.col(p(f"last_close_{suff}")) / F.col(p(f"first_close_{suff}")) - 1)
              .withColumn(p(f"return_sharpe_{suff}"),
                          F.when(rs.isNotNull() & (rs != 0), rm / rs * float(np.sqrt(trading_days)))))
    return df

def _add_cross_window_features(df, ticker):
    p = lambda c: f"{ticker}_{c}"
    for w1, w2 in CROSS_PAIRS:
        s1, s2 = win_suffix(w1), win_suffix(w2)
        for feat, c1, c2 in [
            ("return_mean_ratio", p(f"return_mean_{s1}"), p(f"return_mean_{s2}")),
            ("vol_diff", p(f"return_std_{s1}"), p(f"return_std_{s2}")),
            ("sharpe_diff", p(f"return_sharpe_{s1}"), p(f"return_sharpe_{s2}")),
            ("volume_diff", p(f"volume_mean_{s1}"), p(f"volume_mean_{s2}")),
        ]:
            df = df.withColumn(p(f"{feat}_{s1}_{s2}"), F.col(c1) - F.col(c2))
        for feat, c1, c2 in [
            ("vol_ratio", p(f"return_std_{s1}"), p(f"return_std_{s2}")),
        ]:
            df = df.withColumn(p(f"{feat}_{s1}_{s2}"),
                               F.when(F.col(c2) != 0, F.col(c1) / F.col(c2)))
    return df

def _add_circle_features(df, ticker):
    p = lambda c: f"{ticker}_{c}"
    w = Window.orderBy("snap")
    for period in CIRCLE_PERIODS:
        pc = f"c{period}w"
        for wlen in WINDOWS:
            ws = win_suffix(wlen)
            lc = p(f"last_close_{ws}")
            df = df.withColumn(
                p(f"price_circle_growth_{ws}_{pc}"),
                F.col(lc) / F.lag(lc, period).over(w) - 1
            )
    return df

def select_kept_columns(df, ticker):
    p = lambda c: f"{ticker}_{c}"
    14w = "14w"
    124w = "124w"
    
    kept = [
        p(f"return_sharpe_{14w}"),
        p(f"first_close_{14w}"),
        p(f"distance_to_low_{14w}"),
        p(f"return_cvar95_{14w}"),
        p(f"price_return_{14w}"),
        p(f"return_max_{14w}"),
        p(f"return_std_{14w}"),
        p(f"return_min_{14w}"),
        p(f"price_max_{14w}"),
        p(f"distance_to_high_{124w}"),
        p(f"return_max_{124w}"),
        p(f"std_close_{124w}"),
        p(f"volume_skew_{124w}"),
        p(f"price_percentile_{124w}"),
        p(f"volume_max_{124w}"),
        p(f"volume_std_{124w}"),
        p(f"max_drawdown_{124w}"),
    ]
    
    for feat in ["return_mean_ratio", "volume_diff", "vol_ratio", "vol_diff", "sharpe_diff"]:
        kept.append(p(f"{feat}_{14w}_{124w}"))
        
    for wlen in WINDOWS:
        ws = win_suffix(wlen)
        for period in CIRCLE_PERIODS:
            pc = f"c{period}w"
            kept.append(p(f"price_circle_growth_{ws}_{pc}"))
            
    existing = [c for c in df.columns if c in kept]
    return df.select("snap", *existing)

def run(spark, ticker, raw_store, feat_store, windows, trading_days):
    raw = load_raw(spark, raw_store)
    if not raw.head(1): return
    
    bounds = raw.agg(F.min("date"), F.max("date")).collect()[0]
    min_date, max_date = bounds[0], bounds[1]
    max_weeks = max(windows)
    
    sundays = spark.sql(f"""
        SELECT explode(sequence(date'{min_date}', date'{max_date}', interval 1 day)) AS snap
    """).filter(F.dayofweek("snap") == 1).cache()
    
    joined = (sundays.alias("s")
              .join(raw.alias("r"),
                    (F.col("r.date") > F.col("s.snap") - F.expr(f"INTERVAL {max_weeks * 7} DAYS")) &
                    (F.col("r.date") <= F.col("s.snap")),
                    "left"))
              
    result = _compute_complex_features(joined, sundays, ticker, windows)
    result = _add_derived_features(result, ticker, windows, trading_days)
    result = _add_cross_window_features(result, ticker)
    result = _add_circle_features(result, ticker)
    result = select_kept_columns(result, ticker)
    result = result.withColumn(PART_COL, F.date_format("snap", "yyyy-MM-dd")).drop("snap")
    result = result.select(*sorted(result.columns))
    result.write.mode("overwrite").partitionBy(PART_COL).parquet(feat_store)
    n = result.count()
    print(f"  Features: {n} snapshots")

def update(spark, ticker, raw_store, feat_store, windows, trading_days):
    existing = list_partitions(feat_store)
    if not existing:
        run(spark, ticker, raw_store, feat_store, windows, trading_days)
        return
        
    raw = load_raw(spark, raw_store)
    if not raw.head(1):
        print(f"  No raw data")
        return
        
    bounds = raw.agg(F.min("date"), F.max("date")).collect()[0]
    min_date, max_date = bounds[0], bounds[1]
    max_weeks = max(windows)
    
    new_sundays = spark.sql(f"""
        SELECT explode(sequence(date'{min_date}', date'{max_date}', interval 1 day)) AS snap
    """).filter(F.dayofweek("snap") == 1)
    new_sundays = new_sundays.filter(
        ~F.date_format("snap", "yyyy-MM-dd").isin(list(existing))
    ).cache()
    
    n_new = new_sundays.count()
    if n_new == 0:
        print(f"  Updated: 0 new snapshot(s)")
        return
        
    joined = (new_sundays.alias("s")
              .join(raw.alias("r"),
                    (F.col("r.date") > F.col("s.snap") - F.expr(f"INTERVAL {max_weeks * 7} DAYS")) &
                    (F.col("r.date") <= F.col("s.snap")),
                    "left"))
    result = _compute_complex_features(joined, new_sundays, ticker, windows)
    result = _add_derived_features(result, ticker, windows, trading_days)
    result = _add_cross_window_features(result, ticker)
    result = _add_circle_features(result, ticker)
    result = select_kept_columns(result, ticker)
    result = result.withColumn(PART_COL, F.date_format("snap", "yyyy-MM-dd")).drop("snap")
    result = result.select(*sorted(result.columns))
    result.write.mode("append").partitionBy(PART_COL).parquet(feat_store)
    print(f"  Updated: {n_new} new snapshot(s)")

def process_asset(spark, cfg, do_reset=False, do_run=True, do_update=True):
    ticker = cfg["ticker"]
    csv_path = os.path.join(DATA_DIR, cfg["csv"])
    col_map = COL_MAP
    date_fmt = DATE_FMT
    trading_days = cfg.get("trading_days", TRADING_DAYS.get(ticker, 252))
    raw_store = os.path.join(RAW_DIR or DATA_DIR, f"{ticker}_raw")
    feature_store = os.path.join(FEAT_DIR or DATA_DIR, f"{ticker}_features")
    
    print(f"\n{'-'*50}")
    print(f"Asset: {ticker}")
    print(f"{'-'*50}")
    
    if do_reset:
        ingest_csv(spark, csv_path, raw_store, col_map, date_fmt)
    if do_run:
        run(spark, ticker, raw_store, feature_store, WINDOWS, trading_days)
    if do_update:
        update(spark, ticker, raw_store, feature_store, WINDOWS, trading_days)
        
    rd = sorted(list_partitions(raw_store))
    fd = sorted(list_partitions(feature_store))
    print(f"  Raw: {len(rd)} days | Features: {len(fd)} ({fd[0]} -> {fd[-1]})" if fd else f"  Raw: {len(rd)} days")

for asset in ASSETS:
    process_asset(spark, asset, do_reset=True, do_run=True, do_update=False)

@F.udf(T.DateType())
def get_sunday(d):
    weekday = d.weekday()
    if weekday in [0, 1, 2]:
        return d - timedelta(weekday + 1 + 7)
    else:
        return d - timedelta(weekday + 1)

df_label = spark.read.parquet('/label/client=hc/source_code=20250417_hc_train_credit_score_81k').select('phone_number', 'disburse_date', 'label_value', 'product').dropDuplicates()
df_label.show(1)

df_fts = spark.read.parquet('/user/nguyetnguyen/features/external_sources/macro_sources/btc_features')
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
#   .write.mode('overwrite').parquet('/user/nguyetnguyen/features/hc_2025/external_source_btc_merge_all')
)

cfg = NewFeatureEvalConfig.from_json(path='/apps/jupyter/users/nguyetnguyen/features/external_sources/configs/experiment_config_btc.json')
cfg
df_result_btc = evaluate_features_in_bulk(spark, cfg)
