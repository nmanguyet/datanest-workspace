import sys
sys.path.append('/Users/anhnguyet/Documents/dev_fts/common/src')

from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
# import sys
# sys.path.append('/Users/anhnguyet/Documents/dev_fts/common/src')

from pyspark.sql import functions as F, Window

# DATA_DIR = '/Users/anhnguyet/Documents/dev_fts/external_sources/macro_sources/data'
DATA_DIR = '/apps/jupyter/users/nguyetnguyen/features/external_sources/data'
RAW_DIR = '/user/nguyetnguyen/features/external_sources/data'
FEAT_DIR = '/user/nguyetnguyen/features/external_sources/macro_sources'
PART_COL = 'date'
CIRCLE_PERIODS = [4, 12, 52]

INDICATORS = [
    {"name": "retail_sale", "csv": "retail_sale_202001_202606.csv"},
    {"name": "cpi", "csv": "cpi_202001_202606.csv"},
]

def list_partitions(path):
    import os
    if not os.path.exists(path):
        return set()
    return {n.split('=', 1)[1] for n in os.listdir(path) if n.startswith(f"{PART_COL}=")}

def ingest_csv(spark, csv_path, raw_store, indicator_name):
    import pandas as pd
    import tempfile
    
    pdf = pd.read_csv(csv_path, skipinitialspace=True)
    pdf.columns = [c.strip() for c in pdf.columns]
    pdf["date"] = pd.to_datetime(pdf["date"], errors='coerce')
    pdf["percentage"] = (
        pdf["percentage"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
        .astype(float)
    )

    pdf = pdf.dropna(subset=["date", "percentage"]).sort_values("date").reset_index(drop=True)
    pdf["indicator"] = indicator_name
    
    existing = list_partitions(raw_store)
    latest = pdf[~pdf["date"].dt.strftime("%Y-%m-%d").isin(existing)]
    if latest.empty:
        print(f" {indicator_name}: 0 new date(s)")
        return
        
    df_original = spark.createDataFrame(latest)
    df_monthly = df_original.withColumn("date", F.to_date("date")).withColumn(
        "year_month", F.date_format("date", "yyyy-MM")
    )
    
    min_max = df_monthly.select(F.min("date").alias("min_d"), F.max("date").alias("max_d")).collect()[0]
    start_date, end_date = min_max["min_d"], min_max["max_d"]
    df_sundays = (
        spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as snapshot_date")
        .withColumn("day_of_week", F.dayofweek("snapshot_date"))
        .filter(F.col("day_of_week") == 1)
        .withColumn("year_month", F.date_format("snapshot_date", "yyyy-MM"))
        .drop("day_of_week")
    )
    
    df_snapshot = df_sundays.join(
        df_monthly.select("year_month", "percentage"),
        on="year_month",
        how="inner"
    ).selectExpr("snapshot_date date", "percentage") \
     .orderBy("date")
     
    df_snapshot.write \
        .mode("overwrite") \
        .partitionBy(PART_COL)\
        .parquet(raw_store)
        
    print(f'Done at {datetime.now()} in {raw_store}', )

def run(spark, indicator_name, raw_store, feat_store):
    raw = spark.read.parquet(raw_store)
    if not raw.head(1):
        print(f" {indicator_name}: no data")
        return
        
    raw = raw.dropDuplicates(["date"]).orderBy("date").cache()
    bounds = raw.agg(F.min("date"), F.max("date")).collect()[0]
    min_date, max_date = bounds[0], bounds[1]
    max_lookback = max(CIRCLE_PERIODS) * 7
    
    sundays = spark.sql(f"""
        SELECT explode(sequence(date'{min_date}', date'{max_date}', interval 1 day)) AS snap
    """).filter(F.dayofweek("snap") == 1).cache()
    
    ff = spark.sql(f"""
        SELECT explode(sequence(date'{min_date}', date'{max_date}', interval 1 day)) AS date
    """).join(raw, "date", "left")
    
    w = Window.orderBy("date")
    ff = ff.withColumn("percentage", F.last("percentage", ignorenulls=True).over(
        w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )).filter(F.col("percentage").isNotNull())
    
    result = sundays.alias("s").join(
        ff.alias("r"),
        F.col("r.date") <= F.col("s.snap"),
        "left"
    )
    
    w2 = Window.partitionBy("s.snap").orderBy(F.col("r.date").desc())
    result = result.withColumn("rn", F.row_number().over(w2)).filter(F.col("rn") == 1)
    result = result.select(F.col("s.snap").alias("snap"), F.col("r.percentage").alias(f"{indicator_name}_value"))

    p = lambda c: f"{indicator_name}_{c}"
    for period in CIRCLE_PERIODS:
        pc = f"c{period}w"
        ws = Window.orderBy("snap")
        result = result.withColumn(
            p(f"circle_growth_{pc}"),
            F.col(f"{indicator_name}_value") / F.lag(f"{indicator_name}_value", period).over(ws) - 1
        )
        
    result = result.withColumn(PART_COL, F.date_format("snap", "yyyy-MM-dd")).drop("snap")
    result = result.select(*sorted(result.columns))
    result.write.mode("overwrite").partitionBy(PART_COL).parquet(feat_store)
    n = result.count()
    print(f" {indicator_name}: {n} snapshot(s) written")

def process_indicator(spark, cfg):
    name = cfg["name"]
    csv_path = f"{DATA_DIR}/{cfg['csv']}"
    raw_store = os.path.join(RAW_DIR or DATA_DIR, f"{name}_raw")
    feat_store = os.path.join(FEAT_DIR or DATA_DIR, f"{name}_features")
    print(csv_path)
    
    print(f"\n{'-'*50}")
    print(f"Indicator: {name}")
    print(f"{'-'*50}")
    
    print(raw_store)
    ingest_csv(spark, csv_path, raw_store, name)
    
    run(spark, name, raw_store, feat_store)
    
    print(feat_store)

for indicator in INDICATORS:
    process_indicator(spark, indicator)

(
    spark.read.parquet('/user/nguyetnguyen/features/external_sources/macro_sources/cpi_features')
    .join(spark.read.parquet('/user/nguyetnguyen/features/external_sources/macro_sources/retail_sale_features'), 'date')
    .write.mode('overwrite').parquet('/user/nguyetnguyen/features/external_sources/macro_sources/cpi_retail_sale_features')
)

df_label = spark.read.parquet('/label/client=hc/source_code=20250417_hc_train_credit_score_81k').select('phone_number', 'disburse_date', 'label_value', 'product').dropDuplicates()
df_label.show(1)

df_fts = spark.read.parquet('/user/nguyetnguyen/features/external_sources/macro_sources/cpi_retail_sale_features')

(
    df_label
    .join(
        df_label
        .select('phone_number', 'disburse_date')
        .withColumn('date', get_sunday(F.col('disburse_date')))
        .dropDuplicates()
        .join(df_fts, 'date')
        , ['phone_number', 'disburse_date'], 'left')
    .write.mode('overwrite').parquet('/user/nguyetnguyen/features/hc_2025/external_source_cpi_retail_sale_merge_all')
)

cfg = NewFeatureEvalConfig.from_json(path='/apps/jupyter/users/nguyetnguyen/features/external_sources/configs/experiment_config_cpi_retail_sale.json')
cfg
df_result_btc = evaluate_features_in_bulk(spark, cfg)
