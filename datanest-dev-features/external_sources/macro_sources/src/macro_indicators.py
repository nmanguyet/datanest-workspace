import sys
sys.path.append('/Users/anhnguyet/Documents/dev_fts/common/src')

from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession

DATA_DIR = '/Users/anhnguyet/Documents/dev_fts/external_sources/macro_sources/data'
RAW_DIR = ''
FEAT_DIR = ''
PART_COL = 'date'
CIRCLE_PERIODS = [4, 12, 52]

INDICATORS = [
    {"name": "retail_sale", "csv": "retail_sale.csv"},
    {"name": "cpi_data", "csv": "cpi_data.csv"},
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
        print(f"  {indicator_name}: 0 new date(s)")
        return

    n = len(latest)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as f:
        latest.to_parquet(f.name)
        spark.read.parquet(f.name).write.mode("append").partitionBy(PART_COL).parquet(raw_store)
    print(f"  {indicator_name}: ingested {n} date(s)")


def run(spark, indicator_name, raw_store, feat_store):
    raw = spark.read.parquet(raw_store)
    if not raw.head(1):
        print(f"  {indicator_name}: no data")
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
    result = result.select(F.col("s.snap").alias("snap"), F.col("r.percentage").alias("value"))

    p = lambda c: f"{indicator_name}_{c}"
    for period in CIRCLE_PERIODS:
        pc = f"c{period}w"
        ws = Window.orderBy("snap")
        result = result.withColumn(
            p(f"circle_growth_{pc}"),
            F.col("value") / F.lag("value", period).over(ws) - 1
        )

    result = result.withColumn(PART_COL, F.date_format("snap", "yyyy-MM-dd")).drop("snap")
    result = result.select(*sorted(result.columns))
    result.write.mode("overwrite").partitionBy(PART_COL).parquet(feat_store)
    n = result.count()
    print(f"  {indicator_name}: {n} snapshot(s) written")


def process_indicator(spark, cfg, do_reset=False):
    name = cfg["name"]
    csv_path = f"{DATA_DIR}/{cfg['csv']}"
    raw_store = RAW_DIR or f"{DATA_DIR}/{name}_raw"
    feat_store = FEAT_DIR or f"{DATA_DIR}/{name}_features"

    print(f"\n{'='*50}")
    print(f"Indicator: {name}")
    print(f"{'='*50}")

    if do_reset:
        import shutil, os
        if os.path.exists(raw_store):
            shutil.rmtree(raw_store)
    ingest_csv(spark, csv_path, raw_store, name)

    run(spark, name, raw_store, feat_store)

    rd = sorted(list_partitions(raw_store))
    fd = sorted(list_partitions(feat_store))
    status = f"  Raw: {len(rd)} days"
    if fd:
        status += f" | Features: {len(fd)} ({fd[0]} -> {fd[-1]})"
    print(status)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MacroIndicators").getOrCreate()
    for indicator in INDICATORS:
        process_indicator(spark, indicator, do_reset=True)
    spark.stop()
