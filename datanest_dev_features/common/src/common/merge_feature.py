import os
import time
import pyspark.sql.functions as F

from .utils import dir_ls
from datetime import datetime, timedelta
from typing import Optional
from pyspark.sql import functions as F


def merge_features_from_sample_path(
    spark,
    all_paths: list,
    del_paths: list,
    base_path: str,
    output_dir: str,
    prefix: str = "hc_2025",
    left_join_cols: Optional[list] = None,
    inner_join_cols: Optional[list] = None,
):
    left_join_cols = left_join_cols or ["phone_number", "disburse_date", "product", "label_value"]
    inner_join_cols = inner_join_cols or ["phone_number", "disburse_date"]

    base_df = spark.read.parquet(base_path).select(*left_join_cols)

    for path in all_paths:
        if path in del_paths or "score_model_path" in path:
            continue

        df_fts = spark.read.parquet(path)

        suffix = path.rstrip("/").split("/")[-2]
        rename_exprs = [
            F.col(c).alias(f"{c}_{suffix}")
            for c in df_fts.columns
            if c not in inner_join_cols
        ]
        df_fts = df_fts.select(*inner_join_cols, *rename_exprs)

        joined = base_df.join(df_fts, on=inner_join_cols, how="left")

        out_name = f"{prefix}_{suffix}"
        out_path = os.path.join(output_dir, out_name)
        joined.write.mode("overwrite").parquet(out_path)
        print(f"Saved: {out_path}")

    temp_files = dir_ls(output_dir)
    df_final = base_df

    for path in temp_files:
        if any(x in path for x in ["_all_features", "score_model_path"]) or path in del_paths:
            continue

        print(f"Merging: {path}")
        df_tmp = spark.read.parquet(path)
        new_cols = [c for c in df_tmp.columns if c not in left_join_cols]
        df_final = df_final.join(
            df_tmp.select(*left_join_cols, *new_cols),
            on=left_join_cols,
            how="left",
        )

    df_final = df_final.select([
        F.col(c).cast("double") if t.startswith("decimal") else F.col(c)
        for c, t in df_final.dtypes
    ])

    final_path = os.path.join(output_dir, f"{prefix}_all_features")
    df_final.write.mode("overwrite").parquet(final_path)
    print(f"Final master table: {final_path}")

    return final_path


def merge_each_feature_from_feature_store(
    spark,
    df_label,
    feature_path,
    output_base_path,
    prefix='',
    date_col='date',
    phone_col='phone_number',
    disburse_col='disburse_date',
    date_diff_min=3,
    date_diff_max=9
):
    print(f'Feature: {feature_path}')

    start_time = time.perf_counter()
    suffix = feature_path.split('/')[-1]
    min_date = df_label.select(F.min(disburse_col).alias('min_date')).collect()[0].min_date
    max_date = df_label.select(F.max(disburse_col).alias('max_date')).collect()[0].max_date
    start_date = (min_date - timedelta(days=10)).strftime('%Y-%m-%d')
    end_date = (max_date + timedelta(days=10)).strftime('%Y-%m-%d')

    df_fts = (spark.read.parquet(feature_path)
              .filter((F.col(date_col) >= start_date)
                      & (F.col(date_col) <= end_date)
              ))

    df_phone_date = (df_label
                     .select(phone_col, disburse_col)
                     .dropDuplicates())

    result = (df_label
              .join(df_phone_date
                    .join(df_fts, on=phone_col)
                    .where(F.datediff(F.col(disburse_col), F.col(date_col)).between(date_diff_min, date_diff_max)),
                    on=[phone_col, disburse_col],
                    how='left'
              ))

    output_path = f'{output_base_path}/{prefix}_{suffix}'
    (
        result.write.mode('overwrite').parquet(output_path)
    )

    elapsed = time.perf_counter() - start_time

    print(f'Output : {output_path}')
    print(f'Time   : {elapsed:.2f}s')
    print('-' * 80)

    return result


def merge_all_features(spark, label_path, feature_dir, output_dir, prefix, del_paths=None, left_join_cols=None):
    del_paths = del_paths or []
    left_join_cols = left_join_cols or ["phone_number", "disburse_date", "product", "label_value"]

    df_final = spark.read.parquet(label_path)

    for path in dir_ls(feature_dir):
        if any(x in path for x in ["_all_features", "score_model_path"]) or path in del_paths:
            continue

        print(f"Merging: {path}")

        df_tmp = spark.read.parquet(path)
        new_cols = [c for c in df_tmp.columns if c not in left_join_cols and c not in df_final.columns]

        if new_cols:
            df_final = df_final.join(df_tmp.select(*left_join_cols, *new_cols), on=left_join_cols, how="left")

    df_final = df_final.select([
        F.col(c).cast("double").alias(c) if t.startswith("decimal") else F.col(c)
        for c, t in df_final.dtypes
    ])

    final_path = os.path.join(output_dir, f"{prefix}_all_features")
    df_final.write.mode("overwrite").parquet(final_path)

    print(f"Final master table: {final_path}")

    return df_final, final_path