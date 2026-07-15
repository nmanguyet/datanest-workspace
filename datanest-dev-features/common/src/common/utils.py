import os
import pandas as pd
import IPython
import subprocess
import pyspark.sql.functions as F
import pyspark.sql.types as T

from datetime import timedelta
from pyspark.sql import DataFrame as SparkDataFrame
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))


def dir_ls(dir_path, suffix=''):
    args = "hdfs dfs -ls " + dir_path + " | awk '{print $8}'"
    proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    dirs = s_output.split()

    result = []
    for directory in dirs:
        directory = directory.decode("utf-8")
        if suffix != '':
            directory = directory + f'/{suffix}'
        result.append(directory)
    return sorted(result)


@F.udf(T.DateType())
def get_sunday(d):
    weekday = d.weekday()
    if weekday in [0, 1, 2]:
        return d - timedelta(weekday + 1 + 7)
    else:
        return d - timedelta(weekday + 1)


def detect_wrong_type_columns(
    df,
    sample_size=10000,
    numeric_threshold=0.95,
    int_threshold=0.98,
    verbose=True
):
    # ==========================================================
    # PANDAS
    # ==========================================================
    if isinstance(df, pd.DataFrame):
        if len(df) > sample_size:
            sample_df = df.sample(sample_size, random_state=42)
        else:
            sample_df = df.copy()

        float_cols = []
        int_cols = []
        detail = {}

        candidate_cols = sample_df.select_dtypes(include=["object", "string"]).columns

        for col in candidate_cols:
            s = sample_df[col].dropna()
            if len(s) == 0:
                continue
            # convert numeric
            numeric_s = pd.to_numeric(s, errors="coerce")
            convert_ratio = numeric_s.notna().mean()

            if convert_ratio >= numeric_threshold:
                valid_numeric = numeric_s.dropna()
                # check integer-like
                int_like_ratio = ((valid_numeric % 1 == 0).mean() if len(valid_numeric) > 0 else 0)

                suggest_type = ("int" if int_like_ratio >= int_threshold else "float")

                detail[col] = {
                    "convert_ratio": round(convert_ratio, 4),
                    "int_like_ratio": round(int_like_ratio, 4),
                    "suggest_type": suggest_type,
                }

                if suggest_type == "int":
                    int_cols.append(col)
                else:
                    float_cols.append(col)

        result = {
            "float_columns": float_cols,
            "int_columns": int_cols,
            "detail": detail,
        }

        if verbose:
            print("\n=== WRONG TYPE COLUMNS DETECTED ===")
            print(result)

        return result

    # ==========================================================
    # PYSPARK
    # ==========================================================
    elif isinstance(df, SparkDataFrame):
        total_count = df.count()
        frac = min(sample_size / total_count, 1.0)
        sample_df = df.sample(
            withReplacement=False,
            fraction=frac,
            seed=42
        )

        string_cols = [
            f.name
            for f in sample_df.schema.fields
            if f.dataType.simpleString() == "string"
        ]

        float_cols = []
        int_cols = []
        detail = {}

        for col in string_cols:
            tmp = (
                sample_df
                .select(col)
                .where(F.col(col).isNotNull())
            )
            cnt = tmp.count()
            if cnt == 0:
                continue
            numeric_cnt = (
                tmp.where(F.col(col).cast("double").isNotNull()).count()
            )

            convert_ratio = numeric_cnt / cnt

            if convert_ratio >= numeric_threshold:
                int_cnt = (
                    tmp.where(
                        (
                            F.col(col).cast("double")
                            ==
                            F.col(col).cast("long")
                        )
                        &
                        F.col(col).cast("double").isNotNull()
                    ).count()
                )

                int_like_ratio = int_cnt / numeric_cnt

                suggest_type = ("int" if int_like_ratio >= int_threshold else "float")

                detail[col] = {
                    "convert_ratio": round(convert_ratio, 4),
                    "int_like_ratio": round(int_like_ratio, 4),
                    "suggest_type": suggest_type,
                }

                if suggest_type == "int":
                    int_cols.append(col)
                else:
                    float_cols.append(col)

        result = {
            "float_columns": float_cols,
            "int_columns": int_cols,
            "detail": detail,
        }

        if verbose:
            print("\n=== WRONG TYPE COLUMNS DETECTED ===")
            print(result)

        return result

    else:
        raise TypeError(
            "df must be pandas.DataFrame or pyspark.sql.DataFrame"
        )


def get_name_current_folder(current_dir='./output/model'):
    folders = [item for item in os.listdir(current_dir) if os.path.isdir(os.path.join(current_dir, item))]
    if not folders:
        i = 1
    else:
        sequences = []
        for folder in folders:
            last_3_chars = folder[6:9]
            try:
                num = int(last_3_chars)
                sequences.append(num)
            except ValueError:
                continue

        if not sequences:
            i = 1
        else:
            max_sequences = max(sequences)
            i = max_sequences

    if i < 10:
        suffix = f'00{i}'
    elif i < 100:
        suffix = f'0{i}'
    else:
        suffix = i

    return f'{current_dir}/train_{suffix}'


def get_params_space_catboost():
    '''Catboost parameter space'''
    params = {
        'learning_rate': [0.01, 0.02, 0.025, 0.03, 0.05, 0.1],
        'depth': [4, 5, 6],
        'l2_leaf_reg': [5, 10, 15, 20],
        'border_count': [32, 64, 128, 255],
        'bagging_temperature': [0, 0.1, 0.5],
        'random_strength': [0, 1, 2],
    }
    return params


def display_styled_metrics(selector_shap, color_map={}, all_cols_to_display=[]):
    log_row_list = []
    for log_entry in selector_shap['turn_logs']:
        log_row_dict = {}
        log_row_dict['Turn'] = log_entry.get('turn')
        log_row_dict['Start num fts'] = log_entry.get('num_fts_start')
        log_row_dict['End num fts'] = log_entry.get('num_fts_end')
        log_row_dict['Start num cat fts'] = len(log_entry.get('cat_fts_start'))
        log_row_dict['End num cat fts'] = len(log_entry.get('cat_fts_end'))
        log_row_dict['Best iter'] = log_entry.get('best iter')
        for metric in ['train_metrics', 'valid_metrics', 'test_metrics', 'test_metrics_label0']:
            for name, value in log_entry.get(metric).items():
                log_row_dict[name] = value
        log_row_list.append(log_row_dict)

    log_row = pd.DataFrame(log_row_list)

    cols_to_display = [col for col in all_cols_to_display if col in log_row.columns]

    metrics_to_plot = {
        metric: cmap for metric, cmap in color_map.items()
        if metric in log_row.columns
    }
    styled_summary = log_row[cols_to_display].style

    for metric, cmap in metrics_to_plot.items():
        styled_summary = styled_summary.background_gradient(
            axis=0,
            cmap=cmap,
            subset=(metric)
        )

    IPython.display.display(styled_summary)
    return log_row