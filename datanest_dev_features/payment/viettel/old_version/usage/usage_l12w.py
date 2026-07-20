import sys
from datetime import datetime
from etl.common.utils import log_io_file_path
import pandas as pd
from dateutil import relativedelta
from pyspark.sql import functions as F, types as T

from etl.common import init_spark3
from etl.viettel.common.time_util import is_sunday


def get_last_activated_date_file(
    target_date,
    # TODO (dat): Remove hardcoded path by using local env
    last_activated_date_path='hdfs://datanest-ha/data/processed/credit_score_v1/features/sub/last_activated_date/daily'
):
    lad_file = spark.read.parquet("{}/date={}".format(last_activated_date_path, target_date)) \
        .selectExpr('phone_number', 'start_date last_activated_date')

    return lad_file


def get_spark_instance(date_str):
    spark = init_spark3.setup(
        job_cfg={
            "executor.instances": 6,
            "executor.cores": 6,
            "executor.memory": '25g'
        },
        script_name='usage_features_{}'.format(date_str)
    )

    return spark


def get_all_usage_dates(usage_data_gap='5D'):
    all_usage_dates = []
    for begin_month_date in pd.date_range('2018-02-01', '2030-01-01', freq='MS', closed=None):
        begin_month_date_str = datetime.strftime(begin_month_date, '%Y-%m-%d')
        next_begin_month_date = begin_month_date + relativedelta.relativedelta(months=1)
        next_begin_month_date_str = datetime.strftime(next_begin_month_date, '%Y-%m-%d')

        usage_dates = pd.date_range(begin_month_date_str, next_begin_month_date_str, freq=usage_data_gap)
        usage_dates = sorted([datetime.strftime(date, '%Y-%m-%d') for date in usage_dates])

        all_usage_dates.extend(usage_dates)
    return sorted(set(all_usage_dates))


def get_data_from_date_to_date(
    spark,
    data_path,
    begin_date_str,
    end_date_str,
    dates=None
):
    df = None

    dates = dates if dates else pd.date_range(begin_date_str, end_date_str, freq='1D')
    if not dates:
        return df

    for date in dates:
        if not isinstance(date, str):
            date = datetime.strftime(date, '%Y-%m-%d')

        data_file_path = '{}/date={}'.format(data_path, date)

        # if not check_dir_exist(data_file_path):
        #     continue

        temp_df = (
            spark.read.format('delta').load(data_path).where(f"date == '{date}'").drop("date").
            withColumn(
                'date',
                F.from_unixtime(F.unix_timestamp(F.lit(date), 'yyyy-MM-dd')).cast(T.TimestampType())
            )
        )

        if df is None:
            df = temp_df
        else:
            df = df.union(temp_df)

    lad_df = get_last_activated_date_file(end_date_str)

    df = (
        df.
        join(lad_df, 'phone_number', 'inner').
        filter('date >= last_activated_date').
        drop('last_activated_date')
    )

    return df


def get_usage_data_last_x_months(
    spark,
    usage_type,
    end_date_str,
    backward_months=6,
    usage_path='hdfs://cicdataha/data/viettel_v5/usage_{}/'
):
    if usage_type not in ['pre', 'pos']:
        raise ValueError('usage_type must be of value "pre" or "pos".')

    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    all_date_strs = get_all_usage_dates()
    all_date_strs = sorted([date_str for date_str in all_date_strs if date_str <= end_date_str])
    all_date_strs = [date_str for date_str in all_date_strs if date_str.endswith('-01')]
    all_date_strs = all_date_strs[-backward_months:]

    begin_date_str = all_date_strs[0]

    # backfill logic
    while len(all_date_strs) < backward_months:
        n_months_to_be_filled = backward_months - len(all_date_strs)
        all_date_strs = all_date_strs + all_date_strs[:n_months_to_be_filled]

    print('Selecting usage_{} data from dates={}'.format(usage_type, all_date_strs))

    usage_data_path = usage_path.format(usage_type)
    usage_df = get_data_from_date_to_date(
        spark, usage_data_path, begin_date_str, end_date_str, all_date_strs
    )

    return usage_df


def union_pre_and_pos_data(spark,
                           usage_pre_df,
                           usage_pos_df):
    usage_pre_df_columns = usage_pre_df.columns
    usage_pos_df_columns = usage_pos_df.columns

    for column in usage_pre_df_columns:
        if column not in usage_pos_df_columns:
            usage_pos_df = usage_pos_df.withColumn(column, F.lit(-1))

    usage_pos_df = usage_pos_df.select(*usage_pre_df_columns)

    return usage_pre_df.union(usage_pos_df)


def generate_features_for_usage_data(spark, usage_df, suffix):
    to_be_aggregated_usage_df_columns = set(usage_df.columns)
    to_be_aggregated_usage_df_columns -= set(['phone_number', 'date', 'last_activated_date'])

    feature_name_format = 'usage_{}_{}_{}'
    elementary_function_collections = [
        (F.min, 'min'),
        (F.stddev, 'std'),
        (F.sum, 'sum')
    ]
    # if using usage features for last x days,
    # only compute the sum features
    if suffix.endswith('d'):
        elementary_function_collections = [
            (F.sum, 'sum')
        ]

    features = []
    for column in to_be_aggregated_usage_df_columns:
        usage_df = usage_df.withColumn(column, F.col(column).cast(T.DoubleType()))
        for function, function_name in elementary_function_collections:
            feature_name = feature_name_format.format(column, function_name, suffix).lower()
            features.extend([function(F.col(column)).alias(feature_name)])

    return usage_df.groupBy('phone_number').agg(*features)

percentage_features_dict = {
    'usage_t_tot_cost_sum_l3m': [
        # usage with prom_ prefix
        {
            'feature_name': 'usage_t_prom_cost_sum_l3m',
            'pct_feature_name': 'usage_prom_vs_total_l3m'
        },

        # usage with s_ prefix vs total
        {
            'feature_name': 'usage_s_tot_cost_sum_l3m',
            'pct_feature_name': 'usage_s_vs_total_l3m'
        },
        {
            'feature_name': 'usage_s_ext_org_cost_sum_l3m',
            'pct_feature_name': 'usage_s_ext_vs_total_l3m'
        },
        {
            'feature_name': 'usage_s_int_org_cost_sum_l3m',
            'pct_feature_name': 'usage_s_int_vs_total_l3m'
        },
        {
            'feature_name': 'usage_s_intn_org_cost_sum_l3m',
            'pct_feature_name': 'usage_s_intn_vs_total_l3m'
        },
        {
            'feature_name': 'usage_s_org_cost_sum_l3m',
            'pct_feature_name': 'usage_s_org_vs_total_l3m'
        },
        {
            'feature_name': 'usage_s_prom_cost_sum_l3m',
            'pct_feature_name': 'usage_s_prom_vs_total_l3m'
        },

        # usage with v_ prefix vs total
        {
            'feature_name': 'usage_v_tot_cost_sum_l3m',
            'pct_feature_name': 'usage_v_vs_total_l3m'
        },
        {
            'feature_name': 'usage_v_ext_org_cost_sum_l3m',
            'pct_feature_name': 'usage_v_ext_org_vs_total_l3m'
        },
        {
            'feature_name': 'usage_v_int_org_cost_sum_l3m',
            'pct_feature_name': 'usage_v_int_org_vs_total_l3m'
        },
        {
            'feature_name': 'usage_v_intn_org_cost_sum_l3m',
            'pct_feature_name': 'usage_v_intn_org_vs_total_l3m'
        },
        {
            'feature_name': 'usage_v_org_cost_sum_l3m',
            'pct_feature_name': 'usage_v_org_vs_total_l3m'
        },
        {
            'feature_name': 'usage_v_prom_cost_sum_l3m',
            'pct_feature_name': 'usage_v_prom_vs_total_l3m'
        },

        # usages with re_charge_ prefix vs total
        {
            'feature_name': 'usage_re_charge_sum_l3m',
            'pct_feature_name': 'usage_re_charge_vs_total_l3m'
        },

        # usages with m_ prefix vs total
        {
            'feature_name': 'usage_m_org_cost_sum_l3m',
            'pct_feature_name': 'usage_m_org_vs_total_l3m'
        },

        # usages with g_ prefix vs total
        {
            'feature_name': 'usage_g_monthly_fee_sum_l3m',
            'pct_feature_name': 'usage_g_vs_total_l3m'
        },
        {
            'feature_name': 'usage_g_reg_fee_sum_l3m',
            'pct_feature_name': 'usage_g_reg_vs_total_l3m'
        },
        {
            'feature_name': 'usage_g_org_cost_sum_l3m',
            'pct_feature_name': 'usage_g_org_vs_total_l3m'
        },

        # usage with vas_ prefix
        {
            'feature_name': 'usage_vas_tot_cost_sum_l3m',
            'pct_feature_name': 'usage_vas_vs_total_l3m'
        },
        {
            'feature_name': 'usage_vas_org_cost_sum_l3m',
            'pct_feature_name': 'usage_vas_org_vs_total_l3m'
        },
        {
            'feature_name': 'usage_vas_prom_cost_sum_l3m',
            'pct_feature_name': 'usage_vas_prom_vs_total_l3m'
        },

        # usage with org_ prefix
        {
            'feature_name': 'usage_t_org_cost_sum_l3m',
            'pct_feature_name': 'usage_org_vs_total_l3m'
        }
    ],
}


def get_percentage_usage_features(
    spark,
    target_date,
    percentage_features_dict
):

    usage_df = spark.read.parquet(
        'hdfs://datanest-ha/data/processed/inventory/features/usage/agg/3m/date={}'.format(
            target_date_str
        )
    )

    pct_feature_names = ['phone_number']
    for total_feature_name in percentage_features_dict:
        for feature_dict in percentage_features_dict[total_feature_name]:
            feature_name = feature_dict['feature_name']
            pct_feature_name = feature_dict['pct_feature_name']

            usage_df = (
                usage_df
                .withColumn(
                    pct_feature_name,
                    (
                        F.when(
                            F.col(total_feature_name) != 0.0,
                            F.col(feature_name) / F.col(total_feature_name)
                        ).otherwise(
                            0.0
                        )
                    ).cast(T.DoubleType())
                )
            )

            pct_feature_names.append(pct_feature_name)

    return usage_df.select(*pct_feature_names)


target_date_str = sys.argv[1]

if not is_sunday(datetime.strptime(target_date_str, "%Y-%m-%d")):
    print("only run in sunday")
    exit(0)

spark = get_spark_instance(target_date_str)

for backward_months in [3]:
    usage_pre_df = get_usage_data_last_x_months(spark, 'pre', target_date_str, backward_months).dropDuplicates()
    usage_pos_df = get_usage_data_last_x_months(spark, 'pos', target_date_str, backward_months).dropDuplicates()

    usage_df = union_pre_and_pos_data(spark, usage_pre_df, usage_pos_df)
    usage_df = generate_features_for_usage_data(spark, usage_df, 'l{}m'.format(backward_months))
    usage_df.write.mode('overwrite').parquet(
        'hdfs://datanest-ha/data/processed/inventory/features/usage/agg/{}m/date={}'.format(backward_months, target_date_str)
    )

usage_pct_df = get_percentage_usage_features(spark, target_date_str, percentage_features_dict)

usage_pct_df.write.mode('overwrite').parquet(
    'hdfs://cicdataha/data/processed/inventory/features/usage/percentage/3m/date={}'.format(target_date_str)
)


try:

    log_io_file_path(input_paths=['hdfs://datanest-ha/data/processed/credit_score_v1/features/sub/last_activated_date/daily',
                                 'hdfs://cicdataha/data/viettel_v5/usage_pre',
                                 'hdfs://cicdataha/data/viettel_v5/usage_pos'],
                     output_paths=['hdfs://datanest-ha/data/processed/inventory/features/usage/agg/3m',
                                   'hdfs://cicdataha/data/processed/inventory/features/usage/percentage/3m'],
                     )
except Exception as e:
    print(f'Waring: unable to log data lineage! Err: {e}')