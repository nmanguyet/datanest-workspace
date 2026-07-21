from cmath import log
from datetime import datetime, timedelta
import logging
import sys
from pyspark.sql import Window
from pyspark.sql import functions as F

from etl.common.logger import config_log
from etl.common import date_util, utils, init_spark3
from etl.viettel.common.utils import check_enough_data
from etl.common.utils import log_io_file_path

config_log()
logger = logging.getLogger(__name__)

SNAPSHOT_DATE_STR = sys.argv[1]

INPUT_PATH = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_latest"
OUTPUT_PATH = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_account_feature"


# Check is sunday
date_util.check_is_sunday(SNAPSHOT_DATE_STR)

script_name = utils.get_spark_script_name()
spark = init_spark3.setup(
    job_cfg= {
        "executor.instances": 8,
        "executor.cores": 5,
        "executor.memory": '20g'
    },
    script_name=script_name
)

# ===================================================
# RAW COLUMNS KEPT FOR CROSS FEATURES
# ===================================================
PHONE_COL = "phone_number"

SELECTED_COLUMNS = [
    'MOTHER_SYSTEM',
    'phone_number',
    'mobile_id',

    'provine_district',
    'province_code',
    'district',

    'actv_date',
    'updated_date',
    'pin_date',
    'info_updated_date',
    'money_source_linked_date',
    'customer_type_date',

    'money_source_table',
    'money_sources',

    'profile_status',
    'pin_status',
    'mobile_level',

    'agent_type_customer',
    'is_current',
    'company_id',

    'channel_type',
    'channel_type_id_tele_sale',
    'customer_type',

    'merchant_channel_type',

    'reg_type',
    'create_at_bp',
    'vds_id_channel_code',
    'vtt_id_channel_code'
]

PK_COLS = [
    "phone_number",
    "MOTHER_SYSTEM"
]

CROSS_RAW_COLS = [
    "mobile_id",

    # location
    "province_code",
    "district",

    # status
    "profile_status",
    "is_current",

    # customer
    "customer_type",
    "agent_type_customer",
    "create_at_bp",

    # channel
    "merchant_channel_type",
    "reg_type",
    "channel_type_id_tele_sale",
    "vds_id_channel_code",
    "vtt_id_channel_code",

    # # bank
    # "viettel_bank_code",
    # "money_sources",
]

# ===================================================
# BUILD SERVICE FEATURES
# ===================================================
def build_service_features(df, prefix=None):
    for col in SELECTED_COLUMNS:
        if 'date' in col:
            df = df.withColumn(col, F.to_date(col))

    df = (
        df
        # ----------- Timeline -----------
        .withColumn("account_age_days",
                    F.datediff(F.to_date(F.lit(SNAPSHOT_DATE_STR)), F.col("actv_date")))
        .withColumn("days_since_update",
                    F.datediff(F.to_date(F.lit(SNAPSHOT_DATE_STR)), F.col("updated_date")))
        .withColumn("days_active_to_update",
                    F.datediff(F.col("updated_date"), F.col("actv_date")))

        # ----------- Authentication -----------
        .withColumn("pin_age_days",
                    F.datediff(F.to_date(F.lit(SNAPSHOT_DATE_STR)), F.col("pin_date")))
        .withColumn("days_active_to_pin",
                    F.datediff(F.col("pin_date"), F.col("actv_date")))

        # ----------- Profile -----------
        .withColumn("days_since_profile_update",
                    F.datediff(F.to_date(F.lit(SNAPSHOT_DATE_STR)), F.col("info_updated_date")))

        # ----------- Bank -----------
        .withColumn("money_source_age_days",
                    F.datediff(F.to_date(F.lit(SNAPSHOT_DATE_STR)), F.col("money_source_linked_date")))
        .withColumn("days_active_to_money_source",
                    F.datediff(F.col("money_source_linked_date"), F.col("actv_date")))

        # ----------- Customer -----------
        .withColumn("has_company",
                    F.col("company_id").isNotNull().cast("int"))
        .withColumn("customer_type_age_days",
                    F.datediff(F.to_date(F.lit(SNAPSHOT_DATE_STR)), F.col("customer_type_date")))

        # ----------- Channel -----------
        .withColumn("has_channel_type",
                    F.col("channel_type").isNotNull().cast("int"))
    )

    feature_cols = [
        c for c in df.columns
        if c not in CROSS_RAW_COLS + PK_COLS
    ]

    return df.select(
        "phone_number",
        *[F.col(c).alias(f"{prefix}_{c}") for c in CROSS_RAW_COLS],
        *[F.col(c).alias(f"{prefix}_{c}") for c in feature_cols],
    )

df_weekly = spark.read.parquet(f"{INPUT_PATH}/date={SNAPSHOT_DATE_STR}").select(*SELECTED_COLUMNS)

df_mm_account_features = build_service_features(df_weekly.filter(F.col("MOTHER_SYSTEM") == "MM").drop("MOTHER_SYSTEM"), prefix='mm')
df_vtp_account_features = build_service_features(df_weekly.filter(F.col("MOTHER_SYSTEM") == "VTP").drop("MOTHER_SYSTEM"), prefix='vtp')

df_account_features = (
    df_mm_account_features
    .join(df_vtp_account_features, "phone_number", "outer")
)

df_account_features.write.mode("overwrite").parquet(f'{OUTPUT_PATH}/date={SNAPSHOT_DATE_STR}')

##### Log to data lineage
from etl.common.utils import log_io_file_path

try:
    log_io_file_path(input_paths=[INPUT_PATH],
                     output_paths=[OUTPUT_PATH],
                    )
except Exception as e:
    logger.error(f"Error log io: {e}")