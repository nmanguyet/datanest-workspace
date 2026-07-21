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

ACC_DIR = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_account_feature"
BANK_DIR = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_bank_feature"
OUTPUT_DIR = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_acc_bank_feature"

# ACC_DIR = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/feature/weekly/active_vtp_feature/active_vtp_account_feature"
# BANK_DIR = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/feature/weekly/active_vtp_feature/weekly/active_vtp_bank_feature"
# OUTPUT_DIR = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/feature/weekly/active_vtp_feature/weekly/active_vtp_acc_bank_feature"

PHONE_COL = "phone_number"

COMMON_COLS = [
    "core_bank_code",
    "pin_status",
    "mobile_level",
]

# ===================================================
# BUILD CROSS FEATURES
# ===================================================
def build_cross_features(df):
    df_result = (
        df
        # ----------- Service -----------
        .withColumn("has_mm", F.col("mm_mobile_id").isNotNull().cast("int"))
        .withColumn("has_vtp", F.col("vtp_mobile_id").isNotNull().cast("int"))
        .withColumn("num_service", F.col("has_mm") + F.col("has_vtp"))

        # ----------- Timeline -----------
        .withColumn("max_account_age",
                    F.greatest("mm_account_age_days", "vtp_account_age_days"))
        .withColumn("min_account_age",
                    F.least("mm_account_age_days", "vtp_account_age_days"))
        .withColumn("account_age_gap",
                    F.abs(F.col("mm_account_age_days") - F.col("vtp_account_age_days")))
        .withColumn("max_money_source_age",
                    F.greatest("mm_money_source_age_days", "vtp_money_source_age_days"))
        .withColumn("min_money_source_age",
                    F.least("mm_money_source_age_days", "vtp_money_source_age_days"))
        .withColumn("money_source_age_gap",
                    F.abs(F.col("mm_money_source_age_days") - F.col("vtp_money_source_age_days")))
    )
    return df_result


def add_prefix(df, prefix):
    return df.select(
        PHONE_COL,
        *[F.col(c).alias(f"{prefix}_{c}") for c in df.columns if c != PHONE_COL]
    )


# ===================================================
# MAIN
# ===================================================
df_bank_fts = spark.read.parquet(BANK_DIR + f"/date={SNAPSHOT_DATE_STR}")
df_acc_fts = spark.read.parquet(ACC_DIR + f"/date={SNAPSHOT_DATE_STR}")

df_cross = (
    df_bank_fts
    .join(df_acc_fts, "phone_number")
)

df_cross_features = add_prefix(build_cross_features(df_cross), 'vtm_customer')
other_cols = sorted(c for c in df_cross_features.columns if c != PHONE_COL)
df_cross_features = df_cross_features.select(PHONE_COL, *other_cols)

df_cross_features.write.format("parquet").mode("overwrite").parquet(OUTPUT_DIR + f"/date={SNAPSHOT_DATE_STR}")

##### Log to data lineage
from etl.common.utils import log_io_file_path

try:
    log_io_file_path(input_paths=[ACC_DIR, BANK_DIR],
                     output_paths=[OUTPUT_DIR],
                    )
except Exception as e:
    logger.error(f"Error log io: {e}")