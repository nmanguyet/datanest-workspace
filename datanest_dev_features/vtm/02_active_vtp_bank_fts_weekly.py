from datetime import datetime, timedelta
import logging
import sys
from pyspark.sql import Window
from pyspark.sql import functions as F

from etl.common.logger import config_log
from etl.common import date_util, utils, init_spark3
from etl.viettel.common.utils import check_enough_data

config_log()
logger = logging.getLogger(__name__)

SNAPSHOT_DATE_STR = sys.argv[1]

# Check is sunday
date_util.check_is_sunday(SNAPSHOT_DATE_STR)

script_name = utils.get_spark_script_name()
spark = init_spark3.setup(
    job_cfg={
        "executor.instances": 8,
        "executor.cores": 5,
        "executor.memory": '20g'
    },
    script_name=script_name
)

INPUT_PATH = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_latest"
BANK_MAP_PATH = "hdfs://cicdataha/project/vds_vtm_data_prod/processed/bank_code_map"
OUTPUT_PATH = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_bank_feature"

PK_COLS = ["phone_number", "MOTHER_SYSTEM"]

BANK_MAPPING_CONFIG = {
    "bank_group": [
        "State-Owned Commercial Bank", "State-Owned Limited Liability Bank",
        "Wholly Foreign-Owned Bank", "Non-Bank Financial Institution",
        "Joint Stock Commercial Bank", "Joint Venture Bank",
    ],
    "tier": ["Large", "Medium", "Small"],
}

BANK_TYPES = {
    "bank": [
        "State-Owned Commercial Bank", "State-Owned Limited Liability Bank",
        "Wholly Foreign-Owned Bank", "Joint Stock Commercial Bank", "Joint Venture Bank",
    ],
    "finance": ["Non-Bank Financial Institution"],
}


def add_prefix(df, prefix):
    return df.select(
        "phone_number",
        *[F.col(c).alias(f"{prefix}_{c}") for c in df.columns if c not in PK_COLS]
    )


def slug(value: str) -> str:
    """Turn a free-text label into a safe snake_case column suffix.

    Args:
        value: Raw label, e.g. 'State-Owned Commercial Bank'.

    Returns:
        e.g. 'state_owned_commercial_bank'.
    """
    return value.lower().replace("-", "_").replace(" ", "_")


def clean_bank_code_column(colname: str) -> F.Column:
    """Parse a comma-separated bank code column, e.g. 'VARB#VARB,TCB#TCB,,STB'.

    Takes the segment before '#' in each token (money_sources uses a
    duplicated 'code#code' format), uppercases, trims, drops empty tokens.

    Args:
        colname: Name of the source column.

    Returns:
        Column expression: array of cleaned bank code strings.
    """
    return F.expr(f"""
        array_distinct(
            filter(
                transform(
                    split(coalesce({colname}, ''), ','),
                    x -> upper(trim(element_at(split(x, '#'), 1)))
                ),
                x -> x <> ''
            )
        )
    """)


def build_bank_features(df):
    agg = [
        # row
        F.first("viettel_bank_code", ignorenulls=True).alias("viettel_bank_code"),
        F.first("core_bank_code", ignorenulls=True).alias("core_bank_code"),

        # overall
        F.countDistinct("bank_code").alias("distinct_bank_code"),
        F.countDistinct("bank_group").alias("distinct_bank_group"),
        F.countDistinct("tier").alias("distinct_bank_tier"),
    ]

    # ------------- Summary -------------
    agg += [
        F.countDistinct(
            F.when(
                F.lower(F.col("bank_group")).isin(*[g.lower() for g in groups]),
                F.col("bank_code"),
            )
        ).alias(f"num_distinct_{feature}")
        for feature, groups in BANK_TYPES.items()
    ]

    # ------------- Mapping -------------
    for col_name, values in BANK_MAPPING_CONFIG.items():
        agg += [
            F.countDistinct(
                F.when(
                    F.lower(F.col(col_name)) == value.lower(),
                    F.col("bank_code"),
                )
            ).alias(f"num_distinct_{slug(value)}")
            for value in values
        ]

    return df.groupBy("phone_number").agg(*agg)


# ===================================================
# MAIN
# ===================================================
df_raw = spark.read.parquet(f"{INPUT_PATH}/date={SNAPSHOT_DATE_STR}")
df_bank_map = (
    spark.read.parquet(BANK_MAP_PATH)
    .withColumnRenamed("bank_type", "bank_group")
    .withColumnRenamed("bank_size", "tier")
)

#### Clean bank_code (money_sources), method (money_source_table)
df_extract_bank_code = (
    df_raw
    .where("phone_number IS NOT NULL")
    .select("phone_number", "MOTHER_SYSTEM", "money_sources", "money_source_table")
    ### Extract and clean the bank/finance code from money_sources
    .withColumn("bank_code_split", clean_bank_code_column("money_sources"))
    ### Extract and clean the method code from money_source_table
    .withColumn("mn_src_tbl_arr", F.array_distinct(F.expr("filter(split(money_source_table, ','), x-> x <> '')")))
    .withColumn("distinct_method", F.size(F.col("mn_src_tbl_arr")))
    .withColumn("bank_code", F.explode("bank_code_split"))
    .select("phone_number", "MOTHER_SYSTEM", "bank_code", "mn_src_tbl_arr", "distinct_method")
)

df_extract_bank = (
    df_extract_bank_code
    .drop("distinct_method")
    .join(F.broadcast(df_bank_map), "bank_code", "left")
)

df_distinct_method = (
    df_extract_bank_code
    .select("phone_number", "MOTHER_SYSTEM", "distinct_method")
    .distinct()
)

df_input = (
    df_raw
    .join(df_extract_bank, ["phone_number", "MOTHER_SYSTEM"], "left")
)

#### End extract bank code for input
df_mm_bank_features = add_prefix(
    build_bank_features(
        df_input.filter(F.col("MOTHER_SYSTEM") == "MM").drop("MOTHER_SYSTEM")
    )
    .join(df_distinct_method.where("MOTHER_SYSTEM = 'MM'").drop("MOTHER_SYSTEM"), "phone_number", "left")
    ,
    "mm",
)

df_vtp_bank_features = add_prefix(
    build_bank_features(
        df_input.filter(F.col("MOTHER_SYSTEM") == "VTP").drop("MOTHER_SYSTEM")
    )
    .join(df_distinct_method.where("MOTHER_SYSTEM = 'VTP'").drop("MOTHER_SYSTEM"), "phone_number", "left")
    ,
    "vtp",
)

df_bank_fts_result = (
    df_mm_bank_features
    .join(df_vtp_bank_features, "phone_number", "outer")
)

df_bank_fts_result.write.mode("overwrite").parquet(OUTPUT_PATH + f"/date={SNAPSHOT_DATE_STR}")
logger.info("Writed to output!")

##### Log to data lineage
from etl.common.utils import log_io_file_path

try:
    log_io_file_path(input_paths=[INPUT_PATH, BANK_MAP_PATH],
                     output_paths=[OUTPUT_PATH],
                    )
except Exception as e:
    logger.error(f"Error log io: {e}")