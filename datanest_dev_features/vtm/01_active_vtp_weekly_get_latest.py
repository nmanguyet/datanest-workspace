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
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

# ===================================================
# CONFIG
# ===================================================
INPUT_PATH = "hdfs://cicdataha/data/vtm/active_vtp"
OUTPUT_PATH = "hdfs://cicdataha/user/tridoan/project/vds_vtm_data_prod/features/active_vtp_feature/weekly/active_vtp_latest"

logger.info(f"Indir: {INPUT_PATH}")
logger.info(f"Outdir: {OUTPUT_PATH}")
logger.info(f"Date: {SNAPSHOT_DATE_STR}")

PHONE_COL = "phone_number"
SERVICE_COL = "MOTHER_SYSTEM"
PARTITION_COL = "date"

week_start = (datetime.strptime(SNAPSHOT_DATE_STR, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")

df = (
    spark.read.parquet(INPUT_PATH)
    .filter((F.col(PARTITION_COL) >= week_start) & (F.col(PARTITION_COL) <= SNAPSHOT_DATE_STR))
    .cache()
)

check_enough_data(df, 7, PARTITION_COL, INPUT_PATH)

## Get latest base on date and update_date
w = Window.partitionBy(PHONE_COL, SERVICE_COL).orderBy(F.col(PARTITION_COL).desc())

df_weekly_snapshot = (
    df
    .where('phone_number IS NOT NULL')
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn", "date")
    .cache()
)

# 1 row / (phone_number, MOTHER_SYSTEM)
assert (
    df_weekly_snapshot.select(PHONE_COL, SERVICE_COL).distinct().count()
    == df_weekly_snapshot.count()
), f"Duplicate ({PHONE_COL}, {SERVICE_COL}) found."

df_weekly_snapshot.repartition(SERVICE_COL).write.mode("overwrite").partitionBy(SERVICE_COL).parquet(f'{OUTPUT_PATH}/date={SNAPSHOT_DATE_STR}')

##### Log to data lineage
from etl.common.utils import log_io_file_path

try:
    log_io_file_path(input_paths=[INPUT_PATH],
                     output_paths=[OUTPUT_PATH],
                     )
except Exception as e:
    logger.error(f"Error log io: {e}")