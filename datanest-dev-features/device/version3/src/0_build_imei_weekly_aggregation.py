import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import functions as F

BASE_DIR = Path("/apps/jupyter/users/nguyetnguyen/workspace/feature")

sys.path[:0] = [str(path) for path in [
    BASE_DIR,
    BASE_DIR / "etl/vnpt/feature/device_v3",
]]

import config
from etl.common import init_spark3

spark = init_spark3.setup(
    job_cfg={
        'executor.instances': 8,
        'executor.cores': 8,
        'executor.memory': '20g',
    },
    script_name="build_imei_weekly_aggregation"
)
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")


def main():
    snapshot_date_str = sys.argv[1]
    start_time = time.time()

    snapshot_date = datetime.strptime(snapshot_date_str, config.DATE_FORMAT)
    last_week_str = datetime.strftime(snapshot_date - timedelta(6), config.DATE_FORMAT)

    out_path = f'{config.IMEI_WEEKLY_PATH}/date={snapshot_date_str}'

    print("Extraction date, last week ", snapshot_date_str, last_week_str)

    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')

    df_sms = (
        spark.read.format('delta').load(config.IMEI_SMS_RAW_PATH)
        .where(f"date between '{last_week_str}' and '{snapshot_date_str}'")
        .selectExpr('phone_number', 'imei', 'tac', 'date')
    )

    df_voice = (
        spark.read.format('delta').load(config.IMEI_VOICE_RAW_PATH)
        .where("date not in ('2022-11-16', '2023-01-07', '2023-08-03')")
        .where(f"date between '{last_week_str}' and '{snapshot_date_str}'")
        .selectExpr('phone_number', 'imei', 'tac', 'date')
    )

    df_sample = spark.read.parquet('')

    # df_imei_weekly = df_sms.unionByName(df_voice).dropDuplicates()
    df_imei_weekly = (df_sms
                      .unionByName(df_voice)
                      .join(df_sample, 'phone_number')
                      .dropDuplicates()
    )

    df_imei_weekly_agg = (
        df_imei_weekly
        .groupBy('phone_number', 'imei', 'tac')
        .agg(F.countDistinct('date').alias('device_num_day_l1w'))
    )

    print('out_path', out_path)
    df_imei_weekly_agg.write.mode("overwrite").parquet(out_path)

    end_time = time.time()
    print(f'Done at: {datetime.now()} during {end_time - start_time}')
    print('-' * 50)


if __name__ == "__main__":
    main()
