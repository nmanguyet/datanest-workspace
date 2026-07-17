from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

BASE_DIR = "/apps/jupyter/users/nguyetnguyen/workspace/feature/etl/vnpt/feature"
DEVICE_DIR = f"{BASE_DIR}/device_v3"
PYTHON_BIN = "/apps/anaconda2/envs/notebook-kernel-py3.6.10-pyspark3.0.0/bin/python"

SNAPSHOT_DATE = '{{ (execution_date - macros.timedelta(days=((execution_date.weekday()+1)%7))).in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d") }}'

POOL = "device_features"

args = {
    "owner": "nguyet",
    "retries": 0
}

TASKS = [
    ("build_imei_aggregation_weekly", "0_build_imei_aggregation_weekly.py"),
    ("build_device_imei_features_lxw", "1_build_device_imei_features_lxw.py"),
    ("build_device_tac_features_lxw", "2_build_device_tac_features_lxw.py"),
    ("build_device_current_tac_features_lxw", "3_build_device_current_tac_features_lxw.py"),
    ("build_device_tac_behaviour_features_lxw", "4_build_device_tac_behaviour_features_lxw.py"),
    ("build_device_brand_behaviour_features_lxw", "5_build_device_brand_behaviour_features_lxw.py"),
]

with DAG(
    dag_id="device_features_v3",
    default_args=args,
    start_date=datetime(2021, 11, 2),
    end_date=datetime(2026, 4, 10),
    schedule_interval="0 6 * * WED",
    catchup=True,
    max_active_runs=30,
    tags=["device", "feature"]
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    task_map = {}

    for task_id, script in TASKS:
        task = BashOperator(
            task_id=task_id,
            bash_command=f"""
                cd {DEVICE_DIR} && {PYTHON_BIN} {script} {SNAPSHOT_DATE}
            """,
            pool=POOL
        )
        task_map[task_id] = task

    start >> task_map["build_imei_aggregation_weekly"]

    task_map["build_imei_aggregation_weekly"] >> [
        task_map["build_device_imei_features_lxw"],
        task_map["build_device_tac_features_lxw"],
        task_map["build_device_current_tac_features_lxw"],
    ]

    task_map["build_device_current_tac_features_lxw"] >> [
        task_map["build_device_tac_behaviour_features_lxw"],
        task_map["build_device_brand_behaviour_features_lxw"],
    ]

    [
        task_map["build_device_imei_features_lxw"],
        task_map["build_device_tac_features_lxw"],
        task_map["build_device_tac_behaviour_features_lxw"],
        task_map["build_device_brand_behaviour_features_lxw"],
    ] >> end

if __name__ == "__main__":
    dag.cli()
