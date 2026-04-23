from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id='lakehouse_end_to_end',
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract = BashOperator(
        task_id="extract_api_to_s3",
        bash_command="python /opt/airflow/scripts/extract_api_to_s3.py"
    )

    load = BashOperator(
        task_id="load_s3_to_audit",
        bash_command="python /opt/airflow/scripts/load_s3_to_audit.py"
    )

    quality = BashOperator(
        task_id="quality_check",
        bash_command="python /opt/airflow/scripts/check_data_quality.py"
    )

    fix_data = BashOperator(
        task_id="fix_data_quality",
        bash_command="python /opt/airflow/scripts/fix_data_quality.py"
    )

    recheck = BashOperator(
        task_id="recheck_quality",
        bash_command="python /opt/airflow/scripts/check_data_quality.py"
    )

    spark = BashOperator(
        task_id="spark_transform",
        bash_command="python /opt/airflow/scripts/spark_transform.py"
    )

    dbt = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/proyecto_unl && dbt run"
    )

    end = EmptyOperator(task_id="end")

    extract >> load >> quality >> fix_data >> recheck >> spark >> dbt >> end