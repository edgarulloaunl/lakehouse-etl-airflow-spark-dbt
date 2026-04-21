from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="lakehouse_end_to_end",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # 🔹 1. EXTRACT API → S3
    extract = BashOperator(
        task_id="extract_api_to_s3",
        bash_command="python /opt/airflow/scripts/extract_api_to_s3.py"
    )

    # 🔹 2. LOAD S3 → AUDIT
    load = BashOperator(
        task_id="load_s3_to_audit",
        bash_command="python /opt/airflow/scripts/load_s3_to_audit.py"
    )

    # 🔹 3. QUALITY GATE (BLOQUEANTE)
    validate = BashOperator(
        task_id="validate_quality",
        bash_command="python /opt/airflow/scripts/validate_qualy.py"
    )

    # 🔹 4. SPARK TRANSFORM
    spark = BashOperator(
        task_id="spark_transform",
        bash_command="""
        spark-submit \
        --packages org.postgresql:postgresql:42.6.0 \
        /opt/airflow/scripts/spark_transform.py
        """
    )

    # 🔹 5. DBT MODELS
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt/proyecto_unl && \
        dbt run --profiles-dir /opt/airflow/dbt
        """
    )

    # 🔗 ORQUESTACIÓN
    extract >> load >> validate >> spark >> dbt_run