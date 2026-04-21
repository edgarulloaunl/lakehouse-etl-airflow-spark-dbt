from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import subprocess


def check_quality():
    try:
        result = subprocess.run(
            ["python", "/opt/airflow/scripts/validate_qualy.py"],
            capture_output=True,
            text=True
        )

        print(result.stdout)

        if result.returncode == 0:
            return "spark_transform"
        else:
            return "quality_failed"

    except Exception as e:
        print(str(e))
        return "quality_failed"


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

    # 🔹 1. EXTRACT
    extract = BashOperator(
        task_id="extract_api_to_s3",
        bash_command="python /opt/airflow/scripts/extract_api_to_s3.py"
    )

    # 🔹 2. LOAD
    load = BashOperator(
        task_id="load_s3_to_audit",
        bash_command="python /opt/airflow/scripts/load_s3_to_audit.py"
    )

    # 🔹 3. BRANCH QUALITY
    quality_check = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=check_quality
    )

    # 🔹 4A. SI PASA → SPARK
    spark = BashOperator(
        task_id="spark_transform",
        bash_command="""
        spark-submit \
        --packages org.postgresql:postgresql:42.6.0 \
        /opt/airflow/scripts/spark_transform.py
        """
    )

    # 🔹 5A. DBT
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt/proyecto_unl && \
        dbt run --profiles-dir /opt/airflow/dbt
        """
    )

    # 🔹 4B. SI FALLA → LOG CONTROLADO
    quality_failed = BashOperator(
        task_id="quality_failed",
        bash_command='echo "Quality Gate falló. Pipeline detenido correctamente."'
    )

    # 🔹 FINES
    end_success = EmptyOperator(task_id="end_success")
    end_failed = EmptyOperator(task_id="end_failed")

    # 🔗 FLUJO
    extract >> load >> quality_check

    quality_check >> spark >> dbt_run >> end_success
    quality_check >> quality_failed >> end_failed