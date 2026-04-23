import great_expectations as gx
from great_expectations.dataset import PandasDataset
import pandas as pd
import psycopg2

# conexión
conn = psycopg2.connect(
    host="host.docker.internal",
    port="5433",
    user="user_dbt",
    password="password_dbt",
    dbname="db_warehouse"
)

df = pd.read_sql("SELECT * FROM audit.raw_transactions", conn)
conn.close()

print("Filas cargadas:", len(df))

# GX dataset
dataset = PandasDataset(df)

# reglas
dataset.expect_column_values_to_not_be_null("transaction_id")
dataset.expect_column_values_to_not_be_null("amount")
dataset.expect_column_values_to_be_between("amount", min_value=0)

# validar
results = dataset.validate()

print("Resultado GX:", results["success"])

# 🔥 GUARDAR EN FORMATO QUE GX ENTIENDE
import os

ruta = "/opt/airflow/great_expectations/uncommitted/validations/transactions_suite/run_1"

os.makedirs(ruta, exist_ok=True)

with open(f"{ruta}/validation.json", "w") as f:
    f.write(str(results.to_json_dict()))

print("Validación registrada correctamente")