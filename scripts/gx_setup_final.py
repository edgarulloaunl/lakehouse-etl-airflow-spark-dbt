import great_expectations as gx
import pandas as pd
import psycopg2

# 1. Conectar a PostgreSQL (igual que ya te funciona)
conn = psycopg2.connect(
    host="host.docker.internal",
    port="5433",
    user="user_dbt",
    password="password_dbt",
    dbname="db_warehouse"
)

# 2. Leer datos
df = pd.read_sql("SELECT * FROM audit.raw_transactions", conn)

conn.close()

print(f"Filas cargadas: {len(df)}")

# 3. Crear contexto GX
context = gx.get_context()

# 4. Crear suite si no existe
try:
    context.add_expectation_suite("transactions_suite")
except:
    pass

# 5. Crear validator con Pandas
validator = context.get_validator(
    batch_data=df,
    expectation_suite_name="transactions_suite"
)

# 6. Reglas de calidad
validator.expect_column_values_to_not_be_null("transaction_id")
validator.expect_column_values_to_not_be_null("amount")
validator.expect_column_values_to_be_between("amount", min_value=0)

# 7. Guardar suite
validator.save_expectation_suite()

# 8. Ejecutar validación
results = validator.validate()

print("Resultado GX:", results["success"])