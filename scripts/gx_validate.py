import pandas as pd
import psycopg2
import os
from datetime import datetime

# Conexión DB
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME")
)

# Leer datos
df = pd.read_sql("SELECT * FROM audit.raw_transactions", conn)

total = len(df)

# Reglas de calidad
failed = 0
errors = []

# Regla 1: amount >= 0
negativos = df[df["amount"] < 0]
if len(negativos) > 0:
    failed += len(negativos)
    errors.append("amount negativos")

# Regla 2: user_id NOT NULL
nulos = df[df["user_id"].isnull()]
if len(nulos) > 0:
    failed += len(nulos)
    errors.append("user_id nulos")

success_rate = ((total - failed) / total) * 100 if total > 0 else 100

print(f"Total: {total}")
print(f"Errores: {failed}")
print(f"Success Rate: {success_rate:.2f}%")

# Guardar métricas
cur = conn.cursor()

cur.execute("""
INSERT INTO audit.gx_validation_logs
(validation_timestamp, table_name, total_records, failed_records, success_rate, critical_failures)
VALUES (%s, %s, %s, %s, %s, %s)
""", (
    datetime.now(),
    "raw_transactions",
    total,
    failed,
    success_rate,
    ", ".join(errors)
))

conn.commit()
cur.close()
conn.close()

# Control del flujo
if failed > 0:
    exit(1)
else:
    exit(0)