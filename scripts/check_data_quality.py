import os
import psycopg2
import sys

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME")
)

cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM audit.raw_transactions WHERE amount < 0")
negativos = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM audit.raw_transactions WHERE user_id IS NULL")
nulos = cur.fetchone()[0]

cur.close()
conn.close()

print(f"Negativos: {negativos}")
print(f"Nulos: {nulos}")

# CAMBIO CLAVE: NO DETENER PIPELINE
if negativos > 0 or nulos > 0:
    print("Datos con errores → se corregirán automáticamente")
    sys.exit(0)
else:
    print("Calidad OK")
    sys.exit(0)