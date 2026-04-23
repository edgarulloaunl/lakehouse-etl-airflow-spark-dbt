import os
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME")
)

cur = conn.cursor()

print("Corrigiendo datos...")

# Corregir negativos
cur.execute("""
    UPDATE audit.raw_transactions
    SET amount = 0
    WHERE amount < 0
""")

# Corregir nulos
cur.execute("""
    UPDATE audit.raw_transactions
    SET user_id = 0
    WHERE user_id IS NULL
""")

conn.commit()
cur.close()
conn.close()

print("Datos corregidos")