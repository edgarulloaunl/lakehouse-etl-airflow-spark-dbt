import os
import pandas as pd
import psycopg2
from great_expectations.dataset import PandasDataset

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")


class GXDataset(PandasDataset):
    pass


def get_data():
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

    query = "SELECT * FROM audit.raw_transactions"

    df = pd.read_sql(query, conn)

    conn.close()

    return GXDataset(df)


def run_validation():
    print("Ejecutando validaciones...")

    df = get_data()

    results = []

    # 🔥 EXPECTATIONS (REGLAS)

    results.append(df.expect_column_values_to_not_be_null("transaction_id"))
    results.append(df.expect_column_values_to_not_be_null("user_id"))
    results.append(df.expect_column_values_to_not_be_null("amount"))

    results.append(df.expect_column_values_to_be_in_set(
        "status",
        ["COMPLETED", "PENDING", "FAILED"]
    ))

    results.append(df.expect_column_values_to_be_between(
        "amount",
        min_value=0,
        max_value=100000
    ))

    results.append(df.expect_column_values_to_be_unique("transaction_id"))

    # 🔴 QUALITY GATE
    all_passed = True

    for i, r in enumerate(results):
        if not r["success"]:
            print(f"❌ Regla {i+1} falló:")
            print(r)
            all_passed = False

    print(f"Resultado global: {all_passed}")

    if not all_passed:
        raise Exception("❌ Quality Gate falló")

    print("✅ Quality Gate aprobado")


if __name__ == "__main__":
    run_validation()