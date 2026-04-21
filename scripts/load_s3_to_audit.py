import os
import json
import boto3
import psycopg2

RAW_BUCKET = os.getenv("RAW_BUCKET")

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def get_latest_file():
    s3 = get_s3_client()

    response = s3.list_objects_v2(Bucket=RAW_BUCKET)

    if "Contents" not in response:
        raise Exception("No hay archivos en S3")

    files = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)

    return files[0]["Key"]


def load_to_db(records):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE audit.raw_transactions")

    for r in records:
        cur.execute("""
            INSERT INTO audit.raw_transactions
            (transaction_id, user_id, product_category, amount, currency, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            r.get("transaction_id"),
            r.get("user_id"),
            r.get("product_category"),
            r.get("amount"),
            r.get("currency"),
            r.get("status")
        ))

    conn.commit()
    cur.close()
    conn.close()


def main():
    print("Leyendo archivo desde S3...")

    s3 = get_s3_client()
    key = get_latest_file()

    print(f"Archivo encontrado: {key}")

    obj = s3.get_object(Bucket=RAW_BUCKET, Key=key)
    content = obj["Body"].read().decode("utf-8")

    lines = content.splitlines()

    records = []

    for line in lines:
        if not line.strip():
            continue  # 🔥 ignora líneas vacías

        try:
            obj = json.loads(line)

            if isinstance(obj, dict):
                records.append(obj)
            else:
                print(f"Línea ignorada (no es dict): {line}")

        except Exception as e:
            print(f"Error parseando línea: {line} → {e}")

    print(f"Registros a cargar: {len(records)}")

    load_to_db(records)

    print("Carga completada en audit.raw_transactions")


if __name__ == "__main__":
    main()