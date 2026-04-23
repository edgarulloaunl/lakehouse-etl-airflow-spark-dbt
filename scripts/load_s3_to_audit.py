import os
import json
import boto3
import psycopg2
import uuid

def main():

    print("Leyendo archivo desde S3...")

    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    bucket = os.getenv("RAW_BUCKET")

    response = s3.list_objects_v2(Bucket=bucket)

    latest_file = sorted(
        response['Contents'],
        key=lambda x: x['LastModified'],
        reverse=True
    )[0]['Key']

    print(f"Archivo encontrado: {latest_file}")

    obj = s3.get_object(Bucket=bucket, Key=latest_file)

    lines = obj['Body'].read().decode('utf-8').splitlines()
    data = [json.loads(line) for line in lines if line.strip()]

    print(f"Registros a cargar: {len(data)}")

    conn = psycopg2.connect(
        host="host.docker.internal",
        port="5433",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        dbname=os.getenv("DB_NAME")
    )

    cur = conn.cursor()

    insertados = 0
    descartados = 0

    for row in data:

        transaction_id = row.get("id")

        # SOLUCIÓN: evitar NULL
        if not transaction_id:
            # opción 1: generar id automático
            transaction_id = str(uuid.uuid4())

        user_id = row.get("user_id")
        amount = row.get("amount")

        try:
            cur.execute("""
                INSERT INTO audit.raw_transactions (transaction_id, user_id, amount)
                VALUES (%s, %s, %s)
            """, (transaction_id, user_id, amount))

            insertados += 1

        except Exception as e:
            descartados += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Insertados: {insertados}")
    print(f"Descartados: {descartados}")
    print("Carga finalizada")


if __name__ == "__main__":
    main()