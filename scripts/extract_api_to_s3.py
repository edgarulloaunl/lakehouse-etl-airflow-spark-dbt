import os
import json
import time
import requests
import boto3
from datetime import datetime

API_BASE_URL = os.getenv("API_BASE_URL")
API_KEY = os.getenv("API_KEY")

RAW_BUCKET = os.getenv("RAW_BUCKET")

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def extract_data():
    page = 1
    data_all = []

    MAX_PAGES = 20  # 🔥 límite de seguridad

    while page <= MAX_PAGES:
        retries = 0

        while retries < 5:
            try:
                print(f"Consultando página {page}... intento {retries + 1}")

                response = requests.get(
                    f"{API_BASE_URL}/transactions",
                    params={"page": page, "limit": 100},
                    headers={"x-api-key": API_KEY},
                    timeout=30
                )

                # 🔴 RATE LIMIT
                if response.status_code == 429:
                    print(f"Rate limit en página {page}, intento {retries + 1}")
                    retries += 1

                    if retries >= 3:
                        print(f"Demasiados intentos en página {page}, saltando...")
                        page += 1
                        break

                    time.sleep(15)
                    continue

                # 🔴 ERROR 500
                if response.status_code == 500:
                    print(f"Error 500 en página {page}, saltando...")
                    page += 1
                    break

                # 🔴 OTROS ERRORES
                if response.status_code != 200:
                    print(f"Error HTTP: {response.status_code}")
                    return data_all

                json_response = response.json()

                data = json_response.get("data", [])

                print(f"Registros en página {page}: {len(data)}")

                # 🔚 FIN REAL
                if not data:
                    print("No hay más datos")
                    return data_all

                data_all.extend(data)

                # 🐢 CONTROL DE VELOCIDAD
                time.sleep(1)

                page += 1
                break

            except Exception as e:
                print(f"Error: {e}")
                retries += 1
                time.sleep(min(2 ** retries, 30))

        else:
            print(f"Falló completamente en página {page}")
            page += 1

    print("Se alcanzó el límite máximo de páginas")
    return data_all


def upload_to_s3(data):
    s3 = get_s3_client()

    key = f"transactions/{datetime.utcnow().strftime('%Y/%m/%d')}/data_{int(time.time())}.json"

    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in data)

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=body.encode("utf-8")
    )

    print(f"Archivo subido a S3: {key}")


def main():
    print("Extrayendo datos...")

    data = extract_data()

    print(f"Registros obtenidos: {len(data)}")

    if data:
        upload_to_s3(data)
    else:
        print("No se obtuvieron datos, no se sube archivo.")


if __name__ == "__main__":
    main()