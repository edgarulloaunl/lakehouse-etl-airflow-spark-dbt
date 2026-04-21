import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def main():
    print("Iniciando Spark...")

    spark = SparkSession.builder \
        .appName("ETL Spark Transform") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    print("Leyendo datos desde audit.raw_transactions...")

    df = spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "audit.raw_transactions") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print(f"Registros originales: {df.count()}")

    # 🔥 LIMPIEZA
    df_clean = df \
        .filter(col("amount") >= 0) \
        .dropna(subset=["transaction_id", "user_id", "amount"]) \
        .dropDuplicates(["transaction_id"])

    print(f"Registros limpios: {df_clean.count()}")

    print("Guardando en prod.transactions_clean...")

    df_clean.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "prod.transactions_clean") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Transformación completada")

    spark.stop()


if __name__ == "__main__":
    main()