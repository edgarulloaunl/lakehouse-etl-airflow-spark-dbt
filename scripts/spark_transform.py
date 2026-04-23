import os
from pyspark.sql import SparkSession

def main():

    print("Iniciando Spark...")

    os.environ["PYSPARK_SUBMIT_ARGS"] = \
        "--packages org.postgresql:postgresql:42.6.0 pyspark-shell"

    spark = SparkSession.builder \
        .appName("ETL Transform") \
        .getOrCreate()

    print("Leyendo datos desde PostgreSQL...")

    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/db_warehouse") \
        .option("dbtable", "audit.raw_transactions") \
        .option("user", "user_dbt") \
        .option("password", "password_dbt") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print("Datos cargados")
    df.show(5)

    df_clean = df.filter(df.amount >= 0)

    print("Guardando resultados...")

    df_clean.write.format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/db_warehouse") \
        .option("dbtable", "analytics.cleaned_transactions") \
        .option("user", "user_dbt") \
        .option("password", "password_dbt") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("Proceso completado")

    spark.stop()


if __name__ == "__main__":
    main()