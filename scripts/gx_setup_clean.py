import great_expectations as gx

context = gx.get_context()

# Crear datasource (si no existe)
try:
    datasource = context.sources.add_postgres(
        name="postgres_warehouse",
        connection_string="postgresql+psycopg2://user_dbt:password_dbt@host.docker.internal:5433/db_warehouse"
    )
except Exception:
    datasource = context.get_datasource("postgres_warehouse")

# 🔥 IMPORTANTE: definir schema separado
try:
    datasource.add_table_asset(
        name="raw_transactions",
        table_name="raw_transactions",
        schema_name="audit"
    )
    print("Asset creado correctamente")
except Exception as e:
    print("Asset ya existe o error controlado:", e)