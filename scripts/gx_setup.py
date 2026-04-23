import great_expectations as gx

context = gx.get_context()

# Crear datasource
datasource = context.sources.add_postgres(
    name="postgres_warehouse",
    connection_string="postgresql+psycopg2://user_dbt:password_dbt@host.docker.internal:5433/db_warehouse"
)

# 🔥 CREAR ASSET (ESTO TE FALTABA)
asset = datasource.add_table_asset(
    name="raw_transactions",
    table_name="audit.raw_transactions"
)

print("Datasource y asset creados")