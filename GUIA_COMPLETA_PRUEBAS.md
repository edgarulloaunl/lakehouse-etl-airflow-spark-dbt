# Guía Completa de Pruebas - Proyecto ETL Master (WAP + Spark + GX + dbt)

## Tabla de Contenidos
1. [Arquitectura del Proyecto](#1-arquitectura-del-proyecto)
2. [Levantar el Entorno](#2-levantar-el-entorno)
3. [Fase 1: WRITE - Extracción de Datos](#3-fase-1-write---extracción-de-datos)
4. [Fase 2: MONITOR - Frescura de Datos](#4-fase-2-monitor---frescura-de-datos)
5. [Fase 3: AUDIT - Validación con Great Expectations](#5-fase-3-audit---validación-con-great-expectations)
6. [Fase 4: PUBLISH - Transformación con Spark](#6-fase-4-publish---transformación-con-spark)
7. [Fase 5: DBT - Modelado Analítico](#7-fase-5-dbt---modelado-analítico)
8. [Fase 6: LINEAGE - Registro de Linaje](#8-fase-6-lineage---registro-de-linaje)
9. [Consultas de Verificación](#9-consultas-de-verificación)
10. [Herramientas Adicionales](#10-herramientas-adicionales)
11. [Troubleshooting](#11-troubleshooting)
12. [Resumen Rápido de Comandos](#12-resumen-rápido-de-comandos)

---

## 1. Arquitectura del Proyecto

### Patrón WAP (Write-Audit-Publish)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   WRITE     │────▶│   AUDIT     │────▶│  PUBLISH    │────▶│    DBT      │
│  (API→audit)│     │ (GX checks) │     │(Spark→prod) │     │ (models)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │
                     Si FALLA → Se detiene el pipeline
```

### Flujo de Datos

```
API REST (simulada)
  │  [Extracción paginada con retry exponencial]
  ▼
audit.raw_transactions          ← Zona de cuarentena (datos crudos, sin validar)
  │  [Great Expectations valida 8 expectativas]
  ▼
prod.raw_transactions           ← Zona validada (Spark: filtra, deduplica, enriquece)
  │  [dbt estandariza tipos y agrega tiers]
  ▼
public_staging.stg_raw_transactions      ← Vista staging
  │  [dbt enmascara PII para dev]
  ▼
public_development.stg_masked_transactions  ← Vista enmascarada (desarrollo)
  │
  │  [dbt enriquece con métricas históricas]
  ▼
public_intermediate.int_transactions_enriched  ← Vista intermedia
  │
  ├─────────────────────────────────────┐
  ▼                                     ▼
public_analytics.fct_transactions    public_analytics.dim_users
(Tabla de hechos)                    (Dimensión RFM)
  │
  ▼
public_analytics.analytics_transactions
(Vista pseudonimizada para dashboards/BI)
```

### Servicios Docker

| Servicio | Puerto Host | Puerto Container | Descripción |
|----------|-------------|------------------|-------------|
| **Airflow Webserver** | `8080` | `8080` | Interfaz web del orquestador |
| **PostgreSQL (metadatos)** | — | `5432` | Metadatos de Airflow |
| **PostgreSQL (warehouse)** | `5433` | `5432` | Base de datos de negocio |

### Credenciales

| Recurso | Usuario | Contraseña |
|---------|---------|------------|
| Airflow UI | `airflow` | `airflow` |
| Warehouse DB | `user_dbt` | `password_dbt` |

> **⚠️ Nota importante sobre schemas de dbt:**
> dbt ante-pone el schema base definido en `profiles.yml` (`public`) al schema declarado en cada modelo. Los schemas reales en PostgreSQL son:
>
> | Schema del modelo | Schema real en PostgreSQL |
> |-------------------|--------------------------|
> | `staging` | `public_staging` |
> | `development` | `public_development` |
> | `intermediate` | `public_intermediate` |
> | `analytics` | `public_analytics` |
>
> **Siempre usa el nombre completo** (`public_staging`, `public_analytics`, etc.) en tus consultas SQL.

> **⚠️ Nota sobre sesiones psql:**
> Las sesiones `psql` mantienen un caché de schemas. Si ejecutas dbt mientras tienes una sesión psql abierta, la sesión no verá las nuevas vistas/tablas creadas. **Siempre abre una nueva conexión psql** después de ejecutar dbt run.

---

## 2. Levantar el Entorno

### 2.1 Construir y levantar todos los servicios

```bash
cd c:\Proyecto_IA_UNL\taller_completo_spark_GX_WAP\taller_etl_master
docker compose up -d --build
```

**Qué ocurre:**
- Se construye la imagen de Airflow con las dependencias del `Dockerfile` (PySpark 3.3.0, dbt 1.5.0, Great Expectations 0.17.15, pandas, cryptography, etc.)
- Se levantan 5 contenedores: `postgres` (metadatos), `postgres_warehouse` (datos), `airflow-init`, `airflow-webserver`, `airflow-scheduler`
- El servicio `airflow-init` inicializa la base de datos de metadatos de Airflow y crea el usuario admin
- El script `init_db/setup_wap.sql` se ejecuta **solo la primera vez** que se crea el contenedor `postgres_warehouse`. Crea:
  - **Tablas del schema `audit`:**
    - `raw_transactions` — Zona de cuarentena para datos crudos
    - `gx_validation_logs` — Registro de validaciones GX
    - `data_freshness_monitor` — Monitoreo de frescura de datos
    - `pipeline_metrics` — Métricas de ejecución del pipeline
    - `schema_history` — Historial de cambios de esquema (drift detection)
    - `data_lineage` — Registro de linaje operacional
    - `pii_catalog` — Catálogo de columnas PII
    - `user_encryption_keys` — Claves de cifrado por usuario
    - `data_deletion_requests` — Solicitudes de eliminación (GDPR/LDPD)
  - **Tabla del schema `prod`:** `raw_transactions` — Datos validados
  - **Vistas:** `vw_freshness_dashboard`, `vw_data_quality_dashboard`, `vw_data_lineage`, `vw_pii_catalog`, `v_masked_transactions` (audit y prod), `v_analytics_transactions`
  - **Función:** `pseudonymize_user(INT)` — Pseudonimización con salt
- El volumen `./scripts:/opt/airflow/scripts` monta los scripts auxiliares directamente en el contenedor (ya no es necesario `docker cp`)

**Resultado esperado:**
```
[+] Running 5/5
 ✔ Container taller_etl_master-postgres-1            Running
 ✔ Container taller_etl_master-postgres_warehouse-1  Running
 ✔ Container taller_etl_master-airflow-init-1        Exited
 ✔ Container taller_etl_master-airflow-webserver-1   Running
 ✔ Container taller_etl_master-airflow-scheduler-1   Running
```

### 2.2 Verificar que los servicios están saludables

```bash
docker compose ps
```

Todos los servicios deben estar en estado `Up` (excepto `airflow-init` que debe estar `Exited`).

### 2.3 Acceder a la UI de Airflow

Abrir en el navegador: **http://localhost:8080**
- Usuario: `airflow`
- Contraseña: `airflow`

Una vez dentro, verás el DAG `dag_wap_unl_final` en la lista.

### 2.4 Acceder a la base de datos warehouse

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "PGPASSWORD=password_dbt psql -h postgres_warehouse -U user_dbt -d db_warehouse"
```

### 2.5 Verificar que las tablas y función iniciales existen

Desde la sesión psql:

```sql
-- Tablas del schema audit (deben ser 9)
SELECT tablename FROM pg_tables WHERE schemaname = 'audit' ORDER BY tablename;
```

Deberías ver: `data_deletion_requests`, `data_freshness_monitor`, `data_lineage`, `gx_validation_logs`, `pipeline_metrics`, `pii_catalog`, `raw_transactions`, `schema_history`, `user_encryption_keys`.

```sql
-- Tabla del schema prod
SELECT tablename FROM pg_tables WHERE schemaname = 'prod' ORDER BY tablename;
```

Deberías ver: `raw_transactions`.

```sql
-- Función de pseudonimización
SELECT routine_name FROM information_schema.routines WHERE routine_name = 'pseudonymize_user';
```

Deberías ver: `pseudonymize_user`.

---

## 3. Fase 1: WRITE - Extracción de Datos

### Qué ocurre

El DAG ejecuta la tarea `write_to_audit` que:
1. Se conecta a la API REST simulada (`API_BASE_URL/api/transactions`)
2. Solicita datos paginados (100 registros por página) con **retry exponencial** ante errores 429/500 (hasta 5 intentos, backoff de 2s a 30s)
3. Trunca la tabla `audit.raw_transactions` (limpia ejecuciones anteriores)
4. Inserta cada registro crudo en `audit.raw_transactions`
5. Registra métricas: registros insertados, errores, páginas procesadas

### 3.1 Ejecutar el DAG completo

**Opción A - Desde la UI de Airflow:**
- Ir a http://localhost:8080
- Click en el DAG `dag_wap_unl_final`
- Click en **Trigger DAG** (botón ▶️)

**Opción B - Desde CLI:**
```bash
docker exec -it taller_etl_master-airflow-webserver-1 airflow dags trigger dag_wap_unl_final
```

### 3.2 Verificar la extracción

```sql
SELECT COUNT(*) as total_registros FROM audit.raw_transactions;
```

### 3.3 Ver la muestra de datos crudos

```sql
SELECT transaction_id, user_id, product_category, amount, currency, status
FROM audit.raw_transactions
LIMIT 5;
```

---

## 4. Fase 2: MONITOR - Frescura de Datos

### Qué ocurre

La tarea `monitor_freshness` corre en paralelo con `write_to_audit`:
1. Consulta el `MAX(ingested_at)` de las últimas 24h
2. Calcula el lag en horas
3. Clasifica: `FRESH` (<2h), `STALE` (<6h), `MISSING` (>6h)
4. Registra en `audit.data_freshness_monitor`

### 4.1 Verificar el monitoreo de frescura

```sql
SELECT table_name, actual_arrival_time, freshness_lag, status
FROM audit.data_freshness_monitor
ORDER BY actual_arrival_time DESC
LIMIT 5;
```

### 4.2 Dashboard de frescura

```sql
SELECT * FROM audit.vw_freshness_dashboard LIMIT 5;
```

---

## 5. Fase 3: AUDIT - Validación con Great Expectations

### Qué ocurre

La tarea `audit_with_gx` ejecuta **dos suites** en cascada sobre `audit.raw_transactions`:

#### Suite CRÍTICO (bloqueante) — 6 expectativas
Si cualquiera falla → `AirflowFailException`

| # | Expectativa | Descripción |
|---|-------------|-------------|
| 1 | `expect_table_row_count_to_be_between(min=1)` | Hay al menos 1 registro |
| 2 | `expect_column_values_to_not_be_null('user_id')` | user_id obligatorio |
| 3 | `expect_column_values_to_not_be_null('transaction_id')` | PK obligatoria |
| 4 | `expect_column_values_to_be_unique('transaction_id')` | PK única |
| 5 | `expect_column_values_to_be_in_set('currency', ['USD','EUR','GBP'])` | Monedas válidas |
| 6 | `expect_column_values_to_be_in_set('status', ['COMPLETED','PENDING','FAILED'])` | Estados válidos |

#### Suite ADVERTENCIA (no bloqueante) — 2 expectativas
Si fallan → warning pero el pipeline continúa

| # | Expectativa | Descripción |
|---|-------------|-------------|
| 1 | `expect_column_values_to_be_between('amount', 0.01, 100000, mostly=0.99)` | Montos razonables |
| 2 | `expect_column_values_to_match_regex('transaction_id', '^UUID$')` | Formato UUID |

### 5.1 Verificar resultados de validación

```sql
SELECT expectation_suite_name, total_records, failed_records, success_rate, validation_timestamp
FROM audit.gx_validation_logs
ORDER BY validation_timestamp DESC
LIMIT 5;
```

### 5.2 Ejecutar validación GX standalone

> Los scripts están montados como volumen (`./scripts:/opt/airflow/scripts`), ya no necesitas `docker cp`.

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "python /opt/airflow/scripts/validate_qualy.py --table prod.raw_transactions --suite transactions_quality_suite 2>&1"
```

---

## 6. Fase 4: PUBLISH - Transformación con Spark

### Qué ocurre

La tarea `publish_with_spark`:
1. Lee `audit.raw_transactions` vía JDBC
2. **Filtra:** `status = 'COMPLETED'`, `amount > 0`, `user_id IS NOT NULL`
3. **Enriquece:** añade `data_quality_score = 1.0` y `processed_at = NOW()`
4. **Deduplica:** left-anti join contra `prod.raw_transactions`
5. Escribe nuevos registros a `prod.raw_transactions` (append)

### 6.1 Verificar datos en PROD

```sql
SELECT COUNT(*) as total_prod, COUNT(DISTINCT user_id) as usuarios_unicos, AVG(amount) as monto_promedio
FROM prod.raw_transactions;
```

### 6.2 Comparar AUDIT vs PROD

```sql
SELECT
    (SELECT COUNT(*) FROM audit.raw_transactions) as total_audit,
    (SELECT COUNT(*) FROM prod.raw_transactions) as total_prod,
    (SELECT COUNT(*) FROM audit.raw_transactions) - (SELECT COUNT(*) FROM prod.raw_transactions) as registros_filtrados;
```

---

## 7. Fase 5: DBT - Modelado Analítico

### Qué ocurre

`dbt run` construye **6 modelos** en orden de dependencia:

| Orden | Modelo | Tipo | Schema real | Descripción |
|-------|--------|------|-------------|-------------|
| 1 | `stg_raw_transactions` | VIEW | `public_staging` | Estandarización: tipos, partes de fecha, `data_quality_score`, `data_quality_tier`, `transaction_value_tier`, `is_anomaly`, `is_weekend` |
| 2 | `dim_users` | TABLE | `public_analytics` | Dimensión usuarios con RFM (value/frequency/recency 1-5), clasificación active/at_risk/churned |
| 3 | `int_transactions_enriched` | VIEW | `public_intermediate` | Métricas por usuario, categoría, día; moving average 7 días; `composite_reliability_score` |
| 4 | `stg_masked_transactions` | VIEW | `public_development` | Staging con `user_id` enmascarado (`ab****1234`) |
| 5 | `fct_transactions` | TABLE | `public_analytics` | Tabla de hechos: filtro calidad ≥ 0.7, dimensiones temporales |
| 6 | `analytics_transactions` | VIEW | `public_analytics` | Vista pseudonimizada con `pseudonymize_user()` |

### 7.1 Ejecutar dbt manualmente

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt run --profiles-dir /opt/airflow/dbt --target prod"
```

**Resultado esperado:** `Done. PASS=6 WARN=0 ERROR=0 SKIP=0 TOTAL=6`

### 7.2 Ejecutar tests de dbt (26 tests)

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt test --profiles-dir /opt/airflow/dbt --target prod"
```

### 7.3 dbt build (run + test combinados)

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt build --profiles-dir /opt/airflow/dbt --target prod"
```

### 7.4 Ejecutar un solo modelo

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt run --profiles-dir /opt/airflow/dbt --target prod --select stg_masked_transactions --full-refresh"
```

---

## 8. Fase 6: LINEAGE - Registro de Linaje

### Qué ocurre

La tarea `record_lineage` se ejecuta **siempre** (`trigger_rule='all_done'`) y registra nodos y edges del flujo de datos en `audit.data_lineage`.

### 8.1 Ver nodos del linaje

```sql
SELECT node_name, node_type, schema_name, table_name, description
FROM audit.data_lineage
WHERE lineage_type = 'node'
ORDER BY lineage_id;
```

### 8.2 Ver edges (conexiones)

```sql
SELECT source_id, target_id, transformation
FROM audit.data_lineage
WHERE lineage_type = 'edge'
ORDER BY lineage_id;
```

### 8.3 Vista amigable

```sql
SELECT * FROM audit.vw_data_lineage;
```

---

## 9. Consultas de Verificación

> **⚠️ Recordatorio de schemas:**
> | Modelo dbt | Schema real en PostgreSQL |
> |------------|--------------------------|
> | `staging` | `public_staging` |
> | `development` | `public_development` |
> | `intermediate` | `public_intermediate` |
> | `analytics` | `public_analytics` |
>
> **⚠️ Importante:** Si acabas de ejecutar dbt run, abre una **nueva sesión psql** para que los schemas nuevos sean visibles.

### 9.1 Staging (stg_raw_transactions)

```sql
SELECT transaction_id, user_id, product_category, amount,
       data_quality_score, data_quality_tier, transaction_value_tier,
       is_anomaly, is_weekend, transaction_day, transaction_month
FROM public_staging.stg_raw_transactions
LIMIT 5;
```

### 9.2 Datos enmascarados (stg_masked_transactions)

```sql
SELECT transaction_id, user_id_masked, product_category, amount, status
FROM public_development.stg_masked_transactions
LIMIT 5;
```

> La columna se llama `user_id_masked` (NO `masked_user_id`).

### 9.3 Dimensión de usuarios (dim_users)

```sql
SELECT user_id, total_transactions, lifetime_value, avg_ticket_size,
       value_segment, frequency_segment, recency_segment,
       rfm_score, is_active_user, is_at_risk_user, is_churned_user,
       days_since_last_transaction
FROM public_analytics.dim_users
ORDER BY lifetime_value DESC
LIMIT 10;
```

**Interpretación:**
- `lifetime_value`: total gastado por el usuario
- `value_segment / frequency_segment / recency_segment`: 1 (peor) a 5 (mejor)
- `rfm_score`: promedio de los 3 segmentos
- `is_active_user`: compró en ≤30 días
- `is_at_risk_user`: no compra entre 31-90 días
- `is_churned_user`: no compra hace >90 días

### 9.4 Tabla de hechos (fct_transactions)

```sql
SELECT transaction_id, user_id, product_category, amount,
       transaction_quarter, transaction_semester, transaction_day_name,
       data_quality_score, composite_reliability_score, is_anomaly
FROM public_analytics.fct_transactions
ORDER BY transaction_date DESC
LIMIT 10;
```

### 9.5 Vista analítica final (analytics_transactions)

```sql
SELECT transaction_id, user_pseudo_id, product_category, amount,
       transaction_date, status, data_quality_score,
       transaction_quarter, user_value_quartile, user_frequency_quartile
FROM public_analytics.analytics_transactions
LIMIT 10;
```

### 9.6 Función de pseudonimización

```sql
SELECT pseudonymize_user(42) AS user_pseudo_id;
-- Resultado: U_960523de2a2630bf (ejemplo)
```

### 9.7 Vista intermedia enriquecida

> **⚠️ Si no existe:** ejecuta `dbt run` y abre una **nueva sesión psql**.

```sql
SELECT transaction_id, user_id, amount,
       user_total_transactions, user_avg_transaction_amount,
       category_avg_amount, daily_amount_7day_moving_avg,
       deviation_from_user_avg_pct, deviation_from_daily_avg_pct,
       composite_reliability_score
FROM public_intermediate.int_transactions_enriched
LIMIT 5;
```

### 9.8 Dashboard de calidad de datos

```sql
SELECT fecha, total_registros, registros_monto_negativo,
       registros_monto_nulo, registros_user_nulo, porcentaje_calidad
FROM audit.vw_data_quality_dashboard
ORDER BY fecha DESC
LIMIT 7;
```

### 9.9 Catálogo PII

```sql
SELECT * FROM audit.vw_pii_catalog;
```

---

## 10. Herramientas Adicionales

### 10.1 dbt docs

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt docs generate --profiles-dir /opt/airflow/dbt"
```

### 10.2 Validación GX standalone

```bash
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "python /opt/airflow/scripts/validate_qualy.py --table prod.raw_transactions --suite transactions_quality_suite 2>&1"
```

### 10.3 Detección de Schema Drift

```bash
# Verificar drift en prod.raw_transactions (valores por defecto)
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "python /opt/airflow/scripts/monitor_schema.py 2>&1"

# Verificar drift en otra tabla
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "python /opt/airflow/scripts/monitor_schema.py --schema audit --table raw_transactions 2>&1"
```

**Qué hace:** Compara el esquema actual contra el último snapshot en `audit.schema_history`. Detecta columnas nuevas, removidas o con cambio de tipo.

### 10.4 Crypto Shredding (GDPR "Right to be Forgotten")

```bash
# Solicitar eliminación
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "python /opt/airflow/scripts/crypto_shredding.py request --user-id 42 --reason 'Prueba de borrado GDPR' 2>&1"

# Verificar estado
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "python /opt/airflow/scripts/crypto_shredding.py check --user-id 42 2>&1"
```

**Qué hace:** Revoca la clave de cifrado del usuario. Sin la clave, los datos cifrados son irrecuperables. Registra la solicitud en `audit.data_deletion_requests`.

### 10.5 Ver logs del DAG

```bash
docker logs -f taller_etl_master-airflow-scheduler-1
```

### 10.6 Ver métricas del pipeline

```sql
SELECT * FROM audit.pipeline_metrics ORDER BY execution_date DESC LIMIT 5;
```

---

## 11. Troubleshooting

### 11.1 Error: `relation "audit.data_freshness_monitor" does not exist`

**Causa:** El script `init_db/setup_wap.sql` no se ejecutó porque los scripts en `/docker-entrypoint-initdb.d` solo corren la **primera vez** que se crea el contenedor.

**Solución:**
```bash
docker compose down -v
docker compose up -d --build
```
> ⚠️ `down -v` destruye todos los volúmenes (datos incluidos).

### 11.2 Error: `function pseudonymize_user(integer) does not exist`

**Causa:** La función no fue creada en el script de inicialización.

**Solución:**
```bash
# Crear el archivo SQL local si no existe, luego:
docker exec -i taller_etl_master-airflow-webserver-1 bash -c "PGPASSWORD=password_dbt psql -h postgres_warehouse -U user_dbt -d db_warehouse" < scripts/create_pseudonymize_function.sql
```

### 11.3 Error: `relation "public_development.stg_masked_transactions" does not exist`

**Causa:** dbt no se ha ejecutado, o la sesión psql es anterior a la creación de la vista.

**Solución:**
```bash
# 1. Ejecutar dbt run
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt run --profiles-dir /opt/airflow/dbt --target prod --select stg_masked_transactions --full-refresh"

# 2. Abrir una NUEVA sesión psql (obligatorio)
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "PGPASSWORD=password_dbt psql -h postgres_warehouse -U user_dbt -d db_warehouse"
```

### 11.4 Error: `column "id" does not exist` en data_lineage

**Solución:** Usar `lineage_id` (no `id`): `ORDER BY lineage_id`.

### 11.5 Error: `column "day" does not exist` en vw_data_quality_dashboard

**Solución:** La columna se llama `fecha` (no `day`): `ORDER BY fecha DESC`.

### 11.6 Error: `Could not find profile named 'proyecto_unl'`

**Solución:** Usar `--profiles-dir /opt/airflow/dbt` (donde está `profiles.yml`), NO `/opt/airflow/dbt/proyecto_unl`.

### 11.7 Error: `'TableAsset' object has no attribute 'add_batch_definition_whole_table'`

**Causa:** El script `validate_qualy.py` usaba la API de GX 0.18+, pero el proyecto tiene GX 0.17.15.

**Solución:** El script ya fue reescrito para usar `PandasDataset` directamente (compatible con 0.17.x). Asegúrate de copiar la versión actualizada.

### 11.8 Error: Scripts no producen output en PowerShell

**Causa:** Los scripts tienen saltos de línea CRLF (Windows) y Python en Linux no los ejecuta correctamente.

**Solución:** Convertir a LF antes de copiar al contenedor:
```powershell
# En PowerShell (Windows):
powershell -Command "(Get-Content 'scripts\nombre.py' -Raw) -replace \"`r`n\", \"`n\" | Set-Content 'scripts\nombre.py' -NoNewline"

# Luego copiar al contenedor:
docker cp scripts taller_etl_master-airflow-webserver-1:/opt/airflow/scripts
```

### 11.9 Error: `relation "audit.data_deletion_requests" does not exist`

**Causa:** La tabla existe en `setup_wap.sql` pero el script no se ejecutó porque el contenedor ya existía.

**Solución:** Ejecutar manualmente:
```bash
docker exec -i taller_etl_master-airflow-webserver-1 bash -c "PGPASSWORD=password_dbt psql -h postgres_warehouse -U user_dbt -d db_warehouse" < scripts/create_missing_tables.sql
```

### 11.10 El DAG se queda en "Running" indefinidamente

**Diagnosticar:**
```bash
docker logs -f taller_etl_master-airflow-scheduler-1
docker exec -it taller_etl_master-airflow-webserver-1 bash -c "PGPASSWORD=password_dbt psql -h postgres_warehouse -U user_dbt -d db_warehouse -c 'SELECT 1;'"
```

### 11.11 Destruir y recrear todo desde cero

```bash
docker compose down -v
docker compose up -d --build
```

---

## 12. Resumen Rápido de Comandos

| Acción | Comando |
|--------|---------|
| **Levantar todo** | `docker compose up -d --build` |
| **Detener todo** | `docker compose down` |
| **Destruir todo** | `docker compose down -v` |
| **Trigger DAG** | `docker exec -it ... airflow dags trigger dag_wap_unl_final` |
| **Ver estado DAGs** | `docker exec -it ... airflow dags list` |
| **dbt run** | `docker exec -it ... bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt run --profiles-dir /opt/airflow/dbt --target prod"` |
| **dbt test** | `docker exec -it ... bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt test --profiles-dir /opt/airflow/dbt --target prod"` |
| **dbt build** | `docker exec -it ... bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt build --profiles-dir /opt/airflow/dbt --target prod"` |
| **dbt un modelo** | `docker exec -it ... bash -c "cd /opt/airflow/dbt/proyecto_unl && dbt run --profiles-dir /opt/airflow/dbt --target prod --select dim_users --full-refresh"` |
| **GX standalone** | `docker exec -it ... bash -c "python /opt/airflow/scripts/validate_qualy.py --table prod.raw_transactions --suite transactions_quality_suite 2>&1"` |
| **Schema Drift** | `docker exec -it ... bash -c "python /opt/airflow/scripts/monitor_schema.py 2>&1"` |
| **Crypto Shredding** | `docker exec -it ... bash -c "python /opt/airflow/scripts/crypto_shredding.py request --user-id 42 2>&1"` |
| **Acceder a BD** | `docker exec -it ... bash -c "PGPASSWORD=password_dbt psql -h postgres_warehouse -U user_dbt -d db_warehouse"` |
| **Ver logs** | `docker logs -f taller_etl_master-airflow-scheduler-1` |
| **Airflow UI** | http://localhost:8080 (airflow / airflow) |

---

## Glosario

| Término | Significado |
|---------|-------------|
| **WAP** | Write-Audit-Publish: patrón de ingesta con validación intermedia |
| **GX** | Great Expectations: framework de validación de calidad de datos |
| **dbt** | data build tool: herramienta de transformación SQL en el warehouse |
| **PII** | Personally Identifiable Information: datos personales sensibles |
| **RFM** | Recency-Frequency-Monetary: modelo de segmentación de clientes |
| **Crypto-shredding** | Destrucción criptográfica de claves para hacer datos irrecuperables |
| **Schema drift** | Cambio no planificado en el esquema de una tabla |
| **Lineage** | Linaje: rastreo del flujo de datos de origen a destino |
| **Expectation** | Una regla de validación individual en Great Expectations |
| **Suite** | Colección de expectativas agrupadas por severidad |
