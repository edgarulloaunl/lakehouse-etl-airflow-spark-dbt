\# Lakehouse ETL Pipeline (End-to-End)



Proyecto de integración completa de un pipeline de datos tipo \*\*Lakehouse\*\*, utilizando tecnologías modernas de ingeniería de datos.



\---



\## Arquitectura



El pipeline implementa el patrón \*\*WAP (Write → Audit → Publish)\*\*:





\---



\## Tecnologías utilizadas



\- Apache Airflow → Orquestación

\- Apache Spark → Transformación de datos

\- dbt → Modelado dimensional

\- Great Expectations → Calidad de datos (Quality Gate)

\- MinIO (S3) → Data Lake

\- PostgreSQL → Data Warehouse



\---



\## Flujo del Pipeline



1\. \*\*Ingesta\*\*

&#x20;  - Extracción de datos desde API REST

&#x20;  - Manejo de paginación y errores (429, 500)

&#x20;  - Almacenamiento en MinIO (RAW / Bronze)



2\. \*\*Carga\*\*

&#x20;  - Datos cargados a PostgreSQL (`audit.raw\_transactions`)



3\. \*\*Calidad de Datos\*\*

&#x20;  - Validación con Great Expectations

&#x20;  - Reglas:

&#x20;    - No nulos

&#x20;    - Rango válido (`amount >= 0`)

&#x20;    - Unicidad

&#x20;    - Pipeline se detiene si falla



4\. \*\*Transformación (Spark)\*\*

&#x20;  - Eliminación de valores inválidos

&#x20;  - Eliminación de duplicados

&#x20;  - Dataset limpio (`prod.transactions\_clean`)



5\. \*\*Modelado (dbt)\*\*

&#x20;  - Modelo dimensional tipo estrella:

&#x20;    - `fact\_transactions`

&#x20;    - `dim\_users`

&#x20;    - `dim\_product\_category`



\---



\## Resultados



\- Registros originales: \~1900

\- Registros limpios: \~1835

\- Registros inválidos eliminados: 65



\---



\## Ejecución del proyecto



\### 1. Levantar entorno



```bash

docker compose up -d

