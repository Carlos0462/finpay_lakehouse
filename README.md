# FinPay Lakehouse - Databricks Asset Bundle

## 1. Descripción del caso de uso

FinPay Lakehouse es un proyecto de ingeniería de datos implementado sobre Databricks Lakehouse. El objetivo es construir una arquitectura de datos productiva para procesar información transaccional de una fintech, integrando archivos de transacciones, comercios y usuarios en un flujo de datos gobernado, incremental, observable y desplegable mediante Databricks Asset Bundles.

El caso de uso cubre el procesamiento de datos desde una zona de aterrizaje en Unity Catalog Volumes hasta capas Bronze, Silver y Gold, aplicando validaciones de calidad, manejo de registros rechazados, actualización incremental mediante Auto CDC SCD Type 1, generación de un modelo dimensional y construcción de un dashboard de observabilidad.

El proyecto contempla dos ambientes separados por catálogo:

| Ambiente | Catálogo |
|---|---|
| Desarrollo | `fintech_finpay_dev` |
| Producción | `fintech_finpay` |

Esta separación permite desplegar la misma solución en DEV y PROD sin modificar el código fuente, usando variables del Databricks Asset Bundle.

---

## 2. Arquitectura del proyecto

La arquitectura sigue un enfoque Medallion Architecture:

```text
Landing Zone en Unity Catalog Volume
        ↓
Bronze Raw Tables
        ↓
Bronze CDC-like Changes
        ↓
Bronze Valid / Invalid Changes
        ↓
Silver Current Tables con AUTO CDC SCD Type 1
        ↓
Gold Transactions Enriched
        ↓
Materialized Views del modelo dimensional
        ↓
Dashboard de observabilidad
```

### 2.1 Landing Zone

Los archivos fuente se almacenan en Unity Catalog Volumes:

```text
/Volumes/<catalog>/default/vol_landing/
├── transactions/
├── merchants/
├── users/
└── metadata/
    └── ingestion_archetypes.json
```

El archivo `ingestion_archetypes.json` documenta la configuración metadata-driven de cada fuente: ruta, formato, delimitador, schema location, checkpoint path y tabla destino.

### 2.2 Bronze

La capa Bronze contiene los datos crudos ingestados con Auto Loader. Las columnas de negocio se conservan como `STRING`, siguiendo buenas prácticas de ingesta.

Tablas principales:

```text
bronze.transactions
bronze.merchants
bronze.users
```

También se generan tablas de cambios CDC-like:

```text
bronze.transactions_changes
bronze.merchants_changes
bronze.users_changes
```

Estas tablas modelan cada registro recibido como un evento `UPSERT`, usando la columna propia `_cdc_operation`.

### 2.3 Bronze Valid / Invalid Changes

Después de normalizar y validar los datos, se separan los cambios válidos e inválidos:

```text
bronze.transactions_valid_changes
bronze.transactions_invalid_changes

bronze.merchants_valid_changes
bronze.merchants_invalid_changes

bronze.users_valid_changes
bronze.users_invalid_changes
```

Los registros inválidos se envían luego a cuarentena en Silver.

### 2.4 Silver

La capa Silver contiene las tablas actuales y confiables, construidas con Auto CDC SCD Type 1.

Tablas principales:

```text
silver.transactions
silver.merchants
silver.users
silver.quarantine
```

La tabla `silver.users` aplica reglas de seguridad mediante Unity Catalog:

- Column masking sobre campos PII:
  - `full_name`
  - `document_id`
  - `email`
  - `phone`
- Row-level security para restringir acceso a usuarios autorizados.

### 2.5 Gold

La capa Gold contiene una tabla enriquecida que integra transacciones, usuarios y comercios.

```text
gold.transactions_enriched
```

Esta tabla es la fuente curada del modelo dimensional.

### 2.6 Modelo dimensional

El modelo dimensional se implementa con Materialized Views sobre la capa Gold:

```text
gold.fact_transactions
gold.dim_merchant
gold.dim_user
gold.dim_channel
gold.dim_date
```

El notebook `01_create_materialized_views.ipynb` crea las vistas materializadas una sola vez o cuando cambia el esquema. El notebook `02_refresh_materialized_views.ipynb` refresca las vistas en cada ciclo operativo mediante el job semántico.

### 2.7 Observabilidad

El pipeline tiene event logs persistidos en:

```text
<catalog>.observability.finpay_etl_pipeline_event_log
```

Sobre estos event logs se construyen vistas de soporte:

```text
observability.v_pipeline_events
observability.v_flow_progress
observability.v_processed_records_by_layer
observability.v_quality_expectations
observability.v_rejected_records
observability.v_rejection_rate_by_layer
observability.v_observability_summary
```

Estas vistas alimentan el dashboard AI/BI de observabilidad.

---

## 3. Estructura del repositorio

```text
finpay_lakehouse/
├── .github/
│   └── workflows/
├── databricks.yml
├── resources/
│   ├── finpay_etl_pipeline.yml
│   ├── finpay_ingestion_job.yml
│   ├── finpay_semantic_job.yml
│   └── finpay_observability_dashboard.yml
├── src/
│   ├── utils.py
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── notebooks/
│   ├── 00_setup.ipynb
│   ├── 00_apply_security_rules.ipynb
│   ├── 01_create_materialized_views.ipynb
│   ├── 02_refresh_materialized_views.ipynb
│   └── 03_observability_queries.ipynb
├── dashboard/
│   ├── observability_dev.lvdash.json
│   └── observability_prod.lvdash.json
├── fixtures/
├── README.md
└── pyproject.toml
```

---

## 4. Recursos incluidos en el Databricks Asset Bundle

El DAB empaqueta y despliega los siguientes recursos:

| Recurso | Tipo | Archivo |
|---|---|---|
| Pipeline ETL | Lakeflow Declarative Pipeline | `resources/finpay_etl_pipeline.yml` |
| Job de ingesta | Databricks Job | `resources/finpay_ingestion_job.yml` |
| Job semántico | Databricks Job | `resources/finpay_semantic_job.yml` |
| Dashboard de observabilidad | Databricks AI/BI Dashboard | `resources/finpay_observability_dashboard.yml` |
| Notebooks | Notebooks versionados | `notebooks/` |
| Código fuente | Python Lakeflow | `src/` |
| Dashboard exportado | `.lvdash.json` | `dashboard/` |

---

## 5. Configuración de ambientes

El archivo `databricks.yml` define dos targets:

```yaml
targets:
  dev:
    variables:
      catalog: fintech_finpay_dev
      pipeline_development: true
      observability_dashboard_file: dashboard/observability_dev.lvdash.json

  prod:
    variables:
      catalog: fintech_finpay
      pipeline_development: false
      observability_dashboard_file: dashboard/observability_prod.lvdash.json
```

El mismo código se despliega en ambos ambientes. La diferencia se controla por variables del bundle.

---

## 6. Requisitos previos

Antes de desplegar se requiere:

1. Tener Databricks CLI configurado.
2. Tener un perfil válido en la CLI, por ejemplo:

```bash
databricks auth profiles
```

3. Tener acceso al workspace Databricks.
4. Tener permisos para crear catálogos, schemas, volumes, pipelines, jobs y dashboards.
5. Tener un SQL Warehouse disponible.
6. Tener los archivos fuente en las carpetas correspondientes del volume.

Perfil usado en los comandos:

```bash
--profile finpay-lakehouse
```

---

## 7. Aprovisionamiento inicial manual

El notebook `00_setup.ipynb` se ejecuta manualmente por ambiente. No forma parte de los jobs recurrentes.

### 7.1 Ejecutar setup en DEV

Abrir el notebook:

```text
notebooks/00_setup.ipynb
```

Configurar widgets:

```text
env = dev
catalog = fintech_finpay_dev
landing_volume = vol_landing
```

Ejecutar todo el notebook.

Esto crea:

```text
fintech_finpay_dev.default
fintech_finpay_dev.bronze
fintech_finpay_dev.silver
fintech_finpay_dev.gold
fintech_finpay_dev.observability
/Volumes/fintech_finpay_dev/default/vol_landing
```

### 7.2 Ejecutar setup en PROD

Abrir el mismo notebook:

```text
notebooks/00_setup.ipynb
```

Configurar widgets:

```text
env = prod
catalog = fintech_finpay
landing_volume = vol_landing
```

Ejecutar todo el notebook.

Esto crea:

```text
fintech_finpay.default
fintech_finpay.bronze
fintech_finpay.silver
fintech_finpay.gold
fintech_finpay.observability
/Volumes/fintech_finpay/default/vol_landing
```

---

## 8. Carga de archivos fuente

Después del setup se deben cargar los archivos fuente en el volume correspondiente.

### DEV

```text
/Volumes/fintech_finpay_dev/default/vol_landing/transactions/
/Volumes/fintech_finpay_dev/default/vol_landing/merchants/
/Volumes/fintech_finpay_dev/default/vol_landing/users/
```

### PROD

```text
/Volumes/fintech_finpay/default/vol_landing/transactions/
/Volumes/fintech_finpay/default/vol_landing/merchants/
/Volumes/fintech_finpay/default/vol_landing/users/
```

---

## 9. Despliegue en DEV

### 9.1 Validar bundle en DEV

```bash
databricks bundle validate --target dev --profile finpay-lakehouse
```

### 9.2 Desplegar bundle en DEV

```bash
databricks bundle deploy --target dev --profile finpay-lakehouse
```

### 9.3 Ejecutar Job 1 en DEV

```bash
databricks bundle run finpay_ingestion_job --target dev --profile finpay-lakehouse
```

Este job ejecuta el Lakeflow Declarative Pipeline completo:

```text
Bronze → Silver → Gold
```

### 9.4 Crear materialized views en DEV

Ejecutar manualmente:

```text
notebooks/01_create_materialized_views.ipynb
```

Configurar:

```text
catalog = fintech_finpay_dev
```

Este notebook crea:

```text
fintech_finpay_dev.gold.fact_transactions
fintech_finpay_dev.gold.dim_merchant
fintech_finpay_dev.gold.dim_user
fintech_finpay_dev.gold.dim_channel
fintech_finpay_dev.gold.dim_date
```

### 9.5 Ejecutar Job 2 en DEV

```bash
databricks bundle run finpay_semantic_job --target dev --profile finpay-lakehouse
```

Este job ejecuta:

```text
notebooks/02_refresh_materialized_views.ipynb
```

y refresca las vistas materializadas.

### 9.6 Aplicar seguridad en DEV

Ejecutar manualmente:

```text
notebooks/00_apply_security_rules.ipynb
```

Configurar:

```text
catalog = fintech_finpay_dev
engineering_group = ingenieria
```

### 9.7 Crear vistas de observabilidad en DEV

Ejecutar manualmente:

```text
notebooks/03_observability_queries.ipynb
```

Configurar:

```text
catalog = fintech_finpay_dev
```

---

## 10. Despliegue en PROD

### 10.1 Validar bundle en PROD

```bash
databricks bundle validate --target prod --profile finpay-lakehouse
```

### 10.2 Desplegar bundle en PROD

```bash
databricks bundle deploy --target prod --profile finpay-lakehouse
```

### 10.3 Ejecutar Job 1 en PROD

```bash
databricks bundle run finpay_ingestion_job --target prod --profile finpay-lakehouse
```

Este job ejecuta el Lakeflow Declarative Pipeline en producción:

```text
fintech_finpay.bronze
fintech_finpay.silver
fintech_finpay.gold
```

### 10.4 Crear materialized views en PROD

Ejecutar manualmente:

```text
notebooks/01_create_materialized_views.ipynb
```

Configurar:

```text
catalog = fintech_finpay
```

Este notebook crea:

```text
fintech_finpay.gold.fact_transactions
fintech_finpay.gold.dim_merchant
fintech_finpay.gold.dim_user
fintech_finpay.gold.dim_channel
fintech_finpay.gold.dim_date
```

### 10.5 Ejecutar Job 2 en PROD

```bash
databricks bundle run finpay_semantic_job --target prod --profile finpay-lakehouse
```

Este job refresca el modelo dimensional en producción.

### 10.6 Aplicar seguridad en PROD

Ejecutar manualmente:

```text
notebooks/00_apply_security_rules.ipynb
```

Configurar:

```text
catalog = fintech_finpay
engineering_group = ingenieria
```

### 10.7 Crear vistas de observabilidad en PROD

Ejecutar manualmente:

```text
notebooks/03_observability_queries.ipynb
```

Configurar:

```text
catalog = fintech_finpay
```

---

## 11. Comandos mínimos de demostración

Los comandos mínimos solicitados para evidenciar el despliegue son:

```bash
databricks bundle validate --profile finpay-lakehouse
```

```bash
databricks bundle deploy --target dev --profile finpay-lakehouse
```

```bash
databricks bundle deploy --target prod --profile finpay-lakehouse
```

```bash
databricks bundle run finpay_ingestion_job --target prod --profile finpay-lakehouse
```

```bash
databricks bundle run finpay_semantic_job --target prod --profile finpay-lakehouse
```

También se recomienda mostrar:

```bash
databricks bundle summary --target prod --profile finpay-lakehouse
```

---

## 12. Validaciones SQL recomendadas

### Validar Bronze, Silver y Gold

#### DEV

```sql
SELECT COUNT(*) FROM fintech_finpay_dev.bronze.transactions;
SELECT COUNT(*) FROM fintech_finpay_dev.silver.transactions;
SELECT COUNT(*) FROM fintech_finpay_dev.gold.transactions_enriched;
```

#### PROD

```sql
SELECT COUNT(*) FROM fintech_finpay.bronze.transactions;
SELECT COUNT(*) FROM fintech_finpay.silver.transactions;
SELECT COUNT(*) FROM fintech_finpay.gold.transactions_enriched;
```

### Validar modelo dimensional

#### DEV

```sql
SELECT COUNT(*) FROM fintech_finpay_dev.gold.fact_transactions;
SELECT COUNT(*) FROM fintech_finpay_dev.gold.dim_merchant;
SELECT COUNT(*) FROM fintech_finpay_dev.gold.dim_user;
SELECT COUNT(*) FROM fintech_finpay_dev.gold.dim_channel;
SELECT COUNT(*) FROM fintech_finpay_dev.gold.dim_date;
```

#### PROD

```sql
SELECT COUNT(*) FROM fintech_finpay.gold.fact_transactions;
SELECT COUNT(*) FROM fintech_finpay.gold.dim_merchant;
SELECT COUNT(*) FROM fintech_finpay.gold.dim_user;
SELECT COUNT(*) FROM fintech_finpay.gold.dim_channel;
SELECT COUNT(*) FROM fintech_finpay.gold.dim_date;
```

### Validar observabilidad

#### DEV

```sql
SELECT COUNT(*) FROM fintech_finpay_dev.observability.v_processed_records_by_layer;
SELECT COUNT(*) FROM fintech_finpay_dev.observability.v_rejected_records;
SELECT COUNT(*) FROM fintech_finpay_dev.observability.v_rejection_rate_by_layer;
```

#### PROD

```sql
SELECT COUNT(*) FROM fintech_finpay.observability.v_processed_records_by_layer;
SELECT COUNT(*) FROM fintech_finpay.observability.v_rejected_records;
SELECT COUNT(*) FROM fintech_finpay.observability.v_rejection_rate_by_layer;
```

---

## 13. Seguridad y gobierno

La tabla `silver.users` contiene campos PII. Por ello se aplican controles de seguridad mediante Unity Catalog:

```text
full_name
document_id
email
phone
```

Las reglas implementadas son:

- Column masking: usuarios no autorizados ven datos enmascarados.
- Row-level security: restringe la visibilidad de filas según la función definida.
- El grupo autorizado para ver valores reales es `ingenieria`.

Estas reglas se aplican con:

```text
notebooks/00_apply_security_rules.ipynb
```

---

## 14. Dashboard de observabilidad

El dashboard AI/BI se versiona en:

```text
dashboard/
├── observability_dev.lvdash.json
└── observability_prod.lvdash.json
```

El recurso del DAB es:

```text
resources/finpay_observability_dashboard.yml
```

El dashboard debe mostrar como mínimo:

- Registros procesados exitosamente por capa.
- Registros fallidos o rechazados por calidad.
- Filtro por rango de fechas.
- Filtro por capa.
- Tendencia de registros procesados.
- Tasa de rechazo por capa.
- Distribución de registros por flujo o tabla.

---

## 15. Consideraciones operativas

- `00_setup.ipynb` es manual y se ejecuta una vez por ambiente.
- `01_create_materialized_views.ipynb` es manual y se ejecuta una vez o ante cambios de esquema.
- `finpay_ingestion_job` ejecuta el pipeline completo Bronze, Silver y Gold.
- `finpay_semantic_job` debe ejecutarse después de que el Job 1 finalice correctamente.
- El dashboard debe actualizarse después de que existan event logs y vistas de observabilidad.
- En caso se edite el dashboard desde la UI, se debe exportar nuevamente el `.lvdash.json` y reemplazar el archivo versionado.

---

## 16. Orden recomendado de ejecución en producción

```text
1. Ejecutar 00_setup.ipynb con catalog = fintech_finpay.
2. Cargar archivos fuente en /Volumes/fintech_finpay/default/vol_landing.
3. Validar bundle con target prod.
4. Desplegar bundle con target prod.
5. Ejecutar finpay_ingestion_job en PROD.
6. Ejecutar 01_create_materialized_views.ipynb con catalog = fintech_finpay.
7. Ejecutar finpay_semantic_job en PROD.
8. Ejecutar 00_apply_security_rules.ipynb con catalog = fintech_finpay.
9. Ejecutar 03_observability_queries.ipynb con catalog = fintech_finpay.
10. Validar dashboard de observabilidad en PROD.
```
