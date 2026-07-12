# End-to-End Data Engineering Project — Azure Medallion Lakehouse

An end-to-end, production-style data engineering pipeline built on Azure, implementing incremental **Change Data Capture (CDC)** ingestion from Azure SQL Database into a **Bronze → Silver → Gold** medallion lakehouse, orchestrated with Azure Data Factory and processed with Databricks (PySpark, Delta Live Tables, Structured Streaming), governed by Unity Catalog, and deployed via Databricks Asset Bundles + GitHub CI/CD.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Azure Data Factory (ADF) |
| Source System | Azure SQL Database |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Processing / Compute | Azure Databricks |
| Transformation | PySpark, Delta Live Tables (DLT) |
| Streaming | Spark Structured Streaming |
| Governance / Catalog | Unity Catalog |
| Deployment / IaC | Databricks Asset Bundles (DABs) |
| Version Control / CI-CD | GitHub, GitHub Actions |
| Table Format | Delta Lake |

---

## Architecture Overview

```
Azure SQL DB (Source)
        │  CDC Lookup (last watermark)
        ▼
   Azure Data Factory
   (ForEach + Copy Data + If Condition)
        │
        ▼
  ADLS Gen2 — Bronze Layer  (raw, as-is ingestion)
        │
        ▼  PySpark / DLT / Structured Streaming
  ADLS Gen2 — Silver Layer  (cleansed, deduplicated, conformed)
        │
        ▼  Delta Live Tables (aggregations, business rules)
  ADLS Gen2 — Gold Layer   (curated, analytics-ready)
        │
        ▼
  Unity Catalog (governance, lineage, access control)
        │
        ▼
  BI / Reporting / ML consumption
```

**Deployment flow:** Code and DLT pipeline definitions are version-controlled in **GitHub**, packaged and deployed to Databricks workspaces (dev/stage/prod) using **Databricks Asset Bundles**, with CI/CD triggered on merge to main.

---

## Medallion Architecture

### 🥉 Bronze Layer — Raw Ingestion
- Data landed **as-is** from Azure SQL DB via ADF Copy Data activity.
- Incremental loads driven by a CDC watermark (`last_cdc` value) rather than full loads.
- Schema-on-read, minimal transformation, full history/audit trail preserved.

### 🥈 Silver Layer — Cleansed & Conformed
- Built with **PySpark** and **Delta Live Tables**.
- Deduplication, null handling, type casting, schema enforcement.
- Slowly Changing Dimension (SCD) logic applied where required.
- Streaming tables built using **Spark Structured Streaming** for near-real-time freshness.

### 🥇 Gold Layer — Curated & Business-Ready
- Aggregated, denormalized tables optimized for BI/analytics and reporting.
- Modeled as fact/dimension or wide tables depending on consumption pattern.
- Exposed to consumers through **Unity Catalog** with fine-grained access control.

---

## Pipeline Implementation — Azure Data Factory

### 1. Incremental CDC Ingestion Flow

The core ingestion pattern reads the last processed watermark, pulls only new/changed records from Azure SQL DB, and writes them into the lake — followed by a conditional branch that either updates the watermark or cleans up an empty output file.

![CDC Lookup and Copy Flow](images/cdc_lookup_copy_flow.png)

**Flow breakdown:**
1. **`Last_cdc`** (Lookup) — retrieves the last successfully processed CDC value/timestamp from a control/metadata table.
2. **`current`** (Set Variable) — stores the current run's boundary value, used to scope the incremental read.
3. **`azuresql_to_lake`** (Copy Data) — extracts records from Azure SQL DB between the last CDC value and current boundary, and lands them in the Bronze layer of the data lake.
4. **`If_new_records`** (If Condition) — branches based on whether new records were found:
   - **True →** `max_cdc` (Script activity computes the new max CDC value) → `update_last_cdc` (Copy Data writes the new watermark back to the control table).
   - **False →** `Delete_emptyFile` deletes the empty output file created when no new records exist, keeping the lake clean.

### 2. ForEach Orchestration + Notification

The above CDC pattern runs inside a **ForEach** loop (to iterate across multiple source tables/entities), followed by a **Web** activity used for downstream notification/triggering (e.g., calling a webhook, triggering a Databricks job, or notifying a monitoring system).

![ForEach Loop and Web Activity](images/foreach_web_activity.png)

- **`ForEach1`** iterates over a list of tables/entities, executing the `Last_cdc → azuresql_to_lake → If_new_records` chain for each one.
- **`Web1`** fires after the loop completes (on success), used to signal downstream systems (e.g., kick off the Databricks/DLT job that processes Bronze → Silver → Gold).

### 3. Pipeline Run Output

Successful end-to-end execution, validated via the ADF monitoring **Output** tab, showing each activity's status, duration, and integration runtime.

![Pipeline Run Output](images/pipeline_output_run.png)

| Activity | Type | Status | Duration |
|---|---|---|---|
| azuresql_to_lake | Copy data | ✅ Succeeded | 19s |
| If_new_records | If Condition | ✅ Succeeded | 29s |
| max_cdc | Script | ✅ Succeeded | 9s |
| update_last_cdc | Copy data | ✅ Succeeded | 16s |
| Web1 | Web | ✅ Succeeded | 15s |

---

## Databricks — Transformation & Governance

- **Unity Catalog** used for centralized metastore, catalog/schema/table governance, and access control across Bronze/Silver/Gold catalogs.
- **Delta Live Tables (DLT)** pipelines declaratively define Silver and Gold table transformations with built-in data quality expectations (`@dlt.expect`) and automatic lineage tracking.
- **Spark Structured Streaming** used for near-real-time Bronze → Silver propagation where low latency is required.
- **PySpark** notebooks/jobs handle custom business logic, joins, and Gold-layer aggregations not covered by declarative DLT.

## Deployment — Databricks Asset Bundles & GitHub

- All Databricks jobs, DLT pipelines, and notebook code are defined as code and version-controlled in **GitHub**.
- **Databricks Asset Bundles (DABs)** package notebooks, job configs, and DLT pipeline definitions for repeatable deployment across **dev / staging / production** workspaces.
- CI/CD via **GitHub Actions** validates and deploys bundles (`databricks bundle deploy`) on merge, ensuring consistent, auditable releases.

---

## Repository Structure

```
.
├── adf/
│   └── pipeline/                  # ADF pipeline JSON exports
├── databricks/
│   ├── bronze/                    # Bronze layer notebooks
│   ├── silver/                    # Silver layer PySpark + DLT
│   ├── gold/                      # Gold layer aggregations
│   └── dlt_pipelines/             # Delta Live Tables definitions
├── bundle/
│   └── databricks.yml             # Databricks Asset Bundle config
├── images/                        # Pipeline screenshots (this README)
├── .github/
│   └── workflows/                 # CI/CD pipeline (GitHub Actions)
└── README.md
```

---

## Key Highlights

- ✅ Incremental CDC ingestion — avoids full reloads, tracks watermark per entity
- ✅ Medallion architecture (Bronze/Silver/Gold) for progressive data refinement
- ✅ Declarative data quality via Delta Live Tables expectations
- ✅ Near real-time processing with Spark Structured Streaming
- ✅ Centralized governance and lineage via Unity Catalog
- ✅ Infrastructure-as-code deployment via Databricks Asset Bundles
- ✅ Full CI/CD from GitHub to Databricks workspaces

---

## Author

Feel free to reach out with questions or suggestions about this project.
