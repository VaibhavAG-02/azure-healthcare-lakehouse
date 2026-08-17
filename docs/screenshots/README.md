# Evidence screenshots

Drop screenshots here using these exact filenames (referenced in the main README):

- `01_adf_pipeline_success.png` — ADF Monitor tab, pipeline run detail, showing the Databricks Job activity succeeded with duration
- `02_databricks_job_run.png` — Databricks Jobs & Pipelines, run detail, all 3 tasks (bronze_ingest, silver_transform, gold_aggregate) green with individual durations
- `03_deidentification_columns.png` — output of `patients_scd2` schema/columns list, confirming SSN/FIRST/LAST/ADDRESS/DRIVERS/PASSPORT/MAIDEN/BIRTHPLACE/ZIP are absent
- `04_gold_output.png` — sample rows from `condition_prevalence`
- `05_referential_integrity.png` — Silver notebook output showing "0 orphans" for all 6 child tables
- `06_cost_management.png` — Azure Cost Management, actual spend for this project's resource group
- `07_storage_containers.png` — ADLS Gen2 storage account containers view, showing landing/bronze/silver/gold
