# Databricks notebook source
# Bronze layer: raw ingestion of Synthea CSV exports into Delta Lake.
# No transformation logic here beyond schema inference and lineage metadata -
# Bronze preserves source data as-is.

landing_path = "abfss://landing@healthcarelakehouse01.dfs.core.windows.net/"
bronze_path = "abfss://bronze@healthcarelakehouse01.dfs.core.windows.net/"

from pyspark.sql.functions import current_timestamp, lit

tables = [
    "patients", "encounters", "conditions", "medications",
    "procedures", "claims", "immunizations", "allergies"
]

for t in tables:
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(f"{landing_path}{t}.csv"))
    df = (df
          .withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", lit(f"{t}.csv")))
    df.write.format("delta").mode("overwrite").save(f"{bronze_path}{t}")
    print(f"{t}: {df.count()} rows written to bronze")
