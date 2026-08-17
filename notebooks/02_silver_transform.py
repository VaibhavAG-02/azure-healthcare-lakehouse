# Databricks notebook source
# Silver layer: HIPAA Safe Harbor de-identification, referential integrity
# enforcement, and deduplication.

bronze_path = "abfss://bronze@healthcarelakehouse01.dfs.core.windows.net/"
silver_path = "abfss://silver@healthcarelakehouse01.dfs.core.windows.net/"

from pyspark.sql.functions import col, sha2, expr, floor, months_between, current_date

# --- Patients: de-identification ---
# HIPAA Safe Harbor: drop direct identifiers, generalize quasi-identifiers.
patients_bronze = spark.read.format("delta").load(f"{bronze_path}patients")

patients_silver = (patients_bronze
    .withColumn("patient_hash", sha2(col("Id"), 256))
    .withColumn("age", floor(months_between(current_date(), col("BIRTHDATE")) / 12))
    .withColumn("zip3", expr("substring(ZIP, 1, 3)"))
    .drop("SSN", "DRIVERS", "PASSPORT", "FIRST", "LAST", "MAIDEN",
          "ADDRESS", "BIRTHPLACE", "ZIP")
)
patients_silver.write.format("delta").mode("overwrite").save(f"{silver_path}patients")
print(f"patients_silver: {patients_silver.count()} rows")

# --- Child tables: referential integrity + dedup ---
# Note: claims.csv uses a different foreign key column name (PATIENTID)
# than the other five child tables (PATIENT) - handled explicitly below.
fk_column = {
    "conditions": "PATIENT",
    "medications": "PATIENT",
    "procedures": "PATIENT",
    "immunizations": "PATIENT",
    "allergies": "PATIENT",
    "claims": "PATIENTID",
}

for t, fk in fk_column.items():
    df = spark.read.format("delta").load(f"{bronze_path}{t}")
    orphan_count = df.join(
        patients_bronze, col(fk) == patients_bronze.Id, "left_anti"
    ).count()
    assert orphan_count == 0, f"{t}: {orphan_count} orphaned records"
    df_dedup = df.dropDuplicates()
    df_dedup.write.format("delta").mode("overwrite").save(f"{silver_path}{t}")
    print(f"{t}_silver: {df_dedup.count()} rows, 0 orphans")
