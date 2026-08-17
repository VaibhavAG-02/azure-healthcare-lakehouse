# Databricks notebook source
# Gold layer: analysis-ready tables.
# - patients_scd2: Slowly Changing Dimension Type 2 on the patient dimension
# - condition_prevalence: aggregated cohort table

silver_path = "abfss://silver@healthcarelakehouse01.dfs.core.windows.net/"
gold_path = "abfss://gold@healthcarelakehouse01.dfs.core.windows.net/"

from pyspark.sql.functions import lit, current_date, col, count

patients_silver = spark.read.format("delta").load(f"{silver_path}patients")

# Initial SCD2 load: everyone starts as the current version.
patients_scd2 = (patients_silver
    .withColumn("is_current", lit(True))
    .withColumn("effective_date", current_date())
    .withColumn("end_date", lit(None).cast("date"))
)
patients_scd2.write.format("delta").mode("overwrite").save(f"{gold_path}patients_scd2")
print(f"patients_scd2: {patients_scd2.count()} rows")

# Condition prevalence cohort table.
conditions_silver = spark.read.format("delta").load(f"{silver_path}conditions")
condition_prevalence = (conditions_silver
    .groupBy("DESCRIPTION")
    .agg(count("*").alias("patient_count"))
    .orderBy(col("patient_count").desc())
)
condition_prevalence.write.format("delta").mode("overwrite").save(f"{gold_path}condition_prevalence")
print("Gold layer complete")


# --- SCD2 merge test (run separately after initial load to validate change tracking) ---
# from delta.tables import DeltaTable
# from pyspark.sql.functions import when, rand
#
# patients_silver_v2 = spark.read.format("delta").load(f"{silver_path}patients")
# patients_silver_v2 = patients_silver_v2.withColumn(
#     "zip3", when(rand() < 0.05, lit("999")).otherwise(col("zip3"))
# )
#
# target = DeltaTable.forPath(spark, f"{gold_path}patients_scd2")
# target.alias("t").merge(
#     patients_silver_v2.alias("s"),
#     "t.patient_hash = s.patient_hash AND t.is_current = true"
# ).whenMatchedUpdate(
#     condition="t.zip3 <> s.zip3",
#     set={"is_current": "false", "end_date": "current_date()"}
# ).execute()
#
# new_versions = patients_silver_v2.alias("s").join(
#     target.toDF().alias("t"),
#     (col("s.patient_hash") == col("t.patient_hash")) & (col("t.is_current") == False),
#     "inner"
# ).select("s.*") \
#  .withColumn("is_current", lit(True)) \
#  .withColumn("effective_date", current_date()) \
#  .withColumn("end_date", lit(None).cast("date"))
#
# new_versions.write.format("delta").mode("append").save(f"{gold_path}patients_scd2")
