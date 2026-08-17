# Architecture diagram

Add a diagram here (e.g. `pipeline-diagram.png`) built in Excalidraw, diagrams.net, or similar - showing:

Synthea → landing (ADLS Gen2) → ADF (Databricks Job activity) → Databricks Job (serverless, 3 chained tasks) → bronze → silver (de-identification + integrity checks) → gold (SCD2 + cohort tables)

A text version of this diagram is in the main README under "Architecture".
